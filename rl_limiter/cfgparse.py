# rl_limiter.cfgparse —— 受管端口清单的构建与配置热更轮询（通用形态）。
#
# 本分支不解析任何服务配置：受管"端口清单"直接来自 YAML 的 ports 段
# （名字 → 本机监听端口，运维登记，和机器上跑什么服务无关），只为按
# 端口的静态入向限速（request_limits）提供端口号；整机监控与总量限速
# 不需要任何端口登记。
#
# watch() 轮询 YAML（5s，或被写 API poke 立即重读）：ports /
# request_limits / instance_quota_mbps / dynamic_ingress 变化即热生效；
# log_level / tick_interval_s 启动定型（变化提示重启）。

from __future__ import annotations

import asyncio
import logging
import re
import zlib
from dataclasses import dataclass, field

from . import model

# 段首关键字：出现任意一个即结束上一段。这里列 HAProxy 常见的顶层段。
_SECTION_KEYWORDS = (
    "global", "defaults", "frontend", "listen", "backend", "peers",
    "resolvers", "userlist", "mailers", "cache", "program", "ring",
    "http-errors", "fcgi-app", "crt-store",
)

# bind 参数里的端口：形如 ":8080"、"10.0.0.1:8080"、"*:8080"、
# "ipv4@0.0.0.0:8080"、"[::]:8080"。统一取最后一个冒号后的数字。
_BIND_PORT_RE = re.compile(r":(\d+)$")

# 配置轮询周期（秒）：cfg/YAML 是本机文件，读一次的成本可忽略，5s 与
# 原数据库轮询的节奏一致。
WATCH_INTERVAL_S = 5.0


@dataclass(**model.SLOTS)
class ParsedSection:
    """haproxy.cfg 里一个 frontend/listen 段的解析结果。"""

    name: str
    kind: str                 # frontend | listen
    binds: list[tuple[str, int]] = field(default_factory=list)  # (地址, 端口)
    mode: str = ""            # 段内显式 mode；空 = 继承 defaults


def build_frontends(
    sections: list[ParsedSection],
    quotas: dict[str, float],
    log: logging.Logger,
    _warned: set[str] | None = None,
) -> list[model.FrontendConfig]:
    """把解析出的段与 YAML 登记的限额合并成受管 frontend 清单。

    合并规则：
      - cfg 里有、quotas 里登记了正限额 → 限速 + 监控；
      - cfg 里有、quotas 里没有 → **只监控不限速**（quota_mbps=0，
        不建 tc 类、不做超限判定）——**默认不限速**是正常状态，
        只记一条 info 供排障时核对，不按异常告警；
      - cfg 里有、quotas 里**显式写 0** → 同样只监控不限速，但这是
        运维的明确决定，不再 warn；
      - quotas 里有、cfg 里没有 → warn 一次（多半是 cfg 里删了段、或
        名字拼错），该限额条目被忽略。
    名为 stats 的段与没有 bind 的段直接跳过（监控/限速都无从谈起）。

    _warned 由调用方跨轮次持有：同一内容的告警只发一次，避免每个轮询
    周期刷屏；内容变化后（cfg/quotas 任一变了）watch 会换一个新集合，
    新问题照常告警。
    """
    warned = _warned if _warned is not None else set()
    out: list[model.FrontendConfig] = []
    seen: set[str] = set()
    for s in sections:
        if s.name == "stats":
            continue
        if not s.binds:
            key = f"nobind:{s.name}"
            if key not in warned:
                warned.add(key)
                log.debug("cfg 段没有可解析的 bind 端口，跳过（unix bind 或"
                          "纯 backend 用法） section=%s", s.name)
            continue
        if s.name in seen:
            key = f"dup:{s.name}"
            if key not in warned:
                warned.add(key)
                log.warning("haproxy.cfg 里有重名的 frontend/listen 段，只取"
                            "第一个（stats 的 pxname 无法区分重名段） name=%s",
                            s.name)
            continue
        seen.add(s.name)
        addr, port = s.binds[0]
        if len(s.binds) > 1:
            key = f"multibind:{s.name}"
            if key not in warned:
                warned.add(key)
                log.warning(
                    "段有多条 bind，限速只按第一条的端口分类，其余端口的"
                    "流量不在该段限额内（需要限速的端口应各自成段） "
                    "section=%s used=%s:%d extra=%s",
                    s.name, addr, port,
                    ",".join(f"{a}:{p}" for a, p in s.binds[1:]))
        quota = float(quotas.get(s.name, 0.0))
        if quota <= 0 and s.name not in quotas and quotas:
            # 端口级出向限额（quotas）语义存在时才提示"谁没登记"。
            # 通用形态 quotas 恒空——端口登记只为静态入向限速服务，
            # 这里不该逐端口刷"未登记限额"的噪声。
            key = f"noquota:{s.name}"
            if key not in warned:
                warned.add(key)
                log.info(
                    "端口未登记出向限额，默认不限速（只监控） "
                    "section=%s bind=%s:%d", s.name, addr or "*", port)
        out.append(model.FrontendConfig(
            name=s.name, bind_address=addr, bind_port=port,
            mode=s.mode if s.mode in ("tcp", "http") else "tcp",
            quota_mbps=quota))

    for name in sorted(set(quotas) - seen):
        key = f"orphan:{name}"
        if key not in warned:
            warned.add(key)
            log.warning(
                "quotas 里登记的 frontend 在 haproxy.cfg 中不存在（段被删除"
                "或名字拼错），该限额条目被忽略 name=%s", name)
    return out


def sections_from_ports(ports: dict[str, int]) -> list[ParsedSection]:
    """把 YAML 的 ports 登记（名字 → 本机监听端口）派生成段清单。

    通用形态没有服务配置可解析：端口清单由运维直接登记，仅为按端口的
    静态入向限速服务。bind 地址留空（= 所有地址，tc 只按端口分类）。"""
    return [ParsedSection(name=n, kind="port", binds=[("", p)], mode="tcp")
            for n, p in ports.items()]


def load_frontends(ports: dict[str, int], quotas: dict[str, float],
                   log: logging.Logger,
                   _warned: set[str] | None = None) -> list[model.FrontendConfig]:
    """从端口登记构建受管清单（通用形态：无文件可读，纯派生）。"""
    return build_frontends(sections_from_ports(ports), quotas, log, _warned)


def canonical(frontends: list[model.FrontendConfig],
              instance_quota_mbps: float = 0.0,
              request_limits: "dict | None" = None) -> str:
    """配置内容的精确身份（变更检测用字符串比较，不经哈希）。"""
    body = ";".join(
        f"{f.name}|{f.bind_address}|{f.bind_port}|{f.mode}|{f.quota_mbps:g}"
        for f in sorted(frontends, key=lambda x: x.name)
    )
    req = ",".join(
        f"{name}|{rl.mbps:g}|{rl.conn_rate}"
        for name, rl in sorted((request_limits or {}).items()))
    return f"{body};inst:{instance_quota_mbps:g};req:{req}"


def checksum(frontends: list[model.FrontendConfig],
             instance_quota_mbps: float = 0.0,
             request_limits: "dict | None" = None) -> int:
    """内容校验和：充当配置版本号（控制台展示/核对用）。"""
    return zlib.crc32(canonical(frontends, instance_quota_mbps,
                                request_limits).encode("utf-8"))


async def watch(
    yaml_path: str,
    queue: "asyncio.Queue[model.ControllerConfig]",
    boot_frontends: list[model.FrontendConfig],
    log: logging.Logger,
    interval_s: float = WATCH_INTERVAL_S,
    boot_instance_quota_mbps: float = 0.0,
    boot_request_limits: "dict | None" = None,
    poke: "asyncio.Event | None" = None,
    dyn_ingress_cb=None,
    boot_dynamic_ingress=None,
) -> None:
    """常驻任务：轮询 YAML 配置文件，内容变化就把新
    ControllerConfig 投入 queue（监控循环热应用，tc/告警基准随之更新）。

    - 变更判定用**解析结果**的规范化字符串比较，而不是文件 mtime——
      触发 reload 的运维动作常伴随 touch/原子替换，mtime 变了内容没变
      不该触发一次空应用；
    - 任一文件读不到或解析失败：保留当前配置继续运行（fail-static），
      warn 后下一轮再试——cfg 正在被原子替换的瞬间读到半份文件属于
      预期内抖动；
    - 端口登记（ports）、request_limits/instance_quota_mbps 与闭环参数
      （dynamic_ingress，经 dyn_ingress_cb 整段回调）热生效；接线与服务
      级运行参数（log_level、tick_interval_s）启动时定型，
      运行期改动不热生效（检测到变化会提示需重启）。
    """
    from . import config as configmod  # 延迟导入避免环形依赖

    last = canonical(boot_frontends, boot_instance_quota_mbps,
                     boot_request_limits)
    warned: set[str] = set()
    last_yaml_rest: tuple | None = None
    # di 基线从**启动配置**播种（给了的话）：否则第一轮轮询才建基线，
    # 启动后第一个轮询周期内的修改会被当成基线静默漏掉。
    last_di: "str | None" = (repr(boot_dynamic_ingress)
                             if boot_dynamic_ingress is not None else None)
    while True:
        # poke 事件让写接口（控制台/API 改完配置文件后）立即触发一轮
        # 轮询，不必干等下一个周期。
        if poke is None:
            await asyncio.sleep(interval_s)
        else:
            try:
                await asyncio.wait_for(poke.wait(), timeout=interval_s)
            except asyncio.TimeoutError:
                pass
            poke.clear()
        try:
            svc = configmod.load(yaml_path)
            quotas = dict(svc.quotas)
            yaml_rest = (svc.log_level, svc.tick_interval_s)
            di_repr = repr(svc.dynamic_ingress)
            frontends = load_frontends(svc.ports, quotas, log, warned)
        except Exception as e:
            log.warning(
                "重读配置失败，保留当前配置继续运行（fail-static），下一轮"
                "再试 yaml=%s err=%s", yaml_path, e)
            continue

        if last_yaml_rest is None:
            last_yaml_rest = yaml_rest
        elif yaml_rest != last_yaml_rest:
            last_yaml_rest = yaml_rest
            log.warning(
                "检测到 YAML 里接线/运行参数发生变化（log_level、haproxy "
                "接线等）：这些字段在进程启动时定型，热更新不生效，请重启 "
                "rl-limiter")

        # 闭环参数（dynamic_ingress）整段热更：变化即回调（装配层换给
        # 控制器并按需 poke 下发）。与限额不同，它不走监控循环的配置
        # 队列——控制器与监控循环本就在同一事件循环里，直接换无竞态。
        if last_di is None:
            last_di = di_repr
        elif di_repr != last_di:
            last_di = di_repr
            if dyn_ingress_cb is not None:
                try:
                    dyn_ingress_cb(svc.dynamic_ingress)
                    log.info("dynamic_ingress（闭环参数）热更新已生效 "
                             "enabled=%s interval=%gs 死区=[%g, %g] "
                             "step_up=%g step_down_max=%g floor=%g "
                             "epsilon=%g",
                             svc.dynamic_ingress.enabled,
                             svc.dynamic_ingress.interval_s,
                             svc.dynamic_ingress.low_ratio,
                             svc.dynamic_ingress.high_ratio,
                             svc.dynamic_ingress.step_up,
                             svc.dynamic_ingress.step_down_max,
                             svc.dynamic_ingress.floor_ratio,
                             svc.dynamic_ingress.apply_epsilon)
                except Exception as e:
                    log.warning("dynamic_ingress 热更新回调失败，"
                                "保留旧参数继续运行 err=%s", e)

        cur = canonical(frontends, svc.instance_quota_mbps,
                        svc.request_limits)
        if cur == last:
            continue
        last = cur
        # 内容变了：换一个新的告警去重集合，让新配置里的问题照常告警。
        warned.clear()
        version = zlib.crc32(cur.encode("utf-8"))
        queue.put_nowait(model.ControllerConfig(
            version=version, frontends=frontends,
            instance_quota_mbps=svc.instance_quota_mbps,
            request_limits=dict(svc.request_limits)))
        log.info(
            "检测到 haproxy.cfg / 限额配置变化，已提交监控循环热生效 "
            "version=%s frontends=%d instance_quota=%s request_limits=%d",
            version, len(frontends),
            f"{svc.instance_quota_mbps:g}Mbps"
            if svc.instance_quota_mbps else "不限",
            len(svc.request_limits))
