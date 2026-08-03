# rl_limiter.cfgparse —— 从本机 haproxy.cfg 直接读取受管 frontend 清单。
#
# 设计原则：**haproxy.cfg 是负载均衡配置的唯一权威**。监听端口、模式、
# 后端服务器都由运维直接写在 cfg 里（rl-limiter 不渲染、不改写、没有
# 受管区块、没有任何默认负载均衡配置）；rl-limiter 只**读** cfg，解析出
# 各 frontend/listen 段的 名字 + 监听端口 + 模式，再与 YAML 里登记的
# 限额（quotas）合并成运行期配置——限速（tc 按端口分类）与监控（按
# pxname 采样）都以此为准。
#
# 解析口径（刻意保守）：
#   - 只认 `frontend <name>` 与 `listen <name>` 两种段（backend 段没有
#     监听端口，与限速/采样无关）；
#   - 段内取第一条 `bind` 的地址与端口作为该段的监听端点；一个段有多条
#     bind 时取第一条并 warn（tc 按单端口分类，多 bind 段的其余端口不在
#     限速范围内——需要限速的端口应各自成段）；
#   - `mode` 取段内值，缺省继承最近一个 `defaults` 段（HAProxy 语义），
#     再缺省按 tcp（HAProxy 的默认）；
#   - 名为 "stats" 的段、以及没有 bind 的 frontend/listen 段跳过；
#   - 解析失败宁可少不可错：看不懂的行一律忽略（cfg 的完整语法校验是
#     `haproxy -c` 的职责，不在这里重造）。
#
# 热更新（watch）：轮询 haproxy.cfg 与 YAML 配置文件的内容，任一变化就
# 重新解析/加载并把新 ControllerConfig 投给监控循环——运维改完 cfg 并
# reload HAProxy 后，rl-limiter 的限速与监控视图在一个轮询周期内自动
# 跟上，不需要重启本服务。

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


@dataclass(slots=True)
class ParsedSection:
    """haproxy.cfg 里一个 frontend/listen 段的解析结果。"""

    name: str
    kind: str                 # frontend | listen
    binds: list[tuple[str, int]] = field(default_factory=list)  # (地址, 端口)
    mode: str = ""            # 段内显式 mode；空 = 继承 defaults


def _split_bind_endpoint(spec: str) -> tuple[str, int] | None:
    """解析 bind 的第一个参数为 (地址, 端口)。

    支持 ":8080"、"addr:8080"、"*:8080"、"ipv4@addr:8080"、"[::]:8080"。
    unix socket（"/path" 或 "unix@..."）与 abns 没有端口，返回 None。
    """
    if spec.startswith("/") or spec.startswith("unix@") or spec.startswith("abns@"):
        return None
    m = _BIND_PORT_RE.search(spec)
    if not m:
        return None
    port = int(m.group(1))
    addr = spec[: m.start()]
    # 剥掉协议前缀与通配地址：解析目标是"这个段听哪个端口"，地址仅作
    # 展示；"*" 与空串等价（所有地址）。
    if "@" in addr:
        addr = addr.split("@", 1)[1]
    if addr in ("*", "0.0.0.0", "::", "[::]"):
        addr = ""
    return (addr, port)


def parse_haproxy_cfg(text: str) -> list[ParsedSection]:
    """把 haproxy.cfg 文本解析成 frontend/listen 段清单（含 defaults 的
    mode 继承）。只读取本模块需要的三样信息：段名、bind、mode。"""
    sections: list[ParsedSection] = []
    cur: ParsedSection | None = None
    in_defaults = False
    defaults_mode = ""  # 最近一个 defaults 段的 mode（HAProxy 的继承语义）

    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split()
        kw = parts[0]

        if kw in _SECTION_KEYWORDS:
            cur = None
            in_defaults = False
            if kw == "defaults":
                in_defaults = True
            elif kw in ("frontend", "listen") and len(parts) >= 2:
                cur = ParsedSection(name=parts[1], kind=kw)
                sections.append(cur)
            continue

        if in_defaults and kw == "mode" and len(parts) >= 2:
            defaults_mode = parts[1]
        elif cur is not None:
            if kw == "bind" and len(parts) >= 2:
                ep = _split_bind_endpoint(parts[1])
                if ep is not None:
                    cur.binds.append(ep)
            elif kw == "mode" and len(parts) >= 2:
                cur.mode = parts[1]

    # mode 继承：段内没写的补 defaults 的值；仍没有则按 HAProxy 默认 tcp。
    for s in sections:
        if not s.mode:
            s.mode = defaults_mode or "tcp"
    return sections


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
        不建 tc 类、不做超限判定），warn 一次提示未登记限额；
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
        if quota <= 0 and s.name not in quotas:
            key = f"noquota:{s.name}"
            if key not in warned:
                warned.add(key)
                log.warning(
                    "frontend 未在 YAML 的 quotas 里登记限额，只监控不限速 "
                    "section=%s bind=%s:%d（要限速请在配置文件 quotas 段加"
                    "一行：%s: <Mbps>）",
                    s.name, addr or "*", port, s.name)
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


def load_frontends(cfg_path: str, quotas: dict[str, float],
                   log: logging.Logger,
                   _warned: set[str] | None = None) -> list[model.FrontendConfig]:
    """读 cfg_path 并合并限额，返回受管 frontend 清单。

    文件读不到/读错按异常抛出，调用方决定是启动失败（引导阶段）还是
    保留当前配置（watch 阶段的 fail-static）。
    """
    with open(cfg_path, "r", encoding="utf-8", errors="replace") as f:
        text = f.read()
    return build_frontends(parse_haproxy_cfg(text), quotas, log, _warned)


def canonical(frontends: list[model.FrontendConfig]) -> str:
    """配置内容的精确身份（变更检测用字符串比较，不经哈希）。"""
    return ";".join(
        f"{f.name}|{f.bind_address}|{f.bind_port}|{f.mode}|{f.quota_mbps:g}"
        for f in sorted(frontends, key=lambda x: x.name)
    )


def checksum(frontends: list[model.FrontendConfig]) -> int:
    """内容校验和：充当配置版本号（控制台展示/核对用）。"""
    return zlib.crc32(canonical(frontends).encode("utf-8"))


async def watch(
    cfg_path: str,
    yaml_path: str,
    queue: "asyncio.Queue[model.ControllerConfig]",
    boot_frontends: list[model.FrontendConfig],
    log: logging.Logger,
    interval_s: float = WATCH_INTERVAL_S,
) -> None:
    """常驻任务：轮询 haproxy.cfg 与 YAML 配置文件，内容变化就把新
    ControllerConfig 投入 queue（监控循环热应用，tc/告警基准随之更新）。

    - 变更判定用**解析结果**的规范化字符串比较，而不是文件 mtime——
      触发 reload 的运维动作常伴随 touch/原子替换，mtime 变了内容没变
      不该触发一次空应用；
    - 任一文件读不到或解析失败：保留当前配置继续运行（fail-static），
      warn 后下一轮再试——cfg 正在被原子替换的瞬间读到半份文件属于
      预期内抖动；
    - YAML 里除 quotas 以外的字段（接线、日志级别）启动时定型，运行期
      改动不热生效（检测到变化会提示需重启）。
    """
    from . import config as configmod  # 延迟导入避免环形依赖

    last = canonical(boot_frontends)
    warned: set[str] = set()
    last_yaml_rest: tuple | None = None
    while True:
        await asyncio.sleep(interval_s)
        try:
            svc = configmod.load(yaml_path)
            quotas = dict(svc.quotas)
            yaml_rest = (svc.log_level, svc.tick_interval_s,
                         svc.haproxy.endpoint(), svc.haproxy.cfg_path)
            frontends = load_frontends(cfg_path, quotas, log, warned)
        except Exception as e:
            log.warning(
                "重读配置失败，保留当前配置继续运行（fail-static），下一轮"
                "再试 cfg=%s yaml=%s err=%s", cfg_path, yaml_path, e)
            continue

        if last_yaml_rest is None:
            last_yaml_rest = yaml_rest
        elif yaml_rest != last_yaml_rest:
            last_yaml_rest = yaml_rest
            log.warning(
                "检测到 YAML 里接线/运行参数发生变化（log_level、haproxy "
                "接线等）：这些字段在进程启动时定型，热更新不生效，请重启 "
                "rl-limiter")

        cur = canonical(frontends)
        if cur == last:
            continue
        last = cur
        # 内容变了：换一个新的告警去重集合，让新配置里的问题照常告警。
        warned.clear()
        version = zlib.crc32(cur.encode("utf-8"))
        queue.put_nowait(model.ControllerConfig(
            version=version, frontends=frontends))
        log.info(
            "检测到 haproxy.cfg / 限额配置变化，已提交监控循环热生效 "
            "version=%s frontends=%d", version, len(frontends))
