# rl_limiter.configstore —— 把写 API 的修改回写到 YAML 配置文件。
#
# 这是全项目**唯一**会写配置文件的地方，而且只写 rl-limiter 自己的 YAML
# （-c 指定的那份）——haproxy.cfg 仍然绝对只读，"rl-limiter 不改 cfg、
# 不 reload haproxy"的承诺不变。能改的只有限额类字段：quotas 里的单个
# frontend 限额、网卡总限速 nic_quota_mbps。接线字段（haproxy 段）、
# log_level 这些**不开放**给 API——它们改了要重启进程，属于运维操作。
#
# ## 写入方式：整份重写 + 原子替换
#
# 每次修改都是：读文件 → yaml.safe_load → 改内存里的原始结构 → 用与启动
# 完全相同的校验链（config.from_raw）验证整份配置 → 序列化 → 写临时文件
# → os.replace 原子替换。要点：
#
#   - **改之前先整份校验**：修改后的配置要是不能通过启动校验，这次写入
#     就被拒绝——绝不会写出一份"服务重启就起不来"的 YAML；
#   - **原子替换**：写一半断电/崩溃时旧文件完好无损，cfgparse.watch 也
#     绝不会读到半份文件；临时文件建在同目录（os.replace 跨文件系统会
#     失败，/tmp 往往是另一个挂载点）；
#   - **注释会丢**：yaml.safe_load → dump 的往返只保留数据。文件头部会
#     写一行说明注明这一点。想保留手写注释的运维应把说明性内容放到别处
#     （或干脆不用写 API，直接编辑文件——两条路的最终效果一致）。
#
# ## 并发
#
# 服务内所有写操作都经 asyncio 事件循环里的同一把锁（webconsole 持有）
# 串行化；进程外有人同时手工编辑的话，最后写的赢——与两个运维同时改
# 同一份文件的语义相同，不做花哨的合并。

from __future__ import annotations

import os
import tempfile

import yaml

from . import config as configmod

# 回写文件的头部说明。运维打开文件第一眼就该知道注释为什么没了。
_HEADER = (
    "# 本文件由 rl-limiter 的写 API 重写过：手写注释不会保留（YAML 数据\n"
    "# 往返的限制）。字段含义见 deploy/config/limiter.example.yaml。\n"
    "# haproxy.cfg 仍然只读——rl-limiter 只会写这一份自己的配置。\n"
)


class ConfigStoreError(ValueError):
    """回写失败（配置不合法 / 文件读写问题）。信息面向 API 调用者。"""


def _load_raw(path: str) -> dict:
    """读并解析当前 YAML，返回顶层映射（空文件按空映射处理）。"""
    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = yaml.safe_load(f.read())
    except OSError as e:
        raise ConfigStoreError(f"读不到配置文件 {path}：{e}") from None
    except yaml.YAMLError as e:
        raise ConfigStoreError(
            f"配置文件 {path} 不是合法 YAML，拒绝回写（请先手工修复）：{e}"
        ) from None
    if raw is None:
        raw = {}
    if not isinstance(raw, dict):
        raise ConfigStoreError(
            f"配置文件 {path} 顶层不是键值映射，拒绝回写（请先手工修复）")
    return raw


def _write_atomic(path: str, raw: dict) -> None:
    """校验整份配置后原子写回。校验不过即抛错，文件不被碰。"""
    # 与启动完全相同的校验链：能写出去的配置一定能被服务重新加载。
    configmod.from_raw(raw, source="回写后的配置")
    text = _HEADER + yaml.safe_dump(
        raw, allow_unicode=True, sort_keys=False, default_flow_style=False)
    d = os.path.dirname(os.path.abspath(path)) or "."
    fd, tmp = tempfile.mkstemp(dir=d, prefix=".config-", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(text)
            f.flush()
            os.fsync(f.fileno())
        # 保留原文件的权限位（mkstemp 建出来是 0600，直接 replace 会把
        # 组可读之类的权限抹掉）。
        try:
            os.chmod(tmp, os.stat(path).st_mode & 0o7777)
        except OSError:
            pass
        os.replace(tmp, path)
    except OSError as e:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise ConfigStoreError(f"写配置文件 {path} 失败：{e}") from None


def set_quota(path: str, name: str, quota_mbps: float) -> None:
    """登记/修改一个 frontend 的限额（Mbps；0 = 显式不限速）。

    name 是否真的存在于 haproxy.cfg 由调用方（webconsole）校验——这里
    只管"写出去的 YAML 合法"。
    """
    raw = _load_raw(path)
    quotas = raw.get("quotas")
    if quotas is None:
        quotas = {}
    if not isinstance(quotas, dict):
        raise ConfigStoreError(
            "配置文件里的 quotas 不是键值映射，拒绝回写（请先手工修复）")
    quotas = dict(quotas)
    quotas[name] = quota_mbps
    raw["quotas"] = quotas
    _write_atomic(path, raw)


def remove_quota(path: str, name: str) -> bool:
    """从 quotas 里删掉一个 frontend 的登记（回到"只监控不限速"）。

    返回是否真的删了（本来就没登记时返回 False，也不写文件）。
    """
    raw = _load_raw(path)
    quotas = raw.get("quotas")
    if not isinstance(quotas, dict) or name not in quotas:
        return False
    quotas = dict(quotas)
    del quotas[name]
    raw["quotas"] = quotas
    _write_atomic(path, raw)
    return True


def set_nic_quota(path: str, quota_mbps: float) -> None:
    """设置网卡总限速（Mbps；0 = 不限，等价于删掉该字段）。"""
    raw = _load_raw(path)
    if quota_mbps:
        raw["nic_quota_mbps"] = quota_mbps
    else:
        raw.pop("nic_quota_mbps", None)
    _write_atomic(path, raw)
