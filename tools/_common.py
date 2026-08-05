# tools/_common.py —— tools 目录下各独立脚本共享的小助手。
#
# 这些脚本以 `python3 tools/<name>.py` 方式直跑（sys.path[0] 即 tools/
# 目录），因此可以直接 `from _common import ...` 互相共享，而不必把
# 演示工具的代码搬进 hap_agg 包。

from __future__ import annotations

import os
import sys


def env_int(name: str, default: int, prog: str) -> int:
    """读取整数环境变量：未设置或空白返回 default；非法值打印带 prog
    前缀的中文错误后以退出码 1 结束进程（脚本口径的 fail-fast，区别于
    hap_agg.config 库口径的抛 ValueError）。"""
    raw = (os.environ.get(name) or "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        print(f"{prog}: 环境变量 {name} 必须是整数，当前值 {raw!r}",
              file=sys.stderr)
        raise SystemExit(1)
