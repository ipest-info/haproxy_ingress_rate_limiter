# rl_limiter.tokenpolicy —— 写 API 令牌的强度规则（节点、总控、总控里登记
# 的节点令牌、安装脚本共用这一份）。
#
# 令牌是改限速的唯一凭据：读接口无鉴权、控制台常绑内网地址，令牌一弱，
# 同网段谁都能改生产限速。所以规则刻意定得很硬，**不达标的令牌一律当作
# 没有令牌**——写接口整体禁用（节点/总控），总控里登记的节点令牌按只读
# 观测处理；安装脚本与总控的写 API 则直接拒收。宁可写不了，不可被猜中。
#
# 规则（全部满足才算强）：
#   1. 字符集只收 A-Z a-z 0-9 . _ ~ + / = -（随 HTTP 头发送、写进 systemd
#      EnvironmentFile 都不用转义）；
#   2. 长度 32~256；
#   3. 理论强度 ≥ 128 bit：长度 × log2(用到的字符集大小)——纯十六进制按
#      16、只有小写按 26……如实估，不高估；
#   4. 不是"长但低熵"的串：字符分布香农熵 ≥ 2.5 bit/字符，且不同三元组
#      （相邻 3 字符）占比 ≥ 75%——"aaaa…"、"abab…"、短片段或单词反复
#      （"Pa55w0rd-Pa55w0rd-…"）都过不了；
#   5. 没有人为痕迹：同一字符或 ASCII 顺序/逆序连续 ≥ 8 个（"12345678"、
#      "abcdefgh"）、整串是某个短片段的循环、含常见弱词（password/
#      secret/token/admin/changeme…）。
#
# 阈值按真随机令牌的误杀率定（各实测二十万~三十万个）：十六进制
# （openssl rand -hex 16/24/32、--token auto）零误杀——a-f 拼不出任何
# 弱词；base64/urlsafe 约百万分之几（随机串里恰好拼出 token/admin/fleet
# 这类 5 字母词），真碰上了换一个即可。推荐生成方式：openssl rand -hex 32
# （64 位十六进制，256 bit），或安装时 install.sh --token auto 自动生成。

from __future__ import annotations

import math
import re
from collections import Counter

MIN_LEN = 32
MAX_LEN = 256
MIN_BITS = 128
MIN_SHANNON = 2.5
MIN_TRIGRAM_RATIO = 0.75
MAX_RUN = 7                  # 同字符/顺序连续允许的最长长度（≥8 判人为）
GENERATE_HINT = "openssl rand -hex 32（或安装时 install.sh --token auto）"

_ALLOWED = re.compile(r"^[A-Za-z0-9._~+/=-]+$")
_HEX = re.compile(r"^(?:[0-9a-f]+|[0-9A-F]+)$")
# 人编令牌的常见成分。只放 ≥5 个字母的词：4 个字母的（test/demo/node）
# 在随机 base64 里太常撞上；≥6 个字母的随机撞上的概率在亿分之一量级。
_WEAK_WORDS = ("password", "passwd", "secret", "token", "admin", "changeme",
               "example", "qwerty", "letmein", "default", "fleet",
               "limiter", "server", "cluster", "master", "production",
               "company", "center", "centre", "secure", "welcome", "manager",
               "access", "private", "public")
_RANDOM_HINT = "——如果确实是随机生成的，重新生成一个即可"


def _alphabet(token: str) -> int:
    """估算字符集大小（用到哪几类字符就算哪几类，十六进制单算）。"""
    if _HEX.match(token):
        return 16
    size = 0
    if any(c.islower() for c in token):
        size += 26
    if any(c.isupper() for c in token):
        size += 26
    if any(c.isdigit() for c in token):
        size += 10
    if any(not c.isalnum() for c in token):
        size += 7                               # . _ ~ + / = -
    return size


def _shannon(token: str) -> float:
    n = len(token)
    return -sum(c / n * math.log2(c / n) for c in Counter(token).values())


def _trigram_ratio(token: str) -> float:
    grams = [token[i:i + 3] for i in range(len(token) - 2)]
    return len(set(grams)) / len(grams)


def _longest_run(token: str) -> int:
    """同一字符、或 ASCII 码 +1/-1 递进的最长连续段长度。"""
    best = cur_same = cur_up = cur_down = 1
    for a, b in zip(token, token[1:]):
        d = ord(b) - ord(a)
        cur_same = cur_same + 1 if d == 0 else 1
        cur_up = cur_up + 1 if d == 1 else 1
        cur_down = cur_down + 1 if d == -1 else 1
        best = max(best, cur_same, cur_up, cur_down)
    return best


def _periodic(token: str) -> bool:
    """整串是某个短片段（≤ 一半长度）的循环，如 "q8Zr3Lq8Zr3L…"。"""
    n = len(token)
    for p in range(1, n // 2 + 1):
        if all(token[i] == token[i % p] for i in range(n)):
            return True
    return False


def weakness(token: str) -> str:
    """令牌不够强的原因（面向运维的中文说明）；够强返回空串。

    空令牌返回 "未设置"——调用方据此区分"没配"与"配了但太弱"。
    """
    if not token:
        return "未设置"
    n = len(token)
    if n > MAX_LEN:
        return f"太长（{n} 位，上限 {MAX_LEN}）"
    if not _ALLOWED.match(token):
        return "含不允许的字符（只收字母、数字与 ._~+/=-）"
    if n < MIN_LEN:
        return f"太短（{n} 位，至少 {MIN_LEN} 位）"
    bits = n * math.log2(_alphabet(token))
    if bits < MIN_BITS:
        return (f"强度不足（估算 {bits:.0f} bit，至少 {MIN_BITS} bit——"
                f"纯十六进制至少 32 位）")
    if (_shannon(token) < MIN_SHANNON
            or _trigram_ratio(token) < MIN_TRIGRAM_RATIO):
        return "重复字符/片段过多，不像随机生成的" + _RANDOM_HINT
    if _longest_run(token) > MAX_RUN:
        return (f"含 {MAX_RUN + 1} 位以上的重复或顺序字符（如 aaaaaaaa、"
                f"12345678），不像随机生成的" + _RANDOM_HINT)
    if _periodic(token):
        return "是某个短片段的循环，不像随机生成的" + _RANDOM_HINT
    low = token.lower()
    for w in _WEAK_WORDS:
        if w in low:
            return f"含常见弱词 {w!r}，不像随机生成的" + _RANDOM_HINT
    return ""


def is_strong(token: str) -> bool:
    return not weakness(token)


def effective(token: str) -> "tuple[str, str]":
    """(生效令牌, 作废原因)：够强 → (token, "")；不够强 → ("", 原因)；
    没配 → ("", "")。"不达标就当没有令牌"统一走这里。"""
    token = (token or "").strip()
    if not token:
        return "", ""
    why = weakness(token)
    return ("", why) if why else (token, "")
