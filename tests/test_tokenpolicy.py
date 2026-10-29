# tests.test_tokenpolicy —— 写 API 令牌强度规则。
#
# 两头都要钉住：人编的、看着长实则弱的令牌一律拦下；真随机生成器
# （openssl rand -hex / -base64、secrets.token_*）产出的一律放行——误杀
# 会让运维对着"随机生成的令牌被拒"一头雾水。

from __future__ import annotations

import base64
import random
import secrets

import pytest

from rl_limiter import tokenpolicy as tp


@pytest.mark.parametrize("token, why", [
    ("", "未设置"),
    ("demo-token", "太短"),
    ("x" * 300, "太长"),
    ("Xk9#mP2$vL8@nQ4!wR7&jT5*yU3^hB6%", "不允许的字符"),
    ("has space in the middle of a long token", "不允许的字符"),
    (secrets.token_hex(15), "太短"),                 # 30 位十六进制
    ("a" * 40, "重复"),
    ("ab" * 20, "重复"),
    ("0123456789abcdef0123456789abcdef", "重复"),
    ("passwordpasswordpasswordpassword1", "重复"),
    ("Pa55w0rd-Pa55w0rd-Pa55w0rd-Pa55w0rd-X", "重复"),
    ("q8Zr3Lq8Zr3Lq8Zr3Lq8Zr3Lq8Zr3Lq8Zr3LX", "重复"),
    ("node1-node1-node1-node1-node1-node1-xyz", "重复"),
    ("abcdefghijklmnopqrstuvwxyz0123456789", "顺序"),
    ("my-company-node-api-token-2026-production", "token"),
    ("Shanghai-DataCenter-Node-01-Fleet-Key", "fleet"),
    ("ThisIsMySuperSecretFleetKeyOkay99", "secret"),
    ("rl-limiter-node-sh-01-api-key-v2-2026", "limiter"),
])
def test_weak_tokens_rejected_with_reason(token, why):
    got = tp.weakness(token)
    assert why in got
    assert not tp.is_strong(token)


def test_hex_minimum_is_32_chars():
    """纯十六进制按 16 个字符算强度：32 位 = 128 bit 刚好达标。"""
    t = "6471612859116ad3c81e10ab050f519f"
    assert len(t) == 32 and tp.is_strong(t)
    assert "太短" in tp.weakness(t[:31])


_RNG = random.Random(20261029)        # 固定种子：样本可复现，测试不抖


def _b(n: int) -> bytes:
    return _RNG.getrandbits(8 * n).to_bytes(n, "big")


@pytest.mark.parametrize("gen", [
    lambda: _b(16).hex(),                                    # openssl rand -hex 16
    lambda: _b(24).hex(),
    lambda: _b(32).hex(),                                    # 推荐 / --token auto
    lambda: _b(24).hex().upper(),
    lambda: base64.b64encode(_b(32)).decode(),               # openssl rand -base64 32
    lambda: base64.b64encode(_b(24)).decode(),
    lambda: base64.urlsafe_b64encode(_b(32)).decode().rstrip("="),  # token_urlsafe
    lambda: base64.urlsafe_b64encode(_b(24)).decode().rstrip("="),
], ids=["hex16", "hex24", "hex32", "HEX24", "b64-32", "b64-24", "url32",
        "url24"])
def test_random_generator_formats_pass(gen):
    """生成器格式的随机令牌不能被误杀。

    离线各实测二十万~三十万个：十六进制零误杀（拼不出任何弱词），
    base64 约百万分之几（随机拼出 token/admin/fleet 这类 5 字母词，换一个
    即可）。这里用固定种子抽 2000 个，结果可复现。"""
    bad = [t for t in (gen() for _ in range(2000)) if not tp.is_strong(t)]
    assert not bad, bad[:3]


def test_hex_tokens_cannot_hit_weak_words():
    """--token auto 生成十六进制：a-f 拼不出任何弱词，词表误杀为零。"""
    assert not any(set(w) <= set("abcdef") for w in tp._WEAK_WORDS)


def test_effective_treats_weak_as_none():
    strong = secrets.token_hex(32)
    assert tp.effective(strong) == (strong, "")
    assert tp.effective(f"  {strong}\n") == (strong, "")
    assert tp.effective("") == ("", "")
    tok, why = tp.effective("demo-token")
    assert tok == "" and "太短" in why
