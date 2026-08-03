# tests.test_netlimit_cli —— 命令行与环境变量接线。
#
# 这一层的 bug 不会让计算出错，但会让**运维以为自己配了、其实没配**：
# 参数位置写反了就报 unrecognized、环境变量写错了却被静默忽略。所以这里
# 测的全是"接线有没有真的接上"。

from __future__ import annotations

import pytest

from netlimit import cli
from netlimit import plan as P


def parsed(argv, env=None, monkeypatch=None):
    args = cli.build_parser().parse_args(argv)
    if env is not None:
        for k, v in env.items():
            monkeypatch.setenv(k, v)
    cli.apply_env_defaults(args)
    return args


def test_global_options_work_on_either_side_of_the_subcommand(monkeypatch):
    """`netlimit --only eth0 plan` 与 `netlimit plan --only eth0` 都得能用。

    运维不该被 argparse 的选项位置规则绊住——写错位置时它报的是
    "unrecognized arguments"，看着像是这个选项根本不存在。
    """
    monkeypatch.delenv("NETLIMIT_ONLY", raising=False)
    a = parsed(["--only", "eth0", "plan", "-r", "100"])
    b = parsed(["plan", "--only", "eth0", "-r", "100"])
    assert a.only == b.only == ["eth0"]
    assert a.rate == b.rate == 100


def test_subcommand_default_does_not_clobber_the_global_value(monkeypatch):
    """子命令上的默认值必须是 SUPPRESS，否则它会把主解析器**已经解析到的**
    值覆盖掉——表现为"我明明给了 --only，却没生效"。"""
    monkeypatch.delenv("NETLIMIT_ONLY", raising=False)
    args = parsed(["--only", "eth9", "apply", "-r", "100"])
    assert args.only == ["eth9"]


def test_env_fills_in_what_the_command_line_omitted(monkeypatch):
    args = parsed(["apply"], {
        "NETLIMIT_MBPS": "1500", "NETLIMIT_ONLY": "eth0 eth1",
        "NETLIMIT_EXEMPT": "22 443", "NETLIMIT_IFB": "ifb7",
    }, monkeypatch)
    assert args.rate == 1500.0
    assert args.only == ["eth0", "eth1"]
    assert args.exempt == [22, 443]
    assert args.ifb == "ifb7"


def test_command_line_beats_env(monkeypatch):
    args = parsed(["apply", "-r", "300"], {"NETLIMIT_MBPS": "1500"}, monkeypatch)
    assert args.rate == 300.0


def test_empty_env_rate_means_no_limit(monkeypatch):
    """配置文件里把额度留空 = 不限速。这与 `-r` 不给是同一个语义——
    两条入口必须一致，否则 systemd 那条路会和手敲的行为对不上。"""
    args = parsed(["apply"], {"NETLIMIT_MBPS": ""}, monkeypatch)
    assert args.rate is None
    assert P.LimitPlan.from_mbps(["eth0"], args.rate).off


def test_bad_env_rate_fails_loudly(monkeypatch):
    """写错的额度必须报错。静默忽略的话，运维会以为限速配好了。"""
    with pytest.raises(SystemExit, match="必须是数字"):
        parsed(["apply"], {"NETLIMIT_MBPS": "一千"}, monkeypatch)


def test_bad_env_exempt_fails_loudly(monkeypatch):
    with pytest.raises(SystemExit, match="端口号"):
        parsed(["apply"], {"NETLIMIT_EXEMPT": "22 ssh"}, monkeypatch)


def test_off_subcommand_has_no_rate(monkeypatch):
    """off 就是"没设额度"这个状态，不该接受 -r。"""
    monkeypatch.delenv("NETLIMIT_MBPS", raising=False)
    with pytest.raises(SystemExit):
        cli.build_parser().parse_args(["off", "-r", "100"])
