# hapagg.cli —— 命令行入口。
#
#   hapagg view    -f targets.txt          聚合视图（默认）
#   hapagg view    -f targets.txt --watch  每 N 秒刷新
#   hapagg node    -f targets.txt          每台机器一行的健康小结
#   hapagg drill   -f targets.txt -p fe_x  某个 proxy 在各台上的原值
#   hapagg targets -f targets.txt          只解析清单并打印，不连接任何机器
#   hapagg json    -f targets.txt          机器可读的输出，喂给别的系统
#
# 目标清单从 -f 文件与 -t 命令行汇总，两边同一套解析规则（见 targets.py）。

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time

from . import aggregate, collect, stats, view
from .targets import DEFAULT_PORT, TargetError, load_targets


def _targets(args):
    try:
        ts = load_targets(args.file or [], args.target or [], args.port)
    except TargetError as e:
        print(f"目标清单有问题：{e}", file=sys.stderr)
        raise SystemExit(2)
    if not ts:
        print("目标清单为空。用 -f <文件> 或 -t <IP:端口> 指定要监控的 HAProxy",
              file=sys.stderr)
        raise SystemExit(2)
    return ts


def _collect(args):
    return asyncio.run(collect.collect(_targets(args), args.timeout,
                                       args.concurrency))


def cmd_targets(args) -> int:
    """只解析清单。批量导入写错了要能在**连任何机器之前**就发现。"""
    ts = _targets(args)
    print(f"共 {len(ts)} 个目标：")
    for t in ts:
        name = f"  {t.name}" if t.name else ""
        print(f"  {t.endpoint}{name}")
    return 0


def cmd_view(args) -> int:
    while True:
        coll = _collect(args)
        v = aggregate.aggregate(coll)
        if args.watch:
            print("\033[2J\033[H", end="")        # 清屏 + 光标回原点
            print(time.strftime("%Y-%m-%d %H:%M:%S"))
        print(view.render(v, detail=args.detail, all_cols=args.all_cols))
        if not args.watch:
            # 一台都没采到时用非零退出码：这个命令常被塞进脚本或告警里，
            # "全挂了"和"一切正常"必须在退出码上就能分开。
            return 0 if v.ok_nodes else 1
        time.sleep(args.watch)


def cmd_node(args) -> int:
    coll = _collect(args)
    rows = [[n.label, n.target.endpoint, note]
            for n, note in aggregate.node_health(coll)]
    print(view.table(["名字", "端点", "情况"], rows))
    print(f"\n{len(coll.ok_nodes)}/{len(coll.nodes)} 台采集成功")
    return 0 if coll.ok_nodes else 1


def cmd_drill(args) -> int:
    v = aggregate.aggregate(_collect(args))
    print(view.render_drilldown(v, args.proxy))
    return 0


def cmd_json(args) -> int:
    """机器可读输出。字段名与视图里的维度名一致。"""
    v = aggregate.aggregate(_collect(args))
    doc = {
        "ts": v.ts,
        "coverage": {"total": v.total_nodes, "ok": v.ok_nodes,
                     "complete": v.complete,
                     "failures": [{"node": n, "error": e} for n, e in v.failures]},
        "info": v.info,
        "objects": [
            {"pxname": o.pxname, "svname": o.svname, "type": o.type_name,
             "status": o.status, "nodes": o.nodes, "values": o.values,
             "per_node": o.per_node}
            for o in sorted(v.objects.values(), key=lambda x: (x.type, x.pxname, x.svname))
        ],
        "frontend_totals": aggregate.totals(v, stats.TYPE_FRONTEND),
    }
    json.dump(doc, sys.stdout, ensure_ascii=False, indent=2)
    print()
    return 0 if v.ok_nodes else 1


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        prog="hapagg",
        description="多台 HAProxy 的监控聚合：批量拉 stats socket，合并成一个视图")

    def shared(p, suppress=False):
        import argparse as A
        d = A.SUPPRESS if suppress else None
        p.add_argument("-f", "--file", action="append", metavar="PATH",
                       default=d if suppress else None,
                       help="目标清单文件（每行一个 IP:端口，支持注释/命名/末段区间）")
        p.add_argument("-t", "--target", action="append", metavar="ADDR",
                       default=d if suppress else None,
                       help="直接给目标（可重复，也可用逗号分隔）")
        p.add_argument("-p", "--port", type=int, default=d if suppress else DEFAULT_PORT,
                       metavar="PORT", help=f"默认端口（默认 {DEFAULT_PORT}）")
        p.add_argument("--timeout", type=float,
                       default=d if suppress else collect.DEFAULT_TIMEOUT_S,
                       metavar="SEC", help="单台超时秒数")
        p.add_argument("--concurrency", type=int,
                       default=d if suppress else collect.DEFAULT_CONCURRENCY,
                       metavar="N", help="并发拨号上限")
        return p

    shared(ap)
    sub = ap.add_subparsers(dest="cmd")

    for name, fn, desc in (
        ("view", cmd_view, "聚合视图（默认）"),
        ("node", cmd_node, "每台机器一行的健康小结"),
        ("targets", cmd_targets, "只解析目标清单，不连接任何机器"),
        ("json", cmd_json, "机器可读输出"),
    ):
        sp = shared(sub.add_parser(name, help=desc), suppress=True)
        sp.set_defaults(func=fn)
        if name == "view":
            sp.add_argument("--watch", type=float, nargs="?", const=5.0,
                            metavar="SEC", help="每 N 秒刷新一次（默认 5）")
            sp.add_argument("--detail", action="store_true",
                            help="连 server 明细一起显示")
            sp.add_argument("--all", dest="all_cols", action="store_true",
                            help="显示全部监控维度（默认只显示核心列）")

    sp = shared(sub.add_parser("drill", help="某个 proxy 在各台上的原值"),
                suppress=True)
    sp.add_argument("--proxy", required=True, metavar="PXNAME",
                    help="要下钻的 proxy 名（pxname）")
    sp.set_defaults(func=cmd_drill)
    return ap


def main(argv: list[str] | None = None) -> int:
    ap = build_parser()
    args = ap.parse_args(argv)
    if not getattr(args, "func", None):
        # 不给子命令就当 view —— 这是最常用的那个，别让人多打一个词。
        args = ap.parse_args((argv if argv is not None else sys.argv[1:]) + ["view"])
    for name, default in (("watch", None), ("detail", False),
                          ("all_cols", False)):
        if not hasattr(args, name):
            setattr(args, name, default)
    try:
        return args.func(args)
    except collect.CollectError as e:
        print(f"采集失败：{e}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
