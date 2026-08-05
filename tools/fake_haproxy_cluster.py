#!/usr/bin/env python3
# tools/fake_haproxy_cluster.py —— 起若干个"假 HAProxy stats socket"，
# 用来联调 hapagg（多机监控聚合）。
#
# 与 tools/fake_haproxy.py 的区别：那个是给 rl-limiter 用的，只回 frontend
# 行、只有限速需要的那几列。这个要喂的是**监控聚合**，所以：
#
#   - 回**完整列头**（照真实 HAProxy 2.8 的 CSV 列，含 type 列）；
#   - 每个 proxy 回 frontend + backend + 若干 server 三类行；
#   - 支持故意制造运维现场会遇到的几种情况：某台不监听（连不上）、
#     某台响应很慢、某台少一个 proxy（cfg 没同步）、某个后端 DOWN、
#     某台是老版本（少几列）。
#
# 有这些才能验证聚合程序**在不完美的现实里**表现正确——全都正常时谁写
# 都对，出问题时才见真章。
#
# 用法：
#   python3 tools/fake_haproxy_cluster.py --ports 19001,19002,19003
#   python3 tools/fake_haproxy_cluster.py --ports 19001 --slow 2.0
#   python3 tools/fake_haproxy_cluster.py --ports 19001 --down be_api/srv2
#   python3 tools/fake_haproxy_cluster.py --ports 19001 --drop-proxy fe_api
#   python3 tools/fake_haproxy_cluster.py --ports 19001 --old-version

from __future__ import annotations

import argparse
import asyncio
import random
import time

# 真实 HAProxy 2.8 的 `show stat` 列头。**照抄真实输出**——自洽的假数据
# 会把"按列名取值"这件事的价值整个盖掉（按列序取也能跑通）。
COLUMNS = (
    "pxname,svname,qcur,qmax,scur,smax,slim,stot,bin,bout,dreq,dresp,ereq,econ,"
    "eresp,wretr,wredis,status,weight,act,bck,chkfail,chkdown,lastchg,downtime,"
    "qlimit,pid,iid,sid,throttle,lbtot,tracked,type,rate,rate_lim,rate_max,"
    "check_status,check_code,check_duration,hrsp_1xx,hrsp_2xx,hrsp_3xx,hrsp_4xx,"
    "hrsp_5xx,hrsp_other,hanafail,req_rate,req_rate_max,req_tot,cli_abrt,srv_abrt,"
    "comp_in,comp_out,comp_byp,comp_rsp,lastsess,last_chk,last_agt,qtime,ctime,"
    "rtime,ttime,agent_status,agent_code,agent_duration,check_desc,agent_desc,"
    "check_rise,check_fall,check_health,agent_rise,agent_fall,agent_health,addr,"
    "cookie,mode,algo,conn_rate,conn_rate_max,conn_tot,intercepted,dcon,dses,"
    "wrew,connect,reuse,cache_lookups,cache_hits,srv_icur,src_ilim,qtime_max,"
    "ctime_max,rtime_max,ttime_max,eint,idle_conn_cur,safe_conn_cur,"
    "used_conn_cur,need_conn_est,uweight,agg_server_status"
).split(",")

# 老版本（2.4 之前）没有这些列。用它模拟"集群里混着老机器"。
NEW_ONLY = {"conn_rate", "conn_rate_max", "conn_tot", "qtime_max", "ctime_max",
            "rtime_max", "ttime_max", "eint", "uweight", "agg_server_status",
            "idle_conn_cur", "safe_conn_cur", "used_conn_cur", "need_conn_est"}


class FakeNode:
    def __init__(self, port: int, seed: int, proxies: list[str],
                 slow: float = 0.0, down: set[str] = frozenset(),
                 drop: set[str] = frozenset(), old: bool = False):
        self.port = port
        self.rng = random.Random(seed)
        self.proxies = [p for p in proxies if p not in drop]
        self.slow = slow
        self.down = down
        self.old = old
        self.t0 = time.time()
        # 每台机器的流量基数不同——聚合的意义正在于把不同量级的机器加起来。
        self.scale = 1 + seed % 3

    def _cols(self) -> list[str]:
        return [c for c in COLUMNS if not (self.old and c in NEW_ONLY)]

    def _row(self, values: dict[str, object]) -> str:
        cols = self._cols()
        return ",".join(str(values.get(c, "")) for c in cols)

    def stat_csv(self) -> str:
        el = max(1.0, time.time() - self.t0)
        cols = self._cols()
        out = ["# " + ",".join(cols)]
        for i, px in enumerate(self.proxies):
            base = int(el * 1_000_000 * self.scale * (i + 1))
            scur = self.rng.randint(5, 60) * self.scale
            stot = int(el * 20 * self.scale * (i + 1))
            # frontend 行
            out.append(self._row({
                "pxname": px, "svname": "FRONTEND", "type": 0,
                "scur": scur, "smax": scur + 40, "slim": 100000, "stot": stot,
                "bin": base // 4, "bout": base, "status": "OPEN",
                "rate": 10 * self.scale, "rate_max": 90 * self.scale,
                "conn_rate": 12 * self.scale, "conn_rate_max": 95 * self.scale,
                "conn_tot": stot + 7, "req_rate": 11 * self.scale,
                "req_rate_max": 88 * self.scale, "req_tot": stot * 2,
                "hrsp_2xx": stot * 2 - 5, "hrsp_4xx": 3, "hrsp_5xx": 2,
                "ereq": 1, "dreq": 0, "dcon": 0, "mode": "http",
            }))
            bname = "be_" + px.split("_", 1)[-1]
            srvs = [f"srv{n}" for n in (1, 2)]
            up = [s for s in srvs if f"{bname}/{s}" not in self.down]
            for n, s in enumerate(srvs, 1):
                is_down = f"{bname}/{s}" in self.down
                out.append(self._row({
                    "pxname": bname, "svname": s, "type": 2,
                    "scur": scur // 2, "smax": scur, "stot": stot // 2,
                    "bin": base // 8, "bout": base // 2,
                    "status": "DOWN" if is_down else "UP",
                    "weight": 0 if is_down else 100, "act": 0 if is_down else 1,
                    "bck": 0, "chkfail": 3 if is_down else 0,
                    "chkdown": 1 if is_down else 0, "lbtot": stot // 2,
                    "qcur": 0, "qmax": 2,
                    # 各台的延迟不同，正是加权平均要处理的情形
                    "qtime": 1 * self.scale, "ctime": 2 * self.scale,
                    "rtime": 20 * self.scale, "ttime": 25 * self.scale,
                    "qtime_max": 9, "ctime_max": 11,
                    "rtime_max": 120 * self.scale, "ttime_max": 150 * self.scale,
                    "econ": 0, "eresp": 0, "cli_abrt": 1, "srv_abrt": 0,
                    "check_status": "L4CON" if is_down else "L4OK",
                    "mode": "http", "sid": n,
                }))
            out.append(self._row({
                "pxname": bname, "svname": "BACKEND", "type": 1,
                "scur": scur, "smax": scur + 30, "slim": 100000, "stot": stot,
                "bin": base // 4, "bout": base,
                "status": "UP" if up else "DOWN",
                "weight": 100 * len(up), "act": len(up), "bck": 0,
                "qcur": 0, "qmax": 4, "lbtot": stot,
                "qtime": 1 * self.scale, "ctime": 2 * self.scale,
                "rtime": 20 * self.scale, "ttime": 25 * self.scale,
                "econ": 0, "eresp": 0, "wretr": 0, "wredis": 0,
                "hrsp_2xx": stot * 2 - 5, "hrsp_5xx": 2,
                "mode": "http", "algo": "roundrobin",
            }))
        return "\n".join(out) + "\n"

    def info_text(self) -> str:
        el = int(time.time() - self.t0)
        lines = [
            "Name: HAProxy",
            f"Version: {'2.4.22' if self.old else '2.8.16'}",
            "Release_date: 2025/01/01",
            f"Pid: {10000 + self.port}",
            f"Uptime_sec: {el + self.port % 100}",
            f"CurrConns: {40 * self.scale}",
            f"CumConns: {el * 30 * self.scale}",
            f"CumReq: {el * 60 * self.scale}",
            f"ConnRate: {12 * self.scale}",
            f"SessRate: {11 * self.scale}",
            f"Maxconn: {100000}",
            "MaxconnReached: 0",
            f"CurrSslConns: {2 * self.scale}",
            # 各台空闲率不同：合并要取最小（最差），不是求和也不是平均
            f"Idle_pct: {max(5, 95 - 20 * self.scale)}",
            f"Tasks: {30 + self.scale}",
            f"Run_queue: {self.scale}",
            "Nbthread: 4",
        ]
        return "\n".join(lines) + "\n"

    async def handle(self, reader, writer):
        try:
            line = await asyncio.wait_for(reader.readline(), timeout=5)
            cmd = line.decode(errors="replace").strip()
            if self.slow:
                await asyncio.sleep(self.slow)
            if cmd.startswith("show stat"):
                writer.write(self.stat_csv().encode())
            elif cmd.startswith("show info"):
                writer.write(self.info_text().encode())
            else:
                writer.write(b"Unknown command. Please enter one of the following...\n")
            await writer.drain()
        except Exception:
            pass
        finally:
            # 真实 runtime socket 应答完即关闭连接——客户端每条命令都要
            # 重新拨号。不模拟这一点的话，客户端里"复用长连接"的 bug
            # 在联调时根本暴露不出来。
            writer.close()

    async def serve(self):
        srv = await asyncio.start_server(self.handle, "127.0.0.1", self.port)
        return srv


async def amain(args) -> None:
    ports = [int(p) for p in args.ports.split(",") if p.strip()]
    proxies = [p.strip() for p in args.proxies.split(",") if p.strip()]
    servers = []
    for i, port in enumerate(ports):
        node = FakeNode(
            port=port, seed=i, proxies=proxies, slow=args.slow,
            down=set(x.strip() for x in (args.down or "").split(",") if x.strip()),
            drop=set(x.strip() for x in (args.drop_proxy or "").split(",") if x.strip()),
            old=args.old_version)
        servers.append(await node.serve())
        print(f"假 HAProxy 已监听 127.0.0.1:{port}"
              f"（{len(node.proxies)} 个 proxy）")
    await asyncio.gather(*(s.serve_forever() for s in servers))


def main() -> None:
    ap = argparse.ArgumentParser(description="起若干假 HAProxy stats socket")
    ap.add_argument("--ports", default="19001,19002,19003")
    ap.add_argument("--proxies", default="fe_main,fe_api")
    ap.add_argument("--slow", type=float, default=0.0,
                    help="每次应答前故意睡这么久，模拟慢机器")
    ap.add_argument("--down", default="",
                    help="标记为 DOWN 的后端，如 be_api/srv2（逗号分隔）")
    ap.add_argument("--drop-proxy", default="",
                    help="这台机器上不存在的 proxy，模拟 cfg 没同步")
    ap.add_argument("--old-version", action="store_true",
                    help="模拟老版本 HAProxy（CSV 少若干列）")
    try:
        asyncio.run(amain(ap.parse_args()))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
