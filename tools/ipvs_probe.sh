#!/usr/bin/env bash
# tools/ipvs_probe.sh —— LVS/IPVS 可行性研究的实测脚本（docs/10 §6 第 1、2 步）。
#
# **只在实验机上跑，不要在生产机上跑**：它会建 network namespace、临时的
# IPVS 服务和 tc 队列树。收尾会清理，但中途被打断就得手工清（末尾有命令）。
#
#   sudo tools/ipvs_probe.sh doctor     环境体检：内核有没有 ip_vs、有没有 htb
#   sudo tools/ipvs_probe.sh classify   **第 1 步（地基）**：IPVS-NAT 反向改写
#                                       之后，tc 还能不能按源端口把下行流量
#                                       分到对应的类里
#   sudo tools/ipvs_probe.sh stats      **第 2 步**：/proc/net/ip_vs 的按服务
#                                       字节计数够不够做监控（存在？在涨？64 位？）
#   sudo tools/ipvs_probe.sh clean      清理残留
#
# 为什么第 1 步是地基：本项目的限速是 tc 挂在网卡出向、**按源端口**分类的。
# 换成 IPVS 之后，下行包的源端口由 IPVS 的反向 NAT 改写回 VIP 的服务端口。
# 按内核数据路径（POSTROUTING → dev_queue_xmit → qdisc），tc 应该看到的是
# 改写**之后**的端口，因此现有 tc 规则一个字都不用改——**但这条推论本仓库
# 没有实测过**，而整个方案建立在它上面。这个脚本就是来把它验掉的。
#
# 拓扑（全在 netns 里，不碰宿主机网络）：
#
#   ipvs-cli (10.99.0.2)  ──┐
#                           │  veth
#   ipvs-dir (10.99.0.1) ───┤   ← IPVS 服务 10.99.0.1:VPORT，NAT 到 RS
#                           │      tc HTB 挂在 dir 的客户端侧网卡出向
#   ipvs-rs  (10.99.1.2) ───┘

set -uo pipefail

NS_DIR=ipvs-dir
NS_CLI=ipvs-cli
NS_RS=ipvs-rs
VPORT=${VPORT:-8080}          # 虚拟服务端口 = tc classid 的次要号来源
RPORT=${RPORT:-9000}          # 真实服务器端口
VIP=10.99.0.1
CLI=10.99.0.2
RS=10.99.1.2
DIR_RS=10.99.1.1

ok()   { printf '  [ok]   %s\n' "$*"; }
bad()  { printf '  [FAIL] %s\n' "$*"; }
warn() { printf '  [warn] %s\n' "$*"; }
step() { printf '\n== %s ==\n' "$*"; }

[ "$(id -u)" -eq 0 ] || { echo "需要 root" >&2; exit 1; }

nsx() { ip netns exec "$@"; }

cleanup() {
    for ns in $NS_DIR $NS_CLI $NS_RS; do ip netns del "$ns" 2>/dev/null; done
}

# ---------------------------------------------------------------------------
cmd_doctor() {
    local fail=0
    step "环境体检"
    command -v ipvsadm >/dev/null 2>&1 && ok "ipvsadm 已安装" || {
        bad "缺 ipvsadm（apt install ipvsadm）"; fail=1; }
    modprobe ip_vs 2>/dev/null
    if [ -e /proc/net/ip_vs ]; then
        ok "内核有 ip_vs（/proc/net/ip_vs 存在）"
    else
        bad "内核没有 ip_vs —— 本机跑不了 IPVS，换一台再验"; fail=1
    fi
    modprobe ip_vs_rr 2>/dev/null
    modprobe ip_vs_wrr 2>/dev/null
    # conn_tab_bits 是**模块加载参数，运行期改不了**（docs/10 §3.3）。
    if [ -r /sys/module/ip_vs/parameters/conn_tab_bits ]; then
        local b; b=$(cat /sys/module/ip_vs/parameters/conn_tab_bits)
        ok "conn_tab_bits=$b（连接哈希桶 $((1<<b)) 个）"
        [ "$b" -lt 20 ] && warn "百万连接建议 20；改它要重载模块（会断连），运行期改不了"
    fi
    if tc qdisc add dev lo root handle 9999: htb 2>/dev/null; then
        ok "内核有 sch_htb（限速要用）"; tc qdisc del dev lo root 2>/dev/null
    else
        bad "内核没有 sch_htb —— 限速本身就跑不起来"; fail=1
    fi
    return $fail
}

setup_topo() {
    cleanup
    ip netns add $NS_DIR; ip netns add $NS_CLI; ip netns add $NS_RS
    # 客户端侧
    ip link add vdc type veth peer name vcd
    ip link set vdc netns $NS_DIR; ip link set vcd netns $NS_CLI
    nsx $NS_DIR ip addr add $VIP/24 dev vdc; nsx $NS_DIR ip link set vdc up
    nsx $NS_CLI ip addr add $CLI/24 dev vcd; nsx $NS_CLI ip link set vcd up
    # 后端侧
    ip link add vdr type veth peer name vrd
    ip link set vdr netns $NS_DIR; ip link set vrd netns $NS_RS
    nsx $NS_DIR ip addr add $DIR_RS/24 dev vdr; nsx $NS_DIR ip link set vdr up
    nsx $NS_RS ip addr add $RS/24 dev vrd; nsx $NS_RS ip link set vrd up
    for ns in $NS_DIR $NS_CLI $NS_RS; do nsx $ns ip link set lo up; done
    # LVS-NAT 的硬约束：**RS 的默认网关必须指向 director**，否则回程绕过
    # director 直接发给客户端，源地址不对，客户端一个 RST 就断（docs/10 §3.3）。
    nsx $NS_RS ip route add default via $DIR_RS
    nsx $NS_CLI ip route add default via $VIP
    nsx $NS_DIR sysctl -qw net.ipv4.ip_forward=1

    # IPVS：虚拟服务 VIP:VPORT → 真实服务器 RS:RPORT，NAT 模式（-m）
    nsx $NS_DIR ipvsadm -A -t $VIP:$VPORT -s rr
    nsx $NS_DIR ipvsadm -a -t $VIP:$VPORT -r $RS:$RPORT -m
}

# 在 director 的**客户端侧**网卡出向建一棵和本项目一模一样的 tc 树。
setup_tc() {
    local minor; minor=$(printf '%x' "$VPORT")     # classid 次要号是十六进制
    nsx $NS_DIR tc qdisc add dev vdc root handle 1: htb default 1
    nsx $NS_DIR tc class add dev vdc parent 1: classid 1:1 htb \
        rate 100000000000bit ceil 100000000000bit burst 8388608 cburst 8388608
    nsx $NS_DIR tc class add dev vdc parent 1: classid "1:$minor" htb \
        rate 100000000bit ceil 100000000bit burst 125000 cburst 125000
    nsx $NS_DIR tc filter add dev vdc protocol ip parent 1: prio 1 u32 \
        match ip sport "$VPORT" 0xffff flowid "1:$minor"
    echo "$minor"
}

cmd_classify() {
    cmd_doctor || { bad "环境不满足，先解决上面的问题"; return 1; }
    step "第 1 步：IPVS-NAT 之后 tc 还能不能按源端口分类"
    setup_topo
    local minor; minor=$(setup_tc)
    echo "  已建：IPVS 服务 $VIP:$VPORT --NAT--> $RS:$RPORT"
    echo "  已建：tc 类 1:$minor（匹配 sport $VPORT）挂在 director 客户端侧网卡"

    # RS 上起一个会回大量数据的服务——要测的是**下行**方向。
    nsx $NS_RS python3 -c "
import socket, threading
srv = socket.socket(); srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
srv.bind(('0.0.0.0', $RPORT)); srv.listen(8)
def handle(c):
    try:
        c.recv(64)
        c.sendall(b'x' * 4_000_000)      # 4 MB 下行
    finally:
        c.close()
while True:
    c, _ = srv.accept(); threading.Thread(target=handle, args=(c,), daemon=True).start()
" &
    local RSPID=$!
    sleep 1

    local before after
    before=$(nsx $NS_DIR tc -s class show dev vdc | \
             awk -v c="1:$minor" '$0 ~ "class htb "c" " {found=1} found && /Sent/ {print $2; exit}')
    # 客户端拉三次
    for _ in 1 2 3; do
        nsx $NS_CLI timeout 20 python3 -c "
import socket
s = socket.create_connection(('$VIP', $VPORT), timeout=10)
s.sendall(b'go'); n = 0
while True:
    b = s.recv(65536)
    if not b: break
    n += len(b)
print('    客户端收到', n, '字节')
s.close()"
    done
    after=$(nsx $NS_DIR tc -s class show dev vdc | \
            awk -v c="1:$minor" '$0 ~ "class htb "c" " {found=1} found && /Sent/ {print $2; exit}')
    kill $RSPID 2>/dev/null

    echo
    echo "  tc 类 1:$minor 的 Sent：$before → $after"
    nsx $NS_DIR ipvsadm -L -n --stats
    local delta=$(( ${after:-0} - ${before:-0} ))
    echo
    if [ "$delta" -gt 1000000 ]; then
        ok "**地基成立**：下行 $delta 字节确实进了 1:$minor 这个类。"
        ok "现有的 tc 规则（按源端口分类）在 IPVS-NAT 下原样有效，不用改。"
        cleanup; return 0
    else
        bad "**地基不成立**：类计数只涨了 $delta 字节。"
        bad "说明反向 NAT 之后 tc 没能按源端口匹配到下行流量 ——"
        bad "整个 IPVS 方案作废（限速将完全失效），别再往下做了。"
        bad "排查：tc -s filter show dev vdc 看 filter 有没有命中；"
        bad "      兜底类 1:1 的 Sent 是不是把这些字节吃掉了。"
        cleanup; return 1
    fi
}

cmd_stats() {
    cmd_doctor >/dev/null 2>&1
    step "第 2 步：/proc/net/ip_vs 的按服务字节计数够不够做监控"
    setup_topo
    nsx $NS_RS python3 -c "
import socket, threading
srv = socket.socket(); srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
srv.bind(('0.0.0.0', $RPORT)); srv.listen(8)
def handle(c):
    try:
        c.recv(64); c.sendall(b'x' * 8_000_000)
    finally:
        c.close()
while True:
    c, _ = srv.accept(); threading.Thread(target=handle, args=(c,), daemon=True).start()
" &
    local RSPID=$!
    sleep 1
    echo "  打流前："; nsx $NS_DIR ipvsadm -L -n --stats | sed 's/^/    /'
    nsx $NS_CLI timeout 30 python3 -c "
import socket
s = socket.create_connection(('$VIP', $VPORT), timeout=10); s.sendall(b'go'); n=0
while True:
    b = s.recv(65536)
    if not b: break
    n += len(b)
print('    客户端收到', n, '字节')"
    echo "  打流后："; nsx $NS_DIR ipvsadm -L -n --stats | sed 's/^/    /'
    echo
    echo "  原始 /proc/net/ip_vs_stats："; sed 's/^/    /' /proc/net/ip_vs_stats
    kill $RSPID 2>/dev/null

    step "要自己判断的三件事"
    cat <<'EOF'
  1. OutBytes 是不是**按服务分开**的？（本项目要按 frontend 出曲线）
  2. 计数器是不是 **64 位**？32 位在万兆上几分钟就绕回，差分会算出负数
     或天文数字 —— 用 `ipvsadm -L -n --stats` 跑满一会儿看会不会回绕，
     或直接看内核是否 CONFIG_IP_VS_64BIT_STATS / ip_vs_stats64。
  3. 口径：IPVS 数的是 **IP 层字节**（含 IP/TCP 头与重传），HAProxy 的
     bytes_out 是**应用层字节**。换过去同一份流量的数字会大 3%~8% ——
     **这会直接影响 95 计费**，切换前必须和商务对齐（docs/10 §3.4）。
EOF
    cleanup
}

case "${1:-}" in
    doctor)   cmd_doctor ;;
    classify) cmd_classify ;;
    stats)    cmd_stats ;;
    clean)    cleanup; echo "已清理 netns" ;;
    *)
        sed -n '2,25p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
        echo
        echo "中途被打断的话手工清理： sudo $0 clean"
        exit 2 ;;
esac
