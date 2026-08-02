#!/usr/bin/env bash
# tools/ipvs_probe.sh —— LVS/IPVS 可行性研究的实测脚本（docs/10 §6 第 1、2 步）。
#
# **只在实验机上跑，不要在生产机上跑**：它会建 network namespace、临时的
# IPVS 服务和 tc 队列树。收尾会清理，但中途被打断就得手工清（末尾有命令）。
#
#   sudo tools/ipvs_probe.sh doctor     环境体检：内核有没有 ip_vs、有没有 htb
#                                       （分得清"没加载"和"根本没编进内核"）
#   sudo tools/ipvs_probe.sh forward    **不需要 ip_vs 也能跑**：验证转发 +
#                                       NAT 之后的回程流量是不是真的经过出口
#                                       网卡的根 qdisc、源端口是不是改写后的。
#                                       整机限速全部的依赖就这一条
#   sudo tools/ipvs_probe.sh classify   IPVS-NAT 反向改写之后，tc 还能不能按
#                                       源端口把下行流量分到对应的类里
#                                       （**只有按 frontend 限速时才需要**）
#   sudo tools/ipvs_probe.sh stats      /proc/net/ip_vs 的按服务字节计数够不够
#                                       做监控（存在？在涨？64 位？）
#   sudo tools/ipvs_probe.sh clean      清理残留
#
# 先跑哪个，取决于本项目的限速范围（limit_scope，见 docs/06 §2）：
#
#   host（默认，整机限速）  跑 forward 就够。整机限速不做任何分类，它唯一的
#                           依赖是"转发出去的包会经过出口网卡的根 qdisc"。
#   frontend（按端口限速）  还要跑 classify。这个范围下 tc 按**源端口**分类，
#                           而下行包的源端口由 IPVS 的反向 NAT 改写回 VIP 的
#                           服务端口，能不能匹配上就成了地基。
#
# 按内核数据路径（NAT 改写 → dev_queue_xmit → 根 qdisc），两条都应该成立；
# forward 子命令用普通 iptables DNAT 把这条路径实测了一遍（见该函数的说明），
# **但 IPVS 自己的钩子没有实测过**，真机上还是要用 classify/stats 复核。
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

# 后端服务端：在 RS 的 netns 里起一个只管往外灌数据的 TCP 服务。
#
# 收尸方式值得说一句。最初用的是 `kill $!`——不行，`ip netns exec` 会 fork，
# $! 是它而不是里面的 python，杀了外壳留下孤儿，孤儿占着 netns，下一次
# `ip netns add` 建出来的名字直接不可用（报 Invalid "netns" value）。
#
# 第二版改成给 python 加个唯一标记再 `pkill -f 标记`——**更糟**：pkill -f
# 匹配的是全命令行，任何命令行里恰好出现该标记的进程都会被杀掉，包括
# 运维自己那条 `grep 标记` 或正在编辑脚本的编辑器。实测时它把跑测试的
# shell 一起杀了，表现为"隔一次失败一次"，查了很久。
#
# 现在按 **netns 精确收尸**：只杀待在我们自己那个一次性 netns 里的进程，
# 不做任何模式匹配，也就不可能误伤。
_rs_serve() {   # $1 = 每个连接回多少字节
    # 外层子 shell + setsid：让它彻底脱离本 shell 的作业表，否则收尸时
    # bash 会往输出里插一行 "Terminated"，夹在体检结论中间像是出错了。
    # （setsid 是外部命令，执行不了 nsx 这个 shell 函数，所以这里直写 ip。）
    ( setsid ip netns exec $NS_RS python3 -c "
import socket, threading
s = socket.socket(); s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind(('0.0.0.0', $RPORT)); s.listen(8)
def h(c):
    try: c.recv(64); c.sendall(b'x' * $1)
    finally: c.close()
while True:
    c, _ = s.accept(); threading.Thread(target=h, args=(c,), daemon=True).start()
" </dev/null >/dev/null 2>&1 & )
}

# 杀掉 RS netns 里的一切，并**等它们真的退出**。不等的话下一步
# `ip netns del` 会撞上还在里面的进程，netns 删不干净。
_rs_stop() {
    local pids i
    pids=$(ip netns pids $NS_RS 2>/dev/null)
    [ -n "$pids" ] || return 0
    kill $pids 2>/dev/null
    for i in 1 2 3 4 5 6 7 8 9 10; do
        pids=$(ip netns pids $NS_RS 2>/dev/null)
        [ -n "$pids" ] || return 0
        sleep 0.2
    done
    kill -9 $pids 2>/dev/null
    sleep 0.3
}

ok()   { printf '  [ok]   %s\n' "$*"; }
bad()  { printf '  [FAIL] %s\n' "$*"; }
warn() { printf '  [warn] %s\n' "$*"; }
step() { printf '\n== %s ==\n' "$*"; }

[ "$(id -u)" -eq 0 ] || { echo "需要 root" >&2; exit 1; }

nsx() { ip netns exec "$@"; }

cleanup() {
    _rs_stop            # 先收尸再删 netns：还有进程在里面时 netns 删不干净
    for ns in $NS_DIR $NS_CLI $NS_RS; do ip netns del "$ns" 2>/dev/null; done
}
# 中途被 Ctrl-C / 超时打断也要清理，否则残留的后端进程会占着 netns，
# 下一次跑直接卡住。
trap cleanup EXIT INT TERM

# ---------------------------------------------------------------------------
cmd_doctor() {
    local fail=0
    step "环境体检"
    command -v ipvsadm >/dev/null 2>&1 && ok "ipvsadm 已安装" || {
        bad "缺 ipvsadm（apt install ipvsadm）"; fail=1; }
    command -v modprobe >/dev/null 2>&1 && modprobe ip_vs 2>/dev/null
    if [ -e /proc/net/ip_vs ]; then
        ok "内核有 ip_vs（/proc/net/ip_vs 存在）"
    else
        bad "内核没有 ip_vs"
        # 这两种情况的处置完全不同，必须分清：模块没装/没加载是 apt 或
        # modprobe 能解决的；内核根本没编进去（尤其是连模块支持都关了的
        # 精简内核）就只能换内核，别在这台机器上耗时间。
        kdiag ip_vs "IPVS"
        fail=1
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
        bad "内核没有 sch_htb —— 限速本身就跑不起来"
        kdiag NET_SCH_HTB "HTB 队列规则"
        fail=1
    fi
    return $fail
}

# 内核缺某个特性时，回答「是没加载，还是根本没编进内核」。
# 有 /proc/config.gz 或 /boot/config-$(uname -r) 时这是**确定的答案**，不用猜。
# 这两种情况的处置完全不同：没加载是 modprobe 能解决的；根本没编进去（尤其
# 是连模块支持都关掉的精简内核）就只能换内核，别在这台机器上耗时间。
#
# 注意消息里用的是全角引号「」——半角引号会提前闭合 bash 的字符串。
kdiag() {
    local sym="$1" what="$2" cfg=""
    # 符号一律大写：调用方写 ip_vs 还是 IP_VS 都能用。
    sym=$(printf '%s' "${sym#CONFIG_}" | tr '[:lower:]' '[:upper:]')
    [ -r /proc/config.gz ] && cfg="zcat /proc/config.gz"
    [ -z "$cfg" ] && [ -r "/boot/config-$(uname -r)" ] && cfg="cat /boot/config-$(uname -r)"
    if [ -z "$cfg" ]; then
        warn "  取不到内核 config（无 /proc/config.gz，也无 /boot/config-$(uname -r)）"
        warn "  → 分不清是「没加载」还是「没编进内核」；先试 modprobe，再考虑换内核"
        return
    fi
    local line
    line=$($cfg 2>/dev/null | grep -E "^(# )?CONFIG_${sym}[ =]" | head -1)
    if [ -z "$line" ]; then
        warn "  内核 config 里没有 CONFIG_${sym} 这一项（内核版本对不上？）"
    elif [ "${line#\# }" != "$line" ]; then      # 以 "# " 开头 = is not set
        bad "  内核 config: $line"
        bad "  → ${what} **根本没编进这个内核**，不是「没加载」。modprobe 也没用，"
        bad "     只能换一个带该特性的内核。"
        # 这里必须用 grep -c 而不是 grep -q：本脚本开了 pipefail，而 grep -q
        # 命中即退出会让上游的 zcat 吃到 SIGPIPE（141），整条管道被判为失败
        # ——明明匹配上了却走不进这个分支。实测过一次。
        local modoff
        modoff=$($cfg 2>/dev/null | grep -c "^# CONFIG_MODULES is not set")
        if [ "${modoff:-0}" -gt 0 ]; then
            bad "  → 而且这个内核**连模块支持都关了**（# CONFIG_MODULES is not set），"
            bad "     任何特性都无法事后加载。容器/虚机用的精简内核常见如此。"
        fi
    else
        local lower; lower=$(printf '%s' "$sym" | tr '[:upper:]' '[:lower:]')
        warn "  内核 config: $line —— 编进去了，那多半只是没加载：modprobe ${lower}"
    fi
}


# 只搭网络，不碰 IPVS —— forward 子命令用它（那台机器上未必有 ip_vs）。
setup_topo_plain() {
    cleanup
    # netns 建不出来就立刻停：接着往下跑只会刷出一屏
    # "Cannot open network namespace"，把真正的原因埋掉。
    for ns in $NS_DIR $NS_CLI $NS_RS; do
        if ! ip netns add "$ns" 2>/dev/null; then
            bad "建不了 network namespace $ns"
            bad "  → 同名的还在（先跑 $0 clean），或本环境不允许建 netns"
            exit 1
        fi
    done
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

    # 客户端 → VIP:VPORT 的连接改写到后端。IPVS 用自己的钩子做同样的事，
    # 这里用 iptables nat 表，好让 forward 子命令在没有 ip_vs 的机器上也能跑。
    nsx $NS_DIR iptables -t nat -A PREROUTING -p tcp -d $VIP --dport $VPORT \
        -j DNAT --to-destination $RS:$RPORT
    nsx $NS_DIR iptables -t nat -A POSTROUTING -p tcp -d $RS --dport $RPORT -j MASQUERADE
}

# 真正的 IPVS 拓扑：网络部分同上，转发换成 IPVS-NAT。
setup_topo() {
    setup_topo_plain
    # 上面那两条 iptables 规则是 forward 子命令用的替身，IPVS 场景下要撤掉，
    # 否则两套 NAT 叠在一起，量出来的东西说明不了任何问题。
    nsx $NS_DIR iptables -t nat -F
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

# 出向抓包统计源端口。tcpdump 未必装，AF_PACKET 直接写更省事；而且
# AF_PACKET 在 dev_queue_xmit 里被喂（dev_queue_xmit_nit），拿到的是与
# qdisc **同一个 skb**——所以这里看到的源端口，就是 u32 分类器会匹配的那个。
_SNIFF_PY='
import socket, struct, sys, collections
s = socket.socket(socket.AF_PACKET, socket.SOCK_RAW, socket.htons(3))
s.bind((sys.argv[1], 0)); s.settimeout(float(sys.argv[2]))
n = collections.Counter(); b = collections.Counter()
try:
    while True:
        pkt, ai = s.recvfrom(65535)
        if ai[2] != 4:                     # 4 = PACKET_OUTGOING，只看出向
            continue
        if len(pkt) < 34 or struct.unpack("!H", pkt[12:14])[0] != 0x0800:
            continue
        ihl = (pkt[14] & 0x0F) * 4
        if pkt[14 + 9] != 6:               # 只看 TCP
            continue
        sp = struct.unpack("!H", pkt[14 + ihl:14 + ihl + 2])[0]
        n[sp] += 1; b[sp] += len(pkt)
except socket.timeout:
    pass
for port, cnt in n.most_common(5):
    print("    源端口 %d: %d 包, %d 字节" % (port, cnt, b[port]))
'

# ---------------------------------------------------------------------------
# forward —— **不需要 ip_vs，也不需要 htb，任何机器上都能跑。**
#
# 验的是整机限速（limit_scope=host）唯一的那条依赖：
#
#   转发 + NAT 之后的回程流量，是不是真的经过**出口网卡的根 qdisc**？
#
# 顺带把按 frontend 限速关心的那半也量了：到了出口网卡时，包上的**源端口**
# 是改写前（后端端口）还是改写后（虚拟服务端口）？
#
# 用的是普通 iptables DNAT 而不是 IPVS——两者在数据路径上做的是同一件事
# （地址端口改写 + 转发，最后都落到 dst_output → dev_queue_xmit → 根 qdisc），
# 但**钩子位置不同**（IPVS 有自己的 netfilter 钩子和连接表）。所以这个子命令
# 的结论是"这条内核路径确实是这样走的"，**不能替代**在真机上用 classify /
# stats 复核 IPVS 本身。
#
# 不需要整形 qdisc：要证明的是"包经过了根 qdisc"，pfifo 的 Sent 计数器就够。
cmd_forward() {
    step "转发 + NAT 之后，回程流量走不走出口网卡的根 qdisc"
    setup_topo_plain
    local qd=pfifo
    if ! nsx $NS_DIR tc qdisc add dev vdc root handle 1: $qd limit 1000 2>/dev/null; then
        bad "连 pfifo 都建不了，本机 tc 不可用"; cleanup; return 1
    fi
    ok "已在 director 客户端侧网卡 vdc 上挂根 qdisc（$qd）"
    echo "  DNAT：客户端连 $VIP:$VPORT → 后端 $RS:$RPORT"

    _rs_serve 32_000_000
    sleep 1

    local before after
    before=$(nsx $NS_DIR tc -s qdisc show dev vdc | awk '/Sent/{print $2; exit}')
    nsx $NS_DIR python3 -c "$_SNIFF_PY" vdc 8 > /tmp/.ipvs_probe_ports.$$ &
    local SNIFF=$!
    sleep 0.5
    nsx $NS_CLI timeout 30 python3 -c "
import socket
s = socket.create_connection(('$VIP', $VPORT), timeout=10); s.sendall(b'go'); n = 0
while True:
    b = s.recv(65536)
    if not b: break
    n += len(b)
print('  客户端实收', n, '字节')"
    wait $SNIFF 2>/dev/null
    after=$(nsx $NS_DIR tc -s qdisc show dev vdc | awk '/Sent/{print $2; exit}')
    _rs_stop

    echo
    echo "  根 qdisc Sent：$before → $after"
    local delta=$(( ${after:-0} - ${before:-0} ))
    step "结论"
    if [ "$delta" -gt 30000000 ]; then
        ok "根 qdisc 记到 $delta 字节 —— 转发 + NAT 的回程流量**确实**经过它。"
        ok "整机限速（limit_scope=host）依赖的就是这一条，成立。"
    else
        bad "根 qdisc 只记到 $delta 字节 —— 回程流量没走这个 qdisc。"
        bad "**整机限速会完全失效**，别再往下做了。"
        rm -f /tmp/.ipvs_probe_ports.$$; cleanup; return 1
    fi
    echo
    echo "  出口网卡上看到的源端口（按 frontend 限速时 u32 要匹配的就是它）："
    cat /tmp/.ipvs_probe_ports.$$ 2>/dev/null
    if grep -q "源端口 $VPORT:" /tmp/.ipvs_probe_ports.$$ 2>/dev/null; then
        ok "是**改写后**的虚拟服务端口 $VPORT，不是后端端口 $RPORT ——"
        ok "按源端口分类在这条路径上同样成立。"
    else
        warn "没看到源端口 $VPORT 的包。可能是抓包窗口太短，也可能改写发生在"
        warn "根 qdisc **之后** —— 后者会让按 frontend 限速失效，值得追。"
    fi
    echo
    warn "注意：这里用的是 iptables DNAT，不是 IPVS。两者数据路径的终点相同，"
    warn "但钩子位置不同 —— IPVS 本身仍须在有 ip_vs 的机器上用 classify 复核。"
    rm -f /tmp/.ipvs_probe_ports.$$
    cleanup
}

cmd_classify() {
    cmd_doctor || { bad "环境不满足，先解决上面的问题"; return 1; }
    step "第 1 步：IPVS-NAT 之后 tc 还能不能按源端口分类"
    setup_topo
    local minor; minor=$(setup_tc)
    echo "  已建：IPVS 服务 $VIP:$VPORT --NAT--> $RS:$RPORT"
    echo "  已建：tc 类 1:$minor（匹配 sport $VPORT）挂在 director 客户端侧网卡"

    # RS 上起一个会回大量数据的服务——要测的是**下行**方向。
    _rs_serve 4_000_000
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
    _rs_stop

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
    _rs_serve 8_000_000
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
    _rs_stop

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
    forward)  cmd_forward ;;
    classify) cmd_classify ;;
    stats)    cmd_stats ;;
    clean)    cleanup; echo "已清理 netns" ;;
    *)
        sed -n '2,25p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
        echo
        echo "中途被打断的话手工清理： sudo $0 clean"
        exit 2 ;;
esac
