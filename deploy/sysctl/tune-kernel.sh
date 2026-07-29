#!/usr/bin/env bash
# deploy/sysctl/tune-kernel.sh —— 高并发 / 高带宽下的内核参数调优与体检。
#
# 解决的问题：HAProxy 自己的 maxconn / backlog 配得再大，**内核的默认值
# 会在下面把它悄悄削掉**。最典型的一条：本项目 defaults 里写了
# `backlog 65536`，而 listen(2) 的实际队列长度被 net.core.somaxconn 封顶，
# 发行版默认 4096——配置写着 65536、实际生效 4096，没有任何告警。突发
# 重连（reload、上游重试）时溢出的 SYN 被内核直接丢掉，客户端看到的是
# "偶发连接超时"，HAProxy 的日志里干干净净。
#
# 用法：
#   tune-kernel.sh apply   在当前环境尽力调优，逐项读回校验（默认）
#   tune-kernel.sh check   只体检不修改；有未达标项时退出码为 1
#   tune-kernel.sh dump    输出 sysctl.d 格式，供宿主机落盘：
#                            tune-kernel.sh dump > /etc/sysctl.d/99-rl-limiter.conf
#                            sysctl --system
#
# ---------------------------------------------------------------------------
# 为什么"写完必须读回"——这是本脚本最重要的一条实现约束
# ---------------------------------------------------------------------------
# 容器（独立 network namespace）里，一部分 sysctl 是只读的。此时：
#
#   $ sysctl -w net.core.rmem_max=16777216
#   sysctl: setting key "net.core.rmem_max": Operation not permitted
#   $ echo $?
#   0                        <-- 报了错，退出码却是 0
#
# 也就是说 `sysctl -w ... || echo 失败` 这种写法**永远不会报失败**。所以
# 本脚本一律直接写 /proc/sys 并**读回比对**，只认最终生效值。
# （实测环境：Linux 6.18.5 + procps 的 sysctl。）
#
# ---------------------------------------------------------------------------
# 三类参数：能在容器里设的、只能在宿主机设的、netns 里根本不存在的
# ---------------------------------------------------------------------------
# 在 netns 里逐个实测 /proc/sys 的权限位得出（Linux 6.18.5）：
#
#   可写(644)  net.core.somaxconn、net.ipv4.tcp_*（syn_backlog、tw_reuse、
#              tw_buckets、fin_timeout、slow_start_after_idle、rmem、wmem、
#              ip_local_port_range）
#   只读(444)  net.core.rmem_max / wmem_max、net.netfilter.nf_conntrack_max
#   不存在     net.core.netdev_max_backlog / netdev_budget、net.ipv4.tcp_mem
#              （以及 fs.* —— 那本就不是网络命名空间的东西）
#
# 后两类**在容器里无论如何都调不动**，只能在宿主机上设。本脚本不会假装
# 设置成功，而是把它们单独列出来并给出宿主机上该执行的命令。
#
# 顺带一提：这也是本项目**不用 compose 的 `sysctls:`** 的原因——那里只能
# 写第一类，写错一个容器就直接起不来；而且三台 node 要各写一遍。放在初始
# 化脚本里则是同一份表，docker run / k8s / 裸机 systemd 都能用。

set -uo pipefail

# ---------------------------------------------------------------------------
# 参数表：key|mode|value|说明
#
# mode：
#   set      强制设为该值（当前值"更大"不代表"更好"的场合）
#   min      单值下限，当前值更大就不动
#   minlist  空格分隔的多字段，**逐字段**取较大者
#
# 表里的值都按"32 核、TCP L4 代理、maxconn/maxpipes 各 1000000"这条基线
# 定的（默认值不该成为限制，要限并发请显式配 frontend 级 maxconn）。
# 改 maxconn 时需要连带复核的是 fs.nr_open、nf_conntrack_max 与 somaxconn。
# ---------------------------------------------------------------------------
read -r -d '' TUNABLES <<'EOF'
net.core.somaxconn|min|65536|accept 队列上限。listen(2) 的 backlog 被它封顶，本项目 cfg 里写的 backlog 65536 只有它够大才真的生效；发行版默认 4096
net.ipv4.tcp_max_syn_backlog|min|65536|半连接(SYN)队列。默认 1024，突发建连时溢出即丢 SYN
net.ipv4.tcp_tw_reuse|set|1|复用 TIME_WAIT 给新的出向连接。默认 2 = 只对回环生效，对"代理到后端"这条路径等于没开
net.ipv4.tcp_max_tw_buckets|min|2097152|TIME_WAIT 上限。默认 65536，高连接周转下会刷 "time wait bucket table overflow"；按 100 万连接基线给两倍余量
net.ipv4.tcp_fin_timeout|set|15|FIN_WAIT_2 回收时长。默认 60 秒，压着临时端口不放
net.ipv4.ip_local_port_range|minlist|32768 65535|临时端口。**只抬上界不下探**：tc 按源端口分类，下界降到 1024 会让到后端的临时端口撞上监听端口，把回程流量算进别人的限速类（见 tcshaper.ephemeral_conflicts）
net.ipv4.tcp_slow_start_after_idle|set|0|默认 1：连接空闲一个 RTO 后 cwnd 被打回初始值。本项目大量长连接是"空闲一阵再猛传"，留着它等于每次都重新慢启动
net.ipv4.tcp_rmem|minlist|4096 131072 16777216|接收缓冲自动调优上限
net.ipv4.tcp_wmem|minlist|4096 65536 16777216|发送缓冲自动调优上限。代理是往客户端发数据的一方，这一项比 rmem 更吃紧；发行版默认上限常见 4 MiB
net.core.rmem_max|min|16777216|SO_RCVBUF 显式设置的上限。本项目刻意不写 tune.rcvbuf（留给内核自动调优），所以它只在有人手动写死缓冲时才生效——留着是为了那时不被悄悄削掉
net.core.wmem_max|min|16777216|同上，对应 SO_SNDBUF
net.core.netdev_max_backlog|min|250000|软中断收包队列（每 CPU）。默认 1000，万兆以上最先在这里丢包；丢没丢看 /proc/net/softnet_stat 第 2 列
net.core.netdev_budget|min|600|单次软中断轮询的收包预算，默认 300
net.netfilter.nf_conntrack_max|min|4194304|**容器部署特别容易踩**：Docker 装 iptables 规则会把 conntrack 拉起来，于是每条连接都被跟踪。maxconn 100 万意味着前后各 100 万条再加 TIME_WAIT，默认 262144 直接撑爆，内核开始静默丢包并打 "nf_conntrack: table full"。别忘了 nf_conntrack_buckets 一般取 max/4
fs.nr_open|min|4194304|单进程 RLIMIT_NOFILE 的硬天花板。maxconn/maxpipes 各 100 万需要 4000034 个 fd，而默认只有 1048576——**不抬它 HAProxy 根本起不来**，且它在容器里改不动（compose 演示因此用 HAPROXY_MAXCONN 降到 10 万）
EOF

MODE=${1:-apply}
case "$MODE" in
    apply|check|dump) ;;
    *) echo "用法: $0 [apply|check|dump]" >&2; exit 2 ;;
esac

log() { echo "$(date -Is) SYSCTL $*" >&2; }

path_of() { echo "/proc/sys/$(echo "$1" | tr . /)"; }

# /proc/sys 里多字段值用制表符分隔，统一成单空格好比较。
norm() { tr '\t' ' ' | tr -s ' ' | sed 's/^ *//; s/ *$//'; }

# 逐字段取较大者；字段数以期望值为准。
list_max() {
    awk -v cur="$1" -v want="$2" 'BEGIN {
        nc = split(cur, c, " "); nw = split(want, w, " ")
        for (i = 1; i <= nw; i++) {
            v = (i <= nc && c[i] + 0 > w[i] + 0) ? c[i] : w[i]
            out = out (i > 1 ? " " : "") v
        }
        print out
    }'
}

# 按 mode 算出该写的目标值（可能与期望值不同：min/minlist 会保留更大的现值）。
target_of() {
    local mode=$1 cur=$2 want=$3
    case "$mode" in
        set) echo "$want" ;;
        min) if [ "${cur%% *}" -ge "$want" ] 2>/dev/null; then echo "$cur"; else echo "$want"; fi ;;
        minlist) list_max "$cur" "$want" ;;
    esac
}

if [ "$MODE" = dump ]; then
    echo "# /etc/sysctl.d/99-rl-limiter.conf"
    echo "# 由 deploy/sysctl/tune-kernel.sh dump 生成。落盘后执行 sysctl --system 生效。"
    echo "# 面向：32 核、TCP L4 代理、HAProxy maxconn/maxpipes 各 1000000。"
    echo "# 注意：这些值是**下限**，本机若已有更大的值请不要照抄压低。"
    echo
    while IFS='|' read -r key mode want why; do
        [ -z "${key:-}" ] && continue
        echo "# $why"
        echo "$key = $want"
    done <<<"$TUNABLES"
    exit 0
fi

ok=0; raised=0; blocked=0
blocked_keys=""

while IFS='|' read -r key mode want why; do
    [ -z "${key:-}" ] && continue
    p=$(path_of "$key")

    # netns 里不存在 = 这项根本不是命名空间化的，只有宿主机全局一份。
    if [ ! -e "$p" ]; then
        blocked=$((blocked + 1)); blocked_keys="$blocked_keys $key=$want"
        log "调不动 $key（本环境不存在该 sysctl；它不随 network namespace 隔离，只能在宿主机设）"
        continue
    fi

    cur=$(norm <"$p")
    tgt=$(target_of "$mode" "$cur" "$want")

    if [ "$cur" = "$tgt" ]; then
        ok=$((ok + 1))
        continue
    fi

    if [ "$MODE" = check ]; then
        blocked=$((blocked + 1)); blocked_keys="$blocked_keys $key=$want"
        log "未达标 $key 当前=$cur 期望=$tgt"
        continue
    fi

    # 写完必须读回：只读的 sysctl 上 `sysctl -w` 会报错却仍然退出 0（见文件头）。
    # 重定向失败是 shell 自己报的，printf 的 2>/dev/null 拦不住，要整体包起来，
    # 否则只读项会同时刷一行裸 "Permission denied" 和下面那行带解释的日志。
    { printf '%s' "$tgt" >"$p"; } 2>/dev/null
    got=$(norm <"$p")
    if [ "$got" = "$tgt" ]; then
        raised=$((raised + 1))
        log "已调整 $key $cur -> $got"
    else
        blocked=$((blocked + 1)); blocked_keys="$blocked_keys $key=$want"
        log "调不动 $key 当前=$cur 期望=$tgt（本环境只读）"
    fi
done <<<"$TUNABLES"

log "内核参数：已达标 $ok 项、本次调整 $raised 项、无法在此设置 $blocked 项"

if [ "$blocked" -gt 0 ]; then
    log "上面这些**必须在宿主机上**设置（容器内的 network namespace 对它们只读，"
    log "或者它们根本不随 namespace 隔离）。宿主机执行："
    for kv in $blocked_keys; do
        log "    sysctl -w ${kv%%=*}=\"${kv#*=}\""
    done
    log "要持久化：把 $0 dump 的输出放进 /etc/sysctl.d/99-rl-limiter.conf 再 sysctl --system"
    log "不设也能跑，只是高并发/高带宽下会先撞上内核默认值而不是撞上 HAProxy 的上限。"
    [ "$MODE" = check ] && exit 1
fi

exit 0
