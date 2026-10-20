#!/usr/bin/env bash
# deploy/docker/node-entrypoint.sh —— 一台 LVS director 容器的入口：在
# 同一个容器（= 生产上的同一台服务器）里把 keepalived 与 rl-limiter
# 一起拉起来。
#
# 生产上这两个进程由 systemd 各自管理（keepalived.service 与
# rl-limiter.service）。容器里没有 systemd，用这个脚本承担同样的职责：
#   1. 备好运行环境再启动：tc 限速自检 → ip_vs 自检 → NAT 转发
#      sysctl → 内核参数调优。全部在数据面起来**之前**做完；
#   2. 起 keepalived（VRRP 主备漂 VIP + 下发 virtual_server 到内核
#      IPVS 表；主备**都**装表，这是 rl-limiter 主备对称的前提）；
#      RL_DEMO_HA=0 时退化为单机：直接 ipvsadm 建服务、VIP=本机地址；
#   3. 等内核 IPVS 表出现（rl-limiter 的受管清单来源）；
#   4. 起 rl-limiter（-c 指定本机 YAML 配置，只采本机）；
#   5. 任一进程退出就整体退出（对齐 systemd Restart=always 的语义），
#      由 compose 的 restart 策略拉起；转发 SIGTERM/SIGINT 干净收场。
set -euo pipefail

# 带参数时直接执行参数，不走 director 那套。
#
# 本镜像身兼两职：既是"LVS director"（不带参数 = 默认角色），也被
# web / loadgen 等**只借用它的 Python 环境**的服务复用（compose 用
# `command:` 指定要跑的程序，覆盖的是 CMD、不是 ENTRYPOINT）。
if [ "$#" -gt 0 ]; then
    exec "$@"
fi

RL_YAML_TEMPLATE=${RL_YAML_TEMPLATE:-/etc/rl-limiter/config-template.yaml}
RL_YAML=${RL_YAML:-/etc/rl-limiter/config.yaml}
KEEPALIVED_TEMPLATE=${KEEPALIVED_TEMPLATE:-/etc/rl-limiter/keepalived-template.conf}
KEEPALIVED_CONF=/etc/keepalived/keepalived.conf
IPVS_WAIT_S=${IPVS_WAIT_S:-30}

log() { echo "$(date -Is) ENTRYPOINT $*" >&2; }

# rl-limiter 的 YAML 配置：从只读挂载的模板复制成容器内可写文件
# （演示要能进容器改 quotas 看热生效；写 API 的原子替换也需要本机
# 普通文件）。rl-limiter 对它只读轮询，从不改写模板。
if [ -f "$RL_YAML_TEMPLATE" ]; then
    cp "$RL_YAML_TEMPLATE" "$RL_YAML"
    log "已从模板生成 rl-limiter 配置 template=$RL_YAML_TEMPLATE yaml=$RL_YAML"
elif [ ! -f "$RL_YAML" ]; then
    log "致命：找不到 rl-limiter 的 YAML 配置。compose 应把它挂在" \
        "$RL_YAML_TEMPLATE（参考 deploy/docker/limiter-node.yaml）"
    exit 1
fi

# 限速自检：限速由内核 tc（HTB）执行。限速静默失效是本项目最不能接受
# 的故障，所以在真正启动之前把三个前提逐个验掉，缺哪个说哪个。
tc_selfcheck() {
    if ! command -v tc >/dev/null 2>&1; then
        log "致命：找不到 tc（iproute2）。限速由内核 tc 执行，没有它本节点"
        log "致命：**完全不会限速**。镜像多半是旧的，请重建：docker compose up -d --build"
        return 1
    fi
    if ! tc qdisc add dev lo root handle 9999: pfifo >/dev/null 2>&1; then
        log "致命：没有 CAP_NET_ADMIN，无法操作 tc（本节点将不会限速）。"
        log "致命：请在 compose 的本服务下加：cap_add: [\"NET_ADMIN\"]"
        return 1
    fi
    tc qdisc del dev lo root >/dev/null 2>&1 || true
    if ! tc qdisc add dev lo root handle 9999: htb >/dev/null 2>&1; then
        log "致命：内核不支持 HTB 调度器（sch_htb），限速无法工作。"
        log "致命：宿主机上执行 modprobe sch_htb。"
        return 1
    fi
    tc qdisc del dev lo root >/dev/null 2>&1 || true
    log "限速自检通过：tc 可用、有 CAP_NET_ADMIN、内核支持 HTB"
    return 0
}

# ip_vs 自检：与 tc 自检同一铁律——内核没有 ip_vs 模块（容器里
# modprobe 不了，必须宿主机加载）时**现在就失败**，而不是让
# rl-limiter 起来之后读不到受管清单反复报错。
ipvs_selfcheck() {
    if ! command -v ipvsadm >/dev/null 2>&1; then
        log "致命：找不到 ipvsadm。镜像多半是旧的，请重建镜像。"
        return 1
    fi
    if ! ipvsadm -Ln >/dev/null 2>&1; then
        log "致命：内核 IPVS 不可用（ipvsadm -Ln 失败）。"
        log "致命：请在**宿主机**上执行：modprobe ip_vs ip_vs_rr"
        log "致命：（容器里无法 modprobe；Docker Desktop(Mac/Win) 的精简"
        log "致命：虚拟机内核可能根本没有该模块，需换 Linux 主机。）"
        return 1
    fi
    log "IPVS 自检通过：内核 ip_vs 可用"
    return 0
}

# ---- 双网卡：按网段定位客户端侧/后端侧网卡 ------------------------------
# 容器内 eth0/eth1 的先后顺序不可靠，按 IP 网段找才稳。
iface_by_subnet() {
    ip -o -4 addr show scope global 2>/dev/null \
        | awk -v p="$1" 'index($4, p) == 1 { print $2; exit }'
}
addr_by_subnet() {
    ip -o -4 addr show scope global 2>/dev/null \
        | awk -v p="$1" 'index($4, p) == 1 { split($4, a, "/"); print a[1]; exit }'
}

FRONT_IF=""; BACK_IF=""
if [ -n "${RL_FRONT_SUBNET:-}" ]; then
    FRONT_IF=$(iface_by_subnet "$RL_FRONT_SUBNET")
    if [ -z "$FRONT_IF" ]; then
        log "致命：按网段 $RL_FRONT_SUBNET 找不到客户端侧网卡——compose 的"
        log "致命：networks 配置与 RL_FRONT_SUBNET 对不上。容器内地址："
        ip -o -4 addr show scope global 2>/dev/null | while read -r line; do
            log "致命：  $line"
        done
        exit 1
    fi
    # NAT 模式双向都过 director：出向 HTB 就在客户端侧网卡上直接限速。
    export RL_TC_IFACE="${RL_TC_IFACE:-$FRONT_IF}"
    log "双网卡：已按网段 $RL_FRONT_SUBNET 定位客户端侧网卡 iface=$FRONT_IF" \
        "RL_TC_IFACE=$RL_TC_IFACE（- = 出向不做 tc）"
fi
if [ -n "${RL_BACK_SUBNET:-}" ]; then
    BACK_IF=$(iface_by_subnet "$RL_BACK_SUBNET")
    [ -n "$BACK_IF" ] || { log "致命：按网段 $RL_BACK_SUBNET 找不到后端侧网卡"; exit 1; }
fi

if [ "${RL_TC_IFACE:-}" = "-" ]; then
    log "警告：已显式关闭出向 tc 整形（RL_TC_IFACE=-），本节点只做监控"
elif ! tc_selfcheck; then
    exit 1
fi
ipvs_selfcheck || exit 1

# 入向整形（ifb 模式）预检：只有显式接了入向网卡才检（LVS NAT 的主
# 路径是出向 HTB，入向闭环是可选能力）。
if [ -n "${RL_TC_IN_IFACE:-}" ] && [ "${RL_TC_IN_IFACE}" != "-" ] \
        && [ "${RL_TC_IN_MODE:-ifb}" = "ifb" ]; then
    if ip link add __rlifbchk type ifb >/dev/null 2>&1; then
        ip link del __rlifbchk >/dev/null 2>&1 || true
        log "入向整形自检通过：内核支持 ifb"
    else
        log "致命：内核无法创建 ifb 设备（RL_TC_IN_MODE=ifb 依赖 ifb 模块）。"
        log "致命：改设 RL_TC_IN_MODE=police，或换带通用内核的 Linux 主机，"
        log "致命：或设 RL_TC_IN_IFACE=- 关闭入向限速。"
        exit 1
    fi
fi

# NAT 转发前提：转发开不了整个 NAT 不通，写不动就致命——这不是性能
# 调优，是功能开关。vs.conntrack 让 IPVS 连接也进 conntrack（NAT 回程
# 依赖）。net.* 按 netns 隔离，容器内可写（需 CAP_NET_ADMIN）。
set_sysctl() {
    local key=$1 val=$2 path
    path="/proc/sys/${key//./\/}"
    if ! echo "$val" > "$path" 2>/dev/null || [ "$(cat "$path")" != "$val" ]; then
        log "致命：设不了 $key=$val（NAT 转发的功能前提）。确认容器有"
        log "致命：CAP_NET_ADMIN，且运行时没把 /proc/sys 挂成只读。"
        return 1
    fi
}
set_sysctl net.ipv4.ip_forward 1 || exit 1
set_sysctl net.ipv4.vs.conntrack 1 || exit 1
log "NAT 转发 sysctl 已就位 ip_forward=1 vs.conntrack=1"

# 内核参数调优（性能项，设不全不阻断——详见 docs/08）。
TUNE_KERNEL=${TUNE_KERNEL:-/usr/local/bin/tune-kernel.sh}
if [ "${RL_TUNE_KERNEL:-1}" = "0" ]; then
    log "已跳过内核参数调优（RL_TUNE_KERNEL=0）"
elif [ -x "$TUNE_KERNEL" ]; then
    "$TUNE_KERNEL" apply || true
else
    log "警告：找不到 $TUNE_KERNEL，跳过内核参数调优（镜像多半是旧的）"
fi

KEEPALIVED_PID=""
if [ "${RL_DEMO_HA:-1}" = "1" ] && [ -f "$KEEPALIVED_TEMPLATE" ]; then
    # 主备形态：keepalived 管 VRRP（漂 VIP）+ virtual_server（下发
    # IPVS 表，**主备都装**——这是 rl-limiter 主备对称的前提）。
    # 模板里的网卡名占位符按本机实际网卡替换（容器内名字不可靠）。
    [ -n "$FRONT_IF" ] || { log "致命：HA 形态需要 RL_FRONT_SUBNET 定位客户端侧网卡"; exit 1; }
    [ -n "$BACK_IF" ] || { log "致命：HA 形态需要 RL_BACK_SUBNET 定位后端侧网卡"; exit 1; }
    sed -e "s/@FRONT_IF@/$FRONT_IF/g" \
        -e "s/@BACK_IF@/$BACK_IF/g" \
        -e "s/@STATE@/${KEEPALIVED_STATE:-BACKUP}/g" \
        -e "s/@PRIORITY@/${KEEPALIVED_PRIORITY:-100}/g" \
        "$KEEPALIVED_TEMPLATE" > "$KEEPALIVED_CONF"
    log "启动 keepalived state=${KEEPALIVED_STATE:-BACKUP}" \
        "priority=${KEEPALIVED_PRIORITY:-100} front=$FRONT_IF back=$BACK_IF"
    # -n 前台、-l 日志到控制台、-D 详细。
    keepalived -n -l -D -f "$KEEPALIVED_CONF" &
    KEEPALIVED_PID=$!
else
    # 单机退化形态（RL_DEMO_HA=0）：直接 ipvsadm 建服务，VIP=本机
    # 客户端侧地址。LVS_SERVICE/LVS_BACKENDS 由 compose 提供。
    vip=${LVS_VIP:-$(addr_by_subnet "${RL_FRONT_SUBNET:-}")}
    port=${LVS_PORT:-8080}
    [ -n "$vip" ] || { log "致命：单机形态定不出 VIP（设 LVS_VIP 或 RL_FRONT_SUBNET）"; exit 1; }
    ipvsadm -C
    ipvsadm -A -t "$vip:$port" -s rr
    for be in ${LVS_BACKENDS:-}; do
        # be 形如 名字:端口 或 IP:端口；名字经 getent 解析。
        host=${be%:*}; bport=${be##*:}
        beip=$(getent hosts "$host" | awk '{print $1; exit}')
        [ -n "$beip" ] || beip=$host
        ipvsadm -a -t "$vip:$port" -r "$beip:$bport" -m
        log "已登记后端 $beip:$bport（NAT）"
    done
    log "单机形态：IPVS 服务已建 vip=$vip:$port"
fi

# 等内核 IPVS 表出现至少一个服务（keepalived 下发有先后）：rl-limiter
# 表为空也能起（warn），但演示体验上等一等更干净；等不到也不失败。
waited=0
while [ "$waited" -lt "$IPVS_WAIT_S" ]; do
    if ipvsadm -Ln 2>/dev/null | grep -qE '^(TCP|UDP)'; then
        log "内核 IPVS 表已就绪（等待 ${waited}s）"
        break
    fi
    if [ -n "$KEEPALIVED_PID" ] && ! kill -0 "$KEEPALIVED_PID" 2>/dev/null; then
        log "keepalived 在下发 virtual_server 之前就退出了，放弃启动"
        exit 1
    fi
    sleep 1
    waited=$((waited + 1))
done

log "启动 rl-limiter（同机模式）yaml=$RL_YAML"
rl-limiter -c "$RL_YAML" &
RL_PID=$!

terminate() {
    log "转发停止信号给 keepalived 与 rl-limiter"
    kill -TERM "$RL_PID" ${KEEPALIVED_PID:+$KEEPALIVED_PID} 2>/dev/null || true
}
trap terminate TERM INT

# 任一子进程退出即整体退出（半残状态不如重启）。
status=0
if [ -n "$KEEPALIVED_PID" ]; then
    wait -n "$KEEPALIVED_PID" "$RL_PID" || status=$?
else
    wait "$RL_PID" || status=$?
fi
log "子进程退出或收到信号，容器整体退出交由 compose restart 拉起 exit_status=$status"
terminate
[ -n "$KEEPALIVED_PID" ] && wait "$KEEPALIVED_PID" 2>/dev/null || true
wait "$RL_PID" 2>/dev/null || true
exit "$status"
