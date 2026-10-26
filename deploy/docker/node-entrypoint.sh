#!/usr/bin/env bash
# deploy/docker/node-entrypoint.sh —— 一台节点容器的入口（通用形态）。
#
# 本分支不依赖任何具体服务：节点容器 = rl-limiter + （可选的）一个
# 业务进程示例。演示里业务进程是 tools/random_web.py（随机响应大小的
# HTTP 服务），生产上它可以是 nginx、自研 TCP 服务……或者什么都不是
# ——rl-limiter 做的只是双网卡监控 + 网卡口径限速，与上面跑什么无关。
#
# 启动顺序：
#   1. 备好运行环境：tc 限速自检 → 双网卡定位（按网段）→ 内核参数调优；
#   2. 起业务进程（APP_CMD 非空时；演示 = random_web）；
#   3. 起 rl-limiter（-c 指定本机 YAML 配置）；
#   4. 任一进程退出就整体退出（对齐 systemd Restart=always 语义），由
#      compose 的 restart 策略拉起；SIGTERM/SIGINT 转发给子进程。
set -euo pipefail

# 带参数时直接执行参数，不走节点那套——本镜像身兼两职：既是节点
# （默认角色），也被 web / loadgen / fleet 等只借用 Python 环境的服务
# 复用（compose 的 command: 覆盖 CMD，不覆盖 ENTRYPOINT）。
if [ "$#" -gt 0 ]; then
    exec "$@"
fi

RL_YAML_TEMPLATE=${RL_YAML_TEMPLATE:-/etc/rl-limiter/config-template.yaml}
RL_YAML=${RL_YAML:-/etc/rl-limiter/config.yaml}
# 业务进程命令（空 = 不起业务进程，纯限速/监控节点）。
APP_CMD=${APP_CMD:-}

log() { echo "$(date -Is) ENTRYPOINT $*" >&2; }

# 只读挂进来的 YAML 模板 → 复制成容器内可写的真实配置（演示玩法是
# "进容器改 YAML 看热生效"，只读单文件 bind mount 做不到）。
if [ -f "$RL_YAML_TEMPLATE" ]; then
    cp "$RL_YAML_TEMPLATE" "$RL_YAML"
    log "已从模板生成 rl-limiter 配置 template=$RL_YAML_TEMPLATE yaml=$RL_YAML"
elif [ ! -f "$RL_YAML" ]; then
    log "致命：找不到 rl-limiter 的 YAML 配置。compose 应把它挂在" \
        "$RL_YAML_TEMPLATE（参考 deploy/docker/limiter-node.yaml）"
    exit 1
fi

# ---- tc 自检：能建 HTB 才谈得上限速 --------------------------------------
tc_selfcheck() {
    local dev=$1
    tc qdisc replace dev "$dev" root handle 1: htb default 30 2>/dev/null \
        || return 1
    tc qdisc del dev "$dev" root 2>/dev/null || true
    return 0
}

# ---- 双网卡：按网段定位客户端侧网卡 --------------------------------------
# 容器里网卡名不稳定（eth0/eth1 看接入顺序），写死网卡名不稳——按 IP
# 网段找：RL_FRONT_SUBNET 给出客户端侧网段前缀（如 10.210.1.）。
iface_by_subnet() {
    ip -o -4 addr show | awk -v pre="$1" \
        '$4 ~ "^"pre {print $2; exit}'
}

if [ -n "${RL_FRONT_SUBNET:-}" ]; then
    front_if=$(iface_by_subnet "$RL_FRONT_SUBNET")
    if [ -z "$front_if" ]; then
        log "致命：按网段 $RL_FRONT_SUBNET 找不到客户端侧网卡——compose 的"
        log "networks 配置与 RL_FRONT_SUBNET 对不上。容器内地址："
        ip -o -4 addr show | awk '{print "  " $2 " " $4}' >&2
        exit 1
    fi
    export RL_TC_IFACE="${RL_TC_IFACE:--}"
    export RL_TC_IN_IFACE="${RL_TC_IN_IFACE:-$front_if}"
    export RL_NIC="${RL_NIC:-$front_if}"
    log "双网卡：已按网段 $RL_FRONT_SUBNET 定位客户端侧网卡 iface=$front_if" \
        "RL_TC_IFACE=$RL_TC_IFACE RL_TC_IN_IFACE=$RL_TC_IN_IFACE" \
        "（- = 该方向不做 tc）"
    if [ "$RL_TC_IN_IFACE" != "-" ] && ! tc_selfcheck "$RL_TC_IN_IFACE"; then
        log "致命：tc 自检失败（dev=$RL_TC_IN_IFACE）。容器需要" \
            "cap_add: [NET_ADMIN]，内核需要 sch_htb。"
        exit 1
    fi
elif [ "${RL_TC_IFACE:--}" != "-" ] && ! tc_selfcheck "$RL_TC_IFACE"; then
    log "致命：tc 自检失败（dev=$RL_TC_IFACE）。容器需要 cap_add:" \
        "[NET_ADMIN]，内核需要 sch_htb。"
    exit 1
fi

# ---- 内核参数调优（高并发前提，netns 内生效） ----------------------------
TUNE_KERNEL=${TUNE_KERNEL:-/usr/local/bin/tune-kernel.sh}
if [ -x "$TUNE_KERNEL" ]; then
    "$TUNE_KERNEL" apply || log "警告：tune-kernel 部分参数未能应用" \
        "（容器内部分 sysctl 改不动属预期，详见 docs/08）"
fi

# ---- 启动编排 -------------------------------------------------------------
APP_PID=""
if [ -n "$APP_CMD" ]; then
    log "启动业务进程（演示示例；生产上可以是任何服务） cmd=$APP_CMD"
    bash -c "exec $APP_CMD" &
    APP_PID=$!
fi

log "启动 rl-limiter yaml=$RL_YAML"
rl-limiter -c "$RL_YAML" &
RL_PID=$!

term() {
    [ -n "$APP_PID" ] && kill -TERM "$APP_PID" 2>/dev/null || true
    kill -TERM "$RL_PID" 2>/dev/null || true
}
trap term TERM INT

status=0
if [ -n "$APP_PID" ]; then
    wait -n "$APP_PID" "$RL_PID" || status=$?
else
    wait -n "$RL_PID" || status=$?
fi
log "子进程退出（status=$status），整体退出交给 restart 策略"
term
[ -n "$APP_PID" ] && wait "$APP_PID" 2>/dev/null || true
wait "$RL_PID" 2>/dev/null || true
exit "$status"
