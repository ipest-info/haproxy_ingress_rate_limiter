#!/usr/bin/env bash
# deploy/docker/node-entrypoint.sh —— 一台节点容器的入口：在同一个容器
# （= 生产上的同一台服务器）里把 HAProxy 与 rl-limiter 一起拉起来。
#
# 生产上这两个进程由 systemd 各自管理（haproxy.service 与
# rl-limiter.service，后者 After=haproxy.service，见
# deploy/systemd/rl-limiter.service）。容器里没有 systemd，用这个脚本
# 承担同样的职责：
#   1. 先起 HAProxy，等它把 unix stats socket 建出来——rl-limiter 启动
#      即采样，socket 还没出现会白白刷一轮采样失败告警；
#   2. 再起 rl-limiter（RL_NODE_NAME 指定本机节点名，只采本机）；
#   3. 任一进程退出就整体退出（对齐 systemd Restart=always 的语义：
#      带着半残状态继续跑比重启更危险），由 compose 的 restart 策略拉起；
#   4. 转发 SIGTERM/SIGINT 给两个子进程，docker stop 能干净收场。
set -euo pipefail

HAPROXY_CFG=${HAPROXY_CFG:-/usr/local/etc/haproxy/haproxy.cfg}
HAPROXY_SOCK=${HAPROXY_SOCK:-/run/haproxy/admin.sock}
SOCK_WAIT_S=${SOCK_WAIT_S:-30}

log() { echo "$(date -Is) ENTRYPOINT $*" >&2; }

mkdir -p "$(dirname "$HAPROXY_SOCK")"

# -W: master-worker（与生产的 systemd 形态一致，reload 走 SIGUSR2）
# -db: 不后台化，让 master 进程留在前台受本脚本管理
log "启动 HAProxy cfg=$HAPROXY_CFG"
haproxy -W -db -f "$HAPROXY_CFG" &
HAPROXY_PID=$!

# 等 stats socket 就绪：这是 rl-limiter 的采样入口，没它启动就是白转。
waited=0
while [ ! -S "$HAPROXY_SOCK" ]; do
    if ! kill -0 "$HAPROXY_PID" 2>/dev/null; then
        log "HAProxy 在建立 stats socket 之前就退出了，放弃启动"
        wait "$HAPROXY_PID" || true
        exit 1
    fi
    if [ "$waited" -ge "$SOCK_WAIT_S" ]; then
        log "等待 HAProxy stats socket 超时 sock=$HAPROXY_SOCK timeout_s=$SOCK_WAIT_S"
        kill "$HAPROXY_PID" 2>/dev/null || true
        exit 1
    fi
    sleep 1
    waited=$((waited + 1))
done
log "HAProxy stats socket 已就绪 sock=$HAPROXY_SOCK waited_s=$waited"

log "启动 rl-limiter（同机模式）node=${RL_NODE_NAME:-<未设置>}"
rl-limiter &
RL_PID=$!

terminate() {
    log "转发停止信号给 HAProxy 与 rl-limiter"
    kill -TERM "$RL_PID" "$HAPROXY_PID" 2>/dev/null || true
}
trap terminate TERM INT

# 任一子进程退出即整体退出（半残状态不如重启）。
# `|| status=$?` 不可省：set -e 下，子进程非零退出或信号打断 wait 都会
# 让脚本在这一行直接中止，收不到状态、也跑不到下面的收尾。
status=0
wait -n "$HAPROXY_PID" "$RL_PID" || status=$?
log "子进程退出或收到信号，容器整体退出交由 compose restart 拉起 exit_status=$status"
terminate
# 等两个子进程真正收尾；它们的退出码在这里无意义，用首个退出者的状态。
wait "$HAPROXY_PID" 2>/dev/null || true
wait "$RL_PID" 2>/dev/null || true
exit "$status"
