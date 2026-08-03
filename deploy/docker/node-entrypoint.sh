#!/usr/bin/env bash
# deploy/docker/node-entrypoint.sh —— 一台节点容器的入口：在同一个容器
# （= 生产上的同一台服务器）里把 HAProxy 与 rl-limiter 一起拉起来。
#
# 生产上这两个进程由 systemd 各自管理（haproxy.service 与
# rl-limiter.service，后者 After=haproxy.service，见
# deploy/systemd/rl-limiter.service）。容器里没有 systemd，用这个脚本
# 承担同样的职责：
#   1. 备好运行环境再启动：tc 限速自检 → 内核参数调优 → FD 预检。
#      这三项都必须在 HAProxy 起来**之前**做完——内核参数改晚了对已经
#      建好的监听套接字不生效（backlog 在 listen() 那一刻就定死了），
#      FD 不够则 HAProxy 根本起不来；
#   2. 起 HAProxy，等它把 unix stats socket 建出来——rl-limiter 启动
#      即采样，socket 还没出现会白白刷一轮采样失败告警；
#   3. 再起 rl-limiter（-c 指定本机 YAML 配置，只采本机）；
#   4. 任一进程退出就整体退出（对齐 systemd Restart=always 的语义：
#      带着半残状态继续跑比重启更危险），由 compose 的 restart 策略拉起；
#   5. 转发 SIGTERM/SIGINT 给两个子进程，docker stop 能干净收场。
set -euo pipefail

# 带参数时直接执行参数，不走 HAProxy 节点那套。
#
# 本镜像身兼两职：既是"HAProxy 节点"（不带参数 = 默认角色），也被 web /
# loadgen 等**只借用它的 Python 环境**的服务复用。后者在 compose 里用
# `command:` 指定要跑的程序，而 `command:` 覆盖的是 CMD、**不是
# ENTRYPOINT**——没有这个分支，它们会连同 haproxy 一起被拉起来，且因为
# 没有 cfg/YAML 模板而反复失败重启，症状是
#   rl-limiter: [Errno 2] No such file or directory: '/etc/rl-limiter/config.yaml'
# 而真正该跑的 random_web.py / loadgen.py 一次都没执行。
if [ "$#" -gt 0 ]; then
    exec "$@"
fi

# 只读挂进来的配置模板 → 复制成容器内可写的真实配置。
# 为什么要复制：演示的玩法就是"进容器改 cfg / 改 YAML 看热生效"，而
# compose 的单文件 bind mount 是只读的、也无法被编辑器的原子替换覆盖
# （rename 跨挂载点会失败，sed -i 会换 inode 让容器看到旧文件）。复制
# 一份到容器自己的文件系统后，形态就和生产上"都是本机普通文件"一致了。
# rl-limiter 对两份文件都**只读**（轮询内容做热更新），从不改写它们。
HAPROXY_TEMPLATE=${HAPROXY_TEMPLATE:-/etc/rl-limiter/haproxy-template.cfg}
HAPROXY_CFG=${HAPROXY_CFG:-/etc/haproxy/haproxy.cfg}
RL_YAML_TEMPLATE=${RL_YAML_TEMPLATE:-/etc/rl-limiter/config-template.yaml}
RL_YAML=${RL_YAML:-/etc/rl-limiter/config.yaml}
HAPROXY_SOCK=${HAPROXY_SOCK:-/run/haproxy/admin.sock}
HAPROXY_PIDFILE=${HAPROXY_PIDFILE:-/run/haproxy/master.pid}
SOCK_WAIT_S=${SOCK_WAIT_S:-30}

log() { echo "$(date -Is) ENTRYPOINT $*" >&2; }

mkdir -p "$(dirname "$HAPROXY_SOCK")" "$(dirname "$HAPROXY_CFG")"

# 旧版（v3.1 首版）把 cfg 直接挂在这里。留一条兼容路径：升级过程中
# "新镜像 + 旧 compose" 的组合不至于起不来。
LEGACY_CFG=/usr/local/etc/haproxy/haproxy.cfg

# 配置来源判定必须发生在启动 HAProxy **之前**：让 haproxy 自己去撞
# "文件不存在"再退出，暴露给运维的是一条含糊的"HAProxy 提前退出了"，
# 真正原因（挂载点对不上）被埋在上一行 ALERT 里，且整个容器会无限
# 重启刷屏。这里提前判定并直接说清楚。
if [ -f "$HAPROXY_TEMPLATE" ]; then
    cp "$HAPROXY_TEMPLATE" "$HAPROXY_CFG"
    log "已从模板生成可写配置 template=$HAPROXY_TEMPLATE cfg=$HAPROXY_CFG"
elif [ -f "$LEGACY_CFG" ] && [ "$LEGACY_CFG" != "$HAPROXY_CFG" ]; then
    cp "$LEGACY_CFG" "$HAPROXY_CFG"
    log "警告：用的是旧版挂载点 $LEGACY_CFG。请把 compose 里的挂载改成" \
        "$HAPROXY_TEMPLATE（复制成本机普通文件后才能进容器改配置演示" \
        "热生效，只读单文件 bind mount 做不到）"
elif [ -f "$HAPROXY_CFG" ]; then
    # 注意：Ubuntu 的 haproxy 包**自带**一份 /etc/haproxy/haproxy.cfg，
    # 所以这条分支很容易在"忘了挂模板"时静默命中——haproxy 会正常起来、
    # stats socket 也有（发行版默认配置里就有那一行），看着一切正常，
    # 实际却没有演示的监听端口，rl-limiter 也解析不到任何 frontend。
    log "未挂载模板，沿用镜像内已有的配置 cfg=$HAPROXY_CFG" \
        "（若非有意为之，请检查 compose 是否挂了 $HAPROXY_TEMPLATE）"
else
    log "致命：找不到 HAProxy 配置。已依次查找："
    log "  模板（compose 应挂在这里）: $HAPROXY_TEMPLATE"
    log "  旧版挂载点               : $LEGACY_CFG"
    log "  容器内配置               : $HAPROXY_CFG"
    log "最常见的原因是**镜像与 compose 版本不匹配**（compose 已更新、" \
        "镜像还是旧的）。请重建镜像：docker compose up -d --build"
    exit 1
fi

# rl-limiter 的 YAML 配置：同样从模板复制（理由同上——演示要能进容器
# 改 quotas 看热生效）。没挂模板但容器里已有一份也放行（自定义镜像）。
if [ -f "$RL_YAML_TEMPLATE" ]; then
    cp "$RL_YAML_TEMPLATE" "$RL_YAML"
    log "已从模板生成 rl-limiter 配置 template=$RL_YAML_TEMPLATE yaml=$RL_YAML"
elif [ ! -f "$RL_YAML" ]; then
    log "致命：找不到 rl-limiter 的 YAML 配置。compose 应把它挂在" \
        "$RL_YAML_TEMPLATE（参考 deploy/docker/limiter-node.yaml）"
    exit 1
fi

# 可选：就地覆盖 global 里的 maxconn / maxpipes。
#
# 为什么需要它：模板里的默认值按"**默认不该成为限制**"给到 100 万连接，
# 对应 400 万 fd，而宿主机 `fs.nr_open` 默认只有 1048576（容器里改不动）。
# 生产机器该按这个默认值来，但 compose 演示往往跑在开发机上，起不来。
# 于是演示环境用这两个变量把量级降下来——**降的是演示，不是默认值**，
# 模板本身仍然是那份可以直接抄去生产的骨架。
#
# 只改 global 段里的这两条指令，别的一个字节不动。
override_directive() {
    local name=$1 val=$2
    if ! grep -qE "^[[:space:]]*$name[[:space:]]+[0-9]+" "$HAPROXY_CFG"; then
        log "警告：cfg 里没有 $name 指令，${name^^} 覆盖被忽略"
        return 0
    fi
    sed -i -E "s/^([[:space:]]*)$name[[:space:]]+[0-9]+/\1$name $val/" "$HAPROXY_CFG"
    log "已覆盖 $name=$val（模板默认值被本机环境变量替换）"
}
if [ -n "${HAPROXY_MAXCONN:-}" ]; then
    override_directive maxconn "$HAPROXY_MAXCONN"
fi
if [ -n "${HAPROXY_MAXPIPES:-}" ]; then
    override_directive maxpipes "$HAPROXY_MAXPIPES"
fi

# 限速自检：限速由内核 tc（HTB）执行，不再走 HAProxy 的 bwlim。
# 限速静默失效是本项目最不能接受的故障（用户以为限住了，实际没有），
# 所以这里在真正启动之前就把三个前提逐个验掉，缺哪个说哪个。
#
# 之所以要在**运行期**验而不是只在构建镜像时验：前两项取决于宿主机内核
# 与容器的 capability，构建时根本看不到。
tc_selfcheck() {
    if ! command -v tc >/dev/null 2>&1; then
        log "致命：找不到 tc（iproute2）。限速由内核 tc 执行，没有它本节点"
        log "致命：**完全不会限速**。镜像多半是旧的，请重建：docker compose up -d --build"
        return 1
    fi
    # CAP_NET_ADMIN：容器默认不带，compose 里要写 cap_add: [NET_ADMIN]。
    # 用 lo 上加一个再删掉来实测，比解析 capsh 输出可靠。
    if ! tc qdisc add dev lo root handle 9999: pfifo >/dev/null 2>&1; then
        log "致命：没有 CAP_NET_ADMIN，无法操作 tc（本节点将不会限速）。"
        log "致命：请在 compose 的本服务下加：cap_add: [\"NET_ADMIN\"]"
        return 1
    fi
    tc qdisc del dev lo root >/dev/null 2>&1 || true
    # 内核有没有 HTB 调度器。iproute2 装了不代表内核带 sch_htb——精简内核
    # （容器优化型发行版、部分云厂商镜像）经常把它裁掉，届时 tc 会报
    # "Specified qdisc kind is unknown"，限速一样是静默失效。
    if ! tc qdisc add dev lo root handle 9999: htb >/dev/null 2>&1; then
        log "致命：内核不支持 HTB 调度器（sch_htb），限速无法工作。"
        log "致命：宿主机上执行 modprobe sch_htb；若内核根本没编译该模块，"
        log "致命：需要换一个带完整 net/sched 的内核。"
        return 1
    fi
    tc qdisc del dev lo root >/dev/null 2>&1 || true
    log "限速自检通过：tc 可用、有 CAP_NET_ADMIN、内核支持 HTB"
    return 0
}
if [ "${RL_TC_IFACE:-}" = "-" ]; then
    log "警告：已显式关闭 tc 限速（RL_TC_IFACE=-），本节点只做监控"
elif ! tc_selfcheck; then
    exit 1
fi

# 内核参数调优：HAProxy 的 maxconn/backlog 配得再大，也会被内核默认值在
# 下面削掉——最典型的是 net.core.somaxconn（默认 4096）给 cfg 里的
# `backlog 65536` 封顶，配置写着 65536、实际生效 4096，没有任何告警。
# 这一步在启动 HAProxy **之前**做，逐项读回校验；容器里设不了的会明确
# 列出来并给出宿主机命令。设不全不阻断启动——它影响的是性能上限，
# 不像 tc 那样关系到"限速有没有生效"。详见 docs/08-内核参数调优.md。
TUNE_KERNEL=${TUNE_KERNEL:-/usr/local/bin/tune-kernel.sh}
if [ "${RL_TUNE_KERNEL:-1}" = "0" ]; then
    log "已跳过内核参数调优（RL_TUNE_KERNEL=0）"
elif [ -x "$TUNE_KERNEL" ]; then
    "$TUNE_KERNEL" apply || true
else
    log "警告：找不到 $TUNE_KERNEL，跳过内核参数调优（镜像多半是旧的）"
fi

# FD 预检：HAProxy 需要 maxconn×2 + maxpipes×2 + 34 个 fd（管道那两个是
# splice 用的），给不够它**拒绝启动**并留下
#   [ALERT] Cannot raise FD limit to 4000034, limit is 4096.
# 然后容器进入重启循环刷屏。这里提前把账算给运维看。
#
# 比的是**硬上限**：HAProxy 自己会把软上限抬到硬上限，所以软上限低不要紧。
fd_preflight() {
    local maxconn maxpipes need hard
    maxconn=$(awk '$1=="maxconn" && $2 ~ /^[0-9]+$/ {print $2; exit}' "$HAPROXY_CFG")
    # 没写 maxconn 时 haproxy 自己按 ulimit 反推，没有可核对的目标值。
    [ -n "${maxconn:-}" ] || return 0
    maxpipes=$(awk '$1=="maxpipes" && $2 ~ /^[0-9]+$/ {print $2; exit}' "$HAPROXY_CFG")
    # 不写 maxpipes 时的默认值就是 maxconn/4。
    [ -n "${maxpipes:-}" ] || maxpipes=$((maxconn / 4))
    need=$((maxconn * 2 + maxpipes * 2 + 34))
    hard=$(ulimit -Hn)
    [ "$hard" = unlimited ] && return 0
    if [ "$hard" -lt "$need" ]; then
        log "致命：文件描述符上限不够 maxconn=$maxconn maxpipes=$maxpipes"
        log "致命：需要 $need 个 fd（maxconn×2 + maxpipes×2 + 34），当前硬上限只有 $hard，"
        log "致命：HAProxy 会直接拒绝启动。compose 里给本服务加："
        log "致命：    ulimits:"
        log "致命：      nofile: {soft: $need, hard: $need}"
        log "致命：裸机 systemd 则在 **haproxy 自己的** unit 里设 LimitNOFILE=$need。"
        return 1
    fi
    log "FD 预检通过 need=$need hard=$hard maxconn=$maxconn maxpipes=$maxpipes"
}
fd_preflight || exit 1

# -W: master-worker（与生产的 systemd 形态一致，reload 走 SIGUSR2）
# -db: 不后台化，让 master 进程留在前台受本脚本管理
log "启动 HAProxy cfg=$HAPROXY_CFG"
haproxy -W -db -f "$HAPROXY_CFG" &
HAPROXY_PID=$!
# 自己写 master pid：`-p` 在 -db 模式下不落盘，而"改 cfg 后 reload"要
# 靠它定位该给谁发 SIGUSR2（容器里没有 systemctl reload haproxy）。
echo "$HAPROXY_PID" > "$HAPROXY_PIDFILE"

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

log "启动 rl-limiter（同机模式）yaml=$RL_YAML"
rl-limiter -c "$RL_YAML" &
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
