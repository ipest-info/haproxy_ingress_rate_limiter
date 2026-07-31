#!/usr/bin/env bash
# deploy/bare/rl-limiter.sh —— 裸机部署 rl-limiter 的一键脚本。
#
# 形态：**HAProxy 与 rl-limiter 都直接跑在本机**（不进容器），只有配置库
# 用 Docker 起。前提是本机 HAProxy 已经装好并在跑——本脚本不碰它的安装，
# 也**不会重写它的 haproxy.cfg**（只在缺 stats socket 时告诉你该加哪一行）。
#
# 用法：
#   ./rl-limiter.sh install     装 + 起（幂等，可反复跑）
#   ./rl-limiter.sh check       体检：只看不改，有问题时退出码 1
#   ./rl-limiter.sh start|stop|restart|status|logs
#   ./rl-limiter.sh mysql-up|mysql-down   起/停配置库容器
#   ./rl-limiter.sh uninstall   卸载（保留配置库与 /etc/rl-limiter）
#
# install 干这些事，每一步都会说清楚做了什么：
#   1. 前置检查：python3 ≥ 3.11、haproxy 已装、tc 可用、内核有 HTB、systemd
#   2. 建专用用户 rl-limiter 并加入 haproxy 组（读 stats socket 靠属组）
#   3. venv 装到 /opt/rl-limiter
#   4. 配置文件 /etc/rl-limiter/rl-limiter.env（已存在则**不覆盖**）
#   5. 起配置库容器并等就绪；把本机登记进库、确保至少有一个 frontend
#   6. 授权：haproxy 配置目录可写 + polkit 允许 reload haproxy
#   7. 内核参数调优（裸机上全都能设，不像容器里那样受限）
#   8. 装 systemd unit → daemon-reload → enable --now
#   9. 起来之后**逐项核对**：进程在不在、socket 通不通、tc 类建了没
#
# 设计约束（和本项目其它地方一致）：
#   - 已有的东西一概不覆盖（配置文件、库里的限额、别人的 haproxy.cfg）；
#   - 每一步做完都**读回来确认**，不靠"命令返回 0"当成功；
#   - 缺什么就说缺什么、该执行什么命令，不静默降级。

set -uo pipefail

HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO=$(cd "$HERE/../.." && pwd)

# 可以用环境变量覆盖（装到别处、用别的用户）。
RL_USER=${RL_USER:-rl-limiter}
VENV=${VENV:-/opt/rl-limiter}
CONF_DIR=${CONF_DIR:-/etc/rl-limiter}
ENV_FILE="$CONF_DIR/rl-limiter.env"
UNIT=/etc/systemd/system/rl-limiter.service
POLKIT_RULE=/etc/polkit-1/rules.d/50-rl-limiter-haproxy.rules
COMPOSE_FILE="$HERE/docker-compose.mysql.yml"

# 初始 frontend（只在库里还没有时创建；已有的一个字节都不碰）。
BOOTSTRAP_FRONTEND=${BOOTSTRAP_FRONTEND:-fe_main}
BOOTSTRAP_PORT=${BOOTSTRAP_PORT:-8080}
BOOTSTRAP_QUOTA_MBPS=${BOOTSTRAP_QUOTA_MBPS:-1000}
BOOTSTRAP_BACKEND=${BOOTSTRAP_BACKEND:-}

ok()   { printf '  [ok]   %s\n' "$*"; }
warn() { printf '  [warn] %s\n' "$*"; }
bad()  { printf '  [FAIL] %s\n' "$*"; }
step() { printf '\n== %s ==\n' "$*"; }
die()  { printf '\n致命：%s\n' "$*" >&2; exit 1; }

need_root() {
    [ "$(id -u)" -eq 0 ] || die "这一步需要 root：sudo $0 $*"
}

compose() {
    if docker compose version >/dev/null 2>&1; then
        docker compose -f "$COMPOSE_FILE" "$@"
    elif command -v docker-compose >/dev/null 2>&1; then
        docker-compose -f "$COMPOSE_FILE" "$@"
    else
        die "找不到 docker compose。配置库要用 Docker 起（见 $COMPOSE_FILE）"
    fi
}

# ---------------------------------------------------------------------------
# 前置检查
# ---------------------------------------------------------------------------
# 只看不改。install 会先跑一遍，缺硬前提就不往下走——装到一半再失败比
# 一开始就说清楚难排查得多。
preflight() {
    local fail=0 quiet=${1:-}
    [ -n "$quiet" ] || step "前置检查"

    # Python：本项目要求 3.11+（asyncio 的 TaskGroup 等）
    if ! command -v python3 >/dev/null 2>&1; then
        bad "找不到 python3"; fail=1
    else
        local pv
        pv=$(python3 -c 'import sys;print("%d.%d"%sys.version_info[:2])')
        if python3 -c 'import sys;sys.exit(0 if sys.version_info>=(3,11) else 1)'; then
            ok "python3 $pv"
        else
            bad "python3 $pv 太旧，需要 ≥ 3.11"; fail=1
        fi
    fi
    python3 -c 'import venv' 2>/dev/null && ok "python3-venv 可用" || {
        bad "缺 python3-venv（Debian/Ubuntu: apt install python3-venv）"; fail=1; }

    # HAProxy：**假定已经装好**，这里只确认它在，并提示版本
    if command -v haproxy >/dev/null 2>&1; then
        ok "haproxy $(haproxy -v 2>/dev/null | head -1 | tr -d '\n')"
    else
        bad "找不到 haproxy。本脚本假定本机 HAProxy 已装好"; fail=1
    fi

    # tc：限速的执行者。三个前提缺一个限速就静默失效，逐个实测。
    if ! command -v tc >/dev/null 2>&1; then
        bad "找不到 tc（apt install iproute2）—— 没有它**完全不会限速**"; fail=1
    else
        ok "tc 已安装"
        if [ "$(id -u)" -eq 0 ]; then
            if tc qdisc add dev lo root handle 9999: htb >/dev/null 2>&1; then
                ok "内核支持 HTB 调度器（sch_htb）"
                tc qdisc del dev lo root >/dev/null 2>&1
            else
                bad "内核不支持 HTB（sch_htb）—— 限速无法工作。"
                bad "  试 modprobe sch_htb；模块都没有则需要换内核"
                fail=1
            fi
        else
            warn "非 root，跳过 HTB 实测（install 时会以 root 再验一次）"
        fi
    fi

    command -v systemctl >/dev/null 2>&1 && ok "systemd 可用" || {
        bad "找不到 systemctl，本脚本按 systemd 部署"; fail=1; }
    command -v docker >/dev/null 2>&1 && ok "docker 可用（配置库要用）" || {
        bad "找不到 docker —— 配置库跑在容器里"; fail=1; }

    return $fail
}

# haproxy.cfg 的 stats socket：rl-limiter 的采样入口，没有它什么都采不到。
# **不自动改别人的 cfg**——那是生产配置，改坏了代价太大。缺了就把该加的
# 那一行原样打出来，让人自己贴。
check_stats_socket() {
    local cfg=$1
    [ -r "$cfg" ] || { warn "读不到 $cfg，跳过 stats socket 检查"; return 0; }
    if grep -qE '^[[:space:]]*stats[[:space:]]+socket[[:space:]]+/' "$cfg"; then
        ok "haproxy.cfg 里有 unix stats socket"
        return 0
    fi
    bad "haproxy.cfg 的 global 段里没有 unix stats socket —— rl-limiter 采不到任何数据"
    cat <<'EOF'

         请在 global 段加上这一行（然后 systemctl reload haproxy）：

             stats socket /run/haproxy/admin.sock mode 660 level user

         本脚本**不会替你改 haproxy.cfg**：那是你的生产配置，
         受管区块之外的内容我们一个字节都不碰。
EOF
    return 1
}

# ---------------------------------------------------------------------------
# install 的各步骤
# ---------------------------------------------------------------------------
ensure_user() {
    step "专用用户"
    if id "$RL_USER" >/dev/null 2>&1; then
        ok "用户 $RL_USER 已存在"
    else
        useradd --system --no-create-home --shell /usr/sbin/nologin "$RL_USER" \
            || die "建用户 $RL_USER 失败"
        ok "已建用户 $RL_USER"
    fi
    # 读 HAProxy 的 unix stats socket 靠 haproxy 属组（socket 是 mode 660），
    # 这样服务本身不需要 root。
    if getent group haproxy >/dev/null 2>&1; then
        usermod -aG haproxy "$RL_USER"
        id -nG "$RL_USER" | tr ' ' '\n' | grep -qx haproxy \
            && ok "$RL_USER 已在 haproxy 组里（读 stats socket 靠它）" \
            || warn "把 $RL_USER 加进 haproxy 组失败，采样可能没权限"
    else
        warn "本机没有 haproxy 组。stats socket 的属组要另外确认，"
        warn "否则 rl-limiter 读不了它（表现为持续的采样失败告警）"
    fi
}

install_venv() {
    step "安装 rl-limiter（venv: $VENV）"
    python3 -m venv "$VENV" || die "建 venv 失败"
    "$VENV/bin/pip" install --quiet --upgrade pip >/dev/null 2>&1
    "$VENV/bin/pip" install --quiet "$REPO" || die "pip install 失败"
    [ -x "$VENV/bin/rl-limiter" ] || die "装完了却没有 $VENV/bin/rl-limiter"
    ok "已安装 $("$VENV/bin/rl-limiter" --version 2>&1 | head -1)"
}

install_conf() {
    step "配置文件"
    mkdir -p "$CONF_DIR"
    if [ -f "$ENV_FILE" ]; then
        # 已有配置一概不覆盖——里面是运维填过的库密码、网卡名这些东西。
        ok "$ENV_FILE 已存在，保持不动"
    else
        cp "$HERE/rl-limiter.env.example" "$ENV_FILE"
        chmod 640 "$ENV_FILE"
        chgrp "$RL_USER" "$ENV_FILE" 2>/dev/null || true
        ok "已生成 $ENV_FILE（含库密码，权限 640）"
        warn "**先编辑它**（至少确认 RL_NODE_NAME 与库里一致、"
        warn "RL_MYSQL_PASSWORD 对得上），再继续。"
    fi
}

# 从 env 文件里取一个变量的值（systemd EnvironmentFile 格式，不是 shell）
env_get() {
    local key=$1
    [ -f "$ENV_FILE" ] || return 0
    sed -n "s/^[[:space:]]*${key}=//p" "$ENV_FILE" | tail -1 | tr -d '"'"'"
}

grant_perms() {
    step "授权"
    local cfg dir
    cfg=$(env_get RL_APPLY_HAPROXY_CFG)
    if [ -z "$cfg" ]; then
        warn "未启用配置自动下发（RL_APPLY_HAPROXY_CFG 为空），跳过 cfg 授权"
        return 0
    fi
    dir=$(dirname "$cfg")
    # **目录**要可写而不只是文件：原子替换要在同目录建临时文件再 rename，
    # 跨挂载点的 rename 会失败。
    if [ -d "$dir" ]; then
        chgrp "$RL_USER" "$dir" "$cfg" 2>/dev/null || true
        chmod g+w "$dir" 2>/dev/null || true
        [ -f "$cfg" ] && chmod g+w "$cfg" 2>/dev/null
        if sudo -u "$RL_USER" test -w "$dir"; then
            ok "$dir 对 $RL_USER 可写（原子替换需要目录权限，不只是文件）"
        else
            bad "$dir 对 $RL_USER 仍不可写 —— 配置下发会一直失败"
        fi
    else
        warn "目录 $dir 不存在，跳过"
    fi

    # reload haproxy：用 polkit 而不是 sudo。走 sudo 就必须把 unit 里的
    # NoNewPrivileges 放开，而那类失败只体现在"配置没生效"上，很难联想到。
    if [ -d /etc/polkit-1/rules.d ]; then
        cat >"$POLKIT_RULE" <<EOF
// 由 deploy/bare/rl-limiter.sh 生成：只允许 $RL_USER reload haproxy 这一件事。
polkit.addRule(function(action, subject) {
    if (action.id == "org.freedesktop.systemd1.manage-units" &&
        action.lookup("unit") == "haproxy.service" &&
        action.lookup("verb") == "reload" &&
        subject.user == "$RL_USER") {
        return polkit.Result.YES;
    }
});
EOF
        ok "已装 polkit 规则（仅允许 $RL_USER reload haproxy）：$POLKIT_RULE"
    else
        warn "没有 /etc/polkit-1/rules.d，改用 sudoers。**同时要把 unit 里的"
        warn "NoNewPrivileges 改成 no**，否则 sudo 提权会被内核直接拒绝："
        warn "  $RL_USER ALL=(root) NOPASSWD: /usr/bin/systemctl reload haproxy"
    fi
}

tune_kernel() {
    step "内核参数调优"
    local t="$REPO/deploy/sysctl/tune-kernel.sh"
    [ -x "$t" ] || { warn "找不到 $t，跳过"; return 0; }
    # 裸机上这些参数全都能设（不像容器里 rmem_max/conntrack/fs.* 改不动），
    # 所以这一步在裸机形态下才真正完整。
    "$t" apply || true
    # 持久化，重启后仍在
    if [ -d /etc/sysctl.d ]; then
        "$t" dump >/etc/sysctl.d/99-rl-limiter.conf
        ok "已写 /etc/sysctl.d/99-rl-limiter.conf（重启后仍生效）"
    fi
}

install_unit() {
    step "systemd unit"
    local cfg dir
    cfg=$(env_get RL_APPLY_HAPROXY_CFG)
    dir=${cfg:+$(dirname "$cfg")}
    dir=${dir:-/etc/haproxy}
    sed -e "s|@VENV@|$VENV|g" \
        -e "s|@USER@|$RL_USER|g" \
        -e "s|@ENVFILE@|$ENV_FILE|g" \
        -e "s|@HAPROXY_CFG_DIR@|$dir|g" \
        "$HERE/rl-limiter.service.in" >"$UNIT" || die "生成 unit 失败"
    grep -q '@' "$UNIT" && warn "unit 里还有没替换的占位符，检查 $UNIT"
    systemctl daemon-reload
    ok "已装 $UNIT（ReadWritePaths=$dir，带 CAP_NET_ADMIN）"
}

bootstrap_db() {
    step "配置库"
    compose up -d || die "起配置库容器失败"
    ok "配置库容器已启动（$(basename "$COMPOSE_FILE")）"

    local node backend_args=()
    node=$(env_get RL_NODE_NAME); node=${node:-hap-1}
    if [ -n "$BOOTSTRAP_BACKEND" ]; then
        # 逗号分隔的 地址:端口 列表
        local IFS=,
        for b in $BOOTSTRAP_BACKEND; do backend_args+=(--backend "$b"); done
    fi

    # 环境变量从 env 文件读，跟服务本身用同一份接线参数。
    set -a; . "$ENV_FILE"; set +a
    if [ ${#backend_args[@]} -gt 0 ]; then
        "$VENV/bin/python" "$REPO/tools/bootstrap_db.py" \
            --instance "$node" \
            --socket "$(haproxy_socket_path)" \
            --frontend "$BOOTSTRAP_FRONTEND" --port "$BOOTSTRAP_PORT" \
            --quota-mbps "$BOOTSTRAP_QUOTA_MBPS" "${backend_args[@]}" \
            || die "登记配置库失败"
    else
        "$VENV/bin/python" "$REPO/tools/bootstrap_db.py" \
            --instance "$node" --socket "$(haproxy_socket_path)" || {
            echo
            echo "库里这个实例名下还没有 frontend，rl-limiter 会拒绝启动。"
            echo "给一个初始 frontend 再跑一次即可，例如："
            echo "  BOOTSTRAP_BACKEND=10.0.0.21:9000 $0 install"
            echo "（也可以调 BOOTSTRAP_FRONTEND / BOOTSTRAP_PORT /"
            echo "  BOOTSTRAP_QUOTA_MBPS；这些只在新建时用，不会覆盖已有配置）"
            exit 1
        }
    fi
}

# 从本机 haproxy.cfg 里读出 stats socket 路径；读不到用默认值。
haproxy_socket_path() {
    local cfg p
    cfg=$(env_get RL_APPLY_HAPROXY_CFG); cfg=${cfg:-/etc/haproxy/haproxy.cfg}
    p=$(sed -nE 's/^[[:space:]]*stats[[:space:]]+socket[[:space:]]+(\/[^[:space:]]+).*/\1/p' \
        "$cfg" 2>/dev/null | head -1)
    echo "${p:-/run/haproxy/admin.sock}"
}

# ---------------------------------------------------------------------------
# 起来之后逐项核对——"服务在跑"不等于"限速在生效"
# ---------------------------------------------------------------------------
verify() {
    step "启动后核对"
    local fail=0
    sleep 2
    if systemctl is-active --quiet rl-limiter; then
        ok "rl-limiter 正在运行"
    else
        bad "rl-limiter 没起来。看日志：journalctl -u rl-limiter -n 50 --no-pager"
        return 1
    fi

    local sock; sock=$(haproxy_socket_path)
    [ -S "$sock" ] && ok "HAProxy stats socket 存在（$sock）" || {
        bad "找不到 stats socket $sock —— 采样会一直失败"; fail=1; }

    local iface; iface=$(env_get RL_TC_IFACE)
    if [ "$iface" = "-" ]; then
        warn "限速已被显式关闭（RL_TC_IFACE=-），只做监控与配置下发"
    else
        # 真正要确认的是"tc 队列树建起来了"，而不是"进程活着"。
        sleep 3
        if "$VENV/bin/python" "$REPO/tools/tc_check.py" verify \
                ${iface:+-i "$iface"} 2>/dev/null; then
            ok "tc 限速已按配置生效"
        else
            bad "tc 核对未通过（上面有逐条说明）。限速可能没在工作"
            fail=1
        fi
    fi

    local port; port=$(env_get RL_CONSOLE_PORT)
    local bind; bind=$(env_get RL_CONSOLE_BIND); bind=${bind:-127.0.0.1}
    [ -n "$port" ] && ok "控制台：http://${bind}:${port}"
    return $fail
}

# ---------------------------------------------------------------------------
# 子命令
# ---------------------------------------------------------------------------
cmd_install() {
    need_root install
    preflight || die "前置检查未通过，先把上面标 FAIL 的项解决掉"
    ensure_user
    install_venv
    install_conf
    local hcfg; hcfg=$(env_get RL_APPLY_HAPROXY_CFG)
    check_stats_socket "${hcfg:-/etc/haproxy/haproxy.cfg}" \
        || warn "stats socket 这项没过，服务起来后会一直报采样失败"
    bootstrap_db
    grant_perms
    tune_kernel
    install_unit

    step "启动"
    systemctl enable --now rl-limiter >/dev/null 2>&1 || systemctl restart rl-limiter
    verify
    local rc=$?
    step "完成"
    echo "  配置：  $ENV_FILE（改完 systemctl restart rl-limiter）"
    echo "  日志：  journalctl -u rl-limiter -f"
    echo "  体检：  $0 check"
    return $rc
}

cmd_check() {
    preflight
    local fail=$?
    step "已安装的东西"
    [ -x "$VENV/bin/rl-limiter" ] && ok "venv: $VENV" || { bad "没装 venv"; fail=1; }
    [ -f "$ENV_FILE" ] && ok "配置: $ENV_FILE" || { bad "没有 $ENV_FILE"; fail=1; }
    [ -f "$UNIT" ] && ok "unit: $UNIT" || { bad "没装 systemd unit"; fail=1; }
    systemctl is-active --quiet rl-limiter && ok "服务正在运行" || {
        warn "服务未运行（$0 start）"; }
    local hcfg; hcfg=$(env_get RL_APPLY_HAPROXY_CFG)
    check_stats_socket "${hcfg:-/etc/haproxy/haproxy.cfg}" || fail=1
    step "内核参数"
    "$REPO/deploy/sysctl/tune-kernel.sh" check || fail=1
    return $fail
}

case "${1:-}" in
    install)   cmd_install ;;
    check)     cmd_check ;;
    start)     need_root start; systemctl start rl-limiter && verify ;;
    stop)      need_root stop; systemctl stop rl-limiter && echo "已停止（**限速仍在**：tc 队列树留在网卡上，见 uninstall）" ;;
    restart)   need_root restart; systemctl restart rl-limiter && verify ;;
    status)    systemctl status rl-limiter --no-pager ;;
    logs)      journalctl -u rl-limiter -f ;;
    mysql-up)  compose up -d && compose ps ;;
    mysql-down) compose down ;;
    uninstall)
        need_root uninstall
        systemctl disable --now rl-limiter >/dev/null 2>&1
        rm -f "$UNIT" "$POLKIT_RULE"
        systemctl daemon-reload
        echo "已卸载服务。**以下东西刻意保留**，要清请手工来："
        echo "  配置库容器与数据卷：$0 mysql-down（加 -v 才删数据）"
        echo "  配置文件：$CONF_DIR"
        echo "  venv：    $VENV"
        echo "  网卡上的 tc 限速队列树：tc qdisc del dev <网卡> root"
        echo "  —— 最后这条要留意：**服务停了限速还在**，那是刻意的"
        echo "     （rl-limiter 宕机不该导致限速失效），但卸载时要手工清。"
        ;;
    *)
        sed -n '2,30p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
        exit 2 ;;
esac
