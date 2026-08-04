#!/usr/bin/env bash
# deploy/bare/rl-limiter.sh —— 裸机部署 rl-limiter 的一键脚本。
#
# 形态：**HAProxy 与 rl-limiter 都直接跑在本机**（不进容器，也没有任何
# 数据库）。前提是本机 HAProxy 已经装好并在跑——本脚本不碰它的安装，
# 也**绝不改写 haproxy.cfg**（缺 stats socket 时只告诉你该加哪一行）。
#
# 配置只有两个本机文件：
#   /etc/haproxy/haproxy.cfg      —— 负载均衡配置的**唯一权威**（监听端口、
#                                    后端服务器；运维直接编辑 + reload）
#   /etc/rl-limiter/config.yaml   —— rl-limiter 的接线 + 限额登记（quotas）
# 两份文件的内容都被 rl-limiter 轮询（5s），改了即热生效（cfg 改完记得
# reload haproxy）。
#
# 用法：
#   ./rl-limiter.sh install     装 + 起（幂等，可反复跑）
#   ./rl-limiter.sh check       体检：只看不改，有问题时退出码 1
#   ./rl-limiter.sh start|stop|restart|status|logs
#   ./rl-limiter.sh uninstall   卸载（保留 /etc/rl-limiter）
#
# install 干这些事，每一步都会说清楚做了什么：
#   1. 前置检查：python3 ≥ 3.9、haproxy 已装、tc 可用、内核有 HTB、systemd
#   2. 建专用用户 rl-limiter 并加入 haproxy 组（读 stats socket 靠属组）
#   3. venv 装到 /opt/rl-limiter
#   4. 配置文件 /etc/rl-limiter/{config.yaml,rl-limiter.env}（已存在则**不覆盖**）
#   5. 校验配置能真实加载（YAML + haproxy.cfg 解析各过一遍）
#   6. 内核参数调优（裸机上全都能设，不像容器里那样受限）
#   7. 装 systemd unit → daemon-reload → enable --now
#   8. 起来之后**逐项核对**：进程在不在、socket 通不通、tc 类建了没
#
# 设计约束（和本项目其它地方一致）：
#   - 已有的东西一概不覆盖（配置文件、别人的 haproxy.cfg）；
#   - 每一步做完都**读回来确认**，不靠"命令返回 0"当成功；
#   - 缺什么就说缺什么、该执行什么命令，不静默降级。

set -uo pipefail

HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO=$(cd "$HERE/../.." && pwd)

# 可以用环境变量覆盖（装到别处、用别的用户）。
RL_USER=${RL_USER:-rl-limiter}
VENV=${VENV:-/opt/rl-limiter}
CONF_DIR=${CONF_DIR:-/etc/rl-limiter}
YAML_FILE="$CONF_DIR/config.yaml"
ENV_FILE="$CONF_DIR/rl-limiter.env"
UNIT=/etc/systemd/system/rl-limiter.service

ok()   { printf '  [ok]   %s\n' "$*"; }
warn() { printf '  [warn] %s\n' "$*"; }
bad()  { printf '  [FAIL] %s\n' "$*"; }
step() { printf '\n== %s ==\n' "$*"; }
die()  { printf '\n致命：%s\n' "$*" >&2; exit 1; }

need_root() {
    [ "$(id -u)" -eq 0 ] || die "这一步需要 root：sudo $0 $*"
}

# ---------------------------------------------------------------------------
# 前置检查
# ---------------------------------------------------------------------------
# 只看不改。install 会先跑一遍，缺硬前提就不往下走——装到一半再失败比
# 一开始就说清楚难排查得多。
preflight() {
    local fail=0 quiet=${1:-}
    [ -n "$quiet" ] || step "前置检查"

    # Python：最低 3.9（生产存量机器还有 3.9；推荐 3.11+）。
    if ! command -v python3 >/dev/null 2>&1; then
        bad "找不到 python3"; fail=1
    else
        local pv
        pv=$(python3 -c 'import sys;print("%d.%d"%sys.version_info[:2])')
        if python3 -c 'import sys;sys.exit(0 if sys.version_info>=(3,9) else 1)'; then
            ok "python3 $pv"
        else
            bad "python3 $pv 太旧，需要 ≥ 3.9"; fail=1
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
         我们一个字节都不碰。
EOF
    return 1
}

# contstats：不开的话 TCP 长连接的 bytes_out 只在会话结束时一次性入账，
# 秒级带宽曲线全是脉冲，监控数据不可用。同样只提示不代改。
check_contstats() {
    local cfg=$1
    [ -r "$cfg" ] || return 0
    if grep -qE '^[[:space:]]*option[[:space:]]+contstats' "$cfg"; then
        ok "haproxy.cfg 里有 option contstats（长连接的秒级带宽依赖它）"
    else
        warn "haproxy.cfg 里没找到 option contstats——TCP 长连接的带宽曲线"
        warn "  会脉冲式跳变。请在 defaults 段加：option contstats"
    fi
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
    # 目录属组给服务用户并放开组写：控制台/写 API 改限额是"临时文件 +
    # 原子替换"，需要**目录**的写权限（unit 里配套 ReadWritePaths）。
    chgrp "$RL_USER" "$CONF_DIR" 2>/dev/null || true
    chmod 2770 "$CONF_DIR" 2>/dev/null || chmod 770 "$CONF_DIR"
    if [ -f "$YAML_FILE" ]; then
        # 已有配置一概不覆盖——里面是运维调过的限额。属组/权限仍然校正
        # （老版本装出来的是 640 root:root，写 API 无法回写）。
        chgrp "$RL_USER" "$YAML_FILE" 2>/dev/null || true
        chmod 660 "$YAML_FILE"
        ok "$YAML_FILE 已存在，保持不动（已校正属组/权限）"
    else
        cp "$REPO/deploy/config/limiter.example.yaml" "$YAML_FILE"
        chmod 660 "$YAML_FILE"
        chgrp "$RL_USER" "$YAML_FILE" 2>/dev/null || true
        ok "已生成 $YAML_FILE（示例配置）"
        warn "**先编辑它**：cfg_path/socket_path 指向本机实际路径，"
        warn "quotas 按 haproxy.cfg 里的段名登记限额，再继续。"
    fi
    if [ -f "$ENV_FILE" ]; then
        ok "$ENV_FILE 已存在，保持不动"
    else
        cp "$HERE/rl-limiter.env.example" "$ENV_FILE"
        chmod 640 "$ENV_FILE"
        chgrp "$RL_USER" "$ENV_FILE" 2>/dev/null || true
        ok "已生成 $ENV_FILE（网卡/控制台等本机环境变量）"
    fi
}

# 从 env 文件里取一个变量的值（systemd EnvironmentFile 格式，不是 shell）
env_get() {
    local key=$1
    [ -f "$ENV_FILE" ] || return 0
    sed -n "s/^[[:space:]]*${key}=//p" "$ENV_FILE" | tail -1 | tr -d '"'"'"
}

# 从 YAML 配置里取 haproxy 段的一个标量字段（cfg_path/socket_path）。
yaml_get() {
    local key=$1
    "$VENV/bin/python" - "$YAML_FILE" "$key" <<'PY' 2>/dev/null
import sys, yaml
d = yaml.safe_load(open(sys.argv[1], encoding="utf-8")) or {}
print(((d.get("haproxy") or {}).get(sys.argv[2]) or ""))
PY
}

# 配置能不能真实加载：YAML 校验 + haproxy.cfg 解析各过一遍。
# 在装 unit 之前做——配置有问题时"装好了但起不来 + 重启循环"比这里的
# 一条报错难排查得多。
validate_conf() {
    step "配置校验"
    local out
    if out=$("$VENV/bin/python" - "$YAML_FILE" <<'PY' 2>&1
import logging, sys
from rl_limiter import cfgparse, config
cfg = config.load(sys.argv[1])
fes = cfgparse.load_frontends(cfg.haproxy.cfg_path, cfg.quotas,
                              logging.getLogger("check"))
limited = [f for f in fes if f.limited]
print(f"frontends={len(fes)} limited={len(limited)} "
      f"detail={';'.join(f'{f.name}:{f.quota_mbps:g}Mbps' if f.limited else f'{f.name}:仅监控' for f in fes)}")
PY
    ); then
        ok "配置可加载：$out"
        case "$out" in
            "frontends=0 "*)
                warn "haproxy.cfg 里没解析到任何带监听端口的 frontend/listen——"
                warn "  服务能起，但什么都不会监控/限速。检查 cfg_path 对不对。" ;;
        esac
    else
        bad "配置加载失败："
        printf '%s\n' "$out" | sed 's/^/         /'
        return 1
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
    sed -e "s|@VENV@|$VENV|g" \
        -e "s|@USER@|$RL_USER|g" \
        -e "s|@ENVFILE@|$ENV_FILE|g" \
        -e "s|@CONF_DIR@|$CONF_DIR|g" \
        "$HERE/rl-limiter.service.in" >"$UNIT" || die "生成 unit 失败"
    grep -q '@' "$UNIT" && warn "unit 里还有没替换的占位符，检查 $UNIT"
    systemctl daemon-reload
    ok "已装 $UNIT（全只读 + CAP_NET_ADMIN）"
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

    local sock; sock=$(yaml_get socket_path)
    if [ -n "$sock" ]; then
        [ -S "$sock" ] && ok "HAProxy stats socket 存在（$sock）" || {
            bad "找不到 stats socket $sock —— 采样会一直失败"; fail=1; }
    fi

    local iface; iface=$(env_get RL_TC_IFACE)
    if [ "$iface" = "-" ]; then
        warn "限速已被显式关闭（RL_TC_IFACE=-），只做监控"
    else
        # 真正要确认的是"tc 队列树建起来了"，而不是"进程活着"。
        sleep 3
        if "$VENV/bin/python" "$REPO/tools/tc_check.py" verify \
                ${iface:+-i "$iface"} 2>/dev/null; then
            ok "tc 限速已按配置生效"
        else
            bad "tc 核对未通过（上面有逐条说明）。限速可能没在工作。"
            bad "  注意：quotas 全空（只监控）时没有 tc 类，属正常"
            fail=1
        fi
    fi

    local port; port=$(env_get RL_CONSOLE_PORT)
    local bind; bind=$(env_get RL_CONSOLE_BIND); bind=${bind:-127.0.0.1}
    [ -n "$port" ] && ok "控制台（只读）：http://${bind}:${port}"
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
    local hcfg; hcfg=$(yaml_get cfg_path); hcfg=${hcfg:-/etc/haproxy/haproxy.cfg}
    check_stats_socket "$hcfg" \
        || warn "stats socket 这项没过，服务起来后会一直报采样失败"
    check_contstats "$hcfg"
    validate_conf || die "配置有问题，改好 $YAML_FILE / cfg_path 后重跑 install"
    tune_kernel
    install_unit

    step "启动"
    systemctl enable --now rl-limiter >/dev/null 2>&1 || systemctl restart rl-limiter
    verify
    local rc=$?
    step "完成"
    echo "  限额配置：$YAML_FILE（改 quotas 5s 内热生效，无需重启）"
    echo "  负载均衡：$hcfg（改完 systemctl reload haproxy，rl-limiter 自动跟上）"
    echo "  日志：    journalctl -u rl-limiter -f"
    echo "  体检：    $0 check"
    return $rc
}

cmd_check() {
    preflight
    local fail=$?
    step "已安装的东西"
    [ -x "$VENV/bin/rl-limiter" ] && ok "venv: $VENV" || { bad "没装 venv"; fail=1; }
    [ -f "$YAML_FILE" ] && ok "配置: $YAML_FILE" || { bad "没有 $YAML_FILE"; fail=1; }
    [ -f "$ENV_FILE" ] && ok "环境: $ENV_FILE" || { bad "没有 $ENV_FILE"; fail=1; }
    [ -f "$UNIT" ] && ok "unit: $UNIT" || { bad "没装 systemd unit"; fail=1; }
    systemctl is-active --quiet rl-limiter && ok "服务正在运行" || {
        warn "服务未运行（$0 start）"; }
    if [ -x "$VENV/bin/rl-limiter" ] && [ -f "$YAML_FILE" ]; then
        local hcfg; hcfg=$(yaml_get cfg_path); hcfg=${hcfg:-/etc/haproxy/haproxy.cfg}
        check_stats_socket "$hcfg" || fail=1
        check_contstats "$hcfg"
        validate_conf || fail=1
    fi
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
    uninstall)
        need_root uninstall
        systemctl disable --now rl-limiter >/dev/null 2>&1
        rm -f "$UNIT"
        systemctl daemon-reload
        echo "已卸载服务。**以下东西刻意保留**，要清请手工来："
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
