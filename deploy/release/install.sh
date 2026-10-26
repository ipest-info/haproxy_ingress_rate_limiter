#!/usr/bin/env bash
# rl-limiter 二进制发布包安装脚本（随 tar.gz 一起分发，也可单独下载）。
#
# 两种用法：
#   1. 已经手头有发布包（解开后在包目录里执行）：
#        tar xzf rl-limiter-v0.4.0-linux-x86_64.tar.gz
#        cd rl-limiter-v0.4.0-linux-x86_64 && sudo ./install.sh
#   2. 直接从 GitHub Release 下载并安装（需要能访问 github.com）：
#        sudo ./install.sh --download            # 默认装最新 Release
#        sudo ./install.sh --download v0.5.0     # 或指定版本
#      （校验 .sha256 后解包安装；版本号 = release 的 tag）
#
# 装什么：
#   /usr/local/bin/rl-limiter /usr/local/bin/rl-fleet   二进制（覆盖升级）
#   /etc/rl-limiter/{config.yaml,rl-limiter.env}        配置（已存在**不覆盖**）
#   /etc/systemd/system/rl-limiter.service              节点服务（默认装）
#   /etc/systemd/system/rl-fleet.service                总控服务（--fleet 才装）
#
# 装完不自动启动：先编辑配置，再 systemctl enable --now rl-limiter。
# 升级 = 重跑本脚本（二进制覆盖、配置不动）+ systemctl restart。
set -euo pipefail

REPO="ipest-info/haproxy_ingress_rate_limiter"
PREFIX=${PREFIX:-/usr/local}
CONF_DIR=${CONF_DIR:-/etc/rl-limiter}
UNIT_DIR=${UNIT_DIR:-/etc/systemd/system}
WITH_FLEET=0
DOWNLOAD_TAG=""

usage() { sed -n '2,20p' "$0"; exit "${1:-0}"; }
log()   { echo "install.sh: $*"; }
die()   { echo "install.sh: 错误：$*" >&2; exit 1; }

while [ $# -gt 0 ]; do
    case "$1" in
        --fleet) WITH_FLEET=1 ;;
        --download)
            # 带 tag = 装指定版本；不带（或 latest）= 装最新 Release。
            if [ -n "${2:-}" ] && [ "${2#-}" = "$2" ]; then
                shift; DOWNLOAD_TAG=$1
            else
                DOWNLOAD_TAG=latest
            fi ;;
        -h|--help) usage ;;
        *) die "不认识的参数 $1（--fleet / --download [tag] / --help）" ;;
    esac
    shift
done

[ "$(id -u)" = 0 ] || die "需要 root（写 $PREFIX/bin 与 systemd 单元）"

# ---- 模式 2：先下载发布包，再对解出来的包目录递归调用自己 -------------
if [ -n "$DOWNLOAD_TAG" ]; then
    if [ "$DOWNLOAD_TAG" = "latest" ]; then
        # 解析最新 Release 的 tag（GitHub API，匿名访问即可）。
        DOWNLOAD_TAG=$(curl -fsSL --retry 3 \
            "https://api.github.com/repos/$REPO/releases/latest" \
            | grep -o '"tag_name": *"[^"]*"' | head -1 | cut -d'"' -f4)
        [ -n "$DOWNLOAD_TAG" ] \
            || die "查询最新版本失败（GitHub API 不可达？）——可显式指定：--download v0.5.0"
        log "最新版本：$DOWNLOAD_TAG"
    fi
    ARCH=$(uname -m)
    NAME="rl-limiter-${DOWNLOAD_TAG}-linux-${ARCH}"
    URL="https://github.com/$REPO/releases/download/$DOWNLOAD_TAG/$NAME.tar.gz"
    TMP=$(mktemp -d); trap 'rm -rf "$TMP"' EXIT
    log "下载 $URL"
    curl -fL --retry 3 -o "$TMP/$NAME.tar.gz" "$URL"
    curl -fL --retry 3 -o "$TMP/$NAME.tar.gz.sha256" "$URL.sha256"
    ( cd "$TMP" && sha256sum -c "$NAME.tar.gz.sha256" ) \
        || die "sha256 校验失败——下载不完整或被篡改，终止安装"
    tar -C "$TMP" -xzf "$TMP/$NAME.tar.gz"
    args=(); [ "$WITH_FLEET" = 1 ] && args+=(--fleet)
    exec "$TMP/$NAME/install.sh" "${args[@]+"${args[@]}"}"
fi

# ---- 模式 1：从包目录安装 ------------------------------------------------
HERE=$(cd "$(dirname "$0")" && pwd)
[ -x "$HERE/bin/rl-limiter" ] \
    || die "没在发布包目录里（找不到 bin/rl-limiter）。先 tar xzf 解包，" \
           "进包目录再执行；或用 --download <tag> 直接下载安装"

log "安装二进制 → $PREFIX/bin/"
install -d -m 755 "$PREFIX/bin"
install -m 755 "$HERE/bin/rl-limiter" "$PREFIX/bin/rl-limiter"
install -m 755 "$HERE/bin/rl-fleet"   "$PREFIX/bin/rl-fleet"
"$PREFIX/bin/rl-limiter" --version

log "配置模板 → $CONF_DIR/（已存在的文件不覆盖）"
install -d -m 755 "$CONF_DIR"
[ -e "$CONF_DIR/config.yaml" ] \
    || install -m 640 "$HERE/config/limiter.example.yaml" "$CONF_DIR/config.yaml"
[ -e "$CONF_DIR/rl-limiter.env" ] \
    || install -m 640 "$HERE/config/rl-limiter.env.example" "$CONF_DIR/rl-limiter.env"
if [ "$WITH_FLEET" = 1 ] && [ ! -e "$CONF_DIR/fleet.yaml" ]; then
    install -m 640 "$HERE/config/fleet.example.yaml" "$CONF_DIR/fleet.yaml"
fi

# 专用运行用户（非 root；tc 权限由 unit 的 CAP_NET_ADMIN 提供）。
if ! id rl-limiter >/dev/null 2>&1; then
    log "创建运行用户 rl-limiter"
    useradd --system --no-create-home --shell /usr/sbin/nologin rl-limiter
fi

log "systemd 单元 → $UNIT_DIR/rl-limiter.service"
install -d -m 755 "$UNIT_DIR"
cat > "$UNIT_DIR/rl-limiter.service" <<EOF
# 由 install.sh（二进制发布包）生成。升级重跑 install.sh 会覆盖本文件；
# 配置请改 $CONF_DIR/config.yaml 与 $CONF_DIR/rl-limiter.env。
[Unit]
Description=rl-limiter - server egress rate limiter (NIC-based, tc/HTB)
Documentation=https://github.com/$REPO
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
EnvironmentFile=$CONF_DIR/rl-limiter.env
ExecStart=$PREFIX/bin/rl-limiter -c $CONF_DIR/config.yaml
Restart=always
RestartSec=2
TimeoutStopSec=15
User=rl-limiter
Group=rl-limiter
# tc 下发限速必须有 CAP_NET_ADMIN（少了它启动即失败，刻意行为）。
CapabilityBoundingSet=CAP_NET_ADMIN
AmbientCapabilities=CAP_NET_ADMIN
LogsDirectory=rl-limiter
ReadWritePaths=$CONF_DIR
NoNewPrivileges=yes
ProtectSystem=strict
ProtectHome=yes
PrivateTmp=yes
ProtectKernelTunables=yes
ProtectControlGroups=yes
RestrictSUIDSGID=yes
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
EOF

if [ "$WITH_FLEET" = 1 ]; then
    log "systemd 单元 → $UNIT_DIR/rl-fleet.service"
    cat > "$UNIT_DIR/rl-fleet.service" <<EOF
# 由 install.sh --fleet 生成。总控不碰 tc，不需要 CAP_NET_ADMIN。
[Unit]
Description=rl-fleet - multi-node total-bandwidth controller
Documentation=https://github.com/$REPO
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
EnvironmentFile=-$CONF_DIR/rl-fleet.env
ExecStart=$PREFIX/bin/rl-fleet -c $CONF_DIR/fleet.yaml
Restart=always
RestartSec=2
TimeoutStopSec=15
User=rl-limiter
Group=rl-limiter
LogsDirectory=rl-limiter
ReadWritePaths=$CONF_DIR
NoNewPrivileges=yes
ProtectSystem=strict
ProtectHome=yes
PrivateTmp=yes

[Install]
WantedBy=multi-user.target
EOF
fi

systemctl daemon-reload 2>/dev/null || log "提示：systemctl 不可用（容器？），单元文件已就位"

log "完成。下一步："
log "  1. 编辑 $CONF_DIR/config.yaml（haproxy 接线/限额）与 $CONF_DIR/rl-limiter.env（网卡/令牌）"
log "  2. systemctl enable --now rl-limiter"
[ "$WITH_FLEET" = 1 ] && log "  3. 总控：编辑 $CONF_DIR/fleet.yaml 后 systemctl enable --now rl-fleet"
exit 0
