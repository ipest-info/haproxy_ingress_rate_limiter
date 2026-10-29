#!/usr/bin/env bash
# rl-limiter 二进制发布包安装脚本（随 tar.gz 分发，也作为 Release 附件单独下载）。
#
# 装节点（默认）——本机必须至少有两张物理网卡（客户侧 + 后端侧），否则拒绝安装：
#   sudo ./install.sh [--token <令牌>|auto] [--client-nic <网卡>] [--backend-nic <网卡>]
#     --token        节点写 API 令牌（RL_API_TOKEN；总控登记该节点时填同一个值）。
#                    auto = 自动生成 64 位十六进制随机令牌（256 bit），装完打印出来
#     --client-nic   客户端侧网卡 = 入向限速执行网卡（RL_TC_IN_IFACE，默认 eth0）
#     --backend-nic  后端服务侧网卡（RL_BACKEND_IFACE，默认 eth1）
#   两张网卡必须是不同的网卡（相斥），且都在本机存在。
#
# 装总控（rl-fleet，不碰 tc，不要求双网卡）：
#   sudo ./install.sh --fleet [--token <令牌>|auto]
#     --token        总控写 API 令牌（页面改总带宽、添加/删除节点用）；auto 同上
#
# 令牌必须是**高强度随机串**：≥32 位、估算 ≥128 bit、没有重复/顺序/常见词
# 等人为痕迹（规则见 rl_limiter/tokenpolicy.py）。不达标拒绝安装；运行中的
# rl-limiter / rl-fleet 也会把不达标的令牌当作没有（写接口禁用）。推荐
# --token auto，或 openssl rand -hex 32。
#
# 包从哪来：
#   1. 已有发布包：tar xzf rl-limiter-<tag>-linux-x86_64.tar.gz，进包目录执行；
#   2. --download [tag]：从 GitHub Release 下载（不带 tag = 最新版），校验
#      .sha256 后安装，其余参数原样交给包内的 install.sh。例：
#        sudo ./install.sh --download --token auto --client-nic ens33 --backend-nic ens34
#        sudo ./install.sh --download v0.6.4 --fleet --token auto
#      需要能访问 github.com（含跳转到的 objects.githubusercontent.com）。
#
# 装什么：
#   /usr/local/bin/rl-limiter /usr/local/bin/rl-fleet      二进制（覆盖升级）
#   节点：/etc/rl-limiter/{config.yaml,rl-limiter.env} + rl-limiter.service
#   总控：/etc/rl-limiter/{fleet.yaml,rl-fleet.env}     + rl-fleet.service
#   配置已存在**不覆盖**；只有命令行显式给出的令牌/网卡会写进已有的 env。
#
# 装完不自动启动：先核对配置，再 systemctl enable --now rl-limiter（或 rl-fleet）。
# 升级 = 重跑本脚本（二进制覆盖、配置不动）+ systemctl restart。
#
# 卸载：
#   sudo ./install.sh --uninstall [--fleet] [--purge]
#     节点（默认）：停服务、删单元，并**清掉网卡上的 tc 限速规则**（入向
#     钩子、IFB 设备、出向 HTB）——服务停了限速还在是刻意的，卸载不能留；
#     --fleet 卸载总控。两种角色都卸完才删二进制。
#     --purge  连该角色的配置一起删（config.yaml/rl-limiter.env 或
#              fleet.yaml/rl-fleet.env）；不加则保留，重装即沿用。
set -euo pipefail

REPO="ipest-info/haproxy_ingress_rate_limiter"
PREFIX=${PREFIX:-/usr/local}
CONF_DIR=${CONF_DIR:-/etc/rl-limiter}
UNIT_DIR=${UNIT_DIR:-/etc/systemd/system}
SYS_NET=${SYS_CLASS_NET:-/sys/class/net}
RL_USER=rl-limiter
DEFAULT_CLIENT_NIC=eth0
DEFAULT_BACKEND_NIC=eth1

WITH_FLEET=0
DOWNLOAD=0
UNINSTALL=0
PURGE=0
DOWNLOAD_TAG=""
TOKEN=""
CLIENT_NIC=""
BACKEND_NIC=""

usage() {
    awk 'NR > 1 && /^#/ { sub(/^# ?/, ""); print; next } NR > 1 { exit }' "$0"
    exit "${1:-0}"
}
log()  { echo "install.sh: $*"; }
warn() { echo "install.sh: 注意：$*" >&2; }
die()  { echo "install.sh: 错误：$*" >&2; exit 1; }

# ---- 参数 ------------------------------------------------------------------
# 取选项值：支持 --opt value 与 --opt=value 两种写法。值不许以 - 开头
# （防止 "--token --fleet" 把下一个选项吞成值）；确实以 - 开头的令牌
# 用 --token=<值> 写法。
OPTVAL="" OPTSHIFT=0
take_value() {
    local opt=$1; shift
    case "$opt" in
        *=*) OPTVAL=${opt#*=}; OPTSHIFT=0 ;;
        *)
            [ $# -ge 1 ] && [ -n "$1" ] && [ "${1#-}" = "$1" ] \
                || die "$opt 需要一个值（例：$opt <值>，或 $opt=<值>）"
            OPTVAL=$1; OPTSHIFT=1 ;;
    esac
    [ -n "$OPTVAL" ] || die "${opt%%=*} 的值为空"
}

while [ $# -gt 0 ]; do
    case "$1" in
        --fleet) WITH_FLEET=1 ;;
        --uninstall) UNINSTALL=1 ;;
        --purge) PURGE=1 ;;
        --token|--token=*)
            take_value "$@"; TOKEN=$OPTVAL; shift "$OPTSHIFT" ;;
        --client-nic|--client-nic=*)
            take_value "$@"; CLIENT_NIC=$OPTVAL; shift "$OPTSHIFT" ;;
        --backend-nic|--backend-nic=*)
            take_value "$@"; BACKEND_NIC=$OPTVAL; shift "$OPTSHIFT" ;;
        --download)
            # 带 tag = 装指定版本；不带（下一个是选项或没有了）= 最新版。
            DOWNLOAD=1
            if [ $# -ge 2 ]; then
                case "$2" in
                    '') die "--download 后面的 tag 为空——装最新版请直接省略 tag" ;;
                    -*) ;;
                    *)  DOWNLOAD_TAG=$2; shift ;;
                esac
            fi ;;
        -h|--help) usage ;;
        *) die "不认识的参数 '$1'（--token / --client-nic / --backend-nic /" \
               "--fleet / --download [tag] / --uninstall [--purge] / --help）" ;;
    esac
    shift
done

# ---- 校验（下载之前就做：参数错了不该先下几十 MB 再报错） ----------------
[ "$(id -u)" = 0 ] || die "需要 root（写 $PREFIX/bin、$CONF_DIR 与 systemd 单元）"

# 令牌：这里先做不需要二进制的粗检（字符集、长度），明显不行的在下载
# 之前就拒绝；完整的强度规则在装之前用包里的 rl-limiter --check-token
# 校验（规则只在 Python 里写一份，见 token_weakness）。
TOKEN_HINT="生成一个：--token auto，或 openssl rand -hex 32"
if [ -n "$TOKEN" ] && [ "$TOKEN" != auto ]; then
    [[ "$TOKEN" =~ ^[A-Za-z0-9._~+/=-]+$ ]] \
        || die "令牌含不允许的字符（只收字母、数字与 ._~+/=-）——$TOKEN_HINT"
    [ "${#TOKEN}" -ge 32 ] \
        || die "令牌太短（${#TOKEN} 位，至少 32 位高强度随机串）——$TOKEN_HINT"
fi

if [ "$WITH_FLEET" = 1 ] && { [ -n "$CLIENT_NIC" ] || [ -n "$BACKEND_NIC" ]; }; then
    die "--client-nic / --backend-nic 只用于节点安装；总控（--fleet）不碰网卡"
fi

phys_nics() {   # 本机物理网卡（有 device 链接的才是物理设备），空格分隔
    local d out=""
    for d in "$SYS_NET"/*; do
        [ -e "$d/device" ] && out="$out ${d##*/}"
    done
    echo "${out# }"
}

env_get() {     # env_get <文件> <键>：取 systemd EnvironmentFile 里的值
    [ -f "$1" ] || return 0
    sed -n "s/^[[:space:]]*$2=//p" "$1" | tail -1 | tr -d '"'"'"
}

check_nic() {   # check_nic <网卡> <客户侧|后端侧> <对应选项> <本机物理网卡>
    local nic=$1 label=$2 opt=$3
    [[ "$nic" =~ ^[A-Za-z0-9._-]{1,15}$ ]] \
        || die "${label}网卡名 '$nic' 不合法（$opt；1~15 位字母/数字/._-）"
    [ -e "$SYS_NET/$nic" ] \
        || die "${label}网卡 $nic 在本机不存在（本机物理网卡：$4）——用 $opt <网卡名> 指定"
    [ -e "$SYS_NET/$nic/device" ] \
        || warn "${label}网卡 $nic 不是物理网卡（bond/VLAN 等虚拟设备请确认接线无误）"
}

# 节点接线的生效值：命令行 > 已有 env 文件 > 出厂约定（eth0/eth1）。
EFF_CLIENT="" EFF_BACKEND=""
check_node_nics() {
    local -a phys
    read -r -a phys <<<"$(phys_nics)"
    [ "${#phys[@]}" -ge 2 ] || die "节点要求双网卡（客户侧 + 后端侧），本机物理网卡" \
        "只有 ${#phys[@]} 张（${phys[*]:-无}）——不允许安装。总控请用 --fleet 安装"
    log "双网卡检查通过：本机物理网卡 ${phys[*]}"

    local envf="$CONF_DIR/rl-limiter.env"
    EFF_CLIENT=${CLIENT_NIC:-$(env_get "$envf" RL_TC_IN_IFACE)}
    EFF_CLIENT=${EFF_CLIENT:-$DEFAULT_CLIENT_NIC}
    EFF_BACKEND=${BACKEND_NIC:-$(env_get "$envf" RL_BACKEND_IFACE)}
    EFF_BACKEND=${EFF_BACKEND:-$DEFAULT_BACKEND_NIC}

    # 已有 env 里显式关了入向限速（=-）且这次没给 --client-nic：尊重它。
    if [ "$EFF_CLIENT" = "-" ]; then
        warn "rl-limiter.env 里 RL_TC_IN_IFACE=-（入向限速已关闭），不校验客户侧网卡"
    else
        check_nic "$EFF_CLIENT" 客户侧 --client-nic "${phys[*]}"
    fi
    check_nic "$EFF_BACKEND" 后端侧 --backend-nic "${phys[*]}"
    [ "$EFF_CLIENT" != "$EFF_BACKEND" ] \
        || die "客户侧网卡与后端侧网卡不能是同一张（都是 $EFF_CLIENT）——两者相斥：" \
               "入向限速作用在客户侧网卡上，同一张卡会卡住后端回程流量"
    log "接线：客户侧 $EFF_CLIENT（入向限速执行）/ 后端侧 $EFF_BACKEND"
}

# ---- 卸载 ------------------------------------------------------------------
# 顺序：先停服务（否则它的 30s 兜底 reconcile 会把规则建回来）→ 清 tc
# → 删单元 → 两种角色都没了才删二进制。每一步说清楚做了什么、留了什么。
uninstall() {
    local svc=rl-limiter other=rl-fleet envf="$CONF_DIR/rl-limiter.env"
    local files="config.yaml rl-limiter.env"
    if [ "$WITH_FLEET" = 1 ]; then
        svc=rl-fleet other=rl-limiter envf="$CONF_DIR/rl-fleet.env"
        files="fleet.yaml rl-fleet.env"
    fi
    log "卸载 $svc"
    systemctl disable --now "$svc" >/dev/null 2>&1 \
        && log "已停止并禁用 $svc" || log "$svc 未在运行（或 systemctl 不可用）"

    if [ "$WITH_FLEET" != 1 ]; then
        # 接线以 env 文件为准（与 rl-limiter 启动时读的是同一份）。
        local in out ifb
        in=$(env_get "$envf" RL_TC_IN_IFACE)
        out=$(env_get "$envf" RL_TC_IFACE)
        ifb=$(env_get "$envf" RL_TC_IFB); ifb=${ifb:-ifbrl0}
        [ -f "$envf" ] || warn "没有 $envf，不知道入向接在哪张网卡——只清 IFB 设备 $ifb；" \
            "入向钩子请手工：tc qdisc del dev <客户侧网卡> ingress"
        if ! command -v tc >/dev/null 2>&1; then
            warn "找不到 tc，跳过清理限速规则（手工：tc qdisc del dev <网卡> ingress；ip link del $ifb）"
        else
            if [ -n "$in" ] && [ "$in" != "-" ] \
                    && [ -n "$(tc qdisc show dev "$in" ingress 2>/dev/null)" ]; then
                tc qdisc del dev "$in" ingress && log "已撤掉 $in 的入向限速钩子（ingress qdisc）" \
                    || warn "撤 $in 的入向钩子失败——手工：tc qdisc del dev $in ingress"
            fi
            if ip link show "$ifb" >/dev/null 2>&1; then
                ip link del "$ifb" && log "已删除 IFB 设备 $ifb（连同其上的整形树）" \
                    || warn "删 IFB 设备 $ifb 失败——手工：ip link del $ifb"
            fi
            # 出向 HTB 只在显式接了 RL_TC_IFACE 时才是我们建的；只删 htb。
            if [ -n "$out" ] && [ "$out" != "-" ] \
                    && tc qdisc show dev "$out" root 2>/dev/null | grep -q htb; then
                tc qdisc del dev "$out" root && log "已撤掉 $out 的出向 HTB 整形树" \
                    || warn "撤 $out 的出向 HTB 失败——手工：tc qdisc del dev $out root"
            fi
        fi
    fi

    rm -f "$UNIT_DIR/$svc.service"
    systemctl daemon-reload 2>/dev/null || true
    log "已删除 $UNIT_DIR/$svc.service"

    if [ -e "$UNIT_DIR/$other.service" ]; then
        log "本机还装着 $other，保留二进制 $PREFIX/bin/rl-limiter、rl-fleet"
    else
        rm -f "$PREFIX/bin/rl-limiter" "$PREFIX/bin/rl-fleet"
        log "已删除二进制 $PREFIX/bin/rl-limiter、rl-fleet"
    fi

    local f
    if [ "$PURGE" = 1 ]; then
        for f in $files; do rm -f "$CONF_DIR/$f"; done
        log "已删除配置 $CONF_DIR/{${files// /,}}"
        rmdir "$CONF_DIR" 2>/dev/null && log "已删除空目录 $CONF_DIR" || true
    else
        log "保留配置 $CONF_DIR/{${files// /,}}（重装即沿用；要删加 --purge）"
    fi
    log "刻意保留：监控落盘 /var/log/rl-limiter（计费对账数据，要删请手工）、运行用户 $RL_USER"
    log "卸载完成"
}

if [ "$UNINSTALL" = 1 ]; then
    [ "$DOWNLOAD" = 0 ] && [ -z "$TOKEN$CLIENT_NIC$BACKEND_NIC" ] \
        || die "--uninstall 不能与 --download / --token / --client-nic / --backend-nic 同用"
    uninstall
    exit 0
fi
[ "$PURGE" = 0 ] || die "--purge 只用于卸载：--uninstall --purge"

[ "$WITH_FLEET" = 1 ] || check_node_nics

# ---- 模式 2：先下载发布包，再用包里的 install.sh 安装 ---------------------
if [ "$DOWNLOAD" = 1 ]; then
    if [ -z "$DOWNLOAD_TAG" ]; then
        # 跟随 releases/latest 的跳转拿 tag：只用 github.com，不走
        # api.github.com（匿名 60 次/小时限流，批量装机会撞上）。
        if ! url=$(curl -fsSLI --retry 3 -o /dev/null -w '%{url_effective}' \
                "https://github.com/$REPO/releases/latest"); then
            die "查询最新版本失败（github.com 不可达？）——可显式指定版本：--download v<主>.<次>.<补丁>"
        fi
        case "$url" in */releases/tag/*) ;; *)
            die "没查到正式 Release（跳转到了 $url）——可显式指定版本：--download v<主>.<次>.<补丁>" ;;
        esac
        DOWNLOAD_TAG=${url##*/}
        log "最新版本：$DOWNLOAD_TAG"
    fi
    # 文档里常写不带 v 的版本号，这里补上（tag 规则见 docs/11）。
    case "$DOWNLOAD_TAG" in [0-9]*) DOWNLOAD_TAG="v$DOWNLOAD_TAG" ;; esac
    [[ "$DOWNLOAD_TAG" =~ ^v[0-9]+\.[0-9]+\.[0-9]+(-rc[0-9]+)?$ ]] \
        || die "版本 '$DOWNLOAD_TAG' 不符合 tag 规则 v<主>.<次>.<补丁>[-rcN]（如 v0.6.0）"

    ARCH=$(uname -m)
    NAME="rl-limiter-${DOWNLOAD_TAG}-linux-${ARCH}"
    URL="https://github.com/$REPO/releases/download/$DOWNLOAD_TAG/$NAME.tar.gz"
    TMP=$(mktemp -d); trap 'rm -rf "$TMP"' EXIT
    log "下载 $URL"
    curl -fL --retry 3 -o "$TMP/$NAME.tar.gz" "$URL" \
        && curl -fL --retry 3 -o "$TMP/$NAME.tar.gz.sha256" "$URL.sha256" \
        || die "下载失败：版本 $DOWNLOAD_TAG 不存在、没有 $ARCH 架构的发布包，" \
               "或 Release 附件还没上传完（刚打 tag 时等几分钟再试）"
    ( cd "$TMP" && sha256sum -c "$NAME.tar.gz.sha256" ) \
        || die "sha256 校验失败——下载不完整或被篡改，终止安装"
    tar -C "$TMP" -xzf "$TMP/$NAME.tar.gz"
    [ -f "$TMP/$NAME/install.sh" ] || die "发布包里没有 install.sh（包损坏？）"
    args=()
    [ "$WITH_FLEET" = 1 ] && args+=(--fleet)
    [ -n "$TOKEN" ]       && args+=("--token=$TOKEN")
    [ -n "$CLIENT_NIC" ]  && args+=("--client-nic=$CLIENT_NIC")
    [ -n "$BACKEND_NIC" ] && args+=("--backend-nic=$BACKEND_NIC")
    # 不 exec：要等它结束让 EXIT trap 清掉临时目录；用 bash 跑而不是
    # 直接执行，/tmp 挂了 noexec 的加固机器上也能装。
    rc=0
    bash "$TMP/$NAME/install.sh" "${args[@]+"${args[@]}"}" || rc=$?
    exit "$rc"
fi

# ---- 模式 1：从包目录安装 ------------------------------------------------
HERE=$(cd "$(dirname "$0")" && pwd)
[ -f "$HERE/bin/rl-limiter" ] \
    || die "没在发布包目录里（找不到 bin/rl-limiter）。先 tar xzf 解包，" \
           "进包目录再执行；或用 --download [tag] 直接下载安装"

# 令牌强度：调包里的 rl-limiter --check-token（与运行时同一套规则）。
# 先拷到 $PREFIX/bin 下的临时名再跑——包目录可能在挂了 noexec 的 /tmp
# 下（--download 就是）。达标输出空，否则输出原因。
token_weakness() {
    local chk="$PREFIX/bin/.rl-limiter-tokencheck.$$" out
    install -d -m 755 "$PREFIX/bin"
    install -m 700 "$HERE/bin/rl-limiter" "$chk"
    out=$(printf '%s' "$1" | "$chk" --check-token 2>&1) || true
    rm -f "$chk"
    [ "$out" = OK ] || printf '%s' "${out:-无法校验（rl-limiter --check-token 无输出）}"
}

# --token auto：64 位十六进制（256 bit）。生成后照样过一遍规则（真随机
# 几乎不可能不过，过不了就换一个）。
gen_token() {
    local t i
    for i in 1 2 3 4 5; do
        t=$(od -An -tx1 -N32 /dev/urandom | tr -d ' \n')
        [ -z "$(token_weakness "$t")" ] && { printf '%s' "$t"; return 0; }
    done
    die "生成令牌 5 次都没过强度校验（/dev/urandom 异常？）"
}

GENERATED_TOKEN=""
if [ "$TOKEN" = auto ]; then
    TOKEN=$(gen_token)
    GENERATED_TOKEN=$TOKEN
elif [ -n "$TOKEN" ]; then
    why=$(token_weakness "$TOKEN")
    [ -z "$why" ] || die "令牌强度不足：$why。$TOKEN_HINT"
fi

# 已有 env 里的令牌（升级、没给 --token 时沿用）不达标：新版本会当作
# 没有令牌，提前说清楚。
warn_weak_existing() {
    local file=$1 cur why
    [ -n "$TOKEN" ] && return 0
    cur=$(env_get "$file" RL_API_TOKEN)
    [ -n "$cur" ] || return 0
    why=$(token_weakness "$cur")
    [ -z "$why" ] || warn "$file 里现有的 RL_API_TOKEN 强度不足（$why），本版本起会被当作" \
        "没有令牌（写接口禁用）——加 --token auto 重跑生成新令牌"
}

# env_set <文件> <键> <值>：有这个键的生效行就全部改掉（systemd 后写的
# 覆盖先写的，留一行旧值等于没改）；没有生效行就改第一条注释掉的示例
# 行；都没有就追加。awk 只用 index/sub（老版 mawk 不认 [[:space:]]）；
# 用 cat > 回写而不是 mv，保留文件的属主与权限。
env_set() {
    local file=$1 key=$2 val=$3 mode tmp
    if grep -q "^[[:space:]]*$key=" "$file"; then
        mode=live
    elif grep -q "^#[[:space:]]*$key=" "$file"; then
        mode=example
    else
        printf '%s=%s\n' "$key" "$val" >>"$file"; return 0
    fi
    tmp=$(mktemp)
    awk -v k="$key=" -v kv="$key=$val" -v mode="$mode" '
        { s = $0; sub(/^[ \t]*/, "", s) }
        mode == "live" && index(s, k) == 1 { print kv; next }
        mode == "example" && !done && substr(s, 1, 1) == "#" {
            r = substr(s, 2); sub(/^[ \t]*/, "", r)
            if (index(r, k) == 1) { print kv; done = 1; next }
        }
        { print }' "$file" >"$tmp"
    cat "$tmp" >"$file"; rm -f "$tmp"
}

# 配置文件：首次从模板生成；已存在不动，但属组/权限校正到服务用户可
# 读（老版本装出来的是 640 root:root，服务用户读不到、写 API 回写不了）。
# yaml 服务要回写（写 API），给组写；env 只有 systemd 读，给组读。
place_conf() {
    local src=$1 dst=$2 mode=$3
    if [ -e "$dst" ]; then
        log "$dst 已存在，不覆盖（只校正属组/权限）"
    else
        install -m "$mode" "$src" "$dst"
        log "已生成 $dst"
    fi
    chgrp "$RL_USER" "$dst"; chmod "$mode" "$dst"
}

log "安装二进制 → $PREFIX/bin/"
install -d -m 755 "$PREFIX/bin"
install -m 755 "$HERE/bin/rl-limiter" "$PREFIX/bin/rl-limiter"
install -m 755 "$HERE/bin/rl-fleet"   "$PREFIX/bin/rl-fleet"
"$PREFIX/bin/rl-limiter" --version

# 专用运行用户（非 root；tc 权限由 unit 的 CAP_NET_ADMIN 提供）。
if ! id "$RL_USER" >/dev/null 2>&1; then
    log "创建运行用户 $RL_USER"
    useradd --system --user-group --no-create-home --shell /usr/sbin/nologin "$RL_USER"
fi

# 配置目录属组给服务用户并放开组写：写 API 回写 YAML 是"临时文件 +
# 原子替换"，需要**目录**的写权限（unit 里配套 ReadWritePaths）。
log "配置 → $CONF_DIR/（已存在的文件不覆盖）"
install -d -m 2770 "$CONF_DIR"
chgrp "$RL_USER" "$CONF_DIR"; chmod 2770 "$CONF_DIR"

if [ "$WITH_FLEET" = 1 ]; then
    place_conf "$HERE/config/fleet.example.yaml"   "$CONF_DIR/fleet.yaml"   660
    place_conf "$HERE/config/rl-fleet.env.example" "$CONF_DIR/rl-fleet.env" 640
    if [ -n "$TOKEN" ]; then
        env_set "$CONF_DIR/rl-fleet.env" RL_API_TOKEN "$TOKEN"
        log "总控写 API 令牌已写入 rl-fleet.env"
    fi
    warn_weak_existing "$CONF_DIR/rl-fleet.env"
    # 老版本生成的 rl-fleet.env 没有监控落盘这一项（连注释都没有）：补上
    # 默认值——升级后「历史回查」才有数据。注释掉过的说明是有意关的，不动。
    if ! grep -q "RL_METRICS_LOG=" "$CONF_DIR/rl-fleet.env"; then
        printf '\n# 监控落盘（总带宽/限速分钟数据 + 变更事件，页面「历史回查」读它）\n%s\n' \
            "RL_METRICS_LOG=/var/log/rl-limiter/fleet-metrics.jsonl" >>"$CONF_DIR/rl-fleet.env"
        log "rl-fleet.env 补上监控落盘 RL_METRICS_LOG（升级自老版本）"
    fi
    FLEET_TOKEN=$(env_get "$CONF_DIR/rl-fleet.env" RL_API_TOKEN)
    FLEET_PORT=$(env_get "$CONF_DIR/rl-fleet.env" RL_CONSOLE_PORT)

    log "systemd 单元 → $UNIT_DIR/rl-fleet.service"
    install -d -m 755 "$UNIT_DIR"
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
User=$RL_USER
Group=$RL_USER
LogsDirectory=rl-limiter
ReadWritePaths=$CONF_DIR
NoNewPrivileges=yes
ProtectSystem=strict
ProtectHome=yes
PrivateTmp=yes

[Install]
WantedBy=multi-user.target
EOF
else
    ENVF="$CONF_DIR/rl-limiter.env"
    fresh_env=1; [ -e "$ENVF" ] && fresh_env=0
    place_conf "$HERE/config/limiter.example.yaml"   "$CONF_DIR/config.yaml" 660
    place_conf "$HERE/config/rl-limiter.env.example" "$ENVF" 640
    if [ "$fresh_env" = 1 ]; then
        # 新 env：接线一律显式写死（排障时看这个文件就知道限速接在哪）。
        env_set "$ENVF" RL_TC_IN_IFACE   "$EFF_CLIENT"
        env_set "$ENVF" RL_BACKEND_IFACE "$EFF_BACKEND"
        log "网卡接线已写入 rl-limiter.env：客户侧 $EFF_CLIENT / 后端侧 $EFF_BACKEND"
    else
        # 已有 env：只写命令行显式给出的项，其余是运维调过的，不动。
        [ -n "$CLIENT_NIC" ]  && env_set "$ENVF" RL_TC_IN_IFACE   "$CLIENT_NIC"
        [ -n "$BACKEND_NIC" ] && env_set "$ENVF" RL_BACKEND_IFACE "$BACKEND_NIC"
        [ -n "$CLIENT_NIC$BACKEND_NIC" ] && log "网卡接线已更新到 rl-limiter.env"
        [ -n "$(env_get "$ENVF" RL_BACKEND_IFACE)" ] \
            || warn "rl-limiter.env 没登记后端侧网卡（老版本生成）——加 --backend-nic <网卡>" \
                    "重跑即写入，启动时就会校验两卡相斥"
    fi
    if [ -n "$TOKEN" ]; then
        env_set "$ENVF" RL_API_TOKEN "$TOKEN"
        log "节点写 API 令牌已写入 rl-limiter.env"
    fi
    warn_weak_existing "$ENVF"
    NODE_TOKEN=$(env_get "$ENVF" RL_API_TOKEN)
    NODE_PORT=$(env_get "$ENVF" RL_CONSOLE_PORT)

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
User=$RL_USER
Group=$RL_USER
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
fi

systemctl daemon-reload 2>/dev/null || log "提示：systemctl 不可用（容器？），单元文件已就位"

log "完成。下一步："
if [ "$WITH_FLEET" = 1 ]; then
    log "  1. 核对 $CONF_DIR/fleet.yaml（total_egress_mbps 总带宽）与 rl-fleet.env（端口 ${FLEET_PORT:-未设}）"
    log "  2. systemctl enable --now rl-fleet"
    log "  3. 打开 http://<本机>:${FLEET_PORT:-<端口>} → 页头填令牌 →「节点管理」添加节点"
    [ -n "$FLEET_TOKEN" ] || warn "未设总控令牌（--token）：页面只读，改总带宽/添加节点都会被拒"
    [ -z "$GENERATED_TOKEN" ] || log "总控令牌（已写入 rl-fleet.env，页头填它；请妥善保存）：$GENERATED_TOKEN"
else
    log "  1. 核对 $CONF_DIR/config.yaml（instance_quota_mbps 出向目标 / ports / request_limits）"
    log "  2. systemctl enable --now rl-limiter"
    log "  3. 在总控「节点管理」登记本节点：地址 http://<本机内网地址>:${NODE_PORT:-8090}，令牌同 RL_API_TOKEN"
    [ -n "$NODE_TOKEN" ] || warn "未设节点令牌（--token）：写 API 禁用，总控无法向本节点下发配额"
    [ -z "$GENERATED_TOKEN" ] || log "节点令牌（已写入 rl-limiter.env，总控登记本节点时填它；请妥善保存）：$GENERATED_TOKEN"
fi
exit 0
