# 本仓库的统一镜像（LVS 分支）：**Ubuntu 24.04 LTS + ipvsadm/keepalived
# + 同机的 rl-limiter**。docker compose 里所有自建服务都用它，靠"给不给
# command"区分两种角色：
#
#   node1/2    不给 command → 走 ENTRYPOINT，即"LVS director"角色：
#              容器内跑 keepalived（VRRP 主备 + virtual_server 下发）
#              与 rl-limiter（同机部署）
#   web        command: python3 tools/random_web.py ...
#   loadgen    command: python3 tools/loadgen.py ...
#              → 这两个只借用镜像里的 Python 环境。入口脚本发现有参数就
#                exec 它（`command:` 覆盖的是 CMD 而非 ENTRYPOINT）。
#
# 内核前提：ip_vs 模块必须由**宿主机**加载（modprobe ip_vs ip_vs_rr），
# 容器里 modprobe 不了；入口脚本启动时自检，缺了大声失败。
#
# Python 依赖装进独立 venv：Ubuntu 24.04 起系统 Python 受 PEP 668 保护
# （pip 直接装会被拒），venv 既绕开该限制又不污染系统解释器，与生产上
# 的部署方式一致。

FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive

# ipvsadm：IPVS 管理与 --stats 采样；keepalived：VRRP 主备 + 下发
# virtual_server；python3-venv：装 rl-limiter；ca-certificates：pip 走
# HTTPS 取 wheel；procps：入口脚本进程管理；
# iproute2：**限速本体**——限额由内核 tc（HTB）执行，没有它 rl-limiter
# 一启动就会因为找不到 tc 而下发失败（见 rl_limiter/tcshaper.py）。
#
# 排障工具（进容器手工用，程序不依赖）：net-tools（ifconfig/netstat，
# 双网卡形态下核对哪张网卡在哪个网段）、iputils-ping（ping，容器间连
# 通性）、curl（容器内直接打本机 HAProxy / 控制台接口）。基础镜像一个
# 都不带，真排障时现装还没有网络权限的话就只能干瞪眼。
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ipvsadm \
        keepalived \
        python3 \
        python3-venv \
        ca-certificates \
        procps \
        iproute2 \
        net-tools \
        iputils-ping \
        curl \
    && rm -rf /var/lib/apt/lists/* \
    && ipvsadm --version \
    && keepalived --version \
    # 限速闸门：tc 必须在，且必须能解析 HTB——只装 iproute2 是不够的，
    # 内核还得有 sch_htb。这里只能验证用户态工具，内核侧的检查在
    # 入口脚本里做（那时才拿得到真实网卡）。
    && tc -V

WORKDIR /app

# pip 镜像源（可选）：PyPI 直连慢/不稳的网络环境下，构建时指定国内镜像：
#   docker compose -f docker-compose-demo.yml build \
#       --build-arg PIP_INDEX_URL=https://pypi.tuna.tsinghua.edu.cn/simple
# 不传则走默认源。pip 原生识别 PIP_INDEX_URL 环境变量，空值等于没设。
# 注意：构建报 "Temporary failure in name resolution" 是 Docker 构建
# 环境的 DNS 坏了（连域名都解析不了），换镜像源救不了——先重启
# Docker Desktop / 给 daemon 配 dns。
ARG PIP_INDEX_URL=
ENV PIP_INDEX_URL=${PIP_INDEX_URL}

COPY pyproject.toml ./
COPY rl_limiter ./rl_limiter
COPY tools ./tools

# 独立 venv（PEP 668）。把 venv 的 bin 放进 PATH，后续可直接调 rl-limiter。
RUN python3 -m venv /opt/rl-limiter \
    && /opt/rl-limiter/bin/pip install --no-cache-dir --upgrade pip \
    && /opt/rl-limiter/bin/pip install --no-cache-dir .
ENV PATH=/opt/rl-limiter/bin:$PATH

# /etc/keepalived：入口脚本从模板生成的 keepalived.conf 放这里
#   （改 cfg 后的 reload 靠它给 master 发 SIGUSR2——容器里没有 systemd）。
# /etc/rl-limiter：只读挂进来的 cfg/YAML 模板；入口脚本把它们复制成
#   容器内的普通文件（进容器改副本即可演示热生效，见入口脚本注释）。
RUN mkdir -p /etc/keepalived /etc/rl-limiter

COPY deploy/docker/node-entrypoint.sh /usr/local/bin/node-entrypoint.sh
# 内核参数调优表。入口脚本在启动 HAProxy 之前跑它——发行版的默认值
# （somaxconn 4096、tcp_max_syn_backlog 1024、netdev_max_backlog 1000…）
# 会在 HAProxy 的 maxconn/backlog 下面先一步成为瓶颈，且不会有任何告警。
# 单独放一份可执行的，运维在宿主机上也能直接跑 check / dump。
COPY deploy/sysctl/tune-kernel.sh /usr/local/bin/tune-kernel.sh
RUN chmod +x /usr/local/bin/node-entrypoint.sh /usr/local/bin/tune-kernel.sh

# 不带参数 = HAProxy 节点角色；带参数（compose 的 command:）= 直接执行
# 那条命令，见入口脚本开头的 exec "$@" 分支。
ENTRYPOINT ["/usr/local/bin/node-entrypoint.sh"]
