# 本仓库的统一镜像：**Ubuntu 24.04 LTS + 发行版自带 HAProxy +
# 同机的 rl-limiter**。docker compose 里所有自建服务都用它，靠"给不给
# command"区分两种角色：
#
#   node1/2/3  不给 command → 走 ENTRYPOINT，即"HAProxy 节点"角色：
#              容器内同时跑 haproxy 与 rl-limiter（同机部署）
#   web        command: python3 tools/random_web.py ...
#   loadgen    command: python3 tools/loadgen.py ...
#              → 这两个只借用镜像里的 Python 环境。入口脚本发现有参数就
#                exec 它，不会把 haproxy 也拉起来（`command:` 覆盖的是
#                CMD 而非 ENTRYPOINT，没有那个分支它们会跑成节点）。
#
# 为什么不用 haproxy 官方镜像：官方镜像只有 haproxy 一个进程，而本项目
# 的部署形态是"rl-limiter 与 HAProxy 装在同一台服务器上"——用 Ubuntu
# 基础镜像装两个组件，容器形态才和生产的单台 ECS 一致（同一个文件系统、
# 同一个 /run/haproxy/admin.sock、同一份 systemd 式的进程共存关系）。
#
# HAProxy 版本：Ubuntu 24.04(noble) 主仓的 haproxy 是 2.8.x LTS，正好
# 满足 shared bwlim 的下限要求（bwlim 2.7 实验、2.8 起正式）。构建时会
# 打印实测版本并在低于 2.8 时**直接让构建失败**，避免"镜像建出来了、
# 限速却静默失效"。
#
# Python 依赖装进独立 venv：Ubuntu 24.04 起系统 Python 受 PEP 668 保护
# （pip 直接装会被拒），venv 既绕开该限制又不污染系统解释器，与生产上
# 的部署方式一致。

FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive

# haproxy：数据面；python3-venv：装 rl-limiter；ca-certificates：pip 走
# HTTPS 取 wheel；procps：入口脚本用 pkill 给 haproxy 发 reload 信号；
# iproute2：**限速本体**——限额由内核 tc（HTB）执行，没有它 rl-limiter
# 一启动就会因为找不到 tc 而下发失败（见 rl_limiter/tcshaper.py）。
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        haproxy \
        python3 \
        python3-venv \
        ca-certificates \
        procps \
        iproute2 \
    && rm -rf /var/lib/apt/lists/* \
    && haproxy -v \
    # 版本闸门：本项目的受管区块用到 option splice-*（2.8 起稳定可用），
    # 且监控依赖 h1_open_* 等 2.8 才有的 stats 列。
    && haproxy -v | head -1 | grep -Eq 'version 2\.(8|9)|version [3-9]\.' \
    && echo "haproxy 版本满足要求 (>=2.8)" \
    # 限速闸门：tc 必须在，且必须能解析 HTB——只装 iproute2 是不够的，
    # 内核还得有 sch_htb。这里只能验证用户态工具，内核侧的检查在
    # 入口脚本里做（那时才拿得到真实网卡）。
    && tc -V

WORKDIR /app

COPY pyproject.toml ./
COPY rl_limiter ./rl_limiter
COPY tools ./tools

# 独立 venv（PEP 668）。把 venv 的 bin 放进 PATH，后续可直接调 rl-limiter。
RUN python3 -m venv /opt/rl-limiter \
    && /opt/rl-limiter/bin/pip install --no-cache-dir --upgrade pip \
    && /opt/rl-limiter/bin/pip install --no-cache-dir .
ENV PATH=/opt/rl-limiter/bin:$PATH

# /run/haproxy：unix stats socket（rl-limiter 同机只读采样）+ master pid
#   （限额自动应用靠它给 master 发 SIGUSR2 触发 reload）。
# /etc/rl-limiter：只读挂进来的 cfg 模板；入口脚本把它复制成
#   /etc/haproxy/haproxy.cfg —— 那份必须**可写**，因为限额自动应用要
#   原地改写它（bind mount 的单文件无法被 rename 覆盖，见入口脚本注释）。
RUN mkdir -p /run/haproxy /etc/rl-limiter /etc/haproxy

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
