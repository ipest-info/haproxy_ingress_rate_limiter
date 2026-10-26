# 本仓库的统一镜像（通用形态）：**Ubuntu 24.04 LTS + rl-limiter**。
# 不装任何被监控的服务——本分支的定位就是"与上面跑什么无关"。
# docker compose 里所有自建服务都用它，靠"给不给 command"区分角色：
#
#   node1/2  不给 command → 走 ENTRYPOINT，即"服务器节点"角色：
#            容器内跑 rl-limiter + （APP_CMD 指定的）业务进程示例
#            ——演示里业务进程是 tools/random_web.py，生产上可以是
#            nginx、自研 TCP 服务……或者什么都不是
#   loadgen  command: python3 tools/loadgen.py ...
#   fleet    command: rl-fleet -c ...
#            → 只借用镜像里的 Python 环境。入口脚本发现有参数就 exec
#              它（`command:` 覆盖的是 CMD 而非 ENTRYPOINT）。
#
# Python 依赖装进独立 venv：Ubuntu 24.04 起系统 Python 受 PEP 668 保护，
# venv 既绕开该限制又不污染系统解释器，与生产上的部署方式一致。

FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive

# python3-venv：装 rl-limiter；ca-certificates：pip 走 HTTPS 取 wheel；
# iproute2：**限速本体**——限额由内核 tc（HTB）执行；procps/net-tools/
# iputils-ping/curl：排障工具（进容器手工用，程序不依赖）。
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        python3 \
        python3-venv \
        ca-certificates \
        procps \
        iproute2 \
        net-tools \
        iputils-ping \
        curl \
    && rm -rf /var/lib/apt/lists/* \
    && tc -V

WORKDIR /app

# pip 镜像源（可选）：PyPI 直连慢/不稳的网络环境下，构建时指定国内镜像：
#   docker compose -f docker-compose-demo.yml build \
#       --build-arg PIP_INDEX_URL=https://pypi.tuna.tsinghua.edu.cn/simple
ARG PIP_INDEX_URL=
ENV PIP_INDEX_URL=${PIP_INDEX_URL}

COPY pyproject.toml ./
COPY rl_limiter ./rl_limiter
COPY tools ./tools

RUN python3 -m venv /opt/rl-limiter \
    && /opt/rl-limiter/bin/pip install --no-cache-dir --upgrade pip \
    && /opt/rl-limiter/bin/pip install --no-cache-dir .
ENV PATH=/opt/rl-limiter/bin:$PATH

# /etc/rl-limiter：只读挂进来的 YAML 模板；入口脚本把它复制成容器内的
# 普通文件（进容器改副本即可演示热生效，见入口脚本注释）。
RUN mkdir -p /etc/rl-limiter

COPY deploy/docker/node-entrypoint.sh /usr/local/bin/node-entrypoint.sh
# 内核参数调优表：入口脚本在启动业务进程之前跑它（somaxconn 等发行版
# 默认值会在高并发下先一步成为瓶颈）。
COPY deploy/sysctl/tune-kernel.sh /usr/local/bin/tune-kernel.sh
RUN chmod +x /usr/local/bin/node-entrypoint.sh /usr/local/bin/tune-kernel.sh

# 不带参数 = 服务器节点角色；带参数（compose 的 command:）= 直接执行。
ENTRYPOINT ["/usr/local/bin/node-entrypoint.sh"]
