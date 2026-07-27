# deploy/docker/Dockerfile.node —— 一台"HAProxy 节点"的镜像：
# **Ubuntu 24.04 LTS + 发行版自带 HAProxy + 同机的 rl-limiter**。
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
# HTTPS 取 wheel；procps：入口脚本用 pkill 给 haproxy 发 reload 信号。
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        haproxy \
        python3 \
        python3-venv \
        ca-certificates \
        procps \
    && rm -rf /var/lib/apt/lists/* \
    && haproxy -v \
    # 版本闸门：shared bwlim 需要 >= 2.8，低于此值限速会静默失效。
    && haproxy -v | head -1 | grep -Eq 'version 2\.(8|9)|version [3-9]\.' \
    && echo "haproxy 版本满足 shared bwlim 要求 (>=2.8)"

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
RUN chmod +x /usr/local/bin/node-entrypoint.sh

ENTRYPOINT ["/usr/local/bin/node-entrypoint.sh"]
