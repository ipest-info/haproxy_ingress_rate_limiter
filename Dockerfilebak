# rl-limiter 及其配套联调工具的统一镜像。
#
# docker compose 里三个服务共用本镜像，仅 command 不同：
#   rl-limiter  →  rl-limiter（默认 CMD；配置经 RL_MYSQL_* 环境变量注入）
#   web         →  python3 tools/random_web.py ...（随机大小响应的模拟后端）
#   loadgen     →  python3 tools/loadgen.py ...（可调并发的压测服务）

FROM python:3.11-slim

WORKDIR /app

# 先拷元数据再拷代码没有意义——本包源码即依赖声明的一部分（setuptools
# 需要 rl_limiter/ 存在才能构建），直接整体拷贝安装。
COPY pyproject.toml ./
COPY rl_limiter ./rl_limiter
COPY tools ./tools

RUN pip install --no-cache-dir .

CMD ["rl-limiter"]
