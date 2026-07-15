# deploy/docker/Dockerfile —— 构建 rl-limiter 镜像（v3.0，MySQL 配置源）。
#
# 该镜像同时用于三种角色（compose 里以不同 command 区分）：
#   - rl-limiter 主服务：默认 CMD `python -m rl_limiter`；
#   - 两台假 HAProxy：覆盖 command 跑 tools/fake_haproxy.py（复用同一镜像，
#     省一次构建）。
# MySQL 模式由环境变量 RL_MYSQL_HOST 触发，无需 -c 指定 YAML 配置。

FROM python:3.11-slim

WORKDIR /app

# 先拷贝依赖清单与源码，再安装：安装 .[mysql] 会额外拉入 PyMySQL（纯 Python
# 驱动，无需编译）。PyYAML/aiohttp 为基础依赖。
COPY pyproject.toml ./
COPY rl_limiter/ ./rl_limiter/
COPY tools/ ./tools/

RUN pip install --no-cache-dir ".[mysql]"

# 默认以模块方式启动主服务；检测到 RL_MYSQL_HOST 即进入 MySQL 配置源模式。
CMD ["python", "-m", "rl_limiter"]
