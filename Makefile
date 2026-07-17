# rl-limiter（Python 3.11 版）开发入口。
#
# 本地最小演示（详见 docs/03-限速服务运行指南.md，三个终端）：
#   1. python3 tools/fake_haproxy.py --port 19991 --frontends fe_env_a:2000000
#   2. python3 tools/fake_haproxy.py --port 19992 --frontends fe_env_b:500000
#   3. rl-limiter -c /tmp/limiter.yaml   （由 deploy/config/limiter.example.yaml 改出）
# 完整演示（MySQL 配置 + 真实 HAProxy L4 + Web 控制台）：make demo-up（docs/04）

PY ?= python3

.PHONY: all install test demo-up demo-logs demo-down

all: test

# 可编辑安装（含测试依赖 pytest / pytest-asyncio）
install:
	$(PY) -m pip install -e ".[test]"

test:
	$(PY) -m pytest -q

# docker compose 一键演示环境（MySQL 配置 + 真实 HAProxy + 模拟后端 +
# 可调并发压测），玩法详见 docs/04-DockerCompose演示.md。
demo-up:
	docker compose up -d --build

demo-logs:
	docker compose logs -f rl-limiter loadgen

demo-down:
	docker compose down -v
