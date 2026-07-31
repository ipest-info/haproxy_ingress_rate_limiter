# rl-limiter（Python 3.11 版）开发入口。
#
# 本地最小演示（详见 docs/03-限速服务运行指南.md，两个终端，同机形态）：
#   1. python3 tools/fake_haproxy.py --unix-path /tmp/rl/hap1.sock --frontends fe_main:2000000
#   2. RL_NODE_NAME=hap-1 rl-limiter -c /tmp/limiter.yaml
#      （配置由 deploy/config/limiter.example.yaml 改出）
# 完整演示（MySQL 配置 + 三台 Ubuntu 24.04 节点，每台 HAProxy + 同机
# rl-limiter + Web 控制台）：make demo-up（docs/04）

PY ?= python3

.PHONY: all install test demo-up demo-logs demo-down bare-install bare-check bare-db-up bare-db-down

all: test

# 可编辑安装（含测试依赖 pytest / pytest-asyncio）
install:
	$(PY) -m pip install -e ".[test]"

test:
	$(PY) -m pytest -q

# docker compose 一键演示环境（MySQL 配置 + 三台 Ubuntu 24.04 节点，
# 每台 = HAProxy + 同机 rl-limiter + 模拟后端 + 可调并发压测），
# 玩法详见 docs/04-DockerCompose演示.md。
demo-up:
	docker compose up -d --build

demo-logs:
	docker compose logs -f node1 node2 node3 loadgen

demo-down:
	docker compose down -v

# ---------------------------------------------------------------------------
# 裸机部署（HAProxy 与 rl-limiter 都跑在本机，只有配置库用 Docker）
# 详见 docs/09-裸机部署.md。前提：本机 HAProxy 已装好。
# ---------------------------------------------------------------------------
bare-install:
	sudo deploy/bare/rl-limiter.sh install

bare-check:
	deploy/bare/rl-limiter.sh check

bare-db-up:
	deploy/bare/rl-limiter.sh mysql-up

bare-db-down:
	deploy/bare/rl-limiter.sh mysql-down
