# rl-limiter（Python 3.11 版）开发入口。
#
# 本地最小演示（详见 docs/03-限速服务运行指南.md，两个终端，同机形态）：
#   1. python3 tools/fake_haproxy.py --unix-path /tmp/rl/hap1.sock --frontends fe_main:2000000
#   2. rl-limiter -c /tmp/limiter.yaml
#      （配置由 deploy/config/limiter.example.yaml 改出，cfg_path 指向一份
#       含对应 frontend 段的 haproxy.cfg）
# 完整演示（三台 Ubuntu 24.04 节点，每台 HAProxy + 同机 rl-limiter +
# Web 控制台 + 模拟后端 + 压测）：make demo-up（docs/04）

PY ?= python3

.PHONY: all install test demo-up demo-logs demo-down bare-install bare-check

all: test

# 可编辑安装（含测试依赖 pytest / pytest-asyncio）
install:
	$(PY) -m pip install -e ".[test]"

test:
	$(PY) -m pytest -q

# docker compose 一键演示环境（三台 Ubuntu 24.04 节点，每台 = HAProxy +
# 同机 rl-limiter + 模拟后端 + 可调并发压测），配置全在两份本地文件里
#（haproxy-base.cfg + limiter-node.yaml），玩法详见 docs/04。
demo-up:
	docker compose -f docker-compose-demo.yml up -d --build

demo-logs:
	docker compose -f docker-compose-demo.yml logs -f node1 node2 node3 loadgen

demo-down:
	docker compose -f docker-compose-demo.yml down

# ---------------------------------------------------------------------------
# 裸机部署（HAProxy 与 rl-limiter 都跑在本机，配置就是本机两份文件）
# 详见 docs/09-裸机部署.md。前提：本机 HAProxy 已装好。
# ---------------------------------------------------------------------------
bare-install:
	sudo deploy/bare/rl-limiter.sh install

bare-check:
	deploy/bare/rl-limiter.sh check
