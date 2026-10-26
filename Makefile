# rl-limiter（Python 3.11 版）开发入口。
#
# 本地最小演示（通用形态，一个终端即可）：
#   rl-limiter -c /tmp/limiter.yaml
#   （配置由 deploy/config/limiter.example.yaml 改出；不依赖机器上跑的
#    任何服务——监控=双网卡+内核计数器，限速=客户端侧网卡入向 tc）
# 完整演示（两台节点 + 压测 + 总控）：make demo-up（docs/05）

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
#（haproxy-base.cfg + limiter-node.yaml），玩法详见 docs/05。
demo-up:
	docker compose -f docker-compose-demo.yml up -d --build

demo-logs:
	docker compose -f docker-compose-demo.yml logs -f node1 node2 node3 loadgen

demo-down:
	docker compose -f docker-compose-demo.yml down

# ---------------------------------------------------------------------------
# 裸机部署（HAProxy 与 rl-limiter 都跑在本机，配置就是本机两份文件）
# 详见 docs/04-部署指南.md。前提：本机 HAProxy 已装好。
# ---------------------------------------------------------------------------
bare-install:
	sudo deploy/bare/rl-limiter.sh install

bare-check:
	deploy/bare/rl-limiter.sh check

# ---------------------------------------------------------------------------
# 发布：构建单文件二进制发布包（PyInstaller）。正式发布走 CI（打 tag
# 自动构建上传 Release），规则与安装见 docs/11-发布与安装.md。
# ---------------------------------------------------------------------------
release-build:
	bash deploy/release/build.sh
