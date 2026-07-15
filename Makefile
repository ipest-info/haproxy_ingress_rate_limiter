# rl-limiter（Python 3.11 版）开发入口。
#
# 本地演示（详见 docs/03-限速服务运行指南.md，四个终端）：
#   1. python3 tools/fake_haproxy.py --port 19991 --frontends fe_env_a:16,fe_env_b:4
#   2. python3 tools/fake_haproxy.py --port 19992 --frontends fe_env_a:12
#   3. python3 tools/mock_backend.py --addr 127.0.0.1:9090 --config deploy/config/mock-backend-config.json
#   4. rl-limiter -c /tmp/limiter.yaml   （由 deploy/config/limiter.example.yaml 改出）

PY ?= python3

.PHONY: all install test

all: test

# 可编辑安装（含测试依赖 pytest / pytest-asyncio）
install:
	$(PY) -m pip install -e ".[test]"

test:
	$(PY) -m pytest -q
