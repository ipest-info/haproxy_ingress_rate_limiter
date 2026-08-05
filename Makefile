# hap-agg 开发入口。
#
# 本地最小演示（无需 docker，两个终端）：
#   1. python3 tools/fake_haproxy.py --port 19999 --frontends fe_main:2000000
#   2. printf 'targets: [127.0.0.1:19999]\n' > /tmp/agg.yaml
#      hap-agg -c /tmp/agg.yaml
# 完整演示（三台 HAProxy 官方镜像 + 模拟后端 + 压测）：make demo-up

PY ?= python3

.PHONY: all install test demo-up demo-logs demo-down

all: test

install:
	$(PY) -m pip install -e ".[test]"

test:
	$(PY) -m pytest -q

demo-up:
	docker compose up -d --build

demo-logs:
	docker compose logs -f hapagg loadgen

demo-down:
	docker compose down
