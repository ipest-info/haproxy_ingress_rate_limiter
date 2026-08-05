# hap-agg 演示/部署镜像：python-slim + 本包。demo compose 里三个自建
# 服务共用它：hapagg（聚合器本体）、web（模拟后端）、loadgen（压测）。
# HAProxy 节点用官方镜像（hap-agg 是纯只读观测，目标机器零安装——
# demo 拓扑刻意保持这一点）。
FROM python:3.11-slim

WORKDIR /app
COPY pyproject.toml ./
COPY hap_agg ./hap_agg
COPY tools ./tools
RUN pip install --no-cache-dir .

EXPOSE 8100
CMD ["hap-agg", "-c", "/etc/hap-agg/config.yaml", "--port", "8100", "--bind", "0.0.0.0"]
