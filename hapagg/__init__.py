# hapagg —— 多台 HAProxy 的监控聚合。
#
# 通过各台 haproxy.cfg 里的
#     stats socket ipv4@*:9999 level admin expose-fd listeners
# 批量拉取 `show stat` / `show info`，把 N 台合并成**一个视图**。
#
# 与本仓库其它部分（rl_limiter / netlimit）不共享代码与配置，可以单独拿走。
# 它是**纯只读**的：只发 show stat / show info 两条命令，不改任何运行期状态。
