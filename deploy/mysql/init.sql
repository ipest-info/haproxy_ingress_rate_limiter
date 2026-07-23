-- deploy/mysql/init.sql —— rl-limiter 配置库的建表与演示种子数据。
--
-- 该文件由 mysql 官方镜像的 /docker-entrypoint-initdb.d 机制在数据库
-- **首次初始化**时自动执行（数据卷已存在时不会重跑）。四张表与
-- rl_limiter/dbconfig.py 的查询一一对应，行被组装成与 YAML 同构的
-- 配置后走同一套校验管线。
--
-- 架构定位：限速由各节点 HAProxy 的 shared bwlim（聚合限速，配置常量
-- + reload 调整）执行；本库登记的是 rl-limiter 集中监控所需的信息——
-- 节点接线、节点限额（=超限告警基准，应与该节点 haproxy.cfg 里 shared
-- bwlim 的 limit 一致）、环境分组与挂载点。
--
-- 热更新说明：rl-limiter 每 RL_MYSQL_POLL_S 秒轮询一次全量配置，
-- 节点的 quota_bps 与挂载点归属改表即热生效，无需重启；节点接线列
-- （host/port/timeout）与 service_config 的 log_level/tick_interval_s
-- 在进程启动时定型，改表后需重启。

-- ---------------------------------------------------------------------------
-- 1. service_config：服务级配置（单行表，id 恒为 1）。
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS service_config (
    id              TINYINT UNSIGNED NOT NULL PRIMARY KEY,
    -- debug | info | warn | error
    log_level       VARCHAR(16)  NOT NULL DEFAULT 'info',
    -- 采样节拍（秒），1.0 是设计基准。
    tick_interval_s DOUBLE       NOT NULL DEFAULT 1.0,
    updated_at      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
                                 ON UPDATE CURRENT_TIMESTAMP,
    CONSTRAINT service_config_single_row CHECK (id = 1)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ---------------------------------------------------------------------------
-- 2. haproxy_nodes：受控 HAProxy 节点清单。
--    host:port 指向各节点的内网 TCP stats socket（level user 即够，
--    rl-limiter 只做只读采样）。
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS haproxy_nodes (
    name            VARCHAR(64)  NOT NULL PRIMARY KEY,
    host            VARCHAR(255) NOT NULL,
    port            INT UNSIGNED NOT NULL,
    -- 单次 runtime API 命令超时（连接 + 读写，毫秒）；<=0 按默认 500 处理。
    timeout_ms      INT          NOT NULL DEFAULT 500,
    -- 节点登记限额（bits/s，运维口径，40000000 = 40 Mbps）——超限告警
    -- 基准，**可热更**。应与该节点 haproxy.cfg 里 shared bwlim 的 limit
    -- 保持一致（发布流程保证；实测持续超过此值会触发持续超限告警）。
    -- 被挂载（env_targets 引用）的节点必须设置为 > 0。
    quota_bps       BIGINT UNSIGNED NULL DEFAULT NULL,
    updated_at      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
                                 ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ---------------------------------------------------------------------------
-- 3. envs：业务环境分组（仅分组标识——限额登记在 haproxy_nodes；环境
--    只用于控制台聚合查看成员节点带宽之和）。
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS envs (
    env_id      VARCHAR(64)     NOT NULL PRIMARY KEY,
    updated_at  TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP
                                ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ---------------------------------------------------------------------------
-- 4. env_targets：环境挂载点（环境 × 节点 × frontend）。
--    同一 (node, frontend) 只能属于一个环境（唯一键强制），与配置校验
--    的拒绝规则一致：一对多映射会导致流量重复计入。
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS env_targets (
    env_id   VARCHAR(64) NOT NULL,
    node     VARCHAR(64) NOT NULL,
    frontend VARCHAR(64) NOT NULL,
    PRIMARY KEY (env_id, node, frontend),
    UNIQUE KEY uk_target_owner (node, frontend),
    CONSTRAINT fk_targets_env  FOREIGN KEY (env_id) REFERENCES envs (env_id)
        ON DELETE CASCADE,
    CONSTRAINT fk_targets_node FOREIGN KEY (node) REFERENCES haproxy_nodes (name)
        ON DELETE RESTRICT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ===========================================================================
-- 演示种子数据（与 docker-compose.yml 的服务拓扑一一对应）
-- ===========================================================================

INSERT INTO service_config (id, log_level, tick_interval_s)
VALUES (1, 'info', 1.0);

-- compose 里的三台 HAProxy：容器内 9999 端口为 TCP stats socket。
-- 归属约定（校验强制）：节点是环境的独占资源——一个环境可横跨多台
-- HAProxy，但一台 HAProxy 只允许服务一个环境。
-- 每台节点登记限额 40 Mbps，与 deploy/docker/haproxy-env-*.cfg 里
-- shared bwlim 的 limit（5_000_000 bytes/s）一致。
INSERT INTO haproxy_nodes (name, host, port, timeout_ms, quota_bps)
VALUES ('hap-1', 'haproxy1', 9999, 500, 40000000),
       ('hap-2', 'haproxy2', 9999, 500, 40000000),
       ('hap-3', 'haproxy3', 9999, 500, 40000000);

-- 演示环境两套（纯分组）：env-a = hap-1 + hap-2，env-b = hap-3。
INSERT INTO envs (env_id)
VALUES ('env-a'),
       ('env-b');

INSERT INTO env_targets (env_id, node, frontend)
VALUES ('env-a', 'hap-1', 'fe_env_a'),
       ('env-a', 'hap-2', 'fe_env_a'),
       ('env-b', 'hap-3', 'fe_env_b');

-- ===========================================================================
-- 运行期常用操作速查（在宿主机执行）
--
--   进入 mysql： docker compose exec mysql mysql -url -prl_pass rl_limiter
--
--   调整某台节点的限额（40 Mbps → 20 Mbps）——两步，缺一不可：
--     1) 更新监控基准（改表即热生效）：
--        UPDATE haproxy_nodes SET quota_bps = 20000000 WHERE name = 'hap-1';
--     2) 同步真实限速（原地改该节点 cfg 的 limit 为 2500000 bytes/s
--        后 reload）：
--        docker kill -s HUP <haproxy 容器>
--        （生产：改 haproxy.cfg + systemctl reload haproxy）
--     只做第 1 步不做第 2 步 → rl-limiter 打持续超限告警。
-- ===========================================================================
