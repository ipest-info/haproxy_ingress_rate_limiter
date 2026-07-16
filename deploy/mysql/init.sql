-- deploy/mysql/init.sql —— rl-limiter 配置库的建表与演示种子数据。
--
-- 该文件由 mysql 官方镜像的 /docker-entrypoint-initdb.d 机制在数据库
-- **首次初始化**时自动执行（数据卷已存在时不会重跑）。四张表与
-- rl_limiter/dbconfig.py 的查询一一对应，行被组装成与 YAML 同构的
-- 配置后走同一套校验管线。
--
-- 热更新说明：rl-limiter 每 RL_MYSQL_POLL_S 秒轮询一次全量配置，
-- mode / envs（配额、挂载点、参数覆盖）改表即热生效，无需重启；
-- haproxy_nodes（节点接线）与 service_config 的 node_id/log_level/
-- tick_interval_s 在进程启动时定型，改表后需重启 rl-limiter。

-- ---------------------------------------------------------------------------
-- 1. service_config：服务级配置（单行表，id 恒为 1）。
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS service_config (
    id              TINYINT UNSIGNED NOT NULL PRIMARY KEY,
    -- 本服务实例的唯一标识（心跳/上报归属）。
    node_id         VARCHAR(64)  NOT NULL,
    -- dry-run = 只算不写（观测模式）；enforce = 真实下发限速。
    mode            VARCHAR(16)  NOT NULL DEFAULT 'dry-run',
    -- debug | info | warn | error
    log_level       VARCHAR(16)  NOT NULL DEFAULT 'info',
    -- 快环节拍（秒），1.0 是设计基准。
    tick_interval_s DOUBLE       NOT NULL DEFAULT 1.0,
    updated_at      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
                                 ON UPDATE CURRENT_TIMESTAMP,
    CONSTRAINT service_config_single_row CHECK (id = 1)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ---------------------------------------------------------------------------
-- 2. haproxy_nodes：受控 HAProxy 节点清单（基础设施接线）。
--    host:port 指向各节点的内网 TCP stats socket（level admin），
--    bwlim_map_path 必须与该节点 haproxy.cfg 里 map_str_int(...) 引用
--    的路径一致，否则限速值写了也不生效。
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS haproxy_nodes (
    name            VARCHAR(64)  NOT NULL PRIMARY KEY,
    host            VARCHAR(255) NOT NULL,
    port            INT UNSIGNED NOT NULL,
    bwlim_map_path  VARCHAR(255) NOT NULL DEFAULT '/etc/haproxy/maps/bwlim.map',
    -- 单次 runtime API 命令超时（连接 + 读写，毫秒）；<=0 按默认 500 处理。
    timeout_ms      INT          NOT NULL DEFAULT 500,
    -- 节点级模式覆盖：NULL = 继承全局 service_config.mode；
    -- 'dry-run'/'enforce' = 覆盖。生产灰度用：逐台节点打开 enforce。
    -- 本列是节点行里唯一**热生效**的列（其余为接线字段，改后需重启）。
    mode            VARCHAR(16)  NULL DEFAULT NULL,
    updated_at      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
                                 ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ---------------------------------------------------------------------------
-- 3. envs：环境配额。quota_bps 单位为比特每秒（运维口径，
--    80000000 = 80 Mbps）；params_json 为可选的快环参数覆盖（JSON 对象，
--    字段见 rl_limiter/model.py 的 GovParams，NULL/空串表示全用默认值）。
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS envs (
    env_id      VARCHAR(64)     NOT NULL PRIMARY KEY,
    quota_bps   BIGINT UNSIGNED NOT NULL,
    params_json TEXT            NULL,
    updated_at  TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP
                                ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ---------------------------------------------------------------------------
-- 4. env_targets：环境挂载点（环境 × 节点 × frontend）。
--    同一 (node, frontend) 只能属于一个环境（唯一键强制），与配置校验
--    的拒绝规则一致：一对多映射会导致流量重复计费、限速值互相覆盖。
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

-- 演示直接用 enforce：compose 环境是隔离沙盒，观察限速真实生效才是目的。
-- 生产部署建议先 dry-run 观察，再 UPDATE mode='enforce' 热切换。
INSERT INTO service_config (id, node_id, mode, log_level, tick_interval_s)
VALUES (1, 'rl-limiter-01', 'enforce', 'info', 1.0);

-- compose 里的两台 HAProxy：容器内 9999 端口为 admin 级 TCP stats socket。
-- mode 为 NULL = 继承全局模式；演示逐节点灰度时改这一列即可。
INSERT INTO haproxy_nodes (name, host, port, bwlim_map_path, timeout_ms, mode)
VALUES ('hap-1', 'haproxy1', 9999, '/etc/haproxy/maps/bwlim.map', 500, NULL),
       ('hap-2', 'haproxy2', 9999, '/etc/haproxy/maps/bwlim.map', 500, NULL);

-- 演示环境两套（展示跨节点全局聚合限速）：
--   env-a：80 Mbps，挂载在两台 HAProxy 的 fe_env_a 上——两台的流量
--          全局聚合后统一限速，压一台另一台会自动多分到份额；
--   env-b：40 Mbps，同样横跨两台的 fe_env_b。
INSERT INTO envs (env_id, quota_bps, params_json)
VALUES ('env-a', 80000000, NULL),
       ('env-b', 40000000, NULL);

INSERT INTO env_targets (env_id, node, frontend)
VALUES ('env-a', 'hap-1', 'fe_env_a'),
       ('env-a', 'hap-2', 'fe_env_a'),
       ('env-b', 'hap-1', 'fe_env_b'),
       ('env-b', 'hap-2', 'fe_env_b');

-- ===========================================================================
-- 运行期常用操作速查（在宿主机执行；改完等一个轮询周期即热生效）
--
--   进入 mysql： docker compose exec mysql mysql -url -prl_pass rl_limiter
--
--   调整配额（80 Mbps → 40 Mbps）：
--     UPDATE envs SET quota_bps = 40000000 WHERE env_id = 'env-a';
--
--   dry-run / enforce 全局默认热切换：
--     UPDATE service_config SET mode = 'dry-run' WHERE id = 1;
--
--   按节点覆盖模式（生产灰度：先只放 hap-1 真实生效）：
--     UPDATE haproxy_nodes SET mode = 'enforce' WHERE name = 'hap-1';
--     UPDATE haproxy_nodes SET mode = NULL WHERE name = 'hap-1';  -- 恢复继承
--
--   按环境覆盖快环参数（示例：收紧更狠、恢复更慢）：
--     UPDATE envs SET params_json =
--       '{"md_factor": 0.8, "recover_after_s": 10}' WHERE env_id = 'env-a';
-- ===========================================================================
