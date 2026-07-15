-- deploy/mysql/schema.sql —— rl-limiter v3.0 配置源与用量库的初始化脚本。
--
-- 作用：本文件挂载到 MySQL 官方镜像的 /docker-entrypoint-initdb.d/ 目录，
-- 容器首次启动（数据卷为空）时自动执行，一次性建好全部表并写入种子数据。
-- 之后 rl-limiter 只读 haproxy_nodes / envs / env_targets / settings 得到
-- "受控节点接线 + 环境配额 + 运行模式"，并把每秒用量与心跳写回
-- usage_samples / heartbeats。数据库自此成为"配额权威"。
--
-- 配置版本机制（与 rl_limiter/db.py 的 _VERSION_SQL 对齐，务必理解）：
--   配置版本 = envs / env_targets / settings 三张“配置表” updated_at 的最大值
--   （取 UNIX_TIMESTAMP，单位秒）。这三张表的 updated_at 列都带
--   `ON UPDATE CURRENT_TIMESTAMP`，任一行新增或修改都会推高该最大值，
--   rl-limiter 每 RL_MYSQL_POLL_INTERVAL_S 秒轮询一次版本，发现变大即全量
--   重新拉取配置并热重载——因此“改库即秒级生效、无需重启服务”。
--   注意：haproxy_nodes（基础设施接线）不参与配置版本计算，改节点接线需
--   重启 rl-limiter 才会重新 fetch_nodes。
--
-- 约定：所有表引擎 InnoDB（支持事务与外键），字符集 utf8mb4。

-- compose 已通过 MYSQL_DATABASE=rl_limiter 预建库；这里再幂等建库并切入，
-- 保证脚本单独执行（如手工导入）时也能落到正确的库。
CREATE DATABASE IF NOT EXISTS `rl_limiter`
    CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
USE `rl_limiter`;

-- settings：全局运行开关。rl-limiter 只读取 k='mode' 一行决定运行模式
-- （dry-run 只记录不写 map；enforce 真实下发限速）。属配置表，其
-- updated_at 参与配置版本计算——改 mode 即触发热重载。
CREATE TABLE IF NOT EXISTS `settings` (
    `k`          VARCHAR(64)  NOT NULL COMMENT '配置键，如 mode',
    `v`          VARCHAR(255) NULL     COMMENT '配置值，如 dry-run / enforce',
    `updated_at` TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
                 ON UPDATE CURRENT_TIMESTAMP COMMENT '最后修改时间，参与配置版本',
    PRIMARY KEY (`k`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='全局运行开关（运行模式等），参与配置版本';

-- haproxy_nodes：受控 HAProxy 节点接线（基础设施配置）。rl-limiter 启动时
-- 读取，为每个节点建立内网 TCP runtime 客户端（采集 show stat + 下发 set map
-- 共用）。timeout_ms 为运维口径毫秒，代码内换算为秒。注意本表 updated_at
-- 不参与配置版本，改动节点接线需重启 rl-limiter 才生效。
CREATE TABLE IF NOT EXISTS `haproxy_nodes` (
    `name`           VARCHAR(64)  NOT NULL COMMENT '节点名，env_targets.node 引用它',
    `host`           VARCHAR(255) NOT NULL COMMENT '节点内网 TCP stats socket 主机名/IP',
    `port`           INT          NOT NULL COMMENT 'stats socket 端口（level admin）',
    `bwlim_map_path` VARCHAR(255) NOT NULL DEFAULT '/etc/haproxy/maps/bwlim.map'
                     COMMENT '该节点 bwlim map 文件路径，与其 haproxy.cfg 一致',
    `timeout_ms`     INT          NOT NULL DEFAULT 500
                     COMMENT '单次 runtime 命令超时（毫秒），代码换算为秒',
    `updated_at`     TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
                     ON UPDATE CURRENT_TIMESTAMP COMMENT '最后修改时间',
    PRIMARY KEY (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='受控 HAProxy 节点接线（基础设施配置）';

-- envs：环境配额。quota_mbps 为人类可读的 Mbps（配置口径），进入算法前由
-- 代码换算为 bytes/s。params_json 为可选的快环参数覆盖（NULL 表示用默认
-- AIMD 参数）。配置表，updated_at 参与配置版本——改配额即秒级热生效。
CREATE TABLE IF NOT EXISTS `envs` (
    `env_id`      VARCHAR(64) NOT NULL COMMENT '环境 ID',
    `quota_mbps`  DOUBLE      NOT NULL COMMENT '约定带宽，Mbps（允许小数）',
    `params_json` JSON        NULL     COMMENT '可选快环参数覆盖，NULL=用默认',
    `updated_at`  TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP
                  ON UPDATE CURRENT_TIMESTAMP COMMENT '最后修改时间，参与配置版本',
    PRIMARY KEY (`env_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='环境配额（Mbps），参与配置版本';

-- env_targets：环境 ↔ (节点, frontend) 挂载点映射。一个环境可横跨多台节点
-- 的多个 frontend；rl-limiter 把同一环境所有挂载点的流量全局聚合后做决策，
-- 再按各挂载点近期用量加权拆分整形值写回。配置表，updated_at 参与配置版本。
-- 外键 env_id → envs 级联删除：删环境时其挂载点一并清理。
CREATE TABLE IF NOT EXISTS `env_targets` (
    `env_id`     VARCHAR(64) NOT NULL COMMENT '所属环境 ID',
    `node`       VARCHAR(64) NOT NULL COMMENT '节点名，对应 haproxy_nodes.name',
    `frontend`   VARCHAR(64) NOT NULL COMMENT '该节点上的 frontend 名',
    `updated_at` TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP
                 ON UPDATE CURRENT_TIMESTAMP COMMENT '最后修改时间，参与配置版本',
    PRIMARY KEY (`env_id`, `node`, `frontend`),
    CONSTRAINT `fk_env_targets_env`
        FOREIGN KEY (`env_id`) REFERENCES `envs` (`env_id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='环境↔(节点,frontend)挂载点映射，参与配置版本';

-- usage_samples：每秒用量样本落库（供后台展示/对账）。rl-limiter 每拍缓冲、
-- 每 5s 批量 INSERT。带宽字段为 Mbps（人类可读）。列名/顺序必须与
-- rl_limiter/db.py 的 INSERT 完全一致：
--   ts, node_id, env_id, rate_mbps, mean10_mbps, ewma60_mbps,
--   conn_cur, bwlim_mbps, state, changed
CREATE TABLE IF NOT EXISTS `usage_samples` (
    `id`          BIGINT      NOT NULL AUTO_INCREMENT COMMENT '自增主键',
    `ts`          DOUBLE      NOT NULL COMMENT '采样时刻（epoch 秒，含小数）',
    `node_id`     VARCHAR(64) NULL     COMMENT '产生样本的 rl-limiter 实例 ID',
    `env_id`      VARCHAR(64) NULL     COMMENT '环境 ID',
    `rate_mbps`   DOUBLE      NULL     COMMENT '瞬时速率（Mbps）',
    `mean10_mbps` DOUBLE      NULL     COMMENT '10 秒滑动均值（Mbps）',
    `ewma60_mbps` DOUBLE      NULL     COMMENT '60 秒 EWMA（Mbps）',
    `conn_cur`    INT         NULL     COMMENT '当前并发连接数',
    `bwlim_mbps`  DOUBLE      NULL     COMMENT '本拍下发的整形值（Mbps）',
    `state`       VARCHAR(16) NULL     COMMENT 'AIMD 状态',
    `changed`     TINYINT     NULL     COMMENT '本拍整形值是否变化（1/0）',
    `created_at`  TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '入库时间',
    PRIMARY KEY (`id`),
    KEY `idx_env_ts` (`env_id`, `ts`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='每秒用量样本（Mbps 口径），供后台展示/对账';

-- heartbeats：各 rl-limiter 实例心跳，按 node_id upsert（每 10s 一次），
-- 供后台观测实例存活、当前模式与已应用配置版本。列名与 db.py 的
-- INSERT ... ON DUPLICATE KEY UPDATE 一致：node_id, service_version, mode,
-- config_version。
CREATE TABLE IF NOT EXISTS `heartbeats` (
    `node_id`         VARCHAR(64) NOT NULL COMMENT 'rl-limiter 实例 ID（主键，upsert）',
    `service_version` VARCHAR(32) NULL     COMMENT '服务版本号',
    `mode`            VARCHAR(16) NULL     COMMENT '当前实际运行模式',
    `config_version`  BIGINT      NULL     COMMENT '当前已应用的配置版本',
    `updated_at`      TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP
                      ON UPDATE CURRENT_TIMESTAMP COMMENT '最后心跳时间',
    PRIMARY KEY (`node_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='各限速实例心跳（模式/配置版本）';

-- ---- 种子数据（与原 deploy/config/mock-backend-config.json 对应）----------

-- 运行模式：默认 dry-run（只观测不下发），验证无误后 UPDATE 为 enforce。
INSERT INTO `settings` (`k`, `v`) VALUES ('mode', 'dry-run')
    ON DUPLICATE KEY UPDATE `v` = VALUES(`v`);

-- 两台受控节点：host 用 compose 服务名（同网络内可解析），端口 9999。
INSERT INTO `haproxy_nodes` (`name`, `host`, `port`, `bwlim_map_path`, `timeout_ms`) VALUES
    ('hap-1', 'hap-1', 9999, '/etc/haproxy/maps/bwlim.map', 500),
    ('hap-2', 'hap-2', 9999, '/etc/haproxy/maps/bwlim.map', 500)
    ON DUPLICATE KEY UPDATE `host` = VALUES(`host`), `port` = VALUES(`port`);

-- 两个环境：env-a 200 Mbps 横跨两台节点的 fe_env_a；env-b 50 Mbps 只在 hap-1。
INSERT INTO `envs` (`env_id`, `quota_mbps`, `params_json`) VALUES
    ('env-a', 200, NULL),
    ('env-b', 50,  NULL)
    ON DUPLICATE KEY UPDATE `quota_mbps` = VALUES(`quota_mbps`);

INSERT INTO `env_targets` (`env_id`, `node`, `frontend`) VALUES
    ('env-a', 'hap-1', 'fe_env_a'),
    ('env-a', 'hap-2', 'fe_env_a'),
    ('env-b', 'hap-1', 'fe_env_b')
    ON DUPLICATE KEY UPDATE `frontend` = VALUES(`frontend`);
