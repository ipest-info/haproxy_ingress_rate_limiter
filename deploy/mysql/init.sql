-- rl-limiter 配置库：建表 + 演示种子数据（单 HAProxy 模型，v0.4 起）。
--
-- docker compose 首次启动 mysql 容器时自动执行本文件；生产环境把它作为
-- 建库脚本手工执行一次即可。
--
-- 模型：一个 rl-limiter 实例管**一台**与它同机的 HAProxy。多台机器可以
-- 共用一个配置库——每台用 RL_NODE_NAME 指定自己是哪个 instance，只读写
-- 属于自己的那些行，集中管理与集中审计因此得以保留。
--
-- **这份配置是数据面的唯一数据源**：rl-limiter 把 haproxy_frontends /
-- haproxy_servers 渲染进本机 haproxy.cfg 的受管区块并 reload，所以在这里
-- （或 Web 控制台上）改完即真正生效，不存在"库改了、cfg 忘了改"的漂移。
--
-- 单位约定：quota_bps 一律为 bit/s（运维口径，40000000 = 40 Mbps）；
-- 写进 haproxy.cfg 的 shared bwlim limit 是它 ÷ 8 的 bytes/s。
--
-- 从 v0.3（多节点 + 环境分组）升级：模型不兼容，envs / env_targets /
-- haproxy_nodes 三张表已被下面的新表取代。旧版本见 tag v0.3.0-colocated。

-- ---------------------------------------------------------------------------
-- 1. service_config：服务级运行参数（单行表，id 恒为 1）。
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS service_config (
    id              TINYINT      NOT NULL PRIMARY KEY DEFAULT 1,
    log_level       VARCHAR(16)  NOT NULL DEFAULT 'info',
    -- 采样节拍（秒）。目前只支持 1.0：速率差分与 10 秒窗口、超限持续
    -- 秒数均以「1 拍 = 1 秒」为前提，改成别的值会让口径整体失真。
    tick_interval_s DOUBLE       NOT NULL DEFAULT 1.0,
    CONSTRAINT ck_service_singleton CHECK (id = 1)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO service_config (id, log_level, tick_interval_s)
VALUES (1, 'info', 1.0)
ON DUPLICATE KEY UPDATE log_level = VALUES(log_level);

-- ---------------------------------------------------------------------------
-- 2. haproxy_instances：受管的 HAProxy 实例（一台机器一行）。
--    只存 stats socket 的接线信息——rl-limiter 靠它做只读采样。
--
--    采样通道二选一（校验强制恰好给一种）：
--      - socket_path：本机 unix stats socket 路径（**同机部署，推荐**）。
--        此时 host/port 留 NULL。不占任何网络端口，访问权由文件属主/
--        属组控制。
--      - host:port：内网 TCP stats socket（远程只读观测）。此时
--        socket_path 留 NULL。注意这种形态下**无法下发配置**——改 cfg +
--        reload 必须在本机做。
--
--    接线信息在 rl-limiter 启动时定型，改动需重启进程才生效（改了会记
--    warning 提示）。
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS haproxy_instances (
    name            VARCHAR(64)  NOT NULL PRIMARY KEY,
    host            VARCHAR(255) NULL DEFAULT NULL,
    port            INT UNSIGNED NULL DEFAULT NULL,
    socket_path     VARCHAR(255) NULL DEFAULT NULL,
    -- 单次 runtime API 命令超时（连接 + 读写，毫秒）；<=0 按默认 500 处理。
    timeout_ms      INT          NOT NULL DEFAULT 500
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ---------------------------------------------------------------------------
-- 3. haproxy_frontends：受管 frontend = 一个监听端口 + 一个 shared bwlim
--    速率桶 + 一组后端服务器。**监控与限速的单位都是它**。
--
--    这些字段会被原样渲染进 haproxy.cfg，因此名字/地址的字符集受严格
--    限制（见 rl_limiter/config.py 的白名单）——放宽等于允许通过配置库
--    往配置文件注入指令。
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS haproxy_frontends (
    instance        VARCHAR(64)  NOT NULL,
    -- frontend 名 = stats 里的 pxname，也是 cfg 里的段名。
    name            VARCHAR(64)  NOT NULL,
    -- 监听地址；空串 = 所有地址（HAProxy 的 `bind :port`）。
    bind_address    VARCHAR(64)  NOT NULL DEFAULT '',
    bind_port       INT UNSIGNED NOT NULL,
    mode            VARCHAR(8)   NOT NULL DEFAULT 'tcp',   -- tcp | http
    -- 限额（bit/s）。既是下发给内核 tc 的 limit（真实限速 = ÷8 bytes/s），
    -- 也是超限告警基准——同源，因此不可能漂移。
    --
    -- 默认 50 Gbps：**默认值不该成为限制**。不填限额建出来的 frontend
    -- 应该是"能跑多快跑多快"，等真要收着了再显式往下调；反过来（默认给
    -- 一个小值）会让人在排查慢的时候满世界找原因，最后发现是默认值。
    quota_bps       BIGINT       NOT NULL DEFAULT 50000000000,
    -- 资源保护水位（NULL/0 = 不写该指令，沿用 global 的 maxconn）。
    -- 不是限速手段：防止限速导致连接堆积耗尽内存/fd。默认不设。
    maxconn         INT UNSIGNED NULL DEFAULT NULL,
    balance         VARCHAR(32)  NOT NULL DEFAULT 'roundrobin',
    -- 各项超时（毫秒）。NULL = 采用 rl-limiter 的默认值，不必逐行填。
    timeout_connect_ms INT UNSIGNED NULL DEFAULT NULL,
    timeout_client_ms  INT UNSIGNED NULL DEFAULT NULL,
    timeout_server_ms  INT UNSIGNED NULL DEFAULT NULL,
    PRIMARY KEY (instance, name),
    -- 同一实例上两个 frontend 绑同一个地址端口会让 HAProxy 起不来。
    UNIQUE KEY uk_bind (instance, bind_address, bind_port),
    CONSTRAINT fk_fe_instance FOREIGN KEY (instance)
        REFERENCES haproxy_instances (name) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ---------------------------------------------------------------------------
-- 4. haproxy_servers：各 frontend 的后端服务器（cfg 里的一行 server）。
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS haproxy_servers (
    instance        VARCHAR(64)  NOT NULL,
    frontend        VARCHAR(64)  NOT NULL,
    name            VARCHAR(64)  NOT NULL,
    address         VARCHAR(255) NOT NULL,
    port            INT UNSIGNED NOT NULL,
    weight          INT UNSIGNED NOT NULL DEFAULT 100,   -- HAProxy 取值 0-256
    check_enabled   TINYINT(1)   NOT NULL DEFAULT 1,     -- 主动健康检查
    check_inter_ms  INT UNSIGNED NOT NULL DEFAULT 2000,
    PRIMARY KEY (instance, frontend, name),
    CONSTRAINT fk_srv_frontend FOREIGN KEY (instance, frontend)
        REFERENCES haproxy_frontends (instance, name) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ===========================================================================
-- 演示种子数据（docker compose 环境）
-- ===========================================================================
-- compose 里的三台节点，每台 = 一个 Ubuntu 24.04 容器跑 HAProxy + 同机
-- rl-limiter。采样走本机 unix stats socket（三台恰好同路径——socket_path
-- 是"该实例本机上的路径"）。
INSERT INTO haproxy_instances (name, socket_path, timeout_ms) VALUES
    ('hap-1', '/run/haproxy/admin.sock', 500),
    ('hap-2', '/run/haproxy/admin.sock', 500),
    ('hap-3', '/run/haproxy/admin.sock', 500);

-- 每台一个受管 frontend，监听 8080，限额 40 Mbps（= 5,000,000 bytes/s
-- 的 shared bwlim limit）。这些行会被各自机器上的 rl-limiter 渲染进
-- 本机 haproxy.cfg 的受管区块。
INSERT INTO haproxy_frontends
    (instance, name, bind_address, bind_port, mode, quota_bps, maxconn, balance)
VALUES
    -- maxconn 留 NULL：默认不设并发上限（沿用 global）。演示要看的是
    -- **限速**，并发上限只会在压测调高并发时莫名其妙地先挡住。
    ('hap-1', 'fe_main', '', 8080, 'tcp', 40000000, NULL, 'roundrobin'),
    ('hap-2', 'fe_main', '', 8080, 'tcp', 40000000, NULL, 'roundrobin'),
    ('hap-3', 'fe_main', '', 8080, 'tcp', 40000000, NULL, 'roundrobin');

-- 后端都指向 compose 里的模拟业务服务 web:9000。
INSERT INTO haproxy_servers
    (instance, frontend, name, address, port, weight, check_enabled)
VALUES
    ('hap-1', 'fe_main', 'web1', 'web', 9000, 100, 1),
    ('hap-2', 'fe_main', 'web1', 'web', 9000, 100, 1),
    ('hap-3', 'fe_main', 'web1', 'web', 9000, 100, 1);

-- ===========================================================================
-- 常用运维 SQL（也可以在 Web 控制台上点，两者写的是同一份数据）
-- ===========================================================================
--   进入 mysql： docker compose exec mysql mysql -url -prl_pass rl_limiter
--
--   调整限额（40 Mbps → 20 Mbps）——**只需这一步**：
--     UPDATE haproxy_frontends SET quota_bps = 20000000
--       WHERE instance = 'hap-1' AND name = 'fe_main';
--     该节点的 rl-limiter 会在一个轮询周期内把它写进本机 haproxy.cfg 的
--     受管区块（haproxy -c 校验 → 原子替换 → reload），数据面即时生效。
--
--   加一台后端服务器：
--     INSERT INTO haproxy_servers
--       (instance, frontend, name, address, port) VALUES
--       ('hap-1', 'fe_main', 'web2', 'web', 9000);
--
--   加一个监听端口（记得同时给它至少一台后端，否则会被校验拒绝）：
--     INSERT INTO haproxy_frontends
--       (instance, name, bind_port, quota_bps) VALUES
--       ('hap-1', 'fe_api', 8081, 8000000);
--     INSERT INTO haproxy_servers
--       (instance, frontend, name, address, port) VALUES
--       ('hap-1', 'fe_api', 'api1', 'web', 9000);
--
--   注意：配置写坏（比如端口冲突、frontend 没有后端）会被 rl-limiter 的
--   校验整份拒绝并保留当前配置继续运行（fail-static），数据面不受影响。
