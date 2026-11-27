--
-- Licensed to the Apache Software Foundation (ASF) under one or more
-- contributor license agreements.  See the NOTICE file distributed with
-- this work for additional information regarding copyright ownership.
-- The ASF licenses this file to You under the Apache License, Version 2.0
-- (the "License"); you may not use this file except in compliance with
-- the License.  You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
--

-- proxy-cache-redis: the proxy-cache "memory" strategy, stored in Redis.
--
-- Routes choose single Redis, Redis Sentinel or Redis Cluster with
-- `policy`; connection details live only in config.yaml under
-- plugin_attr.proxy-cache-redis.

local handler = require("apisix.plugins.proxy-cache-redis.handler")
local storage = require("apisix.plugins.proxy-cache-redis.storage")
local util = require("apisix.plugins.proxy-cache.util")
local plugin = require("apisix.plugin")
local core = require("apisix.core")
local ngx = ngx
local ipairs = ipairs

local plugin_name = "proxy-cache-redis"

local POLICY_REDIS = "redis"
local POLICY_CLUSTER = "redis-cluster"
local POLICY_SENTINEL = "redis-sentinel"

-- Cache-key entries that already include the request identity. When any of
-- these is present in the user-supplied cache_key, consumer_isolation is a
-- no-op because the operator has opted into their own scheme.
local IDENTITY_VARS = {
    ["$consumer_name"]      = true,
    ["$consumer_group_id"]  = true,
    ["$remote_user"]        = true,
    ["$http_authorization"] = true,
}


local function cache_key_has_identity(cache_key)
    for _, entry in ipairs(cache_key) do
        if IDENTITY_VARS[entry] then
            return true
        end
    end
    return false
end


-- Route-level schema: identical to proxy-cache, minus cache_zone and
-- cache_strategy, plus `policy` and the opt-in stampede controls.
-- max_resp_body_size defaults to 1 MiB (proxy-cache: 64 MiB) because every
-- hit is read over the network.
local schema = {
    type = "object",
    properties = {
        policy = {
            type = "string",
            enum = {POLICY_REDIS, POLICY_SENTINEL, POLICY_CLUSTER},
            default = POLICY_REDIS,
        },
        purge_prefix_depth = {
            description = "record each cached response under its first N URL "
                       .. "path prefixes so `PURGE /a/b/*` removes everything "
                       .. "below /a/b for the request's host; 0 disables",
            type = "integer",
            minimum = 0,
            maximum = 8,
            default = 0,
        },
        max_resp_body_size = {
            type = "integer",
            minimum = 1,
            default = 1048576,
            description = "maximum response body size in bytes buffered and "
                       .. "stored in Redis; larger responses are streamed "
                       .. "through without being cached",
        },
        cache_lock = {
            description = "on a miss, only one request per URL goes upstream; "
                       .. "the others wait for its response (up to "
                       .. "cache_lock_timeout) instead of hitting the upstream",
            type = "boolean",
            default = false,
        },
        cache_lock_timeout = {
            description = "seconds a request waits for another request's "
                       .. "response, and the lifetime of the lock",
            type = "integer",
            minimum = 1,
            maximum = 60,
            default = 5,
        },
        stale_while_updating = {
            description = "seconds after expiry during which the expired "
                       .. "response is served (status UPDATING) while one "
                       .. "request refreshes it; 0 disables. Bounded by "
                       .. "expired_retention in config.yaml",
            type = "integer",
            minimum = 0,
            default = 0,
        },
        cache_key = {
            type = "array",
            minItems = 1,
            items = {
                description = "a key for caching",
                type = "string",
                pattern = [[(^[^\$].+$|^\$[0-9a-zA-Z_]+$)]],
            },
            default = {"$host", "$request_uri"}
        },
        cache_http_status = {
            type = "array",
            minItems = 1,
            items = {
                description = "http response status",
                type = "integer",
                minimum = 200,
                maximum = 599,
            },
            uniqueItems = true,
            default = {200, 301, 404},
        },
        cache_method = {
            type = "array",
            minItems = 1,
            items = {
                description = "supported http method",
                type = "string",
                enum = {"GET", "POST", "HEAD"},
            },
            uniqueItems = true,
            default = {"GET", "HEAD"},
        },
        hide_cache_headers = {
            type = "boolean",
            default = false,
        },
        cache_control = {
            type = "boolean",
            default = false,
        },
        cache_bypass = {
            type = "array",
            minItems = 1,
            items = {
                type = "string",
                pattern = [[(^[^\$].+$|^\$[0-9a-zA-Z_]+$)]]
            },
        },
        no_cache = {
            type = "array",
            minItems = 1,
            items = {
                type = "string",
                pattern = [[(^[^\$].+$|^\$[0-9a-zA-Z_]+$)]]
            },
        },
        cache_ttl = {
            type = "integer",
            minimum = 1,
            default = 300,
        },
        consumer_isolation = {
            type = "boolean",
            default = true,
        },
        cache_set_cookie = {
            type = "boolean",
            default = false,
        },
    },
}


-- config.yaml schema: plugin_attr.proxy-cache-redis
local attr_schema = {
    type = "object",
    properties = {
        key_prefix = {
            type = "string",
            maxLength = 128,
            -- braces would break the per-URL {hash-tag} used for clusters
            pattern = [[^[^{}]*$]],
            default = "apisix:proxy-cache:",
        },
        expired_retention = {
            description = "seconds an expired entry stays in Redis so it is "
                       .. "reported as EXPIRED (not MISS), PURGE returns 200, "
                       .. "and stale_while_updating can serve it",
            type = "integer",
            minimum = 0,
            default = 60,
        },
        failure_backoff = {
            description = "seconds a worker skips Redis after a connection "
                       .. "failure or timeout; 0 disables",
            type = "number",
            minimum = 0,
            default = 1,
        },
        write_concurrency = {
            description = "background write drainers per worker",
            type = "integer",
            minimum = 1,
            maximum = 64,
            default = 4,
        },
        max_pending_writes = {
            description = "per-worker write queue limit (items); further "
                       .. "writes are dropped and counted",
            type = "integer",
            minimum = 1,
            default = 10000,
        },
        max_pending_bytes = {
            description = "per-worker write queue limit (bytes)",
            type = "integer",
            minimum = 1024,
            default = 67108864,
        },
        [POLICY_REDIS] = {
            type = "object",
            properties = {
                host = { type = "string", minLength = 1 },
                port = { type = "integer", minimum = 1, maximum = 65535, default = 6379 },
                username = { type = "string", minLength = 1 },
                password = { type = "string", minLength = 0 },
                database = { type = "integer", minimum = 0, default = 0 },
                timeout = { type = "integer", minimum = 1, default = 200 },
                ssl = { type = "boolean", default = false },
                ssl_verify = { type = "boolean", default = false },
                server_name = { type = "string", minLength = 1 },
                keepalive_timeout = { type = "integer", minimum = 1000, default = 10000 },
                keepalive_pool = { type = "integer", minimum = 1, default = 100 },
            },
            required = {"host"},
            additionalProperties = false,
        },
        [POLICY_SENTINEL] = {
            type = "object",
            properties = {
                sentinels = {
                    type = "array",
                    minItems = 1,
                    items = { type = "string", minLength = 2, maxLength = 100 },
                },
                master_name = { type = "string", minLength = 1 },
                sentinel_username = { type = "string", minLength = 1 },
                sentinel_password = { type = "string", minLength = 0 },
                username = { type = "string", minLength = 1 },
                password = { type = "string", minLength = 0 },
                database = { type = "integer", minimum = 0, default = 0 },
                timeout = { type = "integer", minimum = 1, default = 200 },
                ssl = { type = "boolean", default = false },
                ssl_verify = { type = "boolean", default = false },
                server_name = { type = "string", minLength = 1 },
                keepalive_timeout = { type = "integer", minimum = 1000, default = 10000 },
                keepalive_pool = { type = "integer", minimum = 1, default = 100 },
            },
            required = {"sentinels", "master_name"},
            additionalProperties = false,
        },
        [POLICY_CLUSTER] = {
            type = "object",
            properties = {
                nodes = {
                    type = "array",
                    minItems = 1,
                    items = { type = "string", minLength = 2, maxLength = 100 },
                },
                name = { type = "string", minLength = 1 },
                password = { type = "string", minLength = 0 },
                timeout = { type = "integer", minimum = 1, default = 200 },
                ssl = { type = "boolean", default = false },
                ssl_verify = { type = "boolean", default = false },
                keepalive_timeout = { type = "integer", minimum = 1000, default = 10000 },
                keepalive_pool = { type = "integer", minimum = 1, default = 100 },
            },
            required = {"nodes", "name"},
            additionalProperties = false,
        },
    },
    additionalProperties = false,
}


local _M = {
    version = 0.3,
    -- one below proxy-cache (1085); do not enable both on the same route
    priority = 1084,
    name = plugin_name,
    schema = schema,
}


-- Translate config.yaml field names to what the connectors expect.
local function to_redis_conf(c)
    return {
        redis_host = c.host,
        redis_port = c.port,
        redis_username = c.username,
        redis_password = c.password,
        redis_database = c.database,
        redis_timeout = c.timeout,
        redis_ssl = c.ssl,
        redis_ssl_verify = c.ssl_verify,
        redis_server_name = c.server_name,
        redis_keepalive_timeout = c.keepalive_timeout,
        redis_keepalive_pool = c.keepalive_pool,
    }
end


local function to_sentinel_conf(c)
    local conf = to_redis_conf(c)
    conf.redis_host = nil
    conf.redis_port = nil
    conf.sentinel_master_name = c.master_name
    conf.sentinel_username = c.sentinel_username
    conf.sentinel_password = c.sentinel_password
    conf.sentinels = {}
    for i, addr in ipairs(c.sentinels) do
        local host, port = core.utils.parse_addr(addr)
        conf.sentinels[i] = { host = host, port = port or 26379 }
    end
    return conf
end


local function to_cluster_conf(c)
    return {
        redis_cluster_nodes = c.nodes,
        redis_cluster_name = c.name,
        redis_password = c.password,
        redis_timeout = c.timeout,
        redis_cluster_ssl = c.ssl,
        redis_cluster_ssl_verify = c.ssl_verify,
        redis_keepalive_timeout = c.keepalive_timeout,
        redis_keepalive_pool = c.keepalive_pool,
    }
end


local function storage_opts(attr, policy, conn)
    return {
        policy = policy,
        conn = conn,
        prefix = attr.key_prefix,
        retention = attr.expired_retention,
        backoff = attr.failure_backoff,
        write_concurrency = attr.write_concurrency,
        max_pending_writes = attr.max_pending_writes,
        max_pending_bytes = attr.max_pending_bytes,
    }
end


-- plugin_attr is re-read whenever config.yaml is reloaded (new table), so
-- cache the validated result per raw table.
local cached_raw, cached_attr, cached_err

local function get_attr()
    local raw = plugin.plugin_attr(plugin_name)
    if raw == cached_raw and (cached_attr or cached_err) then
        return cached_attr, cached_err
    end

    cached_raw = raw
    cached_attr, cached_err = nil, nil

    if not raw then
        cached_err = "plugin_attr." .. plugin_name .. " is not configured in config.yaml"
        return nil, cached_err
    end

    local attr = core.table.deepcopy(raw)
    local ok, err = core.schema.check(attr_schema, attr)
    if not ok then
        cached_err = "invalid plugin_attr." .. plugin_name .. " in config.yaml: " .. err
        return nil, cached_err
    end

    -- one immutable options table per policy: also the key of the
    -- per-worker cluster client and write queue
    attr.storage_opts = {}
    if attr[POLICY_REDIS] then
        attr.storage_opts[POLICY_REDIS] =
            storage_opts(attr, POLICY_REDIS, to_redis_conf(attr[POLICY_REDIS]))
    end
    if attr[POLICY_SENTINEL] then
        attr.storage_opts[POLICY_SENTINEL] =
            storage_opts(attr, POLICY_SENTINEL, to_sentinel_conf(attr[POLICY_SENTINEL]))
    end
    if attr[POLICY_CLUSTER] then
        attr.storage_opts[POLICY_CLUSTER] =
            storage_opts(attr, POLICY_CLUSTER, to_cluster_conf(attr[POLICY_CLUSTER]))
    end

    cached_attr = attr
    return attr
end


local function storage_opts_for(policy)
    local attr, err = get_attr()
    if not attr then
        return nil, err
    end

    local opts = attr.storage_opts[policy]
    if not opts then
        return nil, "policy \"" .. policy .. "\" selected but plugin_attr."
                    .. plugin_name .. "." .. policy
                    .. " is not configured in config.yaml"
    end

    if policy == POLICY_CLUSTER and not ngx.shared[storage.CLUSTER_LOCK_DICT] then
        return nil, "policy \"redis-cluster\" needs a lua_shared_dict named \""
                    .. storage.CLUSTER_LOCK_DICT .. "\"; add it under "
                    .. "nginx_config.http.custom_lua_shared_dict in config.yaml"
    end

    return opts
end


function _M.init()
    -- $proxy_cache_redis_status for access logs and logger plugins
    if core.ctx and core.ctx.register_var then
        core.ctx.register_var("proxy_cache_redis_status", function(ctx)
            return ctx.proxy_cache_redis_status
        end)
    end

    local attr, err = get_attr()
    if not attr then
        -- not fatal: routes using this plugin are rejected by check_schema
        core.log.warn(plugin_name, ": ", err)
    end
end


function _M.check_schema(conf)
    local ok, err = core.schema.check(schema, conf)
    if not ok then
        return false, err
    end

    for _, key in ipairs(conf.cache_key) do
        if key == "$request_method" then
            return false, "cache_key variable " .. key .. " unsupported"
        end
    end

    local opts, opts_err = storage_opts_for(conf.policy)
    if opts_err then
        return false, opts_err
    end

    if conf.stale_while_updating > opts.retention then
        return false, "stale_while_updating (" .. conf.stale_while_updating
                      .. "s) exceeds expired_retention (" .. opts.retention
                      .. "s) in config.yaml: expired entries are deleted before "
                      .. "they could be served stale"
    end

    return true
end


function _M.access(conf, ctx)
    core.log.info("proxy-cache-redis plugin access phase")

    local opts, err = storage_opts_for(conf.policy)
    if not opts then
        -- config.yaml changed after the route was accepted: serve uncached
        storage.log_error(plugin_name .. ": ", err)
        ctx.proxy_cache_redis_status = "BYPASS"
        core.response.set_header("Apisix-Cache-Status", "BYPASS")
        return
    end

    local value = util.generate_complex_value(conf.cache_key, ctx)

    -- When an authenticated identity is available, prepend it to the effective
    -- cache key so that each consumer gets its own cache namespace. The control
    -- character separator is outside the character set permitted by the
    -- consumer username schema, keeping the prefix unambiguous.
    if conf.consumer_isolation and not cache_key_has_identity(conf.cache_key) then
        local identity = ctx.consumer_name
        if not identity or identity == "" then
            identity = ctx.var.remote_user
        end
        if identity and identity ~= "" then
            value = identity .. "\1" .. value
        end
    end

    -- Kept in ctx, not in $upstream_cache_key: APISIX only declares that
    -- nginx variable when proxy-cache or graphql-proxy-cache is enabled, so
    -- writing it would fail on gateways that only enable this plugin.
    ctx.proxy_cache_redis_key = value
    core.log.info("proxy-cache-redis cache key value:", value)

    return handler.access(conf, ctx, opts)
end


function _M.header_filter(conf, ctx)
    core.log.info("proxy-cache-redis plugin header filter phase")
    handler.header_filter(conf, ctx)
end


function _M.body_filter(conf, ctx)
    core.log.info("proxy-cache-redis plugin body filter phase")
    handler.body_filter(conf, ctx)
end


return _M
