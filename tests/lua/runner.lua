-- Drives proxy-cache-redis the way APISIX's plugin runner does.
-- X-Plugin: "redis" (default) | "memory" (upstream proxy-cache, memory
-- strategy, unmodified) | "none" (plain proxy) -- for benchmarks.
-- Test config (which plugin_attr is active) lives in a shared dict so every
-- worker sees the same one.
local plugin = require("apisix.plugins.proxy-cache-redis")
local stub_plugin = require("apisix.plugin")
local core = require("apisix.core")
local cjson = require("cjson.safe")
local ngx = ngx

local mem_plugin  -- upstream apisix/plugins/proxy-cache/init.lua, lazily

local _M = {}

local CLUSTER = {"127.0.0.1:7000", "127.0.0.1:7001", "127.0.0.1:7002"}

local SENTINELS = {"127.0.0.1:26379", "127.0.0.1:26380", "127.0.0.1:26381"}

local ATTRS = {
    both = {
        key_prefix = "test:pc:",
        redis = { host = "127.0.0.1", port = 6390 },
        ["redis-sentinel"] = { sentinels = SENTINELS, master_name = "mymaster" },
        ["redis-cluster"] = { nodes = CLUSTER, name = "test-cluster" },
    },
    sentinel_bad_name = {
        key_prefix = "test:pc:", failure_backoff = 0,
        ["redis-sentinel"] = { sentinels = SENTINELS, master_name = "nope" },
    },
    sentinel_dead = {
        key_prefix = "test:pc:",
        ["redis-sentinel"] = { sentinels = {"127.0.0.1:26999", "127.0.0.1:26998"},
                               master_name = "mymaster" },
    },
    sentinel_partly_dead = {
        key_prefix = "test:pc:",
        ["redis-sentinel"] = { sentinels = {"127.0.0.1:26999", "127.0.0.1:26380"},
                               master_name = "mymaster" },
    },
    single_only = {
        key_prefix = "test:pc:",
        redis = { host = "127.0.0.1", port = 6390 },
    },
    cluster_only = {
        key_prefix = "test:pc:",
        ["redis-cluster"] = { nodes = CLUSTER, name = "test-cluster" },
    },
    dead = {
        key_prefix = "test:pc:", failure_backoff = 1,
        redis = { host = "127.0.0.1", port = 6399, timeout = 200 },
    },
    dead_nobackoff = {
        key_prefix = "test:pc:", failure_backoff = 0,
        redis = { host = "127.0.0.1", port = 6399, timeout = 200 },
    },
    hung = {
        key_prefix = "test:pc:", failure_backoff = 1,
        redis = { host = "127.0.0.1", port = 6398, timeout = 200 },
    },
    hung_nobackoff = {
        key_prefix = "test:pc:", failure_backoff = 0,
        redis = { host = "127.0.0.1", port = 6398, timeout = 200 },
    },
    tiny_queue = {
        key_prefix = "test:pc:", write_concurrency = 1, max_pending_writes = 2,
        redis = { host = "127.0.0.1", port = 6390 },
    },
    acl = {
        key_prefix = "test:pc:",
        redis = { host = "127.0.0.1", port = 6391, username = "cache",
                  password = "s3cret", database = 2 },
    },
    bad_password = {
        key_prefix = "test:pc:", failure_backoff = 0,
        redis = { host = "127.0.0.1", port = 6391, username = "cache",
                  password = "wrong" },
    },
    tls = {
        key_prefix = "test:pc:",
        redis = { host = "127.0.0.1", port = 6392, ssl = true, ssl_verify = false },
    },
    failover = {
        key_prefix = "test:pc:",
        ["redis-cluster"] = {
            nodes = {"127.0.0.1:7100", "127.0.0.1:7101", "127.0.0.1:7102"},
            name = "failover-cluster", timeout = 500,
        },
    },
    bad_prefix = {
        key_prefix = "bad{prefix}",
        redis = { host = "127.0.0.1" },
    },
    unknown_field = {
        redis = { host = "127.0.0.1", hots = "typo" },
    },
}

local confs = {}
local current

local function sync()
    local name = ngx.shared.testcfg:get("attr") or "both"
    if name == current then
        return
    end
    current = name
    if name == "none" then
        stub_plugin.attrs["proxy-cache-redis"] = nil
    else
        -- a fresh table, as a config.yaml reload would produce
        stub_plugin.attrs["proxy-cache-redis"] = cjson.decode(cjson.encode(ATTRS[name]))
    end
    confs = {}
end


function _M.init()
    sync()
    plugin.init()
end


function _M.admin()
    local args = ngx.req.get_uri_args()
    if args.attr then
        ngx.shared.testcfg:set("attr", args.attr)
        return ngx.say("ok")
    end
    sync()
    if args.stats then
        return ngx.say(ngx.worker.pid(), " ", cjson.encode(require(
            "apisix.plugins.proxy-cache-redis.writer").stats_all()))
    end
    if args.check then
        local conf = cjson.decode(args.check)
        local ok, err = plugin.check_schema(conf)
        return ngx.say(ok and "valid" or ("invalid: " .. tostring(err)))
    end
end


local function new_ctx(name)
    return {
        var = setmetatable({}, {
            __index = function(_, k) return ngx.var[k] end,
            __newindex = function(_, k, v) ngx.var[k] = v end,
        }),
        _plugin_name = name,
        consumer_name = ngx.var.http_x_consumer,
    }
end


function _M.access()
    sync()
    local which = ngx.var.http_x_plugin or "redis"
    if which == "none" then
        return
    end

    local p = plugin
    if which == "memory" then
        mem_plugin = mem_plugin or require("apisix.plugins.proxy-cache")
        p = mem_plugin
    end

    local raw = which .. (ngx.var.http_x_conf or "{}")
    local conf = confs[raw]
    if not conf then
        conf = cjson.decode(ngx.var.http_x_conf or "{}")
        if which == "memory" then
            conf.cache_strategy = "memory"
            conf.cache_zone = "memory_cache"
        end
        local ok, err = p.check_schema(conf)
        if not ok then
            ngx.status = 500
            ngx.say("bad conf: ", err)
            return ngx.exit(500)
        end
        confs[raw] = conf
    end

    local ctx = new_ctx(which == "memory" and "proxy-cache" or "proxy-cache-redis")
    ngx.ctx.api_ctx = ctx
    ngx.ctx.conf = conf
    ngx.ctx.p = p

    local code, body = p.access(conf, ctx)
    if code then
        ngx.status = code
        if type(body) == "table" then
            -- APISIX's core.response.exit JSON-encodes table bodies
            ngx.header["Content-Type"] = "application/json"
            ngx.print(cjson.encode(body))
        elseif body then
            ngx.print(body)
        end
        return ngx.exit(code)
    end
end


function _M.header_filter()
    local ctx = ngx.ctx.api_ctx
    if ctx then
        ngx.ctx.p.header_filter(ngx.ctx.conf, ctx)
        local getter = core.ctx.vars.proxy_cache_redis_status
        if getter and ngx.ctx.p == plugin then
            ngx.header["X-Status-Var"] = getter(ctx)
        end
    end
end


function _M.body_filter()
    if ngx.ctx.api_ctx then
        ngx.ctx.p.body_filter(ngx.ctx.conf, ngx.ctx.api_ctx)
    end
end


-- upstream: every response carries a per-URI counter so tests can tell a
-- cache hit (old counter) from a fetch (new counter)
function _M.upstream()
    local args = ngx.req.get_uri_args()
    local dict = ngx.shared.counters
    local n = dict:incr(ngx.var.request_uri, 1, 0, 600) or 0

    if args.delay_ms then
        ngx.sleep(tonumber(args.delay_ms) / 1000)
    end
    if args.vary then ngx.header["Vary"] = args.vary end
    if args.cc then ngx.header["Cache-Control"] = args.cc end
    if args.cookie then ngx.header["Set-Cookie"] = "a=b" end
    if args.expires_in then
        ngx.header["Expires"] = ngx.http_time(ngx.time() + tonumber(args.expires_in))
    end
    ngx.header["X-Upstream"] = "yes"
    ngx.header["X-Count"] = n
    ngx.status = tonumber(args.status) or 200

    local lang = ngx.var.http_accept_language or ""
    local body = "body-" .. n .. "-" .. lang
    if args.size then
        body = body .. string.rep("x", tonumber(args.size))
    end
    if args.binary then
        body = body .. "\0\1\2\255\254" .. "é\n\r\n{\"json\":1}"
    end
    ngx.print(body)
end

return _M
