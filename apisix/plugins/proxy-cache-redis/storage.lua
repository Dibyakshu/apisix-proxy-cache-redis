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

-- Redis storage for proxy-cache-redis (the role memory.lua plays for the
-- memory strategy).
--
-- One instance is used per phase (access, or a write-queue drainer); it
-- opens at most one connection lazily and must be released with :release().
--
-- Fail-fast: once a connection/network error happens, the instance refuses
-- further calls (one request never waits on several timeouts), and the
-- worker skips Redis for `failure_backoff` seconds. For Redis Cluster only a
-- whole-cluster failure (no node answers the slot map) triggers the worker
-- backoff: a single failed node affects only its own slots, which the
-- cluster client already fast-fails per node. When the backoff ends a
-- single request probes Redis while the others keep skipping it; Redis is
-- considered healthy again only after a command succeeds (a hung Redis still
-- accepts TCP connections, so a successful connect proves nothing).

local core              = require("apisix.core")
local rediscluster_util = require("apisix.utils.rediscluster")
local scripts           = require("apisix.plugins.proxy-cache-redis.scripts")
local redis_new         = require("resty.redis").new
local resty_str         = require("resty.string")
local ngx               = ngx
local ngx_null          = ngx.null
local now               = ngx.now
local sha1_bin          = ngx.sha1_bin
local crc32             = ngx.crc32_long
local setmetatable      = setmetatable
local tonumber          = tonumber
local tostring          = tostring
local type              = type
local ipairs            = ipairs
local pairs             = pairs
local unpack            = unpack
local select            = select
local ceil              = math.ceil
local max               = math.max
local str_find          = string.find
local str_sub           = string.sub

local _M = {
    CLUSTER_LOCK_DICT = "plugin-proxy-cache-redis-cluster-slot-lock",
    ERR_BACKOFF = "redis temporarily skipped after a recent failure",
}
local mt = { __index = _M }

local POLICY_CLUSTER = "redis-cluster"
local POLICY_SENTINEL = "redis-sentinel"

-- per-worker state
local down_until = {}         -- conn conf table -> skip Redis until this time
local probe_until = {}        -- conn conf table -> deadline of the recovery probe
local cluster_clients = {}    -- conn conf table -> resty.rediscluster instance
local slot_refresh_at = {}    -- conn conf table -> last forced slot-map refresh
local degraded_until = {}     -- conn conf table -> use failover-safe reads until
local master_cache = {}       -- conn conf table -> {conf = master conn, exp = t}
local good_sentinel = {}      -- conn conf table -> index of the last sentinel that answered

-- seconds a worker trusts the master address Sentinel gave it; any
-- connection error or READONLY reply forgets it earlier
local MASTER_TTL = 10
-- entries purged per round trip by purge_prefix, and its time budget
local PURGE_BATCH = 500
local PURGE_BUDGET = 30

-- seconds a cluster keeps using failover-safe reads after an error
local DEGRADED_SECONDS = 10

local ERR_UNHEALTHY_MASTER = "master node is unhealthy"

for _, s in pairs(scripts) do
    s.sha = resty_str.to_hex(sha1_bin(s.src))
end


-- Rate-limited error log: during an incident every request fails the same
-- way, so each distinct message is logged at most once per LOG_INTERVAL
-- seconds per worker, with the number of suppressed repeats.
local LOG_INTERVAL = 5
local log_state = {}
local log_state_n = 0

function _M.log_error(msg, err)
    local key = msg .. tostring(err)
    local st = log_state[key]
    local t = now()
    if st and t - st.at < LOG_INTERVAL then
        st.suppressed = st.suppressed + 1
        return
    end

    if st and st.suppressed > 0 then
        core.log.error(msg, err, " (", st.suppressed,
                       " similar errors suppressed in the last ", LOG_INTERVAL, "s)")
    else
        core.log.error(msg, err)
    end

    if not st then
        -- bound the table: messages embed node addresses, not request data
        if log_state_n >= 1000 then
            log_state, log_state_n = {}, 0
        end
        log_state_n = log_state_n + 1
        st = {}
        log_state[key] = st
    end
    st.at, st.suppressed = t, 0
end


-- Redis error replies start with an upper-case code ("ERR ...",
-- "NOSCRIPT ...", "WRONGTYPE ..."); anything else (timeout, closed,
-- connection refused, failed to fetch slots, ...) is a transport problem.
local function is_transport_error(err)
    if type(err) ~= "string" then
        return true
    end
    return not str_find(err, "^%u+%s")
end


-- true while this worker is skipping this Redis target after a failure
-- (backoff running, or a recovery probe in flight)
function _M.is_down(opts)
    local until_ts = down_until[opts.conn]
    if not until_ts then
        return false
    end
    local t = now()
    local probe = probe_until[opts.conn]
    return t < until_ts or (probe ~= nil and t < probe)
end


-- Single-node connection. Same pooling rules as apisix.utils.redis, but
-- the socket is closed when AUTH/SELECT fail instead of being leaked.
local function connect_single(conf)
    local red = redis_new()
    local timeout = conf.redis_timeout
    red:set_timeouts(timeout, timeout, timeout)

    -- AUTH, SELECT and the TLS handshake only run on fresh connections, so
    -- different databases / credentials / TLS settings must not share a pool
    local scheme = "redis"
    if conf.redis_ssl then
        scheme = conf.redis_ssl_verify and "rediss-verify" or "rediss"
    end
    local pool = "pcr#" .. scheme .. "#" .. conf.redis_host .. "#" .. conf.redis_port
                 .. "#" .. conf.redis_database
    if conf.redis_password and conf.redis_password ~= "" then
        pool = pool .. "#" .. crc32((conf.redis_username or "") .. ":" .. conf.redis_password)
    end
    if conf.redis_ssl and conf.redis_server_name then
        pool = pool .. "#" .. conf.redis_server_name
    end

    local server_name
    if conf.redis_ssl then
        server_name = conf.redis_server_name or conf.redis_host
        if core.utils.parse_ipv4(server_name) or core.utils.parse_ipv6(server_name) then
            server_name = nil
        end
    end

    local ok, err = red:connect(conf.redis_host, conf.redis_port, {
        ssl = conf.redis_ssl,
        ssl_verify = conf.redis_ssl_verify,
        server_name = server_name,
        pool = pool,
        pool_size = conf.redis_keepalive_pool,
    })
    if not ok then
        return nil, err
    end

    local count
    count, err = red:get_reused_times()
    if not count then
        red:close()
        return nil, err
    end

    if count == 0 then
        if conf.redis_password and conf.redis_password ~= "" then
            if conf.redis_username then
                ok, err = red:auth(conf.redis_username, conf.redis_password)
            else
                ok, err = red:auth(conf.redis_password)
            end
            if not ok then
                red:close()
                return nil, "redis auth failed: " .. tostring(err)
            end
        end

        if conf.redis_database ~= 0 then
            ok, err = red:select(conf.redis_database)
            if not ok then
                red:close()
                return nil, "failed to select redis db: " .. tostring(err)
            end
        end
    end

    return red
end


-- Ask one Sentinel for the current master of `conn.sentinel_master_name`.
local function sentinel_query(conn, s)
    local red = redis_new()
    local timeout = conn.redis_timeout
    red:set_timeouts(timeout, timeout, timeout)

    local ok, err = red:connect(s.host, s.port, {
        ssl = conn.redis_ssl,
        ssl_verify = conn.redis_ssl_verify,
        pool = "pcr-sentinel#" .. s.host .. "#" .. s.port
               .. (conn.redis_ssl and "#ssl" or ""),
        pool_size = conn.redis_keepalive_pool,
    })
    if not ok then
        return nil, err
    end

    local count = red:get_reused_times()
    if count == 0 and conn.sentinel_password and conn.sentinel_password ~= "" then
        if conn.sentinel_username then
            ok, err = red:auth(conn.sentinel_username, conn.sentinel_password)
        else
            ok, err = red:auth(conn.sentinel_password)
        end
        if not ok then
            red:close()
            return nil, "sentinel auth failed: " .. tostring(err)
        end
    end

    local res
    res, err = red:sentinel("get-master-addr-by-name", conn.sentinel_master_name)
    if not res then
        red:close()
        return nil, err
    end
    red:set_keepalive(conn.redis_keepalive_timeout, conn.redis_keepalive_pool)

    if res == ngx_null or type(res) ~= "table" or not res[1] then
        return nil, "unknown master name"
    end
    return res[1], tonumber(res[2])
end


-- Current master connection settings (cached per worker for MASTER_TTL).
local function resolve_master(conn)
    local cached = master_cache[conn]
    if cached and now() < cached.exp then
        return cached.conf
    end

    -- start with the sentinel that answered last time, so a dead or hung
    -- first sentinel does not cost a timeout on every lookup
    local errs = {}
    local n = #conn.sentinels
    local first = good_sentinel[conn] or 1
    for k = 0, n - 1 do
        local i = (first - 1 + k) % n + 1
        local s = conn.sentinels[i]
        local host, port = sentinel_query(conn, s)
        if host then
            good_sentinel[conn] = i
            local conf = setmetatable({ redis_host = host, redis_port = port },
                                      { __index = conn })
            master_cache[conn] = { conf = conf, exp = now() + MASTER_TTL }
            return conf
        end
        errs[#errs + 1] = s.host .. ":" .. s.port .. " " .. tostring(port)
    end

    return nil, "no sentinel returned master '" .. conn.sentinel_master_name
                .. "': " .. table.concat(errs, "; ")
end


-- A connection error, or a write refused by a demoted master, means the
-- cached master address may be stale: ask Sentinel again next time.
local function sentinel_error(self, err)
    if self.policy == POLICY_SENTINEL
            and (is_transport_error(err) or str_find(tostring(err), "^READONLY")) then
        master_cache[self.conn] = nil
    end
end


function _M.new(opts)
    return setmetatable({
        opts     = opts,
        policy   = opts.policy,
        conn     = opts.conn,
        prefix   = opts.prefix,
        retention_ms = opts.retention * 1000,
        backoff  = opts.backoff,
        red      = nil,
        failed   = nil,
        broken   = false,
    }, mt)
end


-- Cluster errors that mean the whole cluster is unreachable (no slot map):
-- only these trip the worker-wide backoff in cluster mode.
local function is_cluster_wide(err)
    return str_find(err, "slots", 1, true) ~= nil
           or str_find(err, "new redis cluster", 1, true) ~= nil
end


function _M:_fail(err)
    err = tostring(err)
    if self.policy == POLICY_CLUSTER and not is_cluster_wide(err) then
        -- one failed node: only its own keys fail (the cluster client
        -- fast-fails them); calls for other nodes in this instance proceed
        return nil, err
    end
    self.failed = err
    self.broken = true
    if self.backoff > 0 then
        down_until[self.conn] = now() + self.backoff
    end
    probe_until[self.conn] = nil
    return nil, err
end


function _M:_connect()
    if self.failed then
        return nil, self.failed
    end

    if self.red then
        return self.red
    end

    local until_ts = down_until[self.conn]
    if until_ts then
        local t = now()
        local probe = probe_until[self.conn]
        if t < until_ts or (probe and t < probe) then
            self.failed = _M.ERR_BACKOFF
            return nil, self.failed
        end
        -- backoff over: this request is the single probe. The deadline
        -- frees the slot even if the probing request is aborted mid-call.
        probe_until[self.conn] = t + 3 * self.conn.redis_timeout / 1000
    end

    local red, err
    if self.policy == POLICY_CLUSTER then
        red = cluster_clients[self.conn]
        if not red then
            red, err = rediscluster_util.new(self.conn, _M.CLUSTER_LOCK_DICT)
            if red then
                cluster_clients[self.conn] = red
            end
        end
    elseif self.policy == POLICY_SENTINEL then
        local conf
        conf, err = resolve_master(self.conn)
        if conf then
            red, err = connect_single(conf)
            if not red then
                master_cache[self.conn] = nil
            end
        end
    else
        red, err = connect_single(self.conn)
    end

    if not red then
        return self:_fail("failed to connect to redis: " .. tostring(err))
    end

    self.red = red
    return red
end


-- resty.rediscluster (api7 fork) fast-fails keys of a master it marked
-- unhealthy but does not refresh its slot map on that path, so after a
-- failover the promoted replica would never be used. Refresh it in the
-- background, at most once per second per worker.
local function refresh_slots_bg(premature, red)
    if premature then
        return
    end
    local ok, err = pcall(red.refresh_slots, red)
    if not ok then
        core.log.warn("proxy-cache-redis: slot map refresh failed: ", err)
    end
end


local function maybe_refresh_slots(self, err)
    if self.policy ~= POLICY_CLUSTER or err ~= ERR_UNHEALTHY_MASTER then
        return
    end
    local t = now()
    if t - (slot_refresh_at[self.conn] or 0) < 1 then
        return
    end
    slot_refresh_at[self.conn] = t
    local ok, terr = ngx.timer.at(0, refresh_slots_bg, self.red)
    if not ok then
        core.log.warn("proxy-cache-redis: failed to schedule slot refresh: ", terr)
    end
end


local function healthy(self)
    if down_until[self.conn] then
        down_until[self.conn] = nil
        probe_until[self.conn] = nil
    end
end


function _M:_cluster_error()
    if self.policy == POLICY_CLUSTER then
        degraded_until[self.conn] = now() + DEGRADED_SECONDS
    end
end


-- Run a Redis command; transport errors poison the instance (fail-fast).
function _M:_cmd(cmd, ...)
    local red, err = self:_connect()
    if not red then
        return nil, err
    end

    local res
    res, err = red[cmd](red, ...)
    -- resty.redis returns false for Redis error replies, nil for I/O errors
    if not res then
        maybe_refresh_slots(self, err)
        sentinel_error(self, err)
        if not str_find(tostring(err), "NOSCRIPT", 1, true) then
            self:_cluster_error()
        end
        if is_transport_error(err) then
            return self:_fail(err)
        end
        return nil, err
    end

    -- a real round trip worked: Redis is healthy (again)
    healthy(self)
    return res
end


local function is_noscript(err)
    return type(err) == "string" and str_find(err, "NOSCRIPT", 1, true) ~= nil
end


function _M:_eval(script, key, ...)
    local res, err = self:_cmd("evalsha", script.sha, 1, key, ...)
    if not res and is_noscript(err) then
        res, err = self:_cmd("eval", script.src, 1, key, ...)
    end
    return res, err
end


function _M:release()
    local red = self.red
    self.red = nil
    if not red or self.policy == POLICY_CLUSTER then
        -- resty.rediscluster pools its own connections per command
        return
    end

    if self.broken then
        red:close()
        return
    end

    local ok, err = red:set_keepalive(self.conn.redis_keepalive_timeout,
                                      self.conn.redis_keepalive_pool)
    if not ok then
        core.log.warn("proxy-cache-redis: failed to set keepalive: ", err)
    end
end


-- Build a stored value: <meta length>\n<meta JSON><body>
function _M.frame(meta, body)
    local meta_json = core.json.encode(meta)
    if not meta_json then
        return nil, "could not encode object"
    end
    return #meta_json .. "\n" .. meta_json .. (body or "")
end


-- Decode a stored value read together with its PTTL.
-- Returns obj, err where err is nil (fresh), "expired" (obj still returned,
-- for stale serving / cleanup) or "not found". `kid` is the MD5 of the cache
-- key; a value written for another cache key (a SHA-1 key collision would
-- also need an MD5 collision) or in an unknown layout reads as "not found"
-- and the next store overwrites it.
function _M.decode(raw, pttl, kid, with_body)
    if raw == nil or raw == ngx_null then
        return nil, "not found"
    end

    local nl = str_find(raw, "\n", 1, true)
    local mlen = nl and tonumber(str_sub(raw, 1, nl - 1))
    if not mlen then
        return nil, "not found"
    end

    local obj = core.json.decode(str_sub(raw, nl + 1, nl + mlen))
    if type(obj) ~= "table" or obj.k ~= kid then
        return nil, "not found"
    end

    obj.version = obj.v
    if with_body then
        obj.body = str_sub(raw, nl + mlen + 1)
    end

    local rt = tonumber(obj.rt) or 0
    pttl = tonumber(pttl)
    if pttl and pttl >= 0 then
        -- age from Redis' clock: physical TTL was ttl + rt at write time
        local ttl_ms = (tonumber(obj.ttl) or 0) * 1000
        local age = max(0, (ttl_ms + rt - pttl) / 1000)
        obj.timestamp = now() - age
        if pttl <= rt then
            return obj, "expired"
        end
    end

    return obj
end


-- GET + PTTL for each key in one round trip. Returns a flat array
-- {value1, pttl1, value2, pttl2, ...}.
-- Normally a plain pipeline: cheapest for Redis (~1.5 us per key vs ~20 us
-- for a script) and no Lua copy of large values. In a cluster, for
-- DEGRADED_SECONDS after any error, reads use a small script instead
-- (all keys share a hash tag, so one node): resty.rediscluster's pipeline
-- path refreshes the slot map synchronously inside the request on every
-- call that hits a failed master, which stalled requests for up to 2 s
-- during a failover in testing, while its single-command path fast-fails.
function _M:read(...)
    if self.policy == POLICY_CLUSTER and now() < (degraded_until[self.conn] or 0) then
        local first = self.prefix .. select(1, ...)
        local rest = {}
        for i = 2, select("#", ...) do
            rest[#rest + 1] = self.prefix .. select(i, ...)
        end
        return self:_eval(scripts.read, first, unpack(rest))
    end

    local red, err = self:_connect()
    if not red then
        return nil, err
    end

    red:init_pipeline()
    for i = 1, select("#", ...) do
        local key = self.prefix .. select(i, ...)
        red:get(key)
        red:pttl(key)
    end

    local res
    res, err = red:commit_pipeline()
    if not res then
        self:_cluster_error()
        maybe_refresh_slots(self, err)
        sentinel_error(self, err)
        if is_transport_error(err) then
            return self:_fail(err)
        end
        return nil, err
    end

    for i = 1, #res do
        local r = res[i]
        if type(r) == "table" and r[1] == false then
            self:_cluster_error()
            return nil, r[2]
        end
    end

    healthy(self)
    return res
end


function _M:purge_all(ekey)
    local res, err = self:_eval(scripts.purge, self.prefix .. ekey)
    if not res then
        return nil, err
    end
    return res == 1
end


-- Returns 1 (acquired), 2 (the response was refreshed meanwhile) or 0.
function _M:lock(ekey, token, ms, data_key)
    return self:_eval(scripts.lock, self.prefix .. ekey, token, ms,
                      self.prefix .. data_key)
end


local function item_cmd(self, item)
    if item.op == "unlock" then
        return { scripts.unlock, self.prefix .. item.ekey, item.token }
    end
    local ttl_ms = item.ttl > 0 and ceil(item.ttl * 1000) or 0
    return { scripts.store, self.prefix .. item.ekey,
             self.retention_ms, ttl_ms, item.value, item.vary_json, item.signature,
             item.version, item.max_variants, item.base_key, item.token or "" }
end


-- Run script calls { {script, key, arg...}, ... } in one pipeline (one
-- round trip per Redis node). Returns the per-call results (a value, or
-- {false, err}) or nil, err when the round trip itself failed. Calls that
-- hit NOSCRIPT (script cache flushed, new node) are retried with EVAL.
function _M:_pipeline(cmds)
    -- Cluster, shortly after an error: one call at a time through the
    -- client's single-command path, which fast-fails keys of a failed
    -- master. Its pipeline path rebuilds the slot map synchronously on every
    -- such call; from several write drainers at once that starved the worker
    -- (p99 1-2 s) during a failover in testing.
    if self.policy == POLICY_CLUSTER and now() < (degraded_until[self.conn] or 0) then
        local results = {}
        for i, c in ipairs(cmds) do
            local res, rerr = self:_eval(unpack(c))
            results[i] = res or { false, rerr }
        end
        return results
    end

    local red, err = self:_connect()
    if not red then
        return nil, err
    end

    red:init_pipeline(#cmds)
    for _, c in ipairs(cmds) do
        red:evalsha(c[1].sha, 1, unpack(c, 2))
    end

    local results
    results, err = red:commit_pipeline()
    if not results then
        self:_cluster_error()
        maybe_refresh_slots(self, err)
        sentinel_error(self, err)
        if is_transport_error(err) then
            self:_fail(err)
        end
        return nil, err
    end
    healthy(self)

    for i, c in ipairs(cmds) do
        local r = results[i]
        if type(r) == "table" and r[1] == false then
            if is_noscript(r[2]) then
                local res, rerr = self:_eval(unpack(c))
                results[i] = res or { false, rerr }
            else
                sentinel_error(self, r[2])
            end
        end
    end
    return results
end


-- Execute a batch of queued writes (store / unlock, plus the prefix-index
-- updates of each store) in one pipeline.
-- Returns the number of failed items and the last error.
function _M:run_batch(batch)
    local cmds, owner = {}, {}
    for i, item in ipairs(batch) do
        cmds[#cmds + 1] = item_cmd(self, item)
        owner[#cmds] = i
        if item.prefixes then
            local life = max(1000, (item.ttl > 0 and ceil(item.ttl * 1000) or 0)
                                   + self.retention_ms)
            for _, zkey in ipairs(item.prefixes) do
                cmds[#cmds + 1] = { scripts.prefix_add, self.prefix .. zkey, item.ekey, life }
                owner[#cmds] = i
            end
        end
    end

    local results, err = self:_pipeline(cmds)
    if not results then
        return #batch, err
    end

    local failed, nfailed = {}, 0
    for i = 1, #cmds do
        local r = results[i]
        if type(r) == "table" and r[1] == false then
            err = r[2]
            if not failed[owner[i]] then
                failed[owner[i]] = true
                nfailed = nfailed + 1
            end
        end
    end
    return nfailed, err
end


-- Purge every entry recorded under a prefix index: batches of
-- PURGE_BATCH entries, each batch one pipeline of PURGE scripts (each
-- entry with its index, variants and lock), then removed from the index.
-- Returns the number of entries that existed, or nil, err.
function _M:purge_prefix(zkey)
    local key = self.prefix .. zkey
    local total = 0
    local deadline = now() + PURGE_BUDGET

    while true do
        local members, err = self:_cmd("zrange", key, 0, PURGE_BATCH - 1)
        if not members then
            return nil, err
        end
        if #members == 0 then
            break
        end

        local cmds = {}
        for i, member in ipairs(members) do
            cmds[i] = { scripts.purge, self.prefix .. member }
        end
        local results
        results, err = self:_pipeline(cmds)
        if not results then
            return nil, err
        end
        for i = 1, #cmds do
            if results[i] == 1 then
                total = total + 1
            end
        end

        local ok
        ok, err = self:_cmd("zrem", key, unpack(members))
        if not ok then
            return nil, err
        end

        if now() > deadline then
            return nil, "prefix purge stopped after " .. PURGE_BUDGET .. "s with "
                        .. total .. " entries purged; repeat the request to continue"
        end
    end

    return total
end


return _M
