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

-- Redis counterpart of apisix/plugins/proxy-cache/memory_handler.lua.
--
-- Copied from apache/apisix master @ 22ce60adae3c51177d32225ace62cdeabbac8706
-- (2026-11-26). The request/response rules (cacheable request/response,
-- Vary parsing and signatures, Cache-Control, TTL derivation, Set-Cookie and
-- size guards, header handling, cache status values) are kept line-for-line
-- from upstream. What differs:
--
--   * storage is Redis (storage.lua), one round trip per lookup (two for
--     URLs that use Vary), values stored as <meta JSON + raw body bytes>;
--   * freshness and Age come from Redis' clock (PTTL), not the node's;
--   * keys are {sha1(cache key)} + suffix: fixed size, one cluster slot per
--     URL, cache key verified on read via an MD5 stored in the value;
--   * writes leave body_filter through a bounded per-worker queue
--     (writer.lua) and run as atomic Redis scripts (scripts.lua);
--   * optional stampede protection: cache_lock and stale_while_updating;
--   * per-request state lives in ctx.proxy_cache_redis (not ctx.cache) so
--     it cannot collide with the built-in proxy-cache plugin.
--
-- When upgrading APISIX, diff upstream memory_handler.lua against the commit
-- above and port relevant changes here.

local redis_storage = require("apisix.plugins.proxy-cache-redis.storage")
local writer = require("apisix.plugins.proxy-cache-redis.writer")
local util = require("apisix.plugins.proxy-cache.util")
local resty_random = require("resty.random")
local resty_str = require("resty.string")
local core = require("apisix.core")
local tab_new = require("table.new")
local ngx_re_gmatch = ngx.re.gmatch
local ngx_re_match = ngx.re.match
local parse_http_time = ngx.parse_http_time
local concat = table.concat
local sort = table.sort
local lower = string.lower
local floor = math.floor
local tostring = tostring
local tonumber = tonumber
local ipairs = ipairs
local ngx = ngx
local md5 = ngx.md5
local type = type
local pairs = pairs
local time = ngx.now
local max = math.max
local min = math.min
local sha1_bin = ngx.sha1_bin
local to_hex = resty_str.to_hex
local ngx_sleep = ngx.sleep
local str_find = string.find
local str_sub = string.sub

-- Upstream: bumped from 1 to 2 for the Vary variant layout, and to 3 for
-- the length-prefixed storage keys. 4: Redis framed layout with hashed keys.
local CACHE_VERSION = 4
local MAX_VARIANTS = 64


-- Every Redis key for one cache key is derived from it: the entry itself,
-- the Vary index, one entry per variant, and the stampede lock. The cache
-- key is client controlled (`cache_key` defaults to `$host` and
-- `$request_uri`), so it is hashed: keys have a fixed size whatever the URL
-- length, carry no URL or consumer name in clear, and the {hash-tag} puts
-- all of a URL's keys in one Redis Cluster slot.
local function entry_key(base_key)
    return "{" .. to_hex(sha1_bin(base_key)) .. "}"
end


-- MD5 of the cache key, stored in every value and checked on read: a
-- value is served only if both its SHA-1 key and this MD5 match, and Redis
-- never holds the URL or consumer name in clear.
local function key_id(base_key)
    return md5(base_key)
end


-- Parse the upstream Vary header into a canonical list.
-- Returns:
--   nil           when the value contains `*` anywhere (RFC 9111 §4.1: not
--                 reusable; caller must refuse to cache).
--   empty table   when the header is absent/empty/whitespace-only.
--   sorted list   of lowercased, trimmed header names, otherwise. Sorting
--                 makes the variant signature independent of the order in
--                 which the upstream lists vary headers.
local function parse_vary_list(vary_value)
    if not vary_value or vary_value == "" then
        return {}
    end

    local result = {}
    local iter, iter_err = ngx_re_gmatch(vary_value, "([^,]+)", "oj")
    if not iter then
        core.log.error("failed to parse Vary header: ", iter_err)
        return {}
    end

    for token, _ in iter do
        local h = token[0]
        h = h:gsub("^%s+", ""):gsub("%s+$", "")
        if h == "*" then
            return nil
        end
        if h ~= "" then
            result[#result + 1] = lower(h)
        end
    end

    sort(result)
    return result
end


-- Hash the request's values for each header in `vary_headers` into a stable
-- per-variant signature. Missing headers contribute an empty string so the
-- same request always produces the same signature on store and lookup.
local function compute_signature(vary_headers, ctx)
    if not vary_headers or #vary_headers == 0 then
        return ""
    end

    local values = tab_new(#vary_headers, 0)
    for i, h in ipairs(vary_headers) do
        local var_name = "http_" .. h:gsub("-", "_")
        values[i] = ctx.var[var_name] or ""
    end
    return md5(concat(values, "\0"))
end


-- Look up the response for this request: the entry (or, when a valid Vary
-- index exists, the variant matching this request's header values).
-- Returns res, err, key like memory.lua's get() plus the Redis key read:
-- err is nil (fresh), "expired" (res is still returned, for stale serving),
-- "not found", or a Redis error. One round trip, two for Vary URLs.
local function lookup(cache, ctx)
    local ekey, kid = cache.ekey, cache.kid
    local r, err = cache.storage:read(ekey, ekey .. ":i")
    if not r then
        return nil, err, ekey
    end

    -- the index only counts while fresh, as in the memory strategy
    local index, ierr = redis_storage.decode(r[3], r[4], kid, false)
    if index and not ierr and index.v == CACHE_VERSION
            and type(index.vary) == "table" and #index.vary > 0 then
        local vkey = ekey .. ":v:" .. compute_signature(index.vary, ctx)
        r, err = cache.storage:read(vkey)
        if not r then
            return nil, err, vkey
        end
        local res, derr = redis_storage.decode(r[1], r[2], kid, true)
        return res, derr, vkey
    end

    local res, derr = redis_storage.decode(r[1], r[2], kid, true)
    return res, derr, ekey
end


local function purge_all_variants(storage, base_key)
    return storage:purge_all(entry_key(base_key))
end


-- Purge by prefix: entries are recorded under the first N path prefixes of
-- the request (`purge_prefix_depth`), per host -- e.g. depth 2 records
-- /products/shoes/42 under /products and /products/shoes.
local MAX_PREFIX_DEPTH = 8

local function prefix_index_key(host, prefix)
    return "pfx:{" .. to_hex(sha1_bin(host .. "\0" .. prefix)) .. "}"
end


-- "/a/b/c" -> {"/a", "/a/b", "/a/b/c"} (at most `depth` levels)
local function path_prefixes(path, depth)
    local out, pos = {}, 1
    for _ = 1, depth do
        local s, e = str_find(path, "/[^/]+", pos)
        if not s or s ~= pos then
            break
        end
        out[#out + 1] = str_sub(path, 1, e)
        pos = e + 1
    end
    return out
end


local function new_token()
    local bytes = resty_random.bytes(12)
    if not bytes then
        return tostring(ngx.worker.pid()) .. ":" .. tostring(time()) .. ":"
               .. tostring(math.random())
    end
    return to_hex(bytes)
end


local _M = {}

-- http://www.w3.org/Protocols/rfc2616/rfc2616-sec13.html#sec13.5.1
-- note content-length & apisix-cache-status are not strictly
-- hop-by-hop but we will be adjusting it here anyhow
local hop_by_hop_headers = {
    ["connection"]          = true,
    ["keep-alive"]          = true,
    ["proxy-authenticate"]  = true,
    ["proxy-authorization"] = true,
    ["te"]                  = true,
    ["trailers"]            = true,
    ["transfer-encoding"]   = true,
    ["upgrade"]             = true,
    ["content-length"]      = true,
    ["apisix-cache-status"] = true,
}


-- Sets the Apisix-Cache-Status header and keeps the value in ctx so it can
-- be logged ($proxy_cache_redis_status, registered by the plugin).
local function set_status(ctx, status)
    ctx.proxy_cache_redis_status = status
    core.response.set_header("Apisix-Cache-Status", status)
end


local function include_cache_header(header)
    local n_header = lower(header)
    if n_header == "expires" or n_header == "cache-control" then
        return true
    end

    return false
end


local function overwritable_header(header)
    local n_header = lower(header)

    return not hop_by_hop_headers[n_header]
            and not ngx_re_match(n_header, "ratelimit-remaining")
end


-- The following format can accept:
--      Cache-Control: no-cache
--      Cache-Control: no-store
--      Cache-Control: max-age=3600
--      Cache-Control: max-stale=3600
--      Cache-Control: min-fresh=3600
--      Cache-Control: private, max-age=600
--      Cache-Control: public, max-age=31536000
-- Refer to: https://www.holisticseo.digital/pagespeed/cache-control/
local function parse_directive_header(h)
    if not h then
        return {}
    end

    if type(h) == "table" then
        h = concat(h, ", ")
    end

    local t    = {}
    local res  = tab_new(3, 0)
    local iter = ngx_re_gmatch(h, "([^,]+)", "oj")

    local m = iter()
    while m do
        local _, err = ngx_re_match(m[0], [[^\s*([^=]+)(?:=(.+))?]],
            "oj", nil, res)
        if err then
            core.log.error(err)
        end

        -- store the directive token as a numeric value if it looks like a number;
        -- otherwise, store the string value. for directives without token, we just
        -- set the key to true
        t[lower(res[1])] = tonumber(res[2]) or res[2] or true

        m = iter()
    end

    return t
end


local function parse_resource_ttl(ctx, cc)
    local max_age = cc["s-maxage"] or cc["max-age"]

    if not max_age then
        local expires = ctx.var.upstream_http_expires

        -- if multiple Expires headers are present, last one wins
        if type(expires) == "table" then
            expires = expires[#expires]
        end

        local exp_time = parse_http_time(tostring(expires))
        if exp_time then
            max_age = exp_time - time()
        end
    end

    return max_age and max(max_age, 0) or 0
end


local function cacheable_request(conf, ctx, cc)
    if not util.match_method(conf, ctx) then
        return false, "MISS"
    end

    if conf.cache_bypass ~= nil then
        local value = util.generate_complex_value(conf.cache_bypass, ctx)
        core.log.info("proxy-cache cache bypass value:", value)
        if value ~= nil and value ~= "" and value ~= "0" then
            return false, "BYPASS"
        end
    end

    if conf.cache_control and (cc["no-store"] or cc["no-cache"]) then
        return false, "BYPASS"
    end

    return true, ""
end


-- Detect a Set-Cookie header in the final response, regardless of its source
-- (upstream or another plugin) and regardless of casing. header_filter calls
-- ngx.resp.get_headers with raw=true, so keys keep their original casing and a
-- plain res_headers["set-cookie"] lookup would miss "Set-Cookie".
local function response_has_set_cookie(res_headers)
    for name, value in pairs(res_headers) do
        if lower(name) == "set-cookie" and value and value ~= "" then
            return true
        end
    end
    return false
end


local function cacheable_response(conf, ctx, cc, res_headers)
    if not util.match_status(conf, ctx) then
        return false
    end

    if conf.no_cache ~= nil then
        local value = util.generate_complex_value(conf.no_cache, ctx)
        core.log.info("proxy-cache no-cache value:", value)

        if value ~= nil and value ~= "" and value ~= "0" then
            return false
        end
    end

    -- Always honor upstream Cache-Control directives that mark the response as
    -- non-shared/non-storable, regardless of the conf.cache_control flag. The
    -- flag governs request-side semantics; upstream response directives are a
    -- safety contract the application uses to mark personalized content.
    if cc["private"] or cc["no-store"] or cc["no-cache"] then
        return false
    end

    if conf.cache_control and parse_resource_ttl(ctx, cc) <= 0 then
        return false
    end

    -- Set-Cookie is per-recipient and not safe for a shared cache to store by
    -- default; require explicit opt-in via cache_set_cookie. Inspect the final
    -- response headers rather than ctx.var.upstream_http_set_cookie so that a
    -- Set-Cookie injected by another plugin (e.g. api-breaker's
    -- break_response_headers, workflow) is caught too, not only one emitted by
    -- the upstream.
    if not conf.cache_set_cookie and response_has_set_cookie(res_headers) then
        return false
    end

    -- Vary: * (RFC 9111 §4.1) means the response is not reusable; refuse to
    -- cache. parse_vary_list returns nil for that case.
    if ctx.var.upstream_http_vary then
        local vary_headers = parse_vary_list(ctx.var.upstream_http_vary)
        if vary_headers == nil then
            return false
        end
    end

    return true
end


local function serve(conf, ctx, cache, res, status)
    cache.hit = true

    for key, value in pairs(res.headers) do
        if conf.hide_cache_headers == true and include_cache_header(key) then
            core.response.set_header(key, "")
        elseif overwritable_header(key) then
            core.response.set_header(key, value)
        end
    end

    core.response.set_header("Age", floor(time() - res.timestamp))
    set_status(ctx, status)

    return res.status, res.body
end


-- Freshness rules from the memory strategy. Returns nil when the entry may
-- be served, or the status to report instead.
local function stale_status(conf, cc, res)
    if conf.cache_control then
        if cc["max-age"] and time() - res.timestamp > cc["max-age"] then
            return "STALE"
        end

        if cc["max-stale"] and time() - res.timestamp - res.ttl > cc["max-stale"] then
            return "STALE"
        end

        if cc["min-fresh"] and res.ttl - (time() - res.timestamp) < cc["min-fresh"] then
            return "STALE"
        end
    else
        if time() - res.timestamp > res.ttl then
            return "STALE"
        end
    end
end


-- Returns "acquired", "fresh" (someone refreshed the response after we
-- read it) or nil (another request holds the lock, or Redis failed).
local function try_lock(cache, conf, data_key)
    local token = new_token()
    local res, err = cache.storage:lock(cache.ekey, token,
                                        conf.cache_lock_timeout * 1000, data_key)
    if res == 1 then
        cache.lock_token = token
        return "acquired"
    end
    if res == 2 then
        return "fresh"
    end
    if err then
        redis_storage.log_error("proxy-cache-redis: failed to take cache lock: ", err)
    end
    return nil
end


-- cache_lock: another request is fetching this URL. Poll until its response
-- is stored, the lock is released (non-cacheable response) or the timeout.
-- Returns res when a fresh response appeared, otherwise nil (go upstream).
local function wait_for_fill(conf, ctx, cache)
    local deadline = time() + conf.cache_lock_timeout
    local delay = 0.005
    while time() < deadline do
        ngx_sleep(delay)
        delay = min(delay * 2, 0.05)

        local res, err, key = lookup(cache, ctx)
        if res and not err then
            return res
        end
        if err ~= "not found" and err ~= "expired" then
            return nil
        end
        if try_lock(cache, conf, key) == "acquired" then
            return nil
        end
    end
    core.log.info("proxy-cache-redis: cache lock wait timed out for ",
                  ctx.proxy_cache_redis_key)
    return nil
end


-- PURGE /a/b/* : purge every entry recorded under /a/b for this host.
local function purge_prefix(conf, ctx, cache)
    local prefix = str_sub(ctx.var.uri, 1, -2):gsub("/+$", "")
    local levels = path_prefixes(prefix, MAX_PREFIX_DEPTH)
    ctx.proxy_cache_redis = nil

    if prefix == "" or levels[#levels] ~= prefix or #levels > conf.purge_prefix_depth then
        return 400, { error_msg = "purge by prefix needs PURGE /<1 to "
                                  .. conf.purge_prefix_depth .. " path segments>/* "
                                  .. "(purge_prefix_depth is " .. conf.purge_prefix_depth
                                  .. "); the whole host cannot be purged this way" }
    end

    local purged, err = cache.storage:purge_prefix(prefix_index_key(ctx.var.host, prefix))
    if not purged then
        redis_storage.log_error("failed to purge prefix, err: ", err)
        return 503, { error_msg = "purge failed: " .. tostring(err) }
    end
    if purged == 0 then
        return 404, { purged = 0 }
    end
    return 200, { purged = purged }
end


local function do_access(conf, ctx, storage_opts, cc)
    if ctx.var.request_method ~= "PURGE" then
        local ret, msg = cacheable_request(conf, ctx, cc)
        if not ret then
            set_status(ctx, msg)
            return
        end
    end

    if not ctx.proxy_cache_redis then
        ctx.proxy_cache_redis = {
            storage = redis_storage.new(storage_opts),
            storage_opts = storage_opts,
            hit = false,
            ttl = 0,
        }
    end

    local cache = ctx.proxy_cache_redis
    local base_key = ctx.proxy_cache_redis_key
    cache.ekey = entry_key(base_key)
    cache.kid = key_id(base_key)

    if ctx.var.request_method == "PURGE" and conf.purge_prefix_depth > 0
            and str_sub(ctx.var.uri, -1) == "*" then
        return purge_prefix(conf, ctx, cache)
    end

    if ctx.var.request_method == "PURGE" then
        -- A URL with Vary support has no base-key entry, only variants
        -- under an index. Treat any of those as a purgeable hit. An
        -- expired entry still inside the retention window is not a miss:
        -- PURGE clears it and returns 200, like the memory strategy.
        local existed, err = purge_all_variants(cache.storage, base_key)
        ctx.proxy_cache_redis = nil
        if existed == nil then
            redis_storage.log_error("failed to purge cache, err: ", err)
            return 503
        end
        if not existed then
            return 404
        end
        return 200
    end

    local res, err, data_key = lookup(cache, ctx)

    if err == "expired" and conf.stale_while_updating > 0 and res
            and res.version == CACHE_VERSION
            and time() - res.timestamp - res.ttl <= conf.stale_while_updating then
        -- one request refreshes the entry, everyone else gets the stale copy
        -- (or the just-refreshed one, which is at most one round trip old)
        local got = try_lock(cache, conf, data_key)
        if got ~= "acquired" and not cache.storage.failed then
            return serve(conf, ctx, cache, res, "UPDATING")
        end
    end

    if (err == "not found" or err == "expired") and conf.cache_lock
            and not cache.lock_token and not cache.storage.failed then
        local got = try_lock(cache, conf, data_key)
        if got == "fresh" then
            res, err = lookup(cache, ctx)
        elseif got ~= "acquired" and not cache.storage.failed then
            local filled = wait_for_fill(conf, ctx, cache)
            if filled then
                res, err = filled, nil
            end
        end
    end

    if err then
        if err == "expired" then
            set_status(ctx, "EXPIRED")

        elseif err ~= "not found" then
            set_status(ctx, "MISS")
            if err == redis_storage.ERR_BACKOFF then
                -- already logged once when Redis failed; don't flood the log
                core.log.info("failed to get from cache, err: ", err)
            else
                redis_storage.log_error("failed to get from cache, err: ", err)
            end

        elseif conf.cache_control and cc["only-if-cached"] then
            set_status(ctx, "MISS")
            return 504

        else
            set_status(ctx, "MISS")
        end
        return
    end

    if res.version ~= CACHE_VERSION then
        core.log.warn("cache format mismatch, purging ", base_key)
        set_status(ctx, "BYPASS")
        local _, perr = purge_all_variants(cache.storage, base_key)
        if perr then
            redis_storage.log_error("failed to purge cache, err: ", perr)
        end
        return
    end

    local stale = stale_status(conf, cc, res)
    if stale then
        set_status(ctx, stale)
        return
    end

    return serve(conf, ctx, cache, res, "HIT")
end


function _M.access(conf, ctx, storage_opts)
    local cc = parse_directive_header(ctx.var.http_cache_control)

    local code, body = do_access(conf, ctx, storage_opts, cc)

    -- return the connection to the pool as soon as the lookup is done;
    -- the write (if any) goes through the per-worker write queue
    local cache = ctx.proxy_cache_redis
    if cache and cache.storage then
        cache.storage:release()
        cache.storage = nil
    end

    return code, body
end


-- Give the stampede lock back early when this response will not be stored,
-- so requests waiting on it go upstream instead of waiting for the timeout.
local function release_lock(cache)
    if cache.lock_token then
        writer.push(cache.storage_opts, {
            op = "unlock",
            ekey = cache.ekey,
            token = cache.lock_token,
            bytes = 64,
        })
        cache.lock_token = nil
    end
end


function _M.header_filter(conf, ctx)
    local cache = ctx.proxy_cache_redis
    if not cache or cache.hit then
        return
    end

    -- Redis failed in this worker meanwhile: don't buffer a body that would
    -- not be stored anyway
    if redis_storage.is_down(cache.storage_opts) then
        ctx.proxy_cache_redis = nil
        return
    end

    local res_headers = ngx.resp.get_headers(0, true)

    for key in pairs(res_headers) do
        if conf.hide_cache_headers == true and include_cache_header(key) then
            core.response.set_header(key, "")
        end
    end

    local cc = parse_directive_header(ctx.var.upstream_http_cache_control)

    if cacheable_response(conf, ctx, cc, res_headers) then
        cache.res_headers = res_headers
        cache.ttl = conf.cache_control and parse_resource_ttl(ctx, cc) or conf.cache_ttl
    else
        release_lock(cache)
        ctx.proxy_cache_redis = nil
    end
end


function _M.body_filter(conf, ctx)
    local cache = ctx.proxy_cache_redis
    if not cache or cache.hit then
        return
    end

    local res_body = core.response.hold_body_chunk(ctx, true, conf.max_resp_body_size)
    if not res_body then
        return
    end

    -- the response is complete: whatever happens below, never run twice
    ctx.proxy_cache_redis = nil

    if conf.max_resp_body_size and #res_body >= conf.max_resp_body_size then
        -- response exceeds the buffering cap; stream it through without
        -- caching so we never store a truncated body
        release_lock(cache)
        return
    end

    -- Redis just failed in this worker: skip the write instead of queueing it
    if redis_storage.is_down(cache.storage_opts) then
        return
    end

    local value, err = redis_storage.frame({
        k      = cache.kid,
        v      = CACHE_VERSION,
        status = ngx.status,
        headers = cache.res_headers,
        ttl    = cache.ttl,
        rt     = cache.storage_opts.retention * 1000,
        -- fallback age source for entries without expiry (ttl = 0)
        timestamp = time(),
    }, res_body)
    if not value then
        core.log.error("failed to set cache, err: ", err)
        release_lock(cache)
        return
    end

    -- cacheable_response has already filtered out Vary: *, so parse_vary_list
    -- returns either an empty list (no vary) or the sorted header list.
    local vary_headers = parse_vary_list(ctx.var.upstream_http_vary) or {}
    local vary_json, signature = "", ""
    if #vary_headers > 0 then
        signature = compute_signature(vary_headers, ctx)
        vary_json = core.json.encode(vary_headers)
    end

    local prefixes
    if conf.purge_prefix_depth > 0 then
        local host = ctx.var.host
        prefixes = path_prefixes(ctx.var.uri, conf.purge_prefix_depth)
        for i, p in ipairs(prefixes) do
            prefixes[i] = prefix_index_key(host, p)
        end
        if #prefixes == 0 then
            prefixes = nil
        end
    end

    local queued = writer.push(cache.storage_opts, {
        op = "store",
        ekey = cache.ekey,
        base_key = cache.kid,
        value = value,
        bytes = #value,
        ttl = cache.ttl,
        vary_json = vary_json,
        signature = signature,
        version = CACHE_VERSION,
        max_variants = MAX_VARIANTS,
        token = cache.lock_token,
        prefixes = prefixes,
    })
    if not queued then
        release_lock(cache)
    end
end


-- Expose the variant-aware purge for callers reusing this strategy.
_M.purge_all_variants = purge_all_variants


return _M
