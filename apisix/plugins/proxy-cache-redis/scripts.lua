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

-- Server-side Lua scripts executed inside Redis.
--
-- Key layout (all share the `{sha1(cache key)}` hash tag, so they live in
-- the same Redis Cluster slot and one script may touch all of them):
--
--     <prefix>{<sha1>}          entry for a response without Vary
--     <prefix>{<sha1>}:i        Vary index
--     <prefix>{<sha1>}:v:<sig>  one entry per Vary variant
--     <prefix>{<sha1>}:l        stampede lock (cache_lock / stale_while_updating)
--     <prefix>pfx:{<sha1>}      purge-by-prefix index of one host + path prefix
--                               (sorted set of entry keys; own slot)
--
-- Every derived key is computed from KEYS[1] because resty.rediscluster only
-- accepts one key per EVAL. Reads (the hot path) do not use scripts: a
-- pipelined GET + PTTL costs Redis ~1 us per key, a script call ~20 us.
--
-- Value layout ("framed"):  <meta length>\n<meta JSON><raw body bytes>
-- The body is stored as-is (no JSON escaping). meta carries an MD5 of the
-- cache key (`k`) so a key collision is detected on read, the logical TTL
-- (`ttl`, seconds) and the retention used at write time (`rt`, ms).
--
-- Expiry uses Redis' clock only: the key's physical TTL is
-- ttl + retention, so an entry is fresh while PTTL > rt and "expired but
-- still readable" while 0 < PTTL <= rt. No APISIX node clock is involved,
-- so clock skew between nodes cannot make entries expire early or late.

local COMMON = [[
local ek = KEYS[1]
local ik = ek .. ":i"
local lk = ek .. ":l"

local function vk(sig)
    return ek .. ":v:" .. sig
end

local function parse(raw)
    if not raw then
        return nil
    end
    local nl = string.find(raw, "\n", 1, true)
    if not nl then
        return nil
    end
    local mlen = tonumber(string.sub(raw, 1, nl - 1))
    if not mlen then
        return nil
    end
    local ok, meta = pcall(cjson.decode, string.sub(raw, nl + 1, nl + mlen))
    if not ok or type(meta) ~= "table" then
        return nil
    end
    return meta
end

local function fresh(key, meta)
    local pttl = redis.call("PTTL", key)
    if pttl == -1 then
        return true
    end
    if pttl < 0 then
        return false
    end
    return pttl > (tonumber(meta.rt) or 0)
end

local function purge_all_variants()
    -- stale-tolerant: an expired index still lists its variants
    local index = parse(redis.call("GET", ik))
    if index and type(index.variants) == "table" then
        for _, sig in ipairs(index.variants) do
            if type(sig) == "string" then
                redis.call("DEL", vk(sig))
            end
        end
    end
    redis.call("DEL", ik)
    redis.call("DEL", ek)
end
]]


-- KEYS[1], ARGV[1..n] = keys (same hash tag).  Returns {GET, PTTL, ...}
-- for KEYS[1] then each ARGV key. Used for reads in Redis Cluster mode
-- only (see storage.read); single-node reads use a plain pipeline.
local READ = [[
local out = { redis.call("GET", KEYS[1]), redis.call("PTTL", KEYS[1]) }
for i = 1, #ARGV do
    out[#out + 1] = redis.call("GET", ARGV[i])
    out[#out + 1] = redis.call("PTTL", ARGV[i])
end
return out
]]


-- KEYS[1] = entry key
-- ARGV: 1 retention_ms, 2 ttl_ms, 3 framed entry, 4 vary_json ("" = no
--       Vary), 5 signature, 6 cache_version, 7 max_variants, 8 cache key
--       id, 9 lock token ("" = none)
-- Mirrors memory_handler.body_filter + update_vary_index, atomically.
local STORE = COMMON .. [[
local rt_ms        = tonumber(ARGV[1])
local ttl_ms       = tonumber(ARGV[2])
local entry        = ARGV[3]
local vary_json    = ARGV[4]
local signature    = ARGV[5]
local version      = tonumber(ARGV[6])
local max_variants = tonumber(ARGV[7])
local base_key     = ARGV[8]
local token        = ARGV[9]

local function put(key, value)
    if ttl_ms > 0 then
        redis.call("SET", key, value, "PX", ttl_ms + rt_ms)
    else
        redis.call("SET", key, value)
    end
end

local function lists_equal(a, b)
    if #a ~= #b then
        return false
    end
    for i = 1, #a do
        if a[i] ~= b[i] then
            return false
        end
    end
    return true
end

if vary_json ~= "" then
    local vary = cjson.decode(vary_json)
    local current = parse(redis.call("GET", ik))
    local variants

    if current
            and current.k == base_key
            and current.v == version
            and type(current.vary) == "table"
            and type(current.variants) == "table"
            and lists_equal(current.vary, vary) then
        variants = current.variants
        local found = false
        for _, s in ipairs(variants) do
            if s == signature then
                found = true
                break
            end
        end
        if not found then
            -- FIFO-evict so a Vary on a high-cardinality header cannot grow
            -- the index without bound
            while #variants >= max_variants do
                local evicted = table.remove(variants, 1)
                if type(evicted) == "string" then
                    redis.call("DEL", vk(evicted))
                end
            end
            variants[#variants + 1] = signature
        end
    else
        if current and type(current.variants) == "table" then
            for _, s in ipairs(current.variants) do
                if type(s) == "string" then
                    redis.call("DEL", vk(s))
                end
            end
        end
        variants = {signature}
    end

    local meta = cjson.encode({
        k = base_key, v = version, vary = vary, variants = variants, rt = rt_ms,
    })
    put(ik, #meta .. "\n" .. meta)

    -- drop any pre-Vary entry stored directly at the base key
    redis.call("DEL", ek)
    put(vk(signature), entry)
else
    -- this response has no Vary, but the URL may have cached a Vary response
    -- earlier: flush the (non-expired) prior index and its variants
    local prior = parse(redis.call("GET", ik))
    if prior and fresh(ik, prior) then
        purge_all_variants()
    end
    put(ek, entry)
end

if token ~= "" and redis.call("GET", lk) == token then
    redis.call("DEL", lk)
end

return 1
]]


-- KEYS[1] = entry key.  Returns 1 when the entry or the Vary index existed
-- (fresh or expired but retained), 0 otherwise; removes entry, index,
-- every variant and any stampede lock.
local PURGE = COMMON .. [[
local existed = redis.call("EXISTS", ek) + redis.call("EXISTS", ik)
purge_all_variants()
redis.call("DEL", lk)
if existed > 0 then
    return 1
end
return 0
]]


-- KEYS[1] = entry key, ARGV[1] = token.  Release the lock if we own it.
local UNLOCK = [[
local lk = KEYS[1] .. ":l"
if redis.call("GET", lk) == ARGV[1] then
    return redis.call("DEL", lk)
end
return 0
]]


-- KEYS[1] = entry key, ARGV[1] = token, ARGV[2] = lock ms, ARGV[3] = key
-- holding the response this request needs (entry or variant).
-- Returns 1 when the lock was acquired, 2 when that response is fresh again
-- (another request refreshed it after we read it: do not fetch again),
-- 0 when someone else holds the lock.
local LOCK = COMMON .. [[
local data = ARGV[3]
local cur = parse(redis.call("GET", data))
if cur and fresh(data, cur) then
    return 2
end
if redis.call("SET", lk, ARGV[1], "NX", "PX", ARGV[2]) then
    return 1
end
return 0
]]


-- KEYS[1] = prefix index (sorted set), ARGV[1] = entry key (without the
-- key prefix), ARGV[2] = lifetime of the entry in ms (ttl + retention).
-- Records the entry under a URL path prefix for purge-by-prefix. Scores are
-- expiry times from Redis' clock; expired members are trimmed on every add,
-- and the index itself expires with its longest-lived member.
local PREFIX_ADD = [[
local t = redis.call("TIME")
local now_ms = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local life = tonumber(ARGV[2])
redis.call("ZADD", KEYS[1], now_ms + life, ARGV[1])
redis.call("ZREMRANGEBYSCORE", KEYS[1], "-inf", now_ms)
if redis.call("PTTL", KEYS[1]) < life then
    redis.call("PEXPIRE", KEYS[1], life)
end
return 1
]]


return {
    prefix_add = { src = PREFIX_ADD },
    read   = { src = READ },
    store  = { src = STORE },
    purge  = { src = PURGE },
    lock   = { src = LOCK },
    unlock = { src = UNLOCK },
}
