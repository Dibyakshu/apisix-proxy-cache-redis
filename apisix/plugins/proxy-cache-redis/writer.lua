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

-- Per-worker write queue.
--
-- Cosockets are not available in header_filter/body_filter, so writes leave
-- the request through a timer. Instead of one timer per response (which can
-- exhaust lua_max_pending_timers during a miss storm on a slow Redis), each
-- worker keeps one bounded queue per Redis target and at most
-- `write_concurrency` drainer timers, each sending batches of writes in a
-- single pipeline. When the queue is full (by item count or by bytes) new
-- writes are dropped and counted: caching is best-effort, serving is not.

local core     = require("apisix.core")
local storage  = require("apisix.plugins.proxy-cache-redis.storage")
local ngx      = ngx
local now      = ngx.now
local timer_at = ngx.timer.at
local pairs    = pairs

local BATCH_ITEMS = 32
local BATCH_BYTES = 4 * 1024 * 1024
local WARN_INTERVAL = 10

local _M = {}

local queues = setmetatable({}, { __mode = "k" })   -- storage opts -> queue


local function warn_dropped(q, reason)
    local t = now()
    if t - q.last_warn >= WARN_INTERVAL then
        core.log.warn("proxy-cache-redis: dropped ", q.dropped,
                      " cache write(s) so far (", reason, ")")
        q.last_warn = t
    end
end


local drain

local function start_drainer(opts, q)
    if q.running >= opts.write_concurrency then
        return
    end
    q.running = q.running + 1
    local ok, err = timer_at(0, drain, opts, q)
    if not ok then
        q.running = q.running - 1
        core.log.error("proxy-cache-redis: failed to start write drainer: ", err)
    end
end


local function drain_loop(opts, q)
    while q.head <= q.tail do
        local batch, bytes = {}, 0
        while q.head <= q.tail and #batch < BATCH_ITEMS and bytes < BATCH_BYTES do
            local item = q.items[q.head]
            q.items[q.head] = nil
            q.head = q.head + 1
            q.bytes = q.bytes - item.bytes
            bytes = bytes + item.bytes
            batch[#batch + 1] = item
        end

        if storage.is_down(opts) then
            q.dropped = q.dropped + #batch
            warn_dropped(q, "redis unavailable")
        else
            local st = storage.new(opts)
            local failed, err = st:run_batch(batch)
            st:release()
            if failed > 0 then
                q.dropped = q.dropped + failed
                if err ~= storage.ERR_BACKOFF then
                    storage.log_error("proxy-cache-redis: failed to write cache item(s): ",
                                      err)
                end
            end
        end
    end
end


-- `premature` (worker shutting down / reloading): still flush what is
-- queued; nginx bounds this by worker_shutdown_timeout. pcall keeps the
-- drainer count right even if something unexpected throws -- otherwise a
-- worker would believe its drainers are busy and never write again.
drain = function(premature, opts, q)
    local ok, err = pcall(drain_loop, opts, q)
    q.running = q.running - 1
    if not ok then
        storage.log_error("proxy-cache-redis: write drainer failed: ", err)
        -- keep going with whatever is still queued
        if q.head <= q.tail and not premature then
            start_drainer(opts, q)
        end
    end
end


-- item: { op = "store" | "unlock", ekey = ..., bytes = n, ... }
-- Returns false when the item was dropped because the queue is full.
function _M.push(opts, item)
    local q = queues[opts]
    if not q then
        q = { items = {}, head = 1, tail = 0, bytes = 0, running = 0,
              dropped = 0, last_warn = 0 }
        queues[opts] = q
    end

    if q.tail - q.head + 1 >= opts.max_pending_writes
            or q.bytes + item.bytes > opts.max_pending_bytes then
        q.dropped = q.dropped + 1
        warn_dropped(q, "write queue full")
        return false
    end

    q.tail = q.tail + 1
    q.items[q.tail] = item
    q.bytes = q.bytes + item.bytes

    start_drainer(opts, q)
    return true
end


-- For tests/diagnostics: every queue of this worker.
function _M.stats_all()
    local out = {}
    for opts, q in pairs(queues) do
        out[#out + 1] = { policy = opts.policy, pending = q.tail - q.head + 1,
                          bytes = q.bytes, dropped = q.dropped, running = q.running }
    end
    return out
end


-- For tests/diagnostics: queue depth and drop counter for one target.
function _M.stats(opts)
    local q = queues[opts]
    if not q then
        return { pending = 0, bytes = 0, dropped = 0, running = 0 }
    end
    return { pending = q.tail - q.head + 1, bytes = q.bytes,
             dropped = q.dropped, running = q.running }
end


return _M
