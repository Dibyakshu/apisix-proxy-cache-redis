-- Minimal test stand-in for apisix.core: only what proxy-cache-redis, the
-- upstream proxy-cache plugin and the real apisix/utils/rediscluster.lua +
-- proxy-cache/util.lua touch.
local cjson = require("cjson.safe")
local jsonschema = require("jsonschema")
local new_tab = require("table.new")
local ngx = ngx
local arg = ngx.arg
local concat_tab = table.concat
local str_sub = string.sub

local _M = {}

local function logger(level)
    return function(...) ngx.log(level, ...) end
end
_M.log = { info = logger(ngx.INFO), warn = logger(ngx.WARN),
           error = logger(ngx.ERR), debug = logger(ngx.DEBUG) }

_M.json = {
    encode = function(v) return cjson.encode(v) end,
    decode = cjson.decode,
    delay_encode = function(v) return cjson.encode(v) end,
}

local validators = setmetatable({}, {__mode = "k"})
_M.schema = {
    check = function(schema, json)
        local v = validators[schema]
        if not v then
            v = jsonschema.generate_validator(schema)
            validators[schema] = v
        end
        return v(json)
    end,
}

-- for the upstream proxy-cache plugin's check_schema (memory zone lookup)
_M.config = {
    local_conf = function()
        return { apisix = { proxy_cache = { zones = {
            { name = "memory_cache", memory_size = "50m" },
        } } } }
    end,
}

-- apisix.core.ctx.register_var: record getters so tests can read them
_M.ctx = { vars = {} }
function _M.ctx.register_var(name, getter)
    _M.ctx.vars[name] = getter
end

local function deepcopy(t)
    if type(t) ~= "table" then return t end
    local r = {}
    for k, v in pairs(t) do r[k] = deepcopy(v) end
    return r
end
_M.table = {
    deepcopy = deepcopy,
    try_read_attr = function(tab, ...)
        for i = 1, select('#', ...) do
            local k = select(i, ...)
            if type(tab) ~= "table" then return nil end
            tab = tab[k]
        end
        return tab
    end,
}

_M.tablepool = {
    fetch = function(_, narr, nrec) return new_tab(narr, nrec) end,
    release = function() end,
}

_M.utils = {
    parse_ipv4 = function(s) return s:match("^%d+%.%d+%.%d+%.%d+$") ~= nil end,
    parse_ipv6 = function(s) return s:find(":", 1, true) ~= nil end,
    parse_addr = function(addr)
        local host, port = addr:match("^(.+):(%d+)$")
        if not host then return addr, nil end
        return host, tonumber(port)
    end,
}

-- copied from apisix/core/response.lua
_M.response = {}
function _M.response.set_header(k, v)
    ngx.header[k] = v
end

function _M.response.hold_body_chunk(ctx, hold_the_copy, max_resp_body_bytes, body_buffer_key)
    local body_buffer
    local chunk, eof = arg[1], arg[2]
    local buffer_key = body_buffer_key or ctx._plugin_name

    if not ctx._body_buffer then
        ctx._body_buffer = {}
    end

    if type(chunk) == "string" and chunk ~= "" then
        body_buffer = ctx._body_buffer[buffer_key]
        if body_buffer and body_buffer.done then
            return nil
        end

        if not body_buffer then
            body_buffer = { chunk, n = 1, bytes = #chunk }
            ctx._body_buffer[buffer_key] = body_buffer
        else
            local n = body_buffer.n + 1
            body_buffer.n = n
            body_buffer[n] = chunk
            body_buffer.bytes = body_buffer.bytes + #chunk
        end

        if max_resp_body_bytes and body_buffer.bytes >= max_resp_body_bytes then
            local body_data = concat_tab(body_buffer, "", 1, body_buffer.n)
            body_data = str_sub(body_data, 1, max_resp_body_bytes)
            body_buffer.done = true
            return body_data
        end
    end

    if eof then
        body_buffer = ctx._body_buffer[buffer_key]
        if not body_buffer then
            if max_resp_body_bytes and #chunk >= max_resp_body_bytes then
                chunk = str_sub(chunk, 1, max_resp_body_bytes)
            end
            return chunk
        end
        if body_buffer.done then
            return nil
        end

        local body_data = concat_tab(body_buffer, "", 1, body_buffer.n)
        ctx._body_buffer[buffer_key] = nil
        return body_data
    end

    if not hold_the_copy then
        arg[1] = nil
    end
    return nil
end

return _M
