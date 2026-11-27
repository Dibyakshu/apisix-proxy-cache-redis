-- Test stand-in for apisix.plugin: plugin_attr comes from a swappable table
-- (APISIX reads it from config.yaml).
local _M = { attrs = {} }

function _M.plugin_attr(name)
    return _M.attrs[name]
end

return _M
