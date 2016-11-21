local _M = {
  _VERSION = "1.0.0",
  _MODULE_TYPE = "stream"
}

local CONFIG = ngx.shared.config_s

function _M.config()
  CONFIG:set("amqp_proxy.upstream_host", "127.0.0.1")
  CONFIG:set("amqp_proxy.upstream_port", 5670)
  CONFIG:set("amqp_proxy.trace_ddl", true)
  CONFIG:set("amqp_proxy.trace_dml", false)
end

return _M