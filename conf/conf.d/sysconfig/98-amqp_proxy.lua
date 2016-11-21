local _M = {
  _VERSION = "1.0.0",
  _MODULE_TYPE = "stream"
}

local CONFIG = ngx.shared.config_s

function _M.config()
  CONFIG:set("amqp_proxy.upstream_host", "192.168.2.12")
  CONFIG:set("amqp_proxy.upstream_port", 5672)
  CONFIG:set("amqp_proxy.trace_ddl", true)
  CONFIG:set("amqp_proxy.trace_dml", true)
end

return _M