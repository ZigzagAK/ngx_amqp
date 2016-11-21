local _M = {
  _VERSION = "1.0.0",
  _MODULE_TYPE = "http"
}

local CONFIG = ngx.shared.config

function _M.config()
  CONFIG:set("amqp.async_queue_size", 2000)
  CONFIG:set("amqp.pool_size", 10)
  CONFIG:set("amqp.timeout", 60)
  CONFIG:set("amqp.retry", 1)
  CONFIG:set("amqp.host", "127.0.0.1")
  CONFIG:set("amqp.port", 5672)
  CONFIG:set("amqp.user", "root")
  CONFIG:set("amqp.password", "1111")
  CONFIG:set("amqp.vhost", "test")
  CONFIG:set("amqp.ssl", false)
  CONFIG:set("amqp.trace_ddl", false)
  CONFIG:set("amqp.trace_publish", false)
  CONFIG:set("amqp.conn_timeout", 30)
  CONFIG:set("amqp.read_timeout", 30)
  CONFIG:set("amqp.heartbeat", 60)
end

return _M
