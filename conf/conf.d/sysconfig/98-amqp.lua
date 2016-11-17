local _M = {
  _VERSION = "1.0.0"
}

local CONFIG = ngx.shared.config

function _M.config()
  CONFIG:set("amqp.async_queue_size", 1000)
  CONFIG:set("amqp.publisher_pool_size", 1)
  CONFIG:set("amqp.publisher_timeout", 1)
  CONFIG:set("amqp.host", "127.0.0.1")
  CONFIG:set("amqp.port", 5672)
  CONFIG:set("amqp.user", "root")
  CONFIG:set("amqp.password", "1111")
  CONFIG:set("amqp.vhost", "/")
  CONFIG:set("amqp.ssl", false)
  CONFIG:set("amqp.trace_messages", true)
  CONFIG:set("amqp.no_wait", false)
end

return _M
