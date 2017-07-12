local _M = {
  _VERSION = "1.3.0",
  _MODULE_TYPE = "http"
}

local shdict = require "shdict"
local CONFIG = shdict.new("config")

function _M.config()
  CONFIG:object_set("amqp.consumers", {
    {
      opts = {
        queue = "queue1",
        exclusive = false,
        auto_ack = false
      },
      callback = "ngx_amqp_consumer.queue1_callback"
    },
    {
      opts = {
        queue = "queue2",
        exclusive = false,
        auto_ack = false
      },
      callback = "ngx_amqp_consumer.queue2_callback"
    },
    {
      opts = {
        queue = "queue3",
        exclusive = false,
        auto_ack = false
      },
      callback = "ngx_amqp_consumer.queue3_callback"
    }
  })
end

return _M