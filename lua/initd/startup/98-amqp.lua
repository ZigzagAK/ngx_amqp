local _M = {
  _VERSION = "1.0.0",
  _MODULE_TYPE = "http"
}

local amqp = require "ngx_amqp"

function _M.startup()
  amqp.startup()
end

return _M