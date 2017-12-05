local _M = {
  _VERSION = "1.3.0",
  _MODULE_TYPE = "http"
}

local shdict = require "shdict"
local CONFIG = shdict.new("config")

local cjson = require "cjson"
local amqp = require "ngx_amqp"

function _M.startup()
  local consumers = CONFIG:object_get("amqp.consumers") or {}
  for i=1,#consumers
  do
    local consumer = consumers[i]
    local p,f = consumer.callback:match("(.+)%.([^%.]+)$")
    local ok, m = pcall(require, p)
    if ok then
      local callback = m[f]
      local ok, err = amqp.consume(consumer.opts, callback)
      if not ok then
        ngx.log(ngx.ERR, "AMQP consumer start: ", err, " ", cjson.encode(consumer))
      end
    else
      ngx.log(ngx.ERR, "AMQP consumer start: ", m)
    end
  end
end

return _M