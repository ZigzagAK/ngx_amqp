local _M = {
  _VERSION = "1.3.0"
}

local cjson = require "cjson"

local function callback(q, t)
  ngx.log(ngx.INFO, "AMQP ", q, " consumer: ", cjson.encode(t))
end

function _M.queue1_callback(t)
  callback("queue1", t)
end

function _M.queue2_callback(t)
  callback("queue2", t)
end

function _M.queue3_callback(t)
  callback("queue3", t)
end

return _M