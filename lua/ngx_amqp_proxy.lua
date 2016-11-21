local _M = {
  _VERSION = "1.0.0"
}

local frame = require "frame"
local c     = require "consts"
local cjson = require "cjson"

local CONFIG = ngx.shared.config_s

local upstream_host = CONFIG:get("amqp_proxy.upstream_host")
local upstream_port = CONFIG:get("amqp_proxy.upstream_port") or 5672

local trace_ddl = CONFIG:get("amqp_proxy.trace_ddl") or true
local trace_dml = CONFIG:get("amqp_proxy.trace_dml") or false

function _M.proxy()
    local sock_down = ngx.req.socket()
    local sock_up = ngx.socket.tcp()

    local ok, err = sock_up:connect(upstream_host, upstream_port)
    if not ok then
      ngx.log(ngx.ERR, "failed to connect: ", err)
      return
    end

    local mt = { __index = {
      receive = function(self, size)
        local data
        data, self.err = self.from:receive(size)
        if data then
          local bytes
          bytes, self.err = self.to:send(data)
          if not bytes then
            return nil, self.err
          end
        end
        return data, self.err
      end
    } }

    local request = {
      from = sock_down,
      to = sock_up,
    }
    local response = {
      from = sock_up,
      to = sock_down,
    }

    setmetatable(request, mt)
    setmetatable(response, mt)

    sock_up:settimeout(100)
    sock_down:settimeout(100)

    local dml_ops = {
      [c.class.BASIC] = true
    }
    
    local thr_func = function(ctx)
      while not ctx.err or ctx.err == "timeout"
      do
        local f, err = frame.consume_frame( {
          sock = ctx.sock
        } )
        if f then
          local is_dml = dml_ops[f.class_id] or f.type == c.frame.BODY_FRAME
          if (trace_dml and is_dml) or (trace_ddl and not is_dml) then
            ngx.log(ngx.INFO, "amqp " .. ctx.desc .. " : " .. cjson.encode(f))
          end
        elseif err and err ~= "timeout" then
          ngx.log(ngx.WARN, "amqp " .. ctx.desc .. " : " .. (err or "?"))
        end
      end
    end

    local thr_up = ngx.thread.spawn(thr_func, { sock = request, desc = "request" })
    local thr_down = ngx.thread.spawn(thr_func, { sock = response, desc = "response" })

    ok, err = ngx.thread.wait(thr_up, thr_down)
    if not ok then
      ngx.log(ngx.ERR, "failed to wait: " .. err)
    end

    ngx.thread.kill(thr_up)
    ngx.thread.kill(thr_down)
  
    sock_up:close()
end

return _M