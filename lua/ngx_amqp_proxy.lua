local _M = {
  _VERSION = "1.0.0"
}

local frame = require "frame"
local c     = require "consts"
local cjson = require "cjson"

local CONFIG = ngx.shared.config_s

local trace_ddl = CONFIG:get("amqp_proxy.trace_ddl") or true
local trace_dml = CONFIG:get("amqp_proxy.trace_dml") or false

function _M.proxy(upstream)
    local sock_down = ngx.req.socket()
    local sock_up = ngx.socket.tcp()

    local ok, err

    if upstream.host:match("^unix:") then
      ok, err = sock_up:connect(upstream.host)
    else
      ok, err = sock_up:connect(upstream.host, upstream.port)
    end

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
            self.where = "on send"
            return nil, self.err
          end
        else
          self.where = "on recv"
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

    sock_up:settimeout(1000)
    sock_down:settimeout(1000)

    local dml_ops = {
      [c.class.BASIC] = true
    }

    local vhost, user

    local ident = function()
      return (user or "none") .. ":" .. (vhost or "none")
    end

    local thr_func = function(ctx)
      while not ctx.sock.err or ctx.sock.err == "timeout"
      do
        local f, err = frame.consume_frame( {
          sock = ctx.sock
        } )
        if f then
          local is_dml = dml_ops[f.class_id] or f.type == c.frame.BODY_FRAME
          if (trace_dml and is_dml) or (trace_ddl and not is_dml) then
            ngx.log(ngx.INFO, "amqp [" .. ident() .. "] : " .. ctx.desc .. " : " .. cjson.encode(f))
          end
          if not user and f.method_id == c.method.connection.START_OK then
            user = f.method.response:match("(%w+)")
          elseif not vhost and f.method_id == c.method.connection.OPEN then
            vhost = f.method.virtual_host
          end
        elseif err then
          if err == "closed" then
            ngx.log(ngx.WARN, "amqp [" .. ident() .. "] : " .. ctx.desc .. " : connection closed " .. ctx.sock.where)
            break
          elseif err == "timeout" then
            goto continue
          end
          ngx.log(ngx.WARN, "amqp " .. ctx.desc .. " : " .. (err or "?"))
        end
::continue::
      end
    end

    local thr_up = ngx.thread.spawn(thr_func, { sock = request, desc = "client -> server" })
    local thr_down = ngx.thread.spawn(thr_func, { sock = response, desc = "client <- server" })

    ok, err = ngx.thread.wait(thr_up, thr_down)
    if not ok then
      ngx.log(ngx.ERR, "failed to wait: " .. err)
    end

    ngx.thread.kill(thr_up)
    ngx.thread.kill(thr_down)
  
    sock_up:close()
end

return _M