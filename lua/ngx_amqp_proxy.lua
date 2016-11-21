local _M = {
  _VERSION = "1.0.0"
}

local frame = require "frame"
local c     = require "consts"
local cjson = require "cjson"

function _M.proxy()
    --local upstream = require "ngx.upstream"


    --[[local servers = upstream.get_servers("amqp")
    if not servers then
      ngx.log(ngx.ERR, "No amqp upstream")
      ngx.exit(444)
    end

    local server;
    for _, peer in pairs(servers)
    do
      if not peer.down then
        server = x
        break
      end
    end

    if not server then
      ngx.log(ngx.ERR, "No alive server available")
      ngx.exit(444)
    end--]]
    
    local server = "192.168.2.12:5672"

    local host, port = server:match("^([^:]+):([0-9]+)$")

    local sock_down = ngx.req.socket()
    local sock_up = ngx.socket.tcp()

    local ok, err = sock_up:connect(host, tonumber(port))
    if not ok then
      ngx.log(ngx.ERR, "failed to connect: ", err)
      ngx.exit(444)
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
    
    local thr_func = function(ctx)
      while not ctx.err or ctx.err == "timeout"
      do
        local f, err = frame.consume_frame( {
          sock = ctx.sock
        } )
        if f then
          ngx.log(ngx.INFO, "amqp " .. ctx.desc .. " : " .. cjson.encode(f))
        elseif err and err ~= "timeout" then
          ngx.log(ngx.WARN, "amqp " .. ctx.desc .. " : " .. (err or "?"))
        end
      end
    end

    local thr_up = ngx.thread.spawn(thr_func, { sock = request, desc = "request" })
    local thr_down = ngx.thread.spawn(thr_func, { sock = response, desc = "response" })

    local ok, err = ngx.thread.wait(thr_up, thr_down)
    if not ok then
      ngx.log(ngx.ERR, "failed to wait: " .. err)
    end

    ngx.thread.kill(thr_up)
    ngx.thread.kill(thr_down)
  
    sock_up:close()
end

return _M