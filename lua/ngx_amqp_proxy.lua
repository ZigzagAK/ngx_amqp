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
    
    local server = "127.0.0.1:5672"

    local host, port = server:match("^([^:]+):([0-9]+)$")

    local downstream_sock = ngx.req.socket()
    local upstream_sock = ngx.socket.tcp()

    local ok, err = upstream_sock:connect(host, tonumber(port))
    if not ok then
      ngx.log(ngx.ERR, "failed to connect: ", err)
      ngx.exit(444)
    end

    local mt = { __index = {
      receive = function(self, size)
        local data, err = self.from:receive(size)
        if data then
          local bytes, err = self.to:send(data)
          if not bytes then
            ngx.log(ngx.ERR, err)
            ngx.exit(444)
          end
        end
        return data, err
      end,
      send = function(self, data)
        return self.from:send(data)
      end
    } }

    local request = {
      from = downstream_sock,
      to = upstream_sock,      
    }
    local response = {
      from = upstream_sock,
      to = downstream_sock,      
    }

    setmetatable(request, mt)
    setmetatable(response, mt)

    upstream_sock:settimeout(100)
    downstream_sock:settimeout(100)
    
    local conn_is_estabilished
    local no_wait = {
      [c.class.CONNECTION] = {
        [c.method.connection.TUNE_OK] = true
      },
      [c.class.BASIC] = {
        [c.method.basic.PUBLISH] = true
      }
    }
    
    while true
    do
:: continue ::
      local f, err = frame.consume_frame( {
        sock = request
      } )

      if not conn_is_estabilished then
        conn_is_estabilished = true
        goto consume
      end
    
      if not f then
        if err then
          if err ~= "timeout" then
            ngx.log(ngx.ERR, "amqp consume error : " .. err)
            break
          end
          goto consume
        end
      end

      ngx.log(ngx.INFO, "amqp request : " .. cjson.encode(f))
  
      if no_wait[f.class_id] and f.method_id and no_wait[f.class_id][f.method_id] then
        goto continue
      end

      if f.class_id == c.class.BASIC then
        if f.type == c.frame.HEADER_FRAME then
          goto continue
        end
      end

      if f.type == c.frame.BODY_FRAME then
        goto continue
      end

:: consume ::
      f, err = frame.consume_frame( {
        sock = response
      })

      if not f then
        if err then
          if err ~= "timeout" then
            ngx.log(ngx.ERR, "amqp consume error : " .. err)
            break
          end
          goto continue
        end
      end
      
      ngx.log(ngx.INFO, "amqp response : " .. cjson.encode(f))    
    end

    upstream_sock:close()
end

return _M