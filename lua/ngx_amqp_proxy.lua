local _M = {
  _VERSION = "1.2.0"
}

local frame = require "frame"
local c     = require "consts"
local cjson = require "cjson"

local CONFIG = ngx.shared.config_s

local c_text = {
  [c.DEFAULT_CHANNEL]           = "DEFAULT_CHANNEL",
  [c.DEFAULT_FRAME_SIZE]        = "DEFAULT_FRAME_SIZE",
  [c.DEFAULT_MAX_CHANNELS]      = "DEFAULT_MAX_CHANNELS",
  [c.DEFAULT_HEARTBEAT]         = "DEFAULT_HEARTBEAT",

  [c.PROTOCOL_VERSION_MAJOR]    = "PROTOCOL_VERSION_MAJOR",
  [c.PROTOCOL_VERSION_MINOR]    = "PROTOCOL_VERSION_MINOR",
  [c.PROTOCOL_VERSION_REVISION] = "PROTOCOL_VERSION_REVISION",
  [c.PROTOCOL_PORT]             = "PROTOCOL_PORT",
  [c.PROTOCOL_SSL_PORT]         = "PROTOCOL_SSL_PORT",

  state = {
    [c.state.CLOSED]        = "CLOSED",
    [c.state.ESTABLISHED]   = "ESTABLISHED",
    [c.state.CLOSE_WAIT]    = "CLOSE_WAIT"
  },

  frame = {
    [c.frame.METHOD_FRAME]    = "METHOD_FRAME",
    [c.frame.HEADER_FRAME]    = "HEADER_FRAME",
    [c.frame.BODY_FRAME]      = "BODY_FRAME",
    [c.frame.HEARTBEAT_FRAME] = "HEARTBEAT_FRAME",
    [c.frame.FRAME_MIN_SIZE]  = "FRAME_MIN_SIZE",
    [c.frame.FRAME_END]       = "FRAME_END"
  },

  method = {
    [c.class.CONNECTION] = { 
      [c.method.connection.START]     = "START",
      [c.method.connection.START_OK]  = "START_OK",
      [c.method.connection.SECURE]    = "SECURE",
      [c.method.connection.SECURE_OK] = "SECURE_OK",
      [c.method.connection.TUNE]      = "TUNE",
      [c.method.connection.TUNE_OK]   = "TUNE_OK",
      [c.method.connection.OPEN]      = "OPEN",
      [c.method.connection.OPEN_OK]   = "OPEN_OK",
      [c.method.connection.CLOSE]     = "CLOSE",
      [c.method.connection.CLOSE_OK]  = "CLOSE_OK",
      [c.method.connection.BLOCKED]   = "BLOCKED",
      [c.method.connection.UNBLOCKED] = "UNBLOCKED",
    },
    [c.class.CHANNEL] = {
      [c.method.channel.OPEN]     = "OPEN",
      [c.method.channel.OPEN_OK]  = "OPEN_OK",
      [c.method.channel.FLOW]     = "FLOW",
      [c.method.channel.FLOW_OK]  = "FLOW_OK",
      [c.method.channel.CLOSE]    = "CLOSE",
      [c.method.channel.CLOSE_OK] = "CLOSE_OK"
    },
    [c.class.EXCHANGE] = {
      [c.method.exchange.DECLARE]     = "DECLARE",
      [c.method.exchange.DECLARE_OK]  = "DECLARE_OK",
      [c.method.exchange.DELETE]      = "DELETE",
      [c.method.exchange.DELETE_OK]   = "DELETE_OK",
      [c.method.exchange.BIND]        = "BIND",
      [c.method.exchange.BIND_OK]     = "BIND_OK",
      [c.method.exchange.UNBIND]      = "UNBIND",
      [c.method.exchange.UNBIND_OK]   = "UNBIND_OK"
    },
    [c.class.QUEUE] = {
      [c.method.queue.DECLARE]     = "DECLARE",
      [c.method.queue.DECLARE_OK]  = "DECLARE_OK",
      [c.method.queue.BIND]        = "BIND",
      [c.method.queue.BIND_OK]     = "BIND_OK",
      [c.method.queue.PURGE]       = "PURGE",
      [c.method.queue.PURGE_OK]    = "PURGE_OK",
      [c.method.queue.DELETE]      = "DELETE",
      [c.method.queue.DELETE_OK]   = "DELETE_OK",
      [c.method.queue.UNBIND]      = "UNBIND",
      [c.method.queue.UNBIND_OK]   = "UNBIND_OK"
    },
    [c.class.BASIC] = {
      [c.method.basic.QOS]        = "QOS",
      [c.method.basic.QOS_OK]     = "QOS_OK",
      [c.method.basic.CONSUME]    = "CONSUME",
      [c.method.basic.CONSUME_OK] = "CONSUME_OK",
      [c.method.basic.CANCEL]     = "CANCEL",
      [c.method.basic.CANCEL_OK]  = "CANCEL_OK",
      [c.method.basic.PUBLISH]    = "PUBLISH",
      [c.method.basic.RETURN]     = "RETURN",
      [c.method.basic.DELIVER]    = "DELIVER",
      [c.method.basic.GET]        = "GET",
      [c.method.basic.GET_OK]     = "GET_OK",
      [c.method.basic.GET_EMPTY]  = "GET_EMPTY",
      [c.method.basic.ACK]        = "ACK",
      [c.method.basic.REJECT]     = "REJECT",
      [c.method.basic.RECOVER_ASYNC]  = "RECOVER_ASYNC",
      [c.method.basic.RECOVER]    = "RECOVER",
      [c.method.basic.RECOVER_OK] = "RECOVER_OK",
      [c.method.basic.NACK]       = "NACK"
    },
    [c.class.TX] = {
      [c.method.tx.SELECT]      = "SELECT",
      [c.method.tx.SELECT_OK]   = "SELECT_OK",
      [c.method.tx.COMMIT]      = "COMMIT",
      [c.method.tx.COMMIT_OK]   = "COMMIT_OK",
      [c.method.tx.ROLLBACK]    = "ROLLBACK",
      [c.method.tx.ROLLBACK_OK] = "ROLLBACK_OK"
    },
    [c.class.CONFIRM] = {
      [c.method.confirm.SELECT]     = "SELECT",
      [c.method.confirm.SELECT_OK]  = "SELECT_OK"
    }
  },

  class = {
    [c.class.CONNECTION]  = "CONNECTION",
    [c.class.CHANNEL]     = "CHANNEL",
    [c.class.EXCHANGE]    = "EXCHANGE",
    [c.class.QUEUE]       = "QUEUE",
    [c.class.BASIC]       = "BASIC",
    [c.class.TX]          = "TX",
    [c.class.CONFIRM]     = "CONFIRM"
  },

  flag = {
    [c.flag.CONTENT_TYPE]     = "CONTENT_TYPE",
    [c.flag.CONTENT_ENCODING] = "CONTENT_ENCODING",
    [c.flag.HEADERS]          = "HEADERS",
    [c.flag.DELIVERY_MODE]    = "DELIVERY_MODE",
    [c.flag.PRIORITY]         = "PRIORITY",
    [c.flag.CORRELATION_ID]   = "CORRELATION_ID",
    [c.flag.REPLY_TO]         = "REPLY_TO",
    [c.flag.EXPIRATION]       = "EXPIRATION",
    [c.flag.MESSAGE_ID]       = "MESSAGE_ID",
    [c.flag.TIMESTAMP]        = "TIMESTAMP",
    [c.flag.TYPE]             = "TYPE",
    [c.flag.USER_ID]          = "USER_ID",
    [c.flag.APP_ID]           = "APP_ID",
    [c.flag.RESERVED1]        = "RESERVED1"
  },

  err = {
    [c.err.REPLY_SUCCESS]       = "REPLY_SUCCESS",
    [c.err.CONTENT_TOO_LARGE]   = "CONTENT_TOO_LARGE",
    [c.err.NO_ROUTE]            = "NO_ROUTE",
    [c.err.NO_CONSUMERS]        = "NO_CONSUMERS",
    [c.err.CONNECTION_FORCED]   = "CONNECTION_FORCED",
    [c.err.INVALID_PATH]        = "INVALID_PATH",
    [c.err.ACCESS_REFUSED]      = "ACCESS_REFUSED",
    [c.err.NOT_FOUND]           = "NOT_FOUND",
    [c.err.RESOURCE_LOCKED]     = "RESOURCE_LOCKED",
    [c.err.PRECONDITION_FAILED] = "PRECONDITION_FAILED",
    [c.err.FRAME_ERROR]         = "FRAME_ERROR",
    [c.err.SYNTAX_ERROR]        = "SYNTAX_ERROR",
    [c.err.COMMAND_INVALID]     = "COMMAND_INVALID",
    [c.err.CHANNEL_ERROR]       = "CHANNEL_ERROR",
    [c.err.UNEXPECTED_FRAME]    = "UNEXPECTED_FRAME",
    [c.err.RESOURCE_ERROR]      = "RESOURCE_ERROR",
    [c.err.NOT_ALLOWED]         = "NOT_ALLOWED",
    [c.err.NOT_IMPLEMENTED]     = "NOT_IMPLEMENTED",
    [c.err.INTERNAL_ERROR]      = "INTERNAL_ERROR"
  }
}

local trace_ddl = CONFIG:get("amqp_proxy.trace_ddl")
if trace_ddl == nil then
    trace_ddl = true
end
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
          if not user and f.method_id == c.method.connection.START_OK then
            user = f.method.response:match("(%w+)")
          elseif not vhost and f.method_id == c.method.connection.OPEN then
            vhost = f.method.virtual_host
          end
          local is_dml = dml_ops[f.class_id] or f.type == c.frame.BODY_FRAME
          if (trace_dml and is_dml) or (trace_ddl and not is_dml) then
            local typ, class_id, method_id = f.type, f.class_id, f.method_id
            if typ then
              f.type = c_text.frame[typ]
            end
            if class_id then
              f.class_id = c_text.class[class_id]
              if f.class_id then
                f.method_id = c_text.method[class_id][method_id]
              end
            end
            ngx.log(ngx.INFO, "amqp [" .. ident() .. "] : " .. ctx.desc .. " : " .. cjson.encode(f))
          end
        elseif err then
          if err == "closed" then
            ngx.log(ngx.WARN, "amqp [" .. ident() .. "] : " .. ctx.desc .. " : connection closed " .. ctx.sock.where)
            break
          elseif err == "timeout" then
            goto continue
          elseif err == "connect event" then
            ngx.log(ngx.INFO, "amqp [" .. ident() .. "] : " .. ctx.desc .. " : connect")
            goto continue
          end
          ngx.log(ngx.ERR, "amqp " .. ctx.desc .. " : " .. (err or "?"))
          break
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