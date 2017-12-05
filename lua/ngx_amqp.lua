local _M = {
  _VERSION = "1.3.0"
}

local amqp  = require "amqp"
local cjson = require "cjson"
local shdict = require "shdict"
local frame = require "frame"
local c = require "consts"
local bit = require "bit"

local band   = bit.band
local bor    = bit.bor
local lshift = bit.lshift

local CONFIG = ngx.shared.config

local AMQP_QUEUE = shdict.new("amqp_queue")
local AMQP_PUBLISHERS = shdict.new("amqp_publishers")

_M.connect_options = {
  host         = CONFIG:get("amqp.host"),
  port         = CONFIG:get("amqp.port"),
  ssl          = CONFIG:get("amqp.ssl"),
  user         = CONFIG:get("amqp.user"),
  password     = CONFIG:get("amqp.password"),
  vhost        = CONFIG:get("amqp.vhost"),
  heartbeat    = CONFIG:get("amqp.heartbeat") or 30,
  conn_timeout = CONFIG:get("amqp.conn_timeout") or 30,
  read_timeout = CONFIG:get("amqp.read_timeout") or 30
}

local pool_size        = CONFIG:get("amqp.pool_size") or 1
local async_queue_size = CONFIG:get("amqp.async_queue_size") or 100
local timeout          = CONFIG:get("amqp.timeout") or 5
local inactive_timeout = CONFIG:get("amqp.inactive_timeout") or 120
local trace_ddl        = CONFIG:get("amqp.trace_ddl")
if trace_ddl == nil then
  trace_ddl = true
end
local trace_publish    = CONFIG:get("amqp.trace_publish")
local retry            = CONFIG:get("amqp.retry") or 3

_M.connect_options.inactive_timeout = inactive_timeout

local function key_fn(o)
  return o.user .. "@" .. o.host .. ":" .. (o.port or "domain").. ":" .. o.vhost
end

local function is_yes(v, def)
  return v == true or v == 1 or ((v ~= false and type(v) == "string" and v:match("^[1Yy]$")) and true or (def or false))
end

local function amqp_connect(opts, role)
  local ctx = amqp.new {
    role             = role,
    ssl              = opts.ssl,
    user             = opts.user,
    password         = opts.password,
    virtual_host     = opts.vhost,
    connect_timeout  = opts.conn_timeout * 1000,
    read_timeout     = opts.read_timeout * 1000,
    inactive_timeout = opts.inactive_timeout or inactive_timeout,
    heartbeat        = opts.heartbeat,
    queue            = role == "consumer" and opts.queue or nil,
    no_wait          = false
  }
  if not ctx then
    return nil, "failed to create context"
  end

  local ok, err
  if opts.port then
    ok, err = ctx:connect(opts.host, opts.port)
  else
    ok, err = ctx:connect(opts.host)
  end
  if not ok then
    return nil, err
  end

  ok, err = ctx:setup()
  if not ok then
    ctx:teardown()
    ctx:close()
    return nil, err
  end

  ctx.opts.ext = opts
  ngx.log(ngx.INFO, "AMQP ", role, " connected, endpoint=", key_fn(ctx.opts.ext))

  return ctx
end

local function amqp_disconnect(ctx, reply_text)
  if not ctx then
    return
  end

  if not ngx.worker.exiting() then
    ctx:teardown({
      reply_code = c.err.CONNECTION_FORCED,
      reply_text = reply_text or "disconnect"
    })
  end

  ctx:close()

  ngx.log(ngx.INFO, "AMQP disconnected, endpoint=", key_fn(ctx.opts.ext))
end

local function compose_error(reply_code, reply_text)
  if reply_text then
    return "reply_code=" .. reply_code .. ", reply_text: " .. reply_text
  end
  return reply_code
end

local function amqp_exchange_declare(ctx, req)
  local ok, reply_code, reply_text = ctx:exchange_declare {
    exchange    = req.exch.exchange,
    typ         = req.exch.typ or "topic",
    passive     = is_yes(req.exch.passive),
    durable     = is_yes(req.exch.durable, true),
    auto_delete = is_yes(req.exch.auto_delete, false),
    internal    = is_yes(req.exch.internal)
  }

  if trace_ddl then
    ngx.log(ngx.INFO, "AMQP declare exchange: endpoint=", key_fn(req.opts), ", exchange=", cjson.encode(req.exch))
  end

  if not ok then
    local err = compose_error(reply_code, reply_text)
    ngx.log(ngx.ERR, "AMQP declare exchange: ", err)
    return nil, err
  end

  return true
end

local function amqp_queue_declare(ctx, req)
  local ok, reply_code, reply_text = ctx:queue_declare {
    queue       = req.queue.name,
    passive     = is_yes(req.queue.passive),
    durable     = is_yes(req.queue.durable, true),
    exclusive   = is_yes(req.queue.exclusive, false),
    auto_delete = is_yes(req.queue.auto_delete, false)
  }

  if trace_ddl then
    ngx.log(ngx.INFO, "AMQP declare queue: endpoint=", key_fn(req.opts), ", queue=", cjson.encode(req.queue))
  end

  if not ok then
    local err = compose_error(reply_code, reply_text)
    ngx.log(ngx.ERR, "AMQP declare queue: ", err)
    return nil, err
  end

  return true
end

local function amqp_queue_bind(ctx, req)
  local ok, reply_code, reply_text = ctx:queue_bind {
    queue       = req.bind.queue,
    exchange    = req.bind.exchange,
    routing_key = req.bind.routing_key or ""
  }

  if trace_ddl then
    ngx.log(ngx.INFO, "AMQP bind queue: endpoint=", key_fn(req.opts), ", opt=", cjson.encode(req.bind))
  end

  if not ok then
    local err = compose_error(reply_code, reply_text)
    ngx.log(ngx.ERR, "AMQP bind queue: ", err)
    return nil, err
  end

  return true
end

local function amqp_queue_unbind(ctx, req)
  local ok, reply_code, reply_text = ctx:queue_unbind {
    queue       = req.unbind.queue,
    exchange    = req.unbind.exchange,
    routing_key = req.unbind.routing_key or ""
  }

  if trace_ddl then
    ngx.log(ngx.INFO, "AMQP unbind queue: endpoint=", key_fn(req.opts), ", opt=", cjson.encode(req.unbind))
  end

  if not ok then
    local err = compose_error(reply_code, reply_text)
    ngx.log(ngx.ERR, "AMQP unbind queue: ", err)
    return nil, err
  end

  return true
end

local function amqp_exchange_bind(ctx, req)
  local ok, reply_code, reply_text = ctx:exchange_bind {
    destination = req.ebind.destination,
    source      = req.ebind.source,
    routing_key = req.ebind.routing_key or ""
  }

  if trace_ddl then
    ngx.log(ngx.INFO, "AMQP bind exchange: endpoint=", key_fn(req.opts), ", opt=", cjson.encode(req.ebind))
  end

  if not ok then
    local err = compose_error(reply_code, reply_text)
    ngx.log(ngx.ERR, "AMQP bind exchange: ", err)
    return nil, err
  end

  return true
end

local function amqp_exchange_unbind(ctx, req)
  local ok, reply_code, reply_text = ctx:exchange_unbind {
    destination = req.eunbind.destination,
    source      = req.eunbind.source,
    routing_key = req.eunbind.routing_key or ""
  }

  if trace_ddl then
    ngx.log(ngx.INFO, "AMQP unbind exchange: endpoint=", key_fn(req.opts), ", opt=", cjson.encode(req.eunbind))
  end

  if not ok then
    local err = compose_error(reply_code, reply_text)
    ngx.log(ngx.ERR, "AMQP unbind exchange: ", err)
    return nil, err
  end

  return true
end

local function amqp_exchange_delete(ctx, req)
  local ok, reply_code, reply_text = ctx:exchange_delete {
    exchange  = req.edelete.exchange,
    if_unused = is_yes(req.edelete.is_unused, true)
  }

  if trace_ddl then
    ngx.log(ngx.INFO, "AMQP delete exchange: endpoint=", key_fn(req.opts), ", opt=", cjson.encode(req.edelete))
  end

  if not ok then
    local err = compose_error(reply_code, reply_text)
    ngx.log(ngx.ERR, "AMQP delete exchange: ", err)
    return nil, err
  end

  return true
end

local function amqp_queue_delete(ctx, req)
  local ok, reply_code, reply_text = ctx:queue_delete {
    queue     = req.qdelete.queue,
    if_empty  = is_yes(req.qdelete.if_empty),
    if_unused = is_yes(req.qdelete.is_unused, true)
  }

  if trace_ddl then
    ngx.log(ngx.INFO, "AMQP delete queue: endpoint=", key_fn(req.opts), ", opt=", cjson.encode(req.qdelete))
  end

  if not ok then
    local err = compose_error(reply_code, reply_text)
    ngx.log(ngx.ERR, "AMQP delete queue: ", err)
    return nil, err
  end

  return true
end

local function amqp_publish_message(ctx, req)
  local ok, err = ctx:publish(req.mesg, req.exch, req.props)

  if trace_publish then
    ngx.log(ngx.INFO, "AMQP publish: endpoint=", key_fn(req.opts),
                                  ", exchange=", cjson.encode(req.exch),
                                   ", message=", req.mesg,
                                ", properties=", cjson.encode(req.props))
  end

  if not ok then
    ngx.log(ngx.ERR, "AMQP publish failed: ", err)
    return nil, err
  end

  return true
end

local function queue_add(req)
  return AMQP_QUEUE:object_rpush("Q", req)
end

local function queue_get()
  return AMQP_QUEUE:object_lpop("Q")
end

local function try_send_heartbeat(amqp_conn)
  local now = ngx.now()
  if now - amqp_conn.hb.last < amqp_conn.ctx.opts.ext.heartbeat then
    return true
  end

  amqp_conn.hb.timeouts = bor(lshift(amqp_conn.hb.timeouts, 1), 1)
  amqp_conn.hb.last = now

  local ok, err = frame.wire_heartbeat(amqp_conn.ctx)
  if not ok then
    return false, "AMQP sent heartbeat error: " .. err
  end

  return true
end

local function check_heartbeat_timeout(amqp_conn)
  local timedout = amqp_conn.ctx:timedout(amqp_conn.hb.timeouts)
  return not timedout, timedout and "heartbeat timeout" or nil
end

local function check_frame(amqp_conn, f)
  if f.type == c.frame.METHOD_FRAME then
    if f.class_id == c.class.CHANNEL then
      if f.method_id == c.method.channel.CLOSE then
        return false, "AMQP channel closed"
      end
    elseif f.class_id == c.class.CONNECTION then
      if f.method_id == c.method.connection.CLOSE then
        return false, "AMQP connection closed"
      end
    end
  elseif f.type == c.frame.HEARTBEAT_FRAME then
    amqp_conn.hb.last = ngx.now()
    amqp_conn.hb.timeouts = band(lshift(amqp_conn.hb.timeouts, 1), 0)
    local ok, err = frame.wire_heartbeat(amqp_conn.ctx)
    if not ok then
      return false, "AMQP heartbeat response send error: " .. (err or "?")
    end
  end
  return true
end

local function consume_frame(amqp_conn)
  local f, err = frame.consume_frame(amqp_conn.ctx)
  if f then
    return check_frame(amqp_conn, f)
  end

  if err == "timeout" then
    return true
  end

  if err == "wantread" then
    return nil, "AMQP SSL socket needs to do handshake again"
  end

  return nil, "unexpected error"
end

local function consume(amqp_conn)
  return consume_frame(amqp_conn)
end

local function amqp_pool(amqp_conn)
  amqp_conn.ctx.sock:settimeout(10)

  local ok, err = consume(amqp_conn)

  -- restore timeout
  amqp_conn.ctx.sock:settimeout(amqp_conn.ctx.opts.ext.read_timeout)

  if ok then
    ok, err = try_send_heartbeat(amqp_conn)
    if ok then
      ok, err = check_heartbeat_timeout(amqp_conn)
    end
  end

  return ok, err
end

local amqp_worker
amqp_worker = {

  pool = {},

  thread_func = function(cache, num)
    ngx.log(ngx.INFO, "AMQP worker #", num, " has been started")

    local yield = coroutine.yield
    local self = coroutine.running()

    while not ngx.worker.exiting()
    do
:: continue ::
      yield(self)

      local ok, err

      local req = queue_get()

      if not req then
        for key, amqp_conn in pairs(cache)
        do
          -- check for available frames
          ok, err = amqp_pool(amqp_conn)
          if not ok then
            ngx.log(ngx.ERR, "#", num, " AMQP error: ", err or "?")
            amqp_disconnect(amqp_conn.ctx, err)
            cache[key] = nil
          elseif amqp_conn.last_op + amqp_conn.ctx.opts.ext.inactive_timeout < ngx.now() then
            ngx.log(ngx.INFO, "#", num, " AMQP close inactive connection")
            amqp_disconnect(amqp_conn.ctx, "inactive")
            cache[key] = nil
          end
        end

        if not ok then
          ngx.sleep(0.01)
        end

        goto continue
      end

      for _=1,retry
      do
        local status = AMQP_QUEUE:object_get(req.status_key)
        if status and status.forgot then
          goto continue
        end

        local key = key_fn(req.opts)
        local amqp_conn = cache[key] or {}

        if not amqp_conn.ctx then
          -- no cached connection
          amqp_conn.ctx, err = amqp_connect(req.opts, "producer")
          if amqp_conn.ctx then
            amqp_conn.hb = { last = ngx.now(), timeouts = 0 }
            cache[key] = amqp_conn
            AMQP_PUBLISHERS:incr(key, 1, 0)
          else
            ngx.log(ngx.ERR, "#", num, " AMQP connect: ", err or "?")
          end
        end

        amqp_conn.last_op = ngx.now()

        local ctx = amqp_conn.ctx
        ok = true

        if req.exch and req.exch.declare then
          ok, err = amqp_exchange_declare(ctx, req)
        end

        if ok and req.queue then
          ok, err = amqp_queue_declare(ctx, req)
        end

        if ok and req.bind then
          ok, err = amqp_queue_bind(ctx, req)
        end

        if ok and req.ebind then
          ok, err = amqp_exchange_bind(ctx, req)
        end

        if ok and req.unbind then
          ok, err = amqp_queue_unbind(ctx, req)
        end

        if ok and req.eunbind then
          ok, err = amqp_exchange_unbind(ctx, req)
        end

        if ok and req.edelete then
          ok, err = amqp_exchange_delete(ctx, req)
        end

        if ok and req.qdelete then
          ok, err = amqp_queue_delete(ctx, req)
        end

        if ok and req.mesg then
          ok, err = amqp_publish_message(ctx, req)
        end

        if ok then
          amqp_conn.hb.last = ngx.now()
          break
        end

        amqp_disconnect(ctx, err)
        cache[key] = nil
        AMQP_PUBLISHERS:incr(key, -1)
      end

      if req.expires then
        AMQP_QUEUE:object_set(req.status_key, {
          status = ok,
          err = err
        }, req.expires - ngx.now())
      end
    end

    for key, amqp_conn in pairs(cache)
    do
      amqp_disconnect(amqp_conn.ctx, "shutdown")
      cache[key] = nil
      AMQP_PUBLISHERS:incr(key, -1)
    end

    ngx.log(ngx.INFO, "AMQP worker #", num, " has been stopped")
  end,

  startup = function()
    ngx.log(ngx.INFO, "AMQP workers pool size=", pool_size)

    for i=1,pool_size
    do
      local thread = { cache = {} }
      thread.id = ngx.thread.spawn(amqp_worker.thread_func, thread.cache, i)
      amqp_worker.pool[i] = thread
    end

    for i=1,pool_size
    do
      ngx.thread.wait(amqp_worker.pool[i].id)
    end
  end
}

local function wait_queue()
  local expires = ngx.now() + timeout
  local now

  repeat
    local queue_size = AMQP_QUEUE:llen("Q") or 0
    if queue_size >= async_queue_size then
      ngx.sleep(0.01)
    end
    now = ngx.now()
  until queue_size < async_queue_size or now >= expires

  if now < expires then
    return expires - now
  end

  return 0, "AMQP throotled"
end

local function wait(req, remains)
  req.expires = ngx.now() + remains
  req.status_key =  "S:" .. AMQP_QUEUE:incr("SEQNO", 1, 0)

  local resp

  local len, err = queue_add(req)
  if not len then
    -- no memory
    return nil, err
  end

  repeat
    ngx.sleep(0.001)
    resp = AMQP_QUEUE:object_get(req.status_key)
  until resp or ngx.now() > req.expires

  if not resp then
    AMQP_QUEUE:object_set(req.status_key, {
      forgot = true
    }, timeout * len)
    return false, "AMQP timeout"
  end

  return resp.status, resp.err
end

function _M.exchange_declare(exchange, options)
  local remains, err = wait_queue()

  if remains == 0 then
    return false, err
  end

  local req = {
    opts = options or _M.connect_options,
    exch = exchange
  }

  req.exch.declare = true

  return wait(req, remains)
end

function _M.queue_declare(q, options)
  local remains, err = wait_queue()

  if remains == 0 then
    return false, err
  end

  local req = {
    opts  = options or _M.connect_options,
    queue = q
  }

  return wait(req, remains)
end

function _M.queue_bind(b, options)
  local remains, err = wait_queue()

  if remains == 0 then
    return false, err
  end

  local req = {
    opts = options or _M.connect_options,
    bind = b
  }

  return wait(req, remains)
end

function _M.queue_unbind(ub, options)
  local remains, err = wait_queue()

  if remains == 0 then
    return false, err
  end

  local req = {
    opts = options or _M.connect_options,
    unbind = ub
  }

  return wait(req, remains)
end

function _M.exchange_bind(eb, options)
  local remains, err = wait_queue()

  if remains == 0 then
    return false, err
  end

  local req = {
    opts = options or _M.connect_options,
    ebind = eb
  }

  return wait(req, remains)
end

function _M.exchange_unbind(eub, options)
  local remains, err = wait_queue()

  if remains == 0 then
    return false, err
  end

  local req = {
    opts = options or _M.connect_options,
    eunbind = eub
  }

  return wait(req, remains)
end

function _M.exchange_delete(ed, options)
  local remains, err = wait_queue()

  if remains == 0 then
    return false, err
  end

  local req = {
    opts = options or _M.connect_options,
    edelete = ed
  }

  return wait(req, remains)
end

function _M.queue_delete(qd, options)
  local remains, err = wait_queue()

  if remains == 0 then
    return false, err
  end

  local req = {
    opts = options or _M.connect_options,
    qdelete = qd
  }

  return wait(req, remains)
end

function _M.publish(exchange, message, async, properties, options)
  local remains, err = wait_queue()

  if remains == 0 then
    return false, err
  end

  local req = {
    opts  = options or _M.connect_options,
    exch  = exchange,
    mesg  = message,
    props = properties
  }

  if async then
    return queue_add(req)
  end

  return wait(req, remains)
end

function _M.startup()
  local ok, err = ngx.timer.at(0, amqp_worker.startup)
  if not ok then
    error("AMQP failed to start workers: " .. err)
  end
end

function _M.info()
  local r = {}

  r.queue_size = AMQP_QUEUE:llen("Q") or 0
  r.publishers = {}

  for _, key in pairs(AMQP_PUBLISHERS:get_keys())
  do
    r.publishers[key] = AMQP_PUBLISHERS:get(key)
  end

  return r
end

function _M.get_properties()
  local req_headers = ngx.req.get_headers()
  local t = {}
  for k, v in pairs(req_headers)
  do
    local amqp_header = k:match("^amqp%-(.+)$")
    if amqp_header then
      if amqp_header == "headers" then
        v = cjson.decode(v)
      end
      t[amqp_header:gsub("-","_")] = v
    end
  end
  return t
end

function _M.consume(opts, callback, options)
  if not opts then
    return nil, "no opts"
  end
  if not opts.queue then
    return nil, "no opts.queue"
  end
  if not callback then
    return nil, "no callback"
  end

  local consume_loop = function()
    ngx.log(ngx.INFO, "AMQP start consumer: ", cjson.encode(opts))

    local o = {}
    for k,v in pairs(options or _M.connect_options) do o[k] = v end

    o.queue = opts.queue

    local amqp_conn, err = amqp_connect(o, "consumer")
    if not amqp_conn then
      return nil, err
    end

    local ok, reply_code, reply_text = amqp_conn:basic_consume {
      no_ack = is_yes(opts.auto_ack),
      exclusive = opts.exclusive
    }
    if not ok then
      amqp_disconnect(amqp_conn)
      return nil, compose_error(reply_code, reply_text)
    end

    ok, err = amqp_conn:consume_loop(callback)
    if not ok then
      ngx.log(ngx.WARN, "AMQP consumer: ", err)
    end

    amqp_disconnect(amqp_conn, "disconnect")
  end

  local consume
  consume = function()
    consume_loop()
    if not ngx.worker.exiting() then
      ngx.timer.at(1, consume)
    end
  end

  return ngx.timer.at(0, consume)
end

do
  -- turn off any log messages inside the library
  local logger = require "logger"
  logger.set_level(0)
end

return _M