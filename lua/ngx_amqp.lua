local _M = {
  _VERSION = "1.0.0"
}

local amqp  = require "amqp"
local cjson = require "cjson"

local CONFIG = ngx.shared.config
local AMQP   = ngx.shared.amqp

_M.connect_options = {
  host     = CONFIG:get("amqp.host"),
  port     = CONFIG:get("amqp.port"),
  ssl      = CONFIG:get("amqp.ssl"),
  user     = CONFIG:get("amqp.user"),
  password = CONFIG:get("amqp.password"),
  vhost    = CONFIG:get("amqp.vhost"),
  no_wait  = CONFIG:get("amqp.no_wait")
}

local pool_size        = CONFIG:get("amqp.pool_size")        or 1
local async_queue_size = CONFIG:get("amqp.async_queue_size") or 100
local timeout          = CONFIG:get("amqp.timeout")          or 5
local trace_ddl        = CONFIG:get("amqp.trace_ddl")        or true
local trace_publish    = CONFIG:get("amqp.trace_publish")    or false
local retry            = CONFIG:get("amqp.retry")            or 3

local function key_fn(o)
  return o.user .. "@" .. o.host .. ":" .. o.port .. ":" .. o.vhost
end

local function default(v, def)
  if v ~= nil then
    return v
  end
  return def
end

local function amqp_connect(opts)
  local ctx = amqp.new {
    role         = "producer",
    ssl          = opts.ssl,
    user         = opts.user,
    password     = opts.password,
    virtual_host = opts.vhost,
    no_wait      = opts.no_wait
  }
  if not ctx then
    return false, nil, "failed to create context"
  end

  local ok, err = ctx:connect(opts.host, opts.port)
  if not ok then
    return false, nil, err
  end

  ok, err = ctx:setup()
  if not ok then
    ctx:teardown()
    ctx:close()
    return false, nil, err
  end

  ctx.opts.add = opts
  ngx.log(ngx.INFO, "AMQP connected, endpoint=" .. key_fn(ctx.opts.add))

  return true, ctx, nil
end

local function amqp_disconnect(ctx)
  if ctx then
    if not ngx.worker.exiting() then
      ctx:teardown()
    end
    ctx:close()
    ngx.log(ngx.INFO, "AMQP disconnected, endpoint=" .. key_fn(ctx.opts.add))
  end
end

local function amqp_exchange_declare(ctx, req)
  local ok, err, err2 = amqp.exchange_declare(ctx, {
    exchange    = req.exch.exchange,
    typ         = default(req.exch.typ, "topic"),
    passive     = default(req.exch.passive, false),
    durable     = default(req.exch.durable, true),
    auto_delete = default(req.exch.auto_delete, false),
    internal    = default(req.exch.internal, false),
    no_wait     = default(req.exch.no_wait, false)
  })

  if trace_ddl then
    ngx.log(ngx.INFO, "AMQP declare exchange: endpoint=" .. key_fn(req.opts) .. ", exchange=" .. cjson.encode(req.exch))
  end

  if not ok then
    if type(err) == "number" then err = err2 end
    ngx.log(ngx.ERR, "AMQP declare exchange: " .. err)
  end

  return ok, err
end

local function amqp_queue_declare(ctx, req)
  local ok, err, err2 = amqp.queue_declare(ctx, {
    queue       = req.queue.name,
    passive     = default(req.queue.passive, false),
    durable     = default(req.queue.durable, true),
    exclusive   = default(req.queue.exclusive, false),
    auto_delete = default(req.queue.auto_delete, false),
    no_wait     = default(req.queue.no_wait, false)
  })

  if trace_ddl then
    ngx.log(ngx.INFO, "AMQP declare queue: endpoint=" .. key_fn(req.opts) .. ", queue=" .. cjson.encode(req.queue))
  end

  if not ok then
    if type(err) == "number" then err = err2 end
    ngx.log(ngx.ERR, "AMQP declare queue: " .. err)
  end

  return ok, err
end

local function amqp_queue_bind(ctx, req)
  local ok, err, err2 = amqp.queue_bind(ctx, {
    queue       = req.bind.queue,
    exchange    = req.bind.exchange,
    routing_key = default(req.bind.routing_key, ""),
    no_wait     = default(req.bind.no_wait, false)
  })

  if trace_ddl then
    ngx.log(ngx.INFO, "AMQP bind queue: endpoint=" .. key_fn(req.opts) .. ", opt=" .. cjson.encode(req.bind))
  end

  if not ok then
    if type(err) == "number" then err = err2 end
    ngx.log(ngx.ERR, "AMQP bind queue: " .. err)
  end

  return ok, err
end

local function amqp_queue_unbind(ctx, req)
  local ok, err, err2 = amqp.queue_unbind(ctx, {
    queue       = req.unbind.queue,
    exchange    = req.unbind.exchange,
    routing_key = default(req.unbind.routing_key, "")
  })

  if trace_ddl then
    ngx.log(ngx.INFO, "AMQP unbind queue: endpoint=" .. key_fn(req.opts) .. ", opt=" .. cjson.encode(req.unbind))
  end

  if not ok then
    if type(err) == "number" then err = err2 end
    ngx.log(ngx.ERR, "AMQP unbind queue: " .. err)
  end

  return ok, err
end

local function amqp_exchange_bind(ctx, req)
  local ok, err, err2 = amqp.exchange_bind(ctx, {
    destination = req.ebind.destination,
    source      = req.ebind.source,
    routing_key = default(req.ebind.routing_key, ""),
    no_wait     = default(req.ebind.no_wait, false)
  })

  if trace_ddl then
    ngx.log(ngx.INFO, "AMQP bind exchange: endpoint=" .. key_fn(req.opts) .. ", opt=" .. cjson.encode(req.ebind))
  end

  if not ok then
    if type(err) == "number" then err = err2 end
    ngx.log(ngx.ERR, "AMQP bind exchange: " .. err)
  end

  return ok, err
end

local function amqp_exchange_unbind(ctx, req)
  local ok, err, err2 = amqp.exchange_unbind(ctx, {
    destination = req.eunbind.destination,
    source      = req.eunbind.source,
    routing_key = default(req.eunbind.routing_key, ""),
    no_wait     = default(req.eunbind.no_wait, false)
  })

  if trace_ddl then
    ngx.log(ngx.INFO, "AMQP unbind exchange: endpoint=" .. key_fn(req.opts) .. ", opt=" .. cjson.encode(req.eunbind))
  end

  if not ok then
    if type(err) == "number" then err = err2 end
    ngx.log(ngx.ERR, "AMQP unbind exchange: " .. err)
  end

  return ok, err
end

local function amqp_exchange_delete(ctx, req)
  local ok, err, err2 = amqp.exchange_delete(ctx, {
    exchange  = req.edelete.exchange,
    if_unused = default(req.edelete.is_unused, true),
    no_wait   = default(req.edelete.no_wait, false)
  })

  if trace_ddl then
    ngx.log(ngx.INFO, "AMQP delete exchange: endpoint=" .. key_fn(req.opts) .. ", opt=" .. cjson.encode(req.edelete))
  end

  if not ok then
    if type(err) == "number" then err = err2 end
    ngx.log(ngx.ERR, "AMQP delete exchange: " .. err)
  end

  return ok, err
end

local function amqp_queue_delete(ctx, req)
  local ok, err, err2 = amqp.queue_delete(ctx, {
    queue     = req.qdelete.queue,
    if_empty  = default(req.qdelete.if_empty, false),
    if_unused = default(req.qdelete.is_unused, true),
    no_wait   = default(req.qdelete.no_wait, false)
  })

  if trace_ddl then
    ngx.log(ngx.INFO, "AMQP delete queue: endpoint=" .. key_fn(req.opts) .. ", opt=" .. cjson.encode(req.qdelete))
  end
  
  if not ok then
    if type(err) == "number" then err = err2 end
    ngx.log(ngx.ERR, "AMQP delete queue: " .. err)
  end

  return ok, err
end

local function amqp_publish_message(ctx, req)
  local ok, err = ctx:publish(req.mesg, req.exch)

  if trace_publish then
    ngx.log(ngx.INFO, "AMQP publish: endpoint=" .. key_fn(req.opts) .. ", exchange=" .. cjson.encode(req.exch) .. ", message=" .. req.mesg or "")
  end

  if not ok then
    ngx.log(ngx.ERR, "AMQP publish failed: " .. err)
  end

  return ok, err
end

local amqp_worker
amqp_worker = {

  queue = {},
  pool = {},

  queue_add = function(req)
    table.insert(amqp_worker.queue, req)
    AMQP:incr("amqp_worker.queue", 1, 0)
  end,

  queue_get = function()
    local req = table.remove(amqp_worker.queue)
    if req then
      AMQP:incr("amqp_worker.queue", -1)
    end
    return req
  end,
  
  thread_func = function(cache, num)
    ngx.log(ngx.INFO, "AMQP worker #" .. num .. " has been started")

    local yield = coroutine.yield
    local self = coroutine.running()

    while true
    do
:: continue ::
      yield(self)

      local req = amqp_worker.queue_get()

      if not req then
        if ngx.worker.exiting() then
          break
        end

        ngx.sleep(0.01)

        goto continue
      end

      local ok, err

      for _=1,retry
      do
        if req.forgot then
          goto continue
        end

        ok = true
        err = nil

        local key = key_fn(req.opts)
        local ctx = cache[key]

        if not ctx then
          ok, ctx, err = amqp_connect(req.opts)
          if ok then
            cache[key] = ctx
            AMQP:incr(key, 1, 0)
          else
            ngx.log(ngx.ERR, "AMQP connect: " .. err)
          end
        end

        if ok and req.exch and req.exch.declare then
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
      
        if ok and req.mesg then
          ok, err = amqp_publish_message(ctx, req)
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

        if not ok then
          amqp_disconnect(ctx)
          cache[key] = nil
          AMQP:incr(key, -1)
        end

        if ok then
          break
        end
      end

      req.err = err
      req.ready = true
    end
 
    for key, ctx in pairs(cache)
    do
      amqp_disconnect(ctx)
      cache[key] = nil
      AMQP:incr(key, -1)
    end

    ngx.log(ngx.INFO, "AMQP worker #" .. num .. " has been stopped")
  end,

  startup = function()
    ngx.log(ngx.INFO, "AMQP workers pool size=" .. pool_size)

    for i=1,pool_size
    do
      local thread = { cache = {} }
      thread.id = ngx.thread.spawn(amqp_worker.thread_func, thread.cache, i)
      table.insert(amqp_worker.pool, thread)
    end

    for _, thread in pairs(amqp_worker.pool)
    do
      ngx.thread.wait(thread.id)
    end
  end
}

local function wait_queue()
  local to = ngx.now() + timeout
  local queue_size = AMQP:get("amqp_worker.queue") or 0
  local remain = timeout

  while queue_size >= async_queue_size and remain > 0
  do
    ngx.sleep(0.01)
    queue_size = AMQP:get("amqp_worker.queue")
    remain = to - ngx.now()
  end

  if remain <= 0 then
    return 0, "AMQP throotled"
  end

  return remain, nil
end

local function wait(req, timeout)
  local to = ngx.now() + timeout

  amqp_worker.queue_add(req)
  
  while not req.ready and ngx.now() < to
  do
    ngx.sleep(0.001)
  end

  if not req.ready then
    req.forgot = true
    return false, "AMQP timeout"
  end

  return not req.err, req.err
end

function _M.exchange_declare(exchange, options)
  local remain, err = wait_queue()

  if remain == 0 then
    return false, err
  end

  local req = {
    opts = options or _M.connect_options,
    exch = exchange
  }
  
  req.exch.declare = true

  return wait(req, remain)
end

function _M.queue_declare(q, options)
  local remain, err = wait_queue()

  if remain == 0 then
    return false, err
  end

  local req = {
    opts  = options or _M.connect_options,
    queue = q
  }

  return wait(req, remain)
end

function _M.queue_bind(b, options)
  local remain, err = wait_queue()

  if remain == 0 then
    return false, err
  end

  local req = {
    opts = options or _M.connect_options,
    bind = b
  }

  return wait(req, remain)
end

function _M.queue_unbind(ub, options)
  local remain, err = wait_queue()

  if remain == 0 then
    return false, err
  end

  local req = {
    opts = options or _M.connect_options,
    unbind = ub
  }

  return wait(req, remain)
end

function _M.exchange_bind(eb, options)
  local remain, err = wait_queue()

  if remain == 0 then
    return false, err
  end

  local req = {
    opts = options or _M.connect_options,
    ebind = eb
  }

  return wait(req, remain)
end

function _M.exchange_unbind(eub, options)
  local remain, err = wait_queue()

  if remain == 0 then
    return false, err
  end

  local req = {
    opts = options or _M.connect_options,
    eunbind = eub
  }

  return wait(req, remain)
end

function _M.exchange_delete(ed, options)
  local remain, err = wait_queue()

  if remain == 0 then
    return false, err
  end

  local req = {
    opts = options or _M.connect_options,
    edelete = ed
  }

  return wait(req, remain)
end

function _M.queue_delete(qd, options)
  local remain, err = wait_queue()

  if remain == 0 then
    return false, err
  end

  local req = {
    opts = options or _M.connect_options,
    qdelete = qd
  }

  return wait(req, remain)
end

function _M.publish(exchange, message, async, options)
  local remain, err = wait_queue()

  if remain == 0 then
    return false, err
  end

  local req = {
    opts = options or _M.connect_options,
    exch = exchange,
    mesg = message,
  }

  if async then
    amqp_worker.queue_add(req)
    return true, nil
  end

  return wait(req, remain)
end

function _M.startup()
  local ok, err = ngx.timer.at(0, amqp_worker.startup)
  if not ok then
    error("AMQP failed to start workers: " .. err)
  end
end

function _M.info()
  local r = {}

  r.queue_size = AMQP:get("amqp_worker.queue") or 0
  r.publishers = {}

  for _, thread in pairs(amqp_worker.pool)
  do
    for endpoint, _ in pairs(thread.cache)
    do
      r.publishers[endpoint] = AMQP:get(endpoint)
    end
  end

  return r
end

return _M