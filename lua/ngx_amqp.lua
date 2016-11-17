local _M = {
  _VERSION = "1.0.0"
}

local amqp  = require "amqp"
local cjson = require "cjson"

local CONFIG = ngx.shared.config

_M.connect_options = {
  host     = CONFIG:get("amqp.host"),
  port     = CONFIG:get("amqp.port"),
  ssl      = CONFIG:get("amqp.ssl"),
  user     = CONFIG:get("amqp.user"),
  password = CONFIG:get("amqp.password"),
  vhost    = CONFIG:get("amqp.vhost"),
  no_wait  = CONFIG:get("amqp.no_wait")
}

local publisher_pool_size = CONFIG:get("amqp.publisher_pool_size") or 1
local async_queue_size    = CONFIG:get("amqp.async_queue_size")    or 100
local publisher_timeout   = CONFIG:get("amqp.publisher_timeout")   or 5
local trace_messages      = CONFIG:get("amqp.trace_messages")      or false

local function key_fn(o)
  return o.user .. "@" .. o.host .. ":" .. o.port .. ":" .. o.vhost
end

local function default(v, def)
  if v ~= nil then
    return v
  end
  return def
end

local function amqp_connect(opts, exch)
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
  ctx:teardown()
  ctx:close()
  ngx.log(ngx.INFO, "AMQP disconnected, endpoint=" .. key_fn(ctx.opts.add))
end

local function amqp_exchange_declare(ctx, req)
  local ok, err, err2 = amqp.exchange_declare(ctx, {
    exchange    = req.exch.name,
    typ         = default(req.exch.typ, "topic"),
    passive     = default(req.exch.passive, false),
    durable     = default(req.exch.durable, true),
    auto_delete = default(req.exch.auto_delete, false),
    internal    = default(req.exch.internal, false),
    no_wait     = default(req.exch.no_wait, false)
  })

  if trace_messages then
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

  if trace_messages then
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

  if trace_messages then
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

  if trace_messages then
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

  if trace_messages then
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

  if trace_messages then
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

  if trace_messages then
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

  if trace_messages then
    ngx.log(ngx.INFO, "AMQP delete queue: endpoint=" .. key_fn(req.opts) .. ", opt=" .. cjson.encode(req.qdelete))
  end
  
  if not ok then
    if type(err) == "number" then err = err2 end
    ngx.log(ngx.ERR, "AMQP delete queue: " .. err)
  end

  return ok, err
end

local function amqp_publish_message(ctx, req)
  ctx.opts.routing_key = req.exch.routing_key
  ctx.opts.exchange    = req.exch.name

  local ok, err = ctx:publish(req.mesg)

  if trace_messages then
    ngx.log(ngx.INFO, "AMQP publish: endpoint=" .. key_fn(req.opts) .. ", exchange=" .. cjson.encode(req.exch) .. ", message=" .. req.mesg or "")
  end

  if not ok then
    ngx.log(ngx.ERR, "AMQP publish failed: " .. err)
  end

  ctx.opts.routing_key = nil
  ctx.opts.exchange    = nil

  return ok, err
end

local amqp_worker
amqp_worker = {
  async_queue_size = 100,
  publisher_timeout = 5,
  queue = {},
  pool = {},
  routine = function(premature, cache, num)
    if premature then
      return
    end

    ngx.log(ngx.INFO, "AMQP worker #" .. num .. " has been started")

    while true
    do
:: continue ::

      local req = table.remove(amqp_worker.queue)

      if not req then
        if ngx.worker.exiting() then
          break
        end

        ngx.sleep(0.1)

        goto continue
      end
 
      CONFIG:incr("amqp_worker.queue", -1)

      if req.forgot then
        goto continue
      end

      local key = key_fn(req.opts)
      local ctx = cache[key]
      local ok = true, err

      if not ctx then
        ok, ctx, err = amqp_connect(req.opts, req.exch)
        if ok then
          cache[key] = ctx
          CONFIG:incr(key, 1, 0)
        else
          ngx.log(ngx.ERR, "AMQP connect: " .. err)
        end
      end

      if ok and req.exch then
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
        CONFIG:incr(key, -1)
      end

      req.err = err
      req.ready = true
    end
 
    for key, ctx in pairs(cache)
    do
      amqp_disconnect(ctx)
      cache[key] = nil
      CONFIG:incr(key, -1)
    end

    ngx.log(ngx.INFO, "AMQP worker #" .. num .. " has been stopped")
  end
}

local function wait_queue()
  local totime = ngx.now() + publisher_timeout
  local queue_size = CONFIG:get("amqp_worker.queue") or 0
  local remain = publisher_timeout

  while queue_size >= async_queue_size and remain > 0
  do
    ngx.log(ngx.WARN, "AMQP throotled")
    ngx.sleep(0.01)
    queue_size = CONFIG:get("amqp_worker.queue")
    remain = totime - ngx.now()
  end

  if remain <= 0 then
    return 0, "AMQP throotled"
  end

  return remain, nil
end

local function wait(req, timeout)
  local totime = ngx.now() + timeout

  while not req.ready and ngx.now() < totime
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

  table.insert(amqp_worker.queue, req)

  CONFIG:incr("amqp_worker.queue", 1, 0)

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

  table.insert(amqp_worker.queue, req)

  CONFIG:incr("amqp_worker.queue", 1, 0)

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

  table.insert(amqp_worker.queue, req)

  CONFIG:incr("amqp_worker.queue", 1, 0)

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

  table.insert(amqp_worker.queue, req)

  CONFIG:incr("amqp_worker.queue", 1, 0)

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

  table.insert(amqp_worker.queue, req)

  CONFIG:incr("amqp_worker.queue", 1, 0)

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

  table.insert(amqp_worker.queue, req)

  CONFIG:incr("amqp_worker.queue", 1, 0)

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

  table.insert(amqp_worker.queue, req)

  CONFIG:incr("amqp_worker.queue", 1, 0)

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

  table.insert(amqp_worker.queue, req)

  CONFIG:incr("amqp_worker.queue", 1, 0)

  return wait(req, remain)
end

function _M.publish(exchange, message, options)
  local remain, err = wait_queue()

  if remain == 0 then
    return false, err
  end

  local req = {
    opts = options or _M.connect_options,
    exch = exchange,
    mesg = message,
  }

  table.insert(amqp_worker.queue, req)

  CONFIG:incr("amqp_worker.queue", 1, 0)

  return wait(req, remain)
end

function _M.startup()
  ngx.log(ngx.INFO, "AMQP workers pool size=" .. publisher_pool_size)
  for i=1,publisher_pool_size
  do
    local cache = {}
    local ok, err = ngx.timer.at(0, amqp_worker.routine, cache, i)
    if not ok then
      error("failed to start AMQP worker: " .. err)
    end
    table.insert(amqp_worker.pool, cache)
  end
end

function _M.info()
  local r = {}

  r.queue_size = CONFIG:get("amqp_worker.queue") or 0
  r.publishers = {}

  for _, cache in pairs(amqp_worker.pool)
  do
    for endpoint, _ in pairs(cache)
    do
      r.publishers[endpoint] = CONFIG:get(endpoint)
    end
  end

  return r
end

return _M