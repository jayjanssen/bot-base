Promise = require 'bluebird'
redis = Promise.promisifyAll require 'redis'
util = require 'util'
winston = require 'winston'
moment = require 'moment'

# Custom error types
class TooManyResultsError extends Error
  constructor: (@message) ->
    @name = "TooManyResultsError"
    Error.captureStackTrace this, TooManyResultsError
global.TooManyResultsError = TooManyResultsError

class NoResultsError extends Error
  constructor: (@message) ->
    @name = "NoResultsError"
    Error.captureStackTrace this, NoResultsError
global.NoResultsError = NoResultsError

class TTLNotExpiredError extends Error
  constructor: (@message) ->
    @name = "TTLNotExpiredError"
    Error.captureStackTrace this, TTLNotExpiredError
global.TTLNotExpiredError = TTLNotExpiredError

class NotCachedError extends Error
  constructor: (@message) ->
    @name = "NotCachedError"
    Error.captureStackTrace this, NotCachedError
global.NotCachedError = NotCachedError

exports.vault = require("node-vault") {
  apiVersion: 'v1'
  endpoint: process.env.VAULT_ADDR
  token: process.env.VAULT_TOKEN
}
exports.vault.get = (path) ->
  exports.vault.read "#{process.env.VAULT_PATH}#{path}"


redis_connection = (prefix) ->
  new Promise (resolve, reject) ->
    rclient = redis.createClient {
      'url': process.env.REDIS_URL
      'prefix': prefix
      'enable_offline_queue': false
      'retry_strategy': (options) ->
        return new Error("Retry time exhausted") if options.total_retry_time > 1000 * 60 * 60
        return Math.max options.attempt * 100, 3000
    }
    rclient.on 'error', reject
    rclient.on 'ready', () ->
      resolve rclient
exports.redis_connection = redis_connection

exports.redis_disposer = (prefix) ->
  redis_connection(prefix).disposer (rclient) ->
    rclient.quit()

exports.logger = (label, level=process.env.LOG_LEVEL) ->
  logger = new winston.Logger {
    levels: winston.config.syslog.levels
    exitOnError: true
    transports: [ new (winston.transports.Console)({
      'level': level
      'label': label
      'colorize': true
    }) ]
  }

  logger

# obj_name: 'string'
# lib_name: 'string'
# rclient: redis client
# logger: logging obj
# obj_ttl: duration
# obj_refresh: duration
# get_all_func: () -> Promise (all_objs)
# format_one_func: (obj) -> Promise (formatted_obj)
# vals_to_index_func: (obj) -> Promise (array of strings that map to obj)

exports.setup_obj_store_and_caching = (obj_name, lib_name, rclient, logger, obj_ttl, obj_refresh, get_all_func, format_one_func, vals_to_index_func, get_one_func) ->

  cache_name = "#{obj_name}_cache"
  index_name = "#{cache_name}_index"
  tmp_index_name = "#{index_name}_tmp"

  cache_objs = () ->
    counter = 0
    clear_old_cache()
    .then () ->
      get_all_func()
    .map (obj) ->
      cache_obj obj
      .then () ->
        counter++
    .then () ->
      logger.info "Cached #{counter} #{obj_name} objects"

  clear_old_cache = () ->
    rclient.hvalsAsync index_name
    .map (old_index) ->
      rclient.del old_index
    .then () ->
      rclient.del index_name

  cache_obj = (obj) ->
    rtrx = rclient.multi()
    id = "#{obj_name}:#{obj.id}"
    format_one_func(obj)
    .then (formatted_obj) ->
      rtrx = rtrx.set id, JSON.stringify formatted_obj
      .expire id, obj_ttl.asSeconds()
    .then () ->
      vals_to_index_func(obj)
      .then (vals) -> vals.concat [obj.id]
    .map (val) -> if typeof val is 'string' then val.toLowerCase() else val
    .then (index_vals) ->
      for index_val in index_vals
        rtrx = rtrx.hset index_name, index_val, "#{index_name}:#{index_val}"
        rtrx = rtrx.sadd "#{index_name}:#{index_val}", id
      rtrx.execAsync()

  get_object = (id) ->
    get_key "#{obj_name}:#{id}"
    .then (obj) ->
      if !obj? and get_one_func?
        logger.debug "Could not find #{obj_name}:#{id}, trying to fetch it independently "
        get_one_func id
        .then (obj) ->
          if obj?
            cache_obj obj
            .then () ->
              obj
        .catch (err) ->
          null
      else
        obj

  get_key = (key) ->
    rclient.getAsync key
    .then (json_str) ->
      JSON.parse json_str

  search_object = (string) ->
    throw new InvalidInputError 'no search string' unless string?
    rclient.hscanAsync index_name, 0, 'MATCH', "#{string.toLowerCase()}*", "COUNT", 100000
    .then (results) ->
      logger.warning "Unfullilled SCAN cursor" if results[0] != '0'
      set_ids = {}
      set_ids[id] = true for id, i in results[1] when i % 2 != 0
      throw new NoResultsError 'no results' unless Object.keys(set_ids).length > 0
      rclient.sunionAsync Object.keys( set_ids )
    .map get_key
    .catch NoResultsError, () ->
      []

  find_object = (string) ->
    search_object string
    .then (results) ->
      throw new TooManyResultsError "#{results.length}" if results.length > 1
      return results[0]

  global[lib_name]["get_#{obj_name}"] = get_object
  global[lib_name]["search_#{obj_name}"] = search_object
  global[lib_name]["find_#{obj_name}"] = find_object
  global[lib_name]["cache_#{obj_name}"] = cache_obj

  # Setup cache refresher

  refresh_cache = () ->
    every = obj_refresh.asSeconds()
    rclient.ttlAsync cache_name
    .then (value) ->
      if value > 0
        every = value
        throw new TTLNotExpiredError 'no need to refresh'
    .then () ->
      logger.profile cache_name
      cache_objs()
      .then () ->
        rclient.multi()
        .set(cache_name, 1)
        .expire(cache_name, every)
        .execAsync()
      .finally () ->
        logger.profile cache_name
    .catch TTLNotExpiredError, (err) ->
      logger.debug "#{cache_name} run since last ttl"
    .finally () ->
      logger.info "Running #{cache_name} again in #{every}s"
      setTimeout refresh_cache, every * 1000

  refresh_cache()


exports.setup_obj_store = (obj_name, lib_name, rclient, logger, obj_ttl, get_one_func) ->
  cache_name = "#{obj_name}_cache"

  get_object = (id) ->
    key = "#{obj_name}:#{id}"
    get_cache(key)
    .then (json_str) ->
      throw new NotCachedError "#{key} was not cached" unless json_str?
      logger.debug "#{key} was cached"
      JSON.parse json_str
    .catch NotCachedError, (err) ->
      logger.debug "#{key} was not cached"
      get_one_func(id).then (obj) ->
        logger.debug "#{key}: " + util.inspect obj
        cache_obj(id, obj)
        .then () ->
          obj
      .catch NoResultsError, (err) ->
        logger.info "Got #{err} fetching #{key}"
        null

  get_cache = (key) ->
    rclient.getAsync key

  cache_obj = (id, obj) ->
    rclient.multi()
    .set "#{obj_name}:#{id}", JSON.stringify obj
    .expire "#{obj_name}:#{id}", obj_ttl.asSeconds()
    .execAsync()

  global[lib_name]["get_#{obj_name}"] = get_object
