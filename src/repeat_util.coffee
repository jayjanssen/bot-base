#!/usr/bin/env coffee

util = require 'util'
utils = require './utils'
Promise = require 'bluebird'
child_process = require 'child_process'
Redlock = require 'redlock'

process.env.REPEAT_LOG_LEVEL ?= 'info'
logger = utils.logger '', process.env.REPEAT_LOG_LEVEL

logger.debug util.inspect process.env

argv = require('minimist')(process.argv.slice(2), {
  boolean: ['lp', 'zd']
  stopEarly: true
  strings: ['interval','timeout']
  alias: {
    'interval': 'i'
    'timeout': 't'
  }
  default: {
    'timeout': 3600
  }
})

logger.debug util.inspect argv

unless argv.i? and argv._? and argv._.length > 0
  console.error "Usage: repeat_util [-i <repeat interval>] [-t <command timeout>] command [arg1 [arg2...]]"
  process.exit 1

interval = argv.i
timeout = argv.t if argv.t?

full_command = argv._
short_command = full_command[0]
args = argv._[1..]

process.on 'SIGTERM', () ->
  console.log "Shutting down [SIGTERM]"
  process.exit 0

key = full_command.join '_'
ttlkey = key + "_last_run"
logger.debug key



get_lock_disposers = (rclient) ->
  redlock = new Redlock [rclient], {
    retryCount: timeout
    retryDelay: 1000
  }

  keys = [key]
  keys.push 'liquidplanner_api' if argv.lp
  keys.push 'zendesk_api' if argv.zd

  logger.debug "Acquiring locks on #{keys.join ', '}"
  keys.map (key) ->
    redlock.disposer( key, 10000 )  # 10s, we renew every 5s

sequential_errors = 0

cycle = () -> 
  wait = interval
  Promise.using utils.redis_disposer('repeat_'), (rclient) ->
    rclient.ttlAsync(ttlkey).then (time_left) ->
      throw new TTLNotExpiredError time_left if time_left > 5
      logger.debug "Trying locks"

      delay_int = setInterval (-> logger.warning "Locking delayed, still trying"), 30000
      Promise.using get_lock_disposers(rclient), (locks) ->
        logger.debug "Got locks"
        logger.debug util.inspect locks
        clearInterval delay_int

        extend_locks = () ->
          Promise.map locks, (lock) ->
            lock.extend 10000
          .then () ->
            logger.debug "Locks extended"
          .catch (err) ->
            logger.debug "Extending locks error: #{err}"
        renew_int = setInterval extend_locks, 5000

        run_sub_process()
        .finally () ->
          clearInterval renew_int
      .then () ->
        rclient.multi().set(ttlkey, true).expire(ttlkey, interval).execAsync()
      .then () ->
        sequential_errors = 0
        logger.notice '.'
      .catch Redlock.LockError, (err) ->
        logger.warning "Could not get lock: #{err.message}" 
        wait = 1
      .catch (err) ->
        logger.error "subprocess error: #{err.message}"
        sequential_errors += 1
        if sequential_errors > 5
          logger.error "Subprocess had #{sequential_errors} in a row, aborting container"
          process.exit 11
        wait = 10
      .finally () ->
        clearInterval delay_int
  .catch TTLNotExpiredError, (err) ->
    logger.notice "#{short_command} run recently, running again in #{err.message}"
    wait = err.message
  .then () ->
    wait_cycle wait
  .catch (err) ->
    logger.error "Unexpected error: #{err.message}, aborting"
    process.exit 10

wait_cycle = (wait) ->
  logger.debug "Waiting for #{wait} seconds for next cycle"
  setTimeout cycle, wait * 1000

run_sub_process = () -> new Promise (resolve, reject) ->
  child = child_process.spawn short_command, args, {
      env: process.env
      stdio: 'inherit'
    }

  logger.debug "Child process spawned"

  child.on 'error', (err) ->
    reject err
  child.on 'close', (code, signal) ->
    if code == 0
      resolve true 
    else if signal?
      reject new Error "terminated with #{signal}"
    else
      reject new Error "exited with #{code}"

  setTimeout (-> child.kill()), timeout * 1000


logger.notice "Repeating #{short_command} every #{interval} seconds"
cycle()