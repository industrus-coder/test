#pragma once

#ifndef DREDIS_CMD_H
#define DREDIS_CMD_H

#include <functional>
#include <unordered_map>

#include "parser.h"

using CMD_MAP =
std::unordered_map<
    commandType,
    std::function<str(const TOKENS &)>
>;

extern CMD_MAP cmd_map;

str execute_command(COMMAND cmd);

bool is_write_command(commandType type);

void init_cmd_map();

str handle_get(const TOKENS &args);

str handle_set(const TOKENS &args);

str handle_getset(const TOKENS &args);

str handle_setnx(const TOKENS &args);

str handle_setex(const TOKENS &args);

str handle_del(const TOKENS &args);

str handle_exists(const TOKENS &args);

str handle_keys(const TOKENS &args);

str handle_rename(const TOKENS &args);

str handle_append(const TOKENS &args);

str handle_strlen(const TOKENS &args);

str handle_incr(const TOKENS &args);

str handle_decr(const TOKENS &args);

str handle_incrby(const TOKENS &args);

str handle_decrby(const TOKENS &args);

str handle_mget(const TOKENS &args);

str handle_mset(const TOKENS &args);

str handle_hset(const TOKENS &args);

str handle_hmset(const TOKENS &args);

str handle_hget(const TOKENS &args);

str handle_hdel(const TOKENS &args);

str handle_hgetall(const TOKENS &args);

str handle_hexists(const TOKENS &args);

str handle_hlen(const TOKENS &args);

str handle_hkeys(const TOKENS &args);

str handle_hvals(const TOKENS &args);

str handle_hincrby(const TOKENS &args);

str handle_sadd(const TOKENS &args);

str handle_srem(const TOKENS &args);

str handle_smembers(const TOKENS &args);

str handle_scard(const TOKENS &args);

str handle_sismember(const TOKENS &args);

str handle_spop(const TOKENS &args);

str handle_zadd(const TOKENS &args);

str handle_zrem(const TOKENS &args);

str handle_zrange(const TOKENS &args);

str handle_zrevrange(const TOKENS &args);

str handle_zrank(const TOKENS &args);

str handle_zscore(const TOKENS &args);

str handle_zcard(const TOKENS &args);

str handle_zpopmin(const TOKENS &args);

str handle_zincrby(const TOKENS &args);

str handle_lpush(const TOKENS &args);

str handle_rpush(const TOKENS &args);

str handle_lpop(const TOKENS &args);

str handle_rpop(const TOKENS &args);

str handle_lrange(const TOKENS &args);

str handle_llen(const TOKENS &args);

str handle_lindex(const TOKENS &args);

str handle_linsert(const TOKENS &args);

str handle_ttl(const TOKENS &args);

str handle_pttl(const TOKENS &args);

str handle_expire(const TOKENS &args);

str handle_pexpire(const TOKENS &args);

str handle_expiretime(const TOKENS &args);

str handle_pexpiretime(const TOKENS &args);

str handle_persist(const TOKENS &args);

str handle_type(const TOKENS &args);

str handle_dbsize(const TOKENS &args);

str handle_flushdb(const TOKENS &args);

str handle_info(const TOKENS &args);

str handle_ping(const TOKENS &args);

str handle_cluster(const TOKENS &args);

str handle_config(const TOKENS &args);

str handle_unknown(const TOKENS &args);

str handle_xadd(const TOKENS &args);

str handle_lset(const TOKENS &args);

str handle_zrangebyscore(const TOKENS &args);

str handle_command(const TOKENS &args);

str handle_lrem(const TOKENS &args);

str handle_vclock(const TOKENS &args);

str handle_bgrewriteaof(const TOKENS &args);

str handle_quit(const TOKENS &args);

str handle_hello(const TOKENS &args);

str handle_auth(const TOKENS &args);

str handle_select(const TOKENS &args);

str handle_client(const TOKENS &args);

str handle_replconf(const TOKENS &args);

str handle_slaveof(const TOKENS &args);

#endif
