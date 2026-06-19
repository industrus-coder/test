#pragma once

#ifndef DREDIS_PARSER_H
#define DREDIS_PARSER_H

#include <functional>
#include <string>
#include <string_view>
#include <vector>
#include <unordered_map>
#include <cstdint>

using str = std::string;
using strv = std::string_view;
using TOKENS = std::vector<str>;

enum class commandType {
    GET,
    SET,
    MGET,
    MSET,
    GETSET,
    SETNX,
    SETEX,
    DEL,
    EXISTS,
    KEYS,
    RENAME,
    APPEND,
    STRLEN,
    INCR,
    INCRBY,
    DECR,
    DECRBY,
    LPUSH,
    RPUSH,
    LPOP,
    RPOP,
    LRANGE,
    LLEN,
    LINDEX,
    LINSERT,
    HSET,
    HMSET,
    HGET,
    HDEL,
    HGETALL,
    HEXISTS,
    HLEN,
    HKEYS,
    HVALS,
    HINCRBY,
    SADD,
    SREM,
    SMEMBERS,
    SCARD,
    SISMEMBER,
    SPOP,
    ZADD,
    ZREM,
    ZRANGE,
    ZREVRANGE,
    ZRANK,
    ZSCORE,
    ZCARD,
    ZPOPMIN,
    ZINCRBY,
    TTL,
    PTTL,
    EXPIRE,
    PEXPIRE,
    EXPIRETIME,
    PEXPIRETIME,
    PERSIST,
    TYPE,
    DBSIZE,
    FLUSHDB,
    INFO,
    PING,
    CLUSTER,
    CONFIG,
    REPLCONF,
    SLAVEOF,
    EVAL,
    EVALSHA,
    SCRIPT,
    COMMAND,
    LREM,
    ZRANGEBYSCORE,
    XADD,
    LSET,
    VCLOCK,
    BGREWRITEAOF,
    QUIT,
    HELLO,
    AUTH,
    SELECT,
    CLIENT,
    UNKNOWN
};

struct COMMAND {
    commandType type;
    TOKENS args;

    bool from_replication = false;
};


// bool is_write_command(commandType);
// bool is_read_command(commandType);
// bool is_multi_key_command(commandType);
// bool requires_replication(commandType);


commandType resolve_type(std::string cmd_name);
std::string command_to_string(commandType type);

COMMAND parse_(const std::string& input_command);
namespace RESP {

    // RESP Utility Helpers
    str simple_string(strv msg);
    str integer(long long num);
    str bulk_string(strv msg);
    str null_bulk_string();
    str array_raw(const TOKENS& args);
    str array(const TOKENS& args);
    str null_array();
    str error(strv msg);
    str error(strv prefix, strv msg);
    str error_wrongtype();
    str error_moved(uint16_t slot, strv host, uint16_t port);
    str error_clusterdown(strv msg = "The cluster is down");
    str error_tryagain();
    str error_oom();
    str ok();
    str pong();

    // Replication Support
    str serialize_command(const COMMAND& cmd);

    struct Parser {
        str buffer;
        size_t cursor = 0;

        void feed(strv data);
        bool next(COMMAND& cmd);
        void clear_consumed();

    private:
        strv read_until_crlf();
        strv read_bytes(size_t len);
    };

}

namespace BIN {

    const uint32_t MAGIC = 0xDDEE1234;
    const uint8_t VERSION = 1;

    enum class FrameType : uint8_t {
        GOSSIP_PING = 0x01,
        GOSSIP_PONG = 0x02,
        GOSSIP_STATE = 0x03,
        REPLICATE_PUT = 0x04,
        REPLICATE_DEL = 0x05,
        REPLICATE_ACK = 0x06,
        READ_REQUEST = 0x07,
        READ_RESPONSE = 0x08,
        ANTIENTROPY_HASH = 0x09,
        ANTIENTROPY_SYNC = 0x0A,
        CLUSTER_JOIN = 0x0B,
        CLUSTER_JOIN_ACK = 0x0C,
        PROXY_REQUEST = 0x0D,
        PROXY_RESPONSE = 0x0E,
        PING = 0x0F,
        PONG = 0x10,
        // Internal types (beyond spec's 16)
        FULL_SYNC_REQUEST = 0x11,
        FULL_SYNC_CHUNK = 0x12
    };

    #pragma pack(push,1)
    struct FrameHeader {
        uint32_t magic;
        uint8_t version;
        uint8_t msg_type;
        uint64_t msg_id;
        uint64_t sender_id;
        uint32_t payload_len;
        uint32_t checksum;
    };
    static_assert(sizeof(FrameHeader) == 30, "FrameHeader must be exactly 30 bytes");
    #pragma pack(pop)

    struct Frame {
        FrameHeader header;
        std::string payload;
    };

    std::string serialize(const Frame&);
    bool parse(str& buffer, Frame& out);

    uint32_t calculate_checksum(const FrameHeader& header, const std::string& payload);

}

#endif