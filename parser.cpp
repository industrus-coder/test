#include <iostream>
#include <algorithm>
#include <unordered_map>
#include <numeric>
#include <charconv>
#include <cstring>
#include <ranges>
#include "parser.h"
#include "crc32c.h"

#include <sstream>

#include "cmd.h"

namespace {
    const std::unordered_map<std::string, commandType> name_to_type_table = {
        {"GET", commandType::GET},
        {"SET", commandType::SET},
        {"PING", commandType::PING},
        {"MGET", commandType::MGET},
        {"MSET", commandType::MSET},
        {"GETSET", commandType::GETSET},
        {"SETNX", commandType::SETNX},
        {"SETEX", commandType::SETEX},
        {"DEL", commandType::DEL},
        {"EXISTS", commandType::EXISTS},
        {"KEYS", commandType::KEYS},
        {"RENAME", commandType::RENAME},
        {"APPEND", commandType::APPEND},
        {"STRLEN", commandType::STRLEN},
        {"INCR", commandType::INCR},
        {"INCRBY", commandType::INCRBY},
        {"DECR", commandType::DECR},
        {"DECRBY", commandType::DECRBY},
        {"LPUSH", commandType::LPUSH},
        {"RPUSH", commandType::RPUSH},
        {"LPOP", commandType::LPOP},
        {"RPOP", commandType::RPOP},
        {"LRANGE", commandType::LRANGE},
        {"LLEN", commandType::LLEN},
        {"LINDEX", commandType::LINDEX},
        {"LINSERT", commandType::LINSERT},
        {"HSET", commandType::HSET},
        {"HMSET", commandType::HMSET},
        {"HGET", commandType::HGET},
        {"HDEL", commandType::HDEL},
        {"HGETALL", commandType::HGETALL},
        {"HEXISTS", commandType::HEXISTS},
        {"HLEN", commandType::HLEN},
        {"HKEYS", commandType::HKEYS},
        {"HVALS", commandType::HVALS},
        {"HINCRBY", commandType::HINCRBY},
        {"SADD", commandType::SADD},
        {"SREM", commandType::SREM},
        {"SMEMBERS", commandType::SMEMBERS},
        {"SCARD", commandType::SCARD},
        {"SISMEMBER", commandType::SISMEMBER},
        {"SPOP", commandType::SPOP},
        {"ZADD", commandType::ZADD},
        {"ZREM", commandType::ZREM},
        {"ZRANGE", commandType::ZRANGE},
        {"ZREVRANGE", commandType::ZREVRANGE},
        {"ZRANK", commandType::ZRANK},
        {"ZSCORE", commandType::ZSCORE},
        {"ZCARD", commandType::ZCARD},
        {"ZPOPMIN", commandType::ZPOPMIN},
        {"ZINCRBY", commandType::ZINCRBY},
        {"TTL", commandType::TTL},
        {"PTTL", commandType::PTTL},
        {"EXPIRE", commandType::EXPIRE},
        {"PEXPIRE", commandType::PEXPIRE},
        {"EXPIRETIME", commandType::EXPIRETIME},
        {"PEXPIRETIME", commandType::PEXPIRETIME},
        {"PERSIST", commandType::PERSIST},
        {"TYPE", commandType::TYPE},
        {"DBSIZE", commandType::DBSIZE},
        {"FLUSHDB", commandType::FLUSHDB},
        {"INFO", commandType::INFO},
        {"CLUSTER", commandType::CLUSTER},
        {"CONFIG", commandType::CONFIG},
        {"REPLCONF", commandType::REPLCONF},
        {"SLAVEOF", commandType::SLAVEOF},
        {"EVAL", commandType::EVAL},
        {"EVALSHA", commandType::EVALSHA},
        {"SCRIPT", commandType::SCRIPT},
        {"COMMAND", commandType::COMMAND},
        {"LREM", commandType::LREM},
        {"ZRANGEBYSCORE", commandType::ZRANGEBYSCORE},
        {"XADD", commandType::XADD},
        {"LSET", commandType::LSET},
        {"VCLOCK", commandType::VCLOCK},
        {"BGREWRITEAOF", commandType::BGREWRITEAOF}
    };

    const std::unordered_map<commandType, std::string> type_to_name_table = [] {
        std::unordered_map<commandType, std::string> map;
        for (const auto& pair : name_to_type_table) {
            map[pair.second] = pair.first;
        }
        return map;
    }();
}

commandType resolve_type(std::string cmd_name) {
    std::ranges::transform(cmd_name, cmd_name.begin(), ::toupper);
    auto it = name_to_type_table.find(cmd_name);
    return (it != name_to_type_table.end()) ? it->second : commandType::UNKNOWN;
}

// Now uses the centralized table, preventing sync issues.
std::string command_to_string(commandType type) {
    auto it = type_to_name_table.find(type);
    return (it != type_to_name_table.end()) ? it->second : "UNKNOWN";
}



namespace RESP {
    str simple_string(strv msg) {
        return "+" + std::string(msg) + "\r\n";
    }

    str integer(long long num) {
        return ":" + std::to_string(num) + "\r\n";
    }

    str error(strv msg) {
        return "-ERR " + std::string(msg) + "\r\n";
    }

    str error(strv prefix, strv msg) {
        return "-" + std::string(prefix) + " " + std::string(msg) + "\r\n";
    }

    str error_wrongtype() {
        return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
    }

    str error_moved(uint16_t slot, strv host, uint16_t port) {
        return "-MOVED " + std::to_string(slot) + " " +
               std::string(host) + ":" + std::to_string(port) + "\r\n";
    }

    str error_clusterdown(strv msg) {
        return "-CLUSTERDOWN " + std::string(msg) + "\r\n";
    }

    str error_tryagain() {
        return "-TRYAGAIN Multiple keys belong to different nodes, try again\r\n";
    }

    str error_oom() {
        return "-OOM command not allowed when used memory > 'maxmemory'\r\n";
    }

    str bulk_string(strv msg) {
        return "$" + std::to_string(msg.size()) + "\r\n" +
               std::string(msg) + "\r\n";
    }

    str null_bulk_string() {
        return "$-1\r\n";
    }

    str ok() {
        return "+OK\r\n";
    }

    str pong() {
        return "+PONG\r\n";
    }

    str array_raw(const TOKENS &args) {
        str res = "*" + std::to_string(args.size()) + "\r\n";
        for (const auto &arg: args)
            res += arg;
        return res;
    }

    str array(const TOKENS &args) {
        str res = "*" + std::to_string(args.size()) + "\r\n";
        for (const auto &arg: args)
            res += bulk_string(arg);
        return res;
    }

    str null_array() {
        return "*-1\r\n";
    }

    str serialize_command(const COMMAND& cmd) {
        TOKENS tokens;
        tokens.push_back(command_to_string(cmd.type));
        tokens.insert(tokens.end(), cmd.args.begin(), cmd.args.end());
        return array(tokens);
    }

    void Parser::feed(strv data) {
        buffer.append(data);
    }

    strv Parser::read_until_crlf() {
        size_t next = buffer.find("\r\n", cursor);
        if (next == str::npos) return {};
        strv res(buffer.data() + cursor, next - cursor);
        cursor = next + 2;
        return res;
    }

    strv Parser::read_bytes(size_t len) {
        if (cursor + len + 2 > buffer.size()) return {};
        // Check for correct CRLF termination *before* creating the string_view
        if (buffer[cursor + len] != '\r' || buffer[cursor + len + 1] != '\n') return {};
        strv res(buffer.data() + cursor, len);
        cursor += len + 2;
        return res;
    }

    bool Parser::next(COMMAND &cmd) {
        cmd = {commandType::UNKNOWN, {}};
        size_t initial_cursor = cursor;

        if (cursor >= buffer.size()) return false;

        TOKENS extracted_strings;

        if (buffer[cursor] == '*') { // Array parsing
            cursor++;

            auto count_sv = read_until_crlf();
            if (count_sv.empty()) {
                cursor = initial_cursor; // Incomplete data, rewind
                return false;
            }

            long long count;
            auto [ptr, ec] = std::from_chars(count_sv.data(), count_sv.data() + count_sv.size(), count);
            if (ec != std::errc()) { // Handles invalid characters
                cursor = initial_cursor; // Malformed, but rewind to allow recovery/logging
                return false;
            }
            if (count < -1) {
                cursor = initial_cursor;
                return false;
            }

            if (count == -1) { // Null array
                return true; // Successfully parsed a "null" command
            }

            extracted_strings.reserve(count);
            for (int i = 0; i < count; i++) {
                if (cursor >= buffer.size() || buffer[cursor] != '$') {
                    cursor = initial_cursor; return false;
                }
                cursor++;

                auto len_sv = read_until_crlf();
                if (len_sv.empty()) {
                    cursor = initial_cursor; return false;
                }

                long long len;
                auto [len_ptr, len_ec] = std::from_chars(len_sv.data(), len_sv.data() + len_sv.size(), len);
                if (len_ec != std::errc()) {
                    cursor = initial_cursor; return false;
                }
                if (len < -1) {
                    cursor = initial_cursor; return false;
                }

                if (len == -1) {
                    extracted_strings.emplace_back(); // Null bulk string
                    continue;
                }

                auto arg = read_bytes(len);
                if (arg.data() == nullptr) { // read_bytes returns empty view on failure
                    cursor = initial_cursor; return false;
                }
                extracted_strings.emplace_back(arg);
            }
        } else { // Inline command parsing
            auto line_sv = read_until_crlf();
            if (line_sv.empty()) {
                cursor = initial_cursor; return false;
            }

            // Simple split on space
            size_t start = 0;
            for (size_t i = 0; i < line_sv.size(); ++i) {
                if (isspace(line_sv[i])) {
                    if (i > start) extracted_strings.push_back(std::string(line_sv.substr(start, i - start)));
                    start = i + 1;
                }
            }
            if (start < line_sv.size()) extracted_strings.push_back(std::string(line_sv.substr(start)));
        }

        if (extracted_strings.empty()) {
            // This can happen with valid input like "*-1\r\n" or "*0\r\n"
            return true;
        }

        cmd.type = resolve_type(extracted_strings[0]);
        cmd.args.assign(extracted_strings.begin() + 1, extracted_strings.end());

        return true;
    }

    void Parser::clear_consumed() {
        if (cursor == 0) return;
        buffer.erase(0, cursor);
        cursor = 0;
    }
}

namespace BIN {
    uint32_t calculate_checksum(const FrameHeader& header, const std::string& payload) {
        uint32_t c = crc32c(&header, offsetof(FrameHeader, checksum));
        if (!payload.empty())
            c = crc32c(payload.data(), payload.size(), c);
        return c;
    }

    std::string serialize(const Frame& frame) {
        Frame mutable_frame = frame;
        mutable_frame.header.checksum = calculate_checksum(mutable_frame.header, mutable_frame.payload);
        std::string serialized_data;
        serialized_data.append(reinterpret_cast<const char*>(&mutable_frame.header), sizeof(FrameHeader));
        serialized_data.append(mutable_frame.payload);
        return serialized_data;
    }

    bool parse(str& buffer, Frame& out) {
        if (buffer.size() < sizeof(FrameHeader)) return false;

        std::memcpy(&out.header, buffer.data(), sizeof(FrameHeader));

        if (out.header.magic != BIN::MAGIC) return false;
        if (out.header.version > 1) return false;
        // Basic sanity check for payload length
        if (out.header.payload_len > 10 * 1024 * 1024) return false; // 10MB limit
        if (buffer.size() < sizeof(FrameHeader) + out.header.payload_len) return false;

        out.payload = buffer.substr(sizeof(FrameHeader), out.header.payload_len);

        // Now the checksum validation will work correctly.
        if (calculate_checksum(out.header, out.payload) != out.header.checksum) return false;

        buffer.erase(0, sizeof(FrameHeader) + out.header.payload_len);
        return true;
    }
}
