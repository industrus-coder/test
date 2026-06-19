#include <variant>
#include <charconv>
#include <chrono>
#include <sstream>
#include <sys/epoll.h>
#include <unistd.h>

#include "parser.h"
#include "cmd.h"
#include "store.h"
#include "cluster.h"
#include "network.h"

CMD_MAP cmd_map;

static int64_t now_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()
    ).count();
}

static bool parse_int(const str &s, int64_t &val) {
    auto [p, ec] = std::from_chars(s.data(), s.data() + s.size(), val);
    return ec == std::errc();
}

// ── String handlers ──

str handle_get(const TOKENS &args) {
    if (args.empty()) return RESP::error("wrong number of arguments for 'get' command");
    ValueEntry *entry = store_get(args[0]);
    if (!entry || entry->type != Type::STRING)
        return RESP::null_bulk_string();
    return RESP::bulk_string(std::get<str>(entry->value));
}

str handle_set(const TOKENS &args) {
    if (args.size() < 2) return RESP::error("wrong number of arguments for 'set' command");
    bool nx = false, xx = false;
    int64_t expiry = -1;
    size_t idx = 2;
    while (idx < args.size()) {
        if (args[idx] == "NX") { nx = true; idx++; }
        else if (args[idx] == "XX") { xx = true; idx++; }
        else if (args[idx] == "KEEPTTL") { idx++; }
        else if (args[idx] == "EX" && idx + 1 < args.size()) {
            int64_t sec;
            if (parse_int(args[idx + 1], sec)) expiry = now_ms() + sec * 1000;
            idx += 2;
        }
        else if (args[idx] == "PX" && idx + 1 < args.size()) {
            int64_t ms;
            if (parse_int(args[idx + 1], ms)) expiry = now_ms() + ms;
            idx += 2;
        }
        else if (args[idx] == "EXAT" && idx + 1 < args.size()) {
            int64_t ts;
            if (parse_int(args[idx + 1], ts)) expiry = ts * 1000;
            idx += 2;
        }
        else if (args[idx] == "PXAT" && idx + 1 < args.size()) {
            int64_t ts;
            if (parse_int(args[idx + 1], ts)) expiry = ts;
            idx += 2;
        }
        else break;
    }
    if (nx || xx) {
        bool exists = store_exists(args[0]);
        if (nx && exists) return RESP::null_bulk_string();
        if (xx && !exists) return RESP::null_bulk_string();
    }
    ValueEntry entry;
    entry.type = Type::STRING;
    entry.value = args[1];
    entry.expiry_ms = expiry;
    store_set(args[0], std::move(entry));
    return RESP::ok();
}

str handle_getset(const TOKENS &args) {
    if (args.size() < 2) return RESP::error("wrong number of arguments for 'getset' command");
    ValueEntry *entry = store_get(args[0]);
    str old = RESP::null_bulk_string();
    if (entry && entry->type == Type::STRING)
        old = RESP::bulk_string(std::get<str>(entry->value));
    ValueEntry ne;
    ne.type = Type::STRING;
    ne.value = args[1];
    store_set(args[0], std::move(ne));
    return old;
}

str handle_setnx(const TOKENS &args) {
    if (args.size() < 2) return RESP::error("wrong number of arguments for 'setnx' command");
    if (store_exists(args[0])) return RESP::integer(0);
    ValueEntry entry;
    entry.type = Type::STRING;
    entry.value = args[1];
    store_set(args[0], std::move(entry));
    return RESP::integer(1);
}

str handle_setex(const TOKENS &args) {
    if (args.size() < 3) return RESP::error("wrong number of arguments for 'setex' command");
    int64_t sec;
    if (!parse_int(args[1], sec)) return RESP::error("value is not an integer or out of range");
    ValueEntry entry;
    entry.type = Type::STRING;
    entry.value = args[2];
    entry.expiry_ms = now_ms() + sec * 1000;
    store_set(args[0], std::move(entry));
    return RESP::ok();
}

str handle_keys(const TOKENS &args) {
    str pattern = args.empty() ? "*" : args[0];
    bool match_all = (pattern == "*");
    TOKENS res;
    for (const auto& key : store_keys()) {
        if (match_all || key == pattern)
            res.push_back(RESP::bulk_string(key));
    }
    return RESP::array_raw(res);
}

str handle_rename(const TOKENS &args) {
    if (args.size() < 2) return RESP::error("wrong number of arguments for 'rename' command");
    if (args[0] == args[1]) return RESP::ok();
    ValueEntry *entry = store_get(args[0]);
    if (!entry) return RESP::error("no such key");
    ValueEntry ne;
    ne.type = entry->type;
    ne.value = entry->value;
    ne.expiry_ms = entry->expiry_ms;
    ne.VecClk = entry->VecClk;
    store_del(args[0]);
    store_set(args[1], std::move(ne));
    return RESP::ok();
}

static int del_single(const str &key) {
    ValueEntry *entry = store_get(key);
    if (!entry) return 0;
    store_del(key);
    return 1;
}

str handle_del(const TOKENS &args) {
    if (args.empty()) return RESP::error("wrong number of arguments for 'del' command");
    int count = 0;
    for (const auto &key: args)
        count += del_single(key);
    return RESP::integer(count);
}

static int exists_single(const str &key) {
    return store_exists(key) ? 1 : 0;
}

str handle_exists(const TOKENS &args) {
    if (args.empty()) return RESP::error("wrong number of arguments for 'exists' command");
    int count = 0;
    for (const auto &key: args)
        count += exists_single(key);
    return RESP::integer(count);
}

str handle_append(const TOKENS &args) {
    if (args.size() < 2) return RESP::error("wrong number of arguments for 'append' command");
    ValueEntry *entry = store_get(args[0]);
    if (!entry || entry->type != Type::STRING) {
        ValueEntry ne;
        ne.type = Type::STRING;
        ne.value = args[1];
        store_set(args[0], std::move(ne));
        return RESP::integer(static_cast<long long>(args[1].size()));
    }
    auto &val = std::get<str>(entry->value);
    entry->expiry_ms = -1;
    if (!g_replication_mode) entry->VecClk[self_node.id]++;
    val += args[1];
    return RESP::integer(static_cast<long long>(val.size()));
}

str handle_strlen(const TOKENS &args) {
    if (args.empty()) return RESP::error("wrong number of arguments for 'strlen' command");
    ValueEntry *entry = store_get(args[0]);
    if (!entry || entry->type != Type::STRING)
        return RESP::integer(0);
    return RESP::integer(static_cast<long long>(std::get<str>(entry->value).size()));
}

str handle_incr(const TOKENS &args) {
    if (args.empty()) return RESP::error("wrong number of arguments for 'incr' command");
    ValueEntry *entry = store_get(args[0]);
    if (!entry || entry->type != Type::STRING) {
        ValueEntry ne;
        ne.type = Type::STRING;
        ne.value = str("1");
        store_set(args[0], std::move(ne));
        return RESP::integer(1);
    }
    int64_t val;
    if (!parse_int(std::get<str>(entry->value), val))
        return RESP::error("value is not an integer or out of range");
    val++;
    std::get<str>(entry->value) = std::to_string(val);
    entry->expiry_ms = -1;
    if (!g_replication_mode) entry->VecClk[self_node.id]++;
    return RESP::integer(val);
}

str handle_decr(const TOKENS &args) {
    if (args.empty()) return RESP::error("wrong number of arguments for 'decr' command");
    ValueEntry *entry = store_get(args[0]);
    if (!entry || entry->type != Type::STRING) {
        ValueEntry ne;
        ne.type = Type::STRING;
        ne.value = str("-1");
        store_set(args[0], std::move(ne));
        return RESP::integer(-1);
    }
    int64_t val;
    if (!parse_int(std::get<str>(entry->value), val))
        return RESP::error("value is not an integer or out of range");
    val--;
    std::get<str>(entry->value) = std::to_string(val);
    entry->expiry_ms = -1;
    if (!g_replication_mode) entry->VecClk[self_node.id]++;
    return RESP::integer(val);
}

str handle_incrby(const TOKENS &args) {
    if (args.size() < 2) return RESP::error("wrong number of arguments for 'incrby' command");
    int64_t delta;
    if (!parse_int(args[1], delta))
        return RESP::error("value is not an integer or out of range");
    ValueEntry *entry = store_get(args[0]);
    if (!entry || entry->type != Type::STRING) {
        ValueEntry ne;
        ne.type = Type::STRING;
        ne.value = std::to_string(delta);
        store_set(args[0], std::move(ne));
        return RESP::integer(delta);
    }
    int64_t val;
    if (!parse_int(std::get<str>(entry->value), val))
        return RESP::error("value is not an integer or out of range");
    val += delta;
    std::get<str>(entry->value) = std::to_string(val);
    entry->expiry_ms = -1;
    if (!g_replication_mode) entry->VecClk[self_node.id]++;
    return RESP::integer(val);
}

str handle_decrby(const TOKENS &args) {
    if (args.size() < 2) return RESP::error("wrong number of arguments for 'decrby' command");
    int64_t delta;
    if (!parse_int(args[1], delta))
        return RESP::error("value is not an integer or out of range");
    ValueEntry *entry = store_get(args[0]);
    if (!entry || entry->type != Type::STRING) {
        ValueEntry ne;
        ne.type = Type::STRING;
        ne.value = std::to_string(-delta);
        store_set(args[0], std::move(ne));
        return RESP::integer(-delta);
    }
    int64_t val;
    if (!parse_int(std::get<str>(entry->value), val))
        return RESP::error("value is not an integer or out of range");
    val -= delta;
    std::get<str>(entry->value) = std::to_string(val);
    entry->expiry_ms = -1;
    if (!g_replication_mode) entry->VecClk[self_node.id]++;
    return RESP::integer(val);
}

str handle_mget(const TOKENS &args) {
    if (args.empty()) return RESP::error("wrong number of arguments for 'mget' command");
    TOKENS res;
    res.reserve(args.size());
    for (const auto &key: args) {
        ValueEntry *entry = store_get(key);
        if (!entry || entry->type != Type::STRING)
            res.push_back(RESP::null_bulk_string());
        else
            res.push_back(RESP::bulk_string(std::get<str>(entry->value)));
    }
    return RESP::array_raw(res);
}

str handle_mset(const TOKENS &args) {
    if (args.size() < 2 || args.size() % 2 != 0)
        return RESP::error("wrong number of arguments for 'mset' command");
    for (size_t i = 0; i < args.size(); i += 2) {
        ValueEntry entry;
        entry.type = Type::STRING;
        entry.value = args[i + 1];
        store_set(args[i], std::move(entry));
    }
    return RESP::ok();
}

// ── Hash handlers ──

str handle_hset(const TOKENS &args) {
    if (args.size() < 3) return RESP::error("wrong number of arguments for 'hset' command");
    ValueEntry *entry = store_get(args[0]);
    if (!entry || entry->type != Type::HASH) {
        ValueEntry ne;
        ne.type = Type::HASH;
        auto &map = ne.value.emplace<std::unordered_map<str, str> >();
        int added = 0;
        for (size_t i = 1; i + 1 < args.size(); i += 2) {
            map[args[i]] = args[i + 1];
            added++;
        }
        store_set(args[0], std::move(ne));
        return RESP::integer(added);
    }
    int64_t old_sz = entry_memory_estimate(args[0], *entry);
    auto &map = std::get<std::unordered_map<str, str> >(entry->value);
    int added = 0;
    for (size_t i = 1; i + 1 < args.size(); i += 2) {
        auto [it, inserted] = map.insert_or_assign(args[i], args[i + 1]);
        if (inserted) added++;
    }
    entry->expiry_ms = -1;
    if (!g_replication_mode) entry->VecClk[self_node.id]++;
    store_commit_update(args[0], old_sz);
    return RESP::integer(added);
}

str handle_hget(const TOKENS &args) {
    if (args.size() < 2) return RESP::error("wrong number of arguments for 'hget' command");
    ValueEntry *entry = store_get(args[0]);
    if (!entry || entry->type != Type::HASH)
        return RESP::null_bulk_string();
    auto &map = std::get<std::unordered_map<str, str> >(entry->value);
    auto it = map.find(args[1]);
    if (it == map.end())
        return RESP::null_bulk_string();
    return RESP::bulk_string(it->second);
}

str handle_hdel(const TOKENS &args) {
    if (args.size() < 2) return RESP::error("wrong number of arguments for 'hdel' command");
    ValueEntry *entry = store_get(args[0]);
    if (!entry || entry->type != Type::HASH)
        return RESP::integer(0);
    int64_t old_sz = entry_memory_estimate(args[0], *entry);
    auto &map = std::get<std::unordered_map<str, str> >(entry->value);
    int removed = 0;
    for (size_t i = 1; i < args.size(); i++) {
        if (map.erase(args[i])) removed++;
    }
    entry->expiry_ms = -1;
    if (!g_replication_mode) entry->VecClk[self_node.id]++;
    store_commit_update(args[0], old_sz);
    return RESP::integer(removed);
}

str handle_hgetall(const TOKENS &args) {
    if (args.empty()) return RESP::error("wrong number of arguments for 'hgetall' command");
    ValueEntry *entry = store_get(args[0]);
    if (!entry || entry->type != Type::HASH)
        return RESP::null_array();
    auto &map = std::get<std::unordered_map<str, str> >(entry->value);
    TOKENS res;
    res.reserve(map.size() * 2);
    for (const auto &[f, v]: map) {
        res.push_back(RESP::bulk_string(f));
        res.push_back(RESP::bulk_string(v));
    }
    return RESP::array_raw(res);
}

str handle_hexists(const TOKENS &args) {
    if (args.size() < 2) return RESP::error("wrong number of arguments for 'hexists' command");
    ValueEntry *entry = store_get(args[0]);
    if (!entry || entry->type != Type::HASH)
        return RESP::integer(0);
    auto &map = std::get<std::unordered_map<str, str> >(entry->value);
    return RESP::integer(map.find(args[1]) != map.end() ? 1 : 0);
}

str handle_hlen(const TOKENS &args) {
    if (args.empty()) return RESP::error("wrong number of arguments for 'hlen' command");
    ValueEntry *entry = store_get(args[0]);
    if (!entry || entry->type != Type::HASH)
        return RESP::integer(0);
    return RESP::integer(static_cast<long long>(
        std::get<std::unordered_map<str, str> >(entry->value).size()));
}

str handle_hmset(const TOKENS &args) {
    if (args.size() < 3) return RESP::error("wrong number of arguments for 'hmset' command");
    handle_hset(args);
    return RESP::ok();
}

str handle_hkeys(const TOKENS &args) {
    if (args.empty()) return RESP::error("wrong number of arguments for 'hkeys' command");
    ValueEntry *entry = store_get(args[0]);
    if (!entry || entry->type != Type::HASH)
        return RESP::null_array();
    auto &map = std::get<std::unordered_map<str, str> >(entry->value);
    TOKENS res;
    res.reserve(map.size());
    for (const auto &[f, _] : map)
        res.push_back(RESP::bulk_string(f));
    return RESP::array_raw(res);
}

str handle_hvals(const TOKENS &args) {
    if (args.empty()) return RESP::error("wrong number of arguments for 'hvals' command");
    ValueEntry *entry = store_get(args[0]);
    if (!entry || entry->type != Type::HASH)
        return RESP::null_array();
    auto &map = std::get<std::unordered_map<str, str> >(entry->value);
    TOKENS res;
    res.reserve(map.size());
    for (const auto &[_, v] : map)
        res.push_back(RESP::bulk_string(v));
    return RESP::array_raw(res);
}

str handle_hincrby(const TOKENS &args) {
    if (args.size() < 3) return RESP::error("wrong number of arguments for 'hincrby' command");
    ValueEntry *entry = store_get(args[0]);
    if (!entry || entry->type != Type::HASH) {
        ValueEntry ne;
        ne.type = Type::HASH;
        auto &map = ne.value.emplace<std::unordered_map<str, str> >();
        int64_t delta;
        if (!parse_int(args[2], delta)) return RESP::error("value is not an integer or out of range");
        map[args[1]] = std::to_string(delta);
        store_set(args[0], std::move(ne));
        return RESP::integer(delta);
    }
    auto &map = std::get<std::unordered_map<str, str> >(entry->value);
    auto it = map.find(args[1]);
    int64_t val = 0;
    if (it != map.end()) {
        if (!parse_int(it->second, val)) return RESP::error("hash value is not an integer");
    }
    int64_t delta;
    if (!parse_int(args[2], delta)) return RESP::error("value is not an integer or out of range");
    
    int64_t old_sz = entry_memory_estimate(args[0], *entry);
    val += delta;
    map[args[1]] = std::to_string(val);
    entry->expiry_ms = -1;
    if (!g_replication_mode) entry->VecClk[self_node.id]++;
    store_commit_update(args[0], old_sz);
    return RESP::integer(val);
}

// ── Set handlers ──

str handle_sadd(const TOKENS &args) {
    if (args.size() < 2) return RESP::error("wrong number of arguments for 'sadd' command");
    ValueEntry *entry = store_get(args[0]);
    if (!entry || entry->type != Type::SET) {
        ValueEntry ne;
        ne.type = Type::SET;
        auto &set = ne.value.emplace<std::unordered_set<str> >();
        int added = 0;
        for (size_t i = 1; i < args.size(); i++) {
            if (set.insert(args[i]).second) added++;
        }
        store_set(args[0], std::move(ne));
        return RESP::integer(added);
    }
    int64_t old_sz = entry_memory_estimate(args[0], *entry);
    auto &set = std::get<std::unordered_set<str> >(entry->value);
    int added = 0;
    for (size_t i = 1; i < args.size(); i++) {
        if (set.insert(args[i]).second) added++;
    }
    entry->expiry_ms = -1;
    if (!g_replication_mode) entry->VecClk[self_node.id]++;
    store_commit_update(args[0], old_sz);
    return RESP::integer(added);
}

str handle_srem(const TOKENS &args) {
    if (args.size() < 2) return RESP::error("wrong number of arguments for 'srem' command");
    ValueEntry *entry = store_get(args[0]);
    if (!entry || entry->type != Type::SET)
        return RESP::integer(0);
    int64_t old_sz = entry_memory_estimate(args[0], *entry);
    auto &set = std::get<std::unordered_set<str> >(entry->value);
    int removed = 0;
    for (size_t i = 1; i < args.size(); i++) {
        if (set.erase(args[i])) removed++;
    }
    entry->expiry_ms = -1;
    if (!g_replication_mode) entry->VecClk[self_node.id]++;
    store_commit_update(args[0], old_sz);
    return RESP::integer(removed);
}

str handle_smembers(const TOKENS &args) {
    if (args.empty()) return RESP::error("wrong number of arguments for 'smembers' command");
    ValueEntry *entry = store_get(args[0]);
    if (!entry || entry->type != Type::SET)
        return RESP::null_array();
    auto &set = std::get<std::unordered_set<str> >(entry->value);
    TOKENS res;
    res.reserve(set.size());
    for (const auto &m: set)
        res.push_back(RESP::bulk_string(m));
    return RESP::array_raw(res);
}

str handle_scard(const TOKENS &args) {
    if (args.empty()) return RESP::error("wrong number of arguments for 'scard' command");
    ValueEntry *entry = store_get(args[0]);
    if (!entry || entry->type != Type::SET)
        return RESP::integer(0);
    return RESP::integer(static_cast<long long>(
        std::get<std::unordered_set<str> >(entry->value).size()));
}

str handle_sismember(const TOKENS &args) {
    if (args.size() < 2) return RESP::error("wrong number of arguments for 'sismember' command");
    ValueEntry *entry = store_get(args[0]);
    if (!entry || entry->type != Type::SET)
        return RESP::integer(0);
    auto &set = std::get<std::unordered_set<str> >(entry->value);
    return RESP::integer(set.find(args[1]) != set.end() ? 1 : 0);
}

str handle_spop(const TOKENS &args) {
    if (args.empty()) return RESP::error("wrong number of arguments for 'spop' command");
    ValueEntry *entry = store_get(args[0]);
    if (!entry || entry->type != Type::SET)
        return RESP::null_bulk_string();
    auto &set = std::get<std::unordered_set<str> >(entry->value);
    if (set.empty())
        return RESP::null_bulk_string();

    int64_t count = 1;
    if (args.size() >= 2) {
        if (!parse_int(args[1], count) || count < 0)
            return RESP::error("value is not an integer or out of range");
    }

    TOKENS popped;
    for (int64_t i = 0; i < count && !set.empty(); ++i) {
        auto it = set.begin();
        popped.push_back(*it);
        set.erase(it);
    }

    entry->expiry_ms = -1;
    if (!g_replication_mode) entry->VecClk[self_node.id]++;

    if (args.size() >= 2) {
        TOKENS res;
        for (const auto &m: popped)
            res.push_back(RESP::bulk_string(m));
        return RESP::array_raw(res);
    }
    return RESP::bulk_string(popped[0]);
}

// ── Sorted Set handlers ──

str handle_zadd(const TOKENS &args) {
    if (args.size() < 3) return RESP::error("wrong number of arguments for 'zadd' command");
    ValueEntry *entry = store_get(args[0]);
    if (!entry || entry->type != Type::ZSET) {
        ValueEntry ne;
        ne.type = Type::ZSET;
        auto &zs = ne.value.emplace<SortedSet>();
        int added = 0;
        for (size_t i = 1; i + 1 < args.size(); i += 2) {
            double s;
            auto [p, ec] = std::from_chars(args[i].data(), args[i].data() + args[i].size(), s);
            if (ec != std::errc()) continue;
            zs.AVAILABLE[{s, args[i + 1]}] = true;
            zs.SCORE[args[i + 1]] = s;
            added++;
        }
        store_set(args[0], std::move(ne));
        return RESP::integer(added);
    }
    auto &zs = std::get<SortedSet>(entry->value);
    int added = 0;
    int64_t old_sz = entry_memory_estimate(args[0], *entry);
    for (size_t i = 1; i + 1 < args.size(); i += 2) {
        double s;
        auto [p, ec] = std::from_chars(args[i].data(), args[i].data() + args[i].size(), s);
        if (ec != std::errc()) continue;
        auto it = zs.SCORE.find(args[i + 1]);
        if (it != zs.SCORE.end()) {
            zs.AVAILABLE.erase({it->second, args[i + 1]});
        } else {
            added++;
        }
        zs.AVAILABLE[{s, args[i + 1]}] = true;
        zs.SCORE[args[i + 1]] = s;
    }
    entry->expiry_ms = -1;
    if (!g_replication_mode) entry->VecClk[self_node.id]++;
    store_commit_update(args[0], old_sz);
    return RESP::integer(added);
}

str handle_zrem(const TOKENS &args) {
    if (args.size() < 2) return RESP::error("wrong number of arguments for 'zrem' command");
    ValueEntry *entry = store_get(args[0]);
    if (!entry || entry->type != Type::ZSET)
        return RESP::integer(0);
    auto &zs = std::get<SortedSet>(entry->value);
    int removed = 0;
    int64_t old_sz = entry_memory_estimate(args[0], *entry);
    for (size_t i = 1; i < args.size(); i++) {
        auto it = zs.SCORE.find(args[i]);
        if (it != zs.SCORE.end()) {
            zs.AVAILABLE.erase({it->second, args[i]});
            zs.SCORE.erase(it);
            removed++;
        }
    }
    entry->expiry_ms = -1;
    if (!g_replication_mode) entry->VecClk[self_node.id]++;
    store_commit_update(args[0], old_sz);
    return RESP::integer(removed);
}

str handle_zrange(const TOKENS &args) {
    if (args.size() < 3) return RESP::error("wrong number of arguments for 'zrange' command");
    ValueEntry *entry = store_get(args[0]);
    if (!entry || entry->type != Type::ZSET)
        return RESP::null_array();
    auto &zs = std::get<SortedSet>(entry->value);
    int64_t start, stop;
    if (!parse_int(args[1], start) || !parse_int(args[2], stop))
        return RESP::error("value is not an integer or out of range");
    bool with_scores = (args.size() >= 4 && args[3] == "WITHSCORES");
    size_t total = zs.AVAILABLE.size();
    if (start < 0) start = static_cast<int64_t>(total) + start;
    if (stop < 0) stop = static_cast<int64_t>(total) + stop;
    if (start < 0) start = 0;
    if (start > static_cast<int64_t>(total)) return RESP::null_array();
    size_t s = static_cast<size_t>(start);
    size_t e = (stop >= static_cast<int64_t>(total)) ? total : static_cast<size_t>(stop + 1);
    if (s >= e) return RESP::null_array();
    TOKENS res;
    size_t idx = 0;
    for (auto it = zs.AVAILABLE.begin(); it != zs.AVAILABLE.end() && idx < e; ++it, ++idx) {
        if (idx >= s) {
            res.push_back(RESP::bulk_string(it->first.second));
            if (with_scores)
                res.push_back(RESP::bulk_string(std::to_string(it->first.first)));
        }
    }
    return RESP::array_raw(res);
}

str handle_zrevrange(const TOKENS &args) {
    if (args.size() < 3) return RESP::error("wrong number of arguments for 'zrevrange' command");
    ValueEntry *entry = store_get(args[0]);
    if (!entry || entry->type != Type::ZSET)
        return RESP::null_array();
    auto &zs = std::get<SortedSet>(entry->value);
    int64_t start, stop;
    if (!parse_int(args[1], start) || !parse_int(args[2], stop))
        return RESP::error("value is not an integer or out of range");
    bool with_scores = (args.size() >= 4 && args[3] == "WITHSCORES");
    size_t total = zs.AVAILABLE.size();
    if (start < 0) start = static_cast<int64_t>(total) + start;
    if (stop < 0) stop = static_cast<int64_t>(total) + stop;
    if (start < 0) start = 0;
    if (stop < 0) return RESP::null_array();
    if (static_cast<size_t>(start) >= total) return RESP::null_array();
    size_t s = static_cast<size_t>(start);
    size_t e = static_cast<size_t>(stop >= static_cast<int64_t>(total - 1) ? total - 1 : stop);
    if (s > e) return RESP::null_array();
    TOKENS res;
    size_t idx = 0;
    for (auto it = zs.AVAILABLE.rbegin(); it != zs.AVAILABLE.rend() && idx <= e; ++it, ++idx) {
        if (idx < s) continue;
        res.push_back(RESP::bulk_string(it->first.second));
        if (with_scores)
            res.push_back(RESP::bulk_string(std::to_string(it->first.first)));
    }
    return RESP::array_raw(res);
}

str handle_zrank(const TOKENS &args) {
    if (args.size() < 2) return RESP::error("wrong number of arguments for 'zrank' command");
    ValueEntry *entry = store_get(args[0]);
    if (!entry || entry->type != Type::ZSET)
        return RESP::null_bulk_string();
    auto &zs = std::get<SortedSet>(entry->value);
    auto sit = zs.SCORE.find(args[1]);
    if (sit == zs.SCORE.end())
        return RESP::null_bulk_string();
    long long rank = 0;
    for (auto it = zs.AVAILABLE.begin(); it != zs.AVAILABLE.end(); ++it, ++rank) {
        if (it->first.second == args[1])
            return RESP::integer(rank);
    }
    return RESP::null_bulk_string();
}

str handle_zscore(const TOKENS &args) {
    if (args.size() < 2) return RESP::error("wrong number of arguments for 'zscore' command");
    ValueEntry *entry = store_get(args[0]);
    if (!entry || entry->type != Type::ZSET)
        return RESP::null_bulk_string();
    auto &zs = std::get<SortedSet>(entry->value);
    auto it = zs.SCORE.find(args[1]);
    if (it == zs.SCORE.end())
        return RESP::null_bulk_string();
    std::ostringstream oss;
    oss << it->second;
    return RESP::bulk_string(oss.str());
}

str handle_zcard(const TOKENS &args) {
    if (args.empty()) return RESP::error("wrong number of arguments for 'zcard' command");
    ValueEntry *entry = store_get(args[0]);
    if (!entry || entry->type != Type::ZSET)
        return RESP::integer(0);
    return RESP::integer(static_cast<long long>(
        std::get<SortedSet>(entry->value).AVAILABLE.size()));
}

str handle_zincrby(const TOKENS &args) {
    if (args.size() < 3) return RESP::error("wrong number of arguments for 'zincrby' command");
    double delta;
    auto [pd, ecd] = std::from_chars(args[1].data(), args[1].data() + args[1].size(), delta);
    if (ecd != std::errc()) return RESP::error("value is not a valid float");
    const str &member = args[2];
    ValueEntry *entry = store_get(args[0]);
    if (!entry || entry->type != Type::ZSET) {
        ValueEntry ne;
        ne.type = Type::ZSET;
        auto &zs = ne.value.emplace<SortedSet>();
        zs.AVAILABLE[{delta, member}] = true;
        zs.SCORE[member] = delta;
        store_set(args[0], std::move(ne));
        std::ostringstream oss;
        oss << delta;
        return RESP::bulk_string(oss.str());
    }
    auto &zs = std::get<SortedSet>(entry->value);
    auto it = zs.SCORE.find(member);
    double new_score = delta;
    int64_t old_sz = entry_memory_estimate(args[0], *entry);
    if (it != zs.SCORE.end()) {
        zs.AVAILABLE.erase({it->second, member});
        new_score = it->second + delta;
    }
    zs.AVAILABLE[{new_score, member}] = true;
    zs.SCORE[member] = new_score;
    entry->expiry_ms = -1;
    if (!g_replication_mode) entry->VecClk[self_node.id]++;
    store_commit_update(args[0], old_sz);
    std::ostringstream oss;
    oss << new_score;
    return RESP::bulk_string(oss.str());
}

str handle_zpopmin(const TOKENS &args) {
    if (args.empty()) return RESP::error("wrong number of arguments for 'zpopmin' command");
    ValueEntry *entry = store_get(args[0]);
    if (!entry || entry->type != Type::ZSET)
        return RESP::null_array();
    auto &zs = std::get<SortedSet>(entry->value);
    if (zs.AVAILABLE.empty())
        return RESP::null_array();

    int64_t count = 1;
    if (args.size() >= 2) {
        if (!parse_int(args[1], count) || count <= 0)
            return RESP::error("value is not an integer or out of range");
    }

    int64_t old_sz = entry_memory_estimate(args[0], *entry);
    TOKENS res;
    for (int64_t i = 0; i < count && !zs.AVAILABLE.empty(); ++i) {
        auto it = zs.AVAILABLE.begin();
        const auto &[score_member, present] = *it;
        double score = score_member.first;
        const str &member = score_member.second;

        res.push_back(RESP::bulk_string(member));
        res.push_back(RESP::bulk_string(std::to_string(score)));

        zs.SCORE.erase(member);
        zs.AVAILABLE.erase(it);
    }

    entry->expiry_ms = -1;
    if (!g_replication_mode) entry->VecClk[self_node.id]++;
    store_commit_update(args[0], old_sz);
    return RESP::array_raw(res);
}

str handle_zrangebyscore(const TOKENS &args) {
    if (args.size() < 3) return RESP::error("wrong number of arguments for 'zrangebyscore' command");
    ValueEntry *entry = store_get(args[0]);
    if (!entry || entry->type != Type::ZSET)
        return RESP::null_array();
    auto &zs = std::get<SortedSet>(entry->value);
    double min_score, max_score;
    auto [p1, ec1] = std::from_chars(args[1].data(), args[1].data() + args[1].size(), min_score);
    auto [p2, ec2] = std::from_chars(args[2].data(), args[2].data() + args[2].size(), max_score);
    if (ec1 != std::errc() || ec2 != std::errc())
        return RESP::error("value is not a valid float");

    bool with_scores = false;
    int64_t limit_offset = -1, limit_count = -1;
    for (size_t i = 3; i < args.size(); i++) {
        if (args[i] == "WITHSCORES") {
            with_scores = true;
        } else if (args[i] == "LIMIT" && i + 2 < args.size()) {
            parse_int(args[i + 1], limit_offset);
            parse_int(args[i + 2], limit_count);
            i += 2;
        }
    }

    TOKENS res;
    int64_t idx = 0, returned = 0;
    for (auto it = zs.AVAILABLE.begin(); it != zs.AVAILABLE.end(); ++it) {
        double score = it->first.first;
        if (score < min_score || score > max_score) continue;
        if (limit_offset >= 0 && idx < limit_offset) { idx++; continue; }
        idx++;
        res.push_back(RESP::bulk_string(it->first.second));
        if (with_scores)
            res.push_back(RESP::bulk_string(std::to_string(score)));
        if (++returned == limit_count) break;
    }
    return RESP::array_raw(res);
}

str handle_command(const TOKENS &) {
    return RESP::ok();
}

// ── List handlers ──

str handle_lrem(const TOKENS &args) {
    if (args.size() < 3) return RESP::error("wrong number of arguments for 'lrem' command");
    int64_t count;
    if (!parse_int(args[1], count))
        return RESP::error("value is not an integer or out of range");
    ValueEntry *entry = store_get(args[0]);
    if (!entry || entry->type != Type::LIST)
        return RESP::integer(0);
    auto &list = std::get<std::deque<str>>(entry->value);
    const str &value = args[2];
    int removed = 0;
    int64_t old_sz = entry_memory_estimate(args[0], *entry);
    if (count == 0) {
        auto it = list.begin();
        while (it != list.end()) {
            if (*it == value) { it = list.erase(it); removed++; }
            else ++it;
        }
    } else if (count > 0) {
        auto it = list.begin();
        while (it != list.end() && removed < count) {
            if (*it == value) { it = list.erase(it); removed++; }
            else ++it;
        }
    } else {
        auto it = list.rbegin();
        while (it != list.rend() && removed < -count) {
            if (*it == value) { it = decltype(it)(list.erase(std::next(it).base())); removed++; }
            else ++it;
        }
    }
    entry->expiry_ms = -1;
    if (!g_replication_mode) entry->VecClk[self_node.id]++;
    store_commit_update(args[0], old_sz);
    return RESP::integer(removed);
}

str handle_lpush(const TOKENS &args) {
    if (args.size() < 2) return RESP::error("wrong number of arguments for 'lpush' command");
    ValueEntry *entry = store_get(args[0]);
    if (!entry || entry->type != Type::LIST) {
        ValueEntry ne;
        ne.type = Type::LIST;
        auto &list = ne.value.emplace<std::deque<str> >();
        for (size_t i = 1; i < args.size(); i++)
            list.push_front(args[i]);
        long long len = static_cast<long long>(list.size());
        store_set(args[0], std::move(ne));
        return RESP::integer(len);
    }
    auto &list = std::get<std::deque<str> >(entry->value);
    int64_t old_sz = entry_memory_estimate(args[0], *entry);
    for (size_t i = 1; i < args.size(); i++)
        list.push_front(args[i]);
    entry->expiry_ms = -1;
    if (!g_replication_mode) entry->VecClk[self_node.id]++;
    store_commit_update(args[0], old_sz);
    return RESP::integer(static_cast<long long>(list.size()));
}

str handle_rpush(const TOKENS &args) {
    if (args.size() < 2) return RESP::error("wrong number of arguments for 'rpush' command");
    ValueEntry *entry = store_get(args[0]);
    if (!entry || entry->type != Type::LIST) {
        ValueEntry ne;
        ne.type = Type::LIST;
        auto &list = ne.value.emplace<std::deque<str> >();
        for (size_t i = 1; i < args.size(); i++)
            list.push_back(args[i]);
        long long len = static_cast<long long>(list.size());
        store_set(args[0], std::move(ne));
        return RESP::integer(len);
    }
    auto &list = std::get<std::deque<str> >(entry->value);
    int64_t old_sz = entry_memory_estimate(args[0], *entry);
    for (size_t i = 1; i < args.size(); i++)
        list.push_back(args[i]);
    entry->expiry_ms = -1;
    if (!g_replication_mode) entry->VecClk[self_node.id]++;
    store_commit_update(args[0], old_sz);
    return RESP::integer(static_cast<long long>(list.size()));
}

str handle_lpop(const TOKENS &args) {
    if (args.empty()) return RESP::error("wrong number of arguments for 'lpop' command");
    ValueEntry *entry = store_get(args[0]);
    if (!entry || entry->type != Type::LIST)
        return RESP::null_bulk_string();
    auto &list = std::get<std::deque<str> >(entry->value);
    if (list.empty()) return RESP::null_bulk_string();
    int64_t old_sz = entry_memory_estimate(args[0], *entry);
    str val = std::move(list.front());
    list.pop_front();
    entry->expiry_ms = -1;
    if (!g_replication_mode) entry->VecClk[self_node.id]++;
    store_commit_update(args[0], old_sz);
    return RESP::bulk_string(val);
}

str handle_rpop(const TOKENS &args) {
    if (args.empty()) return RESP::error("wrong number of arguments for 'rpop' command");
    ValueEntry *entry = store_get(args[0]);
    if (!entry || entry->type != Type::LIST)
        return RESP::null_bulk_string();
    auto &list = std::get<std::deque<str> >(entry->value);
    if (list.empty()) return RESP::null_bulk_string();
    int64_t old_sz = entry_memory_estimate(args[0], *entry);
    str val = std::move(list.back());
    list.pop_back();
    entry->expiry_ms = -1;
    if (!g_replication_mode) entry->VecClk[self_node.id]++;
    store_commit_update(args[0], old_sz);
    return RESP::bulk_string(val);
}

str handle_lrange(const TOKENS &args) {
    if (args.size() < 3) return RESP::error("wrong number of arguments for 'lrange' command");
    ValueEntry *entry = store_get(args[0]);
    if (!entry || entry->type != Type::LIST)
        return RESP::null_array();
    auto &list = std::get<std::deque<str> >(entry->value);
    int64_t start, stop;
    if (!parse_int(args[1], start) || !parse_int(args[2], stop))
        return RESP::error("value is not an integer or out of range");
    size_t total = list.size();
    if (start < 0) start = static_cast<int64_t>(total) + start;
    if (stop < 0) stop = static_cast<int64_t>(total) + stop;
    if (start < 0) start = 0;
    if (start > static_cast<int64_t>(total)) return RESP::null_array();
    size_t s = static_cast<size_t>(start);
    size_t e = (stop >= static_cast<int64_t>(total)) ? total : static_cast<size_t>(stop + 1);
    if (s >= e) return RESP::null_array();
    TOKENS res;
    for (size_t i = s; i < e; i++)
        res.push_back(RESP::bulk_string(list[i]));
    return RESP::array_raw(res);
}

str handle_llen(const TOKENS &args) {
    if (args.empty()) return RESP::error("wrong number of arguments for 'llen' command");
    ValueEntry *entry = store_get(args[0]);
    if (!entry || entry->type != Type::LIST)
        return RESP::integer(0);
    return RESP::integer(static_cast<long long>(
        std::get<std::deque<str> >(entry->value).size()));
}

str handle_lindex(const TOKENS &args) {
    if (args.size() < 2) return RESP::error("wrong number of arguments for 'lindex' command");
    ValueEntry *entry = store_get(args[0]);
    if (!entry || entry->type != Type::LIST)
        return RESP::null_bulk_string();
    auto &list = std::get<std::deque<str> >(entry->value);
    int64_t idx;
    if (!parse_int(args[1], idx))
        return RESP::error("value is not an integer or out of range");
    if (idx < 0) idx = static_cast<int64_t>(list.size()) + idx;
    if (idx < 0 || idx >= static_cast<int64_t>(list.size()))
        return RESP::null_bulk_string();
    return RESP::bulk_string(list[static_cast<size_t>(idx)]);
}

str handle_linsert(const TOKENS &args) {
    if (args.size() < 4) return RESP::error("wrong number of arguments for 'linsert' command");
    ValueEntry *entry = store_get(args[0]);
    if (!entry || entry->type != Type::LIST)
        return RESP::error_wrongtype();
    auto &list = std::get<std::deque<str>>(entry->value);
    const str &pivot = args[2];
    const str &value = args[3];
    bool before = (args[1] == "BEFORE");
    bool after = (args[1] == "AFTER");
    if (!before && !after)
        return RESP::error("syntax error");
    
    int64_t old_sz = entry_memory_estimate(args[0], *entry);
    for (size_t i = 0; i < list.size(); i++) {
        if (list[i] == pivot) {
            if (before)
                list.emplace(list.begin() + static_cast<ptrdiff_t>(i), value);
            else
                list.emplace(list.begin() + static_cast<ptrdiff_t>(i + 1), value);
            entry->expiry_ms = -1;
            if (!g_replication_mode) entry->VecClk[self_node.id]++;
            store_commit_update(args[0], old_sz);
            return RESP::integer(static_cast<long long>(list.size()));
        }
    }
    return RESP::integer(-1);
}

// ── Expiry handlers ──

str handle_expire(const TOKENS &args) {
    if (args.size() < 2) return RESP::error("wrong number of arguments for 'expire' command");
    ValueEntry *entry = store_get(args[0]);
    if (!entry) return RESP::integer(0);
    int64_t sec;
    if (!parse_int(args[1], sec)) return RESP::error("value is not an integer or out of range");
    entry->expiry_ms = now_ms() + sec * 1000;
    if (!g_replication_mode) entry->VecClk[self_node.id]++;
    return RESP::integer(1);
}

str handle_pexpire(const TOKENS &args) {
    if (args.size() < 2) return RESP::error("wrong number of arguments for 'pexpire' command");
    ValueEntry *entry = store_get(args[0]);
    if (!entry) return RESP::integer(0);
    int64_t ms;
    if (!parse_int(args[1], ms)) return RESP::error("value is not an integer or out of range");
    entry->expiry_ms = now_ms() + ms;
    if (!g_replication_mode) entry->VecClk[self_node.id]++;
    return RESP::integer(1);
}

str handle_expiretime(const TOKENS &args) {
    if (args.empty()) return RESP::error("wrong number of arguments for 'expiretime' command");
    ValueEntry *entry = store_get(args[0]);
    if (!entry) return RESP::integer(-2);
    if (entry->expiry_ms == -1) return RESP::integer(-1);
    return RESP::integer(entry->expiry_ms / 1000);
}

str handle_pexpiretime(const TOKENS &args) {
    if (args.empty()) return RESP::error("wrong number of arguments for 'pexpiretime' command");
    ValueEntry *entry = store_get(args[0]);
    if (!entry) return RESP::integer(-2);
    if (entry->expiry_ms == -1) return RESP::integer(-1);
    return RESP::integer(entry->expiry_ms);
}

str handle_ttl(const TOKENS &args) {
    if (args.empty()) return RESP::error("wrong number of arguments for 'ttl' command");
    ValueEntry *entry = store_get(args[0]);
    if (!entry) return RESP::integer(-2);
    if (entry->expiry_ms == -1) return RESP::integer(-1);
    long long rem = (entry->expiry_ms - now_ms()) / 1000;
    return RESP::integer(rem < 0 ? -2 : rem);
}

str handle_pttl(const TOKENS &args) {
    if (args.empty()) return RESP::error("wrong number of arguments for 'pttl' command");
    ValueEntry *entry = store_get(args[0]);
    if (!entry) return RESP::integer(-2);
    if (entry->expiry_ms == -1) return RESP::integer(-1);
    long long rem = entry->expiry_ms - now_ms();
    return RESP::integer(rem < 0 ? -2 : rem);
}

str handle_persist(const TOKENS &args) {
    if (args.empty()) return RESP::error("wrong number of arguments for 'persist' command");
    ValueEntry *entry = store_get(args[0]);
    if (!entry || entry->expiry_ms == -1) return RESP::integer(0);
    entry->expiry_ms = -1;
    if (!g_replication_mode) entry->VecClk[self_node.id]++;
    return RESP::integer(1);
}

// ── Admin handlers ──

str handle_type(const TOKENS &args) {
    if (args.empty()) return RESP::error("wrong number of arguments for 'type' command");
    ValueEntry *entry = store_get(args[0]);
    if (!entry) return RESP::simple_string("none");
    switch (entry->type) {
        case Type::STRING: return RESP::simple_string("string");
        case Type::SET: return RESP::simple_string("set");
        case Type::ZSET: return RESP::simple_string("zset");
        case Type::HASH: return RESP::simple_string("hash");
        case Type::LIST: return RESP::simple_string("list");
        case Type::STREAM: return RESP::simple_string("stream");
        case Type::TOMBSTONE: return RESP::simple_string("none");
    }
    return RESP::simple_string("none");
}

str handle_dbsize(const TOKENS &) {
    return RESP::integer(static_cast<long long>(STORE.size()));
}

str handle_flushdb(const TOKENS &) {
    std::vector<str> keys;
    keys.reserve(STORE.size());
    for (const auto& [k, _] : STORE)
        keys.push_back(k);
    for (const auto& k : keys)
        store_apply_tombstone(k);
    return RESP::ok();
}

str handle_info(const TOKENS &) {
    str info;
    info += "# Server\r\n";
    info += "redis_version:7.2.0\r\n";
    info += "tcp_port:6379\r\n";
    info += "# Keyspace\r\n";
    info += "db0:keys=" + std::to_string(STORE.size()) + ",expires=0\r\n";
    info += "# Cluster\r\n";
    info += "cluster_known_nodes:" + std::to_string(cluster_state.size()) + "\r\n";
    info += "cluster_keys:" + std::to_string(store_keys().size()) + "\r\n";
    info += "cluster_replica_queue_len:" + std::to_string(replica_queue.size()) + "\r\n";
    return RESP::bulk_string(info);
}

str handle_ping(const TOKENS &) {
    return RESP::pong();
}

str handle_cluster(const TOKENS &args) {
    if (args.empty()) return RESP::error("wrong number of arguments for 'cluster' command");
    const str &sub = args[0];
    if (sub == "INFO") {
        size_t alive = 0, suspect = 0, dead = 0, left = 0;
        for (const auto &[node, st] : cluster_state) {
            switch (st) {
                case NodeStatus::ALIVE:   alive++; break;
                case NodeStatus::SUSPECT: suspect++; break;
                case NodeStatus::DEAD:    dead++; break;
                case NodeStatus::LEFT:    left++; break;
            }
        }
        str res = "cluster_state:ok\r\n";
        res += "cluster_slots_assigned:16384\r\n";
        res += "cluster_slots_ok:16384\r\n";
        res += "cluster_known_nodes:" + std::to_string(cluster_state.size()) + "\r\n";
        res += "cluster_size:" + std::to_string(alive) + "\r\n";
        res += "cluster_alive_nodes:" + std::to_string(alive) + "\r\n";
        res += "cluster_suspect_nodes:" + std::to_string(suspect) + "\r\n";
        res += "cluster_dead_nodes:" + std::to_string(dead) + "\r\n";
        res += "cluster_left_nodes:" + std::to_string(left) + "\r\n";
        return RESP::bulk_string(res);
    }
    if (sub == "NODES") {
        str res;
        for (const auto &[node, status]: cluster_state) {
            res += std::to_string(node.id) + " " + node.ip + ":" + std::to_string(node.port) + " ";
            switch (status) {
                case NodeStatus::ALIVE: res += "alive";
                    break;
                case NodeStatus::SUSPECT: res += "suspect";
                    break;
                case NodeStatus::DEAD: res += "dead";
                    break;
                case NodeStatus::LEFT: res += "left";
                    break;
            }
            res += "\n";
        }
        return RESP::bulk_string(res);
    }
    if (sub == "MYID") {
        return RESP::bulk_string(std::to_string(self_node.id));
    }
    if (sub == "MEET") {
        if (args.size() < 3) return RESP::error("wrong number of arguments for 'cluster meet' command");
        str rip = args[1];
        uint16_t rport = static_cast<uint16_t>(std::stoi(args[2]));
        uint64_t temp_id = std::hash<std::string>{}(rip + ":" + std::to_string(rport));

        // Connect and send CLUSTER_JOIN frame with our identity
        auto peer = get_or_connect(temp_id, rip, rport);
        if (peer && peer->fd >= 0) {
            str payload;
            payload.append(reinterpret_cast<const char*>(&self_node.id), sizeof(self_node.id));
            uint32_t ip_len = static_cast<uint32_t>(self_node.ip.size());
            payload.append(reinterpret_cast<const char*>(&ip_len), sizeof(ip_len));
            payload += self_node.ip;
            payload.append(reinterpret_cast<const char*>(&self_node.port), sizeof(self_node.port));
            payload.append(reinterpret_cast<const char*>(&self_node.generation), sizeof(self_node.generation));

            BIN::Frame frame;
            static uint64_t join_id = 1;
            frame.header = {
                .magic = BIN::MAGIC, .version = BIN::VERSION,
                .msg_type = static_cast<uint8_t>(BIN::FrameType::CLUSTER_JOIN),
                .msg_id = join_id++, .sender_id = self_node.id,
                .payload_len = static_cast<uint32_t>(payload.size()), .checksum = 0
            };
            frame.payload = std::move(payload);
            auto serialized = BIN::serialize(frame);
            bool was_empty = peer->write_buf.empty();
            peer->write_buf += serialized;
            if (was_empty) {
                uint32_t flags = EPOLLIN | EPOLLOUT;
                mod_epoll(peer->fd, flags);
            }
        }
        return RESP::ok();
    }
    if (sub == "FORGET") {
        if (args.size() < 2) return RESP::error("wrong number of arguments for 'cluster forget' command");
        int64_t nid;
        if (!parse_int(args[1], nid)) return RESP::error("invalid node id");
        remove_node(NodeID{static_cast<uint64_t>(nid), "", 0});
        uint64_t unid = static_cast<uint64_t>(nid);
        auto pit = peers_by_node_id.find(unid);
        if (pit != peers_by_node_id.end()) {
            fd_to_peer_id.erase(pit->second.fd);
            epoll_ctl(epoll_fd, EPOLL_CTL_DEL, pit->second.fd, nullptr);
            close(pit->second.fd);
            peers_by_node_id.erase(pit);
        }
        return RESP::ok();
    }
    if (sub == "KEYSLOT") {
        if (args.size() < 2) return RESP::error("wrong number of arguments for 'cluster keyslot' command");
        uint64_t token = hash_token(args[1]);
        return RESP::integer(static_cast<long long>(token % 16384));
    }
    if (sub == "SLOTS") {
        TOKENS all_entries;
        for (size_t i = 0; i < 16384; ) {
            NodeID owner = get_owner(std::to_string(i));
            if (owner.id == 0 && self_node.id != 0) owner = self_node;
            size_t end = i;
            while (end < 16383) {
                NodeID next_owner = get_owner(std::to_string(end + 1));
                if (next_owner.id == 0) next_owner = self_node;
                if (next_owner.id != owner.id) break;
                end++;
            }
            TOKENS entry;
            entry.push_back(RESP::integer(static_cast<long long>(i)));
            entry.push_back(RESP::integer(static_cast<long long>(end)));
            TOKENS master;
            master.push_back(RESP::bulk_string(owner.ip));
            master.push_back(RESP::integer(owner.port));
            master.push_back(RESP::bulk_string(std::to_string(owner.id)));
            entry.push_back(RESP::array_raw(master));
            all_entries.push_back(RESP::array_raw(entry));
            i = end + 1;
        }
        return RESP::array_raw(all_entries);
    }
    if (sub == "COUNTKEYSINSLOT") {
        if (args.size() < 2) return RESP::error("wrong number of arguments for 'cluster countkeysinslot' command");
        int64_t slot;
        if (!parse_int(args[1], slot) || slot < 0 || slot >= 16384)
            return RESP::error("invalid slot");
        int64_t count = 0;
        auto all_keys = store_keys();
        for (const auto& k : all_keys) {
            uint64_t token = hash_token(k);
            if (static_cast<uint16_t>(token % 16384) == static_cast<uint16_t>(slot))
                count++;
        }
        return RESP::integer(count);
    }
    if (sub == "GETKEYSINSLOT") {
        if (args.size() < 3) return RESP::error("wrong number of arguments for 'cluster getkeysinslot' command");
        int64_t slot, count;
        if (!parse_int(args[1], slot) || slot < 0 || slot >= 16384)
            return RESP::error("invalid slot");
        if (!parse_int(args[2], count) || count < 0)
            return RESP::error("invalid count");
        TOKENS result;
        auto all_keys = store_keys();
        for (const auto& k : all_keys) {
            if (static_cast<int>(result.size()) >= count) break;
            uint64_t token = hash_token(k);
            if (static_cast<uint16_t>(token % 16384) == static_cast<uint16_t>(slot))
                result.push_back(RESP::bulk_string(k));
        }
        return RESP::array_raw(result);
    }
    if (sub == "RESET") {
        for (auto& [nid, peer] : peers_by_node_id) {
            fd_to_peer_id.erase(peer.fd);
            epoll_ctl(epoll_fd, EPOLL_CTL_DEL, peer.fd, nullptr);
            close(peer.fd);
        }
        peers_by_node_id.clear();
        cluster_state.clear();
        ring.clear();
        return RESP::ok();
    }
    return RESP::error("unknown subcommand for 'cluster' command");
}

str handle_config(const TOKENS &args) {
    if (args.size() < 2) return RESP::error("wrong number of arguments for 'config' command");
    const str &sub = args[0];

    if (sub == "GET") {
        const str &param = args[1];
        if (param == "save" || param == "appendonly" || param == "databases" ||
            param == "maxmemory" || param == "maxmemory-policy") {
            TOKENS res;
            res.push_back(RESP::bulk_string(param));
            if (param == "save")
                res.push_back(RESP::bulk_string(""));
            else if (param == "appendonly")
                res.push_back(RESP::bulk_string("no"));
            else if (param == "databases")
                res.push_back(RESP::bulk_string("16"));
            else if (param == "maxmemory")
                res.push_back(RESP::bulk_string("0"));
            else if (param == "maxmemory-policy")
                res.push_back(RESP::bulk_string("noeviction"));
            return RESP::array_raw(res);
        }
        return RESP::array_raw({RESP::bulk_string(param), RESP::null_bulk_string()});
    }

    if (sub == "SET") {
        return RESP::ok();
    }

    return RESP::error("unknown subcommand for 'config' command");
}

str handle_unknown(const TOKENS &) {
    return RESP::error("unknown command");
}

str handle_xadd(const TOKENS &args) {
    if (args.size() < 4 || (args.size() - 2) % 2 != 0)
        return RESP::error("wrong number of arguments for 'xadd' command");

    const str &key = args[0];
    const str &id_arg = args[1];

    ValueEntry *entry = store_get(key);
    if (entry && entry->type != Type::STREAM)
        return RESP::error_wrongtype();

    Stream stream;
    if (entry && entry->type == Type::STREAM) {
        stream = std::move(std::get<Stream>(entry->value));
    }

    int64_t now_ms_val = now_ms();
    uint64_t ms_part, seq_part;

    if (id_arg == "*") {
        if (now_ms_val > static_cast<int64_t>(stream.last_ms)) {
            stream.last_ms = now_ms_val;
            stream.last_seq = 0;
        } else {
            stream.last_seq++;
        }
        ms_part = stream.last_ms;
        seq_part = stream.last_seq;
    } else {
        auto dash = id_arg.find('-');
        if (dash == str::npos)
            return RESP::error("invalid stream ID format");
        auto [p1, ec1] = std::from_chars(id_arg.data(), id_arg.data() + dash, ms_part);
        auto [p2, ec2] = std::from_chars(id_arg.data() + dash + 1, id_arg.data() + id_arg.size(), seq_part);
        if (ec1 != std::errc() || ec2 != std::errc())
            return RESP::error("invalid stream ID format");
        if (ms_part == 0 && seq_part == 0)
            return RESP::error("The ID specified in XADD must be greater than 0-0");
        if (ms_part < stream.last_ms || (ms_part == stream.last_ms && seq_part <= stream.last_seq))
            return RESP::error("The ID specified in XADD is equal or smaller than the target stream top item");
        stream.last_ms = ms_part;
        stream.last_seq = seq_part;
    }

    StreamEntry se;
    se.id = std::to_string(ms_part) + "-" + std::to_string(seq_part);
    for (size_t i = 2; i + 1 < args.size(); i += 2)
        se.fields[args[i]] = args[i + 1];
    str entry_id = se.id;
    stream.entries.push_back(std::move(se));

    ValueEntry ne;
    ne.type = Type::STREAM;
    ne.value = std::move(stream);
    store_set(key, std::move(ne));
    return RESP::bulk_string(entry_id);
}

str handle_lset(const TOKENS &args) {
    if (args.size() < 3) return RESP::error("wrong number of arguments for 'lset' command");
    ValueEntry *entry = store_get(args[0]);
    if (!entry || entry->type != Type::LIST)
        return RESP::error_wrongtype();
    auto &list = std::get<std::deque<str>>(entry->value);
    int64_t idx;
    if (!parse_int(args[1], idx))
        return RESP::error("value is not an integer or out of range");
    if (idx < 0) idx = static_cast<int64_t>(list.size()) + idx;
    if (idx < 0 || idx >= static_cast<int64_t>(list.size()))
        return RESP::error("index out of range");
    
    int64_t old_sz = entry_memory_estimate(args[0], *entry);
    list[static_cast<size_t>(idx)] = args[2];
    entry->expiry_ms = -1;
    if (!g_replication_mode) entry->VecClk[self_node.id]++;
    store_commit_update(args[0], old_sz);
    return RESP::ok();
}

// ── Replication stubs ──

// Removed replconf and slaveof

str handle_vclock(const TOKENS &args) {
    // VCLOCK <key> <vc_count> <node_id1> <counter1> ...
    if (args.size() < 2) return RESP::ok();
    const str& key = args[0];
    int64_t vc_count;
    if (!parse_int(args[1], vc_count) || vc_count < 0) return RESP::ok();
    if (static_cast<size_t>(2 + vc_count * 2) > args.size()) return RESP::ok();
    ValueEntry* entry = store_get(key);
    if (!entry) return RESP::ok();
    std::unordered_map<uint64_t, counter> restored;
    for (int64_t i = 0; i < vc_count; i++) {
        uint64_t nid = static_cast<uint64_t>(std::atol(args[2 + i * 2].c_str()));
        counter cnt = static_cast<counter>(std::atol(args[2 + i * 2 + 1].c_str()));
        restored[nid] = cnt;
    }
    entry->VecClk = std::move(restored);
    return RESP::ok();
}

str handle_bgrewriteaof(const TOKENS &args) {
    g_rewrite_pending.store(true);
    return RESP::simple_string("Background AOF rewrite scheduled");
}

// ── Command map init ──

bool is_write_command(commandType type) {
    switch (type) {
        case commandType::SET:
        case commandType::SETEX:
        case commandType::SETNX:
        case commandType::GETSET:
        case commandType::MSET:
        case commandType::DEL:
        case commandType::RENAME:
        case commandType::APPEND:
        case commandType::INCR:
        case commandType::INCRBY:
        case commandType::DECR:
        case commandType::DECRBY:
        case commandType::HSET:
        case commandType::HMSET:
        case commandType::HDEL:
        case commandType::HINCRBY:
        case commandType::SADD:
        case commandType::SREM:
        case commandType::SPOP:
        case commandType::ZADD:
        case commandType::ZREM:
        case commandType::ZPOPMIN:
        case commandType::ZINCRBY:
        case commandType::LPUSH:
        case commandType::RPUSH:
        case commandType::LPOP:
        case commandType::RPOP:
        case commandType::LREM:
        case commandType::LINSERT:
        case commandType::XADD:
        case commandType::LSET:
        case commandType::EXPIRE:
        case commandType::PEXPIRE:
        case commandType::PERSIST:
        case commandType::FLUSHDB:
            return true;
        default:
            return false;
    }
}

void init_cmd_map() {
    cmd_map[commandType::GET] = handle_get;
    cmd_map[commandType::SET] = handle_set;
    cmd_map[commandType::GETSET] = handle_getset;
    cmd_map[commandType::SETNX] = handle_setnx;
    cmd_map[commandType::SETEX] = handle_setex;
    cmd_map[commandType::DEL] = handle_del;
    cmd_map[commandType::EXISTS] = handle_exists;
    cmd_map[commandType::KEYS] = handle_keys;
    cmd_map[commandType::RENAME] = handle_rename;
    cmd_map[commandType::APPEND] = handle_append;
    cmd_map[commandType::STRLEN] = handle_strlen;
    cmd_map[commandType::INCR] = handle_incr;
    cmd_map[commandType::DECR] = handle_decr;
    cmd_map[commandType::INCRBY] = handle_incrby;
    cmd_map[commandType::DECRBY] = handle_decrby;
    cmd_map[commandType::MGET] = handle_mget;
    cmd_map[commandType::MSET] = handle_mset;
    cmd_map[commandType::HSET] = handle_hset;
    cmd_map[commandType::HMSET] = handle_hmset;
    cmd_map[commandType::HGET] = handle_hget;
    cmd_map[commandType::HDEL] = handle_hdel;
    cmd_map[commandType::HGETALL] = handle_hgetall;
    cmd_map[commandType::HEXISTS] = handle_hexists;
    cmd_map[commandType::HLEN] = handle_hlen;
    cmd_map[commandType::HKEYS] = handle_hkeys;
    cmd_map[commandType::HVALS] = handle_hvals;
    cmd_map[commandType::HINCRBY] = handle_hincrby;
    cmd_map[commandType::SADD] = handle_sadd;
    cmd_map[commandType::SREM] = handle_srem;
    cmd_map[commandType::SMEMBERS] = handle_smembers;
    cmd_map[commandType::SCARD] = handle_scard;
    cmd_map[commandType::SISMEMBER] = handle_sismember;
    cmd_map[commandType::SPOP] = handle_spop;
    cmd_map[commandType::ZADD] = handle_zadd;
    cmd_map[commandType::ZREM] = handle_zrem;
    cmd_map[commandType::ZRANGE] = handle_zrange;
    cmd_map[commandType::ZREVRANGE] = handle_zrevrange;
    cmd_map[commandType::ZRANK] = handle_zrank;
    cmd_map[commandType::ZSCORE] = handle_zscore;
    cmd_map[commandType::ZCARD] = handle_zcard;
    cmd_map[commandType::ZPOPMIN] = handle_zpopmin;
    cmd_map[commandType::ZINCRBY] = handle_zincrby;
    cmd_map[commandType::ZRANGEBYSCORE] = handle_zrangebyscore;
    cmd_map[commandType::COMMAND] = handle_command;
    cmd_map[commandType::XADD] = handle_xadd;
    cmd_map[commandType::LSET] = handle_lset;
    cmd_map[commandType::LPUSH] = handle_lpush;
    cmd_map[commandType::RPUSH] = handle_rpush;
    cmd_map[commandType::LPOP] = handle_lpop;
    cmd_map[commandType::RPOP] = handle_rpop;
    cmd_map[commandType::LREM] = handle_lrem;
    cmd_map[commandType::LINSERT] = handle_linsert;
    cmd_map[commandType::LRANGE] = handle_lrange;
    cmd_map[commandType::LLEN] = handle_llen;
    cmd_map[commandType::LINDEX] = handle_lindex;
    cmd_map[commandType::TTL] = handle_ttl;
    cmd_map[commandType::PTTL] = handle_pttl;
    cmd_map[commandType::EXPIRE] = handle_expire;
    cmd_map[commandType::PEXPIRE] = handle_pexpire;
    cmd_map[commandType::EXPIRETIME] = handle_expiretime;
    cmd_map[commandType::PEXPIRETIME] = handle_pexpiretime;
    cmd_map[commandType::PERSIST] = handle_persist;
    cmd_map[commandType::TYPE] = handle_type;
    cmd_map[commandType::DBSIZE] = handle_dbsize;
    cmd_map[commandType::FLUSHDB] = handle_flushdb;
    cmd_map[commandType::INFO] = handle_info;
    cmd_map[commandType::PING] = handle_ping;
    cmd_map[commandType::CLUSTER] = handle_cluster;
    cmd_map[commandType::CONFIG] = handle_config;
    cmd_map[commandType::VCLOCK] = handle_vclock;
    cmd_map[commandType::BGREWRITEAOF] = handle_bgrewriteaof;
    cmd_map[commandType::UNKNOWN] = handle_unknown;
}

str execute_command(COMMAND cmd) {
    auto it = cmd_map.find(cmd.type);
    if (it == cmd_map.end()) return handle_unknown(cmd.args);

    return it->second(cmd.args);
}
