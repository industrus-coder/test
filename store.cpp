#include "store.h"

#include <fstream>
#include <iostream>
#include <chrono>
#include <cstdio>
#include <unistd.h>

#include "cmd.h"
#include "cluster.h"
#include "config.h"

bool g_replication_mode = false;
int64_t g_maxmemory_bytes = 256LL * 1024 * 1024;
int64_t g_current_memory_usage = 0;
AOFFsync g_aof_fsync = AOFFsync::EVERYSEC;
std::atomic<uint64_t> g_store_version{0};
std::atomic<bool> g_rewrite_pending{false};
std::unordered_map<Key, ValueEntry> STORE;

static std::ofstream AOF_FILE;
static int AOF_FD = -1;
static str AOF_BUFFER;

static int64_t current_time_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()
    ).count();
}

ValueEntry* store_get(const Key& key) {
    auto it = STORE.find(key);
    if (it == STORE.end())
        return nullptr;

    ValueEntry& entry = it->second;

    if (entry.expiry_ms != -1 && entry.expiry_ms <= current_time_ms()) {
        STORE.erase(it);
        return nullptr;
    }

    if (entry.type == Type::TOMBSTONE)
        return nullptr;

    return &entry;
}

bool store_exists(const Key& key) {
    return store_get(key) != nullptr;
}

bool store_expect_type(const Key& key, Type expected) {
    auto* entry = store_get(key);
    if (!entry) return true; // key doesn't exist → any type is fine
    return entry->type == expected;
}

VClockCmp compare_vclock(
    const std::unordered_map<uint64_t, counter>& a,
    const std::unordered_map<uint64_t, counter>& b)
{
    bool a_newer = false, b_newer = false;
    std::unordered_set<uint64_t> all_ids;
    for (const auto& [id, _] : a) all_ids.insert(id);
    for (const auto& [id, _] : b) all_ids.insert(id);
    for (auto id : all_ids) {
        counter ca = 0, cb = 0;
        auto it = a.find(id);
        if (it != a.end()) ca = it->second;
        it = b.find(id);
        if (it != b.end()) cb = it->second;
        if (ca > cb) a_newer = true;
        if (cb > ca) b_newer = true;
    }
    if (a_newer && !b_newer) return VClockCmp::NEWER;
    if (b_newer && !a_newer) return VClockCmp::OLDER;
    if (!a_newer && !b_newer) return VClockCmp::EQUAL;
    return VClockCmp::CONCURRENT;
}

static str serialize_for_lww(const ValueEntry& entry) {
    str out;
    // Include type
    out += static_cast<char>(entry.type);
    // Include serialized value
    switch (entry.type) {
        case Type::STRING:
            out += std::get<str>(entry.value);
            break;
        case Type::HASH: {
            const auto& map = std::get<std::unordered_map<str, str>>(entry.value);
            std::vector<std::pair<str, str>> sorted(map.begin(), map.end());
            std::sort(sorted.begin(), sorted.end());
            for (const auto& [k, v] : sorted) {
                out += k; out += '\0'; out += v; out += '\0';
            }
            break;
        }
        case Type::SET: {
            const auto& set = std::get<std::unordered_set<str>>(entry.value);
            std::vector<str> sorted(set.begin(), set.end());
            std::sort(sorted.begin(), sorted.end());
            for (const auto& m : sorted) {
                out += m; out += '\0';
            }
            break;
        }
        case Type::ZSET: {
            const auto& zset = std::get<SortedSet>(entry.value);
            std::vector<std::pair<score, member>> sorted;
            sorted.reserve(zset.AVAILABLE.size());
            for (const auto& kv : zset.AVAILABLE)
                sorted.push_back({kv.first.first, kv.first.second});
            std::sort(sorted.begin(), sorted.end());
            for (const auto& [s, m] : sorted)
                out += std::to_string(s) + ':' + m + '\0';
            break;
        }
        case Type::LIST: {
            const auto& list = std::get<std::deque<str>>(entry.value);
            for (const auto& e : list)
                out += e + '\0';
            break;
        }
        case Type::STREAM: {
            const auto& stream = std::get<Stream>(entry.value);
            for (const auto& se : stream.entries) {
                out += se.id + '\0';
                std::vector<std::pair<str, str>> fields(se.fields.begin(), se.fields.end());
                std::sort(fields.begin(), fields.end());
                for (const auto& [k, v] : fields)
                    out += k + '=' + v + '\0';
            }
            break;
        }
        case Type::TOMBSTONE:
            break;
    }
    // Include VecClk deterministically
    std::vector<std::pair<uint64_t, counter>> vclock(entry.VecClk.begin(), entry.VecClk.end());
    std::sort(vclock.begin(), vclock.end());
    for (const auto& [id, cnt] : vclock)
        out += std::to_string(id) + ':' + std::to_string(cnt) + ',';
    return out;
}

int64_t entry_memory_estimate(const Key& key, const ValueEntry& e) {
    int64_t sz = static_cast<int64_t>(key.size() + sizeof(ValueEntry) + 64);
    sz += static_cast<int64_t>(e.VecClk.size() * (sizeof(uint64_t) + sizeof(counter)) + 32);
    if (e.type == Type::STRING && std::holds_alternative<str>(e.value))
        sz += static_cast<int64_t>(std::get<str>(e.value).size());
    return sz;
}

bool check_maxmemory_or_oom() {
    if (g_current_memory_usage >= g_maxmemory_bytes)
        return false;
    return true;
}

void store_bump_vclock(const Key& key) {
    auto it = STORE.find(key);
    if (it != STORE.end())
        it->second.VecClk[self_node.id]++;
}

void store_set(const Key& key, ValueEntry&& entry) {
    if (!g_replication_mode) {
        auto it = STORE.find(key);
        if (it != STORE.end())
            entry.VecClk = it->second.VecClk;
        entry.VecClk[self_node.id]++;
    } else if (!entry.VecClk.empty()) {
        auto it = STORE.find(key);
        if (it != STORE.end()) {
            auto cmp = compare_vclock(entry.VecClk, it->second.VecClk);
            if (cmp == VClockCmp::OLDER) return;
            if (cmp == VClockCmp::CONCURRENT) {
                // Deterministic LWW tiebreak: lexicographically larger serialized value wins
                str local_data = serialize_for_lww(it->second);
                str incoming_data = serialize_for_lww(entry);
                if (local_data >= incoming_data) return;
            }
        }
    }
    auto old_it = STORE.find(key);
    if (old_it != STORE.end())
        g_current_memory_usage -= entry_memory_estimate(key, old_it->second);
    int64_t new_sz = entry_memory_estimate(key, entry);
    STORE[key] = std::move(entry);
    g_current_memory_usage += new_sz;
    g_store_version++;
}

void store_commit_update(const Key& key, int64_t old_sz) {
    auto it = STORE.find(key);
    if (it == STORE.end()) return;
    int64_t new_sz = entry_memory_estimate(key, it->second);
    g_current_memory_usage -= old_sz;
    g_current_memory_usage += new_sz;
    g_store_version++;
}

void store_del(const Key& key) {
    store_apply_tombstone(key);
}

void store_apply_tombstone(const Key& key) {
    auto it = STORE.find(key);
    if (it != STORE.end()) {
        g_current_memory_usage -= entry_memory_estimate(key, it->second);
        ValueEntry& entry = it->second;
        entry.type = Type::TOMBSTONE;
        entry.value = std::monostate{};
        entry.expiry_ms = -1;
        if (!g_replication_mode)
            entry.VecClk[self_node.id]++;
        g_current_memory_usage += entry_memory_estimate(key, entry);
    } else {
        ValueEntry tombstone_entry;
        tombstone_entry.type = Type::TOMBSTONE;
        tombstone_entry.value = std::monostate{};
        tombstone_entry.expiry_ms = -1;
        if (!g_replication_mode)
            tombstone_entry.VecClk[self_node.id]++;
        STORE[key] = std::move(tombstone_entry);
    }
    g_store_version++;
}

str serialize_entry(const Key& key, const ValueEntry& entry) {
    str result;
    switch (entry.type) {
        case Type::STRING: {
            const auto& val = std::get<str>(entry.value);
            COMMAND cmd{commandType::SET, {key, val}};
            result = RESP::serialize_command(cmd);
            break;
        }
        case Type::HASH: {
            const auto& map = std::get<std::unordered_map<str, str>>(entry.value);
            TOKENS args = {key};
            for (const auto& [f, v] : map) {
                args.push_back(f);
                args.push_back(v);
            }
            COMMAND cmd{commandType::HSET, std::move(args)};
            result = RESP::serialize_command(cmd);
            break;
        }
        case Type::SET: {
            const auto& set = std::get<std::unordered_set<str>>(entry.value);
            TOKENS args = {key};
            args.insert(args.end(), set.begin(), set.end());
            COMMAND cmd{commandType::SADD, std::move(args)};
            result = RESP::serialize_command(cmd);
            break;
        }
        case Type::ZSET: {
            const auto& zset = std::get<SortedSet>(entry.value);
            TOKENS args = {key};
            for (const auto& [score_member, _] : zset.AVAILABLE) {
                args.push_back(std::to_string(score_member.first));
                args.push_back(score_member.second);
            }
            COMMAND cmd{commandType::ZADD, std::move(args)};
            result = RESP::serialize_command(cmd);
            break;
        }
        case Type::LIST: {
            const auto& list = std::get<std::deque<str>>(entry.value);
            TOKENS args = {key};
            args.insert(args.end(), list.begin(), list.end());
            COMMAND cmd{commandType::RPUSH, std::move(args)};
            result = RESP::serialize_command(cmd);
            break;
        }
        case Type::STREAM: {
            const auto& stream = std::get<Stream>(entry.value);
            for (const auto& se : stream.entries) {
                TOKENS args = {key, se.id};
                for (const auto& [f, v] : se.fields) {
                    args.push_back(f);
                    args.push_back(v);
                }
                COMMAND cmd{commandType::XADD, std::move(args)};
                result += RESP::serialize_command(cmd);
            }
            return result;
        }
        case Type::TOMBSTONE: {
            COMMAND cmd{commandType::DEL, {key}};
            result = RESP::serialize_command(cmd);
            break;
        }
        default:
            return {};
    }
    // Append PEXPIRE if entry has a TTL
    if (entry.expiry_ms > 0) {
        int64_t ttl = entry.expiry_ms - current_time_ms();
        if (ttl > 0) {
            COMMAND exp{commandType::PEXPIRE, {key, std::to_string(ttl)}};
            result += RESP::serialize_command(exp);
        }
    }
    return result;
}

std::vector<Key> store_keys() {
    std::vector<Key> keys;
    keys.reserve(STORE.size());
    for (const auto& pair : STORE) {
        if (pair.second.type != Type::TOMBSTONE &&
            (pair.second.expiry_ms == -1 || pair.second.expiry_ms > current_time_ms())) {
            keys.push_back(pair.first);
        }
    }
    return keys;
}

void expire_sweep() {
    auto now = current_time_ms();
    size_t checked = 0;
    for (auto it = STORE.begin(); it != STORE.end() && checked < 100; ) {
        if (it->second.type == Type::TOMBSTONE) {
            if (it->second.expiry_ms != -1 && it->second.expiry_ms <= now) {
                g_current_memory_usage -= entry_memory_estimate(it->first, it->second);
                it = STORE.erase(it);
            } else {
                if (it->second.expiry_ms == -1)
                    it->second.expiry_ms = now + config().tombstone_ttl_ms;
                ++it;
            }
        } else if (it->second.expiry_ms != -1 && it->second.expiry_ms <= now) {
            g_current_memory_usage -= entry_memory_estimate(it->first, it->second);
            it = STORE.erase(it);
        } else {
            ++it;
        }
        checked++;
    }
}

void openAOF() {
    if (AOF_FILE.is_open())
        return;
    AOF_FILE.open(
        "append_only.aof",
        std::ios::binary | std::ios::app
    );
    if (AOF_FILE.is_open()) {
        //
    }
}

void fetchAOF() {
    std::ifstream file(
        "append_only.aof",
        std::ios::binary
    );

    if (!file.is_open())
        return;

    str data{
        std::istreambuf_iterator<char>(file),
        std::istreambuf_iterator<char>()
    };

    RESP::Parser parser;
    parser.feed(data);

    COMMAND cmd;

    while (parser.next(cmd)) {
        auto it = cmd_map.find(cmd.type);

        if (it != cmd_map.end())
            it->second(cmd.args);
    }

    parser.clear_consumed();
}

void appendAOF(strv raw_cmd) {
    AOF_BUFFER.append(raw_cmd);
}

void flushAOF() {
    if (!AOF_FILE.is_open())
        return;

    if (AOF_BUFFER.empty())
        return;

    AOF_FILE.write(
        AOF_BUFFER.data(),
        static_cast<std::streamsize>(AOF_BUFFER.size())
    );

    if (g_aof_fsync == AOFFsync::ALWAYS) {
        AOF_FILE.flush();
    } else if (g_aof_fsync == AOFFsync::EVERYSEC) {
        static int64_t last_fsync = 0;
        auto now = current_time_ms();
        if (now - last_fsync >= 1000) {
            AOF_FILE.flush();
            last_fsync = now;
        }
    }
    // NO: rely on kernel flush

    AOF_BUFFER.clear();
}

void closeAOF() {
    flushAOF();

    if (AOF_FILE.is_open())
        AOF_FILE.close();
}

void rewriteAOF() {
    flushAOF();
    if (AOF_FILE.is_open()) AOF_FILE.close();

    std::ofstream tmp("append_only.aof.tmp", std::ios::binary);
    if (!tmp.is_open()) {
        std::cerr << "[AOF] Failed to open temp file for rewrite" << std::endl;
        openAOF();
        return;
    }

    g_replication_mode = true; // prevent VecClk bumps during serialization
    for (const auto& [key, entry] : STORE) {
        if (entry.type == Type::TOMBSTONE) continue;
        if (entry.expiry_ms != -1 && entry.expiry_ms <= current_time_ms()) continue;
        str line = serialize_entry(key, entry);
        if (line.empty()) continue;
        tmp.write(line.data(), static_cast<std::streamsize>(line.size()));
        // Append VCLOCK entry
        if (!entry.VecClk.empty()) {
            TOKENS t;
            t.push_back("VCLOCK");
            t.push_back(key);
            t.push_back(std::to_string(entry.VecClk.size()));
            for (const auto& [nid, cnt] : entry.VecClk) {
                t.push_back(std::to_string(nid));
                t.push_back(std::to_string(cnt));
            }
            str vc_line = RESP::array(t);
            tmp.write(vc_line.data(), static_cast<std::streamsize>(vc_line.size()));
        }
    }
    g_replication_mode = false;
    tmp.flush();
    tmp.close();

    if (std::rename("append_only.aof.tmp", "append_only.aof") != 0) {
        std::cerr << "[AOF] Rewrite rename failed" << std::endl;
        std::remove("append_only.aof.tmp");
    }

    openAOF();
}
