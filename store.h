#pragma once

#ifndef DREDIS_STORE_H
#define DREDIS_STORE_H
#include <cstdint>
#include <atomic>
#include <deque>
#include <map>
#include <variant>
#include <optional>
#include <unordered_set>

#include "parser.h"
using counter = uint64_t;
using score = double;
using member = str;
using present = bool;
using Key = str;

enum class Type {
    STRING,
    SET,
    ZSET,
    HASH,
    LIST,
    STREAM,
    TOMBSTONE
};

struct StreamEntry {
    str id;
    std::unordered_map<str, str> fields;
};

struct Stream {
    std::vector<StreamEntry> entries;
    uint64_t last_ms = 0;
    uint64_t last_seq = 0;
};

struct SortedSet {
    std::map<std::pair<score, member>, present> AVAILABLE;
    std::unordered_map<member, score> SCORE;
};

struct ValueEntry {
    Type type;
    std::variant<
        str,
        std::unordered_set<str>,
        SortedSet,
        std::unordered_map<str, str>,
        std::deque<str>,
        Stream,
        std::monostate
    > value;
    std::unordered_map<uint64_t, counter> VecClk;
    int64_t expiry_ms = -1;
};

extern bool g_replication_mode;
extern int64_t g_maxmemory_bytes;
extern int64_t g_current_memory_usage;

extern std::atomic<uint64_t> g_store_version; // incremented on every write
extern std::atomic<bool> g_rewrite_pending;

enum class AOFFsync { ALWAYS, EVERYSEC, NO };
extern AOFFsync g_aof_fsync;

enum class VClockCmp { NEWER, OLDER, EQUAL, CONCURRENT };

VClockCmp compare_vclock(
    const std::unordered_map<uint64_t, counter>& a,
    const std::unordered_map<uint64_t, counter>& b);

extern std::unordered_map<Key, ValueEntry> STORE;

ValueEntry* store_get(const Key& key);
bool store_exists(const Key& key);
bool store_expect_type(const Key& key, Type expected); // returns true if type matches (or key doesn't exist)
void store_set(const Key& key, ValueEntry&& entry);
void store_commit_update(const Key& key, int64_t old_sz);
int64_t entry_memory_estimate(const Key& key, const ValueEntry& e);
void store_del(const Key& key);
void store_apply_tombstone(const Key& key);
void store_bump_vclock(const Key& key);
std::vector<Key> store_keys();
void expire_sweep();

str serialize_entry(const Key& key, const ValueEntry& entry);
void openAOF();
void fetchAOF();
void appendAOF(strv raw_cmd);
void flushAOF();
void closeAOF();
void rewriteAOF();
bool check_maxmemory_or_oom(); // returns true if OK, false if over maxmemory

#endif
