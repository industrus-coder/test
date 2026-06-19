#include "merkle.h"
#include "crc32c.h"
#include <algorithm>
#include <cstring>

uint16_t get_slot(const Key& key) {
    return static_cast<uint16_t>(crc32c_64(key.data(), key.size()) % MERKLE_SLOTS);
}

static str serialize_for_hash(const ValueEntry& entry) {
    str out;
    switch (entry.type) {
        case Type::STRING:
            out = std::get<str>(entry.value);
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
            for (const auto& p : sorted) {
                out += std::to_string(p.first); out += ':'; out += p.second; out += '\0';
            }
            break;
        }
        case Type::LIST: {
            const auto& list = std::get<std::deque<str>>(entry.value);
            for (const auto& e : list) {
                out += e; out += '\0';
            }
            break;
        }
        case Type::STREAM: {
            const auto& stream = std::get<Stream>(entry.value);
            for (const auto& se : stream.entries) {
                out += se.id; out += '\0';
                std::vector<std::pair<str, str>> fields(se.fields.begin(), se.fields.end());
                std::sort(fields.begin(), fields.end());
                for (const auto& [k, v] : fields) {
                    out += k; out += '='; out += v; out += '\0';
                }
            }
            break;
        }
        case Type::TOMBSTONE:
            break;
    }
    std::vector<std::pair<uint64_t, counter>> vclock(entry.VecClk.begin(), entry.VecClk.end());
    std::sort(vclock.begin(), vclock.end());
    for (const auto& [id, cnt] : vclock) {
        out += std::to_string(id); out += ':'; out += std::to_string(cnt); out += ',';
    }
    return out;
}

MerkleTree compute_merkle_tree() {
    MerkleTree tree{};
    size_t leaf_base = MERKLE_SLOTS - 1;
    // Compute leaf hashes (indices leaf_base to leaf_base + MERKLE_SLOTS - 1)
    for (const auto& [key, entry] : STORE) {
        // Include ALL entries (including tombstones) in Merkle hashes
        // to prevent key resurrection via anti-entropy
        uint16_t slot = get_slot(key);
        str data = key;
        data += serialize_for_hash(entry);
        tree[leaf_base + slot] ^= crc32c_64(data.data(), data.size());
    }
    // Build internal nodes bottom-up (heap layout: root at 0, children at 2*i+1, 2*i+2)
    for (int64_t i = static_cast<int64_t>(MERKLE_SLOTS) - 2; i >= 0; i--) {
        tree[static_cast<size_t>(i)] = tree[static_cast<size_t>(2*i+1)] ^ tree[static_cast<size_t>(2*i+2)];
    }
    return tree;
}

str serialize_merkle_tree(const MerkleTree& tree, uint64_t version) {
    str payload;
    payload.reserve(sizeof(uint64_t) + MERKLE_TREE_NODES * sizeof(uint64_t));
    payload.append(reinterpret_cast<const char*>(&version), sizeof(version));
    payload.append(reinterpret_cast<const char*>(tree.data()), MERKLE_TREE_NODES * sizeof(uint64_t));
    return payload;
}

MerklePayload deserialize_merkle_tree(strv payload) {
    MerklePayload mp{};
    size_t needed = sizeof(uint64_t) + MERKLE_TREE_NODES * sizeof(uint64_t);
    if (payload.size() >= sizeof(uint64_t))
        memcpy(&mp.version, payload.data(), sizeof(uint64_t));
    size_t tree_bytes = std::min(payload.size() - sizeof(uint64_t), MERKLE_TREE_NODES * sizeof(uint64_t));
    if (tree_bytes > 0)
        memcpy(mp.tree.data(), payload.data() + sizeof(uint64_t), tree_bytes);
    return mp;
}

std::vector<uint16_t> find_differing_slots(
    const MerkleTree& local,
    const MerkleTree& remote)
{
    std::vector<uint16_t> diff;
    // Uses a stack for iteration to avoid recursion depth issues
    struct Frame { size_t idx; };
    std::vector<Frame> stack;
    stack.push_back({0}); // start at root (standard heap: root at 0)
    while (!stack.empty()) {
        Frame f = stack.back();
        stack.pop_back();
        if (local[f.idx] == remote[f.idx]) continue;
        if (f.idx < MERKLE_SLOTS - 1) {
            // Internal node — recurse on children
            stack.push_back({2*f.idx + 2}); // right child
            stack.push_back({2*f.idx + 1}); // left child
        } else {
            // Leaf node — differs
            diff.push_back(static_cast<uint16_t>(f.idx - (MERKLE_SLOTS - 1)));
        }
    }
    return diff;
}

str get_slot_entries_payload(uint16_t slot) {
    str payload;
    for (const auto& [key, entry] : STORE) {
        if (get_slot(key) != slot) continue;
        payload += serialize_entry(key, entry);
    }
    return payload;
}
