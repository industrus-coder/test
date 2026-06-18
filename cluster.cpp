#include "cluster.h"
#include "config.h"
#include "crc32c.h"
#include <iostream>
#include <unordered_set>
#include <chrono>
#include <algorithm>
#include <random>
#include <cstring>
#include <arpa/inet.h>

const int VNODE_COUNT = 150;
const int GOSSIP_FANOUT = 3;

NodeID self_node;
std::unordered_map<NodeID, NodeStatus> cluster_state;
std::map<uint64_t, NodeID> ring;
std::unordered_map<NodeID, int64_t> last_seen;
std::unordered_map<uint64_t, uint64_t> node_versions;
uint64_t self_version = 0;
uint64_t ring_version = 0;
size_t active_node_count = 0;

static void build_ring() {
    ring.clear();
    active_node_count = 0;
    for (const auto &[node, status]: cluster_state) {
        if (status == NodeStatus::ALIVE) {
            active_node_count++;
            for (int i = 0; i < VNODE_COUNT; ++i) {
                std::string vnode_key = std::to_string(node.id) + "#" + std::to_string(i);
                uint64_t token = hash_token(vnode_key);
                ring[token] = node;
            }
        }
    }
    ring_version++;
}

void update_last_seen(const NodeID &node) {
    auto it = cluster_state.find(node);
    if (it != cluster_state.end() && it->second == NodeStatus::SUSPECT) {
        it->second = NodeStatus::ALIVE;
        node_versions[node.id] = ++self_version;
    }
    last_seen[node] = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
}

void init_cluster(NodeID self, const std::vector<NodeID> &) {
    self_node = self;
    cluster_state[self_node] = NodeStatus::ALIVE;
    update_last_seen(self_node);
    build_ring();
}

uint64_t hash_token(const std::string &key) {
    return crc32c_64(key.data(), key.size());
}

void add_node(const NodeID &node) {
    auto it = cluster_state.find(node);
    if (it != cluster_state.end() && it->second != NodeStatus::DEAD && it->second != NodeStatus::LEFT) {
        return;
    }
    cluster_state[node] = NodeStatus::ALIVE;
    update_last_seen(node);
    node_versions[node.id] = ++self_version;
    build_ring();
}

void resolve_node_address(const std::string& ip, uint16_t port, uint64_t real_id) {
    for (auto it = cluster_state.begin(); it != cluster_state.end(); ) {
        if (it->first.id != real_id && it->first.id != self_node.id &&
            it->first.ip == ip && it->first.port == port) {
            it = cluster_state.erase(it);
        } else {
            ++it;
        }
    }
    node_versions[self_node.id] = ++self_version;
    build_ring();
}

void remove_node(const NodeID &node) {
    auto it = cluster_state.find(node);
    if (it == cluster_state.end() || it->second == NodeStatus::LEFT) {
        return;
    }
    it->second = NodeStatus::LEFT;
    last_seen.erase(node);
    node_versions[node.id] = ++self_version;
    build_ring();
}

void mark_dead(const NodeID &node) {
    auto it = cluster_state.find(node);
    if (it == cluster_state.end() || it->second == NodeStatus::DEAD) {
        return;
    }
    it->second = NodeStatus::DEAD;
    last_seen.erase(node);
    node_versions[node.id] = ++self_version;
}

void mark_suspect(const NodeID &node) {
    auto it = cluster_state.find(node);
    if (it == cluster_state.end() || it->second != NodeStatus::ALIVE) {
        return;
    }
    it->second = NodeStatus::SUSPECT;
    node_versions[node.id] = ++self_version;
}

void check_timeouts(const std::unordered_set<uint64_t>* connected_peers) {
    int64_t now = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();

    std::vector<NodeID> nodes_to_mark_suspect;
    std::vector<NodeID> nodes_to_mark_dead;

    for (auto const& [node, last_ts] : last_seen) {
        if (node.id == self_node.id) continue;
        auto it = cluster_state.find(node);
        if (it == cluster_state.end()) continue;

        if (it->second == NodeStatus::ALIVE && (now - last_ts > config().failure_timeout_ms)) {
            nodes_to_mark_suspect.push_back(node);
        } else if (it->second == NodeStatus::SUSPECT && (now - last_ts > static_cast<int64_t>(config().failure_timeout_ms) * config().dead_timeout_multiplier)) {
            // Defense-in-depth: only mark DEAD if the peer socket is actually gone.
            // A healthy TCP socket means the node is still reachable even if
            // heartbeats were delayed by scheduling jitter.
            if (!connected_peers || !connected_peers->count(node.id)) {
                nodes_to_mark_dead.push_back(node);
            } else {
                // Socket alive but heartbeats stalled — reset timer to prevent
                // repeated SUSPECT→DEAD checks from dominating the tick.
                last_seen[node] = now;
            }
        }
    }

    bool ring_dirty = false;
    for (const auto& node : nodes_to_mark_suspect) {
        std::cout << "[Cluster] Node " << node.id << " marked SUSPECT" << std::endl;
        mark_suspect(node);
    }
    for (const auto& node : nodes_to_mark_dead) {
        std::cout << "[Cluster] Node " << node.id << " marked DEAD" << std::endl;
        mark_dead(node);
        ring_dirty = true;
    }
    if (ring_dirty) {
        build_ring();
    }
}

NodeID get_owner(const std::string &key) {
    if (ring.empty()) return self_node;
    uint64_t token = hash_token(key);
    auto it = ring.lower_bound(token);
    if (it == ring.end()) {
        it = ring.begin();
    }
    return it->second;
}

std::vector<NodeID> get_replicas(const std::string &key) {
    std::vector<NodeID> res;
    if (ring.empty()) return res;

    std::unordered_set<uint64_t> found_ids;
    uint64_t token = hash_token(key);
    auto it = ring.lower_bound(token);

    const size_t target_count = std::min((size_t)config().replication_factor, active_node_count);
    if (target_count == 0) return res;

    auto start_it = it;
    bool wrapped = false;

    while (res.size() < target_count) {
        if (it == ring.end()) {
            it = ring.begin();
            wrapped = true;
        }

        if (wrapped && it == start_it) {
            break;
        }

        const NodeID &node = it->second;
        if (found_ids.find(node.id) == found_ids.end()) {
            res.push_back(node);
            found_ids.insert(node.id);
        }
        it++;
    }

    return res;
}

std::vector<NodeID> get_all_nodes() {
    std::vector<NodeID> res;
    for (const auto &[node, status]: cluster_state) {
        if (status == NodeStatus::ALIVE || status == NodeStatus::SUSPECT) {
            res.push_back(node);
        }
    }
    return res;
}

uint64_t get_ring_version() {
    return ring_version;
}

std::string encode_gossip_payload() {
    std::vector<NodeID> candidates;
    // Always include self_node so peers know our latest status
    candidates.push_back(self_node);
    for (const auto& [node, status] : cluster_state) {
        if (node.id != self_node.id)
            candidates.push_back(node);
    }
    if (candidates.empty()) return {};

    // Shuffle and pick up to 4 (3 peers + self)
    static std::mt19937 rng(std::random_device{}());
    std::shuffle(candidates.begin() + 1, candidates.end(), rng);
    size_t count = std::min(candidates.size(), size_t(GOSSIP_FANOUT + 1)); // +1 for self

    std::string payload;
    payload.append(reinterpret_cast<const char*>(&count), sizeof(uint32_t));
    for (size_t i = 0; i < count; i++) {
        const auto& node = candidates[i];
        auto it = cluster_state.find(node);
        uint8_t status_byte = it != cluster_state.end() ? static_cast<uint8_t>(it->second) : 0;
        uint8_t ip_len = static_cast<uint8_t>(node.ip.size());
        uint64_t gen = node.generation;
        auto vit = node_versions.find(node.id);
        uint64_t ver = vit != node_versions.end() ? vit->second : 0;

        payload.append(reinterpret_cast<const char*>(&node.id), sizeof(uint64_t));
        payload.push_back(ip_len);
        payload.append(node.ip);
        uint16_t net_port = htons(node.port);
        payload.append(reinterpret_cast<const char*>(&net_port), sizeof(uint16_t));
        payload.push_back(status_byte);
        payload.append(reinterpret_cast<const char*>(&gen), sizeof(uint64_t));
        payload.append(reinterpret_cast<const char*>(&ver), sizeof(uint64_t));
    }
    return payload;
}

void apply_gossip_payload(std::string_view payload) {
    if (payload.size() < sizeof(uint32_t)) return;
    uint32_t count;
    memcpy(&count, payload.data(), sizeof(uint32_t));
    size_t offset = sizeof(uint32_t);

    for (uint32_t i = 0; i < count; i++) {
        if (offset + sizeof(uint64_t) + 1 > payload.size()) break;

        uint64_t id;
        memcpy(&id, payload.data() + offset, sizeof(uint64_t));
        offset += sizeof(uint64_t);

        uint8_t ip_len = static_cast<uint8_t>(payload[offset]); offset++;
        if (offset + ip_len + sizeof(uint16_t) + 1 > payload.size()) break;

        std::string ip(payload.data() + offset, ip_len);
        offset += ip_len;

        uint16_t net_port;
        memcpy(&net_port, payload.data() + offset, sizeof(uint16_t));
        offset += sizeof(uint16_t);
        uint16_t port = ntohs(net_port);

        uint8_t status_byte = static_cast<uint8_t>(payload[offset]); offset++;

        // Read generation and version (backwards-compatible: default 0 if missing)
        uint64_t gossip_gen = 0, gossip_ver = 0;
        if (offset + sizeof(uint64_t) <= payload.size()) {
            memcpy(&gossip_gen, payload.data() + offset, sizeof(uint64_t));
            offset += sizeof(uint64_t);
        }
        if (offset + sizeof(uint64_t) <= payload.size()) {
            memcpy(&gossip_ver, payload.data() + offset, sizeof(uint64_t));
            offset += sizeof(uint64_t);
        }

        if (id == self_node.id) continue;

        NodeID gossip_node{id, ip, port, gossip_gen};
        auto it = cluster_state.find(gossip_node);
        auto gossip_status = static_cast<NodeStatus>(status_byte);

        // Version-based merge: only apply if incoming version > local known version
        auto vit = node_versions.find(id);
        uint64_t local_ver = vit != node_versions.end() ? vit->second : 0;
        if (gossip_ver <= local_ver) continue;

        if (it == cluster_state.end()) {
            // Clean up any temp-ID entry for this address
            resolve_node_address(ip, port, id);
            // Unknown node — add if ALIVE or SUSPECT
            if (gossip_status == NodeStatus::ALIVE || gossip_status == NodeStatus::SUSPECT) {
                add_node(gossip_node);
                node_versions[id] = gossip_ver;
            }
        } else if (gossip_gen > it->first.generation) {
            // Higher generation wins unconditionally
            cluster_state[gossip_node] = gossip_status;
            if (gossip_status != NodeStatus::DEAD && gossip_status != NodeStatus::LEFT)
                update_last_seen(gossip_node);
            node_versions[id] = gossip_ver;
        } else if (it->second == NodeStatus::ALIVE) {
            if (gossip_status == NodeStatus::SUSPECT) {
                mark_suspect(gossip_node);
                update_last_seen(gossip_node);
            } else if (gossip_status == NodeStatus::DEAD) {
                mark_dead(gossip_node);
            } else if (gossip_status == NodeStatus::LEFT) {
                remove_node(gossip_node);
            } else if (gossip_status == NodeStatus::ALIVE) {
                update_last_seen(gossip_node);
            }
            node_versions[id] = gossip_ver;
        } else if (it->second == NodeStatus::SUSPECT) {
            if (gossip_status == NodeStatus::DEAD) {
                mark_dead(gossip_node);
            } else if (gossip_status == NodeStatus::LEFT) {
                remove_node(gossip_node);
            } else if (gossip_status == NodeStatus::ALIVE) {
                update_last_seen(gossip_node);
            }
            node_versions[id] = gossip_ver;
        } else if (it->second == NodeStatus::DEAD && gossip_status == NodeStatus::LEFT) {
            remove_node(gossip_node);
            node_versions[id] = gossip_ver;
        }
    }
}