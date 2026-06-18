#ifndef DREDIS_CLUSTER_H
#define DREDIS_CLUSTER_H
#pragma once

#include <string>
#include <string_view>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <map>
#include <cstdint>

enum class NodeStatus {
    ALIVE,
    SUSPECT,
    DEAD,
    LEFT
};

struct NodeID {
    uint64_t id;
    std::string ip;
    uint16_t port;
    uint64_t generation = 0;

    bool operator==(const NodeID &o) const {
        return id == o.id;
    }
};

namespace std {
    template<>
    struct hash<NodeID> {
        size_t operator()(const NodeID &n) const noexcept {
            return std::hash<uint64_t>{}(n.id);
        }
    };
}

extern NodeID self_node;
extern std::unordered_map<NodeID, NodeStatus> cluster_state;
extern std::map<uint64_t, NodeID> ring;
extern std::unordered_map<NodeID, int64_t> last_seen;
extern std::unordered_map<uint64_t, uint64_t> node_versions; // node_id -> version
extern uint64_t self_version;
extern uint64_t ring_version;

void init_cluster(NodeID self, const std::vector<NodeID> &seeds);

uint64_t hash_token(const std::string &key);

void add_node(const NodeID &node);

void remove_node(const NodeID &node); // For graceful leave
void resolve_node_address(const std::string& ip, uint16_t port, uint64_t real_id); // Replace temp-ID entries with real ID

void mark_dead(const NodeID &node);

void mark_suspect(const NodeID &node);

// This should be called whenever a heartbeat or any message is received from a node.
void update_last_seen(const NodeID &node);

// This function should be called periodically to handle node timeouts.
// connected_peers: optional set of node IDs with healthy TCP sockets.
// When provided, DEAD marking is gated on socket state (defense-in-depth).
void check_timeouts(const std::unordered_set<uint64_t>* connected_peers = nullptr);

NodeID get_owner(const std::string &key);

std::vector<NodeID> get_replicas(const std::string &key);

std::vector<NodeID> get_all_nodes();

uint64_t get_ring_version();

struct GossipNode {
    uint64_t id;
    std::string ip;
    uint16_t port;
    uint64_t generation;
    uint64_t version;
    NodeStatus status;
};

std::string encode_gossip_payload();
void apply_gossip_payload(std::string_view payload);

#endif