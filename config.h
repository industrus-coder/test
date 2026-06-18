#pragma once
#ifndef DREDIS_CONFIG_H
#define DREDIS_CONFIG_H

#include <string>
#include <cstdint>
#include <vector>
#include <unordered_map>

struct SeedNode {
    std::string ip;
    uint16_t port;
};

struct DRedisConfig {
    // Node identity
    std::string node_id_str = "";
    std::string ip = "127.0.0.1";
    uint16_t client_port = 6380;
    uint16_t cluster_port = 0; // 0 = auto: client_port + 10000

    // Cluster
    int replication_factor = 3;
    int read_quorum = 2;
    int write_quorum = 2;

    // Timing (ms)
    int gossip_interval_ms = 200;
    int failure_timeout_ms = 2000;
    int dead_timeout_multiplier = 3;
    int tombstone_ttl_ms = 60000;
    int merkle_interval_ms = 30000;

    // Persistence
    std::string aof_fsync = "everysec";
    int64_t maxmemory_bytes = 256LL * 1024 * 1024;

    // Seeds
    std::vector<SeedNode> seeds;

    // Derived
    uint16_t effective_cluster_port() const {
        return cluster_port != 0 ? cluster_port : (client_port + 10000);
    }

    bool is_valid() const {
        return write_quorum + read_quorum > replication_factor;
    }
};

DRedisConfig load_config(const std::string& path = "dredis.conf");
DRedisConfig& config();

#endif
