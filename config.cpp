#include "config.h"
#include <fstream>
#include <iostream>
#include <sstream>
#include <cstdlib>
#include <algorithm>

static DRedisConfig g_config;

static std::string trim(const std::string& s) {
    size_t start = s.find_first_not_of(" \t\r\n");
    if (start == std::string::npos) return "";
    size_t end = s.find_last_not_of(" \t\r\n");
    return s.substr(start, end - start + 1);
}

static int64_t parse_size(const std::string& s) {
    if (s.empty()) return 0;
    int64_t val = 0;
    size_t pos = 0;
    try {
        val = std::stoll(s, &pos);
    } catch (...) { return 0; }
    std::string unit = trim(s.substr(pos));
    std::transform(unit.begin(), unit.end(), unit.begin(), ::tolower);
    if (unit == "kb" || unit == "k") return val * 1024;
    if (unit == "mb" || unit == "m") return val * 1024 * 1024;
    if (unit == "gb" || unit == "g") return val * 1024 * 1024 * 1024;
    return val;
}

DRedisConfig load_config(const std::string& path) {
    DRedisConfig cfg;

    // Apply env var overrides first
    if (auto* env = std::getenv("DREDIS_PORT")) {
        try { cfg.client_port = static_cast<uint16_t>(std::stoi(env)); }
        catch (...) {}
    }
    if (auto* env = std::getenv("DREDIS_NODE_ID")) cfg.node_id_str = env;
    if (auto* env = std::getenv("DREDIS_IP")) cfg.ip = env;
    if (auto* env = std::getenv("DREDIS_SEEDS")) {
        std::string raw(env);
        size_t pos = 0;
        while (pos < raw.size()) {
            size_t comma = raw.find(',', pos);
            std::string token = raw.substr(pos, comma == std::string::npos ? std::string::npos : comma - pos);
            size_t colon = token.find(':');
            if (colon != std::string::npos) {
                SeedNode sn;
                sn.ip = token.substr(0, colon);
                try { sn.port = static_cast<uint16_t>(std::stoi(token.substr(colon + 1))); }
                catch (...) {}
                cfg.seeds.push_back(sn);
            }
            if (comma == std::string::npos) break;
            pos = comma + 1;
        }
    }
    if (auto* env = std::getenv("DREDIS_REPL_FACTOR")) {
        try { cfg.replication_factor = std::stoi(env); } catch (...) {}
    }
    if (auto* env = std::getenv("DREDIS_WRITE_QUORUM")) {
        try { cfg.write_quorum = std::stoi(env); } catch (...) {}
    }
    if (auto* env = std::getenv("DREDIS_READ_QUORUM")) {
        try { cfg.read_quorum = std::stoi(env); } catch (...) {}
    }

    // Parse config file (overrides env defaults)
    std::ifstream file(path);
    if (!file.is_open()) {
        if (!cfg.is_valid()) {
            std::cerr << "FATAL: W+R > N invariant violated (W=" << cfg.write_quorum
                      << ", R=" << cfg.read_quorum << ", N=" << cfg.replication_factor << ")" << std::endl;
            std::_Exit(1);
        }
        std::cout << "[Config] Using default config (no " << path << " found)" << std::endl;
        g_config = cfg;
        return cfg;
    }

    std::string line;
    while (std::getline(file, line)) {
        line = trim(line);
        if (line.empty() || line[0] == '#') continue;

        std::istringstream iss(line);
        std::string key;
        iss >> key;
        if (key.empty()) continue;

        if (key == "node_id") {
            iss >> cfg.node_id_str;
        } else if (key == "ip") {
            iss >> cfg.ip;
        } else if (key == "client_port") {
            int p; iss >> p; cfg.client_port = static_cast<uint16_t>(p);
        } else if (key == "cluster_port") {
            int p; iss >> p; cfg.cluster_port = static_cast<uint16_t>(p);
        } else if (key == "replication_factor") {
            iss >> cfg.replication_factor;
        } else if (key == "read_quorum") {
            iss >> cfg.read_quorum;
        } else if (key == "write_quorum") {
            iss >> cfg.write_quorum;
        } else if (key == "gossip_interval_ms") {
            iss >> cfg.gossip_interval_ms;
        } else if (key == "failure_timeout_ms") {
            iss >> cfg.failure_timeout_ms;
        } else if (key == "dead_timeout_multiplier") {
            iss >> cfg.dead_timeout_multiplier;
        } else if (key == "tombstone_ttl_ms") {
            iss >> cfg.tombstone_ttl_ms;
        } else if (key == "merkle_interval_ms") {
            iss >> cfg.merkle_interval_ms;
        } else if (key == "aof_fsync") {
            iss >> cfg.aof_fsync;
        } else if (key == "maxmemory") {
            std::string val; iss >> val;
            cfg.maxmemory_bytes = parse_size(val);
        } else if (key == "seed") {
            SeedNode sn;
            std::string addr; iss >> addr;
            size_t colon = addr.find(':');
            if (colon != std::string::npos) {
                sn.ip = addr.substr(0, colon);
                try { sn.port = static_cast<uint16_t>(std::stoi(addr.substr(colon + 1))); }
                catch (...) {}
                cfg.seeds.push_back(sn);
            }
        }
    }

    if (!cfg.is_valid()) {
        std::cerr << "FATAL: W+R > N invariant violated (W=" << cfg.write_quorum
                  << ", R=" << cfg.read_quorum << ", N=" << cfg.replication_factor << ")" << std::endl;
        std::_Exit(1);
    }

    g_config = cfg;
    return cfg;
}

DRedisConfig& config() {
    return g_config;
}
