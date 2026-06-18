#include <iostream>
#include <cstdlib>
#include <csignal>
#include <fstream>
#include <random>
#include "network.h"
#include "cluster.h"
#include "cmd.h"
#include "store.h"
#include "config.h"
#include "benchmarks.h"

static std::vector<NodeID> parse_seeds() {
    std::vector<NodeID> seeds;
    for (const auto &s: config().seeds) {
        uint64_t id = std::hash<std::string>{}(s.ip + ":" + std::to_string(s.port));
        seeds.push_back({id, s.ip, s.port, 0});
    }
    return seeds;
}

extern "C" void shutdown_handler(int) {
    g_shutdown_requested = 1;
}

static void setup_node_identity(uint64_t &node_id, uint64_t &generation) {
    // Try reading node.id file for persistent identity + generation
    std::ifstream id_file("node.id");
    if (id_file.is_open()) {
        std::string line;
        if (std::getline(id_file, line)) node_id = std::stoull(line);
        if (std::getline(id_file, line)) generation = std::stoull(line) + 1;
        id_file.close();
    }

    if (!config().node_id_str.empty()) node_id = std::stoull(config().node_id_str);
    if (node_id == 0) {
        std::mt19937_64 rng(std::random_device{}());
        node_id = rng();
    }

    // Persist node.id with generation
    std::ofstream of("node.id");
    if (of.is_open()) {
        of << node_id << "\n" << generation << "\n";
    }
}

static void init_cluster_node(uint64_t node_id, uint64_t generation) {
    std::string ip = config().ip;

    auto seeds = parse_seeds();
    NodeID self{node_id, ip, config().client_port, generation};
    init_cluster(self, seeds);
    set_seed_addresses(seeds);
    connect_to_peers();

    std::cout << "\033[32m[DRedis] running on port " << config().client_port
            << " cluster port " << config().effective_cluster_port()
            << " node_id " << node_id << "\033[0m" << std::endl;
}

static void configure_system() {
    g_maxmemory_bytes = config().maxmemory_bytes;
    if (config().aof_fsync == "always") g_aof_fsync = AOFFsync::ALWAYS;
    else if (config().aof_fsync == "no") g_aof_fsync = AOFFsync::NO;
    else g_aof_fsync = AOFFsync::EVERYSEC;

    std::string port = std::to_string(config().client_port);
    init_server(port.c_str());
    openAOF();
    fetchAOF();
}

static void setup_signal_handlers() {
    struct sigaction sa{};
    sa.sa_handler = shutdown_handler;
    sa.sa_flags = 0;
    sigemptyset(&sa.sa_mask);
    sigaction(SIGTERM, &sa, nullptr);
    sigaction(SIGINT, &sa, nullptr);
}

int main() {
    init_cmd_map();
    load_config();
    configure_system();

    uint64_t node_id = 0, generation = 0;
    setup_node_identity(node_id, generation);

    // Uncomment to run benchmarks
    // run_benchmarks();

    init_cluster_node(node_id, generation);
    setup_signal_handlers();

    run_loop();
}
