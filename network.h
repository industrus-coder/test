#ifndef DREDIS_NETWORK_H
#define DREDIS_NETWORK_H

#include <unordered_map>
#include <deque>
#include <cstdint>
#include <vector>
#include <csignal>

#include "parser.h"
#include "cluster.h"

enum class ConnType {
    UNKNOWN,
    CLIENT_RESP,
    NODE_BIN
};

struct Client {
    int fd;
    str write_buf;
    RESP::Parser parser;
    bool read_paused_by_backpressure = false;
    int64_t last_active_ms = 0;
    bool idle_timed_out = false;
    ConnType type = ConnType::UNKNOWN;
};

struct PeerConnection {
    int fd = -1;
    str read_buf;
    str write_buf;
    RESP::Parser parser;
    int64_t retry_at = 0;
    int retry_count = 0;
    bool needs_full_sync = true;
};

struct ReplicaOp {
    str raw_command;
    std::vector<uint64_t> target_ids;
    int client_fd = -1;
    str deferred_response = "";
};

struct PendingWrite {
    str response;
    int client_fd;
    int ack_count;
    int target_count;
    int64_t created_at;
};

struct PendingRead {
    str best_response;
    std::unordered_map<uint64_t, uint64_t> best_vclock;
    int client_fd;
    int expected_count;
    int response_count;
    int64_t created_at;
    str key;
    std::unordered_map<uint64_t, uint64_t> local_vclock;
};

struct PendingGather {
    int client_fd;
    commandType cmd_type;
    int expected;
    int received;
    std::vector<str> parts;   // MGET: per-key result RESP strings
    long long accumulated;    // MSET/DEL/EXISTS: accumulated count
    int64_t created_at;
};

inline int server_fd;
inline int epoll_fd;
inline std::unordered_map<int, Client> clients;
inline std::unordered_map<uint64_t, PeerConnection> peers_by_node_id;
inline std::unordered_map<int, uint64_t> fd_to_peer_id;
inline std::deque<ReplicaOp> replica_queue;
inline std::unordered_map<uint64_t, PendingWrite> pending_writes;
inline std::unordered_map<uint64_t, PendingRead> pending_reads;
inline std::unordered_map<uint64_t, PendingGather> pending_gathers;
inline std::unordered_map<uint64_t, uint64_t> gather_parent;         // sub_msg_id -> gather_id
inline std::unordered_map<uint64_t, std::vector<size_t>> gather_keys; // sub_msg_id -> key indices
inline volatile sig_atomic_t g_shutdown_requested = 0;

void init_server(str port);
void mod_epoll(int fd, uint32_t events);
void close_client(int fd);
void accept_new_clients();
bool handle_read(Client& c);
bool handle_write(Client& c);
[[noreturn]] void run_loop();

void connect_to_peers();
void set_seed_addresses(const std::vector<NodeID>& seeds);
PeerConnection* get_or_connect(uint64_t node_id, const str& ip, uint16_t port);
void handle_peer_read(PeerConnection& peer);
void handle_peer_write(PeerConnection& peer);
void process_bin_frame(const BIN::Frame& frame, uint64_t peer_node_id);
void send_heartbeats();
void flush_replica_queue();
void reconnect_peers();
void run_background_tasks();

// Used by both network.cpp and dispatcher.cpp
std::vector<str> split_resp_responses(const str& data);

#endif
