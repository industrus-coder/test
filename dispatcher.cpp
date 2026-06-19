#include "dispatcher.h"
#include "cluster.h"
#include "config.h"
#include "cmd.h"
#include "store.h"
#include "network.h"
#include <chrono>
#include <sys/epoll.h>

#include <cstdlib>

Dispatcher dispatcher;

static std::vector<str> extract_keys(const COMMAND& cmd) {
    switch (cmd.type) {
        case commandType::PING:
        case commandType::DBSIZE:
        case commandType::FLUSHDB:
        case commandType::INFO:
        case commandType::KEYS:
        case commandType::CLUSTER:
            return {};
        case commandType::MGET:
        case commandType::DEL:
        case commandType::EXISTS:
            return cmd.args;
        case commandType::MSET: {
            std::vector<str> keys;
            for (size_t i = 0; i < cmd.args.size(); i += 2)
                keys.push_back(cmd.args[i]);
            return keys;
        }
        default:
            if (cmd.args.empty()) return {};
            return {cmd.args[0]};
    }
}

static bool is_multi_key_command(commandType type) {
    return type == commandType::MGET || type == commandType::MSET ||
           type == commandType::DEL || type == commandType::EXISTS;
}

static str vclock_payload(const Key& key) {
    ValueEntry* entry = store_get(key);
    if (!entry || entry->VecClk.empty()) return {};
    TOKENS t;
    t.push_back("VCLOCK");
    t.push_back(key);
    t.push_back(std::to_string(entry->VecClk.size()));
    for (const auto& [nid, cnt] : entry->VecClk) {
        t.push_back(std::to_string(nid));
        t.push_back(std::to_string(cnt));
    }
    return RESP::array(t);
}

static bool queue_replica(const str& raw_cmd, const std::vector<NodeID>& replicas,
                          int client_fd = -1, const str& deferred_response = "",
                          const str& vclock_extra = "") {
    if (replicas.empty()) return false;
    std::vector<uint64_t> ids;
    for (const auto& r : replicas)
        if (r.id != self_node.id)
            ids.push_back(r.id);
    if (ids.empty()) return false;

    // Only defer if at least one peer is actually connected.
    // NOTE: When no replicas are reachable the write returns immediately to the
    // client with +OK, silently weakening write_quorum guarantees. This is
    // intentional for solo/dev use; for strict quorum a fail-fast policy could
    // be added as a config option.
    bool any_connected = false;
    for (auto id : ids) {
        auto pit = peers_by_node_id.find(id);
        if (pit != peers_by_node_id.end() && pit->second.fd >= 0) {
            any_connected = true;
            break;
        }
    }
    if (!any_connected) return false;

    str payload = raw_cmd + vclock_extra;
    replica_queue.push_back({std::move(payload), std::move(ids), client_fd, deferred_response});
    return true;
}

static void append_vclock_aof(const str& key) {
    ValueEntry* entry = store_get(key);
    if (!entry || entry->VecClk.empty()) return;
    TOKENS t;
    t.push_back("VCLOCK");
    t.push_back(key);
    t.push_back(std::to_string(entry->VecClk.size()));
    for (const auto& [nid, cnt] : entry->VecClk) {
        t.push_back(std::to_string(nid));
        t.push_back(std::to_string(cnt));
    }
    appendAOF(RESP::array(t));
}

// Split concatenated RESP responses into individual elements
// Handles Arrays (*count\r\n...), Bulk Strings ($-1\r\n or $len\r\n...\r\n),
// Integers (:num\r\n), Simple Strings (+OK\r\n)
std::vector<str> split_resp_responses(const str& data) {
    std::vector<str> result;
    size_t pos = 0;
    while (pos < data.size()) {
        if (data[pos] == '*') {
            // Array: skip header and extract individual elements
            size_t end = data.find("\r\n", pos);
            if (end == str::npos) break;
            int count = std::atoi(data.substr(pos + 1, end - pos - 1).c_str());
            pos = end + 2;
            for (int i = 0; i < count && pos < data.size(); i++) {
                if (data[pos] == '$') {
                    size_t e = data.find("\r\n", pos);
                    if (e == str::npos) break;
                    int len = std::atoi(data.substr(pos + 1, e - pos - 1).c_str());
                    if (len == -1) {
                        result.push_back(data.substr(pos, e + 2 - pos));
                        pos = e + 2;
                    } else {
                        size_t total = e + 2 + static_cast<size_t>(len) + 2;
                        if (total > data.size()) break;
                        result.push_back(data.substr(pos, total - pos));
                        pos = total;
                    }
                } else if (data[pos] == ':') {
                    size_t e = data.find("\r\n", pos);
                    if (e == str::npos) break;
                    result.push_back(data.substr(pos, e + 2 - pos));
                    pos = e + 2;
                } else if (data[pos] == '+') {
                    size_t e = data.find("\r\n", pos);
                    if (e == str::npos) break;
                    result.push_back(data.substr(pos, e + 2 - pos));
                    pos = e + 2;
                } else break;
            }
        } else if (data[pos] == '$') {
            size_t end = data.find("\r\n", pos);
            if (end == str::npos) break;
            int len = std::atoi(data.substr(pos + 1, end - pos - 1).c_str());
            if (len == -1) {
                result.push_back(data.substr(pos, end + 2 - pos));
                pos = end + 2;
            } else {
                size_t total = end + 2 + static_cast<size_t>(len) + 2;
                if (total > data.size()) break;
                result.push_back(data.substr(pos, total - pos));
                pos = total;
            }
        } else if (data[pos] == ':') {
            size_t end = data.find("\r\n", pos);
            if (end == str::npos) break;
            result.push_back(data.substr(pos, end + 2 - pos));
            pos = end + 2;
        } else if (data[pos] == '+') {
            size_t end = data.find("\r\n", pos);
            if (end == str::npos) break;
            result.push_back(data.substr(pos, end + 2 - pos));
            pos = end + 2;
        } else break;
    }
    return result;
}

static void dispatch_multi_key(Client& client, COMMAND& cmd, const std::vector<str>& keys) {
    // Group keys by owner
    std::unordered_map<uint64_t, std::vector<size_t>> owner_groups;
    for (size_t i = 0; i < keys.size(); i++) {
        NodeID owner = get_owner(keys[i]);
        owner_groups[owner.id].push_back(i);
    }

    bool has_local = owner_groups.count(self_node.id);
    int remote_targets = static_cast<int>(owner_groups.size()) - (has_local ? 1 : 0);

    // All keys on this node — execute directly
    if (remote_targets == 0) {
        str response = execute_command(cmd);
        if (is_write_command(cmd.type) && !cmd.from_replication) {
            for (auto& k : keys) append_vclock_aof(k);
            str vc;
            for (auto& k : keys) vc += vclock_payload(k);
            if (queue_replica(RESP::serialize_command(cmd), get_replicas(keys[0]),
                              client.fd, response, vc))
                return;
        }
        client.write_buf += response;
        return;
    }

    // For MGET: per-key result slots; for MSET/DEL/EXISTS: accumulated count
    std::vector<str> parts;
    if (cmd.type == commandType::MGET) parts.resize(keys.size());
    long long accumulated = 0;

    // Execute local keys
    if (has_local) {
        const auto& local_indices = owner_groups[self_node.id];
        if (cmd.type == commandType::MGET) {
            TOKENS local_args;
            for (auto idx : local_indices)
                local_args.push_back(cmd.args[idx]);
            COMMAND local_cmd = cmd;
            local_cmd.args = local_args;
            auto elements = split_resp_responses(execute_command(local_cmd));
            for (size_t ei = 0; ei < elements.size() && ei < local_indices.size(); ei++)
                parts[local_indices[ei]] = std::move(elements[ei]);
        } else if (cmd.type == commandType::MSET) {
            TOKENS local_args;
            for (auto idx : local_indices) {
                local_args.push_back(cmd.args[2 * idx]);
                local_args.push_back(cmd.args[2 * idx + 1]);
            }
            COMMAND local_cmd = cmd;
            local_cmd.args = local_args;
            execute_command(local_cmd);
        } else {
            TOKENS local_args;
            for (auto idx : local_indices)
                local_args.push_back(cmd.args[idx]);
            COMMAND local_cmd = cmd;
            local_cmd.args = local_args;
            str local_resp = execute_command(local_cmd);
            if (!local_resp.empty() && local_resp[0] == ':')
                accumulated += std::atol(local_resp.c_str() + 1);
        }
        // Persist vector clock in AOF for locally-executed keys
        if (is_write_command(cmd.type) && !cmd.from_replication) {
            for (auto idx : local_indices) {
                if (cmd.type == commandType::MSET)
                    append_vclock_aof(cmd.args[idx]);
                else
                    append_vclock_aof(cmd.args[idx]);
            }
        }
    }

    static uint64_t gather_id_counter = 1;
    uint64_t gather_id = gather_id_counter++;

    // Send COMMAND frames to each remote owner
    for (const auto& [oid, indices] : owner_groups) {
        if (oid == self_node.id) continue;

        TOKENS remote_args;
        if (cmd.type == commandType::MGET) {
            for (auto idx : indices)
                remote_args.push_back(cmd.args[idx]);
        } else if (cmd.type == commandType::MSET) {
            for (auto idx : indices) {
                remote_args.push_back(cmd.args[2 * idx]);
                remote_args.push_back(cmd.args[2 * idx + 1]);
            }
        } else {
            for (auto idx : indices)
                remote_args.push_back(cmd.args[idx]);
        }

        COMMAND remote_cmd = cmd;
        remote_cmd.args = remote_args;
        str remote_payload = RESP::serialize_command(remote_cmd);

        static uint64_t sub_id_counter = 1;
        uint64_t sub_msg_id = sub_id_counter++;

        BIN::Frame req;
        req.header = {
            .magic = BIN::MAGIC, .version = BIN::VERSION,
            .msg_type = static_cast<uint8_t>(BIN::FrameType::PROXY_REQUEST),
            .msg_id = sub_msg_id, .sender_id = self_node.id,
            .payload_len = static_cast<uint32_t>(remote_payload.size()), .checksum = 0
        };
        req.payload = std::move(remote_payload);
        auto serialized = BIN::serialize(req);

        auto pit = peers_by_node_id.find(oid);
        if (pit != peers_by_node_id.end() && pit->second.fd >= 0) {
            bool was_empty = pit->second.write_buf.empty();
            pit->second.write_buf += serialized;
            if (was_empty) {
                uint32_t flags = EPOLLIN | EPOLLOUT;
                mod_epoll(pit->second.fd, flags);
            }
        }

        gather_parent[sub_msg_id] = gather_id;
        gather_keys[sub_msg_id] = indices;
    }

    auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    pending_gathers[gather_id] = {
        client.fd, cmd.type, remote_targets, 0,
        std::move(parts), accumulated, now_ms
    };
}

void Dispatcher::dispatch(Client& client, COMMAND& cmd) {
    // Reject writes if over maxmemory
    if (is_write_command(cmd.type) && !check_maxmemory_or_oom()) {
        client.write_buf += RESP::error_oom();
        return;
    }

    auto keys = extract_keys(cmd);

    if (keys.empty()) {
        client.write_buf += execute_command(cmd);
        return;
    }

    if (ring.empty()) {
        client.write_buf += execute_command(cmd);
        return;
    }

    // Multi-key scatter-gather
    if (is_multi_key_command(cmd.type) && keys.size() > 1) {
        dispatch_multi_key(client, cmd, keys);
        return;
    }

    NodeID owner = get_owner(keys[0]);
    if (owner == self_node) {
        auto response = execute_command(cmd);
        if (is_write_command(cmd.type) && !cmd.from_replication) {
            // Persist vector clock in AOF
            if (cmd.type == commandType::MSET) {
                for (size_t i = 0; i < cmd.args.size(); i += 2)
                    append_vclock_aof(cmd.args[i]);
            } else {
                for (auto& k : keys)
                    append_vclock_aof(k);
            }
            str vc;
            if (cmd.type == commandType::MSET) {
                for (size_t i = 0; i < cmd.args.size(); i += 2)
                    vc += vclock_payload(cmd.args[i]);
            } else {
                for (auto& k : keys)
                    vc += vclock_payload(k);
            }
            if (queue_replica(RESP::serialize_command(cmd), get_replicas(keys[0]),
                              client.fd, response, vc))
                return;
        }
        client.write_buf += response;
    } else {
        NodeID target = owner;
        // For read commands, if owner is SUSPECT/DEAD, try an ALIVE replica
        if (!is_write_command(cmd.type)) {
            auto it = cluster_state.find(owner);
            if (it == cluster_state.end() || it->second != NodeStatus::ALIVE) {
                auto replicas = get_replicas(keys[0]);
                for (const auto& r : replicas) {
                    if (r.id == self_node.id) {
                        auto local_response = execute_command(cmd);
                        std::vector<uint64_t> other_replicas;
                        for (const auto& rep : replicas) {
                            if (rep.id == self_node.id) continue;
                            auto sit2 = cluster_state.find(rep);
                            if (sit2 != cluster_state.end() && sit2->second == NodeStatus::ALIVE)
                                other_replicas.push_back(rep.id);
                        }
                        if (other_replicas.empty()) {
                            client.write_buf += local_response;
                            return;
                        }
                        std::unordered_map<uint64_t, counter> best_vclock;
                        auto* entry = store_get(keys[0]);
                        if (entry) best_vclock = entry->VecClk;
                        static uint64_t read_msg_id = 1;
                        uint64_t msg_id = read_msg_id++;
                        str request_payload = RESP::serialize_command(cmd);
                        for (auto rep_id : other_replicas) {
                            auto pit = peers_by_node_id.find(rep_id);
                            if (pit == peers_by_node_id.end() || pit->second.fd < 0) continue;
                            BIN::Frame req;
                            req.header = {
                                .magic = BIN::MAGIC, .version = BIN::VERSION,
                                .msg_type = static_cast<uint8_t>(BIN::FrameType::READ_REQUEST),
                                .msg_id = msg_id, .sender_id = self_node.id,
                                .payload_len = static_cast<uint32_t>(request_payload.size()), .checksum = 0
                            };
                            req.payload = request_payload;
                            auto serialized = BIN::serialize(req);
                            bool was_empty = pit->second.write_buf.empty();
                            pit->second.write_buf += serialized;
                            if (was_empty) {
                                uint32_t flags = EPOLLIN | EPOLLOUT;
                                mod_epoll(pit->second.fd, flags);
                            }
                        }
                        auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::system_clock::now().time_since_epoch()).count();
                        int remote_needed = std::max(config().read_quorum - 1, 1);
                        if (remote_needed > static_cast<int>(other_replicas.size()))
                            remote_needed = static_cast<int>(other_replicas.size());
                        pending_reads[msg_id] = {
                            local_response, best_vclock, client.fd,
                            remote_needed, 0, now_ms,
                            keys[0], best_vclock
                        };
                        return;
                    }
                    auto sit = cluster_state.find(r);
                    if (sit != cluster_state.end() && sit->second == NodeStatus::ALIVE) {
                        target = r;
                        break;
                    }
                }
            }
        }
        uint64_t token = hash_token(keys[0]);
        uint16_t slot = static_cast<uint16_t>(token % 16384);
        client.write_buf += RESP::error_moved(slot, target.ip, target.port);
    }
}
