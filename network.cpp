#include "network.h"

#include <iostream>
#include <algorithm>
#include <chrono>
#include <fcntl.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <sys/epoll.h>
#include <unistd.h>

#include <cstring>
#include <cstdlib>
#include <unistd.h>
#include "cmd.h"
#include "dispatcher.h"
#include "cluster.h"
#include "config.h"
#include "store.h"
#include "merkle.h"

constexpr size_t WRITE_BUF_BACKPRESSURE_THRESHOLD = 1024 * 1024;
constexpr int CLIENT_IDLE_TIMEOUT_MS = 300000; // 5 minutes
constexpr int MAX_CLIENTS = 10000;

static void set_tcp_keepalive(int fd) {
    int opt = 1;
    setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &opt, sizeof(opt));
    int idle = 60;
    setsockopt(fd, IPPROTO_TCP, TCP_KEEPIDLE, &idle, sizeof(idle));
    int interval = 10;
    setsockopt(fd, IPPROTO_TCP, TCP_KEEPINTVL, &interval, sizeof(interval));
    int cnt = 5;
    setsockopt(fd, IPPROTO_TCP, TCP_KEEPCNT, &cnt, sizeof(cnt));
}

static int64_t current_time_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()
    ).count();
}

void init_server(str port) {
    server_fd = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    set_tcp_keepalive(server_fd);
    fcntl(server_fd, F_SETFL, fcntl(server_fd, F_GETFL) | O_NONBLOCK);
    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(std::stoi(port));
    addr.sin_addr.s_addr = INADDR_ANY;
    auto BIND = bind(server_fd, reinterpret_cast<struct sockaddr *>(&addr), sizeof(addr));
    if (BIND < 0) {
        std::perror("Bind failed");
        exit(1);
    }
    listen(server_fd, 10);

    epoll_fd = epoll_create1(0);
    if (epoll_fd == -1) {
        std::cerr << "Failed to create epoll instance" << std::endl;
        exit(1);
    }
    struct epoll_event ev{};
    ev.events = EPOLLIN;
    ev.data.fd = server_fd;
    epoll_ctl(epoll_fd, EPOLL_CTL_ADD, server_fd, &ev);
}

void mod_epoll(int fd, uint32_t events) {
    struct epoll_event ev{};
    ev.events = events;
    ev.data.fd = fd;
    epoll_ctl(epoll_fd, EPOLL_CTL_MOD, fd, &ev);
}

void close_client(int fd) {
    epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, nullptr);
    std::cout << "[DREDIS]: Closing Client-> fd: " << fd << std::endl;
    close(fd);
    clients.erase(fd);
}

void accept_new_clients() {
    while (true) {
        int client_fd = accept(server_fd, nullptr, nullptr);

        if (client_fd == -1) {
            if (errno == EAGAIN || errno == EWOULDBLOCK)
                break;
            std::perror("accept failed");
            continue;
        }

        if (clients.size() >= MAX_CLIENTS) {
            close(client_fd);
            continue;
        }

        set_tcp_keepalive(client_fd);
        fcntl(client_fd, F_SETFL, fcntl(client_fd, F_GETFL) | O_NONBLOCK);
        clients[client_fd] = {client_fd, {}, {}, false, current_time_ms(), false};
        std::cout << "\033[33m[DRedis] client connected (fd: " << client_fd << ")\033[0m" << std::endl;
        struct epoll_event ev{};
        ev.events = EPOLLIN;
        ev.data.fd = client_fd;
        epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_fd, &ev);
    }
}

bool handle_read(Client &c) {
    char buf[4096];


    ssize_t n = read(c.fd, buf, sizeof(buf));
    if (n <= 0) {
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
            return true;
        close_client(c.fd);
        return false;
    }

    c.last_active_ms = current_time_ms();
    c.parser.feed(strv(buf, n));

    if (c.parser.buffer.size() >= 4) {
        uint32_t magic;
        memcpy(&magic, c.parser.buffer.data(), sizeof(magic));
        if (magic == BIN::MAGIC) {
            BIN::Frame frame;
            if (BIN::parse(c.parser.buffer, frame)) {
                uint64_t sender_id = frame.header.sender_id;

                auto existing = peers_by_node_id.find(sender_id);
                if (existing != peers_by_node_id.end() && existing->second.fd >= 0) {
                    epoll_ctl(epoll_fd, EPOLL_CTL_DEL, existing->second.fd, nullptr);
                    close(existing->second.fd);
                    peers_by_node_id.erase(sender_id);
                }

                std::cout << "\033[33m[DRedis] peer connection promoted (fd: " << c.fd << ")\033[0m" << std::endl;
                auto &peer = peers_by_node_id[sender_id];
                peer.fd = c.fd;
                peer.write_buf = std::move(c.write_buf);
                peer.read_buf = std::move(c.parser.buffer);
                peer.retry_count = 0;
                peer.retry_at = 0;
                fd_to_peer_id[c.fd] = sender_id;
                clients.erase(c.fd);
                process_bin_frame(frame, sender_id);

                if (peer.needs_full_sync) {
                    peer.needs_full_sync = false;
                    static uint64_t msg_id = 1;
                    BIN::Frame req;
                    req.header = {
                        .magic = BIN::MAGIC, .version = BIN::VERSION,
                        .msg_type = static_cast<uint8_t>(BIN::FrameType::FULL_SYNC_REQUEST),
                        .msg_id = msg_id++, .sender_id = self_node.id,
                        .payload_len = 0, .checksum = 0
                    };
                    peer.write_buf += BIN::serialize(req);
                }

                uint32_t flags = EPOLLIN;
                if (!peer.write_buf.empty()) flags |= EPOLLOUT;
                mod_epoll(peer.fd, flags);
                return false;
            }
            return true;
        }
    }

    COMMAND cmd;
    while (true) {
        size_t cmd_start = c.parser.cursor;
        if (!c.parser.next(cmd)) break;
        strv raw(c.parser.buffer.data() + cmd_start, c.parser.cursor - cmd_start);
        appendAOF(raw);
        dispatcher.dispatch(c, cmd);
    }
    c.parser.clear_consumed();

    if (c.write_buf.size() > WRITE_BUF_BACKPRESSURE_THRESHOLD)
        c.read_paused_by_backpressure = true;

    uint32_t flags = 0;
    if (!c.read_paused_by_backpressure)
        flags |= EPOLLIN;
    if (!c.write_buf.empty())
        flags |= EPOLLOUT;

    mod_epoll(c.fd, flags);
    return true;
}

bool handle_write(Client &c) {
    ssize_t n = send(c.fd, c.write_buf.data(), c.write_buf.size(), MSG_NOSIGNAL);

    if (n < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK)
            return true;
        close_client(c.fd);
        return false;
    }

    c.write_buf.erase(0, n);

    if (c.read_paused_by_backpressure && c.write_buf.size() < WRITE_BUF_BACKPRESSURE_THRESHOLD / 2)
        c.read_paused_by_backpressure = false;

    uint32_t flags = 0;
    if (!c.read_paused_by_backpressure)
        flags |= EPOLLIN;
    if (!c.write_buf.empty())
        flags |= EPOLLOUT;

    mod_epoll(c.fd, flags);
    return true;
}

PeerConnection *get_or_connect(uint64_t node_id, const str &ip, uint16_t port) {
    auto it = peers_by_node_id.find(node_id);
    if (it != peers_by_node_id.end() && it->second.fd >= 0)
        return &it->second;

    // Ensure an entry exists so reconnect_peers() can track backoff.
    auto &peer = peers_by_node_id[node_id];

    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        peer.fd = -1;
        peer.retry_at = current_time_ms() + (1000 << std::min(peer.retry_count, 6));
        peer.retry_count++;
        return nullptr;
    }

    fcntl(fd, F_SETFL, fcntl(fd, F_GETFL) | O_NONBLOCK);

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_pton(AF_INET, ip.c_str(), &addr.sin_addr);

    int rc = connect(fd, (struct sockaddr *) &addr, sizeof(addr));
    if (rc < 0 && errno != EINPROGRESS) {
        close(fd);
        peer.fd = -1;
        peer.retry_at = current_time_ms() + (1000 << std::min(peer.retry_count, 6));
        peer.retry_count++;
        return nullptr;
    }

    peer.fd = fd;
    fd_to_peer_id[fd] = node_id;

    struct epoll_event ev{};
    ev.events = EPOLLIN | EPOLLOUT;
    ev.data.fd = fd;
    epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd, &ev);

    return &peer;
}

void process_bin_frame(const BIN::Frame &frame, uint64_t peer_node_id) {
    switch (static_cast<BIN::FrameType>(frame.header.msg_type)) {
        case BIN::FrameType::PING: {
            NodeID sender;
            sender.id = frame.header.sender_id;
            update_last_seen(sender);
            if (!frame.payload.empty())
                apply_gossip_payload(frame.payload);
            break;
        }
        case BIN::FrameType::CLUSTER_JOIN: {
            // Payload: my_id(8) + ip_len(4) + ip(ip_len) + port(2) + generation(8)
            if (frame.payload.size() < 8 + 4 + 2 + 8) break;
            size_t off = 0;
            uint64_t sid;
            memcpy(&sid, frame.payload.data() + off, sizeof(sid));
            off += sizeof(sid);
            uint32_t ip_len;
            memcpy(&ip_len, frame.payload.data() + off, sizeof(ip_len));
            off += sizeof(ip_len);
            if (off + ip_len + 2 + 8 > frame.payload.size()) break;
            str ip(frame.payload.data() + off, ip_len);
            off += ip_len;
            uint16_t rport;
            memcpy(&rport, frame.payload.data() + off, sizeof(rport));
            off += sizeof(rport);
            uint64_t gen;
            memcpy(&gen, frame.payload.data() + off, sizeof(gen));

            // Clean up any temp-ID entry for this address before adding the real one
            resolve_node_address(ip, rport, sid);
            // Migrate peer entry from temp_id (peer_node_id) to real node ID if different
            if (sid != peer_node_id) {
                auto pit = peers_by_node_id.find(peer_node_id);
                if (pit != peers_by_node_id.end()) {
                    auto &peer = peers_by_node_id[sid];
                    peer = std::move(pit->second);
                    fd_to_peer_id[peer.fd] = sid;
                    peers_by_node_id.erase(pit);
                }
            }
            NodeID sender{sid, ip, rport, gen};
            if (cluster_state.find(sender) == cluster_state.end()) {
                add_node(sender);
                get_or_connect(sid, ip, rport);
            }
            update_last_seen(sender);

            // Send CLUSTER_JOIN_ACK with our identity
            str ack_payload;
            ack_payload.append(reinterpret_cast<const char *>(&self_node.id), sizeof(self_node.id));
            uint32_t my_ip_len = static_cast<uint32_t>(self_node.ip.size());
            ack_payload.append(reinterpret_cast<const char *>(&my_ip_len), sizeof(my_ip_len));
            ack_payload += self_node.ip;
            ack_payload.append(reinterpret_cast<const char *>(&self_node.port), sizeof(self_node.port));
            ack_payload.append(reinterpret_cast<const char *>(&self_node.generation), sizeof(self_node.generation));

            static uint64_t ack_id = 1;
            BIN::Frame ack;
            ack.header = {
                .magic = BIN::MAGIC, .version = BIN::VERSION,
                .msg_type = static_cast<uint8_t>(BIN::FrameType::CLUSTER_JOIN_ACK),
                .msg_id = ack_id++, .sender_id = self_node.id,
                .payload_len = static_cast<uint32_t>(ack_payload.size()), .checksum = 0
            };
            ack.payload = std::move(ack_payload);
            auto serialized = BIN::serialize(ack);
            auto pit = peers_by_node_id.find(sid);
            if (pit != peers_by_node_id.end() && pit->second.fd >= 0) {
                bool was_empty = pit->second.write_buf.empty();
                pit->second.write_buf += serialized;
                if (was_empty) {
                    uint32_t flags = EPOLLIN | EPOLLOUT;
                    mod_epoll(pit->second.fd, flags);
                }
            }
            break;
        }
        case BIN::FrameType::CLUSTER_JOIN_ACK: {
            if (frame.payload.size() < 8 + 4 + 2 + 8) break;
            size_t off = 0;
            uint64_t sid;
            memcpy(&sid, frame.payload.data() + off, sizeof(sid));
            off += sizeof(sid);
            uint32_t ip_len;
            memcpy(&ip_len, frame.payload.data() + off, sizeof(ip_len));
            off += sizeof(ip_len);
            if (off + ip_len + 2 + 8 > frame.payload.size()) break;
            str ip(frame.payload.data() + off, ip_len);
            off += ip_len;
            uint16_t rport;
            memcpy(&rport, frame.payload.data() + off, sizeof(rport));
            off += sizeof(rport);
            uint64_t gen;
            memcpy(&gen, frame.payload.data() + off, sizeof(gen));

            // Migrate peer entry from temp_id (peer_node_id) to real node ID if different
            if (sid != peer_node_id) {
                auto it = peers_by_node_id.find(peer_node_id);
                if (it != peers_by_node_id.end()) {
                    auto &peer = peers_by_node_id[sid];
                    peer = std::move(it->second);
                    fd_to_peer_id[peer.fd] = sid;
                    peers_by_node_id.erase(it);
                }
            }

            // Clean up any temp-ID entry for this address before adding the real one
            resolve_node_address(ip, rport, sid);
            NodeID remote{sid, ip, rport, gen};
            if (cluster_state.find(remote) == cluster_state.end())
                add_node(remote);
            update_last_seen(remote);
            break;
        }
        case BIN::FrameType::REPLICATE_PUT: {
            appendAOF(frame.payload);
            RESP::Parser p;
            p.feed(frame.payload);
            COMMAND cmd;
            g_replication_mode = true;
            while (p.next(cmd)) {
                cmd.from_replication = true;
                execute_command(cmd);
            }
            g_replication_mode = false;
            // Send ACK back to the originator
            BIN::Frame ack;
            ack.header = {
                .magic = BIN::MAGIC,
                .version = BIN::VERSION,
                .msg_type = static_cast<uint8_t>(BIN::FrameType::REPLICATE_ACK),
                .msg_id = frame.header.msg_id,
                .sender_id = self_node.id,
                .payload_len = 0,
                .checksum = 0
            };
            auto ack_serialized = BIN::serialize(ack);
            auto it = peers_by_node_id.find(frame.header.sender_id);
            if (it != peers_by_node_id.end() && it->second.fd >= 0) {
                bool was_empty = it->second.write_buf.empty();
                it->second.write_buf += ack_serialized;
                if (was_empty) {
                    uint32_t flags = EPOLLIN | EPOLLOUT;
                    mod_epoll(it->second.fd, flags);
                }
            }
            break;
        }
        case BIN::FrameType::REPLICATE_ACK: {
            auto pit = pending_writes.find(frame.header.msg_id);
            if (pit == pending_writes.end()) break;
            pit->second.ack_count++;
            if (pit->second.ack_count >= pit->second.target_count) {
                auto cit = clients.find(pit->second.client_fd);
                if (cit != clients.end()) {
                    cit->second.write_buf += pit->second.response;
                    uint32_t flags = EPOLLIN;
                    if (!cit->second.write_buf.empty())
                        flags |= EPOLLOUT;
                    if (cit->second.read_paused_by_backpressure)
                        flags = EPOLLOUT;
                    mod_epoll(cit->second.fd, flags);
                }
                pending_writes.erase(pit);
            }
            break;
        }
        case BIN::FrameType::PROXY_REQUEST: {
            RESP::Parser p;
            p.feed(frame.payload);
            COMMAND cmd;
            str response;
            while (p.next(cmd)) {
                response += execute_command(cmd);
            }
            // Send response back
            BIN::Frame resp;
            resp.header = {
                .magic = BIN::MAGIC, .version = BIN::VERSION,
                .msg_type = static_cast<uint8_t>(BIN::FrameType::PROXY_RESPONSE),
                .msg_id = frame.header.msg_id, .sender_id = self_node.id,
                .payload_len = static_cast<uint32_t>(response.size()), .checksum = 0
            };
            resp.payload = std::move(response);
            auto serialized = BIN::serialize(resp);
            auto pit = peers_by_node_id.find(frame.header.sender_id);
            if (pit != peers_by_node_id.end() && pit->second.fd >= 0) {
                bool was_empty = pit->second.write_buf.empty();
                pit->second.write_buf += serialized;
                if (was_empty) {
                    uint32_t flags = EPOLLIN | EPOLLOUT;
                    mod_epoll(pit->second.fd, flags);
                }
            }
            break;
        }
        case BIN::FrameType::FULL_SYNC_REQUEST: {
            auto peer_it = peers_by_node_id.find(frame.header.sender_id);
            if (peer_it == peers_by_node_id.end() || peer_it->second.fd < 0) break;
            peer_it->second.needs_full_sync = false;

            constexpr size_t CHUNK_SIZE = 50;
            auto keys = store_keys();
            static uint64_t chunk_msg_id = 1;
            for (size_t i = 0; i < keys.size(); i += CHUNK_SIZE) {
                str payload;
                for (size_t j = i; j < i + CHUNK_SIZE && j < keys.size(); j++) {
                    auto *entry = store_get(keys[j]);
                    if (!entry) continue;
                    payload += serialize_entry(keys[j], *entry);
                }
                if (payload.empty()) continue;

                BIN::Frame chunk;
                chunk.header = {
                    .magic = BIN::MAGIC, .version = BIN::VERSION,
                    .msg_type = static_cast<uint8_t>(BIN::FrameType::FULL_SYNC_CHUNK),
                    .msg_id = chunk_msg_id++, .sender_id = self_node.id,
                    .payload_len = static_cast<uint32_t>(payload.size()), .checksum = 0
                };
                chunk.payload = std::move(payload);
                auto serialized = BIN::serialize(chunk);
                bool was_empty = peer_it->second.write_buf.empty();
                peer_it->second.write_buf += serialized;
                if (was_empty && peer_it->second.fd >= 0) {
                    uint32_t flags = EPOLLIN | EPOLLOUT;
                    mod_epoll(peer_it->second.fd, flags);
                }
            }
            break;
        }
        case BIN::FrameType::FULL_SYNC_CHUNK: {
            RESP::Parser p;
            p.feed(frame.payload);
            COMMAND cmd;
            g_replication_mode = true;
            while (p.next(cmd)) {
                cmd.from_replication = true;
                execute_command(cmd);
            }
            g_replication_mode = false;
            break;
        }
        case BIN::FrameType::ANTIENTROPY_HASH: {
            auto remote = deserialize_merkle_tree(frame.payload);
            auto local = compute_merkle_tree();
            auto diff = find_differing_slots(local, remote.tree);
            if (diff.empty()) break;

            str payload;
            payload.append(reinterpret_cast<const char *>(&remote.version), sizeof(remote.version));
            uint16_t n = static_cast<uint16_t>(diff.size());
            payload.append(reinterpret_cast<const char *>(&n), sizeof(n));
            for (auto s: diff) {
                payload.append(reinterpret_cast<const char *>(&s), sizeof(s));
            }
            static uint64_t req_id = 1;
            BIN::Frame req;
            req.header = {
                .magic = BIN::MAGIC, .version = BIN::VERSION,
                .msg_type = static_cast<uint8_t>(BIN::FrameType::ANTIENTROPY_SYNC),
                .msg_id = req_id++, .sender_id = self_node.id,
                .payload_len = static_cast<uint32_t>(payload.size()), .checksum = 0
            };
            req.payload = std::move(payload);
            auto serialized = BIN::serialize(req);
            auto it = peers_by_node_id.find(frame.header.sender_id);
            if (it != peers_by_node_id.end() && it->second.fd >= 0) {
                bool was_empty = it->second.write_buf.empty();
                it->second.write_buf += serialized;
                if (was_empty) {
                    uint32_t flags = EPOLLIN | EPOLLOUT;
                    mod_epoll(it->second.fd, flags);
                }
            }
            break;
        }
        case BIN::FrameType::ANTIENTROPY_SYNC: {
            if (frame.payload.size() < sizeof(uint64_t) + sizeof(uint16_t)) break;
            uint64_t req_version;
            memcpy(&req_version, frame.payload.data(), sizeof(uint64_t));
            if (req_version != g_store_version.load()) break; // concurrent mod — skip, retry next round
            size_t off = sizeof(uint64_t);
            uint16_t num_slots;
            memcpy(&num_slots, frame.payload.data() + off, sizeof(uint16_t));
            off += sizeof(uint16_t);
            for (uint16_t i = 0; i < num_slots && off + sizeof(uint16_t) <= frame.payload.size(); i++) {
                uint16_t slot;
                memcpy(&slot, frame.payload.data() + off, sizeof(uint16_t));
                off += sizeof(uint16_t);
                auto entries_payload = get_slot_entries_payload(slot);
                if (entries_payload.empty()) continue;

                static uint64_t chunk_id = 1;
                BIN::Frame chunk;
                chunk.header = {
                    .magic = BIN::MAGIC, .version = BIN::VERSION,
                    .msg_type = static_cast<uint8_t>(BIN::FrameType::FULL_SYNC_CHUNK),
                    .msg_id = chunk_id++, .sender_id = self_node.id,
                    .payload_len = static_cast<uint32_t>(entries_payload.size()), .checksum = 0
                };
                chunk.payload = std::move(entries_payload);
                auto serialized = BIN::serialize(chunk);
                auto pit = peers_by_node_id.find(frame.header.sender_id);
                if (pit != peers_by_node_id.end() && pit->second.fd >= 0) {
                    bool was_empty = pit->second.write_buf.empty();
                    pit->second.write_buf += serialized;
                    if (was_empty) {
                        uint32_t flags = EPOLLIN | EPOLLOUT;
                        mod_epoll(pit->second.fd, flags);
                    }
                }
            }
            break;
        }
        case BIN::FrameType::READ_REQUEST: {
            RESP::Parser p;
            p.feed(frame.payload);
            COMMAND cmd;
            if (!p.next(cmd) || cmd.args.empty()) break;

            str response = execute_command(cmd);
            std::unordered_map<uint64_t, counter> vclock;
            auto *entry = store_get(cmd.args[0]);
            if (entry) vclock = entry->VecClk;

            str payload;
            uint32_t resp_len = static_cast<uint32_t>(response.size());
            payload.append(reinterpret_cast<const char *>(&resp_len), sizeof(resp_len));
            payload += response;
            uint32_t vc_count = static_cast<uint32_t>(vclock.size());
            payload.append(reinterpret_cast<const char *>(&vc_count), sizeof(vc_count));
            for (const auto &[id, cnt]: vclock) {
                payload.append(reinterpret_cast<const char *>(&id), sizeof(id));
                payload.append(reinterpret_cast<const char *>(&cnt), sizeof(cnt));
            }

            BIN::Frame resp;
            resp.header = {
                .magic = BIN::MAGIC, .version = BIN::VERSION,
                .msg_type = static_cast<uint8_t>(BIN::FrameType::READ_RESPONSE),
                .msg_id = frame.header.msg_id, .sender_id = self_node.id,
                .payload_len = static_cast<uint32_t>(payload.size()), .checksum = 0
            };
            resp.payload = std::move(payload);
            auto serialized = BIN::serialize(resp);

            auto pit = peers_by_node_id.find(frame.header.sender_id);
            if (pit != peers_by_node_id.end() && pit->second.fd >= 0) {
                bool was_empty = pit->second.write_buf.empty();
                pit->second.write_buf += serialized;
                if (was_empty) {
                    uint32_t flags = EPOLLIN | EPOLLOUT;
                    mod_epoll(pit->second.fd, flags);
                }
            }
            break;
        }
        case BIN::FrameType::READ_RESPONSE: {
            if (frame.payload.size() < 4) break;
            uint32_t resp_len;
            memcpy(&resp_len, frame.payload.data(), sizeof(resp_len));
            if (frame.payload.size() < sizeof(resp_len) + resp_len + 4) break;
            str response = frame.payload.substr(sizeof(resp_len), resp_len);
            size_t off = sizeof(resp_len) + resp_len;
            uint32_t vc_count;
            memcpy(&vc_count, frame.payload.data() + off, sizeof(vc_count));
            off += sizeof(vc_count);
            std::unordered_map<uint64_t, counter> vclock;
            for (uint32_t i = 0; i < vc_count && off + 16 <= frame.payload.size(); i++) {
                uint64_t id, cnt;
                memcpy(&id, frame.payload.data() + off, sizeof(id));
                off += sizeof(id);
                memcpy(&cnt, frame.payload.data() + off, sizeof(cnt));
                off += sizeof(cnt);
                vclock[id] = cnt;
            }
            auto rit = pending_reads.find(frame.header.msg_id);
            if (rit == pending_reads.end()) break;
            auto &pr = rit->second;
            auto cmp = compare_vclock(vclock, pr.best_vclock);
            if (cmp == VClockCmp::NEWER) {
                pr.best_response = response;
                pr.best_vclock = vclock;
                // Read-repair: remote has a newer version — update local store
                if (!pr.key.empty()) {
                    auto local_cmp = compare_vclock(vclock, pr.local_vclock);
                    if (local_cmp == VClockCmp::NEWER) {
                        auto *existing = store_get(pr.key);
                        if (existing && existing->type == Type::STRING && !response.empty() && response[0] == '$') {
                            auto cr = response.find("\r\n");
                            if (cr != str::npos && cr >= 1) {
                                str len_str = response.substr(1, cr - 1);
                                char *end = nullptr;
                                long long val_len = std::strtoll(len_str.c_str(), &end, 10);
                                if (end != len_str.c_str() && val_len >= 0) {
                                    str val = response.substr(cr + 2, static_cast<size_t>(val_len));
                                    bool saved = g_replication_mode;
                                    g_replication_mode = true;
                                    ValueEntry repaired;
                                    repaired.type = Type::STRING;
                                    repaired.value = val;
                                    repaired.VecClk = vclock;
                                    store_set(pr.key, std::move(repaired));
                                    g_replication_mode = saved;
                                }
                            }
                        }
                    }
                }
            } else if (cmp == VClockCmp::CONCURRENT) {
                uint64_t sum_in = 0, sum_best = 0;
                for (const auto &[_, c]: vclock) sum_in += c;
                for (const auto &[_, c]: pr.best_vclock) sum_best += c;
                if (sum_in > sum_best) {
                    pr.best_response = response;
                    pr.best_vclock = vclock;
                }
            }
            pr.response_count++;
            if (pr.response_count >= pr.expected_count) {
                auto cit = clients.find(pr.client_fd);
                if (cit != clients.end()) {
                    cit->second.write_buf += pr.best_response;
                    uint32_t flags = EPOLLIN;
                    if (!cit->second.write_buf.empty()) flags |= EPOLLOUT;
                    if (cit->second.read_paused_by_backpressure) flags = EPOLLOUT;
                    mod_epoll(cit->second.fd, flags);
                }
                pending_reads.erase(rit);
            }
            break;
        }
        case BIN::FrameType::PROXY_RESPONSE: {
            auto pit = gather_parent.find(frame.header.msg_id);
            if (pit == gather_parent.end()) break;
            uint64_t gather_id = pit->second;
            auto kit = gather_keys.find(frame.header.msg_id);
            if (kit == gather_keys.end()) {
                gather_parent.erase(pit);
                break;
            }
            auto key_indices = kit->second;
            gather_parent.erase(pit);
            gather_keys.erase(kit);

            auto git = pending_gathers.find(gather_id);
            if (git == pending_gathers.end()) break;
            auto &pg = git->second;
            pg.received++;

            if (pg.cmd_type == commandType::MGET) {
                auto elements = split_resp_responses(frame.payload);
                for (size_t ei = 0; ei < elements.size() && ei < key_indices.size(); ei++) {
                    size_t ki = key_indices[ei];
                    if (ki < pg.parts.size())
                        pg.parts[ki] = std::move(elements[ei]);
                }
            } else if (pg.cmd_type == commandType::MSET) {
                // +OK responses — nothing to accumulate
            } else {
                auto elements = split_resp_responses(frame.payload);
                for (auto &elem: elements) {
                    if (!elem.empty() && elem[0] == ':')
                        pg.accumulated += std::atol(elem.c_str() + 1);
                }
            }

            if (pg.received >= pg.expected) {
                str final_resp;
                if (pg.cmd_type == commandType::MGET) {
                    TOKENS resp_parts;
                    resp_parts.reserve(pg.parts.size());
                    for (auto &p: pg.parts)
                        resp_parts.push_back(std::move(p));
                    final_resp = RESP::array_raw(resp_parts);
                } else if (pg.cmd_type == commandType::MSET) {
                    final_resp = RESP::ok();
                } else {
                    final_resp = RESP::integer(pg.accumulated);
                }

                auto cit = clients.find(pg.client_fd);
                if (cit != clients.end()) {
                    cit->second.write_buf += final_resp;
                    uint32_t flags = EPOLLIN;
                    if (!cit->second.write_buf.empty()) flags |= EPOLLOUT;
                    if (cit->second.read_paused_by_backpressure) flags = EPOLLOUT;
                    mod_epoll(cit->second.fd, flags);
                }
                pending_gathers.erase(git);
            }
            break;
        }
        default:
            break;
    }
}

void handle_peer_read(PeerConnection &peer, uint64_t peer_node_id) {
    char buf[65536];
    ssize_t n = read(peer.fd, buf, sizeof(buf));
    if (n > 0) {
        peer.read_buf.append(buf, n);
    } else if (n == 0 || (n < 0 && errno != EAGAIN && errno != EWOULDBLOCK)) {
        fd_to_peer_id.erase(peer.fd);
        epoll_ctl(epoll_fd, EPOLL_CTL_DEL, peer.fd, nullptr);
        close(peer.fd);
        peer.fd = -1;
        peer.retry_at = current_time_ms() + (1000 << std::min(peer.retry_count, 6));
        peer.retry_count++;
        peer.needs_full_sync = true;
        return;
    }

    BIN::Frame frame;
    while (BIN::parse(peer.read_buf, frame)) {
        uint64_t real_id = frame.header.sender_id;
        if (real_id != peer_node_id) {
            // Real ID differs from the key we looked up — migrate the peer entry
            auto pit = peers_by_node_id.find(peer_node_id);
            if (pit != peers_by_node_id.end()) {
                auto &migrated = peers_by_node_id[real_id];
                migrated = std::move(pit->second);
                fd_to_peer_id[migrated.fd] = real_id;
                peers_by_node_id.erase(pit);
                // Re-lookup: peer reference is now dangling, get fresh one
                auto np = peers_by_node_id.find(real_id);
                if (np == peers_by_node_id.end()) break;
                PeerConnection &fresh_peer = np->second;
                process_bin_frame(frame, real_id);
                while (BIN::parse(fresh_peer.read_buf, frame)) {
                    process_bin_frame(frame, real_id);
                }
                uint32_t flags = EPOLLIN;
                if (!fresh_peer.write_buf.empty()) flags |= EPOLLOUT;
                mod_epoll(fresh_peer.fd, flags);
                return;
            }
        }
        process_bin_frame(frame, real_id);
    }

    uint32_t flags = EPOLLIN;
    if (!peer.write_buf.empty()) flags |= EPOLLOUT;
    mod_epoll(peer.fd, flags);
}

void handle_peer_write(PeerConnection &peer) {
    if (peer.write_buf.empty()) {
        // EPOLLOUT with no data to write — likely connect completion.
        // Check for deferred connect errors (ECONNREFUSED etc.).
        int so_error = 0;
        socklen_t len = sizeof(so_error);
        getsockopt(peer.fd, SOL_SOCKET, SO_ERROR, &so_error, &len);
        if (so_error != 0) {
            fd_to_peer_id.erase(peer.fd);
            epoll_ctl(epoll_fd, EPOLL_CTL_DEL, peer.fd, nullptr);
            close(peer.fd);
            peer.fd = -1;
            peer.retry_at = current_time_ms() + (1000 << std::min(peer.retry_count, 6));
            peer.retry_count++;
            peer.needs_full_sync = true;
            return;
        }
        // Connect succeeded — connection is alive.
        peer.retry_count = 0;
        if (peer.needs_full_sync) {
            peer.needs_full_sync = false;
            static uint64_t msg_id = 1;
            BIN::Frame req;
            req.header = {
                .magic = BIN::MAGIC, .version = BIN::VERSION,
                .msg_type = static_cast<uint8_t>(BIN::FrameType::FULL_SYNC_REQUEST),
                .msg_id = msg_id++, .sender_id = self_node.id,
                .payload_len = 0, .checksum = 0
            };
            auto serialized = BIN::serialize(req);
            bool was_empty = peer.write_buf.empty();
            peer.write_buf += serialized;
            if (was_empty && peer.fd >= 0) {
                uint32_t flags = EPOLLIN | EPOLLOUT;
                mod_epoll(peer.fd, flags);
            }
        }
        return;
    }
    ssize_t n = send(peer.fd, peer.write_buf.data(), peer.write_buf.size(), MSG_NOSIGNAL);
    if (n < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) return;
        fd_to_peer_id.erase(peer.fd);
        epoll_ctl(epoll_fd, EPOLL_CTL_DEL, peer.fd, nullptr);
        close(peer.fd);
        peer.fd = -1;
        peer.retry_at = current_time_ms() + (1000 << std::min(peer.retry_count, 6));
        peer.retry_count++;
        peer.needs_full_sync = true;
        return;
    }
    peer.write_buf.erase(0, n);
    peer.retry_count = 0; // Connection confirmed alive

    uint32_t flags = EPOLLIN;
    if (!peer.write_buf.empty()) flags |= EPOLLOUT;
    mod_epoll(peer.fd, flags);
}

void flush_replica_queue() {
    static uint64_t msg_id = 1;
    while (!replica_queue.empty()) {
        auto &op = replica_queue.front();
        BIN::Frame frame;
        frame.header = {
            .magic = BIN::MAGIC,
            .version = BIN::VERSION,
            .msg_type = static_cast<uint8_t>(BIN::FrameType::REPLICATE_PUT),
            .msg_id = msg_id++,
            .sender_id = self_node.id,
            .payload_len = static_cast<uint32_t>(op.raw_command.size()),
            .checksum = 0
        };
        frame.payload = op.raw_command;
        auto serialized = BIN::serialize(frame);

        if (op.client_fd >= 0 && !op.target_ids.empty()) {
            int wq = std::min(config().write_quorum, static_cast<int>(op.target_ids.size()));
            pending_writes[frame.header.msg_id] = {
                op.deferred_response, op.client_fd, 0,
                wq,
                current_time_ms()
            };
        }

        for (auto id: op.target_ids) {
            auto it = peers_by_node_id.find(id);
            if (it != peers_by_node_id.end() && it->second.fd >= 0)
                it->second.write_buf += serialized;
        }
        replica_queue.pop_front();
    }
}

static std::vector<NodeID> g_seed_addresses;

void set_seed_addresses(const std::vector<NodeID> &seeds) {
    g_seed_addresses = seeds;
}

void connect_to_peers() {
    for (const auto &node: get_all_nodes()) {
        if (node.id > self_node.id)
            get_or_connect(node.id, node.ip, node.port);
    }
    // Connect to seed addresses regardless of ID ordering
    for (const auto &seed: g_seed_addresses) {
        if (seed.id == self_node.id) continue;
        get_or_connect(seed.id, seed.ip, seed.port);
    }
}

void send_heartbeats() {
    static uint64_t msg_id = 1;
    auto gossip = encode_gossip_payload();
    BIN::Frame frame;
    frame.header = {
        .magic = BIN::MAGIC,
        .version = BIN::VERSION,
        .msg_type = static_cast<uint8_t>(BIN::FrameType::PING),
        .msg_id = msg_id++,
        .sender_id = self_node.id,
        .payload_len = static_cast<uint32_t>(gossip.size()),
        .checksum = 0
    };
    frame.payload = std::move(gossip);
    auto serialized = BIN::serialize(frame);
    for (auto &[nid, peer]: peers_by_node_id) {
        if (nid != self_node.id && peer.fd >= 0) {
            bool was_empty = peer.write_buf.empty();
            peer.write_buf += serialized;
            if (was_empty) {
                uint32_t flags = EPOLLIN | EPOLLOUT;
                mod_epoll(peer.fd, flags);
            }
        }
    }
}

void reconnect_peers() {
    auto now = current_time_ms();
    for (auto &[nid, peer]: peers_by_node_id) {
        if (nid <= self_node.id) continue;
        if (peer.fd >= 0) continue;
        if (now < peer.retry_at) continue;

        auto it = cluster_state.find(NodeID{nid, "", 0});
        if (it == cluster_state.end()) continue;
        auto &node = it->first;

        get_or_connect(nid, node.ip, node.port);
    }
}

void run_background_tasks() {
    if (g_shutdown_requested) {
        remove_node(self_node);
        // Broadcast one final PING with LEFT status so peers know
        auto gossip = encode_gossip_payload();
        BIN::Frame bye;
        bye.header = {
            .magic = BIN::MAGIC, .version = BIN::VERSION,
            .msg_type = static_cast<uint8_t>(BIN::FrameType::PING),
            .msg_id = 0, .sender_id = self_node.id,
            .payload_len = static_cast<uint32_t>(gossip.size()), .checksum = 0
        };
        bye.payload = std::move(gossip);
        auto serialized = BIN::serialize(bye);
        for (auto &[nid, peer]: peers_by_node_id) {
            if (peer.fd >= 0) {
                ::write(peer.fd, serialized.data(), serialized.size());
            }
        }
        closeAOF();
        _exit(0);
    }

    if (g_rewrite_pending.exchange(false)) {
        rewriteAOF();
    }

    static int64_t last_tick = 0;
    static int64_t last_heartbeat = 0;
    static int64_t last_timeout_check = 0;
    static int tick_count = 0;
    auto now = current_time_ms();

    flush_replica_queue();

    // Timeout pending writes after 10s — return error (never ack uncommitted writes)
    for (auto it = pending_writes.begin(); it != pending_writes.end();) {
        if (now - it->second.created_at >= 10000) {
            auto cit = clients.find(it->second.client_fd);
            if (cit != clients.end()) {
                cit->second.write_buf += RESP::error("not enough replicas available, write abandoned");
                uint32_t flags = EPOLLIN;
                if (!cit->second.write_buf.empty())
                    flags |= EPOLLOUT;
                if (cit->second.read_paused_by_backpressure)
                    flags = EPOLLOUT;
                mod_epoll(cit->second.fd, flags);
            }
            it = pending_writes.erase(it);
        } else {
            ++it;
        }
    }

    // Timeout pending reads after 5s — respond with best result
    for (auto it = pending_reads.begin(); it != pending_reads.end();) {
        if (now - it->second.created_at >= 5000) {
            auto cit = clients.find(it->second.client_fd);
            if (cit != clients.end()) {
                cit->second.write_buf += it->second.best_response;
                uint32_t flags = EPOLLIN;
                if (!cit->second.write_buf.empty())
                    flags |= EPOLLOUT;
                if (cit->second.read_paused_by_backpressure)
                    flags = EPOLLOUT;
                mod_epoll(cit->second.fd, flags);
            }
            it = pending_reads.erase(it);
        } else {
            ++it;
        }
    }

    // Timeout pending gathers after 5s — respond with partial result
    for (auto it = pending_gathers.begin(); it != pending_gathers.end();) {
        if (now - it->second.created_at >= 5000) {
            str partial;
            if (it->second.cmd_type == commandType::MGET) {
                TOKENS resp_parts;
                resp_parts.reserve(it->second.parts.size());
                for (auto &p: it->second.parts) {
                    if (p.empty()) resp_parts.push_back(RESP::null_bulk_string());
                    else resp_parts.push_back(std::move(p));
                }
                partial = RESP::array_raw(resp_parts);
            } else if (it->second.cmd_type == commandType::MSET) {
                partial = RESP::ok();
            } else {
                partial = RESP::integer(it->second.accumulated);
            }
            auto cit = clients.find(it->second.client_fd);
            if (cit != clients.end()) {
                cit->second.write_buf += partial;
                uint32_t flags = EPOLLIN;
                if (!cit->second.write_buf.empty())
                    flags |= EPOLLOUT;
                if (cit->second.read_paused_by_backpressure)
                    flags = EPOLLOUT;
                mod_epoll(cit->second.fd, flags);
            }
            it = pending_gathers.erase(it);
        } else {
            ++it;
        }
    }

    // Close idle clients
    std::vector<int> idle_fds;
    for (auto& [fd, c] : clients) {
        if (now - c.last_active_ms >= CLIENT_IDLE_TIMEOUT_MS) {
            c.idle_timed_out = true;
            idle_fds.push_back(fd);
        }
    }
    for (int fd : idle_fds) close_client(fd);

    flushAOF();
    expire_sweep();
    reconnect_peers();

    // Connect to new peers discovered via gossip
    for (const auto &[node, status]: cluster_state) {
        if (node.id <= self_node.id) continue;
        if (status != NodeStatus::ALIVE) continue;
        if (peers_by_node_id.find(node.id) != peers_by_node_id.end()) continue;
        get_or_connect(node.id, node.ip, node.port);
    }

    if (now - last_heartbeat >= config().gossip_interval_ms) {
        last_heartbeat = now;
        send_heartbeats();
    }

    if (now - last_timeout_check >= config().gossip_interval_ms) {
        last_timeout_check = now;
        std::unordered_set<uint64_t> connected;
        for (auto &[nid, peer]: peers_by_node_id) {
            if (peer.fd >= 0) connected.insert(nid);
        }
        check_timeouts(&connected);
    }

    static int64_t last_merkle = 0;
    if (now - last_merkle >= config().merkle_interval_ms) {
        last_merkle = now;
        auto tree = compute_merkle_tree();
        auto m_payload = serialize_merkle_tree(tree, g_store_version.load());
        static uint64_t merkle_id = 1;
        BIN::Frame m_frame;
        m_frame.header = {
            .magic = BIN::MAGIC, .version = BIN::VERSION,
            .msg_type = static_cast<uint8_t>(BIN::FrameType::ANTIENTROPY_HASH),
            .msg_id = merkle_id++, .sender_id = self_node.id,
            .payload_len = static_cast<uint32_t>(m_payload.size()), .checksum = 0
        };
        m_frame.payload = std::move(m_payload);
        auto serialized = BIN::serialize(m_frame);
        for (auto &[nid, peer]: peers_by_node_id) {
            if (nid == self_node.id || peer.fd < 0) continue;
            bool was_empty = peer.write_buf.empty();
            peer.write_buf += serialized;
            if (was_empty) {
                uint32_t flags = EPOLLIN | EPOLLOUT;
                mod_epoll(peer.fd, flags);
            }
        }
    }

    if (now - last_tick >= 5000) {
        last_tick = now;
        tick_count++;
        // std::cout << "\033[36mtick " << tick_count << "\033[0m" << std::endl;
    }
}

[[noreturn]] void run_loop() {
    struct epoll_event events[64];
    while (true) {
        int n = epoll_wait(epoll_fd, events, 64, 50);
        for (int i = 0; i < n; ++i) {
            int fd = events[i].data.fd;
            uint32_t revents = events[i].events;

            if (fd == server_fd) {
                accept_new_clients();
                continue;
            }

            auto cit = clients.find(fd);
            if (cit != clients.end()) {
                Client &c = cit->second;
                bool client_alive = true;
                if (revents & EPOLLIN)
                    client_alive = handle_read(c);
                if (client_alive && (revents & EPOLLOUT))
                    handle_write(c);
                continue;
            }

            auto pit = fd_to_peer_id.find(fd);
            if (pit != fd_to_peer_id.end()) {
                auto pn = peers_by_node_id.find(pit->second);
                if (pn != peers_by_node_id.end()) {
                    uint64_t pnid = pn->first;
                    if (revents & EPOLLIN) {
                        handle_peer_read(pn->second, pnid);
                        // Re-lookup: handle_peer_read may have migrated this entry
                        pit = fd_to_peer_id.find(fd);
                        if (pit == fd_to_peer_id.end()) continue;
                        pn = peers_by_node_id.find(pit->second);
                    }
                    if (pn != peers_by_node_id.end() && (revents & EPOLLOUT))
                        handle_peer_write(pn->second);
                }
                continue;
            }
        }
        run_background_tasks();
    }
}
