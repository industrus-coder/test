#ifndef DREDIS_BENCHMARK_H
#define DREDIS_BENCHMARK_H
#pragma once
#include <iostream>
#include <vector>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <cstring>
#include <sys/wait.h>
#include <chrono>
#include <atomic>

enum class BenchCmd { PING, SET, GET, MIXED };

static inline void stress_client(int total, uint16_t port, BenchCmd cmd_type = BenchCmd::PING) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return;
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
    if (connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        close(fd);
        return;
    }

    char b[8192];
    for (int i = 0; i < total; ++i) {
        const char* cmd = nullptr;
        char set_cmd[64];
        char get_cmd[64];
        switch (cmd_type) {
            case BenchCmd::PING:
                cmd = "*1\r\n$4\r\nPING\r\n";
                break;
            case BenchCmd::SET:
                snprintf(set_cmd, sizeof(set_cmd), "*3\r\n$3\r\nSET\r\n$%zu\r\nbenchkey:%d\r\n$%zu\r\nvalue:%d\r\n",
                         std::to_string(i % 1000).size(), i % 1000,
                         std::to_string(i).size(), i);
                cmd = set_cmd;
                break;
            case BenchCmd::GET:
                snprintf(get_cmd, sizeof(get_cmd), "*2\r\n$3\r\nGET\r\n$%zu\r\nbenchkey:%d\r\n",
                         std::to_string(i % 1000).size(), i % 1000);
                cmd = get_cmd;
                break;
            case BenchCmd::MIXED:
                if (i % 2 == 0) {
                    snprintf(set_cmd, sizeof(set_cmd), "*3\r\n$3\r\nSET\r\n$%zu\r\nbenchkey:%d\r\n$%zu\r\nvalue:%d\r\n",
                             std::to_string(i % 1000).size(), i % 1000,
                             std::to_string(i).size(), i);
                    cmd = set_cmd;
                } else {
                    snprintf(get_cmd, sizeof(get_cmd), "*2\r\n$3\r\nGET\r\n$%zu\r\nbenchkey:%d\r\n",
                             std::to_string(i % 1000).size(), i % 1000);
                    cmd = get_cmd;
                }
                break;
        }
        write(fd, cmd, strlen(cmd));
        ssize_t n = read(fd, b, sizeof(b));
        (void)n;
    }
    close(fd);
}

static inline void run_benchmark(uint16_t port = 6379, BenchCmd cmd_type = BenchCmd::PING,
                                  int clients = 50, int req_per_client = 200) {
    if (fork() == 0) {
        sleep(1);
        const char* cmd_name = cmd_type == BenchCmd::PING ? "PING" :
                               cmd_type == BenchCmd::SET ? "SET" :
                               cmd_type == BenchCmd::GET ? "GET" : "MIXED";
        int total = clients * req_per_client;
        std::cout << "[BENCHMARK] " << cmd_name << ": " << clients << " clients, "
                  << total << " requests on port " << port << "..." << std::endl;
        auto t1 = std::chrono::high_resolution_clock::now();
        std::vector<pid_t> pids;
        for (int i = 0; i < clients; ++i) {
            pid_t pid = fork();
            if (pid == 0) {
                stress_client(req_per_client, port, cmd_type);
                std::exit(0);
            } else if (pid > 0) {
                pids.push_back(pid);
            }
        }
        for (pid_t pid : pids) waitpid(pid, nullptr, 0);
        auto t2 = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double, std::milli> ms = t2 - t1;
        double sec = ms.count() / 1000.0;
        std::cout << "[BENCHMARK RESULT] " << cmd_name << ": "
                  << total << " requests in " << sec << "s — "
                  << static_cast<int>(total / sec) << " req/sec" << std::endl;
        std::exit(0);
    }
}
#endif
