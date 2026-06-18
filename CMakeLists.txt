cmake_minimum_required(VERSION 4.2)
project(DRedis)

set(CMAKE_CXX_STANDARD 23)

add_executable(DRedis main.cpp
        parser.cpp
        parser.h
        cmd.cpp
        store.cpp
        store.h
        cmd.h
        network.cpp
        network.h
        benchmark.h
        cluster.cpp
        cluster.h
        dispatcher.cpp
        dispatcher.h
        crc32c.h
        merkle.cpp
        merkle.h
        config.cpp
         config.h
         benchmarks.cpp
         benchmarks.h
)
