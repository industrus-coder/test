cmake_minimum_required(VERSION 3.10)
project(DRedis)

set(CMAKE_CXX_STANDARD 23)
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -Wall -Wextra -Werror")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=address -fsanitize=undefined")

add_executable(DRedis
        src/main.cpp
        src/parser.cpp
        src/parser.h
        src/cmd.cpp
        src/store.cpp
        src/store.h
        src/cmd.h
        src/network.cpp
        src/network.h
        src/cluster.cpp
        src/cluster.h
        src/dispatcher.cpp
        src/dispatcher.h
        src/crc32c.h
        src/merkle.cpp
        src/merkle.h
        src/config.cpp
        src/config.h
)

target_include_directories(DRedis PRIVATE src)

install(TARGETS DRedis DESTINATION bin)

