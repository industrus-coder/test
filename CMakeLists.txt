cmake_minimum_required(VERSION 3.10)
project(DRedis)

set(CMAKE_CXX_STANDARD 23)
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -Wall -Wextra -Werror")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=address -fsanitize=undefined")

add_executable(DRedis
        main.cpp
        parser.cpp
        parser.h
        cmd.cpp
        store.cpp
        store.h
        cmd.h
        network.cpp
        network.h
        cluster.cpp
        cluster.h
        dispatcher.cpp
        dispatcher.h
        crc32c.h
        merkle.cpp
        merkle.h
        config.cpp
        config.h
)

install(TARGETS DRedis DESTINATION bin)

