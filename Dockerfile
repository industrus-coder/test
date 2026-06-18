FROM ubuntu:latest
LABEL authors="parthpathak"

RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    g++ \
    redis-tools \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY . .

RUN rm -rf build && mkdir build && cmake -S . -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build

CMD ["./build/DRedis"]