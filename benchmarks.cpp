#include "benchmarks.h"
#include "benchmark.h"
#include "config.h"

void run_benchmarks() {
    // Run simple ping benchmark
    run_benchmark(static_cast<uint16_t>(config().client_port), BenchCmd::PING);
}
