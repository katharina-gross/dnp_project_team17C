from __future__ import annotations

import json
import tempfile
import time
from pathlib import Path

import matplotlib.pyplot as plt

from coordinator.master import run_job
from framework.config import (
    BENCHMARK_CHUNK_SIZES,
    BENCHMARK_WORKER_COUNTS,
    DEFAULT_BENCHMARK_PLOT_PATH,
    DEFAULT_BENCHMARK_RESULT_PATH,
    DEFAULT_LARGE_LOG_PATH,
    DEFAULT_SAMPLE_LOG_PATH,
)


def benchmark_single_run(log_path: Path, chunk_size: int, worker_count: int) -> float:
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir_path = Path(temp_dir)

        start_time = time.perf_counter()

        run_job(
            log_path=log_path,
            chunk_size=chunk_size,
            worker_count=worker_count,
            output_path=temp_dir_path / "temp_result.json",
            stats_path=temp_dir_path / "temp_stats.json",
        )

        end_time = time.perf_counter()

    return end_time - start_time


def run_benchmarks() -> dict[str, dict[str, dict[str, float]]]:
    benchmark_results: dict[str, dict[str, dict[str, float]]] = {
        "sample_logs": {},
        "large_logs": {},
    }

    datasets = {
        "sample_logs": DEFAULT_SAMPLE_LOG_PATH,
        "large_logs": DEFAULT_LARGE_LOG_PATH,
    }

    for dataset_name, dataset_path in datasets.items():
        for worker_count in BENCHMARK_WORKER_COUNTS:
            worker_key = f"workers_{worker_count}"
            benchmark_results[dataset_name][worker_key] = {}

            for chunk_size in BENCHMARK_CHUNK_SIZES:
                execution_time = benchmark_single_run(
                    log_path=dataset_path,
                    chunk_size=chunk_size,
                    worker_count=worker_count,
                )
                benchmark_results[dataset_name][worker_key][f"chunk_size_{chunk_size}"] = execution_time

    return benchmark_results


def save_benchmark_results(report: dict, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as file:
        json.dump(report, file, indent=4)


def plot_benchmark_results(results: dict[str, dict[str, dict[str, float]]], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    plt.figure(figsize=(10, 6))

    for dataset_name in results:
        for worker_key, worker_results in results[dataset_name].items():
            chunk_sizes = []
            execution_times = []

            for chunk_size in BENCHMARK_CHUNK_SIZES:
                chunk_key = f"chunk_size_{chunk_size}"
                chunk_sizes.append(chunk_size)
                execution_times.append(worker_results[chunk_key])

            label = f"{dataset_name} | {worker_key}"
            plt.plot(chunk_sizes, execution_times, marker="o", label=label)

    plt.xlabel("Chunk size")
    plt.ylabel("Execution time (seconds)")
    plt.title("MapReduce Benchmark Results")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(output_path)
    plt.close()


if __name__ == "__main__":
    benchmark_results = run_benchmarks()
    report = {
        "benchmark": benchmark_results
    }
    save_benchmark_results(report, DEFAULT_BENCHMARK_RESULT_PATH)
    plot_benchmark_results(benchmark_results, DEFAULT_BENCHMARK_PLOT_PATH)

    print("[OK] Benchmark completed")
    print(f"[OK] Results saved to: {DEFAULT_BENCHMARK_RESULT_PATH}")
    print(f"[OK] Plot saved to: {DEFAULT_BENCHMARK_PLOT_PATH}")