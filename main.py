from __future__ import annotations

import argparse
import subprocess
import sys
from dataclasses import asdict

from coordinator.master import run_job, setup_logging
from experiments.benchmark import run_benchmarks, save_benchmark_results, plot_benchmark_results
from experiments.failure_demo import run_failure_demo
from experiments.validation import run_validation
from framework.config import (
    DEFAULT_BENCHMARK_PLOT_PATH,
    DEFAULT_BENCHMARK_RESULT_PATH,
    DEFAULT_CHUNK_SIZE,
    DEFAULT_SAMPLE_LOG_PATH,
    DEFAULT_WORKER_COUNT,
)


def run_default_pipeline() -> tuple[dict[str, int], dict]:
    result, stats = run_job(
        log_path=DEFAULT_SAMPLE_LOG_PATH,
        chunk_size=DEFAULT_CHUNK_SIZE,
        worker_count=DEFAULT_WORKER_COUNT,
    )

    print("[OK] Pipeline completed successfully")
    return result, asdict(stats)


def run_benchmark_mode() -> dict:
    benchmark_results = run_benchmarks()
    report = {
        "benchmark": benchmark_results
    }
    save_benchmark_results(report, DEFAULT_BENCHMARK_RESULT_PATH)
    plot_benchmark_results(benchmark_results, DEFAULT_BENCHMARK_PLOT_PATH)

    print("[OK] Benchmark completed")
    print(f"[OK] Results saved to: {DEFAULT_BENCHMARK_RESULT_PATH}")
    print(f"[OK] Plot saved to: {DEFAULT_BENCHMARK_PLOT_PATH}")

    return benchmark_results


def run_test_mode() -> bool:
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "-q"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )

    if result.returncode == 0:
        print("[OK] All tests passed")
        return True

    print("[FAIL] Some tests failed")
    return False


def run_all_mode() -> None:
    print("\n=== RUN MODE ===")
    run_result, run_stats = run_default_pipeline()

    print("\n=== VALIDATION MODE ===")
    validation_ok = run_validation()

    print("\n=== BENCHMARK MODE ===")
    benchmark_results = run_benchmarks()
    plot_benchmark_results(benchmark_results, DEFAULT_BENCHMARK_PLOT_PATH)

    combined_report = {
        "run_summary": {
            "input_file": str(DEFAULT_SAMPLE_LOG_PATH),
            "worker_count": DEFAULT_WORKER_COUNT,
            "chunk_size": DEFAULT_CHUNK_SIZE,
            "result": run_result,
            "stats": run_stats,
        },
        "validation": {
            "passed": validation_ok,
        },
        "benchmark": benchmark_results,
    }

    save_benchmark_results(combined_report, DEFAULT_BENCHMARK_RESULT_PATH)

    print("[OK] Benchmark completed")
    print(f"[OK] Results saved to: {DEFAULT_BENCHMARK_RESULT_PATH}")
    print(f"[OK] Plot saved to: {DEFAULT_BENCHMARK_PLOT_PATH}")

    print("\n=== FAILURE DEMO MODE ===")
    run_failure_demo()

    print("\n=== TEST MODE ===")
    tests_ok = run_test_mode()

    print("\n=== FINAL SUMMARY ===")
    if validation_ok and tests_ok:
        print("[OK] All stages completed successfully")
    elif not validation_ok:
        print("[WARN] Completed, but validation failed")
    else:
        print("[WARN] Completed, but some tests failed")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Mini MapReduce framework for distributed log analysis."
    )
    parser.add_argument(
        "--mode",
        choices=["run", "validation", "benchmark", "failure", "test", "all"],
        default="run",
        help="Execution mode.",
    )

    args = parser.parse_args()

    setup_logging()

    if args.mode == "run":
        run_default_pipeline()
        return

    if args.mode == "validation":
        run_validation()
        return

    if args.mode == "benchmark":
        run_benchmark_mode()
        return

    if args.mode == "failure":
        run_failure_demo()
        return

    if args.mode == "test":
        run_test_mode()
        return

    if args.mode == "all":
        run_all_mode()
        return


if __name__ == "__main__":
    main()