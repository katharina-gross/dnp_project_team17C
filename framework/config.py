from __future__ import annotations

from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent

DATA_DIR = PROJECT_ROOT / "data"
RESULTS_DIR = PROJECT_ROOT / "results"
FINAL_RESULTS_DIR = RESULTS_DIR / "final"

DEFAULT_SAMPLE_LOG_PATH = DATA_DIR / "sample_logs.txt"
DEFAULT_LARGE_LOG_PATH = DATA_DIR / "large_logs.txt"
DEFAULT_TEST_LOG_PATH = DATA_DIR / "test_logs.txt"
DEFAULT_TEST_EXPECTED_PATH = DATA_DIR / "test_expected.json"

DEFAULT_BENCHMARK_RESULT_PATH = FINAL_RESULTS_DIR / "benchmark.json"
DEFAULT_BENCHMARK_PLOT_PATH = FINAL_RESULTS_DIR / "benchmark_plot.png"
DEFAULT_RECOVERY_LOG_PATH = FINAL_RESULTS_DIR / "recovery_log.txt"

DEFAULT_CHUNK_SIZE = 50
DEFAULT_WORKER_COUNT = 4

BENCHMARK_CHUNK_SIZES = [10, 50, 100]
BENCHMARK_WORKER_COUNTS = [1, 2, 4]

MAP_TASK_TYPE = "map"
REDUCE_TASK_TYPE = "reduce"
STOP_TASK_TYPE = "stop"

LOG_FORMAT = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
LOG_LEVEL = "ERROR"