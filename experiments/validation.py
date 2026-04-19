from __future__ import annotations

import json
import tempfile
from pathlib import Path

from coordinator.master import run_job
from framework.config import (
    DEFAULT_CHUNK_SIZE,
    DEFAULT_TEST_EXPECTED_PATH,
    DEFAULT_TEST_LOG_PATH,
    DEFAULT_WORKER_COUNT,
)


def load_expected_result(file_path: str) -> dict[str, int]:
    with open(file_path, "r", encoding="utf-8") as file:
        return json.load(file)


def validate_results(actual: dict[str, int], expected: dict[str, int]) -> bool:
    return actual == expected


def run_validation() -> bool:
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir_path = Path(temp_dir)

        actual_result, stats = run_job(
            log_path=DEFAULT_TEST_LOG_PATH,
            chunk_size=DEFAULT_CHUNK_SIZE,
            worker_count=DEFAULT_WORKER_COUNT,
            output_path=temp_dir_path / "validation_result.json",
            stats_path=temp_dir_path / "validation_stats.json",
        )

    expected_result = load_expected_result(str(DEFAULT_TEST_EXPECTED_PATH))
    is_valid = validate_results(actual_result, expected_result)

    if is_valid:
        print("[OK] Validation passed")
    else:
        print("[FAIL] Validation failed")

    print(
        f"[OK] Validation stats: total_lines={stats.total_lines}, "
        f"malformed_logs={stats.malformed_logs}, "
        f"map_tasks={stats.map_tasks}, reduce_tasks={stats.reduce_tasks}"
    )

    return is_valid


if __name__ == "__main__":
    run_validation()