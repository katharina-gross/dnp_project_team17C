from __future__ import annotations

import json
import logging
import time
from dataclasses import asdict
from multiprocessing import Process, Queue
from pathlib import Path

from framework.config import (
    DEFAULT_CHUNK_SIZE,
    DEFAULT_SAMPLE_LOG_PATH,
    DEFAULT_WORKER_COUNT,
    LOG_FORMAT,
    LOG_LEVEL,
)
from framework.models import (
    FailedTaskResult,
    MapResult,
    MapTask,
    ReduceResult,
    ReduceTask,
    RunStats,
    StopTask,
)
from framework.splitter import read_lines, split_into_chunks
from worker.worker import worker_process


def setup_logging() -> None:
    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL),
        format=LOG_FORMAT,
    )


def start_workers(worker_count: int, task_queue: Queue, result_queue: Queue) -> list[Process]:
    workers: list[Process] = []

    for worker_index in range(worker_count):
        worker_name = f"worker-{worker_index + 1}"
        process = Process(
            target=worker_process,
            args=(task_queue, result_queue, worker_name),
            name=worker_name,
        )
        process.start()
        workers.append(process)

    return workers


def stop_workers(workers: list[Process], task_queue: Queue) -> None:
    for _ in workers:
        task_queue.put(StopTask())

    for worker in workers:
        worker.join()


def create_map_tasks(chunks: list[list[str]]) -> list[MapTask]:
    map_tasks: list[MapTask] = []

    for chunk_index, chunk in enumerate(chunks):
        task = MapTask(
            task_id=f"map_{chunk_index}",
            chunk_id=chunk_index,
            lines=chunk,
        )
        map_tasks.append(task)

    return map_tasks


def create_reduce_tasks(grouped_results: dict[str, list[int]]) -> list[ReduceTask]:
    reduce_tasks: list[ReduceTask] = []

    for reduce_index, (key, values) in enumerate(sorted(grouped_results.items())):
        task = ReduceTask(
            task_id=f"reduce_{reduce_index}",
            key=key,
            values=values,
        )
        reduce_tasks.append(task)

    return reduce_tasks


def run_map_stage(
    map_tasks: list[MapTask],
    task_queue: Queue,
    result_queue: Queue,
) -> tuple[list[tuple[str, int]], int]:
    all_mapped_pairs: list[tuple[str, int]] = []
    total_malformed_logs = 0

    for task in map_tasks:
        task_queue.put(task)

    completed_tasks = 0
    while completed_tasks < len(map_tasks):
        result = result_queue.get()

        if isinstance(result, FailedTaskResult):
            continue

        if not isinstance(result, MapResult):
            continue

        all_mapped_pairs.extend(result.mapped_pairs)
        total_malformed_logs += result.malformed_logs
        completed_tasks += 1

    return all_mapped_pairs, total_malformed_logs


def shuffle_results(mapped_results: list[tuple[str, int]]) -> dict[str, list[int]]:
    grouped_results: dict[str, list[int]] = {}

    for key, value in mapped_results:
        if key not in grouped_results:
            grouped_results[key] = []

        grouped_results[key].append(value)

    return grouped_results


def run_reduce_stage(
    reduce_tasks: list[ReduceTask],
    task_queue: Queue,
    result_queue: Queue,
) -> dict[str, int]:
    final_result: dict[str, int] = {}

    for task in reduce_tasks:
        task_queue.put(task)

    completed_tasks = 0
    while completed_tasks < len(reduce_tasks):
        result = result_queue.get()

        if isinstance(result, FailedTaskResult):
            continue

        if not isinstance(result, ReduceResult):
            continue

        final_result[result.key] = result.reduced_value
        completed_tasks += 1

    return dict(sorted(final_result.items()))


def save_json(data: dict, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as file:
        json.dump(data, file, indent=4)


def save_result(final_result: dict[str, int], output_path: Path) -> None:
    save_json(final_result, output_path)


def save_run_stats(stats: RunStats, output_path: Path) -> None:
    save_json(asdict(stats), output_path)


def run_job(
    log_path: Path,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    worker_count: int = DEFAULT_WORKER_COUNT,
    output_path: Path | None = None,
    stats_path: Path | None = None,
) -> tuple[dict[str, int], RunStats]:
    total_start_time = time.perf_counter()

    lines = read_lines(str(log_path))
    chunks = split_into_chunks(lines, chunk_size)

    stats = RunStats(
        input_file=str(log_path),
        worker_count=worker_count,
        chunk_size=chunk_size,
        total_lines=len(lines),
        map_tasks=len(chunks),
    )

    task_queue: Queue = Queue()
    result_queue: Queue = Queue()

    workers = start_workers(worker_count, task_queue, result_queue)

    try:
        map_tasks = create_map_tasks(chunks)

        map_start_time = time.perf_counter()
        mapped_results, malformed_logs = run_map_stage(map_tasks, task_queue, result_queue)
        map_end_time = time.perf_counter()

        grouped_results = shuffle_results(mapped_results)
        reduce_tasks = create_reduce_tasks(grouped_results)

        stats.malformed_logs = malformed_logs
        stats.reduce_tasks = len(reduce_tasks)
        stats.map_stage_seconds = map_end_time - map_start_time

        reduce_start_time = time.perf_counter()
        final_result = run_reduce_stage(reduce_tasks, task_queue, result_queue)
        reduce_end_time = time.perf_counter()

        stats.reduce_stage_seconds = reduce_end_time - reduce_start_time
        stats.total_seconds = time.perf_counter() - total_start_time

        if output_path is not None:
            save_result(final_result, output_path)

        if stats_path is not None:
            save_run_stats(stats, stats_path)

        return final_result, stats

    finally:
        stop_workers(workers, task_queue)


if __name__ == "__main__":
    setup_logging()

    result, stats = run_job(
        log_path=DEFAULT_SAMPLE_LOG_PATH,
        chunk_size=DEFAULT_CHUNK_SIZE,
        worker_count=DEFAULT_WORKER_COUNT,
    )

    print("[OK] Pipeline completed successfully")
    print(f"[OK] Result keys: {len(result)}")
    print(f"[OK] Total lines processed: {stats.total_lines}")