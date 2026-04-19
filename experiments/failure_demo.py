from __future__ import annotations

import json
import time
from dataclasses import asdict
from multiprocessing import Process, Queue

from coordinator.master import (
    create_map_tasks,
    create_reduce_tasks,
    setup_logging,
    shuffle_results,
    start_workers,
    stop_workers,
)
from framework.config import (
    DEFAULT_CHUNK_SIZE,
    DEFAULT_RECOVERY_LOG_PATH,
    DEFAULT_SAMPLE_LOG_PATH,
    DEFAULT_WORKER_COUNT,
)
from framework.models import FailedTaskResult, MapResult, ReduceResult, RunStats
from framework.splitter import read_lines, split_into_chunks
from worker.worker import worker_process


def restart_dead_workers(
    workers: list[Process],
    task_queue: Queue,
    result_queue: Queue,
) -> list[Process]:
    active_workers: list[Process] = []

    for worker in workers:
        if worker.is_alive():
            active_workers.append(worker)
            continue

        replacement_name = f"{worker.name}-restarted"
        replacement = Process(
            target=worker_process,
            args=(task_queue, result_queue, replacement_name),
            name=replacement_name,
        )
        replacement.start()
        active_workers.append(replacement)

    return active_workers


def run_failure_demo() -> None:
    lines = read_lines(str(DEFAULT_SAMPLE_LOG_PATH))
    chunks = split_into_chunks(lines, DEFAULT_CHUNK_SIZE)

    map_tasks = create_map_tasks(chunks)
    if map_tasks:
        map_tasks[0].should_fail = True

    stats = RunStats(
        input_file=str(DEFAULT_SAMPLE_LOG_PATH),
        worker_count=DEFAULT_WORKER_COUNT,
        chunk_size=DEFAULT_CHUNK_SIZE,
        total_lines=len(lines),
        map_tasks=len(map_tasks),
    )

    task_queue: Queue = Queue()
    result_queue: Queue = Queue()

    workers = start_workers(DEFAULT_WORKER_COUNT, task_queue, result_queue)

    all_mapped_pairs: list[tuple[str, int]] = []
    total_malformed_logs = 0
    completed_task_ids: set[str] = set()
    failed_task = map_tasks[0] if map_tasks else None

    recovery_messages: list[str] = []

    total_start_time = time.perf_counter()

    try:
        for task in map_tasks:
            task_queue.put(task)

        while len(completed_task_ids) < len(map_tasks):
            workers = restart_dead_workers(workers, task_queue, result_queue)

            while not result_queue.empty():
                result = result_queue.get()

                if isinstance(result, FailedTaskResult):
                    if failed_task is not None and result.task_id == failed_task.task_id:
                        recovery_message = (
                            f"Detected failed map task: {failed_task.task_id}. "
                            f"Reassigning without failure flag."
                        )
                        recovery_messages.append(recovery_message)
                        failed_task.should_fail = False
                        task_queue.put(failed_task)
                        failed_task = None
                    continue

                if not isinstance(result, MapResult):
                    continue

                if result.task_id in completed_task_ids:
                    continue

                completed_task_ids.add(result.task_id)
                all_mapped_pairs.extend(result.mapped_pairs)
                total_malformed_logs += result.malformed_logs

            time.sleep(0.05)

        stats.malformed_logs = total_malformed_logs
        stats.map_stage_seconds = time.perf_counter() - total_start_time

        grouped_results = shuffle_results(all_mapped_pairs)
        reduce_tasks = create_reduce_tasks(grouped_results)
        stats.reduce_tasks = len(reduce_tasks)

        reduce_start_time = time.perf_counter()

        for task in reduce_tasks:
            task_queue.put(task)

        final_result: dict[str, int] = {}
        completed_reduce_tasks = 0

        while completed_reduce_tasks < len(reduce_tasks):
            result = result_queue.get()

            if isinstance(result, FailedTaskResult):
                continue

            if not isinstance(result, ReduceResult):
                continue

            final_result[result.key] = result.reduced_value
            completed_reduce_tasks += 1

        stats.reduce_stage_seconds = time.perf_counter() - reduce_start_time
        stats.total_seconds = time.perf_counter() - total_start_time

        final_result = dict(sorted(final_result.items()))

        DEFAULT_RECOVERY_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(DEFAULT_RECOVERY_LOG_PATH, "w", encoding="utf-8") as file:
            file.write("Failure demo recovery log\n")
            file.write("=" * 40 + "\n")
            for message in recovery_messages:
                file.write(message + "\n")
            file.write("\nFinal result:\n")
            file.write(json.dumps(final_result, indent=4))
            file.write("\n\nRun stats:\n")
            file.write(json.dumps(asdict(stats), indent=4))

        print("[OK] Failure demo completed")
        print(f"[OK] Recovery log saved to: {DEFAULT_RECOVERY_LOG_PATH}")

    finally:
        stop_workers(workers, task_queue)


if __name__ == "__main__":
    setup_logging()
    run_failure_demo()