from __future__ import annotations

import logging
from multiprocessing import Queue
from typing import Union

from framework.models import (
    FailedTaskResult,
    MapResult,
    MapTask,
    ReduceResult,
    ReduceTask,
    StopTask,
)
from jobs.error_frequency_job import map_line, reduce_values
from parsers.log_parser import parse_line


WorkerTask = Union[MapTask, ReduceTask, StopTask]
WorkerResult = Union[MapResult, ReduceResult, FailedTaskResult]


def run_map_task(task: MapTask) -> MapResult:
    mapped_results: list[tuple[str, int]] = []
    malformed_logs = 0

    for line in task.lines:
        parsed_line = parse_line(line)

        if parsed_line is None:
            malformed_logs += 1
            continue

        mapped_result = map_line(line)

        if mapped_result is not None:
            mapped_results.append(mapped_result)

    return MapResult(
        task_id=task.task_id,
        chunk_id=task.chunk_id,
        mapped_pairs=mapped_results,
        malformed_logs=malformed_logs,
        completed=True,
    )


def run_reduce_task(task: ReduceTask) -> ReduceResult:
    reduced_key, reduced_value = reduce_values(task.key, task.values)

    return ReduceResult(
        task_id=task.task_id,
        key=reduced_key,
        reduced_value=reduced_value,
        completed=True,
    )


def worker_process(
    task_queue: Queue,
    result_queue: Queue,
    worker_name: str,
) -> None:
    logger = logging.getLogger(worker_name)

    while True:
        task: WorkerTask = task_queue.get()

        if isinstance(task, StopTask):
            break

        if isinstance(task, MapTask):
            if task.should_fail:
                result_queue.put(
                    FailedTaskResult(
                        task_id=task.task_id,
                        task_type="map",
                        worker_name=worker_name,
                        message=f"Simulated failure in worker {worker_name} on task {task.task_id}",
                    )
                )
                continue

            result = run_map_task(task)
            result_queue.put(result)
            continue

        if isinstance(task, ReduceTask):
            if task.should_fail:
                result_queue.put(
                    FailedTaskResult(
                        task_id=task.task_id,
                        task_type="reduce",
                        worker_name=worker_name,
                        message=f"Simulated failure in worker {worker_name} on task {task.task_id}",
                    )
                )
                continue

            result = run_reduce_task(task)
            result_queue.put(result)
            continue