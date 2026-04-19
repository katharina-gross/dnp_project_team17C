from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal


TaskType = Literal["map", "reduce", "stop"]


@dataclass(slots=True)
class ParsedLogLine:
    ip: str
    timestamp: datetime
    status_code: int
    method: str
    path: str
    protocol: str
    size: int
    minute_bucket: str


@dataclass(slots=True)
class MapTask:
    task_id: str
    chunk_id: int
    lines: list[str]
    task_type: TaskType = "map"
    should_fail: bool = False


@dataclass(slots=True)
class ReduceTask:
    task_id: str
    key: str
    values: list[int]
    task_type: TaskType = "reduce"
    should_fail: bool = False


@dataclass(slots=True)
class StopTask:
    task_type: TaskType = "stop"


@dataclass(slots=True)
class MapResult:
    task_id: str
    chunk_id: int
    mapped_pairs: list[tuple[str, int]]
    malformed_logs: int
    completed: bool = True


@dataclass(slots=True)
class ReduceResult:
    task_id: str
    key: str
    reduced_value: int
    completed: bool = True


@dataclass(slots=True)
class FailedTaskResult:
    task_id: str
    task_type: str
    worker_name: str
    message: str
    completed: bool = False


@dataclass(slots=True)
class RunStats:
    input_file: str
    worker_count: int
    chunk_size: int
    total_lines: int = 0
    malformed_logs: int = 0
    map_tasks: int = 0
    reduce_tasks: int = 0
    map_stage_seconds: float = 0.0
    reduce_stage_seconds: float = 0.0
    total_seconds: float = 0.0
    extra: dict[str, str] = field(default_factory=dict)