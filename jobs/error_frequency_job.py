from __future__ import annotations

from typing import Optional

from parsers.log_parser import parse_line, is_error_status


def map_line(line: str) -> Optional[tuple[str, int]]:
    parsed_line = parse_line(line)

    if parsed_line is None:
        return None

    status_code = parsed_line.status_code
    minute_bucket = parsed_line.minute_bucket

    if not is_error_status(status_code):
        return None

    if 400 <= status_code <= 499:
        return f"{minute_bucket}_4xx", 1

    if 500 <= status_code <= 599:
        return f"{minute_bucket}_5xx", 1

    return None


def reduce_values(key: str, values: list[int]) -> tuple[str, int]:
    return key, sum(values)