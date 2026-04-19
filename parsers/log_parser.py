"""
Expected log format:
IP - - [DD/Mon/YYYY:HH:MM:SS] "METHOD /path HTTP/1.1" STATUS SIZE
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Optional

from framework.models import ParsedLogLine


LOG_PATTERN = re.compile(
    r'^(?P<ip>\S+)\s+-\s+-\s+'
    r'\[(?P<timestamp>\d{2}/[A-Za-z]{3}/\d{4}:\d{2}:\d{2}:\d{2})\]\s+'
    r'"(?P<method>[A-Z]+)\s+(?P<path>\S+)\s+(?P<protocol>HTTP/\d\.\d)"\s+'
    r'(?P<status>\d{3})\s+'
    r'(?P<size>\d+)$'
)

TIMESTAMP_FORMAT = "%d/%b/%Y:%H:%M:%S"


def extract_timestamp(line: str) -> Optional[datetime]:
    match = LOG_PATTERN.match(line.strip())
    if match is None:
        return None

    try:
        return datetime.strptime(match.group("timestamp"), TIMESTAMP_FORMAT)
    except ValueError:
        return None


def extract_status_code(line: str) -> Optional[int]:
    match = LOG_PATTERN.match(line.strip())
    if match is None:
        return None

    try:
        return int(match.group("status"))
    except ValueError:
        return None


def normalize_to_minute(timestamp: datetime) -> str:
    return timestamp.strftime("%Y-%m-%d %H:%M")


def parse_line(line: str) -> Optional[ParsedLogLine]:
    line = line.strip()

    if not line:
        return None

    match = LOG_PATTERN.match(line)
    if match is None:
        return None

    try:
        parsed_timestamp = datetime.strptime(match.group("timestamp"), TIMESTAMP_FORMAT)
        parsed_status = int(match.group("status"))
        parsed_size = int(match.group("size"))
    except ValueError:
        return None

    return ParsedLogLine(
        ip=match.group("ip"),
        timestamp=parsed_timestamp,
        status_code=parsed_status,
        method=match.group("method"),
        path=match.group("path"),
        protocol=match.group("protocol"),
        size=parsed_size,
        minute_bucket=normalize_to_minute(parsed_timestamp),
    )


def is_valid_log_line(line: str) -> bool:
    return parse_line(line) is not None


def is_error_status(status_code: int) -> bool:
    return 400 <= status_code <= 599