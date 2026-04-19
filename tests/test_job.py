from __future__ import annotations

from jobs.error_frequency_job import map_line, reduce_values


def test_map_line_returns_4xx_bucket() -> None:
    line = '127.0.0.1 - - [02/Apr/2026:10:01:15] "GET /login HTTP/1.1" 404 321'

    result = map_line(line)

    assert result == ("2026-04-02 10:01_4xx", 1)


def test_map_line_returns_5xx_bucket() -> None:
    line = '127.0.0.1 - - [02/Apr/2026:10:01:15] "GET /login HTTP/1.1" 500 321'

    result = map_line(line)

    assert result == ("2026-04-02 10:01_5xx", 1)


def test_map_line_returns_none_for_non_error() -> None:
    line = '127.0.0.1 - - [02/Apr/2026:10:01:15] "GET /login HTTP/1.1" 200 321'

    result = map_line(line)

    assert result is None


def test_map_line_returns_none_for_malformed_log() -> None:
    line = "broken log line"

    result = map_line(line)

    assert result is None


def test_reduce_values_sums_correctly() -> None:
    key = "2026-04-02 10:01_4xx"
    values = [1, 1, 1]

    result = reduce_values(key, values)

    assert result == ("2026-04-02 10:01_4xx", 3)