from __future__ import annotations

from parsers.log_parser import parse_line, is_error_status, normalize_to_minute


def test_parse_line_valid() -> None:
    line = '127.0.0.1 - - [02/Apr/2026:10:01:15] "GET /login HTTP/1.1" 404 321'

    parsed_line = parse_line(line)

    assert parsed_line is not None
    assert parsed_line.ip == "127.0.0.1"
    assert parsed_line.status_code == 404
    assert parsed_line.method == "GET"
    assert parsed_line.path == "/login"
    assert parsed_line.protocol == "HTTP/1.1"
    assert parsed_line.size == 321
    assert parsed_line.minute_bucket == "2026-04-02 10:01"


def test_parse_line_invalid() -> None:
    line = 'broken log line'

    parsed_line = parse_line(line)

    assert parsed_line is None


def test_is_error_status() -> None:
    assert is_error_status(404) is True
    assert is_error_status(500) is True
    assert is_error_status(200) is False
    assert is_error_status(302) is False


def test_normalize_to_minute() -> None:
    line = '127.0.0.1 - - [02/Apr/2026:10:01:15] "GET /login HTTP/1.1" 404 321'
    parsed_line = parse_line(line)

    assert parsed_line is not None
    assert normalize_to_minute(parsed_line.timestamp) == "2026-04-02 10:01"