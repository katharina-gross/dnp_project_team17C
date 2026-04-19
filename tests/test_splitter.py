from __future__ import annotations

import pytest

from framework.splitter import split_into_chunks


def test_split_into_chunks_even_split() -> None:
    lines = ["line1", "line2", "line3", "line4"]

    chunks = split_into_chunks(lines, 2)

    assert chunks == [
        ["line1", "line2"],
        ["line3", "line4"],
    ]


def test_split_into_chunks_uneven_split() -> None:
    lines = ["line1", "line2", "line3", "line4", "line5"]

    chunks = split_into_chunks(lines, 2)

    assert chunks == [
        ["line1", "line2"],
        ["line3", "line4"],
        ["line5"],
    ]


def test_split_into_chunks_single_chunk() -> None:
    lines = ["line1", "line2"]

    chunks = split_into_chunks(lines, 10)

    assert chunks == [
        ["line1", "line2"],
    ]


def test_split_into_chunks_invalid_chunk_size() -> None:
    lines = ["line1", "line2"]

    with pytest.raises(ValueError, match="chunk_size must be greater than 0"):
        split_into_chunks(lines, 0)