from __future__ import annotations


def read_lines(file_path: str) -> list[str]:
    with open(file_path, "r", encoding="utf-8") as file:
        return file.readlines()


def split_into_chunks(lines: list[str], chunk_size: int) -> list[list[str]]:
    if chunk_size <= 0:
        raise ValueError("chunk_size must be greater than 0")

    chunks = []

    for i in range(0, len(lines), chunk_size):
        chunk = lines[i:i + chunk_size]
        chunks.append(chunk)

    return chunks


def split_file(file_path: str, chunk_size: int) -> list[list[str]]:
    lines = read_lines(file_path)
    return split_into_chunks(lines, chunk_size)