"""补发游标：位置 = (文件序号, 该文件已处理行数)，分片计划与推进，纯函数。

游标只随已提交的分片前进；中断后从游标继续，已提交的行不重算。
"""

from __future__ import annotations


def initial() -> dict:
    return {"file_index": 0, "line": 0}


def plan_chunks(files: list[dict], budget: int) -> list[dict]:
    """按各文件 rows_done 给出本次要处理的段：{file_index, start, count}。"""
    chunks: list[dict] = []
    left = budget
    for row in sorted(files, key=lambda item: item["file_index"]):
        if left <= 0:
            break
        remaining = int(row["rows_total"]) - int(row["rows_done"])
        if remaining <= 0:
            continue
        take = min(remaining, left)
        chunks.append({
            "file_index": int(row["file_index"]),
            "start": int(row["rows_done"]),
            "count": take,
        })
        left -= take
    return chunks


def advance(file_index: int, rows_done: int, rows_total: int) -> dict:
    """提交一段后的新游标：文件没处理完指向下一段，处理完指向下一文件。"""
    if rows_done >= rows_total:
        return {"file_index": file_index + 1, "line": 0}
    return {"file_index": file_index, "line": rows_done}
