"""重算顺序：影响面反向闭包按深度（最短跳数）升序，深度小的下游先重算。

断链节点在闭包里按直接受影响计入深度 1，顺序与影响面页一致。
"""

from __future__ import annotations


def recalc_plan(run: dict | None) -> list[dict]:
    if not run:
        return []
    rows = (((run.get("impact") or {}).get("impact") or {}).get("rows")) or []
    ordered = sorted(rows, key=lambda row: (row["depth"], row["name"]))
    return [
        {
            "order": index,
            "name": row["name"],
            "kind": row.get("kind") or "task",
            "depth": row["depth"],
            "via": row.get("via") or "field",
            "fields": row.get("fields") or [],
        }
        for index, row in enumerate(ordered, start=1)
    ]
