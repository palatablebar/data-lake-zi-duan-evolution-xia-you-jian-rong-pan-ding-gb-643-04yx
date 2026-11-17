"""补发文件解析与行级校验：结构错误拒文件，数据错误拒行，纯函数不落库。

文件是 JSON：{"caliber": 口径版本, "month": "YYYY-MM", "records": [...]}。
每条记录 {seq, table, day, metric, amount}；amount 走 Decimal，不进 float。
"""

from __future__ import annotations

import json
from datetime import datetime
from decimal import Decimal, InvalidOperation

from app import errors

RECORD_KEYS = ("seq", "table", "day", "metric", "amount")


def parse_file(text: str, name: str) -> dict:
    """解析文件级结构；缺 caliber / month / records 直接拒收整份文件。"""
    try:
        doc = json.loads(text)
    except json.JSONDecodeError as exc:
        raise errors.BadInput("补发文件不是合法 JSON",
                              detail={"file": name, "line": exc.lineno})
    if not isinstance(doc, dict):
        raise errors.BadInput("补发文件必须是 JSON 对象", detail={"file": name})
    caliber = doc.get("caliber")
    month = doc.get("month")
    records = doc.get("records")
    if not isinstance(caliber, str) or not caliber.strip():
        raise errors.BadInput("补发文件缺口径 caliber", detail={"file": name})
    if not isinstance(month, str) or not month.strip():
        raise errors.BadInput("补发文件缺月份 month", detail={"file": name})
    if not isinstance(records, list):
        raise errors.BadInput("补发文件 records 必须是数组", detail={"file": name})
    return {
        "caliber": caliber.strip(),
        "month": month.strip(),
        "records": records,
    }


def _canonical_amount(raw) -> str:
    if isinstance(raw, bool) or not isinstance(raw, (str, int, float)):
        raise ValueError("amount 类型不对")
    try:
        value = Decimal(str(raw))
    except InvalidOperation:
        raise ValueError("amount 不是数值")
    if not value.is_finite():
        raise ValueError("amount 不是有限数值")
    return format(value, "f")


def validate_rows(records: list, month: str) -> list[dict]:
    """逐行校验，返回与 records 等长的结果序列，顺序即处理顺序。

    结果形如 {"ok": True, "row": {...}} 或 {"ok": False, "seq": n, "reason": ..., "detail": ...}；
    重跑同一文件时按同一顺序回放，已提交的段不重算。
    """
    year, mon = int(month[:4]), int(month[5:7])
    outcomes: list[dict] = []
    for position, record in enumerate(records, start=1):
        seq = record.get("seq") if isinstance(record, dict) else None
        label = seq if isinstance(seq, int) and not isinstance(seq, bool) else position

        def reject(reason: str, detail: str) -> dict:
            return {"ok": False, "seq": label, "reason": reason, "detail": detail}

        if not isinstance(record, dict) or any(key not in record for key in RECORD_KEYS):
            outcomes.append(reject("LINE_MALFORMED", "记录缺键或不是对象"))
            continue
        try:
            amount = _canonical_amount(record["amount"])
        except ValueError as exc:
            outcomes.append(reject("LINE_MALFORMED", str(exc)))
            continue
        day_raw = record["day"]
        try:
            day = datetime.strptime(str(day_raw), "%Y-%m-%d")
        except (TypeError, ValueError):
            outcomes.append(reject("LINE_MALFORMED", f"day 无法识别：{day_raw!r}"))
            continue
        if (day.year, day.month) != (year, mon):
            outcomes.append(reject("MONTH_MISMATCH", f"{day_raw} 不在 {month}"))
            continue
        table = str(record["table"] or "").strip()
        metric = str(record["metric"] or "").strip()
        if not table or not metric:
            outcomes.append(reject("LINE_MALFORMED", "table 或 metric 为空"))
            continue
        outcomes.append({
            "ok": True,
            "row": {
                "seq": label,
                "table": table,
                "day": day.strftime("%Y-%m-%d"),
                "metric": metric,
                "amount": amount,
            },
        })
    return outcomes
