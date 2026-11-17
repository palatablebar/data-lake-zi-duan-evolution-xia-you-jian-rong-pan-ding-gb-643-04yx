"""迟到补发编排：登记 → 分片处理（游标可恢复）→ 转正进正式结果。

三条硬规则：
1. 暂存不等于正式：批次在 open / staged 状态只进暂存区，不计入正式结果。
2. 游标随已提交分片前进：中断后从游标继续，重跑同一文件不重复累计。
3. 转正只追加新版本：已有正式批次永不被改写，已转正批次再转正返回原结果。
"""

from __future__ import annotations

import json
from datetime import datetime
from decimal import Decimal

from app import config, errors, manifest as manifest_mod, store
from app.modules.m1_intake import reader
from app.modules.m6_batches import service as batch_service
from app.modules.m6_batches import guard, window as window_mod
from app.modules.m8_backfill import boundary, cursor as cursor_mod, plan as plan_mod, records

STATUS_LABELS = {"open": "处理中", "staged": "待转正", "promoted": "已转正"}
MAX_ISSUES = 20


def month_range(month: str) -> tuple[str, str]:
    """目标月 → 半开区间 [月初, 次月初)。"""
    match = config.BACKFILL_MONTH_RE.match((month or "").strip())
    if not match:
        raise errors.BadInput("目标月份格式必须是 YYYY-MM",
                              detail={"field": "target_month", "value": month})
    year, mon = int(match.group("year")), int(match.group("month"))
    start = datetime(year, mon, 1, tzinfo=config.UTC)
    end = datetime(year + (1 if mon == 12 else 0), 1 if mon == 12 else mon + 1, 1,
                   tzinfo=config.UTC)
    return start.isoformat(), end.isoformat()


def _load_file(name: str) -> dict:
    path = config.resolve_sample(name)
    if path.suffix.lower() != ".json":
        raise errors.BadInput("补发文件只接受 .json", detail={"file": name})
    raw = reader.read_meta_file(path)
    doc = records.parse_file(raw.text, name)
    return {"name": raw.name, "sha256": raw.sha256, **doc}


def _batch_key(month: str, caliber: str, files: list[dict]) -> str:
    payload = json.dumps(
        {
            "month": month,
            "caliber": caliber,
            "ruleset": config.RULESET_VERSION,
            "files": [{"name": item["name"], "sha256": item["sha256"]}
                      for item in sorted(files, key=lambda row: row["name"])],
        },
        ensure_ascii=False, sort_keys=True, separators=(",", ":"),
    )
    return manifest_mod.sha256_bytes(payload.encode("utf-8"))


def create_batch(target_month: str | None = None, files: list[str] | None = None) -> dict:
    """登记补发批次：同一批文件 + 同一目标月命中批次键，直接复用不重置。"""
    run = store.latest_run()
    if not run:
        raise errors.NotFound("还没有正式批次，先在版本历史页跑一次")
    names = list(files) if files is not None else list(config.DEFAULT_BACKFILL_FILES)
    if not names or len(names) > 8:
        raise errors.BadInput("补发文件一次 1 到 8 份", detail={"count": len(names)})
    if len(set(names)) != len(names):
        raise errors.BadInput("补发文件有重复", detail={"files": names})

    parsed = [_load_file(name) for name in names]
    months = {item["month"] for item in parsed}
    if target_month and months != {target_month}:
        raise errors.BadInput(
            "补发文件月份与目标月不一致",
            detail={"reason": "MONTH_MISMATCH", "target_month": target_month,
                    "file_months": sorted(months)},
        )
    if len(months) != 1:
        raise errors.BadInput("补发文件月份不一致，无法确定目标月",
                              detail={"reason": "MONTH_MISMATCH",
                                      "file_months": sorted(months)})
    month = target_month or months.pop()

    caliber = run.get("version_label_old") or ""
    for item in parsed:
        if item["caliber"] != caliber:
            raise errors.BadInput(
                f"补发文件口径 {item['caliber']} 与登记口径 {caliber} 不符",
                detail={"reason": "CALIBER_MISMATCH", "file": item["name"],
                        "declared": item["caliber"], "expected": caliber},
            )

    start_iso, end_iso = month_range(month)
    official_from = window_mod.parse_instant(run["effective_from"], "effective_from")
    if window_mod.parse_instant(end_iso, "month_end") > official_from:
        raise errors.BadInput(
            "目标月份不在正式生效起点之前，请走正式批次",
            detail={"reason": "MONTH_NOT_LATE", "target_month": month,
                    "official_from": run["effective_from"]},
        )

    key = _batch_key(month, caliber, parsed)
    existing = store.find_backfill_batch_by_key(key)
    if existing is not None:
        reused = dict(existing)
        reused["reused"] = True
        return reused

    now = config.now_utc().isoformat()
    batch = {
        "batch_id": "bf-" + key[:12],
        "batch_key": key,
        "target_month": month,
        "caliber": caliber,
        "status": "open",
        "cursor": cursor_mod.initial(),
        "totals": {
            "rows_total": 0,
            "rows_done": 0,
            "rows_applied": 0,
            "rejected_rows": 0,
            "amount_total": "0",
            "by_table": {},
            "issues": [],
        },
        "plan": [],
        "promoted_run_id": None,
        "created_at": now,
        "updated_at": now,
        "reused": False,
    }
    file_rows = []
    for index, item in enumerate(sorted(parsed, key=lambda row: row["name"])):
        outcomes = records.validate_rows(item["records"], month)
        batch["totals"]["rows_total"] += len(outcomes)
        file_rows.append({
            "file_index": index,
            "file_name": item["name"],
            "file_sha256": item["sha256"],
            "rows_total": len(outcomes),
            "rows_done": 0,
            "rejected_rows": 0,
            "status": "pending",
        })
    store.save_backfill_batch(batch, file_rows)
    return batch


def _merge_totals(totals: dict, delta: dict) -> dict:
    merged = dict(totals)
    merged["rows_done"] = int(totals["rows_done"]) + delta["rows"]
    merged["rows_applied"] = int(totals["rows_applied"]) + delta["applied"]
    merged["rejected_rows"] = int(totals["rejected_rows"]) + delta["rejected"]
    merged["amount_total"] = format(
        Decimal(totals["amount_total"]) + delta["amount"], "f")
    by_table = {table: dict(entry) for table, entry in (totals.get("by_table") or {}).items()}
    for table, entry in delta["by_table"].items():
        current = by_table.setdefault(table, {"rows": 0, "amount": "0"})
        current["rows"] = int(current["rows"]) + entry["rows"]
        current["amount"] = format(Decimal(current["amount"]) + Decimal(entry["amount"]), "f")
    merged["by_table"] = by_table
    merged["issues"] = (list(totals.get("issues") or []) + delta["issues"])[:MAX_ISSUES]
    return merged


def _apply_chunk(batch: dict, file_row: dict, start: int, count: int) -> dict:
    """处理一段并落库；解析或校验失败时什么都不提交，游标停在原处。"""
    path = config.resolve_sample(file_row["file_name"])
    raw = reader.read_meta_file(path)
    if raw.sha256 != file_row["file_sha256"]:
        raise errors.Conflict(
            "补发文件内容与登记摘要不符",
            detail={"reason": "FILE_CHANGED", "file": file_row["file_name"]},
        )
    doc = records.parse_file(raw.text, file_row["file_name"])
    outcomes = records.validate_rows(doc["records"], batch["target_month"])
    if len(outcomes) != int(file_row["rows_total"]):
        raise errors.Conflict(
            "补发文件行数与登记不符",
            detail={"reason": "FILE_CHANGED", "file": file_row["file_name"]},
        )

    delta = {"rows": 0, "applied": 0, "rejected": 0, "amount": Decimal("0"),
             "by_table": {}, "issues": []}
    for outcome in outcomes[start:start + count]:
        delta["rows"] += 1
        if outcome["ok"]:
            row = outcome["row"]
            delta["applied"] += 1
            delta["amount"] += Decimal(row["amount"])
            entry = delta["by_table"].setdefault(
                row["table"], {"rows": 0, "amount": Decimal("0")})
            entry["rows"] += 1
            entry["amount"] += Decimal(row["amount"])
        else:
            delta["rejected"] += 1
            delta["issues"].append({
                "file": file_row["file_name"], "seq": outcome["seq"],
                "reason": outcome["reason"], "detail": outcome["detail"],
            })
    for entry in delta["by_table"].values():
        entry["amount"] = format(entry["amount"], "f")

    rows_done = start + count
    file_status = "done" if rows_done >= int(file_row["rows_total"]) else "processing"
    batch["cursor"] = cursor_mod.advance(
        int(file_row["file_index"]), rows_done, int(file_row["rows_total"]))
    batch["totals"] = _merge_totals(batch["totals"], delta)
    batch["updated_at"] = config.now_utc().isoformat()
    store.apply_backfill_chunk(
        batch, int(file_row["file_index"]), file_row["file_sha256"],
        rows_done, int(file_row["rejected_rows"]) + delta["rejected"], file_status)
    return batch


def process_batch(batch_id: str, max_lines: int | None = None) -> dict:
    """推进一个分片预算；已 staged / promoted 的批次是只读的，直接返回。"""
    batch = store.get_backfill_batch(batch_id)
    if batch is None:
        raise errors.NotFound("补发批次不存在", detail={"batch_id": batch_id})
    if batch["status"] in ("staged", "promoted"):
        return batch
    budget = config.BACKFILL_CHUNK if max_lines is None else max_lines
    if not isinstance(budget, int) or isinstance(budget, bool) or not 1 <= budget <= 500:
        raise errors.BadInput("max_lines 必须是 1 到 500 的整数",
                              detail={"value": repr(max_lines)})

    for chunk in cursor_mod.plan_chunks(store.list_backfill_files(batch_id), budget):
        files = {row["file_index"]: row
                 for row in store.list_backfill_files(batch_id)}
        batch = _apply_chunk(batch, files[chunk["file_index"]],
                             chunk["start"], chunk["count"])

    files = store.list_backfill_files(batch_id)
    if files and all(row["status"] == "done" for row in files):
        batch["status"] = "staged"
        batch["plan"] = plan_mod.recalc_plan(store.latest_run())
        batch["updated_at"] = config.now_utc().isoformat()
        store.finalize_backfill_batch(batch)
    return batch


def promote_batch(batch_id: str, base_version_seq=None) -> dict:
    """转正：暂存完整的批次追加成一个新正式版本，不改写已有批次。"""
    batch = store.get_backfill_batch(batch_id)
    if batch is None:
        raise errors.NotFound("补发批次不存在", detail={"batch_id": batch_id})
    if batch["status"] == "promoted":
        run = store.run_by_id(batch["promoted_run_id"]) or {}
        return {"batch_id": batch_id, "run_id": batch["promoted_run_id"],
                "version_seq": run.get("version_seq"), "reused": True,
                "boundary": boundary.current()}
    if batch["status"] != "staged":
        totals = batch.get("totals") or {}
        raise errors.BackfillNotReady(
            "补发批次未完整，不能转正",
            detail={"batch_id": batch_id, "status": batch["status"],
                    "rows_done": totals.get("rows_done"),
                    "rows_total": totals.get("rows_total"),
                    "reason": "BACKFILL_NOT_READY"},
        )

    guard.check_stale(base_version_seq, store.current_version_seq())
    start_iso, end_iso = month_range(batch["target_month"])
    run = batch_service.execute(
        effective_from=start_iso,
        effective_to=end_iso,
        params={"backfill": batch_id, "target_month": batch["target_month"],
                "caliber": batch["caliber"]},
    )
    batch["status"] = "promoted"
    batch["promoted_run_id"] = run["run_id"]
    batch["updated_at"] = config.now_utc().isoformat()
    store.mark_backfill_promoted(batch)
    return {"batch_id": batch_id, "run_id": run["run_id"],
            "version_seq": run["version_seq"], "reused": bool(run.get("reused")),
            "boundary": boundary.current()}


def listing() -> list[dict]:
    out = []
    for batch in store.list_backfill_batches():
        files = store.list_backfill_files(batch["batch_id"])
        out.append({
            **batch,
            "status_label": STATUS_LABELS.get(batch["status"], batch["status"]),
            "files": files,
            "files_done": sum(1 for row in files if row["status"] == "done"),
        })
    return out
