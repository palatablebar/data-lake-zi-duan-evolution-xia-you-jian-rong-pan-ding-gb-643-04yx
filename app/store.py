"""SQLite 读写：批次快照、运行清单、隔离行、键值状态。

只放数据访问，不放判定；调用方拿到的是普通字典。
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
from pathlib import Path

from app import config, errors

_LOCK = threading.Lock()

SCHEMA = """
CREATE TABLE IF NOT EXISTS snapshots (
    schema_id      TEXT PRIMARY KEY,
    version_label  TEXT NOT NULL,
    source_path    TEXT NOT NULL,
    source_sha256  TEXT NOT NULL,
    encoding       TEXT NOT NULL,
    bom            INTEGER NOT NULL,
    field_count    INTEGER NOT NULL,
    blocked_count  INTEGER NOT NULL,
    created_at     TEXT NOT NULL,
    payload_json   TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS runs (
    run_id         TEXT PRIMARY KEY,
    batch_key      TEXT NOT NULL UNIQUE,
    old_schema_id  TEXT NOT NULL,
    new_schema_id  TEXT NOT NULL,
    version_seq    INTEGER NOT NULL,
    input_sha256   TEXT NOT NULL,
    ruleset        TEXT NOT NULL,
    created_at     TEXT NOT NULL,
    effective_from TEXT NOT NULL,
    effective_to   TEXT,
    summary_json   TEXT NOT NULL,
    payload_json   TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS quarantine (
    schema_id     TEXT NOT NULL,
    line          INTEGER,
    table_name    TEXT,
    column_name   TEXT,
    reason        TEXT NOT NULL,
    blocking      INTEGER NOT NULL,
    detail        TEXT,
    raw           TEXT
);
CREATE TABLE IF NOT EXISTS kv (
    k TEXT PRIMARY KEY,
    v TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS backfill_batches (
    batch_id        TEXT PRIMARY KEY,
    batch_key       TEXT NOT NULL UNIQUE,
    target_month    TEXT NOT NULL,
    caliber         TEXT NOT NULL,
    status          TEXT NOT NULL,
    cursor_json     TEXT NOT NULL,
    totals_json     TEXT NOT NULL,
    plan_json       TEXT,
    promoted_run_id TEXT,
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL,
    payload_json    TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS backfill_files (
    batch_id     TEXT NOT NULL,
    file_index   INTEGER NOT NULL,
    file_name    TEXT NOT NULL,
    file_sha256  TEXT NOT NULL,
    rows_total   INTEGER NOT NULL,
    rows_done    INTEGER NOT NULL,
    rejected_rows INTEGER NOT NULL,
    status       TEXT NOT NULL,
    PRIMARY KEY (batch_id, file_index)
);
CREATE INDEX IF NOT EXISTS idx_quarantine_schema ON quarantine(schema_id);
"""


def db_path() -> Path:
    raw = os.environ.get("FIELDMAP_DB", "").strip()
    path = Path(raw) if raw else config.DEFAULT_DB
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(db_path(), timeout=15)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db() -> None:
    with _LOCK, _connect() as conn:
        conn.executescript(SCHEMA)


def reset() -> None:
    """测试与重建用：清空全部业务表。"""
    with _LOCK, _connect() as conn:
        conn.executescript(SCHEMA)
        for table in ("snapshots", "runs", "quarantine", "kv",
                      "backfill_batches", "backfill_files"):
            conn.execute(f"DELETE FROM {table}")


def _dumps(value) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _loads(value):
    if not value:
        return None
    return json.loads(value)


# ---------- 快照 ----------

def save_snapshot(snapshot: dict) -> None:
    with _LOCK, _connect() as conn:
        conn.execute(
            """INSERT INTO snapshots (schema_id, version_label, source_path, source_sha256,
                   encoding, bom, field_count, blocked_count, created_at, payload_json)
               VALUES (?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(schema_id) DO UPDATE SET
                   version_label=excluded.version_label,
                   source_path=excluded.source_path,
                   source_sha256=excluded.source_sha256,
                   encoding=excluded.encoding,
                   bom=excluded.bom,
                   field_count=excluded.field_count,
                   blocked_count=excluded.blocked_count,
                   created_at=excluded.created_at,
                   payload_json=excluded.payload_json""",
            (
                snapshot["schema_id"], snapshot["version_label"], snapshot["source_path"],
                snapshot["source_sha256"], snapshot["encoding"], int(bool(snapshot["bom"])),
                int(snapshot["field_count"]), int(snapshot["blocked_count"]),
                snapshot["created_at"], _dumps(snapshot),
            ),
        )


def load_snapshot(schema_id: str) -> dict | None:
    with _LOCK, _connect() as conn:
        row = conn.execute(
            "SELECT payload_json FROM snapshots WHERE schema_id=?", (schema_id,)
        ).fetchone()
    return _loads(row["payload_json"]) if row else None


def list_snapshots(limit: int = 20) -> list[dict]:
    with _LOCK, _connect() as conn:
        rows = conn.execute(
            """SELECT schema_id, version_label, source_path, source_sha256, encoding, bom,
                      field_count, blocked_count, created_at
               FROM snapshots ORDER BY created_at DESC, schema_id LIMIT ?""",
            (limit,),
        ).fetchall()
    return [dict(row) for row in rows]


def save_quarantine(schema_id: str, rows: list[dict]) -> None:
    with _LOCK, _connect() as conn:
        conn.execute("DELETE FROM quarantine WHERE schema_id=?", (schema_id,))
        conn.executemany(
            """INSERT INTO quarantine (schema_id, line, table_name, column_name, reason,
                   blocking, detail, raw) VALUES (?,?,?,?,?,?,?,?)""",
            [
                (
                    schema_id, int(row.get("line") or 0), row.get("table") or "",
                    row.get("column") or "", row["reason"], int(bool(row.get("blocking"))),
                    row.get("detail") or "", row.get("raw") or "",
                )
                for row in rows
            ],
        )


def list_quarantine(schema_id: str) -> list[dict]:
    with _LOCK, _connect() as conn:
        rows = conn.execute(
            """SELECT line, table_name, column_name, reason, blocking, detail, raw
               FROM quarantine WHERE schema_id=? ORDER BY line, reason""",
            (schema_id,),
        ).fetchall()
    return [
        {
            "line": row["line"],
            "table": row["table_name"],
            "column": row["column_name"],
            "reason": row["reason"],
            "blocking": bool(row["blocking"]),
            "detail": row["detail"],
            "raw": row["raw"],
        }
        for row in rows
    ]


# ---------- 运行清单 ----------

def save_run(run: dict) -> None:
    with _LOCK, _connect() as conn:
        conn.execute(
            """INSERT INTO runs (run_id, batch_key, old_schema_id, new_schema_id, version_seq,
                   input_sha256, ruleset, created_at, effective_from, effective_to,
                   summary_json, payload_json)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                run["run_id"], run["batch_key"], run["old_schema_id"], run["new_schema_id"],
                int(run["version_seq"]), run["input_sha256"], run["ruleset"],
                run["created_at"], run["effective_from"], run["effective_to"],
                _dumps(run["summary"]), _dumps(run),
            ),
        )


def find_run_by_key(batch_key: str) -> dict | None:
    with _LOCK, _connect() as conn:
        row = conn.execute(
            "SELECT payload_json FROM runs WHERE batch_key=?", (batch_key,)
        ).fetchone()
    return _loads(row["payload_json"]) if row else None


def run_by_id(run_id: str) -> dict | None:
    with _LOCK, _connect() as conn:
        row = conn.execute(
            "SELECT payload_json FROM runs WHERE run_id=?", (run_id,)
        ).fetchone()
    return _loads(row["payload_json"]) if row else None


def latest_run() -> dict | None:
    with _LOCK, _connect() as conn:
        row = conn.execute(
            "SELECT payload_json FROM runs ORDER BY version_seq DESC LIMIT 1"
        ).fetchone()
    return _loads(row["payload_json"]) if row else None


def list_runs(limit: int = 30) -> list[dict]:
    with _LOCK, _connect() as conn:
        rows = conn.execute(
            """SELECT summary_json FROM runs ORDER BY version_seq DESC, run_id LIMIT ?""",
            (limit,),
        ).fetchall()
    return [_loads(row["summary_json"]) for row in rows]


def count_runs() -> int:
    with _LOCK, _connect() as conn:
        row = conn.execute("SELECT COUNT(*) AS n FROM runs").fetchone()
    return int(row["n"])


def next_version_seq() -> int:
    with _LOCK, _connect() as conn:
        row = conn.execute("SELECT COALESCE(MAX(version_seq), 0) AS n FROM runs").fetchone()
    return int(row["n"]) + 1


def current_version_seq() -> int:
    with _LOCK, _connect() as conn:
        row = conn.execute("SELECT COALESCE(MAX(version_seq), 0) AS n FROM runs").fetchone()
    return int(row["n"])


def list_windows() -> list[dict]:
    with _LOCK, _connect() as conn:
        rows = conn.execute(
            "SELECT run_id, effective_from, effective_to FROM runs ORDER BY version_seq"
        ).fetchall()
    return [dict(row) for row in rows]


# ---------- 迟到补发 ----------

def save_backfill_batch(batch: dict, files: list[dict]) -> None:
    """登记批次与文件清单，一个事务落库；批次键冲突由调用方先查复用。"""
    with _LOCK, _connect() as conn:
        conn.execute(
            """INSERT INTO backfill_batches (batch_id, batch_key, target_month, caliber,
                   status, cursor_json, totals_json, plan_json, promoted_run_id,
                   created_at, updated_at, payload_json)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                batch["batch_id"], batch["batch_key"], batch["target_month"],
                batch["caliber"], batch["status"], _dumps(batch["cursor"]),
                _dumps(batch["totals"]), _dumps(batch.get("plan") or []),
                batch.get("promoted_run_id"), batch["created_at"], batch["updated_at"],
                _dumps(batch),
            ),
        )
        conn.executemany(
            """INSERT INTO backfill_files (batch_id, file_index, file_name, file_sha256,
                   rows_total, rows_done, rejected_rows, status)
               VALUES (?,?,?,?,?,?,?,?)""",
            [
                (
                    batch["batch_id"], int(row["file_index"]), row["file_name"],
                    row["file_sha256"], int(row["rows_total"]), int(row["rows_done"]),
                    int(row["rejected_rows"]), row["status"],
                )
                for row in files
            ],
        )


def _backfill_payload(row) -> dict | None:
    return _loads(row["payload_json"]) if row else None


def get_backfill_batch(batch_id: str) -> dict | None:
    with _LOCK, _connect() as conn:
        row = conn.execute(
            "SELECT payload_json FROM backfill_batches WHERE batch_id=?", (batch_id,)
        ).fetchone()
    return _backfill_payload(row)


def find_backfill_batch_by_key(batch_key: str) -> dict | None:
    with _LOCK, _connect() as conn:
        row = conn.execute(
            "SELECT payload_json FROM backfill_batches WHERE batch_key=?", (batch_key,)
        ).fetchone()
    return _backfill_payload(row)


def list_backfill_batches(limit: int = 50) -> list[dict]:
    with _LOCK, _connect() as conn:
        rows = conn.execute(
            """SELECT payload_json FROM backfill_batches
               ORDER BY created_at DESC, batch_id LIMIT ?""",
            (limit,),
        ).fetchall()
    return [_loads(row["payload_json"]) for row in rows]


def list_backfill_files(batch_id: str) -> list[dict]:
    with _LOCK, _connect() as conn:
        rows = conn.execute(
            """SELECT file_index, file_name, file_sha256, rows_total, rows_done,
                      rejected_rows, status
               FROM backfill_files WHERE batch_id=? ORDER BY file_index""",
            (batch_id,),
        ).fetchall()
    return [dict(row) for row in rows]


def apply_backfill_chunk(batch: dict, file_index: int, file_sha256: str,
                         rows_done: int, rejected_rows: int, file_status: str) -> None:
    """一个分片的落库：文件进度与批次游标/累计同事务提交。

    中断发生在提交前则整段回滚，游标停在上一段边界，重跑不重复累计。
    """
    with _LOCK, _connect() as conn:
        cur = conn.execute(
            """UPDATE backfill_files SET rows_done=?, rejected_rows=?, status=?
               WHERE batch_id=? AND file_index=? AND file_sha256=?""",
            (
                int(rows_done), int(rejected_rows), file_status,
                batch["batch_id"], int(file_index), file_sha256,
            ),
        )
        if cur.rowcount != 1:
            raise errors.Conflict(
                "补发文件状态与登记不符",
                detail={"batch_id": batch["batch_id"], "file_index": file_index,
                        "reason": "FILE_CHANGED"},
            )
        conn.execute(
            """UPDATE backfill_batches SET status=?, cursor_json=?, totals_json=?,
                   plan_json=?, updated_at=?, payload_json=? WHERE batch_id=?""",
            (
                batch["status"], _dumps(batch["cursor"]), _dumps(batch["totals"]),
                _dumps(batch.get("plan") or []), batch["updated_at"], _dumps(batch),
                batch["batch_id"],
            ),
        )


def finalize_backfill_batch(batch: dict) -> None:
    """open → staged：全部文件处理完，挂上重算顺序，进入待转正。"""
    with _LOCK, _connect() as conn:
        conn.execute(
            """UPDATE backfill_batches SET status=?, plan_json=?, updated_at=?,
                   payload_json=? WHERE batch_id=?""",
            (
                batch["status"], _dumps(batch.get("plan") or []), batch["updated_at"],
                _dumps(batch), batch["batch_id"],
            ),
        )


def mark_backfill_promoted(batch: dict) -> None:
    """open/staged → promoted 的状态迁移；已被别人转正时报冲突而不是覆盖。"""
    with _LOCK, _connect() as conn:
        cur = conn.execute(
            """UPDATE backfill_batches SET status=?, promoted_run_id=?, updated_at=?,
                   payload_json=? WHERE batch_id=? AND status='staged'""",
            (
                batch["status"], batch["promoted_run_id"], batch["updated_at"],
                _dumps(batch), batch["batch_id"],
            ),
        )
        if cur.rowcount != 1:
            raise errors.Conflict(
                "补发批次状态已变化，请重新读取",
                detail={"batch_id": batch["batch_id"], "reason": "BACKFILL_STATE"},
            )


# ---------- 键值 ----------

def set_kv(key: str, value) -> None:
    with _LOCK, _connect() as conn:
        conn.execute(
            "INSERT INTO kv (k, v) VALUES (?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v",
            (key, _dumps(value)),
        )


def get_kv(key: str, default=None):
    with _LOCK, _connect() as conn:
        row = conn.execute("SELECT v FROM kv WHERE k=?", (key,)).fetchone()
    return _loads(row["v"]) if row else default
