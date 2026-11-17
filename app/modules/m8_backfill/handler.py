"""M8 入口：补发批次页、批次登记 / 处理 / 转正、版本边界接口。"""

from __future__ import annotations

import re

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from app import config, errors, store, web
from app.modules.m8_backfill import boundary, service

router = APIRouter()

BATCH_ID_RE = re.compile(r"^bf-[0-9a-f]{12}$")
CREATE_KEYS = {"target_month", "files"}
PROCESS_KEYS = {"max_lines"}
PROMOTE_KEYS = {"base_version_seq"}


def _check_unknown(body: dict, allowed: set[str]) -> None:
    unknown = set(body) - allowed
    if unknown:
        raise errors.BadInput("出现未知参数", detail={"keys": sorted(unknown)})


def _batch_id(raw: str) -> str:
    if not BATCH_ID_RE.match(raw or ""):
        raise errors.BadInput("补发批次号格式不对", detail={"batch_id": raw})
    return raw


@router.get("/backfill", response_class=HTMLResponse)
def backfill_page(request: Request):
    run = store.latest_run()
    return web.page_or_404(
        request,
        "backfill.html",
        "backfill",
        run,
        boundary=boundary.current(),
        batches=service.listing(),
        default_files=config.DEFAULT_BACKFILL_FILES,
    )


@router.get("/api/backfill/boundary")
def boundary_api() -> dict:
    return boundary.current()


@router.get("/api/backfill/batches")
def batches_api() -> dict:
    return {"batches": service.listing()}


@router.post("/api/backfill/batches")
def create_batch(payload: dict | None = None):
    body = payload or {}
    _check_unknown(body, CREATE_KEYS)
    files = body.get("files")
    if files is not None:
        if not isinstance(files, list) or any(not isinstance(item, str) for item in files):
            raise errors.BadInput("files 必须是文件名数组", detail={"field": "files"})
    target_month = body.get("target_month")
    if target_month is not None and not isinstance(target_month, str):
        raise errors.BadInput("target_month 必须是字符串",
                              detail={"field": "target_month"})
    batch = service.create_batch(target_month=target_month, files=files)
    totals = batch.get("totals") or {}
    return {
        "batch_id": batch["batch_id"],
        "reused": bool(batch.get("reused")),
        "status": batch["status"],
        "target_month": batch["target_month"],
        "caliber": batch["caliber"],
        "rows_total": totals.get("rows_total"),
    }


@router.post("/api/backfill/batches/{batch_id}/process")
def process_batch(batch_id: str, payload: dict | None = None):
    body = payload or {}
    _check_unknown(body, PROCESS_KEYS)
    batch = service.process_batch(_batch_id(batch_id), body.get("max_lines"))
    return {
        "batch_id": batch["batch_id"],
        "status": batch["status"],
        "cursor": batch["cursor"],
        "totals": batch["totals"],
        "promoted_run_id": batch.get("promoted_run_id"),
    }


@router.post("/api/backfill/batches/{batch_id}/promote")
def promote_batch(batch_id: str, payload: dict | None = None):
    body = payload or {}
    _check_unknown(body, PROMOTE_KEYS)
    return service.promote_batch(_batch_id(batch_id), body.get("base_version_seq"))
