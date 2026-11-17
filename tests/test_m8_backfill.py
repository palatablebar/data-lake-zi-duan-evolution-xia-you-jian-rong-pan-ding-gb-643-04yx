"""M8 迟到补发：暂存、游标续跑、幂等重跑、转正追加版本、边界一致。"""

from __future__ import annotations

import json

import pytest

from app import config, errors, store
from app.modules.m6_batches import service as batch_service
from app.modules.m8_backfill import boundary, service
from tests.fixtures import expected

FILES = ["backfill_2026_08_a.json", "backfill_2026_08_b.json"]

# 样例账：a 文件 6 行（1 行越月拒收），b 文件 4 行
ROWS_TOTAL = 10
ROWS_APPLIED = 9
ROWS_REJECTED = 1
AMOUNT_TOTAL = "717.00"


def _create(client) -> dict:
    response = client.post("/api/backfill/batches", json={"files": FILES})
    assert response.status_code == 200
    return response.json()


def _process_all(client, batch_id: str) -> dict:
    for _ in range(10):
        body = client.post(f"/api/backfill/batches/{batch_id}/process",
                           json={"max_lines": 50}).json()
        if body["status"] != "open":
            return body
    raise AssertionError("批次没有走完")


def test_create_batch_is_idempotent(client):
    first = _create(client)
    assert first["status"] == "open"
    assert first["target_month"] == "2026-08"
    assert first["caliber"] == "v2026_06"
    assert first["rows_total"] == ROWS_TOTAL

    again = client.post("/api/backfill/batches", json={"files": FILES}).json()
    assert again["batch_id"] == first["batch_id"]
    assert again["reused"] is True
    assert len(store.list_backfill_batches()) == 1


def test_process_chunks_and_cursor(client):
    batch_id = _create(client)["batch_id"]
    first = client.post(f"/api/backfill/batches/{batch_id}/process",
                        json={"max_lines": 3}).json()
    assert first["cursor"] == {"file_index": 0, "line": 3}
    assert first["totals"]["rows_done"] == 3
    assert first["status"] == "open"

    done = _process_all(client, batch_id)
    assert done["status"] == "staged"
    totals = done["totals"]
    assert totals["rows_done"] == ROWS_TOTAL
    assert totals["rows_applied"] == ROWS_APPLIED
    assert totals["rejected_rows"] == ROWS_REJECTED
    assert totals["amount_total"] == AMOUNT_TOTAL
    assert totals["by_table"]["fct_pay"] == {"rows": 5, "amount": "351.25"}
    assert [issue["reason"] for issue in totals["issues"]] == ["MONTH_MISMATCH"]


def test_interrupted_resume_matches_clean_totals(client, monkeypatch):
    """中断：第二段提交前抛错，游标停在第一段边界；续跑后账目与一气呵成一致。"""
    batch_id = _create(client)["batch_id"]
    original = store.apply_backfill_chunk
    state = {"calls": 0, "armed": True}

    def maybe_fail(*args, **kwargs):
        state["calls"] += 1
        if state["armed"] and state["calls"] == 2:
            raise RuntimeError("模拟进程崩溃")
        return original(*args, **kwargs)

    monkeypatch.setattr(store, "apply_backfill_chunk", maybe_fail)
    with pytest.raises(RuntimeError):
        client.post(f"/api/backfill/batches/{batch_id}/process",
                    json={"max_lines": 50})
    state["armed"] = False

    batch = store.get_backfill_batch(batch_id)
    assert batch["status"] == "open"
    assert batch["cursor"] == {"file_index": 1, "line": 0}
    assert batch["totals"]["rows_done"] == 6

    done = _process_all(client, batch_id)
    assert done["status"] == "staged"
    assert done["totals"]["rows_done"] == ROWS_TOTAL
    assert done["totals"]["rows_applied"] == ROWS_APPLIED
    assert done["totals"]["amount_total"] == AMOUNT_TOTAL


def test_rerun_same_file_never_double_counts(client):
    batch_id = _create(client)["batch_id"]
    done = _process_all(client, batch_id)

    # 已完整的批次再处理是只读的
    again = client.post(f"/api/backfill/batches/{batch_id}/process",
                        json={"max_lines": 50}).json()
    assert again["status"] == "staged"
    assert again["totals"] == done["totals"]

    # 同一批文件重新登记命中批次键，不重置游标与累计
    reused = client.post("/api/backfill/batches", json={"files": FILES}).json()
    assert reused["reused"] is True
    kept = store.get_backfill_batch(batch_id)
    assert kept["totals"]["rows_done"] == ROWS_TOTAL
    assert kept["totals"]["amount_total"] == AMOUNT_TOTAL


def test_promote_requires_staged(client):
    batch_id = _create(client)["batch_id"]
    response = client.post(f"/api/backfill/batches/{batch_id}/promote", json={})
    assert response.status_code == 409
    body = response.json()
    assert body["error"] == "BACKFILL_NOT_READY"
    assert body["detail"]["rows_done"] == 0
    assert store.count_runs() == 1


def test_promote_appends_version_and_never_overwrites(client):
    first_run = store.latest_run()
    batch_id = _create(client)["batch_id"]
    _process_all(client, batch_id)

    promoted = client.post(f"/api/backfill/batches/{batch_id}/promote", json={}).json()
    assert promoted["version_seq"] == first_run["version_seq"] + 1
    assert store.count_runs() == 2

    # 上一份完整结果原样保留
    kept = store.run_by_id(first_run["run_id"])
    assert kept is not None
    assert kept["effective_from"] == first_run["effective_from"]
    assert kept["summary"] == first_run["summary"]

    # 新版本覆盖目标月，窗口半开相邻
    new_run = store.run_by_id(promoted["run_id"])
    assert new_run["effective_from"] == "2026-08-01T00:00:00+00:00"
    assert new_run["effective_to"] == "2026-09-01T00:00:00+00:00"
    assert new_run["params"]["backfill"] == batch_id
    assert new_run["warnings"] == []

    # 已转正再转正：返回原结果，不新增行
    again = client.post(f"/api/backfill/batches/{batch_id}/promote", json={}).json()
    assert again["reused"] is True
    assert again["run_id"] == promoted["run_id"]
    assert store.count_runs() == 2


def test_promote_stale_baseline_rejected(client):
    batch_id = _create(client)["batch_id"]
    _process_all(client, batch_id)
    response = client.post(f"/api/backfill/batches/{batch_id}/promote",
                           json={"base_version_seq": 0})
    assert response.status_code == 409
    assert response.json()["error"] == "VERSION_STALE"
    assert store.count_runs() == 1


def test_boundary_same_on_page_and_downloads(client):
    batch_id = _create(client)["batch_id"]
    _process_all(client, batch_id)

    edge = client.get("/api/backfill/boundary").json()
    assert edge["official_version_seq"] == 1
    assert edge["staged_batches"] == 1
    assert edge["staged_rows"] == ROWS_APPLIED

    manifest = json.loads(client.get("/api/export/manifest.json").text)
    assert manifest["boundary"] == edge

    spec = client.get("/api/export/spec.md").text
    assert "## 7 版本边界" in spec
    assert f"| 待转正行数 | {ROWS_APPLIED} |" in spec

    ledger = client.get("/api/export/ledger.csv").text
    assert "staged_rows,9" in ledger

    page = client.get("/backfill").text
    assert "待转正" in page
    assert batch_id in page
    masthead = client.get("/batches").text
    assert "待转正 1 批" in masthead


def test_recalc_plan_is_depth_ordered(client):
    plan = client.get("/api/backfill/boundary").json()["recalc_plan"]
    assert len(plan) == expected.IMPACT_TOTAL
    depths = [row["depth"] for row in plan]
    assert depths == sorted(depths)
    assert depths[0] == 1
    assert [row["order"] for row in plan] == list(range(1, len(plan) + 1))


def test_month_not_late_rejected(client, tmp_path, monkeypatch):
    (tmp_path / "late_2026_09.json").write_text(json.dumps({
        "caliber": "v2026_06", "month": "2026-09",
        "records": [{"seq": 1, "table": "fct_pay", "day": "2026-09-01",
                     "metric": "pay_amt", "amount": "1.00"}],
    }), encoding="utf-8")
    monkeypatch.setattr(config, "SAMPLE_DIR", tmp_path)
    response = client.post("/api/backfill/batches",
                           json={"files": ["late_2026_09.json"]})
    assert response.status_code == 400
    assert response.json()["detail"]["reason"] == "MONTH_NOT_LATE"


def test_caliber_mismatch_rejected(client, tmp_path, monkeypatch):
    (tmp_path / "wrong_caliber.json").write_text(json.dumps({
        "caliber": "v2026_09", "month": "2026-08",
        "records": [{"seq": 1, "table": "fct_pay", "day": "2026-08-01",
                     "metric": "pay_amt", "amount": "1.00"}],
    }), encoding="utf-8")
    monkeypatch.setattr(config, "SAMPLE_DIR", tmp_path)
    response = client.post("/api/backfill/batches",
                           json={"files": ["wrong_caliber.json"]})
    assert response.status_code == 400
    assert response.json()["detail"]["reason"] == "CALIBER_MISMATCH"


def test_target_month_mismatch_rejected(client):
    response = client.post("/api/backfill/batches",
                           json={"target_month": "2026-07", "files": FILES})
    assert response.status_code == 400
    assert response.json()["detail"]["reason"] == "MONTH_MISMATCH"


def test_empty_file_list_rejected(client):
    response = client.post("/api/backfill/batches", json={"files": []})
    assert response.status_code == 400
    assert response.json()["error"] == "BAD_INPUT"


def test_unknown_batch_404_and_bad_id_400(client):
    assert client.post("/api/backfill/batches/bf-000000000000/process",
                       json={}).status_code == 404
    assert client.post("/api/backfill/batches/not-a-batch/process",
                       json={}).status_code == 400


def test_empty_client_pages_and_create(empty_client):
    page = empty_client.get("/backfill")
    assert page.status_code == 200
    assert "还没有补发批次" in page.text
    response = empty_client.post("/api/backfill/batches", json={"files": FILES})
    assert response.status_code == 404


def test_seeded_run_untouched_by_backfill_flow(client):
    """补发全流程走完后，种子批次的内容与计数不变。"""
    before = store.latest_run()
    batch_id = _create(client)["batch_id"]
    _process_all(client, batch_id)
    client.post(f"/api/backfill/batches/{batch_id}/promote", json={})
    kept = store.run_by_id(before["run_id"])
    assert kept["ledger"]["counts"] == before["ledger"]["counts"]
    assert kept["diff"] == before["diff"]
    assert batch_service.current_run()["version_seq"] == 2
