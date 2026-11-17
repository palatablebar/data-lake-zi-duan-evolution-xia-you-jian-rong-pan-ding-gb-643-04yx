"""版本边界：正式结果到哪一版、多少补发待转正、重算顺序。

这是边界的唯一出处：页面页眉、补发页、版本历史页与全部导出件都取这一份，
保证页面和下载件展示同一份版本边界。对象只由库内状态推导，不含时钟，
同一份数据任何时候渲染都相同。
"""

from __future__ import annotations

from app import store
from app.modules.m8_backfill import plan as plan_mod

SEMANTICS = "正式结果只含已转正批次；暂存与处理中的补发不计入"


def current() -> dict:
    run = store.latest_run()
    batches = store.list_backfill_batches()
    staged = [item for item in batches if item["status"] == "staged"]
    open_ = [item for item in batches if item["status"] == "open"]
    return {
        "official_version_seq": (run or {}).get("version_seq") or 0,
        "official_run_id": (run or {}).get("run_id"),
        "official_label": (run or {}).get("version_label_new"),
        "official_window": (
            {"from": run["effective_from"], "to": run.get("effective_to")}
            if run else None
        ),
        "staged_batches": len(staged),
        "staged_rows": sum(int((item.get("totals") or {}).get("rows_applied") or 0)
                           for item in staged),
        "open_batches": len(open_),
        "open_rows_done": sum(int((item.get("totals") or {}).get("rows_done") or 0)
                              for item in open_),
        "recalc_plan": plan_mod.recalc_plan(run),
        "semantics": SEMANTICS,
    }
