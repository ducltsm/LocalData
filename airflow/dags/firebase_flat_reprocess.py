"""Reprocess flatten một dải ngày TỪ fb.events_raw — không đụng BigQuery/GCS.

Dùng khi: đổi quy ước cột, registry có key mới muốn áp ngược cho ngày cũ, hoặc
nghi flatten sai. Idempotent: mỗi ngày là DROP PARTITION fb.events_flat + insert lại.

Params khi trigger: date_from / date_to (YYYY-MM-DD, inclusive).
"""

from __future__ import annotations

import logging

import pendulum
from airflow.decorators import dag, task
from airflow.models.param import Param

log = logging.getLogger(__name__)


@dag(
    dag_id="firebase_flat_reprocess",
    description="Flatten lại fb.events_flat từ fb.events_raw theo dải ngày",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data", "retries": 0},
    params={
        "date_from": Param("2026-08-27", type="string", description="YYYY-MM-DD (inclusive)"),
        "date_to": Param("2026-08-27", type="string", description="YYYY-MM-DD (inclusive)"),
    },
    tags=["firebase", "clickhouse", "flat", "reprocess"],
)
def firebase_flat_reprocess() -> None:
    """Định nghĩa DAG."""

    @task
    def run_reprocess(run_id: str | None = None, params: dict | None = None) -> dict:
        """Loop tuần tự date_from..date_to, chỉ những ngày có dữ liệu raw."""
        from datetime import date, timedelta

        from fb_pipeline.clickhouse.client import get_client
        from fb_pipeline.clickhouse.flat import flatten_day
        from fb_pipeline.clickhouse.ingest import partition_row_count
        from fb_pipeline.config import load_settings

        assert params is not None
        date_from = date.fromisoformat(str(params["date_from"]))
        date_to = date.fromisoformat(str(params["date_to"]))
        if date_from > date_to:
            raise ValueError(f"date_from {date_from} > date_to {date_to}")

        settings = load_settings()
        client = get_client(settings)
        summary: dict[str, list[str]] = {"success": [], "skipped": [], "failed": []}
        current = date_from
        while current <= date_to:
            ds = current.isoformat()
            try:
                if partition_row_count(client, settings.clickhouse_db, ds) == 0:
                    log.warning("Partition raw %s rỗng — bỏ qua", ds)
                    summary["skipped"].append(ds)
                else:
                    result = flatten_day(client, settings, ds, run_id or "")
                    log.info("%s: %s", ds, result)
                    summary["success"].append(ds)
            except Exception:
                log.exception("Reprocess %s FAILED — tiếp tục ngày kế", ds)
                summary["failed"].append(ds)
            current += timedelta(days=1)

        log.info("Tổng kết reprocess: %s", summary)
        if summary["failed"]:
            raise RuntimeError(f"Reprocess có ngày lỗi: {summary['failed']}")
        return summary

    run_reprocess()


firebase_flat_reprocess()
