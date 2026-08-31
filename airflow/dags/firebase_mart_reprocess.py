"""Rebuild các bảng mart_* một dải ngày TỪ fb.events_flat — không đụng BigQuery/GCS.

Dùng khi: thêm/sửa metric trong MART_TABLES, hoặc sau khi reprocess flatten
(firebase_flat_reprocess) làm mart cũ lệch. Idempotent: mỗi ngày là DROP
PARTITION từng bảng mart + insert lại từ events_flat.

Params khi trigger: date_from / date_to (YYYY-MM-DD, inclusive).
"""

from __future__ import annotations

import logging

import pendulum
from airflow.decorators import dag, task
from airflow.models.param import Param

log = logging.getLogger(__name__)


@dag(
    dag_id="firebase_mart_reprocess",
    description="Rebuild các bảng mart_* từ fb.events_flat theo dải ngày",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data", "retries": 0},
    params={
        "date_from": Param("2026-08-27", type="string", description="YYYY-MM-DD (inclusive)"),
        "date_to": Param("2026-08-27", type="string", description="YYYY-MM-DD (inclusive)"),
    },
    tags=["firebase", "clickhouse", "mart", "reprocess"],
)
def firebase_mart_reprocess() -> None:
    """Định nghĩa DAG."""

    @task
    def run_reprocess(run_id: str | None = None, params: dict | None = None) -> dict:
        """Loop tuần tự date_from..date_to, chỉ những ngày có dữ liệu flat."""
        from datetime import date, timedelta

        from fb_pipeline.clickhouse.client import get_client
        from fb_pipeline.clickhouse.ingest import partition_row_count
        from fb_pipeline.clickhouse.mart import build_mart_day
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
                if partition_row_count(client, settings.clickhouse_db, ds, table="events_flat") == 0:
                    log.warning("Partition flat %s rỗng — bỏ qua", ds)
                    summary["skipped"].append(ds)
                else:
                    result = build_mart_day(client, settings, ds, run_id or "")
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


firebase_mart_reprocess()
