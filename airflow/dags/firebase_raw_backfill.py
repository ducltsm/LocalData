"""Backfill thủ công một dải ngày, tuần tự (max_active_runs=1).

Params khi trigger:
- date_from / date_to : YYYY-MM-DD (inclusive)
- use_existing_gcs    : true (mặc định) = đọc thẳng prefix raw đã có sẵn trên bucket
                        (GCS_RAW_PREFIX), tự detect layout, KHÔNG export BigQuery;
                        false = export BigQuery -> staging như DAG daily.

Toàn bộ logic một-ngày nằm ở fb_pipeline.backfill.process_day (DAG chỉ loop).
"""

from __future__ import annotations

import logging

import pendulum
from airflow.decorators import dag, task
from airflow.models.param import Param

log = logging.getLogger(__name__)


@dag(
    dag_id="firebase_raw_backfill",
    description="Backfill fb.events_raw theo dải ngày, tuần tự từng ngày",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data", "retries": 0},
    params={
        "date_from": Param("2026-08-27", type="string", description="YYYY-MM-DD (inclusive)"),
        "date_to": Param("2026-08-27", type="string", description="YYYY-MM-DD (inclusive)"),
        "use_existing_gcs": Param(
            True,
            type="boolean",
            description="true = đọc prefix raw có sẵn (GCS_RAW_PREFIX), bỏ qua export BQ",
        ),
    },
    tags=["firebase", "clickhouse", "raw", "backfill"],
)
def firebase_raw_backfill() -> None:
    """Định nghĩa DAG."""

    @task
    def run_backfill(run_id: str | None = None, params: dict | None = None) -> dict:
        """Loop tuần tự date_from..date_to; lỗi ngày nào ghi log ngày đó rồi đi tiếp."""
        from datetime import date, timedelta

        from fb_pipeline.backfill import process_day
        from fb_pipeline.config import load_settings

        assert params is not None
        date_from = date.fromisoformat(str(params["date_from"]))
        date_to = date.fromisoformat(str(params["date_to"]))
        if date_from > date_to:
            raise ValueError(f"date_from {date_from} > date_to {date_to}")
        use_existing_gcs = bool(params["use_existing_gcs"])

        settings = load_settings()
        summary: dict[str, list[str]] = {"success": [], "skipped": [], "failed": []}
        current = date_from
        while current <= date_to:
            ds = current.isoformat()
            try:
                status = process_day(settings, ds, run_id or "", use_existing_gcs)
                summary[status].append(ds)
            except Exception:
                log.exception("Backfill ngày %s FAILED — tiếp tục ngày kế", ds)
                summary["failed"].append(ds)
            current += timedelta(days=1)

        log.info("Tổng kết backfill: %s", summary)
        if summary["failed"]:
            raise RuntimeError(f"Backfill có ngày lỗi: {summary['failed']} (xem log từng ngày)")
        return summary

    run_backfill()


firebase_raw_backfill()
