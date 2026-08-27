"""Bảo trì ClickHouse hàng tuần (Chủ nhật 03:00): OPTIMIZE partition tuần trước
+ báo cáo system.parts / system.columns, cảnh báo khi vượt ngưỡng.
"""

from __future__ import annotations

import logging

import pendulum
from airflow.decorators import dag, task

log = logging.getLogger(__name__)

# Ngưỡng cảnh báo
MAX_PARTS_PER_PARTITION = 30
MAX_TABLE_DISK_GB = 100


@dag(
    dag_id="clickhouse_maintenance",
    description="OPTIMIZE partition tuần trước + báo cáo dung lượng fb.events_raw",
    schedule="0 3 * * 0",
    start_date=pendulum.datetime(2026, 8, 26, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data", "retries": 1},
    tags=["clickhouse", "maintenance"],
)
def clickhouse_maintenance() -> None:
    """Định nghĩa DAG."""

    @task
    def optimize_last_week(logical_date: pendulum.DateTime | None = None) -> list[str]:
        """OPTIMIZE ... FINAL các partition 7 ngày trước đó (chỉ những partition tồn tại)."""
        import os

        from fb_pipeline.clickhouse.client import get_client
        from fb_pipeline.config import load_settings

        # cùng quy ước với firebase_raw_daily: ngày theo timezone của DAG, không dùng ds (UTC)
        ds = logical_date.in_timezone(os.environ.get("TZ", "Asia/Ho_Chi_Minh")).strftime(
            "%Y-%m-%d"
        )
        settings = load_settings()
        client = get_client(settings)
        result = client.query(
            f"SELECT DISTINCT partition FROM system.parts "
            f"WHERE database = '{settings.clickhouse_db}' AND table = 'events_raw' "
            f"AND active AND partition >= toString(toDate('{ds}') - 7) "
            f"AND partition < toString(toDate('{ds}')) ORDER BY partition"
        )
        partitions = [str(row[0]) for row in result.result_rows]
        for partition in partitions:
            log.info("OPTIMIZE partition %s ...", partition)
            client.command(
                f"OPTIMIZE TABLE {settings.clickhouse_db}.events_raw "
                f"PARTITION '{partition}' FINAL"
            )
        log.info("Đã OPTIMIZE %d partition: %s", len(partitions), partitions)
        return partitions

    @task
    def report_storage() -> None:
        """Log số part + dung lượng theo partition và theo cột; cảnh báo vượt ngưỡng."""
        from fb_pipeline.clickhouse.client import get_client
        from fb_pipeline.config import load_settings

        settings = load_settings()
        client = get_client(settings)

        parts = client.query(
            f"SELECT partition, count() AS n_parts, sum(rows) AS rows, "
            f"formatReadableSize(sum(bytes_on_disk)) AS size, sum(bytes_on_disk) AS bytes "
            f"FROM system.parts "
            f"WHERE database = '{settings.clickhouse_db}' AND table = 'events_raw' AND active "
            f"GROUP BY partition ORDER BY partition"
        ).result_rows
        total_bytes = 0
        for partition, n_parts, rows, size, bytes_ in parts:
            total_bytes += int(bytes_)
            log.info("partition %s: %d part, %d dòng, %s", partition, n_parts, rows, size)
            if int(n_parts) > MAX_PARTS_PER_PARTITION:
                log.warning(
                    "partition %s có %d part (> %d) — merge chưa theo kịp insert",
                    partition, n_parts, MAX_PARTS_PER_PARTITION,
                )
        if total_bytes > MAX_TABLE_DISK_GB * 1024**3:
            log.warning(
                "fb.events_raw chiếm %.1f GiB (> %d GiB) — cân nhắc RAW_TTL_DAYS",
                total_bytes / 1024**3, MAX_TABLE_DISK_GB,
            )

        columns = client.query(
            f"SELECT name, formatReadableSize(data_compressed_bytes) "
            f"FROM system.columns "
            f"WHERE database = '{settings.clickhouse_db}' AND table = 'events_raw' "
            f"ORDER BY data_compressed_bytes DESC LIMIT 15"
        ).result_rows
        # event_params thường chiếm phần lớn — đúng như kỳ vọng với bảng raw nested
        for name, size in columns:
            log.info("cột %s: %s", name, size)

    optimize_last_week() >> report_storage()


clickhouse_maintenance()
