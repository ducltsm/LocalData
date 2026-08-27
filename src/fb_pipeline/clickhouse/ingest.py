"""Toàn bộ logic ingest: render nguồn đọc, DROP PARTITION, INSERT, QC, ingestion_log.

Nguyên tắc: Python không parse dữ liệu — chỉ render SQL để ClickHouse tự đọc Parquet.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, fields
from datetime import date, datetime
from pathlib import Path
from typing import Any

from clickhouse_connect.driver.client import Client

from fb_pipeline.clickhouse import source_schema
from fb_pipeline.config import Settings
from fb_pipeline.templating import render_template

log = logging.getLogger(__name__)

_DS_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def check_ds(ds: str) -> str:
    """Validate ds dạng YYYY-MM-DD trước khi nhúng vào SQL."""
    if not _DS_RE.match(ds):
        raise ValueError(f"ds phải có dạng YYYY-MM-DD, nhận được: {ds!r}")
    return ds


def _sql_str(value: str) -> str:
    """Escape giá trị string trước khi nhúng vào SQL literal."""
    return value.replace("\\", "\\\\").replace("'", "\\'")


# ---------------------------------------------------------------------------
# Đường dẫn staging
# ---------------------------------------------------------------------------
def staging_object_glob(settings: Settings, ds: str) -> str:
    """Glob object trên GCS nơi EXPORT DATA ghi ra (không gồm tên bucket)."""
    return f"{settings.gcs_staging_prefix}/dt={check_ds(ds)}/part-*.parquet"


def local_staging_dir(settings: Settings, ds: str) -> Path:
    """Thư mục staging local trong user_files (strategy `file`)."""
    return settings.ch_user_files_dir / "staging" / f"dt={check_ds(ds)}"


def local_glob(ds: str) -> str:
    """Glob mà file() dùng — tương đối so với user_files."""
    return f"staging/dt={check_ds(ds)}/*.parquet"


# ---------------------------------------------------------------------------
# Render SQL
# ---------------------------------------------------------------------------
def render_read_source(settings: Settings, ds: str, object_glob: str | None = None) -> str:
    """Render macro nguồn đọc (chỗ duy nhất định nghĩa nguồn — read_source.sql.j2).

    ``object_glob`` cho phép backfill trỏ vào prefix raw có sẵn thay vì staging.
    """
    check_ds(ds)
    if settings.ingest_strategy not in ("s3", "file"):
        raise ValueError(f"Strategy không hợp lệ: {settings.ingest_strategy!r}")
    rendered = render_template(
        settings.sql_dir / "ingest" / "read_source.sql.j2",
        strategy=settings.ingest_strategy,
        bucket=settings.gcs_bucket,
        object_glob=object_glob or staging_object_glob(settings, ds),
        local_glob=local_glob(ds),
        structure=source_schema.structure(),
    ).strip()
    if not rendered:
        raise ValueError("read_source render ra rỗng — kiểm tra strategy")
    return rendered


def render_insert_sql(
    settings: Settings,
    *,
    ds: str,
    run_id: str,
    source_table: str,
    is_intraday: int,
    object_glob: str | None = None,
) -> str:
    """Render câu INSERT duy nhất, liệt kê cột tường minh (không SELECT *)."""
    check_ds(ds)
    return render_template(
        settings.sql_dir / "ingest" / "insert_raw.sql.j2",
        database=settings.clickhouse_db,
        columns=source_schema.column_names(),
        ds=ds,
        run_id=_sql_str(run_id),
        source_table=_sql_str(source_table),
        is_intraday=1 if is_intraday else 0,
        read_source=render_read_source(settings, ds, object_glob=object_glob),
    )


def insert_settings(settings: Settings) -> dict[str, Any]:
    """Settings kèm MỌI lệnh đọc Parquet.

    Hai setting đầu để pipeline không vỡ khi Google thêm/bớt field trong export.
    Nhóm setting block để không OOM với ngày dữ liệu lớn (đã dính thật với ~800
    file / 7.5GB / 50M+ dòng): giới hạn block đọc từ Parquet, flush insert sớm,
    và bó số file đọc song song theo MAX_INSERT_THREADS.
    """
    return {
        "input_format_parquet_allow_missing_columns": 1,
        "input_format_null_as_default": 1,
        "input_format_parquet_case_insensitive_column_matching": 1,
        "max_insert_threads": settings.max_insert_threads,
        "max_threads": settings.max_insert_threads,
        "max_memory_usage": settings.max_memory_usage,
        "input_format_parquet_max_block_size": 8192,
        "min_insert_block_size_rows": 65536,
        "min_insert_block_size_bytes": 256 * 1024 * 1024,
    }


# ---------------------------------------------------------------------------
# Thao tác trên ClickHouse
# ---------------------------------------------------------------------------
def drop_partition(client: Client, database: str, ds: str, table: str = "events_raw") -> None:
    """Cơ chế idempotency duy nhất: xoá sạch partition của ds trước khi insert lại."""
    check_ds(ds)
    client.command(f"ALTER TABLE {database}.{table} DROP PARTITION '{ds}'")
    log.info("Đã DROP PARTITION '%s' trên %s.%s", ds, database, table)


def run_insert(client: Client, settings: Settings, sql: str) -> None:
    """Chạy câu INSERT với bộ settings đọc Parquet."""
    client.command(sql, settings=insert_settings(settings))


def partition_row_count(
    client: Client, database: str, ds: str, table: str = "events_raw"
) -> int:
    """Số dòng hiện có trong partition ds."""
    check_ds(ds)
    result = client.query(
        f"SELECT count() FROM {database}.{table} WHERE _dt = toDate('{ds}')"
    )
    return int(result.result_rows[0][0])


def qc_metrics(client: Client, database: str, ds: str) -> dict[str, int]:
    """Một query gom mọi chỉ số quality-check của partition."""
    check_ds(ds)
    row = client.query(
        f"""
        SELECT
            count()                                                AS rows,
            countIf(user_pseudo_id IS NULL OR user_pseudo_id = '') AS null_pseudo,
            countIf(empty(event_params))                           AS empty_params,
            uniqExact(event_name)                                  AS uniq_event_names,
            max(length(event_params))                              AS max_params_len
        FROM {database}.events_raw
        WHERE _dt = toDate('{ds}')
        """
    ).result_rows[0]
    return {
        "rows": int(row[0]),
        "null_pseudo": int(row[1]),
        "empty_params": int(row[2]),
        "uniq_event_names": int(row[3]),
        "max_params_len": int(row[4]),
    }


# ---------------------------------------------------------------------------
# ingestion_log
# ---------------------------------------------------------------------------
@dataclass
class IngestionLogRow:
    """Một dòng fb.ingestion_log — field khớp 03_ingestion_log.sql (đúng thứ tự)."""

    event_date: str
    source_table: str
    is_intraday: int
    strategy: str
    bq_row_count: int
    files_read: int
    bytes_read: int
    rows_inserted: int
    run_id: str
    started_at: datetime
    finished_at: datetime
    duration_sec: int
    status: str
    error_message: str


def write_ingestion_log(client: Client, database: str, row: IngestionLogRow) -> None:
    """Insert một dòng log (gọi cả khi run fail — trigger_rule=ALL_DONE)."""
    columns = [f.name for f in fields(IngestionLogRow)]
    values = [getattr(row, name) for name in columns]
    values[0] = date.fromisoformat(check_ds(row.event_date))
    client.insert(
        "ingestion_log",
        [values],
        column_names=columns,
        database=database,
    )
    log.info("Đã ghi ingestion_log: %s %s (%s)", row.event_date, row.status, row.run_id)
