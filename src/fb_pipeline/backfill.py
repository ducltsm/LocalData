"""Xử lý backfill một ngày — DAG firebase_raw_backfill chỉ loop và gọi hàm này.

Hai chế độ:
- use_existing_gcs=True (mặc định): đọc thẳng prefix raw ĐÃ CÓ trên bucket
  (GCS_RAW_PREFIX), tự detect layout thư mục thực tế, bỏ qua bước export BigQuery.
- use_existing_gcs=False: export BigQuery -> staging như DAG daily rồi ingest.
"""

from __future__ import annotations

import logging
import shutil
from datetime import UTC, datetime

from fb_pipeline.bq import client as bq
from fb_pipeline.bq.export import render_export_sql, run_export
from fb_pipeline.clickhouse import ingest
from fb_pipeline.clickhouse.client import get_client
from fb_pipeline.config import Settings
from fb_pipeline.gcs import client as gcs

log = logging.getLogger(__name__)


def _now() -> datetime:
    return datetime.now(tz=UTC).replace(tzinfo=None, microsecond=0)


def _stage_local(settings: Settings, ds: str, source_prefix: str) -> int:
    """Strategy file: tải parquet của ngày về shared volume (dọn dir cũ trước)."""
    dest = ingest.local_staging_dir(settings, ds)
    shutil.rmtree(dest, ignore_errors=True)
    storage = gcs.get_client(settings.gcp_project_id)
    files = gcs.download_prefix(storage, settings.gcs_bucket, source_prefix, dest)
    if not files:
        raise RuntimeError(f"Không tải được file parquet nào từ {source_prefix}")
    return len(files)


def process_day(settings: Settings, ds: str, run_id: str, use_existing_gcs: bool) -> str:
    """Ingest một ngày; trả về status ('success' | 'skipped'); raise nếu lỗi.

    Luôn ghi fb.ingestion_log, kể cả khi skip hay lỗi.
    """
    ingest.check_ds(ds)
    ds_nodash = ds.replace("-", "")
    started = _now()
    ch = get_client(settings)
    storage = gcs.get_client(settings.gcp_project_id)

    source_table = ""
    is_intraday = 1
    bq_row_count = 0
    files_read = 0
    bytes_read = 0
    rows_inserted = 0
    status = "failed"
    error_message = ""

    try:
        if use_existing_gcs:
            # Không giả định layout — detect trên bucket thật, log vài object đầu
            day_prefix = gcs.detect_day_prefix(
                storage, settings.gcs_bucket, settings.gcs_raw_prefix, ds, ds_nodash
            )
            if day_prefix is None:
                status = "skipped"
                error_message = f"Không tìm thấy parquet cho {ds} dưới {settings.gcs_raw_prefix}"
                log.warning("%s — bỏ qua", error_message)
                return status
            source_table = f"gcs:{day_prefix}"
            object_glob = f"{day_prefix}*.parquet"
        else:
            bq_client = bq.get_client(settings.gcp_project_id, settings.bq_location)
            resolved = bq.choose_source_table(
                bq.table_exists(
                    bq_client, settings.gcp_project_id, settings.bq_dataset, f"events_{ds_nodash}"
                ),
                bq.table_exists(
                    bq_client,
                    settings.gcp_project_id,
                    settings.bq_dataset,
                    f"events_intraday_{ds_nodash}",
                ),
                ds_nodash,
            )
            if resolved is None:
                status = "skipped"
                error_message = f"Không có bảng nguồn nào cho {ds}"
                log.warning("%s — bỏ qua", error_message)
                return status
            source_table, is_intraday = resolved
            bq_row_count = bq.count_rows(
                bq_client, settings.gcp_project_id, settings.bq_dataset, source_table
            )
            run_export(
                bq_client,
                render_export_sql(
                    project_id=settings.gcp_project_id,
                    dataset=settings.bq_dataset,
                    source_table=source_table,
                    bucket=settings.gcs_bucket,
                    staging_prefix=settings.gcs_staging_prefix,
                    ds=ds,
                ),
            )
            object_glob = ingest.staging_object_glob(settings, ds)

        # Thống kê file sẽ đọc (glob prefix = phần trước 'part-*' / '*')
        listing_prefix = object_glob.split("*")[0].rsplit("/", 1)[0] + "/"
        objects = [
            (name, size)
            for name, size in gcs.list_objects(storage, settings.gcs_bucket, listing_prefix)
            if name.endswith(".parquet")
        ]
        files_read = len(objects)
        bytes_read = sum(size for _, size in objects)
        if files_read == 0:
            raise RuntimeError(f"0 file parquet dưới gs://{settings.gcs_bucket}/{listing_prefix}")
        log.info("%s: %d file / %d bytes từ %s", ds, files_read, bytes_read, listing_prefix)

        if settings.ingest_strategy == "file":
            _stage_local(settings, ds, listing_prefix)

        # Idempotency: xoá partition cũ rồi insert lại
        ingest.drop_partition(ch, settings.clickhouse_db, ds)
        sql = ingest.render_insert_sql(
            settings,
            ds=ds,
            run_id=run_id,
            source_table=source_table,
            is_intraday=is_intraday,
            object_glob=object_glob,
        )
        ingest.run_insert(ch, settings, sql)
        rows_inserted = ingest.partition_row_count(ch, settings.clickhouse_db, ds)
        if rows_inserted == 0:
            raise RuntimeError(f"INSERT xong nhưng partition {ds} có 0 dòng")
        if bq_row_count and rows_inserted != bq_row_count:
            drift = abs(rows_inserted - bq_row_count) / bq_row_count
            # Bảng intraday đang streaming: chênh nhẹ giữa count(*) và EXPORT DATA
            # là bình thường (cùng quy tắc với QC của DAG daily)
            if is_intraday and drift <= 0.005:
                log.warning(
                    "Row count lệch %.4f%% (CH=%d, BQ=%d) — intraday streaming, chấp nhận",
                    drift * 100, rows_inserted, bq_row_count,
                )
            else:
                raise RuntimeError(
                    f"Row count lệch: ClickHouse={rows_inserted}, BigQuery={bq_row_count}"
                )

        if settings.cleanup_staging and not use_existing_gcs:
            gcs.delete_prefix(
                storage, settings.gcs_bucket, f"{settings.gcs_staging_prefix}/dt={ds}/"
            )
        if settings.cleanup_staging and settings.ingest_strategy == "file":
            shutil.rmtree(ingest.local_staging_dir(settings, ds), ignore_errors=True)

        status = "success"
        log.info("Backfill %s OK: %d dòng", ds, rows_inserted)
        return status
    except Exception as exc:
        error_message = error_message or str(exc)
        raise
    finally:
        finished = _now()
        ingest.write_ingestion_log(
            ch,
            settings.clickhouse_db,
            ingest.IngestionLogRow(
                event_date=ds,
                source_table=source_table,
                is_intraday=is_intraday,
                strategy=settings.ingest_strategy,
                bq_row_count=bq_row_count,
                files_read=files_read,
                bytes_read=bytes_read,
                rows_inserted=rows_inserted,
                run_id=run_id,
                started_at=started,
                finished_at=finished,
                duration_sec=int((finished - started).total_seconds()),
                status=status,
                error_message=error_message,
            ),
        )
