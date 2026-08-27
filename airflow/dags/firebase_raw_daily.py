"""DAG chính: nạp raw GA4 của một ngày (ds) từ BigQuery vào fb.events_raw.

Luồng: resolve bảng nguồn -> đếm dòng BQ -> EXPORT DATA (thô) -> verify GCS
-> (stage local nếu strategy file) -> DROP PARTITION (idempotency) -> INSERT
-> quality checks -> cleanup staging -> ghi ingestion_log (kể cả khi fail).

DAG chỉ orchestrate — mọi logic nằm ở package fb_pipeline; Python không parse
dữ liệu, ClickHouse tự đọc Parquet.
"""

from __future__ import annotations

import logging
import os
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator
from airflow.utils.task_group import TaskGroup

log = logging.getLogger(__name__)

_PROJECT = os.environ.get("GCP_PROJECT_ID", "")
_LOCATION = os.environ.get("BQ_LOCATION", "US")
_BUCKET = os.environ.get("GCS_BUCKET", "")
_STAGING = os.environ.get("GCS_STAGING_PREFIX", "staging_raw")
_TZ = os.environ.get("TZ", "Asia/Ho_Chi_Minh")


def _target_ds(logical_date: pendulum.DateTime) -> str:
    """Ngày dữ liệu của run = logical date đổi về timezone của DAG.

    KHÔNG dùng macro ``ds`` của Airflow: ds render theo UTC, mà cron 04:00+07
    nằm trước offset (+07) nên ds sẽ lùi 1 ngày so với chủ đích (04:00+07 =
    21:00 UTC hôm trước) — đã verify bằng ``dags test``. Với hàm này:
    ``dags test firebase_raw_daily 2026-08-27`` nạp đúng ngày 2026-08-27, và
    run theo lịch 04:00 sáng xử lý đúng ngày hôm trước.
    """
    return logical_date.in_timezone(_TZ).strftime("%Y-%m-%d")

# Ngưỡng cảnh báo QC (tỉ lệ trên tổng số dòng của partition)
NULL_PSEUDO_WARN_RATIO = 0.05
EMPTY_PARAMS_WARN_RATIO = 0.20
# Bảng intraday đang streaming: chấp nhận lệch row count tối đa 0.5% (final: bắt buộc 0)
INTRADAY_DRIFT_RATIO = 0.005

DEFAULT_ARGS = {
    "owner": "data",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
}


@dag(
    dag_id="firebase_raw_daily",
    description="Nạp raw GA4 (giữ nguyên nested) từ BigQuery export vào fb.events_raw",
    schedule="0 4 * * *",
    start_date=pendulum.datetime(2026, 8, 26, tz="Asia/Ho_Chi_Minh"),
    catchup=True,
    max_active_runs=2,
    default_args=DEFAULT_ARGS,
    tags=["firebase", "clickhouse", "raw"],
)
def firebase_raw_daily() -> None:
    """Định nghĩa DAG (TaskFlow)."""

    @task
    def resolve_source(logical_date: pendulum.DateTime | None = None) -> dict:
        """Ưu tiên bảng daily final, fallback intraday; cả hai không có -> skip run."""
        from fb_pipeline.bq import client as bq
        from fb_pipeline.config import load_settings

        ds = _target_ds(logical_date)
        ds_nodash = ds.replace("-", "")
        settings = load_settings()
        client = bq.get_client(settings.gcp_project_id, settings.bq_location)
        final_exists = bq.table_exists(
            client, settings.gcp_project_id, settings.bq_dataset, f"events_{ds_nodash}"
        )
        intraday_exists = bq.table_exists(
            client, settings.gcp_project_id, settings.bq_dataset, f"events_intraday_{ds_nodash}"
        )
        resolved = bq.choose_source_table(final_exists, intraday_exists, ds_nodash)
        if resolved is None:
            raise AirflowSkipException(
                f"Không có events_{ds_nodash} lẫn events_intraday_{ds_nodash} trong "
                f"{settings.gcp_project_id}.{settings.bq_dataset} — bỏ qua ngày {ds}. "
                "(GA4 thường sinh bảng sau vài giờ; nếu ngày cũ vẫn thiếu thì kiểm tra "
                "BigQuery export của property.)"
            )
        source_table, is_intraday = resolved
        log.info("Bảng nguồn ngày %s: %s (is_intraday=%d)", ds, source_table, is_intraday)
        return {
            "source_table": source_table,
            "is_intraday": is_intraday,
            # BigQueryToGCSOperator nhận dạng project.dataset.table
            "source_fqtn": f"{settings.gcp_project_id}.{settings.bq_dataset}.{source_table}",
        }

    @task
    def count_bq_rows(src: dict) -> int:
        """SELECT count(*) trên bảng nguồn — mốc đối chiếu cho quality check."""
        from fb_pipeline.bq import client as bq
        from fb_pipeline.config import load_settings

        settings = load_settings()
        client = bq.get_client(settings.gcp_project_id, settings.bq_location)
        n = bq.count_rows(
            client, settings.gcp_project_id, settings.bq_dataset, src["source_table"]
        )
        log.info("BigQuery %s: %d dòng", src["source_table"], n)
        return n

    @task
    def clean_staging_prefix(logical_date: pendulum.DateTime | None = None) -> None:
        """Xoá staging prefix của ngày trước khi extract.

        Extract job KHÔNG có overwrite như EXPORT DATA — không dọn trước thì file
        thừa của run cũ nằm lẫn và bị đọc trùng.
        """
        from fb_pipeline.config import load_settings
        from fb_pipeline.gcs import client as gcs

        ds = _target_ds(logical_date)
        settings = load_settings()
        storage = gcs.get_client(settings.gcp_project_id)
        gcs.delete_prefix(storage, settings.gcs_bucket, f"{settings.gcs_staging_prefix}/dt={ds}/")

    # Extract job (bq extract): xuất NGUYÊN bảng ra Parquet — không dùng slot,
    # KHÔNG tính tiền query (EXPORT DATA cũ quét ~57GB logical/ngày với app lớn).
    dump_raw_to_gcs = BigQueryToGCSOperator(
        task_id="dump_raw_to_gcs",
        gcp_conn_id="google_cloud_default",
        source_project_dataset_table="{{ ti.xcom_pull(task_ids='resolve_source')['source_fqtn'] }}",
        destination_cloud_storage_uris=[
            "gs://" + _BUCKET + "/" + _STAGING
            + "/dt={{ logical_date.in_timezone('" + _TZ + "').strftime('%Y-%m-%d') }}/part-*.parquet"
        ],
        export_format="PARQUET",
        compression="SNAPPY",
        location=_LOCATION,
    )

    list_gcs_objects = GCSListObjectsOperator(
        task_id="list_gcs_objects",
        gcp_conn_id="google_cloud_default",
        bucket=_BUCKET,
        # cùng logic _target_ds: logical date đổi về giờ VN, không dùng {{ ds }} (UTC)
        prefix=_STAGING + "/dt={{ logical_date.in_timezone('" + _TZ + "').strftime('%Y-%m-%d') }}/",
    )

    @task
    def verify_gcs_objects(
        object_names: list[str] | None, logical_date: pendulum.DateTime | None = None
    ) -> dict:
        """Fail nếu EXPORT DATA không sinh file; push số file + tổng bytes."""
        from fb_pipeline.config import load_settings
        from fb_pipeline.gcs import client as gcs

        ds = _target_ds(logical_date)
        settings = load_settings()
        prefix = f"{settings.gcs_staging_prefix}/dt={ds}/"
        parquets = [n for n in (object_names or []) if n.endswith(".parquet")]
        if not parquets:
            raise ValueError(
                f"EXPORT DATA không sinh file .parquet nào dưới "
                f"gs://{settings.gcs_bucket}/{prefix}"
            )
        storage = gcs.get_client(settings.gcp_project_id)
        objects = [
            (name, size)
            for name, size in gcs.list_objects(storage, settings.gcs_bucket, prefix)
            if name.endswith(".parquet")
        ]
        total_bytes = sum(size for _, size in objects)
        log.info("%d file / %d bytes. 5 file đầu: %s", len(objects), total_bytes, objects[:5])
        return {"files": len(objects), "bytes": total_bytes}

    @task
    def stage_files(logical_date: pendulum.DateTime | None = None) -> int:
        """Chỉ chạy khi INGEST_STRATEGY=file: tải Parquet vào shared volume user_files."""
        import shutil

        from fb_pipeline.clickhouse.ingest import local_staging_dir
        from fb_pipeline.config import load_settings
        from fb_pipeline.gcs import client as gcs

        ds = _target_ds(logical_date)
        settings = load_settings()
        if settings.ingest_strategy != "file":
            raise AirflowSkipException("INGEST_STRATEGY=s3 — ClickHouse đọc thẳng GCS")
        dest = local_staging_dir(settings, ds)
        shutil.rmtree(dest, ignore_errors=True)  # dọn run cũ cho idempotent
        storage = gcs.get_client(settings.gcp_project_id)
        files = gcs.download_prefix(
            storage, settings.gcs_bucket, f"{settings.gcs_staging_prefix}/dt={ds}/", dest
        )
        if not files:
            raise ValueError("Không tải được file parquet nào về staging local")
        return len(files)

    @task(trigger_rule="none_failed_min_one_success")
    def drop_partition(logical_date: pendulum.DateTime | None = None) -> None:
        """BẮT BUỘC trước insert — toàn bộ cơ chế idempotency của pipeline."""
        from fb_pipeline.clickhouse import ingest
        from fb_pipeline.clickhouse.client import get_client
        from fb_pipeline.config import load_settings

        settings = load_settings()
        ingest.drop_partition(get_client(settings), settings.clickhouse_db, _target_ds(logical_date))

    @task
    def insert_raw(
        src: dict,
        logical_date: pendulum.DateTime | None = None,
        run_id: str | None = None,
    ) -> dict:
        """Một câu INSERT ... SELECT <cột tường minh> FROM s3()/file()."""
        from fb_pipeline.clickhouse import ingest
        from fb_pipeline.clickhouse.client import get_client
        from fb_pipeline.config import load_settings

        ds = _target_ds(logical_date)
        settings = load_settings()
        client = get_client(settings)
        sql = ingest.render_insert_sql(
            settings,
            ds=ds,
            run_id=run_id or "",
            source_table=src["source_table"],
            is_intraday=src["is_intraday"],
        )
        # SQL không chứa credential (HMAC nằm trong named collection gcs_raw)
        log.info("Chạy INSERT:\n%s", sql)
        ingest.run_insert(client, settings, sql)
        rows = ingest.partition_row_count(client, settings.clickhouse_db, ds)
        log.info("Partition %s hiện có %d dòng", ds, rows)
        return {"rows_inserted": rows}

    @task
    def qc_row_count(
        src: dict, bq_rows: int, logical_date: pendulum.DateTime | None = None
    ) -> None:
        """Đối chiếu số dòng ClickHouse với BigQuery.

        Bảng FINAL: bắt buộc khớp tuyệt đối (sai lệch 0). Bảng INTRADAY: đang nhận
        streaming nên giữa lúc count(*) và EXPORT DATA luôn có thể thêm vài dòng —
        chấp nhận lệch <= INTRADAY_DRIFT_RATIO (cảnh báo), vượt ngưỡng mới fail.
        """
        from fb_pipeline.clickhouse.client import get_client
        from fb_pipeline.clickhouse.ingest import partition_row_count
        from fb_pipeline.config import load_settings

        ds = _target_ds(logical_date)
        settings = load_settings()
        ch_rows = partition_row_count(get_client(settings), settings.clickhouse_db, ds)
        diff = abs(ch_rows - bq_rows)
        if diff == 0:
            log.info("Row count khớp tuyệt đối: %d", ch_rows)
            return
        if src.get("is_intraday") and bq_rows and diff / bq_rows <= INTRADAY_DRIFT_RATIO:
            log.warning(
                "Row count lệch %d dòng (CH=%d, BQ=%d, %.4f%%) — bảng intraday đang "
                "streaming nên chênh nhẹ giữa count và export là bình thường. "
                "Ngày này sẽ khớp tuyệt đối khi bảng final xuất hiện và DAG chạy lại.",
                diff, ch_rows, bq_rows, diff / bq_rows * 100,
            )
            return
        raise ValueError(
            f"Row count lệch: ClickHouse={ch_rows}, BigQuery={bq_rows}. "
            "Với bảng final phải khớp tuyệt đối; nguyên nhân hay gặp: intraday bị "
            "thay bằng daily giữa chừng — chạy lại DAG cho ngày này."
        )

    @task
    def qc_metrics(logical_date: pendulum.DateTime | None = None) -> dict:
        """null user_pseudo_id / empty event_params (cảnh báo) + uniqExact(event_name) > 0."""
        from fb_pipeline.clickhouse.client import get_client
        from fb_pipeline.clickhouse.ingest import qc_metrics as fetch_qc
        from fb_pipeline.config import load_settings

        ds = _target_ds(logical_date)
        settings = load_settings()
        m = fetch_qc(get_client(settings), settings.clickhouse_db, ds)
        log.info("QC metrics %s: %s", ds, m)
        if m["rows"] == 0:
            raise ValueError(f"Partition {ds} rỗng")
        null_ratio = m["null_pseudo"] / m["rows"]
        if null_ratio > NULL_PSEUDO_WARN_RATIO:
            log.warning(
                "user_pseudo_id NULL/rỗng: %d/%d (%.1f%%) — vượt ngưỡng %.0f%%",
                m["null_pseudo"], m["rows"], null_ratio * 100,
                NULL_PSEUDO_WARN_RATIO * 100,
            )
        empty_ratio = m["empty_params"] / m["rows"]
        if empty_ratio > EMPTY_PARAMS_WARN_RATIO:
            log.warning(
                "event_params rỗng: %d/%d (%.1f%%) — tỉ lệ cao là dấu hiệu structure "
                "khai báo SAI (chạy make sample-parquet), không phải dữ liệu xấu",
                m["empty_params"], m["rows"], empty_ratio * 100,
            )
        if m["uniq_event_names"] <= 0:
            raise ValueError("uniqExact(event_name) = 0")
        return m

    @task
    def qc_nested_readable(logical_date: pendulum.DateTime | None = None) -> None:
        """Đọc thử một dòng, assert length(event_params) > 0 — bằng chứng nested đọc được."""
        from fb_pipeline.clickhouse.client import get_client
        from fb_pipeline.config import load_settings

        ds = _target_ds(logical_date)
        settings = load_settings()
        client = get_client(settings)
        result = client.query(
            f"SELECT event_name, length(event_params) AS n_params, "
            f"arrayMap(p -> tupleElement(p, 'key'), event_params) AS keys "
            f"FROM {settings.clickhouse_db}.events_raw "
            f"WHERE _dt = toDate('{ds}') AND notEmpty(event_params) LIMIT 1"
        )
        if not result.result_rows or int(result.result_rows[0][1]) <= 0:
            raise ValueError(
                "Không đọc được dòng nào có event_params khác rỗng — structure khai báo "
                "nhiều khả năng sai, chạy `make sample-parquet` để đối chiếu."
            )
        row = result.result_rows[0]
        log.info("Mẫu nested OK: event=%s, %d params, keys=%s", row[0], row[1], row[2])

    @task
    def flatten(logical_date: pendulum.DateTime | None = None, run_id: str | None = None) -> dict:
        """Flatten partition vừa nạp vào fb.events_flat.

        Key mới trong event_params/user_properties -> registry -> tự ALTER TABLE
        ADD COLUMN (xem fb_pipeline.clickhouse.flat). Idempotent theo partition.
        """
        from fb_pipeline.clickhouse.client import get_client
        from fb_pipeline.clickhouse.flat import flatten_day
        from fb_pipeline.config import load_settings

        settings = load_settings()
        result = flatten_day(
            get_client(settings), settings, _target_ds(logical_date), run_id or ""
        )
        log.info("Flatten xong: %s", result)
        return result

    @task(trigger_rule="none_failed_min_one_success")
    def cleanup(logical_date: pendulum.DateTime | None = None) -> None:
        """Xoá staging GCS/local theo CLEANUP_STAGING (chỉ chạy khi mọi bước trước OK)."""
        import shutil

        from fb_pipeline.clickhouse.ingest import local_staging_dir
        from fb_pipeline.config import load_settings
        from fb_pipeline.gcs import client as gcs

        ds = _target_ds(logical_date)
        settings = load_settings()
        if not settings.cleanup_staging:
            log.info("CLEANUP_STAGING=false — giữ nguyên staging")
            return
        storage = gcs.get_client(settings.gcp_project_id)
        gcs.delete_prefix(storage, settings.gcs_bucket, f"{settings.gcs_staging_prefix}/dt={ds}/")
        if settings.ingest_strategy == "file":
            shutil.rmtree(local_staging_dir(settings, ds), ignore_errors=True)

    @task(trigger_rule="all_done")
    def write_ingestion_log(
        logical_date: pendulum.DateTime | None = None, run_id: str | None = None
    ) -> None:
        """Ghi fb.ingestion_log cho run này — chạy cả khi fail/skip (ALL_DONE)."""
        from datetime import UTC, datetime

        from airflow.operators.python import get_current_context

        from fb_pipeline.clickhouse import ingest
        from fb_pipeline.clickhouse.client import get_client
        from fb_pipeline.config import load_settings

        ds = _target_ds(logical_date)
        context = get_current_context()
        ti = context["ti"]
        dag_run = context["dag_run"]

        src = ti.xcom_pull(task_ids="resolve_source") or {}
        bq_rows = ti.xcom_pull(task_ids="count_bq_rows") or 0
        gcs_info = ti.xcom_pull(task_ids="verify_gcs_objects") or {}
        inserted = ti.xcom_pull(task_ids="insert_raw") or {}

        states = {t.task_id: t.state for t in dag_run.get_task_instances()}
        failed = sorted(t for t, s in states.items() if s == "failed")
        if failed:
            status, error_message = "failed", "Task fail: " + ", ".join(failed)
        elif states.get("resolve_source") == "skipped":
            status, error_message = "skipped", "Không có bảng nguồn trên BigQuery"
        else:
            status, error_message = "success", ""

        def _naive_utc(value: datetime) -> datetime:
            return value.astimezone(UTC).replace(tzinfo=None, microsecond=0)

        started = _naive_utc(dag_run.start_date or datetime.now(tz=UTC))
        finished = _naive_utc(datetime.now(tz=UTC))

        settings = load_settings()
        ingest.write_ingestion_log(
            get_client(settings),
            settings.clickhouse_db,
            ingest.IngestionLogRow(
                event_date=ds,
                source_table=src.get("source_table", ""),
                is_intraday=int(src.get("is_intraday", 0)),
                strategy=settings.ingest_strategy,
                bq_row_count=int(bq_rows),
                files_read=int(gcs_info.get("files", 0)),
                bytes_read=int(gcs_info.get("bytes", 0)),
                rows_inserted=int(inserted.get("rows_inserted", 0)),
                run_id=run_id or "",
                started_at=started,
                finished_at=finished,
                duration_sec=int((finished - started).total_seconds()),
                status=status,
                error_message=error_message,
            ),
        )

    # ------------------------------------------------------------------ wiring
    src = resolve_source()
    bq_rows = count_bq_rows(src)
    cleaned_prefix = clean_staging_prefix()
    verified = verify_gcs_objects(list_gcs_objects.output)
    staged = stage_files()
    dropped = drop_partition()
    inserted = insert_raw(src)

    with TaskGroup(group_id="quality_checks"):
        qc_tasks = [qc_row_count(src, bq_rows), qc_metrics(), qc_nested_readable()]

    flattened = flatten()
    cleaned = cleanup()
    logged = write_ingestion_log()

    bq_rows >> cleaned_prefix >> dump_raw_to_gcs >> list_gcs_objects >> verified >> staged
    # drop chạy khi verify OK dù stage bị skip (strategy s3) — và skip khi cả ngày bị skip
    [verified, staged] >> dropped >> inserted
    inserted >> qc_tasks >> flattened >> cleaned >> logged

firebase_raw_daily()
