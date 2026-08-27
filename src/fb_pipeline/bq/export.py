"""Dump thô bảng GA4 ra GCS.

Đường chính: **extract job** (`run_extract`) — job xuất nguyên bảng, KHÔNG dùng
slot, KHÔNG tính tiền query (EXPORT DATA quét 56.8GB logical/ngày với app lớn
~ $0.36/ngày; extract job = $0). Pipeline này dump thô không transform nên khớp
extract job 100%.

Lưu ý vận hành: extract job KHÔNG có overwrite — phải xoá prefix staging trước
khi extract (gcs.delete_prefix), nếu không file thừa của run cũ sẽ bị đọc lẫn.

Đường legacy: `render_export_sql` (EXPORT DATA) giữ lại cho trường hợp sau này
cần export CÓ LỌC/transform — chấp nhận trả phí quét.
"""

from __future__ import annotations

import logging
from pathlib import Path

from google.cloud import bigquery

from fb_pipeline.templating import render_template

log = logging.getLogger(__name__)

_TEMPLATE = Path(__file__).resolve().parent / "sql" / "dump_raw.sql.j2"


def staging_uri(bucket: str, staging_prefix: str, ds: str) -> str:
    """URI đích trên GCS (wildcard bắt buộc vì bảng > 1GB xuất nhiều file)."""
    return f"gs://{bucket}/{staging_prefix}/dt={ds}/part-*.parquet"


def run_extract(
    client: bigquery.Client,
    *,
    project_id: str,
    dataset: str,
    source_table: str,
    bucket: str,
    staging_prefix: str,
    ds: str,
) -> None:
    """Extract job: xuất NGUYÊN bảng ra Parquet/SNAPPY — miễn phí, nested giữ nguyên.

    Nhớ xoá prefix staging trước khi gọi (extract không tự dọn file cũ).
    """
    uri = staging_uri(bucket, staging_prefix, ds)
    log.info("Extract job (miễn phí): %s.%s.%s -> %s", project_id, dataset, source_table, uri)
    job_config = bigquery.job.ExtractJobConfig(
        destination_format="PARQUET", compression="SNAPPY"
    )
    client.extract_table(
        f"{project_id}.{dataset}.{source_table}", uri, job_config=job_config
    ).result()


def render_export_sql(
    *,
    project_id: str,
    dataset: str,
    source_table: str,
    bucket: str,
    staging_prefix: str,
    ds: str,
) -> str:
    """LEGACY — EXPORT DATA (bị tính tiền theo bytes quét). Mặc định dùng run_extract."""
    return render_template(
        _TEMPLATE,
        project_id=project_id,
        dataset=dataset,
        source_table=source_table,
        bucket=bucket,
        staging_prefix=staging_prefix,
        ds=ds,
    )


def run_export(client: bigquery.Client, sql: str) -> None:
    """LEGACY — chạy EXPORT DATA (trả phí quét). Mặc định dùng run_extract."""
    log.info("Chạy EXPORT DATA (LEGACY, có phí quét):\n%s", sql)
    client.query(sql).result()
