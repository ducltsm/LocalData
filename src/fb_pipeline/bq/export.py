"""Render + chạy câu EXPORT DATA (dump thô, không transform)."""

from __future__ import annotations

import logging
from pathlib import Path

from google.cloud import bigquery

from fb_pipeline.templating import render_template

log = logging.getLogger(__name__)

_TEMPLATE = Path(__file__).resolve().parent / "sql" / "dump_raw.sql.j2"


def render_export_sql(
    *,
    project_id: str,
    dataset: str,
    source_table: str,
    bucket: str,
    staging_prefix: str,
    ds: str,
) -> str:
    """Render dump_raw.sql.j2 — chỉ EXPORT DATA ... SELECT *, không UNNEST/cast."""
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
    """Chạy EXPORT DATA và chờ xong (dùng cho DAG backfill; DAG daily dùng operator)."""
    log.info("Chạy EXPORT DATA:\n%s", sql)
    client.query(sql).result()
