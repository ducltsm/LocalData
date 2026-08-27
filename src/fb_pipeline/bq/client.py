"""Client BigQuery mỏng — chỉ những gì pipeline cần, không transform dữ liệu."""

from __future__ import annotations

import logging

from google.api_core.exceptions import NotFound
from google.cloud import bigquery

log = logging.getLogger(__name__)


def get_client(project_id: str, location: str | None = None) -> bigquery.Client:
    """Tạo client BigQuery (credential lấy từ GOOGLE_APPLICATION_CREDENTIALS)."""
    return bigquery.Client(project=project_id, location=location)


def table_exists(client: bigquery.Client, project_id: str, dataset: str, table: str) -> bool:
    """True nếu bảng tồn tại (kể cả bảng intraday do GA4 streaming tạo)."""
    try:
        client.get_table(f"{project_id}.{dataset}.{table}")
        return True
    except NotFound:
        return False


def count_rows(client: bigquery.Client, project_id: str, dataset: str, table: str) -> int:
    """SELECT count(*) trên bảng nguồn — dùng đối chiếu sau khi ingest."""
    sql = f"SELECT count(*) AS n FROM `{project_id}.{dataset}.{table}`"
    result = list(client.query(sql).result())
    return int(result[0]["n"])


def choose_source_table(
    final_exists: bool, intraday_exists: bool, ds_nodash: str
) -> tuple[str, int] | None:
    """Logic chọn bảng nguồn (pure function — unit test không cần mock BigQuery).

    Ưu tiên bảng daily final; fallback intraday; không có gì -> None (DAG sẽ skip).
    Returns: (tên bảng, is_intraday) hoặc None.
    """
    if final_exists:
        return f"events_{ds_nodash}", 0
    if intraday_exists:
        return f"events_intraday_{ds_nodash}", 1
    return None
