"""Kết nối ClickHouse qua HTTP (clickhouse-connect) với timeout dài cho insert lớn."""

from __future__ import annotations

import clickhouse_connect
from clickhouse_connect.driver.client import Client

from fb_pipeline.config import Settings


def get_client(settings: Settings, database: str | None = None) -> Client:
    """Client HTTP tới ClickHouse.

    ``send_receive_timeout`` dài + ``send_progress_in_http_headers`` để câu
    INSERT ... SELECT đọc Parquet lớn không bị đứt kết nối giữa chừng.
    ``database`` override dùng khi database đích CHƯA tồn tại (apply_schema
    lần đầu cho app mới) — truyền 'default' để connect được.
    """
    return clickhouse_connect.get_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password,
        database=database or settings.clickhouse_db,
        connect_timeout=30,
        send_receive_timeout=7200,
        client_name="fb-pipeline",
        settings={"send_progress_in_http_headers": 1},
    )
