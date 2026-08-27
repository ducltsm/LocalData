"""Fixtures chung. Unit test mock hết network; integration test cần ClickHouse thật."""

from __future__ import annotations

import pytest

BASE_ENV: dict[str, str] = {
    "GCP_PROJECT_ID": "test-project",
    "BQ_DATASET": "analytics_1",
    "BQ_LOCATION": "US",
    "GCS_BUCKET": "test-bucket",
    "GCS_RAW_PREFIX": "raw/prefix",
    "GCS_STAGING_PREFIX": "staging_raw",
    "GCS_HMAC_ACCESS_KEY": "HMACKEY",
    "GCS_HMAC_SECRET": "HMACSECRET",
    "INGEST_STRATEGY": "s3",
    "RAW_TTL_DAYS": "0",
    "CLEANUP_STAGING": "true",
    "MAX_MEMORY_USAGE": "1000000",
    "MAX_INSERT_THREADS": "2",
    "CLICKHOUSE_HOST": "clickhouse",
    "CLICKHOUSE_PORT": "8123",
    "CLICKHOUSE_USER": "fb",
    "CLICKHOUSE_PASSWORD": "pw",
    "CLICKHOUSE_DB": "fb",
}


@pytest.fixture
def base_env() -> dict[str, str]:
    """Bản copy env đầy đủ — test tự sửa/xoá key theo case."""
    return dict(BASE_ENV)
