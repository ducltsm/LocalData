"""load_settings: parse env, validate, raise rõ ràng khi thiếu."""

from __future__ import annotations

import pytest

from fb_pipeline.config import ConfigError, load_settings


def test_full_env_ok(base_env: dict[str, str]) -> None:
    s = load_settings(base_env)
    assert s.gcp_project_id == "test-project"
    assert s.ingest_strategy == "s3"
    assert s.clickhouse_port == 8123
    assert s.raw_ttl_days == 0
    assert s.cleanup_staging is True
    assert s.sql_dir.name == "sql"


def test_missing_required_raises_with_names(base_env: dict[str, str]) -> None:
    del base_env["CLICKHOUSE_PASSWORD"]
    del base_env["GCS_BUCKET"]
    with pytest.raises(ConfigError, match="CLICKHOUSE_PASSWORD") as exc_info:
        load_settings(base_env)
    assert "GCS_BUCKET" in str(exc_info.value)


def test_s3_strategy_requires_hmac(base_env: dict[str, str]) -> None:
    base_env["GCS_HMAC_ACCESS_KEY"] = ""
    with pytest.raises(ConfigError, match="HMAC"):
        load_settings(base_env)


def test_file_strategy_ok_without_hmac(base_env: dict[str, str]) -> None:
    base_env["INGEST_STRATEGY"] = "file"
    base_env["GCS_HMAC_ACCESS_KEY"] = ""
    base_env["GCS_HMAC_SECRET"] = ""
    assert load_settings(base_env).ingest_strategy == "file"


def test_invalid_strategy_raises(base_env: dict[str, str]) -> None:
    base_env["INGEST_STRATEGY"] = "ftp"
    with pytest.raises(ConfigError, match="INGEST_STRATEGY"):
        load_settings(base_env)


def test_invalid_int_raises(base_env: dict[str, str]) -> None:
    base_env["RAW_TTL_DAYS"] = "vĩnh viễn"
    with pytest.raises(ConfigError, match="RAW_TTL_DAYS"):
        load_settings(base_env)
