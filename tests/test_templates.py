"""Render các template SQL ra chuỗi hợp lệ (snapshot) — cả hai strategy s3/file."""

from __future__ import annotations

from fb_pipeline.bq.export import render_export_sql
from fb_pipeline.clickhouse import ingest
from fb_pipeline.clickhouse.source_schema import column_names, structure
from fb_pipeline.config import Settings, load_settings

DS = "2026-08-27"


def _settings(base_env: dict[str, str], **overrides: str) -> Settings:
    return load_settings({**base_env, **overrides})


def test_extract_staging_uri() -> None:
    """Đường mặc định (extract job, miễn phí): URI đích có wildcard + đúng layout dt=."""
    from fb_pipeline.bq.export import staging_uri

    assert staging_uri("b", "staging_raw", DS) == (
        "gs://b/staging_raw/dt=2026-08-27/part-*.parquet"
    )


def test_export_sql_is_raw_dump() -> None:
    """LEGACY path (EXPORT DATA — có phí quét): vẫn phải là dump thô nếu được dùng."""
    sql = render_export_sql(
        project_id="p",
        dataset="d",
        source_table="events_20260827",
        bucket="b",
        staging_prefix="staging_raw",
        ds=DS,
    )
    assert "EXPORT DATA OPTIONS(" in sql
    assert "uri = 'gs://b/staging_raw/dt=2026-08-27/part-*.parquet'" in sql
    assert "format = 'PARQUET'" in sql
    assert "overwrite = true" in sql
    # dump thô — không transform
    assert "SELECT * FROM `p.d.events_20260827`" in sql
    assert "UNNEST" not in sql.upper()


def test_read_source_s3_snapshot(base_env: dict[str, str]) -> None:
    rendered = ingest.render_read_source(_settings(base_env), DS)
    expected = (
        "s3(gcs_raw, url = 'https://storage.googleapis.com/test-bucket/"
        "staging_raw/dt=2026-08-27/part-*.parquet', "
        f"format = 'Parquet', structure = '{structure()}')"
    )
    assert rendered == expected
    # credential HMAC nằm trong named collection, không được lộ ra SQL
    assert "HMACKEY" not in rendered
    assert "HMACSECRET" not in rendered


def test_read_source_file_snapshot(base_env: dict[str, str]) -> None:
    rendered = ingest.render_read_source(_settings(base_env, INGEST_STRATEGY="file"), DS)
    assert rendered == f"file('staging/dt=2026-08-27/*.parquet', 'Parquet', '{structure()}')"


def test_read_source_custom_glob_for_backfill(base_env: dict[str, str]) -> None:
    rendered = ingest.render_read_source(
        _settings(base_env), DS, object_glob="raw/prefix/dt=2026-08-27/*.parquet"
    )
    assert "test-bucket/raw/prefix/dt=2026-08-27/*.parquet" in rendered


def test_insert_sql_explicit_columns_no_select_star(base_env: dict[str, str]) -> None:
    sql = ingest.render_insert_sql(
        _settings(base_env),
        ds=DS,
        run_id="manual__2026-08-27T00:00:00",
        source_table="events_20260827",
        is_intraday=0,
    )
    assert "SELECT *" not in sql
    assert "select *" not in sql.lower()
    assert sql.strip().startswith("INSERT INTO fb.events_raw")
    for name in column_names():
        assert name in sql, f"Thiếu cột {name} trong INSERT"
    assert "toDate('2026-08-27') AS _dt" in sql
    assert "'manual__2026-08-27T00:00:00' AS _run_id" in sql
    assert "'events_20260827' AS _source_table" in sql
    assert "0 AS _is_intraday" in sql
    assert "FROM s3(gcs_raw," in sql


def test_insert_sql_file_strategy(base_env: dict[str, str]) -> None:
    sql = ingest.render_insert_sql(
        _settings(base_env, INGEST_STRATEGY="file"),
        ds=DS,
        run_id="r",
        source_table="events_intraday_20260827",
        is_intraday=1,
    )
    assert "FROM file('staging/dt=2026-08-27/*.parquet'" in sql
    assert "1 AS _is_intraday" in sql


def test_insert_sql_escapes_quotes(base_env: dict[str, str]) -> None:
    sql = ingest.render_insert_sql(
        _settings(base_env),
        ds=DS,
        run_id="run'id",
        source_table="tbl",
        is_intraday=0,
    )
    assert "run\\'id" in sql


def test_invalid_ds_rejected(base_env: dict[str, str]) -> None:
    import pytest

    with pytest.raises(ValueError, match="YYYY-MM-DD"):
        ingest.render_read_source(_settings(base_env), "27-08-2026")
