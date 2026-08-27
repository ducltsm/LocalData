"""Cấu hình pipeline: đọc + validate biến môi trường (xem .env.example)."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path


class ConfigError(RuntimeError):
    """Thiếu hoặc sai biến môi trường bắt buộc."""


_REQUIRED = (
    "GCP_PROJECT_ID",
    "BQ_DATASET",
    "GCS_BUCKET",
    "GCS_STAGING_PREFIX",
    "CLICKHOUSE_HOST",
    "CLICKHOUSE_USER",
    "CLICKHOUSE_PASSWORD",
    "CLICKHOUSE_DB",
)

_TRUE = {"1", "true", "yes", "on"}
_STRATEGIES = ("s3", "file")


def _default_sql_dir() -> Path:
    # .../src/fb_pipeline/config.py -> lên 2 cấp là project root -> clickhouse/sql
    return Path(__file__).resolve().parents[2] / "clickhouse" / "sql"


def _int(env: Mapping[str, str], key: str, default: int) -> int:
    raw = env.get(key, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ConfigError(f"Biến {key} phải là số nguyên, nhận được: {raw!r}") from exc


@dataclass(frozen=True)
class Settings:
    """Toàn bộ cấu hình runtime của pipeline (immutable)."""

    gcp_project_id: str
    bq_dataset: str
    bq_location: str
    gcs_bucket: str
    gcs_raw_prefix: str
    gcs_staging_prefix: str
    ingest_strategy: str  # 's3' | 'file'
    raw_ttl_days: int
    cleanup_staging: bool
    max_memory_usage: int
    max_insert_threads: int
    clickhouse_host: str
    clickhouse_port: int
    clickhouse_user: str
    clickhouse_password: str
    clickhouse_db: str
    ch_user_files_dir: Path
    sql_dir: Path


def load_settings(env: Mapping[str, str] | None = None) -> Settings:
    """Đọc Settings từ env; raise :class:`ConfigError` nêu rõ biến nào thiếu/sai."""
    env = env if env is not None else os.environ

    missing = [key for key in _REQUIRED if not env.get(key, "").strip()]
    if missing:
        raise ConfigError(
            "Thiếu biến môi trường bắt buộc: " + ", ".join(missing) + " (xem .env.example)"
        )

    strategy = env.get("INGEST_STRATEGY", "s3").strip().lower()
    if strategy not in _STRATEGIES:
        raise ConfigError(f"INGEST_STRATEGY phải là {_STRATEGIES}, nhận được: {strategy!r}")
    if strategy == "s3" and not (
        env.get("GCS_HMAC_ACCESS_KEY", "").strip() and env.get("GCS_HMAC_SECRET", "").strip()
    ):
        raise ConfigError(
            "INGEST_STRATEGY=s3 cần GCS_HMAC_ACCESS_KEY + GCS_HMAC_SECRET "
            "(tạo ở Cloud Storage -> Settings -> Interoperability). "
            "Chưa có HMAC thì đặt INGEST_STRATEGY=file."
        )

    return Settings(
        gcp_project_id=env["GCP_PROJECT_ID"].strip(),
        bq_dataset=env["BQ_DATASET"].strip(),
        bq_location=env.get("BQ_LOCATION", "US").strip() or "US",
        gcs_bucket=env["GCS_BUCKET"].strip(),
        gcs_raw_prefix=env.get("GCS_RAW_PREFIX", "").strip().strip("/"),
        gcs_staging_prefix=env["GCS_STAGING_PREFIX"].strip().strip("/"),
        ingest_strategy=strategy,
        raw_ttl_days=_int(env, "RAW_TTL_DAYS", 0),
        cleanup_staging=env.get("CLEANUP_STAGING", "true").strip().lower() in _TRUE,
        max_memory_usage=_int(env, "MAX_MEMORY_USAGE", 4_000_000_000),
        max_insert_threads=_int(env, "MAX_INSERT_THREADS", 4),
        clickhouse_host=env["CLICKHOUSE_HOST"].strip(),
        clickhouse_port=_int(env, "CLICKHOUSE_PORT", 8123),
        clickhouse_user=env["CLICKHOUSE_USER"].strip(),
        clickhouse_password=env["CLICKHOUSE_PASSWORD"],
        clickhouse_db=env["CLICKHOUSE_DB"].strip(),
        ch_user_files_dir=Path(
            env.get("CH_USER_FILES_DIR", "/var/lib/clickhouse/user_files").strip()
        ),
        sql_dir=(
            Path(env["FB_SQL_DIR"].strip())
            if env.get("FB_SQL_DIR", "").strip()
            else _default_sql_dir()
        ),
    )
