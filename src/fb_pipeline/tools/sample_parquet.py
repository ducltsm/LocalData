"""Công cụ chính để chốt source_schema.py: DESCRIBE file Parquet THẬT rồi diff.

Gọi: make sample-parquet DATE=2026-08-27  [PREFIX=analytics_.../events_intraday/...]

- List object dưới staging prefix (hoặc --prefix chỉ định) của ngày đó
- DESCRIBE qua s3() (nếu có HMAC) hoặc tải 1 file về user_files rồi DESCRIBE qua file()
- In diff tên cột so với SOURCE_COLUMNS đã khai báo

Lưu ý khi đọc diff: schema inference của ClickHouse bọc gần như mọi thứ trong
Nullable(...) — lệch kiểu kiểu đó là bình thường; thứ phải khớp là TÊN cột và
cấu trúc nested. Cột có trong file nhưng chưa khai báo sẽ bị BỎ QUA khi ingest;
cột khai báo mà file không có sẽ thành NULL/default (allow_missing_columns).
"""

from __future__ import annotations

import argparse
import logging
import re

from fb_pipeline.clickhouse import source_schema
from fb_pipeline.clickhouse.client import get_client
from fb_pipeline.clickhouse.ingest import check_ds
from fb_pipeline.config import Settings, load_settings
from fb_pipeline.gcs import client as gcs

log = logging.getLogger(__name__)


def _describe_via_s3(settings: Settings, object_name: str) -> list[tuple[str, str]]:
    client = get_client(settings)
    url = f"https://storage.googleapis.com/{settings.gcs_bucket}/{object_name}"
    result = client.query(f"DESCRIBE TABLE s3(gcs_raw, url = '{url}', format = 'Parquet')")
    return [(str(r[0]), str(r[1])) for r in result.result_rows]


def _describe_via_file(settings: Settings, object_name: str, ds: str) -> list[tuple[str, str]]:
    sample_dir = settings.ch_user_files_dir / "samples"
    sample_dir.mkdir(parents=True, exist_ok=True)
    target = sample_dir / f"sample_{ds}.parquet"
    storage = gcs.get_client(settings.gcp_project_id)
    storage.bucket(settings.gcs_bucket).blob(object_name).download_to_filename(str(target))
    log.info("Đã tải gs://%s/%s -> %s", settings.gcs_bucket, object_name, target)
    client = get_client(settings)
    result = client.query(f"DESCRIBE TABLE file('samples/sample_{ds}.parquet', 'Parquet')")
    return [(str(r[0]), str(r[1])) for r in result.result_rows]


def _norm(type_str: str) -> str:
    return re.sub(r"\s+", "", type_str)


def _print_diff(actual: list[tuple[str, str]]) -> None:
    declared = dict(source_schema.SOURCE_COLUMNS)
    actual_map = dict(actual)

    print("\n=== Schema THẬT của file Parquet (DESCRIBE) ===")
    for name, type_ in actual:
        print(f"  {name}: {type_}")

    extra = [n for n in actual_map if n not in declared]
    missing = [n for n in declared if n not in actual_map]
    mismatched = [
        n
        for n in declared
        if n in actual_map and _norm(declared[n]) != _norm(actual_map[n])
    ]

    print("\n=== Diff với source_schema.py ===")
    if extra:
        print("Cột CÓ trong file nhưng CHƯA khai báo (hiện bị bỏ qua khi ingest):")
        for n in extra:
            print(f"  + {n}: {actual_map[n]}")
    if missing:
        print("Cột khai báo nhưng KHÔNG có trong file (sẽ NULL/default khi ingest):")
        for n in missing:
            print(f"  - {n}: {declared[n]}")
    if mismatched:
        print("Cột lệch kiểu (Nullable-khác-biệt do schema inference là bình thường):")
        for n in mismatched:
            print(f"  ~ {n}:\n      khai báo : {declared[n]}\n      file     : {actual_map[n]}")
    if not (extra or missing or mismatched):
        print("Khớp hoàn toàn — không cần sửa source_schema.py.")
    else:
        print(
            "\nNếu cần sửa: cập nhật SOURCE_COLUMNS trong "
            "src/fb_pipeline/clickhouse/source_schema.py và clickhouse/sql/02_events_raw.sql "
            "(unit test test_schema_sync sẽ bắt lệch giữa hai file)."
        )


def main() -> None:
    """Entry point CLI."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True, help="YYYY-MM-DD")
    parser.add_argument(
        "--prefix",
        default=None,
        help="Prefix GCS chứa parquet (mặc định: <GCS_STAGING_PREFIX>/dt=<date>/)",
    )
    args = parser.parse_args()
    ds = check_ds(args.date)

    settings = load_settings()
    prefix = args.prefix or f"{settings.gcs_staging_prefix}/dt={ds}/"

    storage = gcs.get_client(settings.gcp_project_id)
    objects = gcs.list_objects(storage, settings.gcs_bucket, prefix, max_results=20)
    parquets = [(n, s) for n, s in objects if n.endswith(".parquet")]
    print(f"gs://{settings.gcs_bucket}/{prefix} — {len(objects)} object đầu tiên:")
    for name, size in objects[:10]:
        print(f"  {name} ({size:,} bytes)")
    if not parquets:
        print(
            "\nKhông thấy file .parquet nào. Chạy EXPORT DATA trước (trigger DAG hoặc "
            "task dump_raw_to_gcs), hoặc chỉ định --prefix trỏ tới chỗ có sẵn parquet."
        )
        raise SystemExit(1)

    object_name = parquets[0][0]
    if settings.ingest_strategy == "s3":
        actual = _describe_via_s3(settings, object_name)
    else:
        actual = _describe_via_file(settings, object_name, ds)
    _print_diff(actual)


if __name__ == "__main__":
    main()
