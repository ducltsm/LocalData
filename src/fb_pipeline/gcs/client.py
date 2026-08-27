"""Client GCS mỏng — chỉ thao tác object (list/download/delete), không đọc dữ liệu."""

from __future__ import annotations

import logging
from pathlib import Path

from google.cloud import storage

log = logging.getLogger(__name__)


def get_client(project_id: str | None = None) -> storage.Client:
    """Tạo client GCS (credential từ GOOGLE_APPLICATION_CREDENTIALS)."""
    return storage.Client(project=project_id)


def list_objects(
    client: storage.Client, bucket: str, prefix: str, max_results: int | None = None
) -> list[tuple[str, int]]:
    """Trả về [(tên object, size bytes)] dưới prefix."""
    blobs = client.list_blobs(bucket, prefix=prefix, max_results=max_results)
    return [(blob.name, int(blob.size or 0)) for blob in blobs]


def download_prefix(
    client: storage.Client,
    bucket: str,
    prefix: str,
    dest_dir: Path,
    suffix: str = ".parquet",
) -> list[Path]:
    """Tải mọi object *suffix dưới prefix về dest_dir (giữ nguyên tên file).

    Hai chi tiết chống race (đã dính thật với EXPORT DATA overwrite chạy chồng):
    - snapshot danh sách tên TRƯỚC khi tải — list_blobs phân trang lazy, các trang
      sau có thể trả object của một lần export khác nếu list trong lúc tải;
    - tải qua ``bucket.blob(name)`` KHÔNG pin generation — blob từ list_blobs mang
      generation cũ, object bị ghi đè là 404 dù tên vẫn tồn tại.
    """
    dest_dir.mkdir(parents=True, exist_ok=True)
    names = [
        blob.name
        for blob in client.list_blobs(bucket, prefix=prefix)
        if blob.name.endswith(suffix)
    ]
    bucket_ref = client.bucket(bucket)
    downloaded: list[Path] = []
    for i, name in enumerate(names, 1):
        target = dest_dir / Path(name).name
        bucket_ref.blob(name).download_to_filename(str(target))
        downloaded.append(target)
        if i % 50 == 0 or i == len(names):
            log.info("Đã tải %d/%d file từ gs://%s/%s", i, len(names), bucket, prefix)
    return downloaded


def delete_prefix(client: storage.Client, bucket: str, prefix: str) -> int:
    """Xoá mọi object dưới prefix; trả về số object đã xoá."""
    count = 0
    for blob in client.list_blobs(bucket, prefix=prefix):
        blob.delete()
        count += 1
    log.info("Đã xoá %d object dưới gs://%s/%s", count, bucket, prefix)
    return count


def detect_day_prefix(
    client: storage.Client,
    bucket: str,
    base_prefix: str,
    ds: str,
    ds_nodash: str,
) -> str | None:
    """Detect layout thư mục thực tế của prefix raw có sẵn cho một ngày.

    Thử lần lượt các pattern hay gặp; nếu không trúng, list vài object đầu của
    base_prefix để người vận hành nhìn thấy layout thật (không giả định).
    Trả về prefix (kết thúc bằng '/') chứa file .parquet của ngày đó, hoặc None.
    """
    base = base_prefix.strip("/")
    candidates = [
        f"{base}/dt={ds}/",
        f"{base}/{ds}/",
        f"{base}/dt={ds_nodash}/",
        f"{base}/{ds_nodash}/",
    ]
    for candidate in candidates:
        objs = list_objects(client, bucket, candidate, max_results=5)
        if any(name.endswith(".parquet") for name, _ in objs):
            log.info("Trúng layout: gs://%s/%s (%d object đầu)", bucket, candidate, len(objs))
            return candidate

    sample = list_objects(client, bucket, base + "/", max_results=10)
    log.warning(
        "Không detect được layout cho ngày %s dưới gs://%s/%s/. 10 object đầu tiên: %s",
        ds,
        bucket,
        base,
        [name for name, _ in sample] or "(rỗng)",
    )
    return None
