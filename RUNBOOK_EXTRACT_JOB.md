# RUNBOOK — chuyển dump BigQuery sang extract job (miễn phí) và cách chạy tay

> Trạng thái: code ĐÃ ĐỔI xong (commit này), unit test + lint pass, **chưa chạy
> với BigQuery thật**. Runbook này là các bước chạy tay + kiểm chứng.

## Đã đổi gì

| | Trước (`EXPORT DATA`) | Sau (**extract job**) |
|---|---|---|
| Chi phí | Tính như query theo bytes quét: du02 ~57GB logical/ngày ≈ **$0.36/ngày**, backfill 1 năm ≈ $130 | **$0** (extract job không dùng slot, không tính tiền) |
| Output | Parquet/SNAPPY, nested giữ nguyên | Y hệt |
| Overwrite | Tự dọn (`overwrite=true`) | KHÔNG có → pipeline tự xoá prefix trước khi extract (task `clean_staging_prefix` / `gcs.delete_prefix` trong backfill) |
| Quota | — | Extract miễn phí tới 50TB/ngày/project — dư xa |

File đã sửa: `src/fb_pipeline/bq/export.py` (thêm `run_extract` + `staging_uri`,
`render_export_sql`/`run_export` giữ lại đánh dấu LEGACY), `airflow/dags/firebase_raw_daily.py`
(`BigQueryToGCSOperator` + task `clean_staging_prefix`), `src/fb_pipeline/backfill.py`.

## Bước 1 — Chạy thử app nhỏ trước (chat01, `fb`)

```bash
docker compose exec airflow-scheduler airflow dags test firebase_raw_daily 2026-08-28
```

(`DATE` = ngày muốn nạp, DAG tự fallback intraday nếu chưa có bảng final.)

**Kỳ vọng trong log:** task `clean_staging_prefix` chạy trước `dump_raw_to_gcs`;
`dump_raw_to_gcs` giờ là `BigQueryToGCSOperator`; các bước sau (verify → insert →
QC → flatten) y như cũ; `DagRun ... state=success`.

## Bước 2 — Kiểm chứng phí quét = 0

Cách chắc chắn nhất: xem loại job trên BigQuery. Chạy trong container:

```bash
docker compose exec airflow-scheduler python -c "
from google.cloud import bigquery
c = bigquery.Client(project='chat-gpt-fb449')
for j in c.list_jobs(max_results=10):
    kind = j.job_type
    billed = getattr(j, 'total_bytes_billed', None)
    print(j.job_id[:40], '| type:', kind, '| bytes_billed:', billed)
"
```

**Kỳ vọng:** job dump mới nhất có `type: extract` (không phải `query`), không có
`bytes_billed`. Job `query` duy nhất còn lại là `SELECT count(*)` — count không
WHERE trả lời từ metadata, **0 bytes billed** (kiểm tra thấy `bytes_billed: 0`).

Xem trên Console cũng được: BigQuery → Job history → job mới nhất phải là **Extract**.

## Bước 3 — Chạy app lớn (du02)

Chạy một ngày mới qua đường backfill/multi-app (đổi ngày theo nhu cầu):

```bash
docker compose exec -T \
  -e GCP_PROJECT_ID=dress-up-2 \
  -e BQ_DATASET=analytics_240445616 \
  -e GCS_BUCKET=du02-android-backup-from-bigquery-table-sufix \
  -e GCS_RAW_PREFIX=analytics_240445616/events_intraday \
  -e CLICKHOUSE_DB=du02 \
  -e MAX_INSERT_THREADS=2 \
  airflow-scheduler bash -c '
    python -c "
from fb_pipeline.config import load_settings
from fb_pipeline.backfill import process_day
print(process_day(load_settings(), \"2026-08-28\", \"manual-du02\", use_existing_gcs=False))" &&
    python -m fb_pipeline.tools.flatten_day --date 2026-08-28'
```

Lưu ý du02 trên máy dev: chạy nền nếu không muốn giữ terminal (download ~7.5GB mất
~25 phút với strategy `file`); RAM ClickHouse cần như `.env` hiện tại
(`CLICKHOUSE_MEM_LIMIT=9g`, `MAX_MEMORY_USAGE=6500000000`). Flatten partition >10M
dòng tự chia chunk — không cần làm gì thêm.

Kiểm chứng phí như Bước 2 nhưng `project='dress-up-2'`.

## Bước 4 — Đối chiếu kết quả

```sql
-- số dòng khớp giữa raw và flat, và khớp count() trên BigQuery
SELECT (SELECT count() FROM du02.events_raw  WHERE _dt = '2026-08-28') AS raw,
       (SELECT count() FROM du02.events_flat WHERE _dt = '2026-08-28') AS flat;
SELECT * FROM du02.ingestion_log ORDER BY started_at DESC LIMIT 3;
```

Bảng intraday cho phép lệch ≤0.5% so với count (streaming); bảng final phải khớp 0.

## Nếu có vấn đề — rollback

Đường `EXPORT DATA` cũ vẫn còn nguyên (LEGACY) trong `bq/export.py`
(`render_export_sql` + `run_export`) và template `src/fb_pipeline/bq/sql/dump_raw.sql.j2`.
Muốn quay lại: revert commit này (`git revert <sha>`), hoặc sửa tạm
`backfill.py`/DAG gọi lại `run_export(render_export_sql(...))`. Không có thay đổi
schema/dữ liệu nào đi kèm — rollback thuần code.

## Lỗi có thể gặp

| Triệu chứng | Xử lý |
|---|---|
| Extract job báo `Permission denied` | SA cần `bigquery.jobUser` (project) + quyền đọc bảng — giống EXPORT DATA, không cần role mới |
| File trùng/nhiều hơn kỳ vọng trên staging | `clean_staging_prefix` bị skip? Kiểm tra task này chạy trước `dump_raw_to_gcs`; xoá tay prefix `staging_raw/dt=<ngày>/` rồi chạy lại |
| Cần export CÓ LỌC (một phần bảng) | Đó là lúc duy nhất quay lại `EXPORT DATA` (trả phí quét) — dùng hàm LEGACY |
