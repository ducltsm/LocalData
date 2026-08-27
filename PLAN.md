# PLAN — Firebase (GA4) BigQuery → ClickHouse

> **Update 2026-08-28 (Phase 2):** đã implement flatten theo yêu cầu mới —
> `fb.events_flat` + registry tự thêm cột (xem ROADMAP.md). Schema raw được mở rộng
> đủ 31 cột theo Parquet thật (sample-parquet), re-ingest 2 ngày 26–27/8, flatten
> chạy trong DAG daily; đã chứng kiến 5 cột tự thêm trên dữ liệu thật ngày 27/8.
> 33/33 test pass (thêm test flatten auto-add + idempotency).

## Phase 1: raw loader

> Trạng thái: **ĐÃ IMPLEMENT + VERIFY** (2026-08-28). Kết quả verify trên container thật:
> - 24/24 pytest pass (kể cả integration: Parquet nested → file() → ClickHouse → idempotency)
> - `allow_nullable_key=1` cần thiết và hoạt động; `DROP PARTITION` trên partition
>   không tồn tại là no-op (an toàn cho lần chạy đầu)
> - Cú pháp tuple trên 24.8: `p.1`/`.2.2` (positional) và `tupleElement(...)` đều OK;
>   chained named access `.value.string_value` sau function call KHÔNG hoạt động → code dùng tupleElement
> - Khác với giả định của đề bài: `parseDateTimeBestEffortOrNull('20260827')` THỰC RA parse được
>   trên 24.8; vẫn dùng `parseDateTimeOrNull('%Y%m%d')` cho tường minh
> - User non-default cần `named_collection_control=1` (users.d) để dùng named collection
>   với s3() — đã cấu hình; s3(gcs_raw, url ảo) trả 404 đúng kỳ vọng
> - Port 8080/8081 trên máy này bận → thêm `AIRFLOW_WEBSERVER_PORT` (.env local đặt 8090)

## 0. Khảo sát môi trường (đã chạy thật)

| Hạng mục | Kết quả | Hệ quả |
|---|---|---|
| `D:\Project\LocalData` | Rỗng | Dùng chính thư mục này làm project root (không tạo thêm cấp `firebase-clickhouse-pipeline/`), `git init` tại đây |
| Docker / Compose | 28.4.0 / v2.39.4 (Desktop) | Verify bằng container thật được ✔ |
| `make` | **Không có** trên host | Vẫn viết `Makefile` đúng spec; khi verify tôi chạy thẳng lệnh `docker compose ...` tương đương. Checklist cuối sẽ có bước cài make (`winget install ezwinports.make` hoặc dùng WSL) |
| Python trên host | **Không có** | Toàn bộ `pytest`/`ruff` chạy **trong container Airflow** (`docker compose exec`). Đây cũng là cách README hướng dẫn — không phụ thuộc Python host |
| PowerShell system | Lỗi assembly | Mọi thao tác shell dùng Git Bash |

## 1. Quyết định kỹ thuật & lý do

1. **Project root = `D:\Project\LocalData`** — thư mục rỗng bạn đã mở Claude Code, đúng tinh thần "paste vào thư mục rỗng". Cấu trúc bên trong y hệt mục 3 của yêu cầu.
2. **ClickHouse `clickhouse/clickhouse-server:24.8`** (LTS) — port 8123/9000, `config.d` (logging, memory `max_server_memory_usage_to_ram_ratio=0.8`, s3), `users.d`, named volume data, `ulimits nofile 262144`, healthcheck `/ping`.
3. **GCS HMAC credential qua named collection** (`clickhouse/config.d/s3.xml`, dùng `from_env` đọc `GCS_HMAC_ACCESS_KEY/SECRET`) thay vì nhét trực tiếp vào chuỗi SQL. Lý do: secret không xuất hiện trong SQL render ra log Airflow / `query_log` của ClickHouse. `read_source.sql.j2` strategy `s3` tham chiếu named collection + URL + structure.
4. **Airflow `apache/airflow:2.10.5-python3.12`**, `LocalExecutor`, Postgres 16-alpine, YAML anchor `x-airflow-common`, pip cài với constraint chính thức của 2.10.5/py3.12. `AIRFLOW__CORE__DEFAULT_TIMEZONE=Asia/Ho_Chi_Minh` để cron `0 4 * * *` chạy 4h sáng giờ VN, và note rõ trong README rằng `ds` là logical date (ngày dữ liệu, thường = hôm qua khi run 4h sáng).
5. **Shared named volume `ch_user_files`** mount `/var/lib/clickhouse/user_files` vào cả ClickHouse lẫn 2 container Airflow (cùng path) — nền cho strategy `file()` và cho integration test (test chạy trong container Airflow nên ghi Parquet thẳng vào volume này được, không cần Python host).
6. **`src/fb_pipeline` cài `pip install -e`** trong image Airflow: Dockerfile COPY `pyproject.toml` + `src/` vào `/opt/fb_pipeline` rồi install editable; compose mount `./src` đè lên để sửa code không cần rebuild. Dev deps (`pytest`, `ruff`, `pyarrow`) nằm trong extra `[dev]`, cài sẵn trong image vì đây là stack local dev.
7. **`source_schema.py` là single source of truth** cho structure Parquet: một constant dạng list `(tên_cột, kiểu)` → render ra chuỗi structure cho `s3()`/`file()`. `02_events_raw.sql` là file SQL tĩnh (đọc được, review được); **unit test đối chiếu từng cột** giữa hai bên để bắt lệch schema. Structure khởi điểm lấy đúng mục 6 của yêu cầu, sau đó **bắt buộc chạy `make sample-parquet DATE=2026-08-27` trên file thật để chốt** — tôi sẽ ghi chú TODO này ở checklist vì tôi không có credential để tự chạy.
8. **`fb.events_raw`**: cột khớp source_schema + `_dt Date / _ingested_at / _run_id / _source_table / _is_intraday`; `MergeTree PARTITION BY _dt ORDER BY (_dt, event_name, user_pseudo_id, event_timestamp)`; `event_date_d Date MATERIALIZED parseDateTimeOrNull(event_date, '%Y%m%d')` (sẽ verify hàm + format trên container 24.8 trước khi chốt); ZSTD(1) cho String dài + nested; TTL chỉ render khi `RAW_TTL_DAYS > 0`. `ORDER BY` chứa `user_pseudo_id Nullable(String)` → sẽ dùng `coalesce`/`assumeNotNull`? **Không** — ClickHouse không cho Nullable trong ORDER BY mặc định; quyết định: bật `allow_nullable_key=1` ở table settings để giữ cột trung thực với nguồn (không đổi kiểu). Điểm này sẽ test thật trên container.
9. **Idempotency = `ALTER TABLE ... DROP PARTITION '{{ ds }}'` trước insert** — partition theo `_dt` nên drop luôn đúng phạm vi run cũ, miễn nhiễm lệch timezone `event_date`/`event_timestamp` (ghi rõ lý do trong README).
10. **Insert**: một câu `INSERT INTO ... SELECT <liệt kê cột tường minh> FROM <read_source>` render từ `insert_raw.sql.j2`, settings `input_format_parquet_allow_missing_columns=1`, `input_format_null_as_default=1`, `input_format_parquet_case_insensitive_column_matching=1`, `max_insert_threads`/`max_memory_usage` từ env, timeout dài + `send_progress_in_http_headers` (qua `clickhouse-connect`).
11. **DAG backfill**: một task Python loop tuần tự `date_from → date_to` (max_active_runs=1), tái dùng đúng các hàm trong `src/fb_pipeline`; khi `use_existing_gcs=true` sẽ list prefix `analytics_352963567/events_intraday/`, in vài object đầu để detect layout thật rồi mới build pattern đọc — không giả định trước.
12. **`ingestion_log`** ghi bằng `clickhouse-connect` từ task `write_ingestion_log` (`trigger_rule=ALL_DONE`), status `success/failed/skipped` + error message.
13. **Credential**: key thật đang ở `D:\Project\apache_beam_scripts\service-account\sm-data-center.json` (ngoài repo). Tôi **không copy, không commit**; `.gitignore` chặn `secrets/`, `.env`, `*.json` (trừ `*.example.json`). Bạn tự copy vào `secrets/gcp-sa.json` (hoặc bảo tôi copy giúp — file đích đã được gitignore). **Lưu ý bảo mật:** key này vừa được đính kèm vào hội thoại → nên rotate sau khi setup xong; README có mục này.
14. **IAM cross-project**: SA `sm-data-center@sm-data-center.iam.gserviceaccount.com` thuộc project `sm-data-center` nhưng BQ dataset + bucket thuộc `chat-gpt-fb449` → role phải gán **trên tài nguyên đích**: `roles/bigquery.jobUser` (project `chat-gpt-fb449`), `roles/bigquery.dataViewer` (dataset `analytics_352963567`), `roles/storage.objectAdmin` (bucket). HMAC key tạo cho chính SA này. README ghi rõ.
15. **RAM**: default `.env` — ClickHouse `mem_limit 6g`, `MAX_MEMORY_USAGE 4G`, Airflow scheduler/webserver ~2g mỗi cái, Postgres 512m → README yêu cầu tối thiểu **12GB RAM** cho Docker Desktop (có thể hạ qua `.env`).

## 2. Thứ tự tạo file

1. **Infra**: `.gitignore` → `git init` → `docker-compose.yml`, `.env.example`, `docker/airflow/{Dockerfile,requirements.txt}`, `clickhouse/config.d/*`, `clickhouse/users.d/*`, `secrets/{.gitkeep,gcp-sa.example.json}`, `Makefile`, `pyproject.toml`
2. **Schema**: `src/fb_pipeline/clickhouse/source_schema.py` → `clickhouse/sql/01_database.sql`, `02_events_raw.sql`, `03_ingestion_log.sql`
3. **Templates**: `clickhouse/sql/ingest/read_source.sql.j2`, `insert_raw.sql.j2`, `src/fb_pipeline/bq/sql/dump_raw.sql.j2`
4. **`src/`**: `config.py`, `bq/{client,export}.py`, `gcs/client.py`, `clickhouse/{client,ddl,ingest}.py`
5. **DAGs**: `firebase_raw_daily.py`, `firebase_raw_backfill.py`, `clickhouse_maintenance.py`
6. **Tests**: unit (schema-sync, render template, config, resolve_source) + integration (Parquet nested thật qua `file()`, idempotency)
7. **Docs**: `README.md`, `ROADMAP.md` (Phase 2)

## 3. Kế hoạch verify bằng container thật (trước khi báo xong)

1. `docker compose config` sạch lỗi; build image Airflow.
2. `docker compose up` ClickHouse → apply `clickhouse/sql/` → xác nhận bảng tạo được (đặc biệt `allow_nullable_key`, codec, MATERIALIZED column).
3. Verify trên `clickhouse-client` trong container: `parseDateTimeOrNull('20260827','%Y%m%d')`, cú pháp tuple `p.1`/`p.2.2` cho `explore-keys`, `DESCRIBE file(...)`.
4. Sinh Parquet GA4 giả bằng `pyarrow` (trong container Airflow) → đặt vào shared volume → chạy integration test thật: insert qua `file()`, đối chiếu số dòng, đọc nested value, DROP PARTITION + insert lại (idempotency).
5. Airflow: `airflow dags list` không lỗi import; `pytest` (unit + integration) + `ruff` + `python -m compileall` pass trong container.
6. Những gì **không** verify được vì cần credential thật (export BQ, đọc GCS bằng HMAC): ghi rõ trong checklist tay.

## 4. Rủi ro / điểm mở

- **Structure Parquet thật có thể khác** constant khởi điểm (GA4 thay đổi theo property/SDK — ví dụ có thể có thêm `items`, `ecommerce`, `collected_traffic_source`, `is_active_user`...). `EXPORT DATA SELECT *` sẽ dump **tất cả** cột, nhưng nhờ `input_format_parquet_allow_missing_columns` + structure tường minh, ClickHouse chỉ đọc các cột khai báo — cột thừa trong file bị bỏ qua, không vỡ. Bước `make sample-parquet` là bắt buộc trước lần chạy DAG đầu tiên.
- `ORDER BY` chứa cột Nullable → cần `allow_nullable_key=1` (mục 1.8), sẽ chốt bằng test thật.
- Cron `0 4 * * *` theo `default_timezone` Asia/Ho_Chi_Minh — nếu bạn muốn giờ UTC thì nói lại.

## 5. Checklist việc tay của bạn (bản preview, README sẽ có bản đầy đủ)

1. Copy key: `D:\Project\apache_beam_scripts\service-account\sm-data-center.json` → `secrets/gcp-sa.json`
2. Tạo **HMAC key** cho SA trên GCS (Cloud Storage → Settings → Interoperability) → điền `.env`
3. Gán IAM role trên project/dataset/bucket của `chat-gpt-fb449` (mục 1.14)
4. Docker Desktop ≥ 12GB RAM
5. Cài `make` (`winget install ezwinports.make`) hoặc dùng WSL — hoặc chạy lệnh `docker compose` tương đương in trong README
6. `make sample-parquet DATE=2026-08-27` để đối chiếu schema thật, sửa `source_schema.py` nếu lệch, rồi mới trigger DAG
7. **Rotate service account key** sau khi setup (key đã bị expose khi đính kèm vào hội thoại)
