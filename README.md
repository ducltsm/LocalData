# Firebase (GA4) → ClickHouse — Phase 1: raw loader

Pipeline nạp dữ liệu Firebase Analytics (GA4) theo ngày từ BigQuery vào ClickHouse,
chạy hoàn toàn bằng Docker Compose (ClickHouse + Apache Airflow). **Phase 1 dừng ở
việc load file raw vào `fb.events_raw`, giữ nguyên cấu trúc nested** — không flatten,
không schema registry (xem [ROADMAP.md](ROADMAP.md)).

## Kiến trúc

```
BigQuery events_[intraday_]YYYYMMDD
        │  EXPORT DATA (SELECT * — không transform)
        ▼
GCS gs://<bucket>/staging_raw/dt=YYYY-MM-DD/part-*.parquet
        │  ClickHouse tự đọc: s3() hoặc file()
        ▼
fb.events_raw   (giữ nguyên nested: event_params, user_properties, device, geo…)
```

Nguyên tắc: BigQuery chỉ dump thô; mọi xử lý sau này làm trong ClickHouse;
Airflow chỉ orchestrate — Python không parse dữ liệu.

## Prerequisites

| Yêu cầu | Ghi chú |
|---|---|
| Docker Desktop / Engine + Compose v2 | đã test với Docker 28.x |
| **RAM tối thiểu 12GB cho Docker** | ClickHouse 6g + 2× Airflow 2g + Postgres. Hạ được qua `CLICKHOUSE_MEM_LIMIT`/`AIRFLOW_MEM_LIMIT`/`MAX_MEMORY_USAGE` trong `.env`, tối thiểu thực dụng ~8GB |
| GNU make (khuyến nghị) | Windows: `winget install ezwinports.make` rồi dùng Git Bash, hoặc WSL. Không có make thì chạy lệnh `docker compose` tương đương trong Makefile |
| Không cần Python trên host | test/lint chạy trong container Airflow |

### IAM role tối thiểu cho service account

Service account (`sm-data-center@sm-data-center.iam.gserviceaccount.com`) thuộc
project khác với dữ liệu (`chat-gpt-fb449`), nên role phải gán **trên tài nguyên đích**:

- `roles/bigquery.jobUser` — trên project `chat-gpt-fb449` (chạy query/EXPORT)
- `roles/bigquery.dataViewer` — trên dataset `analytics_352963567`
- `roles/storage.objectAdmin` — trên bucket `chat-gpt-android-backup-from-bigquery-table-sufix`
  (cần quyền ghi + xoá cho staging)

## Setup (5 bước)

1. `make init` — tạo `.env` từ `.env.example`, rồi điền `CLICKHOUSE_PASSWORD` và các giá trị GCP.
2. Đặt key JSON của service account vào `secrets/gcp-sa.json`
   (mount read-only vào container qua `GOOGLE_APPLICATION_CREDENTIALS`; đã bị `.gitignore` chặn).
3. Tạo **HMAC key** cho GCS (xem mục dưới) và điền `GCS_HMAC_ACCESS_KEY/SECRET` vào `.env`.
   Chưa có HMAC? Đặt `INGEST_STRATEGY=file` để chạy tạm qua shared volume.
4. `make up` — build image, khởi động ClickHouse + Airflow, apply schema.
   Airflow UI: <http://localhost:8080> (user/pass theo `.env`; port 8080 bận thì
   đổi `AIRFLOW_WEBSERVER_PORT`).
5. **Trước lần chạy đầu:** `make sample-parquet DATE=2026-08-27` để DESCRIBE file Parquet
   thật và đối chiếu với `source_schema.py` (xem mục Schema). Sau đó trigger DAG:
   `make dag-test DATE=2026-08-27` hoặc bật `firebase_raw_daily` trên UI.

Kiểm tra kết quả:

```bash
make peek DATE=2026-08-27
```

```sql
SELECT event_params FROM fb.events_raw LIMIT 1;  -- phải trả về nested đọc được
```

## Tạo HMAC key cho GCS (bắt buộc cho INGEST_STRATEGY=s3)

`s3()` của ClickHouse nói chuyện với GCS qua giao thức S3-compatible, xác thực bằng
**HMAC key** — **key JSON của service account KHÔNG dùng được** (đây là lỗi 403 phổ
biến nhất). Cách tạo:

1. Cloud Console → **Cloud Storage → Settings → Interoperability**
2. Mục *Access keys for service accounts* → **Create a key for a service account**
3. Chọn đúng service account của pipeline → nhận `Access key` + `Secret`
4. Điền vào `.env` (`GCS_HMAC_ACCESS_KEY`, `GCS_HMAC_SECRET`) → `docker compose up -d clickhouse` lại

Credential này được nạp vào ClickHouse qua named collection `gcs_raw`
(`clickhouse/config.d/s3.xml`, đọc từ env) — không xuất hiện trong SQL/log.

## Bảng `fb.events_raw`

- Cột nguồn giữ **nguyên kiểu GA4**: `event_params Array(Tuple(...))`,
  `device/geo/app_info/... Tuple(...)`, `event_timestamp Int64` (micro giây, không convert).
- Cột metadata: `_dt Date` (logical date của run), `_ingested_at`, `_run_id`,
  `_source_table`, `_is_intraday`.
- `event_date_d Nullable(Date) MATERIALIZED` — parse từ `event_date` cho tiện query.
- `ENGINE = MergeTree PARTITION BY _dt ORDER BY (_dt, event_name, user_pseudo_id, event_timestamp)`.

**Vì sao partition theo `_dt` chứ không theo `event_date`?** `event_date` của GA4 là
string theo *reporting timezone* của property, còn `event_timestamp` là UTC — hai cái
lệch nhau ở biên ngày. Partition theo `_dt` (ngày của bảng nguồn / logical date của
DAG run) khiến `DROP PARTITION` luôn xoá đúng và đủ những gì run đó đã ghi — đây là
toàn bộ cơ chế idempotency: chạy lại DAG cùng ngày cho kết quả y hệt, không duplicate.
Chuyện timezone sẽ xử lý ở phase 2 khi flatten.

TTL: mặc định `RAW_TTL_DAYS=0` = giữ vĩnh viễn (phase này `events_raw` là đích cuối,
không phải staging). Đặt `>0` rồi `make ch-schema` nếu muốn tự xoá theo `_dt`.

`fb.ingestion_log` ghi mọi lần chạy (kể cả fail/skip): số dòng BQ, số file/bytes đã
đọc, số dòng insert, thời gian, trạng thái.

## Schema nguồn — verify bắt buộc trước lần chạy đầu

Structure trong [`src/fb_pipeline/clickhouse/source_schema.py`](src/fb_pipeline/clickhouse/source_schema.py)
là khởi điểm theo schema GA4 chuẩn, **không phải chân lý** — field khác nhau tuỳ
property/version SDK. Quy trình chốt schema:

```bash
make sample-parquet DATE=2026-08-27
```

Tool sẽ DESCRIBE file Parquet thật (qua `s3()` hoặc `file()`) và in diff với structure
đã khai báo. Nếu lệch: sửa `SOURCE_COLUMNS` + `clickhouse/sql/02_events_raw.sql`
(unit test `test_schema_sync` bắt lệch giữa hai file này).

Mọi lệnh đọc Parquet đều chạy với `input_format_parquet_allow_missing_columns=1` và
`input_format_null_as_default=1` — Google thêm/bớt field sẽ không làm vỡ pipeline
(cột thiếu thành NULL/default, cột thừa bị bỏ qua).

**Vì sao phải là Parquet?** CSV mất nested hoàn toàn; JSON thì BigQuery ghi `INT64`
thành string nên phải cast thêm ở đầu đọc. Parquet là format duy nhất giữ được
`ARRAY<STRUCT<…>>` của GA4 một cách trung thực.

## DAGs

| DAG | Lịch | Việc |
|---|---|---|
| `firebase_raw_daily` | `0 4 * * *` (giờ VN), catchup | resolve nguồn (final → fallback intraday → skip) → count BQ → `EXPORT DATA` → verify GCS → (stage nếu `file`) → `DROP PARTITION` → `INSERT` → quality checks → **flatten vào `fb.events_flat`** → cleanup → ghi log |
| `firebase_raw_backfill` | manual | params `date_from`/`date_to`/`use_existing_gcs` (default `true` — đọc thẳng prefix `analytics_352963567/events_intraday/` có sẵn, tự detect layout, bỏ qua export BQ). Tuần tự từng ngày. Lưu ý: prefix có sẵn chứa file `.gz` (không phải Parquet) nên `use_existing_gcs=true` chỉ dùng được với prefix chứa Parquet |
| `firebase_flat_reprocess` | manual | flatten lại `fb.events_flat` từ `fb.events_raw` theo dải ngày — KHÔNG đụng BigQuery |
| `clickhouse_maintenance` | `0 3 * * 0` | `OPTIMIZE ... FINAL` partition tuần trước + báo cáo `system.parts`/`system.columns`, cảnh báo vượt ngưỡng |

Quality checks của DAG daily: row count ClickHouse == BigQuery — bảng **final** bắt
buộc khớp tuyệt đối, bảng **intraday** (đang nhận streaming, luôn chênh vài dòng giữa
lúc `count(*)` và `EXPORT DATA`) chấp nhận lệch ≤ 0.5% kèm cảnh báo; tỉ lệ
`user_pseudo_id` NULL (cảnh báo > 5%); tỉ lệ `event_params` rỗng (cảnh báo > 20% —
dấu hiệu structure khai báo sai chứ không phải dữ liệu xấu); `uniqExact(event_name) > 0`;
đọc thử một dòng và assert `length(event_params) > 0`.

## Bảng `fb.events_flat` — phẳng, tự mở rộng cột (Phase 2)

Tương đương bảng `chat01_android_*` flatten trên BigQuery cũ, nhưng **không cần sửa
tay khi có key mới**. Ba mảnh ghép (code: `src/fb_pipeline/clickhouse/flat.py`):

1. **Cột cố định** (139 cột): cột gốc + toàn bộ struct đã biết — `privacy_*`,
   `user_ltv_*`, `device_*` (kể cả `web_info`), `geo_*`, `app_info_*`,
   `traffic_source_*`, `cts_*` (collected_traffic_source), `stlc_*`
   (session_traffic_source_last_click: manual/gads/cross/sa360/cm360/dv360),
   `publisher_*`, cộng `event_date` (Date đã parse) và `event_ts` (DateTime64(6)
   từ `event_timestamp` micro giây). Khai báo tại `BASE_COLUMNS`, DDL
   `05_events_flat.sql` sinh từ đó.
2. **Registry `fb.flat_schema_registry`**: mỗi ngày, bước discover quét
   `event_params`/`user_properties` của partition mới; mỗi cặp (key, sub-field có
   giá trị) thành một cột theo quy ước **`<key>_<str|int|float|double>`**
   (vd `ga_session_id_int`, `firebase_screen_class_str`, `_ltv_USD_int`;
   `user_properties.user_id` → `up_user_id_str`; trùng tên giữa hai nguồn thì thêm
   prefix `ep_`/`up_`).
3. **Auto ALTER**: cột trong registry chưa có trên `events_flat` → tự
   `ALTER TABLE ADD COLUMN IF NOT EXISTS`. **Key mới xuất hiện trong dữ liệu là bảng
   tự có cột mới** — đã chứng kiến trên dữ liệu thật: ngày 27/8 có 5 key mới so với
   26/8 (`campaign`, `_ltv_EUR`, `_ltv_IDR`, `_ltv_VND`, `iap_unlock_image_show`)
   và 5 cột được thêm tự động trong run.

Flatten chạy trong DAG daily (sau quality checks), idempotent theo partition `_dt`
(DROP PARTITION + insert lại từ raw). Cột chỉ có trong danh sách kỳ vọng mà chưa có
trong bảng nghĩa là dữ liệu tới giờ chưa từng có giá trị cho key đó — nó sẽ tự xuất
hiện đúng ngày dữ liệu có.

Chạy tay / chạy lại:

```bash
make flatten DATE=2026-08-27
```

hoặc trigger DAG `firebase_flat_reprocess` với `date_from`/`date_to` (đọc từ raw,
không tốn BigQuery). Query thẳng, không cần arrayFirst:

```sql
SELECT event_date, event_name, ga_session_id_int, firebase_screen_class_str, geo_country
FROM fb.events_flat WHERE _dt = '2026-08-27' LIMIT 10;
```

## Query dữ liệu nested

Cách dùng `events_raw` trực tiếp trước khi có phase 2 (cú pháp đã verify trên
ClickHouse 24.8 — với tuple có tên, dùng được cả `.key` lẫn `tupleElement`):

```sql
-- 1. Lấy một param cụ thể (ga_session_id là int_value)
SELECT arrayFirst(p -> p.1 = 'ga_session_id', event_params).2.2 AS ga_session_id
FROM fb.events_raw WHERE _dt = '2026-08-27' LIMIT 10;

-- 2. Đếm event theo ngày
SELECT _dt, event_name, count() AS n
FROM fb.events_raw GROUP BY _dt, event_name ORDER BY _dt, n DESC;

-- 3. Lọc theo sự tồn tại của param
SELECT count()
FROM fb.events_raw
WHERE _dt = '2026-08-27'
  AND has(arrayMap(p -> p.1, event_params), 'click_timestamp');

-- 4. Lấy user property (double_value)
SELECT user_pseudo_id,
       arrayFirst(p -> p.1 = '_ltv_COP', user_properties).2.4 AS ltv
FROM fb.events_raw WHERE _dt = '2026-08-27' LIMIT 10;

-- 5. Liệt kê toàn bộ key của một event
SELECT arrayMap(p -> p.1, event_params) AS keys
FROM fb.events_raw
WHERE _dt = '2026-08-27' AND event_name = 'screen_view' LIMIT 5;
```

Khảo sát toàn bộ key + kiểu (chuẩn bị phase 2): `make explore-keys DATE=2026-08-27`.

## Makefile

`up` `down` `restart` `logs` `ps` `build` `init` `ch-cli` `ch-schema` `airflow-cli`
`test` `lint` `dag-test DATE=…` `clean` — và ba target debug:

- `sample-parquet DATE=…` — công cụ chính để chốt `source_schema.py` (DESCRIBE + diff)
- `peek DATE=…` — xem 1 dòng đầy đủ `FORMAT Vertical`
- `explore-keys DATE=…` — read-only, thống kê key/kiểu của `event_params`/`user_properties`
- `flatten DATE=…` — flatten lại 1 ngày từ `events_raw` vào `events_flat` (không đụng BigQuery)

## Troubleshooting

| Triệu chứng | Nguyên nhân / cách xử lý |
|---|---|
| `s3()` báo 403 | Dùng **HMAC key**, không phải key JSON (mục HMAC ở trên). Kiểm tra `.env` đã điền và đã restart clickhouse |
| Cột nested về rỗng / `event_params` rỗng hàng loạt | Structure khai báo sai so với file thật → `make sample-parquet DATE=…`, sửa `source_schema.py` + `02_events_raw.sql` |
| OOM khi đọc Parquet lớn | Giảm `MAX_INSERT_THREADS`, tăng `MAX_MEMORY_USAGE` / `CLICKHOUSE_MEM_LIMIT` trong `.env` |
| Row count lệch giữa CH và BQ | Bảng intraday bị thay bằng daily giữa chừng (hoặc intraday vẫn đang nhận dữ liệu streaming) → chạy lại DAG ngày đó, nó tự `DROP PARTITION` và nạp lại |
| `parseDateTimeOrNull` trả NULL / `event_date_d` NULL | `event_date` không đúng format `YYYYMMDD` — kiểm tra dữ liệu nguồn, format string `'%Y%m%d'` |
| DAG skip ngày mới nhất | GA4 chưa sinh bảng ngày đó (thường xuất hiện sau vài giờ sáng) — catchup sẽ tự chạy lại khi trigger lần sau, hoặc `make dag-test DATE=…` |

## Security

- **Không commit credential.** `.gitignore` chặn `secrets/`, `.env`, `*.json`
  (trừ `*.example.json`). Key chỉ mount read-only lúc runtime, không `COPY` vào image
  (`.dockerignore` cũng chặn `secrets/`).
- **Rotate key định kỳ** (IAM → Service Accounts → Keys): tạo key mới → thay
  `secrets/gcp-sa.json` → xoá key cũ. Làm ngay nếu nghi ngờ key bị lộ.
- HMAC key của GCS cũng rotate được độc lập (Interoperability → delete/create).
- IAM cấp tối thiểu theo mục Prerequisites — không cấp `roles/owner`/`editor`.

## Chạy nhiều app (mỗi app một database ClickHouse)

Toàn bộ code đọc cấu hình từ env, nên chạy app khác chỉ là **đổi env + một database
riêng** (không trộn app vào `fb` — partition theo `_dt` nên trộn chung sẽ khiến
DROP PARTITION xoá lẫn dữ liệu hai app). Ví dụ app `du02_android`:

```bash
docker compose exec -T \
  -e GCP_PROJECT_ID=dress-up-2 \
  -e BQ_DATASET=analytics_240445616 \
  -e GCS_BUCKET=du02-android-backup-from-bigquery-table-sufix \
  -e GCS_RAW_PREFIX=analytics_240445616/events_intraday \
  -e CLICKHOUSE_DB=du02 \
  airflow-scheduler bash -c '
    python -m fb_pipeline.tools.apply_schema &&
    python -c "
from fb_pipeline.config import load_settings
from fb_pipeline.backfill import process_day
process_day(load_settings(), \"2026-08-27\", \"manual-du02\", use_existing_gcs=False)" &&
    python -m fb_pipeline.tools.flatten_day --date 2026-08-27'
```

- `apply_schema` tự thay database `fb` trong DDL bằng `CLICKHOUSE_DB` — tạo
  `du02.events_raw`, `du02.events_flat`, registry riêng (key/cột của app nào theo
  app đó).
- Registry riêng nghĩa là bảng flat của mỗi app có đúng bộ cột của app đó.
- Lưu ý: KHÔNG chạy hai app song song với strategy `file` — staging local dùng chung
  đường dẫn `user_files/staging/dt=<ngày>`; chạy tuần tự thì an toàn (mỗi run tự dọn).
- Muốn app phụ có DAG schedule riêng thì nhân bản `firebase_raw_daily.py` với bộ env
  riêng (chưa làm — hiện các DAG đọc env của compose, tức app chính `fb`).

## Trạng thái Phase 2

Đã làm: `fb.events_flat` + schema registry + discover key hằng ngày + tự
`ALTER TABLE ADD COLUMN` + DAG reprocess từ raw (mục `fb.events_flat` ở trên).
Chưa làm (xem [ROADMAP.md](ROADMAP.md)): overflow map, đổi kiểu cột tự động
(MODIFY COLUMN), flatten `items`/`ecommerce`.
