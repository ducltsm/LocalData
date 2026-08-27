# Prompt: dựng pipeline GA4 (BigQuery) → ClickHouse trong dự án Airflow CÓ SẴN

> Cách dùng: điền bảng "Thông tin cần điền" bên dưới, rồi copy TOÀN BỘ nội dung từ
> mục 0 trở xuống, paste vào Claude Code mở tại repo Airflow đích.
> Prompt này chưng cất từ pipeline đã chạy thật (repo `ducltsm/LocalData`):
> 2 app, app lớn 52M dòng/ngày, mọi cạm bẫy bên dưới đều đã dính thật và đã có cách xử.

## Thông tin cần điền trước khi paste

| Placeholder | Ý nghĩa | Ví dụ |
|---|---|---|
| `<GCP_PROJECT>` | Project chứa BQ export của GA4 | `dress-up-2` |
| `<BQ_DATASET>` | Dataset analytics | `analytics_240445616` |
| `<GCS_BUCKET>` | Bucket staging | `du02-android-backup-...` |
| `<CH_DB>` | Database ClickHouse riêng cho app này | `du02` |
| `<CH_HOST>` | Host ClickHouse đích | `clickhouse` / IP server |
| `<NGÀY_TEST>` | Một ngày có bảng `events[_intraday]_YYYYMMDD` | `2026-08-27` |

---

## 0. Bối cảnh và phạm vi

Dự án Airflow này đã có sẵn hạ tầng (executor, connections, cách quản lý secrets,
cách deploy DAG). **Đừng dựng lại hạ tầng** — việc đầu tiên là khảo sát repo:
DAG để đâu, code Python đóng gói kiểu gì (package cài đặt hay PYTHONPATH), secrets
lấy qua đâu (Connection/env/Secret Manager), đã có ClickHouse client library chưa.
Viết `PLAN.md` mô tả cách TÍCH HỢP theo convention sẵn có + thứ tự file, hỏi tôi
confirm rồi mới code.

Xây pipeline nạp GA4 export từ BigQuery vào ClickHouse gồm HAI tầng:

```
BigQuery events_[intraday_]YYYYMMDD
        │  EXTRACT JOB (bq extract — nguyên bảng, MIỄN PHÍ; PHẢI là Parquet.
        │  KHÔNG dùng EXPORT DATA: bị tính tiền như query theo bytes quét)
        ▼
GCS gs://<GCS_BUCKET>/staging_raw/dt=YYYY-MM-DD/part-*.parquet
        │  ClickHouse tự đọc: s3() (ưu tiên) hoặc file() (fallback)
        ▼
<CH_DB>.events_raw    — giữ NGUYÊN nested (Array(Tuple)/Tuple), trung thực với nguồn
        │  flatten: CAST → Map, registry tự thêm cột
        ▼
<CH_DB>.events_flat   — wide table, mỗi param/user property một cột
```

Nguyên tắc bất biến: BigQuery chỉ dump thô; Airflow chỉ orchestrate; Python không
parse dữ liệu — ClickHouse tự đọc Parquet. Mỗi app một database ClickHouse riêng
(partition chỉ theo `_dt`, trộn app chung bảng thì DROP PARTITION xoá lẫn nhau).

**Định nghĩa xong:** trigger DAG cho `<NGÀY_TEST>` → dữ liệu ngày đó nằm trong
`<CH_DB>.events_raw` khớp số dòng BigQuery, `<CH_DB>.events_flat` có đủ số dòng
bằng raw và cột động sinh tự động; **job dump trên BigQuery Job history là loại
`extract` (bytes_billed = 0), không phải `query`**; unit + integration test pass;
không secret nào trong git.

## 1. Tầng raw — `<CH_DB>.events_raw`

- Schema nguồn khai báo TƯỜNG MINH trong một constant Python duy nhất
  (`source_schema.py`: list `(tên_cột, kiểu)`), dùng chung cho structure của
  `s3()`/`file()`, danh sách cột INSERT, và đối chiếu DDL bằng unit test.
  **DDL sinh từ constant** (script generate) — đừng viết tay hai nơi.
- Structure khởi điểm: chạy `EXPORT DATA` một ngày rồi `DESCRIBE TABLE
  file(...)/s3(...)` trên file thật, khai đúng theo đó (~31 cột top-level với GA4
  Android hiện tại: có cả `collected_traffic_source`, `session_traffic_source_last_click`
  6 nhóm, `publisher`, `user_ltv`, `ecommerce`, `items`, `is_active_user`, `batch_*`).
  Field của GA4 khác nhau theo property — KHÔNG copy schema từ dự án khác mà không DESCRIBE lại.
- Bảng: `MergeTree PARTITION BY _dt ORDER BY (_dt, event_name, user_pseudo_id,
  event_timestamp)` + cột metadata `_dt Date, _ingested_at, _run_id, _source_table,
  _is_intraday`. `event_timestamp` giữ Int64 micro giây. Codec `ZSTD(1)` cho cột
  String/Array/Tuple.
- Idempotency = `ALTER TABLE ... DROP PARTITION '<ds>'` trước mỗi insert. Chạy lại
  một ngày cho kết quả y hệt.
- Settings kèm MỌI lệnh đọc Parquet: `input_format_parquet_allow_missing_columns=1`,
  `input_format_null_as_default=1`, `input_format_parquet_case_insensitive_column_matching=1`.

## 2. Tầng flat — `<CH_DB>.events_flat` + registry tự mở rộng

- Cột cố định: cột gốc + flatten các struct đã biết (`privacy_*`, `user_ltv_*`,
  `device_*` kể cả `web_info`, `geo_*`, `app_info_*`, `traffic_source_*`,
  `cts_*`, `stlc_manual/gads/cross/sa360/cm360/dv360_*`, `publisher_*`) + `event_date Date`
  (parse từ string `YYYYMMDD`, fallback `_dt`) + `event_ts DateTime64(6)`
  (`fromUnixTimestamp64Micro`). Khai báo dạng list `(tên, kiểu, biểu_thức_SELECT)`,
  DDL sinh từ đó.
- Cột động: bảng registry `flat_schema_registry` (`ReplacingMergeTree(updated_at)`,
  ORDER BY `(source, key, sub_field)`, đọc luôn kèm `FINAL`). Mỗi ngày, bước
  discover `ARRAY JOIN` partition mới đếm non-null theo 4 sub-field; mỗi cặp
  (key, sub-field có giá trị) thành một cột `<key>_<str|int|float|double>`;
  `user_properties.user_id` → `up_user_id_*`; trùng tên giữa hai nguồn → prefix
  `ep_`/`up_`; sanitize key về identifier nhưng luôn backtick tên cột trong SQL.
- Cột trong registry chưa có trên bảng → `ALTER TABLE ADD COLUMN IF NOT EXISTS`.
  **Key mới xuất hiện trong dữ liệu = bảng tự có cột mới, không sửa tay.**
- Flatten đọc từ raw (KHÔNG đụng BigQuery):
  `WITH CAST(event_params, 'Map(String, Tuple(...))') AS _ep, CAST(user_properties, ...) AS _up`
  rồi `tupleElement(_ep['key'], 'sub_field')` cho từng cột — một lần build map mỗi
  dòng, nhanh hơn arrayFirst từng cột.
- DAG daily gắn task flatten sau quality checks; thêm DAG reprocess manual
  (date_from/date_to) flatten lại từ raw.

## 3. DAG daily (những điểm dễ sai)

1. `resolve_source`: có `events_<ds_nodash>` dùng final (`is_intraday=0`), không thì
   fallback `events_intraday_<ds_nodash>`, cả hai không có → `AirflowSkipException`.
2. Dump qua **extract job** — `BigQueryToGCSOperator` (export_format PARQUET,
   compression SNAPPY), source table pull từ XCom của resolve_source. Extract job
   KHÔNG có overwrite → thêm task dọn staging prefix NGAY TRƯỚC nó. (EXPORT DATA
   chỉ dùng khi cần export có lọc — trả phí quét theo bytes.)
3. Verify GCS có file, push số file + bytes.
4. DROP PARTITION → INSERT (một câu, cột tường minh, KHÔNG `SELECT *`).
5. Quality checks: row count so BigQuery — bảng **final bắt buộc lệch 0**, bảng
   **intraday cho phép lệch ≤0.5% kèm warning** (nó đang nhận streaming giữa lúc
   `count(*)` và EXPORT — không thể khớp tuyệt đối, đã đo thật lệch 3–60k dòng);
   cảnh báo tỉ lệ `user_pseudo_id` NULL >5% và `event_params` rỗng >20% (dấu hiệu
   khai sai structure); đọc thử 1 dòng assert `length(event_params) > 0`.
6. Flatten → cleanup staging (chỉ khi mọi bước OK) → ghi bảng `ingestion_log`
   với `trigger_rule=ALL_DONE` (ghi cả khi fail/skip).
7. Trigger rules khi có nhánh skip (stage_files chỉ chạy với strategy file):
   dùng `none_failed_min_one_success` cho drop_partition/cleanup để ngày bị skip
   không kích hoạt nhầm downstream.

## 4. ⚠️ Bài học ĐÃ TRẢ GIÁ — coi là ràng buộc, đừng khám phá lại

**Chi phí BigQuery (đã đo thật):** `EXPORT DATA` là query — bị tính tiền theo
bytes quét ($6.25/TB on-demand): app 52M dòng/ngày = ~57GB logical ≈ $0.36/ngày,
backfill 1 năm ≈ $130 CHỈ RIÊNG tiền quét. **Extract job** (`bq extract` /
`client.extract_table` / `BigQueryToGCSOperator`) xuất nguyên bảng ra Parquet/SNAPPY
y hệt nhưng **$0** (không dùng slot, quota 50TB/ngày). Ba lưu ý: (1) extract không
có `overwrite` → PHẢI có task dọn staging prefix ngay trước nó; (2) chỉ xuất nguyên
bảng, không WHERE/transform — khớp nguyên tắc dump thô; (3) `SELECT count(*)` không
WHERE trả lời từ metadata, 0 bytes billed — bước đối chiếu vốn miễn phí, giữ nguyên.

**Timezone/`ds` (bug off-by-one thật):** macro `ds` của Airflow render theo UTC.
Cron `0 4 * * *` timezone +07 → `ds` lùi 1 ngày so với chủ đích (04:00+07 = 21:00 UTC
hôm trước). KHÔNG dùng `{{ ds }}`; mọi task tính ngày dữ liệu bằng
`logical_date.in_timezone('<tz property>').strftime('%Y-%m-%d')`, template thì
`{{ logical_date.in_timezone('...').strftime('%Y-%m-%d') }}`.

**Alias shadowing (bug nạp cả bảng vào 1 partition thật):** trong
`INSERT ... SELECT`, alias trùng tên cột bảng nguồn (vd `toDate('...') AS _dt`)
SHADOW cột thật trong `WHERE _dt = ...` → filter thành luôn-đúng. INSERT SELECT map
cột theo VỊ TRÍ → **không đặt alias nào trong SELECT**.

**OOM (dính thật 3 lần, hai dạng khác nhau):**
- Đọc Parquet ngày lớn (800 file/7.5GB): giới hạn
  `input_format_parquet_max_block_size=8192`, `max_threads=max_insert_threads` (2–4
  khi RAM hẹp), `min_insert_block_size_bytes=256MB`.
- INSERT vào bảng flat rất rộng (~500 cột): memory TĂNG DẦN theo tiến độ (buffer ghi
  cột × số part) — partition >10M dòng phải **chia thành nhiều câu INSERT theo
  `cityHash64(coalesce(user_pseudo_id,''), event_timestamp) % N = i`** (N = ceil(rows/10M)),
  block ghi to (`min_insert_block_size_rows=262144`), `max_insert_delayed_streams_for_parallel_write=0`.

**GCS download (404 giữa chừng thật):** blob từ `list_blobs` bị PIN theo generation —
object bị EXPORT overwrite ghi đè là 404 dù tên còn. Snapshot danh sách TÊN trước,
tải qua `bucket.blob(name)` không pin generation. Và nhớ: kill `docker compose exec`
phía host KHÔNG kill process trong container — kiểm tra process cũ trước khi chạy lại.

**ClickHouse 24.8 — đã verify, dùng thẳng:**
- `s3()` với GCS cần **HMAC key** (Interoperability), key JSON KHÔNG dùng được.
  Credential đặt trong named collection (config.d, `from_env`) để không lộ ra SQL/log.
  User không phải `default` cần `<named_collection_control>1</named_collection_control>`
  trong users.d (KHÔNG dùng `<grants>` chung với access_management — server từ chối boot);
  file users.d phải mount TÊN KHÁC vì entrypoint image tự ghi `users.d/default-user.xml`.
- Tuple field từ Parquet match theo TÊN, không theo vị trí; struct thiếu field → NULL.
- `ORDER BY` chứa cột Nullable cần `SETTINGS allow_nullable_key=1`.
- `DROP PARTITION` partition không tồn tại là no-op (an toàn lần chạy đầu).
- `parseDateTimeOrNull(event_date, '%Y%m%d')` parse đúng `'20260827'`.
- Truy cập tuple: positional `p.1`/`.2.2` và `tupleElement(p,'key')` đều chạy;
  chained named (`.value.string_value`) sau function call KHÔNG chạy → code dùng tupleElement.
- File DDL: strip comment `--` trước khi split theo `;` (dấu `;` trong comment cắt statement).

**Khác:** file example của service account key phải là placeholder thuần văn bản
(không có `BEGIN PRIVATE KEY`) — GitHub Push Protection chặn cả key fake.
`.gitignore` chặn `secrets/`, `.env`, `*.json` (trừ `*.example.json`) và dùng
`secrets/*` chứ không phải `secrets/` để negation `!secrets/.gitkeep` hoạt động.

## 5. Tests (bắt buộc, không skip)

- Unit: constant schema khớp DDL (tên + kiểu + thứ tự, so sau khi normalize
  whitespace); render template hai strategy; INSERT không chứa `SELECT *` và không
  alias; quy ước đặt tên cột flat (suffix, up_user_id, collision); logic resolve
  final/intraday/skip; config thiếu biến raise rõ ràng.
- Integration trên ClickHouse THẬT: sinh Parquet GA4 nested bằng pyarrow (đủ 4
  sub-field, mảng rỗng, sub-field NULL) → INSERT qua `file()` → assert số dòng,
  đọc lại giá trị nested cụ thể, idempotency (DROP + insert lại y hệt) → flatten →
  assert cột động đúng tên đúng giá trị → thêm Parquet chứa KEY MỚI → flatten lại →
  assert cột tự xuất hiện. Teardown dọn partition test + cột/registry của key test.

## 6. Cách làm việc

- Verify bằng container thật, không đoán — mọi cú pháp SQL phải từng chạy.
- `PLAN.md` trước, confirm rồi code; xong chạy đủ compile + lint + toàn bộ test.
- Python: type hint, docstring, logging (không print), không except trần.
- Không tạo/commit credential thật. Cuối cùng in checklist việc tay: IAM
  (`bigquery.jobUser` trên project, `dataViewer` trên dataset, `storage.objectAdmin`
  trên bucket — chú ý SA có thể thuộc project KHÁC với dữ liệu), HMAC key, và lệnh
  DESCRIBE file thật để chốt schema trước lần chạy đầu.
