# ROADMAP

## Phase 2 — flatten + schema registry: ĐÃ IMPLEMENT (2026-08-28)

Code: `src/fb_pipeline/clickhouse/flat.py`, DDL `04_flat_schema_registry.sql` +
`05_events_flat.sql` (sinh từ `BASE_COLUMNS`), task `flatten` trong DAG daily,
DAG `firebase_flat_reprocess`, tool `make flatten DATE=…`.

Đã có:

- [x] `fb.events_flat` wide table: 139 cột cố định (privacy/user_ltv/device+web_info/
      geo/app_info/traffic_source/cts/stlc đủ 6 nhóm/publisher, `event_date` Date,
      `event_ts` DateTime64(6)) + cột động cho từng (key, sub-field) của
      `event_params`/`user_properties`.
- [x] Quy ước tên `<key>_<str|int|float|double>` (kế thừa bảng BQ cũ;
      `user_properties.user_id` → `up_user_id_*`, trùng tên → prefix `ep_`/`up_`).
- [x] Registry `fb.flat_schema_registry` (ReplacingMergeTree, đọc FINAL):
      key → cột → kiểu → first/last_seen → n_seen.
- [x] Discover key mỗi ngày trên partition vừa nạp + tự sinh
      `ALTER TABLE ADD COLUMN IF NOT EXISTS` — key mới là có cột mới, không sửa tay.
- [x] DAG reprocess flatten lại từ `events_raw`, không đụng BigQuery.
- [x] Idempotent theo partition `_dt` (DROP PARTITION + insert lại), QC số dòng
      flat == raw từng partition.

Ghi chú kỹ thuật đã verify trên 24.8 (đắt giá, đừng làm lại từ đầu):

- Flatten dùng `WITH CAST(event_params, 'Map(String, Tuple(...))') AS _ep` rồi
  `tupleElement(_ep['key'], 'sub')` — nhanh hơn arrayFirst từng cột.
- SELECT trong INSERT **không đặt alias**: alias trùng tên cột nguồn (vd `AS _dt`)
  shadow cột thật trong WHERE → từng gây nạp cả bảng vào một partition.
- Bảng rất rộng phải ép block nhỏ khi INSERT SELECT (`min_insert_block_size_*`,
  `max_block_size`) — mặc định squash ~1M dòng sẽ OOM.

## Chưa làm (phase sau)

- [ ] Overflow map cho key "rác" (hiện MỌI key đều được promote thành cột;
      nếu số key phình quá lớn thì cần ngưỡng promote + cột `Map(String,String)`).
- [ ] Tự đổi kiểu cột (`MODIFY COLUMN`) khi một key đổi sub-field chủ đạo —
      hiện mỗi sub-field là một cột riêng nên chưa cần.
- [ ] Flatten `items` (mảng sản phẩm ecommerce) và `ecommerce` — đã có đủ trong
      `events_raw`, chưa đưa lên flat.
- [ ] `set_timestamp_micros` của user_properties (hiện chỉ nằm trong raw).
- [ ] Materialized view / bảng tổng hợp phân tích trên events_flat.
