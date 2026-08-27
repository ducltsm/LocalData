-- Registry: theo dõi (source, key, sub_field) -> (tên cột, kiểu) của fb.events_flat.
-- Bước discover ghi vào đây mỗi ngày; ensure_flat_columns đọc từ đây để tự
-- ALTER TABLE ADD COLUMN. ReplacingMergeTree(updated_at) giữ bản mới nhất
-- theo khoá (source, key, sub_field) — luôn đọc với FINAL.
CREATE TABLE IF NOT EXISTS fb.flat_schema_registry
(
    source String,
    key String,
    sub_field String,
    column_name String,
    column_type String,
    first_seen Date,
    last_seen Date,
    n_seen UInt64,
    updated_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (source, key, sub_field);
