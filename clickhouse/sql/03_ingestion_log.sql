-- Log mỗi lần ingest (ghi cả khi fail — task write_ingestion_log có trigger_rule=ALL_DONE)
CREATE TABLE IF NOT EXISTS fb.ingestion_log
(
    event_date Date,
    source_table String,
    is_intraday UInt8,
    strategy String,
    bq_row_count UInt64,
    files_read UInt32,
    bytes_read UInt64,
    rows_inserted UInt64,
    run_id String,
    started_at DateTime,
    finished_at DateTime,
    duration_sec UInt32,
    status String,
    error_message String
)
ENGINE = MergeTree
ORDER BY (event_date, started_at);
