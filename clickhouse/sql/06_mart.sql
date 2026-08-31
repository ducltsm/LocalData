-- Các bảng data mart tổng hợp theo ngày từ fb.events_flat. FILE NÀY ĐƯỢC SINH
-- TỪ MART_TABLES trong src/fb_pipeline/clickhouse/mart.py — sửa spec rồi chạy:
--   python -m fb_pipeline.tools.mart_day --print-ddl > clickhouse/sql/06_mart.sql
-- (unit test tests/test_mart.py đối chiếu hai file).
--
-- PARTITION BY _dt như events_flat: rebuild một ngày = DROP PARTITION + INSERT
-- lại từ events_flat — idempotent, không đụng events_raw/BigQuery.

-- KPI tổng theo ngày — đúng 1 dòng mỗi _dt
CREATE TABLE IF NOT EXISTS fb.mart_daily_kpi
(
    `dau` UInt64,
    `new_users` UInt64,
    `sessions` UInt64,
    `engaged_sessions` UInt64,
    `engagement_sec` Float64,
    `events` UInt64,
    `prompts_sent` UInt64,
    `prompt_users` UInt64,
    `prompt_results` UInt64,
    `chat_starts` UInt64,
    `paywall_views` UInt64,
    `paywall_users` UInt64,
    `purchases` UInt64,
    `buyers` UInt64,
    `revenue_usd` Float64,
    `ad_revenue_usd` Float64,
    `app_removes` UInt64,

    -- Metadata
    `_dt` Date,
    `_built_at` DateTime DEFAULT now(),
    `_run_id` String CODEC(ZSTD(1))
)
ENGINE = MergeTree
PARTITION BY _dt
ORDER BY (_dt);

-- Đếm theo từng event — sum(events) phải bằng số dòng events_flat của ngày (QC)
CREATE TABLE IF NOT EXISTS fb.mart_daily_events
(
    `event_name` String CODEC(ZSTD(1)),
    `events` UInt64,
    `users` UInt64,
    `sessions` UInt64,

    -- Metadata
    `_dt` Date,
    `_built_at` DateTime DEFAULT now(),
    `_run_id` String CODEC(ZSTD(1))
)
ENGINE = MergeTree
PARTITION BY _dt
ORDER BY (_dt, event_name);

-- Cắt theo thị trường: country x platform
CREATE TABLE IF NOT EXISTS fb.mart_daily_geo
(
    `country` String CODEC(ZSTD(1)),
    `platform` String CODEC(ZSTD(1)),
    `users` UInt64,
    `new_users` UInt64,
    `sessions` UInt64,
    `events` UInt64,
    `purchases` UInt64,
    `buyers` UInt64,
    `revenue_usd` Float64,

    -- Metadata
    `_dt` Date,
    `_built_at` DateTime DEFAULT now(),
    `_run_id` String CODEC(ZSTD(1))
)
ENGINE = MergeTree
PARTITION BY _dt
ORDER BY (_dt, country, platform);

-- Một dòng / user / ngày hoạt động — nền cho retention (view mart_retention) và LTV
CREATE TABLE IF NOT EXISTS fb.mart_user_daily
(
    `user_pseudo_id` String CODEC(ZSTD(1)),
    `is_new` UInt8,
    `events` UInt64,
    `sessions` UInt64,
    `engagement_sec` Float64,
    `prompts_sent` UInt64,
    `purchases` UInt64,
    `revenue_usd` Float64,
    `first_event_ts` DateTime64(6),
    `last_event_ts` DateTime64(6),
    `platform` String CODEC(ZSTD(1)),
    `country` String CODEC(ZSTD(1)),
    `app_version` String CODEC(ZSTD(1)),

    -- Metadata
    `_dt` Date,
    `_built_at` DateTime DEFAULT now(),
    `_run_id` String CODEC(ZSTD(1))
)
ENGINE = MergeTree
PARTITION BY _dt
ORDER BY (_dt, user_pseudo_id);

CREATE VIEW IF NOT EXISTS fb.mart_retention AS
WITH cohort AS (
    SELECT _dt AS cohort_dt, user_pseudo_id
    FROM fb.mart_user_daily
    WHERE is_new = 1
)
SELECT
    cohort.cohort_dt AS cohort_dt,
    dateDiff('day', cohort.cohort_dt, act._dt) AS day_n,
    uniqExact(act.user_pseudo_id) AS retained_users
FROM cohort
INNER JOIN fb.mart_user_daily AS act ON act.user_pseudo_id = cohort.user_pseudo_id
WHERE act._dt >= cohort.cohort_dt
GROUP BY cohort_dt, day_n;
