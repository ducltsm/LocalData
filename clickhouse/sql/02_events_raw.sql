-- Bảng raw GA4, GIỮ NGUYÊN nested như nguồn (không flatten — fb.events_flat mới là bảng phẳng).
-- FILE NÀY ĐƯỢC SINH TỪ src/fb_pipeline/clickhouse/source_schema.py — sửa schema thì sửa
-- constant SOURCE_COLUMNS trước rồi generate lại; unit test tests/test_schema_sync.py
-- đối chiếu từng cột, đúng thứ tự, giữa hai file.
--
-- Schema đã chốt bằng DESCRIBE trên Parquet thật (make sample-parquet DATE=2026-08-27).
--
-- PARTITION BY _dt (logical date của DAG run) chứ KHÔNG theo event_date:
-- event_date là string theo reporting timezone của property, event_timestamp là UTC —
-- hai cái lệch nhau ở biên ngày. Partition theo _dt khiến DROP PARTITION luôn xoá
-- đúng và đủ những gì run đó đã ghi (cơ chế idempotency duy nhất của pipeline).
--
-- allow_nullable_key=1: user_pseudo_id giữ Nullable(String) trung thực với nguồn
-- nhưng vẫn nằm trong ORDER BY.
--
-- TTL không khai báo ở đây: apply_schema thêm `ALTER ... MODIFY TTL` khi RAW_TTL_DAYS > 0.
CREATE TABLE IF NOT EXISTS fb.events_raw
(
    event_date String CODEC(ZSTD(1)),
    event_timestamp Int64,
    event_name String CODEC(ZSTD(1)),
    event_params Array(Tuple(key String, value Tuple(string_value Nullable(String), int_value Nullable(Int64), float_value Nullable(Float64), double_value Nullable(Float64)))) CODEC(ZSTD(1)),
    event_previous_timestamp Nullable(Int64),
    event_value_in_usd Nullable(Float64),
    event_bundle_sequence_id Nullable(Int64),
    event_server_timestamp_offset Nullable(Int64),
    user_id Nullable(String) CODEC(ZSTD(1)),
    user_pseudo_id Nullable(String) CODEC(ZSTD(1)),
    privacy_info Tuple(analytics_storage Nullable(String), ads_storage Nullable(String), uses_transient_token Nullable(String)) CODEC(ZSTD(1)),
    user_properties Array(Tuple(key String, value Tuple(string_value Nullable(String), int_value Nullable(Int64), float_value Nullable(Float64), double_value Nullable(Float64), set_timestamp_micros Nullable(Int64)))) CODEC(ZSTD(1)),
    user_first_touch_timestamp Nullable(Int64),
    user_ltv Tuple(revenue Nullable(Float64), currency Nullable(String)) CODEC(ZSTD(1)),
    device Tuple(category Nullable(String), mobile_brand_name Nullable(String), mobile_model_name Nullable(String), mobile_marketing_name Nullable(String), mobile_os_hardware_model Nullable(String), operating_system Nullable(String), operating_system_version Nullable(String), vendor_id Nullable(String), advertising_id Nullable(String), language Nullable(String), is_limited_ad_tracking Nullable(String), time_zone_offset_seconds Nullable(Int64), browser Nullable(String), browser_version Nullable(String), web_info Tuple(browser Nullable(String), browser_version Nullable(String), hostname Nullable(String))) CODEC(ZSTD(1)),
    geo Tuple(city Nullable(String), country Nullable(String), continent Nullable(String), region Nullable(String), sub_continent Nullable(String), metro Nullable(String)) CODEC(ZSTD(1)),
    app_info Tuple(id Nullable(String), version Nullable(String), install_store Nullable(String), firebase_app_id Nullable(String), install_source Nullable(String)) CODEC(ZSTD(1)),
    traffic_source Tuple(name Nullable(String), medium Nullable(String), source Nullable(String)) CODEC(ZSTD(1)),
    stream_id Nullable(String) CODEC(ZSTD(1)),
    platform String CODEC(ZSTD(1)),
    event_dimensions Tuple(hostname Nullable(String)) CODEC(ZSTD(1)),
    ecommerce Tuple(total_item_quantity Nullable(Int64), purchase_revenue_in_usd Nullable(Float64), purchase_revenue Nullable(Float64), refund_value_in_usd Nullable(Float64), refund_value Nullable(Float64), shipping_value_in_usd Nullable(Float64), shipping_value Nullable(Float64), tax_value_in_usd Nullable(Float64), tax_value Nullable(Float64), unique_items Nullable(Int64), transaction_id Nullable(String)) CODEC(ZSTD(1)),
    items Array(Tuple(item_id Nullable(String), item_name Nullable(String), item_brand Nullable(String), item_variant Nullable(String), item_category Nullable(String), item_category2 Nullable(String), item_category3 Nullable(String), item_category4 Nullable(String), item_category5 Nullable(String), price_in_usd Nullable(Float64), price Nullable(Float64), quantity Nullable(Int64), item_revenue_in_usd Nullable(Float64), item_revenue Nullable(Float64), item_refund_in_usd Nullable(Float64), item_refund Nullable(Float64), coupon Nullable(String), affiliation Nullable(String), location_id Nullable(String), item_list_id Nullable(String), item_list_name Nullable(String), item_list_index Nullable(String), promotion_id Nullable(String), promotion_name Nullable(String), creative_name Nullable(String), creative_slot Nullable(String), item_params Array(Tuple(key String, value Tuple(string_value Nullable(String), int_value Nullable(Int64), float_value Nullable(Float64), double_value Nullable(Float64)))))) CODEC(ZSTD(1)),
    collected_traffic_source Tuple(manual_campaign_id Nullable(String), manual_campaign_name Nullable(String), manual_source Nullable(String), manual_medium Nullable(String), manual_term Nullable(String), manual_content Nullable(String), manual_source_platform Nullable(String), manual_creative_format Nullable(String), manual_marketing_tactic Nullable(String), gclid Nullable(String), dclid Nullable(String), srsltid Nullable(String)) CODEC(ZSTD(1)),
    is_active_user Nullable(Bool),
    batch_event_index Nullable(Int64),
    batch_page_id Nullable(Int64),
    batch_ordering_id Nullable(Int64),
    session_traffic_source_last_click Tuple(manual_campaign Tuple(campaign_id Nullable(String), campaign_name Nullable(String), source Nullable(String), medium Nullable(String), term Nullable(String), content Nullable(String), source_platform Nullable(String), creative_format Nullable(String), marketing_tactic Nullable(String)), google_ads_campaign Tuple(customer_id Nullable(String), account_name Nullable(String), campaign_id Nullable(String), campaign_name Nullable(String), ad_group_id Nullable(String), ad_group_name Nullable(String)), cross_channel_campaign Tuple(campaign_id Nullable(String), campaign_name Nullable(String), source Nullable(String), medium Nullable(String), source_platform Nullable(String), default_channel_group Nullable(String), primary_channel_group Nullable(String)), sa360_campaign Tuple(campaign_id Nullable(String), campaign_name Nullable(String), source Nullable(String), medium Nullable(String), ad_group_id Nullable(String), ad_group_name Nullable(String), creative_format Nullable(String), engine_account_name Nullable(String), engine_account_type Nullable(String), manager_account_name Nullable(String)), cm360_campaign Tuple(campaign_id Nullable(String), campaign_name Nullable(String), source Nullable(String), medium Nullable(String), account_id Nullable(String), account_name Nullable(String), advertiser_id Nullable(String), advertiser_name Nullable(String), creative_id Nullable(String), creative_format Nullable(String), creative_name Nullable(String), creative_type Nullable(String), creative_type_id Nullable(String), creative_version Nullable(String), placement_id Nullable(String), placement_cost_structure Nullable(String), placement_name Nullable(String), rendering_id Nullable(String), site_id Nullable(String), site_name Nullable(String)), dv360_campaign Tuple(campaign_id Nullable(String), campaign_name Nullable(String), source Nullable(String), medium Nullable(String), advertiser_id Nullable(String), advertiser_name Nullable(String), creative_id Nullable(String), creative_format Nullable(String), creative_name Nullable(String), exchange_id Nullable(String), exchange_name Nullable(String), insertion_order_id Nullable(String), insertion_order_name Nullable(String), line_item_id Nullable(String), line_item_name Nullable(String), partner_id Nullable(String), partner_name Nullable(String))) CODEC(ZSTD(1)),
    publisher Tuple(ad_revenue_in_usd Nullable(Float64), ad_format Nullable(String), ad_source_name Nullable(String), ad_unit_id Nullable(String)) CODEC(ZSTD(1)),
    event_original_occurrence_timestamp Nullable(Int64),

    -- Metadata của lần ingest (không có trong nguồn)
    _dt Date,
    _ingested_at DateTime DEFAULT now(),
    _run_id String CODEC(ZSTD(1)),
    _source_table String CODEC(ZSTD(1)),
    _is_intraday UInt8,

    -- Tiện query: event_date 'YYYYMMDD' -> Date (NULL nếu chuỗi hỏng).
    -- Đã verify trên container 24.8: parseDateTimeOrNull('%Y%m%d') parse đúng.
    -- Dùng format tường minh thay vì BestEffort để không phụ thuộc heuristic.
    event_date_d Nullable(Date) MATERIALIZED toDate(parseDateTimeOrNull(event_date, '%Y%m%d'))
)
ENGINE = MergeTree
PARTITION BY _dt
ORDER BY (_dt, event_name, user_pseudo_id, event_timestamp)
SETTINGS index_granularity = 8192, allow_nullable_key = 1;
