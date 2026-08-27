"""Single source of truth cho structure Parquet GA4 mà BigQuery EXPORT DATA sinh ra.

Constant này dùng chung cho:
- structure của ``s3()`` / ``file()`` (read_source.sql.j2)
- danh sách cột tường minh trong ``insert_raw.sql.j2``
- đối chiếu với DDL ``fb.events_raw`` (unit test tests/test_schema_sync.py)

Schema này đã được chốt bằng ``make sample-parquet DATE=2026-08-27`` trên file
Parquet THẬT của property (DESCRIBE trên ClickHouse 24.8, ngày 2026-08-28) —
đủ 31 cột top-level gồm cả collected_traffic_source, session_traffic_source_last_click,
ecommerce, items, publisher... Field trong Tuple được ClickHouse match theo TÊN
(đã verify), nhưng vẫn giữ đúng thứ tự file cho dễ đối chiếu.

Giữ nguyên kiểu của nguồn: ``event_timestamp`` là Int64 micro giây (không convert
DateTime64), ``event_date`` là String ``YYYYMMDD`` theo reporting timezone.
Mọi biến đổi nằm ở tầng flatten (fb.events_flat) hoặc phase sau.
"""

from __future__ import annotations

# Tuple value của event_params / item_params (GA4: string/int/float/double)
_PARAM_VALUE = (
    "Tuple(string_value Nullable(String), int_value Nullable(Int64), "
    "float_value Nullable(Float64), double_value Nullable(Float64))"
)

# user_properties có thêm set_timestamp_micros
_USER_PROPERTY_VALUE = (
    "Tuple(string_value Nullable(String), int_value Nullable(Int64), "
    "float_value Nullable(Float64), double_value Nullable(Float64), "
    "set_timestamp_micros Nullable(Int64))"
)

_DEVICE = (
    "Tuple(category Nullable(String), mobile_brand_name Nullable(String), "
    "mobile_model_name Nullable(String), mobile_marketing_name Nullable(String), "
    "mobile_os_hardware_model Nullable(String), operating_system Nullable(String), "
    "operating_system_version Nullable(String), vendor_id Nullable(String), "
    "advertising_id Nullable(String), language Nullable(String), "
    "is_limited_ad_tracking Nullable(String), time_zone_offset_seconds Nullable(Int64), "
    "browser Nullable(String), browser_version Nullable(String), "
    "web_info Tuple(browser Nullable(String), browser_version Nullable(String), "
    "hostname Nullable(String)))"
)

_GEO = (
    "Tuple(city Nullable(String), country Nullable(String), continent Nullable(String), "
    "region Nullable(String), sub_continent Nullable(String), metro Nullable(String))"
)

_APP_INFO = (
    "Tuple(id Nullable(String), version Nullable(String), install_store Nullable(String), "
    "firebase_app_id Nullable(String), install_source Nullable(String))"
)

_ECOMMERCE = (
    "Tuple(total_item_quantity Nullable(Int64), purchase_revenue_in_usd Nullable(Float64), "
    "purchase_revenue Nullable(Float64), refund_value_in_usd Nullable(Float64), "
    "refund_value Nullable(Float64), shipping_value_in_usd Nullable(Float64), "
    "shipping_value Nullable(Float64), tax_value_in_usd Nullable(Float64), "
    "tax_value Nullable(Float64), unique_items Nullable(Int64), "
    "transaction_id Nullable(String))"
)

_ITEMS = (
    "Array(Tuple(item_id Nullable(String), item_name Nullable(String), "
    "item_brand Nullable(String), item_variant Nullable(String), "
    "item_category Nullable(String), item_category2 Nullable(String), "
    "item_category3 Nullable(String), item_category4 Nullable(String), "
    "item_category5 Nullable(String), price_in_usd Nullable(Float64), "
    "price Nullable(Float64), quantity Nullable(Int64), "
    "item_revenue_in_usd Nullable(Float64), item_revenue Nullable(Float64), "
    "item_refund_in_usd Nullable(Float64), item_refund Nullable(Float64), "
    "coupon Nullable(String), affiliation Nullable(String), location_id Nullable(String), "
    "item_list_id Nullable(String), item_list_name Nullable(String), "
    "item_list_index Nullable(String), promotion_id Nullable(String), "
    "promotion_name Nullable(String), creative_name Nullable(String), "
    f"creative_slot Nullable(String), item_params Array(Tuple(key String, value {_PARAM_VALUE}))))"
)

_COLLECTED_TRAFFIC_SOURCE = (
    "Tuple(manual_campaign_id Nullable(String), manual_campaign_name Nullable(String), "
    "manual_source Nullable(String), manual_medium Nullable(String), "
    "manual_term Nullable(String), manual_content Nullable(String), "
    "manual_source_platform Nullable(String), manual_creative_format Nullable(String), "
    "manual_marketing_tactic Nullable(String), gclid Nullable(String), "
    "dclid Nullable(String), srsltid Nullable(String))"
)

_STLC = (
    "Tuple("
    "manual_campaign Tuple(campaign_id Nullable(String), campaign_name Nullable(String), "
    "source Nullable(String), medium Nullable(String), term Nullable(String), "
    "content Nullable(String), source_platform Nullable(String), "
    "creative_format Nullable(String), marketing_tactic Nullable(String)), "
    "google_ads_campaign Tuple(customer_id Nullable(String), account_name Nullable(String), "
    "campaign_id Nullable(String), campaign_name Nullable(String), "
    "ad_group_id Nullable(String), ad_group_name Nullable(String)), "
    "cross_channel_campaign Tuple(campaign_id Nullable(String), "
    "campaign_name Nullable(String), source Nullable(String), medium Nullable(String), "
    "source_platform Nullable(String), default_channel_group Nullable(String), "
    "primary_channel_group Nullable(String)), "
    "sa360_campaign Tuple(campaign_id Nullable(String), campaign_name Nullable(String), "
    "source Nullable(String), medium Nullable(String), ad_group_id Nullable(String), "
    "ad_group_name Nullable(String), creative_format Nullable(String), "
    "engine_account_name Nullable(String), engine_account_type Nullable(String), "
    "manager_account_name Nullable(String)), "
    "cm360_campaign Tuple(campaign_id Nullable(String), campaign_name Nullable(String), "
    "source Nullable(String), medium Nullable(String), account_id Nullable(String), "
    "account_name Nullable(String), advertiser_id Nullable(String), "
    "advertiser_name Nullable(String), creative_id Nullable(String), "
    "creative_format Nullable(String), creative_name Nullable(String), "
    "creative_type Nullable(String), creative_type_id Nullable(String), "
    "creative_version Nullable(String), placement_id Nullable(String), "
    "placement_cost_structure Nullable(String), placement_name Nullable(String), "
    "rendering_id Nullable(String), site_id Nullable(String), site_name Nullable(String)), "
    "dv360_campaign Tuple(campaign_id Nullable(String), campaign_name Nullable(String), "
    "source Nullable(String), medium Nullable(String), advertiser_id Nullable(String), "
    "advertiser_name Nullable(String), creative_id Nullable(String), "
    "creative_format Nullable(String), creative_name Nullable(String), "
    "exchange_id Nullable(String), exchange_name Nullable(String), "
    "insertion_order_id Nullable(String), insertion_order_name Nullable(String), "
    "line_item_id Nullable(String), line_item_name Nullable(String), "
    "partner_id Nullable(String), partner_name Nullable(String)))"
)

_PUBLISHER = (
    "Tuple(ad_revenue_in_usd Nullable(Float64), ad_format Nullable(String), "
    "ad_source_name Nullable(String), ad_unit_id Nullable(String))"
)

# (tên cột, kiểu ClickHouse) — THỨ TỰ và TÊN phải khớp DDL fb.events_raw,
# thứ tự cột theo đúng file Parquet thật
SOURCE_COLUMNS: tuple[tuple[str, str], ...] = (
    ("event_date", "String"),
    ("event_timestamp", "Int64"),
    ("event_name", "String"),
    ("event_params", f"Array(Tuple(key String, value {_PARAM_VALUE}))"),
    ("event_previous_timestamp", "Nullable(Int64)"),
    ("event_value_in_usd", "Nullable(Float64)"),
    ("event_bundle_sequence_id", "Nullable(Int64)"),
    ("event_server_timestamp_offset", "Nullable(Int64)"),
    ("user_id", "Nullable(String)"),
    ("user_pseudo_id", "Nullable(String)"),
    (
        "privacy_info",
        "Tuple(analytics_storage Nullable(String), ads_storage Nullable(String), "
        "uses_transient_token Nullable(String))",
    ),
    ("user_properties", f"Array(Tuple(key String, value {_USER_PROPERTY_VALUE}))"),
    ("user_first_touch_timestamp", "Nullable(Int64)"),
    ("user_ltv", "Tuple(revenue Nullable(Float64), currency Nullable(String))"),
    ("device", _DEVICE),
    ("geo", _GEO),
    ("app_info", _APP_INFO),
    (
        "traffic_source",
        "Tuple(name Nullable(String), medium Nullable(String), source Nullable(String))",
    ),
    ("stream_id", "Nullable(String)"),
    ("platform", "String"),
    ("event_dimensions", "Tuple(hostname Nullable(String))"),
    ("ecommerce", _ECOMMERCE),
    ("items", _ITEMS),
    ("collected_traffic_source", _COLLECTED_TRAFFIC_SOURCE),
    ("is_active_user", "Nullable(Bool)"),
    ("batch_event_index", "Nullable(Int64)"),
    ("batch_page_id", "Nullable(Int64)"),
    ("batch_ordering_id", "Nullable(Int64)"),
    ("session_traffic_source_last_click", _STLC),
    ("publisher", _PUBLISHER),
    ("event_original_occurrence_timestamp", "Nullable(Int64)"),
)


def structure() -> str:
    """Chuỗi structure cho s3()/file(): ``name Type, name Type, ...`` (một dòng)."""
    return ", ".join(f"{name} {type_}" for name, type_ in SOURCE_COLUMNS)


def column_names() -> list[str]:
    """Danh sách tên cột nguồn, đúng thứ tự — dùng cho insert tường minh."""
    return [name for name, _ in SOURCE_COLUMNS]
