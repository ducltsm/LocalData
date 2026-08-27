"""fb.events_flat — bảng phẳng từ events_raw, tự mở rộng cột theo key mới.

Ba mảnh ghép:
1. ``BASE_COLUMNS``: cột cố định (cột gốc + các struct đã biết: device, geo, stlc...),
   tương đương phần đầu câu CREATE TABLE trên BigQuery cũ.
2. ``fb.flat_schema_registry``: registry theo dõi (source, key, sub_field) ->
   (tên cột, kiểu). Bước discover quét partition mới, key nào chưa có thì đặt tên
   theo quy ước ``<key>_<int|str|float|double>`` và ghi vào registry.
3. ``ensure_flat_columns``: đọc registry, tự sinh ``ALTER TABLE ADD COLUMN IF NOT
   EXISTS`` cho cột còn thiếu -> event_params/user_properties có key MỚI là bảng
   flat tự có cột mới, không sửa tay.

Flatten dùng ``CAST(array -> Map)`` + ``WITH`` (đã verify trên 24.8): mỗi dòng chỉ
build map một lần thay vì arrayFirst cho từng cột.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import date

from clickhouse_connect.driver.client import Client

from fb_pipeline.clickhouse.ingest import check_ds, insert_settings
from fb_pipeline.config import Settings

log = logging.getLogger(__name__)

FLAT_TABLE = "events_flat"
REGISTRY_TABLE = "flat_schema_registry"

# Thứ tự xử lý cố định: event_params đặt tên trước, user_properties đụng tên thì thêm prefix
SOURCES = ("event_params", "user_properties")

SUFFIX = {
    "string_value": "str",
    "int_value": "int",
    "float_value": "float",
    "double_value": "double",
}
COLUMN_TYPE = {
    "string_value": "Nullable(String)",
    "int_value": "Nullable(Int64)",
    "float_value": "Nullable(Float64)",
    "double_value": "Nullable(Float64)",
}

_EP_MAP = (
    "Map(String, Tuple(string_value Nullable(String), int_value Nullable(Int64), "
    "float_value Nullable(Float64), double_value Nullable(Float64)))"
)
_UP_MAP = (
    "Map(String, Tuple(string_value Nullable(String), int_value Nullable(Int64), "
    "float_value Nullable(Float64), double_value Nullable(Float64), "
    "set_timestamp_micros Nullable(Int64)))"
)
_MAP_ALIAS = {"event_params": "_ep", "user_properties": "_up"}

_S = "Nullable(String)"


def _t(col: str, field: str) -> str:
    return f"tupleElement({col}, '{field}')"


def _t2(col: str, sub: str, field: str) -> str:
    return f"tupleElement(tupleElement({col}, '{sub}'), '{field}')"


def _struct_columns(raw_col: str, prefix: str, fields: tuple[tuple[str, str], ...]):
    return tuple((f"{prefix}_{f}", t, _t(raw_col, f)) for f, t in fields)


_DEVICE_FIELDS = (
    ("category", _S), ("mobile_brand_name", _S), ("mobile_model_name", _S),
    ("mobile_marketing_name", _S), ("mobile_os_hardware_model", _S),
    ("operating_system", _S), ("operating_system_version", _S), ("vendor_id", _S),
    ("advertising_id", _S), ("language", _S), ("is_limited_ad_tracking", _S),
    ("time_zone_offset_seconds", "Nullable(Int64)"), ("browser", _S), ("browser_version", _S),
)
_GEO_FIELDS = (
    ("city", _S), ("continent", _S), ("country", _S),
    ("region", _S), ("sub_continent", _S), ("metro", _S),
)
_APP_INFO_FIELDS = (
    ("id", _S), ("version", _S), ("install_store", _S),
    ("firebase_app_id", _S), ("install_source", _S),
)
_TRAFFIC_SOURCE_FIELDS = (("name", _S), ("medium", _S), ("source", _S))
_CTS_FIELDS = (
    ("manual_campaign_id", _S), ("manual_campaign_name", _S), ("manual_source", _S),
    ("manual_medium", _S), ("manual_term", _S), ("manual_content", _S),
    ("manual_source_platform", _S), ("manual_creative_format", _S),
    ("manual_marketing_tactic", _S), ("gclid", _S), ("dclid", _S), ("srsltid", _S),
)

# session_traffic_source_last_click: (sub-struct trong raw, prefix cột flat, các field)
_STLC_GROUPS = (
    ("manual_campaign", "manual", (
        "campaign_id", "campaign_name", "source", "medium", "term", "content",
        "source_platform", "creative_format", "marketing_tactic")),
    ("google_ads_campaign", "gads", (
        "customer_id", "account_name", "campaign_id", "campaign_name",
        "ad_group_id", "ad_group_name")),
    ("cross_channel_campaign", "cross", (
        "campaign_id", "campaign_name", "source", "medium", "source_platform",
        "default_channel_group", "primary_channel_group")),
    ("sa360_campaign", "sa360", (
        "campaign_id", "campaign_name", "source", "medium", "ad_group_id",
        "ad_group_name", "creative_format", "engine_account_name",
        "engine_account_type", "manager_account_name")),
    ("cm360_campaign", "cm360", (
        "campaign_id", "campaign_name", "source", "medium", "account_id",
        "account_name", "advertiser_id", "advertiser_name", "creative_id",
        "creative_format", "creative_name", "creative_type", "creative_type_id",
        "creative_version", "placement_id", "placement_cost_structure",
        "placement_name", "rendering_id", "site_id", "site_name")),
    ("dv360_campaign", "dv360", (
        "campaign_id", "campaign_name", "source", "medium", "advertiser_id",
        "advertiser_name", "creative_id", "creative_format", "creative_name",
        "exchange_id", "exchange_name", "insertion_order_id", "insertion_order_name",
        "line_item_id", "line_item_name", "partner_id", "partner_name")),
)


def _stlc_columns():
    for group, prefix, fields in _STLC_GROUPS:
        for field in fields:
            yield (
                f"stlc_{prefix}_{field}",
                _S,
                _t2("session_traffic_source_last_click", group, field),
            )


# (tên cột flat, kiểu, biểu thức SELECT từ events_raw) — phần cột CỐ ĐỊNH
BASE_COLUMNS: tuple[tuple[str, str, str], ...] = (
    (
        "event_date",
        "Date",
        "coalesce(toDate(parseDateTimeOrNull(event_date, '%Y%m%d')), _dt)",
    ),
    ("event_timestamp", "Int64", "event_timestamp"),
    ("event_ts", "DateTime64(6)", "fromUnixTimestamp64Micro(event_timestamp)"),
    ("event_name", "String", "event_name"),
    ("event_previous_timestamp", "Nullable(Int64)", "event_previous_timestamp"),
    ("event_value_in_usd", "Nullable(Float64)", "event_value_in_usd"),
    ("event_bundle_sequence_id", "Nullable(Int64)", "event_bundle_sequence_id"),
    ("event_server_timestamp_offset", "Nullable(Int64)", "event_server_timestamp_offset"),
    ("user_id", _S, "user_id"),
    ("user_pseudo_id", _S, "user_pseudo_id"),
    ("user_first_touch_timestamp", "Nullable(Int64)", "user_first_touch_timestamp"),
    ("stream_id", _S, "stream_id"),
    ("platform", "String", "platform"),
    ("is_active_user", "Nullable(Bool)", "is_active_user"),
    ("batch_event_index", "Nullable(Int64)", "batch_event_index"),
    ("batch_page_id", "Nullable(Int64)", "batch_page_id"),
    ("batch_ordering_id", "Nullable(Int64)", "batch_ordering_id"),
    ("privacy_ads_storage", _S, _t("privacy_info", "ads_storage")),
    ("privacy_analytics_storage", _S, _t("privacy_info", "analytics_storage")),
    ("privacy_uses_transient_token", _S, _t("privacy_info", "uses_transient_token")),
    ("event_dimensions_hostname", _S, _t("event_dimensions", "hostname")),
    ("user_ltv_revenue", "Nullable(Float64)", _t("user_ltv", "revenue")),
    ("user_ltv_currency", _S, _t("user_ltv", "currency")),
    *_struct_columns("device", "device", _DEVICE_FIELDS),
    ("device_web_browser", _S, _t2("device", "web_info", "browser")),
    ("device_web_browser_version", _S, _t2("device", "web_info", "browser_version")),
    ("device_web_hostname", _S, _t2("device", "web_info", "hostname")),
    *_struct_columns("geo", "geo", _GEO_FIELDS),
    *_struct_columns("app_info", "app_info", _APP_INFO_FIELDS),
    *_struct_columns("traffic_source", "traffic_source", _TRAFFIC_SOURCE_FIELDS),
    *_struct_columns("collected_traffic_source", "cts", _CTS_FIELDS),
    *_stlc_columns(),
    ("publisher_ad_revenue_in_usd", "Nullable(Float64)", _t("publisher", "ad_revenue_in_usd")),
    ("publisher_ad_format", _S, _t("publisher", "ad_format")),
    ("publisher_ad_source_name", _S, _t("publisher", "ad_source_name")),
    ("publisher_ad_unit_id", _S, _t("publisher", "ad_unit_id")),
)

_METADATA_NAMES = ("_dt", "_ingested_at", "_run_id")


def base_column_names() -> list[str]:
    """Tên các cột cố định của events_flat (không gồm metadata)."""
    return [name for name, _, _ in BASE_COLUMNS]


# ---------------------------------------------------------------------------
# Đặt tên cột động
# ---------------------------------------------------------------------------
def sanitize_key(key: str) -> str:
    """Đưa key về identifier an toàn (tên cột vẫn luôn được backtick khi vào SQL)."""
    name = re.sub(r"[^0-9A-Za-z_]", "_", key)
    if not name:
        name = "_empty"
    if name[0].isdigit():
        name = "_" + name
    return name


def column_name_for(source: str, key: str, sub_field: str, taken: set[str]) -> str:
    """Quy ước tên: ``<key>_<str|int|float|double>``.

    Đặc thù kế thừa từ bảng BQ cũ: user_properties key ``user_id`` -> ``up_user_id_*``
    (tránh nhầm với cột gốc user_id). Đụng tên (giữa hai source hoặc với cột cố định)
    thì thêm prefix ``ep_``/``up_``; vẫn đụng thì đánh số.
    """
    stem = sanitize_key(key)
    if source == "user_properties" and key == "user_id":
        stem = "up_user_id"
    name = f"{stem}_{SUFFIX[sub_field]}"
    if name in taken:
        prefix = "up" if source == "user_properties" else "ep"
        name = f"{prefix}_{stem}_{SUFFIX[sub_field]}"
    counter = 2
    while name in taken:
        name = f"{name.rstrip('0123456789')}{counter}"
        counter += 1
    return name


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------
@dataclass
class RegistryRow:
    """Một dòng fb.flat_schema_registry (khớp 04_flat_schema_registry.sql)."""

    source: str
    key: str
    sub_field: str
    column_name: str
    column_type: str
    first_seen: date
    last_seen: date
    n_seen: int


def discover_keys(client: Client, database: str, ds: str) -> list[tuple[str, str, str, int]]:
    """Quét partition ds của events_raw: (source, key, sub_field, số lần non-null)."""
    check_ds(ds)
    found: list[tuple[str, str, str, int]] = []
    for source in SOURCES:
        counts = ", ".join(
            f"countIf(isNotNull(tupleElement(tupleElement(p, 'value'), '{sub}'))) AS n_{sub}"
            for sub in SUFFIX
        )
        rows = client.query(
            f"SELECT tupleElement(p, 'key') AS key, {counts} "
            f"FROM {database}.events_raw ARRAY JOIN {source} AS p "
            f"WHERE _dt = toDate('{ds}') GROUP BY key ORDER BY key"
        ).result_rows
        for row in rows:
            key = str(row[0])
            for i, sub in enumerate(SUFFIX):
                n = int(row[1 + i])
                if n > 0:
                    found.append((source, key, sub, n))
    return found


def load_registry(client: Client, database: str) -> list[RegistryRow]:
    """Đọc registry (FINAL để gộp các bản ReplacingMergeTree), thứ tự ổn định."""
    rows = client.query(
        f"SELECT source, key, sub_field, column_name, column_type, "
        f"first_seen, last_seen, n_seen "
        f"FROM {database}.{REGISTRY_TABLE} FINAL "
        f"ORDER BY source, key, sub_field"
    ).result_rows
    return [RegistryRow(*row) for row in rows]


def sync_registry(client: Client, database: str, ds: str) -> list[str]:
    """Discover key của partition ds và cập nhật registry.

    Trả về danh sách tên cột MỚI phát hiện (rỗng nếu không có key mới).
    """
    day = date.fromisoformat(check_ds(ds))
    discovered = discover_keys(client, database, ds)
    registry = {(r.source, r.key, r.sub_field): r for r in load_registry(client, database)}
    taken = set(base_column_names()) | set(_METADATA_NAMES)
    taken |= {r.column_name for r in registry.values()}

    to_insert: list[list] = []
    new_columns: list[str] = []
    # event_params trước user_properties (SOURCES) để tên không đổi giữa các lần chạy
    for source in SOURCES:
        for src, key, sub, n in discovered:
            if src != source:
                continue
            existing = registry.get((src, key, sub))
            if existing is not None:
                to_insert.append([
                    src, key, sub, existing.column_name, existing.column_type,
                    existing.first_seen, max(existing.last_seen, day),
                    existing.n_seen + n,
                ])
            else:
                name = column_name_for(src, key, sub, taken)
                taken.add(name)
                new_columns.append(name)
                to_insert.append([src, key, sub, name, COLUMN_TYPE[sub], day, day, n])

    if to_insert:
        client.insert(
            REGISTRY_TABLE,
            to_insert,
            column_names=[
                "source", "key", "sub_field", "column_name", "column_type",
                "first_seen", "last_seen", "n_seen",
            ],
            database=database,
        )
    if new_columns:
        log.info("Registry: %d key/sub-field mới: %s", len(new_columns), new_columns)
    return new_columns


def ensure_flat_columns(client: Client, database: str) -> list[str]:
    """ALTER TABLE ADD COLUMN cho mọi cột trong registry còn thiếu trên events_flat."""
    existing = {
        str(row[0])
        for row in client.query(
            f"SELECT name FROM system.columns "
            f"WHERE database = '{database}' AND table = '{FLAT_TABLE}'"
        ).result_rows
    }
    added: list[str] = []
    for row in load_registry(client, database):
        if row.column_name not in existing:
            client.command(
                f"ALTER TABLE {database}.{FLAT_TABLE} "
                f"ADD COLUMN IF NOT EXISTS `{row.column_name}` {row.column_type}"
            )
            existing.add(row.column_name)
            added.append(row.column_name)
    if added:
        log.info("events_flat: thêm %d cột mới: %s", len(added), added)
    return added


# ---------------------------------------------------------------------------
# Render + chạy flatten
# ---------------------------------------------------------------------------
def _sql_str(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "\\'")


def render_flat_insert(database: str, ds: str, run_id: str, registry: list[RegistryRow]) -> str:
    """Một câu INSERT phẳng hoá partition ds: cột liệt kê tường minh, không SELECT *.

    SELECT KHÔNG đặt alias: INSERT ... SELECT map cột theo VỊ TRÍ, còn alias trùng
    tên cột bảng nguồn (vd ``AS _dt``) sẽ SHADOW cột thật trong WHERE — bug alias
    shadowing đã dính thật trên 24.8 (WHERE _dt bị so với hằng số -> nạp cả bảng).
    Cột nào ứng với biểu thức nào được chú thích bằng thứ tự trùng với danh sách cột.
    """
    check_ds(ds)
    names: list[str] = [f"`{name}`" for name in base_column_names()]
    selects: list[str] = [f"    {expr}" for _name, _t, expr in BASE_COLUMNS]
    for row in registry:
        alias = _MAP_ALIAS[row.source]
        names.append(f"`{row.column_name}`")
        selects.append(
            f"    tupleElement({alias}['{_sql_str(row.key)}'], '{row.sub_field}')"
        )
    names += ["`_dt`", "`_ingested_at`", "`_run_id`"]
    selects += [
        f"    toDate('{ds}')",
        "    now()",
        f"    '{_sql_str(run_id)}'",
    ]
    select_block = ",\n".join(selects)
    column_block = ",\n    ".join(names)
    return (
        f"INSERT INTO {database}.{FLAT_TABLE}\n(\n    {column_block}\n)\n"
        f"WITH\n"
        f"    CAST(event_params, '{_EP_MAP}') AS _ep,\n"
        f"    CAST(user_properties, '{_UP_MAP}') AS _up\n"
        f"SELECT\n{select_block}\n"
        f"FROM {database}.events_raw\nWHERE _dt = toDate('{ds}')"
    )


def flatten_day(client: Client, settings: Settings, ds: str, run_id: str) -> dict[str, object]:
    """Flatten một ngày từ events_raw (KHÔNG đụng BigQuery): discover -> ALTER -> INSERT.

    Idempotent: DROP PARTITION trên events_flat trước khi insert lại.
    """
    from fb_pipeline.clickhouse.ingest import drop_partition, partition_row_count

    database = settings.clickhouse_db
    new_keys = sync_registry(client, database, ds)
    added = ensure_flat_columns(client, database)
    registry = load_registry(client, database)

    drop_partition(client, database, ds, table=FLAT_TABLE)
    sql = render_flat_insert(database, ds, run_id, registry)
    # Bảng flat rất rộng (hàng trăm cột) — block mặc định (~1M dòng squash trước khi
    # ghi) sẽ OOM. Ép block nhỏ + flush sớm theo bytes; đã cân chỉnh thực tế trên
    # partition 253k dòng / MAX_MEMORY_USAGE 4GB.
    flatten_settings = {
        **insert_settings(settings),
        "max_threads": 2,
        "max_insert_threads": 1,
        "max_block_size": 16384,
        "min_insert_block_size_rows": 16384,
        "min_insert_block_size_bytes": 128 * 1024 * 1024,
    }
    client.command(sql, settings=flatten_settings)

    flat_rows = partition_row_count(client, database, ds, table=FLAT_TABLE)
    raw_rows = partition_row_count(client, database, ds)
    if flat_rows != raw_rows:
        raise RuntimeError(f"Flatten lệch dòng: flat={flat_rows}, raw={raw_rows} (ds={ds})")
    log.info(
        "Flatten %s OK: %d dòng, %d cột động, %d cột mới",
        ds, flat_rows, len(registry), len(added),
    )
    return {"rows": flat_rows, "new_keys": new_keys, "added_columns": added}
