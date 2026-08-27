"""Unit test cho fb_pipeline.clickhouse.flat: quy ước tên cột, sync DDL, render INSERT."""

from __future__ import annotations

import re
from datetime import date
from pathlib import Path

from fb_pipeline.clickhouse import flat

DDL_PATH = Path(__file__).resolve().parents[1] / "clickhouse" / "sql" / "05_events_flat.sql"


def _normalize(text: str) -> str:
    return re.sub(r"\s+", " ", text)


# ---------------------------------------------------------------------------
# Quy ước đặt tên cột động
# ---------------------------------------------------------------------------
def test_suffix_convention() -> None:
    taken: set[str] = set()
    assert flat.column_name_for("event_params", "ga_session_id", "int_value", taken) == (
        "ga_session_id_int"
    )
    assert flat.column_name_for("event_params", "screen", "string_value", taken) == "screen_str"
    assert flat.column_name_for("event_params", "value", "float_value", taken) == "value_float"
    assert flat.column_name_for("event_params", "value", "double_value", taken) == "value_double"
    assert flat.column_name_for("user_properties", "_ltv_COP", "int_value", taken) == (
        "_ltv_COP_int"
    )


def test_user_properties_user_id_special_case() -> None:
    """Kế thừa quy ước bảng BQ cũ: user_properties.user_id -> up_user_id_str."""
    assert flat.column_name_for("user_properties", "user_id", "string_value", set()) == (
        "up_user_id_str"
    )


def test_collision_gets_source_prefix() -> None:
    taken = {"value_int"}
    assert flat.column_name_for("user_properties", "value", "int_value", taken) == "up_value_int"
    assert flat.column_name_for("event_params", "value", "int_value", taken) == "ep_value_int"


def test_sanitize_key() -> None:
    assert flat.sanitize_key("ok_key") == "ok_key"
    assert flat.sanitize_key("weird-key!x") == "weird_key_x"
    assert flat.sanitize_key("9lives") == "_9lives"


# ---------------------------------------------------------------------------
# BASE_COLUMNS <-> DDL 05_events_flat.sql
# ---------------------------------------------------------------------------
def test_base_columns_unique_and_expected_names() -> None:
    names = flat.base_column_names()
    assert len(names) == len(set(names))
    # mẫu đối chiếu với danh sách cột yêu cầu (BQ cũ)
    for expected in (
        "event_date", "event_ts", "is_active_user", "batch_ordering_id",
        "privacy_ads_storage", "event_dimensions_hostname", "user_ltv_currency",
        "device_mobile_os_hardware_model", "device_web_browser", "device_web_hostname",
        "geo_metro", "app_info_install_source", "traffic_source_medium",
        "cts_srsltid", "stlc_manual_marketing_tactic", "stlc_gads_customer_id",
        "stlc_cross_primary_channel_group", "stlc_sa360_manager_account_name",
        "stlc_cm360_placement_cost_structure", "stlc_dv360_partner_name",
        "publisher_ad_unit_id",
    ):
        assert expected in names, f"Thiếu cột cố định {expected}"


def test_base_columns_match_ddl_names_types_and_order() -> None:
    ddl = _normalize(DDL_PATH.read_text(encoding="utf-8"))
    last_pos = -1
    for name, type_, _expr in flat.BASE_COLUMNS:
        needle = f"`{name}` {_normalize(type_)}"
        pos = ddl.find(needle)
        assert pos != -1, f"Cột {name!r} kiểu {type_!r} không có trong 05_events_flat.sql"
        assert pos > last_pos, f"Cột {name!r} sai thứ tự so với BASE_COLUMNS"
        last_pos = pos
    for needle in ("PARTITION BY _dt", "allow_nullable_key = 1"):
        assert needle in ddl


# ---------------------------------------------------------------------------
# Render INSERT
# ---------------------------------------------------------------------------
def _registry_rows() -> list[flat.RegistryRow]:
    day = date(2026, 8, 27)
    return [
        flat.RegistryRow(
            "event_params", "ga_session_id", "int_value",
            "ga_session_id_int", "Nullable(Int64)", day, day, 10,
        ),
        flat.RegistryRow(
            "user_properties", "_ltv_COP", "double_value",
            "_ltv_COP_double", "Nullable(Float64)", day, day, 3,
        ),
    ]


def test_render_flat_insert() -> None:
    sql = flat.render_flat_insert("fb", "2026-08-27", "run-1", _registry_rows())
    assert sql.startswith("INSERT INTO fb.events_flat")
    assert "SELECT *" not in sql
    assert "WITH" in sql and "CAST(event_params" in sql and "CAST(user_properties" in sql
    assert "tupleElement(_ep['ga_session_id'], 'int_value')" in sql
    assert "tupleElement(_up['_ltv_COP'], 'double_value')" in sql
    assert "`ga_session_id_int`" in sql and "`_ltv_COP_double`" in sql
    assert "FROM fb.events_raw" in sql
    assert "WHERE _dt = toDate('2026-08-27')" in sql
    # KHÔNG alias trong SELECT — alias trùng tên cột nguồn sẽ shadow WHERE _dt
    assert "AS `_dt`" not in sql
    assert " AS `" not in sql.split("WITH")[1]
    # mọi cột cố định đều có mặt, liệt kê tường minh
    for name in flat.base_column_names():
        assert f"`{name}`" in sql


def test_render_flat_insert_escapes_key_quotes() -> None:
    day = date(2026, 8, 27)
    rows = [
        flat.RegistryRow(
            "event_params", "key'with'quote", "string_value",
            "key_with_quote_str", "Nullable(String)", day, day, 1,
        )
    ]
    sql = flat.render_flat_insert("fb", "2026-08-27", "run-1", rows)
    assert "_ep['key\\'with\\'quote']" in sql
