"""Integration test BẮT BUỘC — bằng chứng duy nhất rằng structure khai báo và cú pháp
tuple là đúng: sinh Parquet GA4 nested bằng pyarrow, INSERT qua file() vào ClickHouse
THẬT, đọc lại giá trị nested, và kiểm tra idempotency (DROP PARTITION + insert lại).

Chạy trong container Airflow (`make test`) — cần ClickHouse healthy + shared volume
user_files. Không skip: thiếu môi trường là FAIL, đúng chủ đích.
"""

from __future__ import annotations

import os
import shutil
from pathlib import Path

import pytest

from fb_pipeline.clickhouse import ddl, ingest
from fb_pipeline.clickhouse.client import get_client
from fb_pipeline.config import Settings, load_settings

pytestmark = pytest.mark.integration

# Ngày giả tương lai xa — không bao giờ đụng partition dữ liệu thật
TEST_DS = "2099-01-01"
TEST_DS_NODASH = "20990101"


@pytest.fixture(scope="module")
def settings() -> Settings:
    env = dict(os.environ)
    env["INGEST_STRATEGY"] = "file"  # integration đi đường file() qua shared volume
    return load_settings(env)


@pytest.fixture(scope="module")
def client(settings: Settings):
    c = get_client(settings)
    ddl.apply_schema(c, settings.sql_dir)
    yield c
    # Dọn sạch mọi dấu vết của test: partition raw + flat, cột/registry của key test
    db = settings.clickhouse_db
    ingest.drop_partition(c, db, TEST_DS)
    ingest.drop_partition(c, db, TEST_DS, table="events_flat")
    c.command(f"ALTER TABLE {db}.events_flat DROP COLUMN IF EXISTS `pytest_new_param_str`")
    c.command(f"ALTER TABLE {db}.flat_schema_registry DELETE WHERE key = 'pytest_new_param'")


@pytest.fixture(scope="module")
def staged_parquet(settings: Settings):
    staging = ingest.local_staging_dir(settings, TEST_DS)
    shutil.rmtree(staging, ignore_errors=True)
    staging.mkdir(parents=True, exist_ok=True)
    _write_sample_parquet(staging / "part-000.parquet")
    yield staging
    shutil.rmtree(staging, ignore_errors=True)


def _write_sample_parquet(dest: Path) -> None:
    """Parquet mô phỏng GA4: nested đầy đủ sub-field, có mảng rỗng và sub-field NULL."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    param_value = pa.struct(
        [
            ("string_value", pa.string()),
            ("int_value", pa.int64()),
            ("float_value", pa.float64()),
            ("double_value", pa.float64()),
        ]
    )
    event_params = pa.list_(pa.struct([("key", pa.string()), ("value", param_value)]))
    up_value = pa.struct(
        [
            ("string_value", pa.string()),
            ("int_value", pa.int64()),
            ("float_value", pa.float64()),
            ("double_value", pa.float64()),
            ("set_timestamp_micros", pa.int64()),
        ]
    )
    user_properties = pa.list_(pa.struct([("key", pa.string()), ("value", up_value)]))
    web_info = pa.struct(
        [("browser", pa.string()), ("browser_version", pa.string()), ("hostname", pa.string())]
    )
    device = pa.struct(
        [
            ("category", pa.string()),
            ("mobile_brand_name", pa.string()),
            ("mobile_model_name", pa.string()),
            ("mobile_marketing_name", pa.string()),
            ("mobile_os_hardware_model", pa.string()),
            ("operating_system", pa.string()),
            ("operating_system_version", pa.string()),
            ("vendor_id", pa.string()),
            ("advertising_id", pa.string()),
            ("language", pa.string()),
            ("is_limited_ad_tracking", pa.string()),
            ("time_zone_offset_seconds", pa.int64()),
            ("browser", pa.string()),
            ("browser_version", pa.string()),
            ("web_info", web_info),
        ]
    )
    geo = pa.struct(
        [
            ("continent", pa.string()),
            ("sub_continent", pa.string()),
            ("country", pa.string()),
            ("region", pa.string()),
            ("city", pa.string()),
            ("metro", pa.string()),
        ]
    )
    app_info = pa.struct(
        [
            ("id", pa.string()),
            ("version", pa.string()),
            ("install_store", pa.string()),
            ("firebase_app_id", pa.string()),
            ("install_source", pa.string()),
        ]
    )
    traffic_source = pa.struct(
        [("name", pa.string()), ("medium", pa.string()), ("source", pa.string())]
    )
    privacy_info = pa.struct(
        [
            ("analytics_storage", pa.string()),
            ("ads_storage", pa.string()),
            ("uses_transient_token", pa.string()),
        ]
    )

    schema = pa.schema(
        [
            ("event_date", pa.string()),
            ("event_timestamp", pa.int64()),
            ("event_previous_timestamp", pa.int64()),
            ("event_name", pa.string()),
            ("event_bundle_sequence_id", pa.int64()),
            ("event_server_timestamp_offset", pa.int64()),
            ("event_params", event_params),
            ("user_id", pa.string()),
            ("user_pseudo_id", pa.string()),
            ("user_first_touch_timestamp", pa.int64()),
            ("user_properties", user_properties),
            ("device", device),
            ("geo", geo),
            ("app_info", app_info),
            ("traffic_source", traffic_source),
            ("platform", pa.string()),
            ("stream_id", pa.string()),
            ("privacy_info", privacy_info),
        ]
    )

    def _p(key: str, s=None, i=None, f=None, d=None) -> dict:
        return {
            "key": key,
            "value": {"string_value": s, "int_value": i, "float_value": f, "double_value": d},
        }

    device_row = {
        "category": "mobile",
        "mobile_brand_name": "Samsung",
        "mobile_model_name": None,
        "mobile_marketing_name": None,
        "mobile_os_hardware_model": "SM-A155F",
        "operating_system": "Android",
        "operating_system_version": "14",
        "vendor_id": None,
        "advertising_id": None,
        "language": "vi-vn",
        "is_limited_ad_tracking": "No",
        "time_zone_offset_seconds": 25200,
        "browser": None,
        "browser_version": None,
        "web_info": {"browser": None, "browser_version": None, "hostname": None},
    }
    geo_row = {
        "continent": "Asia",
        "sub_continent": None,
        "country": "Vietnam",
        "region": None,
        "city": "Hanoi",
        "metro": None,
    }
    app_row = {
        "id": "com.example.app",
        "version": "1.2.3",
        "install_store": None,
        "firebase_app_id": "1:1:android:abc",
        "install_source": "com.android.vending",
    }
    ts_row = {"name": None, "medium": "organic", "source": "google"}
    privacy_row = {"analytics_storage": None, "ads_storage": None, "uses_transient_token": "No"}

    rows = {
        "event_date": [TEST_DS_NODASH] * 3,
        "event_timestamp": [1_000, 2_000, 3_000],
        "event_previous_timestamp": [None, 900, None],
        "event_name": ["session_start", "screen_view", "broken_event"],
        "event_bundle_sequence_id": [1, 2, None],
        "event_server_timestamp_offset": [None, None, None],
        "event_params": [
            [_p("ga_session_id", i=123), _p("page", s="home")],
            [],  # mảng rỗng
            [_p("broken")],  # mọi sub-field NULL
        ],
        "user_id": [None, "u-1", None],
        "user_pseudo_id": ["p-1", "p-2", None],
        "user_first_touch_timestamp": [500, None, None],
        "user_properties": [
            [
                {
                    "key": "_ltv_COP",
                    "value": {
                        "string_value": None,
                        "int_value": None,
                        "float_value": None,
                        "double_value": 1.5,
                        "set_timestamp_micros": 999,
                    },
                },
                {
                    "key": "user_id",
                    "value": {
                        "string_value": "u-42",
                        "int_value": None,
                        "float_value": None,
                        "double_value": None,
                        "set_timestamp_micros": None,
                    },
                },
            ],
            [],
            [],
        ],
        "device": [device_row] * 3,
        "geo": [geo_row] * 3,
        "app_info": [app_row] * 3,
        "traffic_source": [ts_row] * 3,
        "platform": ["ANDROID"] * 3,
        "stream_id": ["352963567", None, "352963567"],
        "privacy_info": [privacy_row] * 3,
    }
    pq.write_table(pa.table(rows, schema=schema), dest, compression="snappy")


def _ingest_once(client, settings: Settings, run_id: str) -> None:
    ingest.drop_partition(client, settings.clickhouse_db, TEST_DS)
    sql = ingest.render_insert_sql(
        settings, ds=TEST_DS, run_id=run_id, source_table="pytest_sample", is_intraday=1
    )
    ingest.run_insert(client, settings, sql)


def test_file_ingest_roundtrip_and_idempotency(client, settings: Settings, staged_parquet) -> None:
    db = settings.clickhouse_db
    _ingest_once(client, settings, "pytest-run-1")

    # 1. Số dòng đúng
    assert ingest.partition_row_count(client, db, TEST_DS) == 3

    # 2. length(event_params) đúng từng dòng (kể cả mảng rỗng)
    rows = client.query(
        f"SELECT event_name, length(event_params) FROM {db}.events_raw "
        f"WHERE _dt = toDate('{TEST_DS}') ORDER BY event_timestamp"
    ).result_rows
    assert [(r[0], r[1]) for r in rows] == [
        ("session_start", 2),
        ("screen_view", 0),
        ("broken_event", 1),
    ]

    # 3. Đọc được giá trị nested cụ thể: int_value và string_value của key
    int_val = client.query(
        f"SELECT tupleElement(tupleElement("
        f"arrayFirst(p -> tupleElement(p, 'key') = 'ga_session_id', event_params), "
        f"'value'), 'int_value') "
        f"FROM {db}.events_raw WHERE _dt = toDate('{TEST_DS}') AND event_timestamp = 1000"
    ).result_rows[0][0]
    assert int_val == 123
    str_val = client.query(
        f"SELECT tupleElement(tupleElement("
        f"arrayFirst(p -> tupleElement(p, 'key') = 'page', event_params), "
        f"'value'), 'string_value') "
        f"FROM {db}.events_raw WHERE _dt = toDate('{TEST_DS}') AND event_timestamp = 1000"
    ).result_rows[0][0]
    assert str_val == "home"

    # 4. user_properties: double_value + set_timestamp_micros
    up = client.query(
        f"SELECT tupleElement(tupleElement(user_properties[1], 'value'), 'double_value'), "
        f"tupleElement(tupleElement(user_properties[1], 'value'), 'set_timestamp_micros') "
        f"FROM {db}.events_raw WHERE _dt = toDate('{TEST_DS}') AND event_timestamp = 1000"
    ).result_rows[0]
    assert up == (1.5, 999)

    # 5. Sub-field NULL giữ nguyên NULL
    broken = client.query(
        f"SELECT tupleElement(tupleElement(event_params[1], 'value'), 'string_value') "
        f"FROM {db}.events_raw WHERE _dt = toDate('{TEST_DS}') AND event_timestamp = 3000"
    ).result_rows[0][0]
    assert broken is None

    # 6. Cột MATERIALIZED event_date_d parse đúng 'YYYYMMDD'
    date_d = client.query(
        f"SELECT DISTINCT event_date_d FROM {db}.events_raw WHERE _dt = toDate('{TEST_DS}')"
    ).result_rows[0][0]
    assert str(date_d) == "2099-01-01"

    # 7. Idempotency: DROP PARTITION + insert lại cho kết quả y hệt
    _ingest_once(client, settings, "pytest-run-2")
    assert ingest.partition_row_count(client, db, TEST_DS) == 3
    run_ids = client.query(
        f"SELECT DISTINCT _run_id FROM {db}.events_raw WHERE _dt = toDate('{TEST_DS}')"
    ).result_rows
    assert run_ids == [("pytest-run-2",)]


def test_qc_metrics_on_sample(client, settings: Settings, staged_parquet) -> None:
    """qc_metrics chạy đúng trên dữ liệu nested mẫu (sau roundtrip ở test trên)."""
    m = ingest.qc_metrics(client, settings.clickhouse_db, TEST_DS)
    assert m["rows"] == 3
    assert m["null_pseudo"] == 1
    assert m["empty_params"] == 1
    assert m["uniq_event_names"] == 3
    assert m["max_params_len"] == 2


def _write_new_key_parquet(staging) -> None:
    """Parquet thứ hai: 1 dòng với key CHƯA TỪNG có — chứng minh cột tự thêm."""
    import pyarrow.parquet as pq

    table = pq.read_table(staging / "part-000.parquet")
    one = table.slice(0, 1).to_pylist()[0]
    one["event_timestamp"] = 4_000
    one["event_name"] = "new_key_event"
    one["event_params"] = [
        {
            "key": "pytest_new_param",
            "value": {
                "string_value": "xin chào",
                "int_value": None,
                "float_value": None,
                "double_value": None,
            },
        }
    ]
    one["user_properties"] = []
    import pyarrow as pa

    pq.write_table(
        pa.Table.from_pylist([one], schema=table.schema),
        staging / "part-001.parquet",
        compression="snappy",
    )


def test_flatten_auto_columns(client, settings: Settings, staged_parquet) -> None:
    """Flatten từ raw: cột động đúng quy ước tên, key MỚI -> tự ALTER thêm cột."""
    from datetime import date as date_type

    from fb_pipeline.clickhouse import flat

    db = settings.clickhouse_db
    _ingest_once(client, settings, "pytest-flat-raw")

    result = flat.flatten_day(client, settings, TEST_DS, "pytest-flat-1")
    assert result["rows"] == 3

    cols = {
        str(r[0])
        for r in client.query(
            f"SELECT name FROM system.columns "
            f"WHERE database = '{db}' AND table = 'events_flat'"
        ).result_rows
    }
    # quy ước tên: <key>_<suffix>; user_properties.user_id -> up_user_id_str
    assert {"ga_session_id_int", "page_str", "_ltv_COP_double", "up_user_id_str"} <= cols

    row = client.query(
        f"SELECT ga_session_id_int, page_str, up_user_id_str, event_date, "
        f"device_operating_system, geo_country "
        f"FROM {db}.events_flat WHERE _dt = toDate('{TEST_DS}') AND event_timestamp = 1000"
    ).result_rows[0]
    assert row == (123, "home", "u-42", date_type(2099, 1, 1), "Android", "Vietnam")

    # Key mới xuất hiện trong dữ liệu -> registry -> cột mới, không sửa tay gì
    _write_new_key_parquet(staged_parquet)
    _ingest_once(client, settings, "pytest-flat-raw-2")
    result2 = flat.flatten_day(client, settings, TEST_DS, "pytest-flat-2")
    assert result2["rows"] == 4
    assert "pytest_new_param_str" in result2["added_columns"]
    value = client.query(
        f"SELECT `pytest_new_param_str` FROM {db}.events_flat "
        f"WHERE _dt = toDate('{TEST_DS}') AND event_timestamp = 4000"
    ).result_rows[0][0]
    assert value == "xin chào"

    # Idempotency: flatten lại partition cho kết quả y hệt, không thêm cột nữa
    result3 = flat.flatten_day(client, settings, TEST_DS, "pytest-flat-3")
    assert result3["rows"] == 4
    assert result3["added_columns"] == []
