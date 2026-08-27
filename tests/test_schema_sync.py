"""source_schema.py phải khớp DDL 02_events_raw.sql — cùng tên, cùng thứ tự, cùng kiểu.

Đây là test bắt lỗi lệch schema sớm nhất: sửa một bên mà quên bên kia là fail ngay.
"""

from __future__ import annotations

import re
from pathlib import Path

from fb_pipeline.clickhouse.source_schema import SOURCE_COLUMNS, column_names, structure

DDL_PATH = Path(__file__).resolve().parents[1] / "clickhouse" / "sql" / "02_events_raw.sql"


def _normalize(text: str) -> str:
    return re.sub(r"\s+", " ", text)


def test_source_columns_match_ddl_names_types_and_order() -> None:
    ddl = _normalize(DDL_PATH.read_text(encoding="utf-8"))
    last_pos = -1
    for name, type_ in SOURCE_COLUMNS:
        needle = f"{name} {_normalize(type_)}"
        pos = ddl.find(needle)
        assert pos != -1, f"Cột {name!r} kiểu {type_!r} không có trong DDL (hoặc kiểu lệch)"
        assert pos > last_pos, f"Cột {name!r} sai thứ tự so với source_schema"
        last_pos = pos


def test_ddl_has_metadata_columns_and_partitioning() -> None:
    ddl = _normalize(DDL_PATH.read_text(encoding="utf-8"))
    for needle in (
        "_dt Date",
        "_ingested_at DateTime DEFAULT now()",
        "_run_id String",
        "_source_table String",
        "_is_intraday UInt8",
        "PARTITION BY _dt",
        "ORDER BY (_dt, event_name, user_pseudo_id, event_timestamp)",
        "allow_nullable_key = 1",
    ):
        assert needle in ddl, f"DDL thiếu: {needle}"


def test_event_timestamp_stays_int64() -> None:
    """Raw giữ nguyên kiểu nguồn — không convert DateTime64 ở phase 1."""
    assert dict(SOURCE_COLUMNS)["event_timestamp"] == "Int64"


def test_structure_is_single_line_and_names_unique() -> None:
    assert "\n" not in structure()
    names = column_names()
    assert len(names) == len(set(names))
    assert names[0] == "event_date"
