"""Unit test cho fb_pipeline.clickhouse.mart: sync DDL 06_mart.sql, render INSERT."""

from __future__ import annotations

from pathlib import Path

from fb_pipeline.clickhouse import mart

DDL_PATH = Path(__file__).resolve().parents[1] / "clickhouse" / "sql" / "06_mart.sql"

_ALL_DYNAMIC = set(mart.DYNAMIC_DEPS)


# ---------------------------------------------------------------------------
# Spec
# ---------------------------------------------------------------------------
def test_table_names_unique_and_prefixed() -> None:
    names = [t.name for t in mart.MART_TABLES]
    assert len(names) == len(set(names))
    assert all(name.startswith("mart_") for name in names)


def test_column_names_unique_per_table() -> None:
    for table in mart.MART_TABLES:
        names = [name for name, _t, _e in table.column_specs()]
        assert len(names) == len(set(names)), f"{table.name}: cột trùng tên"
        # metadata thêm lúc render, không được khai trong spec
        assert not set(names) & {"_dt", "_built_at", "_run_id"}


# ---------------------------------------------------------------------------
# MART_TABLES <-> DDL 06_mart.sql (file sinh từ render_mart_ddl)
# ---------------------------------------------------------------------------
def test_ddl_file_matches_render() -> None:
    assert DDL_PATH.read_text(encoding="utf-8") == mart.render_mart_ddl(), (
        "06_mart.sql lệch với MART_TABLES — chạy lại: "
        "python -m fb_pipeline.tools.mart_day --print-ddl > clickhouse/sql/06_mart.sql"
    )


def test_ddl_contains_retention_view() -> None:
    ddl = DDL_PATH.read_text(encoding="utf-8")
    assert "CREATE VIEW IF NOT EXISTS fb.mart_retention" in ddl
    assert "fb.mart_user_daily" in ddl


# ---------------------------------------------------------------------------
# Render INSERT
# ---------------------------------------------------------------------------
def _table(name: str) -> mart.MartTable:
    return next(t for t in mart.MART_TABLES if t.name == name)


def _render(name: str, available: set[str] = _ALL_DYNAMIC) -> str:
    return mart.render_mart_insert(_table(name), "fb", "2026-08-27", "run-1", available)


def test_render_insert_kpi_no_group_by() -> None:
    sql = _render("mart_daily_kpi")
    assert sql.startswith("INSERT INTO fb.mart_daily_kpi")
    assert "SELECT *" not in sql
    assert "GROUP BY" not in sql  # grain _dt: aggregate cả partition, 1 dòng
    assert "FROM fb.events_flat" in sql
    assert "WHERE _dt = toDate('2026-08-27')" in sql
    assert "`ga_session_id_int`" in sql
    # KHÔNG alias trong SELECT — alias trùng tên cột nguồn sẽ shadow WHERE _dt
    assert " AS `" not in sql


def test_render_insert_group_by_repeats_dimension_exprs() -> None:
    sql = _render("mart_daily_geo")
    assert "GROUP BY coalesce(geo_country, ''), platform" in sql


def test_render_insert_user_daily_filters_null_users() -> None:
    sql = _render("mart_user_daily")
    assert "AND isNotNull(user_pseudo_id)" in sql
    assert "GROUP BY coalesce(user_pseudo_id, '')" in sql


def test_render_insert_missing_dynamic_columns_become_null() -> None:
    """Database mới chưa flatten lần nào: cột động chưa tồn tại -> CAST NULL, không vỡ INSERT."""
    sql = mart.render_mart_insert(_table("mart_daily_kpi"), "fb", "2026-08-27", "run-1", set())
    assert "`ga_session_id_int`" not in sql
    assert "CAST(NULL AS Nullable(Int64))" in sql


def test_render_insert_escapes_run_id() -> None:
    sql = mart.render_mart_insert(
        _table("mart_daily_kpi"), "fb", "2026-08-27", "run'quote", _ALL_DYNAMIC
    )
    assert "'run\\'quote'" in sql
