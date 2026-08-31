"""fb.mart_* — data mart tổng hợp theo ngày từ fb.events_flat.

Bốn bảng, tất cả PARTITION BY _dt, rebuild theo ngày = DROP PARTITION +
INSERT ... SELECT ... GROUP BY từ events_flat — idempotent giống flatten,
không đụng events_raw/BigQuery:

- ``mart_daily_kpi``    grain ``_dt``                      — KPI tổng ngày
- ``mart_daily_events`` grain ``_dt, event_name``          — trend theo event
- ``mart_daily_geo``    grain ``_dt, country, platform``   — cắt theo thị trường
- ``mart_user_daily``   grain ``_dt, user_pseudo_id``      — nền retention/LTV

Kèm view ``mart_retention`` (cohort theo ngày first_open, tính từ
mart_user_daily — dòng day_n = 0 chính là cohort size).

DDL 06_mart.sql ĐƯỢC SINH từ MART_TABLES + RETENTION_VIEW_SQL trong file này:
``python -m fb_pipeline.tools.mart_day --print-ddl`` (unit test tests/test_mart.py
đối chiếu hai file).

Ba cột động của events_flat (ga_session_id_int, session_engaged_int,
engagement_time_msec_int) do registry tạo lúc runtime nên có thể CHƯA tồn tại
trên database mới — biểu thức metric dùng placeholder ``{tên_cột}``, lúc render
thay bằng tên cột nếu bảng đã có, CAST(NULL ...) nếu chưa (metric liên quan
về 0 thay vì vỡ cả câu INSERT).

Metric nghiệp vụ (send_prompt, paywall_view, in_app_purchase...) đặt theo app
chat01; app khác không có event đó thì cột bằng 0 — vô hại.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

from clickhouse_connect.driver.client import Client

from fb_pipeline.clickhouse.ingest import check_ds, drop_partition, partition_row_count
from fb_pipeline.config import Settings

log = logging.getLogger(__name__)

FLAT_TABLE = "events_flat"

# Cột động của events_flat mà metric phụ thuộc: tên -> kiểu (để CAST NULL khi thiếu)
DYNAMIC_DEPS = {
    "ga_session_id_int": "Nullable(Int64)",
    "session_engaged_int": "Nullable(Int64)",
    "engagement_time_msec_int": "Nullable(Int64)",
}

# Biểu thức dùng chung (placeholder {x} = cột động, xem DYNAMIC_DEPS)
_SESSIONS = (
    "uniqExactIf((user_pseudo_id, {ga_session_id_int}), isNotNull({ga_session_id_int}))"
)
_ENGAGED_SESSIONS = (
    "uniqExactIf((user_pseudo_id, {ga_session_id_int}), {session_engaged_int} = 1)"
)
_ENGAGEMENT_SEC = "round(sum(coalesce({engagement_time_msec_int}, 0)) / 1000, 3)"
_REVENUE_USD = "sumIf(coalesce(event_value_in_usd, 0), event_name = 'in_app_purchase')"
_NEW_USERS = "uniqExactIf(user_pseudo_id, event_name = 'first_open')"
_BUYERS = "uniqExactIf(user_pseudo_id, event_name = 'in_app_purchase')"


@dataclass(frozen=True)
class MartTable:
    """Spec một bảng mart: cột = (tên, kiểu, biểu thức SELECT từ events_flat).

    ``dimensions`` vào cả GROUP BY (lặp lại nguyên biểu thức — SELECT không đặt
    alias, tránh bug alias shadowing như flat.py). ``metrics`` là aggregate.
    """

    name: str
    comment: str
    dimensions: tuple[tuple[str, str, str], ...]
    metrics: tuple[tuple[str, str, str], ...]
    extra_where: str = field(default="")

    def column_specs(self) -> tuple[tuple[str, str, str], ...]:
        return self.dimensions + self.metrics

    def order_by(self) -> tuple[str, ...]:
        return ("_dt", *(name for name, _t, _e in self.dimensions))


MART_TABLES: tuple[MartTable, ...] = (
    MartTable(
        name="mart_daily_kpi",
        comment="KPI tổng theo ngày — đúng 1 dòng mỗi _dt",
        dimensions=(),
        metrics=(
            ("dau", "UInt64", "uniqExact(user_pseudo_id)"),
            ("new_users", "UInt64", _NEW_USERS),
            ("sessions", "UInt64", _SESSIONS),
            ("engaged_sessions", "UInt64", _ENGAGED_SESSIONS),
            ("engagement_sec", "Float64", _ENGAGEMENT_SEC),
            ("events", "UInt64", "count()"),
            ("prompts_sent", "UInt64", "countIf(event_name = 'send_prompt')"),
            ("prompt_users", "UInt64", "uniqExactIf(user_pseudo_id, event_name = 'send_prompt')"),
            ("prompt_results", "UInt64", "countIf(event_name = 'prompt_result')"),
            ("chat_starts", "UInt64", "countIf(event_name = 'chat_start')"),
            ("paywall_views", "UInt64", "countIf(event_name = 'paywall_view')"),
            ("paywall_users", "UInt64", "uniqExactIf(user_pseudo_id, event_name = 'paywall_view')"),
            ("purchases", "UInt64", "countIf(event_name = 'in_app_purchase')"),
            ("buyers", "UInt64", _BUYERS),
            ("revenue_usd", "Float64", _REVENUE_USD),
            ("ad_revenue_usd", "Float64", "sum(coalesce(publisher_ad_revenue_in_usd, 0))"),
            ("app_removes", "UInt64", "uniqExactIf(user_pseudo_id, event_name = 'app_remove')"),
        ),
    ),
    MartTable(
        name="mart_daily_events",
        comment="Đếm theo từng event — sum(events) phải bằng số dòng events_flat của ngày (QC)",
        dimensions=(("event_name", "String", "event_name"),),
        metrics=(
            ("events", "UInt64", "count()"),
            ("users", "UInt64", "uniqExact(user_pseudo_id)"),
            ("sessions", "UInt64", _SESSIONS),
        ),
    ),
    MartTable(
        name="mart_daily_geo",
        comment="Cắt theo thị trường: country x platform",
        dimensions=(
            ("country", "String", "coalesce(geo_country, '')"),
            ("platform", "String", "platform"),
        ),
        metrics=(
            ("users", "UInt64", "uniqExact(user_pseudo_id)"),
            ("new_users", "UInt64", _NEW_USERS),
            ("sessions", "UInt64", _SESSIONS),
            ("events", "UInt64", "count()"),
            ("purchases", "UInt64", "countIf(event_name = 'in_app_purchase')"),
            ("buyers", "UInt64", _BUYERS),
            ("revenue_usd", "Float64", _REVENUE_USD),
        ),
    ),
    MartTable(
        name="mart_user_daily",
        comment="Một dòng / user / ngày hoạt động — nền cho retention (view mart_retention) và LTV",
        dimensions=(("user_pseudo_id", "String", "coalesce(user_pseudo_id, '')"),),
        metrics=(
            ("is_new", "UInt8", "max(event_name = 'first_open')"),
            ("events", "UInt64", "count()"),
            ("sessions", "UInt64", "uniqExact({ga_session_id_int})"),
            ("engagement_sec", "Float64", _ENGAGEMENT_SEC),
            ("prompts_sent", "UInt64", "countIf(event_name = 'send_prompt')"),
            ("purchases", "UInt64", "countIf(event_name = 'in_app_purchase')"),
            ("revenue_usd", "Float64", _REVENUE_USD),
            ("first_event_ts", "DateTime64(6)", "min(event_ts)"),
            ("last_event_ts", "DateTime64(6)", "max(event_ts)"),
            ("platform", "String", "argMax(platform, event_timestamp)"),
            ("country", "String", "argMax(coalesce(geo_country, ''), event_timestamp)"),
            ("app_version", "String", "argMax(coalesce(app_info_version, ''), event_timestamp)"),
        ),
        extra_where="isNotNull(user_pseudo_id)",
    ),
)

# Retention cohort theo ngày first_open. Dòng day_n = 0 = cohort size (user mới
# nào cũng hoạt động đúng ngày first_open của mình) -> retention D_n (%) =
# retained_users(day_n) / retained_users(0).
RETENTION_VIEW_SQL = """CREATE VIEW IF NOT EXISTS fb.mart_retention AS
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
GROUP BY cohort_dt, day_n"""

_DDL_HEADER = """\
-- Các bảng data mart tổng hợp theo ngày từ fb.events_flat. FILE NÀY ĐƯỢC SINH
-- TỪ MART_TABLES trong src/fb_pipeline/clickhouse/mart.py — sửa spec rồi chạy:
--   python -m fb_pipeline.tools.mart_day --print-ddl > clickhouse/sql/06_mart.sql
-- (unit test tests/test_mart.py đối chiếu hai file).
--
-- PARTITION BY _dt như events_flat: rebuild một ngày = DROP PARTITION + INSERT
-- lại từ events_flat — idempotent, không đụng events_raw/BigQuery.
"""


def _ddl_type(type_: str) -> str:
    """String thì nén ZSTD như các DDL khác của project."""
    return f"{type_} CODEC(ZSTD(1))" if type_ == "String" else type_


def render_mart_ddl() -> str:
    """Sinh nội dung clickhouse/sql/06_mart.sql (database fb — apply_schema tự thay)."""
    parts = [_DDL_HEADER]
    for table in MART_TABLES:
        lines = [f"    `{name}` {_ddl_type(type_)}," for name, type_, _e in table.column_specs()]
        lines += [
            "",
            "    -- Metadata",
            "    `_dt` Date,",
            "    `_built_at` DateTime DEFAULT now(),",
            "    `_run_id` String CODEC(ZSTD(1))",
        ]
        body = "\n".join(lines)
        order_by = ", ".join(table.order_by())
        parts.append(
            f"-- {table.comment}\n"
            f"CREATE TABLE IF NOT EXISTS fb.{table.name}\n(\n{body}\n)\n"
            f"ENGINE = MergeTree\nPARTITION BY _dt\nORDER BY ({order_by});\n"
        )
    parts.append(f"{RETENTION_VIEW_SQL};\n")
    return "\n".join(parts)


def _placeholder_map(available_columns: set[str]) -> dict[str, str]:
    """Cột động có trên events_flat -> tên cột; chưa có -> NULL đúng kiểu."""
    return {
        col: f"`{col}`" if col in available_columns else f"CAST(NULL AS {type_})"
        for col, type_ in DYNAMIC_DEPS.items()
    }


def render_mart_insert(
    table: MartTable,
    database: str,
    ds: str,
    run_id: str,
    available_columns: set[str],
) -> str:
    """Một câu INSERT rebuild partition ds của một bảng mart.

    SELECT không đặt alias (bug alias shadowing — xem flat.render_flat_insert);
    GROUP BY lặp lại nguyên biểu thức dimension.
    """
    check_ds(ds)
    mapping = _placeholder_map(available_columns)
    names = [f"`{name}`" for name, _t, _e in table.column_specs()]
    selects = [f"    {expr.format(**mapping)}" for _n, _t, expr in table.column_specs()]
    names += ["`_dt`", "`_built_at`", "`_run_id`"]
    run_id_sql = run_id.replace("\\", "\\\\").replace("'", "\\'")
    selects += [f"    toDate('{ds}')", "    now()", f"    '{run_id_sql}'"]

    where = f"WHERE _dt = toDate('{ds}')"
    if table.extra_where:
        where += f" AND {table.extra_where}"
    group_by = ""
    if table.dimensions:
        dim_exprs = ", ".join(expr.format(**mapping) for _n, _t, expr in table.dimensions)
        group_by = f"\nGROUP BY {dim_exprs}"
    return (
        f"INSERT INTO {database}.{table.name}\n(\n    " + ",\n    ".join(names) + "\n)\n"
        "SELECT\n" + ",\n".join(selects) + f"\nFROM {database}.{FLAT_TABLE}\n{where}{group_by}"
    )


def _flat_columns(client: Client, database: str) -> set[str]:
    rows = client.query(
        f"SELECT name FROM system.columns "
        f"WHERE database = '{database}' AND table = '{FLAT_TABLE}'"
    ).result_rows
    return {str(row[0]) for row in rows}


def build_mart_day(client: Client, settings: Settings, ds: str, run_id: str) -> dict[str, int]:
    """Rebuild mọi bảng mart cho một ngày từ events_flat (KHÔNG đụng BigQuery).

    Idempotent: DROP PARTITION từng bảng trước khi insert lại. QC: sum(events)
    của mart_daily_events phải bằng số dòng events_flat của partition.
    """
    database = settings.clickhouse_db
    flat_rows = partition_row_count(client, database, ds, table=FLAT_TABLE)
    if flat_rows == 0:
        raise RuntimeError(f"Partition events_flat {ds} rỗng — flatten trước khi build mart")

    available = _flat_columns(client, database)
    missing = [col for col in DYNAMIC_DEPS if col not in available]
    if missing:
        log.warning("events_flat chưa có cột động %s — metric liên quan sẽ bằng 0", missing)

    # GROUP BY trên partition lớn: cho phép spill ra disk thay vì OOM
    mart_settings = {
        "max_threads": settings.max_insert_threads,
        "max_insert_threads": settings.max_insert_threads,
        "max_memory_usage": settings.max_memory_usage,
        "max_bytes_before_external_group_by": settings.max_memory_usage // 2,
    }
    results: dict[str, int] = {}
    for table in MART_TABLES:
        drop_partition(client, database, ds, table=table.name)
        sql = render_mart_insert(table, database, ds, run_id, available)
        client.command(sql, settings=mart_settings)
        results[table.name] = partition_row_count(client, database, ds, table=table.name)

    mart_events = int(
        client.query(
            f"SELECT coalesce(sum(events), 0) FROM {database}.mart_daily_events "
            f"WHERE _dt = toDate('{ds}')"
        ).result_rows[0][0]
    )
    if mart_events != flat_rows:
        raise RuntimeError(
            f"Mart lệch dòng: sum(mart_daily_events.events)={mart_events}, "
            f"events_flat={flat_rows} (ds={ds})"
        )
    log.info("Mart %s OK: %s (events khớp flat: %d)", ds, results, flat_rows)
    return results
