"""Read-only, KHÔNG thuộc pipeline: khảo sát key/kiểu trong event_params & user_properties.

Gọi: make explore-keys DATE=2026-08-27

Đầu ra là nguyên liệu cho phase 2 (suy kiểu cột khi flatten): mỗi key xuất hiện
bao nhiêu lần, ở sub-field nào (string/int/float/double), kèm một giá trị mẫu.
Dùng tupleElement() thay vì cú pháp p.1 vì ổn định giữa các version ClickHouse.
"""

from __future__ import annotations

import argparse
import logging

from clickhouse_connect.driver.client import Client

from fb_pipeline.clickhouse.client import get_client
from fb_pipeline.clickhouse.ingest import check_ds
from fb_pipeline.config import load_settings

log = logging.getLogger(__name__)

_V = "tupleElement(tupleElement(p, 'value'), '{sub}')"


def _query(client: Client, database: str, ds: str, array_column: str, subfields: list[str]) -> None:
    value_exprs = {sub: _V.format(sub=sub) for sub in subfields}
    count_cols = ",\n            ".join(
        f"countIf(isNotNull({expr})) AS n_{sub}" for sub, expr in value_exprs.items()
    )
    string_expr = value_exprs["string_value"]
    sql = f"""
        SELECT
            tupleElement(p, 'key') AS key,
            {count_cols},
            count() AS n_total,
            anyIf(toString({string_expr}), isNotNull({string_expr})) AS sample
        FROM {database}.events_raw
        ARRAY JOIN {array_column} AS p
        WHERE _dt = toDate('{ds}')
        GROUP BY key
        ORDER BY n_total DESC
    """
    result = client.query(sql)
    headers = [f"n_{sub}" for sub in subfields] + ["n_total", "sample"]
    print(f"\n=== {array_column} ({ds}) — {len(result.result_rows)} key ===")
    print(f"{'key':<40} " + " ".join(f"{h:>12}" for h in headers[:-1]) + "  sample")
    for row in result.result_rows:
        key = str(row[0])[:40]
        nums = " ".join(f"{int(v):>12}" for v in row[1:-1])
        sample = "" if row[-1] is None else str(row[-1])[:40]
        print(f"{key:<40} {nums}  {sample}")


def main() -> None:
    """Entry point CLI."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True, help="YYYY-MM-DD")
    args = parser.parse_args()
    ds = check_ds(args.date)

    settings = load_settings()
    client = get_client(settings)
    _query(
        client,
        settings.clickhouse_db,
        ds,
        "event_params",
        ["string_value", "int_value", "float_value", "double_value"],
    )
    _query(
        client,
        settings.clickhouse_db,
        ds,
        "user_properties",
        ["string_value", "int_value", "float_value", "double_value", "set_timestamp_micros"],
    )


if __name__ == "__main__":
    main()
