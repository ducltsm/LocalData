"""Rebuild bảng mart thủ công một ngày (hoặc dải ngày) từ events_flat — KHÔNG đụng BigQuery.

Gọi: make mart DATE=2026-08-27
     python -m fb_pipeline.tools.mart_day --date-from 2026-08-01 --date-to 2026-08-27
     python -m fb_pipeline.tools.mart_day --print-ddl   # sinh clickhouse/sql/06_mart.sql
"""

from __future__ import annotations

import argparse
import logging
from datetime import date, timedelta

from fb_pipeline.clickhouse.ingest import check_ds
from fb_pipeline.clickhouse.mart import build_mart_day, render_mart_ddl

log = logging.getLogger(__name__)


def main() -> None:
    """Entry point CLI."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", help="YYYY-MM-DD (một ngày)")
    parser.add_argument("--date-from", help="YYYY-MM-DD (bắt đầu dải)")
    parser.add_argument("--date-to", help="YYYY-MM-DD (kết thúc dải, inclusive)")
    parser.add_argument(
        "--print-ddl", action="store_true",
        help="In DDL 06_mart.sql sinh từ MART_TABLES rồi thoát (không cần ClickHouse)",
    )
    args = parser.parse_args()

    if args.print_ddl:
        print(render_mart_ddl(), end="")
        return

    if args.date:
        days = [check_ds(args.date)]
    elif args.date_from and args.date_to:
        start = date.fromisoformat(check_ds(args.date_from))
        end = date.fromisoformat(check_ds(args.date_to))
        days = [
            (start + timedelta(days=i)).isoformat()
            for i in range((end - start).days + 1)
        ]
    else:
        parser.error("Cần --date, cặp --date-from/--date-to, hoặc --print-ddl")
        return

    from fb_pipeline.clickhouse.client import get_client
    from fb_pipeline.config import load_settings

    settings = load_settings()
    client = get_client(settings)
    for ds in days:
        result = build_mart_day(client, settings, ds, run_id=f"manual-mart-{ds}")
        print(f"{ds}: " + ", ".join(f"{t}={n} dòng" for t, n in result.items()))


if __name__ == "__main__":
    main()
