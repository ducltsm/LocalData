"""Flatten thủ công một ngày (hoặc dải ngày) từ events_raw — KHÔNG đụng BigQuery.

Gọi: make flatten DATE=2026-08-27
     python -m fb_pipeline.tools.flatten_day --date-from 2026-08-01 --date-to 2026-08-27
"""

from __future__ import annotations

import argparse
import logging
from datetime import date, timedelta

from fb_pipeline.clickhouse.client import get_client
from fb_pipeline.clickhouse.flat import flatten_day
from fb_pipeline.clickhouse.ingest import check_ds
from fb_pipeline.config import load_settings

log = logging.getLogger(__name__)


def main() -> None:
    """Entry point CLI."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", help="YYYY-MM-DD (một ngày)")
    parser.add_argument("--date-from", help="YYYY-MM-DD (bắt đầu dải)")
    parser.add_argument("--date-to", help="YYYY-MM-DD (kết thúc dải, inclusive)")
    args = parser.parse_args()

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
        parser.error("Cần --date hoặc cặp --date-from/--date-to")
        return

    settings = load_settings()
    client = get_client(settings)
    for ds in days:
        result = flatten_day(client, settings, ds, run_id=f"manual-flatten-{ds}")
        print(f"{ds}: {result['rows']} dòng, cột mới: {result['added_columns'] or 'không'}")


if __name__ == "__main__":
    main()
