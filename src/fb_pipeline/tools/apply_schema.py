"""Apply lại clickhouse/sql/ (idempotent) + TTL theo RAW_TTL_DAYS. Gọi: make ch-schema."""

from __future__ import annotations

import logging

from fb_pipeline.clickhouse import ddl
from fb_pipeline.clickhouse.client import get_client
from fb_pipeline.config import load_settings

log = logging.getLogger(__name__)


def main() -> None:
    """Chạy toàn bộ DDL rồi apply TTL nếu được cấu hình."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    settings = load_settings()
    # connect vào 'default' vì database đích có thể chưa tồn tại (app mới)
    client = get_client(settings, database="default")
    ddl.apply_schema(client, settings.sql_dir, database=settings.clickhouse_db)
    ddl.apply_ttl(client, settings.clickhouse_db, settings.raw_ttl_days)
    log.info("Schema OK — database %s", settings.clickhouse_db)


if __name__ == "__main__":
    main()
