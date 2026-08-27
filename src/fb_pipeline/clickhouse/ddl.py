"""Apply DDL từ clickhouse/sql/ (idempotent) + TTL theo cấu hình."""

from __future__ import annotations

import logging
import re
from collections.abc import Iterator
from pathlib import Path

from clickhouse_connect.driver.client import Client

log = logging.getLogger(__name__)


def iter_statements(sql_text: str) -> Iterator[str]:
    """Tách file SQL thành từng statement theo ';'.

    Strip comment ``--`` trước khi split để dấu ';' trong comment không cắt
    statement giữa chừng. Đủ dùng cho các file DDL của project
    (không có ';' hay '--' trong string literal).
    """
    no_comments = "\n".join(line.split("--", 1)[0] for line in sql_text.splitlines())
    for chunk in no_comments.split(";"):
        stmt = chunk.strip()
        if stmt:
            yield stmt


def schema_files(sql_dir: Path) -> list[Path]:
    """Các file DDL đánh số 0*.sql, đúng thứ tự apply."""
    return sorted(sql_dir.glob("0*.sql"))


def apply_schema(client: Client, sql_dir: Path, database: str = "fb") -> None:
    """Chạy lần lượt mọi statement trong các file 0*.sql (tất cả đều IF NOT EXISTS).

    File DDL viết với database ``fb``; khi ``database`` khác (chạy nhiều app,
    mỗi app một database — vd ``du02``) thì thay prefix lúc apply. Chỉ thay
    token ``fb.`` và ``... EXISTS fb`` — an toàn vì DDL của project không có
    chuỗi nào khác trùng dạng đó.
    """
    for path in schema_files(sql_dir):
        for stmt in iter_statements(path.read_text(encoding="utf-8")):
            if database != "fb":
                stmt = re.sub(r"\bfb\.", f"{database}.", stmt)
                stmt = re.sub(r"(DATABASE IF NOT EXISTS )fb\b", rf"\g<1>{database}", stmt)
            client.command(stmt)
        log.info("Đã apply %s (database=%s)", path.name, database)


def apply_ttl(client: Client, database: str, raw_ttl_days: int) -> None:
    """RAW_TTL_DAYS > 0 thì gắn TTL theo _dt; 0 (mặc định) = giữ vĩnh viễn, không đụng gì."""
    if raw_ttl_days > 0:
        client.command(
            f"ALTER TABLE {database}.events_raw MODIFY TTL _dt + INTERVAL {int(raw_ttl_days)} DAY"
        )
        log.info("Đã đặt TTL %d ngày cho %s.events_raw", raw_ttl_days, database)
    else:
        log.info("RAW_TTL_DAYS=0 — không đặt TTL (giữ vĩnh viễn)")
