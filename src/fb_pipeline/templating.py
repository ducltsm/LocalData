"""Render template Jinja cho SQL — StrictUndefined để thiếu biến là fail ngay."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from jinja2 import Environment, StrictUndefined

_env = Environment(undefined=StrictUndefined, autoescape=False, keep_trailing_newline=False)


def render_template(path: Path, **context: Any) -> str:
    """Đọc file template và render với context; raise nếu template thiếu biến."""
    return _env.from_string(path.read_text(encoding="utf-8")).render(**context)
