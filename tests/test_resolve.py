"""Logic chọn bảng nguồn: final > intraday > None (DAG sẽ skip)."""

from __future__ import annotations

from fb_pipeline.bq.client import choose_source_table


def test_prefers_final_when_both_exist() -> None:
    assert choose_source_table(True, True, "20260827") == ("events_20260827", 0)


def test_final_only() -> None:
    assert choose_source_table(True, False, "20260827") == ("events_20260827", 0)


def test_fallback_to_intraday() -> None:
    assert choose_source_table(False, True, "20260827") == ("events_intraday_20260827", 1)


def test_none_when_nothing_exists() -> None:
    assert choose_source_table(False, False, "20260827") is None
