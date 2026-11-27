"""The time text of DBImport's outputs and REST requests (ADR-0007)."""

from datetime import UTC, datetime, timedelta, timezone

import pytest

from dbimport.common.iso_time import from_iso_time, iso_time
from tests.clock import NOW, stored_time


@pytest.mark.parametrize(
    "value", [NOW, NOW.astimezone(timezone(timedelta(hours=2)))], ids=["utc", "+02"]
)
def test_iso_time_writes_utc_with_its_offset(value: datetime) -> None:
    assert iso_time(value) == "2026-06-01T10:00:00+00:00"


def test_iso_time_of_a_time_without_a_zone_raises() -> None:
    with pytest.raises(ValueError, match="no zone"):
        iso_time(stored_time("2026-06-01 10:00"))


@pytest.mark.parametrize(
    "text", ["2026-06-01T10:00:00+00:00", "2026-06-01T12:00:00+02:00"]
)
def test_from_iso_time_reads_a_time_with_offset_as_utc(text: str) -> None:
    assert from_iso_time(text) == NOW
    assert from_iso_time(text).tzinfo == UTC


@pytest.mark.parametrize("text", ["2026-06-01 10:00:00", "yesterday"])
def test_from_iso_time_of_other_text_raises(text: str) -> None:
    with pytest.raises(ValueError):
        from_iso_time(text)
