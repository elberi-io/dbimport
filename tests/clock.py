"""A host time zone other than UTC and a frozen clock, for tests of the
times DBImport stores and sends.

The test containers and the compose server run in UTC, where local time
and UTC are the same; these helpers make the difference visible.
"""

import os
import time
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, tzinfo

HOST_ZONE = "Europe/Stockholm"
# 12:00 in HOST_ZONE (summer time).
NOW = datetime(2026, 6, 1, 10, 0, 0, tzinfo=UTC)
# NOW as a DATETIME column holds it: UTC, without a zone (ADR-0007).
STORED_NOW = NOW.replace(tzinfo=None)


def stored_time(text: str) -> datetime:
    """A value as a DATETIME column holds it, without a zone, from ISO
    text such as '2026-06-01 12:00'."""
    return datetime.fromisoformat(text)


@contextmanager
def host_zone(zone: str = HOST_ZONE) -> Iterator[None]:
    """Runs the block with zone as the local time zone of the process."""
    before = os.environ.get("TZ")
    os.environ["TZ"] = zone
    time.tzset()
    try:
        if time.timezone == 0 and time.altzone == 0:
            raise RuntimeError(
                f"The time zone {zone} has no effect; is the system time zone "
                "database (tzdata) installed?"
            )
        yield
    finally:
        if before is None:
            del os.environ["TZ"]
        else:
            os.environ["TZ"] = before
        time.tzset()


def frozen_datetime(instant: datetime) -> type[datetime]:
    """A datetime class whose now() returns instant, in local time when no
    zone is given, as datetime.now() does."""

    class FrozenDatetime(datetime):
        @classmethod
        def now(cls, tz: tzinfo | None = None) -> datetime:  # type: ignore[override]  # a plain datetime, which the database driver binds as in production
            return datetime.fromtimestamp(instant.timestamp(), tz)

    return FrozenDatetime
