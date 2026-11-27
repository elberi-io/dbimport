"""store times in utc

Revision ID: 5f880ae9d837
Revises: 6b6d05af061f
Create Date: 2026-11-27 20:00:00.000000

Converts the DATETIME columns from the local time zone the DBImport hosts
wrote them in to UTC (ADR-0007), and back on downgrade. The zone is not
stored anywhere: setup passes it from --localTimeZone, the Alembic command
line from -x local_time_zone=<zone>. Without rows to convert, no zone is
needed.
"""

from collections.abc import Callable, Sequence
from datetime import UTC, datetime, tzinfo
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import sqlalchemy as sa
from alembic import context, op
from alembic.util import CommandError

revision: str = "5f880ae9d837"
down_revision: str | Sequence[str] | None = "6b6d05af061f"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# The source database's own time, compared with the source: not converted.
_NOT_CONVERTED = {("atlas_column_cache", "table_create_time")}
# New values are first written this far ahead, so that no new value meets
# an old one in a primary key (the retry logs and import_failure_log have
# one with a time). 4000 years keep the leap days of the Gregorian calendar.
# Values from year 5000 on are no times DBImport wrote; they stay.
_SHIFT_YEARS = 4000
_LAST_YEAR = 4999
_BATCH = 10000


def upgrade() -> None:
    _convert(lambda zone, value: value.replace(tzinfo=zone).astimezone(UTC))


def downgrade() -> None:
    _convert(lambda zone, value: value.replace(tzinfo=UTC).astimezone(zone))


def _convert(convert: Callable[[tzinfo, datetime], datetime]) -> None:
    conn = op.get_bind()
    columns = _time_columns(conn)
    if not any(_has_values(conn, table, column) for table, column in columns):
        return
    zone = _zone()
    conn.execute(
        sa.text(
            "CREATE TEMPORARY TABLE utc_conversion "
            "(old DATETIME NOT NULL PRIMARY KEY, new DATETIME NOT NULL, "
            "shifted DATETIME NOT NULL, INDEX (shifted))"
        )
    )
    try:
        keyed = _keyed_columns(conn)
        for table, column in columns:
            _convert_column(
                conn, table, column, zone, convert, (table, column) in keyed
            )
    finally:
        conn.execute(sa.text("DROP TEMPORARY TABLE utc_conversion"))


def _time_columns(conn: sa.Connection) -> list[tuple[str, str]]:
    rows = conn.execute(
        sa.text(
            "SELECT TABLE_NAME, COLUMN_NAME FROM information_schema.COLUMNS "
            "WHERE TABLE_SCHEMA = DATABASE() AND DATA_TYPE = 'datetime' "
            "ORDER BY TABLE_NAME, ORDINAL_POSITION"
        )
    )
    return [
        (table, column)
        for table, column in rows
        if (table, column) not in _NOT_CONVERTED
    ]


def _has_values(conn: sa.Connection, table: str, column: str) -> bool:
    return (
        conn.execute(
            sa.text(
                f"SELECT 1 FROM `{table}` WHERE `{column}` >= '1000-01-01'"
                f" AND `{column}` < '{_LAST_YEAR + 1}-01-01' LIMIT 1"
            )
        ).first()
        is not None
    )


def _zone() -> tzinfo:
    name = context.config.attributes.get("local_time_zone") or context.get_x_argument(
        as_dictionary=True
    ).get("local_time_zone")
    if not name:
        raise CommandError(
            "The configuration database holds times in the local time zone of "
            "the DBImport hosts, and this version stores them in UTC. Name that "
            "zone: setup --upgradeDB --localTimeZone=<zone> (for example "
            "Europe/Stockholm), or -x local_time_zone=<zone> for alembic."
        )
    try:
        return ZoneInfo(name)
    except (ZoneInfoNotFoundError, ValueError) as err:
        raise CommandError(f"Unknown time zone: {name}") from err


def _convert_column(
    conn: sa.Connection,
    table: str,
    column: str,
    zone: tzinfo,
    convert: Callable[[tzinfo, datetime], datetime],
    keyed: bool,
) -> None:
    values = conn.execute(
        sa.text(
            f"SELECT DISTINCT `{column}` FROM `{table}` WHERE `{column}` IS NOT NULL"
        )
    ).scalars()
    # A zero date comes back as text; it stays as it is.
    pairs = []
    for value in values:
        if isinstance(value, datetime) and value.year <= _LAST_YEAR:
            new = convert(zone, value).replace(tzinfo=None)
            shifted = new.replace(year=new.year + _SHIFT_YEARS)
            pairs.append({"old": value, "new": new, "shifted": shifted})
    if not pairs:
        return
    if keyed:
        _refuse_equal_times(table, column, pairs)
    conn.execute(sa.text("DELETE FROM utc_conversion"))
    insert = sa.text(
        "INSERT INTO utc_conversion (old, new, shifted) VALUES (:old, :new, :shifted)"
    )
    for start in range(0, len(pairs), _BATCH):
        conn.execute(insert, pairs[start : start + _BATCH])
    conn.execute(
        sa.text(
            f"UPDATE `{table}` t JOIN utc_conversion c ON t.`{column}` = c.old "
            f"SET t.`{column}` = c.shifted"
        )
    )
    conn.execute(
        sa.text(
            f"UPDATE `{table}` t JOIN utc_conversion c ON t.`{column}` = c.shifted "
            f"SET t.`{column}` = c.new"
        )
    )


def _keyed_columns(conn: sa.Connection) -> set[tuple[str, str]]:
    """The columns in a primary key or unique index."""
    rows = conn.execute(
        sa.text(
            "SELECT TABLE_NAME, COLUMN_NAME FROM information_schema.STATISTICS "
            "WHERE TABLE_SCHEMA = DATABASE() AND NON_UNIQUE = 0"
        )
    )
    return {(table, column) for table, column in rows}


def _refuse_equal_times(
    table: str, column: str, pairs: list[dict[str, datetime]]
) -> None:
    """Two times that become one could break the key of the column: in the
    hour that repeats when daylight saving time ends, two UTC times are the
    same local time. The revision stops; its transaction is rolled back."""
    seen: dict[datetime, datetime] = {}
    for pair in pairs:
        other = seen.setdefault(pair["new"], pair["old"])
        if other != pair["old"]:
            raise CommandError(
                f"{table}.{column} holds {other} and {pair['old']}, which both "
                f"become {pair['new']}; its key does not allow that. Change one "
                "of the rows and run the command again."
            )
