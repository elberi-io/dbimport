import os
import subprocess
import sys
from collections.abc import Callable, Iterator
from datetime import datetime

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.script import ScriptDirectory

from tests.clock import stored_time
from tests.configdb import (
    ORM_DATABASE,
    REPO_ROOT,
    alembic_config,
    insert_row,
    schema_rows,
)

SETUP = REPO_ROOT / "bin" / "setup"
# A revision of the migration chain before the baseline (0.80.008).
OLD_REVISION = "af71422a72a2"
VERSION_QUERY = sa.text("SELECT version_num FROM alembic_version")
# The revision before the stored times move to UTC (ADR-0007).
BEFORE_UTC = "6b6d05af061f"
# Local times of a host in Europe/Stockholm (summer UTC+2, winter UTC+1)
# and the same instants in UTC. A value from year 5000 on is no time
# DBImport wrote, and the source create time is not converted.
LOCAL_TIMES = [
    stored_time(text)
    for text in (
        "2026-06-01 12:00",
        "9999-12-31 23:59:59",
        "2026-01-15 12:00",
        "2026-01-15 13:00",
        "2026-06-01 12:00",
    )
]
UTC_TIMES = [
    stored_time(text)
    for text in (
        "2026-06-01 10:00",
        "9999-12-31 23:59:59",
        "2026-01-15 11:00",
        "2026-01-15 12:00",
        "2026-06-01 12:00",
    )
]


def _setup(*options: str, **settings: str) -> subprocess.CompletedProcess[str]:
    """Runs bin/setup; settings override the test configuration, as
    DBIMPORT_<SECTION>__<KEY>=value environment variables."""
    return subprocess.run(
        [sys.executable, str(SETUP), *options],
        capture_output=True,
        text=True,
        timeout=120,
        env={**os.environ, **settings},
        check=False,
    )


@pytest.fixture
def stamp(
    clean_config_db: None, db_connection: sa.Connection
) -> Iterator[Callable[[str | None], None]]:
    """Sets the revision the database claims to be at, without changing the
    schema. The head is restored after the test, because the schema is
    built once per session."""
    head = db_connection.execute(VERSION_QUERY).scalar_one()

    def _stamp(revision: str | None) -> None:
        db_connection.execute(sa.text("DELETE FROM alembic_version"))
        if revision is not None:
            db_connection.execute(
                sa.text("INSERT INTO alembic_version VALUES (:revision)"),
                {"revision": revision},
            )

    try:
        yield _stamp
    finally:
        _stamp(head)


@pytest.fixture
def upgrade_after(clean_config_db: None) -> Iterator[None]:
    """For a test that downgrades: upgrades the schema to the head again
    after it, because the schema is built once per session. Rows a test
    leaves are taken as UTC (ADR-0007)."""
    try:
        yield
    finally:
        config = alembic_config()
        config.attributes["local_time_zone"] = "UTC"
        command.upgrade(config, "head")


def test_setup_downgrade_db_goes_back_one_revision(
    upgrade_after: None, db_connection: sa.Connection
) -> None:
    head = db_connection.execute(VERSION_QUERY).scalar_one()
    script = ScriptDirectory.from_config(alembic_config())

    result = _setup("--downgradeDB")

    assert result.returncode == 0, result.stdout + result.stderr
    assert (
        db_connection.execute(VERSION_QUERY).scalar_one()
        == script.get_revision(head).down_revision
    )


def test_setup_upgrade_db_after_downgrade_db_restores_the_schema(
    upgrade_after: None, db_connection: sa.Connection
) -> None:
    before = schema_rows(db_connection)

    # The newest revision converts data only; the one before it changes
    # the schema.
    downgrades = [_setup("--downgradeDB").returncode for _ in range(2)]
    downgraded = schema_rows(db_connection)
    upgrade = _setup("--upgradeDB")

    assert (downgrades, upgrade.returncode) == ([0, 0], 0)
    assert downgraded != before
    assert schema_rows(db_connection) == before


def test_setup_downgrade_db_at_the_baseline_stops(
    stamp: Callable[[str | None], None], db_connection: sa.Connection
) -> None:
    baseline = ScriptDirectory.from_config(alembic_config()).get_base()
    assert baseline is not None
    stamp(baseline)

    result = _setup("--downgradeDB")

    assert result.returncode == 1
    assert "cannot be downgraded further" in result.stdout + result.stderr
    assert db_connection.execute(VERSION_QUERY).scalar_one() == baseline


def test_setup_downgrade_db_without_a_revision_stops(
    stamp: Callable[[str | None], None], db_connection: sa.Connection
) -> None:
    stamp(None)

    result = _setup("--downgradeDB")

    assert result.returncode == 1
    assert "setup --createDB" in result.stdout + result.stderr
    assert db_connection.execute(VERSION_QUERY).all() == []


def test_setup_upgrade_db_at_a_revision_before_the_baseline_stops(
    stamp: Callable[[str | None], None], db_connection: sa.Connection
) -> None:
    stamp(OLD_REVISION)

    result = _setup("--upgradeDB")

    assert result.returncode == 1
    assert "upgraded with DBImport 1.00.006" in result.stdout + result.stderr
    assert db_connection.execute(VERSION_QUERY).scalar_one() == OLD_REVISION


def test_setup_create_schema_rst_prints_the_database_documentation(
    config_db: None,
) -> None:
    result = _setup("--createSchemaRst")

    assert result.returncode == 0, result.stderr
    assert result.stdout == (REPO_ROOT / "docs" / "database.rst").read_text()


def test_setup_create_schema_rst_documents_the_configured_database(
    orm_db_connection: sa.Connection,
) -> None:
    orm_db_connection.execute(
        sa.text("ALTER TABLE jdbc_connections COMMENT = 'Connections of the copy'")
    )

    result = _setup("--createSchemaRst", DBIMPORT_DATABASE__MYSQL_DATABASE=ORM_DATABASE)

    assert result.returncode == 0, result.stderr
    assert "Connections of the copy" in result.stdout


@pytest.mark.parametrize(
    ("setting", "value", "message"),
    [
        (
            "DBIMPORT_DATABASE__MYSQL_PASSWORD",
            "wrong",
            "Something is wrong with your user name or password",
        ),
        (
            # The grant on DBImport_orm covers this name: "_" is a wildcard.
            "DBIMPORT_DATABASE__MYSQL_DATABASE",
            "DBImportXorm",
            "Database does not exist",
        ),
        (
            "DBIMPORT_DATABASE__MYSQL_DATABASE",
            "DBImport_missing",
            "to database 'DBImport_missing'",
        ),
        (
            "DBIMPORT_DATABASE__MYSQL_DATABASE",
            "x" * 65,
            "Incorrect database name",
        ),
        ("DBIMPORT_DATABASE__MYSQL_PORT", "1", "Can't connect"),
    ],
)
def test_setup_without_a_configuration_database_connection_stops(
    engine: sa.Engine, setting: str, value: str, message: str
) -> None:
    result = _setup("--createSchemaRst", **{setting: value})

    assert result.returncode == 1
    assert message in result.stdout
    assert "There was a problem connecting to the MySQL database" in result.stdout
    assert "Database Tables" not in result.stdout


def _add_times(conn: sa.Connection, times: list[datetime]) -> None:
    """Two failure log rows, two retry log rows an hour apart (their key
    includes the time) and one Atlas cache row with a create time."""
    *failures, first_retry, second_retry, created = times
    for table, failure in zip(("t", "far"), failures, strict=True):
        insert_row(
            conn,
            "import_failure_log",
            hive_db="db",
            hive_table=table,
            eventtime=failure,
        )
    for retry_time in (first_retry, second_retry):
        insert_row(
            conn,
            "import_retries_log",
            hive_db="db",
            hive_table="t",
            retry_time=retry_time,
            stage=1,
            stage_description="retry",
        )
    insert_row(
        conn,
        "atlas_column_cache",
        hostname="sqlhost",
        port="1433",
        database_name="sales",
        schema_name="dbo",
        table_name="orders",
        column_name="id",
        table_create_time=created,
    )


def _times(conn: sa.Connection) -> list[object]:
    return [
        *conn.execute(
            sa.text("SELECT eventtime FROM import_failure_log ORDER BY eventtime")
        ).scalars(),
        *conn.execute(
            sa.text("SELECT retry_time FROM import_retries_log ORDER BY retry_time")
        ).scalars(),
        *conn.execute(
            sa.text("SELECT table_create_time FROM atlas_column_cache")
        ).scalars(),
    ]


def test_setup_upgrade_db_converts_the_stored_times_to_utc(
    stamp: Callable[[str | None], None], db_connection: sa.Connection
) -> None:
    _add_times(db_connection, LOCAL_TIMES)
    stamp(BEFORE_UTC)
    head = ScriptDirectory.from_config(alembic_config()).get_current_head()

    result = _setup("--upgradeDB", "--localTimeZone=Europe/Stockholm")

    assert result.returncode == 0, result.stdout + result.stderr
    assert _times(db_connection) == UTC_TIMES
    assert db_connection.execute(VERSION_QUERY).scalar_one() == head


@pytest.mark.parametrize(
    ("options", "message"),
    [
        ((), "--localTimeZone=<zone>"),
        (("--localTimeZone=Nowhere/Missing",), "Unknown time zone: Nowhere/Missing"),
    ],
)
def test_setup_upgrade_db_of_stored_times_without_a_known_zone_stops(
    stamp: Callable[[str | None], None],
    db_connection: sa.Connection,
    options: tuple[str, ...],
    message: str,
) -> None:
    _add_times(db_connection, LOCAL_TIMES)
    stamp(BEFORE_UTC)

    result = _setup("--upgradeDB", *options)

    assert result.returncode == 1
    assert message in result.stdout
    assert _times(db_connection) == LOCAL_TIMES
    assert db_connection.execute(VERSION_QUERY).scalar_one() == BEFORE_UTC


def test_setup_downgrade_db_converts_the_stored_times_back(
    upgrade_after: None, db_connection: sa.Connection
) -> None:
    _add_times(db_connection, UTC_TIMES)

    result = _setup("--downgradeDB", "--localTimeZone=Europe/Stockholm")

    assert result.returncode == 0, result.stdout + result.stderr
    assert _times(db_connection) == LOCAL_TIMES
    assert db_connection.execute(VERSION_QUERY).scalar_one() == BEFORE_UTC


def test_setup_downgrade_db_of_two_times_that_become_one_stops_and_changes_nothing(
    upgrade_after: None, db_connection: sa.Connection
) -> None:
    # 00:30 and 01:30 UTC on 2026-10-25 are both 02:30 in Stockholm; the
    # retry log has the time in its key.
    times = [
        stored_time(text)
        for text in (
            "2026-06-01 10:00",
            "9999-12-31 23:59:59",
            "2026-10-25 00:30",
            "2026-10-25 01:30",
            "2026-06-01 12:00",
        )
    ]
    _add_times(db_connection, times)
    head = db_connection.execute(VERSION_QUERY).scalar_one()

    result = _setup("--downgradeDB", "--localTimeZone=Europe/Stockholm")

    assert result.returncode == 1
    assert "both become 2026-10-25 02:30:00" in result.stdout
    assert _times(db_connection) == times
    assert db_connection.execute(VERSION_QUERY).scalar_one() == head


def test_setup_upgrade_db_without_times_to_convert_needs_no_zone(
    stamp: Callable[[str | None], None], db_connection: sa.Connection
) -> None:
    insert_row(
        db_connection,
        "import_failure_log",
        hive_db="db",
        hive_table="far",
        eventtime=stored_time("9999-12-31 23:59:59"),
    )
    stamp(BEFORE_UTC)

    result = _setup("--upgradeDB")

    assert result.returncode == 0, result.stdout + result.stderr
