import os
import subprocess
import sys
from collections.abc import Callable, Iterator

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.script import ScriptDirectory

from tests.configdb import ORM_DATABASE, REPO_ROOT, alembic_config, schema_rows

SETUP = REPO_ROOT / "bin" / "setup"
# A revision of the migration chain before the baseline (0.80.008).
OLD_REVISION = "af71422a72a2"
VERSION_QUERY = sa.text("SELECT version_num FROM alembic_version")


def _setup(option: str, **settings: str) -> subprocess.CompletedProcess[str]:
    """Runs bin/setup; settings override the test configuration, as
    DBIMPORT_<SECTION>__<KEY>=value environment variables."""
    return subprocess.run(
        [sys.executable, str(SETUP), option],
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
    after it, because the schema is built once per session."""
    try:
        yield
    finally:
        command.upgrade(alembic_config(), "head")


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

    downgrade = _setup("--downgradeDB")
    downgraded = schema_rows(db_connection)
    upgrade = _setup("--upgradeDB")

    assert (downgrade.returncode, upgrade.returncode) == (0, 0)
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
