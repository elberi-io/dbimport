"""Fixtures for tests against the MariaDB configuration database.

The database comes from compose.yaml (`docker compose up -d --wait`). The
fixtures delete and recreate all tables in it, so they refuse to run
against anything but a local test database.
"""

import configparser
import os
from collections.abc import Iterator
from pathlib import Path

import pytest
import sqlalchemy as sa
from tests.configdb import close_dbimport_connections

_INTEGRATION_DIR = Path(__file__).parent
# Hosts the fixtures may reset. DBIMPORT_TEST_DB_HOST adds one explicitly.
_LOCAL_HOSTS = {"127.0.0.1", "localhost"}
_TEST_DATABASE = "DBImport"


@pytest.hookimpl(tryfirst=True)
def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    # Runs before -m deselects, so every test in this directory is
    # excluded by `-m "not integration"`, also without a pytestmark line.
    for item in items:
        if item.path.is_relative_to(_INTEGRATION_DIR):
            item.add_marker(pytest.mark.integration)


def _engine() -> sa.Engine:
    parser = configparser.ConfigParser()
    parser.read(os.environ["DBIMPORT_CONFIGFILE"])
    db = parser["Database"]
    allowed = _LOCAL_HOSTS | {os.environ.get("DBIMPORT_TEST_DB_HOST", "")}
    if db["mysql_hostname"] not in allowed or db["mysql_database"] != _TEST_DATABASE:
        raise RuntimeError(
            "Refusing to reset a database that is not the local test database: "
            f"{db['mysql_hostname']}/{db['mysql_database']}"
        )
    url = sa.URL.create(
        "mysql+pymysql",
        username=db["mysql_username"],
        password=db["mysql_password"],
        host=db["mysql_hostname"],
        port=int(db["mysql_port"]),
        database=db["mysql_database"],
    )
    # A lock left behind by DBImport code then fails the fixture after 30 s
    # instead of waiting for the server default of one day.
    connect_args = {
        "connect_timeout": 10,
        "init_command": "SET SESSION lock_wait_timeout = 30",
    }
    return sa.create_engine(
        url, connect_args=connect_args, isolation_level="AUTOCOMMIT"
    )


def _tables(conn: sa.Connection) -> list[str]:
    query = sa.text("SHOW FULL TABLES WHERE Table_type = 'BASE TABLE'")
    return list(conn.execute(query).scalars())


@pytest.fixture(scope="session")
def engine() -> Iterator[sa.Engine]:
    eng = _engine()
    try:
        yield eng
    finally:
        eng.dispose()


@pytest.fixture(scope="session")
def config_db(engine: sa.Engine) -> None:
    """Create the full configuration schema once per test session.

    Drops all tables, then runs the `setup --createDB` code: Alembic
    upgrade to head plus seed data.
    """
    from setupOperation import database

    with engine.connect() as conn:
        conn.execute(sa.text("SET FOREIGN_KEY_CHECKS = 0"))
        for table in _tables(conn):
            conn.execute(sa.text(f"DROP TABLE `{table}`"))
        conn.execute(sa.text("SET FOREIGN_KEY_CHECKS = 1"))
    setup = database.initialize()
    try:
        setup.createDB()
    finally:
        close_dbimport_connections(setup)


@pytest.fixture
def clean_config_db(config_db: None, engine: sa.Engine) -> None:
    """Reset all data to the seed state before the test.

    Empties every table except alembic_version, then runs the same seeding
    as `setup --createDB`.
    """
    from setupOperation import database

    with engine.connect() as conn:
        conn.execute(sa.text("SET FOREIGN_KEY_CHECKS = 0"))
        for table in _tables(conn):
            if table != "alembic_version":
                conn.execute(sa.text(f"TRUNCATE TABLE `{table}`"))
        conn.execute(sa.text("SET FOREIGN_KEY_CHECKS = 1"))
    setup = database.initialize()
    try:
        setup.createDefaultEnvironmentTypes()
        setup.updateConfigurationValues()
    finally:
        close_dbimport_connections(setup)


@pytest.fixture
def db_connection(config_db: None, engine: sa.Engine) -> Iterator[sa.Connection]:
    """Autocommit connection to the test configuration database."""
    with engine.connect() as conn:
        yield conn
