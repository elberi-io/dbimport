"""JDBC drivers and connection environments: their seeding by setup and the
driver lookup of the command line programs."""

import pytest
import sqlalchemy as sa

from dbimport.common.exceptions import InvalidConfigurationError
from dbimport.config_db import common_config
from tests.configdb import close_dbimport_connections, set_driver_path

DRIVERS = sa.text(
    "SELECT database_type, version, driver, classpath FROM jdbc_connections_drivers"
)
ENVIRONMENTS = sa.text("SELECT environment FROM jdbc_connections_environments")


def _setup(method: str) -> None:
    from dbimport.setup import database

    setup = database.Initialize()
    try:
        getattr(setup, method)()
    finally:
        close_dbimport_connections(setup)


def test_get_jdbc_driver_config_with_absolute_classpath_returns_it(
    clean_config_db: None, db_connection: sa.Connection
) -> None:
    set_driver_path(db_connection, "PostgreSQL")

    driver = common_config.Config().get_jdbc_driver_config("PostgreSQL", "default")

    assert driver == ("org.postgresql.Driver", "/opt/jdbc/driver.jar")


@pytest.mark.parametrize(
    ("database_type", "version", "classpath"),
    [
        ("PostgreSQL", "default", None),
        ("PostgreSQL", "default", "lib/driver.jar"),
        ("PostgreSQL", "no_such_version", None),
    ],
)
def test_get_jdbc_driver_config_without_usable_driver_raises(
    clean_config_db: None,
    db_connection: sa.Connection,
    database_type: str,
    version: str,
    classpath: str | None,
) -> None:
    if classpath is not None:
        db_connection.execute(
            sa.text(
                "UPDATE jdbc_connections_drivers SET classpath = :path "
                "WHERE database_type = 'PostgreSQL'"
            ),
            {"path": classpath},
        )

    with pytest.raises(InvalidConfigurationError):
        common_config.Config().get_jdbc_driver_config(database_type, version)


def test_seed_adds_missing_database_types_and_keeps_existing_rows(
    clean_config_db: None, db_connection: sa.Connection
) -> None:
    set_driver_path(db_connection, "Oracle")
    db_connection.execute(
        sa.text(
            "DELETE FROM jdbc_connections_drivers "
            "WHERE database_type IN ('MySQL', 'SQL Server')"
        )
    )
    before = set(db_connection.execute(DRIVERS))

    _setup("update_configuration_values")

    added = set(db_connection.execute(DRIVERS)) - before
    assert added == {
        ("MySQL", "default", "com.mysql.jdbc.Driver", "add path to JAR file"),
        (
            "SQL Server",
            "default",
            "com.microsoft.sqlserver.jdbc.SQLServerDriver",
            "add path to JAR file",
        ),
        (
            "SQL Server",
            "jTDS",
            "net.sourceforge.jtds.jdbc.Driver",
            "add path to JAR file",
        ),
    }
    assert (
        "Oracle",
        "default",
        "oracle.jdbc.driver.OracleDriver",
        "/opt/jdbc/driver.jar",
    ) in before


def test_seed_does_not_add_a_version_of_a_database_type_that_has_a_row(
    clean_config_db: None, db_connection: sa.Connection
) -> None:
    db_connection.execute(
        sa.text(
            "DELETE FROM jdbc_connections_drivers "
            "WHERE database_type = 'SQL Server' AND version = 'jTDS'"
        )
    )

    _setup("update_configuration_values")

    versions = db_connection.execute(
        sa.text(
            "SELECT version FROM jdbc_connections_drivers "
            "WHERE database_type = 'SQL Server'"
        )
    ).scalars()
    assert list(versions) == ["default"]


@pytest.mark.parametrize(
    ("existing", "expected"),
    [
        (
            [],
            {"Production", "Integration Test", "System Test", "Development", "Sandbox"},
        ),
        (["Staging"], {"Staging"}),
    ],
)
def test_seed_environments_only_into_an_empty_table(
    clean_config_db: None,
    db_connection: sa.Connection,
    existing: list[str],
    expected: set[str],
) -> None:
    db_connection.execute(sa.text("DELETE FROM jdbc_connections_environments"))
    for environment in existing:
        db_connection.execute(
            sa.text("INSERT INTO jdbc_connections_environments VALUES (:env, NULL)"),
            {"env": environment},
        )

    _setup("create_default_environment_types")

    assert set(db_connection.execute(ENVIRONMENTS).scalars()) == expected
