from collections.abc import Iterator
from typing import Any

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.script import ScriptDirectory
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.schema import CreateColumn
from sqlalchemy.sql.compiler import DDLCompiler

from dbimport.config_db import config_schema
from tests.configdb import REPO_ROOT, alembic_config, schema_rows

SCHEMA_UPGRADE_DIR = REPO_ROOT / "src" / "dbimport" / "migrations"
# Second database of the compose server, created by tests/fixtures/mariadb-init.sql.
ORM_DATABASE = "DBImport_orm"


# sqlalchemy.ext.compiler and the DDL compiler are not annotated.
@compiles(CreateColumn, "mysql")  # type: ignore[no-untyped-call, untyped-decorator]
def _create_column(
    element: CreateColumn, compiler: DDLCompiler, **kw: Any
) -> str | None:
    """Add AUTO_INCREMENT to a column outside the primary key that sets
    info={"auto_increment": True} in config_schema; SQLAlchemy adds it only
    to a primary key column."""
    ddl: str | None = compiler.visit_create_column(element, **kw)  # type: ignore[no-untyped-call]
    if (
        ddl is not None
        and element.element.info.get("auto_increment")
        and "AUTO_INCREMENT" not in ddl
    ):
        ddl += " AUTO_INCREMENT"
    return ddl


@pytest.fixture
def orm_db_connection(engine: sa.Engine) -> Iterator[sa.Connection]:
    """Connection to the second test database, holding only the tables that
    the ORM creates. Uses the host the engine fixture has checked."""
    orm_engine = sa.create_engine(
        engine.url.set(database=ORM_DATABASE),
        connect_args={"connect_timeout": 10},
        isolation_level="AUTOCOMMIT",
    )
    try:
        conn = orm_engine.connect()
    except sa.exc.OperationalError as exc:
        orm_engine.dispose()
        pytest.fail(
            f"Cannot connect to database {ORM_DATABASE} ({exc.orig}). It is "
            "created when the compose server starts on an empty data "
            "directory: docker compose down, then docker compose up -d --wait."
        )
    try:
        with conn:
            conn.execute(sa.text("SET FOREIGN_KEY_CHECKS = 0"))
            for table in conn.execute(sa.text("SHOW TABLES")).scalars().all():
                conn.execute(sa.text(f"DROP TABLE `{table}`"))
            conn.execute(sa.text("SET FOREIGN_KEY_CHECKS = 1"))
            config_schema.Base.metadata.create_all(conn)
            yield conn
    finally:
        orm_engine.dispose()


def test_create_db_on_empty_database_reaches_alembic_head(
    clean_config_db: None, db_connection: sa.Connection
) -> None:
    expected_head = ScriptDirectory(str(SCHEMA_UPGRADE_DIR)).get_current_head()

    versions = list(
        db_connection.execute(
            sa.text("SELECT version_num FROM alembic_version")
        ).scalars()
    )

    assert versions == [expected_head]


def test_create_db_seeds_configuration_drivers_and_environments(
    clean_config_db: None, db_connection: sa.Connection
) -> None:
    def count(table: str) -> int:
        return int(
            db_connection.execute(sa.text(f"SELECT COUNT(*) FROM {table}")).scalar_one()
        )

    environments = set(
        db_connection.execute(
            sa.text("SELECT environment FROM jdbc_connections_environments")
        ).scalars()
    )

    assert count("configuration") == 74
    assert count("jdbc_connections_drivers") == 12
    assert environments == {
        "Production",
        "Integration Test",
        "System Test",
        "Development",
        "Sandbox",
    }


def test_alembic_check_against_migrated_schema_finds_no_difference(
    clean_config_db: None,
) -> None:
    command.check(alembic_config())


def test_orm_creates_the_migrated_schema(
    clean_config_db: None,
    db_connection: sa.Connection,
    orm_db_connection: sa.Connection,
) -> None:
    assert schema_rows(orm_db_connection) == schema_rows(db_connection)
