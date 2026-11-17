from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
import sqlalchemy as sa
from alembic.autogenerate import compare_metadata
from alembic.migration import MigrationContext
from alembic.script import ScriptDirectory
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.schema import CreateColumn
from sqlalchemy.sql.compiler import DDLCompiler

from dbimport.config_db import config_schema

REPO_ROOT = Path(__file__).resolve().parents[2]
SCHEMA_UPGRADE_DIR = REPO_ROOT / "src" / "dbimport" / "migrations"
# Second database of the compose server, created by tests/fixtures/mariadb-init.sql.
ORM_DATABASE = "DBImport_orm"

# What a schema consists of, read from information_schema of the current
# database. compare_metadata() leaves out server defaults, AUTO_INCREMENT,
# constraint names and column order; these queries do not.
_SCHEMA_QUERIES = {
    "table": """
        SELECT TABLE_NAME, TABLE_TYPE, ENGINE, TABLE_COLLATION, TABLE_COMMENT
        FROM information_schema.TABLES
        WHERE TABLE_SCHEMA = DATABASE()""",
    "column": """
        SELECT TABLE_NAME, ORDINAL_POSITION, COLUMN_NAME, COLUMN_TYPE,
            IS_NULLABLE, COLUMN_DEFAULT, EXTRA, COLUMN_COMMENT,
            CHARACTER_SET_NAME, COLLATION_NAME
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()""",
    "index": """
        SELECT TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX, COLUMN_NAME, NON_UNIQUE,
            SUB_PART, INDEX_TYPE, INDEX_COMMENT
        FROM information_schema.STATISTICS
        WHERE TABLE_SCHEMA = DATABASE()""",
    "foreign key": """
        SELECT k.TABLE_NAME, k.CONSTRAINT_NAME, k.ORDINAL_POSITION,
            k.COLUMN_NAME, k.REFERENCED_TABLE_NAME, k.REFERENCED_COLUMN_NAME,
            r.UPDATE_RULE, r.DELETE_RULE
        FROM information_schema.KEY_COLUMN_USAGE k
        JOIN information_schema.REFERENTIAL_CONSTRAINTS r
            ON r.CONSTRAINT_SCHEMA = k.CONSTRAINT_SCHEMA
            AND r.TABLE_NAME = k.TABLE_NAME
            AND r.CONSTRAINT_NAME = k.CONSTRAINT_NAME
        WHERE k.TABLE_SCHEMA = DATABASE()""",
    "check": """
        SELECT TABLE_NAME, CONSTRAINT_NAME, CHECK_CLAUSE
        FROM information_schema.CHECK_CONSTRAINTS
        WHERE CONSTRAINT_SCHEMA = DATABASE()""",
    "trigger": """
        SELECT EVENT_OBJECT_TABLE, TRIGGER_NAME, ACTION_TIMING,
            EVENT_MANIPULATION, ACTION_STATEMENT
        FROM information_schema.TRIGGERS
        WHERE TRIGGER_SCHEMA = DATABASE()""",
}


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


def _describe(diff: Any) -> list[str]:
    """One stable line per difference reported by compare_metadata()."""
    if isinstance(diff, list):
        # modify_type, modify_nullable, modify_comment, modify_default
        return [f"{op[0]} {op[2]}.{op[3]}" for op in diff]
    kind = diff[0]
    if kind in ("add_table", "remove_table"):
        return [f"{kind} {diff[1].name}"]
    if kind in ("add_column", "remove_column"):
        return [f"{kind} {diff[2]}.{diff[3].name}"]
    if kind in ("add_index", "remove_index"):
        return [f"{kind} {diff[1].table.name}.{diff[1].name}"]
    return [f"{kind} {diff[1]!r}"]


def _schema(conn: sa.Connection) -> list[str]:
    return sorted(
        f"{kind} {tuple(row)}"
        for kind, query in _SCHEMA_QUERIES.items()
        for row in conn.execute(sa.text(query))
    )


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


def test_orm_compared_by_alembic_equals_migrated_schema(
    clean_config_db: None, db_connection: sa.Connection
) -> None:
    context = MigrationContext.configure(
        db_connection, opts={"compare_server_default": True}
    )
    diffs = compare_metadata(context, config_schema.Base.metadata)

    actual = sorted(line for diff in diffs for line in _describe(diff))

    assert actual == []


def test_orm_creates_the_migrated_schema(
    clean_config_db: None,
    db_connection: sa.Connection,
    orm_db_connection: sa.Connection,
) -> None:
    assert _schema(orm_db_connection) == _schema(db_connection)
