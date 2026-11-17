"""Helpers for tests that use the configuration database.

Importing this module does not import DBImport code, so it is safe before
pytest_configure has prepared the environment.
"""

from pathlib import Path

import sqlalchemy as sa
from alembic.config import Config

REPO_ROOT = Path(__file__).resolve().parents[1]


def close_dbimport_connections(obj: object) -> None:
    """Close the configuration database connections a DBImport object holds.

    A DBImport command runs in its own process, so its connections end
    with the process. Tests share one process: an open transaction left
    behind by one test would block the next test's TRUNCATE on a
    metadata lock.
    """
    conn = getattr(obj, "mysql_conn", None)
    if conn is not None:
        conn.close()
    engine = getattr(obj, "config_db", None)
    if engine is not None:
        engine.dispose()


def insert_row(conn: sa.Connection, table: str, **values: object) -> None:
    """Insert one row into table; the keyword names are the column names."""
    columns = ", ".join(values)
    params = ", ".join(f":{name}" for name in values)
    conn.execute(sa.text(f"INSERT INTO {table} ({columns}) VALUES ({params})"), values)


def set_driver_path(conn: sa.Connection, database_type: str) -> None:
    """Give the seeded default JDBC driver of database_type an absolute JAR path.

    An operator sets this after installation. DBImport only checks that the
    path is absolute; the file is not read.
    """
    conn.execute(
        sa.text(
            "UPDATE jdbc_connections_drivers SET classpath = :path "
            "WHERE database_type = :type AND version = 'default'"
        ),
        {"path": "/opt/jdbc/driver.jar", "type": database_type},
    )


# What a schema consists of, read from information_schema of the current
# database. Alembic's comparison leaves out server defaults, AUTO_INCREMENT,
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


def schema_rows(conn: sa.Connection) -> list[str]:
    """The schema of the connection's database as sorted lines, for
    comparing two schemas."""
    return sorted(
        f"{kind} {tuple(row)}"
        for kind, query in _SCHEMA_QUERIES.items()
        for row in conn.execute(sa.text(query))
    )


def alembic_config() -> Config:
    """The configuration of the Alembic command line (alembic.ini); the
    database comes from DBIMPORT_CONFIGFILE."""
    return Config(REPO_ROOT / "alembic.ini")
