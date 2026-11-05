"""Helpers for tests that use the configuration database.

Importing this module does not import DBImport code, so it is safe before
pytest_configure has prepared the environment.
"""

import sqlalchemy as sa


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
    engine = getattr(obj, "configDB", None)
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
