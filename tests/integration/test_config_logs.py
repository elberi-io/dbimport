"""The logs common_config writes: import failures, column changes of Hive
and JDBC tables, statistics documents that could not be sent, and Yarn
applications."""

from collections.abc import Iterator
from datetime import datetime
from typing import Any

import pytest
import sqlalchemy as sa

from dbimport.config_db import common_config
from tests.configdb import close_dbimport_connections

HIVE_DB = "testdb"
HIVE_TABLE = "testtable"
EARLIER = datetime(2000, 1, 1)


@pytest.fixture
def config(clean_config_db: None) -> Iterator[common_config.Config]:
    config = common_config.Config()
    config.set_hive_table(HIVE_DB, HIVE_TABLE)
    yield config
    close_dbimport_connections(config)


def _rows(conn: sa.Connection, table: str, *columns: str) -> list[tuple[Any, ...]]:
    return [
        tuple(row)
        for row in conn.execute(sa.text(f"SELECT {', '.join(columns)} FROM {table}"))
    ]


def test_log_import_failure_stores_the_failure_of_the_current_table(
    config: common_config.Config, db_connection: sa.Connection
) -> None:
    config.log_import_failure("lock removed", "Warning", import_type="full")

    [(hive_db, hive_table, eventtime, severity, import_type, error_text)] = _rows(
        db_connection,
        "import_failure_log",
        "hive_db",
        "hive_table",
        "eventtime",
        "severity",
        "import_type",
        "error_text",
    )
    assert (hive_db, hive_table, severity, import_type, error_text) == (
        HIVE_DB,
        HIVE_TABLE,
        "Warning",
        "full",
        "lock removed",
    )
    assert eventtime > EARLIER


def test_log_import_failure_of_another_table_stores_that_table(
    config: common_config.Config, db_connection: sa.Connection
) -> None:
    config.log_import_failure(
        "2 locks removed by force", "Warning", hive_db="otherdb", hive_table="other"
    )

    assert _rows(
        db_connection, "import_failure_log", "hive_db", "hive_table", "import_type"
    ) == [("otherdb", "other", None)]


HISTORY_COLUMNS = (
    "hive_db",
    "hive_table",
    "column_name",
    "event",
    "previous_value",
    "value",
    "description",
)


@pytest.mark.parametrize(
    ("method", "kwargs", "expected"),
    [
        (
            "log_hive_column_add",
            {"column": "id", "column_type": "int"},
            (
                "id",
                "column_added",
                None,
                "int",
                "Column 'id' added to table with type 'int'",
            ),
        ),
        (
            "log_hive_column_add",
            {"column": "id", "description": "Added by hand"},
            ("id", "column_added", None, None, "Added by hand"),
        ),
        (
            "log_hive_column_type_change",
            {"column": "id", "column_type": "bigint"},
            (
                "id",
                "column_type_change",
                None,
                "bigint",
                "Column 'id' type changed to bigint",
            ),
        ),
        (
            "log_hive_column_type_change",
            {"column": "id", "column_type": "bigint", "previous_column_type": "int"},
            (
                "id",
                "column_type_change",
                "int",
                "bigint",
                "Column 'id' type changed from int to bigint",
            ),
        ),
        (
            "log_hive_column_rename",
            {"column_name": "new_id", "previous_column_name": "id"},
            (
                "new_id",
                "column_rename",
                "id",
                "new_id",
                "Column 'id' renamed to new_id",
            ),
        ),
    ],
    ids=[
        "add",
        "add-with-description",
        "type-change",
        "type-change-from",
        "rename",
    ],
)
def test_log_hive_column_change_stores_the_event(
    config: common_config.Config,
    db_connection: sa.Connection,
    method: str,
    kwargs: dict[str, str],
    expected: tuple[str | None, ...],
) -> None:
    getattr(config, method)(**kwargs)

    assert _rows(db_connection, "table_change_history", *HISTORY_COLUMNS) == [
        (HIVE_DB, HIVE_TABLE, *expected)
    ]
    [(eventtime,)] = _rows(db_connection, "table_change_history", "eventtime")
    assert eventtime > EARLIER


def test_log_hive_column_change_of_another_table_stores_that_table(
    config: common_config.Config, db_connection: sa.Connection
) -> None:
    config.log_hive_column_add("id", "int", hive_db="otherdb", hive_table="other")

    assert _rows(db_connection, "table_change_history", "hive_db", "hive_table") == [
        ("otherdb", "other")
    ]


JDBC_TABLE = {
    "db_alias": "test_mysql",
    "database": "targetdb",
    "schema": "dbo",
    "table": "orders",
}
JDBC_COLUMNS = (
    "dbalias",
    "db_name",
    "schema_name",
    "table_name",
    "column_name",
    "event",
    "previous_value",
    "value",
    "description",
)


@pytest.mark.parametrize(
    ("method", "kwargs", "expected"),
    [
        (
            "log_jdbc_column_add",
            {"column": "id", "column_type": "int"},
            (
                "id",
                "column_added",
                None,
                "int",
                "Column 'id' added to table with type 'int'",
            ),
        ),
        (
            "log_jdbc_column_type_change",
            {"column": "id", "column_type": "bigint"},
            (
                "id",
                "column_type_change",
                None,
                "bigint",
                "Column 'id' type changed to bigint",
            ),
        ),
        (
            "log_jdbc_column_type_change",
            {"column": "id", "column_type": "bigint", "previous_column_type": "int"},
            (
                "id",
                "column_type_change",
                "int",
                "bigint",
                "Column 'id' type changed from int to bigint",
            ),
        ),
        (
            "log_jdbc_column_rename",
            {
                "column_name": "new_id",
                "previous_column_name": "id",
                "description": "Renamed by hand",
            },
            ("new_id", "column_rename", "id", "new_id", "Renamed by hand"),
        ),
    ],
    ids=["add", "type-change", "type-change-from", "rename-with-description"],
)
def test_log_jdbc_column_change_stores_the_event(
    config: common_config.Config,
    db_connection: sa.Connection,
    method: str,
    kwargs: dict[str, str],
    expected: tuple[str | None, ...],
) -> None:
    getattr(config, method)(**kwargs, **JDBC_TABLE)

    assert _rows(db_connection, "jdbc_table_change_history", *JDBC_COLUMNS) == [
        ("test_mysql", "targetdb", "dbo", "orders", *expected)
    ]
    [(eventtime,)] = _rows(db_connection, "jdbc_table_change_history", "eventtime")
    assert eventtime > EARLIER


def test_save_json_to_database_stores_the_document_to_send(
    config: common_config.Config, db_connection: sa.Connection
) -> None:
    config.save_json_to_database("import_statistics", "kafka", '{"rows": 10}')

    [(datatype, status, destination, jsondata, create_time)] = _rows(
        db_connection,
        "json_to_send",
        "type",
        "status",
        "destination",
        "jsondata",
        "create_time",
    )
    assert (datatype, status, destination, jsondata) == (
        "import_statistics",
        0,
        "kafka",
        '{"rows": 10}',
    )
    assert create_time > EARLIER


YARN_COLUMNS = (
    "yarn_application_id",
    "operation_type",
    "operation_tool",
    "operation_timestamp",
    "hive_db",
    "hive_table",
    "dbalias",
    "target_schema",
    "target_table",
    "yarn_containers_total",
    "yarn_containers_failed",
    "yarn_containers_killed",
)


def test_update_yarn_statistics_of_an_import_stores_the_application(
    config: common_config.Config, db_connection: sa.Connection
) -> None:
    config.operation_type = "import"
    config.operation_timestamp = EARLIER

    config.update_yarn_statistics("application_1_0001", "spark")

    assert _rows(db_connection, "yarn_statistics", *YARN_COLUMNS) == [
        (
            "application_1_0001",
            "import",
            "spark",
            EARLIER,
            HIVE_DB,
            HIVE_TABLE,
            None,
            None,
            None,
            None,
            None,
            None,
        )
    ]
    [(start, stop)] = _rows(
        db_connection, "yarn_statistics", "application_start", "application_stop"
    )
    assert start > EARLIER and stop is None


def test_update_yarn_statistics_of_an_export_stores_the_application(
    config: common_config.Config, db_connection: sa.Connection
) -> None:
    config.operation_type = "export"
    config.operation_timestamp = EARLIER
    config.db_alias = "test_mysql"
    config.target_schema = "dbo"
    config.target_table = "orders"

    config.update_yarn_statistics("application_1_0001", "sqoop")

    assert _rows(db_connection, "yarn_statistics", *YARN_COLUMNS) == [
        (
            "application_1_0001",
            "export",
            "sqoop",
            EARLIER,
            None,
            None,
            "test_mysql",
            "dbo",
            "orders",
            None,
            None,
            None,
        )
    ]


def test_update_yarn_statistics_with_containers_finishes_the_application(
    config: common_config.Config, db_connection: sa.Connection
) -> None:
    config.operation_type = "import"
    config.operation_timestamp = EARLIER
    config.update_yarn_statistics("application_1_0001", "spark")

    config.update_yarn_statistics(
        "application_1_0001",
        "spark",
        yarn_containers_total=5,
        yarn_containers_failed=1,
        yarn_containers_killed=0,
    )

    [(start, stop, total, failed, killed)] = _rows(
        db_connection,
        "yarn_statistics",
        "application_start",
        "application_stop",
        "yarn_containers_total",
        "yarn_containers_failed",
        "yarn_containers_killed",
    )
    assert (total, failed, killed) == (5, 1, 0)
    assert stop >= start


def test_update_yarn_statistics_with_containers_before_the_start_does_nothing(
    config: common_config.Config, db_connection: sa.Connection
) -> None:
    config.operation_type = "import"
    config.operation_timestamp = EARLIER

    config.update_yarn_statistics(
        "application_1_0001", "spark", yarn_containers_total=5
    )

    assert _rows(db_connection, "yarn_statistics", "yarn_application_id") == []
