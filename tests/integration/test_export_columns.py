"""Columns of an exported table as an export reads and writes them
through export_config, and the import columns it reads to choose a
target column type. The Hive schema is given as the frame the Hive
reader returns: Hive is an external system."""

from collections.abc import Iterator
from datetime import UTC, datetime

import pandas as pd
import pytest
import sqlalchemy as sa

from dbimport.common.exceptions import InvalidConfigurationError
from dbimport.config_db import common_config, export_config
from tests.configdb import insert_row, set_driver_path

DBALIAS = "test_target"
SCHEMA = "dbo"
TABLE = "orders"
HIVE_DB = "testdb"
SYNCED = "2026-01-15 09:00:00"
STALE = "2025-12-01 09:00:00"
STARTED = datetime(2026, 2, 1, 10, 0, tzinfo=UTC)
MYSQL = ("jdbc:mysql://target.example:3306/db", "MySQL")
MSSQL = ("jdbc:sqlserver://target.example:1433;database=db", "SQL Server")


@pytest.fixture
def conn(
    clean_config_db: None, db_connection: sa.Connection
) -> Iterator[sa.Connection]:
    yield db_connection


def _add_target(conn: sa.Connection, target: tuple[str, str] = MYSQL) -> None:
    jdbc_url, driver_type = target
    set_driver_path(conn, driver_type)
    insert_row(conn, "jdbc_connections", dbalias=DBALIAS, jdbc_url=jdbc_url)
    common_config.Config().encrypt_user_password(DBALIAS, "user", "secret")


def _add_export(conn: sa.Connection, **values: object) -> int:
    insert_row(
        conn,
        "export_tables",
        **{
            "dbalias": DBALIAS,
            "target_schema": SCHEMA,
            "target_table": TABLE,
            "hive_db": HIVE_DB,
            "hive_table": TABLE,
            "last_update_from_hive": SYNCED,
            **values,
        },
    )
    table_id: int = conn.execute(
        sa.text("SELECT table_id FROM export_tables")
    ).scalar_one()
    return table_id


def _add_column(
    conn: sa.Connection, table_id: int, name: str, order: int, **values: object
) -> None:
    insert_row(
        conn,
        "export_columns",
        **{
            "table_id": table_id,
            "column_name": name,
            "column_type": "int",
            "column_order": order,
            "last_update_from_hive": SYNCED,
            **values,
        },
    )


def _config() -> export_config.Config:
    config = export_config.Config(DBALIAS, SCHEMA, TABLE)
    config.get_export_config()
    return config


def _hive_columns(*columns: tuple[str, str | None, str | None, int]) -> pd.DataFrame:
    return pd.DataFrame(columns, columns=["name", "type", "comment", "idx"])


def _stored_columns(conn: sa.Connection) -> list[tuple[object, ...]]:
    query = (
        "SELECT column_name, column_type, column_order, hive_db, hive_table, "
        "last_update_from_hive, comment, target_column_name, include_in_export "
        "FROM export_columns ORDER BY column_name"
    )
    return [tuple(row) for row in conn.execute(sa.text(query))]


def test_save_column_data_adds_the_hive_columns(conn: sa.Connection) -> None:
    _add_target(conn)
    _add_export(conn)
    config = _config()
    config.start_date = STARTED

    config.save_column_data(
        _hive_columns(
            ("id", "int", "The `key`; of it", 0),
            ("name", "varchar(20)", None, 1),
        )
    )

    started = datetime(2026, 2, 1, 10, 0)
    assert _stored_columns(conn) == [
        ("id", "int", 0, HIVE_DB, TABLE, started, "The key of it", None, 1),
        ("name", "varchar(20)", 1, HIVE_DB, TABLE, started, None, None, 1),
    ]


def test_save_column_data_updates_a_stored_column_and_keeps_its_settings(
    conn: sa.Connection,
) -> None:
    _add_target(conn)
    table_id = _add_export(conn)
    _add_column(
        conn,
        table_id,
        "id",
        3,
        hive_db="olddb",
        hive_table="oldtable",
        comment="old",
        target_column_name="ID",
        include_in_export=0,
    )
    _add_column(conn, table_id, "gone", 4, last_update_from_hive=STALE)
    column_id = conn.execute(
        sa.text("SELECT column_id FROM export_columns WHERE column_name = 'id'")
    ).scalar_one()
    config = _config()
    config.start_date = STARTED

    config.save_column_data(_hive_columns(("id", "bigint", "new", 0)))

    started = datetime(2026, 2, 1, 10, 0)
    assert _stored_columns(conn) == [
        ("gone", "int", 4, None, None, datetime(2025, 12, 1, 9, 0), None, None, 1),
        ("id", "bigint", 0, HIVE_DB, TABLE, started, "new", "ID", 0),
    ]
    assert (
        conn.execute(
            sa.text("SELECT column_id FROM export_columns WHERE column_name = 'id'")
        ).scalar_one()
        == column_id
    )


@pytest.mark.parametrize(
    ("columns", "needed"),
    [
        pytest.param([("id", {})], False, id="no-target-names"),
        pytest.param([("id", {"target_column_name": "ID"})], True, id="target-name"),
        pytest.param(
            [
                ("id", {}),
                ("gone", {"target_column_name": "G", "last_update_from_hive": STALE}),
            ],
            True,
            id="stale-column-with-target-name",
        ),
    ],
)
def test_is_export_temp_table_needed_when_a_column_has_a_target_name(
    conn: sa.Connection,
    columns: list[tuple[str, dict[str, object]]],
    needed: bool,
) -> None:
    _add_target(conn)
    table_id = _add_export(conn, export_tool="sqoop")
    for order, (name, values) in enumerate(columns):
        _add_column(conn, table_id, name, order, **values)
    config = _config()

    assert config.is_export_temp_table_needed(False, False) is needed


def _add_definition_columns(conn: sa.Connection) -> None:
    table_id = _add_export(conn)
    _add_column(conn, table_id, "b_name", 2, column_type="string", comment="Name")
    _add_column(
        conn,
        table_id,
        "a_id",
        1,
        target_column_name="Id",
        target_column_type="bigint",
    )
    _add_column(
        conn, table_id, "c_skip", 3, include_in_export=0, target_column_type="  "
    )
    _add_column(conn, table_id, "d_gone", 4, last_update_from_hive=STALE)


@pytest.mark.parametrize(
    ("options", "expected"),
    [
        pytest.param(
            {},
            [
                ("a_id", "Id", "bigint", None),
                ("b_name", None, "string", "Name"),
                ("c_skip", None, "int", None),
            ],
            id="default",
        ),
        pytest.param(
            {"exclude_columns": True},
            [("a_id", "Id", "bigint", None), ("b_name", None, "string", "Name")],
            id="excluded",
        ),
        pytest.param(
            {"force_column_uppercase": True},
            [
                ("A_ID", "ID", "bigint", None),
                ("B_NAME", None, "string", "Name"),
                ("C_SKIP", None, "int", None),
            ],
            id="uppercase",
        ),
        pytest.param(
            {"get_replaced_column_types": False},
            [
                ("a_id", "Id", "int", None),
                ("b_name", None, "string", "Name"),
                ("c_skip", None, "int", None),
            ],
            id="hive-types",
        ),
    ],
)
def test_get_columns_from_config_database_returns_the_current_columns_in_order(
    conn: sa.Connection,
    options: dict[str, bool],
    expected: list[tuple[object, ...]],
) -> None:
    _add_target(conn)
    _add_definition_columns(conn)
    config = _config()

    columns = config.get_columns_from_config_database(**options)

    assert list(columns.columns) == [
        "hiveColumnName",
        "targetColumnName",
        "columnType",
        "comment",
    ]
    assert [tuple(row) for row in columns.itertuples(index=False)] == expected


def _add_import(
    conn: sa.Connection,
    hive_table: str = TABLE,
    table_force_string: int = -1,
    **column: object,
) -> None:
    insert_row(
        conn,
        "import_tables",
        hive_db=HIVE_DB,
        hive_table=hive_table,
        dbalias=DBALIAS,
        source_schema="src",
        source_table=hive_table,
        force_string=table_force_string,
    )
    if column:
        table_id = conn.execute(
            sa.text("SELECT table_id FROM import_tables WHERE hive_table = :t"),
            {"t": hive_table},
        ).scalar_one()
        insert_row(
            conn,
            "import_columns",
            **{
                "table_id": table_id,
                "hive_db": HIVE_DB,
                "hive_table": hive_table,
                "column_name": "name",
                "source_column_name": "NAME",
                "column_type": "string",
                "last_update_from_source": SYNCED,
                **column,
            },
        )


@pytest.mark.parametrize(
    ("connection", "table", "column", "expected"),
    [
        pytest.param(1, 1, 0, "text", id="column-0"),
        pytest.param(0, 0, 1, "varchar(20)", id="column-1"),
        pytest.param(1, 0, -1, "text", id="table-0"),
        pytest.param(0, 1, -1, "varchar(20)", id="table-1"),
        pytest.param(0, -1, -1, "text", id="connection-0"),
        pytest.param(1, -1, -1, "varchar(20)", id="connection-1"),
        pytest.param(-1, -1, -1, "varchar(20)", id="none-set"),
    ],
)
def test_convert_column_type_of_a_string_follows_force_string(
    conn: sa.Connection, connection: int, table: int, column: int, expected: str
) -> None:
    _add_target(conn)
    conn.execute(
        sa.text("UPDATE jdbc_connections SET force_string = :f"), {"f": connection}
    )
    _add_export(conn)
    _add_import(
        conn,
        table_force_string=table,
        force_string=column,
        source_column_type="varchar(20)",
    )
    config = _config()

    assert config.convert_column_type_for_target_table("Name", "string") == (
        expected,
        None,
    )


def test_convert_column_type_of_a_binary_uses_the_source_type(
    conn: sa.Connection,
) -> None:
    _add_target(conn)
    _add_export(conn)
    _add_import(conn, column_type="binary", source_column_type="varbinary(16)")
    config = _config()

    assert config.convert_column_type_for_target_table("name", "binary") == (
        "varbinary(16)",
        None,
    )


def test_convert_column_type_uses_the_source_database_type(
    conn: sa.Connection,
) -> None:
    _add_target(conn, MSSQL)
    _add_export(conn)
    _add_import(
        conn,
        force_string=1,
        source_column_type="varchar2(10)",
        source_database_type="oracle",
    )
    config = _config()

    assert config.convert_column_type_for_target_table("name", "string") == (
        "varchar(10)",
        None,
    )


@pytest.mark.parametrize(
    ("imported", "expected"),
    [
        pytest.param(["orders"], "varchar(20)", id="import-without-history"),
        pytest.param(["orders_history"], "varchar(30)", id="import-with-history"),
        pytest.param(["orders", "orders_history"], "varchar(30)", id="both-imported"),
        pytest.param([], "text", id="not-imported"),
    ],
)
def test_convert_column_type_of_a_history_table_reads_the_imported_table(
    conn: sa.Connection, imported: list[str], expected: str
) -> None:
    _add_target(conn)
    _add_export(conn, hive_table="orders_history")
    source_types = {"orders": "varchar(20)", "orders_history": "varchar(30)"}
    for hive_table in imported:
        _add_import(
            conn,
            hive_table,
            force_string=1,
            source_column_type=source_types[hive_table],
        )
    config = _config()

    assert config.convert_column_type_for_target_table("name", "string") == (
        expected,
        None,
    )


def test_convert_column_type_of_a_table_that_was_not_imported_keeps_the_type(
    conn: sa.Connection,
) -> None:
    _add_target(conn)
    _add_export(conn)
    config = _config()

    assert config.convert_column_type_for_target_table("name", "string") == (
        "text",
        None,
    )


@pytest.mark.xfail(
    reason="Known bug: a string or binary column with no import column of "
    "the same name gets the type None",
    raises=TypeError,
    strict=True,
)
@pytest.mark.parametrize(
    ("connection", "hive_type"),
    [pytest.param(1, "string", id="string"), pytest.param(0, "binary", id="binary")],
)
def test_convert_column_type_of_a_column_without_import_column_returns_a_type(
    conn: sa.Connection, connection: int, hive_type: str
) -> None:
    _add_target(conn)
    conn.execute(
        sa.text("UPDATE jdbc_connections SET force_string = :f"), {"f": connection}
    )
    _add_export(conn)
    _add_import(conn)
    config = _config()

    column_type, _ = config.convert_column_type_for_target_table("extra", hive_type)

    assert column_type is not None


def test_save_column_data_before_get_export_config_raises_and_stores_nothing(
    conn: sa.Connection,
) -> None:
    _add_target(conn)
    _add_export(conn)
    config = export_config.Config(DBALIAS, SCHEMA, TABLE)

    with pytest.raises(InvalidConfigurationError, match="not been read"):
        config.save_column_data(_hive_columns(("id", "int", None, 0)))

    assert _stored_columns(conn) == []


def test_save_column_data_with_an_invalid_column_saves_no_column(
    conn: sa.Connection,
) -> None:
    _add_target(conn)
    _add_export(conn)
    config = _config()

    with pytest.raises(sa.exc.DataError):
        config.save_column_data(
            _hive_columns(("id", "int", None, 0), ("big", "x" * 2049, None, 1))
        )

    assert _stored_columns(conn) == []


def test_get_columns_from_config_database_without_current_columns_raises(
    conn: sa.Connection,
) -> None:
    _add_target(conn)
    table_id = _add_export(conn)
    _add_column(conn, table_id, "gone", 0, last_update_from_hive=STALE)
    config = _config()

    with pytest.raises(InvalidConfigurationError, match="no columns"):
        config.get_columns_from_config_database()


def test_get_incr_where_statement_without_the_incremental_column_raises(
    conn: sa.Connection,
) -> None:
    _add_target(conn)
    _add_export(conn, export_type="incr", incr_column="updated")
    config = _config()

    with pytest.raises(InvalidConfigurationError, match="'updated'"):
        config.get_incr_where_statement()


def test_save_column_data_saves_a_hive_column_without_type(
    conn: sa.Connection,
) -> None:
    _add_target(conn)
    _add_export(conn)
    config = _config()
    config.start_date = STARTED

    config.save_column_data(_hive_columns(("id", None, None, 0)))

    assert [row[:2] for row in _stored_columns(conn)] == [("id", None)]


def test_convert_column_type_of_a_binary_with_string_source_type_keeps_string(
    conn: sa.Connection,
) -> None:
    _add_target(conn)
    _add_export(conn)
    _add_import(conn, column_type="binary", source_column_type="string")
    config = _config()

    assert config.convert_column_type_for_target_table("name", "binary") == (
        "text",
        None,
    )
