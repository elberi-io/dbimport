"""Columns, keys and indexes of an imported table as an import reads and
writes them through import_config. The source schema is given as the
frames the schema reader returns: the source database is an external
system."""

from collections.abc import Iterator
from datetime import datetime

import pandas as pd
import pytest
import sqlalchemy as sa

from dbimport.common.exceptions import InvalidConfigurationError
from dbimport.config_db import common_config, import_config
from tests.configdb import insert_row, set_driver_path

HIVE_DB = "testdb"
HIVE_TABLE = "testtable"
DBALIAS = "test_mysql"
SYNCED = "2026-01-15 09:00:00"
STALE = "2025-12-01 09:00:00"


@pytest.fixture
def conn(
    clean_config_db: None, db_connection: sa.Connection
) -> Iterator[sa.Connection]:
    set_driver_path(db_connection, "MySQL")
    insert_row(
        db_connection,
        "jdbc_connections",
        dbalias=DBALIAS,
        jdbc_url="jdbc:mysql://source.example:3306/sourcedb",
    )
    common_config.Config().encrypt_user_password(DBALIAS, "user", "secret")
    yield db_connection


def _add_table(
    conn: sa.Connection, hive_table: str = HIVE_TABLE, /, **values: object
) -> int:
    insert_row(
        conn,
        "import_tables",
        **{
            "hive_db": HIVE_DB,
            "hive_table": hive_table,
            "dbalias": DBALIAS,
            "source_schema": "src",
            "source_table": hive_table,
            "etl_engine": "hive",
            "last_update_from_source": SYNCED,
            **values,
        },
    )
    table_id: int = conn.execute(
        sa.text("SELECT table_id FROM import_tables WHERE hive_table = :t"),
        {"t": hive_table},
    ).scalar_one()
    return table_id


def _add_column(
    conn: sa.Connection,
    table_id: int,
    source_name: str,
    order: int,
    **values: object,
) -> int:
    insert_row(
        conn,
        "import_columns",
        **{
            "table_id": table_id,
            "column_name": source_name.lower(),
            "source_column_name": source_name,
            "column_order": order,
            "column_type": "int",
            "source_column_type": "int(11)",
            "last_update_from_source": SYNCED,
            **values,
        },
    )
    column_id: int = conn.execute(
        sa.text(
            "SELECT column_id FROM import_columns"
            " WHERE table_id = :t AND source_column_name = :c"
        ),
        {"t": table_id, "c": source_name},
    ).scalar_one()
    return column_id


def _config(conn: sa.Connection, **values: object) -> import_config.Config:
    _add_table(conn, **values)
    config = import_config.Config(HIVE_DB, HIVE_TABLE)
    config.get_import_config()
    return config


def _table_id(config: import_config.Config) -> int:
    assert config.table_id is not None
    return config.table_id


def _common(config: import_config.Config) -> common_config.Config:
    assert config.common_config is not None
    return config.common_config


def _rows(conn: sa.Connection, query: str) -> list[tuple[object, ...]]:
    return [tuple(row) for row in conn.execute(sa.text(query))]


def _source_columns(*columns: tuple[str, str, str | None]) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "SOURCE_COLUMN_NAME": name,
                "SOURCE_COLUMN_TYPE": column_type,
                "SOURCE_COLUMN_COMMENT": comment,
                "IS_NULLABLE": "YES",
                "SCHEMA_NAME": "src",
                "TABLE_NAME": HIVE_TABLE,
                "TABLE_COMMENT": "the orders",
            }
            for name, column_type, comment in columns
        ]
    )


def _source_keys(*keys: tuple[str, str, str, str, str, int]) -> pd.DataFrame:
    """keys: constraint name and type, column, referenced table and
    column, key position."""
    return pd.DataFrame(
        [
            {
                "SCHEMA_NAME": "src",
                "TABLE_NAME": HIVE_TABLE,
                "CONSTRAINT_NAME": name,
                "CONSTRAINT_TYPE": key_type,
                "COL_NAME": column,
                "COL_DATA_TYPE": "int",
                "REFERENCE_SCHEMA_NAME": "src",
                "REFERENCE_TABLE_NAME": ref_table,
                "REFERENCE_COL_NAME": ref_column,
                "COL_KEY_POSITION": position,
            }
            for name, key_type, column, ref_table, ref_column, position in keys
        ]
    )


COLUMN_FIELDS = (
    "SELECT column_name, column_order, source_column_name, column_type,"
    " source_column_type, source_database_type, sqoop_column_type, comment"
    " FROM import_columns ORDER BY column_order"
)


def test_save_column_data_inserts_the_source_columns(conn: sa.Connection) -> None:
    config = _config(conn)
    _common(config).source_columns_df = _source_columns(
        ("ID", "int(11)", "the key"),
        ("Created", "datetime", None),
        ("Note", "varchar(50)", "it's free text"),
    )

    config.save_column_data()

    assert _rows(conn, COLUMN_FIELDS) == [
        ("id", 1, "ID", "int", "int(11)", "mysql", None, "the key"),
        ("created", 2, "Created", "timestamp", "datetime", "mysql", "String", None),
        (
            "note",
            3,
            "Note",
            "varchar(50)",
            "varchar(50)",
            "mysql",
            None,
            "its free text",
        ),
    ]
    stored = conn.execute(
        sa.text("SELECT DISTINCT last_update_from_source FROM import_columns")
    ).scalar_one()
    assert config.start_date is not None
    assert stored == datetime.fromisoformat(config.start_date).replace(microsecond=0)
    assert config.sql_generated_hive_column_definition == (
        "( `ID` int COMMENT 'the key', `Created` timestamp,"
        " `Note` varchar(50) COMMENT 'its free text' )"
    )
    assert config.sql_generated_sqoop_query == (
        "select `ID` as `id`, `Created` as `created`, `Note` as `note` from testtable"
    )


def test_save_column_data_updates_known_columns_with_their_settings(
    conn: sa.Connection,
) -> None:
    config = _config(conn)
    table_id = _table_id(config)
    key_id = _add_column(
        conn,
        table_id,
        "ID",
        5,
        source_primary_key=1,
        column_name_override="key_id",
        column_type_override="bigint",
        last_update_from_source=STALE,
    )
    _add_column(conn, table_id, "Secret", 6, anonymization_function="Hash")
    _add_column(conn, table_id, "Skip", 7, include_in_import=0)
    _common(config).source_columns_df = _source_columns(
        ("ID", "int(11)", None),
        ("Secret", "varchar(20)", None),
        ("Skip", "int(11)", None),
    )

    config.save_column_data()

    assert _rows(
        conn,
        "SELECT column_id, column_name, column_order, column_type,"
        " source_primary_key FROM import_columns ORDER BY column_order",
    ) == [
        (key_id, "key_id", 1, "bigint", None),
        (key_id + 1, "secret", 2, "string", None),
        (key_id + 2, "skip", 3, "int", None),
    ]
    assert config.sql_generated_hive_column_definition == (
        "( `key_id` bigint, `Secret` string )"
    )
    assert config.sql_generated_sqoop_query == (
        "select `ID` as `key_id`, `Secret` as `secret` from testtable"
    )


def test_save_column_data_with_a_name_reserved_in_sqoop_saves_and_raises(
    conn: sa.Connection,
) -> None:
    config = _config(conn)
    _common(config).source_columns_df = _source_columns(("long", "int(11)", None))

    with pytest.raises(InvalidConfigurationError):
        config.save_column_data()

    assert _rows(conn, "SELECT source_column_name FROM import_columns") == [("long",)]


@pytest.mark.parametrize(
    ("connection", "table", "column", "expected"),
    [
        (0, -1, -1, "varchar(50)"),
        (1, -1, -1, "string"),
        (1, 0, -1, "varchar(50)"),
        (0, 1, -1, "string"),
        (0, 1, 0, "varchar(50)"),
        (0, 0, 1, "string"),
    ],
)
def test_save_column_data_forces_char_columns_to_string_by_the_closest_setting(
    conn: sa.Connection, connection: int, table: int, column: int, expected: str
) -> None:
    conn.execute(
        sa.text("UPDATE jdbc_connections SET force_string = :v"), {"v": connection}
    )
    config = _config(conn, force_string=table)
    _add_column(conn, _table_id(config), "Note", 1, force_string=column)
    _common(config).source_columns_df = _source_columns(("Note", "varchar(50)", None))

    config.save_column_data()

    assert _rows(conn, "SELECT column_type FROM import_columns") == [(expected,)]


def test_set_primary_key_column_stores_the_key_positions(conn: sa.Connection) -> None:
    config = _config(conn)
    table_id = _table_id(config)
    for order, name in enumerate(["ID", "Line", "Ref"], start=1):
        _add_column(conn, table_id, name, order)
    _common(config).source_keys_df = _source_keys(
        ("PRIMARY", "P", "Line", "", "", 2),
        ("PRIMARY", "P", "ID", "", "", 1),
        ("fk_ref", "F", "Ref", "other", "id", 1),
    )

    config.set_primary_key_column()

    assert _rows(
        conn,
        "SELECT source_column_name, source_primary_key FROM import_columns"
        " ORDER BY column_order",
    ) == [("ID", 1), ("Line", 2), ("Ref", None)]
    assert config.generated_pk_columns == "Line,ID"


@pytest.mark.parametrize("name", ["date", "interval"])
def test_set_primary_key_column_with_a_name_reserved_in_hive_stores_its_position(
    conn: sa.Connection, name: str
) -> None:
    config = _config(conn)
    _add_column(conn, _table_id(config), name, 1)
    _common(config).source_keys_df = _source_keys(("PRIMARY", "P", name, "", "", 1))

    config.set_primary_key_column()

    assert _rows(conn, "SELECT source_primary_key FROM import_columns") == [(1,)]
    assert config.generated_pk_columns == f"{name}_HIVE"


@pytest.mark.parametrize(
    ("convert_to_lower", "expected"), [(True, "id,line"), (False, "ID,Line")]
)
def test_get_pk_columns_returns_the_current_key_columns_in_key_order(
    conn: sa.Connection, convert_to_lower: bool, expected: str
) -> None:
    config = _config(conn)
    table_id = _table_id(config)
    _add_column(conn, table_id, "Line", 1, source_primary_key=2)
    _add_column(conn, table_id, "ID", 2, source_primary_key=1)
    _add_column(
        conn, table_id, "Old", 3, source_primary_key=3, last_update_from_source=STALE
    )
    _add_column(conn, table_id, "Amount", 4)

    assert config.get_pk_columns(convert_to_lower=convert_to_lower) == expected


def test_get_pk_columns_without_key_columns_returns_none(conn: sa.Connection) -> None:
    config = _config(conn)
    _add_column(conn, _table_id(config), "Amount", 1)

    assert config.get_pk_columns() is None


INDEX_FIELDS = (
    "SELECT index_name, index_type, index_unique, `column`, column_type,"
    " column_order, column_is_nullable FROM import_tables_indexes"
    " ORDER BY index_name, column_order"
)


def _source_indexes(*indexes: tuple[str, str, int]) -> pd.DataFrame:
    """indexes: name, column, column order."""
    return pd.DataFrame(
        [
            {
                "Name": name,
                "Type": "BTREE",
                "Unique": "Unique",
                "Column": column,
                "ColumnOrder": order,
                "IsNullable": 0,
            }
            for name, column, order in indexes
        ]
    )


def test_save_index_data_replaces_the_stored_indexes(conn: sa.Connection) -> None:
    config = _config(conn)
    conn.execute(
        sa.text(
            "INSERT INTO import_tables_indexes (table_id, hive_db, hive_table,"
            " index_name, index_type, index_unique, `column`, column_type,"
            " column_order, column_is_nullable) VALUES (:t, 'testdb', 'testtable',"
            " 'idx_old', 'BTREE', 'Unique', 'ID', 'int(11)', 1, 0)"
        ),
        {"t": _table_id(config)},
    )
    _common(config).source_columns_df = _source_columns(
        ("ID", "int(11)", None), ("Line", "smallint(6)", None)
    )
    _common(config).source_index_df = _source_indexes(
        ("idx_pk", "Line", 2),
        ("idx_pk", "ID", 1),
        ("idx_gone", "Missing", 1),
    )

    config.save_index_data()

    assert _rows(conn, INDEX_FIELDS) == [
        ("idx_pk", "BTREE", "Unique", "ID", "int(11)", 1, 0),
        ("idx_pk", "BTREE", "Unique", "Line", "smallint(6)", 2, 0),
    ]


def test_save_index_data_skips_a_duplicate_index_column(conn: sa.Connection) -> None:
    config = _config(conn)
    _common(config).source_columns_df = _source_columns(
        ("ID", "int(11)", None), ("Line", "smallint(6)", None)
    )
    _common(config).source_index_df = _source_indexes(
        ("idx_a", "ID", 1), ("idx_a", "ID", 1), ("idx_b", "Line", 1)
    )

    config.save_index_data()

    assert _rows(conn, "SELECT index_name, `column` FROM import_tables_indexes") == [
        ("idx_a", "ID"),
        ("idx_b", "Line"),
    ]


def _referenced_table(conn: sa.Connection) -> tuple[int, int, int]:
    """The imported table "customers" (MySQL has no schemas: "-"), with
    columns id and region."""
    table_id = _add_table(conn, "customers", source_schema="-")
    id_column = _add_column(
        conn, table_id, "id", 1, hive_db=HIVE_DB, hive_table="customers"
    )
    region = _add_column(
        conn, table_id, "region", 2, hive_db=HIVE_DB, hive_table="customers"
    )
    return table_id, id_column, region


FK_FIELDS = (
    "SELECT table_id, column_id, fk_index, fk_table_id, fk_column_id,"
    " key_position FROM import_foreign_keys ORDER BY fk_index, key_position"
)


def test_save_key_data_stores_the_foreign_keys_to_imported_tables(
    conn: sa.Connection,
) -> None:
    ref_table, ref_id, ref_region = _referenced_table(conn)
    config = _config(conn)
    table_id = _table_id(config)
    customer = _add_column(conn, table_id, "CustomerId", 1)
    region = _add_column(conn, table_id, "Region", 2)
    _common(config).source_keys_df = _source_keys(
        ("PRIMARY", "P", "CustomerId", "", "", 1),
        ("fk_region", "F", "Region", "customers", "region", 2),
        ("fk_region", "F", "CustomerId", "customers", "id", 1),
        ("fk_customer", "F", "CustomerId", "customers", "id", 1),
    )

    config.save_key_data()

    assert _rows(conn, FK_FIELDS) == [
        (table_id, customer, 1, ref_table, ref_id, 1),
        (table_id, region, 1, ref_table, ref_region, 2),
        (table_id, customer, 2, ref_table, ref_id, 1),
    ]


@pytest.mark.parametrize(
    ("ref_table", "ref_column"),
    [("unknown", "id"), ("customers", "missing")],
    ids=["table-not-imported", "column-not-imported"],
)
def test_save_key_data_skips_a_reference_that_is_not_imported(
    conn: sa.Connection, ref_table: str, ref_column: str
) -> None:
    _referenced_table(conn)
    config = _config(conn)
    _add_column(conn, _table_id(config), "CustomerId", 1)
    _common(config).source_keys_df = _source_keys(
        ("fk_customer", "F", "CustomerId", ref_table, ref_column, 1),
    )

    config.save_key_data()

    assert _rows(conn, FK_FIELDS) == []


def _add_foreign_key(
    conn: sa.Connection,
    table_id: int,
    column_id: int,
    ref: tuple[int, int],
    fk_index: int = 1,
    position: int = 1,
) -> None:
    insert_row(
        conn,
        "import_foreign_keys",
        table_id=table_id,
        column_id=column_id,
        fk_index=fk_index,
        fk_table_id=ref[0],
        fk_column_id=ref[1],
        key_position=position,
    )


def test_remove_fk_for_table_removes_only_the_tables_foreign_keys(
    conn: sa.Connection,
) -> None:
    ref_table, ref_id, ref_region = _referenced_table(conn)
    config = _config(conn)
    table_id = _table_id(config)
    customer = _add_column(conn, table_id, "CustomerId", 1)
    _add_foreign_key(conn, table_id, customer, (ref_table, ref_id))
    _add_foreign_key(conn, ref_table, ref_region, (table_id, customer))

    config.remove_fk_for_table()

    assert _rows(conn, "SELECT table_id FROM import_foreign_keys") == [(ref_table,)]


def test_get_foreign_keys_from_config_returns_one_row_per_foreign_key(
    conn: sa.Connection,
) -> None:
    ref_table, ref_id, ref_region = _referenced_table(conn)
    config = _config(conn)
    table_id = _table_id(config)
    customer = _add_column(
        conn, table_id, "CustomerId", 1, hive_db=HIVE_DB, hive_table=HIVE_TABLE
    )
    region = _add_column(
        conn, table_id, "Region", 2, hive_db=HIVE_DB, hive_table=HIVE_TABLE
    )
    _add_foreign_key(conn, table_id, region, (ref_table, ref_region), 1, 2)
    _add_foreign_key(conn, table_id, customer, (ref_table, ref_id), 1, 1)
    _add_foreign_key(conn, table_id, customer, (ref_table, ref_id), 2, 1)

    result = config.get_foreign_keys_from_config()

    assert result.to_dict("records") == [
        {
            "fk_name": "testdb__testtable__fk__customers__customerid",
            "source_hive_db": HIVE_DB,
            "source_hive_table": HIVE_TABLE,
            "ref_hive_db": HIVE_DB,
            "ref_hive_table": "customers",
            "source_column_name": "customerid",
            "ref_column_name": "id",
        },
        {
            "fk_name": "testdb__testtable__fk__customers__customerid_region",
            "source_hive_db": HIVE_DB,
            "source_hive_table": HIVE_TABLE,
            "ref_hive_db": HIVE_DB,
            "ref_hive_table": "customers",
            "source_column_name": "customerid,region",
            "ref_column_name": "id,region",
        },
    ]


def test_get_foreign_keys_from_config_without_keys_returns_an_empty_frame(
    conn: sa.Connection,
) -> None:
    config = _config(conn)

    result = config.get_foreign_keys_from_config()

    assert result.empty
    assert list(result.columns) == [
        "fk_name",
        "source_hive_db",
        "source_hive_table",
        "ref_hive_db",
        "ref_hive_table",
        "source_column_name",
        "ref_column_name",
    ]


def test_get_anonymization_function_returns_the_anonymized_columns(
    conn: sa.Connection,
) -> None:
    config = _config(conn)
    table_id = _table_id(config)
    _add_column(conn, table_id, "ID", 1)
    _add_column(conn, table_id, "Mail", 2, anonymization_function="Hash")
    _add_column(conn, table_id, "Phone", 3, anonymization_function="Show first 4 chars")

    result = config.get_anonymization_function()

    assert result.to_dict("records") == [
        {"source_column_name": "Mail", "anonymization_function": "Hash"},
        {"source_column_name": "Phone", "anonymization_function": "Show first 4 chars"},
    ]
    assert config.is_any_column_anonymized()


def test_is_any_column_anonymized_without_functions_is_false(
    conn: sa.Connection,
) -> None:
    config = _config(conn)
    _add_column(conn, _table_id(config), "ID", 1)

    assert config.get_anonymization_function().empty
    assert not config.is_any_column_anonymized()


def _current_columns(conn: sa.Connection, table_id: int) -> None:
    """ID, Name (Hive name full_name, comment), Skip (not imported) and a
    column from an earlier schema read."""
    _add_column(
        conn,
        table_id,
        "Name",
        2,
        column_name="full_name",
        column_type="string",
        comment="the name",
    )
    _add_column(conn, table_id, "ID", 1)
    _add_column(conn, table_id, "Skip", 3, include_in_import=0)
    _add_column(conn, table_id, "Gone", 4, last_update_from_source=STALE)


@pytest.mark.parametrize(
    ("alias", "expected"),
    [
        (None, "select `ID` as `id`, `Name` as `full_name` from testtable"),
        (
            "T",
            "select `T`.`ID` as `id`, `T`.`Name` as `full_name` from testtable as T",
        ),
    ],
)
def test_get_sql_to_read_from_source_selects_the_imported_columns(
    conn: sa.Connection, alias: str | None, expected: str
) -> None:
    config = _config(conn)
    _current_columns(conn, _table_id(config))

    assert config.get_sql_to_read_from_source(source_table_as_name=alias) == expected


@pytest.mark.parametrize(
    ("include_all_columns", "restrict_columns", "expected"),
    [
        (
            True,
            None,
            [
                ("id", "int", None),
                ("full_name", "string", "the name"),
                ("skip", "int", None),
            ],
        ),
        (
            False,
            None,
            [("id", "int", None), ("full_name", "string", "the name")],
        ),
        (True, "skip,id", [("id", "int", None), ("skip", "int", None)]),
    ],
    ids=["all", "imported", "restricted"],
)
def test_get_columns_from_config_database_returns_the_current_columns(
    conn: sa.Connection,
    include_all_columns: bool,
    restrict_columns: str | None,
    expected: list[tuple[object, ...]],
) -> None:
    config = _config(conn)
    _current_columns(conn, _table_id(config))

    result = config.get_columns_from_config_database(
        restrict_columns=restrict_columns, include_all_columns=include_all_columns
    )

    assert list(result.columns) == ["name", "type", "comment"]
    assert [tuple(row) for row in result.itertuples(index=False)] == expected


def test_get_columns_from_config_database_prefers_the_name_override_and_parquet_names(
    conn: sa.Connection,
) -> None:
    config = _config(conn)
    _add_column(
        conn,
        _table_id(config),
        "Order Date",
        1,
        column_name="order date",
        column_name_override="Day (UTC)",
    )

    result = config.get_columns_from_config_database(source_is_parquet_file=True)

    assert list(result["name"]) == ["day__utc_"]


@pytest.mark.parametrize(
    "method", ["get_sql_to_read_from_source", "get_columns_from_config_database"]
)
def test_reading_the_columns_without_current_columns_raises(
    conn: sa.Connection, method: str
) -> None:
    config = _config(conn)
    _add_column(conn, _table_id(config), "Gone", 1, last_update_from_source=STALE)

    with pytest.raises(
        InvalidConfigurationError, match="no columns from the last read"
    ):
        getattr(config, method)()


def test_save_key_data_for_a_column_that_is_not_stored_raises(
    conn: sa.Connection,
) -> None:
    _referenced_table(conn)
    config = _config(conn)
    _common(config).source_keys_df = _source_keys(
        ("fk_customer", "F", "CustomerId", "customers", "id", 1),
    )

    with pytest.raises(InvalidConfigurationError, match="CustomerId"):
        config.save_key_data()

    assert _rows(conn, FK_FIELDS) == []


def test_save_index_data_without_source_indexes_removes_the_stored_ones(
    conn: sa.Connection,
) -> None:
    config = _config(conn)
    conn.execute(
        sa.text(
            "INSERT INTO import_tables_indexes (table_id, hive_db, hive_table,"
            " index_name, index_type, index_unique, `column`, column_type,"
            " column_order, column_is_nullable) VALUES (:t, 'testdb', 'testtable',"
            " 'idx_old', 'BTREE', 'Unique', 'ID', 'int(11)', 1, 0)"
        ),
        {"t": _table_id(config)},
    )
    _common(config).source_index_df = pd.DataFrame()

    config.save_index_data()

    assert _rows(conn, INDEX_FIELDS) == []


def test_get_select_for_import_view_casts_columns_with_a_sqoop_type(
    conn: sa.Connection,
) -> None:
    config = _config(conn)
    table_id = _table_id(config)
    _add_column(conn, table_id, "ID", 1)
    _add_column(
        conn,
        table_id,
        "Created",
        2,
        column_type="timestamp",
        sqoop_column_type="String",
    )
    _add_column(conn, table_id, "Date", 3, column_type="date", sqoop_column_type=" ")
    _add_column(
        conn,
        table_id,
        "Amount",
        4,
        column_name_override="Total Sum",
        sqoop_column_type_override="Double",
        column_type="double",
    )
    _add_column(conn, table_id, "Gone", 5, last_update_from_source=STALE)

    assert config.get_select_for_import_view() == (
        "select S.`id`, cast(S.`created` as timestamp) `created`, S.`date_hive`,"
        " cast(S.`total_sum` as double) `total_sum` from `etl_import_staging`.`testdb__testtable__staging` as S "
    )


def test_get_select_for_import_view_for_flashback_keeps_the_latest_row_per_key(
    conn: sa.Connection,
) -> None:
    config = _config(
        conn,
        import_phase_type="oracle_flashback",
        etl_phase_type="merge",
        import_tool="sqoop",
    )
    _add_column(conn, _table_id(config), "ID", 1, source_primary_key=1)

    assert config.get_select_for_import_view() == (
        "select S.`id`, S.`datalake_flashback_operation`,"
        " cast(S.`datalake_flashback_startscn` as bigint) datalake_flashback_startscn"
        " from `etl_import_staging`.`testdb__testtable__staging` as S inner join ("
        "   select id, max(datalake_flashback_startscn) as max_startscn "
        "   from `etl_import_staging`.`testdb__testtable__staging`    group by id) G on    S.id = G.id"
        "   and ( S.datalake_flashback_startscn = G.max_startscn"
        " or G.max_startscn is NULL )"
    )
