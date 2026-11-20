"""The import_tables row as an import reads and writes it through
import_config. Calls to the source database are replaced: it is an external
system."""

from collections.abc import Iterator
from datetime import datetime

import pandas as pd
import pytest
import sqlalchemy as sa

from dbimport.common import constants
from dbimport.common.exceptions import InvalidConfigurationError
from dbimport.config_db import common_config, import_config
from tests.configdb import insert_row, set_driver_path

HIVE_DB = "testdb"
HIVE_TABLE = "testtable"
DBALIAS = "test_mysql"
INCREMENTAL = {
    "import_phase_type": "incr",
    "etl_phase_type": "merge",
    "incr_mode": "append",
    "incr_column": "id",
}
FLASHBACK = {
    "import_phase_type": "oracle_flashback",
    "etl_phase_type": "merge",
    "import_tool": "sqoop",
}


def _add_connection(conn: sa.Connection, **values: object) -> None:
    set_driver_path(conn, "MySQL")
    insert_row(
        conn,
        "jdbc_connections",
        dbalias=DBALIAS,
        jdbc_url="jdbc:mysql://source.example:3306/sourcedb",
        **values,
    )
    common_config.Config().encrypt_user_password(DBALIAS, "user", "secret")


def _add_table(conn: sa.Connection, **values: object) -> None:
    insert_row(
        conn,
        "import_tables",
        **{
            "hive_db": HIVE_DB,
            "hive_table": HIVE_TABLE,
            "dbalias": DBALIAS,
            "source_schema": "src",
            "source_table": "orders",
            **values,
        },
    )


@pytest.fixture
def table(
    clean_config_db: None, db_connection: sa.Connection
) -> Iterator[sa.Connection]:
    _add_connection(db_connection)
    yield db_connection


def _config(conn: sa.Connection, **values: object) -> import_config.Config:
    _add_table(conn, **values)
    config = import_config.Config(HIVE_DB, HIVE_TABLE)
    config.get_import_config()
    return config


def _common(config: import_config.Config) -> common_config.Config:
    assert config.common_config is not None
    return config.common_config


def _row(conn: sa.Connection, *columns: str) -> tuple[object, ...]:
    query = f"SELECT {', '.join(columns)} FROM import_tables"
    return tuple(conn.execute(sa.text(query)).one())


FULL_ROW = {
    "import_phase_type": "incr",
    "etl_phase_type": "merge",
    "import_tool": "spark",
    "etl_engine": "hive",
    "last_update_from_source": "2026-01-15 09:00:00",
    "sqoop_sql_where_addition": "a = 1",
    "nomerge_ingestion_sql_addition": "b = 2",
    "validate_import": 0,
    "validationMethod": "customQuery",
    "validate_source": "sqoop",
    "validate_diff_allowed": 7,
    "validationCustomQuerySourceSQL": "select 1",
    "validationCustomQueryHiveSQL": "select 2 from ${HIVE_DB}.${HIVE_TABLE}",
    "validationCustomQueryValidateImportTable": 0,
    "truncate_hive": 0,
    "mappers": 3,
    "soft_delete_during_merge": 1,
    "validationCustomQuerySourceValue": "[1]",
    "validationCustomQueryHiveValue": "[2]",
    "incr_mode": "lastmodified",
    "incr_column": "changed",
    "incr_validation_method": "incr",
    "incr_maxvalue": "2026-01-01 00:00:00",
    "hive_merge_heap": 2048,
    "hive_split_count": 5,
    "sqoop_query": "select x",
    "sqoop_options": "--opt",
    "sqoop_last_size": 1000,
    "sqoop_last_rows": 10,
    "sqoop_last_mappers": 2,
    "sqoop_last_execution": 1700000000,
    "sqoop_use_generated_sql": 1,
    "sqoop_allow_text_splitter": 1,
    "generated_sqoop_query": "select gen",
    "generated_sqoop_options": "--gen",
    "generated_pk_columns": "id",
    "datalake_source": "sales",
    "copy_slave": 0,
    "create_foreign_keys": 0,
    "split_by_column": "id",
    "spark_executor_memory": "2g",
    "spark_executors": 4,
    "custom_max_query": "select max(x)",
    "mergeCompactionMethod": "minor",
    "invalidate_impala": 1,
    "import_database": "imp_db",
    "import_table": "imp_tbl",
    "history_database": "hist_db",
    "history_table": "hist_tbl",
}


def test_get_import_config_reads_every_column_of_the_table_and_connection(
    clean_config_db: None, db_connection: sa.Connection
) -> None:
    _add_connection(
        db_connection,
        create_datalake_import=0,
        datalake_source="conn_src",
        create_foreign_keys=0,
    )

    config = _config(db_connection, **FULL_ROW)

    assert {name: getattr(config, name) for name in EXPECTED_ATTRIBUTES} == (
        EXPECTED_ATTRIBUTES
    )
    assert _common(config).operation_timestamp == datetime(2026, 1, 15, 9, 0)


EXPECTED_ATTRIBUTES = {
    "connection_alias": DBALIAS,
    "source_schema": "src",
    "source_table": "orders",
    "table_id": 1,
    "import_type": "",
    "sqoop_mappers": 3,
    "validate_import": False,
    "truncate_hive_table": False,
    "incr_mode": "lastmodified",
    "incr_column": "changed",
    "sqoop_use_generated_sql": True,
    "incr_maxvalue": "2026-01-01 00:00:00",
    "validate_diff_allowed": 7,
    "soft_delete_during_merge": True,
    "sqoop_last_size": 1000,
    "sqoop_last_rows": 10,
    "sqoop_allow_text_splitter": True,
    "datalake_source_table": "sales",
    "sqoop_last_execution": 1700000000,
    "hive_java_heap": 2048,
    "nomerge_ingestion_sql_addition": "b = 2",
    "sqoop_sql_where_addition": "a = 1",
    "sqoop_options": "--opt",
    "generated_sqoop_options": "--gen",
    "sql_generated_sqoop_query": "select gen",
    "generated_pk_columns": "id",
    "incr_validation_method": "incr",
    "sqoop_last_mappers": 2,
    "sqoop_query": "select x",
    "validate_source": "sqoop",
    "copy_slave": False,
    "import_phase_type": "incr",
    "etl_phase_type": "merge",
    "create_foreign_keys_table": 0,
    "import_tool": "spark",
    "spark_executor_memory": "2g",
    "split_by_column": "id",
    "custom_max_query": "select max(x)",
    "validation_method": "customQuery",
    "validation_custom_query_source_sql": "select 1",
    "validation_custom_query_hive_sql": "select 2 from ${HIVE_DB}.${HIVE_TABLE}",
    "validation_custom_query_validate_import_table": False,
    "validation_custom_query_source_value": "[1]",
    "validation_custom_query_hive_value": "[2]",
    "merge_compaction_method": "minor",
    "hive_split_count": 5,
    "etl_engine": "hive",
    "spark_max_executors": 4,
    "invalidate_impala_metadata": True,
    "import_database_override": "imp_db",
    "import_table_override": "imp_tbl",
    "history_database_override": "hist_db",
    "history_table_override": "hist_tbl",
    "create_datalake_import_column": False,
    "datalake_source_connection": "conn_src",
    "create_foreign_keys_connection": 0,
}


def test_update_last_update_from_source_stores_the_start_time_and_reads_it_back(
    table: sa.Connection,
) -> None:
    config = _config(table)

    config.update_last_update_from_source()

    (stored,) = _row(table, "last_update_from_source")
    assert stored == _common(config).operation_timestamp
    # The DATETIME column has no fractional seconds; MariaDB truncates them.
    assert config.start_date is not None
    start = datetime.strptime(config.start_date, "%Y-%m-%d %H:%M:%S.%f")
    assert stored == start.replace(microsecond=0)


INCR_COLUMNS = (
    "incr_minvalue",
    "incr_maxvalue",
    "incr_minvalue_pending",
    "incr_maxvalue_pending",
)


def test_save_incr_min_value_copies_the_max_value_to_the_pending_min_value(
    table: sa.Connection,
) -> None:
    config = _config(table, **INCREMENTAL, incr_minvalue="5", incr_maxvalue="10")

    config.save_incr_min_value()

    assert _row(table, *INCR_COLUMNS) == ("5", "10", "10", None)


def test_save_incr_pending_values_commits_and_clears_the_pending_values(
    table: sa.Connection,
) -> None:
    config = _config(
        table,
        **INCREMENTAL,
        incr_minvalue="5",
        incr_maxvalue="10",
        incr_minvalue_pending="10",
        incr_maxvalue_pending="20",
    )

    config.save_incr_pending_values()

    assert _row(table, *INCR_COLUMNS) == ("10", "20", None, None)


@pytest.mark.parametrize(
    "method", ["reset_incr_min_max_values", "reset_sqoop_statistics"]
)
def test_reset_of_incremental_values_keeps_only_the_given_max_value(
    table: sa.Connection, method: str
) -> None:
    config = _config(
        table,
        **INCREMENTAL,
        incr_minvalue="5",
        incr_maxvalue="10",
        incr_minvalue_pending="10",
        incr_maxvalue_pending="20",
    )

    getattr(config, method)("99")

    assert _row(table, *INCR_COLUMNS) == (None, "99", None, None)


VALIDATION_COLUMNS = (
    "source_rowcount",
    "source_rowcount_incr",
    "hive_rowcount",
    "validationCustomQuerySourceValue",
    "validationCustomQueryHiveValue",
)


def test_clear_validation_data_empties_the_validation_columns(
    table: sa.Connection,
) -> None:
    config = _config(
        table,
        source_rowcount=1,
        source_rowcount_incr=2,
        hive_rowcount=3,
        validationCustomQuerySourceValue="[1]",
        validationCustomQueryHiveValue="[2]",
    )

    config.clear_validation_data()

    assert _row(table, *VALIDATION_COLUMNS) == (None,) * 5


@pytest.mark.parametrize(
    ("method", "column"),
    [
        ("save_custom_sql_validation_source_value", "validationCustomQuerySourceValue"),
        ("save_custom_sql_validation_hive_value", "validationCustomQueryHiveValue"),
    ],
)
def test_save_custom_sql_validation_value_stores_the_json(
    table: sa.Connection, method: str, column: str
) -> None:
    config = _config(table)

    getattr(config, method)('[["a", 1]]')

    assert _row(table, column) == ('[["a", 1]]',)


@pytest.mark.parametrize(
    ("incr", "expected"), [(False, (42, None)), (True, (None, 42))]
)
def test_save_source_table_row_count_stores_full_or_incremental_count(
    table: sa.Connection, incr: bool, expected: tuple[int | None, int | None]
) -> None:
    config = _config(table)

    config.save_source_table_row_count(42, incr=incr)

    assert _row(table, "source_rowcount", "source_rowcount_incr") == expected


def test_save_source_table_row_count_with_custom_query_validation_stores_nothing(
    table: sa.Connection,
) -> None:
    config = _config(
        table,
        validationMethod="customQuery",
        validationCustomQueryHiveSQL="select 1 from ${HIVE_DB}.${HIVE_TABLE}",
    )

    config.save_source_table_row_count(42)

    assert _row(table, "source_rowcount") == (None,)


def test_save_hive_table_row_count_stores_the_count(table: sa.Connection) -> None:
    config = _config(table)

    config.save_hive_table_row_count(42)

    assert _row(table, "hive_rowcount") == (42,)


STATISTICS_COLUMNS = (
    "sqoop_last_execution",
    "sqoop_last_size",
    "sqoop_last_rows",
    "incr_maxvalue_pending",
    "sqoop_last_mappers",
    "source_rowcount",
    "source_rowcount_incr",
)


@pytest.mark.parametrize(
    ("values", "expected"),
    [
        ({}, (1700000000, 1000, 50, "99", 4, 7, 8)),
        (
            {"validate_source": "sqoop"},
            (1700000000, 1000, 50, "99", 4, 50, None),
        ),
        (
            {"validate_source": "sqoop", **INCREMENTAL},
            (1700000000, 1000, 50, "99", 4, None, 50),
        ),
    ],
    ids=["query", "sqoop-full", "sqoop-incremental"],
)
def test_save_import_statistics_stores_statistics_and_sqoop_row_counts(
    table: sa.Connection,
    values: dict[str, str],
    expected: tuple[object, ...],
) -> None:
    config = _config(table, source_rowcount=7, source_rowcount_incr=8, **values)

    config.save_import_statistics(
        1700000000,
        sqoop_size=1000,
        sqoop_rows=50,
        sqoop_incr_maxvalue_pending="99",
        sqoop_mappers=4,
    )

    assert _row(table, *STATISTICS_COLUMNS) == expected


def test_save_import_statistics_without_optional_values_keeps_them(
    table: sa.Connection,
) -> None:
    config = _config(table, sqoop_last_size=5, sqoop_last_rows=6)

    config.save_import_statistics(1700000000)

    assert _row(
        table, "sqoop_last_execution", "sqoop_last_size", "sqoop_last_rows"
    ) == (
        1700000000,
        5,
        6,
    )


GENERATED_COLUMNS = (
    "generated_hive_column_definition",
    "generated_sqoop_query",
    "comment",
    "generated_sqoop_options",
    "generated_pk_columns",
    "generated_foreign_keys",
    "sqoop_use_generated_sql",
    "sourceTableType",
)


@pytest.mark.parametrize(
    ("map_column_java", "pk_columns", "options", "stored_pk_columns"),
    [
        ([], "", None, None),
        (["amount=String"], "id", "--map-column-java amount=String", "id"),
    ],
)
def test_save_generated_data_stores_the_generated_definitions(
    table: sa.Connection,
    map_column_java: list[str],
    pk_columns: str,
    options: str | None,
    stored_pk_columns: str | None,
) -> None:
    config = _config(table, generated_foreign_keys="old")
    config.sqoop_mapcolumnjava = map_column_java
    config.generated_pk_columns = pk_columns
    config.sqoop_use_generated_sql = True
    config.sql_generated_hive_column_definition = "`id` int"
    config.sql_generated_sqoop_query = "select id from orders"
    config.table_comment = "orders"
    _common(config).table_type = "table"

    config.save_generated_data()

    assert _row(table, *GENERATED_COLUMNS) == (
        "`id` int",
        "select id from orders",
        "orders",
        options,
        stored_pk_columns,
        None,
        1,
        "table",
    )


PENDING_COLUMNS = ("incr_minvalue_pending", "incr_maxvalue_pending")


def test_get_incr_where_statement_for_sqoop_stores_the_pending_range(
    table: sa.Connection,
) -> None:
    config = _config(table, **INCREMENTAL, incr_maxvalue="100")

    config.get_incr_where_statement(where_for_sqoop=True, incremental_max="500")

    assert _row(table, *PENDING_COLUMNS) == ("100", "500")


def test_get_incr_where_statement_reads_the_pending_max_value(
    table: sa.Connection,
) -> None:
    config = _config(table, **INCREMENTAL, incr_maxvalue="100")
    table.execute(sa.text("UPDATE import_tables SET incr_maxvalue_pending = '300'"))

    config.get_incr_where_statement()

    assert config.sqoop_incr_maxvalue_pending == "300"


@pytest.mark.parametrize(
    ("pending", "expected"),
    [((None, None), ("1000", "1000")), (("10", "20"), ("10", "20"))],
)
def test_get_incr_where_statement_for_flashback_reads_the_pending_or_stored_range(
    table: sa.Connection,
    pending: tuple[str | None, str | None],
    expected: tuple[str, str],
) -> None:
    config = _config(
        table,
        **FLASHBACK,
        incr_minvalue="1000",
        incr_maxvalue="1000",
        incr_minvalue_pending=pending[0],
        incr_maxvalue_pending=pending[1],
    )

    config.get_incr_where_statement()

    assert (
        config.sqoop_incr_minvalue_pending,
        config.sqoop_incr_maxvalue_pending,
    ) == expected


def test_get_incr_where_statement_for_flashback_sqoop_stores_the_scn_range(
    table: sa.Connection, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _config(table, **FLASHBACK, incr_maxvalue="1000")
    monkeypatch.setattr(
        _common(config),
        "execute_jdbc_query",
        lambda query: pd.DataFrame({"CURRENT_SCN": [2000]}),
    )

    config.get_incr_where_statement(where_for_sqoop=True)

    assert _row(table, *PENDING_COLUMNS) == ("1000", "2000")


@pytest.mark.parametrize(
    ("values", "counts", "expected"),
    [
        ({}, [42], (42, None)),
        (INCREMENTAL, [42, 7], (42, 7)),
        ({"validate_source": "sqoop"}, [], (None, None)),
    ],
    ids=["full", "incremental", "sqoop-validation"],
)
def test_get_jdbc_table_row_count_stores_the_source_row_counts(
    table: sa.Connection,
    monkeypatch: pytest.MonkeyPatch,
    values: dict[str, str],
    counts: list[int],
    expected: tuple[int | None, int | None],
) -> None:
    config = _config(table, **values, incr_maxvalue="100")
    remaining = iter(counts)
    monkeypatch.setattr(
        _common(config),
        "get_jdbc_table_row_count",
        lambda schema, table, where: next(remaining),
    )

    config.get_jdbc_table_row_count()

    assert _row(table, "source_rowcount", "source_rowcount_incr") == expected


def test_get_hive_table_comment_returns_the_stored_comment(
    table: sa.Connection,
) -> None:
    config = _config(table, comment="orders of the shop")

    assert config.get_hive_table_comment() == "orders of the shop"


@pytest.mark.parametrize(
    ("values", "kwargs", "expected"),
    [
        ({"source_rowcount": 100, "hive_rowcount": 100}, {}, True),
        ({"source_rowcount": 100, "hive_rowcount": 105}, {}, True),
        ({"source_rowcount": 100, "hive_rowcount": 106}, {}, False),
        (
            {"source_rowcount": 100, "hive_rowcount": 101, "validate_diff_allowed": 0},
            {},
            False,
        ),
        ({"source_rowcount": 100}, {}, True),
        ({"hive_rowcount": 100}, {}, False),
        (
            {"source_rowcount_incr": 10, "hive_rowcount": 10, **INCREMENTAL},
            {"incremental": True},
            True,
        ),
        (
            {"source_rowcount": 10, "sqoop_last_rows": 10},
            {"validate_sqoop": True},
            True,
        ),
        (
            {"source_rowcount": 10, "sqoop_last_rows": 30},
            {"validate_sqoop": True},
            False,
        ),
    ],
    ids=[
        "equal",
        "within-default-diff",
        "outside-default-diff",
        "outside-zero-diff",
        "no-target-count",
        "no-source-count",
        "incremental",
        "sqoop-equal",
        "sqoop-different",
    ],
)
def test_validate_row_count_compares_the_stored_row_counts(
    table: sa.Connection,
    values: dict[str, object],
    kwargs: dict[str, bool],
    expected: bool,
) -> None:
    config = _config(table, **values)

    assert config.validate_row_count(**kwargs) is expected


@pytest.mark.parametrize(
    ("pk_for_merge", "expected"), [(False, "id,name"), (True, "id")]
)
def test_get_pk_columns_returns_the_normalised_override(
    table: sa.Connection, pk_for_merge: bool, expected: str
) -> None:
    config = _config(
        table, pk_column_override=" ID , Name ", pk_column_override_mergeonly="Id"
    )

    assert config.get_pk_columns(pk_for_merge=pk_for_merge) == expected


def test_get_import_tables_lists_the_tables_of_one_database(
    table: sa.Connection,
) -> None:
    _add_table(table)
    _add_table(table, hive_table="customers", source_table="customers")
    _add_table(table, hive_db="otherdb")
    config = import_config.Config(HIVE_DB, HIVE_TABLE)

    tables = config.get_import_tables(HIVE_DB)

    assert sorted(tables["hive_table"]) == ["customers", HIVE_TABLE]
    assert list(tables.columns) == [
        "hive_db",
        "hive_table",
        "dbalias",
        "source_schema",
        "source_table",
    ]


def test_get_import_tables_of_unknown_database_returns_an_empty_frame(
    table: sa.Connection,
) -> None:
    config = import_config.Config(HIVE_DB, HIVE_TABLE)

    tables = config.get_import_tables("nodb")

    assert tables.empty
    assert list(tables.columns) == [
        "hive_db",
        "hive_table",
        "dbalias",
        "source_schema",
        "source_table",
    ]


@pytest.mark.parametrize(
    ("catalog", "etl_engine"),
    [(constants.CATALOG_HIVE_DIRECT, "hive"), (constants.CATALOG_GLUE, "spark")],
)
def test_add_import_table_stores_the_table_with_the_catalog_engine(
    table: sa.Connection, catalog: str, etl_engine: str
) -> None:
    config = import_config.Config(HIVE_DB, HIVE_TABLE)

    added = config.add_import_table(
        HIVE_DB, HIVE_TABLE, DBALIAS, "src", "orders", catalog
    )

    assert added is True
    assert _row(
        table,
        "hive_db",
        "hive_table",
        "dbalias",
        "source_schema",
        "source_table",
        "etl_engine",
    ) == (HIVE_DB, HIVE_TABLE, DBALIAS, "src", "orders", etl_engine)


def test_add_import_table_that_exists_returns_false(table: sa.Connection) -> None:
    _add_table(table)
    config = import_config.Config(HIVE_DB, HIVE_TABLE)

    added = config.add_import_table(
        HIVE_DB, HIVE_TABLE, DBALIAS, "src", "orders", constants.CATALOG_HIVE_DIRECT
    )

    assert added is False


def test_add_import_table_with_unknown_catalog_raises(table: sa.Connection) -> None:
    config = import_config.Config(HIVE_DB, HIVE_TABLE)

    with pytest.raises(InvalidConfigurationError):
        config.add_import_table(HIVE_DB, HIVE_TABLE, DBALIAS, "src", "orders", "x")


def test_get_import_config_with_value_outside_an_enum_raises(
    table: sa.Connection,
) -> None:
    _add_table(table)
    # Outside strict mode, MariaDB stores an invalid ENUM value as ''.
    table.execute(
        sa.text(
            "SET STATEMENT sql_mode = '' FOR UPDATE import_tables SET import_tool = 'x'"
        )
    )

    with pytest.raises(InvalidConfigurationError):
        import_config.Config(HIVE_DB, HIVE_TABLE).get_import_config()


def test_write_before_get_import_config_raises_and_stores_nothing(
    table: sa.Connection,
) -> None:
    _add_table(table)
    config = import_config.Config(HIVE_DB, HIVE_TABLE)

    with pytest.raises(InvalidConfigurationError):
        config.save_hive_table_row_count(42)
    assert _row(table, "hive_rowcount") == (None,)


def test_reading_the_row_again_after_it_was_deleted_raises(
    table: sa.Connection,
) -> None:
    config = _config(table)
    table.execute(sa.text("DELETE FROM import_tables"))

    with pytest.raises(InvalidConfigurationError):
        config.get_hive_table_comment()


def test_get_import_config_with_custom_query_validation_without_hive_query_raises(
    table: sa.Connection,
) -> None:
    with pytest.raises(InvalidConfigurationError):
        _config(table, validationMethod="customQuery")
