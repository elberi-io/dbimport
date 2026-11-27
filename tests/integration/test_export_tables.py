"""The export_tables row as an export reads and writes it through
export_config. Calls to the target database are replaced: it is an
external system."""

from collections.abc import Iterator
from datetime import UTC, datetime

import pytest
import sqlalchemy as sa

from dbimport.common import constants
from dbimport.common.exceptions import InvalidConfigurationError
from dbimport.config_db import common_config, export_config
from tests.configdb import insert_row, set_driver_path

DBALIAS = "test_target"
SCHEMA = "dbo"
TABLE = "orders"
INCR_COLUMNS = (
    "incr_minvalue",
    "incr_maxvalue",
    "incr_minvalue_pending",
    "incr_maxvalue_pending",
)
INCREMENTAL = {"export_type": "incr", "incr_column": "updated"}


@pytest.fixture
def target(
    clean_config_db: None, db_connection: sa.Connection
) -> Iterator[sa.Connection]:
    set_driver_path(db_connection, "MySQL")
    insert_row(
        db_connection,
        "jdbc_connections",
        dbalias=DBALIAS,
        jdbc_url="jdbc:mysql://target.example:3306/db",
        datalake_source="sales",
    )
    common_config.Config().encrypt_user_password(DBALIAS, "user", "secret")
    yield db_connection


def _add_export(conn: sa.Connection, table: str = TABLE, /, **values: object) -> None:
    insert_row(
        conn,
        "export_tables",
        **{
            "dbalias": DBALIAS,
            "target_schema": SCHEMA,
            "target_table": table,
            "hive_db": "testdb",
            "hive_table": table,
            **values,
        },
    )


def _config(conn: sa.Connection, **values: object) -> export_config.Config:
    _add_export(conn, **values)
    config = export_config.Config(DBALIAS, SCHEMA, TABLE)
    config.get_export_config()
    return config


def _common(config: export_config.Config) -> common_config.Config:
    assert config.common_config is not None
    return config.common_config


def _row(conn: sa.Connection, *columns: str) -> tuple[object, ...]:
    query = f"SELECT {', '.join(columns)} FROM export_tables"
    return tuple(conn.execute(sa.text(query)).one())


FULL_ROW = {
    "export_type": "incr",
    "export_tool": "spark",
    "last_update_from_hive": "2026-01-15 09:00:00",
    "sql_where_addition": "a = 1",
    "forceCreateTempTable": 1,
    "validate_export": 0,
    "validationMethod": "customQuery",
    "validationCustomQueryHiveSQL": "select 1",
    "validationCustomQueryTargetSQL": "select 2",
    "truncate_target": 0,
    "mappers": 3,
    "validationCustomQueryHiveValue": "[1]",
    "validationCustomQueryTargetValue": "[2]",
    "incr_column": "updated",
    "incr_validation_method": "incr",
    "incr_maxvalue": "2026-01-01 00:00:00",
    "sqoop_last_size": 1000,
    "sqoop_last_rows": 10,
    "sqoop_last_mappers": 2,
    "hive_javaheap": 2048,
}


def test_get_export_config_reads_every_column_of_the_table_and_connection(
    target: sa.Connection,
) -> None:
    config = _config(target, **FULL_ROW)

    table_id = target.execute(sa.text("SELECT table_id FROM export_tables")).scalar()
    assert (
        config.table_id,
        config.export_type,
        config.hive_db,
        config.hive_table,
        config.validate_export,
        config.sqoop_last_size,
        config.sqoop_last_rows,
        config.sqoop_last_mappers,
        config.incr_column,
        config.incr_maxvalue,
        config.truncate_target_table,
        config.incr_validation_method,
        config.hive_java_heap,
        config.export_tool,
        config.sql_where_addition,
        config.validation_method,
        config.validation_custom_query_hive_sql,
        config.validation_custom_query_target_sql,
        config.validation_custom_query_hive_value,
        config.validation_custom_query_target_value,
        config.force_create_temp_table,
        _common(config).operation_timestamp,
        config.datalake_source_connection,
        config.export_phase,
        config.export_is_incremental,
        config.hive_export_temp_table,
    ) == (
        table_id,
        "incr",
        "testdb",
        "orders",
        False,
        1000,
        10,
        2,
        "updated",
        "2026-01-01 00:00:00",
        False,
        "incr",
        2048,
        "spark",
        "a = 1",
        "customQuery",
        "select 1",
        "select 2",
        "[1]",
        "[2]",
        True,
        datetime(2026, 1, 15, 9, 0, tzinfo=UTC),
        "sales",
        constants.EXPORT_PHASE_INCR,
        True,
        "test_target__dbo__orders__exporttemptable",
    )


@pytest.mark.parametrize(
    ("values", "message"),
    [
        ({"export_tool": "sqoop", "sql_where_addition": "a = 1"}, "sqoop exports"),
        ({"export_type": "incr"}, "incr_column"),
    ],
    ids=["sqoop-with-where", "incremental-without-column"],
)
def test_get_export_config_with_invalid_settings_raises(
    target: sa.Connection, values: dict[str, object], message: str
) -> None:
    with pytest.raises(InvalidConfigurationError, match=message):
        _config(target, **values)


def test_get_export_config_of_unknown_table_raises(target: sa.Connection) -> None:
    with pytest.raises(InvalidConfigurationError, match="can't be found"):
        export_config.Config(DBALIAS, SCHEMA, "nope").get_export_config()


@pytest.mark.parametrize(
    ("method", "column"),
    [
        ("save_custom_sql_validation_hive_value", "validationCustomQueryHiveValue"),
        ("save_custom_sql_validation_target_value", "validationCustomQueryTargetValue"),
    ],
)
def test_save_custom_sql_validation_value_stores_the_json(
    target: sa.Connection, method: str, column: str
) -> None:
    config = _config(target)

    getattr(config, method)('[{"0": 5}]')

    assert _row(target, column) == ('[{"0": 5}]',)


def test_update_last_update_from_hive_stores_the_start_time_and_reads_it_back(
    target: sa.Connection,
) -> None:
    config = _config(target)
    config.start_date = datetime(2026, 3, 1, 8, 30, 0, 123456, tzinfo=UTC)

    config.update_last_update_from_hive()

    stored = datetime(2026, 3, 1, 8, 30)
    assert _row(target, "last_update_from_hive") == (stored,)
    assert _common(config).operation_timestamp == stored.replace(tzinfo=UTC)


def test_clear_validation_data_empties_the_validation_columns(
    target: sa.Connection,
) -> None:
    config = _config(
        target,
        hive_rowcount=5,
        target_rowcount=6,
        validationCustomQueryHiveValue="[1]",
        validationCustomQueryTargetValue="[2]",
    )

    config.clear_validation_data()

    assert _row(
        target,
        "hive_rowcount",
        "target_rowcount",
        "validationCustomQueryHiveValue",
        "validationCustomQueryTargetValue",
    ) == (None, None, None, None)


@pytest.mark.xfail(
    reason="Known bug: save_export_statistics builds the UPDATE but runs it "
    "only when first_set is False, which it never becomes",
    raises=AssertionError,
    strict=True,
)
def test_save_export_statistics_stores_the_statistics(target: sa.Connection) -> None:
    config = _config(target)

    config.save_export_statistics(
        sqoop_start_uts=1700000000, sqoop_size=1000, sqoop_rows=10, sqoop_mappers=2
    )

    assert _row(
        target,
        "sqoop_last_execution",
        "sqoop_last_size",
        "sqoop_last_rows",
        "sqoop_last_mappers",
    ) == (1700000000, 1000, 10, 2)


def test_save_hive_table_row_count_stores_the_count(target: sa.Connection) -> None:
    config = _config(target)

    config.save_hive_table_row_count(42)

    assert _row(target, "hive_rowcount") == (42,)


@pytest.mark.parametrize(
    ("export_tool", "expected"), [("spark", (42, 42)), ("sqoop", (42, 7))]
)
def test_get_jdbc_table_row_count_stores_the_target_row_count(
    target: sa.Connection,
    monkeypatch: pytest.MonkeyPatch,
    export_tool: str,
    expected: tuple[int, int],
) -> None:
    config = _config(target, export_tool=export_tool, sqoop_last_rows=7)
    monkeypatch.setattr(
        _common(config),
        "get_jdbc_table_row_count",
        lambda schema, table, where: 42,
    )

    config.get_jdbc_table_row_count()

    assert _row(target, "target_rowcount", "sqoop_last_rows") == expected


def test_reset_incr_min_max_values_keeps_only_the_given_max_value(
    target: sa.Connection,
) -> None:
    config = _config(
        target,
        **INCREMENTAL,
        incr_minvalue="1",
        incr_maxvalue="5",
        incr_minvalue_pending="5",
        incr_maxvalue_pending="9",
    )

    config.reset_incr_min_max_values("7")

    assert _row(target, *INCR_COLUMNS) == (None, "7", None, None)


@pytest.mark.parametrize(
    ("values", "expected"),
    [
        ({"hive_rowcount": 5, "target_rowcount": 5}, True),
        ({"hive_rowcount": 5, "target_rowcount": 6}, False),
        ({"hive_rowcount": 5, "target_rowcount": 6, "validate_export": 0}, True),
    ],
    ids=["equal", "different", "not-validated"],
)
def test_validate_row_count_compares_the_stored_row_counts(
    target: sa.Connection, values: dict[str, object], expected: bool
) -> None:
    config = _config(target, **values)

    assert config.validate_row_count() is expected


@pytest.mark.parametrize(
    ("values", "expected"),
    [
        ({"export_tool": "sqoop"}, (4, 32)),
        ({"export_tool": "sqoop", "sqoop_last_size": 3 * 128 * 1024 * 1024}, (3, 32)),
        ({"export_tool": "sqoop", "sqoop_last_size": 100}, (1, 32)),
        ({"export_tool": "sqoop", "sqoop_last_size": 5 * 64 * 1024 * 1024}, (3, 32)),
        ({"export_tool": "sqoop", "mappers": 6}, (6, 32)),
        ({"export_tool": "spark"}, (32, 32)),
        ({"export_tool": "spark", "mappers": 6}, (6, 6)),
    ],
    ids=[
        "sqoop-default",
        "sqoop-size",
        "sqoop-minimum",
        "sqoop-size-rounded",
        "sqoop-fixed",
        "spark",
        "spark-fixed",
    ],
)
def test_calculate_job_mappers_sizes_the_sessions_from_the_stored_values(
    target: sa.Connection, values: dict[str, object], expected: tuple[int, int]
) -> None:
    config = _config(target, **values)

    config.calculate_job_mappers()

    assert (config.sql_sessions, config.spark_max_executors) == expected


def test_save_incr_min_max_value_stores_the_pending_range(
    target: sa.Connection,
) -> None:
    config = _config(target, **INCREMENTAL, incr_minvalue="1", incr_maxvalue="5")

    config.save_incr_min_max_value("5", "9")

    assert _row(target, *INCR_COLUMNS) == ("1", "5", "5", "9")


def test_save_incr_pending_values_commits_and_clears_the_pending_values(
    target: sa.Connection,
) -> None:
    config = _config(
        target,
        **INCREMENTAL,
        incr_minvalue="1",
        incr_maxvalue="5",
        incr_minvalue_pending="5",
        incr_maxvalue_pending="9",
    )

    config.save_incr_pending_values()

    assert _row(target, *INCR_COLUMNS) == ("5", "9", None, None)


@pytest.mark.parametrize(
    ("pending", "expected"), [(("5", "9"), True), (("9", "9"), False)]
)
def test_is_there_incr_data_to_export_compares_the_pending_range(
    target: sa.Connection, pending: tuple[str, str], expected: bool
) -> None:
    config = _config(
        target,
        **INCREMENTAL,
        incr_minvalue_pending=pending[0],
        incr_maxvalue_pending=pending[1],
    )

    assert config.is_there_incr_data_to_export() is expected


def test_get_incr_where_statement_prefers_the_pending_range(
    target: sa.Connection,
) -> None:
    config = _config(
        target,
        **INCREMENTAL,
        incr_minvalue="1",
        incr_maxvalue="5",
        incr_minvalue_pending="5",
        incr_maxvalue_pending="9",
    )
    table_id = target.execute(sa.text("SELECT table_id FROM export_tables")).scalar()
    insert_row(
        target,
        "export_columns",
        table_id=table_id,
        column_name="updated",
        column_type="int",
        last_update_from_hive="2026-01-01 00:00:00",
    )

    where = config.get_incr_where_statement()

    assert " ".join(where.split()) == "`updated` > 5 and `updated` <= 9"


def test_get_export_tables_lists_the_hive_tables_of_a_target_schema(
    target: sa.Connection,
) -> None:
    _add_export(target, "orders")
    _add_export(target, "lines")
    _add_export(target, "other", target_schema="hr")
    config = export_config.Config()

    tables = config.get_export_tables(dbalias=DBALIAS, schema=SCHEMA)

    assert list(tables.columns) == ["hiveDB", "hiveTable"]
    assert sorted(tables.itertuples(index=False, name=None)) == [
        ("testdb", "lines"),
        ("testdb", "orders"),
    ]


def test_get_export_tables_of_unknown_schema_exits(target: sa.Connection) -> None:
    config = export_config.Config()

    with pytest.raises(SystemExit):
        config.get_export_tables(dbalias=DBALIAS, schema="nope")


def test_add_export_table_stores_it_and_refuses_a_second_one(
    target: sa.Connection,
) -> None:
    config = export_config.Config()

    added = config.add_export_table("testdb", "orders", DBALIAS, SCHEMA, TABLE)
    again = config.add_export_table("testdb", "orders2", DBALIAS, SCHEMA, TABLE)

    assert (added, again) == (True, False)
    assert _row(target, "hive_db", "hive_table", "dbalias", "target_schema") == (
        "testdb",
        "orders",
        DBALIAS,
        SCHEMA,
    )


def test_write_before_get_export_config_raises_and_stores_nothing(
    target: sa.Connection,
) -> None:
    _add_export(target)
    config = export_config.Config(DBALIAS, SCHEMA, TABLE)

    with pytest.raises(InvalidConfigurationError, match="not been read"):
        config.save_hive_table_row_count(42)

    assert _row(target, "hive_rowcount") == (None,)


@pytest.mark.parametrize(
    "method", ["is_there_incr_data_to_export", "calculate_job_mappers"]
)
def test_reading_the_row_again_after_it_was_deleted_raises(
    target: sa.Connection, method: str
) -> None:
    config = _config(target, **INCREMENTAL)
    target.execute(sa.text("DELETE FROM export_tables"))

    with pytest.raises(InvalidConfigurationError, match="can't be found"):
        getattr(config, method)()


def test_get_export_config_reads_a_zero_date_as_none(target: sa.Connection) -> None:
    _add_export(target)
    # Outside strict mode, MariaDB stores an invalid DATETIME as zero.
    target.execute(
        sa.text(
            "SET STATEMENT sql_mode = '' FOR UPDATE export_tables"
            " SET last_update_from_hive = '0000-00-00 00:00:00'"
        )
    )
    config = export_config.Config(DBALIAS, SCHEMA, TABLE)

    config.get_export_config()

    assert _common(config).operation_timestamp is None
