from collections.abc import Callable

import pytest
import sqlalchemy as sa

from dbimport.config_db import common_config, config_keys
from dbimport.config_db.repositories.configuration import UnknownKeyError
from dbimport.schedule import airflow_dags
from dbimport.server import rest_server_calls


def _set_config_row(conn: sa.Connection, key: str, column: str, value: object) -> None:
    conn.execute(
        sa.text(f"UPDATE configuration SET {column} = :value WHERE configKey = :key"),
        {"value": value, "key": key},
    )


@pytest.mark.parametrize(
    ("key", "expected"),
    [
        ("airflow_disable", False),
        ("airflow_create_pool_with_task", True),
        ("airflow_default_pool_size", 24),
        ("rest_timeout", 5),
        ("timezone", "UTC"),
        ("import_staging_database", "etl_import_staging"),
    ],
)
def test_get_config_value_seeded_key_returns_typed_value(
    clean_config_db: None, key: str, expected: object
) -> None:
    value = common_config.Config().get_config_value(key)

    assert value == expected
    assert type(value) is type(expected)


def test_get_config_value_unknown_key_exits(clean_config_db: None) -> None:
    with pytest.raises(SystemExit) as exc_info:
        common_config.Config().get_config_value("no_such_key")

    assert exc_info.value.code == 1


def test_get_config_value_empty_string_value_exits(
    clean_config_db: None, db_connection: sa.Connection
) -> None:
    _set_config_row(db_connection, "timezone", "valueStr", "")

    with pytest.raises(SystemExit) as exc_info:
        common_config.Config().get_config_value("timezone")

    assert exc_info.value.code == 1


def test_get_config_value_bool_key_with_value_other_than_0_or_1_exits(
    clean_config_db: None, db_connection: sa.Connection
) -> None:
    _set_config_row(db_connection, "airflow_disable", "valueInt", 2)

    with pytest.raises(SystemExit) as exc_info:
        common_config.Config().get_config_value("airflow_disable")

    assert exc_info.value.code == 1


def test_get_config_value_whitelisted_key_without_row_exits(
    clean_config_db: None, db_connection: sa.Connection
) -> None:
    db_connection.execute(
        sa.text("DELETE FROM configuration WHERE configKey = 'timezone'")
    )

    with pytest.raises(SystemExit) as exc_info:
        common_config.Config().get_config_value("timezone")

    assert exc_info.value.code == 1


def test_get_config_value_every_key_reads_back_its_default(
    clean_config_db: None,
) -> None:
    config = common_config.Config()
    keys = [key for key in config_keys.KEYS if not callable(key.default)]

    values = {key.name: config.get_config_value(key.name) for key in keys}

    assert {name: (type(v), v) for name, v in values.items()} == {
        key.name: (type(key.default), key.default) for key in keys
    }


def _dag_reader() -> Callable[[str], object]:
    return airflow_dags.Initialize().get_config_value


def _rest_reader() -> Callable[[str], object]:
    return rest_server_calls.DBCalls().get_config_value


# DAG generation and the REST server log a value they cannot use and go
# on with None, where the command line programs exit.
READERS = pytest.mark.parametrize("reader", [_dag_reader, _rest_reader])


@READERS
def test_get_config_value_of_dags_and_rest_returns_typed_value(
    clean_config_db: None, reader: Callable[[], Callable[[str], object]]
) -> None:
    get = reader()

    values = [get("airflow_disable"), get("rest_timeout"), get("timezone")]

    assert [(type(v), v) for v in values] == [(bool, False), (int, 5), (str, "UTC")]


@READERS
@pytest.mark.parametrize(
    ("key", "column", "value"),
    [("timezone", "valueStr", ""), ("airflow_disable", "valueInt", 2)],
)
def test_get_config_value_of_dags_and_rest_unusable_value_returns_none(
    clean_config_db: None,
    db_connection: sa.Connection,
    reader: Callable[[], Callable[[str], object]],
    key: str,
    column: str,
    value: object,
) -> None:
    _set_config_row(db_connection, key, column, value)

    assert reader()(key) is None


@READERS
def test_get_config_value_of_dags_and_rest_key_without_row_returns_none(
    clean_config_db: None,
    db_connection: sa.Connection,
    reader: Callable[[], Callable[[str], object]],
) -> None:
    db_connection.execute(
        sa.text("DELETE FROM configuration WHERE configKey = 'timezone'")
    )

    assert reader()("timezone") is None


@READERS
def test_get_config_value_of_dags_and_rest_unknown_key_raises(
    clean_config_db: None, reader: Callable[[], Callable[[str], object]]
) -> None:
    with pytest.raises(UnknownKeyError):
        reader()("no_such_key")
