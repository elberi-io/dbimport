import pytest
import sqlalchemy as sa

from DBImportConfig import common_config


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
    value = common_config.config().getConfigValue(key)

    assert value == expected
    assert type(value) is type(expected)


def test_get_config_value_unknown_key_exits(clean_config_db: None) -> None:
    with pytest.raises(SystemExit) as exc_info:
        common_config.config().getConfigValue("no_such_key")

    assert exc_info.value.code == 1


def test_get_config_value_empty_string_value_exits(
    clean_config_db: None, db_connection: sa.Connection
) -> None:
    _set_config_row(db_connection, "timezone", "valueStr", "")

    with pytest.raises(SystemExit) as exc_info:
        common_config.config().getConfigValue("timezone")

    assert exc_info.value.code == 1


def test_get_config_value_bool_key_with_value_other_than_0_or_1_exits(
    clean_config_db: None, db_connection: sa.Connection
) -> None:
    _set_config_row(db_connection, "airflow_disable", "valueInt", 2)

    with pytest.raises(SystemExit) as exc_info:
        common_config.config().getConfigValue("airflow_disable")

    assert exc_info.value.code == 1


def test_get_config_value_whitelisted_key_without_row_exits(
    clean_config_db: None, db_connection: sa.Connection
) -> None:
    db_connection.execute(
        sa.text("DELETE FROM configuration WHERE configKey = 'timezone'")
    )

    with pytest.raises(SystemExit) as exc_info:
        common_config.config().getConfigValue("timezone")

    assert exc_info.value.code == 1
