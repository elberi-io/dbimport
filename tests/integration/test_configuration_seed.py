"""Seeding of the configuration table by setup (config_keys, ADR-0003)."""

import sqlalchemy as sa

from dbimport.config_db import config_keys
from tests.configdb import close_dbimport_connections, insert_row


def _seed() -> None:
    from dbimport.setup import database

    setup = database.Initialize()
    try:
        setup.update_configuration_values()
    finally:
        close_dbimport_connections(setup)


def _row(conn: sa.Connection, key: str) -> tuple[object, ...]:
    query = sa.text(
        "SELECT valueInt, valueStr, description FROM configuration "
        "WHERE configKey = :key"
    )
    return tuple(conn.execute(query, {"key": key}).one())


def test_seed_existing_key_keeps_its_value_and_gets_the_current_description(
    clean_config_db: None, db_connection: sa.Connection
) -> None:
    db_connection.execute(
        sa.text(
            "UPDATE configuration SET valueStr = 'Europe/Stockholm', "
            "description = 'old text' WHERE configKey = 'timezone'"
        )
    )

    _seed()

    assert _row(db_connection, "timezone") == (
        None,
        "Europe/Stockholm",
        config_keys.BY_NAME["timezone"].description,
    )


def test_seed_missing_key_is_inserted_with_its_default(
    clean_config_db: None, db_connection: sa.Connection
) -> None:
    db_connection.execute(
        sa.text("DELETE FROM configuration WHERE configKey = 'rest_timeout'")
    )

    _seed()

    assert _row(db_connection, "rest_timeout") == (
        5,
        None,
        config_keys.BY_NAME["rest_timeout"].description,
    )


def test_seed_keeps_the_secret_key_of_the_installation(
    clean_config_db: None, db_connection: sa.Connection
) -> None:
    before = _row(db_connection, "restserver_secret_key")

    _seed()

    assert _row(db_connection, "restserver_secret_key") == before


def test_seed_leaves_rows_of_unknown_keys_alone(
    clean_config_db: None, db_connection: sa.Connection
) -> None:
    insert_row(
        db_connection,
        "configuration",
        configKey="sqoop_max_mappers",
        valueInt=4,
        description="old text",
    )

    _seed()

    assert _row(db_connection, "sqoop_max_mappers") == (4, None, "old text")
