"""The Atlas discovery cache (atlas_column_cache, atlas_key_cache) as
AtlasOperation.discover_atlas_rdbms reads and writes it.

The source database and Atlas are external services: the tests give the
discovery a schema reader that returns fixed tables, columns and keys, and
replace the calls that send to and read from Atlas. The configuration
database is the real one.
"""

import json
from collections.abc import Iterator
from datetime import datetime
from typing import Any

import pandas as pd
import pytest
import sqlalchemy as sa

from dbimport.common import constants
from dbimport.common.exceptions import AtlasError, SchemaError, SQLError
from dbimport.operations.atlas_operations import AtlasOperation
from dbimport.source_schema import schema_reader
from tests.configdb import insert_row

HOSTNAME = "sqlhost"
PORT = "1433"
DATABASE = "sales"
CREATED = datetime(2026, 1, 15, 9, 0)


def _column(
    table: str,
    name: str,
    column_type: str = "int",
    created: datetime | None = CREATED,
) -> dict[str, Any]:
    return {
        "SCHEMA_NAME": "dbo",
        "TABLE_NAME": table,
        "TABLE_COMMENT": f"{table} table",
        "SOURCE_COLUMN_NAME": name,
        "SOURCE_COLUMN_TYPE": column_type,
        "SOURCE_COLUMN_LENGTH": None,
        "SOURCE_COLUMN_COMMENT": f"{name} column",
        "IS_NULLABLE": "NO",
        "TABLE_TYPE": "BASE TABLE",
        "TABLE_CREATE_TIME": created,
        "DEFAULT_VALUE": None,
    }


def _primary_key(table: str, column: str) -> dict[str, Any]:
    return {
        "SCHEMA_NAME": "dbo",
        "TABLE_NAME": table,
        "CONSTRAINT_NAME": f"pk_{table}",
        "CONSTRAINT_TYPE": constants.PRIMARY_KEY,
        "COL_NAME": column,
        "REFERENCE_SCHEMA_NAME": None,
        "REFERENCE_TABLE_NAME": None,
        "REFERENCE_COL_NAME": None,
        "COL_KEY_POSITION": 1,
    }


class FakeSource(schema_reader.Source):
    """The source database: what the schema reader reads from it."""

    def __init__(
        self, columns: list[dict[str, Any]], keys: list[dict[str, Any]]
    ) -> None:
        self.columns = columns
        self.keys = keys

    def get_jdbc_tables_and_views(
        self,
        jdbc_cursor: object,
        server_type: object,
        database: object = None,
        schema_filter: object = None,
        table_filter: object = None,
    ) -> pd.DataFrame:
        tables = sorted({(c["SCHEMA_NAME"], c["TABLE_NAME"]) for c in self.columns})
        return pd.DataFrame(tables, columns=["schema", "table"])

    def read_table_columns(
        self,
        jdbc_cursor: object,
        server_type: object = None,
        database: object = None,
        schema: object = None,
        table: object = None,
    ) -> pd.DataFrame:
        return pd.DataFrame(self.columns)

    def read_table_keys(
        self,
        jdbc_cursor: object,
        server_type: object = None,
        database: object = None,
        schema: object = None,
        table: object = None,
    ) -> pd.DataFrame:
        return pd.DataFrame(self.keys)


class Discovery:
    """An AtlasOperation for one MSSQL connection, with the source database
    and Atlas replaced. sent lists the tables sent to Atlas."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self.operation = AtlasOperation()
        self.operation.set_configuration(
            {
                "dbAlias": "src_mssql",
                "atlasIncludeFilter": "dbo.*",
                "atlasExcludeFilter": "",
                "jdbc_hostname": HOSTNAME,
                "jdbc_port": PORT,
                "jdbc_servertype": constants.MSSQL,
                "jdbc_database": DATABASE,
            }
        )
        self.sent: list[tuple[str, str]] = []
        self.fail_on: set[str] = set()
        monkeypatch.setattr(self.operation, "connect_to_jdbc", lambda force=False: None)
        monkeypatch.setattr(
            self.operation, "update_atlas_with_rdbms_data", self._send_to_atlas
        )
        monkeypatch.setattr(
            self.operation,
            "get_atlas_rdbms_names",
            lambda schema_name, table_name: {"dbUri": f"{DATABASE}@{HOSTNAME}"},
        )
        monkeypatch.setattr(self.operation, "atlas_get_data", self._atlas_database)

    def _send_to_atlas(
        self,
        schema_name: str,
        table_name: str,
        print_info: bool = True,
        error_on_warning: bool = False,
    ) -> None:
        if table_name in self.fail_on:
            raise AtlasError
        self.sent.append((schema_name, table_name))

    def _atlas_database(self, url: str, logger: str = "") -> dict[str, Any]:
        entity: dict[str, Any] = {
            "entity": {"attributes": {"tables": []}},
            "referredEntities": {},
        }
        return {"statusCode": 200, "data": json.dumps(entity)}

    def run(
        self, columns: list[dict[str, Any]], keys: list[dict[str, Any]]
    ) -> list[tuple[str, str]]:
        self.sent.clear()
        self.operation.schema_reader = FakeSource(columns, keys)
        self.operation.discover_atlas_rdbms()
        return list(self.sent)


@pytest.fixture
def discovery(
    clean_config_db: None, monkeypatch: pytest.MonkeyPatch
) -> Iterator[Discovery]:
    discovery = Discovery(monkeypatch)
    yield discovery
    if discovery.operation.config_db_engine is not None:
        discovery.operation.config_db_engine.dispose()


def _cached_columns(conn: sa.Connection) -> list[tuple[Any, ...]]:
    return [
        tuple(row)
        for row in conn.execute(
            sa.text(
                "SELECT hostname, port, database_name, schema_name, table_name, "
                "column_name, column_type, column_comment, table_create_time "
                "FROM atlas_column_cache ORDER BY hostname, table_name, column_name"
            )
        )
    ]


def _cached_keys(conn: sa.Connection) -> list[tuple[Any, ...]]:
    return [
        tuple(row)
        for row in conn.execute(
            sa.text(
                "SELECT hostname, table_name, constraint_name, constraint_type, "
                "column_name, col_key_position "
                "FROM atlas_key_cache ORDER BY hostname, table_name, column_name"
            )
        )
    ]


ORDERS = [_column("orders", "id"), _column("orders", "amount", "decimal(10,2)")]
ORDERS_KEYS = [_primary_key("orders", "id")]


def test_discovery_sends_a_new_table_to_atlas_and_caches_it(
    discovery: Discovery, db_connection: sa.Connection
) -> None:
    sent = discovery.run(ORDERS, ORDERS_KEYS)

    assert sent == [("dbo", "orders")]
    assert _cached_columns(db_connection) == [
        (
            HOSTNAME,
            PORT,
            DATABASE,
            "dbo",
            "orders",
            "amount",
            "decimal(10,2)",
            "amount column",
            CREATED,
        ),
        (HOSTNAME, PORT, DATABASE, "dbo", "orders", "id", "int", "id column", CREATED),
    ]
    assert _cached_keys(db_connection) == [
        (HOSTNAME, "orders", "pk_orders", constants.PRIMARY_KEY, "id", 1)
    ]


@pytest.mark.parametrize(
    "created",
    [CREATED, datetime(1965, 3, 1, 12, 0), None],
)
def test_discovery_of_an_unchanged_table_sends_nothing(
    discovery: Discovery, db_connection: sa.Connection, created: datetime | None
) -> None:
    columns = [_column("orders", "id", created=created)]
    discovery.run(columns, ORDERS_KEYS)
    cached = _cached_columns(db_connection)

    sent = discovery.run(columns, ORDERS_KEYS)

    assert sent == []
    assert _cached_columns(db_connection) == cached


def test_discovery_of_a_changed_table_sends_it_and_replaces_its_cache(
    discovery: Discovery, db_connection: sa.Connection
) -> None:
    discovery.run(ORDERS, ORDERS_KEYS)
    changed = [_column("orders", "id"), _column("orders", "customer", "varchar(40)")]

    sent = discovery.run(changed, ORDERS_KEYS)

    assert sent == [("dbo", "orders")]
    assert [row[5] for row in _cached_columns(db_connection)] == ["customer", "id"]
    assert len(_cached_keys(db_connection)) == 1


def test_discovery_keeps_the_cache_of_other_connections_and_tables(
    discovery: Discovery, db_connection: sa.Connection
) -> None:
    other = {
        "port": PORT,
        "database_name": DATABASE,
        "schema_name": "dbo",
        "column_name": "id",
    }
    insert_row(
        db_connection,
        "atlas_column_cache",
        hostname="otherhost",
        table_name="orders",
        **other,
    )
    insert_row(
        db_connection,
        "atlas_column_cache",
        hostname=HOSTNAME,
        table_name="customers",
        **other,
    )
    discovery.run(ORDERS, ORDERS_KEYS)
    changed = [_column("orders", "id"), _column("orders", "customer", "varchar(40)")]

    discovery.run(changed, ORDERS_KEYS)

    assert [(row[0], row[4], row[5]) for row in _cached_columns(db_connection)] == [
        ("otherhost", "orders", "id"),
        (HOSTNAME, "customers", "id"),
        (HOSTNAME, "orders", "customer"),
        (HOSTNAME, "orders", "id"),
    ]


def test_discovery_with_an_atlas_error_leaves_the_cache_of_that_table_unchanged(
    discovery: Discovery, db_connection: sa.Connection
) -> None:
    discovery.run(ORDERS, ORDERS_KEYS)
    cached = _cached_columns(db_connection)
    discovery.fail_on = {"orders"}
    changed = [_column("orders", "id"), _column("orders", "customer", "varchar(40)")]

    with pytest.raises(AtlasError):
        discovery.run(changed, ORDERS_KEYS)

    assert _cached_columns(db_connection) == cached


def test_discovery_with_a_failing_cache_write_keeps_the_old_cache_rows(
    discovery: Discovery, db_connection: sa.Connection
) -> None:
    discovery.run(ORDERS, ORDERS_KEYS)
    cached = (_cached_columns(db_connection), _cached_keys(db_connection))
    changed = [_column("orders", "id"), _column("orders", "customer", "varchar(40)")]
    duplicate_keys = ORDERS_KEYS + ORDERS_KEYS

    with pytest.raises(SQLError):
        discovery.run(changed, duplicate_keys)

    assert (_cached_columns(db_connection), _cached_keys(db_connection)) == cached


def test_discovery_of_a_table_that_cannot_be_cached_raises_and_keeps_the_cache(
    discovery: Discovery, db_connection: sa.Connection
) -> None:
    discovery.run(ORDERS, ORDERS_KEYS)
    cached = _cached_columns(db_connection)
    not_text = [_column("orders", "id") | {"IS_NULLABLE": True}]

    with pytest.raises(SchemaError):
        discovery.run(not_text, ORDERS_KEYS)

    assert _cached_columns(db_connection) == cached
