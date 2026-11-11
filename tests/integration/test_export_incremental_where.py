"""WHERE statement for the target of an incremental export (export_config)."""

import pytest
import sqlalchemy as sa

from DBImportConfig import common_config, export_config
from tests.configdb import insert_row, set_driver_path

DBALIAS = "test_target"
MIN_VALUE = "2026-01-01 00:00:00"
MAX_VALUE = "2026-02-01 00:00:00"


def _add_incremental_export(
    conn: sa.Connection, jdbc_url: str, driver_type: str
) -> None:
    set_driver_path(conn, driver_type)
    insert_row(conn, "jdbc_connections", dbalias=DBALIAS, jdbc_url=jdbc_url)
    common_config.config().encrypt_user_password(DBALIAS, "user", "secret")
    insert_row(
        conn,
        "export_tables",
        dbalias=DBALIAS,
        target_schema="dbo",
        target_table="orders",
        hive_db="testdb",
        hive_table="orders",
        export_type="incr",
        incr_column="updated",
        incr_validation_method="incr",
        incr_minvalue=MIN_VALUE,
        incr_maxvalue=MAX_VALUE,
    )
    table_id = conn.execute(
        sa.text("SELECT table_id FROM export_tables WHERE dbalias = :alias"),
        {"alias": DBALIAS},
    ).scalar_one()
    insert_row(
        conn,
        "export_columns",
        table_id=table_id,
        column_name="updated",
        column_type="timestamp",
        last_update_from_hive="2026-01-01 00:00:00",
    )


@pytest.mark.parametrize(
    ("jdbc_url", "driver_type", "convert"),
    [
        pytest.param(
            "jdbc:mysql://target.example:3306/db",
            "MySQL",
            "STR_TO_DATE('{}', '%Y-%m-%d %H:%i:%s')",
            id="mysql",
        ),
        pytest.param(
            "jdbc:sqlserver://target.example:1433;database=db",
            "SQL Server",
            "CONVERT(datetime, '{}', 121)",
            id="mssql",
        ),
    ],
)
def test_incr_where_for_target_timestamp_column_converts_values(
    clean_config_db: None,
    db_connection: sa.Connection,
    jdbc_url: str,
    driver_type: str,
    convert: str,
) -> None:
    _add_incremental_export(db_connection, jdbc_url, driver_type)
    config = export_config.config(DBALIAS, "dbo", "orders")
    config.get_export_config()
    config.lookup_connection_alias()

    where = config.get_incr_where_statement(where_for_target=True)

    assert " ".join(where.split()) == (
        f"updated > {convert.format(MIN_VALUE)} "
        f"and updated <= {convert.format(MAX_VALUE)}"
    )
