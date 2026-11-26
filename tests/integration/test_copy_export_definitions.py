"""The copy of an export DAG to another DBImport instance: the DAG, the
export_tables rows of its filter with their export_columns rows and JDBC
connections, its tasks, and its sensors as tasks. The second database of
the compose server, with the schema the ORM creates, is the
configuration database of the other instance."""

from collections.abc import Iterator
from datetime import datetime
from typing import Any

import pytest
import sqlalchemy as sa
from sqlalchemy.orm import close_all_sessions

from dbimport.config_db import common_config
from dbimport.operations import copy_operations
from tests.configdb import ORM_DATABASE, insert_row, set_driver_path

DBALIAS = "test_mysql"
SCHEMA = "dbo"
INSTANCE = "remote"
DAG = "export_dag"
SYNCED = datetime(2026, 1, 2, 3, 4, 5)
TABLE_KEYS = {"table_id", "sqoop_last_execution"}
COLUMN_KEYS = {"table_id", "column_id"}
SECRETS = {"credentials", "private_key_path", "public_key_path"}


@pytest.fixture(autouse=True)
def _close_sessions() -> Iterator[None]:
    """copy_operations leaves its sessions open, and with them a
    transaction; a command ends them with its process, a test here."""
    yield
    close_all_sessions()


def _rows(conn: sa.Connection, sql: str, leave_out: set[str]) -> list[dict[str, Any]]:
    return [
        {key: value for key, value in row.items() if key not in leave_out}
        for row in conn.execute(sa.text(sql)).mappings()
    ]


def _tables(conn: sa.Connection) -> list[dict[str, Any]]:
    return _rows(
        conn, "SELECT * FROM export_tables ORDER BY dbalias, target_table", TABLE_KEYS
    )


def _columns(conn: sa.Connection) -> list[dict[str, Any]]:
    return _rows(
        conn,
        "SELECT c.* FROM export_columns c JOIN export_tables t USING (table_id) "
        "ORDER BY t.target_table, c.column_order",
        COLUMN_KEYS,
    )


def _connections(conn: sa.Connection, leave_out: set[str]) -> list[dict[str, Any]]:
    return _rows(conn, "SELECT * FROM jdbc_connections ORDER BY dbalias", leave_out)


def _dags(conn: sa.Connection) -> list[dict[str, Any]]:
    return _rows(conn, "SELECT * FROM airflow_export_dags", {"auto_regenerate_dag"})


def _tasks(conn: sa.Connection) -> list[dict[str, Any]]:
    return _rows(conn, "SELECT * FROM airflow_tasks ORDER BY task_name", set())


def _add_connection(conn: sa.Connection, dbalias: str = DBALIAS) -> None:
    insert_row(
        conn,
        "jdbc_connections",
        dbalias=dbalias,
        jdbc_url=f"jdbc:mysql://target.example:3306/{dbalias}",
    )
    common_config.Config().encrypt_user_password(dbalias, "user", "secret")


def _add_table(
    conn: sa.Connection,
    target_table: str,
    /,
    dbalias: str = DBALIAS,
    **values: object,
) -> int:
    insert_row(
        conn,
        "export_tables",
        **{
            "dbalias": dbalias,
            "target_schema": SCHEMA,
            "target_table": target_table,
            "hive_db": "testdb",
            "hive_table": target_table,
            **values,
        },
    )
    return int(
        conn.execute(
            sa.text(
                "SELECT table_id FROM export_tables "
                "WHERE dbalias = :d AND target_table = :t"
            ),
            {"d": dbalias, "t": target_table},
        ).scalar_one()
    )


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


def _add_dag(conn: sa.Connection, **values: object) -> None:
    insert_row(
        conn,
        "airflow_export_dags",
        **{
            "dag_name": DAG,
            "filter_dbalias": DBALIAS,
            "schedule_interval": "None",
            **values,
        },
    )


def _add_dags(local: sa.Connection, remote: sa.Connection, **values: object) -> None:
    """The DAG here, and a DAG of that name on the other instance, which
    the copy updates."""
    _add_dag(local, **values)
    insert_row(
        remote,
        "airflow_export_dags",
        dag_name=DAG,
        filter_dbalias="None",
        schedule_interval="None",
    )


@pytest.fixture
def local(
    clean_config_db: None,
    engine: sa.Engine,
    db_connection: sa.Connection,
    orm_db_connection: sa.Connection,
) -> Iterator[sa.Connection]:
    """This instance: a MySQL connection with the export of table orders
    (two columns), and the other instance with the login of the test
    user."""
    set_driver_path(db_connection, "MySQL")
    _add_connection(db_connection)
    table_id = _add_table(db_connection, "orders")
    _add_column(db_connection, table_id, "id", 1)
    _add_column(db_connection, table_id, "name", 2, column_type="string")
    insert_row(
        db_connection,
        "dbimport_instances",
        name=INSTANCE,
        db_hostname=engine.url.host,
        db_port=engine.url.port,
        db_database=ORM_DATABASE,
        hdfs_address="hdfs://remote:8020",
        hdfs_basedir="/apps/remote",
    )
    # As copy --encryptInstance stores it; the copy operation itself is
    # created by each test.
    login = f"{engine.url.username} {engine.url.password}\n"
    db_connection.execute(
        sa.text("UPDATE dbimport_instances SET db_credentials = :c"),
        {"c": common_config.Config().crypto.encrypt(login)},
    )
    yield db_connection


@pytest.fixture
def remote(local: sa.Connection, orm_db_connection: sa.Connection) -> sa.Connection:
    return orm_db_connection


def _copy_dag(**options: bool) -> None:
    copy_operations.Operation().copy_airflow_export_dag(
        airflow_dag_name=DAG, copy_destination=INSTANCE, **options
    )


def test_copy_airflow_export_dag_copies_the_dag_and_the_tables_of_its_filter(
    local: sa.Connection, remote: sa.Connection
) -> None:
    _add_dags(local, remote, retries=3)
    _add_connection(local, "other_mysql")
    _add_table(local, "other", dbalias="other_mysql")

    _copy_dag()

    assert _dags(remote) == _dags(local)
    assert [row["target_table"] for row in _tables(remote)] == ["orders"]
    assert _tables(remote) == [
        row for row in _tables(local) if row["dbalias"] == DBALIAS
    ]
    assert _columns(remote) == _columns(local)
    assert _connections(remote, SECRETS) == [
        row for row in _connections(local, SECRETS) if row["dbalias"] == DBALIAS
    ]


@pytest.mark.parametrize(
    ("schema", "table", "copied"),
    [
        (None, None, ["items", "orders"]),
        ("dbo", "ord*", ["orders"]),
        ("sales", None, []),
    ],
    ids=["all", "schema-and-table", "other-schema"],
)
def test_copy_airflow_export_dag_copies_the_tables_of_its_target_filters(
    local: sa.Connection,
    remote: sa.Connection,
    schema: str | None,
    table: str | None,
    copied: list[str],
) -> None:
    _add_table(local, "items")
    _add_dags(local, remote, filter_target_schema=schema, filter_target_table=table)

    _copy_dag()

    assert [row["target_table"] for row in _tables(remote)] == copied


@pytest.mark.parametrize("regenerate", [False, True])
def test_copy_airflow_export_dag_sets_auto_regenerate_on_request(
    local: sa.Connection, remote: sa.Connection, regenerate: bool
) -> None:
    _add_dags(local, remote, auto_regenerate_dag=0)

    _copy_dag(set_auto_regenerate_dag=regenerate)

    assert remote.execute(
        sa.text("SELECT auto_regenerate_dag FROM airflow_export_dags")
    ).scalar_one() == int(regenerate)


@pytest.mark.parametrize("sync_credentials", [0, 1, 2])
def test_copy_airflow_export_dag_copies_credentials_only_when_the_instance_syncs_them(
    local: sa.Connection, remote: sa.Connection, sync_credentials: int
) -> None:
    _add_dags(local, remote)
    local.execute(
        sa.text("UPDATE dbimport_instances SET sync_credentials = :s"),
        {"s": sync_credentials},
    )

    _copy_dag()

    stored = remote.execute(
        sa.text("SELECT credentials FROM jdbc_connections")
    ).scalar_one()
    expected = local.execute(
        sa.text("SELECT credentials FROM jdbc_connections")
    ).scalar_one()
    assert stored == (expected if sync_credentials == 1 else None)


def test_copy_airflow_export_dag_again_updates_the_copy(
    local: sa.Connection, remote: sa.Connection
) -> None:
    _add_dags(local, remote)
    _copy_dag()
    remote.execute(sa.text("UPDATE export_tables SET sqoop_last_execution = 7"))
    local.execute(sa.text("UPDATE export_tables SET hive_table = 'orders_v2'"))
    local.execute(
        sa.text(
            "UPDATE export_columns SET column_type = 'bigint' WHERE column_name = 'id'"
        )
    )
    local.execute(sa.text("DELETE FROM export_columns WHERE column_name = 'name'"))
    table_id = local.execute(sa.text("SELECT table_id FROM export_tables")).scalar_one()
    _add_column(local, table_id, "created", 3, column_type="timestamp")

    _copy_dag()

    assert _tables(remote) == _tables(local)
    assert [row["column_name"] for row in _columns(remote)] == ["id", "name", "created"]
    assert (
        remote.execute(
            sa.text("SELECT sqoop_last_execution FROM export_tables")
        ).scalar_one()
        == 7
    )


def test_copy_airflow_export_dag_copies_its_tasks(
    local: sa.Connection, remote: sa.Connection
) -> None:
    _add_dags(local, remote)
    for name in ("shell_01", "shell_02"):
        insert_row(
            local,
            "airflow_tasks",
            dag_name=DAG,
            task_name=name,
            task_type="shell script",
            placement="before main",
            airflow_priority=5,
            task_config="/usr/bin/sleep 1",
        )

    _copy_dag()

    assert _tasks(remote) == _tasks(local)


def test_copy_airflow_export_dag_copies_a_task_with_its_connection(
    local: sa.Connection, remote: sa.Connection
) -> None:
    _add_dags(local, remote)
    _add_connection(local, "task_mysql")
    insert_row(
        local,
        "airflow_tasks",
        dag_name=DAG,
        task_name="sql_01",
        task_type="JDBC SQL",
        placement="after main",
        jdbc_dbalias="task_mysql",
        task_config="select 1",
    )

    _copy_dag()

    assert _tasks(remote) == _tasks(local)


def test_copy_airflow_export_dag_copies_tasks_with_an_empty_priority(
    local: sa.Connection, remote: sa.Connection
) -> None:
    _add_dags(local, remote)
    for name, priority in (("shell_01", 5), ("shell_02", None)):
        insert_row(
            local,
            "airflow_tasks",
            dag_name=DAG,
            task_name=name,
            task_type="shell script",
            placement="before main",
            airflow_priority=priority,
            task_config="/usr/bin/sleep 1",
        )

    _copy_dag()

    assert _tasks(remote) == _tasks(local)


def test_copy_airflow_export_dag_copies_its_sensors_as_tasks(
    local: sa.Connection, remote: sa.Connection
) -> None:
    _add_dags(local, remote)
    insert_row(
        local,
        "airflow_dag_sensors",
        dag_name=DAG,
        sensor_name="wait_dag",
        wait_for_dag="import_dag",
    )
    insert_row(
        local,
        "airflow_dag_sensors",
        dag_name=DAG,
        sensor_name="wait_task",
        wait_for_dag="import_dag",
        wait_for_task="orders",
    )

    _copy_dag()

    tasks = remote.execute(
        sa.text(
            "SELECT task_name, task_type, placement, task_config "
            "FROM airflow_tasks ORDER BY task_name"
        )
    )
    assert [tuple(row) for row in tasks] == [
        ("wait_dag", "DAG Sensor", "before main", "import_dag.stop"),
        ("wait_task", "DAG Sensor", "before main", "import_dag.orders"),
    ]


def test_copy_airflow_export_dag_creates_a_dag_missing_on_the_instance(
    local: sa.Connection, remote: sa.Connection
) -> None:
    _add_dag(local)

    _copy_dag()

    assert _dags(remote) == _dags(local)


@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason="every value is turned into text and the text 'None' into NULL",
)
def test_copy_airflow_export_dag_copies_the_text_none_as_it_is(
    local: sa.Connection, remote: sa.Connection
) -> None:
    _add_dags(local, remote)
    local.execute(sa.text("UPDATE export_tables SET sql_where_addition = 'None'"))

    _copy_dag()

    assert _tables(remote) == _tables(local)
