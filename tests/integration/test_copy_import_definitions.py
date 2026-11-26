"""The copy of an import's definitions to another DBImport instance: the
import_tables row, its import_columns rows and its JDBC connection, after
an import and as part of an import DAG. The second database of the
compose server, with the schema the ORM creates, is the configuration
database of the other instance."""

from collections.abc import Iterator
from datetime import datetime
from typing import Any

import pytest
import sqlalchemy as sa
from sqlalchemy.orm import close_all_sessions

from dbimport.config_db import common_config, import_config
from dbimport.operations import common_operations, copy_operations
from tests.configdb import ORM_DATABASE, insert_row, set_driver_path

HIVE_DB = "testdb"
HIVE_TABLE = "orders"
DBALIAS = "test_mysql"
INSTANCE = "remote"
DAG = "import_dag"
SYNCED = datetime(2026, 1, 2, 3, 4, 5)
TABLE_KEYS = {"table_id", "copy_slave", "copy_finished"}
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
    return _rows(conn, "SELECT * FROM import_tables ORDER BY hive_table", TABLE_KEYS)


def _columns(conn: sa.Connection) -> list[dict[str, Any]]:
    return _rows(
        conn,
        "SELECT * FROM import_columns ORDER BY hive_table, column_order",
        COLUMN_KEYS,
    )


def _connections(conn: sa.Connection, leave_out: set[str]) -> list[dict[str, Any]]:
    return _rows(conn, "SELECT * FROM jdbc_connections ORDER BY dbalias", leave_out)


def _copy_state(conn: sa.Connection) -> list[tuple[Any, ...]]:
    rows = conn.execute(
        sa.text(
            "SELECT hive_table, copy_slave, copy_finished IS NOT NULL "
            "FROM import_tables ORDER BY hive_table"
        )
    )
    return [tuple(row) for row in rows]


def _add_connection(conn: sa.Connection, dbalias: str = DBALIAS) -> None:
    insert_row(
        conn,
        "jdbc_connections",
        dbalias=dbalias,
        jdbc_url=f"jdbc:mysql://source.example:3306/{dbalias}",
    )
    common_config.Config().encrypt_user_password(dbalias, "user", "secret")


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
            "last_update_from_source": SYNCED,
            **values,
        },
    )
    return int(
        conn.execute(
            sa.text("SELECT table_id FROM import_tables WHERE hive_table = :t"),
            {"t": hive_table},
        ).scalar_one()
    )


def _add_column(
    conn: sa.Connection, table_id: int, name: str, order: int, **values: object
) -> None:
    insert_row(
        conn,
        "import_columns",
        **{
            "table_id": table_id,
            "hive_db": HIVE_DB,
            "hive_table": HIVE_TABLE,
            "column_name": name.lower(),
            "source_column_name": name,
            "column_order": order,
            "column_type": "int",
            "source_column_type": "int(11)",
            "last_update_from_source": SYNCED,
            **values,
        },
    )


@pytest.fixture
def local(
    clean_config_db: None,
    engine: sa.Engine,
    db_connection: sa.Connection,
    orm_db_connection: sa.Connection,
) -> Iterator[sa.Connection]:
    """This instance: a MySQL connection, the table HIVE_DB.HIVE_TABLE with
    two columns, and the other instance with the login of the test user."""
    set_driver_path(db_connection, "MySQL")
    _add_connection(db_connection)
    table_id = _add_table(db_connection)
    _add_column(db_connection, table_id, "ID", 1)
    _add_column(db_connection, table_id, "Name", 2, column_type="string")
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
    assert engine.url.username is not None
    assert engine.url.password is not None
    # As copy --encryptInstance stores it; the copy operation itself is
    # created by each test, after the import configuration it reads.
    login = f"{engine.url.username} {engine.url.password}\n"
    db_connection.execute(
        sa.text("UPDATE dbimport_instances SET db_credentials = :c"),
        {"c": common_config.Config().crypto.encrypt(login)},
    )
    yield db_connection


@pytest.fixture
def remote(local: sa.Connection, orm_db_connection: sa.Connection) -> sa.Connection:
    return orm_db_connection


def _copy_after_import(
    local: sa.Connection, data_transfer: str = "Synchronous"
) -> None:
    """The schema copy at the end of an import of HIVE_DB.HIVE_TABLE."""
    insert_row(
        local,
        "copy_tables",
        hive_filter=f"{HIVE_DB}.*",
        destination=INSTANCE,
        data_transfer=data_transfer,
    )
    config = import_config.Config(HIVE_DB, HIVE_TABLE)
    config.get_import_config()
    common_operations.Operation(HIVE_DB, HIVE_TABLE)
    copy = copy_operations.Operation()
    copy.get_copy_destinations(HIVE_DB, HIVE_TABLE)
    copy.copy_import_schema_to_destinations()


def test_copy_after_import_creates_the_table_its_columns_and_connection(
    local: sa.Connection, remote: sa.Connection
) -> None:
    _copy_after_import(local)

    assert _tables(remote) == _tables(local)
    assert _columns(remote) == _columns(local)
    assert _connections(remote, SECRETS) == _connections(local, SECRETS)


@pytest.mark.parametrize(
    ("data_transfer", "finished"), [("Synchronous", 1), ("Asynchronous", 0)]
)
def test_copy_after_import_marks_the_copy_by_its_transfer(
    local: sa.Connection, remote: sa.Connection, data_transfer: str, finished: int
) -> None:
    _copy_after_import(local, data_transfer)

    assert _copy_state(remote) == [(HIVE_TABLE, 1, finished)]


@pytest.mark.parametrize("sync_credentials", [0, 1])
def test_copy_after_import_copies_credentials_only_when_the_instance_syncs_them(
    local: sa.Connection, remote: sa.Connection, sync_credentials: int
) -> None:
    local.execute(
        sa.text("UPDATE dbimport_instances SET sync_credentials = :s"),
        {"s": sync_credentials},
    )

    _copy_after_import(local)

    stored = remote.execute(
        sa.text("SELECT credentials FROM jdbc_connections")
    ).scalar_one()
    expected = local.execute(
        sa.text("SELECT credentials FROM jdbc_connections")
    ).scalar_one()
    assert stored == (expected if sync_credentials else None)


def test_copy_after_import_again_updates_the_copy(
    local: sa.Connection, remote: sa.Connection
) -> None:
    _copy_after_import(local)
    local.execute(sa.text("UPDATE import_tables SET source_table = 'orders_v2'"))
    local.execute(
        sa.text(
            "UPDATE import_columns SET column_type = 'bigint' WHERE column_name = 'id'"
        )
    )
    table_id = local.execute(sa.text("SELECT table_id FROM import_tables")).scalar_one()
    _add_column(local, table_id, "Created", 3, column_type="timestamp")

    _copy_after_import(local)

    assert _tables(remote) == _tables(local)
    assert _columns(remote) == _columns(local)


def test_copy_after_import_keeps_columns_removed_here(
    local: sa.Connection, remote: sa.Connection
) -> None:
    _copy_after_import(local)
    local.execute(sa.text("DELETE FROM import_columns WHERE column_name = 'name'"))

    _copy_after_import(local)

    assert [row["column_name"] for row in _columns(remote)] == ["id", "name"]


def test_copy_after_import_copies_a_column_setting_that_is_empty_on_one_column(
    local: sa.Connection, remote: sa.Connection
) -> None:
    local.execute(
        sa.text("UPDATE import_columns SET sqoop_column_type_override = NULL")
    )
    local.execute(
        sa.text(
            "UPDATE import_columns SET column_order = NULL WHERE column_name = 'name'"
        )
    )

    _copy_after_import(local)

    assert _columns(remote) == _columns(local)


@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason="every value is turned into text and the text 'None' into NULL",
)
def test_copy_after_import_copies_the_text_none_as_it_is(
    local: sa.Connection, remote: sa.Connection
) -> None:
    local.execute(sa.text("UPDATE import_tables SET sqoop_sql_where_addition = 'None'"))

    _copy_after_import(local)

    assert _tables(remote) == _tables(local)


@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason="the connection the table had on the other instance is copied",
)
def test_copy_after_import_copies_the_connection_of_a_changed_table(
    local: sa.Connection, remote: sa.Connection
) -> None:
    _copy_after_import(local)
    _add_connection(local, "other_mysql")
    local.execute(sa.text("UPDATE import_tables SET dbalias = 'other_mysql'"))

    _copy_after_import(local)

    assert _connections(remote, SECRETS) == _connections(local, SECRETS)


def _add_dag(conn: sa.Connection, **values: object) -> None:
    insert_row(
        conn,
        "airflow_import_dags",
        **{
            "dag_name": DAG,
            "filter_hive": f"{HIVE_DB}.*",
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
        "airflow_import_dags",
        dag_name=DAG,
        filter_hive="None",
        schedule_interval="None",
    )


def _dags(conn: sa.Connection) -> list[dict[str, Any]]:
    return _rows(conn, "SELECT * FROM airflow_import_dags", {"auto_regenerate_dag"})


def _tasks(conn: sa.Connection) -> list[dict[str, Any]]:
    return _rows(conn, "SELECT * FROM airflow_tasks ORDER BY task_name", set())


def _copy_dag(**options: bool) -> None:
    copy_operations.Operation().copy_airflow_import_dag(
        airflow_dag_name=DAG, copy_destination=INSTANCE, **options
    )


def test_copy_airflow_import_dag_copies_the_dag_and_the_tables_of_its_filter(
    local: sa.Connection, remote: sa.Connection
) -> None:
    _add_dags(local, remote, schedule_interval="0 1 * * *", retries=3)
    _add_table(local, "other", hive_db="otherdb")

    _copy_dag()

    assert _dags(remote) == _dags(local)
    assert [row["hive_table"] for row in _tables(remote)] == [HIVE_TABLE]
    assert _columns(remote) == _columns(local)
    assert _connections(remote, SECRETS) == _connections(local, SECRETS)


@pytest.mark.parametrize(
    ("options", "state"),
    [
        ({}, (HIVE_TABLE, 1, 0)),
        ({"copy_dag_no_slave": True}, (HIVE_TABLE, 0, 0)),
        ({"deploy_mode": True}, (HIVE_TABLE, 0, 0)),
    ],
    ids=["copy", "no-slave", "deploy"],
)
def test_copy_airflow_import_dag_marks_the_tables_by_its_mode(
    local: sa.Connection,
    remote: sa.Connection,
    options: dict[str, bool],
    state: tuple[Any, ...],
) -> None:
    _add_dags(local, remote)

    _copy_dag(**options)

    assert _copy_state(remote) == [state]


@pytest.mark.parametrize("regenerate", [False, True])
def test_copy_airflow_import_dag_sets_auto_regenerate_on_request(
    local: sa.Connection, remote: sa.Connection, regenerate: bool
) -> None:
    _add_dags(local, remote, auto_regenerate_dag=0)

    _copy_dag(set_auto_regenerate_dag=regenerate)

    assert remote.execute(
        sa.text("SELECT auto_regenerate_dag FROM airflow_import_dags")
    ).scalar_one() == int(regenerate)


def test_copy_airflow_import_dag_copies_its_tasks_and_their_connection(
    local: sa.Connection, remote: sa.Connection
) -> None:
    _add_dags(local, remote)
    _add_connection(local, "task_mysql")
    for name in ("sql_01", "sql_02"):
        insert_row(
            local,
            "airflow_tasks",
            dag_name=DAG,
            task_name=name,
            task_type="JDBC SQL",
            placement="after main",
            jdbc_dbalias="task_mysql",
            airflow_priority=5,
            task_config="select 1",
        )

    _copy_dag()

    assert _tasks(remote) == _tasks(local)
    assert _connections(remote, SECRETS) == _connections(local, SECRETS)


@pytest.mark.xfail(
    strict=True,
    raises=sa.exc.OperationalError,
    reason="a task without a connection makes the copy add a connection without a name",
)
def test_copy_airflow_import_dag_copies_a_task_without_a_connection(
    local: sa.Connection, remote: sa.Connection
) -> None:
    _add_dags(local, remote)
    insert_row(
        local,
        "airflow_tasks",
        dag_name=DAG,
        task_name="shell_01",
        task_type="shell script",
        placement="before main",
        task_config="/usr/bin/sleep 1",
    )

    _copy_dag()

    assert _tasks(remote) == _tasks(local)


@pytest.mark.xfail(
    strict=True,
    raises=sa.exc.ProgrammingError,
    reason="an empty number of one task is NaN in the DataFrame of all tasks",
)
def test_copy_airflow_import_dag_copies_tasks_with_an_empty_priority(
    local: sa.Connection, remote: sa.Connection
) -> None:
    _add_dags(local, remote)
    _add_connection(local, "task_mysql")
    for name, priority in (("sql_01", 5), ("sql_02", None)):
        insert_row(
            local,
            "airflow_tasks",
            dag_name=DAG,
            task_name=name,
            task_type="JDBC SQL",
            placement="after main",
            jdbc_dbalias="task_mysql",
            airflow_priority=priority,
            task_config="select 1",
        )

    _copy_dag()

    assert _tasks(remote) == _tasks(local)


@pytest.mark.xfail(
    strict=True,
    raises=sa.exc.IntegrityError,
    reason="the new DAG row is added without schedule_interval, which is NOT NULL",
)
def test_copy_airflow_import_dag_creates_a_dag_missing_on_the_instance(
    local: sa.Connection, remote: sa.Connection
) -> None:
    _add_dag(local)

    _copy_dag()

    assert _dags(remote) == _dags(local)
