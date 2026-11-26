"""The local copy tables (dbimport_instances, copy_tables,
copy_async_status) as copy_operations reads and writes them. Hadoop
distcp is an external tool and is replaced; remote instances are not
used."""

import os
import subprocess
import sys
from collections.abc import Iterator
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest
import sqlalchemy as sa
from sqlalchemy.exc import NoResultFound
from sqlalchemy.orm import close_all_sessions

from dbimport.config_db import common_config, import_config
from dbimport.operations import common_operations, copy_operations
from tests.configdb import insert_row, set_driver_path

COPY = Path(__file__).resolve().parents[2] / "bin" / "copy"
HIVE_DB = "testdb"
HIVE_TABLE = "testtable"
DBALIAS = "test_mysql"
INSTANCE = "remote"
SOURCE_HDFS = "hdfs://local:8020"
SOURCE_BASEDIR = "/apps/local"
TARGET_HDFS = "hdfs://remote:8020"
TARGET_BASEDIR = "/apps/remote"
INCREMENTAL = {
    "import_phase_type": "incr",
    "etl_phase_type": "insert",
    "incr_mode": "append",
    "incr_column": "id",
}
HISTORY = {"etl_phase_type": "merge_history_audit"}


def _set_config(conn: sa.Connection, key: str, value: str) -> None:
    conn.execute(
        sa.text("UPDATE configuration SET valueStr = :value WHERE configKey = :key"),
        {"value": value, "key": key},
    )


def _add_instance(conn: sa.Connection, name: str = INSTANCE) -> None:
    insert_row(
        conn,
        "dbimport_instances",
        name=name,
        db_hostname="remote.example",
        db_port=3306,
        db_database="DBImport",
        hdfs_address=TARGET_HDFS,
        hdfs_basedir=TARGET_BASEDIR,
    )


def _add_copy(
    conn: sa.Connection,
    hive_filter: str,
    destination: str = INSTANCE,
    data_transfer: str = "Synchronous",
) -> None:
    insert_row(
        conn,
        "copy_tables",
        hive_filter=hive_filter,
        destination=destination,
        data_transfer=data_transfer,
    )


def _add_table(
    conn: sa.Connection,
    hive_db: str = HIVE_DB,
    hive_table: str = HIVE_TABLE,
    /,
    **values: object,
) -> int:
    insert_row(
        conn,
        "import_tables",
        hive_db=hive_db,
        hive_table=hive_table,
        dbalias=DBALIAS,
        source_schema="src",
        source_table=hive_table,
        **values,
    )
    return int(
        conn.execute(
            sa.text(
                "SELECT table_id FROM import_tables "
                "WHERE hive_db = :db AND hive_table = :table"
            ),
            {"db": hive_db, "table": hive_table},
        ).scalar_one()
    )


def _add_status(
    conn: sa.Connection,
    table_id: int,
    hive_db: str = HIVE_DB,
    hive_table: str = HIVE_TABLE,
    copy_status: int = 0,
) -> None:
    insert_row(
        conn,
        "copy_async_status",
        table_id=table_id,
        hive_db=hive_db,
        hive_table=hive_table,
        destination=INSTANCE,
        copy_status=copy_status,
        last_status_update=datetime(2026, 1, 2, 3, 4, 5),
        hdfs_source_path="old/source",
        hdfs_target_path="old/target",
    )


def _statuses(conn: sa.Connection) -> list[tuple[Any, ...]]:
    rows = conn.execute(
        sa.text(
            "SELECT table_id, hive_db, hive_table, destination, copy_status, "
            "failures, hdfs_source_path, hdfs_target_path "
            "FROM copy_async_status ORDER BY table_id, destination"
        )
    )
    return [tuple(row) for row in rows]


def _last_status_update(conn: sa.Connection, table_id: int) -> datetime:
    updated: datetime = conn.execute(
        sa.text(
            "SELECT last_status_update FROM copy_async_status WHERE table_id = :id"
        ),
        {"id": table_id},
    ).scalar_one()
    return updated


def _table_id(copy: copy_operations.Operation) -> int:
    table_id = copy.import_config.table_id
    assert table_id is not None
    return table_id


def _paths(hive_db: str = HIVE_DB, hive_table: str = HIVE_TABLE) -> tuple[str, str]:
    return (
        f"{SOURCE_HDFS}{SOURCE_BASEDIR}/{hive_db}/{hive_table}",
        f"{TARGET_HDFS}{TARGET_BASEDIR}/{hive_db}/{hive_table}",
    )


@pytest.fixture(autouse=True)
def _close_sessions() -> Iterator[None]:
    """copy_operations leaves its sessions open, and with them a
    transaction; a command ends them with its process, a test here."""
    yield
    close_all_sessions()


@pytest.fixture
def instance(
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
    _set_config(db_connection, "hdfs_address", SOURCE_HDFS)
    _set_config(db_connection, "hdfs_basedir", SOURCE_BASEDIR)
    _add_instance(db_connection)
    yield db_connection


def _copy(conn: sa.Connection, **values: object) -> copy_operations.Operation:
    """The copy operation of an import of HIVE_DB.HIVE_TABLE, as the import
    command builds it."""
    _add_table(conn, **values)
    config = import_config.Config(HIVE_DB, HIVE_TABLE)
    config.get_import_config()
    common_operations.Operation(HIVE_DB, HIVE_TABLE)
    return copy_operations.Operation()


@pytest.mark.parametrize("name", [INSTANCE, "nope"], ids=["known", "unknown"])
def test_check_dbimport_instance_exits_only_for_an_unknown_instance(
    instance: sa.Connection, name: str
) -> None:
    copy = copy_operations.Operation()

    if name == INSTANCE:
        assert copy.check_dbimport_instance(instance=name) is None
    else:
        with pytest.raises(SystemExit) as exit_info:
            copy.check_dbimport_instance(instance=name)
        assert exit_info.value.code == 1


def test_encrypt_user_password_stores_the_credentials_of_the_instance(
    instance: sa.Connection,
) -> None:
    copy = copy_operations.Operation()

    copy.encrypt_user_password(instance=INSTANCE, username="admin", password="pw")

    stored = instance.execute(
        sa.text("SELECT db_credentials FROM dbimport_instances WHERE name = :name"),
        {"name": INSTANCE},
    ).scalar_one()
    assert copy.crypto.decrypt(stored) == "admin pw"


def test_encrypt_user_password_of_an_unknown_instance_raises(
    instance: sa.Connection,
) -> None:
    copy = copy_operations.Operation()

    with pytest.raises(NoResultFound):
        copy.encrypt_user_password(instance="nope", username="admin", password="pw")


@pytest.mark.parametrize(
    ("copies", "expected"),
    [
        ([], None),
        ([("testdb.testtable", "Synchronous")], ["remote;Synchronous"]),
        ([("test*.*table", "Asynchronous")], ["remote;Asynchronous"]),
        ([("other.x;testdb.test*", "Synchronous")], ["remote;Synchronous"]),
        ([("testdb.other", "Synchronous")], None),
        ([("testdbtesttable", "Synchronous")], None),
        (
            [("testdb.*", "Synchronous"), ("*.testtable", "Asynchronous")],
            ["remote;Asynchronous"],
        ),
        (
            [("testdb.*", "Asynchronous"), ("*.testtable", "Synchronous")],
            ["remote;Asynchronous"],
        ),
        (
            [("testdb.*", "Synchronous"), ("*.testtable", "Synchronous")],
            ["remote;Synchronous"],
        ),
    ],
    ids=[
        "none",
        "exact",
        "wildcards",
        "second-filter",
        "no-match",
        "filter-without-dot",
        "async-after-sync",
        "sync-after-async",
        "sync-twice",
    ],
)
def test_get_copy_destinations_returns_the_matching_destinations(
    instance: sa.Connection,
    copies: list[tuple[str, str]],
    expected: list[str] | None,
) -> None:
    for hive_filter, data_transfer in copies:
        _add_copy(instance, hive_filter, data_transfer=data_transfer)
    copy = copy_operations.Operation()

    destinations = copy.get_copy_destinations(HIVE_DB, HIVE_TABLE)

    assert destinations == expected
    assert copy.copy_destinations == expected


def test_get_copy_destinations_returns_every_destination(
    instance: sa.Connection,
) -> None:
    _add_instance(instance, "second")
    _add_copy(instance, "testdb.*")
    _add_copy(
        instance, "*.testtable", destination="second", data_transfer="Asynchronous"
    )
    copy = copy_operations.Operation()

    destinations = copy.get_copy_destinations(HIVE_DB, HIVE_TABLE)

    assert sorted(destinations or []) == ["remote;Synchronous", "second;Asynchronous"]


def test_get_copy_destinations_with_a_semicolon_in_the_destination_exits(
    instance: sa.Connection,
) -> None:
    _add_instance(instance, "re;mote")
    _add_copy(instance, "other.x", destination="re;mote")
    copy = copy_operations.Operation()

    with pytest.raises(SystemExit) as exit_info:
        copy.get_copy_destinations(HIVE_DB, HIVE_TABLE)

    assert exit_info.value.code == 1


@pytest.mark.parametrize(
    ("values", "copy_status", "expected"),
    [
        ({}, None, True),
        ({}, 0, True),
        ({}, 1, False),
        ({}, 3, False),
        (INCREMENTAL, None, True),
        (INCREMENTAL, 0, False),
    ],
    ids=["full-none", "full-new", "full-started", "full-done", "incr-none", "incr-new"],
)
def test_is_previous_copy_completed_depends_on_the_status_row(
    instance: sa.Connection,
    values: dict[str, object],
    copy_status: int | None,
    expected: bool,
) -> None:
    _add_copy(instance, "testdb.*", data_transfer="Asynchronous")
    copy = _copy(instance, **values)
    copy.get_copy_destinations(HIVE_DB, HIVE_TABLE)
    if copy_status is not None:
        _add_status(instance, _table_id(copy), copy_status=copy_status)

    assert copy.is_previous_copy_completed() is expected


def test_is_previous_copy_completed_without_destinations_is_true(
    instance: sa.Connection,
) -> None:
    copy = _copy(instance)
    _add_status(instance, _table_id(copy), copy_status=1)

    assert copy.is_previous_copy_completed() is True


def test_copy_data_to_destinations_schedules_an_async_copy(
    instance: sa.Connection,
) -> None:
    _add_copy(instance, "testdb.*", data_transfer="Asynchronous")
    copy = _copy(instance)
    copy.get_copy_destinations(HIVE_DB, HIVE_TABLE)

    copy.copy_data_to_destinations()

    table_id = _table_id(copy)
    assert _statuses(instance) == [
        (table_id, HIVE_DB, HIVE_TABLE, INSTANCE, 0, 0, *_paths()),
    ]


def test_copy_data_to_destinations_replaces_a_copy_not_started(
    instance: sa.Connection,
) -> None:
    _add_copy(instance, "testdb.*", data_transfer="Asynchronous")
    copy = _copy(instance)
    copy.get_copy_destinations(HIVE_DB, HIVE_TABLE)
    table_id = _table_id(copy)
    _add_status(instance, table_id)

    copy.copy_data_to_destinations()

    assert _statuses(instance) == [
        (table_id, HIVE_DB, HIVE_TABLE, INSTANCE, 0, 0, *_paths()),
    ]
    assert _last_status_update(instance, table_id) > datetime(2026, 1, 2, 3, 4, 5)


@pytest.mark.parametrize(
    ("values", "copy_status"),
    [({}, 1), ({}, 2), (INCREMENTAL, 0), (HISTORY, 0)],
    ids=["started", "failed", "incremental", "history"],
)
def test_copy_data_to_destinations_with_a_pending_copy_exits(
    instance: sa.Connection, values: dict[str, object], copy_status: int
) -> None:
    _add_copy(instance, "testdb.*", data_transfer="Asynchronous")
    copy = _copy(instance, **values)
    copy.get_copy_destinations(HIVE_DB, HIVE_TABLE)
    table_id = _table_id(copy)
    _add_status(instance, table_id, copy_status=copy_status)

    with pytest.raises(SystemExit) as exit_info:
        copy.copy_data_to_destinations()

    assert exit_info.value.code == 1
    assert _statuses(instance) == [
        (
            table_id,
            HIVE_DB,
            HIVE_TABLE,
            INSTANCE,
            copy_status,
            0,
            "old/source",
            "old/target",
        ),
    ]


def test_copy_data_to_destinations_without_destinations_does_nothing(
    instance: sa.Connection,
) -> None:
    copy = _copy(instance)

    copy.copy_data_to_destinations()

    assert _statuses(instance) == []


def test_schedule_table_async_copy_schedules_the_matching_full_imports(
    instance: sa.Connection,
) -> None:
    first = _add_table(instance, HIVE_DB, "orders")
    second = _add_table(instance, HIVE_DB, "order_lines")
    _add_table(instance, HIVE_DB, "customers")
    _add_table(instance, "otherdb", "orders")
    copy = copy_operations.Operation()

    copy.schedule_table_async_copy(
        hive_filter_db="test*", hive_filter_table="order*", copy_destination=INSTANCE
    )

    assert _statuses(instance) == [
        (first, HIVE_DB, "orders", INSTANCE, 0, 0, *_paths(HIVE_DB, "orders")),
        (
            second,
            HIVE_DB,
            "order_lines",
            INSTANCE,
            0,
            0,
            *_paths(HIVE_DB, "order_lines"),
        ),
    ]


@pytest.mark.parametrize(
    ("options", "scheduled"),
    [({}, ["orders"]), ({"include_incr_imports": True}, ["orders", "events"])],
    ids=["full-only", "with-incremental"],
)
def test_schedule_table_async_copy_includes_incremental_imports_on_request(
    instance: sa.Connection, options: dict[str, bool], scheduled: list[str]
) -> None:
    ids = {
        "orders": _add_table(instance, HIVE_DB, "orders"),
        "events": _add_table(instance, HIVE_DB, "events", **INCREMENTAL),
    }
    copy = copy_operations.Operation()

    copy.schedule_table_async_copy(
        hive_filter_db=HIVE_DB,
        hive_filter_table="*",
        copy_destination=INSTANCE,
        **options,
    )

    assert _statuses(instance) == sorted(
        (ids[name], HIVE_DB, name, INSTANCE, 0, 0, *_paths(HIVE_DB, name))
        for name in scheduled
    )


def test_copy_schedule_table_async_copy_passes_include_incr_imports(
    instance: sa.Connection,
) -> None:
    table_id = _add_table(instance, HIVE_DB, "events", **INCREMENTAL)

    result = subprocess.run(
        [
            sys.executable,
            str(COPY),
            "--scheduleTableAsyncCopy",
            f"--hiveDB={HIVE_DB}",
            "--hiveTable=*",
            f"--destination={INSTANCE}",
            "--includeIncrImports",
        ],
        capture_output=True,
        text=True,
        env=os.environ.copy(),
        timeout=120,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert _statuses(instance) == [
        (table_id, HIVE_DB, "events", INSTANCE, 0, 0, *_paths(HIVE_DB, "events")),
    ]


def test_schedule_table_async_copy_with_a_scheduled_match_exits(
    instance: sa.Connection,
) -> None:
    table_id = _add_table(instance)
    _add_table(instance, HIVE_DB, "other")
    _add_status(instance, table_id)
    copy = copy_operations.Operation()

    with pytest.raises(SystemExit) as exit_info:
        copy.schedule_table_async_copy(
            hive_filter_db=HIVE_DB, hive_filter_table="*", copy_destination=INSTANCE
        )

    assert exit_info.value.code == 1
    assert [row[0] for row in _statuses(instance)] == [table_id]


def test_schedule_table_async_copy_to_an_unknown_instance_exits(
    instance: sa.Connection,
) -> None:
    _add_table(instance)
    copy = copy_operations.Operation()

    with pytest.raises(SystemExit) as exit_info:
        copy.schedule_table_async_copy(
            hive_filter_db=HIVE_DB, hive_filter_table="*", copy_destination="nope"
        )

    assert exit_info.value.code == 1
    assert _statuses(instance) == []


def _add_dag(conn: sa.Connection, filter_hive: str) -> None:
    insert_row(
        conn,
        "airflow_import_dags",
        dag_name="dag_a",
        filter_hive=filter_hive,
        schedule_interval="None",
    )


@pytest.mark.parametrize(
    ("include_incr_imports", "scheduled"),
    [(False, ["orders"]), (True, ["orders", "events"])],
    ids=["full-only", "with-incremental"],
)
def test_schedule_dag_async_copy_schedules_the_tables_of_the_dag(
    instance: sa.Connection, include_incr_imports: bool, scheduled: list[str]
) -> None:
    ids = {
        "orders": _add_table(instance, HIVE_DB, "orders"),
        "events": _add_table(instance, "otherdb", "events", **INCREMENTAL),
    }
    _add_table(instance, HIVE_DB, "customers")
    _add_dag(instance, "testdb.ord*;otherdb.*")
    copy = copy_operations.Operation()

    copy.schedule_dag_async_copy(
        airflow_dag_name="dag_a",
        copy_destination=INSTANCE,
        include_incr_imports=include_incr_imports,
    )

    dbs = {"orders": HIVE_DB, "events": "otherdb"}
    assert _statuses(instance) == sorted(
        (ids[name], dbs[name], name, INSTANCE, 0, 0, *_paths(dbs[name], name))
        for name in scheduled
    )


def test_schedule_dag_async_copy_with_a_scheduled_match_exits(
    instance: sa.Connection,
) -> None:
    table_id = _add_table(instance)
    _add_table(instance, HIVE_DB, "other")
    _add_dag(instance, "testdb.*")
    _add_status(instance, table_id)
    copy = copy_operations.Operation()

    with pytest.raises(SystemExit) as exit_info:
        copy.schedule_dag_async_copy(
            airflow_dag_name="dag_a",
            copy_destination=INSTANCE,
            include_incr_imports=False,
        )

    assert exit_info.value.code == 1
    assert [row[0] for row in _statuses(instance)] == [table_id]


def test_schedule_dag_async_copy_of_an_unknown_dag_raises(
    instance: sa.Connection,
) -> None:
    copy = copy_operations.Operation()

    with pytest.raises(NoResultFound):
        copy.schedule_dag_async_copy(
            airflow_dag_name="nope",
            copy_destination=INSTANCE,
            include_incr_imports=False,
        )


class _FinishedProcess:
    """A distcp run that ends at once without output."""

    def __init__(self, command: list[str], **_kwargs: object) -> None:
        self.command = command
        self.stdout = self

    def poll(self) -> int:
        return 0

    def readlines(self) -> list[bytes]:
        return []


@pytest.mark.parametrize(
    ("from_cluster_mode", "source", "target"),
    [
        (
            True,
            f"{TARGET_HDFS}/remote/path/{HIVE_DB}/{HIVE_TABLE}",
            f"{SOURCE_HDFS}/local/path/{HIVE_DB}/{HIVE_TABLE}",
        ),
        (
            False,
            f"{SOURCE_HDFS}/local/path/{HIVE_DB}/{HIVE_TABLE}",
            f"{TARGET_HDFS}/remote/path/{HIVE_DB}/{HIVE_TABLE}",
        ),
    ],
    ids=["from-cluster", "to-cluster"],
)
def test_copy_table_to_from_cluster_runs_distcp_with_the_instance_address(
    instance: sa.Connection,
    monkeypatch: pytest.MonkeyPatch,
    from_cluster_mode: bool,
    source: str,
    target: str,
) -> None:
    started: list[_FinishedProcess] = []

    def popen(command: list[str], **kwargs: object) -> _FinishedProcess:
        started.append(_FinishedProcess(command, **kwargs))
        return started[-1]

    monkeypatch.setattr(subprocess, "Popen", popen)
    copy = copy_operations.Operation()

    copy.copy_table_to_from_cluster(
        remote_instance=INSTANCE,
        local_hdfs_path="/local/path",
        remote_hdfs_path="/remote/path",
        hive_db=HIVE_DB,
        hive_table=HIVE_TABLE,
        from_cluster_mode=from_cluster_mode,
    )

    assert [process.command[-2:] for process in started] == [[source, target]]


def test_copy_table_to_from_cluster_to_an_unknown_instance_exits(
    instance: sa.Connection,
) -> None:
    copy = copy_operations.Operation()

    with pytest.raises(SystemExit) as exit_info:
        copy.copy_table_to_from_cluster(
            remote_instance="nope",
            local_hdfs_path="/local/path",
            remote_hdfs_path="/remote/path",
            hive_db=HIVE_DB,
            hive_table=HIVE_TABLE,
            from_cluster_mode=False,
        )

    assert exit_info.value.code == 1
