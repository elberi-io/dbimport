from collections.abc import Iterator
from datetime import datetime
from typing import Any

import pytest
import sqlalchemy as sa

from dbimport.config_db import common_config, export_stage
from tests.configdb import close_dbimport_connections, insert_row

DBALIAS = "test_mysql"
SCHEMA = "dbo"
TABLE = "orders"
KEY = {"dbalias": DBALIAS, "target_schema": SCHEMA, "target_table": TABLE}
EARLIER = datetime(2000, 1, 1, 10, 0, 0)


@pytest.fixture
def stage(clean_config_db: None) -> Iterator[export_stage.Stage]:
    """Stage handler for the export table, as export_config creates it."""
    config = common_config.Config()
    stage = export_stage.Stage(config.mysql_conn, DBALIAS, SCHEMA, TABLE)
    yield stage
    close_dbimport_connections(stage.common_config)
    close_dbimport_connections(config)


def _stored(conn: sa.Connection, table: str, *columns: str) -> list[tuple[Any, ...]]:
    return [
        tuple(row)
        for row in conn.execute(
            sa.text(
                f"SELECT {', '.join(columns)} FROM {table} "
                "WHERE dbalias = :dbalias AND target_schema = :target_schema "
                "AND target_table = :target_table ORDER BY stage"
            ),
            KEY,
        )
    ]


def _stages(conn: sa.Connection, table: str) -> list[int]:
    return [int(row[0]) for row in _stored(conn, table, "stage")]


def _add_stage_statistics(conn: sa.Connection, stage: int) -> None:
    insert_row(
        conn,
        "export_stage_statistics",
        **KEY,
        stage=stage,
        start=EARLIER,
        stop=datetime(2000, 1, 1, 10, 0, 5),
        duration=5,
    )


def test_get_stage_without_stored_stage_returns_zero(
    stage: export_stage.Stage,
) -> None:
    assert stage.get_stage() == 0


def test_get_stage_returns_the_stored_stage(
    stage: export_stage.Stage, db_connection: sa.Connection
) -> None:
    insert_row(
        db_connection,
        "export_stage",
        **KEY,
        stage=105,
        stage_description="",
    )

    assert stage.get_stage() == 105


def test_set_stage_stores_the_stage_its_description_and_time(
    stage: export_stage.Stage, db_connection: sa.Connection
) -> None:
    stage.get_stage()

    stage.set_stage(100)

    [(stage_id, description, stage_time)] = _stored(
        db_connection, "export_stage", "stage", "stage_description", "stage_time"
    )
    assert (stage_id, description) == (100, "Getting Hive tableschema")
    assert stage_time > EARLIER


def test_set_stage_lower_than_current_stage_is_ignored(
    stage: export_stage.Stage, db_connection: sa.Connection
) -> None:
    stage.get_stage()
    stage.set_stage(101)

    stage.set_stage(100)

    assert _stages(db_connection, "export_stage") == [101]


def test_set_stage_with_force_allows_lower_stage(
    stage: export_stage.Stage, db_connection: sa.Connection
) -> None:
    stage.get_stage()
    stage.set_stage(101)

    stage.set_stage(100, force=True)

    assert _stages(db_connection, "export_stage") == [100]


def test_set_stage_records_statistics_for_the_stage_it_leaves(
    stage: export_stage.Stage, db_connection: sa.Connection
) -> None:
    stage.get_stage()

    stage.set_stage(100)
    stage.set_stage(101)

    assert _stages(db_connection, "export_stage_statistics") == [0, 100]


def test_set_stage_replaces_the_statistics_of_an_earlier_attempt(
    stage: export_stage.Stage, db_connection: sa.Connection
) -> None:
    _add_stage_statistics(db_connection, 100)
    insert_row(
        db_connection,
        "export_stage",
        **KEY,
        stage=100,
        stage_description="Getting Hive tableschema",
    )
    stage.get_stage()

    stage.set_stage(101)

    [(stage_id, start, stop, duration)] = _stored(
        db_connection, "export_stage_statistics", "stage", "start", "stop", "duration"
    )
    assert stage_id == 100
    assert start > EARLIER and stop >= start
    assert duration == 0
    assert _stored(db_connection, "export_stage", "stage", "stage_description") == [
        (101, "Clear table rowcount")
    ]


def test_set_stage_in_memory_mode_writes_nothing(
    stage: export_stage.Stage, db_connection: sa.Connection
) -> None:
    stage.set_stage_only_in_memory()
    stage.get_stage()

    stage.set_stage(100)

    assert stage.get_stage() == 100
    assert _stages(db_connection, "export_stage") == []
    assert _stages(db_connection, "export_stage_statistics") == []


def test_save_retry_attempt_logs_the_stage_its_description_and_time(
    stage: export_stage.Stage, db_connection: sa.Connection
) -> None:
    stage.save_retry_attempt(100)

    [(stage_id, description, retry_time)] = _stored(
        db_connection,
        "export_retries_log",
        "stage",
        "stage_description",
        "retry_time",
    )
    assert (stage_id, description) == (100, "Getting Hive tableschema")
    assert retry_time > EARLIER


def test_save_retry_attempt_in_memory_mode_writes_nothing(
    stage: export_stage.Stage, db_connection: sa.Connection
) -> None:
    stage.set_stage_only_in_memory()

    stage.save_retry_attempt(100)

    assert _stages(db_connection, "export_retries_log") == []


def test_clear_stage_removes_stage_and_statistics(
    stage: export_stage.Stage, db_connection: sa.Connection
) -> None:
    stage.get_stage()
    stage.set_stage(100)
    stage.set_stage(101)

    stage.clear_stage()

    assert _stages(db_connection, "export_stage") == []
    assert _stages(db_connection, "export_stage_statistics") == []


def test_clear_stage_in_memory_mode_keeps_the_stored_stage(
    stage: export_stage.Stage, db_connection: sa.Connection
) -> None:
    stage.get_stage()
    stage.set_stage(100)
    stage.set_stage_only_in_memory()

    stage.clear_stage()

    assert _stages(db_connection, "export_stage") == [100]
    assert _stages(db_connection, "export_stage_statistics") == [0]


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        (
            "get_hive_tableschema",
            {
                "startTime": "2000-01-01T10:00:00.000000Z",
                "stopTime": "2000-01-01T10:00:05.000000Z",
            },
        ),
        (
            "clear_table_rowcount",
            {
                "startTime": "1970-01-01T00:00:00.000000Z",
                "stopTime": "1970-01-01T00:00:00.000000Z",
            },
        ),
    ],
)
def test_get_stage_start_stop_returns_the_times_of_the_named_stage(
    stage: export_stage.Stage,
    db_connection: sa.Connection,
    name: str,
    expected: dict[str, str],
) -> None:
    _add_stage_statistics(db_connection, 100)

    assert stage.get_stage_start_stop(name) == expected


def test_get_stage_start_stop_ignores_a_zero_date_of_another_stage(
    stage: export_stage.Stage, db_connection: sa.Connection
) -> None:
    _add_stage_statistics(db_connection, 100)
    _add_stage_statistics(db_connection, 101)
    db_connection.execute(
        sa.text(
            "SET STATEMENT sql_mode = '' FOR UPDATE export_stage_statistics"
            " SET start = '0000-00-00 00:00:00' WHERE stage = 101"
        )
    )

    assert stage.get_stage_start_stop("get_hive_tableschema") == {
        "startTime": "2000-01-01T10:00:00.000000Z",
        "stopTime": "2000-01-01T10:00:05.000000Z",
    }
