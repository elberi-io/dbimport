from collections.abc import Iterator
from datetime import datetime
from typing import Any

import pytest
import sqlalchemy as sa

from dbimport.config_db import common_config, import_config, import_stage
from tests.configdb import close_dbimport_connections, insert_row

HIVE_DB = "testdb"
HIVE_TABLE = "testtable"
EARLIER = datetime(2000, 1, 1, 10, 0, 0)


@pytest.fixture
def stage(clean_config_db: None) -> import_stage.Stage:
    """Stage handler for HIVE_DB.HIVE_TABLE, as import_config creates it."""
    conn = common_config.Config(HIVE_DB, HIVE_TABLE).mysql_conn
    return import_stage.Stage(conn, HIVE_DB, HIVE_TABLE)


def _persisted_stage(conn: sa.Connection) -> int | None:
    value = conn.execute(
        sa.text(
            "SELECT stage FROM import_stage WHERE hive_db = :db AND hive_table = :table"
        ),
        {"db": HIVE_DB, "table": HIVE_TABLE},
    ).scalar_one_or_none()
    return None if value is None else int(value)


def _rows(conn: sa.Connection, table: str) -> list[int]:
    return sorted(
        int(stage)
        for stage in conn.execute(
            sa.text(
                f"SELECT stage FROM {table} WHERE hive_db = :db AND hive_table = :table"
            ),
            {"db": HIVE_DB, "table": HIVE_TABLE},
        ).scalars()
    )


def test_get_stage_without_stored_stage_returns_zero(
    stage: import_stage.Stage,
) -> None:
    assert stage.get_stage() == 0


def test_set_stage_persists_stage(
    stage: import_stage.Stage, db_connection: sa.Connection
) -> None:
    stage.get_stage()

    stage.set_stage(1010)

    assert _persisted_stage(db_connection) == 1010


def test_set_stage_lower_than_current_stage_is_ignored(
    stage: import_stage.Stage, db_connection: sa.Connection
) -> None:
    stage.get_stage()
    stage.set_stage(1020)

    stage.set_stage(1010)

    assert _persisted_stage(db_connection) == 1020


def test_set_stage_with_force_allows_lower_stage(
    stage: import_stage.Stage, db_connection: sa.Connection
) -> None:
    stage.get_stage()
    stage.set_stage(1020)

    stage.set_stage(1010, force=True)

    assert _persisted_stage(db_connection) == 1010


def test_set_stage_records_statistics_for_the_stage_it_leaves(
    stage: import_stage.Stage, db_connection: sa.Connection
) -> None:
    stage.get_stage()

    stage.set_stage(1010)
    stage.set_stage(1011)

    assert _rows(db_connection, "import_stage_statistics") == [0, 1010]


def test_set_stage_in_memory_mode_writes_nothing(
    stage: import_stage.Stage, db_connection: sa.Connection
) -> None:
    stage.set_stage_only_in_memory()
    stage.get_stage()

    stage.set_stage(1010)

    assert stage.get_stage() == 1010
    assert _persisted_stage(db_connection) is None
    assert _rows(db_connection, "import_stage_statistics") == []


def test_save_retry_attempt_logs_the_stage(
    stage: import_stage.Stage, db_connection: sa.Connection
) -> None:
    stage.save_retry_attempt(1013)

    assert _rows(db_connection, "import_retries_log") == [1013]


def test_clear_stage_removes_stage_and_statistics(
    stage: import_stage.Stage, db_connection: sa.Connection
) -> None:
    stage.get_stage()
    stage.set_stage(1010)
    stage.set_stage(1011)

    stage.clear_stage()

    assert _persisted_stage(db_connection) is None
    assert _rows(db_connection, "import_stage_statistics") == []


def _stored(conn: sa.Connection, table: str, *columns: str) -> list[tuple[Any, ...]]:
    return [
        tuple(row)
        for row in conn.execute(
            sa.text(
                f"SELECT {', '.join(columns)} FROM {table} "
                "WHERE hive_db = :db AND hive_table = :table ORDER BY stage"
            ),
            {"db": HIVE_DB, "table": HIVE_TABLE},
        )
    ]


def _add_stage_statistics(conn: sa.Connection, stage: int) -> None:
    insert_row(
        conn,
        "import_stage_statistics",
        hive_db=HIVE_DB,
        hive_table=HIVE_TABLE,
        stage=stage,
        start=EARLIER,
        stop=datetime(2000, 1, 1, 10, 0, 5),
        duration=5,
    )


def test_get_stage_returns_the_stored_stage(
    stage: import_stage.Stage, db_connection: sa.Connection
) -> None:
    stage.get_stage()
    stage.set_stage(1010)
    other = import_stage.Stage(stage.mysql_conn, HIVE_DB, HIVE_TABLE)

    assert other.get_stage() == 1010


def test_set_stage_stores_the_description_and_time_of_the_new_stage(
    stage: import_stage.Stage, db_connection: sa.Connection
) -> None:
    stage.get_stage()

    stage.set_stage(1010)

    [(description, stage_time)] = _stored(
        db_connection, "import_stage", "stage_description", "stage_time"
    )
    assert description == "Getting source tableschema"
    assert isinstance(stage_time, datetime) and stage_time > EARLIER


def test_set_stage_replaces_the_statistics_of_an_earlier_attempt(
    stage: import_stage.Stage, db_connection: sa.Connection
) -> None:
    _add_stage_statistics(db_connection, 1010)
    insert_row(
        db_connection,
        "import_stage",
        hive_db=HIVE_DB,
        hive_table=HIVE_TABLE,
        stage=1010,
        stage_description="Getting source tableschema",
    )
    stage.get_stage()

    stage.set_stage(1011)

    [(stage_id, start, stop, duration)] = _stored(
        db_connection, "import_stage_statistics", "stage", "start", "stop", "duration"
    )
    assert stage_id == 1010
    assert start > EARLIER and stop >= start
    assert duration == 0
    assert _stored(db_connection, "import_stage", "stage", "stage_description") == [
        (1011, "Clear table rowcount")
    ]


def test_save_retry_attempt_stores_the_stage_description_and_time(
    stage: import_stage.Stage, db_connection: sa.Connection
) -> None:
    stage.save_retry_attempt(1010)

    [(description, retry_time)] = _stored(
        db_connection, "import_retries_log", "stage_description", "retry_time"
    )
    assert description == "Getting source tableschema"
    assert retry_time > EARLIER


def test_save_retry_attempt_in_memory_mode_writes_nothing(
    stage: import_stage.Stage, db_connection: sa.Connection
) -> None:
    stage.set_stage_only_in_memory()

    stage.save_retry_attempt(1010)

    assert _rows(db_connection, "import_retries_log") == []


def test_clear_stage_in_memory_mode_keeps_the_stored_stage(
    stage: import_stage.Stage, db_connection: sa.Connection
) -> None:
    stage.get_stage()
    stage.set_stage(1010)
    stage.set_stage_only_in_memory()

    stage.clear_stage()

    assert _persisted_stage(db_connection) == 1010
    assert _rows(db_connection, "import_stage_statistics") == [0]


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        (
            "get_source_tableschema",
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
    stage: import_stage.Stage,
    db_connection: sa.Connection,
    name: str,
    expected: dict[str, str],
) -> None:
    _add_stage_statistics(db_connection, 1010)

    assert stage.get_stage_start_stop(name) == expected


def _add_table_in_stage(
    conn: sa.Connection, table: str, stage: int, **values: object
) -> None:
    insert_row(
        conn,
        "import_tables",
        hive_db=HIVE_DB,
        hive_table=table,
        dbalias="test_mysql",
        source_schema="src",
        source_table=table,
        **values,
    )
    insert_row(
        conn,
        "import_stage",
        hive_db=HIVE_DB,
        hive_table=table,
        stage=stage,
        stage_description="",
    )


@pytest.fixture
def config(clean_config_db: None) -> Iterator[import_config.Config]:
    config = import_config.Config(HIVE_DB, HIVE_TABLE)
    yield config
    close_dbimport_connections(config)
    close_dbimport_connections(config.common_config)


def test_get_all_active_incr_imports_lists_incremental_tables_with_a_stage(
    config: import_config.Config, db_connection: sa.Connection
) -> None:
    _add_table_in_stage(db_connection, "phase_incr", 1110, import_phase_type="incr")
    _add_table_in_stage(db_connection, "legacy_incr", 1120, import_type="incr")
    _add_table_in_stage(
        db_connection, "full", 1010, import_type="full", import_phase_type="full"
    )
    insert_row(
        db_connection,
        "import_tables",
        hive_db=HIVE_DB,
        hive_table="incr_without_stage",
        dbalias="test_mysql",
        source_schema="src",
        source_table="incr_without_stage",
        import_phase_type="incr",
    )

    result = config.get_all_active_incr_imports()

    assert list(result.columns) == ["hive_db", "hive_table", "stage"]
    assert sorted(result.itertuples(index=False, name=None)) == [
        (HIVE_DB, "legacy_incr", 1120),
        (HIVE_DB, "phase_incr", 1110),
    ]


def test_get_all_active_incr_imports_without_any_returns_an_empty_frame(
    config: import_config.Config,
) -> None:
    result = config.get_all_active_incr_imports()

    assert result.empty
    assert list(result.columns) == []


def test_get_stage_start_stop_ignores_a_zero_date_of_another_stage(
    stage: import_stage.Stage, db_connection: sa.Connection
) -> None:
    _add_stage_statistics(db_connection, 1010)
    _add_stage_statistics(db_connection, 1011)
    db_connection.execute(
        sa.text(
            "SET STATEMENT sql_mode = '' FOR UPDATE import_stage_statistics"
            " SET start = '0000-00-00 00:00:00' WHERE stage = 1011"
        )
    )

    assert stage.get_stage_start_stop("get_source_tableschema") == {
        "startTime": "2000-01-01T10:00:00.000000Z",
        "stopTime": "2000-01-01T10:00:05.000000Z",
    }
