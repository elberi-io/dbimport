import json
from collections.abc import Iterator
from datetime import datetime, timedelta
from typing import Any

import pytest
import sqlalchemy as sa

from dbimport.config_db import import_config, import_stage
from tests.clock import NOW, STORED_NOW, frozen_datetime
from tests.configdb import close_dbimport_connections, insert_row

HIVE_DB = "testdb"
HIVE_TABLE = "testtable"
EARLIER = datetime(2000, 1, 1, 10, 0, 0)


@pytest.fixture
def stage(clean_config_db: None) -> import_stage.Stage:
    """Stage handler for HIVE_DB.HIVE_TABLE, as import_config creates it."""
    return import_stage.Stage(HIVE_DB, HIVE_TABLE)


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


def _add_stage_statistics(
    conn: sa.Connection, stage: int, start: datetime = EARLIER, seconds: int = 5
) -> None:
    insert_row(
        conn,
        "import_stage_statistics",
        hive_db=HIVE_DB,
        hive_table=HIVE_TABLE,
        stage=stage,
        start=start,
        stop=start + timedelta(seconds=seconds),
        duration=seconds,
    )


def test_get_stage_returns_the_stored_stage(
    stage: import_stage.Stage, db_connection: sa.Connection
) -> None:
    stage.get_stage()
    stage.set_stage(1010)
    other = import_stage.Stage(HIVE_DB, HIVE_TABLE)

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


def test_set_stage_stores_the_stage_time_in_utc(
    stage: import_stage.Stage,
    db_connection: sa.Connection,
    in_host_zone: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(import_stage, "datetime", frozen_datetime(NOW))
    stage.get_stage()

    stage.set_stage(1010)

    assert _stored(db_connection, "import_stage", "stage_time") == [(STORED_NOW,)]


def test_get_stage_start_stop_returns_the_stage_times_in_utc(
    stage: import_stage.Stage,
    db_connection: sa.Connection,
    in_host_zone: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(import_stage, "datetime", frozen_datetime(NOW))
    stage.get_stage()
    stage.set_stage(1010)
    stage.set_stage(1011)

    assert stage.get_stage_start_stop("get_source_tableschema") == {
        "startTime": "2026-06-01T10:00:00.000000Z",
        "stopTime": "2026-06-01T10:00:00.000000Z",
    }


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


IMPORT_RUN: dict[str, object] = {
    "hive_db": HIVE_DB,
    "hive_table": HIVE_TABLE,
    "import_phase": "full",
    "copy_phase": "none",
    "etl_phase": "truncate_insert",
    "incremental": False,
    "dbalias": "test_mysql",
    "source_database": "sourcedb",
    "source_schema": "src",
    "source_table": "orders",
    "size": 1024,
    "rows": 10,
    "sessions": 2,
}
STATISTICS_COLUMNS = (
    "import_phase",
    "incremental",
    "size",
    "`rows`",
    "sessions",
    "get_source_tableschema_start",
    "get_source_tableschema_stop",
    "get_source_tableschema_duration",
    "clear_table_rowcount_duration",
    "start",
    "stop",
    "duration",
)


def _add_run(conn: sa.Connection, *stages: int) -> None:
    """Stage times one after the other, from EARLIER: 5, 15 and 40 s."""
    start = EARLIER
    for stage, seconds in zip(stages, (5, 15, 40), strict=False):
        _add_stage_statistics(conn, stage, start, seconds)
        start += timedelta(seconds=seconds)


def _statistics(conn: sa.Connection, table: str) -> list[tuple[Any, ...]]:
    return [
        tuple(row)
        for row in conn.execute(
            sa.text(f"SELECT {', '.join(STATISTICS_COLUMNS)} FROM {table}")
        )
    ]


def _add_last_statistics(conn: sa.Connection) -> None:
    insert_row(
        conn,
        "import_statistics_last",
        hive_db=HIVE_DB,
        hive_table=HIVE_TABLE,
        size=1,
    )


def test_save_stage_statistics_stores_the_run_with_its_stage_times(
    stage: import_stage.Stage, db_connection: sa.Connection
) -> None:
    _add_run(db_connection, 0, 1010, 1011)
    _add_last_statistics(db_connection)

    stage.save_stage_statistics(**IMPORT_RUN)

    expected = (
        "full",
        0,
        1024,
        10,
        2,
        datetime(2000, 1, 1, 10, 0, 5),
        datetime(2000, 1, 1, 10, 0, 20),
        15,
        40,
        EARLIER,
        datetime(2000, 1, 1, 10, 1, 0),
        60,
    )
    assert _statistics(db_connection, "import_statistics") == [expected]
    assert _statistics(db_connection, "import_statistics_last") == [expected]


def test_save_stage_statistics_without_stage_zero_stores_no_start(
    stage: import_stage.Stage, db_connection: sa.Connection
) -> None:
    _add_run(db_connection, 1010, 1011)

    stage.save_stage_statistics(**IMPORT_RUN)

    [row] = _statistics(db_connection, "import_statistics")
    assert row[-3:] == (None, datetime(2000, 1, 1, 10, 0, 20), 0)


def test_save_stage_statistics_with_duplicate_stage_columns_saves_nothing(
    stage: import_stage.Stage, db_connection: sa.Connection
) -> None:
    _add_run(db_connection, 0, 1010, 1110)
    _add_last_statistics(db_connection)

    stage.save_stage_statistics(**IMPORT_RUN)

    assert _statistics(db_connection, "import_statistics") == []
    assert _statistics(db_connection, "import_statistics_last") == []


class _FakeSendStatistics:
    """Records the documents instead of posting them (external services)."""

    def __init__(self, *, succeed: bool) -> None:
        self.succeed = succeed
        self.sent: dict[str, dict[str, Any]] = {}

    def send_rest_data(self, data: str) -> int:
        self.sent["rest"] = json.loads(data)
        return 200 if self.succeed else 500

    def publish_kafka_data(self, data: str) -> bool:
        self.sent["kafka"] = json.loads(data)
        return self.succeed

    def send_aws_sns_data(self, data: str) -> bool:
        self.sent["awssns"] = json.loads(data)
        return self.succeed


def _enable(conn: sa.Connection, *keys: str) -> None:
    for key in keys:
        conn.execute(
            sa.text("UPDATE configuration SET valueInt = 1 WHERE configKey = :key"),
            {"key": key},
        )


IMPORT_JSON: dict[str, object] = {
    "database": HIVE_DB,
    "table": HIVE_TABLE,
    "history_database": None,
    "history_table": None,
    "import_phase": "full",
    "copy_phase": "none",
    "etl_phase": "truncate_insert",
    "incremental": False,
    "history": False,
    "source_database": "sourcedb",
    "source_schema": "src",
    "source_table": "orders",
    "size": 1024,
    "rows": 10,
    "sessions": 2,
}


def test_convert_stage_statistics_to_json_posts_the_extended_document(
    stage: import_stage.Stage,
    db_connection: sa.Connection,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _add_run(db_connection, 0, 1010)
    _enable(db_connection, "post_data_to_rest", "post_data_to_rest_extended")
    fake = _FakeSendStatistics(succeed=True)
    monkeypatch.setattr(stage, "send_statistics", fake)

    stage.convert_stage_statistics_to_json(**IMPORT_JSON)

    assert fake.sent == {
        "rest": {
            "type": "import",
            "status": "finished",
            **IMPORT_JSON,
            "get_source_tableschema_start": "2000-01-01 10:00:05",
            "get_source_tableschema_stop": "2000-01-01 10:00:20",
            "get_source_tableschema_duration": 15,
            "start": "2000-01-01 10:00:00",
            "stop": "2000-01-01 10:00:20",
            "duration": 20,
        }
    }


def test_convert_stage_statistics_to_json_sends_the_times_of_the_host_zone(
    stage: import_stage.Stage,
    db_connection: sa.Connection,
    in_host_zone: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(import_stage, "datetime", frozen_datetime(NOW))
    stage.get_stage()
    stage.set_stage(1010)
    stage.set_stage(1011)
    _enable(db_connection, "post_data_to_rest")
    fake = _FakeSendStatistics(succeed=True)
    monkeypatch.setattr(stage, "send_statistics", fake)

    stage.convert_stage_statistics_to_json(**IMPORT_JSON)

    sent = fake.sent["rest"]
    assert (sent["start"], sent["stop"]) == ("2026-06-01 12:00:00",) * 2


def test_convert_stage_statistics_to_json_saves_a_failed_document(
    stage: import_stage.Stage,
    db_connection: sa.Connection,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _add_run(db_connection, 0, 1010)
    _enable(db_connection, "post_data_to_kafka")
    fake = _FakeSendStatistics(succeed=False)
    monkeypatch.setattr(stage, "send_statistics", fake)

    stage.convert_stage_statistics_to_json(**IMPORT_JSON)

    sent = fake.sent["kafka"]
    assert "sessions" not in sent and "copy_phase" not in sent
    assert "get_source_tableschema_start" not in sent
    stored = db_connection.execute(
        sa.text("SELECT type, status, destination, jsondata FROM json_to_send")
    ).all()
    assert [(*row[:3], json.loads(row[3])) for row in stored] == [
        ("import_statistics", 0, "kafka", sent)
    ]


def test_convert_stage_statistics_to_json_without_destinations_posts_nothing(
    stage: import_stage.Stage,
    db_connection: sa.Connection,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _add_run(db_connection, 0, 1010)
    fake = _FakeSendStatistics(succeed=True)
    monkeypatch.setattr(stage, "send_statistics", fake)

    stage.convert_stage_statistics_to_json(**IMPORT_JSON)

    assert fake.sent == {}
