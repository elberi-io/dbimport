import json
from collections.abc import Iterator
from datetime import datetime, timedelta
from typing import Any

import pytest
import sqlalchemy as sa

from dbimport.config_db import export_stage
from tests.configdb import close_dbimport_connections, insert_row

DBALIAS = "test_mysql"
SCHEMA = "dbo"
TABLE = "orders"
KEY = {"dbalias": DBALIAS, "target_schema": SCHEMA, "target_table": TABLE}
EARLIER = datetime(2000, 1, 1, 10, 0, 0)


@pytest.fixture
def stage(clean_config_db: None) -> Iterator[export_stage.Stage]:
    """Stage handler for the export table, as export_config creates it."""
    stage = export_stage.Stage(DBALIAS, SCHEMA, TABLE)
    yield stage
    close_dbimport_connections(stage.common_config)


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


def _add_stage_statistics(
    conn: sa.Connection, stage: int, start: datetime = EARLIER, seconds: int = 5
) -> None:
    insert_row(
        conn,
        "export_stage_statistics",
        **KEY,
        stage=stage,
        start=start,
        stop=start + timedelta(seconds=seconds),
        duration=seconds,
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


EXPORT_RUN: dict[str, object] = {
    "dbalias": DBALIAS,
    "target_database": "targetdb",
    "target_schema": SCHEMA,
    "target_table": TABLE,
    "hive_db": "hivedb",
    "hive_table": "orders",
    "export_phase": "full",
    "incremental": False,
    "size": 1024,
    "rows": 10,
    "sessions": 2,
}
STATISTICS_COLUMNS = (
    "export_phase",
    "incremental",
    "size",
    "`rows`",
    "sessions",
    "get_hive_tableschema_start",
    "get_hive_tableschema_stop",
    "get_hive_tableschema_duration",
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
        "export_statistics_last",
        **KEY,
        target_database="targetdb",
        size=1,
    )


def test_save_stage_statistics_stores_the_run_with_its_stage_times(
    stage: export_stage.Stage, db_connection: sa.Connection
) -> None:
    _add_run(db_connection, 0, 100, 101)
    _add_last_statistics(db_connection)

    stage.save_stage_statistics(**EXPORT_RUN)

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
    assert _statistics(db_connection, "export_statistics") == [expected]
    assert _statistics(db_connection, "export_statistics_last") == [expected]


def test_save_stage_statistics_without_stage_times_saves_nothing(
    stage: export_stage.Stage, db_connection: sa.Connection
) -> None:
    _add_last_statistics(db_connection)

    stage.save_stage_statistics(**EXPORT_RUN)

    assert _statistics(db_connection, "export_statistics") == []
    assert len(_statistics(db_connection, "export_statistics_last")) == 1


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


EXPORT_JSON: dict[str, object] = {
    "dbalias": DBALIAS,
    "target_database": "targetdb",
    "target_schema": SCHEMA,
    "target_table": TABLE,
    "hive_db": "hivedb",
    "hive_table": "orders",
    "export_phase": "full",
    "incremental": False,
    "rows": 10,
    "sessions": 2,
}


def test_convert_stage_statistics_to_json_posts_the_extended_document(
    stage: export_stage.Stage,
    db_connection: sa.Connection,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _add_run(db_connection, 0, 100)
    _enable(db_connection, "post_data_to_rest", "post_data_to_rest_extended")
    fake = _FakeSendStatistics(succeed=True)
    monkeypatch.setattr(stage, "send_statistics", fake)

    stage.convert_stage_statistics_to_json(**EXPORT_JSON)

    assert fake.sent == {
        "rest": {
            "type": "export",
            "status": "finished",
            **EXPORT_JSON,
            "get_hive_tableschema_start": "2000-01-01 10:00:05",
            "get_hive_tableschema_stop": "2000-01-01 10:00:20",
            "get_hive_tableschema_duration": 15,
            "start": "2000-01-01 10:00:00",
            "stop": "2000-01-01 10:00:20",
            "duration": 20,
        }
    }


@pytest.mark.xfail(
    reason="Known bug #280: a failed AWS SNS document is saved as the REST "
    "document, for destination rest",
    raises=AssertionError,
    strict=True,
)
def test_convert_stage_statistics_to_json_saves_a_failed_aws_sns_document(
    stage: export_stage.Stage,
    db_connection: sa.Connection,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _add_run(db_connection, 0, 100)
    _enable(db_connection, "post_data_to_awssns", "post_data_to_awssns_extended")
    fake = _FakeSendStatistics(succeed=False)
    monkeypatch.setattr(stage, "send_statistics", fake)

    stage.convert_stage_statistics_to_json(**EXPORT_JSON)

    stored = db_connection.execute(sa.text("SELECT jsondata FROM json_to_send"))
    assert [json.loads(row[0]) for row in stored] == [fake.sent["awssns"]]


def test_save_stage_statistics_without_stage_zero_stores_no_start(
    stage: export_stage.Stage, db_connection: sa.Connection
) -> None:
    _add_run(db_connection, 100, 101)

    stage.save_stage_statistics(**EXPORT_RUN)

    [row] = _statistics(db_connection, "export_statistics")
    assert row[-3:] == (None, datetime(2000, 1, 1, 10, 0, 20), None)


def test_convert_stage_statistics_to_json_without_stage_zero_sends_no_times(
    stage: export_stage.Stage,
    db_connection: sa.Connection,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _add_run(db_connection, 100, 101)
    _enable(db_connection, "post_data_to_rest")
    fake = _FakeSendStatistics(succeed=True)
    monkeypatch.setattr(stage, "send_statistics", fake)

    stage.convert_stage_statistics_to_json(**EXPORT_JSON)

    sent = fake.sent["rest"]
    assert (sent["start"], sent["stop"], sent["duration"]) == ("-1", "-1", -1)


def test_save_stage_statistics_with_duplicate_stage_columns_raises(
    stage: export_stage.Stage, db_connection: sa.Connection
) -> None:
    _add_run(db_connection, 0, 100, 150)
    _add_last_statistics(db_connection)

    with pytest.raises(ValueError, match="get_hive_tableschema_start"):
        stage.save_stage_statistics(**EXPORT_RUN)

    assert _statistics(db_connection, "export_statistics") == []
    assert len(_statistics(db_connection, "export_statistics_last")) == 1
