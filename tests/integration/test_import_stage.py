import pytest
import sqlalchemy as sa

from DBImportConfig import common_config, import_stage

HIVE_DB = "testdb"
HIVE_TABLE = "testtable"


@pytest.fixture
def stage(clean_config_db: None) -> import_stage.stage:
    """Stage handler for HIVE_DB.HIVE_TABLE, as import_config creates it."""
    conn = common_config.config(HIVE_DB, HIVE_TABLE).mysql_conn
    return import_stage.stage(conn, HIVE_DB, HIVE_TABLE)


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
    stage: import_stage.stage,
) -> None:
    assert stage.get_stage() == 0


def test_set_stage_persists_stage(
    stage: import_stage.stage, db_connection: sa.Connection
) -> None:
    stage.get_stage()

    stage.set_stage(1010)

    assert _persisted_stage(db_connection) == 1010


def test_set_stage_lower_than_current_stage_is_ignored(
    stage: import_stage.stage, db_connection: sa.Connection
) -> None:
    stage.get_stage()
    stage.set_stage(1020)

    stage.set_stage(1010)

    assert _persisted_stage(db_connection) == 1020


def test_set_stage_with_force_allows_lower_stage(
    stage: import_stage.stage, db_connection: sa.Connection
) -> None:
    stage.get_stage()
    stage.set_stage(1020)

    stage.set_stage(1010, force=True)

    assert _persisted_stage(db_connection) == 1010


def test_set_stage_records_statistics_for_the_stage_it_leaves(
    stage: import_stage.stage, db_connection: sa.Connection
) -> None:
    stage.get_stage()

    stage.set_stage(1010)
    stage.set_stage(1011)

    assert _rows(db_connection, "import_stage_statistics") == [0, 1010]


def test_set_stage_in_memory_mode_writes_nothing(
    stage: import_stage.stage, db_connection: sa.Connection
) -> None:
    stage.set_stage_only_in_memory()
    stage.get_stage()

    stage.set_stage(1010)

    assert stage.get_stage() == 1010
    assert _persisted_stage(db_connection) is None
    assert _rows(db_connection, "import_stage_statistics") == []


def test_save_retry_attempt_logs_the_stage(
    stage: import_stage.stage, db_connection: sa.Connection
) -> None:
    stage.save_retry_attempt(1013)

    assert _rows(db_connection, "import_retries_log") == [1013]


def test_clear_stage_removes_stage_and_statistics(
    stage: import_stage.stage, db_connection: sa.Connection
) -> None:
    stage.get_stage()
    stage.set_stage(1010)
    stage.set_stage(1011)

    stage.clear_stage()

    assert _persisted_stage(db_connection) is None
    assert _rows(db_connection, "import_stage_statistics") == []
