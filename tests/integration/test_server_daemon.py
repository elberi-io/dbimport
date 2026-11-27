"""How the server hands the asynchronous copies in copy_async_status to
its distcp threads and stores their results. No distcp thread runs; the
request queue stands in for them."""

from collections.abc import Iterator
from datetime import UTC, datetime
from queue import Queue
from typing import Any

import pytest
import sqlalchemy as sa
from sqlalchemy.orm import Session

from dbimport.config_db import engine as config_engine
from dbimport.server import daemon
from tests.configdb import insert_row

OLD = datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)
NOW = datetime(2026, 3, 4, 5, 6, 7, tzinfo=UTC)


def _add_copy(
    conn: sa.Connection,
    table_id: int,
    copy_status: int,
    *,
    updated: datetime = OLD,
    failures: int = 0,
) -> None:
    insert_row(
        conn,
        "copy_async_status",
        table_id=table_id,
        hive_db=f"db{table_id}",
        hive_table="orders",
        destination="remote",
        copy_status=copy_status,
        last_status_update=updated,
        failures=failures,
        hdfs_source_path=f"hdfs://local/db{table_id}/orders",
        hdfs_target_path=f"hdfs://remote/db{table_id}/orders",
    )


def _copies(conn: sa.Connection) -> list[tuple[Any, ...]]:
    rows = conn.execute(
        sa.text(
            "SELECT table_id, copy_status, failures, last_status_update = :now "
            "FROM copy_async_status ORDER BY table_id"
        ),
        {"now": NOW},
    )
    return [tuple(row) for row in rows]


def _request(table_id: int, failures: int) -> dict[str, Any]:
    return {
        "tableID": table_id,
        "hiveDB": f"db{table_id}",
        "hiveTable": "orders",
        "destination": "remote",
        "failures": failures,
        "HDFSsourcePath": f"hdfs://local/db{table_id}/orders",
        "HDFStargetPath": f"hdfs://remote/db{table_id}/orders",
    }


@pytest.fixture
def session(clean_config_db: None) -> Iterator[Session]:
    with Session(config_engine.engine()) as session:
        yield session


def test_start_pending_copies_hands_new_and_long_failed_copies_to_distcp(
    session: Session, db_connection: sa.Connection
) -> None:
    _add_copy(db_connection, 1, 0)
    _add_copy(db_connection, 2, 1)
    _add_copy(db_connection, 3, 2, failures=2)
    _add_copy(
        db_connection,
        4,
        2,
        updated=datetime(2026, 3, 4, 5, 5, 30, tzinfo=UTC),
        failures=1,
    )
    _add_copy(db_connection, 5, 3)
    requests: Queue[dict[str, Any]] = Queue()

    daemon.start_pending_copies(session, requests, NOW)

    handed = sorted(
        (requests.get_nowait() for _ in range(requests.qsize())),
        key=lambda request: request["tableID"],
    )
    assert handed == [_request(1, 0), _request(3, 2)]
    assert _copies(db_connection) == [
        (1, 1, 0, 1),
        (2, 1, 0, 0),
        (3, 1, 2, 1),
        (4, 2, 1, 0),
        (5, 3, 0, 0),
    ]


@pytest.mark.parametrize(
    ("result", "failures", "expected"),
    [(True, 0, (1, 3, 0, 1)), (False, 4, (1, 2, 4, 1))],
    ids=["succeeded", "failed"],
)
def test_save_copy_result_stores_the_status_and_failures(
    session: Session,
    db_connection: sa.Connection,
    result: bool,
    failures: int,
    expected: tuple[int, int, int, int],
) -> None:
    _add_copy(db_connection, 1, 1, failures=3)
    _add_copy(db_connection, 2, 1, failures=3)

    daemon.save_copy_result(
        session,
        {
            "tableID": 1,
            "hiveDB": "db1",
            "hiveTable": "orders",
            "destination": "remote",
            "result": result,
            "failures": failures,
        },
        NOW,
    )

    assert _copies(db_connection) == [expected, (2, 1, 3, 0)]


def test_restart_started_copies_sets_handed_over_copies_back_to_new(
    session: Session, db_connection: sa.Connection
) -> None:
    for table_id, copy_status in enumerate([0, 1, 2, 3], start=1):
        _add_copy(db_connection, table_id, copy_status)

    daemon.restart_started_copies(session, NOW)

    assert _copies(db_connection) == [
        (1, 0, 0, 0),
        (2, 0, 0, 1),
        (3, 2, 0, 0),
        (4, 3, 0, 0),
    ]
