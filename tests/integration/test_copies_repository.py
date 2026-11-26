"""CopiesRepository: what the server reads and removes once distcp has
finished a copy."""

from collections.abc import Iterator

import pytest
import sqlalchemy as sa
from sqlalchemy.orm import Session

from dbimport.config_db import engine as config_engine
from dbimport.config_db.repositories.copies import AsyncCopy, CopiesRepository
from tests.configdb import insert_row


def _add_copy(
    conn: sa.Connection, table_id: int, destination: str, copy_status: int
) -> None:
    insert_row(
        conn,
        "copy_async_status",
        table_id=table_id,
        hive_db="sales",
        hive_table=f"t{table_id}",
        destination=destination,
        copy_status=copy_status,
        failures=1,
        hdfs_source_path=f"hdfs://local/t{table_id}",
        hdfs_target_path=f"hdfs://{destination}/t{table_id}",
    )


@pytest.fixture
def session(clean_config_db: None, db_connection: sa.Connection) -> Iterator[Session]:
    _add_copy(db_connection, 1, "remote", 3)
    _add_copy(db_connection, 1, "second", 2)
    _add_copy(db_connection, 2, "remote", 3)
    with Session(config_engine.engine()) as session:
        yield session


def test_finished_copies_returns_the_copies_with_status_3(session: Session) -> None:
    finished = CopiesRepository(session).finished_copies()

    assert sorted(finished, key=lambda copy: copy.table_id) == [
        AsyncCopy(
            table_id=table_id,
            hive_db="sales",
            hive_table=f"t{table_id}",
            destination="remote",
            failures=1,
            hdfs_source_path=f"hdfs://local/t{table_id}",
            hdfs_target_path=f"hdfs://remote/t{table_id}",
        )
        for table_id in (1, 2)
    ]


def test_delete_async_copy_removes_only_that_copy(
    session: Session, db_connection: sa.Connection
) -> None:
    CopiesRepository(session).delete_async_copy(1, "remote")
    session.commit()

    remaining = db_connection.execute(
        sa.text(
            "SELECT table_id, destination FROM copy_async_status "
            "ORDER BY table_id, destination"
        )
    ).all()
    assert [tuple(row) for row in remaining] == [(1, "second"), (2, "remote")]
