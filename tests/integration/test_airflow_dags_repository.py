"""The reads of the DAG generator that its expected DAG files do not pin:
which schedule a DAG name gets when it is in more than one DAG table, and
the selection of import tables by several Hive patterns."""

from collections.abc import Iterator

import pytest
import sqlalchemy as sa
from sqlalchemy.orm import Session

from dbimport.config_db.repositories.airflow_dags import AirflowDAGsRepository
from dbimport.config_db.repositories.import_tables import ImportTablesRepository
from tests.configdb import insert_row


@pytest.fixture
def session(clean_config_db: None, engine: sa.Engine) -> Iterator[Session]:
    with Session(engine) as session:
        yield session


def _add_dag(conn: sa.Connection, table: str, schedule: str, **values: str) -> None:
    insert_row(conn, table, dag_name="shared", schedule_interval=schedule, **values)


@pytest.mark.parametrize(
    ("tables", "expected"),
    [
        (["import", "export", "custom"], "01:00"),
        (["export", "custom"], "02:00"),
        (["custom"], "03:00"),
        ([], None),
    ],
    ids=["import-first", "export-before-custom", "custom", "missing"],
)
def test_schedule_interval_takes_import_then_export_then_custom(
    session: Session,
    db_connection: sa.Connection,
    tables: list[str],
    expected: str | None,
) -> None:
    if "import" in tables:
        _add_dag(db_connection, "airflow_import_dags", "01:00", filter_hive="db.*")
    if "export" in tables:
        _add_dag(db_connection, "airflow_export_dags", "02:00", filter_dbalias="c")
    if "custom" in tables:
        _add_dag(db_connection, "airflow_custom_dags", "03:00")

    assert AirflowDAGsRepository(session).schedule_interval("shared") == expected


def test_dag_tables_selects_tables_matching_any_pattern_in_key_order(
    session: Session, db_connection: sa.Connection
) -> None:
    for hive_db, hive_table, included in (
        ("sales", "orders", 1),
        ("sales", "events", 1),
        ("hr", "staff", 1),
        ("hr", "salaries", 0),
        ("finance", "ledger", 1),
    ):
        insert_row(
            db_connection,
            "import_tables",
            hive_db=hive_db,
            hive_table=hive_table,
            dbalias="test_mysql",
            source_schema="src",
            source_table=hive_table,
            include_in_airflow=included,
        )

    tables = ImportTablesRepository(session).dag_tables([("sales", "%"), ("hr", "s%")])

    assert [(table.hive_db, table.hive_table) for table in tables] == [
        ("hr", "staff"),
        ("sales", "events"),
        ("sales", "orders"),
    ]
