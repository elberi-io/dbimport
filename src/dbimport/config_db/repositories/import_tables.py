# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""Import tables: the import_tables row of a table that is imported."""

from collections.abc import Mapping, Sequence
from datetime import datetime

from pydantic import BaseModel, ConfigDict
from sqlalchemy import (
    bindparam,
    cast,
    delete,
    func,
    insert,
    or_,
    select,
    update,
)
from sqlalchemy.dialects.mysql import INTEGER
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.orm import Session
from sqlalchemy.sql import ColumnElement

from dbimport.config_db.config_schema import ImportStatisticsLast, ImportTables
from dbimport.config_db.repositories.settings import check_settings
from dbimport.config_db.repositories.types import StoredDateTime


class ImportTable(BaseModel):
    model_config = ConfigDict(frozen=True, from_attributes=True)

    hive_db: str
    hive_table: str
    table_id: int
    dbalias: str
    source_schema: str
    source_table: str
    import_phase_type: str | None
    etl_phase_type: str | None
    import_type: str | None
    import_tool: str
    etl_engine: str
    last_update_from_source: StoredDateTime
    sqoop_sql_where_addition: str | None
    nomerge_ingestion_sql_addition: str | None
    validate_import: int
    validation_method: str
    validate_source: str | None
    validate_diff_allowed: int
    validation_custom_query_source_sql: str | None
    validation_custom_query_hive_sql: str | None
    validation_custom_query_validate_import_table: int
    truncate_hive: int
    mappers: int
    soft_delete_during_merge: int
    source_rowcount: int | None
    source_rowcount_incr: int | None
    hive_rowcount: int | None
    validation_custom_query_source_value: str | None
    validation_custom_query_hive_value: str | None
    incr_mode: str | None
    incr_column: str | None
    incr_validation_method: str | None
    incr_minvalue: str | None
    incr_maxvalue: str | None
    incr_minvalue_pending: str | None
    incr_maxvalue_pending: str | None
    pk_column_override: str | None
    pk_column_override_mergeonly: str | None
    hive_merge_heap: int | None
    hive_split_count: int | None
    sqoop_query: str | None
    sqoop_options: str | None
    sqoop_last_size: int | None
    sqoop_last_rows: int | None
    sqoop_last_mappers: int | None
    sqoop_last_execution: int | None
    sqoop_use_generated_sql: int
    sqoop_allow_text_splitter: int
    comment: str | None
    generated_sqoop_query: str | None
    generated_sqoop_options: str | None
    generated_pk_columns: str | None
    datalake_source: str | None
    copy_slave: int
    create_foreign_keys: int
    split_by_column: str | None
    spark_executor_memory: str | None
    spark_executors: int | None
    custom_max_query: str | None
    merge_compaction_method: str
    invalidate_impala: int
    import_database: str | None
    import_table: str | None
    history_database: str | None
    history_table: str | None
    include_in_airflow: int
    airflow_priority: int | None
    force_string: int
    generated_hive_column_definition: str | None
    operator_notes: str | None
    copy_finished: StoredDateTime
    source_table_type: str | None


class ImportTableEntry(BaseModel):
    """A table as the search lists it."""

    model_config = ConfigDict(frozen=True)

    hive_db: str
    hive_table: str
    dbalias: str
    source_schema: str
    source_table: str
    import_phase_type: str | None
    etl_phase_type: str | None
    import_tool: str
    etl_engine: str
    last_update_from_source: StoredDateTime
    include_in_airflow: int
    operator_notes: str | None


class ImportDAGTable(BaseModel):
    """An import table as an Airflow DAG schedules it."""

    model_config = ConfigDict(frozen=True)

    hive_db: str
    hive_table: str
    dbalias: str
    airflow_priority: int | None
    import_type: str | None
    import_phase_type: str | None
    etl_phase_type: str | None
    sqoop_last_mappers: int | None
    copy_slave: int


class DatabaseSummary(BaseModel):
    """The tables imported into one Hive database."""

    model_config = ConfigDict(frozen=True)

    hive_db: str
    tables: int
    last_import: StoredDateTime
    last_size: int | None
    last_rows: int | None


_SEARCHABLE = frozenset(
    {
        "hive_db",
        "hive_table",
        "dbalias",
        "source_schema",
        "source_table",
        "import_phase_type",
        "etl_phase_type",
        "import_tool",
        "etl_engine",
        "include_in_airflow",
        "operator_notes",
    }
)

_SETTINGS = frozenset(
    {
        "dbalias",
        "source_schema",
        "source_table",
        "import_phase_type",
        "etl_phase_type",
        "import_tool",
        "etl_engine",
        "last_update_from_source",
        "sqoop_sql_where_addition",
        "nomerge_ingestion_sql_addition",
        "include_in_airflow",
        "airflow_priority",
        "validate_import",
        "validation_method",
        "validate_source",
        "validate_diff_allowed",
        "validation_custom_query_source_sql",
        "validation_custom_query_hive_sql",
        "validation_custom_query_validate_import_table",
        "mappers",
        "soft_delete_during_merge",
        "incr_mode",
        "incr_column",
        "incr_validation_method",
        "pk_column_override",
        "pk_column_override_mergeonly",
        "hive_merge_heap",
        "hive_split_count",
        "spark_executor_memory",
        "spark_executors",
        "split_by_column",
        "sqoop_query",
        "sqoop_options",
        "sqoop_use_generated_sql",
        "sqoop_allow_text_splitter",
        "force_string",
        "comment",
        "datalake_source",
        "operator_notes",
        "copy_slave",
        "create_foreign_keys",
        "invalidate_impala",
        "custom_max_query",
        "merge_compaction_method",
        "source_table_type",
        "import_database",
        "import_table",
        "history_database",
        "history_table",
    }
)

_BULK_SETTINGS = frozenset(
    {
        "dbalias",
        "import_phase_type",
        "etl_phase_type",
        "import_tool",
        "etl_engine",
        "include_in_airflow",
        "operator_notes",
    }
)


def _columns(
    values: Mapping[str, object], allowed: frozenset[str]
) -> dict[str, object]:
    """values keyed by column name instead of attribute name. Raises
    KeyError for an attribute that is not allowed."""
    check_settings(values, allowed)
    mapped = ImportTables.__mapper__.columns
    return {mapped[name].name: value for name, value in values.items()}


class GeneratedDefinitions(BaseModel):
    """What an import generates from the source table's columns."""

    model_config = ConfigDict(frozen=True)

    hive_column_definition: str | None
    sqoop_query: str | None
    comment: str | None
    sqoop_options: str | None
    pk_columns: str | None
    use_generated_sql: bool
    source_table_type: str | None


class ImportTablesRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def table(self, hive_db: str, hive_table: str) -> ImportTable | None:
        row = self._session.get(ImportTables, (hive_db, hive_table))
        return None if row is None else ImportTable.model_validate(row)

    def exists(self, hive_db: str, hive_table: str) -> bool:
        return self._session.get(ImportTables, (hive_db, hive_table)) is not None

    def job_mappers(
        self, table_id: int, block_size: int
    ) -> tuple[int | None, int | None, int] | None:
        """sqoop_last_size, the size of the last import in blocks of
        block_size (rounded as MySQL casts it; None without statistics)
        and the configured mappers; None when the table is missing."""
        row = self._session.execute(
            select(
                ImportTables.sqoop_last_size,
                cast(ImportStatisticsLast.size / block_size, INTEGER(unsigned=True)),
                ImportTables.mappers,
            )
            .outerjoin(
                ImportStatisticsLast,
                (ImportStatisticsLast.hive_db == ImportTables.hive_db)
                & (ImportStatisticsLast.hive_table == ImportTables.hive_table),
            )
            .where(ImportTables.table_id == table_id)
        ).one_or_none()
        return None if row is None else (row[0], row[1], row[2])

    def dag_tables(self, patterns: Sequence[tuple[str, str]]) -> list[ImportDAGTable]:
        """The tables included in Airflow whose Hive database and table
        match one of the (database, table) LIKE patterns, ordered by
        database and table."""
        rows = self._session.execute(
            select(
                ImportTables.hive_db,
                ImportTables.hive_table,
                ImportTables.dbalias,
                ImportTables.airflow_priority,
                ImportTables.import_type,
                ImportTables.import_phase_type,
                ImportTables.etl_phase_type,
                ImportTables.sqoop_last_mappers,
                ImportTables.copy_slave,
            )
            .where(
                ImportTables.include_in_airflow == 1,
                or_(
                    *(
                        ImportTables.hive_db.like(hive_db)
                        & ImportTables.hive_table.like(hive_table)
                        for hive_db, hive_table in patterns
                    )
                ),
            )
            .order_by(ImportTables.hive_db, ImportTables.hive_table)
        )
        return [ImportDAGTable.model_validate(row._mapping) for row in rows]

    def table_by_id(self, table_id: int) -> ImportTable | None:
        row = self._session.scalars(
            select(ImportTables).where(ImportTables.table_id == table_id)
        ).one_or_none()
        return None if row is None else ImportTable.model_validate(row)

    def tables_of_database(self, hive_db: str) -> list[tuple[str, str, str, str, str]]:
        """hive_db, hive_table, dbalias, source_schema and source_table of
        every table imported into hive_db."""
        rows = self._session.execute(
            select(
                ImportTables.hive_db,
                ImportTables.hive_table,
                ImportTables.dbalias,
                ImportTables.source_schema,
                ImportTables.source_table,
            ).where(ImportTables.hive_db == hive_db)
        )
        return list(rows.tuples())

    def tables_like(self, hive_db: str, hive_table: str) -> list[tuple[int, str, str]]:
        """table_id, hive_db and hive_table of the tables matching the
        hive_db and hive_table LIKE patterns, ordered by database and
        table."""
        rows = self._session.execute(
            select(ImportTables.table_id, ImportTables.hive_db, ImportTables.hive_table)
            .where(
                ImportTables.hive_db.like(hive_db),
                ImportTables.hive_table.like(hive_table),
            )
            .order_by(ImportTables.hive_db, ImportTables.hive_table)
        )
        return list(rows.tuples())

    def add(
        self,
        hive_db: str,
        hive_table: str,
        dbalias: str,
        source_schema: str,
        source_table: str,
        etl_engine: str,
    ) -> None:
        """Raises sqlalchemy.exc.IntegrityError when the table exists."""
        self._session.execute(
            insert(ImportTables).values(
                hive_db=hive_db,
                hive_table=hive_table,
                dbalias=dbalias,
                source_schema=source_schema,
                source_table=source_table,
                etl_engine=etl_engine,
            )
        )

    def set_last_update_from_source(
        self, hive_db: str, hive_table: str, updated: datetime
    ) -> None:
        self._session.execute(
            update(ImportTables)
            .where(
                ImportTables.hive_db == hive_db, ImportTables.hive_table == hive_table
            )
            .values(last_update_from_source=updated)
        )

    def set_copy_finished(self, table_id: int, finished: datetime) -> None:
        self._update(table_id, copy_finished=finished)

    def begin_incremental_range(self, table_id: int) -> None:
        """The next range starts at the stored maximum."""
        self._update(table_id, incr_minvalue_pending=ImportTables.incr_maxvalue)

    def set_pending_range(
        self, table_id: int, minimum: object, maximum: object
    ) -> None:
        self._update(
            table_id, incr_minvalue_pending=minimum, incr_maxvalue_pending=maximum
        )

    def commit_pending_range(self, table_id: int) -> None:
        """The pending range becomes the stored range and is cleared."""
        self._update(
            table_id,
            incr_minvalue=ImportTables.incr_minvalue_pending,
            incr_maxvalue=ImportTables.incr_maxvalue_pending,
        )
        self._update(table_id, incr_minvalue_pending=None, incr_maxvalue_pending=None)

    def reset_incremental_range(self, table_id: int, maximum: object) -> None:
        """Only the given maximum is kept; the next import starts there."""
        self._update(
            table_id,
            incr_minvalue=None,
            incr_maxvalue=maximum,
            incr_minvalue_pending=None,
            incr_maxvalue_pending=None,
        )

    def clear_validation(self, table_id: int) -> None:
        self._update(
            table_id,
            source_rowcount=None,
            source_rowcount_incr=None,
            hive_rowcount=None,
            validation_custom_query_source_value=None,
            validation_custom_query_hive_value=None,
        )

    def set_row_counts(
        self,
        table_id: int,
        *,
        source: int | None = None,
        source_incr: int | None = None,
        hive: int | None = None,
    ) -> None:
        """Stores the given row counts; a count that is None stays as it is."""
        values = {
            "source_rowcount": source,
            "source_rowcount_incr": source_incr,
            "hive_rowcount": hive,
        }
        self._update(
            table_id,
            **{name: value for name, value in values.items() if value is not None},
        )

    def set_custom_query_value(
        self, table_id: int, *, source: str | None = None, hive: str | None = None
    ) -> None:
        """Stores the given result of a custom validation query; one that is
        None stays as it is."""
        values = {
            "validation_custom_query_source_value": source,
            "validation_custom_query_hive_value": hive,
        }
        self._update(
            table_id,
            **{name: value for name, value in values.items() if value is not None},
        )

    def save_import_statistics(
        self,
        table_id: int,
        execution: int | None,
        *,
        size: int | None = None,
        rows: int | None = None,
        incr_maxvalue_pending: object = None,
        mappers: int | None = None,
        source_row_counts: tuple[int | None, int | None] | None = None,
    ) -> None:
        """Stores the execution time and the given statistics; one that is
        None stays as it is. source_row_counts, when given, replaces both
        source row counts (full, incremental)."""
        values: dict[str, object] = {"sqoop_last_execution": execution}
        optional = {
            "sqoop_last_size": size,
            "sqoop_last_rows": rows,
            "incr_maxvalue_pending": incr_maxvalue_pending,
            "sqoop_last_mappers": mappers,
        }
        values.update(
            {name: value for name, value in optional.items() if value is not None}
        )
        if source_row_counts is not None:
            values["source_rowcount"], values["source_rowcount_incr"] = (
                source_row_counts
            )
        self._update(table_id, **values)

    def save_generated_definitions(
        self, table_id: int, generated: GeneratedDefinitions
    ) -> None:
        self._update(
            table_id,
            generated_hive_column_definition=generated.hive_column_definition,
            generated_sqoop_query=generated.sqoop_query,
            comment=generated.comment,
            generated_sqoop_options=generated.sqoop_options,
            generated_pk_columns=generated.pk_columns,
            generated_foreign_keys=None,
            sqoop_use_generated_sql=1 if generated.use_generated_sql else 0,
            source_table_type=generated.source_table_type,
        )

    def database_summaries(self, limit: int) -> list[DatabaseSummary]:
        """Per Hive database (at most limit): the number of tables, the
        last update from the source and the sums of the last sizes and
        rows."""
        rows = self._session.execute(
            select(
                ImportTables.hive_db,
                func.count(ImportTables.hive_db),
                func.max(ImportTables.last_update_from_source),
                func.sum(ImportTables.sqoop_last_size),
                func.sum(ImportTables.sqoop_last_rows),
            )
            .group_by(ImportTables.hive_db)
            .limit(limit)
        )
        return [
            DatabaseSummary(
                hive_db=hive_db,
                tables=tables,
                last_import=last_import,
                last_size=last_size,
                last_rows=last_rows,
            )
            for hive_db, tables, last_import, last_size, last_rows in rows.tuples()
        ]

    def search(
        self, filters: Mapping[str, object], limit: int
    ) -> tuple[list[ImportTableEntry], int]:
        """At most limit tables whose fields match all filters, ordered by
        Hive database and table, and the number of all matches. A string
        containing "%" is a LIKE pattern; other values must be equal.
        Raises KeyError for a field that cannot be searched."""
        unknown = set(filters) - _SEARCHABLE
        if unknown:
            raise KeyError(", ".join(sorted(unknown)))
        conditions: list[ColumnElement[bool]] = [
            getattr(ImportTables, field).like(value)
            if isinstance(value, str) and "%" in value
            else getattr(ImportTables, field) == value
            for field, value in filters.items()
        ]
        rows = self._session.execute(
            select(
                *(
                    getattr(ImportTables, name).label(name)
                    for name in ImportTableEntry.model_fields
                )
            )
            .where(*conditions)
            .order_by(ImportTables.hive_db, ImportTables.hive_table)
            .limit(limit)
        )
        found = [ImportTableEntry.model_validate(row._mapping) for row in rows]
        total = self._session.scalar(
            select(func.count()).select_from(ImportTables).where(*conditions)
        )
        return found, total or 0

    def save_settings(
        self, hive_db: str, hive_table: str, settings: Mapping[str, object]
    ) -> None:
        """Creates the table with the given settings, or replaces them on
        the existing one; other columns keep their values. Raises KeyError
        for a column that is not a setting."""
        values = _columns(settings, _SETTINGS)
        self._session.execute(
            mysql_insert(ImportTables)
            .values(hive_db=hive_db, hive_table=hive_table, **values)
            .on_duplicate_key_update(values)
        )

    def table_id_of(self, hive_db: str, hive_table: str) -> int:
        """Raises sqlalchemy.exc.NoResultFound when the table is missing."""
        return self._session.scalars(
            select(ImportTables.table_id).where(
                ImportTables.hive_db == hive_db, ImportTables.hive_table == hive_table
            )
        ).one()

    def delete(self, hive_db: str, hive_table: str) -> bool:
        """Deletes the table; False when there is none."""
        result = self._session.execute(
            delete(ImportTables).where(
                ImportTables.hive_db == hive_db, ImportTables.hive_table == hive_table
            )
        )
        return result.rowcount == 1

    def delete_many(self, tables: Sequence[tuple[str, str]]) -> None:
        """Deletes the given (hive_db, hive_table) tables; unknown ones are
        ignored."""
        self._session.connection().execute(
            delete(ImportTables).where(
                ImportTables.hive_db == bindparam("b_hive_db"),
                ImportTables.hive_table == bindparam("b_hive_table"),
            ),
            [{"b_hive_db": db, "b_hive_table": name} for db, name in tables],
        )

    def update_many(
        self, tables: Sequence[tuple[str, str]], settings: Mapping[str, object]
    ) -> None:
        """Sets the given settings on the given (hive_db, hive_table)
        tables. Raises KeyError for a column that cannot be changed in
        bulk."""
        self._session.connection().execute(
            update(ImportTables)
            .where(
                ImportTables.hive_db == bindparam("b_hive_db"),
                ImportTables.hive_table == bindparam("b_hive_table"),
            )
            .values(_columns(settings, _BULK_SETTINGS)),
            [{"b_hive_db": db, "b_hive_table": name} for db, name in tables],
        )

    def _update(self, table_id: int, **values: object) -> None:
        self._session.execute(
            update(ImportTables).where(ImportTables.table_id == table_id).values(values)
        )
