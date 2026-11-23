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

from datetime import datetime

from pydantic import BaseModel, ConfigDict
from sqlalchemy import insert, select, update
from sqlalchemy.orm import Session

from dbimport.config_db.config_schema import ImportTables


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
    last_update_from_source: datetime | None
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
        self, hive_db: str, hive_table: str, updated: str
    ) -> None:
        self._session.execute(
            update(ImportTables)
            .where(
                ImportTables.hive_db == hive_db, ImportTables.hive_table == hive_table
            )
            .values(last_update_from_source=updated)
        )

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

    def _update(self, table_id: int, **values: object) -> None:
        self._session.execute(
            update(ImportTables).where(ImportTables.table_id == table_id).values(values)
        )
