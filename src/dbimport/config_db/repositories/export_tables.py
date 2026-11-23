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
"""Export tables: the export_tables row of a table that is exported."""

from datetime import datetime

from pydantic import BaseModel, ConfigDict
from sqlalchemy import cast, insert, select, update
from sqlalchemy.dialects.mysql import INTEGER
from sqlalchemy.orm import Session

from dbimport.config_db.config_schema import ExportTables

_MAPPER_SIZE = 1024 * 1024 * 128


class ExportTable(BaseModel):
    model_config = ConfigDict(frozen=True, from_attributes=True)

    table_id: int
    dbalias: str
    target_schema: str
    target_table: str
    export_type: str
    hive_db: str
    hive_table: str
    validate_export: int
    sqoop_last_size: int | None
    sqoop_last_rows: int | None
    sqoop_last_mappers: int | None
    incr_column: str | None
    incr_minvalue: str | None
    incr_maxvalue: str | None
    incr_minvalue_pending: str | None
    incr_maxvalue_pending: str | None
    truncate_target: int
    incr_validation_method: str | None
    hive_javaheap: int | None
    export_tool: str
    sql_where_addition: str | None
    validation_method: str
    validation_custom_query_hive_sql: str | None
    validation_custom_query_target_sql: str | None
    validation_custom_query_hive_value: str | None
    validation_custom_query_target_value: str | None
    force_create_temp_table: int
    last_update_from_hive: datetime | None
    hive_rowcount: int | None
    target_rowcount: int | None
    mappers: int


class ExportTablesRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def table(
        self, dbalias: str, target_schema: str, target_table: str
    ) -> ExportTable | None:
        row = self._session.get(ExportTables, (dbalias, target_schema, target_table))
        return None if row is None else ExportTable.model_validate(row)

    def table_by_id(self, table_id: int) -> ExportTable | None:
        row = self._session.scalars(
            select(ExportTables).where(ExportTables.table_id == table_id)
        ).one_or_none()
        return None if row is None else ExportTable.model_validate(row)

    def hive_tables_of_schema(
        self, dbalias: str, target_schema: str
    ) -> list[tuple[str, str]]:
        """hive_db and hive_table of every table exported to the schema."""
        rows = self._session.execute(
            select(ExportTables.hive_db, ExportTables.hive_table).where(
                ExportTables.dbalias == dbalias,
                ExportTables.target_schema == target_schema,
            )
        )
        return list(rows.tuples())

    def add(
        self,
        hive_db: str,
        hive_table: str,
        dbalias: str,
        target_schema: str,
        target_table: str,
    ) -> None:
        """Raises sqlalchemy.exc.IntegrityError when the table exists."""
        self._session.execute(
            insert(ExportTables).values(
                hive_db=hive_db,
                hive_table=hive_table,
                dbalias=dbalias,
                target_schema=target_schema,
                target_table=target_table,
            )
        )

    def job_mappers(self, table_id: int) -> tuple[int | None, int] | None:
        """The last size in units of 128 MB (rounded as MySQL casts it) and
        the configured mappers; None when the table is missing."""
        row = self._session.execute(
            select(
                cast(
                    ExportTables.sqoop_last_size / _MAPPER_SIZE,
                    INTEGER(unsigned=True),
                ),
                ExportTables.mappers,
            ).where(ExportTables.table_id == table_id)
        ).one_or_none()
        return None if row is None else (row[0], row[1])

    def set_custom_query_value(
        self, table_id: int, *, hive: str | None = None, target: str | None = None
    ) -> None:
        """Stores the given result of a custom validation query; one that is
        None stays as it is."""
        values = {
            "validation_custom_query_hive_value": hive,
            "validation_custom_query_target_value": target,
        }
        self._update(
            table_id,
            **{name: value for name, value in values.items() if value is not None},
        )

    def set_last_update_from_hive(self, table_id: int, updated: str | None) -> None:
        self._update(table_id, last_update_from_hive=updated)

    def clear_validation(self, table_id: int) -> None:
        self._update(
            table_id,
            hive_rowcount=None,
            target_rowcount=None,
            validation_custom_query_hive_value=None,
            validation_custom_query_target_value=None,
        )

    def set_row_counts(
        self,
        table_id: int,
        *,
        hive: int | None = None,
        target: object = None,
        last_rows: object = None,
    ) -> None:
        """Stores the given row counts; a count that is None stays as it is."""
        values = {
            "hive_rowcount": hive,
            "target_rowcount": target,
            "sqoop_last_rows": last_rows,
        }
        self._update(
            table_id,
            **{name: value for name, value in values.items() if value is not None},
        )

    def save_export_statistics(self, table_id: int, **statistics: object) -> None:
        """statistics: sqoop_last_execution, sqoop_last_size,
        sqoop_last_rows, incr_maxvalue_pending, sqoop_last_mappers."""
        allowed = {
            "sqoop_last_execution",
            "sqoop_last_size",
            "sqoop_last_rows",
            "incr_maxvalue_pending",
            "sqoop_last_mappers",
        }
        unknown = set(statistics) - allowed
        if unknown:
            raise KeyError(", ".join(sorted(unknown)))
        self._update(table_id, **statistics)

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
            incr_minvalue=ExportTables.incr_minvalue_pending,
            incr_maxvalue=ExportTables.incr_maxvalue_pending,
        )
        self._update(table_id, incr_minvalue_pending=None, incr_maxvalue_pending=None)

    def reset_incremental_range(self, table_id: int, maximum: object) -> None:
        """Only the given maximum is kept; the next export starts there."""
        self._update(
            table_id,
            incr_minvalue=None,
            incr_maxvalue=maximum,
            incr_minvalue_pending=None,
            incr_maxvalue_pending=None,
        )

    def _update(self, table_id: int, **values: object) -> None:
        self._session.execute(
            update(ExportTables).where(ExportTables.table_id == table_id).values(values)
        )
