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

from collections.abc import Mapping, Sequence

from pydantic import BaseModel, ConfigDict
from sqlalchemy import (
    ColumnElement,
    bindparam,
    cast,
    delete,
    func,
    insert,
    select,
    update,
)
from sqlalchemy.dialects.mysql import INTEGER
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.orm import Session

from dbimport.config_db.config_schema import ExportTables
from dbimport.config_db.repositories.settings import check_settings
from dbimport.config_db.repositories.types import StoredDateTime

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
    last_update_from_hive: StoredDateTime
    hive_rowcount: int | None
    target_rowcount: int | None
    mappers: int
    include_in_airflow: int
    airflow_priority: int | None
    uppercase_columns: int
    sqoop_options: str | None
    sqoop_last_execution: int | None
    create_target_table_sql: str | None
    operator_notes: str | None


class ExportTableEntry(BaseModel):
    """A table as the search lists it."""

    model_config = ConfigDict(frozen=True)

    dbalias: str
    target_schema: str
    target_table: str
    hive_db: str
    hive_table: str
    export_type: str
    export_tool: str
    last_update_from_hive: StoredDateTime
    include_in_airflow: int
    operator_notes: str | None


class ExportDAGTable(BaseModel):
    """An export table as an Airflow DAG schedules it."""

    model_config = ConfigDict(frozen=True)

    target_schema: str
    target_table: str
    dbalias: str
    airflow_priority: int | None
    export_type: str
    sqoop_last_mappers: int | None


class ConnectionSummary(BaseModel):
    """The tables exported to one connection."""

    model_config = ConfigDict(frozen=True)

    dbalias: str
    tables: int
    last_export: StoredDateTime
    last_rows: int | None


_SEARCHABLE = frozenset(ExportTableEntry.model_fields) - {"last_update_from_hive"}

_SETTINGS = frozenset(
    {
        "export_type",
        "export_tool",
        "hive_db",
        "hive_table",
        "last_update_from_hive",
        "sql_where_addition",
        "include_in_airflow",
        "airflow_priority",
        "force_create_temp_table",
        "validate_export",
        "validation_method",
        "validation_custom_query_hive_sql",
        "validation_custom_query_target_sql",
        "uppercase_columns",
        "truncate_target",
        "mappers",
        "incr_column",
        "incr_validation_method",
        "sqoop_options",
        "hive_javaheap",
        "create_target_table_sql",
        "operator_notes",
    }
)

_BULK_SETTINGS = frozenset(
    {"export_type", "export_tool", "include_in_airflow", "operator_notes"}
)


def _columns(
    values: Mapping[str, object], allowed: frozenset[str]
) -> dict[str, object]:
    """values keyed by column name instead of attribute name. Raises
    KeyError for an attribute that is not allowed."""
    check_settings(values, allowed)
    mapped = ExportTables.__mapper__.columns
    return {mapped[name].name: value for name, value in values.items()}


def _bound_tables(
    tables: Sequence[tuple[str, str, str]],
) -> list[dict[str, str]]:
    return [
        {"b_dbalias": dbalias, "b_schema": schema, "b_table": table}
        for dbalias, schema, table in tables
    ]


_BOUND_TABLE = (
    (ExportTables.dbalias == bindparam("b_dbalias"))
    & (ExportTables.target_schema == bindparam("b_schema"))
    & (ExportTables.target_table == bindparam("b_table"))
)


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

    def dag_tables(
        self, dbalias: str, target_schema: str, target_table: str
    ) -> list[ExportDAGTable]:
        """The tables included in Airflow that match the LIKE patterns; an
        empty pattern matches every value. Ordered by connection, schema
        and table."""
        query = select(
            ExportTables.target_schema,
            ExportTables.target_table,
            ExportTables.dbalias,
            ExportTables.airflow_priority,
            ExportTables.export_type,
            ExportTables.sqoop_last_mappers,
        ).where(ExportTables.include_in_airflow == 1)
        if dbalias != "":
            query = query.where(ExportTables.dbalias.like(dbalias))
        if target_schema != "":
            query = query.where(ExportTables.target_schema.like(target_schema))
        if target_table != "":
            query = query.where(ExportTables.target_table.like(target_table))
        rows = self._session.execute(
            query.order_by(
                ExportTables.dbalias,
                ExportTables.target_schema,
                ExportTables.target_table,
            )
        )
        return [ExportDAGTable.model_validate(row._mapping) for row in rows]

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

    def connection_summaries(self, limit: int) -> list[ConnectionSummary]:
        """Per connection (at most limit): the number of tables, the last
        update from Hive and the sum of the last rows."""
        rows = self._session.execute(
            select(
                ExportTables.dbalias,
                func.count(ExportTables.dbalias),
                func.max(ExportTables.last_update_from_hive),
                func.sum(ExportTables.sqoop_last_rows),
            )
            .group_by(ExportTables.dbalias)
            .limit(limit)
        )
        return [
            ConnectionSummary(
                dbalias=dbalias,
                tables=tables,
                last_export=last_export,
                last_rows=last_rows,
            )
            for dbalias, tables, last_export, last_rows in rows.tuples()
        ]

    def search(
        self, filters: Mapping[str, object], limit: int
    ) -> tuple[list[ExportTableEntry], int]:
        """At most limit tables whose fields match all filters, ordered by
        connection, target schema and table, and the number of all matches.
        A string containing "%" is a LIKE pattern; other values must be
        equal. Raises KeyError for a field that cannot be searched."""
        unknown = set(filters) - _SEARCHABLE
        if unknown:
            raise KeyError(", ".join(sorted(unknown)))
        conditions: list[ColumnElement[bool]] = [
            getattr(ExportTables, field).like(value)
            if isinstance(value, str) and "%" in value
            else getattr(ExportTables, field) == value
            for field, value in filters.items()
        ]
        rows = self._session.execute(
            select(
                *(
                    getattr(ExportTables, name).label(name)
                    for name in ExportTableEntry.model_fields
                )
            )
            .where(*conditions)
            .order_by(
                ExportTables.dbalias,
                ExportTables.target_schema,
                ExportTables.target_table,
            )
            .limit(limit)
        )
        found = [ExportTableEntry.model_validate(row._mapping) for row in rows]
        total = self._session.scalar(
            select(func.count()).select_from(ExportTables).where(*conditions)
        )
        return found, total or 0

    def save_settings(
        self,
        dbalias: str,
        target_schema: str,
        target_table: str,
        settings: Mapping[str, object],
    ) -> None:
        """Creates the table with the given settings, or replaces them on
        the existing one; other columns keep their values. Raises KeyError
        for a column that is not a setting."""
        values = _columns(settings, _SETTINGS)
        self._session.execute(
            mysql_insert(ExportTables)
            .values(
                dbalias=dbalias,
                target_schema=target_schema,
                target_table=target_table,
                **values,
            )
            .on_duplicate_key_update(values)
        )

    def table_id_of(self, dbalias: str, target_schema: str, target_table: str) -> int:
        """Raises sqlalchemy.exc.NoResultFound when the table is missing."""
        return self._session.scalars(
            select(ExportTables.table_id).where(
                ExportTables.dbalias == dbalias,
                ExportTables.target_schema == target_schema,
                ExportTables.target_table == target_table,
            )
        ).one()

    def delete(self, dbalias: str, target_schema: str, target_table: str) -> bool:
        """Deletes the table and its columns; False when there is none."""
        result = self._session.execute(
            delete(ExportTables).where(
                ExportTables.dbalias == dbalias,
                ExportTables.target_schema == target_schema,
                ExportTables.target_table == target_table,
            )
        )
        return result.rowcount == 1

    def delete_many(self, tables: Sequence[tuple[str, str, str]]) -> None:
        """Deletes the given (dbalias, target_schema, target_table) tables;
        unknown ones are ignored."""
        self._session.connection().execute(
            delete(ExportTables).where(_BOUND_TABLE), _bound_tables(tables)
        )

    def update_many(
        self, tables: Sequence[tuple[str, str, str]], settings: Mapping[str, object]
    ) -> None:
        """Sets the given settings on the given (dbalias, target_schema,
        target_table) tables. Raises KeyError for a column that cannot be
        changed in bulk."""
        self._session.connection().execute(
            update(ExportTables)
            .where(_BOUND_TABLE)
            .values(_columns(settings, _BULK_SETTINGS)),
            _bound_tables(tables),
        )

    def _update(self, table_id: int, **values: object) -> None:
        self._session.execute(
            update(ExportTables).where(ExportTables.table_id == table_id).values(values)
        )
