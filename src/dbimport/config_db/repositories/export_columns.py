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
"""Export columns: the columns of an exported table as the last read of
the Hive schema found them."""

from collections.abc import Mapping
from typing import Any

from pydantic import BaseModel, ConfigDict
from sqlalchemy import Select, SQLColumnExpression, case, func, insert, select, update
from sqlalchemy.orm import Session

from dbimport.config_db.config_schema import ExportColumns, ExportTables
from dbimport.config_db.repositories.types import StoredDateTime


class HiveColumn(BaseModel):
    model_config = ConfigDict(frozen=True)

    column_name: str
    column_type: str | None
    column_order: int
    hive_db: str | None
    hive_table: str | None
    last_update_from_hive: str | None
    comment: str | None


class ExportColumn(BaseModel):
    model_config = ConfigDict(frozen=True, from_attributes=True)

    column_name: str
    column_type: str | None
    column_order: int | None
    target_column_name: str | None
    target_column_type: str | None
    last_update_from_hive: StoredDateTime
    include_in_export: int
    comment: str | None
    operator_notes: str | None


class ColumnDefinition(BaseModel):
    model_config = ConfigDict(frozen=True, from_attributes=True)

    hive_column_name: str
    target_column_name: str | None
    column_type: str | None
    comment: str | None


class ExportColumnsRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def column(self, table_id: int, column_name: str) -> ExportColumn | None:
        row = self._session.scalars(
            select(ExportColumns).where(
                ExportColumns.table_id == table_id,
                ExportColumns.column_name == column_name,
            )
        ).first()
        return None if row is None else ExportColumn.model_validate(row)

    def save_hive_column(self, table_id: int, column: HiveColumn) -> None:
        """Updates the stored column of the same name, or adds it; the
        settings of a stored column are kept."""
        column_id = self._session.scalars(
            select(ExportColumns.column_id).where(
                ExportColumns.table_id == table_id,
                ExportColumns.column_name == column.column_name,
            )
        ).first()
        if column_id is None:
            self._session.execute(
                insert(ExportColumns).values(table_id=table_id, **column.model_dump())
            )
        else:
            self._session.execute(
                update(ExportColumns)
                .where(ExportColumns.column_id == column_id)
                .values(**column.model_dump(exclude={"column_name"}))
            )

    def columns_of_table(
        self, dbalias: str, target_schema: str, target_table: str
    ) -> list[ExportColumn]:
        """Every stored column of the export table, also one no longer in
        Hive, in the order they were added."""
        rows = self._session.scalars(
            select(ExportColumns)
            .join(ExportTables, ExportTables.table_id == ExportColumns.table_id)
            .where(
                ExportTables.dbalias == dbalias,
                ExportTables.target_schema == target_schema,
                ExportTables.target_table == target_table,
            )
            .order_by(ExportColumns.column_id)
        )
        return [ExportColumn.model_validate(row) for row in rows]

    def save_column(self, table_id: int, settings: Mapping[str, object]) -> None:
        """Creates the column, or replaces the given settings on the stored
        column of the table with the same column_name. Raises
        MultipleResultsFound when more than one has that name."""
        column_id = self._session.scalars(
            select(ExportColumns.column_id).where(
                ExportColumns.table_id == table_id,
                ExportColumns.column_name == settings["column_name"],
            )
        ).one_or_none()
        if column_id is None:
            self._session.execute(
                insert(ExportColumns).values(table_id=table_id, **settings)
            )
        else:
            self._session.execute(
                update(ExportColumns)
                .where(
                    ExportColumns.table_id == table_id,
                    ExportColumns.column_id == column_id,
                )
                .values(**settings)
            )

    def has_target_column_names(self, table_id: int) -> bool:
        """True when any stored column, also one no longer in Hive, has a
        target_column_name."""
        count = self._session.scalar(
            select(func.count(ExportColumns.column_id)).where(
                ExportColumns.table_id == table_id,
                ExportColumns.target_column_name.is_not(None),
            )
        )
        return bool(count)

    def column_definitions(
        self,
        table_id: int,
        *,
        included_only: bool,
        uppercase: bool,
        target_types: bool,
    ) -> list[ColumnDefinition]:
        """The columns of the last read of the Hive schema, in their order;
        included_only leaves out excluded columns, uppercase converts the
        names in the database, target_types takes a target_column_type
        that is not blank instead of the Hive type."""
        hive_name: SQLColumnExpression[str | None] = ExportColumns.column_name
        target_name: SQLColumnExpression[str | None] = ExportColumns.target_column_name
        if uppercase:
            hive_name = func.upper(hive_name)
            target_name = func.upper(target_name)
        column_type: SQLColumnExpression[str | None] = ExportColumns.column_type
        if target_types:
            column_type = case(
                (
                    ExportColumns.target_column_type.is_not(None)
                    & (func.trim(ExportColumns.target_column_type) != ""),
                    ExportColumns.target_column_type,
                ),
                else_=ExportColumns.column_type,
            )
        query: Select[tuple[Any, ...]] = (
            select(
                hive_name.label("hive_column_name"),
                target_name.label("target_column_name"),
                column_type.label("column_type"),
                ExportColumns.comment,
            )
            .join(ExportTables, ExportTables.table_id == ExportColumns.table_id)
            .where(
                ExportTables.table_id == table_id,
                ExportTables.last_update_from_hive
                == ExportColumns.last_update_from_hive,
            )
            .order_by(ExportColumns.column_order)
        )
        if included_only:
            query = query.where(ExportColumns.include_in_export == 1)
        return [
            ColumnDefinition.model_validate(row._mapping)
            for row in self._session.execute(query)
        ]
