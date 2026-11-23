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
"""Import columns: the columns, indexes and foreign keys of an imported
table as the last read of the source schema found them."""

from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Any, TypeVar

from pydantic import BaseModel, ConfigDict
from sqlalchemy import Insert, Select, delete, func, insert, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, aliased

from dbimport.config_db.config_schema import (
    ImportColumns,
    ImportForeignKeys,
    ImportTables,
    ImportTablesIndexes,
    JDBCConnections,
)

_DUPLICATE_ENTRY = 1062

_Row = TypeVar("_Row", bound=tuple[Any, ...])


class ImportColumn(BaseModel):
    model_config = ConfigDict(frozen=True, from_attributes=True)

    column_id: int
    source_column_name: str
    column_name: str
    column_name_override: str | None
    column_type: str
    column_type_override: str | None
    sqoop_column_type: str | None
    sqoop_column_type_override: str | None
    include_in_import: int
    comment: str | None
    anonymization_function: str
    column_order: int | None
    source_column_type: str
    source_database_type: str | None
    force_string: int
    source_primary_key: int | None
    last_update_from_source: datetime
    operator_notes: str | None


class SourceColumn(BaseModel):
    """A column as an import saves it from the source schema."""

    model_config = ConfigDict(frozen=True)

    hive_db: str | None
    hive_table: str | None
    column_name: str
    column_order: int
    source_column_name: str
    column_type: str
    source_column_type: str
    source_database_type: str | None
    sqoop_column_type: str | None
    last_update_from_source: str | None
    comment: str | None


class ForeignKeyColumn(BaseModel):
    """One column of a foreign key, with the referenced column."""

    model_config = ConfigDict(frozen=True)

    source_hive_db: str | None
    source_hive_table: str | None
    source_column_name: str
    source_source_column_name: str
    ref_source_schema: str
    ref_source_table: str
    ref_column_name: str
    ref_source_column_name: str
    ref_hive_db: str | None
    ref_hive_table: str | None
    fk_index: int
    key_position: int


class ImportColumnsRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def column(self, table_id: int, source_column_name: str) -> ImportColumn | None:
        row = self._session.scalars(
            select(ImportColumns).where(
                ImportColumns.table_id == table_id,
                ImportColumns.source_column_name == source_column_name,
            )
        ).first()
        return None if row is None else ImportColumn.model_validate(row)

    def column_id(self, table_id: int, source_column_name: str) -> int | None:
        """Matches lower(source_column_name) in the collation of the column."""
        return self._session.scalars(
            select(ImportColumns.column_id).where(
                ImportColumns.table_id == table_id,
                func.lower(ImportColumns.source_column_name) == source_column_name,
            )
        ).first()

    def current_columns(
        self,
        table_id: int,
        *,
        imported_only: bool = False,
        column_names: Sequence[str] = (),
    ) -> list[ImportColumn]:
        """The columns of the last read of the source schema, in their
        order; imported_only leaves out excluded columns, column_names
        restricts them to the given names."""
        query = self._current(select(ImportColumns), table_id).order_by(
            ImportColumns.column_order
        )
        if imported_only:
            query = query.where(ImportColumns.include_in_import == 1)
        if column_names:
            query = query.where(ImportColumns.column_name.in_(column_names))
        return [
            ImportColumn.model_validate(row) for row in self._session.scalars(query)
        ]

    def primary_key_columns(self, table_id: int) -> list[str]:
        """Source names of the current primary key columns, in key order."""
        query = (
            self._current(select(ImportColumns.source_column_name), table_id)
            .where(ImportColumns.source_primary_key.is_not(None))
            .order_by(ImportColumns.source_primary_key)
        )
        return list(self._session.scalars(query))

    def force_string_settings(
        self, table_id: int, column_name: str
    ) -> list[tuple[int | None, int | None, int | None]]:
        """force_string of the connection, the table and the column: one
        row per stored column of that name, one row with None for the
        column when none is stored, no row when the table is missing."""
        query = (
            select(
                JDBCConnections.force_string,
                ImportTables.force_string,
                ImportColumns.force_string,
            )
            .select_from(ImportTables)
            .outerjoin(JDBCConnections, ImportTables.dbalias == JDBCConnections.dbalias)
            .outerjoin(
                ImportColumns,
                (ImportColumns.table_id == ImportTables.table_id)
                & (ImportColumns.column_name == column_name),
            )
            .where(ImportTables.table_id == table_id)
        )
        return list(self._session.execute(query).tuples())

    def anonymized_columns(self, table_id: int) -> list[tuple[str, str]]:
        """source_column_name and anonymization_function of every column
        with a function."""
        query = select(
            ImportColumns.source_column_name, ImportColumns.anonymization_function
        ).where(
            ImportColumns.table_id == table_id,
            ImportColumns.anonymization_function != "None",
        )
        return list(self._session.execute(query).tuples())

    def add_source_column(self, table_id: int, column: SourceColumn) -> None:
        self._session.execute(
            insert(ImportColumns).values(table_id=table_id, **column.model_dump())
        )

    def update_source_column(self, table_id: int, column: SourceColumn) -> None:
        """Updates the stored column of the same source name; its primary
        key position is cleared."""
        values = column.model_dump(exclude={"source_column_name"})
        self._session.execute(
            update(ImportColumns)
            .where(
                ImportColumns.table_id == table_id,
                ImportColumns.source_column_name == column.source_column_name,
            )
            .values(source_primary_key=None, **values)
        )

    def set_primary_key_position(
        self, table_id: int, source_column_name: str, position: object
    ) -> None:
        """Matches lower(source_column_name) in the collation of the column."""
        self._session.execute(
            update(ImportColumns)
            .where(
                ImportColumns.table_id == table_id,
                func.lower(ImportColumns.source_column_name) == source_column_name,
            )
            .values(source_primary_key=position)
        )

    def columns_of_table(self, hive_db: str, hive_table: str) -> list[ImportColumn]:
        """Every stored column with this Hive database and table, in their
        order."""
        rows = self._session.scalars(
            select(ImportColumns)
            .where(
                ImportColumns.hive_db == hive_db, ImportColumns.hive_table == hive_table
            )
            .order_by(ImportColumns.column_order)
        )
        return [ImportColumn.model_validate(row) for row in rows]

    def save_column(self, table_id: int, settings: Mapping[str, object]) -> None:
        """Creates the column, or replaces the given settings on the stored
        column of the table with the same column_name. Raises
        MultipleResultsFound when more than one has that name."""
        column_id = self._session.scalars(
            select(ImportColumns.column_id).where(
                ImportColumns.table_id == table_id,
                ImportColumns.column_name == settings["column_name"],
            )
        ).one_or_none()
        if column_id is None:
            self._session.execute(
                insert(ImportColumns).values(table_id=table_id, **settings)
            )
        else:
            self._session.execute(
                update(ImportColumns)
                .where(
                    ImportColumns.table_id == table_id,
                    ImportColumns.column_id == column_id,
                )
                .values(**settings)
            )

    def remove_indexes(self, table_id: int) -> None:
        self._session.execute(
            delete(ImportTablesIndexes).where(ImportTablesIndexes.table_id == table_id)
        )

    def add_index_column(
        self,
        table_id: int,
        hive_db: str | None,
        hive_table: str | None,
        index: dict[str, object],
    ) -> bool:
        """index: index_name, index_type, index_unique, column,
        column_type, column_order and column_is_nullable. False when the
        table has this index column already."""
        return self._insert_unless_duplicate(
            insert(ImportTablesIndexes).values(
                table_id=table_id, hive_db=hive_db, hive_table=hive_table, **index
            )
        )

    def imported_table_id(
        self, dbalias: str, source_schema: str, source_table: str
    ) -> int | None:
        """The table_id of one table imported from this source table."""
        return self._session.scalars(
            select(ImportTables.table_id)
            .where(
                ImportTables.dbalias == dbalias,
                ImportTables.source_schema == source_schema,
                ImportTables.source_table == source_table,
            )
            .limit(1)
        ).first()

    def add_foreign_key_column(
        self,
        table_id: int,
        column_id: int,
        fk_index: int,
        referenced: tuple[int, int],
        key_position: object,
    ) -> bool:
        """referenced: table_id and column_id. False when the foreign key
        column is stored already."""
        return self._insert_unless_duplicate(
            insert(ImportForeignKeys).values(
                table_id=table_id,
                column_id=column_id,
                fk_index=fk_index,
                fk_table_id=referenced[0],
                fk_column_id=referenced[1],
                key_position=key_position,
            )
        )

    def remove_foreign_keys(self, table_id: int) -> None:
        self._session.execute(
            delete(ImportForeignKeys).where(ImportForeignKeys.table_id == table_id)
        )

    def foreign_keys(self, table_id: int) -> list[ForeignKeyColumn]:
        """The foreign key columns of the table, by key and position."""
        own = aliased(ImportColumns)
        ref = aliased(ImportColumns)
        query = (
            select(
                own.hive_db.label("source_hive_db"),
                own.hive_table.label("source_hive_table"),
                func.lower(own.column_name).label("source_column_name"),
                func.lower(own.source_column_name).label("source_source_column_name"),
                ImportTables.source_schema.label("ref_source_schema"),
                ImportTables.source_table.label("ref_source_table"),
                ref.column_name.label("ref_column_name"),
                func.lower(ref.source_column_name).label("ref_source_column_name"),
                ref.hive_db.label("ref_hive_db"),
                ref.hive_table.label("ref_hive_table"),
                ImportForeignKeys.fk_index,
                ImportForeignKeys.key_position,
            )
            .select_from(ImportForeignKeys)
            .join(
                own,
                (own.table_id == ImportForeignKeys.table_id)
                & (own.column_id == ImportForeignKeys.column_id),
            )
            .join(
                ref,
                (ref.table_id == ImportForeignKeys.fk_table_id)
                & (ref.column_id == ImportForeignKeys.fk_column_id),
            )
            .join(ImportTables, ImportForeignKeys.fk_table_id == ImportTables.table_id)
            .where(ImportForeignKeys.table_id == table_id)
            .order_by(ImportForeignKeys.fk_index, ImportForeignKeys.key_position)
        )
        return [
            ForeignKeyColumn.model_validate(row._mapping)
            for row in self._session.execute(query)
        ]

    def _current(self, query: Select[_Row], table_id: int) -> Select[_Row]:
        return query.join(
            ImportTables,
            (ImportTables.table_id == ImportColumns.table_id)
            & (
                ImportTables.last_update_from_source
                == ImportColumns.last_update_from_source
            ),
        ).where(ImportColumns.table_id == table_id)

    def _insert_unless_duplicate(self, statement: Insert) -> bool:
        try:
            with self._session.begin_nested():
                self._session.execute(statement)
        except IntegrityError as err:
            if err.orig is None or err.orig.args[0] != _DUPLICATE_ENTRY:
                raise
            return False
        return True
