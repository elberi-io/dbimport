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
"""The Atlas discovery cache: the columns and keys of source tables as
they were last sent to Atlas (atlas_column_cache, atlas_key_cache)."""

from collections.abc import Sequence
from datetime import datetime

from pydantic import BaseModel, ConfigDict
from sqlalchemy import delete, insert, select
from sqlalchemy.orm import Session

from dbimport.config_db.config_schema import AtlasColumnCache, AtlasKeyCache


class CachedColumn(BaseModel):
    """An atlas_column_cache row. The field order is the column order of
    the frame Atlas discovery builds from the source: it compares the two
    frames with DataFrame.equals, which also compares the column order."""

    model_config = ConfigDict(frozen=True, from_attributes=True)

    hostname: str
    port: str
    database_name: str
    schema_name: str
    table_name: str
    table_comment: str | None
    column_name: str
    column_type: str | None
    column_length: str | None
    column_comment: str | None
    column_is_nullable: str | None
    table_type: str | None
    table_create_time: datetime | None
    default_value: str | None


class CachedKey(BaseModel):
    """An atlas_key_cache row: one column of a primary or foreign key."""

    model_config = ConfigDict(frozen=True, from_attributes=True)

    hostname: str
    port: str
    database_name: str
    schema_name: str
    table_name: str
    constraint_name: str
    constraint_type: str | None
    column_name: str
    reference_schema_name: str | None
    reference_table_name: str | None
    reference_column_name: str | None
    col_key_position: int | None


class AtlasCacheRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def columns(
        self, hostname: str, port: str, database_name: str, schema_name: str
    ) -> list[CachedColumn]:
        """The cached columns of all tables in one schema of a source
        database."""
        rows = self._session.scalars(
            select(AtlasColumnCache).where(
                AtlasColumnCache.hostname == hostname,
                AtlasColumnCache.port == port,
                AtlasColumnCache.database_name == database_name,
                AtlasColumnCache.schema_name == schema_name,
            )
        )
        return [CachedColumn.model_validate(row) for row in rows]

    def keys(
        self, hostname: str, port: str, database_name: str, schema_name: str
    ) -> list[CachedKey]:
        """The cached key columns of all tables in one schema of a source
        database."""
        rows = self._session.scalars(
            select(AtlasKeyCache).where(
                AtlasKeyCache.hostname == hostname,
                AtlasKeyCache.port == port,
                AtlasKeyCache.database_name == database_name,
                AtlasKeyCache.schema_name == schema_name,
            )
        )
        return [CachedKey.model_validate(row) for row in rows]

    def replace_table(
        self,
        hostname: str,
        port: str,
        database_name: str,
        schema_name: str,
        table_name: str,
        columns: Sequence[CachedColumn],
        keys: Sequence[CachedKey],
    ) -> None:
        """Replaces the cached columns and keys of one table with the given
        ones."""
        for model in (AtlasColumnCache, AtlasKeyCache):
            self._session.execute(
                delete(model).where(
                    model.hostname == hostname,
                    model.port == port,
                    model.database_name == database_name,
                    model.schema_name == schema_name,
                    model.table_name == table_name,
                )
            )
        if columns:
            self._session.execute(
                insert(AtlasColumnCache), [column.model_dump() for column in columns]
            )
        if keys:
            self._session.execute(
                insert(AtlasKeyCache), [key.model_dump() for key in keys]
            )
