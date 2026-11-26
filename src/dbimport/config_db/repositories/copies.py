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
"""Copies to other DBImport instances: the instances, the copy rules of
copy_tables and the asynchronous copies in copy_async_status."""

from datetime import datetime

from pydantic import BaseModel, ConfigDict
from sqlalchemy import delete, insert, select, update
from sqlalchemy.orm import Session

from dbimport.config_db.config_schema import (
    CopyAsyncStatus,
    CopyTables,
    DBImportInstances,
)


class CopyRule(BaseModel):
    """A copy_tables row: tables matching hive_filter are copied to
    destination."""

    model_config = ConfigDict(frozen=True, from_attributes=True)

    copy_id: int
    hive_filter: str
    destination: str
    data_transfer: str


class HDFSLocation(BaseModel):
    model_config = ConfigDict(frozen=True, from_attributes=True)

    hdfs_address: str
    hdfs_basedir: str


class AsyncCopy(BaseModel):
    """A copy_async_status row as the server hands it to distcp."""

    model_config = ConfigDict(frozen=True, from_attributes=True)

    table_id: int
    hive_db: str
    hive_table: str
    destination: str
    failures: int
    hdfs_source_path: str
    hdfs_target_path: str


class CopiesRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def instance_exists(self, name: str) -> bool:
        return (
            self._session.scalars(
                select(DBImportInstances.name).where(DBImportInstances.name == name)
            ).first()
            is not None
        )

    def hdfs_location(self, name: str) -> HDFSLocation:
        """Raises NoResultFound for an unknown instance."""
        row = self._session.execute(
            select(
                DBImportInstances.hdfs_address, DBImportInstances.hdfs_basedir
            ).where(DBImportInstances.name == name)
        ).one()
        return HDFSLocation.model_validate(row._mapping)

    def save_credentials(self, name: str, credentials: str) -> None:
        """Stores the encrypted credentials of the instance. Raises
        NoResultFound for an unknown instance."""
        instance = self._session.scalars(
            select(DBImportInstances).where(DBImportInstances.name == name)
        ).one()
        instance.db_credentials = credentials

    def copy_rules(self) -> list[CopyRule]:
        rows = self._session.scalars(select(CopyTables).order_by(CopyTables.copy_id))
        return [CopyRule.model_validate(row) for row in rows]

    def async_copy_status(self, table_id: int, destination: str) -> int | None:
        """The copy_status of the table's asynchronous copy to destination;
        None when no copy is scheduled."""
        return self._session.scalars(
            select(CopyAsyncStatus.copy_status).where(
                CopyAsyncStatus.table_id == table_id,
                CopyAsyncStatus.destination == destination,
            )
        ).one_or_none()

    def has_async_copies(self, hive_db: str, hive_table: str, destination: str) -> bool:
        """Whether a copy to destination is scheduled for a table matching
        the hive_db and hive_table LIKE patterns."""
        return (
            self._session.scalars(
                select(CopyAsyncStatus.table_id).where(
                    CopyAsyncStatus.hive_db.like(hive_db),
                    CopyAsyncStatus.hive_table.like(hive_table),
                    CopyAsyncStatus.destination == destination,
                )
            ).first()
            is not None
        )

    def schedule_async_copy(
        self,
        table_id: int,
        hive_db: str,
        hive_table: str,
        destination: str,
        *,
        source_path: str,
        target_path: str,
    ) -> None:
        """Adds a copy that has not started (copy_status 0)."""
        self._session.execute(
            insert(CopyAsyncStatus).values(
                table_id=table_id,
                hive_db=hive_db,
                hive_table=hive_table,
                destination=destination,
                hdfs_source_path=source_path,
                hdfs_target_path=target_path,
                copy_status=0,
            )
        )

    def reschedule_async_copy(
        self,
        table_id: int,
        destination: str,
        *,
        source_path: str,
        target_path: str,
        updated: datetime,
    ) -> None:
        """Sets new paths on a copy that has not started."""
        self._session.execute(
            update(CopyAsyncStatus)
            .where(
                CopyAsyncStatus.table_id == table_id,
                CopyAsyncStatus.destination == destination,
            )
            .values(
                hdfs_source_path=source_path,
                hdfs_target_path=target_path,
                last_status_update=updated,
            )
        )

    def restart_started_copies(self, updated: datetime) -> None:
        """Sets every copy handed to distcp (copy_status 1) back to not
        started."""
        self._session.execute(
            update(CopyAsyncStatus)
            .where(CopyAsyncStatus.copy_status == 1)
            .values(copy_status=0, last_status_update=updated)
        )

    def copies_to_start(self, retry_failed_before: datetime) -> list[AsyncCopy]:
        """The copies not started, and the failed ones (copy_status 2) last
        updated at or before retry_failed_before."""
        rows = self._session.scalars(
            select(CopyAsyncStatus).where(
                (CopyAsyncStatus.copy_status == 0)
                | (
                    (CopyAsyncStatus.copy_status == 2)
                    & (CopyAsyncStatus.last_status_update <= retry_failed_before)
                )
            )
        )
        return [AsyncCopy.model_validate(row) for row in rows]

    def finished_copies(self) -> list[AsyncCopy]:
        """The copies distcp has finished (copy_status 3)."""
        rows = self._session.scalars(
            select(CopyAsyncStatus).where(CopyAsyncStatus.copy_status == 3)
        )
        return [AsyncCopy.model_validate(row) for row in rows]

    def set_async_copy_status(
        self,
        table_id: int,
        destination: str,
        copy_status: int,
        *,
        updated: datetime,
        failures: int | None = None,
    ) -> None:
        """Sets copy_status, and failures when given."""
        values: dict[str, object] = {
            "copy_status": copy_status,
            "last_status_update": updated,
        }
        if failures is not None:
            values["failures"] = failures
        self._session.execute(
            update(CopyAsyncStatus)
            .where(
                CopyAsyncStatus.table_id == table_id,
                CopyAsyncStatus.destination == destination,
            )
            .values(values)
        )

    def delete_async_copy(self, table_id: int, destination: str) -> None:
        self._session.execute(
            delete(CopyAsyncStatus).where(
                CopyAsyncStatus.table_id == table_id,
                CopyAsyncStatus.destination == destination,
            )
        )
