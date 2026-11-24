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
"""Import stages: how far the last run of an imported table got, the time
it spent in each stage, and its retries."""

from datetime import datetime

from pydantic import BaseModel, ConfigDict
from sqlalchemy import delete, func, insert, select, update
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.orm import Session

from dbimport.config_db.config_schema import (
    ImportRetriesLog,
    ImportStage,
    ImportStageStatistic,
    ImportTables,
)
from dbimport.config_db.repositories.types import StoredDateTime


class StageTime(BaseModel):
    model_config = ConfigDict(frozen=True, from_attributes=True)

    stage: int
    start: StoredDateTime
    stop: StoredDateTime


class ActiveImportStage(BaseModel):
    model_config = ConfigDict(frozen=True, from_attributes=True)

    hive_db: str
    hive_table: str
    stage: int


class ImportStagesRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def stage(self, hive_db: str, hive_table: str) -> int | None:
        return self._session.scalars(
            select(ImportStage.stage).where(
                ImportStage.hive_db == hive_db, ImportStage.hive_table == hive_table
            )
        ).first()

    def save_stage(
        self,
        hive_db: str,
        hive_table: str,
        stage: int,
        description: str,
        stage_time: datetime,
    ) -> None:
        """Replaces the stored stage of the table, or adds it."""
        stored = self._session.scalar(
            select(func.count(ImportStage.stage)).where(
                ImportStage.hive_db == hive_db, ImportStage.hive_table == hive_table
            )
        )
        if stored == 0:
            self._session.execute(
                insert(ImportStage).values(
                    hive_db=hive_db,
                    hive_table=hive_table,
                    stage=stage,
                    stage_description=description,
                    stage_time=stage_time,
                )
            )
        else:
            self._session.execute(
                update(ImportStage)
                .where(
                    ImportStage.hive_db == hive_db,
                    ImportStage.hive_table == hive_table,
                )
                .values(
                    stage=stage, stage_description=description, stage_time=stage_time
                )
            )

    def save_stage_time(
        self,
        hive_db: str,
        hive_table: str,
        stage: int,
        start: datetime,
        stop: datetime,
        duration: int,
    ) -> None:
        """Stores the time spent in the stage; replaces the time of an
        earlier attempt."""
        self._session.execute(
            mysql_insert(ImportStageStatistic)
            .values(
                hive_db=hive_db,
                hive_table=hive_table,
                stage=stage,
                start=start,
                stop=stop,
                duration=duration,
            )
            .on_duplicate_key_update(start=start, stop=stop, duration=duration)
        )

    def stage_times(self, hive_db: str, hive_table: str) -> list[StageTime]:
        rows = self._session.scalars(
            select(ImportStageStatistic)
            .where(
                ImportStageStatistic.hive_db == hive_db,
                ImportStageStatistic.hive_table == hive_table,
            )
            .order_by(ImportStageStatistic.stage)
        )
        return [StageTime.model_validate(row) for row in rows]

    def clear(self, hive_db: str, hive_table: str) -> None:
        """Deletes the stored stage and the stage times of the table."""
        self._session.execute(
            delete(ImportStage).where(
                ImportStage.hive_db == hive_db, ImportStage.hive_table == hive_table
            )
        )
        self._session.execute(
            delete(ImportStageStatistic).where(
                ImportStageStatistic.hive_db == hive_db,
                ImportStageStatistic.hive_table == hive_table,
            )
        )

    def log_retry(
        self,
        hive_db: str,
        hive_table: str,
        stage: int,
        description: str,
        retry_time: datetime,
    ) -> None:
        self._session.execute(
            insert(ImportRetriesLog).values(
                hive_db=hive_db,
                hive_table=hive_table,
                retry_time=retry_time,
                stage=stage,
                stage_description=description,
            )
        )

    def active_incremental_stages(self) -> list[ActiveImportStage]:
        """The stored stages of incremental imports (import_phase_type incr,
        or a legacy import_type starting with incr)."""
        rows = self._session.execute(
            select(ImportStage.hive_db, ImportStage.hive_table, ImportStage.stage)
            .join(
                ImportTables,
                (ImportTables.hive_db == ImportStage.hive_db)
                & (ImportTables.hive_table == ImportStage.hive_table),
            )
            .where(
                ImportTables.import_type.like("incr%")
                | (ImportTables.import_phase_type == "incr")
            )
        )
        return [ActiveImportStage.model_validate(row._mapping) for row in rows]
