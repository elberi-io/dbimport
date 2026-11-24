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
"""Export stages: how far the last run of an exported table got, the time
it spent in each stage, and its retries."""

from datetime import datetime

from pydantic import BaseModel, ConfigDict
from sqlalchemy import ColumnElement, delete, func, insert, select, update
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.orm import Session

from dbimport.config_db.config_schema import (
    ExportRetriesLog,
    ExportStage,
    ExportStageStatistic,
)
from dbimport.config_db.repositories.types import StoredDateTime


class StageTime(BaseModel):
    model_config = ConfigDict(frozen=True, from_attributes=True)

    stage: int
    start: StoredDateTime
    stop: StoredDateTime
    duration: int


class ExportStagesRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def stage(self, dbalias: str, target_schema: str, target_table: str) -> int | None:
        return self._session.scalars(
            select(ExportStage.stage).where(
                *_stage_of(dbalias, target_schema, target_table)
            )
        ).first()

    def save_stage(
        self,
        dbalias: str,
        target_schema: str,
        target_table: str,
        stage: int,
        description: str,
        stage_time: datetime,
    ) -> None:
        """Replaces the stored stage of the table, or adds it."""
        key = _stage_of(dbalias, target_schema, target_table)
        stored = self._session.scalar(select(func.count(ExportStage.stage)).where(*key))
        if stored == 0:
            self._session.execute(
                insert(ExportStage).values(
                    dbalias=dbalias,
                    target_schema=target_schema,
                    target_table=target_table,
                    stage=stage,
                    stage_description=description,
                    stage_time=stage_time,
                )
            )
        else:
            self._session.execute(
                update(ExportStage)
                .where(*key)
                .values(
                    stage=stage, stage_description=description, stage_time=stage_time
                )
            )

    def save_stage_time(
        self,
        dbalias: str,
        target_schema: str,
        target_table: str,
        stage: int,
        start: datetime,
        stop: datetime,
        duration: int,
    ) -> None:
        """Stores the time spent in the stage; replaces the time of an
        earlier attempt."""
        self._session.execute(
            mysql_insert(ExportStageStatistic)
            .values(
                dbalias=dbalias,
                target_schema=target_schema,
                target_table=target_table,
                stage=stage,
                start=start,
                stop=stop,
                duration=duration,
            )
            .on_duplicate_key_update(start=start, stop=stop, duration=duration)
        )

    def stage_times(
        self, dbalias: str, target_schema: str, target_table: str
    ) -> list[StageTime]:
        rows = self._session.scalars(
            select(ExportStageStatistic)
            .where(*_times_of(dbalias, target_schema, target_table))
            .order_by(ExportStageStatistic.stage)
        )
        return [StageTime.model_validate(row) for row in rows]

    def clear(self, dbalias: str, target_schema: str, target_table: str) -> None:
        """Deletes the stored stage and the stage times of the table."""
        self._session.execute(
            delete(ExportStage).where(*_stage_of(dbalias, target_schema, target_table))
        )
        self._session.execute(
            delete(ExportStageStatistic).where(
                *_times_of(dbalias, target_schema, target_table)
            )
        )

    def log_retry(
        self,
        dbalias: str,
        target_schema: str,
        target_table: str,
        stage: int,
        description: str,
        retry_time: datetime,
    ) -> None:
        self._session.execute(
            insert(ExportRetriesLog).values(
                dbalias=dbalias,
                target_schema=target_schema,
                target_table=target_table,
                retry_time=retry_time,
                stage=stage,
                stage_description=description,
            )
        )


def _stage_of(
    dbalias: str, target_schema: str, target_table: str
) -> tuple[ColumnElement[bool], ...]:
    return (
        ExportStage.dbalias == dbalias,
        ExportStage.target_schema == target_schema,
        ExportStage.target_table == target_table,
    )


def _times_of(
    dbalias: str, target_schema: str, target_table: str
) -> tuple[ColumnElement[bool], ...]:
    return (
        ExportStageStatistic.dbalias == dbalias,
        ExportStageStatistic.target_schema == target_schema,
        ExportStageStatistic.target_table == target_table,
    )
