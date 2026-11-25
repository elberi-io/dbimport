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
"""Airflow DAGs: the import, export and custom DAGs DBImport generates,
their extra tasks and the sensors that wait for other DAGs."""

from datetime import time

from pydantic import BaseModel, ConfigDict
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from dbimport.config_db.config_schema import (
    AirflowCustomDAGs,
    AirflowDAGSensors,
    AirflowETLDAGs,
    AirflowExportDAGs,
    AirflowImportDAGs,
    AirflowTasks,
)


class _DAG(BaseModel):
    model_config = ConfigDict(frozen=True, from_attributes=True)

    dag_name: str
    schedule_interval: str
    auto_regenerate_dag: int
    sudo_user: str | None
    timezone: str | None
    email: str | None
    email_on_failure: int
    email_on_retries: int
    tags: str | None
    sla_warning_time: time | None
    retry_exponential_backoff: int
    concurrency: int | None


class ImportDAG(_DAG):
    filter_hive: str
    retries: int
    retries_stage1: int | None
    retries_stage2: int | None
    pool_stage1: str | None
    pool_stage2: str | None
    run_import_and_etl_separate: int
    finish_all_stage1_first: int
    metadata_import: int


class ExportDAG(_DAG):
    filter_dbalias: str
    filter_target_schema: str | None
    filter_target_table: str | None
    retries: int | None


class CustomDAG(_DAG):
    retries: int


class DAGTask(BaseModel):
    model_config = ConfigDict(frozen=True, from_attributes=True)

    task_name: str
    task_type: str
    placement: str
    airflow_pool: str | None
    airflow_priority: int | None
    task_dependency_downstream: str | None
    task_dependency_upstream: str | None
    task_config: str | None
    jdbc_dbalias: str | None
    hive_db: str | None
    sensor_poke_interval: int | None
    sensor_timeout_minutes: int | None
    sensor_connection: str | None
    sensor_soft_fail: int | None
    sudo_user: str | None


class DAGSensor(BaseModel):
    """A sensor with the schedule of the DAG it waits for, per DAG type."""

    model_config = ConfigDict(frozen=True, from_attributes=True)

    dag_name: str
    sensor_name: str
    wait_for_dag: str
    wait_for_task: str | None
    timeout_minutes: int | None
    sensor_soft_fail: int | None
    import_schedule: str | None
    export_schedule: str | None
    custom_schedule: str | None
    etl_schedule: str | None


class AirflowDAGsRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def import_dags(self) -> list[ImportDAG]:
        rows = self._session.scalars(
            select(AirflowImportDAGs).order_by(AirflowImportDAGs.dag_name)
        )
        return [ImportDAG.model_validate(row) for row in rows]

    def export_dags(self) -> list[ExportDAG]:
        rows = self._session.scalars(
            select(AirflowExportDAGs).order_by(AirflowExportDAGs.dag_name)
        )
        return [ExportDAG.model_validate(row) for row in rows]

    def custom_dags(self) -> list[CustomDAG]:
        rows = self._session.scalars(
            select(AirflowCustomDAGs).order_by(AirflowCustomDAGs.dag_name)
        )
        return [CustomDAG.model_validate(row) for row in rows]

    def schedule_interval(self, dag_name: str) -> str | None:
        """The schedule of the import, export, custom or ETL DAG of that
        name, looked up in this order; None when there is no such DAG."""
        for dags in (
            AirflowImportDAGs,
            AirflowExportDAGs,
            AirflowCustomDAGs,
            AirflowETLDAGs,
        ):
            schedule = self._session.scalars(
                select(dags.schedule_interval).where(dags.dag_name == dag_name)
            ).one_or_none()
            if schedule is not None:
                return schedule
        return None

    def tasks(self, dag_name: str) -> list[DAGTask]:
        """The tasks of the DAG that are included in Airflow."""
        rows = self._session.scalars(
            select(AirflowTasks)
            .where(
                AirflowTasks.dag_name == dag_name,
                AirflowTasks.include_in_airflow == 1,
            )
            .order_by(AirflowTasks.task_name)
        )
        return [DAGTask.model_validate(row) for row in rows]

    def has_sensors(self, dag_name: str) -> bool:
        count = self._session.scalar(
            select(func.count())
            .select_from(AirflowDAGSensors)
            .where(AirflowDAGSensors.dag_name == dag_name)
        )
        return bool(count)

    def sensors(self, dag_name: str) -> list[DAGSensor]:
        rows = self._session.execute(
            select(
                AirflowDAGSensors.dag_name,
                AirflowDAGSensors.sensor_name,
                AirflowDAGSensors.wait_for_dag,
                AirflowDAGSensors.wait_for_task,
                AirflowDAGSensors.timeout_minutes,
                AirflowDAGSensors.sensor_soft_fail,
                AirflowImportDAGs.schedule_interval.label("import_schedule"),
                AirflowExportDAGs.schedule_interval.label("export_schedule"),
                AirflowCustomDAGs.schedule_interval.label("custom_schedule"),
                AirflowETLDAGs.schedule_interval.label("etl_schedule"),
            )
            .outerjoin(
                AirflowImportDAGs,
                AirflowDAGSensors.wait_for_dag == AirflowImportDAGs.dag_name,
            )
            .outerjoin(
                AirflowExportDAGs,
                AirflowDAGSensors.wait_for_dag == AirflowExportDAGs.dag_name,
            )
            .outerjoin(
                AirflowCustomDAGs,
                AirflowDAGSensors.wait_for_dag == AirflowCustomDAGs.dag_name,
            )
            .outerjoin(
                AirflowETLDAGs,
                AirflowDAGSensors.wait_for_dag == AirflowETLDAGs.dag_name,
            )
            .where(AirflowDAGSensors.dag_name == dag_name)
            .order_by(AirflowDAGSensors.sensor_name)
        )
        return [DAGSensor.model_validate(row._mapping) for row in rows]
