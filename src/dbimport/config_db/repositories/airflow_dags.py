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

from collections.abc import Mapping, Sequence
from datetime import time

from pydantic import BaseModel, ConfigDict
from sqlalchemy import bindparam, delete, func, literal, select, union_all, update
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.orm import Session

from dbimport.config_db.config_schema import (
    AirflowCustomDAGs,
    AirflowDAGSensors,
    AirflowETLDAGs,
    AirflowExportDAGs,
    AirflowImportDAGs,
    AirflowTasks,
)
from dbimport.config_db.repositories.settings import check_settings


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
    operator_notes: str | None
    application_notes: str | None
    airflow_notes: str | None


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
    include_in_airflow: int


class DAGSummary(BaseModel):
    """A DAG of any type, as the list of all DAGs shows it."""

    model_config = ConfigDict(frozen=True)

    dag_name: str
    dag_type: str
    schedule_interval: str
    auto_regenerate_dag: int


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


_DAGTable = type[AirflowImportDAGs] | type[AirflowExportDAGs] | type[AirflowCustomDAGs]

_IMPORT_SETTINGS = frozenset(ImportDAG.model_fields) - {"dag_name"}
_EXPORT_SETTINGS = frozenset(ExportDAG.model_fields) - {"dag_name"}
_CUSTOM_SETTINGS = frozenset(CustomDAG.model_fields) - {"dag_name"}
_BULK_SETTINGS = frozenset(
    {
        "schedule_interval",
        "operator_notes",
        "auto_regenerate_dag",
        "sudo_user",
        "timezone",
        "email",
        "email_on_failure",
        "email_on_retries",
        "sla_warning_time",
        "concurrency",
    }
)
_TASK_SETTINGS = frozenset(
    {
        "task_type",
        "placement",
        "jdbc_dbalias",
        "airflow_pool",
        "airflow_priority",
        "include_in_airflow",
        "task_dependency_downstream",
        "task_dependency_upstream",
        "task_config",
        "sensor_poke_interval",
        "sensor_timeout_minutes",
        "sensor_connection",
        "sensor_soft_fail",
        "sudo_user",
    }
)


class AirflowDAGsRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def import_dags(self, limit: int | None = None) -> list[ImportDAG]:
        """The import DAGs ordered by name, at most limit of them."""
        rows = self._session.scalars(
            select(AirflowImportDAGs).order_by(AirflowImportDAGs.dag_name).limit(limit)
        )
        return [ImportDAG.model_validate(row) for row in rows]

    def export_dags(self, limit: int | None = None) -> list[ExportDAG]:
        """The export DAGs ordered by name, at most limit of them."""
        rows = self._session.scalars(
            select(AirflowExportDAGs).order_by(AirflowExportDAGs.dag_name).limit(limit)
        )
        return [ExportDAG.model_validate(row) for row in rows]

    def import_dag(self, dag_name: str) -> ImportDAG | None:
        row = self._session.get(AirflowImportDAGs, dag_name)
        return None if row is None else ImportDAG.model_validate(row)

    def export_dag(self, dag_name: str) -> ExportDAG | None:
        row = self._session.get(AirflowExportDAGs, dag_name)
        return None if row is None else ExportDAG.model_validate(row)

    def dag_types_named(self, dag_name: str) -> set[str]:
        """The types (import, export, custom) that have a DAG of that
        name."""
        return {
            dag_type
            for dag_type, dags in (
                ("import", AirflowImportDAGs),
                ("export", AirflowExportDAGs),
                ("custom", AirflowCustomDAGs),
            )
            if self._session.get(dags, dag_name) is not None
        }

    def save_import_dag(self, dag_name: str, settings: Mapping[str, object]) -> None:
        """Creates the DAG with the given settings, or replaces them on the
        existing one. Raises KeyError for a column that is not a setting,
        ValueError for no settings."""
        check_settings(settings, _IMPORT_SETTINGS)
        self._save(AirflowImportDAGs, dag_name, settings)

    def save_export_dag(self, dag_name: str, settings: Mapping[str, object]) -> None:
        """Creates the DAG with the given settings, or replaces them on the
        existing one. Raises KeyError for a column that is not a setting,
        ValueError for no settings."""
        check_settings(settings, _EXPORT_SETTINGS)
        self._save(AirflowExportDAGs, dag_name, settings)

    def _save(
        self, dags: _DAGTable, dag_name: str, settings: Mapping[str, object]
    ) -> None:
        self._session.execute(
            mysql_insert(dags)
            .values(dag_name=dag_name, **settings)
            .on_duplicate_key_update(dict(settings))
        )

    def delete_import_dags(self, dag_names: Sequence[str]) -> None:
        """Deletes the DAGs and their tasks; unknown names are ignored."""
        self._delete(AirflowImportDAGs, dag_names)

    def delete_export_dags(self, dag_names: Sequence[str]) -> None:
        """Deletes the DAGs and their tasks; unknown names are ignored."""
        self._delete(AirflowExportDAGs, dag_names)

    def _delete(self, dags: _DAGTable, dag_names: Sequence[str]) -> None:
        names = [{"b_dag": name} for name in dag_names]
        connection = self._session.connection()
        # Only the tasks of a DAG of this type: the DAG types share names.
        connection.execute(
            delete(AirflowTasks).where(
                AirflowTasks.dag_name == bindparam("b_dag"),
                select(dags.dag_name)
                .where(dags.dag_name == bindparam("b_dag"))
                .exists(),
            ),
            names,
        )
        connection.execute(
            delete(dags).where(dags.dag_name == bindparam("b_dag")), names
        )

    def update_import_dags(
        self, dag_names: Sequence[str], settings: Mapping[str, object]
    ) -> None:
        """Sets the given settings on the DAGs. Raises KeyError for a column
        that cannot be changed in bulk."""
        self._update(AirflowImportDAGs, dag_names, settings)

    def update_export_dags(
        self, dag_names: Sequence[str], settings: Mapping[str, object]
    ) -> None:
        """Sets the given settings on the DAGs. Raises KeyError for a column
        that cannot be changed in bulk."""
        self._update(AirflowExportDAGs, dag_names, settings)

    def _update(
        self, dags: _DAGTable, dag_names: Sequence[str], settings: Mapping[str, object]
    ) -> None:
        check_settings(settings, _BULK_SETTINGS)
        self._session.connection().execute(
            update(dags).where(dags.dag_name == bindparam("b_dag")).values(settings),
            [{"b_dag": name} for name in dag_names],
        )

    def save_task(
        self, dag_name: str, task_name: str, settings: Mapping[str, object]
    ) -> None:
        """Creates the task with the given settings, or replaces them on the
        existing one. Raises KeyError for a column that is not a setting,
        ValueError for no settings."""
        check_settings(settings, _TASK_SETTINGS)
        self._session.execute(
            mysql_insert(AirflowTasks)
            .values(dag_name=dag_name, task_name=task_name, **settings)
            .on_duplicate_key_update(dict(settings))
        )

    def custom_dags(self, limit: int | None = None) -> list[CustomDAG]:
        """The custom DAGs ordered by name, at most limit of them."""
        rows = self._session.scalars(
            select(AirflowCustomDAGs).order_by(AirflowCustomDAGs.dag_name).limit(limit)
        )
        return [CustomDAG.model_validate(row) for row in rows]

    def custom_dag(self, dag_name: str) -> CustomDAG | None:
        row = self._session.get(AirflowCustomDAGs, dag_name)
        return None if row is None else CustomDAG.model_validate(row)

    def all_dags(self, limit: int | None = None) -> list[DAGSummary]:
        """The custom, then the import, then the export DAGs, each type
        ordered by name; at most limit of them."""
        dag_types: tuple[tuple[str, _DAGTable], ...] = (
            ("custom", AirflowCustomDAGs),
            ("import", AirflowImportDAGs),
            ("export", AirflowExportDAGs),
        )
        summaries = union_all(
            *(
                select(
                    dags.dag_name,
                    literal(dag_type).label("dag_type"),
                    dags.schedule_interval,
                    dags.auto_regenerate_dag,
                    literal(order).label("type_order"),
                )
                for order, (dag_type, dags) in enumerate(dag_types)
            )
        ).subquery()
        rows = self._session.execute(
            select(
                summaries.c.dag_name,
                summaries.c.dag_type,
                summaries.c.schedule_interval,
                summaries.c.auto_regenerate_dag,
            )
            .order_by(summaries.c.type_order, summaries.c.dag_name)
            .limit(limit)
        )
        return [DAGSummary.model_validate(row._mapping) for row in rows]

    def save_custom_dag(self, dag_name: str, settings: Mapping[str, object]) -> None:
        """Creates the DAG with the given settings, or replaces them on the
        existing one. Raises KeyError for a column that is not a setting,
        ValueError for no settings."""
        check_settings(settings, _CUSTOM_SETTINGS)
        self._save(AirflowCustomDAGs, dag_name, settings)

    def delete_custom_dags(self, dag_names: Sequence[str]) -> None:
        """Deletes the DAGs and their tasks; unknown names are ignored."""
        self._delete(AirflowCustomDAGs, dag_names)

    def update_custom_dags(
        self, dag_names: Sequence[str], settings: Mapping[str, object]
    ) -> None:
        """Sets the given settings on the DAGs. Raises KeyError for a column
        that cannot be changed in bulk."""
        self._update(AirflowCustomDAGs, dag_names, settings)

    def delete_task(self, dag_name: str, task_name: str) -> bool:
        """Deletes the task; False when the DAG has no such task."""
        result = self._session.execute(
            delete(AirflowTasks).where(
                AirflowTasks.dag_name == dag_name, AirflowTasks.task_name == task_name
            )
        )
        return bool(result.rowcount)

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

    def tasks(self, dag_name: str, *, included_only: bool = True) -> list[DAGTask]:
        """The tasks of the DAG ordered by name; with included_only, only
        those that are included in Airflow."""
        query = select(AirflowTasks).where(AirflowTasks.dag_name == dag_name)
        if included_only:
            query = query.where(AirflowTasks.include_in_airflow == 1)
        rows = self._session.scalars(query.order_by(AirflowTasks.task_name))
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
