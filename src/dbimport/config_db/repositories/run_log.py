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
"""Run logs: import failures, statistics documents kept for a later send,
and the Yarn applications of imports and exports."""

from datetime import datetime

from sqlalchemy import insert, update
from sqlalchemy.orm import Session

from dbimport.config_db.config_schema import (
    ImportFailureLog,
    JSONToSend,
    t_yarn_statistics,
)


class RunLogRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def log_import_failure(
        self,
        hive_db: str,
        hive_table: str,
        *,
        severity: str,
        import_type: str | None,
        error_text: str,
        eventtime: datetime,
    ) -> None:
        self._session.execute(
            insert(ImportFailureLog).values(
                hive_db=hive_db,
                hive_table=hive_table,
                eventtime=eventtime,
                severity=severity,
                import_type=import_type,
                error_text=error_text,
            )
        )

    def keep_json(self, datatype: str, destination: str, jsondata: str) -> None:
        """Stores a statistics document that could not be sent (status 0)."""
        self._session.execute(
            insert(JSONToSend).values(
                type=datatype, status=0, destination=destination, jsondata=jsondata
            )
        )

    def start_yarn_application(
        self,
        application_id: str,
        *,
        operation_type: str,
        operation_tool: str,
        operation_timestamp: datetime,
        application_start: datetime,
        hive_db: str | None = None,
        hive_table: str | None = None,
        dbalias: str | None = None,
        target_schema: str | None = None,
        target_table: str | None = None,
    ) -> None:
        """Adds the application with the Hive table of an import or the
        target table of an export."""
        self._session.execute(
            insert(t_yarn_statistics).values(
                yarn_application_id=application_id,
                operation_timestamp=operation_timestamp,
                operation_tool=operation_tool,
                operation_type=operation_type,
                application_start=application_start,
                hive_db=hive_db,
                hive_table=hive_table,
                dbalias=dbalias,
                target_schema=target_schema,
                target_table=target_table,
            )
        )

    def finish_yarn_application(
        self,
        application_id: str,
        application_start: datetime,
        *,
        application_stop: datetime,
        containers_total: int | None,
        containers_failed: int | None,
        containers_killed: int | None,
    ) -> None:
        self._session.execute(
            update(t_yarn_statistics)
            .where(
                t_yarn_statistics.c.yarn_application_id == application_id,
                t_yarn_statistics.c.application_start == application_start,
            )
            .values(
                application_stop=application_stop,
                yarn_containers_total=containers_total,
                yarn_containers_failed=containers_failed,
                yarn_containers_killed=containers_killed,
            )
        )
