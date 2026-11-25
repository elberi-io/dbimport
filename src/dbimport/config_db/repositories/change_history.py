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
"""Change history: the column changes found in Hive tables and in JDBC
tables."""

from datetime import datetime

from sqlalchemy import insert
from sqlalchemy.orm import Session

from dbimport.config_db.config_schema import (
    t_jdbc_table_change_history,
    t_table_change_history,
)


class ChangeHistoryRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def log_hive_change(
        self,
        hive_db: str,
        hive_table: str,
        column_name: str,
        event: str,
        *,
        previous_value: str | None,
        value: str | None,
        description: str,
        eventtime: datetime,
    ) -> None:
        self._session.execute(
            insert(t_table_change_history).values(
                hive_db=hive_db,
                hive_table=hive_table,
                column_name=column_name,
                eventtime=eventtime,
                event=event,
                previous_value=previous_value,
                value=value,
                description=description,
            )
        )

    def log_jdbc_change(
        self,
        dbalias: str,
        db_name: str,
        schema_name: str,
        table_name: str,
        column_name: str,
        event: str,
        *,
        previous_value: str | None,
        value: str | None,
        description: str,
        eventtime: datetime,
    ) -> None:
        self._session.execute(
            insert(t_jdbc_table_change_history).values(
                dbalias=dbalias,
                db_name=db_name,
                schema_name=schema_name,
                table_name=table_name,
                column_name=column_name,
                eventtime=eventtime,
                event=event,
                previous_value=previous_value,
                value=value,
                description=description,
            )
        )
