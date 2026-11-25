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
"""Export statistics: one row per finished export run, and the last run
of each table."""

from collections.abc import Mapping

from sqlalchemy import delete, insert
from sqlalchemy.orm import Session

from dbimport.config_db.config_schema import ExportStatistic, ExportStatisticsLast


class ExportStatisticsRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def save_run(
        self,
        dbalias: str,
        target_schema: str,
        target_table: str,
        statistics: Mapping[str, object],
    ) -> None:
        """Adds the statistics of a run, keyed by column name, and makes
        them the last statistics of the table. A key that is no column
        raises CompileError."""
        self._session.execute(insert(ExportStatistic).values(**statistics))
        self._session.execute(
            delete(ExportStatisticsLast).where(
                ExportStatisticsLast.dbalias == dbalias,
                ExportStatisticsLast.target_schema == target_schema,
                ExportStatisticsLast.target_table == target_table,
            )
        )
        self._session.execute(insert(ExportStatisticsLast).values(**statistics))
