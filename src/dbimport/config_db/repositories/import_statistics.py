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
"""Import statistics: one row per finished import run, and the last run
of each table."""

from collections.abc import Mapping

from sqlalchemy import delete, insert
from sqlalchemy.orm import Session

from dbimport.config_db.config_schema import ImportStatistic, ImportStatisticsLast


class ImportStatisticsRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def save_run(
        self, hive_db: str, hive_table: str, statistics: Mapping[str, object]
    ) -> None:
        """Adds the statistics of a run, keyed by column name, and makes
        them the last statistics of the table. A key that is no column
        raises CompileError."""
        self._session.execute(insert(ImportStatistic).values(**statistics))
        self.clear_last(hive_db, hive_table)
        self._session.execute(insert(ImportStatisticsLast).values(**statistics))

    def clear_last(self, hive_db: str, hive_table: str) -> None:
        self._session.execute(
            delete(ImportStatisticsLast).where(
                ImportStatisticsLast.hive_db == hive_db,
                ImportStatisticsLast.hive_table == hive_table,
            )
        )
