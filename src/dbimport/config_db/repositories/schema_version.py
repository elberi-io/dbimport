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
"""The schema version that Alembic records in alembic_version."""

from sqlalchemy import select
from sqlalchemy.orm import Session

from dbimport.config_db.config_schema import AlembicVersion


class SchemaVersionRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def version(self) -> str | None:
        """The revision of the schema; None when no revision is recorded."""
        return self._session.scalars(select(AlembicVersion.version_num)).one_or_none()
