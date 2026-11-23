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
"""JDBC connections, their drivers and the connection environments."""

from collections.abc import Mapping
from datetime import datetime, time

from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import delete, func, insert, select, update
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.orm import InstrumentedAttribute, Session
from sqlalchemy.sql import ColumnElement

from dbimport.config_db.config_schema import (
    JDBCConnections,
    JDBCConnectionsDrivers,
    JDBCConnectionsEnvironments,
)
from dbimport.config_db.repositories.types import StoredDateTime

CLASSPATH_PLACEHOLDER = "add path to JAR file"


class JDBCDriver(BaseModel):
    model_config = ConfigDict(frozen=True, from_attributes=True)

    database_type: str
    version: str
    driver: str
    classpath: str = CLASSPATH_PLACEHOLDER


DEFAULT_DRIVERS: tuple[JDBCDriver, ...] = (
    JDBCDriver(
        database_type="DB2 AS400",
        version="default",
        driver="com.ibm.as400.access.AS400JDBCDriver",
    ),
    JDBCDriver(
        database_type="DB2 UDB", version="default", driver="com.ibm.db2.jcc.DB2Driver"
    ),
    JDBCDriver(
        database_type="MySQL", version="default", driver="com.mysql.jdbc.Driver"
    ),
    JDBCDriver(
        database_type="Oracle",
        version="default",
        driver="oracle.jdbc.driver.OracleDriver",
    ),
    JDBCDriver(
        database_type="PostgreSQL", version="default", driver="org.postgresql.Driver"
    ),
    JDBCDriver(
        database_type="Progress DB",
        version="default",
        driver="com.ddtek.jdbc.openedge.OpenEdgeDriver",
    ),
    JDBCDriver(
        database_type="SQL Server",
        version="default",
        driver="com.microsoft.sqlserver.jdbc.SQLServerDriver",
    ),
    JDBCDriver(
        database_type="SQL Server",
        version="jTDS",
        driver="net.sourceforge.jtds.jdbc.Driver",
    ),
    JDBCDriver(
        database_type="CacheDB",
        version="default",
        driver="com.intersys.jdbc.CacheDriver",
    ),
    JDBCDriver(
        database_type="Snowflake",
        version="default",
        driver="net.snowflake.client.jdbc.SnowflakeDriver",
    ),
    JDBCDriver(
        database_type="Informix",
        version="default",
        driver="com.informix.jdbc.IfxDriver",
    ),
    JDBCDriver(
        database_type="SQL Anywhere",
        version="default",
        driver="com.sybase.jdbc4.jdbc.SybDriver",
    ),
)


class JDBCConnection(BaseModel):
    model_config = ConfigDict(frozen=True, from_attributes=True)

    dbalias: str
    jdbc_url: str
    private_key_path: str | None
    public_key_path: str | None
    credentials: str | None = Field(repr=False)
    datalake_source: str | None
    max_import_sessions: int | None
    force_string: int
    create_datalake_import: int
    timewindow_start: time | None
    timewindow_stop: time | None
    timewindow_timezone: str | None
    operator_notes: str | None
    create_foreign_keys: int
    contact_info: str | None
    description: str | None
    owner: str | None
    atlas_discovery: int
    atlas_include_filter: str | None
    atlas_exclude_filter: str | None
    atlas_last_discovery: StoredDateTime
    environment: str | None
    seed_file: str | None


_SEARCHABLE: dict[
    str, InstrumentedAttribute[str] | InstrumentedAttribute[str | None]
] = {
    "dbalias": JDBCConnections.dbalias,
    "jdbc_url": JDBCConnections.jdbc_url,
    "timewindow_timezone": JDBCConnections.timewindow_timezone,
    "operator_notes": JDBCConnections.operator_notes,
    "contact_info": JDBCConnections.contact_info,
    "description": JDBCConnections.description,
    "owner": JDBCConnections.owner,
}

DEFAULT_ENVIRONMENTS = (
    "Production",
    "Integration Test",
    "System Test",
    "Development",
    "Sandbox",
)


class ConnectionsRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def drivers(self) -> list[JDBCDriver]:
        """All drivers, in the order of the database_type ENUM and then of
        version."""
        rows = self._session.scalars(
            select(JDBCConnectionsDrivers).order_by(
                JDBCConnectionsDrivers.database_type, JDBCConnectionsDrivers.version
            )
        )
        return [JDBCDriver.model_validate(row) for row in rows]

    def classpaths(self) -> list[str]:
        """The classpath of every driver."""
        return list(self._session.scalars(select(JDBCConnectionsDrivers.classpath)))

    def driver(self, database_type: str, version: str) -> JDBCDriver | None:
        row = self._session.get(JDBCConnectionsDrivers, (database_type, version))
        return None if row is None else JDBCDriver.model_validate(row)

    def update_driver(self, driver: JDBCDriver) -> bool:
        """Stores driver and classpath of an existing database type and
        version; False when there is no such row."""
        result = self._session.execute(
            update(JDBCConnectionsDrivers)
            .where(
                JDBCConnectionsDrivers.database_type == driver.database_type,
                JDBCConnectionsDrivers.version == driver.version,
            )
            .values(driver=driver.driver, classpath=driver.classpath)
        )
        return result.rowcount == 1

    def seed_drivers(self) -> None:
        """Adds the DEFAULT_DRIVERS of each database type that has no driver
        row; a database type with a row keeps its rows as they are."""
        present = set(
            self._session.scalars(select(JDBCConnectionsDrivers.database_type))
        )
        missing = [d for d in DEFAULT_DRIVERS if d.database_type not in present]
        if missing:
            self._session.execute(
                insert(JDBCConnectionsDrivers), [d.model_dump() for d in missing]
            )

    def seed_environments(self) -> None:
        """Adds DEFAULT_ENVIRONMENTS when the table is empty."""
        count = self._session.scalar(
            select(func.count()).select_from(JDBCConnectionsEnvironments)
        )
        if count == 0:
            self._session.execute(
                insert(JDBCConnectionsEnvironments),
                [{"environment": name} for name in DEFAULT_ENVIRONMENTS],
            )

    def connection(self, dbalias: str) -> JDBCConnection | None:
        row = self._session.get(JDBCConnections, dbalias)
        return None if row is None else JDBCConnection.model_validate(row)

    def set_credentials(self, dbalias: str, credentials: str) -> None:
        """Stores the encrypted credentials of a connection."""
        self._session.execute(
            update(JDBCConnections)
            .where(JDBCConnections.dbalias == dbalias)
            .values(credentials=credentials)
        )

    def atlas_candidates(self) -> list[JDBCConnection]:
        """Connections with Atlas discovery on, the longest undiscovered
        first (never discovered before all others)."""
        rows = self._session.scalars(
            select(JDBCConnections)
            .where(JDBCConnections.atlas_discovery == 1)
            .order_by(JDBCConnections.atlas_last_discovery)
        )
        return [JDBCConnection.model_validate(row) for row in rows]

    def set_atlas_last_discovery(self, dbalias: str, discovered: datetime) -> None:
        self._session.execute(
            update(JDBCConnections)
            .where(JDBCConnections.dbalias == dbalias)
            .values(atlas_last_discovery=discovered)
        )

    def aliases_and_urls(self, limit: int) -> list[tuple[str, str]]:
        """Alias and JDBC URL of at most limit connections, ordered by alias."""
        rows = self._session.execute(
            select(JDBCConnections.dbalias, JDBCConnections.jdbc_url)
            .order_by(JDBCConnections.dbalias)
            .limit(limit)
        )
        return list(rows.tuples())

    def search(
        self, filters: Mapping[str, str], limit: int
    ) -> tuple[list[tuple[str, str]], int]:
        """Alias and JDBC URL of at most limit connections whose fields match
        all filters, ordered by alias, and the number of all matches. A
        filter value containing "%" is a LIKE pattern; others must be equal.
        Raises KeyError for a field that cannot be searched."""
        conditions: list[ColumnElement[bool]] = [
            _SEARCHABLE[field].like(value)
            if "%" in value
            else _SEARCHABLE[field] == value
            for field, value in filters.items()
        ]
        rows = self._session.execute(
            select(JDBCConnections.dbalias, JDBCConnections.jdbc_url)
            .where(*conditions)
            .order_by(JDBCConnections.dbalias)
            .limit(limit)
        )
        found = list(rows.tuples())
        total = self._session.scalar(
            select(func.count()).select_from(JDBCConnections).where(*conditions)
        )
        return found, total or 0

    def save(self, connection: JDBCConnection) -> None:
        """Creates the connection, or replaces every column of the existing
        one with the same alias."""
        values = connection.model_dump()
        self._session.execute(
            mysql_insert(JDBCConnections)
            .values(values)
            .on_duplicate_key_update(
                {name: value for name, value in values.items() if name != "dbalias"}
            )
        )

    def delete(self, dbalias: str) -> bool:
        """Deletes the connection; False when there is none."""
        result = self._session.execute(
            delete(JDBCConnections).where(JDBCConnections.dbalias == dbalias)
        )
        return result.rowcount == 1
