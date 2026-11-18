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
"""JDBC connections: the drivers and the connection environments."""

from pydantic import BaseModel, ConfigDict
from sqlalchemy import func, insert, select, update
from sqlalchemy.orm import Session

from dbimport.config_db.config_schema import (
    JDBCConnectionsDrivers,
    JDBCConnectionsEnvironments,
)

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
