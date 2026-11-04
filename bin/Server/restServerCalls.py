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

import json
import logging
import os
import re
import subprocess
import sys
from urllib.parse import quote

import bcrypt
import sqlalchemy as sa
import sqlalchemy.orm.exc
from fastapi import HTTPException, status
from fastapi.responses import JSONResponse
from sqlalchemy import bindparam
from sqlalchemy.dialects.mysql import insert
from sqlalchemy.orm import aliased, scoped_session, sessionmaker
from sqlalchemy.sql import delete, func, update
from sqlalchemy.sql.operators import like_op

from common import constants as constant
from common.Exceptions import SQLerror, invalidConfiguration
from ConfigReader import configuration
from DBImportConfig import common_config, configSchema
from Server import dataModels


class dbCalls:
    def __init__(self):

        # 		logFormat = '%(asctime)s %(levelname)s - %(message)s'
        # 		logging.basicConfig(format=logFormat, level=logging.INFO)
        # 		logPropagate = True

        self.logger = "gunicorn.error"
        log = logging.getLogger(self.logger)
        log.debug("Executing Server.restServerCalls.__init__()")

        self.mysql_conn = None
        self.mysql_cursor = None
        self.debugLogLevel = False
        self.configDBSession = None

        if logging.root.level == 10:  # DEBUG
            self.debugLogLevel = True

        self.common_config = common_config.config()
        # self.common_config.disconnectConfigDatabase() # This is needed as we must connect with buffered=False and that is only available during reconnect
        # self.common_config.reconnectConfigDatabase(printReconnectMessage=True, buffered=False)

        self.logdir = configuration.get("Server", "logdir")
        # self.adminUser = self.common_config.getConfigValue("restserver_admin_user")
        # log.warning(self.adminUser)

        self.adminUser = self.getConfigValue("restserver_admin_user")

        self.contentMaxRows = 1000

        self.allConfigKeys = [
            "airflow_aws_instanceids",
            "airflow_aws_pool_to_instanceid",
            "airflow_create_pool_with_task",
            "airflow_dag_directory",
            "airflow_dag_file_group",
            "airflow_dag_file_permission",
            "airflow_dag_staging_directory",
            "airflow_dbimport_commandpath",
            "airflow_default_pool_size",
            "airflow_disable",
            "airflow_dummy_task_queue",
            "airflow_major_version",
            "airflow_sudo_user",
            "airflow_url",
            "atlas_discovery_interval",
            "cluster_name",
            "export_default_sessions",
            "export_max_sessions",
            "export_stage_disable",
            "export_staging_database",
            "export_start_disable",
            "hdfs_address",
            "hdfs_basedir",
            "hdfs_blocksize",
            "hive_acid_with_clusteredby",
            "hive_insert_only_tables",
            "hive_major_compact_after_merge",
            "hive_print_messages",
            "hive_remove_locks_by_force",
            "hive_validate_before_execution",
            "hive_validate_table",
            "impala_invalidate_metadata",
            "import_columnname_delete",
            "import_columnname_histtime",
            "import_columnname_import",
            "import_columnname_insert",
            "import_columnname_iud",
            "import_columnname_source",
            "import_columnname_update",
            "import_default_sessions",
            "import_history_database",
            "import_history_table",
            "import_max_sessions",
            "import_process_empty",
            "import_stage_disable",
            "import_staging_database",
            "import_staging_table",
            "import_start_disable",
            "import_work_database",
            "import_work_table",
            "kafka_brokers",
            "kafka_saslmechanism",
            "kafka_securityprotocol",
            "kafka_topic",
            "kafka_trustcafile",
            "post_airflow_dag_operations",
            "post_data_to_kafka",
            "post_data_to_kafka_extended",
            "post_data_to_rest",
            "post_data_to_rest_extended",
            "post_data_to_awssns",
            "post_data_to_awssns_extended",
            "post_data_to_awssns_topic",
            "restserver_admin_user",
            "restserver_authentication_method",
            "restserver_token_ttl",
            "rest_timeout",
            "rest_trustcafile",
            "rest_url",
            "rest_verifyssl",
            "spark_max_executors",
            "timezone",
            "timestamp_with_timezone",
        ]

        self.createDefaultAdminUser()

    def convertIntToBool(self, intValue):
        if intValue == 1:
            return True
        else:
            return False

    def disconnectDBImportDB(self):
        """Disconnects from the database and removes all sessions and engine"""
        log = logging.getLogger(self.logger)

        if self.configDB != None:
            log.info("Disconnecting from DBImport database")
            self.configDB.dispose()
            self.configDB = None

        self.configDBSession = None

    def getDBImportSession(self):
        """Connects to the configuration database with SQLAlchemy and return a session"""
        log = logging.getLogger(self.logger)

        if self.configDBSession != None:
            # If we already have a connection, we just return the session
            # return self.configDBSession()
            return scoped_session(self.configDBSession)

        mysqlCredentials = self.common_config.getMysqlCredentials()

        self.connectStr = "mysql+pymysql://%s:%s@%s:%s/%s" % (
            mysqlCredentials["mysql_username"],
            quote(mysqlCredentials["mysql_password"], safe=" +"),
            mysqlCredentials["mysql_hostname"],
            mysqlCredentials["mysql_port"],
            mysqlCredentials["mysql_database"],
        )

        try:
            self.configDB = sa.create_engine(
                self.connectStr, echo=self.debugLogLevel, pool_pre_ping=True
            )
            self.configDB.connect()
            self.configDBSession = sessionmaker(bind=self.configDB)

        except sa.exc.OperationalError as err:
            logging.error("%s" % err)
            self.configDBSession = None
            self.configDB = None
            raise SQLerror("Can't connect to DBImport database")

        except:
            print("Unexpected error: ")
            print(sys.exc_info())
            self.configDBSession = None
            self.configDB = None
            raise SQLerror("Can't connect to DBImport database")

        else:
            # return self.configDBSession()
            return scoped_session(self.configDBSession)

    def getConfigValue(self, key):
        """Returns a value from the configuration table based on the supplied key. Value returned can be Int, Str or DateTime"""
        log = logging.getLogger(self.logger)
        returnValue = None
        boolValue = False

        valueColumn, boolValue = self.common_config.getConfigValueColumn(key)

        try:
            session = self.getDBImportSession()
        except SQLerror:
            self.disconnectDBImportDB()
            return None

        configurationTable = aliased(configSchema.configuration)

        configuration = (
            session.query(
                configurationTable.configKey,
                configurationTable.valueInt,
                configurationTable.valueStr,
                configurationTable.valueDate,
            )
            .select_from(configurationTable)
            .filter(configurationTable.configKey == key)
            .one()
        )

        session.remove()
        returnValue = None

        try:
            if valueColumn == "valueInt":
                returnValue = int(configuration[1])

            if valueColumn == "valueStr":
                returnValue = configuration[2]
                if returnValue == None or returnValue.strip() == "":
                    log.error(
                        "Configuration Key '%s' must have a value in '%s'"
                        % (key, valueColumn)
                    )
                    returnValue = None

            if boolValue == True:
                if returnValue == 1:
                    returnValue = True
                elif returnValue == 0:
                    returnValue = False
                else:
                    log.error(
                        "Configuration Key '%s' can only have 0 or 1 in column '%s'"
                        % (key, valueColumn)
                    )
                    returnValue = None
        except TypeError:
            log.error(
                "Configuration key '%s' cant be found. Have you upgraded the database schema to the latest version?"
                % (key)
            )
            returnValue = None

        return returnValue

    def createDefaultAdminUser(self):
        """Creates the default admin user in the auth_users table if it doesnt exist"""
        log = logging.getLogger(self.logger)

        try:
            session = self.getDBImportSession()
        except SQLerror:
            self.disconnectDBImportDB()
            return None

        defaultAdminUser = self.getConfigValue("restserver_admin_user")
        userObject = self.getUser(defaultAdminUser)
        if userObject == None:
            # User does not exists, so lets create it
            log.info(
                "Default admin user does not exists in the 'auth_users' table. Creating it with password 'admin'"
            )

            password_encoded = "admin".encode("utf-8")
            salt = bcrypt.gensalt()
            hashed_password = bcrypt.hashpw(
                password=password_encoded, salt=salt
            ).decode("utf-8")

            query = sa.insert(configSchema.authUsers).values(
                username=defaultAdminUser, password=hashed_password
            )
            session.execute(query)
            session.commit()

        # session.close()
        session.remove()

    def createUser(self, user):
        """Creates a user in the auth_users table"""
        log = logging.getLogger(self.logger)

        try:
            session = self.getDBImportSession()
        except SQLerror:
            self.disconnectDBImportDB()
            return None

        log.info("User '%s' created" % (user.username))

        query = sa.insert(configSchema.authUsers).values(
            username=user.username,
            password=user.password,
            disabled=user.disabled,
            fullname=user.fullname,
            department=user.department,
            email=user.email,
        )
        session.execute(query)
        session.commit()

        # session.close()
        session.remove()

    def getUser(self, username):
        """Returns a dict with user information from the database"""
        log = logging.getLogger(self.logger)

        try:
            session = self.getDBImportSession()
        except SQLerror:
            self.disconnectDBImportDB()
            return None

        tableAuthUsers = aliased(configSchema.authUsers)
        authUser = (
            session.query(
                tableAuthUsers.username,
                tableAuthUsers.password,
                tableAuthUsers.disabled,
                tableAuthUsers.fullname,
                tableAuthUsers.department,
                tableAuthUsers.email,
            )
            .select_from(tableAuthUsers)
            .filter(tableAuthUsers.username == username)
            .first()
        )

        # session.close()
        session.remove()

        user = None

        if authUser != None:
            user = {}
            user["username"] = authUser[0]
            user["password"] = authUser[1]
            user["disabled"] = authUser[2]
            user["fullname"] = authUser[3]
            user["department"] = authUser[4]
            user["email"] = authUser[5]

        return user

    def updateUser(self, user, passwordChanged, currentUser):
        """Update a user object"""
        log = logging.getLogger(self.logger)

        try:
            session = self.getDBImportSession()
        except SQLerror:
            self.disconnectDBImportDB()
            return None

        if passwordChanged == False:
            log.info(
                "User '%s' updated details for user '%s'"
                % (currentUser, user["username"])
            )
        else:
            log.info(
                "User '%s' changed the password for user '%s'"
                % (currentUser, user["username"])
            )

        tableAuthUsers = aliased(configSchema.authUsers)
        session.execute(
            update(tableAuthUsers),
            [
                {
                    "username": user["username"],
                    "password": user["password"],
                    "disabled": user["disabled"],
                    "fullname": user["fullname"],
                    "department": user["department"],
                    "email": user["email"],
                }
            ],
        )
        session.commit()

        # session.close()
        session.remove()

    def deleteUser(self, username):
        """Delete a user"""
        log = logging.getLogger(self.logger)

        try:
            session = self.getDBImportSession()
        except SQLerror:
            self.disconnectDBImportDB()
            return None

        tableAuthUsers = aliased(configSchema.authUsers)

        (
            session.query(tableAuthUsers)
            .filter(tableAuthUsers.username == username)
            .delete()
        )
        session.commit()

        # session.close()
        session.remove()

    def generateJDBCconnectionString(self, connectionValues, currentUser):
        """Generates a JDBC connection string"""
        log = logging.getLogger(self.logger)

        databaseType = getattr(connectionValues, "databaseType")
        databaseVersion = getattr(connectionValues, "version")
        hostname = getattr(connectionValues, "hostname")
        port = getattr(connectionValues, "port")
        database = getattr(connectionValues, "database")
        jdbcConnectionString = ""

        # Validate to make sure we have a valid combination of databaseType and version
        correctDriverVersion = False
        for driver in self.getJDBCdrivers():
            if (
                databaseType == driver["databaseType"]
                and databaseVersion == driver["version"]
            ):
                correctDriverVersion = True

        if correctDriverVersion == False:
            result = "Not a valid combination of databaseType and version"
            returnCode = 404
            return (result, returnCode)

        # Generate the connection string for each combo of databaseType and databaseVersion
        if databaseType == "DB2 AS400" and databaseVersion == "default":
            if getattr(connectionValues, "port") == None:
                jdbcConnectionString = "jdbc:as400://%s/%s" % (hostname, database)
            else:
                jdbcConnectionString = "jdbc:as400://%s:%s/%s" % (
                    hostname,
                    port,
                    database,
                )

        if databaseType == "DB2 UDB" and databaseVersion == "default":
            if getattr(connectionValues, "port") == None:
                jdbcConnectionString = "jdbc:db2://%s/%s" % (hostname, database)
            else:
                jdbcConnectionString = "jdbc:db2://%s:%s/%s" % (
                    hostname,
                    port,
                    database,
                )

        if databaseType == "MySQL" and databaseVersion == "default":
            if getattr(connectionValues, "port") == None:
                jdbcConnectionString = "jdbc:mysql://%s/%s" % (hostname, database)
            else:
                jdbcConnectionString = "jdbc:mysql://%s:%s/%s" % (
                    hostname,
                    port,
                    database,
                )

        if databaseType == "Oracle" and databaseVersion == "default":
            if getattr(connectionValues, "port") == None:
                jdbcConnectionString = (
                    "jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=%s)))(CONNECT_DATA=(SERVICE_NAME=%s)))"
                    % (hostname, database)
                )
            else:
                jdbcConnectionString = (
                    "jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=%s)(PORT=%s)))(CONNECT_DATA=(SERVICE_NAME=%s)))"
                    % (hostname, port, database)
                )

        if databaseType == "PostgreSQL" and databaseVersion == "default":
            if getattr(connectionValues, "port") == None:
                jdbcConnectionString = "jdbc:postgresql://%s/%s" % (hostname, database)
            else:
                jdbcConnectionString = "jdbc:postgresql://%s:%s/%s" % (
                    hostname,
                    port,
                    database,
                )

        if databaseType == "Progress DB" and databaseVersion == "default":
            if getattr(connectionValues, "port") == None:
                jdbcConnectionString = "jdbc:datadirect:openedge://%s/%s" % (
                    hostname,
                    database,
                )
            else:
                jdbcConnectionString = "jdbc:datadirect:openedge://%s:%s/%s" % (
                    hostname,
                    port,
                    database,
                )

        if databaseType == "SQL Server" and databaseVersion == "default":
            if getattr(connectionValues, "port") == None:
                jdbcConnectionString = "jdbc:sqlserver://%s;database=%s" % (
                    hostname,
                    database,
                )
            else:
                jdbcConnectionString = "jdbc:sqlserver://%s:%s;database=%s" % (
                    hostname,
                    port,
                    database,
                )

        if databaseType == "SQL Server" and databaseVersion == "jTDS":
            if getattr(connectionValues, "port") == None:
                jdbcConnectionString = (
                    "jdbc:jtds:sqlserver://%s;useNTLMv2=true;databaseName=%s"
                    % (hostname, database)
                )
            else:
                jdbcConnectionString = (
                    "jdbc:jtds:sqlserver://%s:%s;useNTLMv2=true;databaseName=%s"
                    % (hostname, port, database)
                )

        if databaseType == "CacheDB" and databaseVersion == "default":
            if getattr(connectionValues, "port") == None:
                jdbcConnectionString = "jdbc:Cache://%s/%s" % (hostname, database)
            else:
                jdbcConnectionString = "jdbc:Cache://%s:%s/%s" % (
                    hostname,
                    port,
                    database,
                )

        if databaseType == "Snowflake" and databaseVersion == "default":
            if getattr(connectionValues, "port") == None:
                jdbcConnectionString = "jdbc:snowflake://%s/?db=%s" % (
                    hostname,
                    database,
                )
            else:
                jdbcConnectionString = "jdbc:snowflake://%s:%s/?db=%s" % (
                    hostname,
                    port,
                    database,
                )

        if databaseType == "Informix" and databaseVersion == "default":
            if getattr(connectionValues, "port") == None:
                jdbcConnectionString = "jdbc:informix-sqli://%s/%s" % (
                    hostname,
                    database,
                )
            else:
                jdbcConnectionString = "jdbc:informix-sqli://%s:%s/%s" % (
                    hostname,
                    port,
                    database,
                )

        if databaseType == "SQL Anywhere" and databaseVersion == "default":
            if getattr(connectionValues, "port") == None:
                jdbcConnectionString = "jdbc:sybase:Tds://%s/%s" % (hostname, database)
            else:
                jdbcConnectionString = "jdbc:sybase:Tds://%s:%s/%s" % (
                    hostname,
                    port,
                    database,
                )

        resultMsg = {"connectionString": jdbcConnectionString}

        returnCode = 200
        return (resultMsg, returnCode)

    def getJDBCdrivers(self):
        """Returns all JDBC Driver configuration"""
        log = logging.getLogger(self.logger)

        try:
            session = self.getDBImportSession()
        except SQLerror:
            self.disconnectDBImportDB()
            return None

        tableJDBCconnectionsDrivers = aliased(configSchema.jdbcConnectionsDrivers)
        jdbcConnectionsDrivers = (
            session.query(
                tableJDBCconnectionsDrivers.database_type,
                tableJDBCconnectionsDrivers.version,
                tableJDBCconnectionsDrivers.driver,
                tableJDBCconnectionsDrivers.classpath,
            )
            .select_from(tableJDBCconnectionsDrivers)
            .order_by(tableJDBCconnectionsDrivers.database_type)
            .all()
        )

        listOfJDBCdrivers = []
        for row in jdbcConnectionsDrivers:
            jdbcDriver = {}
            jdbcDriver["databaseType"] = row[0]
            jdbcDriver["version"] = row[1]
            jdbcDriver["driver"] = row[2]
            jdbcDriver["classpath"] = row[3]
            listOfJDBCdrivers.append(jdbcDriver)

        jsonResult = json.loads(json.dumps(listOfJDBCdrivers))

        # session.close()
        session.remove()

        return jsonResult

    def updateJDBCdriver(self, jdbcDriver, currentUser):
        """Returns all configuration items from the configuration table"""
        log = logging.getLogger(self.logger)

        try:
            session = self.getDBImportSession()
        except SQLerror:
            self.disconnectDBImportDB()
            return None

        log.info(
            "User '%s' updated JDBC driver configuration for '%s'"
            % (currentUser, getattr(jdbcDriver, "databaseType"))
        )

        log.debug("databaseType: %s" % getattr(jdbcDriver, "databaseType"))
        log.debug("version: %s" % getattr(jdbcDriver, "version"))
        log.debug("driver: %s" % getattr(jdbcDriver, "driver"))
        log.debug("classpath: %s" % getattr(jdbcDriver, "classpath"))

        tableJDBCconnectionsDrivers = aliased(configSchema.jdbcConnectionsDrivers)
        result = "ok"
        returnCode = 200

        try:
            session.execute(
                update(tableJDBCconnectionsDrivers),
                [
                    {
                        "database_type": getattr(jdbcDriver, "databaseType"),
                        "version": getattr(jdbcDriver, "version"),
                        "driver": getattr(jdbcDriver, "driver"),
                        "classpath": getattr(jdbcDriver, "classpath"),
                    }
                ],
            )
            session.commit()
        # except SQLAlchemyError.StaleDataError:
        except sqlalchemy.orm.exc.StaleDataError:
            result = (
                "JDBC Driver databaseType with version is not a supported combination"
            )
            returnCode = 400

        resultDict = {}
        resultDict["status"] = result
        # session.close()
        session.remove()

        jsonResult = json.loads(json.dumps(resultDict))
        # return (jsonResult, returnCode)
        return (result, returnCode)

    def getConfiguration(self, currentUser):
        """Returns all configuration items from the configuration table"""
        log = logging.getLogger(self.logger)

        try:
            session = self.getDBImportSession()
        except SQLerror:
            self.disconnectDBImportDB()
            return None

        configurationTable = aliased(configSchema.configuration)
        tableJDBCconnections = aliased(configSchema.jdbcConnections)

        configurations = (
            session.query(
                configurationTable.configKey,
                configurationTable.valueInt,
                configurationTable.valueStr,
                configurationTable.valueDate,
            )
            .select_from(configurationTable)
            .order_by(configurationTable.configKey)
            .all()
        )

        resultDict = {}
        for row in configurations:
            if row[0] in ["restserver_secret_key"]:
                # These should not be visable in the REST service. Skip them
                continue

            if row[0] == "restserver_admin_user":
                self.adminUser = row[2]

            valueColumn, boolValue = self.common_config.getConfigValueColumn(
                row[0], ignoreErrorOutput=True
            )
            if valueColumn == None:
                # If there is no handling of the value in self.common_config.getConfigValueColumn we will ignore it.
                # But even if it's handling in that configuration, it must also exists in the datamodel for it to be shown
                continue

            if row[0] in [
                "restserver_admin_user",
                "restserver_authentication_method",
                "restserver_token_ttl",
            ]:
                if currentUser != self.adminUser:
                    continue

            if valueColumn == "valueInt":
                resultDict[row[0]] = row[1]
            elif valueColumn == "valueStr":
                resultDict[row[0]] = row[2]
            elif valueColumn == "valueDate":
                resultDict[row[0]] = row[3]

        session.remove()

        jsonResult = json.loads(json.dumps(resultDict))
        return jsonResult

    def updateConfiguration(self, configuration, currentUser):
        """Update configuration items in the configuration table"""
        log = logging.getLogger(self.logger)

        try:
            session = self.getDBImportSession()
        except SQLerror:
            self.disconnectDBImportDB()
            return None

        configurationTable = aliased(configSchema.configuration)

        # First check to make sure we have valid options in the changed options
        try:
            for key in self.allConfigKeys:
                if type(getattr(configuration, key)) in (str, bool, int):
                    # At this point, only configuration items that exists in the class is iterrated
                    if type(getattr(configuration, key)) == str:
                        if (
                            getattr(configuration, key) is None
                            or getattr(configuration, key).strip() == ""
                        ):
                            raise HTTPException(
                                status_code=status.HTTP_400_BAD_REQUEST,
                                detail="'%s' requres a string value" % (key),
                            )

                    if key == "airflow_major_version" and getattr(
                        configuration, key
                    ) not in (1, 2):
                        raise HTTPException(
                            status_code=status.HTTP_400_BAD_REQUEST,
                            detail="only valid options for 'airflow_major_version' is '1' and '2'",
                        )

                    if key == "restserver_authentication_method" and getattr(
                        configuration, key
                    ) not in ("local", "pam"):
                        raise HTTPException(
                            status_code=status.HTTP_400_BAD_REQUEST,
                            detail="only valid options for 'restserver_authentication_method' is 'local' and 'pam'",
                        )

                    if key in [
                        "restserver_admin_user",
                        "restserver_authentication_method",
                        "restserver_token_ttl",
                    ]:
                        if currentUser != self.adminUser:
                            raise HTTPException(
                                status_code=status.HTTP_403_FORBIDDEN,
                                detail="You are not allowed to update '%s'" % (key),
                            )

        except AttributeError as e:
            print(e)
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="invalid configuration option",
            )

        for key in self.allConfigKeys:
            if type(getattr(configuration, key)) in (str, bool, int):
                # At this point, only configuration items that exists in the class is iterrated
                log.info(
                    "User '%s' updated global configuration. %s = %s"
                    % (currentUser, key, getattr(configuration, key))
                )

                valueColumn, boolValue = self.common_config.getConfigValueColumn(key)

                session.execute(
                    update(configurationTable),
                    [{"configKey": key, valueColumn: getattr(configuration, key)}],
                )
                session.commit()

        result = "ok"
        returnCode = 200
        resultDict = {}
        resultDict["status"] = "ok"
        # session.close()
        session.remove()

        jsonResult = json.loads(json.dumps(resultDict))
        # return jsonResult
        return (result, returnCode)

    def getJDBCserverType(self, name, connectionString):
        """Return the server type based on the connection string"""
        try:
            self.common_config.lookupConnectionAlias(
                name, decryptCredentials=False, jdbcURL=connectionString, quiet=True
            )
        except invalidConfiguration:
            pass

        serverType = None
        if self.common_config.jdbc_servertype == constant.MYSQL:
            serverType = "MySQL"
        if self.common_config.jdbc_servertype == constant.ORACLE:
            serverType = "Oracle"
        if self.common_config.jdbc_servertype == constant.MSSQL:
            serverType = "MSSQL Server"
        if self.common_config.jdbc_servertype == constant.POSTGRESQL:
            serverType = "PostgreSQL"
        if self.common_config.jdbc_servertype == constant.PROGRESS:
            serverType = "Progress"
        if self.common_config.jdbc_servertype == constant.DB2_UDB:
            serverType = "DB2 UDB"
        if self.common_config.jdbc_servertype == constant.DB2_AS400:
            serverType = "DB2 AS400"
        if self.common_config.jdbc_servertype == constant.MONGO:
            serverType = "MongoDB"
        if self.common_config.jdbc_servertype == constant.CACHEDB:
            serverType = "Cache"
        if self.common_config.jdbc_servertype == constant.SNOWFLAKE:
            serverType = "Snowflake"
        if self.common_config.jdbc_servertype == constant.AWS_S3:
            serverType = "AWS S3"
        if self.common_config.jdbc_servertype == constant.INFORMIX:
            serverType = "Informix"
        if self.common_config.jdbc_servertype == constant.SQLANYWHERE:
            serverType = "SQL Anywhere"

        return serverType

    def getAllConnections(self, listOnlyName):
        """Returns all Connections"""
        log = logging.getLogger(self.logger)

        try:
            session = self.getDBImportSession()
        except SQLerror:
            self.disconnectDBImportDB()
            return None

        tableJDBCconnections = aliased(configSchema.jdbcConnections)
        jdbcConnections = (
            session.query(tableJDBCconnections.dbalias, tableJDBCconnections.jdbc_url)
            .select_from(tableJDBCconnections)
            .order_by(tableJDBCconnections.dbalias)
            .limit(self.contentMaxRows)
        )

        listOfConnections = []
        count = 0
        for row in jdbcConnections:
            count = count + 1
            resultDict = {}
            resultDict["name"] = row[0]
            if listOnlyName == False:
                resultDict["connectionString"] = row[1]
                resultDict["serverType"] = self.getJDBCserverType(
                    name=row[0], connectionString=row[1]
                )

            listOfConnections.append(resultDict)

            if count == self.contentMaxRows:
                break

        jsonResult = json.loads(json.dumps(listOfConnections))
        session.remove()

        headers = {
            "content-rows": str(count),
            "content-max-returned-rows": str(self.contentMaxRows),
        }
        return JSONResponse(content=jsonResult, headers=headers, status_code=200)

    def searchConnections(self, searchValues, currentUser):
        """Search for import tables"""
        log = logging.getLogger(self.logger)

        try:
            session = self.getDBImportSession()
        except SQLerror:
            self.disconnectDBImportDB()
            return None

        jdbcConnections = aliased(configSchema.jdbcConnections)

        attributeToColumn = {}
        attributeToColumn["name"] = jdbcConnections.dbalias
        attributeToColumn["connectionString"] = jdbcConnections.jdbc_url
        attributeToColumn["timeWindowTimezone"] = jdbcConnections.timewindow_timezone
        attributeToColumn["operatorNotes"] = jdbcConnections.operator_notes
        attributeToColumn["contactInformation"] = jdbcConnections.contact_info
        attributeToColumn["description"] = jdbcConnections.description
        attributeToColumn["owner"] = jdbcConnections.owner

        # Return a list of connections
        jdbcConnectionsData = session.query(
            jdbcConnections.dbalias, jdbcConnections.jdbc_url
        ).select_from(jdbcConnections)

        jdbcConnectionsDataCount = session.query(func.count(1)).select_from(
            jdbcConnections
        )

        # Add the filters for the specific column that we search on
        for items in searchValues:
            if items[1] == None or items[1] == "" or items[1] == "*" or items[1] == "%":
                # If it's None, empty or search *, it means we dont need to search on that value. So lets skip it
                continue

            column = items[0]
            value = items[1]
            likeSearch = False

            if type(value) is str:
                if "%" in value or "*" in value:
                    value = value.replace("*", "%")
                    likeSearch = True

            log.debug("search filter: %s = %s" % (column, value))

            if likeSearch == True:
                jdbcConnectionsData = jdbcConnectionsData.filter(
                    like_op(attributeToColumn[column], value)
                )
                jdbcConnectionsDataCount = jdbcConnectionsDataCount.filter(
                    like_op(attributeToColumn[column], value)
                )
            else:
                jdbcConnectionsData = jdbcConnectionsData.filter(
                    attributeToColumn[column] == value
                )
                jdbcConnectionsDataCount = jdbcConnectionsDataCount.filter(
                    attributeToColumn[column] == value
                )

        # Fetch all rows matching the filter
        jdbcConnectionsData = jdbcConnectionsData.limit(self.contentMaxRows)
        jdbcConnectionsDataCount = jdbcConnectionsDataCount.one()

        # Loop through result and create the return dict
        count = 0
        listOfConnections = []
        for row in jdbcConnectionsData:
            count = count + 1
            resultDict = {}
            resultDict["name"] = row[0]
            resultDict["connectionString"] = row[1]
            resultDict["serverType"] = self.getJDBCserverType(
                name=row[0], connectionString=row[1]
            )

            listOfConnections.append(resultDict)

            if count == self.contentMaxRows:
                break

        jsonResult = json.loads(json.dumps(listOfConnections))
        # session.close()
        session.remove()

        headers = {
            "content-rows": str(count),
            "content-max-returned-rows": str(self.contentMaxRows),
            "content-total-rows": str(jdbcConnectionsDataCount[0]),
        }
        return JSONResponse(content=jsonResult, headers=headers, status_code=200)

    def updateConnection(self, connection, currentUser):
        """Update or create a Connections"""
        log = logging.getLogger(self.logger)

        try:
            session = self.getDBImportSession()
        except SQLerror:
            self.disconnectDBImportDB()
            return None

        tableJDBCconnections = aliased(configSchema.jdbcConnections)
        result = "ok"
        returnCode = 200

        log.debug(connection)
        log.info(
            "User '%s' updated/created connection '%s'"
            % (currentUser, getattr(connection, "name"))
        )

        if getattr(connection, "forceString") == None:
            setattr(connection, "forceString", 0)
        if getattr(connection, "createDatalakeImport") == None:
            setattr(connection, "createDatalakeImport", 1)
        if getattr(connection, "createForeignKey") == None:
            setattr(connection, "createForeignKey", 1)
        if getattr(connection, "atlasDiscovery") == None:
            setattr(connection, "atlasDiscovery", 1)

        try:
            query = insert(configSchema.jdbcConnections).values(
                dbalias=getattr(connection, "name"),
                jdbc_url=getattr(connection, "connectionString"),
                private_key_path=getattr(connection, "privateKeyPath"),
                public_key_path=getattr(connection, "publicKeyPath"),
                credentials=getattr(connection, "credentials"),
                datalake_source=getattr(connection, "source"),
                force_string=getattr(connection, "forceString"),
                max_import_sessions=getattr(connection, "maxSessions"),
                create_datalake_import=getattr(connection, "createDatalakeImport"),
                timewindow_start=getattr(connection, "timeWindowStart"),
                timewindow_stop=getattr(connection, "timeWindowStop"),
                timewindow_timezone=getattr(connection, "timeWindowTimezone"),
                operator_notes=getattr(connection, "operatorNotes"),
                contact_info=getattr(connection, "contactInformation"),
                description=getattr(connection, "description"),
                owner=getattr(connection, "owner"),
                environment=getattr(connection, "environment"),
                seed_file=getattr(connection, "seedFile"),
                create_foreign_keys=getattr(connection, "createForeignKey"),
                atlas_discovery=getattr(connection, "atlasDiscovery"),
                atlas_include_filter=getattr(connection, "atlasIncludeFilter"),
                atlas_exclude_filter=getattr(connection, "atlasExcludeFilter"),
                atlas_last_discovery=getattr(connection, "atlasLastDiscovery"),
            )

            query = query.on_duplicate_key_update(
                dbalias=getattr(connection, "name"),
                jdbc_url=getattr(connection, "connectionString"),
                private_key_path=getattr(connection, "privateKeyPath"),
                public_key_path=getattr(connection, "publicKeyPath"),
                credentials=getattr(connection, "credentials"),
                datalake_source=getattr(connection, "source"),
                force_string=getattr(connection, "forceString"),
                max_import_sessions=getattr(connection, "maxSessions"),
                create_datalake_import=getattr(connection, "createDatalakeImport"),
                timewindow_start=getattr(connection, "timeWindowStart"),
                timewindow_stop=getattr(connection, "timeWindowStop"),
                timewindow_timezone=getattr(connection, "timeWindowTimezone"),
                operator_notes=getattr(connection, "operatorNotes"),
                contact_info=getattr(connection, "contactInformation"),
                description=getattr(connection, "description"),
                owner=getattr(connection, "owner"),
                environment=getattr(connection, "environment"),
                seed_file=getattr(connection, "seedFile"),
                create_foreign_keys=getattr(connection, "createForeignKey"),
                atlas_discovery=getattr(connection, "atlasDiscovery"),
                atlas_include_filter=getattr(connection, "atlasIncludeFilter"),
                atlas_exclude_filter=getattr(connection, "atlasExcludeFilter"),
                atlas_last_discovery=getattr(connection, "atlasLastDiscovery"),
            )

            session.execute(query)
            session.commit()
        except SQLerror as err:
            log.error(str(err))
            log.error(column)  # noqa: F821  # TODO(#164): column is undefined here

            result = str(err)
            returnCode = 500

        # session.close()
        session.remove()
        return (result, returnCode)

    def getConnection(self, connection):
        """Returns all Connections"""
        log = logging.getLogger(self.logger)

        try:
            session = self.getDBImportSession()
        except SQLerror:
            self.disconnectDBImportDB()
            return None

        tableJDBCconnections = aliased(configSchema.jdbcConnections)
        row = (
            session.query(
                tableJDBCconnections.dbalias,
                tableJDBCconnections.jdbc_url,
                tableJDBCconnections.private_key_path,
                tableJDBCconnections.public_key_path,
                tableJDBCconnections.credentials,
                tableJDBCconnections.datalake_source,
                tableJDBCconnections.force_string,
                tableJDBCconnections.max_import_sessions,
                tableJDBCconnections.create_datalake_import,
                tableJDBCconnections.timewindow_start,
                tableJDBCconnections.timewindow_stop,
                tableJDBCconnections.timewindow_timezone,
                tableJDBCconnections.operator_notes,
                tableJDBCconnections.contact_info,
                tableJDBCconnections.description,
                tableJDBCconnections.owner,
                tableJDBCconnections.environment,
                tableJDBCconnections.seed_file,
                tableJDBCconnections.create_foreign_keys,
                tableJDBCconnections.atlas_discovery,
                tableJDBCconnections.atlas_include_filter,
                tableJDBCconnections.atlas_exclude_filter,
                tableJDBCconnections.atlas_last_discovery,
            )
            .select_from(tableJDBCconnections)
            .filter(tableJDBCconnections.dbalias == connection)
            .one_or_none()
        )

        if row == None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="connection does not exist",
            )

        resultDict = {}
        resultDict["name"] = row[0]
        resultDict["connectionString"] = row[1]
        resultDict["private_key_path"] = row[2]
        resultDict["public_key_path"] = row[3]
        resultDict["credentials"] = row[4]
        resultDict["source"] = row[5]
        resultDict["forceString"] = row[6]
        if (
            resultDict["forceString"] != 0
            and resultDict["forceString"] != 1
            and resultDict["forceString"] != -1
        ):
            resultDict["forceString"] = -1
        resultDict["maxSessions"] = row[7]
        resultDict["createDatalakeImport"] = row[8]
        try:
            resultDict["timeWindowStart"] = row[9].strftime("%H:%M:%S")
        except AttributeError:
            resultDict["timeWindowStart"] = None
        try:
            resultDict["timeWindowStop"] = row[10].strftime("%H:%M:%S")
        except AttributeError:
            resultDict["timeWindowStop"] = None
        resultDict["timeWindowTimezone"] = row[11]
        resultDict["operatorNotes"] = row[12]
        resultDict["contactInformation"] = row[13]
        resultDict["description"] = row[14]
        resultDict["owner"] = row[15]
        resultDict["environment"] = row[16]
        resultDict["seedFile"] = row[17]
        resultDict["createForeignKey"] = row[18]
        resultDict["atlasDiscovery"] = row[19]
        resultDict["atlasIncludeFilter"] = row[20]
        resultDict["atlasExcludeFilter"] = row[21]
        try:
            resultDict["atlasLastDiscovery"] = row[22].strftime("%Y-%m-%d %H:%M:%S")
        except AttributeError:
            resultDict["atlasLastDiscovery"] = None

        jsonResult = json.loads(json.dumps(resultDict))
        # session.close()
        session.remove()

        return jsonResult

    def deleteConnection(self, connection, currentUser):
        """Delete a connection"""
        log = logging.getLogger(self.logger)

        try:
            session = self.getDBImportSession()
        except SQLerror:
            self.disconnectDBImportDB()
            return None

        result = "ok"
        returnCode = 200

        log.info("User '%s' deleted connection '%s'" % (currentUser, connection))

        # Fetch the tableID as that is required to update/create data in import_columns
        row = (
            session.query(configSchema.jdbcConnections.dbalias)
            .select_from(configSchema.jdbcConnections)
            .filter(configSchema.jdbcConnections.dbalias == connection)
            .one_or_none()
        )
        session.commit()

        if row == None:
            result = "Connection does not exist"
            returnCode = 404
        else:
            (
                session.query(configSchema.jdbcConnections)
                .filter(configSchema.jdbcConnections.dbalias == connection)
                .delete()
            )
            session.commit()

        # session.close()
        session.remove()
        return (result, returnCode)

    def encryptedCredentials(self, credentials, currentUser):
        """Encrypt username and password with private key and store in connection"""
        log = logging.getLogger(self.logger)

        manageCmd = "%s/bin/manage" % (os.environ["DBIMPORT_HOME"])

        jsonInput = {}
        jsonInput["connection"] = getattr(credentials, "connection")
        jsonInput["username"] = getattr(credentials, "username")
        jsonInput["password"] = getattr(credentials, "password")

        processKilled = False
        manageSession = subprocess.Popen(
            [manageCmd, "--encryptCredentials", "--jsonInput", "--quiet"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        try:
            stdout, stderr = manageSession.communicate(
                json.dumps(jsonInput).encode(), timeout=10
            )
        except subprocess.TimeoutExpired:
            log.error("Killing 'manage --encryptCredentials' process")
            manageSession.kill()
            stdout, stderr = manageSession.communicate()
            processKilled = True

        if manageSession.returncode != 0 or processKilled == True:
            returnCode = 500
            resultMsg = {
                "result": re.sub("\n$", "", re.sub("^json: ", "", stdout.decode()))
            }
        else:
            returnCode = 200
            resultMsg = {"result": "Credentials updated successfully"}

        return (resultMsg, returnCode)

    def testConnection(self, connection, currentUser):
        """Make a test connection to a JDBC source and return result"""
        log = logging.getLogger(self.logger)

        manageCmd = "%s/bin/manage" % (os.environ["DBIMPORT_HOME"])

        result = subprocess.run(
            [manageCmd, "--testConnection", "-a", connection, "--quiet"],
            stdout=subprocess.PIPE,
            check=False,
        )
        if result.returncode != 0:
            returnCode = 500
        else:
            returnCode = 200

        resultMsg = {"result": re.sub("\n$", "", result.stdout.decode())}

        return (resultMsg, returnCode)

    def getAllImportDatabases(self):
        """Returns all databases that have imports configured in them"""
        log = logging.getLogger(self.logger)

        try:
            session = self.getDBImportSession()
        except SQLerror:
            self.disconnectDBImportDB()
            return None

        importTables = aliased(configSchema.importTables)

        importTableDBs = (
            session.query(
                importTables.hive_db,
                func.count(importTables.hive_db),
                func.max(importTables.last_update_from_source),
                func.sum(importTables.sqoop_last_size),
                func.sum(importTables.sqoop_last_rows),
            )
            .select_from(importTables)
            .group_by(importTables.hive_db)
            .limit(self.contentMaxRows)
        )

        listOfDBs = []
        count = 0
        for row in importTableDBs:
            count = count + 1
            resultDict = {}
            resultDict["name"] = row[0]
            resultDict["tables"] = row[1]
            if row[2] == None:
                resultDict["lastImport"] = ""
            else:
                resultDict["lastImport"] = row[2].strftime("%Y-%m-%d %H:%M:%S")

            if row[3] == None:
                resultDict["lastSize"] = 0
            else:
                resultDict["lastSize"] = int(row[3])

            if row[4] == None:
                resultDict["lastRows"] = 0
            else:
                resultDict["lastRows"] = int(row[4])
            listOfDBs.append(resultDict)

            if count == self.contentMaxRows:
                break

        jsonResult = json.loads(json.dumps(listOfDBs))
        session.remove()

        headers = {
            "content-rows": str(count),
            "content-max-returned-rows": str(self.contentMaxRows),
        }
        return JSONResponse(content=jsonResult, headers=headers, status_code=200)

    def discoverImportTables(self, data, currentUser):
        """Search for import tables"""
        log = logging.getLogger(self.logger)

        manageCmd = "%s/bin/manage" % (os.environ["DBIMPORT_HOME"])

        commandArg = []
        commandArg.append(manageCmd)
        commandArg.append("--addImportTable")
        commandArg.append("-a")
        commandArg.append(getattr(data, "connection"))
        commandArg.append("-h")
        commandArg.append(getattr(data, "database"))

        if (
            getattr(data, "schemaFilter") != None
            and getattr(data, "schemaFilter").strip() != ""
        ):
            commandArg.append("-S")
            commandArg.append(getattr(data, "schemaFilter"))

        if (
            getattr(data, "tableFilter") != None
            and getattr(data, "tableFilter").strip() != ""
        ):
            commandArg.append("-T")
            commandArg.append(getattr(data, "tableFilter"))

        if (
            getattr(data, "addSchemaToTable") != None
            and getattr(data, "addSchemaToTable") == True
        ):
            commandArg.append("--addSchemaToTable")

        if (
            getattr(data, "addCounterToTable") != None
            and getattr(data, "addCounterToTable") == True
        ):
            commandArg.append("--addCounterToTable")

            if getattr(data, "counterStart") != None:
                commandArg.append("--counterStart")
                commandArg.append(str(getattr(data, "counterStart")))

        if (
            getattr(data, "addCustomText") != None
            and getattr(data, "addCustomText").strip() != ""
        ):
            commandArg.append("--addCustomText")
            commandArg.append(getattr(data, "addCustomText"))

        commandArg.append("--quiet")
        commandArg.append("--jsonOutput")

        log.debug(commandArg)

        result = subprocess.run(commandArg, stdout=subprocess.PIPE, check=False)

        if result.returncode != 0:
            raise HTTPException(status_code=500, detail=result.stdout.decode())

        else:
            returnCode = 200
            if (
                result.stdout.decode().strip()
                == "There are no tables in the source database that we dont already have in DBImport"
            ):
                returnMsg = []
            else:
                returnMsg = json.loads(result.stdout.decode().replace("\n", ""))

        return (returnMsg, returnCode)

    def addDiscoveredImportTables(self, data, currentUser):
        """Add discovered import tables"""
        log = logging.getLogger(self.logger)

        createCounter = 0
        for record in data:
            createCounter = createCounter + 1
            database = getattr(record, "database")
            table = getattr(record, "table")

            createData = dataModels.importTableDetailsWrite(
                connection=getattr(record, "connection"),
                sourceSchema=getattr(record, "sourceSchema"),
                sourceTable=getattr(record, "sourceTable"),
                importPhaseType="full",
                etlPhaseType="truncate_insert",
                importTool="spark",
                etlEngine="spark",
            )

            self.updateImportTable(database, table, createData, currentUser)

        returnCode = 200
        resultMsg = {"result": "%s tables created" % (createCounter)}

        return (resultMsg, returnCode)

    def searchImportTables(self, searchValues, currentUser):
        """Search for import tables"""
        log = logging.getLogger(self.logger)

        try:
            session = self.getDBImportSession()
        except SQLerror:
            self.disconnectDBImportDB()
            return None

        importTables = aliased(configSchema.importTables)

        attributeToColumn = {}
        attributeToColumn["database"] = importTables.hive_db
        attributeToColumn["table"] = importTables.hive_table
        attributeToColumn["connection"] = importTables.dbalias
        attributeToColumn["sourceSchema"] = importTables.source_schema
        attributeToColumn["sourceTable"] = importTables.source_table
        attributeToColumn["importPhaseType"] = importTables.import_phase_type
        attributeToColumn["etlPhaseType"] = importTables.etl_phase_type
        attributeToColumn["importTool"] = importTables.import_tool
        attributeToColumn["etlEngine"] = importTables.etl_engine
        attributeToColumn["includeInAirflow"] = importTables.include_in_airflow
        attributeToColumn["operatorNotes"] = importTables.operator_notes

        # Return a list of Hive tables with details
        importTablesData = session.query(
            importTables.hive_db,
            importTables.hive_table,
            importTables.dbalias,
            importTables.source_schema,
            importTables.source_table,
            importTables.import_phase_type,
            importTables.etl_phase_type,
            importTables.import_tool,
            importTables.etl_engine,
            importTables.last_update_from_source,
            importTables.include_in_airflow,
            importTables.operator_notes,
        ).select_from(importTables)

        importTablesDataCount = session.query(func.count(1)).select_from(importTables)

        # Add the filters for the specific column that we search on
        for items in searchValues:
            if items[1] == None or items[1] == "" or items[1] == "*" or items[1] == "%":
                # If it's None, empty or search *, it means we dont need to search on that value. So lets skip it
                continue

            column = items[0]
            value = items[1]
            likeSearch = False

            if type(value) is str:
                if "%" in value or "*" in value:
                    value = value.replace("*", "%")
                    likeSearch = True

            log.debug("search filter: %s = %s" % (column, value))

            if likeSearch == True:
                importTablesData = importTablesData.filter(
                    like_op(attributeToColumn[column], value)
                )
                importTablesDataCount = importTablesDataCount.filter(
                    like_op(attributeToColumn[column], value)
                )
            else:
                importTablesData = importTablesData.filter(
                    attributeToColumn[column] == value
                )
                importTablesDataCount = importTablesDataCount.filter(
                    attributeToColumn[column] == value
                )

        # Fetch all rows matching the filter
        importTablesData = importTablesData.limit(self.contentMaxRows)
        importTablesDataCount = importTablesDataCount.one()

        # Loop through result and create the return dict
        count = 0
        listOfTables = []
        for row in importTablesData:
            count = count + 1
            resultDict = {}
            resultDict["database"] = row[0]
            resultDict["table"] = row[1]
            resultDict["connection"] = row[2]
            resultDict["sourceSchema"] = row[3]
            resultDict["sourceTable"] = row[4]
            resultDict["importPhaseType"] = row[5]
            resultDict["etlPhaseType"] = row[6]
            resultDict["importTool"] = row[7]
            resultDict["etlEngine"] = row[8]
            try:
                resultDict["lastUpdateFromSource"] = row[9].strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
            except AttributeError:
                resultDict["lastUpdateFromSource"] = None
            if row[10] == 1:
                resultDict["includeInAirflow"] = True
            else:
                resultDict["includeInAirflow"] = False
            resultDict["operatorNotes"] = row[11]

            listOfTables.append(resultDict)

            if count == self.contentMaxRows:
                break

        jsonResult = json.loads(json.dumps(listOfTables))
        # session.close()
        session.remove()

        headers = {
            "content-rows": str(count),
            "content-max-returned-rows": str(self.contentMaxRows),
            "content-total-rows": str(importTablesDataCount[0]),
        }
        return JSONResponse(content=jsonResult, headers=headers, status_code=200)

    def getImportTablesOnConnection(self, connection, currentUser):
        """Returns all import tables in a specific database"""
        log = logging.getLogger(self.logger)

        searchValues = dataModels.importTableSearch(
            database="*", table="*", connection=connection
        )

        return self.searchImportTables(searchValues, currentUser)

    def getImportTablesInDatabase(self, database, currentUser):
        """Returns all import tables in a specific database"""
        log = logging.getLogger(self.logger)

        searchValues = dataModels.importTableSearch(database=database, table="*")

        return self.searchImportTables(searchValues, currentUser)

    def getImportTableDetails(self, database, table, includeColumns):
        """Returns all import table details"""
        log = logging.getLogger(self.logger)

        try:
            session = self.getDBImportSession()
        except SQLerror:
            self.disconnectDBImportDB()
            return None

        importTables = aliased(configSchema.importTables)

        # Return a list of Hive tables with details
        row = (
            session.query(
                importTables.hive_db,
                importTables.hive_table,
                importTables.dbalias,
                importTables.source_schema,
                importTables.source_table,
                importTables.import_phase_type,
                importTables.etl_phase_type,
                importTables.import_tool,
                importTables.etl_engine,
                importTables.last_update_from_source,
                importTables.sqoop_sql_where_addition,
                importTables.nomerge_ingestion_sql_addition,
                importTables.include_in_airflow,
                importTables.airflow_priority,
                importTables.validate_import,
                importTables.validationMethod,
                importTables.validate_source,
                importTables.validate_diff_allowed,
                importTables.validationCustomQuerySourceSQL,
                importTables.validationCustomQueryHiveSQL,
                importTables.validationCustomQueryValidateImportTable,
                importTables.truncate_hive,
                importTables.mappers,
                importTables.soft_delete_during_merge,
                importTables.source_rowcount,
                importTables.source_rowcount_incr,
                importTables.hive_rowcount,
                importTables.validationCustomQuerySourceValue,
                importTables.validationCustomQueryHiveValue,
                importTables.incr_mode,
                importTables.incr_column,
                importTables.incr_validation_method,
                importTables.incr_minvalue,
                importTables.incr_maxvalue,
                importTables.incr_minvalue_pending,
                importTables.incr_maxvalue_pending,
                importTables.pk_column_override,
                importTables.pk_column_override_mergeonly,
                importTables.hive_merge_heap,
                importTables.hive_split_count,
                importTables.spark_executor_memory,
                importTables.spark_executors,
                importTables.split_by_column,
                importTables.sqoop_query,
                importTables.sqoop_options,
                importTables.sqoop_last_size,
                importTables.sqoop_last_rows,
                importTables.sqoop_last_mappers,
                importTables.sqoop_last_execution,
                importTables.sqoop_use_generated_sql,
                importTables.sqoop_allow_text_splitter,
                importTables.force_string,
                importTables.comment,
                importTables.generated_hive_column_definition,
                importTables.generated_sqoop_query,
                importTables.generated_sqoop_options,
                importTables.generated_pk_columns,
                importTables.generated_foreign_keys,
                importTables.datalake_source,
                importTables.operator_notes,
                importTables.copy_finished,
                importTables.copy_slave,
                importTables.create_foreign_keys,
                importTables.invalidate_impala,
                importTables.custom_max_query,
                importTables.mergeCompactionMethod,
                importTables.sourceTableType,
                importTables.import_database,
                importTables.import_table,
                importTables.history_database,
                importTables.history_table,
            )
            .select_from(importTables)
            .filter(
                (importTables.hive_db == database) & (importTables.hive_table == table)
            )
            .one_or_none()
        )

        if row == None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="import table does not exist",
            )

        resultDict = {}
        resultDict["database"] = row[0]
        resultDict["table"] = row[1]
        resultDict["connection"] = row[2]
        resultDict["sourceSchema"] = row[3]
        resultDict["sourceTable"] = row[4]
        resultDict["importPhaseType"] = row[5]
        resultDict["etlPhaseType"] = row[6]
        resultDict["importTool"] = row[7]
        resultDict["etlEngine"] = row[8]
        try:
            resultDict["lastUpdateFromSource"] = row[9].strftime("%Y-%m-%d %H:%M:%S")
        except AttributeError:
            resultDict["lastUpdateFromSource"] = None
        resultDict["sqlWhereAddition"] = row[10]
        resultDict["nomergeIngestionSqlAddition"] = row[11]
        if row[12] == 1:
            resultDict["includeInAirflow"] = True
        else:
            resultDict["includeInAirflow"] = False
        resultDict["airflowPriority"] = row[13]
        resultDict["validateImport"] = row[14]
        resultDict["validationMethod"] = row[15]
        resultDict["validateSource"] = row[16]
        resultDict["validateDiffAllowed"] = row[17]
        resultDict["validationCustomQuerySourceSQL"] = row[18]
        resultDict["validationCustomQueryHiveSQL"] = row[19]
        resultDict["validationCustomQueryValidateImportTable"] = row[20]
        # 		resultDict['truncateTable'] = row[21]
        resultDict["sqlSessions"] = row[22]
        resultDict["softDeleteDuringMerge"] = row[23]
        resultDict["sourceRowcount"] = row[24]
        resultDict["sourceRowcountIncr"] = row[25]
        resultDict["targetRowcount"] = row[26]
        resultDict["validationCustomQuerySourceValue"] = row[27]
        resultDict["validationCustomQueryHiveValue"] = row[28]
        if row[29] == None or row[29] == "":
            resultDict["incrMode"] = "lastmodified"
        else:
            resultDict["incrMode"] = row[29]
        resultDict["incrColumn"] = row[30]
        resultDict["incrValidationMethod"] = row[31]
        resultDict["incrMinvalue"] = row[32]
        resultDict["incrMaxvalue"] = row[33]
        resultDict["incrMinvaluePending"] = row[34]
        resultDict["incrMaxvaluePending"] = row[35]
        resultDict["pkColumnOverride"] = row[36]
        resultDict["pkColumnOverrideMergeonly"] = row[37]
        resultDict["hiveContainerSize"] = row[38]
        resultDict["splitCount"] = row[39]
        resultDict["sparkExecutorMemory"] = row[40]
        resultDict["sparkExecutors"] = row[41]
        resultDict["splitByColumn"] = row[42]
        resultDict["sqoopCustomQuery"] = row[43]
        resultDict["sqoopOptions"] = row[44]
        resultDict["lastSize"] = row[45]
        resultDict["lastRows"] = row[46]
        resultDict["lastSqlSessions"] = row[47]
        resultDict["lastExecution"] = row[48]
        if (
            row[49] == -1
        ):  # -1 is not allowed, but default in the database. Setting these to 1
            resultDict["sqoopUseGeneratedSql"] = 1
        else:
            resultDict["sqoopUseGeneratedSql"] = row[49]
        resultDict["allowTextSplitter"] = row[50]
        resultDict["forceString"] = row[51]
        if (
            resultDict["forceString"] != 0
            and resultDict["forceString"] != 1
            and resultDict["forceString"] != -1
        ):
            resultDict["forceString"] = -1
        resultDict["comment"] = row[52]
        resultDict["generatedHiveColumnDefinition"] = row[53]
        resultDict["generatedSqoopQuery"] = row[54]
        resultDict["generatedSqoopOptions"] = row[55]
        resultDict["generatedPkColumns"] = row[56]
        # 		resultDict['generatedForeignKeys'] = row[57]
        resultDict["datalakeSource"] = row[58]
        resultDict["operatorNotes"] = row[59]
        try:
            resultDict["copyFinished"] = row[60].strftime("%Y-%m-%d %H:%M:%S")
        except AttributeError:
            resultDict["copyFinished"] = None
        resultDict["copySlave"] = row[61]
        resultDict["createForeignKeys"] = row[62]
        resultDict["invalidateImpala"] = row[63]
        resultDict["customMaxQuery"] = row[64]
        resultDict["mergeCompactionMethod"] = row[65]
        resultDict["sourceTableType"] = row[66]
        resultDict["importDatabase"] = row[67]
        resultDict["importTable"] = row[68]
        resultDict["historyDatabase"] = row[69]
        resultDict["historyTable"] = row[70]

        if includeColumns == True:
            resultDict["columns"] = self.getImportTableColumns(database, table)

        jsonResult = json.loads(json.dumps(resultDict))
        # session.close()
        session.remove()

        return jsonResult

    def deleteImportTable(self, database, table, currentUser):
        """Update or create an import table"""
        log = logging.getLogger(self.logger)

        try:
            session = self.getDBImportSession()
        except SQLerror:
            self.disconnectDBImportDB()
            return None

        importColumns = aliased(configSchema.importColumns)
        importTables = aliased(configSchema.importTables)
        result = "ok"
        returnCode = 200

        log.info(
            "User '%s' deleted import table '%s.%s'" % (currentUser, database, table)
        )

        # Fetch the tableID as that is required to update/create data in import_columns
        row = (
            session.query(configSchema.importTables.table_id)
            .select_from(configSchema.importTables)
            .filter(
                (configSchema.importTables.hive_db == database)
                & (configSchema.importTables.hive_table == table)
            )
            .one_or_none()
        )
        session.commit()

        if row == None:
            result = "Table does not exist"
            returnCode = 404
        else:
            tableID = row[0]

            (
                session.query(configSchema.importTables)
                .filter(configSchema.importTables.table_id == tableID)
                .delete()
            )
            session.commit()

        # session.close()
        session.remove()
        return (result, returnCode)

    def bulkDeleteImportTable(self, bulkData, currentUser):
        """Bulk delete of import tables"""
        log = logging.getLogger(self.logger)

        if len(bulkData) == 0:
            resultMsg = {
                "result": "Body must contain a list of databas and table to delete"
            }
            returnCode = 422
            return (resultMsg, returnCode)

        try:
            session = self.getDBImportSession()
        except SQLerror:
            self.disconnectDBImportDB()
            return None

        deleteStatement = delete(configSchema.importTables)
        deleteStatement = deleteStatement.where(
            (configSchema.importTables.hive_db == bindparam("database"))
            & (configSchema.importTables.hive_table == bindparam("table"))
        )

        # Create a list of dicts that contain all the databas and table combinations that will be deleted
        tableList = []
        for tables in bulkData:
            tableList.append({"database": tables.database, "table": tables.table})

        session.connection().execute(deleteStatement, tableList)
        session.commit()
        session.remove()

        returnCode = 200
        resultMsg = {"result": "Ok"}

        return (resultMsg, returnCode)

    def bulkUpdateImportTable(self, bulkData, currentUser):
        """Bulk update of import tables"""
        log = logging.getLogger(self.logger)

        if len(getattr(bulkData, "importTables")) == 0:
            resultMsg = {
                "result": "importTables must contain a list of database/tables to update"
            }
            returnCode = 422
            return (resultMsg, returnCode)

        try:
            session = self.getDBImportSession()
        except SQLerror:
            self.disconnectDBImportDB()
            return None

        importTables = aliased(configSchema.importTables)
        updateStatement = update(importTables)
        updateStatement = updateStatement.where(
            (importTables.hive_db == bindparam("database"))
            & (importTables.hive_table == bindparam("table"))
        )

        # This will loop through only the fields specified in the Posted JSON. So if a field have got a default None value in dataModel validation,
        # that field wont be part of this loop
        for updateField in bulkData.__pydantic_fields_set__:
            if updateField == "connection":
                updateStatement = updateStatement.values(
                    dbalias=getattr(bulkData, "connection")
                )
            elif updateField == "importPhaseType":
                updateStatement = updateStatement.values(
                    import_phase_type=getattr(bulkData, "importPhaseType")
                )
            elif updateField == "etlPhaseType":
                updateStatement = updateStatement.values(
                    etl_phase_type=getattr(bulkData, "etlPhaseType")
                )
            elif updateField == "importTool":
                updateStatement = updateStatement.values(
                    import_tool=getattr(bulkData, "importTool")
                )
            elif updateField == "etlEngine":
                updateStatement = updateStatement.values(
                    etl_engine=getattr(bulkData, "etlEngine")
                )
            elif updateField == "includeInAirflow":
                updateStatement = updateStatement.values(
                    include_in_airflow=getattr(bulkData, "includeInAirflow")
                )
            elif updateField == "operatorNotes":
                updateStatement = updateStatement.values(
                    operator_notes=getattr(bulkData, "operatorNotes")
                )

        # Create a list of dicts that contain all the databas/table combinations that will be updated
        tableList = []
        for tables in getattr(bulkData, "importTables"):
            tableList.append({"database": tables.database, "table": tables.table})

        updateStatement = updateStatement.execution_options(synchronize_session=False)
        # 		print(updateStatement)

        session.connection().execute(updateStatement, tableList)
        session.commit()
        session.remove()

        returnCode = 200
        resultMsg = {"result": "Ok"}

        return (resultMsg, returnCode)

    def updateImportTable(self, database, table, data, currentUser):
        """Update or create an import table"""
        log = logging.getLogger(self.logger)

        try:
            session = self.getDBImportSession()
        except SQLerror:
            self.disconnectDBImportDB()
            return None

        importColumns = aliased(configSchema.importColumns)
        importTables = aliased(configSchema.importTables)
        result = "ok"
        returnCode = 200

        log.debug(data)
        # log.info("User '%s' updated/created import table '%s.%s'"%(currentUser, getattr(data, "database"), getattr(data, "table")))
        log.info(
            "User '%s' updated/created import table '%s.%s'"
            % (currentUser, database, table)
        )

        # Set default values
        if getattr(data, "includeInAirflow") == None:
            setattr(data, "includeInAirflow", 1)
        if getattr(data, "validateImport") == None:
            setattr(data, "validateImport", 1)
        if getattr(data, "validationMethod") == None:
            setattr(data, "validationMethod", "rowCount")
        if getattr(data, "validateSource") == None:
            setattr(data, "validateSource", "query")
        if getattr(data, "validateDiffAllowed") == None:
            setattr(data, "validateDiffAllowed", -1)
        if getattr(data, "validationCustomQueryValidateImportTable") == None:
            setattr(data, "validationCustomQueryValidateImportTable", 1)
        if getattr(data, "incrValidationMethod") == None:
            setattr(data, "incrValidationMethod", "full")
            # 		if getattr(data, "truncateTable") == None:								setattr(data, "truncateTable", 1)
        if getattr(data, "sqlSessions") == None:
            setattr(data, "sqlSessions", -1)
        if getattr(data, "softDeleteDuringMerge") == None:
            setattr(data, "softDeleteDuringMerge", 0)
        if getattr(data, "sqoopUseGeneratedSql") == None:
            setattr(data, "sqoopUseGeneratedSql", -1)
        if getattr(data, "allowTextSplitter") == None:
            setattr(data, "allowTextSplitter", 0)
        if getattr(data, "forceString") == None:
            setattr(data, "forceString", -1)
        if getattr(data, "invalidateImpala") == None:
            setattr(data, "invalidateImpala", -1)
        if getattr(data, "mergeCompactionMethod") == None:
            setattr(data, "mergeCompactionMethod", "default")
        if getattr(data, "createForeignKeys") == None:
            setattr(data, "createForeignKeys", -1)
        if getattr(data, "copySlave") == None:
            setattr(data, "copySlave", 0)

        try:
            # 				hive_db = getattr(data, "database"),
            # 				hive_table = getattr(data, "table"),
            query = insert(configSchema.importTables).values(
                hive_db=database,
                hive_table=table,
                dbalias=getattr(data, "connection"),
                source_schema=getattr(data, "sourceSchema"),
                source_table=getattr(data, "sourceTable"),
                import_phase_type=getattr(data, "importPhaseType"),
                etl_phase_type=getattr(data, "etlPhaseType"),
                import_tool=getattr(data, "importTool"),
                etl_engine=getattr(data, "etlEngine"),
                last_update_from_source=getattr(data, "lastUpdateFromSource"),
                sqoop_sql_where_addition=getattr(data, "sqlWhereAddition"),
                nomerge_ingestion_sql_addition=getattr(
                    data, "nomergeIngestionSqlAddition"
                ),
                include_in_airflow=getattr(data, "includeInAirflow"),
                airflow_priority=getattr(data, "airflowPriority"),
                validate_import=getattr(data, "validateImport"),
                validationMethod=getattr(data, "validationMethod"),
                validate_source=getattr(data, "validateSource"),
                validate_diff_allowed=getattr(data, "validateDiffAllowed"),
                validationCustomQuerySourceSQL=getattr(
                    data, "validationCustomQuerySourceSQL"
                ),
                validationCustomQueryHiveSQL=getattr(
                    data, "validationCustomQueryHiveSQL"
                ),
                validationCustomQueryValidateImportTable=getattr(
                    data, "validationCustomQueryValidateImportTable"
                ),
                # 				truncate_hive = getattr(data, "truncateTable"),
                mappers=getattr(data, "sqlSessions"),
                soft_delete_during_merge=getattr(data, "softDeleteDuringMerge"),
                incr_mode=getattr(data, "incrMode"),
                incr_column=getattr(data, "incrColumn"),
                incr_validation_method=getattr(data, "incrValidationMethod"),
                pk_column_override=getattr(data, "pkColumnOverride"),
                pk_column_override_mergeonly=getattr(data, "pkColumnOverrideMergeonly"),
                hive_merge_heap=getattr(data, "hiveContainerSize"),
                hive_split_count=getattr(data, "splitCount"),
                spark_executor_memory=getattr(data, "sparkExecutorMemory"),
                spark_executors=getattr(data, "sparkExecutors"),
                split_by_column=getattr(data, "splitByColumn"),
                sqoop_query=getattr(data, "sqoopCustomQuery"),
                sqoop_options=getattr(data, "sqoopOptions"),
                sqoop_use_generated_sql=getattr(data, "sqoopUseGeneratedSql"),
                sqoop_allow_text_splitter=getattr(data, "allowTextSplitter"),
                force_string=getattr(data, "forceString"),
                comment=getattr(data, "comment"),
                datalake_source=getattr(data, "datalakeSource"),
                operator_notes=getattr(data, "operatorNotes"),
                copy_slave=getattr(data, "copySlave"),
                create_foreign_keys=getattr(data, "createForeignKeys"),
                invalidate_impala=getattr(data, "invalidateImpala"),
                custom_max_query=getattr(data, "customMaxQuery"),
                mergeCompactionMethod=getattr(data, "mergeCompactionMethod"),
                sourceTableType=getattr(data, "sourceTableType"),
                import_database=getattr(data, "importDatabase"),
                import_table=getattr(data, "importTable"),
                history_database=getattr(data, "historyDatabase"),
                history_table=getattr(data, "historyTable"),
            )

            query = query.on_duplicate_key_update(
                dbalias=getattr(data, "connection"),
                source_schema=getattr(data, "sourceSchema"),
                source_table=getattr(data, "sourceTable"),
                import_phase_type=getattr(data, "importPhaseType"),
                etl_phase_type=getattr(data, "etlPhaseType"),
                import_tool=getattr(data, "importTool"),
                etl_engine=getattr(data, "etlEngine"),
                last_update_from_source=getattr(data, "lastUpdateFromSource"),
                sqoop_sql_where_addition=getattr(data, "sqlWhereAddition"),
                nomerge_ingestion_sql_addition=getattr(
                    data, "nomergeIngestionSqlAddition"
                ),
                include_in_airflow=getattr(data, "includeInAirflow"),
                airflow_priority=getattr(data, "airflowPriority"),
                validate_import=getattr(data, "validateImport"),
                validationMethod=getattr(data, "validationMethod"),
                validate_source=getattr(data, "validateSource"),
                validate_diff_allowed=getattr(data, "validateDiffAllowed"),
                validationCustomQuerySourceSQL=getattr(
                    data, "validationCustomQuerySourceSQL"
                ),
                validationCustomQueryHiveSQL=getattr(
                    data, "validationCustomQueryHiveSQL"
                ),
                validationCustomQueryValidateImportTable=getattr(
                    data, "validationCustomQueryValidateImportTable"
                ),
                # 				truncate_hive = getattr(data, "truncateTable"),
                mappers=getattr(data, "sqlSessions"),
                soft_delete_during_merge=getattr(data, "softDeleteDuringMerge"),
                incr_mode=getattr(data, "incrMode"),
                incr_column=getattr(data, "incrColumn"),
                incr_validation_method=getattr(data, "incrValidationMethod"),
                pk_column_override=getattr(data, "pkColumnOverride"),
                pk_column_override_mergeonly=getattr(data, "pkColumnOverrideMergeonly"),
                hive_merge_heap=getattr(data, "hiveContainerSize"),
                hive_split_count=getattr(data, "splitCount"),
                spark_executor_memory=getattr(data, "sparkExecutorMemory"),
                spark_executors=getattr(data, "sparkExecutors"),
                split_by_column=getattr(data, "splitByColumn"),
                sqoop_query=getattr(data, "sqoopCustomQuery"),
                sqoop_options=getattr(data, "sqoopOptions"),
                sqoop_use_generated_sql=getattr(data, "sqoopUseGeneratedSql"),
                sqoop_allow_text_splitter=getattr(data, "allowTextSplitter"),
                force_string=getattr(data, "forceString"),
                comment=getattr(data, "comment"),
                datalake_source=getattr(data, "datalakeSource"),
                operator_notes=getattr(data, "operatorNotes"),
                copy_slave=getattr(data, "copySlave"),
                create_foreign_keys=getattr(data, "createForeignKeys"),
                invalidate_impala=getattr(data, "invalidateImpala"),
                custom_max_query=getattr(data, "customMaxQuery"),
                mergeCompactionMethod=getattr(data, "mergeCompactionMethod"),
                sourceTableType=getattr(data, "sourceTableType"),
                import_database=getattr(data, "importDatabase"),
                import_table=getattr(data, "importTable"),
                history_database=getattr(data, "historyDatabase"),
                history_table=getattr(data, "historyTable"),
            )

            session.execute(query)
            session.commit()
        except SQLerror as err:
            log.error(str(err))
            log.error(column)  # noqa: F821  # TODO(#164): column is undefined here

            result = str(err)
            returnCode = 500

            # session.close()
            session.remove()
            return (result, returnCode)

        # Fetch the tableID as that is required to update/create data in import_columns
        row = (
            session.query(importTables.table_id)
            .select_from(importTables)
            .filter(
                (importTables.hive_db == database) & (importTables.hive_table == table)
            )
            .one()
        )
        # 				.filter((importTables.hive_db == getattr(table, "database")) & (importTables.hive_table == getattr(table, "table")))

        session.execute(query)
        tableID = row[0]

        columns = getattr(data, "columns")
        for column in columns:
            # PK in import_columns is not the most optimal. So we need to check first if it exists and then insert or update. Upsert is not available
            row = (
                session.query(importColumns.column_id)
                .select_from(importColumns)
                .filter(
                    (importColumns.table_id == tableID)
                    & (importColumns.column_name == getattr(column, "columnName"))
                )
                .one_or_none()
            )

            # Force set to Null if string is empty. This will otherwise cause problems during executions as it's not handled there
            if (
                getattr(column, "columnNameOverride") != None
                and getattr(column, "columnNameOverride").strip() == ""
            ):
                setattr(column, "columnNameOverride", None)
            if (
                getattr(column, "columnTypeOverride") != None
                and getattr(column, "columnTypeOverride").strip() == ""
            ):
                setattr(column, "columnTypeOverride", None)

            try:
                if row == None:
                    log.debug("Column does not exist")

                    # 						hive_db = getattr(data, "database"),
                    # 						hive_table = getattr(data, "table"),
                    query = sa.insert(configSchema.importColumns).values(
                        table_id=tableID,
                        hive_db=database,
                        hive_table=table,
                        column_name=getattr(column, "columnName"),
                        column_order=getattr(column, "columnOrder"),
                        source_column_name=getattr(column, "sourceColumnName"),
                        column_type=getattr(column, "columnType"),
                        source_column_type=getattr(column, "sourceColumnType"),
                        source_database_type=getattr(column, "sourceDatabaseType"),
                        column_name_override=getattr(column, "columnNameOverride"),
                        column_type_override=getattr(column, "columnTypeOverride"),
                        sqoop_column_type=getattr(column, "sqoopColumnType"),
                        sqoop_column_type_override=getattr(
                            column, "sqoopColumnTypeOverride"
                        ),
                        force_string=getattr(column, "forceString"),
                        include_in_import=getattr(column, "includeInImport"),
                        source_primary_key=getattr(column, "sourcePrimaryKey"),
                        last_update_from_source=getattr(column, "lastUpdateFromSource"),
                        comment=getattr(column, "comment"),
                        operator_notes=getattr(column, "operatorNotes"),
                        anonymization_function=getattr(column, "anonymizationFunction"),
                    )
                    session.execute(query)

                else:
                    columnID = row[0]
                    log.debug("Import column with id '%s' was updated" % (columnID))
                    # 							"hive_db": getattr(data, "database"),
                    # 							"hive_table": getattr(data, "table"),
                    session.execute(
                        update(importColumns),
                        [
                            {
                                "table_id": tableID,
                                "column_id": columnID,
                                "hive_db": database,
                                "hive_table": table,
                                "column_name": getattr(column, "columnName"),
                                "column_order": getattr(column, "columnOrder"),
                                "source_column_name": getattr(
                                    column, "sourceColumnName"
                                ),
                                "column_type": getattr(column, "columnType"),
                                "source_column_type": getattr(
                                    column, "sourceColumnType"
                                ),
                                "source_database_type": getattr(
                                    column, "sourceDatabaseType"
                                ),
                                "column_name_override": getattr(
                                    column, "columnNameOverride"
                                ),
                                "column_type_override": getattr(
                                    column, "columnTypeOverride"
                                ),
                                "sqoop_column_type": getattr(column, "sqoopColumnType"),
                                "sqoop_column_type_override": getattr(
                                    column, "sqoopColumnTypeOverride"
                                ),
                                "force_string": getattr(column, "forceString"),
                                "include_in_import": getattr(column, "includeInImport"),
                                "source_primary_key": getattr(
                                    column, "sourcePrimaryKey"
                                ),
                                "last_update_from_source": getattr(
                                    column, "lastUpdateFromSource"
                                ),
                                "comment": getattr(column, "comment"),
                                "operator_notes": getattr(column, "operatorNotes"),
                                "anonymization_function": getattr(
                                    column, "anonymizationFunction"
                                ),
                            }
                        ],
                    )

                session.commit()
            except SQLerror as err:
                log.error(str(err))
                log.error(column)

                result = str(err)
                returnCode = 500

                # session.close()
                session.remove()

                return (result, returnCode)

            log.debug(column)

        # session.close()
        session.remove()
        return (result, returnCode)

    def getImportTableColumns(self, database, table):
        """Returns all columns in an import table"""
        log = logging.getLogger(self.logger)

        try:
            session = self.getDBImportSession()
        except SQLerror:
            self.disconnectDBImportDB()
            return None

        importColumns = aliased(configSchema.importColumns)
        listOfColumns = []

        # Return a list of Hive tables with details
        importColumnsData = (
            session.query(
                importColumns.hive_db,
                importColumns.hive_table,
                importColumns.column_name,
                importColumns.column_order,
                importColumns.source_column_name,
                importColumns.column_type,
                importColumns.source_column_type,
                importColumns.source_database_type,
                importColumns.column_name_override,
                importColumns.column_type_override,
                importColumns.sqoop_column_type,
                importColumns.sqoop_column_type_override,
                importColumns.force_string,
                importColumns.include_in_import,
                importColumns.source_primary_key,
                importColumns.last_update_from_source,
                importColumns.comment,
                importColumns.operator_notes,
                importColumns.anonymization_function,
            )
            .select_from(importColumns)
            .filter(
                (importColumns.hive_db == database)
                & (importColumns.hive_table == table)
            )
            .order_by(importColumns.column_order)
            .all()
        )

        for row in importColumnsData:
            resultDict = {}
            # 			resultDict['database'] = row[0]
            # 			resultDict['table'] = row[1]
            resultDict["columnName"] = row[2]
            resultDict["columnOrder"] = row[3]
            resultDict["sourceColumnName"] = row[4]
            resultDict["columnType"] = row[5]
            resultDict["sourceColumnType"] = row[6]
            resultDict["sourceDatabaseType"] = row[7]
            resultDict["columnNameOverride"] = row[8]
            resultDict["columnTypeOverride"] = row[9]
            resultDict["sqoopColumnType"] = row[10]
            resultDict["sqoopColumnTypeOverride"] = row[11]
            resultDict["forceString"] = row[12]
            if (
                resultDict["forceString"] != 0
                and resultDict["forceString"] != 1
                and resultDict["forceString"] != -1
            ):
                resultDict["forceString"] = -1
            resultDict["includeInImport"] = row[13]
            resultDict["sourcePrimaryKey"] = row[14]
            try:
                resultDict["lastUpdateFromSource"] = row[15].strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
            except AttributeError:
                resultDict["lastUpdateFromSource"] = None
            resultDict["comment"] = row[16]
            resultDict["operatorNotes"] = row[17]
            resultDict["anonymizationFunction"] = row[18]

            listOfColumns.append(resultDict)

        # 		jsonResult = json.loads(json.dumps(listOfColumns))
        # session.close()
        session.remove()

        # 		return jsonResult
        return listOfColumns

    def repairImportTable(self, database, table, currentUser):
        """Repair Import table"""
        log = logging.getLogger(self.logger)

        manageCmd = "%s/bin/manage" % (os.environ["DBIMPORT_HOME"])

        commandArg = []
        commandArg.append(manageCmd)
        commandArg.append("--repairIncrementalImport")
        commandArg.append("--yes")
        commandArg.append("-h")
        commandArg.append(database)
        commandArg.append("-t")
        commandArg.append(table)
        commandArg.append("--quiet")

        processKilled = False
        manageSession = subprocess.Popen(
            commandArg,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        try:
            stdout, stderr = manageSession.communicate(timeout=30)
        except subprocess.TimeoutExpired:
            log.error("Killing 'manage --repairIncrementalImport' process")
            manageSession.kill()
            stdout, stderr = manageSession.communicate()
            processKilled = True

        if manageSession.returncode != 0 or processKilled == True:
            returnCode = 500
            resultMsg = {
                "result": re.sub("\n$", "", re.sub("^json: ", "", stdout.decode()))
            }
        else:
            returnCode = 200
            resultMsg = {"result": "Table repair was successfully"}

        return (resultMsg, returnCode)

    def resetImportTable(self, database, table, currentUser):
        """Reset Import table"""
        log = logging.getLogger(self.logger)

        manageCmd = "%s/bin/manage" % (os.environ["DBIMPORT_HOME"])

        commandArg = []
        commandArg.append(manageCmd)
        commandArg.append("--resetIncrementalImport")
        commandArg.append("--yes")
        commandArg.append("-h")
        commandArg.append(database)
        commandArg.append("-t")
        commandArg.append(table)
        commandArg.append("--quiet")

        processKilled = False
        manageSession = subprocess.Popen(
            commandArg,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        try:
            stdout, stderr = manageSession.communicate(timeout=30)
        except subprocess.TimeoutExpired:
            log.error("Killing 'manage --resetIncrementalImport' process")
            manageSession.kill()
            stdout, stderr = manageSession.communicate()
            processKilled = True

        if manageSession.returncode != 0 or processKilled == True:
            returnCode = 500
            resultMsg = {
                "result": re.sub("\n$", "", re.sub("^json: ", "", stdout.decode()))
            }
        else:
            returnCode = 200
            resultMsg = {"result": "Table reset was successfully"}

        return (resultMsg, returnCode)

    def repairExportTable(self, connection, schema, table, currentUser):
        """Repair Import table"""
        log = logging.getLogger(self.logger)

        manageCmd = "%s/bin/manage" % (os.environ["DBIMPORT_HOME"])

        commandArg = []
        commandArg.append(manageCmd)
        commandArg.append("--repairIncrementalExport")
        commandArg.append("--yes")
        commandArg.append("-a")
        commandArg.append(connection)
        commandArg.append("-S")
        commandArg.append(schema)
        commandArg.append("-T")
        commandArg.append(table)
        commandArg.append("--quiet")

        processKilled = False
        manageSession = subprocess.Popen(
            commandArg,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        try:
            stdout, stderr = manageSession.communicate(timeout=30)
        except subprocess.TimeoutExpired:
            log.error("Killing 'manage --repairIncrementalExport' process")
            manageSession.kill()
            stdout, stderr = manageSession.communicate()
            processKilled = True

        if manageSession.returncode != 0 or processKilled == True:
            returnCode = 500
            resultMsg = {
                "result": re.sub("\n$", "", re.sub("^json: ", "", stdout.decode()))
            }
        else:
            returnCode = 200
            resultMsg = {"result": "Table repair was successfully"}

        return (resultMsg, returnCode)

    def resetExportTable(self, connection, schema, table, currentUser):
        """Repair Import table"""
        log = logging.getLogger(self.logger)

        manageCmd = "%s/bin/manage" % (os.environ["DBIMPORT_HOME"])

        commandArg = []
        commandArg.append(manageCmd)
        commandArg.append("--resetIncrementalExport")
        commandArg.append("--yes")
        commandArg.append("-a")
        commandArg.append(connection)
        commandArg.append("-S")
        commandArg.append(schema)
        commandArg.append("-T")
        commandArg.append(table)
        commandArg.append("--quiet")

        processKilled = False
        manageSession = subprocess.Popen(
            commandArg,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        try:
            stdout, stderr = manageSession.communicate(timeout=30)
        except subprocess.TimeoutExpired:
            log.error("Killing 'manage --resetIncrementalExport' process")
            manageSession.kill()
            stdout, stderr = manageSession.communicate()
            processKilled = True

        if manageSession.returncode != 0 or processKilled == True:
            returnCode = 500
            resultMsg = {
                "result": re.sub("\n$", "", re.sub("^json: ", "", stdout.decode()))
            }
        else:
            returnCode = 200
            resultMsg = {"result": "Table reset was successfully"}

        return (resultMsg, returnCode)

    def getExportConnections(self):
        """Returns all connections that have exports configured in them"""
        log = logging.getLogger(self.logger)

        try:
            session = self.getDBImportSession()
        except SQLerror:
            self.disconnectDBImportDB()
            return None

        exportTables = aliased(configSchema.exportTables)

        exportTableDBs = (
            session.query(
                exportTables.dbalias,
                func.count(exportTables.dbalias),
                func.max(exportTables.last_update_from_hive),
                func.sum(exportTables.sqoop_last_rows),
            )
            .select_from(exportTables)
            .group_by(exportTables.dbalias)
            .limit(self.contentMaxRows)
        )

        listOfConnections = []
        count = 0
        for row in exportTableDBs:
            count = count + 1
            connection = {}
            connection["name"] = row[0]
            connection["tables"] = row[1]
            if row[2] == None:
                connection["laportImport"] = ""
            else:
                connection["lastExport"] = row[2].strftime("%Y-%m-%d %H:%M:%S")

            if row[3] == None:
                connection["lastRows"] = 0
            else:
                connection["lastRows"] = int(row[3])
            listOfConnections.append(connection)

            if count == self.contentMaxRows:
                break

        jsonResult = json.loads(json.dumps(listOfConnections))
        # session.close()
        session.remove()

        headers = {
            "content-rows": str(count),
            "content-max-returned-rows": str(self.contentMaxRows),
        }
        return JSONResponse(content=jsonResult, headers=headers, status_code=200)

    def discoverExportTables(self, data, currentUser):
        """Search for import tables"""
        log = logging.getLogger(self.logger)

        # Due to incomplete development environments right now, this function returns a hardcoded json as it's not possible to generate a correct one
        returnList = []
        returnDict1 = {}
        returnDict2 = {}
        returnDict3 = {}
        returnDict1["database"] = "hardcoded"
        returnDict1["table"] = "table1"
        returnDict1["connection"] = "dbimport"
        returnDict1["targetSchema"] = "dbo"
        returnDict1["targetTable"] = "table1"
        returnDict2["database"] = "hardcoded"
        returnDict2["table"] = "table2"
        returnDict2["connection"] = "dbimport"
        returnDict2["targetSchema"] = "dbo"
        returnDict2["targetTable"] = "table2"
        returnDict3["database"] = "hardcoded"
        returnDict3["table"] = "table3"
        returnDict3["connection"] = "dbimport"
        returnDict3["targetSchema"] = "dbo"
        returnDict3["targetTable"] = "table3"
        returnList.append(returnDict1)
        returnList.append(returnDict2)
        returnList.append(returnDict3)

        returnMsg = json.loads(json.dumps(returnList))
        returnCode = 200
        return (returnMsg, returnCode)

        manageCmd = "%s/bin/manage" % (os.environ["DBIMPORT_HOME"])

        commandArg = []
        commandArg.append(manageCmd)
        commandArg.append("--addExportTable")
        commandArg.append("-a")
        commandArg.append(getattr(data, "connection"))
        commandArg.append("-S")
        commandArg.append(getattr(data, "targetSchema"))

        if (
            getattr(data, "databaseFilter") != None
            and getattr(data, "databaseFilter").strip() != ""
        ):
            commandArg.append("-h")
            commandArg.append(getattr(data, "databaseFilter"))

        if (
            getattr(data, "tableFilter") != None
            and getattr(data, "tableFilter").strip() != ""
        ):
            commandArg.append("-t")
            commandArg.append(getattr(data, "tableFilter"))

        if (
            getattr(data, "addDBToTable") != None
            and getattr(data, "addDBToTable") == True
        ):
            commandArg.append("--addDBToTable")

        if (
            getattr(data, "addCounterToTable") != None
            and getattr(data, "addCounterToTable") == True
        ):
            commandArg.append("--addCounterToTable")

            if getattr(data, "counterStart") != None:
                commandArg.append("--counterStart")
                commandArg.append(str(getattr(data, "counterStart")))

        if (
            getattr(data, "addCustomText") != None
            and getattr(data, "addCustomText").strip() != ""
        ):
            commandArg.append("--addCustomText")
            commandArg.append(getattr(data, "addCustomText"))

        commandArg.append("--quiet")
        commandArg.append("--jsonOutput")

        log.debug(commandArg)

        result = subprocess.run(commandArg, stdout=subprocess.PIPE, check=False)

        if result.returncode != 0:
            raise HTTPException(status_code=500, detail=result.stdout.decode())

        else:
            returnCode = 200
            if (
                result.stdout.decode().strip()
                == "There are no tables in the source database that we dont already have in DBImport"
            ):
                returnMsg = []
            else:
                returnMsg = json.loads(result.stdout.decode().replace("\n", ""))

        return (returnMsg, returnCode)

    def addDiscoveredExportTables(self, data, currentUser):
        """Add discovered export tables"""
        log = logging.getLogger(self.logger)

        createCounter = 0
        for record in data:
            createCounter = createCounter + 1
            connection = getattr(record, "connection")
            targetSchema = getattr(record, "targetSchema")
            targetTable = getattr(record, "targetTable")

            createData = dataModels.exportTableDetailsWrite(
                database=getattr(record, "database"),
                table=getattr(record, "table"),
                exportType="full",
                exportTool="spark",
            )

            self.updateExportTable(
                connection, targetSchema, targetTable, createData, currentUser
            )

        returnCode = 200
        resultMsg = {"result": "%s tables created" % (createCounter)}

        return (resultMsg, returnCode)

    def searchExportTables(self, searchValues, currentUser):
        """Search for import tables"""
        log = logging.getLogger(self.logger)

        try:
            session = self.getDBImportSession()
        except SQLerror:
            self.disconnectDBImportDB()
            return None

        exportTables = aliased(configSchema.exportTables)

        attributeToColumn = {}
        attributeToColumn["connection"] = exportTables.dbalias
        attributeToColumn["targetSchema"] = exportTables.target_schema
        attributeToColumn["targetTable"] = exportTables.target_table
        attributeToColumn["database"] = exportTables.hive_db
        attributeToColumn["table"] = exportTables.hive_table
        attributeToColumn["exportType"] = exportTables.export_type
        attributeToColumn["exportTool"] = exportTables.export_tool
        attributeToColumn["includeInAirflow"] = exportTables.include_in_airflow
        attributeToColumn["operatorNotes"] = exportTables.operator_notes

        # Return a list of Hive tables with details
        exportTablesData = session.query(
            exportTables.dbalias,
            exportTables.target_schema,
            exportTables.target_table,
            exportTables.hive_db,
            exportTables.hive_table,
            exportTables.export_type,
            exportTables.export_tool,
            exportTables.last_update_from_hive,
            exportTables.include_in_airflow,
            exportTables.operator_notes,
        ).select_from(exportTables)

        exportTablesDataCount = session.query(func.count(1)).select_from(exportTables)

        # Add the filters for the specific column that we search on
        for items in searchValues:
            if items[1] == None or items[1] == "" or items[1] == "*" or items[1] == "%":
                # If it's None, empty or search *, it means we dont need to search on that value. So lets skip it
                continue

            column = items[0]
            value = items[1]
            likeSearch = False

            if type(value) is str:
                if "%" in value or "*" in value:
                    value = value.replace("*", "%")
                    likeSearch = True

            log.debug("search filter: %s = %s" % (column, value))

            if likeSearch == True:
                exportTablesData = exportTablesData.filter(
                    like_op(attributeToColumn[column], value)
                )
                exportTablesDataCount = exportTablesDataCount.filter(
                    like_op(attributeToColumn[column], value)
                )
            else:
                exportTablesData = exportTablesData.filter(
                    attributeToColumn[column] == value
                )
                exportTablesDataCount = exportTablesDataCount.filter(
                    attributeToColumn[column] == value
                )

        # Fetch all rows matching the filter
        exportTablesData = exportTablesData.limit(self.contentMaxRows)
        exportTablesDataCount = exportTablesDataCount.one()

        # Loop through result and create the return dict
        count = 0
        listOfTables = []
        for row in exportTablesData:
            count = count + 1
            resultDict = {}
            resultDict["connection"] = row[0]
            resultDict["targetSchema"] = row[1]
            resultDict["targetTable"] = row[2]
            resultDict["database"] = row[3]
            resultDict["table"] = row[4]
            resultDict["exportType"] = row[5]
            resultDict["exportTool"] = row[6]
            try:
                resultDict["lastUpdateFromHive"] = row[7].strftime("%Y-%m-%d %H:%M:%S")
            except AttributeError:
                resultDict["lastUpdateFromHive"] = None
            if row[8] == 1:
                resultDict["includeInAirflow"] = True
            else:
                resultDict["includeInAirflow"] = False
            resultDict["operatorNotes"] = row[9]

            listOfTables.append(resultDict)

            if count == self.contentMaxRows:
                break

        jsonResult = json.loads(json.dumps(listOfTables))
        session.remove()

        headers = {
            "content-rows": str(count),
            "content-max-returned-rows": str(self.contentMaxRows),
            "content-total-rows": str(exportTablesDataCount[0]),
        }
        return JSONResponse(content=jsonResult, headers=headers, status_code=200)

    def getExportTables(self, connection, schema, currentUser):
        """Returns all connections that have exports configured in them"""
        log = logging.getLogger(self.logger)

        if schema == None:
            schema = "*"
        searchValues = dataModels.exportTableSearch(
            connection=connection, targetSchema=schema, targetTable="*"
        )

        return self.searchExportTables(searchValues, currentUser)

    def getExportTableDetails(self, connection, schema, table, includeColumns):
        """Returns all connections that have exports configured in them"""
        log = logging.getLogger(self.logger)

        try:
            session = self.getDBImportSession()
        except SQLerror:
            self.disconnectDBImportDB()
            return None

        exportTables = aliased(configSchema.exportTables)

        row = (
            session.query(
                exportTables.dbalias,
                exportTables.target_schema,
                exportTables.target_table,
                exportTables.export_type,
                exportTables.export_tool,
                exportTables.hive_db,
                exportTables.hive_table,
                exportTables.last_update_from_hive,
                exportTables.sql_where_addition,
                exportTables.include_in_airflow,
                exportTables.airflow_priority,
                exportTables.forceCreateTempTable,
                exportTables.validate_export,
                exportTables.validationMethod,
                exportTables.validationCustomQueryHiveSQL,
                exportTables.validationCustomQueryTargetSQL,
                exportTables.uppercase_columns,
                exportTables.truncate_target,
                exportTables.mappers,
                exportTables.hive_rowcount,
                exportTables.target_rowcount,
                exportTables.validationCustomQueryHiveValue,
                exportTables.validationCustomQueryTargetValue,
                exportTables.incr_column,
                exportTables.incr_validation_method,
                exportTables.incr_minvalue,
                exportTables.incr_maxvalue,
                exportTables.incr_minvalue_pending,
                exportTables.incr_maxvalue_pending,
                exportTables.sqoop_options,
                exportTables.sqoop_last_size,
                exportTables.sqoop_last_rows,
                exportTables.sqoop_last_mappers,
                exportTables.sqoop_last_execution,
                exportTables.hive_javaheap,
                exportTables.create_target_table_sql,
                exportTables.operator_notes,
            )
            .select_from(exportTables)
            .filter(
                (exportTables.dbalias == connection)
                & (exportTables.target_schema == schema)
                & (exportTables.target_table == table)
            )
            .one_or_none()
        )

        if row == None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="export table does not exist",
            )

        resultDict = {}
        resultDict["connection"] = row[0]
        resultDict["targetSchema"] = row[1]
        resultDict["targetTable"] = row[2]
        resultDict["exportType"] = row[3]
        resultDict["exportTool"] = row[4]
        resultDict["database"] = row[5]
        resultDict["table"] = row[6]
        try:
            resultDict["lastUpdateFromHive"] = row[7].strftime("%Y-%m-%d %H:%M:%S")
        except AttributeError:
            resultDict["lastUpdateFromHive"] = None
        resultDict["sqlWhereAddition"] = row[8]
        if row[9] == 1:
            resultDict["includeInAirflow"] = True
        else:
            resultDict["includeInAirflow"] = False
        resultDict["airflowPriority"] = row[10]
        resultDict["forceCreateTempTable"] = row[11]
        resultDict["validateExport"] = row[12]
        resultDict["validationMethod"] = row[13]
        resultDict["validationCustomQueryHiveSQL"] = row[14]
        resultDict["validationCustomQueryTargetSQL"] = row[15]
        resultDict["uppercaseColumns"] = row[16]
        resultDict["truncateTarget"] = row[17]
        resultDict["sqlSessions"] = row[18]
        resultDict["tableRowcount"] = row[19]
        resultDict["targetRowcount"] = row[20]
        resultDict["validationCustomQueryHiveValue"] = row[21]
        resultDict["validationCustomQueryTargetValue"] = row[22]
        resultDict["incrColumn"] = row[23]
        resultDict["incrValidationMethod"] = row[24]
        resultDict["incrMinvalue"] = row[25]
        resultDict["incrMaxvalue"] = row[26]
        resultDict["incrMinvaluePending"] = row[27]
        resultDict["incrMaxvaluePending"] = row[28]
        resultDict["sqoopOptions"] = row[29]
        resultDict["lastSize"] = row[30]
        resultDict["lastRows"] = row[31]
        resultDict["lastSqlSessions"] = row[32]
        resultDict["lastExecution"] = row[33]
        resultDict["hiveContainerSize"] = row[34]
        resultDict["createTargetTableSql"] = row[35]
        resultDict["operatorNotes"] = row[36]

        if includeColumns == True:
            resultDict["columns"] = self.getExportTableColumns(
                connection, schema, table
            )

        jsonResult = json.loads(json.dumps(resultDict))
        # session.close()
        session.remove()

        return jsonResult

    def getExportTableColumns(self, connection, schema, table):
        """Returns all columns in an export table"""
        log = logging.getLogger(self.logger)

        try:
            session = self.getDBImportSession()
        except SQLerror:
            self.disconnectDBImportDB()
            return None

        exportColumns = aliased(configSchema.exportColumns)
        exportTables = aliased(configSchema.exportTables)
        listOfColumns = []

        # Return a list of Hive tables with details
        exportColumnsData = (
            session.query(
                exportTables.dbalias,
                exportTables.target_schema,
                exportTables.target_table,
                exportColumns.column_name,
                exportColumns.column_type,
                exportColumns.column_order,
                exportColumns.target_column_name,
                exportColumns.target_column_type,
                exportColumns.last_update_from_hive,
                exportColumns.include_in_export,
                exportColumns.comment,
                exportColumns.operator_notes,
            )
            .select_from(exportTables)
            .join(
                exportColumns,
                exportTables.table_id == exportColumns.table_id,
                isouter=False,
            )
            .filter(
                (exportTables.dbalias == connection)
                & (exportTables.target_schema == schema)
                & (exportTables.target_table == table)
            )
            .all()
        )

        for row in exportColumnsData:
            resultDict = {}

            # 			resultDict['connection'] = row[0]
            # 			resultDict['targetSchema'] = row[1]
            # 			resultDict['targetTable'] = row[2]
            resultDict["columnName"] = row[3]

            if row[4] == None:
                resultDict["columnType"] = "unknown"
            else:
                resultDict["columnType"] = row[4]

            # ColumnOrder should start with 1 in API, but database stores is as a start with 0.
            # For ImportColumns, it starts with 1. So we need to do a +1 here to have the same functionality as importColumns
            if row[5] != None:
                resultDict["columnOrder"] = int(row[5]) + 1

            resultDict["targetColumnName"] = row[6]
            resultDict["targetColumnType"] = row[7]

            try:
                resultDict["lastUpdateFromHive"] = row[8].strftime("%Y-%m-%d %H:%M:%S")
            except AttributeError:
                resultDict["lastUpdateFromHive"] = None

            if row[9] == 1:
                resultDict["includeInExport"] = True
            else:
                resultDict["includeInExport"] = False

            resultDict["comment"] = row[10]
            resultDict["operatorNotes"] = row[11]

            listOfColumns.append(resultDict)

        # session.close()
        session.remove()

        return listOfColumns

    def deleteExportTable(self, connection, schema, table, currentUser):
        """Update or create an export table"""
        log = logging.getLogger(self.logger)

        try:
            session = self.getDBImportSession()
        except SQLerror:
            self.disconnectDBImportDB()
            return None

        exportColumns = aliased(configSchema.exportColumns)
        exportTables = aliased(configSchema.exportTables)
        result = "ok"
        returnCode = 200

        log.info(
            "User '%s' deleted export table '%s.%s' on connection '%s'"
            % (currentUser, schema, table, connection)
        )

        # Fetch the tableID as that is required to update/create data in import_columns
        row = (
            session.query(configSchema.exportTables.table_id)
            .select_from(configSchema.exportTables)
            .filter(
                (configSchema.exportTables.dbalias == connection)
                & (configSchema.exportTables.target_schema == schema)
                & (configSchema.exportTables.target_table == table)
            )
            .one_or_none()
        )
        session.commit()

        if row == None:
            result = "Table does not exist"
            returnCode = 404
        else:
            tableID = row[0]

            (
                session.query(configSchema.exportTables)
                .filter(configSchema.exportTables.table_id == tableID)
                .delete()
            )
            session.commit()

        # session.close()
        session.remove()
        return (result, returnCode)

    def bulkDeleteExportTable(self, bulkData, currentUser):
        """Bulk delete of export tables"""
        log = logging.getLogger(self.logger)

        if len(bulkData) == 0:
            resultMsg = {
                "result": "Body must contain a list of connection, database and table to delete"
            }
            returnCode = 422
            return (resultMsg, returnCode)

        try:
            session = self.getDBImportSession()
        except SQLerror:
            self.disconnectDBImportDB()
            return None

        deleteStatement = delete(configSchema.exportTables)
        deleteStatement = deleteStatement.where(
            (configSchema.exportTables.dbalias == bindparam("connection"))
            & (configSchema.exportTables.target_schema == bindparam("targetSchema"))
            & (configSchema.exportTables.target_table == bindparam("targetTable"))
        )

        # Create a list of dicts that contain all the connection, schema and table combinations that will be deleted
        tableList = []
        for tables in bulkData:
            tableList.append(
                {
                    "connection": tables.connection,
                    "targetSchema": tables.targetSchema,
                    "targetTable": tables.targetTable,
                }
            )

        session.connection().execute(deleteStatement, tableList)
        session.commit()
        session.remove()

        returnCode = 200
        resultMsg = {"result": "Ok"}

        return (resultMsg, returnCode)

    def bulkUpdateExportTable(self, bulkData, currentUser):
        """Bulk update of export tables"""
        log = logging.getLogger(self.logger)

        if len(getattr(bulkData, "exportTables")) == 0:
            resultMsg = {
                "result": "exportTables must contain a list of connections/targetSchema/targetDatabase to update"
            }
            returnCode = 422
            return (resultMsg, returnCode)

        try:
            session = self.getDBImportSession()
        except SQLerror:
            self.disconnectDBImportDB()
            return None

        exportTables = aliased(configSchema.exportTables)
        updateStatement = update(exportTables)
        updateStatement = updateStatement.where(
            (exportTables.dbalias == bindparam("connection"))
            & (exportTables.target_schema == bindparam("targetSchema"))
            & (exportTables.target_table == bindparam("targetTable"))
        )

        # This will loop through only the fields specified in the Posted JSON. So if a field have got a default None value in dataModel validation,
        # that field wont be part of this loop
        for updateField in bulkData.__pydantic_fields_set__:
            if updateField == "exportType":
                updateStatement = updateStatement.values(
                    export_type=getattr(bulkData, "exportType")
                )
            elif updateField == "exportTool":
                updateStatement = updateStatement.values(
                    export_tool=getattr(bulkData, "exportTool")
                )
            elif updateField == "includeInAirflow":
                updateStatement = updateStatement.values(
                    include_in_airflow=getattr(bulkData, "includeInAirflow")
                )
            elif updateField == "operatorNotes":
                updateStatement = updateStatement.values(
                    operator_notes=getattr(bulkData, "operatorNotes")
                )

        # Create a list of dicts that contain all the databas/table combinations that will be updated
        tableList = []
        for tables in getattr(bulkData, "exportTables"):
            tableList.append(
                {
                    "connection": tables.connection,
                    "targetSchema": tables.targetSchema,
                    "targetTable": tables.targetTable,
                }
            )

        updateStatement = updateStatement.execution_options(synchronize_session=False)
        # 		print(updateStatement)

        session.connection().execute(updateStatement, tableList)
        session.commit()
        session.remove()

        returnCode = 200
        resultMsg = {"result": "Ok"}

        return (resultMsg, returnCode)

    def updateExportTable(self, connection, schema, table, data, currentUser):
        """Update or create an export table"""
        log = logging.getLogger(self.logger)

        try:
            session = self.getDBImportSession()
        except SQLerror:
            self.disconnectDBImportDB()
            return None

        exportColumns = aliased(configSchema.exportColumns)
        exportTables = aliased(configSchema.exportTables)
        result = "ok"
        returnCode = 200

        log.debug(table)
        log.info(
            "User '%s' updated/created export table '%s.%s' on connection '%s'"
            % (currentUser, schema, table, connection)
        )

        # Set default values
        if getattr(data, "includeInAirflow") == None:
            setattr(data, "includeInAirflow", 1)
        if getattr(data, "forceCreateTempTable") == None:
            setattr(data, "forceCreateTempTable", 0)
        if getattr(data, "validateExport") == None:
            setattr(data, "validateExport", 1)
        if getattr(data, "validationMethod") == None:
            setattr(data, "validationMethod", "rowCount")
        if getattr(data, "uppercaseColumns") == None:
            setattr(data, "uppercaseColumns", -1)
        if getattr(data, "truncateTarget") == None:
            setattr(data, "truncateTarget", 1)
        if getattr(data, "sqlSessions") == None:
            setattr(data, "sqlSessions", -1)
        if getattr(data, "incrValidationMethod") == None:
            setattr(data, "incrValidationMethod", "full")

        try:
            query = insert(configSchema.exportTables).values(
                dbalias=connection,
                target_schema=schema,
                target_table=table,
                export_type=getattr(data, "exportType"),
                export_tool=getattr(data, "exportTool"),
                hive_db=getattr(data, "database"),
                hive_table=getattr(data, "table"),
                last_update_from_hive=getattr(data, "lastUpdateFromHive"),
                sql_where_addition=getattr(data, "sqlWhereAddition"),
                include_in_airflow=getattr(data, "includeInAirflow"),
                airflow_priority=getattr(data, "airflowPriority"),
                forceCreateTempTable=getattr(data, "forceCreateTempTable"),
                validate_export=getattr(data, "validateExport"),
                validationMethod=getattr(data, "validationMethod"),
                validationCustomQueryHiveSQL=getattr(
                    data, "validationCustomQueryHiveSQL"
                ),
                validationCustomQueryTargetSQL=getattr(
                    data, "validationCustomQueryTargetSQL"
                ),
                uppercase_columns=getattr(data, "uppercaseColumns"),
                truncate_target=getattr(data, "truncateTarget"),
                mappers=getattr(data, "sqlSessions"),
                incr_column=getattr(data, "incrColumn"),
                incr_validation_method=getattr(data, "incrValidationMethod"),
                sqoop_options=getattr(data, "sqoopOptions"),
                hive_javaheap=getattr(data, "hiveContainerSize"),
                create_target_table_sql=getattr(data, "createTargetTableSql"),
                operator_notes=getattr(data, "operatorNotes"),
            )

            query = query.on_duplicate_key_update(
                dbalias=connection,
                target_schema=schema,
                target_table=table,
                export_type=getattr(data, "exportType"),
                export_tool=getattr(data, "exportTool"),
                hive_db=getattr(data, "database"),
                hive_table=getattr(data, "table"),
                last_update_from_hive=getattr(data, "lastUpdateFromHive"),
                sql_where_addition=getattr(data, "sqlWhereAddition"),
                include_in_airflow=getattr(data, "includeInAirflow"),
                airflow_priority=getattr(data, "airflowPriority"),
                forceCreateTempTable=getattr(data, "forceCreateTempTable"),
                validate_export=getattr(data, "validateExport"),
                validationMethod=getattr(data, "validationMethod"),
                validationCustomQueryHiveSQL=getattr(
                    data, "validationCustomQueryHiveSQL"
                ),
                validationCustomQueryTargetSQL=getattr(
                    data, "validationCustomQueryTargetSQL"
                ),
                uppercase_columns=getattr(data, "uppercaseColumns"),
                truncate_target=getattr(data, "truncateTarget"),
                mappers=getattr(data, "sqlSessions"),
                incr_column=getattr(data, "incrColumn"),
                incr_validation_method=getattr(data, "incrValidationMethod"),
                sqoop_options=getattr(data, "sqoopOptions"),
                hive_javaheap=getattr(data, "hiveContainerSize"),
                create_target_table_sql=getattr(data, "createTargetTableSql"),
                operator_notes=getattr(data, "operatorNotes"),
            )

            session.execute(query)
            session.commit()
        except SQLerror as err:
            log.error(str(err))
            log.error(column)  # noqa: F821  # TODO(#164): column is undefined here

            result = str(err)
            returnCode = 500

            # session.close()
            session.remove()
            return (result, returnCode)

        # Fetch the tableID as that is required to update/create data in export_columns
        row = (
            session.query(exportTables.table_id)
            .select_from(exportTables)
            .filter(
                (exportTables.dbalias == connection)
                & (exportTables.target_schema == schema)
                & (exportTables.target_table == table)
            )
            .one()
        )

        session.execute(query)
        tableID = row[0]

        columns = getattr(data, "columns")
        for column in columns:
            # PK in export_columns is not the most optimal. So we need to check first if it exists and then insert or update.
            # Upsert is not available
            row = (
                session.query(exportColumns.column_id)
                .select_from(exportColumns)
                .filter(
                    (exportColumns.table_id == tableID)
                    & (exportColumns.column_name == getattr(column, "columnName"))
                )
                .one_or_none()
            )

            try:
                columnOrder = None
                if getattr(column, "columnOrder") != None:
                    columnOrder = int(getattr(column, "columnOrder")) - 1

                # Force set to Null if string is empty. This will otherwise cause problems during executions as it's not handled there
                if (
                    getattr(column, "targetColumnName") != None
                    and getattr(column, "targetColumnName").strip() == ""
                ):
                    setattr(column, "targetColumnName", None)
                if (
                    getattr(column, "targetColumnType") != None
                    and getattr(column, "targetColumnType").strip() == ""
                ):
                    setattr(column, "targetColumnType", None)

                if row == None:
                    log.debug("Column does not exist")

                    query = sa.insert(configSchema.exportColumns).values(
                        table_id=tableID,
                        column_name=getattr(column, "columnName"),
                        column_type=getattr(column, "columnType"),
                        column_order=columnOrder,
                        hive_db=getattr(data, "database"),
                        hive_table=getattr(data, "table"),
                        target_column_name=getattr(column, "targetColumnName"),
                        target_column_type=getattr(column, "targetColumnType"),
                        last_update_from_hive=getattr(column, "lastUpdateFromHive"),
                        include_in_export=getattr(column, "includeInExport"),
                        comment=getattr(column, "comment"),
                        operator_notes=getattr(column, "operatorNotes"),
                    )

                    session.execute(query)

                else:
                    columnID = row[0]
                    log.debug("Export column with id '%s' was updated" % (columnID))

                    session.execute(
                        update(exportColumns),
                        [
                            {
                                "table_id": tableID,
                                "column_id": columnID,
                                "column_name": getattr(column, "columnName"),
                                "column_type": getattr(column, "columnType"),
                                "column_order": columnOrder,
                                "hive_db": getattr(data, "database"),
                                "hive_table": getattr(data, "table"),
                                "target_column_name": getattr(
                                    column, "targetColumnName"
                                ),
                                "target_column_type": getattr(
                                    column, "targetColumnType"
                                ),
                                "last_update_from_hive": getattr(
                                    column, "lastUpdateFromHive"
                                ),
                                "include_in_export": getattr(column, "includeInExport"),
                                "comment": getattr(column, "comment"),
                                "operator_notes": getattr(column, "operatorNotes"),
                            }
                        ],
                    )

                session.commit()
            except SQLerror as err:
                log.error(str(err))
                log.error(column)

                result = str(err)
                returnCode = 500

                # session.close()
                session.remove()
                return (result, returnCode)

            log.debug(column)

        # session.close()
        session.remove()
        return (result, returnCode)

    def getAllAirflowDags(self):
        """Returns all Airflow DAGs"""
        log = logging.getLogger(self.logger)

        try:
            session = self.getDBImportSession()
        except SQLerror:
            self.disconnectDBImportDB()
            return None

        airflowCustomDags = aliased(configSchema.airflowCustomDags)
        airflowExportDags = aliased(configSchema.airflowExportDags)
        airflowImportDags = aliased(configSchema.airflowImportDags)
        listOfDAGs = []
        processedDAGs = []

        airflowCustomDagsQuery = session.query(
            airflowCustomDags.dag_name,
            sa.sql.expression.literal_column("'custom'").label("dag_type"),
            airflowCustomDags.schedule_interval,
            airflowCustomDags.auto_regenerate_dag,
            airflowCustomDags.operator_notes,
            airflowCustomDags.application_notes,
        ).select_from(airflowCustomDags)

        airflowImportDagsQuery = session.query(
            airflowImportDags.dag_name,
            sa.sql.expression.literal_column("'import'").label("dag_type"),
            airflowImportDags.schedule_interval,
            airflowImportDags.auto_regenerate_dag,
            airflowImportDags.operator_notes,
            airflowImportDags.application_notes,
        ).select_from(airflowImportDags)

        airflowExportDagsQuery = session.query(
            airflowExportDags.dag_name,
            sa.sql.expression.literal_column("'export'").label("dag_type"),
            airflowExportDags.schedule_interval,
            airflowExportDags.auto_regenerate_dag,
            airflowExportDags.operator_notes,
            airflowExportDags.application_notes,
        ).select_from(airflowExportDags)

        airflowDagsData = airflowCustomDagsQuery.union(
            airflowImportDagsQuery, airflowExportDagsQuery
        ).limit(self.contentMaxRows)

        count = 0
        for row in airflowDagsData:
            count = count + 1
            resultDict = {}

            # Just to make sure that two DAGs dont exists with the same name in different tables
            if row[0] in processedDAGs:
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail="Airflow DAG '%s' exists in more than one DAG table"
                    % (row[0]),
                )
            else:
                processedDAGs.append(row[0])

            resultDict["name"] = row[0]
            resultDict["type"] = row[1]
            resultDict["scheduleInterval"] = row[2]
            resultDict["autoRegenerateDag"] = self.convertIntToBool(row[3])
            # 			resultDict['operatorNotes'] = row[4]
            # 			resultDict['applicationNotes'] = row[5]
            listOfDAGs.append(resultDict)

            if count == self.contentMaxRows:
                break

        jsonResult = json.loads(json.dumps(listOfDAGs))
        # session.close()
        session.remove()

        headers = {
            "content-rows": str(count),
            "content-max-returned-rows": str(self.contentMaxRows),
        }
        return JSONResponse(content=jsonResult, headers=headers, status_code=200)

    def getAirflowImportDags(self):
        """Returns all Airflow Import DAGs"""
        log = logging.getLogger(self.logger)

        try:
            session = self.getDBImportSession()
        except SQLerror:
            self.disconnectDBImportDB()
            return None

        airflowURL = self.getConfigValue("airflow_url")

        airflowImportDags = aliased(configSchema.airflowImportDags)
        listOfDAGs = []

        airflowDagsData = (
            session.query(
                airflowImportDags.dag_name,
                airflowImportDags.schedule_interval,
                airflowImportDags.auto_regenerate_dag,
                airflowImportDags.filter_hive,
                airflowImportDags.operator_notes,
                airflowImportDags.sudo_user,
                airflowImportDags.timezone,
                airflowImportDags.email,
                airflowImportDags.email_on_failure,
                airflowImportDags.email_on_retries,
                airflowImportDags.sla_warning_time,
                airflowImportDags.concurrency,
            )
            .select_from(airflowImportDags)
            .limit(self.contentMaxRows)
        )

        count = 0
        for row in airflowDagsData:
            count = count + 1
            resultDict = {}

            resultDict["name"] = row[0]
            resultDict["scheduleInterval"] = row[1]
            resultDict["autoRegenerateDag"] = self.convertIntToBool(row[2])
            resultDict["filterTable"] = row[3]
            resultDict["airflowLink"] = "%s/dags/%s" % (airflowURL, row[0])
            resultDict["operatorNotes"] = row[4]
            resultDict["sudoUser"] = row[5]
            resultDict["timezone"] = row[6]
            resultDict["email"] = row[7]
            resultDict["emailOnFailure"] = self.convertIntToBool(row[8])
            resultDict["emailOnRetries"] = self.convertIntToBool(row[9])
            try:
                resultDict["slaWarningTime"] = row[10].strftime("%H:%M:%S")
            except AttributeError:
                resultDict["slaWarningTime"] = None
            resultDict["concurrency"] = row[11]
            listOfDAGs.append(resultDict)

            if count == self.contentMaxRows:
                break

        jsonResult = json.loads(json.dumps(listOfDAGs))
        # session.close()
        session.remove()

        headers = {
            "content-rows": str(count),
            "content-max-returned-rows": str(self.contentMaxRows),
        }
        return JSONResponse(content=jsonResult, headers=headers, status_code=200)

    def getAirflowExportDags(self):
        """Returns all Airflow Export DAGs"""
        log = logging.getLogger(self.logger)

        try:
            session = self.getDBImportSession()
        except SQLerror:
            self.disconnectDBImportDB()
            return None

        airflowURL = self.getConfigValue("airflow_url")

        airflowExportDags = aliased(configSchema.airflowExportDags)
        listOfDAGs = []

        airflowDagsData = (
            session.query(
                airflowExportDags.dag_name,
                airflowExportDags.schedule_interval,
                airflowExportDags.auto_regenerate_dag,
                airflowExportDags.filter_dbalias,
                airflowExportDags.filter_target_schema,
                airflowExportDags.filter_target_table,
                airflowExportDags.operator_notes,
                airflowExportDags.sudo_user,
                airflowExportDags.timezone,
                airflowExportDags.email,
                airflowExportDags.email_on_failure,
                airflowExportDags.email_on_retries,
                airflowExportDags.sla_warning_time,
                airflowExportDags.concurrency,
            )
            .select_from(airflowExportDags)
            .limit(self.contentMaxRows)
        )

        count = 0
        for row in airflowDagsData:
            count = count + 1
            resultDict = {}

            resultDict["name"] = row[0]
            resultDict["scheduleInterval"] = row[1]
            resultDict["autoRegenerateDag"] = self.convertIntToBool(row[2])
            resultDict["filterConnection"] = row[3]
            resultDict["filterTargetSchema"] = row[4]
            resultDict["filterTargetTable"] = row[5]
            resultDict["airflowLink"] = "%s/dags/%s" % (airflowURL, row[0])
            resultDict["operatorNotes"] = row[6]
            resultDict["sudoUser"] = row[7]
            resultDict["timezone"] = row[8]
            resultDict["email"] = row[9]
            resultDict["emailOnFailure"] = self.convertIntToBool(row[10])
            resultDict["emailOnRetries"] = self.convertIntToBool(row[11])
            try:
                resultDict["slaWarningTime"] = row[12].strftime("%H:%M:%S")
            except AttributeError:
                resultDict["slaWarningTime"] = None
            resultDict["concurrency"] = row[13]

            listOfDAGs.append(resultDict)

        jsonResult = json.loads(json.dumps(listOfDAGs))
        # session.close()
        session.remove()

        headers = {
            "content-rows": str(count),
            "content-max-returned-rows": str(self.contentMaxRows),
        }
        return JSONResponse(content=jsonResult, headers=headers, status_code=200)

    def getAirflowCustomDags(self):
        """Returns all Airflow Custom DAGs"""
        log = logging.getLogger(self.logger)

        try:
            session = self.getDBImportSession()
        except SQLerror:
            self.disconnectDBImportDB()
            return None

        airflowURL = self.getConfigValue("airflow_url")

        airflowCustomDags = aliased(configSchema.airflowCustomDags)
        listOfDAGs = []

        airflowDagsData = (
            session.query(
                airflowCustomDags.dag_name,
                airflowCustomDags.schedule_interval,
                airflowCustomDags.auto_regenerate_dag,
                airflowCustomDags.operator_notes,
                airflowCustomDags.sudo_user,
                airflowCustomDags.timezone,
                airflowCustomDags.email,
                airflowCustomDags.email_on_failure,
                airflowCustomDags.email_on_retries,
                airflowCustomDags.sla_warning_time,
                airflowCustomDags.concurrency,
            )
            .select_from(airflowCustomDags)
            .limit(self.contentMaxRows)
        )

        count = 0
        for row in airflowDagsData:
            count = count + 1
            resultDict = {}

            resultDict["name"] = row[0]
            resultDict["scheduleInterval"] = row[1]
            resultDict["autoRegenerateDag"] = self.convertIntToBool(row[2])
            resultDict["airflowLink"] = "%s/dags/%s" % (airflowURL, row[0])
            resultDict["operatorNotes"] = row[3]
            resultDict["sudoUser"] = row[4]
            resultDict["timezone"] = row[5]
            resultDict["email"] = row[6]
            resultDict["emailOnFailure"] = self.convertIntToBool(row[7])
            resultDict["emailOnRetries"] = self.convertIntToBool(row[8])
            try:
                resultDict["slaWarningTime"] = row[9].strftime("%H:%M:%S")
            except AttributeError:
                resultDict["slaWarningTime"] = None
            resultDict["concurrency"] = row[10]
            listOfDAGs.append(resultDict)

            if count == self.contentMaxRows:
                break

        jsonResult = json.loads(json.dumps(listOfDAGs))
        # session.close()
        session.remove()

        headers = {
            "content-rows": str(count),
            "content-max-returned-rows": str(self.contentMaxRows),
        }
        return JSONResponse(content=jsonResult, headers=headers, status_code=200)

    def getAirflowTasks(self, dagname):
        """Returns a list with all airflow dags associated with a specified dag"""
        log = logging.getLogger(self.logger)

        try:
            session = self.getDBImportSession()
        except SQLerror:
            self.disconnectDBImportDB()
            return None

        airflowTasks = aliased(configSchema.airflowTasks)
        listOfTasks = []

        # Return a list of Hive tables with details
        airflowTasksData = (
            session.query(
                airflowTasks.task_name,
                airflowTasks.task_type,
                airflowTasks.placement,
                airflowTasks.jdbc_dbalias,
                airflowTasks.airflow_pool,
                airflowTasks.airflow_priority,
                airflowTasks.include_in_airflow,
                airflowTasks.task_dependency_downstream,
                airflowTasks.task_dependency_upstream,
                airflowTasks.task_config,
                airflowTasks.sensor_poke_interval,
                airflowTasks.sensor_timeout_minutes,
                airflowTasks.sensor_connection,
                airflowTasks.sensor_soft_fail,
                airflowTasks.sudo_user,
            )
            .select_from(airflowTasks)
            .filter(airflowTasks.dag_name == dagname)
            .all()
        )

        for row in airflowTasksData:
            resultDict = {}
            resultDict["name"] = row[0]
            resultDict["type"] = row[1]
            resultDict["placement"] = row[2]
            resultDict["connection"] = row[3]
            resultDict["airflowPool"] = row[4]
            resultDict["airflowPriority"] = row[5]
            if row[6] == 1:
                resultDict["includeInAirflow"] = True
            else:
                resultDict["includeInAirflow"] = False
            resultDict["taskDependencyDownstream"] = row[7]
            resultDict["taskDependencyUpstream"] = row[8]
            resultDict["taskConfig"] = row[9]
            resultDict["sensorPokeInterval"] = row[10]
            resultDict["sensorTimeoutMinutes"] = row[11]
            resultDict["sensorConnection"] = row[12]
            resultDict["sensorSoftFail"] = row[13]
            resultDict["sudoUser"] = row[14]
            listOfTasks.append(resultDict)

        session.remove()
        return listOfTasks

    def getAirflowCustomDag(self, dagname):
        """Returns an Airflow Custom DAG"""
        log = logging.getLogger(self.logger)

        try:
            session = self.getDBImportSession()
        except SQLerror:
            self.disconnectDBImportDB()
            return None

        airflowURL = self.getConfigValue("airflow_url")

        airflowCustomDags = aliased(configSchema.airflowCustomDags)

        row = (
            session.query(
                airflowCustomDags.dag_name,
                airflowCustomDags.schedule_interval,
                airflowCustomDags.retries,
                airflowCustomDags.operator_notes,
                airflowCustomDags.application_notes,
                airflowCustomDags.airflow_notes,
                airflowCustomDags.auto_regenerate_dag,
                airflowCustomDags.sudo_user,
                airflowCustomDags.timezone,
                airflowCustomDags.email,
                airflowCustomDags.email_on_failure,
                airflowCustomDags.email_on_retries,
                airflowCustomDags.tags,
                airflowCustomDags.sla_warning_time,
                airflowCustomDags.retry_exponential_backoff,
                airflowCustomDags.concurrency,
            )
            .select_from(airflowCustomDags)
            .filter(airflowCustomDags.dag_name == dagname)
            .one_or_none()
        )

        if row == None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Airflow DAG does not exist",
            )

        resultDict = {}
        resultDict["name"] = row[0]
        resultDict["scheduleInterval"] = row[1]
        resultDict["retries"] = row[2]
        if resultDict["retries"] == None or resultDict["retries"] == "":
            resultDict["retries"] = 0
        resultDict["operatorNotes"] = row[3]
        resultDict["applicationNotes"] = row[4]
        resultDict["airflowNotes"] = row[5]
        resultDict["autoRegenerateDag"] = self.convertIntToBool(row[6])
        resultDict["sudoUser"] = row[7]
        resultDict["timezone"] = row[8]
        resultDict["email"] = row[9]
        resultDict["emailOnFailure"] = self.convertIntToBool(row[10])
        resultDict["emailOnRetries"] = self.convertIntToBool(row[11])
        resultDict["tags"] = row[12]
        try:
            resultDict["slaWarningTime"] = row[13].strftime("%H:%M:%S")
        except AttributeError:
            resultDict["slaWarningTime"] = None
        resultDict["retryExponentialBackoff"] = row[14]
        resultDict["concurrency"] = row[15]
        resultDict["airflowLink"] = "%s/dags/%s" % (airflowURL, row[0])

        resultDict["tasks"] = self.getAirflowTasks(dagname)

        jsonResult = json.loads(json.dumps(resultDict))
        # session.close()
        session.remove()

        return jsonResult

    def checkDagNameAvailability(
        self, name, excludeImport=False, excludeExport=False, excludeCustom=False
    ):
        """Checks if the DAG name is available. Returns True or False"""
        log = logging.getLogger(self.logger)

        try:
            session = self.getDBImportSession()
        except SQLerror:
            self.disconnectDBImportDB()
            return None

        airflowCustomDags = aliased(configSchema.airflowCustomDags)
        airflowExportDags = aliased(configSchema.airflowExportDags)
        airflowImportDags = aliased(configSchema.airflowImportDags)
        listOfDAGs = []
        processedDAGs = []
        dagNameIsFree = True

        if excludeCustom == False:
            row = (
                session.query(airflowCustomDags.dag_name)
                .select_from(airflowCustomDags)
                .filter(airflowCustomDags.dag_name == name)
                .one_or_none()
            )

            if row != None:
                dagNameIsFree = False

        if excludeImport == False:
            row = (
                session.query(airflowImportDags.dag_name)
                .select_from(airflowImportDags)
                .filter(airflowImportDags.dag_name == name)
                .one_or_none()
            )

            if row != None:
                dagNameIsFree = False

        if excludeExport == False:
            row = (
                session.query(airflowExportDags.dag_name)
                .select_from(airflowExportDags)
                .filter(airflowExportDags.dag_name == name)
                .one_or_none()
            )

            if row != None:
                dagNameIsFree = False

        session.remove()
        return dagNameIsFree

    def updateAirflowTasks(self, dagname, task, currentUser):
        """Create or update Airflow tasks"""
        log = logging.getLogger(self.logger)

        try:
            session = self.getDBImportSession()
        except SQLerror:
            self.disconnectDBImportDB()
            return None

        airflowTasks = aliased(configSchema.airflowTasks)

        try:
            query = insert(configSchema.airflowTasks).values(
                dag_name=dagname,
                task_name=getattr(task, "name"),
                task_type=getattr(task, "type"),
                placement=getattr(task, "placement"),
                jdbc_dbalias=getattr(task, "connection"),
                airflow_pool=getattr(task, "airflowPool"),
                airflow_priority=getattr(task, "airflowPriority"),
                include_in_airflow=getattr(task, "includeInAirflow"),
                task_dependency_downstream=getattr(task, "taskDependencyDownstream"),
                task_dependency_upstream=getattr(task, "taskDependencyUpstream"),
                task_config=getattr(task, "taskConfig"),
                sensor_poke_interval=getattr(task, "sensorPokeInterval"),
                sensor_timeout_minutes=getattr(task, "sensorTimeoutMinutes"),
                sensor_connection=getattr(task, "sensorConnection"),
                sensor_soft_fail=getattr(task, "sensorSoftFail"),
                sudo_user=getattr(task, "sudoUser"),
            )

            query = query.on_duplicate_key_update(
                dag_name=dagname,
                task_name=getattr(task, "name"),
                task_type=getattr(task, "type"),
                placement=getattr(task, "placement"),
                jdbc_dbalias=getattr(task, "connection"),
                airflow_pool=getattr(task, "airflowPool"),
                airflow_priority=getattr(task, "airflowPriority"),
                include_in_airflow=getattr(task, "includeInAirflow"),
                task_dependency_downstream=getattr(task, "taskDependencyDownstream"),
                task_dependency_upstream=getattr(task, "taskDependencyUpstream"),
                task_config=getattr(task, "taskConfig"),
                sensor_poke_interval=getattr(task, "sensorPokeInterval"),
                sensor_timeout_minutes=getattr(task, "sensorTimeoutMinutes"),
                sensor_connection=getattr(task, "sensorConnection"),
                sensor_soft_fail=getattr(task, "sensorSoftFail"),
                sudo_user=getattr(task, "sudoUser"),
            )

            session.execute(query)
            session.commit()
        except SQLerror as err:
            log.error(str(err))
            log.error(column)  # noqa: F821  # TODO(#164): column is undefined here

            # session.close()
            session.remove()
            return False

        session.remove()
        return True

    def getAirflowExportDag(self, dagname):
        """Returns an Airflow Export DAG"""
        log = logging.getLogger(self.logger)

        try:
            session = self.getDBImportSession()
        except SQLerror:
            self.disconnectDBImportDB()
            return None

        airflowURL = self.getConfigValue("airflow_url")

        airflowExportDags = aliased(configSchema.airflowExportDags)

        row = (
            session.query(
                airflowExportDags.dag_name,
                airflowExportDags.schedule_interval,
                airflowExportDags.filter_dbalias,
                airflowExportDags.filter_target_schema,
                airflowExportDags.filter_target_table,
                airflowExportDags.retries,
                airflowExportDags.operator_notes,
                airflowExportDags.application_notes,
                airflowExportDags.auto_regenerate_dag,
                airflowExportDags.airflow_notes,
                airflowExportDags.sudo_user,
                airflowExportDags.timezone,
                airflowExportDags.email,
                airflowExportDags.email_on_failure,
                airflowExportDags.email_on_retries,
                airflowExportDags.tags,
                airflowExportDags.sla_warning_time,
                airflowExportDags.retry_exponential_backoff,
                airflowExportDags.concurrency,
            )
            .select_from(airflowExportDags)
            .filter(airflowExportDags.dag_name == dagname)
            .one_or_none()
        )

        if row == None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Airflow DAG does not exist",
            )

        resultDict = {}
        resultDict["name"] = row[0]
        resultDict["scheduleInterval"] = row[1]
        resultDict["filterConnection"] = row[2]
        resultDict["filterTargetSchema"] = row[3]
        resultDict["filterTargetTable"] = row[4]
        resultDict["retries"] = row[5]
        if resultDict["retries"] == None or resultDict["retries"] == "":
            resultDict["retries"] = 5
        resultDict["operatorNotes"] = row[6]
        resultDict["applicationNotes"] = row[7]
        resultDict["autoRegenerateDag"] = self.convertIntToBool(row[8])
        resultDict["airflowNotes"] = row[9]
        resultDict["sudoUser"] = row[10]
        resultDict["timezone"] = row[11]
        resultDict["email"] = row[12]
        resultDict["emailOnFailure"] = self.convertIntToBool(row[13])
        resultDict["emailOnRetries"] = self.convertIntToBool(row[14])
        resultDict["tags"] = row[15]
        try:
            resultDict["slaWarningTime"] = row[16].strftime("%H:%M:%S")
        except AttributeError:
            resultDict["slaWarningTime"] = None
        resultDict["retryExponentialBackoff"] = row[17]
        resultDict["concurrency"] = row[18]
        resultDict["airflowLink"] = "%s/dags/%s" % (airflowURL, row[0])

        resultDict["tasks"] = self.getAirflowTasks(dagname)

        jsonResult = json.loads(json.dumps(resultDict))
        # session.close()
        session.remove()

        return jsonResult

    def getAirflowImportDag(self, dagname):
        """Returns an Airflow Import DAG"""
        log = logging.getLogger(self.logger)

        try:
            session = self.getDBImportSession()
        except SQLerror:
            self.disconnectDBImportDB()
            return None

        airflowURL = self.getConfigValue("airflow_url")

        airflowImportDags = aliased(configSchema.airflowImportDags)

        row = (
            session.query(
                airflowImportDags.dag_name,
                airflowImportDags.schedule_interval,
                airflowImportDags.filter_hive,
                airflowImportDags.finish_all_stage1_first,
                airflowImportDags.run_import_and_etl_separate,
                airflowImportDags.retries,
                airflowImportDags.retries_stage1,
                airflowImportDags.retries_stage2,
                airflowImportDags.pool_stage1,
                airflowImportDags.pool_stage2,
                airflowImportDags.operator_notes,
                airflowImportDags.application_notes,
                airflowImportDags.auto_regenerate_dag,
                airflowImportDags.airflow_notes,
                airflowImportDags.sudo_user,
                airflowImportDags.metadata_import,
                airflowImportDags.timezone,
                airflowImportDags.email,
                airflowImportDags.email_on_failure,
                airflowImportDags.email_on_retries,
                airflowImportDags.tags,
                airflowImportDags.sla_warning_time,
                airflowImportDags.retry_exponential_backoff,
                airflowImportDags.concurrency,
            )
            .select_from(airflowImportDags)
            .filter(airflowImportDags.dag_name == dagname)
            .one_or_none()
        )

        if row == None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Airflow DAG does not exist",
            )

        resultDict = {}
        resultDict["name"] = row[0]
        resultDict["scheduleInterval"] = row[1]
        resultDict["filterTable"] = row[2]
        resultDict["finishAllStage1First"] = row[3]
        resultDict["runImportAndEtlSeparate"] = row[4]
        resultDict["retries"] = row[5]
        if resultDict["retries"] == None or resultDict["retries"] == "":
            resultDict["retries"] = 5
        resultDict["retriesStage1"] = row[6]
        resultDict["retriesStage2"] = row[7]
        resultDict["poolStage1"] = row[8]
        resultDict["poolStage2"] = row[9]
        resultDict["operatorNotes"] = row[10]
        resultDict["applicationNotes"] = row[11]
        resultDict["autoRegenerateDag"] = self.convertIntToBool(row[12])
        resultDict["airflowNotes"] = row[13]
        resultDict["sudoUser"] = row[14]
        resultDict["metadataImport"] = row[15]
        resultDict["timezone"] = row[16]
        resultDict["email"] = row[17]
        resultDict["emailOnFailure"] = self.convertIntToBool(row[18])
        resultDict["emailOnRetries"] = self.convertIntToBool(row[19])
        resultDict["tags"] = row[20]
        try:
            resultDict["slaWarningTime"] = row[21].strftime("%H:%M:%S")
        except AttributeError:
            resultDict["slaWarningTime"] = None
        resultDict["retryExponentialBackoff"] = row[22]
        resultDict["concurrency"] = row[23]
        resultDict["airflowLink"] = "%s/dags/%s" % (airflowURL, row[0])

        resultDict["tasks"] = self.getAirflowTasks(dagname)

        jsonResult = json.loads(json.dumps(resultDict))
        # session.close()
        session.remove()

        return jsonResult

    def deleteAirflowTasks(self, dagname):
        """Delete all Airflow tasks for the specified DAG"""
        log = logging.getLogger(self.logger)

        try:
            session = self.getDBImportSession()
        except SQLerror:
            self.disconnectDBImportDB()
            return None

        (
            session.query(configSchema.airflowTasks)
            .filter(configSchema.airflowTasks.dag_name == dagname)
            .delete()
        )
        session.commit()
        session.remove()

    def deleteTaskFromAirflowDag(self, dagname, taskname, currentUser):
        """Delete tasks from an Airflow DAG"""
        log = logging.getLogger(self.logger)

        try:
            session = self.getDBImportSession()
        except SQLerror:
            self.disconnectDBImportDB()
            return None

        airflowImportDags = aliased(configSchema.airflowImportDags)
        resultMsg = {"result": "ok"}
        returnCode = 200

        log.info(
            "User '%s' deleted Airflow task '%s' from DAG '%s'"
            % (currentUser, taskname, dagname)
        )

        # Fetch the tableID as that is required to update/create data in import_columns
        row = (
            session.query(configSchema.airflowTasks.dag_name)
            .select_from(configSchema.airflowTasks)
            .filter(
                (configSchema.airflowTasks.dag_name == dagname)
                & (configSchema.airflowTasks.task_name == taskname)
            )
            .one_or_none()
        )
        session.commit()

        if row == None:
            resultMsg = {"result": "Airflow Task on DAG does not exist"}
            returnCode = 404
        else:
            (
                session.query(configSchema.airflowTasks)
                .filter(
                    (configSchema.airflowTasks.dag_name == dagname)
                    & (configSchema.airflowTasks.task_name == taskname)
                )
                .delete()
            )
            session.commit()

        session.remove()
        return (resultMsg, returnCode)

    def deleteImportAirflowDag(self, dagname, currentUser):
        """Delete an Airflow import DAG"""
        log = logging.getLogger(self.logger)

        try:
            session = self.getDBImportSession()
        except SQLerror:
            self.disconnectDBImportDB()
            return None

        airflowImportDags = aliased(configSchema.airflowImportDags)
        result = "ok"
        returnCode = 200

        log.info("User '%s' deleted Airflow import DAG '%s'" % (currentUser, dagname))

        # Fetch the tableID as that is required to update/create data in import_columns
        row = (
            session.query(configSchema.airflowImportDags.dag_name)
            .select_from(configSchema.airflowImportDags)
            .filter(configSchema.airflowImportDags.dag_name == dagname)
            .one_or_none()
        )
        session.commit()

        if row == None:
            result = "Airflow DAG does not exist"
            returnCode = 404
        else:
            (
                session.query(configSchema.airflowImportDags)
                .filter(configSchema.airflowImportDags.dag_name == dagname)
                .delete()
            )
            session.commit()

            self.deleteAirflowTasks(dagname)

        # session.close()
        session.remove()
        return (result, returnCode)

    def bulkDeleteImportAirflowDag(self, bulkData, currentUser):
        """Bulk delete of Airflow import Dags"""
        log = logging.getLogger(self.logger)

        if len(bulkData) == 0:
            resultMsg = {"result": "Body must contain a list of Airflow dags to delete"}
            returnCode = 422
            return (resultMsg, returnCode)

        try:
            session = self.getDBImportSession()
        except SQLerror:
            self.disconnectDBImportDB()
            return None

        deleteStatement = delete(configSchema.airflowImportDags)
        deleteStatement = deleteStatement.where(
            configSchema.airflowImportDags.dag_name == bindparam("dag")
        )

        # Create a list of dicts that contain all the databas and table combinations that will be deleted
        tableList = []
        for tables in bulkData:
            tableList.append({"dag": tables.name})

        session.connection().execute(deleteStatement, tableList)
        session.commit()
        session.remove()

        returnCode = 200
        resultMsg = {"result": "Ok"}

        return (resultMsg, returnCode)

    def bulkUpdateImportAirflowDag(self, bulkData, currentUser):
        """Bulk update of Airflow import Dags"""
        log = logging.getLogger(self.logger)

        if len(getattr(bulkData, "dags")) == 0:
            resultMsg = {"result": "dags must contain a list of Airflow dags to update"}
            returnCode = 422
            return (resultMsg, returnCode)

        try:
            session = self.getDBImportSession()
        except SQLerror:
            self.disconnectDBImportDB()
            return None

        exportTables = aliased(configSchema.exportTables)
        airflowImportDags = aliased(configSchema.airflowImportDags)
        updateStatement = update(airflowImportDags)
        updateStatement = updateStatement.where(
            airflowImportDags.dag_name == bindparam("dag")
        )

        # This will loop through only the fields specified in the Posted JSON. So if a field have got a default None value in dataModel validation,
        # that field wont be part of this loop
        for updateField in bulkData.__pydantic_fields_set__:
            if updateField == "scheduleInterval":
                updateStatement = updateStatement.values(
                    schedule_interval=getattr(bulkData, "scheduleInterval")
                )
            elif updateField == "operatorNotes":
                updateStatement = updateStatement.values(
                    operator_notes=getattr(bulkData, "operatorNotes")
                )
            elif updateField == "autoRegenerateDag":
                updateStatement = updateStatement.values(
                    auto_regenerate_dag=getattr(bulkData, "autoRegenerateDag")
                )
            elif updateField == "sudoUser":
                updateStatement = updateStatement.values(
                    sudo_user=getattr(bulkData, "sudoUser")
                )
            elif updateField == "timezone":
                updateStatement = updateStatement.values(
                    timezone=getattr(bulkData, "timezone")
                )
            elif updateField == "email":
                updateStatement = updateStatement.values(
                    email=getattr(bulkData, "email")
                )
            elif updateField == "emailOnFailure":
                updateStatement = updateStatement.values(
                    email_on_failure=getattr(bulkData, "emailOnFailure")
                )
            elif updateField == "emailOnRetries":
                updateStatement = updateStatement.values(
                    email_on_retries=getattr(bulkData, "emailOnRetries")
                )
            elif updateField == "slaWarningTime":
                updateStatement = updateStatement.values(
                    sla_warning_time=getattr(bulkData, "slaWarningTime")
                )
            elif updateField == "concurrency":
                updateStatement = updateStatement.values(
                    concurrency=getattr(bulkData, "concurrency")
                )

        # Create a list of dicts that contain all the databas/table combinations that will be updated
        tableList = []
        for tables in getattr(bulkData, "dags"):
            tableList.append({"dag": tables.name})

        updateStatement = updateStatement.execution_options(synchronize_session=False)
        # 		print(updateStatement)

        session.connection().execute(updateStatement, tableList)
        session.commit()
        session.remove()

        returnCode = 200
        resultMsg = {"result": "Ok"}

        return (resultMsg, returnCode)

    def updateImportAirflowDag(self, dagname, airflowDag, currentUser):
        """Update or create an Airflow import DAG"""
        log = logging.getLogger(self.logger)

        try:
            session = self.getDBImportSession()
        except SQLerror:
            self.disconnectDBImportDB()
            return None

        airflowImportDags = aliased(configSchema.airflowImportDags)
        result = "ok"
        returnCode = 200

        if self.checkDagNameAvailability(name=dagname, excludeImport=True) == False:
            log.error(
                "Airflow Import DAG creation was denied as the name was already existing on custom or export DAG"
            )
            result = (
                "Ariflow DAG with the specified name already exists as Custom or Export"
            )
            returnCode = 400
            return (result, returnCode)

        log.debug(airflowDag)
        log.info(
            "User '%s' updated/created Airflow import DAG '%s'" % (currentUser, dagname)
        )

        # Set default values
        if getattr(airflowDag, "scheduleInterval") == None:
            setattr(airflowDag, "scheduleInterval", "None")
        if getattr(airflowDag, "filterTable") == None:
            setattr(airflowDag, "filterTable", "")
        if getattr(airflowDag, "finishAllStage1First") == None:
            setattr(airflowDag, "finishAllStage1First", "0")
        if getattr(airflowDag, "retries") == None:
            setattr(airflowDag, "retries", "5")
        if getattr(airflowDag, "autoRegenerateDag") == None:
            setattr(airflowDag, "autoRegenerateDag", "1")
        if getattr(airflowDag, "runImportAndEtlSeparate") == None:
            setattr(airflowDag, "runImportAndEtlSeparate", "0")
        if getattr(airflowDag, "metadataImport") == None:
            setattr(airflowDag, "metadataImport", "0")
        if getattr(airflowDag, "emailOnFailure") == None:
            setattr(airflowDag, "emailOnFailure", "0")
        if getattr(airflowDag, "emailOnRetries") == None:
            setattr(airflowDag, "emailOnRetries", "0")
        if getattr(airflowDag, "retryExponentialBackoff") == None:
            setattr(airflowDag, "retryExponentialBackoff", "0")

        try:
            query = insert(configSchema.airflowImportDags).values(
                dag_name=dagname,
                schedule_interval=getattr(airflowDag, "scheduleInterval"),
                filter_hive=getattr(airflowDag, "filterTable"),
                finish_all_stage1_first=getattr(airflowDag, "finishAllStage1First"),
                run_import_and_etl_separate=getattr(
                    airflowDag, "runImportAndEtlSeparate"
                ),
                retries=getattr(airflowDag, "retries"),
                retries_stage1=getattr(airflowDag, "retriesStage1"),
                retries_stage2=getattr(airflowDag, "retriesStage2"),
                pool_stage1=getattr(airflowDag, "poolStage1"),
                pool_stage2=getattr(airflowDag, "poolStage2"),
                operator_notes=getattr(airflowDag, "operatorNotes"),
                application_notes=getattr(airflowDag, "applicationNotes"),
                auto_regenerate_dag=getattr(airflowDag, "autoRegenerateDag"),
                airflow_notes=getattr(airflowDag, "airflowNotes"),
                sudo_user=getattr(airflowDag, "sudoUser"),
                metadata_import=getattr(airflowDag, "metadataImport"),
                timezone=getattr(airflowDag, "timezone"),
                email=getattr(airflowDag, "email"),
                email_on_failure=getattr(airflowDag, "emailOnFailure"),
                email_on_retries=getattr(airflowDag, "emailOnRetries"),
                tags=getattr(airflowDag, "tags"),
                sla_warning_time=getattr(airflowDag, "slaWarningTime"),
                retry_exponential_backoff=getattr(
                    airflowDag, "retryExponentialBackoff"
                ),
                concurrency=getattr(airflowDag, "concurrency"),
            )

            query = query.on_duplicate_key_update(
                dag_name=dagname,
                schedule_interval=getattr(airflowDag, "scheduleInterval"),
                filter_hive=getattr(airflowDag, "filterTable"),
                finish_all_stage1_first=getattr(airflowDag, "finishAllStage1First"),
                run_import_and_etl_separate=getattr(
                    airflowDag, "runImportAndEtlSeparate"
                ),
                retries=getattr(airflowDag, "retries"),
                retries_stage1=getattr(airflowDag, "retriesStage1"),
                retries_stage2=getattr(airflowDag, "retriesStage2"),
                pool_stage1=getattr(airflowDag, "poolStage1"),
                pool_stage2=getattr(airflowDag, "poolStage2"),
                operator_notes=getattr(airflowDag, "operatorNotes"),
                application_notes=getattr(airflowDag, "applicationNotes"),
                auto_regenerate_dag=getattr(airflowDag, "autoRegenerateDag"),
                airflow_notes=getattr(airflowDag, "airflowNotes"),
                sudo_user=getattr(airflowDag, "sudoUser"),
                metadata_import=getattr(airflowDag, "metadataImport"),
                timezone=getattr(airflowDag, "timezone"),
                email=getattr(airflowDag, "email"),
                email_on_failure=getattr(airflowDag, "emailOnFailure"),
                email_on_retries=getattr(airflowDag, "emailOnRetries"),
                tags=getattr(airflowDag, "tags"),
                sla_warning_time=getattr(airflowDag, "slaWarningTime"),
                retry_exponential_backoff=getattr(
                    airflowDag, "retryExponentialBackoff"
                ),
                concurrency=getattr(airflowDag, "concurrency"),
            )

            session.execute(query)
            session.commit()
        except SQLerror as err:
            log.error(str(err))
            log.error(column)  # noqa: F821  # TODO(#164): column is undefined here

            result = str(err)
            returnCode = 500

            # session.close()
            session.remove()
            return (result, returnCode)

        # session.close()
        session.remove()

        tasks = getattr(airflowDag, "tasks")
        for task in tasks:
            if (
                self.updateAirflowTasks(
                    dagname=dagname, task=task, currentUser=currentUser
                )
                == False
            ):
                result = "Error while updating/creating Airflow tasks. Please check logs on server"
                returnCode = 400
                return (result, returnCode)

        return (result, returnCode)

    def deleteExportAirflowDag(self, dagname, currentUser):
        """Delete an Airflow export DAG"""
        log = logging.getLogger(self.logger)

        try:
            session = self.getDBImportSession()
        except SQLerror:
            self.disconnectDBImportDB()
            return None

        result = "ok"
        returnCode = 200

        log.info("User '%s' deleted Airflow export DAG '%s'" % (currentUser, dagname))

        # Fetch the tableID as that is required to update/create data in import_columns
        row = (
            session.query(configSchema.airflowExportDags.dag_name)
            .select_from(configSchema.airflowExportDags)
            .filter(configSchema.airflowExportDags.dag_name == dagname)
            .one_or_none()
        )
        session.commit()

        if row == None:
            result = "Airflow DAG does not exist"
            returnCode = 404
        else:
            (
                session.query(configSchema.airflowExportDags)
                .filter(configSchema.airflowExportDags.dag_name == dagname)
                .delete()
            )
            session.commit()

            self.deleteAirflowTasks(dagname)

        # session.close()
        session.remove()
        return (result, returnCode)

    def bulkDeleteExportAirflowDag(self, bulkData, currentUser):
        """Bulk delete of Airflow import Dags"""
        log = logging.getLogger(self.logger)

        if len(bulkData) == 0:
            resultMsg = {"result": "Body must contain a list of Airflow dags to delete"}
            returnCode = 422
            return (resultMsg, returnCode)

        try:
            session = self.getDBImportSession()
        except SQLerror:
            self.disconnectDBImportDB()
            return None

        deleteStatement = delete(configSchema.airflowExportDags)
        deleteStatement = deleteStatement.where(
            configSchema.airflowExportDags.dag_name == bindparam("dag")
        )

        # Create a list of dicts that contain all the databas and table combinations that will be deleted
        tableList = []
        for tables in bulkData:
            tableList.append({"dag": tables.name})

        session.connection().execute(deleteStatement, tableList)
        session.commit()
        session.remove()

        returnCode = 200
        resultMsg = {"result": "Ok"}

        return (resultMsg, returnCode)

    def bulkUpdateExportAirflowDag(self, bulkData, currentUser):
        """Bulk update of Airflow export Dags"""
        log = logging.getLogger(self.logger)

        if len(getattr(bulkData, "dags")) == 0:
            resultMsg = {"result": "dags must contain a list of Airflow dags to update"}
            returnCode = 422
            return (resultMsg, returnCode)

        try:
            session = self.getDBImportSession()
        except SQLerror:
            self.disconnectDBImportDB()
            return None

        exportTables = aliased(configSchema.exportTables)
        airflowExportDags = aliased(configSchema.airflowExportDags)
        updateStatement = update(airflowExportDags)
        updateStatement = updateStatement.where(
            airflowExportDags.dag_name == bindparam("dag")
        )

        # This will loop through only the fields specified in the Posted JSON. So if a field have got a default None value in dataModel validation,
        # that field wont be part of this loop
        for updateField in bulkData.__pydantic_fields_set__:
            if updateField == "scheduleInterval":
                updateStatement = updateStatement.values(
                    schedule_interval=getattr(bulkData, "scheduleInterval")
                )
            elif updateField == "operatorNotes":
                updateStatement = updateStatement.values(
                    operator_notes=getattr(bulkData, "operatorNotes")
                )
            elif updateField == "autoRegenerateDag":
                updateStatement = updateStatement.values(
                    auto_regenerate_dag=getattr(bulkData, "autoRegenerateDag")
                )
            elif updateField == "sudoUser":
                updateStatement = updateStatement.values(
                    sudo_user=getattr(bulkData, "sudoUser")
                )
            elif updateField == "timezone":
                updateStatement = updateStatement.values(
                    timezone=getattr(bulkData, "timezone")
                )
            elif updateField == "email":
                updateStatement = updateStatement.values(
                    email=getattr(bulkData, "email")
                )
            elif updateField == "emailOnFailure":
                updateStatement = updateStatement.values(
                    email_on_failure=getattr(bulkData, "emailOnFailure")
                )
            elif updateField == "emailOnRetries":
                updateStatement = updateStatement.values(
                    email_on_retries=getattr(bulkData, "emailOnRetries")
                )
            elif updateField == "slaWarningTime":
                updateStatement = updateStatement.values(
                    sla_warning_time=getattr(bulkData, "slaWarningTime")
                )
            elif updateField == "concurrency":
                updateStatement = updateStatement.values(
                    concurrency=getattr(bulkData, "concurrency")
                )

        # Create a list of dicts that contain all the databas/table combinations that will be updated
        tableList = []
        for tables in getattr(bulkData, "dags"):
            tableList.append({"dag": tables.name})

        updateStatement = updateStatement.execution_options(synchronize_session=False)
        # 		print(updateStatement)

        session.connection().execute(updateStatement, tableList)
        session.commit()
        session.remove()

        returnCode = 200
        resultMsg = {"result": "Ok"}

        return (resultMsg, returnCode)

    def updateExportAirflowDag(self, dagname, airflowDag, currentUser):
        """Update or create an Airflow export DAG"""
        log = logging.getLogger(self.logger)

        try:
            session = self.getDBImportSession()
        except SQLerror:
            self.disconnectDBImportDB()
            return None

        airflowExportDags = aliased(configSchema.airflowExportDags)
        result = "ok"
        returnCode = 200

        if self.checkDagNameAvailability(name=dagname, excludeExport=True) == False:
            log.error(
                "Airflow Import DAG creation was denied as the name was already existing on custom or export DAG"
            )
            result = (
                "Ariflow DAG with the specified name already exists as Custom or Export"
            )
            returnCode = 400
            return (result, returnCode)

        log.debug(airflowDag)
        log.info(
            "User '%s' updated/created Airflow export DAG '%s'" % (currentUser, dagname)
        )

        # Set default values
        if getattr(airflowDag, "scheduleInterval") == None:
            setattr(airflowDag, "scheduleInterval", "None")
        if getattr(airflowDag, "filterConnection") == None:
            setattr(airflowDag, "filterConnection", "")
        if getattr(airflowDag, "retries") == None:
            setattr(airflowDag, "retries", "5")
        if getattr(airflowDag, "autoRegenerateDag") == None:
            setattr(airflowDag, "autoRegenerateDag", "1")
        if getattr(airflowDag, "emailOnFailure") == None:
            setattr(airflowDag, "emailOnFailure", "0")
        if getattr(airflowDag, "emailOnRetries") == None:
            setattr(airflowDag, "emailOnRetries", "0")
        if getattr(airflowDag, "retryExponentialBackoff") == None:
            setattr(airflowDag, "retryExponentialBackoff", "0")

        try:
            query = insert(configSchema.airflowExportDags).values(
                dag_name=dagname,
                schedule_interval=getattr(airflowDag, "scheduleInterval"),
                filter_dbalias=getattr(airflowDag, "filterConnection"),
                filter_target_schema=getattr(airflowDag, "filterTargetSchema"),
                filter_target_table=getattr(airflowDag, "filterTargetTable"),
                retries=getattr(airflowDag, "retries"),
                operator_notes=getattr(airflowDag, "operatorNotes"),
                application_notes=getattr(airflowDag, "applicationNotes"),
                auto_regenerate_dag=getattr(airflowDag, "autoRegenerateDag"),
                airflow_notes=getattr(airflowDag, "airflowNotes"),
                sudo_user=getattr(airflowDag, "sudoUser"),
                timezone=getattr(airflowDag, "timezone"),
                email=getattr(airflowDag, "email"),
                email_on_failure=getattr(airflowDag, "emailOnFailure"),
                email_on_retries=getattr(airflowDag, "emailOnRetries"),
                tags=getattr(airflowDag, "tags"),
                sla_warning_time=getattr(airflowDag, "slaWarningTime"),
                retry_exponential_backoff=getattr(
                    airflowDag, "retryExponentialBackoff"
                ),
                concurrency=getattr(airflowDag, "concurrency"),
            )

            query = query.on_duplicate_key_update(
                dag_name=dagname,
                schedule_interval=getattr(airflowDag, "scheduleInterval"),
                filter_dbalias=getattr(airflowDag, "filterConnection"),
                filter_target_schema=getattr(airflowDag, "filterTargetSchema"),
                filter_target_table=getattr(airflowDag, "filterTargetTable"),
                retries=getattr(airflowDag, "retries"),
                operator_notes=getattr(airflowDag, "operatorNotes"),
                application_notes=getattr(airflowDag, "applicationNotes"),
                auto_regenerate_dag=getattr(airflowDag, "autoRegenerateDag"),
                airflow_notes=getattr(airflowDag, "airflowNotes"),
                sudo_user=getattr(airflowDag, "sudoUser"),
                timezone=getattr(airflowDag, "timezone"),
                email=getattr(airflowDag, "email"),
                email_on_failure=getattr(airflowDag, "emailOnFailure"),
                email_on_retries=getattr(airflowDag, "emailOnRetries"),
                tags=getattr(airflowDag, "tags"),
                sla_warning_time=getattr(airflowDag, "slaWarningTime"),
                retry_exponential_backoff=getattr(
                    airflowDag, "retryExponentialBackoff"
                ),
                concurrency=getattr(airflowDag, "concurrency"),
            )

            session.execute(query)
            session.commit()
        except SQLerror as err:
            log.error(str(err))
            log.error(column)  # noqa: F821  # TODO(#164): column is undefined here

            result = str(err)
            returnCode = 500

            # session.close()
            session.remove()
            return (result, returnCode)

        # session.close()
        session.remove()

        tasks = getattr(airflowDag, "tasks")
        for task in tasks:
            if (
                self.updateAirflowTasks(
                    dagname=dagname, task=task, currentUser=currentUser
                )
                == False
            ):
                result = "Error while updating/creating Airflow tasks. Please check logs on server"
                returnCode = 400
                return (result, returnCode)

        return (result, returnCode)

    def deleteCustomAirflowDag(self, dagname, currentUser):
        """Delete an Airflow import DAG"""
        log = logging.getLogger(self.logger)

        try:
            session = self.getDBImportSession()
        except SQLerror:
            self.disconnectDBImportDB()
            return None

        result = "ok"
        returnCode = 200

        log.info("User '%s' deleted Airflow import DAG '%s'" % (currentUser, dagname))

        # Fetch the tableID as that is required to update/create data in import_columns
        row = (
            session.query(configSchema.airflowCustomDags.dag_name)
            .select_from(configSchema.airflowCustomDags)
            .filter(configSchema.airflowCustomDags.dag_name == dagname)
            .one_or_none()
        )
        session.commit()

        if row == None:
            result = "Airflow DAG does not exist"
            returnCode = 404
        else:
            (
                session.query(configSchema.airflowCustomDags)
                .filter(configSchema.airflowCustomDags.dag_name == dagname)
                .delete()
            )
            session.commit()

            self.deleteAirflowTasks(dagname)

        # session.close()
        session.remove()
        return (result, returnCode)

    def bulkDeleteCustomAirflowDag(self, bulkData, currentUser):
        """Bulk delete of Airflow custom Dags"""
        log = logging.getLogger(self.logger)

        if len(bulkData) == 0:
            resultMsg = {"result": "Body must contain a list of Airflow dags to delete"}
            returnCode = 422
            return (resultMsg, returnCode)

        try:
            session = self.getDBImportSession()
        except SQLerror:
            self.disconnectDBImportDB()
            return None

        deleteStatement = delete(configSchema.airflowCustomDags)
        deleteStatement = deleteStatement.where(
            configSchema.airflowCustomDags.dag_name == bindparam("dag")
        )

        # Create a list of dicts that contain all the databas and table combinations that will be deleted
        tableList = []
        for tables in bulkData:
            tableList.append({"dag": tables.name})

        session.connection().execute(deleteStatement, tableList)
        session.commit()
        session.remove()

        returnCode = 200
        resultMsg = {"result": "Ok"}

        return (resultMsg, returnCode)

    def bulkUpdateCustomAirflowDag(self, bulkData, currentUser):
        """Bulk update of Airflow custom Dags"""
        log = logging.getLogger(self.logger)

        if len(getattr(bulkData, "dags")) == 0:
            resultMsg = {"result": "dags must contain a list of Airflow dags to update"}
            returnCode = 422
            return (resultMsg, returnCode)

        try:
            session = self.getDBImportSession()
        except SQLerror:
            self.disconnectDBImportDB()
            return None

        exportTables = aliased(configSchema.exportTables)
        airflowCustomDags = aliased(configSchema.airflowCustomDags)
        updateStatement = update(airflowCustomDags)
        updateStatement = updateStatement.where(
            airflowCustomDags.dag_name == bindparam("dag")
        )

        # This will loop through only the fields specified in the Posted JSON. So if a field have got a default None value in dataModel validation,
        # that field wont be part of this loop
        for updateField in bulkData.__pydantic_fields_set__:
            if updateField == "scheduleInterval":
                updateStatement = updateStatement.values(
                    schedule_interval=getattr(bulkData, "scheduleInterval")
                )
            elif updateField == "operatorNotes":
                updateStatement = updateStatement.values(
                    operator_notes=getattr(bulkData, "operatorNotes")
                )
            elif updateField == "autoRegenerateDag":
                updateStatement = updateStatement.values(
                    auto_regenerate_dag=getattr(bulkData, "autoRegenerateDag")
                )
            elif updateField == "sudoUser":
                updateStatement = updateStatement.values(
                    sudo_user=getattr(bulkData, "sudoUser")
                )
            elif updateField == "timezone":
                updateStatement = updateStatement.values(
                    timezone=getattr(bulkData, "timezone")
                )
            elif updateField == "email":
                updateStatement = updateStatement.values(
                    email=getattr(bulkData, "email")
                )
            elif updateField == "emailOnFailure":
                updateStatement = updateStatement.values(
                    email_on_failure=getattr(bulkData, "emailOnFailure")
                )
            elif updateField == "emailOnRetries":
                updateStatement = updateStatement.values(
                    email_on_retries=getattr(bulkData, "emailOnRetries")
                )
            elif updateField == "slaWarningTime":
                updateStatement = updateStatement.values(
                    sla_warning_time=getattr(bulkData, "slaWarningTime")
                )
            elif updateField == "concurrency":
                updateStatement = updateStatement.values(
                    concurrency=getattr(bulkData, "concurrency")
                )

        # Create a list of dicts that contain all the databas/table combinations that will be updated
        tableList = []
        for tables in getattr(bulkData, "dags"):
            tableList.append({"dag": tables.name})

        updateStatement = updateStatement.execution_options(synchronize_session=False)
        # 		print(updateStatement)

        session.connection().execute(updateStatement, tableList)
        session.commit()
        session.remove()

        returnCode = 200
        resultMsg = {"result": "Ok"}

        return (resultMsg, returnCode)

    def updateCustomAirflowDag(self, dagname, airflowDag, currentUser):
        """Update or create an Airflow custom DAG"""
        log = logging.getLogger(self.logger)

        try:
            session = self.getDBImportSession()
        except SQLerror:
            self.disconnectDBImportDB()
            return None

        airflowCustomDags = aliased(configSchema.airflowCustomDags)
        result = "ok"
        returnCode = 200

        if self.checkDagNameAvailability(name=dagname, excludeCustom=True) == False:
            log.error(
                "Airflow Import DAG creation was denied as the name was already existing on custom or export DAG"
            )
            result = (
                "Ariflow DAG with the specified name already exists as Custom or Export"
            )
            returnCode = 400
            return (result, returnCode)

        log.debug(airflowDag)
        log.info(
            "User '%s' updated/created Airflow custom DAG '%s'" % (currentUser, dagname)
        )

        # Set default values
        if getattr(airflowDag, "scheduleInterval") == None:
            setattr(airflowDag, "scheduleInterval", "None")
        if getattr(airflowDag, "retries") == None:
            setattr(airflowDag, "retries", "0")
        if getattr(airflowDag, "autoRegenerateDag") == None:
            setattr(airflowDag, "autoRegenerateDag", "1")
        if getattr(airflowDag, "emailOnFailure") == None:
            setattr(airflowDag, "emailOnFailure", "0")
        if getattr(airflowDag, "emailOnRetries") == None:
            setattr(airflowDag, "emailOnRetries", "0")
        if getattr(airflowDag, "retryExponentialBackoff") == None:
            setattr(airflowDag, "retryExponentialBackoff", "0")

        try:
            query = insert(configSchema.airflowCustomDags).values(
                dag_name=dagname,
                schedule_interval=getattr(airflowDag, "scheduleInterval"),
                retries=getattr(airflowDag, "retries"),
                operator_notes=getattr(airflowDag, "operatorNotes"),
                application_notes=getattr(airflowDag, "applicationNotes"),
                auto_regenerate_dag=getattr(airflowDag, "autoRegenerateDag"),
                airflow_notes=getattr(airflowDag, "airflowNotes"),
                sudo_user=getattr(airflowDag, "sudoUser"),
                timezone=getattr(airflowDag, "timezone"),
                email=getattr(airflowDag, "email"),
                email_on_failure=getattr(airflowDag, "emailOnFailure"),
                email_on_retries=getattr(airflowDag, "emailOnRetries"),
                tags=getattr(airflowDag, "tags"),
                sla_warning_time=getattr(airflowDag, "slaWarningTime"),
                retry_exponential_backoff=getattr(
                    airflowDag, "retryExponentialBackoff"
                ),
                concurrency=getattr(airflowDag, "concurrency"),
            )

            query = query.on_duplicate_key_update(
                dag_name=dagname,
                schedule_interval=getattr(airflowDag, "scheduleInterval"),
                retries=getattr(airflowDag, "retries"),
                operator_notes=getattr(airflowDag, "operatorNotes"),
                application_notes=getattr(airflowDag, "applicationNotes"),
                auto_regenerate_dag=getattr(airflowDag, "autoRegenerateDag"),
                airflow_notes=getattr(airflowDag, "airflowNotes"),
                sudo_user=getattr(airflowDag, "sudoUser"),
                timezone=getattr(airflowDag, "timezone"),
                email=getattr(airflowDag, "email"),
                email_on_failure=getattr(airflowDag, "emailOnFailure"),
                email_on_retries=getattr(airflowDag, "emailOnRetries"),
                tags=getattr(airflowDag, "tags"),
                sla_warning_time=getattr(airflowDag, "slaWarningTime"),
                retry_exponential_backoff=getattr(
                    airflowDag, "retryExponentialBackoff"
                ),
                concurrency=getattr(airflowDag, "concurrency"),
            )

            session.execute(query)
            session.commit()
        except SQLerror as err:
            log.error(str(err))
            log.error(column)  # noqa: F821  # TODO(#164): column is undefined here

            result = str(err)
            returnCode = 500

            # session.close()
            session.remove()
            return (result, returnCode)

        # session.close()
        session.remove()

        tasks = getattr(airflowDag, "tasks")
        for task in tasks:
            if (
                self.updateAirflowTasks(
                    dagname=dagname, task=task, currentUser=currentUser
                )
                == False
            ):
                result = "Error while updating/creating Airflow tasks. Please check logs on server"
                returnCode = 400
                return (result, returnCode)

        return (result, returnCode)
