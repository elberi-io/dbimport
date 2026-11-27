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
from collections.abc import Sequence
from datetime import datetime, time

import bcrypt
import sqlalchemy as sa
from fastapi import HTTPException, status
from fastapi.responses import JSONResponse
from pydantic import ValidationError
from sqlalchemy.orm import scoped_session, sessionmaker

from dbimport.common import constants as constant
from dbimport.common.exceptions import InvalidConfigurationError, SQLError
from dbimport.config_db import common_config, config_keys
from dbimport.config_db import engine as config_engine
from dbimport.config_db.repositories.airflow_dags import AirflowDAGsRepository
from dbimport.config_db.repositories.auth_users import AuthUsersRepository, StoredUser
from dbimport.config_db.repositories.configuration import (
    ConfigurationError,
    ConfigurationRepository,
    UnknownKeyError,
)
from dbimport.config_db.repositories.connections import (
    ConnectionsRepository,
    JDBCConnection,
    JDBCDriver,
)
from dbimport.config_db.repositories.export_columns import ExportColumnsRepository
from dbimport.config_db.repositories.export_tables import ExportTablesRepository
from dbimport.config_db.repositories.import_columns import ImportColumnsRepository
from dbimport.config_db.repositories.import_tables import ImportTablesRepository
from dbimport.server import data_models
from dbimport.settings import get_settings

_CONNECTION_FIELDS = {
    "dbalias": "name",
    "jdbc_url": "connectionString",
    "private_key_path": "privateKeyPath",
    "public_key_path": "publicKeyPath",
    "credentials": "credentials",
    "datalake_source": "source",
    "max_import_sessions": "maxSessions",
    "force_string": "forceString",
    "create_datalake_import": "createDatalakeImport",
    "timewindow_start": "timeWindowStart",
    "timewindow_stop": "timeWindowStop",
    "timewindow_timezone": "timeWindowTimezone",
    "operator_notes": "operatorNotes",
    "create_foreign_keys": "createForeignKey",
    "contact_info": "contactInformation",
    "description": "description",
    "owner": "owner",
    "atlas_discovery": "atlasDiscovery",
    "atlas_include_filter": "atlasIncludeFilter",
    "atlas_exclude_filter": "atlasExcludeFilter",
    "atlas_last_discovery": "atlasLastDiscovery",
    "environment": "environment",
    "seed_file": "seedFile",
}

_CONNECTION_DEFAULTS = {
    "force_string": 0,
    "create_datalake_import": 1,
    "create_foreign_keys": 1,
    "atlas_discovery": 1,
}


_IMPORT_TABLE_FIELDS = {
    "dbalias": "connection",
    "source_schema": "sourceSchema",
    "source_table": "sourceTable",
    "import_phase_type": "importPhaseType",
    "etl_phase_type": "etlPhaseType",
    "import_tool": "importTool",
    "etl_engine": "etlEngine",
    "last_update_from_source": "lastUpdateFromSource",
    "sqoop_sql_where_addition": "sqlWhereAddition",
    "nomerge_ingestion_sql_addition": "nomergeIngestionSqlAddition",
    "include_in_airflow": "includeInAirflow",
    "airflow_priority": "airflowPriority",
    "validate_import": "validateImport",
    "validation_method": "validationMethod",
    "validate_source": "validateSource",
    "validate_diff_allowed": "validateDiffAllowed",
    "validation_custom_query_source_sql": "validationCustomQuerySourceSQL",
    "validation_custom_query_hive_sql": "validationCustomQueryHiveSQL",
    "validation_custom_query_validate_import_table": (
        "validationCustomQueryValidateImportTable"
    ),
    "mappers": "sqlSessions",
    "soft_delete_during_merge": "softDeleteDuringMerge",
    "incr_mode": "incrMode",
    "incr_column": "incrColumn",
    "incr_validation_method": "incrValidationMethod",
    "pk_column_override": "pkColumnOverride",
    "pk_column_override_mergeonly": "pkColumnOverrideMergeonly",
    "hive_merge_heap": "hiveContainerSize",
    "hive_split_count": "splitCount",
    "spark_executor_memory": "sparkExecutorMemory",
    "spark_executors": "sparkExecutors",
    "split_by_column": "splitByColumn",
    "sqoop_query": "sqoopCustomQuery",
    "sqoop_options": "sqoopOptions",
    "sqoop_use_generated_sql": "sqoopUseGeneratedSql",
    "sqoop_allow_text_splitter": "allowTextSplitter",
    "force_string": "forceString",
    "comment": "comment",
    "datalake_source": "datalakeSource",
    "operator_notes": "operatorNotes",
    "copy_slave": "copySlave",
    "create_foreign_keys": "createForeignKeys",
    "invalidate_impala": "invalidateImpala",
    "custom_max_query": "customMaxQuery",
    "merge_compaction_method": "mergeCompactionMethod",
    "source_table_type": "sourceTableType",
    "import_database": "importDatabase",
    "import_table": "importTable",
    "history_database": "historyDatabase",
    "history_table": "historyTable",
}

_IMPORT_TABLE_DEFAULTS = {
    "include_in_airflow": 1,
    "validate_import": 1,
    "validation_method": "rowCount",
    "validate_source": "query",
    "validate_diff_allowed": -1,
    "validation_custom_query_validate_import_table": 1,
    "incr_validation_method": "full",
    "mappers": -1,
    "soft_delete_during_merge": 0,
    "sqoop_use_generated_sql": -1,
    "sqoop_allow_text_splitter": 0,
    "force_string": -1,
    "invalidate_impala": -1,
    "merge_compaction_method": "default",
    "create_foreign_keys": -1,
    "copy_slave": 0,
}

_IMPORT_TABLE_SEARCH = {
    "database": "hive_db",
    "table": "hive_table",
    "connection": "dbalias",
    "sourceSchema": "source_schema",
    "sourceTable": "source_table",
    "importPhaseType": "import_phase_type",
    "etlPhaseType": "etl_phase_type",
    "importTool": "import_tool",
    "etlEngine": "etl_engine",
    "includeInAirflow": "include_in_airflow",
    "operatorNotes": "operator_notes",
}

_IMPORT_TABLE_BULK = {
    "connection": "dbalias",
    "importPhaseType": "import_phase_type",
    "etlPhaseType": "etl_phase_type",
    "importTool": "import_tool",
    "etlEngine": "etl_engine",
    "includeInAirflow": "include_in_airflow",
    "operatorNotes": "operator_notes",
}

_IMPORT_COLUMN_FIELDS = {
    "column_name": "columnName",
    "column_order": "columnOrder",
    "source_column_name": "sourceColumnName",
    "column_type": "columnType",
    "source_column_type": "sourceColumnType",
    "source_database_type": "sourceDatabaseType",
    "column_name_override": "columnNameOverride",
    "column_type_override": "columnTypeOverride",
    "sqoop_column_type": "sqoopColumnType",
    "sqoop_column_type_override": "sqoopColumnTypeOverride",
    "force_string": "forceString",
    "include_in_import": "includeInImport",
    "source_primary_key": "sourcePrimaryKey",
    "last_update_from_source": "lastUpdateFromSource",
    "comment": "comment",
    "operator_notes": "operatorNotes",
    "anonymization_function": "anonymizationFunction",
}


_EXPORT_TABLE_FIELDS = {
    "export_type": "exportType",
    "export_tool": "exportTool",
    "hive_db": "database",
    "hive_table": "table",
    "last_update_from_hive": "lastUpdateFromHive",
    "sql_where_addition": "sqlWhereAddition",
    "include_in_airflow": "includeInAirflow",
    "airflow_priority": "airflowPriority",
    "force_create_temp_table": "forceCreateTempTable",
    "validate_export": "validateExport",
    "validation_method": "validationMethod",
    "validation_custom_query_hive_sql": "validationCustomQueryHiveSQL",
    "validation_custom_query_target_sql": "validationCustomQueryTargetSQL",
    "uppercase_columns": "uppercaseColumns",
    "truncate_target": "truncateTarget",
    "mappers": "sqlSessions",
    "incr_column": "incrColumn",
    "incr_validation_method": "incrValidationMethod",
    "sqoop_options": "sqoopOptions",
    "hive_javaheap": "hiveContainerSize",
    "create_target_table_sql": "createTargetTableSql",
    "operator_notes": "operatorNotes",
}

_EXPORT_TABLE_DEFAULTS = {
    "include_in_airflow": 1,
    "force_create_temp_table": 0,
    "validate_export": 1,
    "validation_method": "rowCount",
    "uppercase_columns": -1,
    "truncate_target": 1,
    "mappers": -1,
    "incr_validation_method": "full",
}

_EXPORT_TABLE_SEARCH = {
    "connection": "dbalias",
    "targetSchema": "target_schema",
    "targetTable": "target_table",
    "database": "hive_db",
    "table": "hive_table",
    "exportType": "export_type",
    "exportTool": "export_tool",
    "includeInAirflow": "include_in_airflow",
    "operatorNotes": "operator_notes",
}

_EXPORT_TABLE_BULK = {
    "exportType": "export_type",
    "exportTool": "export_tool",
    "includeInAirflow": "include_in_airflow",
    "operatorNotes": "operator_notes",
}

_EXPORT_COLUMN_FIELDS = {
    "column_name": "columnName",
    "column_type": "columnType",
    "column_order": "columnOrder",
    "target_column_name": "targetColumnName",
    "target_column_type": "targetColumnType",
    "last_update_from_hive": "lastUpdateFromHive",
    "include_in_export": "includeInExport",
    "comment": "comment",
    "operator_notes": "operatorNotes",
}


_AIRFLOW_IMPORT_DAG_FIELDS = {
    "scheduleInterval": "schedule_interval",
    "filterTable": "filter_hive",
    "finishAllStage1First": "finish_all_stage1_first",
    "runImportAndEtlSeparate": "run_import_and_etl_separate",
    "retries": "retries",
    "retriesStage1": "retries_stage1",
    "retriesStage2": "retries_stage2",
    "poolStage1": "pool_stage1",
    "poolStage2": "pool_stage2",
    "operatorNotes": "operator_notes",
    "applicationNotes": "application_notes",
    "autoRegenerateDag": "auto_regenerate_dag",
    "airflowNotes": "airflow_notes",
    "sudoUser": "sudo_user",
    "metadataImport": "metadata_import",
    "timezone": "timezone",
    "email": "email",
    "emailOnFailure": "email_on_failure",
    "emailOnRetries": "email_on_retries",
    "tags": "tags",
    "slaWarningTime": "sla_warning_time",
    "retryExponentialBackoff": "retry_exponential_backoff",
    "concurrency": "concurrency",
}

_AIRFLOW_EXPORT_DAG_FIELDS = {
    "scheduleInterval": "schedule_interval",
    "filterConnection": "filter_dbalias",
    "filterTargetSchema": "filter_target_schema",
    "filterTargetTable": "filter_target_table",
    "retries": "retries",
    "operatorNotes": "operator_notes",
    "applicationNotes": "application_notes",
    "autoRegenerateDag": "auto_regenerate_dag",
    "airflowNotes": "airflow_notes",
    "sudoUser": "sudo_user",
    "timezone": "timezone",
    "email": "email",
    "emailOnFailure": "email_on_failure",
    "emailOnRetries": "email_on_retries",
    "tags": "tags",
    "slaWarningTime": "sla_warning_time",
    "retryExponentialBackoff": "retry_exponential_backoff",
    "concurrency": "concurrency",
}

_AIRFLOW_CUSTOM_DAG_FIELDS = {
    "scheduleInterval": "schedule_interval",
    "retries": "retries",
    "operatorNotes": "operator_notes",
    "applicationNotes": "application_notes",
    "airflowNotes": "airflow_notes",
    "autoRegenerateDag": "auto_regenerate_dag",
    "sudoUser": "sudo_user",
    "timezone": "timezone",
    "email": "email",
    "emailOnFailure": "email_on_failure",
    "emailOnRetries": "email_on_retries",
    "tags": "tags",
    "slaWarningTime": "sla_warning_time",
    "retryExponentialBackoff": "retry_exponential_backoff",
    "concurrency": "concurrency",
}

_AIRFLOW_DAG_BULK = {
    "scheduleInterval": "schedule_interval",
    "operatorNotes": "operator_notes",
    "autoRegenerateDag": "auto_regenerate_dag",
    "sudoUser": "sudo_user",
    "timezone": "timezone",
    "email": "email",
    "emailOnFailure": "email_on_failure",
    "emailOnRetries": "email_on_retries",
    "slaWarningTime": "sla_warning_time",
    "concurrency": "concurrency",
}

_AIRFLOW_TASK_FIELDS = {
    "type": "task_type",
    "placement": "placement",
    "connection": "jdbc_dbalias",
    "airflowPool": "airflow_pool",
    "airflowPriority": "airflow_priority",
    "includeInAirflow": "include_in_airflow",
    "taskDependencyDownstream": "task_dependency_downstream",
    "taskDependencyUpstream": "task_dependency_upstream",
    "taskConfig": "task_config",
    "sensorPokeInterval": "sensor_poke_interval",
    "sensorTimeoutMinutes": "sensor_timeout_minutes",
    "sensorConnection": "sensor_connection",
    "sensorSoftFail": "sensor_soft_fail",
    "sudoUser": "sudo_user",
}


def _save_airflow_tasks(
    dags: AirflowDAGsRepository,
    dag_name: str,
    tasks: Sequence[data_models.AirflowTask],
) -> None:
    for task in tasks:
        dags.save_task(
            dag_name,
            task.name,
            {
                column: getattr(task, field)
                for field, column in _AIRFLOW_TASK_FIELDS.items()
            },
        )


def _time_text(value: time | None) -> str | None:
    return None if value is None else value.strftime("%H:%M:%S")


def _datetime_text(value: datetime | None) -> str | None:
    return None if value is None else value.strftime("%Y-%m-%d %H:%M:%S")


class DBCalls:
    def __init__(self):

        # 		logFormat = '%(asctime)s %(levelname)s - %(message)s'
        # 		logging.basicConfig(format=logFormat, level=logging.INFO)
        # 		logPropagate = True

        self.logger = "gunicorn.error"
        log = logging.getLogger(self.logger)
        log.debug("Executing dbimport.server.rest_server_calls.__init__()")

        self.mysql_conn = None
        self.mysql_cursor = None
        self.debug_log_level = False
        self.config_db_session = None

        if logging.root.level == 10:  # DEBUG
            self.debug_log_level = True

        self.common_config = common_config.Config()
        # self.common_config.disconnect_config_database() # This is needed as we must connect with buffered=False and that is only available during reconnect
        # self.common_config.reconnect_config_database(printReconnectMessage=True, buffered=False)

        self.logdir = get_settings().server.logdir
        # self.admin_user = self.common_config.get_config_value("restserver_admin_user")
        # log.warning(self.admin_user)

        self.admin_user = self.get_config_value("restserver_admin_user")

        self.content_max_rows = 1000

        self.create_default_admin_user()

    def convert_int_to_bool(self, int_value):
        if int_value == 1:
            return True
        else:
            return False

    def disconnect_dbimport_db(self):
        """Disconnects from the database and removes all sessions and engine"""
        log = logging.getLogger(self.logger)

        if self.config_db != None:
            log.info("Disconnecting from DBImport database")
            self.config_db.dispose()
            self.config_db = None

        self.config_db_session = None

    def get_dbimport_session(self):
        """Connects to the configuration database with SQLAlchemy and return a session"""
        log = logging.getLogger(self.logger)

        if self.config_db_session != None:
            # If we already have a connection, we just return the session
            # return self.config_db_session()
            return scoped_session(self.config_db_session)

        try:
            self.config_db = config_engine.create(echo=self.debug_log_level)
            self.config_db.connect().close()
            self.config_db_session = sessionmaker(bind=self.config_db)

        except sa.exc.OperationalError as err:
            logging.error("%s", err)
            self.config_db_session = None
            self.config_db = None
            raise SQLError("Can't connect to DBImport database")

        except:
            print("Unexpected error: ")
            print(sys.exc_info())
            self.config_db_session = None
            self.config_db = None
            raise SQLError("Can't connect to DBImport database")

        else:
            # return self.config_db_session()
            return scoped_session(self.config_db_session)

    def get_config_value(self, key):
        """Returns the value of key in the configuration table, of the type
        config_keys gives it, or None when its value cannot be used. Raises
        UnknownKeyError for a key that is not in config_keys.KEYS."""
        log = logging.getLogger(self.logger)

        try:
            session = self.get_dbimport_session()
        except SQLError:
            self.disconnect_dbimport_db()
            return None

        try:
            return ConfigurationRepository(session()).value(key)
        except UnknownKeyError:
            raise
        except ConfigurationError as error:
            log.error("%s", error)
            return None
        finally:
            session.remove()

    def create_default_admin_user(self):
        """Creates the default admin user in the auth_users table if it doesnt exist"""
        log = logging.getLogger(self.logger)

        try:
            session = self.get_dbimport_session()
        except SQLError:
            self.disconnect_dbimport_db()
            return None

        default_admin_user = self.get_config_value("restserver_admin_user")

        try:
            users = AuthUsersRepository(session())
            if users.user(default_admin_user) is None:
                log.info(
                    "Default admin user does not exists in the 'auth_users' table. Creating it with password 'admin'"
                )
                hashed_password = bcrypt.hashpw(
                    password=b"admin", salt=bcrypt.gensalt()
                ).decode("utf-8")
                users.add(default_admin_user, hashed_password)
                session.commit()
        finally:
            session.remove()

    def create_user(self, user: data_models.User):
        """Creates a user in the auth_users table"""
        log = logging.getLogger(self.logger)

        try:
            session = self.get_dbimport_session()
        except SQLError:
            self.disconnect_dbimport_db()
            return None

        try:
            AuthUsersRepository(session()).add(
                user.username,
                user.password,
                disabled=user.disabled,
                fullname=user.fullname,
                department=user.department,
                email=user.email,
            )
            session.commit()
        finally:
            session.remove()

        log.info("User '%s' created", user.username)

    def get_user(self, username):
        """Returns a dict with user information from the database"""
        try:
            session = self.get_dbimport_session()
        except SQLError:
            self.disconnect_dbimport_db()
            return None

        try:
            stored = AuthUsersRepository(session()).user(username)
        finally:
            session.remove()

        return None if stored is None else stored.model_dump()

    def update_user(self, user, password_changed, current_user):
        """Update a user object"""
        log = logging.getLogger(self.logger)

        try:
            session = self.get_dbimport_session()
        except SQLError:
            self.disconnect_dbimport_db()
            return None

        if password_changed == False:
            log.info(
                "User '%s' updated details for user '%s'",
                current_user,
                user["username"],
            )
        else:
            log.info(
                "User '%s' changed the password for user '%s'",
                current_user,
                user["username"],
            )

        try:
            AuthUsersRepository(session()).update(StoredUser.model_validate(user))
            session.commit()
        finally:
            session.remove()

    def delete_user(self, username):
        """Delete a user"""
        try:
            session = self.get_dbimport_session()
        except SQLError:
            self.disconnect_dbimport_db()
            return None

        try:
            AuthUsersRepository(session()).delete(username)
            session.commit()
        finally:
            session.remove()

    def generate_jdbc_connection_string(
        self, connection_values: data_models.GenerateJDBCConnectionString, current_user
    ):
        """Generates a JDBC connection string"""
        log = logging.getLogger(self.logger)

        database_type = connection_values.databaseType
        database_version = connection_values.version
        hostname = connection_values.hostname
        port = connection_values.port
        database = connection_values.database
        jdbc_connection_string = ""

        # Validate to make sure we have a valid combination of databaseType and version
        correct_driver_version = False
        for driver in self.get_jdbc_drivers():
            if (
                database_type == driver["databaseType"]
                and database_version == driver["version"]
            ):
                correct_driver_version = True

        if not correct_driver_version:
            result = "Not a valid combination of databaseType and version"
            return_code = 404
            return (result, return_code)

        # Generate the connection string for each combo of databaseType and databaseVersion
        if database_type == "DB2 AS400" and database_version == "default":
            if connection_values.port is None:
                jdbc_connection_string = "jdbc:as400://%s/%s" % (hostname, database)
            else:
                jdbc_connection_string = "jdbc:as400://%s:%s/%s" % (
                    hostname,
                    port,
                    database,
                )

        if database_type == "DB2 UDB" and database_version == "default":
            if connection_values.port is None:
                jdbc_connection_string = "jdbc:db2://%s/%s" % (hostname, database)
            else:
                jdbc_connection_string = "jdbc:db2://%s:%s/%s" % (
                    hostname,
                    port,
                    database,
                )

        if database_type == "MySQL" and database_version == "default":
            if connection_values.port is None:
                jdbc_connection_string = "jdbc:mysql://%s/%s" % (hostname, database)
            else:
                jdbc_connection_string = "jdbc:mysql://%s:%s/%s" % (
                    hostname,
                    port,
                    database,
                )

        if database_type == "Oracle" and database_version == "default":
            if connection_values.port is None:
                jdbc_connection_string = (
                    "jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=%s)))(CONNECT_DATA=(SERVICE_NAME=%s)))"
                    % (hostname, database)
                )
            else:
                jdbc_connection_string = (
                    "jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=%s)(PORT=%s)))(CONNECT_DATA=(SERVICE_NAME=%s)))"
                    % (hostname, port, database)
                )

        if database_type == "PostgreSQL" and database_version == "default":
            if connection_values.port is None:
                jdbc_connection_string = "jdbc:postgresql://%s/%s" % (
                    hostname,
                    database,
                )
            else:
                jdbc_connection_string = "jdbc:postgresql://%s:%s/%s" % (
                    hostname,
                    port,
                    database,
                )

        if database_type == "Progress DB" and database_version == "default":
            if connection_values.port is None:
                jdbc_connection_string = "jdbc:datadirect:openedge://%s/%s" % (
                    hostname,
                    database,
                )
            else:
                jdbc_connection_string = "jdbc:datadirect:openedge://%s:%s/%s" % (
                    hostname,
                    port,
                    database,
                )

        if database_type == "SQL Server" and database_version == "default":
            if connection_values.port is None:
                jdbc_connection_string = "jdbc:sqlserver://%s;database=%s" % (
                    hostname,
                    database,
                )
            else:
                jdbc_connection_string = "jdbc:sqlserver://%s:%s;database=%s" % (
                    hostname,
                    port,
                    database,
                )

        if database_type == "SQL Server" and database_version == "jTDS":
            if connection_values.port is None:
                jdbc_connection_string = (
                    "jdbc:jtds:sqlserver://%s;useNTLMv2=true;databaseName=%s"
                    % (hostname, database)
                )
            else:
                jdbc_connection_string = (
                    "jdbc:jtds:sqlserver://%s:%s;useNTLMv2=true;databaseName=%s"
                    % (hostname, port, database)
                )

        if database_type == "CacheDB" and database_version == "default":
            if connection_values.port is None:
                jdbc_connection_string = "jdbc:Cache://%s/%s" % (hostname, database)
            else:
                jdbc_connection_string = "jdbc:Cache://%s:%s/%s" % (
                    hostname,
                    port,
                    database,
                )

        if database_type == "Snowflake" and database_version == "default":
            if connection_values.port is None:
                jdbc_connection_string = "jdbc:snowflake://%s/?db=%s" % (
                    hostname,
                    database,
                )
            else:
                jdbc_connection_string = "jdbc:snowflake://%s:%s/?db=%s" % (
                    hostname,
                    port,
                    database,
                )

        if database_type == "Informix" and database_version == "default":
            if connection_values.port is None:
                jdbc_connection_string = "jdbc:informix-sqli://%s/%s" % (
                    hostname,
                    database,
                )
            else:
                jdbc_connection_string = "jdbc:informix-sqli://%s:%s/%s" % (
                    hostname,
                    port,
                    database,
                )

        if database_type == "SQL Anywhere" and database_version == "default":
            if connection_values.port is None:
                jdbc_connection_string = "jdbc:sybase:Tds://%s/%s" % (
                    hostname,
                    database,
                )
            else:
                jdbc_connection_string = "jdbc:sybase:Tds://%s:%s/%s" % (
                    hostname,
                    port,
                    database,
                )

        result_msg = {"connectionString": jdbc_connection_string}

        return_code = 200
        return (result_msg, return_code)

    def get_jdbc_drivers(self):
        """Returns all JDBC Driver configuration"""
        log = logging.getLogger(self.logger)

        try:
            session = self.get_dbimport_session()
        except SQLError:
            self.disconnect_dbimport_db()
            return None

        drivers = ConnectionsRepository(session()).drivers()

        list_of_jdbc_drivers = [
            {
                "databaseType": driver.database_type,
                "version": driver.version,
                "driver": driver.driver,
                "classpath": driver.classpath,
            }
            for driver in drivers
        ]

        session.remove()

        return list_of_jdbc_drivers

    def update_jdbc_driver(self, jdbc_driver: data_models.JDBCDriver, current_user):
        """Stores driver and classpath of an existing JDBC driver. Returns the
        message and the HTTP status: 400 for an unknown type and version."""
        log = logging.getLogger(self.logger)

        try:
            session = self.get_dbimport_session()
        except SQLError:
            self.disconnect_dbimport_db()
            return None

        log.info(
            "User '%s' updated JDBC driver configuration for '%s'",
            current_user,
            jdbc_driver.databaseType,
        )

        log.debug("databaseType: %s" % jdbc_driver.databaseType)
        log.debug("version: %s" % jdbc_driver.version)
        log.debug("driver: %s" % jdbc_driver.driver)
        log.debug("classpath: %s" % jdbc_driver.classpath)

        try:
            updated = ConnectionsRepository(session()).update_driver(
                JDBCDriver(
                    database_type=jdbc_driver.databaseType,
                    version=jdbc_driver.version,
                    driver=jdbc_driver.driver,
                    classpath=jdbc_driver.classpath,
                )
            )
            if updated:
                session.commit()
        finally:
            session.remove()

        if not updated:
            return (
                "JDBC Driver databaseType with version is not a supported combination",
                400,
            )
        return ("ok", 200)

    def get_configuration(self, current_user):
        """Returns all configuration items from the configuration table"""
        log = logging.getLogger(self.logger)

        try:
            session = self.get_dbimport_session()
        except SQLError:
            self.disconnect_dbimport_db()
            return None

        stored = ConfigurationRepository(session()).stored_values()
        session.remove()

        result_dict = {}
        for key, value in stored.items():
            config_key = config_keys.BY_NAME[key]
            if not config_key.rest:
                continue
            if key == "restserver_admin_user":
                self.admin_user = value
            if config_key.admin_only and current_user != self.admin_user:
                continue
            result_dict[key] = value

        json_result = json.loads(json.dumps(result_dict))
        return json_result

    def update_configuration(
        self, configuration: data_models.Configuration, current_user
    ):
        """Update configuration items in the configuration table"""
        log = logging.getLogger(self.logger)

        try:
            session = self.get_dbimport_session()
        except SQLError:
            self.disconnect_dbimport_db()
            return None

        # First check to make sure we have valid options in the changed options
        try:
            for key in data_models.Configuration.model_fields:
                if type(getattr(configuration, key)) in (str, bool, int):
                    # At this point, only configuration items that exists in the class is iterrated
                    if type(getattr(configuration, key)) is str:
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

                    if (
                        config_keys.BY_NAME[key].admin_only
                        and current_user != self.admin_user
                    ):
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

        changes = {
            key: getattr(configuration, key)
            for key in data_models.Configuration.model_fields
            if type(getattr(configuration, key)) in (str, bool, int)
        }
        ConfigurationRepository(session()).set_values(changes)
        session.commit()
        for key, value in changes.items():
            log.info(
                "User '%s' updated global configuration. %s = %s",
                current_user,
                key,
                value,
            )

        result = "ok"
        return_code = 200
        result_dict = {}
        result_dict["status"] = "ok"
        # session.close()
        session.remove()

        json_result = json.loads(json.dumps(result_dict))
        # return jsonResult
        return (result, return_code)

    def get_jdbc_server_type(self, name, connection_string):
        """Return the server type based on the connection string"""
        try:
            self.common_config.lookup_connection_alias(
                name, decrypt_credentials=False, jdbc_url=connection_string, quiet=True
            )
        except InvalidConfigurationError:
            pass

        server_type = None
        if self.common_config.jdbc_servertype == constant.MYSQL:
            server_type = "MySQL"
        if self.common_config.jdbc_servertype == constant.ORACLE:
            server_type = "Oracle"
        if self.common_config.jdbc_servertype == constant.MSSQL:
            server_type = "MSSQL Server"
        if self.common_config.jdbc_servertype == constant.POSTGRESQL:
            server_type = "PostgreSQL"
        if self.common_config.jdbc_servertype == constant.PROGRESS:
            server_type = "Progress"
        if self.common_config.jdbc_servertype == constant.DB2_UDB:
            server_type = "DB2 UDB"
        if self.common_config.jdbc_servertype == constant.DB2_AS400:
            server_type = "DB2 AS400"
        if self.common_config.jdbc_servertype == constant.MONGO:
            server_type = "MongoDB"
        if self.common_config.jdbc_servertype == constant.CACHEDB:
            server_type = "Cache"
        if self.common_config.jdbc_servertype == constant.SNOWFLAKE:
            server_type = "Snowflake"
        if self.common_config.jdbc_servertype == constant.AWS_S3:
            server_type = "AWS S3"
        if self.common_config.jdbc_servertype == constant.INFORMIX:
            server_type = "Informix"
        if self.common_config.jdbc_servertype == constant.SQLANYWHERE:
            server_type = "SQL Anywhere"

        return server_type

    def get_all_connections(self, list_only_name):
        """Returns all Connections"""
        try:
            session = self.get_dbimport_session()
        except SQLError:
            self.disconnect_dbimport_db()
            return None

        try:
            connections = ConnectionsRepository(session()).aliases_and_urls(
                self.content_max_rows
            )
        finally:
            session.remove()

        list_of_connections = []
        for dbalias, jdbc_url in connections:
            result_dict = {"name": dbalias}
            if not list_only_name:
                result_dict["connectionString"] = jdbc_url
                result_dict["serverType"] = self.get_jdbc_server_type(
                    name=dbalias, connection_string=jdbc_url
                )
            list_of_connections.append(result_dict)

        headers = {
            "content-rows": str(len(list_of_connections)),
            "content-max-returned-rows": str(self.content_max_rows),
        }
        return JSONResponse(
            content=list_of_connections, headers=headers, status_code=200
        )

    def search_connections(
        self, search_values: data_models.ConnectionSearch, current_user
    ):
        """Search for import tables"""
        log = logging.getLogger(self.logger)

        try:
            session = self.get_dbimport_session()
        except SQLError:
            self.disconnect_dbimport_db()
            return None

        attribute_to_field = {
            "name": "dbalias",
            "connectionString": "jdbc_url",
            "timeWindowTimezone": "timewindow_timezone",
            "operatorNotes": "operator_notes",
            "contactInformation": "contact_info",
            "description": "description",
            "owner": "owner",
        }

        filters = {}
        for attribute, value in search_values:
            if value is None or value in ("", "*", "%"):
                # If it's None, empty or search *, it means we dont need to search on that value. So lets skip it
                continue
            value = value.replace("*", "%")
            log.debug("search filter on %s", attribute)
            filters[attribute_to_field[attribute]] = value

        try:
            connections, total = ConnectionsRepository(session()).search(
                filters, self.content_max_rows
            )
        finally:
            session.remove()

        list_of_connections = [
            {
                "name": dbalias,
                "connectionString": jdbc_url,
                "serverType": self.get_jdbc_server_type(
                    name=dbalias, connection_string=jdbc_url
                ),
            }
            for dbalias, jdbc_url in connections
        ]

        headers = {
            "content-rows": str(len(list_of_connections)),
            "content-max-returned-rows": str(self.content_max_rows),
            "content-total-rows": str(total),
        }
        return JSONResponse(
            content=list_of_connections, headers=headers, status_code=200
        )

    def update_connection(
        self, connection: data_models.ConnectionDetails, current_user
    ):
        """Update or create a Connections"""
        log = logging.getLogger(self.logger)

        values = {
            column: getattr(connection, field)
            for column, field in _CONNECTION_FIELDS.items()
        }
        for column, default in _CONNECTION_DEFAULTS.items():
            if values[column] is None:
                values[column] = default
        try:
            stored = JDBCConnection.model_validate(values)
        except ValidationError as err:
            fields = ", ".join(
                _CONNECTION_FIELDS[str(error["loc"][0])] for error in err.errors()
            )
            return (f"Invalid value for {fields}", 400)

        try:
            session = self.get_dbimport_session()
        except SQLError:
            self.disconnect_dbimport_db()
            return None

        log.info(
            "User '%s' updated/created connection '%s'", current_user, connection.name
        )

        try:
            ConnectionsRepository(session()).save(stored)
            session.commit()
        finally:
            session.remove()

        return ("ok", 200)

    def get_connection(self, connection):
        """Returns all Connections"""
        try:
            session = self.get_dbimport_session()
        except SQLError:
            self.disconnect_dbimport_db()
            return None

        try:
            stored = ConnectionsRepository(session()).connection(connection)
        finally:
            session.remove()

        if stored is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="connection does not exist",
            )

        return {
            "name": stored.dbalias,
            "connectionString": stored.jdbc_url,
            "privateKeyPath": stored.private_key_path,
            "publicKeyPath": stored.public_key_path,
            "credentials": stored.credentials,
            "source": stored.datalake_source,
            "forceString": (
                stored.force_string if stored.force_string in (-1, 0, 1) else -1
            ),
            "maxSessions": stored.max_import_sessions,
            "createDatalakeImport": stored.create_datalake_import,
            "timeWindowStart": _time_text(stored.timewindow_start),
            "timeWindowStop": _time_text(stored.timewindow_stop),
            "timeWindowTimezone": stored.timewindow_timezone,
            "operatorNotes": stored.operator_notes,
            "contactInformation": stored.contact_info,
            "description": stored.description,
            "owner": stored.owner,
            "environment": stored.environment,
            "seedFile": stored.seed_file,
            "createForeignKey": stored.create_foreign_keys,
            "atlasDiscovery": stored.atlas_discovery,
            "atlasIncludeFilter": stored.atlas_include_filter,
            "atlasExcludeFilter": stored.atlas_exclude_filter,
            "atlasLastDiscovery": (
                None
                if stored.atlas_last_discovery is None
                else stored.atlas_last_discovery.strftime("%Y-%m-%d %H:%M:%S")
            ),
        }

    def delete_connection(self, connection, current_user):
        """Delete a connection"""
        log = logging.getLogger(self.logger)

        try:
            session = self.get_dbimport_session()
        except SQLError:
            self.disconnect_dbimport_db()
            return None

        log.info("User '%s' deleted connection '%s'", current_user, connection)

        try:
            deleted = ConnectionsRepository(session()).delete(connection)
            session.commit()
        finally:
            session.remove()

        if not deleted:
            return ("Connection does not exist", 404)
        return ("ok", 200)

    def encrypted_credentials(
        self, credentials: data_models.EncryptCredential, current_user
    ):
        """Encrypt username and password with private key and store in connection"""
        log = logging.getLogger(self.logger)

        manage_cmd = f"{os.environ['DBIMPORT_HOME']}/bin/manage"

        json_input = {}
        json_input["connection"] = credentials.connection
        json_input["username"] = credentials.username
        json_input["password"] = credentials.password

        process_killed = False
        manage_session = subprocess.Popen(
            [manage_cmd, "--encryptCredentials", "--jsonInput", "--quiet"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        try:
            stdout, stderr = manage_session.communicate(
                json.dumps(json_input).encode(), timeout=10
            )
        except subprocess.TimeoutExpired:
            log.error("Killing 'manage --encryptCredentials' process")
            manage_session.kill()
            stdout, stderr = manage_session.communicate()
            process_killed = True

        if manage_session.returncode != 0 or process_killed:
            return_code = 500
            result_msg = {
                "result": re.sub("\n$", "", re.sub("^json: ", "", stdout.decode()))
            }
        else:
            return_code = 200
            result_msg = {"result": "Credentials updated successfully"}

        return (result_msg, return_code)

    def test_connection(self, connection, current_user):
        """Make a test connection to a JDBC source and return result"""
        log = logging.getLogger(self.logger)

        manage_cmd = f"{os.environ['DBIMPORT_HOME']}/bin/manage"

        result = subprocess.run(
            [manage_cmd, "--testConnection", "-a", connection, "--quiet"],
            stdout=subprocess.PIPE,
            check=False,
        )
        if result.returncode != 0:
            return_code = 500
        else:
            return_code = 200

        result_msg = {"result": re.sub("\n$", "", result.stdout.decode())}

        return (result_msg, return_code)

    def get_all_import_databases(self):
        """Returns all databases that have imports configured in them"""
        try:
            session = self.get_dbimport_session()
        except SQLError:
            self.disconnect_dbimport_db()
            return None

        try:
            summaries = ImportTablesRepository(session()).database_summaries(
                self.content_max_rows
            )
        finally:
            session.remove()

        list_of_dbs = [
            {
                "name": summary.hive_db,
                "tables": summary.tables,
                "lastImport": _datetime_text(summary.last_import) or "",
                "lastSize": summary.last_size or 0,
                "lastRows": summary.last_rows or 0,
            }
            for summary in summaries
        ]

        headers = {
            "content-rows": str(len(list_of_dbs)),
            "content-max-returned-rows": str(self.content_max_rows),
        }
        return JSONResponse(content=list_of_dbs, headers=headers, status_code=200)

    def discover_import_tables(
        self, data: data_models.DiscoverImportTableOptions, current_user
    ):
        """Search for import tables"""
        log = logging.getLogger(self.logger)

        manage_cmd = f"{os.environ['DBIMPORT_HOME']}/bin/manage"

        command_arg = []
        command_arg.append(manage_cmd)
        command_arg.append("--addImportTable")
        command_arg.append("-a")
        command_arg.append(data.connection)
        command_arg.append("-h")
        command_arg.append(data.database)

        if data.schemaFilter != None and data.schemaFilter.strip() != "":
            command_arg.append("-S")
            command_arg.append(data.schemaFilter)

        if data.tableFilter != None and data.tableFilter.strip() != "":
            command_arg.append("-T")
            command_arg.append(data.tableFilter)

        if data.addSchemaToTable != None and data.addSchemaToTable == True:
            command_arg.append("--addSchemaToTable")

        if data.addCounterToTable != None and data.addCounterToTable == True:
            command_arg.append("--addCounterToTable")

            if data.counterStart != None:
                command_arg.append("--counterStart")
                command_arg.append(str(data.counterStart))

        if data.addCustomText != None and data.addCustomText.strip() != "":
            command_arg.append("--addCustomText")
            command_arg.append(data.addCustomText)

        command_arg.append("--quiet")
        command_arg.append("--jsonOutput")

        log.debug(command_arg)

        result = subprocess.run(command_arg, stdout=subprocess.PIPE, check=False)

        if result.returncode != 0:
            raise HTTPException(status_code=500, detail=result.stdout.decode())

        else:
            return_code = 200
            if (
                result.stdout.decode().strip()
                == "There are no tables in the source database that we dont already have in DBImport"
            ):
                return_msg = []
            else:
                return_msg = json.loads(result.stdout.decode().replace("\n", ""))

        return (return_msg, return_code)

    def add_discovered_import_tables(
        self, data: list[data_models.DiscoverImportTable], current_user
    ):
        """Add discovered import tables"""
        log = logging.getLogger(self.logger)

        create_counter = 0
        for record in data:
            create_counter = create_counter + 1
            database = record.database
            table = record.table

            create_data = data_models.ImportTableDetailsWrite(
                connection=record.connection,
                sourceSchema=record.sourceSchema,
                sourceTable=record.sourceTable,
                importPhaseType="full",
                etlPhaseType="truncate_insert",
                importTool="spark",
                etlEngine="spark",
            )

            self.update_import_table(database, table, create_data, current_user)

        return_code = 200
        result_msg = {"result": f"{create_counter} tables created"}

        return (result_msg, return_code)

    def search_import_tables(
        self, search_values: data_models.ImportTableSearch, current_user
    ):
        """Search for import tables"""
        log = logging.getLogger(self.logger)

        try:
            session = self.get_dbimport_session()
        except SQLError:
            self.disconnect_dbimport_db()
            return None

        filters = {}
        for attribute, value in search_values:
            if value is None or value in ("", "*", "%"):
                # If it's None, empty or search *, it means we dont need to search on that value. So lets skip it
                continue
            if isinstance(value, str):
                value = value.replace("*", "%")
            log.debug("search filter on %s", attribute)
            filters[_IMPORT_TABLE_SEARCH[attribute]] = value

        try:
            tables, total = ImportTablesRepository(session()).search(
                filters, self.content_max_rows
            )
        finally:
            session.remove()

        list_of_tables = [
            {
                "database": stored.hive_db,
                "table": stored.hive_table,
                "connection": stored.dbalias,
                "sourceSchema": stored.source_schema,
                "sourceTable": stored.source_table,
                "importPhaseType": stored.import_phase_type,
                "etlPhaseType": stored.etl_phase_type,
                "importTool": stored.import_tool,
                "etlEngine": stored.etl_engine,
                "lastUpdateFromSource": _datetime_text(stored.last_update_from_source),
                "includeInAirflow": stored.include_in_airflow == 1,
                "operatorNotes": stored.operator_notes,
            }
            for stored in tables
        ]

        headers = {
            "content-rows": str(len(list_of_tables)),
            "content-max-returned-rows": str(self.content_max_rows),
            "content-total-rows": str(total),
        }
        return JSONResponse(content=list_of_tables, headers=headers, status_code=200)

    def get_import_tables_on_connection(self, connection, current_user):
        """Returns all import tables in a specific database"""
        log = logging.getLogger(self.logger)

        search_values = data_models.ImportTableSearch(
            database="*", table="*", connection=connection
        )

        return self.search_import_tables(search_values, current_user)

    def get_import_tables_in_database(self, database, current_user):
        """Returns all import tables in a specific database"""
        log = logging.getLogger(self.logger)

        search_values = data_models.ImportTableSearch(database=database, table="*")

        return self.search_import_tables(search_values, current_user)

    def get_import_table_details(self, database, table, include_columns):
        """Returns all import table details"""
        try:
            session = self.get_dbimport_session()
        except SQLError:
            self.disconnect_dbimport_db()
            return None

        try:
            stored = ImportTablesRepository(session()).table(database, table)
        finally:
            session.remove()

        if stored is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="import table does not exist",
            )

        result_dict = {
            "database": stored.hive_db,
            "table": stored.hive_table,
            "connection": stored.dbalias,
            "sourceSchema": stored.source_schema,
            "sourceTable": stored.source_table,
            "importPhaseType": stored.import_phase_type,
            "etlPhaseType": stored.etl_phase_type,
            "importTool": stored.import_tool,
            "etlEngine": stored.etl_engine,
            "lastUpdateFromSource": _datetime_text(stored.last_update_from_source),
            "sqlWhereAddition": stored.sqoop_sql_where_addition,
            "nomergeIngestionSqlAddition": stored.nomerge_ingestion_sql_addition,
            "includeInAirflow": stored.include_in_airflow == 1,
            "airflowPriority": stored.airflow_priority,
            "validateImport": stored.validate_import,
            "validationMethod": stored.validation_method,
            "validateSource": stored.validate_source,
            "validateDiffAllowed": stored.validate_diff_allowed,
            "validationCustomQuerySourceSQL": stored.validation_custom_query_source_sql,
            "validationCustomQueryHiveSQL": stored.validation_custom_query_hive_sql,
            "validationCustomQueryValidateImportTable": (
                stored.validation_custom_query_validate_import_table
            ),
            "sqlSessions": stored.mappers,
            "softDeleteDuringMerge": stored.soft_delete_during_merge,
            "sourceRowcount": stored.source_rowcount,
            "sourceRowcountIncr": stored.source_rowcount_incr,
            "targetRowcount": stored.hive_rowcount,
            "validationCustomQuerySourceValue": (
                stored.validation_custom_query_source_value
            ),
            "validationCustomQueryHiveValue": stored.validation_custom_query_hive_value,
            "incrMode": stored.incr_mode or "lastmodified",
            "incrColumn": stored.incr_column,
            "incrValidationMethod": stored.incr_validation_method,
            "incrMinvalue": stored.incr_minvalue,
            "incrMaxvalue": stored.incr_maxvalue,
            "incrMinvaluePending": stored.incr_minvalue_pending,
            "incrMaxvaluePending": stored.incr_maxvalue_pending,
            "pkColumnOverride": stored.pk_column_override,
            "pkColumnOverrideMergeonly": stored.pk_column_override_mergeonly,
            "hiveContainerSize": stored.hive_merge_heap,
            "splitCount": stored.hive_split_count,
            "sparkExecutorMemory": stored.spark_executor_memory,
            "sparkExecutors": stored.spark_executors,
            "splitByColumn": stored.split_by_column,
            "sqoopCustomQuery": stored.sqoop_query,
            "sqoopOptions": stored.sqoop_options,
            "lastSize": stored.sqoop_last_size,
            "lastRows": stored.sqoop_last_rows,
            "lastSqlSessions": stored.sqoop_last_mappers,
            "lastExecution": stored.sqoop_last_execution,
            # -1 is not allowed, but default in the database.
            "sqoopUseGeneratedSql": (
                1
                if stored.sqoop_use_generated_sql == -1
                else stored.sqoop_use_generated_sql
            ),
            "allowTextSplitter": stored.sqoop_allow_text_splitter,
            "forceString": (
                stored.force_string if stored.force_string in (-1, 0, 1) else -1
            ),
            "comment": stored.comment,
            "generatedHiveColumnDefinition": stored.generated_hive_column_definition,
            "generatedSqoopQuery": stored.generated_sqoop_query,
            "generatedSqoopOptions": stored.generated_sqoop_options,
            "generatedPkColumns": stored.generated_pk_columns,
            "datalakeSource": stored.datalake_source,
            "operatorNotes": stored.operator_notes,
            "copyFinished": _datetime_text(stored.copy_finished),
            "copySlave": stored.copy_slave,
            "createForeignKeys": stored.create_foreign_keys,
            "invalidateImpala": stored.invalidate_impala,
            "customMaxQuery": stored.custom_max_query,
            "mergeCompactionMethod": stored.merge_compaction_method,
            "sourceTableType": stored.source_table_type,
            "importDatabase": stored.import_database,
            "importTable": stored.import_table,
            "historyDatabase": stored.history_database,
            "historyTable": stored.history_table,
        }

        if include_columns == True:
            result_dict["columns"] = self.get_import_table_columns(database, table)

        return result_dict

    def delete_import_table(self, database, table, current_user):
        """Delete an import table"""
        log = logging.getLogger(self.logger)

        try:
            session = self.get_dbimport_session()
        except SQLError:
            self.disconnect_dbimport_db()
            return None

        log.info(
            "User '%s' deleted import table '%s.%s'", current_user, database, table
        )

        try:
            deleted = ImportTablesRepository(session()).delete(database, table)
            session.commit()
        finally:
            session.remove()

        if not deleted:
            return ("Table does not exist", 404)
        return ("ok", 200)

    def bulk_delete_import_table(
        self, bulk_data: list[data_models.ImportTablePK], current_user
    ):
        """Bulk delete of import tables"""
        if len(bulk_data) == 0:
            result_msg = {
                "result": "Body must contain a list of databas and table to delete"
            }
            return_code = 422
            return (result_msg, return_code)

        try:
            session = self.get_dbimport_session()
        except SQLError:
            self.disconnect_dbimport_db()
            return None

        try:
            ImportTablesRepository(session()).delete_many(
                [(tables.database, tables.table) for tables in bulk_data]
            )
            session.commit()
        finally:
            session.remove()

        return ({"result": "Ok"}, 200)

    def bulk_update_import_table(
        self, bulk_data: data_models.ImportTableBulkUpdate, current_user
    ):
        """Bulk update of import tables"""
        if len(bulk_data.importTables) == 0:
            result_msg = {
                "result": "importTables must contain a list of database/tables to update"
            }
            return_code = 422
            return (result_msg, return_code)

        try:
            session = self.get_dbimport_session()
        except SQLError:
            self.disconnect_dbimport_db()
            return None

        settings = {
            _IMPORT_TABLE_BULK[field]: getattr(bulk_data, field)
            for field in bulk_data.__pydantic_fields_set__
            if field in _IMPORT_TABLE_BULK
        }

        try:
            ImportTablesRepository(session()).update_many(
                [(tables.database, tables.table) for tables in bulk_data.importTables],
                settings,
            )
            session.commit()
        finally:
            session.remove()

        return ({"result": "Ok"}, 200)

    def update_import_table(
        self, database, table, data: data_models.ImportTableDetailsWrite, current_user
    ):
        """Update or create an import table"""
        log = logging.getLogger(self.logger)

        try:
            session = self.get_dbimport_session()
        except SQLError:
            self.disconnect_dbimport_db()
            return None

        log.info(
            "User '%s' updated/created import table '%s.%s'",
            current_user,
            database,
            table,
        )

        settings = {
            column: getattr(data, field)
            for column, field in _IMPORT_TABLE_FIELDS.items()
        }
        for column, default in _IMPORT_TABLE_DEFAULTS.items():
            if settings[column] is None:
                settings[column] = default

        columns = []
        for column in data.columns:
            values = {
                name: getattr(column, field)
                for name, field in _IMPORT_COLUMN_FIELDS.items()
            }
            # Force set to Null if string is empty. This will otherwise cause problems during executions as it's not handled there
            for name in ("column_name_override", "column_type_override"):
                override = values[name]
                if isinstance(override, str) and override.strip() == "":
                    values[name] = None
            columns.append(values | {"hive_db": database, "hive_table": table})

        try:
            tables = ImportTablesRepository(session())
            tables.save_settings(database, table, settings)
            table_id = tables.table_id_of(database, table)
            stored_columns = ImportColumnsRepository(session())
            for values in columns:
                stored_columns.save_column(table_id, values)
            session.commit()
        finally:
            session.remove()

        return ("ok", 200)

    def get_import_table_columns(self, database, table):
        """Returns all columns in an import table"""
        try:
            session = self.get_dbimport_session()
        except SQLError:
            self.disconnect_dbimport_db()
            return None

        try:
            columns = ImportColumnsRepository(session()).columns_of_table(
                database, table
            )
        finally:
            session.remove()

        return [
            {
                "columnName": column.column_name,
                "columnOrder": column.column_order,
                "sourceColumnName": column.source_column_name,
                "columnType": column.column_type,
                "sourceColumnType": column.source_column_type,
                "sourceDatabaseType": column.source_database_type,
                "columnNameOverride": column.column_name_override,
                "columnTypeOverride": column.column_type_override,
                "sqoopColumnType": column.sqoop_column_type,
                "sqoopColumnTypeOverride": column.sqoop_column_type_override,
                "forceString": (
                    column.force_string if column.force_string in (-1, 0, 1) else -1
                ),
                "includeInImport": column.include_in_import,
                "sourcePrimaryKey": column.source_primary_key,
                "lastUpdateFromSource": _datetime_text(column.last_update_from_source),
                "comment": column.comment,
                "operatorNotes": column.operator_notes,
                "anonymizationFunction": column.anonymization_function,
            }
            for column in columns
        ]

    def repair_import_table(self, database, table, current_user):
        """Repair Import table"""
        log = logging.getLogger(self.logger)

        manage_cmd = f"{os.environ['DBIMPORT_HOME']}/bin/manage"

        command_arg = []
        command_arg.append(manage_cmd)
        command_arg.append("--repairIncrementalImport")
        command_arg.append("--yes")
        command_arg.append("-h")
        command_arg.append(database)
        command_arg.append("-t")
        command_arg.append(table)
        command_arg.append("--quiet")

        process_killed = False
        manage_session = subprocess.Popen(
            command_arg,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        try:
            stdout, stderr = manage_session.communicate(timeout=30)
        except subprocess.TimeoutExpired:
            log.error("Killing 'manage --repairIncrementalImport' process")
            manage_session.kill()
            stdout, stderr = manage_session.communicate()
            process_killed = True

        if manage_session.returncode != 0 or process_killed:
            return_code = 500
            result_msg = {
                "result": re.sub("\n$", "", re.sub("^json: ", "", stdout.decode()))
            }
        else:
            return_code = 200
            result_msg = {"result": "Table repair was successfully"}

        return (result_msg, return_code)

    def reset_import_table(self, database, table, current_user):
        """Reset Import table"""
        log = logging.getLogger(self.logger)

        manage_cmd = f"{os.environ['DBIMPORT_HOME']}/bin/manage"

        command_arg = []
        command_arg.append(manage_cmd)
        command_arg.append("--resetIncrementalImport")
        command_arg.append("--yes")
        command_arg.append("-h")
        command_arg.append(database)
        command_arg.append("-t")
        command_arg.append(table)
        command_arg.append("--quiet")

        process_killed = False
        manage_session = subprocess.Popen(
            command_arg,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        try:
            stdout, stderr = manage_session.communicate(timeout=30)
        except subprocess.TimeoutExpired:
            log.error("Killing 'manage --resetIncrementalImport' process")
            manage_session.kill()
            stdout, stderr = manage_session.communicate()
            process_killed = True

        if manage_session.returncode != 0 or process_killed:
            return_code = 500
            result_msg = {
                "result": re.sub("\n$", "", re.sub("^json: ", "", stdout.decode()))
            }
        else:
            return_code = 200
            result_msg = {"result": "Table reset was successfully"}

        return (result_msg, return_code)

    def repair_export_table(self, connection, schema, table, current_user):
        """Repair Import table"""
        log = logging.getLogger(self.logger)

        manage_cmd = f"{os.environ['DBIMPORT_HOME']}/bin/manage"

        command_arg = []
        command_arg.append(manage_cmd)
        command_arg.append("--repairIncrementalExport")
        command_arg.append("--yes")
        command_arg.append("-a")
        command_arg.append(connection)
        command_arg.append("-S")
        command_arg.append(schema)
        command_arg.append("-T")
        command_arg.append(table)
        command_arg.append("--quiet")

        process_killed = False
        manage_session = subprocess.Popen(
            command_arg,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        try:
            stdout, stderr = manage_session.communicate(timeout=30)
        except subprocess.TimeoutExpired:
            log.error("Killing 'manage --repairIncrementalExport' process")
            manage_session.kill()
            stdout, stderr = manage_session.communicate()
            process_killed = True

        if manage_session.returncode != 0 or process_killed:
            return_code = 500
            result_msg = {
                "result": re.sub("\n$", "", re.sub("^json: ", "", stdout.decode()))
            }
        else:
            return_code = 200
            result_msg = {"result": "Table repair was successfully"}

        return (result_msg, return_code)

    def reset_export_table(self, connection, schema, table, current_user):
        """Repair Import table"""
        log = logging.getLogger(self.logger)

        manage_cmd = f"{os.environ['DBIMPORT_HOME']}/bin/manage"

        command_arg = []
        command_arg.append(manage_cmd)
        command_arg.append("--resetIncrementalExport")
        command_arg.append("--yes")
        command_arg.append("-a")
        command_arg.append(connection)
        command_arg.append("-S")
        command_arg.append(schema)
        command_arg.append("-T")
        command_arg.append(table)
        command_arg.append("--quiet")

        process_killed = False
        manage_session = subprocess.Popen(
            command_arg,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        try:
            stdout, stderr = manage_session.communicate(timeout=30)
        except subprocess.TimeoutExpired:
            log.error("Killing 'manage --resetIncrementalExport' process")
            manage_session.kill()
            stdout, stderr = manage_session.communicate()
            process_killed = True

        if manage_session.returncode != 0 or process_killed:
            return_code = 500
            result_msg = {
                "result": re.sub("\n$", "", re.sub("^json: ", "", stdout.decode()))
            }
        else:
            return_code = 200
            result_msg = {"result": "Table reset was successfully"}

        return (result_msg, return_code)

    def get_export_connections(self):
        """Returns all connections that have exports configured in them"""
        try:
            session = self.get_dbimport_session()
        except SQLError:
            self.disconnect_dbimport_db()
            return None

        try:
            summaries = ExportTablesRepository(session()).connection_summaries(
                self.content_max_rows
            )
        finally:
            session.remove()

        list_of_connections = [
            {
                "name": summary.dbalias,
                "tables": summary.tables,
                "lastExport": _datetime_text(summary.last_export) or "",
                "lastRows": summary.last_rows or 0,
            }
            for summary in summaries
        ]

        headers = {
            "content-rows": str(len(list_of_connections)),
            "content-max-returned-rows": str(self.content_max_rows),
        }
        return JSONResponse(
            content=list_of_connections, headers=headers, status_code=200
        )

    def discover_export_tables(
        self, data: data_models.DiscoverExportTableOptions, current_user
    ):
        """Search for import tables"""
        log = logging.getLogger(self.logger)

        # Due to incomplete development environments right now, this function returns a hardcoded json as it's not possible to generate a correct one
        return_list = []
        return_dict1 = {}
        return_dict2 = {}
        return_dict3 = {}
        return_dict1["database"] = "hardcoded"
        return_dict1["table"] = "table1"
        return_dict1["connection"] = "dbimport"
        return_dict1["targetSchema"] = "dbo"
        return_dict1["targetTable"] = "table1"
        return_dict2["database"] = "hardcoded"
        return_dict2["table"] = "table2"
        return_dict2["connection"] = "dbimport"
        return_dict2["targetSchema"] = "dbo"
        return_dict2["targetTable"] = "table2"
        return_dict3["database"] = "hardcoded"
        return_dict3["table"] = "table3"
        return_dict3["connection"] = "dbimport"
        return_dict3["targetSchema"] = "dbo"
        return_dict3["targetTable"] = "table3"
        return_list.append(return_dict1)
        return_list.append(return_dict2)
        return_list.append(return_dict3)

        return_msg = json.loads(json.dumps(return_list))
        return_code = 200
        return (return_msg, return_code)

        manage_cmd = "%s/bin/manage" % (os.environ["DBIMPORT_HOME"])

        command_arg = []
        command_arg.append(manage_cmd)
        command_arg.append("--addExportTable")
        command_arg.append("-a")
        command_arg.append(data.connection)
        command_arg.append("-S")
        command_arg.append(data.targetSchema)

        if data.databaseFilter != None and data.databaseFilter.strip() != "":
            command_arg.append("-h")
            command_arg.append(data.databaseFilter)

        if data.tableFilter != None and data.tableFilter.strip() != "":
            command_arg.append("-t")
            command_arg.append(data.tableFilter)

        if data.addDBToTable != None and data.addDBToTable == True:
            command_arg.append("--addDBToTable")

        if data.addCounterToTable != None and data.addCounterToTable == True:
            command_arg.append("--addCounterToTable")

            if data.counterStart != None:
                command_arg.append("--counterStart")
                command_arg.append(str(data.counterStart))

        if data.addCustomText != None and data.addCustomText.strip() != "":
            command_arg.append("--addCustomText")
            command_arg.append(data.addCustomText)

        command_arg.append("--quiet")
        command_arg.append("--jsonOutput")

        log.debug(command_arg)

        result = subprocess.run(command_arg, stdout=subprocess.PIPE, check=False)

        if result.returncode != 0:
            raise HTTPException(status_code=500, detail=result.stdout.decode())

        else:
            return_code = 200
            if (
                result.stdout.decode().strip()
                == "There are no tables in the source database that we dont already have in DBImport"
            ):
                return_msg = []
            else:
                return_msg = json.loads(result.stdout.decode().replace("\n", ""))

        return (return_msg, return_code)

    def add_discovered_export_tables(
        self, data: list[data_models.DiscoverExportTable], current_user
    ):
        """Add discovered export tables"""
        log = logging.getLogger(self.logger)

        create_counter = 0
        for record in data:
            create_counter = create_counter + 1
            connection = record.connection
            target_schema = record.targetSchema
            target_table = record.targetTable

            create_data = data_models.ExportTableDetailsWrite(
                database=record.database,
                table=record.table,
                exportType="full",
                exportTool="spark",
            )

            self.update_export_table(
                connection, target_schema, target_table, create_data, current_user
            )

        return_code = 200
        result_msg = {"result": f"{create_counter} tables created"}

        return (result_msg, return_code)

    def search_export_tables(
        self, search_values: data_models.ExportTableSearch, current_user
    ):
        """Search for export tables"""
        log = logging.getLogger(self.logger)

        try:
            session = self.get_dbimport_session()
        except SQLError:
            self.disconnect_dbimport_db()
            return None

        filters = {}
        for attribute, value in search_values:
            if value is None or value in ("", "*", "%"):
                # If it's None, empty or search *, it means we dont need to search on that value. So lets skip it
                continue
            if isinstance(value, str):
                value = value.replace("*", "%")
            log.debug("search filter on %s", attribute)
            filters[_EXPORT_TABLE_SEARCH[attribute]] = value

        try:
            tables, total = ExportTablesRepository(session()).search(
                filters, self.content_max_rows
            )
        finally:
            session.remove()

        list_of_tables = [
            {
                "connection": stored.dbalias,
                "targetSchema": stored.target_schema,
                "targetTable": stored.target_table,
                "database": stored.hive_db,
                "table": stored.hive_table,
                "exportType": stored.export_type,
                "exportTool": stored.export_tool,
                "lastUpdateFromHive": _datetime_text(stored.last_update_from_hive),
                "includeInAirflow": stored.include_in_airflow == 1,
                "operatorNotes": stored.operator_notes,
            }
            for stored in tables
        ]

        headers = {
            "content-rows": str(len(list_of_tables)),
            "content-max-returned-rows": str(self.content_max_rows),
            "content-total-rows": str(total),
        }
        return JSONResponse(content=list_of_tables, headers=headers, status_code=200)

    def get_export_tables(self, connection, schema, current_user):
        """Returns the export tables of a connection, and of a schema in it"""
        if schema == None:
            schema = "*"
        search_values = data_models.ExportTableSearch(
            connection=connection, targetSchema=schema, targetTable="*"
        )

        return self.search_export_tables(search_values, current_user)

    def get_export_table_details(self, connection, schema, table, include_columns):
        """Returns all export table details"""
        try:
            session = self.get_dbimport_session()
        except SQLError:
            self.disconnect_dbimport_db()
            return None

        try:
            stored = ExportTablesRepository(session()).table(connection, schema, table)
        finally:
            session.remove()

        if stored is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="export table does not exist",
            )

        result_dict = {
            "connection": stored.dbalias,
            "targetSchema": stored.target_schema,
            "targetTable": stored.target_table,
            "exportType": stored.export_type,
            "exportTool": stored.export_tool,
            "database": stored.hive_db,
            "table": stored.hive_table,
            "lastUpdateFromHive": _datetime_text(stored.last_update_from_hive),
            "sqlWhereAddition": stored.sql_where_addition,
            "includeInAirflow": stored.include_in_airflow == 1,
            "airflowPriority": stored.airflow_priority,
            "forceCreateTempTable": stored.force_create_temp_table,
            "validateExport": stored.validate_export,
            "validationMethod": stored.validation_method,
            "validationCustomQueryHiveSQL": stored.validation_custom_query_hive_sql,
            "validationCustomQueryTargetSQL": (
                stored.validation_custom_query_target_sql
            ),
            "uppercaseColumns": stored.uppercase_columns,
            "truncateTarget": stored.truncate_target,
            "sqlSessions": stored.mappers,
            "tableRowcount": stored.hive_rowcount,
            "targetRowcount": stored.target_rowcount,
            "validationCustomQueryHiveValue": stored.validation_custom_query_hive_value,
            "validationCustomQueryTargetValue": (
                stored.validation_custom_query_target_value
            ),
            "incrColumn": stored.incr_column,
            "incrValidationMethod": stored.incr_validation_method,
            "incrMinvalue": stored.incr_minvalue,
            "incrMaxvalue": stored.incr_maxvalue,
            "incrMinvaluePending": stored.incr_minvalue_pending,
            "incrMaxvaluePending": stored.incr_maxvalue_pending,
            "sqoopOptions": stored.sqoop_options,
            "lastSize": stored.sqoop_last_size,
            "lastRows": stored.sqoop_last_rows,
            "lastSqlSessions": stored.sqoop_last_mappers,
            "lastExecution": stored.sqoop_last_execution,
            "hiveContainerSize": stored.hive_javaheap,
            "createTargetTableSql": stored.create_target_table_sql,
            "operatorNotes": stored.operator_notes,
        }

        if include_columns == True:
            result_dict["columns"] = self.get_export_table_columns(
                connection, schema, table
            )

        return result_dict

    def get_export_table_columns(self, connection, schema, table):
        """Returns all columns in an export table"""
        try:
            session = self.get_dbimport_session()
        except SQLError:
            self.disconnect_dbimport_db()
            return None

        try:
            columns = ExportColumnsRepository(session()).columns_of_table(
                connection, schema, table
            )
        finally:
            session.remove()

        # ColumnOrder starts with 1 in the API, as for import columns, but with
        # 0 in the database.
        return [
            {
                "columnName": column.column_name,
                "columnType": (
                    "unknown" if column.column_type is None else column.column_type
                ),
                "columnOrder": (
                    None if column.column_order is None else column.column_order + 1
                ),
                "targetColumnName": column.target_column_name,
                "targetColumnType": column.target_column_type,
                "lastUpdateFromHive": _datetime_text(column.last_update_from_hive),
                "includeInExport": column.include_in_export == 1,
                "comment": column.comment,
                "operatorNotes": column.operator_notes,
            }
            for column in columns
        ]

    def delete_export_table(self, connection, schema, table, current_user):
        """Delete an export table"""
        log = logging.getLogger(self.logger)

        try:
            session = self.get_dbimport_session()
        except SQLError:
            self.disconnect_dbimport_db()
            return None

        log.info(
            "User '%s' deleted export table '%s.%s' on connection '%s'",
            current_user,
            schema,
            table,
            connection,
        )

        try:
            deleted = ExportTablesRepository(session()).delete(
                connection, schema, table
            )
            session.commit()
        finally:
            session.remove()

        if not deleted:
            return ("Table does not exist", 404)
        return ("ok", 200)

    def bulk_delete_export_table(
        self, bulk_data: list[data_models.ExportTablePK], current_user
    ):
        """Bulk delete of export tables"""
        if len(bulk_data) == 0:
            result_msg = {
                "result": "Body must contain a list of connection, database and table to delete"
            }
            return_code = 422
            return (result_msg, return_code)

        try:
            session = self.get_dbimport_session()
        except SQLError:
            self.disconnect_dbimport_db()
            return None

        try:
            ExportTablesRepository(session()).delete_many(
                [
                    (tables.connection, tables.targetSchema, tables.targetTable)
                    for tables in bulk_data
                ]
            )
            session.commit()
        finally:
            session.remove()

        return ({"result": "Ok"}, 200)

    def bulk_update_export_table(
        self, bulk_data: data_models.ExportTableBulkUpdate, current_user
    ):
        """Bulk update of export tables"""
        if len(bulk_data.exportTables) == 0:
            result_msg = {
                "result": "exportTables must contain a list of connections/targetSchema/targetDatabase to update"
            }
            return_code = 422
            return (result_msg, return_code)

        try:
            session = self.get_dbimport_session()
        except SQLError:
            self.disconnect_dbimport_db()
            return None

        settings = {
            _EXPORT_TABLE_BULK[field]: getattr(bulk_data, field)
            for field in bulk_data.__pydantic_fields_set__
            if field in _EXPORT_TABLE_BULK
        }

        try:
            ExportTablesRepository(session()).update_many(
                [
                    (tables.connection, tables.targetSchema, tables.targetTable)
                    for tables in bulk_data.exportTables
                ],
                settings,
            )
            session.commit()
        finally:
            session.remove()

        return ({"result": "Ok"}, 200)

    def update_export_table(
        self,
        connection,
        schema,
        table,
        data: data_models.ExportTableDetailsWrite,
        current_user,
    ):
        """Update or create an export table"""
        log = logging.getLogger(self.logger)

        try:
            session = self.get_dbimport_session()
        except SQLError:
            self.disconnect_dbimport_db()
            return None

        log.info(
            "User '%s' updated/created export table '%s.%s' on connection '%s'",
            current_user,
            schema,
            table,
            connection,
        )

        settings = {
            column: getattr(data, field)
            for column, field in _EXPORT_TABLE_FIELDS.items()
        }
        for setting, default in _EXPORT_TABLE_DEFAULTS.items():
            if settings[setting] is None:
                settings[setting] = default

        columns = []
        for column in data.columns:
            values = {
                name: getattr(column, field)
                for name, field in _EXPORT_COLUMN_FIELDS.items()
            }
            if column.columnOrder is not None:
                values["column_order"] = int(column.columnOrder) - 1
            # Force set to Null if string is empty. This will otherwise cause problems during executions as it's not handled there
            for name in ("target_column_name", "target_column_type"):
                target = values[name]
                if isinstance(target, str) and target.strip() == "":
                    values[name] = None
            columns.append(
                values | {"hive_db": data.database, "hive_table": data.table}
            )

        try:
            tables = ExportTablesRepository(session())
            tables.save_settings(connection, schema, table, settings)
            table_id = tables.table_id_of(connection, schema, table)
            stored_columns = ExportColumnsRepository(session())
            for values in columns:
                stored_columns.save_column(table_id, values)
            session.commit()
        finally:
            session.remove()

        return ("ok", 200)

    def get_all_airflow_dags(self):
        """Returns all Airflow DAGs"""
        try:
            session = self.get_dbimport_session()
        except SQLError:
            self.disconnect_dbimport_db()
            return None

        try:
            dags = AirflowDAGsRepository(session()).all_dags(
                limit=self.content_max_rows
            )
        finally:
            session.remove()

        processed_dags: set[str] = set()
        for dag in dags:
            # Just to make sure that two DAGs dont exists with the same name in different tables
            if dag.dag_name in processed_dags:
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail="Airflow DAG '%s' exists in more than one DAG table"
                    % (dag.dag_name),
                )
            processed_dags.add(dag.dag_name)

        list_of_dags = [
            {
                "name": dag.dag_name,
                "type": dag.dag_type,
                "scheduleInterval": dag.schedule_interval,
                "autoRegenerateDag": self.convert_int_to_bool(dag.auto_regenerate_dag),
            }
            for dag in dags
        ]

        headers = {
            "content-rows": str(len(list_of_dags)),
            "content-max-returned-rows": str(self.content_max_rows),
        }
        return JSONResponse(content=list_of_dags, headers=headers, status_code=200)

    def get_airflow_import_dags(self):
        """Returns all Airflow Import DAGs"""
        airflow_url = self.get_config_value("airflow_url")

        try:
            session = self.get_dbimport_session()
        except SQLError:
            self.disconnect_dbimport_db()
            return None

        try:
            dags = AirflowDAGsRepository(session()).import_dags(
                limit=self.content_max_rows
            )
        finally:
            session.remove()

        list_of_dags = [
            {
                "name": dag.dag_name,
                "scheduleInterval": dag.schedule_interval,
                "autoRegenerateDag": self.convert_int_to_bool(dag.auto_regenerate_dag),
                "filterTable": dag.filter_hive,
                "airflowLink": f"{airflow_url}/dags/{dag.dag_name}",
                "operatorNotes": dag.operator_notes,
                "sudoUser": dag.sudo_user,
                "timezone": dag.timezone,
                "email": dag.email,
                "emailOnFailure": self.convert_int_to_bool(dag.email_on_failure),
                "emailOnRetries": self.convert_int_to_bool(dag.email_on_retries),
                "slaWarningTime": _time_text(dag.sla_warning_time),
                "concurrency": dag.concurrency,
            }
            for dag in dags
        ]

        headers = {
            "content-rows": str(len(list_of_dags)),
            "content-max-returned-rows": str(self.content_max_rows),
        }
        return JSONResponse(content=list_of_dags, headers=headers, status_code=200)

    def get_airflow_export_dags(self):
        """Returns all Airflow Export DAGs"""
        airflow_url = self.get_config_value("airflow_url")

        try:
            session = self.get_dbimport_session()
        except SQLError:
            self.disconnect_dbimport_db()
            return None

        try:
            dags = AirflowDAGsRepository(session()).export_dags(
                limit=self.content_max_rows
            )
        finally:
            session.remove()

        list_of_dags = [
            {
                "name": dag.dag_name,
                "scheduleInterval": dag.schedule_interval,
                "autoRegenerateDag": self.convert_int_to_bool(dag.auto_regenerate_dag),
                "filterConnection": dag.filter_dbalias,
                "filterTargetSchema": dag.filter_target_schema,
                "filterTargetTable": dag.filter_target_table,
                "airflowLink": f"{airflow_url}/dags/{dag.dag_name}",
                "operatorNotes": dag.operator_notes,
                "sudoUser": dag.sudo_user,
                "timezone": dag.timezone,
                "email": dag.email,
                "emailOnFailure": self.convert_int_to_bool(dag.email_on_failure),
                "emailOnRetries": self.convert_int_to_bool(dag.email_on_retries),
                "slaWarningTime": _time_text(dag.sla_warning_time),
                "concurrency": dag.concurrency,
            }
            for dag in dags
        ]

        headers = {
            "content-rows": str(len(list_of_dags)),
            "content-max-returned-rows": str(self.content_max_rows),
        }
        return JSONResponse(content=list_of_dags, headers=headers, status_code=200)

    def get_airflow_custom_dags(self):
        """Returns all Airflow Custom DAGs"""
        airflow_url = self.get_config_value("airflow_url")

        try:
            session = self.get_dbimport_session()
        except SQLError:
            self.disconnect_dbimport_db()
            return None

        try:
            dags = AirflowDAGsRepository(session()).custom_dags(
                limit=self.content_max_rows
            )
        finally:
            session.remove()

        list_of_dags = [
            {
                "name": dag.dag_name,
                "scheduleInterval": dag.schedule_interval,
                "autoRegenerateDag": self.convert_int_to_bool(dag.auto_regenerate_dag),
                "airflowLink": f"{airflow_url}/dags/{dag.dag_name}",
                "operatorNotes": dag.operator_notes,
                "sudoUser": dag.sudo_user,
                "timezone": dag.timezone,
                "email": dag.email,
                "emailOnFailure": self.convert_int_to_bool(dag.email_on_failure),
                "emailOnRetries": self.convert_int_to_bool(dag.email_on_retries),
                "slaWarningTime": _time_text(dag.sla_warning_time),
                "concurrency": dag.concurrency,
            }
            for dag in dags
        ]

        headers = {
            "content-rows": str(len(list_of_dags)),
            "content-max-returned-rows": str(self.content_max_rows),
        }
        return JSONResponse(content=list_of_dags, headers=headers, status_code=200)

    def get_airflow_tasks(self, dagname):
        """Returns all Airflow tasks for a specific DAG"""
        try:
            session = self.get_dbimport_session()
        except SQLError:
            self.disconnect_dbimport_db()
            return None

        try:
            tasks = AirflowDAGsRepository(session()).tasks(dagname, included_only=False)
        finally:
            session.remove()

        return [
            {
                "name": task.task_name,
                "type": task.task_type,
                "placement": task.placement,
                "connection": task.jdbc_dbalias,
                "airflowPool": task.airflow_pool,
                "airflowPriority": task.airflow_priority,
                "includeInAirflow": task.include_in_airflow == 1,
                "taskDependencyDownstream": task.task_dependency_downstream,
                "taskDependencyUpstream": task.task_dependency_upstream,
                "taskConfig": task.task_config,
                "sensorPokeInterval": task.sensor_poke_interval,
                "sensorTimeoutMinutes": task.sensor_timeout_minutes,
                "sensorConnection": task.sensor_connection,
                "sensorSoftFail": task.sensor_soft_fail,
                "sudoUser": task.sudo_user,
            }
            for task in tasks
        ]

    def get_airflow_custom_dag(self, dagname):
        """Returns an Airflow Custom DAG"""
        airflow_url = self.get_config_value("airflow_url")

        try:
            session = self.get_dbimport_session()
        except SQLError:
            self.disconnect_dbimport_db()
            return None

        try:
            dag = AirflowDAGsRepository(session()).custom_dag(dagname)
        finally:
            session.remove()

        if dag is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Airflow DAG does not exist",
            )

        return {
            "name": dag.dag_name,
            "scheduleInterval": dag.schedule_interval,
            "retries": dag.retries,
            "operatorNotes": dag.operator_notes,
            "applicationNotes": dag.application_notes,
            "airflowNotes": dag.airflow_notes,
            "autoRegenerateDag": self.convert_int_to_bool(dag.auto_regenerate_dag),
            "sudoUser": dag.sudo_user,
            "timezone": dag.timezone,
            "email": dag.email,
            "emailOnFailure": self.convert_int_to_bool(dag.email_on_failure),
            "emailOnRetries": self.convert_int_to_bool(dag.email_on_retries),
            "tags": dag.tags,
            "slaWarningTime": _time_text(dag.sla_warning_time),
            "retryExponentialBackoff": dag.retry_exponential_backoff,
            "concurrency": dag.concurrency,
            "airflowLink": f"{airflow_url}/dags/{dag.dag_name}",
            "tasks": self.get_airflow_tasks(dagname),
        }

    def get_airflow_export_dag(self, dagname):
        """Returns an Airflow Export DAG"""
        airflow_url = self.get_config_value("airflow_url")

        try:
            session = self.get_dbimport_session()
        except SQLError:
            self.disconnect_dbimport_db()
            return None

        try:
            dag = AirflowDAGsRepository(session()).export_dag(dagname)
        finally:
            session.remove()

        if dag is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Airflow DAG does not exist",
            )

        return {
            "name": dag.dag_name,
            "scheduleInterval": dag.schedule_interval,
            "filterConnection": dag.filter_dbalias,
            "filterTargetSchema": dag.filter_target_schema,
            "filterTargetTable": dag.filter_target_table,
            "retries": 5 if dag.retries is None else dag.retries,
            "operatorNotes": dag.operator_notes,
            "applicationNotes": dag.application_notes,
            "autoRegenerateDag": self.convert_int_to_bool(dag.auto_regenerate_dag),
            "airflowNotes": dag.airflow_notes,
            "sudoUser": dag.sudo_user,
            "timezone": dag.timezone,
            "email": dag.email,
            "emailOnFailure": self.convert_int_to_bool(dag.email_on_failure),
            "emailOnRetries": self.convert_int_to_bool(dag.email_on_retries),
            "tags": dag.tags,
            "slaWarningTime": _time_text(dag.sla_warning_time),
            "retryExponentialBackoff": dag.retry_exponential_backoff,
            "concurrency": dag.concurrency,
            "airflowLink": f"{airflow_url}/dags/{dag.dag_name}",
            "tasks": self.get_airflow_tasks(dagname),
        }

    def get_airflow_import_dag(self, dagname):
        """Returns an Airflow Import DAG"""
        airflow_url = self.get_config_value("airflow_url")

        try:
            session = self.get_dbimport_session()
        except SQLError:
            self.disconnect_dbimport_db()
            return None

        try:
            dag = AirflowDAGsRepository(session()).import_dag(dagname)
        finally:
            session.remove()

        if dag is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Airflow DAG does not exist",
            )

        return {
            "name": dag.dag_name,
            "scheduleInterval": dag.schedule_interval,
            "filterTable": dag.filter_hive,
            "finishAllStage1First": dag.finish_all_stage1_first,
            "runImportAndEtlSeparate": dag.run_import_and_etl_separate,
            "retries": dag.retries,
            "retriesStage1": dag.retries_stage1,
            "retriesStage2": dag.retries_stage2,
            "poolStage1": dag.pool_stage1,
            "poolStage2": dag.pool_stage2,
            "operatorNotes": dag.operator_notes,
            "applicationNotes": dag.application_notes,
            "autoRegenerateDag": self.convert_int_to_bool(dag.auto_regenerate_dag),
            "airflowNotes": dag.airflow_notes,
            "sudoUser": dag.sudo_user,
            "metadataImport": dag.metadata_import,
            "timezone": dag.timezone,
            "email": dag.email,
            "emailOnFailure": self.convert_int_to_bool(dag.email_on_failure),
            "emailOnRetries": self.convert_int_to_bool(dag.email_on_retries),
            "tags": dag.tags,
            "slaWarningTime": _time_text(dag.sla_warning_time),
            "retryExponentialBackoff": dag.retry_exponential_backoff,
            "concurrency": dag.concurrency,
            "airflowLink": f"{airflow_url}/dags/{dag.dag_name}",
            "tasks": self.get_airflow_tasks(dagname),
        }

    def delete_task_from_airflow_dag(self, dagname, taskname, current_user):
        """Delete tasks from an Airflow DAG"""
        log = logging.getLogger(self.logger)

        try:
            session = self.get_dbimport_session()
        except SQLError:
            self.disconnect_dbimport_db()
            return None

        log.info(
            "User '%s' deleted Airflow task '%s' from DAG '%s'",
            current_user,
            taskname,
            dagname,
        )

        try:
            deleted = AirflowDAGsRepository(session()).delete_task(dagname, taskname)
            session.commit()
        finally:
            session.remove()

        if not deleted:
            return ({"result": "Airflow Task on DAG does not exist"}, 404)
        return ({"result": "ok"}, 200)

    def delete_import_airflow_dag(self, dagname, current_user):
        """Delete an Airflow import DAG"""
        log = logging.getLogger(self.logger)

        try:
            session = self.get_dbimport_session()
        except SQLError:
            self.disconnect_dbimport_db()
            return None

        log.info("User '%s' deleted Airflow import DAG '%s'", current_user, dagname)

        try:
            dags = AirflowDAGsRepository(session())
            found = dags.import_dag(dagname) is not None
            if found:
                dags.delete_import_dags([dagname])
                session.commit()
        finally:
            session.remove()

        if not found:
            return ("Airflow DAG does not exist", 404)
        return ("ok", 200)

    def bulk_delete_import_airflow_dag(
        self, bulk_data: list[data_models.AirflowDAGPK], current_user
    ):
        """Bulk delete of Airflow import Dags"""
        if len(bulk_data) == 0:
            result_msg = {
                "result": "Body must contain a list of Airflow dags to delete"
            }
            return (result_msg, 422)

        try:
            session = self.get_dbimport_session()
        except SQLError:
            self.disconnect_dbimport_db()
            return None

        try:
            AirflowDAGsRepository(session()).delete_import_dags(
                [dag.name for dag in bulk_data]
            )
            session.commit()
        finally:
            session.remove()

        return ({"result": "Ok"}, 200)

    def bulk_update_import_airflow_dag(
        self, bulk_data: data_models.AirflowDAGBulkUpdate, current_user
    ):
        """Bulk update of Airflow import Dags"""
        if len(bulk_data.dags) == 0:
            result_msg = {
                "result": "dags must contain a list of Airflow dags to update"
            }
            return (result_msg, 422)

        try:
            session = self.get_dbimport_session()
        except SQLError:
            self.disconnect_dbimport_db()
            return None

        settings = {
            _AIRFLOW_DAG_BULK[field]: getattr(bulk_data, field)
            for field in bulk_data.__pydantic_fields_set__
            if field in _AIRFLOW_DAG_BULK
        }

        try:
            AirflowDAGsRepository(session()).update_import_dags(
                [dag.name for dag in bulk_data.dags], settings
            )
            session.commit()
        finally:
            session.remove()

        return ({"result": "Ok"}, 200)

    def update_import_airflow_dag(
        self, dagname, airflow_dag: data_models.AirflowImportDAG, current_user
    ):
        """Update or create an Airflow import DAG"""
        log = logging.getLogger(self.logger)

        try:
            session = self.get_dbimport_session()
        except SQLError:
            self.disconnect_dbimport_db()
            return None

        try:
            dags = AirflowDAGsRepository(session())
            if dags.dag_types_named(dagname) - {"import"}:
                log.error(
                    "Airflow Import DAG creation was denied as the name was already existing on custom or export DAG"
                )
                return (
                    "Ariflow DAG with the specified name already exists as Custom or Export",
                    400,
                )

            log.info(
                "User '%s' updated/created Airflow import DAG '%s'",
                current_user,
                dagname,
            )

            # Set default values
            if airflow_dag.scheduleInterval is None:
                airflow_dag.scheduleInterval = "None"
            if airflow_dag.filterTable is None:
                airflow_dag.filterTable = ""
            if airflow_dag.finishAllStage1First is None:
                airflow_dag.finishAllStage1First = False
            if airflow_dag.retries is None:
                airflow_dag.retries = 5
            if airflow_dag.autoRegenerateDag is None:
                airflow_dag.autoRegenerateDag = True
            if airflow_dag.runImportAndEtlSeparate is None:
                airflow_dag.runImportAndEtlSeparate = False
            if airflow_dag.metadataImport is None:
                airflow_dag.metadataImport = False
            if airflow_dag.emailOnFailure is None:
                airflow_dag.emailOnFailure = False
            if airflow_dag.emailOnRetries is None:
                airflow_dag.emailOnRetries = False
            if airflow_dag.retryExponentialBackoff is None:
                airflow_dag.retryExponentialBackoff = False

            dags.save_import_dag(
                dagname,
                {
                    column: getattr(airflow_dag, field)
                    for field, column in _AIRFLOW_IMPORT_DAG_FIELDS.items()
                },
            )
            _save_airflow_tasks(dags, dagname, airflow_dag.tasks)
            session.commit()
        finally:
            session.remove()

        return ("ok", 200)

    def delete_export_airflow_dag(self, dagname, current_user):
        """Delete an Airflow export DAG"""
        log = logging.getLogger(self.logger)

        try:
            session = self.get_dbimport_session()
        except SQLError:
            self.disconnect_dbimport_db()
            return None

        log.info("User '%s' deleted Airflow export DAG '%s'", current_user, dagname)

        try:
            dags = AirflowDAGsRepository(session())
            found = dags.export_dag(dagname) is not None
            if found:
                dags.delete_export_dags([dagname])
                session.commit()
        finally:
            session.remove()

        if not found:
            return ("Airflow DAG does not exist", 404)
        return ("ok", 200)

    def bulk_delete_export_airflow_dag(
        self, bulk_data: list[data_models.AirflowDAGPK], current_user
    ):
        """Bulk delete of Airflow export Dags"""
        if len(bulk_data) == 0:
            result_msg = {
                "result": "Body must contain a list of Airflow dags to delete"
            }
            return (result_msg, 422)

        try:
            session = self.get_dbimport_session()
        except SQLError:
            self.disconnect_dbimport_db()
            return None

        try:
            AirflowDAGsRepository(session()).delete_export_dags(
                [dag.name for dag in bulk_data]
            )
            session.commit()
        finally:
            session.remove()

        return ({"result": "Ok"}, 200)

    def bulk_update_export_airflow_dag(
        self, bulk_data: data_models.AirflowDAGBulkUpdate, current_user
    ):
        """Bulk update of Airflow export Dags"""
        if len(bulk_data.dags) == 0:
            result_msg = {
                "result": "dags must contain a list of Airflow dags to update"
            }
            return (result_msg, 422)

        try:
            session = self.get_dbimport_session()
        except SQLError:
            self.disconnect_dbimport_db()
            return None

        settings = {
            _AIRFLOW_DAG_BULK[field]: getattr(bulk_data, field)
            for field in bulk_data.__pydantic_fields_set__
            if field in _AIRFLOW_DAG_BULK
        }

        try:
            AirflowDAGsRepository(session()).update_export_dags(
                [dag.name for dag in bulk_data.dags], settings
            )
            session.commit()
        finally:
            session.remove()

        return ({"result": "Ok"}, 200)

    def update_export_airflow_dag(
        self, dagname, airflow_dag: data_models.AirflowExportDAG, current_user
    ):
        """Update or create an Airflow export DAG"""
        log = logging.getLogger(self.logger)

        try:
            session = self.get_dbimport_session()
        except SQLError:
            self.disconnect_dbimport_db()
            return None

        try:
            dags = AirflowDAGsRepository(session())
            if dags.dag_types_named(dagname) - {"export"}:
                log.error(
                    "Airflow Import DAG creation was denied as the name was already existing on custom or export DAG"
                )
                return (
                    "Ariflow DAG with the specified name already exists as Custom or Export",
                    400,
                )

            log.info(
                "User '%s' updated/created Airflow export DAG '%s'",
                current_user,
                dagname,
            )

            # Set default values
            if airflow_dag.scheduleInterval is None:
                airflow_dag.scheduleInterval = "None"
            if airflow_dag.filterConnection is None:
                airflow_dag.filterConnection = ""
            if airflow_dag.retries is None:
                airflow_dag.retries = 5
            if airflow_dag.autoRegenerateDag is None:
                airflow_dag.autoRegenerateDag = True
            if airflow_dag.emailOnFailure is None:
                airflow_dag.emailOnFailure = False
            if airflow_dag.emailOnRetries is None:
                airflow_dag.emailOnRetries = False
            if airflow_dag.retryExponentialBackoff is None:
                airflow_dag.retryExponentialBackoff = 0

            dags.save_export_dag(
                dagname,
                {
                    column: getattr(airflow_dag, field)
                    for field, column in _AIRFLOW_EXPORT_DAG_FIELDS.items()
                },
            )
            _save_airflow_tasks(dags, dagname, airflow_dag.tasks)
            session.commit()
        finally:
            session.remove()

        return ("ok", 200)

    def delete_custom_airflow_dag(self, dagname, current_user):
        """Delete an Airflow custom DAG"""
        log = logging.getLogger(self.logger)

        try:
            session = self.get_dbimport_session()
        except SQLError:
            self.disconnect_dbimport_db()
            return None

        log.info("User '%s' deleted Airflow custom DAG '%s'", current_user, dagname)

        try:
            dags = AirflowDAGsRepository(session())
            found = dags.custom_dag(dagname) is not None
            if found:
                dags.delete_custom_dags([dagname])
                session.commit()
        finally:
            session.remove()

        if not found:
            return ("Airflow DAG does not exist", 404)
        return ("ok", 200)

    def bulk_delete_custom_airflow_dag(
        self, bulk_data: list[data_models.AirflowDAGPK], current_user
    ):
        """Bulk delete of Airflow custom Dags"""
        if len(bulk_data) == 0:
            result_msg = {
                "result": "Body must contain a list of Airflow dags to delete"
            }
            return (result_msg, 422)

        try:
            session = self.get_dbimport_session()
        except SQLError:
            self.disconnect_dbimport_db()
            return None

        try:
            AirflowDAGsRepository(session()).delete_custom_dags(
                [dag.name for dag in bulk_data]
            )
            session.commit()
        finally:
            session.remove()

        return ({"result": "Ok"}, 200)

    def bulk_update_custom_airflow_dag(
        self, bulk_data: data_models.AirflowDAGBulkUpdate, current_user
    ):
        """Bulk update of Airflow custom Dags"""
        if len(bulk_data.dags) == 0:
            result_msg = {
                "result": "dags must contain a list of Airflow dags to update"
            }
            return (result_msg, 422)

        try:
            session = self.get_dbimport_session()
        except SQLError:
            self.disconnect_dbimport_db()
            return None

        settings = {
            _AIRFLOW_DAG_BULK[field]: getattr(bulk_data, field)
            for field in bulk_data.__pydantic_fields_set__
            if field in _AIRFLOW_DAG_BULK
        }

        try:
            AirflowDAGsRepository(session()).update_custom_dags(
                [dag.name for dag in bulk_data.dags], settings
            )
            session.commit()
        finally:
            session.remove()

        return ({"result": "Ok"}, 200)

    def update_custom_airflow_dag(
        self, dagname, airflow_dag: data_models.AirflowCustomDAG, current_user
    ):
        """Update or create an Airflow custom DAG"""
        log = logging.getLogger(self.logger)

        try:
            session = self.get_dbimport_session()
        except SQLError:
            self.disconnect_dbimport_db()
            return None

        try:
            dags = AirflowDAGsRepository(session())
            if dags.dag_types_named(dagname) - {"custom"}:
                log.error(
                    "Airflow Import DAG creation was denied as the name was already existing on custom or export DAG"
                )
                return (
                    "Ariflow DAG with the specified name already exists as Custom or Export",
                    400,
                )

            log.info(
                "User '%s' updated/created Airflow custom DAG '%s'",
                current_user,
                dagname,
            )

            # Set default values
            if airflow_dag.scheduleInterval is None:
                airflow_dag.scheduleInterval = "None"
            if airflow_dag.retries is None:
                airflow_dag.retries = 0
            if airflow_dag.autoRegenerateDag is None:
                airflow_dag.autoRegenerateDag = True
            if airflow_dag.emailOnFailure is None:
                airflow_dag.emailOnFailure = False
            if airflow_dag.emailOnRetries is None:
                airflow_dag.emailOnRetries = False
            if airflow_dag.retryExponentialBackoff is None:
                airflow_dag.retryExponentialBackoff = 0

            dags.save_custom_dag(
                dagname,
                {
                    column: getattr(airflow_dag, field)
                    for field, column in _AIRFLOW_CUSTOM_DAG_FIELDS.items()
                },
            )
            _save_airflow_tasks(dags, dagname, airflow_dag.tasks)
            session.commit()
        finally:
            session.remove()

        return ("ok", 200)
