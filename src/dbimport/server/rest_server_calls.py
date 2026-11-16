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

from dbimport.common import constants as constant
from dbimport.common.exceptions import InvalidConfigurationError, SQLError
from dbimport.config_db import common_config, config_schema
from dbimport.server import data_models
from dbimport.settings import get_settings


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

        self.all_config_keys = [
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

        mysql_credentials = self.common_config.get_mysql_credentials()

        self.connect_str = f"mysql+pymysql://{mysql_credentials['mysql_username']}:{quote(mysql_credentials['mysql_password'], safe=' +')}@{mysql_credentials['mysql_hostname']}:{mysql_credentials['mysql_port']}/{mysql_credentials['mysql_database']}"

        try:
            self.config_db = sa.create_engine(
                self.connect_str, echo=self.debug_log_level, pool_pre_ping=True
            )
            self.config_db.connect()
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
        """Returns a value from the configuration table based on the supplied key. Value returned can be Int, Str or DateTime"""
        log = logging.getLogger(self.logger)
        return_value = None
        bool_value = False

        value_column, bool_value = self.common_config.get_config_value_column(key)

        try:
            session = self.get_dbimport_session()
        except SQLError:
            self.disconnect_dbimport_db()
            return None

        configuration_table = aliased(config_schema.Configuration)

        configuration = (
            session.query(
                configuration_table.configKey,
                configuration_table.valueInt,
                configuration_table.valueStr,
                configuration_table.valueDate,
            )
            .select_from(configuration_table)
            .filter(configuration_table.configKey == key)
            .one()
        )

        session.remove()
        return_value = None

        try:
            if value_column == "valueInt":
                return_value = int(configuration[1])

            if value_column == "valueStr":
                return_value = configuration[2]
                if return_value == None or return_value.strip() == "":
                    log.error(
                        "Configuration Key '%s' must have a value in '%s'",
                        key,
                        value_column,
                    )
                    return_value = None

            if bool_value:
                if return_value == 1:
                    return_value = True
                elif return_value == 0:
                    return_value = False
                else:
                    log.error(
                        "Configuration Key '%s' can only have 0 or 1 in column '%s'",
                        key,
                        value_column,
                    )
                    return_value = None
        except TypeError:
            log.error(
                "Configuration key '%s' cant be found. Have you upgraded the database schema to the latest version?"
                % (key)
            )
            return_value = None

        return return_value

    def create_default_admin_user(self):
        """Creates the default admin user in the auth_users table if it doesnt exist"""
        log = logging.getLogger(self.logger)

        try:
            session = self.get_dbimport_session()
        except SQLError:
            self.disconnect_dbimport_db()
            return None

        default_admin_user = self.get_config_value("restserver_admin_user")
        user_object = self.get_user(default_admin_user)
        if user_object == None:
            # User does not exists, so lets create it
            log.info(
                "Default admin user does not exists in the 'auth_users' table. Creating it with password 'admin'"
            )

            password_encoded = b"admin"
            salt = bcrypt.gensalt()
            hashed_password = bcrypt.hashpw(
                password=password_encoded, salt=salt
            ).decode("utf-8")

            query = sa.insert(config_schema.AuthUsers).values(
                username=default_admin_user, password=hashed_password
            )
            session.execute(query)
            session.commit()

        # session.close()
        session.remove()

    def create_user(self, user: data_models.User):
        """Creates a user in the auth_users table"""
        log = logging.getLogger(self.logger)

        try:
            session = self.get_dbimport_session()
        except SQLError:
            self.disconnect_dbimport_db()
            return None

        log.info("User '%s' created" % (user.username))

        query = sa.insert(config_schema.AuthUsers).values(
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

    def get_user(self, username):
        """Returns a dict with user information from the database"""
        log = logging.getLogger(self.logger)

        try:
            session = self.get_dbimport_session()
        except SQLError:
            self.disconnect_dbimport_db()
            return None

        table_auth_users = aliased(config_schema.AuthUsers)
        auth_user = (
            session.query(
                table_auth_users.username,
                table_auth_users.password,
                table_auth_users.disabled,
                table_auth_users.fullname,
                table_auth_users.department,
                table_auth_users.email,
            )
            .select_from(table_auth_users)
            .filter(table_auth_users.username == username)
            .first()
        )

        # session.close()
        session.remove()

        user = None

        if auth_user != None:
            user = {}
            user["username"] = auth_user[0]
            user["password"] = auth_user[1]
            user["disabled"] = auth_user[2]
            user["fullname"] = auth_user[3]
            user["department"] = auth_user[4]
            user["email"] = auth_user[5]

        return user

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

        table_auth_users = aliased(config_schema.AuthUsers)
        session.execute(
            update(table_auth_users),
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

    def delete_user(self, username):
        """Delete a user"""
        log = logging.getLogger(self.logger)

        try:
            session = self.get_dbimport_session()
        except SQLError:
            self.disconnect_dbimport_db()
            return None

        table_auth_users = aliased(config_schema.AuthUsers)

        (
            session.query(table_auth_users)
            .filter(table_auth_users.username == username)
            .delete()
        )
        session.commit()

        # session.close()
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

        table_jdbc_connections_drivers = aliased(config_schema.JDBCConnectionsDrivers)
        jdbc_connections_drivers = (
            session.query(
                table_jdbc_connections_drivers.database_type,
                table_jdbc_connections_drivers.version,
                table_jdbc_connections_drivers.driver,
                table_jdbc_connections_drivers.classpath,
            )
            .select_from(table_jdbc_connections_drivers)
            .order_by(table_jdbc_connections_drivers.database_type)
            .all()
        )

        list_of_jdbc_drivers = []
        for row in jdbc_connections_drivers:
            jdbc_driver = {}
            jdbc_driver["databaseType"] = row[0]
            jdbc_driver["version"] = row[1]
            jdbc_driver["driver"] = row[2]
            jdbc_driver["classpath"] = row[3]
            list_of_jdbc_drivers.append(jdbc_driver)

        json_result = json.loads(json.dumps(list_of_jdbc_drivers))

        # session.close()
        session.remove()

        return json_result

    def update_jdbc_driver(self, jdbc_driver: data_models.JDBCDriver, current_user):
        """Returns all configuration items from the configuration table"""
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

        table_jdbc_connections_drivers = aliased(config_schema.JDBCConnectionsDrivers)
        result = "ok"
        return_code = 200

        try:
            session.execute(
                update(table_jdbc_connections_drivers),
                [
                    {
                        "database_type": jdbc_driver.databaseType,
                        "version": jdbc_driver.version,
                        "driver": jdbc_driver.driver,
                        "classpath": jdbc_driver.classpath,
                    }
                ],
            )
            session.commit()
        # except SQLAlchemyError.StaleDataError:
        except sqlalchemy.orm.exc.StaleDataError:
            result = (
                "JDBC Driver databaseType with version is not a supported combination"
            )
            return_code = 400

        result_dict = {}
        result_dict["status"] = result
        # session.close()
        session.remove()

        json_result = json.loads(json.dumps(result_dict))
        # return (jsonResult, returnCode)
        return (result, return_code)

    def get_configuration(self, current_user):
        """Returns all configuration items from the configuration table"""
        log = logging.getLogger(self.logger)

        try:
            session = self.get_dbimport_session()
        except SQLError:
            self.disconnect_dbimport_db()
            return None

        configuration_table = aliased(config_schema.Configuration)
        table_jdbc_connections = aliased(config_schema.JDBCConnections)

        configurations = (
            session.query(
                configuration_table.configKey,
                configuration_table.valueInt,
                configuration_table.valueStr,
                configuration_table.valueDate,
            )
            .select_from(configuration_table)
            .order_by(configuration_table.configKey)
            .all()
        )

        result_dict = {}
        for row in configurations:
            if row[0] in ["restserver_secret_key"]:
                # These should not be visable in the REST service. Skip them
                continue

            if row[0] == "restserver_admin_user":
                self.admin_user = row[2]

            value_column, bool_value = self.common_config.get_config_value_column(
                row[0], ignore_error_output=True
            )
            if value_column is None:
                # If there is no handling of the value in self.common_config.get_config_value_column we will ignore it.
                # But even if it's handling in that configuration, it must also exists in the datamodel for it to be shown
                continue

            if row[0] in [
                "restserver_admin_user",
                "restserver_authentication_method",
                "restserver_token_ttl",
            ]:
                if current_user != self.admin_user:
                    continue

            if value_column == "valueInt":
                result_dict[row[0]] = row[1]
            elif value_column == "valueStr":
                result_dict[row[0]] = row[2]
            elif value_column == "valueDate":
                result_dict[row[0]] = row[3]

        session.remove()

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

        configuration_table = aliased(config_schema.Configuration)

        # First check to make sure we have valid options in the changed options
        try:
            for key in self.all_config_keys:
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

                    if key in [
                        "restserver_admin_user",
                        "restserver_authentication_method",
                        "restserver_token_ttl",
                    ]:
                        if current_user != self.admin_user:
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

        for key in self.all_config_keys:
            if type(getattr(configuration, key)) in (str, bool, int):
                # At this point, only configuration items that exists in the class is iterrated
                log.info(
                    "User '%s' updated global configuration. %s = %s",
                    current_user,
                    key,
                    getattr(configuration, key),
                )

                value_column, bool_value = self.common_config.get_config_value_column(
                    key
                )

                session.execute(
                    update(configuration_table),
                    [{"configKey": key, value_column: getattr(configuration, key)}],
                )
                session.commit()

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
        log = logging.getLogger(self.logger)

        try:
            session = self.get_dbimport_session()
        except SQLError:
            self.disconnect_dbimport_db()
            return None

        table_jdbc_connections = aliased(config_schema.JDBCConnections)
        jdbc_connections = (
            session.query(
                table_jdbc_connections.dbalias, table_jdbc_connections.jdbc_url
            )
            .select_from(table_jdbc_connections)
            .order_by(table_jdbc_connections.dbalias)
            .limit(self.content_max_rows)
        )

        list_of_connections = []
        count = 0
        for row in jdbc_connections:
            count = count + 1
            result_dict = {}
            result_dict["name"] = row[0]
            if list_only_name == False:
                result_dict["connectionString"] = row[1]
                result_dict["serverType"] = self.get_jdbc_server_type(
                    name=row[0], connection_string=row[1]
                )

            list_of_connections.append(result_dict)

            if count == self.content_max_rows:
                break

        json_result = json.loads(json.dumps(list_of_connections))
        session.remove()

        headers = {
            "content-rows": str(count),
            "content-max-returned-rows": str(self.content_max_rows),
        }
        return JSONResponse(content=json_result, headers=headers, status_code=200)

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

        jdbc_connections = aliased(config_schema.JDBCConnections)

        attribute_to_column = {}
        attribute_to_column["name"] = jdbc_connections.dbalias
        attribute_to_column["connectionString"] = jdbc_connections.jdbc_url
        attribute_to_column["timeWindowTimezone"] = jdbc_connections.timewindow_timezone
        attribute_to_column["operatorNotes"] = jdbc_connections.operator_notes
        attribute_to_column["contactInformation"] = jdbc_connections.contact_info
        attribute_to_column["description"] = jdbc_connections.description
        attribute_to_column["owner"] = jdbc_connections.owner

        # Return a list of connections
        jdbc_connections_data = session.query(
            jdbc_connections.dbalias, jdbc_connections.jdbc_url
        ).select_from(jdbc_connections)

        jdbc_connections_data_count = session.query(func.count(1)).select_from(
            jdbc_connections
        )

        # Add the filters for the specific column that we search on
        for items in search_values:
            if items[1] == None or items[1] == "" or items[1] == "*" or items[1] == "%":
                # If it's None, empty or search *, it means we dont need to search on that value. So lets skip it
                continue

            column = items[0]
            value = items[1]
            like_search = False

            if type(value) is str:
                if "%" in value or "*" in value:
                    value = value.replace("*", "%")
                    like_search = True

            log.debug("search filter: %s = %s", column, value)

            if like_search:
                jdbc_connections_data = jdbc_connections_data.filter(
                    like_op(attribute_to_column[column], value)
                )
                jdbc_connections_data_count = jdbc_connections_data_count.filter(
                    like_op(attribute_to_column[column], value)
                )
            else:
                jdbc_connections_data = jdbc_connections_data.filter(
                    attribute_to_column[column] == value
                )
                jdbc_connections_data_count = jdbc_connections_data_count.filter(
                    attribute_to_column[column] == value
                )

        # Fetch all rows matching the filter
        jdbc_connections_data = jdbc_connections_data.limit(self.content_max_rows)
        jdbc_connections_data_count = jdbc_connections_data_count.one()

        # Loop through result and create the return dict
        count = 0
        list_of_connections = []
        for row in jdbc_connections_data:
            count = count + 1
            result_dict = {}
            result_dict["name"] = row[0]
            result_dict["connectionString"] = row[1]
            result_dict["serverType"] = self.get_jdbc_server_type(
                name=row[0], connection_string=row[1]
            )

            list_of_connections.append(result_dict)

            if count == self.content_max_rows:
                break

        json_result = json.loads(json.dumps(list_of_connections))
        # session.close()
        session.remove()

        headers = {
            "content-rows": str(count),
            "content-max-returned-rows": str(self.content_max_rows),
            "content-total-rows": str(jdbc_connections_data_count[0]),
        }
        return JSONResponse(content=json_result, headers=headers, status_code=200)

    def update_connection(
        self, connection: data_models.ConnectionDetails, current_user
    ):
        """Update or create a Connections"""
        log = logging.getLogger(self.logger)

        try:
            session = self.get_dbimport_session()
        except SQLError:
            self.disconnect_dbimport_db()
            return None

        table_jdbc_connections = aliased(config_schema.JDBCConnections)
        result = "ok"
        return_code = 200

        log.debug(connection)
        log.info(
            "User '%s' updated/created connection '%s'", current_user, connection.name
        )

        if connection.forceString is None:
            connection.forceString = 0
        if connection.createDatalakeImport is None:
            connection.createDatalakeImport = 1
        if connection.createForeignKey is None:
            connection.createForeignKey = 1
        if connection.atlasDiscovery is None:
            connection.atlasDiscovery = 1

        try:
            query = insert(config_schema.JDBCConnections).values(
                dbalias=connection.name,
                jdbc_url=connection.connectionString,
                private_key_path=connection.privateKeyPath,
                public_key_path=connection.publicKeyPath,
                credentials=connection.credentials,
                datalake_source=connection.source,
                force_string=connection.forceString,
                max_import_sessions=connection.maxSessions,
                create_datalake_import=connection.createDatalakeImport,
                timewindow_start=connection.timeWindowStart,
                timewindow_stop=connection.timeWindowStop,
                timewindow_timezone=connection.timeWindowTimezone,
                operator_notes=connection.operatorNotes,
                contact_info=connection.contactInformation,
                description=connection.description,
                owner=connection.owner,
                environment=connection.environment,
                seed_file=connection.seedFile,
                create_foreign_keys=connection.createForeignKey,
                atlas_discovery=connection.atlasDiscovery,
                atlas_include_filter=connection.atlasIncludeFilter,
                atlas_exclude_filter=connection.atlasExcludeFilter,
                atlas_last_discovery=connection.atlasLastDiscovery,
            )

            query = query.on_duplicate_key_update(
                dbalias=connection.name,
                jdbc_url=connection.connectionString,
                private_key_path=connection.privateKeyPath,
                public_key_path=connection.publicKeyPath,
                credentials=connection.credentials,
                datalake_source=connection.source,
                force_string=connection.forceString,
                max_import_sessions=connection.maxSessions,
                create_datalake_import=connection.createDatalakeImport,
                timewindow_start=connection.timeWindowStart,
                timewindow_stop=connection.timeWindowStop,
                timewindow_timezone=connection.timeWindowTimezone,
                operator_notes=connection.operatorNotes,
                contact_info=connection.contactInformation,
                description=connection.description,
                owner=connection.owner,
                environment=connection.environment,
                seed_file=connection.seedFile,
                create_foreign_keys=connection.createForeignKey,
                atlas_discovery=connection.atlasDiscovery,
                atlas_include_filter=connection.atlasIncludeFilter,
                atlas_exclude_filter=connection.atlasExcludeFilter,
                atlas_last_discovery=connection.atlasLastDiscovery,
            )

            session.execute(query)
            session.commit()
        except SQLError as err:
            log.error(str(err))
            log.error(column)  # noqa: F821  # TODO(#164): column is undefined here

            result = str(err)
            return_code = 500

        # session.close()
        session.remove()
        return (result, return_code)

    def get_connection(self, connection):
        """Returns all Connections"""
        log = logging.getLogger(self.logger)

        try:
            session = self.get_dbimport_session()
        except SQLError:
            self.disconnect_dbimport_db()
            return None

        table_jdbc_connections = aliased(config_schema.JDBCConnections)
        row = (
            session.query(
                table_jdbc_connections.dbalias,
                table_jdbc_connections.jdbc_url,
                table_jdbc_connections.private_key_path,
                table_jdbc_connections.public_key_path,
                table_jdbc_connections.credentials,
                table_jdbc_connections.datalake_source,
                table_jdbc_connections.force_string,
                table_jdbc_connections.max_import_sessions,
                table_jdbc_connections.create_datalake_import,
                table_jdbc_connections.timewindow_start,
                table_jdbc_connections.timewindow_stop,
                table_jdbc_connections.timewindow_timezone,
                table_jdbc_connections.operator_notes,
                table_jdbc_connections.contact_info,
                table_jdbc_connections.description,
                table_jdbc_connections.owner,
                table_jdbc_connections.environment,
                table_jdbc_connections.seed_file,
                table_jdbc_connections.create_foreign_keys,
                table_jdbc_connections.atlas_discovery,
                table_jdbc_connections.atlas_include_filter,
                table_jdbc_connections.atlas_exclude_filter,
                table_jdbc_connections.atlas_last_discovery,
            )
            .select_from(table_jdbc_connections)
            .filter(table_jdbc_connections.dbalias == connection)
            .one_or_none()
        )

        if row == None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="connection does not exist",
            )

        result_dict = {}
        result_dict["name"] = row[0]
        result_dict["connectionString"] = row[1]
        result_dict["private_key_path"] = row[2]
        result_dict["public_key_path"] = row[3]
        result_dict["credentials"] = row[4]
        result_dict["source"] = row[5]
        result_dict["forceString"] = row[6]
        if (
            result_dict["forceString"] != 0
            and result_dict["forceString"] != 1
            and result_dict["forceString"] != -1
        ):
            result_dict["forceString"] = -1
        result_dict["maxSessions"] = row[7]
        result_dict["createDatalakeImport"] = row[8]
        try:
            result_dict["timeWindowStart"] = row[9].strftime("%H:%M:%S")
        except AttributeError:
            result_dict["timeWindowStart"] = None
        try:
            result_dict["timeWindowStop"] = row[10].strftime("%H:%M:%S")
        except AttributeError:
            result_dict["timeWindowStop"] = None
        result_dict["timeWindowTimezone"] = row[11]
        result_dict["operatorNotes"] = row[12]
        result_dict["contactInformation"] = row[13]
        result_dict["description"] = row[14]
        result_dict["owner"] = row[15]
        result_dict["environment"] = row[16]
        result_dict["seedFile"] = row[17]
        result_dict["createForeignKey"] = row[18]
        result_dict["atlasDiscovery"] = row[19]
        result_dict["atlasIncludeFilter"] = row[20]
        result_dict["atlasExcludeFilter"] = row[21]
        try:
            result_dict["atlasLastDiscovery"] = row[22].strftime("%Y-%m-%d %H:%M:%S")
        except AttributeError:
            result_dict["atlasLastDiscovery"] = None

        json_result = json.loads(json.dumps(result_dict))
        # session.close()
        session.remove()

        return json_result

    def delete_connection(self, connection, current_user):
        """Delete a connection"""
        log = logging.getLogger(self.logger)

        try:
            session = self.get_dbimport_session()
        except SQLError:
            self.disconnect_dbimport_db()
            return None

        result = "ok"
        return_code = 200

        log.info("User '%s' deleted connection '%s'", current_user, connection)

        # Fetch the tableID as that is required to update/create data in import_columns
        row = (
            session.query(config_schema.JDBCConnections.dbalias)
            .select_from(config_schema.JDBCConnections)
            .filter(config_schema.JDBCConnections.dbalias == connection)
            .one_or_none()
        )
        session.commit()

        if row == None:
            result = "Connection does not exist"
            return_code = 404
        else:
            (
                session.query(config_schema.JDBCConnections)
                .filter(config_schema.JDBCConnections.dbalias == connection)
                .delete()
            )
            session.commit()

        # session.close()
        session.remove()
        return (result, return_code)

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
        log = logging.getLogger(self.logger)

        try:
            session = self.get_dbimport_session()
        except SQLError:
            self.disconnect_dbimport_db()
            return None

        import_tables = aliased(config_schema.ImportTables)

        import_table_dbs = (
            session.query(
                import_tables.hive_db,
                func.count(import_tables.hive_db),
                func.max(import_tables.last_update_from_source),
                func.sum(import_tables.sqoop_last_size),
                func.sum(import_tables.sqoop_last_rows),
            )
            .select_from(import_tables)
            .group_by(import_tables.hive_db)
            .limit(self.content_max_rows)
        )

        list_of_dbs = []
        count = 0
        for row in import_table_dbs:
            count = count + 1
            result_dict = {}
            result_dict["name"] = row[0]
            result_dict["tables"] = row[1]
            if row[2] == None:
                result_dict["lastImport"] = ""
            else:
                result_dict["lastImport"] = row[2].strftime("%Y-%m-%d %H:%M:%S")

            if row[3] == None:
                result_dict["lastSize"] = 0
            else:
                result_dict["lastSize"] = int(row[3])

            if row[4] == None:
                result_dict["lastRows"] = 0
            else:
                result_dict["lastRows"] = int(row[4])
            list_of_dbs.append(result_dict)

            if count == self.content_max_rows:
                break

        json_result = json.loads(json.dumps(list_of_dbs))
        session.remove()

        headers = {
            "content-rows": str(count),
            "content-max-returned-rows": str(self.content_max_rows),
        }
        return JSONResponse(content=json_result, headers=headers, status_code=200)

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

        import_tables = aliased(config_schema.ImportTables)

        attribute_to_column = {}
        attribute_to_column["database"] = import_tables.hive_db
        attribute_to_column["table"] = import_tables.hive_table
        attribute_to_column["connection"] = import_tables.dbalias
        attribute_to_column["sourceSchema"] = import_tables.source_schema
        attribute_to_column["sourceTable"] = import_tables.source_table
        attribute_to_column["importPhaseType"] = import_tables.import_phase_type
        attribute_to_column["etlPhaseType"] = import_tables.etl_phase_type
        attribute_to_column["importTool"] = import_tables.import_tool
        attribute_to_column["etlEngine"] = import_tables.etl_engine
        attribute_to_column["includeInAirflow"] = import_tables.include_in_airflow
        attribute_to_column["operatorNotes"] = import_tables.operator_notes

        # Return a list of Hive tables with details
        import_tables_data = session.query(
            import_tables.hive_db,
            import_tables.hive_table,
            import_tables.dbalias,
            import_tables.source_schema,
            import_tables.source_table,
            import_tables.import_phase_type,
            import_tables.etl_phase_type,
            import_tables.import_tool,
            import_tables.etl_engine,
            import_tables.last_update_from_source,
            import_tables.include_in_airflow,
            import_tables.operator_notes,
        ).select_from(import_tables)

        import_tables_data_count = session.query(func.count(1)).select_from(
            import_tables
        )

        # Add the filters for the specific column that we search on
        for items in search_values:
            if items[1] == None or items[1] == "" or items[1] == "*" or items[1] == "%":
                # If it's None, empty or search *, it means we dont need to search on that value. So lets skip it
                continue

            column = items[0]
            value = items[1]
            like_search = False

            if type(value) is str:
                if "%" in value or "*" in value:
                    value = value.replace("*", "%")
                    like_search = True

            log.debug("search filter: %s = %s", column, value)

            if like_search:
                import_tables_data = import_tables_data.filter(
                    like_op(attribute_to_column[column], value)
                )
                import_tables_data_count = import_tables_data_count.filter(
                    like_op(attribute_to_column[column], value)
                )
            else:
                import_tables_data = import_tables_data.filter(
                    attribute_to_column[column] == value
                )
                import_tables_data_count = import_tables_data_count.filter(
                    attribute_to_column[column] == value
                )

        # Fetch all rows matching the filter
        import_tables_data = import_tables_data.limit(self.content_max_rows)
        import_tables_data_count = import_tables_data_count.one()

        # Loop through result and create the return dict
        count = 0
        list_of_tables = []
        for row in import_tables_data:
            count = count + 1
            result_dict = {}
            result_dict["database"] = row[0]
            result_dict["table"] = row[1]
            result_dict["connection"] = row[2]
            result_dict["sourceSchema"] = row[3]
            result_dict["sourceTable"] = row[4]
            result_dict["importPhaseType"] = row[5]
            result_dict["etlPhaseType"] = row[6]
            result_dict["importTool"] = row[7]
            result_dict["etlEngine"] = row[8]
            try:
                result_dict["lastUpdateFromSource"] = row[9].strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
            except AttributeError:
                result_dict["lastUpdateFromSource"] = None
            if row[10] == 1:
                result_dict["includeInAirflow"] = True
            else:
                result_dict["includeInAirflow"] = False
            result_dict["operatorNotes"] = row[11]

            list_of_tables.append(result_dict)

            if count == self.content_max_rows:
                break

        json_result = json.loads(json.dumps(list_of_tables))
        # session.close()
        session.remove()

        headers = {
            "content-rows": str(count),
            "content-max-returned-rows": str(self.content_max_rows),
            "content-total-rows": str(import_tables_data_count[0]),
        }
        return JSONResponse(content=json_result, headers=headers, status_code=200)

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
        log = logging.getLogger(self.logger)

        try:
            session = self.get_dbimport_session()
        except SQLError:
            self.disconnect_dbimport_db()
            return None

        import_tables = aliased(config_schema.ImportTables)

        # Return a list of Hive tables with details
        row = (
            session.query(
                import_tables.hive_db,
                import_tables.hive_table,
                import_tables.dbalias,
                import_tables.source_schema,
                import_tables.source_table,
                import_tables.import_phase_type,
                import_tables.etl_phase_type,
                import_tables.import_tool,
                import_tables.etl_engine,
                import_tables.last_update_from_source,
                import_tables.sqoop_sql_where_addition,
                import_tables.nomerge_ingestion_sql_addition,
                import_tables.include_in_airflow,
                import_tables.airflow_priority,
                import_tables.validate_import,
                import_tables.validationMethod,
                import_tables.validate_source,
                import_tables.validate_diff_allowed,
                import_tables.validationCustomQuerySourceSQL,
                import_tables.validationCustomQueryHiveSQL,
                import_tables.validationCustomQueryValidateImportTable,
                import_tables.truncate_hive,
                import_tables.mappers,
                import_tables.soft_delete_during_merge,
                import_tables.source_rowcount,
                import_tables.source_rowcount_incr,
                import_tables.hive_rowcount,
                import_tables.validationCustomQuerySourceValue,
                import_tables.validationCustomQueryHiveValue,
                import_tables.incr_mode,
                import_tables.incr_column,
                import_tables.incr_validation_method,
                import_tables.incr_minvalue,
                import_tables.incr_maxvalue,
                import_tables.incr_minvalue_pending,
                import_tables.incr_maxvalue_pending,
                import_tables.pk_column_override,
                import_tables.pk_column_override_mergeonly,
                import_tables.hive_merge_heap,
                import_tables.hive_split_count,
                import_tables.spark_executor_memory,
                import_tables.spark_executors,
                import_tables.split_by_column,
                import_tables.sqoop_query,
                import_tables.sqoop_options,
                import_tables.sqoop_last_size,
                import_tables.sqoop_last_rows,
                import_tables.sqoop_last_mappers,
                import_tables.sqoop_last_execution,
                import_tables.sqoop_use_generated_sql,
                import_tables.sqoop_allow_text_splitter,
                import_tables.force_string,
                import_tables.comment,
                import_tables.generated_hive_column_definition,
                import_tables.generated_sqoop_query,
                import_tables.generated_sqoop_options,
                import_tables.generated_pk_columns,
                import_tables.generated_foreign_keys,
                import_tables.datalake_source,
                import_tables.operator_notes,
                import_tables.copy_finished,
                import_tables.copy_slave,
                import_tables.create_foreign_keys,
                import_tables.invalidate_impala,
                import_tables.custom_max_query,
                import_tables.mergeCompactionMethod,
                import_tables.sourceTableType,
                import_tables.import_database,
                import_tables.import_table,
                import_tables.history_database,
                import_tables.history_table,
            )
            .select_from(import_tables)
            .filter(
                (import_tables.hive_db == database)
                & (import_tables.hive_table == table)
            )
            .one_or_none()
        )

        if row == None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="import table does not exist",
            )

        result_dict = {}
        result_dict["database"] = row[0]
        result_dict["table"] = row[1]
        result_dict["connection"] = row[2]
        result_dict["sourceSchema"] = row[3]
        result_dict["sourceTable"] = row[4]
        result_dict["importPhaseType"] = row[5]
        result_dict["etlPhaseType"] = row[6]
        result_dict["importTool"] = row[7]
        result_dict["etlEngine"] = row[8]
        try:
            result_dict["lastUpdateFromSource"] = row[9].strftime("%Y-%m-%d %H:%M:%S")
        except AttributeError:
            result_dict["lastUpdateFromSource"] = None
        result_dict["sqlWhereAddition"] = row[10]
        result_dict["nomergeIngestionSqlAddition"] = row[11]
        if row[12] == 1:
            result_dict["includeInAirflow"] = True
        else:
            result_dict["includeInAirflow"] = False
        result_dict["airflowPriority"] = row[13]
        result_dict["validateImport"] = row[14]
        result_dict["validationMethod"] = row[15]
        result_dict["validateSource"] = row[16]
        result_dict["validateDiffAllowed"] = row[17]
        result_dict["validationCustomQuerySourceSQL"] = row[18]
        result_dict["validationCustomQueryHiveSQL"] = row[19]
        result_dict["validationCustomQueryValidateImportTable"] = row[20]
        # 		resultDict['truncateTable'] = row[21]
        result_dict["sqlSessions"] = row[22]
        result_dict["softDeleteDuringMerge"] = row[23]
        result_dict["sourceRowcount"] = row[24]
        result_dict["sourceRowcountIncr"] = row[25]
        result_dict["targetRowcount"] = row[26]
        result_dict["validationCustomQuerySourceValue"] = row[27]
        result_dict["validationCustomQueryHiveValue"] = row[28]
        if row[29] == None or row[29] == "":
            result_dict["incrMode"] = "lastmodified"
        else:
            result_dict["incrMode"] = row[29]
        result_dict["incrColumn"] = row[30]
        result_dict["incrValidationMethod"] = row[31]
        result_dict["incrMinvalue"] = row[32]
        result_dict["incrMaxvalue"] = row[33]
        result_dict["incrMinvaluePending"] = row[34]
        result_dict["incrMaxvaluePending"] = row[35]
        result_dict["pkColumnOverride"] = row[36]
        result_dict["pkColumnOverrideMergeonly"] = row[37]
        result_dict["hiveContainerSize"] = row[38]
        result_dict["splitCount"] = row[39]
        result_dict["sparkExecutorMemory"] = row[40]
        result_dict["sparkExecutors"] = row[41]
        result_dict["splitByColumn"] = row[42]
        result_dict["sqoopCustomQuery"] = row[43]
        result_dict["sqoopOptions"] = row[44]
        result_dict["lastSize"] = row[45]
        result_dict["lastRows"] = row[46]
        result_dict["lastSqlSessions"] = row[47]
        result_dict["lastExecution"] = row[48]
        if (
            row[49] == -1
        ):  # -1 is not allowed, but default in the database. Setting these to 1
            result_dict["sqoopUseGeneratedSql"] = 1
        else:
            result_dict["sqoopUseGeneratedSql"] = row[49]
        result_dict["allowTextSplitter"] = row[50]
        result_dict["forceString"] = row[51]
        if (
            result_dict["forceString"] != 0
            and result_dict["forceString"] != 1
            and result_dict["forceString"] != -1
        ):
            result_dict["forceString"] = -1
        result_dict["comment"] = row[52]
        result_dict["generatedHiveColumnDefinition"] = row[53]
        result_dict["generatedSqoopQuery"] = row[54]
        result_dict["generatedSqoopOptions"] = row[55]
        result_dict["generatedPkColumns"] = row[56]
        # 		resultDict['generatedForeignKeys'] = row[57]
        result_dict["datalakeSource"] = row[58]
        result_dict["operatorNotes"] = row[59]
        try:
            result_dict["copyFinished"] = row[60].strftime("%Y-%m-%d %H:%M:%S")
        except AttributeError:
            result_dict["copyFinished"] = None
        result_dict["copySlave"] = row[61]
        result_dict["createForeignKeys"] = row[62]
        result_dict["invalidateImpala"] = row[63]
        result_dict["customMaxQuery"] = row[64]
        result_dict["mergeCompactionMethod"] = row[65]
        result_dict["sourceTableType"] = row[66]
        result_dict["importDatabase"] = row[67]
        result_dict["importTable"] = row[68]
        result_dict["historyDatabase"] = row[69]
        result_dict["historyTable"] = row[70]

        if include_columns == True:
            result_dict["columns"] = self.get_import_table_columns(database, table)

        json_result = json.loads(json.dumps(result_dict))
        # session.close()
        session.remove()

        return json_result

    def delete_import_table(self, database, table, current_user):
        """Update or create an import table"""
        log = logging.getLogger(self.logger)

        try:
            session = self.get_dbimport_session()
        except SQLError:
            self.disconnect_dbimport_db()
            return None

        import_columns = aliased(config_schema.ImportColumns)
        import_tables = aliased(config_schema.ImportTables)
        result = "ok"
        return_code = 200

        log.info(
            "User '%s' deleted import table '%s.%s'", current_user, database, table
        )

        # Fetch the tableID as that is required to update/create data in import_columns
        row = (
            session.query(config_schema.ImportTables.table_id)
            .select_from(config_schema.ImportTables)
            .filter(
                (config_schema.ImportTables.hive_db == database)
                & (config_schema.ImportTables.hive_table == table)
            )
            .one_or_none()
        )
        session.commit()

        if row == None:
            result = "Table does not exist"
            return_code = 404
        else:
            table_id = row[0]

            (
                session.query(config_schema.ImportTables)
                .filter(config_schema.ImportTables.table_id == table_id)
                .delete()
            )
            session.commit()

        # session.close()
        session.remove()
        return (result, return_code)

    def bulk_delete_import_table(
        self, bulk_data: list[data_models.ImportTablePK], current_user
    ):
        """Bulk delete of import tables"""
        log = logging.getLogger(self.logger)

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

        delete_statement = delete(config_schema.ImportTables)
        delete_statement = delete_statement.where(
            (config_schema.ImportTables.hive_db == bindparam("database"))
            & (config_schema.ImportTables.hive_table == bindparam("table"))
        )

        # Create a list of dicts that contain all the databas and table combinations that will be deleted
        table_list = []
        for tables in bulk_data:
            table_list.append({"database": tables.database, "table": tables.table})

        session.connection().execute(delete_statement, table_list)
        session.commit()
        session.remove()

        return_code = 200
        result_msg = {"result": "Ok"}

        return (result_msg, return_code)

    def bulk_update_import_table(
        self, bulk_data: data_models.ImportTableBulkUpdate, current_user
    ):
        """Bulk update of import tables"""
        log = logging.getLogger(self.logger)

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

        import_tables = aliased(config_schema.ImportTables)
        update_statement = update(import_tables)
        update_statement = update_statement.where(
            (import_tables.hive_db == bindparam("database"))
            & (import_tables.hive_table == bindparam("table"))
        )

        # This will loop through only the fields specified in the Posted JSON. So if a field have got a default None value in dataModel validation,
        # that field wont be part of this loop
        for update_field in bulk_data.__pydantic_fields_set__:
            if update_field == "connection":
                update_statement = update_statement.values(dbalias=bulk_data.connection)
            elif update_field == "importPhaseType":
                update_statement = update_statement.values(
                    import_phase_type=bulk_data.importPhaseType
                )
            elif update_field == "etlPhaseType":
                update_statement = update_statement.values(
                    etl_phase_type=bulk_data.etlPhaseType
                )
            elif update_field == "importTool":
                update_statement = update_statement.values(
                    import_tool=bulk_data.importTool
                )
            elif update_field == "etlEngine":
                update_statement = update_statement.values(
                    etl_engine=bulk_data.etlEngine
                )
            elif update_field == "includeInAirflow":
                update_statement = update_statement.values(
                    include_in_airflow=bulk_data.includeInAirflow
                )
            elif update_field == "operatorNotes":
                update_statement = update_statement.values(
                    operator_notes=bulk_data.operatorNotes
                )

        # Create a list of dicts that contain all the databas/table combinations that will be updated
        table_list = []
        for tables in bulk_data.importTables:
            table_list.append({"database": tables.database, "table": tables.table})

        update_statement = update_statement.execution_options(synchronize_session=False)
        # 		print(updateStatement)

        session.connection().execute(update_statement, table_list)
        session.commit()
        session.remove()

        return_code = 200
        result_msg = {"result": "Ok"}

        return (result_msg, return_code)

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

        import_columns = aliased(config_schema.ImportColumns)
        import_tables = aliased(config_schema.ImportTables)
        result = "ok"
        return_code = 200

        log.debug(data)
        # log.info("User '%s' updated/created import table '%s.%s'"%(currentUser, getattr(data, "database"), getattr(data, "table")))
        log.info(
            "User '%s' updated/created import table '%s.%s'",
            current_user,
            database,
            table,
        )

        # Set default values
        if data.includeInAirflow is None:
            data.includeInAirflow = 1
        if data.validateImport is None:
            data.validateImport = 1
        if data.validationMethod is None:
            data.validationMethod = "rowCount"
        if data.validateSource is None:
            data.validateSource = "query"
        if data.validateDiffAllowed is None:
            data.validateDiffAllowed = -1
        if data.validationCustomQueryValidateImportTable is None:
            data.validationCustomQueryValidateImportTable = 1
        if data.incrValidationMethod is None:
            data.incrValidationMethod = "full"
            # 		if getattr(data, "truncateTable") == None:								setattr(data, "truncateTable", 1)
        if data.sqlSessions is None:
            data.sqlSessions = -1
        if data.softDeleteDuringMerge is None:
            data.softDeleteDuringMerge = 0
        if data.sqoopUseGeneratedSql is None:
            data.sqoopUseGeneratedSql = -1
        if data.allowTextSplitter is None:
            data.allowTextSplitter = 0
        if data.forceString is None:
            data.forceString = -1
        if data.invalidateImpala is None:
            data.invalidateImpala = -1
        if data.mergeCompactionMethod is None:
            data.mergeCompactionMethod = "default"
        if data.createForeignKeys is None:
            data.createForeignKeys = -1
        if data.copySlave is None:
            data.copySlave = 0

        try:
            # 				hive_db = getattr(data, "database"),
            # 				hive_table = getattr(data, "table"),
            query = insert(config_schema.ImportTables).values(
                hive_db=database,
                hive_table=table,
                dbalias=data.connection,
                source_schema=data.sourceSchema,
                source_table=data.sourceTable,
                import_phase_type=data.importPhaseType,
                etl_phase_type=data.etlPhaseType,
                import_tool=data.importTool,
                etl_engine=data.etlEngine,
                last_update_from_source=data.lastUpdateFromSource,
                sqoop_sql_where_addition=data.sqlWhereAddition,
                nomerge_ingestion_sql_addition=data.nomergeIngestionSqlAddition,
                include_in_airflow=data.includeInAirflow,
                airflow_priority=data.airflowPriority,
                validate_import=data.validateImport,
                validationMethod=data.validationMethod,
                validate_source=data.validateSource,
                validate_diff_allowed=data.validateDiffAllowed,
                validationCustomQuerySourceSQL=data.validationCustomQuerySourceSQL,
                validationCustomQueryHiveSQL=data.validationCustomQueryHiveSQL,
                validationCustomQueryValidateImportTable=data.validationCustomQueryValidateImportTable,
                # 				truncate_hive = getattr(data, "truncateTable"),
                mappers=data.sqlSessions,
                soft_delete_during_merge=data.softDeleteDuringMerge,
                incr_mode=data.incrMode,
                incr_column=data.incrColumn,
                incr_validation_method=data.incrValidationMethod,
                pk_column_override=data.pkColumnOverride,
                pk_column_override_mergeonly=data.pkColumnOverrideMergeonly,
                hive_merge_heap=data.hiveContainerSize,
                hive_split_count=data.splitCount,
                spark_executor_memory=data.sparkExecutorMemory,
                spark_executors=data.sparkExecutors,
                split_by_column=data.splitByColumn,
                sqoop_query=data.sqoopCustomQuery,
                sqoop_options=data.sqoopOptions,
                sqoop_use_generated_sql=data.sqoopUseGeneratedSql,
                sqoop_allow_text_splitter=data.allowTextSplitter,
                force_string=data.forceString,
                comment=data.comment,
                datalake_source=data.datalakeSource,
                operator_notes=data.operatorNotes,
                copy_slave=data.copySlave,
                create_foreign_keys=data.createForeignKeys,
                invalidate_impala=data.invalidateImpala,
                custom_max_query=data.customMaxQuery,
                mergeCompactionMethod=data.mergeCompactionMethod,
                sourceTableType=data.sourceTableType,
                import_database=data.importDatabase,
                import_table=data.importTable,
                history_database=data.historyDatabase,
                history_table=data.historyTable,
            )

            query = query.on_duplicate_key_update(
                dbalias=data.connection,
                source_schema=data.sourceSchema,
                source_table=data.sourceTable,
                import_phase_type=data.importPhaseType,
                etl_phase_type=data.etlPhaseType,
                import_tool=data.importTool,
                etl_engine=data.etlEngine,
                last_update_from_source=data.lastUpdateFromSource,
                sqoop_sql_where_addition=data.sqlWhereAddition,
                nomerge_ingestion_sql_addition=data.nomergeIngestionSqlAddition,
                include_in_airflow=data.includeInAirflow,
                airflow_priority=data.airflowPriority,
                validate_import=data.validateImport,
                validationMethod=data.validationMethod,
                validate_source=data.validateSource,
                validate_diff_allowed=data.validateDiffAllowed,
                validationCustomQuerySourceSQL=data.validationCustomQuerySourceSQL,
                validationCustomQueryHiveSQL=data.validationCustomQueryHiveSQL,
                validationCustomQueryValidateImportTable=data.validationCustomQueryValidateImportTable,
                # 				truncate_hive = getattr(data, "truncateTable"),
                mappers=data.sqlSessions,
                soft_delete_during_merge=data.softDeleteDuringMerge,
                incr_mode=data.incrMode,
                incr_column=data.incrColumn,
                incr_validation_method=data.incrValidationMethod,
                pk_column_override=data.pkColumnOverride,
                pk_column_override_mergeonly=data.pkColumnOverrideMergeonly,
                hive_merge_heap=data.hiveContainerSize,
                hive_split_count=data.splitCount,
                spark_executor_memory=data.sparkExecutorMemory,
                spark_executors=data.sparkExecutors,
                split_by_column=data.splitByColumn,
                sqoop_query=data.sqoopCustomQuery,
                sqoop_options=data.sqoopOptions,
                sqoop_use_generated_sql=data.sqoopUseGeneratedSql,
                sqoop_allow_text_splitter=data.allowTextSplitter,
                force_string=data.forceString,
                comment=data.comment,
                datalake_source=data.datalakeSource,
                operator_notes=data.operatorNotes,
                copy_slave=data.copySlave,
                create_foreign_keys=data.createForeignKeys,
                invalidate_impala=data.invalidateImpala,
                custom_max_query=data.customMaxQuery,
                mergeCompactionMethod=data.mergeCompactionMethod,
                sourceTableType=data.sourceTableType,
                import_database=data.importDatabase,
                import_table=data.importTable,
                history_database=data.historyDatabase,
                history_table=data.historyTable,
            )

            session.execute(query)
            session.commit()
        except SQLError as err:
            log.error(str(err))
            log.error(column)  # noqa: F821  # TODO(#164): column is undefined here

            result = str(err)
            return_code = 500

            # session.close()
            session.remove()
            return (result, return_code)

        # Fetch the tableID as that is required to update/create data in import_columns
        row = (
            session.query(import_tables.table_id)
            .select_from(import_tables)
            .filter(
                (import_tables.hive_db == database)
                & (import_tables.hive_table == table)
            )
            .one()
        )
        # 				.filter((ImportTables.hive_db == getattr(table, "database")) & (ImportTables.hive_table == getattr(table, "table")))

        session.execute(query)
        table_id = row[0]

        columns = data.columns
        for column in columns:
            # PK in import_columns is not the most optimal. So we need to check first if it exists and then insert or update. Upsert is not available
            row = (
                session.query(import_columns.column_id)
                .select_from(import_columns)
                .filter(
                    (import_columns.table_id == table_id)
                    & (import_columns.column_name == column.columnName)
                )
                .one_or_none()
            )

            # Force set to Null if string is empty. This will otherwise cause problems during executions as it's not handled there
            if (
                column.columnNameOverride is not None
                and column.columnNameOverride.strip() == ""
            ):
                column.columnNameOverride = None
            if (
                column.columnTypeOverride is not None
                and column.columnTypeOverride.strip() == ""
            ):
                column.columnTypeOverride = None

            try:
                if row == None:
                    log.debug("Column does not exist")

                    # 						hive_db = getattr(data, "database"),
                    # 						hive_table = getattr(data, "table"),
                    query = sa.insert(config_schema.ImportColumns).values(
                        table_id=table_id,
                        hive_db=database,
                        hive_table=table,
                        column_name=column.columnName,
                        column_order=column.columnOrder,
                        source_column_name=column.sourceColumnName,
                        column_type=column.columnType,
                        source_column_type=column.sourceColumnType,
                        source_database_type=column.sourceDatabaseType,
                        column_name_override=column.columnNameOverride,
                        column_type_override=column.columnTypeOverride,
                        sqoop_column_type=column.sqoopColumnType,
                        sqoop_column_type_override=column.sqoopColumnTypeOverride,
                        force_string=column.forceString,
                        include_in_import=column.includeInImport,
                        source_primary_key=column.sourcePrimaryKey,
                        last_update_from_source=column.lastUpdateFromSource,
                        comment=column.comment,
                        operator_notes=column.operatorNotes,
                        anonymization_function=column.anonymizationFunction,
                    )
                    session.execute(query)

                else:
                    column_id = row[0]
                    log.debug("Import column with id '%s' was updated" % (column_id))
                    # 							"hive_db": getattr(data, "database"),
                    # 							"hive_table": getattr(data, "table"),
                    session.execute(
                        update(import_columns),
                        [
                            {
                                "table_id": table_id,
                                "column_id": column_id,
                                "hive_db": database,
                                "hive_table": table,
                                "column_name": column.columnName,
                                "column_order": column.columnOrder,
                                "source_column_name": column.sourceColumnName,
                                "column_type": column.columnType,
                                "source_column_type": column.sourceColumnType,
                                "source_database_type": column.sourceDatabaseType,
                                "column_name_override": column.columnNameOverride,
                                "column_type_override": column.columnTypeOverride,
                                "sqoop_column_type": column.sqoopColumnType,
                                "sqoop_column_type_override": column.sqoopColumnTypeOverride,
                                "force_string": column.forceString,
                                "include_in_import": column.includeInImport,
                                "source_primary_key": column.sourcePrimaryKey,
                                "last_update_from_source": column.lastUpdateFromSource,
                                "comment": column.comment,
                                "operator_notes": column.operatorNotes,
                                "anonymization_function": column.anonymizationFunction,
                            }
                        ],
                    )

                session.commit()
            except SQLError as err:
                log.error(str(err))
                log.error(column)

                result = str(err)
                return_code = 500

                # session.close()
                session.remove()

                return (result, return_code)

            log.debug(column)

        # session.close()
        session.remove()
        return (result, return_code)

    def get_import_table_columns(self, database, table):
        """Returns all columns in an import table"""
        log = logging.getLogger(self.logger)

        try:
            session = self.get_dbimport_session()
        except SQLError:
            self.disconnect_dbimport_db()
            return None

        import_columns = aliased(config_schema.ImportColumns)
        list_of_columns = []

        # Return a list of Hive tables with details
        import_columns_data = (
            session.query(
                import_columns.hive_db,
                import_columns.hive_table,
                import_columns.column_name,
                import_columns.column_order,
                import_columns.source_column_name,
                import_columns.column_type,
                import_columns.source_column_type,
                import_columns.source_database_type,
                import_columns.column_name_override,
                import_columns.column_type_override,
                import_columns.sqoop_column_type,
                import_columns.sqoop_column_type_override,
                import_columns.force_string,
                import_columns.include_in_import,
                import_columns.source_primary_key,
                import_columns.last_update_from_source,
                import_columns.comment,
                import_columns.operator_notes,
                import_columns.anonymization_function,
            )
            .select_from(import_columns)
            .filter(
                (import_columns.hive_db == database)
                & (import_columns.hive_table == table)
            )
            .order_by(import_columns.column_order)
            .all()
        )

        for row in import_columns_data:
            result_dict = {}
            # 			resultDict['database'] = row[0]
            # 			resultDict['table'] = row[1]
            result_dict["columnName"] = row[2]
            result_dict["columnOrder"] = row[3]
            result_dict["sourceColumnName"] = row[4]
            result_dict["columnType"] = row[5]
            result_dict["sourceColumnType"] = row[6]
            result_dict["sourceDatabaseType"] = row[7]
            result_dict["columnNameOverride"] = row[8]
            result_dict["columnTypeOverride"] = row[9]
            result_dict["sqoopColumnType"] = row[10]
            result_dict["sqoopColumnTypeOverride"] = row[11]
            result_dict["forceString"] = row[12]
            if (
                result_dict["forceString"] != 0
                and result_dict["forceString"] != 1
                and result_dict["forceString"] != -1
            ):
                result_dict["forceString"] = -1
            result_dict["includeInImport"] = row[13]
            result_dict["sourcePrimaryKey"] = row[14]
            try:
                result_dict["lastUpdateFromSource"] = row[15].strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
            except AttributeError:
                result_dict["lastUpdateFromSource"] = None
            result_dict["comment"] = row[16]
            result_dict["operatorNotes"] = row[17]
            result_dict["anonymizationFunction"] = row[18]

            list_of_columns.append(result_dict)

        # 		jsonResult = json.loads(json.dumps(listOfColumns))
        # session.close()
        session.remove()

        # 		return jsonResult
        return list_of_columns

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
        log = logging.getLogger(self.logger)

        try:
            session = self.get_dbimport_session()
        except SQLError:
            self.disconnect_dbimport_db()
            return None

        export_tables = aliased(config_schema.ExportTables)

        export_table_dbs = (
            session.query(
                export_tables.dbalias,
                func.count(export_tables.dbalias),
                func.max(export_tables.last_update_from_hive),
                func.sum(export_tables.sqoop_last_rows),
            )
            .select_from(export_tables)
            .group_by(export_tables.dbalias)
            .limit(self.content_max_rows)
        )

        list_of_connections = []
        count = 0
        for row in export_table_dbs:
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
            list_of_connections.append(connection)

            if count == self.content_max_rows:
                break

        json_result = json.loads(json.dumps(list_of_connections))
        # session.close()
        session.remove()

        headers = {
            "content-rows": str(count),
            "content-max-returned-rows": str(self.content_max_rows),
        }
        return JSONResponse(content=json_result, headers=headers, status_code=200)

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
        """Search for import tables"""
        log = logging.getLogger(self.logger)

        try:
            session = self.get_dbimport_session()
        except SQLError:
            self.disconnect_dbimport_db()
            return None

        export_tables = aliased(config_schema.ExportTables)

        attribute_to_column = {}
        attribute_to_column["connection"] = export_tables.dbalias
        attribute_to_column["targetSchema"] = export_tables.target_schema
        attribute_to_column["targetTable"] = export_tables.target_table
        attribute_to_column["database"] = export_tables.hive_db
        attribute_to_column["table"] = export_tables.hive_table
        attribute_to_column["exportType"] = export_tables.export_type
        attribute_to_column["exportTool"] = export_tables.export_tool
        attribute_to_column["includeInAirflow"] = export_tables.include_in_airflow
        attribute_to_column["operatorNotes"] = export_tables.operator_notes

        # Return a list of Hive tables with details
        export_tables_data = session.query(
            export_tables.dbalias,
            export_tables.target_schema,
            export_tables.target_table,
            export_tables.hive_db,
            export_tables.hive_table,
            export_tables.export_type,
            export_tables.export_tool,
            export_tables.last_update_from_hive,
            export_tables.include_in_airflow,
            export_tables.operator_notes,
        ).select_from(export_tables)

        export_tables_data_count = session.query(func.count(1)).select_from(
            export_tables
        )

        # Add the filters for the specific column that we search on
        for items in search_values:
            if items[1] == None or items[1] == "" or items[1] == "*" or items[1] == "%":
                # If it's None, empty or search *, it means we dont need to search on that value. So lets skip it
                continue

            column = items[0]
            value = items[1]
            like_search = False

            if type(value) is str:
                if "%" in value or "*" in value:
                    value = value.replace("*", "%")
                    like_search = True

            log.debug("search filter: %s = %s", column, value)

            if like_search:
                export_tables_data = export_tables_data.filter(
                    like_op(attribute_to_column[column], value)
                )
                export_tables_data_count = export_tables_data_count.filter(
                    like_op(attribute_to_column[column], value)
                )
            else:
                export_tables_data = export_tables_data.filter(
                    attribute_to_column[column] == value
                )
                export_tables_data_count = export_tables_data_count.filter(
                    attribute_to_column[column] == value
                )

        # Fetch all rows matching the filter
        export_tables_data = export_tables_data.limit(self.content_max_rows)
        export_tables_data_count = export_tables_data_count.one()

        # Loop through result and create the return dict
        count = 0
        list_of_tables = []
        for row in export_tables_data:
            count = count + 1
            result_dict = {}
            result_dict["connection"] = row[0]
            result_dict["targetSchema"] = row[1]
            result_dict["targetTable"] = row[2]
            result_dict["database"] = row[3]
            result_dict["table"] = row[4]
            result_dict["exportType"] = row[5]
            result_dict["exportTool"] = row[6]
            try:
                result_dict["lastUpdateFromHive"] = row[7].strftime("%Y-%m-%d %H:%M:%S")
            except AttributeError:
                result_dict["lastUpdateFromHive"] = None
            if row[8] == 1:
                result_dict["includeInAirflow"] = True
            else:
                result_dict["includeInAirflow"] = False
            result_dict["operatorNotes"] = row[9]

            list_of_tables.append(result_dict)

            if count == self.content_max_rows:
                break

        json_result = json.loads(json.dumps(list_of_tables))
        session.remove()

        headers = {
            "content-rows": str(count),
            "content-max-returned-rows": str(self.content_max_rows),
            "content-total-rows": str(export_tables_data_count[0]),
        }
        return JSONResponse(content=json_result, headers=headers, status_code=200)

    def get_export_tables(self, connection, schema, current_user):
        """Returns all connections that have exports configured in them"""
        log = logging.getLogger(self.logger)

        if schema == None:
            schema = "*"
        search_values = data_models.ExportTableSearch(
            connection=connection, targetSchema=schema, targetTable="*"
        )

        return self.search_export_tables(search_values, current_user)

    def get_export_table_details(self, connection, schema, table, include_columns):
        """Returns all connections that have exports configured in them"""
        log = logging.getLogger(self.logger)

        try:
            session = self.get_dbimport_session()
        except SQLError:
            self.disconnect_dbimport_db()
            return None

        export_tables = aliased(config_schema.ExportTables)

        row = (
            session.query(
                export_tables.dbalias,
                export_tables.target_schema,
                export_tables.target_table,
                export_tables.export_type,
                export_tables.export_tool,
                export_tables.hive_db,
                export_tables.hive_table,
                export_tables.last_update_from_hive,
                export_tables.sql_where_addition,
                export_tables.include_in_airflow,
                export_tables.airflow_priority,
                export_tables.forceCreateTempTable,
                export_tables.validate_export,
                export_tables.validationMethod,
                export_tables.validationCustomQueryHiveSQL,
                export_tables.validationCustomQueryTargetSQL,
                export_tables.uppercase_columns,
                export_tables.truncate_target,
                export_tables.mappers,
                export_tables.hive_rowcount,
                export_tables.target_rowcount,
                export_tables.validationCustomQueryHiveValue,
                export_tables.validationCustomQueryTargetValue,
                export_tables.incr_column,
                export_tables.incr_validation_method,
                export_tables.incr_minvalue,
                export_tables.incr_maxvalue,
                export_tables.incr_minvalue_pending,
                export_tables.incr_maxvalue_pending,
                export_tables.sqoop_options,
                export_tables.sqoop_last_size,
                export_tables.sqoop_last_rows,
                export_tables.sqoop_last_mappers,
                export_tables.sqoop_last_execution,
                export_tables.hive_javaheap,
                export_tables.create_target_table_sql,
                export_tables.operator_notes,
            )
            .select_from(export_tables)
            .filter(
                (export_tables.dbalias == connection)
                & (export_tables.target_schema == schema)
                & (export_tables.target_table == table)
            )
            .one_or_none()
        )

        if row == None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="export table does not exist",
            )

        result_dict = {}
        result_dict["connection"] = row[0]
        result_dict["targetSchema"] = row[1]
        result_dict["targetTable"] = row[2]
        result_dict["exportType"] = row[3]
        result_dict["exportTool"] = row[4]
        result_dict["database"] = row[5]
        result_dict["table"] = row[6]
        try:
            result_dict["lastUpdateFromHive"] = row[7].strftime("%Y-%m-%d %H:%M:%S")
        except AttributeError:
            result_dict["lastUpdateFromHive"] = None
        result_dict["sqlWhereAddition"] = row[8]
        if row[9] == 1:
            result_dict["includeInAirflow"] = True
        else:
            result_dict["includeInAirflow"] = False
        result_dict["airflowPriority"] = row[10]
        result_dict["forceCreateTempTable"] = row[11]
        result_dict["validateExport"] = row[12]
        result_dict["validationMethod"] = row[13]
        result_dict["validationCustomQueryHiveSQL"] = row[14]
        result_dict["validationCustomQueryTargetSQL"] = row[15]
        result_dict["uppercaseColumns"] = row[16]
        result_dict["truncateTarget"] = row[17]
        result_dict["sqlSessions"] = row[18]
        result_dict["tableRowcount"] = row[19]
        result_dict["targetRowcount"] = row[20]
        result_dict["validationCustomQueryHiveValue"] = row[21]
        result_dict["validationCustomQueryTargetValue"] = row[22]
        result_dict["incrColumn"] = row[23]
        result_dict["incrValidationMethod"] = row[24]
        result_dict["incrMinvalue"] = row[25]
        result_dict["incrMaxvalue"] = row[26]
        result_dict["incrMinvaluePending"] = row[27]
        result_dict["incrMaxvaluePending"] = row[28]
        result_dict["sqoopOptions"] = row[29]
        result_dict["lastSize"] = row[30]
        result_dict["lastRows"] = row[31]
        result_dict["lastSqlSessions"] = row[32]
        result_dict["lastExecution"] = row[33]
        result_dict["hiveContainerSize"] = row[34]
        result_dict["createTargetTableSql"] = row[35]
        result_dict["operatorNotes"] = row[36]

        if include_columns == True:
            result_dict["columns"] = self.get_export_table_columns(
                connection, schema, table
            )

        json_result = json.loads(json.dumps(result_dict))
        # session.close()
        session.remove()

        return json_result

    def get_export_table_columns(self, connection, schema, table):
        """Returns all columns in an export table"""
        log = logging.getLogger(self.logger)

        try:
            session = self.get_dbimport_session()
        except SQLError:
            self.disconnect_dbimport_db()
            return None

        export_columns = aliased(config_schema.ExportColumns)
        export_tables = aliased(config_schema.ExportTables)
        list_of_columns = []

        # Return a list of Hive tables with details
        export_columns_data = (
            session.query(
                export_tables.dbalias,
                export_tables.target_schema,
                export_tables.target_table,
                export_columns.column_name,
                export_columns.column_type,
                export_columns.column_order,
                export_columns.target_column_name,
                export_columns.target_column_type,
                export_columns.last_update_from_hive,
                export_columns.include_in_export,
                export_columns.comment,
                export_columns.operator_notes,
            )
            .select_from(export_tables)
            .join(
                export_columns,
                export_tables.table_id == export_columns.table_id,
                isouter=False,
            )
            .filter(
                (export_tables.dbalias == connection)
                & (export_tables.target_schema == schema)
                & (export_tables.target_table == table)
            )
            .all()
        )

        for row in export_columns_data:
            result_dict = {}

            # 			resultDict['connection'] = row[0]
            # 			resultDict['targetSchema'] = row[1]
            # 			resultDict['targetTable'] = row[2]
            result_dict["columnName"] = row[3]

            if row[4] == None:
                result_dict["columnType"] = "unknown"
            else:
                result_dict["columnType"] = row[4]

            # ColumnOrder should start with 1 in API, but database stores is as a start with 0.
            # For ImportColumns, it starts with 1. So we need to do a +1 here to have the same functionality as ImportColumns
            if row[5] != None:
                result_dict["columnOrder"] = int(row[5]) + 1

            result_dict["targetColumnName"] = row[6]
            result_dict["targetColumnType"] = row[7]

            try:
                result_dict["lastUpdateFromHive"] = row[8].strftime("%Y-%m-%d %H:%M:%S")
            except AttributeError:
                result_dict["lastUpdateFromHive"] = None

            if row[9] == 1:
                result_dict["includeInExport"] = True
            else:
                result_dict["includeInExport"] = False

            result_dict["comment"] = row[10]
            result_dict["operatorNotes"] = row[11]

            list_of_columns.append(result_dict)

        # session.close()
        session.remove()

        return list_of_columns

    def delete_export_table(self, connection, schema, table, current_user):
        """Update or create an export table"""
        log = logging.getLogger(self.logger)

        try:
            session = self.get_dbimport_session()
        except SQLError:
            self.disconnect_dbimport_db()
            return None

        export_columns = aliased(config_schema.ExportColumns)
        export_tables = aliased(config_schema.ExportTables)
        result = "ok"
        return_code = 200

        log.info(
            "User '%s' deleted export table '%s.%s' on connection '%s'",
            current_user,
            schema,
            table,
            connection,
        )

        # Fetch the tableID as that is required to update/create data in import_columns
        row = (
            session.query(config_schema.ExportTables.table_id)
            .select_from(config_schema.ExportTables)
            .filter(
                (config_schema.ExportTables.dbalias == connection)
                & (config_schema.ExportTables.target_schema == schema)
                & (config_schema.ExportTables.target_table == table)
            )
            .one_or_none()
        )
        session.commit()

        if row == None:
            result = "Table does not exist"
            return_code = 404
        else:
            table_id = row[0]

            (
                session.query(config_schema.ExportTables)
                .filter(config_schema.ExportTables.table_id == table_id)
                .delete()
            )
            session.commit()

        # session.close()
        session.remove()
        return (result, return_code)

    def bulk_delete_export_table(
        self, bulk_data: list[data_models.ExportTablePK], current_user
    ):
        """Bulk delete of export tables"""
        log = logging.getLogger(self.logger)

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

        delete_statement = delete(config_schema.ExportTables)
        delete_statement = delete_statement.where(
            (config_schema.ExportTables.dbalias == bindparam("connection"))
            & (config_schema.ExportTables.target_schema == bindparam("targetSchema"))
            & (config_schema.ExportTables.target_table == bindparam("targetTable"))
        )

        # Create a list of dicts that contain all the connection, schema and table combinations that will be deleted
        table_list = []
        for tables in bulk_data:
            table_list.append(
                {
                    "connection": tables.connection,
                    "targetSchema": tables.targetSchema,
                    "targetTable": tables.targetTable,
                }
            )

        session.connection().execute(delete_statement, table_list)
        session.commit()
        session.remove()

        return_code = 200
        result_msg = {"result": "Ok"}

        return (result_msg, return_code)

    def bulk_update_export_table(
        self, bulk_data: data_models.ExportTableBulkUpdate, current_user
    ):
        """Bulk update of export tables"""
        log = logging.getLogger(self.logger)

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

        export_tables = aliased(config_schema.ExportTables)
        update_statement = update(export_tables)
        update_statement = update_statement.where(
            (export_tables.dbalias == bindparam("connection"))
            & (export_tables.target_schema == bindparam("targetSchema"))
            & (export_tables.target_table == bindparam("targetTable"))
        )

        # This will loop through only the fields specified in the Posted JSON. So if a field have got a default None value in dataModel validation,
        # that field wont be part of this loop
        for update_field in bulk_data.__pydantic_fields_set__:
            if update_field == "exportType":
                update_statement = update_statement.values(
                    export_type=bulk_data.exportType
                )
            elif update_field == "exportTool":
                update_statement = update_statement.values(
                    export_tool=bulk_data.exportTool
                )
            elif update_field == "includeInAirflow":
                update_statement = update_statement.values(
                    include_in_airflow=bulk_data.includeInAirflow
                )
            elif update_field == "operatorNotes":
                update_statement = update_statement.values(
                    operator_notes=bulk_data.operatorNotes
                )

        # Create a list of dicts that contain all the databas/table combinations that will be updated
        table_list = []
        for tables in bulk_data.exportTables:
            table_list.append(
                {
                    "connection": tables.connection,
                    "targetSchema": tables.targetSchema,
                    "targetTable": tables.targetTable,
                }
            )

        update_statement = update_statement.execution_options(synchronize_session=False)
        # 		print(updateStatement)

        session.connection().execute(update_statement, table_list)
        session.commit()
        session.remove()

        return_code = 200
        result_msg = {"result": "Ok"}

        return (result_msg, return_code)

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

        export_columns = aliased(config_schema.ExportColumns)
        export_tables = aliased(config_schema.ExportTables)
        result = "ok"
        return_code = 200

        log.debug(table)
        log.info(
            "User '%s' updated/created export table '%s.%s' on connection '%s'",
            current_user,
            schema,
            table,
            connection,
        )

        # Set default values
        if data.includeInAirflow is None:
            data.includeInAirflow = 1
        if data.forceCreateTempTable is None:
            data.forceCreateTempTable = 0
        if data.validateExport is None:
            data.validateExport = 1
        if data.validationMethod is None:
            data.validationMethod = "rowCount"
        if data.uppercaseColumns is None:
            data.uppercaseColumns = -1
        if data.truncateTarget is None:
            data.truncateTarget = 1
        if data.sqlSessions is None:
            data.sqlSessions = -1
        if data.incrValidationMethod is None:
            data.incrValidationMethod = "full"

        try:
            query = insert(config_schema.ExportTables).values(
                dbalias=connection,
                target_schema=schema,
                target_table=table,
                export_type=data.exportType,
                export_tool=data.exportTool,
                hive_db=data.database,
                hive_table=data.table,
                last_update_from_hive=data.lastUpdateFromHive,
                sql_where_addition=data.sqlWhereAddition,
                include_in_airflow=data.includeInAirflow,
                airflow_priority=data.airflowPriority,
                forceCreateTempTable=data.forceCreateTempTable,
                validate_export=data.validateExport,
                validationMethod=data.validationMethod,
                validationCustomQueryHiveSQL=data.validationCustomQueryHiveSQL,
                validationCustomQueryTargetSQL=data.validationCustomQueryTargetSQL,
                uppercase_columns=data.uppercaseColumns,
                truncate_target=data.truncateTarget,
                mappers=data.sqlSessions,
                incr_column=data.incrColumn,
                incr_validation_method=data.incrValidationMethod,
                sqoop_options=data.sqoopOptions,
                hive_javaheap=data.hiveContainerSize,
                create_target_table_sql=data.createTargetTableSql,
                operator_notes=data.operatorNotes,
            )

            query = query.on_duplicate_key_update(
                dbalias=connection,
                target_schema=schema,
                target_table=table,
                export_type=data.exportType,
                export_tool=data.exportTool,
                hive_db=data.database,
                hive_table=data.table,
                last_update_from_hive=data.lastUpdateFromHive,
                sql_where_addition=data.sqlWhereAddition,
                include_in_airflow=data.includeInAirflow,
                airflow_priority=data.airflowPriority,
                forceCreateTempTable=data.forceCreateTempTable,
                validate_export=data.validateExport,
                validationMethod=data.validationMethod,
                validationCustomQueryHiveSQL=data.validationCustomQueryHiveSQL,
                validationCustomQueryTargetSQL=data.validationCustomQueryTargetSQL,
                uppercase_columns=data.uppercaseColumns,
                truncate_target=data.truncateTarget,
                mappers=data.sqlSessions,
                incr_column=data.incrColumn,
                incr_validation_method=data.incrValidationMethod,
                sqoop_options=data.sqoopOptions,
                hive_javaheap=data.hiveContainerSize,
                create_target_table_sql=data.createTargetTableSql,
                operator_notes=data.operatorNotes,
            )

            session.execute(query)
            session.commit()
        except SQLError as err:
            log.error(str(err))
            log.error(column)  # noqa: F821  # TODO(#164): column is undefined here

            result = str(err)
            return_code = 500

            # session.close()
            session.remove()
            return (result, return_code)

        # Fetch the tableID as that is required to update/create data in export_columns
        row = (
            session.query(export_tables.table_id)
            .select_from(export_tables)
            .filter(
                (export_tables.dbalias == connection)
                & (export_tables.target_schema == schema)
                & (export_tables.target_table == table)
            )
            .one()
        )

        session.execute(query)
        table_id = row[0]

        columns = data.columns
        for column in columns:
            # PK in export_columns is not the most optimal. So we need to check first if it exists and then insert or update.
            # Upsert is not available
            row = (
                session.query(export_columns.column_id)
                .select_from(export_columns)
                .filter(
                    (export_columns.table_id == table_id)
                    & (export_columns.column_name == column.columnName)
                )
                .one_or_none()
            )

            try:
                column_order = None
                if column.columnOrder is not None:
                    column_order = int(column.columnOrder) - 1

                # Force set to Null if string is empty. This will otherwise cause problems during executions as it's not handled there
                if (
                    column.targetColumnName is not None
                    and column.targetColumnName.strip() == ""
                ):
                    column.targetColumnName = None
                if (
                    column.targetColumnType is not None
                    and column.targetColumnType.strip() == ""
                ):
                    column.targetColumnType = None

                if row == None:
                    log.debug("Column does not exist")

                    query = sa.insert(config_schema.ExportColumns).values(
                        table_id=table_id,
                        column_name=column.columnName,
                        column_type=column.columnType,
                        column_order=column_order,
                        hive_db=data.database,
                        hive_table=data.table,
                        target_column_name=column.targetColumnName,
                        target_column_type=column.targetColumnType,
                        last_update_from_hive=column.lastUpdateFromHive,
                        include_in_export=column.includeInExport,
                        comment=column.comment,
                        operator_notes=column.operatorNotes,
                    )

                    session.execute(query)

                else:
                    column_id = row[0]
                    log.debug("Export column with id '%s' was updated" % (column_id))

                    session.execute(
                        update(export_columns),
                        [
                            {
                                "table_id": table_id,
                                "column_id": column_id,
                                "column_name": column.columnName,
                                "column_type": column.columnType,
                                "column_order": column_order,
                                "hive_db": data.database,
                                "hive_table": data.table,
                                "target_column_name": column.targetColumnName,
                                "target_column_type": column.targetColumnType,
                                "last_update_from_hive": column.lastUpdateFromHive,
                                "include_in_export": column.includeInExport,
                                "comment": column.comment,
                                "operator_notes": column.operatorNotes,
                            }
                        ],
                    )

                session.commit()
            except SQLError as err:
                log.error(str(err))
                log.error(column)

                result = str(err)
                return_code = 500

                # session.close()
                session.remove()
                return (result, return_code)

            log.debug(column)

        # session.close()
        session.remove()
        return (result, return_code)

    def get_all_airflow_dags(self):
        """Returns all Airflow DAGs"""
        log = logging.getLogger(self.logger)

        try:
            session = self.get_dbimport_session()
        except SQLError:
            self.disconnect_dbimport_db()
            return None

        airflow_custom_dags = aliased(config_schema.AirflowCustomDAGs)
        airflow_export_dags = aliased(config_schema.AirflowExportDAGs)
        airflow_import_dags = aliased(config_schema.AirflowImportDAGs)
        list_of_dags = []
        processed_dags = []

        airflow_custom_dags_query = session.query(
            airflow_custom_dags.dag_name,
            sa.sql.expression.literal_column("'custom'").label("dag_type"),
            airflow_custom_dags.schedule_interval,
            airflow_custom_dags.auto_regenerate_dag,
            airflow_custom_dags.operator_notes,
            airflow_custom_dags.application_notes,
        ).select_from(airflow_custom_dags)

        airflow_import_dags_query = session.query(
            airflow_import_dags.dag_name,
            sa.sql.expression.literal_column("'import'").label("dag_type"),
            airflow_import_dags.schedule_interval,
            airflow_import_dags.auto_regenerate_dag,
            airflow_import_dags.operator_notes,
            airflow_import_dags.application_notes,
        ).select_from(airflow_import_dags)

        airflow_export_dags_query = session.query(
            airflow_export_dags.dag_name,
            sa.sql.expression.literal_column("'export'").label("dag_type"),
            airflow_export_dags.schedule_interval,
            airflow_export_dags.auto_regenerate_dag,
            airflow_export_dags.operator_notes,
            airflow_export_dags.application_notes,
        ).select_from(airflow_export_dags)

        airflow_dags_data = airflow_custom_dags_query.union(
            airflow_import_dags_query, airflow_export_dags_query
        ).limit(self.content_max_rows)

        count = 0
        for row in airflow_dags_data:
            count = count + 1
            result_dict = {}

            # Just to make sure that two DAGs dont exists with the same name in different tables
            if row[0] in processed_dags:
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail="Airflow DAG '%s' exists in more than one DAG table"
                    % (row[0]),
                )
            else:
                processed_dags.append(row[0])

            result_dict["name"] = row[0]
            result_dict["type"] = row[1]
            result_dict["scheduleInterval"] = row[2]
            result_dict["autoRegenerateDag"] = self.convert_int_to_bool(row[3])
            # 			resultDict['operatorNotes'] = row[4]
            # 			resultDict['applicationNotes'] = row[5]
            list_of_dags.append(result_dict)

            if count == self.content_max_rows:
                break

        json_result = json.loads(json.dumps(list_of_dags))
        # session.close()
        session.remove()

        headers = {
            "content-rows": str(count),
            "content-max-returned-rows": str(self.content_max_rows),
        }
        return JSONResponse(content=json_result, headers=headers, status_code=200)

    def get_airflow_import_dags(self):
        """Returns all Airflow Import DAGs"""
        log = logging.getLogger(self.logger)

        try:
            session = self.get_dbimport_session()
        except SQLError:
            self.disconnect_dbimport_db()
            return None

        airflow_url = self.get_config_value("airflow_url")

        airflow_import_dags = aliased(config_schema.AirflowImportDAGs)
        list_of_dags = []

        airflow_dags_data = (
            session.query(
                airflow_import_dags.dag_name,
                airflow_import_dags.schedule_interval,
                airflow_import_dags.auto_regenerate_dag,
                airflow_import_dags.filter_hive,
                airflow_import_dags.operator_notes,
                airflow_import_dags.sudo_user,
                airflow_import_dags.timezone,
                airflow_import_dags.email,
                airflow_import_dags.email_on_failure,
                airflow_import_dags.email_on_retries,
                airflow_import_dags.sla_warning_time,
                airflow_import_dags.concurrency,
            )
            .select_from(airflow_import_dags)
            .limit(self.content_max_rows)
        )

        count = 0
        for row in airflow_dags_data:
            count = count + 1
            result_dict = {}

            result_dict["name"] = row[0]
            result_dict["scheduleInterval"] = row[1]
            result_dict["autoRegenerateDag"] = self.convert_int_to_bool(row[2])
            result_dict["filterTable"] = row[3]
            result_dict["airflowLink"] = "%s/dags/%s" % (airflow_url, row[0])
            result_dict["operatorNotes"] = row[4]
            result_dict["sudoUser"] = row[5]
            result_dict["timezone"] = row[6]
            result_dict["email"] = row[7]
            result_dict["emailOnFailure"] = self.convert_int_to_bool(row[8])
            result_dict["emailOnRetries"] = self.convert_int_to_bool(row[9])
            try:
                result_dict["slaWarningTime"] = row[10].strftime("%H:%M:%S")
            except AttributeError:
                result_dict["slaWarningTime"] = None
            result_dict["concurrency"] = row[11]
            list_of_dags.append(result_dict)

            if count == self.content_max_rows:
                break

        json_result = json.loads(json.dumps(list_of_dags))
        # session.close()
        session.remove()

        headers = {
            "content-rows": str(count),
            "content-max-returned-rows": str(self.content_max_rows),
        }
        return JSONResponse(content=json_result, headers=headers, status_code=200)

    def get_airflow_export_dags(self):
        """Returns all Airflow Export DAGs"""
        log = logging.getLogger(self.logger)

        try:
            session = self.get_dbimport_session()
        except SQLError:
            self.disconnect_dbimport_db()
            return None

        airflow_url = self.get_config_value("airflow_url")

        airflow_export_dags = aliased(config_schema.AirflowExportDAGs)
        list_of_dags = []

        airflow_dags_data = (
            session.query(
                airflow_export_dags.dag_name,
                airflow_export_dags.schedule_interval,
                airflow_export_dags.auto_regenerate_dag,
                airflow_export_dags.filter_dbalias,
                airflow_export_dags.filter_target_schema,
                airflow_export_dags.filter_target_table,
                airflow_export_dags.operator_notes,
                airflow_export_dags.sudo_user,
                airflow_export_dags.timezone,
                airflow_export_dags.email,
                airflow_export_dags.email_on_failure,
                airflow_export_dags.email_on_retries,
                airflow_export_dags.sla_warning_time,
                airflow_export_dags.concurrency,
            )
            .select_from(airflow_export_dags)
            .limit(self.content_max_rows)
        )

        count = 0
        for row in airflow_dags_data:
            count = count + 1
            result_dict = {}

            result_dict["name"] = row[0]
            result_dict["scheduleInterval"] = row[1]
            result_dict["autoRegenerateDag"] = self.convert_int_to_bool(row[2])
            result_dict["filterConnection"] = row[3]
            result_dict["filterTargetSchema"] = row[4]
            result_dict["filterTargetTable"] = row[5]
            result_dict["airflowLink"] = "%s/dags/%s" % (airflow_url, row[0])
            result_dict["operatorNotes"] = row[6]
            result_dict["sudoUser"] = row[7]
            result_dict["timezone"] = row[8]
            result_dict["email"] = row[9]
            result_dict["emailOnFailure"] = self.convert_int_to_bool(row[10])
            result_dict["emailOnRetries"] = self.convert_int_to_bool(row[11])
            try:
                result_dict["slaWarningTime"] = row[12].strftime("%H:%M:%S")
            except AttributeError:
                result_dict["slaWarningTime"] = None
            result_dict["concurrency"] = row[13]

            list_of_dags.append(result_dict)

        json_result = json.loads(json.dumps(list_of_dags))
        # session.close()
        session.remove()

        headers = {
            "content-rows": str(count),
            "content-max-returned-rows": str(self.content_max_rows),
        }
        return JSONResponse(content=json_result, headers=headers, status_code=200)

    def get_airflow_custom_dags(self):
        """Returns all Airflow Custom DAGs"""
        log = logging.getLogger(self.logger)

        try:
            session = self.get_dbimport_session()
        except SQLError:
            self.disconnect_dbimport_db()
            return None

        airflow_url = self.get_config_value("airflow_url")

        airflow_custom_dags = aliased(config_schema.AirflowCustomDAGs)
        list_of_dags = []

        airflow_dags_data = (
            session.query(
                airflow_custom_dags.dag_name,
                airflow_custom_dags.schedule_interval,
                airflow_custom_dags.auto_regenerate_dag,
                airflow_custom_dags.operator_notes,
                airflow_custom_dags.sudo_user,
                airflow_custom_dags.timezone,
                airflow_custom_dags.email,
                airflow_custom_dags.email_on_failure,
                airflow_custom_dags.email_on_retries,
                airflow_custom_dags.sla_warning_time,
                airflow_custom_dags.concurrency,
            )
            .select_from(airflow_custom_dags)
            .limit(self.content_max_rows)
        )

        count = 0
        for row in airflow_dags_data:
            count = count + 1
            result_dict = {}

            result_dict["name"] = row[0]
            result_dict["scheduleInterval"] = row[1]
            result_dict["autoRegenerateDag"] = self.convert_int_to_bool(row[2])
            result_dict["airflowLink"] = "%s/dags/%s" % (airflow_url, row[0])
            result_dict["operatorNotes"] = row[3]
            result_dict["sudoUser"] = row[4]
            result_dict["timezone"] = row[5]
            result_dict["email"] = row[6]
            result_dict["emailOnFailure"] = self.convert_int_to_bool(row[7])
            result_dict["emailOnRetries"] = self.convert_int_to_bool(row[8])
            try:
                result_dict["slaWarningTime"] = row[9].strftime("%H:%M:%S")
            except AttributeError:
                result_dict["slaWarningTime"] = None
            result_dict["concurrency"] = row[10]
            list_of_dags.append(result_dict)

            if count == self.content_max_rows:
                break

        json_result = json.loads(json.dumps(list_of_dags))
        # session.close()
        session.remove()

        headers = {
            "content-rows": str(count),
            "content-max-returned-rows": str(self.content_max_rows),
        }
        return JSONResponse(content=json_result, headers=headers, status_code=200)

    def get_airflow_tasks(self, dagname):
        """Returns a list with all airflow dags associated with a specified dag"""
        log = logging.getLogger(self.logger)

        try:
            session = self.get_dbimport_session()
        except SQLError:
            self.disconnect_dbimport_db()
            return None

        airflow_tasks = aliased(config_schema.AirflowTasks)
        list_of_tasks = []

        # Return a list of Hive tables with details
        airflow_tasks_data = (
            session.query(
                airflow_tasks.task_name,
                airflow_tasks.task_type,
                airflow_tasks.placement,
                airflow_tasks.jdbc_dbalias,
                airflow_tasks.airflow_pool,
                airflow_tasks.airflow_priority,
                airflow_tasks.include_in_airflow,
                airflow_tasks.task_dependency_downstream,
                airflow_tasks.task_dependency_upstream,
                airflow_tasks.task_config,
                airflow_tasks.sensor_poke_interval,
                airflow_tasks.sensor_timeout_minutes,
                airflow_tasks.sensor_connection,
                airflow_tasks.sensor_soft_fail,
                airflow_tasks.sudo_user,
            )
            .select_from(airflow_tasks)
            .filter(airflow_tasks.dag_name == dagname)
            .all()
        )

        for row in airflow_tasks_data:
            result_dict = {}
            result_dict["name"] = row[0]
            result_dict["type"] = row[1]
            result_dict["placement"] = row[2]
            result_dict["connection"] = row[3]
            result_dict["airflowPool"] = row[4]
            result_dict["airflowPriority"] = row[5]
            if row[6] == 1:
                result_dict["includeInAirflow"] = True
            else:
                result_dict["includeInAirflow"] = False
            result_dict["taskDependencyDownstream"] = row[7]
            result_dict["taskDependencyUpstream"] = row[8]
            result_dict["taskConfig"] = row[9]
            result_dict["sensorPokeInterval"] = row[10]
            result_dict["sensorTimeoutMinutes"] = row[11]
            result_dict["sensorConnection"] = row[12]
            result_dict["sensorSoftFail"] = row[13]
            result_dict["sudoUser"] = row[14]
            list_of_tasks.append(result_dict)

        session.remove()
        return list_of_tasks

    def get_airflow_custom_dag(self, dagname):
        """Returns an Airflow Custom DAG"""
        log = logging.getLogger(self.logger)

        try:
            session = self.get_dbimport_session()
        except SQLError:
            self.disconnect_dbimport_db()
            return None

        airflow_url = self.get_config_value("airflow_url")

        airflow_custom_dags = aliased(config_schema.AirflowCustomDAGs)

        row = (
            session.query(
                airflow_custom_dags.dag_name,
                airflow_custom_dags.schedule_interval,
                airflow_custom_dags.retries,
                airflow_custom_dags.operator_notes,
                airflow_custom_dags.application_notes,
                airflow_custom_dags.airflow_notes,
                airflow_custom_dags.auto_regenerate_dag,
                airflow_custom_dags.sudo_user,
                airflow_custom_dags.timezone,
                airflow_custom_dags.email,
                airflow_custom_dags.email_on_failure,
                airflow_custom_dags.email_on_retries,
                airflow_custom_dags.tags,
                airflow_custom_dags.sla_warning_time,
                airflow_custom_dags.retry_exponential_backoff,
                airflow_custom_dags.concurrency,
            )
            .select_from(airflow_custom_dags)
            .filter(airflow_custom_dags.dag_name == dagname)
            .one_or_none()
        )

        if row == None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Airflow DAG does not exist",
            )

        result_dict = {}
        result_dict["name"] = row[0]
        result_dict["scheduleInterval"] = row[1]
        result_dict["retries"] = row[2]
        if result_dict["retries"] == None or result_dict["retries"] == "":
            result_dict["retries"] = 0
        result_dict["operatorNotes"] = row[3]
        result_dict["applicationNotes"] = row[4]
        result_dict["airflowNotes"] = row[5]
        result_dict["autoRegenerateDag"] = self.convert_int_to_bool(row[6])
        result_dict["sudoUser"] = row[7]
        result_dict["timezone"] = row[8]
        result_dict["email"] = row[9]
        result_dict["emailOnFailure"] = self.convert_int_to_bool(row[10])
        result_dict["emailOnRetries"] = self.convert_int_to_bool(row[11])
        result_dict["tags"] = row[12]
        try:
            result_dict["slaWarningTime"] = row[13].strftime("%H:%M:%S")
        except AttributeError:
            result_dict["slaWarningTime"] = None
        result_dict["retryExponentialBackoff"] = row[14]
        result_dict["concurrency"] = row[15]
        result_dict["airflowLink"] = "%s/dags/%s" % (airflow_url, row[0])

        result_dict["tasks"] = self.get_airflow_tasks(dagname)

        json_result = json.loads(json.dumps(result_dict))
        # session.close()
        session.remove()

        return json_result

    def check_dag_name_availability(
        self, name, exclude_import=False, exclude_export=False, exclude_custom=False
    ):
        """Checks if the DAG name is available. Returns True or False"""
        log = logging.getLogger(self.logger)

        try:
            session = self.get_dbimport_session()
        except SQLError:
            self.disconnect_dbimport_db()
            return None

        airflow_custom_dags = aliased(config_schema.AirflowCustomDAGs)
        airflow_export_dags = aliased(config_schema.AirflowExportDAGs)
        airflow_import_dags = aliased(config_schema.AirflowImportDAGs)
        list_of_dags = []
        processed_dags = []
        dag_name_is_free = True

        if exclude_custom == False:
            row = (
                session.query(airflow_custom_dags.dag_name)
                .select_from(airflow_custom_dags)
                .filter(airflow_custom_dags.dag_name == name)
                .one_or_none()
            )

            if row != None:
                dag_name_is_free = False

        if exclude_import == False:
            row = (
                session.query(airflow_import_dags.dag_name)
                .select_from(airflow_import_dags)
                .filter(airflow_import_dags.dag_name == name)
                .one_or_none()
            )

            if row != None:
                dag_name_is_free = False

        if exclude_export == False:
            row = (
                session.query(airflow_export_dags.dag_name)
                .select_from(airflow_export_dags)
                .filter(airflow_export_dags.dag_name == name)
                .one_or_none()
            )

            if row != None:
                dag_name_is_free = False

        session.remove()
        return dag_name_is_free

    def update_airflow_tasks(self, dagname, task, current_user):
        """Create or update Airflow tasks"""
        log = logging.getLogger(self.logger)

        try:
            session = self.get_dbimport_session()
        except SQLError:
            self.disconnect_dbimport_db()
            return None

        airflow_tasks = aliased(config_schema.AirflowTasks)

        try:
            query = insert(config_schema.AirflowTasks).values(
                dag_name=dagname,
                task_name=task.name,
                task_type=task.type,
                placement=task.placement,
                jdbc_dbalias=task.connection,
                airflow_pool=task.airflowPool,
                airflow_priority=task.airflowPriority,
                include_in_airflow=task.includeInAirflow,
                task_dependency_downstream=task.taskDependencyDownstream,
                task_dependency_upstream=task.taskDependencyUpstream,
                task_config=task.taskConfig,
                sensor_poke_interval=task.sensorPokeInterval,
                sensor_timeout_minutes=task.sensorTimeoutMinutes,
                sensor_connection=task.sensorConnection,
                sensor_soft_fail=task.sensorSoftFail,
                sudo_user=task.sudoUser,
            )

            query = query.on_duplicate_key_update(
                dag_name=dagname,
                task_name=task.name,
                task_type=task.type,
                placement=task.placement,
                jdbc_dbalias=task.connection,
                airflow_pool=task.airflowPool,
                airflow_priority=task.airflowPriority,
                include_in_airflow=task.includeInAirflow,
                task_dependency_downstream=task.taskDependencyDownstream,
                task_dependency_upstream=task.taskDependencyUpstream,
                task_config=task.taskConfig,
                sensor_poke_interval=task.sensorPokeInterval,
                sensor_timeout_minutes=task.sensorTimeoutMinutes,
                sensor_connection=task.sensorConnection,
                sensor_soft_fail=task.sensorSoftFail,
                sudo_user=task.sudoUser,
            )

            session.execute(query)
            session.commit()
        except SQLError as err:
            log.error(str(err))
            log.error(column)  # noqa: F821  # TODO(#164): column is undefined here

            # session.close()
            session.remove()
            return False

        session.remove()
        return True

    def get_airflow_export_dag(self, dagname):
        """Returns an Airflow Export DAG"""
        log = logging.getLogger(self.logger)

        try:
            session = self.get_dbimport_session()
        except SQLError:
            self.disconnect_dbimport_db()
            return None

        airflow_url = self.get_config_value("airflow_url")

        airflow_export_dags = aliased(config_schema.AirflowExportDAGs)

        row = (
            session.query(
                airflow_export_dags.dag_name,
                airflow_export_dags.schedule_interval,
                airflow_export_dags.filter_dbalias,
                airflow_export_dags.filter_target_schema,
                airflow_export_dags.filter_target_table,
                airflow_export_dags.retries,
                airflow_export_dags.operator_notes,
                airflow_export_dags.application_notes,
                airflow_export_dags.auto_regenerate_dag,
                airflow_export_dags.airflow_notes,
                airflow_export_dags.sudo_user,
                airflow_export_dags.timezone,
                airflow_export_dags.email,
                airflow_export_dags.email_on_failure,
                airflow_export_dags.email_on_retries,
                airflow_export_dags.tags,
                airflow_export_dags.sla_warning_time,
                airflow_export_dags.retry_exponential_backoff,
                airflow_export_dags.concurrency,
            )
            .select_from(airflow_export_dags)
            .filter(airflow_export_dags.dag_name == dagname)
            .one_or_none()
        )

        if row == None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Airflow DAG does not exist",
            )

        result_dict = {}
        result_dict["name"] = row[0]
        result_dict["scheduleInterval"] = row[1]
        result_dict["filterConnection"] = row[2]
        result_dict["filterTargetSchema"] = row[3]
        result_dict["filterTargetTable"] = row[4]
        result_dict["retries"] = row[5]
        if result_dict["retries"] == None or result_dict["retries"] == "":
            result_dict["retries"] = 5
        result_dict["operatorNotes"] = row[6]
        result_dict["applicationNotes"] = row[7]
        result_dict["autoRegenerateDag"] = self.convert_int_to_bool(row[8])
        result_dict["airflowNotes"] = row[9]
        result_dict["sudoUser"] = row[10]
        result_dict["timezone"] = row[11]
        result_dict["email"] = row[12]
        result_dict["emailOnFailure"] = self.convert_int_to_bool(row[13])
        result_dict["emailOnRetries"] = self.convert_int_to_bool(row[14])
        result_dict["tags"] = row[15]
        try:
            result_dict["slaWarningTime"] = row[16].strftime("%H:%M:%S")
        except AttributeError:
            result_dict["slaWarningTime"] = None
        result_dict["retryExponentialBackoff"] = row[17]
        result_dict["concurrency"] = row[18]
        result_dict["airflowLink"] = "%s/dags/%s" % (airflow_url, row[0])

        result_dict["tasks"] = self.get_airflow_tasks(dagname)

        json_result = json.loads(json.dumps(result_dict))
        # session.close()
        session.remove()

        return json_result

    def get_airflow_import_dag(self, dagname):
        """Returns an Airflow Import DAG"""
        log = logging.getLogger(self.logger)

        try:
            session = self.get_dbimport_session()
        except SQLError:
            self.disconnect_dbimport_db()
            return None

        airflow_url = self.get_config_value("airflow_url")

        airflow_import_dags = aliased(config_schema.AirflowImportDAGs)

        row = (
            session.query(
                airflow_import_dags.dag_name,
                airflow_import_dags.schedule_interval,
                airflow_import_dags.filter_hive,
                airflow_import_dags.finish_all_stage1_first,
                airflow_import_dags.run_import_and_etl_separate,
                airflow_import_dags.retries,
                airflow_import_dags.retries_stage1,
                airflow_import_dags.retries_stage2,
                airflow_import_dags.pool_stage1,
                airflow_import_dags.pool_stage2,
                airflow_import_dags.operator_notes,
                airflow_import_dags.application_notes,
                airflow_import_dags.auto_regenerate_dag,
                airflow_import_dags.airflow_notes,
                airflow_import_dags.sudo_user,
                airflow_import_dags.metadata_import,
                airflow_import_dags.timezone,
                airflow_import_dags.email,
                airflow_import_dags.email_on_failure,
                airflow_import_dags.email_on_retries,
                airflow_import_dags.tags,
                airflow_import_dags.sla_warning_time,
                airflow_import_dags.retry_exponential_backoff,
                airflow_import_dags.concurrency,
            )
            .select_from(airflow_import_dags)
            .filter(airflow_import_dags.dag_name == dagname)
            .one_or_none()
        )

        if row == None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Airflow DAG does not exist",
            )

        result_dict = {}
        result_dict["name"] = row[0]
        result_dict["scheduleInterval"] = row[1]
        result_dict["filterTable"] = row[2]
        result_dict["finishAllStage1First"] = row[3]
        result_dict["runImportAndEtlSeparate"] = row[4]
        result_dict["retries"] = row[5]
        if result_dict["retries"] == None or result_dict["retries"] == "":
            result_dict["retries"] = 5
        result_dict["retriesStage1"] = row[6]
        result_dict["retriesStage2"] = row[7]
        result_dict["poolStage1"] = row[8]
        result_dict["poolStage2"] = row[9]
        result_dict["operatorNotes"] = row[10]
        result_dict["applicationNotes"] = row[11]
        result_dict["autoRegenerateDag"] = self.convert_int_to_bool(row[12])
        result_dict["airflowNotes"] = row[13]
        result_dict["sudoUser"] = row[14]
        result_dict["metadataImport"] = row[15]
        result_dict["timezone"] = row[16]
        result_dict["email"] = row[17]
        result_dict["emailOnFailure"] = self.convert_int_to_bool(row[18])
        result_dict["emailOnRetries"] = self.convert_int_to_bool(row[19])
        result_dict["tags"] = row[20]
        try:
            result_dict["slaWarningTime"] = row[21].strftime("%H:%M:%S")
        except AttributeError:
            result_dict["slaWarningTime"] = None
        result_dict["retryExponentialBackoff"] = row[22]
        result_dict["concurrency"] = row[23]
        result_dict["airflowLink"] = "%s/dags/%s" % (airflow_url, row[0])

        result_dict["tasks"] = self.get_airflow_tasks(dagname)

        json_result = json.loads(json.dumps(result_dict))
        # session.close()
        session.remove()

        return json_result

    def delete_airflow_tasks(self, dagname):
        """Delete all Airflow tasks for the specified DAG"""
        log = logging.getLogger(self.logger)

        try:
            session = self.get_dbimport_session()
        except SQLError:
            self.disconnect_dbimport_db()
            return None

        (
            session.query(config_schema.AirflowTasks)
            .filter(config_schema.AirflowTasks.dag_name == dagname)
            .delete()
        )
        session.commit()
        session.remove()

    def delete_task_from_airflow_dag(self, dagname, taskname, current_user):
        """Delete tasks from an Airflow DAG"""
        log = logging.getLogger(self.logger)

        try:
            session = self.get_dbimport_session()
        except SQLError:
            self.disconnect_dbimport_db()
            return None

        airflow_import_dags = aliased(config_schema.AirflowImportDAGs)
        result_msg = {"result": "ok"}
        return_code = 200

        log.info(
            "User '%s' deleted Airflow task '%s' from DAG '%s'",
            current_user,
            taskname,
            dagname,
        )

        # Fetch the tableID as that is required to update/create data in import_columns
        row = (
            session.query(config_schema.AirflowTasks.dag_name)
            .select_from(config_schema.AirflowTasks)
            .filter(
                (config_schema.AirflowTasks.dag_name == dagname)
                & (config_schema.AirflowTasks.task_name == taskname)
            )
            .one_or_none()
        )
        session.commit()

        if row == None:
            result_msg = {"result": "Airflow Task on DAG does not exist"}
            return_code = 404
        else:
            (
                session.query(config_schema.AirflowTasks)
                .filter(
                    (config_schema.AirflowTasks.dag_name == dagname)
                    & (config_schema.AirflowTasks.task_name == taskname)
                )
                .delete()
            )
            session.commit()

        session.remove()
        return (result_msg, return_code)

    def delete_import_airflow_dag(self, dagname, current_user):
        """Delete an Airflow import DAG"""
        log = logging.getLogger(self.logger)

        try:
            session = self.get_dbimport_session()
        except SQLError:
            self.disconnect_dbimport_db()
            return None

        airflow_import_dags = aliased(config_schema.AirflowImportDAGs)
        result = "ok"
        return_code = 200

        log.info("User '%s' deleted Airflow import DAG '%s'", current_user, dagname)

        # Fetch the tableID as that is required to update/create data in import_columns
        row = (
            session.query(config_schema.AirflowImportDAGs.dag_name)
            .select_from(config_schema.AirflowImportDAGs)
            .filter(config_schema.AirflowImportDAGs.dag_name == dagname)
            .one_or_none()
        )
        session.commit()

        if row == None:
            result = "Airflow DAG does not exist"
            return_code = 404
        else:
            (
                session.query(config_schema.AirflowImportDAGs)
                .filter(config_schema.AirflowImportDAGs.dag_name == dagname)
                .delete()
            )
            session.commit()

            self.delete_airflow_tasks(dagname)

        # session.close()
        session.remove()
        return (result, return_code)

    def bulk_delete_import_airflow_dag(
        self, bulk_data: list[data_models.AirflowDAGPK], current_user
    ):
        """Bulk delete of Airflow import Dags"""
        log = logging.getLogger(self.logger)

        if len(bulk_data) == 0:
            result_msg = {
                "result": "Body must contain a list of Airflow dags to delete"
            }
            return_code = 422
            return (result_msg, return_code)

        try:
            session = self.get_dbimport_session()
        except SQLError:
            self.disconnect_dbimport_db()
            return None

        delete_statement = delete(config_schema.AirflowImportDAGs)
        delete_statement = delete_statement.where(
            config_schema.AirflowImportDAGs.dag_name == bindparam("dag")
        )

        # Create a list of dicts that contain all the databas and table combinations that will be deleted
        table_list = []
        for tables in bulk_data:
            table_list.append({"dag": tables.name})

        session.connection().execute(delete_statement, table_list)
        session.commit()
        session.remove()

        return_code = 200
        result_msg = {"result": "Ok"}

        return (result_msg, return_code)

    def bulk_update_import_airflow_dag(
        self, bulk_data: data_models.AirflowDAGBulkUpdate, current_user
    ):
        """Bulk update of Airflow import Dags"""
        log = logging.getLogger(self.logger)

        if len(bulk_data.dags) == 0:
            result_msg = {
                "result": "dags must contain a list of Airflow dags to update"
            }
            return_code = 422
            return (result_msg, return_code)

        try:
            session = self.get_dbimport_session()
        except SQLError:
            self.disconnect_dbimport_db()
            return None

        export_tables = aliased(config_schema.ExportTables)
        airflow_import_dags = aliased(config_schema.AirflowImportDAGs)
        update_statement = update(airflow_import_dags)
        update_statement = update_statement.where(
            airflow_import_dags.dag_name == bindparam("dag")
        )

        # This will loop through only the fields specified in the Posted JSON. So if a field have got a default None value in dataModel validation,
        # that field wont be part of this loop
        for update_field in bulk_data.__pydantic_fields_set__:
            if update_field == "scheduleInterval":
                update_statement = update_statement.values(
                    schedule_interval=bulk_data.scheduleInterval
                )
            elif update_field == "operatorNotes":
                update_statement = update_statement.values(
                    operator_notes=bulk_data.operatorNotes
                )
            elif update_field == "autoRegenerateDag":
                update_statement = update_statement.values(
                    auto_regenerate_dag=bulk_data.autoRegenerateDag
                )
            elif update_field == "sudoUser":
                update_statement = update_statement.values(sudo_user=bulk_data.sudoUser)
            elif update_field == "timezone":
                update_statement = update_statement.values(timezone=bulk_data.timezone)
            elif update_field == "email":
                update_statement = update_statement.values(email=bulk_data.email)
            elif update_field == "emailOnFailure":
                update_statement = update_statement.values(
                    email_on_failure=bulk_data.emailOnFailure
                )
            elif update_field == "emailOnRetries":
                update_statement = update_statement.values(
                    email_on_retries=bulk_data.emailOnRetries
                )
            elif update_field == "slaWarningTime":
                update_statement = update_statement.values(
                    sla_warning_time=bulk_data.slaWarningTime
                )
            elif update_field == "concurrency":
                update_statement = update_statement.values(
                    concurrency=bulk_data.concurrency
                )

        # Create a list of dicts that contain all the databas/table combinations that will be updated
        table_list = []
        for tables in bulk_data.dags:
            table_list.append({"dag": tables.name})

        update_statement = update_statement.execution_options(synchronize_session=False)
        # 		print(updateStatement)

        session.connection().execute(update_statement, table_list)
        session.commit()
        session.remove()

        return_code = 200
        result_msg = {"result": "Ok"}

        return (result_msg, return_code)

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

        airflow_import_dags = aliased(config_schema.AirflowImportDAGs)
        result = "ok"
        return_code = 200

        if self.check_dag_name_availability(name=dagname, exclude_import=True) == False:
            log.error(
                "Airflow Import DAG creation was denied as the name was already existing on custom or export DAG"
            )
            result = (
                "Ariflow DAG with the specified name already exists as Custom or Export"
            )
            return_code = 400
            return (result, return_code)

        log.debug(airflow_dag)
        log.info(
            "User '%s' updated/created Airflow import DAG '%s'", current_user, dagname
        )

        # Set default values
        if airflow_dag.scheduleInterval is None:
            airflow_dag.scheduleInterval = "None"
        if airflow_dag.filterTable is None:
            airflow_dag.filterTable = ""
        if airflow_dag.finishAllStage1First is None:
            airflow_dag.finishAllStage1First = "0"
        if airflow_dag.retries is None:
            airflow_dag.retries = "5"
        if airflow_dag.autoRegenerateDag is None:
            airflow_dag.autoRegenerateDag = "1"
        if airflow_dag.runImportAndEtlSeparate is None:
            airflow_dag.runImportAndEtlSeparate = "0"
        if airflow_dag.metadataImport is None:
            airflow_dag.metadataImport = "0"
        if airflow_dag.emailOnFailure is None:
            airflow_dag.emailOnFailure = "0"
        if airflow_dag.emailOnRetries is None:
            airflow_dag.emailOnRetries = "0"
        if airflow_dag.retryExponentialBackoff is None:
            airflow_dag.retryExponentialBackoff = "0"

        try:
            query = insert(config_schema.AirflowImportDAGs).values(
                dag_name=dagname,
                schedule_interval=airflow_dag.scheduleInterval,
                filter_hive=airflow_dag.filterTable,
                finish_all_stage1_first=airflow_dag.finishAllStage1First,
                run_import_and_etl_separate=airflow_dag.runImportAndEtlSeparate,
                retries=airflow_dag.retries,
                retries_stage1=airflow_dag.retriesStage1,
                retries_stage2=airflow_dag.retriesStage2,
                pool_stage1=airflow_dag.poolStage1,
                pool_stage2=airflow_dag.poolStage2,
                operator_notes=airflow_dag.operatorNotes,
                application_notes=airflow_dag.applicationNotes,
                auto_regenerate_dag=airflow_dag.autoRegenerateDag,
                airflow_notes=airflow_dag.airflowNotes,
                sudo_user=airflow_dag.sudoUser,
                metadata_import=airflow_dag.metadataImport,
                timezone=airflow_dag.timezone,
                email=airflow_dag.email,
                email_on_failure=airflow_dag.emailOnFailure,
                email_on_retries=airflow_dag.emailOnRetries,
                tags=airflow_dag.tags,
                sla_warning_time=airflow_dag.slaWarningTime,
                retry_exponential_backoff=airflow_dag.retryExponentialBackoff,
                concurrency=airflow_dag.concurrency,
            )

            query = query.on_duplicate_key_update(
                dag_name=dagname,
                schedule_interval=airflow_dag.scheduleInterval,
                filter_hive=airflow_dag.filterTable,
                finish_all_stage1_first=airflow_dag.finishAllStage1First,
                run_import_and_etl_separate=airflow_dag.runImportAndEtlSeparate,
                retries=airflow_dag.retries,
                retries_stage1=airflow_dag.retriesStage1,
                retries_stage2=airflow_dag.retriesStage2,
                pool_stage1=airflow_dag.poolStage1,
                pool_stage2=airflow_dag.poolStage2,
                operator_notes=airflow_dag.operatorNotes,
                application_notes=airflow_dag.applicationNotes,
                auto_regenerate_dag=airflow_dag.autoRegenerateDag,
                airflow_notes=airflow_dag.airflowNotes,
                sudo_user=airflow_dag.sudoUser,
                metadata_import=airflow_dag.metadataImport,
                timezone=airflow_dag.timezone,
                email=airflow_dag.email,
                email_on_failure=airflow_dag.emailOnFailure,
                email_on_retries=airflow_dag.emailOnRetries,
                tags=airflow_dag.tags,
                sla_warning_time=airflow_dag.slaWarningTime,
                retry_exponential_backoff=airflow_dag.retryExponentialBackoff,
                concurrency=airflow_dag.concurrency,
            )

            session.execute(query)
            session.commit()
        except SQLError as err:
            log.error(str(err))
            log.error(column)  # noqa: F821  # TODO(#164): column is undefined here

            result = str(err)
            return_code = 500

            # session.close()
            session.remove()
            return (result, return_code)

        # session.close()
        session.remove()

        tasks = airflow_dag.tasks
        for task in tasks:
            if (
                self.update_airflow_tasks(
                    dagname=dagname, task=task, current_user=current_user
                )
                == False
            ):
                result = "Error while updating/creating Airflow tasks. Please check logs on server"
                return_code = 400
                return (result, return_code)

        return (result, return_code)

    def delete_export_airflow_dag(self, dagname, current_user):
        """Delete an Airflow export DAG"""
        log = logging.getLogger(self.logger)

        try:
            session = self.get_dbimport_session()
        except SQLError:
            self.disconnect_dbimport_db()
            return None

        result = "ok"
        return_code = 200

        log.info("User '%s' deleted Airflow export DAG '%s'", current_user, dagname)

        # Fetch the tableID as that is required to update/create data in import_columns
        row = (
            session.query(config_schema.AirflowExportDAGs.dag_name)
            .select_from(config_schema.AirflowExportDAGs)
            .filter(config_schema.AirflowExportDAGs.dag_name == dagname)
            .one_or_none()
        )
        session.commit()

        if row == None:
            result = "Airflow DAG does not exist"
            return_code = 404
        else:
            (
                session.query(config_schema.AirflowExportDAGs)
                .filter(config_schema.AirflowExportDAGs.dag_name == dagname)
                .delete()
            )
            session.commit()

            self.delete_airflow_tasks(dagname)

        # session.close()
        session.remove()
        return (result, return_code)

    def bulk_delete_export_airflow_dag(
        self, bulk_data: list[data_models.AirflowDAGPK], current_user
    ):
        """Bulk delete of Airflow import Dags"""
        log = logging.getLogger(self.logger)

        if len(bulk_data) == 0:
            result_msg = {
                "result": "Body must contain a list of Airflow dags to delete"
            }
            return_code = 422
            return (result_msg, return_code)

        try:
            session = self.get_dbimport_session()
        except SQLError:
            self.disconnect_dbimport_db()
            return None

        delete_statement = delete(config_schema.AirflowExportDAGs)
        delete_statement = delete_statement.where(
            config_schema.AirflowExportDAGs.dag_name == bindparam("dag")
        )

        # Create a list of dicts that contain all the databas and table combinations that will be deleted
        table_list = []
        for tables in bulk_data:
            table_list.append({"dag": tables.name})

        session.connection().execute(delete_statement, table_list)
        session.commit()
        session.remove()

        return_code = 200
        result_msg = {"result": "Ok"}

        return (result_msg, return_code)

    def bulk_update_export_airflow_dag(
        self, bulk_data: data_models.AirflowDAGBulkUpdate, current_user
    ):
        """Bulk update of Airflow export Dags"""
        log = logging.getLogger(self.logger)

        if len(bulk_data.dags) == 0:
            result_msg = {
                "result": "dags must contain a list of Airflow dags to update"
            }
            return_code = 422
            return (result_msg, return_code)

        try:
            session = self.get_dbimport_session()
        except SQLError:
            self.disconnect_dbimport_db()
            return None

        export_tables = aliased(config_schema.ExportTables)
        airflow_export_dags = aliased(config_schema.AirflowExportDAGs)
        update_statement = update(airflow_export_dags)
        update_statement = update_statement.where(
            airflow_export_dags.dag_name == bindparam("dag")
        )

        # This will loop through only the fields specified in the Posted JSON. So if a field have got a default None value in dataModel validation,
        # that field wont be part of this loop
        for update_field in bulk_data.__pydantic_fields_set__:
            if update_field == "scheduleInterval":
                update_statement = update_statement.values(
                    schedule_interval=bulk_data.scheduleInterval
                )
            elif update_field == "operatorNotes":
                update_statement = update_statement.values(
                    operator_notes=bulk_data.operatorNotes
                )
            elif update_field == "autoRegenerateDag":
                update_statement = update_statement.values(
                    auto_regenerate_dag=bulk_data.autoRegenerateDag
                )
            elif update_field == "sudoUser":
                update_statement = update_statement.values(sudo_user=bulk_data.sudoUser)
            elif update_field == "timezone":
                update_statement = update_statement.values(timezone=bulk_data.timezone)
            elif update_field == "email":
                update_statement = update_statement.values(email=bulk_data.email)
            elif update_field == "emailOnFailure":
                update_statement = update_statement.values(
                    email_on_failure=bulk_data.emailOnFailure
                )
            elif update_field == "emailOnRetries":
                update_statement = update_statement.values(
                    email_on_retries=bulk_data.emailOnRetries
                )
            elif update_field == "slaWarningTime":
                update_statement = update_statement.values(
                    sla_warning_time=bulk_data.slaWarningTime
                )
            elif update_field == "concurrency":
                update_statement = update_statement.values(
                    concurrency=bulk_data.concurrency
                )

        # Create a list of dicts that contain all the databas/table combinations that will be updated
        table_list = []
        for tables in bulk_data.dags:
            table_list.append({"dag": tables.name})

        update_statement = update_statement.execution_options(synchronize_session=False)
        # 		print(updateStatement)

        session.connection().execute(update_statement, table_list)
        session.commit()
        session.remove()

        return_code = 200
        result_msg = {"result": "Ok"}

        return (result_msg, return_code)

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

        airflow_export_dags = aliased(config_schema.AirflowExportDAGs)
        result = "ok"
        return_code = 200

        if self.check_dag_name_availability(name=dagname, exclude_export=True) == False:
            log.error(
                "Airflow Import DAG creation was denied as the name was already existing on custom or export DAG"
            )
            result = (
                "Ariflow DAG with the specified name already exists as Custom or Export"
            )
            return_code = 400
            return (result, return_code)

        log.debug(airflow_dag)
        log.info(
            "User '%s' updated/created Airflow export DAG '%s'", current_user, dagname
        )

        # Set default values
        if airflow_dag.scheduleInterval is None:
            airflow_dag.scheduleInterval = "None"
        if airflow_dag.filterConnection is None:
            airflow_dag.filterConnection = ""
        if airflow_dag.retries is None:
            airflow_dag.retries = "5"
        if airflow_dag.autoRegenerateDag is None:
            airflow_dag.autoRegenerateDag = "1"
        if airflow_dag.emailOnFailure is None:
            airflow_dag.emailOnFailure = "0"
        if airflow_dag.emailOnRetries is None:
            airflow_dag.emailOnRetries = "0"
        if airflow_dag.retryExponentialBackoff is None:
            airflow_dag.retryExponentialBackoff = "0"

        try:
            query = insert(config_schema.AirflowExportDAGs).values(
                dag_name=dagname,
                schedule_interval=airflow_dag.scheduleInterval,
                filter_dbalias=airflow_dag.filterConnection,
                filter_target_schema=airflow_dag.filterTargetSchema,
                filter_target_table=airflow_dag.filterTargetTable,
                retries=airflow_dag.retries,
                operator_notes=airflow_dag.operatorNotes,
                application_notes=airflow_dag.applicationNotes,
                auto_regenerate_dag=airflow_dag.autoRegenerateDag,
                airflow_notes=airflow_dag.airflowNotes,
                sudo_user=airflow_dag.sudoUser,
                timezone=airflow_dag.timezone,
                email=airflow_dag.email,
                email_on_failure=airflow_dag.emailOnFailure,
                email_on_retries=airflow_dag.emailOnRetries,
                tags=airflow_dag.tags,
                sla_warning_time=airflow_dag.slaWarningTime,
                retry_exponential_backoff=airflow_dag.retryExponentialBackoff,
                concurrency=airflow_dag.concurrency,
            )

            query = query.on_duplicate_key_update(
                dag_name=dagname,
                schedule_interval=airflow_dag.scheduleInterval,
                filter_dbalias=airflow_dag.filterConnection,
                filter_target_schema=airflow_dag.filterTargetSchema,
                filter_target_table=airflow_dag.filterTargetTable,
                retries=airflow_dag.retries,
                operator_notes=airflow_dag.operatorNotes,
                application_notes=airflow_dag.applicationNotes,
                auto_regenerate_dag=airflow_dag.autoRegenerateDag,
                airflow_notes=airflow_dag.airflowNotes,
                sudo_user=airflow_dag.sudoUser,
                timezone=airflow_dag.timezone,
                email=airflow_dag.email,
                email_on_failure=airflow_dag.emailOnFailure,
                email_on_retries=airflow_dag.emailOnRetries,
                tags=airflow_dag.tags,
                sla_warning_time=airflow_dag.slaWarningTime,
                retry_exponential_backoff=airflow_dag.retryExponentialBackoff,
                concurrency=airflow_dag.concurrency,
            )

            session.execute(query)
            session.commit()
        except SQLError as err:
            log.error(str(err))
            log.error(column)  # noqa: F821  # TODO(#164): column is undefined here

            result = str(err)
            return_code = 500

            # session.close()
            session.remove()
            return (result, return_code)

        # session.close()
        session.remove()

        tasks = airflow_dag.tasks
        for task in tasks:
            if (
                self.update_airflow_tasks(
                    dagname=dagname, task=task, current_user=current_user
                )
                == False
            ):
                result = "Error while updating/creating Airflow tasks. Please check logs on server"
                return_code = 400
                return (result, return_code)

        return (result, return_code)

    def delete_custom_airflow_dag(self, dagname, current_user):
        """Delete an Airflow import DAG"""
        log = logging.getLogger(self.logger)

        try:
            session = self.get_dbimport_session()
        except SQLError:
            self.disconnect_dbimport_db()
            return None

        result = "ok"
        return_code = 200

        log.info("User '%s' deleted Airflow import DAG '%s'", current_user, dagname)

        # Fetch the tableID as that is required to update/create data in import_columns
        row = (
            session.query(config_schema.AirflowCustomDAGs.dag_name)
            .select_from(config_schema.AirflowCustomDAGs)
            .filter(config_schema.AirflowCustomDAGs.dag_name == dagname)
            .one_or_none()
        )
        session.commit()

        if row == None:
            result = "Airflow DAG does not exist"
            return_code = 404
        else:
            (
                session.query(config_schema.AirflowCustomDAGs)
                .filter(config_schema.AirflowCustomDAGs.dag_name == dagname)
                .delete()
            )
            session.commit()

            self.delete_airflow_tasks(dagname)

        # session.close()
        session.remove()
        return (result, return_code)

    def bulk_delete_custom_airflow_dag(
        self, bulk_data: list[data_models.AirflowDAGPK], current_user
    ):
        """Bulk delete of Airflow custom Dags"""
        log = logging.getLogger(self.logger)

        if len(bulk_data) == 0:
            result_msg = {
                "result": "Body must contain a list of Airflow dags to delete"
            }
            return_code = 422
            return (result_msg, return_code)

        try:
            session = self.get_dbimport_session()
        except SQLError:
            self.disconnect_dbimport_db()
            return None

        delete_statement = delete(config_schema.AirflowCustomDAGs)
        delete_statement = delete_statement.where(
            config_schema.AirflowCustomDAGs.dag_name == bindparam("dag")
        )

        # Create a list of dicts that contain all the databas and table combinations that will be deleted
        table_list = []
        for tables in bulk_data:
            table_list.append({"dag": tables.name})

        session.connection().execute(delete_statement, table_list)
        session.commit()
        session.remove()

        return_code = 200
        result_msg = {"result": "Ok"}

        return (result_msg, return_code)

    def bulk_update_custom_airflow_dag(
        self, bulk_data: data_models.AirflowDAGBulkUpdate, current_user
    ):
        """Bulk update of Airflow custom Dags"""
        log = logging.getLogger(self.logger)

        if len(bulk_data.dags) == 0:
            result_msg = {
                "result": "dags must contain a list of Airflow dags to update"
            }
            return_code = 422
            return (result_msg, return_code)

        try:
            session = self.get_dbimport_session()
        except SQLError:
            self.disconnect_dbimport_db()
            return None

        export_tables = aliased(config_schema.ExportTables)
        airflow_custom_dags = aliased(config_schema.AirflowCustomDAGs)
        update_statement = update(airflow_custom_dags)
        update_statement = update_statement.where(
            airflow_custom_dags.dag_name == bindparam("dag")
        )

        # This will loop through only the fields specified in the Posted JSON. So if a field have got a default None value in dataModel validation,
        # that field wont be part of this loop
        for update_field in bulk_data.__pydantic_fields_set__:
            if update_field == "scheduleInterval":
                update_statement = update_statement.values(
                    schedule_interval=bulk_data.scheduleInterval
                )
            elif update_field == "operatorNotes":
                update_statement = update_statement.values(
                    operator_notes=bulk_data.operatorNotes
                )
            elif update_field == "autoRegenerateDag":
                update_statement = update_statement.values(
                    auto_regenerate_dag=bulk_data.autoRegenerateDag
                )
            elif update_field == "sudoUser":
                update_statement = update_statement.values(sudo_user=bulk_data.sudoUser)
            elif update_field == "timezone":
                update_statement = update_statement.values(timezone=bulk_data.timezone)
            elif update_field == "email":
                update_statement = update_statement.values(email=bulk_data.email)
            elif update_field == "emailOnFailure":
                update_statement = update_statement.values(
                    email_on_failure=bulk_data.emailOnFailure
                )
            elif update_field == "emailOnRetries":
                update_statement = update_statement.values(
                    email_on_retries=bulk_data.emailOnRetries
                )
            elif update_field == "slaWarningTime":
                update_statement = update_statement.values(
                    sla_warning_time=bulk_data.slaWarningTime
                )
            elif update_field == "concurrency":
                update_statement = update_statement.values(
                    concurrency=bulk_data.concurrency
                )

        # Create a list of dicts that contain all the databas/table combinations that will be updated
        table_list = []
        for tables in bulk_data.dags:
            table_list.append({"dag": tables.name})

        update_statement = update_statement.execution_options(synchronize_session=False)
        # 		print(updateStatement)

        session.connection().execute(update_statement, table_list)
        session.commit()
        session.remove()

        return_code = 200
        result_msg = {"result": "Ok"}

        return (result_msg, return_code)

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

        airflow_custom_dags = aliased(config_schema.AirflowCustomDAGs)
        result = "ok"
        return_code = 200

        if self.check_dag_name_availability(name=dagname, exclude_custom=True) == False:
            log.error(
                "Airflow Import DAG creation was denied as the name was already existing on custom or export DAG"
            )
            result = (
                "Ariflow DAG with the specified name already exists as Custom or Export"
            )
            return_code = 400
            return (result, return_code)

        log.debug(airflow_dag)
        log.info(
            "User '%s' updated/created Airflow custom DAG '%s'", current_user, dagname
        )

        # Set default values
        if airflow_dag.scheduleInterval is None:
            airflow_dag.scheduleInterval = "None"
        if airflow_dag.retries is None:
            airflow_dag.retries = "0"
        if airflow_dag.autoRegenerateDag is None:
            airflow_dag.autoRegenerateDag = "1"
        if airflow_dag.emailOnFailure is None:
            airflow_dag.emailOnFailure = "0"
        if airflow_dag.emailOnRetries is None:
            airflow_dag.emailOnRetries = "0"
        if airflow_dag.retryExponentialBackoff is None:
            airflow_dag.retryExponentialBackoff = "0"

        try:
            query = insert(config_schema.AirflowCustomDAGs).values(
                dag_name=dagname,
                schedule_interval=airflow_dag.scheduleInterval,
                retries=airflow_dag.retries,
                operator_notes=airflow_dag.operatorNotes,
                application_notes=airflow_dag.applicationNotes,
                auto_regenerate_dag=airflow_dag.autoRegenerateDag,
                airflow_notes=airflow_dag.airflowNotes,
                sudo_user=airflow_dag.sudoUser,
                timezone=airflow_dag.timezone,
                email=airflow_dag.email,
                email_on_failure=airflow_dag.emailOnFailure,
                email_on_retries=airflow_dag.emailOnRetries,
                tags=airflow_dag.tags,
                sla_warning_time=airflow_dag.slaWarningTime,
                retry_exponential_backoff=airflow_dag.retryExponentialBackoff,
                concurrency=airflow_dag.concurrency,
            )

            query = query.on_duplicate_key_update(
                dag_name=dagname,
                schedule_interval=airflow_dag.scheduleInterval,
                retries=airflow_dag.retries,
                operator_notes=airflow_dag.operatorNotes,
                application_notes=airflow_dag.applicationNotes,
                auto_regenerate_dag=airflow_dag.autoRegenerateDag,
                airflow_notes=airflow_dag.airflowNotes,
                sudo_user=airflow_dag.sudoUser,
                timezone=airflow_dag.timezone,
                email=airflow_dag.email,
                email_on_failure=airflow_dag.emailOnFailure,
                email_on_retries=airflow_dag.emailOnRetries,
                tags=airflow_dag.tags,
                sla_warning_time=airflow_dag.slaWarningTime,
                retry_exponential_backoff=airflow_dag.retryExponentialBackoff,
                concurrency=airflow_dag.concurrency,
            )

            session.execute(query)
            session.commit()
        except SQLError as err:
            log.error(str(err))
            log.error(column)  # noqa: F821  # TODO(#164): column is undefined here

            result = str(err)
            return_code = 500

            # session.close()
            session.remove()
            return (result, return_code)

        # session.close()
        session.remove()

        tasks = airflow_dag.tasks
        for task in tasks:
            if (
                self.update_airflow_tasks(
                    dagname=dagname, task=task, current_user=current_user
                )
                == False
            ):
                result = "Error while updating/creating Airflow tasks. Please check logs on server"
                return_code = 400
                return (result, return_code)

        return (result, return_code)
