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
import shutil
import subprocess
import sys
from datetime import datetime, timedelta
from typing import Any
from urllib.parse import quote

import jaydebeapi
import mysql.connector
import pandas as pd
import pendulum
import pymongo
import sqlalchemy as sa
from mysql.connector import errorcode
from sqlalchemy.orm import sessionmaker

from common import constants as constant
from common.Exceptions import SQLerror, invalidConfiguration, undevelopedFeature
from common.Singleton import Singleton
from ConfigReader import configuration
from DBImportConfig import decryption as decryption
from sourceSchemaReader import schemaReader


class config(metaclass=Singleton):
    def __init__(
        self, Hive_DB: str | None = None, Hive_Table: str | None = None
    ) -> None:
        logging.debug("Executing common_config.__init__()")

        self.Hive_DB = None
        self.Hive_Table = None
        if Hive_DB != None and self.Hive_DB == None:
            self.Hive_DB = Hive_DB
        if Hive_Table != None and self.Hive_Table == None:
            self.Hive_Table = Hive_Table

        self.mysql_conn: Any = None
        self.mysql_cursor: Any = None
        self.tempdir = None
        self.startDate = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        self.operationType: str | None = None
        self.dbAlias: str | None = None
        self.targetSchema: str | None = None
        self.targetTable: str | None = None

        # Variables used in lookupConnectionAlias
        self.db_mssql = False
        self.db_oracle = False
        self.db_mysql = False
        self.db_postgresql = False
        self.db_progress = False
        self.db_db2udb = False
        self.db_db2as400 = False
        self.db_mongodb = False
        self.db_cachedb = False
        self.db_snowflake = False
        self.db_awss3 = False
        self.db_informix = False
        self.db_sqlanywhere = False
        self.jdbc_url: str | None = None
        self.jdbc_hostname: str | None = None
        self.jdbc_port: str | None = None
        self.jdbc_database: str | None = None
        self.jdbc_username: str | None = None
        self.jdbc_password: str | None = None
        self.jdbc_classpath: str | None = None
        self.jdbc_driver: str | None = None
        self.jdbc_driver_for_python = None
        self.jdbc_classpath_for_python: str | list[str] | None = None
        self.jdbc_ad_domain = None
        self.jdbc_encrypt: bool | str | None = None
        self.jdbc_encrypt_string: str | None = None
        self.jdbc_trustedservercert: str | bool | None = None
        self.jdbc_trustedservercert_password: str | bool | None = None
        self.jdbc_hostincert: str | None = None
        self.jdbc_logintimeout: str | None = None
        self.jdbc_servertype: str | None = None
        self.jdbc_oracle_sid: str | None = None
        self.jdbc_oracle_servicename: str | None = None
        self.jdbc_force_column_lowercase: bool | None = None
        self.jdbc_environment: str | None = None
        self.seedFile = None
        self.mongo_client: Any = None
        self.mongoDB: Any = None
        self.mongoAuthSource: str | None = None
        self.awsS3assumeRole: str | None = None
        self.awsS3assumeRoleDuration: int | None = None
        self.awsS3proxyServer: str | None = None
        self.awsS3bucket: str | None = None
        self.awsS3region: str | None = None
        self.awsS3externalId: str | None = None
        self.awsS3fileFormat: str | None = None
        self.kerberos_initiated = False

        self.impalaDriver: str | None = None
        self.impalaClasspath: str | None = None
        self.impalaAddress: str | None = None
        self.impala_jdbc_conn: Any = None
        self.impalaJDBCCursor: Any = None

        self.sparkPathAppend = None
        self.sparkPysparkSubmitArgs = None
        self.sparkJarFiles = None
        self.sparkPackages = None
        self.sparkMajorVersion = None
        self.sparkPyFiles = None
        self.sparkMaster = None
        self.sparkDeployMode = None
        self.sparkYarnQueue = None
        self.sparkDynamicAllocation = None
        self.spark_min_executors = None
        self.sparkMaxExecutors = None
        self.sparkExecutorMemory = None
        self.sparkHiveLibrary = None
        self.sparkLogLevel = None

        self.operationTimestamp: datetime | None = None
        self.yarn_application_start: str | None = None

        self.sourceSchema = None
        self.tableType: str | None = None

        self.custom_max_query: str | None = None

        self.source_columns_df = pd.DataFrame()
        self.source_keys_df = pd.DataFrame()

        self.jdbc_conn: Any = None
        self.JDBCCursor: Any = None

        self.sourceSchema = schemaReader.source()

        # SQLAlchemy connection variables
        self.configDB: Any = None
        self.configDBSession: Any = None
        self.mysql_connection_details: dict[str, str] | None = None

        self.debugLogLevel = False
        if logging.root.level == 10:  # DEBUG
            self.debugLogLevel = True

        self.kerberosPrincipal = configuration.get("Kerberos", "principal")
        self.kerberosKeytab = configuration.get("Kerberos", "keytab")

        try:
            self.sparkMajorVersion = int(
                configuration.get("Spark", "spark_major_version", exit_on_error=False)
            )
        except invalidConfiguration:
            logging.warning(
                "Configuration option 'spark_major_version' under [Spark] in the configuration file is missing. Please update your config file"
            )
            self.sparkMajorVersion = 3
        except ValueError:
            logging.warning(
                "Configuration option 'spark_major_version' under [Spark] in the configuration file must be an integer. Please update your config file"
            )
            self.sparkMajorVersion = 3

        if self.sparkMajorVersion < 2 or self.sparkMajorVersion > 4:
            logging.error("Only Spark version 2, 3 and 4 is supported.")
            sys.exit(1)

        try:
            self.sparkLogLevel = configuration.get(
                "Spark", "spark_log_level", exit_on_error=False
            )
        except invalidConfiguration:
            pass

        try:
            self.sparkPackages = configuration.get(
                "Spark", "packages", exit_on_error=False
            )
        except invalidConfiguration:
            logging.warning(
                "Configuration option 'packages' under [Spark] in the configuration file is missing. Please update your config file"
            )

        self.sparkPathAppend = configuration.get("Spark", "path_append")
        self.sparkJarFiles = configuration.get("Spark", "jar_files")
        self.sparkPyFiles = configuration.get("Spark", "py_files")
        self.sparkMaster = configuration.get("Spark", "master")
        self.sparkDeployMode = configuration.get("Spark", "deployMode")
        self.sparkYarnQueue = configuration.get("Spark", "yarnqueue")
        self.sparkExecutorMemory = configuration.get("Spark", "executor_memory")
        self.sparkHiveLibrary = configuration.get("Spark", "hive_library")

        try:
            if (
                configuration.get(
                    "Spark", "s3_access_to_ozone", exit_on_error=False
                ).lower()
                == "true"
            ):
                self.sparkS3AccessToOzone = True
            else:
                self.sparkS3AccessToOzone = False
            self.sparkS3AccessKeyID = configuration.get("Spark", "s3_access_key_id")
            self.sparkS3SecretAccessKey = configuration.get(
                "Spark", "s3_secret_access_key"
            )

        except invalidConfiguration:
            self.sparkS3AccessToOzone = False
            self.sparkS3AccessKeyID = ""
            self.sparkS3SecretAccessKey = ""

        self.awsRegion = None
        try:
            self.awsRegion = configuration.get("AWS", "region", exit_on_error=False)
        except invalidConfiguration:
            pass

        if configuration.get("Spark", "dynamic_allocation").lower() == "true":
            self.sparkDynamicAllocation = True
        else:
            self.sparkDynamicAllocation = False

        self.crypto = decryption.crypto()

        # Sets and create a temporary directory
        self.tempdir = "/tmp/dbimport." + str(os.getpid()) + ".tmp"

        # If the temp dir exists, we just remove it as it's a leftover from another DBImport execution
        try:
            shutil.rmtree(self.tempdir)
        except FileNotFoundError:
            pass
        except PermissionError:
            logging.error(
                "The temporary directory (%s) already exists but cant be removed due to permission error"
                % (self.tempdir)
            )
            sys.exit(1)

        try:
            os.mkdir(self.tempdir)
        except OSError:
            logging.error(
                "Creation of the temporary directory %s failed" % self.tempdir
            )
            sys.exit(1)

        try:
            os.chmod(self.tempdir, 0o700)
        except OSError:
            logging.error("Error while changing permission on %s to 700" % self.tempdir)
            self.remove_temporary_files()
            sys.exit(1)

        self.reconnect_config_database(printReconnectMessage=False)

        logging.debug("Executing common_config.__init__() - Finished")

    def disconnect_config_database(self) -> None:
        """Closing connection against config database. Only used during debug and development"""
        logging.debug("Connection against MySQL Config database have been closed")
        self.mysql_cursor.close()
        self.mysql_conn.close()

    def get_mysql_credentials(self) -> dict[str, str]:
        # Fetch configuration about MySQL database and how to connect to it

        if self.mysql_connection_details != None:
            # This information is already fetched from the configuration. Just return the fetched values
            return self.mysql_connection_details

        mysql_hostname = configuration.get("Database", "mysql_hostname")
        mysql_port = configuration.get("Database", "mysql_port")
        mysql_database = configuration.get("Database", "mysql_database")
        mysql_username = configuration.get("Database", "mysql_username")
        mysql_password = configuration.get("Database", "mysql_password")

        if mysql_password == "" and mysql_username == "" and self.awsRegion != None:
            # No password is configured. Lets try the AWS get_secret function if configured
            aws_secret_name = None

            try:
                aws_secret_name = configuration.get(
                    "Database", "aws_secret_name", exit_on_error=False
                )
            except invalidConfiguration:
                raise invalidConfiguration(
                    "Credentials configuration to configuration database in config file is incorrect. Either use username/password or a AWS Secret Manager name"
                )
                # pass

            import boto3
            from botocore.exceptions import ClientError

            # Create a Secrets Manager client
            session = boto3.session.Session()
            client = session.client(
                service_name="secretsmanager", region_name=self.awsRegion
            )

            try:
                get_secret_value_response = client.get_secret_value(
                    SecretId=aws_secret_name
                )
            except ClientError as e:
                # For a list of exceptions thrown, see
                # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
                raise e

            rds_credentials = json.loads(get_secret_value_response["SecretString"])
            mysql_username = rds_credentials["username"]
            mysql_password = rds_credentials["password"]

        elif mysql_password == "" or mysql_username == "":
            raise invalidConfiguration(
                "Credentials configuration to configuration database in config file is incorrect. Either use username/password or a AWS Secret Manager name"
            )

        self.mysql_connection_details = {}
        self.mysql_connection_details["mysql_hostname"] = mysql_hostname
        self.mysql_connection_details["mysql_port"] = mysql_port
        self.mysql_connection_details["mysql_database"] = mysql_database
        self.mysql_connection_details["mysql_username"] = mysql_username
        self.mysql_connection_details["mysql_password"] = mysql_password

        # print(mysql_username)
        # print(mysql_password)

        return self.mysql_connection_details

    def reconnect_config_database(self, printReconnectMessage: bool = True) -> None:
        if self.mysql_conn == None or self.mysql_conn.is_connected() == False:
            if printReconnectMessage == True:
                logging.warn("Connection to MySQL have been lost. Reconnecting...")

            mysql_credentials = self.get_mysql_credentials()

            # Esablish a connection to the DBImport database in MySQL
            try:
                self.mysql_conn = mysql.connector.connect(
                    host=mysql_credentials["mysql_hostname"],
                    port=mysql_credentials["mysql_port"],
                    database=mysql_credentials["mysql_database"],
                    user=mysql_credentials["mysql_username"],
                    password=mysql_credentials["mysql_password"],
                )
            except mysql.connector.Error as err:
                if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                    logging.error("Something is wrong with your user name or password")
                elif err.errno == errorcode.ER_BAD_DB_ERROR:
                    logging.error("Database does not exist")
                else:
                    logging.error("%s" % err)
                logging.error(
                    "Error: There was a problem connecting to the MySQL database. Please check configuration and serverstatus and try again"
                )
                self.remove_temporary_files()
                sys.exit(1)
            else:
                self.mysql_cursor = self.mysql_conn.cursor(buffered=True)

    def get_atlas_jdbc_connection_data(
        self, db_alias: str | None = None
    ) -> dict[str, Any]:
        """Reads the extended information in jdbc_connections needed by Atlas (contact_info, owner and more)"""
        logging.debug("Executing common_config.get_atlas_jdbc_connection_data()")

        if db_alias == None:
            db_alias = self.dbAlias

        # Fetch data from jdbc_connection table
        query = "select contact_info, description, owner, atlas_discovery, atlas_include_filter, atlas_exclude_filter, atlas_last_discovery from jdbc_connections where dbalias = %s "
        logging.debug("Executing the following SQL: %s" % (query))
        self.mysql_cursor.execute(query, (db_alias,))

        if self.mysql_cursor.rowcount != 1:
            raise invalidConfiguration(
                "The requested connection alias cant be found in the 'jdbc_connections' table"
            )

        row = self.mysql_cursor.fetchone()
        return_dict = {}
        return_dict["contact_info"] = row[0]
        return_dict["description"] = row[1]
        return_dict["owner"] = row[2]
        return_dict["atlas_discovery"] = row[3]
        return_dict["atlas_include_filter"] = row[4]
        return_dict["atlas_exclude_filter"] = row[5]
        return_dict["atlas_last_discovery"] = row[6]

        if return_dict["contact_info"] == None:
            return_dict["contact_info"] = ""
        if return_dict["description"] == None:
            return_dict["description"] = ""
        if return_dict["owner"] == None:
            return_dict["owner"] = ""

        logging.debug(
            "Executing common_config.get_atlas_jdbc_connection_data() - Finished"
        )
        return return_dict

    def disconnect_sqlalchemy(self, logger: str = "") -> None:
        """Disconnects from the database and removes all sessions and engine"""
        log = logging.getLogger(logger)

        if self.configDB != None:
            log.info("Disconnecting from DBImport database")
            self.configDB.dispose()
            self.configDB = None

        self.configDBSession = None

    def connect_sqlalchemy(
        self, exit_if_failure: bool = True, logger: str = ""
    ) -> bool:
        log = logging.getLogger(logger)
        """ Connects to the configuration database with SQLAlchemy """

        if self.configDBSession != None:
            # If we already have a connection, we just say that it's ok....
            return True

        mysql_credentials = self.get_mysql_credentials()

        self.connect_str = "mysql+pymysql://%s:%s@%s:%s/%s" % (
            mysql_credentials["mysql_username"],
            quote(mysql_credentials["mysql_password"], safe=" +"),
            mysql_credentials["mysql_hostname"],
            mysql_credentials["mysql_port"],
            mysql_credentials["mysql_database"],
        )

        try:
            self.configDB = sa.create_engine(
                self.connect_str, echo=self.debugLogLevel, pool_pre_ping=True
            )
            self.configDB.connect()
            self.configDBSession = sessionmaker(bind=self.configDB)

        except sa.exc.OperationalError as err:
            logging.error("%s" % err)
            if exit_if_failure == True:
                self.remove_temporary_files()
                sys.exit(1)
            else:
                self.configDBSession = None
                self.configDB = None
                return False
        except:
            print("Unexpected error: ")
            print(sys.exc_info())
            if exit_if_failure == True:
                self.remove_temporary_files()
                sys.exit(1)
            else:
                self.configDBSession = None
                self.configDB = None
                return False

        else:
            return True

    def set_hive_table(self, Hive_DB: str, Hive_Table: str) -> None:
        """Sets the parameters to work against a new Hive database and table"""
        self.Hive_DB = Hive_DB.lower()
        self.Hive_Table = Hive_Table.lower()

    def get_mysql_cursor(self) -> Any:
        return self.mysql_cursor

    def get_mysql_connector(self) -> Any:
        return self.mysql_conn

    def remove_temporary_files(self) -> None:
        logging.debug("Executing common_config.remove_temporary_files()")

        # Remove the kerberos ticket file
        if self.kerberos_initiated == True:
            try:
                klist_command_list = ["kdestroy"]
                klist_proc = subprocess.Popen(
                    klist_command_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE
                )
                std_out, std_err = klist_proc.communicate()
            except:
                pass

        # We add this check just to make sure that self.tempdir contains a valid path and that no exception forces us to remove example /
        if len(self.tempdir) > 13 and self.tempdir.startswith("/tmp/"):
            try:
                shutil.rmtree(self.tempdir)
            except FileNotFoundError:
                # This can happen as we might call this function multiple times during an error and we dont want an exception because of that
                pass

        logging.debug("Executing common_config.remove_temporary_files() - Finished")

    def check_time_window(
        self, connection_alias: str, atlasDiscoveryMode: bool = False
    ) -> bool:
        logging.debug("Executing common_config.check_time_window()")
        if atlasDiscoveryMode == False:
            logging.info(
                "Checking if we are allowed to use this jdbc connection at this time"
            )

        self.timeZone = self.get_config_value("timezone")

        query = "select timewindow_start, timewindow_stop, timewindow_timezone from jdbc_connections where dbalias = %s"
        self.mysql_cursor.execute(query, (connection_alias,))
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor.statement))

        row = self.mysql_cursor.fetchone()
        hour = row[0]
        minute = row[1]

        # If there is a local timezone set on the JDBC connection, we use that instead of the configured default timezone
        if row[2] is not None and row[2].strip() != "":
            self.timeZone = row[2]

        current_time = pendulum.now(self.timeZone)  # Get time in configured timeZone
        time_window_start = None
        time_window_stop = None

        if hour != None:
            if hour == timedelta(days=1):
                hour = timedelta(days=0)

            time_window_start = current_time.set(
                hour=int(str(hour).split(":")[0]),
                minute=int(str(hour).split(":")[1]),
                second=int(str(hour).split(":")[2]),
            )

        if row[1] != None:
            time_window_stop = current_time.set(
                hour=int(str(minute).split(":")[0]),
                minute=int(str(minute).split(":")[1]),
                second=int(str(minute).split(":")[2]),
            )

        passed_midnight = False
        if (
            time_window_start != None
            and time_window_stop != None
            and time_window_start > time_window_stop
        ):
            # This happens if we pass midnight
            time_window_stop = time_window_stop.add(days=1)
            passed_midnight = True

        logging.debug("timeWindowStart: %s" % (time_window_start))
        logging.debug("timeWindowStop: %s" % (time_window_stop))

        if time_window_start == None and time_window_stop == None:
            if atlasDiscoveryMode == False:
                logging.info(
                    "SUCCESSFUL: This import is allowed to run at any time during the day."
                )
            return True
        elif time_window_start == None or time_window_stop == None:
            if atlasDiscoveryMode == False:
                logging.error(
                    "Atleast one of the TimeWindow settings are NULL in the database. Only way to disable the Time Window"
                )
                logging.error(
                    "function is to put NULL into both columns. Otherwise the configuration is marked as invalid and will exit"
                )
                logging.error("as it's not running inside a correct Time Window.")
                logging.error("Invalid TimeWindow configuration")
                self.remove_temporary_files()
                sys.exit(1)
            else:
                return False
        elif time_window_start == time_window_stop:
            if atlasDiscoveryMode == False:
                logging.error(
                    "The value in timewindow_start column is the same as the value in timewindow_stop."
                )
                logging.error("Invalid TimeWindow configuration")
                self.remove_temporary_files()
                sys.exit(1)
            else:
                return False
        else:
            allowed_time = False

            if current_time > time_window_start and current_time < time_window_stop:
                allowed_time = True

            if passed_midnight == True:
                # If we passed midnight, it means that we added one day to the stop time. But depending on what time it is, and when the
                # window starts, we might be on the previous days window and should be allowed to run. So we need to test for that aswell
                time_window_start = time_window_start.add(days=-1)
                time_window_stop = time_window_stop.add(days=-1)
                logging.debug("timeWindowStart: %s" % (time_window_start))
                logging.debug("timeWindowStop: %s" % (time_window_stop))

                if current_time > time_window_start and current_time < time_window_stop:
                    allowed_time = True

            if allowed_time == False:
                if atlasDiscoveryMode == False:
                    logging.error(
                        "We are not allowed to run this import outside the configured Time Window"
                    )
                    logging.info(
                        "    Current time:     %s (%s)"
                        % (current_time.to_time_string(), self.timeZone)
                    )
                    logging.info(
                        "    TimeWindow Start: %s"
                        % (time_window_start.to_time_string())
                    )
                    logging.info(
                        "    TimeWindow Stop:  %s" % (time_window_stop.to_time_string())
                    )
                    self.remove_temporary_files()
                    sys.exit(1)
                else:
                    return False
            else:
                if atlasDiscoveryMode == False:
                    logging.info(
                        "SUCCESSFUL: There is a configured Time Window for this operation, and we are running inside that window."
                    )

        logging.debug("    currentTime = %s" % (current_time.to_time_string()))
        logging.debug("    timeWindowStart = %s" % (time_window_start.to_time_string()))
        logging.debug("    timeWindowStop = %s" % (time_window_stop.to_time_string()))
        logging.debug("Executing common_config.check_time_window() - Finished")
        return True

    def check_connection_alias(self, connection_alias: str) -> bool:
        """Will check if the connection alias exists in the jdbc_connection table"""
        logging.debug("Executing common_config.check_connection_alias()")

        rc = False

        query = "select count(1) from jdbc_connections where dbalias = %s "
        logging.debug("Executing the following SQL: %s" % (query))
        self.mysql_cursor.execute(query, (connection_alias,))

        row = self.mysql_cursor.fetchone()
        if row[0] == 1:
            rc = True

        logging.debug("Executing common_config.check_connection_alias() - Finished")
        return rc

    def check_kerberos_ticket(self) -> bool:
        """Checks if there is a valid kerberos ticket or not. Runst 'klist -s' and checks the exitCode. Returns True or False"""
        logging.debug("Executing common_config.check_kerberos_ticket()")

        os.environ["KRB5CCNAME"] = "FILE:/tmp/krb5cc_%s_%s" % (os.getuid(), os.getpid())

        if self.kerberosPrincipal == "" and self.kerberosKeytab != "":
            # There is information about a keytab, but not the principal. So lets grab the first from the file
            kinit_command_list = ["klist", "-k", self.kerberosKeytab]
            try:
                kinit_proc = subprocess.Popen(
                    kinit_command_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE
                )
                std_out, std_err = kinit_proc.communicate()
                std_out = std_out.decode("utf-8").rstrip()
                std_err = std_err.decode("utf-8").rstrip()
            except FileNotFoundError:
                logging.error(
                    "kinit command was not found. Have you installed and configured Kerberos?"
                )
                self.remove_temporary_files()
                sys.exit(1)

            for line in std_out.splitlines():
                try:
                    if "@" in line.strip().split(" ")[1]:
                        # Get the first line with a @ in it. That should be the first principal in the keytab file
                        self.kerberosPrincipal = line.strip().split(" ")[1]
                        break
                except IndexError:
                    pass

            if self.kerberosPrincipal == "":
                logging.error("No kerberos principal can be found in the keytab file.")
                self.remove_temporary_files()
                sys.exit(1)

        elif self.kerberosPrincipal == "" or self.kerberosKeytab == "":
            logging.error(
                "The kerberos information is not correct in configuration file."
            )
            self.remove_temporary_files()
            sys.exit(1)

        logging.info("Initialize Kerberos ticket")
        self.kerberos_initiated = True

        kinit_command_list = [
            "kinit",
            "-kt",
            self.kerberosKeytab,
            self.kerberosPrincipal,
        ]
        kinit_proc = subprocess.Popen(
            kinit_command_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        std_out, std_err = kinit_proc.communicate()
        std_out = std_out.decode("utf-8").rstrip()
        std_err = std_err.decode("utf-8").rstrip()

        klist_command_list = ["klist", "-s"]
        klist_proc = subprocess.Popen(
            klist_command_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        std_out, std_err = klist_proc.communicate()
        std_out = std_out.decode("utf-8").rstrip()
        std_err = std_err.decode("utf-8").rstrip()

        if klist_proc.returncode == 0:
            return True
        else:
            return False

        logging.debug("Executing common_config.check_kerberos_ticket() - Finished")

    def encrypt_string(self, str_to_encrypt: str) -> str:
        self.crypto.set_private_key_file(
            configuration.get("Credentials", "private_key")
        )
        self.crypto.set_public_key_file(configuration.get("Credentials", "public_key"))

        encrypted_str = self.crypto.encrypt(str_to_encrypt)
        return encrypted_str

    def encrypt_user_password(
        self, connection_alias: str, username: str, password: str
    ) -> None:

        # Fetch public/private key from jdbc_connection table if it exists
        query = "select private_key_path, public_key_path from jdbc_connections where dbalias = %s "
        logging.debug("Executing the following SQL: %s" % (query))
        self.mysql_cursor.execute(query, (connection_alias,))

        if self.mysql_cursor.rowcount != 1:
            raise invalidConfiguration(
                "The requested connection alias cant be found in the 'jdbc_connections' table"
            )

        row = self.mysql_cursor.fetchone()

        private_key_file = row[0]
        public_key_file = row[1]

        if (
            private_key_file != None
            and public_key_file != None
            and private_key_file.strip() != ""
            and public_key_file.strip() != ""
        ):
            self.crypto.set_private_key_file(private_key_file)
            self.crypto.set_public_key_file(public_key_file)
        else:
            self.crypto.set_private_key_file(
                configuration.get("Credentials", "private_key")
            )
            self.crypto.set_public_key_file(
                configuration.get("Credentials", "public_key")
            )

        str_to_encrypt = "%s %s\n" % (username, password)

        encrypted_str = self.crypto.encrypt(str_to_encrypt)

        if encrypted_str != None and encrypted_str != "":
            query = "update jdbc_connections set credentials = %s where dbalias = %s "
            logging.debug("Executing the following SQL: %s" % (query))
            self.mysql_cursor.execute(query, (encrypted_str, connection_alias))
            self.mysql_conn.commit()

    def get_jdbc_driver_config(
        self, database_type: str, version: str
    ) -> tuple[str, str]:
        logging.debug("Executing common_config.get_jdbc_driver_config()")
        query = "select driver, classpath from jdbc_connections_drivers where database_type = %s and version = %s"
        logging.debug("Executing the following SQL: %s" % (query))
        self.mysql_cursor.execute(query, (database_type, version))

        if self.mysql_cursor.rowcount != 1:
            raise invalidConfiguration(
                "Error: Cant find JDBC driver with database_type as '%s' and version as '%s'"
                % (database_type, version)
            )

        row = self.mysql_cursor.fetchone()
        driver = row[0]
        class_path = row[1]

        if class_path == "add path to JAR file" or not class_path.startswith("/"):
            raise invalidConfiguration(
                "Error: You need to specify the full path to the JAR files in the table 'jdbc_connections_drivers'"
            )

        logging.debug("Executing common_config.get_jdbc_driver_config() - Finished")
        return driver, class_path

    def print_connection_alias_details(self) -> None:
        logging.debug("Executing common_config.print_connection_alias_details()")

        query = "select dbalias from jdbc_connections"
        logging.debug("Executing the following SQL: %s" % (query))
        self.mysql_cursor.execute(query, [])

        counter = 0
        for row in self.mysql_cursor.fetchall():
            counter = counter + 1
            try:
                self.lookup_connection_alias(
                    connection_alias=row[0], exceptionIfFailureToDecrypt=False
                )
            except:
                pass
            else:
                if self.jdbc_username == None:
                    self.jdbc_username = ""
                if self.jdbc_password == None:
                    self.jdbc_password = ""

                print(
                    "£%s£¤£%s£¤£%s£¤£DBImport£"
                    % (self.dbAlias, self.jdbc_username, self.jdbc_password)
                )

                if 0 == 1:
                    if "," in self.jdbc_username:
                        print("self.jdbc_username contains ,")
                        print(
                            "'%s','%s','%s','%s'"
                            % (
                                self.dbAlias,
                                self.jdbc_username,
                                self.jdbc_password,
                                self.jdbc_url,
                            )
                        )
                    if "'" in self.jdbc_username:
                        print("self.jdbc_username contains '")
                        print(
                            "'%s','%s','%s','%s'"
                            % (
                                self.dbAlias,
                                self.jdbc_username,
                                self.jdbc_password,
                                self.jdbc_url,
                            )
                        )
                    if "£" in self.jdbc_username:
                        print("self.jdbc_username contains £")
                        print(
                            "'%s','%s','%s','%s'"
                            % (
                                self.dbAlias,
                                self.jdbc_username,
                                self.jdbc_password,
                                self.jdbc_url,
                            )
                        )
                    if "¤" in self.jdbc_username:
                        print("self.jdbc_username contains ¤")
                        print(
                            "'%s','%s','%s','%s'"
                            % (
                                self.dbAlias,
                                self.jdbc_username,
                                self.jdbc_password,
                                self.jdbc_url,
                            )
                        )

                    if "'" in self.jdbc_password:
                        print("self.jdbc_password contains '")
                        print(
                            "'%s','%s','%s','%s'"
                            % (
                                self.dbAlias,
                                self.jdbc_username,
                                self.jdbc_password,
                                self.jdbc_url,
                            )
                        )
                    if "£" in self.jdbc_password:
                        print("self.jdbc_password contains £")
                        print(
                            "'%s','%s','%s','%s'"
                            % (
                                self.dbAlias,
                                self.jdbc_username,
                                self.jdbc_password,
                                self.jdbc_url,
                            )
                        )
                    if "¤" in self.jdbc_username:
                        print("self.jdbc_username contains ¤")
                        print(
                            "'%s','%s','%s','%s'"
                            % (
                                self.dbAlias,
                                self.jdbc_username,
                                self.jdbc_password,
                                self.jdbc_url,
                            )
                        )

                    if "," in self.jdbc_url:
                        print("self.jdbc_url contains ,")
                        print(
                            "'%s','%s','%s','%s'"
                            % (
                                self.dbAlias,
                                self.jdbc_username,
                                self.jdbc_password,
                                self.jdbc_url,
                            )
                        )
                    if "£" in self.jdbc_url:
                        print("self.jdbc_url contains £")
                        print(
                            "'%s','%s','%s','%s'"
                            % (
                                self.dbAlias,
                                self.jdbc_username,
                                self.jdbc_password,
                                self.jdbc_url,
                            )
                        )
                    if "¤" in self.jdbc_url:
                        print("self.jdbc_url contains ¤")
                        print(
                            "'%s','%s','%s','%s'"
                            % (
                                self.dbAlias,
                                self.jdbc_username,
                                self.jdbc_password,
                                self.jdbc_url,
                            )
                        )

        print()

        logging.debug(
            "Executing common_config.print_connection_alias_details() - Finished"
        )

    def lookup_connection_alias(
        self,
        connection_alias: str,
        decryptCredentials: bool = True,
        copySlave: bool = False,
        exceptionIfFailureToDecrypt: bool = True,
        jdbcURL: str | None = None,
        quiet: bool = False,
    ) -> None:
        logging.debug("Executing common_config.lookup_connection_alias()")

        exit_after_function = False
        self.dbAlias = connection_alias
        self.atlasJdbcSourceSupport = False

        self.db_mssql = False
        self.db_oracle = False
        self.db_mysql = False
        self.db_postgresql = False
        self.db_progress = False
        self.db_db2udb = False
        self.db_db2as400 = False
        self.db_mongodb = False
        self.db_cachedb = False
        self.db_snowflake = False
        self.db_awss3 = False
        self.jdbc_servertype = None

        # Fetch data from jdbc_connection table
        if jdbcURL != None and decryptCredentials == False:
            # This is required from RestAPI when looking up all connections and returning a full list including database types. No need to do the lookup here as well
            self.jdbc_url = jdbcURL
        else:
            query = "select jdbc_url, credentials, private_key_path, public_key_path, environment from jdbc_connections where dbalias = %s "
            logging.debug("Executing the following SQL: %s" % (query))
            self.mysql_cursor.execute(query, (connection_alias,))

            if self.mysql_cursor.rowcount != 1:
                raise invalidConfiguration(
                    "The requested connection alias cant be found in the 'jdbc_connections' table"
                )

            row = self.mysql_cursor.fetchone()

            self.jdbc_url = row[0]
            jdbc_credentials = row[1]
            private_key_file = row[2]
            public_key_file = row[3]
            self.jdbc_environment = row[4]

        if decryptCredentials == True and copySlave == False:
            if jdbc_credentials is not None and jdbc_credentials.startswith(
                "arn:aws:secretsmanager:"
            ):
                # This jdbc connection have its key stored in AWS Secrets Manager
                import boto3
                from botocore.exceptions import ClientError

                # Create a Secrets Manager client
                session = boto3.session.Session()
                client = session.client(
                    service_name="secretsmanager", region_name=self.awsRegion
                )

                try:
                    get_secret_value_response = client.get_secret_value(
                        SecretId=jdbc_credentials
                    )
                except ClientError as e:
                    # For a list of exceptions thrown, see
                    # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
                    raise e

                jdbc_secrets = json.loads(get_secret_value_response["SecretString"])
                credentials: str | None = "%s %s" % (
                    jdbc_secrets["username"],
                    jdbc_secrets["password"],
                )
                # print(json.loads(get_secret_value_response['SecretString']))
                # mysql_username = rds_credentials["username"]
                # mysql_password = rds_credentials["password"]
                # self.remove_temporary_files()
                # sys.exit(1)

            else:
                if (
                    private_key_file != None
                    and public_key_file != None
                    and private_key_file.strip() != ""
                    and public_key_file.strip() != ""
                ):
                    self.crypto.set_private_key_file(private_key_file)
                    self.crypto.set_public_key_file(public_key_file)
                else:
                    self.crypto.set_private_key_file(
                        configuration.get("Credentials", "private_key")
                    )
                    self.crypto.set_public_key_file(
                        configuration.get("Credentials", "public_key")
                    )

                credentials = self.crypto.decrypt(jdbc_credentials)

            if credentials == None and exceptionIfFailureToDecrypt == True:
                raise invalidConfiguration(
                    "Cant decrypt username and password. Check private/public key in config file"
                )

            if credentials != None:
                try:
                    self.jdbc_username = credentials.split(" ")[0]
                    self.jdbc_password = credentials.split(" ")[1]
                except IndexError:
                    raise invalidConfiguration(
                        "Cant decrypt username and password. Please encrypt new credentials with the 'manage --encryptCredentials' function"
                    )

                # Sets a creates the password file that is used by sqoop and other tools
                self.jdbc_password_file = self.tempdir + "/jdbc_passwd"
                f = open(self.jdbc_password_file, "w")
                f.write(self.jdbc_password)
                f.close()
                os.chmod(self.jdbc_password_file, 0o600)
            else:
                self.jdbc_username = None
                self.jdbc_password = None
                self.jdbc_password_file = None
        else:
            self.jdbc_username = None
            self.jdbc_password = None
            self.jdbc_password_file = None

        # Lookup Connection details based on JDBC STRING for all different types we support
        if self.jdbc_url.startswith("jdbc:sqlserver:"):
            self.atlasJdbcSourceSupport = True
            self.db_mssql = True
            self.jdbc_servertype = constant.MSSQL
            if jdbcURL == None:
                # jdbcURL have a value only when executed from restAPI server in order to get a full list of connection. Lookup of drivers is not needed in that case
                self.jdbc_driver, self.jdbc_classpath = self.get_jdbc_driver_config(
                    "SQL Server", "default"
                )
            self.jdbc_classpath_for_python = self.jdbc_classpath
            self.jdbc_hostname = self.jdbc_url[17:].split(":")[0].split(";")[0]

            try:
                self.jdbc_port = self.jdbc_url[17:].split(":")[1].split(";")[0]
            except:
                self.jdbc_port = "1433"
            if self.jdbc_port.isdigit() == False:
                self.jdbc_port = "1433"

            try:
                self.jdbc_database = self.jdbc_url.split("database=")[1].split(";")[0]
            except:
                if quiet == False:
                    logging.error("Cant determine database based on jdbc_string")
                exit_after_function = True

            try:
                self.jdbc_encrypt = (
                    self.jdbc_url.split("encrypt=")[1].split(";")[0].lower()
                )
                if self.jdbc_encrypt == "true":
                    self.jdbc_encrypt = True
            except:
                self.jdbc_encrypt = False

            try:
                self.jdbc_trustedservercert = self.jdbc_url.split(
                    "trustServerCertificate="
                )[1].split(";")[0]
            except:
                self.jdbc_trustedservercert = None

            try:
                self.jdbc_hostincert = self.jdbc_url.split("hostNameInCertificate=")[
                    1
                ].split(";")[0]
            except:
                self.jdbc_hostincert = None

            try:
                self.jdbc_logintimeout = self.jdbc_url.split("loginTimeout=")[1].split(
                    ";"
                )[0]
            except:
                self.jdbc_logintimeout = None

            # If all encrypt settings are available, we create an encryption string that will be used by the Schema Python Program
            if (
                self.jdbc_encrypt == True
                and self.jdbc_trustedservercert != None
                and self.jdbc_hostincert != None
                and self.jdbc_logintimeout != None
            ):
                self.jdbc_encrypt_string = (
                    "encrypt=true;trustServerCertificate="
                    + self.jdbc_trustedservercert
                    + ";hostNameInCertificate="
                    + self.jdbc_hostincert
                    + ";loginTimeout="
                    + self.jdbc_logintimeout
                )

        if self.jdbc_url.startswith("jdbc:jtds:sqlserver:"):
            self.atlasJdbcSourceSupport = True
            self.db_mssql = True
            self.jdbc_servertype = constant.MSSQL
            self.jdbc_force_column_lowercase = True
            if jdbcURL == None:
                # jdbcURL have a value only when executed from restAPI server in order to get a full list of connection. Lookup of drivers is not needed in that case
                self.jdbc_driver, self.jdbc_classpath = self.get_jdbc_driver_config(
                    "SQL Server", "jTDS"
                )
            self.jdbc_classpath_for_python = self.jdbc_classpath
            self.jdbc_hostname = self.jdbc_url[22:].split(":")[0].split(";")[0]
            try:
                self.jdbc_port = self.jdbc_url[22:].split(":")[1].split(";")[0]
            except:
                self.jdbc_port = "1433"
            if self.jdbc_port.isdigit() == False:
                self.jdbc_port = "1433"

            try:
                self.jdbc_database = self.jdbc_url.split("databaseName=")[1].split(";")[
                    0
                ]
            except:
                if quiet == False:
                    logging.error("Cant determine database based on jdbc_string")
                exit_after_function = True

        if self.jdbc_url.startswith("jdbc:oracle:"):
            self.atlasJdbcSourceSupport = True
            self.db_oracle = True
            self.jdbc_servertype = constant.ORACLE
            self.jdbc_force_column_lowercase = False
            if jdbcURL == None:
                # jdbcURL have a value only when executed from restAPI server in order to get a full list of connection. Lookup of drivers is not needed in that case
                self.jdbc_driver, self.jdbc_classpath = self.get_jdbc_driver_config(
                    "Oracle", "default"
                )
            self.jdbc_classpath_for_python = self.jdbc_classpath
            self.jdbc_database = "-"

            if self.jdbc_url.startswith("jdbc:oracle:thin:@ldap"):
                pattern = r"@ldap://([^:/]+):(\d+)/([^,]+),"
                r = re.search(pattern, self.jdbc_url)
                if r is None:
                    if quiet == False:
                        logging.error(
                            "jdbc_string for ldap must be of the form jdbc:oracle:thin@ldap://<ldap_host>:<ldap_port>/<db>"
                        )
                    exit_after_function = True
            else:
                try:
                    self.jdbc_hostname = self.jdbc_url.split("(HOST=")[1].split(")")[0]
                except:
                    if quiet == False:
                        logging.error("Cant determine hostname based on jdbc_string")
                    exit_after_function = True

                try:
                    self.jdbc_port = self.jdbc_url.split("(PORT=")[1].split(")")[0]
                except:
                    if quiet == False:
                        logging.error("Cant determine port based on jdbc_string")
                    exit_after_function = True

                try:
                    self.jdbc_oracle_sid = self.jdbc_url.split("(SID=")[1].split(")")[0]
                except:
                    self.jdbc_oracle_sid = None

                try:
                    self.jdbc_oracle_servicename = self.jdbc_url.split(
                        "(SERVICE_NAME="
                    )[1].split(")")[0]
                except:
                    self.jdbc_oracle_servicename = None

                if (
                    self.jdbc_oracle_sid == None
                    and self.jdbc_oracle_servicename == None
                ):
                    if quiet == False:
                        logging.error(
                            "Cant find either SID or SERVICE_NAME in Oracle URL"
                        )
                    exit_after_function = True

        if self.jdbc_url.startswith("jdbc:mysql:"):
            self.atlasJdbcSourceSupport = True
            self.db_mysql = True
            self.jdbc_servertype = constant.MYSQL
            self.jdbc_force_column_lowercase = True
            if jdbcURL == None:
                # jdbcURL have a value only when executed from restAPI server in order to get a full list of connection. Lookup of drivers is not needed in that case
                self.jdbc_driver, self.jdbc_classpath = self.get_jdbc_driver_config(
                    "MySQL", "default"
                )
            self.jdbc_classpath_for_python = self.jdbc_classpath

            self.jdbc_hostname = (
                self.jdbc_url[13:].split(":")[0].split(";")[0].split("/")[0]
            )
            try:
                self.jdbc_port = self.jdbc_url[13:].split(":")[1].split("/")[0]
            except:
                self.jdbc_port = "3306"
            if self.jdbc_port.isdigit() == False:
                self.jdbc_port = "3306"

            try:
                self.jdbc_database = (
                    self.jdbc_url[13:].split("/")[1].split(";")[0].split("?")[0]
                )
            except:
                if quiet == False:
                    logging.error("Cant determine database based on jdbc_string")
                exit_after_function = True

        if self.jdbc_url.startswith("jdbc:postgresql:"):
            self.atlasJdbcSourceSupport = True
            self.db_postgresql = True
            self.jdbc_servertype = constant.POSTGRESQL
            self.jdbc_force_column_lowercase = True
            if jdbcURL == None:
                # jdbcURL have a value only when executed from restAPI server in order to get a full list of connection. Lookup of drivers is not needed in that case
                self.jdbc_driver, self.jdbc_classpath = self.get_jdbc_driver_config(
                    "PostgreSQL", "default"
                )
            self.jdbc_classpath_for_python = self.jdbc_classpath

            self.jdbc_hostname = (
                self.jdbc_url[18:].split(":")[0].split(";")[0].split("/")[0]
            )
            try:
                self.jdbc_port = self.jdbc_url[18:].split(":")[1].split("/")[0]
            except:
                self.jdbc_port = "5432"
            if self.jdbc_port.isdigit() == False:
                self.jdbc_port = "5432"

            try:
                self.jdbc_database = self.jdbc_url[18:].split("/")[1].split(";")[0]
            except:
                if quiet == False:
                    logging.error("Cant determine database based on jdbc_string")
                exit_after_function = True

        if self.jdbc_url.startswith("jdbc:datadirect:openedge:"):
            self.atlasJdbcSourceSupport = True
            self.db_progress = True
            self.jdbc_servertype = constant.PROGRESS
            self.jdbc_force_column_lowercase = True
            if jdbcURL == None:
                # jdbcURL have a value only when executed from restAPI server in order to get a full list of connection. Lookup of drivers is not needed in that case
                self.jdbc_driver, self.jdbc_classpath = self.get_jdbc_driver_config(
                    "Progress DB", "default"
                )
            self.jdbc_classpath_for_python = self.jdbc_classpath

            self.jdbc_hostname = self.jdbc_url[27:].split(":")[0].split(";")[0]
            try:
                self.jdbc_port = self.jdbc_url[27:].split(":")[1].split(";")[0]
            except:
                self.jdbc_port = "9999"
            if self.jdbc_port.isdigit() == False:
                self.jdbc_port = "9999"

            try:
                self.jdbc_database = self.jdbc_url.split("databaseName=")[1].split(";")[
                    0
                ]
            except:
                if quiet == False:
                    logging.error("Cant determine database based on jdbc_string")
                exit_after_function = True

        if self.jdbc_url.startswith("jdbc:db2://"):
            self.atlasJdbcSourceSupport = True
            self.db_db2udb = True
            self.jdbc_servertype = constant.DB2_UDB
            self.jdbc_force_column_lowercase = False
            if jdbcURL == None:
                # jdbcURL have a value only when executed from restAPI server in order to get a full list of connection. Lookup of drivers is not needed in that case
                self.jdbc_driver, self.jdbc_classpath = self.get_jdbc_driver_config(
                    "DB2 UDB", "default"
                )
            self.jdbc_classpath_for_python = self.jdbc_classpath

            self.jdbc_hostname = (
                self.jdbc_url[11:].split(":")[0].split(";")[0].split("/")[0]
            )
            try:
                self.jdbc_port = self.jdbc_url[11:].split(":")[1].split("/")[0]
            except:
                self.jdbc_port = "50000"
            if self.jdbc_port.isdigit() == False:
                self.jdbc_port = "50000"

            try:
                self.jdbc_database = (
                    self.jdbc_url[11:].split("/")[1].split(";")[0].split(":")[0]
                )
            except:
                if quiet == False:
                    logging.error("Cant determine database based on jdbc_string")
                exit_after_function = True

            try:
                self.jdbc_encrypt = (
                    self.jdbc_url.split("sslConnection=")[1].split(";")[0].lower()
                )
                if self.jdbc_encrypt == "true":
                    self.jdbc_encrypt = True
            except:
                self.jdbc_encrypt = False

            try:
                self.jdbc_trustedservercert = self.jdbc_url.split(
                    "sslTrustStoreLocation="
                )[1].split(";")[0]
            except:
                self.jdbc_trustedservercert = False

            try:
                self.jdbc_trustedservercert_password = self.jdbc_url.split(
                    "sslTrustStorePassword="
                )[1].split(";")[0]
            except:
                self.jdbc_trustedservercert_password = False

        if self.jdbc_url.startswith("jdbc:as400://"):
            self.atlasJdbcSourceSupport = True
            self.db_db2as400 = True
            self.jdbc_servertype = constant.DB2_AS400
            self.jdbc_force_column_lowercase = False
            if jdbcURL == None:
                # jdbcURL have a value only when executed from restAPI server in order to get a full list of connection. Lookup of drivers is not needed in that case
                self.jdbc_driver, self.jdbc_classpath = self.get_jdbc_driver_config(
                    "DB2 AS400", "default"
                )
            self.jdbc_classpath_for_python = self.jdbc_classpath

            self.jdbc_hostname = (
                self.jdbc_url[13:].split(":")[0].split(";")[0].split("/")[0]
            )
            try:
                self.jdbc_port = self.jdbc_url[13:].split(":")[1].split("/")[0]
            except:
                self.jdbc_port = "446"
            if self.jdbc_port.isdigit() == False:
                self.jdbc_port = "446"

            try:
                self.jdbc_database = (
                    self.jdbc_url[13:].split("/")[1].split(";")[0].split(":")[0]
                )
            except:
                if quiet == False:
                    logging.error("Cant determine database based on jdbc_string")
                exit_after_function = True

        if self.jdbc_url.startswith("mongo://"):
            self.atlasJdbcSourceSupport = False
            self.db_mongodb = True
            self.jdbc_servertype = constant.MONGO
            if jdbcURL == None:
                # jdbcURL have a value only when executed from restAPI server in order to get a full list of connection. Lookup of drivers is not needed in that case
                self.jdbc_driver, self.jdbc_classpath = self.get_jdbc_driver_config(
                    "MongoDB", "default"
                )
            self.jdbc_classpath_for_python = self.jdbc_classpath

            self.jdbc_hostname = self.jdbc_url[8:].split(":")[0]
            self.jdbc_port = self.jdbc_url[8:].split(":")[1].split("/")[0]
            self.jdbc_database = self.jdbc_url[8:].split("/")[1].split("?")[0].strip()

            # Get all options in order to find authSource
            try:
                mongo_options = dict(
                    x.split("=") for x in self.jdbc_url.split("?")[1].split("&")
                )
                for k, v in mongo_options.items():
                    if k.lower() == "authsource":
                        self.mongoAuthSource = v
            except IndexError:
                pass

        if self.jdbc_url.startswith("jdbc:Cache://"):
            self.atlasJdbcSourceSupport = True
            self.db_cachedb = True
            self.jdbc_servertype = constant.CACHEDB
            self.jdbc_force_column_lowercase = False
            if jdbcURL == None:
                # jdbcURL have a value only when executed from restAPI server in order to get a full list of connection. Lookup of drivers is not needed in that case
                self.jdbc_driver, self.jdbc_classpath = self.get_jdbc_driver_config(
                    "CacheDB", "default"
                )
            self.jdbc_classpath_for_python = self.jdbc_classpath

            self.jdbc_hostname = (
                self.jdbc_url[13:].split(":")[0].split(";")[0].split("/")[0]
            )
            try:
                self.jdbc_port = self.jdbc_url[13:].split(":")[1].split("/")[0]
            except:
                self.jdbc_port = "1972"
            if self.jdbc_port.isdigit() == False:
                self.jdbc_port = "1972"

            try:
                self.jdbc_database = (
                    self.jdbc_url[13:].split("/")[1].split(";")[0].split(":")[0]
                )
            except:
                if quiet == False:
                    logging.error("Cant determine database based on jdbc_string")
                exit_after_function = True

        if self.jdbc_url.startswith("jdbc:snowflake://"):
            self.atlasJdbcSourceSupport = False
            self.db_snowflake = True
            self.jdbc_servertype = constant.SNOWFLAKE
            self.jdbc_force_column_lowercase = False
            if jdbcURL == None:
                # jdbcURL have a value only when executed from restAPI server in order to get a full list of connection. Lookup of drivers is not needed in that case
                self.jdbc_driver, self.jdbc_classpath = self.get_jdbc_driver_config(
                    "SnowFlake", "default"
                )
            self.jdbc_classpath_for_python = self.jdbc_classpath

            self.jdbc_hostname = self.jdbc_url[17:].split("/")[0]
            self.jdbc_port = None

            database_pos = self.jdbc_url.split("?")[1].find("db")
            if database_pos == -1:
                if quiet == False:
                    logging.error("Cant determine database based on jdbc_string")
                exit_after_function = True
            else:
                try:
                    self.jdbc_database = (
                        self.jdbc_url.split("?")[1][database_pos:]
                        .split("=")[1]
                        .split("&")[0]
                    )
                except:
                    if quiet == False:
                        logging.error("Cant determine database based on jdbc_string")
                    exit_after_function = True

        if self.jdbc_url.startswith("s3a://"):
            self.atlasJdbcSourceSupport = False
            self.db_awss3 = True
            self.jdbc_servertype = constant.AWS_S3
            self.jdbc_force_column_lowercase = False
            self.jdbc_driver = ""
            self.jdbc_classpath = ""
            self.jdbc_database = "-"

            self.awsS3bucket = self.jdbc_url.split(";")[0]

            try:
                self.awsS3assumeRole = self.jdbc_url.split(";assumeRole=")[1].split(
                    ";"
                )[0]
            except:
                self.awsS3assumeRole = None

            try:
                self.awsS3assumeRoleDuration = int(
                    self.jdbc_url.split(";assumeRoleDuration=")[1].split(";")[0]
                )
            except:
                self.awsS3assumeRoleDuration = None

            try:
                self.awsS3proxyServer = self.jdbc_url.split(";proxy=")[1].split(";")[0]
            except:
                self.awsS3proxyServer = None

            try:
                self.awsS3fileFormat = self.jdbc_url.split(";format=")[1].split(";")[0]
                if self.awsS3fileFormat != "parquet":
                    if quiet == False:
                        logging.error("Only parquet files are supported for AWS S3")
                    exit_after_function = True
            except:
                if quiet == False:
                    logging.error(
                        "For AWS S3, you need to specify what file format to use"
                    )
                exit_after_function = True

            try:
                self.awsS3region = self.jdbc_url.split(";region=")[1].split(";")[0]
            except:
                if quiet == False:
                    logging.error(
                        "For AWS S3, you need to specify what AWS region to work with in the jdbc_url string"
                    )
                exit_after_function = True

            try:
                self.awsS3externalId = self.jdbc_url.split(";externalId=")[1].split(
                    ";"
                )[0]
            except:
                self.awsS3externalId = None

        if self.jdbc_url.startswith("jdbc:informix-sqli://"):
            self.atlasJdbcSourceSupport = True
            self.db_informix = True
            self.jdbc_servertype = constant.INFORMIX
            self.jdbc_force_column_lowercase = False
            if jdbcURL == None:
                # jdbcURL have a value only when executed from restAPI server in order to get a full list of connection. Lookup of drivers is not needed in that case
                self.jdbc_driver, self.jdbc_classpath = self.get_jdbc_driver_config(
                    "Informix", "default"
                )
            self.jdbc_classpath_for_python = self.jdbc_classpath

            self.jdbc_hostname = (
                self.jdbc_url[21:].split(":")[0].split(";")[0].split("/")[0]
            )
            try:
                self.jdbc_port = self.jdbc_url[21:].split(":")[1].split("/")[0]
            except:
                self.jdbc_port = "1526"
            if self.jdbc_port.isdigit() == False:
                self.jdbc_port = "1526"

            try:
                self.jdbc_database = (
                    self.jdbc_url[21:].split("/")[1].split(";")[0].split(":")[0]
                )
            except:
                if quiet == False:
                    logging.error("Cant determine database based on jdbc_string")
                exit_after_function = True

            if "delimident=" not in self.jdbc_url.lower():
                if quiet == False:
                    logging.error(
                        "Informix requires DELIMIDENT=Y setting on the JDBC connection string"
                    )
                exit_after_function = True

        if self.jdbc_url.startswith("jdbc:sybase:Tds:"):
            self.atlasJdbcSourceSupport = True
            self.db_sqlanywhere = True
            self.jdbc_servertype = constant.SQLANYWHERE
            self.jdbc_force_column_lowercase = False
            if jdbcURL == None:
                # jdbcURL have a value only when executed from restAPI server in order to get a full list of connection. Lookup of drivers is not needed in that case
                self.jdbc_driver, self.jdbc_classpath = self.get_jdbc_driver_config(
                    "SQL Anywhere", "default"
                )
            self.jdbc_classpath_for_python = self.jdbc_classpath

            self.jdbc_hostname = self.jdbc_url[16:].split("?")[0]
            if ":" in self.jdbc_hostname:
                self.jdbc_port = self.jdbc_hostname.split(":")[1]
                self.jdbc_hostname = self.jdbc_hostname.split(":")[0]
            else:
                self.jdbc_port = "2638"

            if self.jdbc_port.isdigit() == False:
                self.jdbc_port = "2638"

        # Add Impala drivers to CP if configured in config file
        try:
            self.impalaDriver = configuration.get(
                "Impala", "driver", exit_on_error=False
            )
            self.impalaClasspath = configuration.get(
                "Impala", "class", exit_on_error=False
            )
            self.impalaAddress = configuration.get(
                "Impala", "address", exit_on_error=False
            )
            if (
                self.impalaDriver != None
                and self.impalaDriver.startswith("/path/to/") == False
            ):
                self.jdbc_classpath += ":%s" % (self.impalaDriver)
                self.jdbc_classpath_for_python = self.jdbc_classpath
        except:
            pass

        # Check to make sure that we have a supported JDBC string
        if self.jdbc_servertype == "":
            if quiet == False:
                logging.error(
                    "JDBC Connection '%s' is not supported." % (self.jdbc_url)
                )
            exit_after_function = True

        if copySlave == True:
            self.jdbc_hostname = None
            self.jdbc_port = None
            self.jdbc_database = None

        logging.debug("    db_mssql = %s" % (self.db_mssql))
        logging.debug("    db_oracle = %s" % (self.db_oracle))
        logging.debug("    db_mysql = %s" % (self.db_mysql))
        logging.debug("    db_postgresql = %s" % (self.db_postgresql))
        logging.debug("    db_progress = %s" % (self.db_progress))
        logging.debug("    db_db2udb = %s" % (self.db_db2udb))
        logging.debug("    db_db2as400 = %s" % (self.db_db2as400))
        logging.debug("    db_mongodb = %s" % (self.db_mongodb))
        logging.debug("    db_cachedb = %s" % (self.db_cachedb))
        logging.debug("    db_snowflake = %s" % (self.db_snowflake))
        logging.debug("    db_informix = %s" % (self.db_informix))
        logging.debug("    jdbc_servertype = %s" % (self.jdbc_servertype))
        logging.debug("    jdbc_url = %s" % (self.jdbc_url))
        logging.debug("    jdbc_username = %s" % (self.jdbc_username))
        logging.debug("    jdbc_password = <ENCRYPTED>")
        logging.debug("    jdbc_hostname = %s" % (self.jdbc_hostname))
        logging.debug("    jdbc_port = %s" % (self.jdbc_port))
        logging.debug("    jdbc_database = %s" % (self.jdbc_database))
        logging.debug("    jdbc_classpath = %s" % (self.jdbc_classpath))
        logging.debug(
            "    jdbc_classpath_for_python = %s" % (self.jdbc_classpath_for_python)
        )
        logging.debug("    jdbc_driver = %s" % (self.jdbc_driver))
        logging.debug("    jdbc_driver_for_python = %s" % (self.jdbc_driver_for_python))
        logging.debug("    jdbc_ad_domain = %s" % (self.jdbc_ad_domain))
        logging.debug("    jdbc_encrypt = %s" % (self.jdbc_encrypt))
        logging.debug("    jdbc_encrypt_string = %s" % (self.jdbc_encrypt_string))
        logging.debug("    jdbc_trustedservercert = %s" % (self.jdbc_trustedservercert))
        logging.debug(
            "    jdbc_trustedservercert_password = %s"
            % (self.jdbc_trustedservercert_password)
        )
        logging.debug("    jdbc_hostincert = %s" % (self.jdbc_hostincert))
        logging.debug("    jdbc_logintimeout = %s" % (self.jdbc_logintimeout))
        logging.debug("    jdbc_oracle_sid = %s" % (self.jdbc_oracle_sid))
        logging.debug(
            "    jdbc_oracle_servicename = %s" % (self.jdbc_oracle_servicename)
        )
        logging.debug("    self.awsS3assumeRole = %s" % (self.awsS3assumeRole))
        logging.debug(
            "    self.awsS3assumeRoleDuration = %s" % (self.awsS3assumeRoleDuration)
        )
        logging.debug("    self.awsS3proxyServer = %s" % (self.awsS3proxyServer))
        logging.debug("    self.awsS3externalId = %s" % (self.awsS3externalId))
        logging.debug("    self.awsS3region = %s" % (self.awsS3region))
        logging.debug("Executing common_config.lookup_connection_alias() - Finished")

        if exit_after_function == True:
            raise invalidConfiguration

    def get_jdbc_table_definition(
        self, source_schema: str, source_table: str, printInfo: bool = True
    ) -> None:
        logging.debug("Executing common_config.get_jdbc_table_definition()")
        if printInfo == True:
            logging.info("Reading SQL table definitions from source database")
        self.source_schema = source_schema
        self.source_table = source_table

        # Connect to the source database
        self.connect_to_jdbc()

        self.source_columns_df = self.sourceSchema.read_table_columns(
            self.JDBCCursor,
            server_type=self.jdbc_servertype,
            database=self.jdbc_database,
            schema=self.source_schema,
            table=self.source_table,
        )

        self.source_keys_df = self.sourceSchema.read_table_keys(
            self.JDBCCursor,
            server_type=self.jdbc_servertype,
            database=self.jdbc_database,
            schema=self.source_schema,
            table=self.source_table,
        )

        self.source_index_df = self.sourceSchema.read_table_index(
            self.JDBCCursor,
            server_type=self.jdbc_servertype,
            database=self.jdbc_database,
            schema=self.source_schema,
            table=self.source_table,
        )

        try:
            self.tableType = self.sourceSchema.get_jdbc_table_type(
                self.jdbc_servertype, self.source_columns_df.iloc[0]["TABLE_TYPE"]
            )
        except IndexError:
            self.tableType = "unknown"

        logging.debug("Executing common_config.getSourceTableDefinition() - Finished")

    def reconnect_jdbc(self, logger: str = "") -> None:
        self.disconnect_from_jdbc(logger=logger)
        self.connect_to_jdbc(logger=logger)

    def disconnect_from_jdbc(self, logger: str = "") -> None:
        log = logging.getLogger(logger)
        log.debug("Disconnect from JDBC database")

        if self.db_mongodb == True:
            # This is a MongoDB connection. Lets rediret to disconnectFromMongo instead
            return self.disconnect_from_mongo()

        try:
            self.JDBCCursor.close()
            self.jdbc_conn.close()

        except AttributeError:
            pass

        except Exception as exception:
            log.debug("Unknown error during disconnection from JDBC database:")
            log.debug(exception.message())
            pass

        self.JDBCCursor = None

    def connect_to_impala(self, logger: str = "") -> None:
        log = logging.getLogger(logger)
        if self.impalaDriver == None:
            log.info("Impala is not configured")
            return

        if self.impalaJDBCCursor == None:
            log.info("Connecting to Impala")
            log.debug("Connecting to Impala over JDBC")
            log.debug("   impalaDriver = %s" % (self.impalaDriver))
            log.debug("   impalaClasspath = %s" % (self.impalaClasspath))
            log.debug("   impalaAddress = %s" % (self.impalaAddress))

            try:
                jdbc_credentials = [self.jdbc_username, self.jdbc_password]

                self.impala_jdbc_conn = jaydebeapi.connect(
                    self.impalaClasspath, self.impalaAddress, [], self.impalaDriver
                )
                self.impalaJDBCCursor = self.impala_jdbc_conn.cursor()
                log.info("Connected to Impala")
            except Exception as exception:
                log.error("Connection to Impala failed with the following error:")
                log.error(exception.message())

    def disconnect_from_impala(self, logger: str = "") -> None:
        log = logging.getLogger(logger)
        log.info("Disconnecting from Impala")

        try:
            self.impalaJDBCCursor.close()
            self.impala_jdbc_conn.close()

        except AttributeError:
            pass

        except Exception as exception:
            log.info("Unknown error during disconnection from Impala:")
            log.info(exception.message())
            pass

        self.impalaJDBCCursor = None

    def connect_to_jdbc(
        self,
        allJarFiles: bool = False,
        exitIfFailure: bool = True,
        logger: str = "",
        printError: bool = True,
    ) -> bool:
        log = logging.getLogger(logger)

        if self.db_mongodb == True:
            # This is a MongoDB connection. Lets rediret to connectToMongo instead
            return self.connect_to_mongo(exit_if_failure=exitIfFailure, logger=logger)

        if allJarFiles == True:
            query = "select classpath from jdbc_connections_drivers"
            log.debug("Executing the following SQL: %s" % (query))
            self.mysql_cursor.execute(
                query,
            )

            self.jdbc_classpath_for_python = []
            for row in self.mysql_cursor.fetchall():
                if row[0] != "add path to JAR file":
                    self.jdbc_classpath_for_python.append(row[0])

        if self.jdbc_driver == None:
            log.error(
                "There is no support for the configured JDBC Connection on this db alias. Please check configuration"
            )
            self.remove_temporary_files()
            sys.exit(1)

        if self.JDBCCursor == None:
            log.debug("Connecting to database over JDBC")
            log.debug("   jdbc_username = %s" % (self.jdbc_username))
            log.debug("   jdbc_password = <ENCRYPTED>")
            log.debug("   jdbc_driver = %s" % (self.jdbc_driver))
            log.debug("   jdbc_url = %s" % (self.jdbc_url))
            log.debug(
                "   jdbc_classpath_for_python = %s" % (self.jdbc_classpath_for_python)
            )

            jdbc_credentials = [self.jdbc_username, self.jdbc_password]
            try:
                self.jdbc_conn = jaydebeapi.connect(
                    self.jdbc_driver,
                    self.jdbc_url,
                    jdbc_credentials,
                    self.jdbc_classpath_for_python,
                )
                self.JDBCCursor = self.jdbc_conn.cursor()
            except TypeError as exception:
                if printError == True:
                    log.error(
                        "Connection to database over JDBC failed with the following error:"
                    )
                    log.error(str(exception))
                if exitIfFailure == True:
                    self.remove_temporary_files()
                    sys.exit(1)
                else:
                    return False
            except Exception as exception:
                if printError == True:
                    log.error(
                        "Connection to database over JDBC failed with the following error:"
                    )
                    log.error(exception.message())
                if exitIfFailure == True:
                    self.remove_temporary_files()
                    sys.exit(1)
                else:
                    return False

        return True

    def get_jdbc_column_max_value(
        self, source_schema: str, source_table: str, column: str
    ) -> Any:
        logging.debug("Executing common_config.get_jdbc_column_max_value()")

        self.connect_to_jdbc()
        query = None

        if self.db_mssql == True:
            query = "select max(%s) from [%s].[%s].[%s]" % (
                column,
                self.jdbc_database,
                source_schema,
                source_table,
            )

        if self.db_oracle == True:
            query = 'select max(%s) from "%s"."%s"' % (
                column,
                source_schema.upper(),
                source_table.upper(),
            )

        if self.db_mysql == True:
            query = "select max(%s) from %s" % (column, source_table)

        if self.db_postgresql == True:
            query = 'select max(%s) from "%s"."%s"' % (
                column,
                source_schema.lower(),
                source_table.lower(),
            )

        if self.db_progress == True:
            query = 'select max(%s) from "%s"."%s"' % (
                column,
                source_schema,
                source_table,
            )

        if self.db_db2udb == True:
            query = 'select max(%s) from "%s"."%s"' % (
                column,
                source_schema,
                source_table,
            )

        if self.db_db2as400 == True:
            query = 'select max(%s) from "%s"."%s"' % (
                column,
                source_schema,
                source_table,
            )

        if self.db_snowflake == True:
            query = 'select max("%s") from "%s"."%s"' % (
                column,
                source_schema,
                source_table,
            )

        if self.db_informix == True:
            query = 'select max("%s") from "%s"."%s"' % (
                column,
                source_schema,
                source_table,
            )

        if self.custom_max_query != None:
            # If a custom query is configured, we just use that instead and ignore the config above
            query = self.custom_max_query
            logging.info(
                "Using a custom query to get Max value from source table (%s)" % (query)
            )

        self.JDBCCursor.execute(query)
        logging.debug("SQL Statement executed: %s" % (query))
        row = self.JDBCCursor.fetchone()

        logging.info(
            "Max value that will be used in the incremental import is '%s'" % (row[0])
        )

        return row[0]

        logging.debug("Executing common_config.get_jdbc_column_max_value() - Finished")

    def truncate_jdbc_table(self, schema: str, table: str) -> None:
        """Truncates a table on the JDBC connection"""
        logging.debug("Executing common_config.truncate_jdbc_table()")
        self.connect_to_jdbc()
        query = None

        if self.db_oracle == True:
            query = 'truncate table "%s"."%s"' % (schema.upper(), table.upper())

        if self.db_mssql == True:
            query = "truncate table %s.%s" % (schema, table)

        if self.db_db2udb == True:
            query = 'truncate table "%s"."%s" immediate' % (
                schema.upper(),
                table.upper(),
            )

        if self.db_mysql == True:
            query = "truncate table %s" % (table)

        if self.db_postgresql == True:
            query = "truncate table %s.%s" % (schema.lower(), table.lower())

        if self.db_snowflake == True:
            query = 'truncate table "%s"."%s"' % (schema, table)

        if query == None:
            raise undevelopedFeature(
                "There is no support for this database type in common_config.truncate_jdbc_table()"
            )

        logging.debug("SQL Statement executed: %s" % (query))
        self.JDBCCursor.execute(query)

        logging.debug("Executing common_config.truncate_jdbc_table() - Finished")

    def check_jdbc_table(self, schema: str, table: str) -> bool:
        """Checks if a table exists on the JDBC connections. Return True or False"""
        logging.debug("Executing common_config.check_jdbc_table()")
        self.connect_to_jdbc()
        query = None

        if self.db_oracle == True:
            query = (
                "select count(owner) from all_tables where owner = '%s' and table_name = '%s'"
                % (schema.upper(), table.upper())
            )

        if self.db_mssql == True:
            query = (
                "select count(table_name) from INFORMATION_SCHEMA.COLUMNS where table_schema = '%s' and table_name = '%s'"
                % (schema, table)
            )

        if self.db_db2udb == True:
            query = (
                "select count(name) from SYSIBM.SYSTABLES where upper(creator) = '%s' and upper(name) = '%s'"
                % (schema.upper(), table.upper())
            )

        if self.db_mysql == True:
            query = (
                "select count(table_name) from information_schema.tables where table_schema = '%s' and table_name = '%s'"
                % (self.jdbc_database, table)
            )

        if self.db_postgresql == True:
            query = (
                "select count(table_name) from information_schema.tables where table_catalog = '%s' and table_schema = '%s' and table_name = '%s'"
                % (self.jdbc_database, schema.lower(), table.lower())
            )

        if self.db_snowflake == True:
            query = (
                "select count(table_name) from information_schema.tables where table_catalog = '%s' and table_schema = '%s' and table_name = '%s'"
                % (self.jdbc_database, schema, table)
            )

        if query == None:
            raise undevelopedFeature(
                "There is no support for this database type in common_config.check_jdbc_table()"
            )

        logging.debug("SQL Statement executed: %s" % (query))
        self.JDBCCursor.execute(query)
        row = self.JDBCCursor.fetchone()

        table_exists = False

        if int(row[0]) > 0:
            table_exists = True

        logging.debug("Executing common_config.check_jdbc_table() - Finished")
        return table_exists

    def execute_jdbc_query(self, query: str) -> pd.DataFrame:
        """Executes a query against the JDBC database and return the values in a Pandas DF"""
        logging.debug("Executing common_config.execute_jdbc_query()")

        logging.debug("Query to execute: %s" % (query))
        try:
            self.connect_to_jdbc()
            self.JDBCCursor.execute(query)
        except jaydebeapi.DatabaseError as errMsg:
            raise SQLerror(errMsg)

        result_df = pd.DataFrame()
        try:
            result_df = pd.DataFrame(self.JDBCCursor.fetchall())
            if result_df.empty == False:
                result_df_columns = []
                for columns in self.JDBCCursor.description:
                    result_df_columns.append(
                        columns[0]
                    )  # Name of the column is in the first position
                result_df.columns = result_df_columns
        except jaydebeapi.Error:
            logging.debug(
                "An error was raised during JDBCCursor.fetchall(). This happens during SQL operations that dont return any rows like 'create table'"
            )

        # Set the correct column namnes in the DataFrame

        logging.debug("Executing common_config.execute_jdbc_query() - Finished")
        return result_df

    def execute_impala_query(self, query: str) -> pd.DataFrame:
        """Executes a query against Impala and return the values in a Pandas DF"""
        logging.debug("Executing common_config.execute_impala_query()")

        logging.debug("Query to execute: %s" % (query))
        try:
            self.connect_to_impala()
            self.impalaJDBCCursor.execute(query)
        except jaydebeapi.DatabaseError as errMsg:
            raise SQLerror(errMsg)

        result_df = pd.DataFrame()
        try:
            result_df = pd.DataFrame(self.impalaJDBCCursor.fetchall())
            if result_df.empty == False:
                result_df_columns = []
                for columns in self.impalaJDBCCursor.description:
                    result_df_columns.append(
                        columns[0]
                    )  # Name of the column is in the first position
                result_df.columns = result_df_columns
        except jaydebeapi.Error:
            logging.debug(
                "An error was raised during impalaJDBCCursor.fetchall(). This happens during SQL operations that dont return any rows like 'create table'"
            )

        # Set the correct column namnes in the DataFrame

        logging.debug("Executing common_config.execute_impala_query() - Finished")
        return result_df

    def get_jdbc_table_row_count(
        self, source_schema: str, source_table: str, whereStatement: str | None = None
    ) -> int:
        logging.debug("Executing common_config.get_jdbc_table_row_count()")

        self.connect_to_jdbc()
        query = None

        if self.db_mssql == True:
            query = "select count_big(1) from [%s].[%s].[%s]" % (
                self.jdbc_database,
                source_schema,
                source_table,
            )

        if self.db_oracle == True:
            query = 'select count(1) from "%s"."%s"' % (
                source_schema.upper(),
                source_table.upper(),
            )

        if self.db_mysql == True:
            query = "select count(1) from %s" % source_table

        if self.db_postgresql == True:
            query = 'select count(1) from "%s"."%s"' % (
                source_schema.lower(),
                source_table.lower(),
            )

        if self.db_progress == True:
            query = 'select count(1) from "%s"."%s"' % (source_schema, source_table)

        if self.db_db2udb == True:
            query = 'select cast(count(1) as bigint) from "%s"."%s"' % (
                source_schema,
                source_table,
            )

        if self.db_db2as400 == True:
            query = 'select count(1) from "%s"."%s"' % (source_schema, source_table)

        if self.db_snowflake == True:
            query = 'select count(1) from "%s"."%s"' % (source_schema, source_table)

        if self.db_informix == True:
            query = "select count(1) from %s.%s" % (source_schema, source_table)

        if self.db_sqlanywhere == True:
            query = "select count(1) from %s.%s" % (source_schema, source_table)

        if whereStatement != None and whereStatement != "":
            query = query + " where " + whereStatement

        logging.debug("SQL Statement executed: %s" % (query))
        self.JDBCCursor.execute(query)
        row = self.JDBCCursor.fetchone()

        return int(row[0])

        logging.debug("Executing common_config.get_jdbc_table_row_count() - Finished")

    def drop_jdbc_table(self, schema: str, table: str) -> None:
        logging.debug("Executing common_config.drop_jdbc_table()")

        self.connect_to_jdbc()
        query = "drop table %s" % (
            self.get_jdbc_sql_from_table(schema=schema, table=table)
        )
        self.JDBCCursor.execute(query)

        logging.debug("Executing common_config.drop_jdbc_table() - Finished")

    def log_import_failure(
        self,
        error_text: str,
        severity: str,
        import_type: str | None = None,
        hive_db: str | None = None,
        hive_table: str | None = None,
    ) -> None:

        if hive_db == None:
            hive_db = self.Hive_DB
        if hive_table == None:
            hive_table = self.Hive_Table

        query = "insert into import_failure_log "
        query += (
            "( hive_db, hive_table, eventtime, severity, import_type, error_text ) "
        )
        query += "values "
        query += "( %s, %s, %s, %s, %s, %s )"

        logging.debug("SQL Statement executed: %s" % (query))
        self.mysql_cursor.execute(
            query,
            (
                hive_db,
                hive_table,
                datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
                severity,
                import_type,
                error_text,
            ),
        )
        self.mysql_conn.commit()

    def log_hive_column_add(
        self,
        column: str,
        columnType: str | None = None,
        description: str | None = None,
        hiveDB: str | None = None,
        hiveTable: str | None = None,
    ) -> None:
        if description == None:
            description = "Column '%s' added to table with type '%s'" % (
                column,
                columnType,
            )

        if hiveDB == None:
            hiveDB = self.Hive_DB
        if hiveTable == None:
            hiveTable = self.Hive_Table

        query = "insert into table_change_history "
        query += "( hive_db, hive_table, column_name, eventtime, event, value, description ) "
        query += "values "
        query += "( %s, %s, %s, %s, 'column_added', %s, %s )"

        logging.debug("SQL Statement executed: %s" % (query))
        self.mysql_cursor.execute(
            query,
            (
                hiveDB,
                hiveTable,
                column,
                datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
                columnType,
                description,
            ),
        )
        self.mysql_conn.commit()

    def log_hive_column_type_change(
        self,
        column: str,
        columnType: str,
        previous_columnType: str | None = None,
        description: str | None = None,
        hiveDB: str | None = None,
        hiveTable: str | None = None,
    ) -> None:

        if description == None:
            if previous_columnType == None:
                description = "Column '%s' type changed to %s" % (column, columnType)
            else:
                description = "Column '%s' type changed from %s to %s" % (
                    column,
                    previous_columnType,
                    columnType,
                )

        if hiveDB == None:
            hiveDB = self.Hive_DB
        if hiveTable == None:
            hiveTable = self.Hive_Table

        query = "insert into table_change_history "
        query += "( hive_db, hive_table, column_name, eventtime, event, previous_value, value, description ) "
        query += "values "
        query += "( %s, %s, %s, %s, 'column_type_change', %s, %s, %s )"

        logging.debug("SQL Statement executed: %s" % (query))
        self.mysql_cursor.execute(
            query,
            (
                hiveDB,
                hiveTable,
                column,
                datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
                previous_columnType,
                columnType,
                description,
            ),
        )
        self.mysql_conn.commit()

    def log_hive_column_rename(
        self,
        columnName: str,
        previous_columnName: str,
        description: str | None = None,
        hiveDB: str | None = None,
        hiveTable: str | None = None,
    ) -> None:

        if description == None:
            description = "Column '%s' renamed to %s" % (
                previous_columnName,
                columnName,
            )

        if hiveDB == None:
            hiveDB = self.Hive_DB
        if hiveTable == None:
            hiveTable = self.Hive_Table

        query = "insert into table_change_history "
        query += "( hive_db, hive_table, column_name, eventtime, event, previous_value, value, description ) "
        query += "values "
        query += "( %s, %s, %s, %s, 'column_rename', %s, %s, %s )"

        logging.debug("SQL Statement executed: %s" % (query))
        self.mysql_cursor.execute(
            query,
            (
                hiveDB,
                hiveTable,
                columnName,
                datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
                previous_columnName,
                columnName,
                description,
            ),
        )
        self.mysql_conn.commit()

    def log_jdbc_column_add(
        self,
        column: str,
        column_type: str | None = None,
        description: str | None = None,
        db_alias: str | None = None,
        database: str | None = None,
        schema: str | None = None,
        table: str | None = None,
    ) -> None:
        if description == None:
            description = "Column '%s' added to table with type '%s'" % (
                column,
                column_type,
            )

        query = "insert into jdbc_table_change_history "
        query += "( dbalias, db_name, schema_name, table_name, column_name, eventtime, event, value, description ) "
        query += "values "
        query += "( %s, %s, %s, %s, %s, %s, 'column_added', %s, %s )"

        logging.debug("SQL Statement executed: %s" % (query))
        self.mysql_cursor.execute(
            query,
            (
                db_alias,
                database,
                schema,
                table,
                column,
                datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
                column_type,
                description,
            ),
        )
        self.mysql_conn.commit()

    def log_jdbc_column_type_change(
        self,
        column: str,
        column_type: str,
        previous_column_type: str | None = None,
        description: str | None = None,
        db_alias: str | None = None,
        database: str | None = None,
        schema: str | None = None,
        table: str | None = None,
    ) -> None:

        if description == None:
            if previous_column_type == None:
                description = "Column '%s' type changed to %s" % (column, column_type)
            else:
                description = "Column '%s' type changed from %s to %s" % (
                    column,
                    previous_column_type,
                    column_type,
                )

        query = "insert into jdbc_table_change_history "
        query += "( dbalias, db_name, schema_name, table_name, column_name, eventtime, event, previous_value, value, description ) "
        query += "values "
        query += "( %s, %s, %s, %s, %s, %s, 'column_type_change', %s, %s, %s )"

        logging.debug("SQL Statement executed: %s" % (query))
        self.mysql_cursor.execute(
            query,
            (
                db_alias,
                database,
                schema,
                table,
                column,
                datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
                previous_column_type,
                column_type,
                description,
            ),
        )
        self.mysql_conn.commit()

    def log_jdbc_column_rename(
        self,
        column_name: str,
        previous_column_name: str,
        description: str | None = None,
        db_alias: str | None = None,
        database: str | None = None,
        schema: str | None = None,
        table: str | None = None,
    ) -> None:

        if description == None:
            description = "Column '%s' renamed to %s" % (
                previous_column_name,
                column_name,
            )

        query = "insert into jdbc_table_change_history "
        query += "( dbalias, db_name, schema_name, table_name, column_name, eventtime, event, previous_value, value, description ) "
        query += "values "
        query += "( %s, %s, %s, %s, %s, %s, 'column_rename', %s, %s, %s )"

        logging.debug("SQL Statement executed: %s" % (query))
        self.mysql_cursor.execute(
            query,
            (
                db_alias,
                database,
                schema,
                table,
                column_name,
                datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
                previous_column_name,
                column_name,
                description,
            ),
        )
        self.mysql_conn.commit()

    def get_quote_around_column(self) -> str:
        quote_around_column = ""
        if self.jdbc_servertype == constant.MSSQL:
            quote_around_column = '"'
        if self.jdbc_servertype == constant.ORACLE:
            quote_around_column = '"'
        if self.jdbc_servertype == constant.MYSQL:
            quote_around_column = "`"
        if self.jdbc_servertype == constant.POSTGRESQL:
            quote_around_column = '"'
        if self.jdbc_servertype == constant.PROGRESS:
            quote_around_column = '"'
        if self.jdbc_servertype == constant.DB2_UDB:
            quote_around_column = '"'
        if self.jdbc_servertype == constant.DB2_AS400:
            quote_around_column = '"'
        if self.jdbc_servertype == constant.SNOWFLAKE:
            quote_around_column = '"'
        if self.jdbc_servertype == constant.INFORMIX:
            quote_around_column = '"'
        if self.jdbc_servertype == constant.SQLANYWHERE:
            quote_around_column = "`"

        return quote_around_column

    def get_jdbc_upper_case(self) -> bool:
        upper_case = False
        if self.jdbc_servertype == constant.ORACLE:
            upper_case = True
        if self.jdbc_servertype == constant.DB2_UDB:
            upper_case = True

        return upper_case

    def get_jdbc_sql_from_table(self, schema: str, table: str) -> str:
        logging.debug("Executing common_config.get_jdbc_sql_from_table()")

        from_table = ""
        if self.jdbc_servertype == constant.MSSQL:
            from_table = "[%s].[%s].[%s]" % (self.jdbc_database, schema, table)
        if self.jdbc_servertype == constant.ORACLE:
            from_table = '"%s"."%s"' % (schema.upper(), table.upper())
        if self.jdbc_servertype == constant.MYSQL:
            from_table = "%s" % (table)
        if self.jdbc_servertype == constant.POSTGRESQL:
            from_table = '"%s"."%s"' % (schema.lower(), table.lower())
        if self.jdbc_servertype == constant.PROGRESS:
            from_table = '"%s"."%s"' % (schema, table)
        if self.jdbc_servertype == constant.DB2_UDB:
            from_table = '"%s"."%s"' % (schema, table)
        if self.jdbc_servertype == constant.DB2_AS400:
            from_table = '"%s"."%s"' % (schema, table)
        if self.jdbc_servertype == constant.SNOWFLAKE:
            from_table = '"%s"."%s"' % (schema, table)
        if self.jdbc_servertype == constant.INFORMIX:
            from_table = "%s.%s" % (schema, table)
        if self.jdbc_servertype == constant.SQLANYWHERE:
            from_table = "%s" % (table)

        logging.debug("Executing common_config.get_jdbc_sql_from_table() - Finished")
        return from_table

    def get_config_value_column(
        self, key: str, ignore_error_output: bool = False
    ) -> tuple[str | None, bool | None]:
        """Return the type of the column that have the value for the specified key"""

        value_column = ""
        bool_value = False

        if key in (
            "hive_remove_locks_by_force",
            "airflow_disable",
            "import_start_disable",
            "import_stage_disable",
            "export_start_disable",
            "export_stage_disable",
            "hive_validate_before_execution",
            "hive_print_messages",
            "import_process_empty",
            "hive_major_compact_after_merge",
            "hive_insert_only_tables",
            "hive_acid_with_clusteredby",
            "post_data_to_kafka",
            "post_data_to_kafka_extended",
            "post_data_to_rest",
            "post_data_to_rest_extended",
            "post_data_to_awssns",
            "post_data_to_awssns_extended",
            "post_airflow_dag_operations",
            "rest_verifyssl",
            "impala_invalidate_metadata",
            "airflow_aws_pool_to_instanceid",
            "airflow_create_pool_with_task",
            "timestamp_with_timezone",
        ):
            value_column = "valueInt"
            bool_value = True
        elif key in (
            "spark_max_executors",
            "import_default_sessions",
            "import_max_sessions",
            "export_default_sessions",
            "export_max_sessions",
            "atlas_discovery_interval",
            "airflow_major_version",
            "rest_timeout",
            "airflow_default_pool_size",
            "restserver_token_ttl",
        ):
            value_column = "valueInt"
        elif key in (
            "import_staging_table",
            "import_staging_database",
            "import_work_table",
            "import_work_database",
            "import_history_table",
            "import_history_database",
            "export_staging_database",
            "hive_validate_table",
            "airflow_aws_instanceids",
            "airflow_sudo_user",
            "airflow_dbimport_commandpath",
            "airflow_dag_directory",
            "airflow_dag_staging_directory",
            "timezone",
            "airflow_dag_file_group",
            "airflow_dag_file_permission",
            "airflow_dummy_task_queue",
            "airflow_url",
            "cluster_name",
            "hdfs_address",
            "hdfs_blocksize",
            "hdfs_basedir",
            "kafka_brokers",
            "kafka_saslmechanism",
            "kafka_securityprotocol",
            "kafka_topic",
            "kafka_trustcafile",
            "rest_url",
            "rest_trustcafile",
            "import_columnname_delete",
            "import_columnname_import",
            "import_columnname_insert",
            "import_columnname_iud",
            "import_columnname_update",
            "import_columnname_histtime",
            "import_columnname_source",
            "restserver_secret_key",
            "restserver_admin_user",
            "restserver_authentication_method",
            "post_data_to_awssns_topic",
        ):
            value_column = "valueStr"
        else:
            if ignore_error_output == False:
                # This is needed from the REST interface, as it's parsing everything in the database. And entries may exists that isnt a valid config anymore
                logging.error("There is no configuration with the name '%s'" % (key))
                self.remove_temporary_files()
                sys.exit(1)
            else:
                value_column = None
                bool_value = None

        return (value_column, bool_value)

    def get_config_value(self, key: str) -> Any:
        """Returns a value from the configuration table based on the supplied key. Value returned can be Int, Str or DateTime"""
        logging.debug("Executing common_config.get_config_value()")
        return_value = None
        bool_value = False

        value_column, bool_value = self.get_config_value_column(key)

        query = "select %s from configuration where configkey = '%s'" % (
            value_column,
            key,
        )

        logging.debug("SQL Statement executed: %s" % (query))
        self.mysql_cursor.execute(query)
        row = self.mysql_cursor.fetchone()
        self.mysql_conn.commit()

        try:
            if value_column == "valueInt":
                return_value = int(row[0])

            if value_column == "valueStr":
                return_value = row[0]
                if return_value == None or return_value.strip() == "":
                    logging.error(
                        "Configuration Key '%s' must have a value in '%s'"
                        % (key, value_column)
                    )
                    self.remove_temporary_files()
                    sys.exit(1)

            if bool_value == True:
                if return_value == 1:
                    return_value = True
                elif return_value == 0:
                    return_value = False
                else:
                    logging.error(
                        "Configuration Key '%s' can only have 0 or 1 in column '%s'"
                        % (key, value_column)
                    )
                    self.remove_temporary_files()
                    sys.exit(1)
        except TypeError:
            logging.error(
                "Configuration key '%s' cant be found. Have you upgraded the database schema to the latest version?"
                % (key)
            )
            self.remove_temporary_files()
            sys.exit(1)

        logging.debug("Fetched configuration '%s' as '%s'" % (key, return_value))
        logging.debug("Executing common_config.get_config_value() - Finished")
        return return_value

    def connect_to_mongo(self, exit_if_failure: bool = True, logger: str = "") -> bool:
        log = logging.getLogger(logger)
        log.debug("Executing common_config.connect_to_mongo()")

        if self.mongo_client != None:
            return True

        try:
            self.mongo_client = pymongo.MongoClient(
                "mongodb://%s:%s/" % (self.jdbc_hostname, self.jdbc_port)
            )
            self.mongoDB = self.mongo_client[self.jdbc_database.strip()]
            if self.mongoAuthSource == None:
                self.mongoDB.authenticate(self.jdbc_username, self.jdbc_password)
            else:
                self.mongoDB.authenticate(
                    self.jdbc_username, self.jdbc_password, source=self.mongoAuthSource
                )

        except pymongo.errors.ServerSelectionTimeoutError:
            log.error(
                "Timeout Error when connecting to Mongo at %s.%s"
                % (self.jdbc_hostname, self.jdbc_port)
            )
            if exit_if_failure == True:
                self.remove_temporary_files()
                sys.exit(1)
            else:
                return False

        return True

        log.debug("Executing common_config.connect_to_mongo() - Finished")

    def disconnect_from_mongo(self) -> None:
        logging.debug("Executing common_config.disconnect_from_mongo()")

        self.mongo_client.close()
        self.mongo_client = None
        self.mongoDB = None

        logging.debug("Executing common_config.disconnect_from_mongo() - Finished")

    def get_mongo_collections(
        self, collection_filter: str | None = None
    ) -> pd.DataFrame:
        logging.debug("Executing common_config.getMongoTables()")

        # Connect to MongoDB and get a list of all collections
        self.connect_to_mongo()
        mongo_collections = self.mongoDB.list_collection_names()
        self.disconnect_from_mongo()

        if collection_filter == None:
            collection_filter = ""
        collection_filter = collection_filter.replace("*", ".*")

        collection_list = []
        for collection in mongo_collections:
            if re.search(collection_filter, collection):
                collection_dict = {"schema": "-"}
                collection_dict["table"] = collection
                collection_list.append(collection_dict)

        result_df = pd.DataFrame(collection_list)

        logging.debug("Executing common_config.getMongoTables() - Finished")
        return result_df

    def get_jdbc_tables_and_views(
        self, schemaFilter: str | None = None, tableFilter: str | None = None
    ) -> pd.DataFrame:
        logging.debug("Executing common_config.get_jdbc_tables_and_views()")
        self.connect_to_jdbc()

        result_df = self.sourceSchema.get_jdbc_tables_and_views(
            JDBCCursor=self.JDBCCursor,
            serverType=self.jdbc_servertype,
            database=self.jdbc_database,
            schemaFilter=schemaFilter,
            tableFilter=tableFilter,
        )

        logging.debug("Executing common_config.get_jdbc_tables_and_views() - Finished")
        return result_df

    def get_atlas_discover_config_object(
        self, db_alias: str | None = None, logger: str = ""
    ) -> dict[str, Any]:
        """Discover all RDBMS objects on the 'dbAlias' and populate Atlas with them"""
        log = logging.getLogger(logger)
        log.debug("Executing common_config.get_atlas_discover_config_object()")

        if db_alias == None:
            db_alias = self.dbAlias

        self.lookup_connection_alias(db_alias)
        jdbc_connection_data = self.get_atlas_jdbc_connection_data(db_alias)

        config_object = {}
        config_object["dbAlias"] = db_alias
        config_object["contactInfo"] = jdbc_connection_data.get("contact_info")
        config_object["description"] = jdbc_connection_data.get("description")
        config_object["owner"] = jdbc_connection_data.get("owner")
        config_object["atlasIncludeFilter"] = jdbc_connection_data.get(
            "atlas_include_filter"
        )
        config_object["atlasExcludeFilter"] = jdbc_connection_data.get(
            "atlas_exclude_filter"
        )
        config_object["jdbc_hostname"] = self.jdbc_hostname
        config_object["jdbc_port"] = self.jdbc_port
        config_object["jdbc_servertype"] = self.jdbc_servertype
        config_object["jdbc_database"] = self.jdbc_database
        config_object["jdbc_oracle_sid"] = self.jdbc_oracle_sid
        config_object["jdbc_oracle_servicename"] = self.jdbc_oracle_servicename
        config_object["jdbc_username"] = self.jdbc_username
        config_object["jdbc_password"] = self.jdbc_password
        config_object["jdbc_driver"] = self.jdbc_driver
        config_object["jdbc_url"] = self.jdbc_url
        config_object["jdbc_classpath_for_python"] = self.jdbc_classpath_for_python
        config_object["jdbc_environment"] = self.jdbc_environment
        config_object["hdfs_address"] = self.get_config_value(key="hdfs_address")
        config_object["cluster_name"] = self.get_config_value(key="cluster_name")

        log.debug(
            "Executing common_config.get_atlas_discover_config_object() - Finished"
        )
        return config_object

    def get_anonymization_seed(self) -> str:
        logging.debug("Executing common_config.get_anonymization_seed()")

        # Set the default seed from the configuration file
        seed = configuration.get("Anonymization", "seed")

        query = "select seed_file from jdbc_connections where dbalias = %s "
        logging.debug("Executing the following SQL: %s" % (query))
        self.mysql_cursor.execute(query, (self.dbAlias,))

        row = self.mysql_cursor.fetchone()
        seed_file = row[0]

        if seed_file != None:
            if os.path.exists(seed_file) == False:
                logging.error(
                    "The connection have a seed file configured, but the tool is unable to find that file."
                )
                self.remove_temporary_files()
                sys.exit(1)

            seed = open(seed_file).read()

        if len(seed) > 16:
            logging.warning(
                "The seed is longer that 16 characters. Will truncate the seed to only include the first 16 characters"
            )
            seed = seed[:16]

        logging.debug("Executing common_config.get_anonymization_seed() - Finished")
        return seed

    def strip_unwanted_char_comment(self, work_string: str | None) -> str | None:
        if work_string == None:
            return
        work_string = work_string.replace("`", "")
        work_string = work_string.replace("'", "")
        work_string = work_string.replace(";", "")
        work_string = work_string.replace("\n", "")
        work_string = work_string.replace("\\", "")
        work_string = work_string.replace("’", "")
        work_string = work_string.replace('"', "")
        return work_string.strip()

    def strip_unwanted_char_column_name(self, work_string: str | None) -> str | None:
        if work_string == None:
            return
        work_string = work_string.replace("`", "")
        work_string = work_string.replace("'", "")
        work_string = work_string.replace(";", "")
        work_string = work_string.replace("\n", "")
        work_string = work_string.replace("\\", "")
        work_string = work_string.replace("’", "")
        work_string = work_string.replace(":", "")
        work_string = work_string.replace(",", "")
        work_string = work_string.replace(".", "")
        work_string = work_string.replace('"', "")
        return work_string.strip()

    def save_json_to_database(self, datatype: str, destination: str, json: str) -> None:
        """Saves a json that was unable to be sent successfully to the 'json_to_send' table so it can be sent at a later time"""
        logging.debug("Executing common_config.save_json_to_database()")

        query = "insert into json_to_send "
        query += "( type, status, destination, jsondata ) "
        query += "values "
        query += "( %s, 0, %s, %s )"

        logging.debug("SQL Statement executed: %s" % (query))
        self.mysql_cursor.execute(query, (datatype, destination, json))
        self.mysql_conn.commit()

        logging.debug("Executing common_config.save_json_to_database() - Finished")

    def update_yarn_statistics(
        self,
        yarn_application_id: str,
        operation_tool: str,
        yarn_containers_total: int | None = None,
        yarn_containers_failed: int | None = None,
        yarn_containers_killed: int | None = None,
    ) -> None:
        """insert or update data to yarn_statistics"""
        logging.debug("Executing common_config.update_yarn_statistics()")

        query = None
        start_stop_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if yarn_containers_total == None:
            # If it's None, it means that it was called when query was started. At this stage, we need to insert the data

            if self.operationType == "import":
                self.yarn_application_start = start_stop_date

                query = "insert into yarn_statistics "
                query += "( yarn_application_id, operation_timestamp, operation_tool, operation_type, hive_db, hive_table, application_start ) "
                query += "values "
                query += "( %s, %s, %s, 'import', %s, %s, %s )"

                logging.debug("SQL Statement executed: %s" % (query))
                self.mysql_cursor.execute(
                    query,
                    (
                        yarn_application_id,
                        self.operationTimestamp,
                        operation_tool,
                        self.Hive_DB,
                        self.Hive_Table,
                        start_stop_date,
                    ),
                )
                self.mysql_conn.commit()

            if self.operationType == "export":
                self.yarn_application_start = start_stop_date

                query = "insert into yarn_statistics "
                query += "( yarn_application_id, operation_timestamp, operation_tool, operation_type, dbalias, target_schema, target_table, application_start ) "
                query += "values "
                query += "( %s, %s, %s, 'export', %s, %s, %s, %s )"

                logging.debug("SQL Statement executed: %s" % (query))
                self.mysql_cursor.execute(
                    query,
                    (
                        yarn_application_id,
                        self.operationTimestamp,
                        operation_tool,
                        self.dbAlias,
                        self.targetSchema,
                        self.targetTable,
                        start_stop_date,
                    ),
                )
                self.mysql_conn.commit()

        else:
            if self.yarn_application_start == None:
                # This is just a protection-function and should under normal operations never happen as a query must start before we get any data up update about.
                return

            query = "update yarn_statistics "
            query += "set application_stop = %s, yarn_containers_total = %s, yarn_containers_failed = %s, yarn_containers_killed = %s "
            query += "where yarn_application_id = %s and application_start = %s"

            logging.debug("SQL Statement executed: %s" % (query))
            self.mysql_cursor.execute(
                query,
                (
                    start_stop_date,
                    yarn_containers_total,
                    yarn_containers_failed,
                    yarn_containers_killed,
                    yarn_application_id,
                    self.yarn_application_start,
                ),
            )
            self.mysql_conn.commit()

        logging.debug("Executing common_config.update_yarn_statistics() - Finished")
