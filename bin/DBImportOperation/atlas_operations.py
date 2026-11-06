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

import getpass
import json
import logging
import re
import sys

import jaydebeapi
import jpype
import numpy as np
import pandas as pd
import requests
import sqlalchemy as sa
from requests.auth import HTTPBasicAuth
from requests_kerberos import HTTPKerberosAuth
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import aliased, sessionmaker

from common import constants as constant
from common.Exceptions import (
    SQLerror,
    atlasError,
    connectionError,
    foreignKeyError,
    invalidConfiguration,
    schemaError,
)
from ConfigReader import configuration
from DBImportConfig import configSchema
from sourceSchemaReader import schemaReader


class atlasOperation:
    def __init__(self, loggerName=None):
        self.loggerName = loggerName
        self.debugLogLevel = False
        log = logging.getLogger(self.loggerName)
        log.debug("Executing atlas_operations.__init__()")

        if logging.root.level == 10:  # DEBUG
            self.debugLogLevel = True

        self.jdbcConnectionMutex = None
        self.configDBSession = None
        self.configDBEngine = None
        self.JDBCCursor = None
        self.JDBCConn = None

        self.sourceSchema = schemaReader.source()

        # Fetch configuration about MySQL database and how to connect to it
        self.configHostname = configuration.get("Database", "mysql_hostname")
        self.configPort = configuration.get("Database", "mysql_port")
        self.configDatabase = configuration.get("Database", "mysql_database")
        self.configUsername = configuration.get("Database", "mysql_username")
        self.configPassword = configuration.get("Database", "mysql_password")

        # Atlas variables
        self.atlasEnabled = False
        self.atlasSchemaChecked = False
        self.atlasAddress = None
        self.atlasHeaders = None
        self.atlasTimeout = None
        self.atlasSSLverify = None
        self.atlasRestTypeDefDBImportProcess = None
        self.atlasRestEntities = None
        self.atlasRestUniqueAttributeType = None
        self.atlasJdbcSourceSupport = None
        self.atlasAuthentication = None
        self.atlasUsername = None
        self.atlasPassword = None

        self.source_columns_df = pd.DataFrame()
        self.source_keys_df = pd.DataFrame()

        # Get Atlas config
        self.atlasAddress = configuration.get("Atlas", "address")
        if self.atlasAddress != None and self.atlasAddress.strip() != "":
            self.atlasRestTypeDefDBImportProcess = (
                "%s/api/atlas/v2/types/typedef/name/DBImport_Process"
                % (self.atlasAddress)
            )
            self.atlasRestEntities = "%s/api/atlas/v2/entity/bulk" % (self.atlasAddress)
            self.atlasRestUniqueAttributeType = (
                "%s/api/atlas/v2/entity/uniqueAttribute/type" % (self.atlasAddress)
            )
            self.atlasEnabled = True
            self.atlasHeaders = {
                "Content-type": "application/json",
                "Accept": "application/json",
            }

            try:
                self.atlasTimeout = int(configuration.get("Atlas", "timeout"))
            except ValueError:
                self.atlasTimeout = 5
                logging.warning(
                    "Atlas timeout configuration does not contain a valid number. Setting the timeout to 5 seconds"
                )

            if configuration.get("Atlas", "ssl_verify").lower() == "false":
                self.atlasSSLverify = False
            else:
                if configuration.get("Atlas", "ssl_cert_path").strip() != "":
                    self.atlasSSLverify = configuration.get(
                        "Atlas", "ssl_cert_path"
                    ).strip()
                else:
                    self.atlasSSLverify = True

            # Get authentication configuration
            if configuration.get("Atlas", "authentication_type").lower() == "username":
                self.atlasUsername = configuration.get("Atlas", "username")
                self.atlasPassword = configuration.get("Atlas", "password")
                self.atlasAuthentication = HTTPBasicAuth(
                    self.atlasUsername, self.atlasPassword
                )
            elif (
                configuration.get("Atlas", "authentication_type").lower() == "kerberos"
            ):
                self.atlasAuthentication = HTTPKerberosAuth()

            if self.atlasAuthentication == None:
                logging.error(
                    "Atlas authentication is not configured correctly. Valid options are 'username' or 'kerberos'"
                )
                self.atlasEnabled = False

    # 				sys.exit(1)

    def getAtlasRdbmsReferredEntities(
        self, schemaName, tableName, refSchemaName="", refTableName="", hdfsPath=""
    ):
        """Returns a dict that contains the referredEntities part for the RDBMS update rest call"""
        logging.debug("Executing atlas_operations.getAtlasReferredEntities()")

        if self.atlasEnabled == False:
            return None

        return_dict = self.getAtlasRdbmsNames(
            schemaName=schemaName, tableName=tableName
        )
        if return_dict == None:
            return

        table_uri = return_dict["tableUri"]
        db_uri = return_dict["dbUri"]
        db_name = return_dict["dbName"]
        instance_uri = return_dict["instanceUri"]
        instance_name = return_dict["instanceName"]
        instance_type = return_dict["instanceType"]
        environment_type = return_dict["environmentType"]

        if refSchemaName != "" and refTableName != "":
            return_dict = self.getAtlasRdbmsNames(
                schemaName=refSchemaName, tableName=refTableName
            )
            if return_dict == None:
                return

            ref_table_uri = return_dict["tableUri"]

        # TODO: Get the following information from dbimport and source system
        # Index

        json_data = {}
        json_data["referredEntities"] = {}

        if len(self.source_columns_df) > 0:
            table_create_time = self.source_columns_df.iloc[0]["TABLE_CREATE_TIME"]
            table_comment = self.source_columns_df.iloc[0]["TABLE_COMMENT"]
            # 			tableType = self.getJdbcTableType()

            json_data["referredEntities"]["-100"] = {}
            json_data["referredEntities"]["-100"]["guid"] = "-100"
            json_data["referredEntities"]["-100"]["typeName"] = "rdbms_table"
            json_data["referredEntities"]["-100"]["attributes"] = {}
            json_data["referredEntities"]["-100"]["attributes"]["qualifiedName"] = (
                table_uri
            )
            json_data["referredEntities"]["-100"]["attributes"]["name"] = tableName
            if schemaName == "-":
                json_data["referredEntities"]["-100"]["attributes"]["name_path"] = None
            else:
                json_data["referredEntities"]["-100"]["attributes"]["name_path"] = (
                    schemaName
                )
            json_data["referredEntities"]["-100"]["attributes"]["contact_info"] = (
                self.contactInfo
            )
            json_data["referredEntities"]["-100"]["attributes"]["owner"] = self.owner
            json_data["referredEntities"]["-100"]["attributes"]["comment"] = (
                table_comment
            )
            json_data["referredEntities"]["-100"]["attributes"]["type"] = self.tableType
            if table_create_time != None:
                # Atlas only support fractions of a second with 3 digits. So we remove the last 3 as python %f gives 6 digits
                # This datetime object from the source database needs to be in UTC. We might have to convert it
                json_data["referredEntities"]["-100"]["attributes"]["createTime"] = (
                    table_create_time.strftime("%Y-%m-%dT%H:%M:%S.%f")[0:-3] + "Z"
                )
            json_data["referredEntities"]["-100"]["attributes"]["db"] = {
                "guid": "-300",
                "typeName": "rdbms_db",
            }

            if refSchemaName != "" and refTableName != "":
                json_data["referredEntities"]["-101"] = {}
                json_data["referredEntities"]["-101"]["guid"] = "-101"
                json_data["referredEntities"]["-101"]["typeName"] = "rdbms_table"
                json_data["referredEntities"]["-101"]["attributes"] = {}
                json_data["referredEntities"]["-101"]["attributes"]["qualifiedName"] = (
                    ref_table_uri
                )
                json_data["referredEntities"]["-101"]["attributes"]["name"] = (
                    refTableName
                )
                json_data["referredEntities"]["-101"]["attributes"]["db"] = {
                    "guid": "-300",
                    "typeName": "rdbms_db",
                }

            if hdfsPath != "":
                # 				hdfsAddress = self.getConfigValue(key = "hdfs_address")
                # 				clusterName = self.getConfigValue(key = "cluster_name")
                hdfs_uri = "%s%s@%s" % (self.hdfsAddress, hdfsPath, self.clusterName)
                hdfs_full_path = "%s%s" % (self.hdfsAddress, hdfsPath)

                json_data["referredEntities"]["-200"] = {}
                json_data["referredEntities"]["-200"]["guid"] = "-200"
                json_data["referredEntities"]["-200"]["typeName"] = "hdfs_path"
                json_data["referredEntities"]["-200"]["attributes"] = {}
                json_data["referredEntities"]["-200"]["attributes"]["qualifiedName"] = (
                    hdfs_uri
                )
                json_data["referredEntities"]["-200"]["attributes"]["name"] = hdfsPath
                json_data["referredEntities"]["-200"]["attributes"]["path"] = (
                    hdfs_full_path
                )
                json_data["referredEntities"]["-200"]["attributes"]["clusterName"] = (
                    self.clusterName
                )
                json_data["referredEntities"]["-200"]["attributes"]["nameServiceId"] = (
                    self.clusterName
                )

            json_data["referredEntities"]["-300"] = {}
            json_data["referredEntities"]["-300"]["guid"] = "-300"
            json_data["referredEntities"]["-300"]["typeName"] = "rdbms_db"
            json_data["referredEntities"]["-300"]["attributes"] = {}
            json_data["referredEntities"]["-300"]["attributes"]["qualifiedName"] = (
                db_uri
            )
            json_data["referredEntities"]["-300"]["attributes"]["name"] = db_name
            json_data["referredEntities"]["-300"]["attributes"]["contact_info"] = (
                self.contactInfo
            )
            json_data["referredEntities"]["-300"]["attributes"]["owner"] = self.owner
            json_data["referredEntities"]["-300"]["attributes"]["description"] = (
                self.description
            )
            json_data["referredEntities"]["-300"]["attributes"]["prodOrOther"] = (
                environment_type
            )
            json_data["referredEntities"]["-300"]["attributes"]["instance"] = {
                "guid": "-400",
                "typeName": "rdbms_instance",
            }

        json_data["referredEntities"]["-400"] = {}
        json_data["referredEntities"]["-400"]["guid"] = "-400"
        json_data["referredEntities"]["-400"]["typeName"] = "rdbms_instance"
        json_data["referredEntities"]["-400"]["attributes"] = {}
        json_data["referredEntities"]["-400"]["attributes"]["qualifiedName"] = (
            instance_uri
        )
        json_data["referredEntities"]["-400"]["attributes"]["name"] = instance_name
        json_data["referredEntities"]["-400"]["attributes"]["rdbms_type"] = (
            instance_type
        )
        json_data["referredEntities"]["-400"]["attributes"]["hostname"] = (
            self.jdbc_hostname
        )
        json_data["referredEntities"]["-400"]["attributes"]["port"] = self.jdbc_port
        json_data["referredEntities"]["-400"]["attributes"]["platform"] = ""
        json_data["referredEntities"]["-400"]["attributes"]["protocol"] = "jdbc"

        return json_data

        logging.debug(
            "Executing atlas_operations.getAtlasReferredEntities() - Finished"
        )

    def checkAtlasSchema(self, logger=""):
        """Checks if the Atlas schema for DBImport is available and at the correct version. Returns True or False"""
        log = logging.getLogger(logger)
        log.debug("Executing atlas_operations.checkAtlasSchema()")

        # We only check the Atlas schema ones per execution. This will otherwise add more requests to Atlas for the same data
        if self.atlasSchemaChecked == True:
            return self.atlasEnabled

        if self.atlasEnabled == True:
            log.info("Checking Atlas connection")
            response = self.atlasGetData(
                URL=self.atlasRestTypeDefDBImportProcess, logger=logger
            )
            if response == None:
                status_code = "-1"
            else:
                status_code = response["statusCode"]
                try:
                    response_data = json.loads(response["data"])
                except json.decoder.JSONDecodeError as errMsg:
                    response_data = None
                    log.error(
                        "Atlas schema check for DBIMportProcess TypeDef did not return a valid JSON. Data returned:\n%s"
                        % (response["data"])
                    )
                    log.error(errMsg)
                    log.warning("Disable Atlas integration")
                    self.atlasEnabled = False

        if self.atlasEnabled == True and status_code == 404:
            # The TypeDef was not found
            log.warning(
                "The Atlas typeDef 'DBImport_Process' was not found. Did you run the Atlas setup for DBImport?"
            )
            self.atlasEnabled = False

        elif self.atlasEnabled == True and status_code != 200:
            log.warning("Atlas communication failed with response %s" % (status_code))
            self.atlasEnabled = False

        if self.atlasEnabled == True:
            # if self.atlasEnabled = True at this stage, it means that we have a valid json response in DBImportProcessJSON
            type_def_version = response_data["typeVersion"]
            if type_def_version != "1.2":
                log.warning(
                    "The version for Atlas typeDef 'DBImport_Process' is not correct. Version required is 1.2 and version found is %s"
                    % (type_def_version)
                )
                log.warning("Atlas integration is Disabled")
                self.atlasEnabled = False
            else:
                self.atlasSchemaChecked = True

        return self.atlasEnabled

        logging.debug("Executing atlas_operations.checkAtlasSchema() - Finished")

    def atlasGetData(self, URL, logger=""):
        log = logging.getLogger(logger)
        log.debug("Executing atlas_operations.atlasGetData()")
        return self.atlasCommunicate(URL=URL, requestType="GET", logger=logger)

    def atlasPostData(self, URL, data, logger=""):
        log = logging.getLogger(logger)
        log.debug("Executing atlas_operations.atlasPostData()")
        return self.atlasCommunicate(
            URL=URL, requestType="POST", data=data, logger=logger
        )

    def atlasDeleteData(self, URL, logger=""):
        log = logging.getLogger(logger)
        log.debug("Executing atlas_operations.atlasDeleteData()")
        return self.atlasCommunicate(URL=URL, requestType="DELETE", logger=logger)

    def atlasCommunicate(self, URL, requestType, data=None, logger=""):
        log = logging.getLogger(logger)
        log.debug("Executing atlas_operations.atlasCommunicate()")

        return_value = None
        if self.atlasEnabled == True:
            try:
                if requestType == "POST":
                    response = requests.post(
                        URL,
                        headers=self.atlasHeaders,
                        data=data,
                        timeout=self.atlasTimeout,
                        auth=self.atlasAuthentication,
                        verify=self.atlasSSLverify,
                    )

                elif requestType == "GET":
                    response = requests.get(
                        URL,
                        headers=self.atlasHeaders,
                        timeout=self.atlasTimeout,
                        auth=self.atlasAuthentication,
                        verify=self.atlasSSLverify,
                    )

                elif requestType == "DELETE":
                    response = requests.delete(
                        URL,
                        headers=self.atlasHeaders,
                        timeout=self.atlasTimeout,
                        auth=self.atlasAuthentication,
                        verify=self.atlasSSLverify,
                    )

                else:
                    raise ValueError

                return_value = {
                    "statusCode": response.status_code,
                    "data": response.text,
                }
                response.close()
                log.debug("Atlas statusCode = %s" % (response.status_code))
                log.debug("Atlas data = %s" % (response.text))

            except requests.exceptions.SSLError:
                log.error(
                    "Atlas connection failed due to SSL validation. Please check Atlas connection configuration in config file"
                )
                log.warning("Atlas integration is Disabled")
                log.debug("Atlas data: %s" % (data))
                log.debug("Atlas requestType: %s" % (requestType))
                self.atlasEnabled = False
            except requests.exceptions.HTTPError:
                log.error("HTTP error when communicating with Atlas on %s" % (URL))
                log.warning("Atlas integration is Disabled")
                log.debug("Atlas data: %s" % (data))
                log.debug("Atlas requestType: %s" % (requestType))
                self.atlasEnabled = False
            except requests.exceptions.RequestException:
                log.error(
                    "RequestException error when communicating with Atlas on %s" % (URL)
                )
                log.warning("Atlas integration is Disabled")
                log.debug("Atlas data: %s" % (data))
                log.debug("Atlas requestType: %s" % (requestType))
                self.atlasEnabled = False
            except requests.exceptions.ConnectionError:
                log.error(
                    "Connection error when communicating with Atlas on %s" % (URL)
                )
                log.warning("Atlas integration is Disabled")
                log.debug("Atlas data: %s" % (data))
                log.debug("Atlas requestType: %s" % (requestType))
                self.atlasEnabled = False
            except ValueError:
                log.error(
                    "The used requstType (%s) for atlasCommunicate() is not supported"
                    % (requestType)
                )
                log.warning("Atlas integration is Disabled")
                log.debug("Atlas data: %s" % (data))
                log.debug("Atlas requestType: %s" % (requestType))
                self.atlasEnabled = False

        log.debug("Executing atlas_operations.atlasCommunicate() - Finished")
        return return_value

    def setConfiguration(self, configDict, mutex=None):
        """
        configDict must includes the following settings
                dbAlias
                contactInfo
                description
                owner
                atlasIncludeFilter
                atlasExcludeFilter
                jdbc_hostname
                jdbc_port
                jdbc_serverType
                jdbc_database
                jdbc_oracle_sid
                jdbc_username
                jdbc_password
                jdbc_driver
                jdbc_url
                jdbc_classpath_for_python
                jdbc_environment
        """

        log = logging.getLogger(self.loggerName)
        log.debug("Executing atlas_operations.setConfiguration()")

        self.dbAlias = configDict.get("dbAlias")
        self.contactInfo = configDict.get("contactInfo")
        self.description = configDict.get("description")
        self.owner = configDict.get("owner")
        self.atlasIncludeFilter = configDict.get("atlasIncludeFilter")
        self.atlasExcludeFilter = configDict.get("atlasExcludeFilter")
        self.jdbc_hostname = configDict.get("jdbc_hostname")
        self.jdbc_port = configDict.get("jdbc_port")
        self.jdbc_servertype = configDict.get("jdbc_servertype")
        self.jdbc_database = configDict.get("jdbc_database")
        self.jdbc_oracle_servicename = configDict.get("jdbc_oracle_servicename")
        self.jdbc_oracle_sid = configDict.get("jdbc_oracle_sid")
        self.jdbc_username = configDict.get("jdbc_username")
        self.jdbc_password = configDict.get("jdbc_password")
        self.jdbc_driver = configDict.get("jdbc_driver")
        self.jdbc_url = configDict.get("jdbc_url")
        self.jdbc_classpath_for_python = configDict.get("jdbc_classpath_for_python")
        self.jdbc_environment = configDict.get("jdbc_environment")
        self.hdfsAddress = configDict.get("hdfs_address")
        self.clusterName = configDict.get("cluster_name")

        self.jdbcConnectionMutex = mutex

        log.debug("Executing atlas_operations.setConfiguration() - Finished")

    def discoverAtlasRdbms(self):
        """Discover all RDBMS objects on the 'dbAlias' and populate Atlas with them"""

        log = logging.getLogger(self.loggerName)
        log.debug("Executing atlas_operations.discoverAtlasRdbms()")
        log.info("Starting a Atlas discovery on connection '%s'" % (self.dbAlias))

        if self.atlasIncludeFilter == None or self.atlasIncludeFilter == "":
            self.atlasIncludeFilter = "*.*"

        if self.atlasExcludeFilter == None:
            self.atlasExcludeFilter = ""

        self.jdbc_hostname = re.sub("\\r", "", self.jdbc_hostname)
        self.jdbc_hostname = re.sub("\\n", "", self.jdbc_hostname)
        self.jdbc_database = re.sub("\\r", "", self.jdbc_database)
        self.jdbc_database = re.sub("\\n", "", self.jdbc_database)

        if self.jdbcConnectionMutex != None:
            self.jdbcConnectionMutex.acquire()
            log.debug(
                "JDBC connection Mutex acquired for reading all tables and views on '%s'"
                % (self.dbAlias)
            )

        try:
            self.connectToJDBC()
            log.debug("Connected to JDBC source")
        except:
            if self.jdbcConnectionMutex != None:
                self.jdbcConnectionMutex.release()
                log.debug("JDBC connection Mutex released")
            raise connectionError

        try:
            # Get a list of all schemas and tables that can be found on the database connection
            tables_and_views_df = self.sourceSchema.getJDBCtablesAndViews(
                self.JDBCCursor, self.jdbc_servertype, database=self.jdbc_database
            )

            self.disconnectFromJDBC()
        except:
            log.error(
                "Unknown error during reading of tables and views from '%s'"
                % (self.dbAlias)
            )
            if self.jdbcConnectionMutex != None:
                self.jdbcConnectionMutex.release()
                log.debug("JDBC connection Mutex released")
            raise schemaError

        if self.jdbcConnectionMutex != None:
            self.jdbcConnectionMutex.release()
            log.debug("JDBC connection Mutex released")

        if len(tables_and_views_df) == 0:
            create_empty_database_in_atlas = True
        else:
            create_empty_database_in_atlas = False

        if self.atlasIncludeFilter == None or self.atlasIncludeFilter == "":
            # Set default to include the table to True if there is no include filter
            tables_and_views_df.insert(2, "include", True)
        else:
            # Set default to include the table to False if there is an include filter
            tables_and_views_df.insert(2, "include", False)

        # Iterate over DF to determine what tables we should discover and what to not discover based on include/exclude filter
        # This will add a new column to the DF with True/False values depending on if we are going to discover it or not

        for index, row in tables_and_views_df.iterrows():
            update_atlas_with_table = False
            schema = row["schema"].lower()
            table = row["table"].lower()

            # Filter the schema.table so that only the included tables gets processed
            for include in self.atlasIncludeFilter.split(";"):
                try:
                    include_schema = include.split(".")[0].replace("*", ".*").lower()
                    include_table = include.split(".")[1].replace("*", ".*").lower()
                    if re.search(
                        "^" + include_schema + "$", schema.strip()
                    ) and re.search("^" + include_table + "$", table.strip()):
                        tables_and_views_df.at[index, "include"] = True
                        break
                except IndexError:
                    pass

            # Filter the schema.table so we remove the excluded items
            for exclude in self.atlasExcludeFilter.split(";"):
                try:
                    exclude_schema = exclude.split(".")[0].replace("*", ".*").lower()
                    exclude_table = exclude.split(".")[1].replace("*", ".*").lower()
                    if re.search(
                        "^" + exclude_schema + "$", schema.strip()
                    ) and re.search("^" + exclude_table + "$", table.strip()):
                        tables_and_views_df.at[index, "include"] = False
                        break
                except IndexError:
                    pass

        # Remove all rows from dataframe where include = False.
        tables_and_views_df.drop(
            tables_and_views_df[tables_and_views_df.include == False].index,
            inplace=True,
        )

        atlas_column_cache = aliased(configSchema.atlasColumnCache)
        atlas_key_cache = aliased(configSchema.atlasKeyCache)
        self.source_columns_df = pd.DataFrame()
        self.source_keys_df = pd.DataFrame()

        atlas_foreign_key_error_detected = False
        atlas_error_detected = False

        # Iterate over the schemas that we have tables to process in
        for schema in tables_and_views_df.schema.unique():
            schema = re.sub("\\r", "", schema)
            schema = re.sub("\\n", "", schema)

            if self.jdbc_servertype == constant.MYSQL:
                log.info(
                    "Starting discovery of database '%s' on connection '%s'"
                    % (self.jdbc_database, self.dbAlias)
                )
            else:
                log.info(
                    "Starting discovery of schema '%s' on connection '%s'"
                    % (schema.strip(), self.dbAlias)
                )

            # TODO: Must check if the connection is still valid. Very large databases with many table will cause the connection to die

            if self.jdbcConnectionMutex != None:
                self.jdbcConnectionMutex.acquire()
                log.debug(
                    "JDBC connection Mutex acquired for reading columns and keys in schema '%s' on '%s'"
                    % (schema, self.dbAlias)
                )

            try:
                # Connect to the source database
                self.connectToJDBC()
                # TODO: Handle failures of the connection

                full_schema_column_df = self.sourceSchema.readTableColumns(
                    self.JDBCCursor,
                    serverType=self.jdbc_servertype,
                    database=self.jdbc_database,
                    schema=schema,
                )

                full_schema_key_df = self.sourceSchema.readTableKeys(
                    self.JDBCCursor,
                    serverType=self.jdbc_servertype,
                    database=self.jdbc_database,
                    schema=schema,
                )

                self.disconnectFromJDBC()
            except connectionError:
                if self.jdbcConnectionMutex != None:
                    self.jdbcConnectionMutex.release()
                    log.debug("JDBC connection Mutex released")
                raise connectionError
            except:
                log.error(
                    "Unknown error during reading of columns and keys for schema '%s' on '%s'"
                    % (schema, self.dbAlias)
                )
                if self.jdbcConnectionMutex != None:
                    self.jdbcConnectionMutex.release()
                    log.debug("JDBC connection Mutex released")
                raise schemaError

            if self.jdbcConnectionMutex != None:
                self.jdbcConnectionMutex.release()
                log.debug("JDBC connection Mutex released")

            if self.jdbc_servertype == constant.MYSQL:
                # In MySQL the schema is the database
                full_schema_column_df["SCHEMA_NAME"] = "-"
                schema = "-"

            log.debug(
                "DataFrame for schema '%s' was received from the database" % (schema)
            )

            try:
                session = self.getDBImportSession()

                full_cache_source_column_df = pd.DataFrame(
                    session.query(
                        atlas_column_cache.hostname,
                        atlas_column_cache.port,
                        atlas_column_cache.database_name,
                        atlas_column_cache.schema_name,
                        atlas_column_cache.table_name,
                        atlas_column_cache.table_comment,
                        atlas_column_cache.column_name,
                        atlas_column_cache.column_type,
                        atlas_column_cache.column_length,
                        atlas_column_cache.column_comment,
                        atlas_column_cache.column_is_nullable,
                        atlas_column_cache.table_type,
                        atlas_column_cache.table_create_time,
                        atlas_column_cache.default_value,
                    )
                    .select_from(atlas_column_cache)
                    .filter(atlas_column_cache.hostname == self.jdbc_hostname)
                    .filter(atlas_column_cache.port == self.jdbc_port)
                    .filter(atlas_column_cache.database_name == self.jdbc_database)
                    .filter(atlas_column_cache.schema_name == schema.strip())
                    .all()
                )

                session.close()

            except SQLAlchemyError as e:
                log.error(str(e.__dict__["orig"]))
                session.rollback()
                self.disconnectDBImportDB()
                raise SQLerror

            if len(full_cache_source_column_df) > 0:
                # Set oldes timestamp to 1970-01-01 00:00:00.000
                full_cache_source_column_df["table_create_time"] = (
                    full_cache_source_column_df["table_create_time"].clip(
                        lower=pd.Timestamp("1970-01-01 00:00:00.000")
                    )
                )

            log.debug(
                "Column cache for schema '%s' was received from the database" % (schema)
            )

            try:
                session = self.getDBImportSession()

                full_cache_source_key_df = pd.DataFrame(
                    session.query(
                        atlas_key_cache.hostname,
                        atlas_key_cache.port,
                        atlas_key_cache.database_name,
                        atlas_key_cache.schema_name,
                        atlas_key_cache.table_name,
                        atlas_key_cache.constraint_name,
                        atlas_key_cache.constraint_type,
                        atlas_key_cache.column_name,
                        atlas_key_cache.reference_schema_name,
                        atlas_key_cache.reference_table_name,
                        atlas_key_cache.reference_column_name,
                        atlas_key_cache.col_key_position,
                    )
                    .select_from(atlas_key_cache)
                    .filter(atlas_key_cache.hostname == self.jdbc_hostname)
                    .filter(atlas_key_cache.port == self.jdbc_port)
                    .filter(atlas_key_cache.database_name == self.jdbc_database)
                    .filter(atlas_key_cache.schema_name == schema.strip())
                    .all()
                )

                session.close()

            except SQLAlchemyError as e:
                log.error(str(e.__dict__["orig"]))
                session.rollback()
                self.disconnectDBImportDB()
                raise SQLerror

            log.debug(
                "Key cache for schema '%s' was received from the database" % (schema)
            )

            # If the database is empty, we need this objects later when we search for tables to delete
            if len(tables_and_views_df) == 0:
                source_column_df = pd.DataFrame()
                source_key_df = pd.DataFrame()
                self.source_columns_df = pd.DataFrame()
                self.source_keys_df = pd.DataFrame()

            # Loop through all tables and find if there is a difference between values from database and values in cache
            # If there is a difference, we update atlas and update the cache tables
            for index, row in tables_and_views_df[
                tables_and_views_df["schema"] == schema
            ].iterrows():
                schema = row["schema"]
                table = row["table"]

                self.source_columns_df = full_schema_column_df[
                    (full_schema_column_df["SCHEMA_NAME"] == schema.strip())
                    & (full_schema_column_df["TABLE_NAME"] == table.strip())
                ].copy()
                self.source_columns_df.drop(
                    columns=["SCHEMA_NAME", "TABLE_NAME"], inplace=True
                )
                self.source_columns_df.replace({pd.NaT: None}, inplace=True)

                if len(self.source_columns_df) > 0:
                    self.tableType = self.sourceSchema.getJdbcTableType(
                        self.jdbc_servertype,
                        self.source_columns_df.iloc[0]["TABLE_TYPE"],
                    )

                    source_column_df = self.source_columns_df.copy()
                    source_column_df = source_column_df.replace({np.nan: None})
                    source_column_df.insert(0, "hostname", self.jdbc_hostname.lower())
                    source_column_df.insert(1, "port", self.jdbc_port)
                    source_column_df.insert(2, "database_name", self.jdbc_database)
                    source_column_df.insert(3, "schema_name", schema)
                    source_column_df.insert(4, "table_name", table)
                    source_column_df.rename(
                        columns={
                            "TABLE_COMMENT": "table_comment",
                            "SOURCE_COLUMN_NAME": "column_name",
                            "SOURCE_COLUMN_TYPE": "column_type",
                            "SOURCE_COLUMN_LENGTH": "column_length",
                            "SOURCE_COLUMN_COMMENT": "column_comment",
                            "IS_NULLABLE": "column_is_nullable",
                            "TABLE_TYPE": "table_type",
                            "TABLE_CREATE_TIME": "table_create_time",
                            "DEFAULT_VALUE": "default_value",
                        },
                        inplace=True,
                    )
                    source_column_df["table_create_time"] = source_column_df[
                        "table_create_time"
                    ].astype("datetime64[s]")
                    source_column_df.replace({pd.NaT: None}, inplace=True)
                    source_column_df.replace(
                        to_replace=[r"\\t|\\n|\\r", "\t|\n|\r"],
                        value="",
                        regex=True,
                        inplace=True,
                    )
                    source_column_df["column_length"] = source_column_df[
                        "column_length"
                    ].astype("str")
                    source_column_df.sort_values(
                        by="column_name",
                        ignore_index=True,
                        inplace=True,
                        key=lambda col: col.str.lower(),
                    )

                else:
                    source_column_df = pd.DataFrame()

                if len(full_schema_key_df) > 0:
                    self.source_keys_df = full_schema_key_df[
                        (full_schema_key_df["SCHEMA_NAME"] == schema.strip())
                        & (full_schema_key_df["TABLE_NAME"] == table.strip())
                    ].copy()
                    self.source_keys_df.drop(
                        columns=["SCHEMA_NAME", "TABLE_NAME"], inplace=True
                    )
                else:
                    self.source_keys_df = pd.DataFrame()

                if len(self.source_keys_df) > 0:
                    source_key_df = self.source_keys_df.copy()
                    source_key_df = source_key_df.replace({np.nan: None})
                    source_key_df.insert(0, "hostname", self.jdbc_hostname.lower())
                    source_key_df.insert(1, "port", self.jdbc_port)
                    source_key_df.insert(2, "database_name", self.jdbc_database)
                    source_key_df.insert(3, "schema_name", schema)
                    source_key_df.insert(4, "table_name", table)
                    source_key_df.rename(
                        columns={
                            "CONSTRAINT_NAME": "constraint_name",
                            "CONSTRAINT_TYPE": "constraint_type",
                            "COL_NAME": "column_name",
                            "REFERENCE_SCHEMA_NAME": "reference_schema_name",
                            "REFERENCE_TABLE_NAME": "reference_table_name",
                            "REFERENCE_COL_NAME": "reference_column_name",
                            "COL_KEY_POSITION": "col_key_position",
                        },
                        inplace=True,
                    )
                    source_key_df.sort_values(
                        by=["constraint_name", "column_name"],
                        ignore_index=True,
                        inplace=True,
                        key=lambda col: col.str.lower(),
                    )
                    source_key_df.replace(
                        to_replace=[r"\\t|\\n|\\r", "\t|\n|\r"],
                        value="",
                        regex=True,
                        inplace=True,
                    )
                else:
                    source_key_df = pd.DataFrame()

                if len(full_cache_source_column_df) > 0:
                    cache_source_column_df = full_cache_source_column_df[
                        full_cache_source_column_df["table_name"] == table
                    ].copy()
                    if len(cache_source_column_df) > 0:
                        cache_source_column_df.replace({pd.NaT: None}, inplace=True)
                        # Sort by column name in lowercase
                        cache_source_column_df.sort_values(
                            by="column_name",
                            ignore_index=True,
                            inplace=True,
                            key=lambda col: col.str.lower(),
                        )
                    else:
                        cache_source_column_df = pd.DataFrame()
                else:
                    cache_source_column_df = pd.DataFrame()

                if len(full_cache_source_key_df) > 0:
                    cache_source_key_df = full_cache_source_key_df[
                        full_cache_source_key_df["table_name"] == table
                    ].copy()
                    if len(cache_source_key_df) > 0:
                        # Sort by column name in lowercase
                        cache_source_key_df.sort_values(
                            by=["constraint_name", "column_name"],
                            ignore_index=True,
                            inplace=True,
                            key=lambda col: col.str.lower(),
                        )
                    else:
                        cache_source_key_df = pd.DataFrame()
                else:
                    cache_source_key_df = pd.DataFrame()

                schema = row["schema"].strip()
                table = row["table"].strip()

                if 0 == 1:
                    # Used for debugging
                    if cache_source_column_df.equals(source_column_df):
                        print("columns equal")
                    if cache_source_key_df.equals(source_key_df):
                        print("keys equal")
                    pd.set_option("display.max_columns", None)
                    pd.set_option("display.max_rows", None)

                    print(source_column_df)
                    print(
                        "**********************************************************************"
                    )
                    print(cache_source_column_df)
                    print(
                        "**********************************************************************"
                    )
                    print(source_key_df)
                    print(
                        "**********************************************************************"
                    )
                    print(cache_source_key_df)

                cache_operation = None
                if len(source_column_df) > 0:
                    if len(cache_source_column_df) == 0:
                        cache_operation = "insert"
                    else:
                        cache_operation = "update"

                        if cache_source_column_df.equals(
                            source_column_df
                        ) and cache_source_key_df.equals(source_key_df):
                            if row["schema"] != "-":
                                log.debug(
                                    "Atlas table schema for %s.%s on dbalias %s match cached values. No update required"
                                    % (schema, table, self.dbAlias)
                                )
                            else:
                                log.debug(
                                    "Atlas table schema for %s on dbalias %s match cached values. No update required"
                                    % (table, self.dbAlias)
                                )
                            continue

                self.atlasEnabled = True  # This can be set to False in self.updateAtlasWithRDBMSdata depending on what it finds

                if row["schema"] != "-":
                    log.info(
                        "Creating/updating Atlas information for table '%s.%s' on connection '%s'"
                        % (schema, table, self.dbAlias)
                    )
                else:
                    log.info(
                        "Creating/updating Atlas information for table '%s' on connection '%s'"
                        % (table, self.dbAlias)
                    )

                if 0 == 1:
                    # Used for debugging
                    if cache_source_column_df.equals(source_column_df):
                        print("columns equal")
                    if cache_source_key_df.equals(source_key_df):
                        print("keys equal")
                    pd.set_option("display.max_columns", None)
                    pd.set_option("display.max_rows", None)

                    print(source_column_df)
                    print(
                        "**********************************************************************"
                    )
                    print(cache_source_column_df)
                    print(
                        "**********************************************************************"
                    )
                    print(source_key_df)
                    print(
                        "**********************************************************************"
                    )
                    print(cache_source_key_df)

                # We need to save if there is an error and raise them after all tables are processed. If not, tables with foreign keys that dont have
                # the refered table in Atlas will never be able to be created correctly
                try:
                    self.updateAtlasWithRDBMSdata(
                        schemaName=schema,
                        tableName=table,
                        printInfo=False,
                        errorOnWarning=True,
                    )
                except atlasError:
                    atlas_error_detected = True
                    continue
                except foreignKeyError:
                    atlas_foreign_key_error_detected = False
                    continue
                except invalidConfiguration:
                    atlas_error_detected = True
                    continue
                # 				result = self.updateAtlasWithRDBMSdata(schemaName = schema, tableName = table, printInfo = False, errorOnWarning=True)
                # 				if result == False:
                # 					return False

                log.debug("Atlas call completed")

                if cache_operation == "update":
                    try:
                        session = self.getDBImportSession()

                        (
                            session.query(configSchema.atlasColumnCache)
                            .filter(
                                configSchema.atlasColumnCache.hostname
                                == self.jdbc_hostname
                            )
                            .filter(
                                configSchema.atlasColumnCache.port == self.jdbc_port
                            )
                            .filter(
                                configSchema.atlasColumnCache.database_name
                                == self.jdbc_database
                            )
                            .filter(
                                configSchema.atlasColumnCache.schema_name
                                == schema.strip()
                            )
                            .filter(configSchema.atlasColumnCache.table_name == table)
                            .delete()
                        )

                        (
                            session.query(configSchema.atlasKeyCache)
                            .filter(
                                configSchema.atlasKeyCache.hostname
                                == self.jdbc_hostname
                            )
                            .filter(configSchema.atlasKeyCache.port == self.jdbc_port)
                            .filter(
                                configSchema.atlasKeyCache.database_name
                                == self.jdbc_database
                            )
                            .filter(
                                configSchema.atlasKeyCache.schema_name == schema.strip()
                            )
                            .filter(configSchema.atlasKeyCache.table_name == table)
                            .delete()
                        )

                        session.commit()
                        session.close()

                    except SQLAlchemyError as e:
                        log.error(str(e.__dict__["orig"]))
                        session.rollback()
                        self.disconnectDBImportDB()
                        raise SQLerror

                if cache_operation == "insert" or cache_operation == "update":
                    try:
                        session = self.getDBImportSession()
                        source_column_df.to_sql(
                            "atlas_column_cache",
                            self.configDBEngine,
                            index=False,
                            if_exists="append",
                        )
                        source_key_df.to_sql(
                            "atlas_key_cache",
                            self.configDBEngine,
                            index=False,
                            if_exists="append",
                        )
                        session.commit()
                        session.close()

                    except SQLAlchemyError as e:
                        log.error(str(e.__dict__["orig"]))
                        session.rollback()
                        self.disconnectDBImportDB()
                        raise SQLerror

        self.disconnectFromJDBC()

        log.debug("Table parsing completed")

        # Remove tables that exists in Atlas that we didnt find
        # Get the unique names for the rdbms_db. No need for schema or table as we are only intressted in the dbUri result
        atlas_name_dict = self.getAtlasRdbmsNames(schemaName="", tableName="")
        if atlas_name_dict == None:
            log.warning(
                "Could not check for deleted tables in Atlas as the name specifications in DBImport was not created correctly"
            )
            raise ValueError("Atlas RDBMS names could not be built")
        db_uri = atlas_name_dict["dbUri"]

        # if there are no tables in the database, we need to create it in Atlas manually
        if create_empty_database_in_atlas == True:
            table_uri = atlas_name_dict["tableUri"]
            db_name = atlas_name_dict["dbName"]
            instance_uri = atlas_name_dict["instanceUri"]
            instance_name = atlas_name_dict["instanceName"]
            instance_type = atlas_name_dict["instanceType"]
            environment_type = atlas_name_dict["environmentType"]

            json_data = self.getAtlasRdbmsReferredEntities(schemaName="", tableName="")

            json_data["entities"] = []
            db_data = {}
            db_data["typeName"] = "rdbms_db"
            db_data["createdBy"] = "DBImport"
            db_data["attributes"] = {}

            db_data["attributes"] = {}
            db_data["attributes"]["qualifiedName"] = db_uri
            db_data["attributes"]["name"] = db_name
            db_data["attributes"]["contact_info"] = self.contactInfo
            db_data["attributes"]["owner"] = self.owner
            db_data["attributes"]["description"] = self.description
            db_data["attributes"]["prodOrOther"] = environment_type
            db_data["attributes"]["instance"] = {
                "guid": "-400",
                "typeName": "rdbms_instance",
            }

            json_data["entities"].append(db_data)

            # 			log.info("======================================")
            # 			log.info("JSON to send to Atlas!")
            # 			log.info(json.dumps(jsonData, indent=3))
            # 			log.info("======================================")

            response = self.atlasPostData(
                URL=self.atlasRestEntities, data=json.dumps(json_data)
            )
            if response == None:
                log.warning(
                    "No response from POST rest call to Atlas when creating an empty database"
                )
                raise atlasError

            status_code = response["statusCode"]
            if status_code != 200:
                log.warning(
                    "Request from Atlas when updating database schema was %s."
                    % (status_code)
                )
                log.warning("%s" % (response["data"]))
                self.atlasEnabled == False  # noqa: B015  # TODO(#182): = was meant
                raise atlasError

        # query the rdbms_db object in Atlas
        atlas_rest_url = "%s/rdbms_db?minExtInfo=true&attr:qualifiedName=%s" % (
            self.atlasRestUniqueAttributeType,
            db_uri,
        )
        response = self.atlasGetData(URL=atlas_rest_url)
        if response == None:
            # If there is an error in self.atlasGetData(), we need to stop here as the response is None
            log.warning(
                "No response from GET rest call to Atlas when reading table object for deletion"
            )
            raise atlasError

        status_code = response["statusCode"]
        response_data = json.loads(response["data"])

        if status_code != 200:
            log.warning(
                "Request from Atlas when updating source schema was %s." % (status_code)
            )
            log.warning("%s" % (response["data"]))
            self.atlasEnabled == False  # noqa: B015  # TODO(#182): = was meant
            raise atlasError

        # Fetch all tables defined on the rdbms_db object
        tables_found_in_db_list = []
        for tables in response_data["entity"]["attributes"]["tables"]:
            guid = tables["guid"]
            qualified_name = response_data["referredEntities"][guid]["attributes"][
                "qualifiedName"
            ]

            if response_data["referredEntities"][guid]["status"] != "ACTIVE":
                continue

            if self.jdbc_servertype == constant.MYSQL:
                table = qualified_name.split("@")[0].split(".")[-1]
                schema = "-"
            else:
                table = qualified_name.split("@")[0].split(".")[-1]
                schema = qualified_name.split("@")[0].split(".")[-2]

            if self.atlasIncludeFilter == None or self.atlasIncludeFilter == "":
                process_table_for_delete = True
            else:
                process_table_for_delete = False

            # Filter the schema.table so that only the included tables gets processed
            for include in self.atlasIncludeFilter.split(";"):
                try:
                    include_schema = include.split(".")[0].replace("*", ".*").lower()
                    include_table = include.split(".")[1].replace("*", ".*").lower()
                    if re.search(
                        "^" + include_schema + "$", schema.lower().strip()
                    ) and re.search("^" + include_table + "$", table.lower().strip()):
                        process_table_for_delete = True
                        break
                except IndexError:
                    pass

            # Filter the schema.table so we remove the excluded items
            for exclude in self.atlasExcludeFilter.split(";"):
                try:
                    exclude_schema = exclude.split(".")[0].replace("*", ".*").lower()
                    exclude_table = exclude.split(".")[1].replace("*", ".*").lower()
                    if re.search(
                        "^" + exclude_schema + "$", schema.lower().strip()
                    ) and re.search("^" + exclude_table + "$", table.lower().strip()):
                        process_table_for_delete = False
                        break
                except IndexError:
                    pass

            if process_table_for_delete == False:
                continue

            tables_found_in_db_dict = {}
            tables_found_in_db_dict["schema"] = schema.strip()
            tables_found_in_db_dict["table"] = table.strip()
            tables_found_in_db_dict["qualifiedName"] = qualified_name
            tables_found_in_db_list.append(tables_found_in_db_dict)

        tables_and_views_df.drop(columns="include", inplace=True)
        tables_and_views_df["schema"] = tables_and_views_df["schema"].str.strip()
        tables_and_views_df["table"] = tables_and_views_df["table"].str.strip()

        tables_found_in_db = pd.DataFrame(tables_found_in_db_list)
        if tables_found_in_db.empty:
            tables_found_in_db = pd.DataFrame(columns=["schema", "table"])

        merge_df = pd.merge(
            tables_and_views_df,
            tables_found_in_db,
            on=None,
            how="outer",
            indicator="Exist",
        )
        for index, row in merge_df.loc[merge_df["Exist"] == "right_only"].iterrows():
            if row["schema"] != "-":
                log.info(
                    "Deleting Atlas table for %s.%s on dbalias %s"
                    % (row["schema"], row["table"], self.dbAlias)
                )
            else:
                log.info(
                    "Deleting Atlas table for %s on dbalias %s"
                    % (row["table"], self.dbAlias)
                )

            atlas_rest_url = "%s/rdbms_table?attr:qualifiedName=%s" % (
                self.atlasRestUniqueAttributeType,
                row["qualifiedName"],
            )
            response = self.atlasDeleteData(URL=atlas_rest_url)
            if response == None:
                log.warning(
                    "No response from DELETE rest call to Atlas when deleting table object"
                )
                raise atlasError

            status_code = response["statusCode"]
            if status_code != 200:
                log.warning(
                    "Request from Atlas when deleting table was %s." % (status_code)
                )
                log.warning(response["data"])
                raise atlasError

        if atlas_error_detected == True:
            raise atlasError
        if atlas_foreign_key_error_detected == True:
            raise foreignKeyError

        log.info("Finished Atlas discovery on connection '%s'" % (self.dbAlias))
        log.debug("Executing atlas_operations.discoverAtlasRdbms() - Finished")

    def disconnectFromJDBC(self):
        log = logging.getLogger(self.loggerName)
        log.debug("Disconnect from JDBC database")

        try:
            self.JDBCCursor.close()
            self.JDBCConn.close()

        except AttributeError:
            pass

        except jpype.JavaException as exception:
            log.info(
                "Disconnection to database over JDBC failed with the following error:"
            )
            log.info(exception.message())
            pass

        except Exception as exception:
            log.info("Unknown error during disconnection to JDBC database:")
            log.info(exception.message())
            pass

        self.JDBCCursor = None

    def connectToJDBC(self, force=False):
        log = logging.getLogger(self.loggerName)

        if self.JDBCCursor == None or force == True:
            log.debug("Connecting to database over JDBC")
            log.debug("	self.jdbc_username = %s" % (self.jdbc_username))
            log.debug("	self.jdbc_password = %s" % (self.jdbc_password))
            log.debug("	self.jdbc_driver = %s" % (self.jdbc_driver))
            log.debug("	self.jdbc_url = %s" % (self.jdbc_url))
            log.debug(
                "	self.jdbc_classpath_for_python = %s"
                % (self.jdbc_classpath_for_python)
            )

            jdbc_credentials = [self.jdbc_username, self.jdbc_password]
            try:
                self.JDBCConn = jaydebeapi.connect(
                    self.jdbc_driver,
                    self.jdbc_url,
                    jdbc_credentials,
                    self.jdbc_classpath_for_python,
                )
                self.JDBCCursor = self.JDBCConn.cursor()
            except jpype.JavaException as exception:
                # 				log.error("Connection to database over JDBC failed with the following error:")
                log.error(exception.message())
                raise connectionError

    def getDBImportSession(self):
        log = logging.getLogger(self.loggerName)
        if self.configDBSession == None:
            self.connectDBImportDB()

        return self.configDBSession()

    def connectDBImportDB(self):
        # Esablish a SQLAlchemy connection to the DBImport database
        log = logging.getLogger(self.loggerName)
        self.connectStr = "mysql+pymysql://%s:%s@%s:%s/%s" % (
            self.configUsername,
            self.configPassword,
            self.configHostname,
            self.configPort,
            self.configDatabase,
        )

        try:
            self.configDBEngine = sa.create_engine(
                self.connectStr, echo=self.debugLogLevel
            )
            self.configDBEngine.connect()
            self.configDBSession = sessionmaker(bind=self.configDBEngine)

        except sa.exc.OperationalError as err:
            raise SQLerror(err)
        except:
            raise SQLerror(sys.exc_info())

        log.info("Connected successful against DBImport database")

    def disconnectDBImportDB(self):
        """Disconnects from the database and removes all sessions and engine"""
        log = logging.getLogger(self.loggerName)

        if self.configDBEngine != None:
            log.info("Disconnecting from DBImport database")
            self.configDBEngine.dispose()
            self.configDBEngine = None

        self.configDBSession = None

    def updateAtlasWithRDBMSdata(
        self, schemaName, tableName, printInfo=True, errorOnWarning=False
    ):
        """This will update Atlas metadata with the information about the remote table schema"""
        log = logging.getLogger(self.loggerName)
        log.debug("Executing atlas_operations.updateAtlasWithSourceSchema()")

        if self.atlasEnabled == False:
            return False

        delete_column_error_found = False
        foreign_key_error_found = False

        if self.source_columns_df.empty == True:
            self.atlasEnabled = False
            log.warning(
                "Atlas opertions class was not initialized correctly. source_columns_df is empty. Atlas integrations disabled"
            )
            log.warning("No columns could be found on the source system.")
            raise invalidConfiguration

        if printInfo == True:
            log.info("Updating Atlas with remote database schema")

        self.tableType = self.sourceSchema.getJdbcTableType(
            self.jdbc_servertype, self.source_columns_df.iloc[0]["TABLE_TYPE"]
        )

        # Get the referredEntities part of the JSON. This is common for both import and export as the rdbms_* in Atlas is the same
        json_data = self.getAtlasRdbmsReferredEntities(
            schemaName=schemaName, tableName=tableName
        )

        # Get the unique names for the rdbms_* entities. This is common for both import and export as the rdbms_* in Atlas is the same
        return_dict = self.getAtlasRdbmsNames(
            schemaName=schemaName, tableName=tableName
        )
        if return_dict == None:
            log.warning(
                "Invalid configuration/data when calculating Atlas names for Rdbms table"
            )
            raise invalidConfiguration

        table_uri = return_dict["tableUri"]
        db_uri = return_dict["dbUri"]
        db_name = return_dict["dbName"]
        instance_uri = return_dict["instanceUri"]
        instance_name = return_dict["instanceName"]
        instance_type = return_dict["instanceType"]

        json_data["entities"] = []

        # Loop through the columns and add them to the JSON
        for index, row in self.source_columns_df.iterrows():
            column_data = {}
            column_data["typeName"] = "rdbms_column"
            column_data["createdBy"] = "DBImport"
            column_data["attributes"] = {}

            column_name = row["SOURCE_COLUMN_NAME"]
            column_uri = self.getAtlasRdbmsColumnURI(
                schemaName=schemaName, tableName=tableName, columnName=column_name
            )
            if row["IS_NULLABLE"] == "YES":
                column_nullable = True
            else:
                column_nullable = False

            column_is_primary_key = False
            for keys_index, keys_row in self.source_keys_df.iterrows():
                if (
                    keys_row["COL_NAME"] == column_name
                    and keys_row["CONSTRAINT_TYPE"] == constant.PRIMARY_KEY
                ):
                    column_is_primary_key = True

            column_data["attributes"]["qualifiedName"] = column_uri
            column_data["attributes"]["owner"] = self.owner

            if (
                row["SOURCE_COLUMN_COMMENT"] != None
                and row["SOURCE_COLUMN_COMMENT"].strip() != ""
            ):
                column_data["attributes"]["comment"] = row["SOURCE_COLUMN_COMMENT"]

            try:
                column_length = int(
                    str(row["SOURCE_COLUMN_LENGTH"]).split(".")[0].split(":")[0]
                )
            except ValueError:
                column_length = None

            if column_length != None:
                column_data["attributes"]["length"] = str(column_length)

            column_data["attributes"]["name"] = column_name
            column_data["attributes"]["data_type"] = row["SOURCE_COLUMN_TYPE"]
            column_data["attributes"]["isNullable"] = column_nullable
            column_data["attributes"]["isPrimaryKey"] = column_is_primary_key
            column_data["attributes"]["table"] = {
                "guid": "-100",
                "typeName": "rdbms_table",
            }

            json_data["entities"].append(column_data)

        log.debug("======================================")
        log.debug("JSON to send to Atlas!")
        log.debug(json.dumps(json_data, indent=3))
        log.debug("======================================")

        response = self.atlasPostData(
            URL=self.atlasRestEntities, data=json.dumps(json_data)
        )
        if response == None:
            log.warning(
                "No response from POST rest call to Atlas when sending the new/updated table object"
            )
            raise atlasError

        status_code = response["statusCode"]
        if status_code != 200:
            log.warning(
                "Request from Atlas when updating source schema was %s." % (status_code)
            )
            log.warning("%s" % (response["data"]))
            self.atlasEnabled == False  # noqa: B015  # TODO(#182): = was meant
            raise atlasError

        # We now have to find columns that exists in DBImport but not in the source anymore.
        # These columns need to be deleted from Atlas

        # Fetch the table from Atlas so we can check all columns configured on it
        atlas_rest_url = "%s/rdbms_table?attr:qualifiedName=%s" % (
            self.atlasRestUniqueAttributeType,
            table_uri,
        )
        response = self.atlasGetData(URL=atlas_rest_url)
        if response == None:
            log.warning(
                "No response from GET rest call to Atlas when reading the table"
            )
            raise atlasError

        status_code = response["statusCode"]
        response_data = json.loads(response["data"])

        if status_code != 200:
            log.warning(
                "Request from Atlas when updating source schema was %s." % (status_code)
            )
            log.warning("%s" % (response["data"]))
            self.atlasEnabled == False  # noqa: B015  # TODO(#182): = was meant
            raise atlasError

        for json_ref_entities in response_data["referredEntities"]:
            json_ref_entity = response_data["referredEntities"][json_ref_entities]
            if json_ref_entity["typeName"] == "rdbms_column":
                # We found on column in the ""referredEntities" part. We will now check the column we found
                column_qualified_name = json_ref_entity["attributes"]["qualifiedName"]
                column_name = json_ref_entity["attributes"]["name"]

                # Loop through the source columns to see if it exists in there
                found_source_column = False
                for index, row in self.source_columns_df.iterrows():
                    source_column_name = row["SOURCE_COLUMN_NAME"]
                    source_column_uri = self.getAtlasRdbmsColumnURI(
                        schemaName=schemaName,
                        tableName=tableName,
                        columnName=source_column_name,
                    )
                    if (
                        source_column_name == column_name
                        and source_column_uri == column_qualified_name
                    ):
                        found_source_column = True

                if (
                    found_source_column == False
                    and json_ref_entity["status"] == "ACTIVE"
                ):
                    # The column defined in Atlas cant be found on the source, and it's still marked as ACTIVE. Lets delete it!
                    log.debug(
                        "Deleting Atlas column with qualifiedName = '%s'"
                        % (column_qualified_name)
                    )

                    atlas_rest_url = "%s/rdbms_column?attr:qualifiedName=%s" % (
                        self.atlasRestUniqueAttributeType,
                        column_qualified_name,
                    )
                    response = self.atlasDeleteData(URL=atlas_rest_url)
                    if response == None:
                        log.warning(
                            "No response from DELETE rest call to Atlas when deleting non-existing columns"
                        )
                        raise atlasError

                    status_code = response["statusCode"]
                    if status_code != 200:
                        log.warning(
                            "Request from Atlas when deleting old columns was %s."
                            % (status_code)
                        )
                        log.warning("%s" % (response["data"]))
                        self.atlasEnabled == False  # noqa: B015  # TODO(#182): = was meant
                        delete_column_error_found = True

        # Code to add Foreign Keys to table. This is done in a new JSON as we reference the table we just created. One JSON per FK

        if self.source_keys_df.empty == False:
            # Select only rows where CONSTRAINT_TYPE = FK and the slice so that we only see the CONSTRAINT_NAME and make it unique
            text_printed = False
            for fk_name in self.source_keys_df.loc[
                self.source_keys_df["CONSTRAINT_TYPE"] == constant.FOREIGN_KEY
            ][["CONSTRAINT_NAME"]].CONSTRAINT_NAME.unique():
                if text_printed == False:
                    if printInfo == True:
                        log.info("Updating Atlas with remote database foreign keys")
                    text_printed = True

                json_data = None
                column_count = 0

                for index, row in self.source_keys_df.loc[
                    self.source_keys_df["CONSTRAINT_NAME"] == fk_name
                ].iterrows():
                    # Each 'row' now contains information about the column in the FK
                    if json_data == None:
                        # The refered table must exists, or we will get a 404 error from Atlas.
                        # We do the check here and exit later depending on status of refTableExists
                        ref_schema_name = row["REFERENCE_SCHEMA_NAME"].strip()
                        ref_table_name = row["REFERENCE_TABLE_NAME"].strip()
                        return_dict = self.getAtlasRdbmsNames(
                            schemaName=ref_schema_name, tableName=ref_table_name
                        )
                        ref_table_uri = return_dict["tableUri"]

                        atlas_rest_url = "%s/rdbms_table?attr:qualifiedName=%s" % (
                            self.atlasRestUniqueAttributeType,
                            ref_table_uri,
                        )
                        response = self.atlasGetData(URL=atlas_rest_url)
                        if response == None:
                            log.warning(
                                "No response from GET rest call to Atlas when checking if table that is used for ForeignKeys exists in Atlas"
                            )
                            raise atlasError

                        status_code = response["statusCode"]
                        if status_code == 200:
                            ref_table_exists = True
                        else:
                            ref_table_exists = False

                        # First time we loop through the columns and jsonData is empty. Lets create the base for the json
                        json_data = self.getAtlasRdbmsReferredEntities(
                            schemaName=schemaName,
                            tableName=tableName,
                            refSchemaName=row["REFERENCE_SCHEMA_NAME"],
                            refTableName=row["REFERENCE_TABLE_NAME"],
                        )

                        entities_data = {}
                        entities_data["typeName"] = "rdbms_foreign_key"
                        entities_data["createdBy"] = "DBImport"
                        entities_data["attributes"] = {}
                        entities_data["attributes"]["qualifiedName"] = "%s.%s" % (
                            fk_name,
                            table_uri,
                        )
                        entities_data["attributes"]["name"] = fk_name
                        entities_data["relationshipAttributes"] = {}
                        entities_data["relationshipAttributes"]["table"] = {
                            "guid": "-100",
                            "typeName": "rdbms_table",
                        }
                        entities_data["relationshipAttributes"]["references_table"] = {
                            "guid": "-101",
                            "typeName": "rdbms_table",
                        }

                        entities_data["relationshipAttributes"]["key_columns"] = []
                        entities_data["relationshipAttributes"][
                            "references_columns"
                        ] = []

                    # We now have a jsonData object with reference to the two tables (on -100 and -101) + db and instance.
                    # Lets add the columns

                    # Reference to the table column
                    source_column_uri = self.getAtlasRdbmsColumnURI(
                        schemaName=schemaName,
                        tableName=tableName,
                        columnName=row["COL_NAME"],
                    )

                    uuid_ref = str(1000 + column_count)
                    json_data["referredEntities"][uuid_ref] = {}
                    json_data["referredEntities"][uuid_ref]["guid"] = uuid_ref
                    json_data["referredEntities"][uuid_ref]["typeName"] = "rdbms_column"
                    json_data["referredEntities"][uuid_ref]["attributes"] = {}
                    json_data["referredEntities"][uuid_ref]["attributes"][
                        "qualifiedName"
                    ] = source_column_uri
                    json_data["referredEntities"][uuid_ref]["attributes"]["name"] = row[
                        "COL_NAME"
                    ]
                    json_data["referredEntities"][uuid_ref]["attributes"]["table"] = {
                        "guid": "-100",
                        "typeName": "rdbms_table",
                    }
                    column_count = column_count + 1

                    key_columns_data = {}
                    key_columns_data["guid"] = uuid_ref
                    key_columns_data["typeName"] = "rdbms_column"
                    entities_data["relationshipAttributes"]["key_columns"].append(
                        key_columns_data
                    )

                    # Reference to the refTable column
                    ref_column_uri = self.getAtlasRdbmsColumnURI(
                        schemaName=row["REFERENCE_SCHEMA_NAME"],
                        tableName=row["REFERENCE_TABLE_NAME"],
                        columnName=row["REFERENCE_COL_NAME"],
                    )

                    uuid_ref = str(1000 + column_count)
                    json_data["referredEntities"][uuid_ref] = {}
                    json_data["referredEntities"][uuid_ref]["guid"] = uuid_ref
                    json_data["referredEntities"][uuid_ref]["typeName"] = "rdbms_column"
                    json_data["referredEntities"][uuid_ref]["attributes"] = {}
                    json_data["referredEntities"][uuid_ref]["attributes"][
                        "qualifiedName"
                    ] = ref_column_uri
                    json_data["referredEntities"][uuid_ref]["attributes"]["name"] = row[
                        "REFERENCE_COL_NAME"
                    ]
                    json_data["referredEntities"][uuid_ref]["attributes"]["table"] = {
                        "guid": "-101",
                        "typeName": "rdbms_table",
                    }
                    column_count = column_count + 1

                    ref_columns_data = {}
                    ref_columns_data["guid"] = uuid_ref
                    ref_columns_data["typeName"] = "rdbms_column"
                    entities_data["relationshipAttributes"][
                        "references_columns"
                    ].append(ref_columns_data)

                json_data["entities"] = []
                json_data["entities"].append(entities_data)

                if ref_table_exists == True:
                    log.debug(json.dumps(json_data, indent=3))

                    response = self.atlasPostData(
                        URL=self.atlasRestEntities, data=json.dumps(json_data)
                    )
                    if response == None:
                        log.warning(
                            "No response from POST rest call to Atlas when adding/changeing ForeignKeys data"
                        )
                        raise atlasError

                    status_code = response["statusCode"]
                    if status_code != 200:
                        log.warning(
                            "Request from Atlas when creating Foreign Keys was %s"
                            % (status_code)
                        )
                        log.warning("%s" % (response["data"]))
                        self.atlasEnabled == False  # noqa: B015  # TODO(#182): = was meant
                        foreign_key_error_found = True
                    else:
                        log.debug(
                            "Creating/updating Atlas ForeignKey against %s.%s"
                            % (ref_schema_name, ref_table_name)
                        )
                else:
                    log.warning(
                        "Foreign Key cant be created as refered table(%s.%s) does not exists in Atlas"
                        % (ref_schema_name, ref_table_name)
                    )
                    foreign_key_error_found = True

        if errorOnWarning == True:
            if foreign_key_error_found == True:
                raise foreignKeyError
            if delete_column_error_found == True:
                raise atlasError

        log.debug("Executing atlas_operations.updateAtlasWithSourceSchema() - Finished")

    def getAtlasRdbmsColumnURI(self, schemaName, tableName, columnName):
        """Returns a string with the correct column URI for Atlas"""
        log = logging.getLogger(self.loggerName)
        log.debug("Executing atlas_operations.getAtlasRdbmsColumnURI()")

        if self.atlasEnabled == False:
            return None

        if self.jdbc_servertype == constant.ORACLE:
            if self.jdbc_oracle_sid != None and self.jdbc_oracle_sid != "None":
                oracle_db = self.jdbc_oracle_sid
            if (
                self.jdbc_oracle_servicename != None
                and self.jdbc_oracle_servicename != "None"
            ):
                oracle_db = self.jdbc_oracle_servicename
            column_uri = "%s.%s.%s.%s@%s:%s" % (
                oracle_db,
                schemaName,
                tableName,
                columnName,
                self.jdbc_hostname,
                self.jdbc_port,
            )

        elif self.jdbc_servertype == constant.MSSQL:
            column_uri = "%s.%s.%s.%s@%s:%s" % (
                self.jdbc_database,
                schemaName,
                tableName,
                columnName,
                self.jdbc_hostname,
                self.jdbc_port,
            )

        elif self.jdbc_servertype == constant.MYSQL:
            column_uri = "%s.%s.%s@%s:%s" % (
                self.jdbc_database,
                tableName,
                columnName,
                self.jdbc_hostname,
                self.jdbc_port,
            )

        elif self.jdbc_servertype == constant.POSTGRESQL:
            column_uri = "%s.%s.%s.%s@%s:%s" % (
                self.jdbc_database,
                schemaName,
                tableName,
                columnName,
                self.jdbc_hostname,
                self.jdbc_port,
            )

        elif self.jdbc_servertype == constant.PROGRESS:
            column_uri = "%s.%s.%s.%s@%s:%s" % (
                self.jdbc_database,
                schemaName,
                tableName,
                columnName,
                self.jdbc_hostname,
                self.jdbc_port,
            )

        elif self.jdbc_servertype == constant.DB2_UDB:
            column_uri = "%s.%s.%s.%s@%s:%s" % (
                self.jdbc_database,
                schemaName,
                tableName,
                columnName,
                self.jdbc_hostname,
                self.jdbc_port,
            )

        elif self.jdbc_servertype == constant.DB2_AS400:
            column_uri = "%s.%s.%s.%s@%s:%s" % (
                self.jdbc_database,
                schemaName,
                tableName,
                columnName,
                self.jdbc_hostname,
                self.jdbc_port,
            )

        elif self.jdbc_servertype == constant.INFORMIX:
            column_uri = "%s.%s.%s.%s@%s:%s" % (
                self.jdbc_database,
                schemaName,
                tableName,
                columnName,
                self.jdbc_hostname,
                self.jdbc_port,
            )

        else:
            logging.warning(
                "The Atlas integrations does not support this database type. Atlas integration disabled"
            )
            self.atlasEnabled = False
            return None

        log.debug("Executing atlas_operations.getAtlasRdbmsColumnURI() - Finished")
        return column_uri

    def getAtlasRdbmsNames(self, schemaName, tableName):
        """Returns a dict with the names needed for the rdbms_* configuration, depending on database type"""
        log = logging.getLogger(self.loggerName)
        log.debug("Executing atlas_operations.getAtlasRdbmsNames()")

        table_uri = None
        db_uri = None
        instance_uri = None
        instance_name = None
        instance_type = None
        environment_type = None

        if self.atlasEnabled == False:
            return None

        if self.jdbc_environment == None:
            environment_type = "Unspecified"
        else:
            environment_type = self.jdbc_environment

        if self.jdbc_servertype == constant.ORACLE:
            if self.jdbc_oracle_sid != None and self.jdbc_oracle_sid != "None":
                oracle_db = self.jdbc_oracle_sid
            if (
                self.jdbc_oracle_servicename != None
                and self.jdbc_oracle_servicename != "None"
            ):
                oracle_db = self.jdbc_oracle_servicename

            table_uri = "%s.%s.%s@%s:%s" % (
                oracle_db,
                schemaName,
                tableName,
                self.jdbc_hostname,
                self.jdbc_port,
            )
            db_uri = "%s@%s:%s" % (oracle_db, self.jdbc_hostname, self.jdbc_port)
            db_name = oracle_db
            instance_uri = "Oracle@%s:%s" % (self.jdbc_hostname, self.jdbc_port)
            instance_name = "Oracle"
            instance_type = "Oracle"

        elif self.jdbc_servertype == constant.MSSQL:
            table_uri = "%s.%s.%s@%s:%s" % (
                self.jdbc_database,
                schemaName,
                tableName,
                self.jdbc_hostname,
                self.jdbc_port,
            )
            db_uri = "%s@%s:%s" % (
                self.jdbc_database,
                self.jdbc_hostname,
                self.jdbc_port,
            )
            db_name = self.jdbc_database
            instance_uri = "MSSQL@%s:%s" % (self.jdbc_hostname, self.jdbc_port)
            instance_name = "MSSQL"
            instance_type = "MSSQL"

        elif self.jdbc_servertype == constant.MYSQL:
            table_uri = "%s.%s@%s:%s" % (
                self.jdbc_database,
                tableName,
                self.jdbc_hostname,
                self.jdbc_port,
            )
            db_uri = "%s@%s:%s" % (
                self.jdbc_database,
                self.jdbc_hostname,
                self.jdbc_port,
            )
            db_name = self.jdbc_database
            instance_uri = "MySQL@%s:%s" % (self.jdbc_hostname, self.jdbc_port)
            instance_name = "MySQL"
            instance_type = "MySQL"

        elif self.jdbc_servertype == constant.POSTGRESQL:
            table_uri = "%s.%s.%s@%s:%s" % (
                self.jdbc_database,
                schemaName,
                tableName,
                self.jdbc_hostname,
                self.jdbc_port,
            )
            db_uri = "%s@%s:%s" % (
                self.jdbc_database,
                self.jdbc_hostname,
                self.jdbc_port,
            )
            db_name = self.jdbc_database
            instance_uri = "PostgreSQL@%s:%s" % (self.jdbc_hostname, self.jdbc_port)
            instance_name = "PostgreSQL"
            instance_type = "PostgreSQL"

        elif self.jdbc_servertype == constant.PROGRESS:
            table_uri = "%s.%s.%s@%s:%s" % (
                self.jdbc_database,
                schemaName,
                tableName,
                self.jdbc_hostname,
                self.jdbc_port,
            )
            db_uri = "%s@%s:%s" % (
                self.jdbc_database,
                self.jdbc_hostname,
                self.jdbc_port,
            )
            db_name = self.jdbc_database
            instance_uri = "Progress@%s:%s" % (self.jdbc_hostname, self.jdbc_port)
            instance_name = "Progress"
            instance_type = "Progress"

        elif self.jdbc_servertype == constant.DB2_UDB:
            table_uri = "%s.%s.%s@%s:%s" % (
                self.jdbc_database,
                schemaName,
                tableName,
                self.jdbc_hostname,
                self.jdbc_port,
            )
            db_uri = "%s@%s:%s" % (
                self.jdbc_database,
                self.jdbc_hostname,
                self.jdbc_port,
            )
            db_name = self.jdbc_database
            instance_uri = "DB2UDB@%s:%s" % (self.jdbc_hostname, self.jdbc_port)
            instance_name = "DB2"
            instance_type = "DB2"

        elif self.jdbc_servertype == constant.DB2_AS400:
            table_uri = "%s.%s.%s@%s:%s" % (
                self.jdbc_database,
                schemaName,
                tableName,
                self.jdbc_hostname,
                self.jdbc_port,
            )
            db_uri = "%s@%s:%s" % (
                self.jdbc_database,
                self.jdbc_hostname,
                self.jdbc_port,
            )
            db_name = self.jdbc_database
            instance_uri = "DB2AS400@%s:%s" % (self.jdbc_hostname, self.jdbc_port)
            instance_name = "DB2"
            instance_type = "DB2"

        elif self.jdbc_servertype == constant.INFORMIX:
            table_uri = "%s.%s.%s@%s:%s" % (
                self.jdbc_database,
                schemaName,
                tableName,
                self.jdbc_hostname,
                self.jdbc_port,
            )
            db_uri = "%s@%s:%s" % (
                self.jdbc_database,
                self.jdbc_hostname,
                self.jdbc_port,
            )
            db_name = self.jdbc_database
            instance_uri = "INFORMIX@%s:%s" % (self.jdbc_hostname, self.jdbc_port)
            instance_name = "INFORMIX"
            instance_type = "INFORMIX"

        else:
            logging.warning(
                "The Atlas integrations does not support this database type. Atlas integration disabled"
            )
            self.atlasEnabled = False
            return None

        return_dict = {}
        return_dict["tableUri"] = table_uri
        return_dict["dbUri"] = db_uri
        return_dict["dbName"] = db_name
        return_dict["instanceUri"] = instance_uri
        return_dict["instanceName"] = instance_name
        return_dict["instanceType"] = instance_type
        return_dict["environmentType"] = environment_type

        log.debug("Executing atlas_operations.getAtlasRdbmsNames() - Finished")
        return return_dict

    def updateAtlasWithImportLineage(
        self,
        Hive_DB,
        Hive_Table,
        sourceSchema,
        sourceTable,
        sqoopHdfsLocation,
        startStopDict,
        fullExecutedCommand,
        importTool,
    ):
        """This will update Atlas lineage for the import process"""
        log = logging.getLogger(self.loggerName)
        log.debug("Executing atlas_operations.updateAtlasWithImportLineage()")

        if self.atlasEnabled == False:
            return

        log.info("Updating Atlas with import lineage")

        # Get the referredEntities part of the JSON. This is common for both import and export as the rdbms_* in Atlas is the same
        json_data = self.getAtlasRdbmsReferredEntities(
            schemaName=sourceSchema, tableName=sourceTable, hdfsPath=sqoopHdfsLocation
        )

        # Get the unique names for the rdbms_* entities. This is common for both import and export as the rdbms_* in Atlas is the same
        return_dict = self.getAtlasRdbmsNames(
            schemaName=sourceSchema, tableName=sourceTable
        )
        if return_dict == None:
            return

        table_uri = return_dict["tableUri"]
        db_uri = return_dict["dbUri"]
        db_name = return_dict["dbName"]
        instance_uri = return_dict["instanceUri"]
        instance_name = return_dict["instanceName"]
        instance_type = return_dict["instanceType"]

        process_name = "%s.%s@DBimport" % (Hive_DB, Hive_Table)

        lineage_data = {}
        lineage_data["typeName"] = "DBImport_Process"
        lineage_data["createdBy"] = "DBImport"
        lineage_data["attributes"] = {}
        lineage_data["attributes"]["qualifiedName"] = process_name
        lineage_data["attributes"]["uri"] = process_name
        lineage_data["attributes"]["name"] = process_name
        lineage_data["attributes"]["operation"] = "import"
        lineage_data["attributes"]["commandlineOpts"] = fullExecutedCommand
        lineage_data["attributes"]["description"] = "Import of %s.%s" % (
            Hive_DB,
            Hive_Table,
        )
        lineage_data["attributes"]["startTime"] = startStopDict["startTime"]
        lineage_data["attributes"]["endTime"] = startStopDict["stopTime"]
        lineage_data["attributes"]["userName"] = getpass.getuser()
        lineage_data["attributes"]["importTool"] = importTool
        lineage_data["attributes"]["inputs"] = [
            {"guid": "-100", "typeName": "rdbms_table"}
        ]
        lineage_data["attributes"]["outputs"] = [
            {"guid": "-200", "typeName": "hdfs_path"}
        ]

        json_data["entities"] = []
        json_data["entities"].append(lineage_data)

        logging.debug(json.dumps(json_data, indent=3))

        response = self.atlasPostData(
            URL=self.atlasRestEntities, data=json.dumps(json_data)
        )
        status_code = response["statusCode"]
        if status_code != 200:
            log.warning(
                "Request from Atlas when updating import lineage was %s."
                % (status_code)
            )
            self.atlasEnabled == False  # noqa: B015  # TODO(#182): = was meant

        log.debug(
            "Executing atlas_operations.updateAtlasWithImportLineage() - Finished"
        )

    def updateAtlasWithExportLineage(
        self,
        hiveDB,
        hiveTable,
        hiveExportTempDB,
        hiveExportTempTable,
        targetSchema,
        targetTable,
        tempTableNeeded,
        startStopDict,
        fullExecutedCommand,
        exportTool,
    ):
        """This will update Atlas lineage for the export process"""
        log = logging.getLogger(self.loggerName)
        log.debug("Executing atlas_operations.updateAtlasWithExportLineage()")

        if self.atlasEnabled == False:
            return

        log.info("Updating Atlas with export lineage")

        # Get the referredEntities part of the JSON. This is common for both import and export as the rdbms_* in Atlas is the same
        json_data = self.getAtlasRdbmsReferredEntities(
            schemaName=targetSchema,
            tableName=targetTable,
        )

        # Get the unique names for the rdbms_* entities. This is common for both import and export as the rdbms_* in Atlas is the same
        return_dict = self.getAtlasRdbmsNames(
            schemaName=targetSchema, tableName=targetTable
        )
        if return_dict == None:
            return

        table_uri = return_dict["tableUri"]
        db_uri = return_dict["dbUri"]
        db_name = return_dict["dbName"]
        instance_uri = return_dict["instanceUri"]
        instance_name = return_dict["instanceName"]
        instance_type = return_dict["instanceType"]

        process_name = "%s.%s.%s@DBimport" % (self.dbAlias, targetSchema, targetTable)

        if tempTableNeeded == True:
            hive_qualified_name = "%s.%s@%s" % (
                hiveExportTempDB,
                hiveExportTempTable,
                self.clusterName,
            )
        else:
            hive_qualified_name = "%s.%s@%s" % (hiveDB, hiveTable, self.clusterName)

        json_data["referredEntities"]["-500"] = {}
        json_data["referredEntities"]["-500"]["guid"] = "-500"
        json_data["referredEntities"]["-500"]["typeName"] = "hive_table"
        json_data["referredEntities"]["-500"]["attributes"] = {}
        json_data["referredEntities"]["-500"]["attributes"]["qualifiedName"] = (
            hive_qualified_name
        )
        json_data["referredEntities"]["-500"]["attributes"]["name"] = hiveTable

        lineage_data = {}
        lineage_data["typeName"] = "DBImport_Process"
        lineage_data["createdBy"] = "DBImport"
        lineage_data["attributes"] = {}
        lineage_data["attributes"]["qualifiedName"] = process_name
        lineage_data["attributes"]["uri"] = process_name
        lineage_data["attributes"]["name"] = process_name
        lineage_data["attributes"]["operation"] = "export"
        lineage_data["attributes"]["commandlineOpts"] = fullExecutedCommand
        lineage_data["attributes"]["description"] = "Export of %s.%s" % (
            hiveDB,
            hiveTable,
        )
        lineage_data["attributes"]["startTime"] = startStopDict["startTime"]
        lineage_data["attributes"]["endTime"] = startStopDict["stopTime"]
        lineage_data["attributes"]["userName"] = getpass.getuser()
        lineage_data["attributes"]["exportTool"] = exportTool
        lineage_data["attributes"]["inputs"] = [
            {"guid": "-500", "typeName": "hive_table"}
        ]
        lineage_data["attributes"]["outputs"] = [
            {"guid": "-100", "typeName": "rdbms_table"}
        ]

        json_data["entities"] = []
        json_data["entities"].append(lineage_data)

        logging.debug(json.dumps(json_data, indent=3))

        response = self.atlasPostData(
            URL=self.atlasRestEntities, data=json.dumps(json_data)
        )
        status_code = response["statusCode"]
        if status_code != 200:
            log.warning(
                "Request from Atlas when updating import lineage was %s."
                % (status_code)
            )
            self.atlasEnabled == False  # noqa: B015  # TODO(#182): = was meant

        log.debug(
            "Executing atlas_operations.updateAtlasWithExportLineage() - Finished"
        )
