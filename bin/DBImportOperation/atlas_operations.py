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


class atlasOperation(object):
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

        returnDict = self.getAtlasRdbmsNames(schemaName=schemaName, tableName=tableName)
        if returnDict == None:
            return

        tableUri = returnDict["tableUri"]
        dbUri = returnDict["dbUri"]
        dbName = returnDict["dbName"]
        instanceUri = returnDict["instanceUri"]
        instanceName = returnDict["instanceName"]
        instanceType = returnDict["instanceType"]
        environmentType = returnDict["environmentType"]

        if refSchemaName != "" and refTableName != "":
            returnDict = self.getAtlasRdbmsNames(
                schemaName=refSchemaName, tableName=refTableName
            )
            if returnDict == None:
                return

            refTableUri = returnDict["tableUri"]

        # TODO: Get the following information from dbimport and source system
        # Index

        jsonData = {}
        jsonData["referredEntities"] = {}

        if len(self.source_columns_df) > 0:
            tableCreateTime = self.source_columns_df.iloc[0]["TABLE_CREATE_TIME"]
            tableComment = self.source_columns_df.iloc[0]["TABLE_COMMENT"]
            # 			tableType = self.getJdbcTableType()

            jsonData["referredEntities"]["-100"] = {}
            jsonData["referredEntities"]["-100"]["guid"] = "-100"
            jsonData["referredEntities"]["-100"]["typeName"] = "rdbms_table"
            jsonData["referredEntities"]["-100"]["attributes"] = {}
            jsonData["referredEntities"]["-100"]["attributes"]["qualifiedName"] = (
                tableUri
            )
            jsonData["referredEntities"]["-100"]["attributes"]["name"] = tableName
            if schemaName == "-":
                jsonData["referredEntities"]["-100"]["attributes"]["name_path"] = None
            else:
                jsonData["referredEntities"]["-100"]["attributes"]["name_path"] = (
                    schemaName
                )
            jsonData["referredEntities"]["-100"]["attributes"]["contact_info"] = (
                self.contactInfo
            )
            jsonData["referredEntities"]["-100"]["attributes"]["owner"] = self.owner
            jsonData["referredEntities"]["-100"]["attributes"]["comment"] = tableComment
            jsonData["referredEntities"]["-100"]["attributes"]["type"] = self.tableType
            if tableCreateTime != None:
                # Atlas only support fractions of a second with 3 digits. So we remove the last 3 as python %f gives 6 digits
                # This datetime object from the source database needs to be in UTC. We might have to convert it
                jsonData["referredEntities"]["-100"]["attributes"]["createTime"] = (
                    tableCreateTime.strftime("%Y-%m-%dT%H:%M:%S.%f")[0:-3] + "Z"
                )
            jsonData["referredEntities"]["-100"]["attributes"]["db"] = {
                "guid": "-300",
                "typeName": "rdbms_db",
            }

            if refSchemaName != "" and refTableName != "":
                jsonData["referredEntities"]["-101"] = {}
                jsonData["referredEntities"]["-101"]["guid"] = "-101"
                jsonData["referredEntities"]["-101"]["typeName"] = "rdbms_table"
                jsonData["referredEntities"]["-101"]["attributes"] = {}
                jsonData["referredEntities"]["-101"]["attributes"]["qualifiedName"] = (
                    refTableUri
                )
                jsonData["referredEntities"]["-101"]["attributes"]["name"] = (
                    refTableName
                )
                jsonData["referredEntities"]["-101"]["attributes"]["db"] = {
                    "guid": "-300",
                    "typeName": "rdbms_db",
                }

            if hdfsPath != "":
                # 				hdfsAddress = self.getConfigValue(key = "hdfs_address")
                # 				clusterName = self.getConfigValue(key = "cluster_name")
                hdfsUri = "%s%s@%s" % (self.hdfsAddress, hdfsPath, self.clusterName)
                hdfsFullPath = "%s%s" % (self.hdfsAddress, hdfsPath)

                jsonData["referredEntities"]["-200"] = {}
                jsonData["referredEntities"]["-200"]["guid"] = "-200"
                jsonData["referredEntities"]["-200"]["typeName"] = "hdfs_path"
                jsonData["referredEntities"]["-200"]["attributes"] = {}
                jsonData["referredEntities"]["-200"]["attributes"]["qualifiedName"] = (
                    hdfsUri
                )
                jsonData["referredEntities"]["-200"]["attributes"]["name"] = hdfsPath
                jsonData["referredEntities"]["-200"]["attributes"]["path"] = (
                    hdfsFullPath
                )
                jsonData["referredEntities"]["-200"]["attributes"]["clusterName"] = (
                    self.clusterName
                )
                jsonData["referredEntities"]["-200"]["attributes"]["nameServiceId"] = (
                    self.clusterName
                )

            jsonData["referredEntities"]["-300"] = {}
            jsonData["referredEntities"]["-300"]["guid"] = "-300"
            jsonData["referredEntities"]["-300"]["typeName"] = "rdbms_db"
            jsonData["referredEntities"]["-300"]["attributes"] = {}
            jsonData["referredEntities"]["-300"]["attributes"]["qualifiedName"] = dbUri
            jsonData["referredEntities"]["-300"]["attributes"]["name"] = dbName
            jsonData["referredEntities"]["-300"]["attributes"]["contact_info"] = (
                self.contactInfo
            )
            jsonData["referredEntities"]["-300"]["attributes"]["owner"] = self.owner
            jsonData["referredEntities"]["-300"]["attributes"]["description"] = (
                self.description
            )
            jsonData["referredEntities"]["-300"]["attributes"]["prodOrOther"] = (
                environmentType
            )
            jsonData["referredEntities"]["-300"]["attributes"]["instance"] = {
                "guid": "-400",
                "typeName": "rdbms_instance",
            }

        jsonData["referredEntities"]["-400"] = {}
        jsonData["referredEntities"]["-400"]["guid"] = "-400"
        jsonData["referredEntities"]["-400"]["typeName"] = "rdbms_instance"
        jsonData["referredEntities"]["-400"]["attributes"] = {}
        jsonData["referredEntities"]["-400"]["attributes"]["qualifiedName"] = (
            instanceUri
        )
        jsonData["referredEntities"]["-400"]["attributes"]["name"] = instanceName
        jsonData["referredEntities"]["-400"]["attributes"]["rdbms_type"] = instanceType
        jsonData["referredEntities"]["-400"]["attributes"]["hostname"] = (
            self.jdbc_hostname
        )
        jsonData["referredEntities"]["-400"]["attributes"]["port"] = self.jdbc_port
        jsonData["referredEntities"]["-400"]["attributes"]["platform"] = ""
        jsonData["referredEntities"]["-400"]["attributes"]["protocol"] = "jdbc"

        return jsonData

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
                statusCode = "-1"
            else:
                statusCode = response["statusCode"]
                try:
                    responseData = json.loads(response["data"])
                except json.decoder.JSONDecodeError as errMsg:
                    responseData = None
                    log.error(
                        "Atlas schema check for DBIMportProcess TypeDef did not return a valid JSON. Data returned:\n%s"
                        % (response["data"])
                    )
                    log.error(errMsg)
                    log.warning("Disable Atlas integration")
                    self.atlasEnabled = False

        if self.atlasEnabled == True and statusCode == 404:
            # The TypeDef was not found
            log.warning(
                "The Atlas typeDef 'DBImport_Process' was not found. Did you run the Atlas setup for DBImport?"
            )
            self.atlasEnabled = False

        elif self.atlasEnabled == True and statusCode != 200:
            log.warning("Atlas communication failed with response %s" % (statusCode))
            self.atlasEnabled = False

        if self.atlasEnabled == True:
            # if self.atlasEnabled = True at this stage, it means that we have a valid json response in DBImportProcessJSON
            typeDefVersion = responseData["typeVersion"]
            if typeDefVersion != "1.2":
                log.warning(
                    "The version for Atlas typeDef 'DBImport_Process' is not correct. Version required is 1.2 and version found is %s"
                    % (typeDefVersion)
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

        returnValue = None
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

                returnValue = {
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
        return returnValue

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
            tablesAndViewsDF = self.sourceSchema.getJDBCtablesAndViews(
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

        if len(tablesAndViewsDF) == 0:
            createEmptyDatabaseInAtlas = True
        else:
            createEmptyDatabaseInAtlas = False

        if self.atlasIncludeFilter == None or self.atlasIncludeFilter == "":
            # Set default to include the table to True if there is no include filter
            tablesAndViewsDF.insert(2, "include", True)
        else:
            # Set default to include the table to False if there is an include filter
            tablesAndViewsDF.insert(2, "include", False)

        # Iterate over DF to determine what tables we should discover and what to not discover based on include/exclude filter
        # This will add a new column to the DF with True/False values depending on if we are going to discover it or not

        for index, row in tablesAndViewsDF.iterrows():
            updateAtlasWithTable = False
            schema = row["schema"].lower()
            table = row["table"].lower()

            # Filter the schema.table so that only the included tables gets processed
            for include in self.atlasIncludeFilter.split(";"):
                try:
                    includeSchema = include.split(".")[0].replace("*", ".*").lower()
                    includeTable = include.split(".")[1].replace("*", ".*").lower()
                    if re.search(
                        "^" + includeSchema + "$", schema.strip()
                    ) and re.search("^" + includeTable + "$", table.strip()):
                        tablesAndViewsDF.at[index, "include"] = True
                        break
                except IndexError:
                    pass

            # Filter the schema.table so we remove the excluded items
            for exclude in self.atlasExcludeFilter.split(";"):
                try:
                    excludeSchema = exclude.split(".")[0].replace("*", ".*").lower()
                    excludeTable = exclude.split(".")[1].replace("*", ".*").lower()
                    if re.search(
                        "^" + excludeSchema + "$", schema.strip()
                    ) and re.search("^" + excludeTable + "$", table.strip()):
                        tablesAndViewsDF.at[index, "include"] = False
                        break
                except IndexError:
                    pass

        # Remove all rows from dataframe where include = False.
        tablesAndViewsDF.drop(
            tablesAndViewsDF[tablesAndViewsDF.include == False].index, inplace=True
        )

        atlasColumnCache = aliased(configSchema.atlasColumnCache)
        atlasKeyCache = aliased(configSchema.atlasKeyCache)
        self.source_columns_df = pd.DataFrame()
        self.source_keys_df = pd.DataFrame()

        atlasForeignKeyErrorDetected = False
        atlasErrorDetected = False

        # Iterate over the schemas that we have tables to process in
        for schema in tablesAndViewsDF.schema.unique():
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

                fullSchemaColumnDF = self.sourceSchema.readTableColumns(
                    self.JDBCCursor,
                    serverType=self.jdbc_servertype,
                    database=self.jdbc_database,
                    schema=schema,
                )

                fullSchemaKeyDF = self.sourceSchema.readTableKeys(
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
                fullSchemaColumnDF["SCHEMA_NAME"] = "-"
                schema = "-"

            log.debug(
                "DataFrame for schema '%s' was received from the database" % (schema)
            )

            try:
                session = self.getDBImportSession()

                fullCacheSourceColumnDF = pd.DataFrame(
                    session.query(
                        atlasColumnCache.hostname,
                        atlasColumnCache.port,
                        atlasColumnCache.database_name,
                        atlasColumnCache.schema_name,
                        atlasColumnCache.table_name,
                        atlasColumnCache.table_comment,
                        atlasColumnCache.column_name,
                        atlasColumnCache.column_type,
                        atlasColumnCache.column_length,
                        atlasColumnCache.column_comment,
                        atlasColumnCache.column_is_nullable,
                        atlasColumnCache.table_type,
                        atlasColumnCache.table_create_time,
                        atlasColumnCache.default_value,
                    )
                    .select_from(atlasColumnCache)
                    .filter(atlasColumnCache.hostname == self.jdbc_hostname)
                    .filter(atlasColumnCache.port == self.jdbc_port)
                    .filter(atlasColumnCache.database_name == self.jdbc_database)
                    .filter(atlasColumnCache.schema_name == schema.strip())
                    .all()
                )

                session.close()

            except SQLAlchemyError as e:
                log.error(str(e.__dict__["orig"]))
                session.rollback()
                self.disconnectDBImportDB()
                raise SQLerror

            if len(fullCacheSourceColumnDF) > 0:
                # Set oldes timestamp to 1970-01-01 00:00:00.000
                fullCacheSourceColumnDF["table_create_time"] = fullCacheSourceColumnDF[
                    "table_create_time"
                ].clip(lower=pd.Timestamp("1970-01-01 00:00:00.000"))

            log.debug(
                "Column cache for schema '%s' was received from the database" % (schema)
            )

            try:
                session = self.getDBImportSession()

                fullCacheSourceKeyDF = pd.DataFrame(
                    session.query(
                        atlasKeyCache.hostname,
                        atlasKeyCache.port,
                        atlasKeyCache.database_name,
                        atlasKeyCache.schema_name,
                        atlasKeyCache.table_name,
                        atlasKeyCache.constraint_name,
                        atlasKeyCache.constraint_type,
                        atlasKeyCache.column_name,
                        atlasKeyCache.reference_schema_name,
                        atlasKeyCache.reference_table_name,
                        atlasKeyCache.reference_column_name,
                        atlasKeyCache.col_key_position,
                    )
                    .select_from(atlasKeyCache)
                    .filter(atlasKeyCache.hostname == self.jdbc_hostname)
                    .filter(atlasKeyCache.port == self.jdbc_port)
                    .filter(atlasKeyCache.database_name == self.jdbc_database)
                    .filter(atlasKeyCache.schema_name == schema.strip())
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
            if len(tablesAndViewsDF) == 0:
                sourceColumnDF = pd.DataFrame()
                sourceKeyDF = pd.DataFrame()
                self.source_columns_df = pd.DataFrame()
                self.source_keys_df = pd.DataFrame()

            # Loop through all tables and find if there is a difference between values from database and values in cache
            # If there is a difference, we update atlas and update the cache tables
            for index, row in tablesAndViewsDF[
                tablesAndViewsDF["schema"] == schema
            ].iterrows():
                schema = row["schema"]
                table = row["table"]

                self.source_columns_df = fullSchemaColumnDF[
                    (fullSchemaColumnDF["SCHEMA_NAME"] == schema.strip())
                    & (fullSchemaColumnDF["TABLE_NAME"] == table.strip())
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

                    sourceColumnDF = self.source_columns_df.copy()
                    sourceColumnDF = sourceColumnDF.replace({np.nan: None})
                    sourceColumnDF.insert(0, "hostname", self.jdbc_hostname.lower())
                    sourceColumnDF.insert(1, "port", self.jdbc_port)
                    sourceColumnDF.insert(2, "database_name", self.jdbc_database)
                    sourceColumnDF.insert(3, "schema_name", schema)
                    sourceColumnDF.insert(4, "table_name", table)
                    sourceColumnDF.rename(
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
                    sourceColumnDF["table_create_time"] = sourceColumnDF[
                        "table_create_time"
                    ].astype("datetime64[s]")
                    sourceColumnDF.replace({pd.NaT: None}, inplace=True)
                    sourceColumnDF.replace(
                        to_replace=[r"\\t|\\n|\\r", "\t|\n|\r"],
                        value="",
                        regex=True,
                        inplace=True,
                    )
                    sourceColumnDF["column_length"] = sourceColumnDF[
                        "column_length"
                    ].astype("str")
                    sourceColumnDF.sort_values(
                        by="column_name",
                        ignore_index=True,
                        inplace=True,
                        key=lambda col: col.str.lower(),
                    )

                else:
                    sourceColumnDF = pd.DataFrame()

                if len(fullSchemaKeyDF) > 0:
                    self.source_keys_df = fullSchemaKeyDF[
                        (fullSchemaKeyDF["SCHEMA_NAME"] == schema.strip())
                        & (fullSchemaKeyDF["TABLE_NAME"] == table.strip())
                    ].copy()
                    self.source_keys_df.drop(
                        columns=["SCHEMA_NAME", "TABLE_NAME"], inplace=True
                    )
                else:
                    self.source_keys_df = pd.DataFrame()

                if len(self.source_keys_df) > 0:
                    sourceKeyDF = self.source_keys_df.copy()
                    sourceKeyDF = sourceKeyDF.replace({np.nan: None})
                    sourceKeyDF.insert(0, "hostname", self.jdbc_hostname.lower())
                    sourceKeyDF.insert(1, "port", self.jdbc_port)
                    sourceKeyDF.insert(2, "database_name", self.jdbc_database)
                    sourceKeyDF.insert(3, "schema_name", schema)
                    sourceKeyDF.insert(4, "table_name", table)
                    sourceKeyDF.rename(
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
                    sourceKeyDF.sort_values(
                        by=["constraint_name", "column_name"],
                        ignore_index=True,
                        inplace=True,
                        key=lambda col: col.str.lower(),
                    )
                    sourceKeyDF.replace(
                        to_replace=[r"\\t|\\n|\\r", "\t|\n|\r"],
                        value="",
                        regex=True,
                        inplace=True,
                    )
                else:
                    sourceKeyDF = pd.DataFrame()

                if len(fullCacheSourceColumnDF) > 0:
                    cacheSourceColumnDF = fullCacheSourceColumnDF[
                        fullCacheSourceColumnDF["table_name"] == table
                    ].copy()
                    if len(cacheSourceColumnDF) > 0:
                        cacheSourceColumnDF.replace({pd.NaT: None}, inplace=True)
                        # Sort by column name in lowercase
                        cacheSourceColumnDF.sort_values(
                            by="column_name",
                            ignore_index=True,
                            inplace=True,
                            key=lambda col: col.str.lower(),
                        )
                    else:
                        cacheSourceColumnDF = pd.DataFrame()
                else:
                    cacheSourceColumnDF = pd.DataFrame()

                if len(fullCacheSourceKeyDF) > 0:
                    cacheSourceKeyDF = fullCacheSourceKeyDF[
                        fullCacheSourceKeyDF["table_name"] == table
                    ].copy()
                    if len(cacheSourceKeyDF) > 0:
                        # Sort by column name in lowercase
                        cacheSourceKeyDF.sort_values(
                            by=["constraint_name", "column_name"],
                            ignore_index=True,
                            inplace=True,
                            key=lambda col: col.str.lower(),
                        )
                    else:
                        cacheSourceKeyDF = pd.DataFrame()
                else:
                    cacheSourceKeyDF = pd.DataFrame()

                schema = row["schema"].strip()
                table = row["table"].strip()

                if 0 == 1:
                    # Used for debugging
                    if cacheSourceColumnDF.equals(sourceColumnDF):
                        print("columns equal")
                    if cacheSourceKeyDF.equals(sourceKeyDF):
                        print("keys equal")
                    pd.set_option("display.max_columns", None)
                    pd.set_option("display.max_rows", None)

                    print(sourceColumnDF)
                    print(
                        "**********************************************************************"
                    )
                    print(cacheSourceColumnDF)
                    print(
                        "**********************************************************************"
                    )
                    print(sourceKeyDF)
                    print(
                        "**********************************************************************"
                    )
                    print(cacheSourceKeyDF)

                cacheOperation = None
                if len(sourceColumnDF) > 0:
                    if len(cacheSourceColumnDF) == 0:
                        cacheOperation = "insert"
                    else:
                        cacheOperation = "update"

                        if cacheSourceColumnDF.equals(
                            sourceColumnDF
                        ) and cacheSourceKeyDF.equals(sourceKeyDF):
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
                    if cacheSourceColumnDF.equals(sourceColumnDF):
                        print("columns equal")
                    if cacheSourceKeyDF.equals(sourceKeyDF):
                        print("keys equal")
                    pd.set_option("display.max_columns", None)
                    pd.set_option("display.max_rows", None)

                    print(sourceColumnDF)
                    print(
                        "**********************************************************************"
                    )
                    print(cacheSourceColumnDF)
                    print(
                        "**********************************************************************"
                    )
                    print(sourceKeyDF)
                    print(
                        "**********************************************************************"
                    )
                    print(cacheSourceKeyDF)

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
                    atlasErrorDetected = True
                    continue
                except foreignKeyError:
                    atlasForeignKeyErrorDetected = False
                    continue
                except invalidConfiguration:
                    atlasErrorDetected = True
                    continue
                # 				result = self.updateAtlasWithRDBMSdata(schemaName = schema, tableName = table, printInfo = False, errorOnWarning=True)
                # 				if result == False:
                # 					return False

                log.debug("Atlas call completed")

                if cacheOperation == "update":
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

                if cacheOperation == "insert" or cacheOperation == "update":
                    try:
                        session = self.getDBImportSession()
                        sourceColumnDF.to_sql(
                            "atlas_column_cache",
                            self.configDBEngine,
                            index=False,
                            if_exists="append",
                        )
                        sourceKeyDF.to_sql(
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
        atlasNameDict = self.getAtlasRdbmsNames(schemaName="", tableName="")
        if atlasNameDict == None:
            log.warning(
                "Could not check for deleted tables in Atlas as the name specifications in DBImport was not created correctly"
            )
            raise valueError  # noqa: F821  # TODO(#165): should be ValueError
        dbUri = atlasNameDict["dbUri"]

        # if there are no tables in the database, we need to create it in Atlas manually
        if createEmptyDatabaseInAtlas == True:
            tableUri = atlasNameDict["tableUri"]
            dbName = atlasNameDict["dbName"]
            instanceUri = atlasNameDict["instanceUri"]
            instanceName = atlasNameDict["instanceName"]
            instanceType = atlasNameDict["instanceType"]
            environmentType = atlasNameDict["environmentType"]

            jsonData = self.getAtlasRdbmsReferredEntities(schemaName="", tableName="")

            jsonData["entities"] = []
            dbData = {}
            dbData["typeName"] = "rdbms_db"
            dbData["createdBy"] = "DBImport"
            dbData["attributes"] = {}

            dbData["attributes"] = {}
            dbData["attributes"]["qualifiedName"] = dbUri
            dbData["attributes"]["name"] = dbName
            dbData["attributes"]["contact_info"] = self.contactInfo
            dbData["attributes"]["owner"] = self.owner
            dbData["attributes"]["description"] = self.description
            dbData["attributes"]["prodOrOther"] = environmentType
            dbData["attributes"]["instance"] = {
                "guid": "-400",
                "typeName": "rdbms_instance",
            }

            jsonData["entities"].append(dbData)

            # 			log.info("======================================")
            # 			log.info("JSON to send to Atlas!")
            # 			log.info(json.dumps(jsonData, indent=3))
            # 			log.info("======================================")

            response = self.atlasPostData(
                URL=self.atlasRestEntities, data=json.dumps(jsonData)
            )
            if response == None:
                log.warning(
                    "No response from POST rest call to Atlas when creating an empty database"
                )
                raise atlasError

            statusCode = response["statusCode"]
            if statusCode != 200:
                log.warning(
                    "Request from Atlas when updating database schema was %s."
                    % (statusCode)
                )
                log.warning("%s" % (response["data"]))
                self.atlasEnabled == False
                raise atlasError

        # query the rdbms_db object in Atlas
        atlasRestURL = "%s/rdbms_db?minExtInfo=true&attr:qualifiedName=%s" % (
            self.atlasRestUniqueAttributeType,
            dbUri,
        )
        response = self.atlasGetData(URL=atlasRestURL)
        if response == None:
            # If there is an error in self.atlasGetData(), we need to stop here as the response is None
            log.warning(
                "No response from GET rest call to Atlas when reading table object for deletion"
            )
            raise atlasError

        statusCode = response["statusCode"]
        responseData = json.loads(response["data"])

        if statusCode != 200:
            log.warning(
                "Request from Atlas when updating source schema was %s." % (statusCode)
            )
            log.warning("%s" % (response["data"]))
            self.atlasEnabled == False
            raise atlasError

        # Fetch all tables defined on the rdbms_db object
        tablesFoundInDBList = []
        for tables in responseData["entity"]["attributes"]["tables"]:
            guid = tables["guid"]
            qualifiedName = responseData["referredEntities"][guid]["attributes"][
                "qualifiedName"
            ]

            if responseData["referredEntities"][guid]["status"] != "ACTIVE":
                continue

            if self.jdbc_servertype == constant.MYSQL:
                table = qualifiedName.split("@")[0].split(".")[-1]
                schema = "-"
            else:
                table = qualifiedName.split("@")[0].split(".")[-1]
                schema = qualifiedName.split("@")[0].split(".")[-2]

            if self.atlasIncludeFilter == None or self.atlasIncludeFilter == "":
                processTableForDelete = True
            else:
                processTableForDelete = False

            # Filter the schema.table so that only the included tables gets processed
            for include in self.atlasIncludeFilter.split(";"):
                try:
                    includeSchema = include.split(".")[0].replace("*", ".*").lower()
                    includeTable = include.split(".")[1].replace("*", ".*").lower()
                    if re.search(
                        "^" + includeSchema + "$", schema.lower().strip()
                    ) and re.search("^" + includeTable + "$", table.lower().strip()):
                        processTableForDelete = True
                        break
                except IndexError:
                    pass

            # Filter the schema.table so we remove the excluded items
            for exclude in self.atlasExcludeFilter.split(";"):
                try:
                    excludeSchema = exclude.split(".")[0].replace("*", ".*").lower()
                    excludeTable = exclude.split(".")[1].replace("*", ".*").lower()
                    if re.search(
                        "^" + excludeSchema + "$", schema.lower().strip()
                    ) and re.search("^" + excludeTable + "$", table.lower().strip()):
                        processTableForDelete = False
                        break
                except IndexError:
                    pass

            if processTableForDelete == False:
                continue

            tablesFoundInDBDict = {}
            tablesFoundInDBDict["schema"] = schema.strip()
            tablesFoundInDBDict["table"] = table.strip()
            tablesFoundInDBDict["qualifiedName"] = qualifiedName
            tablesFoundInDBList.append(tablesFoundInDBDict)

        tablesAndViewsDF.drop(columns="include", inplace=True)
        tablesAndViewsDF["schema"] = tablesAndViewsDF["schema"].str.strip()
        tablesAndViewsDF["table"] = tablesAndViewsDF["table"].str.strip()

        tablesFoundInDB = pd.DataFrame(tablesFoundInDBList)
        if tablesFoundInDB.empty:
            tablesFoundInDB = pd.DataFrame(columns=["schema", "table"])

        mergeDF = pd.merge(
            tablesAndViewsDF, tablesFoundInDB, on=None, how="outer", indicator="Exist"
        )
        for index, row in mergeDF.loc[mergeDF["Exist"] == "right_only"].iterrows():
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

            atlasRestURL = "%s/rdbms_table?attr:qualifiedName=%s" % (
                self.atlasRestUniqueAttributeType,
                row["qualifiedName"],
            )
            response = self.atlasDeleteData(URL=atlasRestURL)
            if response == None:
                log.warning(
                    "No response from DELETE rest call to Atlas when deleting table object"
                )
                raise atlasError

            statusCode = response["statusCode"]
            if statusCode != 200:
                log.warning(
                    "Request from Atlas when deleting table was %s." % (statusCode)
                )
                log.warning(response["data"])
                raise atlasError

        if atlasErrorDetected == True:
            raise atlasError
        if atlasForeignKeyErrorDetected == True:
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

            JDBCCredentials = [self.jdbc_username, self.jdbc_password]
            try:
                self.JDBCConn = jaydebeapi.connect(
                    self.jdbc_driver,
                    self.jdbc_url,
                    JDBCCredentials,
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

        deleteColumnErrorFound = False
        foreignKeyErrorFound = False

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
        jsonData = self.getAtlasRdbmsReferredEntities(
            schemaName=schemaName, tableName=tableName
        )

        # Get the unique names for the rdbms_* entities. This is common for both import and export as the rdbms_* in Atlas is the same
        returnDict = self.getAtlasRdbmsNames(schemaName=schemaName, tableName=tableName)
        if returnDict == None:
            log.warning(
                "Invalid configuration/data when calculating Atlas names for Rdbms table"
            )
            raise invalidConfiguration

        tableUri = returnDict["tableUri"]
        dbUri = returnDict["dbUri"]
        dbName = returnDict["dbName"]
        instanceUri = returnDict["instanceUri"]
        instanceName = returnDict["instanceName"]
        instanceType = returnDict["instanceType"]

        jsonData["entities"] = []

        # Loop through the columns and add them to the JSON
        for index, row in self.source_columns_df.iterrows():
            columnData = {}
            columnData["typeName"] = "rdbms_column"
            columnData["createdBy"] = "DBImport"
            columnData["attributes"] = {}

            columnName = row["SOURCE_COLUMN_NAME"]
            columnUri = self.getAtlasRdbmsColumnURI(
                schemaName=schemaName, tableName=tableName, columnName=columnName
            )
            if row["IS_NULLABLE"] == "YES":
                columnNullable = True
            else:
                columnNullable = False

            columnIsPrimaryKey = False
            for keysIndex, keysRow in self.source_keys_df.iterrows():
                if (
                    keysRow["COL_NAME"] == columnName
                    and keysRow["CONSTRAINT_TYPE"] == constant.PRIMARY_KEY
                ):
                    columnIsPrimaryKey = True

            columnData["attributes"]["qualifiedName"] = columnUri
            columnData["attributes"]["owner"] = self.owner

            if (
                row["SOURCE_COLUMN_COMMENT"] != None
                and row["SOURCE_COLUMN_COMMENT"].strip() != ""
            ):
                columnData["attributes"]["comment"] = row["SOURCE_COLUMN_COMMENT"]

            try:
                columnLength = int(
                    str(row["SOURCE_COLUMN_LENGTH"]).split(".")[0].split(":")[0]
                )
            except ValueError:
                columnLength = None

            if columnLength != None:
                columnData["attributes"]["length"] = str(columnLength)

            columnData["attributes"]["name"] = columnName
            columnData["attributes"]["data_type"] = row["SOURCE_COLUMN_TYPE"]
            columnData["attributes"]["isNullable"] = columnNullable
            columnData["attributes"]["isPrimaryKey"] = columnIsPrimaryKey
            columnData["attributes"]["table"] = {
                "guid": "-100",
                "typeName": "rdbms_table",
            }

            jsonData["entities"].append(columnData)

        log.debug("======================================")
        log.debug("JSON to send to Atlas!")
        log.debug(json.dumps(jsonData, indent=3))
        log.debug("======================================")

        response = self.atlasPostData(
            URL=self.atlasRestEntities, data=json.dumps(jsonData)
        )
        if response == None:
            log.warning(
                "No response from POST rest call to Atlas when sending the new/updated table object"
            )
            raise atlasError

        statusCode = response["statusCode"]
        if statusCode != 200:
            log.warning(
                "Request from Atlas when updating source schema was %s." % (statusCode)
            )
            log.warning("%s" % (response["data"]))
            self.atlasEnabled == False
            raise atlasError

        # We now have to find columns that exists in DBImport but not in the source anymore.
        # These columns need to be deleted from Atlas

        # Fetch the table from Atlas so we can check all columns configured on it
        atlasRestURL = "%s/rdbms_table?attr:qualifiedName=%s" % (
            self.atlasRestUniqueAttributeType,
            tableUri,
        )
        response = self.atlasGetData(URL=atlasRestURL)
        if response == None:
            log.warning(
                "No response from GET rest call to Atlas when reading the table"
            )
            raise atlasError

        statusCode = response["statusCode"]
        responseData = json.loads(response["data"])

        if statusCode != 200:
            log.warning(
                "Request from Atlas when updating source schema was %s." % (statusCode)
            )
            log.warning("%s" % (response["data"]))
            self.atlasEnabled == False
            raise atlasError

        for jsonRefEntities in responseData["referredEntities"]:
            jsonRefEntity = responseData["referredEntities"][jsonRefEntities]
            if jsonRefEntity["typeName"] == "rdbms_column":
                # We found on column in the ""referredEntities" part. We will now check the column we found
                columnQualifiedName = jsonRefEntity["attributes"]["qualifiedName"]
                columnName = jsonRefEntity["attributes"]["name"]

                # Loop through the source columns to see if it exists in there
                foundSourceColumn = False
                for index, row in self.source_columns_df.iterrows():
                    sourceColumnName = row["SOURCE_COLUMN_NAME"]
                    sourceColumnUri = self.getAtlasRdbmsColumnURI(
                        schemaName=schemaName,
                        tableName=tableName,
                        columnName=sourceColumnName,
                    )
                    if (
                        sourceColumnName == columnName
                        and sourceColumnUri == columnQualifiedName
                    ):
                        foundSourceColumn = True

                if foundSourceColumn == False and jsonRefEntity["status"] == "ACTIVE":
                    # The column defined in Atlas cant be found on the source, and it's still marked as ACTIVE. Lets delete it!
                    log.debug(
                        "Deleting Atlas column with qualifiedName = '%s'"
                        % (columnQualifiedName)
                    )

                    atlasRestURL = "%s/rdbms_column?attr:qualifiedName=%s" % (
                        self.atlasRestUniqueAttributeType,
                        columnQualifiedName,
                    )
                    response = self.atlasDeleteData(URL=atlasRestURL)
                    if response == None:
                        log.warning(
                            "No response from DELETE rest call to Atlas when deleting non-existing columns"
                        )
                        raise atlasError

                    statusCode = response["statusCode"]
                    if statusCode != 200:
                        log.warning(
                            "Request from Atlas when deleting old columns was %s."
                            % (statusCode)
                        )
                        log.warning("%s" % (response["data"]))
                        self.atlasEnabled == False
                        deleteColumnErrorFound = True

        # Code to add Foreign Keys to table. This is done in a new JSON as we reference the table we just created. One JSON per FK

        if self.source_keys_df.empty == False:
            # Select only rows where CONSTRAINT_TYPE = FK and the slice so that we only see the CONSTRAINT_NAME and make it unique
            textPrinted = False
            for fkName in self.source_keys_df.loc[
                self.source_keys_df["CONSTRAINT_TYPE"] == constant.FOREIGN_KEY
            ][["CONSTRAINT_NAME"]].CONSTRAINT_NAME.unique():
                if textPrinted == False:
                    if printInfo == True:
                        log.info("Updating Atlas with remote database foreign keys")
                    textPrinted = True

                jsonData = None
                columnCount = 0

                for index, row in self.source_keys_df.loc[
                    self.source_keys_df["CONSTRAINT_NAME"] == fkName
                ].iterrows():
                    # Each 'row' now contains information about the column in the FK
                    if jsonData == None:
                        # The refered table must exists, or we will get a 404 error from Atlas.
                        # We do the check here and exit later depending on status of refTableExists
                        refSchemaName = row["REFERENCE_SCHEMA_NAME"].strip()
                        refTableName = row["REFERENCE_TABLE_NAME"].strip()
                        returnDict = self.getAtlasRdbmsNames(
                            schemaName=refSchemaName, tableName=refTableName
                        )
                        refTableUri = returnDict["tableUri"]

                        atlasRestURL = "%s/rdbms_table?attr:qualifiedName=%s" % (
                            self.atlasRestUniqueAttributeType,
                            refTableUri,
                        )
                        response = self.atlasGetData(URL=atlasRestURL)
                        if response == None:
                            log.warning(
                                "No response from GET rest call to Atlas when checking if table that is used for ForeignKeys exists in Atlas"
                            )
                            raise atlasError

                        statusCode = response["statusCode"]
                        if statusCode == 200:
                            refTableExists = True
                        else:
                            refTableExists = False

                        # First time we loop through the columns and jsonData is empty. Lets create the base for the json
                        jsonData = self.getAtlasRdbmsReferredEntities(
                            schemaName=schemaName,
                            tableName=tableName,
                            refSchemaName=row["REFERENCE_SCHEMA_NAME"],
                            refTableName=row["REFERENCE_TABLE_NAME"],
                        )

                        entitiesData = {}
                        entitiesData["typeName"] = "rdbms_foreign_key"
                        entitiesData["createdBy"] = "DBImport"
                        entitiesData["attributes"] = {}
                        entitiesData["attributes"]["qualifiedName"] = "%s.%s" % (
                            fkName,
                            tableUri,
                        )
                        entitiesData["attributes"]["name"] = fkName
                        entitiesData["relationshipAttributes"] = {}
                        entitiesData["relationshipAttributes"]["table"] = {
                            "guid": "-100",
                            "typeName": "rdbms_table",
                        }
                        entitiesData["relationshipAttributes"]["references_table"] = {
                            "guid": "-101",
                            "typeName": "rdbms_table",
                        }

                        entitiesData["relationshipAttributes"]["key_columns"] = []
                        entitiesData["relationshipAttributes"][
                            "references_columns"
                        ] = []

                    # We now have a jsonData object with reference to the two tables (on -100 and -101) + db and instance.
                    # Lets add the columns

                    # Reference to the table column
                    sourceColumnUri = self.getAtlasRdbmsColumnURI(
                        schemaName=schemaName,
                        tableName=tableName,
                        columnName=row["COL_NAME"],
                    )

                    uuidRef = str(1000 + columnCount)
                    jsonData["referredEntities"][uuidRef] = {}
                    jsonData["referredEntities"][uuidRef]["guid"] = uuidRef
                    jsonData["referredEntities"][uuidRef]["typeName"] = "rdbms_column"
                    jsonData["referredEntities"][uuidRef]["attributes"] = {}
                    jsonData["referredEntities"][uuidRef]["attributes"][
                        "qualifiedName"
                    ] = sourceColumnUri
                    jsonData["referredEntities"][uuidRef]["attributes"]["name"] = row[
                        "COL_NAME"
                    ]
                    jsonData["referredEntities"][uuidRef]["attributes"]["table"] = {
                        "guid": "-100",
                        "typeName": "rdbms_table",
                    }
                    columnCount = columnCount + 1

                    keyColumnsData = {}
                    keyColumnsData["guid"] = uuidRef
                    keyColumnsData["typeName"] = "rdbms_column"
                    entitiesData["relationshipAttributes"]["key_columns"].append(
                        keyColumnsData
                    )

                    # Reference to the refTable column
                    refColumnUri = self.getAtlasRdbmsColumnURI(
                        schemaName=row["REFERENCE_SCHEMA_NAME"],
                        tableName=row["REFERENCE_TABLE_NAME"],
                        columnName=row["REFERENCE_COL_NAME"],
                    )

                    uuidRef = str(1000 + columnCount)
                    jsonData["referredEntities"][uuidRef] = {}
                    jsonData["referredEntities"][uuidRef]["guid"] = uuidRef
                    jsonData["referredEntities"][uuidRef]["typeName"] = "rdbms_column"
                    jsonData["referredEntities"][uuidRef]["attributes"] = {}
                    jsonData["referredEntities"][uuidRef]["attributes"][
                        "qualifiedName"
                    ] = refColumnUri
                    jsonData["referredEntities"][uuidRef]["attributes"]["name"] = row[
                        "REFERENCE_COL_NAME"
                    ]
                    jsonData["referredEntities"][uuidRef]["attributes"]["table"] = {
                        "guid": "-101",
                        "typeName": "rdbms_table",
                    }
                    columnCount = columnCount + 1

                    refColumnsData = {}
                    refColumnsData["guid"] = uuidRef
                    refColumnsData["typeName"] = "rdbms_column"
                    entitiesData["relationshipAttributes"]["references_columns"].append(
                        refColumnsData
                    )

                jsonData["entities"] = []
                jsonData["entities"].append(entitiesData)

                if refTableExists == True:
                    log.debug(json.dumps(jsonData, indent=3))

                    response = self.atlasPostData(
                        URL=self.atlasRestEntities, data=json.dumps(jsonData)
                    )
                    if response == None:
                        log.warning(
                            "No response from POST rest call to Atlas when adding/changeing ForeignKeys data"
                        )
                        raise atlasError

                    statusCode = response["statusCode"]
                    if statusCode != 200:
                        log.warning(
                            "Request from Atlas when creating Foreign Keys was %s"
                            % (statusCode)
                        )
                        log.warning("%s" % (response["data"]))
                        self.atlasEnabled == False
                        foreignKeyErrorFound = True
                    else:
                        log.debug(
                            "Creating/updating Atlas ForeignKey against %s.%s"
                            % (refSchemaName, refTableName)
                        )
                else:
                    log.warning(
                        "Foreign Key cant be created as refered table(%s.%s) does not exists in Atlas"
                        % (refSchemaName, refTableName)
                    )
                    foreignKeyErrorFound = True

        if errorOnWarning == True:
            if foreignKeyErrorFound == True:
                raise foreignKeyError
            if deleteColumnErrorFound == True:
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
                oracleDB = self.jdbc_oracle_sid
            if (
                self.jdbc_oracle_servicename != None
                and self.jdbc_oracle_servicename != "None"
            ):
                oracleDB = self.jdbc_oracle_servicename
            columnUri = "%s.%s.%s.%s@%s:%s" % (
                oracleDB,
                schemaName,
                tableName,
                columnName,
                self.jdbc_hostname,
                self.jdbc_port,
            )

        elif self.jdbc_servertype == constant.MSSQL:
            columnUri = "%s.%s.%s.%s@%s:%s" % (
                self.jdbc_database,
                schemaName,
                tableName,
                columnName,
                self.jdbc_hostname,
                self.jdbc_port,
            )

        elif self.jdbc_servertype == constant.MYSQL:
            columnUri = "%s.%s.%s@%s:%s" % (
                self.jdbc_database,
                tableName,
                columnName,
                self.jdbc_hostname,
                self.jdbc_port,
            )

        elif self.jdbc_servertype == constant.POSTGRESQL:
            columnUri = "%s.%s.%s.%s@%s:%s" % (
                self.jdbc_database,
                schemaName,
                tableName,
                columnName,
                self.jdbc_hostname,
                self.jdbc_port,
            )

        elif self.jdbc_servertype == constant.PROGRESS:
            columnUri = "%s.%s.%s.%s@%s:%s" % (
                self.jdbc_database,
                schemaName,
                tableName,
                columnName,
                self.jdbc_hostname,
                self.jdbc_port,
            )

        elif self.jdbc_servertype == constant.DB2_UDB:
            columnUri = "%s.%s.%s.%s@%s:%s" % (
                self.jdbc_database,
                schemaName,
                tableName,
                columnName,
                self.jdbc_hostname,
                self.jdbc_port,
            )

        elif self.jdbc_servertype == constant.DB2_AS400:
            columnUri = "%s.%s.%s.%s@%s:%s" % (
                self.jdbc_database,
                schemaName,
                tableName,
                columnName,
                self.jdbc_hostname,
                self.jdbc_port,
            )

        elif self.jdbc_servertype == constant.INFORMIX:
            columnUri = "%s.%s.%s.%s@%s:%s" % (
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
        return columnUri

    def getAtlasRdbmsNames(self, schemaName, tableName):
        """Returns a dict with the names needed for the rdbms_* configuration, depending on database type"""
        log = logging.getLogger(self.loggerName)
        log.debug("Executing atlas_operations.getAtlasRdbmsNames()")

        tableUri = None
        dbUri = None
        instanceUri = None
        instanceName = None
        instanceType = None
        environmentType = None

        if self.atlasEnabled == False:
            return None

        if self.jdbc_environment == None:
            environmentType = "Unspecified"
        else:
            environmentType = self.jdbc_environment

        if self.jdbc_servertype == constant.ORACLE:
            if self.jdbc_oracle_sid != None and self.jdbc_oracle_sid != "None":
                oracleDB = self.jdbc_oracle_sid
            if (
                self.jdbc_oracle_servicename != None
                and self.jdbc_oracle_servicename != "None"
            ):
                oracleDB = self.jdbc_oracle_servicename

            tableUri = "%s.%s.%s@%s:%s" % (
                oracleDB,
                schemaName,
                tableName,
                self.jdbc_hostname,
                self.jdbc_port,
            )
            dbUri = "%s@%s:%s" % (oracleDB, self.jdbc_hostname, self.jdbc_port)
            dbName = oracleDB
            instanceUri = "Oracle@%s:%s" % (self.jdbc_hostname, self.jdbc_port)
            instanceName = "Oracle"
            instanceType = "Oracle"

        elif self.jdbc_servertype == constant.MSSQL:
            tableUri = "%s.%s.%s@%s:%s" % (
                self.jdbc_database,
                schemaName,
                tableName,
                self.jdbc_hostname,
                self.jdbc_port,
            )
            dbUri = "%s@%s:%s" % (
                self.jdbc_database,
                self.jdbc_hostname,
                self.jdbc_port,
            )
            dbName = self.jdbc_database
            instanceUri = "MSSQL@%s:%s" % (self.jdbc_hostname, self.jdbc_port)
            instanceName = "MSSQL"
            instanceType = "MSSQL"

        elif self.jdbc_servertype == constant.MYSQL:
            tableUri = "%s.%s@%s:%s" % (
                self.jdbc_database,
                tableName,
                self.jdbc_hostname,
                self.jdbc_port,
            )
            dbUri = "%s@%s:%s" % (
                self.jdbc_database,
                self.jdbc_hostname,
                self.jdbc_port,
            )
            dbName = self.jdbc_database
            instanceUri = "MySQL@%s:%s" % (self.jdbc_hostname, self.jdbc_port)
            instanceName = "MySQL"
            instanceType = "MySQL"

        elif self.jdbc_servertype == constant.POSTGRESQL:
            tableUri = "%s.%s.%s@%s:%s" % (
                self.jdbc_database,
                schemaName,
                tableName,
                self.jdbc_hostname,
                self.jdbc_port,
            )
            dbUri = "%s@%s:%s" % (
                self.jdbc_database,
                self.jdbc_hostname,
                self.jdbc_port,
            )
            dbName = self.jdbc_database
            instanceUri = "PostgreSQL@%s:%s" % (self.jdbc_hostname, self.jdbc_port)
            instanceName = "PostgreSQL"
            instanceType = "PostgreSQL"

        elif self.jdbc_servertype == constant.PROGRESS:
            tableUri = "%s.%s.%s@%s:%s" % (
                self.jdbc_database,
                schemaName,
                tableName,
                self.jdbc_hostname,
                self.jdbc_port,
            )
            dbUri = "%s@%s:%s" % (
                self.jdbc_database,
                self.jdbc_hostname,
                self.jdbc_port,
            )
            dbName = self.jdbc_database
            instanceUri = "Progress@%s:%s" % (self.jdbc_hostname, self.jdbc_port)
            instanceName = "Progress"
            instanceType = "Progress"

        elif self.jdbc_servertype == constant.DB2_UDB:
            tableUri = "%s.%s.%s@%s:%s" % (
                self.jdbc_database,
                schemaName,
                tableName,
                self.jdbc_hostname,
                self.jdbc_port,
            )
            dbUri = "%s@%s:%s" % (
                self.jdbc_database,
                self.jdbc_hostname,
                self.jdbc_port,
            )
            dbName = self.jdbc_database
            instanceUri = "DB2UDB@%s:%s" % (self.jdbc_hostname, self.jdbc_port)
            instanceName = "DB2"
            instanceType = "DB2"

        elif self.jdbc_servertype == constant.DB2_AS400:
            tableUri = "%s.%s.%s@%s:%s" % (
                self.jdbc_database,
                schemaName,
                tableName,
                self.jdbc_hostname,
                self.jdbc_port,
            )
            dbUri = "%s@%s:%s" % (
                self.jdbc_database,
                self.jdbc_hostname,
                self.jdbc_port,
            )
            dbName = self.jdbc_database
            instanceUri = "DB2AS400@%s:%s" % (self.jdbc_hostname, self.jdbc_port)
            instanceName = "DB2"
            instanceType = "DB2"

        elif self.jdbc_servertype == constant.INFORMIX:
            tableUri = "%s.%s.%s@%s:%s" % (
                self.jdbc_database,
                schemaName,
                tableName,
                self.jdbc_hostname,
                self.jdbc_port,
            )
            dbUri = "%s@%s:%s" % (
                self.jdbc_database,
                self.jdbc_hostname,
                self.jdbc_port,
            )
            dbName = self.jdbc_database
            instanceUri = "INFORMIX@%s:%s" % (self.jdbc_hostname, self.jdbc_port)
            instanceName = "INFORMIX"
            instanceType = "INFORMIX"

        else:
            logging.warning(
                "The Atlas integrations does not support this database type. Atlas integration disabled"
            )
            self.atlasEnabled = False
            return None

        returnDict = {}
        returnDict["tableUri"] = tableUri
        returnDict["dbUri"] = dbUri
        returnDict["dbName"] = dbName
        returnDict["instanceUri"] = instanceUri
        returnDict["instanceName"] = instanceName
        returnDict["instanceType"] = instanceType
        returnDict["environmentType"] = environmentType

        log.debug("Executing atlas_operations.getAtlasRdbmsNames() - Finished")
        return returnDict

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
        jsonData = self.getAtlasRdbmsReferredEntities(
            schemaName=sourceSchema, tableName=sourceTable, hdfsPath=sqoopHdfsLocation
        )

        # Get the unique names for the rdbms_* entities. This is common for both import and export as the rdbms_* in Atlas is the same
        returnDict = self.getAtlasRdbmsNames(
            schemaName=sourceSchema, tableName=sourceTable
        )
        if returnDict == None:
            return

        tableUri = returnDict["tableUri"]
        dbUri = returnDict["dbUri"]
        dbName = returnDict["dbName"]
        instanceUri = returnDict["instanceUri"]
        instanceName = returnDict["instanceName"]
        instanceType = returnDict["instanceType"]

        processName = "%s.%s@DBimport" % (Hive_DB, Hive_Table)

        lineageData = {}
        lineageData["typeName"] = "DBImport_Process"
        lineageData["createdBy"] = "DBImport"
        lineageData["attributes"] = {}
        lineageData["attributes"]["qualifiedName"] = processName
        lineageData["attributes"]["uri"] = processName
        lineageData["attributes"]["name"] = processName
        lineageData["attributes"]["operation"] = "import"
        lineageData["attributes"]["commandlineOpts"] = fullExecutedCommand
        lineageData["attributes"]["description"] = "Import of %s.%s" % (
            Hive_DB,
            Hive_Table,
        )
        lineageData["attributes"]["startTime"] = startStopDict["startTime"]
        lineageData["attributes"]["endTime"] = startStopDict["stopTime"]
        lineageData["attributes"]["userName"] = getpass.getuser()
        lineageData["attributes"]["importTool"] = importTool
        lineageData["attributes"]["inputs"] = [
            {"guid": "-100", "typeName": "rdbms_table"}
        ]
        lineageData["attributes"]["outputs"] = [
            {"guid": "-200", "typeName": "hdfs_path"}
        ]

        jsonData["entities"] = []
        jsonData["entities"].append(lineageData)

        logging.debug(json.dumps(jsonData, indent=3))

        response = self.atlasPostData(
            URL=self.atlasRestEntities, data=json.dumps(jsonData)
        )
        statusCode = response["statusCode"]
        if statusCode != 200:
            log.warning(
                "Request from Atlas when updating import lineage was %s." % (statusCode)
            )
            self.atlasEnabled == False

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
        jsonData = self.getAtlasRdbmsReferredEntities(
            schemaName=targetSchema,
            tableName=targetTable,
        )

        # Get the unique names for the rdbms_* entities. This is common for both import and export as the rdbms_* in Atlas is the same
        returnDict = self.getAtlasRdbmsNames(
            schemaName=targetSchema, tableName=targetTable
        )
        if returnDict == None:
            return

        tableUri = returnDict["tableUri"]
        dbUri = returnDict["dbUri"]
        dbName = returnDict["dbName"]
        instanceUri = returnDict["instanceUri"]
        instanceName = returnDict["instanceName"]
        instanceType = returnDict["instanceType"]

        processName = "%s.%s.%s@DBimport" % (self.dbAlias, targetSchema, targetTable)

        if tempTableNeeded == True:
            hiveQualifiedName = "%s.%s@%s" % (
                hiveExportTempDB,
                hiveExportTempTable,
                self.clusterName,
            )
        else:
            hiveQualifiedName = "%s.%s@%s" % (hiveDB, hiveTable, self.clusterName)

        jsonData["referredEntities"]["-500"] = {}
        jsonData["referredEntities"]["-500"]["guid"] = "-500"
        jsonData["referredEntities"]["-500"]["typeName"] = "hive_table"
        jsonData["referredEntities"]["-500"]["attributes"] = {}
        jsonData["referredEntities"]["-500"]["attributes"]["qualifiedName"] = (
            hiveQualifiedName
        )
        jsonData["referredEntities"]["-500"]["attributes"]["name"] = hiveTable

        lineageData = {}
        lineageData["typeName"] = "DBImport_Process"
        lineageData["createdBy"] = "DBImport"
        lineageData["attributes"] = {}
        lineageData["attributes"]["qualifiedName"] = processName
        lineageData["attributes"]["uri"] = processName
        lineageData["attributes"]["name"] = processName
        lineageData["attributes"]["operation"] = "export"
        lineageData["attributes"]["commandlineOpts"] = fullExecutedCommand
        lineageData["attributes"]["description"] = "Export of %s.%s" % (
            hiveDB,
            hiveTable,
        )
        lineageData["attributes"]["startTime"] = startStopDict["startTime"]
        lineageData["attributes"]["endTime"] = startStopDict["stopTime"]
        lineageData["attributes"]["userName"] = getpass.getuser()
        lineageData["attributes"]["exportTool"] = exportTool
        lineageData["attributes"]["inputs"] = [
            {"guid": "-500", "typeName": "hive_table"}
        ]
        lineageData["attributes"]["outputs"] = [
            {"guid": "-100", "typeName": "rdbms_table"}
        ]

        jsonData["entities"] = []
        jsonData["entities"].append(lineageData)

        logging.debug(json.dumps(jsonData, indent=3))

        response = self.atlasPostData(
            URL=self.atlasRestEntities, data=json.dumps(jsonData)
        )
        statusCode = response["statusCode"]
        if statusCode != 200:
            log.warning(
                "Request from Atlas when updating import lineage was %s." % (statusCode)
            )
            self.atlasEnabled == False

        log.debug(
            "Executing atlas_operations.updateAtlasWithExportLineage() - Finished"
        )
