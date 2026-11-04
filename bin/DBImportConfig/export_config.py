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
import re
import sys
from datetime import datetime

import mysql.connector
import pandas as pd

from common import constants as constant
from common.Exceptions import invalidConfiguration, undevelopedFeature, validationError
from common.Singleton import Singleton
from DBImportConfig import common_config, export_stage, sendStatistics


class config(object, metaclass=Singleton):
    def __init__(self, connectionAlias=None, targetSchema=None, targetTable=None):
        logging.debug("Executing export_config.__init__()")
        self.mysql_conn = None
        self.mysql_cursor01 = None
        self.startDate = None
        self.common_config = None

        self.connectionAlias = connectionAlias
        self.targetSchema = targetSchema
        self.targetTable = targetTable

        self.common_config = common_config.config()
        self.common_config.operationType = "export"
        self.common_config.targetSchema = targetSchema
        self.common_config.targetTable = targetTable

        self.sendStatistics = sendStatistics.sendStatistics()

        self.startDate = self.common_config.startDate
        self.mysql_conn = self.common_config.mysql_conn
        self.mysql_cursor01 = self.mysql_conn.cursor(buffered=True)
        self.mysql_cursor02 = self.mysql_conn.cursor(buffered=True)

        self.tableID = None
        self.exportType = None
        self.hiveDB = None
        self.hiveTable = None
        self.exportPhase = None
        self.exportPhaseDescription = None
        self.datalakeSourceConnection = None
        self.exportIsIncremental = None
        self.validateExport = None
        self.hiveJavaHeap = None

        self.tempTableNeeded = None

        self.fullExecutedCommand = None

        self.sqoop_mapcolumnjava = None
        self.sqoop_last_mappers = None
        self.sqoop_last_size = None
        self.sqoop_last_rows = None
        self.incr_column = None
        self.incr_maxvalue = None
        self.incr_validation_method = None
        self.sqlWhereAddition = None
        self.truncateTargetTable = None
        self.sqlSessions = None
        self.sqoopMappers = None
        self.generatedSqoopOptions = None
        self.forceCreateTempTable = None

        self.awsS3bucket = None
        self.awsS3path = None
        self.awsS3bucketPath = None

        self.validationMethod = None
        self.validationCustomQueryHiveSQL = None
        self.validationCustomQueryTargetSQL = None
        self.validationCustomQueryHiveValue = None
        self.validationCustomQueryTargetValue = None

        # Initialize the stage class that will handle all stage operations for us
        self.stage = export_stage.stage(
            self.mysql_conn,
            connectionAlias=connectionAlias,
            targetSchema=targetSchema,
            targetTable=targetTable,
        )

        logging.debug("Executing export_config.__init__() - Finished")

    def logHiveColumnAdd(
        self, column, columnType, description=None, hiveDB=None, hiveTable=None
    ):
        self.common_config.logHiveColumnAdd(
            column=column,
            columnType=columnType,
            description=description,
            hiveDB=hiveDB,
            hiveTable=hiveTable,
        )

    def logHiveColumnTypeChange(
        self,
        column,
        columnType,
        previous_columnType=None,
        description=None,
        hiveDB=None,
        hiveTable=None,
    ):
        self.common_config.logHiveColumnTypeChange(
            column,
            columnType,
            previous_columnType=previous_columnType,
            description=description,
            hiveDB=hiveDB,
            hiveTable=hiveTable,
        )

    def logHiveColumnRename(
        self,
        columnName,
        previous_columnName,
        description=None,
        hiveDB=None,
        hiveTable=None,
    ):
        self.common_config.logHiveColumnRename(
            columnName,
            previous_columnName,
            description=description,
            hiveDB=hiveDB,
            hiveTable=hiveTable,
        )

    def logTargetColumnAdd(
        self,
        column,
        columnType,
        description=None,
        dbAlias=None,
        database=None,
        schema=None,
        table=None,
    ):
        self.common_config.logJDBCColumnAdd(
            column=column,
            columnType=columnType,
            description=description,
            dbAlias=dbAlias,
            database=database,
            schema=schema,
            table=table,
        )

    def logTargetColumnTypeChange(
        self,
        column,
        columnType,
        previous_columnType=None,
        description=None,
        dbAlias=None,
        database=None,
        schema=None,
        table=None,
    ):
        self.common_config.logJDBCColumnTypeChange(
            column,
            columnType,
            previous_columnType=previous_columnType,
            description=description,
            dbAlias=dbAlias,
            database=database,
            schema=schema,
            table=table,
        )

    def logTargetColumnRename(
        self,
        columnName,
        previous_columnName,
        description=None,
        dbAlias=None,
        database=None,
        schema=None,
        table=None,
    ):
        self.common_config.logJDBCColumnRename(
            columnName,
            previous_columnName,
            description=description,
            dbAlias=dbAlias,
            database=database,
            schema=schema,
            table=table,
        )

    def remove_temporary_files(self):
        self.common_config.remove_temporary_files()

    def setStage(self, stage, force=False):
        self.stage.setStage(stage, force=force)

    def getStage(self):
        return self.stage.getStage()

    def clearStage(self):
        self.stage.clearStage()

    def saveRetryAttempt(self, stage):
        self.stage.saveRetryAttempt(stage)

    def setStageOnlyInMemory(self):
        self.stage.setStageOnlyInMemory()

    def saveStageStatistics(self):
        self.stage.saveStageStatistics(
            dbalias=self.connectionAlias,
            target_database=self.common_config.jdbc_database,
            target_schema=self.targetSchema,
            target_table=self.targetTable,
            hive_db=self.hiveDB,
            hive_table=self.hiveTable,
            export_phase=self.exportPhase,
            incremental=self.exportIsIncremental,
            size=self.sqoop_last_size,
            rows=self.sqoop_last_rows,
            sessions=self.sqoop_last_mappers,
        )

    def convertStageStatisticsToJSON(self):
        self.stage.convertStageStatisticsToJSON(
            dbalias=self.connectionAlias,
            target_database=self.common_config.jdbc_database,
            target_schema=self.targetSchema,
            target_table=self.targetTable,
            hive_db=self.hiveDB,
            hive_table=self.hiveTable,
            export_phase=self.exportPhase,
            incremental=self.exportIsIncremental,
            rows=self.sqoop_last_rows,
            sessions=self.sqoop_last_mappers,
        )

    def sendStartJSON(self):
        """Sends a start JSON document to REST and/or Kafka"""
        logging.debug("Executing export_config.sendStartJSON()")

        self.postDataToREST = self.common_config.getConfigValue(key="post_data_to_rest")
        self.postDataToKafka = self.common_config.getConfigValue(
            key="post_data_to_kafka"
        )
        self.postDataToAWSSNS = self.common_config.getConfigValue(
            key="post_data_to_awssns"
        )

        if (
            self.postDataToREST == False
            and self.postDataToKafka == False
            and self.postDataToAWSSNS == False
        ):
            return

        import_stop = None
        jsonData = {}
        jsonData["type"] = "export"
        jsonData["status"] = "started"
        jsonData["dbalias"] = self.connectionAlias
        jsonData["target_database"] = self.common_config.jdbc_database
        jsonData["target_schema"] = self.targetSchema
        jsonData["target_table"] = self.targetTable
        jsonData["hive_db"] = self.hiveDB
        jsonData["hive_table"] = self.hiveTable
        jsonData["export_phase"] = self.exportPhase
        jsonData["incremental"] = self.exportIsIncremental

        if self.postDataToKafka == True:
            result = self.sendStatistics.publishKafkaData(json.dumps(jsonData))
            if result == False:
                logging.warning("Kafka publish failed! No start message posted")

        if self.postDataToREST == True:
            response = self.sendStatistics.sendRESTdata(json.dumps(jsonData))
            if response != 200:
                logging.warn("REST call failed! No start message posted")

        if self.postDataToAWSSNS == True:
            result = self.sendStatistics.sendAWSSNSdata(json.dumps(jsonData))
            if result == False:
                logging.warning("AWS SNS publish failed! No start message posted")

        logging.debug("Executing export_config.sendStartJSON() - Finished")

    def checkTimeWindow(self):
        self.common_config.checkTimeWindow(self.connectionAlias)

    def checkJDBCTable(self):
        return self.common_config.checkJDBCTable(
            schema=self.targetSchema, table=self.targetTable
        )

    def getExportConfig(self):
        logging.debug("Executing export_config.getExportConfig()")

        query = "select "
        query += "    table_id, "
        query += "    export_type, "
        query += "    hive_db, "
        query += "    hive_table, "
        query += "    validate_export, "
        query += "    sqoop_last_size, "
        query += "    sqoop_last_rows, "
        query += "    sqoop_last_mappers, "
        query += "    incr_column, "
        query += "    incr_maxvalue, "
        query += "    truncate_target, "
        query += "    incr_validation_method, "
        query += "    hive_javaheap, "
        query += "    export_tool, "
        query += "    sql_where_addition, "
        query += "    validationMethod, "
        query += "    validationCustomQueryHiveSQL, "
        query += "    validationCustomQueryTargetSQL, "
        query += "    validationCustomQueryHiveValue, "
        query += "    validationCustomQueryTargetValue, "
        query += "    forceCreateTempTable, "
        query += "    last_update_from_hive "
        query += "from export_tables "
        query += "where "
        query += "    dbalias = %s "
        query += "    and target_schema = %s "
        query += "    and target_table = %s "

        self.mysql_cursor01.execute(
            query, (self.connectionAlias, self.targetSchema, self.targetTable)
        )
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement))

        if self.mysql_cursor01.rowcount != 1:
            raise invalidConfiguration(
                "The specified Hive Database and Table can't be found in the configuration database. Please check that you specified the correct database and table and that the configuration exists in the configuration database"
            )

        row = self.mysql_cursor01.fetchone()

        self.tableID = row[0]
        self.exportType = row[1]
        self.hiveDB = row[2]
        self.hiveTable = row[3]
        self.validateExport = row[4]
        self.sqoop_last_size = row[5]
        self.sqoop_last_rows = row[6]
        self.sqoop_last_mappers = row[7]
        self.incr_column = row[8]
        self.incr_maxvalue = row[9]
        truncate_target_table = row[10]
        self.incr_validation_method = row[11]
        self.hiveJavaHeap = row[12]
        self.exportTool = row[13]
        self.sqlWhereAddition = row[14]
        self.validationMethod = row[15]
        self.validationCustomQueryHiveSQL = row[16]
        self.validationCustomQueryTargetSQL = row[17]
        self.validationCustomQueryHiveValue = row[18]
        self.validationCustomQueryTargetValue = row[19]
        self.forceCreateTempTable = row[20]
        self.common_config.operationTimestamp = row[21]

        if self.validateExport == 0:
            self.validateExport = False
        else:
            self.validateExport = True

        if truncate_target_table == 1:
            self.truncateTargetTable = True
        else:
            self.truncateTargetTable = False

        if self.forceCreateTempTable == 1:
            self.forceCreateTempTable = True
        else:
            self.forceCreateTempTable = False

        if (
            self.validationMethod != constant.VALIDATION_METHOD_CUSTOMQUERY
            and self.validationMethod != constant.VALIDATION_METHOD_ROWCOUNT
        ):
            raise invalidConfiguration(
                "Only the values '%s' or '%s' is valid for column validationMethod in export_tables."
                % (
                    constant.VALIDATION_METHOD_ROWCOUNT,
                    constant.VALIDATION_METHOD_CUSTOMQUERY,
                )
            )

        if (
            self.incr_validation_method != "full"
            and self.incr_validation_method != "incr"
        ):
            raise invalidConfiguration(
                "Only the values 'full' or 'incr' is valid for column incr_validation_method in export_tables."
            )

        if self.exportTool != "sqoop" and self.exportTool != "spark":
            logging.error("ERROR: Unsupported export Tool configured.")
            raise invalidConfiguration(
                "Unsupported export Tool configured (%s). Please check configuration"
                % (self.exportTool)
            )

        if self.exportTool == "sqoop":
            if self.sqlWhereAddition != None and self.sqlWhereAddition.strip() != "":
                logging.error("ERROR: Unsupported export configuration")
                raise invalidConfiguration(
                    "Using a SQL Where configuration in 'sql_where_addition' column is not supported with sqoop exports"
                )

        # Get the connection details. This is needed in order to determine what kind of connection we talk about.
        self.lookupConnectionAlias()

        if self.exportType == "full" and self.common_config.db_awss3 == False:
            self.exportPhase = constant.EXPORT_PHASE_FULL
            self.exportPhaseDescription = "Full Export"

        if self.exportType == "full" and self.common_config.db_awss3 == True:
            self.exportPhase = constant.EXPORT_PHASE_AWSS3_FULL
            self.exportPhaseDescription = "Full Export"

        if self.exportType == "incr" and self.common_config.db_awss3 == False:
            self.exportPhase = constant.EXPORT_PHASE_INCR
            self.exportPhaseDescription = "Incremental Export"

        if self.exportType == "incr" and self.common_config.db_awss3 == True:
            raise invalidConfiguration(
                "Incremental exports to AWS S3 is not supported in this version of DBImport"
            )

        if self.common_config.db_awss3 == True:
            if self.targetSchema == "-" or self.targetSchema == "":
                self.awsS3bucketPath = "%s/%s" % (
                    self.common_config.awsS3bucket,
                    self.targetTable,
                )
            else:
                self.awsS3bucketPath = "%s/%s/%s" % (
                    self.common_config.awsS3bucket,
                    self.targetSchema,
                    self.targetTable,
                )

            self.awsS3bucket = self.awsS3bucketPath.split("/")[
                2
            ]  # Need to start on 2 as s3a:// is first
            self.awsS3path = "/".join(self.awsS3bucketPath.split("/")[3:])

        if self.exportPhase == None:
            raise invalidConfiguration(
                "Import type '%s' is not a valid type. Please check configuration"
                % (self.import_type)
            )

        if self.exportPhase == constant.EXPORT_PHASE_INCR:
            self.exportIsIncremental = True
            if self.incr_column == None:
                raise invalidConfiguration(
                    "Incremental import requires a column configured in the 'incr_column' column"
                )
        else:
            self.exportIsIncremental = False

        # Fetch data from jdbc_connection table
        query = "select datalake_source from jdbc_connections where dbalias = %s "
        self.mysql_cursor01.execute(query, (self.connectionAlias,))
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement))

        if self.mysql_cursor01.rowcount != 1:
            raise invalidConfiguration(
                "There is no JDBC Connection defined with the alias '%s'"
                % (self.connectionAlias)
            )

        row = self.mysql_cursor01.fetchone()

        self.datalakeSourceConnection = row[0]

        # Set the name of the history tables, temporary tables and such
        self.hiveExportTempDB = self.common_config.getConfigValue(
            key="export_staging_database"
        )
        if self.targetSchema == "-":
            self.hiveExportTempTable = (
                self.connectionAlias.replace("-", "_")
                + "__"
                + self.targetTable
                + "__exporttemptable"
            )
        else:
            self.hiveExportTempTable = (
                self.connectionAlias.replace("-", "_")
                + "__"
                + self.targetSchema
                + "__"
                + self.targetTable
                + "__exporttemptable"
            )
        self.hiveExportTempTable = self.hiveExportTempTable.lower()

        logging.debug("Settings from export_config.getExportConfig()")
        logging.debug("    tableID = %s" % (self.tableID))
        logging.debug("    exportType = %s" % (self.exportType))
        logging.debug("    hiveJavaHeap = %s" % (self.hiveJavaHeap))
        logging.debug("    hiveDB = %s" % (self.hiveDB))
        logging.debug("    hiveTable = %s" % (self.hiveTable))
        logging.debug("    sqlWhereAddition = %s" % (self.sqlWhereAddition))
        logging.debug("    exportIsIncremental = %s" % (self.exportIsIncremental))
        logging.debug(
            "    datalakeSourceConnection = %s" % (self.datalakeSourceConnection)
        )
        logging.debug("    exportPhaseDescription = %s" % (self.exportPhaseDescription))
        logging.debug("    exportPhase = %s" % (self.exportPhase))
        logging.debug("    hiveExportTempDB = %s" % (self.hiveExportTempDB))
        logging.debug("    hiveExportTempTable = %s" % (self.hiveExportTempTable))
        logging.debug("Executing export_config.getExportConfig() - Finished")

    def lookupConnectionAlias(self, connectionAlias=None):

        if connectionAlias == None:
            connectionAlias = self.connectionAlias

        self.common_config.lookupConnectionAlias(connectionAlias)

    def validateCustomQuery(self):
        """Validates the custom queries"""
        if (
            self.validationCustomQueryTargetValue == None
            or self.validationCustomQueryHiveValue == None
        ):
            logging.error(
                "Validation failed! One of the custom queries did not return a result"
            )
            logging.info(
                "Result from Hive query:   %s" % (self.validationCustomQueryHiveValue)
            )
            logging.info(
                "Result from target query: %s" % (self.validationCustomQueryTargetValue)
            )
            raise validationError()

        if self.validationCustomQueryTargetValue != self.validationCustomQueryHiveValue:
            logging.error(
                "Validation failed! The custom queries did not return the same result"
            )
            logging.info(
                "Result from Hive query:   %s" % (self.validationCustomQueryHiveValue)
            )
            logging.info(
                "Result from target query: %s" % (self.validationCustomQueryTargetValue)
            )
            raise validationError()

        logging.info("Custom query validation successful!")
        return True

    def saveCustomSQLValidationHiveValue(self, jsonValue, printInfo=True):
        logging.debug("Executing export_config.saveCustomSQLValidationHiveValue()")
        if printInfo == True:
            logging.info(
                "Saving the custom SQL validation data from Hive table to the configuration database"
            )

        query = "update export_tables set validationCustomQueryHiveValue = %s where table_id = %s"

        self.mysql_cursor01.execute(query, (jsonValue, self.tableID))
        self.mysql_conn.commit()
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement))

        logging.debug(
            "Executing export_config.saveCustomSQLValidationHiveValue() - Finished"
        )

    def saveCustomSQLValidationTargetValue(self, jsonValue, printInfo=True):
        logging.debug("Executing export_config.saveCustomSQLValidationTargetValue()")
        if printInfo == True:
            logging.info(
                "Saving the custom SQL validation data from target table to the configuration database"
            )

        query = "update export_tables set validationCustomQueryTargetValue = %s where table_id = %s"

        self.mysql_cursor01.execute(query, (jsonValue, self.tableID))
        self.mysql_conn.commit()
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement))

        logging.debug(
            "Executing export_config.saveCustomSQLValidationTargetValue() - Finished"
        )

    def updateLastUpdateFromHive(self):
        # This function will update the import_tables.last_update_from_source to the startDate from the common class. This will later
        # be used to determine if all the columns exists in the source system as we set the same datatime on all columns
        # that we read from the source system
        logging.debug("")
        logging.debug("Executing export_config.updateLastUpdateFromHive()")

        # Update the import_tables.last_update_from_source with the current date
        query = (
            "update export_tables set last_update_from_hive = %s where table_id = %s "
        )
        self.mysql_cursor01.execute(query, (self.startDate, self.tableID))
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement))
        self.mysql_conn.commit()

        # We re-read the date just to make sure we have the exakt same in common_config. This will later be used to group yarn applications per import
        query = "select last_update_from_hive from export_tables where table_id = %s "
        self.mysql_cursor01.execute(query, (self.tableID,))
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement))

        row = self.mysql_cursor01.fetchone()
        self.common_config.operationTimestamp = row[0]

        logging.debug("Executing export_config.updateLastUpdateFromHive() - Finished")

    def saveColumnData(self, columnsDF=None):
        logging.debug("Executing export_config.saveColumnData()")
        logging.info("Saving column data to MySQL table - export_columns")

        for index, row in columnsDF.iterrows():
            columnName = row["name"]
            columnType = row["type"]
            columnComment = self.common_config.stripUnwantedCharComment(row["comment"])
            columnOrder = row["idx"]

            query = "select column_id from export_columns where table_id = %s and column_name = %s "
            self.mysql_cursor01.execute(query, (self.tableID, columnName))
            logging.debug(
                "SQL Statement executed: %s" % (self.mysql_cursor01.statement)
            )

            row = self.mysql_cursor01.fetchone()
            if row != None:
                columnID = row[0]
            else:
                columnID = None

            if columnID == None:
                query = (
                    "insert into export_columns "
                    "( "
                    "    table_id, "
                    "    column_name, "
                    "    column_type, "
                    "    column_order, "
                    "    hive_db, "
                    "    hive_table, "
                    "    last_update_from_hive, "
                    "    comment "
                    ") values ( %s, %s, %s, %s, %s, %s, %s, %s )"
                )

                self.mysql_cursor01.execute(
                    query,
                    (
                        self.tableID,
                        columnName,
                        columnType,
                        columnOrder,
                        self.hiveDB,
                        self.hiveTable,
                        self.startDate,
                        columnComment,
                    ),
                )
            else:
                query = (
                    "update export_columns set "
                    "    column_type = %s, "
                    "    column_order = %s, "
                    "    hive_db = %s, "
                    "    hive_table = %s, "
                    "    last_update_from_hive = %s, "
                    "    comment = %s "
                    "where column_id = %s "
                )

                self.mysql_cursor01.execute(
                    query,
                    (
                        columnType,
                        columnOrder,
                        self.hiveDB,
                        self.hiveTable,
                        self.startDate,
                        columnComment,
                        columnID,
                    ),
                )

            logging.debug(
                "SQL Statement executed: %s" % (self.mysql_cursor01.statement)
            )
            self.mysql_conn.commit()

        logging.debug("Executing export_config.saveColumnData() - Finished")

    def clearValidationData(self):
        logging.debug("Executing export_config.clearValidationData()")
        logging.info("Clearing rowcounts from previous exports")

        self.validationCustomQueryHiveValue = None
        self.validationCustomQueryTargetValue = None

        query = "update export_tables set hive_rowcount = NULL, target_rowcount = NULL, validationCustomQueryHiveValue = NULL, validationCustomQueryTargetValue = NULL where table_id = %s"
        self.mysql_cursor01.execute(query, (self.tableID,))
        self.mysql_conn.commit()
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement))

        logging.debug("Executing export_config.clearValidationData() - Finished")

    def isExportTempTableNeeded(self, hiveTableIsTransactional, hiveTableIsView):
        """This function return True or False if a temptable is needed to export from"""
        logging.debug("Executing export_config.isExportTempTableNeeded()")

        self.tempTableNeeded = False

        if self.forceCreateTempTable == True:
            logging.info("Temporary table usage is forced")
            self.tempTableNeeded = True
            return self.tempTableNeeded

        if self.exportTool == "spark":
            return self.tempTableNeeded

        if (
            hiveTableIsTransactional == True
            or hiveTableIsView == True
            or self.exportIsIncremental == True
        ):
            self.tempTableNeeded = True

        query = "select count(column_id) from export_columns "
        query += "where table_id = %s and target_column_name is not null"
        self.mysql_cursor01.execute(query, (self.tableID,))
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement))

        row = self.mysql_cursor01.fetchone()
        if row[0] > 0:
            self.tempTableNeeded = True

        logging.debug("tempTableNeeded: %s" % (self.tempTableNeeded))
        logging.debug("Executing export_config.isExportTempTableNeeded() - Finished")
        return self.tempTableNeeded

    def resetMaxValueFromTarget(self):
        logging.debug("Executing export_config.resetMaxValueFromTarget()")

        columQuotation = self.common_config.getQuoteAroundColumn()
        column = self.incr_column
        if self.common_config.getJDBCUpperCase() == True:
            column = column.upper()
        query = "select max(%s%s%s) from " % (columQuotation, column, columQuotation)
        query += self.common_config.getJDBCsqlFromTable(
            schema=self.targetSchema, table=self.targetTable
        )
        resultDF = self.common_config.executeJDBCquery(query)
        maxValue = maxValue = resultDF.loc[0][0]
        self.resetIncrMinMaxValues(maxValue=maxValue)

        logging.debug("Executing export_config.resetMaxValueFromTarget() - Finished")

    def getColumnsFromConfigDatabase(
        self,
        excludeColumns=False,
        forceColumnUppercase=False,
        getReplacedColumnTypes=True,
    ):
        """Reads the columns from the configuration database and returns the information in a Pandas DF with the columns name, type and comment"""
        logging.debug("Executing export_config.getColumnsFromConfigDatabase()")
        hiveColumnDefinition = ""

        query = "select "
        if forceColumnUppercase == False:
            query += "	c.column_name as hiveColumnName, "
            query += "	c.target_column_name as targetColumnName, "
        else:
            query += "	upper(c.column_name) as hiveColumnName, "
            query += "	upper(c.target_column_name) as targetColumnName, "
        if getReplacedColumnTypes == True:
            query += "	if (c.target_column_type is not null and trim(c.target_column_type) != '', c.target_column_type, c.column_type) as columnType, "
        else:
            query += "	c.column_type as columnType, "
        query += " 	c.comment as comment "
        query += "from export_tables t "
        query += "join export_columns c on t.table_id = c.table_id "
        query += "where t.table_id = %s and t.last_update_from_hive = c.last_update_from_hive "
        if excludeColumns == True:
            query += "   and c.include_in_export = 1 "
        query += "order by c.column_order"

        self.mysql_cursor01.execute(query, (self.tableID,))
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement))

        result_df = pd.DataFrame(self.mysql_cursor01.fetchall())

        # Set the correct column namnes in the DataFrame
        result_df_columns = []
        for columns in self.mysql_cursor01.description:
            result_df_columns.append(
                columns[0]
            )  # Name of the column is in the first position
        result_df.columns = result_df_columns

        logging.debug(
            "Executing export_config.getColumnsFromConfigDatabase() - Finished"
        )
        return result_df

    def convertColumnTypeForTargetTable(self, columnName, columnType):
        """Converts the column type to a type that is supported by the Target database type"""
        logging.debug("Executing export_config.convertColumnTypeForTargetTable()")

        mapColumnJava = None
        sourceDatabaseType = ""

        sourceDB = self.hiveDB
        sourceTable = self.hiveTable
        historyTableExport = False

        if sourceTable.endswith("_history"):
            # First check to see that we dont have a import table that actually is called _history in the end
            query = "select count(hive_table) from import_tables where hive_db = %s and hive_table = %s"
            self.mysql_cursor01.execute(query, (sourceDB, sourceTable))
            logging.debug(
                "SQL Statement executed: %s" % (self.mysql_cursor01.statement)
            )
            row = self.mysql_cursor01.fetchone()
            if row[0] == 0:
                # No import table exists with this name. Lets try without the _history part
                sourceTable = re.sub("_history$", "", sourceTable)
                self.mysql_cursor01.execute(query, (sourceDB, sourceTable))
                logging.debug(
                    "SQL Statement executed: %s" % (self.mysql_cursor01.statement)
                )
                row = self.mysql_cursor01.fetchone()
                if row[0] == 1:
                    # A table without _history exists. So lets use that to determine if we need force_string and such
                    historyTableExport = True
                else:
                    sourceTable = self.hiveTable

        if columnType in ("string", "binary"):
            # If the columnType is "string", it might be because we are exporting a table that was imported with force_string = 1.

            query = "select "
            query += "  jc.force_string, "
            query += "  t.force_string, "
            query += "  c.force_string, "
            query += "  c.source_column_type, "
            query += "  c.source_database_type "
            query += "from import_tables t "
            query += "left join jdbc_connections jc "
            query += "  on t.dbalias = jc.dbalias "
            query += "left join import_columns c "
            query += "  on c.hive_db = t.hive_db "
            query += "  and c.hive_table = t.hive_table "
            query += "  and c.column_name = %s "
            query += "where "
            query += "  t.hive_db = %s "
            query += "  and t.hive_table = %s "

            self.mysql_cursor01.execute(
                query, (columnName.lower(), sourceDB, sourceTable)
            )
            logging.debug(
                "SQL Statement executed: %s" % (self.mysql_cursor01.statement)
            )

            if columnType == "binary":
                if self.mysql_cursor01.rowcount == 1:
                    row = self.mysql_cursor01.fetchone()
                    sourceColumnType = row[3]
                    sourceDatabaseType = row[4]
                    columnType = sourceColumnType

            if columnType == "string":
                forceString = False
                if self.mysql_cursor01.rowcount == 1:
                    row = self.mysql_cursor01.fetchone()
                    force_string_connection = row[0]
                    force_string_table = row[1]
                    force_string_column = row[2]
                    sourceColumnType = row[3]
                    sourceDatabaseType = row[4]

                    if force_string_column == 0:
                        forceString = False
                    elif force_string_column == 1:
                        forceString = True
                    elif force_string_table == 0:
                        forceString = False
                    elif force_string_table == 1:
                        forceString = True
                    elif force_string_connection == 0:
                        forceString = False
                    elif force_string_connection == 1:
                        forceString = True
                    else:
                        forceString = True

                if forceString == True:
                    columnType = sourceColumnType

        if self.common_config.db_mssql == True:
            columnType = re.sub("^timestamp", "datetime", columnType)
            columnType = re.sub("^double$", "real", columnType)
            columnType = re.sub("^string$", "varchar(max)", columnType)
            columnType = re.sub("^boolean$", "tinyint", columnType)

            if sourceDatabaseType == constant.ORACLE:
                columnType = re.sub(r"^varchar2\(", "varchar(", columnType)

        if self.common_config.db_oracle == True:
            columnType = re.sub(r"^varchar\(-1\)$", "string", columnType)
            columnType = re.sub(r"^nvarchar\(-1\)$", "string", columnType)
            columnType = re.sub("^tinyint$", "number(3)", columnType)
            columnType = re.sub("^smallint$", "number(5)", columnType)
            columnType = re.sub(r"^decimal\(", "number(", columnType)
            columnType = re.sub("^bigint$", "number(19)", columnType)
            columnType = re.sub("^double$", "binary_double", columnType)
            columnType = re.sub("^boolean$", "number(1)", columnType)
            columnType = re.sub("^uniqueidentifier$", "varchar2(36)", columnType)
            columnType = re.sub(r"^nvarchar\(", "varchar2(", columnType)
            columnType = re.sub(r"^varchar\(", "varchar2(", columnType)
            columnType = re.sub("^timestamp$", "date", columnType)
            columnType = re.sub("^text$", "string", columnType)
            columnType = re.sub("^int$", "number", columnType)

            # If the column is a NUMBER but with decimal 0, it will be read without the decimal.
            if re.search(r"^number\([0-9]*,0\)$", columnType):
                columnType = re.sub(r",0\)$", ")", columnType)

            # Oracle only supports varchar up to 4000. After that it have to be a CLOB. This will rewrite the soure tabletype
            # to string and will later be converted to clob.
            if columnType.startswith("varchar2("):
                columnType = re.sub(r" char\)$", ")", columnType)
                columnSize = int(columnType.split("(")[1].split(")")[0])
                if columnSize > 4000:
                    columnType = "clob"

            if columnType == "string":
                raise invalidConfiguration(
                    "Error: Exports of column '%s' with type 'string' to Oracle is not supported. Please set another columntype in 'target_column_type' or ignore this column in the export."
                    % (columnName)
                )

        if self.common_config.db_mysql == True:
            columnType = re.sub("^string$", "text", columnType)

        if self.common_config.db_db2udb == True:
            if columnType == "string":
                columnType = "clob"
                mapColumnJava = "String"
            columnType = re.sub("^timestamp$", "timestmp", columnType)

        if self.common_config.db_postgresql == True:
            if columnType == "string":
                columnType = "text"
                mapColumnJava = "String"
            columnType = re.sub("^tinyint$", "integer", columnType)
            columnType = re.sub(
                "^timestamp$", "timestamp without time zone", columnType
            )
            columnType = re.sub(r"^decimal\(", "numeric(", columnType)
            columnType = re.sub("^int$", "integer", columnType)
            columnType = re.sub(r"^char\(", "character(", columnType)
            columnType = re.sub(r"^varchar\(", "character varying(", columnType)

        if self.common_config.db_snowflake == True:
            columnType = re.sub("^timestamp$", "TIMESTAMP_NTZ", columnType)
            columnType = re.sub(r"^char\(", "TEXT(", columnType)
            columnType = re.sub(r"^varchar\(", "TEXT(", columnType)

        logging.debug(
            "Executing export_config.convertColumnTypeForTargetTable() - Finished"
        )
        return columnType, mapColumnJava

    def updateTargetTable(self):
        """Updates the target table with new column definitions"""
        logging.debug("Executing export_config.updateTargetTable()")
        logging.info("Updating the Target Table")

        columnsSource = self.getColumnsFromConfigDatabase(excludeColumns=True)
        forceUppercase = False

        targetSchema = self.targetSchema
        targetTable = self.targetTable

        if self.common_config.jdbc_servertype in (constant.ORACLE, constant.DB2_UDB):
            forceUppercase = True
            targetSchema = self.targetSchema.upper()
            targetTable = self.targetTable.upper()

        elif self.common_config.jdbc_servertype in (constant.POSTGRESQL):
            targetSchema = self.targetSchema.lower()
            targetTable = self.targetTable.lower()

        elif self.common_config.jdbc_servertype in (constant.SNOWFLAKE):
            targetSchema = self.targetSchema
            targetTable = self.targetTable

        self.sqoop_mapcolumnjava = []
        firstLoop = True
        alterTableExecuted = False

        for index, row in columnsSource.iterrows():
            columnType = row["columnType"]
            columnComment = row["comment"]
            targetColumnName = row["targetColumnName"]
            hiveColumnName = row["hiveColumnName"]
            if targetColumnName != None and targetColumnName.strip() != "":
                columnName = targetColumnName
            else:
                columnName = hiveColumnName

            # Need to force it to lower so the self.convertColumnTypeForTargetTable works
            columnType = columnType.lower()
            columnType, mapcolumnjava = self.convertColumnTypeForTargetTable(
                hiveColumnName, columnType
            )

            if forceUppercase == True:
                columnName = columnName.upper()
                columnType = columnType.upper()

            if mapcolumnjava != None:
                self.sqoop_mapcolumnjava.append(columnName + "=" + mapcolumnjava)

            columnsSource.iloc[index, columnsSource.columns.get_loc("columnType")] = (
                columnType
            )
            columnsSource.iloc[
                index, columnsSource.columns.get_loc("targetColumnName")
            ] = columnName

        columnsSource.drop("hiveColumnName", axis=1, inplace=True)
        columnsSource.rename(
            columns={"targetColumnName": "name", "columnType": "type"}, inplace=True
        )

        self.common_config.getJDBCTableDefinition(
            source_schema=targetSchema, source_table=targetTable, printInfo=False
        )
        columnsTarget = self.common_config.source_columns_df
        columnsTarget.rename(
            columns={
                "SOURCE_COLUMN_NAME": "name",
                "SOURCE_COLUMN_COMMENT": "comment",
                "SOURCE_COLUMN_TYPE": "type",
            },
            inplace=True,
        )
        columnsTarget.drop("SOURCE_COLUMN_LENGTH", axis=1, inplace=True)
        columnsTarget.drop("IS_NULLABLE", axis=1, inplace=True)
        columnsTarget.drop("TABLE_COMMENT", axis=1, inplace=True)
        columnsTarget.drop("TABLE_TYPE", axis=1, inplace=True)
        columnsTarget.drop("TABLE_CREATE_TIME", axis=1, inplace=True)
        columnsTarget.drop("DEFAULT_VALUE", axis=1, inplace=True)

        # Check for missing columns
        columnsSourceOnlyName = columnsSource.filter(["name"])
        columnsTargetOnlyName = columnsTarget.filter(["name"])
        columnsMergeOnlyName = pd.merge(
            columnsSourceOnlyName,
            columnsTargetOnlyName,
            on=None,
            how="outer",
            indicator="Exist",
        )

        columnsSourceCount = len(columnsSourceOnlyName)
        columnsTargetCount = len(columnsTargetOnlyName)
        columnsMergeLeftOnlyCount = len(
            columnsMergeOnlyName.loc[columnsMergeOnlyName["Exist"] == "left_only"]
        )
        columnsMergeRightOnlyCount = len(
            columnsMergeOnlyName.loc[columnsMergeOnlyName["Exist"] == "right_only"]
        )

        logging.debug("columnsSourceOnlyName")
        logging.debug(columnsSourceOnlyName)
        logging.debug(
            "================================================================"
        )
        logging.debug("columnsTargetOnlyName")
        logging.debug(columnsTargetOnlyName)
        logging.debug(
            "================================================================"
        )
        logging.debug("columnsMergeOnlyName")
        logging.debug(columnsMergeOnlyName)
        logging.debug("")

        if columnsSourceCount == columnsTargetCount and columnsMergeLeftOnlyCount > 0:
            # The number of columns in config and Hive is the same, but there is a difference in name.
            # This is most likely because of a rename of one or more of the columns
            # To handle this, we try a rename. This might fail if the column types are also changed to an incompatable type
            # The logic here is to
            # 1. get all columns from mergeDF that exists in Left_Only
            # 2. Get the position in configDF with that column name
            # 3. Get the column in the same position from HiveDF
            # 4. Check if that column name exists in the mergeDF with Right_Only. If it does, the column was just renamed
            for index, row in columnsMergeOnlyName.loc[
                columnsMergeOnlyName["Exist"] == "left_only"
            ].iterrows():
                rowInSource = columnsSource.loc[
                    columnsSource["name"] == row["name"]
                ].iloc[0]
                indexInSource = columnsSource.loc[
                    columnsSource["name"] == row["name"]
                ].index.item()
                rowInTarget = columnsTarget.iloc[indexInSource]

                if (
                    len(
                        columnsMergeOnlyName.loc[
                            (columnsMergeOnlyName["Exist"] == "right_only")
                            & (columnsMergeOnlyName["name"] == rowInTarget["name"])
                        ]
                    )
                    > 0
                ):
                    # This is executed if the column is renamed and exists in the same position
                    logging.debug("Name in Source:  %s" % (rowInSource["name"]))
                    logging.debug("Type in Source:  %s" % (rowInSource["type"]))
                    logging.debug("Index in Source: %s" % (indexInSource))
                    logging.debug("--------------------")
                    logging.debug("Name in Target: %s" % (rowInTarget["name"]))
                    logging.debug("Type in Target: %s" % (rowInTarget["type"]))
                    logging.debug("======================================")
                    logging.debug("")

                    if self.common_config.jdbc_servertype == constant.MSSQL:
                        query = "EXEC sp_rename '%s.%s.%s', '%s', 'COLUMN'" % (
                            targetSchema,
                            targetTable,
                            rowInTarget["name"],
                            rowInSource["name"],
                        )

                    if self.common_config.jdbc_servertype == constant.ORACLE:
                        query = 'ALTER TABLE "%s"."%s" RENAME COLUMN "%s" TO "%s"' % (
                            targetSchema,
                            targetTable,
                            rowInTarget["name"],
                            rowInSource["name"],
                        )

                    if self.common_config.jdbc_servertype == constant.DB2_UDB:
                        query = "ALTER TABLE %s.%s RENAME COLUMN %s TO %s" % (
                            targetSchema,
                            targetTable,
                            rowInTarget["name"],
                            rowInSource["name"],
                        )

                    if self.common_config.jdbc_servertype == constant.MYSQL:
                        query = "ALTER TABLE %s CHANGE COLUMN %s %s %s" % (
                            targetTable,
                            rowInTarget["name"],
                            rowInSource["name"],
                            rowInSource["type"],
                        )

                    if self.common_config.jdbc_servertype == constant.POSTGRESQL:
                        query = "ALTER TABLE %s.%s RENAME COLUMN %s TO %s" % (
                            targetSchema,
                            targetTable,
                            rowInTarget["name"],
                            rowInSource["name"],
                        )

                    if self.common_config.jdbc_servertype == constant.SNOWFLAKE:
                        query = 'ALTER TABLE "%s"."%s" RENAME COLUMN "%s" TO "%s"' % (
                            targetSchema,
                            targetTable,
                            rowInTarget["name"],
                            rowInSource["name"],
                        )

                    self.common_config.executeJDBCquery(query)
                    alterTableExecuted = True

                    logging.info(
                        "Column %s renamed to %s in Target Table"
                        % (rowInTarget["name"], rowInSource["name"])
                    )
                    self.logTargetColumnRename(
                        rowInSource["name"],
                        rowInTarget["name"],
                        dbAlias=self.connectionAlias,
                        database=self.common_config.jdbc_database,
                        schema=targetSchema,
                        table=targetTable,
                    )

                    if rowInSource["type"] != rowInTarget["type"]:
                        logging.info(
                            "Column %s changed type from %s to %s"
                            % (
                                rowInSource["name"],
                                rowInTarget["type"],
                                rowInSource["type"],
                            )
                        )
                        self.logTargetColumnTypeChange(
                            rowInSource["name"],
                            rowInSource["type"],
                            previous_columnType=rowInTarget["type"],
                            dbAlias=self.connectionAlias,
                            database=self.common_config.jdbc_database,
                            schema=targetSchema,
                            table=targetTable,
                        )
                else:
                    if (
                        columnsMergeLeftOnlyCount == 1
                        and columnsMergeRightOnlyCount == 1
                    ):
                        # So the columns are not in the same position, but it's only one column that changed. In that case, we just rename that one column
                        rowInMergeLeft = columnsMergeOnlyName.loc[
                            columnsMergeOnlyName["Exist"] == "left_only"
                        ].iloc[0]
                        rowInMergeRight = columnsMergeOnlyName.loc[
                            columnsMergeOnlyName["Exist"] == "right_only"
                        ].iloc[0]
                        rowInSource = columnsSource.loc[
                            columnsSource["name"] == rowInMergeLeft["name"]
                        ].iloc[0]
                        rowInTarget = columnsTarget.loc[
                            columnsTarget["name"] == rowInMergeRight["name"]
                        ].iloc[0]
                        logging.debug(rowInSource["name"])
                        logging.debug(rowInSource["type"])
                        logging.debug("--------------------")
                        logging.debug(rowInTarget["name"])
                        logging.debug(rowInTarget["type"])

                        if self.common_config.jdbc_servertype == constant.MSSQL:
                            query = "EXEC sp_rename '%s.%s.%s', '%s', 'COLUMN'" % (
                                targetSchema,
                                targetTable,
                                rowInTarget["name"],
                                rowInSource["name"],
                            )

                        if self.common_config.jdbc_servertype == constant.ORACLE:
                            query = (
                                'ALTER TABLE "%s"."%s" RENAME COLUMN "%s" TO "%s"'
                                % (
                                    targetSchema,
                                    targetTable,
                                    rowInTarget["name"],
                                    rowInSource["name"],
                                )
                            )

                        if self.common_config.jdbc_servertype == constant.DB2_UDB:
                            query = "ALTER TABLE %s.%s RENAME COLUMN %s TO %s" % (
                                targetSchema,
                                targetTable,
                                rowInTarget["name"],
                                rowInSource["name"],
                            )

                        if self.common_config.jdbc_servertype == constant.MYSQL:
                            query = "ALTER TABLE %s CHANGE COLUMN %s %s %s NULL" % (
                                targetTable,
                                rowInTarget["name"],
                                rowInSource["name"],
                                rowInSource["type"],
                            )

                        if self.common_config.jdbc_servertype == constant.POSTGRESQL:
                            query = "ALTER TABLE %s.%s RENAME COLUMN %s TO %s" % (
                                targetSchema,
                                targetTable,
                                rowInTarget["name"],
                                rowInSource["name"],
                            )

                        if self.common_config.jdbc_servertype == constant.SNOWFLAKE:
                            query = (
                                'ALTER TABLE "%s"."%s" RENAME COLUMN "%s" TO "%s"'
                                % (
                                    targetSchema,
                                    targetTable,
                                    rowInTarget["name"],
                                    rowInSource["name"],
                                )
                            )

                        self.common_config.executeJDBCquery(query)
                        self.common_config.executeJDBCquery(query)
                        alterTableExecuted = True

                        logging.info(
                            "Column %s renamed to %s in Target Table"
                            % (rowInTarget["name"], rowInSource["name"])
                        )
                        self.logTargetColumnRename(
                            rowInSource["name"],
                            rowInTarget["name"],
                            dbAlias=self.connectionAlias,
                            database=self.common_config.jdbc_database,
                            schema=targetSchema,
                            table=targetTable,
                        )

                        if rowInSource["type"] != rowInTarget[
                            "type"
                        ] and self.common_config.jdbc_servertype in (constant.MYSQL):
                            logging.info(
                                "Column %s changed type from %s to %s"
                                % (
                                    rowInSource["name"],
                                    rowInTarget["type"],
                                    rowInSource["type"],
                                )
                            )
                            self.logTargetColumnTypeChange(
                                rowInSource["name"],
                                rowInSource["type"],
                                previous_columnType=rowInTarget["type"],
                                dbAlias=self.connectionAlias,
                                database=self.common_config.jdbc_database,
                                schema=targetSchema,
                                table=targetTable,
                            )

            # Update the DF because the target table was changed
            self.common_config.getJDBCTableDefinition(
                source_schema=targetSchema, source_table=targetTable, printInfo=False
            )
            columnsTarget = self.common_config.source_columns_df
            columnsTarget.rename(
                columns={
                    "SOURCE_COLUMN_NAME": "name",
                    "SOURCE_COLUMN_COMMENT": "comment",
                    "SOURCE_COLUMN_TYPE": "type",
                },
                inplace=True,
            )
            columnsTarget.drop("SOURCE_COLUMN_LENGTH", axis=1, inplace=True)
            columnsTarget.drop("IS_NULLABLE", axis=1, inplace=True)
            columnsTarget.drop("TABLE_COMMENT", axis=1, inplace=True)
            columnsTarget.drop("TABLE_TYPE", axis=1, inplace=True)
            columnsTarget.drop("TABLE_CREATE_TIME", axis=1, inplace=True)
            columnsTarget.drop("DEFAULT_VALUE", axis=1, inplace=True)
            columnsTargetOnlyName = columnsTarget.filter(["name"])
            columnsMergeOnlyName = pd.merge(
                columnsSourceOnlyName,
                columnsTargetOnlyName,
                on=None,
                how="outer",
                indicator="Exist",
            )

        for index, row in columnsMergeOnlyName.loc[
            columnsMergeOnlyName["Exist"] == "left_only"
        ].iterrows():
            # This will iterate over columns that only exists in the config and not in Target table. We add these to the table
            fullRow = columnsSource.loc[columnsSource["name"] == row["name"]].iloc[0]

            if self.common_config.jdbc_servertype == constant.MSSQL:
                query = "ALTER TABLE %s.%s ADD %s %s NULL" % (
                    targetSchema,
                    targetTable,
                    fullRow["name"],
                    fullRow["type"],
                )

            if self.common_config.jdbc_servertype == constant.ORACLE:
                query = 'ALTER TABLE "%s"."%s" ADD "%s" %s' % (
                    targetSchema,
                    targetTable,
                    fullRow["name"],
                    fullRow["type"],
                )

            if self.common_config.jdbc_servertype == constant.DB2_UDB:
                query = "ALTER TABLE %s.%s ADD COLUMN %s %s" % (
                    targetSchema,
                    targetTable,
                    fullRow["name"],
                    fullRow["type"],
                )

            if self.common_config.jdbc_servertype == constant.MYSQL:
                query = "ALTER TABLE %s ADD COLUMN %s %s NULL" % (
                    targetTable,
                    fullRow["name"],
                    fullRow["type"],
                )

            if self.common_config.jdbc_servertype == constant.POSTGRESQL:
                query = "ALTER TABLE %s.%s ADD COLUMN %s %s" % (
                    targetSchema,
                    targetTable,
                    fullRow["name"],
                    fullRow["type"],
                )

            if self.common_config.jdbc_servertype == constant.SNOWFLAKE:
                query = 'ALTER TABLE "%s"."%s" ADD COLUMN "%s" %s' % (
                    targetSchema,
                    targetTable,
                    fullRow["name"],
                    fullRow["type"],
                )

            self.common_config.executeJDBCquery(query)
            alterTableExecuted = True

            logging.info(
                "Column %s (%s) added to Target Table"
                % (fullRow["name"], fullRow["type"])
            )
            self.logTargetColumnAdd(
                fullRow["name"],
                columnType=fullRow["type"],
                dbAlias=self.connectionAlias,
                database=self.common_config.jdbc_database,
                schema=targetSchema,
                table=targetTable,
            )

        # Update the DF because the target table was changed
        self.common_config.getJDBCTableDefinition(
            source_schema=targetSchema, source_table=targetTable, printInfo=False
        )
        columnsTarget = self.common_config.source_columns_df
        columnsTarget.rename(
            columns={
                "SOURCE_COLUMN_NAME": "name",
                "SOURCE_COLUMN_COMMENT": "comment",
                "SOURCE_COLUMN_TYPE": "type",
            },
            inplace=True,
        )
        columnsTarget.drop("SOURCE_COLUMN_LENGTH", axis=1, inplace=True)
        columnsTarget.drop("IS_NULLABLE", axis=1, inplace=True)
        columnsTarget.drop("TABLE_COMMENT", axis=1, inplace=True)
        columnsTarget.drop("TABLE_TYPE", axis=1, inplace=True)
        columnsTarget.drop("TABLE_CREATE_TIME", axis=1, inplace=True)
        columnsTarget.drop("DEFAULT_VALUE", axis=1, inplace=True)

        # Check for changed column types
        columnsSourceOnlyNameType = columnsSource.filter(["name", "type"]).sort_values(
            by=["name"], ascending=True
        )
        columnsTargetOnlyNameType = columnsTarget.filter(["name", "type"]).sort_values(
            by=["name"], ascending=True
        )

        # Some columns types must be changed as the function to read the schema will return a precision, but we
        # dont have that in the configuration. If this is not done, there will be an alter table every time an export
        # is running and it wont really change anything.
        columnsTargetOnlyNameType["type"] = columnsTargetOnlyNameType[
            "type"
        ].str.replace(r"^FLOAT\(.*\)", "FLOAT", regex=True)
        columnsTargetOnlyNameType["type"] = columnsTargetOnlyNameType[
            "type"
        ].str.replace(r"^varchar\(-1\)", "varchar(max)", regex=True)

        # Remove newline in columntype
        columnsSourceOnlyNameType["type"] = columnsSourceOnlyNameType[
            "type"
        ].str.replace(r"\n$|\r$", "", regex=True)
        columnsTargetOnlyNameType["type"] = columnsTargetOnlyNameType[
            "type"
        ].str.replace(r"\n$|\r$", "", regex=True)

        # Merge and find the difference
        columnsMergeOnlyNameType = pd.merge(
            columnsSourceOnlyNameType,
            columnsTargetOnlyNameType,
            on=None,
            how="outer",
            indicator="Exist",
        )

        logging.debug("columnsSourceOnlyNameType")
        logging.debug(columnsSourceOnlyNameType)
        logging.debug(
            "================================================================"
        )
        logging.debug("columnsTargetOnlyNameType")
        logging.debug(columnsTargetOnlyNameType)
        logging.debug(
            "================================================================"
        )
        logging.debug("columnsMergeOnlyNameType")
        logging.debug(columnsMergeOnlyNameType)
        logging.debug("")

        for index, row in columnsMergeOnlyNameType.loc[
            columnsMergeOnlyNameType["Exist"] == "left_only"
        ].iterrows():
            # This will iterate over columns that had the type changed from the source
            if self.common_config.jdbc_servertype == constant.MSSQL:
                query = "ALTER TABLE %s.%s ALTER COLUMN %s %s NULL" % (
                    targetSchema,
                    targetTable,
                    row["name"],
                    row["type"],
                )

            if self.common_config.jdbc_servertype == constant.ORACLE:
                query = 'ALTER TABLE "%s"."%s" MODIFY ("%s" %s)' % (
                    targetSchema,
                    targetTable,
                    row["name"],
                    row["type"],
                )

            if self.common_config.jdbc_servertype == constant.DB2_UDB:
                query = "ALTER TABLE %s.%s ALTER COLUMN %s SET DATA TYPE %s" % (
                    targetSchema,
                    targetTable,
                    row["name"],
                    row["type"],
                )

            if self.common_config.jdbc_servertype == constant.MYSQL:
                query = "ALTER TABLE %s CHANGE COLUMN %s %s %s NULL" % (
                    targetTable,
                    row["name"],
                    row["name"],
                    row["type"],
                )

            if self.common_config.jdbc_servertype == constant.POSTGRESQL:
                query = "ALTER TABLE %s.%s ALTER COLUMN %s TYPE %s" % (
                    targetSchema,
                    targetTable,
                    row["name"],
                    row["type"],
                )

            if self.common_config.jdbc_servertype == constant.SNOWFLAKE:
                query = 'ALTER TABLE "%s"."%s" ALTER COLUMN "%s" TYPE %s' % (
                    targetSchema,
                    targetTable,
                    row["name"],
                    row["type"],
                )

            self.common_config.executeJDBCquery(query)
            alterTableExecuted = True

            # Get the previous column type from the Pandas DF with right_only in Exist column
            previous_columnType = (
                (
                    columnsMergeOnlyNameType.loc[
                        (columnsMergeOnlyNameType["name"] == row["name"])
                        & (columnsMergeOnlyNameType["Exist"] == "right_only")
                    ]
                )
                .reset_index()
                .at[0, "type"]
            )

            logging.info(
                "Column %s changed type from '%s' to '%s'"
                % (row["name"], previous_columnType, row["type"])
            )
            self.logTargetColumnTypeChange(
                row["name"],
                columnType=row["type"],
                previous_columnType=previous_columnType,
                dbAlias=self.connectionAlias,
                database=self.common_config.jdbc_database,
                schema=targetSchema,
                table=targetTable,
            )

        # Check for comment changes
        # Update the DF because the target table was changed
        self.common_config.getJDBCTableDefinition(
            source_schema=targetSchema, source_table=targetTable, printInfo=False
        )
        columnsTarget = self.common_config.source_columns_df
        columnsTarget.rename(
            columns={
                "SOURCE_COLUMN_NAME": "name",
                "SOURCE_COLUMN_COMMENT": "comment",
                "SOURCE_COLUMN_TYPE": "type",
            },
            inplace=True,
        )
        columnsTarget.drop("SOURCE_COLUMN_LENGTH", axis=1, inplace=True)
        columnsTarget.drop("IS_NULLABLE", axis=1, inplace=True)
        columnsTarget.drop("TABLE_COMMENT", axis=1, inplace=True)
        columnsTarget.drop("TABLE_TYPE", axis=1, inplace=True)
        columnsTarget.drop("TABLE_CREATE_TIME", axis=1, inplace=True)
        columnsTarget.drop("DEFAULT_VALUE", axis=1, inplace=True)
        columnsTarget["comment"].replace(
            "", None, inplace=True
        )  # Replace blank column comments with None as it would otherwise trigger an alter table on every run
        columnsTarget["comment"].replace("nan", None, inplace=True)

        if (
            alterTableExecuted == True
            and self.common_config.jdbc_servertype == constant.DB2_UDB
        ):
            # DB2 require a reorg if the table was changed
            logging.info("Running a reorg on the target table")
            query = "call SYSPROC.ADMIN_CMD('REORG TABLE %s.%s')" % (
                targetSchema,
                targetTable,
            )
            self.common_config.executeJDBCquery(query)

        # print(columnsSource)
        # print(columnsTarget)
        columnsMerge = pd.merge(
            columnsSource, columnsTarget, on=None, how="outer", indicator="Exist"
        )
        # print(columnsMerge)
        for index, row in columnsMerge.loc[
            columnsMerge["Exist"] == "left_only"
        ].iterrows():
            if row["comment"] == None:
                row["comment"] = ""

            if self.common_config.jdbc_servertype == constant.MSSQL:
                query = "IF NOT EXISTS ("
                query += "SELECT NULL FROM SYS.EXTENDED_PROPERTIES "
                query += "WHERE [major_id] = OBJECT_ID('%s')" % (targetTable)
                query += "   AND [name] = N'MS_Description' "
                query += "   AND [minor_id] = ("
                query += "      SELECT [column_id] FROM SYS.COLUMNS "
                query += "      WHERE [name] = '%s' " % (row["name"])
                query += "         AND [object_id] = OBJECT_ID('%s')" % (targetTable)
                query += "      )"
                query += "   )"
                query += (
                    "EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'%s' , @level0type=N'SCHEMA',@level0name=N'%s', @level1type=N'TABLE',@level1name=N'%s', @level2type=N'COLUMN',@level2name=N'%s' "
                    % (row["comment"], targetSchema, targetTable, row["name"])
                )
                query += "ELSE "
                query += (
                    "EXEC sys.sp_updateextendedproperty  @name=N'MS_Description', @value=N'%s' , @level0type=N'SCHEMA',@level0name=N'%s', @level1type=N'TABLE',@level1name=N'%s', @level2type=N'COLUMN',@level2name=N'%s' "
                    % (row["comment"], targetSchema, targetTable, row["name"])
                )

            if self.common_config.jdbc_servertype == constant.ORACLE:
                query = 'comment on column "%s"."%s"."%s" is \'%s\'' % (
                    targetSchema,
                    targetTable,
                    row["name"],
                    row["comment"],
                )

            if self.common_config.jdbc_servertype == constant.DB2_UDB:
                query = "COMMENT ON COLUMN %s.%s.%s IS '%s'" % (
                    targetSchema,
                    targetTable,
                    row["name"],
                    row["comment"],
                )

            if self.common_config.jdbc_servertype == constant.MYSQL:
                query = "ALTER TABLE %s CHANGE COLUMN %s %s %s NULL COMMENT '%s'" % (
                    targetTable,
                    row["name"],
                    row["name"],
                    row["type"],
                    row["comment"],
                )

            if self.common_config.jdbc_servertype == constant.POSTGRESQL:
                query = 'comment on column "%s"."%s"."%s" is \'%s\'' % (
                    targetSchema,
                    targetTable,
                    row["name"],
                    row["comment"],
                )

            if self.common_config.jdbc_servertype == constant.SNOWFLAKE:
                query = 'ALTER TABLE "%s"."%s" ALTER COLUMN "%s" COMMENT \'%s\'' % (
                    targetSchema,
                    targetTable,
                    row["name"],
                    row["comment"],
                )

            self.common_config.executeJDBCquery(query)
            alterTableExecuted = True

        logging.debug("Executing export_config.updateTargetTable() - Finished")

    def getSqoopMapColumnJava(self):
        """Returns the generated mapColumnJava settngs for all columns"""
        logging.debug("Executing export_config.getSqoopMapColumnJava()")

        columnsDF = self.getColumnsFromConfigDatabase(excludeColumns=True)
        self.sqoop_mapcolumnjava = []
        forceColumnUppercase = False

        if self.common_config.jdbc_servertype in (constant.ORACLE, constant.DB2_UDB):
            forceColumnUppercase = True

        for index, row in columnsDF.iterrows():
            columnType = row["columnType"]
            targetColumnName = row["targetColumnName"]
            hiveColumnName = row["hiveColumnName"]
            if targetColumnName != None and targetColumnName.strip() != "":
                columnName = targetColumnName
            else:
                columnName = hiveColumnName

            if forceColumnUppercase == True:
                columnName = columnName.upper()

            columnType, mapColumnJava = self.convertColumnTypeForTargetTable(
                hiveColumnName, columnType
            )
            if mapColumnJava != None:
                self.sqoop_mapcolumnjava.append(columnName + "=" + mapColumnJava)

        logging.debug("Executing export_config.getSqoopMapColumnJava() - Finished")
        return self.sqoop_mapcolumnjava

    def createTargetTable(self):
        """Creates the target table on the JDBC connection. This is the table we will export to"""
        logging.debug("Executing export_config.createTargetTable()")
        logging.info("Creating the Target Table")

        columnsDF = self.getColumnsFromConfigDatabase(excludeColumns=True)
        forceColumnUppercase = False
        columnQuote = ""

        if self.common_config.jdbc_servertype in (constant.ORACLE, constant.DB2_UDB):
            targetSchema = self.targetSchema.upper()
            targetTable = self.targetTable.upper()
            forceColumnUppercase = True
            columnQuote = '"'
            query = 'create table "%s"."%s" (' % (targetSchema, targetTable)

        elif self.common_config.jdbc_servertype in (constant.POSTGRESQL):
            targetSchema = self.targetSchema.lower()
            targetTable = self.targetTable.lower()
            query = "create table %s.%s (" % (targetSchema, targetTable)

        elif self.common_config.jdbc_servertype in (constant.SNOWFLAKE):
            targetSchema = self.targetSchema
            targetTable = self.targetTable
            query = 'create table "%s"."%s" (' % (targetSchema, targetTable)
            columnQuote = '"'

        else:
            targetSchema = self.targetSchema
            targetTable = self.targetTable
            if targetSchema == "-":
                query = "create table %s (" % (targetTable)
            else:
                query = "create table %s.%s (" % (targetSchema, targetTable)

        self.sqoop_mapcolumnjava = []
        firstLoop = True

        for index, row in columnsDF.iterrows():
            columnType = row["columnType"]
            columnComment = row["comment"]
            targetColumnName = row["targetColumnName"]
            hiveColumnName = row["hiveColumnName"]
            if targetColumnName != None and targetColumnName.strip() != "":
                columnName = targetColumnName
            else:
                columnName = hiveColumnName

            if forceColumnUppercase == True:
                columnName = columnName.upper()

            if firstLoop == False:
                query += ", "
            else:
                firstLoop = False

            columnType, mapColumnJava = self.convertColumnTypeForTargetTable(
                hiveColumnName, columnType
            )
            if mapColumnJava != None:
                self.sqoop_mapcolumnjava.append(columnName + "=" + mapColumnJava)

            query += "%s%s%s %s NULL " % (
                columnQuote,
                columnName,
                columnQuote,
                columnType,
            )

            if columnComment != None and columnComment.strip() != "":
                if self.common_config.jdbc_servertype in (constant.SNOWFLAKE):
                    query += "comment '%s'" % (columnComment)

                elif self.common_config.jdbc_servertype not in (
                    constant.ORACLE,
                    constant.MSSQL,
                    constant.POSTGRESQL,
                ):
                    query += 'comment "%s"' % (columnComment)

        query += ")"

        self.common_config.executeJDBCquery(query)

        logging.debug("Executing export_config.createTargetTable() - Finished")

    def saveExportStatistics(
        self,
        sqoopStartUTS=None,
        sqoopSize=None,
        sqoopRows=None,
        sqoopIncrMaxvaluePending=None,
        sqoopMappers=None,
    ):
        logging.debug("Executing export_config.saveExportStatistics()")
        logging.info("Saving export statistics")

        self.sqoopStartUTS = sqoopStartUTS
        self.sqoop_last_execution_timestamp = datetime.utcfromtimestamp(
            sqoopStartUTS
        ).strftime("%Y-%m-%d %H:%M:%S.000")

        queryParam = []
        query = "update export_tables set "
        firstSet = True
        if sqoopStartUTS != None:
            if firstSet == False:
                query += " ,"
                firstSet = False

            query += "sqoop_last_execution = %s"
            queryParam.append(sqoopStartUTS)

        if sqoopSize != None:
            if firstSet == False:
                query += " , "
                firstSet = False

            query += "sqoop_last_size = %s"
            self.sqoop_last_size = sqoopSize
            queryParam.append(sqoopSize)

        if sqoopRows != None:
            if firstSet == False:
                query += " , "
                firstSet = False

            query += "sqoop_last_rows = %s"
            self.sqoop_last_rows = sqoopRows
            queryParam.append(sqoopRows)

        if sqoopIncrMaxvaluePending != None:
            if firstSet == False:
                query += " , "
                firstSet = False

            query += "incr_maxvalue_pending = %s"
            self.sqoopIncrMaxvaluePending = sqoopIncrMaxvaluePending
            queryParam.append(sqoopIncrMaxvaluePending)

        if sqoopMappers != None:
            if firstSet == False:
                query += " , "
                firstSet = False

            query += "sqoop_last_mappers = %s"
            self.sqoop_last_mappers = sqoopMappers
            queryParam.append(sqoopMappers)

        if firstSet == False:
            # Only run this if any of the set columns are not None
            query += " where table_id = %s "
            queryParam.append(self.tableID)

            self.mysql_cursor01.execute(query, queryParam)
            logging.debug(
                "SQL Statement executed: %s" % (self.mysql_cursor01.statement)
            )
            self.mysql_conn.commit()

        logging.debug("Executing export_config.saveExportStatistics() - Finished")

    def saveHiveTableRowCount(self, rowCount):
        logging.debug("Executing export_config.saveHiveTableRowCount()")
        logging.info(
            "Saving the number of rows in the Hive Table to the configuration database"
        )

        # Save the value to the database
        query = "update export_tables set hive_rowcount = %s where table_id = %s"
        self.mysql_cursor01.execute(query, (rowCount, self.tableID))
        self.mysql_conn.commit()
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement))

        logging.debug("Executing export_config.saveHiveTableRowCount() - Finished")

    def runCustomValidationQueryOnJDBCTable(self):
        logging.debug("Executing export_config.runCustomValidationQueryOnJDBCTable()")

        logging.info("Executing custom validation query on target table.")

        query = self.validationCustomQueryTargetSQL
        query = query.replace("${TARGET_SCHEMA}", self.targetSchema)
        query = query.replace("${TARGET_TABLE}", self.targetTable)

        logging.debug("Validation Query on JDBC table: %s" % (query))

        resultDF = self.common_config.executeJDBCquery(query)
        resultJSON = resultDF.to_json(orient="values")
        self.validationCustomQueryTargetValue = resultJSON

        self.saveCustomSQLValidationTargetValue(jsonValue=resultJSON, printInfo=False)

        if len(self.validationCustomQueryTargetValue) > 1024:
            logging.warning(
                "'%s' is to large." % (self.validationCustomQueryTargetValue)
            )
            raise invalidConfiguration(
                "The size of the json document on the custom query exceeds 1024 bytes. Change the query to create a result with less than 512 bytes"
            )

        logging.debug("resultDF:")
        logging.debug(resultDF)
        logging.debug("resultJSON: %s" % (resultJSON))

        logging.debug(
            "Executing export_config.runCustomValidationQueryOnJDBCTable() - Finished"
        )

    def getJDBCTableRowCount(self, whereStatement=None):
        logging.debug("Executing export_config.getJDBCTableRowCount()")

        logging.info(
            "Reading and saving number of rows in target table. This will later be used for validating the export"
        )

        JDBCRowsFull = None
        JDBCRowsFull = self.common_config.getJDBCTableRowCount(
            self.targetSchema, self.targetTable, whereStatement
        )

        # Save the value to the database
        query = "update export_tables set "
        query += "target_rowcount = %s " % (JDBCRowsFull)
        if self.exportTool == "spark":
            # Need to save the sqoop_last_rows if the export tool is spark, as we dont get this from spark the same way as sqoop gives us
            self.sqoop_last_rows = JDBCRowsFull
            query += ",sqoop_last_rows = %s " % (JDBCRowsFull)

        query += "where table_id = %s" % (self.tableID)

        self.mysql_cursor01.execute(query)
        self.mysql_conn.commit()
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement))

        logging.debug("Executing export_config.getJDBCTableRowCount() - Finished")

    def resetIncrMinMaxValues(self, maxValue):
        logging.debug("Executing export_config.resetIncrMinMaxValues()")
        if maxValue != None:
            logging.info(
                "Reseting incremental values for exports. New max value is: %s"
                % (maxValue)
            )
        else:
            logging.info("Reseting incremental values for exports.")

        query = "update export_tables set "
        query += "	incr_minvalue = NULL, "
        query += "	incr_maxvalue = %s, "
        query += "	incr_minvalue_pending = NULL, "
        query += "	incr_maxvalue_pending = NULL "
        query += "where table_id = %s "

        self.mysql_cursor01.execute(query, (maxValue, self.tableID))
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement))
        self.mysql_conn.commit()

        logging.debug("Executing export_config.resetIncrMinMaxValues() - Finished")

    def validateRowCount(self, validateSqoop=False, incremental=False):
        """Validates the rows based on values stored in export_tables -> hive_rowcount and target_rowcount. Returns True or False"""
        # incremental=True is used for normal validations but only on incremented rows, regardless if we are going to validate full or incr.

        logging.debug("Executing export_config.validateRowCount()")
        returnValue = None

        if self.validateExport == True:
            # Reading the saved number from the configurationdatabase
            # 			if validateSqoop == False:
            if 1 == 1:
                if self.exportIsIncremental == False or (
                    self.exportIsIncremental == True
                    and self.incr_validation_method == "full"
                ):
                    # Standard full validation
                    query = "select hive_rowcount, target_rowcount from export_tables where table_id = %s "
                    validateTextTarget = "Target table"
                    validateTextSource = "Hive table"
                    validateText = validateTextTarget
                else:
                    query = "select hive_rowcount, target_rowcount from export_tables where table_id = %s "
                    validateTextTarget = "Target table (incr)"
                    validateTextSource = "Hive table (incr)"
                    validateText = "Target table"
            else:
                raise undevelopedFeature("Unsupported validation")

            self.mysql_cursor01.execute(query, (self.tableID,))
            logging.debug(
                "SQL Statement executed: %s" % (self.mysql_cursor01.statement)
            )

            row = self.mysql_cursor01.fetchone()
            source_rowcount = row[0]
            target_rowcount = row[1]
            diffAllowed = 0
            logging.debug("source_rowcount: %s" % (source_rowcount))
            logging.debug("target_rowcount: %s" % (target_rowcount))

            if source_rowcount != target_rowcount:
                logging.error(
                    "Validation failed! The %s exceedes the allowed limit compared to the %s table"
                    % (validateTextSource, validateTextTarget)
                )
                if target_rowcount > source_rowcount:
                    logging.info(
                        "Diff between tables: %s" % (target_rowcount - source_rowcount)
                    )
                else:
                    logging.info(
                        "Diff between tables: %s" % (source_rowcount - target_rowcount)
                    )
                logging.info("%s rowcount: %s" % (validateTextSource, source_rowcount))
                logging.info("%s rowcount: %s" % (validateTextTarget, target_rowcount))
                returnValue = False
            else:
                logging.info("%s validation successful!" % (validateText))
                logging.info("%s rowcount: %s" % (validateTextSource, source_rowcount))
                logging.info("%s rowcount: %s" % (validateTextTarget, target_rowcount))
                logging.info("")
                returnValue = True

        else:
            returnValue = True

        logging.debug("Executing export_config.validateRowCount() - Finished")
        return returnValue

    def calculateJobMappers(self):
        """Based on previous sqoop size, the number of mappers for sqoop will be calculated. Formula is sqoop_size / 128MB."""

        logging.debug("Executing export_config.calculateJobMappers()")
        logging.info(
            "Calculating the number of SQL sessions in the source system that the export will use"
        )

        sqlSessionsMin = 1
        sqlSessionsMax = None

        # TODO: Add max_export_sessions to jdbc_connections

        row = None

        if row != None and row[0] > 0:
            sqlSessionsMax = row[0]
            logging.info(
                "\u001b[33mThis connection is limited to a maximum of %s parallel sessions during export\u001b[0m"
                % (sqlSessionsMax)
            )

        if row != None and row[0] == 0:
            logging.warning(
                "Unsupported value of 0 in column 'max_export_sessions' in table 'jdbc_connections'"
            )

        sqlSessionsMaxFromConfig = int(
            self.common_config.getConfigValue(key="export_max_sessions")
        )
        sqlSessionsDefault = int(
            self.common_config.getConfigValue(key="export_default_sessions")
        )

        if sqlSessionsMax == None:
            sqlSessionsMax = sqlSessionsMaxFromConfig
        if sqlSessionsMaxFromConfig < sqlSessionsMax:
            sqlSessionsMax = sqlSessionsMaxFromConfig
        if sqlSessionsDefault > sqlSessionsMax:
            sqlSessionsDefault = sqlSessionsMax

        self.sparkMaxExecutors = sqlSessionsMax

        # Execute SQL query that calculates the value
        query = (
            "select sqoop_last_size, "
            "cast(sqoop_last_size / (1024*1024*128) as unsigned) as calculated_mappers, "
            "mappers "
            "from export_tables where "
            "table_id = %s"
        )
        self.mysql_cursor01.execute(query, (self.tableID,))
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement))

        if self.mysql_cursor01.rowcount != 1:
            logging.error(
                "Error: Number of rows returned from query on 'export_table' was not one."
            )
            logging.error("Rows returned: %d" % (self.mysql_cursor01.rowcount))
            logging.error(
                "SQL Statement that generated the error: %s"
                % (self.mysql_cursor01.statement)
            )
            raise Exception

        row = self.mysql_cursor01.fetchone()
        sqlSessionsLast = row[0]
        self.sqlSessions = row[1]
        self.sqoopMappers = row[2]

        if self.exportTool == "sqoop":
            if self.sqoopMappers > 0:
                logging.info("Setting the number of mappers to a fixed value")
                self.sqlSessions = self.sqoopMappers

            if self.sqlSessions == None:
                logging.info(
                    "Cant find the previous number of SQL sessions used. Will default to %s"
                    % (sqlSessionsDefault)
                )
                self.sqlSessions = sqlSessionsDefault
            else:
                if self.sqlSessions < sqlSessionsMin:
                    self.sqlSessions = sqlSessionsMin
                elif self.sqlSessions > sqlSessionsMax:
                    self.sqlSessions = sqlSessionsMax

                logging.info(
                    "The export will use %s parallell SQL sessions in the source system."
                    % (self.sqlSessions)
                )

        elif self.exportTool == "spark":
            if self.sqoopMappers > 0:
                logging.info("Setting the number of executors to a fixed value")
                logging.info(
                    "The export will use %s parallell SQL sessions in the source system."
                    % (self.sqoopMappers)
                )
                self.sparkMaxExecutors = self.sqoopMappers
                self.sqlSessions = self.sqoopMappers
            else:
                # If 'mappers' is set to -1, we just use the value from the default configuration
                self.sqlSessions = self.sparkMaxExecutors

        logging.debug("Executing export_config.calculateJobMappers() - Finished")

    def saveIncrMinMaxValue(self, minValue, maxValue):
        """Save the min value in the pendings column"""
        logging.debug("Executing export_config.saveIncrMinValue()")

        query = "update export_tables set "
        query += "	incr_minvalue_pending = %s, "
        query += "	incr_maxvalue_pending = %s "
        query += "where table_id = %s"

        self.mysql_cursor01.execute(
            query,
            (
                minValue,
                maxValue,
                self.tableID,
            ),
        )
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement))
        self.mysql_conn.commit()

        logging.debug("Executing export_config.saveIncrMinValue() - Finished")

    def saveIncrPendingValues(self):
        """After the incremental export is completed, we save the pending values so the next export starts from the correct spot"""
        logging.debug("Executing export_config.savePendingIncrValues()")

        query = "update export_tables set "
        query += "	incr_minvalue = incr_minvalue_pending, "
        query += "	incr_maxvalue = incr_maxvalue_pending "
        query += "where table_id = %s"

        self.mysql_cursor01.execute(query, (self.tableID,))
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement))
        self.mysql_conn.commit()

        query = "update export_tables set "
        query += "	incr_minvalue_pending = NULL, "
        query += "	incr_maxvalue_pending = NULL "
        query += "where table_id = %s"

        self.mysql_cursor01.execute(query, (self.tableID,))
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement))
        self.mysql_conn.commit()

        logging.debug("Executing export_config.savePendingIncrValues() - Finished")

    def isThereIncrDataToExport(self):
        """Returns False if minvalue_pending = maxvalue_pending. That means that there is no new data in Hive to export"""
        logging.debug("Executing export_config.isThereIncrDataToExport()")

        dataToExport = True

        query = "select incr_minvalue_pending, incr_maxvalue_pending from export_tables where table_id = %s "
        self.mysql_cursor01.execute(query, (self.tableID,))
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement))
        row = self.mysql_cursor01.fetchone()
        minValue = row[0]
        maxValue = row[1]

        if minValue == maxValue:
            dataToExport = False

        logging.debug("dataToExport: %s" % (dataToExport))
        logging.debug("Executing export_config.isThereIncrDataToExport() - Finished")
        return dataToExport

    def getIncrWhereStatement(self, excludeMinValue=False, whereForTarget=False):
        """Returns the where statement that is needed to only work on the rows that was loaded incr"""
        logging.debug("Executing export_config.getIncrWhereStatement()")

        columnName = self.incr_column
        columnQuotation = ""
        whereStatement = ""

        query = "select column_type from export_columns where table_id = %s and column_name = %s "
        self.mysql_cursor01.execute(query, (self.tableID, columnName))
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement))
        row = self.mysql_cursor01.fetchone()
        columnType = row[0]

        # COALESCE is needed if you are going to call a function that counts source target rows outside the normal import
        query = "select "
        query += "   COALESCE(incr_minvalue_pending, incr_minvalue), "
        query += "   COALESCE(incr_maxvalue_pending, incr_maxvalue) "
        query += "from export_tables where table_id = %s "
        self.mysql_cursor01.execute(query, (self.tableID,))
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement))
        row = self.mysql_cursor01.fetchone()
        minValue = row[0]
        maxValue = row[1]

        # Regardless of how we translate the minValue in the next couple of rows, if it's NONE here, it will never be included in the where
        if minValue == None:
            excludeMinValue = True

        if whereForTarget == True and self.common_config.jdbc_servertype in (
            constant.ORACLE,
            constant.DB2_UDB,
        ):
            columnName = columnName.upper()

        if (
            whereForTarget == True
            and columnType == "timestamp"
            and self.common_config.jdbc_servertype == constant.MSSQL
        ):
            minValue += "CONVERT(datetime, '%s', 121) " % (minValue)
            maxValue += "CONVERT(datetime, '%s', 121) " % (maxValue)
        elif (
            whereForTarget == True
            and columnType == "timestamp"
            and self.common_config.jdbc_servertype == constant.MYSQL
        ):
            minValue += "STR_TO_DATE('%s', '%Y-%m-%d %H:%i:%s')" % (minValue)
            maxValue += "STR_TO_DATE('%s', '%Y-%m-%d %H:%i:%s')" % (maxValue)
        elif (
            whereForTarget == True
            and columnType == "timestamp"
            and self.common_config.jdbc_servertype == constant.ORACLE
        ):
            minValue = "TO_TIMESTAMP('%s', 'YYYY-MM-DD HH24:MI:SS.FF')" % (minValue)
            maxValue = "TO_TIMESTAMP('%s', 'YYYY-MM-DD HH24:MI:SS.FF')" % (maxValue)
        elif (
            whereForTarget == True
            and columnType == "timestamp"
            and self.common_config.jdbc_servertype == constant.SNOWFLAKE
        ):
            minValue = "TO_TIMESTAMP('%s', 'YYYY-MM-DD HH24:MI:SS.FF6')" % (minValue)
            maxValue = "TO_TIMESTAMP('%s', 'YYYY-MM-DD HH24:MI:SS.FF6')" % (maxValue)
        elif (
            whereForTarget == True
            and columnType == "timestamp"
            and self.common_config.jdbc_servertype == constant.POSTGRESQL
        ):
            minValue = "TO_TIMESTAMP('%s', 'YYYY-MM-DD HH24:MI:SS')" % (minValue)
            maxValue = "TO_TIMESTAMP('%s', 'YYYY-MM-DD HH24:MI:SS')" % (maxValue)
        elif columnType not in (
            "int",
            "integer",
            "bigint",
            "tinyint",
            "smallint",
            "decimal",
            "double",
            "float",
            "boolean",
        ):
            minValue = '"%s"' % (minValue)
            maxValue = '"%s"' % (maxValue)

        if (
            whereForTarget == True
            and self.common_config.jdbc_servertype == constant.ORACLE
        ):
            columnQuotation = '"'

        if (
            whereForTarget == True
            and self.common_config.jdbc_servertype == constant.SNOWFLAKE
        ):
            columnQuotation = '"'

        if whereForTarget == False:
            columnQuotation = "`"

        if minValue != None and excludeMinValue == False:
            whereStatement += "%s%s%s > %s " % (
                columnQuotation,
                columnName,
                columnQuotation,
                minValue,
            )
            whereStatement += "and "
        whereStatement += "%s%s%s <= %s " % (
            columnQuotation,
            columnName,
            columnQuotation,
            maxValue,
        )

        print(whereStatement)

        logging.debug("Executing export_config.getIncrWhereStatement() - Finished")
        return whereStatement

    def getExportTables(self, dbalias, schema):
        """Return all tables that are exported to a specific connection and schema"""
        logging.debug("Executing export_config.getExportTables()")

        query = "select hive_db as hiveDB, hive_table as hiveTable from export_tables where dbalias = %s and target_schema = %s "
        self.mysql_cursor01.execute(query, (dbalias, schema))
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement))

        result_df = pd.DataFrame(self.mysql_cursor01.fetchall())
        if len(result_df) == 0:
            print(
                "Error: No data returned when searching for tables that are exported to dbalias '%s' and schema '%s'"
                % (dbalias, schema)
            )
            self.remove_temporary_files()
            sys.exit(1)

        # Set the correct column namnes in the DataFrame
        result_df_columns = []
        for columns in self.mysql_cursor01.description:
            result_df_columns.append(
                columns[0]
            )  # Name of the column is in the first position
        result_df.columns = result_df_columns

        logging.debug("Executing export_config.getExportTables() - Finished")
        return result_df

    def addExportTable(self, hiveDB, hiveTable, dbalias, schema, table):
        """Add source table to export_tables"""
        logging.debug("Executing export_config.addExportTable()")
        returnValue = True

        query = (
            "insert into export_tables "
            "("
            "    hive_db, "
            "    hive_table, "
            "    dbalias, "
            "    target_schema, "
            "    target_table "
            ") values ( %s, %s, %s, %s, %s )"
        )

        logging.debug("hiveDB:    %s" % (hiveDB))
        logging.debug("hiveTable: %s" % (hiveTable))
        logging.debug("dbalias:   %s" % (dbalias))
        logging.debug("schema:    %s" % (schema))
        logging.debug("table:     %s" % (table))

        try:
            self.mysql_cursor01.execute(
                query, (hiveDB, hiveTable, dbalias, schema, table)
            )
            self.mysql_conn.commit()
        except mysql.connector.errors.IntegrityError:
            logging.warning(
                "Hive table %s.%s cant be added. The Connection name, Schema and Table already exists"
                % (hiveDB, hiveTable)
            )
            returnValue = False

        logging.debug("Executing export_config.addExportTable() - Finished")
        return returnValue
