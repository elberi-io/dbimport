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


class config(metaclass=Singleton):
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
        column_type,
        description=None,
        db_alias=None,
        database=None,
        schema=None,
        table=None,
    ):
        self.common_config.logJDBCColumnAdd(
            column=column,
            column_type=column_type,
            description=description,
            db_alias=db_alias,
            database=database,
            schema=schema,
            table=table,
        )

    def logTargetColumnTypeChange(
        self,
        column,
        column_type,
        previous_column_type=None,
        description=None,
        db_alias=None,
        database=None,
        schema=None,
        table=None,
    ):
        self.common_config.logJDBCColumnTypeChange(
            column,
            column_type,
            previous_column_type=previous_column_type,
            description=description,
            db_alias=db_alias,
            database=database,
            schema=schema,
            table=table,
        )

    def logTargetColumnRename(
        self,
        column_name,
        previous_column_name,
        description=None,
        db_alias=None,
        database=None,
        schema=None,
        table=None,
    ):
        self.common_config.logJDBCColumnRename(
            column_name,
            previous_column_name,
            description=description,
            db_alias=db_alias,
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
        json_data = {}
        json_data["type"] = "export"
        json_data["status"] = "started"
        json_data["dbalias"] = self.connectionAlias
        json_data["target_database"] = self.common_config.jdbc_database
        json_data["target_schema"] = self.targetSchema
        json_data["target_table"] = self.targetTable
        json_data["hive_db"] = self.hiveDB
        json_data["hive_table"] = self.hiveTable
        json_data["export_phase"] = self.exportPhase
        json_data["incremental"] = self.exportIsIncremental

        if self.postDataToKafka == True:
            result = self.sendStatistics.publishKafkaData(json.dumps(json_data))
            if result == False:
                logging.warning("Kafka publish failed! No start message posted")

        if self.postDataToREST == True:
            response = self.sendStatistics.sendRESTdata(json.dumps(json_data))
            if response != 200:
                logging.warn("REST call failed! No start message posted")

        if self.postDataToAWSSNS == True:
            result = self.sendStatistics.sendAWSSNSdata(json.dumps(json_data))
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

    def saveCustomSQLValidationTargetValue(self, json_value, print_info=True):
        logging.debug("Executing export_config.saveCustomSQLValidationTargetValue()")
        if print_info == True:
            logging.info(
                "Saving the custom SQL validation data from target table to the configuration database"
            )

        query = "update export_tables set validationCustomQueryTargetValue = %s where table_id = %s"

        self.mysql_cursor01.execute(query, (json_value, self.tableID))
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
            column_name = row["name"]
            column_type = row["type"]
            column_comment = self.common_config.stripUnwantedCharComment(row["comment"])
            column_order = row["idx"]

            query = "select column_id from export_columns where table_id = %s and column_name = %s "
            self.mysql_cursor01.execute(query, (self.tableID, column_name))
            logging.debug(
                "SQL Statement executed: %s" % (self.mysql_cursor01.statement)
            )

            row = self.mysql_cursor01.fetchone()
            if row != None:
                column_id = row[0]
            else:
                column_id = None

            if column_id == None:
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
                        column_name,
                        column_type,
                        column_order,
                        self.hiveDB,
                        self.hiveTable,
                        self.startDate,
                        column_comment,
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
                        column_type,
                        column_order,
                        self.hiveDB,
                        self.hiveTable,
                        self.startDate,
                        column_comment,
                        column_id,
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

        colum_quotation = self.common_config.getQuoteAroundColumn()
        column = self.incr_column
        if self.common_config.getJDBCUpperCase() == True:
            column = column.upper()
        query = "select max(%s%s%s) from " % (colum_quotation, column, colum_quotation)
        query += self.common_config.getJDBCsqlFromTable(
            schema=self.targetSchema, table=self.targetTable
        )
        result_df = self.common_config.executeJDBCquery(query)
        max_value = max_value = result_df.loc[0][0]
        self.resetIncrMinMaxValues(maxValue=max_value)

        logging.debug("Executing export_config.resetMaxValueFromTarget() - Finished")

    def getColumnsFromConfigDatabase(
        self,
        excludeColumns=False,
        forceColumnUppercase=False,
        getReplacedColumnTypes=True,
    ):
        """Reads the columns from the configuration database and returns the information in a Pandas DF with the columns name, type and comment"""
        logging.debug("Executing export_config.getColumnsFromConfigDatabase()")
        hive_column_definition = ""

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

    def convertColumnTypeForTargetTable(self, column_name, column_type):
        """Converts the column type to a type that is supported by the Target database type"""
        logging.debug("Executing export_config.convertColumnTypeForTargetTable()")

        map_column_java = None
        source_database_type = ""

        source_db = self.hiveDB
        source_table = self.hiveTable
        history_table_export = False

        if source_table.endswith("_history"):
            # First check to see that we dont have a import table that actually is called _history in the end
            query = "select count(hive_table) from import_tables where hive_db = %s and hive_table = %s"
            self.mysql_cursor01.execute(query, (source_db, source_table))
            logging.debug(
                "SQL Statement executed: %s" % (self.mysql_cursor01.statement)
            )
            row = self.mysql_cursor01.fetchone()
            if row[0] == 0:
                # No import table exists with this name. Lets try without the _history part
                source_table = re.sub("_history$", "", source_table)
                self.mysql_cursor01.execute(query, (source_db, source_table))
                logging.debug(
                    "SQL Statement executed: %s" % (self.mysql_cursor01.statement)
                )
                row = self.mysql_cursor01.fetchone()
                if row[0] == 1:
                    # A table without _history exists. So lets use that to determine if we need force_string and such
                    history_table_export = True
                else:
                    source_table = self.hiveTable

        if column_type in ("string", "binary"):
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
                query, (column_name.lower(), source_db, source_table)
            )
            logging.debug(
                "SQL Statement executed: %s" % (self.mysql_cursor01.statement)
            )

            if column_type == "binary":
                if self.mysql_cursor01.rowcount == 1:
                    row = self.mysql_cursor01.fetchone()
                    source_column_type = row[3]
                    source_database_type = row[4]
                    column_type = source_column_type

            if column_type == "string":
                force_string = False
                if self.mysql_cursor01.rowcount == 1:
                    row = self.mysql_cursor01.fetchone()
                    force_string_connection = row[0]
                    force_string_table = row[1]
                    force_string_column = row[2]
                    source_column_type = row[3]
                    source_database_type = row[4]

                    if force_string_column == 0:
                        force_string = False
                    elif force_string_column == 1:
                        force_string = True
                    elif force_string_table == 0:
                        force_string = False
                    elif force_string_table == 1:
                        force_string = True
                    elif force_string_connection == 0:
                        force_string = False
                    elif force_string_connection == 1:
                        force_string = True
                    else:
                        force_string = True

                if force_string == True:
                    column_type = source_column_type

        if self.common_config.db_mssql == True:
            column_type = re.sub("^timestamp", "datetime", column_type)
            column_type = re.sub("^double$", "real", column_type)
            column_type = re.sub("^string$", "varchar(max)", column_type)
            column_type = re.sub("^boolean$", "tinyint", column_type)

            if source_database_type == constant.ORACLE:
                column_type = re.sub(r"^varchar2\(", "varchar(", column_type)

        if self.common_config.db_oracle == True:
            column_type = re.sub(r"^varchar\(-1\)$", "string", column_type)
            column_type = re.sub(r"^nvarchar\(-1\)$", "string", column_type)
            column_type = re.sub("^tinyint$", "number(3)", column_type)
            column_type = re.sub("^smallint$", "number(5)", column_type)
            column_type = re.sub(r"^decimal\(", "number(", column_type)
            column_type = re.sub("^bigint$", "number(19)", column_type)
            column_type = re.sub("^double$", "binary_double", column_type)
            column_type = re.sub("^boolean$", "number(1)", column_type)
            column_type = re.sub("^uniqueidentifier$", "varchar2(36)", column_type)
            column_type = re.sub(r"^nvarchar\(", "varchar2(", column_type)
            column_type = re.sub(r"^varchar\(", "varchar2(", column_type)
            column_type = re.sub("^timestamp$", "date", column_type)
            column_type = re.sub("^text$", "string", column_type)
            column_type = re.sub("^int$", "number", column_type)

            # If the column is a NUMBER but with decimal 0, it will be read without the decimal.
            if re.search(r"^number\([0-9]*,0\)$", column_type):
                column_type = re.sub(r",0\)$", ")", column_type)

            # Oracle only supports varchar up to 4000. After that it have to be a CLOB. This will rewrite the soure tabletype
            # to string and will later be converted to clob.
            if column_type.startswith("varchar2("):
                column_type = re.sub(r" char\)$", ")", column_type)
                column_size = int(column_type.split("(")[1].split(")")[0])
                if column_size > 4000:
                    column_type = "clob"

            if column_type == "string":
                raise invalidConfiguration(
                    "Error: Exports of column '%s' with type 'string' to Oracle is not supported. Please set another columntype in 'target_column_type' or ignore this column in the export."
                    % (column_name)
                )

        if self.common_config.db_mysql == True:
            column_type = re.sub("^string$", "text", column_type)

        if self.common_config.db_db2udb == True:
            if column_type == "string":
                column_type = "clob"
                map_column_java = "String"
            column_type = re.sub("^timestamp$", "timestmp", column_type)

        if self.common_config.db_postgresql == True:
            if column_type == "string":
                column_type = "text"
                map_column_java = "String"
            column_type = re.sub("^tinyint$", "integer", column_type)
            column_type = re.sub(
                "^timestamp$", "timestamp without time zone", column_type
            )
            column_type = re.sub(r"^decimal\(", "numeric(", column_type)
            column_type = re.sub("^int$", "integer", column_type)
            column_type = re.sub(r"^char\(", "character(", column_type)
            column_type = re.sub(r"^varchar\(", "character varying(", column_type)

        if self.common_config.db_snowflake == True:
            column_type = re.sub("^timestamp$", "TIMESTAMP_NTZ", column_type)
            column_type = re.sub(r"^char\(", "TEXT(", column_type)
            column_type = re.sub(r"^varchar\(", "TEXT(", column_type)

        logging.debug(
            "Executing export_config.convertColumnTypeForTargetTable() - Finished"
        )
        return column_type, map_column_java

    def updateTargetTable(self):
        """Updates the target table with new column definitions"""
        logging.debug("Executing export_config.updateTargetTable()")
        logging.info("Updating the Target Table")

        columns_source = self.getColumnsFromConfigDatabase(excludeColumns=True)
        force_uppercase = False

        target_schema = self.targetSchema
        target_table = self.targetTable

        if self.common_config.jdbc_servertype in (constant.ORACLE, constant.DB2_UDB):
            force_uppercase = True
            target_schema = self.targetSchema.upper()
            target_table = self.targetTable.upper()

        elif self.common_config.jdbc_servertype in (constant.POSTGRESQL):
            target_schema = self.targetSchema.lower()
            target_table = self.targetTable.lower()

        elif self.common_config.jdbc_servertype in (constant.SNOWFLAKE):
            target_schema = self.targetSchema
            target_table = self.targetTable

        self.sqoop_mapcolumnjava = []
        first_loop = True
        alter_table_executed = False

        for index, row in columns_source.iterrows():
            column_type = row["columnType"]
            column_comment = row["comment"]
            target_column_name = row["targetColumnName"]
            hive_column_name = row["hiveColumnName"]
            if target_column_name != None and target_column_name.strip() != "":
                column_name = target_column_name
            else:
                column_name = hive_column_name

            # Need to force it to lower so the self.convertColumnTypeForTargetTable works
            column_type = column_type.lower()
            column_type, mapcolumnjava = self.convertColumnTypeForTargetTable(
                hive_column_name, column_type
            )

            if force_uppercase == True:
                column_name = column_name.upper()
                column_type = column_type.upper()

            if mapcolumnjava != None:
                self.sqoop_mapcolumnjava.append(column_name + "=" + mapcolumnjava)

            columns_source.iloc[index, columns_source.columns.get_loc("columnType")] = (
                column_type
            )
            columns_source.iloc[
                index, columns_source.columns.get_loc("targetColumnName")
            ] = column_name

        columns_source.drop("hiveColumnName", axis=1, inplace=True)
        columns_source.rename(
            columns={"targetColumnName": "name", "columnType": "type"}, inplace=True
        )

        self.common_config.getJDBCTableDefinition(
            source_schema=target_schema, source_table=target_table, printInfo=False
        )
        columns_target = self.common_config.source_columns_df
        columns_target.rename(
            columns={
                "SOURCE_COLUMN_NAME": "name",
                "SOURCE_COLUMN_COMMENT": "comment",
                "SOURCE_COLUMN_TYPE": "type",
            },
            inplace=True,
        )
        columns_target.drop("SOURCE_COLUMN_LENGTH", axis=1, inplace=True)
        columns_target.drop("IS_NULLABLE", axis=1, inplace=True)
        columns_target.drop("TABLE_COMMENT", axis=1, inplace=True)
        columns_target.drop("TABLE_TYPE", axis=1, inplace=True)
        columns_target.drop("TABLE_CREATE_TIME", axis=1, inplace=True)
        columns_target.drop("DEFAULT_VALUE", axis=1, inplace=True)

        # Check for missing columns
        columns_source_only_name = columns_source.filter(["name"])
        columns_target_only_name = columns_target.filter(["name"])
        columns_merge_only_name = pd.merge(
            columns_source_only_name,
            columns_target_only_name,
            on=None,
            how="outer",
            indicator="Exist",
        )

        columns_source_count = len(columns_source_only_name)
        columns_target_count = len(columns_target_only_name)
        columns_merge_left_only_count = len(
            columns_merge_only_name.loc[columns_merge_only_name["Exist"] == "left_only"]
        )
        columns_merge_right_only_count = len(
            columns_merge_only_name.loc[
                columns_merge_only_name["Exist"] == "right_only"
            ]
        )

        logging.debug("columnsSourceOnlyName")
        logging.debug(columns_source_only_name)
        logging.debug(
            "================================================================"
        )
        logging.debug("columnsTargetOnlyName")
        logging.debug(columns_target_only_name)
        logging.debug(
            "================================================================"
        )
        logging.debug("columnsMergeOnlyName")
        logging.debug(columns_merge_only_name)
        logging.debug("")

        if (
            columns_source_count == columns_target_count
            and columns_merge_left_only_count > 0
        ):
            # The number of columns in config and Hive is the same, but there is a difference in name.
            # This is most likely because of a rename of one or more of the columns
            # To handle this, we try a rename. This might fail if the column types are also changed to an incompatable type
            # The logic here is to
            # 1. get all columns from mergeDF that exists in Left_Only
            # 2. Get the position in configDF with that column name
            # 3. Get the column in the same position from HiveDF
            # 4. Check if that column name exists in the mergeDF with Right_Only. If it does, the column was just renamed
            for index, row in columns_merge_only_name.loc[
                columns_merge_only_name["Exist"] == "left_only"
            ].iterrows():
                row_in_source = columns_source.loc[
                    columns_source["name"] == row["name"]
                ].iloc[0]
                index_in_source = columns_source.loc[
                    columns_source["name"] == row["name"]
                ].index.item()
                row_in_target = columns_target.iloc[index_in_source]

                if (
                    len(
                        columns_merge_only_name.loc[
                            (columns_merge_only_name["Exist"] == "right_only")
                            & (columns_merge_only_name["name"] == row_in_target["name"])
                        ]
                    )
                    > 0
                ):
                    # This is executed if the column is renamed and exists in the same position
                    logging.debug("Name in Source:  %s" % (row_in_source["name"]))
                    logging.debug("Type in Source:  %s" % (row_in_source["type"]))
                    logging.debug("Index in Source: %s" % (index_in_source))
                    logging.debug("--------------------")
                    logging.debug("Name in Target: %s" % (row_in_target["name"]))
                    logging.debug("Type in Target: %s" % (row_in_target["type"]))
                    logging.debug("======================================")
                    logging.debug("")

                    if self.common_config.jdbc_servertype == constant.MSSQL:
                        query = "EXEC sp_rename '%s.%s.%s', '%s', 'COLUMN'" % (
                            target_schema,
                            target_table,
                            row_in_target["name"],
                            row_in_source["name"],
                        )

                    if self.common_config.jdbc_servertype == constant.ORACLE:
                        query = 'ALTER TABLE "%s"."%s" RENAME COLUMN "%s" TO "%s"' % (
                            target_schema,
                            target_table,
                            row_in_target["name"],
                            row_in_source["name"],
                        )

                    if self.common_config.jdbc_servertype == constant.DB2_UDB:
                        query = "ALTER TABLE %s.%s RENAME COLUMN %s TO %s" % (
                            target_schema,
                            target_table,
                            row_in_target["name"],
                            row_in_source["name"],
                        )

                    if self.common_config.jdbc_servertype == constant.MYSQL:
                        query = "ALTER TABLE %s CHANGE COLUMN %s %s %s" % (
                            target_table,
                            row_in_target["name"],
                            row_in_source["name"],
                            row_in_source["type"],
                        )

                    if self.common_config.jdbc_servertype == constant.POSTGRESQL:
                        query = "ALTER TABLE %s.%s RENAME COLUMN %s TO %s" % (
                            target_schema,
                            target_table,
                            row_in_target["name"],
                            row_in_source["name"],
                        )

                    if self.common_config.jdbc_servertype == constant.SNOWFLAKE:
                        query = 'ALTER TABLE "%s"."%s" RENAME COLUMN "%s" TO "%s"' % (
                            target_schema,
                            target_table,
                            row_in_target["name"],
                            row_in_source["name"],
                        )

                    self.common_config.executeJDBCquery(query)
                    alter_table_executed = True

                    logging.info(
                        "Column %s renamed to %s in Target Table"
                        % (row_in_target["name"], row_in_source["name"])
                    )
                    self.logTargetColumnRename(
                        row_in_source["name"],
                        row_in_target["name"],
                        db_alias=self.connectionAlias,
                        database=self.common_config.jdbc_database,
                        schema=target_schema,
                        table=target_table,
                    )

                    if row_in_source["type"] != row_in_target["type"]:
                        logging.info(
                            "Column %s changed type from %s to %s"
                            % (
                                row_in_source["name"],
                                row_in_target["type"],
                                row_in_source["type"],
                            )
                        )
                        self.logTargetColumnTypeChange(
                            row_in_source["name"],
                            row_in_source["type"],
                            previous_column_type=row_in_target["type"],
                            db_alias=self.connectionAlias,
                            database=self.common_config.jdbc_database,
                            schema=target_schema,
                            table=target_table,
                        )
                else:
                    if (
                        columns_merge_left_only_count == 1
                        and columns_merge_right_only_count == 1
                    ):
                        # So the columns are not in the same position, but it's only one column that changed. In that case, we just rename that one column
                        row_in_merge_left = columns_merge_only_name.loc[
                            columns_merge_only_name["Exist"] == "left_only"
                        ].iloc[0]
                        row_in_merge_right = columns_merge_only_name.loc[
                            columns_merge_only_name["Exist"] == "right_only"
                        ].iloc[0]
                        row_in_source = columns_source.loc[
                            columns_source["name"] == row_in_merge_left["name"]
                        ].iloc[0]
                        row_in_target = columns_target.loc[
                            columns_target["name"] == row_in_merge_right["name"]
                        ].iloc[0]
                        logging.debug(row_in_source["name"])
                        logging.debug(row_in_source["type"])
                        logging.debug("--------------------")
                        logging.debug(row_in_target["name"])
                        logging.debug(row_in_target["type"])

                        if self.common_config.jdbc_servertype == constant.MSSQL:
                            query = "EXEC sp_rename '%s.%s.%s', '%s', 'COLUMN'" % (
                                target_schema,
                                target_table,
                                row_in_target["name"],
                                row_in_source["name"],
                            )

                        if self.common_config.jdbc_servertype == constant.ORACLE:
                            query = (
                                'ALTER TABLE "%s"."%s" RENAME COLUMN "%s" TO "%s"'
                                % (
                                    target_schema,
                                    target_table,
                                    row_in_target["name"],
                                    row_in_source["name"],
                                )
                            )

                        if self.common_config.jdbc_servertype == constant.DB2_UDB:
                            query = "ALTER TABLE %s.%s RENAME COLUMN %s TO %s" % (
                                target_schema,
                                target_table,
                                row_in_target["name"],
                                row_in_source["name"],
                            )

                        if self.common_config.jdbc_servertype == constant.MYSQL:
                            query = "ALTER TABLE %s CHANGE COLUMN %s %s %s NULL" % (
                                target_table,
                                row_in_target["name"],
                                row_in_source["name"],
                                row_in_source["type"],
                            )

                        if self.common_config.jdbc_servertype == constant.POSTGRESQL:
                            query = "ALTER TABLE %s.%s RENAME COLUMN %s TO %s" % (
                                target_schema,
                                target_table,
                                row_in_target["name"],
                                row_in_source["name"],
                            )

                        if self.common_config.jdbc_servertype == constant.SNOWFLAKE:
                            query = (
                                'ALTER TABLE "%s"."%s" RENAME COLUMN "%s" TO "%s"'
                                % (
                                    target_schema,
                                    target_table,
                                    row_in_target["name"],
                                    row_in_source["name"],
                                )
                            )

                        self.common_config.executeJDBCquery(query)
                        self.common_config.executeJDBCquery(query)
                        alter_table_executed = True

                        logging.info(
                            "Column %s renamed to %s in Target Table"
                            % (row_in_target["name"], row_in_source["name"])
                        )
                        self.logTargetColumnRename(
                            row_in_source["name"],
                            row_in_target["name"],
                            db_alias=self.connectionAlias,
                            database=self.common_config.jdbc_database,
                            schema=target_schema,
                            table=target_table,
                        )

                        if row_in_source["type"] != row_in_target[
                            "type"
                        ] and self.common_config.jdbc_servertype in (constant.MYSQL):
                            logging.info(
                                "Column %s changed type from %s to %s"
                                % (
                                    row_in_source["name"],
                                    row_in_target["type"],
                                    row_in_source["type"],
                                )
                            )
                            self.logTargetColumnTypeChange(
                                row_in_source["name"],
                                row_in_source["type"],
                                previous_column_type=row_in_target["type"],
                                db_alias=self.connectionAlias,
                                database=self.common_config.jdbc_database,
                                schema=target_schema,
                                table=target_table,
                            )

            # Update the DF because the target table was changed
            self.common_config.getJDBCTableDefinition(
                source_schema=target_schema, source_table=target_table, printInfo=False
            )
            columns_target = self.common_config.source_columns_df
            columns_target.rename(
                columns={
                    "SOURCE_COLUMN_NAME": "name",
                    "SOURCE_COLUMN_COMMENT": "comment",
                    "SOURCE_COLUMN_TYPE": "type",
                },
                inplace=True,
            )
            columns_target.drop("SOURCE_COLUMN_LENGTH", axis=1, inplace=True)
            columns_target.drop("IS_NULLABLE", axis=1, inplace=True)
            columns_target.drop("TABLE_COMMENT", axis=1, inplace=True)
            columns_target.drop("TABLE_TYPE", axis=1, inplace=True)
            columns_target.drop("TABLE_CREATE_TIME", axis=1, inplace=True)
            columns_target.drop("DEFAULT_VALUE", axis=1, inplace=True)
            columns_target_only_name = columns_target.filter(["name"])
            columns_merge_only_name = pd.merge(
                columns_source_only_name,
                columns_target_only_name,
                on=None,
                how="outer",
                indicator="Exist",
            )

        for index, row in columns_merge_only_name.loc[
            columns_merge_only_name["Exist"] == "left_only"
        ].iterrows():
            # This will iterate over columns that only exists in the config and not in Target table. We add these to the table
            full_row = columns_source.loc[columns_source["name"] == row["name"]].iloc[0]

            if self.common_config.jdbc_servertype == constant.MSSQL:
                query = "ALTER TABLE %s.%s ADD %s %s NULL" % (
                    target_schema,
                    target_table,
                    full_row["name"],
                    full_row["type"],
                )

            if self.common_config.jdbc_servertype == constant.ORACLE:
                query = 'ALTER TABLE "%s"."%s" ADD "%s" %s' % (
                    target_schema,
                    target_table,
                    full_row["name"],
                    full_row["type"],
                )

            if self.common_config.jdbc_servertype == constant.DB2_UDB:
                query = "ALTER TABLE %s.%s ADD COLUMN %s %s" % (
                    target_schema,
                    target_table,
                    full_row["name"],
                    full_row["type"],
                )

            if self.common_config.jdbc_servertype == constant.MYSQL:
                query = "ALTER TABLE %s ADD COLUMN %s %s NULL" % (
                    target_table,
                    full_row["name"],
                    full_row["type"],
                )

            if self.common_config.jdbc_servertype == constant.POSTGRESQL:
                query = "ALTER TABLE %s.%s ADD COLUMN %s %s" % (
                    target_schema,
                    target_table,
                    full_row["name"],
                    full_row["type"],
                )

            if self.common_config.jdbc_servertype == constant.SNOWFLAKE:
                query = 'ALTER TABLE "%s"."%s" ADD COLUMN "%s" %s' % (
                    target_schema,
                    target_table,
                    full_row["name"],
                    full_row["type"],
                )

            self.common_config.executeJDBCquery(query)
            alter_table_executed = True

            logging.info(
                "Column %s (%s) added to Target Table"
                % (full_row["name"], full_row["type"])
            )
            self.logTargetColumnAdd(
                full_row["name"],
                column_type=full_row["type"],
                db_alias=self.connectionAlias,
                database=self.common_config.jdbc_database,
                schema=target_schema,
                table=target_table,
            )

        # Update the DF because the target table was changed
        self.common_config.getJDBCTableDefinition(
            source_schema=target_schema, source_table=target_table, printInfo=False
        )
        columns_target = self.common_config.source_columns_df
        columns_target.rename(
            columns={
                "SOURCE_COLUMN_NAME": "name",
                "SOURCE_COLUMN_COMMENT": "comment",
                "SOURCE_COLUMN_TYPE": "type",
            },
            inplace=True,
        )
        columns_target.drop("SOURCE_COLUMN_LENGTH", axis=1, inplace=True)
        columns_target.drop("IS_NULLABLE", axis=1, inplace=True)
        columns_target.drop("TABLE_COMMENT", axis=1, inplace=True)
        columns_target.drop("TABLE_TYPE", axis=1, inplace=True)
        columns_target.drop("TABLE_CREATE_TIME", axis=1, inplace=True)
        columns_target.drop("DEFAULT_VALUE", axis=1, inplace=True)

        # Check for changed column types
        columns_source_only_name_type = columns_source.filter(
            ["name", "type"]
        ).sort_values(by=["name"], ascending=True)
        columns_target_only_name_type = columns_target.filter(
            ["name", "type"]
        ).sort_values(by=["name"], ascending=True)

        # Some columns types must be changed as the function to read the schema will return a precision, but we
        # dont have that in the configuration. If this is not done, there will be an alter table every time an export
        # is running and it wont really change anything.
        columns_target_only_name_type["type"] = columns_target_only_name_type[
            "type"
        ].str.replace(r"^FLOAT\(.*\)", "FLOAT", regex=True)
        columns_target_only_name_type["type"] = columns_target_only_name_type[
            "type"
        ].str.replace(r"^varchar\(-1\)", "varchar(max)", regex=True)

        # Remove newline in columntype
        columns_source_only_name_type["type"] = columns_source_only_name_type[
            "type"
        ].str.replace(r"\n$|\r$", "", regex=True)
        columns_target_only_name_type["type"] = columns_target_only_name_type[
            "type"
        ].str.replace(r"\n$|\r$", "", regex=True)

        # Merge and find the difference
        columns_merge_only_name_type = pd.merge(
            columns_source_only_name_type,
            columns_target_only_name_type,
            on=None,
            how="outer",
            indicator="Exist",
        )

        logging.debug("columnsSourceOnlyNameType")
        logging.debug(columns_source_only_name_type)
        logging.debug(
            "================================================================"
        )
        logging.debug("columnsTargetOnlyNameType")
        logging.debug(columns_target_only_name_type)
        logging.debug(
            "================================================================"
        )
        logging.debug("columnsMergeOnlyNameType")
        logging.debug(columns_merge_only_name_type)
        logging.debug("")

        for index, row in columns_merge_only_name_type.loc[
            columns_merge_only_name_type["Exist"] == "left_only"
        ].iterrows():
            # This will iterate over columns that had the type changed from the source
            if self.common_config.jdbc_servertype == constant.MSSQL:
                query = "ALTER TABLE %s.%s ALTER COLUMN %s %s NULL" % (
                    target_schema,
                    target_table,
                    row["name"],
                    row["type"],
                )

            if self.common_config.jdbc_servertype == constant.ORACLE:
                query = 'ALTER TABLE "%s"."%s" MODIFY ("%s" %s)' % (
                    target_schema,
                    target_table,
                    row["name"],
                    row["type"],
                )

            if self.common_config.jdbc_servertype == constant.DB2_UDB:
                query = "ALTER TABLE %s.%s ALTER COLUMN %s SET DATA TYPE %s" % (
                    target_schema,
                    target_table,
                    row["name"],
                    row["type"],
                )

            if self.common_config.jdbc_servertype == constant.MYSQL:
                query = "ALTER TABLE %s CHANGE COLUMN %s %s %s NULL" % (
                    target_table,
                    row["name"],
                    row["name"],
                    row["type"],
                )

            if self.common_config.jdbc_servertype == constant.POSTGRESQL:
                query = "ALTER TABLE %s.%s ALTER COLUMN %s TYPE %s" % (
                    target_schema,
                    target_table,
                    row["name"],
                    row["type"],
                )

            if self.common_config.jdbc_servertype == constant.SNOWFLAKE:
                query = 'ALTER TABLE "%s"."%s" ALTER COLUMN "%s" TYPE %s' % (
                    target_schema,
                    target_table,
                    row["name"],
                    row["type"],
                )

            self.common_config.executeJDBCquery(query)
            alter_table_executed = True

            # Get the previous column type from the Pandas DF with right_only in Exist column
            previous_column_type = (
                (
                    columns_merge_only_name_type.loc[
                        (columns_merge_only_name_type["name"] == row["name"])
                        & (columns_merge_only_name_type["Exist"] == "right_only")
                    ]
                )
                .reset_index()
                .at[0, "type"]
            )

            logging.info(
                "Column %s changed type from '%s' to '%s'"
                % (row["name"], previous_column_type, row["type"])
            )
            self.logTargetColumnTypeChange(
                row["name"],
                column_type=row["type"],
                previous_column_type=previous_column_type,
                db_alias=self.connectionAlias,
                database=self.common_config.jdbc_database,
                schema=target_schema,
                table=target_table,
            )

        # Check for comment changes
        # Update the DF because the target table was changed
        self.common_config.getJDBCTableDefinition(
            source_schema=target_schema, source_table=target_table, printInfo=False
        )
        columns_target = self.common_config.source_columns_df
        columns_target.rename(
            columns={
                "SOURCE_COLUMN_NAME": "name",
                "SOURCE_COLUMN_COMMENT": "comment",
                "SOURCE_COLUMN_TYPE": "type",
            },
            inplace=True,
        )
        columns_target.drop("SOURCE_COLUMN_LENGTH", axis=1, inplace=True)
        columns_target.drop("IS_NULLABLE", axis=1, inplace=True)
        columns_target.drop("TABLE_COMMENT", axis=1, inplace=True)
        columns_target.drop("TABLE_TYPE", axis=1, inplace=True)
        columns_target.drop("TABLE_CREATE_TIME", axis=1, inplace=True)
        columns_target.drop("DEFAULT_VALUE", axis=1, inplace=True)
        columns_target["comment"].replace(
            "", None, inplace=True
        )  # Replace blank column comments with None as it would otherwise trigger an alter table on every run
        columns_target["comment"].replace("nan", None, inplace=True)

        if (
            alter_table_executed == True
            and self.common_config.jdbc_servertype == constant.DB2_UDB
        ):
            # DB2 require a reorg if the table was changed
            logging.info("Running a reorg on the target table")
            query = "call SYSPROC.ADMIN_CMD('REORG TABLE %s.%s')" % (
                target_schema,
                target_table,
            )
            self.common_config.executeJDBCquery(query)

        # print(columnsSource)
        # print(columnsTarget)
        columns_merge = pd.merge(
            columns_source, columns_target, on=None, how="outer", indicator="Exist"
        )
        # print(columnsMerge)
        for index, row in columns_merge.loc[
            columns_merge["Exist"] == "left_only"
        ].iterrows():
            if row["comment"] == None:
                row["comment"] = ""

            if self.common_config.jdbc_servertype == constant.MSSQL:
                query = "IF NOT EXISTS ("
                query += "SELECT NULL FROM SYS.EXTENDED_PROPERTIES "
                query += "WHERE [major_id] = OBJECT_ID('%s')" % (target_table)
                query += "   AND [name] = N'MS_Description' "
                query += "   AND [minor_id] = ("
                query += "      SELECT [column_id] FROM SYS.COLUMNS "
                query += "      WHERE [name] = '%s' " % (row["name"])
                query += "         AND [object_id] = OBJECT_ID('%s')" % (target_table)
                query += "      )"
                query += "   )"
                query += (
                    "EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'%s' , @level0type=N'SCHEMA',@level0name=N'%s', @level1type=N'TABLE',@level1name=N'%s', @level2type=N'COLUMN',@level2name=N'%s' "
                    % (row["comment"], target_schema, target_table, row["name"])
                )
                query += "ELSE "
                query += (
                    "EXEC sys.sp_updateextendedproperty  @name=N'MS_Description', @value=N'%s' , @level0type=N'SCHEMA',@level0name=N'%s', @level1type=N'TABLE',@level1name=N'%s', @level2type=N'COLUMN',@level2name=N'%s' "
                    % (row["comment"], target_schema, target_table, row["name"])
                )

            if self.common_config.jdbc_servertype == constant.ORACLE:
                query = 'comment on column "%s"."%s"."%s" is \'%s\'' % (
                    target_schema,
                    target_table,
                    row["name"],
                    row["comment"],
                )

            if self.common_config.jdbc_servertype == constant.DB2_UDB:
                query = "COMMENT ON COLUMN %s.%s.%s IS '%s'" % (
                    target_schema,
                    target_table,
                    row["name"],
                    row["comment"],
                )

            if self.common_config.jdbc_servertype == constant.MYSQL:
                query = "ALTER TABLE %s CHANGE COLUMN %s %s %s NULL COMMENT '%s'" % (
                    target_table,
                    row["name"],
                    row["name"],
                    row["type"],
                    row["comment"],
                )

            if self.common_config.jdbc_servertype == constant.POSTGRESQL:
                query = 'comment on column "%s"."%s"."%s" is \'%s\'' % (
                    target_schema,
                    target_table,
                    row["name"],
                    row["comment"],
                )

            if self.common_config.jdbc_servertype == constant.SNOWFLAKE:
                query = 'ALTER TABLE "%s"."%s" ALTER COLUMN "%s" COMMENT \'%s\'' % (
                    target_schema,
                    target_table,
                    row["name"],
                    row["comment"],
                )

            self.common_config.executeJDBCquery(query)
            alter_table_executed = True

        logging.debug("Executing export_config.updateTargetTable() - Finished")

    def getSqoopMapColumnJava(self):
        """Returns the generated mapColumnJava settngs for all columns"""
        logging.debug("Executing export_config.getSqoopMapColumnJava()")

        columns_df = self.getColumnsFromConfigDatabase(excludeColumns=True)
        self.sqoop_mapcolumnjava = []
        force_column_uppercase = False

        if self.common_config.jdbc_servertype in (constant.ORACLE, constant.DB2_UDB):
            force_column_uppercase = True

        for index, row in columns_df.iterrows():
            column_type = row["columnType"]
            target_column_name = row["targetColumnName"]
            hive_column_name = row["hiveColumnName"]
            if target_column_name != None and target_column_name.strip() != "":
                column_name = target_column_name
            else:
                column_name = hive_column_name

            if force_column_uppercase == True:
                column_name = column_name.upper()

            column_type, map_column_java = self.convertColumnTypeForTargetTable(
                hive_column_name, column_type
            )
            if map_column_java != None:
                self.sqoop_mapcolumnjava.append(column_name + "=" + map_column_java)

        logging.debug("Executing export_config.getSqoopMapColumnJava() - Finished")
        return self.sqoop_mapcolumnjava

    def createTargetTable(self):
        """Creates the target table on the JDBC connection. This is the table we will export to"""
        logging.debug("Executing export_config.createTargetTable()")
        logging.info("Creating the Target Table")

        columns_df = self.getColumnsFromConfigDatabase(excludeColumns=True)
        force_column_uppercase = False
        column_quote = ""

        if self.common_config.jdbc_servertype in (constant.ORACLE, constant.DB2_UDB):
            target_schema = self.targetSchema.upper()
            target_table = self.targetTable.upper()
            force_column_uppercase = True
            column_quote = '"'
            query = 'create table "%s"."%s" (' % (target_schema, target_table)

        elif self.common_config.jdbc_servertype in (constant.POSTGRESQL):
            target_schema = self.targetSchema.lower()
            target_table = self.targetTable.lower()
            query = "create table %s.%s (" % (target_schema, target_table)

        elif self.common_config.jdbc_servertype in (constant.SNOWFLAKE):
            target_schema = self.targetSchema
            target_table = self.targetTable
            query = 'create table "%s"."%s" (' % (target_schema, target_table)
            column_quote = '"'

        else:
            target_schema = self.targetSchema
            target_table = self.targetTable
            if target_schema == "-":
                query = "create table %s (" % (target_table)
            else:
                query = "create table %s.%s (" % (target_schema, target_table)

        self.sqoop_mapcolumnjava = []
        first_loop = True

        for index, row in columns_df.iterrows():
            column_type = row["columnType"]
            column_comment = row["comment"]
            target_column_name = row["targetColumnName"]
            hive_column_name = row["hiveColumnName"]
            if target_column_name != None and target_column_name.strip() != "":
                column_name = target_column_name
            else:
                column_name = hive_column_name

            if force_column_uppercase == True:
                column_name = column_name.upper()

            if first_loop == False:
                query += ", "
            else:
                first_loop = False

            column_type, map_column_java = self.convertColumnTypeForTargetTable(
                hive_column_name, column_type
            )
            if map_column_java != None:
                self.sqoop_mapcolumnjava.append(column_name + "=" + map_column_java)

            query += "%s%s%s %s NULL " % (
                column_quote,
                column_name,
                column_quote,
                column_type,
            )

            if column_comment != None and column_comment.strip() != "":
                if self.common_config.jdbc_servertype in (constant.SNOWFLAKE):
                    query += "comment '%s'" % (column_comment)

                elif self.common_config.jdbc_servertype not in (
                    constant.ORACLE,
                    constant.MSSQL,
                    constant.POSTGRESQL,
                ):
                    query += 'comment "%s"' % (column_comment)

        query += ")"

        self.common_config.executeJDBCquery(query)

        logging.debug("Executing export_config.createTargetTable() - Finished")

    def saveExportStatistics(
        self,
        sqoop_start_uts=None,
        sqoop_size=None,
        sqoop_rows=None,
        sqoop_incr_maxvalue_pending=None,
        sqoop_mappers=None,
    ):
        logging.debug("Executing export_config.saveExportStatistics()")
        logging.info("Saving export statistics")

        self.sqoopStartUTS = sqoop_start_uts
        self.sqoop_last_execution_timestamp = datetime.utcfromtimestamp(
            sqoop_start_uts
        ).strftime("%Y-%m-%d %H:%M:%S.000")

        query_param = []
        query = "update export_tables set "
        first_set = True
        if sqoop_start_uts != None:
            if first_set == False:
                query += " ,"
                first_set = False

            query += "sqoop_last_execution = %s"
            query_param.append(sqoop_start_uts)

        if sqoop_size != None:
            if first_set == False:
                query += " , "
                first_set = False

            query += "sqoop_last_size = %s"
            self.sqoop_last_size = sqoop_size
            query_param.append(sqoop_size)

        if sqoop_rows != None:
            if first_set == False:
                query += " , "
                first_set = False

            query += "sqoop_last_rows = %s"
            self.sqoop_last_rows = sqoop_rows
            query_param.append(sqoop_rows)

        if sqoop_incr_maxvalue_pending != None:
            if first_set == False:
                query += " , "
                first_set = False

            query += "incr_maxvalue_pending = %s"
            self.sqoopIncrMaxvaluePending = sqoop_incr_maxvalue_pending
            query_param.append(sqoop_incr_maxvalue_pending)

        if sqoop_mappers != None:
            if first_set == False:
                query += " , "
                first_set = False

            query += "sqoop_last_mappers = %s"
            self.sqoop_last_mappers = sqoop_mappers
            query_param.append(sqoop_mappers)

        if first_set == False:
            # Only run this if any of the set columns are not None
            query += " where table_id = %s "
            query_param.append(self.tableID)

            self.mysql_cursor01.execute(query, query_param)
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

        result_df = self.common_config.executeJDBCquery(query)
        result_json = result_df.to_json(orient="values")
        self.validationCustomQueryTargetValue = result_json

        self.saveCustomSQLValidationTargetValue(
            json_value=result_json, print_info=False
        )

        if len(self.validationCustomQueryTargetValue) > 1024:
            logging.warning(
                "'%s' is to large." % (self.validationCustomQueryTargetValue)
            )
            raise invalidConfiguration(
                "The size of the json document on the custom query exceeds 1024 bytes. Change the query to create a result with less than 512 bytes"
            )

        logging.debug("resultDF:")
        logging.debug(result_df)
        logging.debug("resultJSON: %s" % (result_json))

        logging.debug(
            "Executing export_config.runCustomValidationQueryOnJDBCTable() - Finished"
        )

    def getJDBCTableRowCount(self, whereStatement=None):
        logging.debug("Executing export_config.getJDBCTableRowCount()")

        logging.info(
            "Reading and saving number of rows in target table. This will later be used for validating the export"
        )

        jdbc_rows_full = None
        jdbc_rows_full = self.common_config.getJDBCTableRowCount(
            self.targetSchema, self.targetTable, whereStatement
        )

        # Save the value to the database
        query = "update export_tables set "
        query += "target_rowcount = %s " % (jdbc_rows_full)
        if self.exportTool == "spark":
            # Need to save the sqoop_last_rows if the export tool is spark, as we dont get this from spark the same way as sqoop gives us
            self.sqoop_last_rows = jdbc_rows_full
            query += ",sqoop_last_rows = %s " % (jdbc_rows_full)

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
        return_value = None

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
                    validate_text_target = "Target table"
                    validate_text_source = "Hive table"
                    validate_text = validate_text_target
                else:
                    query = "select hive_rowcount, target_rowcount from export_tables where table_id = %s "
                    validate_text_target = "Target table (incr)"
                    validate_text_source = "Hive table (incr)"
                    validate_text = "Target table"
            else:
                raise undevelopedFeature("Unsupported validation")

            self.mysql_cursor01.execute(query, (self.tableID,))
            logging.debug(
                "SQL Statement executed: %s" % (self.mysql_cursor01.statement)
            )

            row = self.mysql_cursor01.fetchone()
            source_rowcount = row[0]
            target_rowcount = row[1]
            diff_allowed = 0
            logging.debug("source_rowcount: %s" % (source_rowcount))
            logging.debug("target_rowcount: %s" % (target_rowcount))

            if source_rowcount != target_rowcount:
                logging.error(
                    "Validation failed! The %s exceedes the allowed limit compared to the %s table"
                    % (validate_text_source, validate_text_target)
                )
                if target_rowcount > source_rowcount:
                    logging.info(
                        "Diff between tables: %s" % (target_rowcount - source_rowcount)
                    )
                else:
                    logging.info(
                        "Diff between tables: %s" % (source_rowcount - target_rowcount)
                    )
                logging.info(
                    "%s rowcount: %s" % (validate_text_source, source_rowcount)
                )
                logging.info(
                    "%s rowcount: %s" % (validate_text_target, target_rowcount)
                )
                return_value = False
            else:
                logging.info("%s validation successful!" % (validate_text))
                logging.info(
                    "%s rowcount: %s" % (validate_text_source, source_rowcount)
                )
                logging.info(
                    "%s rowcount: %s" % (validate_text_target, target_rowcount)
                )
                logging.info("")
                return_value = True

        else:
            return_value = True

        logging.debug("Executing export_config.validateRowCount() - Finished")
        return return_value

    def calculateJobMappers(self):
        """Based on previous sqoop size, the number of mappers for sqoop will be calculated. Formula is sqoop_size / 128MB."""

        logging.debug("Executing export_config.calculateJobMappers()")
        logging.info(
            "Calculating the number of SQL sessions in the source system that the export will use"
        )

        sql_sessions_min = 1
        sql_sessions_max = None

        # TODO: Add max_export_sessions to jdbc_connections

        row = None

        if row != None and row[0] > 0:
            sql_sessions_max = row[0]
            logging.info(
                "\u001b[33mThis connection is limited to a maximum of %s parallel sessions during export\u001b[0m"
                % (sql_sessions_max)
            )

        if row != None and row[0] == 0:
            logging.warning(
                "Unsupported value of 0 in column 'max_export_sessions' in table 'jdbc_connections'"
            )

        sql_sessions_max_from_config = int(
            self.common_config.getConfigValue(key="export_max_sessions")
        )
        sql_sessions_default = int(
            self.common_config.getConfigValue(key="export_default_sessions")
        )

        if sql_sessions_max == None:
            sql_sessions_max = sql_sessions_max_from_config
        if sql_sessions_max_from_config < sql_sessions_max:
            sql_sessions_max = sql_sessions_max_from_config
        if sql_sessions_default > sql_sessions_max:
            sql_sessions_default = sql_sessions_max

        self.sparkMaxExecutors = sql_sessions_max

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
        sql_sessions_last = row[0]
        self.sqlSessions = row[1]
        self.sqoopMappers = row[2]

        if self.exportTool == "sqoop":
            if self.sqoopMappers > 0:
                logging.info("Setting the number of mappers to a fixed value")
                self.sqlSessions = self.sqoopMappers

            if self.sqlSessions == None:
                logging.info(
                    "Cant find the previous number of SQL sessions used. Will default to %s"
                    % (sql_sessions_default)
                )
                self.sqlSessions = sql_sessions_default
            else:
                if self.sqlSessions < sql_sessions_min:
                    self.sqlSessions = sql_sessions_min
                elif self.sqlSessions > sql_sessions_max:
                    self.sqlSessions = sql_sessions_max

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

    def saveIncrMinMaxValue(self, min_value, max_value):
        """Save the min value in the pendings column"""
        logging.debug("Executing export_config.saveIncrMinValue()")

        query = "update export_tables set "
        query += "	incr_minvalue_pending = %s, "
        query += "	incr_maxvalue_pending = %s "
        query += "where table_id = %s"

        self.mysql_cursor01.execute(
            query,
            (
                min_value,
                max_value,
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

        data_to_export = True

        query = "select incr_minvalue_pending, incr_maxvalue_pending from export_tables where table_id = %s "
        self.mysql_cursor01.execute(query, (self.tableID,))
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement))
        row = self.mysql_cursor01.fetchone()
        min_value = row[0]
        max_value = row[1]

        if min_value == max_value:
            data_to_export = False

        logging.debug("dataToExport: %s" % (data_to_export))
        logging.debug("Executing export_config.isThereIncrDataToExport() - Finished")
        return data_to_export

    def getIncrWhereStatement(self, excludeMinValue=False, whereForTarget=False):
        """Returns the where statement that is needed to only work on the rows that was loaded incr"""
        logging.debug("Executing export_config.getIncrWhereStatement()")

        column_name = self.incr_column
        column_quotation = ""
        where_statement = ""

        query = "select column_type from export_columns where table_id = %s and column_name = %s "
        self.mysql_cursor01.execute(query, (self.tableID, column_name))
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement))
        row = self.mysql_cursor01.fetchone()
        column_type = row[0]

        # COALESCE is needed if you are going to call a function that counts source target rows outside the normal import
        query = "select "
        query += "   COALESCE(incr_minvalue_pending, incr_minvalue), "
        query += "   COALESCE(incr_maxvalue_pending, incr_maxvalue) "
        query += "from export_tables where table_id = %s "
        self.mysql_cursor01.execute(query, (self.tableID,))
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement))
        row = self.mysql_cursor01.fetchone()
        min_value = row[0]
        max_value = row[1]

        # Regardless of how we translate the minValue in the next couple of rows, if it's NONE here, it will never be included in the where
        if min_value == None:
            excludeMinValue = True

        if whereForTarget == True and self.common_config.jdbc_servertype in (
            constant.ORACLE,
            constant.DB2_UDB,
        ):
            column_name = column_name.upper()

        if (
            whereForTarget == True
            and column_type == "timestamp"
            and self.common_config.jdbc_servertype == constant.MSSQL
        ):
            min_value = "CONVERT(datetime, '%s', 121) " % (min_value)
            max_value = "CONVERT(datetime, '%s', 121) " % (max_value)
        elif (
            whereForTarget == True
            and column_type == "timestamp"
            and self.common_config.jdbc_servertype == constant.MYSQL
        ):
            min_value = f"STR_TO_DATE('{min_value}', '%Y-%m-%d %H:%i:%s')"
            max_value = f"STR_TO_DATE('{max_value}', '%Y-%m-%d %H:%i:%s')"
        elif (
            whereForTarget == True
            and column_type == "timestamp"
            and self.common_config.jdbc_servertype == constant.ORACLE
        ):
            min_value = "TO_TIMESTAMP('%s', 'YYYY-MM-DD HH24:MI:SS.FF')" % (min_value)
            max_value = "TO_TIMESTAMP('%s', 'YYYY-MM-DD HH24:MI:SS.FF')" % (max_value)
        elif (
            whereForTarget == True
            and column_type == "timestamp"
            and self.common_config.jdbc_servertype == constant.SNOWFLAKE
        ):
            min_value = "TO_TIMESTAMP('%s', 'YYYY-MM-DD HH24:MI:SS.FF6')" % (min_value)
            max_value = "TO_TIMESTAMP('%s', 'YYYY-MM-DD HH24:MI:SS.FF6')" % (max_value)
        elif (
            whereForTarget == True
            and column_type == "timestamp"
            and self.common_config.jdbc_servertype == constant.POSTGRESQL
        ):
            min_value = "TO_TIMESTAMP('%s', 'YYYY-MM-DD HH24:MI:SS')" % (min_value)
            max_value = "TO_TIMESTAMP('%s', 'YYYY-MM-DD HH24:MI:SS')" % (max_value)
        elif column_type not in (
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
            min_value = '"%s"' % (min_value)
            max_value = '"%s"' % (max_value)

        if (
            whereForTarget == True
            and self.common_config.jdbc_servertype == constant.ORACLE
        ):
            column_quotation = '"'

        if (
            whereForTarget == True
            and self.common_config.jdbc_servertype == constant.SNOWFLAKE
        ):
            column_quotation = '"'

        if whereForTarget == False:
            column_quotation = "`"

        if min_value != None and excludeMinValue == False:
            where_statement += "%s%s%s > %s " % (
                column_quotation,
                column_name,
                column_quotation,
                min_value,
            )
            where_statement += "and "
        where_statement += "%s%s%s <= %s " % (
            column_quotation,
            column_name,
            column_quotation,
            max_value,
        )

        print(where_statement)

        logging.debug("Executing export_config.getIncrWhereStatement() - Finished")
        return where_statement

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

    def addExportTable(self, hive_db, hive_table, dbalias, schema, table):
        """Add source table to export_tables"""
        logging.debug("Executing export_config.addExportTable()")
        return_value = True

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

        logging.debug("hiveDB:    %s" % (hive_db))
        logging.debug("hiveTable: %s" % (hive_table))
        logging.debug("dbalias:   %s" % (dbalias))
        logging.debug("schema:    %s" % (schema))
        logging.debug("table:     %s" % (table))

        try:
            self.mysql_cursor01.execute(
                query, (hive_db, hive_table, dbalias, schema, table)
            )
            self.mysql_conn.commit()
        except mysql.connector.errors.IntegrityError:
            logging.warning(
                "Hive table %s.%s cant be added. The Connection name, Schema and Table already exists"
                % (hive_db, hive_table)
            )
            return_value = False

        logging.debug("Executing export_config.addExportTable() - Finished")
        return return_value
