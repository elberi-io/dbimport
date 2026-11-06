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

import logging
import os
import subprocess
import sys
import time
from datetime import datetime

import boto3
import pandas as pd
import requests
from requests_kerberos import OPTIONAL, HTTPKerberosAuth

from common import constants as constant
from common.Exceptions import SQLerror, databaseNotFound, invalidConfiguration
from common.Singleton import Singleton
from ConfigReader import configuration
from DBImportConfig import export_config
from DBImportOperation import atlas_operations, common_operations


class operation(metaclass=Singleton):
    def __init__(self, connectionAlias=None, targetSchema=None, targetTable=None):
        logging.debug("Executing export_operations.__init__()")

        self.connectionAlias = connectionAlias
        self.targetSchema = targetSchema
        self.targetTable = targetTable
        self.hiveDB = None
        self.hiveTable = None
        self.hiveExportTempDB = None
        self.hiveExportTempTable = None
        self.tempTableNeeded = None

        self.sqoopSize = None
        self.sqoopRows = None
        self.sqoopMappers = None

        self.globalHiveConfigurationSet = False

        self.atlasOperation = atlas_operations.atlasOperation()

        if connectionAlias == None and targetSchema == None and targetTable == None:
            self.export_config = export_config.config()
            self.common_operations = common_operations.operation()
        else:
            try:
                self.export_config = export_config.config(
                    connectionAlias=connectionAlias,
                    targetSchema=targetSchema,
                    targetTable=targetTable,
                )
                self.common_operations = common_operations.operation()

                self.export_config.getExportConfig()

                self.hiveDB = self.export_config.hiveDB
                self.hiveTable = self.export_config.hiveTable
                self.hiveExportTempDB = self.export_config.hiveExportTempDB
                self.hiveExportTempTable = self.export_config.hiveExportTempTable

                self.checkDB(self.hiveDB)
                self.checkTable(self.hiveDB, self.hiveTable)

                self.common_operations.setHiveTable(
                    Hive_DB=self.hiveDB, Hive_Table=self.hiveTable
                )
                self.hiveTableIsTransactional = (
                    self.common_operations.isTableTransactional(
                        hiveDB=self.hiveDB, hiveTable=self.hiveTable
                    )
                )
                self.hiveTableIsView = self.common_operations.isTableView(
                    hiveDB=self.hiveDB, hiveTable=self.hiveTable
                )

            except invalidConfiguration as errMsg:
                logging.error(errMsg)
                self.export_config.remove_temporary_files()
                sys.exit(1)
            except:
                try:
                    self.export_config.remove_temporary_files()
                except:
                    pass
                raise
                sys.exit(1)

        logging.debug("Executing export_operations.__init__() - Finished")

    def runStage(self, stage):
        self.export_config.setStage(stage)

        if (
            self.export_config.common_config.getConfigValue(key="export_stage_disable")
            == True
        ):
            logging.error("Stage execution disabled from DBImport configuration")
            self.export_config.remove_temporary_files()
            sys.exit(1)

        temp_stage = self.export_config.getStage()
        if stage == temp_stage:
            return True
        else:
            return False

    def setStage(self, stage, force=False):
        self.export_config.setStage(stage, force=force)

    def getStage(self):
        return self.export_config.getStage()

    def clearStage(self):
        self.export_config.clearStage()

    def saveRetryAttempt(self, stage):
        self.export_config.saveRetryAttempt(stage)

    def setStageOnlyInMemory(self):
        self.export_config.setStageOnlyInMemory()

    def convertStageStatisticsToJSON(self):
        self.export_config.convertStageStatisticsToJSON()

    def sendStartJSON(self):
        self.export_config.sendStartJSON()

    def saveStageStatistics(self):
        self.export_config.saveStageStatistics()

    def saveIncrPendingValues(self):
        self.export_config.saveIncrPendingValues()

    def resetIncrMinMaxValues(self, maxValue):
        self.export_config.resetIncrMinMaxValues(maxValue=maxValue)

    def resetMaxValueFromTarget(self):
        self.export_config.resetMaxValueFromTarget()

    def removeHiveLocks(self):
        if (
            self.export_config.common_config.getConfigValue(
                key="hive_remove_locks_by_force"
            )
            == True
        ):
            self.common_operations.removeLocksByForce(
                self.hiveExportTempDB, self.hiveExportTempTable
            )

    def checkTable(self, hiveDB, hiveTable):
        if self.common_operations.checkTable(hiveDB, hiveTable) == False:
            logging.error(
                "Hive table '%s' cant be found in '%s' database" % (hiveTable, hiveDB)
            )
            self.export_config.remove_temporary_files()
            sys.exit(1)

    def updateAtlasWithExportData(self):
        if self.atlasOperation.checkAtlasSchema() == True:
            target_schema = self.export_config.targetSchema
            target_table = self.export_config.targetTable

            if self.export_config.common_config.jdbc_servertype in (
                constant.ORACLE,
                constant.DB2_UDB,
            ):
                target_schema = self.export_config.targetSchema.upper()
                target_table = self.export_config.targetTable.upper()

            if self.export_config.common_config.jdbc_servertype in (
                constant.POSTGRESQL
            ):
                target_schema = self.export_config.targetSchema.lower()
                target_table = self.export_config.targetTable.lower()

            config_object = (
                self.export_config.common_config.getAtlasDiscoverConfigObject()
            )
            self.atlasOperation.setConfiguration(config_object)

            start_stop_dict = self.export_config.stage.getStageStartStop(
                stage=self.export_config.exportTool
            )

            # Fetch the remote system schema again as it might have been updated in the export
            self.export_config.common_config.getJDBCTableDefinition(
                source_schema=target_schema, source_table=target_table, printInfo=False
            )
            self.atlasOperation.source_columns_df = (
                self.export_config.common_config.source_columns_df
            )
            self.atlasOperation.source_keys_df = (
                self.export_config.common_config.source_keys_df
            )

            try:
                self.atlasOperation.updateAtlasWithRDBMSdata(
                    schemaName=target_schema, tableName=target_table
                )

                self.atlasOperation.updateAtlasWithExportLineage(
                    hiveDB=self.hiveDB,
                    hiveTable=self.hiveTable,
                    hiveExportTempDB=self.export_config.hiveExportTempDB,
                    hiveExportTempTable=self.export_config.hiveExportTempTable,
                    targetSchema=target_schema,
                    targetTable=target_table,
                    tempTableNeeded=self.export_config.tempTableNeeded,
                    startStopDict=start_stop_dict,
                    fullExecutedCommand=self.export_config.fullExecutedCommand,
                    exportTool=self.export_config.exportTool,
                )
            except:
                pass

    def checkDB(self, hiveDB):
        try:
            self.common_operations.checkDB(hiveDB)
        except databaseNotFound as errMsg:
            logging.error(errMsg)
            self.export_config.remove_temporary_files()
            sys.exit(1)
        except:
            self.export_config.remove_temporary_files()
            raise

    def getHiveTableSchema(self):
        try:
            self.export_config.updateLastUpdateFromHive()
            self.export_config.saveColumnData(
                columnsDF=self.common_operations.getColumns(
                    self.hiveDB,
                    self.hiveTable,
                    includeType=True,
                    includeComment=True,
                    includeIdx=True,
                )
            )
        except invalidConfiguration as errMsg:
            logging.error(errMsg)
            self.export_config.remove_temporary_files()
            sys.exit(1)
        except:
            logging.exception(
                "Fatal error when reading and/or processing Hive table schema"
            )
            self.export_config.remove_temporary_files()
            sys.exit(1)

    def dropJDBCTable(self):
        try:
            if self.export_config.truncateTargetTable == True:
                if (
                    self.export_config.common_config.checkJDBCTable(
                        schema=self.targetSchema, table=self.targetTable
                    )
                    == True
                ):
                    logging.info("Dropping Target Table")
                    self.export_config.common_config.dropJDBCTable(
                        schema=self.targetSchema, table=self.targetTable
                    )
                else:
                    logging.warning("Nothing to drop. Target table does not exists")
        except invalidConfiguration as errMsg:
            logging.error(errMsg)
            self.export_config.remove_temporary_files()
            sys.exit(1)
        except:
            logging.exception("Fatal error when truncating target table")
            self.export_config.remove_temporary_files()
            sys.exit(1)

    def truncateJDBCTable(self, force=False):
        if self.export_config.truncateTargetTable == True or force == True:
            logging.info("Truncating Target Table")
            self.export_config.common_config.truncateJDBCTable(
                schema=self.targetSchema, table=self.targetTable
            )

    def createTargetTable(self):
        try:
            if self.export_config.checkJDBCTable() == False:
                self.export_config.createTargetTable()
        except SQLerror as errMsg:
            logging.error(errMsg)
            self.export_config.remove_temporary_files()
            sys.exit(1)
        except:
            logging.exception("Fatal error when creating the target table")
            self.export_config.remove_temporary_files()
            sys.exit(1)

    def updateTargetTable(self):
        try:
            self.export_config.updateTargetTable()
        except SQLerror as errMsg:
            if "ORA-22859: invalid modification of columns" in str(errMsg):
                # We get this message from Oracle when we try to change a column type that is not supported
                if self.export_config.exportIsIncremental == False:
                    try:
                        logging.info(
                            "There is a column type change that is not supported by Oracle. But because this export is a full export, we will drop the Target table and recreate it automatically"
                        )
                        self.dropJDBCTable()
                        self.createTargetTable()
                        self.export_config.updateTargetTable()
                    except invalidConfiguration as errMsg:
                        logging.error(errMsg)
                        self.export_config.remove_temporary_files()
                        sys.exit(1)
                    except:
                        logging.exception("Fatal error when updating the target table")
                        self.export_config.remove_temporary_files()
                        sys.exit(1)
                else:
                    logging.error(
                        "There is a column type change required on the target table but Oracle doesnt support that change. Drop the table or disable the column is the only option. As this is an incremental export, we cant do that automatically as it might result in loss of data"
                    )
                    self.export_config.remove_temporary_files()
                    sys.exit(1)
            else:
                logging.error(errMsg)
                self.export_config.remove_temporary_files()
                sys.exit(1)
        except invalidConfiguration as errMsg:
            logging.error(errMsg)
            self.export_config.remove_temporary_files()
            sys.exit(1)
        except:
            logging.exception("Fatal error when updating the target table")
            self.export_config.remove_temporary_files()
            sys.exit(1)

    def getJDBCTableRowCount(self):
        try:
            self.export_config.getJDBCTableRowCount()
        except:
            logging.exception("Fatal error when reading source table row count")
            self.export_config.remove_temporary_files()
            sys.exit(1)

    def discoverAndAddTablesFromHive(
        self,
        dbalias,
        schema,
        dbFilter=None,
        tableFilter=None,
        addDBToTable=False,
        addCustomText=None,
        addCounterToTable=False,
        counterStart=None,
    ):
        """This is the main function to search for tables/view in Hive and add them to export_tables"""
        logging.debug("Executing export_operations.discoverAndAddTablesFromHive()")
        error_during_add = False

        source_df = self.common_operations.getTables(
            dbFilter=dbFilter, tableFilter=tableFilter
        )

        if len(source_df) == 0:
            print(
                "There are no tables in the source database that we dont already have in DBImport"
            )
            self.export_config.remove_temporary_files()
            sys.exit(0)

        export_df = self.export_config.getExportTables(dbalias=dbalias, schema=schema)

        merge_df = pd.merge(
            source_df, export_df, on=None, how="outer", indicator="Exist"
        )
        merge_df["targetTable"] = merge_df["hiveTable"]
        discovered_tables = len(merge_df.loc[merge_df["Exist"] == "left_only"])

        if addCounterToTable == True or addDBToTable == True or addCustomText != None:
            for index, row in merge_df.iterrows():
                if merge_df.loc[index, "Exist"] == "left_only":
                    merge_df.loc[index, "targetTable"] = (
                        "_%s" % (merge_df.loc[index, "targetTable"])
                    )

        if addCounterToTable == True:
            if counterStart == None:
                counterStart = "1"
            number_length = len(counterStart)
            try:
                start_value = int(counterStart)
            except ValueError:
                logging.error("The value specified for --counterStart must be a number")
                self.export_config.remove_temporary_files()
                sys.exit(1)

            for index, row in merge_df.iterrows():
                if merge_df.loc[index, "Exist"] == "left_only":
                    zero_to_add = ""
                    while len(zero_to_add) < (number_length - len(str(start_value))):
                        zero_to_add += "0"

                    merge_df.loc[index, "targetTable"] = "%s%s%s" % (
                        zero_to_add,
                        start_value,
                        merge_df.loc[index, "targetTable"],
                    )
                    start_value += 1

        if addDBToTable == True:
            for index, row in merge_df.iterrows():
                if merge_df.loc[index, "Exist"] == "left_only":
                    merge_df.loc[index, "targetTable"] = "%s%s" % (
                        merge_df.loc[index, "hiveDB"],
                        merge_df.loc[index, "targetTable"],
                    )

        if addCustomText != None:
            for index, row in merge_df.iterrows():
                if merge_df.loc[index, "Exist"] == "left_only":
                    merge_df.loc[index, "targetTable"] = "%s%s" % (
                        addCustomText.lower().strip(),
                        merge_df.loc[index, "targetTable"],
                    )

        if discovered_tables == 0:
            print(
                "There are no tables in the source database that we dont already have in DBImport"
            )
            self.export_config.remove_temporary_files()
            sys.exit(0)

        # At this stage, we have discovered tables in the source system that we dont know about in DBImport
        print(
            "The following tables and/or views have been discovered in Hive and not found as export tables in DBImport"
        )
        print("")
        print(
            "%-20s %-40s %-30s %-20s %s"
            % ("Hive DB", "Hive Table", "Connection Alias", "Schema", "Table/View")
        )
        print(
            "============================================================================================================================="
        )

        for index, row in merge_df.loc[merge_df["Exist"] == "left_only"].iterrows():
            print(
                "%-20s %-40s %-30s %-20s %s"
                % (row["hiveDB"], row["hiveTable"], dbalias, schema, row["targetTable"])
            )

        answer = input("Do you want to add these exports to DBImport? (y/N): ")
        if answer == "y":
            print("")
            for index, row in merge_df.loc[merge_df["Exist"] == "left_only"].iterrows():
                add_result = self.export_config.addExportTable(
                    dbalias=dbalias,
                    schema=schema,
                    table=row["targetTable"],
                    hiveDB=row["hiveDB"],
                    hiveTable=row["hiveTable"],
                )

                if add_result == False:
                    error_during_add = True

            if error_during_add == False:
                print("All tables saved successfully in DBImport")
            else:
                print("")
                print(
                    "Not all tables was saved to DBImport. Please check log and output"
                )
        else:
            print("")
            print("Aborting")

        logging.debug(
            "Executing export_operations.discoverAndAddTablesFromHive() - Finished"
        )

    def clearValidationData(self):
        try:
            self.export_config.clearValidationData()
        except invalidConfiguration as errMsg:
            logging.error(errMsg)
            self.export_config.remove_temporary_files()
            sys.exit(1)
        except:
            logging.exception(
                "Fatal error when clearing validation data from previous exports"
            )
            self.export_config.remove_temporary_files()
            sys.exit(1)

    def isExportTempTableNeeded(self):
        try:
            self.tempTableNeeded = self.export_config.isExportTempTableNeeded(
                hiveTableIsTransactional=self.hiveTableIsTransactional,
                hiveTableIsView=self.hiveTableIsView,
            )
        except invalidConfiguration as errMsg:
            logging.error(errMsg)
            self.export_config.remove_temporary_files()
            sys.exit(1)
        except:
            logging.exception(
                "Fatal error when clearing row counts from previous exports"
            )
            self.export_config.remove_temporary_files()
            sys.exit(1)

        return self.tempTableNeeded

    def getIncrMaxvalueFromHive(self, column=None, hiveDB=None, hiveTable=None):
        logging.debug("Executing export_operations.getIncrMaxvalueFromHive()")

        if hiveDB == None:
            hiveDB = self.hiveDB
        if hiveTable == None:
            hiveTable = self.hiveTable
        if column == None:
            column = self.export_config.incr_column

        query = "select max(`%s`) from `%s`.`%s`" % (column, hiveDB, hiveTable)
        result_df = self.common_operations.executeHiveQuery(query)
        max_value = result_df.loc[0][0]
        logging.debug("Maxvalue: %s" % (max_value))

        logging.debug(
            "Executing export_operations.getIncrMaxvalueFromHive() - Finished"
        )
        return max_value

    def updateStatisticsOnExportedTable(
        self,
    ):
        if (
            self.common_operations.isTableView(
                hiveDB=self.hiveDB, hiveTable=self.hiveTable
            )
            == False
        ):
            logging.info("Updating the Hive statistics on the exported table")
            self.common_operations.updateHiveTableStatistics(
                self.hiveDB, self.hiveTable
            )

    def createExportTempTable(self):
        logging.debug("Executing export_operations.createExportTempTable()")

        if (
            self.common_operations.checkTable(
                self.hiveExportTempDB, self.hiveExportTempTable
            )
            == False
        ):
            # Target table does not exist. We just create it in that case
            logging.info(
                "Creating Export Temp table %s.%s in Hive"
                % (self.hiveExportTempDB, self.hiveExportTempTable)
            )

            if self.export_config.exportTool == "sqoop":
                columns_df = self.export_config.getColumnsFromConfigDatabase(
                    excludeColumns=True, getReplacedColumnTypes=True
                )
            else:
                columns_df = self.export_config.getColumnsFromConfigDatabase(
                    excludeColumns=True, getReplacedColumnTypes=False
                )

            query = "create table `%s`.`%s` (" % (
                self.hiveExportTempDB,
                self.hiveExportTempTable,
            )

            first_loop = True
            for index, row in columns_df.iterrows():
                target_column_name = row["targetColumnName"]

                if target_column_name != None and target_column_name.strip() != "":
                    column_name = target_column_name
                else:
                    column_name = row["hiveColumnName"]

                if first_loop == False:
                    query += ", "
                query += "`%s` %s" % (column_name, row["columnType"])
                if row["comment"] != None:
                    query += ' COMMENT "%s"' % (row["comment"])
                first_loop = False
            query += ") STORED AS ORC TBLPROPERTIES ('orc.compress'='ZLIB')"

            self.common_operations.executeHiveQuery(query)
            self.common_operations.reconnectHiveMetaStore()

        logging.debug("Executing export_operations.createExportTempTable() - Finished")

    def connectToHive(
        self,
    ):
        logging.debug("Executing export_operations.connectToHive()")

        try:
            self.common_operations.connectToHive()
        except Exception as ex:
            logging.error(ex)
            self.export_config.remove_temporary_files()
            sys.exit(1)

        if self.globalHiveConfigurationSet == False:
            self.globalHiveConfigurationSet = True
            if self.export_config.hiveJavaHeap != None:
                query = "set hive.tez.container.size=%s" % (
                    self.export_config.hiveJavaHeap
                )
                self.common_operations.executeHiveQuery(query)

        logging.debug("Executing export_operations.connectToHive() - Finished")

    def remove_temporary_files(self):
        self.export_config.remove_temporary_files()

    def checkTimeWindow(self):
        self.export_config.checkTimeWindow()

    def updateExportTempTable(self):
        """Update the Export Temp table based on the column information in the configuration database"""
        logging.debug("Executing export_operations.updateExportTempTable()")
        hive_db = self.hiveExportTempDB
        hive_table = self.hiveExportTempTable

        columns_hive = self.common_operations.getColumns(
            hive_db, hive_table, includeType=True, includeIdx=False
        )

        if self.export_config.exportTool == "sqoop":
            columns_config = self.export_config.getColumnsFromConfigDatabase(
                excludeColumns=True, getReplacedColumnTypes=True
            )
        else:
            columns_config = self.export_config.getColumnsFromConfigDatabase(
                excludeColumns=True, getReplacedColumnTypes=False
            )

        columns_config.rename(
            columns={"hiveColumnName": "name", "columnType": "type"}, inplace=True
        )

        for index, row in columns_config.iterrows():
            target_column_name = row["targetColumnName"]
            if target_column_name != None and target_column_name.strip() != "":
                columns_config.iloc[index]["name"] = target_column_name

        columns_config.drop("targetColumnName", axis=1, inplace=True)

        # Check for missing columns
        columns_config_only_name = columns_config.filter(["name"])
        columns_hive_only_name = columns_hive.filter(["name"])
        columns_merge_only_name = pd.merge(
            columns_config_only_name,
            columns_hive_only_name,
            on=None,
            how="outer",
            indicator="Exist",
        )

        columns_config_count = len(columns_config_only_name)
        columns_hive_count = len(columns_hive_only_name)
        columns_merge_left_only_count = len(
            columns_merge_only_name.loc[columns_merge_only_name["Exist"] == "left_only"]
        )
        columns_merge_right_only_count = len(
            columns_merge_only_name.loc[
                columns_merge_only_name["Exist"] == "right_only"
            ]
        )

        logging.debug("columnsConfigOnlyName")
        logging.debug(columns_config_only_name)
        logging.debug(
            "================================================================"
        )
        logging.debug("columnsHiveOnlyName")
        logging.debug(columns_hive_only_name)
        logging.debug(
            "================================================================"
        )
        logging.debug("columnsMergeOnlyName")
        logging.debug(columns_merge_only_name)
        logging.debug("")

        if (
            columns_config_count == columns_hive_count
            and columns_merge_left_only_count > 0
        ):
            # The number of columns in config and Hive is the same, but there is a difference in name. This is most likely because of a rename of one or more of the columns
            # To handle this, we try a rename. This might fail if the column types are also changed to an incompatable type
            # The logic here is to
            # 1. get all columns from mergeDF that exists in Left_Only
            # 2. Get the position in configDF with that column name
            # 3. Get the column in the same position from HiveDF
            # 4. Check if that column name exists in the mergeDF with Right_Only. If it does, the column was just renamed
            for index, row in columns_merge_only_name.loc[
                columns_merge_only_name["Exist"] == "left_only"
            ].iterrows():
                row_in_config = columns_config.loc[
                    columns_config["name"] == row["name"]
                ].iloc[0]
                index_in_config = columns_config.loc[
                    columns_config["name"] == row["name"]
                ].index.item()
                row_in_hive = columns_hive.iloc[index_in_config]

                if (
                    len(
                        columns_merge_only_name.loc[
                            (columns_merge_only_name["Exist"] == "right_only")
                            & (columns_merge_only_name["name"] == row_in_hive["name"])
                        ]
                    )
                    > 0
                ):
                    # This is executed if the column is renamed and exists in the same position
                    logging.debug("Name in config:  %s" % (row_in_config["name"]))
                    logging.debug("Type in config:  %s" % (row_in_config["type"]))
                    logging.debug("Index in config: %s" % (index_in_config))
                    logging.debug("--------------------")
                    logging.debug("Name in Hive: %s" % (row_in_hive["name"]))
                    logging.debug("Type in Hive: %s" % (row_in_hive["type"]))
                    logging.debug("======================================")
                    logging.debug("")

                    query = "alter table `%s`.`%s` change column `%s` `%s` %s" % (
                        hive_db,
                        hive_table,
                        row_in_hive["name"],
                        row_in_config["name"],
                        row_in_config["type"],
                    )
                    self.common_operations.executeHiveQuery(query)

                    self.export_config.logHiveColumnRename(
                        row_in_config["name"],
                        row_in_hive["name"],
                        hiveDB=hive_db,
                        hiveTable=hive_table,
                    )

                    if row_in_config["type"] != row_in_hive["type"]:
                        self.export_config.logHiveColumnTypeChange(
                            row_in_config["name"],
                            row_in_config["type"],
                            previous_columnType=row_in_hive["type"],
                            hiveDB=hive_db,
                            hiveTable=hive_table,
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
                        row_in_config = columns_config.loc[
                            columns_config["name"] == row_in_merge_left["name"]
                        ].iloc[0]
                        row_in_hive = columns_hive.loc[
                            columns_hive["name"] == row_in_merge_right["name"]
                        ].iloc[0]
                        logging.debug(row_in_config["name"])
                        logging.debug(row_in_config["type"])
                        logging.debug("--------------------")
                        logging.debug(row_in_hive["name"])
                        logging.debug(row_in_hive["type"])

                        query = "alter table `%s`.`%s` change column `%s` `%s` %s" % (
                            hive_db,
                            hive_table,
                            row_in_hive["name"],
                            row_in_config["name"],
                            row_in_hive["type"],
                        )
                        self.common_operations.executeHiveQuery(query)

                        self.export_config.logHiveColumnRename(
                            row_in_config["name"],
                            row_in_hive["name"],
                            hiveDB=hive_db,
                            hiveTable=hive_table,
                        )

            self.common_operations.reconnectHiveMetaStore()
            columns_hive = self.common_operations.getColumns(
                hive_db, hive_table, includeType=True, includeIdx=False
            )
            columns_hive_only_name = columns_hive.filter(["name"])
            columns_merge_only_name = pd.merge(
                columns_config_only_name,
                columns_hive_only_name,
                on=None,
                how="outer",
                indicator="Exist",
            )

        for index, row in columns_merge_only_name.loc[
            columns_merge_only_name["Exist"] == "left_only"
        ].iterrows():
            # This will iterate over columns that only exists in the config and not in Hive. We add these to Hive
            full_row = columns_config.loc[columns_config["name"] == row["name"]].iloc[0]
            query = "alter table `%s`.`%s` add columns (`%s` %s" % (
                hive_db,
                hive_table,
                full_row["name"],
                full_row["type"],
            )
            query += ")"

            self.common_operations.executeHiveQuery(query)

            self.export_config.logHiveColumnAdd(
                full_row["name"],
                columnType=full_row["type"],
                hiveDB=hive_db,
                hiveTable=hive_table,
            )

        # Check for changed column types
        self.common_operations.reconnectHiveMetaStore()
        columns_hive = self.common_operations.getColumns(
            hive_db, hive_table, includeType=True, includeIdx=False, includeComment=True
        )

        columns_config_only_name_type = columns_config.filter(
            ["name", "type"]
        ).sort_values(by=["name"], ascending=True)
        columns_hive_only_name_type = columns_hive.filter(["name", "type"]).sort_values(
            by=["name"], ascending=True
        )
        columns_merge_only_name_type = pd.merge(
            columns_config_only_name_type,
            columns_hive_only_name_type,
            on=None,
            how="outer",
            indicator="Exist",
        )

        logging.debug("columnsConfigOnlyNameType")
        logging.debug(columns_config_only_name_type)
        logging.debug(
            "================================================================"
        )
        logging.debug("columnsHiveOnlyNameType")
        logging.debug(columns_hive_only_name_type)
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
            query = "alter table `%s`.`%s` change column `%s` `%s` %s" % (
                hive_db,
                hive_table,
                row["name"],
                row["name"],
                row["type"],
            )
            self.common_operations.executeHiveQuery(query)

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

            self.export_config.logHiveColumnTypeChange(
                row["name"],
                columnType=row["type"],
                previous_columnType=previous_column_type,
                hiveDB=hive_db,
                hiveTable=hive_table,
            )

        # Check for change column comments
        self.common_operations.reconnectHiveMetaStore()
        columns_hive = self.common_operations.getColumns(
            hive_db, hive_table, includeType=True, includeIdx=False, includeComment=True
        )
        columns_hive["comment"].replace(
            "", None, inplace=True
        )  # Replace blank column comments with None as it would otherwise trigger an alter table on every run
        columns_merge = pd.merge(
            columns_config, columns_hive, on=None, how="outer", indicator="Exist"
        )

        for index, row in columns_merge.loc[
            columns_merge["Exist"] == "left_only"
        ].iterrows():
            if row["comment"] == None:
                row["comment"] = ""
            query = 'alter table `%s`.`%s` change column `%s` `%s` %s comment "%s"' % (
                hive_db,
                hive_table,
                row["name"],
                row["name"],
                row["type"],
                row["comment"],
            )

            self.common_operations.executeHiveQuery(query)

        logging.debug("Executing export_operations.updateTargetTable() - Finished")

    def truncateExportTempTable(
        self,
    ):
        logging.info("Truncating Export Temp table in Hive")
        self.common_operations.truncateHiveTable(
            self.hiveExportTempDB, self.hiveExportTempTable
        )

    def updateStatisticsOnExportTempTable(
        self,
    ):
        logging.info("Updating the Hive statistics on the Export Temp table")
        self.common_operations.updateHiveTableStatistics(
            self.hiveExportTempDB, self.hiveExportTempTable
        )

    def insertDataIntoExportTempTable(self):
        """Insert data from the source Hive Table into the Export Temp Table"""
        logging.debug("Executing export_operations.insertDataIntoExportTempTable()")
        logging.info("Inserting data into the the Export Temp table")

        columns_config = self.export_config.getColumnsFromConfigDatabase(
            excludeColumns=True
        )

        query = "insert into `%s`.`%s` (" % (
            self.hiveExportTempDB,
            self.hiveExportTempTable,
        )

        first_loop = True
        for index, row in columns_config.iterrows():
            if first_loop == False:
                query += ", "
            target_column_name = row["targetColumnName"]
            column_name = row["hiveColumnName"]
            if target_column_name != None and target_column_name.strip() != "":
                column_name = target_column_name

            query += "`%s`" % (column_name)
            first_loop = False

        query += ") select "
        first_loop = True
        for index, row in columns_config.iterrows():
            if first_loop == False:
                query += ", "
            query += "`%s`" % (row["hiveColumnName"])
            first_loop = False

        query += " from `%s`.`%s` " % (self.hiveDB, self.hiveTable)

        if self.export_config.exportIsIncremental == True:
            query += "where "
            query += self.export_config.getIncrWhereStatement()

        self.common_operations.executeHiveQuery(query)
        logging.debug(
            "Executing export_operations.insertDataIntoExportTempTable() - Finished"
        )

    def fetchIncrMinMaxvalue(self):
        logging.debug("Executing export_operations.fetchIncrMinMaxvalue()")

        max_value = str(self.getIncrMaxvalueFromHive())
        min_value = self.export_config.incr_maxvalue

        self.export_config.saveIncrMinMaxValue(minValue=min_value, maxValue=max_value)

        logging.debug("Executing export_operations.fetchIncrMinMaxvalue() - Finished")

    def runSparkExport(self):
        logging.debug("Executing export_operations.runSparkExport()")

        # Fetch the number of executors and sql splits that should be used
        self.export_config.calculateJobMappers()

        self.sparkStartUTS = int(time.time())

        force_column_uppercase = False
        if self.export_config.common_config.jdbc_servertype in (
            constant.ORACLE,
            constant.DB2_UDB,
        ):
            force_column_uppercase = True
            target_schema = self.targetSchema.upper()
            target_table = self.targetTable.upper()
        elif self.export_config.common_config.jdbc_servertype in (constant.POSTGRESQL):
            target_schema = self.targetSchema.lower()
            target_table = self.targetTable.lower()
        else:
            target_schema = self.targetSchema
            target_table = self.targetTable

        if self.export_config.common_config.jdbc_servertype == constant.AWS_S3:
            # As we cant verify schema on S3 files, we are just going to read what we have in Hive and export all columns

            columns_config = self.export_config.getColumnsFromConfigDatabase(
                excludeColumns=True, forceColumnUppercase=force_column_uppercase
            )

            # Generate the SQL used to query Hive
            column_list = ""
            column_starting_with_underscore_found = False
            for index, row in columns_config.iterrows():
                column_name = row["hiveColumnName"]
                target_column_name = row["targetColumnName"]

                if column_list != "":
                    column_list += ", "

                if target_column_name != None and target_column_name.strip() != "":
                    if self.isExportTempTableNeeded() == True:
                        column_list += "`%s`" % (target_column_name)
                    else:
                        column_list += "`%s` as `%s`" % (
                            column_name,
                            target_column_name,
                        )
                else:
                    # No rename of the column
                    column_list += "`%s`" % (column_name)
                    target_column_name = ""

                if column_name.startswith("_") or target_column_name.startswith("_"):
                    # This might or might now be supported depending on spark version. Will check after spark is initialized
                    column_starting_with_underscore_found = True

            if self.isExportTempTableNeeded() == True:
                hive_db = self.hiveExportTempDB
                hive_table = self.hiveExportTempTable
            else:
                hive_db = self.hiveDB
                hive_table = self.hiveTable

            spark_query = "select %s from `%s`.`%s`" % (
                column_list,
                hive_db,
                hive_table,
            )

        else:
            self.export_config.common_config.getJDBCTableDefinition(
                source_schema=target_schema, source_table=target_table
            )
            columns_target = self.export_config.common_config.source_columns_df
            columns_target.rename(columns={"SOURCE_COLUMN_NAME": "name"}, inplace=True)
            columns_target.drop("IS_NULLABLE", axis=1, inplace=True)
            columns_target.drop("SOURCE_COLUMN_COMMENT", axis=1, inplace=True)
            columns_target.drop("SOURCE_COLUMN_TYPE", axis=1, inplace=True)
            columns_target.drop("SOURCE_COLUMN_LENGTH", axis=1, inplace=True)
            columns_target.drop("TABLE_TYPE", axis=1, inplace=True)
            columns_target.drop("TABLE_COMMENT", axis=1, inplace=True)
            columns_target.drop("TABLE_CREATE_TIME", axis=1, inplace=True)
            columns_target.drop("DEFAULT_VALUE", axis=1, inplace=True)

            columns_config = self.export_config.getColumnsFromConfigDatabase(
                excludeColumns=True, forceColumnUppercase=force_column_uppercase
            )

            logging.debug(
                "================================================================="
            )
            logging.debug("columnsTarget")
            logging.debug(columns_target)
            logging.debug(
                "================================================================="
            )

            logging.debug("columnsConfig")
            logging.debug(columns_config)
            logging.debug(
                "================================================================="
            )

            # Generate the SQL used to query Hive
            column_list = ""
            column_starting_with_underscore_found = False
            for index, row in columns_config.iterrows():
                column_name = row["hiveColumnName"]
                target_column_name = row["targetColumnName"]

                if target_column_name != None and target_column_name.strip() != "":
                    check_column = target_column_name
                else:
                    check_column = column_name
                    target_column_name = ""

                if (
                    columns_target[columns_target["name"] == check_column].empty
                    == False
                ):
                    # Column exists in target table
                    if column_list != "":
                        column_list += ", "

                    if check_column == column_name:
                        # No rename of the column
                        column_list += "`%s`" % (column_name)
                    else:
                        # Rename of the column
                        if self.isExportTempTableNeeded() == True:
                            column_list += "`%s`" % (target_column_name)
                        else:
                            column_list += "`%s` as `%s`" % (
                                column_name,
                                target_column_name,
                            )

                    if column_name.startswith("_") or target_column_name.startswith(
                        "_"
                    ):
                        # This might or might now be supported depending on spark version. Will check after spark is initialized
                        column_starting_with_underscore_found = True

            if self.isExportTempTableNeeded() == True:
                hive_db = self.hiveExportTempDB
                hive_table = self.hiveExportTempTable
            else:
                hive_db = self.hiveDB
                hive_table = self.hiveTable

            spark_query = "select %s from `%s`.`%s`" % (
                column_list,
                hive_db,
                hive_table,
            )

            # Sets the correct spark table and schema that will be used to write to
            spark_write_table = ""
            if self.export_config.common_config.db_mysql == True:
                spark_write_table = target_table
            else:
                if (
                    self.export_config.common_config.jdbc_servertype
                    == constant.SNOWFLAKE
                ):
                    spark_write_table = '"%s"."%s"' % (target_schema, target_table)
                else:
                    spark_write_table = "%s.%s" % (target_schema, target_table)

        # Setup the additional path required to find the libraries/modules
        for path in self.export_config.common_config.sparkPathAppend.split(","):
            sys.path.append(path.strip())

        # Create a valid PYSPARK_SUBMIT_ARGS string
        spark_pyspark_submit_args = "--jars "
        spark_jars = ""

        first_loop = True
        if self.export_config.common_config.sparkJarFiles.strip() != "":
            for jar_file in self.export_config.common_config.sparkJarFiles.split(","):
                if first_loop == False:
                    spark_pyspark_submit_args += ","
                    spark_jars += ","
                spark_pyspark_submit_args += jar_file.strip()
                spark_jars += jar_file.strip()
                first_loop = False

        for jar_file in self.export_config.common_config.jdbc_classpath.split(":"):
            if first_loop == False:
                spark_pyspark_submit_args += ","
                spark_jars += ","
            spark_pyspark_submit_args += jar_file.strip()
            spark_jars += jar_file.strip()
            first_loop = False

        if self.export_config.common_config.sparkPyFiles.strip() != "":
            spark_pyspark_submit_args += " --py-files "
            first_loop = True
            for py_file in self.export_config.common_config.sparkPyFiles.split(","):
                if first_loop == False:
                    spark_pyspark_submit_args += ","
                spark_pyspark_submit_args += py_file.strip()
                first_loop = False

        spark_pyspark_submit_args += " pyspark-shell"

        # Set required OS parameters
        os.environ["PYSPARK_SUBMIT_ARGS"] = spark_pyspark_submit_args

        logging.debug("")
        logging.debug(
            "======================================================================="
        )
        logging.debug("sparkQuery: %s" % (spark_query))
        logging.debug("parallell sessions: %s" % (self.export_config.sqlSessions))
        logging.debug(
            "Spark Executor Memory: %s"
            % (self.export_config.common_config.sparkExecutorMemory)
        )
        logging.debug(
            "Spark Max Executors: %s" % (self.export_config.sparkMaxExecutors)
        )
        logging.debug(
            "Spark Dynamic Executors: %s"
            % (self.export_config.common_config.sparkDynamicAllocation)
        )
        logging.debug(
            "======================================================================="
        )

        print(" _____________________ ")
        print("|                     |")
        print("| Spark Export starts |")
        print("|_____________________|")
        print("")
        logging.info("Exporting Hive table `%s`.`%s`" % (hive_db, hive_table))
        sys.stdout.flush()

        # import all packages after the environment is set
        from pyspark import HiveContext
        from pyspark.context import SparkConf, SparkContext
        from pyspark.sql import SparkSession

        conf = SparkConf()
        conf.setMaster(self.export_config.common_config.sparkMaster)
        conf.set(
            "spark.submit.deployMode", self.export_config.common_config.sparkDeployMode
        )
        conf.setAppName("DBImport Export - %s.%s" % (self.hiveDB, self.hiveTable))
        conf.set("spark.jars", spark_jars)
        conf.set(
            "spark.executor.memory",
            self.export_config.common_config.sparkExecutorMemory,
        )
        conf.set("spark.yarn.queue", self.export_config.common_config.sparkYarnQueue)
        conf.set("spark.hadoop.yarn.timeline-service.enabled", "false")
        conf.set("spark.hive.llap.execution.mode", "only")
        conf.set("spark.sql.extensions", "com.hortonworks.spark.sql.rule.Extensions")
        conf.set("spark.datasource.hive.warehouse.read.mode", "DIRECT_READER_V1")
        conf.set("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY")

        if self.export_config.common_config.sparkDynamicAllocation == True:
            conf.set("spark.shuffle.service.enabled", "true")
            conf.set("spark.dynamicAllocation.enabled", "true")
            conf.set("spark.dynamicAllocation.minExecutors", "0")
            conf.set(
                "spark.dynamicAllocation.maxExecutors",
                str(self.export_config.sparkMaxExecutors),
            )
            logging.info(
                "Number of executors is dynamic with a max value of %s executors"
                % (self.export_config.sparkMaxExecutors)
            )
            self.sqoopMappers = -1
        else:
            conf.set("spark.dynamicAllocation.enabled", "false")
            conf.set("spark.shuffle.service.enabled", "false")
            if self.export_config.sqlSessions < self.export_config.sparkMaxExecutors:
                conf.set(
                    "spark.executor.instances", str(self.export_config.sqlSessions)
                )
                logging.info(
                    "Number of executors is fixed at %s"
                    % (self.export_config.sqlSessions)
                )
                self.sqoopMappers = self.export_config.sqlSessions
            else:
                conf.set(
                    "spark.executor.instances",
                    str(self.export_config.sparkMaxExecutors),
                )
                logging.info(
                    "Number of executors is fixed at %s"
                    % (self.export_config.sparkMaxExecutors)
                )
                self.sqoopMappers = self.export_config.sparkMaxExecutors

        if self.export_config.common_config.jdbc_servertype == constant.AWS_S3:
            conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")

            if self.export_config.common_config.awsS3proxyServer != None:
                os.environ["HTTPS_PROXY"] = (
                    self.export_config.common_config.awsS3proxyServer
                )
                aws_s3_proxy_host = (
                    self.export_config.common_config.awsS3proxyServer.split("//")[
                        1
                    ].split(":")[0]
                )
                aws_s3_proxy_port = (
                    self.export_config.common_config.awsS3proxyServer.split("//")[
                        1
                    ].split(":")[1]
                )
                conf.set("spark.hadoop.fs.s3a.proxy.host", aws_s3_proxy_host)
                conf.set("spark.hadoop.fs.s3a.proxy.port", aws_s3_proxy_port)
                logging.debug("awsS3proxyHost: %s" % aws_s3_proxy_host)
                logging.debug("awsS3proxyPort: %s" % aws_s3_proxy_port)

            if self.export_config.common_config.awsS3assumeRole != None:
                session = boto3.Session(
                    aws_access_key_id=self.export_config.common_config.jdbc_username,
                    aws_secret_access_key=self.export_config.common_config.jdbc_password,
                    region_name=self.export_config.common_config.awsS3region,
                )

                sts = session.client("sts")
                role_session_name = "DBImport_export_%s.%s" % (
                    self.hiveDB,
                    self.hiveTable,
                )

                if (
                    self.export_config.common_config.awsS3externalId == None
                    and self.export_config.common_config.awsS3assumeRoleDuration == None
                ):
                    response = sts.assume_role(
                        RoleArn=self.export_config.common_config.awsS3assumeRole,
                        RoleSessionName=role_session_name[0:64],
                    )
                elif (
                    self.export_config.common_config.awsS3externalId == None
                    and self.export_config.common_config.awsS3assumeRoleDuration != None
                ):
                    response = sts.assume_role(
                        RoleArn=self.export_config.common_config.awsS3assumeRole,
                        RoleSessionName=role_session_name[0:64],
                        DurationSeconds=self.export_config.common_config.awsS3assumeRoleDuration,
                    )
                elif (
                    self.export_config.common_config.awsS3externalId != None
                    and self.export_config.common_config.awsS3assumeRoleDuration == None
                ):
                    response = sts.assume_role(
                        RoleArn=self.export_config.common_config.awsS3assumeRole,
                        RoleSessionName=role_session_name[0:64],
                        ExternalId=self.export_config.common_config.awsS3externalId,
                    )
                else:
                    response = sts.assume_role(
                        RoleArn=self.export_config.common_config.awsS3assumeRole,
                        RoleSessionName=role_session_name[0:64],
                        ExternalId=self.export_config.common_config.awsS3externalId,
                        DurationSeconds=self.export_config.common_config.awsS3assumeRoleDuration,
                    )

                credentials = response["Credentials"]
                logging.debug(credentials)

                conf.set(
                    "spark.hadoop.fs.s3a.aws.credentials.provider",
                    "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider",
                )
                conf.set("spark.hadoop.fs.s3a.access.key", credentials["AccessKeyId"])
                conf.set(
                    "spark.hadoop.fs.s3a.secret.key", credentials["SecretAccessKey"]
                )
                conf.set(
                    "spark.hadoop.fs.s3a.session.token", credentials["SessionToken"]
                )

                s3client = boto3.resource(
                    "s3",
                    region_name=self.export_config.common_config.awsS3region,
                    config=boto3.session.Config(s3={"addressing_style": "virtual"}),
                    aws_access_key_id=credentials["AccessKeyId"],
                    aws_secret_access_key=credentials["SecretAccessKey"],
                    aws_session_token=credentials["SessionToken"],
                )

            else:
                conf.set(
                    "spark.hadoop.fs.s3a.access.key",
                    self.export_config.common_config.jdbc_username,
                )
                conf.set(
                    "spark.hadoop.fs.s3a.secret.key",
                    self.export_config.common_config.jdbc_password,
                )

                s3client = boto3.resource(
                    "s3",
                    aws_access_key_id=self.export_config.common_config.jdbc_username,
                    aws_secret_access_key=self.export_config.common_config.jdbc_password,
                )

            conf.set(
                "spark.hadoop.fs.s3a.endpoint",
                "s3.%s.amazonaws.com" % self.export_config.common_config.awsS3region,
            )
        else:
            jdbc_connection_properties = {}
            jdbc_connection_properties["user"] = (
                self.export_config.common_config.jdbc_username
            )
            jdbc_connection_properties["password"] = (
                self.export_config.common_config.jdbc_password
            )
            jdbc_connection_properties["driver"] = (
                self.export_config.common_config.jdbc_driver
            )

        logging.debug(conf.getAll())

        if self.export_config.common_config.sparkHiveLibrary == "HiveWarehouseSession":
            # Configuration for HDP 3.x and CDP
            from pyspark_llap import HiveWarehouseSession

            sc = SparkContext(conf=conf)
            spark = SparkSession(sc)

        elif self.export_config.common_config.sparkHiveLibrary == "HiveContext":
            # Configuration for HDP 2.x
            sc = SparkContext(conf=conf)
            spark = SparkSession(sc)

        self.yarnApplicationID = sc.applicationId
        logging.info("Yarn application started with id %s" % (self.yarnApplicationID))
        sys.stdout.flush()

        self.export_config.common_config.updateYarnStatistics(
            self.yarnApplicationID, "spark"
        )

        # Determine Spark version and find incompatable exports
        spark_version_split = spark.version.split(".")
        spark_major_version = int(spark_version_split[0])
        spark_version = "%s.%s.%s" % (
            spark_version_split[0],
            spark_version_split[1],
            spark_version_split[2],
        )
        logging.info("Running with Spark Version %s" % (spark_version))

        if spark_version < "2.3.2" and column_starting_with_underscore_found == True:
            logging.error(
                "Spark version 2.3.2 or higher is required to export columns starting with '_'. This export is running with version %s"
                % (spark_version)
            )
            sc.stop()
            self.remove_temporary_files()
            sys.exit(1)

        sys.stdout.flush()

        if self.export_config.common_config.sparkHiveLibrary == "HiveWarehouseSession":
            # Get DataFrame for HDP 3.x
            hive = HiveWarehouseSession.session(spark).build()
            df = hive.sql(spark_query)

        elif self.export_config.common_config.sparkHiveLibrary == "HiveContext":
            # Get DataFrame for HDP 2.x
            df = HiveContext(sc).sql(spark_query)

        if self.export_config.exportIsIncremental == True:
            df = df.filter(self.export_config.getIncrWhereStatement())

        if self.export_config.sqlWhereAddition != None:
            df = df.filter("(%s)" % (self.export_config.sqlWhereAddition))

        if self.export_config.common_config.jdbc_servertype == constant.AWS_S3:
            logging.info("Writing data to AWS S3 Bucket")
            sys.stdout.flush()
            df.write.mode("overwrite").parquet(self.export_config.awsS3bucketPath)

            logging.info("Writing schema to AWS S3 Bucket")
            sys.stdout.flush()
            df_schema = df.schema.json().encode("ascii")
            s3client.Object(
                self.export_config.awsS3bucket,
                "%s/_schema.json" % (self.export_config.awsS3path),
            ).put(Body=df_schema)

        else:
            # Write data to target database
            df.write.mode("append").jdbc(
                url=self.export_config.common_config.jdbc_url,
                table=spark_write_table,
                properties=jdbc_connection_properties,
            )
        spark_url = "%s/api/v1/applications/%s/allexecutors" % (
            sc.uiWebUrl,
            self.yarnApplicationID,
        )
        self.kerberosPrincipal = configuration.get("Kerberos", "principal")
        spark_auth = HTTPKerberosAuth(
            force_preemptive=True,
            principal=self.kerberosPrincipal,
            mutual_authentication=OPTIONAL,
        )

        requests.packages.urllib3.disable_warnings()  # Disable SSL/Cert warnings for Spark application API call
        response = requests.get(spark_url, auth=spark_auth, verify=False)
        response_json = response.json()

        spark_executor_count = 0
        for i in response_json:
            if i["id"] != "driver":
                spark_executor_count += 1

        self.export_config.common_config.updateYarnStatistics(
            self.yarnApplicationID, "spark", yarnContainersTotal=spark_executor_count
        )

        time.sleep(
            1
        )  # Sleep 1 sec in order to avoid Yarn applications finished before program is able to get state
        sc.stop()

        print(" ________________________ ")
        print("|                        |")
        print("| Spark Export completed |")
        print("|________________________|")
        print("")
        sys.stdout.flush()

        try:
            self.export_config.saveExportStatistics(
                self.sparkStartUTS, sqoopMappers=self.sqoopMappers
            )
        except:
            logging.error("Fatal error when saving spark statistics")
            pass
            self.export_config.remove_temporary_files()
            sys.exit(1)

        logging.debug("Executing export_operations.runSparkExport() - Finished")

    def runSqoopExport(self):
        logging.debug("Executing export_operations.runSqoopExport()")

        self.sqoopStartTimestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        self.sqoopStartUTS = int(time.time())

        force_column_uppercase = False
        if self.export_config.common_config.jdbc_servertype in (
            constant.ORACLE,
            constant.DB2_UDB,
        ):
            force_column_uppercase = True
            target_schema = self.targetSchema.upper()
            target_table = self.targetTable.upper()
        elif self.export_config.common_config.jdbc_servertype in (constant.POSTGRESQL):
            target_schema = self.targetSchema.lower()
            target_table = self.targetTable.lower()
        else:
            target_schema = self.targetSchema
            target_table = self.targetTable

        self.export_config.common_config.getJDBCTableDefinition(
            source_schema=target_schema, source_table=target_table
        )
        columns_target = self.export_config.common_config.source_columns_df
        columns_target.rename(columns={"SOURCE_COLUMN_NAME": "name"}, inplace=True)
        columns_target.drop("IS_NULLABLE", axis=1, inplace=True)
        columns_target.drop("SOURCE_COLUMN_COMMENT", axis=1, inplace=True)
        columns_target.drop("SOURCE_COLUMN_TYPE", axis=1, inplace=True)
        columns_target.drop("SOURCE_COLUMN_LENGTH", axis=1, inplace=True)
        columns_target.drop("TABLE_TYPE", axis=1, inplace=True)
        columns_target.drop("TABLE_COMMENT", axis=1, inplace=True)
        columns_target.drop("TABLE_CREATE_TIME", axis=1, inplace=True)
        columns_target.drop("DEFAULT_VALUE", axis=1, inplace=True)

        if self.isExportTempTableNeeded() == True:
            hive_db = self.hiveExportTempDB
            hive_table = self.hiveExportTempTable
        else:
            hive_db = self.hiveDB
            hive_table = self.hiveTable

        logging.debug("forceColumnUppercase: %s" % (force_column_uppercase))

        columns_config = self.export_config.getColumnsFromConfigDatabase(
            excludeColumns=True, forceColumnUppercase=force_column_uppercase
        )
        columns_config.rename(columns={"hiveColumnName": "name"}, inplace=True)
        for index, row in columns_config.iterrows():
            target_column_name = row["targetColumnName"]
            if target_column_name != None and target_column_name.strip() != "":
                columns_config.iloc[index]["name"] = target_column_name
        columns_config.drop("targetColumnName", axis=1, inplace=True)
        columns_config.drop("columnType", axis=1, inplace=True)
        columns_config.drop("comment", axis=1, inplace=True)
        columns_merge = pd.merge(
            columns_config, columns_target, on=None, how="outer", indicator="Exist"
        )

        column_list = []
        for index, row in columns_merge.loc[
            columns_merge["Exist"] == "both"
        ].iterrows():
            if force_column_uppercase == True:
                column_list.append(row["name"].upper())
            else:
                column_list.append(row["name"])

        schema_options = ""

        # Fetch the number of mappers that should be used
        self.export_config.calculateJobMappers()

        # Sets the correct sqoop table and schema that will be used if a custom SQL is not used
        sqoop_target_schema = []
        sqoop_target_table = ""
        sqoop_direct_option = ""
        if (
            self.export_config.common_config.db_mssql == True
            or self.export_config.common_config.db_postgresql == True
        ):
            sqoop_target_schema = ["--", "--schema", target_schema]
            sqoop_target_table = target_table
        if self.export_config.common_config.db_oracle == True:
            sqoop_target_table = "%s.%s" % (target_schema, target_table)
        if self.export_config.common_config.db_mysql == True:
            sqoop_target_table = target_table
        if self.export_config.common_config.db_db2udb == True:
            sqoop_target_table = "%s.%s" % (target_schema, target_table)

        sqoop_command = []
        sqoop_command.extend(
            ["sqoop", "export", "-D", "mapreduce.job.user.classpath.first=true"]
        )
        sqoop_command.extend(
            [
                "-D",
                "mapreduce.job.queuename=%s"
                % (configuration.get("Sqoop", "yarnqueue")),
            ]
        )
        sqoop_command.extend(["-D", "oraoop.disabled=true"])
        sqoop_command.extend(["-D", "yarn.timeline-service.enabled=false"])
        # TODO: Add records to settings
        sqoop_command.extend(["-D", "sqoop.export.records.per.statement=10000"])
        sqoop_command.extend(["-D", "sqoop.export.records.per.transaction=1"])
        sqoop_command.extend(["--class-name", "dbimport"])

        sqoop_command.extend(["--hcatalog-database", hive_db])
        sqoop_command.extend(["--hcatalog-table", hive_table])

        sqoop_command.extend(["--outdir", self.export_config.common_config.tempdir])
        sqoop_command.extend(
            ["--connect", '"%s"' % (self.export_config.common_config.jdbc_url)]
        )
        sqoop_command.extend(
            ["--username", self.export_config.common_config.jdbc_username]
        )
        sqoop_command.extend(
            [
                "--password-file",
                "file://%s" % (self.export_config.common_config.jdbc_password_file),
            ]
        )
        if sqoop_direct_option != "":
            sqoop_command.append(sqoop_direct_option)

        map_column_java = self.export_config.getSqoopMapColumnJava()
        if len(map_column_java) > 0:
            sqoop_command.extend(["--map-column-java", ",".join(map_column_java)])

        sqoop_command.extend(["--num-mappers", str(self.export_config.sqlSessions)])

        # If we dont have a SQL query to use for sqoop, then we need to specify the table instead
        sqoop_command.extend(["--columns", ",".join(column_list)])
        sqoop_command.extend(["--table", sqoop_target_table])

        sqoop_command.extend(sqoop_target_schema)

        logging.info("Starting sqoop with the following command: %s" % (sqoop_command))
        sqoop_output = ""

        print(" _____________________ ")
        print("|                     |")
        print("| Sqoop Export starts |")
        print("|_____________________|")
        print("")

        # Start Sqoop
        sh_session = subprocess.Popen(
            sqoop_command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
        )

        # Print Stdout and stderr while sqoop is running
        while sh_session.poll() == None:
            row = sh_session.stdout.readline().decode("utf-8").rstrip()
            if row != "":
                print(row)
                sys.stdout.flush()
                sqoop_output += row + "\n"
                self.parseSqoopOutput(row)

        # Print what is left in output after sqoop finished
        for row in sh_session.stdout.readlines():
            row = row.decode("utf-8").rstrip()
            if row != "":
                print(row)
                sys.stdout.flush()
                sqoop_output += row + "\n"
                self.parseSqoopOutput(row)

        if sh_session.returncode != 0:
            sqoop_output += " ERROR "

        print(" ________________________ ")
        print("|                        |")
        print("| Sqoop Export completed |")
        print("|________________________|")
        print("")

        # At this stage, the entire sqoop job is run and we fetched all data from the output. Lets parse and store it
        if self.sqoopSize == None:
            self.sqoopSize = 0
        if self.sqoopRows == None:
            self.sqoopRows = 0
        if self.sqoopMappers == None:
            self.sqoopMappers = 0

        # Check for errors in output
        sqoop_warning = False
        if " ERROR " in sqoop_output:
            for row in sqoop_output.split("\n"):
                if "Not authorized to access topics: " in row:
                    # This is not really an error, but we cant verify the table.
                    logging.warning("Problem sending data to Atlas")
                    sqoop_warning = True

                if "ERROR hook.AtlasHook" in row:
                    # This is not really an error, but we cant send to Atlas
                    logging.warning("Problem sending data to Atlas")
                    sqoop_warning = True

            if sqoop_warning == False:
                # We detected the string ERROR in the output but we havent found a vaild reason for it in the IF statements above.
                # We need to mark the sqoop command as error and exit the program
                logging.error(
                    "Unknown error in sqoop export. Please check the output for errors and try again"
                )
                self.remove_temporary_files()
                sys.exit(1)

        # No errors detected in the output and we are ready to store the result
        logging.info("Sqoop executed successfully")

        try:
            self.export_config.saveExportStatistics(
                self.sqoopStartUTS,
                sqoopSize=self.sqoopSize,
                sqoopRows=self.sqoopRows,
                sqoopMappers=self.sqoopMappers,
            )
        except:
            logging.exception("Fatal error when saving sqoop statistics")
            self.export_config.remove_temporary_files()
            sys.exit(1)

        logging.debug("Executing export_operations.runSqoopExport() - Finished")

    def parseSqoopOutput(self, row):
        # HDFS: Number of bytes written=17
        if "HDFS: Number of bytes read" in row:
            self.sqoopSize = int(row.split("=")[1])

        # 19/05/09 11:15:59 INFO mapreduce.ExportJobBase: Exported 8 records.
        if "mapreduce.ExportJobBase: Exported" in row:
            self.sqoopRows = int(row.split(" ")[5])

        # Launched map tasks=1
        if "Launched map tasks" in row:
            self.sqoopMappers = int(row.split("=")[1])

    def getHiveTableValidationData(self):
        if self.export_config.validateExport == False:
            return

        if self.export_config.validationMethod == constant.VALIDATION_METHOD_ROWCOUNT:
            self.getHiveTableRowCount()
        else:
            self.runCustomValidationQueryOnHiveTable()

    def getTargetTableValidationData(self):
        if self.export_config.validateExport == False:
            return

        if self.export_config.validationMethod == constant.VALIDATION_METHOD_ROWCOUNT:
            self.getTargetTableRowCount()
        else:
            self.export_config.runCustomValidationQueryOnJDBCTable()

    def validateExport(self):
        if self.export_config.validateExport == False:
            logging.info("Validation disabled for this export")
            return

        if self.export_config.validationMethod == constant.VALIDATION_METHOD_ROWCOUNT:
            self.validateRowCount()
        else:
            self.export_config.validateCustomQuery()

    def getHiveTableRowCount(self):
        try:
            where_statement = None

            hive_db = self.hiveDB
            hive_table = self.hiveTable

            if self.export_config.incr_validation_method == "incr":
                where_statement = self.export_config.getIncrWhereStatement(
                    excludeMinValue=False
                )
            else:
                # Needed to make sure that we are not validating against new rows that might have been saved in Hive
                if self.export_config.exportIsIncremental == True:
                    where_statement = self.export_config.getIncrWhereStatement(
                        excludeMinValue=True
                    )

            if self.export_config.sqlWhereAddition != None:
                if where_statement == None:
                    where_statement = self.export_config.sqlWhereAddition
                else:
                    where_statement += " and (%s)" % (
                        self.export_config.sqlWhereAddition
                    )

            hive_table_row_count = self.common_operations.getHiveTableRowCount(
                hive_db, hive_table, whereStatement=where_statement
            )
            self.export_config.saveHiveTableRowCount(hive_table_row_count)
        except:
            logging.exception("Fatal error when reading Hive table row count")
            self.export_config.remove_temporary_files()
            sys.exit(1)

    def runCustomValidationQueryOnHiveTable(self):
        logging.debug(
            "Executing export_operations.runCustomValidationQueryOnTargetTable()"
        )

        logging.info("Executing custom validation query on Hive table.")

        query = self.export_config.validationCustomQueryHiveSQL
        query = query.replace("${HIVE_DB}", self.export_config.hiveDB)
        query = query.replace("${HIVE_TABLE}", self.export_config.hiveTable)

        if self.export_config.sqlWhereAddition != None:
            if " where " in query:
                query += " and (%s)" % (self.export_config.sqlWhereAddition)
            else:
                query += " where %s" % (self.export_config.sqlWhereAddition)

        logging.debug("Validation Query on Hive table: %s" % (query))

        result_df = self.common_operations.executeHiveQuery(query)
        result_json = result_df.to_json(orient="values")
        self.export_config.validationCustomQueryHiveValue = result_json

        if len(self.export_config.validationCustomQueryHiveValue) > 1024:
            logging.warning(
                "'%s' is to large."
                % (self.export_config.validationCustomQueryHiveValue)
            )
            raise invalidConfiguration(
                "The size of the json document on the custom query exceeds 1024 bytes. Change the query to create a result with less than 512 bytes"
            )

        self.export_config.saveCustomSQLValidationHiveValue(
            jsonValue=result_json, printInfo=False
        )

        logging.debug("resultDF:")
        logging.debug(result_df)
        logging.debug("resultJSON: %s" % (result_json))

        logging.debug(
            "Executing export_operations.runCustomValidationQueryOnTargetTable() - Finished"
        )

    def isThereIncrDataToExport(self):
        try:
            return self.export_config.isThereIncrDataToExport()
        except:
            logging.exception("Fatal error when reading Hive table row count")
            self.export_config.remove_temporary_files()
            sys.exit(1)

    def getTargetTableRowCount(self):
        try:
            where_statement = None
            if self.export_config.incr_validation_method == "incr":
                where_statement = self.export_config.getIncrWhereStatement(
                    whereForTarget=True, excludeMinValue=False
                )
            self.export_config.getJDBCTableRowCount(whereStatement=where_statement)
        except:
            logging.exception("Fatal error when reading Target table row count")
            self.export_config.remove_temporary_files()
            sys.exit(1)

    def validateRowCount(self):
        try:
            validate_result = self.export_config.validateRowCount()
        except:
            logging.exception("Fatal error when validating exported rows")
            self.export_config.remove_temporary_files()
            sys.exit(1)

        if validate_result == False:
            self.export_config.remove_temporary_files()
            sys.exit(1)
