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

import re
import logging
import json
import math

# import urllib
import mysql.connector
from common.Singleton import Singleton
from common import constants as constant
from common.Exceptions import SQLerror, invalidConfiguration, validationError
from DBImportConfig import common_config
from DBImportConfig import sendStatistics
from DBImportConfig import import_stage as stage
from datetime import datetime
import pandas as pd
import numpy as np


class config(object, metaclass=Singleton):
    def __init__(self, Hive_DB=None, Hive_Table=None):
        logging.debug("Executing import_config.__init__()")
        self.Hive_DB = Hive_DB
        self.Hive_Table = Hive_Table
        self.mysql_conn = None
        self.mysql_cursor01 = None
        self.startDate = None
        self.common_config = None

        self.connection_alias = None
        self.source_schema = None
        self.source_table = None
        self.table_id = None
        self.import_type = None
        self.import_phase_type = None
        self.etl_phase_type = None
        self.import_type_description = None
        self.sqoop_mappers = None
        self.validate_import = None
        self.truncate_hive_table = None
        self.incr_mode = None
        self.incr_column = None
        self.sqoop_use_generated_sql = None
        self.incr_maxvalue = None
        self.validate_diff_allowed = None
        self.soft_delete_during_merge = None
        self.sqoop_last_size = None
        self.sqoop_last_rows = None
        self.sqoop_last_mappers = None
        self.sqoop_allow_text_splitter = None
        self.datalake_source = None
        self.datalake_source_table = None
        self.datalake_source_connection = None
        self.sqoop_last_execution = None
        self.sqoop_last_execution_timestamp = None
        self.hiveJavaHeap = None
        self.hiveSplitCount = None
        self.hdfsBaseDir = None
        self.sqoop_hdfs_location = None
        self.sqoop_hdfs_location_pkonly = None
        self.sqoop_hdfs_location_sparketl = None
        self.sqoop_incr_mode = None
        self.sqoop_incr_column = None
        self.sqoop_incr_lastvalue = None
        self.sqoop_incr_validation_method = None
        self.import_is_incremental = None
        self.create_table_with_acid = None
        self.create_table_with_acid_insert_only = None
        self.import_with_merge = None
        self.import_with_history_table = None
        self.full_table_compare = None
        self.create_datalake_import_column = None
        self.nomerge_ingestion_sql_addition = None
        self.sqoop_sql_where_addition = None
        self.sqoop_options = None
        self.table_comment = None
        self.sqlGeneratedHiveColumnDefinition = None
        self.sqlGeneratedSqoopQuery = None
        self.sqoop_query = None
        self.generatedSqoopOptions = None
        self.generatedPKcolumns = None
        self.sqoop_mapcolumnjava = []
        self.sqoopStartUTS = None
        self.sqoopIncrMaxvaluePending = None
        self.incr_validation_method = None
        self.splitByColumn = ""
        self.sqoopBoundaryQuery = ""
        self.pk_column_override = None
        self.pk_column_override_mergeonly = None
        self.validate_source = None
        self.copy_slave = None
        self.create_foreign_keys = None
        self.create_foreign_keys_table = None
        self.create_foreign_keys_connection = None
        self.importTool = None
        self.etlEngine = None
        self.spark_executor_memory = None
        self.sparkMaxExecutors = None
        self.split_by_column = None
        self.splitAddedToSqoopOptions = False
        self.custom_max_query = None
        self.mergeCompactionMethod = None
        self.printSQLWhereAddition = True
        self.invalidateImpalaMetadata = False

        self.validationMethod = None
        self.validationCustomQuerySourceSQL = None
        self.validationCustomQueryHiveSQL = None
        self.validationCustomQuerySourceValue = None
        self.validationCustomQueryHiveValue = None
        self.validationCustomQueryValidateImportTable = True

        self.importDatabaseOverride = None
        self.importTableOverride = None
        self.historyDatabaseOverride = None
        self.historyTableOverride = None

        self.importPhase = None
        self.importPhaseDescription = None
        self.copyPhase = None
        self.copyPhaseDescription = None
        self.etlPhase = None
        self.etlPhaseDescription = None

        self.phaseThree = None
        self.sqlSessions = None
        self.fullExecutedCommand = None
        self.mongoImport = None

        self.common_config = common_config.config(Hive_DB, Hive_Table)
        self.common_config.operationType = "import"
        self.sendStatistics = sendStatistics.sendStatistics()

        self.startDate = self.common_config.startDate
        self.mysql_conn = self.common_config.mysql_conn
        self.mysql_cursor01 = self.mysql_conn.cursor(buffered=True)
        self.mysql_cursor02 = self.mysql_conn.cursor(buffered=True)

        # Initialize the stage class that will handle all stage operations for us
        self.stage = stage.stage(self.mysql_conn, self.Hive_DB, self.Hive_Table)

        logging.debug("Executing import_config.__init__() - Finished")

    def reconnectConfigDatabase(self):
        # During long imports, depending on connection timout, the connection against the config database might be closed. This checks if it is closed and reconnect if needed
        if self.mysql_conn.is_connected() == False:
            self.mysql_conn.close()
            self.mysql_cursor01.close()
            self.mysql_cursor02.close()

            self.common_config.reconnectConfigDatabase()

            self.mysql_conn = self.common_config.mysql_conn
            self.mysql_cursor01 = self.mysql_conn.cursor(buffered=True)
            self.mysql_cursor02 = self.mysql_conn.cursor(buffered=True)

            self.stage.setMySQLConnection(self.mysql_conn)

    def setHiveTable(self, Hive_DB, Hive_Table):
        """Sets the parameters to work against a new Hive database and table"""
        self.Hive_DB = Hive_DB.lower()
        self.Hive_Table = Hive_Table.lower()

        self.common_config.setHiveTable(Hive_DB, Hive_Table)
        self.stage.setHiveTable(Hive_DB, Hive_Table)

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

    def remove_temporary_files(self):
        self.common_config.remove_temporary_files()

    def setStage(self, stage, force=False):
        self.stage.setStage(stage, force=force)

    def getStage(self):
        return self.stage.getStage()

    def getStageShortName(self, stage):
        return self.stage.getStageShortName(stage)

    def clearStage(self):
        self.stage.clearStage()

    def saveRetryAttempt(self, stage):
        self.stage.saveRetryAttempt(stage)

    def setStageOnlyInMemory(self):
        self.stage.setStageOnlyInMemory()

    def sendStartJSON(self):
        """Sends a start JSON document to REST and/or Kafka"""
        logging.debug("Executing import_config.sendStartJSON()")

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
        jsonData["type"] = "import"
        jsonData["status"] = "started"
        jsonData["database"] = self.Hive_DB
        jsonData["table"] = self.Hive_Table
        if self.import_with_history_table == True:
            jsonData["history_database"] = self.Hive_History_DB
            jsonData["history_table"] = self.Hive_History_Table
        else:
            jsonData["history_database"] = None
            jsonData["history_table"] = None
        jsonData["import_phase"] = self.importPhase
        jsonData["copy_phase"] = self.copyPhase
        jsonData["etl_phase"] = self.etlPhase
        jsonData["incremental"] = self.import_is_incremental
        jsonData["history"] = self.import_with_history_table
        jsonData["dbalias"] = self.connection_alias
        jsonData["source_database"] = self.common_config.jdbc_database
        jsonData["source_schema"] = self.source_schema
        jsonData["source_table"] = self.source_table

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

        logging.debug("Executing import_config.sendStartJSON() - Finished")

    def saveStageStatistics(self):
        self.stage.saveStageStatistics(
            hive_db=self.Hive_DB,
            hive_table=self.Hive_Table,
            import_phase=self.importPhase,
            copy_phase=self.copyPhase,
            etl_phase=self.etlPhase,
            incremental=self.import_is_incremental,
            dbalias=self.connection_alias,
            source_database=self.common_config.jdbc_database,
            source_schema=self.source_schema,
            source_table=self.source_table,
            size=self.sqoop_last_size,
            rows=self.sqoop_last_rows,
            sessions=self.sqoop_last_mappers,
        )

    def convertStageStatisticsToJSON(self):

        if self.import_with_history_table == True:
            HistoryDBforJSON = self.Hive_History_DB
            HistoryTableforJSON = self.Hive_History_Table
        else:
            HistoryDBforJSON = None
            HistoryTableforJSON = None

        self.stage.convertStageStatisticsToJSON(
            database=self.Hive_DB,
            table=self.Hive_Table,
            history_database=HistoryDBforJSON,
            history_table=HistoryTableforJSON,
            import_phase=self.importPhase,
            copy_phase=self.copyPhase,
            etl_phase=self.etlPhase,
            incremental=self.import_is_incremental,
            history=self.import_with_history_table,
            source_database=self.common_config.jdbc_database,
            source_schema=self.source_schema,
            source_table=self.source_table,
            size=self.sqoop_last_size,
            rows=self.sqoop_last_rows,
            sessions=self.sqoop_last_mappers,
        )

    # 			hive_db=self.Hive_DB,
    # 			hive_table=self.Hive_Table,

    def lookupConnectionAlias(self, connection_alias=None):

        if connection_alias == None:
            connection_alias = self.connection_alias

        # If this is called before getImportConfig is called, we dont know if it's a copy table or not. So we need to set this to false
        # This happens during table discovery for ./manage --addImportTable
        if self.copy_slave == None:
            self.copy_slave = False

        self.common_config.lookupConnectionAlias(
            connection_alias, copySlave=self.copy_slave
        )

        # Save if the source is a MongoDB database to a local parameter
        self.mongoImport = self.common_config.db_mongodb

    def checkTimeWindow(self):
        self.common_config.checkTimeWindow(self.connection_alias)

    def getJDBCTableDefinition(self):
        self.common_config.getJDBCTableDefinition(self.source_schema, self.source_table)

    def getImportConfig(self):
        logging.debug("Executing import_config.getImportConfig()")

        query = (
            "select "
            "    dbalias,"
            "    source_schema,"
            "    source_table,"
            "    table_id,"
            "    import_type,"
            "    mappers,"
            "    validate_import,"
            "    truncate_hive,"
            "    incr_mode,"
            "    incr_column,"
            "    sqoop_use_generated_sql,"
            "    incr_maxvalue,"
            "    validate_diff_allowed,"
            "    soft_delete_during_merge,"
            "    sqoop_last_size,"
            "    sqoop_last_rows,"
            "    sqoop_allow_text_splitter,"
            "    datalake_source,"
            "    sqoop_last_execution,"
            "    hive_merge_heap, "
            "    nomerge_ingestion_sql_addition, "
            "    sqoop_sql_where_addition, "
            "    sqoop_options, "
            "    generated_sqoop_options, "
            "    generated_sqoop_query, "
            "    generated_pk_columns, "
            "    incr_validation_method, "
            "    sqoop_last_mappers, "
            "    sqoop_query, "
            "    validate_source, "
            "    copy_slave, "
            "    import_phase_type, "
            "    etl_phase_type, "
            "    create_foreign_keys, "
            "    import_tool, "
            "    spark_executor_memory, "
            "    split_by_column, "
            "    custom_max_query, "
            "    validationMethod, "
            "    validationCustomQuerySourceSQL, "
            "    validationCustomQueryHiveSQL, "
            "    validationCustomQueryValidateImportTable, "
            "    validationCustomQuerySourceValue, "
            "    validationCustomQueryHiveValue, "
            "    mergeCompactionMethod, "
            "    hive_split_count, "
            "    etl_engine, "
            "    spark_executors, "
            "    last_update_from_source, "
            "    invalidate_impala, "
            "    import_database, "
            "    import_table, "
            "    history_database, "
            "    history_table "
            "from import_tables "
            "where "
            "    hive_db = %s"
            "    and hive_table = %s "
        )

        self.mysql_cursor01.execute(query, (self.Hive_DB, self.Hive_Table))
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement))
        if self.mysql_cursor01.rowcount != 1:
            raise invalidConfiguration(
                "The specified Hive Database and Table can't be found in the configuration database. Please check that you specified the correct database and table and that the configuration exists in the configuration database"
            )

        row = self.mysql_cursor01.fetchone()

        self.connection_alias = row[0]
        self.source_schema = row[1]
        self.source_table = row[2]
        self.table_id = row[3]
        self.import_type = row[4]
        self.sqoop_mappers = row[5]

        if row[6] == 0:
            self.validate_import = False
        else:
            self.validate_import = True

        if row[7] == 1:
            self.truncate_hive_table = True
        else:
            self.truncate_hive_table = False

        self.incr_mode = row[8]
        self.incr_column = row[9]

        if row[10] == 1:
            self.sqoop_use_generated_sql = True
        else:
            self.sqoop_use_generated_sql = False

        self.incr_maxvalue = row[11]
        self.validate_diff_allowed = row[12]

        if row[13] == 1:
            self.soft_delete_during_merge = True
        else:
            self.soft_delete_during_merge = False

        self.sqoop_last_size = row[14]
        self.sqoop_last_rows = row[15]

        if row[16] == 1:
            self.sqoop_allow_text_splitter = True
        else:
            self.sqoop_allow_text_splitter = False

        self.datalake_source_table = row[17]
        self.sqoop_last_execution = row[18]
        self.hiveJavaHeap = row[19]
        self.nomerge_ingestion_sql_addition = row[20]
        self.sqoop_sql_where_addition = row[21]
        self.sqoop_options = row[22]
        self.generatedSqoopOptions = row[23]
        self.sqlGeneratedSqoopQuery = row[24]
        self.generatedPKcolumns = row[25]
        self.incr_validation_method = row[26]
        self.sqoop_last_mappers = row[27]
        self.sqoop_query = row[28]
        self.validate_source = row[29]

        if row[30] == 1:
            self.copy_slave = True
        else:
            self.copy_slave = False

        self.import_phase_type = row[31]
        self.etl_phase_type = row[32]
        self.create_foreign_keys_table = row[33]
        self.importTool = row[34]
        self.spark_executor_memory = row[35]
        self.split_by_column = row[36]
        self.custom_max_query = row[37]
        self.common_config.custom_max_query = self.custom_max_query

        self.validationMethod = row[38]
        self.validationCustomQuerySourceSQL = row[39]
        self.validationCustomQueryHiveSQL = row[40]

        if row[41] == 1:
            self.validationCustomQueryValidateImportTable = True
        else:
            self.validationCustomQueryValidateImportTable = False

        self.validationCustomQuerySourceValue = row[42]
        self.validationCustomQueryHiveValue = row[43]
        self.mergeCompactionMethod = row[44]
        self.hiveSplitCount = row[45]
        self.etlEngine = row[46]
        self.sparkMaxExecutors = row[47]

        self.common_config.operationTimestamp = row[48]

        invalidateImpalaMetadataValue = row[49]

        self.importDatabaseOverride = row[50]
        self.importTableOverride = row[51]
        self.historyDatabaseOverride = row[52]
        self.historyTableOverride = row[53]

        if (
            self.validationMethod != constant.VALIDATION_METHOD_CUSTOMQUERY
            and self.validationMethod != constant.VALIDATION_METHOD_ROWCOUNT
        ):
            raise invalidConfiguration(
                "Only the values '%s' or '%s' is valid for column validationMethod in import_tables."
                % (
                    constant.VALIDATION_METHOD_ROWCOUNT,
                    constant.VALIDATION_METHOD_CUSTOMQUERY,
                )
            )

        if self.validationMethod == "customQuery":
            # Check settings to make sure all data is available before starting the import
            if (
                "${HIVE_DB}" not in self.validationCustomQueryHiveSQL
                or "${HIVE_TABLE}" not in self.validationCustomQueryHiveSQL
            ):
                raise invalidConfiguration(
                    "When using customQuery as the validation method, the Hive query must have the strings ${HIVE_DB} and ${HIVE_TABLE} instead of the actual database and table names. This is needed as validation happens on both import and target table."
                )

        if self.importTool != "sqoop" and self.importTool != "spark":
            raise invalidConfiguration(
                "Only the values 'sqoop' or 'spark' is valid for column import_tool in import_tables."
            )

        if self.validate_source != "query" and self.validate_source != "sqoop":
            raise invalidConfiguration(
                "Only the values 'query' or 'sqoop' is valid for column validate_source in import_tables."
            )

        if self.sqoop_query != None and self.sqoop_query.strip() == "":
            self.sqoop_query = None

        if self.sqoop_options == None:
            self.sqoop_options = ""  # This is needed as we check if this contains a --split-by and it needs to be string for that

        # If the self.sqoop_last_execution contains an unix time stamp, we convert it so it's usuable by sqoop
        if self.sqoop_last_execution != None:
            self.sqoop_last_execution_timestamp = datetime.utcfromtimestamp(
                self.sqoop_last_execution
            ).strftime("%Y-%m-%d %H:%M:%S.000")

        if self.validate_diff_allowed == -1:
            self.validate_diff_allowed = None

        if (
            self.incr_validation_method != "full"
            and self.incr_validation_method != "incr"
        ):
            raise invalidConfiguration(
                "Only the values 'full' or 'incr' is valid for column incr_validation_method in import_tables."
            )

        # Set sqoop NULL values
        if self.sqoop_last_size == None:
            self.sqoop_last_size = 0

        if self.sqoop_last_rows == None:
            self.sqoop_last_rows = 0

        self.hdfsBaseDir = self.common_config.getConfigValue(key="hdfs_basedir")

        # Set various sqoop variables
        # changing the HDFS path also requires you to change it in copy_operation.py under copyDataToDestinations()
        self.sqoop_hdfs_location = (
            (self.hdfsBaseDir + "/" + self.Hive_DB + "/" + self.Hive_Table + "/data")
            .replace("$", "")
            .replace(" ", "")
        )
        self.sqoop_hdfs_location_pkonly = (
            (
                self.hdfsBaseDir
                + "/"
                + self.Hive_DB
                + "/"
                + self.Hive_Table
                + "/data_PKonly"
            )
            .replace("$", "")
            .replace(" ", "")
        )
        self.sqoop_hdfs_location_sparkETL = (
            (
                self.hdfsBaseDir
                + "/"
                + self.Hive_DB
                + "/"
                + self.Hive_Table
                + "/data_sparketl"
            )
            .replace("$", "")
            .replace(" ", "")
        )
        self.sqoop_incr_mode = self.incr_mode
        self.sqoop_incr_column = self.incr_column
        self.sqoop_incr_lastvalue = self.incr_maxvalue
        self.sqoop_incr_validation_method = self.incr_validation_method

        if self.import_type != None and (
            self.import_phase_type != None or self.etl_phase_type != None
        ):
            raise invalidConfiguration(
                "It's not supported to enter a value in 'import_type' at the same time as 'import_phase_type' or 'etl_phase_type' is not empty"
            )

        if self.import_type == None and (
            self.import_phase_type == None or self.etl_phase_type == None
        ):
            raise invalidConfiguration(
                "You need to enter a value in both 'import_phase_type' and 'etl_phase_type'"
            )

        if self.import_phase_type != None and self.etl_phase_type != None:
            validCombination = False
            if (
                self.import_phase_type == "full"
                and self.etl_phase_type == "truncate_insert"
            ):
                validCombination = True

            if self.import_phase_type == "full" and self.etl_phase_type == "insert":
                validCombination = True

            if self.import_phase_type == "full" and self.etl_phase_type == "merge":
                validCombination = True

            if (
                self.import_phase_type == "full"
                and self.etl_phase_type == "merge_history_audit"
            ):
                validCombination = True

            if self.import_phase_type == "full" and self.etl_phase_type == "none":
                validCombination = True

            if self.import_phase_type == "full" and self.etl_phase_type == "external":
                validCombination = True

            if self.import_phase_type == "incr" and self.etl_phase_type == "insert":
                validCombination = True

            if self.import_phase_type == "incr" and self.etl_phase_type == "merge":
                validCombination = True

            if (
                self.import_phase_type == "incr"
                and self.etl_phase_type == "merge_history_audit"
            ):
                validCombination = True

            if (
                self.import_phase_type == "oracle_flashback"
                and self.etl_phase_type == "merge"
            ):
                validCombination = True

            if (
                self.import_phase_type == "oracle_flashback"
                and self.etl_phase_type == "merge_history_audit"
            ):
                validCombination = True

            if (
                self.import_phase_type == "mssql_change_tracking"
                and self.etl_phase_type == "merge"
            ):
                validCombination = True

            if (
                self.import_phase_type == "mssql_change_tracking"
                and self.etl_phase_type == "merge_history_audit"
            ):
                validCombination = True

            if validCombination == False:
                raise invalidConfiguration(
                    "The combination of '%s' and '%s' as import methods is not valid. Please check documentation for supported combinations"
                    % (self.import_phase_type, self.etl_phase_type)
                )

        # Set correct import_types. We do this because of old names of import_type
        if self.import_type != None:
            logging.warning(
                "Specifying the import type in the 'import_type' column is deprecated. Please use 'import_phase_type' and 'etl_phase_type'"
            )
            if self.import_type == "merge_acid":
                self.import_type = "incr_merge_direct"

            if self.import_type == "full_merge":
                self.import_type = "full_merge_direct"

            if self.import_type == "full_history":
                self.import_type = "full_merge_direct_history"

            if self.import_type == "full_append":
                self.import_type = "full_insert"
        else:
            self.import_type = ""

        # Get the connection details. This is needed in order to determine what kind of connection we talk about.
        self.lookupConnectionAlias()

        # Check each import type and set correct phase and description
        etlEngineSparkSupported = False

        if self.import_type in ("full", "full_direct") or (
            self.import_phase_type == "full"
            and self.etl_phase_type == "truncate_insert"
        ):
            if self.mongoImport == None or self.mongoImport == False:
                self.import_type_description = "Full import of table to Hive"
                self.importPhase = constant.IMPORT_PHASE_FULL
                self.copyPhase = constant.COPY_PHASE_NONE
                self.etlPhase = constant.ETL_PHASE_TRUNCATEINSERT
                self.importPhaseDescription = "Full"
                self.copyPhaseDescription = "No cluster copy"
                self.etlPhaseDescription = "Truncate and insert"
                etlEngineSparkSupported = True
            else:
                self.import_type_description = "Full import of Mongo collection to Hive"
                self.importPhase = constant.IMPORT_PHASE_MONGO_FULL
                self.copyPhase = constant.COPY_PHASE_NONE
                self.etlPhase = constant.ETL_PHASE_TRUNCATEINSERT
                self.importPhaseDescription = "Full"
                self.copyPhaseDescription = "No cluster copy"
                self.etlPhaseDescription = "Truncate and insert"

        if self.import_type == "full_insert" or (
            self.import_phase_type == "full" and self.etl_phase_type == "insert"
        ):
            self.import_type_description = (
                "Full import of table to Hive. Data is appended to target"
            )
            self.importPhase = constant.IMPORT_PHASE_FULL
            self.copyPhase = constant.COPY_PHASE_NONE
            self.etlPhase = constant.ETL_PHASE_INSERT
            self.importPhaseDescription = "Full"
            self.copyPhaseDescription = "No cluster copy"
            self.etlPhaseDescription = "Append"
            etlEngineSparkSupported = True

        if self.import_type == "full_merge_direct" or (
            self.import_phase_type == "full" and self.etl_phase_type == "merge"
        ):
            self.import_type_description = "Full import and merge of Hive table"
            self.importPhase = constant.IMPORT_PHASE_FULL
            self.copyPhase = constant.COPY_PHASE_NONE
            self.etlPhase = constant.ETL_PHASE_MERGEONLY
            self.importPhaseDescription = "Full"
            self.copyPhaseDescription = "No cluster copy"
            self.etlPhaseDescription = "Merge"
            etlEngineSparkSupported = True

        if self.import_phase_type == "full" and self.etl_phase_type == "none":
            self.import_type_description = "Full import with no ETL phace"
            self.importPhase = constant.IMPORT_PHASE_FULL
            self.copyPhase = constant.COPY_PHASE_NONE
            self.etlPhase = constant.ETL_PHASE_NONE
            self.importPhaseDescription = "Full"
            self.copyPhaseDescription = "No cluster copy"
            self.etlPhaseDescription = "No ETL phase"
            etlEngineSparkSupported = True

        if self.import_phase_type == "full" and self.etl_phase_type == "external":
            self.import_type_description = "Full import with no ETL phase. Will only create the target table as an external table"
            self.importPhase = constant.IMPORT_PHASE_FULL
            self.copyPhase = constant.COPY_PHASE_NONE
            self.etlPhase = constant.ETL_PHASE_EXTERNAL
            self.importPhaseDescription = "Full"
            self.copyPhaseDescription = "No cluster copy"
            self.etlPhaseDescription = "Only create an external table"

        if self.import_type == "full_merge_direct_history" or (
            self.import_phase_type == "full"
            and self.etl_phase_type == "merge_history_audit"
        ):
            self.import_type_description = (
                "Full import and merge of Hive table. Will also create a History table"
            )
            self.importPhase = constant.IMPORT_PHASE_FULL
            self.copyPhase = constant.COPY_PHASE_NONE
            self.etlPhase = constant.ETL_PHASE_MERGEHISTORYAUDIT
            self.importPhaseDescription = "Full"
            self.copyPhaseDescription = "No cluster copy"
            self.etlPhaseDescription = "Merge History Audit"
            etlEngineSparkSupported = True

        if self.import_type == "incr_merge_direct" or (
            self.import_phase_type == "incr" and self.etl_phase_type == "merge"
        ):
            self.import_type_description = "Incremental & merge import of Hive table"
            self.importPhase = constant.IMPORT_PHASE_INCR
            self.copyPhase = constant.COPY_PHASE_NONE
            self.etlPhase = constant.ETL_PHASE_MERGEONLY
            self.importPhaseDescription = "Incremental"
            self.copyPhaseDescription = "No cluster copy"
            self.etlPhaseDescription = "Merge"
            etlEngineSparkSupported = True

        if self.import_type == "incr_merge_direct_history" or (
            self.import_phase_type == "incr"
            and self.etl_phase_type == "merge_history_audit"
        ):
            self.import_type_description = "Incremental & merge import of Hive table. Will also create a History table"
            self.importPhase = constant.IMPORT_PHASE_INCR
            self.copyPhase = constant.COPY_PHASE_NONE
            self.etlPhase = constant.ETL_PHASE_MERGEHISTORYAUDIT
            self.importPhaseDescription = "Incremental"
            self.copyPhaseDescription = "No cluster copy"
            self.etlPhaseDescription = "Merge History Audit"
            etlEngineSparkSupported = True

        if self.import_type == "incr" or (
            self.import_phase_type == "incr" and self.etl_phase_type == "insert"
        ):
            self.import_type_description = "Incremental import of Hive table"
            self.importPhase = constant.IMPORT_PHASE_INCR
            self.copyPhase = constant.COPY_PHASE_NONE
            self.etlPhase = constant.ETL_PHASE_INSERT
            self.importPhaseDescription = "Incremental"
            self.copyPhaseDescription = "No cluster copy"
            self.etlPhaseDescription = "Insert"
            etlEngineSparkSupported = True

        if self.import_type == "oracle_flashback_merge" or (
            self.import_phase_type == "oracle_flashback"
            and self.etl_phase_type == "merge"
        ):
            self.import_type_description = "Import with the help of Oracle Flashback"
            self.importPhase = constant.IMPORT_PHASE_ORACLE_FLASHBACK
            self.copyPhase = constant.COPY_PHASE_NONE
            self.etlPhase = constant.ETL_PHASE_MERGEONLY
            self.importPhaseDescription = "Oracle Flashback"
            self.copyPhaseDescription = "No cluster copy"
            self.etlPhaseDescription = "Merge"

            if self.soft_delete_during_merge == True:
                raise invalidConfiguration(
                    "Oracle Flashback imports doesnt support 'Soft delete during Merge'. Please check configuration"
                )

            if self.importTool != "sqoop":
                raise invalidConfiguration(
                    "Oracle Flashback only supports imports with sqoop in this version"
                )

        if (
            self.import_phase_type == "oracle_flashback"
            and self.etl_phase_type == "merge_history_audit"
        ):
            self.import_type_description = "Import with the help of Oracle Flashback. Will also create a History table"
            self.importPhase = constant.IMPORT_PHASE_ORACLE_FLASHBACK
            self.copyPhase = constant.COPY_PHASE_NONE
            self.etlPhase = constant.ETL_PHASE_MERGEHISTORYAUDIT
            self.importPhaseDescription = "Oracle Flashback"
            self.copyPhaseDescription = "No cluster copy"
            self.etlPhaseDescription = "Merge History Audit"

            if self.soft_delete_during_merge == True:
                raise invalidConfiguration(
                    "Oracle Flashback imports doesnt support 'Soft delete during Merge'. Please check configuration"
                )

            if self.importTool != "sqoop":
                raise invalidConfiguration(
                    "Oracle Flashback only supports imports with sqoop in this version"
                )

        if (
            self.import_phase_type == "mssql_change_tracking"
            and self.etl_phase_type == "merge"
        ):
            self.import_type_description = (
                "Import with the help of Microsoft SQL Change Tracking"
            )
            self.importPhase = constant.IMPORT_PHASE_MSSQL_CHANGE_TRACKING
            self.copyPhase = constant.COPY_PHASE_NONE
            self.etlPhase = constant.ETL_PHASE_MERGEONLY
            self.importPhaseDescription = "Microsoft SQL Change Tracking"
            self.copyPhaseDescription = "No cluster copy"
            self.etlPhaseDescription = "Merge"

        if (
            self.import_phase_type == "mssql_change_tracking"
            and self.etl_phase_type == "merge_history_audit"
        ):
            self.import_type_description = "Import with the help of Microsoft SQL Change Tracking. Will also create a History table"
            self.importPhase = constant.IMPORT_PHASE_MSSQL_CHANGE_TRACKING
            self.copyPhase = constant.COPY_PHASE_NONE
            self.etlPhase = constant.ETL_PHASE_MERGEHISTORYAUDIT
            self.importPhaseDescription = "Microsoft SQL Change Tracking"
            self.copyPhaseDescription = "No cluster copy"
            self.etlPhaseDescription = "Merge History Audit"

        # If this is a slave table, we will disable the import phase as thats already done on the Master instance
        if self.copy_slave == True:
            self.importPhaseDescription = "No Import phase (slave table)"

        # Check ETL engine
        if self.importTool == "sqoop" and self.etlEngine != constant.ETL_ENGINE_HIVE:
            logging.warning(
                "Sqoop does not support spark as the ETL engine. Will use Hive as ETL engine instead."
            )
            self.etlEngine = constant.ETL_ENGINE_HIVE

        # Check Mongo ETL engine
        if self.mongoImport == True and self.etlEngine == constant.ETL_ENGINE_SPARK:
            raise invalidConfiguration(
                "Mongo Import only supports Hive as the ETL engine"
            )

        # Check SQL Anywhere engine
        if self.common_config.db_sqlanywhere == True and self.importTool != "spark":
            raise invalidConfiguration(
                "SQL Anywhere only supports Spark as the import tool"
            )

        # Check if import type is supported with Spark as the ETL engine
        if (
            self.etlEngine == constant.ETL_ENGINE_SPARK
            and etlEngineSparkSupported == False
        ):
            raise invalidConfiguration(
                "This import type does not support Spark as the ETL engine. Please change to Hive"
            )

        # Check validation method for Spark ETL engine
        if (
            self.etlEngine == constant.ETL_ENGINE_SPARK
            and self.validationMethod == constant.VALIDATION_METHOD_CUSTOMQUERY
        ):
            raise invalidConfiguration(
                "Spark ETL engine only supports row-count validation method. Please change and rerun the import"
            )

        # Check Mongo validation methods
        if (
            self.mongoImport == True
            and self.validationMethod == constant.VALIDATION_METHOD_CUSTOMQUERY
        ):
            raise invalidConfiguration(
                "Mongo Import only supports %s as a validation method"
                % (constant.VALIDATION_METHOD_ROWCOUNT)
            )

        # Check to see that we have a valid import type
        # This will only happen if the old 'import_type' column is used. For the import_phase_type we already check if we have a valid combo
        if self.importPhase == None or self.etlPhase == None:
            raise invalidConfiguration(
                "Import type '%s' is not a valid type. Please check configuration"
                % (self.import_type)
            )

        # Check for MSSQL Change Tracking
        if self.importPhase == constant.IMPORT_PHASE_MSSQL_CHANGE_TRACKING:
            if self.soft_delete_during_merge == True:
                raise invalidConfiguration(
                    "Microsoft SQL Change Tracking imports doesnt support 'Soft delete during Merge'. Please check configuration"
                )

            if self.importTool != "spark":
                raise invalidConfiguration(
                    "Microsoft SQL Change Tracking imports only support spark as import tool. Please check configuration"
                )

            if self.incr_validation_method != "full":
                raise invalidConfiguration(
                    "Microsoft SQL Change Tracking imports only support 'full' validation. Please check configuration"
                )

        # Determine if it's a Mongo import or not
        if (
            self.importPhase == constant.IMPORT_PHASE_MONGO_FULL
            and self.importTool != "spark"
        ):
            raise invalidConfiguration("Import of MongoDB is only supported by spark")

        # Determine if it's an incremental import based on the importPhase
        if self.importPhase in (
            constant.IMPORT_PHASE_INCR,
            constant.IMPORT_PHASE_ORACLE_FLASHBACK,
            constant.IMPORT_PHASE_MSSQL_CHANGE_TRACKING,
        ):
            self.import_is_incremental = True
            if self.importPhase == constant.IMPORT_PHASE_INCR:
                if self.sqoop_incr_column == None:
                    raise invalidConfiguration(
                        "An incremental import requires a valid column name specified in 'incr_column'. Please check configuration"
                    )
                if self.incr_mode != "append" and self.incr_mode != "lastmodified":
                    raise invalidConfiguration(
                        "Only the values 'append' or 'lastmodified' is valid for column incr_mode in import_tables."
                    )
        else:
            self.import_is_incremental = False

        # Determine if it's an merge and need ACID tables
        if self.etlPhase in (
            constant.ETL_PHASE_MERGEHISTORYAUDIT,
            constant.ETL_PHASE_MERGEONLY,
            constant.IMPORT_PHASE_ORACLE_FLASHBACK,
            constant.IMPORT_PHASE_MSSQL_CHANGE_TRACKING,
        ):
            self.create_table_with_acid = True
            self.create_table_with_acid_insert_only = False
            self.import_with_merge = True
        else:
            if self.common_config.getConfigValue(key="hive_insert_only_tables") == True:
                self.create_table_with_acid = True
                self.create_table_with_acid_insert_only = True
            else:
                self.create_table_with_acid = False
                self.create_table_with_acid_insert_only = False
            self.import_with_merge = False

        # Determine if a history table should be created
        if self.etlPhase in (constant.ETL_PHASE_MERGEHISTORYAUDIT):
            self.import_with_history_table = True
        else:
            self.import_with_history_table = False

        # Determine if we are doing a full table compare
        if self.etlPhase in (
            constant.ETL_PHASE_MERGEHISTORYAUDIT,
            constant.ETL_PHASE_MERGEONLY,
        ):
            self.full_table_compare = True
        else:
            self.full_table_compare = False

        # Set the number of Spark Executors
        if self.sparkMaxExecutors == None or self.sparkMaxExecutors == 0:
            self.sparkMaxExecutors = sqlSessionsMaxFromConfig = int(
                self.common_config.getConfigValue(key="spark_max_executors")
            )

        # Determine if we are going to invalidate the metadata in Impala or not
        if invalidateImpalaMetadataValue == 0:
            self.invalidateImpalaMetadata = False
        elif invalidateImpalaMetadataValue == 1:
            self.invalidateImpalaMetadata = True
        else:
            self.invalidateImpalaMetadata = self.common_config.getConfigValue(
                key="impala_invalidate_metadata"
            )

        # Fetch data from jdbc_connection table
        query = "select create_datalake_import, datalake_source, create_foreign_keys from jdbc_connections where dbalias = %s "
        self.mysql_cursor01.execute(query, (self.connection_alias,))
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement))

        if self.mysql_cursor01.rowcount != 1:
            raise invalidConfiguration(
                "The configured connection alias for this table cant be found in the 'jdbc_connections' table"
            )

        row = self.mysql_cursor01.fetchone()

        if row[0] == 1:
            self.create_datalake_import_column = True
        else:
            self.create_datalake_import_column = False

        if (
            self.importPhase == constant.IMPORT_PHASE_FULL
            and self.etlPhase == constant.ETL_PHASE_INSERT
        ):
            if self.create_datalake_import_column == False:
                raise invalidConfiguration(
                    "Full Insert imports requires 'create_datalake_import' to be set in jdbc_connections table"
                )

        self.datalake_source_connection = row[1]

        # Set self.datalake_source based on values from either self.datalake_source_table or self.datalake_source_connection
        if (
            self.datalake_source_table != None
            and self.datalake_source_table.strip() != ""
        ):
            self.datalake_source = self.datalake_source_table
        elif (
            self.datalake_source_connection != None
            and self.datalake_source_connection.strip() != ""
        ):
            self.datalake_source = self.datalake_source_connection

        self.create_foreign_keys_connection = row[2]

        # Set self.create_foreign_keys based on values from either self.create_foreign_keys_table or self.create_foreign_keys_connection
        if self.create_foreign_keys_table == 0 or self.create_foreign_keys_table == 1:
            self.create_foreign_keys = self.create_foreign_keys_table
        elif (
            self.create_foreign_keys_connection == 0
            or self.create_foreign_keys_connection == 1
        ):
            self.create_foreign_keys = self.create_foreign_keys_connection
        else:
            self.create_foreign_keys = 1

        importWorkDBconfig = self.common_config.getConfigValue(
            key="import_work_database"
        )
        importWorkTableConfig = self.common_config.getConfigValue(
            key="import_work_table"
        )
        importStagingDBconfig = self.common_config.getConfigValue(
            key="import_staging_database"
        )
        importStagingTableConfig = self.common_config.getConfigValue(
            key="import_staging_table"
        )
        importHistoryDBconfig = self.common_config.getConfigValue(
            key="import_history_database"
        )
        importHistoryTableConfig = self.common_config.getConfigValue(
            key="import_history_table"
        )

        # Set the name of the history tables, temporary tables and such
        if importWorkDBconfig.startswith("r/"):
            importWorkDB = re.sub(
                importWorkDBconfig.split("/")[1],
                importWorkDBconfig.split("/")[2],
                self.Hive_DB,
            )
        else:
            importWorkDB = importWorkDBconfig.replace("{DATABASE}", self.Hive_DB)

        if importWorkTableConfig.startswith("r/"):
            importWorkTable = re.sub(
                importWorkTableConfig.split("/")[1],
                importWorkTableConfig.split("/")[2],
                self.Hive_DB,
            )
        else:
            importWorkTable = importWorkTableConfig.replace(
                "{DATABASE}", self.Hive_DB
            ).replace("{TABLE}", self.Hive_Table)

        if importStagingDBconfig.startswith("r/"):
            importStagingDB = re.sub(
                importStagingDBconfig.split("/")[1],
                importStagingDBconfig.split("/")[2],
                self.Hive_DB,
            )
        else:
            importStagingDB = importStagingDBconfig.replace("{DATABASE}", self.Hive_DB)

        if importStagingTableConfig.startswith("r/"):
            importStagingTable = re.sub(
                importStagingTableConfig.split("/")[1],
                importStagingTableConfig.split("/")[2],
                self.Hive_DB,
            )
        else:
            importStagingTable = importStagingTableConfig.replace(
                "{DATABASE}", self.Hive_DB
            ).replace("{TABLE}", self.Hive_Table)

        if importHistoryDBconfig.startswith("r/"):
            importHistoryDB = re.sub(
                importHistoryDBconfig.split("/")[1],
                importHistoryDBconfig.split("/")[2],
                self.Hive_DB,
            )
        else:
            importHistoryDB = importHistoryDBconfig.replace("{DATABASE}", self.Hive_DB)

        if importHistoryTableConfig.startswith("r/"):
            importHistoryTable = re.sub(
                importHistoryTableConfig.split("/")[1],
                importHistoryTableConfig.split("/")[2],
                self.Hive_DB,
            )
        else:
            importHistoryTable = importHistoryTableConfig.replace(
                "{DATABASE}", self.Hive_DB
            ).replace("{TABLE}", self.Hive_Table)

        self.Hive_Import_DB = importStagingDB
        self.Hive_Import_Table = importStagingTable
        self.Hive_Import_View = importStagingTable + "_view"
        self.Hive_History_DB = importHistoryDB
        self.Hive_History_Table = importHistoryTable
        self.Hive_HistoryTemp_DB = importWorkDB
        self.Hive_HistoryTemp_Table = importWorkTable + "__temporary"
        self.Hive_Import_PKonly_DB = importWorkDB
        self.Hive_Import_PKonly_Table = importWorkTable + "__pkonly__staging"
        self.Hive_Delete_DB = importWorkDB
        self.Hive_Delete_Table = importWorkTable + "__pkonly__deleted"

        if self.etlPhase == constant.ETL_PHASE_EXTERNAL:
            self.Hive_Import_DB = self.Hive_DB
            self.Hive_Import_Table = self.Hive_Table

        # If the database or table for import or history have been overridden by setting the options in the import_table
        if self.importDatabaseOverride != None:
            self.Hive_Import_DB = self.importDatabaseOverride

        if self.importTableOverride != None:
            self.Hive_Import_Table = self.importTableOverride
            self.Hive_Import_View = self.importTableOverride + "_view"

        if self.historyDatabaseOverride != None:
            self.Hive_History_DB = self.historyDatabaseOverride

        if self.historyTableOverride != None:
            self.Hive_History_Table = self.historyTableOverride

        self.Hive_ColumnName_Import = self.common_config.getConfigValue(
            key="import_columnname_import"
        )
        self.Hive_ColumnName_Insert = self.common_config.getConfigValue(
            key="import_columnname_insert"
        )
        self.Hive_ColumnName_Update = self.common_config.getConfigValue(
            key="import_columnname_update"
        )
        self.Hive_ColumnName_Delete = self.common_config.getConfigValue(
            key="import_columnname_delete"
        )
        self.Hive_ColumnName_IUD = self.common_config.getConfigValue(
            key="import_columnname_iud"
        )
        self.Hive_ColumnName_HistoryTimestamp = self.common_config.getConfigValue(
            key="import_columnname_histtime"
        )
        self.Hive_ColumnName_Source = self.common_config.getConfigValue(
            key="import_columnname_source"
        )

        logging.debug("Settings from import_config.getImportConfig()")
        logging.debug("    connection_alias = %s" % (self.connection_alias))
        logging.debug("    source_schema = %s" % (self.source_schema))
        logging.debug("    source_table = %s" % (self.source_table))
        logging.debug("    table_id = %s" % (self.table_id))
        logging.debug("    import_type = %s" % (self.import_type))
        logging.debug("    validate_import = %s" % (self.validate_import))
        logging.debug("    truncate_hive_table = %s" % (self.truncate_hive_table))
        logging.debug("    incr_mode = %s" % (self.incr_mode))
        logging.debug("    incr_column = %s" % (self.incr_column))
        logging.debug("    incr_maxvalue = %s" % (self.incr_maxvalue))
        logging.debug("    validate_diff_allowed = %s" % (self.validate_diff_allowed))
        logging.debug(
            "    soft_delete_during_merge = %s" % (self.soft_delete_during_merge)
        )
        logging.debug("    datalake_source = %s" % (self.datalake_source))
        logging.debug("    datalake_source_table = %s" % (self.datalake_source_table))
        logging.debug(
            "    datalake_source_connection = %s" % (self.datalake_source_connection)
        )
        logging.debug("    hiveJavaHeap = %s" % (self.hiveJavaHeap))
        logging.debug("    hiveSplitCount = %s" % (self.hiveSplitCount))
        logging.debug(
            "    sqoop_use_generated_sql = %s" % (self.sqoop_use_generated_sql)
        )
        logging.debug("    sqoop_mappers = %s" % (self.sqoop_mappers))
        logging.debug("    sqoop_last_size = %s" % (self.sqoop_last_size))
        logging.debug("    sqoop_last_rows = %s" % (self.sqoop_last_rows))
        logging.debug(
            "    sqoop_allow_text_splitter = %s" % (self.sqoop_allow_text_splitter)
        )
        logging.debug("    sqoop_last_execution = %s" % (self.sqoop_last_execution))
        logging.debug(
            "    sqoop_last_execution_timestamp = %s"
            % (self.sqoop_last_execution_timestamp)
        )
        logging.debug("    sqoop_hdfs_location = %s" % (self.sqoop_hdfs_location))
        logging.debug(
            "    sqoop_hdfs_location_pkonly = %s" % (self.sqoop_hdfs_location_pkonly)
        )
        logging.debug(
            "    sqoop_hdfs_location_sparkETL = %s"
            % (self.sqoop_hdfs_location_sparkETL)
        )
        logging.debug("    sqoop_incr_mode = %s" % (self.sqoop_incr_mode))
        logging.debug("    sqoop_incr_column = %s" % (self.sqoop_incr_column))
        logging.debug("    sqoop_incr_lastvalue = %s" % (self.sqoop_incr_lastvalue))
        logging.debug("    sqoop_options = %s" % (self.sqoop_options))
        logging.debug("    import_is_incremental = %s" % (self.import_is_incremental))
        logging.debug("    create_table_with_acid = %s" % (self.create_table_with_acid))
        logging.debug("    import_with_merge = %s" % (self.import_with_merge))
        logging.debug(
            "    import_with_history_table = %s" % (self.import_with_history_table)
        )
        logging.debug("    full_table_compare = %s" % (self.full_table_compare))
        logging.debug(
            "    create_datalake_import_column = %s"
            % (self.create_datalake_import_column)
        )
        logging.debug(
            "    nomerge_ingestion_sql_addition = %s"
            % (self.nomerge_ingestion_sql_addition)
        )
        logging.debug(
            "    sqoop_sql_where_addition = %s" % (self.sqoop_sql_where_addition)
        )
        logging.debug("    Hive_Import_DB = %s" % (self.Hive_Import_DB))
        logging.debug("    Hive_Import_Table = %s" % (self.Hive_Import_Table))
        logging.debug("    Hive_Import_View = %s" % (self.Hive_Import_View))
        logging.debug("    Hive_History_DB = %s" % (self.Hive_History_DB))
        logging.debug("    Hive_History_Table = %s" % (self.Hive_History_Table))
        logging.debug("    Hive_HistoryTemp_DB = %s" % (self.Hive_HistoryTemp_DB))
        logging.debug("    Hive_HistoryTemp_Table = %s" % (self.Hive_HistoryTemp_Table))
        logging.debug("    Hive_Import_PKonly_DB = %s" % (self.Hive_Import_PKonly_DB))
        logging.debug(
            "    Hive_Import_PKonly_Table = %s" % (self.Hive_Import_PKonly_Table)
        )
        logging.debug("    Hive_Delete_DB = %s" % (self.Hive_Delete_DB))
        logging.debug("    Hive_Delete_Table = %s" % (self.Hive_Delete_Table))
        logging.debug("    Hive_ColumnName_Import = %s" % (self.Hive_ColumnName_Import))
        logging.debug("    Hive_ColumnName_Insert = %s" % (self.Hive_ColumnName_Insert))
        logging.debug("    Hive_ColumnName_Update = %s" % (self.Hive_ColumnName_Update))
        logging.debug("    Hive_ColumnName_Delete = %s" % (self.Hive_ColumnName_Delete))
        logging.debug("    Hive_ColumnName_IUD = %s" % (self.Hive_ColumnName_IUD))
        logging.debug(
            "    Hive_ColumnName_HistoryTimestamp = %s"
            % (self.Hive_ColumnName_HistoryTimestamp)
        )
        logging.debug("    Hive_ColumnName_Source = %s" % (self.Hive_ColumnName_Source))
        logging.debug("Executing import_config.getImportConfig() - Finished")

    def updateLastUpdateFromSource(self):
        # This function will update the import_tables.last_update_from_source to the startDate from the common class. This will later
        # be used to determine if all the columns exists in the source system as we set the same datatime on all columns
        # that we read from the source system
        logging.debug("")
        logging.debug("Executing import_config.updateLastUpdateFromSource()")

        # Update the import_tables.last_update_from_source with the current date
        query = "update import_tables set last_update_from_source = %s where hive_db = %s and hive_table = %s "
        logging.debug("")
        logging.debug("Updating the last_update_from_source in import_tables")
        self.mysql_cursor01.execute(
            query, (self.startDate, self.Hive_DB, self.Hive_Table)
        )
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement))
        self.mysql_conn.commit()

        # We re-read the date just to make sure we have the exakt same in common_config. This will later be used to group yarn applications per import
        query = "select last_update_from_source from import_tables where hive_db = %s and hive_table = %s "
        self.mysql_cursor01.execute(query, (self.Hive_DB, self.Hive_Table))
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement))

        row = self.mysql_cursor01.fetchone()
        self.common_config.operationTimestamp = row[0]

        logging.debug("Executing import_config.updateLastUpdateFromSource() - Finished")

    def saveIncrMinValue(self):
        """Save the min value in the pendings column"""
        logging.debug("Executing import_config.savePendingIncrValues()")

        query = "update import_tables set "
        query += "	incr_minvalue_pending = incr_maxvalue "
        query += "where table_id = %s"

        self.mysql_cursor01.execute(query, (self.table_id,))
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement))
        self.mysql_conn.commit()

        logging.debug("Executing import_config.savePendingIncrValues() - Finished")

    def saveIncrPendingValues(self):
        """After the incremental import is completed, we save the pending values so the next import starts from the correct spot"""
        logging.debug("Executing import_config.savePendingIncrValues()")

        query = "update import_tables set "
        query += "	incr_minvalue = incr_minvalue_pending, "
        query += "	incr_maxvalue = incr_maxvalue_pending "
        query += "where table_id = %s"

        self.mysql_cursor01.execute(query, (self.table_id,))
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement))
        self.mysql_conn.commit()

        query = "update import_tables set "
        query += "	incr_minvalue_pending = NULL, "
        query += "	incr_maxvalue_pending = NULL "
        query += "where table_id = %s"

        self.mysql_cursor01.execute(query, (self.table_id,))
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement))
        self.mysql_conn.commit()

        logging.debug("Executing import_config.savePendingIncrValues() - Finished")

    def removeFKforTable(self):
        # This function will remove all ForeignKey definitions for the current table.
        logging.debug("")
        logging.debug("Executing import_config.removeFKforTable()")

        query = "delete from import_foreign_keys where table_id = %s"
        logging.debug("")
        logging.debug("Deleting FK's from import_foreign_keys")
        self.mysql_cursor01.execute(query, (self.table_id,))
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement))
        self.mysql_conn.commit()

        logging.debug("Executing import_config.removeFKforTable() - Finished")

    def stripUnwantedCharComment(self, work_string):
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

    def stripUnwantedCharColumnName(self, work_string):
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

    def getColumnForceString(self, column_name):
        logging.debug("Executing import_config.getColumnForceString()")
        # Used to determine if char/varchar fields should be forced to string in Hive

        query = "select "
        query += "	jc.force_string, "
        query += "	t.force_string, "
        query += "	c.force_string "
        query += "from import_tables t "
        query += "left join jdbc_connections jc "
        query += "	on t.dbalias = jc.dbalias "
        query += "left join import_columns c "
        query += "	on c.table_id = t.table_id "
        query += "	and c.column_name = %s "
        query += "where "
        query += "	t.hive_db = %s "
        query += "	and t.hive_table = %s "

        self.mysql_cursor01.execute(
            query, (column_name.lower(), self.Hive_DB, self.Hive_Table)
        )
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement))

        if self.mysql_cursor01.rowcount != 1:
            logging.error(
                "Error: Number of rows returned from query on 'import_tables' was not one."
            )
            logging.error("Rows returned: %d" % (self.mysql_cursor01.rowcount))
            logging.error(
                "SQL Statement that generated the error: %s"
                % (self.mysql_cursor01.statement)
            )
            raise Exception

        row = self.mysql_cursor01.fetchone()
        force_string_connection = row[0]
        force_string_table = row[1]
        force_string_column = row[2]

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

        logging.debug("force_string_connection: %s" % (force_string_connection))
        logging.debug("force_string_table: %s" % (force_string_table))
        logging.debug("force_string_column: %s" % (force_string_column))
        logging.debug("forceString: %s" % (forceString))
        logging.debug("Executing import_config.getColumnForceString() - Finished")
        return forceString

    def convertHiveColumnNameReservedWords(self, columnName):
        if columnName.lower() in ("synchronized", "date", "interval", "int"):
            columnName = "%s_hive" % (columnName)

        return columnName

    def isAnyColumnAnonymized(
        self,
    ):
        columnDF = self.getAnonymizationFunction()

        if columnDF.empty == True:
            return False
        else:
            return True

    def getAnonymizationFunction(
        self,
    ):
        """Reads all columns and returns a dict with column name and anonymization function"""
        logging.debug("Executing import_config.getAnonymizationFunction()")

        query = "select "
        query += "	source_column_name, "
        query += "	anonymization_function "
        query += "from import_columns where table_id = %s and anonymization_function != 'None'"
        self.mysql_cursor01.execute(query, (self.table_id,))
        logging.debug("SQL Statement executed: \n%s" % (self.mysql_cursor01.statement))

        result_df = pd.DataFrame()
        result_df = pd.DataFrame(self.mysql_cursor01.fetchall())
        if result_df.empty == False:
            # Set the correct column namnes in the DataFrame
            result_df_columns = []
            for columns in self.mysql_cursor01.description:
                result_df_columns.append(
                    columns[0]
                )  # Name of the column is in the first position
            result_df.columns = result_df_columns

        logging.debug("Executing import_config.getAnonymizationFunction() - Finished")
        return result_df

    def saveColumnData(
        self,
    ):
        """This is one of the main functions when it comes to source system schemas. This will parse the output from the Python Schema Program
        and insert/update the data in the import_columns table. This also takes care of column type conversions if it's needed"""
        logging.debug("")
        logging.debug("Executing import_config.saveColumnData()")
        logging.info("Saving column data to MySQL table - import_columns")

        # IS_NULLABLE
        # SCHEMA_NAME
        # SOURCE_COLUMN_COMMENT
        # SOURCE_COLUMN_NAME
        # SOURCE_COLUMN_TYPE
        # TABLE_COMMENT
        # TABLE_NAME
        self.sqlGeneratedHiveColumnDefinition = ""
        self.sqlGeneratedSqoopQuery = ""
        self.sqoop_mapcolumnjava = []
        columnOrder = 0
        self.sqoop_use_generated_sql = False
        columnNameReserved = False

        for index, row in self.common_config.source_columns_df.iterrows():
            column_name = self.stripUnwantedCharColumnName(row["SOURCE_COLUMN_NAME"])
            if self.mongoImport == False:
                column_type = row["SOURCE_COLUMN_TYPE"].lower()
            else:
                column_type = row["SOURCE_COLUMN_TYPE"]
            source_column_type = column_type
            source_column_name = row["SOURCE_COLUMN_NAME"]
            source_column_comment = self.stripUnwantedCharComment(
                row["SOURCE_COLUMN_COMMENT"]
            )
            self.table_comment = self.stripUnwantedCharComment(row["TABLE_COMMENT"])

            # Get the settings for this specific column
            query = "select "
            query += "	column_id, "
            query += "	include_in_import, "
            query += "	column_name_override, "
            query += "	column_type_override, "
            query += "	sqoop_column_type_override, "
            query += "	anonymization_function "
            query += (
                "from import_columns where table_id = %s and source_column_name = %s "
            )
            self.mysql_cursor01.execute(query, (self.table_id, source_column_name))
            logging.debug(
                "SQL Statement executed: \n%s" % (self.mysql_cursor01.statement)
            )

            includeColumnInImport = True
            columnID = None
            columnTypeOverride = None
            sqoopColumnTypeOverride = None
            anonymizationFunction = "None"

            columnRow = self.mysql_cursor01.fetchone()
            if columnRow != None:
                columnID = columnRow[0]
                if columnRow[1] == 0:
                    includeColumnInImport = False
                if columnRow[2] != None and columnRow[2].strip() != "":
                    column_name = columnRow[2].strip().lower()
                if columnRow[3] != None and columnRow[3].strip() != "":
                    columnTypeOverride = columnRow[3].strip().lower()
                if columnRow[4] != None and columnRow[4].strip() != "":
                    sqoopColumnTypeOverride = columnRow[4].strip().lower().capitalize()

                anonymizationFunction = columnRow[5]

            # Handle reserved column names in Hive
            column_name = self.convertHiveColumnNameReservedWords(column_name)

            # TODO: Get the maximum column comment size from Hive Metastore and use that instead
            if source_column_comment != None and len(source_column_comment) > 256:
                source_column_comment = source_column_comment[:256]

            columnOrder += 1

            # sqoop_column_type is just an information that will be stored in import_columns.
            # But it's also used to create the --map-column-java
            sqoop_column_type = None

            if self.common_config.db_mssql == True:
                if source_column_type == "bit":
                    sqoop_column_type = "Integer"

                if source_column_type == "float":
                    sqoop_column_type = "Float"

                if source_column_type in (
                    "timestamp",
                    "uniqueidentifier",
                    "ntext",
                    "xml",
                    "text",
                    "varchar(-1)",
                    "nvarchar(-1)",
                ):
                    sqoop_column_type = "String"
                    column_type = "string"

                if source_column_type == "time":
                    sqoop_column_type = "String"
                    column_type = "string"

                column_type = re.sub("^nvarchar\(", "varchar(", column_type)
                column_type = re.sub("^datetime$", "timestamp", column_type)
                column_type = re.sub("^datetime2$", "timestamp", column_type)
                column_type = re.sub("^smalldatetime$", "timestamp", column_type)
                column_type = re.sub("^bit$", "tinyint", column_type)
                column_type = re.sub("^binary\([0-9]*\)$", "binary", column_type)
                column_type = re.sub("^varbinary$", "binary", column_type)
                column_type = re.sub("^varbinary\([0-9]*\)$", "binary", column_type)
                column_type = re.sub("^geometry$", "binary", column_type)
                column_type = re.sub("^geography\(-1\)$", "binary", column_type)
                column_type = re.sub("^image$", "binary", column_type)
                column_type = re.sub("^money$", "decimal", column_type)
                column_type = re.sub("^nchar\(", "char(", column_type)
                column_type = re.sub("^numeric\(", "decimal(", column_type)
                column_type = re.sub("^real$", "float", column_type)
                column_type = re.sub("^varchar\(-1\)$", "string", column_type)
                column_type = re.sub("^varchar\(65355\)$", "string", column_type)
                column_type = re.sub("^smallmoney$", "float", column_type)

            if self.common_config.db_oracle == True:
                if source_column_type.startswith("rowid("):
                    sqoop_column_type = "String"

                if source_column_type.startswith("sdo_geometry("):
                    sqoop_column_type = "String"
                    column_type = "binary"

                if source_column_type.startswith("jtf_pf_page_object("):
                    sqoop_column_type = "String"
                    column_type = "binary"

                if source_column_type.startswith("wf_event_t("):
                    sqoop_column_type = "String"
                    column_type = "binary"

                if source_column_type.startswith("ih_bulk_type("):
                    sqoop_column_type = "String"
                    column_type = "binary"

                if source_column_type.startswith("anydata("):
                    sqoop_column_type = "String"
                    column_type = "binary"

                if source_column_type in ("clob", "nclob", "nlob", "long", "long raw"):
                    sqoop_column_type = "String"
                    column_type = "string"

                if source_column_type.startswith("float"):
                    sqoop_column_type = "Float"

                column_type = re.sub("^nvarchar\(", "varchar(", column_type)
                column_type = re.sub(" char\)$", ")", column_type)
                column_type = re.sub(" byte\)$", ")", column_type)
                column_type = re.sub("^char\(0\)$", "char(4000)", column_type)
                column_type = re.sub("^varchar2\(0\)$", "varchar(4000)", column_type)
                column_type = re.sub("^varchar2\(", "varchar(", column_type)
                column_type = re.sub("^nvarchar2\(", "varchar(", column_type)
                column_type = re.sub("^rowid\(", "varchar(", column_type)
                column_type = re.sub("^number$", "decimal(38,19)", column_type)
                if re.search("^number\([0-9]\)", column_type):
                    column_type = "int"
                    sqoop_column_type = "Integer"
                if re.search("^number\(1[0-8]\)", column_type):
                    column_type = "bigint"
                    sqoop_column_type = "String"
                column_type = re.sub("^number\(", "decimal(", column_type)
                column_type = re.sub("^date$", "timestamp", column_type)
                column_type = re.sub(
                    "^timestamp\([0-9]*\) with time zone", "timestamp", column_type
                )
                column_type = re.sub("^blob$", "binary", column_type)
                column_type = re.sub("^xmltype\([0-9]*\)$", "string", column_type)
                column_type = re.sub("^raw$", "binary", column_type)
                column_type = re.sub("^raw\([0-9]*\)$", "binary", column_type)
                column_type = re.sub("^timestamp\([0-9]\)", "timestamp", column_type)
                column_type = re.sub("^long raw\([0-9]*\)$", "string", column_type)
                column_type = re.sub(
                    "^decimal\(3,4\)", "decimal(8,4)", column_type
                )  # Very stange Oracle type number(3,4), how can pricision be smaller than scale?
                if re.search("^decimal\([0-9][0-9]\)$", column_type) != None:
                    column_type = re.sub("\)$", ",0)", column_type)

            if self.common_config.db_mysql == True:
                if source_column_type == "bit":
                    column_type = "int"
                    sqoop_column_type = "Integer"

                if source_column_type in ("tinyint", "smallint", "mediumint"):
                    column_type = "int"
                    sqoop_column_type = "Integer"

                if re.search("^enum\(", column_type):
                    column_type = "string"
                    sqoop_column_type = "String"

                column_type = re.sub("^character varying\(", "varchar(", column_type)
                column_type = re.sub("^mediumtext\([0-9]*\)", "string", column_type)
                column_type = re.sub("^tinytext\([0-9]*\)", "varchar(255)", column_type)
                column_type = re.sub("^text\([0-9]*\)", "string", column_type)
                column_type = re.sub("^json$", "string", column_type)
                column_type = re.sub("^datetime$", "timestamp", column_type)
                column_type = re.sub("^varbinary$", "binary", column_type)
                column_type = re.sub("^varbinary\([0-9]*\)$", "binary", column_type)
                column_type = re.sub("^binary\([0-9]*\)$", "binary", column_type)
                column_type = re.sub("^blob\([0-9]*\)$", "binary", column_type)
                if column_type == "time":
                    column_type = "string"
                    sqoop_column_type = "String"

            if self.common_config.db_postgresql == True:
                column_type = re.sub("^character varying\(", "varchar(", column_type)
                column_type = re.sub("^text", "string", column_type)

            if self.common_config.db_progress == True:
                column_type = re.sub("^integer", "int", column_type)
                column_type = re.sub("^numeric\(", "decimal(", column_type)
                column_type = re.sub("^date\([0-9]\)", "date", column_type)
                column_type = re.sub("^bit\([1]\)", "boolean", column_type)
                column_type = re.sub(",none\)", ",0)", column_type)

            if self.common_config.db_db2udb == True:
                if re.search("^clob", column_type):
                    column_type = "string"
                    sqoop_column_type = "String"

                if re.search("^time\([0-9]\)", column_type):
                    column_type = "varchar(9)"
                    sqoop_column_type = "String"

                column_type = re.sub("^integer", "int", column_type)
                column_type = re.sub("^timestmp", "timestamp", column_type)
                column_type = re.sub("^blob", "binary", column_type)
                column_type = re.sub("^real", "float", column_type)
                column_type = re.sub("^vargraph", "varchar", column_type)
                column_type = re.sub("^graphic", "varchar", column_type)

            if self.common_config.db_db2as400 == True:
                if re.search("^numeric\(", column_type):
                    column_type = re.sub("^numeric\(", "decimal(", column_type)
                    column_type = re.sub("\)$", ",0)", column_type)

                if re.search("^clob", column_type):
                    column_type = "string"
                    sqoop_column_type = "String"

                column_type = re.sub("^integer", "int", column_type)
                column_type = re.sub("^timestmp", "timestamp", column_type)
                column_type = re.sub("^timestamp\(.*\)", "timestamp", column_type)
                column_type = re.sub("^varbinary$", "binary", column_type)
                column_type = re.sub("^varbinary\([0-9]*\)$", "binary", column_type)
                column_type = re.sub("^blob", "binary", column_type)
                column_type = re.sub("^real", "float", column_type)

            if self.common_config.db_mongodb == True:
                column_type = re.sub(":null", ":string", column_type)
                column_type = re.sub("^null$", "string", column_type)

            if self.common_config.db_snowflake == True:
                if column_type.startswith("text("):
                    column_type = "string"

                column_type = re.sub("^timestamp_ltz$", "timestamp", column_type)
                column_type = re.sub("^timestamp_ntz$", "timestamp", column_type)
                column_type = re.sub("^timestamp_tz$", "timestamp", column_type)
                column_type = re.sub("^number$", "decimal(38,0)", column_type)

            if self.common_config.db_informix == True:
                if source_column_type in (
                    "tinyint",
                    "smallint",
                    "mediumint",
                    "integer",
                ):
                    column_type = "int"
                    sqoop_column_type = "Integer"

                if source_column_type == "clob":
                    sqoop_column_type = "String"
                    column_type = "string"

            if self.common_config.db_sqlanywhere == True:
                column_type = re.sub("^long varchar$", "string", column_type)
                column_type = re.sub("^long binary$", "binary", column_type)

                if source_column_type in (
                    "unsigned int",
                    "unsigned smallint",
                    "unsigned mediumint",
                    "unsigned int",
                    "tinyint",
                    "smallint",
                    "mediumint",
                    "integer",
                ):
                    sqoop_column_type = "integer"
                    column_type = "int"

                if source_column_type == "unsigned bigint":
                    sqoop_column_type = "integer"
                    column_type = "bigint"

                if source_column_type == "long varbit":
                    sqoop_column_type = "String"
                    column_type = "string"

            # Hive only allow max 255 in size for char's. If we get a char that is larger than 255, we convert it to a varchar
            if column_type.startswith("char("):
                column_precision = int(column_type.split("(")[1].split(")")[0])
                if column_precision > 255:
                    column_type = re.sub("^char\(", "varchar(", column_type)

            # Remove precision from datatypes that doesnt include a precision
            column_type = re.sub("^float\([0-9]*\)", "float", column_type)
            column_type = re.sub("^bigint\([0-9]*\)", "bigint", column_type)
            column_type = re.sub("^int\([0-9]*\)", "int", column_type)

            if columnTypeOverride != None:
                column_type = columnTypeOverride

            # If the data in the column will be anonymized, we will set the column type to a string type.
            if anonymizationFunction != "None":
                column_type = "string"

            # As Parquet imports some column types wrong, we need to map them all to string
            if column_type in ("timestamp", "date", "bigint"):
                sqoop_column_type = "String"
            if re.search("^decimal\(", column_type):
                sqoop_column_type = "String"

            # Fetch if we should force this column to 'string' in Hive
            if (
                column_type.startswith("char(") == True
                or column_type.startswith("varchar(") == True
            ):
                columnForceString = self.getColumnForceString(column_name)
                if columnForceString == True:
                    column_type = "string"

            if self.etlEngine == constant.ETL_ENGINE_SPARK:
                # This means Iceberg file format. So we need to handle conversion between Hive and Iceberg column types
                # The logic is to first translate source columns to Hive and from Hive to Iceberg. That is a far simpler thing
                # compared to translate all different source tables to Iceberg as well
                # https://docs.cloudera.com/cdw-runtime/1.5.1/iceberg-how-to/topics/iceberg-data-types.html?
                if (
                    column_type.startswith("char(") == True
                    or column_type.startswith("varchar(") == True
                ):
                    column_type = "string"

                if source_column_type == "bit":
                    column_type = "boolean"

                if column_type in ("tinyint", "smallint"):
                    column_type = "int"

            if includeColumnInImport == True:
                if sqoopColumnTypeOverride != None:
                    self.sqoop_mapcolumnjava.append(
                        column_name + "=" + sqoopColumnTypeOverride
                    )
                elif sqoop_column_type != None:
                    self.sqoop_mapcolumnjava.append(
                        column_name + "=" + sqoop_column_type
                    )

                # Add , between column names in the list
                if len(self.sqlGeneratedHiveColumnDefinition) > 0:
                    self.sqlGeneratedHiveColumnDefinition = (
                        self.sqlGeneratedHiveColumnDefinition + ", "
                    )
                # Add the column to the sqlGeneratedHiveColumnDefinition variable. This will be the base for the auto generated SQL
                self.sqlGeneratedHiveColumnDefinition += (
                    "`" + column_name + "` " + column_type
                )
                if source_column_comment != None:
                    self.sqlGeneratedHiveColumnDefinition += (
                        " COMMENT '" + source_column_comment + "'"
                    )

                # Add the column to the SQL query that can be used by sqoop or spark
                column_name_parquet_supported = self.getParquetColumnName(column_name)
                quote = self.common_config.getQuoteAroundColumn()

                if len(self.sqlGeneratedSqoopQuery) == 0:
                    self.sqlGeneratedSqoopQuery = "select "
                else:
                    self.sqlGeneratedSqoopQuery += ", "

                if source_column_name != column_name_parquet_supported:
                    if re.search('"', source_column_name):
                        self.sqlGeneratedSqoopQuery += (
                            "'"
                            + source_column_name
                            + "' as \""
                            + column_name_parquet_supported
                            + '"'
                        )
                    else:
                        self.sqlGeneratedSqoopQuery += (
                            quote
                            + source_column_name
                            + quote
                            + " as "
                            + quote
                            + column_name_parquet_supported
                            + quote
                        )

                    if (
                        self.isColumnNameReservedInSqoop(column_name_parquet_supported)
                        == True
                    ):
                        columnNameReserved = True
                        logging.warning(
                            "The column '%s' is a reserved column namn in Sqoop. Please rename the column in 'column_name_override'"
                            % (column_name_parquet_supported)
                        )

                    self.sqoop_use_generated_sql = True
                else:
                    self.sqlGeneratedSqoopQuery += quote + source_column_name + quote

                    if self.isColumnNameReservedInSqoop(source_column_name) == True:
                        columnNameReserved = True
                        logging.warning(
                            "The column '%s' is a reserved column namn in Sqoop. Please rename the column in 'column_name_override'"
                            % (source_column_name)
                        )

            if columnID == None:
                query = (
                    "insert into import_columns "
                    "("
                    "    table_id,"
                    "    hive_db,"
                    "    hive_table,"
                    "    column_name,"
                    "    column_order,"
                    "    source_column_name,"
                    "    column_type,"
                    "    source_column_type,"
                    "    source_database_type,"
                    "    sqoop_column_type,"
                    "    last_update_from_source,"
                    "    comment"
                    ") values ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s )"
                )

                self.mysql_cursor01.execute(
                    query,
                    (
                        self.table_id,
                        self.Hive_DB,
                        self.Hive_Table,
                        column_name.lower(),
                        columnOrder,
                        source_column_name,
                        column_type,
                        source_column_type,
                        self.common_config.jdbc_servertype,
                        sqoop_column_type,
                        self.startDate,
                        source_column_comment,
                    ),
                )
                logging.debug(
                    "SQL Statement executed: %s" % (self.mysql_cursor01.statement)
                )
            else:
                query = (
                    "update import_columns set "
                    "    hive_db = %s, "
                    "    hive_table = %s, "
                    "    column_name = %s, "
                    "    column_order = %s, "
                    "    column_type = %s, "
                    "    source_column_type = %s, "
                    "    source_database_type = %s, "
                    "    sqoop_column_type = %s, "
                    "    source_primary_key = NULL, "
                    "    last_update_from_source = %s, "
                    "    comment = %s "
                    "where table_id = %s and source_column_name = %s "
                )

                self.mysql_cursor01.execute(
                    query,
                    (
                        self.Hive_DB,
                        self.Hive_Table,
                        column_name.lower(),
                        columnOrder,
                        column_type,
                        source_column_type,
                        self.common_config.jdbc_servertype,
                        sqoop_column_type,
                        self.startDate,
                        source_column_comment,
                        self.table_id,
                        source_column_name,
                    ),
                )
                logging.debug(
                    "SQL Statement executed: %s" % (self.mysql_cursor01.statement)
                )

        # Commit all the changes to the import_column column
        self.mysql_conn.commit()

        # Some columnnames are reserved. But in order to change them, we need the information inside import_columns. So we just mark
        # the columns with columnNameReserved = True and saves the data. After the save we check if it's equal to True
        # and raise an exception. This way, the user have a chance to fix the problem

        if columnNameReserved == True:
            raise invalidConfiguration(
                "There are columns with reserved words. DBImport cant continue until that is handled"
            )

        # Add the source the to the generated sql query
        self.sqlGeneratedSqoopQuery += " from %s" % (
            self.common_config.getJDBCsqlFromTable(
                schema=self.source_schema, table=self.source_table
            )
        )

        # Add ( and ) to the Hive column definition so it contains a valid string for later use in the solution
        self.sqlGeneratedHiveColumnDefinition = (
            "( " + self.sqlGeneratedHiveColumnDefinition + " )"
        )

        logging.debug("Settings from import_config.saveColumnData()")
        logging.debug("    sqlGeneratedSqoopQuery = %s" % (self.sqlGeneratedSqoopQuery))
        logging.debug(
            "    sqlGeneratedHiveColumnDefinition = %s"
            % (self.sqlGeneratedHiveColumnDefinition)
        )
        logging.debug("Executing import_config.saveColumnData() - Finished")

    def isColumnNameReservedInSqoop(self, columnName):
        """Returns True or False depending if the column_name is reserved in Sqoop"""

        if columnName in ("const", "private", "public", "default", "long"):
            return True
        else:
            return False

    def getParquetColumnName(self, column_name):

        # Changing the mapping in here also requires you to change it in DBImportOperation/common_operations.py, funtion getHiveColumnNameDiff

        column_name = (
            column_name.lower()
            .replace(" ", "_")
            .replace("%", "pct")
            .replace("(", "_")
            .replace(")", "_")
            .replace("ü", "u")
            .replace("å", "a")
            .replace("ä", "a")
            .replace("ö", "o")
            .replace("#", "hash")
            .replace("`", "")
            .replace("/", "")
            .replace("'", "")
            .replace(";", "")
            .replace("\n", "")
            .replace("\\", "")
            .replace("’", "")
            .replace(":", "")
            .replace(",", "")
            .replace(".", "")
            .replace('"', "")
        )

        if column_name.startswith("_") == True:
            # 			column_name = column_name[1:]
            column_name = "underscore%s" % (column_name)

        return column_name

    def setPrimaryKeyColumn(
        self,
    ):
        # This is one of the main functions when it comes to source system schemas. This will parse the output from the Python Schema Program
        # and update the source_primary_key column in the import_columns table with the information on what key is part of the PK
        logging.debug("")
        logging.debug("Executing import_config.setPrimaryKeyColumn()")
        logging.info("Setting PrimaryKey information in MySQL table - import_columns")

        # COL_DATA_TYPE
        # COL_KEY_POSITION
        # COL_NAME
        # CONSTRAINT_NAME
        # CONSTRAINT_TYPE
        # REFERENCE_COL_NAME
        # REFERENCE_SCHEMA_NAME
        # REFERENCE_TABLE_NAME
        # SCHEMA_NAME
        # TABLE_NAME

        self.generatedPKcolumns = ""

        for index, row in self.common_config.source_keys_df.iterrows():
            key_type = row["CONSTRAINT_TYPE"]
            key_id = row["COL_KEY_POSITION"]
            column_name = self.stripUnwantedCharColumnName(row["COL_NAME"])

            # Handle reserved column names in Hive
            if column_name == "date":
                column_name = column_name + "_HIVE"
            if column_name == "interval":
                column_name = column_name + "_HIVE"

                # TODO: Loop through only PK's in the for loop when external Python code is completed. Same as we do for FK's
                # Determine what the PK is called in the Pandas dataframe for each database type
            key_type_reference = None

            if key_type_reference == None:
                key_type_reference = constant.PRIMARY_KEY

            # As we only work with PK in this function, we ignore all other kind of keys (thats usually Foreign Keys we ignore)
            if key_type != key_type_reference:
                continue

            logging.debug("	key_type: %s" % (key_type))
            logging.debug("	key_type_reference: %s" % (key_type_reference))
            logging.debug("	key_id: %s" % (key_id))
            logging.debug("	column_name: %s" % (column_name))

            # Append the column_name to the generated PK list.
            if len(self.generatedPKcolumns) > 0:
                self.generatedPKcolumns += ","
            self.generatedPKcolumns += column_name

            query = (
                "update import_columns set "
                "    source_primary_key = %s "
                "where table_id = %s and lower(source_column_name) = %s "
            )

            self.mysql_cursor01.execute(query, (key_id, self.table_id, column_name))
            logging.debug(
                "SQL Statement executed: %s" % (self.mysql_cursor01.statement)
            )

        # Commit all the changes to the import_column column
        self.mysql_conn.commit()

        logging.debug("Executing import_config.setPrimaryKeyColumn() - Finished")

    def saveIndexData(
        self,
    ):
        logging.debug("Executing import_config.saveIndexData()")
        logging.info("Saving index metadata to MySQL table - import_tables_indexes")

        try:
            query = "delete from import_tables_indexes where table_id = %s"
            self.mysql_cursor01.execute(query, (self.table_id,))
            logging.debug(
                "SQL Statement executed: %s" % (self.mysql_cursor01.statement)
            )
        except mysql.connector.errors.IntegrityError as e:
            logging.error(
                "Unknown error when deleting old index data from DBImport configuration database"
            )
            raise (e)

        try:
            for index, row in self.common_config.source_index_df.sort_values(
                by=["Name", "ColumnOrder"]
            ).iterrows():
                # Save the Index data to the MySQL table
                query = (
                    "insert into import_tables_indexes "
                    "("
                    "    `table_id`, "
                    "    `hive_db`, "
                    "    `hive_table`, "
                    "    `index_name`, "
                    "    `index_type`, "
                    "    `index_unique`, "
                    "    `column`, "
                    "    `column_type`, "
                    "    `column_order`, "
                    "    `column_is_nullable` "
                    ") values ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s )"
                )

                try:
                    columnType = self.common_config.source_columns_df.loc[
                        self.common_config.source_columns_df["SOURCE_COLUMN_NAME"]
                        == row[3],
                        "SOURCE_COLUMN_TYPE",
                    ].iloc[0]
                except IndexError:
                    # There are examples where an index indicate that a specific columns should be included, but that column does not exist in the table.
                    # If that is the case, we will get an IndexError here and that is what we need to handle as it obvious is incorrect
                    continue

                try:
                    self.mysql_cursor02.execute(
                        query,
                        (
                            self.table_id,
                            self.Hive_DB,
                            self.Hive_Table,
                            row[0],
                            row[1],
                            row[2],
                            row[3],
                            columnType,
                            row[4],
                            row[5],
                        ),
                    )
                    self.mysql_conn.commit()
                    logging.debug(
                        "SQL Statement executed: %s" % (self.mysql_cursor02.statement)
                    )
                except mysql.connector.errors.IntegrityError as e:
                    if "Duplicate entry" in str(e):
                        logging.warning(
                            "Table indexes cant be saved as name/id is not unique in DBImport Configuration Database"
                        )
                    else:
                        logging.error(
                            "Unknown error when saving index data to DBImport configuration database"
                        )
                        raise (e)
        except KeyError:
            pass

        logging.debug("Executing import_config.saveIndexData() - Finished")

    def saveKeyData(
        self,
    ):
        # This is one of the main functions when it comes to source system schemas. This will parse the output from the Python Schema Program
        # and update the import_foreign_keys table with the information on what FK's are available for the table
        logging.debug("")
        logging.debug("Executing import_config.saveKeyData()")
        logging.info(
            "Setting ForeignKey information in MySQL table - import_foreign_keys"
        )

        # COL_DATA_TYPE
        # COL_KEY_POSITION
        # COL_NAME
        # CONSTRAINT_NAME
        # CONSTRAINT_TYPE
        # REFERENCE_COL_NAME
        # REFERENCE_SCHEMA_NAME
        # REFERENCE_TABLE_NAME
        # SCHEMA_NAME
        # TABLE_NAME

        key_type_reference = ""
        fk_index = 1
        fk_index_counter = 1
        fk_index_dict = {}

        if key_type_reference == "":
            key_type_reference = constant.FOREIGN_KEY

        if self.common_config.source_keys_df.empty == True:
            return

        # Create a new DF that only contains FK's
        source_fk_df = self.common_config.source_keys_df[
            self.common_config.source_keys_df["CONSTRAINT_TYPE"] == key_type_reference
        ]

        # Loop through all unique ForeignKeys that is presented in the source.
        for source_fk in source_fk_df.CONSTRAINT_NAME.unique():
            logging.debug("Parsing FK with name '%s'" % (source_fk))

            # Iterate over the rows that matches the FK name
            for index, row in source_fk_df[
                source_fk_df["CONSTRAINT_NAME"] == source_fk
            ].iterrows():
                source_fk_name = row["CONSTRAINT_NAME"]
                column_name = row["COL_NAME"]
                ref_schema_name = row["REFERENCE_SCHEMA_NAME"]
                ref_table_name = row["REFERENCE_TABLE_NAME"]
                ref_column_name = row["REFERENCE_COL_NAME"]
                key_position = row["COL_KEY_POSITION"]

                # If we already worked with this FK, we fetch the fk_index from the dictinary.
                # If not, we add it to the dictionary and increase the counter so the next FK gets a higher index number.
                if source_fk_name in fk_index_dict:
                    fk_index = fk_index_dict[source_fk_name]
                else:
                    fk_index_dict[source_fk_name] = fk_index_counter
                    fk_index = fk_index_counter
                    fk_index_counter += 1

                # MySQL dont have schemas. So we make them default to dash
                if self.common_config.db_mysql == True:
                    ref_schema_name = "-"

                # Select table_id's that matches the source based on dbalias, schema and table name
                # This can be many table_id's as we might have imported the same source multiple times
                # under different names.
                # TODO: Right now, we only support reference to one table per FK. If we have imported the same table more than once
                # the FK will add with primary key validation. To get this error, remove the "limit 1" in the sql statement and import
                # the same source table twice
                query = "select table_id from import_tables where dbalias = %s and source_schema = %s and source_table = %s limit 1"
                self.mysql_cursor01.execute(
                    query, (self.connection_alias, ref_schema_name, ref_table_name)
                )
                logging.debug(
                    "SQL Statement executed: %s" % (self.mysql_cursor01.statement)
                )

                if self.mysql_cursor01.rowcount == 0:
                    logging.warning(
                        "Reference table '%s.%s' on alias '%s' does not exist in MySQL. Skipping FK on this table"
                        % (ref_schema_name, ref_table_name, self.connection_alias)
                    )
                else:
                    rows = self.mysql_cursor01.fetchall()
                    for ref_table_id in rows:
                        # Looping through all table_id's that matches the source from the previous select
                        logging.debug(
                            "Parsing referenced table with table_id: %s"
                            % (ref_table_id[0])
                        )

                        # Fetch the column_id for the table own column
                        query = "select column_id from import_columns where table_id = %s and lower(source_column_name) = %s"
                        self.mysql_cursor02.execute(query, (self.table_id, column_name))
                        logging.debug(
                            "SQL Statement executed: %s"
                            % (self.mysql_cursor02.statement)
                        )
                        row = self.mysql_cursor02.fetchone()
                        source_column_id = row[0]

                        # Fetch the column_id for the column in the referensed table
                        query = "select column_id from import_columns where table_id = %s and lower(source_column_name) = %s"
                        self.mysql_cursor02.execute(
                            query, (ref_table_id[0], ref_column_name)
                        )
                        logging.debug(
                            "SQL Statement executed: %s"
                            % (self.mysql_cursor02.statement)
                        )
                        row = self.mysql_cursor02.fetchone()

                        if self.mysql_cursor02.rowcount == 0:
                            logging.warning(
                                "Referenced column '%s' in '%s.%s' cant be found in MySQL. Skipping FK on this table"
                                % (ref_column_name, ref_schema_name, ref_table_name)
                            )
                            ref_column_id = None
                        else:
                            ref_column_id = row[0]

                            # Save the FK data to the MySQL table
                            query = (
                                "insert into import_foreign_keys "
                                "("
                                "    table_id, "
                                "    column_id, "
                                "    fk_index, "
                                "    fk_table_id, "
                                "    fk_column_id, "
                                "    key_position "
                                ") values ( %s, %s, %s, %s, %s, %s )"
                            )

                            logging.debug("self.table_id:    %s" % (self.table_id))
                            logging.debug("source_column_id: %s" % (source_column_id))
                            logging.debug("fk_index:         %s" % (fk_index))
                            logging.debug("ref_table_id[0]:  %s" % (ref_table_id[0]))
                            logging.debug("ref_column_id:    %s" % (ref_column_id))
                            logging.debug("key_position:     %s" % (key_position))

                            try:
                                self.mysql_cursor02.execute(
                                    query,
                                    (
                                        self.table_id,
                                        source_column_id,
                                        fk_index,
                                        ref_table_id[0],
                                        ref_column_id,
                                        key_position,
                                    ),
                                )
                                self.mysql_conn.commit()
                                logging.debug(
                                    "SQL Statement executed: %s"
                                    % (self.mysql_cursor02.statement)
                                )
                            except mysql.connector.errors.IntegrityError as e:
                                if "Duplicate entry" in str(e):
                                    logging.warning(
                                        "Foreign Key cant be saved as name is not unique in DBImport Configuration Database"
                                    )
                                else:
                                    logging.error(
                                        "Unknown error when saving ForeignKey data to DBImport configuration database"
                                    )
                                    raise (e)

        logging.debug("Executing import_config.saveKeyData() - Finished")

    def saveGeneratedData(self):
        # This will save data to the generated* columns in import_table
        logging.debug("")
        logging.debug("Executing import_config.saveGeneratedData()")
        logging.info("Saving generated data to MySQL table - import_tables")

        if self.importPhase == constant.IMPORT_PHASE_ORACLE_FLASHBACK:
            # We need to force one of the Oracle Flashback columns to a map-column-java option.
            self.sqoop_mapcolumnjava.append("datalake_flashback_startscn=String")

        # Create a valid self.generatedSqoopOptions value
        self.generatedSqoopOptions = None
        if len(self.sqoop_mapcolumnjava) > 0:
            self.generatedSqoopOptions = "--map-column-java "
            for column_map in self.sqoop_mapcolumnjava:
                column, value = column_map.split("=")

                column = self.convertHiveColumnNameReservedWords(column)

                self.generatedSqoopOptions += "%s=%s," % (
                    self.getParquetColumnName(column),
                    value,
                )
            # Remove the last ","
            self.generatedSqoopOptions = self.generatedSqoopOptions[:-1]

        if self.generatedPKcolumns == "":
            self.generatedPKcolumns = None

        if self.sqoop_use_generated_sql == True:
            sqoop_use_generated_sql_local = 1
        else:
            sqoop_use_generated_sql_local = 0

        query = (
            "update import_tables set "
            "    generated_hive_column_definition = %s, "
            "    generated_sqoop_query = %s, "
            "    comment = %s, "
            "    generated_sqoop_options = %s, "
            "    generated_pk_columns = %s, "
            "    generated_foreign_keys = NULL, "
            "    sqoop_use_generated_sql = %s, "
            "    sourceTableType = %s "
            "where table_id = %s "
        )

        self.mysql_cursor01.execute(
            query,
            (
                self.sqlGeneratedHiveColumnDefinition,
                self.sqlGeneratedSqoopQuery,
                self.table_comment,
                self.generatedSqoopOptions,
                self.generatedPKcolumns,
                sqoop_use_generated_sql_local,
                self.common_config.tableType,
                self.table_id,
            ),
        )
        self.mysql_conn.commit()
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement))

        logging.debug("Executing import_config.saveGeneratedData() - Finished")

    def calculateJobMappers(self):
        """Based on previous sqoop size, the number of mappers for sqoop will be calculated. Formula is sqoop_size / 128MB."""
        logging.debug("Executing import_config.calculateJobMappers()")

        if self.mongoImport == False:
            logging.info(
                "Calculating the number of SQL sessions in the source system that the import will use"
            )

        sqlSessionsMin = 1
        sqlSessionsMax = None

        query = "select max_import_sessions from jdbc_connections where dbalias = %s "
        self.mysql_cursor01.execute(query, (self.connection_alias,))
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement))

        row = self.mysql_cursor01.fetchone()

        if row[0] != None and row[0] > 0:
            sqlSessionsMax = row[0]
            logging.info(
                "\u001b[33mThis connection is limited to a maximum of %s parallel sessions during import\u001b[0m"
                % (sqlSessionsMax)
            )

        if row[0] != None and row[0] == 0:
            logging.warning(
                "Unsupported value of 0 in column 'max_import_sessions' in table 'jdbc_connections'"
            )

        sqlSessionsMaxFromConfig = int(
            self.common_config.getConfigValue(key="import_max_sessions")
        )
        sqlSessionsDefault = int(
            self.common_config.getConfigValue(key="import_default_sessions")
        )

        if sqlSessionsMax == None:
            sqlSessionsMax = sqlSessionsMaxFromConfig
        if sqlSessionsMaxFromConfig < sqlSessionsMax:
            sqlSessionsMax = sqlSessionsMaxFromConfig
        if sqlSessionsDefault > sqlSessionsMax:
            sqlSessionsDefault = sqlSessionsMax

        if self.mongoImport == True:
            # If it's a Mongo import, we dont need to find the number of mappers based on size. This is handled by the Mongo API
            self.sqlSessions = sqlSessionsDefault
            return

        hdfsBlocksize = int(self.common_config.getConfigValue(key="hdfs_blocksize"))
        if hdfsBlocksize == None or hdfsBlocksize == 0:
            logging.error(
                "The setting 'hdfs_blocksize' in configuration table is incorrect. Please enter a correct size"
            )
            raise Exception

        # Execute SQL query that calculates the value
        query = "select T.sqoop_last_size, "
        query += "cast(S.size / %s as unsigned) as calculated_mappers, " % (
            hdfsBlocksize
        )
        query += "T.mappers "
        query += "from import_tables as T "
        query += "left join import_statistics_last as S "
        query += "   on T.hive_db = S.hive_db and T.hive_table = S.hive_table "
        query += "where table_id = %s "

        self.mysql_cursor01.execute(query, (self.table_id,))
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement))

        if self.mysql_cursor01.rowcount != 1:
            logging.error(
                "Error: Number of rows returned from query on 'import_table' was not one."
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
        self.sqoop_mappers = row[2]

        logging.debug("sqlSessionsLast: %s" % (sqlSessionsLast))
        logging.debug("sqlSessions:     %s" % (self.sqlSessions))
        logging.debug("sqoop_mappers:   %s" % (self.sqoop_mappers))

        if self.sqoop_mappers > 0:
            self.sqlSessions = self.sqoop_mappers
            logging.info(
                "Setting the number of SQL splits to %s (fixed value)"
                % (self.sqlSessions)
            )
        else:
            if self.sqlSessions == None:
                logging.info(
                    "Cant find the previous import size so it's impossible to calculate the correct amount of SQL splits. Will default to %s"
                    % (sqlSessionsDefault)
                )
                self.sqlSessions = sqlSessionsDefault
            else:
                if self.sqlSessions < sqlSessionsMin:
                    self.sqlSessions = sqlSessionsMin
                elif self.sqlSessions > sqlSessionsMax:
                    self.sqlSessions = sqlSessionsMax
                logging.info(
                    "The import will use %s SQL splits in the source system."
                    % (self.sqlSessions)
                )

        logging.debug("Executing import_config.calculateJobMappers() - Finished")

    def clearValidationData(self):
        logging.debug("Executing import_config.clearValidationData()")
        logging.info("Clearing validation data from previous imports")

        self.validationCustomQuerySourceValue = None
        self.validationCustomQueryHiveValue = None

        query = "update import_tables set source_rowcount = NULL, source_rowcount_incr = NULL, hive_rowcount = NULL, validationCustomQuerySourceValue = NULL, validationCustomQueryHiveValue = NULL where table_id = %s"
        self.mysql_cursor01.execute(query, (self.table_id,))
        self.mysql_conn.commit()
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement))

        logging.debug("Executing import_config.clearValidationData() - Finished")

    def getIncrWhereStatement(
        self,
        forceIncr=False,
        ignoreIfOnlyIncrMax=False,
        whereForSourceTable=False,
        whereForSqoop=False,
        ignoreSQLwhereAddition=False,
        incrementalMax=None,
    ):
        """Returns the where statement that is needed to only work on the rows that was loaded incr"""
        logging.debug("Executing import_config.getIncrWhereStatement()")
        logging.debug("forceIncr = %s" % (forceIncr))
        logging.debug("ignoreIfOnlyIncrMax = %s" % (ignoreIfOnlyIncrMax))
        logging.debug("whereForSourceTable = %s" % (whereForSourceTable))
        logging.debug("whereForSqoop = %s" % (whereForSqoop))
        logging.debug("sqoopIncrMaxvaluePending = %s" % (self.sqoopIncrMaxvaluePending))
        logging.debug("sqoop_incr_mode = %s" % (self.sqoop_incr_mode))
        logging.debug(
            "sqoop_incr_validation_method = %s" % (self.sqoop_incr_validation_method)
        )
        logging.debug("sqoop_incr_lastvalue = %s" % (self.sqoop_incr_lastvalue))

        whereStatement = None

        if self.import_is_incremental == False:
            logging.debug(
                "Executing import_config.getIncrWhereStatement() - Exited (Not an incremental import)"
            )
            return None

        if self.importPhase == constant.IMPORT_PHASE_ORACLE_FLASHBACK:
            # This is where we handle the specific where statement for Oracle FlashBack Imports
            if whereForSqoop == True:
                maxSCN = int(
                    self.common_config.executeJDBCquery(
                        "SELECT CURRENT_SCN FROM V$DATABASE"
                    ).iloc[0]["CURRENT_SCN"]
                )
                self.sqoopIncrMaxvaluePending = maxSCN
                self.sqoopIncrMinvaluePending = self.sqoop_incr_lastvalue

                query = "update import_tables set incr_minvalue_pending = %s, incr_maxvalue_pending = %s where table_id = %s"
                self.mysql_cursor01.execute(
                    query,
                    (
                        self.sqoopIncrMinvaluePending,
                        self.sqoopIncrMaxvaluePending,
                        self.table_id,
                    ),
                )
                self.mysql_conn.commit()
                logging.debug(
                    "SQL Statement executed: %s" % (self.mysql_cursor01.statement)
                )
            else:
                # COALESCE is needed if you are going to call a function that counts source target rows outside the normal import
                query = "select COALESCE(incr_minvalue_pending, incr_minvalue), COALESCE(incr_maxvalue_pending, incr_maxvalue) from import_tables where table_id = %s"
                self.mysql_cursor01.execute(query, (self.table_id,))
                logging.debug(
                    "SQL Statement executed: %s" % (self.mysql_cursor01.statement)
                )

                row = self.mysql_cursor01.fetchone()
                self.sqoopIncrMinvaluePending = row[0]
                self.sqoopIncrMaxvaluePending = row[1]

            if self.incr_maxvalue == None:
                # There is no MaxValue stored form previous imports. That means we need to do a full import up to SCN number
                # It also means that this will only be executed by sqoop, as after sqoop there will be a maxvalue present
                if whereForSourceTable == True or whereForSqoop == True:
                    try:
                        self.sqoopIncrMinvaluePending = int(
                            self.common_config.executeJDBCquery(
                                "SELECT OLDEST_FLASHBACK_SCN FROM V$FLASHBACK_DATABASE_LOG"
                            ).iloc[0]["OLDEST_FLASHBACK_SCN"]
                        )
                        whereStatement = "VERSIONS BETWEEN SCN %s AND %s " % (
                            self.sqoopIncrMinvaluePending,
                            self.sqoopIncrMaxvaluePending,
                        )
                    except SQLerror as errMsg:
                        # Some versions of Oracle dont have the V$FLASHBACK_DATABASE_LOG table. So it it's not found, we use the MINVALUE function instead
                        if "view does not exist" in str(errMsg):
                            whereStatement = "VERSIONS BETWEEN SCN MINVALUE AND %s " % (
                                self.sqoopIncrMaxvaluePending
                            )
                        else:
                            raise SQLerror(str(errMsg))

                    whereStatement += "WHERE VERSIONS_ENDTIME IS NULL AND (VERSIONS_OPERATION != 'D' OR VERSIONS_OPERATION IS NULL)"

            else:
                if whereForSourceTable == True or whereForSqoop == True:
                    whereStatement = "VERSIONS BETWEEN SCN %s AND %s " % (
                        self.sqoopIncrMinvaluePending,
                        self.sqoopIncrMaxvaluePending,
                    )
                    if forceIncr == True or whereForSqoop == True:
                        whereStatement += (
                            "WHERE VERSIONS_STARTSCN > %s AND VERSIONS_STARTSCN  <= %s AND VERSIONS_OPERATION IS NOT NULL AND VERSIONS_ENDTIME IS NULL "
                            % (
                                self.sqoopIncrMinvaluePending,
                                self.sqoopIncrMaxvaluePending,
                            )
                        )
                    else:
                        whereStatement += "WHERE VERSIONS_ENDTIME IS NULL AND (VERSIONS_OPERATION != 'D' OR VERSIONS_OPERATION IS NULL)"
                else:
                    # This will be the where statement for the Target table in Hive
                    if self.sqoop_incr_validation_method == "incr":
                        whereStatement = "datalake_update = '%s' " % (
                            self.sqoop_last_execution_timestamp
                        )

            if self.getSQLWhereAddition() != None and ignoreSQLwhereAddition == False:
                if whereStatement != None:
                    whereStatement += "AND %s " % (self.getSQLWhereAddition())
                else:
                    whereStatement = "%s " % (self.getSQLWhereAddition())

        else:
            if whereForSqoop == True:
                # If it is the where for sqoop, we need to get the max value for the configured column and save that into the config database

                if incrementalMax != None:
                    # This way, we can forcefully override the max value in an incremental import
                    maxValue = incrementalMax
                else:
                    maxValue = self.common_config.getJDBCcolumnMaxValue(
                        self.source_schema, self.source_table, self.sqoop_incr_column
                    )

                if maxValue == None:
                    # If there is no MaxValue, it means that there is no data. And by that, there is no need for a where statement
                    return ""
                if self.sqoop_incr_mode == "append":
                    self.sqoopIncrMaxvaluePending = maxValue
                try:
                    if self.sqoop_incr_mode == "lastmodified":
                        if re.search("\.([0-9]{3}|[0-9]{6})$", maxValue):
                            self.sqoopIncrMaxvaluePending = datetime.strptime(
                                maxValue, "%Y-%m-%d %H:%M:%S.%f"
                            )
                        else:
                            self.sqoopIncrMaxvaluePending = datetime.strptime(
                                maxValue, "%Y-%m-%d %H:%M:%S"
                            )

                        if self.common_config.jdbc_servertype == constant.MSSQL:
                            # MSSQL gets and error if there are microseconds in the timestamp
                            (dt, micro) = self.sqoopIncrMaxvaluePending.strftime(
                                "%Y-%m-%d %H:%M:%S.%f"
                            ).split(".")
                            self.sqoopIncrMaxvaluePending = "%s.%03d" % (
                                dt,
                                int(micro) / 1000,
                            )
                except TypeError:
                    raise invalidConfiguration(
                        "Specified column for incremental loads does not look like it's a timestamp. If it's an integer based value, please select 'append' mode instead"
                    )

                self.sqoopIncrMinvaluePending = self.sqoop_incr_lastvalue

                query = "update import_tables set incr_minvalue_pending = %s, incr_maxvalue_pending = %s where table_id = %s"
                self.mysql_cursor01.execute(
                    query,
                    (
                        self.sqoopIncrMinvaluePending,
                        self.sqoopIncrMaxvaluePending,
                        self.table_id,
                    ),
                )
                self.mysql_conn.commit()
                logging.debug(
                    "SQL Statement executed: %s" % (self.mysql_cursor01.statement)
                )

            if self.sqoopIncrMaxvaluePending == None:
                # COALESCE is needed if you are going to call a function that counts source target rows outside the normal import
                query = "select COALESCE(incr_maxvalue_pending, incr_maxvalue) from import_tables where table_id = %s"
                self.mysql_cursor01.execute(query, (self.table_id,))
                logging.debug(
                    "SQL Statement executed: %s" % (self.mysql_cursor01.statement)
                )

                row = self.mysql_cursor01.fetchone()
                self.sqoopIncrMaxvaluePending = row[0]
                logging.debug(
                    "sqoopIncrMaxvaluePending = %s" % (self.sqoopIncrMaxvaluePending)
                )

            if self.sqoop_incr_mode == "lastmodified":
                if self.common_config.jdbc_servertype == constant.MSSQL and (
                    whereForSourceTable == True or whereForSqoop == True
                ):
                    whereStatement = "%s <= CONVERT(datetime, '%s', 121) " % (
                        self.sqoop_incr_column,
                        self.sqoopIncrMaxvaluePending,
                    )
                elif self.common_config.jdbc_servertype == constant.MYSQL and (
                    whereForSourceTable == True or whereForSqoop == True
                ):
                    whereStatement = (
                        "%s <= STR_TO_DATE('%s', "
                        % (self.sqoop_incr_column, self.sqoopIncrMaxvaluePending)
                        + "'%Y-%m-%d %H:%i:%s') "
                    )
                elif self.common_config.jdbc_servertype == constant.SNOWFLAKE and (
                    whereForSourceTable == True or whereForSqoop == True
                ):
                    whereStatement = (
                        "\"%s\" <= TO_TIMESTAMP('%s', 'yyyy-mm-dd hh24:mi:ss.ff6') "
                        % (self.sqoop_incr_column, self.sqoopIncrMaxvaluePending)
                    )
                elif whereForSourceTable == True or whereForSqoop == True:
                    if self.sqoopIncrMaxvaluePending != None:
                        whereStatement = (
                            "%s <= TO_TIMESTAMP('%s', 'YYYY-MM-DD HH24:MI:SS.FF') "
                            % (self.sqoop_incr_column, self.sqoopIncrMaxvaluePending)
                        )
                else:
                    whereStatement = "`%s` <= '%s' " % (
                        self.sqoop_incr_column,
                        self.sqoopIncrMaxvaluePending,
                    )
            else:
                if whereForSourceTable == True or whereForSqoop == True:
                    whereStatement = "%s <= %s " % (
                        self.sqoop_incr_column,
                        self.sqoopIncrMaxvaluePending,
                    )
                else:
                    whereStatement = "`%s` <= %s " % (
                        self.sqoop_incr_column,
                        self.sqoopIncrMaxvaluePending,
                    )

            whereStatementSaved = whereStatement

            if self.getSQLWhereAddition() != None and ignoreSQLwhereAddition == False:
                whereStatement += "and %s " % (self.getSQLWhereAddition())

            if (
                self.sqoop_incr_validation_method == "incr"
                or forceIncr == True
                or whereForSqoop == True
            ) and self.sqoop_incr_lastvalue != None:
                if self.sqoop_incr_mode == "lastmodified":
                    if self.common_config.jdbc_servertype == constant.MSSQL and (
                        whereForSourceTable == True or whereForSqoop == True
                    ):
                        whereStatement += "and %s > CONVERT(datetime, '%s', 121) " % (
                            self.sqoop_incr_column,
                            self.sqoop_incr_lastvalue,
                        )
                    elif self.common_config.jdbc_servertype == constant.MYSQL and (
                        whereForSourceTable == True or whereForSqoop == True
                    ):
                        whereStatement += (
                            "and %s > STR_TO_DATE('%s', "
                            % (self.sqoop_incr_column, self.sqoop_incr_lastvalue)
                            + "'%Y-%m-%d %H:%i:%s') "
                        )
                    elif self.common_config.jdbc_servertype == constant.SNOWFLAKE and (
                        whereForSourceTable == True or whereForSqoop == True
                    ):
                        whereStatement += (
                            "and \"%s\" > TO_TIMESTAMP('%s', 'yyyy-mm-dd hh24:mi:ss.ff6') "
                            % (self.sqoop_incr_column, self.sqoop_incr_lastvalue)
                        )
                    elif whereForSourceTable == True or whereForSqoop == True:
                        whereStatement += (
                            "and %s > TO_TIMESTAMP('%s', 'YYYY-MM-DD HH24:MI:SS.FF') "
                            % (self.sqoop_incr_column, self.sqoop_incr_lastvalue)
                        )
                    else:
                        whereStatement += "and `%s` > '%s' " % (
                            self.sqoop_incr_column,
                            self.sqoop_incr_lastvalue,
                        )
                else:
                    if whereForSourceTable == True or whereForSqoop == True:
                        whereStatement += "and %s > %s " % (
                            self.sqoop_incr_column,
                            self.sqoop_incr_lastvalue,
                        )
                    else:
                        whereStatement += "and `%s` > %s " % (
                            self.sqoop_incr_column,
                            self.sqoop_incr_lastvalue,
                        )

            logging.debug("whereStatement = %s" % (whereStatement))
            if whereStatementSaved == whereStatement and ignoreIfOnlyIncrMax == True:
                # We have only added the MAX value to there WHERE statement and have a setting to ignore it if that is the only value
                whereStatement = None

        logging.debug("whereStatement: %s" % (whereStatement))
        logging.debug("Executing import_config.getIncrWhereStatement() - Finished")
        return whereStatement

    def runCustomValidationQueryOnJDBCTable(self):
        logging.debug("Executing import_config.runCustomValidationQueryOnJDBCTable()")

        logging.info("Executing custom validation query on source table.")

        query = self.validationCustomQuerySourceSQL
        query = query.replace("${SOURCE_SCHEMA}", self.source_schema)
        query = query.replace("${SOURCE_TABLE}", self.source_table)

        logging.debug("Validation Query on JDBC table: %s" % (query))

        resultDF = self.common_config.executeJDBCquery(query)
        resultJSON = resultDF.to_json(orient="values")
        self.validationCustomQuerySourceValue = resultJSON

        if len(self.validationCustomQuerySourceValue) > 1024:
            logging.warning(
                "'%s' is to large." % (self.validationCustomQuerySourceValue)
            )
            raise invalidConfiguration(
                "The size of the json document on the custom query exceeds 1024 bytes. Change the query to create a result with less than 512 bytes"
            )

        self.saveCustomSQLValidationSourceValue(jsonValue=resultJSON)

        logging.debug("resultDF:")
        logging.debug(resultDF)
        logging.debug("resultJSON: %s" % (resultJSON))

        logging.debug(
            "Executing import_config.runCustomValidationQueryOnJDBCTable() - Finished"
        )

    def getJDBCTableRowCount(self):
        logging.debug("Executing import_config.getJDBCTableRowCount()")

        if self.validate_source == "sqoop":
            logging.debug(
                "Executing import_config.getJDBCTableRowCount() - Finished (because self.validate_source == sqoop)"
            )
            return

        logging.info(
            "Reading number of rows in source table. This will later be used for validating the import"
        )

        JDBCRowsFull = None
        JDBCRowsIncr = None
        whereStatement = None

        if (
            self.import_is_incremental == True
            and self.importPhase != constant.IMPORT_PHASE_MSSQL_CHANGE_TRACKING
        ):
            # MSSQL Change Tracking only supports full incremental validation
            if self.importPhase == constant.IMPORT_PHASE_ORACLE_FLASHBACK:
                if self.sqoop_incr_validation_method == "full":
                    whereStatement = self.getIncrWhereStatement(
                        forceIncr=False, whereForSourceTable=True
                    )
                    query = 'SELECT COUNT(1) FROM "%s"."%s" ' % (
                        self.source_schema.upper(),
                        self.source_table.upper(),
                    )
                    query += whereStatement
                    JDBCRowsFull = int(
                        self.common_config.executeJDBCquery(query).iloc[0]["COUNT(1)"]
                    )

                whereStatement = self.getIncrWhereStatement(
                    forceIncr=True, whereForSourceTable=True
                )
                query = 'SELECT COUNT(1) FROM "%s"."%s" ' % (
                    self.source_schema.upper(),
                    self.source_table.upper(),
                )
                query += whereStatement
                JDBCRowsIncr = int(
                    self.common_config.executeJDBCquery(query).iloc[0]["COUNT(1)"]
                )
            else:
                whereStatement = self.getIncrWhereStatement(
                    forceIncr=False, whereForSourceTable=True
                )
                logging.debug(
                    "Where statement for forceIncr = False: %s" % (whereStatement)
                )
                JDBCRowsFull = self.common_config.getJDBCTableRowCount(
                    self.source_schema, self.source_table, whereStatement
                )
                logging.debug(
                    "Got %s rows from getJDBCTableRowCount()" % (JDBCRowsFull)
                )

                whereStatement = self.getIncrWhereStatement(
                    forceIncr=True, whereForSourceTable=True
                )
                logging.debug(
                    "Where statement for forceIncr = True: %s" % (whereStatement)
                )
                JDBCRowsIncr = self.common_config.getJDBCTableRowCount(
                    self.source_schema, self.source_table, whereStatement
                )
                logging.debug(
                    "Got %s rows from getJDBCTableRowCount()" % (JDBCRowsIncr)
                )
        else:
            if self.getSQLWhereAddition() != None:
                logging.debug(
                    "Where statement for full imports: %s"
                    % (self.getSQLWhereAddition())
                )
                whereStatement = self.getSQLWhereAddition()
            else:
                whereStatement = ""

            JDBCRowsFull = self.common_config.getJDBCTableRowCount(
                self.source_schema, self.source_table, whereStatement
            )

        # Save the value to the database
        query = "update import_tables set "

        if JDBCRowsIncr != None:
            query += "source_rowcount_incr = %s " % (JDBCRowsIncr)
            logging.debug(
                "Source table contains %s rows for the incremental part"
                % (JDBCRowsIncr)
            )

        if JDBCRowsFull != None:
            if JDBCRowsIncr != None:
                query += ", "
            query += "source_rowcount = %s " % (JDBCRowsFull)
            logging.debug("Source table contains %s rows" % (JDBCRowsFull))

        query += "where table_id = %s" % (self.table_id)

        self.mysql_cursor01.execute(query)
        self.mysql_conn.commit()
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement))

        logging.debug("Executing import_config.getJDBCTableRowCount() - Finished")

    def getMongoRowCount(self):
        logging.debug("Executing import_config.getMongoRowCount()")

        self.common_config.connectToMongo()
        mongoCollection = self.common_config.mongoDB[self.source_table]
        mongoCollectionCount = mongoCollection.count()
        self.common_config.disconnectFromMongo()

        self.saveSourceTableRowCount(rowCount=mongoCollectionCount)

        logging.debug("Executing import_config.getMongoRowCount() - Finished")

    def saveCustomSQLValidationSourceValue(self, jsonValue, printInfo=True):
        logging.debug("Executing import_config.saveCustomSQLValidationSourceValue()")
        if printInfo == True:
            logging.info(
                "Saving the custom SQL validation data from Source Table to the configuration database"
            )

        query = "update import_tables set validationCustomQuerySourceValue = %s where table_id = %s"

        self.mysql_cursor01.execute(query, (jsonValue, self.table_id))
        self.mysql_conn.commit()
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement))

        logging.debug(
            "Executing import_config.saveCustomSQLValidationSourceValue() - Finished"
        )

    def saveCustomSQLValidationHiveValue(self, jsonValue, printInfo=True):
        logging.debug("Executing import_config.saveCustomSQLValidationHiveValue()")
        if printInfo == True:
            logging.info(
                "Saving the custom SQL validation data from Hive Table to the configuration database"
            )

        query = "update import_tables set validationCustomQueryHiveValue = %s where table_id = %s"

        self.mysql_cursor01.execute(query, (jsonValue, self.table_id))
        self.mysql_conn.commit()
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement))

        logging.debug(
            "Executing import_config.saveCustomSQLValidationHiveValue() - Finished"
        )

    def saveSourceTableRowCount(self, rowCount, incr=False, printInfo=True):
        logging.debug("Executing import_config.saveSourceTableRowCount()")
        if printInfo == True:
            logging.info(
                "Saving the number of rows in the Source Table to the configuration database"
            )

        if self.validationMethod == constant.VALIDATION_METHOD_CUSTOMQUERY:
            logging.debug(
                "Executing import_config.saveSourceTableRowCount() - Finished (Because self.validationMethod = %s"
                % (constant.VALIDATION_METHOD_CUSTOMQUERY)
            )
            return

        # Save the value to the database
        if incr == False:
            query = "update import_tables set source_rowcount = %s where table_id = %s"
        else:
            query = (
                "update import_tables set source_rowcount_incr = %s where table_id = %s"
            )

        self.mysql_cursor01.execute(query, (rowCount, self.table_id))
        self.mysql_conn.commit()
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement))

        logging.debug("Executing import_config.saveSourceTableRowCount() - Finished")

    def saveHiveTableRowCount(self, rowCount):
        logging.debug("Executing import_config.saveHiveTableRowCount()")
        logging.info(
            "Saving the number of rows in the Hive Table to the configuration database"
        )

        # Save the value to the database
        query = "update import_tables set hive_rowcount = %s where table_id = %s"
        self.mysql_cursor01.execute(query, (rowCount, self.table_id))
        self.mysql_conn.commit()
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement))

        logging.debug("Executing import_config.saveHiveTableRowCount() - Finished")

    def resetSqoopStatistics(self, maxValue):
        logging.debug("Executing import_config.resetSqoopStatistics()")
        if maxValue != None:
            logging.info(
                "Reseting incremental values for imports. New max value is: %s"
                % (maxValue)
            )

        query = "update import_tables set "
        query += "	incr_minvalue = NULL, "
        query += "	incr_maxvalue = %s, "
        query += "	incr_minvalue_pending = NULL, "
        query += "	incr_maxvalue_pending = NULL "
        query += "where table_id = %s "

        self.mysql_cursor01.execute(query, (maxValue, self.table_id))
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement))
        self.mysql_conn.commit()

        logging.debug("Executing import_config.resetSqoopStatistics() - Finished")

    def saveImportStatistics(
        self,
        sqoopStartUTS,
        sqoopSize=None,
        sqoopRows=None,
        sqoopIncrMaxvaluePending=None,
        sqoopMappers=None,
    ):
        logging.debug("Executing import_config.saveImportStatistics()")
        logging.info("Saving %s statistics" % (self.importTool))

        self.sqoopStartUTS = sqoopStartUTS
        try:
            self.sqoop_last_execution_timestamp = datetime.utcfromtimestamp(
                sqoopStartUTS
            ).strftime("%Y-%m-%d %H:%M:%S.000")
        except TypeError:
            self.sqoop_last_execution_timestamp = None

        queryParam = []
        query = "update import_tables set "
        query += "	sqoop_last_execution = %s "
        queryParam.append(sqoopStartUTS)

        if sqoopSize != None:
            query += "  ,sqoop_last_size = %s "
            self.sqoop_last_size = sqoopSize
            queryParam.append(sqoopSize)

        if sqoopRows != None:
            query += "  ,sqoop_last_rows = %s "
            self.sqoop_last_rows = sqoopRows
            queryParam.append(sqoopRows)

        if sqoopIncrMaxvaluePending != None:
            query += "  ,incr_maxvalue_pending = %s "
            self.sqoopIncrMaxvaluePending = sqoopIncrMaxvaluePending
            queryParam.append(sqoopIncrMaxvaluePending)

        if sqoopMappers != None:
            query += "  ,sqoop_last_mappers = %s "
            self.sqoop_last_mappers = sqoopMappers
            queryParam.append(sqoopMappers)

        if self.validate_source == "sqoop" and self.validationMethod == "rowCount":
            logging.info(
                "Saving the imported row count as the number of rows in the source system."
            )
            if self.import_is_incremental == True:
                query += "  ,source_rowcount = NULL "
                query += "  ,source_rowcount_incr = %s "
            else:
                query += "  ,source_rowcount = %s "
                query += "  ,source_rowcount_incr = NULL "
            queryParam.append(sqoopRows)

        query += "where table_id = %s "
        queryParam.append(self.table_id)

        self.mysql_cursor01.execute(query, queryParam)
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement))
        self.mysql_conn.commit()

        logging.debug("Executing import_config.saveImportStatistics() - Finished")

    def checkMSSQLChangeTrackingMinValidVersion(self):
        """Checks the Minimum valid version from MSSQL Change Tracking. If the min valid version is higher than what we want to read, we will force a full import instead of an incremental import"""
        logging.debug(
            "Executing import_config.checkMSSQLChangeTrackingMinValidVersion()"
        )

        query = (
            "select CHANGE_TRACKING_MIN_VALID_VERSION(OBJECT_ID('%s')) AS MINVERSION"
            % (
                self.common_config.getJDBCsqlFromTable(
                    schema=self.source_schema, table=self.source_table
                )
            )
        )

        try:
            minVersion = int(
                self.common_config.executeJDBCquery(query).iloc[0]["MINVERSION"]
            )
        except TypeError:
            # Cant read the value. It means permission error or that Change Tracking isnt available on the table
            raise invalidConfiguration(
                "Unable to read Change Tracking version. Verify that Change Tracking is enabled on the table and that the user that is accessing it have the correct permission"
            )

        if self.incr_maxvalue == None:
            # No need to check as it will be a full load anyway
            return

        if minVersion > int(self.incr_maxvalue):
            # If we hit this part of the code, it means that the minimal version on the MSSQL Server is higher thatn what we require.
            # Only way to get out of this situation is to do a full load
            logging.warning(
                "The minimal version in MSSQL is larger than what is required. This means that we need to force a full initial load"
            )
            self.resetSqoopStatistics(maxValue=None)
            self.incr_maxvalue = None

        logging.debug(
            "Executing import_config.checkMSSQLChangeTrackingMinValidVersion() - Finished"
        )

    def getSQLtoReadFromSourceWithMSSQLChangeTracking(self):
        """Creates and return the SQL needed to read the rows from the source database with the help of MSSQL Change Tracking function"""
        logging.debug(
            "Executing import_config.getSQLtoReadFromSourceWithMSSQLChangeTracking()"
        )
        query = self.getSQLtoReadFromSource(sourceTableAsName="ST")

        query = re.sub(
            "^select",
            'select "CT"."SYS_CHANGE_VERSION" as "datalake_mssql_changetrack_version", "CT"."SYS_CHANGE_OPERATION" as "datalake_mssql_changetrack_operation",',
            query,
        )

        if self.incr_maxvalue == None:
            # This means that there is no previous version. So we need to do an initial load
            query += " LEFT JOIN CHANGETABLE(CHANGES %s, 0 ) as CT" % (
                self.common_config.getJDBCsqlFromTable(
                    schema=self.source_schema, table=self.source_table
                )
            )
            PKColumns = self.getPKcolumns(convertToLower=False)
            joinColumns = ""
            for i, targetColumn in enumerate(PKColumns.split(",")):
                if joinColumns == "":
                    joinColumns = " ON "
                else:
                    joinColumns += " AND "

                joinColumns += '"CT"."%s" = "ST"."%s"' % (targetColumn, targetColumn)

            query += joinColumns

        else:
            # This is an incremental load
            query += " RIGHT OUTER JOIN CHANGETABLE(CHANGES %s, %s ) as CT" % (
                self.common_config.getJDBCsqlFromTable(
                    schema=self.source_schema, table=self.source_table
                ),
                self.incr_maxvalue,
            )

            PKColumns = self.getPKcolumns(convertToLower=False)
            joinColumns = ""

            # PK must come from the CHANGETABLE function. This is needed in case of deletes, as the column would be NULL otherwise and the merge
            # in Hive would not be able to match and remove the row
            for i, targetColumn in enumerate(PKColumns.split(",")):
                query = re.sub(
                    '"ST"."%s"' % (targetColumn), '"CT"."%s"' % (targetColumn), query
                )

            for i, targetColumn in enumerate(PKColumns.split(",")):
                if joinColumns == "":
                    joinColumns = " ON "
                else:
                    joinColumns += " AND "

                joinColumns += '"CT"."%s" = "ST"."%s"' % (targetColumn, targetColumn)

            query += joinColumns

        return query
        logging.debug(
            "Executing import_config.getSQLtoReadFromSourceWithMSSQLChangeTracking() - Finished"
        )

    def getSQLtoReadFromSource(self, sourceTableAsName=None):
        """Creates and return the SQL needed to read all rows from the source database"""
        logging.debug("Executing import_config.getSQLtoReadFromSource()")

        quote = self.common_config.getQuoteAroundColumn()

        query = "select "
        query += "   c.source_column_name, "
        query += "   c.column_name "
        query += "from import_tables t "
        query += "join import_columns c on t.table_id = c.table_id "
        query += "where t.table_id = %s and t.last_update_from_source = c.last_update_from_source "
        query += "and c.include_in_import = 1 "
        query += "order by c.column_order"

        self.mysql_cursor01.execute(query, (self.table_id,))
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement))

        if self.mysql_cursor01.rowcount == 0:
            logging.error("Error: Zero rows returned from query on 'import_table'")
            logging.error(
                "SQL Statement that generated the error: %s"
                % (self.mysql_cursor01.statement)
            )
            raise Exception

        result_df = pd.DataFrame(self.mysql_cursor01.fetchall())
        result_df.columns = ["source_name", "name"]

        sparkQuery = ""
        for index, row in result_df.iterrows():
            if len(sparkQuery) == 0:
                sparkQuery = "select "
            else:
                sparkQuery += ", "
            if sourceTableAsName == None:
                if row["source_name"] != row["name"]:
                    sparkQuery += (
                        quote
                        + row["source_name"]
                        + quote
                        + " as "
                        + quote
                        + row["name"]
                        + quote
                    )
                else:
                    sparkQuery += quote + row["source_name"] + quote
            else:
                if row["source_name"] != row["name"]:
                    sparkQuery += (
                        quote
                        + sourceTableAsName
                        + quote
                        + "."
                        + quote
                        + row["source_name"]
                        + quote
                        + " as "
                        + quote
                        + row["name"]
                        + quote
                    )
                else:
                    sparkQuery += (
                        quote
                        + sourceTableAsName
                        + quote
                        + "."
                        + quote
                        + row["source_name"]
                        + quote
                    )

        # Add the source the to the generated sql query
        sparkQuery += " from %s" % (
            self.common_config.getJDBCsqlFromTable(
                schema=self.source_schema, table=self.source_table
            )
        )
        if sourceTableAsName != None:
            sparkQuery += " as %s" % (sourceTableAsName)

        logging.debug("Executing import_config.getSQLtoReadFromSource() - Finished")
        return sparkQuery

    def getColumnsFromConfigDatabase(
        self, restrictColumns=None, sourceIsParquetFile=False, includeAllColumns=True
    ):
        """Reads the columns from the configuration database and returns the information in a Pandas DF with the columns name, type and comment"""
        logging.debug("Executing import_config.getColumnsFromConfigDatabase()")
        hiveColumnDefinition = ""

        restrictColumnsList = []
        if restrictColumns != None:
            restrictColumnsList = restrictColumns.split(",")

        query = "select "
        query += "   COALESCE(c.column_name_override, c.column_name) as name, "
        query += "   c.column_type as type, "
        query += "   c.comment "
        query += "from import_tables t "
        query += "join import_columns c on t.table_id = c.table_id "
        query += "where t.table_id = %s and t.last_update_from_source = c.last_update_from_source "

        if includeAllColumns == False:
            query += " and c.include_in_import = 1 "

        if restrictColumnsList != []:
            query += " and c.column_name in ('"
            query += "', '".join(restrictColumnsList)
            query += "') "

        query += "order by c.column_order"
        self.mysql_cursor01.execute(query, (self.table_id,))
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement))

        if self.mysql_cursor01.rowcount == 0:
            logging.error("Error: Zero rows returned from query on 'import_table'")
            logging.error(
                "SQL Statement that generated the error: %s"
                % (self.mysql_cursor01.statement)
            )
            raise Exception

        result_df = pd.DataFrame(self.mysql_cursor01.fetchall())

        # Set the correct column namnes in the DataFrame
        result_df_columns = []
        for columns in self.mysql_cursor01.description:
            result_df_columns.append(
                columns[0]
            )  # Name of the column is in the first position
        result_df.columns = result_df_columns

        if sourceIsParquetFile == True:
            result_df["name"] = result_df["name"].apply(
                lambda x: self.getParquetColumnName(x)
            )

        logging.debug(
            "Executing import_config.getColumnsFromConfigDatabase() - Finished"
        )
        return result_df

    def getSQLWhereAddition(
        self,
    ):
        """Returns the SQL Where addition used by the sqoop and spark"""

        if (
            self.sqoop_sql_where_addition == None
            or self.sqoop_sql_where_addition.strip() == ""
        ):
            return None

        if self.common_config.db_snowflake == True:
            sqlWhere = self.sqoop_sql_where_addition
        else:
            sqlWhere = self.sqoop_sql_where_addition.replace('"', "'")

        printCustomSQL = False

        if "${MIN_VALUE}" in sqlWhere:
            printCustomSQL = True
            sqlWhere = sqlWhere.replace("${MIN_VALUE}", str(self.sqoop_incr_lastvalue))

        if "${MAX_VALUE}" in sqlWhere:
            printCustomSQL = True
            sqlWhere = sqlWhere.replace(
                "${MAX_VALUE}", str(self.sqoopIncrMaxvaluePending)
            )

        if printCustomSQL == True and self.printSQLWhereAddition == True:
            logging.info(
                "Values have been replaced in the SQL where addition. New value is:"
            )
            self.printSQLWhereAddition = False
            print(sqlWhere)

        return sqlWhere

    def getHiveTableComment(
        self,
    ):
        """Returns the table comment stored in import_tables.comment"""
        logging.debug("Executing import_config.getHiveTableComment()")
        hiveColumnDefinition = ""

        query = "select comment from import_tables where table_id = %s "
        self.mysql_cursor01.execute(query, (self.table_id,))
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement))

        if self.mysql_cursor01.rowcount == 0:
            logging.error("Error: Zero rows returned from query on 'import_table'")
            logging.error(
                "SQL Statement that generated the error: %s"
                % (self.mysql_cursor01.statement)
            )
            raise Exception

        row = self.mysql_cursor01.fetchone()

        logging.debug("Executing import_config.getHiveTableComment() - Finished")
        return row[0]

    def validateCustomQuery(self):
        """Validates the custom queries"""
        if (
            self.validationCustomQuerySourceValue == None
            or self.validationCustomQueryHiveValue == None
        ):
            logging.error(
                "Validation failed! One of the custom queries did not return a result"
            )
            logging.info(
                "Result from source query: %s" % (self.validationCustomQuerySourceValue)
            )
            logging.info(
                "Result from Hive query:   %s" % (self.validationCustomQueryHiveValue)
            )
            raise validationError()

        if self.validationCustomQuerySourceValue != self.validationCustomQueryHiveValue:
            logging.error(
                "Validation failed! The custom queries did not return the same result"
            )
            logging.info(
                "Result from source query: %s" % (self.validationCustomQuerySourceValue)
            )
            logging.info(
                "Result from Hive query:   %s" % (self.validationCustomQueryHiveValue)
            )
            raise validationError()

        return True

    def validateRowCount(self, validateSqoop=False, incremental=False):
        """Validates the rows based on values stored in import_tables -> source_columns and target_columns. Returns True or False"""
        # incremental=True is used for normal validations but on ly on incremented rows, regardless if we are going to validate full or incr.
        # This is used to validate the Import table if the validation method is full for incremental import

        logging.debug("Executing import_config.validateRowCount()")
        logging.debug("validateSqoop = %s" % (validateSqoop))
        logging.debug("incremental = %s" % (incremental))
        returnValue = None

        if self.validate_import == True:
            # Reading the saved number from the configurationdatabase
            if validateSqoop == False:
                if self.import_is_incremental == False:
                    logging.debug(
                        "validateSqoop == False & self.import_is_incremental == False"
                    )
                    # Standard full validation
                    query = "select source_rowcount, hive_rowcount from import_tables where table_id = %s "
                    if self.etlEngine == constant.ETL_ENGINE_HIVE:
                        validateTextTarget = "Hive table"
                    else:
                        validateTextTarget = "Target table"
                    validateTextSource = "Source table"
                    validateText = validateTextTarget
                elif (
                    self.sqoop_incr_validation_method == "full" and incremental == False
                ):
                    logging.debug(
                        "validateSqoop == False & self.sqoop_incr_validation_method == 'full' and incremental == False"
                    )
                    # We are not validating the sqoop import, but the validation is an incremental import and
                    # we are going to validate all the data
                    query = "select source_rowcount, hive_rowcount from import_tables where table_id = %s "
                    validateTextTarget = "Hive table"
                    validateTextSource = "Source table"
                    validateText = validateTextTarget
                else:
                    logging.debug("validateSqoop == False & else")
                    # We are not validating the sqoop import, but the validation is an incremental import and
                    # we are going to validate only the incremental part of the import (what sqoop imported)
                    query = "select source_rowcount_incr, hive_rowcount from import_tables where table_id = %s "
                    validateTextTarget = "Hive table (incr)"
                    validateTextSource = "Source table (incr)"
                    validateText = "Hive table"
            else:
                if self.import_is_incremental == False:
                    logging.debug(
                        "validateSqoop == True & self.import_is_incremental == False"
                    )
                    # Sqoop validation for full imports
                    query = (
                        "select source_rowcount from import_tables where table_id = %s "
                    )
                    validateTextTarget = "%s import" % (self.importTool.capitalize())
                    validateTextSource = "Source table"
                    validateText = validateTextTarget
                else:
                    logging.debug("validateSqoop == True & else")
                    # Sqoop validation for incremental imports
                    query = "select source_rowcount_incr from import_tables where table_id = %s "
                    validateTextTarget = "%s import (incr)" % (
                        self.importTool.capitalize()
                    )
                    validateTextSource = "Source table (incr)"
                    validateText = "%s import" % (self.importTool.capitalize())
            self.mysql_cursor01.execute(query, (self.table_id,))
            logging.debug(
                "SQL Statement executed: %s" % (self.mysql_cursor01.statement)
            )

            row = self.mysql_cursor01.fetchone()
            source_rowcount = row[0]
            if validateSqoop == False:
                target_rowcount = row[1]
            else:
                target_rowcount = self.sqoop_last_rows
                if target_rowcount == -1:
                    # Import didnt now report the number of rows imported. Validation impossible
                    logging.warning(
                        "No validation of import possible as the import didnt report the number of rows written."
                    )
                    return True
            diffAllowed = 0
            logging.debug("source_rowcount: %s" % (source_rowcount))
            logging.debug("target_rowcount: %s" % (target_rowcount))

            if source_rowcount == None:
                logging.error(
                    "There is no information about rowcount stored in import_tables. Are you running with sqoop validation method and using 'full' validation on an incremental import?"
                )
                return False

            if source_rowcount > 0:
                if self.validate_diff_allowed != None:
                    diffAllowed = int(self.validate_diff_allowed)
                else:
                    diffAllowed = int(
                        source_rowcount * (50 / (100 * math.sqrt(source_rowcount)))
                    )

            upperValidationLimit = source_rowcount + diffAllowed
            lowerValidationLimit = source_rowcount - diffAllowed

            logging.debug("upperValidationLimit: %s" % (upperValidationLimit))
            logging.debug("lowerValidationLimit: %s" % (lowerValidationLimit))
            logging.debug("diffAllowed: %s" % (diffAllowed))

            if target_rowcount == None:
                logging.warn(
                    "There is no information about rowcounts in target table. Skipping validation"
                )
                return True

            if (
                target_rowcount > upperValidationLimit
                or target_rowcount < lowerValidationLimit
            ):
                logging.error(
                    "Validation failed! The %s exceedes the allowed limit compared top the source table"
                    % (validateText)
                )
                if target_rowcount > source_rowcount:
                    logging.info(
                        "Diff between source and target table: %s"
                        % (target_rowcount - source_rowcount)
                    )
                else:
                    logging.info(
                        "Diff between source and target table: %s"
                        % (source_rowcount - target_rowcount)
                    )
                logging.info("%s rowcount: %s" % (validateTextSource, source_rowcount))
                logging.info("%s rowcount: %s" % (validateTextTarget, target_rowcount))
                logging.info("Validation diff allowed: %s" % (diffAllowed))
                logging.info("Upper limit: %s" % (upperValidationLimit))
                logging.info("Lower limit: %s" % (lowerValidationLimit))
                returnValue = False
            else:
                logging.info("%s validation successful!" % (validateText))
                logging.info("%s rowcount: %s" % (validateTextSource, source_rowcount))
                logging.info("%s rowcount: %s" % (validateTextTarget, target_rowcount))
                print()
                returnValue = True

        else:
            returnValue = True

        logging.debug("Executing import_config.validateRowCount() - Finished")
        return returnValue

    def getPKcolumns(self, PKforMerge=False, convertToLower=True):
        """Returns a comma seperated list of columns that is part of the PK"""
        logging.debug("Executing import_config.getPKcolumns()")
        returnValue = None

        # First fetch the override if it exists
        query = "select pk_column_override, pk_column_override_mergeonly from import_tables where table_id = %s "
        self.mysql_cursor01.execute(query, (self.table_id,))
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement))

        row = self.mysql_cursor01.fetchone()
        self.pk_column_override = row[0]
        self.pk_column_override_mergeonly = row[1]

        if self.importPhase == constant.IMPORT_PHASE_MSSQL_CHANGE_TRACKING:
            if (
                self.pk_column_override != None
                or self.pk_column_override_mergeonly != None
            ):
                logging.warning(
                    "MSSQL Change Tracking dont support overriding of PrimaryKeys. This configuration will be ignored"
                )
                self.pk_column_override = None
                self.pk_column_override_mergeonly = None

        if (
            self.pk_column_override != None
            and self.pk_column_override.strip() != ""
            and PKforMerge == False
        ):
            logging.debug("Executing import_config.getPKcolumns() - Finished")
            self.pk_column_override = re.sub(
                ", *", ",", self.pk_column_override.strip()
            )
            self.pk_column_override = re.sub(" *,", ",", self.pk_column_override)
            return self.pk_column_override.lower()

        if (
            self.pk_column_override_mergeonly != None
            and self.pk_column_override_mergeonly.strip() != ""
            and PKforMerge == True
        ):
            logging.debug("Executing import_config.getPKcolumns() - Finished")
            self.pk_column_override_mergeonly = re.sub(
                ", *", ",", self.pk_column_override_mergeonly.strip()
            )
            self.pk_column_override_mergeonly = re.sub(
                " *,", ",", self.pk_column_override_mergeonly
            )
            return self.pk_column_override_mergeonly.lower()

        elif (
            self.pk_column_override != None
            and self.pk_column_override.strip() != ""
            and PKforMerge == True
        ):
            logging.debug("Executing import_config.getPKcolumns() - Finished")
            self.pk_column_override = re.sub(
                ", *", ",", self.pk_column_override.strip()
            )
            self.pk_column_override = re.sub(" *,", ",", self.pk_column_override)
            return self.pk_column_override.lower()

        # If we reach this part, it means that we didnt override the PK. So lets fetch the real one
        query = "select c.source_column_name "
        query += "from import_tables t "
        query += "join import_columns c on t.table_id = c.table_id "
        query += "where t.table_id = %s and c.source_primary_key is not null and t.last_update_from_source = c.last_update_from_source "
        query += "order by c.source_primary_key "

        self.mysql_cursor01.execute(query, (self.table_id,))
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement))

        for row in self.mysql_cursor01.fetchall():
            if returnValue == None:
                returnValue = str(row[0])
            else:
                returnValue += "," + str(row[0])

            if convertToLower == True:
                returnValue = returnValue.lower()

        return returnValue

    def getForeignKeysFromConfig(
        self,
    ):
        """Reads the ForeignKeys from the configuration tables and return the result in a Pandas DF"""
        logging.debug("Executing import_config.getForeignKeysFromConfig()")
        result_df = None

        query = "select "
        query += "   s.hive_db as source_hive_db, "
        query += "   s.hive_table as source_hive_table, "
        query += "   lower(s.column_name) as source_column_name, "
        query += "   lower(s.source_column_name) as source_source_column_name, "
        query += "   refT.source_schema as ref_source_schema, "
        query += "   refT.source_table as ref_source_table, "
        query += "   ref.column_name as ref_column_name, "
        query += "   lower(ref.source_column_name) as ref_source_column_name, "
        query += "   ref.hive_db as ref_hive_db, "
        query += "   ref.hive_table as ref_hive_table, "
        query += "   fk.fk_index as fk_index, "
        query += "   fk.key_position as key_position "
        query += "from import_foreign_keys fk "
        query += "   join import_columns s on s.table_id = fk.table_id and s.column_id = fk.column_id "
        query += "   join import_columns ref on ref.table_id = fk.fk_table_id and ref.column_id = fk.fk_column_id "
        query += "   join import_tables refT on fk.fk_table_id = refT.table_id "
        query += "where fk.table_id = %s "
        query += "order by fk.fk_index, fk.key_position "

        self.mysql_cursor01.execute(query, (self.table_id,))
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement))

        if self.mysql_cursor01.rowcount == 0:
            return pd.DataFrame(
                columns=[
                    "fk_name",
                    "source_hive_db",
                    "source_hive_table",
                    "ref_hive_db",
                    "ref_hive_table",
                    "source_column_name",
                    "ref_column_name",
                ]
            )

        result_df = pd.DataFrame(self.mysql_cursor01.fetchall())

        # Set the correct column namnes in the DataFrame
        result_df_columns = []
        for columns in self.mysql_cursor01.description:
            result_df_columns.append(
                columns[0]
            )  # Name of the column is in the first position
        result_df.columns = result_df_columns

        result_df = (
            result_df.groupby(
                by=[
                    "fk_index",
                    "source_hive_db",
                    "source_hive_table",
                    "ref_hive_db",
                    "ref_hive_table",
                ]
            )
            .aggregate(
                {
                    "source_column_name": lambda a: ",".join(a),
                    "ref_column_name": lambda a: ",".join(a),
                }
            )
            .reset_index()
        )

        result_df.rename(columns={"fk_index": "fk_name"}, inplace=True)

        result_df["fk_name"] = (
            result_df["source_hive_db"]
            + "__"
            + result_df["source_hive_table"]
            + "__fk__"
            + result_df["ref_hive_table"]
            + "__"
            + result_df["source_column_name"].str.replace(",", "_")
        )

        logging.debug("Executing import_config.getForeignKeysFromConfig() - Finished")
        return result_df.sort_values(by=["fk_name"], ascending=True)

    def getAllActiveIncrImports(self):
        """Return all rows from import_stage that uses an incremental import_type"""
        logging.debug("Executing import_config.getAllActiveIncrImports()")
        result_df = None

        query = "select "
        query += "	stage.hive_db, "
        query += "	stage.hive_table, "
        query += "	stage.stage "
        query += "from import_stage stage "
        query += "join import_tables tabl "
        query += "	on stage.hive_db = tabl.hive_db and stage.hive_table = tabl.hive_table "
        query += (
            "where tabl.import_type like 'incr%' or tabl.import_phase_type = 'incr' "
        )

        self.mysql_cursor01.execute(query)
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement))

        if self.mysql_cursor01.rowcount == 0:
            return pd.DataFrame()

        result_df = pd.DataFrame(self.mysql_cursor01.fetchall())

        # Set the correct column namnes in the DataFrame
        result_df_columns = []
        for columns in self.mysql_cursor01.description:
            result_df_columns.append(
                columns[0]
            )  # Name of the column is in the first position
        result_df.columns = result_df_columns

        logging.debug("Executing import_config.getAllActiveIncrImports() - Finished")
        return result_df

    def generateSqoopSplitBy(self):
        """This function will try to generate a split-by"""
        logging.debug("Executing import_config.generateSqoopSplitBy()")

        self.splitByColumn = ""

        self.getPKcolumns()

        logging.debug("self.split_by_column: %s" % (self.split_by_column))
        logging.debug("self.sqoop_options: %s" % (self.sqoop_options))
        logging.debug("self.generatedPKcolumns: %s" % (self.generatedPKcolumns))
        logging.debug(
            "self.splitAddedToSqoopOptions: %s" % (self.splitAddedToSqoopOptions)
        )

        # self.splitAddedToSqoopOptions is needed as this code is executed many times during an import. And the --split-by is inserted into self.sqoop_options
        # and we dont want a warning for that when the code did it by it self

        if (
            self.split_by_column != None
            and self.split_by_column.strip() != ""
            and "split-by" in self.sqoop_options.lower()
            and self.splitAddedToSqoopOptions == False
        ):
            raise invalidConfiguration(
                "Specifying both a '--split-by' in sqoop_options column and a value in 'split_by_column' is not supported. Please remove the '--split-by' statement in sqoop_options and rerun the import."
            )

        if self.split_by_column != None and self.split_by_column.strip() != "":
            # self.split_by_column comes from configuration database
            self.splitByColumn = self.split_by_column

            if "split-by" not in self.sqoop_options.lower():
                if self.sqoop_options != "":
                    self.sqoop_options += " "
                self.sqoop_options += '--split-by "%s"' % (self.splitByColumn)
                self.splitAddedToSqoopOptions = True

        elif (
            "split-by" in self.sqoop_options.lower()
            and self.splitByColumn == ""
            and self.splitAddedToSqoopOptions == False
        ):
            # Try to read the column from the --split-by config in sqoop_options
            for id, value in enumerate(self.sqoop_options.split(" ")):
                if value == "--split-by":
                    logging.warning(
                        "Specifying the column to split on in 'sqoop_options' is deprecated. Please use 'split_by_column'"
                    )
                    self.splitByColumn = self.sqoop_options.split(" ")[id + 1].replace(
                        '"', ""
                    )

        elif self.generatedPKcolumns != None and self.generatedPKcolumns.strip() != "":
            # If there is a primary key, we use that one to split on. If multi column PK, we take the first column
            self.splitByColumn = self.generatedPKcolumns.split(",")[0]

            if "split-by" not in self.sqoop_options.lower():
                if self.sqoop_options != "":
                    self.sqoop_options += " "
                self.sqoop_options += '--split-by "%s"' % (
                    self.splitByColumn
                )  # This is needed, otherwise PK with space in them fail
                self.splitAddedToSqoopOptions = True

        elif self.pk_column_override != None and self.pk_column_override.strip() != "":
            self.splitByColumn = self.pk_column_override.split(",")[0]

            if "split-by" not in self.sqoop_options.lower():
                if self.sqoop_options != "":
                    self.sqoop_options += " "
                self.sqoop_options += '--split-by "%s"' % (
                    self.splitByColumn
                )  # This is needed, otherwise PK with space in them fail
                self.splitAddedToSqoopOptions = True

        logging.debug("Executing import_config.generateSqoopSplitBy() - Finished")

    def generateSqoopBoundaryQuery(self):
        logging.debug("Executing import_config.generateSqoopBoundaryQuery()")
        self.generateSqoopSplitBy()

        if self.splitByColumn != "":
            self.sqoopBoundaryQuery = "select min(%s), max(%s) from %s" % (
                self.splitByColumn,
                self.splitByColumn,
                self.common_config.getJDBCsqlFromTable(
                    schema=self.source_schema, table=self.source_table
                ),
            )

        logging.debug("SplitByColumn = %s" % (self.splitByColumn))
        logging.debug("sqoopBoundaryQuery = %s" % (self.sqoopBoundaryQuery))
        logging.debug("Executing import_config.generateSqoopBoundaryQuery() - Finished")

    def getMinMaxBoundaryValues(self):
        """Returns a dictionary with two values called min and max that contains the boundary values"""
        logging.debug("Executing import_config.getMinMaxBoundaryValues()")

        returnDict = {}
        if self.mongoImport == True:
            returnDict["min"] = None
            returnDict["max"] = None
            return returnDict

        self.generateSqoopBoundaryQuery()
        if self.sqoopBoundaryQuery == "":
            logging.info(
                "No columns to split on can be found. Forcing the number of sessions to 1"
            )
            self.sqlSessions = 1

            logging.debug(
                "self.sqoopBoundaryQuery is empty so generating a dummy MIN&MAX dict"
            )
            returnDict = {}
            returnDict["min"] = "Unknown"
            returnDict["max"] = "Unknown"
            return returnDict

        logging.info("Getting boundary MIN and MAX values")
        logging.debug(self.sqoopBoundaryQuery)
        minMaxValues = self.common_config.executeJDBCquery(self.sqoopBoundaryQuery)

        minValue = minMaxValues.iloc[0].values[0]
        maxValue = minMaxValues.iloc[0].values[1]

        if isinstance(minValue, np.float64) == True:
            if minValue < 0 and maxValue > 0:
                minValue = np.int64(minValue) - 1
            else:
                minValue = np.int64(minValue)

        if isinstance(maxValue, np.float64) == True:
            if maxValue < 0:
                maxValue = np.int64(maxValue) - 1
            else:
                maxValue = np.int64(maxValue) + 1

        returnDict["min"] = minValue
        returnDict["max"] = maxValue

        logging.debug("Executing import_config.getMinMaxBoundaryValues() - Finished")
        return returnDict

    def getImportTables(self, hiveDB):
        """Return all tables that we import to a specific Hive DB"""
        logging.debug("Executing import_config.getImportTables()")
        result_df = None

        query = "select "
        query += "	hive_db, "
        query += "	hive_table, "
        query += "	dbalias, "
        query += "	source_schema, "
        query += "	source_table "
        query += "from import_tables "
        query += "where hive_db = %s"

        self.mysql_cursor01.execute(query, (hiveDB,))
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement))

        if self.mysql_cursor01.rowcount == 0:
            return pd.DataFrame(
                columns=[
                    "hive_db",
                    "hive_table",
                    "dbalias",
                    "source_schema",
                    "source_table",
                ]
            )

        result_df = pd.DataFrame(self.mysql_cursor01.fetchall())

        # Set the correct column namnes in the DataFrame
        result_df_columns = []
        for columns in self.mysql_cursor01.description:
            result_df_columns.append(
                columns[0]
            )  # Name of the column is in the first position
        result_df.columns = result_df_columns

        logging.debug("Executing import_config.getImportTables() - Finished")
        return result_df

    def addImportTable(self, hiveDB, hiveTable, dbalias, schema, table, catalog):
        """Add source table to import_tables"""
        logging.debug("Executing import_config.addImportTable()")
        returnValue = True

        if catalog == constant.CATALOG_HIVE_DIRECT:
            query = (
                "insert into import_tables "
                "("
                "    hive_db, "
                "    hive_table, "
                "    dbalias, "
                "    source_schema, "
                "    source_table,  "
                "    etl_engine  "
                ") values ( %s, %s, %s, %s, %s, 'hive' )"
            )
        elif catalog == constant.CATALOG_GLUE:
            query = (
                "insert into import_tables "
                "("
                "    hive_db, "
                "    hive_table, "
                "    dbalias, "
                "    source_schema, "
                "    source_table,  "
                "    etl_engine  "
                ") values ( %s, %s, %s, %s, %s, 'spark' )"
            )
        else:
            raise invalidConfiguration(
                "catalog option supplied to import_config.addImportTable does not contain a valid option"
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
                "Hive table %s.%s cant be added as it's not unique in the configuration database"
                % (hiveDB, hiveTable)
            )
            returnValue = False

        logging.debug("Executing import_config.addImportTable() - Finished")
        return returnValue

    def isExternalViewRequired(self):
        logging.debug("Executing import_config.isExternalViewRequired()")
        returnValue = False

        if self.importTool == "spark":
            returnValue = False

        elif self.generatedSqoopOptions != None:
            returnValue = True

        logging.debug("Executing import_config.isExternalViewRequired() - Finished")
        return returnValue

    def getSelectForImportView(self):
        logging.debug("Executing import_config.getSelectForImportView()")

        query = "select c.column_name as name, c.column_name_override, c.column_type as type, c.sqoop_column_type, c.sqoop_column_type_override "
        query += "from import_tables t "
        query += "join import_columns c on t.table_id = c.table_id "
        query += "where t.table_id = %s and t.last_update_from_source = c.last_update_from_source "
        query += "order by c.column_order"

        self.mysql_cursor01.execute(query, (self.table_id,))
        logging.debug("SQL Statement executed: \n%s" % (self.mysql_cursor01.statement))

        selectQuery = ""

        for row in self.mysql_cursor01.fetchall():
            columnName = row[0]
            columnNameOverride = row[1]
            columnType = row[2]
            sqoopColumnType = row[3]
            sqoopColumnTypeOverride = row[4]

            if columnNameOverride != None:
                columnName = columnNameOverride
            columnName = self.convertHiveColumnNameReservedWords(columnName)
            columnName = self.getParquetColumnName(columnName)

            if sqoopColumnType != None and sqoopColumnType.strip() == "":
                sqoopColumnType = None

            if (
                sqoopColumnTypeOverride != None
                and sqoopColumnTypeOverride.strip() == ""
            ):
                sqoopColumnTypeOverride = None

            if selectQuery != "":
                selectQuery += ", "

            if sqoopColumnType == None and sqoopColumnTypeOverride == None:
                selectQuery += "S.`%s`" % (columnName)
            else:
                selectQuery += "cast(S.`%s` as %s) `%s`" % (
                    columnName,
                    columnType,
                    columnName,
                )

            # Handle reserved column names in Hive

            # TODO: Get the maximum column comment size from Hive Metastore and use that instead
        selectQuery = "select %s" % (selectQuery)

        if self.importPhase == constant.IMPORT_PHASE_ORACLE_FLASHBACK:
            # Add the specific Oracle FlashBack columns that we need to import
            selectQuery += ", S.`datalake_flashback_operation`"
            selectQuery += ", cast(S.`datalake_flashback_startscn` as bigint) datalake_flashback_startscn"

        selectQuery += " from `%s`.`%s` as S " % (
            self.Hive_Import_DB,
            self.Hive_Import_Table,
        )

        if self.importPhase == constant.IMPORT_PHASE_ORACLE_FLASHBACK:
            selectQuery += "inner join ("
            selectQuery += (
                "   select %s, max(datalake_flashback_startscn) as max_startscn "
                % (self.getPKcolumns())
            )
            selectQuery += "   from `%s`.`%s` " % (
                self.Hive_Import_DB,
                self.Hive_Import_Table,
            )
            selectQuery += "   group by %s) G " % (self.getPKcolumns())
            selectQuery += "on "

            firstRow = True
            for PKcolumn in self.getPKcolumns().split(","):
                if firstRow == True:
                    selectQuery += "   S.%s = G.%s" % (PKcolumn, PKcolumn)
                    firstRow = False
                else:
                    selectQuery += "   and S.%s = G.%s" % (PKcolumn, PKcolumn)
            selectQuery += "   and ( S.datalake_flashback_startscn = G.max_startscn or G.max_startscn is NULL )"

        logging.debug("Executing import_config.getSelectForImportView() - Finished")
        return selectQuery

    def resetIncrMinMaxValues(self, maxValue):
        logging.debug("Executing import_config.resetIncrMinMaxValues()")
        if maxValue != None:
            logging.info(
                "Reseting incremental values for import. New max value is: %s"
                % (maxValue)
            )
        else:
            logging.info("Reseting incremental values for import.")

        query = "update import_tables set "
        query += "  incr_minvalue = NULL, "
        query += "  incr_maxvalue = %s, "
        query += "  incr_minvalue_pending = NULL, "
        query += "  incr_maxvalue_pending = NULL "
        query += "where table_id = %s "

        self.mysql_cursor01.execute(query, (maxValue, self.table_id))
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement))
        self.mysql_conn.commit()

        logging.debug("Executing import_config.resetIncrMinMaxValues() - Finished")

    def invalidateImpala(self):
        """Connects to Impala and invalidates the metadata"""
        logging.debug("Executing import_config.invalidateImpala()")

        if self.invalidateImpalaMetadata == False:
            return

        self.common_config.connectToImpala()

        # TODO: Add invalidation of history table as well
        # self.import_with_history_table = True
        logging.info("Invalidating Impala table")
        query = "invalidate metadata `%s`.`%s`" % (self.Hive_DB, self.Hive_Table)
        self.common_config.executeImpalaQuery(query)

        self.common_config.disconnectFromImpala()

        logging.debug("Executing import_config.invalidateImpala() - Finished")
