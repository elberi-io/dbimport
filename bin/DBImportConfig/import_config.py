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
import math
import re
from datetime import datetime

import mysql.connector
import numpy as np
import pandas as pd

from common import constants as constant
from common.Exceptions import SQLerror, invalidConfiguration, validationError
from common.Singleton import Singleton
from DBImportConfig import common_config, sendStatistics
from DBImportConfig import import_stage as stage


class config(metaclass=Singleton):
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
        self.print_sql_where_addition = True
        self.invalidate_impala_metadata = False

        self.validationMethod = None
        self.validationCustomQuerySourceSQL = None
        self.validationCustomQueryHiveSQL = None
        self.validationCustomQuerySourceValue = None
        self.validationCustomQueryHiveValue = None
        self.validationCustomQueryValidateImportTable = True

        self.import_database_override = None
        self.import_table_override = None
        self.history_database_override = None
        self.history_table_override = None

        self.importPhase = None
        self.importPhaseDescription = None
        self.copyPhase = None
        self.copy_phase_description = None
        self.etlPhase = None
        self.etlPhaseDescription = None

        self.phase_three = None
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

    def reconnect_config_database(self):
        # During long imports, depending on connection timout, the connection against the config database might be closed. This checks if it is closed and reconnect if needed
        if self.mysql_conn.is_connected() == False:
            self.mysql_conn.close()
            self.mysql_cursor01.close()
            self.mysql_cursor02.close()

            self.common_config.reconnect_config_database()

            self.mysql_conn = self.common_config.mysql_conn
            self.mysql_cursor01 = self.mysql_conn.cursor(buffered=True)
            self.mysql_cursor02 = self.mysql_conn.cursor(buffered=True)

            self.stage.set_mysql_connection(self.mysql_conn)

    def set_hive_table(self, Hive_DB, Hive_Table):
        """Sets the parameters to work against a new Hive database and table"""
        self.Hive_DB = Hive_DB.lower()
        self.Hive_Table = Hive_Table.lower()

        self.common_config.set_hive_table(Hive_DB, Hive_Table)
        self.stage.set_hive_table(Hive_DB, Hive_Table)

    def log_hive_column_add(
        self, column, columnType, description=None, hiveDB=None, hiveTable=None
    ):
        self.common_config.log_hive_column_add(
            column=column,
            columnType=columnType,
            description=description,
            hiveDB=hiveDB,
            hiveTable=hiveTable,
        )

    def log_hive_column_type_change(
        self,
        column,
        columnType,
        previous_columnType=None,
        description=None,
        hiveDB=None,
        hiveTable=None,
    ):
        self.common_config.log_hive_column_type_change(
            column,
            columnType,
            previous_columnType=previous_columnType,
            description=description,
            hiveDB=hiveDB,
            hiveTable=hiveTable,
        )

    def log_hive_column_rename(
        self,
        columnName,
        previous_columnName,
        description=None,
        hiveDB=None,
        hiveTable=None,
    ):
        self.common_config.log_hive_column_rename(
            columnName,
            previous_columnName,
            description=description,
            hiveDB=hiveDB,
            hiveTable=hiveTable,
        )

    def remove_temporary_files(self):
        self.common_config.remove_temporary_files()

    def set_stage(self, stage, force=False):
        self.stage.set_stage(stage, force=force)

    def get_stage(self):
        return self.stage.get_stage()

    def get_stage_short_name(self, stage):
        return self.stage.get_stage_short_name(stage)

    def clear_stage(self):
        self.stage.clear_stage()

    def save_retry_attempt(self, stage):
        self.stage.save_retry_attempt(stage)

    def set_stage_only_in_memory(self):
        self.stage.set_stage_only_in_memory()

    def send_start_json(self):
        """Sends a start JSON document to REST and/or Kafka"""
        logging.debug("Executing import_config.send_start_json()")

        self.postDataToREST = self.common_config.get_config_value(
            key="post_data_to_rest"
        )
        self.postDataToKafka = self.common_config.get_config_value(
            key="post_data_to_kafka"
        )
        self.post_data_to_aws_sns = self.common_config.get_config_value(
            key="post_data_to_awssns"
        )

        if (
            self.postDataToREST == False
            and self.postDataToKafka == False
            and self.post_data_to_aws_sns == False
        ):
            return

        import_stop = None
        json_data = {}
        json_data["type"] = "import"
        json_data["status"] = "started"
        json_data["database"] = self.Hive_DB
        json_data["table"] = self.Hive_Table
        if self.import_with_history_table == True:
            json_data["history_database"] = self.Hive_History_DB
            json_data["history_table"] = self.Hive_History_Table
        else:
            json_data["history_database"] = None
            json_data["history_table"] = None
        json_data["import_phase"] = self.importPhase
        json_data["copy_phase"] = self.copyPhase
        json_data["etl_phase"] = self.etlPhase
        json_data["incremental"] = self.import_is_incremental
        json_data["history"] = self.import_with_history_table
        json_data["dbalias"] = self.connection_alias
        json_data["source_database"] = self.common_config.jdbc_database
        json_data["source_schema"] = self.source_schema
        json_data["source_table"] = self.source_table

        if self.postDataToKafka == True:
            result = self.sendStatistics.publish_kafka_data(json.dumps(json_data))
            if result == False:
                logging.warning("Kafka publish failed! No start message posted")

        if self.postDataToREST == True:
            response = self.sendStatistics.send_rest_data(json.dumps(json_data))
            if response != 200:
                logging.warn("REST call failed! No start message posted")

        if self.post_data_to_aws_sns == True:
            result = self.sendStatistics.send_aws_sns_data(json.dumps(json_data))
            if result == False:
                logging.warning("AWS SNS publish failed! No start message posted")

        logging.debug("Executing import_config.send_start_json() - Finished")

    def save_stage_statistics(self):
        self.stage.save_stage_statistics(
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

    def convert_stage_statistics_to_json(self):

        if self.import_with_history_table == True:
            history_db_for_json = self.Hive_History_DB
            history_table_for_json = self.Hive_History_Table
        else:
            history_db_for_json = None
            history_table_for_json = None

        self.stage.convert_stage_statistics_to_json(
            database=self.Hive_DB,
            table=self.Hive_Table,
            history_database=history_db_for_json,
            history_table=history_table_for_json,
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

    def lookup_connection_alias(self, connection_alias=None):

        if connection_alias == None:
            connection_alias = self.connection_alias

        # If this is called before getImportConfig is called, we dont know if it's a copy table or not. So we need to set this to false
        # This happens during table discovery for ./manage --addImportTable
        if self.copy_slave == None:
            self.copy_slave = False

        self.common_config.lookup_connection_alias(
            connection_alias, copySlave=self.copy_slave
        )

        # Save if the source is a MongoDB database to a local parameter
        self.mongoImport = self.common_config.db_mongodb

    def check_time_window(self):
        self.common_config.check_time_window(self.connection_alias)

    def get_jdbc_table_definition(self):
        self.common_config.get_jdbc_table_definition(
            self.source_schema, self.source_table
        )

    def get_import_config(self):
        logging.debug("Executing import_config.get_import_config()")

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

        invalidate_impala_metadata_value = row[49]

        self.import_database_override = row[50]
        self.import_table_override = row[51]
        self.history_database_override = row[52]
        self.history_table_override = row[53]

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

        self.hdfsBaseDir = self.common_config.get_config_value(key="hdfs_basedir")

        # Set various sqoop variables
        # changing the HDFS path also requires you to change it in copy_operation.py under copy_data_to_destinations()
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
            valid_combination = False
            if (
                self.import_phase_type == "full"
                and self.etl_phase_type == "truncate_insert"
            ):
                valid_combination = True

            if self.import_phase_type == "full" and self.etl_phase_type == "insert":
                valid_combination = True

            if self.import_phase_type == "full" and self.etl_phase_type == "merge":
                valid_combination = True

            if (
                self.import_phase_type == "full"
                and self.etl_phase_type == "merge_history_audit"
            ):
                valid_combination = True

            if self.import_phase_type == "full" and self.etl_phase_type == "none":
                valid_combination = True

            if self.import_phase_type == "full" and self.etl_phase_type == "external":
                valid_combination = True

            if self.import_phase_type == "incr" and self.etl_phase_type == "insert":
                valid_combination = True

            if self.import_phase_type == "incr" and self.etl_phase_type == "merge":
                valid_combination = True

            if (
                self.import_phase_type == "incr"
                and self.etl_phase_type == "merge_history_audit"
            ):
                valid_combination = True

            if (
                self.import_phase_type == "oracle_flashback"
                and self.etl_phase_type == "merge"
            ):
                valid_combination = True

            if (
                self.import_phase_type == "oracle_flashback"
                and self.etl_phase_type == "merge_history_audit"
            ):
                valid_combination = True

            if (
                self.import_phase_type == "mssql_change_tracking"
                and self.etl_phase_type == "merge"
            ):
                valid_combination = True

            if (
                self.import_phase_type == "mssql_change_tracking"
                and self.etl_phase_type == "merge_history_audit"
            ):
                valid_combination = True

            if valid_combination == False:
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
        self.lookup_connection_alias()

        # Check each import type and set correct phase and description
        etl_engine_spark_supported = False

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
                self.copy_phase_description = "No cluster copy"
                self.etlPhaseDescription = "Truncate and insert"
                etl_engine_spark_supported = True
            else:
                self.import_type_description = "Full import of Mongo collection to Hive"
                self.importPhase = constant.IMPORT_PHASE_MONGO_FULL
                self.copyPhase = constant.COPY_PHASE_NONE
                self.etlPhase = constant.ETL_PHASE_TRUNCATEINSERT
                self.importPhaseDescription = "Full"
                self.copy_phase_description = "No cluster copy"
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
            self.copy_phase_description = "No cluster copy"
            self.etlPhaseDescription = "Append"
            etl_engine_spark_supported = True

        if self.import_type == "full_merge_direct" or (
            self.import_phase_type == "full" and self.etl_phase_type == "merge"
        ):
            self.import_type_description = "Full import and merge of Hive table"
            self.importPhase = constant.IMPORT_PHASE_FULL
            self.copyPhase = constant.COPY_PHASE_NONE
            self.etlPhase = constant.ETL_PHASE_MERGEONLY
            self.importPhaseDescription = "Full"
            self.copy_phase_description = "No cluster copy"
            self.etlPhaseDescription = "Merge"
            etl_engine_spark_supported = True

        if self.import_phase_type == "full" and self.etl_phase_type == "none":
            self.import_type_description = "Full import with no ETL phace"
            self.importPhase = constant.IMPORT_PHASE_FULL
            self.copyPhase = constant.COPY_PHASE_NONE
            self.etlPhase = constant.ETL_PHASE_NONE
            self.importPhaseDescription = "Full"
            self.copy_phase_description = "No cluster copy"
            self.etlPhaseDescription = "No ETL phase"
            etl_engine_spark_supported = True

        if self.import_phase_type == "full" and self.etl_phase_type == "external":
            self.import_type_description = "Full import with no ETL phase. Will only create the target table as an external table"
            self.importPhase = constant.IMPORT_PHASE_FULL
            self.copyPhase = constant.COPY_PHASE_NONE
            self.etlPhase = constant.ETL_PHASE_EXTERNAL
            self.importPhaseDescription = "Full"
            self.copy_phase_description = "No cluster copy"
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
            self.copy_phase_description = "No cluster copy"
            self.etlPhaseDescription = "Merge History Audit"
            etl_engine_spark_supported = True

        if self.import_type == "incr_merge_direct" or (
            self.import_phase_type == "incr" and self.etl_phase_type == "merge"
        ):
            self.import_type_description = "Incremental & merge import of Hive table"
            self.importPhase = constant.IMPORT_PHASE_INCR
            self.copyPhase = constant.COPY_PHASE_NONE
            self.etlPhase = constant.ETL_PHASE_MERGEONLY
            self.importPhaseDescription = "Incremental"
            self.copy_phase_description = "No cluster copy"
            self.etlPhaseDescription = "Merge"
            etl_engine_spark_supported = True

        if self.import_type == "incr_merge_direct_history" or (
            self.import_phase_type == "incr"
            and self.etl_phase_type == "merge_history_audit"
        ):
            self.import_type_description = "Incremental & merge import of Hive table. Will also create a History table"
            self.importPhase = constant.IMPORT_PHASE_INCR
            self.copyPhase = constant.COPY_PHASE_NONE
            self.etlPhase = constant.ETL_PHASE_MERGEHISTORYAUDIT
            self.importPhaseDescription = "Incremental"
            self.copy_phase_description = "No cluster copy"
            self.etlPhaseDescription = "Merge History Audit"
            etl_engine_spark_supported = True

        if self.import_type == "incr" or (
            self.import_phase_type == "incr" and self.etl_phase_type == "insert"
        ):
            self.import_type_description = "Incremental import of Hive table"
            self.importPhase = constant.IMPORT_PHASE_INCR
            self.copyPhase = constant.COPY_PHASE_NONE
            self.etlPhase = constant.ETL_PHASE_INSERT
            self.importPhaseDescription = "Incremental"
            self.copy_phase_description = "No cluster copy"
            self.etlPhaseDescription = "Insert"
            etl_engine_spark_supported = True

        if self.import_type == "oracle_flashback_merge" or (
            self.import_phase_type == "oracle_flashback"
            and self.etl_phase_type == "merge"
        ):
            self.import_type_description = "Import with the help of Oracle Flashback"
            self.importPhase = constant.IMPORT_PHASE_ORACLE_FLASHBACK
            self.copyPhase = constant.COPY_PHASE_NONE
            self.etlPhase = constant.ETL_PHASE_MERGEONLY
            self.importPhaseDescription = "Oracle Flashback"
            self.copy_phase_description = "No cluster copy"
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
            self.copy_phase_description = "No cluster copy"
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
            self.copy_phase_description = "No cluster copy"
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
            self.copy_phase_description = "No cluster copy"
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
            and etl_engine_spark_supported == False
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
            if (
                self.common_config.get_config_value(key="hive_insert_only_tables")
                == True
            ):
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
            self.sparkMaxExecutors = sql_sessions_max_from_config = int(
                self.common_config.get_config_value(key="spark_max_executors")
            )

        # Determine if we are going to invalidate the metadata in Impala or not
        if invalidate_impala_metadata_value == 0:
            self.invalidate_impala_metadata = False
        elif invalidate_impala_metadata_value == 1:
            self.invalidate_impala_metadata = True
        else:
            self.invalidate_impala_metadata = self.common_config.get_config_value(
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

        import_work_db_config = self.common_config.get_config_value(
            key="import_work_database"
        )
        import_work_table_config = self.common_config.get_config_value(
            key="import_work_table"
        )
        import_staging_db_config = self.common_config.get_config_value(
            key="import_staging_database"
        )
        import_staging_table_config = self.common_config.get_config_value(
            key="import_staging_table"
        )
        import_history_db_config = self.common_config.get_config_value(
            key="import_history_database"
        )
        import_history_table_config = self.common_config.get_config_value(
            key="import_history_table"
        )

        # Set the name of the history tables, temporary tables and such
        if import_work_db_config.startswith("r/"):
            import_work_db = re.sub(
                import_work_db_config.split("/")[1],
                import_work_db_config.split("/")[2],
                self.Hive_DB,
            )
        else:
            import_work_db = import_work_db_config.replace("{DATABASE}", self.Hive_DB)

        if import_work_table_config.startswith("r/"):
            import_work_table = re.sub(
                import_work_table_config.split("/")[1],
                import_work_table_config.split("/")[2],
                self.Hive_DB,
            )
        else:
            import_work_table = import_work_table_config.replace(
                "{DATABASE}", self.Hive_DB
            ).replace("{TABLE}", self.Hive_Table)

        if import_staging_db_config.startswith("r/"):
            import_staging_db = re.sub(
                import_staging_db_config.split("/")[1],
                import_staging_db_config.split("/")[2],
                self.Hive_DB,
            )
        else:
            import_staging_db = import_staging_db_config.replace(
                "{DATABASE}", self.Hive_DB
            )

        if import_staging_table_config.startswith("r/"):
            import_staging_table = re.sub(
                import_staging_table_config.split("/")[1],
                import_staging_table_config.split("/")[2],
                self.Hive_DB,
            )
        else:
            import_staging_table = import_staging_table_config.replace(
                "{DATABASE}", self.Hive_DB
            ).replace("{TABLE}", self.Hive_Table)

        if import_history_db_config.startswith("r/"):
            import_history_db = re.sub(
                import_history_db_config.split("/")[1],
                import_history_db_config.split("/")[2],
                self.Hive_DB,
            )
        else:
            import_history_db = import_history_db_config.replace(
                "{DATABASE}", self.Hive_DB
            )

        if import_history_table_config.startswith("r/"):
            import_history_table = re.sub(
                import_history_table_config.split("/")[1],
                import_history_table_config.split("/")[2],
                self.Hive_DB,
            )
        else:
            import_history_table = import_history_table_config.replace(
                "{DATABASE}", self.Hive_DB
            ).replace("{TABLE}", self.Hive_Table)

        self.Hive_Import_DB = import_staging_db
        self.Hive_Import_Table = import_staging_table
        self.Hive_Import_View = import_staging_table + "_view"
        self.Hive_History_DB = import_history_db
        self.Hive_History_Table = import_history_table
        self.Hive_HistoryTemp_DB = import_work_db
        self.Hive_HistoryTemp_Table = import_work_table + "__temporary"
        self.Hive_Import_PKonly_DB = import_work_db
        self.Hive_Import_PKonly_Table = import_work_table + "__pkonly__staging"
        self.Hive_Delete_DB = import_work_db
        self.Hive_Delete_Table = import_work_table + "__pkonly__deleted"

        if self.etlPhase == constant.ETL_PHASE_EXTERNAL:
            self.Hive_Import_DB = self.Hive_DB
            self.Hive_Import_Table = self.Hive_Table

        # If the database or table for import or history have been overridden by setting the options in the import_table
        if self.import_database_override != None:
            self.Hive_Import_DB = self.import_database_override

        if self.import_table_override != None:
            self.Hive_Import_Table = self.import_table_override
            self.Hive_Import_View = self.import_table_override + "_view"

        if self.history_database_override != None:
            self.Hive_History_DB = self.history_database_override

        if self.history_table_override != None:
            self.Hive_History_Table = self.history_table_override

        self.Hive_ColumnName_Import = self.common_config.get_config_value(
            key="import_columnname_import"
        )
        self.Hive_ColumnName_Insert = self.common_config.get_config_value(
            key="import_columnname_insert"
        )
        self.Hive_ColumnName_Update = self.common_config.get_config_value(
            key="import_columnname_update"
        )
        self.Hive_ColumnName_Delete = self.common_config.get_config_value(
            key="import_columnname_delete"
        )
        self.Hive_ColumnName_IUD = self.common_config.get_config_value(
            key="import_columnname_iud"
        )
        self.Hive_ColumnName_HistoryTimestamp = self.common_config.get_config_value(
            key="import_columnname_histtime"
        )
        self.Hive_ColumnName_Source = self.common_config.get_config_value(
            key="import_columnname_source"
        )

        logging.debug("Settings from import_config.get_import_config()")
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
        logging.debug("Executing import_config.get_import_config() - Finished")

    def update_last_update_from_source(self):
        # This function will update the import_tables.last_update_from_source to the startDate from the common class. This will later
        # be used to determine if all the columns exists in the source system as we set the same datatime on all columns
        # that we read from the source system
        logging.debug("")
        logging.debug("Executing import_config.update_last_update_from_source()")

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

        logging.debug(
            "Executing import_config.update_last_update_from_source() - Finished"
        )

    def save_incr_min_value(self):
        """Save the min value in the pendings column"""
        logging.debug("Executing import_config.savePendingIncrValues()")

        query = "update import_tables set "
        query += "	incr_minvalue_pending = incr_maxvalue "
        query += "where table_id = %s"

        self.mysql_cursor01.execute(query, (self.table_id,))
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement))
        self.mysql_conn.commit()

        logging.debug("Executing import_config.savePendingIncrValues() - Finished")

    def save_incr_pending_values(self):
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

    def remove_fk_for_table(self):
        # This function will remove all ForeignKey definitions for the current table.
        logging.debug("")
        logging.debug("Executing import_config.remove_fk_for_table()")

        query = "delete from import_foreign_keys where table_id = %s"
        logging.debug("")
        logging.debug("Deleting FK's from import_foreign_keys")
        self.mysql_cursor01.execute(query, (self.table_id,))
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement))
        self.mysql_conn.commit()

        logging.debug("Executing import_config.remove_fk_for_table() - Finished")

    def strip_unwanted_char_comment(self, work_string):
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

    def strip_unwanted_char_column_name(self, work_string):
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

    def get_column_force_string(self, column_name):
        logging.debug("Executing import_config.get_column_force_string()")
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

        logging.debug("force_string_connection: %s" % (force_string_connection))
        logging.debug("force_string_table: %s" % (force_string_table))
        logging.debug("force_string_column: %s" % (force_string_column))
        logging.debug("forceString: %s" % (force_string))
        logging.debug("Executing import_config.get_column_force_string() - Finished")
        return force_string

    def convert_hive_column_name_reserved_words(self, column_name):
        if column_name.lower() in ("synchronized", "date", "interval", "int"):
            column_name = "%s_hive" % (column_name)

        return column_name

    def is_any_column_anonymized(
        self,
    ):
        column_df = self.get_anonymization_function()

        if column_df.empty == True:
            return False
        else:
            return True

    def get_anonymization_function(
        self,
    ):
        """Reads all columns and returns a dict with column name and anonymization function"""
        logging.debug("Executing import_config.get_anonymization_function()")

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

        logging.debug("Executing import_config.get_anonymization_function() - Finished")
        return result_df

    def save_column_data(
        self,
    ):
        """This is one of the main functions when it comes to source system schemas. This will parse the output from the Python Schema Program
        and insert/update the data in the import_columns table. This also takes care of column type conversions if it's needed"""
        logging.debug("")
        logging.debug("Executing import_config.save_column_data()")
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
        column_order = 0
        self.sqoop_use_generated_sql = False
        column_name_reserved = False

        for _index, row in self.common_config.source_columns_df.iterrows():
            column_name = self.strip_unwanted_char_column_name(
                row["SOURCE_COLUMN_NAME"]
            )
            if self.mongoImport == False:
                column_type = row["SOURCE_COLUMN_TYPE"].lower()
            else:
                column_type = row["SOURCE_COLUMN_TYPE"]
            source_column_type = column_type
            source_column_name = row["SOURCE_COLUMN_NAME"]
            source_column_comment = self.strip_unwanted_char_comment(
                row["SOURCE_COLUMN_COMMENT"]
            )
            self.table_comment = self.strip_unwanted_char_comment(row["TABLE_COMMENT"])

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

            include_column_in_import = True
            column_id = None
            column_type_override = None
            sqoop_column_type_override = None
            anonymization_function = "None"

            column_row = self.mysql_cursor01.fetchone()
            if column_row != None:
                column_id = column_row[0]
                if column_row[1] == 0:
                    include_column_in_import = False
                if column_row[2] != None and column_row[2].strip() != "":
                    column_name = column_row[2].strip().lower()
                if column_row[3] != None and column_row[3].strip() != "":
                    column_type_override = column_row[3].strip().lower()
                if column_row[4] != None and column_row[4].strip() != "":
                    sqoop_column_type_override = (
                        column_row[4].strip().lower().capitalize()
                    )

                anonymization_function = column_row[5]

            # Handle reserved column names in Hive
            column_name = self.convert_hive_column_name_reserved_words(column_name)

            # TODO: Get the maximum column comment size from Hive Metastore and use that instead
            if source_column_comment != None and len(source_column_comment) > 256:
                source_column_comment = source_column_comment[:256]

            column_order += 1

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

                column_type = re.sub(r"^nvarchar\(", "varchar(", column_type)
                column_type = re.sub("^datetime$", "timestamp", column_type)
                column_type = re.sub("^datetime2$", "timestamp", column_type)
                column_type = re.sub("^smalldatetime$", "timestamp", column_type)
                column_type = re.sub("^bit$", "tinyint", column_type)
                column_type = re.sub(r"^binary\([0-9]*\)$", "binary", column_type)
                column_type = re.sub("^varbinary$", "binary", column_type)
                column_type = re.sub(r"^varbinary\([0-9]*\)$", "binary", column_type)
                column_type = re.sub("^geometry$", "binary", column_type)
                column_type = re.sub(r"^geography\(-1\)$", "binary", column_type)
                column_type = re.sub("^image$", "binary", column_type)
                column_type = re.sub("^money$", "decimal", column_type)
                column_type = re.sub(r"^nchar\(", "char(", column_type)
                column_type = re.sub(r"^numeric\(", "decimal(", column_type)
                column_type = re.sub("^real$", "float", column_type)
                column_type = re.sub(r"^varchar\(-1\)$", "string", column_type)
                column_type = re.sub(r"^varchar\(65355\)$", "string", column_type)
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

                column_type = re.sub(r"^nvarchar\(", "varchar(", column_type)
                column_type = re.sub(r" char\)$", ")", column_type)
                column_type = re.sub(r" byte\)$", ")", column_type)
                column_type = re.sub(r"^char\(0\)$", "char(4000)", column_type)
                column_type = re.sub(r"^varchar2\(0\)$", "varchar(4000)", column_type)
                column_type = re.sub(r"^varchar2\(", "varchar(", column_type)
                column_type = re.sub(r"^nvarchar2\(", "varchar(", column_type)
                column_type = re.sub(r"^rowid\(", "varchar(", column_type)
                column_type = re.sub("^number$", "decimal(38,19)", column_type)
                if re.search(r"^number\([0-9]\)", column_type):
                    column_type = "int"
                    sqoop_column_type = "Integer"
                if re.search(r"^number\(1[0-8]\)", column_type):
                    column_type = "bigint"
                    sqoop_column_type = "String"
                column_type = re.sub(r"^number\(", "decimal(", column_type)
                column_type = re.sub("^date$", "timestamp", column_type)
                column_type = re.sub(
                    r"^timestamp\([0-9]*\) with time zone", "timestamp", column_type
                )
                column_type = re.sub("^blob$", "binary", column_type)
                column_type = re.sub(r"^xmltype\([0-9]*\)$", "string", column_type)
                column_type = re.sub("^raw$", "binary", column_type)
                column_type = re.sub(r"^raw\([0-9]*\)$", "binary", column_type)
                column_type = re.sub(r"^timestamp\([0-9]\)", "timestamp", column_type)
                column_type = re.sub(r"^long raw\([0-9]*\)$", "string", column_type)
                column_type = re.sub(
                    r"^decimal\(3,4\)", "decimal(8,4)", column_type
                )  # Very stange Oracle type number(3,4), how can pricision be smaller than scale?
                if re.search(r"^decimal\([0-9][0-9]\)$", column_type) != None:
                    column_type = re.sub(r"\)$", ",0)", column_type)

            if self.common_config.db_mysql == True:
                if source_column_type == "bit":
                    column_type = "int"
                    sqoop_column_type = "Integer"

                if source_column_type in ("tinyint", "smallint", "mediumint"):
                    column_type = "int"
                    sqoop_column_type = "Integer"

                if re.search(r"^enum\(", column_type):
                    column_type = "string"
                    sqoop_column_type = "String"

                column_type = re.sub(r"^character varying\(", "varchar(", column_type)
                column_type = re.sub(r"^mediumtext\([0-9]*\)", "string", column_type)
                column_type = re.sub(
                    r"^tinytext\([0-9]*\)", "varchar(255)", column_type
                )
                column_type = re.sub(r"^text\([0-9]*\)", "string", column_type)
                column_type = re.sub("^json$", "string", column_type)
                column_type = re.sub("^datetime$", "timestamp", column_type)
                column_type = re.sub("^varbinary$", "binary", column_type)
                column_type = re.sub(r"^varbinary\([0-9]*\)$", "binary", column_type)
                column_type = re.sub(r"^binary\([0-9]*\)$", "binary", column_type)
                column_type = re.sub(r"^blob\([0-9]*\)$", "binary", column_type)
                if column_type == "time":
                    column_type = "string"
                    sqoop_column_type = "String"

            if self.common_config.db_postgresql == True:
                column_type = re.sub(r"^character varying\(", "varchar(", column_type)
                column_type = re.sub("^text", "string", column_type)

            if self.common_config.db_progress == True:
                column_type = re.sub("^integer", "int", column_type)
                column_type = re.sub(r"^numeric\(", "decimal(", column_type)
                column_type = re.sub(r"^date\([0-9]\)", "date", column_type)
                column_type = re.sub(r"^bit\([1]\)", "boolean", column_type)
                column_type = re.sub(r",none\)", ",0)", column_type)

            if self.common_config.db_db2udb == True:
                if re.search("^clob", column_type):
                    column_type = "string"
                    sqoop_column_type = "String"

                if re.search(r"^time\([0-9]\)", column_type):
                    column_type = "varchar(9)"
                    sqoop_column_type = "String"

                column_type = re.sub("^integer", "int", column_type)
                column_type = re.sub("^timestmp", "timestamp", column_type)
                column_type = re.sub("^blob", "binary", column_type)
                column_type = re.sub("^real", "float", column_type)
                column_type = re.sub("^vargraph", "varchar", column_type)
                column_type = re.sub("^graphic", "varchar", column_type)

            if self.common_config.db_db2as400 == True:
                if re.search(r"^numeric\(", column_type):
                    column_type = re.sub(r"^numeric\(", "decimal(", column_type)
                    column_type = re.sub(r"\)$", ",0)", column_type)

                if re.search("^clob", column_type):
                    column_type = "string"
                    sqoop_column_type = "String"

                column_type = re.sub("^integer", "int", column_type)
                column_type = re.sub("^timestmp", "timestamp", column_type)
                column_type = re.sub(r"^timestamp\(.*\)", "timestamp", column_type)
                column_type = re.sub("^varbinary$", "binary", column_type)
                column_type = re.sub(r"^varbinary\([0-9]*\)$", "binary", column_type)
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
                    column_type = re.sub(r"^char\(", "varchar(", column_type)

            # Remove precision from datatypes that doesnt include a precision
            column_type = re.sub(r"^float\([0-9]*\)", "float", column_type)
            column_type = re.sub(r"^bigint\([0-9]*\)", "bigint", column_type)
            column_type = re.sub(r"^int\([0-9]*\)", "int", column_type)

            if column_type_override != None:
                column_type = column_type_override

            # If the data in the column will be anonymized, we will set the column type to a string type.
            if anonymization_function != "None":
                column_type = "string"

            # As Parquet imports some column types wrong, we need to map them all to string
            if column_type in ("timestamp", "date", "bigint"):
                sqoop_column_type = "String"
            if re.search(r"^decimal\(", column_type):
                sqoop_column_type = "String"

            # Fetch if we should force this column to 'string' in Hive
            if (
                column_type.startswith("char(") == True
                or column_type.startswith("varchar(") == True
            ):
                column_force_string = self.get_column_force_string(column_name)
                if column_force_string == True:
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

            if include_column_in_import == True:
                if sqoop_column_type_override != None:
                    self.sqoop_mapcolumnjava.append(
                        column_name + "=" + sqoop_column_type_override
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
                column_name_parquet_supported = self.get_parquet_column_name(
                    column_name
                )
                quote = self.common_config.get_quote_around_column()

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
                        self.is_column_name_reserved_in_sqoop(
                            column_name_parquet_supported
                        )
                        == True
                    ):
                        column_name_reserved = True
                        logging.warning(
                            "The column '%s' is a reserved column namn in Sqoop. Please rename the column in 'column_name_override'"
                            % (column_name_parquet_supported)
                        )

                    self.sqoop_use_generated_sql = True
                else:
                    self.sqlGeneratedSqoopQuery += quote + source_column_name + quote

                    if (
                        self.is_column_name_reserved_in_sqoop(source_column_name)
                        == True
                    ):
                        column_name_reserved = True
                        logging.warning(
                            "The column '%s' is a reserved column namn in Sqoop. Please rename the column in 'column_name_override'"
                            % (source_column_name)
                        )

            if column_id == None:
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
                        column_order,
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
                        column_order,
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

        if column_name_reserved == True:
            raise invalidConfiguration(
                "There are columns with reserved words. DBImport cant continue until that is handled"
            )

        # Add the source the to the generated sql query
        self.sqlGeneratedSqoopQuery += " from %s" % (
            self.common_config.get_jdbc_sql_from_table(
                schema=self.source_schema, table=self.source_table
            )
        )

        # Add ( and ) to the Hive column definition so it contains a valid string for later use in the solution
        self.sqlGeneratedHiveColumnDefinition = (
            "( " + self.sqlGeneratedHiveColumnDefinition + " )"
        )

        logging.debug("Settings from import_config.save_column_data()")
        logging.debug("    sqlGeneratedSqoopQuery = %s" % (self.sqlGeneratedSqoopQuery))
        logging.debug(
            "    sqlGeneratedHiveColumnDefinition = %s"
            % (self.sqlGeneratedHiveColumnDefinition)
        )
        logging.debug("Executing import_config.save_column_data() - Finished")

    def is_column_name_reserved_in_sqoop(self, column_name):
        """Returns True or False depending if the column_name is reserved in Sqoop"""

        if column_name in ("const", "private", "public", "default", "long"):
            return True
        else:
            return False

    def get_parquet_column_name(self, column_name):

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

    def set_primary_key_column(
        self,
    ):
        # This is one of the main functions when it comes to source system schemas. This will parse the output from the Python Schema Program
        # and update the source_primary_key column in the import_columns table with the information on what key is part of the PK
        logging.debug("")
        logging.debug("Executing import_config.set_primary_key_column()")
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

        for _index, row in self.common_config.source_keys_df.iterrows():
            key_type = row["CONSTRAINT_TYPE"]
            key_id = row["COL_KEY_POSITION"]
            column_name = self.strip_unwanted_char_column_name(row["COL_NAME"])

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

        logging.debug("Executing import_config.set_primary_key_column() - Finished")

    def save_index_data(
        self,
    ):
        logging.debug("Executing import_config.save_index_data()")
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
            for _index, row in self.common_config.source_index_df.sort_values(
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
                    column_type = self.common_config.source_columns_df.loc[
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
                            column_type,
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

        logging.debug("Executing import_config.save_index_data() - Finished")

    def save_key_data(
        self,
    ):
        # This is one of the main functions when it comes to source system schemas. This will parse the output from the Python Schema Program
        # and update the import_foreign_keys table with the information on what FK's are available for the table
        logging.debug("")
        logging.debug("Executing import_config.save_key_data()")
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
            for _index, row in source_fk_df[
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

        logging.debug("Executing import_config.save_key_data() - Finished")

    def save_generated_data(self):
        # This will save data to the generated* columns in import_table
        logging.debug("")
        logging.debug("Executing import_config.save_generated_data()")
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

                column = self.convert_hive_column_name_reserved_words(column)

                self.generatedSqoopOptions += "%s=%s," % (
                    self.get_parquet_column_name(column),
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

        logging.debug("Executing import_config.save_generated_data() - Finished")

    def calculate_job_mappers(self):
        """Based on previous sqoop size, the number of mappers for sqoop will be calculated. Formula is sqoop_size / 128MB."""
        logging.debug("Executing import_config.calculate_job_mappers()")

        if self.mongoImport == False:
            logging.info(
                "Calculating the number of SQL sessions in the source system that the import will use"
            )

        sql_sessions_min = 1
        sql_sessions_max = None

        query = "select max_import_sessions from jdbc_connections where dbalias = %s "
        self.mysql_cursor01.execute(query, (self.connection_alias,))
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement))

        row = self.mysql_cursor01.fetchone()

        if row[0] != None and row[0] > 0:
            sql_sessions_max = row[0]
            logging.info(
                "\u001b[33mThis connection is limited to a maximum of %s parallel sessions during import\u001b[0m"
                % (sql_sessions_max)
            )

        if row[0] != None and row[0] == 0:
            logging.warning(
                "Unsupported value of 0 in column 'max_import_sessions' in table 'jdbc_connections'"
            )

        sql_sessions_max_from_config = int(
            self.common_config.get_config_value(key="import_max_sessions")
        )
        sql_sessions_default = int(
            self.common_config.get_config_value(key="import_default_sessions")
        )

        if sql_sessions_max == None:
            sql_sessions_max = sql_sessions_max_from_config
        if sql_sessions_max_from_config < sql_sessions_max:
            sql_sessions_max = sql_sessions_max_from_config
        if sql_sessions_default > sql_sessions_max:
            sql_sessions_default = sql_sessions_max

        if self.mongoImport == True:
            # If it's a Mongo import, we dont need to find the number of mappers based on size. This is handled by the Mongo API
            self.sqlSessions = sql_sessions_default
            return

        hdfs_blocksize = int(self.common_config.get_config_value(key="hdfs_blocksize"))
        if hdfs_blocksize == None or hdfs_blocksize == 0:
            logging.error(
                "The setting 'hdfs_blocksize' in configuration table is incorrect. Please enter a correct size"
            )
            raise Exception

        # Execute SQL query that calculates the value
        query = "select T.sqoop_last_size, "
        query += "cast(S.size / %s as unsigned) as calculated_mappers, " % (
            hdfs_blocksize
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
        sql_sessions_last = row[0]
        self.sqlSessions = row[1]
        self.sqoop_mappers = row[2]

        logging.debug("sqlSessionsLast: %s" % (sql_sessions_last))
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
                    % (sql_sessions_default)
                )
                self.sqlSessions = sql_sessions_default
            else:
                if self.sqlSessions < sql_sessions_min:
                    self.sqlSessions = sql_sessions_min
                elif self.sqlSessions > sql_sessions_max:
                    self.sqlSessions = sql_sessions_max
                logging.info(
                    "The import will use %s SQL splits in the source system."
                    % (self.sqlSessions)
                )

        logging.debug("Executing import_config.calculate_job_mappers() - Finished")

    def clear_validation_data(self):
        logging.debug("Executing import_config.clear_validation_data()")
        logging.info("Clearing validation data from previous imports")

        self.validationCustomQuerySourceValue = None
        self.validationCustomQueryHiveValue = None

        query = "update import_tables set source_rowcount = NULL, source_rowcount_incr = NULL, hive_rowcount = NULL, validationCustomQuerySourceValue = NULL, validationCustomQueryHiveValue = NULL where table_id = %s"
        self.mysql_cursor01.execute(query, (self.table_id,))
        self.mysql_conn.commit()
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement))

        logging.debug("Executing import_config.clear_validation_data() - Finished")

    def get_incr_where_statement(
        self,
        forceIncr=False,
        ignoreIfOnlyIncrMax=False,
        whereForSourceTable=False,
        whereForSqoop=False,
        ignoreSQLwhereAddition=False,
        incrementalMax=None,
    ):
        """Returns the where statement that is needed to only work on the rows that was loaded incr"""
        logging.debug("Executing import_config.get_incr_where_statement()")
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

        where_statement = None

        if self.import_is_incremental == False:
            logging.debug(
                "Executing import_config.get_incr_where_statement() - Exited (Not an incremental import)"
            )
            return None

        if self.importPhase == constant.IMPORT_PHASE_ORACLE_FLASHBACK:
            # This is where we handle the specific where statement for Oracle FlashBack Imports
            if whereForSqoop == True:
                max_scn = int(
                    self.common_config.execute_jdbc_query(
                        "SELECT CURRENT_SCN FROM V$DATABASE"
                    ).iloc[0]["CURRENT_SCN"]
                )
                self.sqoopIncrMaxvaluePending = max_scn
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
                            self.common_config.execute_jdbc_query(
                                "SELECT OLDEST_FLASHBACK_SCN FROM V$FLASHBACK_DATABASE_LOG"
                            ).iloc[0]["OLDEST_FLASHBACK_SCN"]
                        )
                        where_statement = "VERSIONS BETWEEN SCN %s AND %s " % (
                            self.sqoopIncrMinvaluePending,
                            self.sqoopIncrMaxvaluePending,
                        )
                    except SQLerror as errMsg:
                        # Some versions of Oracle dont have the V$FLASHBACK_DATABASE_LOG table. So it it's not found, we use the MINVALUE function instead
                        if "view does not exist" in str(errMsg):
                            where_statement = (
                                "VERSIONS BETWEEN SCN MINVALUE AND %s "
                                % (self.sqoopIncrMaxvaluePending)
                            )
                        else:
                            raise SQLerror(str(errMsg))

                    where_statement += "WHERE VERSIONS_ENDTIME IS NULL AND (VERSIONS_OPERATION != 'D' OR VERSIONS_OPERATION IS NULL)"

            else:
                if whereForSourceTable == True or whereForSqoop == True:
                    where_statement = "VERSIONS BETWEEN SCN %s AND %s " % (
                        self.sqoopIncrMinvaluePending,
                        self.sqoopIncrMaxvaluePending,
                    )
                    if forceIncr == True or whereForSqoop == True:
                        where_statement += (
                            "WHERE VERSIONS_STARTSCN > %s AND VERSIONS_STARTSCN  <= %s AND VERSIONS_OPERATION IS NOT NULL AND VERSIONS_ENDTIME IS NULL "
                            % (
                                self.sqoopIncrMinvaluePending,
                                self.sqoopIncrMaxvaluePending,
                            )
                        )
                    else:
                        where_statement += "WHERE VERSIONS_ENDTIME IS NULL AND (VERSIONS_OPERATION != 'D' OR VERSIONS_OPERATION IS NULL)"
                else:
                    # This will be the where statement for the Target table in Hive
                    if self.sqoop_incr_validation_method == "incr":
                        where_statement = "datalake_update = '%s' " % (
                            self.sqoop_last_execution_timestamp
                        )

            if (
                self.get_sql_where_addition() != None
                and ignoreSQLwhereAddition == False
            ):
                if where_statement != None:
                    where_statement += "AND %s " % (self.get_sql_where_addition())
                else:
                    where_statement = "%s " % (self.get_sql_where_addition())

        else:
            if whereForSqoop == True:
                # If it is the where for sqoop, we need to get the max value for the configured column and save that into the config database

                if incrementalMax != None:
                    # This way, we can forcefully override the max value in an incremental import
                    max_value = incrementalMax
                else:
                    max_value = self.common_config.get_jdbc_column_max_value(
                        self.source_schema, self.source_table, self.sqoop_incr_column
                    )

                if max_value == None:
                    # If there is no MaxValue, it means that there is no data. And by that, there is no need for a where statement
                    return ""
                if self.sqoop_incr_mode == "append":
                    self.sqoopIncrMaxvaluePending = max_value
                try:
                    if self.sqoop_incr_mode == "lastmodified":
                        if re.search(r"\.([0-9]{3}|[0-9]{6})$", max_value):
                            self.sqoopIncrMaxvaluePending = datetime.strptime(
                                max_value, "%Y-%m-%d %H:%M:%S.%f"
                            )
                        else:
                            self.sqoopIncrMaxvaluePending = datetime.strptime(
                                max_value, "%Y-%m-%d %H:%M:%S"
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
                    where_statement = "%s <= CONVERT(datetime, '%s', 121) " % (
                        self.sqoop_incr_column,
                        self.sqoopIncrMaxvaluePending,
                    )
                elif self.common_config.jdbc_servertype == constant.MYSQL and (
                    whereForSourceTable == True or whereForSqoop == True
                ):
                    where_statement = (
                        "%s <= STR_TO_DATE('%s', "
                        % (self.sqoop_incr_column, self.sqoopIncrMaxvaluePending)
                        + "'%Y-%m-%d %H:%i:%s') "
                    )
                elif self.common_config.jdbc_servertype == constant.SNOWFLAKE and (
                    whereForSourceTable == True or whereForSqoop == True
                ):
                    where_statement = (
                        "\"%s\" <= TO_TIMESTAMP('%s', 'yyyy-mm-dd hh24:mi:ss.ff6') "
                        % (self.sqoop_incr_column, self.sqoopIncrMaxvaluePending)
                    )
                elif whereForSourceTable == True or whereForSqoop == True:
                    if self.sqoopIncrMaxvaluePending != None:
                        where_statement = (
                            "%s <= TO_TIMESTAMP('%s', 'YYYY-MM-DD HH24:MI:SS.FF') "
                            % (self.sqoop_incr_column, self.sqoopIncrMaxvaluePending)
                        )
                else:
                    where_statement = "`%s` <= '%s' " % (
                        self.sqoop_incr_column,
                        self.sqoopIncrMaxvaluePending,
                    )
            else:
                if whereForSourceTable == True or whereForSqoop == True:
                    where_statement = "%s <= %s " % (
                        self.sqoop_incr_column,
                        self.sqoopIncrMaxvaluePending,
                    )
                else:
                    where_statement = "`%s` <= %s " % (
                        self.sqoop_incr_column,
                        self.sqoopIncrMaxvaluePending,
                    )

            where_statement_saved = where_statement

            if (
                self.get_sql_where_addition() != None
                and ignoreSQLwhereAddition == False
            ):
                where_statement += "and %s " % (self.get_sql_where_addition())

            if (
                self.sqoop_incr_validation_method == "incr"
                or forceIncr == True
                or whereForSqoop == True
            ) and self.sqoop_incr_lastvalue != None:
                if self.sqoop_incr_mode == "lastmodified":
                    if self.common_config.jdbc_servertype == constant.MSSQL and (
                        whereForSourceTable == True or whereForSqoop == True
                    ):
                        where_statement += "and %s > CONVERT(datetime, '%s', 121) " % (
                            self.sqoop_incr_column,
                            self.sqoop_incr_lastvalue,
                        )
                    elif self.common_config.jdbc_servertype == constant.MYSQL and (
                        whereForSourceTable == True or whereForSqoop == True
                    ):
                        where_statement += (
                            "and %s > STR_TO_DATE('%s', "
                            % (self.sqoop_incr_column, self.sqoop_incr_lastvalue)
                            + "'%Y-%m-%d %H:%i:%s') "
                        )
                    elif self.common_config.jdbc_servertype == constant.SNOWFLAKE and (
                        whereForSourceTable == True or whereForSqoop == True
                    ):
                        where_statement += (
                            "and \"%s\" > TO_TIMESTAMP('%s', 'yyyy-mm-dd hh24:mi:ss.ff6') "
                            % (self.sqoop_incr_column, self.sqoop_incr_lastvalue)
                        )
                    elif whereForSourceTable == True or whereForSqoop == True:
                        where_statement += (
                            "and %s > TO_TIMESTAMP('%s', 'YYYY-MM-DD HH24:MI:SS.FF') "
                            % (self.sqoop_incr_column, self.sqoop_incr_lastvalue)
                        )
                    else:
                        where_statement += "and `%s` > '%s' " % (
                            self.sqoop_incr_column,
                            self.sqoop_incr_lastvalue,
                        )
                else:
                    if whereForSourceTable == True or whereForSqoop == True:
                        where_statement += "and %s > %s " % (
                            self.sqoop_incr_column,
                            self.sqoop_incr_lastvalue,
                        )
                    else:
                        where_statement += "and `%s` > %s " % (
                            self.sqoop_incr_column,
                            self.sqoop_incr_lastvalue,
                        )

            logging.debug("whereStatement = %s" % (where_statement))
            if where_statement_saved == where_statement and ignoreIfOnlyIncrMax == True:
                # We have only added the MAX value to there WHERE statement and have a setting to ignore it if that is the only value
                where_statement = None

        logging.debug("whereStatement: %s" % (where_statement))
        logging.debug("Executing import_config.get_incr_where_statement() - Finished")
        return where_statement

    def run_custom_validation_query_on_jdbc_table(self):
        logging.debug(
            "Executing import_config.run_custom_validation_query_on_jdbc_table()"
        )

        logging.info("Executing custom validation query on source table.")

        query = self.validationCustomQuerySourceSQL
        query = query.replace("${SOURCE_SCHEMA}", self.source_schema)
        query = query.replace("${SOURCE_TABLE}", self.source_table)

        logging.debug("Validation Query on JDBC table: %s" % (query))

        result_df = self.common_config.execute_jdbc_query(query)
        result_json = result_df.to_json(orient="values")
        self.validationCustomQuerySourceValue = result_json

        if len(self.validationCustomQuerySourceValue) > 1024:
            logging.warning(
                "'%s' is to large." % (self.validationCustomQuerySourceValue)
            )
            raise invalidConfiguration(
                "The size of the json document on the custom query exceeds 1024 bytes. Change the query to create a result with less than 512 bytes"
            )

        self.save_custom_sql_validation_source_value(json_value=result_json)

        logging.debug("resultDF:")
        logging.debug(result_df)
        logging.debug("resultJSON: %s" % (result_json))

        logging.debug(
            "Executing import_config.run_custom_validation_query_on_jdbc_table() - Finished"
        )

    def get_jdbc_table_row_count(self):
        logging.debug("Executing import_config.get_jdbc_table_row_count()")

        if self.validate_source == "sqoop":
            logging.debug(
                "Executing import_config.get_jdbc_table_row_count() - Finished (because self.validate_source == sqoop)"
            )
            return

        logging.info(
            "Reading number of rows in source table. This will later be used for validating the import"
        )

        jdbc_rows_full = None
        jdbc_rows_incr = None
        where_statement = None

        if (
            self.import_is_incremental == True
            and self.importPhase != constant.IMPORT_PHASE_MSSQL_CHANGE_TRACKING
        ):
            # MSSQL Change Tracking only supports full incremental validation
            if self.importPhase == constant.IMPORT_PHASE_ORACLE_FLASHBACK:
                if self.sqoop_incr_validation_method == "full":
                    where_statement = self.get_incr_where_statement(
                        forceIncr=False, whereForSourceTable=True
                    )
                    query = 'SELECT COUNT(1) FROM "%s"."%s" ' % (
                        self.source_schema.upper(),
                        self.source_table.upper(),
                    )
                    query += where_statement
                    jdbc_rows_full = int(
                        self.common_config.execute_jdbc_query(query).iloc[0]["COUNT(1)"]
                    )

                where_statement = self.get_incr_where_statement(
                    forceIncr=True, whereForSourceTable=True
                )
                query = 'SELECT COUNT(1) FROM "%s"."%s" ' % (
                    self.source_schema.upper(),
                    self.source_table.upper(),
                )
                query += where_statement
                jdbc_rows_incr = int(
                    self.common_config.execute_jdbc_query(query).iloc[0]["COUNT(1)"]
                )
            else:
                where_statement = self.get_incr_where_statement(
                    forceIncr=False, whereForSourceTable=True
                )
                logging.debug(
                    "Where statement for forceIncr = False: %s" % (where_statement)
                )
                jdbc_rows_full = self.common_config.get_jdbc_table_row_count(
                    self.source_schema, self.source_table, where_statement
                )
                logging.debug(
                    "Got %s rows from get_jdbc_table_row_count()" % (jdbc_rows_full)
                )

                where_statement = self.get_incr_where_statement(
                    forceIncr=True, whereForSourceTable=True
                )
                logging.debug(
                    "Where statement for forceIncr = True: %s" % (where_statement)
                )
                jdbc_rows_incr = self.common_config.get_jdbc_table_row_count(
                    self.source_schema, self.source_table, where_statement
                )
                logging.debug(
                    "Got %s rows from get_jdbc_table_row_count()" % (jdbc_rows_incr)
                )
        else:
            if self.get_sql_where_addition() != None:
                logging.debug(
                    "Where statement for full imports: %s"
                    % (self.get_sql_where_addition())
                )
                where_statement = self.get_sql_where_addition()
            else:
                where_statement = ""

            jdbc_rows_full = self.common_config.get_jdbc_table_row_count(
                self.source_schema, self.source_table, where_statement
            )

        # Save the value to the database
        query = "update import_tables set "

        if jdbc_rows_incr != None:
            query += "source_rowcount_incr = %s " % (jdbc_rows_incr)
            logging.debug(
                "Source table contains %s rows for the incremental part"
                % (jdbc_rows_incr)
            )

        if jdbc_rows_full != None:
            if jdbc_rows_incr != None:
                query += ", "
            query += "source_rowcount = %s " % (jdbc_rows_full)
            logging.debug("Source table contains %s rows" % (jdbc_rows_full))

        query += "where table_id = %s" % (self.table_id)

        self.mysql_cursor01.execute(query)
        self.mysql_conn.commit()
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement))

        logging.debug("Executing import_config.get_jdbc_table_row_count() - Finished")

    def get_mongo_row_count(self):
        logging.debug("Executing import_config.get_mongo_row_count()")

        self.common_config.connect_to_mongo()
        mongo_collection = self.common_config.mongoDB[self.source_table]
        mongo_collection_count = mongo_collection.count()
        self.common_config.disconnect_from_mongo()

        self.save_source_table_row_count(row_count=mongo_collection_count)

        logging.debug("Executing import_config.get_mongo_row_count() - Finished")

    def save_custom_sql_validation_source_value(self, json_value, print_info=True):
        logging.debug(
            "Executing import_config.save_custom_sql_validation_source_value()"
        )
        if print_info == True:
            logging.info(
                "Saving the custom SQL validation data from Source Table to the configuration database"
            )

        query = "update import_tables set validationCustomQuerySourceValue = %s where table_id = %s"

        self.mysql_cursor01.execute(query, (json_value, self.table_id))
        self.mysql_conn.commit()
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement))

        logging.debug(
            "Executing import_config.save_custom_sql_validation_source_value() - Finished"
        )

    def save_custom_sql_validation_hive_value(self, jsonValue, printInfo=True):
        logging.debug("Executing import_config.save_custom_sql_validation_hive_value()")
        if printInfo == True:
            logging.info(
                "Saving the custom SQL validation data from Hive Table to the configuration database"
            )

        query = "update import_tables set validationCustomQueryHiveValue = %s where table_id = %s"

        self.mysql_cursor01.execute(query, (jsonValue, self.table_id))
        self.mysql_conn.commit()
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement))

        logging.debug(
            "Executing import_config.save_custom_sql_validation_hive_value() - Finished"
        )

    def save_source_table_row_count(self, row_count, incr=False, print_info=True):
        logging.debug("Executing import_config.save_source_table_row_count()")
        if print_info == True:
            logging.info(
                "Saving the number of rows in the Source Table to the configuration database"
            )

        if self.validationMethod == constant.VALIDATION_METHOD_CUSTOMQUERY:
            logging.debug(
                "Executing import_config.save_source_table_row_count() - Finished (Because self.validationMethod = %s"
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

        self.mysql_cursor01.execute(query, (row_count, self.table_id))
        self.mysql_conn.commit()
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement))

        logging.debug(
            "Executing import_config.save_source_table_row_count() - Finished"
        )

    def save_hive_table_row_count(self, rowCount):
        logging.debug("Executing import_config.save_hive_table_row_count()")
        logging.info(
            "Saving the number of rows in the Hive Table to the configuration database"
        )

        # Save the value to the database
        query = "update import_tables set hive_rowcount = %s where table_id = %s"
        self.mysql_cursor01.execute(query, (rowCount, self.table_id))
        self.mysql_conn.commit()
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement))

        logging.debug("Executing import_config.save_hive_table_row_count() - Finished")

    def reset_sqoop_statistics(self, max_value):
        logging.debug("Executing import_config.reset_sqoop_statistics()")
        if max_value != None:
            logging.info(
                "Reseting incremental values for imports. New max value is: %s"
                % (max_value)
            )

        query = "update import_tables set "
        query += "	incr_minvalue = NULL, "
        query += "	incr_maxvalue = %s, "
        query += "	incr_minvalue_pending = NULL, "
        query += "	incr_maxvalue_pending = NULL "
        query += "where table_id = %s "

        self.mysql_cursor01.execute(query, (max_value, self.table_id))
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement))
        self.mysql_conn.commit()

        logging.debug("Executing import_config.reset_sqoop_statistics() - Finished")

    def save_import_statistics(
        self,
        sqoop_start_uts,
        sqoop_size=None,
        sqoop_rows=None,
        sqoop_incr_maxvalue_pending=None,
        sqoop_mappers=None,
    ):
        logging.debug("Executing import_config.save_import_statistics()")
        logging.info("Saving %s statistics" % (self.importTool))

        self.sqoopStartUTS = sqoop_start_uts
        try:
            self.sqoop_last_execution_timestamp = datetime.utcfromtimestamp(
                sqoop_start_uts
            ).strftime("%Y-%m-%d %H:%M:%S.000")
        except TypeError:
            self.sqoop_last_execution_timestamp = None

        query_param = []
        query = "update import_tables set "
        query += "	sqoop_last_execution = %s "
        query_param.append(sqoop_start_uts)

        if sqoop_size != None:
            query += "  ,sqoop_last_size = %s "
            self.sqoop_last_size = sqoop_size
            query_param.append(sqoop_size)

        if sqoop_rows != None:
            query += "  ,sqoop_last_rows = %s "
            self.sqoop_last_rows = sqoop_rows
            query_param.append(sqoop_rows)

        if sqoop_incr_maxvalue_pending != None:
            query += "  ,incr_maxvalue_pending = %s "
            self.sqoopIncrMaxvaluePending = sqoop_incr_maxvalue_pending
            query_param.append(sqoop_incr_maxvalue_pending)

        if sqoop_mappers != None:
            query += "  ,sqoop_last_mappers = %s "
            self.sqoop_last_mappers = sqoop_mappers
            query_param.append(sqoop_mappers)

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
            query_param.append(sqoop_rows)

        query += "where table_id = %s "
        query_param.append(self.table_id)

        self.mysql_cursor01.execute(query, query_param)
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement))
        self.mysql_conn.commit()

        logging.debug("Executing import_config.save_import_statistics() - Finished")

    def check_mssql_change_tracking_min_valid_version(self):
        """Checks the Minimum valid version from MSSQL Change Tracking. If the min valid version is higher than what we want to read, we will force a full import instead of an incremental import"""
        logging.debug(
            "Executing import_config.check_mssql_change_tracking_min_valid_version()"
        )

        query = (
            "select CHANGE_TRACKING_MIN_VALID_VERSION(OBJECT_ID('%s')) AS MINVERSION"
            % (
                self.common_config.get_jdbc_sql_from_table(
                    schema=self.source_schema, table=self.source_table
                )
            )
        )

        try:
            min_version = int(
                self.common_config.execute_jdbc_query(query).iloc[0]["MINVERSION"]
            )
        except TypeError:
            # Cant read the value. It means permission error or that Change Tracking isnt available on the table
            raise invalidConfiguration(
                "Unable to read Change Tracking version. Verify that Change Tracking is enabled on the table and that the user that is accessing it have the correct permission"
            )

        if self.incr_maxvalue == None:
            # No need to check as it will be a full load anyway
            return

        if min_version > int(self.incr_maxvalue):
            # If we hit this part of the code, it means that the minimal version on the MSSQL Server is higher thatn what we require.
            # Only way to get out of this situation is to do a full load
            logging.warning(
                "The minimal version in MSSQL is larger than what is required. This means that we need to force a full initial load"
            )
            self.reset_sqoop_statistics(max_value=None)
            self.incr_maxvalue = None

        logging.debug(
            "Executing import_config.check_mssql_change_tracking_min_valid_version() - Finished"
        )

    def get_sql_to_read_from_source_with_mssql_change_tracking(self):
        """Creates and return the SQL needed to read the rows from the source database with the help of MSSQL Change Tracking function"""
        logging.debug(
            "Executing import_config.get_sql_to_read_from_source_with_mssql_change_tracking()"
        )
        query = self.get_sql_to_read_from_source(source_table_as_name="ST")

        query = re.sub(
            "^select",
            'select "CT"."SYS_CHANGE_VERSION" as "datalake_mssql_changetrack_version", "CT"."SYS_CHANGE_OPERATION" as "datalake_mssql_changetrack_operation",',
            query,
        )

        if self.incr_maxvalue == None:
            # This means that there is no previous version. So we need to do an initial load
            query += " LEFT JOIN CHANGETABLE(CHANGES %s, 0 ) as CT" % (
                self.common_config.get_jdbc_sql_from_table(
                    schema=self.source_schema, table=self.source_table
                )
            )
            pk_columns = self.get_pk_columns(convert_to_lower=False)
            join_columns = ""
            for _i, target_column in enumerate(pk_columns.split(",")):
                if join_columns == "":
                    join_columns = " ON "
                else:
                    join_columns += " AND "

                join_columns += '"CT"."%s" = "ST"."%s"' % (target_column, target_column)

            query += join_columns

        else:
            # This is an incremental load
            query += " RIGHT OUTER JOIN CHANGETABLE(CHANGES %s, %s ) as CT" % (
                self.common_config.get_jdbc_sql_from_table(
                    schema=self.source_schema, table=self.source_table
                ),
                self.incr_maxvalue,
            )

            pk_columns = self.get_pk_columns(convert_to_lower=False)
            join_columns = ""

            # PK must come from the CHANGETABLE function. This is needed in case of deletes, as the column would be NULL otherwise and the merge
            # in Hive would not be able to match and remove the row
            for _i, target_column in enumerate(pk_columns.split(",")):
                query = re.sub(
                    '"ST"."%s"' % (target_column), '"CT"."%s"' % (target_column), query
                )

            for _i, target_column in enumerate(pk_columns.split(",")):
                if join_columns == "":
                    join_columns = " ON "
                else:
                    join_columns += " AND "

                join_columns += '"CT"."%s" = "ST"."%s"' % (target_column, target_column)

            query += join_columns

        return query
        logging.debug(
            "Executing import_config.get_sql_to_read_from_source_with_mssql_change_tracking() - Finished"
        )

    def get_sql_to_read_from_source(self, source_table_as_name=None):
        """Creates and return the SQL needed to read all rows from the source database"""
        logging.debug("Executing import_config.get_sql_to_read_from_source()")

        quote = self.common_config.get_quote_around_column()

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

        spark_query = ""
        for _index, row in result_df.iterrows():
            if len(spark_query) == 0:
                spark_query = "select "
            else:
                spark_query += ", "
            if source_table_as_name == None:
                if row["source_name"] != row["name"]:
                    spark_query += (
                        quote
                        + row["source_name"]
                        + quote
                        + " as "
                        + quote
                        + row["name"]
                        + quote
                    )
                else:
                    spark_query += quote + row["source_name"] + quote
            else:
                if row["source_name"] != row["name"]:
                    spark_query += (
                        quote
                        + source_table_as_name
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
                    spark_query += (
                        quote
                        + source_table_as_name
                        + quote
                        + "."
                        + quote
                        + row["source_name"]
                        + quote
                    )

        # Add the source the to the generated sql query
        spark_query += " from %s" % (
            self.common_config.get_jdbc_sql_from_table(
                schema=self.source_schema, table=self.source_table
            )
        )
        if source_table_as_name != None:
            spark_query += " as %s" % (source_table_as_name)

        logging.debug(
            "Executing import_config.get_sql_to_read_from_source() - Finished"
        )
        return spark_query

    def get_columns_from_config_database(
        self, restrictColumns=None, sourceIsParquetFile=False, includeAllColumns=True
    ):
        """Reads the columns from the configuration database and returns the information in a Pandas DF with the columns name, type and comment"""
        logging.debug("Executing import_config.get_columns_from_config_database()")
        hive_column_definition = ""

        restrict_columns_list = []
        if restrictColumns != None:
            restrict_columns_list = restrictColumns.split(",")

        query = "select "
        query += "   COALESCE(c.column_name_override, c.column_name) as name, "
        query += "   c.column_type as type, "
        query += "   c.comment "
        query += "from import_tables t "
        query += "join import_columns c on t.table_id = c.table_id "
        query += "where t.table_id = %s and t.last_update_from_source = c.last_update_from_source "

        if includeAllColumns == False:
            query += " and c.include_in_import = 1 "

        if restrict_columns_list != []:
            query += " and c.column_name in ('"
            query += "', '".join(restrict_columns_list)
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
                lambda x: self.get_parquet_column_name(x)
            )

        logging.debug(
            "Executing import_config.get_columns_from_config_database() - Finished"
        )
        return result_df

    def get_sql_where_addition(
        self,
    ):
        """Returns the SQL Where addition used by the sqoop and spark"""

        if (
            self.sqoop_sql_where_addition == None
            or self.sqoop_sql_where_addition.strip() == ""
        ):
            return None

        if self.common_config.db_snowflake == True:
            sql_where = self.sqoop_sql_where_addition
        else:
            sql_where = self.sqoop_sql_where_addition.replace('"', "'")

        print_custom_sql = False

        if "${MIN_VALUE}" in sql_where:
            print_custom_sql = True
            sql_where = sql_where.replace(
                "${MIN_VALUE}", str(self.sqoop_incr_lastvalue)
            )

        if "${MAX_VALUE}" in sql_where:
            print_custom_sql = True
            sql_where = sql_where.replace(
                "${MAX_VALUE}", str(self.sqoopIncrMaxvaluePending)
            )

        if print_custom_sql == True and self.print_sql_where_addition == True:
            logging.info(
                "Values have been replaced in the SQL where addition. New value is:"
            )
            self.print_sql_where_addition = False
            print(sql_where)

        return sql_where

    def get_hive_table_comment(
        self,
    ):
        """Returns the table comment stored in import_tables.comment"""
        logging.debug("Executing import_config.get_hive_table_comment()")
        hive_column_definition = ""

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

        logging.debug("Executing import_config.get_hive_table_comment() - Finished")
        return row[0]

    def validate_custom_query(self):
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

    def validate_row_count(self, validateSqoop=False, incremental=False):
        """Validates the rows based on values stored in import_tables -> source_columns and target_columns. Returns True or False"""
        # incremental=True is used for normal validations but on ly on incremented rows, regardless if we are going to validate full or incr.
        # This is used to validate the Import table if the validation method is full for incremental import

        logging.debug("Executing import_config.validate_row_count()")
        logging.debug("validateSqoop = %s" % (validateSqoop))
        logging.debug("incremental = %s" % (incremental))
        return_value = None

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
                        validate_text_target = "Hive table"
                    else:
                        validate_text_target = "Target table"
                    validate_text_source = "Source table"
                    validate_text = validate_text_target
                elif (
                    self.sqoop_incr_validation_method == "full" and incremental == False
                ):
                    logging.debug(
                        "validateSqoop == False & self.sqoop_incr_validation_method == 'full' and incremental == False"
                    )
                    # We are not validating the sqoop import, but the validation is an incremental import and
                    # we are going to validate all the data
                    query = "select source_rowcount, hive_rowcount from import_tables where table_id = %s "
                    validate_text_target = "Hive table"
                    validate_text_source = "Source table"
                    validate_text = validate_text_target
                else:
                    logging.debug("validateSqoop == False & else")
                    # We are not validating the sqoop import, but the validation is an incremental import and
                    # we are going to validate only the incremental part of the import (what sqoop imported)
                    query = "select source_rowcount_incr, hive_rowcount from import_tables where table_id = %s "
                    validate_text_target = "Hive table (incr)"
                    validate_text_source = "Source table (incr)"
                    validate_text = "Hive table"
            else:
                if self.import_is_incremental == False:
                    logging.debug(
                        "validateSqoop == True & self.import_is_incremental == False"
                    )
                    # Sqoop validation for full imports
                    query = (
                        "select source_rowcount from import_tables where table_id = %s "
                    )
                    validate_text_target = "%s import" % (self.importTool.capitalize())
                    validate_text_source = "Source table"
                    validate_text = validate_text_target
                else:
                    logging.debug("validateSqoop == True & else")
                    # Sqoop validation for incremental imports
                    query = "select source_rowcount_incr from import_tables where table_id = %s "
                    validate_text_target = "%s import (incr)" % (
                        self.importTool.capitalize()
                    )
                    validate_text_source = "Source table (incr)"
                    validate_text = "%s import" % (self.importTool.capitalize())
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
            diff_allowed = 0
            logging.debug("source_rowcount: %s" % (source_rowcount))
            logging.debug("target_rowcount: %s" % (target_rowcount))

            if source_rowcount == None:
                logging.error(
                    "There is no information about rowcount stored in import_tables. Are you running with sqoop validation method and using 'full' validation on an incremental import?"
                )
                return False

            if source_rowcount > 0:
                if self.validate_diff_allowed != None:
                    diff_allowed = int(self.validate_diff_allowed)
                else:
                    diff_allowed = int(
                        source_rowcount * (50 / (100 * math.sqrt(source_rowcount)))
                    )

            upper_validation_limit = source_rowcount + diff_allowed
            lower_validation_limit = source_rowcount - diff_allowed

            logging.debug("upperValidationLimit: %s" % (upper_validation_limit))
            logging.debug("lowerValidationLimit: %s" % (lower_validation_limit))
            logging.debug("diffAllowed: %s" % (diff_allowed))

            if target_rowcount == None:
                logging.warn(
                    "There is no information about rowcounts in target table. Skipping validation"
                )
                return True

            if (
                target_rowcount > upper_validation_limit
                or target_rowcount < lower_validation_limit
            ):
                logging.error(
                    "Validation failed! The %s exceedes the allowed limit compared top the source table"
                    % (validate_text)
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
                logging.info(
                    "%s rowcount: %s" % (validate_text_source, source_rowcount)
                )
                logging.info(
                    "%s rowcount: %s" % (validate_text_target, target_rowcount)
                )
                logging.info("Validation diff allowed: %s" % (diff_allowed))
                logging.info("Upper limit: %s" % (upper_validation_limit))
                logging.info("Lower limit: %s" % (lower_validation_limit))
                return_value = False
            else:
                logging.info("%s validation successful!" % (validate_text))
                logging.info(
                    "%s rowcount: %s" % (validate_text_source, source_rowcount)
                )
                logging.info(
                    "%s rowcount: %s" % (validate_text_target, target_rowcount)
                )
                print()
                return_value = True

        else:
            return_value = True

        logging.debug("Executing import_config.validate_row_count() - Finished")
        return return_value

    def get_pk_columns(self, pk_for_merge=False, convert_to_lower=True):
        """Returns a comma seperated list of columns that is part of the PK"""
        logging.debug("Executing import_config.get_pk_columns()")
        return_value = None

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
            and pk_for_merge == False
        ):
            logging.debug("Executing import_config.get_pk_columns() - Finished")
            self.pk_column_override = re.sub(
                ", *", ",", self.pk_column_override.strip()
            )
            self.pk_column_override = re.sub(" *,", ",", self.pk_column_override)
            return self.pk_column_override.lower()

        if (
            self.pk_column_override_mergeonly != None
            and self.pk_column_override_mergeonly.strip() != ""
            and pk_for_merge == True
        ):
            logging.debug("Executing import_config.get_pk_columns() - Finished")
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
            and pk_for_merge == True
        ):
            logging.debug("Executing import_config.get_pk_columns() - Finished")
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
            if return_value == None:
                return_value = str(row[0])
            else:
                return_value += "," + str(row[0])

            if convert_to_lower == True:
                return_value = return_value.lower()

        return return_value

    def get_foreign_keys_from_config(
        self,
    ):
        """Reads the ForeignKeys from the configuration tables and return the result in a Pandas DF"""
        logging.debug("Executing import_config.get_foreign_keys_from_config()")
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

        logging.debug(
            "Executing import_config.get_foreign_keys_from_config() - Finished"
        )
        return result_df.sort_values(by=["fk_name"], ascending=True)

    def get_all_active_incr_imports(self):
        """Return all rows from import_stage that uses an incremental import_type"""
        logging.debug("Executing import_config.get_all_active_incr_imports()")
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

        logging.debug(
            "Executing import_config.get_all_active_incr_imports() - Finished"
        )
        return result_df

    def generate_sqoop_split_by(self):
        """This function will try to generate a split-by"""
        logging.debug("Executing import_config.generate_sqoop_split_by()")

        self.splitByColumn = ""

        self.get_pk_columns()

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

        logging.debug("Executing import_config.generate_sqoop_split_by() - Finished")

    def generate_sqoop_boundary_query(self):
        logging.debug("Executing import_config.generate_sqoop_boundary_query()")
        self.generate_sqoop_split_by()

        if self.splitByColumn != "":
            self.sqoopBoundaryQuery = "select min(%s), max(%s) from %s" % (
                self.splitByColumn,
                self.splitByColumn,
                self.common_config.get_jdbc_sql_from_table(
                    schema=self.source_schema, table=self.source_table
                ),
            )

        logging.debug("SplitByColumn = %s" % (self.splitByColumn))
        logging.debug("sqoopBoundaryQuery = %s" % (self.sqoopBoundaryQuery))
        logging.debug(
            "Executing import_config.generate_sqoop_boundary_query() - Finished"
        )

    def get_min_max_boundary_values(self):
        """Returns a dictionary with two values called min and max that contains the boundary values"""
        logging.debug("Executing import_config.get_min_max_boundary_values()")

        return_dict = {}
        if self.mongoImport == True:
            return_dict["min"] = None
            return_dict["max"] = None
            return return_dict

        self.generate_sqoop_boundary_query()
        if self.sqoopBoundaryQuery == "":
            logging.info(
                "No columns to split on can be found. Forcing the number of sessions to 1"
            )
            self.sqlSessions = 1

            logging.debug(
                "self.sqoopBoundaryQuery is empty so generating a dummy MIN&MAX dict"
            )
            return_dict = {}
            return_dict["min"] = "Unknown"
            return_dict["max"] = "Unknown"
            return return_dict

        logging.info("Getting boundary MIN and MAX values")
        logging.debug(self.sqoopBoundaryQuery)
        min_max_values = self.common_config.execute_jdbc_query(self.sqoopBoundaryQuery)

        min_value = min_max_values.iloc[0].values[0]
        max_value = min_max_values.iloc[0].values[1]

        if isinstance(min_value, np.float64) == True:
            if min_value < 0 and max_value > 0:
                min_value = np.int64(min_value) - 1
            else:
                min_value = np.int64(min_value)

        if isinstance(max_value, np.float64) == True:
            if max_value < 0:
                max_value = np.int64(max_value) - 1
            else:
                max_value = np.int64(max_value) + 1

        return_dict["min"] = min_value
        return_dict["max"] = max_value

        logging.debug(
            "Executing import_config.get_min_max_boundary_values() - Finished"
        )
        return return_dict

    def get_import_tables(self, hive_db):
        """Return all tables that we import to a specific Hive DB"""
        logging.debug("Executing import_config.get_import_tables()")
        result_df = None

        query = "select "
        query += "	hive_db, "
        query += "	hive_table, "
        query += "	dbalias, "
        query += "	source_schema, "
        query += "	source_table "
        query += "from import_tables "
        query += "where hive_db = %s"

        self.mysql_cursor01.execute(query, (hive_db,))
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

        logging.debug("Executing import_config.get_import_tables() - Finished")
        return result_df

    def add_import_table(self, hive_db, hive_table, dbalias, schema, table, catalog):
        """Add source table to import_tables"""
        logging.debug("Executing import_config.add_import_table()")
        return_value = True

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
                "catalog option supplied to import_config.add_import_table does not contain a valid option"
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
                "Hive table %s.%s cant be added as it's not unique in the configuration database"
                % (hive_db, hive_table)
            )
            return_value = False

        logging.debug("Executing import_config.add_import_table() - Finished")
        return return_value

    def is_external_view_required(self):
        logging.debug("Executing import_config.is_external_view_required()")
        return_value = False

        if self.importTool == "spark":
            return_value = False

        elif self.generatedSqoopOptions != None:
            return_value = True

        logging.debug("Executing import_config.is_external_view_required() - Finished")
        return return_value

    def get_select_for_import_view(self):
        logging.debug("Executing import_config.get_select_for_import_view()")

        query = "select c.column_name as name, c.column_name_override, c.column_type as type, c.sqoop_column_type, c.sqoop_column_type_override "
        query += "from import_tables t "
        query += "join import_columns c on t.table_id = c.table_id "
        query += "where t.table_id = %s and t.last_update_from_source = c.last_update_from_source "
        query += "order by c.column_order"

        self.mysql_cursor01.execute(query, (self.table_id,))
        logging.debug("SQL Statement executed: \n%s" % (self.mysql_cursor01.statement))

        select_query = ""

        for row in self.mysql_cursor01.fetchall():
            column_name = row[0]
            column_name_override = row[1]
            column_type = row[2]
            sqoop_column_type = row[3]
            sqoop_column_type_override = row[4]

            if column_name_override != None:
                column_name = column_name_override
            column_name = self.convert_hive_column_name_reserved_words(column_name)
            column_name = self.get_parquet_column_name(column_name)

            if sqoop_column_type != None and sqoop_column_type.strip() == "":
                sqoop_column_type = None

            if (
                sqoop_column_type_override != None
                and sqoop_column_type_override.strip() == ""
            ):
                sqoop_column_type_override = None

            if select_query != "":
                select_query += ", "

            if sqoop_column_type == None and sqoop_column_type_override == None:
                select_query += "S.`%s`" % (column_name)
            else:
                select_query += "cast(S.`%s` as %s) `%s`" % (
                    column_name,
                    column_type,
                    column_name,
                )

            # Handle reserved column names in Hive

            # TODO: Get the maximum column comment size from Hive Metastore and use that instead
        select_query = "select %s" % (select_query)

        if self.importPhase == constant.IMPORT_PHASE_ORACLE_FLASHBACK:
            # Add the specific Oracle FlashBack columns that we need to import
            select_query += ", S.`datalake_flashback_operation`"
            select_query += ", cast(S.`datalake_flashback_startscn` as bigint) datalake_flashback_startscn"

        select_query += " from `%s`.`%s` as S " % (
            self.Hive_Import_DB,
            self.Hive_Import_Table,
        )

        if self.importPhase == constant.IMPORT_PHASE_ORACLE_FLASHBACK:
            select_query += "inner join ("
            select_query += (
                "   select %s, max(datalake_flashback_startscn) as max_startscn "
                % (self.get_pk_columns())
            )
            select_query += "   from `%s`.`%s` " % (
                self.Hive_Import_DB,
                self.Hive_Import_Table,
            )
            select_query += "   group by %s) G " % (self.get_pk_columns())
            select_query += "on "

            first_row = True
            for pk_column in self.get_pk_columns().split(","):
                if first_row == True:
                    select_query += "   S.%s = G.%s" % (pk_column, pk_column)
                    first_row = False
                else:
                    select_query += "   and S.%s = G.%s" % (pk_column, pk_column)
            select_query += "   and ( S.datalake_flashback_startscn = G.max_startscn or G.max_startscn is NULL )"

        logging.debug("Executing import_config.get_select_for_import_view() - Finished")
        return select_query

    def reset_incr_min_max_values(self, maxValue):
        logging.debug("Executing import_config.reset_incr_min_max_values()")
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

        logging.debug("Executing import_config.reset_incr_min_max_values() - Finished")

    def invalidateImpala(self):
        """Connects to Impala and invalidates the metadata"""
        logging.debug("Executing import_config.invalidateImpala()")

        if self.invalidate_impala_metadata == False:
            return

        self.common_config.connect_to_impala()

        # TODO: Add invalidation of history table as well
        # self.import_with_history_table = True
        logging.info("Invalidating Impala table")
        query = "invalidate metadata `%s`.`%s`" % (self.Hive_DB, self.Hive_Table)
        self.common_config.execute_impala_query(query)

        self.common_config.disconnect_from_impala()

        logging.debug("Executing import_config.invalidateImpala() - Finished")
