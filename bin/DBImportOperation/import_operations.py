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
import shlex
import subprocess
import sys
import time
from datetime import datetime, timedelta

import pandas as pd
import requests
from requests_kerberos import OPTIONAL, HTTPKerberosAuth

from common import constants as constant
from common import sparkUDF as sparkUDF
from common.Exceptions import (
    SQLerror,
    databaseNotFound,
    invalidConfiguration,
    undevelopedFeature,
)
from common.Singleton import Singleton
from ConfigReader import configuration
from DBImportConfig import import_config
from DBImportOperation import atlas_operations, common_operations


class operation(metaclass=Singleton):
    def __init__(self, Hive_DB=None, Hive_Table=None):
        logging.debug("Executing import_operations.__init__()")
        self.Hive_DB = None
        self.Hive_Table = None
        self.mysql_conn = None
        self.mysql_cursor = None
        self.startDate = None
        self.import_config = None
        self.sqlGeneratedHiveColumnDefinition = None
        self.sqlGeneratedSqoopQuery = None
        self.sqoop_start_timestamp = None
        self.sqoopStartUTS = None
        self.sqoopSize = None
        self.sqoopRows = None
        self.sqoop_incr_max_value_pending = None
        self.sqoopIncrNoNewRows = None
        self.sqoop_launched_map_tasks = None
        self.sqoop_failed_map_tasks = None
        self.sqoop_killed_map_tasks = None
        self.sqoop_launched_reduce_tasks = None
        self.sqoop_failed_reduce_tasks = None
        self.sqoop_killed_reduce_tasks = None
        self.spark_context = None
        self.spark = None
        self.sparkUDF = None
        self.udf_hash_column = None
        self.udf_replace_char_with_star = None
        self.udf_show_first_four_characters = None
        self.yarnApplicationID = None

        self.global_hive_configuration_set = False

        self.import_config = import_config.config(Hive_DB, Hive_Table)
        self.common_operations = common_operations.operation(Hive_DB, Hive_Table)
        self.atlasOperation = atlas_operations.atlasOperation()

        # self.import_config.sparkCatalogName = self.common_operations.sparkCatalogName

        if Hive_DB != None and Hive_Table != None:
            self.set_hive_table(Hive_DB, Hive_Table)

        logging.debug("Executing import_operations.__init__() - Finished")

    def set_hive_table(self, Hive_DB, Hive_Table):
        """Sets the parameters to work against a new Hive database and table"""
        self.Hive_DB = Hive_DB.lower()
        self.Hive_Table = Hive_Table.lower()

        self.common_operations.set_hive_table(self.Hive_DB, self.Hive_Table)
        self.import_config.set_hive_table(self.Hive_DB, self.Hive_Table)

        self.import_config.get_import_config()
        self.startDate = self.import_config.startDate

        self.Hive_Import_DB = self.import_config.Hive_Import_DB
        self.Hive_Import_Table = self.import_config.Hive_Import_Table
        self.Hive_Import_View = self.import_config.Hive_Import_View
        self.Hive_History_DB = self.import_config.Hive_History_DB
        self.Hive_History_Table = self.import_config.Hive_History_Table
        self.Hive_HistoryTemp_DB = self.import_config.Hive_HistoryTemp_DB
        self.Hive_HistoryTemp_Table = self.import_config.Hive_HistoryTemp_Table
        self.Hive_Import_PKonly_DB = self.import_config.Hive_Import_PKonly_DB
        self.Hive_Import_PKonly_Table = self.import_config.Hive_Import_PKonly_Table
        self.Hive_Delete_DB = self.import_config.Hive_Delete_DB
        self.Hive_Delete_Table = self.import_config.Hive_Delete_Table
        self.Hive_ColumnName_Import = self.import_config.Hive_ColumnName_Import
        self.Hive_ColumnName_Insert = self.import_config.Hive_ColumnName_Insert
        self.Hive_ColumnName_Update = self.import_config.Hive_ColumnName_Update
        self.Hive_ColumnName_Delete = self.import_config.Hive_ColumnName_Delete
        self.Hive_ColumnName_IUD = self.import_config.Hive_ColumnName_IUD
        self.Hive_ColumnName_HistoryTimestamp = (
            self.import_config.Hive_ColumnName_HistoryTimestamp
        )
        self.Hive_ColumnName_Source = self.import_config.Hive_ColumnName_Source

        self.common_operations.set_table_and_column_names(
            hive_import_db=self.Hive_Import_DB,
            hive_import_table=self.Hive_Import_Table,
            hive_import_view=self.Hive_Import_View,
            hive_history_db=self.Hive_History_DB,
            hive_history_table=self.Hive_History_Table,
            hive_history_temp_db=self.Hive_HistoryTemp_DB,
            hive_history_temp_table=self.Hive_HistoryTemp_Table,
            hive_import_pk_only_db=self.Hive_Import_PKonly_DB,
            hive_import_pk_only_table=self.Hive_Import_PKonly_Table,
            hive_delete_db=self.Hive_Delete_DB,
            hive_delete_table=self.Hive_Delete_Table,
            hive_column_name_import=self.Hive_ColumnName_Import,
            hive_column_name_insert=self.Hive_ColumnName_Insert,
            hive_column_name_update=self.Hive_ColumnName_Update,
            hive_column_name_delete=self.Hive_ColumnName_Delete,
            hive_column_name_iud=self.Hive_ColumnName_IUD,
            hive_column_name_history_timestamp=self.Hive_ColumnName_HistoryTimestamp,
            hive_column_name_source=self.Hive_ColumnName_Source,
        )

        self.import_config.lookup_connection_alias()

    def remove_temporary_files(self):
        self.import_config.remove_temporary_files()

    def check_time_window(self):
        self.import_config.check_time_window()

    def run_stage(self, stage, stageMax=None):
        self.import_config.set_stage(stage)

        if (
            self.import_config.common_config.get_config_value(
                key="import_stage_disable"
            )
            == True
        ):
            logging.error("Stage execution disabled from DBImport configuration")
            self.import_config.remove_temporary_files()
            sys.exit(1)

        temp_stage = self.import_config.get_stage()
        if stageMax == None:
            if temp_stage == stage:
                return True
            else:
                return False
        else:
            if temp_stage >= stage and temp_stage <= stageMax:
                return True
            else:
                return False

    def set_stage(self, stage, force=False):
        self.import_config.set_stage(stage, force=force)

    def get_stage(self):
        return self.import_config.get_stage()

    def clear_stage(self):
        self.import_config.clear_stage()

    def save_retry_attempt(self, stage):
        self.import_config.save_retry_attempt(stage)

    def set_stage_only_in_memory(self):
        self.import_config.set_stage_only_in_memory()

    def save_incr_pending_values(self):
        self.import_config.save_incr_pending_values()

    def save_incr_min_value(self):
        self.import_config.save_incr_min_value()

    def convert_stage_statistics_to_json(self):
        self.import_config.convert_stage_statistics_to_json()

    def save_stage_statistics(self):
        self.import_config.save_stage_statistics()

    def send_start_json(self):
        self.import_config.send_start_json()

    def invalidateImpala(self):
        self.import_config.invalidateImpala()

    def update_atlas_with_import_data(self):
        if self.atlasOperation.check_atlas_schema() == True:
            config_object = (
                self.import_config.common_config.get_atlas_discover_config_object()
            )
            self.atlasOperation.set_configuration(config_object)

            if self.import_config.common_config.source_columns_df.empty == True:
                self.import_config.get_jdbc_table_definition()

            self.atlasOperation.source_columns_df = (
                self.import_config.common_config.source_columns_df
            )
            self.atlasOperation.source_keys_df = (
                self.import_config.common_config.source_keys_df
            )

            start_stop_dict = self.import_config.stage.get_stage_start_stop(
                stage=self.import_config.importTool
            )

            try:
                self.atlasOperation.update_atlas_with_rdbms_data(
                    schema_name=self.import_config.source_schema,
                    table_name=self.import_config.source_table,
                )

                self.atlasOperation.update_atlas_with_import_lineage(
                    hive_db=self.Hive_DB,
                    source_schema=self.import_config.source_schema,
                    source_table=self.import_config.source_table,
                    sqoop_hdfs_location=self.import_config.sqoop_hdfs_location,
                    hive_table=self.Hive_Table,
                    start_stop_dict=start_stop_dict,
                    full_executed_command=self.import_config.fullExecutedCommand,
                    import_tool=self.import_config.importTool,
                )
            except:
                pass

            logging.info("")  # Just to get a blank row before connecting to Hive

    def check_db(self, hiveDB):
        try:
            self.common_operations.check_db(hiveDB)
        except databaseNotFound as errMsg:
            logging.error(errMsg)
            self.import_config.remove_temporary_files()
            sys.exit(1)
        except:
            self.import_config.remove_temporary_files()
            raise

    def get_jdbc_table_validation_data(self):
        if self.import_config.validationMethod == "customQuery":
            self.import_config.run_custom_validation_query_on_jdbc_table()
        else:
            self.get_jdbc_table_row_count()

    def validate_import_tool(self):
        # 		if self.import_config.etlEngine == constant.ETL_ENGINE_HIVE:
        if self.import_config.validationMethod == "customQuery":
            logging.info(
                "No validation of spark/sqoop import when running with a customQuery validation"
            )
            pass
        else:
            self.validate_import_row_count()

    def get_import_table_validation_data(self):
        if self.import_config.etlEngine == constant.ETL_ENGINE_HIVE:
            if self.import_config.validationMethod == "customQuery":
                if self.import_config.validationCustomQueryValidateImportTable == True:
                    self.run_custom_validation_query_on_import_table()
                else:
                    logging.info(
                        "No validation of import table as that is disabled in configuration"
                    )
            else:
                self.get_import_table_row_count()

    def get_target_table_validation_data(self, incrementalMax=None):
        if self.import_config.validationMethod == "customQuery":
            if self.import_config.etlEngine == constant.ETL_ENGINE_SPARK:
                raise invalidConfiguration(
                    "Custom Query validation is not supported with Spark as the ETL engine (yet)"
                )
            else:
                self.run_custom_validation_query_on_target_table()
        else:
            self.get_target_table_row_count(incrementalMax=incrementalMax)

    def run_custom_validation_query_on_import_table(self):
        logging.debug(
            "Executing import_operations.run_custom_validation_query_on_import_table()"
        )

        logging.info("Executing custom validation query on Import table.")

        query = self.import_config.validationCustomQueryHiveSQL
        query = query.replace("${HIVE_DB}", self.import_config.Hive_Import_DB)
        query = query.replace("${HIVE_TABLE}", self.import_config.Hive_Import_Table)

        logging.debug("Validation Query on Import table: %s" % (query))

        result_df = self.common_operations.execute_hive_query(query)
        result_json = result_df.to_json(orient="values")
        self.import_config.validationCustomQueryHiveValue = result_json

        if len(self.import_config.validationCustomQueryHiveValue) > 1024:
            logging.warning(
                "'%s' is to large."
                % (self.import_config.validationCustomQueryHiveValue)
            )
            raise invalidConfiguration(
                "The size of the json document on the custom query exceeds 1024 bytes. Change the query to create a result with less than 512 bytes"
            )

        self.import_config.save_custom_sql_validation_hive_value(jsonValue=result_json)

        logging.debug("resultDF:")
        logging.debug(result_df)
        logging.debug("resultJSON: %s" % (result_json))

        logging.debug(
            "Executing import_operations.run_custom_validation_query_on_import_table() - Finished"
        )

    def run_custom_validation_query_on_target_table(self):
        logging.debug(
            "Executing import_operations.run_custom_validation_query_on_target_table()"
        )

        logging.info("Executing custom validation query on Target table.")

        query = self.import_config.validationCustomQueryHiveSQL
        query = query.replace("${HIVE_DB}", self.import_config.Hive_DB)
        query = query.replace("${HIVE_TABLE}", self.import_config.Hive_Table)

        logging.debug("Validation Query on Target table: %s" % (query))

        result_df = self.common_operations.execute_hive_query(query)
        result_json = result_df.to_json(orient="values")
        self.import_config.validationCustomQueryHiveValue = result_json

        if len(self.import_config.validationCustomQueryHiveValue) > 1024:
            logging.warning(
                "'%s' is to large."
                % (self.import_config.validationCustomQueryHiveValue)
            )
            raise invalidConfiguration(
                "The size of the json document on the custom query exceeds 1024 bytes. Change the query to create a result with less than 512 bytes"
            )

        self.import_config.save_custom_sql_validation_hive_value(jsonValue=result_json)

        logging.debug("resultDF:")
        logging.debug(result_df)
        logging.debug("resultJSON: %s" % (result_json))

        logging.debug(
            "Executing import_operations.run_custom_validation_query_on_target_table() - Finished"
        )

    def get_jdbc_table_row_count(self):
        try:
            self.import_config.get_jdbc_table_row_count()
        except:
            logging.exception("Fatal error when reading source table row count")
            self.import_config.remove_temporary_files()
            sys.exit(1)

    def get_mongo_row_count(self):
        try:
            self.import_config.get_mongo_row_count()
        except:
            logging.exception("Fatal error when reading Mongo row count")
            self.import_config.remove_temporary_files()
            sys.exit(1)

    def get_import_view_row_count_as_source(self, incr=False):
        where_statement = None
        if self.import_config.importPhase == constant.IMPORT_PHASE_ORACLE_FLASHBACK:
            # Needed otherwise we will count rows that will be deleted and that will cause the validation to fail
            where_statement = "datalake_flashback_operation != 'D' or datalake_flashback_operation is null"

        try:
            import_view_row_count = self.common_operations.get_hive_table_row_count(
                self.import_config.Hive_Import_DB,
                self.import_config.Hive_Import_Table,
                whereStatement=where_statement,
            )
            self.import_config.save_source_table_row_count(
                import_view_row_count, incr=incr
            )
        except:
            logging.exception("Fatal error when reading Hive view row count")
            self.import_config.remove_temporary_files()
            sys.exit(1)

    def get_import_table_row_count(self, exclude_oracle_flashback_deletes=False):

        where_statement = None

        if exclude_oracle_flashback_deletes == True:
            # Needed otherwise we will count rows that will be deleted and that will cause the validation to fail
            where_statement = "datalake_flashback_operation != 'D' or datalake_flashback_operation is null"

        try:
            import_table_row_count = self.common_operations.get_hive_table_row_count(
                self.import_config.Hive_Import_DB,
                self.import_config.Hive_Import_Table,
                whereStatement=where_statement,
            )
            self.import_config.save_hive_table_row_count(import_table_row_count)
        except:
            logging.exception("Fatal error when reading Hive table row count")
            self.import_config.remove_temporary_files()
            sys.exit(1)

    def get_spark_table_row_count(self, hive_db, hive_table, where_statement=None):
        logging.debug("Executing import_operations.get_spark_table_row_count()")
        logging.info("Reading the number of rows from %s.%s" % (hive_db, hive_table))
        row_count = 0

        query = "select count(1) as rowcount from %s.`%s`.`%s` " % (
            self.common_operations.sparkCatalogName,
            hive_db,
            hive_table,
        )
        if where_statement != None:
            query += "where " + where_statement
        else:
            query += "limit 1"

        self.start_spark()
        logging.info(query)
        table_size_result = self.spark.sql(query)
        row_count = int(table_size_result.first()["rowcount"])

        logging.debug("Rowcount from %s.%s: %s" % (hive_db, hive_table, row_count))
        logging.debug(
            "Executing import_operations.get_spark_table_row_count() - Finished"
        )
        return row_count

    def get_target_table_row_count(self, incrementalMax=None):
        try:
            where_statement = self.import_config.get_incr_where_statement(
                ignoreIfOnlyIncrMax=True,
                ignoreSQLwhereAddition=True,
                incrementalMax=incrementalMax,
            )

            if (
                where_statement == None
                and self.import_config.import_with_merge == True
                and self.import_config.soft_delete_during_merge == True
            ):
                # whereStatement = "datalake_iud != 'D' or datalake_iud is null"
                where_statement = "%s != 'D' or %s is null" % (
                    self.Hive_ColumnName_IUD,
                    self.Hive_ColumnName_IUD,
                )

            if (
                where_statement == None
                and self.import_config.importPhase == constant.IMPORT_PHASE_FULL
                and self.import_config.etlPhase == constant.ETL_PHASE_INSERT
            ):
                # whereStatement = "datalake_import == '%s'"%(self.import_config.sqoop_last_execution_timestamp)
                where_statement = "%s == '%s'" % (
                    self.Hive_ColumnName_Import,
                    self.import_config.sqoop_last_execution_timestamp,
                )

            logging.debug("whereStatement: %s" % (where_statement))

            if self.import_config.etlEngine == constant.ETL_ENGINE_HIVE:
                target_table_row_count = (
                    self.common_operations.get_hive_table_row_count(
                        self.import_config.Hive_DB,
                        self.import_config.Hive_Table,
                        whereStatement=where_statement,
                    )
                )

            if self.import_config.etlEngine == constant.ETL_ENGINE_SPARK:
                target_table_row_count = self.get_spark_table_row_count(
                    self.import_config.Hive_DB,
                    self.import_config.Hive_Table,
                    where_statement=where_statement,
                )

            self.import_config.save_hive_table_row_count(target_table_row_count)

        except:
            logging.exception("Fatal error when reading Hive table row count")
            self.import_config.remove_temporary_files()
            sys.exit(1)

    def clear_validation_data(self):
        try:
            self.import_config.clear_validation_data()
        except:
            logging.exception(
                "Fatal error when clearing validation data from previous imports"
            )
            self.import_config.remove_temporary_files()
            sys.exit(1)

    def validate_import_table(self):
        # This is executed as the first step in the ETL stage. If it's not Hive, then we dont need to execute this as the table is already verified by the import tool
        if self.import_config.etlEngine == constant.ETL_ENGINE_HIVE:
            if self.import_config.validationMethod == "customQuery":
                if self.import_config.validationCustomQueryValidateImportTable == True:
                    if self.import_config.validate_custom_query() == True:
                        logging.info("Import table validation successful!")
            else:
                if self.import_config.import_is_incremental == False:
                    self.validate_row_count()
                else:
                    self.validate_incr_row_count()

    def validate_target_table(self):
        if self.import_config.validationMethod == "customQuery":
            if self.import_config.etlEngine == constant.ETL_ENGINE_SPARK:
                raise invalidConfiguration(
                    "Custom Query validation is not supported with Spark as the ETL engine (yet)"
                )

            if self.import_config.validate_custom_query() == True:
                logging.info("Target table validation successful!")
        else:
            self.validate_row_count()

    def validate_row_count(self):
        logging.debug("Executing import_operations.validate_row_count()")
        try:
            validate_result = self.import_config.validate_row_count(
                validateSqoop=False, incremental=False
            )
        except:
            logging.exception("Fatal error when validating imported rows")
            self.import_config.remove_temporary_files()
            sys.exit(1)

        if validate_result == False:
            self.import_config.remove_temporary_files()
            sys.exit(1)

    def validate_import_row_count(self):
        logging.debug("Executing import_operations.validate_import_row_count()")
        try:
            validate_result = self.import_config.validate_row_count(
                validateSqoop=True, incremental=False
            )
        except:
            logging.exception("Fatal error when validating imported rows by sqoop")
            self.import_config.remove_temporary_files()
            sys.exit(1)

        if validate_result == False:
            self.import_config.remove_temporary_files()
            sys.exit(1)

    def validate_incr_row_count(self):
        logging.debug("Executing import_operations.validate_incr_row_count()")
        try:
            validate_result = self.import_config.validate_row_count(
                validateSqoop=False, incremental=True
            )
        except:
            logging.exception("Fatal error when validating imported incremental rows")
            self.import_config.remove_temporary_files()
            sys.exit(1)

        if validate_result == False:
            self.import_config.remove_temporary_files()
            sys.exit(1)

    def get_source_table_schema(self):
        try:
            if self.import_config.mongoImport == False:
                # Mongo gets is TableDefinition from the spark code. We cant use the get_jdbc_table_definition() for Mongo
                self.import_config.get_jdbc_table_definition()

            self.import_config.update_last_update_from_source()
            self.import_config.remove_fk_for_table()
            self.import_config.save_column_data()
            self.import_config.set_primary_key_column()
            self.import_config.save_key_data()
            self.import_config.save_index_data()
            self.import_config.save_generated_data()
        except invalidConfiguration as errMsg:
            logging.error(errMsg)
            self.import_config.remove_temporary_files()
            sys.exit(1)
        except SystemExit:
            self.import_config.remove_temporary_files()
            sys.exit(1)
        except:
            logging.exception(
                "Fatal error when reading and/or processing source table schema"
            )
            self.import_config.remove_temporary_files()
            sys.exit(1)

    def discover_and_add_tables_from_source(
        self,
        dbalias,
        hive_db,
        schema_filter=None,
        table_filter=None,
        add_schema_to_table=False,
        add_custom_text=None,
        add_counter_to_table=False,
        counter_start=None,
        only_json_output=False,
    ):
        """This is the main function to search for tables/view on source database and add them to import_tables"""
        logging.debug(
            "Executing import_operations.discover_and_add_tables_from_source()"
        )
        error_during_add = False
        self.import_config.lookup_connection_alias(connection_alias=dbalias)

        if self.import_config.mongoImport == False:
            source_df = self.import_config.common_config.get_jdbc_tables_and_views(
                schemaFilter=schema_filter, tableFilter=table_filter
            )
        else:
            source_df = self.import_config.common_config.get_mongo_collections(
                collection_filter=table_filter
            )

        if len(source_df) == 0:
            print(
                "There are no tables in the source database that we dont already have in DBImport"
            )
            self.import_config.remove_temporary_files()
            sys.exit(0)

        import_df = self.import_config.get_import_tables(hive_db=hive_db)
        import_df = import_df.loc[import_df["dbalias"] == dbalias].filter(
            ["source_schema", "source_table"]
        )
        import_df.rename(
            columns={"source_schema": "schema", "source_table": "table"}, inplace=True
        )

        merge_df = pd.merge(
            source_df, import_df, on=None, how="outer", indicator="Exist"
        )
        merge_df["hiveTable"] = merge_df["table"]
        discovered_tables = len(merge_df.loc[merge_df["Exist"] == "left_only"])

        if (
            add_counter_to_table == True
            or add_schema_to_table == True
            or add_custom_text != None
        ):
            for index, row in merge_df.iterrows():
                if merge_df.loc[index, "Exist"] == "left_only":
                    merge_df.loc[index, "hiveTable"] = "_%s" % (
                        merge_df.loc[index, "hiveTable"].strip()
                    )

        if add_counter_to_table == True:
            if counter_start == None:
                counter_start = "1"
            number_length = len(counter_start)
            try:
                start_value = int(counter_start)
            except ValueError:
                logging.error("The value specified for --counterStart must be a number")
                self.import_config.remove_temporary_files()
                sys.exit(1)

            for index, row in merge_df.iterrows():
                if merge_df.loc[index, "Exist"] == "left_only":
                    zero_to_add = ""
                    while len(zero_to_add) < (number_length - len(str(start_value))):
                        zero_to_add += "0"

                    merge_df.loc[index, "hiveTable"] = "%s%s%s" % (
                        zero_to_add,
                        start_value,
                        merge_df.loc[index, "hiveTable"],
                    )
                    start_value += 1

        if add_schema_to_table == True:
            for index, row in merge_df.iterrows():
                if merge_df.loc[index, "Exist"] == "left_only":
                    merge_df.loc[index, "hiveTable"] = "%s%s" % (
                        merge_df.loc[index, "schema"].lower().strip(),
                        merge_df.loc[index, "hiveTable"],
                    )

        if add_custom_text != None:
            for index, row in merge_df.iterrows():
                if merge_df.loc[index, "Exist"] == "left_only":
                    merge_df.loc[index, "hiveTable"] = "%s%s" % (
                        add_custom_text.lower().strip(),
                        merge_df.loc[index, "hiveTable"],
                    )

        if discovered_tables == 0:
            print(
                "There are no tables in the source database that we dont already have in DBImport"
            )
            self.import_config.remove_temporary_files()
            sys.exit(0)

        # At this stage, we have discovered tables in the source system that we dont know about in DBImport

        if only_json_output == False:
            print(
                "The following tables and/or views have been discovered in the source database and not found in DBImport"
            )
            print("")
            print(
                "%-20s%-40s%-30s%-20s%s"
                % ("Hive DB", "Hive Table", "Connection Alias", "Schema", "Table/View")
            )
            print(
                "============================================================================================================================="
            )

            for index, row in merge_df.loc[merge_df["Exist"] == "left_only"].iterrows():
                print(
                    "%-20s%-40s%-30s%-20s%s"
                    % (
                        hive_db,
                        row["hiveTable"].lower(),
                        dbalias,
                        row["schema"],
                        row["table"],
                    )
                )

            answer = input("Do you want to add these imports to DBImport? (y/N): ")
            # 			answer = "N"
            if answer == "y":
                print("")
                for index, row in merge_df.loc[
                    merge_df["Exist"] == "left_only"
                ].iterrows():
                    if (
                        self.common_operations.metastore_type
                        == constant.CATALOG_HIVE_DIRECT
                    ):
                        add_result = self.import_config.add_import_table(
                            hive_db=hive_db,
                            hive_table=row["hiveTable"].lower(),
                            dbalias=dbalias,
                            schema=row["schema"].strip(),
                            table=row["table"].strip(),
                            catalog=constant.CATALOG_HIVE_DIRECT,
                        )

                    elif self.common_operations.metastore_type == constant.CATALOG_GLUE:
                        add_result = self.import_config.add_import_table(
                            hive_db=hive_db,
                            hive_table=row["hiveTable"].lower(),
                            dbalias=dbalias,
                            schema=row["schema"].strip(),
                            table=row["table"].strip(),
                            catalog=constant.CATALOG_GLUE,
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

        else:
            # Only output the json file
            # This is used by the rest interface to return a list of tables to the webUI

            table_list = []
            for index, row in merge_df.loc[merge_df["Exist"] == "left_only"].iterrows():
                result_dict = {}
                result_dict["database"] = hive_db
                result_dict["table"] = row["hiveTable"].lower()
                result_dict["connection"] = dbalias
                result_dict["sourceSchema"] = row["schema"]
                result_dict["sourceTable"] = row["table"]
                table_list.append(result_dict)

            json_result = json.dumps(table_list)
            print(json_result)

        logging.debug(
            "Executing import_operations.discover_and_add_tables_from_source() - Finished"
        )

    def check_oracle_flashback_scn_number(self):
        """Checks if the Oracle Flashback SCN number used in the previous import is valid. If not, we force a full import"""

        incr_where_statement = self.import_config.get_incr_where_statement(
            whereForSqoop=True
        ).replace('"', "'")
        query = "%s %s AND 1 = 0" % (
            self.import_config.sqlGeneratedSqoopQuery,
            incr_where_statement,
        )

        try:
            self.import_config.common_config.execute_jdbc_query(query)
        except SQLerror as e:
            error_msg = str(e)
            if (
                "java.sql.SQLException: ORA-08181:" in error_msg
                or "java.sql.SQLException: ORA-30052" in error_msg
            ):
                logging.warning(
                    "The last import with Oracle Flashback is to far into the past. In order to continue, we will be forced to do a new initial load"
                )
                logging.warning("Reseting Oracle Flashback versions")

                self.reset_incr_min_max_values(maxValue=None)
                self.truncateTargetTable()
                self.import_config.sqoopIncrMinvaluePending = None
                self.import_config.sqoopIncrMaxvaluePending = None
                self.import_config.incr_maxvalue = None

            elif "java.sql.SQLException: ORA-01466:" in error_msg:
                logging.warning(
                    "Table definition have changed. One reason when this happens is when the source table is loaded or truncated. In order to continue, we will be forced to do a new initial load"
                )
                logging.warning("Reseting Oracle Flashback versions")

                self.reset_incr_min_max_values(maxValue=None)
                self.truncateTargetTable()
                self.import_config.sqoopIncrMinvaluePending = None
                self.import_config.sqoopIncrMaxvaluePending = None
                self.import_config.incr_maxvalue = None

            else:
                logging.error(
                    "Unknown Oracle Flashback error when checking for valid SCN number"
                )
                print(error_msg)
                self.import_config.remove_temporary_files()
                sys.exit(1)

    def convert_spark_type_to_binary(self, schema):
        import pyspark.sql

        if schema.__class__ == pyspark.sql.types.StructType:
            return pyspark.sql.types.StructType(
                [self.convert_spark_type_to_binary(f) for f in schema.fields]
            )
        if schema.__class__ == pyspark.sql.types.StructField:
            return pyspark.sql.types.StructField(
                schema.name,
                self.convert_spark_type_to_binary(schema.dataType),
                schema.nullable,
            )
        if schema.__class__ == pyspark.sql.types.ArrayType:
            return pyspark.sql.types.ArrayType(
                self.convert_spark_type_to_binary(schema.elementType)
            )
        if schema.__class__ == pyspark.sql.types.BinaryType:
            return pyspark.sql.types.StringType()
        return schema

    def convert_spark_schema(self, schema, force_date_as_string=False):
        import pyspark.sql

        if schema.__class__ == pyspark.sql.types.StructType:
            return pyspark.sql.types.StructType(
                [
                    self.convert_spark_schema(f, force_date_as_string)
                    for f in schema.fields
                ]
            )
        if schema.__class__ == pyspark.sql.types.StructField:
            return pyspark.sql.types.StructField(
                schema.name,
                self.convert_spark_schema(schema.dataType, force_date_as_string),
                schema.nullable,
            )
        if schema.__class__ == pyspark.sql.types.ArrayType:
            return pyspark.sql.types.ArrayType(
                self.convert_spark_schema(schema.elementType, force_date_as_string)
            )
        if schema.__class__ == pyspark.sql.types.NullType:
            return pyspark.sql.types.StringType()
        if force_date_as_string:
            if schema.__class__ in [
                pyspark.sql.types.DateType,
                pyspark.sql.types.TimestampType,
            ]:
                return pyspark.sql.types.StringType()
        return schema

    def run_spark_import_for_mongo(self):
        logging.debug("Executing import_operations.run_spark_import_for_mongo()")

        self.spark_start_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        self.spark_start_uts = int(time.time())
        spark_query = ""

        # Fetch the number of executors that should be used.
        # This is already handled in self.import_config.calculate_job_mappers, so we use that
        # function for this even if we dont need the JobMappers
        self.import_config.calculate_job_mappers()

        # Override Spark Executor memory if it's set on the import table configuration
        if (
            self.import_config.spark_executor_memory != None
            and self.import_config.spark_executor_memory.strip() != ""
        ):
            self.import_config.common_config.sparkExecutorMemory = (
                self.import_config.spark_executor_memory
            )

        logging.debug("")
        logging.debug(
            "======================================================================="
        )
        logging.debug("Target HDFS: %s" % (self.import_config.sqoop_hdfs_location))
        logging.debug(
            "Spark Executor Memory: %s"
            % (self.import_config.common_config.sparkExecutorMemory)
        )
        logging.debug(
            "Spark Max Executors: %s" % (self.import_config.sparkMaxExecutors)
        )
        logging.debug(
            "Spark Dynamic Executors: %s"
            % (self.import_config.common_config.sparkDynamicAllocation)
        )
        logging.debug(
            "======================================================================="
        )

        # Create a valid PYSPARK_SUBMIT_ARGS string
        spark_pyspark_submit_args = "--jars "
        spark_jars = ""

        first_loop = True
        if self.import_config.common_config.sparkJarFiles.strip() != "":
            for jar_file in self.import_config.common_config.sparkJarFiles.split(","):
                if first_loop == False:
                    spark_pyspark_submit_args += ","
                    spark_jars += ","
                spark_pyspark_submit_args += jar_file.strip()
                spark_jars += jar_file.strip()
                first_loop = False

        for jar_file in self.import_config.common_config.jdbc_classpath.split(":"):
            if first_loop == False:
                spark_pyspark_submit_args += ","
                spark_jars += ","
            spark_pyspark_submit_args += jar_file.strip()
            spark_jars += jar_file.strip()
            first_loop = False

        if self.import_config.common_config.sparkPyFiles.strip() != "":
            spark_pyspark_submit_args += " --py-files "
            first_loop = True
            for py_file in self.import_config.common_config.sparkPyFiles.split(","):
                if first_loop == False:
                    spark_pyspark_submit_args += ","
                spark_pyspark_submit_args += py_file.strip()
                first_loop = False

        spark_pyspark_submit_args += " pyspark-shell"

        # Setup the additional path required to find the libraries/modules
        for path in self.import_config.common_config.sparkPathAppend.split(","):
            sys.path.append(path.strip())

        # Set required OS parameters
        # os.environ['HDP_VERSION'] = self.import_config.common_config.sparkHDPversion
        os.environ["PYSPARK_SUBMIT_ARGS"] = spark_pyspark_submit_args

        print(" _____________________ ")
        print("|                     |")
        print("| Spark Import starts |")
        print("|_____________________|")
        print("")

        mongo_uri = "mongodb://%s:%s@%s:%s/%s.%s" % (
            self.import_config.common_config.jdbc_username,
            self.import_config.common_config.jdbc_password,
            self.import_config.common_config.jdbc_hostname,
            self.import_config.common_config.jdbc_port,
            self.import_config.common_config.jdbc_database,
            self.import_config.source_table,
        )

        if self.import_config.common_config.mongoAuthSource != None:
            mongo_uri = (
                mongo_uri
                + "?authSource="
                + self.import_config.common_config.mongoAuthSource
            )

        # import all packages after the environment is set
        import pyspark.sql
        import pyspark.sql.types
        from pyspark.context import SparkConf, SparkContext
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import udf

        conf = SparkConf()
        conf.setMaster(self.import_config.common_config.sparkMaster)
        conf.set("spark.mongodb.input.uri", mongo_uri)
        conf.set(
            "spark.submit.deployMode", self.import_config.common_config.sparkDeployMode
        )
        conf.setAppName("DBImport Import - %s.%s" % (self.Hive_DB, self.Hive_Table))
        conf.set("spark.jars", spark_jars)
        conf.set(
            "spark.executor.memory",
            self.import_config.common_config.sparkExecutorMemory,
        )
        conf.set("spark.yarn.queue", self.import_config.common_config.sparkYarnQueue)
        conf.set("spark.hadoop.yarn.timeline-service.enabled", "false")
        if self.import_config.common_config.sparkDynamicAllocation == True:
            conf.set("spark.shuffle.service.enabled", "true")
            conf.set("spark.dynamicAllocation.enabled", "true")
            conf.set("spark.dynamicAllocation.minExecutors", "0")
            conf.set(
                "spark.dynamicAllocation.maxExecutors",
                str(self.import_config.sparkMaxExecutors),
            )
            logging.info(
                "Number of executors is dynamic with a max value of %s executors"
                % (self.import_config.sparkMaxExecutors)
            )
        else:
            conf.set("spark.dynamicAllocation.enabled", "false")
            conf.set("spark.shuffle.service.enabled", "false")
            if self.import_config.sqlSessions < self.import_config.sparkMaxExecutors:
                conf.set(
                    "spark.executor.instances", str(self.import_config.sqlSessions)
                )
                logging.info(
                    "Number of executors is fixed at %s"
                    % (self.import_config.sqlSessions)
                )
            else:
                conf.set(
                    "spark.executor.instances",
                    str(self.import_config.sparkMaxExecutors),
                )
                logging.info(
                    "Number of executors is fixed at %s"
                    % (self.import_config.sparkMaxExecutors)
                )

        sys.stdout.flush()

        sc = SparkContext(conf=conf)
        sc.addPyFile("%s/bin/common/sparkUDF.py" % (os.environ["DBIMPORT_HOME"]))
        from sparkUDF import base64_encode_array, parse_struct_date

        sys.stdout.flush()

        spark = SparkSession(sc)
        spark.sql("set spark.sql.orc.impl=native")
        sys.stdout.flush()

        self.yarnApplicationID = sc.applicationId
        logging.info("Yarn application started with id %s" % (self.yarnApplicationID))
        sys.stdout.flush()

        print("Loading data from Mongo")
        sys.stdout.flush()
        df = spark.read.format("mongo").load()
        sys.stdout.flush()

        # The logic here is the following.
        # Read 2 schemas but one of the forces all data and timestamp columns into strings. In the DF where we are not forcing them to string
        # we search for timestamp and date columns and saves them. Once we have them, we try to run a Spark SQL against that column to determine
        # if the data in the column can be a timestamp or not.
        # If we find out that the column is infact a timestamp, we convert the column first to a string based on unixtimestamp and later
        # cast it to a timestamp. The result of this is that the DF with strings gets it's columns replace to timestamp where the data actually
        # is a timestamp.
        # Without this logic, the data would be strings for ever or would generate errors when the syntax is not a timestamp.

        print("Loading Schema from Mongo")
        sys.stdout.flush()
        spark_schema = self.convert_spark_schema(df.schema, force_date_as_string=False)
        spark_schema_string = self.convert_spark_schema(
            df.schema, force_date_as_string=True
        )
        df = spark.read.format("mongo").schema(spark_schema).load()
        df_string = spark.read.format("mongo").schema(spark_schema_string).load()
        sys.stdout.flush()

        # Save the datatypes. Will later be used to update import_columns table
        print("Getting column types")
        sys.stdout.flush()
        data_types = dict(df.dtypes)
        data_types_string = dict(df_string.dtypes)

        # Find what columns are timestamp or date columns
        date_columns = {}
        for column, column_type in data_types.items():
            if column_type.lower() in ["timestamp", "date"]:
                date_columns[column] = column_type

        # Test the columns we just found to verify if it actually is a timestamp or not
        df_string.createOrReplaceTempView("find_timestamp")
        sys.stdout.flush()

        incorrect_date_columns = []
        correct_date_columns = {}
        for column, column_type in date_columns.items():
            print(
                "Checking if column '%s' really is a timestamp column and compatible with Hive"
                % (column)
            )
            sys.stdout.flush()

            df_date = spark.sql(
                "SELECT %s FROM find_timestamp where %s is not null and CAST(%s as timestamp) is not null"
                % (column, column, column)
            )
            if len(df_date.head(1)) != 0:
                incorrect_date_columns.append(column)
            else:
                correct_date_columns[column] = column_type

        # Convert the column in the DF based on the fact that it can be converted or not to a timestamp
        udf_parse_struct_date = udf(parse_struct_date, pyspark.sql.types.StringType())

        for column in incorrect_date_columns:
            print("Converting date/timestamp column '%s'" % (column))
            sys.stdout.flush()
            df_string = df_string.withColumn(column, udf_parse_struct_date(column))

        for column, column_type in correct_date_columns.items():
            print("Converting date/timestamp column '%s'" % (column))
            sys.stdout.flush()
            df_string = df_string.withColumn(column, udf_parse_struct_date(column))
            df_string = df_string.withColumn(
                column, df_string[column].cast(column_type)
            )

        sys.stdout.flush()

        # Find and convert binary fields into a Base64 encoded binary field
        print("Finding binary columns")
        sys.stdout.flush()
        columns = df_string.columns
        types = [f.dataType for f in df_string.schema.fields]
        columns_and_types = list(zip(columns, types))

        column_types_binary = {}
        for column, column_type in columns_and_types:
            if "struct<subType:tinyint,data:binary>" in data_types[column]:
                column_types_binary[column] = self.convert_spark_type_to_binary(
                    column_type
                )

        for column, column_type in column_types_binary.items():
            print("Converting binary column '%s'" % (column))
            sys.stdout.flush()
            udf_base64_encode_array = udf(base64_encode_array, column_type)
            df_string = df_string.withColumn(column, udf_base64_encode_array(column))

        sys.stdout.flush()

        # Get new dataTypes as we have updated and converted from binary to string
        print("Getting updated column types after column changes")
        sys.stdout.flush()
        data_types = dict(df_string.dtypes)
        sys.stdout.flush()

        print("Writing data to HDFS as ORC")
        sys.stdout.flush()
        df_string.write.mode("overwrite").format("orc").save(
            self.import_config.sqoop_hdfs_location
        )
        sys.stdout.flush()

        # Get the number of rows from the ORC files
        print("Reading ORC files from HDFS to verify size and rows")
        sys.stdout.flush()
        hdfs_data_df = spark.read.format("orc").load(
            self.import_config.sqoop_hdfs_location
        )
        rows_written_by_spark = hdfs_data_df.count()
        sys.stdout.flush()
        logging.info("Number of rows written by spark = %s" % (rows_written_by_spark))
        sys.stdout.flush()

        # Get size of all files on HDFS that spark wrote
        hdfs_command_list = [
            "hdfs",
            "dfs",
            "-du",
            "-s",
            self.import_config.sqoop_hdfs_location,
        ]
        hdfs_proc = subprocess.Popen(
            hdfs_command_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        std_out, std_err = hdfs_proc.communicate()
        std_out = std_out.decode("utf-8").rstrip()
        std_err = std_err.decode("utf-8").rstrip()

        size_written_by_spark = std_out.split()[0]
        logging.info(
            "Size of data written by spark = %s bytes" % (size_written_by_spark)
        )
        sys.stdout.flush()

        sc.stop()

        print(" ________________________ ")
        print("|                        |")
        print("| Spark Import completed |")
        print("|________________________|")
        print("")
        sys.stdout.flush()

        # As the import can take a very long time on a large table, we need to verify that the MySQL connection against the configuration database is still valid
        self.import_config.reconnect_config_database()

        column_dict = {}
        for column in data_types:
            column_type = data_types[column]

            for list1 in column_type.split("<"):
                for list2 in list1.split(">"):
                    for list3 in list2.split(","):
                        if len(list3.split(":")) == 2:
                            column_type = re.sub(
                                r"([<>,])" + list3 + "([<>,])",
                                r"\1`%s`:%s\2"
                                % (list3.split(":")[0], list3.split(":")[1]),
                                column_type,
                            )
            column_dict[column] = column_type

        data_types = column_dict

        # Create the colum pandasDF as we cant get that from the source as we do on normal JDBC sources
        rows_list = []
        for column in data_types:
            line_dict = {}
            line_dict["TABLE_COMMENT"] = None
            line_dict["SOURCE_COLUMN_NAME"] = column
            line_dict["SOURCE_COLUMN_TYPE"] = data_types[column]
            line_dict["SOURCE_COLUMN_LENGTH"] = None
            line_dict["SOURCE_COLUMN_COMMENT"] = None
            line_dict["IS_NULLABLE"] = None
            line_dict["TABLE_TYPE"] = None
            line_dict["TABLE_CREATE_TIME"] = None
            line_dict["DEFAULT_VALUE"] = None
            rows_list.append(line_dict)
        self.import_config.common_config.source_columns_df = pd.DataFrame(rows_list)
        self.import_config.common_config.source_keys_df = result_df = pd.DataFrame()

        # Update import_columns with column information
        self.get_source_table_schema()

        try:
            self.import_config.save_import_statistics(
                self.spark_start_uts,
                sqoop_size=size_written_by_spark,
                sqoop_rows=rows_written_by_spark,
                sqoop_mappers=self.import_config.sqlSessions,
            )
        except:
            logging.exception("Fatal error when saving spark statistics")
            self.import_config.remove_temporary_files()
            sys.exit(1)

        logging.debug(
            "Executing import_operations.run_spark_import_for_mongo() - Finished"
        )

    def start_spark(self):
        logging.debug("Executing import_operations.start_spark()")

        if self.spark != None:
            # Spark is already running. Skip this
            return

        # Override Spark Executor memory if it's set on the import table configuration
        if (
            self.import_config.spark_executor_memory != None
            and self.import_config.spark_executor_memory.strip() != ""
        ):
            self.import_config.common_config.sparkExecutorMemory = (
                self.import_config.spark_executor_memory
            )

        logging.debug("")
        logging.debug(
            "======================================================================="
        )
        logging.debug("splitByColumn: %s" % (self.import_config.splitByColumn))
        logging.debug(
            "sqoopBoundaryQuery: %s" % (self.import_config.sqoopBoundaryQuery)
        )
        logging.debug("parallell sessions: %s" % (self.import_config.sqlSessions))
        logging.debug("Target HDFS: %s" % (self.import_config.sqoop_hdfs_location))
        logging.debug(
            "Spark Executor Memory: %s"
            % (self.import_config.common_config.sparkExecutorMemory)
        )
        logging.debug(
            "Spark Max Executors: %s" % (self.import_config.sparkMaxExecutors)
        )
        logging.debug(
            "Spark Dynamic Executors: %s"
            % (self.import_config.common_config.sparkDynamicAllocation)
        )
        logging.debug(
            "======================================================================="
        )

        # Create a valid PYSPARK_SUBMIT_ARGS string
        spark_pyspark_submit_args = "--jars "
        spark_jars = ""

        first_loop = True
        if self.import_config.common_config.sparkJarFiles.strip() != "":
            for jar_file in self.import_config.common_config.sparkJarFiles.split(","):
                if first_loop == False:
                    spark_pyspark_submit_args += ","
                    spark_jars += ","
                spark_pyspark_submit_args += jar_file.strip()
                spark_jars += jar_file.strip()
                first_loop = False

        for jar_file in self.import_config.common_config.jdbc_classpath.split(":"):
            if first_loop == False:
                spark_pyspark_submit_args += ","
                spark_jars += ","
            spark_pyspark_submit_args += jar_file.strip()
            spark_jars += jar_file.strip()
            first_loop = False

        if self.import_config.common_config.sparkPyFiles.strip() != "":
            spark_pyspark_submit_args += " --py-files "
            first_loop = True
            for py_file in self.import_config.common_config.sparkPyFiles.split(","):
                if first_loop == False:
                    spark_pyspark_submit_args += ","
                spark_pyspark_submit_args += py_file.strip()
                first_loop = False

        spark_pyspark_submit_args += " pyspark-shell"

        # Setup the additional path required to find the libraries/modules
        for path in self.import_config.common_config.sparkPathAppend.split(","):
            sys.path.append(path.strip())

        # Set required OS parameters
        # os.environ['HDP_VERSION'] = self.import_config.common_config.sparkHDPversion
        os.environ["PYSPARK_SUBMIT_ARGS"] = spark_pyspark_submit_args
        # print(sparkPysparkSubmitArgs)

        if self.import_config.etlEngine == constant.ETL_ENGINE_HIVE:
            print(" _____________________ ")
            print("|                     |")
            print("| Spark Import starts |")
            print("|_____________________|")
            print("")

        import pyspark

        conf = pyspark.context.SparkConf()
        conf.setMaster(self.import_config.common_config.sparkMaster)
        conf.set(
            "spark.submit.deployMode", self.import_config.common_config.sparkDeployMode
        )
        conf.setAppName("DBImport Import - %s.%s" % (self.Hive_DB, self.Hive_Table))
        conf.set("spark.jars", spark_jars)
        if (
            self.import_config.common_config.sparkPackages != None
            and self.import_config.common_config.sparkPackages != ""
        ):
            conf.set(
                "spark.jars.packages", self.import_config.common_config.sparkPackages
            )
        conf.set(
            "spark.executor.memory",
            self.import_config.common_config.sparkExecutorMemory,
        )
        conf.set("spark.yarn.queue", self.import_config.common_config.sparkYarnQueue)
        conf.set("spark.port.maxRetries", "128")
        if self.import_config.common_config.sparkLogLevel != None:
            conf.set("spark.log.level", self.import_config.common_config.sparkLogLevel)
        conf.set("spark.hadoop.yarn.timeline-service.enabled", "false")
        conf.set("spark.driver.log.persistToDfs.enabled", "false")

        if self.import_config.common_config.sparkMajorVersion == 2:
            conf.set(
                "spark.yarn.keytab", self.import_config.common_config.kerberosKeytab
            )
            conf.set(
                "spark.yarn.principal",
                self.import_config.common_config.kerberosPrincipal,
            )
        else:
            conf.set(
                "spark.kerberos.keytab", self.import_config.common_config.kerberosKeytab
            )
            conf.set(
                "spark.kerberos.principal",
                self.import_config.common_config.kerberosPrincipal,
            )
            conf.set("spark.sql.debug.maxToStringFields", 1000)

        if self.import_config.etlEngine == constant.ETL_ENGINE_SPARK:
            if self.common_operations.metastore_type == constant.CATALOG_GLUE:
                conf.set(
                    "spark.sql.extensions",
                    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
                )
                conf.set(
                    "spark.sql.catalog.glue", "org.apache.iceberg.spark.SparkCatalog"
                )
                conf.set(
                    "spark.sql.catalog.glue.warehouse", self.import_config.hdfsBaseDir
                )
                conf.set(
                    "spark.sql.catalog.glue.catalog-impl",
                    "org.apache.iceberg.aws.glue.GlueCatalog",
                )
                conf.set(
                    "spark.sql.catalog.glue.io-impl",
                    "org.apache.iceberg.aws.s3.S3FileIO",
                )
            else:
                if self.import_config.common_config.sparkS3AccessToOzone == False:
                    # 					conf.set('spark.sql.extensions', 'com.hortonworks.spark.sql.rule.Extensions,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
                    conf.set(
                        "spark.sql.extensions",
                        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
                    )
                    conf.set(
                        "spark.sql.catalog.hive",
                        "org.apache.iceberg.spark.SparkCatalog",
                    )
                    conf.set("spark.sql.catalog.hive.type", "hive")
                    conf.set("spark.sql.catalog.hive.cache-enabled", "false")
                    conf.set(
                        "spark.sql.catalog.local",
                        "org.apache.iceberg.spark.SparkCatalog",
                    )
                    conf.set("spark.sql.catalog.local.type", "hadoop")
                    conf.set(
                        "spark.sql.catalog.local.warehouse",
                        self.import_config.hdfsBaseDir,
                    )
                else:
                    conf.set(
                        "spark.sql.extensions",
                        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
                    )
                    conf.set(
                        "spark.sql.catalog.hive",
                        "org.apache.iceberg.spark.SparkCatalog",
                    )
                    conf.set("spark.sql.catalog.hive.type", "hive")
                    conf.set("spark.sql.catalog.hive.cache-enabled", "false")
                    conf.set(
                        "spark.sql.catalog.hive.io-impl",
                        "org.apache.iceberg.aws.s3.S3FileIO",
                    )
                    conf.set("spark.sql.catalog.hive.http-client.type", "urlconnection")
                    conf.set("spark.sql.catalog.hive.s3.path-style-access", "true")
                    conf.set(
                        "spark.sql.catalog.hive.client.assume-role.region", "us-east-1"
                    )
                    conf.set(
                        "spark.sql.catalog.hive.s3.access-key-id",
                        self.import_config.common_config.sparkS3AccessKeyID,
                    )
                    conf.set(
                        "spark.sql.catalog.hive.s3.secret-access-key",
                        self.import_config.common_config.sparkS3SecretAccessKey,
                    )
                    conf.set(
                        "spark.hadoop.fs.s3a.access.key",
                        self.import_config.common_config.sparkS3AccessKeyID,
                    )
                    conf.set(
                        "spark.hadoop.fs.s3a.secret.key",
                        self.import_config.common_config.sparkS3SecretAccessKey,
                    )

        if self.import_config.common_config.sparkDynamicAllocation == True:
            conf.set("spark.shuffle.service.enabled", "true")
            conf.set("spark.dynamicAllocation.enabled", "true")
            conf.set("spark.dynamicAllocation.minExecutors", "0")
            conf.set(
                "spark.dynamicAllocation.maxExecutors",
                str(self.import_config.sparkMaxExecutors),
            )
            logging.info(
                "Number of executors is dynamic with a max value of %s executors"
                % (self.import_config.sparkMaxExecutors)
            )
        else:
            conf.set("spark.dynamicAllocation.enabled", "false")
            conf.set("spark.shuffle.service.enabled", "false")
            if self.import_config.sqlSessions < self.import_config.sparkMaxExecutors:
                conf.set(
                    "spark.executor.instances", str(self.import_config.sqlSessions)
                )
                logging.info(
                    "Number of executors is fixed at %s"
                    % (self.import_config.sqlSessions)
                )
            else:
                conf.set(
                    "spark.executor.instances",
                    str(self.import_config.sparkMaxExecutors),
                )
                logging.info(
                    "Number of executors is fixed at %s"
                    % (self.import_config.sparkMaxExecutors)
                )

        if (
            self.import_config.common_config.get_config_value(
                key="timestamp_with_timezone"
            )
            == False
        ):
            # conf.set('spark.sql.iceberg.handle-timestamp-without-timezone', 'true')
            # conf.set('spark.sql.iceberg.use-timestamp-without-timezone-in-new-tables', 'true')
            conf.set("spark.sql.timestampType", "TIMESTAMP_NTZ")

        sys.stdout.flush()
        self.spark_context = pyspark.context.SparkContext(conf=conf)
        sys.stdout.flush()
        self.spark = pyspark.sql.SparkSession(self.spark_context)
        sys.stdout.flush()

        spark_version_split = self.spark.version.split(".")
        spark_major_version = int(spark_version_split[0])
        spark_minor_version = int(spark_version_split[1])
        spark_version = "%s.%s.%s" % (
            spark_version_split[0],
            spark_version_split[1],
            spark_version_split[2],
        )
        logging.info("Running with Spark Version %s" % (spark_version))

        if (
            spark_major_version == 2
            and self.import_config.etlEngine == constant.ETL_ENGINE_SPARK
        ):
            self.stop_spark()
            raise invalidConfiguration(
                "Using Spark as the ETL engine is only supported in Spark3."
            )
        elif spark_major_version == 3 and spark_minor_version != 4:
            if (
                self.import_config.common_config.get_config_value(
                    key="timestamp_with_timezone"
                )
                == False
            ):
                self.spark.conf.set(
                    "spark.sql.iceberg.handle-timestamp-without-timezone", "true"
                )
                self.spark.conf.set(
                    "spark.sql.iceberg.use-timestamp-without-timezone-in-new-tables",
                    "true",
                )

        self.spark_context.addPyFile(
            "%s/bin/common/sparkUDF2.py" % (os.environ["DBIMPORT_HOME"])
        )
        import sparkUDF2

        spark_udf = sparkUDF2.sparkUDFClass()

        # Anonymization of data in columns
        spark_udf.set_seed_string(
            self.import_config.common_config.get_anonymization_seed()
        )

        if spark_major_version == 2:
            from pyspark.sql.functions import udf

            self.udf_hash_column = udf(
                spark_udf.hash_column, pyspark.sql.types.StringType()
            )
            self.udf_replace_char_with_star = udf(
                spark_udf.replace_char_with_star, pyspark.sql.types.StringType()
            )
            self.udf_show_first_four_characters = udf(
                spark_udf.show_first_four_characters, pyspark.sql.types.StringType()
            )
        else:
            self.udf_hash_column = pyspark.sql.functions.udf(
                spark_udf.hash_column, pyspark.sql.types.StringType()
            )
            self.udf_replace_char_with_star = pyspark.sql.functions.udf(
                spark_udf.replace_char_with_star, pyspark.sql.types.StringType()
            )
            self.udf_show_first_four_characters = pyspark.sql.functions.udf(
                spark_udf.show_first_four_characters, pyspark.sql.types.StringType()
            )

        # get Yarn info
        self.yarnApplicationID = self.spark_context.applicationId
        logging.info("Yarn application started with id %s" % (self.yarnApplicationID))
        self.import_config.common_config.update_yarn_statistics(
            self.yarnApplicationID, "spark"
        )

        sys.stdout.flush()

        self.spark.sql("set spark.sql.orc.impl=native")

        logging.debug("Executing import_operations.start_spark()")

    def stop_spark(self):

        # This might be called multiple times. Just return in case it's already closed
        if self.spark_context == None:
            return

        # Connect to the Spark UI and request the API to get the total number of Executors used. This will be saved in the 'yarn_statistics' table
        spark_url = "%s/api/v1/applications/%s/allexecutors" % (
            self.spark_context.uiWebUrl,
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

        self.import_config.common_config.update_yarn_statistics(
            self.yarnApplicationID, "spark", yarn_containers_total=spark_executor_count
        )

        logging.info("Stopping Spark context")
        self.spark_context.stop()
        self.spark_context = None
        self.spark = None

    def run_spark_import(self, pk_only_import, incremental_max=None):
        logging.debug("Executing import_operations.run_spark_import()")

        self.spark_start_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        self.spark_start_uts = int(time.time())
        spark_query = ""

        # Fetch the number of executors and sql splits that should be used
        self.import_config.calculate_job_mappers()

        row_count = 0
        incr_where_statement = ""
        if (
            self.import_config.import_is_incremental == True
            and pk_only_import == False
            and self.import_config.importPhase
            != constant.IMPORT_PHASE_MSSQL_CHANGE_TRACKING
        ):
            if self.import_config.common_config.jdbc_servertype == constant.SNOWFLAKE:
                incr_where_statement = self.import_config.get_incr_where_statement(
                    whereForSqoop=True, incrementalMax=incremental_max
                )
            else:
                incr_where_statement = self.import_config.get_incr_where_statement(
                    whereForSqoop=True, incrementalMax=incremental_max
                ).replace('"', "'")

            if incr_where_statement == "":
                # DBImport is unable to find the max value for the configured incremental column. Is the table empty?
                row_count = self.import_config.common_config.get_jdbc_table_row_count(
                    self.import_config.source_schema, self.import_config.source_table
                )
                if row_count == 0:
                    self.sqoopIncrNoNewRows = True
                    try:
                        logging.warning(
                            "There are no rows in the source table. As this is an incremental import, spark will not run"
                        )
                        self.import_config.save_import_statistics(
                            self.sqoopStartUTS,
                            sqoop_size=0,
                            sqoop_rows=0,
                            sqoop_mappers=0,
                        )
                        self.import_config.save_source_table_row_count(
                            row_count=0, incr=False
                        )
                        self.import_config.save_source_table_row_count(
                            row_count=0, incr=True, print_info=False
                        )
                        self.delete_sqoop_hdfs_location()
                    except:
                        logging.exception("Fatal error when saving sqoop statistics")
                        self.import_config.remove_temporary_files()
                        sys.exit(1)
                    return
                else:
                    logging.error(
                        "DBImport is unable to find the max value for the configured incremental column."
                    )
                    self.import_config.remove_temporary_files()
                    sys.exit(1)

        # Fetch Min and Max values for Boundary query
        if self.import_config.sqlSessions > 1:
            self.import_config.generate_sqoop_split_by()
            if (
                self.import_config.import_is_incremental == True
                and self.import_config.splitByColumn
                == self.import_config.sqoop_incr_column
            ):
                # If it is an incremental import and both splitBy and incr_columns are the same, we can just use the min/max values for the BoundaryValues
                if (
                    self.import_config.sqoopIncrMinvaluePending != None
                    and self.import_config.sqoopIncrMaxvaluePending != None
                ):
                    min_max_dict = {}
                    min_max_dict["min"] = self.import_config.sqoopIncrMinvaluePending
                    min_max_dict["max"] = self.import_config.sqoopIncrMaxvaluePending
                elif incremental_max != None:
                    min_max_dict = {}
                    min_max_dict["min"] = self.import_config.sqoopIncrMinvaluePending
                    min_max_dict["max"] = incremental_max
                else:
                    # Generate the SQL that fetch the min and max values from the column that is defined in self.import_config.splitByColumn
                    min_max_dict = self.import_config.get_min_max_boundary_values()
            else:
                # Generate the SQL that fetch the min and max values from the column that is defined in self.import_config.splitByColumn
                min_max_dict = self.import_config.get_min_max_boundary_values()
        else:
            min_max_dict = {}
            min_max_dict["min"] = "Unknown"
            min_max_dict["max"] = "Unknown"

        # Create the SQL that will be used to fetch the data. This will be used on a "from" statement
        if (
            self.import_config.importPhase
            == constant.IMPORT_PHASE_MSSQL_CHANGE_TRACKING
        ):
            spark_query = "(%s) %s" % (
                self.import_config.get_sql_to_read_from_source_with_mssql_change_tracking(),
                self.Hive_Table,
            )
        elif incr_where_statement != "":
            spark_query = "(%s where %s) %s" % (
                self.import_config.get_sql_to_read_from_source(),
                incr_where_statement,
                self.Hive_Table,
            )
        elif self.import_config.get_sql_where_addition() != None:
            spark_query = "(%s where %s) %s" % (
                self.import_config.get_sql_to_read_from_source(),
                self.import_config.get_sql_where_addition(),
                self.Hive_Table,
            )
        else:
            spark_query = "(%s) %s" % (
                self.import_config.get_sql_to_read_from_source(),
                self.Hive_Table,
            )

        # 		print("import_is_incremental: %s"%self.import_config.import_is_incremental)
        # 		print("incrWhereStatement: %s"%incrWhereStatement)
        # 		print("Rowcount: %s"%rowCount)
        # 		print("sparkQuery: %s"%sparkQuery)
        # 		self.import_config.remove_temporary_files()
        # 		sys.exit(1)

        # Start the spark session
        self.start_spark()
        import pyspark

        quote_around_column = self.import_config.common_config.get_quote_around_column()
        partition_column = "%s%s%s" % (
            quote_around_column,
            str(self.import_config.splitByColumn).lower(),
            quote_around_column,
        )

        if self.import_config.etlEngine == constant.ETL_ENGINE_SPARK:
            iceberg_staging_table = "%s.%s.%s" % (
                self.common_operations.sparkCatalogName,
                self.import_config.Hive_Import_DB,
                self.import_config.Hive_Import_Table,
            )
            iceberg_pk_only_deleted_table = "%s.%s.%s__pkonly__deleted" % (
                self.common_operations.sparkCatalogName,
                self.import_config.Hive_Import_DB,
                self.import_config.Hive_Import_Table,
            )
            iceberg_target_table = "%s.%s.%s" % (
                self.common_operations.sparkCatalogName,
                self.import_config.Hive_DB,
                self.import_config.Hive_Table,
            )
            iceberg_history_table = "%s.%s.%s_history" % (
                self.common_operations.sparkCatalogName,
                self.import_config.Hive_DB,
                self.import_config.Hive_Table,
            )

        # 			print("----------------------------------------------------------------------------")
        # 			print("icebergStagingTable       = %s"%(icebergStagingTable))
        # 			print("icebergPKOnlyDeletedTable = %s"%(icebergPKOnlyDeletedTable))
        # 			print("icebergTargetTable        = %s"%(icebergTargetTable))
        # 			print("icebergHistoryTable       = %s"%(icebergHistoryTable))
        # 			print("----------------------------------------------------------------------------")

        # Only run the import if the stage have a shortname of "spark". This way, we dont have to bother about individual stage numbers
        if (
            self.import_config.get_stage_short_name(self.import_config.get_stage())
            == "spark"
        ):
            print("")
            logging.info("Ingesting data from Source table to Import table")
            sys.stdout.flush()

            if self.import_config.sqlSessions < 2:
                df = (
                    self.spark.read.format("jdbc")
                    .option("driver", self.import_config.common_config.jdbc_driver)
                    .option("url", self.import_config.common_config.jdbc_url)
                    .option("dbtable", spark_query)
                    .option("user", self.import_config.common_config.jdbc_username)
                    .option("password", self.import_config.common_config.jdbc_password)
                    .option("fetchsize", "10000")
                    .load()
                )
            else:
                df = (
                    self.spark.read.format("jdbc")
                    .option("driver", self.import_config.common_config.jdbc_driver)
                    .option("url", self.import_config.common_config.jdbc_url)
                    .option("dbtable", spark_query)
                    .option("user", self.import_config.common_config.jdbc_username)
                    .option("password", self.import_config.common_config.jdbc_password)
                    .option("fetchsize", "10000")
                    .option("partitionColumn", partition_column)
                    .option("lowerBound", min_max_dict["min"])
                    .option("upperBound", min_max_dict["max"])
                    .option("numPartitions", self.import_config.sqlSessions)
                    .load()
                )

            # Get a Pandas DF with columnames and anonymization function
            anonymization_function = self.import_config.get_anonymization_function()

            for index, row in anonymization_function.iterrows():
                c_name = row["source_column_name"]
                c_function = row["anonymization_function"]

                logging.info(
                    "Anonymizing data in column '%s' with function '%s'"
                    % (c_name, c_function)
                )
                sys.stdout.flush()

                if c_function == "Hash":
                    df = df.withColumn(c_name, self.udf_hash_column(c_name))
                elif c_function == "Replace with star":
                    df = df.withColumn(c_name, self.udf_replace_char_with_star(c_name))
                elif c_function == "Show first 4 chars":
                    df = df.withColumn(
                        c_name, self.udf_show_first_four_characters(c_name)
                    )

            # Write ORC files
            sys.stdout.flush()
            if self.import_config.etlEngine == constant.ETL_ENGINE_HIVE:
                df.write.mode("overwrite").format("orc").save(
                    self.import_config.sqoop_hdfs_location
                )
                sys.stdout.flush()

                logging.info("Import data saved to Import tables")
                sys.stdout.flush()

                hdfs_data_df = self.spark.read.format("orc").load(
                    self.import_config.sqoop_hdfs_location
                )
                rows_written_by_spark = hdfs_data_df.count()

            if self.import_config.etlEngine == constant.ETL_ENGINE_SPARK:
                # ETL Engine is Spark, that means IceBerg tables

                if (
                    self.common_operations.check_table(
                        self.import_config.Hive_Import_DB,
                        self.import_config.Hive_Import_Table,
                    )
                    == True
                ):
                    if (
                        self.common_operations.is_table_external_iceberg_format(
                            self.import_config.Hive_Import_DB,
                            self.import_config.Hive_Import_Table,
                        )
                        == False
                    ):
                        # Staging table exists and is not an Iceberg table. We need to drop it in Hive
                        logging.warning(
                            "Staging table exists and is not an Iceberg table. Dropping the staging table"
                        )
                        self.common_operations.connect_to_hive(forceSkipTest=True)
                        self.common_operations.drop_hive_table(
                            self.import_config.Hive_Import_DB,
                            self.import_config.Hive_Import_Table,
                        )

                # Drop the current Staging table
                logging.info("Dropping old Iceberg Import table")
                sys.stdout.flush()
                try:
                    query = "drop table if exists %s purge" % (iceberg_staging_table)
                    logging.info(query)
                    self.spark.sql(query)
                except pyspark.sql.utils.AnalysisException as e:
                    if "Table or view not found" in str(e):
                        pass
                    else:
                        raise

                # Create the table
                logging.info(
                    "Creating Iceberg Import table and writing data from source database into it. This is the main loading step of data from source database"
                )
                sys.stdout.flush()
                df.writeTo(iceberg_staging_table).create()

                logging.info("Data saved to Import table")
                logging.info("Fetching number of rows written from Import table")
                sys.stdout.flush()

                hdfs_data_df = self.spark.table(iceberg_staging_table)
                rows_written_by_spark = hdfs_data_df.count()

            sys.stdout.flush()
            if self.import_config.etlEngine == constant.ETL_ENGINE_SPARK:
                logging.info(
                    "Number of rows written by spark to import location = %s"
                    % (rows_written_by_spark)
                )
            else:
                logging.info(
                    "Number of rows written by spark = %s" % (rows_written_by_spark)
                )
            sys.stdout.flush()

            if (
                self.import_config.importPhase
                == constant.IMPORT_PHASE_MSSQL_CHANGE_TRACKING
            ):
                if rows_written_by_spark == 0:
                    # If there is no new rows, we cant get the MaxValue from the output.
                    # So we need to set it to the previous value
                    incr_maxvalue_pending = self.import_config.incr_maxvalue
                    logging.warning("There are no new rows in the source table.")
                else:
                    # We need to read the max version number and use that as the min value on next import
                    incr_maxvalue_pending = (
                        hdfs_data_df.groupBy()
                        .max("datalake_mssql_changetrack_version")
                        .first()[0]
                    )
                    logging.info(
                        "Max Version number for MSSQL Change Tracking that was fetched = %s"
                        % (incr_maxvalue_pending)
                    )
            else:
                incr_maxvalue_pending = None

            # Get the size of the table on disk
            if self.import_config.etlEngine == constant.ETL_ENGINE_SPARK:
                staging_table_size = self.spark.sql(
                    "select sum(file_size_in_bytes) as size from %s.all_data_files"
                    % (iceberg_staging_table)
                )
                size_written_by_spark = staging_table_size.first()["size"]
            else:
                # Get size of all files on HDFS that spark wrote
                hdfs_command_list = [
                    "hdfs",
                    "dfs",
                    "-du",
                    "-s",
                    self.import_config.sqoop_hdfs_location,
                ]
                hdfs_proc = subprocess.Popen(
                    hdfs_command_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE
                )
                std_out, std_err = hdfs_proc.communicate()
                std_out = std_out.decode("utf-8").rstrip()
                std_err = std_err.decode("utf-8").rstrip()

                try:
                    size_written_by_spark = std_out.split()[0]
                except IndexError:
                    size_written_by_spark = -1

            logging.info(
                "Size of data written by spark = %s bytes" % (size_written_by_spark)
            )
            sys.stdout.flush()

            # As the import can take a very long time on a large table, we need to verify that the MySQL connection against the configuration database is still valid
            self.import_config.reconnect_config_database()

            if pk_only_import == False:
                try:
                    self.import_config.save_import_statistics(
                        self.spark_start_uts,
                        sqoop_size=size_written_by_spark,
                        sqoop_rows=rows_written_by_spark,
                        sqoop_mappers=self.import_config.sqlSessions,
                        sqoop_incr_maxvalue_pending=incr_maxvalue_pending,
                    )
                except:
                    logging.exception("Fatal error when saving spark statistics")
                    self.import_config.remove_temporary_files()
                    sys.exit(1)

                if (
                    self.import_config.importPhase
                    == constant.IMPORT_PHASE_MSSQL_CHANGE_TRACKING
                ):
                    self.import_config.save_source_table_row_count(
                        row_count=rows_written_by_spark, incr=True, print_info=True
                    )

        if self.import_config.etlEngine == constant.ETL_ENGINE_HIVE:
            # If it's the Spark ETL engine, we need the spark context later in the import
            self.stop_spark()

            print(" ________________________ ")
            print("|                        |")
            print("| Spark Import completed |")
            print("|________________________|")
            print("")
            sys.stdout.flush()

        logging.debug("Executing import_operations.run_spark_import() - Finished")

    def purge_iceberg_versions_on_target_table(self):
        logging.debug(
            "Executing import_operations.purge_iceberg_versions_on_target_table()"
        )
        # Removes old Iceberg snapshot version on the target table

        if self.import_config.etlEngine == constant.ETL_ENGINE_HIVE:
            return

        iceberg_target_table = "%s.%s" % (
            self.import_config.Hive_DB,
            self.import_config.Hive_Table,
        )

        delete_older_than_ref = datetime.utcnow() - timedelta(hours=24)
        delete_older_than = delete_older_than_ref.strftime("%Y-%m-%d %H:%M:%S.000")

        self.start_spark()
        logging.info("Expire older Iceberg snapshots on Target table")
        # logging.warning("LINE REMARKED - MUST BE FIXED")
        self.spark.sql(
            "CALL %s.system.expire_snapshots(table => '%s', older_than => TIMESTAMP '%s', retain_last => 1, stream_results => true)"
            % (
                self.common_operations.sparkCatalogName,
                iceberg_target_table,
                delete_older_than,
            )
        )

        logging.info("Removing orphan Iceberg files on Target table")
        # logging.warning("LINE REMARKED - MUST BE FIXED")
        self.spark.sql(
            "CALL %s.system.remove_orphan_files(table => '%s', older_than => TIMESTAMP '%s')"
            % (
                self.common_operations.sparkCatalogName,
                iceberg_target_table,
                delete_older_than,
            )
        )

        logging.debug(
            "Executing import_operations.purge_iceberg_versions_on_target_table() - Finsihed"
        )

    def delete_sqoop_hdfs_location(self):
        logging.debug("Executing import_operations.delete_sqoop_hdfs_location()")
        logging.info(
            "Deleting old imported data from previous DBImport executions on HDFS"
        )

        hdfs_command_list = [
            "hdfs",
            "dfs",
            "-rm",
            "-r",
            "-skipTrash",
            self.import_config.sqoop_hdfs_location,
        ]
        hdfs_proc = subprocess.Popen(
            hdfs_command_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        std_out, std_err = hdfs_proc.communicate()

        hdfs_command_list = [
            "hdfs",
            "dfs",
            "-mkdir",
            self.import_config.sqoop_hdfs_location,
        ]
        hdfs_proc = subprocess.Popen(
            hdfs_command_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        std_out, std_err = hdfs_proc.communicate()

        logging.debug(
            "Executing import_operations.delete_sqoop_hdfs_location() - Finished"
        )

    def run_sqoop_import(self, pk_only_import, incremental_max=None):
        logging.debug("Executing import_operations.run_sqoop_import()")

        self.sqoop_start_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        self.sqoopStartUTS = int(time.time())

        # Check if any column is anonymized. If it is, we need to exit as that is only supported with Spark
        if self.import_config.is_any_column_anonymized() == True:
            logging.error(
                "There are column in this table that is configured for anonymization. Those functions are only supported by spark and not by sqoop."
            )
            self.import_config.remove_temporary_files()
            sys.exit(1)

        # Fetch the number of mappers that should be used
        self.import_config.calculate_job_mappers()

        self.sqoop_launched_map_tasks = None
        self.sqoop_failed_map_tasks = None
        self.sqoop_killed_map_tasks = None
        self.sqoop_launched_reduce_tasks = None
        self.sqoop_failed_reduce_tasks = None
        self.sqoop_killed_reduce_tasks = None

        # Sets the correct sqoop table and schema that will be used if a custom SQL is not used
        sqoop_query = ""
        sqoop_source_schema = []
        sqoop_source_table = ""
        sqoop_direct_option = ""
        if self.import_config.common_config.db_mssql == True:
            sqoop_source_schema = ["--", "--schema", self.import_config.source_schema]
            sqoop_source_table = self.import_config.source_table
        if self.import_config.common_config.db_oracle == True:
            sqoop_source_table = "%s.%s" % (
                self.import_config.source_schema.upper(),
                self.import_config.source_table.upper(),
            )
            # sqoopDirectOption = "--direct"
        if self.import_config.common_config.db_postgresql == True:
            sqoop_source_schema = ["--", "--schema", self.import_config.source_schema]
            sqoop_source_table = self.import_config.source_table
        if self.import_config.common_config.db_progress == True:
            sqoop_source_table = "%s.%s" % (
                self.import_config.source_schema,
                self.import_config.source_table,
            )
            # sqoopDirectOption = "--direct"
        if self.import_config.common_config.db_mysql == True:
            sqoop_source_table = self.import_config.source_table
        if self.import_config.common_config.db_db2udb == True:
            sqoop_source_table = "%s.%s" % (
                self.import_config.source_schema,
                self.import_config.source_table.upper(),
            )
        if self.import_config.common_config.db_db2as400 == True:
            sqoop_source_table = "%s.%s" % (
                self.import_config.source_schema,
                self.import_config.source_table.upper(),
            )
            # sqoopDirectOption = "--direct"
        if self.import_config.common_config.db_informix == True:
            sqoop_source_table = self.import_config.source_table
            # sqoopSourceTable = "%s.%s"%(self.import_config.source_schema, self.import_config.source_table)

        if (
            self.import_config.sqoop_use_generated_sql == True
            and self.import_config.sqoop_query == None
        ):
            sqoop_query = self.import_config.sqlGeneratedSqoopQuery

        if self.import_config.importPhase == constant.IMPORT_PHASE_ORACLE_FLASHBACK:
            # Add the specific Oracle FlashBack columns that we need to import
            sqoop_query = re.sub(
                "^select",
                'select "VERSIONS_OPERATION" as "datalake_flashback_operation", "VERSIONS_STARTSCN" as "datalake_flashback_startscn",',
                self.import_config.sqlGeneratedSqoopQuery,
            )

        if self.import_config.sqoop_query != None:
            sqoop_query = self.import_config.sqoop_query

        # Handle mappers, split-by with custom query
        if sqoop_query != "":
            if "split-by" not in self.import_config.sqoop_options.lower():
                self.import_config.generate_sqoop_split_by()

        self.import_config.generate_sqoop_boundary_query()

        # Handle the situation where the source dont have a PK and there is no split-by (force mappers to 1)
        if (
            self.import_config.generatedPKcolumns == None
            and "split-by" not in self.import_config.sqoop_options.lower()
        ):
            logging.warning(
                "There is no PrimaryKey in source system and no --split-by in the sqoop_options columns. This will force the import to use only 1 mapper"
            )
            self.import_config.sqlSessions = 1

        # From here and forward we are building the sqoop command with all options
        sqoop_command = []
        sqoop_command.extend(
            ["sqoop", "import", "-D", "mapreduce.job.user.classpath.first=true"]
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
        sqoop_command.extend(
            [
                "-D",
                "org.apache.sqoop.splitter.allow_text_splitter=%s"
                % (self.import_config.sqoop_allow_text_splitter),
            ]
        )
        sqoop_command.extend(["-D", "sqoop.parquet.logical_types.decimal.enable=false"])
        sqoop_command.extend(["-D", "sqoop.avro.logical_types.decimal.enable=false"])

        if "split-by" not in self.import_config.sqoop_options.lower():
            sqoop_command.append("--autoreset-to-one-mapper")

        # Progress and DB2 AS400 imports needs to know the class name for the JDBC driver.
        if (
            self.import_config.common_config.db_progress == True
            or self.import_config.common_config.db_db2as400 == True
            or self.import_config.common_config.db_informix == True
            or self.import_config.common_config.db_sqlanywhere == True
        ):
            sqoop_command.extend(
                ["--driver", self.import_config.common_config.jdbc_driver]
            )

        sqoop_command.extend(["--class-name", "dbimport"])
        sqoop_command.append("--fetch-size=10000")

        sqoop_command.append("--as-parquetfile")
        sqoop_command.append("--compress")
        sqoop_command.extend(["--compression-codec", "snappy"])

        if pk_only_import == True:
            sqoop_command.extend(
                ["--target-dir", self.import_config.sqoop_hdfs_location_pkonly]
            )
        else:
            sqoop_command.extend(
                ["--target-dir", self.import_config.sqoop_hdfs_location]
            )

        sqoop_command.extend(["--outdir", self.import_config.common_config.tempdir])
        sqoop_command.extend(
            ["--connect", '"%s"' % (self.import_config.common_config.jdbc_url)]
        )
        sqoop_command.extend(
            ["--username", self.import_config.common_config.jdbc_username]
        )
        sqoop_command.extend(
            [
                "--password-file",
                "file://%s" % (self.import_config.common_config.jdbc_password_file),
            ]
        )
        if sqoop_direct_option != "":
            sqoop_command.append(sqoop_direct_option)

        sqoop_command.extend(["--num-mappers", str(self.import_config.sqlSessions)])

        # If we dont have a SQL query to use for sqoop, then we need to specify the table instead
        if (
            sqoop_query == ""
            and self.import_config.importPhase != constant.IMPORT_PHASE_ORACLE_FLASHBACK
        ):
            sqoop_command.extend(["--table", sqoop_source_table])

        sqoop_command.append("--delete-target-dir")

        if (
            self.import_config.generatedSqoopOptions != ""
            and self.import_config.generatedSqoopOptions != None
        ):
            sqoop_command.extend(shlex.split(self.import_config.generatedSqoopOptions))

        if (
            self.import_config.sqoop_options.strip() != ""
            and self.import_config.sqoop_options != None
        ):
            if self.import_config.common_config.jdbc_force_column_lowercase == True:
                sqoop_command.extend(
                    shlex.split(self.import_config.sqoop_options.lower())
                )
            else:
                sqoop_command.extend(shlex.split(self.import_config.sqoop_options))

        if (
            self.import_config.sqoopBoundaryQuery.strip() != ""
            and self.import_config.sqlSessions > 1
        ):
            sqoop_command.extend(
                ["--boundary-query", self.import_config.sqoopBoundaryQuery]
            )

        if self.import_config.import_is_incremental == True and pk_only_import == False:
            incr_where_statement = self.import_config.get_incr_where_statement(
                whereForSqoop=True, incrementalMax=incremental_max
            ).replace('"', "'")
            if incr_where_statement == "":
                # DBImport is unable to find the max value for the configured incremental column. Is the table empty?
                row_count = self.import_config.common_config.get_jdbc_table_row_count(
                    self.import_config.source_schema, self.import_config.source_table
                )
                if row_count == 0:
                    self.sqoopIncrNoNewRows = True
                    try:
                        logging.warning(
                            "There are no rows in the source table. As this is an incremental import, sqoop will not run"
                        )
                        self.import_config.save_import_statistics(
                            self.sqoopStartUTS,
                            sqoop_size=0,
                            sqoop_rows=0,
                            sqoop_mappers=0,
                        )
                        self.import_config.save_source_table_row_count(
                            row_count=0, incr=False
                        )
                        self.import_config.save_source_table_row_count(
                            row_count=0, incr=True, print_info=False
                        )
                        self.delete_sqoop_hdfs_location()
                    except:
                        logging.exception("Fatal error when saving sqoop statistics")
                        self.import_config.remove_temporary_files()
                        sys.exit(1)
                    return
                else:
                    logging.error(
                        "DBImport is unable to find the max value for the configured incremental column."
                    )
                    self.import_config.remove_temporary_files()
                    sys.exit(1)

        if self.import_config.importPhase == constant.IMPORT_PHASE_ORACLE_FLASHBACK:
            sqoop_command.extend(
                [
                    "--query",
                    "%s %s AND $CONDITIONS " % (sqoop_query, incr_where_statement),
                ]
            )
        elif sqoop_query == "":
            if (
                self.import_config.import_is_incremental == True
                and pk_only_import == False
            ):
                if self.import_config.get_sql_where_addition() != None:
                    sqoop_command.extend(
                        [
                            "--where",
                            "%s and %s"
                            % (
                                incr_where_statement,
                                self.import_config.get_sql_where_addition(),
                            ),
                        ]
                    )
                else:
                    sqoop_command.extend(["--where", incr_where_statement])
            else:
                if self.import_config.get_sql_where_addition() != None:
                    sqoop_command.extend(
                        ["--where", self.import_config.get_sql_where_addition()]
                    )
            sqoop_command.extend(sqoop_source_schema)
        else:
            if (
                self.import_config.import_is_incremental == True
                and pk_only_import == False
            ):
                sqoop_command.extend(
                    [
                        "--query",
                        "%s where $CONDITIONS and %s"
                        % (sqoop_query, incr_where_statement),
                    ]
                )
            else:
                if self.import_config.get_sql_where_addition() != None:
                    sqoop_command.extend(
                        [
                            "--query",
                            "%s where $CONDITIONS and %s"
                            % (
                                sqoop_query,
                                self.import_config.get_sql_where_addition(),
                            ),
                        ]
                    )
                else:
                    sqoop_command.extend(
                        ["--query", "%s where $CONDITIONS" % (sqoop_query)]
                    )

        logging.info("Starting sqoop with the following command: %s" % (sqoop_command))
        sqoop_output = ""

        print(" _____________________ ")
        print("|                     |")
        print("| Sqoop Import starts |")
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
                self.parse_sqoop_output(row)

        # Print what is left in output after sqoop finished
        for row in sh_session.stdout.readlines():
            row = row.decode("utf-8").rstrip()
            if row != "":
                print(row)
                sys.stdout.flush()
                sqoop_output += row + "\n"
                self.parse_sqoop_output(row)

        print(" ________________________ ")
        print("|                        |")
        print("| Sqoop Import completed |")
        print("|________________________|")
        print("")

        # As the import can take a very long time on a large table, we need to verify that the MySQL connection against the configuration database is still valid
        self.import_config.reconnect_config_database()

        # At this stage, the entire sqoop job is run and we fetched all data from the output. Lets parse and store it
        if self.sqoopSize == None:
            self.sqoopSize = 0
        if self.sqoopRows == None:
            self.sqoopRows = 0

        # Update table 'yarn_statistics'
        yarn_containers_total = 0
        yarn_containers_failed = 0
        yarn_containers_killed = 0
        if self.sqoop_launched_map_tasks != None:
            yarn_containers_total += self.sqoop_launched_map_tasks
        if self.sqoop_launched_reduce_tasks != None:
            yarn_containers_total += self.sqoop_launched_reduce_tasks
        if self.sqoop_failed_map_tasks != None:
            yarn_containers_failed += self.sqoop_failed_map_tasks
        if self.sqoop_failed_reduce_tasks != None:
            yarn_containers_failed += self.sqoop_failed_reduce_tasks
        if self.sqoop_killed_map_tasks != None:
            yarn_containers_killed += self.sqoop_killed_map_tasks
        if self.sqoop_killed_reduce_tasks != None:
            yarn_containers_killed += self.sqoop_killed_reduce_tasks

        self.import_config.common_config.update_yarn_statistics(
            self.yarnApplicationID,
            "sqoop",
            yarn_containers_total=yarn_containers_total,
            yarn_containers_failed=yarn_containers_failed,
            yarn_containers_killed=yarn_containers_killed,
        )

        # Check for errors in output
        sqoop_warning = False

        if " ERROR " in sqoop_output:
            for row in sqoop_output.split("\n"):
                if (
                    "Arithmetic overflow error converting expression to data type int"
                    in row
                ):
                    # This is not really an error, but we cant verify the table.
                    logging.warning(
                        "Unable to verify the import due to errors from MsSQL server. Will continue regardless of this"
                    )
                    sqoop_warning = True

                if "Not authorized to access topics: [" in row:
                    # This is not really an error, but we cant verify the table.
                    logging.warning("Problem sending data to Atlas")
                    sqoop_warning = True

                if "ERROR hook.AtlasHook" in row:
                    # This is not really an error, but we cant send to Atlas
                    logging.warning("Problem sending data to Atlas")
                    sqoop_warning = True

                if "clients.NetworkClient: [Producer clientId" in row:
                    # Unknown Kafka error
                    sqoop_warning = True

                if "java.sql.SQLException: ORA-30052:" in row:
                    # java.sql.SQLException: ORA-30052: invalid lower limit snapshot expression
                    logging.error(
                        "The Oracle Flashback import has passed the valid timeframe for undo logging in Oracle. To recover from this failure, you need to do an initial load."
                    )
                    logging.error(
                        "This is best done with './manage --resetCDCImport -h <Hive_DB> -t <Hive_Table>'"
                    )
                    self.remove_temporary_files()
                    sys.exit(1)

                # The following two tests is just to ensure that we dont mark the import as failed just because there is a column called error
                if " ERROR " in row and "Precision" in row and "Scale" in row:
                    sqoop_warning = True

                if " ERROR " in row and "The HCatalog field" in row:
                    sqoop_warning = True

            if sqoop_warning == False:
                # We detected the string ERROR in the output but we havent found a vaild reason for it in the IF statements above.
                # We need to mark the sqoop command as error and exit the program
                logging.error(
                    "Unknown error in sqoop import. Please check the output for errors and try again"
                )
                self.remove_temporary_files()
                sys.exit(1)

        # No errors detected in the output and we are ready to store the result
        logging.info("Sqoop executed successfully")

        if self.sqoopIncrNoNewRows == True:
            # If there is no new rows, we cant get the MaxValue from the output.
            # So we need to set it to what we used to launch sqoop
            self.sqoop_incr_max_value_pending = self.import_config.sqoop_incr_lastvalue
            self.sqoopSize = 0
            self.sqoopRows = 0
            logging.warning("There are no new rows in the source table.")

        if pk_only_import == False:
            try:
                self.import_config.save_import_statistics(
                    self.sqoopStartUTS,
                    sqoop_size=self.sqoopSize,
                    sqoop_rows=self.sqoopRows,
                    sqoop_mappers=self.import_config.sqlSessions,
                )
            except:
                logging.exception("Fatal error when saving sqoop statistics")
                self.import_config.remove_temporary_files()
                sys.exit(1)

        logging.debug("Executing import_operations.run_sqoop_import() - Finished")

    def parse_sqoop_output(self, row):
        # 19/03/28 05:15:44 HDFS: Number of bytes written=17
        if "HDFS: Number of bytes written" in row:
            self.sqoopSize = int(row.split("=")[1])

        # 19/03/28 05:15:44 INFO mapreduce.ImportJobBase: Retrieved 2 records.
        if "mapreduce.ImportJobBase: Retrieved" in row:
            self.sqoopRows = int(row.split(" ")[5])

        # 19/04/23 17:19:58 INFO tool.ImportTool:   --last-value 34
        if "tool.ImportTool:   --last-value" in row:
            self.sqoop_incr_max_value_pending = row.split("last-value ")[1].strip()

        # 19/04/24 07:13:00 INFO tool.ImportTool: No new rows detected since last import.
        if "No new rows detected since last import" in row:
            self.sqoopIncrNoNewRows = True

        # 23/10/04 04:52:07 INFO impl.YarnClientImpl: Submitted application application_1695989285495_0371
        if "Submitted application application_" in row:
            self.yarnApplicationID = row.split("Submitted application ")[1].strip()
            self.import_config.common_config.update_yarn_statistics(
                self.yarnApplicationID, "sqoop"
            )

        # Used for updating table 'yarn_statistics' after Sqoop is finished
        if "Launched map tasks=" in row:
            self.sqoop_launched_map_tasks = int(row.split("=")[1].strip())
        if "Failed map tasks=" in row:
            self.sqoop_failed_map_tasks = int(row.split("=")[1].strip())
        if "Killed map tasks=" in row:
            self.sqoop_killed_map_tasks = int(row.split("=")[1].strip())
        if "Launched reduce tasks=" in row:
            self.sqoop_launched_reduce_tasks = int(row.split("=")[1].strip())
        if "Failed reduce tasks=" in row:
            self.sqoop_failed_reduce_tasks = int(row.split("=")[1].strip())
        if "Killed reduce tasks=" in row:
            self.sqoop_killed_reduce_tasks = int(row.split("=")[1].strip())

    def connect_to_hive(self, forceSkipTest=False):
        logging.debug("Executing import_operations.connect_to_hive()")

        if self.import_config.etlEngine == constant.ETL_ENGINE_SPARK:
            # No need to connect to Hive when ETL engine is Spark
            return

        try:
            self.common_operations.connect_to_hive(forceSkipTest=forceSkipTest)
        except Exception as ex:
            logging.error(ex)
            self.import_config.remove_temporary_files()
            sys.exit(1)

        if self.global_hive_configuration_set == False:
            self.global_hive_configuration_set = True
            if self.import_config.hiveJavaHeap != None:
                query = "set hive.tez.container.size=%s" % (
                    self.import_config.hiveJavaHeap
                )
                self.common_operations.execute_hive_query(query)
            else:
                query = "set hive.tez.container.size=3072"
                self.common_operations.execute_hive_query(query)

            if (
                self.import_config.hiveSplitCount != None
                and self.import_config.hiveSplitCount > 0
            ):
                query = "set tez.grouping.split-count=%s" % (
                    self.import_config.hiveSplitCount
                )
                self.common_operations.execute_hive_query(query)

        logging.debug("Executing import_operations.connect_to_hive() - Finished")

    def create_external_import_view(self):
        logging.debug("Executing import_operations.create_external_import_view()")

        if self.import_config.is_external_view_required() == True:
            if (
                self.common_operations.check_table(
                    self.import_config.Hive_Import_DB,
                    self.import_config.Hive_Import_View,
                )
                == False
            ):
                logging.info("Creating External Import View")

                query = "create view `%s`.`%s` as %s " % (
                    self.import_config.Hive_Import_DB,
                    self.import_config.Hive_Import_View,
                    self.import_config.get_select_for_import_view(),
                )

                self.common_operations.execute_hive_query(query)
                self.common_operations.reconnect_hive_meta_store()

        logging.debug(
            "Executing import_operations.create_external_import_view() - Finished"
        )

    def update_external_import_view(self):
        logging.debug("Executing import_operations.update_external_import_view()")

        if self.import_config.is_external_view_required() == True:
            columns_config = self.import_config.get_columns_from_config_database(
                sourceIsParquetFile=True, includeAllColumns=False
            )

            hive_db = self.import_config.Hive_Import_DB
            hive_view = self.import_config.Hive_Import_View
            columns_hive = self.common_operations.get_columns(
                hive_db, hive_view, includeType=True, excludeDataLakeColumns=True
            )
            columns_merge = pd.merge(
                columns_config, columns_hive, on=None, how="outer", indicator="Exist"
            )
            columns_diff_count = len(
                columns_merge.loc[columns_merge["Exist"] != "both"]
            )

            if columns_diff_count > 0:
                # There is a diff between the configuration and the view. Lets update it
                logging.info(
                    "Updating Import View as columns in Hive is not the same as in the configuration"
                )
                query = "alter view `%s`.`%s` as %s " % (
                    self.import_config.Hive_Import_DB,
                    self.import_config.Hive_Import_View,
                    self.import_config.get_select_for_import_view(),
                )

                self.common_operations.execute_hive_query(query)
                self.common_operations.reconnect_hive_meta_store()

        logging.debug(
            "Executing import_operations.update_external_import_view() - Finished"
        )

    def create_external_import_table(self):
        if self.import_config.etlEngine == constant.ETL_ENGINE_HIVE:
            self.create_external_table(table_type="import")

    def create_external_target_table(self):
        if self.import_config.etlEngine == constant.ETL_ENGINE_HIVE:
            self.create_external_table(table_type="target")

    def create_external_table(self, table_type=""):
        logging.debug("Executing import_operations.create_external_import_table()")

        if table_type == "import":
            hive_db = self.import_config.Hive_Import_DB
            hive_table = self.import_config.Hive_Import_Table
            hdfs_location = self.import_config.sqoop_hdfs_location

        if table_type == "target":
            hive_db = self.Hive_DB
            hive_table = self.Hive_Table
            hdfs_location = self.import_config.sqoop_hdfs_location_sparkETL

        if self.common_operations.check_table(hive_db, hive_table) == True:
            # Table exist, so we need to make sure that it's a managed table and not an external table
            if (
                self.common_operations.is_table_external_parquet_format(
                    hive_db, hive_table
                )
                == False
                and self.import_config.importTool == "sqoop"
            ):
                logging.info(
                    "Dropping %s table as it's not an external table based on parquet"
                    % (table_type)
                )
                self.common_operations.drop_hive_table(hive_db, hive_table)
                self.common_operations.reconnect_hive_meta_store()

            if self.common_operations.is_table_external_orc_format(
                hive_db, hive_table
            ) == False and self.import_config.importTool in ("spark", "local"):
                logging.info(
                    "Dropping %s table as it's not an external table based on ORC"
                    % (table_type)
                )
                self.common_operations.drop_hive_table(hive_db, hive_table)
                self.common_operations.reconnect_hive_meta_store()

        if self.common_operations.check_table(hive_db, hive_table) == False:
            # Import table does not exist. We just create it in that case
            logging.info("Creating External %s Table" % (table_type))

            query = "create external table `%s`.`%s` (" % (hive_db, hive_table)
            columns_df = self.import_config.get_columns_from_config_database(
                includeAllColumns=False
            )

            if self.import_config.importTool == "sqoop":
                columns_df = self.update_columns_for_import_table(columns_df)
            # 			elif self.import_config.importTool == "spark":
            # 				columnsDF = self.update_columns_for_import_table(columnsDF, replaceNameOnly=True)

            first_loop = True

            if (
                self.import_config.importPhase
                == constant.IMPORT_PHASE_MSSQL_CHANGE_TRACKING
            ):
                # Add the specific MSSQL Change Tracking columns that we need to import
                first_loop = False
                query += "`datalake_mssql_changetrack_version` bigint"
                query += ", `datalake_mssql_changetrack_operation` varchar(2)"

            for index, row in columns_df.iterrows():
                if first_loop == False:
                    query += ", "

                query += "`%s` %s" % (row["name"], row["type"])

                if row["comment"] != None:
                    query += ' COMMENT "%s"' % (row["comment"])
                first_loop = False

            if self.import_config.importPhase == constant.IMPORT_PHASE_ORACLE_FLASHBACK:
                # Add the specific Oracle FlashBack columns that we need to import
                query += ", `datalake_flashback_operation` varchar(2)"
                query += ", `datalake_flashback_startscn` string"

            query += ") "

            table_comment = self.import_config.get_hive_table_comment()
            if table_comment != None:
                query += 'COMMENT "%s" ' % (table_comment)

            if self.import_config.importTool == "sqoop":
                query += "stored as parquet "
                query += "LOCATION '%s%s/' " % (
                    self.common_operations.hdfs_address,
                    hdfs_location,
                )
                query += "TBLPROPERTIES ('parquet.compress' = 'SNAPPY') "
            elif self.import_config.importTool in ("spark", "local"):
                query += "stored as ORC "
                query += "LOCATION '%s%s/' " % (
                    self.common_operations.hdfs_address,
                    hdfs_location,
                )

            self.common_operations.execute_hive_query(query)
            self.common_operations.reconnect_hive_meta_store()

        logging.debug(
            "Executing import_operations.create_external_import_table() - Finished"
        )

    def convert_hive_table_to_acid(self):
        if self.import_config.etlEngine == constant.ETL_ENGINE_HIVE:
            if (
                self.common_operations.is_table_transactional(
                    self.Hive_DB, self.Hive_Table
                )
                == False
            ):
                self.common_operations.convert_hive_table_to_acid(
                    self.Hive_DB,
                    self.Hive_Table,
                    createDeleteColumn=self.import_config.soft_delete_during_merge,
                    createMergeColumns=True,
                )

    def add_hive_dbimport_columns_to_history_table(self):
        """Will add the required DBImport columns in the Hive table"""
        logging.debug("Executing import_operations.createHiveMergeColumns()")

        if self.import_config.etlEngine == constant.ETL_ENGINE_SPARK:
            self.start_spark()

        # self.Hive_ColumnName_IUD = self.import_config.Hive_ColumnName_IUD
        # self.Hive_ColumnName_HistoryTimestamp = self.import_config.Hive_ColumnName_HistoryTimestamp

        hive_history_db = self.import_config.Hive_History_DB
        hive_history_table = self.import_config.Hive_History_Table

        columns = self.common_operations.get_columns(
            hiveDB=hive_history_db,
            hiveTable=hive_history_table,
            includeType=False,
            includeComment=False,
        )

        if (
            columns[columns["name"] == self.Hive_ColumnName_Source].empty == True
            and self.import_config.datalake_source != None
        ):
            if self.import_config.etlEngine == constant.ETL_ENGINE_HIVE:
                query = "alter table `%s`.`%s` add columns ( %s varchar(256) )" % (
                    hive_history_db,
                    hive_history_table,
                    self.Hive_ColumnName_Source,
                )
                self.common_operations.execute_hive_query(query)

            elif self.import_config.etlEngine == constant.ETL_ENGINE_SPARK:
                query = "alter table `%s`.`%s`.`%s` add columns ( %s string )" % (
                    self.common_operations.sparkCatalogName,
                    hive_history_db,
                    hive_history_table,
                    self.Hive_ColumnName_Source,
                )
                logging.info(query)
                self.spark.sql(query)

            if self.common_operations.metastore_type == constant.CATALOG_HIVE_DIRECT:
                self.common_operations.reconnect_hive_meta_store()

        if columns[columns["name"] == self.Hive_ColumnName_IUD].empty == True:
            if self.import_config.etlEngine == constant.ETL_ENGINE_HIVE:
                query = (
                    'alter table `%s`.`%s` add columns ( %s char(1) COMMENT "Last operation of this record was I=Insert, U=Update or D=Delete")'
                    % (hive_history_db, hive_history_table, self.Hive_ColumnName_IUD)
                )
                self.common_operations.execute_hive_query(query)

            elif self.import_config.etlEngine == constant.ETL_ENGINE_SPARK:
                query = (
                    'alter table `%s`.`%s`.`%s` add columns ( %s string COMMENT "Last operation of this record was I=Insert, U=Update or D=Delete")'
                    % (
                        self.common_operations.sparkCatalogName,
                        hive_history_db,
                        hive_history_table,
                        self.Hive_ColumnName_IUD,
                    )
                )
                logging.info(query)
                self.spark.sql(query)

            if self.common_operations.metastore_type == constant.CATALOG_HIVE_DIRECT:
                self.common_operations.reconnect_hive_meta_store()

        if (
            columns[columns["name"] == self.Hive_ColumnName_HistoryTimestamp].empty
            == True
        ):
            if self.import_config.etlEngine == constant.ETL_ENGINE_HIVE:
                query = (
                    'alter table `%s`.`%s` add columns ( %s timestamp COMMENT "Timestamp for insert in Datalake")'
                    % (
                        hive_history_db,
                        hive_history_table,
                        self.Hive_ColumnName_HistoryTimestamp,
                    )
                )
                self.common_operations.execute_hive_query(query)

            elif self.import_config.etlEngine == constant.ETL_ENGINE_SPARK:
                query = (
                    'alter table `%s`.`%s`.`%s` add columns ( %s timestamp COMMENT "Timestamp for insert in Datalake" )'
                    % (
                        self.common_operations.sparkCatalogName,
                        hive_history_db,
                        hive_history_table,
                        self.Hive_ColumnName_HistoryTimestamp,
                    )
                )
                logging.info(query)
                self.spark.sql(query)

            if self.common_operations.metastore_type == constant.CATALOG_HIVE_DIRECT:
                self.common_operations.reconnect_hive_meta_store()

    def add_hive_dbimport_columns(self, merge_operation):
        """Will add the required DBImport columns in the Hive table"""
        logging.debug("Executing import_operations.createHiveMergeColumns()")

        if self.import_config.etlEngine == constant.ETL_ENGINE_SPARK:
            self.start_spark()

        columns = self.common_operations.get_columns(
            hiveDB=self.Hive_DB,
            hiveTable=self.Hive_Table,
            includeType=False,
            includeComment=False,
        )

        if (
            columns[columns["name"] == self.Hive_ColumnName_Source].empty == True
            and self.import_config.datalake_source != None
        ):
            if self.import_config.etlEngine == constant.ETL_ENGINE_HIVE:
                query = "alter table `%s`.`%s` add columns ( %s varchar(256) )" % (
                    self.Hive_DB,
                    self.Hive_Table,
                    self.Hive_ColumnName_Source,
                )
                self.common_operations.execute_hive_query(query)

            elif self.import_config.etlEngine == constant.ETL_ENGINE_SPARK:
                query = "alter table `%s`.`%s`.`%s` add columns ( %s string )" % (
                    self.common_operations.sparkCatalogName,
                    self.Hive_DB,
                    self.Hive_Table,
                    self.Hive_ColumnName_Source,
                )
                logging.info(query)
                self.spark.sql(query)

            if self.common_operations.metastore_type == constant.CATALOG_HIVE_DIRECT:
                self.common_operations.reconnect_hive_meta_store()

        if merge_operation == False:
            if self.import_config.create_datalake_import_column == True:
                # if columns[columns['name'] == 'datalake_insert'].empty == False and columns[columns['name'] == 'datalake_import'].empty == True:
                if (
                    columns[columns["name"] == self.Hive_ColumnName_Insert].empty
                    == False
                    and columns[columns["name"] == self.Hive_ColumnName_Import].empty
                    == True
                ):
                    if self.import_config.etlEngine == constant.ETL_ENGINE_HIVE:
                        # query = "alter table `%s`.`%s` change column datalake_insert datalake_import timestamp"%(self.Hive_DB, self.Hive_Table)
                        query = (
                            "alter table `%s`.`%s` change column %s %s timestamp"
                            % (
                                self.Hive_DB,
                                self.Hive_Table,
                                self.Hive_ColumnName_Insert,
                                self.Hive_ColumnName_Import,
                            )
                        )
                        self.common_operations.execute_hive_query(query)

                    elif self.import_config.etlEngine == constant.ETL_ENGINE_SPARK:
                        query = "alter table `%s`.`%s`.`%s` rename column %s to %s" % (
                            self.common_operations.sparkCatalogName,
                            self.Hive_DB,
                            self.Hive_Table,
                            self.Hive_ColumnName_Insert,
                            self.Hive_ColumnName_Import,
                        )
                        # 						print(query)
                        logging.info(query)
                        self.spark.sql(query)

                    if (
                        self.common_operations.metastore_type
                        == constant.CATALOG_HIVE_DIRECT
                    ):
                        self.common_operations.reconnect_hive_meta_store()

                # elif columns[columns['name'] == 'datalake_import'].empty == True:
                elif (
                    columns[columns["name"] == self.Hive_ColumnName_Import].empty
                    == True
                ):
                    if self.import_config.etlEngine == constant.ETL_ENGINE_HIVE:
                        # query = "alter table `%s`.`%s` add columns ( datalake_import timestamp COMMENT \"Import time from source database\")"%(self.Hive_DB, self.Hive_Table)
                        query = (
                            'alter table `%s`.`%s` add columns ( %s timestamp COMMENT "Import time from source database")'
                            % (
                                self.Hive_DB,
                                self.Hive_Table,
                                self.Hive_ColumnName_Import,
                            )
                        )
                        self.common_operations.execute_hive_query(query)

                    elif self.import_config.etlEngine == constant.ETL_ENGINE_SPARK:
                        # query = "alter table `%s`.`%s`.`%s` add columns ( datalake_import timestamp COMMENT \"Import time from source database\" )" \
                        # 		%(self.common_operations.sparkCatalogName, self.Hive_DB, self.Hive_Table)
                        query = (
                            'alter table `%s`.`%s`.`%s` add columns ( %s timestamp COMMENT "Import time from source database" )'
                            % (
                                self.common_operations.sparkCatalogName,
                                self.Hive_DB,
                                self.Hive_Table,
                                self.Hive_ColumnName_Import,
                            )
                        )
                        # 						print(query)
                        logging.info(query)
                        self.spark.sql(query)

                    if (
                        self.common_operations.metastore_type
                        == constant.CATALOG_HIVE_DIRECT
                    ):
                        self.common_operations.reconnect_hive_meta_store()

        else:
            # if columns[columns['name'] == 'datalake_import'].empty == False:
            # if columns[columns['name'] == self.Hive_ColumnName_Import].empty == False:
            if (
                columns[columns["name"] == self.Hive_ColumnName_Import].empty == False
                and columns[columns["name"] == self.Hive_ColumnName_Insert].empty
                == True
            ):
                if self.import_config.etlEngine == constant.ETL_ENGINE_HIVE:
                    # query = "alter table `%s`.`%s` change column datalake_import datalake_insert timestamp"%(self.Hive_DB, self.Hive_Table)
                    query = "alter table `%s`.`%s` change column %s %s timestamp" % (
                        self.Hive_DB,
                        self.Hive_Table,
                        self.Hive_ColumnName_Import,
                        self.Hive_ColumnName_Insert,
                    )
                    self.common_operations.execute_hive_query(query)

                elif self.import_config.etlEngine == constant.ETL_ENGINE_SPARK:
                    # query = "alter table `%s`.`%s`.`%s` rename column datalake_import to datalake_insert" \
                    # 		%(self.common_operations.sparkCatalogName, self.Hive_DB, self.Hive_Table)
                    query = "alter table `%s`.`%s`.`%s` rename column %s to %s" % (
                        self.common_operations.sparkCatalogName,
                        self.Hive_DB,
                        self.Hive_Table,
                        self.Hive_ColumnName_Import,
                        self.Hive_ColumnName_Insert,
                    )
                    # 					print(query)
                    logging.info(query)
                    self.spark.sql(query)

                if (
                    self.common_operations.metastore_type
                    == constant.CATALOG_HIVE_DIRECT
                ):
                    self.common_operations.reconnect_hive_meta_store()

            # elif columns[columns['name'] == 'datalake_insert'].empty == True:
            elif columns[columns["name"] == self.Hive_ColumnName_Insert].empty == True:
                if self.import_config.etlEngine == constant.ETL_ENGINE_HIVE:
                    # query = "alter table `%s`.`%s` add columns ( datalake_insert timestamp COMMENT \"Timestamp for insert in Datalake\")"%(self.Hive_DB, self.Hive_Table)
                    query = (
                        'alter table `%s`.`%s` add columns ( %s timestamp COMMENT "Timestamp for insert in Datalake")'
                        % (self.Hive_DB, self.Hive_Table, self.Hive_ColumnName_Insert)
                    )
                    self.common_operations.execute_hive_query(query)

                elif self.import_config.etlEngine == constant.ETL_ENGINE_SPARK:
                    query = (
                        'alter table `%s`.`%s`.`%s` add columns ( %s timestamp COMMENT "Timestamp for insert in Datalake" )'
                        % (
                            self.common_operations.sparkCatalogName,
                            self.Hive_DB,
                            self.Hive_Table,
                            self.Hive_ColumnName_Insert,
                        )
                    )
                    logging.info(query)
                    self.spark.sql(query)

                if (
                    self.common_operations.metastore_type
                    == constant.CATALOG_HIVE_DIRECT
                ):
                    self.common_operations.reconnect_hive_meta_store()

            # 			if columns[columns['name'] == 'datalake_iud'].empty == True:
            if columns[columns["name"] == self.Hive_ColumnName_IUD].empty == True:
                if self.import_config.etlEngine == constant.ETL_ENGINE_HIVE:
                    query = (
                        'alter table `%s`.`%s` add columns ( %s char(1) COMMENT "Last operation of this record was I=Insert, U=Update or D=Delete")'
                        % (self.Hive_DB, self.Hive_Table, self.Hive_ColumnName_IUD)
                    )
                    self.common_operations.execute_hive_query(query)

                elif self.import_config.etlEngine == constant.ETL_ENGINE_SPARK:
                    query = (
                        'alter table `%s`.`%s`.`%s` add columns ( %s string COMMENT "Last operation of this record was I=Insert, U=Update or D=Delete")'
                        % (
                            self.common_operations.sparkCatalogName,
                            self.Hive_DB,
                            self.Hive_Table,
                            self.Hive_ColumnName_IUD,
                        )
                    )
                    logging.info(query)
                    self.spark.sql(query)

                if (
                    self.common_operations.metastore_type
                    == constant.CATALOG_HIVE_DIRECT
                ):
                    self.common_operations.reconnect_hive_meta_store()

            # if columns[columns['name'] == 'datalake_update'].empty == True:
            if columns[columns["name"] == self.Hive_ColumnName_Update].empty == True:
                if self.import_config.etlEngine == constant.ETL_ENGINE_HIVE:
                    # query = "alter table `%s`.`%s` add columns ( datalake_update timestamp COMMENT \"Timestamp for last update in Datalake\")"%(self.Hive_DB, self.Hive_Table)
                    query = (
                        'alter table `%s`.`%s` add columns ( %s timestamp COMMENT "Timestamp for last update in Datalake")'
                        % (self.Hive_DB, self.Hive_Table, self.Hive_ColumnName_Update)
                    )
                    self.common_operations.execute_hive_query(query)

                elif self.import_config.etlEngine == constant.ETL_ENGINE_SPARK:
                    # query = "alter table `%s`.`%s`.`%s` add columns ( datalake_update timestamp COMMENT \"Timestamp for last update in Datalake\")" \
                    query = (
                        'alter table `%s`.`%s`.`%s` add columns ( %s timestamp COMMENT "Timestamp for last update in Datalake")'
                        % (
                            self.common_operations.sparkCatalogName,
                            self.Hive_DB,
                            self.Hive_Table,
                            self.Hive_ColumnName_Update,
                        )
                    )
                    # print(query)
                    logging.info(query)
                    self.spark.sql(query)

                if (
                    self.common_operations.metastore_type
                    == constant.CATALOG_HIVE_DIRECT
                ):
                    self.common_operations.reconnect_hive_meta_store()

            # if columns[columns['name'] == 'datalake_delete'].empty == True and self.import_config.soft_delete_during_merge == True:
            if (
                columns[columns["name"] == self.Hive_ColumnName_Delete].empty == True
                and self.import_config.soft_delete_during_merge == True
            ):
                if self.import_config.etlEngine == constant.ETL_ENGINE_HIVE:
                    # query = "alter table `%s`.`%s` add columns ( datalake_delete timestamp COMMENT \"Timestamp for soft delete in Datalake\")" \
                    query = (
                        'alter table `%s`.`%s` add columns ( %s timestamp COMMENT "Timestamp for soft delete in Datalake")'
                        % (self.Hive_DB, self.Hive_Table, self.Hive_ColumnName_Delete)
                    )
                    self.common_operations.execute_hive_query(query)

                elif self.import_config.etlEngine == constant.ETL_ENGINE_SPARK:
                    # query = "alter table `%s`.`%s`.`%s` add columns ( datalake_delete timestamp COMMENT \"Timestamp for soft delete in Datalake\")" \
                    query = (
                        'alter table `%s`.`%s`.`%s` add columns ( %s timestamp COMMENT "Timestamp for soft delete in Datalake")'
                        % (
                            self.common_operations.sparkCatalogName,
                            self.Hive_DB,
                            self.Hive_Table,
                            self.Hive_ColumnName_Delete,
                        )
                    )
                    logging.info(query)
                    self.spark.sql(query)

                if (
                    self.common_operations.metastore_type
                    == constant.CATALOG_HIVE_DIRECT
                ):
                    self.common_operations.reconnect_hive_meta_store()

        logging.debug("Executing import_operations.createHiveMergeColumns() - Finished")

    def generate_create_target_table_sql(
        self,
        hive_db,
        hive_table,
        acid_table=False,
        buckets=4,
        restrict_columns=None,
        acid_insert_only=False,
        iceberg_table=False,
    ):
        """Will generate the common create table for the target table as a list."""
        logging.debug("Executing import_operations.generate_create_target_table_sql()")

        query_list = []
        if iceberg_table == True:
            query = "create table %s.`%s`.`%s` (" % (
                self.common_operations.sparkCatalogName,
                hive_db,
                hive_table,
            )
        else:
            query = "create table `%s`.`%s` (" % (hive_db, hive_table)
        columns_df = self.import_config.get_columns_from_config_database(
            includeAllColumns=False
        )

        restrict_columns_list = []
        if restrict_columns != None:
            restrict_columns_list = restrict_columns.split(",")

        first_loop = True
        for index, row in columns_df.iterrows():
            if restrict_columns_list != []:
                if row["name"] not in restrict_columns_list:
                    continue
            if first_loop == False:
                query += ", "
            query += "`%s` %s" % (row["name"], row["type"])
            if row["comment"] != None:
                query += ' COMMENT "%s"' % (row["comment"])
            first_loop = False
        query_list.append(query)

        query = ") "
        table_comment = self.import_config.get_hive_table_comment()

        if iceberg_table == True:
            # Generate a create table statment that will be executed through Spark
            query += "USING iceberg "

            if table_comment != None:
                query += 'COMMENT "%s" ' % (table_comment)
        else:
            if table_comment != None:
                query += 'COMMENT "%s" ' % (table_comment)

            if acid_table == False:
                query += "STORED AS ORC TBLPROPERTIES ('orc.compress'='ZLIB') "
            else:
                if (
                    self.import_config.common_config.get_config_value(
                        key="hive_acid_with_clusteredby"
                    )
                    == True
                ):
                    query += "CLUSTERED BY ("
                    first_column = True

                    if (
                        self.import_config.get_pk_columns(pk_for_merge=True) == None
                    ):  # To look at both pk_column_override and pk_override_merge_only
                        logging.error(
                            "There is no Primary Key for this table. Please add one in source system or in 'pk_column_override'"
                        )
                        self.import_config.remove_temporary_files()
                        sys.exit(1)

                    for column in self.import_config.get_pk_columns(
                        pk_for_merge=True
                    ).split(","):
                        if first_column == False:
                            query += ", "
                        query += "`" + column + "`"
                        first_column = False
                    # TODO: Smarter calculation of the number of buckets
                    query += ") into %s buckets " % (buckets)

                if acid_insert_only == False:
                    query += "STORED AS ORC TBLPROPERTIES ('orc.compress'='ZLIB', 'transactional'='true') "
                else:
                    query += "STORED AS ORC TBLPROPERTIES ('orc.compress'='ZLIB', 'transactional'='true', 'transactional_properties'='insert_only') "

        query_list.append(query)

        logging.debug(
            "Executing import_operations.generate_create_target_table_sql() - Finished"
        )
        return query_list

    def create_delete_table(self):
        logging.debug("Executing import_operations.create_delete_table()")

        hive_delete_db = self.import_config.Hive_Delete_DB
        hive_delete_table = self.import_config.Hive_Delete_Table

        if self.import_config.etlEngine == constant.ETL_ENGINE_HIVE:
            if (
                self.common_operations.check_table(hive_delete_db, hive_delete_table)
                == False
            ):
                # Target table does not exist. We just create it in that case
                logging.info(
                    "Creating Delete table %s.%s in Hive"
                    % (hive_delete_db, hive_delete_table)
                )
                if (
                    self.import_config.common_config.get_config_value(
                        key="hive_insert_only_tables"
                    )
                    == True
                ):
                    query_list = self.generate_create_target_table_sql(
                        hive_db=hive_delete_db,
                        hive_table=hive_delete_table,
                        acid_table=True,
                        acid_insert_only=True,
                        restrict_columns=self.import_config.get_pk_columns(
                            pk_for_merge=True
                        ),
                    )
                else:
                    query_list = self.generate_create_target_table_sql(
                        hive_db=hive_delete_db,
                        hive_table=hive_delete_table,
                        acid_table=False,
                        acid_insert_only=False,
                        restrict_columns=self.import_config.get_pk_columns(
                            pk_for_merge=True
                        ),
                    )

                query = "".join(query_list)

                self.common_operations.execute_hive_query(query)
                self.common_operations.reconnect_hive_meta_store()

        if self.import_config.etlEngine == constant.ETL_ENGINE_SPARK:
            if (
                self.common_operations.check_table(hive_delete_db, hive_delete_table)
                == True
            ):
                # Table exist, so we need to make sure that it's an Iceberg table and not something else

                if (
                    self.common_operations.is_table_external_iceberg_format(
                        hive_delete_db, hive_delete_table
                    )
                    == False
                ):
                    logging.info(
                        "Delete table is not a Iceberg table. Dropping and recreating the Delete table"
                    )
                    self.common_operations.drop_hive_table(
                        hive_delete_db, hive_delete_table
                    )

            # We need to check again as the table might just been droped because it was an external table to begin with
            if (
                self.common_operations.check_table(hive_delete_db, hive_delete_table)
                == False
            ):
                logging.info(
                    "Creating Delete table %s.%s in Spark"
                    % (hive_delete_db, hive_delete_table)
                )

                # Create the Iceberg table
                query_list = self.generate_create_target_table_sql(
                    hive_db=hive_delete_db,
                    hive_table=hive_delete_table,
                    restrict_columns=self.import_config.get_pk_columns(
                        pk_for_merge=True
                    ),
                    iceberg_table=True,
                )

                query = "".join(query_list)

                self.start_spark()
                logging.info(query)
                self.spark.sql(query)
                self.common_operations.reconnect_hive_meta_store()

        logging.debug("Executing import_operations.create_delete_table() - Finished")

    def create_history_table(self):
        logging.debug("Executing import_operations.create_history_table()")

        hive_history_db = self.import_config.Hive_History_DB
        hive_history_table = self.import_config.Hive_History_Table

        if self.import_config.etlEngine == constant.ETL_ENGINE_HIVE:
            if (
                self.common_operations.check_table(hive_history_db, hive_history_table)
                == False
            ):
                # Target table does not exist. We just create it in that case
                logging.info(
                    "Creating History table %s.%s in Hive"
                    % (hive_history_db, hive_history_table)
                )
                if (
                    self.import_config.common_config.get_config_value(
                        key="hive_insert_only_tables"
                    )
                    == True
                ):
                    query_list = self.generate_create_target_table_sql(
                        hive_db=hive_history_db,
                        hive_table=hive_history_table,
                        acid_table=True,
                        acid_insert_only=True,
                    )
                else:
                    query_list = self.generate_create_target_table_sql(
                        hive_db=hive_history_db,
                        hive_table=hive_history_table,
                        acid_table=False,
                        acid_insert_only=False,
                    )

                query = query_list[0]

                if self.import_config.datalake_source != None:
                    # query += ", datalake_source varchar(256)"
                    query += ", %s varchar(256)" % (self.Hive_ColumnName_Source)

                # query += ", datalake_iud char(1) COMMENT \"SQL operation of this record was I=Insert, U=Update or D=Delete\""
                # query += ", datalake_timestamp timestamp COMMENT \"Timestamp for SQL operation in Datalake\""
                query += (
                    ', %s char(1) COMMENT "SQL operation of this record was I=Insert, U=Update or D=Delete"'
                    % (self.Hive_ColumnName_IUD)
                )
                query += (
                    ', %s timestamp COMMENT "Timestamp for SQL operation in Datalake"'
                    % (self.Hive_ColumnName_HistoryTimestamp)
                )
                query += query_list[1]

                self.common_operations.execute_hive_query(query)
                self.common_operations.reconnect_hive_meta_store()

        if self.import_config.etlEngine == constant.ETL_ENGINE_SPARK:
            if (
                self.common_operations.check_table(hive_history_db, hive_history_table)
                == True
            ):
                # Table exist, so we need to make sure that it's an Iceberg table and not something else

                if (
                    self.common_operations.is_table_external_iceberg_format(
                        hive_history_db, hive_history_table
                    )
                    == False
                ):
                    # Table exists and is not an Iceberg table
                    raise undevelopedFeature(
                        "The history table already exists, but it's not an Iceberg table. Convertion between manage table and external Iceberg table is not handled automatically. Please save the data and drop the table manually to continue with the import"
                    )
                else:
                    return
            else:
                logging.info(
                    "Creating History table %s.%s in Spark"
                    % (hive_history_db, hive_history_table)
                )

                # Create the Iceberg table
                query_list = self.generate_create_target_table_sql(
                    hive_db=hive_history_db,
                    hive_table=hive_history_table,
                    iceberg_table=True,
                )

                query = query_list[0]

                if self.import_config.datalake_source != None:
                    # query += ", datalake_source varchar(256)"
                    query += ", %s varchar(256)" % (self.Hive_ColumnName_Source)

                # query += ", datalake_iud char(1) COMMENT \"Last operation of this record was I=Insert, U=Update or D=Delete\""
                # query += ", datalake_timestamp timestamp COMMENT \"Timestamp for SQL operation in Datalake\""
                query += (
                    ', %s char(1) COMMENT "Last operation of this record was I=Insert, U=Update or D=Delete"'
                    % (self.Hive_ColumnName_IUD)
                )
                query += (
                    ', %s timestamp COMMENT "Timestamp for SQL operation in Datalake"'
                    % (self.Hive_ColumnName_HistoryTimestamp)
                )
                query += query_list[1]

                self.start_spark()
                logging.info(query)
                self.spark.sql(query)
                self.common_operations.reconnect_hive_meta_store()

        logging.debug("Executing import_operations.create_history_table() - Finished")

    def create_target_table(self):
        logging.debug("Executing import_operations.create_target_table()")

        if self.import_config.etlEngine == constant.ETL_ENGINE_HIVE:
            if (
                self.common_operations.check_table(self.Hive_DB, self.Hive_Table)
                == True
            ):
                # Table exist, so we need to make sure that it's a managed table and not an external table
                if (
                    self.common_operations.is_table_external(
                        self.Hive_DB, self.Hive_Table
                    )
                    == True
                ):
                    if self.import_config.importPhase == constant.IMPORT_PHASE_FULL:
                        # If the import stage is full, it's safe to drop the table and recreate it as the import have all the data anyway
                        self.common_operations.drop_hive_table(
                            self.Hive_DB, self.Hive_Table
                        )

            # We need to check again as the table might just been droped because it was an external table to begin with
            if (
                self.common_operations.check_table(self.Hive_DB, self.Hive_Table)
                == False
            ):
                # Target table does not exist. We just create it in that case
                logging.info(
                    "Creating Target table %s.%s in Hive"
                    % (self.Hive_DB, self.Hive_Table)
                )

                query_list = self.generate_create_target_table_sql(
                    hive_db=self.Hive_DB,
                    hive_table=self.Hive_Table,
                    acid_table=self.import_config.create_table_with_acid,
                    acid_insert_only=self.import_config.create_table_with_acid_insert_only,
                )

                query = query_list[0]

                if self.import_config.datalake_source != None:
                    # query += ", datalake_source varchar(256)"
                    query += ", %s varchar(256)" % (self.Hive_ColumnName_Source)

                if self.import_config.import_with_merge == False:
                    if self.import_config.create_datalake_import_column == True:
                        # query += ", datalake_import timestamp COMMENT \"Import time from source database\""
                        query += (
                            ', %s timestamp COMMENT "Import time from source database"'
                            % (self.Hive_ColumnName_Import)
                        )
                else:
                    # query += ", datalake_iud char(1) COMMENT \"Last operation of this record was I=Insert, U=Update or D=Delete\""
                    # query += ", datalake_insert timestamp COMMENT \"Timestamp for insert in Datalake\""
                    # query += ", datalake_update timestamp COMMENT \"Timestamp for last update in Datalake\""
                    query += (
                        ', %s char(1) COMMENT "Last operation of this record was I=Insert, U=Update or D=Delete"'
                        % (self.Hive_ColumnName_IUD)
                    )
                    query += (
                        ', %s timestamp COMMENT "Timestamp for insert in Datalake"'
                        % (self.Hive_ColumnName_Insert)
                    )
                    query += (
                        ', %s timestamp COMMENT "Timestamp for last update in Datalake"'
                        % (self.Hive_ColumnName_Update)
                    )

                    if self.import_config.soft_delete_during_merge == True:
                        # query += ", datalake_delete timestamp COMMENT \"Timestamp for soft delete in Datalake\""
                        query += (
                            ', %s timestamp COMMENT "Timestamp for soft delete in Datalake"'
                            % (self.Hive_ColumnName_Delete)
                        )

                query += query_list[1]

                self.common_operations.execute_hive_query(query)
                self.common_operations.reconnect_hive_meta_store()

        if self.import_config.etlEngine == constant.ETL_ENGINE_SPARK:
            if (
                self.common_operations.check_table(self.Hive_DB, self.Hive_Table)
                == True
            ):
                # Table exist, so we need to make sure that it's an Iceberg table and not something else

                if (
                    self.common_operations.is_table_external_iceberg_format(
                        self.Hive_DB, self.Hive_Table
                    )
                    == False
                ):
                    # Table exists and is not an Iceberg table
                    logging.warning("Target table exists and is not an Iceberg table")
                    if self.import_config.importPhase == constant.IMPORT_PHASE_FULL:
                        # If the import stage is full, it's safe to drop the table and recreate it as the import have all the data anyway
                        self.common_operations.drop_hive_table(
                            self.Hive_DB, self.Hive_Table
                        )
                    else:
                        raise undevelopedFeature(
                            "This is an incremental import and the target table already exists, but it's not an Iceberg table. Convertion between manage table and external Iceberg table is not handled automatically. Please save the data and drop the table manually to continue with the import"
                        )

            # We need to check again as the table might just been droped because it was an external table to begin with
            if (
                self.common_operations.check_table(self.Hive_DB, self.Hive_Table)
                == False
            ):
                # Target table does not exist. We just create it in that case
                logging.info(
                    "Creating Target table %s.%s in Spark"
                    % (self.Hive_DB, self.Hive_Table)
                )

                # Create the Iceberg table
                query_list = self.generate_create_target_table_sql(
                    hive_db=self.Hive_DB, hive_table=self.Hive_Table, iceberg_table=True
                )

                query = query_list[0]

                if self.import_config.datalake_source != None:
                    # query += ", datalake_source varchar(256)"
                    query += ", %s varchar(256)" % (self.Hive_ColumnName_Source)

                if self.import_config.import_with_merge == False:
                    if self.import_config.create_datalake_import_column == True:
                        # query += ", datalake_import timestamp COMMENT \"Import time from source database\""
                        query += (
                            ', %s timestamp COMMENT "Import time from source database"'
                            % (self.Hive_ColumnName_Import)
                        )
                else:
                    # query += ", datalake_iud char(1) COMMENT \"Last operation of this record was I=Insert, U=Update or D=Delete\""
                    # query += ", datalake_insert timestamp COMMENT \"Timestamp for insert in Datalake\""
                    # query += ", datalake_update timestamp COMMENT \"Timestamp for last update in Datalake\""
                    query += (
                        ', %s char(1) COMMENT "Last operation of this record was I=Insert, U=Update or D=Delete"'
                        % (self.Hive_ColumnName_IUD)
                    )
                    query += (
                        ', %s timestamp COMMENT "Timestamp for insert in Datalake"'
                        % (self.Hive_ColumnName_Insert)
                    )
                    query += (
                        ', %s timestamp COMMENT "Timestamp for last update in Datalake"'
                        % (self.Hive_ColumnName_Update)
                    )

                    if self.import_config.soft_delete_during_merge == True:
                        # query += ", datalake_delete timestamp COMMENT \"Timestamp for soft delete in Datalake\""
                        query += (
                            ', %s timestamp COMMENT "Timestamp for soft delete in Datalake"'
                            % (self.Hive_ColumnName_Delete)
                        )

                query += query_list[1]

                self.start_spark()
                logging.info(query)
                self.spark.sql(query)
                self.common_operations.reconnect_hive_meta_store()

        logging.debug("Executing import_operations.create_target_table() - Finished")

    def dropTargetTable(self):
        logging.debug("Executing import_operations.dropTargetTable()")
        if self.common_operations.check_table(self.Hive_DB, self.Hive_Table) == True:
            logging.info(
                "Dropping Target table %s.%s" % (self.Hive_DB, self.Hive_Table)
            )

            if self.import_config.etlEngine == constant.ETL_ENGINE_HIVE:
                query = "drop table if exists `%s`.`%s`" % (
                    self.Hive_DB,
                    self.Hive_Table,
                )

                self.common_operations.execute_hive_query(query)
                self.common_operations.reconnect_hive_meta_store()

            if self.import_config.etlEngine == constant.ETL_ENGINE_SPARK:
                self.start_spark()

                query = "drop table if exists %s.`%s`.`%s`" % (
                    self.common_operations.sparkCatalogName,
                    self.Hive_DB,
                    self.Hive_Table,
                )
                logging.info(query)
                self.spark.sql(query)

        logging.debug("Executing import_operations.dropTargetTable() - Finished")

    def update_target_table(self):
        if self.import_config.etlEngine == constant.ETL_ENGINE_HIVE:
            logging.info("Updating Target table columns based on source system schema")
            self.update_hive_table(self.Hive_DB, self.Hive_Table)

        if self.import_config.etlEngine == constant.ETL_ENGINE_SPARK:
            logging.info("Updating Target table columns based on source system schema")
            self.update_hive_table(self.Hive_DB, self.Hive_Table)

    def update_history_table(self):
        if self.import_config.etlEngine == constant.ETL_ENGINE_HIVE:
            logging.info("Updating History table columns based on source system schema")
            self.update_hive_table(
                self.import_config.Hive_History_DB,
                self.import_config.Hive_History_Table,
            )

        if self.import_config.etlEngine == constant.ETL_ENGINE_SPARK:
            logging.info("Updating History table columns based on source system schema")
            self.update_hive_table(
                self.import_config.Hive_History_DB,
                self.import_config.Hive_History_Table,
            )
            # self.update_hive_table(self.Hive_DB, self.Hive_Table)

    def update_delete_table(self):
        if self.import_config.etlEngine == constant.ETL_ENGINE_HIVE:
            logging.info("Updating Delete table columns based on source system schema")
            self.update_hive_table(
                self.import_config.Hive_Delete_DB,
                self.import_config.Hive_Delete_Table,
                restrict_columns=self.import_config.get_pk_columns(pk_for_merge=True),
            )

        if self.import_config.etlEngine == constant.ETL_ENGINE_SPARK:
            logging.info("Updating Delete table columns based on source system schema")
            self.update_hive_table(
                self.import_config.Hive_Delete_DB,
                self.import_config.Hive_Delete_Table,
                restrict_columns=self.import_config.get_pk_columns(pk_for_merge=True),
            )

    def update_external_import_table(self):
        if self.import_config.etlEngine == constant.ETL_ENGINE_HIVE:
            logging.info("Updating Import table columns based on source system schema")
            if self.import_config.importTool == "sqoop":
                self.update_hive_table(
                    self.import_config.Hive_Import_DB,
                    self.import_config.Hive_Import_Table,
                    source_is_parquet_file=True,
                )
            elif self.import_config.importTool in ("spark", "local"):
                self.update_hive_table(
                    self.import_config.Hive_Import_DB,
                    self.import_config.Hive_Import_Table,
                    source_is_parquet_file=False,
                )

            table_location = self.common_operations.get_table_location(
                self.import_config.Hive_Import_DB, self.import_config.Hive_Import_Table
            )
            configured_location = "%s%s" % (
                self.common_operations.hdfs_address,
                self.import_config.sqoop_hdfs_location,
            )

            if table_location != configured_location:
                logging.info(
                    "The configured location for the external table have changed. Updating the external table"
                )
                logging.debug("tableLocation:      %s" % (table_location))
                logging.debug("configuredLocation: %s" % (configured_location))

                query = 'alter table `%s`.`%s` set location "%s"' % (
                    self.import_config.Hive_Import_DB,
                    self.import_config.Hive_Import_Table,
                    self.import_config.sqoop_hdfs_location,
                )
                self.common_operations.execute_hive_query(query)

    def update_columns_for_import_table(self, columns_df, replace_name_only=False):
        """Parquet import format from sqoop cant handle all datatypes correctly. So for the column definition, we need to change some. We also replace <SPACE> with underscore in the column name"""
        if replace_name_only == False:
            columns_df["type"].replace(["date"], "string", inplace=True)
            columns_df["type"].replace(["timestamp"], "string", inplace=True)
            columns_df["type"].replace(
                ["decimal(.*)"], "string", regex=True, inplace=True
            )
            columns_df["type"].replace(["bigint"], "string", regex=True, inplace=True)

        # If you change any of the name replace rows, you also need to change the same data in function self.copy_hive_table() and import_definitions.save_column_data()
        columns_df["name"].replace([" "], "_", regex=True, inplace=True)
        columns_df["name"].replace(["%"], "pct", regex=True, inplace=True)
        columns_df["name"].replace([r"\("], "_", regex=True, inplace=True)
        columns_df["name"].replace([r"\)"], "_", regex=True, inplace=True)
        columns_df["name"].replace(["ü"], "u", regex=True, inplace=True)
        columns_df["name"].replace(["å"], "a", regex=True, inplace=True)
        columns_df["name"].replace(["ä"], "a", regex=True, inplace=True)
        columns_df["name"].replace(["ö"], "o", regex=True, inplace=True)

        return columns_df

    def update_columns_for_mongo_tables(self, columns_df):
        """Mongo Imports have both structs and arrays as columntypes. For this, the columnames needs a ` around them inside
        the struct or array. If this is included in the columnType when doing a compare with Hive, there will be an alter table
        at every import as the columntype is never the same"""

        columns_df["type"].replace(["`"], "", regex=True, inplace=True)
        return columns_df

    def update_hive_table(
        self, hive_db, hive_table, restrict_columns=None, source_is_parquet_file=False
    ):
        """Update the target table based on the column information in the configuration database"""
        logging.debug("Executing import_operations.update_hive_table()")
        columns_config = self.import_config.get_columns_from_config_database(
            restrictColumns=restrict_columns,
            sourceIsParquetFile=source_is_parquet_file,
            includeAllColumns=False,
        )
        columns_hive = self.common_operations.get_columns(
            hive_db, hive_table, includeType=True, excludeDataLakeColumns=True
        )

        if self.import_config.etlEngine == constant.ETL_ENGINE_SPARK:
            self.start_spark()
            import pyspark

        # 		pd.set_option('display.max_rows', None)
        # 		print(columnsConfig)
        # 		print("==============================================")
        # 		print(columnsHive)

        # 		raise undevelopedFeature("Stopping here to debug")

        # If we are working on the import table, we need to change some column types to handle Parquet files
        if self.import_config.importTool == "sqoop":
            if (
                hive_db == self.import_config.Hive_Import_DB
                and hive_table == self.import_config.Hive_Import_Table
            ):
                columns_config = self.update_columns_for_import_table(columns_config)

        # There is a difference between comparing column types and then updating Hive with that columntype. For example. Mongo import
        # requires that ` is around the columns inside the structs and arrays. But Hive doesnt store that. So doing a compare will
        # always fail due to that. So we need to compare without the ` around the column names. But  if there is a difference, we need to
        # run the alter command WITH the `, otherwise the Hive query will fail. columnTypesChanged is used to identify that
        column_types_changed = False
        columns_config_saved = columns_config.copy()

        # Update the column types if the import is a Mongo import
        if self.import_config.mongoImport == True:
            columns_config = self.update_columns_for_mongo_tables(columns_config)
            column_types_changed = True

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

        if self.import_config.importTool in ("spark", "local"):
            # Orc files needs the column to be in the right order. This can only be fixed with a drop/create
            # And it's only needed for the external tables
            if (
                hive_db == self.import_config.Hive_Import_DB
                and hive_table == self.import_config.Hive_Import_Table
            ):
                found_column_error = False
                for hive_index, hive_row in columns_hive_only_name.iterrows():
                    hive_name = hive_row["name"]
                    index_in_config = -1
                    if (
                        len(
                            columns_config_only_name.loc[
                                columns_config_only_name["name"] == hive_name
                            ]
                        )
                        > 0
                    ):
                        # Row in Hive exists in Config aswell. Need to check position
                        index_in_config = columns_config_only_name.loc[
                            columns_config_only_name["name"] == hive_name
                        ].index.tolist()[0]
                        if hive_index != index_in_config:
                            found_column_error = True

                if found_column_error == True:
                    # There was an error with the column order. We need to drop and recreate
                    logging.warning(
                        "The column order was not correct in the External Import Table. As this is a ORC table, we will have to drop and recreate it. This is needed because Hive tables based on ORC files does not support reordering of columns"
                    )
                    self.common_operations.drop_hive_table(hive_db, hive_table)
                    self.create_external_import_table()
                    self.common_operations.reconnect_hive_meta_store()

        # pd.set_option('display.max_rows', None)
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

                    if self.import_config.etlEngine == constant.ETL_ENGINE_HIVE:
                        query = "alter table `%s`.`%s` change column `%s` `%s` %s" % (
                            hive_db,
                            hive_table,
                            row_in_hive["name"],
                            row_in_config["name"],
                            row_in_config["type"],
                        )
                        self.common_operations.execute_hive_query(query)

                    elif self.import_config.etlEngine == constant.ETL_ENGINE_SPARK:
                        query = (
                            "alter table `%s`.`%s`.`%s` rename column `%s` to %s"
                            % (
                                self.common_operations.sparkCatalogName,
                                hive_db,
                                hive_table,
                                row_in_hive["name"],
                                row_in_config["name"],
                            )
                        )
                        logging.info(query)
                        self.spark.sql(query)

                    self.import_config.log_hive_column_rename(
                        row_in_config["name"],
                        row_in_hive["name"],
                        hiveDB=hive_db,
                        hiveTable=hive_table,
                    )
                    if row_in_config["type"] != row_in_hive["type"]:
                        self.import_config.log_hive_column_type_change(
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

                        if self.import_config.etlEngine == constant.ETL_ENGINE_HIVE:
                            query = (
                                "alter table `%s`.`%s` change column `%s` `%s` %s"
                                % (
                                    hive_db,
                                    hive_table,
                                    row_in_hive["name"],
                                    row_in_config["name"],
                                    row_in_hive["type"],
                                )
                            )
                            self.common_operations.execute_hive_query(query)

                        elif self.import_config.etlEngine == constant.ETL_ENGINE_SPARK:
                            query = (
                                "alter table `%s`.`%s`.`%s` rename column `%s` to %s"
                                % (
                                    self.common_operations.sparkCatalogName,
                                    hive_db,
                                    hive_table,
                                    row_in_hive["name"],
                                    row_in_config["name"],
                                )
                            )
                            logging.info(query)
                            self.spark.sql(query)

                        self.import_config.log_hive_column_rename(
                            row_in_config["name"],
                            row_in_hive["name"],
                            hiveDB=hive_db,
                            hiveTable=hive_table,
                        )
                        if row_in_config["type"] != row_in_hive["type"]:
                            self.import_config.log_hive_column_type_change(
                                row_in_config["name"],
                                row_in_config["type"],
                                previous_columnType=row_in_hive["type"],
                                hiveDB=hive_db,
                                hiveTable=hive_table,
                            )

            if self.common_operations.metastore_type == constant.CATALOG_HIVE_DIRECT:
                self.common_operations.reconnect_hive_meta_store()

            columns_hive = self.common_operations.get_columns(
                hive_db,
                hive_table,
                includeType=True,
                includeComment=True,
                excludeDataLakeColumns=True,
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

            if self.import_config.etlEngine == constant.ETL_ENGINE_HIVE:
                query = "alter table `%s`.`%s` add columns (`%s` %s" % (
                    hive_db,
                    hive_table,
                    full_row["name"],
                    full_row["type"],
                )
                if full_row["comment"] != None:
                    query += ' COMMENT "%s"' % (full_row["comment"])
                query += ")"

                self.common_operations.execute_hive_query(query)

            elif self.import_config.etlEngine == constant.ETL_ENGINE_SPARK:
                query = "alter table `%s`.`%s`.`%s` add columns (`%s` %s" % (
                    self.common_operations.sparkCatalogName,
                    hive_db,
                    hive_table,
                    full_row["name"],
                    full_row["type"],
                )
                if full_row["comment"] != None:
                    query += ' comment "%s"' % (full_row["comment"])
                query += ")"

                logging.info(query)
                self.spark.sql(query)

            self.import_config.log_hive_column_add(
                full_row["name"],
                columnType=full_row["type"],
                hiveDB=hive_db,
                hiveTable=hive_table,
            )

        # Check for changed column types
        if self.common_operations.metastore_type == constant.CATALOG_HIVE_DIRECT:
            self.common_operations.reconnect_hive_meta_store()

        columns_hive = self.common_operations.get_columns(
            hive_db, hive_table, includeType=True, excludeDataLakeColumns=True
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

        # pd.set_option('display.max_rows', None)
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

        # 		for index, row in columnsMergeOnlyNameType.loc[columnsMergeOnlyNameType['Exist'] == 'right_only'].iterrows():
        # 			# This will iterate over columns that only exists in Target table and not on the import table
        # 			if self.import_config.import_is_incremental == False and self.import_config.import_with_merge == False:
        # 				# As the aim is to drop the column, we must make sure to only do that when we are sure to not loose data
        # 				columnName = row['name']
        #
        # 				# Drop column is not supported in Hive. Thats why the Hive code is not here
        # 				if self.import_config.etlEngine == constant.ETL_ENGINE_SPARK:
        # 					logging.info("Dropping column '%s'"%(columnName))
        # 					try:
        # 						query = "alter table `%s`.`%s`.`%s` drop column `%s` " \
        # 								%(self.common_operations.sparkCatalogName, hiveDB, hiveTable, columnName)
        # 						logging.info(query)
        # 						self.spark.sql(query)
        #
        # 					except pyspark.sql.utils.AnalysisException as e:
        # 						raise SQLerror(str(e))

        for index, row in columns_merge_only_name_type.loc[
            columns_merge_only_name_type["Exist"] == "left_only"
        ].iterrows():
            # This will iterate over columns that had the type changed from the source
            column_name = row["name"]
            column_type = row["type"]

            if column_types_changed == True:
                # This means that we compare with another value than will be used in the alter. We need to get the correct alter column type
                column_type = columns_config_saved.loc[
                    columns_config_saved["name"] == column_name
                ]["type"].iloc[0]

            if self.import_config.etlEngine == constant.ETL_ENGINE_HIVE:
                query = "alter table `%s`.`%s` change column `%s` `%s` %s" % (
                    hive_db,
                    hive_table,
                    column_name,
                    column_name,
                    column_type,
                )
                self.common_operations.execute_hive_query(query)

            elif self.import_config.etlEngine == constant.ETL_ENGINE_SPARK:
                query = "alter table `%s`.`%s`.`%s` alter column `%s` type %s" % (
                    self.common_operations.sparkCatalogName,
                    hive_db,
                    hive_table,
                    column_name,
                    column_type,
                )
                try:
                    logging.info(query)
                    self.spark.sql(query)
                except pyspark.sql.utils.AnalysisException as e:
                    if "Cannot update" in str(e) and "cannot be cast to" in str(e):
                        if (
                            self.import_config.import_is_incremental == False
                            and self.import_config.import_with_merge == False
                        ):
                            # This is a full import without merge. So it's safe to drop the column and recreate it
                            logging.info(
                                "Alter type of column '%s' failed due to incompatible column types. But as this is a full none-merge import, we will drop and recreate the column instead"
                            )
                            try:
                                query = (
                                    "alter table `%s`.`%s`.`%s` drop column `%s` "
                                    % (
                                        self.common_operations.sparkCatalogName,
                                        hive_db,
                                        hive_table,
                                        column_name,
                                    )
                                )
                                logging.info(query)
                                self.spark.sql(query)

                                query = (
                                    "alter table `%s`.`%s`.`%s` add columns (`%s` %s) "
                                    % (
                                        self.common_operations.sparkCatalogName,
                                        hive_db,
                                        hive_table,
                                        column_name,
                                        column_type,
                                    )
                                )
                                logging.info(query)
                                self.spark.sql(query)

                            except pyspark.sql.utils.AnalysisException as e:
                                raise SQLerror(str(e))
                        else:
                            logging.error(
                                "The source table schema have changed. During alter of column to change the type in the Iceberg table, Spark returned an error."
                            )
                            logging.error(
                                "This mostly means that the column types change is not supported and cant be handled by DBImport automatically. "
                            )
                            logging.error(
                                "To get out of this problem, the table needs to be recreated manually before a successful import can be executed again"
                            )
                            raise SQLerror(str(e))
                    else:
                        raise SQLerror(str(e))

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

            self.import_config.log_hive_column_type_change(
                row["name"],
                columnType=row["type"],
                previous_columnType=previous_column_type,
                hiveDB=hive_db,
                hiveTable=hive_table,
            )

        # Check for change column comments
        if self.common_operations.metastore_type == constant.CATALOG_HIVE_DIRECT:
            self.common_operations.reconnect_hive_meta_store()

        columns_hive = self.common_operations.get_columns(
            hive_db,
            hive_table,
            includeType=True,
            includeComment=True,
            excludeDataLakeColumns=True,
        )

        try:
            columns_hive["comment"] = columns_hive["comment"].replace(
                ["^$"], [None], regex=True
            )  # Replace blank column comments with None as it would otherwise trigger an alter table on every run
        except ValueError:
            # This exception will happen if all columns are None.
            pass

        columns_merge = pd.merge(
            columns_config, columns_hive, on=None, how="outer", indicator="Exist"
        )

        for index, row in columns_merge.loc[
            columns_merge["Exist"] == "left_only"
        ].iterrows():
            if row["comment"] == None:
                row["comment"] = ""

            column_name = row["name"]
            column_type = row["type"]
            column_comment = row["comment"]

            if column_types_changed == True:
                # This means that we compare with another value than will be used in the alter. We need to get the correct alter column type
                column_type = columns_config_saved.loc[
                    columns_config_saved["name"] == column_name
                ]["type"].iloc[0]

            if self.import_config.etlEngine == constant.ETL_ENGINE_HIVE:
                query = (
                    'alter table `%s`.`%s` change column `%s` `%s` %s comment "%s"'
                    % (
                        hive_db,
                        hive_table,
                        column_name,
                        column_name,
                        column_type,
                        column_comment,
                    )
                )
                self.common_operations.execute_hive_query(query)

            elif self.import_config.etlEngine == constant.ETL_ENGINE_SPARK:
                query = 'alter table `%s`.`%s`.`%s` alter column `%s` comment "%s"' % (
                    self.common_operations.sparkCatalogName,
                    hive_db,
                    hive_table,
                    column_name,
                    column_comment,
                )
                logging.info(query)
                self.spark.sql(query)

        logging.debug("Executing import_operations.update_hive_table() - Finished")

    def update_pk_on_target_table(
        self,
    ):
        """Update the PrimaryKey definition on the Target Hive Table"""
        logging.debug("Executing import_operations.update_pk_on_target_table()")
        if self.import_config.etlEngine == constant.ETL_ENGINE_HIVE:
            # Only required if using Hive as the ETL Engine. Spark Iceberg tables does not support primary keys

            self.update_pk_on_table(
                self.import_config.Hive_DB, self.import_config.Hive_Table
            )

        logging.debug(
            "Executing import_operations.update_pk_on_target_table() - Finished"
        )

    def update_pk_on_table(self, hive_db, hive_table):
        """Update the PrimaryKey definition on a Hive Table"""
        logging.debug("Executing import_operations.update_pk_on_table()")
        if self.import_config.get_pk_columns() == None:
            logging.info("No Primary Key informaton exists for this table.")
        else:
            logging.info("Creating Primary Key on table.")
            pk_on_hive_table = self.common_operations.get_pk(hive_db, hive_table)
            pk_in_config_db = self.import_config.get_pk_columns()
            self.common_operations.reconnect_hive_meta_store()

            if pk_on_hive_table != pk_in_config_db:
                # The PK information is not correct. We need to figure out what the problem is and alter the table to fix it
                logging.debug("PK in Hive:   %s" % (pk_on_hive_table))
                logging.debug("PK in Config: %s" % (pk_in_config_db))
                logging.info(
                    "PrimaryKey definition is not correct. If it exists, we will drop it and recreate"
                )
                logging.info("PrimaryKey on Hive table:     %s" % (pk_on_hive_table))
                logging.info("PrimaryKey from source table: %s" % (pk_in_config_db))

                current_pk_name = self.common_operations.get_pk_name(
                    hive_db, hive_table
                )
                if current_pk_name != None:
                    query = "alter table `%s`.`%s` " % (
                        self.import_config.Hive_DB,
                        self.import_config.Hive_Table,
                    )
                    query += "drop constraint %s" % (current_pk_name)
                    self.common_operations.execute_hive_query(query)

                # There are now no PK on the table. So it's safe to create one
                pk_name = "%s__%s__PK" % (
                    self.import_config.Hive_DB,
                    self.import_config.Hive_Table,
                )
                query = "alter table `%s`.`%s` " % (
                    self.import_config.Hive_DB,
                    self.import_config.Hive_Table,
                )
                query += "add constraint %s primary key (" % (pk_name)

                first_column = True
                for column in self.import_config.get_pk_columns().split(","):
                    if first_column == False:
                        query += ", "
                    query += "`" + column + "`"
                    first_column = False

                query += ") DISABLE NOVALIDATE"
                self.common_operations.execute_hive_query(query)
                self.common_operations.reconnect_hive_meta_store()

            else:
                logging.info("PrimaryKey definition is correct. No change required.")

        logging.debug("Executing import_operations.update_pk_on_table() - Finished")

    def update_fk_on_target_table(
        self,
    ):
        """Update the ForeignKeys definition on the Target Hive Table"""
        logging.debug("Executing import_operations.update_pk_on_target_table()")
        if self.import_config.etlEngine == constant.ETL_ENGINE_HIVE:
            # Only required if using Hive as the ETL Engine. Spark Iceberg tables does not support foreign keys

            self.update_fk_on_table(
                self.import_config.Hive_DB, self.import_config.Hive_Table
            )

        logging.debug(
            "Executing import_operations.update_pk_on_target_table() - Finished"
        )

    def update_fk_on_table(self, hive_db, hive_table):
        """Update the ForeignKeys definition on a Hive Table"""
        logging.debug("Executing import_operations.update_fk_on_table()")

        if self.import_config.create_foreign_keys == 0:
            logging.info(
                "No Foreign Keys will be created for this table as it's disabled in the configuration"
            )
            return

        foreign_keys_from_config = self.import_config.get_foreign_keys_from_config()
        foreign_keys_from_hive = self.common_operations.get_fks(hive_db, hive_table)

        if (
            foreign_keys_from_config.empty == True
            and foreign_keys_from_hive.empty == True
        ):
            logging.info("No Foreign Keys informaton exists for this table")
            return

        if foreign_keys_from_config.equals(foreign_keys_from_hive) == True:
            logging.info("ForeignKey definitions is correct. No change required.")
        else:
            logging.info(
                "ForeignKey definitions is not correct. Will check what exists and create/drop required foreign keys."
            )

            foreign_keys_diff = pd.merge(
                foreign_keys_from_config,
                foreign_keys_from_hive,
                on=None,
                how="outer",
                indicator="Exist",
            )

            for index, row in foreign_keys_diff.loc[
                foreign_keys_diff["Exist"] == "right_only"
            ].iterrows():
                # This will iterate over ForeignKeys that only exists in Hive
                # If it only exists in Hive, we delete it!

                logging.info(
                    "Dropping FK in Hive as it doesnt match the FK's in DBImport config database"
                )
                query = "alter table `%s`.`%s` drop constraint %s" % (
                    hive_db,
                    hive_table,
                    row["fk_name"],
                )
                try:
                    self.common_operations.execute_hive_query(query)
                except Exception as ex:
                    logging.error(ex)
                    logging.error(
                        "There was a problem when dropping FK. Will stop trying to drop the rest of the FK's on this table"
                    )
                    break

            for index, row in foreign_keys_diff.loc[
                foreign_keys_diff["Exist"] == "left_only"
            ].iterrows():
                # This will iterate over ForeignKeys that only exists in the DBImport configuration database
                # If it doesnt exist in Hive, we create it

                if (
                    self.common_operations.check_table(
                        row["ref_hive_db"],
                        row["ref_hive_table"],
                    )
                    == True
                ):
                    query = "alter table `%s`.`%s` add constraint %s foreign key (" % (
                        hive_db,
                        hive_table,
                        row["fk_name"],
                    )
                    first_loop = True
                    for column in row["source_column_name"].split(","):
                        if first_loop == False:
                            query += ", "
                        query += "`%s`" % (column)
                        first_loop = False

                    query += ") REFERENCES `%s`.`%s` (" % (
                        row["ref_hive_db"],
                        row["ref_hive_table"],
                    )
                    first_loop = True
                    for column in row["ref_column_name"].split(","):
                        if first_loop == False:
                            query += ", "
                        query += "`%s`" % (column)
                        first_loop = False
                    query += ") DISABLE NOVALIDATE RELY"

                    logging.info("Creating FK in Hive as it doesnt exist")

                    try:
                        self.common_operations.execute_hive_query(query)
                    except Exception as ex:
                        logging.error(ex)
                        logging.error(
                            "There was a problem when creating FK. Will stop trying to create the rest of the FK's on this table"
                        )
                        break

            self.common_operations.reconnect_hive_meta_store()

        logging.debug("Executing import_operations.update_fk_on_table()  - Finished")

    def remove_hive_locks(
        self,
    ):
        if self.import_config.etlEngine == constant.ETL_ENGINE_HIVE:
            if (
                self.import_config.common_config.get_config_value(
                    key="hive_remove_locks_by_force"
                )
                == True
            ):
                self.common_operations.remove_locks_by_force(
                    self.Hive_DB, self.Hive_Table
                )
                self.common_operations.remove_locks_by_force(
                    self.import_config.Hive_Import_DB,
                    self.import_config.Hive_Import_Table,
                )
                self.common_operations.remove_locks_by_force(
                    self.import_config.Hive_Import_DB,
                    self.import_config.Hive_Import_View,
                )
                self.common_operations.remove_locks_by_force(
                    self.import_config.Hive_Import_PKonly_DB,
                    self.import_config.Hive_Import_PKonly_Table,
                )
                self.common_operations.remove_locks_by_force(
                    self.import_config.Hive_Delete_DB,
                    self.import_config.Hive_Delete_Table,
                )
                self.common_operations.remove_locks_by_force(
                    self.import_config.Hive_HistoryTemp_DB,
                    self.import_config.Hive_HistoryTemp_Table,
                )

    def run_hive_major_compaction(
        self,
    ):
        self.common_operations.connect_to_hive(forceSkipTest=True)
        self.common_operations.run_hive_major_compaction(self.Hive_DB, self.Hive_Table)

    def run_hive_compaction(
        self,
    ):
        """Force a compaction on a Hive table. The type of compaction is configured per table or a major as default"""
        logging.debug("Executing import_operations.run_hive_compaction()")

        compaction_method = "none"
        and_wait = False
        compaction_description = ""

        if self.import_config.etlEngine == constant.ETL_ENGINE_SPARK:
            return

        if self.import_config.mergeCompactionMethod == "default":
            if (
                self.import_config.common_config.get_config_value(
                    key="hive_major_compact_after_merge"
                )
                == True
            ):
                compaction_method = "major"
                compaction_description = compaction_method
        else:
            if self.import_config.mergeCompactionMethod == "minor_and_wait":
                compaction_method = "minor"
                and_wait = True
                compaction_description = "minor and wait"
            elif self.import_config.mergeCompactionMethod == "major_and_wait":
                compaction_method = "major"
                and_wait = True
                compaction_description = "major and wait"
            else:
                compaction_method = self.import_config.mergeCompactionMethod
                compaction_description = compaction_method

        if compaction_method != "none":
            logging.info(
                "Running a '%s' compaction on Hive table" % (compaction_description)
            )

            try:
                if and_wait == False:
                    self.common_operations.execute_hive_query(
                        "alter table `%s`.`%s` compact '%s' "
                        % (self.Hive_DB, self.Hive_Table, compaction_method)
                    )
                else:
                    self.common_operations.execute_hive_query(
                        "alter table `%s`.`%s` compact '%s' and wait "
                        % (self.Hive_DB, self.Hive_Table, compaction_method)
                    )
            except:
                logging.error(
                    "Major compaction failed with the following error message:"
                )
                logging.warning(sys.exc_info())
            pass
        else:
            logging.info("Compaction is disabled for this import")

        logging.debug("Executing import_operations.run_hive_compaction() - Finished")

    def truncateTargetTable(
        self,
    ):
        if self.import_config.etlEngine == constant.ETL_ENGINE_HIVE:
            logging.info("Truncating Target table in Hive")
            self.common_operations.connect_to_hive(forceSkipTest=True)
            self.common_operations.truncateHiveTable(self.Hive_DB, self.Hive_Table)

        if self.import_config.etlEngine == constant.ETL_ENGINE_SPARK:
            logging.info("Truncating Target table in Spark")
            self.start_spark()

            query = "truncate table %s.`%s`.`%s`" % (
                self.common_operations.sparkCatalogName,
                self.Hive_DB,
                self.Hive_Table,
            )
            logging.info(query)
            self.spark.sql(query)

    def update_statistics_on_target_table(
        self,
    ):
        if self.import_config.etlEngine == constant.ETL_ENGINE_HIVE:
            logging.info("Updating the Hive statistics on the target table")
            self.common_operations.update_hive_table_statistics(
                self.Hive_DB, self.Hive_Table
            )

    def load_data_from_import_to_target_table(
        self,
    ):
        logging.info("Loading data from import table to target table")
        import_table_exists = self.common_operations.check_table(
            self.import_config.Hive_Import_DB, self.import_config.Hive_Import_Table
        )
        target_table_exists = self.common_operations.check_table(
            self.Hive_DB, self.Hive_Table
        )

        if import_table_exists == False:
            logging.error(
                "The import table %s.%s does not exist"
                % (
                    self.import_config.Hive_Import_DB,
                    self.import_config.Hive_Import_Table,
                )
            )
            self.import_config.remove_temporary_files()
            sys.exit(1)

        if target_table_exists == False:
            logging.error(
                "The target table %s.%s does not exist"
                % (self.Hive_DB, self.Hive_Table)
            )
            self.import_config.remove_temporary_files()
            sys.exit(1)

        if self.import_config.etlEngine == constant.ETL_ENGINE_HIVE:
            self.copy_hive_table(
                self.import_config.Hive_Import_DB,
                self.import_config.Hive_Import_Table,
                self.Hive_DB,
                self.Hive_Table,
            )

        if self.import_config.etlEngine == constant.ETL_ENGINE_SPARK:
            self.copy_spark_table(
                self.import_config.Hive_Import_DB,
                self.import_config.Hive_Import_Table,
                self.Hive_DB,
                self.Hive_Table,
            )

    def generate_copy_table_statement(
        self, source_db, source_table, target_db, target_table, iceberg_table=False
    ):
        """Generate the SQL statement that is required for inserting all rows from one table to another were column match"""
        logging.debug("Executing import_operations.generate_copy_table_statement()")
        column_merge = self.common_operations.get_hive_column_name_diff(
            source_db=source_db,
            source_table=source_table,
            target_db=target_db,
            target_table=target_table,
            import_tool=self.import_config.importTool,
            source_is_import_table=True,
        )

        first_loop = True
        column_definition_source = ""
        column_definition_target = ""
        for index, row in column_merge.loc[column_merge["Exist"] == "both"].iterrows():
            if first_loop == False:
                column_definition_source += ", "
            if first_loop == False:
                column_definition_target += ", "
            column_definition_source += "`%s`" % (row["sourceName"])
            column_definition_target += "`%s`" % (row["targetName"])
            first_loop = False

        if iceberg_table == False:
            query = "insert into `%s`.`%s` (" % (target_db, target_table)
        else:
            query = "insert into %s.`%s`.`%s` (" % (
                self.common_operations.sparkCatalogName,
                target_db,
                target_table,
            )

        query += column_definition_target
        if self.import_config.datalake_source != None:
            # query += ", datalake_source"
            query += ", %s" % (self.Hive_ColumnName_Source)
        if self.import_config.create_datalake_import_column == True:
            # query += ", datalake_import"
            query += ", %s" % (self.Hive_ColumnName_Import)

        query += ") select "
        query += column_definition_source
        if self.import_config.datalake_source != None:
            query += ", '%s'" % (self.import_config.datalake_source)
        if self.import_config.create_datalake_import_column == True:
            if iceberg_table == False:
                query += ", '%s'" % (self.import_config.sqoop_last_execution_timestamp)
            else:
                query += ", TIMESTAMP('%s')" % (
                    self.import_config.sqoop_last_execution_timestamp
                )

        if iceberg_table == False:
            query += " from `%s`.`%s` " % (source_db, source_table)
        else:
            query += " from %s.`%s`.`%s` " % (
                self.common_operations.sparkCatalogName,
                source_db,
                source_table,
            )

        if self.import_config.nomerge_ingestion_sql_addition != None:
            query += self.import_config.nomerge_ingestion_sql_addition

        logging.debug(
            "Executing import_operations.generate_copy_table_statement() - Finished"
        )
        return query

    def copy_hive_table(self, source_db, source_table, target_db, target_table):
        """Copy one Hive table into another for the columns that have the same name"""
        logging.debug("Executing import_operations.copy_hive_table()")

        query = self.generate_copy_table_statement(
            source_db, source_table, target_db, target_table, iceberg_table=False
        )
        self.common_operations.execute_hive_query(query)

        logging.debug("Executing import_operations.copy_hive_table() - Finished")

    def copy_spark_table(self, source_db, source_table, target_db, target_table):
        """Copy one Hive table into another for the columns that have the same name"""
        logging.debug("Executing import_operations.copy_hive_table()")

        query = self.generate_copy_table_statement(
            source_db, source_table, target_db, target_table, iceberg_table=True
        )
        self.start_spark()
        logging.info(query)
        self.spark.sql(query)

        logging.debug("Executing import_operations.copy_hive_table() - Finished")

    def reset_incr_min_max_values(self, maxValue):
        self.import_config.reset_incr_min_max_values(maxValue=maxValue)

    def reset_incr_max_value(self, hive_db=None, hive_table=None):
        """Will read the Max value from the Hive table and save that into the incr_maxvalue column"""
        logging.debug("Executing import_operations.reset_incr_max_value()")

        if hive_db == None:
            hive_db = self.Hive_DB
        if hive_table == None:
            hive_table = self.Hive_Table

        if self.import_config.importPhase == constant.IMPORT_PHASE_ORACLE_FLASHBACK:
            logging.error(
                "Oracle Flashback imports does not support repairing the table."
            )
            self.import_config.remove_temporary_files()
            sys.exit(1)

        if self.import_config.import_is_incremental == True:
            self.sqoopIncrNoNewRows = False
            query = "select max(`%s`) from `%s`.`%s` " % (
                self.import_config.sqoop_incr_column,
                hive_db,
                hive_table,
            )

            self.common_operations.connect_to_hive(forceSkipTest=True)
            result_df = self.common_operations.execute_hive_query(query)
            max_value = result_df.loc[0][0]

            if type(max_value) is pd._libs.tslibs.timestamps.Timestamp:
                (dt, micro) = max_value.strftime("%Y-%m-%d %H:%M:%S.%f").split(".")
                max_value = "%s.%03d" % (dt, int(micro) / 1000)
            else:
                if max_value != None:
                    max_value = int(max_value)

            if max_value != None:
                self.import_config.reset_sqoop_statistics(max_value)
                self.import_config.clear_stage()
        else:
            logging.error("This is not an incremental import. Nothing to repair.")
            self.import_config.remove_temporary_files()
            sys.exit(1)

        logging.debug("Executing import_operations.reset_incr_max_value() - Finished")

    def repair_all_incremental_imports(self):
        logging.debug("Executing import_operations.repair_all_incremental_imports()")
        result_df = self.import_config.get_all_active_incr_imports()

        tables_repaired = False
        for index, row in result_df.iterrows():
            tables_repaired = True
            hive_db = row[0]
            hive_table = row[1]
            logging.info(
                "Repairing table \u001b[33m%s.%s\u001b[0m" % (hive_db, hive_table)
            )
            self.set_hive_table(hive_db, hive_table)
            self.reset_incr_max_value(hive_db, hive_table)
            logging.info("")

        if tables_repaired == False:
            logging.info(
                "\n\u001b[32mNo incremental tables found that could be repaired\u001b[0m\n"
            )

        logging.debug(
            "Executing import_operations.repair_all_incremental_imports() - Finished"
        )

    def concatenateTable(self, hive_db=None, hive_table=None):
        """Execute a concatenate table on the specified table, or what is already set in the class"""
        logging.debug("Executing import_operations.concatenateTable()")

        self.connect_to_hive(forceSkipTest=True)

        if hive_db == None:
            hive_db = self.Hive_DB
        if hive_table == None:
            hive_table = self.Hive_Table

        hdfs_blocksize = int(
            self.import_config.common_config.get_config_value(key="hdfs_blocksize")
        )
        query = "set mapreduce.input.fileinputformat.split.minsize=%s" % (
            hdfs_blocksize
        )
        self.common_operations.execute_hive_query(query)

        query = "alter table `%s`.`%s` concatenate" % (hive_db, hive_table)
        self.common_operations.execute_hive_query(query)

        logging.debug("Executing import_operations.concatenateTable() - Finished")
