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
from typing import Any

import mysql.connector
import pandas as pd

from common import constants as constant
from common.Exceptions import invalidConfiguration, undevelopedFeature, validationError
from common.Singleton import Singleton
from DBImportConfig import common_config, export_stage, sendStatistics


class config(metaclass=Singleton):
    def __init__(
        self,
        connection_alias: str | None = None,
        target_schema: str | None = None,
        target_table: str | None = None,
    ) -> None:
        logging.debug("Executing export_config.__init__()")
        self.mysql_conn: Any = None
        self.mysql_cursor01: Any = None
        self.start_date: str | None = None
        self.common_config = None

        self.connection_alias = connection_alias
        self.target_schema = target_schema
        self.target_table = target_table

        self.common_config = common_config.config()
        self.common_config.operation_type = "export"
        self.common_config.target_schema = target_schema
        self.common_config.target_table = target_table

        self.send_statistics = sendStatistics.sendStatistics()

        self.start_date = self.common_config.start_date
        self.mysql_conn = self.common_config.mysql_conn
        self.mysql_cursor01 = self.mysql_conn.cursor(buffered=True)
        self.mysql_cursor02 = self.mysql_conn.cursor(buffered=True)

        self.table_id: int | None = None
        self.export_type: str | None = None
        self.hive_db: str | None = None
        self.hive_table: str | None = None
        self.export_phase: str | None = None
        self.export_phase_description: str | None = None
        self.datalake_source_connection: str | None = None
        self.export_is_incremental: bool | None = None
        self.validate_export: bool | None = None
        self.hive_java_heap: int | None = None

        self.temp_table_needed: bool | None = None

        self.full_executed_command: str | None = None

        self.sqoop_mapcolumnjava: list[str] | None = None
        self.sqoop_last_mappers: int | None = None
        self.sqoop_last_size: int | None = None
        self.sqoop_last_rows: int | None = None
        self.incr_column: str | None = None
        self.incr_maxvalue: str | None = None
        self.incr_validation_method: str | None = None
        self.sql_where_addition: str | None = None
        self.truncate_target_table: bool | None = None
        self.sql_sessions: int | None = None
        self.sqoop_mappers: int | None = None
        self.generated_sqoop_options = None
        self.force_create_temp_table: bool | None = None

        self.aws_s3_bucket: str | None = None
        self.aws_s3_path: str | None = None
        self.aws_s3_bucket_path: str | None = None

        self.validation_method: str | None = None
        self.validation_custom_query_hive_sql: str | None = None
        self.validation_custom_query_target_sql: str | None = None
        self.validation_custom_query_hive_value: str | None = None
        self.validation_custom_query_target_value: str | None = None

        # Initialize the stage class that will handle all stage operations for us
        self.stage = export_stage.stage(
            self.mysql_conn,
            connection_alias=connection_alias,
            target_schema=target_schema,
            target_table=target_table,
        )

        logging.debug("Executing export_config.__init__() - Finished")

    def log_hive_column_add(
        self,
        column: str,
        column_type: str | None,
        description: str | None = None,
        hive_db: str | None = None,
        hive_table: str | None = None,
    ) -> None:
        self.common_config.log_hive_column_add(
            column=column,
            column_type=column_type,
            description=description,
            hive_db=hive_db,
            hive_table=hive_table,
        )

    def log_hive_column_type_change(
        self,
        column: str,
        column_type: str,
        previous_column_type: str | None = None,
        description: str | None = None,
        hive_db: str | None = None,
        hive_table: str | None = None,
    ) -> None:
        self.common_config.log_hive_column_type_change(
            column,
            column_type,
            previous_column_type=previous_column_type,
            description=description,
            hive_db=hive_db,
            hive_table=hive_table,
        )

    def log_hive_column_rename(
        self,
        column_name: str,
        previous_column_name: str,
        description: str | None = None,
        hive_db: str | None = None,
        hive_table: str | None = None,
    ) -> None:
        self.common_config.log_hive_column_rename(
            column_name,
            previous_column_name,
            description=description,
            hive_db=hive_db,
            hive_table=hive_table,
        )

    def log_target_column_add(
        self,
        column: str,
        column_type: str | None,
        description: str | None = None,
        db_alias: str | None = None,
        database: str | None = None,
        schema: str | None = None,
        table: str | None = None,
    ) -> None:
        self.common_config.log_jdbc_column_add(
            column=column,
            column_type=column_type,
            description=description,
            db_alias=db_alias,
            database=database,
            schema=schema,
            table=table,
        )

    def log_target_column_type_change(
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
        self.common_config.log_jdbc_column_type_change(
            column,
            column_type,
            previous_column_type=previous_column_type,
            description=description,
            db_alias=db_alias,
            database=database,
            schema=schema,
            table=table,
        )

    def log_target_column_rename(
        self,
        column_name: str,
        previous_column_name: str,
        description: str | None = None,
        db_alias: str | None = None,
        database: str | None = None,
        schema: str | None = None,
        table: str | None = None,
    ) -> None:
        self.common_config.log_jdbc_column_rename(
            column_name,
            previous_column_name,
            description=description,
            db_alias=db_alias,
            database=database,
            schema=schema,
            table=table,
        )

    def remove_temporary_files(self) -> None:
        self.common_config.remove_temporary_files()

    def set_stage(self, stage: int, force: bool = False) -> None:
        self.stage.set_stage(stage, force=force)

    def get_stage(self) -> int:
        return self.stage.get_stage()

    def clear_stage(self) -> None:
        self.stage.clear_stage()

    def save_retry_attempt(self, stage: int) -> None:
        self.stage.save_retry_attempt(stage)

    def set_stage_only_in_memory(self) -> None:
        self.stage.set_stage_only_in_memory()

    def save_stage_statistics(self) -> None:
        self.stage.save_stage_statistics(
            dbalias=self.connection_alias,
            target_database=self.common_config.jdbc_database,
            target_schema=self.target_schema,
            target_table=self.target_table,
            hive_db=self.hive_db,
            hive_table=self.hive_table,
            export_phase=self.export_phase,
            incremental=self.export_is_incremental,
            size=self.sqoop_last_size,
            rows=self.sqoop_last_rows,
            sessions=self.sqoop_last_mappers,
        )

    def convert_stage_statistics_to_json(self) -> None:
        self.stage.convert_stage_statistics_to_json(
            dbalias=self.connection_alias,
            target_database=self.common_config.jdbc_database,
            target_schema=self.target_schema,
            target_table=self.target_table,
            hive_db=self.hive_db,
            hive_table=self.hive_table,
            export_phase=self.export_phase,
            incremental=self.export_is_incremental,
            rows=self.sqoop_last_rows,
            sessions=self.sqoop_last_mappers,
        )

    def send_start_json(self) -> None:
        """Sends a start JSON document to REST and/or Kafka"""
        logging.debug("Executing export_config.send_start_json()")

        self.post_data_to_rest = self.common_config.get_config_value(
            key="post_data_to_rest"
        )
        self.post_data_to_kafka = self.common_config.get_config_value(
            key="post_data_to_kafka"
        )
        self.post_data_to_aws_sns = self.common_config.get_config_value(
            key="post_data_to_awssns"
        )

        if (
            self.post_data_to_rest == False
            and self.post_data_to_kafka == False
            and self.post_data_to_aws_sns == False
        ):
            return

        import_stop = None
        json_data: dict[str, Any] = {}
        json_data["type"] = "export"
        json_data["status"] = "started"
        json_data["dbalias"] = self.connection_alias
        json_data["target_database"] = self.common_config.jdbc_database
        json_data["target_schema"] = self.target_schema
        json_data["target_table"] = self.target_table
        json_data["hive_db"] = self.hive_db
        json_data["hive_table"] = self.hive_table
        json_data["export_phase"] = self.export_phase
        json_data["incremental"] = self.export_is_incremental

        if self.post_data_to_kafka == True:
            result = self.send_statistics.publish_kafka_data(json.dumps(json_data))
            if result == False:
                logging.warning("Kafka publish failed! No start message posted")

        if self.post_data_to_rest == True:
            response = self.send_statistics.send_rest_data(json.dumps(json_data))
            if response != 200:
                logging.warning("REST call failed! No start message posted")

        if self.post_data_to_aws_sns == True:
            result = self.send_statistics.send_aws_sns_data(json.dumps(json_data))
            if result == False:
                logging.warning("AWS SNS publish failed! No start message posted")

        logging.debug("Executing export_config.send_start_json() - Finished")

    def check_time_window(self) -> None:
        self.common_config.check_time_window(self.connection_alias)

    def check_jdbc_table(self) -> bool:
        return self.common_config.check_jdbc_table(
            schema=self.target_schema, table=self.target_table
        )

    def get_export_config(self) -> None:
        logging.debug("Executing export_config.get_export_config()")

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
            query, (self.connection_alias, self.target_schema, self.target_table)
        )
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement))

        if self.mysql_cursor01.rowcount != 1:
            raise invalidConfiguration(
                "The specified Hive Database and Table can't be found in the configuration database. Please check that you specified the correct database and table and that the configuration exists in the configuration database"
            )

        row = self.mysql_cursor01.fetchone()

        self.table_id = row[0]
        self.export_type = row[1]
        self.hive_db = row[2]
        self.hive_table = row[3]
        self.validate_export = row[4]
        self.sqoop_last_size = row[5]
        self.sqoop_last_rows = row[6]
        self.sqoop_last_mappers = row[7]
        self.incr_column = row[8]
        self.incr_maxvalue = row[9]
        truncate_target_table = row[10]
        self.incr_validation_method = row[11]
        self.hive_java_heap = row[12]
        self.export_tool = row[13]
        self.sql_where_addition = row[14]
        self.validation_method = row[15]
        self.validation_custom_query_hive_sql = row[16]
        self.validation_custom_query_target_sql = row[17]
        self.validation_custom_query_hive_value = row[18]
        self.validation_custom_query_target_value = row[19]
        self.force_create_temp_table = row[20]
        self.common_config.operation_timestamp = row[21]

        if self.validate_export == 0:
            self.validate_export = False
        else:
            self.validate_export = True

        if truncate_target_table == 1:
            self.truncate_target_table = True
        else:
            self.truncate_target_table = False

        if self.force_create_temp_table == 1:
            self.force_create_temp_table = True
        else:
            self.force_create_temp_table = False

        if (
            self.validation_method != constant.VALIDATION_METHOD_CUSTOMQUERY
            and self.validation_method != constant.VALIDATION_METHOD_ROWCOUNT
        ):
            raise invalidConfiguration(
                f"Only the values '{constant.VALIDATION_METHOD_ROWCOUNT}' or '{constant.VALIDATION_METHOD_CUSTOMQUERY}' is valid for column validationMethod in export_tables."
            )

        if (
            self.incr_validation_method != "full"
            and self.incr_validation_method != "incr"
        ):
            raise invalidConfiguration(
                "Only the values 'full' or 'incr' is valid for column incr_validation_method in export_tables."
            )

        if self.export_tool != "sqoop" and self.export_tool != "spark":
            logging.error("ERROR: Unsupported export Tool configured.")
            raise invalidConfiguration(
                "Unsupported export Tool configured (%s). Please check configuration"
                % (self.export_tool)
            )

        if self.export_tool == "sqoop":
            if (
                self.sql_where_addition != None
                and self.sql_where_addition.strip() != ""
            ):
                logging.error("ERROR: Unsupported export configuration")
                raise invalidConfiguration(
                    "Using a SQL Where configuration in 'sql_where_addition' column is not supported with sqoop exports"
                )

        # Get the connection details. This is needed in order to determine what kind of connection we talk about.
        self.lookup_connection_alias()

        if self.export_type == "full" and self.common_config.db_awss3 == False:
            self.export_phase = constant.EXPORT_PHASE_FULL
            self.export_phase_description = "Full Export"

        if self.export_type == "full" and self.common_config.db_awss3 == True:
            self.export_phase = constant.EXPORT_PHASE_AWSS3_FULL
            self.export_phase_description = "Full Export"

        if self.export_type == "incr" and self.common_config.db_awss3 == False:
            self.export_phase = constant.EXPORT_PHASE_INCR
            self.export_phase_description = "Incremental Export"

        if self.export_type == "incr" and self.common_config.db_awss3 == True:
            raise invalidConfiguration(
                "Incremental exports to AWS S3 is not supported in this version of DBImport"
            )

        if self.common_config.db_awss3 == True:
            if self.target_schema == "-" or self.target_schema == "":
                self.aws_s3_bucket_path = "%s/%s" % (
                    self.common_config.aws_s3_bucket,
                    self.target_table,
                )
            else:
                self.aws_s3_bucket_path = "%s/%s/%s" % (
                    self.common_config.aws_s3_bucket,
                    self.target_schema,
                    self.target_table,
                )

            self.aws_s3_bucket = self.aws_s3_bucket_path.split("/")[
                2
            ]  # Need to start on 2 as s3a:// is first
            self.aws_s3_path = "/".join(self.aws_s3_bucket_path.split("/")[3:])

        if self.export_phase is None:
            raise invalidConfiguration(
                "Import type '%s' is not a valid type. Please check configuration"
                % (self.import_type)
            )

        if self.export_phase == constant.EXPORT_PHASE_INCR:
            self.export_is_incremental = True
            if self.incr_column == None:
                raise invalidConfiguration(
                    "Incremental import requires a column configured in the 'incr_column' column"
                )
        else:
            self.export_is_incremental = False

        # Fetch data from jdbc_connection table
        query = "select datalake_source from jdbc_connections where dbalias = %s "
        self.mysql_cursor01.execute(query, (self.connection_alias,))
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement))

        if self.mysql_cursor01.rowcount != 1:
            raise invalidConfiguration(
                f"There is no JDBC Connection defined with the alias '{self.connection_alias}'"
            )

        row = self.mysql_cursor01.fetchone()

        self.datalake_source_connection = row[0]

        # Set the name of the history tables, temporary tables and such
        self.hive_export_temp_db: str = self.common_config.get_config_value(
            key="export_staging_database"
        )
        if self.target_schema == "-":
            self.hive_export_temp_table: str = (
                self.connection_alias.replace("-", "_")
                + "__"
                + self.target_table
                + "__exporttemptable"
            )
        else:
            self.hive_export_temp_table = (
                self.connection_alias.replace("-", "_")
                + "__"
                + self.target_schema
                + "__"
                + self.target_table
                + "__exporttemptable"
            )
        self.hive_export_temp_table = self.hive_export_temp_table.lower()

        logging.debug("Settings from export_config.get_export_config()")
        logging.debug("    tableID = %s", self.table_id)
        logging.debug("    exportType = %s", self.export_type)
        logging.debug("    hiveJavaHeap = %s", self.hive_java_heap)
        logging.debug("    hiveDB = %s", self.hive_db)
        logging.debug("    hiveTable = %s", self.hive_table)
        logging.debug("    sqlWhereAddition = %s", self.sql_where_addition)
        logging.debug("    exportIsIncremental = %s", self.export_is_incremental)
        logging.debug(
            "    datalakeSourceConnection = %s", self.datalake_source_connection
        )
        logging.debug("    exportPhaseDescription = %s", self.export_phase_description)
        logging.debug("    exportPhase = %s", self.export_phase)
        logging.debug("    hiveExportTempDB = %s" % (self.hive_export_temp_db))
        logging.debug("    hiveExportTempTable = %s", self.hive_export_temp_table)
        logging.debug("Executing export_config.get_export_config() - Finished")

    def lookup_connection_alias(self, connection_alias: str | None = None) -> None:

        if connection_alias == None:
            connection_alias = self.connection_alias

        self.common_config.lookup_connection_alias(connection_alias)

    def validate_custom_query(self) -> bool:
        """Validates the custom queries"""
        if (
            self.validation_custom_query_target_value == None
            or self.validation_custom_query_hive_value == None
        ):
            logging.error(
                "Validation failed! One of the custom queries did not return a result"
            )
            logging.info(
                "Result from Hive query:   %s", self.validation_custom_query_hive_value
            )
            logging.info(
                "Result from target query: %s",
                self.validation_custom_query_target_value,
            )
            raise validationError()

        if (
            self.validation_custom_query_target_value
            != self.validation_custom_query_hive_value
        ):
            logging.error(
                "Validation failed! The custom queries did not return the same result"
            )
            logging.info(
                "Result from Hive query:   %s", self.validation_custom_query_hive_value
            )
            logging.info(
                "Result from target query: %s",
                self.validation_custom_query_target_value,
            )
            raise validationError()

        logging.info("Custom query validation successful!")
        return True

    def save_custom_sql_validation_hive_value(
        self, json_value: str, print_info: bool = True
    ) -> None:
        logging.debug("Executing export_config.save_custom_sql_validation_hive_value()")
        if print_info == True:
            logging.info(
                "Saving the custom SQL validation data from Hive table to the configuration database"
            )

        query = "update export_tables set validationCustomQueryHiveValue = %s where table_id = %s"

        self.mysql_cursor01.execute(query, (json_value, self.table_id))
        self.mysql_conn.commit()
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement))

        logging.debug(
            "Executing export_config.save_custom_sql_validation_hive_value() - Finished"
        )

    def save_custom_sql_validation_target_value(
        self, json_value: str, print_info: bool = True
    ) -> None:
        logging.debug(
            "Executing export_config.save_custom_sql_validation_target_value()"
        )
        if print_info == True:
            logging.info(
                "Saving the custom SQL validation data from target table to the configuration database"
            )

        query = "update export_tables set validationCustomQueryTargetValue = %s where table_id = %s"

        self.mysql_cursor01.execute(query, (json_value, self.table_id))
        self.mysql_conn.commit()
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement))

        logging.debug(
            "Executing export_config.save_custom_sql_validation_target_value() - Finished"
        )

    def update_last_update_from_hive(self) -> None:
        # This function will update the import_tables.last_update_from_source to the startDate from the common class. This will later
        # be used to determine if all the columns exists in the source system as we set the same datatime on all columns
        # that we read from the source system
        logging.debug("")
        logging.debug("Executing export_config.update_last_update_from_hive()")

        # Update the import_tables.last_update_from_source with the current date
        query = (
            "update export_tables set last_update_from_hive = %s where table_id = %s "
        )
        self.mysql_cursor01.execute(query, (self.start_date, self.table_id))
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement))
        self.mysql_conn.commit()

        # We re-read the date just to make sure we have the exakt same in common_config. This will later be used to group yarn applications per import
        query = "select last_update_from_hive from export_tables where table_id = %s "
        self.mysql_cursor01.execute(query, (self.table_id,))
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement))

        row = self.mysql_cursor01.fetchone()
        self.common_config.operation_timestamp = row[0]

        logging.debug(
            "Executing export_config.update_last_update_from_hive() - Finished"
        )

    def save_column_data(self, columns_df: pd.DataFrame | None = None) -> None:
        logging.debug("Executing export_config.save_column_data()")
        logging.info("Saving column data to MySQL table - export_columns")

        for _index, row in columns_df.iterrows():
            column_name = row["name"]
            column_type = row["type"]
            column_comment = self.common_config.strip_unwanted_char_comment(
                row["comment"]
            )
            column_order = row["idx"]

            query = "select column_id from export_columns where table_id = %s and column_name = %s "
            self.mysql_cursor01.execute(query, (self.table_id, column_name))
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
                        self.table_id,
                        column_name,
                        column_type,
                        column_order,
                        self.hive_db,
                        self.hive_table,
                        self.start_date,
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
                        self.hive_db,
                        self.hive_table,
                        self.start_date,
                        column_comment,
                        column_id,
                    ),
                )

            logging.debug(
                "SQL Statement executed: %s" % (self.mysql_cursor01.statement)
            )
            self.mysql_conn.commit()

        logging.debug("Executing export_config.save_column_data() - Finished")

    def clear_validation_data(self) -> None:
        logging.debug("Executing export_config.clear_validation_data()")
        logging.info("Clearing rowcounts from previous exports")

        self.validation_custom_query_hive_value = None
        self.validation_custom_query_target_value = None

        query = "update export_tables set hive_rowcount = NULL, target_rowcount = NULL, validationCustomQueryHiveValue = NULL, validationCustomQueryTargetValue = NULL where table_id = %s"
        self.mysql_cursor01.execute(query, (self.table_id,))
        self.mysql_conn.commit()
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement))

        logging.debug("Executing export_config.clear_validation_data() - Finished")

    def is_export_temp_table_needed(
        self, hive_table_is_transactional: bool, hive_table_is_view: bool
    ) -> bool:
        """This function return True or False if a temptable is needed to export from"""
        logging.debug("Executing export_config.is_export_temp_table_needed()")

        self.temp_table_needed = False

        if self.force_create_temp_table == True:
            logging.info("Temporary table usage is forced")
            self.temp_table_needed = True
            return self.temp_table_needed

        if self.export_tool == "spark":
            return self.temp_table_needed

        if (
            hive_table_is_transactional == True
            or hive_table_is_view == True
            or self.export_is_incremental == True
        ):
            self.temp_table_needed = True

        query = "select count(column_id) from export_columns "
        query += "where table_id = %s and target_column_name is not null"
        self.mysql_cursor01.execute(query, (self.table_id,))
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement))

        row = self.mysql_cursor01.fetchone()
        if row[0] > 0:
            self.temp_table_needed = True

        logging.debug("tempTableNeeded: %s", self.temp_table_needed)
        logging.debug(
            "Executing export_config.is_export_temp_table_needed() - Finished"
        )
        return self.temp_table_needed

    def reset_max_value_from_target(self) -> None:
        logging.debug("Executing export_config.reset_max_value_from_target()")

        colum_quotation = self.common_config.get_quote_around_column()
        column = self.incr_column
        if self.common_config.get_jdbc_upper_case() == True:
            column = column.upper()
        query = "select max(%s%s%s) from " % (colum_quotation, column, colum_quotation)
        query += self.common_config.get_jdbc_sql_from_table(
            schema=self.target_schema, table=self.target_table
        )
        result_df = self.common_config.execute_jdbc_query(query)
        max_value = max_value = result_df.loc[0][0]
        self.reset_incr_min_max_values(max_value=max_value)

        logging.debug(
            "Executing export_config.reset_max_value_from_target() - Finished"
        )

    def get_columns_from_config_database(
        self,
        exclude_columns: bool = False,
        force_column_uppercase: bool = False,
        get_replaced_column_types: bool = True,
    ) -> pd.DataFrame:
        """Reads the columns from the configuration database and returns the information in a Pandas DF with the columns name, type and comment"""
        logging.debug("Executing export_config.get_columns_from_config_database()")
        hive_column_definition = ""

        query = "select "
        if force_column_uppercase == False:
            query += "	c.column_name as hiveColumnName, "
            query += "	c.target_column_name as targetColumnName, "
        else:
            query += "	upper(c.column_name) as hiveColumnName, "
            query += "	upper(c.target_column_name) as targetColumnName, "
        if get_replaced_column_types == True:
            query += "	if (c.target_column_type is not null and trim(c.target_column_type) != '', c.target_column_type, c.column_type) as columnType, "
        else:
            query += "	c.column_type as columnType, "
        query += " 	c.comment as comment "
        query += "from export_tables t "
        query += "join export_columns c on t.table_id = c.table_id "
        query += "where t.table_id = %s and t.last_update_from_hive = c.last_update_from_hive "
        if exclude_columns == True:
            query += "   and c.include_in_export = 1 "
        query += "order by c.column_order"

        self.mysql_cursor01.execute(query, (self.table_id,))
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
            "Executing export_config.get_columns_from_config_database() - Finished"
        )
        return result_df

    def convert_column_type_for_target_table(
        self, column_name: str, column_type: str
    ) -> tuple[str, str | None]:
        """Converts the column type to a type that is supported by the Target database type"""
        logging.debug("Executing export_config.convert_column_type_for_target_table()")

        map_column_java = None
        source_database_type = ""

        source_db = self.hive_db
        source_table = self.hive_table
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
                    source_table = self.hive_table

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
                    f"Error: Exports of column '{column_name}' with type 'string' to Oracle is not supported. Please set another columntype in 'target_column_type' or ignore this column in the export."
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
            "Executing export_config.convert_column_type_for_target_table() - Finished"
        )
        return column_type, map_column_java

    def update_target_table(self) -> None:
        """Updates the target table with new column definitions"""
        logging.debug("Executing export_config.update_target_table()")
        logging.info("Updating the Target Table")

        columns_source = self.get_columns_from_config_database(exclude_columns=True)
        force_uppercase = False

        target_schema = self.target_schema
        target_table = self.target_table

        if self.common_config.jdbc_servertype in (constant.ORACLE, constant.DB2_UDB):
            force_uppercase = True
            target_schema = self.target_schema.upper()
            target_table = self.target_table.upper()

        elif self.common_config.jdbc_servertype in (constant.POSTGRESQL):
            target_schema = self.target_schema.lower()
            target_table = self.target_table.lower()

        elif self.common_config.jdbc_servertype in (constant.SNOWFLAKE):
            target_schema = self.target_schema
            target_table = self.target_table

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

            # Need to force it to lower so the self.convert_column_type_for_target_table works
            column_type = column_type.lower()
            column_type, mapcolumnjava = self.convert_column_type_for_target_table(
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

        self.common_config.get_jdbc_table_definition(
            source_schema=target_schema, source_table=target_table, print_info=False
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
            for _, row in columns_merge_only_name.loc[
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

                    self.common_config.execute_jdbc_query(query)
                    alter_table_executed = True

                    logging.info(
                        "Column %s renamed to %s in Target Table",
                        row_in_target["name"],
                        row_in_source["name"],
                    )
                    self.log_target_column_rename(
                        row_in_source["name"],
                        row_in_target["name"],
                        db_alias=self.connection_alias,
                        database=self.common_config.jdbc_database,
                        schema=target_schema,
                        table=target_table,
                    )

                    if row_in_source["type"] != row_in_target["type"]:
                        logging.info(
                            "Column %s changed type from %s to %s",
                            row_in_source["name"],
                            row_in_target["type"],
                            row_in_source["type"],
                        )
                        self.log_target_column_type_change(
                            row_in_source["name"],
                            row_in_source["type"],
                            previous_column_type=row_in_target["type"],
                            db_alias=self.connection_alias,
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

                        self.common_config.execute_jdbc_query(query)
                        self.common_config.execute_jdbc_query(query)
                        alter_table_executed = True

                        logging.info(
                            "Column %s renamed to %s in Target Table",
                            row_in_target["name"],
                            row_in_source["name"],
                        )
                        self.log_target_column_rename(
                            row_in_source["name"],
                            row_in_target["name"],
                            db_alias=self.connection_alias,
                            database=self.common_config.jdbc_database,
                            schema=target_schema,
                            table=target_table,
                        )

                        if row_in_source["type"] != row_in_target[
                            "type"
                        ] and self.common_config.jdbc_servertype in (constant.MYSQL):
                            logging.info(
                                "Column %s changed type from %s to %s",
                                row_in_source["name"],
                                row_in_target["type"],
                                row_in_source["type"],
                            )
                            self.log_target_column_type_change(
                                row_in_source["name"],
                                row_in_source["type"],
                                previous_column_type=row_in_target["type"],
                                db_alias=self.connection_alias,
                                database=self.common_config.jdbc_database,
                                schema=target_schema,
                                table=target_table,
                            )

            # Update the DF because the target table was changed
            self.common_config.get_jdbc_table_definition(
                source_schema=target_schema, source_table=target_table, print_info=False
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

        for _, row in columns_merge_only_name.loc[
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

            self.common_config.execute_jdbc_query(query)
            alter_table_executed = True

            logging.info(
                "Column %s (%s) added to Target Table",
                full_row["name"],
                full_row["type"],
            )
            self.log_target_column_add(
                full_row["name"],
                column_type=full_row["type"],
                db_alias=self.connection_alias,
                database=self.common_config.jdbc_database,
                schema=target_schema,
                table=target_table,
            )

        # Update the DF because the target table was changed
        self.common_config.get_jdbc_table_definition(
            source_schema=target_schema, source_table=target_table, print_info=False
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

        for _, row in columns_merge_only_name_type.loc[
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

            self.common_config.execute_jdbc_query(query)
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
                "Column %s changed type from '%s' to '%s'",
                row["name"],
                previous_column_type,
                row["type"],
            )
            self.log_target_column_type_change(
                row["name"],
                column_type=row["type"],
                previous_column_type=previous_column_type,
                db_alias=self.connection_alias,
                database=self.common_config.jdbc_database,
                schema=target_schema,
                table=target_table,
            )

        # Check for comment changes
        # Update the DF because the target table was changed
        self.common_config.get_jdbc_table_definition(
            source_schema=target_schema, source_table=target_table, print_info=False
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
            self.common_config.execute_jdbc_query(query)

        # print(columnsSource)
        # print(columnsTarget)
        columns_merge = pd.merge(
            columns_source, columns_target, on=None, how="outer", indicator="Exist"
        )
        # print(columnsMerge)
        for _, row in columns_merge.loc[
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

            self.common_config.execute_jdbc_query(query)
            alter_table_executed = True

        logging.debug("Executing export_config.update_target_table() - Finished")

    def get_sqoop_map_column_java(self) -> list[str]:
        """Returns the generated mapColumnJava settngs for all columns"""
        logging.debug("Executing export_config.get_sqoop_map_column_java()")

        columns_df = self.get_columns_from_config_database(exclude_columns=True)
        self.sqoop_mapcolumnjava = []
        force_column_uppercase = False

        if self.common_config.jdbc_servertype in (constant.ORACLE, constant.DB2_UDB):
            force_column_uppercase = True

        for _index, row in columns_df.iterrows():
            column_type = row["columnType"]
            target_column_name = row["targetColumnName"]
            hive_column_name = row["hiveColumnName"]
            if target_column_name != None and target_column_name.strip() != "":
                column_name = target_column_name
            else:
                column_name = hive_column_name

            if force_column_uppercase == True:
                column_name = column_name.upper()

            column_type, map_column_java = self.convert_column_type_for_target_table(
                hive_column_name, column_type
            )
            if map_column_java != None:
                self.sqoop_mapcolumnjava.append(column_name + "=" + map_column_java)

        logging.debug("Executing export_config.get_sqoop_map_column_java() - Finished")
        return self.sqoop_mapcolumnjava

    def create_target_table(self) -> None:
        """Creates the target table on the JDBC connection. This is the table we will export to"""
        logging.debug("Executing export_config.create_target_table()")
        logging.info("Creating the Target Table")

        columns_df = self.get_columns_from_config_database(exclude_columns=True)
        force_column_uppercase = False
        column_quote = ""

        if self.common_config.jdbc_servertype in (constant.ORACLE, constant.DB2_UDB):
            target_schema = self.target_schema.upper()
            target_table = self.target_table.upper()
            force_column_uppercase = True
            column_quote = '"'
            query = 'create table "%s"."%s" (' % (target_schema, target_table)

        elif self.common_config.jdbc_servertype in (constant.POSTGRESQL):
            target_schema = self.target_schema.lower()
            target_table = self.target_table.lower()
            query = "create table %s.%s (" % (target_schema, target_table)

        elif self.common_config.jdbc_servertype in (constant.SNOWFLAKE):
            target_schema = self.target_schema
            target_table = self.target_table
            query = 'create table "%s"."%s" (' % (target_schema, target_table)
            column_quote = '"'

        else:
            target_schema = self.target_schema
            target_table = self.target_table
            if target_schema == "-":
                query = "create table %s (" % (target_table)
            else:
                query = "create table %s.%s (" % (target_schema, target_table)

        self.sqoop_mapcolumnjava = []
        first_loop = True

        for _index, row in columns_df.iterrows():
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

            column_type, map_column_java = self.convert_column_type_for_target_table(
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

        self.common_config.execute_jdbc_query(query)

        logging.debug("Executing export_config.create_target_table() - Finished")

    def save_export_statistics(
        self,
        sqoop_start_uts: int | None = None,
        sqoop_size: int | None = None,
        sqoop_rows: int | None = None,
        sqoop_incr_maxvalue_pending: Any = None,
        sqoop_mappers: int | None = None,
    ) -> None:
        logging.debug("Executing export_config.save_export_statistics()")
        logging.info("Saving export statistics")

        self.sqoop_start_uts = sqoop_start_uts
        self.sqoop_last_execution_timestamp = datetime.utcfromtimestamp(
            sqoop_start_uts
        ).strftime("%Y-%m-%d %H:%M:%S.000")

        query_param = []
        query = "update export_tables set "
        first_set = True
        if sqoop_start_uts is not None:
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
            self.sqoop_incr_maxvalue_pending = sqoop_incr_maxvalue_pending
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
            query_param.append(self.table_id)

            self.mysql_cursor01.execute(query, query_param)
            logging.debug(
                "SQL Statement executed: %s" % (self.mysql_cursor01.statement)
            )
            self.mysql_conn.commit()

        logging.debug("Executing export_config.save_export_statistics() - Finished")

    def save_hive_table_row_count(self, row_count: int) -> None:
        logging.debug("Executing export_config.save_hive_table_row_count()")
        logging.info(
            "Saving the number of rows in the Hive Table to the configuration database"
        )

        # Save the value to the database
        query = "update export_tables set hive_rowcount = %s where table_id = %s"
        self.mysql_cursor01.execute(query, (row_count, self.table_id))
        self.mysql_conn.commit()
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement))

        logging.debug("Executing export_config.save_hive_table_row_count() - Finished")

    def run_custom_validation_query_on_jdbc_table(self) -> None:
        logging.debug(
            "Executing export_config.run_custom_validation_query_on_jdbc_table()"
        )

        logging.info("Executing custom validation query on target table.")

        query = self.validation_custom_query_target_sql
        query = query.replace("${TARGET_SCHEMA}", self.target_schema)
        query = query.replace("${TARGET_TABLE}", self.target_table)

        logging.debug("Validation Query on JDBC table: %s", query)

        result_df = self.common_config.execute_jdbc_query(query)
        result_json = result_df.to_json(orient="values")
        self.validation_custom_query_target_value = result_json

        self.save_custom_sql_validation_target_value(
            json_value=result_json, print_info=False
        )

        if len(self.validation_custom_query_target_value) > 1024:
            logging.warning(
                "'%s' is to large.", self.validation_custom_query_target_value
            )
            raise invalidConfiguration(
                "The size of the json document on the custom query exceeds 1024 bytes. Change the query to create a result with less than 512 bytes"
            )

        logging.debug("resultDF:")
        logging.debug(result_df)
        logging.debug("resultJSON: %s" % (result_json))

        logging.debug(
            "Executing export_config.run_custom_validation_query_on_jdbc_table() - Finished"
        )

    def get_jdbc_table_row_count(self, where_statement: str | None = None) -> None:
        logging.debug("Executing export_config.get_jdbc_table_row_count()")

        logging.info(
            "Reading and saving number of rows in target table. This will later be used for validating the export"
        )

        jdbc_rows_full = None
        jdbc_rows_full = self.common_config.get_jdbc_table_row_count(
            self.target_schema, self.target_table, where_statement
        )

        # Save the value to the database
        query = "update export_tables set "
        query += "target_rowcount = %s " % (jdbc_rows_full)
        if self.export_tool == "spark":
            # Need to save the sqoop_last_rows if the export tool is spark, as we dont get this from spark the same way as sqoop gives us
            self.sqoop_last_rows = jdbc_rows_full
            query += ",sqoop_last_rows = %s " % (jdbc_rows_full)

        query += "where table_id = %s" % (self.table_id)

        self.mysql_cursor01.execute(query)
        self.mysql_conn.commit()
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement))

        logging.debug("Executing export_config.get_jdbc_table_row_count() - Finished")

    def reset_incr_min_max_values(self, max_value: Any) -> None:
        logging.debug("Executing export_config.reset_incr_min_max_values()")
        if max_value != None:
            logging.info(
                "Reseting incremental values for exports. New max value is: %s"
                % (max_value)
            )
        else:
            logging.info("Reseting incremental values for exports.")

        query = "update export_tables set "
        query += "	incr_minvalue = NULL, "
        query += "	incr_maxvalue = %s, "
        query += "	incr_minvalue_pending = NULL, "
        query += "	incr_maxvalue_pending = NULL "
        query += "where table_id = %s "

        self.mysql_cursor01.execute(query, (max_value, self.table_id))
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement))
        self.mysql_conn.commit()

        logging.debug("Executing export_config.reset_incr_min_max_values() - Finished")

    def validate_row_count(
        self, validate_sqoop: bool = False, incremental: bool = False
    ) -> bool:
        """Validates the rows based on values stored in export_tables -> hive_rowcount and target_rowcount. Returns True or False"""
        # incremental=True is used for normal validations but only on incremented rows, regardless if we are going to validate full or incr.

        logging.debug("Executing export_config.validate_row_count()")
        return_value = None

        if self.validate_export == True:
            # Reading the saved number from the configurationdatabase
            # 			if validateSqoop == False:
            if 1 == 1:
                if self.export_is_incremental == False or (
                    self.export_is_incremental == True
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

            self.mysql_cursor01.execute(query, (self.table_id,))
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
                    "Validation failed! The %s exceedes the allowed limit compared to the %s table",
                    validate_text_source,
                    validate_text_target,
                )
                if target_rowcount > source_rowcount:
                    logging.info(
                        "Diff between tables: %s" % (target_rowcount - source_rowcount)
                    )
                else:
                    logging.info(
                        "Diff between tables: %s" % (source_rowcount - target_rowcount)
                    )
                logging.info("%s rowcount: %s", validate_text_source, source_rowcount)
                logging.info("%s rowcount: %s", validate_text_target, target_rowcount)
                return_value = False
            else:
                logging.info("%s validation successful!", validate_text)
                logging.info("%s rowcount: %s", validate_text_source, source_rowcount)
                logging.info("%s rowcount: %s", validate_text_target, target_rowcount)
                logging.info("")
                return_value = True

        else:
            return_value = True

        logging.debug("Executing export_config.validate_row_count() - Finished")
        return return_value

    def calculate_job_mappers(self) -> None:
        """Based on previous sqoop size, the number of mappers for sqoop will be calculated. Formula is sqoop_size / 128MB."""

        logging.debug("Executing export_config.calculate_job_mappers()")
        logging.info(
            "Calculating the number of SQL sessions in the source system that the export will use"
        )

        sql_sessions_min = 1
        sql_sessions_max = int(
            self.common_config.get_config_value(key="export_max_sessions")
        )
        sql_sessions_default = int(
            self.common_config.get_config_value(key="export_default_sessions")
        )

        if sql_sessions_default > sql_sessions_max:
            sql_sessions_default = sql_sessions_max

        self.spark_max_executors = sql_sessions_max

        # Execute SQL query that calculates the value
        query = (
            "select sqoop_last_size, "
            "cast(sqoop_last_size / (1024*1024*128) as unsigned) as calculated_mappers, "
            "mappers "
            "from export_tables where "
            "table_id = %s"
        )
        self.mysql_cursor01.execute(query, (self.table_id,))
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
        self.sql_sessions = row[1]
        self.sqoop_mappers = row[2]

        if self.export_tool == "sqoop":
            if self.sqoop_mappers > 0:
                logging.info("Setting the number of mappers to a fixed value")
                self.sql_sessions = self.sqoop_mappers

            if self.sql_sessions == None:
                logging.info(
                    "Cant find the previous number of SQL sessions used. Will default to %s",
                    sql_sessions_default,
                )
                self.sql_sessions = sql_sessions_default
            else:
                if self.sql_sessions < sql_sessions_min:
                    self.sql_sessions = sql_sessions_min
                elif self.sql_sessions > sql_sessions_max:
                    self.sql_sessions = sql_sessions_max

                logging.info(
                    "The export will use %s parallell SQL sessions in the source system.",
                    self.sql_sessions,
                )

        elif self.export_tool == "spark":
            if self.sqoop_mappers > 0:
                logging.info("Setting the number of executors to a fixed value")
                logging.info(
                    "The export will use %s parallell SQL sessions in the source system.",
                    self.sqoop_mappers,
                )
                self.spark_max_executors = self.sqoop_mappers
                self.sql_sessions = self.sqoop_mappers
            else:
                # If 'mappers' is set to -1, we just use the value from the default configuration
                self.sql_sessions = self.spark_max_executors

        logging.debug("Executing export_config.calculate_job_mappers() - Finished")

    def save_incr_min_max_value(self, min_value: Any, max_value: Any) -> None:
        """Save the min value in the pendings column"""
        logging.debug("Executing export_config.save_incr_min_value()")

        query = "update export_tables set "
        query += "	incr_minvalue_pending = %s, "
        query += "	incr_maxvalue_pending = %s "
        query += "where table_id = %s"

        self.mysql_cursor01.execute(
            query,
            (
                min_value,
                max_value,
                self.table_id,
            ),
        )
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement))
        self.mysql_conn.commit()

        logging.debug("Executing export_config.save_incr_min_value() - Finished")

    def save_incr_pending_values(self) -> None:
        """After the incremental export is completed, we save the pending values so the next export starts from the correct spot"""
        logging.debug("Executing export_config.savePendingIncrValues()")

        query = "update export_tables set "
        query += "	incr_minvalue = incr_minvalue_pending, "
        query += "	incr_maxvalue = incr_maxvalue_pending "
        query += "where table_id = %s"

        self.mysql_cursor01.execute(query, (self.table_id,))
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement))
        self.mysql_conn.commit()

        query = "update export_tables set "
        query += "	incr_minvalue_pending = NULL, "
        query += "	incr_maxvalue_pending = NULL "
        query += "where table_id = %s"

        self.mysql_cursor01.execute(query, (self.table_id,))
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement))
        self.mysql_conn.commit()

        logging.debug("Executing export_config.savePendingIncrValues() - Finished")

    def is_there_incr_data_to_export(self) -> bool:
        """Returns False if minvalue_pending = maxvalue_pending. That means that there is no new data in Hive to export"""
        logging.debug("Executing export_config.is_there_incr_data_to_export()")

        data_to_export = True

        query = "select incr_minvalue_pending, incr_maxvalue_pending from export_tables where table_id = %s "
        self.mysql_cursor01.execute(query, (self.table_id,))
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement))
        row = self.mysql_cursor01.fetchone()
        min_value = row[0]
        max_value = row[1]

        if min_value == max_value:
            data_to_export = False

        logging.debug("dataToExport: %s", data_to_export)
        logging.debug(
            "Executing export_config.is_there_incr_data_to_export() - Finished"
        )
        return data_to_export

    def get_incr_where_statement(
        self, exclude_min_value: bool = False, where_for_target: bool = False
    ) -> str:
        """Returns the where statement that is needed to only work on the rows that was loaded incr"""
        logging.debug("Executing export_config.get_incr_where_statement()")

        column_name = self.incr_column
        column_quotation = ""
        where_statement = ""

        query = "select column_type from export_columns where table_id = %s and column_name = %s "
        self.mysql_cursor01.execute(query, (self.table_id, column_name))
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement))
        row = self.mysql_cursor01.fetchone()
        column_type = row[0]

        # COALESCE is needed if you are going to call a function that counts source target rows outside the normal import
        query = "select "
        query += "   COALESCE(incr_minvalue_pending, incr_minvalue), "
        query += "   COALESCE(incr_maxvalue_pending, incr_maxvalue) "
        query += "from export_tables where table_id = %s "
        self.mysql_cursor01.execute(query, (self.table_id,))
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement))
        row = self.mysql_cursor01.fetchone()
        min_value = row[0]
        max_value = row[1]

        # Regardless of how we translate the minValue in the next couple of rows, if it's NONE here, it will never be included in the where
        if min_value == None:
            exclude_min_value = True

        if where_for_target == True and self.common_config.jdbc_servertype in (
            constant.ORACLE,
            constant.DB2_UDB,
        ):
            column_name = column_name.upper()

        if (
            where_for_target == True
            and column_type == "timestamp"
            and self.common_config.jdbc_servertype == constant.MSSQL
        ):
            min_value = "CONVERT(datetime, '%s', 121) " % (min_value)
            max_value = "CONVERT(datetime, '%s', 121) " % (max_value)
        elif (
            where_for_target == True
            and column_type == "timestamp"
            and self.common_config.jdbc_servertype == constant.MYSQL
        ):
            min_value = f"STR_TO_DATE('{min_value}', '%Y-%m-%d %H:%i:%s')"
            max_value = f"STR_TO_DATE('{max_value}', '%Y-%m-%d %H:%i:%s')"
        elif (
            where_for_target == True
            and column_type == "timestamp"
            and self.common_config.jdbc_servertype == constant.ORACLE
        ):
            min_value = "TO_TIMESTAMP('%s', 'YYYY-MM-DD HH24:MI:SS.FF')" % (min_value)
            max_value = "TO_TIMESTAMP('%s', 'YYYY-MM-DD HH24:MI:SS.FF')" % (max_value)
        elif (
            where_for_target == True
            and column_type == "timestamp"
            and self.common_config.jdbc_servertype == constant.SNOWFLAKE
        ):
            min_value = "TO_TIMESTAMP('%s', 'YYYY-MM-DD HH24:MI:SS.FF6')" % (min_value)
            max_value = "TO_TIMESTAMP('%s', 'YYYY-MM-DD HH24:MI:SS.FF6')" % (max_value)
        elif (
            where_for_target == True
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
            where_for_target == True
            and self.common_config.jdbc_servertype == constant.ORACLE
        ):
            column_quotation = '"'

        if (
            where_for_target == True
            and self.common_config.jdbc_servertype == constant.SNOWFLAKE
        ):
            column_quotation = '"'

        if where_for_target == False:
            column_quotation = "`"

        if min_value != None and exclude_min_value == False:
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

        logging.debug("Executing export_config.get_incr_where_statement() - Finished")
        return where_statement

    def get_export_tables(self, dbalias: str, schema: str) -> pd.DataFrame:
        """Return all tables that are exported to a specific connection and schema"""
        logging.debug("Executing export_config.get_export_tables()")

        query = "select hive_db as hiveDB, hive_table as hiveTable from export_tables where dbalias = %s and target_schema = %s "
        self.mysql_cursor01.execute(query, (dbalias, schema))
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor01.statement))

        result_df = pd.DataFrame(self.mysql_cursor01.fetchall())
        if len(result_df) == 0:
            print(
                f"Error: No data returned when searching for tables that are exported to dbalias '{dbalias}' and schema '{schema}'"
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

        logging.debug("Executing export_config.get_export_tables() - Finished")
        return result_df

    def add_export_table(
        self, hive_db: str, hive_table: str, dbalias: str, schema: str, table: str
    ) -> bool:
        """Add source table to export_tables"""
        logging.debug("Executing export_config.add_export_table()")
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

        logging.debug("hiveDB:    %s", hive_db)
        logging.debug("hiveTable: %s", hive_table)
        logging.debug("dbalias:   %s", dbalias)
        logging.debug("schema:    %s", schema)
        logging.debug("table:     %s", table)

        try:
            self.mysql_cursor01.execute(
                query, (hive_db, hive_table, dbalias, schema, table)
            )
            self.mysql_conn.commit()
        except mysql.connector.errors.IntegrityError:
            logging.warning(
                "Hive table %s.%s cant be added. The Connection name, Schema and Table already exists",
                hive_db,
                hive_table,
            )
            return_value = False

        logging.debug("Executing export_config.add_export_table() - Finished")
        return return_value
