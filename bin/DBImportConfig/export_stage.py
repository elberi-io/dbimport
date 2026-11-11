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
import time
from datetime import datetime
from typing import Any

from DBImportConfig import common_config, sendStatistics


class stage:
    def __init__(
        self,
        mysql_conn: Any,
        connection_alias: str | None = None,
        target_schema: str | None = None,
        target_table: str | None = None,
    ) -> None:
        logging.debug("Executing stage.__init__()")

        self.connectionAlias = connection_alias
        self.targetSchema = target_schema
        self.targetTable = target_table

        self.mysql_conn = mysql_conn
        self.mysql_cursor = self.mysql_conn.cursor(buffered=False)
        self.current_stage: int | None = None
        self.memory_stage = False
        self.stageTimeStart: str | None = None
        self.stageTimeStop: str | None = None
        self.stageDurationStart = 0.0
        self.stageDurationStop = 0.0
        self.stageDurationTime = 0.0

        self.common_config = common_config.config()
        self.sendStatistics = sendStatistics.sendStatistics()

    # 		if configuration.get("REST_statistics", "post_export_data").lower() == "true":
    # 			self.post_export_data = True
    # 		else:
    # 			self.post_export_data = False
    #
    # 		self.rest = rest.restInterface()

    def set_target_table(
        self, connection_alias: str, target_schema: str, target_table: str
    ) -> None:
        """Sets the parameters to work against a new Target table"""

        self.connectionAlias = connection_alias
        self.targetSchema = target_schema
        self.targetTable = target_table

    def get_stage_description(self, stage: int) -> str:
        stage_description = ""

        # RULES FOR STAGE NUMBERS
        # 1. Take a series of even 50 numbers to a specific method
        # 2. Never use a number between 0 - 999
        # 3. 0, 1000 and 9999 is reserved
        # 4. IMPORT PHASE numbers must end with ??49 and the descripton "Import Phase Completed"
        # 5. 1001 -> 1999 is reserved for IMPORT PHASE
        # 6. 2000 -> 2999 is reserved for COPY PHASE
        # 7. 3000 -> 3999 is reserved for ETL PHASE

        if stage == 0:
            stage_description = "Start of program"

        # getHiveTableSchema
        # createExportTempTable
        # truncateExportTempTable
        # insertDataIntoExportTempTable
        # createTargetTable
        # truncateJDBCTable
        # sqoop
        # validate

        # Export_Phase_FULL
        elif stage == 100:
            stage_description = "Getting Hive tableschema"
        elif stage == 101:
            stage_description = "Clear table rowcount"
        elif stage == 102:
            stage_description = "Update Hive statistics on target table"
        elif stage == 103:
            stage_description = "Create Export Temp table"
        elif stage == 104:
            stage_description = "Truncate Export Temp table"
        elif stage == 105:
            stage_description = "Insert data into Export Temp table"
        elif stage == 106:
            stage_description = "Create Target table"
        elif stage == 107:
            stage_description = "Truncate Target table"
        elif stage == 108:
            stage_description = "Update Target table"
        elif stage == 109:
            stage_description = "Sqoop Export"
        elif stage == 110:
            stage_description = "Spark Export"
        elif stage == 111:
            stage_description = "Validations"

        # Export_Phase_INCR
        elif stage == 150:
            stage_description = "Getting Hive tableschema"
        elif stage == 151:
            stage_description = "Clear table rowcount"
        elif stage == 152:
            stage_description = "Update Hive statistics on target table"
        elif stage == 153:
            stage_description = "Create Export Temp table"
        elif stage == 154:
            stage_description = "Truncate Export Temp table"
        elif stage == 155:
            stage_description = "Fetching the Max value from Hive"
        elif stage == 156:
            stage_description = "Insert data into Export Temp table"
        elif stage == 157:
            stage_description = "Create Target table"
        elif stage == 158:
            stage_description = "Update Target table"
        elif stage == 159:
            stage_description = "Sqoop Export"
        elif stage == 160:
            stage_description = "Spark Export"
        elif stage == 161:
            stage_description = "Validations"
        elif stage == 162:
            stage_description = "Saving pending incremental values"

        elif stage == 950:
            stage_description = "Update Atlas Schema"

        elif stage == 999:
            stage_description = "DBImport completed successfully"

        return stage_description

    def get_stage_short_name(self, stage: int) -> str:

        stage_short_name = ""
        if stage == 0:
            stage_short_name = "skip"

        # RULES FOR STAGE NUMBERS
        # 1. Take a series of even 50 numbers to a specific method
        # 2. Never use a number between 0 - 999
        # 3. 0, 1000 and 9999 is reserved
        # 4. IMPORT PHASE numbers must end with ??49 and the descripton "Import Phase Completed"
        # 5. 1001 -> 1999 is reserved for IMPORT PHASE
        # 6. 2000 -> 2999 is reserved for COPY PHASE
        # 7. 3000 -> 3999 is reserved for ETL PHASE

        # Export_Phase_FULL
        elif stage == 100:
            stage_short_name = "get_hive_tableschema"
        elif stage == 101:
            stage_short_name = "clear_table_rowcount"
        elif stage == 102:
            stage_short_name = "update_statistics"
        elif stage == 103:
            stage_short_name = "create_temp_table"
        elif stage == 104:
            stage_short_name = "truncate_temp_table"
        elif stage == 105:
            stage_short_name = "insert_into_temp_table"
        elif stage == 106:
            stage_short_name = "create_target_table"
        elif stage == 107:
            stage_short_name = "truncate_target_table"
        elif stage == 108:
            stage_short_name = "update_target_table"
        elif stage == 109:
            stage_short_name = "sqoop"
        elif stage == 110:
            stage_short_name = "spark"
        elif stage == 111:
            stage_short_name = "validate"

        # Export_Phase_INCR
        elif stage == 150:
            stage_short_name = "get_hive_tableschema"
        elif stage == 151:
            stage_short_name = "clear_table_rowcount"
        elif stage == 152:
            stage_short_name = "update_statistics"
        elif stage == 153:
            stage_short_name = "create_temp_table"
        elif stage == 154:
            stage_short_name = "truncate_temp_table"
        elif stage == 155:
            stage_short_name = "fetch_maxvalue"
        elif stage == 156:
            stage_short_name = "insert_into_temp_table"
        elif stage == 157:
            stage_short_name = "create_target_table"
        elif stage == 158:
            stage_short_name = "update_target_table"
        elif stage == 159:
            stage_short_name = "sqoop"
        elif stage == 160:
            stage_short_name = "spark"
        elif stage == 161:
            stage_short_name = "validate"
        elif stage == 162:
            stage_short_name = "skip"

        elif stage == 950:
            stage_short_name = "atlas_schema"

        elif stage == 999:
            stage_short_name = "skip"

        return stage_short_name

    def convert_stage_statistics_to_json(self, **kwargs: Any) -> None:
        """Reads the export_stage_statistics and convert the information to a JSON document"""
        logging.debug("Executing export_stage.convert_stage_statistics_to_json()")

        self.postDataToREST = self.common_config.get_config_value(
            key="post_data_to_rest"
        )
        self.postDataToRESTextended = self.common_config.get_config_value(
            key="post_data_to_rest_extended"
        )
        self.postDataToKafka = self.common_config.get_config_value(
            key="post_data_to_kafka"
        )
        self.postDataToKafkaExtended = self.common_config.get_config_value(
            key="post_data_to_kafka_extended"
        )
        self.post_data_to_aws_sns = self.common_config.get_config_value(
            key="post_data_to_awssns"
        )
        self.postDataToAWSSNSExtended = self.common_config.get_config_value(
            key="post_data_to_awssns_extended"
        )

        # 		self.postDataToREST = True

        if (
            self.postDataToREST == False
            and self.postDataToKafka == False
            and self.post_data_to_aws_sns == False
        ):
            return

        export_stop = None
        json_data_kafka = {}
        json_data_kafka["type"] = "export"
        json_data_kafka["status"] = "finished"
        json_data_rest = json_data_kafka.copy()
        json_data_aws_sns = json_data_kafka.copy()

        for key, value in kwargs.items():
            json_data_kafka[key] = value
            json_data_rest[key] = value
            json_data_aws_sns[key] = value

        if self.postDataToKafkaExtended == False:
            json_data_kafka.pop("sessions")

        if self.postDataToRESTextended == False:
            json_data_rest.pop("sessions")

        if self.postDataToAWSSNSExtended == False:
            json_data_aws_sns.pop("sessions")

        query = "select stage, start, stop, duration from export_stage_statistics where dbalias = %s and target_schema = %s and target_table = %s"
        self.mysql_cursor.execute(
            query, (self.connectionAlias, self.targetSchema, self.targetTable)
        )
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor.statement))

        for row in self.mysql_cursor.fetchall():
            stage = row[0]
            stage_start = row[1]
            stage_stop = row[2]
            stage_duration = row[3]

            stage_short_name = self.get_stage_short_name(stage)

            if stage_short_name == "":
                logging.error(
                    "There is no stage description for the JSON data for stage %s"
                    % (stage)
                )
                logging.error("Will put the raw stage number into the JSON document")
                stage_short_name = str(stage)

            if stage_short_name != "skip" and self.postDataToRESTextended == True:
                json_data_rest[f"{stage_short_name}_start"] = str(stage_start)
                json_data_rest[f"{stage_short_name}_stop"] = str(stage_stop)
                json_data_rest[f"{stage_short_name}_duration"] = stage_duration

            if stage_short_name != "skip" and self.postDataToKafkaExtended == True:
                json_data_kafka[f"{stage_short_name}_start"] = str(stage_start)
                json_data_kafka[f"{stage_short_name}_stop"] = str(stage_stop)
                json_data_kafka[f"{stage_short_name}_duration"] = stage_duration

            if stage_short_name != "skip" and self.postDataToAWSSNSExtended == True:
                json_data_aws_sns[f"{stage_short_name}_start"] = str(stage_start)
                json_data_aws_sns[f"{stage_short_name}_stop"] = str(stage_stop)
                json_data_aws_sns[f"{stage_short_name}_duration"] = stage_duration

            if stage == 0:
                export_start = stage_start

            if export_stop == None or stage_stop > export_stop:
                export_stop = stage_stop

        export_duration = int((export_stop - export_start).total_seconds())

        json_data_kafka["start"] = str(export_start)
        json_data_kafka["stop"] = str(export_stop)
        json_data_kafka["duration"] = export_duration

        json_data_rest["start"] = str(export_start)
        json_data_rest["stop"] = str(export_stop)
        json_data_rest["duration"] = export_duration

        json_data_aws_sns["start"] = str(export_start)
        json_data_aws_sns["stop"] = str(export_stop)
        json_data_aws_sns["duration"] = export_duration

        # 		print(jsonDataKafka)
        # 		print("================================================")
        # 		print(jsonDataREST)

        if self.postDataToKafka == True:
            result = self.sendStatistics.publish_kafka_data(json.dumps(json_data_kafka))
            if result == False:
                logging.info("Kafka publish failed!")
                logging.info("Saving the JSON to the json_to_send table instead")
                self.common_config.save_json_to_database(
                    "import_statistics", "kafka", json.dumps(json_data_kafka)
                )

        if self.postDataToREST == True:
            response = self.sendStatistics.send_rest_data(json.dumps(json_data_rest))
            if response != 200:
                logging.info("REST call failed!")
                logging.info("Saving the JSON to the json_to_send table instead")
                self.common_config.save_json_to_database(
                    "import_statistics", "rest", json.dumps(json_data_rest)
                )

        if self.post_data_to_aws_sns == True:
            result = self.sendStatistics.send_aws_sns_data(
                json.dumps(json_data_aws_sns)
            )
            if result == False:
                logging.info("AWS SNS publish failed!")
                logging.info("Saving the JSON to the json_to_send table instead")
                self.common_config.save_json_to_database(
                    "import_statistics", "rest", json.dumps(json_data_rest)
                )

    def save_stage_statistics(self, **kwargs: Any) -> None:
        """Reads the export_stage_statistics and insert it to the export_statistics table"""
        logging.debug("Executing stage.save_stage_statistics()")

        columns_part = []
        values_part = []
        export_start = None
        export_stop = None
        export_duration = None

        for key, value in kwargs.items():
            columns_part.append(key)
            if value == True:
                value = 1
            if value == False:
                value = 0
            values_part.append(value)

        query = "select stage, start, stop, duration from export_stage_statistics where dbalias = %s and target_schema = %s and target_table = %s"
        self.mysql_cursor.execute(
            query, (self.connectionAlias, self.targetSchema, self.targetTable)
        )
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor.statement))

        row_counter = 0
        for row in self.mysql_cursor.fetchall():
            row_counter += 1
            stage = row[0]
            stage_start = row[1]
            stage_stop = row[2]
            stage_duration = row[3]

            stage_short_name = self.get_stage_short_name(stage)

            if stage_short_name != "" and stage_short_name != "skip":
                columns_part.append(f"{stage_short_name}_start")
                values_part.append(str(stage_start))

                columns_part.append(f"{stage_short_name}_stop")
                values_part.append(str(stage_stop))

                columns_part.append(f"{stage_short_name}_duration")
                values_part.append(str(stage_duration))

            if stage == 0:
                export_start = stage_start

            if export_stop == None or stage_stop > export_stop:
                export_stop = stage_stop

        if row_counter == 0:
            logging.warning("There is no statistical data to save.")
            return

        if export_stop != None and export_start != None:
            export_duration = int((export_stop - export_start).total_seconds())
        else:
            export_duration = None

        columns_part.append("start")
        values_part.append(str(export_start))

        columns_part.append("stop")
        values_part.append(str(export_stop))

        columns_part.append("duration")
        values_part.append(str(export_duration))

        query = "insert into export_statistics "
        insert_query = "(`"
        insert_query += "`, `".join(map(str, columns_part))
        insert_query += "`) values ("

        str_insert = ""
        for _i in values_part:
            if str_insert != "":
                str_insert += ", "
            str_insert += "%s"

        insert_query += str_insert
        insert_query += ")"
        query += insert_query

        # 		print(columnsPart)
        # 		print("-------------------------------------------------")
        # 		print(valuesPart)
        # 		print("-------------------------------------------------")
        # 		print(insertQuery)

        self.mysql_cursor.execute(query, values_part)
        self.mysql_conn.commit()
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor.statement))

        query = "delete from export_statistics_last where dbalias = %s and target_schema = %s and target_table = %s"
        self.mysql_cursor.execute(
            query, (self.connectionAlias, self.targetSchema, self.targetTable)
        )
        self.mysql_conn.commit()
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor.statement))

        query = "insert into export_statistics_last "
        query += insert_query
        self.mysql_cursor.execute(query, values_part)
        self.mysql_conn.commit()
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor.statement))

        logging.debug("Executing stage.save_stage_statistics() - Finished")

    def get_stage_start_stop(self, stage: str | None) -> dict[str, str]:
        """Returns the start and stop time for a specific stage. If no info can be found, 1970-01-01T00:00:00.000000Z is returned for both"""
        logging.debug("Executing stage.get_stage_start_stop()")

        query = "select stage, start, stop, duration from export_stage_statistics where dbalias = %s and target_schema = %s and target_table = %s"
        self.mysql_cursor.execute(
            query, (self.connectionAlias, self.targetSchema, self.targetTable)
        )
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor.statement))

        return_dict = {}
        return_dict["startTime"] = "1970-01-01T00:00:00.000000Z"
        return_dict["stopTime"] = "1970-01-01T00:00:00.000000Z"

        for row in self.mysql_cursor.fetchall():
            stage_id = row[0]
            stage_start = row[1]
            stage_stop = row[2]
            stage_duration = row[3]

            stage_short_name = self.get_stage_short_name(stage_id)
            if stage_short_name == stage:
                return_dict["startTime"] = stage_start.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                return_dict["stopTime"] = stage_stop.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                break

        return return_dict

    def set_stage_only_in_memory(self) -> None:
        self.memory_stage = True

    def clear_stage(self) -> None:
        """Removes all stage information from the export_stage table"""
        logging.debug("Executing stage.clear_stage()")

        if self.memory_stage == False:
            query = "delete from export_stage where dbalias = %s and target_schema = %s and target_table = %s"
            self.mysql_cursor.execute(
                query, (self.connectionAlias, self.targetSchema, self.targetTable)
            )
            self.mysql_conn.commit()
            logging.debug("SQL Statement executed: %s" % (self.mysql_cursor.statement))

            query = "delete from export_stage_statistics where dbalias = %s and target_schema = %s and target_table = %s"
            self.mysql_cursor.execute(
                query, (self.connectionAlias, self.targetSchema, self.targetTable)
            )
            self.mysql_conn.commit()
            logging.debug("SQL Statement executed: %s" % (self.mysql_cursor.statement))

        logging.debug("Executing stage.clear_stage() - Finished")

    def get_stage(self) -> int:
        """Fetches the stage from export_stage table and return stage value. If no stage, 0 is returned"""
        logging.debug("Executing stage.get_stage()")

        if self.current_stage == None:
            if self.memory_stage == True:
                self.current_stage = 0
            else:
                query = "select stage from export_stage where dbalias = %s and target_schema = %s and target_table = %s"
                self.mysql_cursor.execute(
                    query, (self.connectionAlias, self.targetSchema, self.targetTable)
                )
                logging.debug(
                    "SQL Statement executed: %s" % (self.mysql_cursor.statement)
                )

                row = self.mysql_cursor.fetchone()
                if row == None:
                    self.current_stage = 0
                else:
                    self.current_stage = row[0]

        self.stageTimeStart = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        self.stageTimeStop = None
        self.stageDurationStart = time.monotonic()
        self.stageDurationStop = 0.0
        self.stageDurationTime = 0.0

        logging.debug("Executing stage.get_stage() - Finished")
        return self.current_stage

    def set_stage(self, new_stage: int, force: bool = False) -> None:
        """Saves the stage information that is used for finding the correct step in the retries operations"""
        logging.debug("Executing stage.set_stage()")

        if force == True:
            self.current_stage = 0

        if self.current_stage != None and self.current_stage > new_stage:
            logging.debug("Executing stage.set_stage() - Finished (no stage set)")
            return

        if self.memory_stage == True:
            self.current_stage = new_stage
            logging.debug(
                "Executing stage.set_stage() - Finished (stage only in memory)"
            )
            return

        # Calculate time and save to export_stage_statistics
        self.stage_duration_stop = time.monotonic()
        self.stageTimeStop = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        self.stageDurationStop = time.monotonic()
        self.stageDurationTime = self.stageDurationStop - self.stageDurationStart

        logging.debug("stageTimeStart: %s", self.stageTimeStart)
        logging.debug("stageTimeStop:  %s", self.stageTimeStop)
        logging.debug("stageDurationStart: %s", self.stageDurationStart)
        logging.debug("stageDurationStop:  %s", self.stageDurationStop)
        logging.debug("stageDurationTime:  %s", self.stageDurationTime)

        query = "select count(stage) from export_stage_statistics where dbalias = %s and target_schema = %s and target_table = %s and stage = %s"
        self.mysql_cursor.execute(
            query,
            (
                self.connectionAlias,
                self.targetSchema,
                self.targetTable,
                self.current_stage,
            ),
        )
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor.statement))

        row = self.mysql_cursor.fetchone()
        row_count = row[0]
        if row_count == 0:
            query = "insert into export_stage_statistics "
            query += (
                "( dbalias, target_schema, target_table, stage, start, stop, duration )"
            )
            query += "values "
            query += "( "
            query += "	'%s', " % (self.connectionAlias)
            query += "	'%s', " % (self.targetSchema)
            query += "	'%s', " % (self.targetTable)
            query += "	%s, " % (self.current_stage)
            query += "	'%s', " % (self.stageTimeStart)
            query += "	'%s', " % (self.stageTimeStop)
            query += "	%s " % (round(self.stageDurationTime))
            query += ") "
        else:
            query = "update export_stage_statistics set"
            query += "	start = '%s', " % (self.stageTimeStart)
            query += "	stop = '%s', " % (self.stageTimeStop)
            query += "	duration = %s " % (round(self.stageDurationTime))
            query += "where "
            query += "	dbalias = '%s' " % (self.connectionAlias)
            query += "	and target_schema = '%s' " % (self.targetSchema)
            query += "	and target_table = '%s' " % (self.targetTable)
            query += "	and stage = '%s' " % (self.current_stage)

        self.mysql_cursor.execute(query)
        self.mysql_conn.commit()
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor.statement))

        stage_description = self.get_stage_description(new_stage)

        # Save stage information in export_stage
        query = "select count(stage) from export_stage where dbalias = %s and target_schema = %s and target_table = %s"
        self.mysql_cursor.execute(
            query, (self.connectionAlias, self.targetSchema, self.targetTable)
        )
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor.statement))

        row = self.mysql_cursor.fetchone()
        row_count = row[0]
        if row_count == 0:
            query = "insert into export_stage "
            query += "( "
            query += "	dbalias, "
            query += "	target_schema, "
            query += "	target_table, "
            query += "	stage, "
            query += "	stage_description, "
            query += "	stage_time "
            # 			query += "	unrecoverable_error "
            query += ") "
            query += "values "
            query += "( "
            query += "	'%s', " % (self.connectionAlias)
            query += "	'%s', " % (self.targetSchema)
            query += "	'%s', " % (self.targetTable)
            query += "	%s, " % (new_stage)
            query += "	'%s', " % (stage_description)
            query += "	'%s' " % (datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"))
            # 			query += "	0 "
            query += ") "
        else:
            query = "update export_stage set"
            query += "	stage = %s, " % (new_stage)
            query += "	stage_description = '%s', " % (stage_description)
            query += "	stage_time = '%s' " % (
                datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
            )
            query += "where "
            query += "	dbalias = '%s' " % (self.connectionAlias)
            query += "	and target_schema = '%s' " % (self.targetSchema)
            query += "	and target_table = '%s' " % (self.targetTable)

        self.mysql_cursor.execute(query)
        self.mysql_conn.commit()
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor.statement))

        self.current_stage = new_stage
        self.stageTimeStart = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        self.stageTimeStop = None
        self.stageDurationStart = time.monotonic()
        self.stageDurationStop = 0.0
        self.stageDurationTime = 0.0

        logging.debug("Executing stage.set_stage() - Finished")

    def save_retry_attempt(self, stage: int) -> None:
        """Saves the retry attempt in the export_retries_log table"""
        logging.debug("Executing stage.save_retry_attempt()")

        if self.memory_stage == True:
            return

        stage_description = self.get_stage_description(stage)

        query = "insert into export_retries_log "
        query += "( "
        query += "	dbalias, "
        query += "	target_schema, "
        query += "	target_table, "
        query += "	retry_time, "
        query += "	stage, "
        query += "	stage_description "
        query += ") "
        query += "values (%s, %s, %s, %s, %s, %s)"

        self.mysql_cursor.execute(
            query,
            (
                self.connectionAlias,
                self.targetSchema,
                self.targetTable,
                datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
                stage,
                stage_description,
            ),
        )
        self.mysql_conn.commit()
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor.statement))

        logging.debug("Executing stage.save_retry_attempt() - Finished")
