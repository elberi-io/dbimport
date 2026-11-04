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

from DBImportConfig import common_config, sendStatistics


class stage:
    def __init__(
        self, mysql_conn, connectionAlias=None, targetSchema=None, targetTable=None
    ):
        logging.debug("Executing stage.__init__()")

        self.connectionAlias = connectionAlias
        self.targetSchema = targetSchema
        self.targetTable = targetTable

        self.mysql_conn = mysql_conn
        self.mysql_cursor = self.mysql_conn.cursor(buffered=False)
        self.currentStage = None
        self.memoryStage = False
        self.stageTimeStart = None
        self.stageTimeStop = None
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

    def setTargetTable(self, connectionAlias, targetSchema, targetTable):
        """Sets the parameters to work against a new Target table"""

        self.connectionAlias = connectionAlias
        self.targetSchema = targetSchema
        self.targetTable = targetTable

    def getStageDescription(self, stage):
        stageDescription = ""

        # RULES FOR STAGE NUMBERS
        # 1. Take a series of even 50 numbers to a specific method
        # 2. Never use a number between 0 - 999
        # 3. 0, 1000 and 9999 is reserved
        # 4. IMPORT PHASE numbers must end with ??49 and the descripton "Import Phase Completed"
        # 5. 1001 -> 1999 is reserved for IMPORT PHASE
        # 6. 2000 -> 2999 is reserved for COPY PHASE
        # 7. 3000 -> 3999 is reserved for ETL PHASE

        if stage == 0:
            stageDescription = "Start of program"

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
            stageDescription = "Getting Hive tableschema"
        elif stage == 101:
            stageDescription = "Clear table rowcount"
        elif stage == 102:
            stageDescription = "Update Hive statistics on target table"
        elif stage == 103:
            stageDescription = "Create Export Temp table"
        elif stage == 104:
            stageDescription = "Truncate Export Temp table"
        elif stage == 105:
            stageDescription = "Insert data into Export Temp table"
        elif stage == 106:
            stageDescription = "Create Target table"
        elif stage == 107:
            stageDescription = "Truncate Target table"
        elif stage == 108:
            stageDescription = "Update Target table"
        elif stage == 109:
            stageDescription = "Sqoop Export"
        elif stage == 110:
            stageDescription = "Spark Export"
        elif stage == 111:
            stageDescription = "Validations"

        # Export_Phase_INCR
        elif stage == 150:
            stageDescription = "Getting Hive tableschema"
        elif stage == 151:
            stageDescription = "Clear table rowcount"
        elif stage == 152:
            stageDescription = "Update Hive statistics on target table"
        elif stage == 153:
            stageDescription = "Create Export Temp table"
        elif stage == 154:
            stageDescription = "Truncate Export Temp table"
        elif stage == 155:
            stageDescription = "Fetching the Max value from Hive"
        elif stage == 156:
            stageDescription = "Insert data into Export Temp table"
        elif stage == 157:
            stageDescription = "Create Target table"
        elif stage == 158:
            stageDescription = "Update Target table"
        elif stage == 159:
            stageDescription = "Sqoop Export"
        elif stage == 160:
            stageDescription = "Spark Export"
        elif stage == 161:
            stageDescription = "Validations"
        elif stage == 162:
            stageDescription = "Saving pending incremental values"

        elif stage == 950:
            stageDescription = "Update Atlas Schema"

        elif stage == 999:
            stageDescription = "DBImport completed successfully"

        return stageDescription

    def getStageShortName(self, stage):

        stageShortName = ""
        if stage == 0:
            stageShortName = "skip"

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
            stageShortName = "get_hive_tableschema"
        elif stage == 101:
            stageShortName = "clear_table_rowcount"
        elif stage == 102:
            stageShortName = "update_statistics"
        elif stage == 103:
            stageShortName = "create_temp_table"
        elif stage == 104:
            stageShortName = "truncate_temp_table"
        elif stage == 105:
            stageShortName = "insert_into_temp_table"
        elif stage == 106:
            stageShortName = "create_target_table"
        elif stage == 107:
            stageShortName = "truncate_target_table"
        elif stage == 108:
            stageShortName = "update_target_table"
        elif stage == 109:
            stageShortName = "sqoop"
        elif stage == 110:
            stageShortName = "spark"
        elif stage == 111:
            stageShortName = "validate"

        # Export_Phase_INCR
        elif stage == 150:
            stageShortName = "get_hive_tableschema"
        elif stage == 151:
            stageShortName = "clear_table_rowcount"
        elif stage == 152:
            stageShortName = "update_statistics"
        elif stage == 153:
            stageShortName = "create_temp_table"
        elif stage == 154:
            stageShortName = "truncate_temp_table"
        elif stage == 155:
            stageShortName = "fetch_maxvalue"
        elif stage == 156:
            stageShortName = "insert_into_temp_table"
        elif stage == 157:
            stageShortName = "create_target_table"
        elif stage == 158:
            stageShortName = "update_target_table"
        elif stage == 159:
            stageShortName = "sqoop"
        elif stage == 160:
            stageShortName = "spark"
        elif stage == 161:
            stageShortName = "validate"
        elif stage == 162:
            stageShortName = "skip"

        elif stage == 950:
            stageShortName = "atlas_schema"

        elif stage == 999:
            stageShortName = "skip"

        return stageShortName

    def convertStageStatisticsToJSON(self, **kwargs):
        """Reads the export_stage_statistics and convert the information to a JSON document"""
        logging.debug("Executing export_stage.convertStageStatisticsToJSON()")

        self.postDataToREST = self.common_config.getConfigValue(key="post_data_to_rest")
        self.postDataToRESTextended = self.common_config.getConfigValue(
            key="post_data_to_rest_extended"
        )
        self.postDataToKafka = self.common_config.getConfigValue(
            key="post_data_to_kafka"
        )
        self.postDataToKafkaExtended = self.common_config.getConfigValue(
            key="post_data_to_kafka_extended"
        )
        self.postDataToAWSSNS = self.common_config.getConfigValue(
            key="post_data_to_awssns"
        )
        self.postDataToAWSSNSExtended = self.common_config.getConfigValue(
            key="post_data_to_awssns_extended"
        )

        # 		self.postDataToREST = True

        if (
            self.postDataToREST == False
            and self.postDataToKafka == False
            and self.postDataToAWSSNS == False
        ):
            return

        export_stop = None
        jsonDataKafka = {}
        jsonDataKafka["type"] = "export"
        jsonDataKafka["status"] = "finished"
        jsonDataREST = jsonDataKafka.copy()
        jsonDataAWSSNS = jsonDataKafka.copy()

        for key, value in kwargs.items():
            jsonDataKafka[key] = value
            jsonDataREST[key] = value
            jsonDataAWSSNS[key] = value

        if self.postDataToKafkaExtended == False:
            jsonDataKafka.pop("sessions")

        if self.postDataToRESTextended == False:
            jsonDataREST.pop("sessions")

        if self.postDataToAWSSNSExtended == False:
            jsonDataAWSSNS.pop("sessions")

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

            stageShortName = self.getStageShortName(stage)

            if stageShortName == "":
                logging.error(
                    "There is no stage description for the JSON data for stage %s"
                    % (stage)
                )
                logging.error("Will put the raw stage number into the JSON document")
                stageShortName = str(stage)

            if stageShortName != "skip" and self.postDataToRESTextended == True:
                jsonDataREST["%s_start" % (stageShortName)] = str(stage_start)
                jsonDataREST["%s_stop" % (stageShortName)] = str(stage_stop)
                jsonDataREST["%s_duration" % (stageShortName)] = stage_duration

            if stageShortName != "skip" and self.postDataToKafkaExtended == True:
                jsonDataKafka["%s_start" % (stageShortName)] = str(stage_start)
                jsonDataKafka["%s_stop" % (stageShortName)] = str(stage_stop)
                jsonDataKafka["%s_duration" % (stageShortName)] = stage_duration

            if stageShortName != "skip" and self.postDataToAWSSNSExtended == True:
                jsonDataAWSSNS["%s_start" % (stageShortName)] = str(stage_start)
                jsonDataAWSSNS["%s_stop" % (stageShortName)] = str(stage_stop)
                jsonDataAWSSNS["%s_duration" % (stageShortName)] = stage_duration

            if stage == 0:
                export_start = stage_start

            if export_stop == None or stage_stop > export_stop:
                export_stop = stage_stop

        export_duration = int((export_stop - export_start).total_seconds())

        jsonDataKafka["start"] = str(export_start)
        jsonDataKafka["stop"] = str(export_stop)
        jsonDataKafka["duration"] = export_duration

        jsonDataREST["start"] = str(export_start)
        jsonDataREST["stop"] = str(export_stop)
        jsonDataREST["duration"] = export_duration

        jsonDataAWSSNS["start"] = str(export_start)
        jsonDataAWSSNS["stop"] = str(export_stop)
        jsonDataAWSSNS["duration"] = export_duration

        # 		print(jsonDataKafka)
        # 		print("================================================")
        # 		print(jsonDataREST)

        if self.postDataToKafka == True:
            result = self.sendStatistics.publishKafkaData(json.dumps(jsonDataKafka))
            if result == False:
                logging.info("Kafka publish failed!")
                logging.info("Saving the JSON to the json_to_send table instead")
                self.common_config.saveJsonToDatabase(
                    "import_statistics", "kafka", json.dumps(jsonDataKafka)
                )

        if self.postDataToREST == True:
            response = self.sendStatistics.sendRESTdata(json.dumps(jsonDataREST))
            if response != 200:
                logging.info("REST call failed!")
                logging.info("Saving the JSON to the json_to_send table instead")
                self.common_config.saveJsonToDatabase(
                    "import_statistics", "rest", json.dumps(jsonDataREST)
                )

        if self.postDataToAWSSNS == True:
            result = self.sendStatistics.sendAWSSNSdata(json.dumps(jsonDataAWSSNS))
            if result == False:
                logging.info("AWS SNS publish failed!")
                logging.info("Saving the JSON to the json_to_send table instead")
                self.common_config.saveJsonToDatabase(
                    "import_statistics", "rest", json.dumps(jsonDataREST)
                )

    def saveStageStatistics(self, **kwargs):
        """Reads the export_stage_statistics and insert it to the export_statistics table"""
        logging.debug("Executing stage.saveStageStatistics()")

        columnsPart = []
        valuesPart = []
        export_start = None
        export_stop = None
        export_duration = None

        for key, value in kwargs.items():
            columnsPart.append(key)
            if value == True:
                value = 1
            if value == False:
                value = 0
            valuesPart.append(value)

        query = "select stage, start, stop, duration from export_stage_statistics where dbalias = %s and target_schema = %s and target_table = %s"
        self.mysql_cursor.execute(
            query, (self.connectionAlias, self.targetSchema, self.targetTable)
        )
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor.statement))

        rowCounter = 0
        for row in self.mysql_cursor.fetchall():
            rowCounter += 1
            stage = row[0]
            stage_start = row[1]
            stage_stop = row[2]
            stage_duration = row[3]

            stageShortName = self.getStageShortName(stage)

            if stageShortName != "" and stageShortName != "skip":
                columnsPart.append("%s_start" % (stageShortName))
                valuesPart.append(str(stage_start))

                columnsPart.append("%s_stop" % (stageShortName))
                valuesPart.append(str(stage_stop))

                columnsPart.append("%s_duration" % (stageShortName))
                valuesPart.append(str(stage_duration))

            if stage == 0:
                export_start = stage_start

            if export_stop == None or stage_stop > export_stop:
                export_stop = stage_stop

        if rowCounter == 0:
            logging.warning("There is no statistical data to save.")
            return

        if export_stop != None and export_start != None:
            export_duration = int((export_stop - export_start).total_seconds())
        else:
            export_duration = None

        columnsPart.append("start")
        valuesPart.append(str(export_start))

        columnsPart.append("stop")
        valuesPart.append(str(export_stop))

        columnsPart.append("duration")
        valuesPart.append(str(export_duration))

        query = "insert into export_statistics "
        insertQuery = "(`"
        insertQuery += "`, `".join(map(str, columnsPart))
        insertQuery += "`) values ("

        strInsert = ""
        for i in valuesPart:
            if strInsert != "":
                strInsert += ", "
            strInsert += "%s"

        insertQuery += strInsert
        insertQuery += ")"
        query += insertQuery

        # 		print(columnsPart)
        # 		print("-------------------------------------------------")
        # 		print(valuesPart)
        # 		print("-------------------------------------------------")
        # 		print(insertQuery)

        self.mysql_cursor.execute(query, valuesPart)
        self.mysql_conn.commit()
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor.statement))

        query = "delete from export_statistics_last where dbalias = %s and target_schema = %s and target_table = %s"
        self.mysql_cursor.execute(
            query, (self.connectionAlias, self.targetSchema, self.targetTable)
        )
        self.mysql_conn.commit()
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor.statement))

        query = "insert into export_statistics_last "
        query += insertQuery
        self.mysql_cursor.execute(query, valuesPart)
        self.mysql_conn.commit()
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor.statement))

        logging.debug("Executing stage.saveStageStatistics() - Finished")

    def getStageStartStop(self, stage):
        """Returns the start and stop time for a specific stage. If no info can be found, None is returned"""
        logging.debug("Executing stage.getStageStartStop()")

        query = "select stage, start, stop, duration from export_stage_statistics where dbalias = %s and target_schema = %s and target_table = %s"
        self.mysql_cursor.execute(
            query, (self.connectionAlias, self.targetSchema, self.targetTable)
        )
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor.statement))

        returnDict = {}
        returnDict["startTime"] = "1970-01-01T00:00:00.000000Z"
        returnDict["stopTime"] = "1970-01-01T00:00:00.000000Z"

        for row in self.mysql_cursor.fetchall():
            stage_id = row[0]
            stage_start = row[1]
            stage_stop = row[2]
            stage_duration = row[3]

            stageShortName = self.getStageShortName(stage_id)
            if stageShortName == stage:
                returnDict["startTime"] = stage_start.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                returnDict["stopTime"] = stage_stop.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                break

        return returnDict

        logging.debug("Executing stage.getStageStartStop() - Finished")

    def setStageOnlyInMemory(self):
        self.memoryStage = True

    def clearStage(self):
        """Removes all stage information from the export_stage table"""
        logging.debug("Executing stage.clearStage()")

        if self.memoryStage == False:
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

        logging.debug("Executing stage.clearStage() - Finished")

    def getStage(self):
        """Fetches the stage from export_stage table and return stage value. If no stage, 0 is returned"""
        logging.debug("Executing stage.getStage()")

        if self.currentStage == None:
            if self.memoryStage == True:
                self.currentStage = 0
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
                    self.currentStage = 0
                else:
                    self.currentStage = row[0]

        self.stageTimeStart = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        self.stageTimeStop = None
        self.stageDurationStart = time.monotonic()
        self.stageDurationStop = 0.0
        self.stageDurationTime = 0.0

        logging.debug("Executing stage.getStage() - Finished")
        return self.currentStage

    def setStage(self, newStage, force=False):
        """Saves the stage information that is used for finding the correct step in the retries operations"""
        logging.debug("Executing stage.setStage()")

        if force == True:
            self.currentStage = 0

        if self.currentStage != None and self.currentStage > newStage:
            logging.debug("Executing stage.setStage() - Finished (no stage set)")
            return

        if self.memoryStage == True:
            self.currentStage = newStage
            logging.debug(
                "Executing stage.setStage() - Finished (stage only in memory)"
            )
            return

        # Calculate time and save to export_stage_statistics
        self.stage_duration_stop = time.monotonic()
        self.stageTimeStop = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        self.stageDurationStop = time.monotonic()
        self.stageDurationTime = self.stageDurationStop - self.stageDurationStart

        logging.debug("stageTimeStart: %s" % (self.stageTimeStart))
        logging.debug("stageTimeStop:  %s" % (self.stageTimeStop))
        logging.debug("stageDurationStart: %s" % (self.stageDurationStart))
        logging.debug("stageDurationStop:  %s" % (self.stageDurationStop))
        logging.debug("stageDurationTime:  %s" % (self.stageDurationTime))

        query = "select count(stage) from export_stage_statistics where dbalias = %s and target_schema = %s and target_table = %s and stage = %s"
        self.mysql_cursor.execute(
            query,
            (
                self.connectionAlias,
                self.targetSchema,
                self.targetTable,
                self.currentStage,
            ),
        )
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor.statement))

        row = self.mysql_cursor.fetchone()
        rowCount = row[0]
        if rowCount == 0:
            query = "insert into export_stage_statistics "
            query += (
                "( dbalias, target_schema, target_table, stage, start, stop, duration )"
            )
            query += "values "
            query += "( "
            query += "	'%s', " % (self.connectionAlias)
            query += "	'%s', " % (self.targetSchema)
            query += "	'%s', " % (self.targetTable)
            query += "	%s, " % (self.currentStage)
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
            query += "	and stage = '%s' " % (self.currentStage)

        self.mysql_cursor.execute(query)
        self.mysql_conn.commit()
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor.statement))

        stageDescription = self.getStageDescription(newStage)

        # Save stage information in export_stage
        query = "select count(stage) from export_stage where dbalias = %s and target_schema = %s and target_table = %s"
        self.mysql_cursor.execute(
            query, (self.connectionAlias, self.targetSchema, self.targetTable)
        )
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor.statement))

        row = self.mysql_cursor.fetchone()
        rowCount = row[0]
        if rowCount == 0:
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
            query += "	%s, " % (newStage)
            query += "	'%s', " % (stageDescription)
            query += "	'%s' " % (datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"))
            # 			query += "	0 "
            query += ") "
        else:
            query = "update export_stage set"
            query += "	stage = %s, " % (newStage)
            query += "	stage_description = '%s', " % (stageDescription)
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

        self.currentStage = newStage
        self.stageTimeStart = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        self.stageTimeStop = None
        self.stageDurationStart = time.monotonic()
        self.stageDurationStop = 0.0
        self.stageDurationTime = 0.0

        logging.debug("Executing stage.setStage() - Finished")

    def saveRetryAttempt(self, stage):
        """Saves the retry attempt in the export_retries_log table"""
        logging.debug("Executing stage.saveRetryAttempt()")

        if self.memoryStage == True:
            return

        stageDescription = self.getStageDescription(stage)

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
                stageDescription,
            ),
        )
        self.mysql_conn.commit()
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor.statement))

        logging.debug("Executing stage.saveRetryAttempt() - Finished")
