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
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any, TypeVar

from sqlalchemy.orm import Session

from dbimport.common.host_time import host_time_text
from dbimport.config_db import common_config, send_statistics
from dbimport.config_db import engine as config_engine
from dbimport.config_db.repositories.export_stages import ExportStagesRepository
from dbimport.config_db.repositories.export_statistics import (
    ExportStatisticsRepository,
)

_T = TypeVar("_T")


class Stage:
    def __init__(
        self,
        connection_alias: str | None = None,
        target_schema: str | None = None,
        target_table: str | None = None,
    ) -> None:
        logging.debug("Executing Stage.__init__()")

        self.connection_alias = connection_alias
        self.target_schema = target_schema
        self.target_table = target_table

        self.current_stage: int | None = None
        self.memory_stage = False
        self.stage_time_start: datetime | None = None
        self.stage_time_stop: datetime | None = None
        self.stage_duration_start = 0.0
        self.stage_duration_stop = 0.0
        self.stage_duration_time = 0.0

        self.common_config = common_config.Config()
        self.send_statistics = send_statistics.SendStatistics()

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

        self.connection_alias = connection_alias
        self.target_schema = target_schema
        self.target_table = target_table

    def _table(self) -> tuple[str, str, str]:
        if (
            self.connection_alias is None
            or self.target_schema is None
            or self.target_table is None
        ):
            raise ValueError("The stage has no target table")
        return self.connection_alias, self.target_schema, self.target_table

    @contextmanager
    def _write_stages(self) -> Iterator[ExportStagesRepository]:
        with Session(config_engine.engine()) as session:
            yield ExportStagesRepository(session)
            session.commit()

    @contextmanager
    def _write_statistics(self) -> Iterator[ExportStatisticsRepository]:
        with Session(config_engine.engine()) as session:
            yield ExportStatisticsRepository(session)
            session.commit()

    def _read_stages(self, read: Callable[[ExportStagesRepository], _T]) -> _T:
        with Session(config_engine.engine()) as session:
            return read(ExportStagesRepository(session))

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

        self.post_data_to_rest = self.common_config.get_config_value(
            key="post_data_to_rest"
        )
        self.post_data_to_rest_extended = self.common_config.get_config_value(
            key="post_data_to_rest_extended"
        )
        self.post_data_to_kafka = self.common_config.get_config_value(
            key="post_data_to_kafka"
        )
        self.post_data_to_kafka_extended = self.common_config.get_config_value(
            key="post_data_to_kafka_extended"
        )
        self.post_data_to_aws_sns = self.common_config.get_config_value(
            key="post_data_to_awssns"
        )
        self.post_data_to_aws_sns_extended = self.common_config.get_config_value(
            key="post_data_to_awssns_extended"
        )

        # 		self.post_data_to_rest = True

        if (
            self.post_data_to_rest == False
            and self.post_data_to_kafka == False
            and self.post_data_to_aws_sns == False
        ):
            return

        export_start: datetime | None = None
        export_stop: datetime | None = None
        json_data_kafka: dict[str, Any] = {}
        json_data_kafka["type"] = "export"
        json_data_kafka["status"] = "finished"
        json_data_rest = json_data_kafka.copy()
        json_data_aws_sns = json_data_kafka.copy()

        for key, value in kwargs.items():
            json_data_kafka[key] = value
            json_data_rest[key] = value
            json_data_aws_sns[key] = value

        if self.post_data_to_kafka_extended == False:
            json_data_kafka.pop("sessions")

        if self.post_data_to_rest_extended == False:
            json_data_rest.pop("sessions")

        if self.post_data_to_aws_sns_extended == False:
            json_data_aws_sns.pop("sessions")

        stage_times = self._read_stages(
            lambda stages: stages.stage_times(*self._table())
        )

        for stage_time in stage_times:
            stage = stage_time.stage
            stage_start = stage_time.start
            stage_stop = stage_time.stop
            stage_duration = stage_time.duration

            stage_short_name = self.get_stage_short_name(stage)

            if stage_short_name == "":
                logging.error(
                    "There is no stage description for the JSON data for stage %s"
                    % (stage)
                )
                logging.error("Will put the raw stage number into the JSON document")
                stage_short_name = str(stage)

            if stage_short_name != "skip" and self.post_data_to_rest_extended == True:
                json_data_rest[f"{stage_short_name}_start"] = host_time_text(
                    stage_start
                )
                json_data_rest[f"{stage_short_name}_stop"] = host_time_text(stage_stop)
                json_data_rest[f"{stage_short_name}_duration"] = stage_duration

            if stage_short_name != "skip" and self.post_data_to_kafka_extended == True:
                json_data_kafka[f"{stage_short_name}_start"] = host_time_text(
                    stage_start
                )
                json_data_kafka[f"{stage_short_name}_stop"] = host_time_text(stage_stop)
                json_data_kafka[f"{stage_short_name}_duration"] = stage_duration

            if (
                stage_short_name != "skip"
                and self.post_data_to_aws_sns_extended == True
            ):
                json_data_aws_sns[f"{stage_short_name}_start"] = host_time_text(
                    stage_start
                )
                json_data_aws_sns[f"{stage_short_name}_stop"] = host_time_text(
                    stage_stop
                )
                json_data_aws_sns[f"{stage_short_name}_duration"] = stage_duration

            if stage == 0:
                export_start = stage_start

            if export_stop is None or (
                stage_stop is not None and stage_stop > export_stop
            ):
                export_stop = stage_stop

        if export_start is None or export_stop is None:
            start_text = stop_text = "-1"
            export_duration = -1
        else:
            start_text = host_time_text(export_start)
            stop_text = host_time_text(export_stop)
            export_duration = int((export_stop - export_start).total_seconds())

        json_data_kafka["start"] = start_text
        json_data_kafka["stop"] = stop_text
        json_data_kafka["duration"] = export_duration

        json_data_rest["start"] = start_text
        json_data_rest["stop"] = stop_text
        json_data_rest["duration"] = export_duration

        json_data_aws_sns["start"] = start_text
        json_data_aws_sns["stop"] = stop_text
        json_data_aws_sns["duration"] = export_duration

        # 		print(jsonDataKafka)
        # 		print("================================================")
        # 		print(jsonDataREST)

        if self.post_data_to_kafka == True:
            result = self.send_statistics.publish_kafka_data(
                json.dumps(json_data_kafka)
            )
            if not result:
                logging.info("Kafka publish failed!")
                logging.info("Saving the JSON to the json_to_send table instead")
                self.common_config.save_json_to_database(
                    "import_statistics", "kafka", json.dumps(json_data_kafka)
                )

        if self.post_data_to_rest == True:
            response = self.send_statistics.send_rest_data(json.dumps(json_data_rest))
            if response != 200:
                logging.info("REST call failed!")
                logging.info("Saving the JSON to the json_to_send table instead")
                self.common_config.save_json_to_database(
                    "import_statistics", "rest", json.dumps(json_data_rest)
                )

        if self.post_data_to_aws_sns == True:
            result = self.send_statistics.send_aws_sns_data(
                json.dumps(json_data_aws_sns)
            )
            if not result:
                logging.info("AWS SNS publish failed!")
                logging.info("Saving the JSON to the json_to_send table instead")
                self.common_config.save_json_to_database(
                    "import_statistics", "rest", json.dumps(json_data_rest)
                )

    def save_stage_statistics(self, **kwargs: Any) -> None:
        """Reads the export_stage_statistics and insert it to the export_statistics table"""
        logging.debug("Executing Stage.save_stage_statistics()")

        statistics: dict[str, object] = {}
        export_start: datetime | None = None
        export_stop: datetime | None = None
        export_duration: int | None = None

        for key, value in kwargs.items():
            if value == True:
                value = 1
            if value == False:
                value = 0
            statistics[key] = value

        table = self._table()
        stage_times = self._read_stages(lambda stages: stages.stage_times(*table))

        for stage_time in stage_times:
            stage_short_name = self.get_stage_short_name(stage_time.stage)

            if stage_short_name != "" and stage_short_name != "skip":
                if f"{stage_short_name}_start" in statistics:
                    raise ValueError(
                        f"Two stages have the statistics column {stage_short_name}_start"
                    )
                statistics[f"{stage_short_name}_start"] = stage_time.start
                statistics[f"{stage_short_name}_stop"] = stage_time.stop
                statistics[f"{stage_short_name}_duration"] = stage_time.duration

            if stage_time.stage == 0:
                export_start = stage_time.start

            if export_stop is None or (
                stage_time.stop is not None and stage_time.stop > export_stop
            ):
                export_stop = stage_time.stop

        if not stage_times:
            logging.warning("There is no statistical data to save.")
            return

        if export_stop is not None and export_start is not None:
            export_duration = int((export_stop - export_start).total_seconds())

        statistics["start"] = export_start
        statistics["stop"] = export_stop
        statistics["duration"] = export_duration

        with self._write_statistics() as run_statistics:
            run_statistics.save_run(*table, statistics)

        logging.debug("Executing Stage.save_stage_statistics() - Finished")

    def get_stage_start_stop(self, stage: str | None) -> dict[str, str]:
        """Returns the start and stop time for a specific stage. If no info can be found, 1970-01-01T00:00:00.000000Z is returned for both"""
        logging.debug("Executing Stage.get_stage_start_stop()")

        return_dict = {}
        return_dict["startTime"] = "1970-01-01T00:00:00.000000Z"
        return_dict["stopTime"] = "1970-01-01T00:00:00.000000Z"

        stage_times = self._read_stages(
            lambda stages: stages.stage_times(*self._table())
        )
        for stage_time in stage_times:
            stage_short_name = self.get_stage_short_name(stage_time.stage)
            if stage_short_name == stage:
                if stage_time.start is None or stage_time.stop is None:
                    raise ValueError(
                        f"Stage {stage_time.stage} has an invalid start or stop time"
                    )
                return_dict["startTime"] = stage_time.start.strftime(
                    "%Y-%m-%dT%H:%M:%S.%fZ"
                )
                return_dict["stopTime"] = stage_time.stop.strftime(
                    "%Y-%m-%dT%H:%M:%S.%fZ"
                )
                break

        return return_dict

    def set_stage_only_in_memory(self) -> None:
        self.memory_stage = True

    def clear_stage(self) -> None:
        """Removes all stage information from the export_stage table"""
        logging.debug("Executing Stage.clear_stage()")

        if not self.memory_stage:
            with self._write_stages() as stages:
                stages.clear(*self._table())

        logging.debug("Executing Stage.clear_stage() - Finished")

    def get_stage(self) -> int:
        """Fetches the stage from export_stage table and return stage value. If no stage, 0 is returned"""
        logging.debug("Executing Stage.get_stage()")

        if self.current_stage == None:
            if self.memory_stage:
                self.current_stage = 0
            else:
                stored = self._read_stages(lambda stages: stages.stage(*self._table()))
                self.current_stage = 0 if stored is None else stored

        self.stage_time_start = datetime.now(UTC)
        self.stage_time_stop = None
        self.stage_duration_start = time.monotonic()
        self.stage_duration_stop = 0.0
        self.stage_duration_time = 0.0

        logging.debug("Executing Stage.get_stage() - Finished")
        return self.current_stage

    def set_stage(self, new_stage: int, force: bool = False) -> None:
        """Saves the stage information that is used for finding the correct step in the retries operations"""
        logging.debug("Executing Stage.set_stage()")

        if force == True:
            self.current_stage = 0

        if self.current_stage != None and self.current_stage > new_stage:
            logging.debug("Executing Stage.set_stage() - Finished (no stage set)")
            return

        if self.memory_stage:
            self.current_stage = new_stage
            logging.debug(
                "Executing Stage.set_stage() - Finished (stage only in memory)"
            )
            return

        # Calculate time and save to export_stage_statistics
        self.stage_time_stop = datetime.now(UTC)
        self.stage_duration_stop = time.monotonic()
        self.stage_duration_time = self.stage_duration_stop - self.stage_duration_start

        logging.debug("stageTimeStart: %s", self.stage_time_start)
        logging.debug("stageTimeStop:  %s", self.stage_time_stop)
        logging.debug("stageDurationStart: %s", self.stage_duration_start)
        logging.debug("stageDurationStop:  %s", self.stage_duration_stop)
        logging.debug("stageDurationTime:  %s", self.stage_duration_time)

        if self.current_stage is None or self.stage_time_start is None:
            raise ValueError("get_stage() must run before set_stage()")

        table = self._table()
        with self._write_stages() as stages:
            stages.save_stage_time(
                *table,
                self.current_stage,
                self.stage_time_start,
                self.stage_time_stop,
                round(self.stage_duration_time),
            )
            stages.save_stage(
                *table,
                new_stage,
                self.get_stage_description(new_stage),
                datetime.now(UTC),
            )

        self.current_stage = new_stage
        self.stage_time_start = datetime.now(UTC)
        self.stage_time_stop = None
        self.stage_duration_start = time.monotonic()
        self.stage_duration_stop = 0.0
        self.stage_duration_time = 0.0

        logging.debug("Executing Stage.set_stage() - Finished")

    def save_retry_attempt(self, stage: int) -> None:
        """Saves the retry attempt in the export_retries_log table"""
        logging.debug("Executing Stage.save_retry_attempt()")

        if self.memory_stage:
            return

        with self._write_stages() as stages:
            stages.log_retry(
                *self._table(),
                stage,
                self.get_stage_description(stage),
                datetime.now(UTC),
            )

        logging.debug("Executing Stage.save_retry_attempt() - Finished")
