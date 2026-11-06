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
    def __init__(self, mysql_conn, Hive_DB, Hive_Table):
        logging.debug("Executing stage.__init__()")

        self.Hive_DB = Hive_DB
        self.Hive_Table = Hive_Table
        self.mysql_conn = mysql_conn
        self.mysql_cursor = self.mysql_conn.cursor(buffered=False)
        self.currentStage = None
        self.memoryStage = False
        self.stageTimeStart = None
        self.stageTimeStop = None
        self.stageDurationStart = 0.0
        self.stageDurationStop = 0.0
        self.stageDurationTime = 0.0

        self.common_config = common_config.config(Hive_DB, Hive_Table)
        # 		if configuration.get("REST_statistics", "post_import_data").lower() == "true":
        # 			self.post_import_data = True
        # 		else:
        # 			self.post_import_data = False

        self.sendStatistics = sendStatistics.sendStatistics()

    def setMySQLConnection(self, mysql_conn):
        self.mysql_conn = mysql_conn
        self.mysql_cursor = self.mysql_conn.cursor(buffered=False)

    def setHiveTable(self, Hive_DB, Hive_Table):
        """Sets the parameters to work against a new Hive database and table"""
        self.Hive_DB = Hive_DB.lower()
        self.Hive_Table = Hive_Table.lower()

    def getStageDescription(self, stage):
        stage_description = ""

        # RULES FOR STAGE NUMBERS
        # 1. Take a series of even 50 numbers to a specific method
        # 2. Never use a number between 0 - 999
        # 3. 0, 1000 and 9999 is reserved
        # 4. IMPORT PHASE numbers must end with ??49 and the descripton "Import Phase Completed"
        # 5. 1001 -> 1999 is reserved for IMPORT PHASE
        # 6. 2000 -> 2999 is reserved for COPY PHASE
        # 7. 3000 -> 3999 is reserved for ETL PHASE

        # Import_Phase_FULL
        if stage == 1010:
            stage_description = "Getting source tableschema"
        elif stage == 1011:
            stage_description = "Clear table rowcount"
        elif stage == 1012:
            stage_description = "Get source table rowcount"
        elif stage == 1013:
            stage_description = "Sqoop import"
        elif stage == 1014:
            stage_description = "Spark import"
        elif stage == 1015:
            stage_description = "Validate spark import table"
        elif stage == 1016:
            stage_description = "Spark ETL"
        elif stage == 1017:
            stage_description = "Validate spark target table"
        elif stage == 1018:
            stage_description = "Spark ETL finished"
        elif stage == 1020:
            stage_description = "Validate import"
        elif stage == 1045:
            stage_description = "Update Atlas"
        elif stage == 1049:
            stage_description = "Import Phase Completed"

        # Import_Phase_INCR
        elif stage == 1110:
            stage_description = "Getting source tableschema"
        elif stage == 1111:
            stage_description = "Clear table rowcount"
        elif stage == 1112:
            stage_description = "Sqoop import"
        elif stage == 1113:
            stage_description = "Spark import"
        elif stage == 1120:
            stage_description = "Get source table rowcount"
        elif stage == 1121:
            stage_description = "Validate import"
        elif stage == 1145:
            stage_description = "Update Atlas"
        elif stage == 1149:
            stage_description = "Import Phase Completed"

        # Import_Phase_ORACLE_FLASHBACK
        elif stage == 1210:
            stage_description = "Getting source tableschema"
        elif stage == 1211:
            stage_description = "Clear table rowcount"
        elif stage == 1212:
            stage_description = "Checking SCN number"
        elif stage == 1215:
            stage_description = "Sqoop import"
        elif stage == 1216:
            stage_description = "Spark import"
        elif stage == 1220:
            stage_description = "Get source table rowcount"
        elif stage == 1221:
            stage_description = "Validate sqoop import"
        elif stage == 1245:
            stage_description = "Update Atlas"
        elif stage == 1249:
            stage_description = "Import Phase Completed"

        # Import_Phase_MSSQL_CHANGE_TRACKING
        elif stage == 1260:
            stage_description = "Getting source tableschema"
        elif stage == 1261:
            stage_description = "Clear table rowcount"
        elif stage == 1262:
            stage_description = "Checking MSSQL Change Tracking functions"
        elif stage == 1264:
            stage_description = "Sqoop import"
        elif stage == 1265:
            stage_description = "Spark import"
        elif stage == 1270:
            stage_description = "Get source table rowcount"
        elif stage == 1271:
            stage_description = "Validate sqoop import"
        elif stage == 1295:
            stage_description = "Update Atlas"
        elif stage == 1299:
            stage_description = "Import Phase Completed"

        # Import_Phase_MONGO_FULL
        elif stage == 1310:
            stage_description = "Clear table rowcount"
        elif stage == 1315:
            stage_description = "Spark import"
        elif stage == 1320:
            stage_description = "Get Mongo rowcount"
        elif stage == 1325:
            stage_description = "Validate Spark rowcount"
        elif stage == 1349:
            stage_description = "Import Phase Completed"

        # Copy Phase
        elif stage == 2000:
            stage_description = "Validating schema version on remote DBImport instance"
        elif stage == 2001:
            stage_description = "Copy data to remote DBImport instance"
        elif stage == 2002:
            stage_description = "Copy schema to remote DBImport instance"
        elif stage == 2099:
            stage_description = "Copy Phase Completed"

        # Import_Phase_FULL & ETL_Phase_TRUNCATEINSERT
        elif stage == 3050:
            stage_description = "Connecting to Hive"
        elif stage == 3051:
            stage_description = "Creating the import table in the staging database"
        elif stage == 3052:
            stage_description = "Get Import table rowcount"
        elif stage == 3053:
            stage_description = "Validate import table"
        elif stage == 3054:
            stage_description = "Removing Hive locks by force"
        elif stage == 3055:
            stage_description = "Creating the target table"
        elif stage == 3056:
            stage_description = "Truncate target table"
        elif stage == 3057:
            stage_description = "Copy rows from import to target table"
        elif stage == 3058:
            stage_description = "Update Hive statistics on target table"
        elif stage == 3059:
            stage_description = "Get Target table rowcount"
        elif stage == 3060:
            stage_description = "Validate target table"
        elif stage == 3095:
            stage_description = "Invalidate Impala metadata"

        # Import_Phase_FULL & ETL_Phase_INSERT
        elif stage == 3100:
            stage_description = "Connecting to Hive"
        elif stage == 3101:
            stage_description = "Creating the import table in the staging database"
        elif stage == 3102:
            stage_description = "Get Import table rowcount"
        elif stage == 3103:
            stage_description = "Validate import table"
        elif stage == 3104:
            stage_description = "Removing Hive locks by force"
        elif stage == 3105:
            stage_description = "Creating the target table"
        elif stage == 3106:
            stage_description = "Copy rows from import to target table"
        elif stage == 3107:
            stage_description = "Update Hive statistics on target table"
        elif stage == 3108:
            stage_description = "Get Target table rowcount"
        elif stage == 3109:
            stage_description = "Validate target table"
        elif stage == 3145:
            stage_description = "Invalidate Impala metadata"

        # Import_Phase_INCR & ETL_Phase_INSERT
        elif stage == 3150:
            stage_description = "Connecting to Hive"
        elif stage == 3151:
            stage_description = "Creating the import table in the staging database"
        elif stage == 3152:
            stage_description = "Get Import table rowcount"
        elif stage == 3153:
            stage_description = "Validate import table"
        elif stage == 3154:
            stage_description = "Removing Hive locks by force"
        elif stage == 3155:
            stage_description = "Creating the target table"
        elif stage == 3156:
            stage_description = "Copy rows from import to target table"
        elif stage == 3157:
            stage_description = "Update Hive statistics on target table"
        elif stage == 3158:
            stage_description = "Get Target table rowcount"
        elif stage == 3159:
            stage_description = "Validate target table"
        elif stage == 3160:
            stage_description = "Saving pending incremental values"
        elif stage == 3195:
            stage_description = "Invalidate Impala metadata"

        # Import_Phase_FULL & ETL_Phase_MERGEHISTORYAUDIT
        elif stage == 3200:
            stage_description = "Connecting to Hive"
        elif stage == 3201:
            stage_description = "Creating the import table in the staging database"
        elif stage == 3202:
            stage_description = "Get Import table rowcount"
        elif stage == 3203:
            stage_description = "Validate import table"
        elif stage == 3204:
            stage_description = "Removing Hive locks by force"
        elif stage == 3205:
            stage_description = "Creating the target table"
        elif stage == 3206:
            stage_description = "Creating the history table"
        elif stage == 3207:
            stage_description = "Creating the delete table"
        elif stage == 3208:
            stage_description = "Merge Import table with Target table"
        elif stage == 3209:
            stage_description = "Update Hive statistics on target table"
        elif stage == 3210:
            stage_description = "Get Target table rowcount"
        elif stage == 3211:
            stage_description = "Validate target table"
        elif stage == 3212:
            stage_description = "Running major compaction on Target table"
        elif stage == 3245:
            stage_description = "Invalidate Impala metadata"

        # Import_Phase_FULL & ETL_Phase_MERGEONLY
        elif stage == 3250:
            stage_description = "Connecting to Hive"
        elif stage == 3251:
            stage_description = "Creating the import table in the staging database"
        elif stage == 3252:
            stage_description = "Get Import table rowcount"
        elif stage == 3253:
            stage_description = "Validate import table"
        elif stage == 3254:
            stage_description = "Removing Hive locks by force"
        elif stage == 3255:
            stage_description = "Creating the target table"
        elif stage == 3256:
            stage_description = "Creating the delete table"
        elif stage == 3257:
            stage_description = "Merge Import table with Target table"
        elif stage == 3258:
            stage_description = "Update Hive statistics on target table"
        elif stage == 3259:
            stage_description = "Get Target table rowcount"
        elif stage == 3260:
            stage_description = "Validate target table"
        elif stage == 3261:
            stage_description = "Running major compaction on Target table"
        elif stage == 3295:
            stage_description = "Invalidate Impala metadata"

        # Import_Phase_INCR & ETL_Phase_MERGEONLY
        elif stage == 3300:
            stage_description = "Connecting to Hive"
        elif stage == 3301:
            stage_description = "Creating the import table in the staging database"
        elif stage == 3302:
            stage_description = "Get Import table rowcount"
        elif stage == 3303:
            stage_description = "Validate import table"
        elif stage == 3304:
            stage_description = "Removing Hive locks by force"
        elif stage == 3305:
            stage_description = "Creating the target table"
        elif stage == 3306:
            stage_description = "Merge Import table with Target table"
        elif stage == 3307:
            stage_description = "Update Hive statistics on target table"
        elif stage == 3308:
            stage_description = "Get Target table rowcount"
        elif stage == 3309:
            stage_description = "Validate target table"
        elif stage == 3310:
            stage_description = "Saving pending incremental values"
        elif stage == 3311:
            stage_description = "Running major compaction on Target table"
        elif stage == 3345:
            stage_description = "Invalidate Impala metadata"

        # Import_Phase_INCR & ETL_Phase_MERGEHISTORYAUDIT
        elif stage == 3350:
            stage_description = "Connecting to Hive"
        elif stage == 3351:
            stage_description = "Creating the import table in the staging database"
        elif stage == 3352:
            stage_description = "Get Import table rowcount"
        elif stage == 3353:
            stage_description = "Validate import table"
        elif stage == 3354:
            stage_description = "Removing Hive locks by force"
        elif stage == 3355:
            stage_description = "Creating the target table"
        elif stage == 3356:
            stage_description = "Creating the history table"
        elif stage == 3357:
            stage_description = "Merge Import table with Target table"
        elif stage == 3358:
            stage_description = "Update Hive statistics on target table"
        elif stage == 3359:
            stage_description = "Get Target table rowcount"
        elif stage == 3360:
            stage_description = "Validate target table"
        elif stage == 3361:
            stage_description = "Saving pending incremental values"
        elif stage == 3362:
            stage_description = "Running major compaction on Target table"
        elif stage == 3395:
            stage_description = "Invalidate Impala metadata"

        # Import_Phase_ORACLE_FLASHBACK & ETL_Phase_MERGEONLY
        elif stage == 3400:
            stage_description = "Connecting to Hive"
        elif stage == 3401:
            stage_description = "Creating the import table in the staging database"
        elif stage == 3402:
            stage_description = "Get Import table rowcount"
        elif stage == 3403:
            stage_description = "Validate import table"
        elif stage == 3404:
            stage_description = "Removing Hive locks by force"
        elif stage == 3405:
            stage_description = "Creating the target table"
        elif stage == 3406:
            stage_description = "Merge Import table with Target table"
        elif stage == 3407:
            stage_description = "Update Hive statistics on target table"
        elif stage == 3408:
            stage_description = "Get Target table rowcount"
        elif stage == 3409:
            stage_description = "Validate target table"
        elif stage == 3410:
            stage_description = "Saving pending incremental values"
        elif stage == 3411:
            stage_description = "Running major compaction on Target table"
        elif stage == 3445:
            stage_description = "Invalidate Impala metadata"

        # Import_Phase_FULL & ETL_Phase_NONE
        elif stage == 3450:
            stage_description = "Connecting to Hive"
        elif stage == 3451:
            stage_description = "Creating the import table in the staging database"
        elif stage == 3452:
            stage_description = "Get Import table rowcount"
        elif stage == 3453:
            stage_description = "Validate import table"
        elif stage == 3495:
            stage_description = "Invalidate Impala metadata"

        # Import_Phase_Mongo_FULL & ETL_Phase_TRUNCATEINSERT
        elif stage == 3500:
            stage_description = "Connecting to Hive"
        elif stage == 3501:
            stage_description = "Creating the import table in the staging database"
        elif stage == 3502:
            stage_description = "Get Import table rowcount"
        elif stage == 3503:
            stage_description = "Validate import table"
        elif stage == 3504:
            stage_description = "Removing Hive locks by force"
        elif stage == 3505:
            stage_description = "Creating the target table"
        elif stage == 3506:
            stage_description = "Truncate target table"
        elif stage == 3507:
            stage_description = "Copy rows from import to target table"
        elif stage == 3508:
            stage_description = "Update Hive statistics on target table"
        elif stage == 3509:
            stage_description = "Get Target table rowcount"
        elif stage == 3510:
            stage_description = "Validate target table"
        elif stage == 3545:
            stage_description = "Invalidate Impala metadata"

        # Import_Phase_ORACLE_FLASHBACK & ETL_Phase_MERGEHISTORYAUDIT
        elif stage == 3550:
            stage_description = "Connecting to Hive"
        elif stage == 3551:
            stage_description = "Creating the import table in the staging database"
        elif stage == 3552:
            stage_description = "Get Import table rowcount"
        elif stage == 3553:
            stage_description = "Validate import table"
        elif stage == 3554:
            stage_description = "Removing Hive locks by force"
        elif stage == 3555:
            stage_description = "Creating the target table"
        elif stage == 3556:
            stage_description = "Creating the history table"
        elif stage == 3557:
            stage_description = "Merge Import table with Target table"
        elif stage == 3558:
            stage_description = "Update Hive statistics on target table"
        elif stage == 3559:
            stage_description = "Get Target table rowcount"
        elif stage == 3560:
            stage_description = "Validate target table"
        elif stage == 3561:
            stage_description = "Saving pending incremental values"
        elif stage == 3562:
            stage_description = "Running major compaction on Target table"
        elif stage == 3595:
            stage_description = "Invalidate Impala metadata"

        # Import_Phase_MSSQL_CHANGE_TRACKING & ETL_Phase_MERGEONLY
        elif stage == 3600:
            stage_description = "Connecting to Hive"
        elif stage == 3601:
            stage_description = "Creating the import table in the staging database"
        elif stage == 3602:
            stage_description = "Get Import table rowcount"
        elif stage == 3603:
            stage_description = "Validate import table"
        elif stage == 3604:
            stage_description = "Removing Hive locks by force"
        elif stage == 3605:
            stage_description = "Creating the target table"
        elif stage == 3607:
            stage_description = "Merge Import table with Target table"
        elif stage == 3608:
            stage_description = "Update Hive statistics on target table"
        elif stage == 3609:
            stage_description = "Get Target table rowcount"
        elif stage == 3610:
            stage_description = "Validate target table"
        elif stage == 3611:
            stage_description = "Saving pending incremental values"
        elif stage == 3612:
            stage_description = "Running major compaction on Target table"
        elif stage == 3645:
            stage_description = "Invalidate Impala metadata"

        # Import_Phase_MSSQL_CHANGE_TRACKING & ETL_Phase_MERGEHISTORYAUDIT
        elif stage == 3650:
            stage_description = "Connecting to Hive"
        elif stage == 3651:
            stage_description = "Creating the import table in the staging database"
        elif stage == 3652:
            stage_description = "Get Import table rowcount"
        elif stage == 3653:
            stage_description = "Validate import table"
        elif stage == 3654:
            stage_description = "Removing Hive locks by force"
        elif stage == 3655:
            stage_description = "Creating the target table"
        elif stage == 3656:
            stage_description = "Creating the history table"
        elif stage == 3657:
            stage_description = "Creating the delete table"
        elif stage == 3658:
            stage_description = "Merge Import table with Target table"
        elif stage == 3659:
            stage_description = "Update Hive statistics on target table"
        elif stage == 3660:
            stage_description = "Get Target table rowcount"
        elif stage == 3661:
            stage_description = "Validate target table"
        elif stage == 3662:
            stage_description = "Saving pending incremental values"
        elif stage == 3663:
            stage_description = "Running major compaction on Target table"
        elif stage == 3695:
            stage_description = "Invalidate Impala metadata"

        elif stage == 9999:
            stage_description = "DBImport completed successfully"
        return stage_description

    def getStageShortName(self, stage):

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

        # Import_Phase_FULL
        elif stage == 1010:
            stage_short_name = "get_source_tableschema"
        elif stage == 1011:
            stage_short_name = "clear_table_rowcount"
        elif stage == 1012:
            stage_short_name = "get_source_rowcount"
        elif stage == 1013:
            stage_short_name = "sqoop"
        elif stage == 1014:
            stage_short_name = "spark"
        elif stage == 1015:
            stage_short_name = "validate_import_table"
        elif stage == 1016:
            stage_short_name = "spark_etl"
        elif stage == 1017:
            stage_short_name = "validate_target_table"
        elif stage == 1018:
            stage_short_name = "skip"
        elif stage == 1020:
            stage_short_name = "validate_sqoop_import"
        elif stage == 1045:
            stage_short_name = "atlas_schema"
        elif stage == 1049:
            stage_short_name = "skip"

        # Import_Phase_INCR
        elif stage == 1110:
            stage_short_name = "get_source_tableschema"
        elif stage == 1111:
            stage_short_name = "clear_table_rowcount"
        elif stage == 1112:
            stage_short_name = "sqoop"
        elif stage == 1113:
            stage_short_name = "spark"
        elif stage == 1120:
            stage_short_name = "get_source_rowcount"
        elif stage == 1121:
            stage_short_name = "validate_sqoop_import"
        elif stage == 1145:
            stage_short_name = "atlas_schema"
        elif stage == 1149:
            stage_short_name = "skip"

        # Import_Phase_ORACLE_FLASHBACK
        elif stage == 1210:
            stage_short_name = "get_source_tableschema"
        elif stage == 1211:
            stage_short_name = "clear_table_rowcount"
        elif stage == 1212:
            stage_short_name = "skip"
        elif stage == 1215:
            stage_short_name = "sqoop"
        elif stage == 1216:
            stage_short_name = "spark"
        elif stage == 1220:
            stage_short_name = "get_source_rowcount"
        elif stage == 1221:
            stage_short_name = "validate_sqoop_import"
        elif stage == 1245:
            stage_short_name = "atlas_schema"
        elif stage == 1249:
            stage_short_name = "skip"

        # Import_Phase_MSSQL_CHANGE_TRACKING
        elif stage == 1260:
            stage_short_name = "get_source_tableschema"
        elif stage == 1261:
            stage_short_name = "clear_table_rowcount"
        elif stage == 1262:
            stage_short_name = "skip"
        elif stage == 1264:
            stage_short_name = "sqoop"
        elif stage == 1265:
            stage_short_name = "spark"
        elif stage == 1270:
            stage_short_name = "get_source_rowcount"
        elif stage == 1271:
            stage_short_name = "validate_sqoop_import"
        elif stage == 1295:
            stage_short_name = "atlas_schema"
        elif stage == 1299:
            stage_short_name = "skip"

        # Import_Phase_MONGO_FULL
        elif stage == 1310:
            stage_short_name = "skip"
        elif stage == 1315:
            stage_short_name = "skip"
        elif stage == 1320:
            stage_short_name = "skip"
        elif stage == 1325:
            stage_short_name = "skip"
        elif stage == 1349:
            stage_short_name = "skip"

        # Copy Phase
        elif stage == 2000:
            stage_short_name = "skip"
        elif stage == 2001:
            stage_short_name = "copy_data"
        elif stage == 2002:
            stage_short_name = "copy_schema"
        elif stage == 2099:
            stage_short_name = "skip"

        # Import_Phase_FULL & ETL_Phase_TRUNCATEINSERT
        elif stage == 3050:
            stage_short_name = "connect_to_hive"
        elif stage == 3051:
            stage_short_name = "create_import_table"
        elif stage == 3052:
            stage_short_name = "get_import_rowcount"
        elif stage == 3053:
            stage_short_name = "validate_import_table"
        elif stage == 3054:
            stage_short_name = "clear_hive_locks"
        elif stage == 3055:
            stage_short_name = "create_target_table"
        elif stage == 3056:
            stage_short_name = "truncate_target_table"
        elif stage == 3057:
            stage_short_name = "hive_import"
        elif stage == 3058:
            stage_short_name = "update_statistics"
        elif stage == 3059:
            stage_short_name = "get_target_rowcount"
        elif stage == 3060:
            stage_short_name = "validate_target_table"
        elif stage == 3095:
            stage_short_name = "skip"

        # Import_Phase_FULL & ETL_Phase_INSERT
        elif stage == 3100:
            stage_short_name = "connect_to_hive"
        elif stage == 3101:
            stage_short_name = "create_import_table"
        elif stage == 3102:
            stage_short_name = "get_import_rowcount"
        elif stage == 3103:
            stage_short_name = "validate_import_table"
        elif stage == 3104:
            stage_short_name = "clear_hive_locks"
        elif stage == 3105:
            stage_short_name = "create_target_table"
        elif stage == 3106:
            stage_short_name = "hive_import"
        elif stage == 3107:
            stage_short_name = "update_statistics"
        elif stage == 3108:
            stage_short_name = "get_target_rowcount"
        elif stage == 3109:
            stage_short_name = "validate_target_table"
        elif stage == 3145:
            stage_short_name = "skip"

        # Import_Phase_INCR & ETL_Phase_INSERT
        elif stage == 3150:
            stage_short_name = "connect_to_hive"
        elif stage == 3151:
            stage_short_name = "create_import_table"
        elif stage == 3152:
            stage_short_name = "get_import_rowcount"
        elif stage == 3153:
            stage_short_name = "validate_import_table"
        elif stage == 3154:
            stage_short_name = "clear_hive_locks"
        elif stage == 3155:
            stage_short_name = "create_target_table"
        elif stage == 3156:
            stage_short_name = "hive_import"
        elif stage == 3157:
            stage_short_name = "update_statistics"
        elif stage == 3158:
            stage_short_name = "get_target_rowcount"
        elif stage == 3159:
            stage_short_name = "validate_target_table"
        elif stage == 3160:
            stage_short_name = "skip"
        elif stage == 3195:
            stage_short_name = "skip"

        # Import_Phase_FULL & ETL_Phase_MERGEHISTORYAUDIT
        elif stage == 3200:
            stage_short_name = "connect_to_hive"
        elif stage == 3201:
            stage_short_name = "create_import_table"
        elif stage == 3202:
            stage_short_name = "get_import_rowcount"
        elif stage == 3203:
            stage_short_name = "validate_import_table"
        elif stage == 3204:
            stage_short_name = "clear_hive_locks"
        elif stage == 3205:
            stage_short_name = "create_target_table"
        elif stage == 3206:
            stage_short_name = "create_history_table"
        elif stage == 3207:
            stage_short_name = "create_delete_table"
        elif stage == 3208:
            stage_short_name = "merge_table"
        elif stage == 3209:
            stage_short_name = "update_statistics"
        elif stage == 3210:
            stage_short_name = "get_target_rowcount"
        elif stage == 3211:
            stage_short_name = "validate_target_table"
        elif stage == 3212:
            stage_short_name = "skip"
        elif stage == 3245:
            stage_short_name = "skip"

        # Import_Phase_FULL & ETL_Phase_MERGEONLY
        elif stage == 3250:
            stage_short_name = "connect_to_hive"
        elif stage == 3251:
            stage_short_name = "create_import_table"
        elif stage == 3252:
            stage_short_name = "get_import_rowcount"
        elif stage == 3253:
            stage_short_name = "validate_import_table"
        elif stage == 3254:
            stage_short_name = "clear_hive_locks"
        elif stage == 3255:
            stage_short_name = "create_target_table"
        elif stage == 3256:
            stage_short_name = "create_delete_table"
        elif stage == 3257:
            stage_short_name = "merge_table"
        elif stage == 3258:
            stage_short_name = "update_statistics"
        elif stage == 3259:
            stage_short_name = "get_target_rowcount"
        elif stage == 3260:
            stage_short_name = "validate_target_table"
        elif stage == 3261:
            stage_short_name = "skip"
        elif stage == 3295:
            stage_short_name = "skip"

        # Import_Phase_INCR & ETL_Phase_MERGEONLY
        elif stage == 3300:
            stage_short_name = "connect_to_hive"
        elif stage == 3301:
            stage_short_name = "create_import_table"
        elif stage == 3302:
            stage_short_name = "get_import_rowcount"
        elif stage == 3303:
            stage_short_name = "validate_import_table"
        elif stage == 3304:
            stage_short_name = "clear_hive_locks"
        elif stage == 3305:
            stage_short_name = "create_target_table"
        elif stage == 3306:
            stage_short_name = "merge_table"
        elif stage == 3307:
            stage_short_name = "update_statistics"
        elif stage == 3308:
            stage_short_name = "get_target_rowcount"
        elif stage == 3309:
            stage_short_name = "validate_target_table"
        elif stage == 3310:
            stage_short_name = "skip"
        elif stage == 3311:
            stage_short_name = "skip"
        elif stage == 3345:
            stage_short_name = "skip"

        # Import_Phase_INCR & ETL_Phase_MERGEHISTORYAUDIT
        elif stage == 3350:
            stage_short_name = "connect_to_hive"
        elif stage == 3351:
            stage_short_name = "create_import_table"
        elif stage == 3352:
            stage_short_name = "get_import_rowcount"
        elif stage == 3353:
            stage_short_name = "validate_import_table"
        elif stage == 3354:
            stage_short_name = "clear_hive_locks"
        elif stage == 3355:
            stage_short_name = "create_target_table"
        elif stage == 3356:
            stage_short_name = "create_history_table"
        elif stage == 3357:
            stage_short_name = "merge_table"
        elif stage == 3358:
            stage_short_name = "update_statistics"
        elif stage == 3359:
            stage_short_name = "get_target_rowcount"
        elif stage == 3360:
            stage_short_name = "validate_target_table"
        elif stage == 3361:
            stage_short_name = "skip"
        elif stage == 3362:
            stage_short_name = "skip"
        elif stage == 3395:
            stage_short_name = "skip"

        # Import_Phase_ORACLE_FLASHBACK & ETL_Phase_MERGEONLY
        elif stage == 3400:
            stage_short_name = "connect_to_hive"
        elif stage == 3401:
            stage_short_name = "create_import_table"
        elif stage == 3402:
            stage_short_name = "get_import_rowcount"
        elif stage == 3403:
            stage_short_name = "validate_import_table"
        elif stage == 3404:
            stage_short_name = "clear_hive_locks"
        elif stage == 3405:
            stage_short_name = "create_target_table"
        elif stage == 3406:
            stage_short_name = "merge_table"
        elif stage == 3407:
            stage_short_name = "update_statistics"
        elif stage == 3408:
            stage_short_name = "get_target_rowcount"
        elif stage == 3409:
            stage_short_name = "validate_target_table"
        elif stage == 3410:
            stage_short_name = "skip"
        elif stage == 3411:
            stage_short_name = "skip"
        elif stage == 3445:
            stage_short_name = "skip"

        # Import_Phase_FULL & ETL_Phase_TRUNCATEINSERT
        elif stage == 3450:
            stage_short_name = "connect_to_hive"
        elif stage == 3451:
            stage_short_name = "create_import_table"
        elif stage == 3452:
            stage_short_name = "get_import_rowcount"
        elif stage == 3453:
            stage_short_name = "validate_import_table"
        elif stage == 3495:
            stage_short_name = "skip"

        # Import_Phase_Mongo_FULL & ETL_Phase_TRUNCATEINSERT
        elif stage == 3500:
            stage_short_name = "connect_to_hive"
        elif stage == 3501:
            stage_short_name = "create_import_table"
        elif stage == 3502:
            stage_short_name = "get_import_rowcount"
        elif stage == 3503:
            stage_short_name = "validate_import_table"
        elif stage == 3504:
            stage_short_name = "clear_hive_locks"
        elif stage == 3505:
            stage_short_name = "create_target_table"
        elif stage == 3506:
            stage_short_name = "truncate_target_table"
        elif stage == 3507:
            stage_short_name = "hive_import"
        elif stage == 3508:
            stage_short_name = "update_statistics"
        elif stage == 3509:
            stage_short_name = "get_target_rowcount"
        elif stage == 3510:
            stage_short_name = "validate_target_table"
        elif stage == 3545:
            stage_short_name = "skip"

        # Import_Phase_ORACLE_FLASHBACK & ETL_Phase_MERGEHISTORYAUDIT
        elif stage == 3550:
            stage_short_name = "connect_to_hive"
        elif stage == 3551:
            stage_short_name = "create_import_table"
        elif stage == 3552:
            stage_short_name = "get_import_rowcount"
        elif stage == 3553:
            stage_short_name = "validate_import_table"
        elif stage == 3554:
            stage_short_name = "clear_hive_locks"
        elif stage == 3555:
            stage_short_name = "create_target_table"
        elif stage == 3556:
            stage_short_name = "create_history_table"
        elif stage == 3557:
            stage_short_name = "merge_table"
        elif stage == 3558:
            stage_short_name = "update_statistics"
        elif stage == 3559:
            stage_short_name = "get_target_rowcount"
        elif stage == 3560:
            stage_short_name = "validate_target_table"
        elif stage == 3561:
            stage_short_name = "skip"
        elif stage == 3562:
            stage_short_name = "skip"
        elif stage == 3595:
            stage_short_name = "skip"

        # Import_Phase_MSSQL_CHANGE_TRACKING & ETL_Phase_MERGEONLY
        elif stage == 3600:
            stage_short_name = "connect_to_hive"
        elif stage == 3601:
            stage_short_name = "create_import_table"
        elif stage == 3602:
            stage_short_name = "get_import_rowcount"
        elif stage == 3603:
            stage_short_name = "validate_import_table"
        elif stage == 3604:
            stage_short_name = "clear_hive_locks"
        elif stage == 3605:
            stage_short_name = "create_target_table"
        elif stage == 3607:
            stage_short_name = "merge_table"
        elif stage == 3608:
            stage_short_name = "update_statistics"
        elif stage == 3609:
            stage_short_name = "get_target_rowcount"
        elif stage == 3610:
            stage_short_name = "validate_target_table"
        elif stage == 3611:
            stage_short_name = "skip"
        elif stage == 3612:
            stage_short_name = "skip"
        elif stage == 3645:
            stage_short_name = "skip"

        # Import_Phase_MSSQL_CHANGE_TRACKING & ETL_Phase_MERGEHISTORYAUDIT
        elif stage == 3650:
            stage_short_name = "connect_to_hive"
        elif stage == 3651:
            stage_short_name = "create_import_table"
        elif stage == 3652:
            stage_short_name = "get_import_rowcount"
        elif stage == 3653:
            stage_short_name = "validate_import_table"
        elif stage == 3654:
            stage_short_name = "clear_hive_locks"
        elif stage == 3655:
            stage_short_name = "create_target_table"
        elif stage == 3656:
            stage_short_name = "create_history_table"
        elif stage == 3657:
            stage_short_name = "create_delete_table"
        elif stage == 3658:
            stage_short_name = "merge_table"
        elif stage == 3659:
            stage_short_name = "update_statistics"
        elif stage == 3660:
            stage_short_name = "get_target_rowcount"
        elif stage == 3661:
            stage_short_name = "validate_target_table"
        elif stage == 3662:
            stage_short_name = "skip"
        elif stage == 3663:
            stage_short_name = "skip"
        elif stage == 3695:
            stage_short_name = "skip"

        elif stage == 9999:
            stage_short_name = "skip"

        return stage_short_name

    def convertStageStatisticsToJSON(self, **kwargs):
        """Reads the import_stage_statistics and convert the information to a JSON document"""
        logging.debug("Executing stage.convertStageStatisticsToJSON()")

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
        # 		self.postDataToKafka = True
        # 		self.postDataToKafkaExtended = True

        if (
            self.postDataToREST == False
            and self.postDataToKafka == False
            and self.postDataToAWSSNS == False
        ):
            return

        import_stop = None
        json_data_kafka = {}
        json_data_kafka["type"] = "import"
        json_data_kafka["status"] = "finished"
        json_data_rest = json_data_kafka.copy()
        json_data_aws_sns = json_data_kafka.copy()

        for key, value in kwargs.items():
            json_data_kafka[key] = value
            json_data_rest[key] = value
            json_data_aws_sns[key] = value

        if self.postDataToKafkaExtended == False:
            json_data_kafka.pop("sessions")
            json_data_kafka.pop("copy_phase")

        if self.postDataToRESTextended == False:
            json_data_rest.pop("sessions")
            json_data_rest.pop("copy_phase")

        if self.postDataToAWSSNSExtended == False:
            json_data_aws_sns.pop("sessions")
            json_data_aws_sns.pop("copy_phase")

        query = "select stage, start, stop, duration from import_stage_statistics where hive_db = %s and hive_table = %s"
        self.mysql_cursor.execute(query, (self.Hive_DB, self.Hive_Table))
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor.statement))

        for row in self.mysql_cursor.fetchall():
            stage = row[0]
            stage_start = row[1]
            stage_stop = row[2]
            stage_duration = row[3]

            stage_short_name = self.getStageShortName(stage)

            if stage_short_name == "":
                logging.error(
                    "There is no stage description for the JSON data for stage %s"
                    % (stage)
                )
                logging.error("Will put the raw stage number into the JSON document")
                stage_short_name = str(stage)

            if stage_short_name != "skip" and self.postDataToRESTextended == True:
                json_data_rest["%s_start" % (stage_short_name)] = str(stage_start)
                json_data_rest["%s_stop" % (stage_short_name)] = str(stage_stop)
                json_data_rest["%s_duration" % (stage_short_name)] = stage_duration

            if stage_short_name != "skip" and self.postDataToKafkaExtended == True:
                json_data_kafka["%s_start" % (stage_short_name)] = str(stage_start)
                json_data_kafka["%s_stop" % (stage_short_name)] = str(stage_stop)
                json_data_kafka["%s_duration" % (stage_short_name)] = stage_duration

            if stage_short_name != "skip" and self.postDataToAWSSNSExtended == True:
                json_data_aws_sns["%s_start" % (stage_short_name)] = str(stage_start)
                json_data_aws_sns["%s_stop" % (stage_short_name)] = str(stage_stop)
                json_data_aws_sns["%s_duration" % (stage_short_name)] = stage_duration

            if stage == 0:
                import_start = stage_start

            if import_stop == None or stage_stop > import_stop:
                import_stop = stage_stop

        try:
            import_duration = int((import_stop - import_start).total_seconds())
        except UnboundLocalError:
            import_start = -1
            import_stop = -1
            import_duration = -1

        json_data_kafka["start"] = str(import_start)
        json_data_kafka["stop"] = str(import_stop)
        json_data_kafka["duration"] = import_duration

        json_data_rest["start"] = str(import_start)
        json_data_rest["stop"] = str(import_stop)
        json_data_rest["duration"] = import_duration

        json_data_aws_sns["start"] = str(import_start)
        json_data_aws_sns["stop"] = str(import_stop)
        json_data_aws_sns["duration"] = import_duration

        if self.postDataToKafka == True:
            result = self.sendStatistics.publishKafkaData(json.dumps(json_data_kafka))
            if result == False:
                logging.info("Kafka publish failed!")
                logging.info("Saving the JSON to the json_to_send table instead")
                self.common_config.saveJsonToDatabase(
                    "import_statistics", "kafka", json.dumps(json_data_kafka)
                )

        if self.postDataToREST == True:
            response = self.sendStatistics.sendRESTdata(json.dumps(json_data_rest))
            if response != 200:
                logging.info("REST call failed!")
                logging.info("Saving the JSON to the json_to_send table instead")
                self.common_config.saveJsonToDatabase(
                    "import_statistics", "rest", json.dumps(json_data_rest)
                )

        if self.postDataToAWSSNS == True:
            result = self.sendStatistics.sendAWSSNSdata(json.dumps(json_data_aws_sns))
            if result == False:
                logging.info("AWS SNS publish failed!")
                logging.info("Saving the JSON to the json_to_send table instead")
                self.common_config.saveJsonToDatabase(
                    "import_statistics", "rest", json.dumps(json_data_aws_sns)
                )

        logging.debug("Executing stage.convertStageStatisticsToJSON() - Finished")

    def saveStageStatistics(self, **kwargs):
        """Reads the import_stage_statistics and insert it to the import_statistics table"""
        logging.debug("Executing stage.saveStageStatistics()")

        columns_part = []
        values_part = []
        import_start = None
        import_stop = None
        import_duration = None

        for key, value in kwargs.items():
            columns_part.append(key)
            if value == True:
                value = 1
            if value == False:
                value = 0
            values_part.append(value)

        query = "select stage, start, stop, duration from import_stage_statistics where hive_db = %s and hive_table = %s"
        self.mysql_cursor.execute(query, (self.Hive_DB, self.Hive_Table))
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor.statement))

        duplicate_columns = False
        for row in self.mysql_cursor.fetchall():
            stage = row[0]
            stage_start = row[1]
            stage_stop = row[2]
            stage_duration = row[3]

            stage_short_name = self.getStageShortName(stage)

            if stage_short_name != "" and stage_short_name != "skip":
                if "%s_start" % (stage_short_name) in columns_part:
                    duplicate_columns = True
                columns_part.append("%s_start" % (stage_short_name))
                values_part.append(str(stage_start))

                columns_part.append("%s_stop" % (stage_short_name))
                values_part.append(str(stage_stop))

                columns_part.append("%s_duration" % (stage_short_name))
                values_part.append(str(stage_duration))

            if stage == 0:
                import_start = stage_start

            if import_stop == None or stage_stop > import_stop:
                import_stop = stage_stop

        try:
            import_duration = int((import_stop - import_start).total_seconds())
        except TypeError:
            logging.warning(
                "'import_duration' cant be calculated due to invalid format of start or stop time. It will be set to 0"
            )
            import_duration = 0

        columns_part.append("start")
        if import_start == None:
            values_part.append(import_start)
        else:
            values_part.append(str(import_start))

        columns_part.append("stop")
        if import_stop == None:
            values_part.append(import_stop)
        else:
            values_part.append(str(import_stop))

        columns_part.append("duration")
        values_part.append(str(import_duration))

        query = "insert into import_statistics "
        # 		insertQuery = "("
        # 		insertQuery += ", ".join(map(str, columnsPart))
        # 		insertQuery += ") values ("
        insert_query = "(`"
        insert_query += "`, `".join(map(str, columns_part))
        insert_query += "`) values ("

        str_insert = ""
        for i in values_part:
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

        if duplicate_columns == True:
            logging.warning(
                "There are duplicate column in the statistics. This usually happens when changing import_method on a ongoing import without reseting the stage in between. As a result, the statistics wont be saved for this import"
            )
        else:
            self.mysql_cursor.execute(query, values_part)
            self.mysql_conn.commit()
            logging.debug("SQL Statement executed: %s" % (self.mysql_cursor.statement))

        query = (
            "delete from import_statistics_last where hive_db = %s and hive_table = %s"
        )
        self.mysql_cursor.execute(query, (self.Hive_DB, self.Hive_Table))
        self.mysql_conn.commit()
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor.statement))

        if duplicate_columns == False:
            query = "insert into import_statistics_last "
            query += insert_query
            self.mysql_cursor.execute(query, values_part)
            self.mysql_conn.commit()
            logging.debug("SQL Statement executed: %s" % (self.mysql_cursor.statement))

        logging.debug("Executing stage.saveStageStatistics() - Finished")

    def setStageOnlyInMemory(self):
        self.memoryStage = True

    def clearStage(self):
        """Removes all stage information from the import_stage table"""
        logging.debug("Executing stage.clearStage()")

        if self.memoryStage == False:
            query = "delete from import_stage where hive_db = %s and hive_table = %s"
            self.mysql_cursor.execute(query, (self.Hive_DB, self.Hive_Table))
            self.mysql_conn.commit()
            logging.debug("SQL Statement executed: %s" % (self.mysql_cursor.statement))

            query = "delete from import_stage_statistics where hive_db = %s and hive_table = %s"
            self.mysql_cursor.execute(query, (self.Hive_DB, self.Hive_Table))
            self.mysql_conn.commit()
            logging.debug("SQL Statement executed: %s" % (self.mysql_cursor.statement))

        logging.debug("Executing stage.clearStage() - Finished")

    def getStage(self):
        """Fetches the stage from import_stage table and return stage value. If no stage, 0 is returned"""
        logging.debug("Executing stage.getStage()")

        if self.currentStage == None:
            if self.memoryStage == True:
                self.currentStage = 0
            else:
                query = "select stage from import_stage where hive_db = %s and hive_table = %s"
                self.mysql_cursor.execute(query, (self.Hive_DB, self.Hive_Table))
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

    def getStageStartStop(self, stage):
        """Returns the start and stop time for a specific stage. If no info can be found, None is returned"""
        logging.debug("Executing stage.getStageStartStop()")

        query = "select stage, start, stop, duration from import_stage_statistics where hive_db = %s and hive_table = %s "
        self.mysql_cursor.execute(query, (self.Hive_DB, self.Hive_Table))
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor.statement))

        return_dict = {}
        return_dict["startTime"] = "1970-01-01T00:00:00.000000Z"
        return_dict["stopTime"] = "1970-01-01T00:00:00.000000Z"

        for row in self.mysql_cursor.fetchall():
            stage_id = row[0]
            stage_start = row[1]
            stage_stop = row[2]
            stage_duration = row[3]

            stage_short_name = self.getStageShortName(stage_id)
            if stage_short_name == stage:
                return_dict["startTime"] = stage_start.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                return_dict["stopTime"] = stage_stop.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                break

        return return_dict

        logging.debug("Executing stage.getStageStartStop() - Finished")

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

        # Calculate time and save to import_stage_statistics
        self.stage_duration_stop = time.monotonic()
        self.stageTimeStop = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        self.stageDurationStop = time.monotonic()
        self.stageDurationTime = self.stageDurationStop - self.stageDurationStart

        logging.debug("Setting stage to %s" % (newStage))
        logging.debug("stageTimeStart: %s" % (self.stageTimeStart))
        logging.debug("stageTimeStop:  %s" % (self.stageTimeStop))
        logging.debug("stageDurationStart: %s" % (self.stageDurationStart))
        logging.debug("stageDurationStop:  %s" % (self.stageDurationStop))
        logging.debug("stageDurationTime:  %s" % (self.stageDurationTime))

        query = "select count(stage) from import_stage_statistics where hive_db = %s and hive_table = %s and stage = %s"
        self.mysql_cursor.execute(
            query, (self.Hive_DB, self.Hive_Table, self.currentStage)
        )
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor.statement))

        row = self.mysql_cursor.fetchone()
        row_count = row[0]
        if row_count == 0:
            query = "insert into import_stage_statistics "
            query += "( hive_db, hive_table, stage, start, stop, duration )"
            query += "values "
            query += "( "
            query += "	'%s', " % (self.Hive_DB)
            query += "	'%s', " % (self.Hive_Table)
            query += "	%s, " % (self.currentStage)
            query += "	'%s', " % (self.stageTimeStart)
            query += "	'%s', " % (self.stageTimeStop)
            query += "	%s " % (round(self.stageDurationTime))
            query += ") "
        else:
            query = "update import_stage_statistics set"
            query += "	start = '%s', " % (self.stageTimeStart)
            query += "	stop = '%s', " % (self.stageTimeStop)
            query += "	duration = %s " % (round(self.stageDurationTime))
            query += "where "
            query += "	hive_db = '%s' " % (self.Hive_DB)
            query += "	and hive_table = '%s' " % (self.Hive_Table)
            query += "	and stage = '%s' " % (self.currentStage)

        self.mysql_cursor.execute(query)
        self.mysql_conn.commit()
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor.statement))

        stage_description = self.getStageDescription(newStage)

        # Save stage information in import_stage
        query = "select count(stage) from import_stage where hive_db = %s and hive_table = %s"
        self.mysql_cursor.execute(query, (self.Hive_DB, self.Hive_Table))
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor.statement))

        row = self.mysql_cursor.fetchone()
        row_count = row[0]
        if row_count == 0:
            query = "insert into import_stage "
            query += "( "
            query += "	hive_db, "
            query += "	hive_table, "
            query += "	stage, "
            query += "	stage_description, "
            query += "	stage_time "
            # 			query += "	unrecoverable_error "
            query += ") "
            query += "values "
            query += "( "
            query += "	'%s', " % (self.Hive_DB)
            query += "	'%s', " % (self.Hive_Table)
            query += "	%s, " % (newStage)
            query += "	'%s', " % (stage_description)
            query += "	'%s' " % (datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"))
            # 			query += "	0 "
            query += ") "
        else:
            query = "update import_stage set"
            query += "	stage = %s, " % (newStage)
            query += "	stage_description = '%s', " % (stage_description)
            query += "	stage_time = '%s' " % (
                datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
            )
            query += "where "
            query += "	hive_db = '%s' " % (self.Hive_DB)
            query += "	and hive_table = '%s' " % (self.Hive_Table)

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
        """Saves the retry attempt in the import_retries_log table"""
        logging.debug("Executing stage.saveRetryAttempt()")

        if self.memoryStage == True:
            return

        stage_description = self.getStageDescription(stage)

        query = "insert into import_retries_log "
        query += "( "
        query += "	hive_db, "
        query += "	hive_table, "
        query += "	retry_time, "
        query += "	stage, "
        query += "	stage_description "
        query += ") "
        query += "values (%s, %s, %s, %s, %s)"

        self.mysql_cursor.execute(
            query,
            (
                self.Hive_DB,
                self.Hive_Table,
                datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
                stage,
                stage_description,
            ),
        )
        self.mysql_conn.commit()
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor.statement))

        logging.debug("Executing stage.saveRetryAttempt() - Finished")
