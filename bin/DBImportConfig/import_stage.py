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


class stage(object):
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
        self.stageDurationStart = float()
        self.stageDurationStop = float()
        self.stageDurationTime = float()

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
        stageDescription = ""

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
            stageDescription = "Getting source tableschema"
        elif stage == 1011:
            stageDescription = "Clear table rowcount"
        elif stage == 1012:
            stageDescription = "Get source table rowcount"
        elif stage == 1013:
            stageDescription = "Sqoop import"
        elif stage == 1014:
            stageDescription = "Spark import"
        elif stage == 1015:
            stageDescription = "Validate spark import table"
        elif stage == 1016:
            stageDescription = "Spark ETL"
        elif stage == 1017:
            stageDescription = "Validate spark target table"
        elif stage == 1018:
            stageDescription = "Spark ETL finished"
        elif stage == 1020:
            stageDescription = "Validate import"
        elif stage == 1045:
            stageDescription = "Update Atlas"
        elif stage == 1049:
            stageDescription = "Import Phase Completed"

        # Import_Phase_INCR
        elif stage == 1110:
            stageDescription = "Getting source tableschema"
        elif stage == 1111:
            stageDescription = "Clear table rowcount"
        elif stage == 1112:
            stageDescription = "Sqoop import"
        elif stage == 1113:
            stageDescription = "Spark import"
        elif stage == 1120:
            stageDescription = "Get source table rowcount"
        elif stage == 1121:
            stageDescription = "Validate import"
        elif stage == 1145:
            stageDescription = "Update Atlas"
        elif stage == 1149:
            stageDescription = "Import Phase Completed"

        # Import_Phase_ORACLE_FLASHBACK
        elif stage == 1210:
            stageDescription = "Getting source tableschema"
        elif stage == 1211:
            stageDescription = "Clear table rowcount"
        elif stage == 1212:
            stageDescription = "Checking SCN number"
        elif stage == 1215:
            stageDescription = "Sqoop import"
        elif stage == 1216:
            stageDescription = "Spark import"
        elif stage == 1220:
            stageDescription = "Get source table rowcount"
        elif stage == 1221:
            stageDescription = "Validate sqoop import"
        elif stage == 1245:
            stageDescription = "Update Atlas"
        elif stage == 1249:
            stageDescription = "Import Phase Completed"

        # Import_Phase_MSSQL_CHANGE_TRACKING
        elif stage == 1260:
            stageDescription = "Getting source tableschema"
        elif stage == 1261:
            stageDescription = "Clear table rowcount"
        elif stage == 1262:
            stageDescription = "Checking MSSQL Change Tracking functions"
        elif stage == 1264:
            stageDescription = "Sqoop import"
        elif stage == 1265:
            stageDescription = "Spark import"
        elif stage == 1270:
            stageDescription = "Get source table rowcount"
        elif stage == 1271:
            stageDescription = "Validate sqoop import"
        elif stage == 1295:
            stageDescription = "Update Atlas"
        elif stage == 1299:
            stageDescription = "Import Phase Completed"

        # Import_Phase_MONGO_FULL
        elif stage == 1310:
            stageDescription = "Clear table rowcount"
        elif stage == 1315:
            stageDescription = "Spark import"
        elif stage == 1320:
            stageDescription = "Get Mongo rowcount"
        elif stage == 1325:
            stageDescription = "Validate Spark rowcount"
        elif stage == 1349:
            stageDescription = "Import Phase Completed"

        # Copy Phase
        elif stage == 2000:
            stageDescription = "Validating schema version on remote DBImport instance"
        elif stage == 2001:
            stageDescription = "Copy data to remote DBImport instance"
        elif stage == 2002:
            stageDescription = "Copy schema to remote DBImport instance"
        elif stage == 2099:
            stageDescription = "Copy Phase Completed"

        # Import_Phase_FULL & ETL_Phase_TRUNCATEINSERT
        elif stage == 3050:
            stageDescription = "Connecting to Hive"
        elif stage == 3051:
            stageDescription = "Creating the import table in the staging database"
        elif stage == 3052:
            stageDescription = "Get Import table rowcount"
        elif stage == 3053:
            stageDescription = "Validate import table"
        elif stage == 3054:
            stageDescription = "Removing Hive locks by force"
        elif stage == 3055:
            stageDescription = "Creating the target table"
        elif stage == 3056:
            stageDescription = "Truncate target table"
        elif stage == 3057:
            stageDescription = "Copy rows from import to target table"
        elif stage == 3058:
            stageDescription = "Update Hive statistics on target table"
        elif stage == 3059:
            stageDescription = "Get Target table rowcount"
        elif stage == 3060:
            stageDescription = "Validate target table"
        elif stage == 3095:
            stageDescription = "Invalidate Impala metadata"

        # Import_Phase_FULL & ETL_Phase_INSERT
        elif stage == 3100:
            stageDescription = "Connecting to Hive"
        elif stage == 3101:
            stageDescription = "Creating the import table in the staging database"
        elif stage == 3102:
            stageDescription = "Get Import table rowcount"
        elif stage == 3103:
            stageDescription = "Validate import table"
        elif stage == 3104:
            stageDescription = "Removing Hive locks by force"
        elif stage == 3105:
            stageDescription = "Creating the target table"
        elif stage == 3106:
            stageDescription = "Copy rows from import to target table"
        elif stage == 3107:
            stageDescription = "Update Hive statistics on target table"
        elif stage == 3108:
            stageDescription = "Get Target table rowcount"
        elif stage == 3109:
            stageDescription = "Validate target table"
        elif stage == 3145:
            stageDescription = "Invalidate Impala metadata"

        # Import_Phase_INCR & ETL_Phase_INSERT
        elif stage == 3150:
            stageDescription = "Connecting to Hive"
        elif stage == 3151:
            stageDescription = "Creating the import table in the staging database"
        elif stage == 3152:
            stageDescription = "Get Import table rowcount"
        elif stage == 3153:
            stageDescription = "Validate import table"
        elif stage == 3154:
            stageDescription = "Removing Hive locks by force"
        elif stage == 3155:
            stageDescription = "Creating the target table"
        elif stage == 3156:
            stageDescription = "Copy rows from import to target table"
        elif stage == 3157:
            stageDescription = "Update Hive statistics on target table"
        elif stage == 3158:
            stageDescription = "Get Target table rowcount"
        elif stage == 3159:
            stageDescription = "Validate target table"
        elif stage == 3160:
            stageDescription = "Saving pending incremental values"
        elif stage == 3195:
            stageDescription = "Invalidate Impala metadata"

        # Import_Phase_FULL & ETL_Phase_MERGEHISTORYAUDIT
        elif stage == 3200:
            stageDescription = "Connecting to Hive"
        elif stage == 3201:
            stageDescription = "Creating the import table in the staging database"
        elif stage == 3202:
            stageDescription = "Get Import table rowcount"
        elif stage == 3203:
            stageDescription = "Validate import table"
        elif stage == 3204:
            stageDescription = "Removing Hive locks by force"
        elif stage == 3205:
            stageDescription = "Creating the target table"
        elif stage == 3206:
            stageDescription = "Creating the history table"
        elif stage == 3207:
            stageDescription = "Creating the delete table"
        elif stage == 3208:
            stageDescription = "Merge Import table with Target table"
        elif stage == 3209:
            stageDescription = "Update Hive statistics on target table"
        elif stage == 3210:
            stageDescription = "Get Target table rowcount"
        elif stage == 3211:
            stageDescription = "Validate target table"
        elif stage == 3212:
            stageDescription = "Running major compaction on Target table"
        elif stage == 3245:
            stageDescription = "Invalidate Impala metadata"

        # Import_Phase_FULL & ETL_Phase_MERGEONLY
        elif stage == 3250:
            stageDescription = "Connecting to Hive"
        elif stage == 3251:
            stageDescription = "Creating the import table in the staging database"
        elif stage == 3252:
            stageDescription = "Get Import table rowcount"
        elif stage == 3253:
            stageDescription = "Validate import table"
        elif stage == 3254:
            stageDescription = "Removing Hive locks by force"
        elif stage == 3255:
            stageDescription = "Creating the target table"
        elif stage == 3256:
            stageDescription = "Creating the delete table"
        elif stage == 3257:
            stageDescription = "Merge Import table with Target table"
        elif stage == 3258:
            stageDescription = "Update Hive statistics on target table"
        elif stage == 3259:
            stageDescription = "Get Target table rowcount"
        elif stage == 3260:
            stageDescription = "Validate target table"
        elif stage == 3261:
            stageDescription = "Running major compaction on Target table"
        elif stage == 3295:
            stageDescription = "Invalidate Impala metadata"

        # Import_Phase_INCR & ETL_Phase_MERGEONLY
        elif stage == 3300:
            stageDescription = "Connecting to Hive"
        elif stage == 3301:
            stageDescription = "Creating the import table in the staging database"
        elif stage == 3302:
            stageDescription = "Get Import table rowcount"
        elif stage == 3303:
            stageDescription = "Validate import table"
        elif stage == 3304:
            stageDescription = "Removing Hive locks by force"
        elif stage == 3305:
            stageDescription = "Creating the target table"
        elif stage == 3306:
            stageDescription = "Merge Import table with Target table"
        elif stage == 3307:
            stageDescription = "Update Hive statistics on target table"
        elif stage == 3308:
            stageDescription = "Get Target table rowcount"
        elif stage == 3309:
            stageDescription = "Validate target table"
        elif stage == 3310:
            stageDescription = "Saving pending incremental values"
        elif stage == 3311:
            stageDescription = "Running major compaction on Target table"
        elif stage == 3345:
            stageDescription = "Invalidate Impala metadata"

        # Import_Phase_INCR & ETL_Phase_MERGEHISTORYAUDIT
        elif stage == 3350:
            stageDescription = "Connecting to Hive"
        elif stage == 3351:
            stageDescription = "Creating the import table in the staging database"
        elif stage == 3352:
            stageDescription = "Get Import table rowcount"
        elif stage == 3353:
            stageDescription = "Validate import table"
        elif stage == 3354:
            stageDescription = "Removing Hive locks by force"
        elif stage == 3355:
            stageDescription = "Creating the target table"
        elif stage == 3356:
            stageDescription = "Creating the history table"
        elif stage == 3357:
            stageDescription = "Merge Import table with Target table"
        elif stage == 3358:
            stageDescription = "Update Hive statistics on target table"
        elif stage == 3359:
            stageDescription = "Get Target table rowcount"
        elif stage == 3360:
            stageDescription = "Validate target table"
        elif stage == 3361:
            stageDescription = "Saving pending incremental values"
        elif stage == 3362:
            stageDescription = "Running major compaction on Target table"
        elif stage == 3395:
            stageDescription = "Invalidate Impala metadata"

        # Import_Phase_ORACLE_FLASHBACK & ETL_Phase_MERGEONLY
        elif stage == 3400:
            stageDescription = "Connecting to Hive"
        elif stage == 3401:
            stageDescription = "Creating the import table in the staging database"
        elif stage == 3402:
            stageDescription = "Get Import table rowcount"
        elif stage == 3403:
            stageDescription = "Validate import table"
        elif stage == 3404:
            stageDescription = "Removing Hive locks by force"
        elif stage == 3405:
            stageDescription = "Creating the target table"
        elif stage == 3406:
            stageDescription = "Merge Import table with Target table"
        elif stage == 3407:
            stageDescription = "Update Hive statistics on target table"
        elif stage == 3408:
            stageDescription = "Get Target table rowcount"
        elif stage == 3409:
            stageDescription = "Validate target table"
        elif stage == 3410:
            stageDescription = "Saving pending incremental values"
        elif stage == 3411:
            stageDescription = "Running major compaction on Target table"
        elif stage == 3445:
            stageDescription = "Invalidate Impala metadata"

        # Import_Phase_FULL & ETL_Phase_NONE
        elif stage == 3450:
            stageDescription = "Connecting to Hive"
        elif stage == 3451:
            stageDescription = "Creating the import table in the staging database"
        elif stage == 3452:
            stageDescription = "Get Import table rowcount"
        elif stage == 3453:
            stageDescription = "Validate import table"
        elif stage == 3495:
            stageDescription = "Invalidate Impala metadata"

        # Import_Phase_Mongo_FULL & ETL_Phase_TRUNCATEINSERT
        elif stage == 3500:
            stageDescription = "Connecting to Hive"
        elif stage == 3501:
            stageDescription = "Creating the import table in the staging database"
        elif stage == 3502:
            stageDescription = "Get Import table rowcount"
        elif stage == 3503:
            stageDescription = "Validate import table"
        elif stage == 3504:
            stageDescription = "Removing Hive locks by force"
        elif stage == 3505:
            stageDescription = "Creating the target table"
        elif stage == 3506:
            stageDescription = "Truncate target table"
        elif stage == 3507:
            stageDescription = "Copy rows from import to target table"
        elif stage == 3508:
            stageDescription = "Update Hive statistics on target table"
        elif stage == 3509:
            stageDescription = "Get Target table rowcount"
        elif stage == 3510:
            stageDescription = "Validate target table"
        elif stage == 3545:
            stageDescription = "Invalidate Impala metadata"

        # Import_Phase_ORACLE_FLASHBACK & ETL_Phase_MERGEHISTORYAUDIT
        elif stage == 3550:
            stageDescription = "Connecting to Hive"
        elif stage == 3551:
            stageDescription = "Creating the import table in the staging database"
        elif stage == 3552:
            stageDescription = "Get Import table rowcount"
        elif stage == 3553:
            stageDescription = "Validate import table"
        elif stage == 3554:
            stageDescription = "Removing Hive locks by force"
        elif stage == 3555:
            stageDescription = "Creating the target table"
        elif stage == 3556:
            stageDescription = "Creating the history table"
        elif stage == 3557:
            stageDescription = "Merge Import table with Target table"
        elif stage == 3558:
            stageDescription = "Update Hive statistics on target table"
        elif stage == 3559:
            stageDescription = "Get Target table rowcount"
        elif stage == 3560:
            stageDescription = "Validate target table"
        elif stage == 3561:
            stageDescription = "Saving pending incremental values"
        elif stage == 3562:
            stageDescription = "Running major compaction on Target table"
        elif stage == 3595:
            stageDescription = "Invalidate Impala metadata"

        # Import_Phase_MSSQL_CHANGE_TRACKING & ETL_Phase_MERGEONLY
        elif stage == 3600:
            stageDescription = "Connecting to Hive"
        elif stage == 3601:
            stageDescription = "Creating the import table in the staging database"
        elif stage == 3602:
            stageDescription = "Get Import table rowcount"
        elif stage == 3603:
            stageDescription = "Validate import table"
        elif stage == 3604:
            stageDescription = "Removing Hive locks by force"
        elif stage == 3605:
            stageDescription = "Creating the target table"
        elif stage == 3607:
            stageDescription = "Merge Import table with Target table"
        elif stage == 3608:
            stageDescription = "Update Hive statistics on target table"
        elif stage == 3609:
            stageDescription = "Get Target table rowcount"
        elif stage == 3610:
            stageDescription = "Validate target table"
        elif stage == 3611:
            stageDescription = "Saving pending incremental values"
        elif stage == 3612:
            stageDescription = "Running major compaction on Target table"
        elif stage == 3645:
            stageDescription = "Invalidate Impala metadata"

        # Import_Phase_MSSQL_CHANGE_TRACKING & ETL_Phase_MERGEHISTORYAUDIT
        elif stage == 3650:
            stageDescription = "Connecting to Hive"
        elif stage == 3651:
            stageDescription = "Creating the import table in the staging database"
        elif stage == 3652:
            stageDescription = "Get Import table rowcount"
        elif stage == 3653:
            stageDescription = "Validate import table"
        elif stage == 3654:
            stageDescription = "Removing Hive locks by force"
        elif stage == 3655:
            stageDescription = "Creating the target table"
        elif stage == 3656:
            stageDescription = "Creating the history table"
        elif stage == 3657:
            stageDescription = "Creating the delete table"
        elif stage == 3658:
            stageDescription = "Merge Import table with Target table"
        elif stage == 3659:
            stageDescription = "Update Hive statistics on target table"
        elif stage == 3660:
            stageDescription = "Get Target table rowcount"
        elif stage == 3661:
            stageDescription = "Validate target table"
        elif stage == 3662:
            stageDescription = "Saving pending incremental values"
        elif stage == 3663:
            stageDescription = "Running major compaction on Target table"
        elif stage == 3695:
            stageDescription = "Invalidate Impala metadata"

        elif stage == 9999:
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

        # Import_Phase_FULL
        elif stage == 1010:
            stageShortName = "get_source_tableschema"
        elif stage == 1011:
            stageShortName = "clear_table_rowcount"
        elif stage == 1012:
            stageShortName = "get_source_rowcount"
        elif stage == 1013:
            stageShortName = "sqoop"
        elif stage == 1014:
            stageShortName = "spark"
        elif stage == 1015:
            stageShortName = "validate_import_table"
        elif stage == 1016:
            stageShortName = "spark_etl"
        elif stage == 1017:
            stageShortName = "validate_target_table"
        elif stage == 1018:
            stageShortName = "skip"
        elif stage == 1020:
            stageShortName = "validate_sqoop_import"
        elif stage == 1045:
            stageShortName = "atlas_schema"
        elif stage == 1049:
            stageShortName = "skip"

        # Import_Phase_INCR
        elif stage == 1110:
            stageShortName = "get_source_tableschema"
        elif stage == 1111:
            stageShortName = "clear_table_rowcount"
        elif stage == 1112:
            stageShortName = "sqoop"
        elif stage == 1113:
            stageShortName = "spark"
        elif stage == 1120:
            stageShortName = "get_source_rowcount"
        elif stage == 1121:
            stageShortName = "validate_sqoop_import"
        elif stage == 1145:
            stageShortName = "atlas_schema"
        elif stage == 1149:
            stageShortName = "skip"

        # Import_Phase_ORACLE_FLASHBACK
        elif stage == 1210:
            stageShortName = "get_source_tableschema"
        elif stage == 1211:
            stageShortName = "clear_table_rowcount"
        elif stage == 1212:
            stageShortName = "skip"
        elif stage == 1215:
            stageShortName = "sqoop"
        elif stage == 1216:
            stageShortName = "spark"
        elif stage == 1220:
            stageShortName = "get_source_rowcount"
        elif stage == 1221:
            stageShortName = "validate_sqoop_import"
        elif stage == 1245:
            stageShortName = "atlas_schema"
        elif stage == 1249:
            stageShortName = "skip"

        # Import_Phase_MSSQL_CHANGE_TRACKING
        elif stage == 1260:
            stageShortName = "get_source_tableschema"
        elif stage == 1261:
            stageShortName = "clear_table_rowcount"
        elif stage == 1262:
            stageShortName = "skip"
        elif stage == 1264:
            stageShortName = "sqoop"
        elif stage == 1265:
            stageShortName = "spark"
        elif stage == 1270:
            stageShortName = "get_source_rowcount"
        elif stage == 1271:
            stageShortName = "validate_sqoop_import"
        elif stage == 1295:
            stageShortName = "atlas_schema"
        elif stage == 1299:
            stageShortName = "skip"

        # Import_Phase_MONGO_FULL
        elif stage == 1310:
            stageShortName = "skip"
        elif stage == 1315:
            stageShortName = "skip"
        elif stage == 1320:
            stageShortName = "skip"
        elif stage == 1325:
            stageShortName = "skip"
        elif stage == 1349:
            stageShortName = "skip"

        # Copy Phase
        elif stage == 2000:
            stageShortName = "skip"
        elif stage == 2001:
            stageShortName = "copy_data"
        elif stage == 2002:
            stageShortName = "copy_schema"
        elif stage == 2099:
            stageShortName = "skip"

        # Import_Phase_FULL & ETL_Phase_TRUNCATEINSERT
        elif stage == 3050:
            stageShortName = "connect_to_hive"
        elif stage == 3051:
            stageShortName = "create_import_table"
        elif stage == 3052:
            stageShortName = "get_import_rowcount"
        elif stage == 3053:
            stageShortName = "validate_import_table"
        elif stage == 3054:
            stageShortName = "clear_hive_locks"
        elif stage == 3055:
            stageShortName = "create_target_table"
        elif stage == 3056:
            stageShortName = "truncate_target_table"
        elif stage == 3057:
            stageShortName = "hive_import"
        elif stage == 3058:
            stageShortName = "update_statistics"
        elif stage == 3059:
            stageShortName = "get_target_rowcount"
        elif stage == 3060:
            stageShortName = "validate_target_table"
        elif stage == 3095:
            stageShortName = "skip"

        # Import_Phase_FULL & ETL_Phase_INSERT
        elif stage == 3100:
            stageShortName = "connect_to_hive"
        elif stage == 3101:
            stageShortName = "create_import_table"
        elif stage == 3102:
            stageShortName = "get_import_rowcount"
        elif stage == 3103:
            stageShortName = "validate_import_table"
        elif stage == 3104:
            stageShortName = "clear_hive_locks"
        elif stage == 3105:
            stageShortName = "create_target_table"
        elif stage == 3106:
            stageShortName = "hive_import"
        elif stage == 3107:
            stageShortName = "update_statistics"
        elif stage == 3108:
            stageShortName = "get_target_rowcount"
        elif stage == 3109:
            stageShortName = "validate_target_table"
        elif stage == 3145:
            stageShortName = "skip"

        # Import_Phase_INCR & ETL_Phase_INSERT
        elif stage == 3150:
            stageShortName = "connect_to_hive"
        elif stage == 3151:
            stageShortName = "create_import_table"
        elif stage == 3152:
            stageShortName = "get_import_rowcount"
        elif stage == 3153:
            stageShortName = "validate_import_table"
        elif stage == 3154:
            stageShortName = "clear_hive_locks"
        elif stage == 3155:
            stageShortName = "create_target_table"
        elif stage == 3156:
            stageShortName = "hive_import"
        elif stage == 3157:
            stageShortName = "update_statistics"
        elif stage == 3158:
            stageShortName = "get_target_rowcount"
        elif stage == 3159:
            stageShortName = "validate_target_table"
        elif stage == 3160:
            stageShortName = "skip"
        elif stage == 3195:
            stageShortName = "skip"

        # Import_Phase_FULL & ETL_Phase_MERGEHISTORYAUDIT
        elif stage == 3200:
            stageShortName = "connect_to_hive"
        elif stage == 3201:
            stageShortName = "create_import_table"
        elif stage == 3202:
            stageShortName = "get_import_rowcount"
        elif stage == 3203:
            stageShortName = "validate_import_table"
        elif stage == 3204:
            stageShortName = "clear_hive_locks"
        elif stage == 3205:
            stageShortName = "create_target_table"
        elif stage == 3206:
            stageShortName = "create_history_table"
        elif stage == 3207:
            stageShortName = "create_delete_table"
        elif stage == 3208:
            stageShortName = "merge_table"
        elif stage == 3209:
            stageShortName = "update_statistics"
        elif stage == 3210:
            stageShortName = "get_target_rowcount"
        elif stage == 3211:
            stageShortName = "validate_target_table"
        elif stage == 3212:
            stageShortName = "skip"
        elif stage == 3245:
            stageShortName = "skip"

        # Import_Phase_FULL & ETL_Phase_MERGEONLY
        elif stage == 3250:
            stageShortName = "connect_to_hive"
        elif stage == 3251:
            stageShortName = "create_import_table"
        elif stage == 3252:
            stageShortName = "get_import_rowcount"
        elif stage == 3253:
            stageShortName = "validate_import_table"
        elif stage == 3254:
            stageShortName = "clear_hive_locks"
        elif stage == 3255:
            stageShortName = "create_target_table"
        elif stage == 3256:
            stageShortName = "create_delete_table"
        elif stage == 3257:
            stageShortName = "merge_table"
        elif stage == 3258:
            stageShortName = "update_statistics"
        elif stage == 3259:
            stageShortName = "get_target_rowcount"
        elif stage == 3260:
            stageShortName = "validate_target_table"
        elif stage == 3261:
            stageShortName = "skip"
        elif stage == 3295:
            stageShortName = "skip"

        # Import_Phase_INCR & ETL_Phase_MERGEONLY
        elif stage == 3300:
            stageShortName = "connect_to_hive"
        elif stage == 3301:
            stageShortName = "create_import_table"
        elif stage == 3302:
            stageShortName = "get_import_rowcount"
        elif stage == 3303:
            stageShortName = "validate_import_table"
        elif stage == 3304:
            stageShortName = "clear_hive_locks"
        elif stage == 3305:
            stageShortName = "create_target_table"
        elif stage == 3306:
            stageShortName = "merge_table"
        elif stage == 3307:
            stageShortName = "update_statistics"
        elif stage == 3308:
            stageShortName = "get_target_rowcount"
        elif stage == 3309:
            stageShortName = "validate_target_table"
        elif stage == 3310:
            stageShortName = "skip"
        elif stage == 3311:
            stageShortName = "skip"
        elif stage == 3345:
            stageShortName = "skip"

        # Import_Phase_INCR & ETL_Phase_MERGEHISTORYAUDIT
        elif stage == 3350:
            stageShortName = "connect_to_hive"
        elif stage == 3351:
            stageShortName = "create_import_table"
        elif stage == 3352:
            stageShortName = "get_import_rowcount"
        elif stage == 3353:
            stageShortName = "validate_import_table"
        elif stage == 3354:
            stageShortName = "clear_hive_locks"
        elif stage == 3355:
            stageShortName = "create_target_table"
        elif stage == 3356:
            stageShortName = "create_history_table"
        elif stage == 3357:
            stageShortName = "merge_table"
        elif stage == 3358:
            stageShortName = "update_statistics"
        elif stage == 3359:
            stageShortName = "get_target_rowcount"
        elif stage == 3360:
            stageShortName = "validate_target_table"
        elif stage == 3361:
            stageShortName = "skip"
        elif stage == 3362:
            stageShortName = "skip"
        elif stage == 3395:
            stageShortName = "skip"

        # Import_Phase_ORACLE_FLASHBACK & ETL_Phase_MERGEONLY
        elif stage == 3400:
            stageShortName = "connect_to_hive"
        elif stage == 3401:
            stageShortName = "create_import_table"
        elif stage == 3402:
            stageShortName = "get_import_rowcount"
        elif stage == 3403:
            stageShortName = "validate_import_table"
        elif stage == 3404:
            stageShortName = "clear_hive_locks"
        elif stage == 3405:
            stageShortName = "create_target_table"
        elif stage == 3406:
            stageShortName = "merge_table"
        elif stage == 3407:
            stageShortName = "update_statistics"
        elif stage == 3408:
            stageShortName = "get_target_rowcount"
        elif stage == 3409:
            stageShortName = "validate_target_table"
        elif stage == 3410:
            stageShortName = "skip"
        elif stage == 3411:
            stageShortName = "skip"
        elif stage == 3445:
            stageShortName = "skip"

        # Import_Phase_FULL & ETL_Phase_TRUNCATEINSERT
        elif stage == 3450:
            stageShortName = "connect_to_hive"
        elif stage == 3451:
            stageShortName = "create_import_table"
        elif stage == 3452:
            stageShortName = "get_import_rowcount"
        elif stage == 3453:
            stageShortName = "validate_import_table"
        elif stage == 3495:
            stageShortName = "skip"

        # Import_Phase_Mongo_FULL & ETL_Phase_TRUNCATEINSERT
        elif stage == 3500:
            stageShortName = "connect_to_hive"
        elif stage == 3501:
            stageShortName = "create_import_table"
        elif stage == 3502:
            stageShortName = "get_import_rowcount"
        elif stage == 3503:
            stageShortName = "validate_import_table"
        elif stage == 3504:
            stageShortName = "clear_hive_locks"
        elif stage == 3505:
            stageShortName = "create_target_table"
        elif stage == 3506:
            stageShortName = "truncate_target_table"
        elif stage == 3507:
            stageShortName = "hive_import"
        elif stage == 3508:
            stageShortName = "update_statistics"
        elif stage == 3509:
            stageShortName = "get_target_rowcount"
        elif stage == 3510:
            stageShortName = "validate_target_table"
        elif stage == 3545:
            stageShortName = "skip"

        # Import_Phase_ORACLE_FLASHBACK & ETL_Phase_MERGEHISTORYAUDIT
        elif stage == 3550:
            stageShortName = "connect_to_hive"
        elif stage == 3551:
            stageShortName = "create_import_table"
        elif stage == 3552:
            stageShortName = "get_import_rowcount"
        elif stage == 3553:
            stageShortName = "validate_import_table"
        elif stage == 3554:
            stageShortName = "clear_hive_locks"
        elif stage == 3555:
            stageShortName = "create_target_table"
        elif stage == 3556:
            stageShortName = "create_history_table"
        elif stage == 3557:
            stageShortName = "merge_table"
        elif stage == 3558:
            stageShortName = "update_statistics"
        elif stage == 3559:
            stageShortName = "get_target_rowcount"
        elif stage == 3560:
            stageShortName = "validate_target_table"
        elif stage == 3561:
            stageShortName = "skip"
        elif stage == 3562:
            stageShortName = "skip"
        elif stage == 3595:
            stageShortName = "skip"

        # Import_Phase_MSSQL_CHANGE_TRACKING & ETL_Phase_MERGEONLY
        elif stage == 3600:
            stageShortName = "connect_to_hive"
        elif stage == 3601:
            stageShortName = "create_import_table"
        elif stage == 3602:
            stageShortName = "get_import_rowcount"
        elif stage == 3603:
            stageShortName = "validate_import_table"
        elif stage == 3604:
            stageShortName = "clear_hive_locks"
        elif stage == 3605:
            stageShortName = "create_target_table"
        elif stage == 3607:
            stageShortName = "merge_table"
        elif stage == 3608:
            stageShortName = "update_statistics"
        elif stage == 3609:
            stageShortName = "get_target_rowcount"
        elif stage == 3610:
            stageShortName = "validate_target_table"
        elif stage == 3611:
            stageShortName = "skip"
        elif stage == 3612:
            stageShortName = "skip"
        elif stage == 3645:
            stageShortName = "skip"

        # Import_Phase_MSSQL_CHANGE_TRACKING & ETL_Phase_MERGEHISTORYAUDIT
        elif stage == 3650:
            stageShortName = "connect_to_hive"
        elif stage == 3651:
            stageShortName = "create_import_table"
        elif stage == 3652:
            stageShortName = "get_import_rowcount"
        elif stage == 3653:
            stageShortName = "validate_import_table"
        elif stage == 3654:
            stageShortName = "clear_hive_locks"
        elif stage == 3655:
            stageShortName = "create_target_table"
        elif stage == 3656:
            stageShortName = "create_history_table"
        elif stage == 3657:
            stageShortName = "create_delete_table"
        elif stage == 3658:
            stageShortName = "merge_table"
        elif stage == 3659:
            stageShortName = "update_statistics"
        elif stage == 3660:
            stageShortName = "get_target_rowcount"
        elif stage == 3661:
            stageShortName = "validate_target_table"
        elif stage == 3662:
            stageShortName = "skip"
        elif stage == 3663:
            stageShortName = "skip"
        elif stage == 3695:
            stageShortName = "skip"

        elif stage == 9999:
            stageShortName = "skip"

        return stageShortName

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
        jsonDataKafka = {}
        jsonDataKafka["type"] = "import"
        jsonDataKafka["status"] = "finished"
        jsonDataREST = jsonDataKafka.copy()
        jsonDataAWSSNS = jsonDataKafka.copy()

        for key, value in kwargs.items():
            jsonDataKafka[key] = value
            jsonDataREST[key] = value
            jsonDataAWSSNS[key] = value

        if self.postDataToKafkaExtended == False:
            jsonDataKafka.pop("sessions")
            jsonDataKafka.pop("copy_phase")

        if self.postDataToRESTextended == False:
            jsonDataREST.pop("sessions")
            jsonDataREST.pop("copy_phase")

        if self.postDataToAWSSNSExtended == False:
            jsonDataAWSSNS.pop("sessions")
            jsonDataAWSSNS.pop("copy_phase")

        query = "select stage, start, stop, duration from import_stage_statistics where hive_db = %s and hive_table = %s"
        self.mysql_cursor.execute(query, (self.Hive_DB, self.Hive_Table))
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
                import_start = stage_start

            if import_stop == None or stage_stop > import_stop:
                import_stop = stage_stop

        try:
            import_duration = int((import_stop - import_start).total_seconds())
        except UnboundLocalError:
            import_start = -1
            import_stop = -1
            import_duration = -1

        jsonDataKafka["start"] = str(import_start)
        jsonDataKafka["stop"] = str(import_stop)
        jsonDataKafka["duration"] = import_duration

        jsonDataREST["start"] = str(import_start)
        jsonDataREST["stop"] = str(import_stop)
        jsonDataREST["duration"] = import_duration

        jsonDataAWSSNS["start"] = str(import_start)
        jsonDataAWSSNS["stop"] = str(import_stop)
        jsonDataAWSSNS["duration"] = import_duration

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
                    "import_statistics", "rest", json.dumps(jsonDataAWSSNS)
                )

        logging.debug("Executing stage.convertStageStatisticsToJSON() - Finished")

    def saveStageStatistics(self, **kwargs):
        """Reads the import_stage_statistics and insert it to the import_statistics table"""
        logging.debug("Executing stage.saveStageStatistics()")

        columnsPart = []
        valuesPart = []
        import_start = None
        import_stop = None
        import_duration = None

        for key, value in kwargs.items():
            columnsPart.append(key)
            if value == True:
                value = 1
            if value == False:
                value = 0
            valuesPart.append(value)

        query = "select stage, start, stop, duration from import_stage_statistics where hive_db = %s and hive_table = %s"
        self.mysql_cursor.execute(query, (self.Hive_DB, self.Hive_Table))
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor.statement))

        duplicateColumns = False
        for row in self.mysql_cursor.fetchall():
            stage = row[0]
            stage_start = row[1]
            stage_stop = row[2]
            stage_duration = row[3]

            stageShortName = self.getStageShortName(stage)

            if stageShortName != "" and stageShortName != "skip":
                if "%s_start" % (stageShortName) in columnsPart:
                    duplicateColumns = True
                columnsPart.append("%s_start" % (stageShortName))
                valuesPart.append(str(stage_start))

                columnsPart.append("%s_stop" % (stageShortName))
                valuesPart.append(str(stage_stop))

                columnsPart.append("%s_duration" % (stageShortName))
                valuesPart.append(str(stage_duration))

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

        columnsPart.append("start")
        if import_start == None:
            valuesPart.append(import_start)
        else:
            valuesPart.append(str(import_start))

        columnsPart.append("stop")
        if import_stop == None:
            valuesPart.append(import_stop)
        else:
            valuesPart.append(str(import_stop))

        columnsPart.append("duration")
        valuesPart.append(str(import_duration))

        query = "insert into import_statistics "
        # 		insertQuery = "("
        # 		insertQuery += ", ".join(map(str, columnsPart))
        # 		insertQuery += ") values ("
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

        if duplicateColumns == True:
            logging.warning(
                "There are duplicate column in the statistics. This usually happens when changing import_method on a ongoing import without reseting the stage in between. As a result, the statistics wont be saved for this import"
            )
        else:
            self.mysql_cursor.execute(query, valuesPart)
            self.mysql_conn.commit()
            logging.debug("SQL Statement executed: %s" % (self.mysql_cursor.statement))

        query = (
            "delete from import_statistics_last where hive_db = %s and hive_table = %s"
        )
        self.mysql_cursor.execute(query, (self.Hive_DB, self.Hive_Table))
        self.mysql_conn.commit()
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor.statement))

        if duplicateColumns == False:
            query = "insert into import_statistics_last "
            query += insertQuery
            self.mysql_cursor.execute(query, valuesPart)
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
        self.stageDurationStop = float()
        self.stageDurationTime = float()

        logging.debug("Executing stage.getStage() - Finished")
        return self.currentStage

    def getStageStartStop(self, stage):
        """Returns the start and stop time for a specific stage. If no info can be found, None is returned"""
        logging.debug("Executing stage.getStageStartStop()")

        query = "select stage, start, stop, duration from import_stage_statistics where hive_db = %s and hive_table = %s "
        self.mysql_cursor.execute(query, (self.Hive_DB, self.Hive_Table))
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
        rowCount = row[0]
        if rowCount == 0:
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

        stageDescription = self.getStageDescription(newStage)

        # Save stage information in import_stage
        query = "select count(stage) from import_stage where hive_db = %s and hive_table = %s"
        self.mysql_cursor.execute(query, (self.Hive_DB, self.Hive_Table))
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor.statement))

        row = self.mysql_cursor.fetchone()
        rowCount = row[0]
        if rowCount == 0:
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
            query += "	'%s', " % (stageDescription)
            query += "	'%s' " % (datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"))
            # 			query += "	0 "
            query += ") "
        else:
            query = "update import_stage set"
            query += "	stage = %s, " % (newStage)
            query += "	stage_description = '%s', " % (stageDescription)
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
        self.stageDurationStop = float()
        self.stageDurationTime = float()

        logging.debug("Executing stage.setStage() - Finished")

    def saveRetryAttempt(self, stage):
        """Saves the retry attempt in the import_retries_log table"""
        logging.debug("Executing stage.saveRetryAttempt()")

        if self.memoryStage == True:
            return

        stageDescription = self.getStageDescription(stage)

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
                stageDescription,
            ),
        )
        self.mysql_conn.commit()
        logging.debug("SQL Statement executed: %s" % (self.mysql_cursor.statement))

        logging.debug("Executing stage.saveRetryAttempt() - Finished")
