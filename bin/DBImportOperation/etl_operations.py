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
import sys
from datetime import datetime

from common import constants as constant
from common.Singleton import Singleton
from DBImportConfig import import_config
from DBImportOperation import common_operations, import_operations


class operation(metaclass=Singleton):
    def __init__(self, Hive_DB=None, Hive_Table=None):
        logging.debug("Executing etl_operations.__init__()")
        self.Hive_DB = None
        self.Hive_Table = None
        self.mysql_conn = None
        self.mysql_cursor = None
        self.startDate = None

        self.common_operations = common_operations.operation(Hive_DB, Hive_Table)
        self.import_operations = import_operations.operation(Hive_DB, Hive_Table)
        self.import_config = import_config.config(Hive_DB, Hive_Table)

        if Hive_DB != None and Hive_Table != None:
            self.setHiveTable(Hive_DB, Hive_Table)
        else:
            # If the class already is initialized, we just pull the parameters and set them here
            self.Hive_DB = self.common_operations.Hive_DB
            self.Hive_Table = self.common_operations.Hive_Table
            self.startDate = self.import_config.startDate

        logging.debug("Executing etl_operations.__init__() - Finished")

    def setHiveTable(self, Hive_DB, Hive_Table):
        """Sets the parameters to work against a new Hive database and table"""
        self.Hive_DB = Hive_DB.lower()
        self.Hive_Table = Hive_Table.lower()

        self.common_operations.setHiveTable(self.Hive_DB, self.Hive_Table)
        self.import_config.setHiveTable(self.Hive_DB, self.Hive_Table)

        try:
            self.import_config.getImportConfig()
            self.startDate = self.import_config.startDate
            self.import_config.lookupConnectionAlias()
        except:
            self.import_config.remove_temporary_files()
            sys.exit(1)

    def remove_temporary_files(self):
        self.import_config.remove_temporary_files()

    def connectToHive(
        self,
    ):
        logging.debug("Executing etl_operations.connectToHive()")

        try:
            self.common_operations.connectToHive()
        except Exception as ex:
            logging.error(ex)
            self.import_config.remove_temporary_files()
            sys.exit(1)

        logging.debug("Executing etl_operations.connectToHive() - Finished")

    def executeSQLQuery(self, query):
        if self.import_config.etlEngine == constant.ETL_ENGINE_HIVE:
            self.common_operations.executeHiveQuery(query)

        if self.import_config.etlEngine == constant.ETL_ENGINE_SPARK:
            logging.info(query)
            self.import_operations.spark.sql(query)

    def mergeHiveTables(
        self,
        sourceDB,
        sourceTable,
        targetDB,
        targetTable,
        historyDB=None,
        historyTable=None,
        targetDeleteDB=None,
        targetDeleteTable=None,
        createHistoryAudit=False,
        sourceIsIncremental=False,
        sourceIsImportTable=False,
        softDelete=False,
        mergeTime=datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),  # noqa: B008  # every caller passes mergeTime
        datalakeSource=None,
        PKColumns=None,
        hiveMergeJavaHeap=None,
        oracleFlashbackSource=False,
        mssqlChangeTrackingSource=False,
        oracleFlashbackImportTable=None,
        mssqlChangeTrackingImportTable=None,
        ChangeDataTrackingInitialLoad=False,
    ):
        """Merge source table into Target table. Also populate a History Audit table if selected"""
        logging.debug("Executing etl_operations.mergeHiveTables()")

        targetColumns = self.common_operations.getColumns(
            hiveDB=targetDB,
            hiveTable=targetTable,
            includeType=False,
            includeComment=False,
        )
        columnMerge = self.common_operations.getHiveColumnNameDiff(
            sourceDB=sourceDB,
            sourceTable=sourceTable,
            targetDB=targetDB,
            targetTable=targetTable,
            importTool=self.import_config.importTool,
            sourceIsImportTable=True,
        )
        if PKColumns == None:
            PKColumns = self.common_operations.getPK(
                hiveDB=targetDB, hiveTable=targetTable, quotedColumns=False
            )

        sourceDBandTable = "`%s`.`%s`" % (sourceDB, sourceTable)
        targetDBandTable = "`%s`.`%s`" % (targetDB, targetTable)
        historyDBandTable = "`%s`.`%s`" % (historyDB, historyTable)
        targetDeleteDBandTable = "`%s`.`%s`" % (targetDeleteDB, targetDeleteTable)
        oracleFlashbackImportDBandTable = "`%s`.`%s`" % (
            sourceDB,
            oracleFlashbackImportTable,
        )
        mssqlChangeTrackingImportDBandTable = "`%s`.`%s`" % (
            sourceDB,
            mssqlChangeTrackingImportTable,
        )

        if self.import_config.etlEngine == constant.ETL_ENGINE_SPARK:
            sourceDBandTable = "%s.%s" % (
                self.common_operations.sparkCatalogName,
                sourceDBandTable,
            )
            targetDBandTable = "%s.%s" % (
                self.common_operations.sparkCatalogName,
                targetDBandTable,
            )
            historyDBandTable = "%s.%s" % (
                self.common_operations.sparkCatalogName,
                historyDBandTable,
            )
            targetDeleteDBandTable = "%s.%s" % (
                self.common_operations.sparkCatalogName,
                targetDeleteDBandTable,
            )
            oracleFlashbackImportDBandTable = "%s.%s" % (
                self.common_operations.sparkCatalogName,
                oracleFlashbackImportDBandTable,
            )
            mssqlChangeTrackingImportDBandTable = "%s.%s" % (
                self.common_operations.sparkCatalogName,
                mssqlChangeTrackingImportDBandTable,
            )
            self.import_operations.startSpark()

        datalakeIUDExists = False
        datalakeInsertExists = False
        datalakeUpdateExists = False
        datalakeDeleteExists = False
        datalakeSourceExists = False

        for index, row in targetColumns.iterrows():
            # if row['name'] == "datalake_iud":    datalakeIUDExists = True
            # if row['name'] == "datalake_insert": datalakeInsertExists = True
            # if row['name'] == "datalake_update": datalakeUpdateExists = True
            # if row['name'] == "datalake_delete": datalakeDeleteExists = True
            # if row['name'] == "datalake_source": datalakeSourceExists = True
            if row["name"] == self.common_operations.Hive_ColumnName_IUD:
                datalakeIUDExists = True
            if row["name"] == self.common_operations.Hive_ColumnName_Insert:
                datalakeInsertExists = True
            if row["name"] == self.common_operations.Hive_ColumnName_Update:
                datalakeUpdateExists = True
            if row["name"] == self.common_operations.Hive_ColumnName_Delete:
                datalakeDeleteExists = True
            if row["name"] == self.common_operations.Hive_ColumnName_Source:
                datalakeSourceExists = True

        if self.import_config.etlEngine == constant.ETL_ENGINE_HIVE:
            if hiveMergeJavaHeap != None:
                query = "set hive.tez.container.size=%s" % (hiveMergeJavaHeap)
                self.common_operations.executeHiveQuery(query)

        query = "merge into %s as T \n" % (targetDBandTable)
        query += "using %s as S \n" % (sourceDBandTable)
        query += "on \n"

        for i, targetColumn in enumerate(PKColumns.split(",")):
            try:
                sourceColumn = (
                    columnMerge.loc[columnMerge["Exist"] == "both"]
                    .loc[columnMerge["targetName"] == targetColumn]
                    .iloc[0]["sourceName"]
                )
            except IndexError:
                logging.error(
                    "Primary Key cant be found in the source target table. Please check PK override"
                )
                self.import_config.remove_temporary_files()
                sys.exit(1)

            if sourceColumn == None:
                logging.error(
                    "ERROR: Problem determine column name in source table for primary key column '%s'"
                    % (targetColumn)
                )
                self.import_config.remove_temporary_files()
                sys.exit(1)

            if i == 0:
                query += "   T.`%s` = S.`%s` " % (targetColumn, sourceColumn)
            else:
                query += "and\n   T.`%s` = S.`%s` " % (targetColumn, sourceColumn)
        query += "\n"

        query += "when matched "
        if sourceIsIncremental == False:
            # If the source is not incremental, it means that we need to check all the values in
            # all columns as we dont know if the row have changed or not
            query += "and (\n"
            firstIteration = True
            for index, row in columnMerge.loc[
                columnMerge["Exist"] == "both"
            ].iterrows():
                foundPKcolumn = False
                for column in PKColumns.split(","):
                    if row["targetName"] == column:
                        foundPKcolumn = True
                if foundPKcolumn == False:
                    if firstIteration == True:
                        query += "   "
                        firstIteration = False
                    else:
                        query += "   or "
                    query += "T.`%s` != S.`%s` " % (
                        row["targetName"],
                        row["sourceName"],
                    )
                    query += "or ( T.`%s` is null and S.`%s` is not null ) " % (
                        row["targetName"],
                        row["sourceName"],
                    )
                    query += "or ( T.`%s` is not null and S.`%s` is null ) " % (
                        row["targetName"],
                        row["sourceName"],
                    )
                    query += "\n"

            if softDelete == True and datalakeIUDExists == True:
                # If a row is deleted and then inserted again with the same values in all fields, this will still trigger an update
                # query += "   or T.datalake_iud = 'D' \n"
                query += "   or T.%s = 'D' \n" % (
                    self.common_operations.Hive_ColumnName_IUD
                )

            query += ") \n"

        if oracleFlashbackSource == True:
            query += "and ( S.datalake_flashback_operation is null or S.datalake_flashback_operation != 'D' ) \n"

        if mssqlChangeTrackingSource == True:
            query += "and ( S.datalake_mssql_changetrack_operation is null or S.datalake_mssql_changetrack_operation != 'D' ) \n"

        query += "then update set "
        firstIteration = True
        nonPKcolumnFound = False
        for index, row in columnMerge.loc[columnMerge["Exist"] == "both"].iterrows():
            foundPKcolumn = False
            for column in PKColumns.split(","):
                if row["targetName"] == column:
                    foundPKcolumn = True
            if foundPKcolumn == False:
                if firstIteration == True:
                    firstIteration = False
                    query += " \n"
                else:
                    query += ", \n"
                query += "   `%s` = S.`%s`" % (row["targetName"], row["sourceName"])
                nonPKcolumnFound = True

        if nonPKcolumnFound == False:
            # This will happen if there are only columns that is part of the PK in the table. Impossible to merge it with full history
            logging.error(
                "This table only have columns that is part of the PrimaryKey. Merge operations cant be used"
            )
            self.import_config.remove_temporary_files()
            sys.exit(1)

        if datalakeIUDExists == True:
            # query += ", \n   `datalake_iud` = 'U'"
            query += ", \n   `%s` = 'U'" % (self.common_operations.Hive_ColumnName_IUD)

        if datalakeUpdateExists == True:
            # query += ", \n   `datalake_update` = TIMESTAMP('%s')"%(mergeTime)
            query += ", \n   `%s` = TIMESTAMP('%s')" % (
                self.common_operations.Hive_ColumnName_Update,
                mergeTime,
            )

        if datalakeSourceExists == True and datalakeSource != None:
            # query += ", \n   `datalake_source` = '%s'"%(datalakeSource)
            query += ", \n   `%s` = '%s'" % (
                self.common_operations.Hive_ColumnName_Source,
                datalakeSource,
            )

        query += " \n"

        if oracleFlashbackSource == True:
            query += (
                "when matched and S.datalake_flashback_operation = 'D' then delete \n"
            )

        # We should only delete rows during merge if we dont need to create the History table. Otherwise we will miss the values in all columns except PK columns
        if mssqlChangeTrackingSource == True and createHistoryAudit == False:
            query += "when matched and S.datalake_mssql_changetrack_operation = 'D' then delete \n"

        query += "when not matched "

        if oracleFlashbackSource == True:
            query += "and ( S.datalake_flashback_operation is null or S.datalake_flashback_operation != 'D' ) \n"

        if mssqlChangeTrackingSource == True:
            query += "and ( S.datalake_mssql_changetrack_operation is null or S.datalake_mssql_changetrack_operation != 'D' ) \n"

        query += "then insert ( "
        firstIteration = True
        for index, row in targetColumns.iterrows():
            ColumnName = row["name"]
            if firstIteration == True:
                firstIteration = False
                query += " \n"
            else:
                query += ", \n"
            query += "   `%s`" % (ColumnName)
        query += " \n) values ("

        firstIteration = True
        for index, row in targetColumns.iterrows():
            ColumnName = row["name"]
            sourceColumnName = (
                columnMerge.loc[columnMerge["targetName"] == ColumnName]["sourceName"]
                .fillna("")
                .iloc[0]
            )
            if firstIteration == True:
                firstIteration = False
                query += " \n"
            else:
                query += ", \n"
            if sourceColumnName != "":
                query += "   S.`%s`" % (sourceColumnName)
            # elif ColumnName == "datalake_iud":
            elif ColumnName == self.common_operations.Hive_ColumnName_IUD:
                query += "   'I'"
            # elif ColumnName == "datalake_insert":
            elif ColumnName == self.common_operations.Hive_ColumnName_Insert:
                query += "   TIMESTAMP('%s')" % (mergeTime)
            # elif ColumnName == "datalake_update":
            elif ColumnName == self.common_operations.Hive_ColumnName_Update:
                query += "   TIMESTAMP('%s')" % (mergeTime)
            # elif ColumnName == "datalake_source":
            elif ColumnName == self.common_operations.Hive_ColumnName_Source:
                query += "   '%s'" % (datalakeSource)
            else:
                query += "   NULL"

        query += " \n) \n"

        self.executeSQLQuery(query)

        # If a row was previously deleted and now inserted again and we are using Soft Delete,
        # then the information in the datalake_iud, datalake_insert and datalake_delete is wrong.
        if softDelete == True:
            query = "update %s set " % (targetDBandTable)
            # query += "   datalake_iud = 'I', "
            # query += "   datalake_insert = datalake_update, "
            # query += "   datalake_delete = null "
            query += "   %s = 'I', " % (self.common_operations.Hive_ColumnName_IUD)
            query += "   %s = %s, " % (
                self.common_operations.Hive_ColumnName_Insert,
                self.common_operations.Hive_ColumnName_Update,
            )
            query += "   %s = null " % (self.common_operations.Hive_ColumnName_Delete)
            query += "where "
            # query += "   datalake_iud = 'U' and "
            # query += "   datalake_delete is not null \n"
            query += "   %s = 'U' and " % (self.common_operations.Hive_ColumnName_IUD)
            query += "   %s is not null \n" % (
                self.common_operations.Hive_ColumnName_Delete
            )

            self.executeSQLQuery(query)

        # Statement to select all rows that was changed in the Target table and insert them to the History table
        if (
            createHistoryAudit == True
            and historyDB != None
            and historyTable != None
            and oracleFlashbackSource == False
            and mssqlChangeTrackingSource == False
        ) or (
            createHistoryAudit == True
            and historyDB != None
            and historyTable != None
            and mssqlChangeTrackingSource == True
            and sourceIsIncremental == False
        ):
            query = "insert into table %s \n" % (historyDBandTable)
            query += "( "
            firstIteration = True
            for index, row in columnMerge.loc[
                columnMerge["Exist"] == "both"
            ].iterrows():
                if firstIteration == True:
                    firstIteration = False
                    query += " \n"
                else:
                    query += ", \n"
                query += "   `%s`" % (row["targetName"])
            if datalakeSourceExists == True:
                # query += ",\n   `datalake_source`"
                query += ",\n   `%s`" % (self.common_operations.Hive_ColumnName_Source)
            # query += ",\n   `datalake_iud`"
            # query += ",\n   `datalake_timestamp`"
            query += ",\n   `%s`" % (self.common_operations.Hive_ColumnName_IUD)
            query += ",\n   `%s`" % (
                self.common_operations.Hive_ColumnName_HistoryTimestamp
            )
            query += "\n) \n"

            query += "select "
            firstIteration = True
            for index, row in columnMerge.loc[
                columnMerge["Exist"] == "both"
            ].iterrows():
                if firstIteration == True:
                    firstIteration = False
                    query += " \n"
                else:
                    query += ", \n"
                query += "   `%s`" % (row["targetName"])
            if datalakeSourceExists == True:
                query += ",\n   '%s'" % (datalakeSource)
            # query += ",\n   `datalake_iud`"
            # query += ",\n   `datalake_update`"
            # query += "\nfrom %s \n"%(targetDBandTable)
            # query += "where datalake_update = TIMESTAMP('%s') \n"%(mergeTime)
            query += ",\n   `%s`" % (self.common_operations.Hive_ColumnName_IUD)
            query += ",\n   `%s`" % (self.common_operations.Hive_ColumnName_Update)
            query += "\nfrom %s \n" % (targetDBandTable)
            query += "where %s = TIMESTAMP('%s') \n" % (
                self.common_operations.Hive_ColumnName_Update,
                mergeTime,
            )

            self.executeSQLQuery(query)

        if (
            sourceIsIncremental == False
            and targetDeleteDB != None
            and targetDeleteTable != None
        ):
            # Start with truncating the History Delete table as we need to rebuild this one from scratch to determine what rows are deleted
            query = "truncate table %s" % (targetDeleteDBandTable)
            self.executeSQLQuery(query)

            # Insert all rows (PK columns only) that exists in the Target Table but dont exists in the Import table (the ones that was deleted)
            query = "insert into table %s \n(`" % (targetDeleteDBandTable)
            query += "`, `".join(PKColumns.split(","))
            query += "`) \nselect T.`"
            query += "`, T.`".join(PKColumns.split(","))
            query += "` \nfrom %s as T \n" % (targetDBandTable)
            query += "left outer join %s as S \n" % (sourceDBandTable)
            query += "on \n"
            for i, targetColumn in enumerate(PKColumns.split(",")):
                sourceColumn = (
                    columnMerge.loc[columnMerge["Exist"] == "both"]
                    .loc[columnMerge["targetName"] == targetColumn]
                    .iloc[0]["sourceName"]
                )

                if i == 0:
                    query += "   T.`%s` = S.`%s` " % (targetColumn, sourceColumn)
                else:
                    query += "and\n   T.`%s` = S.`%s` " % (targetColumn, sourceColumn)

            query += "\nwhere \n"

            for i, targetColumn in enumerate(PKColumns.split(",")):
                sourceColumn = (
                    columnMerge.loc[columnMerge["Exist"] == "both"]
                    .loc[columnMerge["targetName"] == targetColumn]
                    .iloc[0]["sourceName"]
                )
                if i == 0:
                    query += "   S.`%s` is null " % (sourceColumn)
                else:
                    query += "and\n   S.`%s` is null " % (sourceColumn)
            query += "\n"

            self.executeSQLQuery(query)

        if oracleFlashbackSource == True and createHistoryAudit == True:
            # If it is a history merge with Oracle Flashback, we need to handle the deletes separatly
            query = "insert into table %s \n" % (historyDBandTable)
            query += "( "
            firstIteration = True
            for index, row in columnMerge.loc[
                columnMerge["Exist"] == "both"
            ].iterrows():
                if firstIteration == True:
                    firstIteration = False
                    query += " \n"
                else:
                    query += ", \n"
                query += "   `%s`" % (row["targetName"])
            if datalakeSourceExists == True:
                # query += ",\n   `datalake_source`"
                query += ",\n   `%s`" % (self.common_operations.Hive_ColumnName_Source)
            # query += ",\n   `datalake_iud`"
            # query += ",\n   `datalake_timestamp`"
            query += ",\n   `%s`" % (self.common_operations.Hive_ColumnName_IUD)
            query += ",\n   `%s`" % (
                self.common_operations.Hive_ColumnName_HistoryTimestamp
            )
            query += "\n) \n"

            query += "select "
            firstIteration = True
            for index, row in columnMerge.loc[
                columnMerge["Exist"] == "both"
            ].iterrows():
                if firstIteration == True:
                    firstIteration = False
                    query += " \n"
                else:
                    query += ", \n"
                query += "   `%s`" % (row["targetName"])
            if datalakeSourceExists == True:
                query += ",\n   '%s'" % (datalakeSource)
            # query += ",\n   `datalake_flashback_operation` as `datalake_iud`"
            # query += ",\n   timestamp('%s') as `datalake_timestamp`"%(mergeTime)
            query += ",\n   `datalake_flashback_operation` as `%s`" % (
                self.common_operations.Hive_ColumnName_IUD
            )
            query += ",\n   timestamp('%s') as `%s`" % (
                mergeTime,
                self.common_operations.Hive_ColumnName_HistoryTimestamp,
            )
            query += "\nfrom %s \n" % (oracleFlashbackImportDBandTable)

            self.executeSQLQuery(query)

        if (
            mssqlChangeTrackingSource == True
            and createHistoryAudit == True
            and sourceIsIncremental == True
        ):
            # We only run this of it's an incremental load, i.e CDT is working. Full load, falling over the edge and such should not load from import table but instead from target table
            # and that is handled above with the same code as full import with history is using
            query = "insert into table %s \n" % (historyDBandTable)
            query += "( "
            firstIteration = True
            for index, row in columnMerge.loc[
                columnMerge["Exist"] == "both"
            ].iterrows():
                if firstIteration == True:
                    firstIteration = False
                    query += " \n"
                else:
                    query += ", \n"
                query += "   `%s`" % (row["targetName"])
            if datalakeSourceExists == True:
                # query += ",\n   `datalake_source`"
                query += ",\n   `%s`" % (self.common_operations.Hive_ColumnName_Source)
            # query += ",\n   `datalake_iud`"
            # query += ",\n   `datalake_timestamp`"
            query += ",\n   `%s`" % (self.common_operations.Hive_ColumnName_IUD)
            query += ",\n   `%s`" % (
                self.common_operations.Hive_ColumnName_HistoryTimestamp
            )
            query += "\n) \n"

            query += "select "
            firstIteration = True
            for index, row in columnMerge.loc[
                columnMerge["Exist"] == "both"
            ].iterrows():
                if firstIteration == True:
                    firstIteration = False
                    query += " \n"
                else:
                    query += ", \n"
                query += "   `%s`" % (row["targetName"])
            if datalakeSourceExists == True:
                query += ",\n   '%s'" % (datalakeSource)
            # query += ",\n   `datalake_mssql_changetrack_operation` as `datalake_iud`"
            # query += ",\n   timestamp('%s') as `datalake_timestamp`"%(mergeTime)
            query += ",\n   `datalake_mssql_changetrack_operation` as `%s`" % (
                self.common_operations.Hive_ColumnName_IUD
            )
            query += ",\n   timestamp('%s') as `%s`" % (
                mergeTime,
                self.common_operations.Hive_ColumnName_HistoryTimestamp,
            )
            query += "\nfrom %s " % (mssqlChangeTrackingImportDBandTable)
            query += "\nwhere datalake_mssql_changetrack_operation != 'D' \n"

            self.executeSQLQuery(query)

            # This query inserts the deleted rows, including all columns into the History table.
            # Without it, the column not part of the PK would not be available in History table
            query = "insert into table %s \n" % (historyDBandTable)
            query += "( "
            firstIteration = True
            for index, row in columnMerge.loc[
                columnMerge["Exist"] == "both"
            ].iterrows():
                if firstIteration == True:
                    firstIteration = False
                    query += " \n"
                else:
                    query += ", \n"
                query += "   `%s`" % (row["targetName"])
            if datalakeSourceExists == True:
                # query += ",\n   `datalake_source`"
                query += ",\n   `%s`" % (self.common_operations.Hive_ColumnName_Source)
            # query += ",\n   `datalake_iud`"
            # query += ",\n   `datalake_timestamp`"
            query += ",\n   `%s`" % (self.common_operations.Hive_ColumnName_IUD)
            query += ",\n   `%s`" % (
                self.common_operations.Hive_ColumnName_HistoryTimestamp
            )
            query += "\n) \n"

            query += "select "
            firstIteration = True
            for index, row in columnMerge.loc[
                columnMerge["Exist"] == "both"
            ].iterrows():
                if firstIteration == True:
                    firstIteration = False
                    query += " \n"
                else:
                    query += ", \n"
                query += "   T.`%s`" % (row["targetName"])
            if datalakeSourceExists == True:
                query += ",\n   '%s' as `%s`" % (
                    datalakeSource,
                    self.common_operations.Hive_ColumnName_Source,
                )
            # query += ",\n   'D' as `datalake_iud`"
            # query += ",\n   timestamp('%s') as `datalake_timestamp`"%(mergeTime)
            query += ",\n   'D' as `%s`" % (self.common_operations.Hive_ColumnName_IUD)
            query += ",\n   timestamp('%s') as `%s`" % (
                mergeTime,
                self.common_operations.Hive_ColumnName_HistoryTimestamp,
            )
            query += "\nfrom %s as D \n" % (mssqlChangeTrackingImportDBandTable)
            query += "inner join %s as T \n" % (targetDBandTable)
            query += "on \n"
            for i, column in enumerate(PKColumns.split(",")):
                if i == 0:
                    query += "   T.`%s` = D.`%s` " % (column, column)
                else:
                    query += "and\n   T.`%s` = D.`%s` " % (column, column)
            query += "\nwhere D.datalake_mssql_changetrack_operation == 'D' \n"

            self.executeSQLQuery(query)

            # The last step is to delete the rows in the Target table based on what rows have D in the import table
            query = "merge into %s as T \n" % (targetDBandTable)
            query += "using %s as D \n" % (mssqlChangeTrackingImportDBandTable)
            query += "on \n"

            for i, column in enumerate(PKColumns.split(",")):
                if i == 0:
                    query += "   T.`%s` = D.`%s` " % (column, column)
                else:
                    query += "and\n   T.`%s` = D.`%s` " % (column, column)
            query += "\n"
            query += "and D.datalake_mssql_changetrack_operation == 'D' \n"
            query += "when matched then delete \n"

            self.executeSQLQuery(query)

        # Insert the deleted rows into the History table. Without this, it's impossible to see what values the column had before the delete
        if (
            sourceIsIncremental == False
            and createHistoryAudit == True
            and historyDB != None
            and historyTable != None
            and targetDeleteDB != None
            and targetDeleteTable != None
        ):
            query = "insert into table %s \n" % (historyDBandTable)
            query += "( "
            firstIteration = True
            for index, row in columnMerge.loc[
                columnMerge["Exist"] == "both"
            ].iterrows():
                if firstIteration == True:
                    firstIteration = False
                    query += " \n"
                else:
                    query += ", \n"
                query += "   `%s`" % (row["targetName"])
            if datalakeSourceExists == True:
                # query += ",\n   `datalake_source`"
                query += ",\n   `%s`" % (self.common_operations.Hive_ColumnName_Source)
            # query += ",\n   `datalake_iud`"
            # query += ",\n   `datalake_timestamp`"
            query += ",\n   `%s`" % (self.common_operations.Hive_ColumnName_IUD)
            query += ",\n   `%s`" % (
                self.common_operations.Hive_ColumnName_HistoryTimestamp
            )
            query += "\n) \n"

            query += "select "
            firstIteration = True
            for index, row in columnMerge.loc[
                columnMerge["Exist"] == "both"
            ].iterrows():
                if firstIteration == True:
                    firstIteration = False
                    query += " \n"
                else:
                    query += ", \n"
                query += "   T.`%s`" % (row["targetName"])
            if datalakeSourceExists == True:
                # query += ",\n   '%s' as `datalake_source`"%(datalakeSource)
                query += ",\n   '%s' as `%s`" % (
                    datalakeSource,
                    self.common_operations.Hive_ColumnName_Source,
                )
            # query += ",\n   'D' as `datalake_iud`"
            # query += ",\n   timestamp('%s') as `datalake_timestamp`"%(mergeTime)
            query += ",\n   'D' as `%s`" % (self.common_operations.Hive_ColumnName_IUD)
            query += ",\n   timestamp('%s') as `%s`" % (
                mergeTime,
                self.common_operations.Hive_ColumnName_HistoryTimestamp,
            )
            query += "\nfrom %s as D \n" % (targetDeleteDBandTable)
            query += "left join %s as T \n" % (targetDBandTable)
            query += "on \n"
            for i, column in enumerate(PKColumns.split(",")):
                if i == 0:
                    query += "   T.`%s` = D.`%s` " % (column, column)
                else:
                    query += "and\n   T.`%s` = D.`%s` " % (column, column)

            self.executeSQLQuery(query)

        if (
            sourceIsIncremental == False
            and targetDeleteDB != None
            and targetDeleteTable != None
        ):
            # Use the merge command to delete found rows between the Delete Table and the History Table
            if (
                self.import_config.etlEngine == constant.ETL_ENGINE_SPARK
                and softDelete == False
            ):
                # Due to Iceberg bug #8126 (https://github.com/apache/iceberg/issues/8126), we need to do a workaround for deletes without soft delete
                query = "delete from %s as T \n" % (targetDBandTable)
                query += "where exists ( \n"
                query += "   select "
                for i, column in enumerate(PKColumns.split(",")):
                    if i == 0:
                        query += "`%s` " % (column)
                    else:
                        query += ",`%s` " % (column)
                query += "\n"
                query += "   from %s as D \n" % (targetDeleteDBandTable)
                query += "   where \n"
                for i, column in enumerate(PKColumns.split(",")):
                    if i == 0:
                        query += "      T.`%s` = D.`%s` " % (column, column)
                    else:
                        query += "and\n      T.`%s` = D.`%s` " % (column, column)
                query += "\n"
                query += "   ) \n"

            else:
                query = "merge into %s as T \n" % (targetDBandTable)
                query += "using %s as D \n" % (targetDeleteDBandTable)
                query += "on \n"

                for i, column in enumerate(PKColumns.split(",")):
                    if i == 0:
                        query += "   T.`%s` = D.`%s` " % (column, column)
                    else:
                        query += "and\n   T.`%s` = D.`%s` " % (column, column)
                if softDelete == True:
                    # query += "and\n   T.`datalake_delete` != 'D' "
                    query += "and\n   T.`%s` != 'D' " % (
                        self.common_operations.Hive_ColumnName_Delete
                    )
                query += "\n"

                if softDelete == False:
                    query += "when matched then delete \n"
                else:
                    query += "when matched then update set \n"
                    # query += "datalake_iud  = 'D', \n"
                    # query += "datalake_update = timestamp('%s'), \n"%(mergeTime)
                    # query += "datalake_delete = timestamp('%s') "%(mergeTime)
                    query += "%s  = 'D', \n" % (
                        self.common_operations.Hive_ColumnName_IUD
                    )
                    query += "%s = timestamp('%s'), \n" % (
                        self.common_operations.Hive_ColumnName_Update,
                        mergeTime,
                    )
                    query += "%s = timestamp('%s') " % (
                        self.common_operations.Hive_ColumnName_Delete,
                        mergeTime,
                    )

            self.executeSQLQuery(query)

        logging.debug("Executing etl_operations.mergeHiveTables() - Finished")
