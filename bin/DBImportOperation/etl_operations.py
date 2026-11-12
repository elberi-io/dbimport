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


class Operation(metaclass=Singleton):
    def __init__(self, hive_db=None, hive_table=None):
        logging.debug("Executing etl_operations.__init__()")
        self.hive_db = None
        self.hive_table = None
        self.mysql_conn = None
        self.mysql_cursor = None
        self.start_date = None

        self.common_operations = common_operations.Operation(hive_db, hive_table)
        self.import_operations = import_operations.Operation(hive_db, hive_table)
        self.import_config = import_config.Config(hive_db, hive_table)

        if hive_db != None and hive_table != None:
            self.set_hive_table(hive_db, hive_table)
        else:
            # If the class already is initialized, we just pull the parameters and set them here
            self.hive_db = self.common_operations.hive_db
            self.hive_table = self.common_operations.hive_table
            self.start_date = self.import_config.start_date

        logging.debug("Executing etl_operations.__init__() - Finished")

    def set_hive_table(self, hive_db, hive_table):
        """Sets the parameters to work against a new Hive database and table"""
        self.hive_db = hive_db.lower()
        self.hive_table = hive_table.lower()

        self.common_operations.set_hive_table(self.hive_db, self.hive_table)
        self.import_config.set_hive_table(self.hive_db, self.hive_table)

        try:
            self.import_config.get_import_config()
            self.start_date = self.import_config.start_date
            self.import_config.lookup_connection_alias()
        except:
            self.import_config.remove_temporary_files()
            sys.exit(1)

    def remove_temporary_files(self):
        self.import_config.remove_temporary_files()

    def connect_to_hive(
        self,
    ):
        logging.debug("Executing etl_operations.connect_to_hive()")

        try:
            self.common_operations.connect_to_hive()
        except Exception as ex:
            logging.error(ex)
            self.import_config.remove_temporary_files()
            sys.exit(1)

        logging.debug("Executing etl_operations.connect_to_hive() - Finished")

    def execute_sql_query(self, query):
        if self.import_config.etl_engine == constant.ETL_ENGINE_HIVE:
            self.common_operations.execute_hive_query(query)

        if self.import_config.etl_engine == constant.ETL_ENGINE_SPARK:
            logging.info(query)
            self.import_operations.spark.sql(query)

    def merge_hive_tables(
        self,
        source_db,
        source_table,
        target_db,
        target_table,
        history_db=None,
        history_table=None,
        target_delete_db=None,
        target_delete_table=None,
        create_history_audit=False,
        source_is_incremental=False,
        source_is_import_table=False,
        soft_delete=False,
        merge_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),  # noqa: B008  # every caller passes it; a fallback would hide a missing value
        datalake_source=None,
        pk_columns=None,
        hive_merge_java_heap=None,
        oracle_flashback_source=False,
        mssql_change_tracking_source=False,
        oracle_flashback_import_table=None,
        mssql_change_tracking_import_table=None,
        change_data_tracking_initial_load=False,
    ):
        """Merge source table into Target table. Also populate a History Audit table if selected"""
        logging.debug("Executing etl_operations.merge_hive_tables()")

        target_columns = self.common_operations.get_columns(
            hive_db=target_db,
            hive_table=target_table,
            include_type=False,
            include_comment=False,
        )
        column_merge = self.common_operations.get_hive_column_name_diff(
            source_db=source_db,
            source_table=source_table,
            target_db=target_db,
            target_table=target_table,
            import_tool=self.import_config.import_tool,
            source_is_import_table=True,
        )
        if pk_columns == None:
            pk_columns = self.common_operations.get_pk(
                hive_db=target_db, hive_table=target_table, quoted_columns=False
            )

        source_db_and_table = "`%s`.`%s`" % (source_db, source_table)
        target_db_and_table = "`%s`.`%s`" % (target_db, target_table)
        history_db_and_table = "`%s`.`%s`" % (history_db, history_table)
        target_delete_db_and_table = "`%s`.`%s`" % (
            target_delete_db,
            target_delete_table,
        )
        oracle_flashback_import_db_and_table = "`%s`.`%s`" % (
            source_db,
            oracle_flashback_import_table,
        )
        mssql_change_tracking_import_db_and_table = "`%s`.`%s`" % (
            source_db,
            mssql_change_tracking_import_table,
        )

        if self.import_config.etl_engine == constant.ETL_ENGINE_SPARK:
            source_db_and_table = "%s.%s" % (
                self.common_operations.spark_catalog_name,
                source_db_and_table,
            )
            target_db_and_table = "%s.%s" % (
                self.common_operations.spark_catalog_name,
                target_db_and_table,
            )
            history_db_and_table = "%s.%s" % (
                self.common_operations.spark_catalog_name,
                history_db_and_table,
            )
            target_delete_db_and_table = "%s.%s" % (
                self.common_operations.spark_catalog_name,
                target_delete_db_and_table,
            )
            oracle_flashback_import_db_and_table = "%s.%s" % (
                self.common_operations.spark_catalog_name,
                oracle_flashback_import_db_and_table,
            )
            mssql_change_tracking_import_db_and_table = "%s.%s" % (
                self.common_operations.spark_catalog_name,
                mssql_change_tracking_import_db_and_table,
            )
            self.import_operations.start_spark()

        datalake_iud_exists = False
        datalake_insert_exists = False
        datalake_update_exists = False
        datalake_delete_exists = False
        datalake_source_exists = False

        for _index, row in target_columns.iterrows():
            # if row['name'] == "datalake_iud":    datalakeIUDExists = True
            # if row['name'] == "datalake_insert": datalakeInsertExists = True
            # if row['name'] == "datalake_update": datalakeUpdateExists = True
            # if row['name'] == "datalake_delete": datalakeDeleteExists = True
            # if row['name'] == "datalake_source": datalakeSourceExists = True
            if row["name"] == self.common_operations.hive_column_name_iud:
                datalake_iud_exists = True
            if row["name"] == self.common_operations.hive_column_name_insert:
                datalake_insert_exists = True
            if row["name"] == self.common_operations.hive_column_name_update:
                datalake_update_exists = True
            if row["name"] == self.common_operations.hive_column_name_delete:
                datalake_delete_exists = True
            if row["name"] == self.common_operations.hive_column_name_source:
                datalake_source_exists = True

        if self.import_config.etl_engine == constant.ETL_ENGINE_HIVE:
            if hive_merge_java_heap != None:
                query = "set hive.tez.container.size=%s" % (hive_merge_java_heap)
                self.common_operations.execute_hive_query(query)

        query = "merge into %s as T \n" % (target_db_and_table)
        query += "using %s as S \n" % (source_db_and_table)
        query += "on \n"

        for i, target_column in enumerate(pk_columns.split(",")):
            try:
                source_column = (
                    column_merge.loc[column_merge["Exist"] == "both"]
                    .loc[column_merge["targetName"] == target_column]
                    .iloc[0]["sourceName"]
                )
            except IndexError:
                logging.error(
                    "Primary Key cant be found in the source target table. Please check PK override"
                )
                self.import_config.remove_temporary_files()
                sys.exit(1)

            if source_column == None:
                logging.error(
                    "ERROR: Problem determine column name in source table for primary key column '%s'"
                    % (target_column)
                )
                self.import_config.remove_temporary_files()
                sys.exit(1)

            if i == 0:
                query += "   T.`%s` = S.`%s` " % (target_column, source_column)
            else:
                query += "and\n   T.`%s` = S.`%s` " % (target_column, source_column)
        query += "\n"

        query += "when matched "
        if source_is_incremental == False:
            # If the source is not incremental, it means that we need to check all the values in
            # all columns as we dont know if the row have changed or not
            query += "and (\n"
            first_iteration = True
            for _index, row in column_merge.loc[
                column_merge["Exist"] == "both"
            ].iterrows():
                found_pk_column = False
                for column in pk_columns.split(","):
                    if row["targetName"] == column:
                        found_pk_column = True
                if not found_pk_column:
                    if first_iteration:
                        query += "   "
                        first_iteration = False
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

            if soft_delete == True and datalake_iud_exists:
                # If a row is deleted and then inserted again with the same values in all fields, this will still trigger an update
                # query += "   or T.datalake_iud = 'D' \n"
                query += "   or T.%s = 'D' \n" % (
                    self.common_operations.hive_column_name_iud
                )

            query += ") \n"

        if oracle_flashback_source == True:
            query += "and ( S.datalake_flashback_operation is null or S.datalake_flashback_operation != 'D' ) \n"

        if mssql_change_tracking_source == True:
            query += "and ( S.datalake_mssql_changetrack_operation is null or S.datalake_mssql_changetrack_operation != 'D' ) \n"

        query += "then update set "
        first_iteration = True
        non_pk_column_found = False
        for _index, row in column_merge.loc[column_merge["Exist"] == "both"].iterrows():
            found_pk_column = False
            for column in pk_columns.split(","):
                if row["targetName"] == column:
                    found_pk_column = True
            if not found_pk_column:
                if first_iteration:
                    first_iteration = False
                    query += " \n"
                else:
                    query += ", \n"
                query += "   `%s` = S.`%s`" % (row["targetName"], row["sourceName"])
                non_pk_column_found = True

        if not non_pk_column_found:
            # This will happen if there are only columns that is part of the PK in the table. Impossible to merge it with full history
            logging.error(
                "This table only have columns that is part of the PrimaryKey. Merge operations cant be used"
            )
            self.import_config.remove_temporary_files()
            sys.exit(1)

        if datalake_iud_exists:
            # query += ", \n   `datalake_iud` = 'U'"
            query += ", \n   `%s` = 'U'" % (self.common_operations.hive_column_name_iud)

        if datalake_update_exists:
            # query += ", \n   `datalake_update` = TIMESTAMP('%s')"%(mergeTime)
            query += ", \n   `%s` = TIMESTAMP('%s')" % (
                self.common_operations.hive_column_name_update,
                merge_time,
            )

        if datalake_source_exists and datalake_source != None:
            # query += ", \n   `datalake_source` = '%s'"%(datalakeSource)
            query += ", \n   `%s` = '%s'" % (
                self.common_operations.hive_column_name_source,
                datalake_source,
            )

        query += " \n"

        if oracle_flashback_source == True:
            query += (
                "when matched and S.datalake_flashback_operation = 'D' then delete \n"
            )

        # We should only delete rows during merge if we dont need to create the History table. Otherwise we will miss the values in all columns except PK columns
        if mssql_change_tracking_source == True and create_history_audit == False:
            query += "when matched and S.datalake_mssql_changetrack_operation = 'D' then delete \n"

        query += "when not matched "

        if oracle_flashback_source == True:
            query += "and ( S.datalake_flashback_operation is null or S.datalake_flashback_operation != 'D' ) \n"

        if mssql_change_tracking_source == True:
            query += "and ( S.datalake_mssql_changetrack_operation is null or S.datalake_mssql_changetrack_operation != 'D' ) \n"

        query += "then insert ( "
        first_iteration = True
        for _index, row in target_columns.iterrows():
            column_name = row["name"]
            if first_iteration:
                first_iteration = False
                query += " \n"
            else:
                query += ", \n"
            query += "   `%s`" % (column_name)
        query += " \n) values ("

        first_iteration = True
        for _index, row in target_columns.iterrows():
            column_name = row["name"]
            source_column_name = (
                column_merge.loc[column_merge["targetName"] == column_name][
                    "sourceName"
                ]
                .fillna("")
                .iloc[0]
            )
            if first_iteration:
                first_iteration = False
                query += " \n"
            else:
                query += ", \n"
            if source_column_name != "":
                query += "   S.`%s`" % (source_column_name)
            # elif ColumnName == "datalake_iud":
            elif column_name == self.common_operations.hive_column_name_iud:
                query += "   'I'"
            # elif ColumnName == "datalake_insert":
            elif column_name == self.common_operations.hive_column_name_insert:
                query += "   TIMESTAMP('%s')" % (merge_time)
            # elif ColumnName == "datalake_update":
            elif column_name == self.common_operations.hive_column_name_update:
                query += "   TIMESTAMP('%s')" % (merge_time)
            # elif ColumnName == "datalake_source":
            elif column_name == self.common_operations.hive_column_name_source:
                query += "   '%s'" % (datalake_source)
            else:
                query += "   NULL"

        query += " \n) \n"

        self.execute_sql_query(query)

        # If a row was previously deleted and now inserted again and we are using Soft Delete,
        # then the information in the datalake_iud, datalake_insert and datalake_delete is wrong.
        if soft_delete == True:
            query = "update %s set " % (target_db_and_table)
            # query += "   datalake_iud = 'I', "
            # query += "   datalake_insert = datalake_update, "
            # query += "   datalake_delete = null "
            query += "   %s = 'I', " % (self.common_operations.hive_column_name_iud)
            query += "   %s = %s, " % (
                self.common_operations.hive_column_name_insert,
                self.common_operations.hive_column_name_update,
            )
            query += "   %s = null " % (self.common_operations.hive_column_name_delete)
            query += "where "
            # query += "   datalake_iud = 'U' and "
            # query += "   datalake_delete is not null \n"
            query += "   %s = 'U' and " % (self.common_operations.hive_column_name_iud)
            query += "   %s is not null \n" % (
                self.common_operations.hive_column_name_delete
            )

            self.execute_sql_query(query)

        # Statement to select all rows that was changed in the Target table and insert them to the History table
        if (
            create_history_audit == True
            and history_db != None
            and history_table != None
            and oracle_flashback_source == False
            and mssql_change_tracking_source == False
        ) or (
            create_history_audit == True
            and history_db != None
            and history_table != None
            and mssql_change_tracking_source == True
            and source_is_incremental == False
        ):
            query = "insert into table %s \n" % (history_db_and_table)
            query += "( "
            first_iteration = True
            for _index, row in column_merge.loc[
                column_merge["Exist"] == "both"
            ].iterrows():
                if first_iteration:
                    first_iteration = False
                    query += " \n"
                else:
                    query += ", \n"
                query += "   `%s`" % (row["targetName"])
            if datalake_source_exists:
                # query += ",\n   `datalake_source`"
                query += ",\n   `%s`" % (self.common_operations.hive_column_name_source)
            # query += ",\n   `datalake_iud`"
            # query += ",\n   `datalake_timestamp`"
            query += ",\n   `%s`" % (self.common_operations.hive_column_name_iud)
            query += ",\n   `%s`" % (
                self.common_operations.hive_column_name_history_timestamp
            )
            query += "\n) \n"

            query += "select "
            first_iteration = True
            for _index, row in column_merge.loc[
                column_merge["Exist"] == "both"
            ].iterrows():
                if first_iteration:
                    first_iteration = False
                    query += " \n"
                else:
                    query += ", \n"
                query += "   `%s`" % (row["targetName"])
            if datalake_source_exists:
                query += ",\n   '%s'" % (datalake_source)
            # query += ",\n   `datalake_iud`"
            # query += ",\n   `datalake_update`"
            # query += "\nfrom %s \n"%(targetDBandTable)
            # query += "where datalake_update = TIMESTAMP('%s') \n"%(mergeTime)
            query += ",\n   `%s`" % (self.common_operations.hive_column_name_iud)
            query += ",\n   `%s`" % (self.common_operations.hive_column_name_update)
            query += "\nfrom %s \n" % (target_db_and_table)
            query += "where %s = TIMESTAMP('%s') \n" % (
                self.common_operations.hive_column_name_update,
                merge_time,
            )

            self.execute_sql_query(query)

        if (
            source_is_incremental == False
            and target_delete_db != None
            and target_delete_table != None
        ):
            # Start with truncating the History Delete table as we need to rebuild this one from scratch to determine what rows are deleted
            query = "truncate table %s" % (target_delete_db_and_table)
            self.execute_sql_query(query)

            # Insert all rows (PK columns only) that exists in the Target Table but dont exists in the Import table (the ones that was deleted)
            query = "insert into table %s \n(`" % (target_delete_db_and_table)
            query += "`, `".join(pk_columns.split(","))
            query += "`) \nselect T.`"
            query += "`, T.`".join(pk_columns.split(","))
            query += "` \nfrom %s as T \n" % (target_db_and_table)
            query += "left outer join %s as S \n" % (source_db_and_table)
            query += "on \n"
            for i, target_column in enumerate(pk_columns.split(",")):
                source_column = (
                    column_merge.loc[column_merge["Exist"] == "both"]
                    .loc[column_merge["targetName"] == target_column]
                    .iloc[0]["sourceName"]
                )

                if i == 0:
                    query += "   T.`%s` = S.`%s` " % (target_column, source_column)
                else:
                    query += "and\n   T.`%s` = S.`%s` " % (target_column, source_column)

            query += "\nwhere \n"

            for i, target_column in enumerate(pk_columns.split(",")):
                source_column = (
                    column_merge.loc[column_merge["Exist"] == "both"]
                    .loc[column_merge["targetName"] == target_column]
                    .iloc[0]["sourceName"]
                )
                if i == 0:
                    query += "   S.`%s` is null " % (source_column)
                else:
                    query += "and\n   S.`%s` is null " % (source_column)
            query += "\n"

            self.execute_sql_query(query)

        if oracle_flashback_source == True and create_history_audit == True:
            # If it is a history merge with Oracle Flashback, we need to handle the deletes separatly
            query = "insert into table %s \n" % (history_db_and_table)
            query += "( "
            first_iteration = True
            for _index, row in column_merge.loc[
                column_merge["Exist"] == "both"
            ].iterrows():
                if first_iteration:
                    first_iteration = False
                    query += " \n"
                else:
                    query += ", \n"
                query += "   `%s`" % (row["targetName"])
            if datalake_source_exists:
                # query += ",\n   `datalake_source`"
                query += ",\n   `%s`" % (self.common_operations.hive_column_name_source)
            # query += ",\n   `datalake_iud`"
            # query += ",\n   `datalake_timestamp`"
            query += ",\n   `%s`" % (self.common_operations.hive_column_name_iud)
            query += ",\n   `%s`" % (
                self.common_operations.hive_column_name_history_timestamp
            )
            query += "\n) \n"

            query += "select "
            first_iteration = True
            for _index, row in column_merge.loc[
                column_merge["Exist"] == "both"
            ].iterrows():
                if first_iteration:
                    first_iteration = False
                    query += " \n"
                else:
                    query += ", \n"
                query += "   `%s`" % (row["targetName"])
            if datalake_source_exists:
                query += ",\n   '%s'" % (datalake_source)
            # query += ",\n   `datalake_flashback_operation` as `datalake_iud`"
            # query += ",\n   timestamp('%s') as `datalake_timestamp`"%(mergeTime)
            query += ",\n   `datalake_flashback_operation` as `%s`" % (
                self.common_operations.hive_column_name_iud
            )
            query += ",\n   timestamp('%s') as `%s`" % (
                merge_time,
                self.common_operations.hive_column_name_history_timestamp,
            )
            query += "\nfrom %s \n" % (oracle_flashback_import_db_and_table)

            self.execute_sql_query(query)

        if (
            mssql_change_tracking_source == True
            and create_history_audit == True
            and source_is_incremental == True
        ):
            # We only run this of it's an incremental load, i.e CDT is working. Full load, falling over the edge and such should not load from import table but instead from target table
            # and that is handled above with the same code as full import with history is using
            query = "insert into table %s \n" % (history_db_and_table)
            query += "( "
            first_iteration = True
            for _index, row in column_merge.loc[
                column_merge["Exist"] == "both"
            ].iterrows():
                if first_iteration:
                    first_iteration = False
                    query += " \n"
                else:
                    query += ", \n"
                query += "   `%s`" % (row["targetName"])
            if datalake_source_exists:
                # query += ",\n   `datalake_source`"
                query += ",\n   `%s`" % (self.common_operations.hive_column_name_source)
            # query += ",\n   `datalake_iud`"
            # query += ",\n   `datalake_timestamp`"
            query += ",\n   `%s`" % (self.common_operations.hive_column_name_iud)
            query += ",\n   `%s`" % (
                self.common_operations.hive_column_name_history_timestamp
            )
            query += "\n) \n"

            query += "select "
            first_iteration = True
            for _index, row in column_merge.loc[
                column_merge["Exist"] == "both"
            ].iterrows():
                if first_iteration:
                    first_iteration = False
                    query += " \n"
                else:
                    query += ", \n"
                query += "   `%s`" % (row["targetName"])
            if datalake_source_exists:
                query += ",\n   '%s'" % (datalake_source)
            # query += ",\n   `datalake_mssql_changetrack_operation` as `datalake_iud`"
            # query += ",\n   timestamp('%s') as `datalake_timestamp`"%(mergeTime)
            query += ",\n   `datalake_mssql_changetrack_operation` as `%s`" % (
                self.common_operations.hive_column_name_iud
            )
            query += ",\n   timestamp('%s') as `%s`" % (
                merge_time,
                self.common_operations.hive_column_name_history_timestamp,
            )
            query += "\nfrom %s " % (mssql_change_tracking_import_db_and_table)
            query += "\nwhere datalake_mssql_changetrack_operation != 'D' \n"

            self.execute_sql_query(query)

            # This query inserts the deleted rows, including all columns into the History table.
            # Without it, the column not part of the PK would not be available in History table
            query = "insert into table %s \n" % (history_db_and_table)
            query += "( "
            first_iteration = True
            for _index, row in column_merge.loc[
                column_merge["Exist"] == "both"
            ].iterrows():
                if first_iteration:
                    first_iteration = False
                    query += " \n"
                else:
                    query += ", \n"
                query += "   `%s`" % (row["targetName"])
            if datalake_source_exists:
                # query += ",\n   `datalake_source`"
                query += ",\n   `%s`" % (self.common_operations.hive_column_name_source)
            # query += ",\n   `datalake_iud`"
            # query += ",\n   `datalake_timestamp`"
            query += ",\n   `%s`" % (self.common_operations.hive_column_name_iud)
            query += ",\n   `%s`" % (
                self.common_operations.hive_column_name_history_timestamp
            )
            query += "\n) \n"

            query += "select "
            first_iteration = True
            for _index, row in column_merge.loc[
                column_merge["Exist"] == "both"
            ].iterrows():
                if first_iteration:
                    first_iteration = False
                    query += " \n"
                else:
                    query += ", \n"
                query += "   T.`%s`" % (row["targetName"])
            if datalake_source_exists:
                query += ",\n   '%s' as `%s`" % (
                    datalake_source,
                    self.common_operations.hive_column_name_source,
                )
            # query += ",\n   'D' as `datalake_iud`"
            # query += ",\n   timestamp('%s') as `datalake_timestamp`"%(mergeTime)
            query += ",\n   'D' as `%s`" % (self.common_operations.hive_column_name_iud)
            query += ",\n   timestamp('%s') as `%s`" % (
                merge_time,
                self.common_operations.hive_column_name_history_timestamp,
            )
            query += "\nfrom %s as D \n" % (mssql_change_tracking_import_db_and_table)
            query += "inner join %s as T \n" % (target_db_and_table)
            query += "on \n"
            for i, column in enumerate(pk_columns.split(",")):
                if i == 0:
                    query += "   T.`%s` = D.`%s` " % (column, column)
                else:
                    query += "and\n   T.`%s` = D.`%s` " % (column, column)
            query += "\nwhere D.datalake_mssql_changetrack_operation == 'D' \n"

            self.execute_sql_query(query)

            # The last step is to delete the rows in the Target table based on what rows have D in the import table
            query = "merge into %s as T \n" % (target_db_and_table)
            query += "using %s as D \n" % (mssql_change_tracking_import_db_and_table)
            query += "on \n"

            for i, column in enumerate(pk_columns.split(",")):
                if i == 0:
                    query += "   T.`%s` = D.`%s` " % (column, column)
                else:
                    query += "and\n   T.`%s` = D.`%s` " % (column, column)
            query += "\n"
            query += "and D.datalake_mssql_changetrack_operation == 'D' \n"
            query += "when matched then delete \n"

            self.execute_sql_query(query)

        # Insert the deleted rows into the History table. Without this, it's impossible to see what values the column had before the delete
        if (
            source_is_incremental == False
            and create_history_audit == True
            and history_db != None
            and history_table != None
            and target_delete_db != None
            and target_delete_table != None
        ):
            query = "insert into table %s \n" % (history_db_and_table)
            query += "( "
            first_iteration = True
            for _index, row in column_merge.loc[
                column_merge["Exist"] == "both"
            ].iterrows():
                if first_iteration:
                    first_iteration = False
                    query += " \n"
                else:
                    query += ", \n"
                query += "   `%s`" % (row["targetName"])
            if datalake_source_exists:
                # query += ",\n   `datalake_source`"
                query += ",\n   `%s`" % (self.common_operations.hive_column_name_source)
            # query += ",\n   `datalake_iud`"
            # query += ",\n   `datalake_timestamp`"
            query += ",\n   `%s`" % (self.common_operations.hive_column_name_iud)
            query += ",\n   `%s`" % (
                self.common_operations.hive_column_name_history_timestamp
            )
            query += "\n) \n"

            query += "select "
            first_iteration = True
            for _index, row in column_merge.loc[
                column_merge["Exist"] == "both"
            ].iterrows():
                if first_iteration:
                    first_iteration = False
                    query += " \n"
                else:
                    query += ", \n"
                query += "   T.`%s`" % (row["targetName"])
            if datalake_source_exists:
                # query += ",\n   '%s' as `datalake_source`"%(datalakeSource)
                query += ",\n   '%s' as `%s`" % (
                    datalake_source,
                    self.common_operations.hive_column_name_source,
                )
            # query += ",\n   'D' as `datalake_iud`"
            # query += ",\n   timestamp('%s') as `datalake_timestamp`"%(mergeTime)
            query += ",\n   'D' as `%s`" % (self.common_operations.hive_column_name_iud)
            query += ",\n   timestamp('%s') as `%s`" % (
                merge_time,
                self.common_operations.hive_column_name_history_timestamp,
            )
            query += "\nfrom %s as D \n" % (target_delete_db_and_table)
            query += "left join %s as T \n" % (target_db_and_table)
            query += "on \n"
            for i, column in enumerate(pk_columns.split(",")):
                if i == 0:
                    query += "   T.`%s` = D.`%s` " % (column, column)
                else:
                    query += "and\n   T.`%s` = D.`%s` " % (column, column)

            self.execute_sql_query(query)

        if (
            source_is_incremental == False
            and target_delete_db != None
            and target_delete_table != None
        ):
            # Use the merge command to delete found rows between the Delete Table and the History Table
            if (
                self.import_config.etl_engine == constant.ETL_ENGINE_SPARK
                and soft_delete == False
            ):
                # Due to Iceberg bug #8126 (https://github.com/apache/iceberg/issues/8126), we need to do a workaround for deletes without soft delete
                query = "delete from %s as T \n" % (target_db_and_table)
                query += "where exists ( \n"
                query += "   select "
                for i, column in enumerate(pk_columns.split(",")):
                    if i == 0:
                        query += "`%s` " % (column)
                    else:
                        query += ",`%s` " % (column)
                query += "\n"
                query += "   from %s as D \n" % (target_delete_db_and_table)
                query += "   where \n"
                for i, column in enumerate(pk_columns.split(",")):
                    if i == 0:
                        query += "      T.`%s` = D.`%s` " % (column, column)
                    else:
                        query += "and\n      T.`%s` = D.`%s` " % (column, column)
                query += "\n"
                query += "   ) \n"

            else:
                query = "merge into %s as T \n" % (target_db_and_table)
                query += "using %s as D \n" % (target_delete_db_and_table)
                query += "on \n"

                for i, column in enumerate(pk_columns.split(",")):
                    if i == 0:
                        query += "   T.`%s` = D.`%s` " % (column, column)
                    else:
                        query += "and\n   T.`%s` = D.`%s` " % (column, column)
                if soft_delete == True:
                    # query += "and\n   T.`datalake_delete` != 'D' "
                    query += "and\n   T.`%s` != 'D' " % (
                        self.common_operations.hive_column_name_delete
                    )
                query += "\n"

                if soft_delete == False:
                    query += "when matched then delete \n"
                else:
                    query += "when matched then update set \n"
                    # query += "datalake_iud  = 'D', \n"
                    # query += "datalake_update = timestamp('%s'), \n"%(mergeTime)
                    # query += "datalake_delete = timestamp('%s') "%(mergeTime)
                    query += "%s  = 'D', \n" % (
                        self.common_operations.hive_column_name_iud
                    )
                    query += "%s = timestamp('%s'), \n" % (
                        self.common_operations.hive_column_name_update,
                        merge_time,
                    )
                    query += "%s = timestamp('%s') " % (
                        self.common_operations.hive_column_name_delete,
                        merge_time,
                    )

            self.execute_sql_query(query)

        logging.debug("Executing etl_operations.merge_hive_tables() - Finished")
