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
import random
import re
import subprocess
import sys
import time

import pandas as pd
import puretransport
import requests
from pyhive import exc, hive
from requests_kerberos import HTTPKerberosAuth
from TCLIService.ttypes import TOperationState

from catalogService import glue, hiveDirect
from common import constants as constant
from common.Exceptions import SQLerror, invalidConfiguration
from common.Singleton import Singleton
from ConfigReader import configuration
from DBImportConfig import common_config


class operation(metaclass=Singleton):
    def __init__(self, Hive_DB=None, Hive_Table=None):
        logging.debug("Executing common_operation.__init__()")

        self.Hive_DB = Hive_DB
        self.Hive_Table = Hive_Table
        self.hive_conn = None
        self.hive_cursor = None
        self.sparkCatalogName = None
        self.debugLogLevel = False

        if logging.root.level == 10:  # DEBUG
            self.debugLogLevel = True

        # Fetch and initialize the Kerberos configuration
        self.kerberosPrincipal = configuration.get("Kerberos", "principal")
        self.webHCatAuth = HTTPKerberosAuth(
            force_preemptive=True, principal=self.kerberosPrincipal
        )

        self.common_config = common_config.config()

        # Fetch configuration details about Hive
        self.hive_servers = configuration.get("Hive", "servers")
        self.hive_kerberos_service_name = configuration.get(
            "Hive", "kerberos_service_name"
        )
        self.hive_kerberos_realm = configuration.get("Hive", "kerberos_realm")
        self.hive_print_messages = self.common_config.getConfigValue(
            key="hive_print_messages"
        )
        if configuration.get("Hive", "use_ssl").lower() == "true":
            self.hive_use_ssl = True
        else:
            self.hive_use_ssl = False

        # HDFS Settings
        self.hdfs_address = self.common_config.getConfigValue(key="hdfs_address")
        self.hdfs_basedir = self.common_config.getConfigValue(key="hdfs_basedir")
        self.hdfs_blocksize = self.common_config.getConfigValue(key="hdfs_blocksize")

        # Metastore connections
        self.metastore_type = configuration.get("Metastore", "metastore_type")
        if self.metastore_type == constant.CATALOG_HIVE_DIRECT:
            self.metastore = hiveDirect.hiveMetastoreDirect()
            self.sparkCatalogName = "hive"
        elif self.metastore_type == constant.CATALOG_GLUE:
            self.metastore = glue.glueCatalog()
            self.sparkCatalogName = "glue"
        else:
            raise invalidConfiguration(
                "Configuration 'metastore_type' under [Metastore] in the configuration file contains an invalid option"
            )

        logging.debug("Executing common_operations.__init__() - Finished")

    def __del__(self):
        """Make sure that the Hive connector is closed correctly"""

        try:
            if self.hive_cursor != None:
                self.hive_cursor.close()
                self.hive_conn.close()
        except:
            # We dont really care. Close if you can. If you cant, well, we still are closing the application :)
            pass

    def reconnectHiveMetaStore(self):
        logging.debug(
            "Reconnecting to Hive Metastore should not be needed anymore. Code is not really doing anything"
        )

    def setHiveTable(self, Hive_DB, Hive_Table):
        """Sets the parameters to work against a new Hive database and table"""
        self.Hive_DB = Hive_DB.lower()
        self.Hive_Table = Hive_Table.lower()

    def setTableAndColumnNames(
        self,
        hive_import_db,
        hive_import_table,
        hive_import_view,
        hive_history_db,
        hive_history_table,
        hive_history_temp_db,
        hive_history_temp_table,
        hive_import_pk_only_db,
        hive_import_pk_only_table,
        hive_delete_db,
        hive_delete_table,
        hive_column_name_import,
        hive_column_name_insert,
        hive_column_name_update,
        hive_column_name_delete,
        hive_column_name_iud,
        hive_column_name_history_timestamp,
        hive_column_name_source,
    ):
        """Sets the name of the databases, tables and columns"""

        self.Hive_Import_DB = hive_import_db
        self.Hive_Import_Table = hive_import_table
        self.Hive_Import_View = hive_import_view
        self.Hive_History_DB = hive_history_db
        self.Hive_History_Table = hive_history_table
        self.Hive_HistoryTemp_DB = hive_history_temp_db
        self.Hive_HistoryTemp_Table = hive_history_temp_table
        self.Hive_Import_PKonly_DB = hive_import_pk_only_db
        self.Hive_Import_PKonly_Table = hive_import_pk_only_table
        self.Hive_Delete_DB = hive_delete_db
        self.Hive_Delete_Table = hive_delete_table
        self.Hive_ColumnName_Import = hive_column_name_import
        self.Hive_ColumnName_Insert = hive_column_name_insert
        self.Hive_ColumnName_Update = hive_column_name_update
        self.Hive_ColumnName_Delete = hive_column_name_delete
        self.Hive_ColumnName_IUD = hive_column_name_iud
        self.Hive_ColumnName_HistoryTimestamp = hive_column_name_history_timestamp
        self.Hive_ColumnName_Source = hive_column_name_source

    def checkHiveMetaStore(self):
        logging.debug("Executing common_operations.checkHiveMetaStore()")

        # Loop through all Hive webHCat servers from the configuration and checks if one of them works
        self.hiveMetaStore = None
        web_hcat_response = None
        for hive_url in self.hiveMetaStoreList:
            try:
                url = hive_url.strip() + "/templeton/v1/status"
                logging.debug("Testing status for webHCat on URL: %s" % (url))
                web_hcat_response = requests.get(url, auth=self.webHCatAuth)
            except:
                continue
            if web_hcat_response.status_code == 200:
                self.hiveMetaStore = hive_url.strip()
                break

        if self.hiveMetaStore == None:
            logging.error(
                "Cant find a working Hive webHCat server. Please check configuration and server status"
            )
            # TODO: Handle the exit here

        logging.debug("Executing common_operation.checkHiveMetaStore() - Finished")

    def checkDB(self, hiveDB):
        result = self.metastore.checkDB(hiveDB)
        return result

    def checkTable(self, hiveDB, hiveTable):
        result = self.metastore.checkTable(hiveDB=hiveDB, hiveTable=hiveTable)
        return result

    def getTableLocation(self, hiveDB, hiveTable):
        result = self.metastore.getTableLocation(hiveDB=hiveDB, hiveTable=hiveTable)
        return result

    def isTableExternalOrcFormat(self, hiveDB, hiveTable):
        result = self.metastore.isTableExternalOrcFormat(
            hiveDB=hiveDB, hiveTable=hiveTable
        )
        return result

    def isTableExternalParquetFormat(self, hiveDB, hiveTable):
        result = self.metastore.isTableExternalParquetFormat(
            hiveDB=hiveDB, hiveTable=hiveTable
        )
        return result

    def isTableExternalIcebergFormat(self, hiveDB, hiveTable):
        result = self.metastore.isTableExternalIcebergFormat(
            hiveDB=hiveDB, hiveTable=hiveTable
        )
        return result

    def isTableExternal(self, hiveDB, hiveTable):
        result = self.metastore.isTableExternal(hiveDB=hiveDB, hiveTable=hiveTable)
        return result

    def isTableTransactional(self, hiveDB, hiveTable):
        result = self.metastore.isTableTransactional(hiveDB=hiveDB, hiveTable=hiveTable)
        return result

    def isTableView(self, hiveDB, hiveTable):
        result = self.metastore.isTableView(hiveDB=hiveDB, hiveTable=hiveTable)
        return result

    def getPK(self, hiveDB, hiveTable, quotedColumns=False):
        result = self.metastore.getPK(
            hiveDB=hiveDB, hiveTable=hiveTable, quotedColumns=quotedColumns
        )
        return result

    def getPKname(self, hiveDB, hiveTable):
        result = self.metastore.getPKname(hiveDB=hiveDB, hiveTable=hiveTable)
        return result

    def getFKs(self, hiveDB, hiveTable):
        result = self.metastore.getFKs(hiveDB=hiveDB, hiveTable=hiveTable)
        return result

    def removeLocksByForce(self, hiveDB, hiveTable):
        result = self.metastore.removeLocksByForce(hiveDB=hiveDB, hiveTable=hiveTable)
        return result

    def getTables(self, dbFilter=None, tableFilter=None):
        result = self.metastore.getTables(dbFilter=dbFilter, tableFilter=tableFilter)
        return result

    def getColumns(
        self,
        hiveDB,
        hiveTable,
        includeType=False,
        includeComment=False,
        includeIdx=False,
        forceColumnUppercase=False,
        excludeDataLakeColumns=False,
    ):
        result = self.metastore.getColumns(
            hiveDB=hiveDB,
            hiveTable=hiveTable,
            includeType=includeType,
            includeComment=includeComment,
            includeIdx=includeIdx,
            forceColumnUppercase=forceColumnUppercase,
        )

        if excludeDataLakeColumns == True:
            result = result[
                ~result["name"].isin(
                    [
                        self.Hive_ColumnName_Import,
                        self.Hive_ColumnName_Insert,
                        self.Hive_ColumnName_Update,
                        self.Hive_ColumnName_Delete,
                        self.Hive_ColumnName_IUD,
                        self.Hive_ColumnName_HistoryTimestamp,
                        self.Hive_ColumnName_Source,
                    ]
                )
            ]

            # Index needs to be reset if anything was droped in the DF.
            # The merge will otherwise indicate that there is a difference and table will be recreated
            result.reset_index(drop=True, inplace=True)

        return result

    def disconnectFromHive(self):
        logging.debug("Executing common_operations.disconnectFromHive()")
        if self.hive_cursor != None:
            self.hive_cursor.close()
            self.hive_conn.close()
            self.hive_cursor = None
            self.hive_conn = None

        logging.debug("Executing common_operations.disconnectFromHive() - Finished")

    def connectToHive(self, forceSkipTest=False, quiet=False):
        logging.debug("Executing common_operations.connectToHive()")

        # Make sure we only connect if we havent done so before. Reason is that this function can be called from many different places
        # due to the retry functionallity
        if self.hive_cursor == None:
            hive_server_list = self.hive_servers.split(",")
            random.shuffle(hive_server_list)

            for hive_server in hive_server_list:
                hive_hostname = hive_server.split(":")[0]
                hive_port = hive_server.split(":")[1]

                try:
                    if quiet == False:
                        logging.info(
                            "Connecting to Hive at %s:%s" % (hive_hostname, hive_port)
                        )

                    if self.hive_use_ssl == True:
                        transport = puretransport.transport_factory(
                            host=hive_hostname,
                            port=hive_port,
                            kerberos_service_name=self.hive_kerberos_service_name,
                            use_sasl=True,
                            sasl_auth="GSSAPI",
                            use_ssl=True,
                            username="dummy",
                            password="dummy",
                        )

                        self.hive_conn = hive.connect(
                            thrift_transport=transport,
                            configuration={"hive.llap.execution.mode": "none"},
                        )

                    else:
                        self.hive_conn = hive.connect(
                            host=hive_hostname,
                            port=hive_port,
                            database="default",
                            auth="KERBEROS",
                            kerberos_service_name=self.hive_kerberos_service_name,
                            configuration={"hive.llap.execution.mode": "none"},
                        )

                    self.hive_cursor = self.hive_conn.cursor()
                    self.hive_hostname = hive_hostname
                    self.hive_port = hive_port

                except TypeError:
                    logging.warning(
                        "Could not connect to Hive at %s:%s. This might be because the address was not resolvable in the DNS"
                        % (hive_hostname, hive_port)
                    )
                except hive.thrift.transport.TTransport.TTransportException:
                    logging.warning(
                        "Could not connect to Hive at %s:%s."
                        % (hive_hostname, hive_port)
                    )
                except:
                    logging.warning(
                        "General connection error to Hive at %s:%s."
                        % (hive_hostname, hive_port)
                    )
                    logging.warning(sys.exc_info())

                if self.hive_cursor != None:
                    break

            if self.hive_cursor == None:
                raise ValueError("Could not connect to any Hive Server.")

            if (
                self.common_config.getConfigValue(key="hive_validate_before_execution")
                == True
                and forceSkipTest == False
            ):
                self.testHiveQueryExecution()

        logging.debug("Executing common_operations.connectToHive() - Finished")

    def testHiveQueryExecution(
        self,
    ):
        logging.debug("Executing common_operations.testHiveQueryExecution()")
        logging.info("Checking Hive functionality by querying the reference table")

        hive_table = self.common_config.getConfigValue(key="hive_validate_table")

        result_df = self.executeHiveQuery(
            "select id, value, count(value) as counter from %s group by id, value order by id"
            % (hive_table)
        )
        reference_df = pd.DataFrame(
            {
                "id": [1, 2, 3, 4, 5],
                "value": ["Hive", "LLAP", "is", "working", "fine"],
                "counter": [1, 1, 1, 1, 1],
            }
        )

        if reference_df.equals(result_df) == False:
            logging.debug("Hive Query Result:\n%s" % (result_df))
            logging.debug("Reference table:\n%s" % (reference_df))
            raise ValueError(
                "The Hive Query Execution test failed. That means that the result from the reference table did not return the exact same result as it should"
            )
        logging.debug("Executing common_operations.testHiveQueryExecution() - Finished")

    def executeHiveQuery(self, query, quiet=False):
        logging.debug("Executing common_operations.executeHiveQuery()")
        # This function executes a Hive query and check that the result is correct. It works agains a pre-defined table

        # Sets the start time. We use this to determine elapsed time for the query
        hive_query_start_time = time.monotonic()

        # Execute the query in async mode
        try:
            self.hive_cursor.execute(query, async_=True)
        except exc.OperationalError as errMsg:
            sql_error_message_position = str(errMsg).find(" errorMessage=")
            if sql_error_message_position > 0:
                sql_error_message = str(errMsg)[sql_error_message_position:]
                first_quote_position = sql_error_message.find('"')
                second_quote_position = sql_error_message[
                    first_quote_position + 1 :
                ].find('"')
                sql_error_message = sql_error_message[
                    first_quote_position + 1 : first_quote_position
                    + second_quote_position
                    + 1
                ]
                raise SQLerror(sql_error_message)
            else:
                raise SQLerror(errMsg)

        status = self.hive_cursor.poll().operationState
        headers = self.hive_cursor.poll().progressUpdateResponse.headerNames

        # Read and print the output from the async Hive query
        result_df = None
        first_output_line = True
        errors_found = False
        lines_to_jump_up = 0
        yarn_application_id = None
        while status in (
            TOperationState.INITIALIZED_STATE,
            TOperationState.RUNNING_STATE,
        ):
            # If the user configured to print the logs, we do it here
            logs = self.hive_cursor.fetch_logs()
            for message in logs:
                if self.hive_print_messages == True:
                    print(message)
                else:
                    if "Executing on YARN cluster with App id" in message:
                        print(message)
                        if yarn_application_id == None:
                            # In rare cases, this happened twice with duplicate key error. With this IF statement, that problem is gone
                            yarn_application_id = (
                                "application_"
                                + message.split("application_")[1].split(")")[0]
                            )
                            self.common_config.updateYarnStatistics(
                                yarn_application_id, "hive"
                            )

                if re.search("^ERROR ", message):
                    logging.error("An ERROR occured in the Hive execution")
                    logging.error(message)
                    self.hive_print_messages = (
                        True  # Turn on logging if there is an error
                    )
                    errors_found = True

            # Print the progress of the query
            for row in self.hive_cursor.poll().progressUpdateResponse.rows:
                if first_output_line == True:
                    # If it's the first one, we print the header and read how many vertexes there is
                    lines_to_jump_up = (
                        len(self.hive_cursor.poll().progressUpdateResponse.rows) + 1
                    )
                    print("\u001b[36m")  # Cyan
                    try:
                        print(
                            "%-16s %12s %12s %11s %11s %9s %9s %8s %8s"
                            % (
                                headers[0],
                                headers[1],
                                headers[2],
                                headers[3],
                                headers[4],
                                headers[5],
                                headers[6],
                                headers[7],
                                headers[8],
                            )
                        )
                    except IndexError:
                        print(headers)
                    sys.stdout.write("\u001b[0m")  # Reset
                    sys.stdout.flush()
                    first_output_line = False

                # Print the actual vertex information of the query
                try:
                    print(
                        "%-16s %12s %12s %11s %11s %9s %9s %8s %8s"
                        % (
                            row[0],
                            row[1],
                            row[2],
                            row[3],
                            row[4],
                            row[5],
                            row[6],
                            row[7],
                            row[8],
                        )
                    )
                except IndexError:
                    print(headers)

            sys.stdout.write("\u001b[31m")  # Red
            sys.stdout.flush()
            print(
                "ELAPSED TIME: %.2f seconds"
                % (time.monotonic() - hive_query_start_time)
            )
            sys.stdout.write("\u001b[0m")  # Reset
            sys.stdout.flush()

            if first_output_line == False:
                # This means that we printed atleast one chunk of mappers and reducers. Because of that, we will now move the cursor up
                # This information was found on http://www.lihaoyi.com/post/BuildyourownCommandLinewithANSIescapecodes.html
                sys.stdout.write(
                    "\u001b[" + str(lines_to_jump_up) + "A" + "\u001b[1000D"
                )
                sys.stdout.flush()

            status = self.hive_cursor.poll().operationState

        # At this point, the query is not running anymore. We now print the last result of the Vertex informaton
        rows_printed = False
        yarn_containers_total = 0
        yarn_containers_failed = 0
        yarn_containers_killed = 0
        for row in self.hive_cursor.poll().progressUpdateResponse.rows:
            try:
                print(
                    "%-16s %12s %12s %11s %11s %9s %9s %8s %8s"
                    % (
                        row[0],
                        row[1],
                        row[2],
                        row[3],
                        row[4],
                        row[5],
                        row[6],
                        row[7],
                        row[8],
                    )
                )
                if row[2] == "SUCCEEDED":
                    yarn_containers_total += int(row[3])
                    yarn_containers_failed += int(row[7])
                    yarn_containers_killed += int(row[8])
            except IndexError:
                print(headers)
            rows_printed = True
        if rows_printed == True:
            print("")  # So we get passed the elapsed time message
        print("")

        if yarn_application_id != None:
            self.common_config.updateYarnStatistics(
                yarn_application_id,
                "hive",
                yarn_containers_total,
                yarn_containers_failed,
                yarn_containers_killed,
            )
        # 			print("yarnApplicationID: %s"%yarnApplicationID)
        # 			print("yarnContainersTotal: %s"%yarnContainersTotal)
        # 			print("yarnContainersFailed: %s"%yarnContainersFailed)
        # 			print("yarnContainersKilled: %s"%yarnContainersKilled)
        # 			print("")

        # If the user configured to print the logs, we do it here
        logs = self.hive_cursor.fetch_logs()
        if self.hive_print_messages == True:
            for message in logs:
                print(message)
            print("")
        else:
            for message in logs:
                if message.lower().startswith("error"):
                    errors_found = True
                if errors_found == True:
                    print(message)

        if errors_found == True:
            raise Exception("Hive Query Error")

        result_df = pd.DataFrame()
        # Retreive the data
        try:
            result_df = pd.DataFrame(self.hive_cursor.fetchall())

            # Set the correct column namnes in the DataFrame
            result_df_columns = []
            for columns in self.hive_cursor.description:
                result_df_columns.append(
                    columns[0]
                )  # Name of the column is in the first position
            result_df.columns = result_df_columns
        except exc.ProgrammingError:
            logging.debug(
                "An error was raised during hive_cursor.fetchall(). This happens during SQL operations that dont return any rows like 'create table'"
            )

        logging.debug("Executing common_operations.executeHiveQuery() - Finished")
        return result_df

    def executeBeelineScript(self, hive_script_file, use_db=None):
        logging.debug("Executing common_operations.executeBeelineScript()")

        if use_db == None:
            use_db = ""

        # Connect to Hive to get a working server in the list of Hive servers
        # This sets the self.hive_hostname and self.hive_port so that beeline can connect to a working Hive server
        self.connectToHive(forceSkipTest=True, quiet=True)
        self.disconnectFromHive()

        if self.hive_use_ssl == False:
            beeline_connect_str = "jdbc:hive2://%s:%s/%s;principal=%s/_HOST@%s" % (
                self.hive_hostname,
                self.hive_port,
                use_db,
                self.hive_kerberos_service_name,
                self.hive_kerberos_realm,
            )
        else:
            beeline_connect_str = (
                "jdbc:hive2://%s:%s/%s;ssl=true;principal=%s/_HOST@%s"
                % (
                    self.hive_hostname,
                    self.hive_port,
                    use_db,
                    self.hive_kerberos_service_name,
                    self.hive_kerberos_realm,
                )
            )

        logging.debug("JDBC Connection String: %s" % (beeline_connect_str))

        beeline_command = [
            "beeline",
            "--silent=false",
            "-u",
            beeline_connect_str,
            "-f",
            hive_script_file,
        ]

        # Start Beeline
        sh_session = subprocess.Popen(
            beeline_command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
        )

        # Print Stdout and stderr while beeline is running
        while sh_session.poll() == None:
            row = sh_session.stdout.readline().decode("utf-8").rstrip()
            if row != "":
                print(row)
                sys.stdout.flush()

        # Print what is left in output after beeline finished
        for row in sh_session.stdout.readlines():
            row = row.decode("utf-8").rstrip()
            if row != "":
                print(row)
                sys.stdout.flush()

        return sh_session.returncode

        logging.debug("Executing common_operations.executeBeelineScript() - Finished")

    def getHiveTableRowCount(self, hiveDB, hiveTable, whereStatement=None):
        logging.debug("Executing common_operations.getHiveTableRowCount()")
        logging.info("Reading the number of rows from %s.%s" % (hiveDB, hiveTable))
        row_count = 0

        query = "select count(1) as rowcount from `%s`.`%s` " % (hiveDB, hiveTable)
        if whereStatement != None:
            query += "where " + whereStatement
        else:
            query += "limit 1"

        result_df = self.executeHiveQuery(query)
        row_count = int(result_df["rowcount"].iloc[0])
        logging.debug("Rowcount from %s.%s: %s" % (hiveDB, hiveTable, row_count))

        logging.debug("Executing common_operations.getHiveTableRowCount() - Finished")
        return row_count

    def dropHiveTable(self, hive_db, hive_table):
        logging.debug("Executing common_operations.dropHiveTable()")
        logging.info("Dropping table %s.%s" % (hive_db, hive_table))

        self.connectToHive(forceSkipTest=True, quiet=True)
        self.executeHiveQuery("drop table if exists `%s`.`%s`" % (hive_db, hive_table))

        logging.debug("Executing common_operations.dropHiveTable() - Finished")

    def truncateHiveTable(self, hive_db, hive_table):
        """Truncates a Hive table"""
        logging.debug("Executing common_operations.truncateHiveTable()")

        self.executeHiveQuery("truncate table `%s`.`%s`" % (hive_db, hive_table))
        logging.debug("Executing common_operations.truncateHiveTable() - Finished")

    def updateHiveTableStatistics(self, hive_db, hive_table):
        """Compute Hive statistics on a Hive table"""
        logging.debug("Executing common_operations.updateHiveTableStatistics()")

        self.executeHiveQuery(
            "analyze table `%s`.`%s` compute statistics " % (hive_db, hive_table)
        )
        logging.debug(
            "Executing common_operations.updateHiveTableStatistics() - Finished"
        )

    def runHiveMajorCompaction(self, hiveDB, hiveTable):
        """Force a major compaction on a Hive table"""
        logging.debug("Executing common_operations.runHiveMajorCompaction()")

        if (
            self.common_config.getConfigValue(key="hive_major_compact_after_merge")
            == True
        ):
            logging.info("Running a Major compaction on Hive table")
            try:
                self.executeHiveQuery(
                    "alter table `%s`.`%s` compact 'major' " % (hiveDB, hiveTable)
                )
            except:
                logging.error(
                    "Major compaction failed with the following error message:"
                )
                logging.warning(sys.exc_info())
                pass

        logging.debug("Executing common_operations.runHiveMajorCompaction() - Finished")

    def convertHiveTableToACID(
        self, hiveDB, hiveTable, createDeleteColumn=False, createMergeColumns=False
    ):
        """Checks if a table is ACID or not, and it it isn't, it converts the table to an ACID table"""
        # TODO: HDP3 shouldnt run this
        logging.debug("Executing common_operations.convertHiveTableToACID()")
        logging.info("Converting table to an ACID & Transactional enabled table")

        buckets = 1
        hive_table_bucketed = "%s_hive_bucketed" % (hiveTable)
        logging.info("The new table will use %s buckets" % (buckets))

        if self.checkTable(hiveDB=hiveDB, hiveTable=hive_table_bucketed) == True:
            logging.warning(
                "Temporary table used during conversion to ACID already exists. We will drop this table and recreate it again"
            )
            self.executeHiveQuery(
                "drop table `%s`.`%s` " % (hiveDB, hive_table_bucketed)
            )

        column_df = self.getColumns(
            hiveDB=hiveDB, hiveTable=hiveTable, includeType=True, includeComment=True
        )
        hive_columns_create = []
        hive_columns_insert = []
        hive_columns_create_adding = []
        hive_columns_insert_adding = []
        datalake_import_exists = False

        pk_columns = self.getPK(hiveDB=hiveDB, hiveTable=hiveTable, quotedColumns=True)
        if pk_columns == None or pk_columns == "":
            logging.error(
                "There are no PrimaryKey defined on the table. Please create one first before converting to ACID (PK is needed for bucket columns)"
            )
            self.common_config.remove_temporary_files()
            sys.exit(1)

        for index, row in column_df.iterrows():
            column_name = row["name"]
            column_type = row["type"]
            column_comment = row["comment"]

            # if columnName in ["datalake_import"] and createMergeColumns == True:
            if (
                column_name in [self.Hive_ColumnName_Import]
                and createMergeColumns == True
            ):
                datalake_import_exists = True
                continue

            if column_comment != None and column_comment != "":
                hive_columns_create.append(
                    '%s %s COMMENT "%s"' % (column_name, column_type, column_comment)
                )
            else:
                hive_columns_create.append("%s %s" % (column_name, column_type))
            hive_columns_insert.append(column_name)

        if createMergeColumns == True:
            # hiveColumnsCreateAdding.append("datalake_iud char(1) COMMENT \"Last operation of this record was I=Insert, U=Update or D=Delete\"")
            # hiveColumnsCreateAdding.append("datalake_insert timestamp COMMENT \"Timestamp for insert in Datalake\"")
            # hiveColumnsCreateAdding.append("datalake_update timestamp COMMENT \"Timestamp for last update in Datalake\"")
            # hiveColumnsInsertAdding.append("datalake_iud")
            # hiveColumnsInsertAdding.append("datalake_insert")
            # hiveColumnsInsertAdding.append("datalake_update")
            hive_columns_create_adding.append(
                '%s char(1) COMMENT "Last operation of this record was I=Insert, U=Update or D=Delete"'
                % (self.Hive_ColumnName_IUD)
            )
            hive_columns_create_adding.append(
                '%s timestamp COMMENT "Timestamp for insert in Datalake"'
                % (self.Hive_ColumnName_Insert)
            )
            hive_columns_create_adding.append(
                '%s timestamp COMMENT "Timestamp for last update in Datalake"'
                % (self.Hive_ColumnName_Update)
            )
            hive_columns_insert_adding.append(self.Hive_ColumnName_IUD)
            hive_columns_insert_adding.append(self.Hive_ColumnName_Insert)
            hive_columns_insert_adding.append(self.Hive_ColumnName_Update)

            if createDeleteColumn == True:
                # hiveColumnsCreateAdding.append("datalake_delete timestamp COMMENT \"Timestamp for soft delete in Datalake\"")
                # hiveColumnsInsertAdding.append("datalake_delete")
                hive_columns_create_adding.append(
                    '%s timestamp COMMENT "Timestamp for soft delete in Datalake"'
                    % (self.Hive_ColumnName_Delete)
                )
                hive_columns_insert_adding.append(self.Hive_ColumnName_Delete)

        # Create the Bucketed table
        query = "create table `%s`.`%s` " % (hiveDB, hive_table_bucketed)
        query += "( %s" % (", ".join(hive_columns_create))
        if createMergeColumns == True:
            query += ", %s" % (", ".join(hive_columns_create_adding))
        query += (
            ") clustered by (%s) into %s buckets stored as orc tblproperties ('orc.compress'='ZLIB', 'transactional'='true')"
            % (pk_columns, buckets)
        )
        self.executeHiveQuery(query)

        # Insert data into the Bucketed table
        query = "insert into `%s`.`%s` " % (hiveDB, hive_table_bucketed)
        query += "( %s" % (", ".join(hive_columns_insert))
        if createMergeColumns == True:
            query += ", %s" % (", ".join(hive_columns_insert_adding))
        query += ") "
        query += "select "
        query += "%s" % (", ".join(hive_columns_insert))
        if createMergeColumns == True:
            if datalake_import_exists == True:
                # query += ", 'I', datalake_import, datalake_import"
                query += ", 'I', %s, %s" % (
                    self.Hive_ColumnName_Import,
                    self.Hive_ColumnName_Import,
                )
            else:
                query += ', \'I\', timestamp("1900-01-01 00:00:00.000"), timestamp("1900-01-01 00:00:00.000")'

            if createDeleteColumn == True:
                query += ", NULL"
        query += " from `%s`.`%s` " % (hiveDB, hiveTable)
        self.executeHiveQuery(query)

        # Drop Target table
        query = "drop table `%s`.`%s` " % (hiveDB, hiveTable)
        self.executeHiveQuery(query)

        # Rename bucketed table to Target table
        query = "alter table `%s`.`%s` rename to `%s`.`%s`" % (
            hiveDB,
            hive_table_bucketed,
            hiveDB,
            hiveTable,
        )
        self.executeHiveQuery(query)

        self.reconnectHiveMetaStore()

        logging.debug("Executing common_operations.convertHiveTableToACID() - Finished")

    def getHiveColumnNameDiff(
        self,
        source_db,
        source_table,
        target_db,
        target_table,
        import_tool,
        source_is_import_table=False,
    ):
        """Returns a dataframe that includes the diff between the source and target table. If sourceIsImportTable=True, then there is additional columns that contains the correct name for the import table columns"""
        logging.debug("Executing common_operations.getHiveColumnNameDiff()")

        source_columns = self.getColumns(
            hiveDB=source_db,
            hiveTable=source_table,
            includeType=False,
            includeComment=False,
        )
        target_columns = self.getColumns(
            hiveDB=target_db,
            hiveTable=target_table,
            includeType=False,
            includeComment=False,
        )

        if source_is_import_table == True:
            # Logic here is to create a new column in both DF and call them sourceName vs targetName. These are the original names. Then we replace the values in targetColumn DF column col
            # with the name that the column should be called in the source system. This is needed to handle characters that is not supported in Parquet files, like SPACE
            source_columns["sourceName"] = source_columns["name"]
            target_columns["targetName"] = target_columns["name"]

            if import_tool != "spark":
                # If you change any of the name replace operations, you also need to change the same data in functions
                # import_operations.updateColumnsForImportTable() and import_definitions.saveColumnData()
                target_columns["name"] = target_columns["name"].str.replace(
                    r" ", "_", regex=True
                )
                target_columns["name"] = target_columns["name"].str.replace(
                    r"\%", "pct", regex=True
                )
                target_columns["name"] = target_columns["name"].str.replace(
                    r"\(", "_", regex=True
                )
                target_columns["name"] = target_columns["name"].str.replace(
                    r"\)", "_", regex=True
                )
                target_columns["name"] = target_columns["name"].str.replace(
                    r"/", "", regex=True
                )
                target_columns["name"] = target_columns["name"].str.replace(
                    r"ü", "u", regex=True
                )
                target_columns["name"] = target_columns["name"].str.replace(
                    r"å", "a", regex=True
                )
                target_columns["name"] = target_columns["name"].str.replace(
                    r"ä", "a", regex=True
                )
                target_columns["name"] = target_columns["name"].str.replace(
                    r"ö", "o", regex=True
                )
                target_columns["name"] = target_columns["name"].str.replace(
                    r"#", "hash", regex=True
                )
                target_columns["name"] = target_columns["name"].str.replace(
                    r"^_", "underscore_", regex=True
                )

        column_merge = pd.merge(
            source_columns, target_columns, on=None, how="outer", indicator="Exist"
        )
        logging.debug("\n%s" % (column_merge))

        logging.debug("Executing common_operations.getHiveColumnNameDiff() - Finished")
        return column_merge
