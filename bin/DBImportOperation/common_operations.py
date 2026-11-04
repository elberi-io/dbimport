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


class operation(object, metaclass=Singleton):
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
        Hive_Import_DB,
        Hive_Import_Table,
        Hive_Import_View,
        Hive_History_DB,
        Hive_History_Table,
        Hive_HistoryTemp_DB,
        Hive_HistoryTemp_Table,
        Hive_Import_PKonly_DB,
        Hive_Import_PKonly_Table,
        Hive_Delete_DB,
        Hive_Delete_Table,
        Hive_ColumnName_Import,
        Hive_ColumnName_Insert,
        Hive_ColumnName_Update,
        Hive_ColumnName_Delete,
        Hive_ColumnName_IUD,
        Hive_ColumnName_HistoryTimestamp,
        Hive_ColumnName_Source,
    ):
        """Sets the name of the databases, tables and columns"""

        self.Hive_Import_DB = Hive_Import_DB
        self.Hive_Import_Table = Hive_Import_Table
        self.Hive_Import_View = Hive_Import_View
        self.Hive_History_DB = Hive_History_DB
        self.Hive_History_Table = Hive_History_Table
        self.Hive_HistoryTemp_DB = Hive_HistoryTemp_DB
        self.Hive_HistoryTemp_Table = Hive_HistoryTemp_Table
        self.Hive_Import_PKonly_DB = Hive_Import_PKonly_DB
        self.Hive_Import_PKonly_Table = Hive_Import_PKonly_Table
        self.Hive_Delete_DB = Hive_Delete_DB
        self.Hive_Delete_Table = Hive_Delete_Table
        self.Hive_ColumnName_Import = Hive_ColumnName_Import
        self.Hive_ColumnName_Insert = Hive_ColumnName_Insert
        self.Hive_ColumnName_Update = Hive_ColumnName_Update
        self.Hive_ColumnName_Delete = Hive_ColumnName_Delete
        self.Hive_ColumnName_IUD = Hive_ColumnName_IUD
        self.Hive_ColumnName_HistoryTimestamp = Hive_ColumnName_HistoryTimestamp
        self.Hive_ColumnName_Source = Hive_ColumnName_Source

    def checkHiveMetaStore(self):
        logging.debug("Executing common_operations.checkHiveMetaStore()")

        # Loop through all Hive webHCat servers from the configuration and checks if one of them works
        self.hiveMetaStore = None
        webHCatResponse = None
        for HiveURL in self.hiveMetaStoreList:
            try:
                URL = HiveURL.strip() + "/templeton/v1/status"
                logging.debug("Testing status for webHCat on URL: %s" % (URL))
                webHCatResponse = requests.get(URL, auth=self.webHCatAuth)
            except:
                continue
            if webHCatResponse.status_code == 200:
                self.hiveMetaStore = HiveURL.strip()
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
            hiveServerList = self.hive_servers.split(",")
            random.shuffle(hiveServerList)

            for hive_server in hiveServerList:
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

        hiveTable = self.common_config.getConfigValue(key="hive_validate_table")

        result_df = self.executeHiveQuery(
            "select id, value, count(value) as counter from %s group by id, value order by id"
            % (hiveTable)
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
        hiveQueryStartTime = time.monotonic()

        # Execute the query in async mode
        try:
            self.hive_cursor.execute(query, async_=True)
        except exc.OperationalError as errMsg:
            SQLerrorMessagePosition = str(errMsg).find(" errorMessage=")
            if SQLerrorMessagePosition > 0:
                SQLerrorMessage = str(errMsg)[SQLerrorMessagePosition:]
                firstQuotePosition = SQLerrorMessage.find('"')
                secondQuotePosition = SQLerrorMessage[firstQuotePosition + 1 :].find(
                    '"'
                )
                SQLerrorMessage = SQLerrorMessage[
                    firstQuotePosition + 1 : firstQuotePosition
                    + secondQuotePosition
                    + 1
                ]
                raise SQLerror(SQLerrorMessage)
            else:
                raise SQLerror(errMsg)

        status = self.hive_cursor.poll().operationState
        headers = self.hive_cursor.poll().progressUpdateResponse.headerNames

        # Read and print the output from the async Hive query
        result_df = None
        firstOutputLine = True
        errorsFound = False
        linesToJumpUp = 0
        yarnApplicationID = None
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
                        if yarnApplicationID == None:
                            # In rare cases, this happened twice with duplicate key error. With this IF statement, that problem is gone
                            yarnApplicationID = (
                                "application_"
                                + message.split("application_")[1].split(")")[0]
                            )
                            self.common_config.updateYarnStatistics(
                                yarnApplicationID, "hive"
                            )

                if re.search("^ERROR ", message):
                    logging.error("An ERROR occured in the Hive execution")
                    logging.error(message)
                    self.hive_print_messages = (
                        True  # Turn on logging if there is an error
                    )
                    errorsFound = True

            # Print the progress of the query
            for row in self.hive_cursor.poll().progressUpdateResponse.rows:
                if firstOutputLine == True:
                    # If it's the first one, we print the header and read how many vertexes there is
                    linesToJumpUp = (
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
                    firstOutputLine = False

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
                "ELAPSED TIME: %.2f seconds" % (time.monotonic() - hiveQueryStartTime)
            )
            sys.stdout.write("\u001b[0m")  # Reset
            sys.stdout.flush()

            if firstOutputLine == False:
                # This means that we printed atleast one chunk of mappers and reducers. Because of that, we will now move the cursor up
                # This information was found on http://www.lihaoyi.com/post/BuildyourownCommandLinewithANSIescapecodes.html
                sys.stdout.write("\u001b[" + str(linesToJumpUp) + "A" + "\u001b[1000D")
                sys.stdout.flush()

            status = self.hive_cursor.poll().operationState

        # At this point, the query is not running anymore. We now print the last result of the Vertex informaton
        rowsPrinted = False
        yarnContainersTotal = 0
        yarnContainersFailed = 0
        yarnContainersKilled = 0
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
                    yarnContainersTotal += int(row[3])
                    yarnContainersFailed += int(row[7])
                    yarnContainersKilled += int(row[8])
            except IndexError:
                print(headers)
            rowsPrinted = True
        if rowsPrinted == True:
            print("")  # So we get passed the elapsed time message
        print("")

        if yarnApplicationID != None:
            self.common_config.updateYarnStatistics(
                yarnApplicationID,
                "hive",
                yarnContainersTotal,
                yarnContainersFailed,
                yarnContainersKilled,
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
                    errorsFound = True
                if errorsFound == True:
                    print(message)

        if errorsFound == True:
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

    def executeBeelineScript(self, hiveScriptFile, useDB=None):
        logging.debug("Executing common_operations.executeBeelineScript()")

        if useDB == None:
            useDB = ""

        # Connect to Hive to get a working server in the list of Hive servers
        # This sets the self.hive_hostname and self.hive_port so that beeline can connect to a working Hive server
        self.connectToHive(forceSkipTest=True, quiet=True)
        self.disconnectFromHive()

        if self.hive_use_ssl == False:
            beelineConnectStr = "jdbc:hive2://%s:%s/%s;principal=%s/_HOST@%s" % (
                self.hive_hostname,
                self.hive_port,
                useDB,
                self.hive_kerberos_service_name,
                self.hive_kerberos_realm,
            )
        else:
            beelineConnectStr = (
                "jdbc:hive2://%s:%s/%s;ssl=true;principal=%s/_HOST@%s"
                % (
                    self.hive_hostname,
                    self.hive_port,
                    useDB,
                    self.hive_kerberos_service_name,
                    self.hive_kerberos_realm,
                )
            )

        logging.debug("JDBC Connection String: %s" % (beelineConnectStr))

        beelineCommand = [
            "beeline",
            "--silent=false",
            "-u",
            beelineConnectStr,
            "-f",
            hiveScriptFile,
        ]

        # Start Beeline
        sh_session = subprocess.Popen(
            beelineCommand, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
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
        rowCount = 0

        query = "select count(1) as rowcount from `%s`.`%s` " % (hiveDB, hiveTable)
        if whereStatement != None:
            query += "where " + whereStatement
        else:
            query += "limit 1"

        result_df = self.executeHiveQuery(query)
        rowCount = int(result_df["rowcount"].iloc[0])
        logging.debug("Rowcount from %s.%s: %s" % (hiveDB, hiveTable, rowCount))

        logging.debug("Executing common_operations.getHiveTableRowCount() - Finished")
        return rowCount

    def dropHiveTable(self, hiveDB, hiveTable):
        logging.debug("Executing common_operations.dropHiveTable()")
        logging.info("Dropping table %s.%s" % (hiveDB, hiveTable))

        self.connectToHive(forceSkipTest=True, quiet=True)
        self.executeHiveQuery("drop table if exists `%s`.`%s`" % (hiveDB, hiveTable))

        logging.debug("Executing common_operations.dropHiveTable() - Finished")

    def truncateHiveTable(self, hiveDB, hiveTable):
        """Truncates a Hive table"""
        logging.debug("Executing common_operations.truncateHiveTable()")

        self.executeHiveQuery("truncate table `%s`.`%s`" % (hiveDB, hiveTable))
        logging.debug("Executing common_operations.truncateHiveTable() - Finished")

    def updateHiveTableStatistics(self, hiveDB, hiveTable):
        """Compute Hive statistics on a Hive table"""
        logging.debug("Executing common_operations.updateHiveTableStatistics()")

        self.executeHiveQuery(
            "analyze table `%s`.`%s` compute statistics " % (hiveDB, hiveTable)
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
        hiveTableBucketed = "%s_hive_bucketed" % (hiveTable)
        logging.info("The new table will use %s buckets" % (buckets))

        if self.checkTable(hiveDB=hiveDB, hiveTable=hiveTableBucketed) == True:
            logging.warning(
                "Temporary table used during conversion to ACID already exists. We will drop this table and recreate it again"
            )
            self.executeHiveQuery("drop table `%s`.`%s` " % (hiveDB, hiveTableBucketed))

        columnDF = self.getColumns(
            hiveDB=hiveDB, hiveTable=hiveTable, includeType=True, includeComment=True
        )
        hiveColumnsCreate = []
        hiveColumnsInsert = []
        hiveColumnsCreateAdding = []
        hiveColumnsInsertAdding = []
        datalakeImportExists = False

        PKColumns = self.getPK(hiveDB=hiveDB, hiveTable=hiveTable, quotedColumns=True)
        if PKColumns == None or PKColumns == "":
            logging.error(
                "There are no PrimaryKey defined on the table. Please create one first before converting to ACID (PK is needed for bucket columns)"
            )
            self.common_config.remove_temporary_files()
            sys.exit(1)

        for index, row in columnDF.iterrows():
            columnName = row["name"]
            columnType = row["type"]
            columnComment = row["comment"]

            # if columnName in ["datalake_import"] and createMergeColumns == True:
            if (
                columnName in [self.Hive_ColumnName_Import]
                and createMergeColumns == True
            ):
                datalakeImportExists = True
                continue

            if columnComment != None and columnComment != "":
                hiveColumnsCreate.append(
                    '%s %s COMMENT "%s"' % (columnName, columnType, columnComment)
                )
            else:
                hiveColumnsCreate.append("%s %s" % (columnName, columnType))
            hiveColumnsInsert.append(columnName)

        if createMergeColumns == True:
            # hiveColumnsCreateAdding.append("datalake_iud char(1) COMMENT \"Last operation of this record was I=Insert, U=Update or D=Delete\"")
            # hiveColumnsCreateAdding.append("datalake_insert timestamp COMMENT \"Timestamp for insert in Datalake\"")
            # hiveColumnsCreateAdding.append("datalake_update timestamp COMMENT \"Timestamp for last update in Datalake\"")
            # hiveColumnsInsertAdding.append("datalake_iud")
            # hiveColumnsInsertAdding.append("datalake_insert")
            # hiveColumnsInsertAdding.append("datalake_update")
            hiveColumnsCreateAdding.append(
                '%s char(1) COMMENT "Last operation of this record was I=Insert, U=Update or D=Delete"'
                % (self.Hive_ColumnName_IUD)
            )
            hiveColumnsCreateAdding.append(
                '%s timestamp COMMENT "Timestamp for insert in Datalake"'
                % (self.Hive_ColumnName_Insert)
            )
            hiveColumnsCreateAdding.append(
                '%s timestamp COMMENT "Timestamp for last update in Datalake"'
                % (self.Hive_ColumnName_Update)
            )
            hiveColumnsInsertAdding.append(self.Hive_ColumnName_IUD)
            hiveColumnsInsertAdding.append(self.Hive_ColumnName_Insert)
            hiveColumnsInsertAdding.append(self.Hive_ColumnName_Update)

            if createDeleteColumn == True:
                # hiveColumnsCreateAdding.append("datalake_delete timestamp COMMENT \"Timestamp for soft delete in Datalake\"")
                # hiveColumnsInsertAdding.append("datalake_delete")
                hiveColumnsCreateAdding.append(
                    '%s timestamp COMMENT "Timestamp for soft delete in Datalake"'
                    % (self.Hive_ColumnName_Delete)
                )
                hiveColumnsInsertAdding.append(self.Hive_ColumnName_Delete)

        # Create the Bucketed table
        query = "create table `%s`.`%s` " % (hiveDB, hiveTableBucketed)
        query += "( %s" % (", ".join(hiveColumnsCreate))
        if createMergeColumns == True:
            query += ", %s" % (", ".join(hiveColumnsCreateAdding))
        query += (
            ") clustered by (%s) into %s buckets stored as orc tblproperties ('orc.compress'='ZLIB', 'transactional'='true')"
            % (PKColumns, buckets)
        )
        self.executeHiveQuery(query)

        # Insert data into the Bucketed table
        query = "insert into `%s`.`%s` " % (hiveDB, hiveTableBucketed)
        query += "( %s" % (", ".join(hiveColumnsInsert))
        if createMergeColumns == True:
            query += ", %s" % (", ".join(hiveColumnsInsertAdding))
        query += ") "
        query += "select "
        query += "%s" % (", ".join(hiveColumnsInsert))
        if createMergeColumns == True:
            if datalakeImportExists == True:
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
            hiveTableBucketed,
            hiveDB,
            hiveTable,
        )
        self.executeHiveQuery(query)

        self.reconnectHiveMetaStore()

        logging.debug("Executing common_operations.convertHiveTableToACID() - Finished")

    def getHiveColumnNameDiff(
        self,
        sourceDB,
        sourceTable,
        targetDB,
        targetTable,
        importTool,
        sourceIsImportTable=False,
    ):
        """Returns a dataframe that includes the diff between the source and target table. If sourceIsImportTable=True, then there is additional columns that contains the correct name for the import table columns"""
        logging.debug("Executing common_operations.getHiveColumnNameDiff()")

        sourceColumns = self.getColumns(
            hiveDB=sourceDB,
            hiveTable=sourceTable,
            includeType=False,
            includeComment=False,
        )
        targetColumns = self.getColumns(
            hiveDB=targetDB,
            hiveTable=targetTable,
            includeType=False,
            includeComment=False,
        )

        if sourceIsImportTable == True:
            # Logic here is to create a new column in both DF and call them sourceName vs targetName. These are the original names. Then we replace the values in targetColumn DF column col
            # with the name that the column should be called in the source system. This is needed to handle characters that is not supported in Parquet files, like SPACE
            sourceColumns["sourceName"] = sourceColumns["name"]
            targetColumns["targetName"] = targetColumns["name"]

            if importTool != "spark":
                # If you change any of the name replace operations, you also need to change the same data in functions
                # import_operations.updateColumnsForImportTable() and import_definitions.saveColumnData()
                targetColumns["name"] = targetColumns["name"].str.replace(
                    r" ", "_", regex=True
                )
                targetColumns["name"] = targetColumns["name"].str.replace(
                    r"\%", "pct", regex=True
                )
                targetColumns["name"] = targetColumns["name"].str.replace(
                    r"\(", "_", regex=True
                )
                targetColumns["name"] = targetColumns["name"].str.replace(
                    r"\)", "_", regex=True
                )
                targetColumns["name"] = targetColumns["name"].str.replace(
                    r"/", "", regex=True
                )
                targetColumns["name"] = targetColumns["name"].str.replace(
                    r"ü", "u", regex=True
                )
                targetColumns["name"] = targetColumns["name"].str.replace(
                    r"å", "a", regex=True
                )
                targetColumns["name"] = targetColumns["name"].str.replace(
                    r"ä", "a", regex=True
                )
                targetColumns["name"] = targetColumns["name"].str.replace(
                    r"ö", "o", regex=True
                )
                targetColumns["name"] = targetColumns["name"].str.replace(
                    r"#", "hash", regex=True
                )
                targetColumns["name"] = targetColumns["name"].str.replace(
                    r"^_", "underscore_", regex=True
                )

        columnMerge = pd.merge(
            sourceColumns, targetColumns, on=None, how="outer", indicator="Exist"
        )
        logging.debug("\n%s" % (columnMerge))

        logging.debug("Executing common_operations.getHiveColumnNameDiff() - Finished")
        return columnMerge
