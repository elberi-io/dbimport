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
    def __init__(self, hive_db=None, hive_table=None):
        logging.debug("Executing common_operation.__init__()")

        self.hive_db = hive_db
        self.hive_table = hive_table
        self.hive_conn = None
        self.hive_cursor = None
        self.spark_catalog_name = None
        self.debug_log_level = False

        if logging.root.level == 10:  # DEBUG
            self.debug_log_level = True

        # Fetch and initialize the Kerberos configuration
        self.kerberos_principal = configuration.get("Kerberos", "principal")
        self.web_hcat_auth = HTTPKerberosAuth(
            force_preemptive=True, principal=self.kerberos_principal
        )

        self.common_config = common_config.config()

        # Fetch configuration details about Hive
        self.hive_servers = configuration.get("Hive", "servers")
        self.hive_kerberos_service_name = configuration.get(
            "Hive", "kerberos_service_name"
        )
        self.hive_kerberos_realm = configuration.get("Hive", "kerberos_realm")
        self.hive_print_messages = self.common_config.get_config_value(
            key="hive_print_messages"
        )
        if configuration.get("Hive", "use_ssl").lower() == "true":
            self.hive_use_ssl = True
        else:
            self.hive_use_ssl = False

        # HDFS Settings
        self.hdfs_address = self.common_config.get_config_value(key="hdfs_address")
        self.hdfs_basedir = self.common_config.get_config_value(key="hdfs_basedir")
        self.hdfs_blocksize = self.common_config.get_config_value(key="hdfs_blocksize")

        # Metastore connections
        self.metastore_type = configuration.get("Metastore", "metastore_type")
        if self.metastore_type == constant.CATALOG_HIVE_DIRECT:
            self.metastore = hiveDirect.hiveMetastoreDirect()
            self.spark_catalog_name = "hive"
        elif self.metastore_type == constant.CATALOG_GLUE:
            self.metastore = glue.glueCatalog()
            self.spark_catalog_name = "glue"
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

    def reconnect_hive_meta_store(self):
        logging.debug(
            "Reconnecting to Hive Metastore should not be needed anymore. Code is not really doing anything"
        )

    def set_hive_table(self, hive_db, hive_table):
        """Sets the parameters to work against a new Hive database and table"""
        self.hive_db = hive_db.lower()
        self.hive_table = hive_table.lower()

    def set_table_and_column_names(
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

        self.hive_import_db = hive_import_db
        self.hive_import_table = hive_import_table
        self.hive_import_view = hive_import_view
        self.hive_history_db = hive_history_db
        self.hive_history_table = hive_history_table
        self.hive_history_temp_db = hive_history_temp_db
        self.hive_history_temp_table = hive_history_temp_table
        self.hive_import_pk_only_db = hive_import_pk_only_db
        self.hive_import_pk_only_table = hive_import_pk_only_table
        self.hive_delete_db = hive_delete_db
        self.hive_delete_table = hive_delete_table
        self.hive_column_name_import = hive_column_name_import
        self.hive_column_name_insert = hive_column_name_insert
        self.hive_column_name_update = hive_column_name_update
        self.hive_column_name_delete = hive_column_name_delete
        self.hive_column_name_iud = hive_column_name_iud
        self.hive_column_name_history_timestamp = hive_column_name_history_timestamp
        self.hive_column_name_source = hive_column_name_source

    def check_hive_meta_store(self):
        logging.debug("Executing common_operations.check_hive_meta_store()")

        # Loop through all Hive webHCat servers from the configuration and checks if one of them works
        self.hive_meta_store = None
        web_hcat_response = None
        for hive_url in self.hiveMetaStoreList:
            try:
                url = hive_url.strip() + "/templeton/v1/status"
                logging.debug("Testing status for webHCat on URL: %s" % (url))
                web_hcat_response = requests.get(url, auth=self.web_hcat_auth)
            except:
                continue
            if web_hcat_response.status_code == 200:
                self.hive_meta_store = hive_url.strip()
                break

        if self.hive_meta_store == None:
            logging.error(
                "Cant find a working Hive webHCat server. Please check configuration and server status"
            )
            # TODO(#229): Handle the exit here

        logging.debug("Executing common_operation.check_hive_meta_store() - Finished")

    def check_db(self, hive_db):
        result = self.metastore.check_db(hive_db)
        return result

    def check_table(self, hive_db, hive_table):
        result = self.metastore.check_table(hive_db=hive_db, hive_table=hive_table)
        return result

    def get_table_location(self, hive_db, hive_table):
        result = self.metastore.get_table_location(
            hive_db=hive_db, hive_table=hive_table
        )
        return result

    def is_table_external_orc_format(self, hive_db, hive_table):
        result = self.metastore.is_table_external_orc_format(
            hive_db=hive_db, hive_table=hive_table
        )
        return result

    def is_table_external_parquet_format(self, hive_db, hive_table):
        result = self.metastore.is_table_external_parquet_format(
            hive_db=hive_db, hive_table=hive_table
        )
        return result

    def is_table_external_iceberg_format(self, hive_db, hive_table):
        result = self.metastore.is_table_external_iceberg_format(
            hive_db=hive_db, hive_table=hive_table
        )
        return result

    def is_table_external(self, hive_db, hive_table):
        result = self.metastore.is_table_external(
            hive_db=hive_db, hive_table=hive_table
        )
        return result

    def is_table_transactional(self, hive_db, hive_table):
        result = self.metastore.is_table_transactional(
            hive_db=hive_db, hive_table=hive_table
        )
        return result

    def is_table_view(self, hive_db, hive_table):
        result = self.metastore.is_table_view(hive_db=hive_db, hive_table=hive_table)
        return result

    def get_pk(self, hive_db, hive_table, quoted_columns=False):
        result = self.metastore.get_pk(
            hive_db=hive_db, hive_table=hive_table, quoted_columns=quoted_columns
        )
        return result

    def get_pk_name(self, hive_db, hive_table):
        result = self.metastore.get_pk_name(hive_db=hive_db, hive_table=hive_table)
        return result

    def get_fks(self, hive_db, hive_table):
        result = self.metastore.get_fks(hive_db=hive_db, hive_table=hive_table)
        return result

    def remove_locks_by_force(self, hive_db, hive_table):
        result = self.metastore.remove_locks_by_force(
            hive_db=hive_db, hive_table=hive_table
        )
        return result

    def get_tables(self, db_filter=None, table_filter=None):
        result = self.metastore.get_tables(
            db_filter=db_filter, table_filter=table_filter
        )
        return result

    def get_columns(
        self,
        hive_db,
        hive_table,
        include_type=False,
        include_comment=False,
        include_idx=False,
        force_column_uppercase=False,
        exclude_data_lake_columns=False,
    ):
        result = self.metastore.get_columns(
            hive_db=hive_db,
            hive_table=hive_table,
            include_type=include_type,
            include_comment=include_comment,
            include_idx=include_idx,
            force_column_uppercase=force_column_uppercase,
        )

        if exclude_data_lake_columns == True:
            result = result[
                ~result["name"].isin(
                    [
                        self.hive_column_name_import,
                        self.hive_column_name_insert,
                        self.hive_column_name_update,
                        self.hive_column_name_delete,
                        self.hive_column_name_iud,
                        self.hive_column_name_history_timestamp,
                        self.hive_column_name_source,
                    ]
                )
            ]

            # Index needs to be reset if anything was droped in the DF.
            # The merge will otherwise indicate that there is a difference and table will be recreated
            result.reset_index(drop=True, inplace=True)

        return result

    def disconnect_from_hive(self):
        logging.debug("Executing common_operations.disconnect_from_hive()")
        if self.hive_cursor != None:
            self.hive_cursor.close()
            self.hive_conn.close()
            self.hive_cursor = None
            self.hive_conn = None

        logging.debug("Executing common_operations.disconnect_from_hive() - Finished")

    def connect_to_hive(self, force_skip_test=False, quiet=False):
        logging.debug("Executing common_operations.connect_to_hive()")

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
                            "Connecting to Hive at %s:%s", hive_hostname, hive_port
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
                        "Could not connect to Hive at %s:%s. This might be because the address was not resolvable in the DNS",
                        hive_hostname,
                        hive_port,
                    )
                except hive.thrift.transport.TTransport.TTransportException:
                    logging.warning(
                        "Could not connect to Hive at %s:%s.", hive_hostname, hive_port
                    )
                except:
                    logging.warning(
                        "General connection error to Hive at %s:%s.",
                        hive_hostname,
                        hive_port,
                    )
                    logging.warning(sys.exc_info())

                if self.hive_cursor != None:
                    break

            if self.hive_cursor == None:
                raise ValueError("Could not connect to any Hive Server.")

            if (
                self.common_config.get_config_value(
                    key="hive_validate_before_execution"
                )
                == True
                and force_skip_test == False
            ):
                self.test_hive_query_execution()

        logging.debug("Executing common_operations.connect_to_hive() - Finished")

    def test_hive_query_execution(
        self,
    ):
        logging.debug("Executing common_operations.test_hive_query_execution()")
        logging.info("Checking Hive functionality by querying the reference table")

        hive_table = self.common_config.get_config_value(key="hive_validate_table")

        result_df = self.execute_hive_query(
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
        logging.debug(
            "Executing common_operations.test_hive_query_execution() - Finished"
        )

    def execute_hive_query(self, query, quiet=False):
        logging.debug("Executing common_operations.execute_hive_query()")
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
                            self.common_config.update_yarn_statistics(
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
                            f"{headers[0]!s:<16} {headers[1]!s:>12} {headers[2]!s:>12} {headers[3]!s:>11} {headers[4]!s:>11} {headers[5]!s:>9} {headers[6]!s:>9} {headers[7]!s:>8} {headers[8]!s:>8}"
                        )
                    except IndexError:
                        print(headers)
                    sys.stdout.write("\u001b[0m")  # Reset
                    sys.stdout.flush()
                    first_output_line = False

                # Print the actual vertex information of the query
                try:
                    print(
                        f"{row[0]!s:<16} {row[1]!s:>12} {row[2]!s:>12} {row[3]!s:>11} {row[4]!s:>11} {row[5]!s:>9} {row[6]!s:>9} {row[7]!s:>8} {row[8]!s:>8}"
                    )
                except IndexError:
                    print(headers)

            sys.stdout.write("\u001b[31m")  # Red
            sys.stdout.flush()
            print(
                f"ELAPSED TIME: {time.monotonic() - hive_query_start_time:.2f} seconds"
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
                    f"{row[0]!s:<16} {row[1]!s:>12} {row[2]!s:>12} {row[3]!s:>11} {row[4]!s:>11} {row[5]!s:>9} {row[6]!s:>9} {row[7]!s:>8} {row[8]!s:>8}"
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
            self.common_config.update_yarn_statistics(
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

        logging.debug("Executing common_operations.execute_hive_query() - Finished")
        return result_df

    def execute_beeline_script(self, hive_script_file, use_db=None):
        logging.debug("Executing common_operations.execute_beeline_script()")

        if use_db == None:
            use_db = ""

        # Connect to Hive to get a working server in the list of Hive servers
        # This sets the self.hive_hostname and self.hive_port so that beeline can connect to a working Hive server
        self.connect_to_hive(force_skip_test=True, quiet=True)
        self.disconnect_from_hive()

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

        logging.debug("JDBC Connection String: %s", beeline_connect_str)

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

        logging.debug("Executing common_operations.execute_beeline_script() - Finished")

    def get_hive_table_row_count(self, hive_db, hive_table, where_statement=None):
        logging.debug("Executing common_operations.get_hive_table_row_count()")
        logging.info("Reading the number of rows from %s.%s", hive_db, hive_table)
        row_count = 0

        query = "select count(1) as rowcount from `%s`.`%s` " % (hive_db, hive_table)
        if where_statement != None:
            query += "where " + where_statement
        else:
            query += "limit 1"

        result_df = self.execute_hive_query(query)
        row_count = int(result_df["rowcount"].iloc[0])
        logging.debug("Rowcount from %s.%s: %s", hive_db, hive_table, row_count)

        logging.debug(
            "Executing common_operations.get_hive_table_row_count() - Finished"
        )
        return row_count

    def drop_hive_table(self, hive_db, hive_table):
        logging.debug("Executing common_operations.drop_hive_table()")
        logging.info("Dropping table %s.%s", hive_db, hive_table)

        self.connect_to_hive(force_skip_test=True, quiet=True)
        self.execute_hive_query(
            "drop table if exists `%s`.`%s`" % (hive_db, hive_table)
        )

        logging.debug("Executing common_operations.drop_hive_table() - Finished")

    def truncate_hive_table(self, hive_db, hive_table):
        """Truncates a Hive table"""
        logging.debug("Executing common_operations.truncate_hive_table()")

        self.execute_hive_query("truncate table `%s`.`%s`" % (hive_db, hive_table))
        logging.debug("Executing common_operations.truncate_hive_table() - Finished")

    def update_hive_table_statistics(self, hive_db, hive_table):
        """Compute Hive statistics on a Hive table"""
        logging.debug("Executing common_operations.update_hive_table_statistics()")

        self.execute_hive_query(
            "analyze table `%s`.`%s` compute statistics " % (hive_db, hive_table)
        )
        logging.debug(
            "Executing common_operations.update_hive_table_statistics() - Finished"
        )

    def run_hive_major_compaction(self, hive_db, hive_table):
        """Force a major compaction on a Hive table"""
        logging.debug("Executing common_operations.run_hive_major_compaction()")

        if (
            self.common_config.get_config_value(key="hive_major_compact_after_merge")
            == True
        ):
            logging.info("Running a Major compaction on Hive table")
            try:
                self.execute_hive_query(
                    "alter table `%s`.`%s` compact 'major' " % (hive_db, hive_table)
                )
            except:
                logging.error(
                    "Major compaction failed with the following error message:"
                )
                logging.warning(sys.exc_info())
                pass

        logging.debug(
            "Executing common_operations.run_hive_major_compaction() - Finished"
        )

    def convert_hive_table_to_acid(
        self,
        hive_db,
        hive_table,
        create_delete_column=False,
        create_merge_columns=False,
    ):
        """Checks if a table is ACID or not, and it it isn't, it converts the table to an ACID table"""
        # TODO(#229): HDP3 shouldnt run this
        logging.debug("Executing common_operations.convert_hive_table_to_acid()")
        logging.info("Converting table to an ACID & Transactional enabled table")

        buckets = 1
        hive_table_bucketed = "%s_hive_bucketed" % (hive_table)
        logging.info("The new table will use %s buckets", buckets)

        if self.check_table(hive_db=hive_db, hive_table=hive_table_bucketed) == True:
            logging.warning(
                "Temporary table used during conversion to ACID already exists. We will drop this table and recreate it again"
            )
            self.execute_hive_query(
                "drop table `%s`.`%s` " % (hive_db, hive_table_bucketed)
            )

        column_df = self.get_columns(
            hive_db=hive_db,
            hive_table=hive_table,
            include_type=True,
            include_comment=True,
        )
        hive_columns_create = []
        hive_columns_insert = []
        hive_columns_create_adding = []
        hive_columns_insert_adding = []
        datalake_import_exists = False

        pk_columns = self.get_pk(
            hive_db=hive_db, hive_table=hive_table, quoted_columns=True
        )
        if pk_columns == None or pk_columns == "":
            logging.error(
                "There are no PrimaryKey defined on the table. Please create one first before converting to ACID (PK is needed for bucket columns)"
            )
            self.common_config.remove_temporary_files()
            sys.exit(1)

        for _index, row in column_df.iterrows():
            column_name = row["name"]
            column_type = row["type"]
            column_comment = row["comment"]

            # if columnName in ["datalake_import"] and createMergeColumns == True:
            if (
                column_name in [self.hive_column_name_import]
                and create_merge_columns == True
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

        if create_merge_columns == True:
            # hiveColumnsCreateAdding.append("datalake_iud char(1) COMMENT \"Last operation of this record was I=Insert, U=Update or D=Delete\"")
            # hiveColumnsCreateAdding.append("datalake_insert timestamp COMMENT \"Timestamp for insert in Datalake\"")
            # hiveColumnsCreateAdding.append("datalake_update timestamp COMMENT \"Timestamp for last update in Datalake\"")
            # hiveColumnsInsertAdding.append("datalake_iud")
            # hiveColumnsInsertAdding.append("datalake_insert")
            # hiveColumnsInsertAdding.append("datalake_update")
            hive_columns_create_adding.append(
                '%s char(1) COMMENT "Last operation of this record was I=Insert, U=Update or D=Delete"'
                % (self.hive_column_name_iud)
            )
            hive_columns_create_adding.append(
                '%s timestamp COMMENT "Timestamp for insert in Datalake"'
                % (self.hive_column_name_insert)
            )
            hive_columns_create_adding.append(
                '%s timestamp COMMENT "Timestamp for last update in Datalake"'
                % (self.hive_column_name_update)
            )
            hive_columns_insert_adding.append(self.hive_column_name_iud)
            hive_columns_insert_adding.append(self.hive_column_name_insert)
            hive_columns_insert_adding.append(self.hive_column_name_update)

            if create_delete_column == True:
                # hiveColumnsCreateAdding.append("datalake_delete timestamp COMMENT \"Timestamp for soft delete in Datalake\"")
                # hiveColumnsInsertAdding.append("datalake_delete")
                hive_columns_create_adding.append(
                    '%s timestamp COMMENT "Timestamp for soft delete in Datalake"'
                    % (self.hive_column_name_delete)
                )
                hive_columns_insert_adding.append(self.hive_column_name_delete)

        # Create the Bucketed table
        query = "create table `%s`.`%s` " % (hive_db, hive_table_bucketed)
        query += "( %s" % (", ".join(hive_columns_create))
        if create_merge_columns == True:
            query += ", %s" % (", ".join(hive_columns_create_adding))
        query += (
            ") clustered by (%s) into %s buckets stored as orc tblproperties ('orc.compress'='ZLIB', 'transactional'='true')"
            % (pk_columns, buckets)
        )
        self.execute_hive_query(query)

        # Insert data into the Bucketed table
        query = "insert into `%s`.`%s` " % (hive_db, hive_table_bucketed)
        query += "( %s" % (", ".join(hive_columns_insert))
        if create_merge_columns == True:
            query += ", %s" % (", ".join(hive_columns_insert_adding))
        query += ") "
        query += "select "
        query += "%s" % (", ".join(hive_columns_insert))
        if create_merge_columns == True:
            if datalake_import_exists == True:
                # query += ", 'I', datalake_import, datalake_import"
                query += ", 'I', %s, %s" % (
                    self.hive_column_name_import,
                    self.hive_column_name_import,
                )
            else:
                query += ', \'I\', timestamp("1900-01-01 00:00:00.000"), timestamp("1900-01-01 00:00:00.000")'

            if create_delete_column == True:
                query += ", NULL"
        query += " from `%s`.`%s` " % (hive_db, hive_table)
        self.execute_hive_query(query)

        # Drop Target table
        query = "drop table `%s`.`%s` " % (hive_db, hive_table)
        self.execute_hive_query(query)

        # Rename bucketed table to Target table
        query = "alter table `%s`.`%s` rename to `%s`.`%s`" % (
            hive_db,
            hive_table_bucketed,
            hive_db,
            hive_table,
        )
        self.execute_hive_query(query)

        self.reconnect_hive_meta_store()

        logging.debug(
            "Executing common_operations.convert_hive_table_to_acid() - Finished"
        )

    def get_hive_column_name_diff(
        self,
        source_db,
        source_table,
        target_db,
        target_table,
        import_tool,
        source_is_import_table=False,
    ):
        """Returns a dataframe that includes the diff between the source and target table. If sourceIsImportTable=True, then there is additional columns that contains the correct name for the import table columns"""
        logging.debug("Executing common_operations.get_hive_column_name_diff()")

        source_columns = self.get_columns(
            hive_db=source_db,
            hive_table=source_table,
            include_type=False,
            include_comment=False,
        )
        target_columns = self.get_columns(
            hive_db=target_db,
            hive_table=target_table,
            include_type=False,
            include_comment=False,
        )

        if source_is_import_table == True:
            # Logic here is to create a new column in both DF and call them sourceName vs targetName. These are the original names. Then we replace the values in targetColumn DF column col
            # with the name that the column should be called in the source system. This is needed to handle characters that is not supported in Parquet files, like SPACE
            source_columns["sourceName"] = source_columns["name"]
            target_columns["targetName"] = target_columns["name"]

            if import_tool != "spark":
                # If you change any of the name replace operations, you also need to change the same data in functions
                # import_operations.update_columns_for_import_table() and import_definitions.save_column_data()
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

        logging.debug(
            "Executing common_operations.get_hive_column_name_diff() - Finished"
        )
        return column_merge
