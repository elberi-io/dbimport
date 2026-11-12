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

import binascii
import logging
import subprocess
import sys
import threading
import time
from datetime import datetime, timedelta
from queue import Empty, Queue

import pandas as pd
import sqlalchemy as sa
from daemons.prefab import run
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import aliased, sessionmaker

from common.Exceptions import SQLerror
from ConfigReader import configuration
from DBImportConfig import common_config, configSchema
from Server import atlasDiscovery


class distCP(threading.Thread):
    def __init__(
        self, name, dist_cp_req_queue, dist_cp_res_queue, thread_stop_event, logger_name
    ):
        threading.Thread.__init__(self)
        self.name = name
        self.dist_cp_req_queue = dist_cp_req_queue
        self.dist_cp_res_queue = dist_cp_res_queue
        self.thread_stop_event = thread_stop_event
        self.logger_name = logger_name

    def run(self):
        log = logging.getLogger(self.logger_name)
        log.info("distCP %s started", self.name)

        yarn_queue = configuration.get("Server", "distCP_yarnqueue")

        while not self.thread_stop_event.isSet():
            dist_cp_request = self.dist_cp_req_queue.get()
            if dist_cp_request is None:
                time.sleep(1)
                break

            table_id = dist_cp_request.get("tableID")
            hive_db = dist_cp_request.get("hiveDB")
            hive_table = dist_cp_request.get("hiveTable")
            destination = dist_cp_request.get("destination")
            failures = dist_cp_request.get("failures")
            hdfs_source_path = dist_cp_request.get("HDFSsourcePath")
            hdfs_target_path = dist_cp_request.get("HDFStargetPath")

            log.info(
                "Thread %s: Starting a new distCP copy with the following paramaters",
                self.name,
            )
            log.info(
                "Thread %s: --------------------------------------------------------",
                self.name,
            )
            log.info("Thread %s: tableID = %s", self.name, table_id)
            log.info("Thread %s: hiveDB = %s", self.name, hive_db)
            log.info("Thread %s: hiveTable = %s", self.name, hive_table)
            log.info("Thread %s: destination = %s", self.name, destination)
            log.info("Thread %s: HDFSsourcePath = %s", self.name, hdfs_source_path)
            log.info("Thread %s: HDFStargetPath = %s", self.name, hdfs_target_path)
            log.info(
                "Thread %s: --------------------------------------------------------",
                self.name,
            )

            distcp_command = [
                "hadoop",
                "distcp",
                "-D",
                "yarn.timeline-service.enabled=false",
                "-D",
                f"mapreduce.job.queuename={yarn_queue}",
                "-overwrite",
                "-delete",
                "%s" % (hdfs_source_path),
                "%s" % (hdfs_target_path),
            ]

            log.info("Thread %s:  ______________________ ", self.name)
            log.info("Thread %s: |                      |", self.name)
            log.info("Thread %s: | Hadoop distCp starts |", self.name)
            log.info("Thread %s: |______________________|", self.name)
            log.info("Thread %s: ", self.name)

            # Start distcp
            sh_session = subprocess.Popen(
                distcp_command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
            )
            dist_cp_output = ""

            # Print Stdout and stderr while distcp is running
            while sh_session.poll() == None:
                row = sh_session.stdout.readline().decode("utf-8").rstrip()
                if row != "":
                    log.info("Thread %s: %s", self.name, row)
                    dist_cp_output += row + "\n"
                    sys.stdout.flush()

            # Print what is left in output after distcp is finished
            for row in sh_session.stdout.readlines():
                row = row.decode("utf-8").rstrip()
                if row != "":
                    log.info("Thread %s: %s", self.name, row)
                    dist_cp_output += row + "\n"
                    sys.stdout.flush()

            log.info("Thread %s:  _________________________ ", self.name)
            log.info("Thread %s: |                         |", self.name)
            log.info("Thread %s: | Hadoop distCp completed |", self.name)
            log.info("Thread %s: |_________________________|", self.name)
            log.info("Thread %s: ", self.name)

            dis_cp_result = False
            if " ERROR " in dist_cp_output:
                log.error("Thread %s: ERROR detected during distCP copy.", self.name)
                failures = failures + 1
            elif " completed successfully" in dist_cp_output:
                dis_cp_result = True
                failures = 0
            else:
                log.error(
                    "Thread %s: Unknown status of distCP. Marked as failure as it cant find that it was finished successful",
                    self.name,
                )
                failures = failures + 1

            dist_cp_response = {}
            dist_cp_response["tableID"] = table_id
            dist_cp_response["hiveDB"] = hive_db
            dist_cp_response["hiveTable"] = hive_table
            dist_cp_response["destination"] = destination
            dist_cp_response["result"] = dis_cp_result
            dist_cp_response["failures"] = failures

            self.dist_cp_res_queue.put(dist_cp_response)

        log.info("distCP %s stopped", self.name)


class serverDaemon(run.RunDaemon):
    def set_config(self, action, log_format):
        self.action = action
        self.log_format = log_format

    def run(self):
        # This is the main event loop where the 'real' daemonwork happens
        log = logging.getLogger("server")
        log.debug("Executing daemon.serverDaemon.run()")
        log.info("Server initializing")
        self.mysql_conn = None
        self.mysql_cursor = None
        self.debug_log_level = False

        if logging.root.level == 10:  # DEBUG
            self.debug_log_level = True

        self.common_config = common_config.config()

        self.logdir = configuration.get("Server", "logdir")

        self.crypto = self.common_config.crypto
        self.crypto.set_private_key_file(
            configuration.get("Credentials", "private_key")
        )
        self.crypto.set_public_key_file(configuration.get("Credentials", "public_key"))

        self.remote_dbimport_engines = {}
        self.remote_dbimport_sessions = {}
        self.remote_instance_config_db = None

        self.config_db_session = None
        self.config_db_engine = None

        self.dist_cp_req_queue = Queue()
        self.dist_cp_res_queue = Queue()
        self.thread_stop_event = threading.Event()

        # Start the Atlas Discovery Thread
        self.atlas_discovery_thread = atlasDiscovery.atlasDiscovery(
            self.thread_stop_event
        )
        self.atlas_discovery_thread.daemon = True
        self.atlas_discovery_thread.start()

        # Start the distCP threads
        if configuration.get("Server", "distCP_separate_logs").lower() == "true":
            dist_cp_separate_logs = True
        else:
            dist_cp_separate_logs = False

        dist_cp_objects = []
        dist_cp_threads = int(configuration.get("Server", "distCP_threads"))
        if dist_cp_threads == 0:
            log.info("Distcp disabled as the number of threads is set to 0")
        else:
            log.info("Starting %s distCp threads", dist_cp_threads)

        for thread_id in range(0, dist_cp_threads):
            if dist_cp_separate_logs == False:
                dist_cp_log_name = "distCP"
            else:
                dist_cp_log_name = f"distCP-thread{thread_id}"

            thread = distCP(
                name=str(thread_id),
                dist_cp_req_queue=self.dist_cp_req_queue,
                dist_cp_res_queue=self.dist_cp_res_queue,
                thread_stop_event=self.thread_stop_event,
                logger_name=dist_cp_log_name,
            )
            thread.daemon = True
            thread.start()
            dist_cp_objects.append(thread)

        # Fetch configuration about MySQL database and how to connect to it
        self.config_hostname = configuration.get("Database", "mysql_hostname")
        self.config_port = configuration.get("Database", "mysql_port")
        self.config_database = configuration.get("Database", "mysql_database")
        self.config_username = configuration.get("Database", "mysql_username")
        self.config_password = configuration.get("Database", "mysql_password")

        # Set all rows that have copy_status = 1 to 0. This is needed in the startup as if they are 1 in this stage, it means that a previous
        # server marked it as 1 but didnt finish the copy. We need to retry that copy here and now
        if dist_cp_threads > 0:
            try:
                update_dict = {}
                update_dict["last_status_update"] = str(
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
                )
                update_dict["copy_status"] = 0

                session = self.get_dbimport_session()

                (
                    session.query(configSchema.copyASyncStatus)
                    .filter(configSchema.copyASyncStatus.copy_status == 1)
                    .update(update_dict)
                )
                session.commit()
                session.close()

                log.debug("Init part of daemon.serverDaemon.run() completed")

                log.info("Server startup completed")

            except SQLAlchemyError as e:
                log.error(str(e.__dict__["orig"]))
                log.error("Server startup failed")
                self.disconnect_dbimport_db()

                # As we require this operation to be completed successful before entering the main loop, we will exit if there is a problem
                self.common_config.remove_temporary_files()
                sys.exit(1)

            except SQLerror:
                log.error("Server startup failed. Cant connect to config database")
                self.disconnect_dbimport_db()
                self.common_config.remove_temporary_files()
                sys.exit(1)

        import_tables = aliased(configSchema.importTables)
        dbimport_instances = aliased(configSchema.dbimportInstances)
        copy_async_status = aliased(configSchema.copyASyncStatus)

        while True:
            # ***********************************
            # Main Loop for server
            # ***********************************

            if dist_cp_threads > 0:
                try:
                    session = self.get_dbimport_session()

                    # status 0 = New data from import
                    # status 1 = Data sent to distCP thread
                    # status 2 = Data returned from distCP and was a failure
                    # status 3 = Data returned from distCP and was a success

                    # ------------------------------------------
                    # Fetch all rows from copyASyncStatus that contains the status 0 and send them to distCP threads
                    # ------------------------------------------

                    # TODO(#229): make the 1 min interval a configured param
                    status2check_timestamp = (
                        datetime.now() - timedelta(minutes=1)
                    ).strftime("%Y-%m-%d %H:%M:%S.%f")

                    async_row = pd.DataFrame(
                        session.query(
                            copy_async_status.table_id,
                            copy_async_status.hive_db,
                            copy_async_status.hive_table,
                            copy_async_status.destination,
                            copy_async_status.failures,
                            copy_async_status.hdfs_source_path,
                            copy_async_status.hdfs_target_path,
                        )
                        .select_from(copy_async_status)
                        .filter(
                            (copy_async_status.copy_status == 0)
                            | (
                                (copy_async_status.copy_status == 2)
                                & (
                                    copy_async_status.last_status_update
                                    <= status2check_timestamp
                                )
                            )
                        )
                        .all()
                    )

                    for _index, row in async_row.iterrows():
                        table_id = row["table_id"]
                        destination = row["destination"]
                        hive_db = row["hive_db"]
                        hive_table = row["hive_table"]
                        failures = row["failures"]
                        hdfs_source_path = row["hdfs_source_path"]
                        hdfs_target_path = row["hdfs_target_path"]

                        log.info(
                            "New sync request for table %s.%s", hive_db, hive_table
                        )

                        update_dict = {}
                        update_dict["copy_status"] = 1
                        update_dict["last_status_update"] = str(
                            datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
                        )

                        (
                            session.query(configSchema.copyASyncStatus)
                            .filter(configSchema.copyASyncStatus.table_id == table_id)
                            .filter(
                                configSchema.copyASyncStatus.destination == destination
                            )
                            .update(update_dict)
                        )
                        session.commit()

                        dist_cp_request = {}
                        dist_cp_request["tableID"] = table_id
                        dist_cp_request["hiveDB"] = hive_db
                        dist_cp_request["hiveTable"] = hive_table
                        dist_cp_request["destination"] = destination
                        dist_cp_request["failures"] = failures
                        dist_cp_request["HDFSsourcePath"] = hdfs_source_path
                        dist_cp_request["HDFStargetPath"] = hdfs_target_path
                        self.dist_cp_req_queue.put(dist_cp_request)

                        log.debug(
                            "Status changed to 1 for table %s.%s and sent to distCP threads",
                            hive_db,
                            hive_table,
                        )

                    session.close()

                except SQLAlchemyError as e:
                    log.error(str(e.__dict__["orig"]))
                    session.rollback()
                    self.disconnect_dbimport_db()

                except SQLerror:
                    self.disconnect_dbimport_db()

                # ------------------------------------------
                # Read the response from the distCP threads
                # ------------------------------------------
                try:
                    dist_cp_response = self.dist_cp_res_queue.get(block=False)
                except Empty:
                    pass
                else:
                    update_dict = {}
                    update_dict["last_status_update"] = str(
                        datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
                    )
                    update_dict["failures"] = dist_cp_response.get("failures")

                    dist_cp_result = dist_cp_response.get("result")
                    if dist_cp_result == True:
                        update_dict["copy_status"] = 3
                    else:
                        update_dict["copy_status"] = 2

                    try:
                        session = self.get_dbimport_session()
                        (
                            session.query(configSchema.copyASyncStatus)
                            .filter(
                                configSchema.copyASyncStatus.table_id
                                == dist_cp_response.get("tableID")
                            )
                            .filter(
                                configSchema.copyASyncStatus.destination
                                == dist_cp_response.get("destination")
                            )
                            .update(update_dict)
                        )
                        session.commit()
                        session.close()

                    except SQLAlchemyError as e:
                        log.error(str(e.__dict__["orig"]))
                        session.rollback()
                        self.disconnect_dbimport_db()

                    except SQLerror:
                        self.disconnect_dbimport_db()

                # ------------------------------------------
                # Fetch all rows from copyASyncStatus that contains the status 3 and update the remote DBImport instance database
                # Also dlete the record from the copyASyncStatus table
                # ------------------------------------------

                try:
                    session = self.get_dbimport_session()
                    async_row = pd.DataFrame(
                        session.query(
                            copy_async_status.table_id,
                            copy_async_status.hive_db,
                            copy_async_status.hive_table,
                            copy_async_status.destination,
                            copy_async_status.failures,
                            copy_async_status.hdfs_source_path,
                            copy_async_status.hdfs_target_path,
                        )
                        .select_from(copy_async_status)
                        .filter(copy_async_status.copy_status == 3)
                        .all()
                    )
                    session.close()

                except SQLAlchemyError as e:
                    log.error(str(e.__dict__["orig"]))
                    session.rollback()
                    self.disconnect_dbimport_db()

                except SQLerror:
                    self.disconnect_dbimport_db()

                else:
                    for _index, row in async_row.iterrows():
                        table_id = row["table_id"]
                        destination = row["destination"]
                        hive_db = row["hive_db"]
                        hive_table = row["hive_table"]
                        failures = row["failures"]
                        hdfs_source_path = row["hdfs_source_path"]
                        hdfs_target_path = row["hdfs_target_path"]

                        # Get the remote sessions. if sessions is not available, we just continue to the next item in the database
                        _remote_session = self.get_dbimport_remote_session(destination)
                        if _remote_session == None:
                            continue

                        try:
                            remote_session = _remote_session()

                            # Get the table_id from the table at the remote instance
                            remote_import_table_id = (
                                remote_session.query(import_tables.table_id)
                                .select_from(import_tables)
                                .filter(import_tables.hive_db == hive_db)
                                .filter(import_tables.hive_table == hive_table)
                                .one()
                            )

                            remote_table_id = remote_import_table_id[0]

                            update_dict = {}
                            update_dict["copy_finished"] = str(
                                datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
                            )

                            # Update the values in import_table on the remote instance
                            (
                                remote_session.query(configSchema.importTables)
                                .filter(
                                    configSchema.importTables.table_id
                                    == remote_table_id
                                )
                                .update(update_dict)
                            )
                            remote_session.commit()

                            remote_session.close()

                        except SQLAlchemyError as e:
                            log.error(str(e.__dict__["orig"]))
                            remote_session.rollback()
                            self.disconnect_remote_session(destination)

                        else:
                            # Delete the record from copyASyncStatus
                            try:
                                session = self.get_dbimport_session()
                                (
                                    session.query(configSchema.copyASyncStatus)
                                    .filter(
                                        configSchema.copyASyncStatus.table_id
                                        == table_id
                                    )
                                    .filter(
                                        configSchema.copyASyncStatus.destination
                                        == destination
                                    )
                                    .delete()
                                )
                                session.commit()
                                session.close()

                            except SQLAlchemyError as e:
                                log.error(str(e.__dict__["orig"]))
                                session.rollback()
                                self.disconnect_dbimport_db()

                            except SQLerror:
                                self.disconnect_dbimport_db()

                            else:
                                log.info(
                                    "Table %s.%s copied successfully to '%s'",
                                    hive_db,
                                    hive_table,
                                    destination,
                                )

                session.close()

            # ***********************************
            # DistCP Part of main loop is finished
            # ***********************************

            # 			log.info("Starting wait")
            time.sleep(1)

        log.info("Server stopped")
        log.debug("Executing daemon.serverDaemon.run() - Finished")

    def disconnect_dbimport_db(self):
        """Disconnects from the database and removes all sessions and engine"""
        log = logging.getLogger("server")

        if self.config_db_engine != None:
            log.info("Disconnecting from DBImport database")
            self.config_db_engine.dispose()
            self.config_db_engine = None

        self.config_db_session = None

    def get_dbimport_session(self):
        log = logging.getLogger("server")
        if self.config_db_session == None:
            if self.connect_dbimport_db() == False:
                raise SQLerror("Can't connect to DBImport database")

        return self.config_db_session()

    def connect_dbimport_db(self):
        # Esablish a SQLAlchemy connection to the DBImport database
        log = logging.getLogger("server")
        self.connect_str = f"mysql+pymysql://{self.config_username}:{self.config_password}@{self.config_hostname}:{self.config_port}/{self.config_database}"

        try:
            self.config_db_engine = sa.create_engine(
                self.connect_str, echo=self.debug_log_level
            )
            self.config_db_engine.connect()
            self.config_db_session = sessionmaker(bind=self.config_db_engine)

        except sa.exc.OperationalError as err:
            log.error("%s", err)
            self.config_db_session = None
            self.config_db_engine = None
            return False
        # 			self.common_config.remove_temporary_files()
        # 			sys.exit(1)

        except:
            print("Unexpected error: ")
            print(sys.exc_info())
            self.config_db_session = None
            self.config_db_engine = None
            return False
        # 			self.common_config.remove_temporary_files()
        # 			sys.exit(1)

        log.info("Connected successful against DBImport database")
        return True

    def disconnect_remote_session(self, instance):
        """Disconnects from the remote database and removes all sessions and engine"""
        log = logging.getLogger("server")

        try:
            engine = self.remote_dbimport_engines.get(instance)
            if engine != None:
                log.info(
                    "Disconnecting from remote DBImport database for '%s'" % (instance)
                )
                engine.dispose()
            self.remote_dbimport_engines.pop(instance)
            self.remote_dbimport_sessions.pop(instance)
        except KeyError:
            log.debug("Cant remove DBImport session or engine. Key does not exist")

    def get_dbimport_remote_session(self, instance):
        """Connects to the remote configuration database with SQLAlchemy"""
        log = logging.getLogger("server")

        # A dictionary of all remote DBImport configuration databases are keept in self.remote_dbimport_sessions
        # This will make only one sessions to the database and then save that for each and every connection after that
        if instance in self.remote_dbimport_sessions:
            return self.remote_dbimport_sessions.get(instance)

        log.info("Connecting to remote DBImport database for '%s'" % (instance))

        connect_status = True

        try:
            session = self.get_dbimport_session()
        except SQLerror:
            self.disconnect_dbimport_db()
            return None

        dbimport_instances = aliased(configSchema.dbimportInstances)

        row = (
            session.query(
                dbimport_instances.db_hostname,
                dbimport_instances.db_port,
                dbimport_instances.db_database,
                dbimport_instances.db_credentials,
            )
            .filter(dbimport_instances.name == instance)
            .one()
        )

        session.close()

        if row[3] == None:
            log.warning(
                "There is no credentials saved in 'dbimport_instance' for %s"
                % (instance)
            )
            return None

        try:
            db_credentials = self.crypto.decrypt(row[3])
        except binascii.Error as err:
            log.warning(
                "Decryption of credentials resulted in error with text: '%s'", err
            )
            return None
        except:
            log.error("Unexpected warning: ")
            log.error(sys.exc_info())
            return None

        if db_credentials == None:
            log.warning(
                "Cant decrypt username and password. Check private/public key in config file"
            )
            return None

        username = db_credentials.split(" ")[0]
        password = db_credentials.split(" ")[1]

        instance_connect_str = "mysql+pymysql://%s:%s@%s:%s/%s" % (
            username,
            password,
            row[0],
            row[1],
            row[2],
        )

        try:
            remote_instance_config_db_engine = sa.create_engine(
                instance_connect_str, echo=self.debug_log_level
            )
            remote_instance_config_db_engine.connect()
            remote_instance_config_db_session = sessionmaker(
                bind=remote_instance_config_db_engine
            )

        except sa.exc.OperationalError as err:
            log.error("%s", err)
            return None
        except:
            log.error("Unexpected error: ")
            log.error(sys.exc_info())
            return None

        self.remote_dbimport_engines[instance] = remote_instance_config_db_engine
        self.remote_dbimport_sessions[instance] = remote_instance_config_db_session
        return remote_instance_config_db_session
