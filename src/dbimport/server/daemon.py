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
import subprocess
import sys
import threading
import time
from datetime import UTC, datetime, timedelta
from queue import Empty, Queue
from typing import Any

import sqlalchemy as sa
from daemons.prefab import run
from sqlalchemy.exc import DBAPIError, SQLAlchemyError
from sqlalchemy.orm import Session, sessionmaker

from dbimport.common.exceptions import InvalidConfigurationError, SQLError
from dbimport.config_db import common_config, instances
from dbimport.config_db import engine as config_engine
from dbimport.config_db.repositories.copies import CopiesRepository
from dbimport.config_db.repositories.import_tables import ImportTablesRepository
from dbimport.server import atlas_discovery
from dbimport.settings import get_settings


class DistCP(threading.Thread):
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

        yarn_queue = get_settings().server.distcp_yarnqueue

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
            while sh_session.poll() is None:
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


def restart_started_copies(session: Session, now: datetime) -> None:
    """Sets the copies a previous server handed to distcp but did not finish
    back to not started, so that they are copied again; now is their new
    last_status_update."""
    CopiesRepository(session).restart_started_copies(now)
    session.commit()


def start_pending_copies(
    session: Session, requests: Queue[dict[str, Any]], now: datetime
) -> None:
    """Hands every copy not started, and every failed copy last updated a
    minute or more before now, to the distcp threads and marks it as handed
    over (copy_status 1) at now."""
    log = logging.getLogger("server")
    copies = CopiesRepository(session)

    # TODO(#229): make the 1 min interval a configured param
    for copy in copies.copies_to_start(now - timedelta(minutes=1)):
        log.info("New sync request for table %s.%s", copy.hive_db, copy.hive_table)

        copies.set_async_copy_status(copy.table_id, copy.destination, 1, updated=now)
        session.commit()

        requests.put(
            {
                "tableID": copy.table_id,
                "hiveDB": copy.hive_db,
                "hiveTable": copy.hive_table,
                "destination": copy.destination,
                "failures": copy.failures,
                "HDFSsourcePath": copy.hdfs_source_path,
                "HDFStargetPath": copy.hdfs_target_path,
            }
        )

        log.debug(
            "Status changed to 1 for table %s.%s and sent to distCP threads",
            copy.hive_db,
            copy.hive_table,
        )


def save_copy_result(session: Session, response: dict[str, Any], now: datetime) -> None:
    """Stores the result of a distcp thread at now: copy_status 3 for a
    copy that succeeded, 2 for one that failed, and its failures."""
    copy_status = 3 if response.get("result") == True else 2
    CopiesRepository(session).set_async_copy_status(
        response["tableID"],
        response["destination"],
        copy_status,
        updated=now,
        failures=response.get("failures"),
    )
    session.commit()


def finish_copy(
    remote_session: Session, hive_db: str, hive_table: str, now: datetime
) -> None:
    """Records in the configuration database of the destination that the
    copy of the table finished at now. Raises NoResultFound when the table
    is missing there."""
    tables = ImportTablesRepository(remote_session)
    tables.set_copy_finished(tables.table_id_of(hive_db, hive_table), now)
    remote_session.commit()


class ServerDaemon(run.RunDaemon):
    def set_config(self, action, log_format):
        self.action = action
        self.log_format = log_format

    def run(self):
        # This is the main event loop where the 'real' daemonwork happens
        log = logging.getLogger("server")
        log.debug("Executing daemon.ServerDaemon.run()")
        log.info("Server initializing")
        self.debug_log_level = False

        if logging.root.level == 10:  # DEBUG
            self.debug_log_level = True

        self.common_config = common_config.Config()

        self.logdir = get_settings().server.logdir

        self.crypto = self.common_config.crypto
        self.crypto.set_private_key_file(get_settings().credentials.private_key)
        self.crypto.set_public_key_file(get_settings().credentials.public_key)

        self.remote_dbimport_engines = {}
        self.remote_dbimport_sessions = {}
        self.remote_instance_config_db = None

        self.config_db_session = None
        self.config_db_engine = None

        self.dist_cp_req_queue = Queue()
        self.dist_cp_res_queue = Queue()
        self.thread_stop_event = threading.Event()

        # Start the Atlas Discovery Thread
        self.atlas_discovery_thread = atlas_discovery.AtlasDiscovery(
            self.thread_stop_event
        )
        self.atlas_discovery_thread.daemon = True
        self.atlas_discovery_thread.start()

        # Start the DistCP threads
        dist_cp_separate_logs = get_settings().server.distcp_separate_logs

        dist_cp_objects = []
        dist_cp_threads = get_settings().server.distcp_threads
        if dist_cp_threads == 0:
            log.info("Distcp disabled as the number of threads is set to 0")
        else:
            log.info("Starting %s distCp threads", dist_cp_threads)

        for thread_id in range(0, dist_cp_threads):
            if not dist_cp_separate_logs:
                dist_cp_log_name = "distCP"
            else:
                dist_cp_log_name = f"distCP-thread{thread_id}"

            thread = DistCP(
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

        # Set all rows that have copy_status = 1 to 0. This is needed in the startup as if they are 1 in this stage, it means that a previous
        # server marked it as 1 but didnt finish the copy. We need to retry that copy here and now
        if dist_cp_threads > 0:
            try:
                session = self.get_dbimport_session()
                restart_started_copies(session, datetime.now(UTC))
                session.close()

                log.debug("Init part of daemon.ServerDaemon.run() completed")

                log.info("Server startup completed")

            except SQLAlchemyError as e:
                log.error("%s", e)
                log.error("Server startup failed")
                self.disconnect_dbimport_db()

                # As we require this operation to be completed successful before entering the main loop, we will exit if there is a problem
                self.common_config.remove_temporary_files()
                sys.exit(1)

            except SQLError:
                log.error("Server startup failed. Cant connect to config database")
                self.disconnect_dbimport_db()
                self.common_config.remove_temporary_files()
                sys.exit(1)

        while True:
            # ***********************************
            # Main Loop for server
            # ***********************************

            if dist_cp_threads > 0:
                try:
                    session = self.get_dbimport_session()

                    # status 0 = New data from import
                    # status 1 = Data sent to DistCP thread
                    # status 2 = Data returned from DistCP and was a failure
                    # status 3 = Data returned from DistCP and was a success

                    # ------------------------------------------
                    # Fetch all rows from CopyAsyncStatus that contains the status 0 and send them to DistCP threads
                    # ------------------------------------------

                    start_pending_copies(
                        session, self.dist_cp_req_queue, datetime.now(UTC)
                    )

                    session.close()

                except SQLAlchemyError as e:
                    log.error("%s", e)
                    session.rollback()
                    self.disconnect_dbimport_db()

                except SQLError:
                    self.disconnect_dbimport_db()

                # ------------------------------------------
                # Read the response from the DistCP threads
                # ------------------------------------------
                try:
                    dist_cp_response = self.dist_cp_res_queue.get(block=False)
                except Empty:
                    pass
                else:
                    try:
                        session = self.get_dbimport_session()
                        save_copy_result(session, dist_cp_response, datetime.now(UTC))
                        session.close()

                    except SQLAlchemyError as e:
                        log.error("%s", e)
                        session.rollback()
                        self.disconnect_dbimport_db()

                    except SQLError:
                        self.disconnect_dbimport_db()

                # ------------------------------------------
                # Fetch all rows from CopyAsyncStatus that contains the status 3 and update the remote DBImport instance database
                # Also dlete the record from the CopyAsyncStatus table
                # ------------------------------------------

                try:
                    session = self.get_dbimport_session()
                    finished = CopiesRepository(session).finished_copies()
                    session.close()

                except SQLAlchemyError as e:
                    log.error("%s", e)
                    session.rollback()
                    self.disconnect_dbimport_db()

                except SQLError:
                    self.disconnect_dbimport_db()

                else:
                    for copy in finished:
                        table_id = copy.table_id
                        destination = copy.destination
                        hive_db = copy.hive_db
                        hive_table = copy.hive_table

                        # Get the remote sessions. if sessions is not available, we just continue to the next item in the database
                        _remote_session = self.get_dbimport_remote_session(destination)
                        if _remote_session == None:
                            continue

                        try:
                            remote_session = _remote_session()
                            finish_copy(
                                remote_session, hive_db, hive_table, datetime.now(UTC)
                            )
                            remote_session.close()

                        except SQLAlchemyError as e:
                            log.error("%s", e)
                            remote_session.rollback()
                            self.disconnect_remote_session(destination)

                        else:
                            # Delete the record from CopyAsyncStatus
                            try:
                                session = self.get_dbimport_session()
                                CopiesRepository(session).delete_async_copy(
                                    table_id, destination
                                )
                                session.commit()
                                session.close()

                            except SQLAlchemyError as e:
                                log.error("%s", e)
                                session.rollback()
                                self.disconnect_dbimport_db()

                            except SQLError:
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
        log.debug("Executing daemon.ServerDaemon.run() - Finished")

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
                raise SQLError("Can't connect to DBImport database")

        return self.config_db_session()

    def connect_dbimport_db(self):
        # Esablish a SQLAlchemy connection to the DBImport database
        log = logging.getLogger("server")

        try:
            self.config_db_engine = config_engine.create(echo=self.debug_log_level)
            self.config_db_engine.connect().close()
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
        """The sessionmaker of the configuration database of the DBImport
        instance, connected on first use; None when its credentials cannot
        be used or it cannot be reached."""
        log = logging.getLogger("server")

        # A dictionary of all remote DBImport configuration databases are keept in self.remote_dbimport_sessions
        # This will make only one sessions to the database and then save that for each and every connection after that
        if instance in self.remote_dbimport_sessions:
            return self.remote_dbimport_sessions.get(instance)

        log.info("Connecting to remote DBImport database for '%s'", instance)

        try:
            session = self.get_dbimport_session()
        except SQLError:
            self.disconnect_dbimport_db()
            return None

        try:
            with session:
                connection = CopiesRepository(session).instance_connection(instance)
        except DBAPIError as err:
            log.error("%s", err)
            self.disconnect_dbimport_db()
            return None

        try:
            engine = instances.instance_engine(
                connection, self.crypto, echo=self.debug_log_level
            )
        except InvalidConfigurationError as err:
            log.warning("%s", err)
            return None
        except DBAPIError as err:
            log.error("%s", err)
            return None

        self.remote_dbimport_engines[instance] = engine
        self.remote_dbimport_sessions[instance] = sessionmaker(bind=engine)
        return self.remote_dbimport_sessions[instance]
