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
import threading
import time
from datetime import datetime, timedelta
from queue import Empty, Queue

import jpype
import pandas as pd
import sqlalchemy as sa
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import aliased, sessionmaker

from common.Exceptions import (
    atlasError,
    connectionError,
    foreignKeyError,
    invalidConfiguration,
)
from ConfigReader import configuration
from DBImportConfig import common_config, configSchema
from DBImportOperation import atlas_operations


class atlasCrawler(threading.Thread):
    def __init__(
        self,
        name,
        atlasCrawlerProcessQueue,
        atlasCrawlerResultQueue,
        threadStopEvent,
        loggerName,
        mutex,
    ):
        threading.Thread.__init__(self)
        self.name = name
        self.jdbc_connection_mutex = mutex
        self.atlas_crawler_process_queue = atlasCrawlerProcessQueue
        self.atlasCrawlerResultQueue = atlasCrawlerResultQueue
        self.thread_stop_event = threadStopEvent
        self.logger_name = loggerName
        self.atlasOperation = atlas_operations.atlasOperation(loggerName)

    # 		self.sourceSchema = schemaReader.source()
    # 		self.jdbc_conn = None
    # 		self.JDBCCursor = None

    def run(self):
        log = logging.getLogger(self.logger_name)
        log.info("Atlas crawler thread %s started", self.name)
        self.atlasOperation.testVariable = self.name
        self.atlasCrawlerRequest = None

        if self.atlasOperation.check_atlas_schema(logger=self.logger_name) == False:
            return False

        while not self.thread_stop_event.isSet():
            try:
                self.atlasCrawlerRequest = self.atlas_crawler_process_queue.get(
                    block=True, timeout=None
                )
            except Empty:
                self.atlasCrawlerRequest = None

            if self.atlasCrawlerRequest is None:
                time.sleep(1)
                continue

            response = {}
            response["dbAlias"] = self.atlasCrawlerRequest.get("dbAlias")

            try:
                self.atlasOperation.set_configuration(
                    self.atlasCrawlerRequest, self.jdbc_connection_mutex
                )
                atlas_operation_result = self.atlasOperation.discover_atlas_rdbms()
            except invalidConfiguration:
                response["result"] = False
                response["blacklist"] = True
            except atlasError:
                response["result"] = False
                response["blacklist"] = False
            except foreignKeyError:
                response["result"] = False
                response["blacklist"] = False
            except connectionError:
                response["result"] = False
                response["blacklist"] = True
            except:
                log.error(sys.exc_info()[0])
                response["result"] = False
                response["blacklist"] = True
            else:
                response["result"] = True
                response["blacklist"] = False

            self.atlasCrawlerResultQueue.put(response)

        log.info("Atlas crawler thread %s stopped", self.name)


class atlasDiscovery(threading.Thread):
    def __init__(self, threadStopEvent):
        threading.Thread.__init__(self)
        self.thread_stop_event = threadStopEvent

    def run(self):
        logger = "atlasDiscovery"
        log = logging.getLogger(logger)
        self.mysql_conn = None
        self.mysql_cursor = None
        self.configDBSession = None
        self.config_db_engine = None
        self.debugLogLevel = False
        atlas_enabled = True
        # 		self.atlasOperation = atlas_operations.atlasOperation(logger)

        if logging.root.level == 10:  # DEBUG
            self.debugLogLevel = True

        self.atlas_crawler_process_queue = Queue()
        self.atlasCrawlerResultQueue = Queue()
        self.jdbc_connection_mutex = threading.Lock()

        # Fetch configuration about MySQL database and how to connect to it
        self.config_hostname = configuration.get("Database", "mysql_hostname")
        self.config_port = configuration.get("Database", "mysql_port")
        self.config_database = configuration.get("Database", "mysql_database")
        self.config_username = configuration.get("Database", "mysql_username")
        self.config_password = configuration.get("Database", "mysql_password")

        atlas_crawler_objects = []
        atlas_crawler_threads = int(configuration.get("Server", "atlas_threads"))
        if atlas_crawler_threads == 0:
            log.info("Atlas discovery disabled as the number of threads is set to 0")
            atlas_enabled = False
        else:
            log.info("Starting %s Atlas crawler threads", atlas_crawler_threads)

        for thread_id in range(0, atlas_crawler_threads):
            # 			if distCP_separate_logs == False:
            atlas_crawler_log_name = f"atlasCrawler-thread{thread_id}"
            # 			else:
            # 				distCPlogName = "distCP-thread%s"%(str(threadID))

            thread = atlasCrawler(
                name=str(thread_id),
                atlasCrawlerProcessQueue=self.atlas_crawler_process_queue,
                atlasCrawlerResultQueue=self.atlasCrawlerResultQueue,
                threadStopEvent=self.thread_stop_event,
                loggerName=atlas_crawler_log_name,
                mutex=self.jdbc_connection_mutex,
            )
            thread.daemon = True
            thread.start()
            atlas_crawler_objects.append(thread)

        self.common_config = common_config.config()
        jdbc_connections = aliased(configSchema.jdbcConnections)
        self.failure_log = {}
        self.connections_sent_to_crawlers = []

        # The interval between the scans. This is in hours
        atlas_discovery_interval = self.common_config.get_config_value(
            key="atlas_discovery_interval"
        )

        # 		if atlasEnabled == True:
        # 			atlasEnabled = self.atlasOperation.check_atlas_schema(logger=logger)

        if atlas_enabled == True:
            log.info("atlasDiscovery started")
            log.info(
                "Atlas discovery interval is set to %s hours"
                % (atlas_discovery_interval)
            )

        while not self.thread_stop_event.isSet() and atlas_enabled == True:
            # ****************************************************************
            # Read data from jdbc_connection and put in queue for processing
            # ****************************************************************

            if self.atlas_crawler_process_queue.qsize() < atlas_crawler_threads:
                # Only read the database if there isn't enough items in the queue to the crawlers to processes. This will save
                # a large number of sql requests if the queue is full
                try:
                    # Read a list of connection aliases that we are going to process in this iteration
                    session = self.get_dbimport_session()
                    atlas_discovery_check_time = datetime.utcnow() - timedelta(
                        hours=atlas_discovery_interval
                    )

                    # TODO(#229): Probably only run this if jdbc_connections_df is empty from the previous run
                    jdbc_connections_df = pd.DataFrame(
                        session.query(
                            jdbc_connections.dbalias,
                            jdbc_connections.atlas_last_discovery,
                            jdbc_connections.atlas_discovery,
                            jdbc_connections.contact_info,
                            jdbc_connections.description,
                            jdbc_connections.owner,
                            jdbc_connections.atlas_include_filter,
                            jdbc_connections.atlas_exclude_filter,
                        )
                        .select_from(jdbc_connections)
                        .filter(jdbc_connections.atlas_discovery == 1)
                        # 						.filter((jdbcConnections.atlas_last_discovery < atlasDiscoveryCheckTime) | (jdbcConnections.atlas_last_discovery == None))
                        .order_by(jdbc_connections.atlas_last_discovery)
                        .all()
                    )
                    session.close()

                except SQLAlchemyError as e:
                    log.error(str(e.__dict__["orig"]))
                    session.rollback()
                    self.disconnect_dbimport_db()

                else:
                    for _index, row in jdbc_connections_df.iterrows():
                        db_alias = row["dbalias"]

                        # TODO(#229): Move this into the thread
                        if (
                            self.common_config.check_time_window(
                                db_alias, atlas_discovery_mode=True
                            )
                            == False
                        ):
                            continue

                        # Find out if the dbAlias is blacklisted
                        if self.is_connection_blacklisted(db_alias) == True:
                            continue

                        if db_alias in self.connections_sent_to_crawlers:
                            # 							log.warning("This connection is already being processed. Skipping....")
                            continue

                        altas_operation_failed = False
                        print_black_list_warning = True

                        self.common_config.mysql_conn.commit()
                        try:
                            self.common_config.lookup_connection_alias(db_alias)
                        except invalidConfiguration as err:
                            if self.common_config.atlasJdbcSourceSupport == True:
                                log.error(
                                    "Connection '%s' have invalid configuration. Failed with '%s'",
                                    db_alias,
                                    err,
                                )
                                altas_operation_failed = True

                        if self.common_config.atlasJdbcSourceSupport == False:
                            # This source type does not support Atlas discovery
                            log.debug(
                                "Connection '%s' does not support Atlas discovery. Skipping..."
                                % (db_alias)
                            )
                            altas_operation_failed = True
                            print_black_list_warning = False

                        # Start the Jpype JVM as that needs to be running before the crawlers starts to use it
                        if jpype.isJVMStarted() == False:
                            log.debug("Starting jpype JVM")
                            self.common_config.connect_to_jdbc(
                                all_jar_files=True,
                                exit_if_failure=False,
                                logger=logger,
                                print_error=False,
                            )
                            self.common_config.disconnect_from_jdbc()

                        # 						if altasOperationFailed == False and self.common_config.connect_to_jdbc(allJarFiles=True, exitIfFailure=False, logger=logger) == True:
                        if altas_operation_failed == False:
                            # 							self.common_config.atlasEnabled = True
                            self.connections_sent_to_crawlers.append(db_alias)

                            log.debug("Sending alias '%s' to queue" % (db_alias))
                            atlas_crawler_request = {}
                            atlas_crawler_request["dbAlias"] = row["dbalias"]
                            atlas_crawler_request["contactInfo"] = row["contact_info"]
                            atlas_crawler_request["description"] = row["description"]
                            atlas_crawler_request["owner"] = row["owner"]
                            atlas_crawler_request["atlasIncludeFilter"] = row[
                                "atlas_include_filter"
                            ]
                            atlas_crawler_request["atlasExcludeFilter"] = row[
                                "atlas_exclude_filter"
                            ]
                            atlas_crawler_request["jdbc_hostname"] = (
                                self.common_config.jdbc_hostname
                            )
                            atlas_crawler_request["jdbc_port"] = (
                                self.common_config.jdbc_port
                            )
                            atlas_crawler_request["jdbc_servertype"] = (
                                self.common_config.jdbc_servertype
                            )
                            atlas_crawler_request["jdbc_database"] = (
                                self.common_config.jdbc_database
                            )
                            atlas_crawler_request["jdbc_oracle_sid"] = (
                                self.common_config.jdbc_oracle_sid
                            )
                            atlas_crawler_request["jdbc_oracle_servicename"] = (
                                self.common_config.jdbc_oracle_servicename
                            )
                            atlas_crawler_request["jdbc_username"] = (
                                self.common_config.jdbc_username
                            )
                            atlas_crawler_request["jdbc_password"] = (
                                self.common_config.jdbc_password
                            )
                            atlas_crawler_request["jdbc_driver"] = (
                                self.common_config.jdbc_driver
                            )
                            atlas_crawler_request["jdbc_url"] = (
                                self.common_config.jdbc_url
                            )
                            atlas_crawler_request["jdbc_classpath_for_python"] = (
                                self.common_config.jdbc_classpath_for_python
                            )
                            atlas_crawler_request["jdbc_environment"] = (
                                self.common_config.jdbc_environment
                            )
                            atlas_crawler_request["hdfs_address"] = None
                            atlas_crawler_request["cluster_name"] = None

                            self.atlas_crawler_process_queue.put(atlas_crawler_request)

                        else:
                            # 							altasOperationFailed = True

                            # 						if altasOperationFailed == True:
                            self.blacklist_connection(
                                db_alias, print_black_list_warning
                            )

            # ********************************
            # Read response from atlasCrawler
            # ********************************

            try:
                atlas_crawler_result = self.atlasCrawlerResultQueue.get(
                    block=False, timeout=1
                )
            except Empty:
                atlas_crawler_result = None

            if atlas_crawler_result is not None:
                db_alias = atlas_crawler_result.get("dbAlias")
                result = atlas_crawler_result.get("result")
                blacklist = atlas_crawler_result.get("blacklist")
                log.debug("atlasCrawlerResultQueue: %s" % (atlas_crawler_result))

                self.connections_sent_to_crawlers.remove(db_alias)

                if result == True:
                    update_dict = {}
                    update_dict["atlas_last_discovery"] = str(
                        datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
                    )

                    try:
                        session = self.get_dbimport_session()
                        (
                            session.query(configSchema.jdbcConnections)
                            .filter(configSchema.jdbcConnections.dbalias == db_alias)
                            .update(update_dict)
                        )
                        session.commit()
                        session.close()

                    except SQLAlchemyError as e:
                        log.error(str(e.__dict__["orig"]))
                        session.rollback()
                        self.disconnect_dbimport_db()

                    else:
                        self.remove_blacklist(db_alias)
                else:
                    if blacklist == True:
                        log.error(
                            "Connection '%s' failed during crawling of database schema"
                            % (db_alias)
                        )
                        self.blacklist_connection(db_alias)
                    else:
                        log.warning(
                            "A Warning was detected when crawling connection '%s'. It will not be marked as completed and will retry the operation"
                            % (db_alias)
                        )

            time.sleep(1)

        self.disconnect_dbimport_db()
        if atlas_enabled == True:
            log.info("atlasDiscovery stopped")

    def is_connection_blacklisted(self, db_alias):
        if self.failure_log.get(db_alias, None) != None:
            black_list_enable_time = self.failure_log[db_alias][
                "blackListStart"
            ] + timedelta(hours=self.failure_log[db_alias]["blackListTime"])

            if datetime.now() < black_list_enable_time:
                # This dbAlias is still blacklisted
                return True

        return False

    def remove_blacklist(self, db_alias):
        self.failure_log.pop(db_alias, None)

    def blacklist_connection(self, db_alias, print_black_list_warning=True):
        log = logging.getLogger("atlasDiscovery")

        black_list_data = self.failure_log.get(db_alias, None)
        if black_list_data == None:
            black_list_time = 1
        else:
            black_list_time = self.failure_log[db_alias]["blackListTime"] * 2

            # Max blacklist time is 24 hours
            if black_list_time > 24:
                black_list_time = 24

        self.failure_log[db_alias] = {
            "blackListTime": black_list_time,
            "blackListStart": datetime.now(),
        }

        if print_black_list_warning == True:
            log.warning(
                "Connection '%s' is blacklisted for %s hours",
                db_alias,
                self.failure_log[db_alias]["blackListTime"],
            )

    def get_dbimport_session(self):
        log = logging.getLogger("atlasDiscovery")
        if self.configDBSession == None:
            self.connect_dbimport_db()

        return self.configDBSession()

    def connect_dbimport_db(self):
        # Esablish a SQLAlchemy connection to the DBImport database
        log = logging.getLogger("atlasDiscovery")
        self.connect_str = f"mysql+pymysql://{self.config_username}:{self.config_password}@{self.config_hostname}:{self.config_port}/{self.config_database}"

        try:
            self.config_db_engine = sa.create_engine(
                self.connect_str, echo=self.debugLogLevel
            )
            self.config_db_engine.connect()
            self.configDBSession = sessionmaker(bind=self.config_db_engine)

        except sa.exc.OperationalError as err:
            log.error("%s", err)
            self.common_config.remove_temporary_files()
            sys.exit(1)
        except:
            print("Unexpected error: ")
            print(sys.exc_info())
            self.common_config.remove_temporary_files()
            sys.exit(1)

        log.info("Connected successful against DBImport database")

    def disconnect_dbimport_db(self):
        """Disconnects from the database and removes all sessions and engine"""
        log = logging.getLogger("atlasDiscovery")

        if self.config_db_engine != None:
            log.info("Disconnecting from DBImport database")
            self.config_db_engine.dispose()
            self.config_db_engine = None

        self.configDBSession = None
