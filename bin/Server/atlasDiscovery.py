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
        self.jdbcConnectionMutex = mutex
        self.atlasCrawlerProcessQueue = atlasCrawlerProcessQueue
        self.atlasCrawlerResultQueue = atlasCrawlerResultQueue
        self.threadStopEvent = threadStopEvent
        self.loggerName = loggerName
        self.atlasOperation = atlas_operations.atlasOperation(loggerName)

    # 		self.sourceSchema = schemaReader.source()
    # 		self.JDBCConn = None
    # 		self.JDBCCursor = None

    def run(self):
        log = logging.getLogger(self.loggerName)
        log.info("Atlas crawler thread %s started" % (self.name))
        self.atlasOperation.testVariable = self.name
        self.atlasCrawlerRequest = None

        if self.atlasOperation.checkAtlasSchema(logger=self.loggerName) == False:
            return False

        while not self.threadStopEvent.isSet():
            try:
                self.atlasCrawlerRequest = self.atlasCrawlerProcessQueue.get(
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
                self.atlasOperation.setConfiguration(
                    self.atlasCrawlerRequest, self.jdbcConnectionMutex
                )
                atlasOperationResult = self.atlasOperation.discoverAtlasRdbms()
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

        log.info("Atlas crawler thread %s stopped" % (self.name))


class atlasDiscovery(threading.Thread):
    def __init__(self, threadStopEvent):
        threading.Thread.__init__(self)
        self.threadStopEvent = threadStopEvent

    def run(self):
        logger = "atlasDiscovery"
        log = logging.getLogger(logger)
        self.mysql_conn = None
        self.mysql_cursor = None
        self.configDBSession = None
        self.configDBEngine = None
        self.debugLogLevel = False
        atlasEnabled = True
        # 		self.atlasOperation = atlas_operations.atlasOperation(logger)

        if logging.root.level == 10:  # DEBUG
            self.debugLogLevel = True

        self.atlasCrawlerProcessQueue = Queue()
        self.atlasCrawlerResultQueue = Queue()
        self.jdbcConnectionMutex = threading.Lock()

        # Fetch configuration about MySQL database and how to connect to it
        self.configHostname = configuration.get("Database", "mysql_hostname")
        self.configPort = configuration.get("Database", "mysql_port")
        self.configDatabase = configuration.get("Database", "mysql_database")
        self.configUsername = configuration.get("Database", "mysql_username")
        self.configPassword = configuration.get("Database", "mysql_password")

        atlasCrawlerObjects = []
        atlasCrawlerThreads = int(configuration.get("Server", "atlas_threads"))
        if atlasCrawlerThreads == 0:
            log.info("Atlas discovery disabled as the number of threads is set to 0")
            atlasEnabled = False
        else:
            log.info("Starting %s Atlas crawler threads" % (atlasCrawlerThreads))

        for threadID in range(0, atlasCrawlerThreads):
            # 			if distCP_separate_logs == False:
            atlasCrawlerLogName = "atlasCrawler-thread%s" % (str(threadID))
            # 			else:
            # 				distCPlogName = "distCP-thread%s"%(str(threadID))

            thread = atlasCrawler(
                name=str(threadID),
                atlasCrawlerProcessQueue=self.atlasCrawlerProcessQueue,
                atlasCrawlerResultQueue=self.atlasCrawlerResultQueue,
                threadStopEvent=self.threadStopEvent,
                loggerName=atlasCrawlerLogName,
                mutex=self.jdbcConnectionMutex,
            )
            thread.daemon = True
            thread.start()
            atlasCrawlerObjects.append(thread)

        self.common_config = common_config.config()
        jdbcConnections = aliased(configSchema.jdbcConnections)
        self.failureLog = {}
        self.connectionsSentToCrawlers = []

        # The interval between the scans. This is in hours
        atlasDiscoveryInterval = self.common_config.getConfigValue(
            key="atlas_discovery_interval"
        )

        # 		if atlasEnabled == True:
        # 			atlasEnabled = self.atlasOperation.checkAtlasSchema(logger=logger)

        if atlasEnabled == True:
            log.info("atlasDiscovery started")
            log.info(
                "Atlas discovery interval is set to %s hours" % (atlasDiscoveryInterval)
            )

        while not self.threadStopEvent.isSet() and atlasEnabled == True:
            # ****************************************************************
            # Read data from jdbc_connection and put in queue for processing
            # ****************************************************************

            if self.atlasCrawlerProcessQueue.qsize() < atlasCrawlerThreads:
                # Only read the database if there isn't enough items in the queue to the crawlers to processes. This will save
                # a large number of sql requests if the queue is full
                try:
                    # Read a list of connection aliases that we are going to process in this iteration
                    session = self.getDBImportSession()
                    atlasDiscoveryCheckTime = datetime.utcnow() - timedelta(
                        hours=atlasDiscoveryInterval
                    )

                    # TODO: Antagligen bara köra denna om jdbcConnectionsDf är tom från föregående körning
                    jdbcConnectionsDf = pd.DataFrame(
                        session.query(
                            jdbcConnections.dbalias,
                            jdbcConnections.atlas_last_discovery,
                            jdbcConnections.atlas_discovery,
                            jdbcConnections.contact_info,
                            jdbcConnections.description,
                            jdbcConnections.owner,
                            jdbcConnections.atlas_include_filter,
                            jdbcConnections.atlas_exclude_filter,
                        )
                        .select_from(jdbcConnections)
                        .filter(jdbcConnections.atlas_discovery == 1)
                        # 						.filter((jdbcConnections.atlas_last_discovery < atlasDiscoveryCheckTime) | (jdbcConnections.atlas_last_discovery == None))
                        .order_by(jdbcConnections.atlas_last_discovery)
                        .all()
                    )
                    session.close()

                except SQLAlchemyError as e:
                    log.error(str(e.__dict__["orig"]))
                    session.rollback()
                    self.disconnectDBImportDB()

                else:
                    for index, row in jdbcConnectionsDf.iterrows():
                        dbAlias = row["dbalias"]

                        # TODO: Flytta denna till thread
                        if (
                            self.common_config.checkTimeWindow(
                                dbAlias, atlasDiscoveryMode=True
                            )
                            == False
                        ):
                            continue

                        # Find out if the dbAlias is blacklisted
                        if self.isConnectionBlacklisted(dbAlias) == True:
                            continue

                        if dbAlias in self.connectionsSentToCrawlers:
                            # 							log.warning("This connection is already being processed. Skipping....")
                            continue

                        altasOperationFailed = False
                        printBlackListWarning = True

                        self.common_config.mysql_conn.commit()
                        try:
                            self.common_config.lookupConnectionAlias(dbAlias)
                        except invalidConfiguration as err:
                            if self.common_config.atlasJdbcSourceSupport == True:
                                log.error(
                                    "Connection '%s' have invalid configuration. Failed with '%s'"
                                    % (dbAlias, err)
                                )
                                altasOperationFailed = True

                        if self.common_config.atlasJdbcSourceSupport == False:
                            # This source type does not support Atlas discovery
                            log.debug(
                                "Connection '%s' does not support Atlas discovery. Skipping..."
                                % (dbAlias)
                            )
                            altasOperationFailed = True
                            printBlackListWarning = False

                        # Start the Jpype JVM as that needs to be running before the crawlers starts to use it
                        if jpype.isJVMStarted() == False:
                            log.debug("Starting jpype JVM")
                            self.common_config.connectToJDBC(
                                allJarFiles=True,
                                exitIfFailure=False,
                                logger=logger,
                                printError=False,
                            )
                            self.common_config.disconnectFromJDBC()

                        # 						if altasOperationFailed == False and self.common_config.connectToJDBC(allJarFiles=True, exitIfFailure=False, logger=logger) == True:
                        if altasOperationFailed == False:
                            # 							self.common_config.atlasEnabled = True
                            self.connectionsSentToCrawlers.append(dbAlias)

                            log.debug("Sending alias '%s' to queue" % (dbAlias))
                            atlasCrawlerRequest = {}
                            atlasCrawlerRequest["dbAlias"] = row["dbalias"]
                            atlasCrawlerRequest["contactInfo"] = row["contact_info"]
                            atlasCrawlerRequest["description"] = row["description"]
                            atlasCrawlerRequest["owner"] = row["owner"]
                            atlasCrawlerRequest["atlasIncludeFilter"] = row[
                                "atlas_include_filter"
                            ]
                            atlasCrawlerRequest["atlasExcludeFilter"] = row[
                                "atlas_exclude_filter"
                            ]
                            atlasCrawlerRequest["jdbc_hostname"] = (
                                self.common_config.jdbc_hostname
                            )
                            atlasCrawlerRequest["jdbc_port"] = (
                                self.common_config.jdbc_port
                            )
                            atlasCrawlerRequest["jdbc_servertype"] = (
                                self.common_config.jdbc_servertype
                            )
                            atlasCrawlerRequest["jdbc_database"] = (
                                self.common_config.jdbc_database
                            )
                            atlasCrawlerRequest["jdbc_oracle_sid"] = (
                                self.common_config.jdbc_oracle_sid
                            )
                            atlasCrawlerRequest["jdbc_oracle_servicename"] = (
                                self.common_config.jdbc_oracle_servicename
                            )
                            atlasCrawlerRequest["jdbc_username"] = (
                                self.common_config.jdbc_username
                            )
                            atlasCrawlerRequest["jdbc_password"] = (
                                self.common_config.jdbc_password
                            )
                            atlasCrawlerRequest["jdbc_driver"] = (
                                self.common_config.jdbc_driver
                            )
                            atlasCrawlerRequest["jdbc_url"] = (
                                self.common_config.jdbc_url
                            )
                            atlasCrawlerRequest["jdbc_classpath_for_python"] = (
                                self.common_config.jdbc_classpath_for_python
                            )
                            atlasCrawlerRequest["jdbc_environment"] = (
                                self.common_config.jdbc_environment
                            )
                            atlasCrawlerRequest["hdfs_address"] = None
                            atlasCrawlerRequest["cluster_name"] = None

                            self.atlasCrawlerProcessQueue.put(atlasCrawlerRequest)

                        else:
                            # 							altasOperationFailed = True

                            # 						if altasOperationFailed == True:
                            self.blacklistConnection(dbAlias, printBlackListWarning)

            # ********************************
            # Read response from atlasCrawler
            # ********************************

            try:
                atlasCrawlerResult = self.atlasCrawlerResultQueue.get(
                    block=False, timeout=1
                )
            except Empty:
                atlasCrawlerResult = None

            if atlasCrawlerResult is not None:
                dbAlias = atlasCrawlerResult.get("dbAlias")
                result = atlasCrawlerResult.get("result")
                blacklist = atlasCrawlerResult.get("blacklist")
                log.debug("atlasCrawlerResultQueue: %s" % (atlasCrawlerResult))

                self.connectionsSentToCrawlers.remove(dbAlias)

                if result == True:
                    updateDict = {}
                    updateDict["atlas_last_discovery"] = str(
                        datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
                    )

                    try:
                        session = self.getDBImportSession()
                        (
                            session.query(configSchema.jdbcConnections)
                            .filter(configSchema.jdbcConnections.dbalias == dbAlias)
                            .update(updateDict)
                        )
                        session.commit()
                        session.close()

                    except SQLAlchemyError as e:
                        log.error(str(e.__dict__["orig"]))
                        session.rollback()
                        self.disconnectDBImportDB()

                    else:
                        self.removeBlacklist(dbAlias)
                else:
                    if blacklist == True:
                        log.error(
                            "Connection '%s' failed during crawling of database schema"
                            % (dbAlias)
                        )
                        self.blacklistConnection(dbAlias)
                    else:
                        log.warning(
                            "A Warning was detected when crawling connection '%s'. It will not be marked as completed and will retry the operation"
                            % (dbAlias)
                        )

            time.sleep(1)

        self.disconnectDBImportDB()
        if atlasEnabled == True:
            log.info("atlasDiscovery stopped")

    def isConnectionBlacklisted(self, dbAlias):
        if self.failureLog.get(dbAlias, None) != None:
            blackListEnableTime = self.failureLog[dbAlias][
                "blackListStart"
            ] + timedelta(hours=self.failureLog[dbAlias]["blackListTime"])

            if datetime.now() < blackListEnableTime:
                # This dbAlias is still blacklisted
                return True

        return False

    def removeBlacklist(self, dbAlias):
        self.failureLog.pop(dbAlias, None)

    def blacklistConnection(self, dbAlias, printBlackListWarning=True):
        log = logging.getLogger("atlasDiscovery")

        blackListData = self.failureLog.get(dbAlias, None)
        if blackListData == None:
            blackListTime = 1
        else:
            blackListTime = self.failureLog[dbAlias]["blackListTime"] * 2

            # Max blacklist time is 24 hours
            if blackListTime > 24:
                blackListTime = 24

        self.failureLog[dbAlias] = {
            "blackListTime": blackListTime,
            "blackListStart": datetime.now(),
        }

        if printBlackListWarning == True:
            log.warning(
                "Connection '%s' is blacklisted for %s hours"
                % (dbAlias, self.failureLog[dbAlias]["blackListTime"])
            )

    def getDBImportSession(self):
        log = logging.getLogger("atlasDiscovery")
        if self.configDBSession == None:
            self.connectDBImportDB()

        return self.configDBSession()

    def connectDBImportDB(self):
        # Esablish a SQLAlchemy connection to the DBImport database
        log = logging.getLogger("atlasDiscovery")
        self.connectStr = "mysql+pymysql://%s:%s@%s:%s/%s" % (
            self.configUsername,
            self.configPassword,
            self.configHostname,
            self.configPort,
            self.configDatabase,
        )

        try:
            self.configDBEngine = sa.create_engine(
                self.connectStr, echo=self.debugLogLevel
            )
            self.configDBEngine.connect()
            self.configDBSession = sessionmaker(bind=self.configDBEngine)

        except sa.exc.OperationalError as err:
            log.error("%s" % err)
            self.common_config.remove_temporary_files()
            sys.exit(1)
        except:
            print("Unexpected error: ")
            print(sys.exc_info())
            self.common_config.remove_temporary_files()
            sys.exit(1)

        log.info("Connected successful against DBImport database")

    def disconnectDBImportDB(self):
        """Disconnects from the database and removes all sessions and engine"""
        log = logging.getLogger("atlasDiscovery")

        if self.configDBEngine != None:
            log.info("Disconnecting from DBImport database")
            self.configDBEngine.dispose()
            self.configDBEngine = None

        self.configDBSession = None
