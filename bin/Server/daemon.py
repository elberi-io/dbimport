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

# import Queue
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
        self, name, distCPreqQueue, distCPresQueue, threadStopEvent, loggerName
    ):
        threading.Thread.__init__(self)
        self.name = name
        self.distCPreqQueue = distCPreqQueue
        self.distCPresQueue = distCPresQueue
        self.threadStopEvent = threadStopEvent
        self.loggerName = loggerName

    def run(self):
        log = logging.getLogger(self.loggerName)
        log.info("distCP %s started" % (self.name))

        yarnQueue = configuration.get("Server", "distCP_yarnqueue")

        while not self.threadStopEvent.isSet():
            distCPrequest = self.distCPreqQueue.get()
            if distCPrequest is None:
                time.sleep(1)
                break

            tableID = distCPrequest.get("tableID")
            hiveDB = distCPrequest.get("hiveDB")
            hiveTable = distCPrequest.get("hiveTable")
            destination = distCPrequest.get("destination")
            failures = distCPrequest.get("failures")
            HDFSsourcePath = distCPrequest.get("HDFSsourcePath")
            HDFStargetPath = distCPrequest.get("HDFStargetPath")

            log.info(
                "Thread %s: Starting a new distCP copy with the following paramaters"
                % (self.name)
            )
            log.info(
                "Thread %s: --------------------------------------------------------"
                % (self.name)
            )
            log.info("Thread %s: tableID = %s" % (self.name, tableID))
            log.info("Thread %s: hiveDB = %s" % (self.name, hiveDB))
            log.info("Thread %s: hiveTable = %s" % (self.name, hiveTable))
            log.info("Thread %s: destination = %s" % (self.name, destination))
            log.info("Thread %s: HDFSsourcePath = %s" % (self.name, HDFSsourcePath))
            log.info("Thread %s: HDFStargetPath = %s" % (self.name, HDFStargetPath))
            log.info(
                "Thread %s: --------------------------------------------------------"
                % (self.name)
            )

            distcpCommand = [
                "hadoop",
                "distcp",
                "-D",
                "yarn.timeline-service.enabled=false",
                "-D",
                "mapreduce.job.queuename=%s" % (yarnQueue),
                "-overwrite",
                "-delete",
                "%s" % (HDFSsourcePath),
                "%s" % (HDFStargetPath),
            ]

            log.info("Thread %s:  ______________________ " % (self.name))
            log.info("Thread %s: |                      |" % (self.name))
            log.info("Thread %s: | Hadoop distCp starts |" % (self.name))
            log.info("Thread %s: |______________________|" % (self.name))
            log.info("Thread %s: " % (self.name))

            # Start distcp
            sh_session = subprocess.Popen(
                distcpCommand, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
            )
            distCPoutput = ""

            # Print Stdout and stderr while distcp is running
            while sh_session.poll() == None:
                row = sh_session.stdout.readline().decode("utf-8").rstrip()
                if row != "":
                    log.info("Thread %s: %s" % (self.name, row))
                    distCPoutput += row + "\n"
                    sys.stdout.flush()

            # Print what is left in output after distcp is finished
            for row in sh_session.stdout.readlines():
                row = row.decode("utf-8").rstrip()
                if row != "":
                    log.info("Thread %s: %s" % (self.name, row))
                    distCPoutput += row + "\n"
                    sys.stdout.flush()

            log.info("Thread %s:  _________________________ " % (self.name))
            log.info("Thread %s: |                         |" % (self.name))
            log.info("Thread %s: | Hadoop distCp completed |" % (self.name))
            log.info("Thread %s: |_________________________|" % (self.name))
            log.info("Thread %s: " % (self.name))

            disCPresult = False
            if " ERROR " in distCPoutput:
                log.error("Thread %s: ERROR detected during distCP copy." % (self.name))
                failures = failures + 1
            elif " completed successfully" in distCPoutput:
                disCPresult = True
                failures = 0
            else:
                log.error(
                    "Thread %s: Unknown status of distCP. Marked as failure as it cant find that it was finished successful"
                    % (self.name)
                )
                failures = failures + 1

            distCPresponse = {}
            distCPresponse["tableID"] = tableID
            distCPresponse["hiveDB"] = hiveDB
            distCPresponse["hiveTable"] = hiveTable
            distCPresponse["destination"] = destination
            distCPresponse["result"] = disCPresult
            distCPresponse["failures"] = failures

            self.distCPresQueue.put(distCPresponse)

        log.info("distCP %s stopped" % (self.name))


class serverDaemon(run.RunDaemon):
    def setConfig(self, action, logFormat):
        self.action = action
        self.logFormat = logFormat

    def run(self):
        # This is the main event loop where the 'real' daemonwork happens
        log = logging.getLogger("server")
        log.debug("Executing daemon.serverDaemon.run()")
        log.info("Server initializing")
        self.mysql_conn = None
        self.mysql_cursor = None
        self.debugLogLevel = False

        if logging.root.level == 10:  # DEBUG
            self.debugLogLevel = True

        self.common_config = common_config.config()

        self.logdir = configuration.get("Server", "logdir")

        self.crypto = self.common_config.crypto
        self.crypto.setPrivateKeyFile(configuration.get("Credentials", "private_key"))
        self.crypto.setPublicKeyFile(configuration.get("Credentials", "public_key"))

        self.remoteDBImportEngines = {}
        self.remoteDBImportSessions = {}
        self.remoteInstanceConfigDB = None

        self.configDBSession = None
        self.configDBEngine = None

        self.distCPreqQueue = Queue()
        self.distCPresQueue = Queue()
        self.threadStopEvent = threading.Event()

        # Start the Atlas Discovery Thread
        self.atlasDiscoveryThread = atlasDiscovery.atlasDiscovery(self.threadStopEvent)
        self.atlasDiscoveryThread.daemon = True
        self.atlasDiscoveryThread.start()

        # Start the distCP threads
        if configuration.get("Server", "distCP_separate_logs").lower() == "true":
            distCP_separate_logs = True
        else:
            distCP_separate_logs = False

        distCPobjects = []
        distCPthreads = int(configuration.get("Server", "distCP_threads"))
        if distCPthreads == 0:
            log.info("Distcp disabled as the number of threads is set to 0")
        else:
            log.info("Starting %s distCp threads" % (distCPthreads))

        for threadID in range(0, distCPthreads):
            if distCP_separate_logs == False:
                distCPlogName = "distCP"
            else:
                distCPlogName = "distCP-thread%s" % (str(threadID))

            thread = distCP(
                name=str(threadID),
                distCPreqQueue=self.distCPreqQueue,
                distCPresQueue=self.distCPresQueue,
                threadStopEvent=self.threadStopEvent,
                loggerName=distCPlogName,
            )
            thread.daemon = True
            thread.start()
            distCPobjects.append(thread)

        # Fetch configuration about MySQL database and how to connect to it
        self.configHostname = configuration.get("Database", "mysql_hostname")
        self.configPort = configuration.get("Database", "mysql_port")
        self.configDatabase = configuration.get("Database", "mysql_database")
        self.configUsername = configuration.get("Database", "mysql_username")
        self.configPassword = configuration.get("Database", "mysql_password")

        # Set all rows that have copy_status = 1 to 0. This is needed in the startup as if they are 1 in this stage, it means that a previous
        # server marked it as 1 but didnt finish the copy. We need to retry that copy here and now
        if distCPthreads > 0:
            try:
                updateDict = {}
                updateDict["last_status_update"] = str(
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
                )
                updateDict["copy_status"] = 0

                session = self.getDBImportSession()

                (
                    session.query(configSchema.copyASyncStatus)
                    .filter(configSchema.copyASyncStatus.copy_status == 1)
                    .update(updateDict)
                )
                session.commit()
                session.close()

                log.debug("Init part of daemon.serverDaemon.run() completed")

                log.info("Server startup completed")

            except SQLAlchemyError as e:
                log.error(str(e.__dict__["orig"]))
                log.error("Server startup failed")
                self.disconnectDBImportDB()

                # As we require this operation to be completed successful before entering the main loop, we will exit if there is a problem
                self.common_config.remove_temporary_files()
                sys.exit(1)

            except SQLerror:
                log.error("Server startup failed. Cant connect to config database")
                self.disconnectDBImportDB()
                self.common_config.remove_temporary_files()
                sys.exit(1)

        importTables = aliased(configSchema.importTables)
        dbimportInstances = aliased(configSchema.dbimportInstances)
        copyASyncStatus = aliased(configSchema.copyASyncStatus)

        while True:
            # ***********************************
            # Main Loop for server
            # ***********************************

            if distCPthreads > 0:
                try:
                    session = self.getDBImportSession()

                    # status 0 = New data from import
                    # status 1 = Data sent to distCP thread
                    # status 2 = Data returned from distCP and was a failure
                    # status 3 = Data returned from distCP and was a success

                    # ------------------------------------------
                    # Fetch all rows from copyASyncStatus that contains the status 0 and send them to distCP threads
                    # ------------------------------------------

                    # TODO: make the 1 min interval a configured param
                    status2checkTimestamp = (
                        datetime.now() - timedelta(minutes=1)
                    ).strftime("%Y-%m-%d %H:%M:%S.%f")

                    aSyncRow = pd.DataFrame(
                        session.query(
                            copyASyncStatus.table_id,
                            copyASyncStatus.hive_db,
                            copyASyncStatus.hive_table,
                            copyASyncStatus.destination,
                            copyASyncStatus.failures,
                            copyASyncStatus.hdfs_source_path,
                            copyASyncStatus.hdfs_target_path,
                        )
                        .select_from(copyASyncStatus)
                        .filter(
                            (copyASyncStatus.copy_status == 0)
                            | (
                                (copyASyncStatus.copy_status == 2)
                                & (
                                    copyASyncStatus.last_status_update
                                    <= status2checkTimestamp
                                )
                            )
                        )
                        .all()
                    )

                    for index, row in aSyncRow.iterrows():
                        tableID = row["table_id"]
                        destination = row["destination"]
                        hiveDB = row["hive_db"]
                        hiveTable = row["hive_table"]
                        failures = row["failures"]
                        HDFSsourcePath = row["hdfs_source_path"]
                        HDFStargetPath = row["hdfs_target_path"]

                        log.info(
                            "New sync request for table %s.%s" % (hiveDB, hiveTable)
                        )

                        updateDict = {}
                        updateDict["copy_status"] = 1
                        updateDict["last_status_update"] = str(
                            datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
                        )

                        (
                            session.query(configSchema.copyASyncStatus)
                            .filter(configSchema.copyASyncStatus.table_id == tableID)
                            .filter(
                                configSchema.copyASyncStatus.destination == destination
                            )
                            .update(updateDict)
                        )
                        session.commit()

                        distCPrequest = {}
                        distCPrequest["tableID"] = tableID
                        distCPrequest["hiveDB"] = hiveDB
                        distCPrequest["hiveTable"] = hiveTable
                        distCPrequest["destination"] = destination
                        distCPrequest["failures"] = failures
                        distCPrequest["HDFSsourcePath"] = HDFSsourcePath
                        distCPrequest["HDFStargetPath"] = HDFStargetPath
                        self.distCPreqQueue.put(distCPrequest)

                        log.debug(
                            "Status changed to 1 for table %s.%s and sent to distCP threads"
                            % (hiveDB, hiveTable)
                        )

                    session.close()

                except SQLAlchemyError as e:
                    log.error(str(e.__dict__["orig"]))
                    session.rollback()
                    self.disconnectDBImportDB()

                except SQLerror:
                    self.disconnectDBImportDB()

                # ------------------------------------------
                # Read the response from the distCP threads
                # ------------------------------------------
                try:
                    distCPresponse = self.distCPresQueue.get(block=False)
                except Empty:
                    pass
                else:
                    updateDict = {}
                    updateDict["last_status_update"] = str(
                        datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
                    )
                    updateDict["failures"] = distCPresponse.get("failures")

                    distCPresult = distCPresponse.get("result")
                    if distCPresult == True:
                        updateDict["copy_status"] = 3
                    else:
                        updateDict["copy_status"] = 2

                    try:
                        session = self.getDBImportSession()
                        (
                            session.query(configSchema.copyASyncStatus)
                            .filter(
                                configSchema.copyASyncStatus.table_id
                                == distCPresponse.get("tableID")
                            )
                            .filter(
                                configSchema.copyASyncStatus.destination
                                == distCPresponse.get("destination")
                            )
                            .update(updateDict)
                        )
                        session.commit()
                        session.close()

                    except SQLAlchemyError as e:
                        log.error(str(e.__dict__["orig"]))
                        session.rollback()
                        self.disconnectDBImportDB()

                    except SQLerror:
                        self.disconnectDBImportDB()

                # ------------------------------------------
                # Fetch all rows from copyASyncStatus that contains the status 3 and update the remote DBImport instance database
                # Also dlete the record from the copyASyncStatus table
                # ------------------------------------------

                try:
                    session = self.getDBImportSession()
                    aSyncRow = pd.DataFrame(
                        session.query(
                            copyASyncStatus.table_id,
                            copyASyncStatus.hive_db,
                            copyASyncStatus.hive_table,
                            copyASyncStatus.destination,
                            copyASyncStatus.failures,
                            copyASyncStatus.hdfs_source_path,
                            copyASyncStatus.hdfs_target_path,
                        )
                        .select_from(copyASyncStatus)
                        .filter(copyASyncStatus.copy_status == 3)
                        .all()
                    )
                    session.close()

                except SQLAlchemyError as e:
                    log.error(str(e.__dict__["orig"]))
                    session.rollback()
                    self.disconnectDBImportDB()

                except SQLerror:
                    self.disconnectDBImportDB()

                else:
                    for index, row in aSyncRow.iterrows():
                        tableID = row["table_id"]
                        destination = row["destination"]
                        hiveDB = row["hive_db"]
                        hiveTable = row["hive_table"]
                        failures = row["failures"]
                        HDFSsourcePath = row["hdfs_source_path"]
                        HDFStargetPath = row["hdfs_target_path"]

                        # Get the remote sessions. if sessions is not available, we just continue to the next item in the database
                        _remoteSession = self.getDBImportRemoteSession(destination)
                        if _remoteSession == None:
                            continue

                        try:
                            remoteSession = _remoteSession()

                            # Get the table_id from the table at the remote instance
                            remoteImportTableID = (
                                remoteSession.query(importTables.table_id)
                                .select_from(importTables)
                                .filter(importTables.hive_db == hiveDB)
                                .filter(importTables.hive_table == hiveTable)
                                .one()
                            )

                            remoteTableID = remoteImportTableID[0]

                            updateDict = {}
                            updateDict["copy_finished"] = str(
                                datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
                            )

                            # Update the values in import_table on the remote instance
                            (
                                remoteSession.query(configSchema.importTables)
                                .filter(
                                    configSchema.importTables.table_id == remoteTableID
                                )
                                .update(updateDict)
                            )
                            remoteSession.commit()

                            remoteSession.close()

                        except SQLAlchemyError as e:
                            log.error(str(e.__dict__["orig"]))
                            remoteSession.rollback()
                            self.disconnectRemoteSession(destination)

                        else:
                            # Delete the record from copyASyncStatus
                            try:
                                session = self.getDBImportSession()
                                (
                                    session.query(configSchema.copyASyncStatus)
                                    .filter(
                                        configSchema.copyASyncStatus.table_id == tableID
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
                                self.disconnectDBImportDB()

                            except SQLerror:
                                self.disconnectDBImportDB()

                            else:
                                log.info(
                                    "Table %s.%s copied successfully to '%s'"
                                    % (hiveDB, hiveTable, destination)
                                )

                session.close()

            # ***********************************
            # DistCP Part of main loop is finished
            # ***********************************

            # 			log.info("Starting wait")
            time.sleep(1)

        log.info("Server stopped")
        log.debug("Executing daemon.serverDaemon.run() - Finished")

    def disconnectDBImportDB(self):
        """Disconnects from the database and removes all sessions and engine"""
        log = logging.getLogger("server")

        if self.configDBEngine != None:
            log.info("Disconnecting from DBImport database")
            self.configDBEngine.dispose()
            self.configDBEngine = None

        self.configDBSession = None

    def getDBImportSession(self):
        log = logging.getLogger("server")
        if self.configDBSession == None:
            if self.connectDBImportDB() == False:
                raise SQLerror("Can't connect to DBImport database")

        return self.configDBSession()

    def connectDBImportDB(self):
        # Esablish a SQLAlchemy connection to the DBImport database
        log = logging.getLogger("server")
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
            self.configDBSession = None
            self.configDBEngine = None
            return False
        # 			self.common_config.remove_temporary_files()
        # 			sys.exit(1)

        except:
            print("Unexpected error: ")
            print(sys.exc_info())
            self.configDBSession = None
            self.configDBEngine = None
            return False
        # 			self.common_config.remove_temporary_files()
        # 			sys.exit(1)

        log.info("Connected successful against DBImport database")
        return True

    def disconnectRemoteSession(self, instance):
        """Disconnects from the remote database and removes all sessions and engine"""
        log = logging.getLogger("server")

        try:
            engine = self.remoteDBImportEngines.get(instance)
            if engine != None:
                log.info(
                    "Disconnecting from remote DBImport database for '%s'" % (instance)
                )
                engine.dispose()
            self.remoteDBImportEngines.pop(instance)
            self.remoteDBImportSessions.pop(instance)
        except KeyError:
            log.debug("Cant remove DBImport session or engine. Key does not exist")

    def getDBImportRemoteSession(self, instance):
        """Connects to the remote configuration database with SQLAlchemy"""
        log = logging.getLogger("server")

        # A dictionary of all remote DBImport configuration databases are keept in self.remoteDBImportSessions
        # This will make only one sessions to the database and then save that for each and every connection after that
        if instance in self.remoteDBImportSessions:
            return self.remoteDBImportSessions.get(instance)

        log.info("Connecting to remote DBImport database for '%s'" % (instance))

        connectStatus = True

        try:
            session = self.getDBImportSession()
        except SQLerror:
            self.disconnectDBImportDB()
            return None

        dbimportInstances = aliased(configSchema.dbimportInstances)

        row = (
            session.query(
                dbimportInstances.db_hostname,
                dbimportInstances.db_port,
                dbimportInstances.db_database,
                dbimportInstances.db_credentials,
            )
            .filter(dbimportInstances.name == instance)
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
                "Decryption of credentials resulted in error with text: '%s'" % err
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

        instanceConnectStr = "mysql+pymysql://%s:%s@%s:%s/%s" % (
            username,
            password,
            row[0],
            row[1],
            row[2],
        )

        try:
            remoteInstanceConfigDBEngine = sa.create_engine(
                instanceConnectStr, echo=self.debugLogLevel
            )
            remoteInstanceConfigDBEngine.connect()
            remoteInstanceConfigDBSession = sessionmaker(
                bind=remoteInstanceConfigDBEngine
            )

        except sa.exc.OperationalError as err:
            log.error("%s" % err)
            return None
        except:
            log.error("Unexpected error: ")
            log.error(sys.exc_info())
            return None

        self.remoteDBImportEngines[instance] = remoteInstanceConfigDBEngine
        self.remoteDBImportSessions[instance] = remoteInstanceConfigDBSession
        return remoteInstanceConfigDBSession
