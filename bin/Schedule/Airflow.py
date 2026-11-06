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

import json
import logging
import os
import random
import re
import shutil
import sys
from datetime import datetime, time, timedelta
from urllib.parse import quote

import boto3
import botocore
import pandas as pd
import pendulum
import sqlalchemy as sa
from sqlalchemy.orm import Query, aliased, scoped_session, sessionmaker

from common.Exceptions import SQLerror, invalidConfiguration
from ConfigReader import configuration
from DBImportConfig import common_config, configSchema, sendStatistics
from Schedule import airflowSchema


class initialize:
    def __init__(self):
        logging.debug("Executing Airflow.__init__()")

        self.mysql_conn = None
        self.mysql_cursor = None
        self.debugLogLevel = False

        self.logger = "root"
        log = logging.getLogger(self.logger)

        self.configDBSession = None
        self.configDB = None

        if logging.root.level == 10:  # DEBUG
            self.debugLogLevel = True

        self.common_config = common_config.config()
        self.sendStatistics = sendStatistics.sendStatistics()

        self.dbimportCommandPath = self.getConfigValue("airflow_dbimport_commandpath")
        self.defaultSudoUser = self.getConfigValue("airflow_sudo_user")
        self.DAGdirectory = self.getConfigValue("airflow_dag_directory")
        self.DAGstagingDirectory = self.getConfigValue("airflow_dag_staging_directory")
        self.DAGfileGroup = self.getConfigValue("airflow_dag_file_group")
        self.DAGfilePermission = self.getConfigValue("airflow_dag_file_permission")
        self.TaskQueueForDummy = self.getConfigValue("airflow_dummy_task_queue")
        self.airflowMajorVersion = self.getConfigValue("airflow_major_version")
        self.defaultTimeZone = self.getConfigValue("timezone")
        self.airflowAWSinstanceIds = self.getConfigValue("airflow_aws_instanceids")
        self.airflowAWSpoolToInstanceId = self.getConfigValue(
            "airflow_aws_pool_to_instanceid"
        )
        self.airflowDefaultPoolSize = self.getConfigValue("airflow_default_pool_size")
        self.createPoolsWithTasks = self.getConfigValue("airflow_create_pool_with_task")

        self.DAGfile = None
        self.DAGfilename = None
        self.DAGfilenameInAirflow = None
        self.DAGfilenameInAwsMWAA = None
        self.writeDAG = None

        self.sensorStartTask = None
        self.sensorStopTask = None
        self.preStartTask = None
        self.preStopTask = None
        self.mainStartTask = None
        self.mainStopTask = None
        self.postStartTask = None
        self.postStopTask = None

        self.airflowMode = "default"

        self.createdPools = []
        self.AWSinstanceIdAssignedPools = {}

        try:
            self.airflowMode = configuration.get(
                "Airflow", "airflow_mode", exitOnError=False
            )
            if self.airflowMode != "default" and self.airflowMode != "aws_mwaa":
                raise invalidConfiguration(
                    "Invalid option specified for [Airflow][airflow_mode] in the configuration file."
                )
        # 				log.error("Invalid option specified for [Airflow][airflow_mode] in the configuration file.")
        # 				self.common_config.remove_temporary_files()
        # 				sys.exit(1)

        except invalidConfiguration:
            pass

        if self.airflowMode == "default" and self.createPoolsWithTasks == False:
            # Establish a SQLAlchemy connection to the Airflow database
            airflow_connect_str = configuration.get("Airflow", "airflow_alchemy_conn")
            try:
                self.airflowDB = sa.create_engine(
                    airflow_connect_str, echo=self.debugLogLevel
                )
                self.airflowDB.connect()
                self.airflowDBSession = sessionmaker(bind=self.airflowDB)

            except sa.exc.OperationalError as err:
                raise SQLerror(err)
            # 				log.error("%s"%err)
            # 				self.common_config.remove_temporary_files()
            # 				sys.exit(1)
            except:
                raise SQLerror(sys.exc_info())
        # 				print("Unexpected error: ")
        # 				print(sys.exc_info())
        # 				self.common_config.remove_temporary_files()
        # 				sys.exit(1)

        log.debug("Executing Airflow.__init__() - Finished")

    def disconnectDBImportDB(self):
        """Disconnects from the database and removes all sessions and engine"""
        log = logging.getLogger(self.logger)

        if self.configDB != None:
            log.info("Disconnecting from DBImport database")
            self.configDB.dispose()
            self.configDB = None

        self.configDBSession = None

    def getDBImportSession(self):
        """Connects to the configuration database with SQLAlchemy and return a session"""
        log = logging.getLogger(self.logger)

        if self.configDBSession != None:
            # If we already have a connection, we just return the session
            # return self.configDBSession()
            return scoped_session(self.configDBSession)

        mysql_credentials = self.common_config.getMysqlCredentials()

        self.connectStr = "mysql+pymysql://%s:%s@%s:%s/%s" % (
            mysql_credentials["mysql_username"],
            quote(mysql_credentials["mysql_password"], safe=" +"),
            mysql_credentials["mysql_hostname"],
            mysql_credentials["mysql_port"],
            mysql_credentials["mysql_database"],
        )

        try:
            self.configDB = sa.create_engine(
                self.connectStr, echo=self.debugLogLevel, pool_pre_ping=True
            )
            self.configDB.connect()
            self.configDBSession = sessionmaker(bind=self.configDB)

        except sa.exc.OperationalError as err:
            log.error("%s" % err)
            self.configDBSession = None
            self.configDB = None
            raise SQLerror("Can't connect to DBImport database")

        except:
            print("Unexpected error: ")
            print(sys.exc_info())
            self.configDBSession = None
            self.configDB = None
            raise SQLerror("Can't connect to DBImport database")

        else:
            # return self.configDBSession()
            return scoped_session(self.configDBSession)

    def getConfigValue(self, key):
        """Returns a value from the configuration table based on the supplied key. Value returned can be Int, Str or DateTime"""
        log = logging.getLogger(self.logger)

        return_value = None
        bool_value = False

        value_column, bool_value = self.common_config.getConfigValueColumn(key)

        try:
            session = self.getDBImportSession()
        except SQLerror:
            self.disconnectDBImportDB()
            return None

        configuration_table = aliased(configSchema.configuration)

        configuration = (
            session.query(
                configuration_table.configKey,
                configuration_table.valueInt,
                configuration_table.valueStr,
                configuration_table.valueDate,
            )
            .select_from(configuration_table)
            .filter(configuration_table.configKey == key)
            .one()
        )

        session.remove()
        return_value = None

        try:
            if value_column == "valueInt":
                return_value = int(configuration[1])

            if value_column == "valueStr":
                return_value = configuration[2]
                if return_value == None or return_value.strip() == "":
                    log.error(
                        "Configuration Key '%s' must have a value in '%s'"
                        % (key, value_column)
                    )
                    return_value = None

            if bool_value == True:
                if return_value == 1:
                    return_value = True
                elif return_value == 0:
                    return_value = False
                else:
                    log.error(
                        "Configuration Key '%s' can only have 0 or 1 in column '%s'"
                        % (key, value_column)
                    )
                    return_value = None
        except TypeError:
            log.error(
                "Configuration key '%s' cant be found. Have you upgraded the database schema to the latest version?"
                % (key)
            )
            return_value = None

        return return_value

    def checkExecution(self):
        """Checks the 'airflow_disable' settings and exit with 0 or 1 depending on that"""

        airflow_execution_disabled = self.getConfigValue("airflow_disable")
        if airflow_execution_disabled == False:
            print("Airflow execution is enabled")
            self.common_config.remove_temporary_files()
            sys.exit(0)
        else:
            print("Airflow execution is disabled")
            self.common_config.remove_temporary_files()
            sys.exit(1)

    def getDBImportCommandPath(self, sudoUser=""):

        if sudoUser == None or sudoUser == "":
            sudoUser = self.defaultSudoUser

        return self.dbimportCommandPath.replace("${SUDO_USER}", sudoUser)

    def generateDAGfromREST(self, name, currentUser):
        log = logging.getLogger(self.logger)

        try:
            self.generateDAG(name=name, writeDAG=True)
        except (invalidConfiguration, SQLerror, RuntimeError) as errMsg:
            result = str(errMsg)
            return_code = 500
        except:
            log.error(sys.exc_info())
            result = str(sys.exc_info())
            return_code = 500
        else:
            result = "Airflow DAG generated"
            return_code = 200

        return (result, return_code)

    def generateDAG(self, name=None, writeDAG=False, autoDAGonly=False, DAGFolder=None):
        log = logging.getLogger(self.logger)

        self.writeDAG = writeDAG
        self.DAGFolder = DAGFolder

        # session = self.configDBSession()
        session = self.getDBImportSession()
        airflow_custom_dags = aliased(configSchema.airflowCustomDags)
        airflow_export_dags = aliased(configSchema.airflowExportDags)
        airflow_import_dags = aliased(configSchema.airflowImportDags)
        airflow_etl_dags = aliased(configSchema.airflowEtlDags)

        export_dag = pd.DataFrame(
            session.query(
                airflow_export_dags.dag_name,
                airflow_export_dags.schedule_interval,
                airflow_export_dags.filter_dbalias,
                airflow_export_dags.filter_target_schema,
                airflow_export_dags.filter_target_table,
                airflow_export_dags.retries,
                airflow_export_dags.auto_regenerate_dag,
                airflow_export_dags.sudo_user,
                airflow_export_dags.timezone,
                airflow_export_dags.email,
                airflow_export_dags.email_on_failure,
                airflow_export_dags.email_on_retries,
                airflow_export_dags.tags,
                airflow_export_dags.sla_warning_time,
                airflow_export_dags.retry_exponential_backoff,
                airflow_export_dags.concurrency,
            )
            .select_from(airflow_export_dags)
            .all()
        ).fillna("")

        import_dag = pd.DataFrame(
            session.query(
                airflow_import_dags.dag_name,
                airflow_import_dags.schedule_interval,
                airflow_import_dags.filter_hive,
                airflow_import_dags.retries,
                airflow_import_dags.retries_stage1,
                airflow_import_dags.retries_stage2,
                airflow_import_dags.pool_stage1,
                airflow_import_dags.pool_stage2,
                airflow_import_dags.run_import_and_etl_separate,
                airflow_import_dags.finish_all_stage1_first,
                airflow_import_dags.auto_regenerate_dag,
                airflow_import_dags.sudo_user,
                airflow_import_dags.metadata_import,
                airflow_import_dags.timezone,
                airflow_import_dags.email,
                airflow_import_dags.email_on_failure,
                airflow_import_dags.email_on_retries,
                airflow_import_dags.tags,
                airflow_import_dags.sla_warning_time,
                airflow_import_dags.retry_exponential_backoff,
                airflow_import_dags.concurrency,
            )
            .select_from(airflow_import_dags)
            .all()
        ).fillna("")

        etl_dag = pd.DataFrame(
            session.query(
                airflow_etl_dags.dag_name,
                airflow_etl_dags.schedule_interval,
                airflow_etl_dags.filter_job,
                airflow_etl_dags.filter_task,
                airflow_etl_dags.filter_source_db,
                airflow_etl_dags.filter_target_db,
                airflow_etl_dags.retries,
                airflow_etl_dags.auto_regenerate_dag,
                airflow_etl_dags.sudo_user,
                airflow_etl_dags.timezone,
                airflow_etl_dags.email,
                airflow_etl_dags.email_on_failure,
                airflow_etl_dags.email_on_retries,
                airflow_etl_dags.tags,
                airflow_etl_dags.sla_warning_time,
                airflow_etl_dags.retry_exponential_backoff,
                airflow_etl_dags.concurrency,
            )
            .select_from(airflow_etl_dags)
            .all()
        ).fillna("")

        custom_dag = pd.DataFrame(
            session.query(
                airflow_custom_dags.dag_name,
                airflow_custom_dags.schedule_interval,
                airflow_custom_dags.retries,
                airflow_custom_dags.auto_regenerate_dag,
                airflow_custom_dags.sudo_user,
                airflow_custom_dags.timezone,
                airflow_custom_dags.email,
                airflow_custom_dags.email_on_failure,
                airflow_custom_dags.email_on_retries,
                airflow_custom_dags.tags,
                airflow_custom_dags.sla_warning_time,
                airflow_custom_dags.retry_exponential_backoff,
                airflow_custom_dags.concurrency,
            )
            .select_from(airflow_custom_dags)
            .all()
        ).fillna("")

        if name != None:
            if import_dag.empty == False:
                import_dag = import_dag.loc[import_dag["dag_name"] == name]
            if export_dag.empty == False:
                export_dag = export_dag.loc[export_dag["dag_name"] == name]
            if custom_dag.empty == False:
                custom_dag = custom_dag.loc[custom_dag["dag_name"] == name]

        if autoDAGonly == True:
            if import_dag.empty == False:
                import_dag = import_dag.loc[import_dag["auto_regenerate_dag"] == 1]
            if export_dag.empty == False:
                export_dag = export_dag.loc[export_dag["auto_regenerate_dag"] == 1]
            if custom_dag.empty == False:
                custom_dag = custom_dag.loc[custom_dag["auto_regenerate_dag"] == 1]

        dag_found = False

        if name == None or len(import_dag) > 0:
            dag_found = True
            for index, row in import_dag.iterrows():
                self.generateImportDAG(DAG=row)

        if name == None or len(export_dag) > 0:
            dag_found = True
            for index, row in export_dag.iterrows():
                self.generateExportDAG(DAG=row)

        if name == None or len(custom_dag) > 0:
            dag_found = True
            for index, row in custom_dag.iterrows():
                self.generateCustomDAG(DAG=row)

        if dag_found == False and name != None:
            raise invalidConfiguration("Can't find DAG with that name")
        # 			log.error("Can't find DAG with that name")
        # 			self.common_config.remove_temporary_files()
        # 			sys.exit(1)

        session.close()

    def getAirflowHostPoolName(self):
        log = logging.getLogger(self.logger)

        if self.airflowMode == "aws_mwaa" and self.airflowAWSpoolToInstanceId == True:
            # We can choice to override the poolname and set it to the instance ID if we are running on AWS
            pool_name = self.airflowAWSinstanceIds
        else:
            try:
                hostname = (
                    self.common_config.jdbc_hostname.lower()
                    .split("/")[0]
                    .split("\\")[0]
                )
                pool_name = "DBImport_server_%s" % (hostname)
            except (TypeError, AttributeError):
                log.warning(
                    "Cant find hostname in jdbc_connections table. Setting pool to 'default'"
                )
                pool_name = "default"

        return pool_name[0:50]

    def AWSappendInstanceIDasPools(self):
        # This will split the instanceID configration setting based on , and append them all as pool names
        log = logging.getLogger(self.logger)

        used_pools = []

        for aws_pool in self.airflowAWSinstanceIds.split(","):
            used_pools.append(aws_pool.strip())

        self.createAirflowPools(pools=used_pools)

    def getAWSInstanceID(self, configObject):
        # Return one instanceID from a comma seperated list.
        log = logging.getLogger(self.logger)

        if configObject in self.AWSinstanceIdAssignedPools:
            return self.AWSinstanceIdAssignedPools[configObject]
        else:
            random_instance_id = random.choice(
                self.airflowAWSinstanceIds.strip().split(",")
            )
            self.AWSinstanceIdAssignedPools[configObject] = random_instance_id
            return random_instance_id

        # return random.choice(self.airflowAWSinstanceIds.strip().split(','))

    def generateExportDAG(self, DAG):
        """Generates a Import DAG"""
        log = logging.getLogger(self.logger)

        used_pools = []
        table_filters = []
        default_pool = DAG["dag_name"]
        sudo_user = DAG["sudo_user"]
        used_pools.append(default_pool)

        cron_schedule = self.convertTimeToCron(DAG["schedule_interval"])
        self.createDAGfileWithHeader(
            dagName=DAG["dag_name"],
            cronSchedule=cron_schedule,
            defaultPool=default_pool,
            sudoUser=sudo_user,
            dagTimeZone=DAG["timezone"],
            email=DAG["email"],
            email_on_failure=DAG["email_on_failure"],
            email_on_retries=DAG["email_on_retries"],
            tags=DAG["tags"],
            slaWarningTime=DAG["sla_warning_time"],
            retryExponentialBackoff=DAG["retry_exponential_backoff"],
            concurrency=DAG["concurrency"],
        )

        # session = self.configDBSession()
        session = self.getDBImportSession()
        export_tables = aliased(configSchema.exportTables)

        export_tables_query = Query(
            [
                export_tables.target_schema,
                export_tables.target_table,
                export_tables.dbalias,
                export_tables.airflow_priority,
                export_tables.export_type,
                export_tables.sqoop_last_mappers,
            ]
        )
        export_tables_query = export_tables_query.filter(
            export_tables.include_in_airflow == 1
        )

        filter_db_alias = DAG["filter_dbalias"].strip().replace(r"*", "%")
        filter_target_schema = DAG["filter_target_schema"].strip().replace(r"*", "%")
        filter_target_table = DAG["filter_target_table"].strip().replace(r"*", "%")

        # 		if filterDBAlias == '':
        # 			log.error("'filter_dbalias' in airflow_export_dags cant be empty")
        # 			self.DAGfile.close()
        # 			self.common_config.remove_temporary_files()
        # 			sys.exit(1)

        if filter_db_alias != "":
            export_tables_query = export_tables_query.filter(
                export_tables.dbalias.like(filter_db_alias)
            )
        if filter_target_schema != "":
            export_tables_query = export_tables_query.filter(
                export_tables.target_schema.like(filter_target_schema)
            )
        if filter_target_table != "":
            export_tables_query = export_tables_query.filter(
                export_tables.target_table.like(filter_target_table)
            )
        tables = pd.DataFrame(export_tables_query.with_session(session).all()).fillna(
            ""
        )

        if DAG["retries"] == None or DAG["retries"] == "":
            retries = 5
        else:
            retries = int(DAG["retries"])

        # in 'tables' we now have all the tables that will be part of the DAG
        previous_connection_alias = ""

        for index, row in tables.iterrows():
            if row["dbalias"] != previous_connection_alias:
                # We save the previousConnectionAlias just to avoid making lookups for every dbalias even if they are all the same
                try:
                    self.common_config.lookupConnectionAlias(
                        connection_alias=row["dbalias"], decryptCredentials=False
                    )
                    previous_connection_alias = row["dbalias"]

                except invalidConfiguration as errMsg:
                    log.warning(
                        "The connection alias '%s' cant be found in the configuration database"
                        % (row["dbalias"])
                    )
                    previous_connection_alias = None
                    continue

            export_pool = self.getAirflowHostPoolName()

            # usedPools is later used to check if the pools that we just are available in Airflow
            if export_pool not in used_pools:
                used_pools.append(export_pool)

            task_id = row["target_table"].replace(r"/", "_").replace(r".", "_")
            dbexport_cmd = "%sbin/export" % (
                self.getDBImportCommandPath(sudoUser=sudo_user)
            )
            dbexport_clear_stage_cmd = "%sbin/manage --clearExportStage" % (
                self.getDBImportCommandPath(sudoUser=sudo_user)
            )

            airflow_priority = 1  # Default Airflow Priority
            if row["airflow_priority"] != None and row["airflow_priority"] != "":
                airflow_priority = int(row["airflow_priority"])
            elif row["sqoop_last_mappers"] != None and row["sqoop_last_mappers"] != "":
                airflow_priority = int(row["sqoop_last_mappers"])

            clear_stage_required = False
            if row["export_type"] == "full":
                clear_stage_required = True

            if clear_stage_required == True:
                self.DAGfile.write("%s_clearStage = BashOperator(\n" % (task_id))
                self.DAGfile.write("    task_id='%s_clearStage',\n" % (task_id))
                self.DAGfile.write(
                    "    bash_command='%s -a %s -S %s -T %s ',\n"
                    % (
                        dbexport_clear_stage_cmd,
                        row["dbalias"],
                        row["target_schema"],
                        row["target_table"],
                    )
                )
                self.DAGfile.write("    priority_weight=%s,\n" % (airflow_priority))
                self.DAGfile.write("    weight_rule='absolute',\n")
                self.DAGfile.write("    retries=%s,\n" % (retries))
                self.DAGfile.write("    dag=dag)\n")
                self.DAGfile.write("\n")

            self.DAGfile.write("%s = BashOperator(\n" % (task_id))
            self.DAGfile.write("    task_id='%s',\n" % (task_id))
            self.DAGfile.write(
                "    bash_command='%s -a %s -S %s -T %s ',\n"
                % (
                    dbexport_cmd,
                    row["dbalias"],
                    row["target_schema"],
                    row["target_table"],
                )
            )
            self.DAGfile.write("    pool='%s',\n" % (export_pool))
            self.DAGfile.write("    priority_weight=%s,\n" % (airflow_priority))
            self.DAGfile.write("    weight_rule='absolute',\n")
            self.DAGfile.write("    retries=%s,\n" % (retries))
            self.DAGfile.write("    dag=dag)\n")
            self.DAGfile.write("\n")

            if clear_stage_required == True:
                self.DAGfile.write(
                    "%s.set_downstream(%s_clearStage)\n" % (self.mainStartTask, task_id)
                )
                self.DAGfile.write(
                    "%s_clearStage.set_downstream(%s)\n" % (task_id, task_id)
                )
                self.DAGfile.write(
                    "%s.set_downstream(%s)\n" % (task_id, self.mainStopTask)
                )
            else:
                self.DAGfile.write(
                    "%s.set_downstream(%s)\n" % (self.mainStartTask, task_id)
                )
                self.DAGfile.write(
                    "%s.set_downstream(%s)\n" % (task_id, self.mainStopTask)
                )
            self.DAGfile.write("\n")

        self.addTasksToDAGfile(
            dagName=DAG["dag_name"],
            mainDagSchedule=DAG["schedule_interval"],
            defaultRetries=retries,
            defaultSudoUser=sudo_user,
        )
        self.addSensorsToDAGfile(
            dagName=DAG["dag_name"], mainDagSchedule=DAG["schedule_interval"]
        )
        self.createAirflowPools(pools=used_pools)
        self.closeDAGfile()
        session.close()

    def generateImportDAG(self, DAG):
        """Generates a Import DAG"""
        log = logging.getLogger(self.logger)

        import_phase_finish_first = False
        if DAG["finish_all_stage1_first"] == 1:
            import_phase_finish_first = True

        run_import_and_etl_separate = False
        if DAG["run_import_and_etl_separate"] == 1:
            run_import_and_etl_separate = True

        used_pools = []
        table_filters = []
        default_pool = DAG["dag_name"]
        sudo_user = DAG["sudo_user"]
        used_pools.append(default_pool)

        if DAG["metadata_import"] == 1:
            meta_data_import_option = "-m"
        else:
            meta_data_import_option = ""

        cron_schedule = self.convertTimeToCron(DAG["schedule_interval"])
        self.createDAGfileWithHeader(
            dagName=DAG["dag_name"],
            cronSchedule=cron_schedule,
            importPhaseFinishFirst=import_phase_finish_first,
            defaultPool=default_pool,
            sudoUser=sudo_user,
            dagTimeZone=DAG["timezone"],
            email=DAG["email"],
            email_on_failure=DAG["email_on_failure"],
            email_on_retries=DAG["email_on_retries"],
            tags=DAG["tags"],
            slaWarningTime=DAG["sla_warning_time"],
            retryExponentialBackoff=DAG["retry_exponential_backoff"],
            concurrency=DAG["concurrency"],
        )

        # session = self.configDBSession()
        session = self.getDBImportSession()
        import_tables = aliased(configSchema.importTables)

        import_tables_query = Query(
            [
                import_tables.hive_db,
                import_tables.hive_table,
                import_tables.dbalias,
                import_tables.airflow_priority,
                import_tables.import_type,
                import_tables.import_phase_type,
                import_tables.etl_phase_type,
                import_tables.sqoop_last_mappers,
                import_tables.copy_slave,
            ]
        )
        import_tables_query = import_tables_query.filter(
            import_tables.include_in_airflow == 1
        )

        for hive_target in DAG["filter_hive"].split(";"):
            try:
                hive_db = hive_target.split(".")[0].strip().replace(r"*", "%")
                hive_table = hive_target.split(".")[1].strip().replace(r"*", "%")
                if (
                    hive_db == None
                    or hive_table == None
                    or hive_db == ""
                    or hive_table == ""
                ):
                    self.DAGfile.close()
                    raise invalidConfiguration(
                        "Syntax for filter_hive column is <HIVE_DB>.<HIVE_TABLE>;<HIVE_DB>.<HIVE_TABLE>;....."
                    )
            except IndexError:
                self.DAGfile.close()
                raise invalidConfiguration(
                    "Syntax for filter_hive column is <HIVE_DB>.<HIVE_TABLE>;<HIVE_DB>.<HIVE_TABLE>;....."
                )
            # 				log.error("Syntax for filter_hive column is <HIVE_DB>.<HIVE_TABLE>;<HIVE_DB>.<HIVE_TABLE>;.....")
            # 				self.DAGfile.close()
            # 				self.common_config.remove_temporary_files()
            # 				sys.exit(1)

            table_filters.append(
                (import_tables.hive_db.like(hive_db))
                & (import_tables.hive_table.like(hive_table))
            )

        import_tables_query = import_tables_query.filter(sa.or_(*table_filters))
        tables = pd.DataFrame(import_tables_query.with_session(session).all()).fillna(
            ""
        )

        retries = int(DAG["retries"])
        try:
            retries_import_phase = int(DAG["retries_stage1"])
        except ValueError:
            retries_import_phase = retries

        try:
            retries_etl_phase = int(DAG["retries_stage2"])
        except ValueError:
            retries_etl_phase = retries

        # in 'tables' we now have all the tables that will be part of the DAG
        previous_connection_alias = ""

        for index, row in tables.iterrows():
            try:
                if row["dbalias"] != previous_connection_alias:
                    # We save the previousConnectionAlias just to avoid making lookups for every dbalias even if they are all the same
                    self.common_config.lookupConnectionAlias(
                        connection_alias=row["dbalias"], decryptCredentials=False
                    )
                    previous_connection_alias = row["dbalias"]
            except:
                continue

            if row["copy_slave"] == 1:
                import_phase_as_sensor = True
            else:
                import_phase_as_sensor = False

            if (
                self.airflowMode == "aws_mwaa"
                and self.airflowAWSpoolToInstanceId == True
            ):
                # We can choice to override the poolname and set it to the instance ID if we are running on AWS
                instance_id = self.getAWSInstanceID(row["hive_table"])
                import_phase_pool = instance_id
                etl_phase_pool = instance_id
            else:
                import_phase_pool = self.getAirflowHostPoolName()
                etl_phase_pool = DAG["dag_name"][0:50]

                if DAG["pool_stage1"] != "":
                    import_phase_pool = DAG["pool_stage1"]

                if DAG["pool_stage2"] != "":
                    etl_phase_pool = DAG["pool_stage2"]

            # usedPools is later used to check if the pools that we just are available in Airflow
            if import_phase_pool not in used_pools:
                used_pools.append(import_phase_pool)

            if etl_phase_pool not in used_pools:
                used_pools.append(etl_phase_pool)

            dbimport_cmd = "%sbin/import" % (
                self.getDBImportCommandPath(sudoUser=sudo_user)
            )
            dbimport_clear_stage_cmd = "%sbin/manage --clearImportStage" % (
                self.getDBImportCommandPath(sudoUser=sudo_user)
            )

            task_id = row["hive_table"].replace(r"/", "_").replace(r".", "_")

            airflow_priority = 1  # Default Airflow Priority
            if row["airflow_priority"] != None and row["airflow_priority"] != "":
                airflow_priority = int(row["airflow_priority"])
            elif row["sqoop_last_mappers"] != None and row["sqoop_last_mappers"] != "":
                airflow_priority = int(row["sqoop_last_mappers"])

            clear_stage_required = False
            if row["import_type"] in (
                "full_direct",
                "full",
                "oracle_flashback_merge",
                "full_history",
                "full_merge_direct_history",
                "full_merge_direct",
                "full_append",
            ):
                clear_stage_required = True

            if row["import_phase_type"] in (
                "full",
                "oracle_flashback",
                "mssql_change_tracking",
            ):
                clear_stage_required = True

            if clear_stage_required == True:
                if self.airflowMode == "default":
                    self.DAGfile.write("%s_clearStage = BashOperator(\n" % (task_id))
                    self.DAGfile.write("    task_id='%s_clearStage',\n" % (task_id))
                    self.DAGfile.write(
                        "    bash_command='%s -h %s -t %s ',\n"
                        % (dbimport_clear_stage_cmd, row["hive_db"], row["hive_table"])
                    )
                    self.DAGfile.write("    pool='%s',\n" % (import_phase_pool))
                    self.DAGfile.write("    priority_weight=%s,\n" % (airflow_priority))
                    self.DAGfile.write("    weight_rule='absolute',\n")
                    self.DAGfile.write("    retries=%s,\n" % (retries))
                    self.DAGfile.write("    dag=dag)\n")
                    self.DAGfile.write("\n")
                elif self.airflowMode == "aws_mwaa":
                    instance_id = self.getAWSInstanceID(row["hive_table"])
                    self.DAGfile.write("%s_clearStage = PythonOperator(\n" % (task_id))
                    self.DAGfile.write("    task_id='%s_clearStage',\n" % (task_id))
                    self.DAGfile.write("    python_callable=startDBImportExecution,\n")
                    # self.DAGfile.write("    op_kwargs={\"command\": \"manage --clearImportStage -h %s -t %s\"},\n"%(row['hive_db'], row['hive_table']))
                    self.DAGfile.write(
                        '    op_kwargs={"command": "manage --clearImportStage -h %s -t %s", "instanceID": "%s"},\n'
                        % (row["hive_db"], row["hive_table"], instance_id)
                    )
                    self.DAGfile.write("    pool='%s',\n" % (import_phase_pool))
                    # self.DAGfile.write("    pool='%s',\n"%(instanceID))
                    self.DAGfile.write("    priority_weight=%s,\n" % (airflow_priority))
                    self.DAGfile.write("    weight_rule='absolute',\n")
                    self.DAGfile.write("    retries=%s,\n" % (retries))
                    self.DAGfile.write("    dag=dag)\n")
                    self.DAGfile.write("\n")

            if (
                DAG["finish_all_stage1_first"] == 1
                or run_import_and_etl_separate == True
            ):
                if import_phase_as_sensor == True:
                    # Running Import phase as a sensor
                    self.DAGfile.write("%s_sensor = SqlSensor(\n" % (task_id))
                    self.DAGfile.write("    task_id='%s_sensor',\n" % (task_id))
                    self.DAGfile.write("    conn_id='DBImport',\n")
                    self.DAGfile.write(
                        "    sql=\"\"\"select count(*) from import_tables where hive_db = '%s' and hive_table = '%s' and "
                        % (row["hive_db"], row["hive_table"])
                    )
                    self.DAGfile.write(
                        "copy_finished >= '{{ next_execution_date.strftime('%Y-%m-%d %H:%M:%S.%f') }}'\"\"\",\n"
                    )
                    self.DAGfile.write("    pool='%s',\n" % (import_phase_pool))
                    self.DAGfile.write("    priority_weight=%s,\n" % (airflow_priority))
                    self.DAGfile.write("    weight_rule='absolute',\n")
                    self.DAGfile.write("    timeout=18000,\n")
                    self.DAGfile.write("    poke_interval=300,\n")
                    self.DAGfile.write("    mode='reschedule',\n")
                    self.DAGfile.write("    dag=dag)\n")
                    self.DAGfile.write("\n")

                if self.airflowMode == "default":
                    self.DAGfile.write("%s_import = BashOperator(\n" % (task_id))
                    self.DAGfile.write("    task_id='%s_import',\n" % (task_id))
                    self.DAGfile.write(
                        "    bash_command='%s -h %s -t %s -I -C ',\n"
                        % (dbimport_cmd, row["hive_db"], row["hive_table"])
                    )
                    self.DAGfile.write("    pool='%s',\n" % (import_phase_pool))
                    self.DAGfile.write("    priority_weight=%s,\n" % (airflow_priority))
                    self.DAGfile.write("    weight_rule='absolute',\n")
                    self.DAGfile.write("    retries=%s,\n" % (retries_import_phase))
                    self.DAGfile.write("    dag=dag)\n")
                    self.DAGfile.write("\n")

                    self.DAGfile.write("%s_etl = BashOperator(\n" % (task_id))
                    self.DAGfile.write("    task_id='%s_etl',\n" % (task_id))
                    self.DAGfile.write(
                        "    bash_command='%s -h %s -t %s -E ',\n"
                        % (dbimport_cmd, row["hive_db"], row["hive_table"])
                    )
                    self.DAGfile.write("    pool='%s',\n" % (etl_phase_pool))
                    self.DAGfile.write("    priority_weight=%s,\n" % (airflow_priority))
                    self.DAGfile.write("    weight_rule='absolute',\n")
                    self.DAGfile.write("    retries=%s,\n" % (retries_etl_phase))
                    self.DAGfile.write("    dag=dag)\n")
                    self.DAGfile.write("\n")

                elif self.airflowMode == "aws_mwaa":
                    instance_id = self.getAWSInstanceID(row["hive_table"])
                    self.DAGfile.write("%s_import = PythonOperator(\n" % (task_id))
                    self.DAGfile.write("    task_id='%s_import',\n" % (task_id))
                    self.DAGfile.write("    python_callable=startDBImportExecution,\n")
                    self.DAGfile.write(
                        '    op_kwargs={"command": "import -d %s -t %s -I -C", "instanceID": "%s"},\n'
                        % (row["hive_db"], row["hive_table"], instance_id)
                    )
                    self.DAGfile.write("    pool='%s',\n" % (import_phase_pool))
                    self.DAGfile.write("    priority_weight=%s,\n" % (airflow_priority))
                    self.DAGfile.write("    weight_rule='absolute',\n")
                    self.DAGfile.write("    retries=%s,\n" % (retries_import_phase))
                    self.DAGfile.write("    dag=dag)\n")
                    self.DAGfile.write("\n")

                    self.DAGfile.write("%s_etl = PythonOperator(\n" % (task_id))
                    self.DAGfile.write("    task_id='%s_etl',\n" % (task_id))
                    self.DAGfile.write("    python_callable=startDBImportExecution,\n")
                    self.DAGfile.write(
                        '    op_kwargs={"command": "import -d %s -t %s -E", "instanceID": "%s"},\n'
                        % (row["hive_db"], row["hive_table"], instance_id)
                    )
                    self.DAGfile.write("    pool='%s',\n" % (etl_phase_pool))
                    self.DAGfile.write("    priority_weight=%s,\n" % (airflow_priority))
                    self.DAGfile.write("    weight_rule='absolute',\n")
                    self.DAGfile.write("    retries=%s,\n" % (retries_etl_phase))
                    self.DAGfile.write("    dag=dag)\n")
                    self.DAGfile.write("\n")

                if clear_stage_required == True and DAG["finish_all_stage1_first"] == 1:
                    self.DAGfile.write(
                        "%s.set_downstream(%s_clearStage)\n"
                        % (self.mainStartTask, task_id)
                    )
                    if import_phase_as_sensor == True:
                        self.DAGfile.write(
                            "%s_clearStage.set_downstream(%s_sensor)\n"
                            % (task_id, task_id)
                        )
                        self.DAGfile.write(
                            "%s_sensor.set_downstream(%s_import)\n" % (task_id, task_id)
                        )
                    else:
                        self.DAGfile.write(
                            "%s_clearStage.set_downstream(%s_import)\n"
                            % (task_id, task_id)
                        )
                    self.DAGfile.write(
                        "%s_import.set_downstream(Import_Phase_Finished)\n" % (task_id)
                    )
                    self.DAGfile.write(
                        "Import_Phase_Finished.set_downstream(%s_etl)\n" % (task_id)
                    )
                    self.DAGfile.write(
                        "%s_etl.set_downstream(%s)\n" % (task_id, self.mainStopTask)
                    )
                elif (
                    clear_stage_required == True and DAG["finish_all_stage1_first"] == 0
                ):  # This means that runImportAndEtlSeparate == True
                    self.DAGfile.write(
                        "%s.set_downstream(%s_clearStage)\n"
                        % (self.mainStartTask, task_id)
                    )
                    if import_phase_as_sensor == True:
                        self.DAGfile.write(
                            "%s_clearStage.set_downstream(%s_sensor)\n"
                            % (task_id, task_id)
                        )
                        self.DAGfile.write(
                            "%s_sensor.set_downstream(%s_import)\n" % (task_id, task_id)
                        )
                    else:
                        self.DAGfile.write(
                            "%s_clearStage.set_downstream(%s_import)\n"
                            % (task_id, task_id)
                        )
                    self.DAGfile.write(
                        "%s_import.set_downstream(%s_etl)\n" % (task_id, task_id)
                    )
                    self.DAGfile.write(
                        "%s_etl.set_downstream(%s)\n" % (task_id, self.mainStopTask)
                    )
                elif (
                    clear_stage_required == False
                    and DAG["finish_all_stage1_first"] == 1
                ):
                    if import_phase_as_sensor == True:
                        self.DAGfile.write(
                            "%s.set_downstream(%s_sensor)\n"
                            % (self.mainStartTask, task_id)
                        )
                        self.DAGfile.write(
                            "%s_sensor.set_downstream(%s_import)\n" % (task_id, task_id)
                        )
                    else:
                        self.DAGfile.write(
                            "%s.set_downstream(%s_import)\n"
                            % (self.mainStartTask, task_id)
                        )
                    self.DAGfile.write(
                        "%s_import.set_downstream(Import_Phase_Finished)\n" % (task_id)
                    )
                    self.DAGfile.write(
                        "Import_Phase_Finished.set_downstream(%s_etl)\n" % (task_id)
                    )
                    self.DAGfile.write(
                        "%s_etl.set_downstream(%s)\n" % (task_id, self.mainStopTask)
                    )
                else:
                    if import_phase_as_sensor == True:
                        self.DAGfile.write(
                            "%s.set_downstream(%s_sensor)\n"
                            % (self.mainStartTask, task_id)
                        )
                        self.DAGfile.write(
                            "%s_sensor.set_downstream(%s_import)\n" % (task_id, task_id)
                        )
                    else:
                        self.DAGfile.write(
                            "%s.set_downstream(%s_import)\n"
                            % (self.mainStartTask, task_id)
                        )
                    self.DAGfile.write(
                        "%s_import.set_downstream(%s_etl)\n" % (task_id, task_id)
                    )
                    self.DAGfile.write(
                        "%s_etl.set_downstream(%s)\n" % (task_id, self.mainStopTask)
                    )
                self.DAGfile.write("\n")

            else:
                if import_phase_as_sensor == True:
                    # Running Import phase as a sensor
                    self.DAGfile.write("%s_sensor = SqlSensor(\n" % (task_id))
                    self.DAGfile.write("    task_id='%s_sensor',\n" % (task_id))
                    self.DAGfile.write("    conn_id='DBImport',\n")
                    self.DAGfile.write(
                        "    sql=\"\"\"select count(*) from import_tables where hive_db = '%s' and hive_table = '%s' and "
                        % (row["hive_db"], row["hive_table"])
                    )
                    self.DAGfile.write(
                        "copy_finished >= '{{ next_execution_date.strftime('%Y-%m-%d %H:%M:%S.%f') }}'\"\"\",\n"
                    )
                    self.DAGfile.write("    pool='%s',\n" % (import_phase_pool))
                    self.DAGfile.write("    priority_weight=%s,\n" % (airflow_priority))
                    self.DAGfile.write("    weight_rule='absolute',\n")
                    self.DAGfile.write("    timeout=18000,\n")
                    self.DAGfile.write("    poke_interval=300,\n")
                    self.DAGfile.write("    mode='reschedule',\n")
                    self.DAGfile.write("    dag=dag)\n")
                    self.DAGfile.write("\n")

                if self.airflowMode == "default":
                    self.DAGfile.write("%s = BashOperator(\n" % (task_id))
                    self.DAGfile.write("    task_id='%s',\n" % (task_id))
                    self.DAGfile.write(
                        "    bash_command='%s -h %s -t %s %s ',\n"
                        % (
                            dbimport_cmd,
                            row["hive_db"],
                            row["hive_table"],
                            meta_data_import_option,
                        )
                    )
                    self.DAGfile.write("    pool='%s',\n" % (etl_phase_pool))
                    self.DAGfile.write("    priority_weight=%s,\n" % (airflow_priority))
                    self.DAGfile.write("    weight_rule='absolute',\n")
                    self.DAGfile.write("    retries=%s,\n" % (retries))
                    self.DAGfile.write("    dag=dag)\n")
                    self.DAGfile.write("\n")
                elif self.airflowMode == "aws_mwaa":
                    instance_id = self.getAWSInstanceID(row["hive_table"])
                    self.DAGfile.write("%s = PythonOperator(\n" % (task_id))
                    self.DAGfile.write("    task_id='%s',\n" % (task_id))
                    self.DAGfile.write("    python_callable=startDBImportExecution,\n")
                    self.DAGfile.write(
                        '    op_kwargs={"command": "import -d %s -t %s %s", "instanceID": "%s"},\n'
                        % (
                            row["hive_db"],
                            row["hive_table"],
                            meta_data_import_option,
                            instance_id,
                        )
                    )
                    self.DAGfile.write("    pool='%s',\n" % (etl_phase_pool))
                    self.DAGfile.write("    priority_weight=%s,\n" % (airflow_priority))
                    self.DAGfile.write("    weight_rule='absolute',\n")
                    self.DAGfile.write("    retries=%s,\n" % (retries))
                    self.DAGfile.write("    dag=dag)\n")
                    self.DAGfile.write("\n")

                if clear_stage_required == True:
                    if import_phase_as_sensor == True:
                        self.DAGfile.write(
                            "%s.set_downstream(%s_clearStage)\n"
                            % (self.mainStartTask, task_id)
                        )
                        self.DAGfile.write(
                            "%s_clearStage.set_downstream(%s_sensor)\n"
                            % (task_id, task_id)
                        )
                        self.DAGfile.write(
                            "%s_sensor.set_downstream(%s)\n" % (task_id, task_id)
                        )
                        self.DAGfile.write(
                            "%s.set_downstream(%s)\n" % (task_id, self.mainStopTask)
                        )
                    else:
                        self.DAGfile.write(
                            "%s.set_downstream(%s_clearStage)\n"
                            % (self.mainStartTask, task_id)
                        )
                        self.DAGfile.write(
                            "%s_clearStage.set_downstream(%s)\n" % (task_id, task_id)
                        )
                        self.DAGfile.write(
                            "%s.set_downstream(%s)\n" % (task_id, self.mainStopTask)
                        )
                else:
                    if import_phase_as_sensor == True:
                        self.DAGfile.write(
                            "%s.set_downstream(%s_sensor)\n"
                            % (self.mainStartTask, task_id)
                        )
                        self.DAGfile.write(
                            "%s_sensor.set_downstream(%s)\n" % (task_id, task_id)
                        )
                        self.DAGfile.write(
                            "%s.set_downstream(%s)\n" % (task_id, self.mainStopTask)
                        )
                    else:
                        self.DAGfile.write(
                            "%s.set_downstream(%s)\n" % (self.mainStartTask, task_id)
                        )
                        self.DAGfile.write(
                            "%s.set_downstream(%s)\n" % (task_id, self.mainStopTask)
                        )
                self.DAGfile.write("\n")

        self.addTasksToDAGfile(
            dagName=DAG["dag_name"],
            mainDagSchedule=DAG["schedule_interval"],
            defaultRetries=retries,
            defaultSudoUser=sudo_user,
        )
        self.addSensorsToDAGfile(
            dagName=DAG["dag_name"], mainDagSchedule=DAG["schedule_interval"]
        )
        self.createAirflowPools(pools=used_pools)
        if self.airflowMode == "aws_mwaa" and self.airflowAWSpoolToInstanceId == True:
            self.AWSappendInstanceIDasPools()
        self.closeDAGfile()
        session.close()

    def createAirflowPools(self, pools):
        """Creates the pools in Airflow database"""
        log = logging.getLogger(self.logger)

        if self.airflowMode == "default" and self.createPoolsWithTasks == False:
            session = self.airflowDBSession()
            slot_pool = aliased(airflowSchema.slotPool)

            airflow_pools = pd.DataFrame(
                session.query(slot_pool.pool, slot_pool.slots, slot_pool.description)
                .select_from(slot_pool)
                .all()
            )

            for pool in pools:
                # Just to make sure we dont create the same pool twice. That will generate an error in Airflow
                if pool in self.createdPools:
                    continue
                else:
                    self.createdPools.append(pool)

                if (
                    len(airflow_pools) == 0
                    or len(airflow_pools.loc[airflow_pools["pool"] == pool]) == 0
                ):
                    try:
                        log.info(
                            "Creating the Airflow pool '%s' with %s slots"
                            % (pool, self.airflowDefaultPoolSize)
                        )
                        new_pool = airflowSchema.slotPool(
                            pool=pool,
                            slots=self.airflowDefaultPoolSize,
                            include_deferred=0,
                        )
                        session.add(new_pool)
                        session.commit()
                    except sa.exc.IntegrityError:
                        log.warning(
                            "Cant create pool '%s' as there is a duplicate pool already in the Airflow database."
                            % (pool)
                        )
                        session.rollback()

            session.close()

        elif self.createPoolsWithTasks == True:
            for pool in pools:
                # Just to make sure we dont create the same pool twice. That will generate an error in Airflow
                if pool in self.createdPools:
                    continue
                else:
                    self.createdPools.append(pool)

                task_name = "createPool_%s" % (pool.replace(".", "_").replace("-", "_"))
                self.DAGfile.write("\n")
                self.DAGfile.write("%s = CreatePoolOperator(\n" % (task_name))
                self.DAGfile.write("    name='%s',\n" % (pool))
                self.DAGfile.write("    slots=%s,\n" % (self.airflowDefaultPoolSize))
                self.DAGfile.write("    task_id='%s',\n" % (task_name))
                self.DAGfile.write("    retries=5,\n")
                self.DAGfile.write("    priority_weight=1,\n")
                self.DAGfile.write("    weight_rule='absolute',\n")
                self.DAGfile.write("    pool=None,\n")
                self.DAGfile.write("    dag=dag)\n")
                self.DAGfile.write("\n")
                self.DAGfile.write("start.set_downstream(%s)\n" % (task_name))
                self.DAGfile.write(
                    "%s.set_downstream(%s)\n" % (task_name, self.preStopTask)
                )
                self.DAGfile.write("\n")

    def generateCustomDAG(self, DAG):
        """Generates a Custom DAG"""
        log = logging.getLogger(self.logger)

        # session = self.configDBSession()
        session = self.getDBImportSession()
        airflow_tasks = aliased(configSchema.airflowTasks)

        tasks = (
            session.query(airflow_tasks.task_name)
            .select_from(airflow_tasks)
            .filter(airflow_tasks.dag_name == DAG["dag_name"])
            .filter(airflow_tasks.include_in_airflow == 1)
            .filter(airflow_tasks.placement == "in main")
            .count()
        )

        if tasks == 0:
            log.warning(
                "There are no tasks defined 'in main' for DAG '%s'. This is required for a custom DAG"
                % (DAG["dag_name"])
            )
            session.close()
            return

        used_pools = []
        # 		if self.airflowMode == "aws_mwaa" and self.airflowAWSpoolToInstanceId == True:
        # 			# We can choice to override the poolname and set it to the instance ID if we are running on AWS
        # 			defaultPool = self.airflowAWSinstanceIds
        # 		else:
        # 			defaultPool = DAG['dag_name']
        default_pool = DAG["dag_name"]
        sudo_user = DAG["sudo_user"]
        used_pools.append(default_pool)

        if DAG["retries"] == None or DAG["retries"] == "":
            retries = 0
        else:
            retries = int(DAG["retries"])

        cron_schedule = self.convertTimeToCron(DAG["schedule_interval"])
        self.createDAGfileWithHeader(
            dagName=DAG["dag_name"],
            cronSchedule=cron_schedule,
            defaultPool=default_pool,
            sudoUser=sudo_user,
            dagTimeZone=DAG["timezone"],
            email=DAG["email"],
            email_on_failure=DAG["email_on_failure"],
            email_on_retries=DAG["email_on_retries"],
            tags=DAG["tags"],
            slaWarningTime=DAG["sla_warning_time"],
            retryExponentialBackoff=DAG["retry_exponential_backoff"],
            concurrency=DAG["concurrency"],
        )
        self.addTasksToDAGfile(
            dagName=DAG["dag_name"],
            mainDagSchedule=DAG["schedule_interval"],
            defaultRetries=retries,
            defaultSudoUser=sudo_user,
        )
        self.addSensorsToDAGfile(
            dagName=DAG["dag_name"], mainDagSchedule=DAG["schedule_interval"]
        )
        self.createAirflowPools(pools=used_pools)
        self.closeDAGfile()
        session.close()

    def convertTimeToCron(self, time):
        """Converts a string in format HH:MM to a CRON time string based on 'minute hour day month weekday'"""
        log = logging.getLogger(self.logger)
        return_value = time

        if re.search("^[0-2][0-9]:[0-5][0-9]$", time):
            hour = re.sub(":[0-5][0-9]$", "", time)
            minute = re.sub("^[0-2][0-9]:", "", time)
            return_value = "'%s %s * * *'" % (int(minute), int(hour))

        return return_value

    def validateTimeZone(self, timeZone):
        log = logging.getLogger(self.logger)
        try:
            pendulum.timezone(timeZone)
        except ValueError:
            return False
        else:
            return True

    def createDAGfileWithHeader(
        self,
        dagName,
        cronSchedule,
        defaultPool,
        importPhaseFinishFirst=False,
        sudoUser="",
        dagTimeZone=None,
        email=None,
        email_on_failure=None,
        email_on_retries=None,
        tags=None,
        slaWarningTime=None,
        retryExponentialBackoff=None,
        concurrency=None,
    ):
        log = logging.getLogger(self.logger)
        # session = self.configDBSession()
        session = self.getDBImportSession()

        self.sensorStartTask = "start"
        self.sensorStopTask = "dag_sensors_finished"
        self.preStartTask = "start"
        self.preStopTask = "before_tasks_finished"
        self.mainStartTask = "start"
        self.mainStopTask = "stop"
        self.postStartTask = "main_tasks_finished"
        self.postStopTask = "stop"

        if (
            self.TaskQueueForDummy != None
            and self.TaskQueueForDummy.strip() != ""
            and self.TaskQueueForDummy.strip() != "default"
        ):
            self.TaskQueueForDummy = self.TaskQueueForDummy.strip()
        else:
            self.TaskQueueForDummy = None

        tasks_before_main_exists = False
        tasks_after_main_exists = False
        tasks_sensors_exists = False

        if (
            dagTimeZone != None
            and dagTimeZone != ""
            and self.validateTimeZone(dagTimeZone) == False
        ):
            log.warning(
                "Time zone value specified for DAG is not valid, (%s)" % (dagTimeZone)
            )
            log.warning("Using default time zone value instead")
            dagTimeZone = None

        if self.DAGFolder == None:
            self.DAGfilename = "%s/%s.py" % (self.DAGstagingDirectory, dagName)
            if not os.path.exists(self.DAGstagingDirectory):
                try:
                    log.info(
                        "Creating DAG staging directory (%s)"
                        % (self.DAGstagingDirectory)
                    )
                    os.makedirs(self.DAGstagingDirectory)
                except:
                    pass
        else:
            self.DAGfilename = "%s/%s.py" % (self.DAGFolder, dagName)

        self.DAGfilenameInAwsMWAA = "dags/%s.py" % (dagName)
        self.DAGfilenameInAirflow = "%s/%s.py" % (self.DAGdirectory, dagName)

        try:
            self.DAGfile = open(self.DAGfilename, "w")
        except FileNotFoundError:
            log.error(
                "Cant open file '%s'. Please verify that the directory exists and that the configuration 'airflow_dbimport_commandpath' is correct"
                % (self.DAGfilename)
            )
            raise RuntimeError
            # self.common_config.remove_temporary_files()
            # sys.exit(1)

        self.DAGfile.write("# -*- coding: utf-8 -*-\n")
        self.DAGfile.write("import airflow\n")
        self.DAGfile.write("from airflow import DAG\n")
        self.DAGfile.write("from airflow.models import Variable\n")

        if self.airflowMajorVersion == 2:
            self.DAGfile.write(
                "from airflow.sensors.external_task import ExternalTaskSensor\n"
            )
        else:
            self.DAGfile.write(
                "from airflow.operators.sensors import ExternalTaskSensor\n"
            )

        self.DAGfile.write("from airflow.sensors.sql_sensor import SqlSensor\n")
        self.DAGfile.write("from airflow.operators.bash_operator import BashOperator\n")
        self.DAGfile.write(
            "from airflow.operators.python_operator import BranchPythonOperator\n"
        )
        self.DAGfile.write(
            "from airflow.operators.dagrun_operator import TriggerDagRunOperator\n"
        )
        self.DAGfile.write(
            "from airflow.operators.dummy_operator import DummyOperator\n"
        )
        self.DAGfile.write(
            "from airflow.api.common.experimental.pool import get_pool, create_pool\n"
        )
        self.DAGfile.write("from airflow.exceptions import PoolNotFound\n")
        self.DAGfile.write("from airflow.models import BaseOperator\n")

        if self.airflowMode == "aws_mwaa":
            self.DAGfile.write(
                "from airflow.operators.dummy_operator import DummyOperator\n"
            )
            self.DAGfile.write(
                "from airflow.operators.python_operator import PythonOperator\n"
            )
            self.DAGfile.write("import botocore\n")
            self.DAGfile.write("import boto3\n")
            self.DAGfile.write("import time\n")

        self.DAGfile.write("from datetime import datetime, timedelta, timezone\n")
        self.DAGfile.write("import pendulum\n")
        self.DAGfile.write("\n")
        self.DAGfile.write("try:\n")
        self.DAGfile.write('    Email_receiver = Variable.get("Email_receiver")\n')
        self.DAGfile.write("except KeyError:\n")
        self.DAGfile.write('    Email_receiver = "not-set@domain.com"\n')
        self.DAGfile.write("\n")
        self.DAGfile.write("\n")

        self.DAGfile.write("class CreatePoolOperator(BaseOperator):\n")
        self.DAGfile.write("    ui_color = '#b8e9ee'\n")
        self.DAGfile.write("\n")
        self.DAGfile.write("    def __init__(\n")
        self.DAGfile.write("            self,\n")
        self.DAGfile.write("            name,\n")
        self.DAGfile.write("            slots,\n")
        self.DAGfile.write("            description='',\n")
        self.DAGfile.write("            *args, **kwargs):\n")
        self.DAGfile.write(
            "        super(CreatePoolOperator, self).__init__(*args, **kwargs)\n"
        )
        self.DAGfile.write("        self.description = description\n")
        self.DAGfile.write("        self.slots = slots\n")
        self.DAGfile.write("        self.name = name\n")
        self.DAGfile.write("\n")
        self.DAGfile.write("    def execute(self, context):\n")
        self.DAGfile.write("        try:\n")
        self.DAGfile.write("            pool = get_pool(name=self.name)\n")
        # self.DAGfile.write("            if pool:\n")
        self.DAGfile.write("            print('Pool exists: %s'%(pool))\n")
        self.DAGfile.write("        except PoolNotFound:\n")
        self.DAGfile.write("            # create the pool\n")
        self.DAGfile.write(
            "            pool = create_pool(name=self.name, slots=self.slots, description=self.description)\n"
        )
        self.DAGfile.write("            print('Pool exists: %s'%(pool))\n")
        self.DAGfile.write("\n")
        self.DAGfile.write("\n")

        if dagTimeZone == None or dagTimeZone == "":
            self.DAGfile.write(
                'local_tz = pendulum.timezone("%s")\n' % (self.defaultTimeZone)
            )
            self.DAGfile.write("\n")
        else:
            self.DAGfile.write('local_tz = pendulum.timezone("%s")\n' % (dagTimeZone))
            self.DAGfile.write("\n")

        if self.airflowMode == "aws_mwaa":
            self.DAGfile.write(
                "def printAndFindEventErrors(event, startWithNewline = False):\n"
            )
            self.DAGfile.write("    errorFound = False\n")
            self.DAGfile.write("\n")
            self.DAGfile.write("    if startWithNewline == True:\n")
            self.DAGfile.write('        findPattern = "\\nERROR"\n')
            self.DAGfile.write("    else:\n")
            self.DAGfile.write('        findPattern = "ERROR"\n')
            self.DAGfile.write("\n")
            self.DAGfile.write("    try:\n")
            self.DAGfile.write("        print(event['message'], end=\"\")\n")
            self.DAGfile.write("        if event['message'].find(findPattern) != -1:\n")
            # self.DAGfile.write("        if event['message'].find(\"ERROR\") != -1:\n")
            self.DAGfile.write("            errorFound = True\n")
            # self.DAGfile.write("            print(\"Error Found (A) !!!!!\")\n")
            self.DAGfile.write("            print(event)\n")
            self.DAGfile.write("    except:\n")
            self.DAGfile.write("        print(event)\n")
            self.DAGfile.write("        if str(event).find(findPattern) != -1:\n")
            # self.DAGfile.write("        if str(event).find(\"ERROR\") != -1:\n")
            self.DAGfile.write("            errorFound = True\n")
            # self.DAGfile.write("            print(\"Error Found (B) !!!!!\")\n")
            self.DAGfile.write("\n")
            self.DAGfile.write("    return errorFound\n")
            self.DAGfile.write("\n")
            self.DAGfile.write("\n")
            self.DAGfile.write("def startDBImportExecution(command, instanceID):\n")
            self.DAGfile.write("\n")
            self.DAGfile.write(
                "    ssm_client = boto3.client('ssm',region_name='eu-west-1')\n"
            )
            self.DAGfile.write(
                "    logs_client = boto3.client('logs',region_name='eu-west-1')\n"
            )
            self.DAGfile.write("\n")
            # self.DAGfile.write("    instanceID = \"%s\"\n"%(self.airflowAWSinstanceIds))
            self.DAGfile.write(
                '    commandPath = "%s"\n' % (self.getDBImportCommandPath())
            )
            self.DAGfile.write(
                '    command = "%sbin/%s || echo ERROR; sleep 1"%(commandPath, command)\n'
            )
            self.DAGfile.write('    cloudWatchLogGroupName = "dbimport"\n')
            self.DAGfile.write("    errorFound = False\n")
            self.DAGfile.write("\n")
            self.DAGfile.write("\n")
            self.DAGfile.write(
                "    # Start the import by the help of SSM Send Command\n"
            )
            self.DAGfile.write("    sendCommand_response = ssm_client.send_command(\n")
            self.DAGfile.write("        InstanceIds=[instanceID],\n")
            self.DAGfile.write('        DocumentName="AWS-RunShellScript",\n')
            self.DAGfile.write(
                "        Parameters={'commands': [command], 'executionTimeout': ['172800']},\n"
            )
            self.DAGfile.write(
                "        CloudWatchOutputConfig={'CloudWatchLogGroupName': cloudWatchLogGroupName, 'CloudWatchOutputEnabled': True}\n"
            )
            self.DAGfile.write("        )\n")
            self.DAGfile.write("\n")
            self.DAGfile.write("\n")
            self.DAGfile.write(
                "    # Get the commandID and set the logStreamName for CloudWatch\n"
            )
            self.DAGfile.write(
                "    commandID = sendCommand_response['Command']['CommandId']\n"
            )
            self.DAGfile.write(
                '    logStreamNameStdOut = "%s/%s/aws-runShellScript/stdout"%(commandID, instanceID)\n'
            )
            self.DAGfile.write(
                '    logStreamNameStdErr = "%s/%s/aws-runShellScript/stderr"%(commandID, instanceID)\n'
            )
            self.DAGfile.write("\n")
            self.DAGfile.write("    print('')\n")
            self.DAGfile.write("    print('CloudWatch logs is available at:')\n")
            self.DAGfile.write("    print(logStreamNameStdOut)\n")
            self.DAGfile.write("    print(logStreamNameStdErr)\n")
            self.DAGfile.write("    print('')\n")
            self.DAGfile.write("\n")
            self.DAGfile.write("\n")
            self.DAGfile.write(
                "    # Run a loop until the command is actually running\n"
            )
            self.DAGfile.write("    commandRunning = False\n")
            self.DAGfile.write("    while commandRunning == False:\n")
            self.DAGfile.write("        try:\n")
            self.DAGfile.write(
                "            commandInvocation_response = ssm_client.get_command_invocation(\n"
            )
            self.DAGfile.write("                CommandId=commandID,\n")
            self.DAGfile.write("                InstanceId=instanceID\n")
            self.DAGfile.write("                )\n")
            self.DAGfile.write(
                "            commandStatus = commandInvocation_response['Status']\n"
            )
            self.DAGfile.write("\n")
            # self.DAGfile.write("            print('commandStatus = %s'%(commandStatus))\n")
            # self.DAGfile.write("\n")
            self.DAGfile.write(
                "            if commandStatus in ('InProgress', 'Success'):\n"
            )
            self.DAGfile.write("                commandRunning = True\n")
            self.DAGfile.write(
                "            elif commandStatus in ('Pending', 'Delayed'):\n"
            )
            self.DAGfile.write("                commandRunning = False\n")
            self.DAGfile.write(
                "            elif commandStatus in ('DeliveryTimedOut', 'ExecutionTimedOut', 'Failed', 'Cancelled', 'Undeliverable', 'Terminated', 'InvalidPlatform', 'AccessDenied'):\n"
            )
            self.DAGfile.write("                raise\n")
            self.DAGfile.write("\n")
            self.DAGfile.write(
                "        except botocore.exceptions.ClientError as err:\n"
            )
            self.DAGfile.write(
                "            if err.response['Error']['Code'] == 'InvocationDoesNotExist':\n"
            )
            self.DAGfile.write("                pass\n")
            self.DAGfile.write("            else:\n")
            self.DAGfile.write("                raise err\n")
            self.DAGfile.write("\n")
            self.DAGfile.write("        time.sleep(5)\n")
            self.DAGfile.write("\n")
            self.DAGfile.write(
                "    # Start fetching log from CloudWatch. Initial fetch will be from head and also fetch the token for upcomming messages\n"
            )
            self.DAGfile.write("    nextTokenStdOut = None\n")
            self.DAGfile.write("    nextTokenStdErr = None\n")
            self.DAGfile.write("    logEventsRunning = False\n")
            self.DAGfile.write("    while logEventsRunning == False:\n")
            self.DAGfile.write("        try:\n")
            self.DAGfile.write(
                "            responseStdOut = logs_client.get_log_events(\n"
            )
            self.DAGfile.write("                logGroupName='dbimport',\n")
            self.DAGfile.write("                logStreamName=logStreamNameStdOut,\n")
            self.DAGfile.write("                startFromHead=True\n")
            self.DAGfile.write("                )\n")
            self.DAGfile.write("\n")
            self.DAGfile.write("            for event in responseStdOut['events']:\n")
            self.DAGfile.write(
                "                if printAndFindEventErrors(event, startWithNewline = False) == True:\n"
            )
            self.DAGfile.write("                    errorFound = True\n")
            # self.DAGfile.write("                try:\n")
            # self.DAGfile.write("                    print(event['message'], end=\"\")\n")
            # self.DAGfile.write("                    if event['message'].find(\"\\nERROR\") != -1:\n")
            # # self.DAGfile.write("                    if event['message'].find(\"ERROR\") != -1:\n")
            # self.DAGfile.write("                        errorFound = True\n")
            # self.DAGfile.write("                        print(\"Error Found (1) !!!!!\")\n")
            # self.DAGfile.write("                        print(event)\n")
            # self.DAGfile.write("                except:\n")
            # self.DAGfile.write("                    print(event)\n")
            # self.DAGfile.write("                    if str(event).find(\"\\nERROR\") != -1:\n")
            # # self.DAGfile.write("                    if str(event).find(\"ERROR\") != -1:\n")
            # self.DAGfile.write("                        errorFound = True\n")
            # self.DAGfile.write("                        print(\"Error Found (2) !!!!!\")\n")
            self.DAGfile.write("\n")
            self.DAGfile.write(
                "            nextTokenStdOut = responseStdOut['nextForwardToken']\n"
            )
            self.DAGfile.write("            logEventsRunning = True\n")
            self.DAGfile.write("\n")
            self.DAGfile.write(
                "            responseStdErr = logs_client.get_log_events(\n"
            )
            self.DAGfile.write("                logGroupName='dbimport',\n")
            self.DAGfile.write("                logStreamName=logStreamNameStdErr,\n")
            self.DAGfile.write("                startFromHead=True\n")
            self.DAGfile.write("                )\n")
            self.DAGfile.write("\n")
            self.DAGfile.write(
                "            nextTokenStdErr = responseStdErr['nextForwardToken']\n"
            )
            self.DAGfile.write("\n")
            self.DAGfile.write("            for event in responseStdErr['events']:\n")
            self.DAGfile.write(
                "                if printAndFindEventErrors(event, startWithNewline = True) == True:\n"
            )
            self.DAGfile.write("                    errorFound = True\n")
            # self.DAGfile.write("                try:\n")
            # self.DAGfile.write("                    print(event['message'], end=\"\")\n")
            # self.DAGfile.write("                    if event['message'].find(\"\\nERROR\") != -1:\n")
            # # self.DAGfile.write("                    if event['message'].find(\"ERROR\") != -1:\n")
            # self.DAGfile.write("                        errorFound = True\n")
            # self.DAGfile.write("                        print(\"Error Found (1) !!!!!\")\n")
            # self.DAGfile.write("                        print(event)\n")
            # self.DAGfile.write("                except:\n")
            # self.DAGfile.write("                    print(event)\n")
            # self.DAGfile.write("                    if str(event).find(\"\\nERROR\") != -1:\n")
            # # self.DAGfile.write("                    if str(event).find(\"ERROR\") != -1:\n")
            # self.DAGfile.write("                        errorFound = True\n")
            # self.DAGfile.write("                        print(\"Error Found (2) !!!!!\")\n")
            self.DAGfile.write("\n")
            self.DAGfile.write(
                "        except botocore.exceptions.ClientError as err:\n"
            )
            self.DAGfile.write(
                "            if err.response['Error']['Code'] == 'ResourceNotFoundException':\n"
            )
            self.DAGfile.write("                pass\n")
            self.DAGfile.write(
                "            elif err.response['Error']['Code'] == 'ThrottlingException':\n"
            )
            self.DAGfile.write("                pass\n")
            self.DAGfile.write("            else:\n")
            self.DAGfile.write(
                "                print(\"DEBUG: %s\"%(err.response['Error']['Code']))\n"
            )
            self.DAGfile.write("                raise err\n")
            self.DAGfile.write("\n")
            self.DAGfile.write("\n")
            self.DAGfile.write("        time.sleep(1)\n")
            self.DAGfile.write("\n")
            self.DAGfile.write("    if commandStatus == 'TimedOut':\n")
            self.DAGfile.write("        print('AWS SSM Send-command timeout!')\n")
            self.DAGfile.write("        errorFound = True\n")
            self.DAGfile.write("\n")
            self.DAGfile.write(
                "    # Loop while the command is running and print all messages from CloudWatch at the same time\n"
            )
            self.DAGfile.write("    while commandStatus == 'InProgress':\n")
            self.DAGfile.write("        try:\n")
            self.DAGfile.write(
                "            commandInvocation_response = ssm_client.get_command_invocation(\n"
            )
            self.DAGfile.write("                CommandId=commandID,\n")
            self.DAGfile.write("                InstanceId=instanceID\n")
            self.DAGfile.write("                )\n")
            self.DAGfile.write("\n")
            self.DAGfile.write(
                "            commandStatus = commandInvocation_response['Status']\n"
            )
            # self.DAGfile.write("            print(commandInvocation_response)\n")
            self.DAGfile.write("\n")
            self.DAGfile.write("            if commandStatus == 'TimedOut':\n")
            self.DAGfile.write(
                "                print('AWS SSM Send-command timeout!')\n"
            )
            self.DAGfile.write("                errorFound = True\n")
            self.DAGfile.write("                break\n")
            self.DAGfile.write("            elif commandStatus != 'InProgress':\n")
            self.DAGfile.write("                break\n")
            self.DAGfile.write("\n")
            self.DAGfile.write(
                "            responseStdOut = logs_client.get_log_events(\n"
            )
            self.DAGfile.write("                logGroupName='dbimport',\n")
            self.DAGfile.write("                logStreamName=logStreamNameStdOut,\n")
            self.DAGfile.write("                nextToken=nextTokenStdOut\n")
            self.DAGfile.write("                )\n")
            self.DAGfile.write("\n")
            self.DAGfile.write(
                "            nextTokenStdOut = responseStdOut['nextForwardToken']\n"
            )
            self.DAGfile.write("\n")
            self.DAGfile.write("            for event in responseStdOut['events']:\n")
            self.DAGfile.write(
                "                if printAndFindEventErrors(event, startWithNewline = False) == True:\n"
            )
            self.DAGfile.write("                    errorFound = True\n")
            # self.DAGfile.write("                try:\n")
            # self.DAGfile.write("                    print(str(event['message']), end=\"\")\n")
            # self.DAGfile.write("                    if event['message'].find(\"\\nERROR\") != -1:\n")
            # # self.DAGfile.write("                    if event['message'].find(\"ERROR\") != -1:\n")
            # self.DAGfile.write("                        errorFound = True\n")
            # self.DAGfile.write("                        print(\"Error Found (3) !!!!!\")\n")
            # self.DAGfile.write("                        print(event)\n")
            # self.DAGfile.write("                except:\n")
            # self.DAGfile.write("                    print(event)\n")
            # self.DAGfile.write("                    if str(event).find(\"\\nERROR\") != -1:\n")
            # # self.DAGfile.write("                    if str(event).find(\"ERROR\") != -1:\n")
            # self.DAGfile.write("                        errorFound = True\n")
            # self.DAGfile.write("                        print(\"Error Found (4) !!!!!\")\n")
            self.DAGfile.write("\n")
            self.DAGfile.write("            if nextTokenStdErr == None:\n")
            self.DAGfile.write(
                "                responseStdErr = logs_client.get_log_events(\n"
            )
            self.DAGfile.write("                    logGroupName='dbimport',\n")
            self.DAGfile.write(
                "                    logStreamName=logStreamNameStdErr,\n"
            )
            self.DAGfile.write("                    startFromHead=True\n")
            self.DAGfile.write("                    )\n")
            self.DAGfile.write("            else:\n")
            self.DAGfile.write(
                "                responseStdErr = logs_client.get_log_events(\n"
            )
            self.DAGfile.write("                    logGroupName='dbimport',\n")
            self.DAGfile.write(
                "                    logStreamName=logStreamNameStdErr,\n"
            )
            self.DAGfile.write("                    nextToken=nextTokenStdErr\n")
            self.DAGfile.write("                    )\n")
            self.DAGfile.write("\n")
            self.DAGfile.write(
                "            nextTokenStdErr = responseStdErr['nextForwardToken']\n"
            )
            self.DAGfile.write("\n")
            self.DAGfile.write("            for event in responseStdErr['events']:\n")
            self.DAGfile.write(
                "                if printAndFindEventErrors(event, startWithNewline = True) == True:\n"
            )
            self.DAGfile.write("                    errorFound = True\n")
            # self.DAGfile.write("                try:\n")
            # self.DAGfile.write("                    print(str(event['message']), end=\"\")\n")
            # self.DAGfile.write("                    if event['message'].find(\"\\nERROR\") != -1:\n")
            # # self.DAGfile.write("                    if event['message'].find(\"ERROR\") != -1:\n")
            # self.DAGfile.write("                        errorFound = True\n")
            # self.DAGfile.write("                        print(\"Error Found (3) !!!!!\")\n")
            # self.DAGfile.write("                        print(event)\n")
            # self.DAGfile.write("                except:\n")
            # self.DAGfile.write("                    print(event)\n")
            # self.DAGfile.write("                    if str(event).find(\"\\nERROR\") != -1:\n")
            # # self.DAGfile.write("                    if str(event).find(\"ERROR\") != -1:\n")
            # self.DAGfile.write("                        errorFound = True\n")
            # self.DAGfile.write("                        print(\"Error Found (4) !!!!!\")\n")
            self.DAGfile.write("\n")
            self.DAGfile.write(
                "        except botocore.exceptions.ClientError as err:\n"
            )
            self.DAGfile.write(
                "            if err.response['Error']['Code'] == 'ResourceNotFoundException':\n"
            )
            self.DAGfile.write("                pass\n")
            self.DAGfile.write(
                "            elif err.response['Error']['Code'] == 'ThrottlingException':\n"
            )
            self.DAGfile.write("                pass\n")
            self.DAGfile.write("            else:\n")
            self.DAGfile.write(
                "                print(\"DEBUG: %s\"%(err.response['Error']['Code']))\n"
            )
            self.DAGfile.write("                raise err\n")
            self.DAGfile.write("\n")
            self.DAGfile.write("        except UnboundLocalError:\n")
            self.DAGfile.write("            pass\n")
            self.DAGfile.write("\n")
            self.DAGfile.write("        time.sleep(15)\n")
            self.DAGfile.write("\n")
            self.DAGfile.write("\n")
            self.DAGfile.write(
                "    # Loop to get the last message and print in the output\n"
            )
            self.DAGfile.write("    lastMessagePrinted = False\n")
            self.DAGfile.write("    while lastMessagePrinted == False:\n")
            self.DAGfile.write("        try:\n")
            self.DAGfile.write(
                "            responseStdOut = logs_client.get_log_events(\n"
            )
            self.DAGfile.write("                logGroupName='dbimport',\n")
            self.DAGfile.write("                logStreamName=logStreamNameStdOut,\n")
            self.DAGfile.write("                nextToken=nextTokenStdOut\n")
            self.DAGfile.write("                )\n")
            self.DAGfile.write("\n")
            self.DAGfile.write(
                "            nextTokenStdOut = responseStdOut['nextForwardToken']\n"
            )
            self.DAGfile.write("            for event in responseStdOut['events']:\n")
            self.DAGfile.write(
                "                if printAndFindEventErrors(event, startWithNewline = False) == True:\n"
            )
            self.DAGfile.write("                    errorFound = True\n")
            # self.DAGfile.write("                try:\n")
            # self.DAGfile.write("                    print(str(event['message']), end=\"\")\n")
            # self.DAGfile.write("                    if event['message'].find(\"\\nERROR\") != -1:\n")
            # # self.DAGfile.write("                    if event['message'].find(\"ERROR\") != -1:\n")
            # self.DAGfile.write("                        errorFound = True\n")
            # self.DAGfile.write("                        print(\"Error Found (5) !!!!!\")\n")
            # self.DAGfile.write("                        print(event)\n")
            # self.DAGfile.write("                except:\n")
            # self.DAGfile.write("                    print(event)\n")
            # self.DAGfile.write("                    if str(event).find(\"\\nERROR\") != -1:\n")
            # # self.DAGfile.write("                    if str(event).find(\"ERROR\") != -1:\n")
            # self.DAGfile.write("                        errorFound = True\n")
            # self.DAGfile.write("                        print(\"Error Found (6) !!!!!\")\n")
            self.DAGfile.write("\n")
            self.DAGfile.write("            lastMessagePrinted = True\n")
            self.DAGfile.write("\n")
            self.DAGfile.write("            if nextTokenStdErr == None:\n")
            self.DAGfile.write(
                "                responseStdErr = logs_client.get_log_events(\n"
            )
            self.DAGfile.write("                    logGroupName='dbimport',\n")
            self.DAGfile.write(
                "                    logStreamName=logStreamNameStdErr,\n"
            )
            self.DAGfile.write("                    startFromHead=True\n")
            self.DAGfile.write("                    )\n")
            self.DAGfile.write("            else:\n")
            self.DAGfile.write(
                "                responseStdErr = logs_client.get_log_events(\n"
            )
            self.DAGfile.write("                    logGroupName='dbimport',\n")
            self.DAGfile.write(
                "                    logStreamName=logStreamNameStdErr,\n"
            )
            self.DAGfile.write("                    nextToken=nextTokenStdErr\n")
            self.DAGfile.write("                    )\n")
            self.DAGfile.write("\n")
            self.DAGfile.write(
                "            nextTokenStdErr = responseStdErr['nextForwardToken']\n"
            )
            self.DAGfile.write("\n")
            self.DAGfile.write("            for event in responseStdErr['events']:\n")
            self.DAGfile.write(
                "                if printAndFindEventErrors(event, startWithNewline = True) == True:\n"
            )
            self.DAGfile.write("                    errorFound = True\n")
            # self.DAGfile.write("                try:\n")
            # self.DAGfile.write("                    print(str(event['message']), end=\"\")\n")
            # self.DAGfile.write("                    if event['message'].find(\"\\nERROR\") != -1:\n")
            # # self.DAGfile.write("                    if event['message'].find(\"ERROR\") != -1:\n")
            # self.DAGfile.write("                        errorFound = True\n")
            # self.DAGfile.write("                        print(\"Error Found (5) !!!!!\")\n")
            # self.DAGfile.write("                        print(event)\n")
            # self.DAGfile.write("                except:\n")
            # self.DAGfile.write("                    print(event)\n")
            # self.DAGfile.write("                    if str(event).find(\"\\nERROR\") != -1:\n")
            # # self.DAGfile.write("                    if str(event).find(\"ERROR\") != -1:\n")
            # self.DAGfile.write("                        errorFound = True\n")
            # self.DAGfile.write("                        print(\"Error Found (6) !!!!!\")\n")
            self.DAGfile.write("\n")
            self.DAGfile.write(
                "        except botocore.exceptions.ClientError as err:\n"
            )
            self.DAGfile.write(
                "            if err.response['Error']['Code'] == 'ResourceNotFoundException':\n"
            )
            self.DAGfile.write("                pass\n")
            self.DAGfile.write(
                "            elif err.response['Error']['Code'] == 'ThrottlingException':\n"
            )
            self.DAGfile.write("                pass\n")
            self.DAGfile.write("            else:\n")
            self.DAGfile.write(
                "                print(\"DEBUG: %s\"%(err.response['Error']['Code']))\n"
            )
            self.DAGfile.write("                raise err\n")
            self.DAGfile.write("\n")
            self.DAGfile.write("        except UnboundLocalError:\n")
            self.DAGfile.write("            pass\n")
            self.DAGfile.write("\n")
            self.DAGfile.write("        time.sleep(1)\n")
            self.DAGfile.write("\n")
            # self.DAGfile.write("    print(response)\n")
            # self.DAGfile.write("    print('Sequence - 008')\n")
            # self.DAGfile.write("    print(errorFound)\n")
            # self.DAGfile.write("    print(commandInvocation_response)\n")
            # self.DAGfile.write("    if commandStatus != 'Success':\n")
            self.DAGfile.write("    if errorFound == True:\n")
            # self.DAGfile.write("        print('Raising error as errorFound = True')\n")
            self.DAGfile.write("        raise\n")
            self.DAGfile.write("\n")
            # self.DAGfile.write("    print('AWS ssm send-command exit')\n")
            self.DAGfile.write("\n")

        self.DAGfile.write("\n")
        self.DAGfile.write("default_args = {\n")
        self.DAGfile.write("    'owner': 'airflow',\n")
        self.DAGfile.write("    'depends_on_past': False,\n")
        self.DAGfile.write(
            "    'start_date': datetime(2017, 1, 1, 0, 0, tzinfo=local_tz),\n"
        )
        self.DAGfile.write("    'max_active_runs': 1,\n")

        if email != None and email.strip() != "":
            email = email.split(",")
            self.DAGfile.write("    'email': %s,\n" % (email))
        else:
            self.DAGfile.write("    'email': Email_receiver,\n")

        if email_on_failure == 1:
            self.DAGfile.write("    'email_on_failure': True,\n")
        else:
            self.DAGfile.write("    'email_on_failure': False,\n")

        if email_on_retries == 1:
            self.DAGfile.write("    'email_on_retry': True,\n")
        else:
            self.DAGfile.write("    'email_on_retry': False,\n")

        if retryExponentialBackoff == 1:
            self.DAGfile.write("    'retry_exponential_backoff': True,\n")
        else:
            self.DAGfile.write("    'retry_exponential_backoff': False,\n")

        self.DAGfile.write("    'retries': 0,\n")
        self.DAGfile.write("    'pool': '%s',\n" % (defaultPool))
        self.DAGfile.write("    'retry_delay': timedelta(minutes=5),\n")

        if slaWarningTime != None and type(slaWarningTime) == time:
            self.DAGfile.write(
                "    'sla': timedelta(hours=%s, minutes=%s, seconds=%s),\n"
                % (slaWarningTime.hour, slaWarningTime.minute, slaWarningTime.second)
            )

        self.DAGfile.write("}\n")
        self.DAGfile.write("\n")
        self.DAGfile.write("dag = DAG(\n")
        self.DAGfile.write("    '%s',\n" % (dagName))
        self.DAGfile.write("    default_args=default_args,\n")
        self.DAGfile.write("    description='%s',\n" % (dagName))
        self.DAGfile.write("    catchup=False,\n")

        if tags != None and tags.strip() != "":
            self.DAGfile.write(
                "    tags=[%s],\n"
                % (
                    ", ".join(
                        [
                            '"{}"'.format(value)
                            for value in [x.strip() for x in tags.split(",")]
                        ]
                    )
                )
            )

        try:
            if int(concurrency) > 0:
                self.DAGfile.write("    concurrency=%s,\n" % (int(concurrency)))
        except ValueError:
            pass

        self.DAGfile.write("    schedule_interval=%s)\n" % (cronSchedule))

        self.DAGfile.write("\n")
        if self.airflowMode == "default":
            self.DAGfile.write("start = BashOperator(\n")
            self.DAGfile.write("    task_id='start',\n")
            if self.TaskQueueForDummy != None:
                self.DAGfile.write(
                    "    queue='%s',\n" % (self.TaskQueueForDummy.strip())
                )
            self.DAGfile.write(
                "    bash_command='%sbin/manage --checkAirflowExecution --airflowDAG=%s',\n"
                % (self.getDBImportCommandPath(sudoUser=sudoUser), dagName)
            )
            self.DAGfile.write("    priority_weight=100,\n")
            self.DAGfile.write("    weight_rule='absolute',\n")
            self.DAGfile.write("    pool=None,\n")
            self.DAGfile.write("    dag=dag)\n")
            self.DAGfile.write("\n")

            self.DAGfile.write("stop = BashOperator(\n")
            self.DAGfile.write("    task_id='stop',\n")
            if self.TaskQueueForDummy != None:
                self.DAGfile.write(
                    "    queue='%s',\n" % (self.TaskQueueForDummy.strip())
                )
            self.DAGfile.write(
                "    bash_command='%sbin/manage --sendAirflowStopMessage --airflowDAG=%s',\n"
                % (self.getDBImportCommandPath(sudoUser=sudoUser), dagName)
            )
            self.DAGfile.write("    priority_weight=100,\n")
            self.DAGfile.write("    weight_rule='absolute',\n")
            self.DAGfile.write("    pool=None,\n")
            self.DAGfile.write("    dag=dag)\n")
            self.DAGfile.write("\n")

        elif self.airflowMode == "aws_mwaa":
            self.DAGfile.write("start = PythonOperator(\n")
            self.DAGfile.write("    task_id='start',\n")
            if self.TaskQueueForDummy != None:
                self.DAGfile.write(
                    "    queue='%s',\n" % (self.TaskQueueForDummy.strip())
                )
            self.DAGfile.write("    python_callable=startDBImportExecution,\n")
            self.DAGfile.write(
                '    op_kwargs={"command": "manage --checkAirflowExecution --airflowDAG=%s", "instanceID": "%s"},\n'
                % (dagName, self.getAWSInstanceID("start"))
            )
            self.DAGfile.write("    priority_weight=100,\n")
            self.DAGfile.write("    weight_rule='absolute',\n")
            self.DAGfile.write("    pool=None,\n")
            self.DAGfile.write("    dag=dag)\n")
            self.DAGfile.write("\n")

            self.DAGfile.write("stop = PythonOperator(\n")
            self.DAGfile.write("    task_id='stop',\n")
            if self.TaskQueueForDummy != None:
                self.DAGfile.write(
                    "    queue='%s',\n" % (self.TaskQueueForDummy.strip())
                )
            self.DAGfile.write("    python_callable=startDBImportExecution,\n")
            self.DAGfile.write(
                '    op_kwargs={"command": "manage --checkAirflowExecution --airflowDAG=%s", "instanceID": "%s"},\n'
                % (dagName, self.getAWSInstanceID("stop"))
            )
            self.DAGfile.write("    priority_weight=100,\n")
            self.DAGfile.write("    weight_rule='absolute',\n")
            self.DAGfile.write("    pool=None,\n")
            self.DAGfile.write("    dag=dag)\n")
            self.DAGfile.write("\n")

        if self.airflowMajorVersion == 1:
            self.DAGfile.write("def always_trigger(context, dag_run_obj):\n")
            self.DAGfile.write("    return dag_run_obj\n")
            self.DAGfile.write("\n")

        if importPhaseFinishFirst == True:
            self.DAGfile.write("Import_Phase_Finished = DummyOperator(\n")
            self.DAGfile.write("    task_id='Import_Phase_Finished',\n")
            if self.TaskQueueForDummy != None:
                self.DAGfile.write(
                    "    queue='%s',\n" % (self.TaskQueueForDummy.strip())
                )
            self.DAGfile.write("    priority_weight=100,\n")
            self.DAGfile.write("    weight_rule='absolute',\n")
            self.DAGfile.write("    dag=dag)\n")
            self.DAGfile.write("\n")

        airflow_dag_sensors = aliased(configSchema.airflowDagSensors, name="ads")
        sensors = (
            session.query(airflow_dag_sensors.dag_name)
            .select_from(airflow_dag_sensors)
            .filter(airflow_dag_sensors.dag_name == dagName)
            .count()
        )

        if sensors > 0:
            tasks_sensors_exists = True

        airflow_tasks = aliased(configSchema.airflowTasks)
        tasks = pd.DataFrame(
            session.query(airflow_tasks.dag_name, airflow_tasks.placement)
            .select_from(airflow_tasks)
            .filter(airflow_tasks.dag_name == dagName)
            .filter(airflow_tasks.include_in_airflow == 1)
            .all()
        )

        if len(tasks) == 0:
            tasks = pd.DataFrame(columns=["dag_name", "placement"])

        if (
            len(tasks.loc[tasks["placement"] == "before main"]) > 0
            or self.createPoolsWithTasks == True
        ):
            tasks_before_main_exists = True

        if len(tasks.loc[tasks["placement"] == "after main"]) > 0:
            tasks_after_main_exists = True

        if tasks_before_main_exists == True:
            self.DAGfile.write("%s = DummyOperator(\n" % (self.preStopTask))
            self.DAGfile.write("    task_id='%s',\n" % (self.preStopTask))
            if self.TaskQueueForDummy != None:
                self.DAGfile.write(
                    "    queue='%s',\n" % (self.TaskQueueForDummy.strip())
                )
            self.DAGfile.write("    priority_weight=100,\n")
            self.DAGfile.write("    weight_rule='absolute',\n")
            self.DAGfile.write("    dag=dag)\n")
            self.DAGfile.write("\n")

        if tasks_after_main_exists == True:
            self.DAGfile.write("%s = DummyOperator(\n" % (self.postStartTask))
            self.DAGfile.write("    task_id='%s',\n" % (self.postStartTask))
            if self.TaskQueueForDummy != None:
                self.DAGfile.write(
                    "    queue='%s',\n" % (self.TaskQueueForDummy.strip())
                )
            self.DAGfile.write("    priority_weight=100,\n")
            self.DAGfile.write("    weight_rule='absolute',\n")
            self.DAGfile.write("    dag=dag)\n")
            self.DAGfile.write("\n")

        if tasks_sensors_exists == True:
            self.DAGfile.write("%s = DummyOperator(\n" % (self.sensorStopTask))
            self.DAGfile.write("    task_id='%s',\n" % (self.sensorStopTask))
            if self.TaskQueueForDummy != None:
                self.DAGfile.write(
                    "    queue='%s',\n" % (self.TaskQueueForDummy.strip())
                )
            self.DAGfile.write("    priority_weight=100,\n")
            self.DAGfile.write("    weight_rule='absolute',\n")
            self.DAGfile.write("    dag=dag)\n")
            self.DAGfile.write("\n")

        if tasks_sensors_exists == False and tasks_before_main_exists == True:
            self.mainStartTask = self.preStopTask

        if tasks_sensors_exists == True and tasks_before_main_exists == False:
            self.mainStartTask = self.sensorStopTask

        if tasks_sensors_exists == True and tasks_before_main_exists == True:
            self.preStartTask = self.sensorStopTask
            self.mainStartTask = self.preStopTask

        if tasks_after_main_exists == True:
            self.mainStopTask = self.postStartTask

        session.close()

        log.debug("sensorStartTask = %s" % (self.sensorStartTask))
        log.debug("sensorStopTask = %s" % (self.sensorStopTask))
        log.debug("preStartTask = %s" % (self.preStartTask))
        log.debug("preStopTask = %s" % (self.preStopTask))
        log.debug("mainStartTask = %s" % (self.mainStartTask))
        log.debug("mainStopTask = %s" % (self.mainStopTask))
        log.debug("postStartTask = %s" % (self.postStartTask))
        log.debug("postStopTask = %s" % (self.postStopTask))

    def closeDAGfile(self):
        log = logging.getLogger(self.logger)
        self.DAGfile.close()

        try:
            os.chmod(self.DAGfilename, int(self.DAGfilePermission, 8))
        except PermissionError:
            log.warning("Could not change file mode to '%s'" % (self.DAGfilePermission))

        try:
            shutil.chown(self.DAGfilename, group=self.DAGfileGroup)
        except PermissionError:
            log.warning(
                "Could not change group owner of file to '%s'. Permission Denied"
                % (self.DAGfileGroup)
            )
        except LookupError:
            log.warning(
                "Could not change group owner of file to '%s'. Group does not exists"
                % (self.DAGfileGroup)
            )

        if self.writeDAG == True:
            if self.airflowMode == "default":
                try:
                    shutil.copy(self.DAGfilename, self.DAGfilenameInAirflow)
                    os.chmod(self.DAGfilenameInAirflow, int(self.DAGfilePermission, 8))
                    shutil.chown(self.DAGfilenameInAirflow, group=self.DAGfileGroup)
                except PermissionError:
                    log.warning(
                        "Could not copy and change permission of file '%s'. Permission Denied"
                        % (self.DAGfilenameInAirflow)
                    )
                except LookupError:
                    pass
                else:
                    print("DAG file written to %s" % (self.DAGfilenameInAirflow))

            elif self.airflowMode == "aws_mwaa":
                try:
                    s3client = boto3.client(
                        "s3", region_name=self.common_config.awsRegion
                    )
                    # response = s3client.upload_file(self.DAGfilename, "dl-mwaa-artifacts-uat-eu-west-1-176686534647", self.DAGfilenameInAwsMWAA)
                    response = s3client.upload_file(
                        self.DAGfilename, self.DAGdirectory, self.DAGfilenameInAwsMWAA
                    )
                except botocore.exceptions.ClientError as err:
                    log.error(err)
                except boto3.exceptions.S3UploadFailedError as err:
                    log.error(err)
                except botocore.exceptions.ParamValidationError as err:
                    log.error(err)

        else:
            print("DAG file written to %s" % (self.DAGfilename))

    def addFailureTask(self, taskName):
        log = logging.getLogger(self.logger)
        self.DAGfile.write("%s = BashOperator(\n" % (taskName))
        self.DAGfile.write("    task_id='%s',\n" % (taskName))
        self.DAGfile.write("    bash_command='exit 1 ',\n")
        self.DAGfile.write("    priority_weight=1,\n")
        self.DAGfile.write("    weight_rule='absolute',\n")
        self.DAGfile.write("    dag=dag)\n")
        self.DAGfile.write("\n")

    def addTasksToDAGfile(
        self, dagName, mainDagSchedule, defaultRetries=0, defaultSudoUser=""
    ):
        log = logging.getLogger(self.logger)

        # session = self.configDBSession()
        session = self.getDBImportSession()

        if defaultSudoUser == None:
            defaultSudoUser = ""

        airflow_tasks = aliased(configSchema.airflowTasks)

        tasks = pd.DataFrame(
            session.query(
                airflow_tasks.task_name,
                airflow_tasks.task_type,
                airflow_tasks.placement,
                airflow_tasks.airflow_pool,
                airflow_tasks.airflow_priority,
                airflow_tasks.task_dependency_downstream,
                airflow_tasks.task_dependency_upstream,
                airflow_tasks.task_config,
                airflow_tasks.jdbc_dbalias,
                airflow_tasks.hive_db,
                airflow_tasks.sensor_poke_interval,
                airflow_tasks.sensor_timeout_minutes,
                airflow_tasks.sensor_connection,
                airflow_tasks.sensor_soft_fail,
                airflow_tasks.sudo_user,
            )
            .select_from(airflow_tasks)
            .filter(airflow_tasks.dag_name == dagName)
            .filter(airflow_tasks.include_in_airflow == 1)
            .all()
        ).fillna("")

        task_dependencies = ""
        all_task_dependencies = tasks.filter(
            [
                "task_name",
                "task_dependency_upstream",
                "task_dependency_downstream",
                "placement",
            ]
        )
        all_task_dependencies_upstream = tasks.filter(["task_dependency_upstream"])
        all_task_dependencies_downstream = tasks.filter(["task_dependency_downstream"])
        task_dependency_warning_printed = False

        # 		print(allTaskDependencies)
        # 		print(type(allTaskDependencies))

        for index, row in tasks.iterrows():
            sensor_poke_interval = row["sensor_poke_interval"]
            task_name = row["task_name"]

            sudo_user = row["sudo_user"]
            if sudo_user == None or sudo_user == "":
                sudo_user = defaultSudoUser

            if sensor_poke_interval == "":
                # Default to 5 minutes
                sensor_poke_interval = 300

            sensor_timeout_seconds = row["sensor_timeout_minutes"]
            if sensor_timeout_seconds == "":
                # Default to 5 hours
                sensor_timeout_seconds = "18000"
            else:
                sensor_timeout_seconds = sensor_timeout_seconds * 60

            if row["task_type"] == "DAG Sensor":
                add_dag_sensor = True
                airflow_custom_dags = aliased(configSchema.airflowCustomDags)
                airflow_etl_dags = aliased(configSchema.airflowEtlDags)
                airflow_export_dags = aliased(configSchema.airflowExportDags)
                airflow_import_dags = aliased(configSchema.airflowImportDags)

                if "." in row["task_config"]:
                    wait_for_dag = row["task_config"].split(".")[0]
                    wait_for_task = row["task_config"].split(".")[1]
                else:
                    wait_for_dag = row["task_config"].split(".")[0]
                    wait_for_task = "stop"

                import_sensor_schedule = (
                    session.query(airflow_import_dags.schedule_interval)
                    .filter(airflow_import_dags.dag_name == wait_for_dag)
                    .one_or_none()
                )
                export_sensor_schedule = (
                    session.query(airflow_export_dags.schedule_interval)
                    .filter(airflow_export_dags.dag_name == wait_for_dag)
                    .one_or_none()
                )
                custom_sensor_schedule = (
                    session.query(airflow_custom_dags.schedule_interval)
                    .filter(airflow_custom_dags.dag_name == wait_for_dag)
                    .one_or_none()
                )
                etl_sensor_schedule = (
                    session.query(airflow_etl_dags.schedule_interval)
                    .filter(airflow_etl_dags.dag_name == wait_for_dag)
                    .one_or_none()
                )

                wait_dag_schedule = ""
                if import_sensor_schedule != None:
                    wait_dag_schedule = import_sensor_schedule[0]
                elif export_sensor_schedule != None:
                    wait_dag_schedule = export_sensor_schedule[0]
                elif custom_sensor_schedule != None:
                    wait_dag_schedule = custom_sensor_schedule[0]
                elif etl_sensor_schedule != None:
                    wait_dag_schedule = etl_sensor_schedule[0]

                if wait_dag_schedule == "":
                    log.warning("Issue on DAG '%s'" % (dagName))
                    log.warning("There is no schedule interval for DAG to wait for")
                    log.warning(
                        "The DAG Sensor will not be added to the DAG. Instead, there will be a Task that always failes. The DAG will never execute"
                    )

                    row["task_name"] = "%s_FAILURE" % (row["task_name"])
                    self.addFailureTask(row["task_name"])
                    add_dag_sensor = False
                # 					continue
                # 					log.error("Cant find schedule interval for DAG to wait for")
                # 					self.DAGfile.close()
                # 					self.common_config.remove_temporary_files()
                # 					sys.exit(1)

                main_dag_match_hour_min = re.search(
                    "^[0-2][0-9]:[0-5][0-9]$", mainDagSchedule
                )
                wait_dag_match_hour_min = re.search(
                    "^[0-2][0-9]:[0-5][0-9]$", wait_dag_schedule
                )

                time_diff = "0"

                if (
                    main_dag_match_hour_min == None and wait_dag_match_hour_min != None
                ) or (
                    main_dag_match_hour_min != None and wait_dag_match_hour_min == None
                ):
                    log.warning("Issue on DAG '%s'" % (dagName))
                    log.warning(
                        "Both the current DAG and the DAG the sensor is waiting for must have the same scheduling format (HH:MM or cron)"
                    )
                    log.warning(
                        "The DAG Sensor will not be added to the DAG. Instead, there will be a Task that always failes. The DAG will never execute"
                    )
                    row["task_name"] = "%s_FAILURE" % (row["task_name"])
                    self.addFailureTask(row["task_name"])
                    add_dag_sensor = False

                if main_dag_match_hour_min != None and add_dag_sensor == True:
                    # Time is in HH:MM format for both DAG's. So now we can calculate the diff in time between them
                    noon = datetime.strptime("12:00", "%H:%M")
                    main_dag_schedule_date_time = datetime.strptime(
                        mainDagSchedule, "%H:%M"
                    )
                    wait_dag_schedule_date_time = datetime.strptime(
                        wait_dag_schedule, "%H:%M"
                    )

                    # Add or remove half a day so we dont calculate over midnight.
                    if main_dag_schedule_date_time > noon:
                        main_dag_schedule_date_time = (
                            main_dag_schedule_date_time - timedelta(seconds=43200)
                        )
                    else:
                        main_dag_schedule_date_time = (
                            main_dag_schedule_date_time + timedelta(seconds=43200)
                        )

                    if wait_dag_schedule_date_time > noon:
                        wait_dag_schedule_date_time = (
                            wait_dag_schedule_date_time - timedelta(seconds=43200)
                        )
                    else:
                        wait_dag_schedule_date_time = (
                            wait_dag_schedule_date_time + timedelta(seconds=43200)
                        )

                    if main_dag_schedule_date_time > wait_dag_schedule_date_time:
                        time_diff = (
                            main_dag_schedule_date_time - wait_dag_schedule_date_time
                        )
                        minus_text = ""
                    else:
                        time_diff = (
                            wait_dag_schedule_date_time - main_dag_schedule_date_time
                        )
                        minus_text = "-"

                    time_diff = str(minus_text + str(time_diff.seconds))
                elif add_dag_sensor == True:
                    if mainDagSchedule != wait_dag_schedule:
                        log.warning("Issue on DAG '%s'" % (dagName))
                        log.warning(
                            "When using cron or cron alias schedules for DAG sensors, the schedule time in both DAG's must match"
                        )
                        log.warning(
                            "The DAG Sensor will not be added to the DAG. Instead, there will be a Task that always failes. The DAG will never execute"
                        )
                        row["task_name"] = "%s_FAILURE" % (row["task_name"])
                        self.addFailureTask(row["task_name"])
                        add_dag_sensor = False

                if add_dag_sensor == True:
                    self.DAGfile.write(
                        "%s = ExternalTaskSensor(\n" % (row["task_name"])
                    )
                    self.DAGfile.write("    task_id='%s',\n" % (row["task_name"]))
                    self.DAGfile.write("    external_dag_id='%s',\n" % (wait_for_dag))
                    self.DAGfile.write("    external_task_id='%s',\n" % (wait_for_task))
                    self.DAGfile.write("    retries=0,\n")
                    self.DAGfile.write(
                        "    execution_delta=timedelta(seconds=%s),\n" % (time_diff)
                    )
                    if row["airflow_pool"] != "":
                        self.DAGfile.write("    pool='%s',\n" % (row["airflow_pool"]))
                    if row["airflow_priority"] != "":
                        self.DAGfile.write(
                            "    priority_weight=%s,\n" % (int(row["airflow_priority"]))
                        )
                        self.DAGfile.write("    weight_rule='absolute',\n")
                    else:
                        self.DAGfile.write("    priority_weight=100,\n")
                        self.DAGfile.write("    weight_rule='absolute',\n")
                    self.DAGfile.write(
                        "    timeout=%s,\n" % (int(sensor_timeout_seconds))
                    )
                    self.DAGfile.write(
                        "    poke_interval=%s,\n" % (int(sensor_poke_interval))
                    )
                    self.DAGfile.write("    mode='reschedule',\n")
                    if (
                        row["sensor_soft_fail"] is not None
                        and row["sensor_soft_fail"] == 1
                    ):
                        self.DAGfile.write("    soft_fail=True,\n")
                    self.DAGfile.write("    dag=dag)\n")
                    self.DAGfile.write("\n")

            if row["task_type"] == "SQL Sensor":
                if row["sensor_connection"] == "":
                    log.error(
                        "SQL Sensors requires a valid Airflow Connection ID in column 'sensor_connection'"
                    )
                else:
                    # 					self.DAGfile.close()
                    # 					self.common_config.remove_temporary_files()
                    # 					sys.exit(1)

                    sensor_poke_interval = row["sensor_poke_interval"]
                    if sensor_poke_interval == "":
                        # Default to 5 minutes
                        sensor_poke_interval = 300

                    self.DAGfile.write("%s = SqlSensor(\n" % (row["task_name"]))
                    self.DAGfile.write("    task_id='%s',\n" % (row["task_name"]))
                    self.DAGfile.write(
                        "    conn_id='%s',\n" % (row["sensor_connection"])
                    )
                    self.DAGfile.write('    sql="""%s""",\n' % (row["task_config"]))
                    if row["airflow_pool"] != "":
                        self.DAGfile.write("    pool='%s',\n" % (row["airflow_pool"]))
                    if row["airflow_priority"] != "":
                        self.DAGfile.write(
                            "    priority_weight=%s,\n" % (int(row["airflow_priority"]))
                        )
                        self.DAGfile.write("    weight_rule='absolute',\n")
                    else:
                        self.DAGfile.write("    priority_weight=100,\n")
                        self.DAGfile.write("    weight_rule='absolute',\n")
                    self.DAGfile.write(
                        "    timeout=%s,\n" % (int(sensor_timeout_seconds))
                    )
                    self.DAGfile.write(
                        "    poke_interval=%s,\n" % (int(sensor_poke_interval))
                    )
                    self.DAGfile.write("    mode='reschedule',\n")
                    if (
                        row["sensor_soft_fail"] is not None
                        and row["sensor_soft_fail"] == 1
                    ):
                        self.DAGfile.write("    soft_fail=True,\n")
                    self.DAGfile.write("    dag=dag)\n")
                    self.DAGfile.write("\n")

            if row["task_type"] == "Trigger DAG":
                self.DAGfile.write("%s = TriggerDagRunOperator(\n" % (row["task_name"]))
                self.DAGfile.write("    task_id='%s',\n" % (row["task_name"]))
                self.DAGfile.write("    trigger_dag_id='%s',\n" % (row["task_config"]))
                if row["airflow_pool"] != "":
                    self.DAGfile.write("    pool='%s',\n" % (row["airflow_pool"]))
                if row["airflow_priority"] != "":
                    self.DAGfile.write(
                        "    priority_weight=%s,\n" % (int(row["airflow_priority"]))
                    )
                    self.DAGfile.write("    weight_rule='absolute',\n")
                else:
                    self.DAGfile.write("    priority_weight=100,\n")
                    self.DAGfile.write("    weight_rule='absolute',\n")
                if self.airflowMajorVersion == 1:
                    self.DAGfile.write("    python_callable=always_trigger,\n")
                self.DAGfile.write("    dag=dag)\n")
                self.DAGfile.write("\n")

            if row["task_type"] == "shell script":
                self.DAGfile.write("%s = BashOperator(\n" % (row["task_name"]))
                self.DAGfile.write("    task_id='%s',\n" % (row["task_name"]))
                self.DAGfile.write("    retries=%s,\n" % (defaultRetries))
                self.DAGfile.write("    bash_command='%s ',\n" % (row["task_config"]))
                if row["airflow_pool"] != "":
                    self.DAGfile.write("    pool='%s',\n" % (row["airflow_pool"]))
                if row["airflow_priority"] != "":
                    self.DAGfile.write(
                        "    priority_weight=%s,\n" % (int(row["airflow_priority"]))
                    )
                    self.DAGfile.write("    weight_rule='absolute',\n")
                else:
                    self.DAGfile.write("    priority_weight=1,\n")
                    self.DAGfile.write("    weight_rule='absolute',\n")
                self.DAGfile.write("    dag=dag)\n")
                self.DAGfile.write("\n")

            if row["task_type"] == "Hive SQL Script":
                self.DAGfile.write("%s = BashOperator(\n" % (row["task_name"]))
                self.DAGfile.write("    task_id='%s',\n" % (row["task_name"]))
                self.DAGfile.write("    retries=%s,\n" % (defaultRetries))

                if row["hive_db"] != None and row["hive_db"].strip() != "":
                    self.DAGfile.write(
                        "    bash_command='%sbin/manage --runHiveScript=%s --hiveDB=%s ',\n"
                        % (
                            self.getDBImportCommandPath(sudoUser=sudo_user),
                            row["task_config"],
                            row["hive_db"],
                        )
                    )
                else:
                    self.DAGfile.write(
                        "    bash_command='%sbin/manage --runHiveScript=%s ',\n"
                        % (
                            self.getDBImportCommandPath(sudoUser=sudo_user),
                            row["task_config"],
                        )
                    )

                if row["airflow_pool"] != "":
                    self.DAGfile.write("    pool='%s',\n" % (row["airflow_pool"]))
                if row["airflow_priority"] != "":
                    self.DAGfile.write(
                        "    priority_weight=%s,\n" % (int(row["airflow_priority"]))
                    )
                    self.DAGfile.write("    weight_rule='absolute',\n")
                else:
                    self.DAGfile.write("    priority_weight=1,\n")
                    self.DAGfile.write("    weight_rule='absolute',\n")
                self.DAGfile.write("    dag=dag)\n")
                self.DAGfile.write("\n")

            if row["task_type"] == "Hive SQL":
                jdbc_sql = row["task_config"].replace(r"'", "\\'")
                self.DAGfile.write("%s = BashOperator(\n" % (row["task_name"]))
                self.DAGfile.write("    task_id='%s',\n" % (row["task_name"]))
                self.DAGfile.write("    retries=%s,\n" % (defaultRetries))
                self.DAGfile.write(
                    "    bash_command='%sbin/manage --runHiveQuery=\"%s\" ',\n"
                    % (self.getDBImportCommandPath(sudoUser=sudo_user), jdbc_sql)
                )
                if row["airflow_pool"] != "":
                    self.DAGfile.write("    pool='%s',\n" % (row["airflow_pool"]))
                if row["airflow_priority"] != "":
                    self.DAGfile.write(
                        "    priority_weight=%s,\n" % (int(row["airflow_priority"]))
                    )
                    self.DAGfile.write("    weight_rule='absolute',\n")
                else:
                    self.DAGfile.write("    priority_weight=1,\n")
                    self.DAGfile.write("    weight_rule='absolute',\n")
                self.DAGfile.write("    dag=dag)\n")
                self.DAGfile.write("\n")

            if row["task_type"] == "JDBC SQL":
                jdbc_sql = row["task_config"].replace(r"'", "\\'")
                self.DAGfile.write("%s = BashOperator(\n" % (row["task_name"]))
                self.DAGfile.write("    task_id='%s',\n" % (row["task_name"]))
                self.DAGfile.write("    retries=%s,\n" % (defaultRetries))
                self.DAGfile.write(
                    "    bash_command='%sbin/manage --dbAlias=%s --runJDBCQuery=\"%s\" ',\n"
                    % (
                        self.getDBImportCommandPath(sudoUser=sudo_user),
                        row["jdbc_dbalias"],
                        jdbc_sql,
                    )
                )
                if row["airflow_pool"] != "":
                    self.DAGfile.write("    pool='%s',\n" % (row["airflow_pool"]))
                if row["airflow_priority"] != "":
                    self.DAGfile.write(
                        "    priority_weight=%s,\n" % (int(row["airflow_priority"]))
                    )
                    self.DAGfile.write("    weight_rule='absolute',\n")
                else:
                    self.DAGfile.write("    priority_weight=1,\n")
                    self.DAGfile.write("    weight_rule='absolute',\n")
                self.DAGfile.write("    dag=dag)\n")
                self.DAGfile.write("\n")

            if row["task_type"] == "DBImport command":
                dbimport_command = row["task_config"].replace(r"'", "\\'")
                if self.airflowMode == "default":
                    self.DAGfile.write("%s = BashOperator(\n" % (row["task_name"]))
                    self.DAGfile.write("    task_id='%s',\n" % (row["task_name"]))
                    self.DAGfile.write("    retries=%s,\n" % (defaultRetries))
                    self.DAGfile.write(
                        "    bash_command='%sbin/%s',\n"
                        % (
                            self.getDBImportCommandPath(sudoUser=sudo_user),
                            dbimport_command,
                        )
                    )
                    if row["airflow_pool"] != "":
                        self.DAGfile.write("    pool='%s',\n" % (row["airflow_pool"]))
                    if row["airflow_priority"] != "":
                        self.DAGfile.write(
                            "    priority_weight=%s,\n" % (int(row["airflow_priority"]))
                        )
                        self.DAGfile.write("    weight_rule='absolute',\n")
                    else:
                        self.DAGfile.write("    priority_weight=1,\n")
                        self.DAGfile.write("    weight_rule='absolute',\n")
                    self.DAGfile.write("    dag=dag)\n")
                    self.DAGfile.write("\n")
                elif self.airflowMode == "aws_mwaa":
                    instance_id = self.getAWSInstanceID(row["task_name"])
                    self.DAGfile.write("%s = PythonOperator(\n" % (row["task_name"]))
                    self.DAGfile.write("    task_id='%s',\n" % (row["task_name"]))
                    self.DAGfile.write("    retries=%s,\n" % (defaultRetries))
                    self.DAGfile.write("    python_callable=startDBImportExecution,\n")
                    self.DAGfile.write(
                        '    op_kwargs={"command": "%s", "instanceID": "%s"},\n'
                        % (dbimport_command, instance_id)
                    )
                    if row["airflow_pool"] != "":
                        self.DAGfile.write("    pool='%s',\n" % (row["airflow_pool"]))
                    else:
                        if self.airflowAWSpoolToInstanceId == True:
                            self.DAGfile.write("    pool='%s',\n" % (instance_id))
                        else:
                            self.DAGfile.write("    pool='%s',\n" % (dagName))
                    if row["airflow_priority"] != "":
                        self.DAGfile.write(
                            "    priority_weight=%s,\n" % (int(row["airflow_priority"]))
                        )
                        self.DAGfile.write("    weight_rule='absolute',\n")
                    else:
                        self.DAGfile.write("    priority_weight=1,\n")
                        self.DAGfile.write("    weight_rule='absolute',\n")
                    self.DAGfile.write("    dag=dag)\n")
                    self.DAGfile.write("\n")

            if row["placement"] == "before main":
                temp_start_task = self.preStartTask
                temp_stop_task = self.preStopTask

            if row["placement"] == "in main":
                temp_start_task = self.mainStartTask
                temp_stop_task = self.mainStopTask

            if row["placement"] == "after main":
                temp_start_task = self.postStartTask
                temp_stop_task = self.postStopTask

            for dep_index, dep_row in all_task_dependencies.iterrows():
                if (
                    row["task_name"] == dep_row["task_dependency_upstream"]
                    or row["task_name"] == dep_row["task_dependency_downstream"]
                ) and row["placement"] != dep_row["placement"]:
                    if task_dependency_warning_printed == False:
                        log.warning("Issue on DAG '%s'" % (dagName))
                        log.warning(
                            "There are custom tasks that have dependencies between 'in_main' and 'after_main'. This is not supported and the dependencies will be incorrect in Airflow"
                        )
                        task_dependency_warning_printed = True

            # if row['placement'] == "in main" or row['placement'] == "after main":
            if 1 == 1:
                # 				if row['task_dependency_in_main_downstream'] == '':
                # 					# No dependency, set to "mainStart" task, what ever that might be
                # 					taskDependencies += "%s.set_downstream(%s)\n"%(self.mainStartTask, row['task_name'])
                # 				else:
                # 					# Check if there is any dependencies for this task
                # 					for dep in row['task_dependency_in_main_downstream'].split(','):
                # 						dep = dep.strip()
                # 						if dep == "main_start":
                # 							dep = self.mainStartTask
                #
                # 						taskDependencies += "%s.set_downstream(%s)\n"%(dep, row['task_name'])

                down_stream_found = False
                up_stream_found = False

                for dep_index, dep_row in all_task_dependencies.iterrows():
                    for dep_on_dep in dep_row["task_dependency_downstream"].split(","):
                        # depOnDep contains that tasks dependencies and we need to check if there is a dependency against the task we are working on
                        dep_on_dep = dep_on_dep.strip()
                        if dep_row["placement"] == row["placement"]:
                            # Only do this if both placements are the same
                            if row["task_name"] == dep_on_dep:
                                # There is a match that means there is a downstrem dependency for another ask agains the current task
                                up_stream_found = True

                    for dep_on_dep in dep_row["task_dependency_upstream"].split(","):
                        # depOnDep contains that tasks dependencies and we need to check if there is a dependency against the task we are working on
                        dep_on_dep = dep_on_dep.strip()
                        if dep_row["placement"] == row["placement"]:
                            # Only do this if both placements are the same
                            if row["task_name"] == dep_on_dep:
                                # There is a match that means there is a downstrem dependency for another ask agains the current task
                                down_stream_found = True

                for dep in row["task_dependency_downstream"].split(","):
                    dep = dep.strip()
                    if dep == "":
                        continue

                    # Get the task that this have a depenceny against and check if it's in the same placement
                    dep_row = all_task_dependencies.loc[
                        all_task_dependencies["task_name"] == dep, ["placement"]
                    ]
                    if dep_row.empty == False:
                        dep_row_value = dep_row.values[0][0]
                        if dep_row.values[0][0] == row["placement"]:
                            # Only do this if both placements are the same
                            task_dependencies += "%s.set_downstream(%s)\n" % (
                                row["task_name"],
                                dep,
                            )
                            down_stream_found = True
                        else:
                            log.warning(
                                "Downstream dependency against '%s' wont work as it's not in the same placement as '%s'."
                                % (dep, row["task_name"])
                            )
                    else:
                        # The depRow dataframe is empty. So the dependent task is not a customTask, but instead from import_tables or export_tables. So we just add it as a dependency
                        task_dependencies += "%s.set_downstream(%s)\n" % (
                            row["task_name"],
                            dep,
                        )
                        down_stream_found = True

                for dep in row["task_dependency_upstream"].split(","):
                    dep = dep.strip()
                    if dep == "":
                        continue

                    # Get the task that this have a depenceny against and check if it's in the same placement
                    dep_row = all_task_dependencies.loc[
                        all_task_dependencies["task_name"] == dep, ["placement"]
                    ]
                    if dep_row.empty == False:
                        dep_row_value = dep_row.values[0][0]
                        if dep_row.values[0][0] == row["placement"]:
                            # Only do this if both placements are the same
                            task_dependencies += "%s.set_upstream(%s)\n" % (
                                row["task_name"],
                                dep,
                            )
                            up_stream_found = True
                        else:
                            log.warning(
                                "Upstream dependency against '%s' wont work as it's not in the same placement as '%s'."
                                % (dep, row["task_name"])
                            )
                    else:
                        # The depRow dataframe is empty. So the dependent task is not a customTask, but instead from import_tables or export_tables. So we just add it as a dependency
                        task_dependencies += "%s.set_upstream(%s)\n" % (
                            row["task_name"],
                            dep,
                        )
                        up_stream_found = True

                if up_stream_found == False:
                    task_dependencies += "%s.set_downstream(%s)\n" % (
                        temp_start_task,
                        row["task_name"],
                    )

                if down_stream_found == False:
                    task_dependencies += "%s.set_downstream(%s)\n" % (
                        row["task_name"],
                        temp_stop_task,
                    )

                # Check if other tasks have this task as upstream or downstream
                # 				for depIndex, depRow in allTaskDependencies.iterrows():
                # 					pass

                # 				if row['task_dependency_in_main_upstream'] == '':
                # 					# No dependency, set to "mainStart" task, what ever that might be
                # 					# taskDependencies += "%s.set_upstream(%s)\n"%(row['task_name'], self.mainStartTask)
                # 					taskDependencies += "%s.set_upstream(%s)\n"%(row['task_name'], tempStartTask)
                # 				else:
                # 					# Check if there is any dependencies for this task
                # 					for dep in row['task_dependency_in_main_upstream'].split(','):
                # 						dep = dep.strip()
                # 						taskDependencies += "%s.set_upstream(%s)\n"%(row['task_name'], dep)
                #
                # 				if row['task_dependency_in_main_downstream'] != '':
                # 					for dep in row['task_dependency_in_main_downstream'].split(','):
                # 						dep = dep.strip()
                # 						taskDependencies += "%s.set_downstream(%s)\n"%(row['task_name'], dep)

                # We also need to check if there is any dependencies on this task. If there is, we dont add a set_downstream to the stop taks as that will be handled by the other task
                # 				foundDependency = False
                # 				for dep in allTaskDependenciesUpstream.values:
                # 					for depTask in dep[0].split(','):
                # 						depTask = depTask.strip()
                # 						if depTask != '' and depTask == row['task_name']:
                # 							foundDependency = True
                #
                # 				if foundDependency == False and row['task_dependency_downstream'] == '':
                # 					# taskDependencies += "%s.set_downstream(%s)\n"%(row['task_name'], self.mainStopTask)
                # 					taskDependencies += "%s.set_downstream(%s)\n"%(row['task_name'], tempStopTask)

                task_dependencies += "\n"

        self.DAGfile.write(task_dependencies)
        session.close()

    def addSensorsToDAGfile(self, dagName, mainDagSchedule):
        log = logging.getLogger(self.logger)
        # session = self.configDBSession()
        session = self.getDBImportSession()

        airflow_dag_sensors = aliased(configSchema.airflowDagSensors, name="ads")
        airflow_custom_dags = aliased(configSchema.airflowCustomDags, name="acd")
        airflow_etl_dags = aliased(configSchema.airflowEtlDags, name="aetld")
        airflow_export_dags = aliased(configSchema.airflowExportDags, name="aed")
        airflow_import_dags = aliased(configSchema.airflowImportDags, name="aid")

        sensors = pd.DataFrame(
            session.query(
                airflow_dag_sensors.dag_name,
                airflow_dag_sensors.sensor_name,
                airflow_dag_sensors.wait_for_dag,
                airflow_dag_sensors.wait_for_task,
                airflow_dag_sensors.timeout_minutes,
                airflow_dag_sensors.sensor_soft_fail,
                airflow_import_dags.schedule_interval.label("import_schedule"),
                airflow_export_dags.schedule_interval.label("export_schedule"),
                airflow_custom_dags.schedule_interval.label("custom_schedule"),
                airflow_etl_dags.schedule_interval.label("etl_schedule"),
            )
            .select_from(airflow_dag_sensors)
            .join(
                airflow_import_dags,
                airflow_dag_sensors.wait_for_dag == airflow_import_dags.dag_name,
                isouter=True,
            )
            .join(
                airflow_export_dags,
                airflow_dag_sensors.wait_for_dag == airflow_export_dags.dag_name,
                isouter=True,
            )
            .join(
                airflow_custom_dags,
                airflow_dag_sensors.wait_for_dag == airflow_custom_dags.dag_name,
                isouter=True,
            )
            .join(
                airflow_etl_dags,
                airflow_dag_sensors.wait_for_dag == airflow_etl_dags.dag_name,
                isouter=True,
            )
            .filter(airflow_dag_sensors.dag_name == dagName)
            .all()
        ).fillna("")

        for index, row in sensors.iterrows():
            wait_dag_schedule = ""
            if row["import_schedule"] != "":
                wait_dag_schedule = row["import_schedule"]
            elif row["export_schedule"] != "":
                wait_dag_schedule = row["export_schedule"]
            elif row["custom_schedule"] != "":
                wait_dag_schedule = row["custom_schedule"]
            elif row["etl_schedule"] != "":
                wait_dag_schedule = row["etl_schedule"]

            if wait_dag_schedule == "":
                log.warning("Issue on DAG '%s'" % (dagName))
                log.warning("There is no schedule interval for DAG to wait for")
                log.warning(
                    "The DAG Sensor will not be added to the DAG. Instead, there will be a Task that always failes. The DAG will never execute"
                )
                self.addFailureTask("%s_FAILURE" % (row["sensor_name"]))
                self.DAGfile.write(
                    "%s.set_downstream(%s_FAILURE)\n"
                    % (self.sensorStartTask, row["sensor_name"])
                )
                self.DAGfile.write(
                    "%s_FAILURE.set_downstream(%s)\n"
                    % (row["sensor_name"], self.sensorStopTask)
                )
                self.DAGfile.write("\n")
                continue
            # 				log.error("Cant find schedule interval for DAG to wait for")
            # 				self.DAGfile.close()
            # 				self.common_config.remove_temporary_files()
            # 				sys.exit(1)

            wait_for_task = row["wait_for_task"]
            if wait_for_task == "":
                wait_for_task = "stop"

            timeout_seconds = row["timeout_minutes"]
            if timeout_seconds == "":
                # Default to 5 hours
                timeout_seconds = "18000"
            else:
                timeout_seconds = timeout_seconds * 60

            main_dag_match_hour_min = re.search(
                "^[0-2][0-9]:[0-5][0-9]$", mainDagSchedule
            )
            wait_dag_match_hour_min = re.search(
                "^[0-2][0-9]:[0-5][0-9]$", wait_dag_schedule
            )

            # 			print(mainDagSchedule)
            # 			print(waitDagSchedule)
            # 			print(mainDagMatchHourMin)
            # 			print(waitDagMatchHourMin)

            time_diff = "0"

            if (
                main_dag_match_hour_min == None and wait_dag_match_hour_min != None
            ) or (main_dag_match_hour_min != None and wait_dag_match_hour_min == None):
                log.warning("Issue on DAG '%s'" % (dagName))
                log.warning(
                    "Both the current DAG and the DAG the sensor is waiting for must have the same scheduling format (HH:MM or cron)"
                )
                log.warning(
                    "The DAG Sensor will not be added to the DAG. Instead, there will be a Task that always failes. The DAG will never execute"
                )
                self.addFailureTask("%s_FAILURE" % (row["sensor_name"]))
                self.DAGfile.write(
                    "%s.set_downstream(%s_FAILURE)\n"
                    % (self.sensorStartTask, row["sensor_name"])
                )
                self.DAGfile.write(
                    "%s_FAILURE.set_downstream(%s)\n"
                    % (row["sensor_name"], self.sensorStopTask)
                )
                self.DAGfile.write("\n")
                continue
            # 				self.DAGfile.close()
            # 				self.common_config.remove_temporary_files()
            # 				sys.exit(1)

            if main_dag_match_hour_min != None:
                # Time is in HH:MM format for both DAG's. So now we can calculate the diff in time between them
                noon = datetime.strptime("12:00", "%H:%M")
                main_dag_schedule_date_time = datetime.strptime(
                    mainDagSchedule, "%H:%M"
                )
                wait_dag_schedule_date_time = datetime.strptime(
                    wait_dag_schedule, "%H:%M"
                )

                # Add or remove half a day so we dont calculate over midnight.
                if main_dag_schedule_date_time > noon:
                    main_dag_schedule_date_time = (
                        main_dag_schedule_date_time - timedelta(seconds=43200)
                    )
                else:
                    main_dag_schedule_date_time = (
                        main_dag_schedule_date_time + timedelta(seconds=43200)
                    )

                if wait_dag_schedule_date_time > noon:
                    wait_dag_schedule_date_time = (
                        wait_dag_schedule_date_time - timedelta(seconds=43200)
                    )
                else:
                    wait_dag_schedule_date_time = (
                        wait_dag_schedule_date_time + timedelta(seconds=43200)
                    )

                # 				print (mainDagScheduleDateTime)
                # 				print (waitDagScheduleDateTime)

                if main_dag_schedule_date_time > wait_dag_schedule_date_time:
                    time_diff = (
                        main_dag_schedule_date_time - wait_dag_schedule_date_time
                    )
                    minus_text = ""
                else:
                    time_diff = (
                        wait_dag_schedule_date_time - main_dag_schedule_date_time
                    )
                    minus_text = "-"

                time_diff = str(minus_text + str(time_diff.seconds))
            # 				print (timeDiff)
            else:
                if mainDagSchedule != wait_dag_schedule:
                    log.warning("Issue on DAG '%s'" % (dagName))
                    log.warning(
                        "When using cron or cron alias schedules for DAG sensors, the schedule time in both DAG's must match"
                    )
                    log.warning(
                        "The DAG Sensor will not be added to the DAG. Instead, there will be a Task that always failes. The DAG will never execute"
                    )
                    self.addFailureTask("%s_FAILURE" % (row["sensor_name"]))
                    self.DAGfile.write(
                        "%s.set_downstream(%s_FAILURE)\n"
                        % (self.sensorStartTask, row["sensor_name"])
                    )
                    self.DAGfile.write(
                        "%s_FAILURE.set_downstream(%s)\n"
                        % (row["sensor_name"], self.sensorStopTask)
                    )
                    self.DAGfile.write("\n")
                    continue
            # 					self.DAGfile.close()
            # 					self.common_config.remove_temporary_files()
            # 					sys.exit(1)

            self.DAGfile.write("%s = ExternalTaskSensor(\n" % (row["sensor_name"]))
            self.DAGfile.write("    task_id='%s',\n" % (row["sensor_name"]))
            self.DAGfile.write("    external_dag_id='%s',\n" % (row["wait_for_dag"]))
            self.DAGfile.write("    external_task_id='%s',\n" % (wait_for_task))
            self.DAGfile.write("    retries=0,\n")
            # 			self.DAGfile.write("    execution_timeout=timedelta(seconds=%s),\n"%(timeoutSeconds))
            self.DAGfile.write(
                "    execution_delta=timedelta(seconds=%s),\n" % (time_diff)
            )
            self.DAGfile.write("    timeout=%s,\n" % (int(timeout_seconds)))
            self.DAGfile.write("    poke_interval=300,\n")
            self.DAGfile.write("    mode='reschedule',\n")
            if row["sensor_soft_fail"] is not None and row["sensor_soft_fail"] == 1:
                self.DAGfile.write("    soft_fail=True,\n")
            self.DAGfile.write("    dag=dag)\n")
            self.DAGfile.write("\n")
            self.DAGfile.write(
                "%s.set_downstream(%s)\n" % (self.sensorStartTask, row["sensor_name"])
            )
            self.DAGfile.write(
                "%s.set_downstream(%s)\n" % (row["sensor_name"], self.sensorStopTask)
            )
            self.DAGfile.write("\n")

        session.close()

    def sendJSON(self, airflowDAG, status):
        """Sends a start JSON document to REST and/or Kafka"""
        log = logging.getLogger(self.logger)
        log.debug("Executing Airflow.sendStartJSON()")

        self.postAirflowDAGoperations = self.common_config.getConfigValue(
            key="post_airflow_dag_operations"
        )
        self.postDataToREST = self.common_config.getConfigValue(key="post_data_to_rest")
        self.postDataToKafka = self.common_config.getConfigValue(
            key="post_data_to_kafka"
        )

        # 		self.postDataToREST = True

        if airflowDAG == None:
            return

        if self.postAirflowDAGoperations == False:
            log.info(
                "Airflow DAG post to Kafka and/or Rest have been disabled. No message will be sent"
            )
            return

        json_data = {}
        json_data["type"] = "airflow_dag"
        json_data["status"] = status
        json_data["dag"] = airflowDAG
        json_data["airflow_timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")

        if self.postDataToKafka == True:
            result = self.sendStatistics.publishKafkaData(json.dumps(json_data))
            if result == False:
                log.warning("Kafka publish failed! No start message posted")

        if self.postDataToREST == True:
            response = self.sendStatistics.sendRESTdata(json.dumps(json_data))
            if response != 200:
                log.warn("REST call failed! No start message posted")

        log.debug("Executing Airflow.sendStartJSON() - Finished")
