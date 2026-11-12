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

from common.Exceptions import InvalidConfigurationError, SQLError
from ConfigReader import configuration
from DBImportConfig import common_config, configSchema, sendStatistics
from Schedule import airflowSchema


class initialize:
    def __init__(self):
        logging.debug("Executing Airflow.__init__()")

        self.mysql_conn = None
        self.mysql_cursor = None
        self.debug_log_level = False

        self.logger = "root"
        log = logging.getLogger(self.logger)

        self.config_db_session = None
        self.config_db = None

        if logging.root.level == 10:  # DEBUG
            self.debug_log_level = True

        self.common_config = common_config.config()
        self.send_statistics = sendStatistics.sendStatistics()

        self.dbimport_command_path = self.get_config_value(
            "airflow_dbimport_commandpath"
        )
        self.default_sudo_user = self.get_config_value("airflow_sudo_user")
        self.dag_directory = self.get_config_value("airflow_dag_directory")
        self.dag_staging_directory = self.get_config_value(
            "airflow_dag_staging_directory"
        )
        self.dag_file_group = self.get_config_value("airflow_dag_file_group")
        self.dag_file_permission = self.get_config_value("airflow_dag_file_permission")
        self.task_queue_for_dummy = self.get_config_value("airflow_dummy_task_queue")
        self.airflow_major_version = self.get_config_value("airflow_major_version")
        self.default_time_zone = self.get_config_value("timezone")
        self.airflow_aws_instance_ids = self.get_config_value("airflow_aws_instanceids")
        self.airflow_aws_pool_to_instance_id = self.get_config_value(
            "airflow_aws_pool_to_instanceid"
        )
        self.airflow_default_pool_size = self.get_config_value(
            "airflow_default_pool_size"
        )
        self.create_pools_with_tasks = self.get_config_value(
            "airflow_create_pool_with_task"
        )

        self.dag_file = None
        self.dag_filename = None
        self.dag_filename_in_airflow = None
        self.dag_filename_in_aws_mwaa = None
        self.write_dag = None

        self.sensor_start_task = None
        self.sensor_stop_task = None
        self.pre_start_task = None
        self.pre_stop_task = None
        self.main_start_task = None
        self.main_stop_task = None
        self.post_start_task = None
        self.post_stop_task = None

        self.airflow_mode = "default"

        self.created_pools = []
        self.aws_instance_id_assigned_pools = {}

        try:
            self.airflow_mode = configuration.get(
                "Airflow", "airflow_mode", exit_on_error=False
            )
            if self.airflow_mode != "default" and self.airflow_mode != "aws_mwaa":
                raise InvalidConfigurationError(
                    "Invalid option specified for [Airflow][airflow_mode] in the configuration file."
                )
        # 				log.error("Invalid option specified for [Airflow][airflow_mode] in the configuration file.")
        # 				self.common_config.remove_temporary_files()
        # 				sys.exit(1)

        except InvalidConfigurationError:
            pass

        if self.airflow_mode == "default" and self.create_pools_with_tasks == False:
            # Establish a SQLAlchemy connection to the Airflow database
            airflow_connect_str = configuration.get("Airflow", "airflow_alchemy_conn")
            try:
                self.airflow_db = sa.create_engine(
                    airflow_connect_str, echo=self.debug_log_level
                )
                self.airflow_db.connect()
                self.airflow_db_session = sessionmaker(bind=self.airflow_db)

            except sa.exc.OperationalError as err:
                raise SQLError(err)
            # 				log.error("%s"%err)
            # 				self.common_config.remove_temporary_files()
            # 				sys.exit(1)
            except:
                raise SQLError(sys.exc_info())
        # 				print("Unexpected error: ")
        # 				print(sys.exc_info())
        # 				self.common_config.remove_temporary_files()
        # 				sys.exit(1)

        log.debug("Executing Airflow.__init__() - Finished")

    def disconnect_dbimport_db(self):
        """Disconnects from the database and removes all sessions and engine"""
        log = logging.getLogger(self.logger)

        if self.config_db != None:
            log.info("Disconnecting from DBImport database")
            self.config_db.dispose()
            self.config_db = None

        self.config_db_session = None

    def get_dbimport_session(self):
        """Connects to the configuration database with SQLAlchemy and return a session"""
        log = logging.getLogger(self.logger)

        if self.config_db_session != None:
            # If we already have a connection, we just return the session
            # return self.config_db_session()
            return scoped_session(self.config_db_session)

        mysql_credentials = self.common_config.get_mysql_credentials()

        self.connect_str = f"mysql+pymysql://{mysql_credentials['mysql_username']}:{quote(mysql_credentials['mysql_password'], safe=' +')}@{mysql_credentials['mysql_hostname']}:{mysql_credentials['mysql_port']}/{mysql_credentials['mysql_database']}"

        try:
            self.config_db = sa.create_engine(
                self.connect_str, echo=self.debug_log_level, pool_pre_ping=True
            )
            self.config_db.connect()
            self.config_db_session = sessionmaker(bind=self.config_db)

        except sa.exc.OperationalError as err:
            log.error("%s", err)
            self.config_db_session = None
            self.config_db = None
            raise SQLError("Can't connect to DBImport database")

        except:
            print("Unexpected error: ")
            print(sys.exc_info())
            self.config_db_session = None
            self.config_db = None
            raise SQLError("Can't connect to DBImport database")

        else:
            # return self.config_db_session()
            return scoped_session(self.config_db_session)

    def get_config_value(self, key):
        """Returns a value from the configuration table based on the supplied key. Value returned can be Int, Str or DateTime"""
        log = logging.getLogger(self.logger)

        return_value = None
        bool_value = False

        value_column, bool_value = self.common_config.get_config_value_column(key)

        try:
            session = self.get_dbimport_session()
        except SQLError:
            self.disconnect_dbimport_db()
            return None

        configuration_table = aliased(configSchema.Configuration)

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
                        "Configuration Key '%s' must have a value in '%s'",
                        key,
                        value_column,
                    )
                    return_value = None

            if bool_value:
                if return_value == 1:
                    return_value = True
                elif return_value == 0:
                    return_value = False
                else:
                    log.error(
                        "Configuration Key '%s' can only have 0 or 1 in column '%s'",
                        key,
                        value_column,
                    )
                    return_value = None
        except TypeError:
            log.error(
                "Configuration key '%s' cant be found. Have you upgraded the database schema to the latest version?"
                % (key)
            )
            return_value = None

        return return_value

    def check_execution(self):
        """Checks the 'airflow_disable' settings and exit with 0 or 1 depending on that"""

        airflow_execution_disabled = self.get_config_value("airflow_disable")
        if airflow_execution_disabled == False:
            print("Airflow execution is enabled")
            self.common_config.remove_temporary_files()
            sys.exit(0)
        else:
            print("Airflow execution is disabled")
            self.common_config.remove_temporary_files()
            sys.exit(1)

    def get_dbimport_command_path(self, sudo_user=""):

        if sudo_user == None or sudo_user == "":
            sudo_user = self.default_sudo_user

        return self.dbimport_command_path.replace("${SUDO_USER}", sudo_user)

    def generate_dag_from_rest(self, name, current_user):
        log = logging.getLogger(self.logger)

        try:
            self.generate_dag(name=name, write_dag=True)
        except (InvalidConfigurationError, SQLError, RuntimeError) as errMsg:
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

    def generate_dag(
        self, name=None, write_dag=False, auto_dag_only=False, dag_folder=None
    ):
        log = logging.getLogger(self.logger)

        self.write_dag = write_dag
        self.dag_folder = dag_folder

        # session = self.config_db_session()
        session = self.get_dbimport_session()
        airflow_custom_dags = aliased(configSchema.AirflowCustomDAGs)
        airflow_export_dags = aliased(configSchema.AirflowExportDAGs)
        airflow_import_dags = aliased(configSchema.AirflowImportDAGs)
        airflow_etl_dags = aliased(configSchema.AirflowETLDAGs)

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

        if auto_dag_only == True:
            if import_dag.empty == False:
                import_dag = import_dag.loc[import_dag["auto_regenerate_dag"] == 1]
            if export_dag.empty == False:
                export_dag = export_dag.loc[export_dag["auto_regenerate_dag"] == 1]
            if custom_dag.empty == False:
                custom_dag = custom_dag.loc[custom_dag["auto_regenerate_dag"] == 1]

        dag_found = False

        if name == None or len(import_dag) > 0:
            dag_found = True
            for _index, row in import_dag.iterrows():
                self.generate_import_dag(dag=row)

        if name == None or len(export_dag) > 0:
            dag_found = True
            for _index, row in export_dag.iterrows():
                self.generate_export_dag(dag=row)

        if name == None or len(custom_dag) > 0:
            dag_found = True
            for _index, row in custom_dag.iterrows():
                self.generate_custom_dag(dag=row)

        if not dag_found and name != None:
            raise InvalidConfigurationError("Can't find DAG with that name")
        # 			log.error("Can't find DAG with that name")
        # 			self.common_config.remove_temporary_files()
        # 			sys.exit(1)

        session.close()

    def get_airflow_host_pool_name(self):
        log = logging.getLogger(self.logger)

        if (
            self.airflow_mode == "aws_mwaa"
            and self.airflow_aws_pool_to_instance_id == True
        ):
            # We can choice to override the poolname and set it to the instance ID if we are running on AWS
            pool_name = self.airflow_aws_instance_ids
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

    def aws_append_instance_id_as_pools(self):
        # This will split the instanceID configration setting based on , and append them all as pool names
        log = logging.getLogger(self.logger)

        used_pools = []

        for aws_pool in self.airflow_aws_instance_ids.split(","):
            used_pools.append(aws_pool.strip())

        self.create_airflow_pools(pools=used_pools)

    def get_aws_instance_id(self, config_object):
        # Return one instanceID from a comma seperated list.
        log = logging.getLogger(self.logger)

        if config_object in self.aws_instance_id_assigned_pools:
            return self.aws_instance_id_assigned_pools[config_object]
        else:
            random_instance_id = random.choice(
                self.airflow_aws_instance_ids.strip().split(",")
            )
            self.aws_instance_id_assigned_pools[config_object] = random_instance_id
            return random_instance_id

        # return random.choice(self.airflow_aws_instance_ids.strip().split(','))

    def generate_export_dag(self, dag):
        """Generates a Import DAG"""
        log = logging.getLogger(self.logger)

        used_pools = []
        table_filters = []
        default_pool = dag["dag_name"]
        sudo_user = dag["sudo_user"]
        used_pools.append(default_pool)

        cron_schedule = self.convert_time_to_cron(dag["schedule_interval"])
        self.create_dag_file_with_header(
            dag_name=dag["dag_name"],
            cron_schedule=cron_schedule,
            default_pool=default_pool,
            sudo_user=sudo_user,
            dag_time_zone=dag["timezone"],
            email=dag["email"],
            email_on_failure=dag["email_on_failure"],
            email_on_retries=dag["email_on_retries"],
            tags=dag["tags"],
            sla_warning_time=dag["sla_warning_time"],
            retry_exponential_backoff=dag["retry_exponential_backoff"],
            concurrency=dag["concurrency"],
        )

        # session = self.config_db_session()
        session = self.get_dbimport_session()
        export_tables = aliased(configSchema.ExportTables)

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

        filter_db_alias = dag["filter_dbalias"].strip().replace(r"*", "%")
        filter_target_schema = dag["filter_target_schema"].strip().replace(r"*", "%")
        filter_target_table = dag["filter_target_table"].strip().replace(r"*", "%")

        # 		if filterDBAlias == '':
        # 			log.error("'filter_dbalias' in airflow_export_dags cant be empty")
        # 			self.dag_file.close()
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

        if dag["retries"] == None or dag["retries"] == "":
            retries = 5
        else:
            retries = int(dag["retries"])

        # in 'tables' we now have all the tables that will be part of the DAG
        previous_connection_alias = ""

        for _index, row in tables.iterrows():
            if row["dbalias"] != previous_connection_alias:
                # We save the previousConnectionAlias just to avoid making lookups for every dbalias even if they are all the same
                try:
                    self.common_config.lookup_connection_alias(
                        connection_alias=row["dbalias"], decrypt_credentials=False
                    )
                    previous_connection_alias = row["dbalias"]

                except InvalidConfigurationError as errMsg:
                    log.warning(
                        "The connection alias '%s' cant be found in the configuration database"
                        % (row["dbalias"])
                    )
                    previous_connection_alias = None
                    continue

            export_pool = self.get_airflow_host_pool_name()

            # usedPools is later used to check if the pools that we just are available in Airflow
            if export_pool not in used_pools:
                used_pools.append(export_pool)

            task_id = row["target_table"].replace(r"/", "_").replace(r".", "_")
            dbexport_cmd = "%sbin/export" % (
                self.get_dbimport_command_path(sudo_user=sudo_user)
            )
            dbexport_clear_stage_cmd = "%sbin/manage --clearExportStage" % (
                self.get_dbimport_command_path(sudo_user=sudo_user)
            )

            airflow_priority = 1  # Default Airflow Priority
            if row["airflow_priority"] != None and row["airflow_priority"] != "":
                airflow_priority = int(row["airflow_priority"])
            elif row["sqoop_last_mappers"] != None and row["sqoop_last_mappers"] != "":
                airflow_priority = int(row["sqoop_last_mappers"])

            clear_stage_required = False
            if row["export_type"] == "full":
                clear_stage_required = True

            if clear_stage_required:
                self.dag_file.write("%s_clearStage = BashOperator(\n" % (task_id))
                self.dag_file.write("    task_id='%s_clearStage',\n" % (task_id))
                self.dag_file.write(
                    "    bash_command='%s -a %s -S %s -T %s ',\n"
                    % (
                        dbexport_clear_stage_cmd,
                        row["dbalias"],
                        row["target_schema"],
                        row["target_table"],
                    )
                )
                self.dag_file.write(f"    priority_weight={airflow_priority},\n")
                self.dag_file.write("    weight_rule='absolute',\n")
                self.dag_file.write(f"    retries={retries},\n")
                self.dag_file.write("    dag=dag)\n")
                self.dag_file.write("\n")

            self.dag_file.write("%s = BashOperator(\n" % (task_id))
            self.dag_file.write("    task_id='%s',\n" % (task_id))
            self.dag_file.write(
                "    bash_command='%s -a %s -S %s -T %s ',\n"
                % (
                    dbexport_cmd,
                    row["dbalias"],
                    row["target_schema"],
                    row["target_table"],
                )
            )
            self.dag_file.write("    pool='%s',\n" % (export_pool))
            self.dag_file.write(f"    priority_weight={airflow_priority},\n")
            self.dag_file.write("    weight_rule='absolute',\n")
            self.dag_file.write(f"    retries={retries},\n")
            self.dag_file.write("    dag=dag)\n")
            self.dag_file.write("\n")

            if clear_stage_required:
                self.dag_file.write(
                    "%s.set_downstream(%s_clearStage)\n"
                    % (self.main_start_task, task_id)
                )
                self.dag_file.write(
                    "%s_clearStage.set_downstream(%s)\n" % (task_id, task_id)
                )
                self.dag_file.write(
                    "%s.set_downstream(%s)\n" % (task_id, self.main_stop_task)
                )
            else:
                self.dag_file.write(
                    "%s.set_downstream(%s)\n" % (self.main_start_task, task_id)
                )
                self.dag_file.write(
                    "%s.set_downstream(%s)\n" % (task_id, self.main_stop_task)
                )
            self.dag_file.write("\n")

        self.add_tasks_to_dag_file(
            dag_name=dag["dag_name"],
            main_dag_schedule=dag["schedule_interval"],
            default_retries=retries,
            default_sudo_user=sudo_user,
        )
        self.add_sensors_to_dag_file(
            dag_name=dag["dag_name"], main_dag_schedule=dag["schedule_interval"]
        )
        self.create_airflow_pools(pools=used_pools)
        self.close_dag_file()
        session.close()

    def generate_import_dag(self, dag):
        """Generates a Import DAG"""
        log = logging.getLogger(self.logger)

        import_phase_finish_first = False
        if dag["finish_all_stage1_first"] == 1:
            import_phase_finish_first = True

        run_import_and_etl_separate = False
        if dag["run_import_and_etl_separate"] == 1:
            run_import_and_etl_separate = True

        used_pools = []
        table_filters = []
        default_pool = dag["dag_name"]
        sudo_user = dag["sudo_user"]
        used_pools.append(default_pool)

        if dag["metadata_import"] == 1:
            meta_data_import_option = "-m"
        else:
            meta_data_import_option = ""

        cron_schedule = self.convert_time_to_cron(dag["schedule_interval"])
        self.create_dag_file_with_header(
            dag_name=dag["dag_name"],
            cron_schedule=cron_schedule,
            import_phase_finish_first=import_phase_finish_first,
            default_pool=default_pool,
            sudo_user=sudo_user,
            dag_time_zone=dag["timezone"],
            email=dag["email"],
            email_on_failure=dag["email_on_failure"],
            email_on_retries=dag["email_on_retries"],
            tags=dag["tags"],
            sla_warning_time=dag["sla_warning_time"],
            retry_exponential_backoff=dag["retry_exponential_backoff"],
            concurrency=dag["concurrency"],
        )

        # session = self.config_db_session()
        session = self.get_dbimport_session()
        import_tables = aliased(configSchema.ImportTables)

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

        for hive_target in dag["filter_hive"].split(";"):
            try:
                hive_db = hive_target.split(".")[0].strip().replace(r"*", "%")
                hive_table = hive_target.split(".")[1].strip().replace(r"*", "%")
                if (
                    hive_db == None
                    or hive_table == None
                    or hive_db == ""
                    or hive_table == ""
                ):
                    self.dag_file.close()
                    raise InvalidConfigurationError(
                        "Syntax for filter_hive column is <HIVE_DB>.<HIVE_TABLE>;<HIVE_DB>.<HIVE_TABLE>;....."
                    )
            except IndexError:
                self.dag_file.close()
                raise InvalidConfigurationError(
                    "Syntax for filter_hive column is <HIVE_DB>.<HIVE_TABLE>;<HIVE_DB>.<HIVE_TABLE>;....."
                )
            # 				log.error("Syntax for filter_hive column is <HIVE_DB>.<HIVE_TABLE>;<HIVE_DB>.<HIVE_TABLE>;.....")
            # 				self.dag_file.close()
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

        retries = int(dag["retries"])
        try:
            retries_import_phase = int(dag["retries_stage1"])
        except ValueError:
            retries_import_phase = retries

        try:
            retries_etl_phase = int(dag["retries_stage2"])
        except ValueError:
            retries_etl_phase = retries

        # in 'tables' we now have all the tables that will be part of the DAG
        previous_connection_alias = ""

        for _index, row in tables.iterrows():
            try:
                if row["dbalias"] != previous_connection_alias:
                    # We save the previousConnectionAlias just to avoid making lookups for every dbalias even if they are all the same
                    self.common_config.lookup_connection_alias(
                        connection_alias=row["dbalias"], decrypt_credentials=False
                    )
                    previous_connection_alias = row["dbalias"]
            except:
                continue

            if row["copy_slave"] == 1:
                import_phase_as_sensor = True
            else:
                import_phase_as_sensor = False

            if (
                self.airflow_mode == "aws_mwaa"
                and self.airflow_aws_pool_to_instance_id == True
            ):
                # We can choice to override the poolname and set it to the instance ID if we are running on AWS
                instance_id = self.get_aws_instance_id(row["hive_table"])
                import_phase_pool = instance_id
                etl_phase_pool = instance_id
            else:
                import_phase_pool = self.get_airflow_host_pool_name()
                etl_phase_pool = dag["dag_name"][0:50]

                if dag["pool_stage1"] != "":
                    import_phase_pool = dag["pool_stage1"]

                if dag["pool_stage2"] != "":
                    etl_phase_pool = dag["pool_stage2"]

            # usedPools is later used to check if the pools that we just are available in Airflow
            if import_phase_pool not in used_pools:
                used_pools.append(import_phase_pool)

            if etl_phase_pool not in used_pools:
                used_pools.append(etl_phase_pool)

            dbimport_cmd = "%sbin/import" % (
                self.get_dbimport_command_path(sudo_user=sudo_user)
            )
            dbimport_clear_stage_cmd = "%sbin/manage --clearImportStage" % (
                self.get_dbimport_command_path(sudo_user=sudo_user)
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

            if clear_stage_required:
                if self.airflow_mode == "default":
                    self.dag_file.write("%s_clearStage = BashOperator(\n" % (task_id))
                    self.dag_file.write("    task_id='%s_clearStage',\n" % (task_id))
                    self.dag_file.write(
                        "    bash_command='%s -h %s -t %s ',\n"
                        % (dbimport_clear_stage_cmd, row["hive_db"], row["hive_table"])
                    )
                    self.dag_file.write("    pool='%s',\n" % (import_phase_pool))
                    self.dag_file.write(f"    priority_weight={airflow_priority},\n")
                    self.dag_file.write("    weight_rule='absolute',\n")
                    self.dag_file.write(f"    retries={retries},\n")
                    self.dag_file.write("    dag=dag)\n")
                    self.dag_file.write("\n")
                elif self.airflow_mode == "aws_mwaa":
                    instance_id = self.get_aws_instance_id(row["hive_table"])
                    self.dag_file.write("%s_clearStage = PythonOperator(\n" % (task_id))
                    self.dag_file.write("    task_id='%s_clearStage',\n" % (task_id))
                    self.dag_file.write("    python_callable=startDBImportExecution,\n")
                    # self.dag_file.write("    op_kwargs={\"command\": \"manage --clearImportStage -h %s -t %s\"},\n"%(row['hive_db'], row['hive_table']))
                    self.dag_file.write(
                        '    op_kwargs={"command": "manage --clearImportStage -h %s -t %s", "instanceID": "%s"},\n'
                        % (row["hive_db"], row["hive_table"], instance_id)
                    )
                    self.dag_file.write("    pool='%s',\n" % (import_phase_pool))
                    # self.dag_file.write("    pool='%s',\n"%(instanceID))
                    self.dag_file.write(f"    priority_weight={airflow_priority},\n")
                    self.dag_file.write("    weight_rule='absolute',\n")
                    self.dag_file.write(f"    retries={retries},\n")
                    self.dag_file.write("    dag=dag)\n")
                    self.dag_file.write("\n")

            if dag["finish_all_stage1_first"] == 1 or run_import_and_etl_separate:
                if import_phase_as_sensor:
                    # Running Import phase as a sensor
                    self.dag_file.write("%s_sensor = SqlSensor(\n" % (task_id))
                    self.dag_file.write("    task_id='%s_sensor',\n" % (task_id))
                    self.dag_file.write("    conn_id='DBImport',\n")
                    self.dag_file.write(
                        "    sql=\"\"\"select count(*) from import_tables where hive_db = '%s' and hive_table = '%s' and "
                        % (row["hive_db"], row["hive_table"])
                    )
                    self.dag_file.write(
                        "copy_finished >= '{{ next_execution_date.strftime('%Y-%m-%d %H:%M:%S.%f') }}'\"\"\",\n"
                    )
                    self.dag_file.write("    pool='%s',\n" % (import_phase_pool))
                    self.dag_file.write(f"    priority_weight={airflow_priority},\n")
                    self.dag_file.write("    weight_rule='absolute',\n")
                    self.dag_file.write("    timeout=18000,\n")
                    self.dag_file.write("    poke_interval=300,\n")
                    self.dag_file.write("    mode='reschedule',\n")
                    self.dag_file.write("    dag=dag)\n")
                    self.dag_file.write("\n")

                if self.airflow_mode == "default":
                    self.dag_file.write("%s_import = BashOperator(\n" % (task_id))
                    self.dag_file.write("    task_id='%s_import',\n" % (task_id))
                    self.dag_file.write(
                        "    bash_command='%s -h %s -t %s -I -C ',\n"
                        % (dbimport_cmd, row["hive_db"], row["hive_table"])
                    )
                    self.dag_file.write("    pool='%s',\n" % (import_phase_pool))
                    self.dag_file.write(f"    priority_weight={airflow_priority},\n")
                    self.dag_file.write("    weight_rule='absolute',\n")
                    self.dag_file.write(f"    retries={retries_import_phase},\n")
                    self.dag_file.write("    dag=dag)\n")
                    self.dag_file.write("\n")

                    self.dag_file.write("%s_etl = BashOperator(\n" % (task_id))
                    self.dag_file.write("    task_id='%s_etl',\n" % (task_id))
                    self.dag_file.write(
                        "    bash_command='%s -h %s -t %s -E ',\n"
                        % (dbimport_cmd, row["hive_db"], row["hive_table"])
                    )
                    self.dag_file.write("    pool='%s',\n" % (etl_phase_pool))
                    self.dag_file.write(f"    priority_weight={airflow_priority},\n")
                    self.dag_file.write("    weight_rule='absolute',\n")
                    self.dag_file.write(f"    retries={retries_etl_phase},\n")
                    self.dag_file.write("    dag=dag)\n")
                    self.dag_file.write("\n")

                elif self.airflow_mode == "aws_mwaa":
                    instance_id = self.get_aws_instance_id(row["hive_table"])
                    self.dag_file.write("%s_import = PythonOperator(\n" % (task_id))
                    self.dag_file.write("    task_id='%s_import',\n" % (task_id))
                    self.dag_file.write("    python_callable=startDBImportExecution,\n")
                    self.dag_file.write(
                        '    op_kwargs={"command": "import -d %s -t %s -I -C", "instanceID": "%s"},\n'
                        % (row["hive_db"], row["hive_table"], instance_id)
                    )
                    self.dag_file.write("    pool='%s',\n" % (import_phase_pool))
                    self.dag_file.write(f"    priority_weight={airflow_priority},\n")
                    self.dag_file.write("    weight_rule='absolute',\n")
                    self.dag_file.write(f"    retries={retries_import_phase},\n")
                    self.dag_file.write("    dag=dag)\n")
                    self.dag_file.write("\n")

                    self.dag_file.write("%s_etl = PythonOperator(\n" % (task_id))
                    self.dag_file.write("    task_id='%s_etl',\n" % (task_id))
                    self.dag_file.write("    python_callable=startDBImportExecution,\n")
                    self.dag_file.write(
                        '    op_kwargs={"command": "import -d %s -t %s -E", "instanceID": "%s"},\n'
                        % (row["hive_db"], row["hive_table"], instance_id)
                    )
                    self.dag_file.write("    pool='%s',\n" % (etl_phase_pool))
                    self.dag_file.write(f"    priority_weight={airflow_priority},\n")
                    self.dag_file.write("    weight_rule='absolute',\n")
                    self.dag_file.write(f"    retries={retries_etl_phase},\n")
                    self.dag_file.write("    dag=dag)\n")
                    self.dag_file.write("\n")

                if clear_stage_required and dag["finish_all_stage1_first"] == 1:
                    self.dag_file.write(
                        "%s.set_downstream(%s_clearStage)\n"
                        % (self.main_start_task, task_id)
                    )
                    if import_phase_as_sensor:
                        self.dag_file.write(
                            "%s_clearStage.set_downstream(%s_sensor)\n"
                            % (task_id, task_id)
                        )
                        self.dag_file.write(
                            "%s_sensor.set_downstream(%s_import)\n" % (task_id, task_id)
                        )
                    else:
                        self.dag_file.write(
                            "%s_clearStage.set_downstream(%s_import)\n"
                            % (task_id, task_id)
                        )
                    self.dag_file.write(
                        "%s_import.set_downstream(Import_Phase_Finished)\n" % (task_id)
                    )
                    self.dag_file.write(
                        "Import_Phase_Finished.set_downstream(%s_etl)\n" % (task_id)
                    )
                    self.dag_file.write(
                        "%s_etl.set_downstream(%s)\n" % (task_id, self.main_stop_task)
                    )
                elif (
                    clear_stage_required and dag["finish_all_stage1_first"] == 0
                ):  # This means that runImportAndEtlSeparate == True
                    self.dag_file.write(
                        "%s.set_downstream(%s_clearStage)\n"
                        % (self.main_start_task, task_id)
                    )
                    if import_phase_as_sensor:
                        self.dag_file.write(
                            "%s_clearStage.set_downstream(%s_sensor)\n"
                            % (task_id, task_id)
                        )
                        self.dag_file.write(
                            "%s_sensor.set_downstream(%s_import)\n" % (task_id, task_id)
                        )
                    else:
                        self.dag_file.write(
                            "%s_clearStage.set_downstream(%s_import)\n"
                            % (task_id, task_id)
                        )
                    self.dag_file.write(
                        "%s_import.set_downstream(%s_etl)\n" % (task_id, task_id)
                    )
                    self.dag_file.write(
                        "%s_etl.set_downstream(%s)\n" % (task_id, self.main_stop_task)
                    )
                elif not clear_stage_required and dag["finish_all_stage1_first"] == 1:
                    if import_phase_as_sensor:
                        self.dag_file.write(
                            "%s.set_downstream(%s_sensor)\n"
                            % (self.main_start_task, task_id)
                        )
                        self.dag_file.write(
                            "%s_sensor.set_downstream(%s_import)\n" % (task_id, task_id)
                        )
                    else:
                        self.dag_file.write(
                            "%s.set_downstream(%s_import)\n"
                            % (self.main_start_task, task_id)
                        )
                    self.dag_file.write(
                        "%s_import.set_downstream(Import_Phase_Finished)\n" % (task_id)
                    )
                    self.dag_file.write(
                        "Import_Phase_Finished.set_downstream(%s_etl)\n" % (task_id)
                    )
                    self.dag_file.write(
                        "%s_etl.set_downstream(%s)\n" % (task_id, self.main_stop_task)
                    )
                else:
                    if import_phase_as_sensor:
                        self.dag_file.write(
                            "%s.set_downstream(%s_sensor)\n"
                            % (self.main_start_task, task_id)
                        )
                        self.dag_file.write(
                            "%s_sensor.set_downstream(%s_import)\n" % (task_id, task_id)
                        )
                    else:
                        self.dag_file.write(
                            "%s.set_downstream(%s_import)\n"
                            % (self.main_start_task, task_id)
                        )
                    self.dag_file.write(
                        "%s_import.set_downstream(%s_etl)\n" % (task_id, task_id)
                    )
                    self.dag_file.write(
                        "%s_etl.set_downstream(%s)\n" % (task_id, self.main_stop_task)
                    )
                self.dag_file.write("\n")

            else:
                if import_phase_as_sensor:
                    # Running Import phase as a sensor
                    self.dag_file.write("%s_sensor = SqlSensor(\n" % (task_id))
                    self.dag_file.write("    task_id='%s_sensor',\n" % (task_id))
                    self.dag_file.write("    conn_id='DBImport',\n")
                    self.dag_file.write(
                        "    sql=\"\"\"select count(*) from import_tables where hive_db = '%s' and hive_table = '%s' and "
                        % (row["hive_db"], row["hive_table"])
                    )
                    self.dag_file.write(
                        "copy_finished >= '{{ next_execution_date.strftime('%Y-%m-%d %H:%M:%S.%f') }}'\"\"\",\n"
                    )
                    self.dag_file.write("    pool='%s',\n" % (import_phase_pool))
                    self.dag_file.write(f"    priority_weight={airflow_priority},\n")
                    self.dag_file.write("    weight_rule='absolute',\n")
                    self.dag_file.write("    timeout=18000,\n")
                    self.dag_file.write("    poke_interval=300,\n")
                    self.dag_file.write("    mode='reschedule',\n")
                    self.dag_file.write("    dag=dag)\n")
                    self.dag_file.write("\n")

                if self.airflow_mode == "default":
                    self.dag_file.write("%s = BashOperator(\n" % (task_id))
                    self.dag_file.write("    task_id='%s',\n" % (task_id))
                    self.dag_file.write(
                        "    bash_command='%s -h %s -t %s %s ',\n"
                        % (
                            dbimport_cmd,
                            row["hive_db"],
                            row["hive_table"],
                            meta_data_import_option,
                        )
                    )
                    self.dag_file.write("    pool='%s',\n" % (etl_phase_pool))
                    self.dag_file.write(f"    priority_weight={airflow_priority},\n")
                    self.dag_file.write("    weight_rule='absolute',\n")
                    self.dag_file.write(f"    retries={retries},\n")
                    self.dag_file.write("    dag=dag)\n")
                    self.dag_file.write("\n")
                elif self.airflow_mode == "aws_mwaa":
                    instance_id = self.get_aws_instance_id(row["hive_table"])
                    self.dag_file.write("%s = PythonOperator(\n" % (task_id))
                    self.dag_file.write("    task_id='%s',\n" % (task_id))
                    self.dag_file.write("    python_callable=startDBImportExecution,\n")
                    self.dag_file.write(
                        '    op_kwargs={"command": "import -d %s -t %s %s", "instanceID": "%s"},\n'
                        % (
                            row["hive_db"],
                            row["hive_table"],
                            meta_data_import_option,
                            instance_id,
                        )
                    )
                    self.dag_file.write("    pool='%s',\n" % (etl_phase_pool))
                    self.dag_file.write(f"    priority_weight={airflow_priority},\n")
                    self.dag_file.write("    weight_rule='absolute',\n")
                    self.dag_file.write(f"    retries={retries},\n")
                    self.dag_file.write("    dag=dag)\n")
                    self.dag_file.write("\n")

                if clear_stage_required:
                    if import_phase_as_sensor:
                        self.dag_file.write(
                            "%s.set_downstream(%s_clearStage)\n"
                            % (self.main_start_task, task_id)
                        )
                        self.dag_file.write(
                            "%s_clearStage.set_downstream(%s_sensor)\n"
                            % (task_id, task_id)
                        )
                        self.dag_file.write(
                            "%s_sensor.set_downstream(%s)\n" % (task_id, task_id)
                        )
                        self.dag_file.write(
                            "%s.set_downstream(%s)\n" % (task_id, self.main_stop_task)
                        )
                    else:
                        self.dag_file.write(
                            "%s.set_downstream(%s_clearStage)\n"
                            % (self.main_start_task, task_id)
                        )
                        self.dag_file.write(
                            "%s_clearStage.set_downstream(%s)\n" % (task_id, task_id)
                        )
                        self.dag_file.write(
                            "%s.set_downstream(%s)\n" % (task_id, self.main_stop_task)
                        )
                else:
                    if import_phase_as_sensor:
                        self.dag_file.write(
                            "%s.set_downstream(%s_sensor)\n"
                            % (self.main_start_task, task_id)
                        )
                        self.dag_file.write(
                            "%s_sensor.set_downstream(%s)\n" % (task_id, task_id)
                        )
                        self.dag_file.write(
                            "%s.set_downstream(%s)\n" % (task_id, self.main_stop_task)
                        )
                    else:
                        self.dag_file.write(
                            "%s.set_downstream(%s)\n" % (self.main_start_task, task_id)
                        )
                        self.dag_file.write(
                            "%s.set_downstream(%s)\n" % (task_id, self.main_stop_task)
                        )
                self.dag_file.write("\n")

        self.add_tasks_to_dag_file(
            dag_name=dag["dag_name"],
            main_dag_schedule=dag["schedule_interval"],
            default_retries=retries,
            default_sudo_user=sudo_user,
        )
        self.add_sensors_to_dag_file(
            dag_name=dag["dag_name"], main_dag_schedule=dag["schedule_interval"]
        )
        self.create_airflow_pools(pools=used_pools)
        if (
            self.airflow_mode == "aws_mwaa"
            and self.airflow_aws_pool_to_instance_id == True
        ):
            self.aws_append_instance_id_as_pools()
        self.close_dag_file()
        session.close()

    def create_airflow_pools(self, pools):
        """Creates the pools in Airflow database"""
        log = logging.getLogger(self.logger)

        if self.airflow_mode == "default" and self.create_pools_with_tasks == False:
            session = self.airflow_db_session()
            slot_pool = aliased(airflowSchema.SlotPool)

            airflow_pools = pd.DataFrame(
                session.query(slot_pool.pool, slot_pool.slots, slot_pool.description)
                .select_from(slot_pool)
                .all()
            )

            for pool in pools:
                # Just to make sure we dont create the same pool twice. That will generate an error in Airflow
                if pool in self.created_pools:
                    continue
                else:
                    self.created_pools.append(pool)

                if (
                    len(airflow_pools) == 0
                    or len(airflow_pools.loc[airflow_pools["pool"] == pool]) == 0
                ):
                    try:
                        log.info(
                            "Creating the Airflow pool '%s' with %s slots",
                            pool,
                            self.airflow_default_pool_size,
                        )
                        new_pool = airflowSchema.SlotPool(
                            pool=pool,
                            slots=self.airflow_default_pool_size,
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

        elif self.create_pools_with_tasks == True:
            for pool in pools:
                # Just to make sure we dont create the same pool twice. That will generate an error in Airflow
                if pool in self.created_pools:
                    continue
                else:
                    self.created_pools.append(pool)

                task_name = "createPool_%s" % (pool.replace(".", "_").replace("-", "_"))
                self.dag_file.write("\n")
                self.dag_file.write(f"{task_name} = CreatePoolOperator(\n")
                self.dag_file.write("    name='%s',\n" % (pool))
                self.dag_file.write(
                    "    slots=%s,\n" % (self.airflow_default_pool_size)
                )
                self.dag_file.write("    task_id='%s',\n" % (task_name))
                self.dag_file.write("    retries=5,\n")
                self.dag_file.write("    priority_weight=1,\n")
                self.dag_file.write("    weight_rule='absolute',\n")
                self.dag_file.write("    pool=None,\n")
                self.dag_file.write("    dag=dag)\n")
                self.dag_file.write("\n")
                self.dag_file.write(f"start.set_downstream({task_name})\n")
                self.dag_file.write(
                    "%s.set_downstream(%s)\n" % (task_name, self.pre_stop_task)
                )
                self.dag_file.write("\n")

    def generate_custom_dag(self, dag):
        """Generates a Custom DAG"""
        log = logging.getLogger(self.logger)

        # session = self.config_db_session()
        session = self.get_dbimport_session()
        airflow_tasks = aliased(configSchema.AirflowTasks)

        tasks = (
            session.query(airflow_tasks.task_name)
            .select_from(airflow_tasks)
            .filter(airflow_tasks.dag_name == dag["dag_name"])
            .filter(airflow_tasks.include_in_airflow == 1)
            .filter(airflow_tasks.placement == "in main")
            .count()
        )

        if tasks == 0:
            log.warning(
                "There are no tasks defined 'in main' for DAG '%s'. This is required for a custom DAG"
                % (dag["dag_name"])
            )
            session.close()
            return

        used_pools = []
        # 		if self.airflow_mode == "aws_mwaa" and self.airflow_aws_pool_to_instance_id == True:
        # 			# We can choice to override the poolname and set it to the instance ID if we are running on AWS
        # 			defaultPool = self.airflow_aws_instance_ids
        # 		else:
        # 			defaultPool = DAG['dag_name']
        default_pool = dag["dag_name"]
        sudo_user = dag["sudo_user"]
        used_pools.append(default_pool)

        if dag["retries"] == None or dag["retries"] == "":
            retries = 0
        else:
            retries = int(dag["retries"])

        cron_schedule = self.convert_time_to_cron(dag["schedule_interval"])
        self.create_dag_file_with_header(
            dag_name=dag["dag_name"],
            cron_schedule=cron_schedule,
            default_pool=default_pool,
            sudo_user=sudo_user,
            dag_time_zone=dag["timezone"],
            email=dag["email"],
            email_on_failure=dag["email_on_failure"],
            email_on_retries=dag["email_on_retries"],
            tags=dag["tags"],
            sla_warning_time=dag["sla_warning_time"],
            retry_exponential_backoff=dag["retry_exponential_backoff"],
            concurrency=dag["concurrency"],
        )
        self.add_tasks_to_dag_file(
            dag_name=dag["dag_name"],
            main_dag_schedule=dag["schedule_interval"],
            default_retries=retries,
            default_sudo_user=sudo_user,
        )
        self.add_sensors_to_dag_file(
            dag_name=dag["dag_name"], main_dag_schedule=dag["schedule_interval"]
        )
        self.create_airflow_pools(pools=used_pools)
        self.close_dag_file()
        session.close()

    def convert_time_to_cron(self, time):
        """Converts a string in format HH:MM to a CRON time string based on 'minute hour day month weekday'"""
        log = logging.getLogger(self.logger)
        return_value = time

        if re.search("^[0-2][0-9]:[0-5][0-9]$", time):
            hour = re.sub(":[0-5][0-9]$", "", time)
            minute = re.sub("^[0-2][0-9]:", "", time)
            return_value = f"'{int(minute)} {int(hour)} * * *'"

        return return_value

    def validate_time_zone(self, time_zone):
        log = logging.getLogger(self.logger)
        try:
            pendulum.timezone(time_zone)
        except ValueError:
            return False
        else:
            return True

    def create_dag_file_with_header(
        self,
        dag_name,
        cron_schedule,
        default_pool,
        import_phase_finish_first=False,
        sudo_user="",
        dag_time_zone=None,
        email=None,
        email_on_failure=None,
        email_on_retries=None,
        tags=None,
        sla_warning_time=None,
        retry_exponential_backoff=None,
        concurrency=None,
    ):
        log = logging.getLogger(self.logger)
        # session = self.config_db_session()
        session = self.get_dbimport_session()

        self.sensor_start_task = "start"
        self.sensor_stop_task = "dag_sensors_finished"
        self.pre_start_task = "start"
        self.pre_stop_task = "before_tasks_finished"
        self.main_start_task = "start"
        self.main_stop_task = "stop"
        self.post_start_task = "main_tasks_finished"
        self.post_stop_task = "stop"

        if (
            self.task_queue_for_dummy != None
            and self.task_queue_for_dummy.strip() != ""
            and self.task_queue_for_dummy.strip() != "default"
        ):
            self.task_queue_for_dummy = self.task_queue_for_dummy.strip()
        else:
            self.task_queue_for_dummy = None

        tasks_before_main_exists = False
        tasks_after_main_exists = False
        tasks_sensors_exists = False

        if (
            dag_time_zone != None
            and dag_time_zone != ""
            and self.validate_time_zone(dag_time_zone) == False
        ):
            log.warning(
                "Time zone value specified for DAG is not valid, (%s)" % (dag_time_zone)
            )
            log.warning("Using default time zone value instead")
            dag_time_zone = None

        if self.dag_folder == None:
            self.dag_filename = "%s/%s.py" % (self.dag_staging_directory, dag_name)
            if not os.path.exists(self.dag_staging_directory):
                try:
                    log.info(
                        "Creating DAG staging directory (%s)"
                        % (self.dag_staging_directory)
                    )
                    os.makedirs(self.dag_staging_directory)
                except:
                    pass
        else:
            self.dag_filename = "%s/%s.py" % (self.dag_folder, dag_name)

        self.dag_filename_in_aws_mwaa = "dags/%s.py" % (dag_name)
        self.dag_filename_in_airflow = "%s/%s.py" % (self.dag_directory, dag_name)

        try:
            self.dag_file = open(self.dag_filename, "w")
        except FileNotFoundError:
            log.error(
                "Cant open file '%s'. Please verify that the directory exists and that the configuration 'airflow_dbimport_commandpath' is correct",
                self.dag_filename,
            )
            raise RuntimeError
            # self.common_config.remove_temporary_files()
            # sys.exit(1)

        self.dag_file.write("# -*- coding: utf-8 -*-\n")
        self.dag_file.write("import airflow\n")
        self.dag_file.write("from airflow import DAG\n")
        self.dag_file.write("from airflow.models import Variable\n")

        if self.airflow_major_version == 2:
            self.dag_file.write(
                "from airflow.sensors.external_task import ExternalTaskSensor\n"
            )
        else:
            self.dag_file.write(
                "from airflow.operators.sensors import ExternalTaskSensor\n"
            )

        self.dag_file.write("from airflow.sensors.sql_sensor import SqlSensor\n")
        self.dag_file.write(
            "from airflow.operators.bash_operator import BashOperator\n"
        )
        self.dag_file.write(
            "from airflow.operators.python_operator import BranchPythonOperator\n"
        )
        self.dag_file.write(
            "from airflow.operators.dagrun_operator import TriggerDagRunOperator\n"
        )
        self.dag_file.write(
            "from airflow.operators.dummy_operator import DummyOperator\n"
        )
        self.dag_file.write(
            "from airflow.api.common.experimental.pool import get_pool, create_pool\n"
        )
        self.dag_file.write("from airflow.exceptions import PoolNotFound\n")
        self.dag_file.write("from airflow.models import BaseOperator\n")

        if self.airflow_mode == "aws_mwaa":
            self.dag_file.write(
                "from airflow.operators.dummy_operator import DummyOperator\n"
            )
            self.dag_file.write(
                "from airflow.operators.python_operator import PythonOperator\n"
            )
            self.dag_file.write("import botocore\n")
            self.dag_file.write("import boto3\n")
            self.dag_file.write("import time\n")

        self.dag_file.write("from datetime import datetime, timedelta, timezone\n")
        self.dag_file.write("import pendulum\n")
        self.dag_file.write("\n")
        self.dag_file.write("try:\n")
        self.dag_file.write('    Email_receiver = Variable.get("Email_receiver")\n')
        self.dag_file.write("except KeyError:\n")
        self.dag_file.write('    Email_receiver = "not-set@domain.com"\n')
        self.dag_file.write("\n")
        self.dag_file.write("\n")

        self.dag_file.write("class CreatePoolOperator(BaseOperator):\n")
        self.dag_file.write("    ui_color = '#b8e9ee'\n")
        self.dag_file.write("\n")
        self.dag_file.write("    def __init__(\n")
        self.dag_file.write("            self,\n")
        self.dag_file.write("            name,\n")
        self.dag_file.write("            slots,\n")
        self.dag_file.write("            description='',\n")
        self.dag_file.write("            *args, **kwargs):\n")
        self.dag_file.write(
            "        super(CreatePoolOperator, self).__init__(*args, **kwargs)\n"
        )
        self.dag_file.write("        self.description = description\n")
        self.dag_file.write("        self.slots = slots\n")
        self.dag_file.write("        self.name = name\n")
        self.dag_file.write("\n")
        self.dag_file.write("    def execute(self, context):\n")
        self.dag_file.write("        try:\n")
        self.dag_file.write("            pool = get_pool(name=self.name)\n")
        # self.dag_file.write("            if pool:\n")
        self.dag_file.write("            print('Pool exists: %s'%(pool))\n")
        self.dag_file.write("        except PoolNotFound:\n")
        self.dag_file.write("            # create the pool\n")
        self.dag_file.write(
            "            pool = create_pool(name=self.name, slots=self.slots, description=self.description)\n"
        )
        self.dag_file.write("            print('Pool exists: %s'%(pool))\n")
        self.dag_file.write("\n")
        self.dag_file.write("\n")

        if dag_time_zone == None or dag_time_zone == "":
            self.dag_file.write(
                'local_tz = pendulum.timezone("%s")\n' % (self.default_time_zone)
            )
            self.dag_file.write("\n")
        else:
            self.dag_file.write(
                'local_tz = pendulum.timezone("%s")\n' % (dag_time_zone)
            )
            self.dag_file.write("\n")

        if self.airflow_mode == "aws_mwaa":
            self.dag_file.write(
                "def printAndFindEventErrors(event, startWithNewline = False):\n"
            )
            self.dag_file.write("    errorFound = False\n")
            self.dag_file.write("\n")
            self.dag_file.write("    if startWithNewline == True:\n")
            self.dag_file.write('        findPattern = "\\nERROR"\n')
            self.dag_file.write("    else:\n")
            self.dag_file.write('        findPattern = "ERROR"\n')
            self.dag_file.write("\n")
            self.dag_file.write("    try:\n")
            self.dag_file.write("        print(event['message'], end=\"\")\n")
            self.dag_file.write(
                "        if event['message'].find(findPattern) != -1:\n"
            )
            # self.dag_file.write("        if event['message'].find(\"ERROR\") != -1:\n")
            self.dag_file.write("            errorFound = True\n")
            # self.dag_file.write("            print(\"Error Found (A) !!!!!\")\n")
            self.dag_file.write("            print(event)\n")
            self.dag_file.write("    except:\n")
            self.dag_file.write("        print(event)\n")
            self.dag_file.write("        if str(event).find(findPattern) != -1:\n")
            # self.dag_file.write("        if str(event).find(\"ERROR\") != -1:\n")
            self.dag_file.write("            errorFound = True\n")
            # self.dag_file.write("            print(\"Error Found (B) !!!!!\")\n")
            self.dag_file.write("\n")
            self.dag_file.write("    return errorFound\n")
            self.dag_file.write("\n")
            self.dag_file.write("\n")
            self.dag_file.write("def startDBImportExecution(command, instanceID):\n")
            self.dag_file.write("\n")
            self.dag_file.write(
                "    ssm_client = boto3.client('ssm',region_name='eu-west-1')\n"
            )
            self.dag_file.write(
                "    logs_client = boto3.client('logs',region_name='eu-west-1')\n"
            )
            self.dag_file.write("\n")
            # self.dag_file.write("    instanceID = \"%s\"\n"%(self.airflow_aws_instance_ids))
            self.dag_file.write(
                '    commandPath = "%s"\n' % (self.get_dbimport_command_path())
            )
            self.dag_file.write(
                '    command = "%sbin/%s || echo ERROR; sleep 1"%(commandPath, command)\n'
            )
            self.dag_file.write('    cloudWatchLogGroupName = "dbimport"\n')
            self.dag_file.write("    errorFound = False\n")
            self.dag_file.write("\n")
            self.dag_file.write("\n")
            self.dag_file.write(
                "    # Start the import by the help of SSM Send Command\n"
            )
            self.dag_file.write("    sendCommand_response = ssm_client.send_command(\n")
            self.dag_file.write("        InstanceIds=[instanceID],\n")
            self.dag_file.write('        DocumentName="AWS-RunShellScript",\n')
            self.dag_file.write(
                "        Parameters={'commands': [command], 'executionTimeout': ['172800']},\n"
            )
            self.dag_file.write(
                "        CloudWatchOutputConfig={'CloudWatchLogGroupName': cloudWatchLogGroupName, 'CloudWatchOutputEnabled': True}\n"
            )
            self.dag_file.write("        )\n")
            self.dag_file.write("\n")
            self.dag_file.write("\n")
            self.dag_file.write(
                "    # Get the commandID and set the logStreamName for CloudWatch\n"
            )
            self.dag_file.write(
                "    commandID = sendCommand_response['Command']['CommandId']\n"
            )
            self.dag_file.write(
                '    logStreamNameStdOut = "%s/%s/aws-runShellScript/stdout"%(commandID, instanceID)\n'
            )
            self.dag_file.write(
                '    logStreamNameStdErr = "%s/%s/aws-runShellScript/stderr"%(commandID, instanceID)\n'
            )
            self.dag_file.write("\n")
            self.dag_file.write("    print('')\n")
            self.dag_file.write("    print('CloudWatch logs is available at:')\n")
            self.dag_file.write("    print(logStreamNameStdOut)\n")
            self.dag_file.write("    print(logStreamNameStdErr)\n")
            self.dag_file.write("    print('')\n")
            self.dag_file.write("\n")
            self.dag_file.write("\n")
            self.dag_file.write(
                "    # Run a loop until the command is actually running\n"
            )
            self.dag_file.write("    commandRunning = False\n")
            self.dag_file.write("    while commandRunning == False:\n")
            self.dag_file.write("        try:\n")
            self.dag_file.write(
                "            commandInvocation_response = ssm_client.get_command_invocation(\n"
            )
            self.dag_file.write("                CommandId=commandID,\n")
            self.dag_file.write("                InstanceId=instanceID\n")
            self.dag_file.write("                )\n")
            self.dag_file.write(
                "            commandStatus = commandInvocation_response['Status']\n"
            )
            self.dag_file.write("\n")
            # self.dag_file.write("            print('commandStatus = %s'%(commandStatus))\n")
            # self.dag_file.write("\n")
            self.dag_file.write(
                "            if commandStatus in ('InProgress', 'Success'):\n"
            )
            self.dag_file.write("                commandRunning = True\n")
            self.dag_file.write(
                "            elif commandStatus in ('Pending', 'Delayed'):\n"
            )
            self.dag_file.write("                commandRunning = False\n")
            self.dag_file.write(
                "            elif commandStatus in ('DeliveryTimedOut', 'ExecutionTimedOut', 'Failed', 'Cancelled', 'Undeliverable', 'Terminated', 'InvalidPlatform', 'AccessDenied'):\n"
            )
            self.dag_file.write("                raise\n")
            self.dag_file.write("\n")
            self.dag_file.write(
                "        except botocore.exceptions.ClientError as err:\n"
            )
            self.dag_file.write(
                "            if err.response['Error']['Code'] == 'InvocationDoesNotExist':\n"
            )
            self.dag_file.write("                pass\n")
            self.dag_file.write("            else:\n")
            self.dag_file.write("                raise err\n")
            self.dag_file.write("\n")
            self.dag_file.write("        time.sleep(5)\n")
            self.dag_file.write("\n")
            self.dag_file.write(
                "    # Start fetching log from CloudWatch. Initial fetch will be from head and also fetch the token for upcomming messages\n"
            )
            self.dag_file.write("    nextTokenStdOut = None\n")
            self.dag_file.write("    nextTokenStdErr = None\n")
            self.dag_file.write("    logEventsRunning = False\n")
            self.dag_file.write("    while logEventsRunning == False:\n")
            self.dag_file.write("        try:\n")
            self.dag_file.write(
                "            responseStdOut = logs_client.get_log_events(\n"
            )
            self.dag_file.write("                logGroupName='dbimport',\n")
            self.dag_file.write("                logStreamName=logStreamNameStdOut,\n")
            self.dag_file.write("                startFromHead=True\n")
            self.dag_file.write("                )\n")
            self.dag_file.write("\n")
            self.dag_file.write("            for event in responseStdOut['events']:\n")
            self.dag_file.write(
                "                if printAndFindEventErrors(event, startWithNewline = False) == True:\n"
            )
            self.dag_file.write("                    errorFound = True\n")
            # self.dag_file.write("                try:\n")
            # self.dag_file.write("                    print(event['message'], end=\"\")\n")
            # self.dag_file.write("                    if event['message'].find(\"\\nERROR\") != -1:\n")
            # # self.dag_file.write("                    if event['message'].find(\"ERROR\") != -1:\n")
            # self.dag_file.write("                        errorFound = True\n")
            # self.dag_file.write("                        print(\"Error Found (1) !!!!!\")\n")
            # self.dag_file.write("                        print(event)\n")
            # self.dag_file.write("                except:\n")
            # self.dag_file.write("                    print(event)\n")
            # self.dag_file.write("                    if str(event).find(\"\\nERROR\") != -1:\n")
            # # self.dag_file.write("                    if str(event).find(\"ERROR\") != -1:\n")
            # self.dag_file.write("                        errorFound = True\n")
            # self.dag_file.write("                        print(\"Error Found (2) !!!!!\")\n")
            self.dag_file.write("\n")
            self.dag_file.write(
                "            nextTokenStdOut = responseStdOut['nextForwardToken']\n"
            )
            self.dag_file.write("            logEventsRunning = True\n")
            self.dag_file.write("\n")
            self.dag_file.write(
                "            responseStdErr = logs_client.get_log_events(\n"
            )
            self.dag_file.write("                logGroupName='dbimport',\n")
            self.dag_file.write("                logStreamName=logStreamNameStdErr,\n")
            self.dag_file.write("                startFromHead=True\n")
            self.dag_file.write("                )\n")
            self.dag_file.write("\n")
            self.dag_file.write(
                "            nextTokenStdErr = responseStdErr['nextForwardToken']\n"
            )
            self.dag_file.write("\n")
            self.dag_file.write("            for event in responseStdErr['events']:\n")
            self.dag_file.write(
                "                if printAndFindEventErrors(event, startWithNewline = True) == True:\n"
            )
            self.dag_file.write("                    errorFound = True\n")
            # self.dag_file.write("                try:\n")
            # self.dag_file.write("                    print(event['message'], end=\"\")\n")
            # self.dag_file.write("                    if event['message'].find(\"\\nERROR\") != -1:\n")
            # # self.dag_file.write("                    if event['message'].find(\"ERROR\") != -1:\n")
            # self.dag_file.write("                        errorFound = True\n")
            # self.dag_file.write("                        print(\"Error Found (1) !!!!!\")\n")
            # self.dag_file.write("                        print(event)\n")
            # self.dag_file.write("                except:\n")
            # self.dag_file.write("                    print(event)\n")
            # self.dag_file.write("                    if str(event).find(\"\\nERROR\") != -1:\n")
            # # self.dag_file.write("                    if str(event).find(\"ERROR\") != -1:\n")
            # self.dag_file.write("                        errorFound = True\n")
            # self.dag_file.write("                        print(\"Error Found (2) !!!!!\")\n")
            self.dag_file.write("\n")
            self.dag_file.write(
                "        except botocore.exceptions.ClientError as err:\n"
            )
            self.dag_file.write(
                "            if err.response['Error']['Code'] == 'ResourceNotFoundException':\n"
            )
            self.dag_file.write("                pass\n")
            self.dag_file.write(
                "            elif err.response['Error']['Code'] == 'ThrottlingException':\n"
            )
            self.dag_file.write("                pass\n")
            self.dag_file.write("            else:\n")
            self.dag_file.write(
                "                print(\"DEBUG: %s\"%(err.response['Error']['Code']))\n"
            )
            self.dag_file.write("                raise err\n")
            self.dag_file.write("\n")
            self.dag_file.write("\n")
            self.dag_file.write("        time.sleep(1)\n")
            self.dag_file.write("\n")
            self.dag_file.write("    if commandStatus == 'TimedOut':\n")
            self.dag_file.write("        print('AWS SSM Send-command timeout!')\n")
            self.dag_file.write("        errorFound = True\n")
            self.dag_file.write("\n")
            self.dag_file.write(
                "    # Loop while the command is running and print all messages from CloudWatch at the same time\n"
            )
            self.dag_file.write("    while commandStatus == 'InProgress':\n")
            self.dag_file.write("        try:\n")
            self.dag_file.write(
                "            commandInvocation_response = ssm_client.get_command_invocation(\n"
            )
            self.dag_file.write("                CommandId=commandID,\n")
            self.dag_file.write("                InstanceId=instanceID\n")
            self.dag_file.write("                )\n")
            self.dag_file.write("\n")
            self.dag_file.write(
                "            commandStatus = commandInvocation_response['Status']\n"
            )
            # self.dag_file.write("            print(commandInvocation_response)\n")
            self.dag_file.write("\n")
            self.dag_file.write("            if commandStatus == 'TimedOut':\n")
            self.dag_file.write(
                "                print('AWS SSM Send-command timeout!')\n"
            )
            self.dag_file.write("                errorFound = True\n")
            self.dag_file.write("                break\n")
            self.dag_file.write("            elif commandStatus != 'InProgress':\n")
            self.dag_file.write("                break\n")
            self.dag_file.write("\n")
            self.dag_file.write(
                "            responseStdOut = logs_client.get_log_events(\n"
            )
            self.dag_file.write("                logGroupName='dbimport',\n")
            self.dag_file.write("                logStreamName=logStreamNameStdOut,\n")
            self.dag_file.write("                nextToken=nextTokenStdOut\n")
            self.dag_file.write("                )\n")
            self.dag_file.write("\n")
            self.dag_file.write(
                "            nextTokenStdOut = responseStdOut['nextForwardToken']\n"
            )
            self.dag_file.write("\n")
            self.dag_file.write("            for event in responseStdOut['events']:\n")
            self.dag_file.write(
                "                if printAndFindEventErrors(event, startWithNewline = False) == True:\n"
            )
            self.dag_file.write("                    errorFound = True\n")
            # self.dag_file.write("                try:\n")
            # self.dag_file.write("                    print(str(event['message']), end=\"\")\n")
            # self.dag_file.write("                    if event['message'].find(\"\\nERROR\") != -1:\n")
            # # self.dag_file.write("                    if event['message'].find(\"ERROR\") != -1:\n")
            # self.dag_file.write("                        errorFound = True\n")
            # self.dag_file.write("                        print(\"Error Found (3) !!!!!\")\n")
            # self.dag_file.write("                        print(event)\n")
            # self.dag_file.write("                except:\n")
            # self.dag_file.write("                    print(event)\n")
            # self.dag_file.write("                    if str(event).find(\"\\nERROR\") != -1:\n")
            # # self.dag_file.write("                    if str(event).find(\"ERROR\") != -1:\n")
            # self.dag_file.write("                        errorFound = True\n")
            # self.dag_file.write("                        print(\"Error Found (4) !!!!!\")\n")
            self.dag_file.write("\n")
            self.dag_file.write("            if nextTokenStdErr == None:\n")
            self.dag_file.write(
                "                responseStdErr = logs_client.get_log_events(\n"
            )
            self.dag_file.write("                    logGroupName='dbimport',\n")
            self.dag_file.write(
                "                    logStreamName=logStreamNameStdErr,\n"
            )
            self.dag_file.write("                    startFromHead=True\n")
            self.dag_file.write("                    )\n")
            self.dag_file.write("            else:\n")
            self.dag_file.write(
                "                responseStdErr = logs_client.get_log_events(\n"
            )
            self.dag_file.write("                    logGroupName='dbimport',\n")
            self.dag_file.write(
                "                    logStreamName=logStreamNameStdErr,\n"
            )
            self.dag_file.write("                    nextToken=nextTokenStdErr\n")
            self.dag_file.write("                    )\n")
            self.dag_file.write("\n")
            self.dag_file.write(
                "            nextTokenStdErr = responseStdErr['nextForwardToken']\n"
            )
            self.dag_file.write("\n")
            self.dag_file.write("            for event in responseStdErr['events']:\n")
            self.dag_file.write(
                "                if printAndFindEventErrors(event, startWithNewline = True) == True:\n"
            )
            self.dag_file.write("                    errorFound = True\n")
            # self.dag_file.write("                try:\n")
            # self.dag_file.write("                    print(str(event['message']), end=\"\")\n")
            # self.dag_file.write("                    if event['message'].find(\"\\nERROR\") != -1:\n")
            # # self.dag_file.write("                    if event['message'].find(\"ERROR\") != -1:\n")
            # self.dag_file.write("                        errorFound = True\n")
            # self.dag_file.write("                        print(\"Error Found (3) !!!!!\")\n")
            # self.dag_file.write("                        print(event)\n")
            # self.dag_file.write("                except:\n")
            # self.dag_file.write("                    print(event)\n")
            # self.dag_file.write("                    if str(event).find(\"\\nERROR\") != -1:\n")
            # # self.dag_file.write("                    if str(event).find(\"ERROR\") != -1:\n")
            # self.dag_file.write("                        errorFound = True\n")
            # self.dag_file.write("                        print(\"Error Found (4) !!!!!\")\n")
            self.dag_file.write("\n")
            self.dag_file.write(
                "        except botocore.exceptions.ClientError as err:\n"
            )
            self.dag_file.write(
                "            if err.response['Error']['Code'] == 'ResourceNotFoundException':\n"
            )
            self.dag_file.write("                pass\n")
            self.dag_file.write(
                "            elif err.response['Error']['Code'] == 'ThrottlingException':\n"
            )
            self.dag_file.write("                pass\n")
            self.dag_file.write("            else:\n")
            self.dag_file.write(
                "                print(\"DEBUG: %s\"%(err.response['Error']['Code']))\n"
            )
            self.dag_file.write("                raise err\n")
            self.dag_file.write("\n")
            self.dag_file.write("        except UnboundLocalError:\n")
            self.dag_file.write("            pass\n")
            self.dag_file.write("\n")
            self.dag_file.write("        time.sleep(15)\n")
            self.dag_file.write("\n")
            self.dag_file.write("\n")
            self.dag_file.write(
                "    # Loop to get the last message and print in the output\n"
            )
            self.dag_file.write("    lastMessagePrinted = False\n")
            self.dag_file.write("    while lastMessagePrinted == False:\n")
            self.dag_file.write("        try:\n")
            self.dag_file.write(
                "            responseStdOut = logs_client.get_log_events(\n"
            )
            self.dag_file.write("                logGroupName='dbimport',\n")
            self.dag_file.write("                logStreamName=logStreamNameStdOut,\n")
            self.dag_file.write("                nextToken=nextTokenStdOut\n")
            self.dag_file.write("                )\n")
            self.dag_file.write("\n")
            self.dag_file.write(
                "            nextTokenStdOut = responseStdOut['nextForwardToken']\n"
            )
            self.dag_file.write("            for event in responseStdOut['events']:\n")
            self.dag_file.write(
                "                if printAndFindEventErrors(event, startWithNewline = False) == True:\n"
            )
            self.dag_file.write("                    errorFound = True\n")
            # self.dag_file.write("                try:\n")
            # self.dag_file.write("                    print(str(event['message']), end=\"\")\n")
            # self.dag_file.write("                    if event['message'].find(\"\\nERROR\") != -1:\n")
            # # self.dag_file.write("                    if event['message'].find(\"ERROR\") != -1:\n")
            # self.dag_file.write("                        errorFound = True\n")
            # self.dag_file.write("                        print(\"Error Found (5) !!!!!\")\n")
            # self.dag_file.write("                        print(event)\n")
            # self.dag_file.write("                except:\n")
            # self.dag_file.write("                    print(event)\n")
            # self.dag_file.write("                    if str(event).find(\"\\nERROR\") != -1:\n")
            # # self.dag_file.write("                    if str(event).find(\"ERROR\") != -1:\n")
            # self.dag_file.write("                        errorFound = True\n")
            # self.dag_file.write("                        print(\"Error Found (6) !!!!!\")\n")
            self.dag_file.write("\n")
            self.dag_file.write("            lastMessagePrinted = True\n")
            self.dag_file.write("\n")
            self.dag_file.write("            if nextTokenStdErr == None:\n")
            self.dag_file.write(
                "                responseStdErr = logs_client.get_log_events(\n"
            )
            self.dag_file.write("                    logGroupName='dbimport',\n")
            self.dag_file.write(
                "                    logStreamName=logStreamNameStdErr,\n"
            )
            self.dag_file.write("                    startFromHead=True\n")
            self.dag_file.write("                    )\n")
            self.dag_file.write("            else:\n")
            self.dag_file.write(
                "                responseStdErr = logs_client.get_log_events(\n"
            )
            self.dag_file.write("                    logGroupName='dbimport',\n")
            self.dag_file.write(
                "                    logStreamName=logStreamNameStdErr,\n"
            )
            self.dag_file.write("                    nextToken=nextTokenStdErr\n")
            self.dag_file.write("                    )\n")
            self.dag_file.write("\n")
            self.dag_file.write(
                "            nextTokenStdErr = responseStdErr['nextForwardToken']\n"
            )
            self.dag_file.write("\n")
            self.dag_file.write("            for event in responseStdErr['events']:\n")
            self.dag_file.write(
                "                if printAndFindEventErrors(event, startWithNewline = True) == True:\n"
            )
            self.dag_file.write("                    errorFound = True\n")
            # self.dag_file.write("                try:\n")
            # self.dag_file.write("                    print(str(event['message']), end=\"\")\n")
            # self.dag_file.write("                    if event['message'].find(\"\\nERROR\") != -1:\n")
            # # self.dag_file.write("                    if event['message'].find(\"ERROR\") != -1:\n")
            # self.dag_file.write("                        errorFound = True\n")
            # self.dag_file.write("                        print(\"Error Found (5) !!!!!\")\n")
            # self.dag_file.write("                        print(event)\n")
            # self.dag_file.write("                except:\n")
            # self.dag_file.write("                    print(event)\n")
            # self.dag_file.write("                    if str(event).find(\"\\nERROR\") != -1:\n")
            # # self.dag_file.write("                    if str(event).find(\"ERROR\") != -1:\n")
            # self.dag_file.write("                        errorFound = True\n")
            # self.dag_file.write("                        print(\"Error Found (6) !!!!!\")\n")
            self.dag_file.write("\n")
            self.dag_file.write(
                "        except botocore.exceptions.ClientError as err:\n"
            )
            self.dag_file.write(
                "            if err.response['Error']['Code'] == 'ResourceNotFoundException':\n"
            )
            self.dag_file.write("                pass\n")
            self.dag_file.write(
                "            elif err.response['Error']['Code'] == 'ThrottlingException':\n"
            )
            self.dag_file.write("                pass\n")
            self.dag_file.write("            else:\n")
            self.dag_file.write(
                "                print(\"DEBUG: %s\"%(err.response['Error']['Code']))\n"
            )
            self.dag_file.write("                raise err\n")
            self.dag_file.write("\n")
            self.dag_file.write("        except UnboundLocalError:\n")
            self.dag_file.write("            pass\n")
            self.dag_file.write("\n")
            self.dag_file.write("        time.sleep(1)\n")
            self.dag_file.write("\n")
            # self.dag_file.write("    print(response)\n")
            # self.dag_file.write("    print('Sequence - 008')\n")
            # self.dag_file.write("    print(errorFound)\n")
            # self.dag_file.write("    print(commandInvocation_response)\n")
            # self.dag_file.write("    if commandStatus != 'Success':\n")
            self.dag_file.write("    if errorFound == True:\n")
            # self.dag_file.write("        print('Raising error as errorFound = True')\n")
            self.dag_file.write("        raise\n")
            self.dag_file.write("\n")
            # self.dag_file.write("    print('AWS ssm send-command exit')\n")
            self.dag_file.write("\n")

        self.dag_file.write("\n")
        self.dag_file.write("default_args = {\n")
        self.dag_file.write("    'owner': 'airflow',\n")
        self.dag_file.write("    'depends_on_past': False,\n")
        self.dag_file.write(
            "    'start_date': datetime(2017, 1, 1, 0, 0, tzinfo=local_tz),\n"
        )
        self.dag_file.write("    'max_active_runs': 1,\n")

        if email != None and email.strip() != "":
            email = email.split(",")
            self.dag_file.write("    'email': %s,\n" % (email))
        else:
            self.dag_file.write("    'email': Email_receiver,\n")

        if email_on_failure == 1:
            self.dag_file.write("    'email_on_failure': True,\n")
        else:
            self.dag_file.write("    'email_on_failure': False,\n")

        if email_on_retries == 1:
            self.dag_file.write("    'email_on_retry': True,\n")
        else:
            self.dag_file.write("    'email_on_retry': False,\n")

        if retry_exponential_backoff == 1:
            self.dag_file.write("    'retry_exponential_backoff': True,\n")
        else:
            self.dag_file.write("    'retry_exponential_backoff': False,\n")

        self.dag_file.write("    'retries': 0,\n")
        self.dag_file.write("    'pool': '%s',\n" % (default_pool))
        self.dag_file.write("    'retry_delay': timedelta(minutes=5),\n")

        if sla_warning_time != None and type(sla_warning_time) is time:
            self.dag_file.write(
                f"    'sla': timedelta(hours={sla_warning_time.hour}, minutes={sla_warning_time.minute}, seconds={sla_warning_time.second}),\n"
            )

        self.dag_file.write("}\n")
        self.dag_file.write("\n")
        self.dag_file.write("dag = DAG(\n")
        self.dag_file.write("    '%s',\n" % (dag_name))
        self.dag_file.write("    default_args=default_args,\n")
        self.dag_file.write("    description='%s',\n" % (dag_name))
        self.dag_file.write("    catchup=False,\n")

        if tags != None and tags.strip() != "":
            self.dag_file.write(
                "    tags=[%s],\n"
                % (
                    ", ".join(
                        [f'"{value}"' for value in [x.strip() for x in tags.split(",")]]
                    )
                )
            )

        try:
            if int(concurrency) > 0:
                self.dag_file.write(f"    concurrency={int(concurrency)},\n")
        except ValueError:
            pass

        self.dag_file.write("    schedule_interval=%s)\n" % (cron_schedule))

        self.dag_file.write("\n")
        if self.airflow_mode == "default":
            self.dag_file.write("start = BashOperator(\n")
            self.dag_file.write("    task_id='start',\n")
            if self.task_queue_for_dummy != None:
                self.dag_file.write(
                    "    queue='%s',\n" % (self.task_queue_for_dummy.strip())
                )
            self.dag_file.write(
                "    bash_command='%sbin/manage --checkAirflowExecution --airflowDAG=%s',\n"
                % (self.get_dbimport_command_path(sudo_user=sudo_user), dag_name)
            )
            self.dag_file.write("    priority_weight=100,\n")
            self.dag_file.write("    weight_rule='absolute',\n")
            self.dag_file.write("    pool=None,\n")
            self.dag_file.write("    dag=dag)\n")
            self.dag_file.write("\n")

            self.dag_file.write("stop = BashOperator(\n")
            self.dag_file.write("    task_id='stop',\n")
            if self.task_queue_for_dummy != None:
                self.dag_file.write(
                    "    queue='%s',\n" % (self.task_queue_for_dummy.strip())
                )
            self.dag_file.write(
                "    bash_command='%sbin/manage --sendAirflowStopMessage --airflowDAG=%s',\n"
                % (self.get_dbimport_command_path(sudo_user=sudo_user), dag_name)
            )
            self.dag_file.write("    priority_weight=100,\n")
            self.dag_file.write("    weight_rule='absolute',\n")
            self.dag_file.write("    pool=None,\n")
            self.dag_file.write("    dag=dag)\n")
            self.dag_file.write("\n")

        elif self.airflow_mode == "aws_mwaa":
            self.dag_file.write("start = PythonOperator(\n")
            self.dag_file.write("    task_id='start',\n")
            if self.task_queue_for_dummy != None:
                self.dag_file.write(
                    "    queue='%s',\n" % (self.task_queue_for_dummy.strip())
                )
            self.dag_file.write("    python_callable=startDBImportExecution,\n")
            self.dag_file.write(
                '    op_kwargs={"command": "manage --checkAirflowExecution --airflowDAG=%s", "instanceID": "%s"},\n'
                % (dag_name, self.get_aws_instance_id("start"))
            )
            self.dag_file.write("    priority_weight=100,\n")
            self.dag_file.write("    weight_rule='absolute',\n")
            self.dag_file.write("    pool=None,\n")
            self.dag_file.write("    dag=dag)\n")
            self.dag_file.write("\n")

            self.dag_file.write("stop = PythonOperator(\n")
            self.dag_file.write("    task_id='stop',\n")
            if self.task_queue_for_dummy != None:
                self.dag_file.write(
                    "    queue='%s',\n" % (self.task_queue_for_dummy.strip())
                )
            self.dag_file.write("    python_callable=startDBImportExecution,\n")
            self.dag_file.write(
                '    op_kwargs={"command": "manage --checkAirflowExecution --airflowDAG=%s", "instanceID": "%s"},\n'
                % (dag_name, self.get_aws_instance_id("stop"))
            )
            self.dag_file.write("    priority_weight=100,\n")
            self.dag_file.write("    weight_rule='absolute',\n")
            self.dag_file.write("    pool=None,\n")
            self.dag_file.write("    dag=dag)\n")
            self.dag_file.write("\n")

        if self.airflow_major_version == 1:
            self.dag_file.write("def always_trigger(context, dag_run_obj):\n")
            self.dag_file.write("    return dag_run_obj\n")
            self.dag_file.write("\n")

        if import_phase_finish_first == True:
            self.dag_file.write("Import_Phase_Finished = DummyOperator(\n")
            self.dag_file.write("    task_id='Import_Phase_Finished',\n")
            if self.task_queue_for_dummy != None:
                self.dag_file.write(
                    "    queue='%s',\n" % (self.task_queue_for_dummy.strip())
                )
            self.dag_file.write("    priority_weight=100,\n")
            self.dag_file.write("    weight_rule='absolute',\n")
            self.dag_file.write("    dag=dag)\n")
            self.dag_file.write("\n")

        airflow_dag_sensors = aliased(configSchema.AirflowDAGSensors, name="ads")
        sensors = (
            session.query(airflow_dag_sensors.dag_name)
            .select_from(airflow_dag_sensors)
            .filter(airflow_dag_sensors.dag_name == dag_name)
            .count()
        )

        if sensors > 0:
            tasks_sensors_exists = True

        airflow_tasks = aliased(configSchema.AirflowTasks)
        tasks = pd.DataFrame(
            session.query(airflow_tasks.dag_name, airflow_tasks.placement)
            .select_from(airflow_tasks)
            .filter(airflow_tasks.dag_name == dag_name)
            .filter(airflow_tasks.include_in_airflow == 1)
            .all()
        )

        if len(tasks) == 0:
            tasks = pd.DataFrame(columns=["dag_name", "placement"])

        if (
            len(tasks.loc[tasks["placement"] == "before main"]) > 0
            or self.create_pools_with_tasks == True
        ):
            tasks_before_main_exists = True

        if len(tasks.loc[tasks["placement"] == "after main"]) > 0:
            tasks_after_main_exists = True

        if tasks_before_main_exists:
            self.dag_file.write(f"{self.pre_stop_task} = DummyOperator(\n")
            self.dag_file.write("    task_id='%s',\n" % (self.pre_stop_task))
            if self.task_queue_for_dummy != None:
                self.dag_file.write(
                    "    queue='%s',\n" % (self.task_queue_for_dummy.strip())
                )
            self.dag_file.write("    priority_weight=100,\n")
            self.dag_file.write("    weight_rule='absolute',\n")
            self.dag_file.write("    dag=dag)\n")
            self.dag_file.write("\n")

        if tasks_after_main_exists:
            self.dag_file.write(f"{self.post_start_task} = DummyOperator(\n")
            self.dag_file.write("    task_id='%s',\n" % (self.post_start_task))
            if self.task_queue_for_dummy != None:
                self.dag_file.write(
                    "    queue='%s',\n" % (self.task_queue_for_dummy.strip())
                )
            self.dag_file.write("    priority_weight=100,\n")
            self.dag_file.write("    weight_rule='absolute',\n")
            self.dag_file.write("    dag=dag)\n")
            self.dag_file.write("\n")

        if tasks_sensors_exists:
            self.dag_file.write(f"{self.sensor_stop_task} = DummyOperator(\n")
            self.dag_file.write("    task_id='%s',\n" % (self.sensor_stop_task))
            if self.task_queue_for_dummy != None:
                self.dag_file.write(
                    "    queue='%s',\n" % (self.task_queue_for_dummy.strip())
                )
            self.dag_file.write("    priority_weight=100,\n")
            self.dag_file.write("    weight_rule='absolute',\n")
            self.dag_file.write("    dag=dag)\n")
            self.dag_file.write("\n")

        if not tasks_sensors_exists and tasks_before_main_exists:
            self.main_start_task = self.pre_stop_task

        if tasks_sensors_exists and not tasks_before_main_exists:
            self.main_start_task = self.sensor_stop_task

        if tasks_sensors_exists and tasks_before_main_exists:
            self.pre_start_task = self.sensor_stop_task
            self.main_start_task = self.pre_stop_task

        if tasks_after_main_exists:
            self.main_stop_task = self.post_start_task

        session.close()

        log.debug("sensorStartTask = %s", self.sensor_start_task)
        log.debug("sensorStopTask = %s", self.sensor_stop_task)
        log.debug("preStartTask = %s", self.pre_start_task)
        log.debug("preStopTask = %s", self.pre_stop_task)
        log.debug("mainStartTask = %s", self.main_start_task)
        log.debug("mainStopTask = %s", self.main_stop_task)
        log.debug("postStartTask = %s", self.post_start_task)
        log.debug("postStopTask = %s", self.post_stop_task)

    def close_dag_file(self):
        log = logging.getLogger(self.logger)
        self.dag_file.close()

        try:
            os.chmod(self.dag_filename, int(self.dag_file_permission, 8))
        except PermissionError:
            log.warning(
                "Could not change file mode to '%s'" % (self.dag_file_permission)
            )

        try:
            shutil.chown(self.dag_filename, group=self.dag_file_group)
        except PermissionError:
            log.warning(
                "Could not change group owner of file to '%s'. Permission Denied"
                % (self.dag_file_group)
            )
        except LookupError:
            log.warning(
                "Could not change group owner of file to '%s'. Group does not exists"
                % (self.dag_file_group)
            )

        if self.write_dag == True:
            if self.airflow_mode == "default":
                try:
                    shutil.copy(self.dag_filename, self.dag_filename_in_airflow)
                    os.chmod(
                        self.dag_filename_in_airflow, int(self.dag_file_permission, 8)
                    )
                    shutil.chown(
                        self.dag_filename_in_airflow, group=self.dag_file_group
                    )
                except PermissionError:
                    log.warning(
                        "Could not copy and change permission of file '%s'. Permission Denied",
                        self.dag_filename_in_airflow,
                    )
                except LookupError:
                    pass
                else:
                    print("DAG file written to %s" % (self.dag_filename_in_airflow))

            elif self.airflow_mode == "aws_mwaa":
                try:
                    s3client = boto3.client(
                        "s3", region_name=self.common_config.aws_region
                    )
                    # response = s3client.upload_file(self.dag_filename, "dl-mwaa-artifacts-uat-eu-west-1-176686534647", self.dag_filename_in_aws_mwaa)
                    response = s3client.upload_file(
                        self.dag_filename,
                        self.dag_directory,
                        self.dag_filename_in_aws_mwaa,
                    )
                except botocore.exceptions.ClientError as err:
                    log.error(err)
                except boto3.exceptions.S3UploadFailedError as err:
                    log.error(err)
                except botocore.exceptions.ParamValidationError as err:
                    log.error(err)

        else:
            print("DAG file written to %s" % (self.dag_filename))

    def add_failure_task(self, task_name):
        log = logging.getLogger(self.logger)
        self.dag_file.write("%s = BashOperator(\n" % (task_name))
        self.dag_file.write("    task_id='%s',\n" % (task_name))
        self.dag_file.write("    bash_command='exit 1 ',\n")
        self.dag_file.write("    priority_weight=1,\n")
        self.dag_file.write("    weight_rule='absolute',\n")
        self.dag_file.write("    dag=dag)\n")
        self.dag_file.write("\n")

    def add_tasks_to_dag_file(
        self, dag_name, main_dag_schedule, default_retries=0, default_sudo_user=""
    ):
        log = logging.getLogger(self.logger)

        # session = self.config_db_session()
        session = self.get_dbimport_session()

        if default_sudo_user == None:
            default_sudo_user = ""

        airflow_tasks = aliased(configSchema.AirflowTasks)

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
            .filter(airflow_tasks.dag_name == dag_name)
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

        for _index, row in tasks.iterrows():
            sensor_poke_interval = row["sensor_poke_interval"]
            task_name = row["task_name"]

            sudo_user = row["sudo_user"]
            if sudo_user == None or sudo_user == "":
                sudo_user = default_sudo_user

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
                airflow_custom_dags = aliased(configSchema.AirflowCustomDAGs)
                airflow_etl_dags = aliased(configSchema.AirflowETLDAGs)
                airflow_export_dags = aliased(configSchema.AirflowExportDAGs)
                airflow_import_dags = aliased(configSchema.AirflowImportDAGs)

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
                    log.warning("Issue on DAG '%s'" % (dag_name))
                    log.warning("There is no schedule interval for DAG to wait for")
                    log.warning(
                        "The DAG Sensor will not be added to the DAG. Instead, there will be a Task that always failes. The DAG will never execute"
                    )

                    row["task_name"] = "%s_FAILURE" % (row["task_name"])
                    self.add_failure_task(row["task_name"])
                    add_dag_sensor = False
                # 					continue
                # 					log.error("Cant find schedule interval for DAG to wait for")
                # 					self.dag_file.close()
                # 					self.common_config.remove_temporary_files()
                # 					sys.exit(1)

                main_dag_match_hour_min = re.search(
                    "^[0-2][0-9]:[0-5][0-9]$", main_dag_schedule
                )
                wait_dag_match_hour_min = re.search(
                    "^[0-2][0-9]:[0-5][0-9]$", wait_dag_schedule
                )

                time_diff = "0"

                if (
                    main_dag_match_hour_min is None
                    and wait_dag_match_hour_min is not None
                ) or (
                    main_dag_match_hour_min is not None
                    and wait_dag_match_hour_min is None
                ):
                    log.warning("Issue on DAG '%s'" % (dag_name))
                    log.warning(
                        "Both the current DAG and the DAG the sensor is waiting for must have the same scheduling format (HH:MM or cron)"
                    )
                    log.warning(
                        "The DAG Sensor will not be added to the DAG. Instead, there will be a Task that always failes. The DAG will never execute"
                    )
                    row["task_name"] = "%s_FAILURE" % (row["task_name"])
                    self.add_failure_task(row["task_name"])
                    add_dag_sensor = False

                if main_dag_match_hour_min is not None and add_dag_sensor:
                    # Time is in HH:MM format for both DAG's. So now we can calculate the diff in time between them
                    noon = datetime.strptime("12:00", "%H:%M")
                    main_dag_schedule_date_time = datetime.strptime(
                        main_dag_schedule, "%H:%M"
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
                elif add_dag_sensor:
                    if main_dag_schedule != wait_dag_schedule:
                        log.warning("Issue on DAG '%s'" % (dag_name))
                        log.warning(
                            "When using cron or cron alias schedules for DAG sensors, the schedule time in both DAG's must match"
                        )
                        log.warning(
                            "The DAG Sensor will not be added to the DAG. Instead, there will be a Task that always failes. The DAG will never execute"
                        )
                        row["task_name"] = "%s_FAILURE" % (row["task_name"])
                        self.add_failure_task(row["task_name"])
                        add_dag_sensor = False

                if add_dag_sensor:
                    self.dag_file.write(
                        "%s = ExternalTaskSensor(\n" % (row["task_name"])
                    )
                    self.dag_file.write("    task_id='%s',\n" % (row["task_name"]))
                    self.dag_file.write("    external_dag_id='%s',\n" % (wait_for_dag))
                    self.dag_file.write(
                        "    external_task_id='%s',\n" % (wait_for_task)
                    )
                    self.dag_file.write("    retries=0,\n")
                    self.dag_file.write(
                        f"    execution_delta=timedelta(seconds={time_diff}),\n"
                    )
                    if row["airflow_pool"] != "":
                        self.dag_file.write("    pool='%s',\n" % (row["airflow_pool"]))
                    if row["airflow_priority"] != "":
                        self.dag_file.write(
                            f"    priority_weight={int(row['airflow_priority'])},\n"
                        )
                        self.dag_file.write("    weight_rule='absolute',\n")
                    else:
                        self.dag_file.write("    priority_weight=100,\n")
                        self.dag_file.write("    weight_rule='absolute',\n")
                    self.dag_file.write(f"    timeout={int(sensor_timeout_seconds)},\n")
                    self.dag_file.write(
                        f"    poke_interval={int(sensor_poke_interval)},\n"
                    )
                    self.dag_file.write("    mode='reschedule',\n")
                    if (
                        row["sensor_soft_fail"] is not None
                        and row["sensor_soft_fail"] == 1
                    ):
                        self.dag_file.write("    soft_fail=True,\n")
                    self.dag_file.write("    dag=dag)\n")
                    self.dag_file.write("\n")

            if row["task_type"] == "SQL Sensor":
                if row["sensor_connection"] == "":
                    log.error(
                        "SQL Sensors requires a valid Airflow Connection ID in column 'sensor_connection'"
                    )
                else:
                    # 					self.dag_file.close()
                    # 					self.common_config.remove_temporary_files()
                    # 					sys.exit(1)

                    sensor_poke_interval = row["sensor_poke_interval"]
                    if sensor_poke_interval == "":
                        # Default to 5 minutes
                        sensor_poke_interval = 300

                    self.dag_file.write("%s = SqlSensor(\n" % (row["task_name"]))
                    self.dag_file.write("    task_id='%s',\n" % (row["task_name"]))
                    self.dag_file.write(
                        "    conn_id='%s',\n" % (row["sensor_connection"])
                    )
                    self.dag_file.write('    sql="""%s""",\n' % (row["task_config"]))
                    if row["airflow_pool"] != "":
                        self.dag_file.write("    pool='%s',\n" % (row["airflow_pool"]))
                    if row["airflow_priority"] != "":
                        self.dag_file.write(
                            f"    priority_weight={int(row['airflow_priority'])},\n"
                        )
                        self.dag_file.write("    weight_rule='absolute',\n")
                    else:
                        self.dag_file.write("    priority_weight=100,\n")
                        self.dag_file.write("    weight_rule='absolute',\n")
                    self.dag_file.write(f"    timeout={int(sensor_timeout_seconds)},\n")
                    self.dag_file.write(
                        f"    poke_interval={int(sensor_poke_interval)},\n"
                    )
                    self.dag_file.write("    mode='reschedule',\n")
                    if (
                        row["sensor_soft_fail"] is not None
                        and row["sensor_soft_fail"] == 1
                    ):
                        self.dag_file.write("    soft_fail=True,\n")
                    self.dag_file.write("    dag=dag)\n")
                    self.dag_file.write("\n")

            if row["task_type"] == "Trigger DAG":
                self.dag_file.write(
                    "%s = TriggerDagRunOperator(\n" % (row["task_name"])
                )
                self.dag_file.write("    task_id='%s',\n" % (row["task_name"]))
                self.dag_file.write("    trigger_dag_id='%s',\n" % (row["task_config"]))
                if row["airflow_pool"] != "":
                    self.dag_file.write("    pool='%s',\n" % (row["airflow_pool"]))
                if row["airflow_priority"] != "":
                    self.dag_file.write(
                        f"    priority_weight={int(row['airflow_priority'])},\n"
                    )
                    self.dag_file.write("    weight_rule='absolute',\n")
                else:
                    self.dag_file.write("    priority_weight=100,\n")
                    self.dag_file.write("    weight_rule='absolute',\n")
                if self.airflow_major_version == 1:
                    self.dag_file.write("    python_callable=always_trigger,\n")
                self.dag_file.write("    dag=dag)\n")
                self.dag_file.write("\n")

            if row["task_type"] == "shell script":
                self.dag_file.write("%s = BashOperator(\n" % (row["task_name"]))
                self.dag_file.write("    task_id='%s',\n" % (row["task_name"]))
                self.dag_file.write("    retries=%s,\n" % (default_retries))
                self.dag_file.write("    bash_command='%s ',\n" % (row["task_config"]))
                if row["airflow_pool"] != "":
                    self.dag_file.write("    pool='%s',\n" % (row["airflow_pool"]))
                if row["airflow_priority"] != "":
                    self.dag_file.write(
                        f"    priority_weight={int(row['airflow_priority'])},\n"
                    )
                    self.dag_file.write("    weight_rule='absolute',\n")
                else:
                    self.dag_file.write("    priority_weight=1,\n")
                    self.dag_file.write("    weight_rule='absolute',\n")
                self.dag_file.write("    dag=dag)\n")
                self.dag_file.write("\n")

            if row["task_type"] == "Hive SQL Script":
                self.dag_file.write("%s = BashOperator(\n" % (row["task_name"]))
                self.dag_file.write("    task_id='%s',\n" % (row["task_name"]))
                self.dag_file.write("    retries=%s,\n" % (default_retries))

                if row["hive_db"] != None and row["hive_db"].strip() != "":
                    self.dag_file.write(
                        "    bash_command='%sbin/manage --runHiveScript=%s --hiveDB=%s ',\n"
                        % (
                            self.get_dbimport_command_path(sudo_user=sudo_user),
                            row["task_config"],
                            row["hive_db"],
                        )
                    )
                else:
                    self.dag_file.write(
                        "    bash_command='%sbin/manage --runHiveScript=%s ',\n"
                        % (
                            self.get_dbimport_command_path(sudo_user=sudo_user),
                            row["task_config"],
                        )
                    )

                if row["airflow_pool"] != "":
                    self.dag_file.write("    pool='%s',\n" % (row["airflow_pool"]))
                if row["airflow_priority"] != "":
                    self.dag_file.write(
                        f"    priority_weight={int(row['airflow_priority'])},\n"
                    )
                    self.dag_file.write("    weight_rule='absolute',\n")
                else:
                    self.dag_file.write("    priority_weight=1,\n")
                    self.dag_file.write("    weight_rule='absolute',\n")
                self.dag_file.write("    dag=dag)\n")
                self.dag_file.write("\n")

            if row["task_type"] == "Hive SQL":
                jdbc_sql = row["task_config"].replace(r"'", "\\'")
                self.dag_file.write("%s = BashOperator(\n" % (row["task_name"]))
                self.dag_file.write("    task_id='%s',\n" % (row["task_name"]))
                self.dag_file.write("    retries=%s,\n" % (default_retries))
                self.dag_file.write(
                    "    bash_command='%sbin/manage --runHiveQuery=\"%s\" ',\n"
                    % (self.get_dbimport_command_path(sudo_user=sudo_user), jdbc_sql)
                )
                if row["airflow_pool"] != "":
                    self.dag_file.write("    pool='%s',\n" % (row["airflow_pool"]))
                if row["airflow_priority"] != "":
                    self.dag_file.write(
                        f"    priority_weight={int(row['airflow_priority'])},\n"
                    )
                    self.dag_file.write("    weight_rule='absolute',\n")
                else:
                    self.dag_file.write("    priority_weight=1,\n")
                    self.dag_file.write("    weight_rule='absolute',\n")
                self.dag_file.write("    dag=dag)\n")
                self.dag_file.write("\n")

            if row["task_type"] == "JDBC SQL":
                jdbc_sql = row["task_config"].replace(r"'", "\\'")
                self.dag_file.write("%s = BashOperator(\n" % (row["task_name"]))
                self.dag_file.write("    task_id='%s',\n" % (row["task_name"]))
                self.dag_file.write("    retries=%s,\n" % (default_retries))
                self.dag_file.write(
                    "    bash_command='%sbin/manage --dbAlias=%s --runJDBCQuery=\"%s\" ',\n"
                    % (
                        self.get_dbimport_command_path(sudo_user=sudo_user),
                        row["jdbc_dbalias"],
                        jdbc_sql,
                    )
                )
                if row["airflow_pool"] != "":
                    self.dag_file.write("    pool='%s',\n" % (row["airflow_pool"]))
                if row["airflow_priority"] != "":
                    self.dag_file.write(
                        f"    priority_weight={int(row['airflow_priority'])},\n"
                    )
                    self.dag_file.write("    weight_rule='absolute',\n")
                else:
                    self.dag_file.write("    priority_weight=1,\n")
                    self.dag_file.write("    weight_rule='absolute',\n")
                self.dag_file.write("    dag=dag)\n")
                self.dag_file.write("\n")

            if row["task_type"] == "DBImport command":
                dbimport_command = row["task_config"].replace(r"'", "\\'")
                if self.airflow_mode == "default":
                    self.dag_file.write("%s = BashOperator(\n" % (row["task_name"]))
                    self.dag_file.write("    task_id='%s',\n" % (row["task_name"]))
                    self.dag_file.write("    retries=%s,\n" % (default_retries))
                    self.dag_file.write(
                        "    bash_command='%sbin/%s',\n"
                        % (
                            self.get_dbimport_command_path(sudo_user=sudo_user),
                            dbimport_command,
                        )
                    )
                    if row["airflow_pool"] != "":
                        self.dag_file.write("    pool='%s',\n" % (row["airflow_pool"]))
                    if row["airflow_priority"] != "":
                        self.dag_file.write(
                            f"    priority_weight={int(row['airflow_priority'])},\n"
                        )
                        self.dag_file.write("    weight_rule='absolute',\n")
                    else:
                        self.dag_file.write("    priority_weight=1,\n")
                        self.dag_file.write("    weight_rule='absolute',\n")
                    self.dag_file.write("    dag=dag)\n")
                    self.dag_file.write("\n")
                elif self.airflow_mode == "aws_mwaa":
                    instance_id = self.get_aws_instance_id(row["task_name"])
                    self.dag_file.write("%s = PythonOperator(\n" % (row["task_name"]))
                    self.dag_file.write("    task_id='%s',\n" % (row["task_name"]))
                    self.dag_file.write("    retries=%s,\n" % (default_retries))
                    self.dag_file.write("    python_callable=startDBImportExecution,\n")
                    self.dag_file.write(
                        '    op_kwargs={"command": "%s", "instanceID": "%s"},\n'
                        % (dbimport_command, instance_id)
                    )
                    if row["airflow_pool"] != "":
                        self.dag_file.write("    pool='%s',\n" % (row["airflow_pool"]))
                    else:
                        if self.airflow_aws_pool_to_instance_id == True:
                            self.dag_file.write("    pool='%s',\n" % (instance_id))
                        else:
                            self.dag_file.write("    pool='%s',\n" % (dag_name))
                    if row["airflow_priority"] != "":
                        self.dag_file.write(
                            f"    priority_weight={int(row['airflow_priority'])},\n"
                        )
                        self.dag_file.write("    weight_rule='absolute',\n")
                    else:
                        self.dag_file.write("    priority_weight=1,\n")
                        self.dag_file.write("    weight_rule='absolute',\n")
                    self.dag_file.write("    dag=dag)\n")
                    self.dag_file.write("\n")

            if row["placement"] == "before main":
                temp_start_task = self.pre_start_task
                temp_stop_task = self.pre_stop_task

            if row["placement"] == "in main":
                temp_start_task = self.main_start_task
                temp_stop_task = self.main_stop_task

            if row["placement"] == "after main":
                temp_start_task = self.post_start_task
                temp_stop_task = self.post_stop_task

            for _dep_index, dep_row in all_task_dependencies.iterrows():
                if (
                    row["task_name"] == dep_row["task_dependency_upstream"]
                    or row["task_name"] == dep_row["task_dependency_downstream"]
                ) and row["placement"] != dep_row["placement"]:
                    if not task_dependency_warning_printed:
                        log.warning("Issue on DAG '%s'" % (dag_name))
                        log.warning(
                            "There are custom tasks that have dependencies between 'in_main' and 'after_main'. This is not supported and the dependencies will be incorrect in Airflow"
                        )
                        task_dependency_warning_printed = True

            # if row['placement'] == "in main" or row['placement'] == "after main":
            if 1 == 1:
                # 				if row['task_dependency_in_main_downstream'] == '':
                # 					# No dependency, set to "mainStart" task, what ever that might be
                # 					taskDependencies += "%s.set_downstream(%s)\n"%(self.main_start_task, row['task_name'])
                # 				else:
                # 					# Check if there is any dependencies for this task
                # 					for dep in row['task_dependency_in_main_downstream'].split(','):
                # 						dep = dep.strip()
                # 						if dep == "main_start":
                # 							dep = self.main_start_task
                #
                # 						taskDependencies += "%s.set_downstream(%s)\n"%(dep, row['task_name'])

                down_stream_found = False
                up_stream_found = False

                for _dep_index, dep_row in all_task_dependencies.iterrows():
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
                                "Downstream dependency against '%s' wont work as it's not in the same placement as '%s'.",
                                dep,
                                row["task_name"],
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
                                "Upstream dependency against '%s' wont work as it's not in the same placement as '%s'.",
                                dep,
                                row["task_name"],
                            )
                    else:
                        # The depRow dataframe is empty. So the dependent task is not a customTask, but instead from import_tables or export_tables. So we just add it as a dependency
                        task_dependencies += "%s.set_upstream(%s)\n" % (
                            row["task_name"],
                            dep,
                        )
                        up_stream_found = True

                if not up_stream_found:
                    task_dependencies += "%s.set_downstream(%s)\n" % (
                        temp_start_task,
                        row["task_name"],
                    )

                if not down_stream_found:
                    task_dependencies += "%s.set_downstream(%s)\n" % (
                        row["task_name"],
                        temp_stop_task,
                    )

                # Check if other tasks have this task as upstream or downstream
                # 				for depIndex, depRow in allTaskDependencies.iterrows():
                # 					pass

                # 				if row['task_dependency_in_main_upstream'] == '':
                # 					# No dependency, set to "mainStart" task, what ever that might be
                # 					# taskDependencies += "%s.set_upstream(%s)\n"%(row['task_name'], self.main_start_task)
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
                # 					# taskDependencies += "%s.set_downstream(%s)\n"%(row['task_name'], self.main_stop_task)
                # 					taskDependencies += "%s.set_downstream(%s)\n"%(row['task_name'], tempStopTask)

                task_dependencies += "\n"

        self.dag_file.write(task_dependencies)
        session.close()

    def add_sensors_to_dag_file(self, dag_name, main_dag_schedule):
        log = logging.getLogger(self.logger)
        # session = self.config_db_session()
        session = self.get_dbimport_session()

        airflow_dag_sensors = aliased(configSchema.AirflowDAGSensors, name="ads")
        airflow_custom_dags = aliased(configSchema.AirflowCustomDAGs, name="acd")
        airflow_etl_dags = aliased(configSchema.AirflowETLDAGs, name="aetld")
        airflow_export_dags = aliased(configSchema.AirflowExportDAGs, name="aed")
        airflow_import_dags = aliased(configSchema.AirflowImportDAGs, name="aid")

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
            .filter(airflow_dag_sensors.dag_name == dag_name)
            .all()
        ).fillna("")

        for _index, row in sensors.iterrows():
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
                log.warning("Issue on DAG '%s'" % (dag_name))
                log.warning("There is no schedule interval for DAG to wait for")
                log.warning(
                    "The DAG Sensor will not be added to the DAG. Instead, there will be a Task that always failes. The DAG will never execute"
                )
                self.add_failure_task("%s_FAILURE" % (row["sensor_name"]))
                self.dag_file.write(
                    "%s.set_downstream(%s_FAILURE)\n"
                    % (self.sensor_start_task, row["sensor_name"])
                )
                self.dag_file.write(
                    "%s_FAILURE.set_downstream(%s)\n"
                    % (row["sensor_name"], self.sensor_stop_task)
                )
                self.dag_file.write("\n")
                continue
            # 				log.error("Cant find schedule interval for DAG to wait for")
            # 				self.dag_file.close()
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
                "^[0-2][0-9]:[0-5][0-9]$", main_dag_schedule
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
                main_dag_match_hour_min is None and wait_dag_match_hour_min is not None
            ) or (
                main_dag_match_hour_min is not None and wait_dag_match_hour_min is None
            ):
                log.warning("Issue on DAG '%s'" % (dag_name))
                log.warning(
                    "Both the current DAG and the DAG the sensor is waiting for must have the same scheduling format (HH:MM or cron)"
                )
                log.warning(
                    "The DAG Sensor will not be added to the DAG. Instead, there will be a Task that always failes. The DAG will never execute"
                )
                self.add_failure_task("%s_FAILURE" % (row["sensor_name"]))
                self.dag_file.write(
                    "%s.set_downstream(%s_FAILURE)\n"
                    % (self.sensor_start_task, row["sensor_name"])
                )
                self.dag_file.write(
                    "%s_FAILURE.set_downstream(%s)\n"
                    % (row["sensor_name"], self.sensor_stop_task)
                )
                self.dag_file.write("\n")
                continue
            # 				self.dag_file.close()
            # 				self.common_config.remove_temporary_files()
            # 				sys.exit(1)

            if main_dag_match_hour_min is not None:
                # Time is in HH:MM format for both DAG's. So now we can calculate the diff in time between them
                noon = datetime.strptime("12:00", "%H:%M")
                main_dag_schedule_date_time = datetime.strptime(
                    main_dag_schedule, "%H:%M"
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
                if main_dag_schedule != wait_dag_schedule:
                    log.warning("Issue on DAG '%s'" % (dag_name))
                    log.warning(
                        "When using cron or cron alias schedules for DAG sensors, the schedule time in both DAG's must match"
                    )
                    log.warning(
                        "The DAG Sensor will not be added to the DAG. Instead, there will be a Task that always failes. The DAG will never execute"
                    )
                    self.add_failure_task("%s_FAILURE" % (row["sensor_name"]))
                    self.dag_file.write(
                        "%s.set_downstream(%s_FAILURE)\n"
                        % (self.sensor_start_task, row["sensor_name"])
                    )
                    self.dag_file.write(
                        "%s_FAILURE.set_downstream(%s)\n"
                        % (row["sensor_name"], self.sensor_stop_task)
                    )
                    self.dag_file.write("\n")
                    continue
            # 					self.dag_file.close()
            # 					self.common_config.remove_temporary_files()
            # 					sys.exit(1)

            self.dag_file.write("%s = ExternalTaskSensor(\n" % (row["sensor_name"]))
            self.dag_file.write("    task_id='%s',\n" % (row["sensor_name"]))
            self.dag_file.write("    external_dag_id='%s',\n" % (row["wait_for_dag"]))
            self.dag_file.write("    external_task_id='%s',\n" % (wait_for_task))
            self.dag_file.write("    retries=0,\n")
            # 			self.dag_file.write("    execution_timeout=timedelta(seconds=%s),\n"%(timeoutSeconds))
            self.dag_file.write(
                f"    execution_delta=timedelta(seconds={time_diff}),\n"
            )
            self.dag_file.write(f"    timeout={int(timeout_seconds)},\n")
            self.dag_file.write("    poke_interval=300,\n")
            self.dag_file.write("    mode='reschedule',\n")
            if row["sensor_soft_fail"] is not None and row["sensor_soft_fail"] == 1:
                self.dag_file.write("    soft_fail=True,\n")
            self.dag_file.write("    dag=dag)\n")
            self.dag_file.write("\n")
            self.dag_file.write(
                "%s.set_downstream(%s)\n" % (self.sensor_start_task, row["sensor_name"])
            )
            self.dag_file.write(
                "%s.set_downstream(%s)\n" % (row["sensor_name"], self.sensor_stop_task)
            )
            self.dag_file.write("\n")

        session.close()

    def send_json(self, airflow_dag, status):
        """Sends a start JSON document to REST and/or Kafka"""
        log = logging.getLogger(self.logger)
        log.debug("Executing Airflow.send_start_json()")

        self.post_airflow_dag_operations = self.common_config.get_config_value(
            key="post_airflow_dag_operations"
        )
        self.post_data_to_rest = self.common_config.get_config_value(
            key="post_data_to_rest"
        )
        self.post_data_to_kafka = self.common_config.get_config_value(
            key="post_data_to_kafka"
        )

        # 		self.post_data_to_rest = True

        if airflow_dag == None:
            return

        if self.post_airflow_dag_operations == False:
            log.info(
                "Airflow DAG post to Kafka and/or Rest have been disabled. No message will be sent"
            )
            return

        json_data = {}
        json_data["type"] = "airflow_dag"
        json_data["status"] = status
        json_data["dag"] = airflow_dag
        json_data["airflow_timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")

        if self.post_data_to_kafka == True:
            result = self.send_statistics.publish_kafka_data(json.dumps(json_data))
            if not result:
                log.warning("Kafka publish failed! No start message posted")

        if self.post_data_to_rest == True:
            response = self.send_statistics.send_rest_data(json.dumps(json_data))
            if response != 200:
                log.warning("REST call failed! No start message posted")

        log.debug("Executing Airflow.send_start_json() - Finished")
