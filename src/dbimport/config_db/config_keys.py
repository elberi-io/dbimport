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
"""The keys of the `configuration` table (ADR-0003).

`KEYS` is the only list of them. `setup` seeds the table from it,
`get_config_value` accepts only its keys, the REST model
`Configuration` has a field for every key with `rest` set, and the key
table in docs/configuration.rst is generated from it.

A bool key is stored as 0 or 1 in `valueInt`, an int key in `valueInt`
and a str key in `valueStr`.
"""

import secrets
from collections.abc import Callable
from dataclasses import dataclass
from typing import Literal


def _new_secret_key() -> str:
    return secrets.token_urlsafe(48)


@dataclass(frozen=True)
class ConfigKey:
    """A key: its kind, the value a new installation gets (a function is
    called once per installation), its description, and whether the REST
    API shows and changes it."""

    name: str
    kind: Literal["bool", "int", "str"]
    default: int | str | Callable[[], str]
    description: str
    rest: bool = True

    @property
    def column(self) -> Literal["valueInt", "valueStr"]:
        return "valueStr" if self.kind == "str" else "valueInt"

    def initial_value(self) -> int | str:
        """The value to seed: the default, or a new value from its function."""
        if callable(self.default):
            return self.default()
        return self.default


KEYS: tuple[ConfigKey, ...] = (
    ConfigKey(
        "airflow_aws_instanceids",
        "str",
        "i-xxxxxxxxxxxxxxxxx",
        "AWS Instance IDs to start the DBImport commands on. Comma-separated list of instance IDs. Tasks will be randomly assigned to one of the EMR clusters specified.",
    ),
    ConfigKey(
        "airflow_aws_pool_to_instanceid",
        "bool",
        True,
        "When using AWS, should the Airflow pool be set to the same name as the instance id?",
    ),
    ConfigKey(
        "airflow_create_pool_with_task",
        "bool",
        True,
        "If 1, then the Airflow pool will be created with standard tasks. If 0, then a direct database connection is required to add the pool through sql commands directly the airflow database. Connection details are available in dbimport.cfg for this.",
    ),
    ConfigKey(
        "airflow_dag_directory",
        "str",
        "/usr/local/airflow/dags",
        "Airflow path to DAG directory. When using AWS MWAA, this is the name of the S3 bucket.",
    ),
    ConfigKey(
        "airflow_dag_file_group", "str", "airflow", "Group owner of created DAG file"
    ),
    ConfigKey(
        "airflow_dag_file_permission",
        "str",
        "660",
        "File permission of created DAG file",
    ),
    ConfigKey(
        "airflow_dag_staging_directory",
        "str",
        "/usr/local/airflow/dags_generated_from_dbimport",
        "Airflow path to staging DAG directory",
    ),
    ConfigKey(
        "airflow_dbimport_commandpath",
        "str",
        "sudo -iu ${SUDO_USER} /usr/local/dbimport/",
        "This is the path to DBImport. If sudo is required, this can be added here aswell. Use the variable ${SUDO_USER} instead of hardcoding the sudo username. Must end with a /",
    ),
    ConfigKey(
        "airflow_default_pool_size",
        "int",
        24,
        "If DBImport creates the Airflow pool, this is the initial size of it",
    ),
    ConfigKey(
        "airflow_disable",
        "bool",
        False,
        'Disable All executions from Airflow. This is what the "start" Task is looking at',
    ),
    ConfigKey(
        "airflow_dummy_task_queue",
        "str",
        "default",
        "Queue to use for dummy tasks (stop, stage_one_complete and more)",
    ),
    ConfigKey(
        "airflow_major_version",
        "int",
        2,
        "What is the major version of Airflow? 1 or 2 is valid options. Controls how the DAG files are generated",
    ),
    ConfigKey(
        "airflow_sudo_user",
        "str",
        "dbimport",
        "What user will Airflow sudo to for executing DBImport. This value will replace the ${SUDO_USER} variable in airflow_dbimport_commandpath setting",
    ),
    ConfigKey("airflow_url", "str", "http://localhost:8080", "URL to Airflow webUI"),
    ConfigKey(
        "atlas_discovery_interval",
        "int",
        24,
        "How many hours there should pass between each Atlas discovery of a jdbc connection",
    ),
    ConfigKey("cluster_name", "str", "hadoopcluster", "Name of Hadoop cluster"),
    ConfigKey(
        "export_default_sessions",
        "int",
        4,
        "How many SQL sessions should be used for tables that have never been exported before",
    ),
    ConfigKey(
        "export_max_sessions",
        "int",
        32,
        "The maximum number of SQL sessions to use during export",
    ),
    ConfigKey(
        "export_stage_disable",
        "bool",
        False,
        "With 1, you prevent new Export tasks from starting and running tasks will stop after the current stage is completed.",
    ),
    ConfigKey(
        "export_staging_database",
        "str",
        "etl_export_staging",
        "Name of staging database to use during Exports",
    ),
    ConfigKey(
        "export_start_disable",
        "bool",
        False,
        "With 1, you prevent new Export tasks from starting. Running tasks will be completed",
    ),
    ConfigKey("hdfs_address", "str", "hdfs://hadoopcluster:8020", "Address to HDFS"),
    ConfigKey(
        "hdfs_basedir",
        "str",
        "/apps/dbimport",
        "The base dir to write data to. Example /apps/dbimport",
    ),
    ConfigKey(
        "hdfs_blocksize",
        "str",
        "134217728",
        "The HDFS blocksize in bytes. Can usually be found in /etc/hadoop/conf/hdfs-site.xml (search for dfs.blocksize)",
    ),
    ConfigKey(
        "hive_acid_with_clusteredby",
        "bool",
        False,
        "If 1, then ACID tables will be created with a clustered by option based on the PK. Not required with Hive3 and later",
    ),
    ConfigKey(
        "hive_insert_only_tables",
        "bool",
        True,
        "If 1, then the non-merge tables in Hive will be ACID insert-only",
    ),
    ConfigKey(
        "hive_major_compact_after_merge",
        "bool",
        False,
        "With 1, DBImport will run a major compaction after the merge operations is completed",
    ),
    ConfigKey(
        "hive_print_messages",
        "bool",
        False,
        "With 1, Hive will print additional messages during SQL operations",
    ),
    ConfigKey(
        "hive_remove_locks_by_force",
        "bool",
        False,
        "With 1, DBImport will remove Hive locks before import by force",
    ),
    ConfigKey(
        "hive_validate_before_execution",
        "bool",
        True,
        "With 1, DBImport will run a group by query agains the validate table and verify the result against reference values hardcoded in DBImport",
    ),
    ConfigKey(
        "hive_validate_table",
        "str",
        "dbimport.validate_table",
        "The table to run the validate query against",
    ),
    ConfigKey(
        "impala_invalidate_metadata",
        "bool",
        False,
        "If 1, then DBImport will connect to Impala and invalidate or refresh the metadata after import is completed",
    ),
    ConfigKey(
        "import_columnname_delete",
        "str",
        "datalake_delete",
        "Column name for timestamp column with delete date that is added to table during merge operations. Only exists if soft-delete is enabled for the import",
    ),
    ConfigKey(
        "import_columnname_histtime",
        "str",
        "datalake_timestamp",
        "Column name for timestamp column added to history table to contain information about when the insert, update or delete operation was done",
    ),
    ConfigKey(
        "import_columnname_import",
        "str",
        "datalake_import",
        "Column name for timestamp column added to table during none-merge operations",
    ),
    ConfigKey(
        "import_columnname_insert",
        "str",
        "datalake_insert",
        "Column name for timestamp column with insert time that is added to table during merge operations",
    ),
    ConfigKey(
        "import_columnname_iud",
        "str",
        "datalake_iud",
        "Column name for char(1) column added to table during merge operations with information if the last operation is an insert, update or delete",
    ),
    ConfigKey(
        "import_columnname_source",
        "str",
        "dbimport_source",
        "Column name for source information if that is configured in jdbc_connections table",
    ),
    ConfigKey(
        "import_columnname_update",
        "str",
        "datalake_update",
        "Column name for timestamp column with update time that is added to table during merge operations",
    ),
    ConfigKey(
        "import_default_sessions",
        "int",
        4,
        "How many SQL sessions should be used for tables that have never been imported before",
    ),
    ConfigKey(
        "import_history_database",
        "str",
        "{DATABASE}",
        "Name of history database to use during Imports. {DATABASE} is supported within the name and will be replaced during import with the name of the import database.",
    ),
    ConfigKey(
        "import_history_table",
        "str",
        "{TABLE}_history",
        'Name of history table to use during Imports. Both {DATABASE} and {TABLE} is supported within the name and will be replaced during import with the name of the import database and table.  Both {DATABASE} and {TABLE} must be used once, but {DATABASE} could be specified in the "import_history_database" setting instead',
    ),
    ConfigKey(
        "import_max_sessions",
        "int",
        32,
        "The maximum number of SQL sessions to use during imports",
    ),
    ConfigKey(
        "import_process_empty",
        "bool",
        False,
        "If 1, then the import will do a full processing of import even if they contain no data.",
    ),
    ConfigKey(
        "import_stage_disable",
        "bool",
        False,
        "With 1, you prevent new tasks from starting and running Import tasks will stop after the current stage is completed.",
    ),
    ConfigKey(
        "import_staging_database",
        "str",
        "etl_import_staging",
        "Name of staging database to use during Imports. {DATABASE} is supported within the name and will be replaced during import with the name of the import database.",
    ),
    ConfigKey(
        "import_staging_table",
        "str",
        "{DATABASE}__{TABLE}__staging",
        'Name of staging table to use during Imports. Both {DATABASE} and {TABLE} is supported within the name and will be replaced during import with the name of the import database and table.  Both {DATABASE} and {TABLE} must be used once, but {DATABASE} could be specified in the "import_staging_database" setting instead',
    ),
    ConfigKey(
        "import_start_disable",
        "bool",
        False,
        "With 1, you prevent new Import tasks from starting. Running tasks will be completed",
    ),
    ConfigKey(
        "import_work_database",
        "str",
        "etl_import_staging",
        "Name of work database to use during Imports. This is used for internal tables and should not be accessed by end-users. {DATABASE} is supported within the name and will be replaced during import with the name of the import database.",
    ),
    ConfigKey(
        "import_work_table",
        "str",
        "{DATABASE}__{TABLE}__staging",
        'Prefix-name of work tables to use during Imports. These tables is used for internal DBImport tables and should not be access by end-users. Both {DATABASE} and {TABLE} is supported within the name and will be replaced during import with the name of the import database and table.  Both {DATABASE} and {TABLE} must be used once, but {DATABASE} could be specified in the "import_work_database" setting instead',
    ),
    ConfigKey(
        "kafka_brokers",
        "str",
        "localhost:9092",
        "Comma separeted list of Kafka brokers",
    ),
    ConfigKey("kafka_saslmechanism", "str", "GSSAPI", "Kafka SASL mechanism"),
    ConfigKey("kafka_securityprotocol", "str", "SASL_SSL", "Kafka Security Protocol"),
    ConfigKey(
        "kafka_topic", "str", "dbimport_topic", "Kafka topic to send the data to"
    ),
    ConfigKey(
        "kafka_trustcafile",
        "str",
        "/etc/pki/tls/certs/ca-bundle.crt",
        "Kafka CA Trust file for SSL",
    ),
    ConfigKey(
        "post_airflow_dag_operations",
        "bool",
        False,
        "Post start and stop activities for Airflow DAGs to Kafka and/or Rest, depending on what is enabled",
    ),
    ConfigKey(
        "post_data_to_kafka_extended",
        "bool",
        False,
        "Enable extended statistics in Kafka data",
    ),
    ConfigKey(
        "post_data_to_kafka",
        "bool",
        False,
        "Enable the Kafka endpoint to be able to receive information regarding completed imports and exports",
    ),
    ConfigKey(
        "post_data_to_rest_extended",
        "bool",
        False,
        "Enable extended statistics in the REST data",
    ),
    ConfigKey(
        "post_data_to_rest",
        "bool",
        False,
        "Enable the REST endpoint to be able to receive information regarding completed imports and exports",
    ),
    ConfigKey(
        "post_data_to_awssns_extended",
        "bool",
        False,
        "Enable extended statistics in AWS SNS data",
    ),
    ConfigKey(
        "post_data_to_awssns",
        "bool",
        False,
        "Enable the AWS SNS endpoint to be able to receive information regarding completed imports and exports",
    ),
    ConfigKey(
        "post_data_to_awssns_topic",
        "str",
        "arn:aws:sns:region:account:topic",
        "Full ARN to AWS SNS Topic where the json message will be sent",
    ),
    ConfigKey(
        "restserver_admin_user",
        "str",
        "admin",
        "Username of administrator on the REST server with all permission",
    ),
    ConfigKey(
        "restserver_authentication_method",
        "str",
        "local",
        "REST server authentication method. The following options are supported: local, pam",
    ),
    ConfigKey(
        "restserver_secret_key",
        "str",
        _new_secret_key,
        "Secret key used for oauth2 tokens",
        rest=False,
    ),
    ConfigKey(
        "restserver_token_ttl",
        "int",
        120,
        "Time-to-live, in minutes, that the oauth2 tokens are valid",
    ),
    ConfigKey("rest_timeout", "int", 5, "Timeout for the REST call"),
    ConfigKey(
        "rest_trustcafile",
        "str",
        "/etc/pki/tls/certs/ca-bundle.crt",
        "REST CA Trust file for SSL",
    ),
    ConfigKey("rest_url", "str", "https://localhost:8443/dbimport", "Rest server URL"),
    ConfigKey(
        "rest_verifyssl", "bool", True, "Verify SSL certificate during REST call"
    ),
    ConfigKey(
        "spark_max_executors",
        "int",
        128,
        "The default value for maximum number of spark executors",
    ),
    ConfigKey(
        "timezone",
        "str",
        "UTC",
        "The timezone that the configured times are meant for (example is Europe/Stockholm) ",
    ),
    ConfigKey(
        "timestamp_with_timezone",
        "bool",
        False,
        "With Spark as the ETL engine, it's possible to save timestamp columns with or without the timezone",
    ),
)

BY_NAME = {key.name: key for key in KEYS}
