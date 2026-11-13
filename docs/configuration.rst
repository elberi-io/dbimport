Configuration
=============

.. note:: The current version of DBImport dont have the webUI available. That means that the most of the configuration that needs to be done have to be directly in the MySQL database. Current users of DBImport use HeidiSQL_ as the client tool to change the configuration in the database

.. _HeidiSQL: https://www.heidisql.com/

Global configurations
---------------------

There are two places where global configurations are handled. The conf/dbimport.cfg configuration file and the configuration_ table. All available options in the configuration file is already present in the file, so for a new installation, go over them all and set them according to you environment. An environment variable *DBIMPORT_<SECTION>__<KEY>* (for example *DBIMPORT_DATABASE__MYSQL_PASSWORD*) overrides the value in the file. For the configuration_ table, the following options exists.

============================== ======== ===========================================================================================================================================================================
airflow_dag_directory          valueStr Airflow path to DAG directory
airflow_dag_file_group         valueStr Group owner of created DAG file
airflow_dag_file_permission    valueStr File permission of created DAG file
airflow_dag_staging_directory  valueStr Airflow path to staging DAG directory
airflow_dbimport_commandpath   valueStr This is the path to DBImport. If sudo is required, this can be added here aswell. Use the variable ${SUDO_USER} instead of hardcoding the sudo username. Must end with a /
airflow_disable                valueInt Disable All executions from Airflow. This is what the "start" Task is looking at
airflow_dummy_task_queue       valueStr Queue to use for dummy tasks (stop, stage_one_complete and more)
airflow_major_version          valueInt What is the major version of Airflow? 1 or 2 is valid options. Controls how the DAG files are generated
airflow_sudo_user              valueStr What user will Airflow sudo to for executing DBImport. This value will replace the ${SUDO_USER} variable in airflow_dbimport_commandpath setting
atlas_discovery_interval       valueInt How many hours there should pass between each Atlas discovery of a jdbc connection
cluster_name                   valueStr Name of Hadoop cluster
export_stage_disable           valueInt With 1, you prevent new Export tasks from starting and running tasks will stop after the current stage is completed.
export_staging_database        valueStr Name of staging database to use during Exports
export_start_disable           valueInt With 1, you prevent new Export tasks from starting. Running tasks will be completed
hdfs_address                   valueStr Address to HDFS
hdfs_basedir                   valueStr The base dir to write data to. Example /apps/dbimport
hdfs_blocksize                 valueStr The HDFS blocksize in bytes. Can usually be found in /etc/hadoop/conf/hdfs-site.xml (search for dfs.blocksize)
hive_acid_with_clusteredby     valueInt If 1, then ACID tables will be created with a clustered by option based on the PK. Not required with Hive3 and later
hive_insert_only_tables        valueInt If 1, then the non-merge tables in Hive will be ACID insert-only
hive_major_compact_after_merge valueInt With 1, DBImport will run a major compaction after the merge operations is completed
hive_print_messages            valueInt With 1, Hive will print additional messages during SQL operations
hive_remove_locks_by_force     valueInt With 1, DBImport will remove Hive locks by force
hive_validate_before_execution valueInt With 1, DBImport will run a group by query agains the validate table and verify the result against reference values hardcoded in DBImport
hive_validate_table            valueStr The table to run the validate query against
import_process_empty           valueInt If 1, then the import will do a full processing of import even if they contain no data.
import_stage_disable           valueInt With 1, you prevent new tasks from starting and running Import tasks will stop after the current stage is completed.
import_staging_database        valueStr Name of staging database to use during Imports
import_start_disable           valueInt With 1, you prevent new Import tasks from starting. Running tasks will be completed
kafka_brokers                  valueStr Comma separeted list of Kafka brokers
kafka_saslmechanism            valueStr Kafka SASL mechanism
kafka_securityprotocol         valueStr Kafka Security Protocol
kafka_topic                    valueStr Kafka topic to send the data to
kafka_trustcafile              valueStr Kafka CA Trust file for SSL
post_airflow_dag_operations    valueInt Post start and stop activities for Airflow DAGs to Kafka and/or Rest, depending on what is enabled
post_data_to_kafka             valueInt Enable the Kafka endpoint to be able to receive information regarding completed imports and exports
post_data_to_kafka_extended    valueInt Enable extended statistics in Kafka data
post_data_to_rest              valueInt Enable the REST endpoint to be able to receive information regarding completed imports and exports
post_data_to_rest_extended     valueInt Enable extended statistics in the REST data
rest_timeout                   valueInt Timeout for the REST call
rest_trustcafile               valueStr REST CA Trust file for SSL
rest_url                       valueStr Rest server URL
rest_verifyssl                 valueInt Verify SSL certificate during REST call
spark_export_default_executors valueInt How many executors should be used for tables who have never been exported before
spark_export_max_executors     valueInt The maximum number of executors to use during exports
spark_import_default_executors valueInt How many executors should be used for tables who have never been imported before
spark_import_max_executors     valueInt The maximum number of executors to use during imports
sqoop_export_default_mappers   valueInt How many mappers should be used for tables who have never been exported before
sqoop_export_max_mappers       valueInt The maximum number of mappers to use during exports
sqoop_import_default_mappers   valueInt How many mappers should be used for tables who have never been imported before
sqoop_import_max_mappers       valueInt The maximum number of mappers to use during imports
timezone                       valueStr The timezone that the configured times are meant for (example is Europe/Stockholm) 
============================== ======== ===========================================================================================================================================================================



Database Connections
--------------------

All communications against a source or target system goes against a Database Connection. This connection is configured in the jdbc_connections table. 

Username and Password
^^^^^^^^^^^^^^^^^^^^^

The username and password is encrypted and stored in the jdbc_connection table together with JDBC connection string and other information. To encrypt and save the username and password, you need to run the *manage* command tool::

    manage --encryptCredentials

You will first get a question about what Database Connection that the username and password should be used on, and then the username and password itself. Once all three items are entered, the username and password will be encrypted and saved in the *credentials* column in *jdbc_connections* table. 


JDBC Connection String
^^^^^^^^^^^^^^^^^^^^^^

The JDBC string needs to be entered manually into the *jdbc_url* column in the *jdbc_connections* table. Common for all JDBC connection strings is that you can add additional settings that is separated by a ; after the JDBC string that is documentat at each database type. 


**DB2 AS400**::

    jdbc:as400://<HOSTNAME>:<PORT>/<DATABASE>

**DB2 UDB**::

    jdbc:db2://<HOSTNAME>:<PORT>/<DATABASE>

**Microsoft SQL Server**

There are two different ways to enter the JDBC URL for MSSQL. Default Microsoft JDBC or jTDS JDBC. jTDS is used when you are autenticating with a user that is in AD and the standard Microsoft JDBC is used when the SQL Server have local users that you connect with::

    jdbc:sqlserver://<HOSTNAME>:<PORT>;database=<DATBASE NAME>
    jdbc:jtds:sqlserver://<HOSTNAME>:<PORT>;useNTLMv2=true;domain=<DOMAIN>;databaseName=<DATBASE NAME>

**MySQL**::

    jdbc:mysql://<HOSTNAME>:<PORT>/<DATABASE>

**Oracle**::

    jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=<HOSTNAME>)(PORT=<PORT>)))(CONNECT_DATA=(SERVICE_NAME=<SERVICE NAME>)))

**PostgreSQL**::

    jdbc:postgresql://<HOSTNAME>:<PORT>/<DATABASE>

**Progress**::

    jdbc:datadirect:openedge://<HOSTNAME>:<PORT>;databaseName=<DATABASE>


Testing connection
^^^^^^^^^^^^^^^^^^

After the Database Connection is created, JDBC string is entered and username/password is encrypted and saved, you are ready to test the connection to make sure that DBImport can connect to the remote database.:: 

    ./manage --testConnection -a <DATABASE CONNECTION>


AWS S3 Connections
------------------

DBImport have support for writing data from Hive into a S3 bucket. Even if this is strictly not a JDBC connection, the configuration and handling of the S3 connection is still configured in the jdbc_connections table. In order to make the configuation of AWS connections compatiable with the rest of DBImport, the connection string in the *jdbc_url* column wont be following the S3 default bucket string path, but instead containing additional information that is required in order to successfully write data to S3.

AccessKeyId and SecretAccessKey
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The AccessKeyId and SecretAccessKey is encrypted and stored in the jdbc_connection table together with S3 adress string and other information. To encrypt and save the AccessKeyId and SecretAccessKey, you need to run the *manage* command tool::

    manage --encryptCredentials

You will first get a question about what Database Connection that the keys should be used on, and then the AccessKeyId and SecretAccessKey itself. Once all three items are entered, the keys will be encrypted and saved in the *credentials* column in *jdbc_connections* table. 

Required S3 permissions
^^^^^^^^^^^^^^^^^^^^^^^

The permissions needed on the user or the role to assume in order to be able to save data to S3 is the following

-   s3:AbortMultipartUpload
-   s3:ListMultipartUploadParts
-   s3:DeleteObject
-   s3:GetObject
-   s3:PutObject
-   s3:ListBucket
-   s3:ListBucketMultipartUpload


Table schema
^^^^^^^^^^^^

In order to make it easier to understand and access the files on S3, there is a schema file created in the same directory as the data is located. This schema file is the output of sparks function *df.schema.json()* and stored untouched and in clear json text in a file called *_schema.json* on S3.  


Connection String
^^^^^^^^^^^^^^^^^

The S3 address string needs to be entered manually into the *jdbc_url* column in the *jdbc_connections* table. Whats specific for the DBImport S3 string is that additional configuration, separated by a ; after the bucket address is required in order to write data to S3::

    s3a://<BUCKET>/<FOLDER IF NEEDED>;region=eu-west-1;format=parquet

There are two additional properties that can be entered together with the mandatory *region* setting, and that is the following.

=========== ========= ===========================================================================================
region      Required  Name of AWS Region
format      Required  Format of the files written to S3. Only supported option is *parquet*
assumeRole  Optional  What iam role to assume before starting to write to S3. 
proxy       Optional  Proxy server used to communicate outside the company network. Format is https://server:port
=========== ========= ===========================================================================================

A full example of a S3 string with both a role and a proxy conbfigured::

    s3a://my-special-bucket/data-from-hive;format=parquet;assumeRole=arn:aws:iam::123456789012:role/AssumeRoleForMe;proxy=https://proxyserver.democompany.se:8080;region=eu-west-1

Target bucket and folder where the export will write it's data will be in the bucket plus folder specified in the *jdbc_url* column plus the value in *export_tables.target_schema* if it's not empty or *-* plus the value in *export_tables.target_table*. So with the example above, and the values in the specified column is *default* and *test_hive_table*, the full S3 path will be::

    s3a://my-special-bucket/data-from-hive/default/test_hive_table


Adding tables to Import
-----------------------

There are two ways to add tables from sources that we are going to import. Manually direct in the database or by running the search tool and add the dicsovered tables to the import_tables table. This documentation is about the search tool

The most simple way to search for tables to import is by running the following::

./manage --addImportTable -a <DATABASE CONNECTION> -h <HIVE DB>

This will add all tables and view that the tool can discover on the source database specified by <DATABASE CONNECTION> and add them to the <HIVE DB>.

In some cases, you dont want to add all the tables that the tool discovers. Maybe the tool discovers system tables, temp tables or other unwanted stuff that is not needed. To handle that, you can add filters for the schema and the table on the source system. The is done by adding the following to the **manage** command.

== ===================================================
-S Filter the schema name. * as wildcard is supported
-T Filter the table name. * as wildcard is supported
== ===================================================

You also have the ability to controll what the table in Hive should be called. The following options are available for you to change the table name

===========================  ===================================================================================================================
\\-\\-addCounterToTable      Adds a number to the table name. Starts from 1 if not \\-\\-counterStart is supplied
\\-\\-counterStart=<NUMBER>  Forces \\-\\-addCounterToTable to start from a specific number. Both with or without 0 in the beginning is supported
\\-\\-addSchemaToTable       Adds the schema from the source system to the Hive table
\\-\\-addCustomText          Adds a custom text to the Hive table
===========================  ===================================================================================================================


Adding tables to Export
-----------------------

There are two ways to add tables from Hive that we are going to export. Manually direct in the database or by running the search tool and add the dicsovered tables to the export_tables table. This documentation is about the search tool

The most simple way to search for tables to export is by running the following::

./manage --addExportTable -a <DATABASE CONNECTION> -S <SCHEMA>

This will add all tables and view that the tool can discover in Hive as exports to the connection specified by <DATABASE CONNECTION> and in the schema specified in <SCHEMA>.

In most cases, you dont want to export all tables in Hive to a specific database. To handle that, you can add filters for the Hive database and/or table. The is done by adding the following to the **manage** command.

== ======================================================
-h Filter the Hive Database. * as wildcard is supported
-t Filter the Hive Table. * as wildcard is supported
== ======================================================

You also have the ability to controll what the table in the remote database should be called. The following options are available for you to change the table name

===========================  ====================================================================================================================
\\-\\-addCounterToTable      Adds a number to the table name. Starts from 1 if not \\-\\-counterStart is supplied
\\-\\-counterStart=<NUMBER>  Forces \\-\\-addCounterToTable to start from a specific number. Both with or without 0 in the beginning is supported
\\-\\-addDBToTable           Adds the schema from the source system to the Hive table
\\-\\-addCustomText          Adds a custom text to the Hive table
===========================  ====================================================================================================================


Validation
----------

There are two validation methods available for DBImport. Row count and custom SQL. Row count is doing exactly what it says it's doing. Count the number of rows available in the source/target database and count the number of rows in Hive. If these match, validation succeeded. There is a certain amonut of missmatch allowed and this can be configured to allow a certain amount of missmatched rows. The other option is to use a custom SQL code. There is one SQL for the source/target database and one SQL for the Hive database. These SQL codes will be executed and the result will be converted to a json document with only the values. Columnnames and such are not part of the json document. When both SQL queries have been executed, the two json documents must match. So if you want to use a sum() on the primary key and compare that result, it would work. Or if you want a max() on a timestamp column, that works as well. Or maybe just the last 10 rows inserted, that is also possible. There is a limit on 512 bytes for the json file, but nothing else.


Row count validation
^^^^^^^^^^^^^^^^^^^^

**Imports**

For imports, these are the configuration properties in import_tables that are used to configure row count validation


validate_import
 Should the import be validated at all. 0 for no validation and 1 for validation. 

validationMethod
 Validation method to use. For row count validation, you select, believe it or not, 'rowCount'

validate_source
 Where should the source row count come from. There are two option. DBImport can execute a "select count(*) from ..." or just take the number of rows that spark or sqoop imported and use that as the number of rows in the source system. 
 Both have it's advantages. Running the select count(*) statement will return the actual rows on the source systemen, regardless of how many rows sqoop or spark imported. But lets say it's a log table and the table is filled with new data all the time. Then the number of rows that was added between the select statement and the time for spark or sqoop to execute will most likely exceed the allowed number of difference in row count between source and Hive. In this case, it's better to use the 'sqoop' method. Then the number of rows in the source system will be what spark or sqoop imported. 
 **Note**: Even if the setting is 'sqoop', it also works for spark. This is a legacy setting that was created when only sqoop was supported by DBImport.

validate_diff_allowed
 The default setting is -1. That means that the number of rows that are allowed to diff is handled automaticly. If it's a large table with many rows, the allowed diff is larger than a small table. 
 Setting this to a fixed value will only allow these many rows in diff. 
 **Note**: Formula for auto settings is the following. *rowcount*(50/(100*math.sqrt(rowcount)))*

incr_validation_method
 If the import is an incremental import, then you have the option to choose if you are going to validate against the full number of rows or only validate the incremental rows that you are importing. There are cases when for example the source system only keeps a X number of days data in their tables. Then after X number of days of incremental imports, there will be more data in Hive compared to the source system. Then the 'full' ince_validation_method will fail as the total number of rows will be different. In this case, the 'incr' method should be used. What it basically does is to add the min and max values for the incremental load to the select count statement. So only the incrementally loaded rows are counted.

**Exports**

For exports, these are the configuration properties in export_tables that are used to configure row count validation


validate_export
 Should the export be validated at all. 0 for no validation and 1 for validation. 

validationMethod
 Validation method to use. For row count validation, you select, believe it or not, 'rowCount'

incr_validation_method
 If the export is an incremental export, then you have the option to choose if you are going to validate against the full number of rows or only validate the incremental rows that you are exporting. 


custom SQL validation
^^^^^^^^^^^^^^^^^^^^^

**Variables**

There are certain variables that can be used in the queries. These will during runtime be replaced with the real values. It makes it faster to configure the same custom SQL queries on multiple tables when only for example the tablename is different.

These are the available variables

================== ================
${HIVE_DB}         Replaced with the Hive Database configured in both imports and exports 
${HIVE_TABLE}      Replaced with the Hive Table configured in both imports and exports
${SOURCE_SCHEMA}   Replaced with source database schema in imports
${SOURCE_TABLE}    Replaced with source database table in imports
${TARGET_SCHEMA}   Replaced with target database schema in exports
${TARGET_TABLE}    Replaced with target database table in exports
================== ================


**Imports**

These are the configuration properties in import_tables that are used to configure custom SQL validation


validate_import
 Should the import be validated at all. 0 for no validation and 1 for validation.

validationMethod
 Validation method to use. For custom SQL validation, you select 'customQuery'

validationCustomQuerySourceSQL
 The SQL query that will be executed in the source database

validationCustomQueryHiveSQL
 The SQL query that will be executed in Hive. ${HIVE_DB} and ${HIVE_TABLE} variable must be used as the query will be executed on both the *Import Table* and *Target Table*

validationCustomQueryValidateImportTable
 For certain imports, like incremental imports, running the custom sql against the *import table* have a large risk of returning the incorrect result. So for custom SQL imports, it's possible to disable the validation on the *import table* and only do the validation on the *target table*. Putting 0 in this column will disable validation on the *import table*


**Exports**

These are the configuration properties in export_tables that are used to configure custom SQL validation


validate_export
 Should the export be validated at all. -1 for no validation and 1 for validation.

validationMethod
 Validation method to use. For custom SQL validation, you select 'customQuery'

validationCustomQueryHiveSQL
 The SQL query that will be executed in Hive. 

validationCustomQueryTargetSQL
 The SQL query that will be executed in the target database


