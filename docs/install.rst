Installing DBImport
===================

Installation
------------

To install DBImport, please follow the steps on this page. That will setup DBImport and make you able to import your first database in notime.

Download the source package from the latest release from the `GitHub Release Page <https://github.com/BerryOsterlund/DBImport/releases>`_. Create a directory and unpack the file::

        mkdir /usr/local/dbimport
        cd /usr/local/dbimport
        tar -xf dbimport-VERSION.tar
   

There is a number of Python dependencies that needs to be in place before DBImport can start. DBImport supports Python 3.11. This needs Spark 3.4 or later on the cluster, as earlier Spark versions do not support Python 3.11. It's also preferable to use a virtual environment for Python, but creating a virtual environment is not part of this documentation. All required Python packages and their pinned versions are listed in *bin/requirements.txt*, which is generated from *uv.lock*.

Some of the packages are built from source during installation. This requires a C compiler and the Kerberos development headers, for example the *libkrb5-dev* package on Debian and Ubuntu, or the equivalent package on other distributions.


**Python 3.11**

Install the Python packages and then DBImport itself with::

        python3.11 -m pip install -r bin/requirements.txt
        python3.11 -m pip install --no-deps .

The second command builds DBImport with its build backend *uv_build*, which pip downloads from PyPI. On a host without internet access, provide the *uv_build* wheel, or install a DBImport wheel built elsewhere (``uv build --wheel``) instead of the source directory.

If you use `uv <https://docs.astral.sh/uv/>`_, you can create the environment from the lock file instead. This also installs DBImport::

        uv sync --locked

The commands in *bin* (*import*, *export*, *manage*, ...) work as before. The same commands are also installed into the environment as *dbimport-import*, *dbimport-export*, *dbimport-manage* and so on.



Base configuration
------------------

There is a file inside the *conf* directory called *dbimport.cfg*. This file contains configuration that is required to connect databases, HDFS and such. Before running any DBImport commands, this file needs to be configured. The important settings that you must configure is the following

  - Section [Database], all settings starting with *mysql*. This is the connection to the DBImport configuration database.
  - Section [REST_statistics]. Put *post_column_data* and *post_column_data* to *false* or configure a valid REST interface on *rest_endpoint*
  - Section [Kerberos]. *keytab* and *principal* need to point to a valid keytab file and the principal that it contains.
  - Section [Hive]. *hive_metastore_alchemy_conn* must have a valid Alchemy connection string to the Hive Metastore SQL database. 
  - Section [Hive]. *servers*, *port* and *kerberos_service_name* must all be set against a Hive server with binary transport mode.
  - Section [Spark]. *servers*, *port* and *kerberos_service_name* must all be set against a Hive server with binary transport mode.
  - Section [Airflow]. *airflow_alchemy_conn* must have a valid Alchemy connection string to the Airflow SQL database. Only needed if Airflow integrations will be used
  - Section [Credentials]. Both public and private keys need to be valid. See SSL keys section further down on this side for more information.

.. note:: If the *hive_remove_locks_by_force* setting (in the configuration table) is set to 1, the user configured for the *hive_metastore_alchemy_conn* must have delete permissions to the *HIVE_LOCKS* table. Other than that, the user only requires select permissions. 

SSL keys
--------

As DBImport is saving connection details to all databases its importing from, this data needs to be encrypted and stored in a secure way. To do that, we have a privat/public keypair that encrypts and decrypts the credentials to the source system. These keys needs to be configured. Default configuration is to have them under the conf directory. This can be changed in the *dbimport.cfg* if if needed. To create the keys with the default path, run the following command::

        openssl genrsa -out conf/dbimport_private_key.pem 8192
        openssl rsa -in conf/dbimport_private_key.pem -out conf/dbimport_public_key.pem -outform PEM -pubout

Once created, make sure that the unix permission only enabled the correct users to read these files.


Database Setup
--------------

As the configuration is all located in a MariaDB database, you need to create this database, give the DBImport user the permissions to create tables and then the *setup* tool will create the required tables in that database. We assume that you have a working MariaDB server as the setup of that is not part of this documentation.

.. note:: Due to incombability between MariaDB and SQLAlchemy, MariaDB version 10.2.8 or earlier in the 10.2 series is not supported. Either run 10.1.x or 10.2.9 or newer

.. note:: The configuration database must use *latin1* as its character set. With *utf8mb3* or *utf8mb4*, creating the schema fails with error 1071 (specified key was too long), because several primary keys exceed the InnoDB key length limit of 3072 bytes. Create the database with::

        CREATE DATABASE DBImport CHARACTER SET latin1;

   This was verified with MariaDB 11.4.

Create the configuration schema running this command::

        bin/setup --createDB
   
Database Configuration
----------------------

There is also global configuration stored inside the newly created database. These settings are stored in the *configuration* table. There is one value that is important to configure correct as it requires creation of a table in Hive if enabled. And that is *hive_validate_before_execution*. If this is set to 1 in *valueInt* column, it will do a *select ... group by ....* from the table configured by the *hive_validate_table* setting. Default value is *dbimport.validate_table*. To create this table, please run the following in Hive::

        create database dbimport
        create table dbimport.validate_table ( id integer, value string )
        insert into dbimport.validate_table (id, value) values (1, 'Hive'), (2, 'LLAP'), (3, 'is'), (4, 'working'), (5, 'fine')

DBImport uses staging databases for creating of Import Tables and Temporary Export Tables. These databases needs to exist before running import and/or exports. The name of the databases are configured by the *import_staging_database* and the *export_staging_database*. To create the default databases, please run the following in Hive::

        create database etl_import_staging
        create database etl_export_staging

JDBC Driver location
--------------------

In order to connect to a specific database type, the path to the JDBC JAR file must be updated. Inside the *jdbc_connections_drivers* table you will find the supported database types with the JDBC driver name and the classpath data. For a new and fresh installation of DBImport the *classpath* will be 'add path to JAR file'. This is obviously not correct and needs to up update to point to the full path of the jar file that includes the driver specified in the *driver* column. Enter the correct path to your jar file and also make sure that sqoop have these files in its classpath. 

Thats it. DBImport is now installed and ready for usage. If this is your first time working with DBImport, the :doc:`quickstart` guide can help you to get up to speed fast.

Support for Spark
-----------------

If you are planning to use Spark there might be an incombability problem with python2 vs python3 and the topology script file in HDFS. 

.. note:: If you see the error ** SyntaxError: Missing parentheses in call to 'print'. Did you mean print(rack)?** when using the spark tool in DBImport, you know that you will have to fix this

The usual way to change this is the following

  - change the *net.topology.script.file.name* to another filename in core-site.xml. In this example we will use */etc/hadoop/conf/custom_topology_script.py*
  - cp /etc/hadoop/conf/ topology_script.py /etc/hadoop/conf/custom_topology_script.py
  - Change the first row from “#!/usr/bin/env python” to “#!/usr/bin/python2”
  - Make the same change on all nodes in the cluster. 
  - Restart affected services (HDFS, Yarn and so on)

.. note:: If you dont want to create a seperate topology file and configuration, you can of course just change the first row in the default topology file on all nodes in the cluster

Once that is changed, you need to update the Spark settings in the configuration file. These two different versions have been testad and works on HDP 2.6.5 and HDP 3.1.4

Verified Spark settings for HDP 2.6.5::

        path_append = /usr/hdp/current/spark2-client/python/, /usr/hdp/current/spark2-client/python/lib/py4j-0.10.6-src.zip
        jar_files =
        py_files = /usr/hdp/current/spark2-client/python/lib/py4j-src.zip
        master = yarn
        deployMode = client
        yarnqueue = default
        dynamic_allocation = true
        executor_memory = 2688M
        hdp_version = 2.6.5.0-292
        hive_library = HiveContext

Verified Spark settings for HDP 3.1.4::

        path_append = /usr/hdp/current/spark2-client/python/, /usr/hdp/current/spark2-client/python/lib/py4j-0.10.7-src.zip, /usr/hdp/current/hive_warehouse_connector/pyspark_hwc-1.0.0.3.1.4.0-315.zip
        jar_files = /usr/hdp/current/hive_warehouse_connector/hive-warehouse-connector-assembly-1.0.0.3.1.4.0-315.jar
        py_files = /usr/hdp/current/hive_warehouse_connector/pyspark_hwc-1.0.0.3.1.4.0-315.zip
        master = yarn
        deployMode = client
        yarnqueue = default
        dynamic_allocation = true
        executor_memory = 2688M
        hdp_version = 3.1.4.0-315
        hive_library = HiveWarehouseSession

Verified Spark settings for CDP 7.1.7 SP1::

        path_append = /opt/cloudera/parcels/CDH/lib/spark/python, /opt/cloudera/parcels/CDH-7.1.7-1.cdh7.1.7.p1000.24102687/lib/spark/python/lib/py4j-0.10.7-src.zip, /opt/cloudera/parcels/CDH-7.1.7-1.cdh7.1.7.p1000.24102687/lib/hive_warehouse_connector/pyspark_hwc-1.0.0.7.1.7.1000-141.zip
        jar_files = /opt/cloudera/parcels/CDH-7.1.7-1.cdh7.1.7.p1000.24102687/jars/hive-warehouse-connector-assembly-1.0.0.7.1.7.1000-141.jar
        py_files = /opt/cloudera/parcels/CDH-7.1.7-1.cdh7.1.7.p1000.24102687/lib/hive_warehouse_connector/pyspark_hwc-1.0.0.7.1.7.1000-141.zip
        master = yarn
        deployMode = client
        yarnqueue = default
        dynamic_allocation = true
        executor_memory = 2688M
        hdp_version = 7.1.7.1000-141
        hive_library = HiveWarehouseSession

*Spark support for AWS S3 exports*

if you are planning to use AWS S3 exports, you need to add the jar files to the Spark configuration. The two files needed is *hadoop-aws-VERSION.jar* and *aws-java-sdk-s3-VERSION.jar*. For the CDP 7.1.7 SP1 example above, replace the jar_files row with the following row:: 

        jar_files = /opt/cloudera/parcels/CDH-7.1.7-1.cdh7.1.7.p1000.24102687/jars/hive-warehouse-connector-assembly-1.0.0.7.1.7.1000-141.jar, /opt/cloudera/parcels/CDH-7.1.7-1.cdh7.1.7.p1000.24102687/jars/hadoop-aws-3.1.1.7.1.7.1000-141.jar, /opt/cloudera/parcels/CDH-7.1.7-1.cdh7.1.7.p1000.24102687/jars/aws-java-sdk-s3-1.12.124.jar

*Python version for Spark*

Both the local DBImport code and the spark code must run with the same python version. In order to do so, set the PYSPARK_PYTHON variable to the python version you are running before you execute the import or exports::

    export PYSPARK_PYTHON=python3.6

Support for Spark3
------------------

The most obvious activity to enable Spark3 is to of course install Spark3 in the cluster. If you are using CDP, Spark3 is available as a separate Parcel and will be installed in parallel with Spark2, both being active in the cluster at the same time. Once Spark3 is installed, you need to tell DBImport to use the jar files from Spark3 instead of Spark2.

Verified Spark settings for CDP 7.1.8::

        path_append = /opt/cloudera/parcels/SPARK3/lib/spark3/python, /opt/cloudera/parcels/SPARK3/lib/spark3/python/lib/py4j-0.10.9.5-src.zip, /opt/cloudera/parcels/SPARK3/lib/hwc_for_spark3/pyspark_hwc-spark3-1.0.0.3.3.7180.5-1.zip
        jar_files = /opt/cloudera/parcels/SPARK3/lib/hwc_for_spark3/hive-warehouse-connector-spark3-assembly-1.0.0.3.3.7180.5-1.jar
        py_files = /opt/cloudera/parcels/SPARK3/lib/hwc_for_spark3/pyspark_hwc-spark3-1.0.0.3.3.7180.5-1.zip

        master = yarn
        deployMode = client
        yarnqueue = default
        dynamic_allocation = true
        executor_memory = 2688M
        hdp_version = 7.1.8.11-3 
        hive_library = HiveWarehouseSession


Iceberg
-------

.. note:: Iceberg support is in tech-preview state and havent been used as much in a stable production environment as normal imports have. Please keep this in mind when using it.

As Iceberg isn’t fully supported yet by CDP Private Cloud Base Edition, we need to manually install Iceberg and enable support for it. Download the Iceberg spark jar from Apache Iceberg homepage and upload that jar file to all nodes in the cluster. During development of Iceberg support in DBImport, the file and version used together with CDP 7.1.8 was *iceberg-spark-runtime-3.3_2.12-1.1.0.jar*.
To enable DBImport to use Iceberg file format, you need to add the jar file to the *[Spark] jar_files* configuration in the config file.

Verified Spark settings for CDP 7.1.8 with Iceberg::

        path_append = /opt/cloudera/parcels/SPARK3/lib/spark3/python, /opt/cloudera/parcels/SPARK3/lib/spark3/python/lib/py4j-0.10.9.5-src.zip, /opt/cloudera/parcels/SPARK3/lib/hwc_for_spark3/pyspark_hwc-spark3-1.0.0.3.3.7180.5-1.zip
        jar_files = /opt/cloudera/parcels/SPARK3/lib/hwc_for_spark3/hive-warehouse-connector-spark3-assembly-1.0.0.3.3.7180.5-1.jar, /opt/datalake/jars/iceberg-spark-runtime-3.3_2.12-1.1.0.jar
        py_files = /opt/cloudera/parcels/SPARK3/lib/hwc_for_spark3/pyspark_hwc-spark3-1.0.0.3.3.7180.5-1.zip

Due to a limitation in CDP regarding TEZ-4248 implementation, CDP Private Cloud Base Edition cant access the Iceberg tables through Hive. But Impala and direct spark access works fine. Regardless of Hive working or not, the following settings must be enabled in the HiveServer2 in CDP for Iceberg to work even with Impala.

====================================== ===========================================================================================
hive.vectorized.execution.enabled      false
iceberg.engine.hive.enabled            true
tez.mrreader.config.update.properties  hive.io.file.readcolumn.names,hive.io.file.readcolumn.ids
====================================== ===========================================================================================



Atlas
--------------------

There are three things you need to configure in order to use the Atlas integration.

.. note:: DBImport requires Atlas version 1.1.0 or higher

**DBImport configuration file**

In the configuration file for DBImport, you need to specify the address to the Atlas server. If Atlas is running with SSL, you also need to configure the certificate path for the CA file. See the default configuration file for syntax on these properties.

**Atlas Schema**

DBImport uses it’s own schema in Atlas. This needs to be created manually. The json file for the schema is located under *atlas/dbimportProcess.json*. In order to create the schema, run the following commands in a session where you have a valid Kerberos ticket with admin permission in Atlas::

        curl --negotiate -u : -i -X POST -H 'Content-Type: application/json' -H 'Accept: application/json' "https://<ATLAS SERVER HOSTNAME>:<PORT>/api/atlas/v2/types/typedefs" -d "@<FULL_PATH_TO_DBIMPORT>/atlas/dbimportProcess.json"

**Atlas process Pictures**

To get the correct representation of the DBImport Lineage in Atlas, you need to copy the DBImport icon into the Atlas Server directory for lineage icons. The picture is located under *atlas/ DBImport_Process.png*. The target directory is *server/webapp/atlas/img/entity-icon* under Atlas installation directory. Example

HDP::

        cp /usr/local/dbimport/atlas/DBImport_Process.png /usr/hdp/current/atlas-server/server/webapp/atlas/img/entity-icon

CDP/CDH::

        cp /usr/local/dbimport/atlas/DBImport_Process.png /opt/cloudera/parcels/CDH/lib/atlas/server/webapp/atlas/img/entity-icon



DBImport Server
--------------------

The server part of DBImport handles Atlas auto discovery of remote RDBMS schemas and the asynchronous copy of data between two or more DBImport instances. It also contains a simple REST interface in order to query the status of the DBImport Server for monitoring purposes. 

Two directories is required for the DBImport server. One for log files and where the pid file should be written. These are configured in the DBImport configurarion file in the [Server] section. Please create these two directories and change permission to the user running the DBImport server. For default user and path, the following can be used::

        mkdir /var/log/dbimport
        mkdir /var/run/dbimport
        chown dbimport /var/log/dbimport
        chown dbimport /var/run/dbimport
        chmod 700 /var/log/dbimport
        chmod 700 /var/run/dbimport

**distcp configuration for multi instance ingestion**

The DBImport server uses *distcp* in the background to copy files between Hadoop clusters. This is done in parallel and the number of parallel sessions in controlled by the *distCP_threads* under the [Server] section in the configuration file. 

If the number of *distcp* commands running in parallel gets to high, the log file will be hard to read as the output come in one file. This behavior can be changed so that each thread log in its own file. *distCP_separate_logs* controls this.


Upgrading
--------------------

In order to upgrade DBImport, all files from the source package needs to be replaced with new versions. Many versions also needs a configuration database schema upgrade. To be sure that you are on the last schema for the configuration database, please run the following after upgrading the binary files::

        ./setup --upgradeDB

This command will only upgrade the schema if it's not in the last version. So it's safe to run it even if there are no new schema.

There might also be changes in the *conf/dbimport.cfg* configuration file. Please compare your local version with the default config file available in the release. If you are missing certain settings in your local configuration, please add those. If you dont do that, you will most likely get an error saying that you cant find a specific key in the configuration. 

