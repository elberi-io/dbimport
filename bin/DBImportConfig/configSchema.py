# coding: utf-8
from sqlalchemy import (
    Column,
    DateTime,
    Enum,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    SmallInteger,
    String,
    Text,
    Time,
    text,
)
from sqlalchemy.dialects.mysql import BIGINT, TINYINT
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()
metadata = Base.metadata


class alembicVersion(Base):
    __tablename__ = "alembic_version"

    version_num = Column(String(32), primary_key=True, nullable=False)


class dbimportInstances(Base):
    __tablename__ = "dbimport_instances"
    __table_args__ = {
        "comment": "This table contains all DBInstance that will receive data from this instance during the copy phase"
    }

    instance_id = Column(
        BIGINT(20),
        primary_key=True,
        autoincrement=True,
        comment="Auto Incremented PrimaryKey of the table",
    )
    name = Column(
        String(32), nullable=False, unique=True, comment="Name of the DBImport instance"
    )
    db_hostname = Column(
        String(64), nullable=False, comment="MySQL Hostname to DBImport database"
    )
    db_port = Column(
        SmallInteger, nullable=False, comment="MySQL Port to DBImport database"
    )
    db_database = Column(
        String(64), nullable=False, comment="MySQL Database to DBImport database"
    )
    db_credentials = Column(
        Text, nullable=True, comment="MySQL Username and Password to DBImport database"
    )
    hdfs_address = Column(
        String(64), nullable=False, comment="HDFS address. Example hdfs://hadoopcluster"
    )
    hdfs_basedir = Column(
        String(64),
        nullable=False,
        comment="The base dir to write data to. Example /apps/dbimport",
    )
    sync_credentials = Column(
        TINYINT(4),
        nullable=False,
        server_default=text("'0'"),
        comment="0 = Credentials wont be synced, 1 = The credentials information will be synced to the other cluster",
    )


class copyTables(Base):
    __tablename__ = "copy_tables"
    __table_args__ = {
        "comment": "When the copy phase starts, it will look in this table to understand if its going to copy its data and to what DBImport instances."
    }

    copy_id = Column(
        BIGINT(20),
        primary_key=True,
        autoincrement=True,
        comment="Auto Incremented PrimaryKey of the table",
    )
    hive_filter = Column(
        String(256),
        nullable=False,
        comment="Filter string for database and table. ; separated. Wildcards (*) allowed. Example HIVE_DB.HIVE_TABLE; HIVE_DB.HIVE_TABLE",
    )
    destination = Column(
        String(32),
        ForeignKey("dbimport_instances.name"),
        nullable=False,
        index=True,
        comment="DBImport instances to copy the imported data to",
    )
    data_transfer = Column(
        Enum("Synchronous", "Asynchronous"),
        nullable=False,
        comment="Synchronous will transfer the data as part of the Import. Asynchronous will transfer the data by a separate process and not part of the Import",
        server_default=text("'Synchronous'"),
    )

    dbimport_instances = relationship("dbimportInstances")


class copyASyncStatus(Base):
    __tablename__ = "copy_async_status"
    __table_args__ = {
        "comment": "The status table for asynchronous copy between DBImport instances."
    }

    table_id = Column(
        Integer,
        primary_key=True,
        nullable=False,
        index=True,
        comment="Reference to import_table.table_id",
    )
    hive_db = Column(String(256), nullable=False, comment="Hive Database")
    hive_table = Column(String(256), nullable=False, comment="Hive Table to copy")
    destination = Column(
        String(32),
        primary_key=True,
        nullable=False,
        comment="DBImport instances to copy the imported data to",
    )
    copy_status = Column(
        TINYINT(4),
        nullable=False,
        server_default=text("'0'"),
        comment="Status of the copy operation",
    )
    last_status_update = Column(
        DateTime,
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP"),
        comment="Last time the server changed progress on this copy",
    )
    failures = Column(
        Integer,
        server_default=text("'0'"),
        nullable=False,
        comment="Number of failures on current state",
    )
    hdfs_source_path = Column(
        String(768), nullable=False, comment="HDFS path to copy from"
    )
    hdfs_target_path = Column(
        String(768), nullable=False, comment="HDFS path to copy to"
    )


class airflowCustomDags(Base):
    __tablename__ = "airflow_custom_dags"
    __table_args__ = {
        "comment": 'Its possible to construct a DAG that have no import, export or ETL definitions in it, but instead just Tasks from the airflow_task table. That might nbe useful to for example run custom Hive Code after an import is completed as a separate DAG. Defining a DAG in here also requires you to have at least one task in airflow_tasks defined "in main"'
    }

    dag_name = Column(String(64), primary_key=True, comment="Name of Airflow DAG.")
    schedule_interval = Column(
        String(128),
        nullable=False,
        comment="Time to execute dag",
        server_default=text("'None'"),
    )
    retries = Column(
        Integer,
        nullable=False,
        server_default=text("'0'"),
        comment="How many retries should be Task do in Airflow before it failes",
    )
    operator_notes = Column(
        Text, comment="Free text field to write a note about the custom DAG. "
    )
    application_notes = Column(
        Text,
        comment="Free text field that can be used for application documentaton, notes or links. ",
    )
    airflow_notes = Column(
        Text,
        comment="Documentation that will be available in Airflow. Markdown syntax.",
    )
    auto_regenerate_dag = Column(
        TINYINT(4),
        nullable=False,
        server_default=text("'1'"),
        comment="1 = The DAG will be auto regenerated by manage command",
    )
    sudo_user = Column(
        String(64),
        comment="All tasks in DAG will use this user for sudo instead of default",
    )
    timezone = Column(
        String(64),
        comment="Timezone used for schedule_interval column. Use full text timezone, example Europe/Stockholm.",
    )
    email = Column(
        String(256),
        comment="Email to send message to in case email_on_retry or email_on_failure is set to True. Comma separated list of email addresses",
    )
    email_on_failure = Column(
        TINYINT(4),
        nullable=False,
        server_default=text("'0'"),
        comment="Send email on failures",
    )
    email_on_retries = Column(
        TINYINT(4),
        nullable=False,
        server_default=text("'0'"),
        comment="Send email on retries",
    )
    tags = Column(
        String(256),
        nullable=True,
        comment="Comma seperated list of Airflow tags that will be set on the Dag",
    )
    sla_warning_time = Column(
        Time,
        nullable=True,
        comment="Maximum time this DAG should run before Airflow triggers a SLA miss",
    )
    retry_exponential_backoff = Column(
        TINYINT(4),
        nullable=False,
        server_default=text("'0'"),
        comment="1 = Use the retry_exponential_backoff Airflow function that will cause the retry between failed tasks to be longer and longer each time instead of a fixed time, 0 = Run with a fixed time of 5 min between the task retries",
    )
    concurrency = Column(
        TINYINT(4),
        nullable=True,
        comment="Set the max number of concurrent tasks in the DAG while executing. Overrides the default value specified in Airflow configuration",
    )


class airflowDagSensors(Base):
    __tablename__ = "airflow_dag_sensors"
    __table_args__ = {"comment": "This table is used only in Legacy DBImport"}

    dag_name = Column(
        String(64),
        primary_key=True,
        nullable=False,
        comment="Name of DAG to add sensor to",
    )
    sensor_name = Column(
        String(64), primary_key=True, nullable=False, comment="name of the sensor Task"
    )
    wait_for_dag = Column(
        String(64),
        nullable=False,
        comment="Name of DAG to wait for. Must be defined in DBImport",
    )
    wait_for_task = Column(
        String(64), comment="Name of task to wait for.Default is 'stop'"
    )
    timeout_minutes = Column(Integer, comment="Number of minutes to wait for DAG")
    sensor_soft_fail = Column(
        Integer,
        nullable=True,
        comment="Setting this to 1 will add soft_fail=True on sensor",
    )


class airflowEtlDags(Base):
    __tablename__ = "airflow_etl_dags"
    __table_args__ = {
        "comment": "To create a DAG in Airflow for only ETL jobs, this is the table that holds all definitions of the DAG configuration, including the filter that defines what ETL jobs to run, schedules times, pool names and much more. "
    }

    dag_name = Column(String(64), primary_key=True, comment="Name of Airflow DAG.")
    schedule_interval = Column(
        String(128),
        nullable=False,
        comment="Time to execute dag",
        server_default=text("'None'"),
    )
    filter_job = Column(
        String(64), nullable=False, comment="Filter string for JOB in etl_jobs table"
    )
    filter_task = Column(String(64), comment="Filter string for TASK in etl_jobs table")
    filter_source_db = Column(
        String(256), comment="Filter string for SOURCE_DB in etl_jobs table"
    )
    filter_target_db = Column(
        String(256), comment="Filter string for TARGET_DB in etl_jobs table"
    )
    retries = Column(
        TINYINT(4),
        comment="How many retries should be Task do in Airflow before it failes",
    )
    trigger_dag_on_success = Column(String(64), comment="<NOT USED>")
    operator_notes = Column(
        Text, comment="Free text field to write a note about the ETL job. "
    )
    application_notes = Column(
        Text,
        comment="Free text field that can be used for application documentaton, notes or links. ",
    )
    airflow_notes = Column(
        Text,
        comment="Documentation that will be available in Airflow. Markdown syntax.",
    )
    auto_regenerate_dag = Column(
        TINYINT(4),
        nullable=False,
        server_default=text("'1'"),
        comment="1 = The DAG will be auto regenerated by manage command",
    )
    sudo_user = Column(
        String(64),
        comment="All tasks in DAG will use this user for sudo instead of default",
    )
    timezone = Column(
        String(64),
        comment="Timezone used for schedule_interval column. Use full text timezone, example Europe/Stockholm.",
    )
    email = Column(
        String(256),
        comment="Email to send message to in case email_on_retry or email_on_failure is set to True. Comma separated list of email addresses",
    )
    email_on_failure = Column(
        TINYINT(4),
        nullable=False,
        server_default=text("'0'"),
        comment="Send email on failures",
    )
    email_on_retries = Column(
        TINYINT(4),
        nullable=False,
        server_default=text("'0'"),
        comment="Send email on retries",
    )
    tags = Column(
        String(256),
        nullable=True,
        comment="Comma seperated list of Airflow tags that will be set on the Dag",
    )
    sla_warning_time = Column(
        Time,
        nullable=True,
        comment="Maximum time this DAG should run before Airflow triggers a SLA miss",
    )
    retry_exponential_backoff = Column(
        TINYINT(4),
        nullable=False,
        server_default=text("'0'"),
        comment="1 = Use the retry_exponential_backoff Airflow function that will cause the retry between failed tasks to be longer and longer each time instead of a fixed time, 0 = Run with a fixed time of 5 min between the task retries",
    )
    concurrency = Column(
        TINYINT(4),
        nullable=True,
        comment="Set the max number of concurrent tasks in the DAG while executing. Overrides the default value specified in Airflow configuration",
    )


class airflowExportDags(Base):
    __tablename__ = "airflow_export_dags"
    __table_args__ = {
        "comment": "To create a DAG in Airflow for Exports, this is the table that holds all definitions of the DAG configuration, including the filter that defines what tables to export, schedules times, pool names and much more. "
    }

    dag_name = Column(String(64), primary_key=True, comment="Name of Airflow DAG.")
    schedule_interval = Column(
        String(128),
        nullable=False,
        comment="Time to execute dag",
        server_default=text("'None'"),
    )
    filter_dbalias = Column(
        String(256),
        nullable=False,
        comment="Filter string for DBALIAS in export_tables",
    )
    filter_target_schema = Column(
        String(256), comment="Filter string for TARGET_SCHEMA  in export_tables"
    )
    filter_target_table = Column(
        String(256), comment="Filter string for TARGET_TABLE  in export_tables"
    )
    use_python_dbimport = Column(
        TINYINT(4),
        nullable=False,
        server_default=text("'1'"),
        comment="Legacy use only. Always put this to 1",
    )
    retries = Column(
        TINYINT(4),
        comment="How many retries should be Task do in Airflow before it failes",
    )
    trigger_dag_on_success = Column(
        String(64),
        comment="Name of DAG to trigger if export is successfull. Comma seperated list of DAGs (optional)",
    )
    operator_notes = Column(
        Text, comment="Free text field to write a note about the export. "
    )
    application_notes = Column(
        Text,
        comment="Free text field that can be used for application documentaton, notes or links. ",
    )
    auto_regenerate_dag = Column(
        TINYINT(4),
        nullable=False,
        server_default=text("'1'"),
        comment="1 = The DAG will be auto regenerated by manage command",
    )
    airflow_notes = Column(
        Text,
        comment="Documentation that will be available in Airflow. Markdown syntax.",
    )
    sudo_user = Column(
        String(64),
        comment="All tasks in DAG will use this user for sudo instead of default",
    )
    timezone = Column(
        String(64),
        comment="Timezone used for schedule_interval column. Use full text timezone, example Europe/Stockholm.",
    )
    email = Column(
        String(256),
        comment="Email to send message to in case email_on_retry or email_on_failure is set to True. Comma separated list of email addresses",
    )
    email_on_failure = Column(
        TINYINT(4),
        nullable=False,
        server_default=text("'0'"),
        comment="Send email on failures",
    )
    email_on_retries = Column(
        TINYINT(4),
        nullable=False,
        server_default=text("'0'"),
        comment="Send email on retries",
    )
    tags = Column(
        String(256),
        nullable=True,
        comment="Comma seperated list of Airflow tags that will be set on the Dag",
    )
    sla_warning_time = Column(
        Time,
        nullable=True,
        comment="Maximum time this DAG should run before Airflow triggers a SLA miss",
    )
    retry_exponential_backoff = Column(
        TINYINT(4),
        nullable=False,
        server_default=text("'0'"),
        comment="1 = Use the retry_exponential_backoff Airflow function that will cause the retry between failed tasks to be longer and longer each time instead of a fixed time, 0 = Run with a fixed time of 5 min between the task retries",
    )
    concurrency = Column(
        TINYINT(4),
        nullable=True,
        comment="Set the max number of concurrent tasks in the DAG while executing. Overrides the default value specified in Airflow configuration",
    )


class airflowImportDags(Base):
    __tablename__ = "airflow_import_dags"
    __table_args__ = {
        "comment": "To create a DAG in Airflow for Imports, this is the table that holds all definitions of the DAG configuration, including the filter that defines what tables to import, schedules times, pool names and much more."
    }

    dag_name = Column(String(64), primary_key=True, comment="Name of Airflow DAG.")
    schedule_interval = Column(
        String(128),
        nullable=False,
        comment="Time to execute dag",
        server_default=text("'None'"),
    )
    filter_hive = Column(
        String(1024),
        nullable=False,
        comment="Filter string for database and table. ; separated. Wildcards (*) allowed. Example HIVE_DB.HIVE_TABLE; HIVE_DB.HIVE_TABLE",
    )
    use_python_dbimport = Column(
        TINYINT(4),
        nullable=False,
        server_default=text("'1'"),
        comment="Legacy use only. Always put this to 1",
    )
    finish_all_stage1_first = Column(
        TINYINT(4),
        nullable=False,
        server_default=text("'0'"),
        comment="1 = All Import phase jobs will be completed first, and when all is successfull, the ETL phase start",
    )
    run_import_and_etl_separate = Column(
        TINYINT(4),
        nullable=False,
        server_default=text("'0'"),
        comment="1 = The Import and ETL phase will run in separate Tasks. ",
    )
    retries = Column(
        TINYINT(4),
        nullable=False,
        server_default=text("'5'"),
        comment="How many retries should be Task do in Airflow before it failes",
    )
    retries_stage1 = Column(
        TINYINT(4), comment="Specific retries number for Import Phase"
    )
    retries_stage2 = Column(TINYINT(4), comment="Specific retries number for ETL Phase")
    pool_stage1 = Column(
        String(256),
        comment="Airflow pool used for stage1 tasks. NULL for the default Hostname pool",
    )
    pool_stage2 = Column(
        String(256),
        comment="Airflow pool used for stage2 tasks. NULL for the default DAG pool",
    )
    operator_notes = Column(
        Text, comment="Free text field to write a note about the import. "
    )
    application_notes = Column(
        Text,
        comment="Free text field that can be used for application documentaton, notes or links. ",
    )
    auto_table_discovery = Column(
        TINYINT(4), nullable=False, server_default=text("'1'"), comment="<NOT USED>"
    )
    auto_regenerate_dag = Column(
        TINYINT(4),
        nullable=False,
        server_default=text("'1'"),
        comment="1 = The DAG will be auto regenerated by manage command",
    )
    airflow_notes = Column(
        Text,
        comment="Documentation that will be available in Airflow. Markdown syntax.",
    )
    sudo_user = Column(
        String(64),
        comment="All tasks in DAG will use this user for sudo instead of default",
    )
    metadata_import = Column(
        TINYINT(4),
        nullable=False,
        server_default=text("'0'"),
        comment="1 = Run only getSchema and getRowCount, 0 = Run a normal import",
    )
    timezone = Column(
        String(64),
        comment="Timezone used for schedule_interval column. Use full text timezone, example Europe/Stockholm.",
    )
    email = Column(
        String(256),
        comment="Email to send message to in case email_on_retry or email_on_failure is set to True. Comma separated list of email addresses",
    )
    email_on_failure = Column(
        TINYINT(4),
        nullable=False,
        server_default=text("'0'"),
        comment="Send email on failures",
    )
    email_on_retries = Column(
        TINYINT(4),
        nullable=False,
        server_default=text("'0'"),
        comment="Send email on retries",
    )
    tags = Column(
        String(256),
        nullable=True,
        comment="Comma seperated list of Airflow tags that will be set on the Dag",
    )
    sla_warning_time = Column(
        Time,
        nullable=True,
        comment="Maximum time this DAG should run before Airflow triggers a SLA miss",
    )
    retry_exponential_backoff = Column(
        TINYINT(4),
        nullable=False,
        server_default=text("'0'"),
        comment="1 = Use the retry_exponential_backoff Airflow function that will cause the retry between failed tasks to be longer and longer each time instead of a fixed time, 0 = Run with a fixed time of 5 min between the task retries",
    )
    concurrency = Column(
        TINYINT(4),
        nullable=True,
        comment="Set the max number of concurrent tasks in the DAG while executing. Overrides the default value specified in Airflow configuration",
    )


class autoDiscoveredTable(Base):
    __tablename__ = "auto_discovered_tables"

    hive_db = Column(String(256), primary_key=True, nullable=False)
    dbalias = Column(String(256), primary_key=True, nullable=False)
    source_schema = Column(String(256), primary_key=True, nullable=False)
    source_table = Column(String(256), primary_key=True, nullable=False)
    discovery_time = Column(
        DateTime, nullable=False, server_default=text("CURRENT_TIMESTAMP")
    )
    migrate_to_import_tables = Column(
        TINYINT(4), nullable=False, server_default=text("'0'")
    )


class configuration(Base):
    __tablename__ = "configuration"
    __table_args__ = {
        "comment": "This is the DBImport global configuration table. In here you can configure items such as the name of the staging database, disable global executions, max number of mappers and much more. "
    }

    configKey = Column(
        String(32),
        primary_key=True,
        comment="Name of the configuration item. These are controlled and maintained by thee setup tool. Dont change these manually",
    )
    valueInt = Column(Integer, comment="Integer based Value")
    valueStr = Column(String(256), comment="String based Value")
    valueDate = Column(DateTime, comment="Date based Value")
    description = Column(Text, comment="Description on the setting")


class etlJobs(Base):
    __tablename__ = "etl_jobs"

    job = Column(String(64), primary_key=True, nullable=False)
    task = Column(String(64), primary_key=True, nullable=False)
    job_id = Column(Integer, nullable=False, unique=True, autoincrement=True)
    etl_type = Column(String(32), nullable=False, server_default=text("'0'"))
    include_in_airflow = Column(TINYINT(4), nullable=False, server_default=text("'1'"))
    source_db = Column(String(256))
    source_table = Column(String(256))
    target_db = Column(String(256))
    target_table = Column(String(256))
    operator_notes = Column(
        String(256), comment="Free text field to write a note about the import. "
    )


class exportRetriesLog(Base):
    __tablename__ = "export_retries_log"
    __table_args__ = {"comment": "Log of all retries that have happened. "}

    dbalias = Column(
        String(256),
        primary_key=True,
        nullable=False,
        comment="Database connection name that we export to",
    )
    target_schema = Column(
        String(256),
        primary_key=True,
        nullable=False,
        comment="Schema on the target system",
    )
    target_table = Column(
        String(256),
        primary_key=True,
        nullable=False,
        comment="Table on the target system",
    )
    retry_time = Column(
        DateTime,
        primary_key=True,
        nullable=False,
        comment="Time when the retry was started",
    )
    stage = Column(
        Integer,
        nullable=False,
        comment="The stage of the import that the retry started from. This is an internal stage and has nothing to do with stage1 and stage2 in Airflow DAG's",
    )
    stage_description = Column(Text, nullable=False, comment="Description of the stage")


class exportStage(Base):
    __tablename__ = "export_stage"
    __table_args__ = {
        "comment": 'The export tool keeps track of how far in the export the tool have succeeded. So in case of an error, lets say that Hive is not responding, the next time an export is executed it will skip the first part and continue from where it ended in error on the previous run. If you want to rerun from the begining, the information in this table needs to be cleared. This is done with the "manage --clearExportStage" tool. Keep in mind that clearing the stage of an incremental export might result in the loss of the data.'
    }

    dbalias = Column(
        String(256),
        primary_key=True,
        nullable=False,
        comment="Database connection name that we export to",
    )
    target_schema = Column(
        String(256),
        primary_key=True,
        nullable=False,
        comment="Schema on the target system",
    )
    target_table = Column(
        String(256),
        primary_key=True,
        nullable=False,
        comment="Table on the target system",
    )
    stage = Column(
        Integer,
        nullable=False,
        comment="Current stage of the export. This is the internal stage number",
    )
    stage_description = Column(Text, nullable=False, comment="Description of the stage")
    stage_time = Column(
        DateTime, comment="The date and time when the import entered the stage"
    )


class exportStageStatistic(Base):
    __tablename__ = "export_stage_statistics"
    __table_args__ = {
        "comment": "As DBImport progress through the different stages of the export, it also keeps track of start and stop time for each stage together with the duration. That information is kept in this table"
    }

    dbalias = Column(
        String(256),
        primary_key=True,
        nullable=False,
        comment="Database connection name that we export to",
    )
    target_schema = Column(
        String(256),
        primary_key=True,
        nullable=False,
        comment="Schema on the target system",
    )
    target_table = Column(
        String(256),
        primary_key=True,
        nullable=False,
        comment="Table on the target system",
    )
    stage = Column(
        Integer,
        primary_key=True,
        nullable=False,
        comment="Current stage of the export. This is the internal stage number",
    )
    start = Column(DateTime, nullable=False, comment="Time when stage started")
    stop = Column(DateTime, nullable=False, comment="Time when stage was completed")
    duration = Column(Integer, nullable=False, comment="Duration of stage")


class exportStatistic(Base):
    __tablename__ = "export_statistics"
    __table_args__ = (
        Index(
            "dbalias_target_schema_target_table",
            "dbalias",
            "target_schema",
            "target_table",
        ),
        {
            "comment": "At the end of each export, all statistics about how long each part took aswell as general information about Hive database and table, number of rows imported, size of the export and much more are logged in this table. This table grows and will never be truncated by DBImport itself. If it becomes to large for you, it's up to each user to delete or truncate this table as you see fit."
        },
    )

    id = Column(
        BIGINT(20),
        primary_key=True,
        autoincrement=True,
        comment="Auto incremented PrimaryKey of the table",
    )
    dbalias = Column(String(256), comment="ID of the Database Connection")
    target_database = Column(String(256), comment="Name of the source database")
    target_schema = Column(String(256), comment="Name of the source schema")
    target_table = Column(String(256), comment="Name of the source table")
    hive_db = Column(String(256), comment="Hive Database")
    hive_table = Column(String(256), comment="Hive Table")
    export_phase = Column(String(32), comment="Import Phase method")
    incremental = Column(TINYINT(4), comment="0 = Full import, 1 = Incremental import")
    rows = Column(BIGINT(20), comment="How many rows that was imported")
    size = Column(BIGINT(20), comment="The total size in bytes that was imported")
    sessions = Column(
        TINYINT(4),
        comment="How many parallell sessions was used against the source (sqoop mappers)",
    )
    duration = Column(Integer, comment="Tota duration in seconds")
    start = Column(DateTime, comment="Timestamp of start")
    stop = Column(DateTime, comment="Timestamp of stop")
    get_hive_tableschema_duration = Column(Integer)
    get_hive_tableschema_start = Column(DateTime)
    get_hive_tableschema_stop = Column(DateTime)
    clear_table_rowcount_duration = Column(Integer)
    clear_table_rowcount_start = Column(DateTime)
    clear_table_rowcount_stop = Column(DateTime)
    create_temp_table_duration = Column(Integer)
    create_temp_table_start = Column(DateTime)
    create_temp_table_stop = Column(DateTime)
    truncate_temp_table_duration = Column(Integer)
    truncate_temp_table_start = Column(DateTime)
    truncate_temp_table_stop = Column(DateTime)
    fetch_maxvalue_start = Column(DateTime)
    fetch_maxvalue_stop = Column(DateTime)
    fetch_maxvalue_duration = Column(Integer)
    insert_into_temp_table_duration = Column(Integer)
    insert_into_temp_table_start = Column(DateTime)
    insert_into_temp_table_stop = Column(DateTime)
    create_target_table_duration = Column(Integer)
    create_target_table_start = Column(DateTime)
    create_target_table_stop = Column(DateTime)
    update_target_table_duration = Column(Integer)
    update_target_table_start = Column(DateTime)
    update_target_table_stop = Column(DateTime)
    truncate_target_table_duration = Column(Integer)
    truncate_target_table_start = Column(DateTime)
    truncate_target_table_stop = Column(DateTime)
    sqoop_duration = Column(Integer)
    sqoop_start = Column(DateTime)
    sqoop_stop = Column(DateTime)
    spark_duration = Column(Integer)
    spark_start = Column(DateTime)
    spark_stop = Column(DateTime)
    validate_duration = Column(Integer)
    validate_start = Column(DateTime)
    validate_stop = Column(DateTime)
    update_statistics_duration = Column(Integer)
    update_statistics_start = Column(DateTime)
    update_statistics_stop = Column(DateTime)
    atlas_schema_duration = Column(Integer)
    atlas_schema_start = Column(DateTime)
    atlas_schema_stop = Column(DateTime)


class exportStatisticsLast(Base):
    __tablename__ = "export_statistics_last"
    __table_args__ = {
        "comment": "The last entry in table export_statistics is also stored in this table. This makes it easier to find the latest data without first grouping to find the latest entry. When export_statistics table grows to a high number of million rows, it saves alot of cpu power for the database server."
    }

    dbalias = Column(
        String(256),
        primary_key=True,
        nullable=False,
        comment="ID of the Database Connection",
    )
    target_database = Column(
        String(256), nullable=False, comment="Name of the source database"
    )
    target_schema = Column(
        String(256),
        primary_key=True,
        nullable=False,
        comment="Name of the source schema",
    )
    target_table = Column(
        String(256),
        primary_key=True,
        nullable=False,
        comment="Name of the source table",
    )
    hive_db = Column(String(256), comment="Hive Database")
    hive_table = Column(String(256), comment="Hive Table")
    export_phase = Column(String(32), comment="Import Phase method")
    incremental = Column(TINYINT(4), comment="0 = Full import, 1 = Incremental import")
    rows = Column(BIGINT(20), comment="How many rows that was imported")
    size = Column(BIGINT(20), comment="The total size in bytes that was imported")
    sessions = Column(
        TINYINT(4),
        comment="How many parallell sessions was used against the source (sqoop mappers)",
    )
    duration = Column(Integer, comment="Tota duration in seconds")
    start = Column(DateTime, comment="Timestamp of start")
    stop = Column(DateTime, comment="Timestamp of stop")
    get_hive_tableschema_duration = Column(Integer)
    get_hive_tableschema_start = Column(DateTime)
    get_hive_tableschema_stop = Column(DateTime)
    clear_table_rowcount_duration = Column(Integer)
    clear_table_rowcount_start = Column(DateTime)
    clear_table_rowcount_stop = Column(DateTime)
    create_temp_table_duration = Column(Integer)
    create_temp_table_start = Column(DateTime)
    create_temp_table_stop = Column(DateTime)
    truncate_temp_table_duration = Column(Integer)
    truncate_temp_table_start = Column(DateTime)
    truncate_temp_table_stop = Column(DateTime)
    fetch_maxvalue_start = Column(DateTime)
    fetch_maxvalue_stop = Column(DateTime)
    fetch_maxvalue_duration = Column(Integer)
    insert_into_temp_table_duration = Column(Integer)
    insert_into_temp_table_start = Column(DateTime)
    insert_into_temp_table_stop = Column(DateTime)
    create_target_table_duration = Column(Integer)
    create_target_table_start = Column(DateTime)
    create_target_table_stop = Column(DateTime)
    update_target_table_duration = Column(Integer)
    update_target_table_start = Column(DateTime)
    update_target_table_stop = Column(DateTime)
    truncate_target_table_duration = Column(Integer)
    truncate_target_table_start = Column(DateTime)
    truncate_target_table_stop = Column(DateTime)
    sqoop_duration = Column(Integer)
    sqoop_start = Column(DateTime)
    sqoop_stop = Column(DateTime)
    spark_duration = Column(Integer)
    spark_start = Column(DateTime)
    spark_stop = Column(DateTime)
    validate_duration = Column(Integer)
    validate_start = Column(DateTime)
    validate_stop = Column(DateTime)
    update_statistics_duration = Column(Integer)
    update_statistics_start = Column(DateTime)
    update_statistics_stop = Column(DateTime)
    atlas_schema_duration = Column(Integer)
    atlas_schema_start = Column(DateTime)
    atlas_schema_stop = Column(DateTime)


class exportTables(Base):
    __tablename__ = "export_tables"
    __table_args__ = {
        "comment": "Main table where all tables that we can export are stored. "
    }

    dbalias = Column(
        String(256),
        primary_key=True,
        nullable=False,
        comment="Database connection name that we export to",
    )
    target_schema = Column(
        String(256),
        primary_key=True,
        nullable=False,
        comment="Schema on the target system",
    )
    target_table = Column(
        String(256),
        primary_key=True,
        nullable=False,
        comment="Table on the target system",
    )
    table_id = Column(
        Integer,
        nullable=False,
        unique=True,
        autoincrement=True,
        comment="Unique identifier of the table",
    )
    export_type = Column(
        Enum("full", "incr"),
        nullable=False,
        comment="What export method to use",
        server_default=text("'full'"),
    )
    export_tool = Column(
        Enum("spark", "sqoop"),
        server_default=text("'spark'"),
        nullable=False,
        comment="What tool should be used for exporting data",
    )
    hive_db = Column(
        String(256), nullable=False, comment="Name of Hive Database that we export from"
    )
    hive_table = Column(
        String(256), nullable=False, comment="Name of Hive Table that we export from"
    )
    last_update_from_hive = Column(
        DateTime, comment="Timestamp of last schema update from Hive"
    )
    sql_where_addition = Column(
        String(1024),
        comment='Will be added AFTER the SQL WHERE. If it\'s an incr export, this will be after the incr limit statements. Example "orderId > 1000"',
    )
    include_in_airflow = Column(
        TINYINT(4),
        nullable=False,
        comment="Will the table be included in Airflow DAG when it matches the DAG selection",
        server_default=text("'1'"),
    )
    airflow_priority = Column(
        TINYINT(4), comment="This will set priority_weight in Airflow"
    )
    notUsed01 = Column(
        TINYINT(4), nullable=False, comment="<NOT USED>", server_default=text("'0'")
    )
    forceCreateTempTable = Column(
        TINYINT(4),
        nullable=False,
        comment="Force export to create a Hive table and export that instead. Useful when exporting views",
        server_default=text("'0'"),
    )
    notUsed02 = Column(
        TINYINT(4), nullable=False, comment="<NOT USED>", server_default=text("'0'")
    )
    validate_export = Column(
        TINYINT(4),
        nullable=False,
        comment="1 = Validate the export once it's done. 0 = Disable validation",
        server_default=text("'1'"),
    )
    validationMethod = Column(
        Enum("customQuery", "rowCount"),
        nullable=False,
        comment="Validation method to use",
        server_default=text("'rowCount'"),
    )
    validationCustomQueryHiveSQL = Column(
        Text, comment="Custom SQL query for Hive table"
    )
    validationCustomQueryTargetSQL = Column(
        Text, comment="Custom SQL query for target table"
    )
    uppercase_columns = Column(
        TINYINT(4),
        nullable=False,
        comment="-1 = auto (Oracle = uppercase, other databases = lowercase)",
        server_default=text("'-1'"),
    )
    truncate_target = Column(
        TINYINT(4),
        nullable=False,
        comment="1 = Truncate the target table before we export the data. Not used by incremental exports",
        server_default=text("'1'"),
    )
    mappers = Column(
        TINYINT(4),
        nullable=False,
        comment="-1 = auto, 0 = invalid. Auto updated by 'export_main.sh'",
        server_default=text("'-1'"),
    )
    hive_rowcount = Column(
        BIGINT(20), comment="Number of rows in Hive table. Dont change manually"
    )
    target_rowcount = Column(
        BIGINT(20), comment="Number of rows in Target table. Dont change manually"
    )
    validationCustomQueryHiveValue = Column(
        Text, comment="Used for validation. Dont change manually"
    )
    validationCustomQueryTargetValue = Column(
        Text, comment="Used for validation. Dont change manually"
    )
    incr_column = Column(
        String(256),
        comment="The column in the Hive table that will be used to identify new rows for the incremental export. Must be a timestamp column",
    )
    incr_validation_method = Column(
        Enum("full", "incr"),
        comment="full or incr. Full means that the validation will check to total number of rows up until maxvalue and compare source with target. Incr will only compare the rows between min and max value (the data that sqoop just wrote)",
        server_default=text("'full'"),
    )
    incr_minvalue = Column(
        String(32),
        comment="Used by incremental exports to keep track of progress. Dont change manually",
    )
    incr_maxvalue = Column(
        String(32),
        comment="Used by incremental exports to keep track of progress. Dont change manually",
    )
    incr_minvalue_pending = Column(
        String(32),
        comment="Used by incremental exports to keep track of progress. Dont change manually",
    )
    incr_maxvalue_pending = Column(
        String(32),
        comment="Used by incremental exports to keep track of progress. Dont change manually",
    )
    sqoop_options = Column(String(1024), comment="Sqoop options to use during export.")
    sqoop_last_size = Column(
        BIGINT(20), comment="Used to track sqoop operation. Dont change manually"
    )
    sqoop_last_rows = Column(
        BIGINT(20), comment="Used to track sqoop operation. Dont change manually"
    )
    sqoop_last_mappers = Column(
        TINYINT(4), comment="Used to track sqoop operation. Dont change manually"
    )
    sqoop_last_execution = Column(
        BIGINT(20), comment="Used to track sqoop operation. Dont change manually"
    )
    hive_javaheap = Column(BIGINT(20), comment="Heap size for Hive")
    create_target_table_sql = Column(
        Text,
        comment="SQL statement that was used to create the target table. Dont change manually",
    )
    operator_notes = Column(
        Text, comment="Free text field to write a note about the export. "
    )


class importFailureLog(Base):
    __tablename__ = "import_failure_log"
    __table_args__ = (
        Index("hive_db_hive_table", "hive_db", "hive_table"),
        {
            "comment": "If there is an error or a warning during import, bu the import still continues, these errors are logged in this table. An example could be that  a column cant be altered, foreign key not created, no new columns can be added and such."
        },
    )

    hive_db = Column(
        String(256), primary_key=True, nullable=False, comment="Hive Database"
    )
    hive_table = Column(
        String(256), primary_key=True, nullable=False, comment="Hive Table"
    )
    eventtime = Column(
        DateTime,
        primary_key=True,
        nullable=False,
        comment="Time when error/warning occurred",
        server_default=text("CURRENT_TIMESTAMP"),
    )
    severity = Column(Text, comment="The Severity of the event. ")
    import_type = Column(Text, comment="The import method used")
    error_text = Column(Text, comment="Text describing the failure")


# t_import_foreign_keys_VIEW = Table(
#    'import_foreign_keys_VIEW', metadata,
#    Column('hive_db', String(256)),
#    Column('hive_table', String(256)),
#    Column('fk_index', INTEGER(10)),
#    Column('column_name', String(256)),
#    Column('ref_hive_Db', String(256)),
#    Column('ref_hive_table', String(256)),
#    Column('ref_column_name', String(256))
# )


class importRetriesLog(Base):
    __tablename__ = "import_retries_log"
    __table_args__ = {"comment": "Log of all retries that have happened. "}

    hive_db = Column(String(256), primary_key=True, nullable=False, comment="Hive DB")
    hive_table = Column(
        String(256), primary_key=True, nullable=False, comment="Hive Table"
    )
    retry_time = Column(
        DateTime,
        primary_key=True,
        nullable=False,
        comment="Time when the retry was started",
    )
    stage = Column(
        Integer,
        nullable=False,
        comment="The stage of the import that the retry started from. This is an internal stage and has nothing to do with stage1 and stage2 in Airflow DAG's",
    )
    stage_description = Column(Text, nullable=False, comment="Description of the stage")


class importStage(Base):
    __tablename__ = "import_stage"
    __table_args__ = {
        "comment": 'The import tool keeps track of how far in the import the tool have succeeded. So in case of an error, lets say that Hive is not responding, the next time an import is executed it will skip the first part and continue from where it ended in error on the previous run. If you want to rerun from the begining, the information in this table needs to be cleared. This is done with the "manage --clearImportStage" tool. Keep in mind that clearing the stage of an incremental import might result in the loss of the data.'
    }

    hive_db = Column(
        String(256), primary_key=True, nullable=False, comment="Hive Database"
    )
    hive_table = Column(
        String(256), primary_key=True, nullable=False, comment="Hive Table"
    )
    stage = Column(
        Integer,
        primary_key=True,
        nullable=False,
        comment="Current stage of the import. This is an internal stage and has nothing to do with stage1 and stage2 in Airflow DAG's",
    )
    stage_description = Column(Text, nullable=False, comment="Description of the stage")
    stage_time = Column(
        DateTime, comment="The date and time when the import entered the stage"
    )


class importStageStatistic(Base):
    __tablename__ = "import_stage_statistics"
    __table_args__ = {
        "comment": "As DBImport progress through the different stages of the import, it also keeps track of start and stop time for each stage together with the duration. That information is kept in this table"
    }

    hive_db = Column(
        String(256), primary_key=True, nullable=False, comment="Hive Database"
    )
    hive_table = Column(
        String(256), primary_key=True, nullable=False, comment="Hive Table"
    )
    stage = Column(
        Integer,
        primary_key=True,
        nullable=False,
        comment="Current stage of the import. This is an internal stage and has nothing to do with stage1 and stage2 in Airflow DAG's",
    )
    start = Column(DateTime, nullable=False, comment="Time when stage started")
    stop = Column(DateTime, nullable=False, comment="Time when stage was completed")
    duration = Column(Integer, nullable=False, comment="Duration of stage")


class importStatistic(Base):
    __tablename__ = "import_statistics"
    __table_args__ = (
        Index("hive_db_hive_table", "hive_db", "hive_table"),
        {
            "comment": "At the end of each import, all statistics about how long each part took aswell as general information about Hive datbase and table, number of rows imported, size of the import and much more are logged in this table. This table grows and will never be truncated by DBImport itself. If it becomes to large for you, it's up to each user to delete or truncate this table as you see fit."
        },
    )

    id = Column(
        BIGINT(20),
        primary_key=True,
        autoincrement=True,
        comment="Auto incremented PrimaryKey of the table",
    )
    hive_db = Column(String(256), comment="Hive Database")
    hive_table = Column(String(256), comment="Hive Table")
    importtype = Column(String(32), comment="What kind of import type that was used")
    import_phase = Column(String(32), comment="Import Phase method")
    copy_phase = Column(String(32), comment="Copy Phase method")
    etl_phase = Column(String(32), comment="ETL Phase method")
    incremental = Column(TINYINT(4), comment="0 = Full import, 1 = Incremental import")
    dbalias = Column(String(256), comment="ID of the Database Connection")
    source_database = Column(String(256), comment="Name of the source database")
    source_schema = Column(String(256), comment="Name of the source schema")
    source_table = Column(String(256), comment="Name of the source table")
    rows = Column(BIGINT(20), comment="How many rows that was imported")
    size = Column(BIGINT(20), comment="The total size in bytes that was imported")
    sessions = Column(
        TINYINT(4),
        comment="How many parallell sessions was used against the source (sqoop mappers)",
    )
    duration = Column(Integer, comment="Tota duration in seconds")
    start = Column(DateTime, comment="Timestamp of start")
    stop = Column(DateTime, comment="Timestamp of stop")
    sqoop_duration = Column(Integer)
    sqoop_start = Column(DateTime)
    sqoop_stop = Column(DateTime)
    spark_duration = Column(Integer)
    spark_start = Column(DateTime)
    spark_stop = Column(DateTime)
    spark_etl_duration = Column(Integer)
    spark_etl_start = Column(DateTime)
    spark_etl_stop = Column(DateTime)
    clear_hive_locks_duration = Column(Integer)
    clear_hive_locks_start = Column(DateTime)
    clear_hive_locks_stop = Column(DateTime)
    clear_table_rowcount_duration = Column(Integer)
    clear_table_rowcount_start = Column(DateTime)
    clear_table_rowcount_stop = Column(DateTime)
    connect_to_hive_duration = Column(Integer)
    connect_to_hive_start = Column(DateTime)
    connect_to_hive_stop = Column(DateTime)
    create_import_table_duration = Column(Integer)
    create_import_table_start = Column(DateTime)
    create_import_table_stop = Column(DateTime)
    create_target_table_duration = Column(Integer)
    create_target_table_start = Column(DateTime)
    create_target_table_stop = Column(DateTime)
    get_import_rowcount_duration = Column(Integer)
    get_import_rowcount_start = Column(DateTime)
    get_import_rowcount_stop = Column(DateTime)
    get_source_rowcount_duration = Column(Integer)
    get_source_rowcount_start = Column(DateTime)
    get_source_rowcount_stop = Column(DateTime)
    get_source_tableschema_duration = Column(Integer)
    get_source_tableschema_start = Column(DateTime)
    get_source_tableschema_stop = Column(DateTime)
    get_target_rowcount_duration = Column(Integer)
    get_target_rowcount_start = Column(DateTime)
    get_target_rowcount_stop = Column(DateTime)
    hive_import_duration = Column(Integer)
    hive_import_start = Column(DateTime)
    hive_import_stop = Column(DateTime)
    truncate_target_table_duration = Column(Integer)
    truncate_target_table_start = Column(DateTime)
    truncate_target_table_stop = Column(DateTime)
    merge_table_duration = Column(Integer)
    merge_table_start = Column(DateTime)
    merge_table_stop = Column(DateTime)
    create_history_table_duration = Column(Integer)
    create_history_table_start = Column(DateTime)
    create_history_table_stop = Column(DateTime)
    create_delete_table_duration = Column(Integer)
    create_delete_table_start = Column(DateTime)
    create_delete_table_stop = Column(DateTime)
    update_statistics_duration = Column(Integer)
    update_statistics_start = Column(DateTime)
    update_statistics_stop = Column(DateTime)
    validate_import_table_duration = Column(Integer)
    validate_import_table_start = Column(DateTime)
    validate_import_table_stop = Column(DateTime)
    validate_sqoop_import_duration = Column(Integer)
    validate_sqoop_import_start = Column(DateTime)
    validate_sqoop_import_stop = Column(DateTime)
    validate_target_table_duration = Column(Integer)
    validate_target_table_start = Column(DateTime)
    validate_target_table_stop = Column(DateTime)
    copy_data_duration = Column(Integer)
    copy_data_start = Column(DateTime)
    copy_data_stop = Column(DateTime)
    copy_schema_duration = Column(Integer)
    copy_schema_start = Column(DateTime)
    copy_schema_stop = Column(DateTime)
    atlas_schema_duration = Column(Integer)
    atlas_schema_start = Column(DateTime)
    atlas_schema_stop = Column(DateTime)


class importStatisticsLast(Base):
    __tablename__ = "import_statistics_last"
    __table_args__ = {
        "comment": "The last entry in table import_statistics is also stored in this table. This makes it easier to find the latest data without first grouping to find the latest entry. When import_statistics table grows to a high number of million rows, it saves alot of cpu power for the database server."
    }

    hive_db = Column(String(256), primary_key=True, comment="Hive Database")
    hive_table = Column(String(256), primary_key=True, comment="Hive Table")
    importtype = Column(String(32), comment="What kind of import type that was used")
    import_phase = Column(String(32), comment="Import Phase method")
    copy_phase = Column(String(32), comment="Copy Phase method")
    etl_phase = Column(String(32), comment="ETL Phase method")
    incremental = Column(TINYINT(4), comment="0 = Full import, 1 = Incremental import")
    dbalias = Column(String(256), comment="ID of the Database Connection")
    source_database = Column(String(256), comment="Name of the source database")
    source_schema = Column(String(256), comment="Name of the source schema")
    source_table = Column(String(256), comment="Name of the source table")
    rows = Column(BIGINT(20), comment="How many rows that was imported")
    size = Column(BIGINT(20), comment="The total size in bytes that was imported")
    sessions = Column(
        TINYINT(4),
        comment="How many parallell sessions was used against the source (sqoop mappers)",
    )
    duration = Column(Integer, comment="Tota duration in seconds")
    start = Column(DateTime, comment="Timestamp of start")
    stop = Column(DateTime, comment="Timestamp of stop")
    sqoop_duration = Column(Integer)
    sqoop_start = Column(DateTime)
    sqoop_stop = Column(DateTime)
    spark_duration = Column(Integer)
    spark_start = Column(DateTime)
    spark_stop = Column(DateTime)
    spark_etl_duration = Column(Integer)
    spark_etl_start = Column(DateTime)
    spark_etl_stop = Column(DateTime)
    clear_hive_locks_duration = Column(Integer)
    clear_hive_locks_start = Column(DateTime)
    clear_hive_locks_stop = Column(DateTime)
    clear_table_rowcount_duration = Column(Integer)
    clear_table_rowcount_start = Column(DateTime)
    clear_table_rowcount_stop = Column(DateTime)
    connect_to_hive_duration = Column(Integer)
    connect_to_hive_start = Column(DateTime)
    connect_to_hive_stop = Column(DateTime)
    create_import_table_duration = Column(Integer)
    create_import_table_start = Column(DateTime)
    create_import_table_stop = Column(DateTime)
    create_target_table_duration = Column(Integer)
    create_target_table_start = Column(DateTime)
    create_target_table_stop = Column(DateTime)
    get_import_rowcount_duration = Column(Integer)
    get_import_rowcount_start = Column(DateTime)
    get_import_rowcount_stop = Column(DateTime)
    get_source_rowcount_duration = Column(Integer)
    get_source_rowcount_start = Column(DateTime)
    get_source_rowcount_stop = Column(DateTime)
    get_source_tableschema_duration = Column(Integer)
    get_source_tableschema_start = Column(DateTime)
    get_source_tableschema_stop = Column(DateTime)
    get_target_rowcount_duration = Column(Integer)
    get_target_rowcount_start = Column(DateTime)
    get_target_rowcount_stop = Column(DateTime)
    hive_import_duration = Column(Integer)
    hive_import_start = Column(DateTime)
    hive_import_stop = Column(DateTime)
    truncate_target_table_duration = Column(Integer)
    truncate_target_table_start = Column(DateTime)
    truncate_target_table_stop = Column(DateTime)
    merge_table_duration = Column(Integer)
    merge_table_start = Column(DateTime)
    merge_table_stop = Column(DateTime)
    create_history_table_duration = Column(Integer)
    create_history_table_start = Column(DateTime)
    create_history_table_stop = Column(DateTime)
    create_delete_table_duration = Column(Integer)
    create_delete_table_start = Column(DateTime)
    create_delete_table_stop = Column(DateTime)
    update_statistics_duration = Column(Integer)
    update_statistics_start = Column(DateTime)
    update_statistics_stop = Column(DateTime)
    validate_import_table_duration = Column(Integer)
    validate_import_table_start = Column(DateTime)
    validate_import_table_stop = Column(DateTime)
    validate_sqoop_import_duration = Column(Integer)
    validate_sqoop_import_start = Column(DateTime)
    validate_sqoop_import_stop = Column(DateTime)
    validate_target_table_duration = Column(Integer)
    validate_target_table_start = Column(DateTime)
    validate_target_table_stop = Column(DateTime)
    copy_data_duration = Column(Integer)
    copy_data_start = Column(DateTime)
    copy_data_stop = Column(DateTime)
    copy_schema_duration = Column(Integer)
    copy_schema_start = Column(DateTime)
    copy_schema_stop = Column(DateTime)
    atlas_schema_duration = Column(Integer)
    atlas_schema_start = Column(DateTime)
    atlas_schema_stop = Column(DateTime)


class importTables(Base):
    __tablename__ = "import_tables"
    __table_args__ = (
        Index(
            "hive_db_dbalias_source_schema_source_table",
            "hive_db",
            "dbalias",
            "source_schema",
            "source_table",
        ),
        {"comment": "Main table where all tables that we can import are stored. "},
    )

    hive_db = Column(
        String(256),
        primary_key=True,
        nullable=False,
        comment="Hive Database to import to",
    )
    hive_table = Column(
        String(256), primary_key=True, nullable=False, comment="Hive Table to import to"
    )
    table_id = Column(
        Integer,
        nullable=False,
        unique=True,
        autoincrement=True,
        comment="Unique identifier",
    )
    dbalias = Column(
        String(256),
        nullable=False,
        comment="Name of database connection from jdbc_connections table",
    )
    source_schema = Column(
        String(256), nullable=False, comment="Name of the schema in the remote database"
    )
    source_table = Column(
        String(256), nullable=False, comment="Name of the table in the remote database"
    )
    import_type = Column(String(32), nullable=True, comment="What import method to use")
    import_phase_type = Column(
        Enum("full", "incr", "oracle_flashback", "mssql_change_tracking"),
        nullable=True,
        comment="What method to use for Import phase",
        server_default=text("'full'"),
    )
    etl_phase_type = Column(
        Enum(
            "truncate_insert",
            "insert",
            "merge",
            "merge_history_audit",
            "none",
            "external",
        ),
        nullable=True,
        comment="What method to use for ETL phase",
        server_default=text("'truncate_insert'"),
    )
    import_tool = Column(
        Enum("spark", "sqoop"),
        server_default=text("'sqoop'"),
        nullable=False,
        comment="What tool should be used for importing data",
    )
    etl_engine = Column(
        Enum("hive", "spark"),
        server_default=text("'hive'"),
        nullable=False,
        comment="What engine will be used to process etl stage",
    )
    last_update_from_source = Column(
        DateTime, comment="Timestamp of last schema update from source"
    )
    sqoop_sql_where_addition = Column(
        String(1024),
        comment='Will be added AFTER the SQL WHERE. If it\'s an incr import, this will be after the incr limit statements. Example "orderId > 1000"',
    )
    nomerge_ingestion_sql_addition = Column(
        String(2048),
        comment="This will be added to the data ingestion of None-Merge imports (full, full_direct and incr). Usefull to filter out data from import tables to target tables",
    )
    include_in_airflow = Column(
        TINYINT(4),
        nullable=False,
        comment="Will the table be included in Airflow DAG when it matches the DAG selection",
        server_default=text("'1'"),
    )
    airflow_priority = Column(
        TINYINT(4), comment="This will set priority_weight in Airflow"
    )
    validate_import = Column(
        TINYINT(4),
        nullable=False,
        comment="Should the import be validated",
        server_default=text("'1'"),
    )
    validationMethod = Column(
        Enum("customQuery", "rowCount"),
        nullable=False,
        comment="Validation method to use",
        server_default=text("'rowCount'"),
    )
    validate_source = Column(
        Enum("query", "sqoop"),
        comment="query = Run a 'select count(*) from ...' to get the number of rows in the source table. sqoop = Use the number of rows imported by sqoop as the number of rows in the source table",
        server_default=text("'query'"),
    )
    validate_diff_allowed = Column(
        BIGINT(20),
        nullable=False,
        comment="-1 = auto calculated diff allowed. If a positiv number, this is the amount of rows that the diff is allowed to have",
        server_default=text("'-1'"),
    )
    validationCustomQuerySourceSQL = Column(
        Text, comment="Custom SQL query for source table"
    )
    validationCustomQueryHiveSQL = Column(
        Text, comment="Custom SQL query for Hive table"
    )
    validationCustomQueryValidateImportTable = Column(
        TINYINT(4),
        nullable=False,
        comment="1 = Validate Import table, 0 = Dont validate Import table",
        server_default=text("'1'"),
    )
    truncate_hive = Column(
        TINYINT(4), nullable=False, comment="<NOT USED>", server_default=text("'1'")
    )
    mappers = Column(
        TINYINT(4),
        nullable=False,
        comment="-1 = auto or positiv number for a fixed number of mappers. If Auto, then it's calculated based of last sqoop import size",
        server_default=text("'-1'"),
    )
    soft_delete_during_merge = Column(
        TINYINT(4),
        nullable=False,
        comment="If 1, then the row will be marked as deleted instead of actually being removed from the table. Only used for Merge imports",
        server_default=text("'0'"),
    )
    source_rowcount = Column(
        BIGINT(20), comment="Used for validation. Dont change manually"
    )
    source_rowcount_incr = Column(BIGINT(20))
    hive_rowcount = Column(
        BIGINT(20), comment="Used for validation. Dont change manually"
    )
    validationCustomQuerySourceValue = Column(
        Text, comment="Used for validation. Dont change manually"
    )
    validationCustomQueryHiveValue = Column(
        Text, comment="Used for validation. Dont change manually"
    )
    incr_mode = Column(
        Enum("append", "lastmodified"), comment="Incremental import mode"
    )
    incr_column = Column(String(256), comment="What column to use to identify new rows")
    incr_validation_method = Column(
        Enum("full", "incr"),
        comment="full or incr. Full means that the validation will check to total number of rows up until maxvalue and compare source with target. Incr will only compare the rows between min and max value (the data that sqoop just wrote)",
        server_default=text("'full'"),
    )
    incr_minvalue = Column(
        String(32), comment="Used for incremental imports. Dont change manually"
    )
    incr_maxvalue = Column(
        String(32), comment="Used for incremental imports. Dont change manually"
    )
    incr_minvalue_pending = Column(
        String(32), comment="Used for incremental imports. Dont change manually"
    )
    incr_maxvalue_pending = Column(
        String(32), comment="Used for incremental imports. Dont change manually"
    )
    pk_column_override = Column(
        String(1024),
        comment="Force the import and Hive table to define another PrimaryKey constraint. Comma separeted list of columns",
    )
    pk_column_override_mergeonly = Column(
        String(1024),
        comment="Force the import to use another PrimaryKey constraint during Merge operations. Comma separeted list of columns",
    )
    hive_merge_heap = Column(
        Integer,
        comment="Should be a multiple of Yarn container size. If NULL then it will use the default specified in Yarn and TEZ",
    )
    hive_split_count = Column(
        Integer, comment="Sets tez.grouping.split-count in the Hive session"
    )
    spark_executor_memory = Column(
        String(8),
        comment="Memory used by spark when importing data. Overrides default value in global configuration",
    )
    spark_executors = Column(
        Integer,
        comment="Number of Spark executors to use. Overrides default value in global configuration",
    )
    concatenate_hive_table = Column(
        TINYINT(4), nullable=False, comment="<NOT USED>", server_default=text("'-1'")
    )
    split_by_column = Column(
        String(64),
        comment="Column to split by when doing import with multiple sessions",
    )
    sqoop_query = Column(
        Text, comment="Use a custom query in sqoop to read data from source table"
    )
    sqoop_options = Column(Text, comment="Options to send to sqoop.")
    sqoop_last_size = Column(
        BIGINT(20), comment="Used to track sqoop operation. Dont change manually"
    )
    sqoop_last_rows = Column(
        BIGINT(20), comment="Used to track sqoop operation. Dont change manually"
    )
    sqoop_last_mappers = Column(
        TINYINT(4), comment="Used to track sqoop operation. Dont change manually"
    )
    sqoop_last_execution = Column(
        BIGINT(20), comment="Used to track sqoop operation. Dont change manually"
    )
    sqoop_use_generated_sql = Column(
        TINYINT(4),
        nullable=False,
        comment="1 = Use the generated SQL that is saved in the generated_sqoop_query column",
        server_default=text("'-1'"),
    )
    sqoop_allow_text_splitter = Column(
        TINYINT(4),
        nullable=False,
        comment="Allow splits on text columns. Use with caution",
        server_default=text("'0'"),
    )
    force_string = Column(
        TINYINT(4),
        nullable=False,
        comment="If set to 1, all character based fields (char, varchar) will become string in Hive. Overrides the same setting in jdbc_connections table",
        server_default=text("'-1'"),
    )
    comment = Column(
        Text, comment="Table comment from source system. Dont change manually"
    )
    generated_hive_column_definition = Column(
        Text,
        comment="Generated column definition for Hive create table. Dont change manually",
    )
    generated_sqoop_query = Column(
        Text, comment="Generated query for sqoop. Dont change manually"
    )
    generated_sqoop_options = Column(
        Text, comment="Generated options for sqoop. Dont change manually"
    )
    generated_pk_columns = Column(
        Text, comment="Generated Primary Keys. Dont change manually"
    )
    generated_foreign_keys = Column(Text, comment="<NOT USED>")
    datalake_source = Column(
        String(256),
        comment="This value will come in the dbimport_source column if present. Overrides the same setting in jdbc_connections table",
    )
    operator_notes = Column(
        Text, comment="Free text field to write a note about the import. "
    )
    copy_finished = Column(
        DateTime,
        comment="Time when last copy from Master DBImport instance was completed. Dont change manually",
    )
    copy_slave = Column(
        TINYINT(4),
        nullable=False,
        comment="Defines if this table is a Master table or a Slave table. Dont change manually",
        server_default=text("'0'"),
    )
    create_foreign_keys = Column(
        TINYINT(4),
        nullable=False,
        comment="-1 (default) = Get information from jdbc_connections table",
        server_default=text("'-1'"),
    )
    invalidate_impala = Column(
        TINYINT(4),
        nullable=False,
        comment="-1 = Use default value from configuration table, otherwise 1 will invalidate the table in Impala and 0 will not",
        server_default=text("'-1'"),
    )
    custom_max_query = Column(
        String(256),
        comment="You can use a custom SQL query that will get the Max value from the source database. This Max value will be used in an inremental import to know how much to read in each execution",
    )
    mergeCompactionMethod = Column(
        Enum("default", "none", "minor", "minor_and_wait", "major", "major_and_wait"),
        nullable=False,
        comment="Compaction method to use after import using merge is completed. Default means a major compaction if it is configured to do so in the configuration table",
        server_default=text("'default'"),
    )
    sourceTableType = Column(
        String(16), comment="Type of table on the source system. This is a read-only"
    )
    import_database = Column(
        String(256), comment="Override the database name used for the import table"
    )
    import_table = Column(
        String(256), comment="Override the table name used for the import table"
    )
    history_database = Column(
        String(256),
        comment="Override the database name used for the history table if that exists",
    )
    history_table = Column(
        String(256),
        comment="Override the table name used for the history table if that exists",
    )


class jdbcConnectionsEnvironments(Base):
    __tablename__ = "jdbc_connections_environments"
    __table_args__ = {
        "comment": "Environments types. Used in jdbc_connections table to define what kind of connection type it is"
    }

    environment = Column(
        String(32), primary_key=True, comment="Name of the Environment type"
    )
    description = Column(
        String(256), nullable=True, comment="Description of the Environment type"
    )


class jdbcConnections(Base):
    __tablename__ = "jdbc_connections"
    __table_args__ = {"comment": "Database connection definitions"}

    dbalias = Column(
        String(256), primary_key=True, comment="Name of the Database connection"
    )
    private_key_path = Column(
        String(128),
        comment="Custom PrivateKey to encrypt/decrypt the credentials field",
    )
    public_key_path = Column(
        String(128), comment="Custom PublicKey to encrypt/decrypt the credentials field"
    )
    jdbc_url = Column(Text, nullable=False, comment="The JDBC URL String")
    credentials = Column(
        Text,
        comment="Encrypted fields for credentials.m Changed by the saveCredentialTool",
    )
    datalake_source = Column(
        String(256),
        comment="This value will come in the dbimport_source column if present. Priority is table, connection",
    )
    max_import_sessions = Column(
        TINYINT(4),
        comment="You can limit the number of parallel sessions during import with this value. If NULL, then Max will come from configuration file",
    )
    force_string = Column(
        TINYINT(4),
        nullable=False,
        comment="If set to 1, all character based fields (char, varchar) will become string in Hive",
        server_default=text("'0'"),
    )
    create_datalake_import = Column(
        TINYINT(4),
        nullable=False,
        comment="If set to 1, the datalake_import column will be created on all tables that is using this dbalias",
        server_default=text("'1'"),
    )
    timewindow_start = Column(
        Time,
        comment="Start of the time window when we are allowed to run against this connection.",
    )
    timewindow_stop = Column(
        Time,
        comment="End of the time window when we are allowed to run against this connection.",
    )
    timewindow_timezone = Column(
        String(64),
        comment="Timezone used for timewindow_start and timewindow_stop columns. Use full text timezone, example Europe/Stockholm.",
    )
    operator_notes = Column(
        Text, comment="Free text field to write a note about the connection"
    )
    create_foreign_keys = Column(
        TINYINT(4),
        nullable=False,
        comment="1 = Create foreign keys, 0 = Dont create foreign keys",
        server_default=text("'1'"),
    )
    contact_info = Column(
        String(255), comment="Contact information. Used by Atlas integration"
    )
    description = Column(String(255), comment="Description. Used by Atlas integration")
    owner = Column(
        String(255), comment="Owner of system and/or data. Used by Atlas integration"
    )
    atlas_discovery = Column(
        TINYINT(4),
        nullable=False,
        comment="1 = Discover tables/views on this connection, 0 = Dont use Atlas discovery on this connection",
        server_default=text("'1'"),
    )
    atlas_include_filter = Column(
        String(256), comment="Include filter for Atlas discovery"
    )
    atlas_exclude_filter = Column(
        String(256), comment="Exclude filter for Atlas discovery"
    )
    atlas_last_discovery = Column(DateTime, comment="Time of last Atlas discovery")
    environment = Column(
        String(32),
        ForeignKey("jdbc_connections_environments.environment"),
        nullable=True,
        comment="Name of the Environment type",
    )
    seed_file = Column(
        String(256),
        comment="File that will be used to fetch the custom seed that will be used for anonymization functions on data from the connection",
    )

    jdbc_connections_environments = relationship("jdbcConnectionsEnvironments")


class jdbcConnectionsDrivers(Base):
    __tablename__ = "jdbc_connections_drivers"

    database_type = Column(
        Enum(
            "DB2 AS400",
            "DB2 UDB",
            "MySQL",
            "Oracle",
            "PostgreSQL",
            "Progress DB",
            "SQL Server",
            "MongoDB",
            "CacheDB",
            "Snowflake",
            "Informix",
            "SQL Anywhere",
        ),
        primary_key=True,
        nullable=False,
        comment="Name of database type.  Name is hardcoded into Python scripts, so use only defined values",
    )
    version = Column(
        String(16),
        primary_key=True,
        nullable=False,
        comment="Free-text field with version. Has nothing to do with version of driver itself.",
    )
    driver = Column(String(128), nullable=False, comment="Java class for JDBC driver")
    classpath = Column(
        String(255),
        nullable=False,
        comment="Full path to JDBC driver/jar file. If more than one file is required, separate them with : and no spaces",
    )


class jdbcTableChangeHistory(Base):
    __tablename__ = "jdbc_table_change_history"
    __table_args__ = (
        Index(
            "dbalias_database_schema_table",
            "dbalias",
            "db_name",
            "schema_name",
            "table_name",
        ),
        {
            "comment": "This table keeps track of all changes that was done to a JDBC table after the initial creation. Example could be that a colum type was changed from char(10) to varchar(10). That kind of information is logged in this table"
        },
    )

    dbalias = Column(
        String(256),
        primary_key=True,
        nullable=False,
        comment="Database connection name",
    )
    db_name = Column(String(256), nullable=False, comment="Database name")
    schema_name = Column(
        String(256), primary_key=True, nullable=False, comment="Schema Name"
    )
    table_name = Column(
        String(256), primary_key=True, nullable=False, comment="Table Name"
    )
    column_name = Column(String(256), nullable=False, comment="Column Name")
    eventtime = Column(
        DateTime, primary_key=True, nullable=False, comment="Time and date of the event"
    )
    event = Column(String(128), nullable=False, comment="Event name.")
    previous_value = Column(String(256), comment="Value before the change")
    value = Column(String(256), comment="Value after the change")
    description = Column(
        String(512), nullable=False, comment="Detailed description of the event"
    )


class jsonToSend(Base):
    __tablename__ = "json_to_send"
    __table_args__ = {
        "comment": "Temporary storage of JSON payloads that will be sent to a REST interface if the tool is configured to do so."
    }

    id = Column(
        BIGINT(20), primary_key=True, autoincrement=True, comment="Unique Identifier"
    )
    type = Column(
        String(50),
        nullable=False,
        comment="The type of JSON data that is saved in the 'jsondata' column",
    )
    create_time = Column(
        DateTime,
        nullable=False,
        comment="Time when the JSON data was created",
        server_default=text("CURRENT_TIMESTAMP"),
    )
    status = Column(
        TINYINT(4),
        nullable=False,
        comment="Internal status to keep track of what the status of the transmissions is",
    )
    destination = Column(
        String(8), nullable=False, comment="The destination where to send the json."
    )
    jsondata = Column(Text, nullable=False, comment="The payload to send")


class tableChangeHistory(Base):
    __tablename__ = "table_change_history"
    __table_args__ = (
        Index("hive_db_hive_table", "hive_db", "hive_table"),
        {
            "comment": "This table keeps track of all changes that was done to an abject after the initial load. Example could be that a colum type was changed from char(10) to varchar(10). That kind of information is logged in this table"
        },
    )

    hive_db = Column(
        String(256), primary_key=True, nullable=False, comment="Hive Database"
    )
    hive_table = Column(
        String(256), primary_key=True, nullable=False, comment="Hive Table"
    )
    column_name = Column(String(256), nullable=False, comment="Column Name")
    eventtime = Column(
        DateTime, primary_key=True, nullable=False, comment="Time and date of the event"
    )
    event = Column(String(128), nullable=False, comment="Event name.")
    previous_value = Column(String(256), comment="Value before the change")
    value = Column(String(256), comment="Value after the change")
    description = Column(
        String(512), nullable=False, comment="Detailed description of the event"
    )


class airflowTasks(Base):
    __tablename__ = "airflow_tasks"
    __table_args__ = {
        "comment": "All DAGs can be customized by adding Tasks into the DAG. Depending on what placement and type of Tasks that is created, DBImport will add custom placeholders to keep the DAG separated in three different parts. Before, In and After Main. In main is where all regular Imports, export or ETL jobs are executed. If you want to execute something before these, you place it in Before Main. And if you want to execute something after, you place it in After main. Please check the Airflow Integration part of the documentation for more examples and better understanding of the data you can put into this table"
    }

    dag_name = Column(
        String(64),
        primary_key=True,
        nullable=False,
        comment="Name of DAG to add Tasks to",
    )
    task_name = Column(
        String(64),
        primary_key=True,
        nullable=False,
        comment="Name of the Task in Airflow",
    )
    task_type = Column(
        Enum(
            "shell script",
            "Hive SQL",
            "Hive SQL Script",
            "JDBC SQL",
            "Trigger DAG",
            "DAG Sensor",
            "SQL Sensor",
            "DBImport command",
        ),
        nullable=False,
        index=True,
        server_default=text("'Hive SQL Script'"),
        comment="The type of the Task",
    )
    placement = Column(
        Enum("before main", "after main", "in main"),
        nullable=False,
        server_default=text("'after main'"),
        comment="Placement for the Task",
    )
    jdbc_dbalias = Column(
        ForeignKey("jdbc_connections.dbalias"),
        index=True,
        comment="For  'JDBC SQL' Task Type, this specifies what database the SQL should run against",
    )
    hive_db = Column(String(256), comment="<NOT USED>")
    airflow_pool = Column(String(64), comment="Airflow Pool to use.")
    airflow_priority = Column(
        TINYINT(4), comment="Airflow Priority. Higher number, higher priority"
    )
    include_in_airflow = Column(
        TINYINT(4),
        nullable=False,
        server_default=text("'1'"),
        comment="Enable or disable the Task in the DAG during creation of DAG file.",
    )
    task_dependency_downstream = Column(
        String(256),
        comment="Defines the downstream dependency for the Task. Comma separated list",
    )
    task_dependency_upstream = Column(
        String(256),
        comment="Defines the upstream dependency for the Task. Comma separated list",
    )
    task_config = Column(
        String(512),
        comment="The configuration for the Task. Depends on what Task type it is.",
    )
    sensor_poke_interval = Column(
        Integer, nullable=True, comment="Poke interval for sensors in seconds"
    )
    sensor_timeout_minutes = Column(
        Integer, nullable=True, comment="Timeout for sensors in minutes"
    )
    sensor_connection = Column(
        String(64), nullable=True, comment="Name of Connection in Airflow"
    )
    sensor_soft_fail = Column(
        Integer,
        nullable=True,
        comment="Setting this to 1 will add soft_fail=True on sensor",
    )
    sudo_user = Column(
        String(64), comment="The task will use this user for sudo instead of default"
    )

    jdbc_connection = relationship("jdbcConnections")


class exportColumns(Base):
    __tablename__ = "export_columns"
    __table_args__ = {
        "comment": "This table contains all columns that exists on all tables that we are exporting. Unlike the export_tables table, this one gets created automatically by the export tool"
    }

    table_id = Column(
        ForeignKey("export_tables.table_id", ondelete="CASCADE"),
        primary_key=True,
        nullable=False,
        comment="Foreign Key to export_tables column 'table_id'",
    )
    column_id = Column(
        Integer,
        primary_key=True,
        nullable=False,
        unique=True,
        autoincrement=True,
        comment="Unique identifier",
    )
    column_name = Column(
        String(256),
        primary_key=True,
        nullable=False,
        comment="Name of column in target table. Dont change this manually",
    )
    column_type = Column(
        String(2048), comment="Column type from Hive. Dont change this manually"
    )
    column_order = Column(
        Integer, comment="The order of the columns. Dont change this manually"
    )
    hive_db = Column(
        String(256),
        comment="Only used to make it easier to read the table. No real usage",
    )
    hive_table = Column(
        String(256),
        comment="Only used to make it easier to read the table. No real usage",
    )
    target_column_name = Column(
        String(256), comment="Override the name of column in the target system"
    )
    target_column_type = Column(
        String(256), comment="Override the column type in the target system"
    )
    last_update_from_hive = Column(
        DateTime,
        nullable=False,
        comment="Timestamp of last schema update from Hive. Dont change this manually",
    )
    last_export_time = Column(
        DateTime, comment="Timestamp of last export. Dont change this manually"
    )
    selection = Column(String(256), comment="<NOT USED>")
    include_in_export = Column(
        TINYINT(4),
        nullable=False,
        comment="1 = Include column in export, 0 = Exclude column in export",
        server_default=text("'1'"),
    )
    comment = Column(
        Text,
        comment="The column comment from the source system. Dont change this manually",
    )
    operator_notes = Column(
        Text, comment="Free text field to write a note about the import. "
    )

    table = relationship("exportTables")


class importColumns(Base):
    __tablename__ = "import_columns"
    __table_args__ = (
        Index("table_id_source_column_name", "table_id", "source_column_name"),
        {
            "comment": "This table contains all columns that exists on all tables that we are importing. Unlike the import_tables table, this one gets created automatically by the 'Get Source TableSchema' stage. "
        },
    )

    table_id = Column(
        ForeignKey("import_tables.table_id", ondelete="CASCADE"),
        primary_key=True,
        nullable=False,
        comment="Foreign Key to import_tables column 'table_id'",
    )
    column_id = Column(
        Integer,
        primary_key=True,
        nullable=False,
        unique=True,
        autoincrement=True,
        comment="Unique identifier of the column",
    )
    column_order = Column(
        Integer, comment="In what order does the column exist in the source system. "
    )
    column_name = Column(
        String(256),
        primary_key=True,
        nullable=False,
        comment="Name of column in Hive. Dont change this manually",
    )
    hive_db = Column(String(256), comment="Hive Database")
    hive_table = Column(String(256), comment="Hive Table")
    source_column_name = Column(
        String(256),
        nullable=False,
        comment="Name of column in source system. Dont change this manually",
    )
    column_type = Column(
        String(2048),
        nullable=False,
        comment="Column type in Hive. Dont change this manually",
    )
    source_column_type = Column(
        String(2048),
        nullable=False,
        comment="Column type in source system. Dont change this manually",
    )
    source_database_type = Column(
        String(256), comment="That database type was the column imported from"
    )
    column_name_override = Column(
        String(256), comment="Set a custom name of the column in Hive"
    )
    column_type_override = Column(
        String(256), comment="Set a custom column type in Hive"
    )
    sqoop_column_type = Column(
        String(256),
        comment="Used to create a correct --map-column-java setting for sqoop ",
    )
    sqoop_column_type_override = Column(
        String(256),
        comment="Set the --map-column-java field to a fixed value and not calculated by DBImport ",
    )
    force_string = Column(
        TINYINT(4),
        nullable=False,
        comment="If set to 1, all character based fields (char, varchar) will become string in Hive. Overrides the same setting in import_tables and jdbc_connections table",
        server_default=text("'-1'"),
    )
    include_in_import = Column(
        TINYINT(4),
        nullable=False,
        comment="1 = Include column in import, 0 = Exclude column in import",
        server_default=text("'1'"),
    )
    source_primary_key = Column(
        TINYINT(4),
        comment="Number starting from 1 listing the order of the column in the PK. Dont change this manually",
    )
    last_update_from_source = Column(
        DateTime, nullable=False, comment="Timestamp of last schema update from source"
    )
    comment = Column(Text, comment="The column comment from the source system")
    operator_notes = Column(
        Text, comment="Free text field to write a note about the column"
    )
    anonymization_function = Column(
        Enum("None", "Hash", "Replace with star", "Show first 4 chars"),
        nullable=False,
        comment="What anonymization function should be used with the data in this column",
        server_default=text("'None'"),
    )

    table = relationship("importTables")


class importForeignKeys(Base):
    __tablename__ = "import_foreign_keys"
    __table_args__ = (
        ForeignKeyConstraint(
            ["fk_column_id"],
            ["import_columns.column_id"],
            name="FK_import_foreign_keys_import_columns_fk",
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["column_id"],
            ["import_columns.column_id"],
            name="FK_import_foreign_keys_import_columns",
            ondelete="CASCADE",
        ),
        Index("FK_table_id_column_id", "fk_table_id", "fk_column_id"),
        {
            "comment": "All foreign key definitions is saved in this table. The information in this table is recreated all the time, so no manually changes are allowed here. For a better understanding of this table, please use the view called import_foreign_keys_view instead"
        },
    )

    table_id = Column(
        Integer,
        primary_key=True,
        nullable=False,
        comment="Table ID in import_tables that have the FK",
    )
    column_id = Column(
        Integer,
        primary_key=True,
        nullable=False,
        comment="Column ID in import_columns that have the FK",
    )
    fk_index = Column(Integer, primary_key=True, nullable=False, comment="Index of FK")
    fk_table_id = Column(
        Integer,
        nullable=False,
        comment="Table ID in import_tables that the table is having a reference against",
    )
    fk_column_id = Column(
        Integer,
        nullable=False,
        comment="Column ID in import_columns that the table is having a reference against",
    )
    key_position = Column(Integer, nullable=False, comment="Position of the key")

    fk_column = relationship(
        "importColumns",
        primaryjoin="importForeignKeys.fk_column_id == importColumns.column_id",
    )
    column = relationship(
        "importColumns",
        primaryjoin="importForeignKeys.column_id == importColumns.column_id",
    )


class atlasColumnCache(Base):
    __tablename__ = "atlas_column_cache"
    __table_args__ = {
        "comment": "Atlas discovery uses this table to cache values in order to detect changes instead of putting a heavy load on the Atlas server."
    }

    hostname = Column(
        String(256),
        primary_key=True,
        nullable=False,
        comment="Hostname for the database",
    )
    port = Column(
        String(8), primary_key=True, nullable=False, comment="Port for the database"
    )
    database_name = Column(
        String(256), primary_key=True, nullable=False, comment="Database name"
    )
    schema_name = Column(
        String(256), primary_key=True, nullable=False, comment="Database schema"
    )
    table_name = Column(
        String(256), primary_key=True, nullable=False, comment="Database table"
    )
    column_name = Column(
        String(256), primary_key=True, nullable=True, comment="Name of the column"
    )
    column_type = Column(String(2048), nullable=True, comment="Type of the column")
    column_length = Column(String(64), nullable=True, comment="Length of the column")
    column_is_nullable = Column(
        String(16), nullable=True, comment="Is null values allowed in the column"
    )
    column_comment = Column(Text, nullable=True, comment="Comment on the column")
    table_comment = Column(Text, nullable=True, comment="Comment on the table")
    table_type = Column(String(256), nullable=True, comment="Table type. ")
    table_create_time = Column(
        DateTime, nullable=True, comment="Timestamp for when the table was created"
    )
    default_value = Column(Text, nullable=True, comment="Default value of the column")


class atlasKeyCache(Base):
    __tablename__ = "atlas_key_cache"
    __table_args__ = {
        "comment": "Atlas discovery uses this table to cache values in order to detect changes instead of putting a heavy load on the Atlas server."
    }

    hostname = Column(
        String(256),
        primary_key=True,
        nullable=False,
        comment="Hostname for the database",
    )
    port = Column(
        String(8), primary_key=True, nullable=False, comment="Port for the database"
    )
    database_name = Column(
        String(256), primary_key=True, nullable=False, comment="Database name"
    )
    schema_name = Column(
        String(256), primary_key=True, nullable=False, comment="Database schema"
    )
    table_name = Column(
        String(256), primary_key=True, nullable=False, comment="Database table"
    )
    constraint_name = Column(
        String(256), primary_key=True, nullable=True, comment="Name of the constraint"
    )
    constraint_type = Column(String(8), nullable=True, comment="Type of the constraint")
    column_name = Column(
        String(256), primary_key=True, nullable=True, comment="Name of the column"
    )
    reference_schema_name = Column(
        String(256), nullable=True, comment="Name of the schema that is referenced"
    )
    reference_table_name = Column(
        String(256), nullable=True, comment="Name of the table that is referenced"
    )
    reference_column_name = Column(
        String(256), nullable=True, comment="Name of the column that is referenced"
    )
    col_key_position = Column(Integer, nullable=True, comment="Position of the key")


class authUsers(Base):
    __tablename__ = "auth_users"
    __table_args__ = {"comment": "User database for rest interface and ui"}

    username = Column(String(256), primary_key=True, nullable=False, comment="Username")
    password = Column(
        String(256),
        nullable=False,
        comment="Password. Only used if authentication_method is local",
    )
    disabled = Column(
        TINYINT(4),
        nullable=False,
        server_default=text("'0'"),
        comment="1 = User is disabled and cant login.",
    )
    fullname = Column(String(256), nullable=True, comment="Full name of the user")
    department = Column(String(256), nullable=True, comment="Users department")
    email = Column(String(256), nullable=True, comment="Users email address")
