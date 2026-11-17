from datetime import datetime, time

from sqlalchemy import (
    Column,
    DateTime,
    Enum,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    PrimaryKeyConstraint,
    SmallInteger,
    String,
    Table,
    Text,
    Time,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.mysql import BIGINT, TINYINT
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


metadata = Base.metadata


class AlembicVersion(Base):
    __tablename__ = "alembic_version"

    version_num: Mapped[str] = mapped_column(
        String(32), primary_key=True, nullable=False
    )


class DBImportInstances(Base):
    __tablename__ = "dbimport_instances"
    __table_args__ = {
        "comment": "This table contains all DBInstance that will receive data from this instance during the copy phase"
    }

    instance_id: Mapped[int] = mapped_column(
        BIGINT(20),
        primary_key=True,
        autoincrement=True,
        comment="Auto Incremented PrimaryKey of the table",
    )
    name: Mapped[str] = mapped_column(
        String(32), nullable=False, unique=True, comment="Name of the DBImport instance"
    )
    db_hostname: Mapped[str] = mapped_column(
        String(64), nullable=False, comment="MySQL Hostname to DBImport database"
    )
    db_port: Mapped[int] = mapped_column(
        SmallInteger, nullable=False, comment="MySQL Port to DBImport database"
    )
    db_database: Mapped[str] = mapped_column(
        String(64), nullable=False, comment="MySQL Database to DBImport database"
    )
    db_credentials: Mapped[str | None] = mapped_column(
        Text, nullable=True, comment="MySQL Username and Password to DBImport database"
    )
    hdfs_address: Mapped[str] = mapped_column(
        String(64), nullable=False, comment="HDFS address. Example hdfs://hadoopcluster"
    )
    hdfs_basedir: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
        comment="The base dir to write data to. Example /apps/dbimport",
    )
    sync_credentials: Mapped[int] = mapped_column(
        TINYINT(4),
        nullable=False,
        server_default=text("'0'"),
        comment="0 = Credentials won't be synced, 1 = The credentials information will be synced to the other cluster",
    )


class CopyTables(Base):
    __tablename__ = "copy_tables"
    __table_args__ = {
        "comment": "When the copy phase starts, it will look in this table to understand if its going to copy its data and to what DBImport instances."
    }

    copy_id: Mapped[int] = mapped_column(
        BIGINT(20),
        primary_key=True,
        autoincrement=True,
        comment="Auto Incremented PrimaryKey of the table",
    )
    hive_filter: Mapped[str] = mapped_column(
        String(256),
        nullable=False,
        comment="Filter string for database and table. ; separated. Wildcards (*) allowed. Example HIVE_DB.HIVE_TABLE; HIVE_DB.HIVE_TABLE",
    )
    destination: Mapped[str] = mapped_column(
        String(32),
        ForeignKey("dbimport_instances.name"),
        nullable=False,
        comment="DBImport instances to copy the imported data to",
    )
    data_transfer: Mapped[str] = mapped_column(
        Enum("Synchronous", "Asynchronous"),
        nullable=False,
        comment="Synchronous will transfer the data as part of the Import. Asynchronous will transfer the data by a separate process and not part of the Import",
        server_default=text("'Synchronous'"),
    )

    dbimport_instances: Mapped["DBImportInstances"] = relationship("DBImportInstances")


class CopyAsyncStatus(Base):
    __tablename__ = "copy_async_status"
    __table_args__ = {
        "comment": "The status table for asynchronous copy between DBImport instances."
    }

    table_id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        nullable=False,
        comment="Reference to import_table.table_id",
    )
    hive_db: Mapped[str] = mapped_column(
        String(256), nullable=False, comment="Hive Database"
    )
    hive_table: Mapped[str] = mapped_column(
        String(256), nullable=False, comment="Hive Table to copy"
    )
    destination: Mapped[str] = mapped_column(
        String(32),
        primary_key=True,
        nullable=False,
        comment="DBImport instances to copy the imported data to",
    )
    copy_status: Mapped[int] = mapped_column(
        TINYINT(4),
        nullable=False,
        server_default=text("'0'"),
        comment="Status of the copy operation",
    )
    last_status_update: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP"),
        comment="Last time the server changed progress on this copy",
    )
    failures: Mapped[int] = mapped_column(
        Integer,
        server_default=text("'0'"),
        nullable=False,
        comment="Number of failures on current state",
    )
    hdfs_source_path: Mapped[str] = mapped_column(
        String(768), nullable=False, comment="HDFS path to copy from"
    )
    hdfs_target_path: Mapped[str] = mapped_column(
        String(768), nullable=False, comment="HDFS path to copy to"
    )


class AirflowCustomDAGs(Base):
    __tablename__ = "airflow_custom_dags"
    __table_args__ = {
        "comment": 'Its possible to construct a DAG that have no import, export or ETL definitions in it, but instead just Tasks from the airflow_task table. That might be useful to for example run custom Hive Code after an import is completed as a separate DAG. Defining a DAG in here also requires you to have at least one task in airflow_tasks defined "in main"'
    }

    dag_name: Mapped[str] = mapped_column(
        String(64), primary_key=True, comment="Name of Airflow DAG."
    )
    schedule_interval: Mapped[str] = mapped_column(
        String(128),
        nullable=False,
        comment="Time to execute dag",
    )
    retries: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        server_default=text("'0'"),
        comment="How many retries should be Task do in Airflow before it fails",
    )
    operator_notes: Mapped[str | None] = mapped_column(
        Text, comment="Free text field to write a note about the custom DAG. "
    )
    application_notes: Mapped[str | None] = mapped_column(
        Text,
        comment="Free text field that can be used for application documentation, notes or links. ",
    )
    auto_regenerate_dag: Mapped[int] = mapped_column(
        TINYINT(4),
        nullable=False,
        server_default=text("'1'"),
        comment="1 = The DAG will be auto regenerated by manage command",
    )
    airflow_notes: Mapped[str | None] = mapped_column(
        Text,
        comment="Documentation that will be available in Airflow. Markdown syntax",
    )
    sudo_user: Mapped[str | None] = mapped_column(
        String(64),
        comment="All tasks in DAG will use this user for sudo instead of default",
    )
    timezone: Mapped[str | None] = mapped_column(
        String(64),
        comment="Timezone used for schedule_interval column. Use full text timezone, example Europe/Stockholm.",
    )
    email: Mapped[str | None] = mapped_column(
        String(256),
        comment="Email to send message to in case email_on_retry or email_on_failure is set to True. Comma separated list of email addresses",
    )
    email_on_failure: Mapped[int] = mapped_column(
        TINYINT(4),
        nullable=False,
        server_default=text("'0'"),
        comment="Send email on failures",
    )
    email_on_retries: Mapped[int] = mapped_column(
        TINYINT(4),
        nullable=False,
        server_default=text("'0'"),
        comment="Send email on retries",
    )
    tags: Mapped[str | None] = mapped_column(
        String(256),
        nullable=True,
        comment="Comma separated list of Airflow tags that will be set on the Dag",
    )
    sla_warning_time: Mapped[time | None] = mapped_column(
        Time,
        nullable=True,
        comment="Maximum time this DAG should run before Airflow triggers a SLA miss",
    )
    retry_exponential_backoff: Mapped[int] = mapped_column(
        TINYINT(4),
        nullable=False,
        server_default=text("'0'"),
        comment="1 = Use the retry_exponential_backoff Airflow function that will cause the retry between failed tasks to be longer and longer each time instead of a fixed time, 0 = Run with a fixed time of 5 min between the task retries",
    )
    concurrency: Mapped[int | None] = mapped_column(
        TINYINT(4),
        nullable=True,
        comment="Set the max number of concurrent tasks in the DAG while executing. Overrides the default value specified in Airflow configuration",
    )


class AirflowDAGSensors(Base):
    __tablename__ = "airflow_dag_sensors"
    __table_args__ = {"comment": "This table is used only in Legacy DBImport"}

    dag_name: Mapped[str] = mapped_column(
        String(64),
        primary_key=True,
        nullable=False,
        comment="Name of DAG to add sensor to",
    )
    sensor_name: Mapped[str] = mapped_column(
        String(64), primary_key=True, nullable=False, comment="name of the sensor Task"
    )
    wait_for_dag: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
        comment="Name of DAG to wait for. Must be defined in DBImport",
    )
    wait_for_task: Mapped[str | None] = mapped_column(
        String(64), comment="Name of task to wait for.Default is 'stop'"
    )
    timeout_minutes: Mapped[int | None] = mapped_column(
        Integer, comment="Number of minutes to wait for DAG."
    )
    sensor_soft_fail: Mapped[int | None] = mapped_column(
        Integer,
        nullable=True,
        comment="Setting this to 1 will add soft_fail=True on sensor",
    )


class AirflowETLDAGs(Base):
    __tablename__ = "airflow_etl_dags"
    __table_args__ = {
        "comment": "To create a DAG in Airflow for only ETL jobs, this is the table that holds all definitions of the DAG configuration, including the filter that defines what ETL jobs to run, schedules times, pool names and much more. "
    }

    dag_name: Mapped[str] = mapped_column(
        String(64), primary_key=True, comment="Name of Airflow DAG."
    )
    schedule_interval: Mapped[str] = mapped_column(
        String(128),
        nullable=False,
        comment="Time to execute dag",
    )
    filter_job: Mapped[str] = mapped_column(
        String(64), nullable=False, comment="Filter string for JOB in etl_jobs table"
    )
    filter_task: Mapped[str | None] = mapped_column(
        String(64), comment="Filter string for TASK in etl_jobs table"
    )
    filter_source_db: Mapped[str | None] = mapped_column(
        String(256), comment="Filter string for SOURCE_DB in etl_jobs table"
    )
    filter_target_db: Mapped[str | None] = mapped_column(
        String(256), comment="Filter string for TARGET_DB in etl_jobs table"
    )
    retries: Mapped[int | None] = mapped_column(
        TINYINT(4),
        comment="How many retries should be Task do in Airflow before it fails",
    )
    trigger_dag_on_success: Mapped[str | None] = mapped_column(
        String(64), comment="<NOT USED>"
    )
    operator_notes: Mapped[str | None] = mapped_column(
        Text, comment="Free text field to write a note about the ETL job. "
    )
    application_notes: Mapped[str | None] = mapped_column(
        Text,
        comment="Free text field that can be used for application documentation, notes or links. ",
    )
    auto_regenerate_dag: Mapped[int] = mapped_column(
        TINYINT(4),
        nullable=False,
        server_default=text("'1'"),
        comment="1 = The DAG will be auto regenerated by manage command",
    )
    airflow_notes: Mapped[str | None] = mapped_column(
        Text,
        comment="Documentation that will be available in Airflow. Markdown syntax",
    )
    sudo_user: Mapped[str | None] = mapped_column(
        String(64),
        comment="All tasks in DAG will use this user for sudo instead of default",
    )
    timezone: Mapped[str | None] = mapped_column(
        String(64),
        comment="Timezone used for schedule_interval column. Use full text timezone, example Europe/Stockholm.",
    )
    email: Mapped[str | None] = mapped_column(
        String(256),
        comment="Email to send message to in case email_on_retry or email_on_failure is set to True. Comma separated list of email addresses",
    )
    email_on_failure: Mapped[int] = mapped_column(
        TINYINT(4),
        nullable=False,
        server_default=text("'0'"),
        comment="Send email on failures",
    )
    email_on_retries: Mapped[int] = mapped_column(
        TINYINT(4),
        nullable=False,
        server_default=text("'0'"),
        comment="Send email on retries",
    )
    tags: Mapped[str | None] = mapped_column(
        String(256),
        nullable=True,
        comment="Comma separated list of Airflow tags that will be set on the Dag",
    )
    sla_warning_time: Mapped[time | None] = mapped_column(
        Time,
        nullable=True,
        comment="Maximum time this DAG should run before Airflow triggers a SLA miss",
    )
    retry_exponential_backoff: Mapped[int] = mapped_column(
        TINYINT(4),
        nullable=False,
        server_default=text("'0'"),
        comment="1 = Use the retry_exponential_backoff Airflow function that will cause the retry between failed tasks to be longer and longer each time instead of a fixed time, 0 = Run with a fixed time of 5 min between the task retries",
    )
    concurrency: Mapped[int | None] = mapped_column(
        TINYINT(4),
        nullable=True,
        comment="Set the max number of concurrent tasks in the DAG while executing. Overrides the default value specified in Airflow configuration",
    )


class AirflowExportDAGs(Base):
    __tablename__ = "airflow_export_dags"
    __table_args__ = {
        "comment": "To create a DAG in Airflow for Exports, this is the table that holds all definitions of the DAG configuration, including the filter that defines what tables to export, schedules times, pool names and much more. "
    }

    dag_name: Mapped[str] = mapped_column(
        String(64), primary_key=True, comment="Name of Airflow DAG."
    )
    schedule_interval: Mapped[str] = mapped_column(
        String(128),
        nullable=False,
        comment="Time to execute dag",
    )
    filter_dbalias: Mapped[str] = mapped_column(
        String(256),
        nullable=False,
        comment="Filter string for DBALIAS in export_tables",
    )
    filter_target_schema: Mapped[str | None] = mapped_column(
        String(256), comment="Filter string for TARGET_SCHEMA  in export_tables"
    )
    filter_target_table: Mapped[str | None] = mapped_column(
        String(256), comment="Filter string for TARGET_TABLE  in export_tables"
    )
    use_python_dbimport: Mapped[int] = mapped_column(
        TINYINT(4),
        nullable=False,
        server_default=text("'1'"),
        comment="Legacy use only. Always put this to 1",
    )
    retries: Mapped[int | None] = mapped_column(
        TINYINT(4),
        comment="How many retries should be Task do in Airflow before it fails",
    )
    trigger_dag_on_success: Mapped[str | None] = mapped_column(
        String(64),
        comment="Name of DAG to trigger if export is successful. Comma separated list of DAGs (optional)",
    )
    operator_notes: Mapped[str | None] = mapped_column(
        Text, comment="Free text field to write a note about the export. "
    )
    application_notes: Mapped[str | None] = mapped_column(
        Text,
        comment="Free text field that can be used for application documentation, notes or links. ",
    )
    auto_regenerate_dag: Mapped[int] = mapped_column(
        TINYINT(4),
        nullable=False,
        server_default=text("'1'"),
        comment="1 = The DAG will be auto regenerated by manage command",
    )
    airflow_notes: Mapped[str | None] = mapped_column(
        Text,
        comment="Documentation that will be available in Airflow. Markdown syntax",
    )
    sudo_user: Mapped[str | None] = mapped_column(
        String(64),
        comment="All tasks in DAG will use this user for sudo instead of default",
    )
    timezone: Mapped[str | None] = mapped_column(
        String(64),
        comment="Timezone used for schedule_interval column. Use full text timezone, example Europe/Stockholm.",
    )
    email: Mapped[str | None] = mapped_column(
        String(256),
        comment="Email to send message to in case email_on_retry or email_on_failure is set to True. Comma separated list of email addresses",
    )
    email_on_failure: Mapped[int] = mapped_column(
        TINYINT(4),
        nullable=False,
        server_default=text("'0'"),
        comment="Send email on failures",
    )
    email_on_retries: Mapped[int] = mapped_column(
        TINYINT(4),
        nullable=False,
        server_default=text("'0'"),
        comment="Send email on retries",
    )
    tags: Mapped[str | None] = mapped_column(
        String(256),
        nullable=True,
        comment="Comma separated list of Airflow tags that will be set on the Dag",
    )
    sla_warning_time: Mapped[time | None] = mapped_column(
        Time,
        nullable=True,
        comment="Maximum time this DAG should run before Airflow triggers a SLA miss",
    )
    retry_exponential_backoff: Mapped[int] = mapped_column(
        TINYINT(4),
        nullable=False,
        server_default=text("'0'"),
        comment="1 = Use the retry_exponential_backoff Airflow function that will cause the retry between failed tasks to be longer and longer each time instead of a fixed time, 0 = Run with a fixed time of 5 min between the task retries",
    )
    concurrency: Mapped[int | None] = mapped_column(
        TINYINT(4),
        nullable=True,
        comment="Set the max number of concurrent tasks in the DAG while executing. Overrides the default value specified in Airflow configuration",
    )


class AirflowImportDAGs(Base):
    __tablename__ = "airflow_import_dags"
    __table_args__ = {
        "comment": "To create a DAG in Airflow for Imports, this is the table that holds all definitions of the DAG configuration, including the filter that defines what tables to import, schedules times, pool names and much more."
    }

    dag_name: Mapped[str] = mapped_column(
        String(64), primary_key=True, comment="Name of Airflow DAG."
    )
    schedule_interval: Mapped[str] = mapped_column(
        String(128),
        nullable=False,
        comment="Time to execute dag",
    )
    filter_hive: Mapped[str] = mapped_column(
        String(16384),
        nullable=False,
        comment="Filter string for database and table. ; separated. Wildcards (*) allowed. Example HIVE_DB.HIVE_TABLE; HIVE_DB.HIVE_TABLE",
    )
    use_python_dbimport: Mapped[int] = mapped_column(
        TINYINT(4),
        nullable=False,
        server_default=text("'1'"),
        comment="Legacy use only. Always put this to 1",
    )
    finish_all_stage1_first: Mapped[int] = mapped_column(
        TINYINT(4),
        nullable=False,
        server_default=text("'0'"),
        comment="1 = All Import phase jobs will be completed first, and when all is successful, the ETL phase start",
    )
    retries: Mapped[int] = mapped_column(
        TINYINT(4),
        nullable=False,
        server_default=text("'5'"),
        comment="How many retries should be Task do in Airflow before it fails",
    )
    retries_stage1: Mapped[int | None] = mapped_column(
        TINYINT(4), comment="Specific retries number for Import Phase"
    )
    retries_stage2: Mapped[int | None] = mapped_column(
        TINYINT(4), comment="Specific retries number for ETL Phase"
    )
    pool_stage1: Mapped[str | None] = mapped_column(
        String(256),
        comment="Airflow pool used for stage1 tasks. NULL for the default Hostname pool",
    )
    pool_stage2: Mapped[str | None] = mapped_column(
        String(256),
        comment="Airflow pool used for stage2 tasks. NULL for the default DAG pool",
    )
    operator_notes: Mapped[str | None] = mapped_column(
        Text, comment="Free text field to write a note about the import. "
    )
    application_notes: Mapped[str | None] = mapped_column(
        Text,
        comment="Free text field that can be used for application documentation, notes or links. ",
    )
    auto_table_discovery: Mapped[int] = mapped_column(
        TINYINT(4), nullable=False, server_default=text("'1'"), comment="<NOT USED>"
    )
    auto_regenerate_dag: Mapped[int] = mapped_column(
        TINYINT(4),
        nullable=False,
        server_default=text("'1'"),
        comment="1 = The DAG will be auto regenerated by manage command",
    )
    run_import_and_etl_separate: Mapped[int] = mapped_column(
        TINYINT(4),
        nullable=False,
        server_default=text("'0'"),
        comment="1 = The Import and ETL phase will run in separate Tasks. ",
    )
    airflow_notes: Mapped[str | None] = mapped_column(
        Text,
        comment="Documentation that will be available in Airflow. Markdown syntax",
    )
    sudo_user: Mapped[str | None] = mapped_column(
        String(64),
        comment="All tasks in DAG will use this user for sudo instead of default",
    )
    metadata_import: Mapped[int] = mapped_column(
        TINYINT(4),
        nullable=False,
        server_default=text("'0'"),
        comment="1 = Run only getSchema and getRowCount, 0 = Run a normal import",
    )
    timezone: Mapped[str | None] = mapped_column(
        String(64),
        comment="Timezone used for schedule_interval column. Use full text timezone, example Europe/Stockholm.",
    )
    email: Mapped[str | None] = mapped_column(
        String(256),
        comment="Email to send message to in case email_on_retry or email_on_failure is set to True. Comma separated list of email addresses",
    )
    email_on_failure: Mapped[int] = mapped_column(
        TINYINT(4),
        nullable=False,
        server_default=text("'0'"),
        comment="Send email on failures",
    )
    email_on_retries: Mapped[int] = mapped_column(
        TINYINT(4),
        nullable=False,
        server_default=text("'0'"),
        comment="Send email on retries",
    )
    tags: Mapped[str | None] = mapped_column(
        String(256),
        nullable=True,
        comment="Comma separated list of Airflow tags that will be set on the Dag",
    )
    sla_warning_time: Mapped[time | None] = mapped_column(
        Time,
        nullable=True,
        comment="Maximum time this DAG should run before Airflow triggers a SLA miss",
    )
    retry_exponential_backoff: Mapped[int] = mapped_column(
        TINYINT(4),
        nullable=False,
        server_default=text("'0'"),
        comment="1 = Use the retry_exponential_backoff Airflow function that will cause the retry between failed tasks to be longer and longer each time instead of a fixed time, 0 = Run with a fixed time of 5 min between the task retries",
    )
    concurrency: Mapped[int | None] = mapped_column(
        TINYINT(4),
        nullable=True,
        comment="Set the max number of concurrent tasks in the DAG while executing. Overrides the default value specified in Airflow configuration",
    )


class AutoDiscoveredTable(Base):
    __tablename__ = "auto_discovered_tables"

    hive_db: Mapped[str] = mapped_column(String(256), primary_key=True, nullable=False)
    dbalias: Mapped[str] = mapped_column(String(256), primary_key=True, nullable=False)
    source_schema: Mapped[str] = mapped_column(
        String(256), primary_key=True, nullable=False
    )
    source_table: Mapped[str] = mapped_column(
        String(256), primary_key=True, nullable=False
    )
    discovery_time: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, server_default=text("CURRENT_TIMESTAMP")
    )
    migrate_to_import_tables: Mapped[int] = mapped_column(
        TINYINT(4), nullable=False, server_default=text("'0'")
    )


class Configuration(Base):
    __tablename__ = "configuration"
    __table_args__ = {
        "comment": "This is the DBImport global configuration table. In here you can configure items such as the name of the staging database, disable global executions, max number of mappers and much more. "
    }

    config_key: Mapped[str] = mapped_column(
        "configKey",
        String(32),
        primary_key=True,
        comment="Name of the configuration item. These are controlled and maintained by the setup tool. Don't change these manually",
    )
    value_int: Mapped[int | None] = mapped_column(
        "valueInt", Integer, comment="Integer based Value"
    )
    value_str: Mapped[str | None] = mapped_column(
        "valueStr", String(256), comment="String based Value"
    )
    value_date: Mapped[datetime | None] = mapped_column(
        "valueDate", DateTime, comment="Date based Value"
    )
    description: Mapped[str | None] = mapped_column(
        Text, comment="Description on the setting"
    )


class ETLJobs(Base):
    __tablename__ = "etl_jobs"

    job: Mapped[str] = mapped_column(String(64), primary_key=True, nullable=False)
    task: Mapped[str] = mapped_column(String(64), primary_key=True, nullable=False)
    job_id: Mapped[int] = mapped_column(
        Integer, nullable=False, unique=True, info={"auto_increment": True}
    )
    etl_type: Mapped[str] = mapped_column(
        String(32), nullable=False, server_default=text("'0'")
    )
    include_in_airflow: Mapped[int] = mapped_column(
        TINYINT(4), nullable=False, server_default=text("'1'")
    )
    source_db: Mapped[str | None] = mapped_column(String(256))
    source_table: Mapped[str | None] = mapped_column(String(256))
    target_db: Mapped[str | None] = mapped_column(String(256))
    target_table: Mapped[str | None] = mapped_column(String(256))
    operator_notes: Mapped[str | None] = mapped_column(
        String(256), comment="Free text field to write a note about the import. "
    )


class ExportRetriesLog(Base):
    __tablename__ = "export_retries_log"
    __table_args__ = {"comment": "Log of all retries that have happened. "}

    dbalias: Mapped[str] = mapped_column(
        String(256),
        primary_key=True,
        nullable=False,
        comment="Database connection name that we export to",
    )
    target_schema: Mapped[str] = mapped_column(
        String(256),
        primary_key=True,
        nullable=False,
        comment="Schema on the target system",
    )
    target_table: Mapped[str] = mapped_column(
        String(256),
        primary_key=True,
        nullable=False,
        comment="Table on the target system",
    )
    retry_time: Mapped[datetime] = mapped_column(
        DateTime,
        primary_key=True,
        nullable=False,
        comment="Time when the retry was started",
    )
    stage: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        comment="The stage of the import that the retry started from. This is an internal stage and has nothing to do with stage1 and stage2 in Airflow DAG's",
    )
    stage_description: Mapped[str] = mapped_column(
        Text, nullable=False, comment="Description of the stage"
    )


class ExportStage(Base):
    __tablename__ = "export_stage"
    __table_args__ = {
        "comment": 'The export tool keeps track of how far in the export the tool have succeeded. So in case of an error, lets say that Hive is not responding, the next time an export is executed it will skip the first part and continue from where it ended in error on the previous run. If you want to rerun from the beginning, the information in this table needs to be cleared. This is done with the "manage --clearExportStage" tool. Keep in mind that clearing the stage of an incremental export might result in the loss of the data.'
    }

    dbalias: Mapped[str] = mapped_column(
        String(256),
        primary_key=True,
        nullable=False,
        comment="Database connection name that we export to",
    )
    target_schema: Mapped[str] = mapped_column(
        String(256),
        primary_key=True,
        nullable=False,
        comment="Schema on the target system",
    )
    target_table: Mapped[str] = mapped_column(
        String(256),
        primary_key=True,
        nullable=False,
        comment="Table on the target system",
    )
    stage: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        comment="Current stage of the export. This is the internal stage number",
    )
    stage_description: Mapped[str] = mapped_column(
        Text, nullable=False, comment="Description of the stage"
    )
    stage_time: Mapped[datetime | None] = mapped_column(
        DateTime, comment="The date and time when the import entered the stage"
    )


class ExportStageStatistic(Base):
    __tablename__ = "export_stage_statistics"
    __table_args__ = {
        "comment": "As DBImport progress through the different stages of the export, it also keeps track of start and stop time for each stage together with the duration. That information is kept in this table"
    }

    dbalias: Mapped[str] = mapped_column(
        String(256),
        primary_key=True,
        nullable=False,
        comment="Database connection name that we export to",
    )
    target_schema: Mapped[str] = mapped_column(
        String(256),
        primary_key=True,
        nullable=False,
        comment="Schema on the target system",
    )
    target_table: Mapped[str] = mapped_column(
        String(256),
        primary_key=True,
        nullable=False,
        comment="Table on the target system",
    )
    stage: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        nullable=False,
        comment="Current stage of the export. This is the internal stage number",
    )
    start: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, comment="Time when stage started"
    )
    stop: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, comment="Time when stage was completed"
    )
    duration: Mapped[int] = mapped_column(
        Integer, nullable=False, comment="Duration of stage"
    )


class ExportStatistic(Base):
    __tablename__ = "export_statistics"
    __table_args__ = (
        Index(
            "dbalias_target_schema_target_table",
            "dbalias",
            "target_schema",
            "target_table",
        ),
        {
            "comment": "At the end of each export, all statistics about how long each part took as well as general information about Hive database and table, number of rows imported, size of the export and much more are logged in this table. This table grows and will never be truncated by DBImport itself. If it becomes to large for you, it's up to each user to delete or truncate this table as you see fit."
        },
    )

    id: Mapped[int] = mapped_column(
        BIGINT(20),
        primary_key=True,
        autoincrement=True,
        comment="Auto incremented PrimaryKey of the table",
    )
    dbalias: Mapped[str | None] = mapped_column(
        String(256), comment="ID of the Database Connection"
    )
    target_database: Mapped[str | None] = mapped_column(
        String(256), comment="Name of the source database"
    )
    target_schema: Mapped[str | None] = mapped_column(
        String(256), comment="Name of the source schema"
    )
    target_table: Mapped[str | None] = mapped_column(
        String(256), comment="Name of the source table"
    )
    hive_db: Mapped[str | None] = mapped_column(String(256), comment="Hive Database")
    hive_table: Mapped[str | None] = mapped_column(String(256), comment="Hive Table")
    export_phase: Mapped[str | None] = mapped_column(
        String(32), comment="Import Phase method"
    )
    incremental: Mapped[int | None] = mapped_column(
        TINYINT(4), comment="0 = Full import, 1 = Incremental import"
    )
    rows: Mapped[int | None] = mapped_column(
        BIGINT(20), comment="How many rows that was imported"
    )
    size: Mapped[int | None] = mapped_column(
        BIGINT(20), comment="The total size in bytes that was imported"
    )
    sessions: Mapped[int | None] = mapped_column(
        TINYINT(4),
        comment="How many parallel sessions was used against the source (sqoop mappers)",
    )
    duration: Mapped[int | None] = mapped_column(
        Integer, comment="Total duration in seconds"
    )
    start: Mapped[datetime | None] = mapped_column(
        DateTime, comment="Timestamp of start"
    )
    stop: Mapped[datetime | None] = mapped_column(DateTime, comment="Timestamp of stop")
    get_hive_tableschema_duration: Mapped[int | None] = mapped_column(Integer)
    get_hive_tableschema_start: Mapped[datetime | None] = mapped_column(DateTime)
    get_hive_tableschema_stop: Mapped[datetime | None] = mapped_column(DateTime)
    clear_table_rowcount_duration: Mapped[int | None] = mapped_column(Integer)
    clear_table_rowcount_start: Mapped[datetime | None] = mapped_column(DateTime)
    clear_table_rowcount_stop: Mapped[datetime | None] = mapped_column(DateTime)
    create_temp_table_duration: Mapped[int | None] = mapped_column(Integer)
    create_temp_table_start: Mapped[datetime | None] = mapped_column(DateTime)
    create_temp_table_stop: Mapped[datetime | None] = mapped_column(DateTime)
    truncate_temp_table_duration: Mapped[int | None] = mapped_column(Integer)
    truncate_temp_table_start: Mapped[datetime | None] = mapped_column(DateTime)
    truncate_temp_table_stop: Mapped[datetime | None] = mapped_column(DateTime)
    fetch_maxvalue_start: Mapped[datetime | None] = mapped_column(DateTime)
    fetch_maxvalue_stop: Mapped[datetime | None] = mapped_column(DateTime)
    fetch_maxvalue_duration: Mapped[int | None] = mapped_column(Integer)
    insert_into_temp_table_duration: Mapped[int | None] = mapped_column(Integer)
    insert_into_temp_table_start: Mapped[datetime | None] = mapped_column(DateTime)
    insert_into_temp_table_stop: Mapped[datetime | None] = mapped_column(DateTime)
    create_target_table_duration: Mapped[int | None] = mapped_column(Integer)
    create_target_table_start: Mapped[datetime | None] = mapped_column(DateTime)
    create_target_table_stop: Mapped[datetime | None] = mapped_column(DateTime)
    truncate_target_table_duration: Mapped[int | None] = mapped_column(Integer)
    truncate_target_table_start: Mapped[datetime | None] = mapped_column(DateTime)
    truncate_target_table_stop: Mapped[datetime | None] = mapped_column(DateTime)
    sqoop_duration: Mapped[int | None] = mapped_column(Integer)
    sqoop_start: Mapped[datetime | None] = mapped_column(DateTime)
    sqoop_stop: Mapped[datetime | None] = mapped_column(DateTime)
    validate_duration: Mapped[int | None] = mapped_column(Integer)
    validate_start: Mapped[datetime | None] = mapped_column(DateTime)
    validate_stop: Mapped[datetime | None] = mapped_column(DateTime)
    update_statistics_duration: Mapped[int | None] = mapped_column(Integer)
    update_statistics_start: Mapped[datetime | None] = mapped_column(DateTime)
    update_statistics_stop: Mapped[datetime | None] = mapped_column(DateTime)
    update_target_table_duration: Mapped[int | None] = mapped_column(Integer)
    update_target_table_start: Mapped[datetime | None] = mapped_column(DateTime)
    update_target_table_stop: Mapped[datetime | None] = mapped_column(DateTime)
    spark_duration: Mapped[int | None] = mapped_column(Integer)
    spark_start: Mapped[datetime | None] = mapped_column(DateTime)
    spark_stop: Mapped[datetime | None] = mapped_column(DateTime)
    atlas_schema_duration: Mapped[int | None] = mapped_column(Integer)
    atlas_schema_start: Mapped[datetime | None] = mapped_column(DateTime)
    atlas_schema_stop: Mapped[datetime | None] = mapped_column(DateTime)


class ExportStatisticsLast(Base):
    __tablename__ = "export_statistics_last"
    __table_args__ = {
        "comment": "The last entry in table export_statistics is also stored in this table. This makes it easier to find the latest data without first grouping to find the latest entry. When export_statistics table grows to a high number of million rows, it saves a lot of cpu power for the database server."
    }

    dbalias: Mapped[str] = mapped_column(
        String(256),
        primary_key=True,
        nullable=False,
        comment="ID of the Database Connection",
    )
    target_database: Mapped[str] = mapped_column(
        String(256), nullable=False, comment="Name of the source database"
    )
    target_schema: Mapped[str] = mapped_column(
        String(256),
        primary_key=True,
        nullable=False,
        comment="Name of the source schema",
    )
    target_table: Mapped[str] = mapped_column(
        String(256),
        primary_key=True,
        nullable=False,
        comment="Name of the source table",
    )
    hive_db: Mapped[str | None] = mapped_column(String(256), comment="Hive Database")
    hive_table: Mapped[str | None] = mapped_column(String(256), comment="Hive Table")
    export_phase: Mapped[str | None] = mapped_column(
        String(32), comment="Import Phase method"
    )
    incremental: Mapped[int | None] = mapped_column(
        TINYINT(4), comment="0 = Full import, 1 = Incremental import"
    )
    rows: Mapped[int | None] = mapped_column(
        BIGINT(20), comment="How many rows that was imported"
    )
    size: Mapped[int | None] = mapped_column(
        BIGINT(20), comment="The total size in bytes that was imported"
    )
    sessions: Mapped[int | None] = mapped_column(
        TINYINT(4),
        comment="How many parallel sessions was used against the source (sqoop mappers)",
    )
    duration: Mapped[int | None] = mapped_column(
        Integer, comment="Total duration in seconds"
    )
    start: Mapped[datetime | None] = mapped_column(
        DateTime, comment="Timestamp of start"
    )
    stop: Mapped[datetime | None] = mapped_column(DateTime, comment="Timestamp of stop")
    get_hive_tableschema_duration: Mapped[int | None] = mapped_column(Integer)
    get_hive_tableschema_start: Mapped[datetime | None] = mapped_column(DateTime)
    get_hive_tableschema_stop: Mapped[datetime | None] = mapped_column(DateTime)
    clear_table_rowcount_duration: Mapped[int | None] = mapped_column(Integer)
    clear_table_rowcount_start: Mapped[datetime | None] = mapped_column(DateTime)
    clear_table_rowcount_stop: Mapped[datetime | None] = mapped_column(DateTime)
    create_temp_table_duration: Mapped[int | None] = mapped_column(Integer)
    create_temp_table_start: Mapped[datetime | None] = mapped_column(DateTime)
    create_temp_table_stop: Mapped[datetime | None] = mapped_column(DateTime)
    truncate_temp_table_duration: Mapped[int | None] = mapped_column(Integer)
    truncate_temp_table_start: Mapped[datetime | None] = mapped_column(DateTime)
    truncate_temp_table_stop: Mapped[datetime | None] = mapped_column(DateTime)
    fetch_maxvalue_start: Mapped[datetime | None] = mapped_column(DateTime)
    fetch_maxvalue_stop: Mapped[datetime | None] = mapped_column(DateTime)
    fetch_maxvalue_duration: Mapped[int | None] = mapped_column(Integer)
    insert_into_temp_table_duration: Mapped[int | None] = mapped_column(Integer)
    insert_into_temp_table_start: Mapped[datetime | None] = mapped_column(DateTime)
    insert_into_temp_table_stop: Mapped[datetime | None] = mapped_column(DateTime)
    create_target_table_duration: Mapped[int | None] = mapped_column(Integer)
    create_target_table_start: Mapped[datetime | None] = mapped_column(DateTime)
    create_target_table_stop: Mapped[datetime | None] = mapped_column(DateTime)
    truncate_target_table_duration: Mapped[int | None] = mapped_column(Integer)
    truncate_target_table_start: Mapped[datetime | None] = mapped_column(DateTime)
    truncate_target_table_stop: Mapped[datetime | None] = mapped_column(DateTime)
    sqoop_duration: Mapped[int | None] = mapped_column(Integer)
    sqoop_start: Mapped[datetime | None] = mapped_column(DateTime)
    sqoop_stop: Mapped[datetime | None] = mapped_column(DateTime)
    validate_duration: Mapped[int | None] = mapped_column(Integer)
    validate_start: Mapped[datetime | None] = mapped_column(DateTime)
    validate_stop: Mapped[datetime | None] = mapped_column(DateTime)
    update_statistics_duration: Mapped[int | None] = mapped_column(Integer)
    update_statistics_start: Mapped[datetime | None] = mapped_column(DateTime)
    update_statistics_stop: Mapped[datetime | None] = mapped_column(DateTime)
    update_target_table_duration: Mapped[int | None] = mapped_column(Integer)
    update_target_table_start: Mapped[datetime | None] = mapped_column(DateTime)
    update_target_table_stop: Mapped[datetime | None] = mapped_column(DateTime)
    spark_duration: Mapped[int | None] = mapped_column(Integer)
    spark_start: Mapped[datetime | None] = mapped_column(DateTime)
    spark_stop: Mapped[datetime | None] = mapped_column(DateTime)
    atlas_schema_duration: Mapped[int | None] = mapped_column(Integer)
    atlas_schema_start: Mapped[datetime | None] = mapped_column(DateTime)
    atlas_schema_stop: Mapped[datetime | None] = mapped_column(DateTime)


class ExportTables(Base):
    __tablename__ = "export_tables"
    __table_args__ = {
        "comment": "Main table where all tables that we can export are stored. "
    }

    dbalias: Mapped[str] = mapped_column(
        String(256),
        primary_key=True,
        nullable=False,
        comment="Database connection name that we export to",
    )
    target_schema: Mapped[str] = mapped_column(
        String(256),
        primary_key=True,
        nullable=False,
        comment="Schema on the target system",
    )
    target_table: Mapped[str] = mapped_column(
        String(256),
        primary_key=True,
        nullable=False,
        comment="Table on the target system",
    )
    table_id: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        unique=True,
        info={"auto_increment": True},
        comment="Unique identifier of the table",
    )
    hive_db: Mapped[str] = mapped_column(
        String(256), nullable=False, comment="Name of Hive Database that we export from"
    )
    hive_table: Mapped[str] = mapped_column(
        String(256), nullable=False, comment="Name of Hive Table that we export from"
    )
    export_type: Mapped[str] = mapped_column(
        Enum("full", "incr"),
        nullable=False,
        comment="What export method to use. Only full and incr is supported.",
        server_default=text("'full'"),
    )
    export_tool: Mapped[str] = mapped_column(
        Enum("spark", "sqoop"),
        server_default=text("'spark'"),
        nullable=False,
        comment="What tool should be used for exporting data",
    )
    last_update_from_hive: Mapped[datetime | None] = mapped_column(
        DateTime, comment="Timestamp of last schema update from Hive"
    )
    sql_where_addition: Mapped[str | None] = mapped_column(
        String(1024),
        comment='Will be added AFTER the SQL WHERE. If it\'s an incr export, this will be after the incr limit statements. Example "orderId > 1000"',
    )
    include_in_airflow: Mapped[int] = mapped_column(
        TINYINT(4),
        nullable=False,
        comment="Will the table be included in Airflow DAG when it matches the DAG selection",
        server_default=text("'1'"),
    )
    not_used_01: Mapped[int] = mapped_column(
        "notUsed01",
        TINYINT(4),
        nullable=False,
        comment="<NOT USED>",
        server_default=text("'0'"),
    )
    force_create_temp_table: Mapped[int] = mapped_column(
        "forceCreateTempTable",
        TINYINT(4),
        nullable=False,
        comment="Force export to create a Hive table and export that instead. Useful when exporting views",
        server_default=text("'0'"),
    )
    not_used_02: Mapped[int] = mapped_column(
        "notUsed02",
        TINYINT(4),
        nullable=False,
        comment="<NOT USED>",
        server_default=text("'0'"),
    )
    validate_export: Mapped[int] = mapped_column(
        TINYINT(4),
        nullable=False,
        comment="1 = Validate the export once it's done. 0 = Disable validation",
        server_default=text("'1'"),
    )
    validation_method: Mapped[str] = mapped_column(
        "validationMethod",
        Enum("customQuery", "rowCount"),
        nullable=False,
        comment="Validation method to use",
        server_default=text("'rowCount'"),
    )
    validation_custom_query_hive_sql: Mapped[str | None] = mapped_column(
        "validationCustomQueryHiveSQL", Text, comment="Custom SQL query for Hive table"
    )
    validation_custom_query_target_sql: Mapped[str | None] = mapped_column(
        "validationCustomQueryTargetSQL",
        Text,
        comment="Custom SQL query for target table",
    )
    uppercase_columns: Mapped[int] = mapped_column(
        TINYINT(4),
        nullable=False,
        comment="-1 = auto (Oracle = uppercase, other databases = lowercase)",
        server_default=text("'-1'"),
    )
    truncate_target: Mapped[int] = mapped_column(
        TINYINT(4),
        nullable=False,
        comment="1 = Truncate the target table before we export the data. Not used by incremental exports",
        server_default=text("'1'"),
    )
    mappers: Mapped[int] = mapped_column(
        TINYINT(4),
        nullable=False,
        comment="-1 = auto, 0 = invalid. Auto updated by 'export_main.sh'",
        server_default=text("'-1'"),
    )
    hive_rowcount: Mapped[int | None] = mapped_column(
        BIGINT(20), comment="Number of rows in Hive table. Don't change manually"
    )
    target_rowcount: Mapped[int | None] = mapped_column(
        BIGINT(20), comment="Number of rows in Target table. Don't change manually"
    )
    validation_custom_query_hive_value: Mapped[str | None] = mapped_column(
        "validationCustomQueryHiveValue",
        Text,
        comment="Used for validation. Don't change manually",
    )
    validation_custom_query_target_value: Mapped[str | None] = mapped_column(
        "validationCustomQueryTargetValue",
        Text,
        comment="Used for validation. Don't change manually",
    )
    incr_column: Mapped[str | None] = mapped_column(
        String(256),
        comment="The column in the Hive table that will be used to identify new rows for the incremental export. Must be a timestamp column",
    )
    incr_validation_method: Mapped[str | None] = mapped_column(
        Enum("full", "incr"),
        comment="full or incr. Full means that the validation will check to total number of rows up until maxvalue and compare source with target. Incr will only compare the rows between min and max value (the data that sqoop just wrote)",
        server_default=text("'full'"),
    )
    incr_minvalue: Mapped[str | None] = mapped_column(
        String(32),
        comment="Used by incremental exports to keep track of progress. Don't change manually",
    )
    incr_maxvalue: Mapped[str | None] = mapped_column(
        String(32),
        comment="Used by incremental exports to keep track of progress. Don't change manually",
    )
    incr_minvalue_pending: Mapped[str | None] = mapped_column(
        String(32),
        comment="Used by incremental exports to keep track of progress. Don't change manually",
    )
    incr_maxvalue_pending: Mapped[str | None] = mapped_column(
        String(32),
        comment="Used by incremental exports to keep track of progress. Don't change manually",
    )
    sqoop_options: Mapped[str | None] = mapped_column(
        String(1024), comment="Sqoop options to use during export."
    )
    sqoop_last_size: Mapped[int | None] = mapped_column(
        BIGINT(20), comment="Used to track sqoop operation. Don't change manually"
    )
    sqoop_last_rows: Mapped[int | None] = mapped_column(
        BIGINT(20), comment="Used to track sqoop operation. Don't change manually"
    )
    sqoop_last_mappers: Mapped[int | None] = mapped_column(
        TINYINT(4), comment="Used to track sqoop operation. Don't change manually"
    )
    sqoop_last_execution: Mapped[int | None] = mapped_column(
        BIGINT(20), comment="Used to track sqoop operation. Don't change manually"
    )
    create_target_table_sql: Mapped[str | None] = mapped_column(
        Text,
        comment="SQL statement that was used to create the target table. Don't change manually",
    )
    operator_notes: Mapped[str | None] = mapped_column(
        Text, comment="Free text field to write a note about the export. "
    )
    hive_javaheap: Mapped[int | None] = mapped_column(
        BIGINT(20), comment="Heap size for Hive"
    )
    airflow_priority: Mapped[int | None] = mapped_column(
        TINYINT(4), comment="This will set priority_weight in Airflow"
    )
    airflow_notes: Mapped[str | None] = mapped_column(
        Text, comment="Documentation that will be available in Airflow. Markdown syntax"
    )


class ImportFailureLog(Base):
    __tablename__ = "import_failure_log"
    __table_args__ = (
        Index("hive_db_hive_table", "hive_db", "hive_table"),
        {
            "comment": "If there is an error or a warning during import, but the import still continues, these errors are logged in this table. An example could be that  a column can't be altered, foreign key not created, no new columns can be added and such."
        },
    )

    hive_db: Mapped[str] = mapped_column(
        String(256), primary_key=True, nullable=False, comment="Hive Database"
    )
    hive_table: Mapped[str] = mapped_column(
        String(256), primary_key=True, nullable=False, comment="Hive Table"
    )
    eventtime: Mapped[datetime] = mapped_column(
        DateTime,
        primary_key=True,
        nullable=False,
        comment="Time when error/warning occurred",
        server_default=text("CURRENT_TIMESTAMP"),
    )
    severity: Mapped[str | None] = mapped_column(
        Text, comment="The Severity of the event. "
    )
    import_type: Mapped[str | None] = mapped_column(
        Text, comment="The import method used"
    )
    error_text: Mapped[str | None] = mapped_column(
        Text, comment="Text describing the failure"
    )


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


class ImportRetriesLog(Base):
    __tablename__ = "import_retries_log"
    __table_args__ = {"comment": "Log of all retries that have happened. "}

    hive_db: Mapped[str] = mapped_column(
        String(256), primary_key=True, nullable=False, comment="Hive DB"
    )
    hive_table: Mapped[str] = mapped_column(
        String(256), primary_key=True, nullable=False, comment="Hive Table"
    )
    retry_time: Mapped[datetime] = mapped_column(
        DateTime,
        primary_key=True,
        nullable=False,
        comment="Time when the retry was started",
    )
    stage: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        comment="The stage of the import that the retry started from. This is an internal stage and has nothing to do with stage1 and stage2 in Airflow DAG's",
    )
    stage_description: Mapped[str] = mapped_column(
        Text, nullable=False, comment="Description of the stage"
    )


class ImportStage(Base):
    __tablename__ = "import_stage"
    __table_args__ = {
        "comment": 'The import tool keeps track of how far in the import the tool have succeeded. So in case of an error, lets say that Hive is not responding, the next time an import is executed it will skip the first part and continue from where it ended in error on the previous run. If you want to rerun from the beginning, the information in this table needs to be cleared. This is done with the "manage --clearImportStage" tool. Keep in mind that clearing the stage of an incremental import might result in the loss of the data.'
    }

    hive_db: Mapped[str] = mapped_column(
        String(256), primary_key=True, nullable=False, comment="Hive Database"
    )
    hive_table: Mapped[str] = mapped_column(
        String(256), primary_key=True, nullable=False, comment="Hive Table"
    )
    stage: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        nullable=False,
        comment="Current stage of the import. This is an internal stage and has nothing to do with stage1 and stage2 in Airflow DAG's",
    )
    stage_description: Mapped[str] = mapped_column(
        Text, nullable=False, comment="Description of the stage"
    )
    stage_time: Mapped[datetime | None] = mapped_column(
        DateTime, comment="The date and time when the import entered the stage"
    )


class ImportStageStatistic(Base):
    __tablename__ = "import_stage_statistics"
    __table_args__ = {
        "comment": "As DBImport progress through the different stages of the import, it also keeps track of start and stop time for each stage together with the duration. That information is kept in this table"
    }

    hive_db: Mapped[str] = mapped_column(
        String(256), primary_key=True, nullable=False, comment="Hive Database"
    )
    hive_table: Mapped[str] = mapped_column(
        String(256), primary_key=True, nullable=False, comment="Hive Table"
    )
    stage: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        nullable=False,
        comment="Current stage of the import. This is an internal stage and has nothing to do with stage1 and stage2 in Airflow DAG's",
    )
    start: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, comment="Time when stage started"
    )
    stop: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, comment="Time when stage was completed"
    )
    duration: Mapped[int] = mapped_column(
        Integer, nullable=False, comment="Duration of stage"
    )


class ImportStatistic(Base):
    __tablename__ = "import_statistics"
    __table_args__ = (
        Index("hive_db_hive_table", "hive_db", "hive_table"),
        {
            "comment": "At the end of each import, all statistics about how long each part took as well as general information about Hive database and table, number of rows imported, size of the import and much more are logged in this table. This table grows and will never be truncated by DBImport itself. If it becomes to large for you, it's up to each user to delete or truncate this table as you see fit."
        },
    )

    id: Mapped[int] = mapped_column(
        BIGINT(20),
        primary_key=True,
        autoincrement=True,
        comment="Auto incremented PrimaryKey of the table",
    )
    hive_db: Mapped[str | None] = mapped_column(String(256), comment="Hive Database")
    hive_table: Mapped[str | None] = mapped_column(String(256), comment="Hive Table")
    importtype: Mapped[str | None] = mapped_column(
        String(32), comment="What kind of import type that was used"
    )
    import_phase: Mapped[str | None] = mapped_column(
        String(32), comment="Import Phase method"
    )
    copy_phase: Mapped[str | None] = mapped_column(
        String(32), comment="Copy Phase method"
    )
    etl_phase: Mapped[str | None] = mapped_column(
        String(32), comment="ETL Phase method"
    )
    incremental: Mapped[int | None] = mapped_column(
        TINYINT(4), comment="0 = Full import, 1 = Incremental import"
    )
    dbalias: Mapped[str | None] = mapped_column(
        String(256), comment="ID of the Database Connection"
    )
    source_database: Mapped[str | None] = mapped_column(
        String(256), comment="Name of the source database"
    )
    source_schema: Mapped[str | None] = mapped_column(
        String(256), comment="Name of the source schema"
    )
    source_table: Mapped[str | None] = mapped_column(
        String(256), comment="Name of the source table"
    )
    rows: Mapped[int | None] = mapped_column(
        BIGINT(20), comment="How many rows that was imported"
    )
    size: Mapped[int | None] = mapped_column(
        BIGINT(20), comment="The total size in bytes that was imported"
    )
    sessions: Mapped[int | None] = mapped_column(
        TINYINT(4),
        comment="How many parallel sessions was used against the source (sqoop mappers)",
    )
    duration: Mapped[int | None] = mapped_column(
        Integer, comment="Total duration in seconds"
    )
    start: Mapped[datetime | None] = mapped_column(
        DateTime, comment="Timestamp of start"
    )
    stop: Mapped[datetime | None] = mapped_column(DateTime, comment="Timestamp of stop")
    sqoop_duration: Mapped[int | None] = mapped_column(Integer)
    sqoop_start: Mapped[datetime | None] = mapped_column(DateTime)
    sqoop_stop: Mapped[datetime | None] = mapped_column(DateTime)
    clear_hive_locks_duration: Mapped[int | None] = mapped_column(Integer)
    clear_hive_locks_start: Mapped[datetime | None] = mapped_column(DateTime)
    clear_hive_locks_stop: Mapped[datetime | None] = mapped_column(DateTime)
    clear_table_rowcount_duration: Mapped[int | None] = mapped_column(Integer)
    clear_table_rowcount_start: Mapped[datetime | None] = mapped_column(DateTime)
    clear_table_rowcount_stop: Mapped[datetime | None] = mapped_column(DateTime)
    connect_to_hive_duration: Mapped[int | None] = mapped_column(Integer)
    connect_to_hive_start: Mapped[datetime | None] = mapped_column(DateTime)
    connect_to_hive_stop: Mapped[datetime | None] = mapped_column(DateTime)
    create_import_table_duration: Mapped[int | None] = mapped_column(Integer)
    create_import_table_start: Mapped[datetime | None] = mapped_column(DateTime)
    create_import_table_stop: Mapped[datetime | None] = mapped_column(DateTime)
    create_target_table_duration: Mapped[int | None] = mapped_column(Integer)
    create_target_table_start: Mapped[datetime | None] = mapped_column(DateTime)
    create_target_table_stop: Mapped[datetime | None] = mapped_column(DateTime)
    get_import_rowcount_duration: Mapped[int | None] = mapped_column(Integer)
    get_import_rowcount_start: Mapped[datetime | None] = mapped_column(DateTime)
    get_import_rowcount_stop: Mapped[datetime | None] = mapped_column(DateTime)
    get_source_rowcount_duration: Mapped[int | None] = mapped_column(Integer)
    get_source_rowcount_start: Mapped[datetime | None] = mapped_column(DateTime)
    get_source_rowcount_stop: Mapped[datetime | None] = mapped_column(DateTime)
    get_source_tableschema_duration: Mapped[int | None] = mapped_column(Integer)
    get_source_tableschema_start: Mapped[datetime | None] = mapped_column(DateTime)
    get_source_tableschema_stop: Mapped[datetime | None] = mapped_column(DateTime)
    get_target_rowcount_duration: Mapped[int | None] = mapped_column(Integer)
    get_target_rowcount_start: Mapped[datetime | None] = mapped_column(DateTime)
    get_target_rowcount_stop: Mapped[datetime | None] = mapped_column(DateTime)
    hive_import_duration: Mapped[int | None] = mapped_column(Integer)
    hive_import_start: Mapped[datetime | None] = mapped_column(DateTime)
    hive_import_stop: Mapped[datetime | None] = mapped_column(DateTime)
    truncate_target_table_duration: Mapped[int | None] = mapped_column(Integer)
    truncate_target_table_start: Mapped[datetime | None] = mapped_column(DateTime)
    truncate_target_table_stop: Mapped[datetime | None] = mapped_column(DateTime)
    merge_table_duration: Mapped[int | None] = mapped_column(Integer)
    merge_table_start: Mapped[datetime | None] = mapped_column(DateTime)
    merge_table_stop: Mapped[datetime | None] = mapped_column(DateTime)
    create_history_table_duration: Mapped[int | None] = mapped_column(Integer)
    create_history_table_start: Mapped[datetime | None] = mapped_column(DateTime)
    create_history_table_stop: Mapped[datetime | None] = mapped_column(DateTime)
    create_delete_table_duration: Mapped[int | None] = mapped_column(Integer)
    create_delete_table_start: Mapped[datetime | None] = mapped_column(DateTime)
    create_delete_table_stop: Mapped[datetime | None] = mapped_column(DateTime)
    update_statistics_duration: Mapped[int | None] = mapped_column(Integer)
    update_statistics_start: Mapped[datetime | None] = mapped_column(DateTime)
    update_statistics_stop: Mapped[datetime | None] = mapped_column(DateTime)
    validate_import_table_duration: Mapped[int | None] = mapped_column(Integer)
    validate_import_table_start: Mapped[datetime | None] = mapped_column(DateTime)
    validate_import_table_stop: Mapped[datetime | None] = mapped_column(DateTime)
    validate_sqoop_import_duration: Mapped[int | None] = mapped_column(Integer)
    validate_sqoop_import_start: Mapped[datetime | None] = mapped_column(DateTime)
    validate_sqoop_import_stop: Mapped[datetime | None] = mapped_column(DateTime)
    validate_target_table_duration: Mapped[int | None] = mapped_column(Integer)
    validate_target_table_start: Mapped[datetime | None] = mapped_column(DateTime)
    validate_target_table_stop: Mapped[datetime | None] = mapped_column(DateTime)
    copy_data_duration: Mapped[int | None] = mapped_column(Integer)
    copy_data_start: Mapped[datetime | None] = mapped_column(DateTime)
    copy_data_stop: Mapped[datetime | None] = mapped_column(DateTime)
    copy_schema_duration: Mapped[int | None] = mapped_column(Integer)
    copy_schema_start: Mapped[datetime | None] = mapped_column(DateTime)
    copy_schema_stop: Mapped[datetime | None] = mapped_column(DateTime)
    spark_duration: Mapped[int | None] = mapped_column(Integer)
    spark_start: Mapped[datetime | None] = mapped_column(DateTime)
    spark_stop: Mapped[datetime | None] = mapped_column(DateTime)
    spark_etl_duration: Mapped[int | None] = mapped_column(Integer)
    spark_etl_start: Mapped[datetime | None] = mapped_column(DateTime)
    spark_etl_stop: Mapped[datetime | None] = mapped_column(DateTime)
    atlas_schema_duration: Mapped[int | None] = mapped_column(Integer)
    atlas_schema_start: Mapped[datetime | None] = mapped_column(DateTime)
    atlas_schema_stop: Mapped[datetime | None] = mapped_column(DateTime)


class ImportStatisticsLast(Base):
    __tablename__ = "import_statistics_last"
    __table_args__ = {
        "comment": "The last entry in table import_statistics is also stored in this table. This makes it easier to find the latest data without first grouping to find the latest entry. When import_statistics table grows to a high number of million rows, it saves a lot of cpu power for the database server."
    }

    hive_db: Mapped[str] = mapped_column(
        String(256), primary_key=True, comment="Hive Database"
    )
    hive_table: Mapped[str] = mapped_column(
        String(256), primary_key=True, comment="Hive Table"
    )
    importtype: Mapped[str | None] = mapped_column(
        String(32), comment="What kind of import type that was used"
    )
    import_phase: Mapped[str | None] = mapped_column(
        String(32), comment="Import Phase method"
    )
    copy_phase: Mapped[str | None] = mapped_column(
        String(32), comment="Copy Phase method"
    )
    etl_phase: Mapped[str | None] = mapped_column(
        String(32), comment="ETL Phase method"
    )
    incremental: Mapped[int | None] = mapped_column(
        TINYINT(4), comment="0 = Full import, 1 = Incremental import"
    )
    dbalias: Mapped[str | None] = mapped_column(
        String(256), comment="ID of the Database Connection"
    )
    source_database: Mapped[str | None] = mapped_column(
        String(256), comment="Name of the source database"
    )
    source_schema: Mapped[str | None] = mapped_column(
        String(256), comment="Name of the source schema"
    )
    source_table: Mapped[str | None] = mapped_column(
        String(256), comment="Name of the source table"
    )
    rows: Mapped[int | None] = mapped_column(
        BIGINT(20), comment="How many rows that was imported"
    )
    size: Mapped[int | None] = mapped_column(
        BIGINT(20), comment="The total size in bytes that was imported"
    )
    sessions: Mapped[int | None] = mapped_column(
        TINYINT(4),
        comment="How many parallel sessions was used against the source (sqoop mappers)",
    )
    duration: Mapped[int | None] = mapped_column(
        Integer, comment="Total duration in seconds"
    )
    start: Mapped[datetime | None] = mapped_column(
        DateTime, comment="Timestamp of start"
    )
    stop: Mapped[datetime | None] = mapped_column(DateTime, comment="Timestamp of stop")
    sqoop_duration: Mapped[int | None] = mapped_column(Integer)
    sqoop_start: Mapped[datetime | None] = mapped_column(DateTime)
    sqoop_stop: Mapped[datetime | None] = mapped_column(DateTime)
    clear_hive_locks_duration: Mapped[int | None] = mapped_column(Integer)
    clear_hive_locks_start: Mapped[datetime | None] = mapped_column(DateTime)
    clear_hive_locks_stop: Mapped[datetime | None] = mapped_column(DateTime)
    clear_table_rowcount_duration: Mapped[int | None] = mapped_column(Integer)
    clear_table_rowcount_start: Mapped[datetime | None] = mapped_column(DateTime)
    clear_table_rowcount_stop: Mapped[datetime | None] = mapped_column(DateTime)
    connect_to_hive_duration: Mapped[int | None] = mapped_column(Integer)
    connect_to_hive_start: Mapped[datetime | None] = mapped_column(DateTime)
    connect_to_hive_stop: Mapped[datetime | None] = mapped_column(DateTime)
    create_import_table_duration: Mapped[int | None] = mapped_column(Integer)
    create_import_table_start: Mapped[datetime | None] = mapped_column(DateTime)
    create_import_table_stop: Mapped[datetime | None] = mapped_column(DateTime)
    create_target_table_duration: Mapped[int | None] = mapped_column(Integer)
    create_target_table_start: Mapped[datetime | None] = mapped_column(DateTime)
    create_target_table_stop: Mapped[datetime | None] = mapped_column(DateTime)
    get_import_rowcount_duration: Mapped[int | None] = mapped_column(Integer)
    get_import_rowcount_start: Mapped[datetime | None] = mapped_column(DateTime)
    get_import_rowcount_stop: Mapped[datetime | None] = mapped_column(DateTime)
    get_source_rowcount_duration: Mapped[int | None] = mapped_column(Integer)
    get_source_rowcount_start: Mapped[datetime | None] = mapped_column(DateTime)
    get_source_rowcount_stop: Mapped[datetime | None] = mapped_column(DateTime)
    get_source_tableschema_duration: Mapped[int | None] = mapped_column(Integer)
    get_source_tableschema_start: Mapped[datetime | None] = mapped_column(DateTime)
    get_source_tableschema_stop: Mapped[datetime | None] = mapped_column(DateTime)
    get_target_rowcount_duration: Mapped[int | None] = mapped_column(Integer)
    get_target_rowcount_start: Mapped[datetime | None] = mapped_column(DateTime)
    get_target_rowcount_stop: Mapped[datetime | None] = mapped_column(DateTime)
    hive_import_duration: Mapped[int | None] = mapped_column(Integer)
    hive_import_start: Mapped[datetime | None] = mapped_column(DateTime)
    hive_import_stop: Mapped[datetime | None] = mapped_column(DateTime)
    truncate_target_table_duration: Mapped[int | None] = mapped_column(Integer)
    truncate_target_table_start: Mapped[datetime | None] = mapped_column(DateTime)
    truncate_target_table_stop: Mapped[datetime | None] = mapped_column(DateTime)
    merge_table_duration: Mapped[int | None] = mapped_column(Integer)
    merge_table_start: Mapped[datetime | None] = mapped_column(DateTime)
    merge_table_stop: Mapped[datetime | None] = mapped_column(DateTime)
    create_history_table_duration: Mapped[int | None] = mapped_column(Integer)
    create_history_table_start: Mapped[datetime | None] = mapped_column(DateTime)
    create_history_table_stop: Mapped[datetime | None] = mapped_column(DateTime)
    create_delete_table_duration: Mapped[int | None] = mapped_column(Integer)
    create_delete_table_start: Mapped[datetime | None] = mapped_column(DateTime)
    create_delete_table_stop: Mapped[datetime | None] = mapped_column(DateTime)
    update_statistics_duration: Mapped[int | None] = mapped_column(Integer)
    update_statistics_start: Mapped[datetime | None] = mapped_column(DateTime)
    update_statistics_stop: Mapped[datetime | None] = mapped_column(DateTime)
    validate_import_table_duration: Mapped[int | None] = mapped_column(Integer)
    validate_import_table_start: Mapped[datetime | None] = mapped_column(DateTime)
    validate_import_table_stop: Mapped[datetime | None] = mapped_column(DateTime)
    validate_sqoop_import_duration: Mapped[int | None] = mapped_column(Integer)
    validate_sqoop_import_start: Mapped[datetime | None] = mapped_column(DateTime)
    validate_sqoop_import_stop: Mapped[datetime | None] = mapped_column(DateTime)
    validate_target_table_duration: Mapped[int | None] = mapped_column(Integer)
    validate_target_table_start: Mapped[datetime | None] = mapped_column(DateTime)
    validate_target_table_stop: Mapped[datetime | None] = mapped_column(DateTime)
    copy_data_duration: Mapped[int | None] = mapped_column(Integer)
    copy_data_start: Mapped[datetime | None] = mapped_column(DateTime)
    copy_data_stop: Mapped[datetime | None] = mapped_column(DateTime)
    copy_schema_duration: Mapped[int | None] = mapped_column(Integer)
    copy_schema_start: Mapped[datetime | None] = mapped_column(DateTime)
    copy_schema_stop: Mapped[datetime | None] = mapped_column(DateTime)
    spark_duration: Mapped[int | None] = mapped_column(Integer)
    spark_start: Mapped[datetime | None] = mapped_column(DateTime)
    spark_stop: Mapped[datetime | None] = mapped_column(DateTime)
    spark_etl_duration: Mapped[int | None] = mapped_column(Integer)
    spark_etl_start: Mapped[datetime | None] = mapped_column(DateTime)
    spark_etl_stop: Mapped[datetime | None] = mapped_column(DateTime)
    atlas_schema_duration: Mapped[int | None] = mapped_column(Integer)
    atlas_schema_start: Mapped[datetime | None] = mapped_column(DateTime)
    atlas_schema_stop: Mapped[datetime | None] = mapped_column(DateTime)


class ImportTables(Base):
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

    hive_db: Mapped[str] = mapped_column(
        String(256),
        primary_key=True,
        nullable=False,
        comment="Hive Database to import to",
    )
    hive_table: Mapped[str] = mapped_column(
        String(256), primary_key=True, nullable=False, comment="Hive Table to import to"
    )
    table_id: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        unique=True,
        info={"auto_increment": True},
        comment="Unique identifier",
    )
    dbalias: Mapped[str] = mapped_column(
        String(256),
        nullable=False,
        comment="Name of database connection from jdbc_connections table",
    )
    source_schema: Mapped[str] = mapped_column(
        String(256), nullable=False, comment="Name of the schema in the remote database"
    )
    source_table: Mapped[str] = mapped_column(
        String(256), nullable=False, comment="Name of the table in the remote database"
    )
    import_phase_type: Mapped[str | None] = mapped_column(
        Enum("full", "incr", "oracle_flashback", "mssql_change_tracking"),
        nullable=True,
        comment="What method to use for ETL phase",
        server_default=text("'full'"),
    )
    etl_phase_type: Mapped[str | None] = mapped_column(
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
    import_type: Mapped[str | None] = mapped_column(
        String(32), nullable=True, comment="What import method to use"
    )
    import_tool: Mapped[str] = mapped_column(
        Enum("spark", "sqoop"),
        server_default=text("'spark'"),
        nullable=False,
        comment="What tool should be used for importing data",
    )
    etl_engine: Mapped[str] = mapped_column(
        Enum("hive", "spark"),
        server_default=text("'hive'"),
        nullable=False,
        comment="What engine will be used to process etl stage",
    )
    last_update_from_source: Mapped[datetime | None] = mapped_column(
        DateTime, comment="Timestamp of last schema update from source"
    )
    sqoop_sql_where_addition: Mapped[str | None] = mapped_column(
        String(1024),
        comment='Will be added AFTER the SQL WHERE. If it\'s an incr import, this will be after the incr limit statements. Example "orderId > 1000"',
    )
    nomerge_ingestion_sql_addition: Mapped[str | None] = mapped_column(
        String(2048),
        comment="This will be added to the data ingestion of None-Merge imports (full, full_direct and incr). Useful to filter out data from import tables to target tables",
    )
    include_in_airflow: Mapped[int] = mapped_column(
        TINYINT(4),
        nullable=False,
        comment="Will the table be included in Airflow DAG when it matches the DAG selection",
        server_default=text("'1'"),
    )
    airflow_priority: Mapped[int | None] = mapped_column(
        TINYINT(4), comment="This will set priority_weight in Airflow"
    )
    validate_import: Mapped[int] = mapped_column(
        TINYINT(4),
        nullable=False,
        comment="Should the import be validated",
        server_default=text("'1'"),
    )
    validation_method: Mapped[str] = mapped_column(
        "validationMethod",
        Enum("customQuery", "rowCount"),
        nullable=False,
        comment="Validation method to use",
        server_default=text("'rowCount'"),
    )
    validate_source: Mapped[str | None] = mapped_column(
        Enum("query", "sqoop"),
        comment="query = Run a 'select count(*) from ...' to get the number of rows in the source table. sqoop = Use the number of rows imported by sqoop as the number of rows in the source table",
        server_default=text("'query'"),
    )
    validate_diff_allowed: Mapped[int] = mapped_column(
        BIGINT(20),
        nullable=False,
        comment="-1 = auto calculated diff allowed. If a positive number, this is the amount of rows that the diff is allowed to have",
        server_default=text("'-1'"),
    )
    validation_custom_query_source_sql: Mapped[str | None] = mapped_column(
        "validationCustomQuerySourceSQL",
        Text,
        comment="Custom SQL query for source table",
    )
    validation_custom_query_hive_sql: Mapped[str | None] = mapped_column(
        "validationCustomQueryHiveSQL", Text, comment="Custom SQL query for Hive table"
    )
    validation_custom_query_validate_import_table: Mapped[int] = mapped_column(
        "validationCustomQueryValidateImportTable",
        TINYINT(4),
        nullable=False,
        comment="1 = Validate Import table, 0 = Don't validate Import table",
        server_default=text("'1'"),
    )
    truncate_hive: Mapped[int] = mapped_column(
        TINYINT(4), nullable=False, comment="<NOT USED>", server_default=text("'1'")
    )
    mappers: Mapped[int] = mapped_column(
        TINYINT(4),
        nullable=False,
        comment="-1 = auto or positive number for a fixed number of mappers. If Auto, then it's calculated based of last sqoop import size",
        server_default=text("'-1'"),
    )
    soft_delete_during_merge: Mapped[int] = mapped_column(
        TINYINT(4),
        nullable=False,
        comment="If 1, then the row will be marked as deleted instead of actually being removed from the table. Only used for Merge imports",
        server_default=text("'0'"),
    )
    source_rowcount: Mapped[int | None] = mapped_column(
        BIGINT(20), comment="Used for validation. Don't change manually"
    )
    source_rowcount_incr: Mapped[int | None] = mapped_column(BIGINT(20))
    hive_rowcount: Mapped[int | None] = mapped_column(
        BIGINT(20), comment="Used for validation. Don't change manually"
    )
    validation_custom_query_source_value: Mapped[str | None] = mapped_column(
        "validationCustomQuerySourceValue",
        Text,
        comment="Used for validation. Don't change manually",
    )
    validation_custom_query_hive_value: Mapped[str | None] = mapped_column(
        "validationCustomQueryHiveValue",
        Text,
        comment="Used for validation. Don't change manually",
    )
    incr_mode: Mapped[str | None] = mapped_column(
        Enum("append", "lastmodified"), comment="Incremental import mode"
    )
    incr_column: Mapped[str | None] = mapped_column(
        String(256), comment="What column to use to identify new rows"
    )
    incr_validation_method: Mapped[str | None] = mapped_column(
        Enum("full", "incr"),
        comment="full or incr. Full means that the validation will check to total number of rows up until maxvalue and compare source with target. Incr will only compare the rows between min and max value (the data that sqoop just wrote)",
        server_default=text("'full'"),
    )
    incr_minvalue: Mapped[str | None] = mapped_column(
        String(32), comment="Used for incremental imports. Don't change manually"
    )
    incr_maxvalue: Mapped[str | None] = mapped_column(
        String(32), comment="Used for incremental imports. Don't change manually"
    )
    incr_minvalue_pending: Mapped[str | None] = mapped_column(
        String(32), comment="Used for incremental imports. Don't change manually"
    )
    incr_maxvalue_pending: Mapped[str | None] = mapped_column(
        String(32), comment="Used for incremental imports. Don't change manually"
    )
    pk_column_override: Mapped[str | None] = mapped_column(
        String(1024),
        comment="Force the import and Hive table to define another PrimaryKey constraint. Comma separated list of columns",
    )
    pk_column_override_mergeonly: Mapped[str | None] = mapped_column(
        String(1024),
        comment="Force the import to use another PrimaryKey constraint during Merge operations. Comma separated list of columns",
    )
    hive_merge_heap: Mapped[int | None] = mapped_column(
        Integer,
        comment="Should be a multiple of Yarn container size. If NULL then it will use the default specified in Yarn and TEZ",
    )
    hive_split_count: Mapped[int | None] = mapped_column(
        Integer, comment="Sets tez.grouping.split-count in the Hive session"
    )
    concatenate_hive_table: Mapped[int] = mapped_column(
        TINYINT(4), nullable=False, comment="<NOT USED>", server_default=text("'-1'")
    )
    sqoop_query: Mapped[str | None] = mapped_column(
        Text, comment="Use a custom query in sqoop to read data from source table"
    )
    sqoop_options: Mapped[str | None] = mapped_column(
        Text, comment="Options to send to sqoop. Most common used for --split-by option"
    )
    sqoop_last_size: Mapped[int | None] = mapped_column(
        BIGINT(20), comment="Used to track sqoop operation. Don't change manually"
    )
    sqoop_last_rows: Mapped[int | None] = mapped_column(
        BIGINT(20), comment="Used to track sqoop operation. Don't change manually"
    )
    sqoop_last_mappers: Mapped[int | None] = mapped_column(
        TINYINT(4), comment="Used to track sqoop operation. Don't change manually"
    )
    sqoop_last_execution: Mapped[int | None] = mapped_column(
        BIGINT(20), comment="Used to track sqoop operation. Don't change manually"
    )
    sqoop_use_generated_sql: Mapped[int] = mapped_column(
        TINYINT(4),
        nullable=False,
        comment="1 = Use the generated SQL that is saved in the generated_sqoop_query column",
        server_default=text("'-1'"),
    )
    sqoop_allow_text_splitter: Mapped[int] = mapped_column(
        TINYINT(4),
        nullable=False,
        comment="Allow splits on text columns. Use with caution",
        server_default=text("'0'"),
    )
    force_string: Mapped[int] = mapped_column(
        TINYINT(4),
        nullable=False,
        comment="If set to 1, all character based fields (char, varchar) will become string in Hive. Overrides the same setting in jdbc_connections table",
        server_default=text("'-1'"),
    )
    comment: Mapped[str | None] = mapped_column(
        Text, comment="Table comment from source system. Don't change manually"
    )
    generated_hive_column_definition: Mapped[str | None] = mapped_column(
        Text,
        comment="Generated column definition for Hive create table. Don't change manually",
    )
    generated_sqoop_query: Mapped[str | None] = mapped_column(
        Text, comment="Generated query for sqoop. Don't change manually"
    )
    generated_sqoop_options: Mapped[str | None] = mapped_column(
        Text, comment="Generated options for sqoop. Don't change manually"
    )
    generated_pk_columns: Mapped[str | None] = mapped_column(
        Text, comment="Generated Primary Keys. Don't change manually"
    )
    generated_foreign_keys: Mapped[str | None] = mapped_column(
        Text, comment="<NOT USED>"
    )
    datalake_source: Mapped[str | None] = mapped_column(
        String(256),
        comment="This value will come in the dbimport_source column if present. Overrides the same setting in jdbc_connections table",
    )
    operator_notes: Mapped[str | None] = mapped_column(
        Text, comment="Free text field to write a note about the import. "
    )
    copy_finished: Mapped[datetime | None] = mapped_column(
        DateTime,
        comment="Time when last copy from Master DBImport instance was completed. Don't change manually",
    )
    copy_slave: Mapped[int] = mapped_column(
        TINYINT(4),
        nullable=False,
        comment="Defines if this table is a Master table or a Slave table. Don't change manually",
        server_default=text("'0'"),
    )
    create_foreign_keys: Mapped[int] = mapped_column(
        TINYINT(4),
        nullable=False,
        comment="-1 (default) = Get information from jdbc_connections table",
        server_default=text("'-1'"),
    )
    airflow_notes: Mapped[str | None] = mapped_column(
        Text, comment="Documentation that will be available in Airflow. Markdown syntax"
    )
    split_by_column: Mapped[str | None] = mapped_column(
        String(64),
        comment="Column to split by when doing import with multiple sessions",
    )
    spark_executor_memory: Mapped[str | None] = mapped_column(
        String(8),
        comment="Memory used by spark when importing data. Overrides default value in global configuration",
    )
    spark_executors: Mapped[int | None] = mapped_column(
        Integer,
        comment="Number of Spark executors to use. Overrides default value in global configuration",
    )
    custom_max_query: Mapped[str | None] = mapped_column(
        String(256),
        comment="You can use a custom SQL query that will get the Max value from the source database. This Max value will be used in an incremental import to know how much to read in each execution",
    )
    merge_compaction_method: Mapped[str] = mapped_column(
        "mergeCompactionMethod",
        Enum("default", "none", "minor", "minor_and_wait", "major", "major_and_wait"),
        nullable=False,
        comment="Compaction method to use after import using merge is completed. Default means a major compaction if it is configured to do so in the configuration table",
        server_default=text("'default'"),
    )
    source_table_type: Mapped[str | None] = mapped_column(
        "sourceTableType",
        String(16),
        comment="Type of table on the source system. This is a read-only",
    )
    invalidate_impala: Mapped[int] = mapped_column(
        TINYINT(4),
        nullable=False,
        comment="-1 = Use default value from configuration table, otherwise 1 will invalidate the table in Impala and 0 will not",
        server_default=text("'-1'"),
    )
    import_database: Mapped[str | None] = mapped_column(
        String(256), comment="Override the database name used for the import table"
    )
    import_table: Mapped[str | None] = mapped_column(
        String(256), comment="Override the table name used for the import table"
    )
    history_database: Mapped[str | None] = mapped_column(
        String(256),
        comment="Override the database name used for the history table if that exists",
    )
    history_table: Mapped[str | None] = mapped_column(
        String(256),
        comment="Override the table name used for the history table if that exists",
    )


class JDBCConnectionsEnvironments(Base):
    __tablename__ = "jdbc_connections_environments"
    __table_args__ = {
        "comment": "Environments types. Used in jdbc_connections table to define what kind of connection type it is"
    }

    environment: Mapped[str] = mapped_column(
        String(32), primary_key=True, comment="Name of the Environment type"
    )
    description: Mapped[str | None] = mapped_column(
        String(256), nullable=True, comment="Description of the Environment type"
    )


class JDBCConnections(Base):
    __tablename__ = "jdbc_connections"
    __table_args__ = {"comment": "Database connection definitions"}

    dbalias: Mapped[str] = mapped_column(
        String(256), primary_key=True, comment="Name of the Database connection"
    )
    private_key_path: Mapped[str | None] = mapped_column(
        String(128),
        comment="<NOT USED>",
    )
    public_key_path: Mapped[str | None] = mapped_column(
        String(128), comment="<NOT USED>"
    )
    jdbc_url: Mapped[str] = mapped_column(
        Text, nullable=False, comment="The JDBC URL String"
    )
    credentials: Mapped[str | None] = mapped_column(
        Text,
        comment="Encrypted fields for credentials.m Changed by the saveCredentialTool",
    )
    datalake_source: Mapped[str | None] = mapped_column(
        String(256),
        comment="This value will come in the dbimport_source column if present. Priority is table, connection",
    )
    max_import_sessions: Mapped[int | None] = mapped_column(
        TINYINT(4),
        comment="You can limit the number of parallel sessions during import with this value. If NULL, then Max will come from configuration file",
    )
    force_string: Mapped[int] = mapped_column(
        TINYINT(4),
        nullable=False,
        comment="If set to 1, all character based fields (char, varchar) will become string in Hive",
        server_default=text("'0'"),
    )
    create_datalake_import: Mapped[int] = mapped_column(
        TINYINT(4),
        nullable=False,
        comment="If set to 1, the datalake_import column will be created on all tables that is using this dbalias",
        server_default=text("'1'"),
    )
    timewindow_start: Mapped[time | None] = mapped_column(
        Time,
        comment="Start of the time window when we are allowed to run against this connection.",
    )
    timewindow_stop: Mapped[time | None] = mapped_column(
        Time,
        comment="End of the time window when we are allowed to run against this connection.",
    )
    timewindow_timezone: Mapped[str | None] = mapped_column(
        String(64),
        comment="Timezone used for timewindow_start and timewindow_stop columns. Use full text timezone, example Europe/Stockholm.",
    )
    operator_notes: Mapped[str | None] = mapped_column(
        Text, comment="Free text field to write a note about the connection"
    )
    create_foreign_keys: Mapped[int] = mapped_column(
        TINYINT(4),
        nullable=False,
        comment="1 = Create foreign keys, 0 = Don't create foreign keys",
        server_default=text("'1'"),
    )
    contact_info: Mapped[str | None] = mapped_column(
        String(255), comment="Contact information. Used by Atlas integration"
    )
    description: Mapped[str | None] = mapped_column(
        String(255), comment="Description. Used by Atlas integration"
    )
    owner: Mapped[str | None] = mapped_column(
        String(255), comment="Owner of system and/or data. Used by Atlas integration"
    )
    atlas_discovery: Mapped[int] = mapped_column(
        TINYINT(4),
        nullable=False,
        comment="1 = Discover tables/views on this connection, 0 = Don't use Atlas discovery on this connection",
        server_default=text("'1'"),
    )
    atlas_include_filter: Mapped[str | None] = mapped_column(
        String(256), comment="Include filter for Atlas discovery"
    )
    atlas_exclude_filter: Mapped[str | None] = mapped_column(
        String(256), comment="Exclude filter for Atlas discovery"
    )
    atlas_last_discovery: Mapped[datetime | None] = mapped_column(DateTime)
    environment: Mapped[str | None] = mapped_column(
        String(32),
        ForeignKey(
            "jdbc_connections_environments.environment",
            name="FK_jdbc_connections__jdbc_connections_environments",
        ),
        nullable=True,
        comment="Name of the Environment type",
    )
    seed_file: Mapped[str | None] = mapped_column(
        String(256),
        comment="File that will be used to fetch the custom seed that will be used for anonymization functions on data from the connection",
    )

    jdbc_connections_environments: Mapped["JDBCConnectionsEnvironments | None"] = (
        relationship("JDBCConnectionsEnvironments")
    )


class JDBCConnectionsDrivers(Base):
    __tablename__ = "jdbc_connections_drivers"

    database_type: Mapped[str] = mapped_column(
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
    version: Mapped[str] = mapped_column(
        String(16),
        primary_key=True,
        nullable=False,
        comment="Free-text field with version. Has nothing to do with version of driver itself.",
    )
    driver: Mapped[str] = mapped_column(
        String(128), nullable=False, comment="Java class for JDBC driver"
    )
    classpath: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
        comment="Full path to JDBC driver/jar file. If more than one file is required, separate them with : and no spaces",
    )


t_jdbc_table_change_history = Table(
    "jdbc_table_change_history",
    metadata,
    Column("dbalias", String(256), nullable=False, comment="Database connection name"),
    Column("db_name", String(256), nullable=False, comment="Database name"),
    Column("schema_name", String(256), nullable=False, comment="Schema Name"),
    Column("table_name", String(256), nullable=False, comment="Table Name"),
    Column("column_name", String(256), nullable=False, comment="Column Name"),
    Column("eventtime", DateTime, nullable=False, comment="Time and date of the event"),
    Column("event", String(128), nullable=False, comment="Event name."),
    Column("previous_value", String(256), comment="Value before the change"),
    Column("value", String(256), comment="Value after the change"),
    Column(
        "description",
        String(512),
        nullable=False,
        comment="Detailed description of the event",
    ),
    Index(
        "dbalias_database_schema_table",
        "dbalias",
        "db_name",
        "schema_name",
        "table_name",
    ),
    comment="This table keeps track of all changes that was done to a JDBC table after the initial creation. Example could be that a column type was changed from char(10) to varchar(10). That kind of information is logged in this table",
)


class JSONToSend(Base):
    __tablename__ = "json_to_send"
    __table_args__ = {
        "comment": "Temporary storage of JSON payloads that will be sent to a REST interface if the tool is configured to do so."
    }

    id: Mapped[int] = mapped_column(
        BIGINT(20), primary_key=True, autoincrement=True, comment="Unique Identifier"
    )
    type: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        comment="The type of JSON data that is saved in the 'jsondata' column",
    )
    create_time: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        comment="Time when the JSON data was created",
        server_default=text("CURRENT_TIMESTAMP"),
    )
    status: Mapped[int] = mapped_column(
        TINYINT(4),
        nullable=False,
        comment="Internal status to keep track of what the status of the transmissions is",
    )
    destination: Mapped[str] = mapped_column(
        String(8), nullable=False, comment="The destination where to send the json."
    )
    jsondata: Mapped[str] = mapped_column(
        Text, nullable=False, comment="The payload to send"
    )


t_table_change_history = Table(
    "table_change_history",
    metadata,
    Column("hive_db", String(256), nullable=False, comment="Hive Database"),
    Column("hive_table", String(256), nullable=False, comment="Hive Table"),
    Column("column_name", String(256), nullable=False, comment="Column Name"),
    Column("eventtime", DateTime, nullable=False, comment="Time and date of the event"),
    Column("event", String(128), nullable=False, comment="Event name."),
    Column("previous_value", String(256), comment="Value before the change"),
    Column("value", String(256), comment="Value after the change"),
    Column(
        "description",
        String(512),
        nullable=False,
        comment="Detailed description of the event",
    ),
    Index("hive_db_hive_table", "hive_db", "hive_table"),
    comment="This table keeps track of all changes that was done to an object after the initial load. Example could be that a column type was changed from char(10) to varchar(10). That kind of information is logged in this table",
)


class AirflowTasks(Base):
    __tablename__ = "airflow_tasks"
    __table_args__ = {
        "comment": "All DAGs can be customized by adding Tasks into the DAG. Depending on what placement and type of Tasks that is created, DBImport will add custom placeholders to keep the DAG separated in three different parts. Before, In and After Main. In main is where all regular Imports, export or ETL jobs are executed. If you want to execute something before these, you place it in Before Main. And if you want to execute something after, you place it in After main. Please check the Airflow Integration part of the documentation for more examples and better understanding of the data you can put into this table"
    }

    dag_name: Mapped[str] = mapped_column(
        String(64),
        primary_key=True,
        nullable=False,
        comment="Name of DAG to add Tasks to",
    )
    task_name: Mapped[str] = mapped_column(
        String(64),
        primary_key=True,
        nullable=False,
        comment="Name of the Task in Airflow",
    )
    task_type: Mapped[str] = mapped_column(
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
        server_default=text("'Hive SQL Script'"),
        comment="The type of the Task",
    )
    placement: Mapped[str] = mapped_column(
        Enum("before main", "after main", "in main"),
        nullable=False,
        server_default=text("'after main'"),
        comment="Placement for the Task",
    )
    jdbc_dbalias: Mapped[str | None] = mapped_column(
        ForeignKey(
            "jdbc_connections.dbalias", name="FK_airflow_tasks_jdbc_connections"
        ),
        comment="For  'JDBC SQL' Task Type, this specifies what database the SQL should run against",
    )
    hive_db: Mapped[str | None] = mapped_column(String(256), comment="<NOT USED>")
    airflow_pool: Mapped[str | None] = mapped_column(
        String(64), comment="Airflow Pool to use."
    )
    airflow_priority: Mapped[int | None] = mapped_column(
        TINYINT(4), comment="Airflow Priority. Higher number, higher priority"
    )
    include_in_airflow: Mapped[int] = mapped_column(
        TINYINT(4),
        nullable=False,
        server_default=text("'1'"),
        comment="Enable or disable the Task in the DAG during creation of DAG file.",
    )
    task_dependency_upstream: Mapped[str | None] = mapped_column(
        String(256),
        comment="Defines the upstream dependency for the Task. Comma separated list",
    )
    task_dependency_downstream: Mapped[str | None] = mapped_column(
        String(256),
        comment="Defines the downstream dependency for the Task. Comma separated list",
    )
    task_config: Mapped[str | None] = mapped_column(
        String(512),
        comment="The configuration for the Task. Depends on what Task type it is.",
    )
    sensor_connection: Mapped[str | None] = mapped_column(
        String(64), nullable=True, comment="Name of Connection in Airflow"
    )
    sensor_poke_interval: Mapped[int | None] = mapped_column(
        Integer, nullable=True, comment="Poke interval for sensors in seconds"
    )
    sensor_timeout_minutes: Mapped[int | None] = mapped_column(
        Integer, nullable=True, comment="Timeout for sensors in minutes"
    )
    sensor_soft_fail: Mapped[int | None] = mapped_column(
        Integer,
        nullable=True,
        comment="Setting this to 1 will add soft_fail=True on sensor",
    )
    sudo_user: Mapped[str | None] = mapped_column(
        String(64), comment="The task will use this user for sudo instead of default"
    )

    jdbc_connection: Mapped["JDBCConnections | None"] = relationship("JDBCConnections")


class ExportColumns(Base):
    __tablename__ = "export_columns"
    __table_args__ = (
        PrimaryKeyConstraint("table_id", "column_id", "column_name"),
        {
            "comment": "This table contains all columns that exists on all tables that we are exporting. Unlike the export_tables table, this one gets created automatically by the export tool"
        },
    )

    table_id: Mapped[int] = mapped_column(
        ForeignKey("export_tables.table_id", ondelete="CASCADE"),
        primary_key=True,
        nullable=False,
        comment="Foreign Key to export_tables column 'table_id'",
    )
    column_id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        nullable=False,
        unique=True,
        autoincrement=True,
        comment="Unique identifier",
    )
    column_name: Mapped[str] = mapped_column(
        String(256),
        primary_key=True,
        nullable=False,
        comment="Name of column in target table. Don't change this manually",
    )
    column_type: Mapped[str | None] = mapped_column(
        String(2048), comment="Column type from Hive. Don't change this manually"
    )
    column_order: Mapped[int | None] = mapped_column(
        Integer, comment="The order of the columns. Don't change this manually"
    )
    hive_db: Mapped[str | None] = mapped_column(
        String(256),
        comment="Only used to make it easier to read the table. No real usage",
    )
    hive_table: Mapped[str | None] = mapped_column(
        String(256),
        comment="Only used to make it easier to read the table. No real usage",
    )
    target_column_name: Mapped[str | None] = mapped_column(
        String(256), comment="Override the name of column in the target system"
    )
    target_column_type: Mapped[str | None] = mapped_column(
        String(256), comment="Override the column type in the target system"
    )
    last_update_from_hive: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        comment="Timestamp of last schema update from Hive. Don't change this manually",
    )
    last_export_time: Mapped[datetime | None] = mapped_column(
        DateTime, comment="Timestamp of last export. Don't change this manually"
    )
    selection: Mapped[str | None] = mapped_column(String(256), comment="<NOT USED>")
    include_in_export: Mapped[int] = mapped_column(
        TINYINT(4),
        nullable=False,
        comment="1 = Include column in export, 0 = Exclude column in export",
        server_default=text("'1'"),
    )
    comment: Mapped[str | None] = mapped_column(
        Text,
        comment="The column comment from the source system. Don't change this manually",
    )
    operator_notes: Mapped[str | None] = mapped_column(
        Text, comment="Free text field to write a note about the import. "
    )

    table: Mapped["ExportTables"] = relationship("ExportTables")


class ImportColumns(Base):
    __tablename__ = "import_columns"
    __table_args__ = (
        PrimaryKeyConstraint("table_id", "column_id", "column_name"),
        Index("table_id_source_column_name", "table_id", "source_column_name"),
        {
            "comment": "This table contains all columns that exists on all tables that we are importing. Unlike the import_tables table, this one gets created automatically by the 'Get Source TableSchema' stage. "
        },
    )

    table_id: Mapped[int] = mapped_column(
        ForeignKey("import_tables.table_id", ondelete="CASCADE"),
        primary_key=True,
        nullable=False,
        comment="Foreign Key to import_tables column 'table_id'",
    )
    column_id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        nullable=False,
        unique=True,
        autoincrement=True,
        comment="Unique identifier of the column",
    )
    column_order: Mapped[int | None] = mapped_column(
        Integer, comment="In what order does the column exist in the source system. "
    )
    column_name: Mapped[str] = mapped_column(
        String(256),
        primary_key=True,
        nullable=False,
        comment="Name of column in Hive. Don't change this manually",
    )
    hive_db: Mapped[str | None] = mapped_column(String(256), comment="Hive Database")
    hive_table: Mapped[str | None] = mapped_column(String(256), comment="Hive Table")
    source_column_name: Mapped[str] = mapped_column(
        String(256),
        nullable=False,
        comment="Name of column in source system. Don't change this manually",
    )
    column_type: Mapped[str] = mapped_column(
        String(2048),
        nullable=False,
        comment="Column type in Hive. Don't change this manually",
    )
    source_column_type: Mapped[str] = mapped_column(
        String(2048),
        nullable=False,
        comment="Column type in source system. Don't change this manually",
    )
    source_database_type: Mapped[str | None] = mapped_column(
        String(256), comment="That database type was the column imported from"
    )
    column_name_override: Mapped[str | None] = mapped_column(
        String(256), comment="Set a custom name of the column in Hive"
    )
    column_type_override: Mapped[str | None] = mapped_column(
        String(256), comment="Set a custom column type in Hive"
    )
    sqoop_column_type: Mapped[str | None] = mapped_column(
        String(256),
        comment="Used to create a correct --map-column-java setting for sqoop. ",
    )
    force_string: Mapped[int] = mapped_column(
        TINYINT(4),
        nullable=False,
        comment="If set to 1, all character based fields (char, varchar) will become string in Hive. Overrides the same setting in import_tables and jdbc_connections table",
        server_default=text("'-1'"),
    )
    include_in_import: Mapped[int] = mapped_column(
        TINYINT(4),
        nullable=False,
        comment="1 = Include column in import, 0 = Exclude column in import",
        server_default=text("'1'"),
    )
    source_primary_key: Mapped[int | None] = mapped_column(
        TINYINT(4),
        comment="Number starting from 1 listing the order of the column in the PK. Don't change this manually",
    )
    last_update_from_source: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, comment="Timestamp of last schema update from source"
    )
    comment: Mapped[str | None] = mapped_column(
        Text, comment="The column comment from the source system"
    )
    operator_notes: Mapped[str | None] = mapped_column(
        Text, comment="Free text field to write a note about the column"
    )
    sqoop_column_type_override: Mapped[str | None] = mapped_column(
        String(256),
        comment="Set the --map-column-java field to a fixed value and not calculated by DBImport",
    )
    anonymization_function: Mapped[str] = mapped_column(
        Enum("None", "Hash", "Replace with star", "Show first 4 chars"),
        nullable=False,
        comment="What anonymization function should be used with the data in this column",
        server_default=text("'None'"),
    )

    table: Mapped["ImportTables"] = relationship("ImportTables")


class ImportForeignKeys(Base):
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

    table_id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        nullable=False,
        comment="Table ID in import_tables that have the FK",
    )
    column_id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        nullable=False,
        comment="Column ID in import_columns that have the FK",
    )
    fk_index: Mapped[int] = mapped_column(
        Integer, primary_key=True, nullable=False, comment="Index of FK"
    )
    fk_table_id: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        comment="Table ID in import_tables that the table is having a reference against",
    )
    fk_column_id: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        comment="Column ID in import_columns that the table is having a reference against",
    )
    key_position: Mapped[int] = mapped_column(
        Integer, nullable=False, comment="Position of the key"
    )

    fk_column: Mapped["ImportColumns"] = relationship(
        "ImportColumns",
        primaryjoin="ImportForeignKeys.fk_column_id == ImportColumns.column_id",
    )
    column: Mapped["ImportColumns"] = relationship(
        "ImportColumns",
        primaryjoin="ImportForeignKeys.column_id == ImportColumns.column_id",
    )


class AtlasColumnCache(Base):
    __tablename__ = "atlas_column_cache"
    __table_args__ = {
        "comment": "Atlas discovery uses this table to cache values in order to detect changes instead of putting a heavy load on the Atlas server."
    }

    hostname: Mapped[str] = mapped_column(
        String(256),
        primary_key=True,
        nullable=False,
        comment="Hostname for the database",
    )
    port: Mapped[str] = mapped_column(
        String(8), primary_key=True, nullable=False, comment="Port for the database"
    )
    database_name: Mapped[str] = mapped_column(
        String(256), primary_key=True, nullable=False, comment="Database name"
    )
    schema_name: Mapped[str] = mapped_column(
        String(256), primary_key=True, nullable=False, comment="Database schema"
    )
    table_name: Mapped[str] = mapped_column(
        String(256), primary_key=True, nullable=False, comment="Database table"
    )
    column_name: Mapped[str] = mapped_column(
        String(256), primary_key=True, nullable=False, comment="Name of the column"
    )
    column_type: Mapped[str | None] = mapped_column(
        String(2048), nullable=True, comment="Type of the column"
    )
    column_length: Mapped[str | None] = mapped_column(
        String(64), nullable=True, comment="Length of the column"
    )
    column_is_nullable: Mapped[str | None] = mapped_column(
        String(16), nullable=True, comment="Is null values allowed in the column"
    )
    column_comment: Mapped[str | None] = mapped_column(
        Text, nullable=True, comment="Comment on the column"
    )
    table_comment: Mapped[str | None] = mapped_column(
        Text, nullable=True, comment="Comment on the table"
    )
    table_type: Mapped[str | None] = mapped_column(
        String(256), nullable=True, comment="Table type. "
    )
    table_create_time: Mapped[datetime | None] = mapped_column(
        DateTime, nullable=True, comment="Timestamp for when the table was created"
    )
    default_value: Mapped[str | None] = mapped_column(
        Text, nullable=True, comment="Default value of the column"
    )


class AtlasKeyCache(Base):
    __tablename__ = "atlas_key_cache"
    __table_args__ = {
        "comment": "Atlas discovery uses this table to cache values in order to detect changes instead of putting a heavy load on the Atlas server."
    }

    hostname: Mapped[str] = mapped_column(
        String(256),
        primary_key=True,
        nullable=False,
        comment="Hostname for the database",
    )
    port: Mapped[str] = mapped_column(
        String(8), primary_key=True, nullable=False, comment="Port for the database"
    )
    database_name: Mapped[str] = mapped_column(
        String(256), primary_key=True, nullable=False, comment="Database name"
    )
    schema_name: Mapped[str] = mapped_column(
        String(256), primary_key=True, nullable=False, comment="Database schema"
    )
    table_name: Mapped[str] = mapped_column(
        String(256), primary_key=True, nullable=False, comment="Database table"
    )
    constraint_name: Mapped[str] = mapped_column(
        String(256), primary_key=True, nullable=False, comment="Name of the constraint"
    )
    constraint_type: Mapped[str | None] = mapped_column(
        String(8), nullable=True, comment="Type of the constraint"
    )
    column_name: Mapped[str] = mapped_column(
        String(256), primary_key=True, nullable=False, comment="Name of the column"
    )
    reference_schema_name: Mapped[str | None] = mapped_column(
        String(256), nullable=True, comment="Name of the schema that is referenced"
    )
    reference_table_name: Mapped[str | None] = mapped_column(
        String(256), nullable=True, comment="Name of the table that is referenced"
    )
    reference_column_name: Mapped[str | None] = mapped_column(
        String(256), nullable=True, comment="Name of the column that is referenced"
    )
    col_key_position: Mapped[int | None] = mapped_column(
        Integer, nullable=True, comment="Position of the key"
    )


class AuthUsers(Base):
    __tablename__ = "auth_users"
    __table_args__ = {"comment": "User database for rest interface and ui"}

    username: Mapped[str] = mapped_column(
        String(256), primary_key=True, nullable=False, comment="Username of the account"
    )
    password: Mapped[str | None] = mapped_column(
        String(256), comment="Password. External accounts are not encrypted"
    )
    disabled: Mapped[int] = mapped_column(
        TINYINT(4),
        nullable=False,
        server_default=text("'0'"),
        comment="0 = Account is enabled. 1 = Account is disabled",
    )
    fullname: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
        server_default=text("''"),
        comment="Users full name. Will be displayed in various menus and logs",
    )
    department: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
        server_default=text("''"),
        comment="Users department. Information only",
    )
    email: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
        server_default=text("''"),
        comment="Users email. Information only",
    )


class ImportTablesIndexes(Base):
    __tablename__ = "import_tables_indexes"
    __table_args__ = (
        UniqueConstraint(
            "hive_db", "hive_table", "index_name", "column", name="hive_db"
        ),
        {
            "comment": "Information table that contains what indexes are available on the source system. The data in the table is not used by DBImport itself, instead it's a help for the users to know what indexes exists to make an effective import."
        },
    )

    table_id: Mapped[int] = mapped_column(
        ForeignKey("import_tables.table_id", ondelete="CASCADE"),
        primary_key=True,
        nullable=False,
        comment="Foreign Key to import_tables column 'table_id'",
    )
    hive_db: Mapped[str | None] = mapped_column(String(256), comment="Hive Database")
    hive_table: Mapped[str | None] = mapped_column(String(256), comment="Hive Table")
    index_name: Mapped[str] = mapped_column(
        String(256), primary_key=True, nullable=False, comment="Name of the index "
    )
    index_type: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
        comment="The type of the index. This is the name from the source database vendor",
    )
    index_unique: Mapped[str] = mapped_column(
        String(50), nullable=False, comment="Does the index have unique values or not?"
    )
    column: Mapped[str] = mapped_column(
        String(256), primary_key=True, nullable=False, comment="Column name"
    )
    column_type: Mapped[str] = mapped_column(
        String(2048), nullable=False, comment="Column type in source system"
    )
    column_order: Mapped[int] = mapped_column(
        Integer, nullable=False, comment="The order of the column in the index."
    )
    column_is_nullable: Mapped[int] = mapped_column(
        TINYINT(4),
        nullable=False,
        comment="1 = Column may contain null values, 0 = Nulls are not allowed in the column",
    )


t_yarn_statistics = Table(
    "yarn_statistics",
    metadata,
    Column(
        "yarn_application_id",
        String(64),
        nullable=False,
        comment="Name of the application in Yarn",
    ),
    Column(
        "operation_type",
        Enum("import", "export"),
        nullable=False,
        comment="What kind of DBImport job it is",
    ),
    Column(
        "operation_tool",
        Enum("hive", "spark", "sqoop"),
        nullable=False,
        comment="The tool or operation that was used",
    ),
    Column(
        "operation_timestamp",
        DateTime,
        nullable=False,
        comment="The timestamp for the complete operation. Can be used for grouping together all applications for a specific operation, i.e a complete import/export",
    ),
    Column("hive_db", String(255), comment="Name of the Hive DB. Used for imports"),
    Column(
        "hive_table", String(255), comment="Name of the Hive Table. Used for imports"
    ),
    Column(
        "dbalias", String(255), comment="Name of the database alias. Used for exports"
    ),
    Column(
        "target_schema",
        String(255),
        comment="Name of the target schema. Used for exports",
    ),
    Column(
        "target_table",
        String(255),
        comment="Name for the target table. Used for exports",
    ),
    Column(
        "application_start",
        DateTime,
        nullable=False,
        comment="Timestamp when the operation started",
    ),
    Column(
        "application_stop",
        DateTime,
        comment="Timestamp when the operation was completed",
    ),
    Column(
        "yarn_containers_total",
        Integer,
        comment="Number of containers used. For spark and sqoop, this will be the maximum that was used during the operation",
    ),
    Column(
        "yarn_containers_failed", Integer, comment="Number of containers that failed"
    ),
    Column(
        "yarn_containers_killed",
        Integer,
        comment="Number of containers that was killed",
    ),
    Index(
        "hive_db_hive_table_last_update_from_source",
        "hive_db",
        "hive_table",
        "operation_timestamp",
    ),
    Index(
        "dbalias_target_schema_target_table_last_update_from_source",
        "dbalias",
        "target_schema",
        "target_table",
        "operation_timestamp",
    ),
    Index("yarn_application_id", "yarn_application_id"),
    comment="Statistics about Yarn applications that DBImport launches. Used for rough resource estimations used and connection between Yarn application ID and DBImport job",
)
