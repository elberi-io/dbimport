"""fix spelling in table and column comments

Revision ID: 6b6d05af061f
Revises: a5899e8b06a9
Create Date: 2026-11-17 15:12:11.815951

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import mysql

revision: str = "6b6d05af061f"
down_revision: str | Sequence[str] | None = "a5899e8b06a9"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.alter_column(
        "airflow_custom_dags",
        "retries",
        existing_type=mysql.INTEGER(display_width=11),
        comment="How many retries should be Task do in Airflow before it fails",
        existing_comment="How many retries should be Task do in Airflow before it failes",
        existing_nullable=False,
        existing_server_default=sa.text("0"),
    )
    op.alter_column(
        "airflow_custom_dags",
        "application_notes",
        existing_type=mysql.TEXT(),
        comment="Free text field that can be used for application documentation, notes or links. ",
        existing_comment="Free text field that can be used for application documentaton, notes or links. ",
        existing_nullable=True,
    )
    op.alter_column(
        "airflow_custom_dags",
        "tags",
        existing_type=mysql.VARCHAR(length=256),
        comment="Comma separated list of Airflow tags that will be set on the Dag",
        existing_comment="Comma seperated list of Airflow tags that will be set on the Dag",
        existing_nullable=True,
    )
    op.create_table_comment(
        "airflow_custom_dags",
        'Its possible to construct a DAG that have no import, export or ETL definitions in it, but instead just Tasks from the airflow_task table. That might be useful to for example run custom Hive Code after an import is completed as a separate DAG. Defining a DAG in here also requires you to have at least one task in airflow_tasks defined "in main"',
        existing_comment='Its possible to construct a DAG that have no import, export or ETL definitions in it, but instead just Tasks from the airflow_task table. That might nbe useful to for example run custom Hive Code after an import is completed as a separate DAG. Defining a DAG in here also requires you to have at least one task in airflow_tasks defined "in main"',
        schema=None,
    )
    op.alter_column(
        "airflow_etl_dags",
        "retries",
        existing_type=mysql.TINYINT(display_width=4),
        comment="How many retries should be Task do in Airflow before it fails",
        existing_comment="How many retries should be Task do in Airflow before it failes",
        existing_nullable=True,
    )
    op.alter_column(
        "airflow_etl_dags",
        "application_notes",
        existing_type=mysql.TEXT(),
        comment="Free text field that can be used for application documentation, notes or links. ",
        existing_comment="Free text field that can be used for application documentaton, notes or links. ",
        existing_nullable=True,
    )
    op.alter_column(
        "airflow_etl_dags",
        "tags",
        existing_type=mysql.VARCHAR(length=256),
        comment="Comma separated list of Airflow tags that will be set on the Dag",
        existing_comment="Comma seperated list of Airflow tags that will be set on the Dag",
        existing_nullable=True,
    )
    op.alter_column(
        "airflow_export_dags",
        "retries",
        existing_type=mysql.TINYINT(display_width=4),
        comment="How many retries should be Task do in Airflow before it fails",
        existing_comment="How many retries should be Task do in Airflow before it failes",
        existing_nullable=True,
    )
    op.alter_column(
        "airflow_export_dags",
        "trigger_dag_on_success",
        existing_type=mysql.VARCHAR(length=64),
        comment="Name of DAG to trigger if export is successful. Comma separated list of DAGs (optional)",
        existing_comment="Name of DAG to trigger if export is successfull. Comma seperated list of DAGs (optional)",
        existing_nullable=True,
    )
    op.alter_column(
        "airflow_export_dags",
        "application_notes",
        existing_type=mysql.TEXT(),
        comment="Free text field that can be used for application documentation, notes or links. ",
        existing_comment="Free text field that can be used for application documentaton, notes or links. ",
        existing_nullable=True,
    )
    op.alter_column(
        "airflow_export_dags",
        "tags",
        existing_type=mysql.VARCHAR(length=256),
        comment="Comma separated list of Airflow tags that will be set on the Dag",
        existing_comment="Comma seperated list of Airflow tags that will be set on the Dag",
        existing_nullable=True,
    )
    op.alter_column(
        "airflow_import_dags",
        "finish_all_stage1_first",
        existing_type=mysql.TINYINT(display_width=4),
        comment="1 = All Import phase jobs will be completed first, and when all is successful, the ETL phase start",
        existing_comment="1 = All Import phase jobs will be completed first, and when all is successfull, the ETL phase start",
        existing_nullable=False,
        existing_server_default=sa.text("0"),
    )
    op.alter_column(
        "airflow_import_dags",
        "retries",
        existing_type=mysql.TINYINT(display_width=4),
        comment="How many retries should be Task do in Airflow before it fails",
        existing_comment="How many retries should be Task do in Airflow before it failes",
        existing_nullable=False,
        existing_server_default=sa.text("5"),
    )
    op.alter_column(
        "airflow_import_dags",
        "application_notes",
        existing_type=mysql.TEXT(),
        comment="Free text field that can be used for application documentation, notes or links. ",
        existing_comment="Free text field that can be used for application documentaton, notes or links. ",
        existing_nullable=True,
    )
    op.alter_column(
        "airflow_import_dags",
        "tags",
        existing_type=mysql.VARCHAR(length=256),
        comment="Comma separated list of Airflow tags that will be set on the Dag",
        existing_comment="Comma seperated list of Airflow tags that will be set on the Dag",
        existing_nullable=True,
    )
    op.alter_column(
        "airflow_tasks",
        "task_dependency_downstream",
        existing_type=mysql.VARCHAR(length=256),
        comment="Defines the downstream dependency for the Task. Comma separated list",
        existing_comment="Defines the upstream dependency for the Task. Comma separated list",
        existing_nullable=True,
    )
    op.alter_column(
        "configuration",
        "configKey",
        existing_type=mysql.VARCHAR(length=32),
        comment="Name of the configuration item. These are controlled and maintained by the setup tool. Don't change these manually",
        existing_comment="Name of the configuration item. These are controlled and maintained by thee setup tool. Dont change these manually",
        existing_nullable=False,
    )
    op.alter_column(
        "dbimport_instances",
        "sync_credentials",
        existing_type=mysql.TINYINT(display_width=4),
        comment="0 = Credentials won't be synced, 1 = The credentials information will be synced to the other cluster",
        existing_comment="0 = Credentials wont be synced, 1 = The credentials information will be synced to the other cluster",
        existing_nullable=False,
        existing_server_default=sa.text("0"),
    )
    op.alter_column(
        "export_columns",
        "column_name",
        existing_type=mysql.VARCHAR(length=256),
        comment="Name of column in target table. Don't change this manually",
        existing_comment="Name of column in target table. Dont change this manually",
        existing_nullable=False,
    )
    op.alter_column(
        "export_columns",
        "column_type",
        existing_type=mysql.VARCHAR(length=2048),
        comment="Column type from Hive. Don't change this manually",
        existing_comment="Column type from Hive. Dont change this manually",
        existing_nullable=True,
    )
    op.alter_column(
        "export_columns",
        "column_order",
        existing_type=mysql.INTEGER(display_width=11),
        comment="The order of the columns. Don't change this manually",
        existing_comment="The order of the columns. Dont change this manually",
        existing_nullable=True,
    )
    op.alter_column(
        "export_columns",
        "last_update_from_hive",
        existing_type=mysql.DATETIME(),
        comment="Timestamp of last schema update from Hive. Don't change this manually",
        existing_comment="Timestamp of last schema update from Hive. Dont change this manually",
        existing_nullable=False,
    )
    op.alter_column(
        "export_columns",
        "last_export_time",
        existing_type=mysql.DATETIME(),
        comment="Timestamp of last export. Don't change this manually",
        existing_comment="Timestamp of last export. Dont change this manually",
        existing_nullable=True,
    )
    op.alter_column(
        "export_columns",
        "comment",
        existing_type=mysql.TEXT(),
        comment="The column comment from the source system. Don't change this manually",
        existing_comment="The column comment from the source system. Dont change this manually",
        existing_nullable=True,
    )
    op.create_table_comment(
        "export_stage",
        'The export tool keeps track of how far in the export the tool have succeeded. So in case of an error, lets say that Hive is not responding, the next time an export is executed it will skip the first part and continue from where it ended in error on the previous run. If you want to rerun from the beginning, the information in this table needs to be cleared. This is done with the "manage --clearExportStage" tool. Keep in mind that clearing the stage of an incremental export might result in the loss of the data.',
        existing_comment='The export tool keeps track of how far in the export the tool have succeeded. So in case of an error, lets say that Hive is not responding, the next time an export is executed it will skip the first part and continue from where it ended in error on the previous run. If you want to rerun from the begining, the information in this table needs to be cleared. This is done with the "manage --clearExportStage" tool. Keep in mind that clearing the stage of an incremental export might result in the loss of the data.',
        schema=None,
    )
    op.alter_column(
        "export_statistics",
        "sessions",
        existing_type=mysql.TINYINT(display_width=4),
        comment="How many parallel sessions was used against the source (sqoop mappers)",
        existing_comment="How many parallell sessions was used against the source (sqoop mappers)",
        existing_nullable=True,
    )
    op.alter_column(
        "export_statistics",
        "duration",
        existing_type=mysql.INTEGER(display_width=11),
        comment="Total duration in seconds",
        existing_comment="Tota duration in seconds",
        existing_nullable=True,
    )
    op.create_table_comment(
        "export_statistics",
        "At the end of each export, all statistics about how long each part took as well as general information about Hive database and table, number of rows imported, size of the export and much more are logged in this table. This table grows and will never be truncated by DBImport itself. If it becomes to large for you, it's up to each user to delete or truncate this table as you see fit.",
        existing_comment="At the end of each export, all statistics about how long each part took aswell as general information about Hive database and table, number of rows imported, size of the export and much more are logged in this table. This table grows and will never be truncated by DBImport itself. If it becomes to large for you, it's up to each user to delete or truncate this table as you see fit.",
        schema=None,
    )
    op.alter_column(
        "export_statistics_last",
        "sessions",
        existing_type=mysql.TINYINT(display_width=4),
        comment="How many parallel sessions was used against the source (sqoop mappers)",
        existing_comment="How many parallell sessions was used against the source (sqoop mappers)",
        existing_nullable=True,
    )
    op.alter_column(
        "export_statistics_last",
        "duration",
        existing_type=mysql.INTEGER(display_width=11),
        comment="Total duration in seconds",
        existing_comment="Tota duration in seconds",
        existing_nullable=True,
    )
    op.create_table_comment(
        "export_statistics_last",
        "The last entry in table export_statistics is also stored in this table. This makes it easier to find the latest data without first grouping to find the latest entry. When export_statistics table grows to a high number of million rows, it saves a lot of cpu power for the database server.",
        existing_comment="The last entry in table export_statistics is also stored in this table. This makes it easier to find the latest data without first grouping to find the latest entry. When export_statistics table grows to a high number of million rows, it saves alot of cpu power for the database server.",
        schema=None,
    )
    op.alter_column(
        "export_tables",
        "hive_rowcount",
        existing_type=mysql.BIGINT(display_width=20),
        comment="Number of rows in Hive table. Don't change manually",
        existing_comment="Number of rows in Hive table. Dont change manually",
        existing_nullable=True,
    )
    op.alter_column(
        "export_tables",
        "target_rowcount",
        existing_type=mysql.BIGINT(display_width=20),
        comment="Number of rows in Target table. Don't change manually",
        existing_comment="Number of rows in Target table. Dont change manually",
        existing_nullable=True,
    )
    op.alter_column(
        "export_tables",
        "validationCustomQueryHiveValue",
        existing_type=mysql.TEXT(),
        comment="Used for validation. Don't change manually",
        existing_comment="Used for validation. Dont change manually",
        existing_nullable=True,
    )
    op.alter_column(
        "export_tables",
        "validationCustomQueryTargetValue",
        existing_type=mysql.TEXT(),
        comment="Used for validation. Don't change manually",
        existing_comment="Used for validation. Dont change manually",
        existing_nullable=True,
    )
    op.alter_column(
        "export_tables",
        "incr_minvalue",
        existing_type=mysql.VARCHAR(length=32),
        comment="Used by incremental exports to keep track of progress. Don't change manually",
        existing_comment="Used by incremental exports to keep track of progress. Dont change manually",
        existing_nullable=True,
    )
    op.alter_column(
        "export_tables",
        "incr_maxvalue",
        existing_type=mysql.VARCHAR(length=32),
        comment="Used by incremental exports to keep track of progress. Don't change manually",
        existing_comment="Used by incremental exports to keep track of progress. Dont change manually",
        existing_nullable=True,
    )
    op.alter_column(
        "export_tables",
        "incr_minvalue_pending",
        existing_type=mysql.VARCHAR(length=32),
        comment="Used by incremental exports to keep track of progress. Don't change manually",
        existing_comment="Used by incremental exports to keep track of progress. Dont change manually",
        existing_nullable=True,
    )
    op.alter_column(
        "export_tables",
        "incr_maxvalue_pending",
        existing_type=mysql.VARCHAR(length=32),
        comment="Used by incremental exports to keep track of progress. Don't change manually",
        existing_comment="Used by incremental exports to keep track of progress. Dont change manually",
        existing_nullable=True,
    )
    op.alter_column(
        "export_tables",
        "sqoop_last_size",
        existing_type=mysql.BIGINT(display_width=20),
        comment="Used to track sqoop operation. Don't change manually",
        existing_comment="Used to track sqoop operation. Dont change manually",
        existing_nullable=True,
    )
    op.alter_column(
        "export_tables",
        "sqoop_last_rows",
        existing_type=mysql.BIGINT(display_width=20),
        comment="Used to track sqoop operation. Don't change manually",
        existing_comment="Used to track sqoop operation. Dont change manually",
        existing_nullable=True,
    )
    op.alter_column(
        "export_tables",
        "sqoop_last_mappers",
        existing_type=mysql.TINYINT(display_width=4),
        comment="Used to track sqoop operation. Don't change manually",
        existing_comment="Used to track sqoop operation. Dont change manually",
        existing_nullable=True,
    )
    op.alter_column(
        "export_tables",
        "sqoop_last_execution",
        existing_type=mysql.BIGINT(display_width=20),
        comment="Used to track sqoop operation. Don't change manually",
        existing_comment="Used to track sqoop operation. Dont change manually",
        existing_nullable=True,
    )
    op.alter_column(
        "export_tables",
        "create_target_table_sql",
        existing_type=mysql.TEXT(),
        comment="SQL statement that was used to create the target table. Don't change manually",
        existing_comment="SQL statement that was used to create the target table. Dont change manually",
        existing_nullable=True,
    )
    op.alter_column(
        "import_columns",
        "column_name",
        existing_type=mysql.VARCHAR(length=256),
        comment="Name of column in Hive. Don't change this manually",
        existing_comment="Name of column in Hive. Dont change this manually",
        existing_nullable=False,
    )
    op.alter_column(
        "import_columns",
        "source_column_name",
        existing_type=mysql.VARCHAR(length=256),
        comment="Name of column in source system. Don't change this manually",
        existing_comment="Name of column in source system. Dont change this manually",
        existing_nullable=False,
    )
    op.alter_column(
        "import_columns",
        "column_type",
        existing_type=mysql.VARCHAR(length=2048),
        comment="Column type in Hive. Don't change this manually",
        existing_comment="Column type in Hive. Dont change this manually",
        existing_nullable=False,
    )
    op.alter_column(
        "import_columns",
        "source_column_type",
        existing_type=mysql.VARCHAR(length=2048),
        comment="Column type in source system. Don't change this manually",
        existing_comment="Column type in source system. Dont change this manually",
        existing_nullable=False,
    )
    op.alter_column(
        "import_columns",
        "source_primary_key",
        existing_type=mysql.TINYINT(display_width=4),
        comment="Number starting from 1 listing the order of the column in the PK. Don't change this manually",
        existing_comment="Number starting from 1 listing the order of the column in the PK. Dont change this manually",
        existing_nullable=True,
    )
    op.create_table_comment(
        "import_failure_log",
        "If there is an error or a warning during import, but the import still continues, these errors are logged in this table. An example could be that  a column can't be altered, foreign key not created, no new columns can be added and such.",
        existing_comment="If there is an error or a warning during import, bu the import still continues, these errors are logged in this table. An example could be that  a column cant be altered, foreign key not created, no new columns can be added and such.",
        schema=None,
    )
    op.create_table_comment(
        "import_stage",
        'The import tool keeps track of how far in the import the tool have succeeded. So in case of an error, lets say that Hive is not responding, the next time an import is executed it will skip the first part and continue from where it ended in error on the previous run. If you want to rerun from the beginning, the information in this table needs to be cleared. This is done with the "manage --clearImportStage" tool. Keep in mind that clearing the stage of an incremental import might result in the loss of the data.',
        existing_comment='The import tool keeps track of how far in the import the tool have succeeded. So in case of an error, lets say that Hive is not responding, the next time an import is executed it will skip the first part and continue from where it ended in error on the previous run. If you want to rerun from the begining, the information in this table needs to be cleared. This is done with the "manage --clearImportStage" tool. Keep in mind that clearing the stage of an incremental import might result in the loss of the data.',
        schema=None,
    )
    op.alter_column(
        "import_statistics",
        "sessions",
        existing_type=mysql.TINYINT(display_width=4),
        comment="How many parallel sessions was used against the source (sqoop mappers)",
        existing_comment="How many parallell sessions was used against the source (sqoop mappers)",
        existing_nullable=True,
    )
    op.alter_column(
        "import_statistics",
        "duration",
        existing_type=mysql.INTEGER(display_width=11),
        comment="Total duration in seconds",
        existing_comment="Tota duration in seconds",
        existing_nullable=True,
    )
    op.create_table_comment(
        "import_statistics",
        "At the end of each import, all statistics about how long each part took as well as general information about Hive database and table, number of rows imported, size of the import and much more are logged in this table. This table grows and will never be truncated by DBImport itself. If it becomes to large for you, it's up to each user to delete or truncate this table as you see fit.",
        existing_comment="At the end of each import, all statistics about how long each part took aswell as general information about Hive datbase and table, number of rows imported, size of the import and much more are logged in this table. This table grows and will never be truncated by DBImport itself. If it becomes to large for you, it's up to each user to delete or truncate this table as you see fit.",
        schema=None,
    )
    op.alter_column(
        "import_statistics_last",
        "sessions",
        existing_type=mysql.TINYINT(display_width=4),
        comment="How many parallel sessions was used against the source (sqoop mappers)",
        existing_comment="How many parallell sessions was used against the source (sqoop mappers)",
        existing_nullable=True,
    )
    op.alter_column(
        "import_statistics_last",
        "duration",
        existing_type=mysql.INTEGER(display_width=11),
        comment="Total duration in seconds",
        existing_comment="Tota duration in seconds",
        existing_nullable=True,
    )
    op.create_table_comment(
        "import_statistics_last",
        "The last entry in table import_statistics is also stored in this table. This makes it easier to find the latest data without first grouping to find the latest entry. When import_statistics table grows to a high number of million rows, it saves a lot of cpu power for the database server.",
        existing_comment="The last entry in table import_statistics is also stored in this table. This makes it easier to find the latest data without first grouping to find the latest entry. When import_statistics table grows to a high number of million rows, it saves alot of cpu power for the database server.",
        schema=None,
    )
    op.alter_column(
        "import_tables",
        "nomerge_ingestion_sql_addition",
        existing_type=mysql.VARCHAR(length=2048),
        comment="This will be added to the data ingestion of None-Merge imports (full, full_direct and incr). Useful to filter out data from import tables to target tables",
        existing_comment="This will be added to the data ingestion of None-Merge imports (full, full_direct and incr). Usefull to filter out data from import tables to target tables",
        existing_nullable=True,
    )
    op.alter_column(
        "import_tables",
        "validate_diff_allowed",
        existing_type=mysql.BIGINT(display_width=20),
        comment="-1 = auto calculated diff allowed. If a positive number, this is the amount of rows that the diff is allowed to have",
        existing_comment="-1 = auto calculated diff allowed. If a positiv number, this is the amount of rows that the diff is allowed to have",
        existing_nullable=False,
        existing_server_default=sa.text("-1"),
    )
    op.alter_column(
        "import_tables",
        "validationCustomQueryValidateImportTable",
        existing_type=mysql.TINYINT(display_width=4),
        comment="1 = Validate Import table, 0 = Don't validate Import table",
        existing_comment="1 = Validate Import table, 0 = Dont validate Import table",
        existing_nullable=False,
        existing_server_default=sa.text("1"),
    )
    op.alter_column(
        "import_tables",
        "mappers",
        existing_type=mysql.TINYINT(display_width=4),
        comment="-1 = auto or positive number for a fixed number of mappers. If Auto, then it's calculated based of last sqoop import size",
        existing_comment="-1 = auto or positiv number for a fixed number of mappers. If Auto, then it's calculated based of last sqoop import size",
        existing_nullable=False,
        existing_server_default=sa.text("-1"),
    )
    op.alter_column(
        "import_tables",
        "source_rowcount",
        existing_type=mysql.BIGINT(display_width=20),
        comment="Used for validation. Don't change manually",
        existing_comment="Used for validation. Dont change manually",
        existing_nullable=True,
    )
    op.alter_column(
        "import_tables",
        "hive_rowcount",
        existing_type=mysql.BIGINT(display_width=20),
        comment="Used for validation. Don't change manually",
        existing_comment="Used for validation. Dont change manually",
        existing_nullable=True,
    )
    op.alter_column(
        "import_tables",
        "validationCustomQuerySourceValue",
        existing_type=mysql.TEXT(),
        comment="Used for validation. Don't change manually",
        existing_comment="Used for validation. Dont change manually",
        existing_nullable=True,
    )
    op.alter_column(
        "import_tables",
        "validationCustomQueryHiveValue",
        existing_type=mysql.TEXT(),
        comment="Used for validation. Don't change manually",
        existing_comment="Used for validation. Dont change manually",
        existing_nullable=True,
    )
    op.alter_column(
        "import_tables",
        "incr_minvalue",
        existing_type=mysql.VARCHAR(length=32),
        comment="Used for incremental imports. Don't change manually",
        existing_comment="Used for incremental imports. Dont change manually",
        existing_nullable=True,
    )
    op.alter_column(
        "import_tables",
        "incr_maxvalue",
        existing_type=mysql.VARCHAR(length=32),
        comment="Used for incremental imports. Don't change manually",
        existing_comment="Used for incremental imports. Dont change manually",
        existing_nullable=True,
    )
    op.alter_column(
        "import_tables",
        "incr_minvalue_pending",
        existing_type=mysql.VARCHAR(length=32),
        comment="Used for incremental imports. Don't change manually",
        existing_comment="Used for incremental imports. Dont change manually",
        existing_nullable=True,
    )
    op.alter_column(
        "import_tables",
        "incr_maxvalue_pending",
        existing_type=mysql.VARCHAR(length=32),
        comment="Used for incremental imports. Don't change manually",
        existing_comment="Used for incremental imports. Dont change manually",
        existing_nullable=True,
    )
    op.alter_column(
        "import_tables",
        "pk_column_override",
        existing_type=mysql.VARCHAR(length=1024),
        comment="Force the import and Hive table to define another PrimaryKey constraint. Comma separated list of columns",
        existing_comment="Force the import and Hive table to define another PrimaryKey constraint. Comma separeted list of columns",
        existing_nullable=True,
    )
    op.alter_column(
        "import_tables",
        "pk_column_override_mergeonly",
        existing_type=mysql.VARCHAR(length=1024),
        comment="Force the import to use another PrimaryKey constraint during Merge operations. Comma separated list of columns",
        existing_comment="Force the import to use another PrimaryKey constraint during Merge operations. Comma separeted list of columns",
        existing_nullable=True,
    )
    op.alter_column(
        "import_tables",
        "sqoop_last_size",
        existing_type=mysql.BIGINT(display_width=20),
        comment="Used to track sqoop operation. Don't change manually",
        existing_comment="Used to track sqoop operation. Dont change manually",
        existing_nullable=True,
    )
    op.alter_column(
        "import_tables",
        "sqoop_last_rows",
        existing_type=mysql.BIGINT(display_width=20),
        comment="Used to track sqoop operation. Don't change manually",
        existing_comment="Used to track sqoop operation. Dont change manually",
        existing_nullable=True,
    )
    op.alter_column(
        "import_tables",
        "sqoop_last_mappers",
        existing_type=mysql.TINYINT(display_width=4),
        comment="Used to track sqoop operation. Don't change manually",
        existing_comment="Used to track sqoop operation. Dont change manually",
        existing_nullable=True,
    )
    op.alter_column(
        "import_tables",
        "sqoop_last_execution",
        existing_type=mysql.BIGINT(display_width=20),
        comment="Used to track sqoop operation. Don't change manually",
        existing_comment="Used to track sqoop operation. Dont change manually",
        existing_nullable=True,
    )
    op.alter_column(
        "import_tables",
        "comment",
        existing_type=mysql.TEXT(),
        comment="Table comment from source system. Don't change manually",
        existing_comment="Table comment from source system. Dont change manually",
        existing_nullable=True,
    )
    op.alter_column(
        "import_tables",
        "generated_hive_column_definition",
        existing_type=mysql.TEXT(),
        comment="Generated column definition for Hive create table. Don't change manually",
        existing_comment="Generated column definition for Hive create table. Dont change manually",
        existing_nullable=True,
    )
    op.alter_column(
        "import_tables",
        "generated_sqoop_query",
        existing_type=mysql.TEXT(),
        comment="Generated query for sqoop. Don't change manually",
        existing_comment="Generated query for sqoop. Dont change manually",
        existing_nullable=True,
    )
    op.alter_column(
        "import_tables",
        "generated_sqoop_options",
        existing_type=mysql.TEXT(),
        comment="Generated options for sqoop. Don't change manually",
        existing_comment="Generated options for sqoop. Dont change manually",
        existing_nullable=True,
    )
    op.alter_column(
        "import_tables",
        "generated_pk_columns",
        existing_type=mysql.TEXT(),
        comment="Generated Primary Keys. Don't change manually",
        existing_comment="Generated Primary Keys. Dont change manually",
        existing_nullable=True,
    )
    op.alter_column(
        "import_tables",
        "copy_finished",
        existing_type=mysql.DATETIME(),
        comment="Time when last copy from Master DBImport instance was completed. Don't change manually",
        existing_comment="Time when last copy from Master DBImport instance was completed. Dont change manually",
        existing_nullable=True,
    )
    op.alter_column(
        "import_tables",
        "copy_slave",
        existing_type=mysql.TINYINT(display_width=4),
        comment="Defines if this table is a Master table or a Slave table. Don't change manually",
        existing_comment="Defines if this table is a Master table or a Slave table. Dont change manually",
        existing_nullable=False,
        existing_server_default=sa.text("0"),
    )
    op.alter_column(
        "import_tables",
        "spark_executor_memory",
        existing_type=mysql.VARCHAR(length=8),
        comment="Memory used by spark when importing data. Overrides default value in global configuration",
        existing_comment="Memory used by spark when importring data. Overrides default value in global configuration",
        existing_nullable=True,
    )
    op.alter_column(
        "import_tables",
        "custom_max_query",
        existing_type=mysql.VARCHAR(length=256),
        comment="You can use a custom SQL query that will get the Max value from the source database. This Max value will be used in an incremental import to know how much to read in each execution",
        existing_comment="You can use a custom SQL query that will get the Max value from the source database. This Max value will be used in an inremental import to know how much to read in each execution",
        existing_nullable=True,
    )
    op.create_table_comment(
        "import_tables_indexes",
        "Information table that contains what indexes are available on the source system. The data in the table is not used by DBImport itself, instead it's a help for the users to know what indexes exists to make an effective import.",
        existing_comment="Information table that contains what indexes are available on the source system. The data in the table is not used by DBImport itsealf, instead it's a help for the users to know what indexes exists to make an effective import.",
        schema=None,
    )
    op.alter_column(
        "jdbc_connections",
        "create_foreign_keys",
        existing_type=mysql.TINYINT(display_width=4),
        comment="1 = Create foreign keys, 0 = Don't create foreign keys",
        existing_comment="1 = Create foreign keys, 0 = Dont create foreign keys",
        existing_nullable=False,
        existing_server_default=sa.text("1"),
    )
    op.alter_column(
        "jdbc_connections",
        "atlas_discovery",
        existing_type=mysql.TINYINT(display_width=4),
        comment="1 = Discover tables/views on this connection, 0 = Don't use Atlas discovery on this connection",
        existing_comment="1 = Discover tables/views on this connection, 0 = Dont use Atlas discovery on this connection",
        existing_nullable=False,
        existing_server_default=sa.text("1"),
    )
    op.create_table_comment(
        "jdbc_table_change_history",
        "This table keeps track of all changes that was done to a JDBC table after the initial creation. Example could be that a column type was changed from char(10) to varchar(10). That kind of information is logged in this table",
        existing_comment="This table keeps track of all changes that was done to a JDBC table after the initial creation. Example could be that a colum type was changed from char(10) to varchar(10). That kind of information is logged in this table",
        schema=None,
    )
    op.create_table_comment(
        "table_change_history",
        "This table keeps track of all changes that was done to an object after the initial load. Example could be that a column type was changed from char(10) to varchar(10). That kind of information is logged in this table",
        existing_comment="This table keeps track of all changes that was done to an abject after the initial load. Example could be that a colum type was changed from char(10) to varchar(10). That kind of information is logged in this table",
        schema=None,
    )
    op.alter_column(
        "yarn_statistics",
        "application_stop",
        existing_type=mysql.DATETIME(),
        comment="Timestamp when the operation was completed",
        existing_comment="Timestampe when the operation was completed",
        existing_nullable=True,
    )
    op.alter_column(
        "yarn_statistics",
        "yarn_containers_failed",
        existing_type=mysql.INTEGER(display_width=11),
        comment="Number of containers that failed",
        existing_comment="Number of contaners that failed",
        existing_nullable=True,
    )
    op.alter_column(
        "yarn_statistics",
        "yarn_containers_killed",
        existing_type=mysql.INTEGER(display_width=11),
        comment="Number of containers that was killed",
        existing_comment="Number of contaners that was killed",
        existing_nullable=True,
    )


def downgrade() -> None:
    op.alter_column(
        "yarn_statistics",
        "yarn_containers_killed",
        existing_type=mysql.INTEGER(display_width=11),
        comment="Number of contaners that was killed",
        existing_comment="Number of containers that was killed",
        existing_nullable=True,
    )
    op.alter_column(
        "yarn_statistics",
        "yarn_containers_failed",
        existing_type=mysql.INTEGER(display_width=11),
        comment="Number of contaners that failed",
        existing_comment="Number of containers that failed",
        existing_nullable=True,
    )
    op.alter_column(
        "yarn_statistics",
        "application_stop",
        existing_type=mysql.DATETIME(),
        comment="Timestampe when the operation was completed",
        existing_comment="Timestamp when the operation was completed",
        existing_nullable=True,
    )
    op.create_table_comment(
        "table_change_history",
        "This table keeps track of all changes that was done to an abject after the initial load. Example could be that a colum type was changed from char(10) to varchar(10). That kind of information is logged in this table",
        existing_comment="This table keeps track of all changes that was done to an object after the initial load. Example could be that a column type was changed from char(10) to varchar(10). That kind of information is logged in this table",
        schema=None,
    )
    op.create_table_comment(
        "jdbc_table_change_history",
        "This table keeps track of all changes that was done to a JDBC table after the initial creation. Example could be that a colum type was changed from char(10) to varchar(10). That kind of information is logged in this table",
        existing_comment="This table keeps track of all changes that was done to a JDBC table after the initial creation. Example could be that a column type was changed from char(10) to varchar(10). That kind of information is logged in this table",
        schema=None,
    )
    op.alter_column(
        "jdbc_connections",
        "atlas_discovery",
        existing_type=mysql.TINYINT(display_width=4),
        comment="1 = Discover tables/views on this connection, 0 = Dont use Atlas discovery on this connection",
        existing_comment="1 = Discover tables/views on this connection, 0 = Don't use Atlas discovery on this connection",
        existing_nullable=False,
        existing_server_default=sa.text("1"),
    )
    op.alter_column(
        "jdbc_connections",
        "create_foreign_keys",
        existing_type=mysql.TINYINT(display_width=4),
        comment="1 = Create foreign keys, 0 = Dont create foreign keys",
        existing_comment="1 = Create foreign keys, 0 = Don't create foreign keys",
        existing_nullable=False,
        existing_server_default=sa.text("1"),
    )
    op.create_table_comment(
        "import_tables_indexes",
        "Information table that contains what indexes are available on the source system. The data in the table is not used by DBImport itsealf, instead it's a help for the users to know what indexes exists to make an effective import.",
        existing_comment="Information table that contains what indexes are available on the source system. The data in the table is not used by DBImport itself, instead it's a help for the users to know what indexes exists to make an effective import.",
        schema=None,
    )
    op.alter_column(
        "import_tables",
        "custom_max_query",
        existing_type=mysql.VARCHAR(length=256),
        comment="You can use a custom SQL query that will get the Max value from the source database. This Max value will be used in an inremental import to know how much to read in each execution",
        existing_comment="You can use a custom SQL query that will get the Max value from the source database. This Max value will be used in an incremental import to know how much to read in each execution",
        existing_nullable=True,
    )
    op.alter_column(
        "import_tables",
        "spark_executor_memory",
        existing_type=mysql.VARCHAR(length=8),
        comment="Memory used by spark when importring data. Overrides default value in global configuration",
        existing_comment="Memory used by spark when importing data. Overrides default value in global configuration",
        existing_nullable=True,
    )
    op.alter_column(
        "import_tables",
        "copy_slave",
        existing_type=mysql.TINYINT(display_width=4),
        comment="Defines if this table is a Master table or a Slave table. Dont change manually",
        existing_comment="Defines if this table is a Master table or a Slave table. Don't change manually",
        existing_nullable=False,
        existing_server_default=sa.text("0"),
    )
    op.alter_column(
        "import_tables",
        "copy_finished",
        existing_type=mysql.DATETIME(),
        comment="Time when last copy from Master DBImport instance was completed. Dont change manually",
        existing_comment="Time when last copy from Master DBImport instance was completed. Don't change manually",
        existing_nullable=True,
    )
    op.alter_column(
        "import_tables",
        "generated_pk_columns",
        existing_type=mysql.TEXT(),
        comment="Generated Primary Keys. Dont change manually",
        existing_comment="Generated Primary Keys. Don't change manually",
        existing_nullable=True,
    )
    op.alter_column(
        "import_tables",
        "generated_sqoop_options",
        existing_type=mysql.TEXT(),
        comment="Generated options for sqoop. Dont change manually",
        existing_comment="Generated options for sqoop. Don't change manually",
        existing_nullable=True,
    )
    op.alter_column(
        "import_tables",
        "generated_sqoop_query",
        existing_type=mysql.TEXT(),
        comment="Generated query for sqoop. Dont change manually",
        existing_comment="Generated query for sqoop. Don't change manually",
        existing_nullable=True,
    )
    op.alter_column(
        "import_tables",
        "generated_hive_column_definition",
        existing_type=mysql.TEXT(),
        comment="Generated column definition for Hive create table. Dont change manually",
        existing_comment="Generated column definition for Hive create table. Don't change manually",
        existing_nullable=True,
    )
    op.alter_column(
        "import_tables",
        "comment",
        existing_type=mysql.TEXT(),
        comment="Table comment from source system. Dont change manually",
        existing_comment="Table comment from source system. Don't change manually",
        existing_nullable=True,
    )
    op.alter_column(
        "import_tables",
        "sqoop_last_execution",
        existing_type=mysql.BIGINT(display_width=20),
        comment="Used to track sqoop operation. Dont change manually",
        existing_comment="Used to track sqoop operation. Don't change manually",
        existing_nullable=True,
    )
    op.alter_column(
        "import_tables",
        "sqoop_last_mappers",
        existing_type=mysql.TINYINT(display_width=4),
        comment="Used to track sqoop operation. Dont change manually",
        existing_comment="Used to track sqoop operation. Don't change manually",
        existing_nullable=True,
    )
    op.alter_column(
        "import_tables",
        "sqoop_last_rows",
        existing_type=mysql.BIGINT(display_width=20),
        comment="Used to track sqoop operation. Dont change manually",
        existing_comment="Used to track sqoop operation. Don't change manually",
        existing_nullable=True,
    )
    op.alter_column(
        "import_tables",
        "sqoop_last_size",
        existing_type=mysql.BIGINT(display_width=20),
        comment="Used to track sqoop operation. Dont change manually",
        existing_comment="Used to track sqoop operation. Don't change manually",
        existing_nullable=True,
    )
    op.alter_column(
        "import_tables",
        "pk_column_override_mergeonly",
        existing_type=mysql.VARCHAR(length=1024),
        comment="Force the import to use another PrimaryKey constraint during Merge operations. Comma separeted list of columns",
        existing_comment="Force the import to use another PrimaryKey constraint during Merge operations. Comma separated list of columns",
        existing_nullable=True,
    )
    op.alter_column(
        "import_tables",
        "pk_column_override",
        existing_type=mysql.VARCHAR(length=1024),
        comment="Force the import and Hive table to define another PrimaryKey constraint. Comma separeted list of columns",
        existing_comment="Force the import and Hive table to define another PrimaryKey constraint. Comma separated list of columns",
        existing_nullable=True,
    )
    op.alter_column(
        "import_tables",
        "incr_maxvalue_pending",
        existing_type=mysql.VARCHAR(length=32),
        comment="Used for incremental imports. Dont change manually",
        existing_comment="Used for incremental imports. Don't change manually",
        existing_nullable=True,
    )
    op.alter_column(
        "import_tables",
        "incr_minvalue_pending",
        existing_type=mysql.VARCHAR(length=32),
        comment="Used for incremental imports. Dont change manually",
        existing_comment="Used for incremental imports. Don't change manually",
        existing_nullable=True,
    )
    op.alter_column(
        "import_tables",
        "incr_maxvalue",
        existing_type=mysql.VARCHAR(length=32),
        comment="Used for incremental imports. Dont change manually",
        existing_comment="Used for incremental imports. Don't change manually",
        existing_nullable=True,
    )
    op.alter_column(
        "import_tables",
        "incr_minvalue",
        existing_type=mysql.VARCHAR(length=32),
        comment="Used for incremental imports. Dont change manually",
        existing_comment="Used for incremental imports. Don't change manually",
        existing_nullable=True,
    )
    op.alter_column(
        "import_tables",
        "validationCustomQueryHiveValue",
        existing_type=mysql.TEXT(),
        comment="Used for validation. Dont change manually",
        existing_comment="Used for validation. Don't change manually",
        existing_nullable=True,
    )
    op.alter_column(
        "import_tables",
        "validationCustomQuerySourceValue",
        existing_type=mysql.TEXT(),
        comment="Used for validation. Dont change manually",
        existing_comment="Used for validation. Don't change manually",
        existing_nullable=True,
    )
    op.alter_column(
        "import_tables",
        "hive_rowcount",
        existing_type=mysql.BIGINT(display_width=20),
        comment="Used for validation. Dont change manually",
        existing_comment="Used for validation. Don't change manually",
        existing_nullable=True,
    )
    op.alter_column(
        "import_tables",
        "source_rowcount",
        existing_type=mysql.BIGINT(display_width=20),
        comment="Used for validation. Dont change manually",
        existing_comment="Used for validation. Don't change manually",
        existing_nullable=True,
    )
    op.alter_column(
        "import_tables",
        "mappers",
        existing_type=mysql.TINYINT(display_width=4),
        comment="-1 = auto or positiv number for a fixed number of mappers. If Auto, then it's calculated based of last sqoop import size",
        existing_comment="-1 = auto or positive number for a fixed number of mappers. If Auto, then it's calculated based of last sqoop import size",
        existing_nullable=False,
        existing_server_default=sa.text("-1"),
    )
    op.alter_column(
        "import_tables",
        "validationCustomQueryValidateImportTable",
        existing_type=mysql.TINYINT(display_width=4),
        comment="1 = Validate Import table, 0 = Dont validate Import table",
        existing_comment="1 = Validate Import table, 0 = Don't validate Import table",
        existing_nullable=False,
        existing_server_default=sa.text("1"),
    )
    op.alter_column(
        "import_tables",
        "validate_diff_allowed",
        existing_type=mysql.BIGINT(display_width=20),
        comment="-1 = auto calculated diff allowed. If a positiv number, this is the amount of rows that the diff is allowed to have",
        existing_comment="-1 = auto calculated diff allowed. If a positive number, this is the amount of rows that the diff is allowed to have",
        existing_nullable=False,
        existing_server_default=sa.text("-1"),
    )
    op.alter_column(
        "import_tables",
        "nomerge_ingestion_sql_addition",
        existing_type=mysql.VARCHAR(length=2048),
        comment="This will be added to the data ingestion of None-Merge imports (full, full_direct and incr). Usefull to filter out data from import tables to target tables",
        existing_comment="This will be added to the data ingestion of None-Merge imports (full, full_direct and incr). Useful to filter out data from import tables to target tables",
        existing_nullable=True,
    )
    op.create_table_comment(
        "import_statistics_last",
        "The last entry in table import_statistics is also stored in this table. This makes it easier to find the latest data without first grouping to find the latest entry. When import_statistics table grows to a high number of million rows, it saves alot of cpu power for the database server.",
        existing_comment="The last entry in table import_statistics is also stored in this table. This makes it easier to find the latest data without first grouping to find the latest entry. When import_statistics table grows to a high number of million rows, it saves a lot of cpu power for the database server.",
        schema=None,
    )
    op.alter_column(
        "import_statistics_last",
        "duration",
        existing_type=mysql.INTEGER(display_width=11),
        comment="Tota duration in seconds",
        existing_comment="Total duration in seconds",
        existing_nullable=True,
    )
    op.alter_column(
        "import_statistics_last",
        "sessions",
        existing_type=mysql.TINYINT(display_width=4),
        comment="How many parallell sessions was used against the source (sqoop mappers)",
        existing_comment="How many parallel sessions was used against the source (sqoop mappers)",
        existing_nullable=True,
    )
    op.create_table_comment(
        "import_statistics",
        "At the end of each import, all statistics about how long each part took aswell as general information about Hive datbase and table, number of rows imported, size of the import and much more are logged in this table. This table grows and will never be truncated by DBImport itself. If it becomes to large for you, it's up to each user to delete or truncate this table as you see fit.",
        existing_comment="At the end of each import, all statistics about how long each part took as well as general information about Hive database and table, number of rows imported, size of the import and much more are logged in this table. This table grows and will never be truncated by DBImport itself. If it becomes to large for you, it's up to each user to delete or truncate this table as you see fit.",
        schema=None,
    )
    op.alter_column(
        "import_statistics",
        "duration",
        existing_type=mysql.INTEGER(display_width=11),
        comment="Tota duration in seconds",
        existing_comment="Total duration in seconds",
        existing_nullable=True,
    )
    op.alter_column(
        "import_statistics",
        "sessions",
        existing_type=mysql.TINYINT(display_width=4),
        comment="How many parallell sessions was used against the source (sqoop mappers)",
        existing_comment="How many parallel sessions was used against the source (sqoop mappers)",
        existing_nullable=True,
    )
    op.create_table_comment(
        "import_stage",
        'The import tool keeps track of how far in the import the tool have succeeded. So in case of an error, lets say that Hive is not responding, the next time an import is executed it will skip the first part and continue from where it ended in error on the previous run. If you want to rerun from the begining, the information in this table needs to be cleared. This is done with the "manage --clearImportStage" tool. Keep in mind that clearing the stage of an incremental import might result in the loss of the data.',
        existing_comment='The import tool keeps track of how far in the import the tool have succeeded. So in case of an error, lets say that Hive is not responding, the next time an import is executed it will skip the first part and continue from where it ended in error on the previous run. If you want to rerun from the beginning, the information in this table needs to be cleared. This is done with the "manage --clearImportStage" tool. Keep in mind that clearing the stage of an incremental import might result in the loss of the data.',
        schema=None,
    )
    op.create_table_comment(
        "import_failure_log",
        "If there is an error or a warning during import, bu the import still continues, these errors are logged in this table. An example could be that  a column cant be altered, foreign key not created, no new columns can be added and such.",
        existing_comment="If there is an error or a warning during import, but the import still continues, these errors are logged in this table. An example could be that  a column can't be altered, foreign key not created, no new columns can be added and such.",
        schema=None,
    )
    op.alter_column(
        "import_columns",
        "source_primary_key",
        existing_type=mysql.TINYINT(display_width=4),
        comment="Number starting from 1 listing the order of the column in the PK. Dont change this manually",
        existing_comment="Number starting from 1 listing the order of the column in the PK. Don't change this manually",
        existing_nullable=True,
    )
    op.alter_column(
        "import_columns",
        "source_column_type",
        existing_type=mysql.VARCHAR(length=2048),
        comment="Column type in source system. Dont change this manually",
        existing_comment="Column type in source system. Don't change this manually",
        existing_nullable=False,
    )
    op.alter_column(
        "import_columns",
        "column_type",
        existing_type=mysql.VARCHAR(length=2048),
        comment="Column type in Hive. Dont change this manually",
        existing_comment="Column type in Hive. Don't change this manually",
        existing_nullable=False,
    )
    op.alter_column(
        "import_columns",
        "source_column_name",
        existing_type=mysql.VARCHAR(length=256),
        comment="Name of column in source system. Dont change this manually",
        existing_comment="Name of column in source system. Don't change this manually",
        existing_nullable=False,
    )
    op.alter_column(
        "import_columns",
        "column_name",
        existing_type=mysql.VARCHAR(length=256),
        comment="Name of column in Hive. Dont change this manually",
        existing_comment="Name of column in Hive. Don't change this manually",
        existing_nullable=False,
    )
    op.alter_column(
        "export_tables",
        "create_target_table_sql",
        existing_type=mysql.TEXT(),
        comment="SQL statement that was used to create the target table. Dont change manually",
        existing_comment="SQL statement that was used to create the target table. Don't change manually",
        existing_nullable=True,
    )
    op.alter_column(
        "export_tables",
        "sqoop_last_execution",
        existing_type=mysql.BIGINT(display_width=20),
        comment="Used to track sqoop operation. Dont change manually",
        existing_comment="Used to track sqoop operation. Don't change manually",
        existing_nullable=True,
    )
    op.alter_column(
        "export_tables",
        "sqoop_last_mappers",
        existing_type=mysql.TINYINT(display_width=4),
        comment="Used to track sqoop operation. Dont change manually",
        existing_comment="Used to track sqoop operation. Don't change manually",
        existing_nullable=True,
    )
    op.alter_column(
        "export_tables",
        "sqoop_last_rows",
        existing_type=mysql.BIGINT(display_width=20),
        comment="Used to track sqoop operation. Dont change manually",
        existing_comment="Used to track sqoop operation. Don't change manually",
        existing_nullable=True,
    )
    op.alter_column(
        "export_tables",
        "sqoop_last_size",
        existing_type=mysql.BIGINT(display_width=20),
        comment="Used to track sqoop operation. Dont change manually",
        existing_comment="Used to track sqoop operation. Don't change manually",
        existing_nullable=True,
    )
    op.alter_column(
        "export_tables",
        "incr_maxvalue_pending",
        existing_type=mysql.VARCHAR(length=32),
        comment="Used by incremental exports to keep track of progress. Dont change manually",
        existing_comment="Used by incremental exports to keep track of progress. Don't change manually",
        existing_nullable=True,
    )
    op.alter_column(
        "export_tables",
        "incr_minvalue_pending",
        existing_type=mysql.VARCHAR(length=32),
        comment="Used by incremental exports to keep track of progress. Dont change manually",
        existing_comment="Used by incremental exports to keep track of progress. Don't change manually",
        existing_nullable=True,
    )
    op.alter_column(
        "export_tables",
        "incr_maxvalue",
        existing_type=mysql.VARCHAR(length=32),
        comment="Used by incremental exports to keep track of progress. Dont change manually",
        existing_comment="Used by incremental exports to keep track of progress. Don't change manually",
        existing_nullable=True,
    )
    op.alter_column(
        "export_tables",
        "incr_minvalue",
        existing_type=mysql.VARCHAR(length=32),
        comment="Used by incremental exports to keep track of progress. Dont change manually",
        existing_comment="Used by incremental exports to keep track of progress. Don't change manually",
        existing_nullable=True,
    )
    op.alter_column(
        "export_tables",
        "validationCustomQueryTargetValue",
        existing_type=mysql.TEXT(),
        comment="Used for validation. Dont change manually",
        existing_comment="Used for validation. Don't change manually",
        existing_nullable=True,
    )
    op.alter_column(
        "export_tables",
        "validationCustomQueryHiveValue",
        existing_type=mysql.TEXT(),
        comment="Used for validation. Dont change manually",
        existing_comment="Used for validation. Don't change manually",
        existing_nullable=True,
    )
    op.alter_column(
        "export_tables",
        "target_rowcount",
        existing_type=mysql.BIGINT(display_width=20),
        comment="Number of rows in Target table. Dont change manually",
        existing_comment="Number of rows in Target table. Don't change manually",
        existing_nullable=True,
    )
    op.alter_column(
        "export_tables",
        "hive_rowcount",
        existing_type=mysql.BIGINT(display_width=20),
        comment="Number of rows in Hive table. Dont change manually",
        existing_comment="Number of rows in Hive table. Don't change manually",
        existing_nullable=True,
    )
    op.create_table_comment(
        "export_statistics_last",
        "The last entry in table export_statistics is also stored in this table. This makes it easier to find the latest data without first grouping to find the latest entry. When export_statistics table grows to a high number of million rows, it saves alot of cpu power for the database server.",
        existing_comment="The last entry in table export_statistics is also stored in this table. This makes it easier to find the latest data without first grouping to find the latest entry. When export_statistics table grows to a high number of million rows, it saves a lot of cpu power for the database server.",
        schema=None,
    )
    op.alter_column(
        "export_statistics_last",
        "duration",
        existing_type=mysql.INTEGER(display_width=11),
        comment="Tota duration in seconds",
        existing_comment="Total duration in seconds",
        existing_nullable=True,
    )
    op.alter_column(
        "export_statistics_last",
        "sessions",
        existing_type=mysql.TINYINT(display_width=4),
        comment="How many parallell sessions was used against the source (sqoop mappers)",
        existing_comment="How many parallel sessions was used against the source (sqoop mappers)",
        existing_nullable=True,
    )
    op.create_table_comment(
        "export_statistics",
        "At the end of each export, all statistics about how long each part took aswell as general information about Hive database and table, number of rows imported, size of the export and much more are logged in this table. This table grows and will never be truncated by DBImport itself. If it becomes to large for you, it's up to each user to delete or truncate this table as you see fit.",
        existing_comment="At the end of each export, all statistics about how long each part took as well as general information about Hive database and table, number of rows imported, size of the export and much more are logged in this table. This table grows and will never be truncated by DBImport itself. If it becomes to large for you, it's up to each user to delete or truncate this table as you see fit.",
        schema=None,
    )
    op.alter_column(
        "export_statistics",
        "duration",
        existing_type=mysql.INTEGER(display_width=11),
        comment="Tota duration in seconds",
        existing_comment="Total duration in seconds",
        existing_nullable=True,
    )
    op.alter_column(
        "export_statistics",
        "sessions",
        existing_type=mysql.TINYINT(display_width=4),
        comment="How many parallell sessions was used against the source (sqoop mappers)",
        existing_comment="How many parallel sessions was used against the source (sqoop mappers)",
        existing_nullable=True,
    )
    op.create_table_comment(
        "export_stage",
        'The export tool keeps track of how far in the export the tool have succeeded. So in case of an error, lets say that Hive is not responding, the next time an export is executed it will skip the first part and continue from where it ended in error on the previous run. If you want to rerun from the begining, the information in this table needs to be cleared. This is done with the "manage --clearExportStage" tool. Keep in mind that clearing the stage of an incremental export might result in the loss of the data.',
        existing_comment='The export tool keeps track of how far in the export the tool have succeeded. So in case of an error, lets say that Hive is not responding, the next time an export is executed it will skip the first part and continue from where it ended in error on the previous run. If you want to rerun from the beginning, the information in this table needs to be cleared. This is done with the "manage --clearExportStage" tool. Keep in mind that clearing the stage of an incremental export might result in the loss of the data.',
        schema=None,
    )
    op.alter_column(
        "export_columns",
        "comment",
        existing_type=mysql.TEXT(),
        comment="The column comment from the source system. Dont change this manually",
        existing_comment="The column comment from the source system. Don't change this manually",
        existing_nullable=True,
    )
    op.alter_column(
        "export_columns",
        "last_export_time",
        existing_type=mysql.DATETIME(),
        comment="Timestamp of last export. Dont change this manually",
        existing_comment="Timestamp of last export. Don't change this manually",
        existing_nullable=True,
    )
    op.alter_column(
        "export_columns",
        "last_update_from_hive",
        existing_type=mysql.DATETIME(),
        comment="Timestamp of last schema update from Hive. Dont change this manually",
        existing_comment="Timestamp of last schema update from Hive. Don't change this manually",
        existing_nullable=False,
    )
    op.alter_column(
        "export_columns",
        "column_order",
        existing_type=mysql.INTEGER(display_width=11),
        comment="The order of the columns. Dont change this manually",
        existing_comment="The order of the columns. Don't change this manually",
        existing_nullable=True,
    )
    op.alter_column(
        "export_columns",
        "column_type",
        existing_type=mysql.VARCHAR(length=2048),
        comment="Column type from Hive. Dont change this manually",
        existing_comment="Column type from Hive. Don't change this manually",
        existing_nullable=True,
    )
    op.alter_column(
        "export_columns",
        "column_name",
        existing_type=mysql.VARCHAR(length=256),
        comment="Name of column in target table. Dont change this manually",
        existing_comment="Name of column in target table. Don't change this manually",
        existing_nullable=False,
    )
    op.alter_column(
        "dbimport_instances",
        "sync_credentials",
        existing_type=mysql.TINYINT(display_width=4),
        comment="0 = Credentials wont be synced, 1 = The credentials information will be synced to the other cluster",
        existing_comment="0 = Credentials won't be synced, 1 = The credentials information will be synced to the other cluster",
        existing_nullable=False,
        existing_server_default=sa.text("0"),
    )
    op.alter_column(
        "configuration",
        "configKey",
        existing_type=mysql.VARCHAR(length=32),
        comment="Name of the configuration item. These are controlled and maintained by thee setup tool. Dont change these manually",
        existing_comment="Name of the configuration item. These are controlled and maintained by the setup tool. Don't change these manually",
        existing_nullable=False,
    )
    op.alter_column(
        "airflow_tasks",
        "task_dependency_downstream",
        existing_type=mysql.VARCHAR(length=256),
        comment="Defines the upstream dependency for the Task. Comma separated list",
        existing_comment="Defines the downstream dependency for the Task. Comma separated list",
        existing_nullable=True,
    )
    op.alter_column(
        "airflow_import_dags",
        "tags",
        existing_type=mysql.VARCHAR(length=256),
        comment="Comma seperated list of Airflow tags that will be set on the Dag",
        existing_comment="Comma separated list of Airflow tags that will be set on the Dag",
        existing_nullable=True,
    )
    op.alter_column(
        "airflow_import_dags",
        "application_notes",
        existing_type=mysql.TEXT(),
        comment="Free text field that can be used for application documentaton, notes or links. ",
        existing_comment="Free text field that can be used for application documentation, notes or links. ",
        existing_nullable=True,
    )
    op.alter_column(
        "airflow_import_dags",
        "retries",
        existing_type=mysql.TINYINT(display_width=4),
        comment="How many retries should be Task do in Airflow before it failes",
        existing_comment="How many retries should be Task do in Airflow before it fails",
        existing_nullable=False,
        existing_server_default=sa.text("5"),
    )
    op.alter_column(
        "airflow_import_dags",
        "finish_all_stage1_first",
        existing_type=mysql.TINYINT(display_width=4),
        comment="1 = All Import phase jobs will be completed first, and when all is successfull, the ETL phase start",
        existing_comment="1 = All Import phase jobs will be completed first, and when all is successful, the ETL phase start",
        existing_nullable=False,
        existing_server_default=sa.text("0"),
    )
    op.alter_column(
        "airflow_export_dags",
        "tags",
        existing_type=mysql.VARCHAR(length=256),
        comment="Comma seperated list of Airflow tags that will be set on the Dag",
        existing_comment="Comma separated list of Airflow tags that will be set on the Dag",
        existing_nullable=True,
    )
    op.alter_column(
        "airflow_export_dags",
        "application_notes",
        existing_type=mysql.TEXT(),
        comment="Free text field that can be used for application documentaton, notes or links. ",
        existing_comment="Free text field that can be used for application documentation, notes or links. ",
        existing_nullable=True,
    )
    op.alter_column(
        "airflow_export_dags",
        "trigger_dag_on_success",
        existing_type=mysql.VARCHAR(length=64),
        comment="Name of DAG to trigger if export is successfull. Comma seperated list of DAGs (optional)",
        existing_comment="Name of DAG to trigger if export is successful. Comma separated list of DAGs (optional)",
        existing_nullable=True,
    )
    op.alter_column(
        "airflow_export_dags",
        "retries",
        existing_type=mysql.TINYINT(display_width=4),
        comment="How many retries should be Task do in Airflow before it failes",
        existing_comment="How many retries should be Task do in Airflow before it fails",
        existing_nullable=True,
    )
    op.alter_column(
        "airflow_etl_dags",
        "tags",
        existing_type=mysql.VARCHAR(length=256),
        comment="Comma seperated list of Airflow tags that will be set on the Dag",
        existing_comment="Comma separated list of Airflow tags that will be set on the Dag",
        existing_nullable=True,
    )
    op.alter_column(
        "airflow_etl_dags",
        "application_notes",
        existing_type=mysql.TEXT(),
        comment="Free text field that can be used for application documentaton, notes or links. ",
        existing_comment="Free text field that can be used for application documentation, notes or links. ",
        existing_nullable=True,
    )
    op.alter_column(
        "airflow_etl_dags",
        "retries",
        existing_type=mysql.TINYINT(display_width=4),
        comment="How many retries should be Task do in Airflow before it failes",
        existing_comment="How many retries should be Task do in Airflow before it fails",
        existing_nullable=True,
    )
    op.create_table_comment(
        "airflow_custom_dags",
        'Its possible to construct a DAG that have no import, export or ETL definitions in it, but instead just Tasks from the airflow_task table. That might nbe useful to for example run custom Hive Code after an import is completed as a separate DAG. Defining a DAG in here also requires you to have at least one task in airflow_tasks defined "in main"',
        existing_comment='Its possible to construct a DAG that have no import, export or ETL definitions in it, but instead just Tasks from the airflow_task table. That might be useful to for example run custom Hive Code after an import is completed as a separate DAG. Defining a DAG in here also requires you to have at least one task in airflow_tasks defined "in main"',
        schema=None,
    )
    op.alter_column(
        "airflow_custom_dags",
        "tags",
        existing_type=mysql.VARCHAR(length=256),
        comment="Comma seperated list of Airflow tags that will be set on the Dag",
        existing_comment="Comma separated list of Airflow tags that will be set on the Dag",
        existing_nullable=True,
    )
    op.alter_column(
        "airflow_custom_dags",
        "application_notes",
        existing_type=mysql.TEXT(),
        comment="Free text field that can be used for application documentaton, notes or links. ",
        existing_comment="Free text field that can be used for application documentation, notes or links. ",
        existing_nullable=True,
    )
    op.alter_column(
        "airflow_custom_dags",
        "retries",
        existing_type=mysql.INTEGER(display_width=11),
        comment="How many retries should be Task do in Airflow before it failes",
        existing_comment="How many retries should be Task do in Airflow before it fails",
        existing_nullable=False,
        existing_server_default=sa.text("0"),
    )
