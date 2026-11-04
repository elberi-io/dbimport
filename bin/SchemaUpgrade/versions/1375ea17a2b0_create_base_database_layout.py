"""Create Base database layout

Revision ID: 1375ea17a2b0
Revises:
Create Date: 2019-06-12 08:06:54.186762

"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql

# revision identifiers, used by Alembic.
revision = "1375ea17a2b0"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "airflow_custom_dags",
        sa.Column(
            "dag_name",
            sa.String(length=64),
            nullable=False,
            comment="Name of Airflow DAG.",
        ),
        sa.Column(
            "schedule_interval",
            sa.String(length=20),
            server_default=sa.text("'None'"),
            nullable=False,
            comment="Time to execute dag",
        ),
        sa.Column(
            "retries", sa.Integer(), server_default=sa.text("'0'"), nullable=False
        ),
        sa.Column(
            "operator_notes",
            sa.Text(),
            nullable=True,
            comment="Free text field to write a note about the import. ",
        ),
        sa.Column(
            "application_notes",
            sa.Text(),
            nullable=True,
            comment="Free text field that can be used for application documentaton, notes or links. ",
        ),
        sa.PrimaryKeyConstraint("dag_name"),
    )
    op.create_table(
        "airflow_dag_sensors",
        sa.Column(
            "dag_name",
            sa.String(length=64),
            nullable=False,
            comment="Name of DAG to add sensor to",
        ),
        sa.Column(
            "sensor_name",
            sa.String(length=64),
            nullable=False,
            comment="name of the sensor Task",
        ),
        sa.Column(
            "wait_for_dag",
            sa.String(length=64),
            nullable=False,
            comment="Name of DAG to wait for. Must be defined in DBImport",
        ),
        sa.Column(
            "wait_for_task",
            sa.String(length=64),
            nullable=True,
            comment="Name of task to wait for.Default is 'stop'",
        ),
        sa.Column(
            "timeout_minutes",
            sa.Integer(),
            nullable=True,
            comment="Number of minutes to wait for DAG.",
        ),
        sa.PrimaryKeyConstraint("dag_name", "sensor_name"),
    )
    op.create_table(
        "airflow_etl_dags",
        sa.Column(
            "dag_name",
            sa.String(length=64),
            nullable=False,
            comment="Name of Airflow DAG.",
        ),
        sa.Column(
            "schedule_interval",
            sa.String(length=20),
            server_default=sa.text("'None'"),
            nullable=False,
            comment="Time to execute dag",
        ),
        sa.Column("filter_job", sa.String(length=64), nullable=False),
        sa.Column("filter_task", sa.String(length=64), nullable=True),
        sa.Column("filter_source_db", sa.String(length=256), nullable=True),
        sa.Column("filter_target_db", sa.String(length=256), nullable=True),
        sa.Column("retries", mysql.TINYINT(display_width=4), nullable=True),
        sa.Column("trigger_dag_on_success", sa.String(length=64), nullable=True),
        sa.Column(
            "operator_notes",
            sa.Text(),
            nullable=True,
            comment="Free text field to write a note about the import. ",
        ),
        sa.Column(
            "application_notes",
            sa.Text(),
            nullable=True,
            comment="Free text field that can be used for application documentaton, notes or links. ",
        ),
        sa.PrimaryKeyConstraint("dag_name"),
    )
    op.create_table(
        "airflow_execution_type",
        sa.Column("executionid", sa.Integer(), nullable=False),
        sa.Column("execution_type", sa.Text(), nullable=False),
        sa.PrimaryKeyConstraint("executionid"),
    )
    op.create_table(
        "airflow_export_dags",
        sa.Column(
            "dag_name",
            sa.String(length=64),
            nullable=False,
            comment="Name of Airflow DAG.",
        ),
        sa.Column(
            "schedule_interval",
            sa.String(length=20),
            server_default=sa.text("'None'"),
            nullable=False,
            comment="Time to execute dag",
        ),
        sa.Column(
            "filter_dbalias",
            sa.String(length=256),
            nullable=False,
            comment="Filter string for DBALIAS in export_tables",
        ),
        sa.Column(
            "filter_target_schema",
            sa.String(length=256),
            nullable=True,
            comment="Filter string for TARGET_SCHEMA  in export_tables",
        ),
        sa.Column(
            "filter_target_table",
            sa.String(length=256),
            nullable=True,
            comment="Filter string for TARGET_TABLE  in export_tables",
        ),
        sa.Column(
            "use_python_dbimport",
            mysql.TINYINT(display_width=4),
            server_default=sa.text("'1'"),
            nullable=False,
        ),
        sa.Column("retries", mysql.TINYINT(display_width=4), nullable=True),
        sa.Column(
            "trigger_dag_on_success",
            sa.String(length=64),
            nullable=True,
            comment="Name of DAG to trigger if export is successfull. Comma seperated list of DAGs (optional)",
        ),
        sa.Column(
            "operator_notes",
            sa.Text(),
            nullable=True,
            comment="Free text field to write a note about the import. ",
        ),
        sa.Column(
            "application_notes",
            sa.Text(),
            nullable=True,
            comment="Free text field that can be used for application documentaton, notes or links. ",
        ),
        sa.PrimaryKeyConstraint("dag_name"),
    )
    op.create_table(
        "airflow_import_dags",
        sa.Column(
            "dag_name",
            sa.String(length=64),
            nullable=False,
            comment="Name of Airflow DAG.",
        ),
        sa.Column(
            "schedule_interval",
            sa.String(length=32),
            server_default=sa.text("'None'"),
            nullable=False,
            comment="Time to execute dag",
        ),
        sa.Column(
            "filter_hive",
            sa.String(length=256),
            nullable=False,
            comment="Filter string for database and table. ; separated. Wildcards (*) allowed. Example HIVE_DB.HIVE_TABLE; HIVE_DB.HIVE_TABLE",
        ),
        sa.Column(
            "filter_hive_db_OLD",
            sa.String(length=256),
            nullable=True,
            comment="NOT USED: Filter string for HIVE_DB in import_tables",
        ),
        sa.Column(
            "filter_hive_table_OLD",
            sa.String(length=256),
            nullable=True,
            comment="NOT USED: Filter string for HIVE_TABLE in import_tables",
        ),
        sa.Column(
            "use_python_dbimport",
            mysql.TINYINT(display_width=4),
            server_default=sa.text("'1'"),
            nullable=False,
        ),
        sa.Column(
            "finish_all_stage1_first",
            mysql.TINYINT(display_width=4),
            server_default=sa.text("'0'"),
            nullable=False,
        ),
        sa.Column(
            "retries",
            mysql.TINYINT(display_width=4),
            server_default=sa.text("'5'"),
            nullable=False,
        ),
        sa.Column("retries_stage1", mysql.TINYINT(display_width=4), nullable=True),
        sa.Column("retries_stage2", mysql.TINYINT(display_width=4), nullable=True),
        sa.Column(
            "pool_stage1",
            sa.String(length=256),
            nullable=True,
            comment="Airflow pool used for stage1 tasks. NULL for default pool",
        ),
        sa.Column(
            "pool_stage2",
            sa.String(length=256),
            nullable=True,
            comment="Airflow pool used for stage2 tasks. NULL for default pool",
        ),
        sa.Column(
            "operator_notes",
            sa.Text(),
            nullable=True,
            comment="Free text field to write a note about the import. ",
        ),
        sa.Column(
            "application_notes",
            sa.Text(),
            nullable=True,
            comment="Free text field that can be used for application documentaton, notes or links. ",
        ),
        sa.Column(
            "auto_table_discovery",
            mysql.TINYINT(display_width=4),
            server_default=sa.text("'1'"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("dag_name"),
    )

    op.create_table(
        "auto_discovered_tables",
        sa.Column("hive_db", sa.String(length=256), nullable=False),
        sa.Column("dbalias", sa.String(length=256), nullable=False),
        sa.Column("source_schema", sa.String(length=256), nullable=False),
        sa.Column("source_table", sa.String(length=256), nullable=False),
        sa.Column(
            "discovery_time",
            sa.DateTime(),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.Column(
            "migrate_to_import_tables",
            mysql.TINYINT(display_width=4),
            server_default=sa.text("'0'"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("hive_db", "dbalias", "source_schema", "source_table"),
    )

    op.create_table(
        "configuration",
        sa.Column(
            "configKey",
            sa.String(length=32),
            nullable=False,
            comment="Name of the configuration item. These are controlled and maintained by thee setup tool. Dont change these manually",
        ),
        sa.Column(
            "valueInt", sa.Integer(), nullable=True, comment="Integer based Value"
        ),
        sa.Column(
            "valueStr",
            sa.String(length=256),
            nullable=True,
            comment="String based Value",
        ),
        sa.Column(
            "valueDate", sa.DateTime(), nullable=True, comment="Date based Value"
        ),
        sa.Column(
            "description",
            sa.Text(),
            nullable=True,
            comment="Description on the setting",
        ),
        sa.PrimaryKeyConstraint("configKey"),
        comment="This is the DBImport global configuration table. In here you can configure items such as the name of the staging database, disable global executions, max number of mappers and much more. ",
    )

    #    op.create_table('db_version',
    #    sa.Column('version', sa.Integer(), autoincrement=False, nullable=False, comment='Version of deployed DBImport schema'),
    #    sa.Column('install_date', sa.DateTime(), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=False),
    #    sa.PrimaryKeyConstraint('version')
    #    )

    op.create_table(
        "etl_jobs",
        sa.Column("job", sa.String(length=64), nullable=False),
        sa.Column("task", sa.String(length=64), nullable=False),
        sa.Column("job_id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column(
            "etl_type",
            sa.String(length=32),
            server_default=sa.text("'0'"),
            nullable=False,
        ),
        sa.Column(
            "include_in_airflow",
            mysql.TINYINT(display_width=4),
            server_default=sa.text("'1'"),
            nullable=False,
        ),
        sa.Column("source_db", sa.String(length=256), nullable=True),
        sa.Column("source_table", sa.String(length=256), nullable=True),
        sa.Column("target_db", sa.String(length=256), nullable=True),
        sa.Column("target_table", sa.String(length=256), nullable=True),
        sa.Column(
            "operator_notes",
            sa.String(length=256),
            nullable=True,
            comment="Free text field to write a note about the import. ",
        ),
        sa.PrimaryKeyConstraint("job", "task"),
        sa.UniqueConstraint("job_id"),
    )

    op.alter_column(
        "etl_jobs", "job_id", existing_type=sa.Integer(), autoincrement=True
    )

    op.create_table(
        "export_retries_log",
        sa.Column(
            "dbalias",
            sa.String(length=256),
            nullable=False,
            comment="Database connection name that we export to",
        ),
        sa.Column(
            "target_schema",
            sa.String(length=256),
            nullable=False,
            comment="Schema on the target system",
        ),
        sa.Column(
            "target_table",
            sa.String(length=256),
            nullable=False,
            comment="Table on the target system",
        ),
        sa.Column(
            "retry_time",
            sa.DateTime(),
            nullable=False,
            comment="Time when the retry was started",
        ),
        sa.Column(
            "stage",
            sa.Integer(),
            nullable=False,
            comment="The stage of the import that the retry started from. This is an internal stage and has nothing to do with stage1 and stage2 in Airflow DAG's",
        ),
        sa.Column(
            "stage_description",
            sa.Text(),
            nullable=False,
            comment="Description of the stage",
        ),
        sa.PrimaryKeyConstraint(
            "dbalias", "target_schema", "target_table", "retry_time"
        ),
        comment="Log of all retries that have happened. ",
    )
    op.create_table(
        "export_stage",
        sa.Column(
            "dbalias",
            sa.String(length=256),
            nullable=False,
            comment="Database connection name that we export to",
        ),
        sa.Column(
            "target_schema",
            sa.String(length=256),
            nullable=False,
            comment="Schema on the target system",
        ),
        sa.Column(
            "target_table",
            sa.String(length=256),
            nullable=False,
            comment="Table on the target system",
        ),
        sa.Column(
            "stage",
            sa.Integer(),
            nullable=False,
            comment="Current stage of the export. This is the internal stage number",
        ),
        sa.Column(
            "stage_description",
            sa.Text(),
            nullable=False,
            comment="Description of the stage",
        ),
        sa.Column(
            "stage_time",
            sa.DateTime(),
            nullable=True,
            comment="The date and time when the import entered the stage",
        ),
        sa.PrimaryKeyConstraint("dbalias", "target_schema", "target_table"),
        comment='The export tool keeps track of how far in the export the tool have succeeded. So in case of an error, lets say that Hive is not responding, the next time an export is executed it will skip the first part and continue from where it ended in error on the previous run. If you want to rerun from the begining, the information in this table needs to be cleared. This is done with the "manage --clearExportStage" tool. Keep in mind that clearing the stage of an incremental export might result in the loss of the data.',
    )
    op.create_table(
        "export_stage_statistics",
        sa.Column(
            "dbalias",
            sa.String(length=256),
            nullable=False,
            comment="Database connection name that we export to",
        ),
        sa.Column(
            "target_schema",
            sa.String(length=256),
            nullable=False,
            comment="Schema on the target system",
        ),
        sa.Column(
            "target_table",
            sa.String(length=256),
            nullable=False,
            comment="Table on the target system",
        ),
        sa.Column(
            "stage",
            sa.Integer(),
            nullable=False,
            comment="Current stage of the export. This is the internal stage number",
        ),
        sa.Column(
            "start", sa.DateTime(), nullable=False, comment="Time when stage started"
        ),
        sa.Column(
            "stop",
            sa.DateTime(),
            nullable=False,
            comment="Time when stage was completed",
        ),
        sa.Column(
            "duration", sa.Integer(), nullable=False, comment="Duration of stage"
        ),
        sa.PrimaryKeyConstraint("dbalias", "target_schema", "target_table", "stage"),
        comment="As DBImport progress through the different stages of the export, it also keeps track of start and stop time for each stage together with the duration. That information is kept in this table",
    )
    op.create_table(
        "export_statistics",
        sa.Column(
            "id",
            mysql.BIGINT(display_width=20),
            autoincrement=True,
            nullable=False,
            comment="Auto incremented PrimaryKey of the table",
        ),
        sa.Column(
            "dbalias",
            sa.String(length=256),
            nullable=True,
            comment="ID of the Database Connection",
        ),
        sa.Column(
            "target_database",
            sa.String(length=256),
            nullable=True,
            comment="Name of the source database",
        ),
        sa.Column(
            "target_schema",
            sa.String(length=256),
            nullable=True,
            comment="Name of the source schema",
        ),
        sa.Column(
            "target_table",
            sa.String(length=256),
            nullable=True,
            comment="Name of the source table",
        ),
        sa.Column(
            "hive_db", sa.String(length=256), nullable=True, comment="Hive Database"
        ),
        sa.Column(
            "hive_table", sa.String(length=256), nullable=True, comment="Hive Table"
        ),
        sa.Column(
            "export_phase",
            sa.String(length=32),
            nullable=True,
            comment="Import Phase method",
        ),
        sa.Column(
            "incremental",
            mysql.TINYINT(display_width=4),
            nullable=True,
            comment="0 = Full import, 1 = Incremental import",
        ),
        sa.Column(
            "rows",
            mysql.BIGINT(display_width=20),
            nullable=True,
            comment="How many rows that was imported",
        ),
        sa.Column(
            "size",
            mysql.BIGINT(display_width=20),
            nullable=True,
            comment="The total size in bytes that was imported",
        ),
        sa.Column(
            "sessions",
            mysql.TINYINT(display_width=4),
            nullable=True,
            comment="How many parallell sessions was used against the source (sqoop mappers)",
        ),
        sa.Column(
            "duration", sa.Integer(), nullable=True, comment="Tota duration in seconds"
        ),
        sa.Column("start", sa.DateTime(), nullable=True, comment="Timestamp of start"),
        sa.Column("stop", sa.DateTime(), nullable=True, comment="Timestamp of stop"),
        sa.Column("get_hive_tableschema_duration", sa.Integer(), nullable=True),
        sa.Column("get_hive_tableschema_start", sa.DateTime(), nullable=True),
        sa.Column("get_hive_tableschema_stop", sa.DateTime(), nullable=True),
        sa.Column("clear_table_rowcount_duration", sa.Integer(), nullable=True),
        sa.Column("clear_table_rowcount_start", sa.DateTime(), nullable=True),
        sa.Column("clear_table_rowcount_stop", sa.DateTime(), nullable=True),
        sa.Column("create_temp_table_duration", sa.Integer(), nullable=True),
        sa.Column("create_temp_table_start", sa.DateTime(), nullable=True),
        sa.Column("create_temp_table_stop", sa.DateTime(), nullable=True),
        sa.Column("truncate_temp_table_duration", sa.Integer(), nullable=True),
        sa.Column("truncate_temp_table_start", sa.DateTime(), nullable=True),
        sa.Column("truncate_temp_table_stop", sa.DateTime(), nullable=True),
        sa.Column("fetch_maxvalue_start", sa.DateTime(), nullable=True),
        sa.Column("fetch_maxvalue_stop", sa.DateTime(), nullable=True),
        sa.Column("fetch_maxvalue_duration", sa.Integer(), nullable=True),
        sa.Column("insert_into_temp_table_duration", sa.Integer(), nullable=True),
        sa.Column("insert_into_temp_table_start", sa.DateTime(), nullable=True),
        sa.Column("insert_into_temp_table_stop", sa.DateTime(), nullable=True),
        sa.Column("create_target_table_duration", sa.Integer(), nullable=True),
        sa.Column("create_target_table_start", sa.DateTime(), nullable=True),
        sa.Column("create_target_table_stop", sa.DateTime(), nullable=True),
        sa.Column("truncate_target_table_duration", sa.Integer(), nullable=True),
        sa.Column("truncate_target_table_start", sa.DateTime(), nullable=True),
        sa.Column("truncate_target_table_stop", sa.DateTime(), nullable=True),
        sa.Column("sqoop_duration", sa.Integer(), nullable=True),
        sa.Column("sqoop_start", sa.DateTime(), nullable=True),
        sa.Column("sqoop_stop", sa.DateTime(), nullable=True),
        sa.Column("validate_duration", sa.Integer(), nullable=True),
        sa.Column("validate_start", sa.DateTime(), nullable=True),
        sa.Column("validate_stop", sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        comment="At the end of each export, all statistics about how long each part took aswell as general information about Hive database and table, number of rows imported, size of the export and much more are logged in this table. This table grows and will never be truncated by DBImport itself. If it becomes to large for you, it's up to each user to delete or truncate this table as you see fit.",
    )
    op.create_index(
        "dbalias_target_schema_target_table",
        "export_statistics",
        ["dbalias", "target_schema", "target_table"],
        unique=False,
    )
    op.create_table(
        "export_statistics_last",
        sa.Column(
            "dbalias",
            sa.String(length=256),
            nullable=False,
            comment="ID of the Database Connection",
        ),
        sa.Column(
            "target_database",
            sa.String(length=256),
            nullable=False,
            comment="Name of the source database",
        ),
        sa.Column(
            "target_schema",
            sa.String(length=256),
            nullable=False,
            comment="Name of the source schema",
        ),
        sa.Column(
            "target_table",
            sa.String(length=256),
            nullable=False,
            comment="Name of the source table",
        ),
        sa.Column(
            "hive_db", sa.String(length=256), nullable=True, comment="Hive Database"
        ),
        sa.Column(
            "hive_table", sa.String(length=256), nullable=True, comment="Hive Table"
        ),
        sa.Column(
            "export_phase",
            sa.String(length=32),
            nullable=True,
            comment="Import Phase method",
        ),
        sa.Column(
            "incremental",
            mysql.TINYINT(display_width=4),
            nullable=True,
            comment="0 = Full import, 1 = Incremental import",
        ),
        sa.Column(
            "rows",
            mysql.BIGINT(display_width=20),
            nullable=True,
            comment="How many rows that was imported",
        ),
        sa.Column(
            "size",
            mysql.BIGINT(display_width=20),
            nullable=True,
            comment="The total size in bytes that was imported",
        ),
        sa.Column(
            "sessions",
            mysql.TINYINT(display_width=4),
            nullable=True,
            comment="How many parallell sessions was used against the source (sqoop mappers)",
        ),
        sa.Column(
            "duration", sa.Integer(), nullable=True, comment="Tota duration in seconds"
        ),
        sa.Column("start", sa.DateTime(), nullable=True, comment="Timestamp of start"),
        sa.Column("stop", sa.DateTime(), nullable=True, comment="Timestamp of stop"),
        sa.Column("get_hive_tableschema_duration", sa.Integer(), nullable=True),
        sa.Column("get_hive_tableschema_start", sa.DateTime(), nullable=True),
        sa.Column("get_hive_tableschema_stop", sa.DateTime(), nullable=True),
        sa.Column("clear_table_rowcount_duration", sa.Integer(), nullable=True),
        sa.Column("clear_table_rowcount_start", sa.DateTime(), nullable=True),
        sa.Column("clear_table_rowcount_stop", sa.DateTime(), nullable=True),
        sa.Column("create_temp_table_duration", sa.Integer(), nullable=True),
        sa.Column("create_temp_table_start", sa.DateTime(), nullable=True),
        sa.Column("create_temp_table_stop", sa.DateTime(), nullable=True),
        sa.Column("truncate_temp_table_duration", sa.Integer(), nullable=True),
        sa.Column("truncate_temp_table_start", sa.DateTime(), nullable=True),
        sa.Column("truncate_temp_table_stop", sa.DateTime(), nullable=True),
        sa.Column("fetch_maxvalue_start", sa.DateTime(), nullable=True),
        sa.Column("fetch_maxvalue_stop", sa.DateTime(), nullable=True),
        sa.Column("fetch_maxvalue_duration", sa.Integer(), nullable=True),
        sa.Column("insert_into_temp_table_duration", sa.Integer(), nullable=True),
        sa.Column("insert_into_temp_table_start", sa.DateTime(), nullable=True),
        sa.Column("insert_into_temp_table_stop", sa.DateTime(), nullable=True),
        sa.Column("create_target_table_duration", sa.Integer(), nullable=True),
        sa.Column("create_target_table_start", sa.DateTime(), nullable=True),
        sa.Column("create_target_table_stop", sa.DateTime(), nullable=True),
        sa.Column("truncate_target_table_duration", sa.Integer(), nullable=True),
        sa.Column("truncate_target_table_start", sa.DateTime(), nullable=True),
        sa.Column("truncate_target_table_stop", sa.DateTime(), nullable=True),
        sa.Column("sqoop_duration", sa.Integer(), nullable=True),
        sa.Column("sqoop_start", sa.DateTime(), nullable=True),
        sa.Column("sqoop_stop", sa.DateTime(), nullable=True),
        sa.Column("validate_duration", sa.Integer(), nullable=True),
        sa.Column("validate_start", sa.DateTime(), nullable=True),
        sa.Column("validate_stop", sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint("dbalias", "target_schema", "target_table"),
        comment="The last entry in table export_statistics is also stored in this table. This makes it easier to find the latest data without first grouping to find the latest entry. When export_statistics table grows to a high number of million rows, it saves alot of cpu power for the database server.",
    )

    op.create_table(
        "export_tables",
        sa.Column(
            "dbalias",
            sa.String(length=256),
            nullable=False,
            comment="Database connection name that we export to",
        ),
        sa.Column(
            "target_schema",
            sa.String(length=256),
            nullable=False,
            comment="Schema on the target system",
        ),
        sa.Column(
            "target_table",
            sa.String(length=256),
            nullable=False,
            comment="Table on the target system",
        ),
        sa.Column(
            "table_id",
            sa.Integer(),
            autoincrement=True,
            nullable=False,
            comment="Unique identifier of the table",
        ),
        sa.Column(
            "export_type",
            sa.String(length=16),
            server_default=sa.text("'full'"),
            nullable=False,
            comment="What export method to use. Only full and incr is supported.",
        ),
        sa.Column(
            "hive_db",
            sa.String(length=256),
            nullable=False,
            comment="Name of Hive Database that we export from",
        ),
        sa.Column(
            "hive_table",
            sa.String(length=256),
            nullable=False,
            comment="Name of Hive Table that we export from",
        ),
        sa.Column(
            "last_update_from_hive",
            sa.DateTime(),
            nullable=True,
            comment="Timestamp of last schema update from Hive",
        ),
        sa.Column(
            "sql_where_addition",
            sa.String(length=1024),
            nullable=True,
            comment='Will be added AFTER the SQL WHERE. If it\'s an incr export, this will be after the incr limit statements. Example "orderId > 1000"',
        ),
        sa.Column(
            "include_in_airflow",
            mysql.TINYINT(display_width=4),
            server_default=sa.text("'1'"),
            nullable=False,
            comment="Will the table be included in Airflow DAG when it matches the DAG selection",
        ),
        sa.Column(
            "export_history",
            mysql.TINYINT(display_width=4),
            server_default=sa.text("'0'"),
            nullable=False,
            comment="<NOT USED>",
        ),
        sa.Column(
            "source_is_view",
            mysql.TINYINT(display_width=4),
            server_default=sa.text("'0'"),
            nullable=False,
            comment="<NOT USED>",
        ),
        sa.Column(
            "source_is_acid",
            mysql.TINYINT(display_width=4),
            server_default=sa.text("'0'"),
            nullable=False,
            comment="<NOT USED>",
        ),
        sa.Column(
            "validate_export",
            mysql.TINYINT(display_width=4),
            server_default=sa.text("'1'"),
            nullable=False,
            comment="1 = Validate the export once it's done. 0 = Disable validation",
        ),
        sa.Column(
            "uppercase_columns",
            mysql.TINYINT(display_width=4),
            server_default=sa.text("'-1'"),
            nullable=False,
            comment="-1 = auto (Oracle = uppercase, other databases = lowercase)",
        ),
        sa.Column(
            "truncate_target",
            mysql.TINYINT(display_width=4),
            server_default=sa.text("'1'"),
            nullable=False,
            comment="1 = Truncate the target table before we export the data. Not used by incremental exports",
        ),
        sa.Column(
            "mappers",
            mysql.TINYINT(display_width=4),
            server_default=sa.text("'-1'"),
            nullable=False,
            comment="-1 = auto, 0 = invalid. Auto updated by 'export_main.sh'",
        ),
        sa.Column(
            "hive_rowcount",
            mysql.BIGINT(display_width=20),
            nullable=True,
            comment="Number of rows in Hive table. Dont change manually",
        ),
        sa.Column(
            "target_rowcount",
            mysql.BIGINT(display_width=20),
            nullable=True,
            comment="Number of rows in Target table. Dont change manually",
        ),
        sa.Column(
            "incr_column",
            sa.String(length=256),
            nullable=True,
            comment="The column in the Hive table that will be used to identify new rows for the incremental export. Must be a timestamp column",
        ),
        sa.Column(
            "incr_validation_method",
            sa.Enum("full", "incr"),
            server_default=sa.text("'full'"),
            nullable=True,
            comment="full or incr. Full means that the validation will check to total number of rows up until maxvalue and compare source with target. Incr will only compare the rows between min and max value (the data that sqoop just wrote)",
        ),
        sa.Column(
            "incr_minvalue",
            sa.String(length=25),
            nullable=True,
            comment="Used by incremental exports to keep track of progress. Dont change manually",
        ),
        sa.Column(
            "incr_maxvalue",
            sa.String(length=25),
            nullable=True,
            comment="Used by incremental exports to keep track of progress. Dont change manually",
        ),
        sa.Column(
            "incr_minvalue_pending",
            sa.String(length=25),
            nullable=True,
            comment="Used by incremental exports to keep track of progress. Dont change manually",
        ),
        sa.Column(
            "incr_maxvalue_pending",
            sa.String(length=25),
            nullable=True,
            comment="Used by incremental exports to keep track of progress. Dont change manually",
        ),
        sa.Column(
            "sqoop_options",
            sa.String(length=1024),
            nullable=True,
            comment="Sqoop options to use during export.",
        ),
        sa.Column(
            "sqoop_last_size",
            mysql.BIGINT(display_width=20),
            nullable=True,
            comment="Used to track sqoop operation. Dont change manually",
        ),
        sa.Column(
            "sqoop_last_rows",
            mysql.BIGINT(display_width=20),
            nullable=True,
            comment="Used to track sqoop operation. Dont change manually",
        ),
        sa.Column(
            "sqoop_last_mappers",
            mysql.TINYINT(display_width=4),
            nullable=True,
            comment="Used to track sqoop operation. Dont change manually",
        ),
        sa.Column(
            "sqoop_last_execution",
            mysql.BIGINT(display_width=20),
            nullable=True,
            comment="Used to track sqoop operation. Dont change manually",
        ),
        sa.Column(
            "create_target_table_sql",
            sa.Text(),
            nullable=True,
            comment="SQL statement that was used to create the target table. Dont change manually",
        ),
        sa.Column(
            "operator_notes",
            sa.Text(),
            nullable=True,
            comment="Free text field to write a note about the export. ",
        ),
        sa.PrimaryKeyConstraint("dbalias", "target_schema", "target_table"),
        sa.UniqueConstraint("table_id"),
        comment="Main table where all tables that we can export are stored. ",
    )

    op.alter_column(
        "export_tables", "table_id", existing_type=sa.Integer(), autoincrement=True
    )

    op.create_table(
        "import_failure_log",
        sa.Column(
            "hive_db", sa.String(length=256), nullable=False, comment="Hive Database"
        ),
        sa.Column(
            "hive_table", sa.String(length=256), nullable=False, comment="Hive Table"
        ),
        sa.Column(
            "eventtime",
            sa.DateTime(),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
            comment="Time when error/warning occurred",
        ),
        sa.Column(
            "severity", sa.Text(), nullable=True, comment="The Severity of the event. "
        ),
        sa.Column(
            "import_type", sa.Text(), nullable=True, comment="The import method used"
        ),
        sa.Column(
            "error_text",
            sa.Text(),
            nullable=True,
            comment="Text describing the failure",
        ),
        sa.PrimaryKeyConstraint("hive_db", "hive_table", "eventtime"),
        comment="If there is an error or a warning during import, bu the import still continues, these errors are logged in this table. An example could be that  a column cant be altered, foreign key not created, no new columns can be added and such.",
    )
    op.create_index(
        "hive_db_hive_table",
        "import_failure_log",
        ["hive_db", "hive_table"],
        unique=False,
    )
    op.create_table(
        "import_retries_log",
        sa.Column("hive_db", sa.String(length=256), nullable=False, comment="Hive DB"),
        sa.Column(
            "hive_table", sa.String(length=256), nullable=False, comment="Hive Table"
        ),
        sa.Column(
            "retry_time",
            sa.DateTime(),
            nullable=False,
            comment="Time when the retry was started",
        ),
        sa.Column(
            "stage",
            sa.Integer(),
            nullable=False,
            comment="The stage of the import that the retry started from. This is an internal stage and has nothing to do with stage1 and stage2 in Airflow DAG's",
        ),
        sa.Column(
            "stage_description",
            sa.Text(),
            nullable=False,
            comment="Description of the stage",
        ),
        sa.PrimaryKeyConstraint("hive_db", "hive_table", "retry_time"),
        comment="Log of all retries that have happened. ",
    )
    op.create_table(
        "import_stage",
        sa.Column(
            "hive_db", sa.String(length=256), nullable=False, comment="Hive Database"
        ),
        sa.Column(
            "hive_table", sa.String(length=256), nullable=False, comment="Hive Table"
        ),
        sa.Column(
            "stage",
            sa.Integer(),
            nullable=False,
            comment="Current stage of the import. This is an internal stage and has nothing to do with stage1 and stage2 in Airflow DAG's",
        ),
        sa.Column(
            "stage_description",
            sa.Text(),
            nullable=False,
            comment="Description of the stage",
        ),
        sa.Column(
            "stage_time",
            sa.DateTime(),
            nullable=True,
            comment="The date and time when the import entered the stage",
        ),
        sa.PrimaryKeyConstraint("hive_db", "hive_table", "stage"),
        comment='The import tool keeps track of how far in the import the tool have succeeded. So in case of an error, lets say that Hive is not responding, the next time an import is executed it will skip the first part and continue from where it ended in error on the previous run. If you want to rerun from the begining, the information in this table needs to be cleared. This is done with the "manage --clearImportStage" tool. Keep in mind that clearing the stage of an incremental import might result in the loss of the data.',
    )
    op.create_table(
        "import_stage_statistics",
        sa.Column(
            "hive_db", sa.String(length=256), nullable=False, comment="Hive Database"
        ),
        sa.Column(
            "hive_table", sa.String(length=256), nullable=False, comment="Hive Table"
        ),
        sa.Column(
            "stage",
            sa.Integer(),
            nullable=False,
            comment="Current stage of the import. This is an internal stage and has nothing to do with stage1 and stage2 in Airflow DAG's",
        ),
        sa.Column(
            "start", sa.DateTime(), nullable=False, comment="Time when stage started"
        ),
        sa.Column(
            "stop",
            sa.DateTime(),
            nullable=False,
            comment="Time when stage was completed",
        ),
        sa.Column(
            "duration", sa.Integer(), nullable=False, comment="Duration of stage"
        ),
        sa.PrimaryKeyConstraint("hive_db", "hive_table", "stage"),
        comment="As DBImport progress through the different stages of the import, it also keeps track of start and stop time for each stage together with the duration. That information is kept in this table",
    )
    op.create_table(
        "import_statistics",
        sa.Column(
            "id",
            mysql.BIGINT(display_width=20),
            autoincrement=True,
            nullable=False,
            comment="Auto incremented PrimaryKey of the table",
        ),
        sa.Column(
            "hive_db", sa.String(length=256), nullable=True, comment="Hive Database"
        ),
        sa.Column(
            "hive_table", sa.String(length=256), nullable=True, comment="Hive Table"
        ),
        sa.Column(
            "importtype",
            sa.String(length=32),
            nullable=True,
            comment="What kind of import type that was used",
        ),
        sa.Column(
            "import_phase",
            sa.String(length=32),
            nullable=True,
            comment="Import Phase method",
        ),
        sa.Column(
            "copy_phase",
            sa.String(length=32),
            nullable=True,
            comment="Copy Phase method",
        ),
        sa.Column(
            "etl_phase", sa.String(length=32), nullable=True, comment="ETL Phase method"
        ),
        sa.Column(
            "incremental",
            mysql.TINYINT(display_width=4),
            nullable=True,
            comment="0 = Full import, 1 = Incremental import",
        ),
        sa.Column(
            "dbalias",
            sa.String(length=256),
            nullable=True,
            comment="ID of the Database Connection",
        ),
        sa.Column(
            "source_database",
            sa.String(length=256),
            nullable=True,
            comment="Name of the source database",
        ),
        sa.Column(
            "source_schema",
            sa.String(length=256),
            nullable=True,
            comment="Name of the source schema",
        ),
        sa.Column(
            "source_table",
            sa.String(length=256),
            nullable=True,
            comment="Name of the source table",
        ),
        sa.Column(
            "rows",
            mysql.BIGINT(display_width=20),
            nullable=True,
            comment="How many rows that was imported",
        ),
        sa.Column(
            "size",
            mysql.BIGINT(display_width=20),
            nullable=True,
            comment="The total size in bytes that was imported",
        ),
        sa.Column(
            "sessions",
            mysql.TINYINT(display_width=4),
            nullable=True,
            comment="How many parallell sessions was used against the source (sqoop mappers)",
        ),
        sa.Column(
            "duration", sa.Integer(), nullable=True, comment="Tota duration in seconds"
        ),
        sa.Column("start", sa.DateTime(), nullable=True, comment="Timestamp of start"),
        sa.Column("stop", sa.DateTime(), nullable=True, comment="Timestamp of stop"),
        sa.Column("sqoop_duration", sa.Integer(), nullable=True),
        sa.Column("sqoop_start", sa.DateTime(), nullable=True),
        sa.Column("sqoop_stop", sa.DateTime(), nullable=True),
        sa.Column("clear_hive_locks_duration", sa.Integer(), nullable=True),
        sa.Column("clear_hive_locks_start", sa.DateTime(), nullable=True),
        sa.Column("clear_hive_locks_stop", sa.DateTime(), nullable=True),
        sa.Column("clear_table_rowcount_duration", sa.Integer(), nullable=True),
        sa.Column("clear_table_rowcount_start", sa.DateTime(), nullable=True),
        sa.Column("clear_table_rowcount_stop", sa.DateTime(), nullable=True),
        sa.Column("connect_to_hive_duration", sa.Integer(), nullable=True),
        sa.Column("connect_to_hive_start", sa.DateTime(), nullable=True),
        sa.Column("connect_to_hive_stop", sa.DateTime(), nullable=True),
        sa.Column("create_import_table_duration", sa.Integer(), nullable=True),
        sa.Column("create_import_table_start", sa.DateTime(), nullable=True),
        sa.Column("create_import_table_stop", sa.DateTime(), nullable=True),
        sa.Column("create_target_table_duration", sa.Integer(), nullable=True),
        sa.Column("create_target_table_start", sa.DateTime(), nullable=True),
        sa.Column("create_target_table_stop", sa.DateTime(), nullable=True),
        sa.Column("get_import_rowcount_duration", sa.Integer(), nullable=True),
        sa.Column("get_import_rowcount_start", sa.DateTime(), nullable=True),
        sa.Column("get_import_rowcount_stop", sa.DateTime(), nullable=True),
        sa.Column("get_source_rowcount_duration", sa.Integer(), nullable=True),
        sa.Column("get_source_rowcount_start", sa.DateTime(), nullable=True),
        sa.Column("get_source_rowcount_stop", sa.DateTime(), nullable=True),
        sa.Column("get_source_tableschema_duration", sa.Integer(), nullable=True),
        sa.Column("get_source_tableschema_start", sa.DateTime(), nullable=True),
        sa.Column("get_source_tableschema_stop", sa.DateTime(), nullable=True),
        sa.Column("get_target_rowcount_duration", sa.Integer(), nullable=True),
        sa.Column("get_target_rowcount_start", sa.DateTime(), nullable=True),
        sa.Column("get_target_rowcount_stop", sa.DateTime(), nullable=True),
        sa.Column("hive_import_duration", sa.Integer(), nullable=True),
        sa.Column("hive_import_start", sa.DateTime(), nullable=True),
        sa.Column("hive_import_stop", sa.DateTime(), nullable=True),
        sa.Column("truncate_target_table_duration", sa.Integer(), nullable=True),
        sa.Column("truncate_target_table_start", sa.DateTime(), nullable=True),
        sa.Column("truncate_target_table_stop", sa.DateTime(), nullable=True),
        sa.Column("merge_table_duration", sa.Integer(), nullable=True),
        sa.Column("merge_table_start", sa.DateTime(), nullable=True),
        sa.Column("merge_table_stop", sa.DateTime(), nullable=True),
        sa.Column("create_history_table_duration", sa.Integer(), nullable=True),
        sa.Column("create_history_table_start", sa.DateTime(), nullable=True),
        sa.Column("create_history_table_stop", sa.DateTime(), nullable=True),
        sa.Column("create_delete_table_duration", sa.Integer(), nullable=True),
        sa.Column("create_delete_table_start", sa.DateTime(), nullable=True),
        sa.Column("create_delete_table_stop", sa.DateTime(), nullable=True),
        sa.Column("update_statistics_duration", sa.Integer(), nullable=True),
        sa.Column("update_statistics_start", sa.DateTime(), nullable=True),
        sa.Column("update_statistics_stop", sa.DateTime(), nullable=True),
        sa.Column("validate_import_table_duration", sa.Integer(), nullable=True),
        sa.Column("validate_import_table_start", sa.DateTime(), nullable=True),
        sa.Column("validate_import_table_stop", sa.DateTime(), nullable=True),
        sa.Column("validate_sqoop_import_duration", sa.Integer(), nullable=True),
        sa.Column("validate_sqoop_import_start", sa.DateTime(), nullable=True),
        sa.Column("validate_sqoop_import_stop", sa.DateTime(), nullable=True),
        sa.Column("validate_target_table_duration", sa.Integer(), nullable=True),
        sa.Column("validate_target_table_start", sa.DateTime(), nullable=True),
        sa.Column("validate_target_table_stop", sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        comment="At the end of each import, all statistics about how long each part took aswell as general information about Hive datbase and table, number of rows imported, size of the import and much more are logged in this table. This table grows and will never be truncated by DBImport itself. If it becomes to large for you, it's up to each user to delete or truncate this table as you see fit.",
    )
    op.create_index(
        "hive_db_hive_table",
        "import_statistics",
        ["hive_db", "hive_table"],
        unique=False,
    )
    op.create_table(
        "import_statistics_last",
        sa.Column(
            "hive_db", sa.String(length=256), nullable=False, comment="Hive Database"
        ),
        sa.Column(
            "hive_table", sa.String(length=256), nullable=False, comment="Hive Table"
        ),
        sa.Column(
            "importtype",
            sa.String(length=32),
            nullable=True,
            comment="What kind of import type that was used",
        ),
        sa.Column(
            "import_phase",
            sa.String(length=32),
            nullable=True,
            comment="Import Phase method",
        ),
        sa.Column(
            "copy_phase",
            sa.String(length=32),
            nullable=True,
            comment="Copy Phase method",
        ),
        sa.Column(
            "etl_phase", sa.String(length=32), nullable=True, comment="ETL Phase method"
        ),
        sa.Column(
            "incremental",
            mysql.TINYINT(display_width=4),
            nullable=True,
            comment="0 = Full import, 1 = Incremental import",
        ),
        sa.Column(
            "dbalias",
            sa.String(length=256),
            nullable=True,
            comment="ID of the Database Connection",
        ),
        sa.Column(
            "source_database",
            sa.String(length=256),
            nullable=True,
            comment="Name of the source database",
        ),
        sa.Column(
            "source_schema",
            sa.String(length=256),
            nullable=True,
            comment="Name of the source schema",
        ),
        sa.Column(
            "source_table",
            sa.String(length=256),
            nullable=True,
            comment="Name of the source table",
        ),
        sa.Column(
            "rows",
            mysql.BIGINT(display_width=20),
            nullable=True,
            comment="How many rows that was imported",
        ),
        sa.Column(
            "size",
            mysql.BIGINT(display_width=20),
            nullable=True,
            comment="The total size in bytes that was imported",
        ),
        sa.Column(
            "sessions",
            mysql.TINYINT(display_width=4),
            nullable=True,
            comment="How many parallell sessions was used against the source (sqoop mappers)",
        ),
        sa.Column(
            "duration", sa.Integer(), nullable=True, comment="Tota duration in seconds"
        ),
        sa.Column("start", sa.DateTime(), nullable=True, comment="Timestamp of start"),
        sa.Column("stop", sa.DateTime(), nullable=True, comment="Timestamp of stop"),
        sa.Column("sqoop_duration", sa.Integer(), nullable=True),
        sa.Column("sqoop_start", sa.DateTime(), nullable=True),
        sa.Column("sqoop_stop", sa.DateTime(), nullable=True),
        sa.Column("clear_hive_locks_duration", sa.Integer(), nullable=True),
        sa.Column("clear_hive_locks_start", sa.DateTime(), nullable=True),
        sa.Column("clear_hive_locks_stop", sa.DateTime(), nullable=True),
        sa.Column("clear_table_rowcount_duration", sa.Integer(), nullable=True),
        sa.Column("clear_table_rowcount_start", sa.DateTime(), nullable=True),
        sa.Column("clear_table_rowcount_stop", sa.DateTime(), nullable=True),
        sa.Column("connect_to_hive_duration", sa.Integer(), nullable=True),
        sa.Column("connect_to_hive_start", sa.DateTime(), nullable=True),
        sa.Column("connect_to_hive_stop", sa.DateTime(), nullable=True),
        sa.Column("create_import_table_duration", sa.Integer(), nullable=True),
        sa.Column("create_import_table_start", sa.DateTime(), nullable=True),
        sa.Column("create_import_table_stop", sa.DateTime(), nullable=True),
        sa.Column("create_target_table_duration", sa.Integer(), nullable=True),
        sa.Column("create_target_table_start", sa.DateTime(), nullable=True),
        sa.Column("create_target_table_stop", sa.DateTime(), nullable=True),
        sa.Column("get_import_rowcount_duration", sa.Integer(), nullable=True),
        sa.Column("get_import_rowcount_start", sa.DateTime(), nullable=True),
        sa.Column("get_import_rowcount_stop", sa.DateTime(), nullable=True),
        sa.Column("get_source_rowcount_duration", sa.Integer(), nullable=True),
        sa.Column("get_source_rowcount_start", sa.DateTime(), nullable=True),
        sa.Column("get_source_rowcount_stop", sa.DateTime(), nullable=True),
        sa.Column("get_source_tableschema_duration", sa.Integer(), nullable=True),
        sa.Column("get_source_tableschema_start", sa.DateTime(), nullable=True),
        sa.Column("get_source_tableschema_stop", sa.DateTime(), nullable=True),
        sa.Column("get_target_rowcount_duration", sa.Integer(), nullable=True),
        sa.Column("get_target_rowcount_start", sa.DateTime(), nullable=True),
        sa.Column("get_target_rowcount_stop", sa.DateTime(), nullable=True),
        sa.Column("hive_import_duration", sa.Integer(), nullable=True),
        sa.Column("hive_import_start", sa.DateTime(), nullable=True),
        sa.Column("hive_import_stop", sa.DateTime(), nullable=True),
        sa.Column("truncate_target_table_duration", sa.Integer(), nullable=True),
        sa.Column("truncate_target_table_start", sa.DateTime(), nullable=True),
        sa.Column("truncate_target_table_stop", sa.DateTime(), nullable=True),
        sa.Column("merge_table_duration", sa.Integer(), nullable=True),
        sa.Column("merge_table_start", sa.DateTime(), nullable=True),
        sa.Column("merge_table_stop", sa.DateTime(), nullable=True),
        sa.Column("create_history_table_duration", sa.Integer(), nullable=True),
        sa.Column("create_history_table_start", sa.DateTime(), nullable=True),
        sa.Column("create_history_table_stop", sa.DateTime(), nullable=True),
        sa.Column("create_delete_table_duration", sa.Integer(), nullable=True),
        sa.Column("create_delete_table_start", sa.DateTime(), nullable=True),
        sa.Column("create_delete_table_stop", sa.DateTime(), nullable=True),
        sa.Column("update_statistics_duration", sa.Integer(), nullable=True),
        sa.Column("update_statistics_start", sa.DateTime(), nullable=True),
        sa.Column("update_statistics_stop", sa.DateTime(), nullable=True),
        sa.Column("validate_import_table_duration", sa.Integer(), nullable=True),
        sa.Column("validate_import_table_start", sa.DateTime(), nullable=True),
        sa.Column("validate_import_table_stop", sa.DateTime(), nullable=True),
        sa.Column("validate_sqoop_import_duration", sa.Integer(), nullable=True),
        sa.Column("validate_sqoop_import_start", sa.DateTime(), nullable=True),
        sa.Column("validate_sqoop_import_stop", sa.DateTime(), nullable=True),
        sa.Column("validate_target_table_duration", sa.Integer(), nullable=True),
        sa.Column("validate_target_table_start", sa.DateTime(), nullable=True),
        sa.Column("validate_target_table_stop", sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint("hive_db", "hive_table"),
        comment="The last entry in table import_statistics is also stored in this table. This makes it easier to find the latest data without first grouping to find the latest entry. When import_statistics table grows to a high number of million rows, it saves alot of cpu power for the database server.",
    )

    op.create_table(
        "import_tables",
        sa.Column(
            "hive_db",
            sa.String(length=256),
            nullable=False,
            comment="Hive Database to import to",
        ),
        sa.Column(
            "hive_table",
            sa.String(length=256),
            nullable=False,
            comment="Hive Table to import to",
        ),
        sa.Column(
            "table_id",
            sa.Integer(),
            autoincrement=True,
            nullable=False,
            comment="Unique identifier",
        ),
        sa.Column(
            "dbalias",
            sa.String(length=256),
            nullable=False,
            comment="Name of database connection from jdbc_connections table",
        ),
        sa.Column(
            "source_schema",
            sa.String(length=256),
            nullable=False,
            comment="Name of the schema in the remote database",
        ),
        sa.Column(
            "source_table",
            sa.String(length=256),
            nullable=False,
            comment="Name of the table in the remote database",
        ),
        sa.Column(
            "import_type",
            sa.String(length=32),
            server_default=sa.text("'full'"),
            nullable=False,
            comment="What import method to use",
        ),
        sa.Column(
            "last_update_from_source",
            sa.DateTime(),
            nullable=True,
            comment="Timestamp of last schema update from source",
        ),
        sa.Column(
            "sqoop_sql_where_addition",
            sa.String(length=1024),
            nullable=True,
            comment='Will be added AFTER the SQL WHERE. If it\'s an incr import, this will be after the incr limit statements. Example "orderId > 1000"',
        ),
        sa.Column(
            "nomerge_ingestion_sql_addition",
            sa.String(length=2048),
            nullable=True,
            comment="This will be added to the data ingestion of None-Merge imports (full, full_direct and incr). Usefull to filter out data from import tables to target tables",
        ),
        sa.Column(
            "include_in_airflow",
            mysql.TINYINT(display_width=4),
            server_default=sa.text("'1'"),
            nullable=False,
            comment="Will the table be included in Airflow DAG when it matches the DAG selection",
        ),
        sa.Column(
            "airflow_priority",
            mysql.TINYINT(display_width=4),
            nullable=True,
            comment="This will set priority_weight in Airflow",
        ),
        sa.Column(
            "validate_import",
            mysql.TINYINT(display_width=4),
            server_default=sa.text("'1'"),
            nullable=False,
            comment="Should the import be validated",
        ),
        sa.Column(
            "validate_source",
            sa.Enum("query", "sqoop"),
            server_default=sa.text("'query'"),
            nullable=True,
            comment="query = Run a 'select count(*) from ...' to get the number of rows in the source table. sqoop = Use the number of rows imported by sqoop as the number of rows in the source table",
        ),
        sa.Column(
            "validate_diff_allowed",
            mysql.BIGINT(display_width=20),
            server_default=sa.text("'-1'"),
            nullable=False,
            comment="-1 = auto calculated diff allowed. If a positiv number, this is the amount of rows that the diff is allowed to have",
        ),
        sa.Column(
            "truncate_hive",
            mysql.TINYINT(display_width=4),
            server_default=sa.text("'1'"),
            nullable=False,
            comment="Truncate Hive table before loading it. ",
        ),
        sa.Column(
            "mappers",
            mysql.TINYINT(display_width=4),
            server_default=sa.text("'-1'"),
            nullable=False,
            comment="-1 = auto or positiv number for a fixed number of mappers. If Auto, then it's calculated based of last sqoop import size",
        ),
        sa.Column(
            "soft_delete_during_merge",
            mysql.TINYINT(display_width=4),
            server_default=sa.text("'0'"),
            nullable=False,
            comment="If 1, then the row will be marked as deleted instead of actually being removed from the table. Only used for Merge imports",
        ),
        sa.Column(
            "source_rowcount",
            mysql.BIGINT(display_width=20),
            nullable=True,
            comment="Used for validation. Dont change manually",
        ),
        sa.Column(
            "source_rowcount_incr", mysql.BIGINT(display_width=20), nullable=True
        ),
        sa.Column(
            "hive_rowcount",
            mysql.BIGINT(display_width=20),
            nullable=True,
            comment="Used for validation. Dont change manually",
        ),
        sa.Column(
            "incr_mode",
            sa.String(length=16),
            nullable=True,
            comment="append or lastmodified",
        ),
        sa.Column(
            "incr_column",
            sa.String(length=256),
            nullable=True,
            comment="What column to use to identify new rows",
        ),
        sa.Column(
            "incr_validation_method",
            sa.Enum("full", "incr"),
            server_default=sa.text("'full'"),
            nullable=True,
            comment="full or incr. Full means that the validation will check to total number of rows up until maxvalue and compare source with target. Incr will only compare the rows between min and max value (the data that sqoop just wrote)",
        ),
        sa.Column(
            "incr_minvalue",
            sa.String(length=32),
            nullable=True,
            comment="Used for incremental imports. Dont change manually",
        ),
        sa.Column(
            "incr_maxvalue",
            sa.String(length=32),
            nullable=True,
            comment="Used for incremental imports. Dont change manually",
        ),
        sa.Column(
            "incr_minvalue_pending",
            sa.String(length=32),
            nullable=True,
            comment="Used for incremental imports. Dont change manually",
        ),
        sa.Column(
            "incr_maxvalue_pending",
            sa.String(length=32),
            nullable=True,
            comment="Used for incremental imports. Dont change manually",
        ),
        sa.Column(
            "pk_column_override",
            sa.String(length=1024),
            nullable=True,
            comment="Force the import and Hive table to define another PrimaryKey constraint. Comma separeted list of columns",
        ),
        sa.Column(
            "pk_column_override_mergeonly",
            sa.String(length=1024),
            nullable=True,
            comment="Force the import to use another PrimaryKey constraint during Merge operations. Comma separeted list of columns",
        ),
        sa.Column(
            "hive_merge_heap",
            sa.Integer(),
            nullable=True,
            comment="Should be a multiple of Yarn container size. If NULL then it will use the default specified in Yarn and TEZ",
        ),
        sa.Column(
            "concatenate_hive_table",
            mysql.TINYINT(display_width=4),
            server_default=sa.text("'-1'"),
            nullable=False,
            comment="<NOT USED>",
        ),
        sa.Column(
            "sqoop_query",
            sa.Text(),
            nullable=True,
            comment="Use a custom query in sqoop to read data from source table",
        ),
        sa.Column(
            "sqoop_options",
            sa.Text(),
            nullable=True,
            comment="Options to send to sqoop. Most common used for --split-by option",
        ),
        sa.Column(
            "sqoop_last_size",
            mysql.BIGINT(display_width=20),
            nullable=True,
            comment="Used to track sqoop operation. Dont change manually",
        ),
        sa.Column(
            "sqoop_last_rows",
            mysql.BIGINT(display_width=20),
            nullable=True,
            comment="Used to track sqoop operation. Dont change manually",
        ),
        sa.Column(
            "sqoop_last_mappers",
            mysql.TINYINT(display_width=4),
            nullable=True,
            comment="Used to track sqoop operation. Dont change manually",
        ),
        sa.Column(
            "sqoop_last_execution",
            mysql.BIGINT(display_width=20),
            nullable=True,
            comment="Used to track sqoop operation. Dont change manually",
        ),
        sa.Column(
            "sqoop_use_generated_sql",
            mysql.TINYINT(display_width=4),
            server_default=sa.text("'-1'"),
            nullable=False,
            comment="1 = Use the generated SQL that is saved in the generated_sqoop_query column",
        ),
        sa.Column(
            "sqoop_allow_text_splitter",
            mysql.TINYINT(display_width=4),
            server_default=sa.text("'0'"),
            nullable=False,
            comment="Allow splits on text columns. Use with caution",
        ),
        sa.Column(
            "force_string",
            mysql.TINYINT(display_width=4),
            server_default=sa.text("'-1'"),
            nullable=False,
            comment="If set to 1, all character based fields (char, varchar) will become string in Hive. Overrides the same setting in jdbc_connections table",
        ),
        sa.Column(
            "comment",
            sa.Text(),
            nullable=True,
            comment="Table comment from source system. Dont change manually",
        ),
        sa.Column(
            "generated_hive_column_definition",
            sa.Text(),
            nullable=True,
            comment="Generated column definition for Hive create table. Dont change manually",
        ),
        sa.Column(
            "generated_sqoop_query",
            sa.Text(),
            nullable=True,
            comment="Generated query for sqoop. Dont change manually",
        ),
        sa.Column(
            "generated_sqoop_options",
            sa.Text(),
            nullable=True,
            comment="Generated options for sqoop. Dont change manually",
        ),
        sa.Column(
            "generated_pk_columns",
            sa.Text(),
            nullable=True,
            comment="Generated Primary Keys. Dont change manually",
        ),
        sa.Column(
            "generated_foreign_keys", sa.Text(), nullable=True, comment="<NOT USED>"
        ),
        sa.Column(
            "datalake_source",
            sa.String(length=256),
            nullable=True,
            comment="This value will come in the dbimport_source column if present. Overrides the same setting in jdbc_connections table",
        ),
        sa.Column(
            "operator_notes",
            sa.Text(),
            nullable=True,
            comment="Free text field to write a note about the import. ",
        ),
        sa.PrimaryKeyConstraint("hive_db", "hive_table"),
        sa.UniqueConstraint("table_id"),
        comment="Main table where all tables that we can import are stored. ",
    )

    op.alter_column(
        "import_tables", "table_id", existing_type=sa.Integer(), autoincrement=True
    )

    op.create_index(
        "hive_db_dbalias_source_schema_source_table",
        "import_tables",
        ["hive_db", "dbalias", "source_schema", "source_table"],
        unique=False,
    )

    op.create_table(
        "jdbc_connections",
        sa.Column(
            "dbalias",
            sa.String(length=256),
            nullable=False,
            comment="Name of the Database connection",
        ),
        sa.Column(
            "private_key_path",
            sa.String(length=128),
            nullable=True,
            comment="<NOT USED>",
        ),
        sa.Column(
            "public_key_path",
            sa.String(length=128),
            nullable=True,
            comment="<NOT USED>",
        ),
        sa.Column("jdbc_url", sa.Text(), nullable=False, comment="The JDBC URL String"),
        sa.Column(
            "credentials",
            sa.Text(),
            nullable=True,
            comment="Encrypted fields for credentials.m Changed by the saveCredentialTool",
        ),
        sa.Column(
            "datalake_source",
            sa.String(length=256),
            nullable=True,
            comment="This value will come in the dbimport_source column if present. Priority is table, connection",
        ),
        sa.Column(
            "max_import_sessions",
            mysql.TINYINT(display_width=4),
            nullable=True,
            comment="You can limit the number of parallel sessions during import with this value. If NULL, then Max will come from configuration file",
        ),
        sa.Column(
            "force_string",
            mysql.TINYINT(display_width=4),
            server_default=sa.text("'0'"),
            nullable=False,
            comment="If set to 1, all character based fields (char, varchar) will become string in Hive",
        ),
        sa.Column(
            "create_datalake_import",
            mysql.TINYINT(display_width=4),
            server_default=sa.text("'1'"),
            nullable=False,
            comment="If set to 1, the datalake_import column will be created on all tables that is using this dbalias",
        ),
        sa.Column(
            "timewindow_start",
            sa.Time(),
            nullable=True,
            comment="Start of the time window when we are allowed to run against this connection.",
        ),
        sa.Column(
            "timewindow_stop",
            sa.Time(),
            nullable=True,
            comment="End of the time window when we are allowed to run against this connection.",
        ),
        sa.Column(
            "operator_notes",
            sa.Text(),
            nullable=True,
            comment="Free text field to write a note about the connection",
        ),
        sa.PrimaryKeyConstraint("dbalias"),
        comment="Database connection definitions",
    )
    op.create_table(
        "jdbc_connections_drivers",
        sa.Column(
            "database_type",
            sa.Enum(
                "DB2 AS400",
                "DB2 UDB",
                "MySQL",
                "Oracle",
                "PostgreSQL",
                "Progress DB",
                "SQL Server",
            ),
            nullable=False,
            comment="Name of database type.  Name is hardcoded into Python scripts, so use only defined values",
        ),
        sa.Column(
            "version",
            sa.String(length=16),
            nullable=False,
            comment="Free-text field with version. Has nothing to do with version of driver itself.",
        ),
        sa.Column(
            "driver",
            sa.String(length=128),
            nullable=False,
            comment="Java class for JDBC driver",
        ),
        sa.Column(
            "classpath",
            sa.String(length=255),
            nullable=False,
            comment="Full path to JDBC driver/jar file. If more than one file is required, separate them with : and no spaces",
        ),
        sa.PrimaryKeyConstraint("database_type", "version"),
    )
    op.create_table(
        "jdbc_table_change_history",
        sa.Column(
            "dbalias",
            sa.String(length=256),
            nullable=False,
            comment="Database connection name",
        ),
        sa.Column(
            "db_name", sa.String(length=256), nullable=False, comment="Database name"
        ),
        sa.Column(
            "schema_name", sa.String(length=256), nullable=False, comment="Schema Name"
        ),
        sa.Column(
            "table_name", sa.String(length=256), nullable=False, comment="Table Name"
        ),
        sa.Column(
            "column_name", sa.String(length=256), nullable=False, comment="Column Name"
        ),
        sa.Column(
            "eventtime",
            sa.DateTime(),
            nullable=False,
            comment="Time and date of the event",
        ),
        sa.Column(
            "event", sa.String(length=128), nullable=False, comment="Event name."
        ),
        sa.Column(
            "previous_value",
            sa.String(length=256),
            nullable=True,
            comment="Value before the change",
        ),
        sa.Column(
            "value",
            sa.String(length=256),
            nullable=True,
            comment="Value after the change",
        ),
        sa.Column(
            "description",
            sa.String(length=512),
            nullable=False,
            comment="Detailed description of the event",
        ),
        sa.PrimaryKeyConstraint("dbalias", "schema_name", "table_name", "eventtime"),
        comment="This table keeps track of all changes that was done to a JDBC table after the initial creation. Example could be that a colum type was changed from char(10) to varchar(10). That kind of information is logged in this table",
    )
    op.create_index(
        "dbalias_database_schema_table",
        "jdbc_table_change_history",
        ["dbalias", "db_name", "schema_name", "table_name"],
        unique=False,
    )
    op.create_table(
        "json_to_rest",
        sa.Column(
            "id",
            mysql.BIGINT(display_width=20),
            autoincrement=True,
            nullable=False,
            comment="Unique Identifier",
        ),
        sa.Column(
            "type",
            sa.String(length=50),
            nullable=False,
            comment="The type of JSON data that is saved in the 'jsondata' column",
        ),
        sa.Column(
            "create_time",
            sa.DateTime(),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
            comment="Time when the JSON data was created",
        ),
        sa.Column(
            "status",
            mysql.TINYINT(display_width=4),
            nullable=False,
            comment="Internal status to keep track of what the status of the transmissions is",
        ),
        sa.Column("jsondata", sa.Text(), nullable=False, comment="The payload to send"),
        sa.PrimaryKeyConstraint("id"),
        comment="Temporary storage of JSON payloads that will be sent to a REST interface if the tool is configured to do so.",
    )
    op.create_table(
        "table_change_history",
        sa.Column(
            "hive_db", sa.String(length=256), nullable=False, comment="Hive Database"
        ),
        sa.Column(
            "hive_table", sa.String(length=256), nullable=False, comment="Hive Table"
        ),
        sa.Column(
            "column_name", sa.String(length=256), nullable=False, comment="Column Name"
        ),
        sa.Column(
            "eventtime",
            sa.DateTime(),
            nullable=False,
            comment="Time and date of the event",
        ),
        sa.Column(
            "event", sa.String(length=128), nullable=False, comment="Event name."
        ),
        sa.Column(
            "previous_value",
            sa.String(length=256),
            nullable=True,
            comment="Value before the change",
        ),
        sa.Column(
            "value",
            sa.String(length=256),
            nullable=True,
            comment="Value after the change",
        ),
        sa.Column(
            "description",
            sa.String(length=512),
            nullable=False,
            comment="Detailed description of the event",
        ),
        sa.PrimaryKeyConstraint("hive_db", "hive_table", "eventtime"),
        comment="This table keeps track of all changes that was done to an abject after the initial load. Example could be that a colum type was changed from char(10) to varchar(10). That kind of information is logged in this table",
    )
    op.create_index(
        "hive_db_hive_table",
        "table_change_history",
        ["hive_db", "hive_table"],
        unique=False,
    )
    op.create_table(
        "airflow_import_dag_execution",
        sa.Column("dag_name", sa.String(length=64), nullable=False),
        sa.Column("task_name", sa.String(length=256), nullable=False),
        sa.Column("task_config", sa.Text(), nullable=False),
        sa.Column("executionid", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["executionid"],
            ["airflow_execution_type.executionid"],
        ),
        sa.PrimaryKeyConstraint("dag_name", "task_name"),
    )
    op.create_index(
        op.f("FK_airflow_import_dag_execution_airflow_execution_type"),
        "airflow_import_dag_execution",
        ["executionid"],
        unique=False,
    )
    op.create_table(
        "airflow_import_task_execution",
        sa.Column("hive_db", sa.String(length=256), nullable=False),
        sa.Column("hive_table", sa.String(length=256), nullable=False),
        sa.Column("stage", mysql.TINYINT(display_width=4), nullable=False),
        sa.Column("executionid", sa.Integer(), nullable=False),
        sa.Column("task_config", sa.Text(), nullable=False),
        sa.Column("task_name", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(
            ["executionid"],
            ["airflow_execution_type.executionid"],
        ),
        sa.PrimaryKeyConstraint("hive_db", "hive_table", "stage"),
    )
    op.create_index(
        op.f("FK_airflow_import_task_execution_airflow_execution_type"),
        "airflow_import_task_execution",
        ["executionid"],
        unique=False,
    )
    op.create_table(
        "airflow_tasks",
        sa.Column("dag_name", sa.String(length=64), nullable=False),
        sa.Column("task_name", sa.String(length=64), nullable=False),
        sa.Column(
            "task_type",
            sa.Enum("shell script", "Hive SQL Script", "JDBC SQL"),
            server_default=sa.text("'Hive SQL Script'"),
            nullable=False,
        ),
        sa.Column(
            "placement",
            sa.Enum("before main", "after main", "in main"),
            server_default=sa.text("'after main'"),
            nullable=False,
        ),
        sa.Column(
            "jdbc_dbalias",
            sa.String(length=256),
            nullable=True,
            comment="For  'JDBC SQL' Task Type, this specifies what database the SQL should run against",
        ),
        sa.Column("hive_db", sa.String(length=256), nullable=True),
        sa.Column("airflow_pool", sa.String(length=64), nullable=True),
        sa.Column("airflow_priority", mysql.TINYINT(display_width=4), nullable=True),
        sa.Column(
            "include_in_airflow",
            mysql.TINYINT(display_width=4),
            server_default=sa.text("'1'"),
            nullable=False,
        ),
        sa.Column("task_dependency_in_main", sa.String(length=256), nullable=True),
        sa.Column("task_config", sa.String(length=256), nullable=True),
        sa.ForeignKeyConstraint(
            ["jdbc_dbalias"],
            ["jdbc_connections.dbalias"],
        ),
        sa.PrimaryKeyConstraint("dag_name", "task_name"),
    )

    op.create_index(
        op.f("FK_airflow_tasks_jdbc_connections"),
        "airflow_tasks",
        ["jdbc_dbalias"],
        unique=False,
    )

    op.create_table(
        "export_columns",
        sa.Column(
            "table_id",
            sa.Integer(),
            nullable=False,
            comment="Foreign Key to export_tables column 'table_id'",
        ),
        sa.Column(
            "column_id",
            sa.Integer(),
            autoincrement=True,
            nullable=False,
            comment="Unique identifier",
        ),
        sa.Column(
            "column_name",
            sa.String(length=256),
            nullable=False,
            comment="Name of column in target table. Dont change this manually",
        ),
        sa.Column(
            "column_type",
            sa.String(length=2048),
            nullable=True,
            comment="Column type from Hive. Dont change this manually",
        ),
        sa.Column(
            "column_order",
            sa.Integer(),
            nullable=True,
            comment="The order of the columns. Dont change this manually",
        ),
        sa.Column(
            "hive_db",
            sa.String(length=256),
            nullable=True,
            comment="Only used to make it easier to read the table. No real usage",
        ),
        sa.Column(
            "hive_table",
            sa.String(length=256),
            nullable=True,
            comment="Only used to make it easier to read the table. No real usage",
        ),
        sa.Column(
            "target_column_name",
            sa.String(length=256),
            nullable=True,
            comment="Override the name of column in the target system",
        ),
        sa.Column(
            "target_column_type",
            sa.String(length=256),
            nullable=True,
            comment="Override the column type in the target system",
        ),
        sa.Column(
            "last_update_from_hive",
            sa.DateTime(),
            nullable=False,
            comment="Timestamp of last schema update from Hive. Dont change this manually",
        ),
        sa.Column(
            "last_export_time",
            sa.DateTime(),
            nullable=True,
            comment="Timestamp of last export. Dont change this manually",
        ),
        sa.Column(
            "selection", sa.String(length=256), nullable=True, comment="<NOT USED>"
        ),
        sa.Column(
            "include_in_export",
            mysql.TINYINT(display_width=4),
            server_default=sa.text("'1'"),
            nullable=False,
            comment="1 = Include column in export, 0 = Exclude column in export",
        ),
        sa.Column(
            "comment",
            sa.Text(),
            nullable=True,
            comment="The column comment from the source system. Dont change this manually",
        ),
        sa.Column(
            "operator_notes",
            sa.Text(),
            nullable=True,
            comment="Free text field to write a note about the import. ",
        ),
        sa.ForeignKeyConstraint(
            ["table_id"], ["export_tables.table_id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("table_id", "column_id", "column_name"),
        sa.UniqueConstraint("column_id"),
        comment="This table contains all columns that exists on all tables that we are exporting. Unlike the export_tables table, this one gets created automatically by the export tool",
    )
    op.create_table(
        "import_columns",
        sa.Column(
            "table_id",
            sa.Integer(),
            nullable=False,
            comment="Foreign Key to import_tables column 'table_id'",
        ),
        sa.Column(
            "column_id",
            sa.Integer(),
            autoincrement=True,
            nullable=False,
            comment="Unique identifier of the column",
        ),
        sa.Column(
            "column_order",
            sa.Integer(),
            nullable=True,
            comment="In what order does the column exist in the source system. ",
        ),
        sa.Column(
            "column_name",
            sa.String(length=256),
            nullable=False,
            comment="Name of column in Hive. Dont change this manually",
        ),
        sa.Column(
            "hive_db", sa.String(length=256), nullable=True, comment="Hive Database"
        ),
        sa.Column(
            "hive_table", sa.String(length=256), nullable=True, comment="Hive Table"
        ),
        sa.Column(
            "source_column_name",
            sa.String(length=256),
            nullable=False,
            comment="Name of column in source system. Dont change this manually",
        ),
        sa.Column(
            "column_type",
            sa.String(length=2048),
            nullable=False,
            comment="Column type in Hive. Dont change this manually",
        ),
        sa.Column(
            "source_column_type",
            sa.String(length=2048),
            nullable=False,
            comment="Column type in source system. Dont change this manually",
        ),
        sa.Column(
            "source_database_type",
            sa.String(length=256),
            nullable=True,
            comment="That database type was the column imported from",
        ),
        sa.Column(
            "column_name_override",
            sa.String(length=256),
            nullable=True,
            comment="Set a custom name of the column in Hive",
        ),
        sa.Column(
            "column_type_override",
            sa.String(length=256),
            nullable=True,
            comment="Set a custom column type in Hive",
        ),
        sa.Column(
            "sqoop_column_type",
            sa.String(length=256),
            nullable=True,
            comment="Used to create a correct --map-column-java setting for sqoop. ",
        ),
        sa.Column(
            "force_string",
            mysql.TINYINT(display_width=4),
            server_default=sa.text("'-1'"),
            nullable=False,
            comment="If set to 1, all character based fields (char, varchar) will become string in Hive. Overrides the same setting in import_tables and jdbc_connections table",
        ),
        sa.Column(
            "include_in_import",
            mysql.TINYINT(display_width=4),
            server_default=sa.text("'1'"),
            nullable=False,
            comment="1 = Include column in import, 0 = Exclude column in import",
        ),
        sa.Column(
            "source_primary_key",
            mysql.TINYINT(display_width=4),
            nullable=True,
            comment="Number starting from 1 listing the order of the column in the PK. Dont change this manually",
        ),
        sa.Column(
            "last_update_from_source",
            sa.DateTime(),
            nullable=False,
            comment="Timestamp of last schema update from source",
        ),
        sa.Column(
            "comment",
            sa.Text(),
            nullable=True,
            comment="The column comment from the source system",
        ),
        sa.Column(
            "operator_notes",
            sa.Text(),
            nullable=True,
            comment="Free text field to write a note about the column",
        ),
        sa.ForeignKeyConstraint(
            ["table_id"], ["import_tables.table_id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("table_id", "column_id", "column_name"),
        sa.UniqueConstraint("column_id"),
        comment="This table contains all columns that exists on all tables that we are importing. Unlike the import_tables table, this one gets created automatically by the 'Get Source TableSchema' stage. ",
    )
    op.create_index(
        "table_id_source_column_name",
        "import_columns",
        ["table_id", "source_column_name"],
        unique=False,
    )
    op.create_table(
        "import_foreign_keys",
        sa.Column(
            "table_id",
            sa.Integer(),
            nullable=False,
            comment="Table ID in import_tables that have the FK",
        ),
        sa.Column(
            "column_id",
            sa.Integer(),
            nullable=False,
            comment="Column ID in import_columns that have the FK",
        ),
        sa.Column("fk_index", sa.Integer(), nullable=False, comment="Index of FK"),
        sa.Column(
            "fk_table_id",
            sa.Integer(),
            nullable=False,
            comment="Table ID in import_tables that the table is having a reference against",
        ),
        sa.Column(
            "fk_column_id",
            sa.Integer(),
            nullable=False,
            comment="Column ID in import_columns that the table is having a reference against",
        ),
        sa.Column(
            "key_position", sa.Integer(), nullable=False, comment="Position of the key"
        ),
        sa.ForeignKeyConstraint(
            ["column_id"],
            ["import_columns.column_id"],
            name="FK_import_foreign_keys_import_columns",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["fk_column_id"],
            ["import_columns.column_id"],
            name="FK_import_foreign_keys_import_columns_fk",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("table_id", "column_id", "fk_index"),
        comment="All foreign key definitions is saved in this table. The information in this table is recreated all the time, so no manually changes are allowed here. For a better understanding of this table, please use the view called import_foreign_keys_view instead",
    )
    op.create_index(
        "FK_table_id_column_id",
        "import_foreign_keys",
        ["fk_table_id", "fk_column_id"],
        unique=False,
    )
    # ### end Alembic commands ###


def downgrade():
    pass


#    # ### commands auto generated by Alembic - please adjust! ###
#    op.drop_table('import_foreign_keys')
#    op.drop_table('import_columns')
#    op.drop_table('export_columns')
#    op.drop_table('airflow_tasks')
#    op.drop_table('airflow_import_task_execution')
#    op.drop_table('airflow_import_dag_execution')
#    op.drop_table('table_change_history')
#    op.drop_table('json_to_rest')
#    op.drop_table('jdbc_table_change_history')
#    op.drop_table('jdbc_connections_drivers')
#    op.drop_table('jdbc_connections')
#    op.drop_table('import_tables')
#    op.drop_table('import_statistics_last')
#    op.drop_table('import_statistics')
#    op.drop_table('import_stage_statistics')
#    op.drop_table('import_stage')
#    op.drop_table('import_retries_log')
#    op.drop_table('import_failure_log')
#    op.drop_table('export_tables')
#    op.drop_table('export_statistics_last')
#    op.drop_table('export_statistics')
#    op.drop_table('export_stage_statistics')
#    op.drop_table('export_stage')
#    op.drop_table('export_retries_log')
#    op.drop_table('etl_jobs')
#    op.drop_table('db_version')
#    op.drop_table('configuration')
#    op.drop_table('auto_discovered_tables')
#    op.drop_table('airflow_import_dags')
#    op.drop_table('airflow_export_dags')
#    op.drop_table('airflow_execution_type')
#    op.drop_table('airflow_etl_dags')
#    op.drop_table('airflow_dag_sensors')
#    op.drop_table('airflow_custom_dags')
#    # ### end Alembic commands ###
