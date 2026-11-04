"""Version 0.68.007

Revision ID: 4c7ab7d3b46c
Revises: e317884da580
Create Date: 2021-10-22 06:59:47.134546

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import mysql

# revision identifiers, used by Alembic.
revision = "4c7ab7d3b46c"
down_revision = "e317884da580"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "airflow_tasks",
        sa.Column(
            "sensor_soft_fail",
            sa.Integer(),
            nullable=True,
            comment="Setting this to 1 will add soft_fail=True on sensor",
        ),
    )
    op.execute(
        "ALTER TABLE `airflow_tasks` CHANGE COLUMN `sensor_soft_fail` `sensor_soft_fail` INTEGER NULL COMMENT 'Setting this to 1 will add soft_fail=True on sensor' AFTER `sensor_timeout_minutes`"
    )

    op.add_column(
        "airflow_dag_sensors",
        sa.Column(
            "sensor_soft_fail",
            sa.Integer(),
            nullable=True,
            comment="Setting this to 1 will add soft_fail=True on sensor",
        ),
    )

    op.alter_column(
        "airflow_custom_dags",
        "schedule_interval",
        existing_type=mysql.VARCHAR(length=32),
        type_=mysql.VARCHAR(length=128),
        comment="Time to execute dag",
        existing_nullable=False,
    )

    op.alter_column(
        "airflow_etl_dags",
        "schedule_interval",
        existing_type=mysql.VARCHAR(length=32),
        type_=mysql.VARCHAR(length=128),
        comment="Time to execute dag",
        existing_nullable=False,
    )

    op.alter_column(
        "airflow_export_dags",
        "schedule_interval",
        existing_type=mysql.VARCHAR(length=32),
        type_=mysql.VARCHAR(length=128),
        comment="Time to execute dag",
        existing_nullable=False,
    )

    op.alter_column(
        "airflow_import_dags",
        "schedule_interval",
        existing_type=mysql.VARCHAR(length=32),
        type_=mysql.VARCHAR(length=128),
        comment="Time to execute dag",
        existing_nullable=False,
    )


def downgrade():
    op.drop_column("airflow_tasks", "sensor_soft_fail")
    op.drop_column("airflow_dag_sensors", "sensor_soft_fail")

    op.alter_column(
        "airflow_custom_dags",
        "schedule_interval",
        existing_type=mysql.VARCHAR(length=128),
        type_=mysql.VARCHAR(length=32),
        comment="Time to execute dag",
        existing_nullable=False,
    )

    op.alter_column(
        "airflow_etl_dags",
        "schedule_interval",
        existing_type=mysql.VARCHAR(length=128),
        type_=mysql.VARCHAR(length=32),
        comment="Time to execute dag",
        existing_nullable=False,
    )

    op.alter_column(
        "airflow_export_dags",
        "schedule_interval",
        existing_type=mysql.VARCHAR(length=128),
        type_=mysql.VARCHAR(length=32),
        comment="Time to execute dag",
        existing_nullable=False,
    )

    op.alter_column(
        "airflow_import_dags",
        "schedule_interval",
        existing_type=mysql.VARCHAR(length=128),
        type_=mysql.VARCHAR(length=32),
        comment="Time to execute dag",
        existing_nullable=False,
    )
