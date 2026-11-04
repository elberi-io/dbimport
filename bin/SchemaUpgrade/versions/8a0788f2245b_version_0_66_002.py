"""Version 0.66.002

Revision ID: 8a0788f2245b
Revises: f33e544af3e0
Create Date: 2020-10-02 03:33:17.120549

"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql
from sqlalchemy import Enum

# revision identifiers, used by Alembic.
revision = "8a0788f2245b"
down_revision = "f33e544af3e0"
branch_labels = None
depends_on = None


def upgrade():
    # import_tables
    op.alter_column(
        "import_tables",
        "validationCustomQuerySourceSQL",
        existing_type=sa.String(length=256),
        type_=sa.Text(),
        comment="Custom SQL query for source table",
        existing_nullable=True,
    )

    op.alter_column(
        "import_tables",
        "validationCustomQueryHiveSQL",
        existing_type=sa.String(length=256),
        type_=sa.Text(),
        comment="Custom SQL query for Hive table",
        existing_nullable=True,
    )

    op.alter_column(
        "import_tables",
        "validationCustomQuerySourceValue",
        existing_type=sa.String(length=512),
        type_=sa.Text(),
        comment="Used for validation. Dont change manually",
        existing_nullable=True,
    )

    op.alter_column(
        "import_tables",
        "validationCustomQueryHiveValue",
        existing_type=sa.String(length=512),
        type_=sa.Text(),
        comment="Used for validation. Dont change manually",
        existing_nullable=True,
    )

    # export_tables
    op.alter_column(
        "export_tables",
        "validationCustomQueryTargetSQL",
        existing_type=sa.String(length=256),
        type_=sa.Text(),
        comment="Custom SQL query for target table",
        existing_nullable=True,
    )

    op.alter_column(
        "export_tables",
        "validationCustomQueryHiveSQL",
        existing_type=sa.String(length=256),
        type_=sa.Text(),
        comment="Custom SQL query for Hive table",
        existing_nullable=True,
    )

    op.alter_column(
        "export_tables",
        "validationCustomQueryTargetValue",
        existing_type=sa.String(length=512),
        type_=sa.Text(),
        comment="Used for validation. Dont change manually",
        existing_nullable=True,
    )

    op.alter_column(
        "export_tables",
        "validationCustomQueryHiveValue",
        existing_type=sa.String(length=512),
        type_=sa.Text(),
        comment="Used for validation. Dont change manually",
        existing_nullable=True,
    )

    # Airflow DAG tables
    op.add_column(
        "airflow_import_dags",
        sa.Column(
            "sudo_user",
            sa.String(length=64),
            nullable=True,
            comment="All tasks in DAG will use this user for sudo instead of default",
        ),
    )
    op.add_column(
        "airflow_export_dags",
        sa.Column(
            "sudo_user",
            sa.String(length=64),
            nullable=True,
            comment="All tasks in DAG will use this user for sudo instead of default",
        ),
    )
    op.add_column(
        "airflow_etl_dags",
        sa.Column(
            "sudo_user",
            sa.String(length=64),
            nullable=True,
            comment="All tasks in DAG will use this user for sudo instead of default",
        ),
    )
    op.add_column(
        "airflow_custom_dags",
        sa.Column(
            "sudo_user",
            sa.String(length=64),
            nullable=True,
            comment="All tasks in DAG will use this user for sudo instead of default",
        ),
    )

    # Airflow Task table
    op.add_column(
        "airflow_tasks",
        sa.Column(
            "sudo_user",
            sa.String(length=64),
            nullable=True,
            comment="The task will use this user for sudo instead of default",
        ),
    )


def downgrade():
    # import_tables
    op.alter_column(
        "import_tables",
        "validationCustomQuerySourceSQL",
        existing_type=sa.Text(),
        type_=sa.String(length=256),
        comment="Custom SQL query for source table",
        existing_nullable=True,
    )

    op.alter_column(
        "import_tables",
        "validationCustomQueryHiveSQL",
        existing_type=sa.Text(),
        type_=sa.String(length=256),
        comment="Custom SQL query for Hive table",
        existing_nullable=True,
    )

    op.alter_column(
        "import_tables",
        "validationCustomQuerySourceValue",
        existing_type=sa.Text(),
        type_=sa.String(length=512),
        comment="Used for validation. Dont change manually",
        existing_nullable=True,
    )

    op.alter_column(
        "import_tables",
        "validationCustomQueryHiveValue",
        existing_type=sa.Text(),
        type_=sa.String(length=512),
        comment="Used for validation. Dont change manually",
        existing_nullable=True,
    )

    # export_tables
    op.alter_column(
        "export_tables",
        "validationCustomQueryTargetSQL",
        existing_type=sa.Text(),
        type_=sa.String(length=256),
        comment="Custom SQL query for target table",
        existing_nullable=True,
    )

    op.alter_column(
        "export_tables",
        "validationCustomQueryHiveSQL",
        existing_type=sa.Text(),
        type_=sa.String(length=256),
        comment="Custom SQL query for Hive table",
        existing_nullable=True,
    )

    op.alter_column(
        "export_tables",
        "validationCustomQueryTargetValue",
        existing_type=sa.Text(),
        type_=sa.String(length=512),
        comment="Used for validation. Dont change manually",
        existing_nullable=True,
    )

    op.alter_column(
        "export_tables",
        "validationCustomQueryHiveValue",
        existing_type=sa.Text(),
        type_=sa.String(length=512),
        comment="Used for validation. Dont change manually",
        existing_nullable=True,
    )

    # Airflow DAG tables
    op.drop_column("airflow_import_dags", "sudo_user")
    op.drop_column("airflow_export_dags", "sudo_user")
    op.drop_column("airflow_etl_dags", "sudo_user")
    op.drop_column("airflow_custom_dags", "sudo_user")

    # Airflow Task table
    op.drop_column("airflow_tasks", "sudo_user")
