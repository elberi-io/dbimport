"""Version 0.80.009

Revision ID: a5899e8b06a9
Revises: af71422a72a2
Create Date: 2024-03-22 04:34:16.998040

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy import Enum
from sqlalchemy.dialects import mysql

# revision identifiers, used by Alembic.
revision = "a5899e8b06a9"
down_revision = "af71422a72a2"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "auth_users",
        sa.Column(
            "username",
            sa.String(length=256),
            nullable=False,
            comment="Username of the account",
        ),
        sa.Column(
            "password",
            sa.String(length=256),
            nullable=True,
            comment="Password. External accounts are not encrypted",
        ),
        sa.Column(
            "disabled",
            mysql.TINYINT(display_width=4),
            server_default=sa.text("'0'"),
            nullable=False,
            comment="0 = Account is enabled. 1 = Account is disabled",
        ),
        sa.Column(
            "fullname",
            sa.String(length=255),
            nullable=False,
            server_default=sa.text("''"),
            comment="Users full name. Will be displayed in various menus and logs",
        ),
        sa.Column(
            "department",
            sa.String(length=255),
            nullable=False,
            server_default=sa.text("''"),
            comment="Users department. Information only",
        ),
        sa.Column(
            "email",
            sa.String(length=255),
            nullable=False,
            server_default=sa.text("''"),
            comment="Users email. Information only",
        ),
        sa.PrimaryKeyConstraint("username"),
        comment="User database for rest interface and ui",
    )

    op.alter_column(
        "airflow_tasks",
        "task_type",
        existing_type=Enum(
            "shell script",
            "Hive SQL",
            "Hive SQL Script",
            "JDBC SQL",
            "Trigger DAG",
            "DAG Sensor",
            "SQL Sensor",
        ),
        type_=Enum(
            "shell script",
            "Hive SQL",
            "Hive SQL Script",
            "JDBC SQL",
            "Trigger DAG",
            "DAG Sensor",
            "SQL Sensor",
            "DBImport command",
        ),
        comment="The type of the Task",
        existing_nullable=False,
        existing_server_default=sa.text("'Hive SQL Script'"),
    )


def downgrade():
    op.alter_column(
        "airflow_tasks",
        "task_type",
        existing_type=Enum(
            "shell script",
            "Hive SQL",
            "Hive SQL Script",
            "JDBC SQL",
            "Trigger DAG",
            "DAG Sensor",
            "SQL Sensor",
            "DBImport command",
        ),
        type_=Enum(
            "shell script",
            "Hive SQL",
            "Hive SQL Script",
            "JDBC SQL",
            "Trigger DAG",
            "DAG Sensor",
            "SQL Sensor",
        ),
        comment="The type of the Task",
        existing_nullable=False,
        existing_server_default=sa.text("'Hive SQL Script'"),
    )

    op.drop_table("auth_users")
