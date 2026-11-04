"""Change Task Type Enum

Revision ID: 8f0519d44032
Revises: b44c290dbe15
Create Date: 2019-06-17 07:24:03.271804

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy import Enum

# revision identifiers, used by Alembic.
revision = "8f0519d44032"
down_revision = "b44c290dbe15"
branch_labels = None
depends_on = None


def upgrade():
    op.alter_column(
        "airflow_tasks",
        "task_type",
        existing_type=Enum("shell script", "Hive SQL Script", "JDBC SQL"),
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
        ),
        type_=Enum("shell script", "Hive SQL Script", "JDBC SQL"),
        comment="The type of the Task",
        existing_nullable=False,
        existing_server_default=sa.text("'Hive SQL Script'"),
    )
