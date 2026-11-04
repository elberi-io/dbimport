"""Version 0.80.004

Revision ID: 67a2742fc1fb
Revises: 384c5573c1a9
Create Date: 2023-05-03 06:53:31.690970

"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql


# revision identifiers, used by Alembic.
revision = "67a2742fc1fb"
down_revision = "384c5573c1a9"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "airflow_import_dags",
        sa.Column(
            "retry_exponential_backoff",
            mysql.TINYINT(display_width=4),
            server_default=sa.text("'0'"),
            nullable=False,
            comment="1 = Use the retry_exponential_backoff Airflow function that will cause the retry between failed tasks to be longer and longer each time instead of a fixed time, 0 = Run with a fixed time of 5 min between the task retries",
        ),
    )
    op.add_column(
        "airflow_export_dags",
        sa.Column(
            "retry_exponential_backoff",
            mysql.TINYINT(display_width=4),
            server_default=sa.text("'0'"),
            nullable=False,
            comment="1 = Use the retry_exponential_backoff Airflow function that will cause the retry between failed tasks to be longer and longer each time instead of a fixed time, 0 = Run with a fixed time of 5 min between the task retries",
        ),
    )
    op.add_column(
        "airflow_custom_dags",
        sa.Column(
            "retry_exponential_backoff",
            mysql.TINYINT(display_width=4),
            server_default=sa.text("'0'"),
            nullable=False,
            comment="1 = Use the retry_exponential_backoff Airflow function that will cause the retry between failed tasks to be longer and longer each time instead of a fixed time, 0 = Run with a fixed time of 5 min between the task retries",
        ),
    )
    op.add_column(
        "airflow_etl_dags",
        sa.Column(
            "retry_exponential_backoff",
            mysql.TINYINT(display_width=4),
            server_default=sa.text("'0'"),
            nullable=False,
            comment="1 = Use the retry_exponential_backoff Airflow function that will cause the retry between failed tasks to be longer and longer each time instead of a fixed time, 0 = Run with a fixed time of 5 min between the task retries",
        ),
    )

    op.alter_column(
        "airflow_tasks",
        "task_dependency_in_main",
        new_column_name="task_dependency_upstream",
        existing_type=sa.String(length=256),
        existing_comment="Defines the upstream dependency for the Task. Comma separated list",
    )

    op.add_column(
        "airflow_tasks",
        sa.Column(
            "task_dependency_downstream",
            sa.String(length=256),
            comment="Defines the downstream dependency for the Task. Comma separated list",
        ),
    )
    op.execute(
        "ALTER TABLE `airflow_tasks` CHANGE COLUMN `task_dependency_downstream` `task_dependency_downstream` VARCHAR(256) COMMENT 'Defines the upstream dependency for the Task. Comma separated list' AFTER `task_dependency_upstream`"
    )


def downgrade():
    op.drop_column("airflow_import_dags", "retry_exponential_backoff")
    op.drop_column("airflow_export_dags", "retry_exponential_backoff")
    op.drop_column("airflow_custom_dags", "retry_exponential_backoff")
    op.drop_column("airflow_etl_dags", "retry_exponential_backoff")

    op.alter_column(
        "airflow_tasks",
        "task_dependency_upstream",
        new_column_name="task_dependency_in_main",
        existing_type=sa.String(length=256),
        existing_comment="If placement is In Main, this defines a dependency for the Task. Comma separated list",
    )
    op.drop_column("airflow_tasks", "task_dependency_downstream")
