"""Version 0.70.002

Revision ID: 77c42a5a9f39
Revises: b67e952eeb47
Create Date: 2022-06-09 06:51:41.442784

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import mysql

# revision identifiers, used by Alembic.
revision = "77c42a5a9f39"
down_revision = "b67e952eeb47"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "airflow_import_dags",
        sa.Column(
            "email",
            sa.String(length=256),
            nullable=True,
            comment="Email to send message to in case email_on_retry or email_on_failure is set to True. Comma separated list of email addresses",
        ),
    )
    op.add_column(
        "airflow_import_dags",
        sa.Column(
            "email_on_failure",
            mysql.TINYINT(display_width=4),
            server_default=sa.text("'0'"),
            nullable=False,
            comment="Send email on failures",
        ),
    )
    op.add_column(
        "airflow_import_dags",
        sa.Column(
            "email_on_retries",
            mysql.TINYINT(display_width=4),
            server_default=sa.text("'0'"),
            nullable=False,
            comment="Send email on retries",
        ),
    )

    op.add_column(
        "airflow_export_dags",
        sa.Column(
            "email",
            sa.String(length=256),
            nullable=True,
            comment="Email to send message to in case email_on_retry or email_on_failure is set to True. Comma separated list of email addresses",
        ),
    )
    op.add_column(
        "airflow_export_dags",
        sa.Column(
            "email_on_failure",
            mysql.TINYINT(display_width=4),
            server_default=sa.text("'0'"),
            nullable=False,
            comment="Send email on failures",
        ),
    )
    op.add_column(
        "airflow_export_dags",
        sa.Column(
            "email_on_retries",
            mysql.TINYINT(display_width=4),
            server_default=sa.text("'0'"),
            nullable=False,
            comment="Send email on retries",
        ),
    )

    op.add_column(
        "airflow_etl_dags",
        sa.Column(
            "email",
            sa.String(length=256),
            nullable=True,
            comment="Email to send message to in case email_on_retry or email_on_failure is set to True. Comma separated list of email addresses",
        ),
    )
    op.add_column(
        "airflow_etl_dags",
        sa.Column(
            "email_on_failure",
            mysql.TINYINT(display_width=4),
            server_default=sa.text("'0'"),
            nullable=False,
            comment="Send email on failures",
        ),
    )
    op.add_column(
        "airflow_etl_dags",
        sa.Column(
            "email_on_retries",
            mysql.TINYINT(display_width=4),
            server_default=sa.text("'0'"),
            nullable=False,
            comment="Send email on retries",
        ),
    )

    op.add_column(
        "airflow_custom_dags",
        sa.Column(
            "email",
            sa.String(length=256),
            nullable=True,
            comment="Email to send message to in case email_on_retry or email_on_failure is set to True. Comma separated list of email addresses",
        ),
    )
    op.add_column(
        "airflow_custom_dags",
        sa.Column(
            "email_on_failure",
            mysql.TINYINT(display_width=4),
            server_default=sa.text("'0'"),
            nullable=False,
            comment="Send email on failures",
        ),
    )
    op.add_column(
        "airflow_custom_dags",
        sa.Column(
            "email_on_retries",
            mysql.TINYINT(display_width=4),
            server_default=sa.text("'0'"),
            nullable=False,
            comment="Send email on retries",
        ),
    )


def downgrade():
    op.drop_column("airflow_import_dags", "email")
    op.drop_column("airflow_import_dags", "email_on_failure")
    op.drop_column("airflow_import_dags", "email_on_retries")

    op.drop_column("airflow_export_dags", "email")
    op.drop_column("airflow_export_dags", "email_on_failure")
    op.drop_column("airflow_export_dags", "email_on_retries")

    op.drop_column("airflow_etl_dags", "email")
    op.drop_column("airflow_etl_dags", "email_on_failure")
    op.drop_column("airflow_etl_dags", "email_on_retries")

    op.drop_column("airflow_custom_dags", "email")
    op.drop_column("airflow_custom_dags", "email_on_failure")
    op.drop_column("airflow_custom_dags", "email_on_retries")
