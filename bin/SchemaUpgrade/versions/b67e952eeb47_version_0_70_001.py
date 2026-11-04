"""Version 0.70.001

Revision ID: b67e952eeb47
Revises: 82bb45ccaca2
Create Date: 2022-06-07 11:13:00.589011

"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql
from sqlalchemy import Enum


# revision identifiers, used by Alembic.
revision = "b67e952eeb47"
down_revision = "82bb45ccaca2"
branch_labels = None
depends_on = None


def upgrade():
    op.alter_column(
        "import_tables",
        "etl_phase_type",
        existing_type=Enum(
            "truncate_insert", "insert", "merge", "merge_history_audit", "none"
        ),
        type_=Enum(
            "truncate_insert",
            "insert",
            "merge",
            "merge_history_audit",
            "none",
            "external",
        ),
        comment="What method to use for ETL phase",
        existing_nullable=True,
    )


def downgrade():
    op.alter_column(
        "import_tables",
        "etl_phase_type",
        existing_type=Enum(
            "truncate_insert",
            "insert",
            "merge",
            "merge_history_audit",
            "none",
            "external",
        ),
        type_=Enum("truncate_insert", "insert", "merge", "merge_history_audit", "none"),
        comment="What method to use for ETL phase",
        existing_nullable=True,
    )
