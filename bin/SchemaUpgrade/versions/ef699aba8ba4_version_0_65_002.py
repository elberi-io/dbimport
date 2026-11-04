"""Version 0.65.002

Revision ID: ef699aba8ba4
Revises: c8582887a25f
Create Date: 2019-11-09 07:07:10.795416

"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql
from sqlalchemy import Enum


# revision identifiers, used by Alembic.
revision = "ef699aba8ba4"
down_revision = "c8582887a25f"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "export_statistics",
        sa.Column("atlas_schema_duration", sa.Integer(), nullable=True),
    )
    op.add_column(
        "export_statistics",
        sa.Column("atlas_schema_start", sa.DateTime(), nullable=True),
    )
    op.add_column(
        "export_statistics",
        sa.Column("atlas_schema_stop", sa.DateTime(), nullable=True),
    )
    op.add_column(
        "export_statistics_last",
        sa.Column("atlas_schema_duration", sa.Integer(), nullable=True),
    )
    op.add_column(
        "export_statistics_last",
        sa.Column("atlas_schema_start", sa.DateTime(), nullable=True),
    )
    op.add_column(
        "export_statistics_last",
        sa.Column("atlas_schema_stop", sa.DateTime(), nullable=True),
    )


def downgrade():
    op.drop_column("export_statistics", "atlas_schema_duration")
    op.drop_column("export_statistics", "atlas_schema_start")
    op.drop_column("export_statistics", "atlas_schema_stop")
    op.drop_column("export_statistics_last", "atlas_schema_duration")
    op.drop_column("export_statistics_last", "atlas_schema_start")
    op.drop_column("export_statistics_last", "atlas_schema_stop")
