"""Version 0.64.001

Revision ID: a4a53d72fd75
Revises: 0ac155cc2d5e
Create Date: 2019-10-25 06:13:13.913402

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "a4a53d72fd75"
down_revision = "0ac155cc2d5e"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "export_tables",
        sa.Column(
            "export_tool",
            sa.Enum("spark", "sqoop"),
            server_default=sa.text("'sqoop'"),
            nullable=False,
            comment="What tool should be used for exporting data",
        ),
    )
    op.add_column(
        "export_statistics",
        sa.Column("update_target_table_duration", sa.Integer(), nullable=True),
    )
    op.add_column(
        "export_statistics",
        sa.Column("update_target_table_start", sa.DateTime(), nullable=True),
    )
    op.add_column(
        "export_statistics",
        sa.Column("update_target_table_stop", sa.DateTime(), nullable=True),
    )
    op.add_column(
        "export_statistics_last",
        sa.Column("update_target_table_duration", sa.Integer(), nullable=True),
    )
    op.add_column(
        "export_statistics_last",
        sa.Column("update_target_table_start", sa.DateTime(), nullable=True),
    )
    op.add_column(
        "export_statistics_last",
        sa.Column("update_target_table_stop", sa.DateTime(), nullable=True),
    )
    op.add_column(
        "export_statistics", sa.Column("spark_duration", sa.Integer(), nullable=True)
    )
    op.add_column(
        "export_statistics", sa.Column("spark_start", sa.DateTime(), nullable=True)
    )
    op.add_column(
        "export_statistics", sa.Column("spark_stop", sa.DateTime(), nullable=True)
    )
    op.add_column(
        "export_statistics_last",
        sa.Column("spark_duration", sa.Integer(), nullable=True),
    )
    op.add_column(
        "export_statistics_last", sa.Column("spark_start", sa.DateTime(), nullable=True)
    )
    op.add_column(
        "export_statistics_last", sa.Column("spark_stop", sa.DateTime(), nullable=True)
    )


def downgrade():
    op.drop_column("export_tables", "export_tool")
    op.drop_column("export_statistics", "update_target_table_duration")
    op.drop_column("export_statistics", "update_target_table_start")
    op.drop_column("export_statistics", "update_target_table_stop")
    op.drop_column("export_statistics_last", "update_target_table_duration")
    op.drop_column("export_statistics_last", "update_target_table_start")
    op.drop_column("export_statistics_last", "update_target_table_stop")
    op.drop_column("export_statistics", "spark_duration")
    op.drop_column("export_statistics", "spark_start")
    op.drop_column("export_statistics", "spark_stop")
    op.drop_column("export_statistics_last", "spark_duration")
    op.drop_column("export_statistics_last", "spark_start")
    op.drop_column("export_statistics_last", "spark_stop")
