"""Add copy statistics columns

Revision ID: 07d17bb3b23b
Revises: 29d4b35bd4ca
Create Date: 2019-06-24 08:46:49.587529

"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql

# revision identifiers, used by Alembic.
revision = "07d17bb3b23b"
down_revision = "29d4b35bd4ca"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "import_statistics",
        sa.Column("copy_data_duration", sa.Integer(), nullable=True),
    )
    op.add_column(
        "import_statistics", sa.Column("copy_data_start", sa.DateTime(), nullable=True)
    )
    op.add_column(
        "import_statistics", sa.Column("copy_data_stop", sa.DateTime(), nullable=True)
    )
    op.add_column(
        "import_statistics",
        sa.Column("copy_schema_duration", sa.Integer(), nullable=True),
    )
    op.add_column(
        "import_statistics",
        sa.Column("copy_schema_start", sa.DateTime(), nullable=True),
    )
    op.add_column(
        "import_statistics", sa.Column("copy_schema_stop", sa.DateTime(), nullable=True)
    )
    op.add_column(
        "import_statistics_last",
        sa.Column("copy_data_duration", sa.Integer(), nullable=True),
    )
    op.add_column(
        "import_statistics_last",
        sa.Column("copy_data_start", sa.DateTime(), nullable=True),
    )
    op.add_column(
        "import_statistics_last",
        sa.Column("copy_data_stop", sa.DateTime(), nullable=True),
    )
    op.add_column(
        "import_statistics_last",
        sa.Column("copy_schema_duration", sa.Integer(), nullable=True),
    )
    op.add_column(
        "import_statistics_last",
        sa.Column("copy_schema_start", sa.DateTime(), nullable=True),
    )
    op.add_column(
        "import_statistics_last",
        sa.Column("copy_schema_stop", sa.DateTime(), nullable=True),
    )
    op.add_column(
        "export_statistics",
        sa.Column("update_statistics_duration", sa.Integer(), nullable=True),
    )
    op.add_column(
        "export_statistics",
        sa.Column("update_statistics_start", sa.DateTime(), nullable=True),
    )
    op.add_column(
        "export_statistics",
        sa.Column("update_statistics_stop", sa.DateTime(), nullable=True),
    )
    op.add_column(
        "export_statistics_last",
        sa.Column("update_statistics_duration", sa.Integer(), nullable=True),
    )
    op.add_column(
        "export_statistics_last",
        sa.Column("update_statistics_start", sa.DateTime(), nullable=True),
    )
    op.add_column(
        "export_statistics_last",
        sa.Column("update_statistics_stop", sa.DateTime(), nullable=True),
    )
    op.alter_column(
        "import_tables",
        "truncate_hive",
        existing_type=mysql.TINYINT(display_width=4),
        comment="<NOT USED>",
        existing_comment="Truncate Hive table before loading it. ",
        existing_nullable=False,
        existing_server_default=sa.text("'1'"),
    )


def downgrade():
    op.alter_column(
        "import_tables",
        "truncate_hive",
        existing_type=mysql.TINYINT(display_width=4),
        comment="Truncate Hive table before loading it. ",
        existing_comment="<NOT USED>",
        existing_nullable=False,
        existing_server_default=sa.text("'1'"),
    )
    op.drop_column("import_statistics_last", "copy_schema_stop")
    op.drop_column("import_statistics_last", "copy_schema_start")
    op.drop_column("import_statistics_last", "copy_schema_duration")
    op.drop_column("import_statistics_last", "copy_data_stop")
    op.drop_column("import_statistics_last", "copy_data_start")
    op.drop_column("import_statistics_last", "copy_data_duration")
    op.drop_column("import_statistics", "copy_schema_stop")
    op.drop_column("import_statistics", "copy_schema_start")
    op.drop_column("import_statistics", "copy_schema_duration")
    op.drop_column("import_statistics", "copy_data_stop")
    op.drop_column("import_statistics", "copy_data_start")
    op.drop_column("import_statistics", "copy_data_duration")
    op.drop_column("export_statistics", "update_statistics_duration")
    op.drop_column("export_statistics", "update_statistics_start")
    op.drop_column("export_statistics", "update_statistics_stop")
    op.drop_column("export_statistics_last", "update_statistics_duration")
    op.drop_column("export_statistics_last", "update_statistics_start")
    op.drop_column("export_statistics_last", "update_statistics_stop")
