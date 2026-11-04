"""Version 0.66.003

Revision ID: 6f4b0fbc594c
Revises: 8a0788f2245b
Create Date: 2020-10-12 05:35:15.131754

"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql
from sqlalchemy import Enum

# revision identifiers, used by Alembic.
revision = "6f4b0fbc594c"
down_revision = "8a0788f2245b"
branch_labels = None
depends_on = None


def upgrade():
    op.alter_column(
        "import_tables",
        "etl_phase_type",
        existing_type=Enum(
            "truncate_insert", "insert", "merge", "merge_history_audit", "none"
        ),
        comment="What method to use for ETL phase",
        existing_nullable=True,
        server_default=sa.text("'truncate_insert'"),
    )

    op.alter_column(
        "export_tables",
        "source_is_view",
        existing_type=mysql.TINYINT(display_width=4),
        nullable=False,
        new_column_name="forceCreateTempTable",
        server_default=sa.text("'0'"),
        comment="Force export to create a Hive table and export that instead. Useful when exporting views",
    )

    op.alter_column(
        "export_tables",
        "export_history",
        existing_type=mysql.TINYINT(display_width=4),
        nullable=False,
        new_column_name="notUsed01",
        server_default=sa.text("'0'"),
        comment="<NOT USED>",
    )

    op.alter_column(
        "export_tables",
        "source_is_acid",
        existing_type=mysql.TINYINT(display_width=4),
        nullable=False,
        new_column_name="notUsed02",
        server_default=sa.text("'0'"),
        comment="<NOT USED>",
    )


def downgrade():
    op.alter_column(
        "export_tables",
        "forceCreateTempTable",
        existing_type=mysql.TINYINT(display_width=4),
        nullable=False,
        new_column_name="source_is_view",
        server_default=sa.text("'0'"),
        comment="<NOT USED>",
    )

    op.alter_column(
        "export_tables",
        "notUsed02",
        existing_type=mysql.TINYINT(display_width=4),
        nullable=False,
        new_column_name="source_is_acid",
        server_default=sa.text("'0'"),
        comment="<NOT USED>",
    )

    op.alter_column(
        "export_tables",
        "notUsed01",
        existing_type=mysql.TINYINT(display_width=4),
        nullable=False,
        new_column_name="export_history",
        server_default=sa.text("'0'"),
        comment="<NOT USED>",
    )
