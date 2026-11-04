"""Change definition of import type

Revision ID: c0c3337bb44a
Revises: 07d17bb3b23b
Create Date: 2019-06-26 04:29:34.598203

"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql

# revision identifiers, used by Alembic.
revision = "c0c3337bb44a"
down_revision = "07d17bb3b23b"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "import_tables",
        sa.Column(
            "etl_phase_type",
            sa.Enum("truncate_insert", "insert", "merge", "merge_history_audit"),
            server_default=sa.text("'truncate_insert'"),
            nullable=True,
            comment="What method to use for ETL phase",
        ),
    )
    op.add_column(
        "import_tables",
        sa.Column(
            "import_phase_type",
            sa.Enum("full", "incr", "oracle_flashback"),
            server_default=sa.text("'full'"),
            nullable=True,
            comment="What method to use for Import phase",
        ),
    )
    op.alter_column(
        "import_tables",
        "import_type",
        existing_type=mysql.VARCHAR(length=32),
        nullable=True,
        server_default=None,
        existing_comment="What import method to use",
        existing_server_default=sa.text("'full'"),
    )
    op.execute(
        "ALTER TABLE `import_tables` CHANGE COLUMN `import_phase_type` `import_phase_type` ENUM('full','incr','oracle_flashback') NULL DEFAULT 'full' COMMENT 'What method to use for Import phase' AFTER `source_table`,CHANGE COLUMN `etl_phase_type` `etl_phase_type` ENUM('truncate_insert','insert','merge','merge_history_audit') NULL DEFAULT 'truncate_insert' COMMENT 'What method to use for ETL phase' AFTER `import_phase_type`"
    )


def downgrade():
    op.alter_column(
        "import_tables",
        "import_type",
        existing_type=mysql.VARCHAR(length=32),
        nullable=False,
        server_default=sa.text("'full'"),
        existing_comment="What import method to use",
        existing_server_default=None,
    )
    op.drop_column("import_tables", "import_phase_type")
    op.drop_column("import_tables", "etl_phase_type")
