"""Version 0.66.005

Revision ID: 4368d52462a3
Revises: 4a7fffd40f62
Create Date: 2020-12-03 05:05:06.519319

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "4368d52462a3"
down_revision = "4a7fffd40f62"
branch_labels = None
depends_on = None


def upgrade():
    # Add columns to import_tables
    op.add_column(
        "import_tables",
        sa.Column(
            "mergeCompactionMethod",
            sa.Enum(
                "default", "none", "minor", "minor_and_wait", "major", "major_and_wait"
            ),
            server_default=sa.text("'default'"),
            nullable=False,
            comment="Compaction method to use after import using merge is completed. Default means a major compaction if it is configured to do so in the configuration table",
        ),
    )

    # Change order of columns in import_tables


# 	op.execute("ALTER TABLE `import_tables` CHANGE COLUMN `compactionMethod` `compactionMethod` ENUM('default', 'none', 'minor', 'minor_and_wait', 'major', 'major_and_wait') NOT NULL DEFAULT 'default' COMMENT 'Compaction method to use after import is completed. Default means a major compaction if it is configured to do so in the configuration table' AFTER `validate_import`")


def downgrade():
    op.drop_column("import_tables", "mergeCompactionMethod")
