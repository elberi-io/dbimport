"""Version 0.68.006

Revision ID: e317884da580
Revises: 8b07d8cde6f6
Create Date: 2021-10-08 04:00:14.209332

"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "e317884da580"
down_revision = "8b07d8cde6f6"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "import_tables",
        sa.Column(
            "hive_split_count",
            sa.Integer(),
            nullable=True,
            comment="Sets tez.grouping.split-count in the Hive session",
        ),
    )

    # Change order of the column
    op.execute(
        "ALTER TABLE `import_tables` CHANGE COLUMN `hive_split_count` `hive_split_count` INTEGER NULL COMMENT 'Sets tez.grouping.split-count in the Hive session' AFTER `hive_merge_heap`"
    )


def downgrade():
    op.drop_column("import_tables", "hive_split_count")
