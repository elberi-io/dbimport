"""Version 0.68.003

Revision ID: 414ee1e3a8a1
Revises: 2714c9baba79
Create Date: 2021-06-18 06:53:43.264323

"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql


# revision identifiers, used by Alembic.
revision = "414ee1e3a8a1"
down_revision = "2714c9baba79"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "import_tables_indexes",
        sa.Column(
            "table_id",
            sa.Integer(),
            nullable=False,
            comment="Foreign Key to import_tables column 'table_id'",
        ),
        sa.Column(
            "hive_db", sa.String(length=256), nullable=True, comment="Hive Database"
        ),
        sa.Column(
            "hive_table", sa.String(length=256), nullable=True, comment="Hive Table"
        ),
        sa.Column(
            "index_name",
            sa.String(length=256),
            nullable=True,
            comment="Name of the index ",
        ),
        sa.Column(
            "index_type",
            sa.String(length=64),
            nullable=False,
            comment="The type of the index. This is the name from the source database vendor",
        ),
        sa.Column(
            "index_unique",
            sa.String(length=50),
            nullable=False,
            comment="Does the index have unique values or not?",
        ),
        sa.Column(
            "column", sa.String(length=256), nullable=False, comment="Column name"
        ),
        sa.Column(
            "column_type",
            sa.String(length=2048),
            nullable=False,
            comment="Column type in source system",
        ),
        sa.Column(
            "column_order",
            sa.Integer(),
            nullable=False,
            comment="The order of the column in the index.",
        ),
        sa.Column(
            "column_is_nullable",
            mysql.TINYINT(display_width=4),
            nullable=False,
            comment="1 = Column may contain null values, 0 = Nulls are not allowed in the column",
        ),
        sa.ForeignKeyConstraint(
            ["table_id"], ["import_tables.table_id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("table_id", "index_name", "column"),
        sa.UniqueConstraint("hive_db", "hive_table", "index_name", "column"),
        comment="Information table that contains what indexes are available on the source system. The data in the table is not used by DBImport itsealf, instead it's a help for the users to know what indexes exists to make an effective import.",
    )

    op.add_column(
        "import_tables",
        sa.Column(
            "sourceTableType",
            sa.String(length=16),
            nullable=True,
            comment="Type of table on the source system. This is a read-only",
        ),
    )

    # Change default import method to Spark
    op.execute(
        "ALTER TABLE `import_tables` CHANGE COLUMN `import_tool` `import_tool` ENUM('spark','sqoop') NOT NULL DEFAULT 'spark' COMMENT 'What tool should be used for importing data' AFTER `import_type`"
    )


def downgrade():
    # Change default import method to Sqoop
    op.execute(
        "ALTER TABLE `import_tables` CHANGE COLUMN `import_tool` `import_tool` ENUM('spark','sqoop') NOT NULL DEFAULT 'sqoop' COMMENT 'What tool should be used for importing data' AFTER `import_type`"
    )

    op.drop_column("import_tables", "sourceTableType")

    op.drop_table("import_tables_indexes")
