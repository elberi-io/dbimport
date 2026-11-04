"""Version 0.80.008

Revision ID: af71422a72a2
Revises: 769cc11bdff9
Create Date: 2024-03-07 08:20:14.557199

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "af71422a72a2"
down_revision = "769cc11bdff9"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "import_tables",
        sa.Column(
            "import_database",
            sa.String(length=256),
            nullable=True,
            comment="Override the database name used for the import table",
        ),
    )
    op.add_column(
        "import_tables",
        sa.Column(
            "import_table",
            sa.String(length=256),
            nullable=True,
            comment="Override the table name used for the import table",
        ),
    )
    op.add_column(
        "import_tables",
        sa.Column(
            "history_database",
            sa.String(length=256),
            nullable=True,
            comment="Override the database name used for the history table if that exists",
        ),
    )
    op.add_column(
        "import_tables",
        sa.Column(
            "history_table",
            sa.String(length=256),
            nullable=True,
            comment="Override the table name used for the history table if that exists",
        ),
    )


def downgrade():
    op.drop_column("import_tables", "import_database")
    op.drop_column("import_tables", "import_table")
    op.drop_column("import_tables", "history_database")
    op.drop_column("import_tables", "history_table")
