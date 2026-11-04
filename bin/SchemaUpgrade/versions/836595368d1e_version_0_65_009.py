"""Version 0.65.009

Revision ID: 836595368d1e
Revises: ab55a08d3539
Create Date: 2020-03-23 09:56:54.626123

"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql
from sqlalchemy import Enum


# revision identifiers, used by Alembic.
revision = "836595368d1e"
down_revision = "ab55a08d3539"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "import_columns",
        sa.Column(
            "anonymization_function",
            sa.Enum("None", "Hash", "Replace with star", "Show first 4 chars"),
            server_default=sa.text("'None'"),
            nullable=False,
            comment="What anonymization function should be used with the data in this column",
        ),
    )
    op.add_column(
        "import_tables",
        sa.Column(
            "custom_max_query",
            sa.String(length=256),
            nullable=True,
            comment="You can use a custom SQL query that will get the Max value from the source database. This Max value will be used in an inremental import to know how much to read in each execution",
        ),
    )
    op.add_column(
        "jdbc_connections",
        sa.Column(
            "seed_file",
            sa.String(length=256),
            nullable=True,
            comment="File that will be used to fetch the custom seed that will be used for anonymization functions on data from the connection",
        ),
    )


def downgrade():
    op.drop_column("import_columns", "anonymization_function")
    op.drop_column("import_columns", "custom_max_query")
    op.drop_column("jdbc_connections", "seed_file")
