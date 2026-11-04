"""Version 0.66.004

Revision ID: 4a7fffd40f62
Revises: 6f4b0fbc594c
Create Date: 2020-11-19 06:41:35.640976

"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql
from sqlalchemy import Enum


# revision identifiers, used by Alembic.
revision = "4a7fffd40f62"
down_revision = "6f4b0fbc594c"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "atlas_column_cache",
        sa.Column(
            "hostname",
            sa.String(length=256),
            nullable=False,
            comment="Hostname for the database",
        ),
        sa.Column(
            "port", sa.String(length=8), nullable=False, comment="Port for the database"
        ),
        sa.Column(
            "database_name",
            sa.String(length=256),
            nullable=False,
            comment="Database name",
        ),
        sa.Column(
            "schema_name",
            sa.String(length=256),
            nullable=False,
            comment="Database schema",
        ),
        sa.Column(
            "table_name",
            sa.String(length=256),
            nullable=False,
            comment="Database table",
        ),
        sa.Column(
            "column_name",
            sa.String(length=256),
            nullable=True,
            comment="Name of the column",
        ),
        sa.Column(
            "column_type",
            sa.String(length=2048),
            nullable=True,
            comment="Type of the column",
        ),
        sa.Column(
            "column_length",
            sa.String(length=64),
            nullable=True,
            comment="Length of the column",
        ),
        sa.Column(
            "column_is_nullable",
            sa.String(length=16),
            nullable=True,
            comment="Is null values allowed in the column",
        ),
        sa.Column(
            "column_comment", sa.Text(), nullable=True, comment="Comment on the column"
        ),
        sa.Column(
            "table_comment", sa.Text(), nullable=True, comment="Comment on the table"
        ),
        sa.Column(
            "table_type", sa.String(length=256), nullable=True, comment="Table type. "
        ),
        sa.Column(
            "table_create_time",
            sa.DateTime(),
            nullable=True,
            comment="Timestamp for when the table was created",
        ),
        sa.Column(
            "default_value",
            sa.Text(),
            nullable=True,
            comment="Default value of the column",
        ),
        sa.PrimaryKeyConstraint(
            "hostname",
            "port",
            "database_name",
            "schema_name",
            "table_name",
            "column_name",
        ),
        comment="Atlas discovery uses this table to cache values in order to detect changes instead of putting a heavy load on the Atlas server.",
    )

    op.create_table(
        "atlas_key_cache",
        sa.Column(
            "hostname",
            sa.String(length=256),
            nullable=False,
            comment="Hostname for the database",
        ),
        sa.Column(
            "port", sa.String(length=8), nullable=False, comment="Port for the database"
        ),
        sa.Column(
            "database_name",
            sa.String(length=256),
            nullable=False,
            comment="Database name",
        ),
        sa.Column(
            "schema_name",
            sa.String(length=256),
            nullable=False,
            comment="Database schema",
        ),
        sa.Column(
            "table_name",
            sa.String(length=256),
            nullable=False,
            comment="Database table",
        ),
        sa.Column(
            "constraint_name",
            sa.String(length=256),
            nullable=True,
            comment="Name of the constraint",
        ),
        sa.Column(
            "constraint_type",
            sa.String(length=8),
            nullable=True,
            comment="Type of the constraint",
        ),
        sa.Column(
            "column_name",
            sa.String(length=256),
            nullable=True,
            comment="Name of the column",
        ),
        sa.Column(
            "reference_schema_name",
            sa.String(length=256),
            nullable=True,
            comment="Name of the schema that is referenced",
        ),
        sa.Column(
            "reference_table_name",
            sa.String(length=256),
            nullable=True,
            comment="Name of the table that is referenced",
        ),
        sa.Column(
            "reference_column_name",
            sa.String(length=256),
            nullable=True,
            comment="Name of the column that is referenced",
        ),
        sa.Column(
            "col_key_position", sa.Integer, nullable=True, comment="Position of the key"
        ),
        sa.PrimaryKeyConstraint(
            "hostname",
            "port",
            "database_name",
            "schema_name",
            "table_name",
            "constraint_name",
            "column_name",
        ),
        comment="Atlas discovery uses this table to cache values in order to detect changes instead of putting a heavy load on the Atlas server.",
    )


#                fullSourceKeyDF.insert(0, "hostname", self.jdbc_hostname)
#                fullSourceKeyDF.insert(1, "port", self.jdbc_port)
#                fullSourceKeyDF.insert(2, "database_name", self.jdbc_database)
#                fullSourceKeyDF.insert(3, "schema_name", schema)
#                fullSourceKeyDF.insert(4, "table_name", table)
#                fullSourceKeyDF.rename(columns={'CONSTRAINT_NAME': 'constraint_name',
#                                                'CONSTRAINT_TYPE': 'constraint_type',
#                                                'COL_NAME': 'col_name',
#                                                'REFERENCE_SCHEMA_NAME': 'reference_schema_name',
#                                                'REFERENCE_TABLE_NAME': 'reference_table_name',
#                                                'REFERENCE_COL_NAME': 'reference_col_name',
#                                                'COL_KEY_POSITION': 'col_key_position'},
#                                                inplace=True)
#                fullSourceKeyDF.sort_values(by=['constraint_name', 'col_name'], ignore_index=True, inplace=True, key=lambda col: col.str.lower())


def downgrade():
    op.drop_table("atlas_column_cache")
    op.drop_table("atlas_key_cache")
