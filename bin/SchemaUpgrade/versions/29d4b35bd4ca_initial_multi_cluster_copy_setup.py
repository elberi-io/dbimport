"""Initial multi-cluster copy setup

Revision ID: 29d4b35bd4ca
Revises: 8f0519d44032
Create Date: 2019-06-20 05:15:02.943359

"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql

# revision identifiers, used by Alembic.
revision = "29d4b35bd4ca"
down_revision = "8f0519d44032"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "dbimport_instances",
        sa.Column(
            "instance_id",
            mysql.BIGINT(display_width=20),
            autoincrement=True,
            nullable=False,
            comment="Auto Incremented PrimaryKey of the table",
        ),
        sa.Column(
            "name",
            sa.String(length=32),
            nullable=False,
            comment="Name of the DBImport instance",
        ),
        sa.Column(
            "db_hostname",
            sa.String(length=64),
            nullable=False,
            comment="MySQL Hostname to DBImport database",
        ),
        sa.Column(
            "db_port",
            sa.SmallInteger(),
            nullable=False,
            comment="MySQL Port to DBImport database",
        ),
        sa.Column(
            "db_database",
            sa.String(length=64),
            nullable=False,
            comment="MySQL Database to DBImport database",
        ),
        sa.Column(
            "db_credentials",
            sa.Text(),
            nullable=True,
            comment="MySQL Username and Password to DBImport database",
        ),
        sa.Column(
            "hdfs_address",
            sa.String(length=64),
            nullable=False,
            comment="HDFS address. Example hdfs://hadoopcluster",
        ),
        sa.Column(
            "hdfs_basedir",
            sa.String(length=64),
            nullable=False,
            comment="The base dir to write data to. Example /apps/dbimport",
        ),
        sa.PrimaryKeyConstraint("instance_id"),
        sa.UniqueConstraint("name"),
        comment="This table contains all DBInstance that will receive data from this instance during the copy phase",
    )

    op.create_table(
        "copy_tables",
        sa.Column(
            "copy_id",
            mysql.BIGINT(display_width=20),
            autoincrement=True,
            nullable=False,
            comment="Auto Incremented PrimaryKey of the table",
        ),
        sa.Column(
            "hive_filter",
            sa.String(length=256),
            nullable=False,
            comment="Filter string for database and table. ; separated. Wildcards (*) allowed. Example HIVE_DB.HIVE_TABLE; HIVE_DB.HIVE_TABLE",
        ),
        sa.Column(
            "destination",
            sa.String(length=32),
            nullable=False,
            comment="DBImport instances to copy the imported data to",
        ),
        sa.Column(
            "data_transfer",
            sa.Enum("Synchronous", "Asynchronous"),
            server_default=sa.text("'Synchronous'"),
            nullable=False,
            comment="Synchronous will transfer the data as part of the Import. Asynchronous will transfer the data by a separate process and not part of the Import",
        ),
        sa.ForeignKeyConstraint(
            ["destination"],
            ["dbimport_instances.name"],
        ),
        sa.PrimaryKeyConstraint("copy_id"),
        comment="When the copy phase starts, it will look in this table to understand if its going to copy its data and to what DBImport instances.",
    )

    op.add_column(
        "import_tables",
        sa.Column(
            "copy_finished",
            sa.DateTime(),
            nullable=True,
            comment="Time when last copy from Master DBImport instance was completed. Dont change manually",
        ),
    )
    op.add_column(
        "import_tables",
        sa.Column(
            "copy_slave",
            mysql.TINYINT(display_width=4),
            server_default=sa.text("'0'"),
            nullable=False,
            comment="Defines if this table is a Master table or a Slave table. Dont change manually",
        ),
    )
    # ### end Alembic commands ###


def downgrade():
    op.drop_column("import_tables", "copy_slave")
    op.drop_column("import_tables", "copy_finished")
    op.drop_table("copy_tables")
    op.drop_table("dbimport_instances")
    # ### end Alembic commands ###
