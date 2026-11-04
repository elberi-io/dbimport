"""Version 0.64.002

Revision ID: 8f373ec72eb1
Revises: a4a53d72fd75
Create Date: 2019-10-29 07:21:03.723578

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "8f373ec72eb1"
down_revision = "a4a53d72fd75"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "import_tables",
        sa.Column(
            "import_tool",
            sa.Enum("spark", "sqoop"),
            server_default=sa.text("'sqoop'"),
            nullable=False,
            comment="What tool should be used for importing data",
        ),
    )
    op.add_column(
        "import_tables",
        sa.Column(
            "split_by_column",
            sa.String(length=64),
            nullable=True,
            comment="Column to split by when doing import with multiple sessions",
        ),
    )
    op.add_column(
        "import_tables",
        sa.Column(
            "spark_executor_memory",
            sa.String(length=8),
            nullable=True,
            comment="Memory used by spark when importring data. Overrides default value in global configuration",
        ),
    )
    op.add_column(
        "import_statistics", sa.Column("spark_duration", sa.Integer(), nullable=True)
    )
    op.add_column(
        "import_statistics", sa.Column("spark_start", sa.DateTime(), nullable=True)
    )
    op.add_column(
        "import_statistics", sa.Column("spark_stop", sa.DateTime(), nullable=True)
    )
    op.add_column(
        "import_statistics_last",
        sa.Column("spark_duration", sa.Integer(), nullable=True),
    )
    op.add_column(
        "import_statistics_last", sa.Column("spark_start", sa.DateTime(), nullable=True)
    )
    op.add_column(
        "import_statistics_last", sa.Column("spark_stop", sa.DateTime(), nullable=True)
    )

    op.execute("ALTER TABLE `table_change_history` DROP PRIMARY KEY;")
    op.execute("ALTER TABLE `jdbc_table_change_history` DROP PRIMARY KEY;")


def downgrade():
    op.drop_column("import_tables", "import_tool")
    op.drop_column("import_tables", "split_by_column")
    op.drop_column("import_tables", "spark_executor_memory")
    op.drop_column("import_statistics", "spark_duration")
    op.drop_column("import_statistics", "spark_start")
    op.drop_column("import_statistics", "spark_stop")
    op.drop_column("import_statistics_last", "spark_duration")
    op.drop_column("import_statistics_last", "spark_start")
    op.drop_column("import_statistics_last", "spark_stop")

    op.execute(
        "ALTER TABLE `table_change_history` ADD PRIMARY KEY (`eventtime`, `column_name`, `hive_table`);"
    )
    op.execute(
        "ALTER TABLE `jdbc_table_change_history` ADD PRIMARY KEY (`eventtime`, `column_name`, `table_name`);"
    )
