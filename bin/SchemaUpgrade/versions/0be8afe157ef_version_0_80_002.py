"""Version 0.80.002

Revision ID: 0be8afe157ef
Revises: 9f02bade6c68
Create Date: 2022-12-12 09:18:05.650435

"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql


# revision identifiers, used by Alembic.
revision = "0be8afe157ef"
down_revision = "9f02bade6c68"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "import_tables",
        sa.Column(
            "etl_engine",
            sa.Enum("hive", "spark"),
            server_default=sa.text("'hive'"),
            nullable=False,
            comment="What engine will be used to process etl stage",
        ),
    )
    op.add_column(
        "import_tables",
        sa.Column(
            "spark_executors",
            sa.Integer(),
            nullable=True,
            comment="Number of Spark executors to use. Overrides default value in global configuration",
        ),
    )
    op.add_column(
        "import_tables",
        sa.Column(
            "invalidate_impala",
            mysql.TINYINT(display_width=4),
            server_default=sa.text("'-1'"),
            nullable=False,
            comment="-1 = Use default value from configuration table, otherwise 1 will invalidate the table in Impala and 0 will not",
        ),
    )

    op.add_column(
        "import_statistics",
        sa.Column("spark_etl_duration", sa.Integer(), nullable=True),
    )
    op.add_column(
        "import_statistics", sa.Column("spark_etl_start", sa.DateTime(), nullable=True)
    )
    op.add_column(
        "import_statistics", sa.Column("spark_etl_stop", sa.DateTime(), nullable=True)
    )
    op.add_column(
        "import_statistics_last",
        sa.Column("spark_etl_duration", sa.Integer(), nullable=True),
    )
    op.add_column(
        "import_statistics_last",
        sa.Column("spark_etl_start", sa.DateTime(), nullable=True),
    )
    op.add_column(
        "import_statistics_last",
        sa.Column("spark_etl_stop", sa.DateTime(), nullable=True),
    )

    # Change order of the column
    op.execute(
        "ALTER TABLE `import_tables` CHANGE COLUMN `etl_engine` `etl_engine` ENUM('hive', 'spark') NOT NULL DEFAULT 'hive' COMMENT 'What engine will be used to process etl stage' AFTER `import_tool`"
    )
    op.execute(
        "ALTER TABLE `import_tables` CHANGE COLUMN `spark_executors` `spark_executors` INTEGER NULL COMMENT 'Number of Spark executors to use. Overrides default value in global configuration' AFTER `spark_executor_memory`"
    )
    op.execute(
        "ALTER TABLE `import_tables` CHANGE COLUMN `invalidate_impala` `invalidate_impala` TINYINT NOT NULL DEFAULT '-1' COMMENT '-1 = Use default value from configuration table, otherwise 1 will invalidate the table in Impala and 0 will not'"
    )

    op.execute(
        "ALTER TABLE `import_statistics` CHANGE COLUMN `spark_etl_duration` `spark_etl_duration` INTEGER NULL AFTER `spark_stop`"
    )
    op.execute(
        "ALTER TABLE `import_statistics` CHANGE COLUMN `spark_etl_start` `spark_etl_start` DATETIME NULL AFTER `spark_etl_duration`"
    )
    op.execute(
        "ALTER TABLE `import_statistics` CHANGE COLUMN `spark_etl_stop` `spark_etl_stop` DATETIME NULL AFTER `spark_etl_start`"
    )
    op.execute(
        "ALTER TABLE `import_statistics_last` CHANGE COLUMN `spark_etl_duration` `spark_etl_duration` INTEGER NULL AFTER `spark_stop`"
    )
    op.execute(
        "ALTER TABLE `import_statistics_last` CHANGE COLUMN `spark_etl_start` `spark_etl_start` DATETIME NULL AFTER `spark_etl_duration`"
    )
    op.execute(
        "ALTER TABLE `import_statistics_last` CHANGE COLUMN `spark_etl_stop` `spark_etl_stop` DATETIME NULL AFTER `spark_etl_start`"
    )


def downgrade():
    op.drop_column("import_tables", "etl_engine")
    op.drop_column("import_tables", "spark_executors")
    op.drop_column("import_tables", "invalidate_impala")

    op.drop_column("import_statistics", "spark_etl_duration")
    op.drop_column("import_statistics", "spark_etl_start")
    op.drop_column("import_statistics", "spark_etl_stop")
    op.drop_column("import_statistics_last", "spark_etl_duration")
    op.drop_column("import_statistics_last", "spark_etl_start")
    op.drop_column("import_statistics_last", "spark_etl_stop")
