"""Version 0.66.001

Revision ID: f33e544af3e0
Revises: 836595368d1e
Create Date: 2020-09-25 05:52:02.580943

"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql
from sqlalchemy import Enum

# revision identifiers, used by Alembic.
revision = "f33e544af3e0"
down_revision = "836595368d1e"
branch_labels = None
depends_on = None


def upgrade():
    # Add columns to import_tables
    op.add_column(
        "import_tables",
        sa.Column(
            "validationMethod",
            sa.Enum("customQuery", "rowCount"),
            server_default=sa.text("'rowCount'"),
            nullable=False,
            comment="Validation method to use",
        ),
    )
    op.add_column(
        "import_tables",
        sa.Column(
            "validationCustomQuerySourceSQL",
            sa.String(length=256),
            nullable=True,
            comment="Custom SQL query for source table",
        ),
    )
    op.add_column(
        "import_tables",
        sa.Column(
            "validationCustomQueryHiveSQL",
            sa.String(length=256),
            nullable=True,
            comment="Custom SQL query for Hive table",
        ),
    )
    op.add_column(
        "import_tables",
        sa.Column(
            "validationCustomQueryValidateImportTable",
            mysql.TINYINT(display_width=4),
            server_default=sa.text("'1'"),
            nullable=False,
            comment="1 = Validate Import table, 0 = Dont validate Import table",
        ),
    )
    op.add_column(
        "import_tables",
        sa.Column(
            "validationCustomQuerySourceValue",
            sa.String(length=512),
            nullable=True,
            comment="Used for validation. Dont change manually",
        ),
    )
    op.add_column(
        "import_tables",
        sa.Column(
            "validationCustomQueryHiveValue",
            sa.String(length=512),
            nullable=True,
            comment="Used for validation. Dont change manually",
        ),
    )

    op.alter_column(
        "import_tables",
        "incr_mode",
        existing_type=sa.String(length=16),
        type_=Enum("append", "lastmodified"),
        comment="Incremental import mode",
        existing_nullable=True,
    )

    op.alter_column(
        "import_tables",
        "etl_phase_type",
        existing_type=Enum("truncate_insert", "insert", "merge", "merge_history_audit"),
        type_=Enum("truncate_insert", "insert", "merge", "merge_history_audit", "none"),
        comment="What method to use for ETL phase",
        existing_nullable=True,
    )

    # Add columns to export_tables
    op.add_column(
        "export_tables",
        sa.Column(
            "validationMethod",
            sa.Enum("customQuery", "rowCount"),
            server_default=sa.text("'rowCount'"),
            nullable=False,
            comment="Validation method to use",
        ),
    )
    op.add_column(
        "export_tables",
        sa.Column(
            "validationCustomQueryHiveSQL",
            sa.String(length=256),
            nullable=True,
            comment="Custom SQL query for Hive table",
        ),
    )
    op.add_column(
        "export_tables",
        sa.Column(
            "validationCustomQueryTargetSQL",
            sa.String(length=256),
            nullable=True,
            comment="Custom SQL query for target table",
        ),
    )
    op.add_column(
        "export_tables",
        sa.Column(
            "validationCustomQueryHiveValue",
            sa.String(length=512),
            nullable=True,
            comment="Used for validation. Dont change manually",
        ),
    )
    op.add_column(
        "export_tables",
        sa.Column(
            "validationCustomQueryTargetValue",
            sa.String(length=512),
            nullable=True,
            comment="Used for validation. Dont change manually",
        ),
    )

    op.alter_column(
        "export_tables",
        "export_type",
        existing_type=sa.String(length=16),
        type_=Enum("full", "incr"),
        comment="What export method to use",
        existing_nullable=False,
        existing_server_default=sa.text("'full'"),
    )

    op.alter_column(
        "export_tables",
        "export_tool",
        existing_type=Enum("spark", "sqoop"),
        type_=Enum("spark", "sqoop"),
        comment="What tool should be used for exporting data",
        existing_nullable=False,
        existing_server_default=sa.text("'sqoop'"),
        server_default=sa.text("'spark'"),
    )

    # Change order of columns in import_tables
    op.execute(
        "ALTER TABLE `import_tables` CHANGE COLUMN `validationMethod` `validationMethod` ENUM('customQuery','rowCount') NOT NULL DEFAULT 'rowCount' COMMENT 'Validation method to use' AFTER `validate_import`"
    )
    op.execute(
        "ALTER TABLE `import_tables` CHANGE COLUMN `validationCustomQuerySourceSQL` `validationCustomQuerySourceSQL` VARCHAR(256) NULL COMMENT 'Custom SQL query for source table' AFTER `validate_diff_allowed`"
    )
    op.execute(
        "ALTER TABLE `import_tables` CHANGE COLUMN `validationCustomQueryHiveSQL` `validationCustomQueryHiveSQL` VARCHAR(256) NULL COMMENT 'Custom SQL query for Hive table' AFTER `validationCustomQuerySourceSQL`"
    )
    op.execute(
        "ALTER TABLE `import_tables` CHANGE COLUMN `validationCustomQueryValidateImportTable` `validationCustomQueryValidateImportTable` TINYINT NOT NULL DEFAULT '1' COMMENT '1 = Validate Import table, 0 = Dont validate Import table' AFTER `validationCustomQueryHiveSQL`"
    )
    op.execute(
        "ALTER TABLE `import_tables` CHANGE COLUMN `validationCustomQuerySourceValue` `validationCustomQuerySourceValue` VARCHAR(512) NULL COMMENT 'Used for validation. Dont change manually' AFTER `hive_rowcount`"
    )
    op.execute(
        "ALTER TABLE `import_tables` CHANGE COLUMN `validationCustomQueryHiveValue` `validationCustomQueryHiveValue` VARCHAR(512) NULL COMMENT 'Used for validation. Dont change manually' AFTER `validationCustomQuerySourceValue`"
    )

    op.execute(
        "ALTER TABLE `import_tables` CHANGE COLUMN `import_tool` `import_tool` ENUM('spark','sqoop') NOT NULL DEFAULT 'sqoop' COMMENT 'What tool should be used for importing data' AFTER `import_type`"
    )

    # Change order of columns in export_tables
    op.execute(
        "ALTER TABLE `export_tables` CHANGE COLUMN `validationMethod` `validationMethod` ENUM('customQuery','rowCount') NOT NULL DEFAULT 'rowCount' COMMENT 'Validation method to use' AFTER `validate_export`"
    )
    op.execute(
        "ALTER TABLE `export_tables` CHANGE COLUMN `validationCustomQueryHiveSQL` `validationCustomQueryHiveSQL` VARCHAR(256) NULL COMMENT 'Custom SQL query for Hive table' AFTER `validationMethod`"
    )
    op.execute(
        "ALTER TABLE `export_tables` CHANGE COLUMN `validationCustomQueryTargetSQL` `validationCustomQueryTargetSQL` VARCHAR(256) NULL COMMENT 'Custom SQL query for target table' AFTER `validationCustomQueryHiveSQL`"
    )
    op.execute(
        "ALTER TABLE `export_tables` CHANGE COLUMN `validationCustomQueryHiveValue` `validationCustomQueryHiveValue` VARCHAR(512) NULL COMMENT 'Used for validation. Dont change manually' AFTER `target_rowcount`"
    )
    op.execute(
        "ALTER TABLE `export_tables` CHANGE COLUMN `validationCustomQueryTargetValue` `validationCustomQueryTargetValue` VARCHAR(512) NULL COMMENT 'Used for validation. Dont change manually' AFTER `validationCustomQueryHiveValue`"
    )

    op.execute(
        "ALTER TABLE `export_tables` CHANGE COLUMN `export_type` `export_type` ENUM('full','incr') NOT NULL DEFAULT 'full' COMMENT 'What export method to use. Only full and incr is supported.' AFTER `hive_table`"
    )
    op.execute(
        "ALTER TABLE `export_tables` CHANGE COLUMN `export_tool` `export_tool` ENUM('spark','sqoop') NOT NULL DEFAULT 'spark' COMMENT 'What tool should be used for exporting data' AFTER `export_type`"
    )


def downgrade():
    op.drop_column("import_tables", "validationMethod")
    op.drop_column("import_tables", "validationCustomQuerySourceSQL")
    op.drop_column("import_tables", "validationCustomQueryHiveSQL")
    op.drop_column("import_tables", "validationCustomQueryValidateImportTable")
    op.drop_column("import_tables", "validationCustomQuerySourceValue")
    op.drop_column("import_tables", "validationCustomQueryHiveValue")

    op.drop_column("export_tables", "validationMethod")
    op.drop_column("export_tables", "validationCustomQueryHiveSQL")
    op.drop_column("export_tables", "validationCustomQueryTargetSQL")
    op.drop_column("export_tables", "validationCustomQueryHiveValue")
    op.drop_column("export_tables", "validationCustomQueryTargetValue")

    op.alter_column(
        "export_tables",
        "export_type",
        existing_type=Enum("full", "incr"),
        type_=sa.String(length=16),
        comment="What export method to use. Only full and incr is supported.",
        existing_nullable=False,
        existing_server_default=sa.text("'full'"),
    )

    op.alter_column(
        "export_tables",
        "export_tool",
        existing_type=Enum("spark", "sqoop"),
        type_=Enum("spark", "sqoop"),
        comment="What tool should be used for exporting data",
        existing_nullable=False,
        existing_server_default=sa.text("'spark'"),
        server_default=sa.text("'sqoop'"),
    )

    op.alter_column(
        "import_tables",
        "incr_mode",
        existing_type=Enum("append", "lastmodified"),
        type_=sa.String(length=16),
        comment="append or lastmodified",
        existing_nullable=True,
    )

    op.alter_column(
        "import_tables",
        "etl_phase_type",
        existing_type=Enum(
            "truncate_insert", "insert", "merge", "merge_history_audit", "none"
        ),
        type_=Enum("truncate_insert", "insert", "merge", "merge_history_audit"),
        comment="What method to use for ETL phase",
        existing_nullable=True,
    )
