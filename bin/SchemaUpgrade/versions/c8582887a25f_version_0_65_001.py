"""Version 0.65.001

Revision ID: c8582887a25f
Revises: 8f373ec72eb1
Create Date: 2019-11-09 05:47:57.139067

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "c8582887a25f"
down_revision = "8f373ec72eb1"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "jdbc_connections",
        sa.Column(
            "contact_info",
            sa.String(length=255),
            nullable=True,
            comment="Contact information. Used by Atlas integration",
        ),
    )
    op.add_column(
        "jdbc_connections",
        sa.Column(
            "description",
            sa.String(length=255),
            nullable=True,
            comment="Description. Used by Atlas integration",
        ),
    )
    op.add_column(
        "jdbc_connections",
        sa.Column(
            "owner",
            sa.String(length=255),
            nullable=True,
            comment="Owner of system and/or data. Used by Atlas integration",
        ),
    )
    op.add_column(
        "import_statistics",
        sa.Column("atlas_schema_duration", sa.Integer(), nullable=True),
    )
    op.add_column(
        "import_statistics",
        sa.Column("atlas_schema_start", sa.DateTime(), nullable=True),
    )
    op.add_column(
        "import_statistics",
        sa.Column("atlas_schema_stop", sa.DateTime(), nullable=True),
    )
    op.add_column(
        "import_statistics_last",
        sa.Column("atlas_schema_duration", sa.Integer(), nullable=True),
    )
    op.add_column(
        "import_statistics_last",
        sa.Column("atlas_schema_start", sa.DateTime(), nullable=True),
    )
    op.add_column(
        "import_statistics_last",
        sa.Column("atlas_schema_stop", sa.DateTime(), nullable=True),
    )


def downgrade():
    op.drop_column("jdbc_connections", "contact_info")
    op.drop_column("jdbc_connections", "description")
    op.drop_column("jdbc_connections", "owner")
    op.drop_column("import_statistics", "atlas_schema_duration")
    op.drop_column("import_statistics", "atlas_schema_start")
    op.drop_column("import_statistics", "atlas_schema_stop")
    op.drop_column("import_statistics_last", "atlas_schema_duration")
    op.drop_column("import_statistics_last", "atlas_schema_start")
    op.drop_column("import_statistics_last", "atlas_schema_stop")
