"""How Alembic autogenerate writes the ORM's own column types into a
revision."""

from typing import Literal

from alembic.autogenerate.api import AutogenContext

from dbimport.config_db.config_schema import UTCDateTime


def render_item(
    type_: str, obj: object, autogen_context: AutogenContext
) -> str | Literal[False]:
    """Writes UTCDateTime as the DATETIME it is in the database (ADR-0007),
    so that a revision needs no import of the ORM; everything else as
    Alembic does."""
    if type_ == "type" and isinstance(obj, UTCDateTime):
        return "sa.DateTime()"
    return False
