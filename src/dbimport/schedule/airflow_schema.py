from sqlalchemy import Integer, String, Text
from sqlalchemy.dialects.mysql import TINYINT
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


metadata = Base.metadata


class SlotPool(Base):
    __tablename__ = "slot_pool"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    pool: Mapped[str | None] = mapped_column(String(50), nullable=True, unique=True)
    slots: Mapped[int | None] = mapped_column(Integer, nullable=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    include_deferred: Mapped[int] = mapped_column(TINYINT(4), nullable=False)
