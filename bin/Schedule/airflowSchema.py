# coding: utf-8
from sqlalchemy import Column, Integer, String, Text
from sqlalchemy.dialects.mysql import TINYINT
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata


class slotPool(Base):
    __tablename__ = "slot_pool"

    id = Column(Integer, primary_key=True, autoincrement=True)
    pool = Column(String(50), nullable=True, unique=True)
    slots = Column(Integer, nullable=True)
    description = Column(Text, nullable=True)
    include_deferred = Column(TINYINT(4), nullable=False)
