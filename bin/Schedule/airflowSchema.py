# coding: utf-8
from sqlalchemy import *
from sqlalchemy.dialects.mysql import BIGINT, INTEGER, TINYINT, SMALLINT
from sqlalchemy.orm import relationship, aliased
from sqlalchemy.sql import alias, select, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy_utils import create_view

Base = declarative_base()
metadata = Base.metadata


class slotPool(Base):
    __tablename__ = "slot_pool"

    id = Column(Integer, primary_key=True, autoincrement=True)
    pool = Column(String(50), nullable=True, unique=True)
    slots = Column(Integer, nullable=True)
    description = Column(Text, nullable=True)
    include_deferred = Column(TINYINT(4), nullable=False)
