# coding: utf-8
from sqlalchemy import (
    BigInteger,
    Column,
    ForeignKey,
    Index,
    Integer,
    SmallInteger,
    String,
    Text,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()
metadata = Base.metadata


class CDS(Base):
    __tablename__ = "CDS"

    CD_ID = Column(BigInteger, primary_key=True)


class DBS(Base):
    __tablename__ = "DBS"

    DB_ID = Column(BigInteger, primary_key=True)
    DESC = Column(String(4000))
    DB_LOCATION_URI = Column(String(4000), nullable=False)
    NAME = Column(String(128), unique=True)
    OWNER_NAME = Column(String(128))
    OWNER_TYPE = Column(String(10))


class HIVE_LOCKS(Base):
    __tablename__ = "HIVE_LOCKS"

    HL_LOCK_EXT_ID = Column(BigInteger, primary_key=True, nullable=False)
    HL_LOCK_INT_ID = Column(BigInteger, primary_key=True, nullable=False)
    HL_TXNID = Column(BigInteger, index=True)
    HL_DB = Column(String(128), nullable=False)
    HL_TABLE = Column(String(128))
    HL_PARTITION = Column(String(767))
    HL_LOCK_STATE = Column(String(1), nullable=False)
    HL_LOCK_TYPE = Column(String(1), nullable=False)
    HL_LAST_HEARTBEAT = Column(BigInteger, nullable=False)
    HL_ACQUIRED_AT = Column(BigInteger)
    HL_USER = Column(String(128), nullable=False)
    HL_HOST = Column(String(128), nullable=False)
    HL_HEARTBEAT_COUNT = Column(Integer)
    HL_AGENT_INFO = Column(String(128))
    HL_BLOCKEDBY_EXT_ID = Column(BigInteger)
    HL_BLOCKEDBY_INT_ID = Column(BigInteger)


class KEY_CONSTRAINTS(Base):
    __tablename__ = "KEY_CONSTRAINTS"

    #    CHILD_CD_ID = Column(BigInteger)
    #    CHILD_INTEGER_IDX = Column(Integer)
    #    CHILD_TBL_ID = Column(BigInteger)
    CHILD_CD_ID = Column(ForeignKey("COLUMNS_V2.CD_ID"))
    CHILD_INTEGER_IDX = Column(ForeignKey("COLUMNS_V2.INTEGER_IDX"))
    CHILD_TBL_ID = Column(ForeignKey("TBLS.TBL_ID"))
    #    PARENT_CD_ID = Column(BigInteger, nullable=False)
    #    PARENT_INTEGER_IDX = Column(Integer, nullable=False)
    #    PARENT_TBL_ID = Column(BigInteger, nullable=False, index=True)
    PARENT_CD_ID = Column(ForeignKey("COLUMNS_V2.CD_ID"), nullable=False)
    PARENT_INTEGER_IDX = Column(ForeignKey("COLUMNS_V2.INTEGER_IDX"), nullable=False)
    PARENT_TBL_ID = Column(ForeignKey("TBLS.TBL_ID"), nullable=False, index=True)
    POSITION = Column(BigInteger, primary_key=True, nullable=False)
    CONSTRAINT_NAME = Column(String(400), primary_key=True, nullable=False)
    CONSTRAINT_TYPE = Column(SmallInteger, nullable=False)
    UPDATE_RULE = Column(SmallInteger)
    DELETE_RULE = Column(SmallInteger)
    ENABLE_VALIDATE_RELY = Column(SmallInteger, nullable=False)

    TBLS_PARENT = relationship(
        "TBLS", primaryjoin="KEY_CONSTRAINTS.PARENT_TBL_ID == TBLS.TBL_ID"
    )
    TBLS_CHILD = relationship(
        "TBLS", primaryjoin="KEY_CONSTRAINTS.CHILD_TBL_ID == TBLS.TBL_ID"
    )
    COLUMNS_V2_PARENT_CD = relationship(
        "COLUMNS_V2", primaryjoin="KEY_CONSTRAINTS.PARENT_CD_ID == COLUMNS_V2.CD_ID"
    )
    COLUMNS_V2_PARENT_IDX = relationship(
        "COLUMNS_V2",
        primaryjoin="KEY_CONSTRAINTS.PARENT_INTEGER_IDX == COLUMNS_V2.INTEGER_IDX",
    )
    COLUMNS_V2_CHILD_CD = relationship(
        "COLUMNS_V2", primaryjoin="KEY_CONSTRAINTS.CHILD_CD_ID == COLUMNS_V2.CD_ID"
    )
    COLUMNS_V2_CHILD_IDX = relationship(
        "COLUMNS_V2",
        primaryjoin="KEY_CONSTRAINTS.CHILD_INTEGER_IDX == COLUMNS_V2.INTEGER_IDX",
    )


class VERSION(Base):
    __tablename__ = "VERSION"

    VER_ID = Column(BigInteger, primary_key=True)
    SCHEMA_VERSION = Column(String(127), nullable=False)
    VERSION_COMMENT = Column(String(255))


class COLUMNS_V2(Base):
    __tablename__ = "COLUMNS_V2"

    CD_ID = Column(
        ForeignKey("CDS.CD_ID"), primary_key=True, nullable=False, index=True
    )
    COMMENT = Column(String(256))
    COLUMN_NAME = Column(String(767), primary_key=True, nullable=False)
    TYPE_NAME = Column(String(4000))
    INTEGER_IDX = Column(Integer, nullable=False)

    CDS = relationship("CDS")


class SERDES(Base):
    __tablename__ = "SERDES"

    SERDE_ID = Column(BigInteger, primary_key=True)
    NAME = Column(String(128))
    SLIB = Column(String(4000))


class SDS(Base):
    __tablename__ = "SDS"

    SD_ID = Column(BigInteger, primary_key=True)
    CD_ID = Column(ForeignKey("CDS.CD_ID"), index=True)
    INPUT_FORMAT = Column(String(4000))
    IS_COMPRESSED = Column(SmallInteger, nullable=False)
    IS_STOREDASSUBDIRECTORIES = Column(SmallInteger, nullable=False)
    LOCATION = Column(String(4000))
    NUM_BUCKETS = Column(Integer, nullable=False)
    OUTPUT_FORMAT = Column(String(4000))
    SERDE_ID = Column(ForeignKey("SERDES.SERDE_ID"), index=True)

    CDS = relationship("CDS")
    SERDES = relationship("SERDES")


class TBLS(Base):
    __tablename__ = "TBLS"
    __table_args__ = (Index("UNIQUETABLE", "TBL_NAME", "DB_ID", unique=True),)

    TBL_ID = Column(BigInteger, primary_key=True)
    CREATE_TIME = Column(Integer, nullable=False)
    DB_ID = Column(ForeignKey("DBS.DB_ID"), index=True)
    LAST_ACCESS_TIME = Column(Integer, nullable=False)
    OWNER = Column(String(767))
    RETENTION = Column(Integer, nullable=False)
    SD_ID = Column(ForeignKey("SDS.SD_ID"), index=True)
    TBL_NAME = Column(String(128))
    TBL_TYPE = Column(String(128))
    VIEW_EXPANDED_TEXT = Column(Text)
    VIEW_ORIGINAL_TEXT = Column(Text)

    DBS = relationship("DBS")
    SDS = relationship("SDS")


class IDXS(Base):
    __tablename__ = "IDXS"
    __table_args__ = (Index("UNIQUEINDEX", "INDEX_NAME", "ORIG_TBL_ID", unique=True),)

    INDEX_ID = Column(BigInteger, primary_key=True)
    CREATE_TIME = Column(Integer, nullable=False)
    DEFERRED_REBUILD = Column(SmallInteger, nullable=False)
    INDEX_HANDLER_CLASS = Column(String(4000))
    INDEX_NAME = Column(String(128))
    INDEX_TBL_ID = Column(ForeignKey("TBLS.TBL_ID"), index=True)
    LAST_ACCESS_TIME = Column(Integer, nullable=False)
    ORIG_TBL_ID = Column(ForeignKey("TBLS.TBL_ID"), index=True)
    SD_ID = Column(ForeignKey("SDS.SD_ID"), index=True)

    TBLS = relationship("TBLS", primaryjoin="IDXS.INDEX_TBL_ID == TBLS.TBL_ID")
    TBLS1 = relationship("TBLS", primaryjoin="IDXS.ORIG_TBL_ID == TBLS.TBL_ID")
    SDS = relationship("SDS")


class TABLE_PARAMS(Base):
    __tablename__ = "TABLE_PARAMS"

    TBL_ID = Column(
        ForeignKey("TBLS.TBL_ID"), primary_key=True, nullable=False, index=True
    )
    PARAM_KEY = Column(String(256), primary_key=True, nullable=False)
    PARAM_VALUE = Column(String(60000))

    TBL = relationship("TBLS")
