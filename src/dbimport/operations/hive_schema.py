from sqlalchemy import (
    BigInteger,
    ForeignKey,
    Index,
    Integer,
    SmallInteger,
    String,
    Text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


metadata = Base.metadata


class CDS(Base):
    __tablename__ = "CDS"

    CD_ID: Mapped[int] = mapped_column(BigInteger, primary_key=True)


class DBS(Base):
    __tablename__ = "DBS"

    DB_ID: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    DESC: Mapped[str | None] = mapped_column(String(4000))
    DB_LOCATION_URI: Mapped[str] = mapped_column(String(4000), nullable=False)
    NAME: Mapped[str | None] = mapped_column(String(128), unique=True)
    OWNER_NAME: Mapped[str | None] = mapped_column(String(128))
    OWNER_TYPE: Mapped[str | None] = mapped_column(String(10))


class HiveLocks(Base):
    __tablename__ = "HIVE_LOCKS"

    HL_LOCK_EXT_ID: Mapped[int] = mapped_column(
        BigInteger, primary_key=True, nullable=False
    )
    HL_LOCK_INT_ID: Mapped[int] = mapped_column(
        BigInteger, primary_key=True, nullable=False
    )
    HL_TXNID: Mapped[int | None] = mapped_column(BigInteger, index=True)
    HL_DB: Mapped[str] = mapped_column(String(128), nullable=False)
    HL_TABLE: Mapped[str | None] = mapped_column(String(128))
    HL_PARTITION: Mapped[str | None] = mapped_column(String(767))
    HL_LOCK_STATE: Mapped[str] = mapped_column(String(1), nullable=False)
    HL_LOCK_TYPE: Mapped[str] = mapped_column(String(1), nullable=False)
    HL_LAST_HEARTBEAT: Mapped[int] = mapped_column(BigInteger, nullable=False)
    HL_ACQUIRED_AT: Mapped[int | None] = mapped_column(BigInteger)
    HL_USER: Mapped[str] = mapped_column(String(128), nullable=False)
    HL_HOST: Mapped[str] = mapped_column(String(128), nullable=False)
    HL_HEARTBEAT_COUNT: Mapped[int | None] = mapped_column(Integer)
    HL_AGENT_INFO: Mapped[str | None] = mapped_column(String(128))
    HL_BLOCKEDBY_EXT_ID: Mapped[int | None] = mapped_column(BigInteger)
    HL_BLOCKEDBY_INT_ID: Mapped[int | None] = mapped_column(BigInteger)


class KeyConstraints(Base):
    __tablename__ = "KEY_CONSTRAINTS"

    #    CHILD_CD_ID = Column(BigInteger)
    #    CHILD_INTEGER_IDX = Column(Integer)
    #    CHILD_TBL_ID = Column(BigInteger)
    CHILD_CD_ID: Mapped[int | None] = mapped_column(ForeignKey("COLUMNS_V2.CD_ID"))
    CHILD_INTEGER_IDX: Mapped[int | None] = mapped_column(
        ForeignKey("COLUMNS_V2.INTEGER_IDX")
    )
    CHILD_TBL_ID: Mapped[int | None] = mapped_column(ForeignKey("TBLS.TBL_ID"))
    #    PARENT_CD_ID = Column(BigInteger, nullable=False)
    #    PARENT_INTEGER_IDX = Column(Integer, nullable=False)
    #    PARENT_TBL_ID = Column(BigInteger, nullable=False, index=True)
    PARENT_CD_ID: Mapped[int] = mapped_column(
        ForeignKey("COLUMNS_V2.CD_ID"), nullable=False
    )
    PARENT_INTEGER_IDX: Mapped[int] = mapped_column(
        ForeignKey("COLUMNS_V2.INTEGER_IDX"), nullable=False
    )
    PARENT_TBL_ID: Mapped[int] = mapped_column(
        ForeignKey("TBLS.TBL_ID"), nullable=False, index=True
    )
    POSITION: Mapped[int] = mapped_column(BigInteger, primary_key=True, nullable=False)
    CONSTRAINT_NAME: Mapped[str] = mapped_column(
        String(400), primary_key=True, nullable=False
    )
    CONSTRAINT_TYPE: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    UPDATE_RULE: Mapped[int | None] = mapped_column(SmallInteger)
    DELETE_RULE: Mapped[int | None] = mapped_column(SmallInteger)
    ENABLE_VALIDATE_RELY: Mapped[int] = mapped_column(SmallInteger, nullable=False)

    TBLS_PARENT: Mapped["TBLS"] = relationship(
        "TBLS", primaryjoin="KeyConstraints.PARENT_TBL_ID == TBLS.TBL_ID"
    )
    TBLS_CHILD: Mapped["TBLS | None"] = relationship(
        "TBLS", primaryjoin="KeyConstraints.CHILD_TBL_ID == TBLS.TBL_ID"
    )
    COLUMNS_V2_PARENT_CD: Mapped["ColumnsV2"] = relationship(
        "ColumnsV2", primaryjoin="KeyConstraints.PARENT_CD_ID == ColumnsV2.CD_ID"
    )
    COLUMNS_V2_PARENT_IDX: Mapped["ColumnsV2"] = relationship(
        "ColumnsV2",
        primaryjoin="KeyConstraints.PARENT_INTEGER_IDX == ColumnsV2.INTEGER_IDX",
    )
    COLUMNS_V2_CHILD_CD: Mapped["ColumnsV2 | None"] = relationship(
        "ColumnsV2", primaryjoin="KeyConstraints.CHILD_CD_ID == ColumnsV2.CD_ID"
    )
    COLUMNS_V2_CHILD_IDX: Mapped["ColumnsV2 | None"] = relationship(
        "ColumnsV2",
        primaryjoin="KeyConstraints.CHILD_INTEGER_IDX == ColumnsV2.INTEGER_IDX",
    )


class VERSION(Base):
    __tablename__ = "VERSION"

    VER_ID: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    SCHEMA_VERSION: Mapped[str] = mapped_column(String(127), nullable=False)
    VERSION_COMMENT: Mapped[str | None] = mapped_column(String(255))


class ColumnsV2(Base):
    __tablename__ = "COLUMNS_V2"

    CD_ID: Mapped[int] = mapped_column(
        ForeignKey("CDS.CD_ID"), primary_key=True, nullable=False, index=True
    )
    COMMENT: Mapped[str | None] = mapped_column(String(256))
    COLUMN_NAME: Mapped[str] = mapped_column(
        String(767), primary_key=True, nullable=False
    )
    TYPE_NAME: Mapped[str | None] = mapped_column(String(4000))
    INTEGER_IDX: Mapped[int] = mapped_column(Integer, nullable=False)

    CDS: Mapped["CDS"] = relationship("CDS")


class SERDES(Base):
    __tablename__ = "SERDES"

    SERDE_ID: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    NAME: Mapped[str | None] = mapped_column(String(128))
    SLIB: Mapped[str | None] = mapped_column(String(4000))


class SDS(Base):
    __tablename__ = "SDS"

    SD_ID: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    CD_ID: Mapped[int | None] = mapped_column(ForeignKey("CDS.CD_ID"), index=True)
    INPUT_FORMAT: Mapped[str | None] = mapped_column(String(4000))
    IS_COMPRESSED: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    IS_STOREDASSUBDIRECTORIES: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    LOCATION: Mapped[str | None] = mapped_column(String(4000))
    NUM_BUCKETS: Mapped[int] = mapped_column(Integer, nullable=False)
    OUTPUT_FORMAT: Mapped[str | None] = mapped_column(String(4000))
    SERDE_ID: Mapped[int | None] = mapped_column(
        ForeignKey("SERDES.SERDE_ID"), index=True
    )

    CDS: Mapped["CDS | None"] = relationship("CDS")
    SERDES: Mapped["SERDES | None"] = relationship("SERDES")


class TBLS(Base):
    __tablename__ = "TBLS"
    __table_args__ = (Index("UNIQUETABLE", "TBL_NAME", "DB_ID", unique=True),)

    TBL_ID: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    CREATE_TIME: Mapped[int] = mapped_column(Integer, nullable=False)
    DB_ID: Mapped[int | None] = mapped_column(ForeignKey("DBS.DB_ID"), index=True)
    LAST_ACCESS_TIME: Mapped[int] = mapped_column(Integer, nullable=False)
    OWNER: Mapped[str | None] = mapped_column(String(767))
    RETENTION: Mapped[int] = mapped_column(Integer, nullable=False)
    SD_ID: Mapped[int | None] = mapped_column(ForeignKey("SDS.SD_ID"), index=True)
    TBL_NAME: Mapped[str | None] = mapped_column(String(128))
    TBL_TYPE: Mapped[str | None] = mapped_column(String(128))
    VIEW_EXPANDED_TEXT: Mapped[str | None] = mapped_column(Text)
    VIEW_ORIGINAL_TEXT: Mapped[str | None] = mapped_column(Text)

    DBS: Mapped["DBS | None"] = relationship("DBS")
    SDS: Mapped["SDS | None"] = relationship("SDS")


class IDXS(Base):
    __tablename__ = "IDXS"
    __table_args__ = (Index("UNIQUEINDEX", "INDEX_NAME", "ORIG_TBL_ID", unique=True),)

    INDEX_ID: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    CREATE_TIME: Mapped[int] = mapped_column(Integer, nullable=False)
    DEFERRED_REBUILD: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    INDEX_HANDLER_CLASS: Mapped[str | None] = mapped_column(String(4000))
    INDEX_NAME: Mapped[str | None] = mapped_column(String(128))
    INDEX_TBL_ID: Mapped[int | None] = mapped_column(
        ForeignKey("TBLS.TBL_ID"), index=True
    )
    LAST_ACCESS_TIME: Mapped[int] = mapped_column(Integer, nullable=False)
    ORIG_TBL_ID: Mapped[int | None] = mapped_column(
        ForeignKey("TBLS.TBL_ID"), index=True
    )
    SD_ID: Mapped[int | None] = mapped_column(ForeignKey("SDS.SD_ID"), index=True)

    TBLS: Mapped["TBLS | None"] = relationship(
        "TBLS", primaryjoin="IDXS.INDEX_TBL_ID == TBLS.TBL_ID"
    )
    # The relationship TBLS above hides the class TBLS from mypy here;
    # SQLAlchemy looks the name up in its class registry.
    TBLS1: Mapped["TBLS | None"] = relationship(  # type: ignore[valid-type]
        "TBLS", primaryjoin="IDXS.ORIG_TBL_ID == TBLS.TBL_ID"
    )
    SDS: Mapped["SDS | None"] = relationship("SDS")


class TableParams(Base):
    __tablename__ = "TABLE_PARAMS"

    TBL_ID: Mapped[int] = mapped_column(
        ForeignKey("TBLS.TBL_ID"), primary_key=True, nullable=False, index=True
    )
    PARAM_KEY: Mapped[str] = mapped_column(
        String(256), primary_key=True, nullable=False
    )
    PARAM_VALUE: Mapped[str | None] = mapped_column(String(60000))

    TBL: Mapped["TBLS"] = relationship("TBLS")
