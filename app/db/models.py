from sqlalchemy import String, Integer, DateTime, func, JSON, Index, text, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

class Base(DeclarativeBase):
    pass

class ScrapedCompound(Base):
    __tablename__ = "scraped_compounds"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    query: Mapped[str] = mapped_column(String(255), index=True)
    source: Mapped[str] = mapped_column(String(64), index=True)              # "pubchem"
    external_id: Mapped[str | None] = mapped_column(String(128), index=True) # CID
    properties: Mapped[dict | None] = mapped_column(JSON)
    created_at: Mapped[str] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[str] = mapped_column(DateTime(timezone=True),
                                           server_default=func.now(),
                                           onupdate=func.now())

    __table_args__ = (
        Index("ix_scraped_query_source", "query", "source"),
        UniqueConstraint("source", "external_id", name="uq_source_external_id"),  # avoid dup rows per CID
    )

class ScrapingLog(Base):
    __tablename__ = "scraping_logs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    job_type: Mapped[str] = mapped_column(String(64))  # e.g., "compound_lookup"
    query: Mapped[str] = mapped_column(String(255))
    source: Mapped[str] = mapped_column(String(64))    # "pubchem"
    status: Mapped[str] = mapped_column(String(32))    # "success" | "error"
    message: Mapped[str | None] = mapped_column(String(1000))
    attempt: Mapped[int] = mapped_column(Integer, server_default=text("0"))
    created_at: Mapped[str] = mapped_column(DateTime(timezone=True), server_default=func.now())
