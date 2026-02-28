from sqlalchemy import (
    Column, String, Integer, Float, 
    DateTime, Text, Numeric, Index
)

from sqlalchemy.orm import DeclarativeBase
from datetime import datetime, timezone

class Base(DeclarativeBase):
    pass 

class HourlyMetrics(Base):
    """
    One row per hour per endpoint per status category.
    This is the Gold layer - aggregated, query-optimized,
    ready for dashboards and alerting.
    
    Granularity decision: hour x endpoint x status_category
    gives enough resolution for incident detection without
    the storage cost of storing every individual log line.
    """
    __tablename__ = "hourly_metrics"

    # composite primary key - uniquely identifies one metric row
    # If pipeline reurns for the same window, upsert updates in place
    window_start = Column(DateTime, primary_key=True)
    endpoint = Column(String(200), primary_key=True)
    status_category = Column(String(10), primary_key=True)

    # Metrics
    request_count = Column(Integer, nullable=False)
    error_rate = Column(Numeric(5, 2), nullable=False)
    avg_response_ms = Column(Numeric(10, 2), nullable=False)
    p95_response_ms = Column(Numeric(10, 2), nullable=False)
    total_bytes = Column(Integer, nullable=False)

    # Audit columns
    log_date = Column(String(10), nullable=False)
    loaded_at = Column(DateTime, default=datetime.now(timezone.utc))

    # Index on window_start for time-range queries
    # Most dashboards query filter by time first

    __table_args__ = (
        Index("idx_metrics_window","window_start"),
        Index("idx_metrics_endpoint","endpoint")
    )

class RejectedLogs(Base):
    """
    Dead letter table for malformed log lines.
    Preserves rejected lines with rejection reason for investigation.
    """

    __tablename__ = "rejected_logs"
    id = Column(Integer, primary_key=True, autoincrement=True)
    raw_line = Column(Text, nullable=False)
    rejection_reason = Column(String(200), nullable=False)
    log_date = Column(String(10), nullable=False)
    rejected_at = Column(DateTime, default=datetime.now(timezone.utc))