import logging 
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
from sqlalchemy.engine import Engine
from dotenv import load_dotenv
import os 

load_dotenv() 
logger = logging.getLogger(__name__)

def get_engine():
    """
    Creates a SQLAlchemy engine from environment credentials
    """
    url = (
        f"postgresql+psycopg2://"
        f"{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}"
        f"@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}"
        f"/{os.getenv('POSTGRES_DB')}"
    )

    return create_engine(url)

def upsert_metrics(df: pd.DataFrame, engine: Engine) -> int:
    """
    Upserts hourly metrics into the hourly_metrics table
    
    Why upsert over append?
    Idempotency - if the pipeline reruns for the same date, metrics
    are updated in place rather than duplicated. The composite key
    (window_start, endpoint, status_category) uniquely identifies each metric row.
    
    Returns count of rows upserted.
    """
    if df.empty:
        logger.warning(f"Empty metrics DataFrame - nothing to upsert")
        return 0
    
    upsert_sql = """
    INSERT INTO hourly_metrics (
    window_start, endpoint, status_category, request_count, error_rate,
    avg_response_ms, p95_response_ms, total_bytes, log_date, loaded_at
    ) VALUES (
    :window_start, :endpoint, :status_category, :request_count,:error_rate, 
    :avg_response_ms, :p95_response_ms, :total_bytes, :log_date, :loaded_at
    )
    ON CONFLICT (window_start, endpoint, status_category)
    DO UPDATE SET
        request_count   = EXCLUDED.request_count,
        error_rate      = EXCLUDED.error_rate,
        avg_response_ms = EXCLUDED.avg_response_ms,
        p95_response_ms = EXCLUDED.p95_response_ms,
        total_bytes     = EXCLUDED.total_bytes,
        loaded_at       = EXCLUDED.loaded_at;
    """

    rows = df.to_dict(orient="records")

    with engine.connect() as connection:
        connection.execute(text(upsert_sql), rows)
        connection.commit()

    logger.info(f"Upserted {len(rows)} metric rows into hourly_metrics")
    return len(rows)

def insert_injected_logs(
        df: pd.DataFrame, engine: Engine
) -> int: 
    """
    Insert rejected log lines into the rejected_logs table
    
    Why insert instead of upsert?
    Rejected logs have no natural primary key - the same malformed
    line could appear multiple times across different pipeline runs on
    different days. Each rejection is a distinct event worth recording independently.
    
    We do check for duplicates within the same log_date to avoid re-inserting
    the same rejected lines if the pipeline reruns for the same day
    
    Returns count of rows inserted.
    """
    if df.empty:
        logger.info("No rejected logs to insert")
        return 0
    
    insert_sql = """
    INSERT INTO rejected_logs (
        raw_line, rejection_reason, log_date, rejected_at
    )
    SELECT 
        :raw_line, :rejection_reason, :log_date, :rejected_at
    WHERE NOT EXISTS (
    SELECT 1 FROM rejected_logs
    WHERE raw_line = :raw_line
    AND log_date = :log_date
    );
    """

    rows = df.to_dict(orient="records")

    with engine.connect() as connection:
        connection.execute(text(insert_sql), rows)
        connection.commit()

    logger.info(f"Inserted rejected logs into rejected_logs table")
    return len(rows)

def load_top_ips(
        df: pd.DataFrame, engine: Engine, log_date: str
) -> int:
    """
    Stores top IP addresses for the day in a separate table
    
    Why a separate table?
    IP-level data has different access control requirements
    than performance metrics. Security teams need it.
    Product and engineering teams querying dashboard metrics don't - 
    and shouldn't necessarily have access to raw IP data for privacy reasons.
    
    Replaces previous day's top IPs on reruns - only the current top N
    matters not historical versions
    """
    if df.empty:
        logger.info("No IP data to load")
        return 0 
    
    # Delete existing entries for this date before inserting
    # This is replace semantics - only current top N is kept
    delete_sql = "DELETE FROM top_ips WHERE log_date = :log_date"

    insert_sql = """
        INSERT INTO top_ips (
        ip, request_count, unique_endpoints,
        avg_response_ms, log_date
        ) VALUES (
        :ip, :request_count, :unique_endpoints,
        :avg_response_ms, :log_date
        );
    """

    df = df.copy()
    df["log_date"] = log_date
    rows = df.to_dict(orient="records")

    with engine.connect() as conn:
        conn.execute(text(delete_sql), {"log_date": log_date})
        conn.execute(text(insert_sql), rows)
        conn.commit()

    logger.info(f"Loaded {len(rows)} top IPs for {log_date}")
    return len(rows)