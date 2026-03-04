import logging 
import os
from dotenv import load_dotenv
import uuid 
from datetime import datetime, date, timedelta, timezone
from pathlib import Path
import sys 

load_dotenv()

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.ingest import read_log_from_s3
from src.parser import parse_logs
from src.aggregator import aggregate, get_top_ips
from src.load import get_engine, upsert_metrics, insert_injected_logs, load_top_ips

def setup_logging(run_id: str) -> None:
    log_dir = Path("logs/")
    log_dir.mkdir(parents=True, exist_ok=True)

    log_format = (
        f"%(asctime)s | run={run_id} | "
        f"%(levelname)s | %(name)s | %(message)s"
    )

    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_dir / f"pipeline_{run_id}.log")
        ]
    )

def run_pipeline(target_date: str | None = None) -> None:
    """
    Orchestrates the complete log processing pipeline.
    
    Stage order:
        0. Setup - run ID, logging, DB engine
        1. Ingest - read raw log file from S3
        2. Parse - extract structure from raw text
        3. Aggregate - compute hourly metrics
        4. Load - upsert metrics to PostgreSQL
        
    Args:
        target_date : date to process. Defaults to yesterday UTC.
        Accepts explicit date for manual backfills    
    """
    run_id = str(uuid.uuid4())[:8]
    setup_logging(run_id)
    logger = logging.getLogger(__name__)

    if target_date is None: 
        target_date = datetime.now(timezone.utc).date() - timedelta(days=1)

    date_str = target_date.strftime("%Y-%m-%d")
    start_time = datetime.now(timezone.utc)

    logger.info("="*60)
    logger.info("LOG PIPELINE STARTING")
    logger.info(f"Run ID : {run_id}")
    logger.info(f"Target Date : {date_str}")
    logger.info("="*60)

    # Stage 0: Database engine
    try:
        engine = get_engine()
        logger.info(f"Database engine created successfully")
    
    except Exception as e:
        logger.error(f"Failed to create database engine: {e}")
        sys.exit(1)

    # Stage 1: Ingest
    try:
        lines = read_log_from_s3(target_date)
        logger.info(f"Ingested {len(lines)} log lines from S3")

    except FileNotFoundError as e:
        logger.error(f"Log file not found in S3: {e}")
        logger.error(f"Check log delivery - pipeline halted")
        sys.exit(1)

    except Exception as e:
        logger.error(f"Ingestion failed: {e}")
        sys.exit(1)


    # stage 2: Parse
    try:
        parsed_df, rejected_df = parse_logs(lines, date_str)

    except Exception as e:
        logger.error(f"Parsing failed unexpectedly: {e}", exc_info=True)
        sys.exit(1)

    if parsed_df.empty:
        logger.error("Zero valid log lines after parsing - pipeline halted")
        sys.exit(1)

    # stage 3: Aggregate
    try:
        metrics_df = aggregate(parsed_df, date_str)
        top_ips_df = get_top_ips(parsed_df)

    except Exception as e:
        logger.error(f"Aggregation failed: {e}", exc_info=True)
        sys.exit(1)

    if metrics_df.empty:
        logger.error(f"Aggregation produced no metrics - pipeline halted")
        sys.exit(1)

    # stage 4: Load
    try:
        metrics_count = upsert_metrics(metrics_df, engine)
        rejected_count = insert_injected_logs(rejected_df, engine)
        top_ips_count = load_top_ips(top_ips_df, engine, date_str)

    except Exception as e:
        logger.error(f"Load stage failed: {e}", exc_info=True)
        sys.exit(1)

    # Pipeline summary
    duration = (datetime.now(timezone.utc) - start_time).total_seconds()
    rejected_rate = (
        len(rejected_df) / len(lines) * 100 if lines else 0
    )

    logger.info("="*60)
    logger.info("LOG PIPELINE COMPLETE")
    logger.info(f"Run ID : {run_id}")
    logger.info(f"Target date : {date_str}")
    logger.info(f"Duration L {duration:.2f}s")
    logger.info(f"Lines ingested : {len(lines)}")
    logger.info(f"Lines parsed : {len(parsed_df)}")
    logger.info(f"Lines rejected : {len(rejected_df)} ({rejected_rate:.2f}%)")
    logger.info(f"Metric rows : {metrics_count}")
    logger.info(f"Top IPs tracked : {top_ips_count}")
    logger.info(f"Endpoints tracked: {metrics_df['endpoint'].nunique()}")
    logger.info(f"Windows tracked : {metrics_df['window_start'].nunique()}")
    logger.info("="*60)

if __name__ == "__main__":
    run_pipeline()