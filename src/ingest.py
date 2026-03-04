import boto3
import logging
from datetime import datetime, date, timedelta
from dotenv import load_dotenv
import os 

load_dotenv()
logger = logging.getLogger(__name__)

def read_log_from_s3(target_date: str) -> list[str]:
    """
    Reads a raw log file from S3 and returns lines as a list.
    
    Why return a list of strings rather than a file object?
    The parser processes lines independently. A list makes that 
    iteration explicit and testable - you can pass any list 
    of strings to the parser without needing S3.
    
    Raises:
        FileNotFoundError: if no log file exists for target_date
    """
    bucket = os.getenv("S3_BUCKET_NAME")
    region = os.getenv("AWS_REGION")

    partition = (
        f"year={target_date.year}/month={target_date.strftime('%m')}"
        f"/day={target_date.strftime('%d')}/server.log"
    )
    logger.info(f"Reading s3://{bucket}/{partition}")

    s3 = boto3.client(
        "s3",
        region_name = region,
        aws_access_key_id = os.getenv("ACCESS_KEY_ID"),
        aws_secret_access_key = os.getenv("SECRET_ACCESS_KEY")
    )

    try:
        response = s3.get_object(Bucket=bucket, Key=partition)
        content = response["Body"].read().decode('utf-8')
        lines = content.splitlines()

        logger.info(f"Read {len(lines)} lines from S3")

        return lines 
    
    except s3.exceptions.NoSuchKey:
        raise FileNotFoundError(
            f"No log file found at s3://{bucket}/{partition}"
        )