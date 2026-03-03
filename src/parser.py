import re
import logging
import pandas as pd
from datetime import datetime, timezone
from typing import Optional
import yaml
from pathlib import Path

logger = logging.getLogger(__name__)

# Load pattern from config once at module level
# Compiling the regex once is significantly faster than
# recompiling on every line — the compiled pattern is cached
_CONFIG_PATH = Path(__file__).parent.parent / "config" / "config.yaml"
with open(_CONFIG_PATH) as f:
    _config = yaml.safe_load(f)

# re.VERBOSE allows whitespace and comments in the pattern
# re.compile caches the compiled pattern for reuse across all lines
LOG_PATTERN = re.compile(
    _config["log_format"]["pattern"],
    re.VERBOSE
)

TIMESTAMP_FORMAT = "%d/%b/%Y:%H:%M:%S %z"


def _parse_line(line: str) -> tuple[Optional[dict], Optional[str]]:
    """
    Attempts to parse a single log line into a structured dict.

    Returns a tuple of (parsed_dict, rejection_reason):
        - (dict, None)  → successful parse
        - (None, str)   → failed parse with specific reason

    Why a tuple instead of raising exceptions?
    The caller processes thousands of lines. Exception handling
    per line would be expensive and verbose. A tuple return
    makes the success/failure check a simple None comparison.

    Two distinct failure modes are captured separately:
        "pattern_mismatch"    → line doesn't match log format at all
        "invalid_field:name"  → matched but field value is unusable
    """
    line = line.strip()

    # Empty lines are the most common malformed case
    # Check first to avoid regex overhead on blank lines
    if not line:
        return None, "empty_line"

    # Attempt pattern match
    match = LOG_PATTERN.match(line)

    if not match:
        return None, "pattern_mismatch"

    # Extract all named groups into a dict
    fields = match.groupdict()

    # ── Field-level validation ────────────────────────────────
    # Pattern matched but values may still be invalid
    # These are Failure Mode 2 — format correct, values wrong

    try:
        status_code = int(fields["status"])
        if not (100 <= status_code <= 599):
            return None, f"invalid_field:status={fields['status']}"
    except ValueError:
        return None, f"invalid_field:status_not_integer={fields['status']}"

    try:
        response_time = float(fields["response_time"])
        if response_time < 0.0:
            return None, f"invalid_field:negative_response_time"
    except ValueError:
        return None, f"invalid_field:response_time_not_float"

    try:
        timestamp = datetime.strptime(
            fields["timestamp"],
            TIMESTAMP_FORMAT
        )
    except ValueError:
        return None, f"invalid_field:unparseable_timestamp={fields['timestamp']}"

    try:
        bytes_sent = int(fields["bytes"])
    except ValueError:
        return None, f"invalid_field:bytes_not_integer={fields['bytes']}"

    # ── Build clean record ────────────────────────────────────
    return {
        "ip":            fields["ip"],
        "timestamp":     pd.Timestamp(timestamp),
        "method":        fields["method"],
        "endpoint":      fields["endpoint"].split("?")[0],  # strip query params
        "status_code":   status_code,
        "status_category": f"{status_code // 100}xx",       # 200→"2xx", 404→"4xx"
        "bytes_sent":    bytes_sent,
        "response_time_ms": round(response_time * 1000, 2), # seconds → ms
        "window_start":  pd.Timestamp(timestamp).floor("h") # tumbling window
    }, None


def parse_logs(
    lines: list[str],
    log_date: str
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Parses a list of raw log lines into structured DataFrames.

    Processes every line regardless of failures — one bad line
    never stops processing of subsequent lines.

    Args:
        lines: raw log lines from S3
        log_date: date string YYYY-MM-DD for audit columns

    Returns:
        - parsed_df:   DataFrame of valid parsed log lines
        - rejected_df: DataFrame of failed lines with rejection reasons
    """
    logger.info(f"Parsing {len(lines)} log lines for {log_date}")

    parsed_rows = []
    rejected_rows = []

    for line in lines:
        record, rejection_reason = _parse_line(line)

        if record is not None:
            record["log_date"] = log_date
            parsed_rows.append(record)
        else:
            rejected_rows.append({
                "raw_line":        line[:500],  # truncate very long lines
                "rejection_reason": rejection_reason,
                "log_date":        log_date,
                "rejected_at":     datetime.now(timezone.utc)
            })

    parsed_df = pd.DataFrame(parsed_rows) if parsed_rows else pd.DataFrame()
    rejected_df = pd.DataFrame(rejected_rows) if rejected_rows else pd.DataFrame()

    # Summary statistics for logging
    total = len(lines)
    valid = len(parsed_rows)
    rejected = len(rejected_rows)
    rejection_rate = (rejected / total * 100) if total > 0 else 0

    logger.info(
        f"Parse complete — "
        f"valid: {valid}, "
        f"rejected: {rejected}, "
        f"rejection_rate: {rejection_rate:.1f}%"
    )

    if not rejected_df.empty:
        # Log breakdown of rejection reasons
        reason_counts = rejected_df["rejection_reason"].value_counts()
        logger.warning(f"Rejection breakdown:\n{reason_counts.to_string()}")

    return parsed_df, rejected_df


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    from pathlib import Path
    from datetime import date, timedelta

    yesterday = str(date.today() - timedelta(days=1))
    log_file = Path(f"data/logs/server_{yesterday}.log")

    with open(log_file) as f:
        lines = f.readlines()

    parsed_df, rejected_df = parse_logs(lines, yesterday)

    print(f"\nParsed shape: {parsed_df.shape}")
    print(f"Columns: {list(parsed_df.columns)}")
    print(f"\nFirst row:\n{parsed_df.iloc[0]}")
    print(f"\nRejection reasons:\n"
        f"{rejected_df['rejection_reason'].value_counts()}")
    print(f"\nStatus categories:\n"
        f"{parsed_df['status_category'].value_counts()}")
    print(f"\nSample window_starts:\n"
        f"{parsed_df['window_start'].unique()[:5]}")