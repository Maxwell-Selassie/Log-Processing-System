import pandas as pd
import logging 
import numpy as np
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

def categorize_status(status_code: int) -> str:
    """
    Maps a status code to its category string.
    200 -> "2xx", 404 -> "4xx", 503 -> "5xx"
    
    Integer division by 100 gives the category digit.
    This is the grouping that makes error rate meaningful - 
    you care about "what percentage failed" not "what 
    percentage got exactly 503 vs exactly 500."
    """
    return f"{status_code // 100}xx"

def compute_error_rate(status_codes: pd.Series) -> float:
    """
    Computes the percentage of requests that returned 
    a 4xx or 5xx status code.
    
    Error rate = (4xx count + 5xx count) / toatal count * 100 
    
    Why include 4xx? 
    4xx errors (Bad Request, Unauthorized, Not Found) indicate
    client-side problems but still represent failed requests
    from the user's perspective. A spike in 4xx often indicates
    a broken client deployment or an attack
    """
    total = len(status_codes)
    if total == 0:
        return 0.0
    
    errors = status_codes[status_codes >= 400].count()
    return round((errors / total) * 100, 2)

def compute_p95(response_times: pd.Series) -> float:
    """
    Compute the 95th percentile response time.
    
    Why p95 over average?
    Averages hide outliers. If 95% of requests complete in
    50ms but 5% takes 10 seconds, the average might be 550ms - 
    misleadingly high. P95 tells you: 95% of your users
    experienced a response time at or below this value.
    That's the metric that reflects real user experience.
    
    numpy.percentile is used over pandas.quantile because 
    it handles edge cases (empty series, single value) more
    predictably for our use case.
    """
    if len(response_times) == 0:
        return 0.0
    
    return round(float(np.percentile(response_times, 95)), 2)

def aggregate(parsed_df: pd.DataFrame, log_date: str) -> pd.DataFrame:
    """
    Computes hourly metrics per endpoint per status category
    from a DataFrame of parsed log lines.
    
    Granularity: one row per window_start x endpoint x status_category
    
    This is the Gold layer computation - transforming millions
    of individual log lines into a compact, queryable metrics
    table that dashboards and alerts can query efficiently.

    Args: 
        parsed_df: output of parser.parse_logs - one row per valid log line
        log_date: date string YYYY-MM-DD for audit column

    Returns:
        DataFrame with one row per window/endpoint/status combination
    """
    if parsed_df.empty:
        logger.warning("Empty DataFrame passed to aggregate - nothing to aggregate")
        return pd.DataFrame()
    
    # Group by the three dimensions
    # window_start -> tumbling hourly window
    # endpoint -> API route
    # status category -> 2xx, 4xx, 5xx

    # Every metric is computed within each group independently.
    # A group is one specific hour + one specific endpoint +
    # one specific status category.
    group_keys = ["window_start", "endpoint", "status_category"]
    
    grouped = parsed_df.groupby(group_keys)

    # compute metrics per groups
    # agg() applies multiple functions to specific columns simultaneously
    # Named aggregations (new_name=("column","function")) keep output clean
    metrics_df =  grouped.agg(
        request_count = ("endpoint", "count"),
        avg_response_ms = ("response_time_ms", "mean"),
        total_bytes = ("bytes_sent", "sum")
    ).reset_index()

    # P95 requires custom computation
    # pandas agg() doesn't support percentile directly with named syntax
    # compute separately and merge back 
    p95_df = (
        parsed_df.groupby(group_keys)["response_time_ms"]
        .apply(compute_p95).reset_index()
        .rename(columns={"response_time_ms" : "p95_response_ms"})
    )

    metrics_df = metrics_df.merge(p95_df, on=group_keys)

    # Error rate requires the full status code series
    # status_category alone doesn't tell us error rate within a group
    # we need to look at the raw status codes within each window/endpoint
    # combination regardless of category
    # Solution: compute error rate at window x endpoint level
    # (ignoring status_category) then merge back
    error_rate_df = (
        parsed_df.groupby(["window_start", "endpoint"])["status_code"]
        .apply(compute_error_rate).reset_index()
        .rename(columns={"status_code" : "error_rate"})
    )

    metrics_df = metrics_df.merge(error_rate_df, on=["window_start", "endpoint"])


    # round numeric columns
    metrics_df["avg_response_ms"] = metrics_df["avg_response_ms"].round(2)

    # add audit column
    metrics_df["log_date"] = log_date
    metrics_df["loaded_at"] = datetime.now(timezone.utc)

    final_columns = [
        "window_start",
        "endpoint",
        "status_category",
        "request_count",
        "error_rate",
        "avg_response_ms",
        "p95_response_ms",
        "total_bytes",
        "log_date",
        "loaded_at"
    ]

    metrics_df = metrics_df[final_columns]

    logger.info(
        f"Aggregation complete - "
        f"{len(metrics_df)} metric rows produced "
        f"from {len(parsed_df)} log lines "
        f"({parsed_df['endpoint'].nunique()} endpoints, "
        f"{parsed_df['window_start'].nunique()} windows)"
    )

    return metrics_df

def get_top_ips(parsed_df: pd.DataFrame, top_n: int = 10) -> pd.DataFrame:
    """
    Identifies the top N IP addresses by request volume.
    
    Stored separately from hourly_metrics because IP-level
    data has different retention and access control requirements
    than performance metrics - security teams need it, 
    product teams usually don't.
    
    Also useful for detecting suspicious traffic patterns - 
    a single IP making thousands of requests per hour is
    likely a bot or an attack.
    """
    if parsed_df.empty:
        return pd.DataFrame()

    top_ips = (
        parsed_df.groupby("ip").agg(
            request_count = ("endpoint", "count"),
            unique_endpoints = ("endpoint", "nunique"),
            avg_response_ms = ("response_time_ms", "mean")
        )
        .reset_index()
        .sort_values("request_count", ascending=False)
        .head(top_n)
    )

    top_ips["avg_repsonse_ms"] = top_ips["avg_response_ms"].round(2)

    return top_ips


if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.INFO)
    from pathlib import Path
    from datetime import date, timezone, timedelta
    import sys 

    sys.path.insert(0, str(Path(__file__).parent.parent))
    from src.parser import parse_logs

    yesterday = str(date.today() - timedelta(days=1))
    log_file = Path(f'data/logs/server_{yesterday}.log')

    with open(log_file, "r")  as file:
        lines = file.readlines()

    parsed_df, _ = parse_logs(lines, yesterday)
    metrics_df = aggregate(parsed_df, yesterday)
    top_ips = get_top_ips(parsed_df)

    print(f"\nMetrics shape: {metrics_df.shape}")
    print(f"Columns: {list(metrics_df.columns)}")
    print(f"Sample metrics: \n{metrics_df.head(5).to_string()}")
    print(f'Top 5 IPs: \n{top_ips.head(5).to_string()}')
    print(f"Endpoints tracked: {metrics_df['endpoint'].nunique()}")
    print(f"Windows tracked: {metrics_df['window_start'].nunique()}")
    print(f"Status categories: {metrics_df['status_category'].nunique()}")