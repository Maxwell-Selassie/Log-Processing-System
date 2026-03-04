# Technical Documentation — Log Processing Pipeline

This document records the engineering decisions made during the design and implementation of the server log processing pipeline. It explains not just what the code does, but why each decision was made, what alternatives were considered, and what would change at production scale.

---

## Table of Contents

1. [Why This Pipeline Exists](#why-this-pipeline-exists)
2. [Architecture Decisions](#architecture-decisions)
3. [Parser Design](#parser-design)
4. [Aggregation Design](#aggregation-design)
5. [Load Design](#load-design)
6. [Schema Management with Alembic](#schema-management-with-alembic)
7. [Observability Design](#observability-design)
8. [Scale Considerations](#scale-considerations)
9. [Known Limitations](#known-limitations)

---

## Why This Pipeline Exists


Server logs are fundamentally different. They are unstructured text — a single string per event with no field delimiters, no column names, no type information. The information is there, but extracting it requires pattern matching against the raw text. This is the core engineering challenge this pipeline was built to solve.


---

## Architecture Decisions

### Why S3 as the Starting Point

The pipeline's responsibility begins at S3 — not at the web server. In production, a log collection agent (Fluentd, Logstash, AWS CloudWatch Agent) runs on each server and streams log lines to S3 as they are written. This is a push model — logs arrive in S3 continuously without the pipeline polling for them.

Our pipeline is the processing layer, not the collection layer. These are separate concerns operated by different teams. Keeping them separate means the pipeline can be deployed, restarted, and backfilled independently of the log collection infrastructure.

For development, synthetic logs are generated and uploaded manually. In production this step is automated by the collection layer.

### Why S3 for Raw Log Preservation

Three reasons, each independently sufficient:

**Historical auditing.** Engineering teams investigating incidents need to examine raw logs from the time of the incident — not just aggregated metrics. Aggregations tell you that error rate spiked at 09:00. Raw logs tell you which specific requests failed, from which IPs, with which error messages. Without raw log preservation, post-incident analysis is severely limited.

**Reprocessability.** Aggregation logic will change — new metrics added, bugs fixed, granularity adjusted. When logic changes, historical data needs to be reprocessed. If only aggregated metrics exist, reprocessing requires re-collecting logs from the original servers. If raw logs are in S3, reprocessing reads from S3 — fast, free, no dependency on server availability.

**Schema drift protection.** Log formats change when servers are upgraded or reconfigured. If the parser fails on a new format, raw logs from before the format change are still safely stored. Fix the parser and reprocess — no data is lost.

This is the Bronze layer principle: preserve the ground truth before any transformation touches it.

### Why PostgreSQL for Aggregated Metrics

The metrics table is queried by dashboards — Grafana or similar tools that issue SQL queries for time-range aggregations. PostgreSQL is optimized for this pattern: indexed range scans on `window_start`, fast aggregations on numeric columns, and ACID guarantees that ensure dashboards always see consistent metric values.

S3 would be inappropriate for the metrics layer — it is not queryable via SQL without an intermediary like Athena, and Athena adds latency unsuitable for live dashboards. PostgreSQL provides millisecond query response times directly.

### Why Alembic for Schema Management

The metrics table schema is not fixed — it evolves as monitoring requirements grow. A new metric column (P99 response time, byte transfer rates) added without a migration framework means manually running ALTER TABLE statements in each environment, with no record of when the change was made or why.

Alembic provides three guarantees:

**Versioning.** Every schema change is a numbered migration file committed to version control. The history of every table modification is auditable.

**Environment consistency.** `alembic upgrade head` brings any environment — development, staging, production — to the exact same schema state. No manual coordination.

**Reversibility.** Every migration has an `upgrade` and `downgrade` function. If a schema change causes problems, `alembic downgrade -1` reverts it cleanly.

For a project this size, Alembic is optional. It was chosen deliberately to build the habit of schema management from day one — a decision that pays compounding returns as schemas grow more complex.

---

## Parser Design

### Why Regex Over String Splitting

Log lines could theoretically be parsed by splitting on spaces or delimiters. The Apache Combined Log Format has a predictable structure — fields separated by spaces, with some fields enclosed in brackets or quotes.

The problem with splitting: the request field `"GET /api/products?page=2 HTTP/1.1"` contains spaces. Splitting on spaces breaks this field into three parts. Handling this with split logic requires complex state management — tracking whether you're inside quotes, counting fields, handling edge cases.

Regex handles all of this in one pattern. Named groups — `(?P<ip>...)`, `(?P<timestamp>...)` — extract exactly the right content regardless of internal whitespace. The pattern is declarative: it describes what a valid log line looks like, and the regex engine handles all matching complexity.

### Why the Regex Pattern Lives in Config

The log format pattern is in `config.yaml`, not hardcoded in `parser.py`. This is the most important design decision in the parser module.

Web servers change their log formats. Apache and Nginx have different default formats. Custom fields are added by DevOps teams. A pattern hardcoded in Python requires a code change, a code review, and a deployment to update. A pattern in YAML requires one line edit.

More importantly: the format pattern is configuration data, not application logic. It describes the shape of an external data source. Like database connection strings and API URLs, it belongs in config, not code.

### Why Two Distinct Failure Modes

The parser distinguishes two types of failure:

**Failure Mode 1 — Pattern mismatch.** The line does not match the regex at all. `re.match()` returns `None`. The line is structurally invalid — it does not conform to the expected log format.

**Failure Mode 2 — Invalid field value.** The line matches the regex — all groups are captured — but extracted values are semantically wrong. Status code `999` is three digits (matches `\d{3}`) but is not a real HTTP status code. Timestamp `32/Jue/2025:25:61:00` matches the bracket pattern but cannot be parsed by `strptime`.

The distinction matters because the two modes have different root causes:

```
Pattern mismatch    →  log format changed, collection agent misconfigured,
                        or non-log content mixed into the file
                        Action: investigate log collection infrastructure

Invalid field value →  application server producing incorrect values,
                        clock synchronization bug, custom status codes
                        Action: investigate the application producing the logs
```

A rejection breakdown that only shows "parse failed" obscures which problem you have. A breakdown showing `pattern_mismatch: 200, invalid_field:status=999: 45` tells you immediately that the format is fine but the application is using non-standard status codes.

### Why Return a Tuple Instead of Raising Exceptions

`_parse_line()` returns `(dict, None)` on success and `(None, rejection_reason)` on failure rather than raising exceptions for invalid lines.

Processing 13,000 lines in a loop with exception handling per line would be expensive — Python exception handling has significant overhead compared to a simple None check. More importantly, a failed line is not an exceptional event in log processing — it is an expected, normal outcome that happens at a predictable rate. Exceptions should be reserved for unexpected failures, not expected data quality variations.

---

## Aggregation Design

### Why window_start × endpoint × status_category as Granularity

This three-dimensional granularity was chosen to maximize query flexibility for downstream consumers:

**By time** — filter `WHERE window_start BETWEEN ...` for any time range analysis.
**By endpoint** — filter `WHERE endpoint = '/api/payments/process'` for endpoint-specific monitoring.
**By status category** — filter `WHERE status_category = '5xx'` for error-only analysis.

Any coarser granularity — daily instead of hourly, all endpoints combined — would prevent certain queries. Any finer granularity — per-minute windows, per-status-code instead of per-category — would increase table size without meaningful additional insight for operational monitoring.

### Why Error Rate is Computed at window_start × endpoint Only

Error rate is the percentage of requests that returned a 4xx or 5xx status code. Computing it within `status_category` groups would produce meaningless results — every row in the `4xx` group would have 100% error rate by definition, and every row in the `2xx` group would have 0%.

Error rate is only meaningful when computed across all status categories for a given endpoint and window — "8% of all requests to this endpoint failed in this hour."

### Why P95 Over Average Response Time

Average response time is computed per group alongside P95. Both metrics serve different purposes:

**Average** — overall performance trend. Useful for capacity planning and long-term monitoring.

**P95** — tail latency. 95% of users experienced a response time at or below this value. A single 30-second timeout doesn't significantly raise the average for a busy endpoint, but it shows up clearly in P95. For user experience monitoring, P95 is the more honest metric.

Production systems typically monitor P50, P95, and P99 simultaneously. P50 (median) shows typical performance. P95 shows the experience for most users. P99 shows the worst-case experience for a small but significant minority.

### Why numpy.percentile Over pandas.quantile

Both produce the same result for well-populated groups. `numpy.percentile` was chosen because it handles edge cases — empty series, single-element series — more predictably when wrapped in a custom aggregation function applied via `groupby().apply()`.

A known limitation: P95 is statistically unreliable for groups with fewer than 20 samples. A window with 3 requests has a "P95" that is effectively just the maximum value — not a meaningful percentile. Production implementations flag low-sample windows with a `sample_size_warning` column so dashboards can suppress unreliable metrics.

---

## Load Design

### Three Idempotency Strategies

The load module uses three different strategies depending on the semantic requirements of each table:

**`hourly_metrics` — ON CONFLICT DO UPDATE (upsert)**

Metrics have a natural composite primary key: `(window_start, endpoint, status_category)`. This combination uniquely identifies one metric row. If the pipeline reruns for the same date, the existing rows are updated with recalculated values. No duplicates, no gaps, always reflects the most recent calculation.

**`rejected_logs` — WHERE NOT EXISTS**

Rejected log lines have no natural primary key. The same malformed line could theoretically appear in multiple log files on different days, and each occurrence is a distinct rejection event worth recording. `ON CONFLICT` requires a unique constraint — without one, PostgreSQL cannot determine what constitutes a duplicate.

`WHERE NOT EXISTS` checks content equality directly: don't insert this raw_line + log_date combination if it already exists. This prevents re-inserting the same rejected lines on pipeline reruns while allowing the same malformed pattern to appear across different dates.

**`top_ips` — DELETE then INSERT**

Top IP rankings are snapshot data — only the current day's rankings matter. Historical rankings for previous days are not meaningful for security monitoring (today's suspicious IP is what matters, not last Tuesday's). Delete-then-insert replaces the snapshot cleanly on every rerun without accumulating stale rows.

The decision framework for load strategy:

```
Natural unique key + needs updates      →  ON CONFLICT DO UPDATE
No natural unique key + avoid duplicates →  WHERE NOT EXISTS
Snapshot semantics (replace on rerun)   →  DELETE then INSERT
Append-only (history matters)           →  plain INSERT
```

---

## Schema Management with Alembic

Alembic tracks schema versions in the `alembic_version` table — a single row containing the current migration version identifier. Every time `alembic upgrade head` runs, it checks the current version, applies any unapplied migrations in sequence, and updates the version identifier.

Migration files in `alembic/versions/` are generated automatically via `alembic revision --autogenerate`. Autogenerate inspects the SQLAlchemy models in `models/tables.py` and compares them against the live database schema. Any difference — new table, new column, new index — becomes an `op.create_table()` or `op.add_column()` call in the migration file.

The workflow for any schema change:

```
1. Update models/tables.py with the new column or table
2. alembic revision --autogenerate -m "description_of_change"
3. Review the generated migration file — verify it does what you expect
4. alembic upgrade head — apply to current environment
5. Commit both the model change and the migration file to version control
```

This workflow ensures schema changes are always reviewed before being applied and always reversible via `alembic downgrade -1`.

---

## Observability Design

### Run ID Per Execution

Every pipeline run generates a UUID-based run ID at startup. This ID appears in every log line and in the log filename. When debugging a failure, grep for the run ID to isolate one execution's complete story from concurrent or sequential runs.

### Rejection Rate as Primary Health Metric

The pipeline summary reports rejection rate — rejected lines divided by total lines ingested. This is the single most important health signal for a log processing pipeline.

Normal rejection rate for this pipeline is approximately 2% — mostly empty lines and minor malformations that occur in any real server log file. A sudden spike indicates:

- `>5%` — worth investigating, possible upstream change
- `>15%` — likely a log format change or collection agent problem
- `>50%` — critical, pipeline is processing the wrong file or log format has changed completely
- `100%` — pipeline halts with error, zero valid lines is a data crisis not a normal state

In production, rejection rate would feed into an alerting system with dynamic thresholds based on rolling historical baseline rather than fixed percentages.

---

## Scale Considerations

### Sequential Parsing Becomes the Bottleneck

The current parser processes lines sequentially in a Python loop. For 14,000 lines this takes under 2 seconds. At 10x volume (140,000 lines) it remains fast — under 20 seconds. At 100x volume (1,400,000 lines) sequential parsing becomes the pipeline's bottleneck — potentially several minutes for the parsing stage alone.

The solution is chunked parallel processing:

```python
# Divide lines into chunks, process each chunk in a separate process
from multiprocessing import Pool

chunks = [lines[i:i+10000] for i in range(0, len(lines), 10000)]
with Pool(processes=4) as pool:
    results = pool.map(parse_chunk, chunks)
```

At very large scale (hundreds of millions of log lines per day), PySpark replaces the Python parser entirely — the same regex logic distributed across a cluster.

### Single Log File Per Day

The current partition structure stores one log file per day per server. At high traffic volumes, a single daily log file for a busy server can be several gigabytes. Reading a multi-gigabyte file into memory at once is not viable.

The solution is streaming reads with chunked processing — read and parse the file in blocks rather than loading it entirely into memory. S3's `get_object` with `Range` headers supports byte-range reads for exactly this purpose.

The partition structure would also change from daily to hourly for high-volume servers — smaller files, faster reads, and more granular reprocessing when issues occur.

---

## Known Limitations

**Single server log format.** The parser supports one log format defined in `config.yaml`. Production environments often have multiple servers with different formats (Apache, Nginx, custom application logs). Supporting multiple formats would require a format detection stage before parsing, or separate pipeline instances per format.

**No real-time alerting.** The pipeline runs in batch mode — alerts fire hours after problems occur. Real-time alerting requires streaming processing (Kafka + Flink or Spark Streaming) where metrics are computed on a rolling window as log lines arrive.

**No data retention policy.** Raw logs and rejected logs accumulate indefinitely. Production systems define retention policies — raw logs older than 90 days archived to S3 Glacier, rejected logs older than 30 days purged, metrics older than 2 years aggregated to daily granularity.

**P95 unreliable for low-traffic windows.** Hourly windows with fewer than 20 requests produce statistically unreliable P95 values. Production implementations flag these with a `sample_size_warning` column so dashboards can suppress or visually distinguish low-confidence metrics.

**No log deduplication.** If a log collection agent delivers the same log file twice (a known failure mode for at-least-once delivery systems), the pipeline processes duplicate lines. The metrics upsert handles this correctly, but rejected_logs and top_ips may contain duplicated data. A content hash on the raw log file would detect and skip duplicate deliveries.