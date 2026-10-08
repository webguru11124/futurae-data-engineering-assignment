# Event metrics pipeline

This project cleans a sample of streaming service events and aggregates them into per-service,
per-minute request metrics. A small FastAPI app serves the result. The assignment brief is in
[HomeAssignment.pdf](HomeAssignment.pdf).

## Quick start

```bash
python -m venv .venv
source .venv/bin/activate            # Windows: .venv\Scripts\activate
pip install -e ".[dev]"

event-pipeline                       # data/events.jsonl -> data/output/
uvicorn event_metrics.api.app:create_app --factory    # docs at http://127.0.0.1:8000/docs
pytest                               # 88 tests; ruff and mypy --strict are clean too
```

```bash
curl "localhost:8000/metrics?service=checkout&from=2025-01-12T10:00:00Z&to=2025-01-12T10:05:00Z"
curl "localhost:8000/metrics/summary?from=2025-01-12T10:00:00Z&to=2025-01-12T11:00:00Z"
```

Built with Python 3.11, FastAPI 0.142 and Pydantic 2.13. The pipeline itself uses only the
standard library. The outputs from the sample are committed in `data/output/` so you can inspect
them without running anything.

## Layout

Each stage is written as a per-element function or a mergeable combiner, so moving it to
Beam/Dataflow is mechanical:

| Module | What it does | Beam equivalent |
|---|---|---|
| `parsing.py` | Field normalisers: timestamp, latency, status code | – |
| `cleaning.py` | Validates one record and applies the drop-vs-keep policy | `ParDo` with a dead-letter output |
| `dedup.py` | First copy of each `event_id` wins | Stateful `DoFn` keyed by `event_id`, with a TTL |
| `aggregation.py` | Per-service, per-minute stats that merge exactly | `FixedWindows(60)` + `CombinePerKey(CombineFn)` |
| `pipeline.py` | Batch driver and CLI; writes outputs atomically | Pipeline graph |
| `api/` | FastAPI app factory, repository, schemas, routes | – |

`tests/` has unit tests for each stage, API tests, and a regression test on the sample file. The
expected counts in that test were checked against a separate profile of the raw data.

---

## Part 1: Cleaning and aggregation

### What was in the data and how it is handled

Of 2,020 records, **1,964 are clean, 36 are rejected (1.8%) and 20 are duplicates that were
dropped**. Aggregation produces 719 service-minute rows. Full counts are in
`data/output/run_summary.json`.

| Issue found in the sample | Events | Handling |
|---|---:|---|
| `timestamp` missing, `null` or `""` | 24 | **Reject** |
| `timestamp` impossible (`2025-13-40T25:61:61Z`) | 6 | **Reject** |
| `service` missing | 7 | **Reject** (one row also lacks a timestamp) |
| Same `event_id` seen again: 18 of 19 groups differ only by a timestamp shifted 1–5 s | 20 | **Drop later copies** |
| `timestamp` in another format: `12/01/2025 10:00:00`, `2025-01-12 10:00:00Z` | 10 | Parse to UTC and flag |
| `latency_ms` as `"123ms"` / `status_code` as `"200"` | 3 / 2 | Convert to integer and flag |
| `latency_ms` missing, `""`, `null` or negative | 14 | Set to null and flag |
| `status_code` missing, `null`, `"ERR"`, `0` or `700` | 19 | Set to null and flag |
| `user_id` missing | 9 | Set to null and flag |
| Unknown `extra_field` (string, object, bool or int) | 20 | Keep in `extra_fields` and flag |
| `request_failed` with a 2xx status | 63 | Keep and flag (errors are defined by status code) |
| Latency outliers (3.5–21.5 s) | a few | **Keep**: these are real signal |

### Drop vs keep

- **Reject** an event when it can't be placed in the metrics:
  - `event_id` is the dedup key.
  - `timestamp` decides the window.
  - `service` is the grouping key.
  - `event_type` says whether the event is a request at all.

  Rejected events are never dropped silently. They go to `rejected_events.jsonl` with the line
  number, *all* the reasons and the raw line, so they can be replayed once the cause is fixed.
- **Keep the event, set the field to null and flag it** when only a measurement is bad. The
  request still happened, so it still counts towards `request_count`. It is left out of the
  denominator of the one metric that needs the missing value. Dropping it would under-count
  traffic, and filling in a guessed value would make up data.
- Every kept event has `quality_flags`. Analytics and ML users can filter down to rows with no
  flags, or study the issues.
- Validation runs before deduplication, so a broken copy of an event can't hide a valid one.

### Definitions (judgement calls)

- **A request is a terminal event**: `request_completed` or `request_failed`. A
  `request_started` event has no outcome yet, so counting it too would count twice every request
  that sends both events. Its latency and status values can't be meaningful at start time. These
  events stay in the clean data and are only left out of the metrics. The rule lives in one
  constant, `TERMINAL_EVENT_TYPES`.
- **An error is a status code outside 200–299**, as the brief specifies. That means 3xx counts as
  an error (22 of the 569 errors here). The event type is not used for this; a `request_failed`
  with a 2xx status is flagged instead.
- **`error_rate`** is computed over requests with a valid status code, and **`avg_latency_ms`**
  over requests with a valid latency. Both are null when there is nothing to measure.
- **Rows store additive parts**: counts, sums and the max, alongside the derived rates. Any time
  range can then be rolled up exactly; `/metrics/summary` does this. Averaging per-minute
  averages would give the wrong answer.
- **Windows** are 1-minute tumbling windows on event time, in UTC. The input arrives out of order
  (only about half of adjacent lines are in time order), and the aggregation doesn't depend on
  order. A minute with no requests has no row.
- **Dedup keeps the first copy.** The data can't tell us which copy has the "right" timestamp.
  Keeping the first one seen is what a streaming dedupe can do without taking back output it
  already emitted, and it gives the same result for the same input order.

### Things to raise with the event producers

- **All 10 oddly formatted timestamps are exactly `10:00:00`.** That looks like a default value
  rather than a real time. They push payments' 10:00 minute to 7 requests, against a typical 2
  per minute. I followed the brief and normalised them, and they are flagged. If they turn out to
  be placeholders, rejecting them is a one-line change, or downstream users can filter on the
  flag.
- **`12/01/2025` is ambiguous.** I read it day-first (12 January), which matches every other
  event; month-first would put it 11 months later. The real fix is a fixed timestamp format in
  the producer contract.

### Outputs (`data/output/`)

| File | Contents |
|---|---|
| `clean_events.jsonl` | One row per unique valid event: typed fields, `event_time` in UTC, `is_error`, `quality_flags`, `extra_fields` |
| `rejected_events.jsonl` | Dead-letter records: `line_number`, `reasons`, `raw` |
| `metrics_per_minute.jsonl` | `service`, `window_start`, `request_count`, `error_count`, `error_rate`, `avg_latency_ms`, `max_latency_ms`, plus the additive parts |
| `run_summary.json` | Data-quality report: counts per reject reason and per quality flag |

---

## Part 2: Storage and modelling

```
Pub/Sub ─► Dataflow ─┬─► BigQuery events_raw            every message, untouched
                     ├─► BigQuery events_dead_letter    rejected events + reasons
                     ├─► BigQuery events_clean          typed, deduplicated, flagged
                     └─► 1-min windows ─┬─► BigQuery service_metrics_minute   history, SQL
                                        └─► Bigtable service_metrics          low-latency reads
```

**Raw events go to BigQuery `events_raw`.** Every message is stored before any parsing:

- Columns: the payload as `JSON` (or `STRING` if it isn't valid JSON), plus `message_id`,
  `publish_time` and `ingest_time`.
- Partitioned by ingestion day, with a partition expiry of 30–90 days. A cheaper archive can go to
  GCS if longer retention is needed.

It exists for replay and backfill when the cleaning rules change (for example if the `10:00:00`
timestamps turn out to be fake), and for audit and debugging. Because the payload has no fixed
schema, a producer change can never break ingestion. A Pub/Sub BigQuery subscription can do this
with no code.

**Clean events go to BigQuery `events_clean`**, with the same columns as `clean_events.jsonl`:

- `quality_flags` is an `ARRAY<STRING>` and `extra_fields` is `JSON`.
- `PARTITION BY DATE(event_time) CLUSTER BY service, event_type`, because nearly every query
  filters on time and service.

BigQuery fits because the readers here are analytics (scans, joins) and ML feature engineering.
Rejected events go to `events_dead_letter` with the reasons and the pipeline version.

**Aggregated metrics go to both stores, because they serve different readers:**

- **BigQuery `service_metrics_minute`** (partitioned by day, clustered by `service`) is the
  system of record. It serves dashboards, trends and joins. It stores the additive parts, so
  hourly and daily rollups are exact `SUM`s.
- **Bigtable `service_metrics`** serves the API and live dashboards:
  - The row key is `service#yyyyMMddHHmm`, so `GET /metrics?service=&from=&to=` is a single
    contiguous range scan with single-digit-millisecond reads.
  - Columns are the counts and sums.
  - A max-age garbage-collection policy (say 30 days) keeps it a serving cache.

  BigQuery takes seconds per query and charges per query, which is wrong for an endpoint polled
  every few seconds.
- **Honest note:** at today's volume (5 services × 1,440 minutes ≈ 7k rows a day), BigQuery plus
  a cache in the API would be enough. Bigtable starts to pay off when keys become high-cardinality
  (per endpoint, per customer, or per-user features for online ML serving) or when read traffic
  is high.
- **Late data:** streaming windows fire again when late events arrive. Bigtable writes are
  idempotent puts of the full window value, so the newest one wins. In BigQuery, either upsert on
  `(service, window_start)` with the Storage Write API's change-data-capture support, or append
  every update and expose a view that picks the latest one per key.

**Schema evolution:**

- **Producer contract.** Events carry a `schema_version`, and the schema is registered as a
  Pub/Sub schema (Avro or Protobuf, with revisions). Producer CI checks compatibility, so a
  breaking change is caught before it is published.
- **Only backward-compatible changes in place:** new optional fields. Never rename a field,
  change its type or change its meaning in place.
- **The raw layer accepts anything**, since it is JSON.
- **The clean layer doesn't lose unknown fields.** They land in `extra_fields`, as `extra_field`
  does today, and the `unknown_fields` flag rate shows the drift. To promote one to a real
  column: BigQuery `ADD COLUMN` (nullable, a metadata-only change), update the pipeline, then
  backfill from raw.
- **Breaking changes** get a new field or a versioned table. The steps:
  1. Write to both the old and new versions.
  2. Backfill from raw.
  3. Move consumers to the new version, then retire the old one.

  Consumers read through views, so their interface stays stable.
- **Metric definitions are versioned too.** If, say, 3xx should stop counting as an error, that
  becomes a new metric, not a silent rewrite of history; otherwise dashboards show a step change
  that never happened.
- **The API** wraps responses in `{"items": [...]}`, so things like pagination can be added
  without breaking clients. Breaking changes would go under `/v2`.

---

## Part 3: API

| Endpoint | Returns |
|---|---|
| `GET /metrics?service=&from=&to=` | Per-minute rows. `from` is inclusive and `to` exclusive, so adjacent ranges never count anything twice. |
| `GET /metrics/summary?service=&from=&to=` | One exact rollup per service over the range |
| `GET /health` | Liveness, plus `latest_window` to show how fresh the data is |

All parameters are optional. Datetimes are ISO 8601; values without a timezone are read as UTC.
Use `Z`, or URL-encode `+` as `%2B`.

- **Structure.** An app factory builds the app, and the repository is injected as a FastAPI
  dependency. Tests run against in-memory data, and a Bigtable-backed repository could replace it
  without touching the routes.
- **Unknown service returns 404** and lists the known services, so a typo isn't mistaken for "no
  traffic". A known service with no data in the range returns `200` with an empty list.
- **Errors.** A reversed range returns 422 in FastAPI's own error format, so clients only have to
  parse one error shape.
- **Startup.** The app fails fast if the metrics file is missing, instead of serving empty
  results. The file path comes from the `EVENT_METRICS_PATH` environment variable.
- **Sync handlers**, because the repository is in memory and CPU-only. With a network-backed
  store I would switch to async clients and `async def`.

---

## Part 4: Running it in production

### What I would monitor

- **Freshness and lag:** age of the oldest unacknowledged Pub/Sub message, Dataflow system lag
  and watermark lag. Together these tell you how stale the metrics are.
- **Completeness:** `records in = clean + rejected + duplicates` for each window. This is the same
  check the tests make; if it doesn't hold, data is being lost.
- **Data quality:** reject rates by reason and quality-flag rates, per service, taken from the
  same counters as `run_summary.json`.
  - Alert on changes against a baseline rather than fixed thresholds: if `latency_invalid` jumps
    for one service, a producer deploy probably broke something.
  - A jump in the duplicate rate points to an upstream retry storm.
  - The `unknown_fields` rate is a schema-drift detector.
- **Late events:** count events that arrive after their window has closed.
- **Volume per service** compared with the same time last week. A service that goes quiet is an
  outage, not "zero errors".
- **The API:** latency, 5xx rate, and `latest_window` compared with the current time.

### What could break as volume grows

- **Dedup state grows without limit.** It is an in-memory set today. It needs keyed state with a
  TTL; duplicates here arrive within seconds, so about 10 minutes is plenty. Copies arriving after
  the TTL can be caught by a periodic `MERGE` on `event_id` in BigQuery.
- **Everything runs in one process with every clean event in memory.** That needs Dataflow's
  parallelism, and the per-element design makes the move mechanical.
- **Hot keys.** There are only 5 service keys, so one busy service can bottleneck a worker. A
  `CombineFn` pre-aggregates before the shuffle, and hot-key fanout spreads the rest.
- **Late and out-of-order data.** A batch run sees everything. Streaming needs a chosen allowed
  lateness and trigger policy, and late updates must be idempotent upserts.
- **Percentiles can't be merged exactly.** They need sketches: KLL or t-digest, or BigQuery's
  `APPROX_QUANTILES` and KLL functions.
- **The API loads everything into memory and returns lists of any length.** It needs a real store
  behind it, pagination or a maximum range, and caching.

### What I would do next with more time

1. **Latency percentiles (p50/p95/p99) using sketches.** The average misleads here:
   - Checkout's 10:09 minute averages 7,222 ms because one 21.5 s request sits among three.
   - Overall, mean latency is 270 ms while the median is 178 ms.
2. **Event-time plausibility checks.** Reject events stamped far in the future relative to
   processing time, or older than retention, and add late-data counters.
3. **Port to Apache Beam** using the mapping in the Layout table, and add property-based tests
   (Hypothesis) for the parsers.
4. **A data contract shared with producers**, so values like `"123ms"` and `12/01/2025` are fixed
   at the source instead of guessed at downstream.
