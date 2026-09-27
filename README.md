# Cyber Insurance Data Engineering Pipeline

An end-to-end batch data pipeline for ingesting, processing, and transforming cybersecurity event and customer data using **Apache Airflow, AWS S3, Snowflake, dbt, Docker, and Python**.

The pipeline ingests data from an API, lands immutable raw files in S3, loads them into Snowflake, and uses dbt to build staging, intermediate, historical, and analytics-ready models.

The implementation also handles several common data engineering concerns, including duplicate records, late-arriving data, incremental processing, changing customer attributes, data quality validation, retries, and pipeline idempotency.

## Tech Stack

- Python
- FastAPI
- Apache Airflow
- Docker / Docker Compose
- AWS S3
- Snowflake
- dbt
- SQL

## Architecture

The pipeline follows this flow:

**FastAPI → Airflow → AWS S3 → Snowflake RAW → dbt staging/intermediate → dbt snapshots → analytics marts**

The FastAPI application acts as the source system. Airflow handles extraction and orchestration, S3 provides the raw landing layer, Snowflake is used as the data warehouse, and dbt handles transformation, incremental processing, historical tracking, and data quality testing.

## Pipeline Workflow

The Airflow DAG consists of five main tasks:

1. `extract_and_land`
2. `copy_into_snowflake`
3. `dbt_staging_and_intermediate`
4. `dbt_customer_snapshot`
5. `dbt_marts_and_tests`

The DAG is scheduled hourly and can also be triggered manually.

### API Ingestion

The `producer` service is a FastAPI application that exposes two endpoints:

- `/events`
- `/customers`

The API simulates changing source data rather than returning a completely static dataset.

Security events include scenarios such as duplicate records, updated events, and late-arriving events. Customer records can also change between requests, which allows customer history to be tracked downstream.

Airflow extracts both datasets, validates the responses, converts them to newline-delimited JSON (NDJSON), and lands each batch in S3.

Files are stored using date-partitioned paths with unique filenames so that new pipeline runs do not overwrite previous batches.

Example:

`raw/security_events/year=2026/month=09/day=27/<run_id>.json`

`raw/customers/year=2026/month=09/day=27/<run_id>.json`

This keeps the S3 landing layer append-oriented and preserves the original ingestion history.

## Snowflake RAW Layer

Snowflake external stages reference the S3 landing locations.

Airflow executes `COPY INTO` statements to load the files into:

- `CYBER_INSURANCE.RAW.SECURITY_EVENTS`
- `CYBER_INSURANCE.RAW.CUSTOMERS`

The RAW layer intentionally stays close to the source data. Duplicate records and multiple versions of the same business entity are preserved instead of being removed during ingestion.

An `ingestion_timestamp` is added when records enter Snowflake so that source event time can be separated from pipeline arrival time.

## dbt Transformation Layers

The dbt project is organized into three primary modeling layers:

### Staging

The staging models provide a clean interface over the RAW Snowflake tables and prepare the source data for downstream transformations.

Models include:

- `stg_security_events`
- `stg_customers`

### Intermediate

The intermediate layer handles business-level transformation logic such as deduplication and identifying the latest version of a record.

Models include:

- `int_latest_events`
- `int_latest_customers`

### Analytics

The final analytics layer contains:

- `dim_customers`
- `fct_security_events`
- `daily_security_summary`

The staging and intermediate models are primarily materialized as views, while the analytics models are materialized as tables.

## Deduplication

The source API intentionally produces duplicate versions of security events.

Rather than removing those records during ingestion, the duplicates are preserved in RAW and resolved in the dbt intermediate layer.

`int_latest_events.sql` uses the event business key and record timestamps to determine which version of an event should continue downstream.

This keeps ingestion relatively simple while preserving the original source records for troubleshooting and reprocessing.

## Late-Arriving Data

The pipeline also accounts for events that arrive significantly later than when they occurred.

For example, an event may have:

- `event_timestamp` — when the security event actually happened
- `updated_at` — when the source record was last updated
- `ingestion_timestamp` — when the record entered the data platform

A record could therefore have an `event_timestamp` and `updated_at` from several days ago while having an `ingestion_timestamp` from the current pipeline run.

This distinction matters for incremental processing.

Using only `updated_at` with a short lookback window could miss a record that arrives several days late. The fact model therefore uses `ingestion_timestamp` as the primary processing signal for newly arrived data.

A lookback window is also applied so that a small amount of previously processed data is reconsidered during each incremental run.

## Incremental Fact Processing

`fct_security_events` is implemented as a dbt incremental model.

The incremental strategy combines:

- `ingestion_timestamp`
- a lookback window
- event-level deduplication
- a unique business key
- merge-based incremental processing

The lookback intentionally reprocesses a small amount of recently ingested data. This helps account for processing boundaries and delayed records.

Because events are deduplicated and matched using their business key, rereading records from the lookback window does not result in duplicate facts.

This keeps the incremental load idempotent while still allowing recently arrived data to be reconsidered.

## Customer History — SCD Type 2

Customer attributes can change between source API requests.

For example, a customer's `plan_tier` may change from `standard` to `premium`.

Instead of overwriting the previous state, customer history is maintained using a dbt snapshot:

`dbt/snapshots/customer_history.sql`

The snapshot implements Slowly Changing Dimension Type 2 behavior using dbt validity fields.

This preserves historical customer versions while still making it possible to identify the current version of each customer.

## Analytics Models

### `dim_customers`

Provides the current customer dimension for downstream analytics.

### `fct_security_events`

Contains the deduplicated security events processed through the incremental pipeline.

### `daily_security_summary`

Aggregates security activity at the daily level for reporting and analytical use cases.

Together, these models provide a simple dimensional layer on top of the cleaned and historical data.

## Data Quality

dbt tests are included as part of the pipeline rather than being treated as a separate manual validation step.

The tests validate assumptions such as:

- unique business keys
- required non-null fields
- relationships between datasets

The final Airflow task executes the analytics models followed by the dbt test suite.

If the tests fail, the Airflow task fails as well, preventing an invalid pipeline run from being treated as successful.

## Airflow Orchestration

Airflow manages dependencies between ingestion, warehouse loading, dbt transformations, snapshots, and tests.

The DAG is configured with retries for transient failures and:

`max_active_runs=1`

This prevents a new scheduled pipeline run from overlapping with an unfinished previous run.

The local environment uses Airflow's `LocalExecutor`.

For this workload, LocalExecutor parallelism is set to `4`. The pipeline is mostly sequential, so the default parallelism of `32` created unnecessary worker processes and resource usage without providing additional throughput.

During repeated scheduled runs, this became visible through elevated container memory usage and Airflow heartbeat failures.

After isolating the dbt and Snowflake layers from the orchestration layer, the LocalExecutor worker pool was identified as a major source of unnecessary resource pressure.

Reducing parallelism from 32 to 4 reduced Airflow container memory usage from approximately 5.9 GB to 1.8 GB in the local environment. The complete pipeline subsequently executed successfully in approximately 1 minute 16 seconds.

## Airflow Metadata Persistence

Airflow's metadata database is stored outside the disposable container using a mounted host directory.

This allows DAG run history and task-instance state to survive container restarts and recreation.

The metadata database itself is excluded from version control.

## Docker

Docker Compose is used to run the local environment.

The main services are:

- `mock-api`
- `airflow`

The FastAPI application and Airflow run in separate containers.

Airflow reaches the API through the Docker Compose network using:

`http://mock-api:8000`

rather than `localhost`.

Environment-specific configuration and credentials are supplied through environment variables rather than being hard-coded into the application.

## Project Structure

```text
.
├── airflow/
│   └── dags/
│       └── cyber_api_to_snowflake.py
│
├── producer/
│   ├── app.py
│   ├── Dockerfile
│   └── requirements.txt
│
├── snowflake/
│   ├── setup.sql
│   ├── create_stage.sql
│   └── copy_into.sql
│
├── dbt/
│   ├── models/
│   │   ├── staging/
│   │   │   ├── sources.yml
│   │   │   ├── stg_customers.sql
│   │   │   └── stg_security_events.sql
│   │   │
│   │   ├── intermediate/
│   │   │   ├── int_latest_customers.sql
│   │   │   └── int_latest_events.sql
│   │   │
│   │   └── marts/
│   │       ├── dim_customers.sql
│   │       ├── fct_security_events.sql
│   │       ├── daily_security_summary.sql
│   │       └── schema.yml
│   │
│   ├── snapshots/
│   │   └── customer_history.sql
│   │
│   ├── dbt_project.yml
│   └── profiles.yml
│
├── docker-compose.yml
├── Dockerfile
├── .env.example
├── .gitignore
└── README.md
```

## Running the Project

Create a `.env` file based on `.env.example` and provide the required AWS and Snowflake configuration.

The actual `.env` file is excluded from version control.

Build and start the environment:

```bash
docker compose up --build -d
```

Check the running services:

```bash
docker compose ps
```

The mock API is available locally on port `8000`:

```text
http://localhost:8000/events
http://localhost:8000/customers
```

Airflow is available locally on port `8080`.

Once the environment is running, enable the `cyber_api_to_snowflake` DAG in Airflow and either trigger it manually or allow the hourly schedule to execute it.

## Engineering Considerations

Several design decisions in the project are intentional.

**RAW data is preserved before transformation.** Duplicate and updated records remain available in the RAW layer, while business-level deduplication happens downstream.

**Ingestion time is used for incremental processing.** This prevents late-arriving records from being excluded simply because their source `updated_at` values are old.

**A lookback window is combined with idempotent processing.** Reprocessing a small amount of recent data provides additional protection against delayed records and processing boundaries without creating duplicate facts.

**Customer changes are historized rather than overwritten.** dbt snapshots preserve previous customer states using SCD Type 2.

**Data quality is part of the DAG.** dbt tests participate directly in pipeline success or failure.

**Pipeline runs are prevented from overlapping.** `max_active_runs=1` keeps scheduled runs from competing for the same local resources.

**Executor parallelism matches the workload.** The LocalExecutor worker pool is intentionally kept small because this DAG has limited task-level concurrency.

**Airflow metadata is persistent.** Execution history is independent of the lifecycle of the Airflow container.

## Potential Extensions

The current implementation is focused on batch ELT. Some natural extensions would include:

- a pipeline control table with explicit `last_successful_run` watermarks
- CI/CD for dbt and Airflow changes
- centralized secrets management
- pipeline alerting and observability
- automated data reconciliation
- event-driven S3-to-Snowflake ingestion with Snowpipe
- deployment to a managed Airflow environment

Streaming requirements would be better addressed through a separate event-driven architecture rather than introducing Kafka into this pipeline without a clear real-time requirement.
```text
/events
/customers
```

The API isn't just generating static data. I designed it to simulate some situations that make data pipelines more interesting:

- duplicate security events
- records that change between API calls
- late-arriving events
- customers whose attributes change over time

Airflow calls both endpoints and validates the responses before continuing.

The JSON response is converted to NDJSON and written to S3 using paths such as:

```text
raw/security_events/year=2026/month=09/day=27/<run_id>.json

raw/customers/year=2026/month=09/day=27/<run_id>.json
```

Each run gets its own file instead of overwriting previous data.

## Loading into Snowflake

The next Airflow task executes Snowflake `COPY INTO` statements.

The S3 data is loaded into:

```text
CYBER_INSURANCE.RAW.SECURITY_EVENTS
CYBER_INSURANCE.RAW.CUSTOMERS
```

I intentionally keep the RAW layer close to what arrived from the source.

That means RAW can contain duplicates and multiple versions of the same record. I handle those problems downstream instead of modifying the original data during ingestion.

I also add an `ingestion_timestamp` when records enter Snowflake.

## Deduplication

The API intentionally sends duplicate versions of `e-100`.

So RAW might contain:

```text
e-100
e-100
e-100
e-200
e-300
```

The intermediate dbt model:

```text
dbt/models/intermediate/int_latest_events.sql
```

deduplicates the data and keeps the appropriate latest version of each event.

This gives me a clean dataset for the downstream fact table while still preserving the original records in RAW.

## Handling Late-Arriving Data

I also wanted the project to demonstrate late-arriving data.

The API deliberately creates an event called `e-200` that happened several days before it actually arrives in the pipeline.

For example:

```text
event_timestamp      = September 24
updated_at           = September 24
ingestion_timestamp  = September 27
```

This distinction is important.

`event_timestamp` tells me when the security event happened.

`updated_at` tells me when the source says the record was last changed.

`ingestion_timestamp` tells me when my pipeline actually received it.

If I only relied on `updated_at` with a short lookback window, a record arriving three days late could be missed.

For this pipeline, I therefore use the ingestion timestamp as the main signal for incremental processing.

## Incremental Fact Table

The main fact model is:

```text
dbt/models/marts/fct_security_events.sql
```

It is implemented as an incremental dbt model.

The approach combines:

- `ingestion_timestamp`
- a lookback window
- deduplication
- a unique event key
- incremental merge behavior

The lookback means I intentionally re-read a small amount of recently ingested data.

The deduplication and unique key make that safe because previously processed events aren't blindly inserted again.

This also makes the pipeline more resilient to records that arrive around the boundary between two runs.

## Customer History with SCD Type 2

The customer endpoint also changes customer attributes between runs.

For example, a customer's `plan_tier` can change from:

```text
standard → premium
```

Instead of overwriting the previous value and losing that history, I use a dbt snapshot:

```text
dbt/snapshots/customer_history.sql
```

This implements SCD Type 2 behavior.

The resulting history can look like:

```text
customer_id | plan_tier | dbt_valid_from | dbt_valid_to
------------|-----------|----------------|-------------
c-1         | standard  | ...            | ...
c-1         | premium   | ...            | NULL
```

The row with `dbt_valid_to = NULL` is the current version.

This allows me to answer both:

> What does this customer look like now?

and:

> What did this customer look like at a previous point in time?

## Analytics Models

The final analytics layer contains three main models:

### `dim_customers`

The current customer dimension used for analytics.

### `fct_security_events`

The deduplicated security-event fact table.

### `daily_security_summary`

A daily aggregation of cybersecurity activity that could be used for reporting or dashboards.

The staging and intermediate models are mainly views, while the final analytics models are materialized as tables.

## Data Quality

I added dbt tests for important fields and assumptions in the models, including things like:

- unique keys
- non-null fields
- relationships between datasets

The final Airflow task runs:

```bash
dbt run --select marts
dbt test
```

If the tests fail, the Airflow task fails as well. This prevents the pipeline from silently treating bad data as a successful run.

## Airflow

Airflow orchestrates the entire workflow.

The DAG is:

```text
cyber_api_to_snowflake
```

I configured it with retries for transient failures and:

```python
max_active_runs=1
```

so multiple instances of the pipeline don't overlap.

For the local environment I'm using `LocalExecutor`.

During testing, I found that Airflow's default parallelism of 32 was unnecessary for this pipeline. Because the DAG is mostly sequential, all of those idle LocalExecutor workers were consuming a large amount of memory.

I reduced the parallelism to:

```text
4
```

which significantly reduced the memory footprint of the Airflow container and made the local environment much more stable.

## Docker

Docker Compose runs the local environment.

There are two main services:

```text
mock-api
airflow
```

The API runs in its own container and Airflow runs in another.

Because they're on the same Docker Compose network, Airflow reaches the API using:

```text
http://mock-api:8000
```

instead of `localhost`.

This was also a useful way to practice the difference between communication from my Mac to a container and communication between containers.

## Persistent Airflow Metadata

I also configured the Airflow metadata database to persist outside the disposable container.

This means DAG run history and task state aren't lost whenever the Airflow container is restarted or recreated.

The metadata database itself is excluded from Git.

## Project Structure

```text
.
├── airflow/
│   └── dags/
│       └── cyber_api_to_snowflake.py
│
├── producer/
│   ├── app.py
│   ├── Dockerfile
│   └── requirements.txt
│
├── snowflake/
│   ├── setup.sql
│   ├── create_stage.sql
│   └── copy_into.sql
│
├── dbt/
│   ├── models/
│   │   ├── staging/
│   │   │   ├── sources.yml
│   │   │   ├── stg_customers.sql
│   │   │   └── stg_security_events.sql
│   │   │
│   │   ├── intermediate/
│   │   │   ├── int_latest_customers.sql
│   │   │   └── int_latest_events.sql
│   │   │
│   │   └── marts/
│   │       ├── dim_customers.sql
│   │       ├── fct_security_events.sql
│   │       ├── daily_security_summary.sql
│   │       └── schema.yml
│   │
│   ├── snapshots/
│   │   └── customer_history.sql
│   │
│   ├── dbt_project.yml
│   └── profiles.yml
│
├── docker-compose.yml
├── Dockerfile
├── .env.example
├── .gitignore
└── README.md
```

## Running the Project

Clone the repository and create a local `.env` based on `.env.example`.

The real `.env` file is excluded from Git because it contains environment-specific credentials.

Build and start the containers:

```bash
docker compose up --build -d
```

Check that they're running:

```bash
docker compose ps
```

The mock API is available locally at:

```text
http://localhost:8000
```

The two endpoints are:

```text
http://localhost:8000/events
http://localhost:8000/customers
```

Airflow is available at:

```text
http://localhost:8080
```

From Airflow, enable and trigger:

```text
cyber_api_to_snowflake
```

## What I Learned

The most useful part of this project wasn't just getting data from an API into Snowflake. It was working through the problems that happen between those steps.

I got hands-on experience with:

- building an end-to-end ELT pipeline
- orchestrating dependencies with Airflow
- working with S3 and Snowflake
- building dbt staging, intermediate, and mart layers
- incremental loading
- handling late-arriving data
- deduplicating source records
- designing idempotent loads
- implementing SCD Type 2
- writing dbt data-quality tests
- Docker networking
- managing credentials with environment variables
- debugging Airflow task failures
- investigating container resource usage
- tuning LocalExecutor parallelism
- persisting Airflow metadata

One of the more useful debugging exercises was discovering that some failed pipeline runs weren't caused by the SQL or dbt models at all.

I tested dbt independently, checked Airflow's logs and task heartbeats, inspected the processes running inside the container, and found that the local Airflow environment was running far more LocalExecutor workers than this pipeline needed.

Reducing the parallelism from 32 to 4 significantly reduced the container's memory usage, and the full pipeline subsequently completed successfully.

That debugging process was probably as valuable as building the pipeline itself.

## Possible Next Steps

There are several things I could add later, but I intentionally kept the current project focused on batch data engineering.

Possible extensions include:

- a control table for `last_successful_run` watermarks
- Snowflake Streams / CDC
- Snowpipe for event-driven ingestion
- Kafka for real-time events
- CI/CD for dbt and Airflow
- alerting and monitoring
- managed secrets instead of local environment variables
- deploying Airflow to a managed environment

For now, the project demonstrates the complete batch flow from **source API → S3 → Snowflake → dbt → analytics**, including the failure and data-quality scenarios I wanted to explore.
