# Cyber Insurance Data Pipeline

I built this project to practice building an end-to-end data engineering pipeline using tools that are commonly used together in production: **Airflow, AWS S3, Snowflake, dbt, Docker, and Python**.

The pipeline pulls cybersecurity event and customer data from a mock API, lands the raw data in S3, loads it into Snowflake, and uses dbt to clean, deduplicate, track customer history, and build analytics-ready tables.

I also added a few problems that data pipelines commonly have to deal with, including duplicate records, late-arriving data, source updates, incremental loads, and changing customer attributes.

## Architecture

```text
FastAPI Mock API
        |
        v
     Airflow
        |
        v
     AWS S3
   (Raw NDJSON)
        |
        v
 Snowflake RAW
        |
        v
   dbt Staging
        |
        v
 dbt Intermediate
   |          |
   |          +--> Deduplication
   |          +--> Latest records
   |
   v
dbt Snapshot (SCD2)
        |
        v
  Analytics Marts
   |      |      |
   v      v      v
  DIM    FACT   Daily Summary
```

## Tech Stack

- Python
- FastAPI
- Apache Airflow
- Docker / Docker Compose
- AWS S3
- Snowflake
- dbt
- SQL

## How the Pipeline Works

The Airflow DAG runs the pipeline in five main steps:

```text
extract_and_land
        ↓
copy_into_snowflake
        ↓
dbt_staging_and_intermediate
        ↓
dbt_customer_snapshot
        ↓
dbt_marts_and_tests
```

The DAG is scheduled hourly, although it can also be triggered manually from Airflow.

### 1. Extracting the API Data

The `producer` folder contains a small FastAPI application that acts as the source system for the project.

It exposes two endpoints:

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