# ⚙️ Apache Airflow Orchestration

This directory contains the Apache Airflow orchestration layer for the
**Weather Data Engineering Platform**.

The local Airflow environment is managed using the **Astronomer Astro CLI**.

---

## DAG

The primary DAG is:

```text
weather_etl_pipeline
```

It runs every **30 minutes** and orchestrates three ingestion tasks:

```text
extract_and_load_center ────┐
                            ├──> extract_and_load_forecast
extract_and_load_airport ───┘
```

### Tasks

#### `extract_and_load_center`

Retrieves current city-level weather observations from Open-Meteo and loads them into the application PostgreSQL database.

#### `extract_and_load_airport`

Retrieves current METAR observations for configured airports and loads them into the application PostgreSQL database.

#### `extract_and_load_forecast`

Removes outdated forecast records, retrieves five-day forecasts and performs database UPSERT operations.

---

## Database Architecture

Airflow uses its own PostgreSQL database for orchestration metadata.

This database is separate from the Weather Data Platform application database.

```text
Airflow Metadata PostgreSQL
        │
        ├── DAG runs
        ├── Task instances
        ├── Scheduler state
        └── Airflow metadata


Airflow DAG Tasks
        │
        │ host.docker.internal:5432
        ▼
Weather PostgreSQL 15
        │
        ├── Weather observations
        ├── METAR data
        ├── Forecasts
        └── Historical climate data
```

---

## Local Environment Variables

Copy:

```text
airflow/.env.example
```

to:

```text
airflow/.env
```

The local Airflow configuration should contain:

```env
DB_HOST=host.docker.internal
DB_PORT=5432
DB_NAME=Weather_Data_Pipeline
DB_USER=postgres
DB_PASSWORD=your_password
```

The real `.env` file must not be committed to source control.

---

## Run Locally

Make sure the main Weather PostgreSQL container is running first.

From the repository root:

```bash
docker compose up -d
```

Then move into the Airflow project:

```bash
cd airflow
```

Start Astro:

```bash
astro dev start
```

Airflow UI:

```text
http://localhost:8080
```

---

## Stop Airflow

```bash
astro dev stop
```

---

## Main Source Files

```text
airflow/
│
├── dags/
│   ├── src/
│   │   ├── database.py
│   │   ├── extract.py
│   │   ├── load.py
│   │   └── logger.py
│   │
│   └── weather_pipeline_dag.py
│
├── Dockerfile
├── requirements.txt
├── packages.txt
└── README.md
```

---

## Responsibilities

Apache Airflow is used as the **orchestration layer**.

It is responsible for:

- Scheduling ingestion jobs
- Managing task dependencies
- Retrying failed tasks
- Tracking execution state
- Running Python extraction and load functions

Airflow is **not** used as the main weather data store.

Weather datasets are persisted in the separate PostgreSQL application database.
