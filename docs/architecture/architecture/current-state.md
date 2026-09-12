# Current System State — Pre-Spark Baseline

## Operational Pipeline

Weather data is currently collected from Open-Meteo, AviationWeather
and historical weather sources.

Near-real-time ingestion is orchestrated by Apache Airflow.

Current flow:

Weather APIs
    ↓
Apache Airflow
    ↓
Python ETL
    ↓
PostgreSQL 15
    ↓
Streamlit / Plotly

## Main Application Stack

The main Docker Compose stack contains:

- PostgreSQL 15
- Streamlit
- pgAdmin

Ports:

- PostgreSQL: 5432
- Streamlit: 8501
- pgAdmin: 5050

## Airflow

Apache Airflow is managed separately through Astronomer Astro.

Airflow connects to the Weather PostgreSQL database using:

host.docker.internal:5432

The Airflow metadata database and Weather application database are
logically separate.

## Historical Data

Historical climate records are stored in:

historical_weather

Main columns:

- date
- tavg
- tmin
- tmax
- prcp
- snow
- wdir
- wspd
- wpgt
- pres
- tsun
- city_name

## Spark Integration Principle

Spark will not replace the operational ingestion pipeline.

Spark will be introduced as a separate analytical batch-processing layer
for historical weather data.

Target architecture:

Operational Pipeline:

APIs
  ↓
Airflow
  ↓
Python ETL
  ↓
PostgreSQL
  ↓
Streamlit

Analytical Pipeline:

Historical Weather
  ↓
HDFS
  ↓
PySpark
  ↓
Bronze / Silver / Gold
  ↓
Parquet
  ↓
Analytics