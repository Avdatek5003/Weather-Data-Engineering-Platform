# Spark Stack Compatibility

## Project 1 Goal

Project 1 uses Spark as a learning-oriented analytical batch layer.

The operational ingestion pipeline will continue to use Python,
Airflow and PostgreSQL.

## Planned Stack

| Component | Target |
|---|---|
| Apache Spark | 3.5.x |
| PySpark | Same Spark release |
| Java | 17 |
| Python | 3.11 |
| Hadoop / HDFS | 3.x |
| PostgreSQL | 15 |
| Analytical Format | Parquet |
| Initial Spark Mode | local[*] |
| Later Spark Mode | Standalone |
| Cloud Lab | Databricks Free Edition |

## Principles

- Do not use unpinned `latest` images.
- Spark and PySpark versions must match.
- The exact Docker image tags will be pinned before infrastructure setup.
- HDFS is used for distributed-storage learning.
- Spark does not replace PostgreSQL.
- Spark Structured Streaming and Kafka are reserved for Project 2.