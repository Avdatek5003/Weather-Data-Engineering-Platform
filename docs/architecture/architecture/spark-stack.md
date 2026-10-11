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

## Week 2 - Day 1: Spark Standalone

### Environment
- Spark: 3.5.9
- Java: OpenJDK 17
- HDFS/Hadoop: 3.4.1
- Spark Master: spark://spark-master:7077
- Worker resources: 2 cores / 2 GiB RAM
- Deployment: Docker containers on weather_data_net

### Local vs Standalone Smoke Test

Same 1,000,000-row aggregation workload was executed in both modes.

#### Local mode
- Master: local[*]
- Application ID: local-1791371724338
- Default parallelism: 16
- Input partitions: 16
- Stage 0 tasks: 16
- Result partitions: 1

#### Spark Standalone
- Master: spark://spark-master:7077
- Application ID: app-20261007113129-0000
- Default parallelism: 2
- Input partitions: 2
- Stage 0 tasks: 2
- Result partitions: 1

### Technical observations
1. local[*] used the 16 logical cores visible to the Spark container.
2. Standalone execution used the resources registered by the worker: 2 cores and 2 GiB RAM.
3. The same DataFrame workload produced identical results in both deployment modes.
4. In Standalone mode the Driver registered the application with the Spark Master, and the Master allocated Worker resources for the Executor.
5. Partition count influenced the number of tasks, while available Executor cores limited how many tasks could execute concurrently.
6. The Standalone application appeared in the Master/Worker UI, while the local[*] application did not use the Standalone cluster.
7. Spark Master manages cluster resources; Driver coordinates the application; Worker hosts Executor processes; Executors execute tasks.

## Week 2 — Day 2: Resource Allocation and Partition Experiments

### Cluster Configuration

- Spark: 3.5.9
- Standalone Master: spark://spark-master:7077
- Workers: 1
- Worker cores: 2
- Worker advertised memory: 2 GiB
- Executor cores: 2
- Executor memory: 1 GiB

### Historical Silver Dataset

- Total rows: 775,442
- Cities: 27
- Storage: HDFS / Parquet
- Path: /weather/silver/weather_clean

### Repartition Benchmark

| Partitions | Measured Duration |
|---|---|
| 2 | 1.273 s |
| 8 | 0.726 s |
| 16 | 0.683 s |

These are single-run measurements, not definitive performance rankings.

### Repartition vs Coalesce

- repartition(n) introduces a shuffle Exchange.
- coalesce(n), when reducing partitions with shuffle disabled, does not require a new shuffle.
- Both approaches produced two Parquet output files.
- Repartition write showed approximately 40.3 MiB Shuffle Write.
- Equal output file counts do not imply equal execution cost.
- Eight written Parquet files were read as two Spark scan partitions, demonstrating that file count and read partition count are different concepts.

### City-Based Data Skew

Normal repartition(8):
- Nearly balanced distribution, approximately 96,930 rows per partition.

Hash repartition(8, "city_name"):
- P0: 140,132
- P1: 46,966
- P2: 150,857
- P3: 113,651
- P4: 122,359
- P5: 93,401
- P6: 108,076
- P7: 0

Largest/smallest non-empty partition ratio: approximately 3.21.

Conclusion:
- Hash partitioning groups records by city, but does not guarantee equal partition sizes.
- Data skew was observed in row counts.
- No salting optimization was introduced without evidence of a significant task-duration bottleneck.

### Resource Monitoring

- Docker memory usage: [add observed values]
- Docker CPU usage during measurement: [add observed values]
- System stability: [add observations]

### Docker Resource Monitoring

Measurement: 2026-10-11

| Container | CPU | Memory |
|---|---:|---|
| Spark Driver container | 2.65% | 1.865 GiB |
| Spark Master | 0.12% | 235.1 MiB |
| Spark Worker | 0.24% | 756.4 MiB |
| HDFS NameNode | 0.54% | 398.2 MiB |
| HDFS DataNode | 0.14% | 358 MiB |

Combined memory for these five containers: ~3.57 GiB.

Docker reported a 7.609 GiB memory limit per container.
The Spark Worker's advertised 2 GiB resource capacity
is not equivalent to a Docker-level memory limit.

CPU values are point-in-time measurements, not peak usage.

### Engineering Conclusions

- Increasing partition count increases potential task parallelism,
  but available Executor cores limit concurrency.
- More partitions do not automatically mean faster execution.
- Coalesce can reduce partitions without an additional shuffle;
  repartition introduces an Exchange/shuffle.
- Parquet output file count and Spark read partition count
  are not necessarily equal.
- Hash partitioning by city_name produced an uneven distribution:
  7 non-empty partitions and 1 empty partition.
- The largest non-empty partition contained approximately
  3.21 times as many rows as the smallest.
- Skew was observed in partition row counts, but a significant
  task-duration bottleneck has not been demonstrated.