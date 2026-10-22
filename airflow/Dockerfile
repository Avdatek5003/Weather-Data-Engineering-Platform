# Existing Spark image: no Apache download needed
FROM spark-hdfs-spark:latest AS spark_source

# Java 17
FROM eclipse-temurin:17-jre AS java17

# Astro Runtime
FROM astrocrpublic.azurecr.io/runtime:3.3-2

FROM astrocrpublic.azurecr.io/runtime:3.3-2

# Slow PyPI connections: longer timeout and retries
ENV PIP_DEFAULT_TIMEOUT=300
ENV PIP_RETRIES=10
ENV PIP_PROGRESS_BAR=off


USER root

# Minimal system dependencies
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Java 17 runtime
COPY --from=java17 /opt/java/openjdk /opt/java/openjdk

ENV JAVA_HOME=/opt/java/openjdk
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Spark 3.5.9 from our existing local Docker image
COPY --from=spark_source /opt/spark /opt/spark
RUN chmod 644 /opt/spark/jars/postgresql-42.7.13.jar

ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${PATH}"

# Separate Python 3.10 for Spark.
# Astro/Airflow keeps its original Python 3.14.
RUN python -m pip install --no-cache-dir uv \
    && uv python install 3.10 --install-dir /opt/python \
    && PY310="$(find -L /opt/python -type f -name python3.10 | head -n 1)" \
    && test -n "$PY310" \
    && ln -s "$PY310" /usr/local/bin/python3.10

ENV PYSPARK_PYTHON=/usr/local/bin/python3.10
ENV PYSPARK_DRIVER_PYTHON=/usr/local/bin/python3.10

USER astro