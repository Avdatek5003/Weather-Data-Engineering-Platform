import json
from uuid import UUID


COMPLETED_ROOT = (
    "hdfs://namenode:8020/weather/lab/final_sprint/completed"
)


def get_hdfs(spark):
    """Return Hadoop FileSystem using Spark's active configuration."""
    jvm = spark._jvm
    hadoop_conf = spark._jsc.hadoopConfiguration()

    uri = jvm.java.net.URI.create(COMPLETED_ROOT)
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(
        uri, hadoop_conf
    )

    return fs


def marker_path(spark, batch_key):
    jvm = spark._jvm

    return jvm.org.apache.hadoop.fs.Path(
        f"{COMPLETED_ROOT}/{batch_key}.json"
    )


def completion_exists(spark, batch_key):
    fs = get_hdfs(spark)
    path = marker_path(spark, batch_key)
    return bool(fs.exists(path))

def validate_completion(spark, batch_key):
    """
    Validate completion marker metadata and HDFS outputs.

    Returns the validated metadata dictionary.
    Raises ValueError if validation fails.
    """
    fs = get_hdfs(spark)
    jvm = spark._jvm

    path = marker_path(spark, batch_key)

    if not fs.exists(path):
        raise ValueError(
            f"Completion marker missing: {batch_key}"
        )

    # Read marker JSON through Hadoop FileSystem.
    stream = fs.open(path)

    try:
        reader = jvm.java.io.BufferedReader(
            jvm.java.io.InputStreamReader(
                stream,
                "UTF-8",
            )
        )

        lines = []

        while True:
            line = reader.readLine()

            if line is None:
                break

            lines.append(line)

        raw_json = "\n".join(lines)

    finally:
        stream.close()

    try:
        metadata = json.loads(raw_json)
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"Invalid completion JSON: {batch_key}"
        ) from exc

    if not isinstance(metadata, dict):
        raise ValueError(
            "Completion metadata must be a JSON object"
        )

    if metadata.get("batch_key") != batch_key:
        raise ValueError(
            f"Batch key mismatch: {batch_key}"
        )

    if metadata.get("status") != "SUCCESS":
        raise ValueError(
            f"Batch is not marked SUCCESS: {batch_key}"
        )

    run_id = metadata.get("run_id")

    try:
        run_uuid = UUID(run_id)
    except (TypeError, ValueError, AttributeError) as exc:
        raise ValueError(
            f"Invalid run_id in marker: {batch_key}"
        ) from exc

    if str(run_uuid) != run_id:
        raise ValueError(
            f"Non-canonical run_id: {run_id}"
        )

    counts = metadata.get("counts")

    required_counts = (
        "landing",
        "silver",
        "quarantine",
        "monthly",
        "yearly",
    )

    if not isinstance(counts, dict):
        raise ValueError(
            "Completion counts must be an object"
        )

    for key in required_counts:
        value = counts.get(key)

        if type(value) is not int or value < 0:
            raise ValueError(
                f"Invalid completion count: {key}={value}"
            )

    if counts["landing"] <= 0:
        raise ValueError("Landing count must be positive")

    if counts["silver"] <= 0:
        raise ValueError("Silver count must be positive")

    if (
        counts["landing"]
        != counts["silver"] + counts["quarantine"]
    ):
        raise ValueError(
            "Landing/Silver/Quarantine count mismatch"
        )

    if counts["monthly"] <= 0 or counts["yearly"] <= 0:
        raise ValueError(
            "Gold output counts must be positive"
        )

    run_base = (
        "hdfs://namenode:8020/weather/lab/final_sprint"
        f"/runs/{run_id}"
    )

    required_paths = (
        "landing",
        "silver",
        "quarantine",
        "gold/monthly",
        "gold/yearly",
    )

    for relative_path in required_paths:
        success_path = jvm.org.apache.hadoop.fs.Path(
            f"{run_base}/{relative_path}/_SUCCESS"
        )

        if not fs.exists(success_path):
            raise ValueError(
                f"Missing HDFS success marker: {success_path}"
            )

    return metadata


def write_completion(spark, batch_key, run_id, counts):
    """
    Write completion metadata to a temporary HDFS file,
    then publish it under the final marker name.

    Assumes one writer per batch. Concurrent publication
    is handled separately.
    """
    fs = get_hdfs(spark)
    jvm = spark._jvm

    final_path = marker_path(spark, batch_key)
    temp_path = jvm.org.apache.hadoop.fs.Path(
        f"{COMPLETED_ROOT}/.{batch_key}.{run_id}.tmp"
    )

    parent = final_path.getParent()

    if not fs.exists(parent):
        if not fs.mkdirs(parent):
            raise RuntimeError(
                f"Cannot create completion directory: {parent}"
            )

    if fs.exists(final_path):
        raise FileExistsError(
            f"Completion marker already exists: {batch_key}"
        )

    payload = {
        "batch_key": batch_key,
        "run_id": run_id,
        "status": "SUCCESS",
        "counts": counts,
    }

    data = json.dumps(payload, sort_keys=True).encode("utf-8")

    stream = None

    try:
        stream = fs.create(temp_path, False)
        stream.write(bytearray(data))
        stream.hflush()
        stream.close()
        stream = None

        if fs.exists(final_path):
            raise FileExistsError(
                f"Completion marker already exists: {batch_key}"
            )

        if not fs.rename(temp_path, final_path):
            raise RuntimeError(
                f"Cannot publish completion marker: {batch_key}"
            )

    except Exception:
        if stream is not None:
            stream.close()

        if fs.exists(temp_path):
            fs.delete(temp_path, False)

        raise