"""Confluent Kafka -> Bronze via Structured Streaming, with a dead-letter path.

Malformed events (bad JSON, or missing a configured required key) are written to the topic's
dead-letter table instead of failing the stream (Review Focus #1).
"""
from __future__ import annotations

from maven_market.config import get

# Databricks ships a SHADED Kafka client. The unshaded login module name is rejected with
# RESTRICTED_STREAMING_OPTION, so this exact prefix matters.
_JAAS_MODULE = "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule"


def topic_settings(cfg: dict, topic: str) -> dict:
    return get(cfg, f"kafka.topics.{topic}")


def kafka_options(cfg: dict, topic: str, bootstrap: str, api_key: str, api_secret: str) -> dict:
    topic_settings(cfg, topic)  # fail fast on unknown topic
    return {
        "kafka.bootstrap.servers": bootstrap,
        "subscribe": topic,
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.mechanism": "PLAIN",
        "kafka.sasl.jaas.config": (
            f'{_JAAS_MODULE} required username="{api_key}" password="{api_secret}";'
        ),
        "startingOffsets": get(cfg, "kafka.starting_offsets"),
    }


def read_credentials(dbutils, cfg: dict) -> tuple[str, str, str]:  # pragma: no cover - needs Databricks
    scope = get(cfg, "secret_scope")
    names = get(cfg, "kafka.secrets")
    return tuple(dbutils.secrets.get(scope, names[k]) for k in ("bootstrap", "api_key", "api_secret"))


def split_valid_and_dead(raw_df, schema: str, required: list[str]):  # pragma: no cover - needs Spark
    """Parse JSON values; return ``(valid_df, dead_letter_df)``.

    ``from_json`` yields a NULL struct for unparseable JSON, and individual NULL fields for
    missing ones, so both failure modes are caught by the same check.
    """
    from pyspark.sql import functions as F

    df = raw_df.select(
        F.col("value").cast("string").alias("raw_value"),
        F.col("topic").alias("_kafka_topic"),
        F.col("partition").alias("_kafka_partition"),
        F.col("offset").alias("_kafka_offset"),
        F.col("timestamp").alias("_kafka_timestamp"),
    ).withColumn("parsed", F.from_json("raw_value", schema))

    is_valid = F.col("parsed").isNotNull()
    for key in required:
        is_valid = is_valid & F.col(f"parsed.{key}").isNotNull()

    meta = ["_kafka_topic", "_kafka_partition", "_kafka_offset", "_kafka_timestamp"]
    valid = df.filter(is_valid).select("parsed.*", "raw_value", *meta, F.current_timestamp().alias("_ingested_at"))
    dead = df.filter(~is_valid).select(
        "raw_value", *meta,
        F.when(F.col("parsed").isNull(), F.lit("unparseable_json"))
        .otherwise(F.lit("missing_required_key")).alias("_error"),
        F.current_timestamp().alias("_ingested_at"),
    )
    return valid, dead


def ingest(spark, dbutils, cfg: dict, topic: str, audit=None):  # pragma: no cover - needs Databricks
    """Drain all new events for ``topic`` and stop (``availableNow``).

    Valid rows -> Bronze, bad rows -> dead-letter. ``availableNow`` is used because serverless
    compute only supports that trigger; schedule the job to pick up newly arrived events.
    """
    t = topic_settings(cfg, topic)
    catalog = get(cfg, "catalog")
    bronze, dead_table = f"{catalog}.{t['bronze_table']}", f"{catalog}.{t['dead_letter_table']}"
    bootstrap, key, secret = read_credentials(dbutils, cfg)
    raw = spark.readStream.format("kafka").options(**kafka_options(cfg, topic, bootstrap, key, secret)).load()

    def _write(batch_df, batch_id):
        valid, dead = split_valid_and_dead(batch_df, t["schema"], t["required"])
        valid.write.mode("append").option("mergeSchema", "true").saveAsTable(bronze)
        dead.write.mode("append").saveAsTable(dead_table)

    checkpoint = f"{get(cfg, 'storage.landing_root').rstrip('/')}/{get(cfg, 'kafka.checkpoint_dir')}/kafka_{topic}"

    def _run():
        query = (
            raw.writeStream.foreachBatch(_write)
            .option("checkpointLocation", checkpoint)
            .trigger(availableNow=True)
            .start()
        )
        query.awaitTermination()
        return query

    if audit is None:
        return _run()
    with audit.track("bronze", f"kafka.{topic}") as metrics:
        query = _run()
        metrics["rows_in"] = metrics["rows_out"] = sum(p.get("numInputRows", 0) for p in (query.recentProgress or []))
        return query
