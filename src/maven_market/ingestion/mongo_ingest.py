"""MongoDB Atlas -> Bronze via the MongoDB Spark Connector v10.x.

Requires **Dedicated (single-user) access mode** compute; the connector does not work on
Standard/Shared clusters. The connection URI is read from the secret scope, never from config.
"""
from __future__ import annotations

from maven_market.config import get


def mongo_options(cfg: dict, uri: str, collection: str) -> dict:
    get(cfg, f"mongo.collections.{collection}")  # fail fast on unknown collection
    return {
        "connection.uri": uri,
        "database": get(cfg, "mongo.database"),
        "collection": collection,
    }


def ingest(spark, dbutils, cfg: dict, collection: str, audit=None):  # pragma: no cover - needs Databricks
    """Batch-read a collection and append it to its Bronze table."""
    from pyspark.sql import functions as F

    uri = dbutils.secrets.get(get(cfg, "secret_scope"), get(cfg, "mongo.uri_secret"))
    target = f"{get(cfg, 'catalog')}.{get(cfg, f'mongo.collections.{collection}.bronze_table')}"

    def _run():
        df = spark.read.format("mongodb").options(**mongo_options(cfg, uri, collection)).load()
        # ObjectId -> string so the Delta table has a plain, joinable key
        df = df.withColumn("_id", F.col("_id").cast("string")).withColumn("_ingested_at", F.current_timestamp())
        df.write.mode("append").option("mergeSchema", "true").saveAsTable(target)
        return df.count()

    if audit is None:
        return _run()
    with audit.track("bronze", f"mongo.{collection}") as metrics:
        n = _run()
        metrics["rows_in"] = metrics["rows_out"] = n
        return n
