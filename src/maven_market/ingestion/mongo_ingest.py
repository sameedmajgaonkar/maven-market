"""MongoDB Atlas -> Bronze (product master snapshot).

Two interchangeable methods, chosen by ``mongo.method`` in config:

* ``pymongo`` (default): read the collection with the pure-Python driver and build a DataFrame.
  Works on serverless compute and is plenty for master data (thousands of documents).
* ``spark_connector``: MongoDB Spark Connector v10.x (``format("mongodb")``). Needs a
  **Dedicated (single-user)** classic cluster with the connector Maven library installed.

The connection URI is read from the secret scope; it is never in config or code.
Bronze keeps the latest full snapshot (overwrite), because the source is a master table.
"""
from __future__ import annotations

from maven_market.config import get

_METHODS = {"pymongo", "spark_connector"}


def method(cfg: dict) -> str:
    chosen = get(cfg, "mongo.method")
    if chosen not in _METHODS:
        raise ValueError(f"mongo.method must be one of {sorted(_METHODS)}, got {chosen!r}")
    return chosen


def mongo_options(cfg: dict, uri: str, collection: str) -> dict:
    """Spark-connector read options."""
    get(cfg, f"mongo.collections.{collection}")  # fail fast on unknown collection
    return {
        "connection.uri": uri,
        "database": get(cfg, "mongo.database"),
        "collection": collection,
    }


def schema_columns(ddl: str) -> list[str]:
    """Column names from a ``"a INT, b STRING"`` DDL string."""
    return [part.split()[0] for part in ddl.split(",") if part.strip()]


def documents_to_rows(docs: list[dict], columns: list[str]) -> list[dict]:
    """Project Mongo documents onto the configured columns.

    ``_id`` (an ObjectId) becomes a string; fields missing from a document become NULL; fields not
    in ``columns`` are dropped, so an unexpected new field cannot break the load.
    """
    rows = []
    for doc in docs:
        row = {c: doc.get(c) for c in columns}
        if "_id" in row and row["_id"] is not None:
            row["_id"] = str(row["_id"])
        rows.append(row)
    return rows


def _read_pymongo(cfg: dict, uri: str, collection: str) -> list[dict]:  # pragma: no cover - needs Atlas
    from pymongo import MongoClient  # lazy: only needed on the machine that talks to Atlas

    client = MongoClient(uri, serverSelectionTimeoutMS=20000)
    try:
        return list(client[get(cfg, "mongo.database")][collection].find({}))
    finally:
        client.close()


def ingest(spark, dbutils, cfg: dict, collection: str, audit=None):  # pragma: no cover - needs Databricks
    """Snapshot ``collection`` into its Bronze table; returns the row count."""
    from pyspark.sql import functions as F

    spec = get(cfg, f"mongo.collections.{collection}")
    uri = dbutils.secrets.get(get(cfg, "secret_scope"), get(cfg, "mongo.uri_secret"))
    target = f"{get(cfg, 'catalog')}.{spec['bronze_table']}"

    def _run() -> int:
        if method(cfg) == "pymongo":
            docs = _read_pymongo(cfg, uri, collection)
            rows = documents_to_rows(docs, schema_columns(spec["schema"]))
            df = spark.createDataFrame(rows, schema=spec["schema"])
        else:
            df = spark.read.format("mongodb").options(**mongo_options(cfg, uri, collection)).load()
            df = df.withColumn("_id", F.col("_id").cast("string"))
        df = df.withColumn("_ingested_at", F.current_timestamp())
        df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(target)
        return df.count()

    if audit is None:
        return _run()
    with audit.track("bronze", f"mongo.{collection}") as metrics:
        n = _run()
        metrics["rows_in"] = metrics["rows_out"] = n
        return n
