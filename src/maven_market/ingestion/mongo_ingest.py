"""MongoDB Atlas -> Bronze (customers + products master data).

Two interchangeable methods, chosen by ``mongo.method`` in config:

* ``pymongo`` (default): read the collection with the pure-Python driver. Works on serverless
  compute and is plenty for master data (thousands of documents).
* ``spark_connector``: MongoDB Spark Connector v10.x (``format("mongodb")``). Needs a
  **Dedicated (single-user)** classic cluster with the connector Maven library installed.

Bronze is append-only like every other Bronze table, but a master-data extract is a full snapshot,
so only documents that are NEW or CHANGED since the latest Bronze version are appended. Re-running
with nothing changed appends zero rows, and Silver's SCD2 flow sees real changes only.

The connection URI is read from the secret scope; it is never in config or code.
"""
from __future__ import annotations

from datetime import date, datetime

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


def schema_types(ddl: str) -> dict[str, str]:
    """``{column: TYPE}`` from a ``"a INT, b STRING"`` DDL string (simple, non-parameterised types)."""
    out = {}
    for part in ddl.split(","):
        if part.strip():
            name, type_ = part.split()[:2]
            out[name] = type_.upper()
    return out


def schema_columns(ddl: str) -> list[str]:
    """Column names from a ``"a INT, b STRING"`` DDL string, in order."""
    return list(schema_types(ddl))


def _as_date(value):
    """datetime -> date; ISO string -> date; date/None unchanged."""
    if value is None or (isinstance(value, date) and not isinstance(value, datetime)):
        return value
    if isinstance(value, datetime):
        return value.date()
    return date.fromisoformat(str(value)[:10])


def documents_to_rows(docs: list[dict], columns: list[str], types: dict[str, str] | None = None) -> list[dict]:
    """Project Mongo documents onto the configured columns.

    ``_id`` (an ObjectId) becomes a string; fields missing from a document become NULL; fields not
    in ``columns`` are dropped, so an unexpected new field cannot break the load. BSON dates arrive
    as ``datetime`` but Spark's DATE type needs ``date``, so DATE columns are converted.
    """
    types = types or {}
    rows = []
    for doc in docs:
        row = {c: doc.get(c) for c in columns}
        if "_id" in row and row["_id"] is not None:
            row["_id"] = str(row["_id"])
        for column, type_ in types.items():
            if type_ == "DATE":
                row[column] = _as_date(row.get(column))
        rows.append(row)
    return rows


def changed_rows(rows: list[dict], latest_by_id: dict, id_field: str, columns: list[str]) -> list[dict]:
    """Rows that are new, or differ from the latest stored version of the same id.

    ``_id`` is ignored in the comparison (it is Mongo's own identifier, not business data).
    Comparison is against the LATEST version only, so a value that reverts to an older one is still
    recognised as a change.
    """
    compare = [c for c in columns if c != "_id"]
    out = []
    for row in rows:
        previous = latest_by_id.get(row[id_field])
        if previous is None or any(previous.get(c) != row.get(c) for c in compare):
            out.append(row)
    return out


def _read_pymongo(cfg: dict, uri: str, collection: str) -> list[dict]:  # pragma: no cover - needs Atlas
    from pymongo import MongoClient  # lazy: only needed on the machine that talks to Atlas

    client = MongoClient(uri, serverSelectionTimeoutMS=20000)
    try:
        return list(client[get(cfg, "mongo.database")][collection].find({}))
    finally:
        client.close()


def _latest_existing(spark, table: str, id_field: str) -> dict:  # pragma: no cover - needs Spark
    """``{id: row-dict}`` of the newest Bronze version per id (empty if the table does not exist yet)."""
    if not spark.catalog.tableExists(table):
        return {}
    latest = spark.sql(
        f"SELECT * EXCEPT (_rn) FROM (SELECT *, row_number() OVER "
        f"(PARTITION BY {id_field} ORDER BY _ingested_at DESC) AS _rn FROM {table}) WHERE _rn = 1"
    )
    return {r[id_field]: r.asDict() for r in latest.collect()}


def ingest(spark, dbutils, cfg: dict, collection: str, audit=None):  # pragma: no cover - needs Databricks
    """Append new/changed documents of ``collection`` to its Bronze table; returns rows appended."""
    from pyspark.sql import functions as F

    spec = get(cfg, f"mongo.collections.{collection}")
    id_field, ddl = spec["id_field"], spec["schema"]
    columns, types = schema_columns(ddl), schema_types(ddl)
    uri = dbutils.secrets.get(get(cfg, "secret_scope"), get(cfg, "mongo.uri_secret"))
    target = f"{get(cfg, 'catalog')}.{spec['bronze_table']}"

    def _run(metrics: dict | None = None) -> int:
        if method(cfg) == "pymongo":
            docs = _read_pymongo(cfg, uri, collection)
        else:
            frame = spark.read.format("mongodb").options(**mongo_options(cfg, uri, collection)).load()
            docs = [r.asDict(recursive=True) for r in frame.collect()]
        if not docs:
            raise ValueError(f"MongoDB collection '{collection}' returned no documents; refusing to continue")

        rows = documents_to_rows(docs, columns, types)
        new = changed_rows(rows, _latest_existing(spark, target, id_field), id_field, columns)
        if metrics is not None:
            metrics["rows_in"], metrics["rows_out"] = len(rows), len(new)
        if new:
            (spark.createDataFrame(new, schema=ddl)
             .withColumn("_ingested_at", F.current_timestamp())
             .write.mode("append").option("mergeSchema", "true").saveAsTable(target))
        return len(new)

    if audit is None:
        return _run()
    with audit.track("bronze", f"mongo.{collection}") as metrics:
        return _run(metrics)
