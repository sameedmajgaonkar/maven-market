"""Helpers that only make sense on Databricks (lazy imports, so unit tests never need them)."""
from __future__ import annotations


def get_dbutils(spark):  # pragma: no cover - needs Databricks
    """Return ``dbutils``. Wheel/script tasks are not given it automatically, unlike notebooks."""
    try:
        from pyspark.dbutils import DBUtils

        return DBUtils(spark)
    except ImportError:
        from databricks.sdk.runtime import dbutils

        return dbutils
