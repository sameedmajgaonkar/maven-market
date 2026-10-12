"""CSV -> Bronze via Auto Loader (``cloudFiles``).

Every path, table name and option comes from config; nothing is hardcoded here.
The option builders are pure functions (unit-tested without Spark); ``read_stream`` /
``ingest`` are thin Spark wrappers.
"""
from __future__ import annotations

from maven_market.config import get


def _landing(cfg: dict) -> str:
    return get(cfg, "storage.landing_root").rstrip("/")


def source_path(cfg: dict, source: str) -> str:
    return f"{_landing(cfg)}/{get(cfg, f'sources.{source}.path')}"


def bronze_table(cfg: dict, source: str) -> str:
    """Fully qualified ``catalog.schema.table`` for a source's Bronze table."""
    return f"{get(cfg, 'catalog')}.{get(cfg, f'sources.{source}.bronze_table')}"


def checkpoint_path(cfg: dict, source: str) -> str:
    get(cfg, f"sources.{source}")  # fail fast on unknown source
    return f"{_landing(cfg)}/{get(cfg, 'autoloader.checkpoint_dir')}/{source}"


def autoloader_options(cfg: dict, source: str) -> dict:
    get(cfg, f"sources.{source}")
    return {
        "cloudFiles.format": get(cfg, f"sources.{source}.format"),
        # required by Auto Loader: where the inferred schema is persisted
        "cloudFiles.schemaLocation": f"{_landing(cfg)}/{get(cfg, 'autoloader.schema_dir')}/{source}",
        # without this every CSV column is a string
        "cloudFiles.inferColumnTypes": str(get(cfg, "autoloader.infer_column_types")).lower(),
        # new/renamed columns are captured in the rescued column instead of failing the stream
        "cloudFiles.schemaEvolutionMode": get(cfg, "autoloader.schema_evolution_mode"),
        # NOTE: this option has no 'cloudFiles.' prefix
        "rescuedDataColumn": get(cfg, "autoloader.rescued_data_column"),
        "header": "true",
    }


def read_stream(spark, cfg: dict, source: str):  # pragma: no cover - needs Spark
    from pyspark.sql import functions as F

    df = (
        spark.readStream.format("cloudFiles")
        .options(**autoloader_options(cfg, source))
        .load(source_path(cfg, source))
    )
    # `_metadata` is a hidden column: it must be selected explicitly to be kept for lineage.
    return df.select(
        "*",
        F.col("_metadata.file_path").alias("_source_file_path"),
        F.col("_metadata.file_name").alias("_source_file_name"),
        F.col("_metadata.file_modification_time").alias("_source_file_modified_at"),
        F.current_timestamp().alias("_ingested_at"),
    )


def ingest(spark, cfg: dict, source: str, audit=None):  # pragma: no cover - needs Spark
    """Incrementally load new files for ``source`` into its Bronze table (availableNow)."""
    def _run():
        query = (
            read_stream(spark, cfg, source)
            .writeStream.option("checkpointLocation", checkpoint_path(cfg, source))
            .option("mergeSchema", "true")
            .trigger(availableNow=True)
            .toTable(bronze_table(cfg, source))
        )
        query.awaitTermination()
        return query

    if audit is None:
        return _run()
    with audit.track("bronze", source) as metrics:
        query = _run()
        progress = query.recentProgress or []
        metrics["rows_in"] = metrics["rows_out"] = sum(p.get("numInputRows", 0) for p in progress)
        return query
