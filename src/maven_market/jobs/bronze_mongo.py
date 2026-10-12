"""Job entry point: snapshot the configured MongoDB collections into Bronze."""
from __future__ import annotations

import argparse
import sys

from maven_market.config import get, load_config
from maven_market.ingestion import mongo_ingest
from maven_market.jobs.bronze_csv import audit_table
from maven_market.logging_utils import AuditLogger


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Load MongoDB collections into Bronze")
    p.add_argument("--env", default="dev")
    p.add_argument("--config-dir", default=None)
    p.add_argument(
        "--collections", default=None, type=lambda s: [x.strip() for x in s.split(",") if x.strip()],
        help="Comma-separated subset of configured collections (default: all)",
    )
    return p.parse_args(argv)


def select_collections(cfg: dict, requested: list[str] | None) -> list[str]:
    available = get(cfg, "mongo.collections")
    if requested is None:
        return list(available)
    unknown = [c for c in requested if c not in available]
    if unknown:
        raise KeyError(f"Unknown collection(s) {unknown}; configured: {sorted(available)}")
    return list(requested)


def main(argv: list[str] | None = None) -> None:  # pragma: no cover - needs Databricks + Atlas
    args = parse_args(argv if argv is not None else sys.argv[1:])
    cfg = load_config(args.env, config_dir=args.config_dir)

    from pyspark.sql import SparkSession

    from maven_market.databricks_utils import get_dbutils

    spark = SparkSession.builder.getOrCreate()
    dbutils = get_dbutils(spark)
    audit = AuditLogger(table=audit_table(cfg))

    failed = []
    for collection in select_collections(cfg, args.collections):
        try:
            mongo_ingest.ingest(spark, dbutils, cfg, collection, audit=audit)
        except Exception as exc:  # already recorded as FAILED by audit.track
            print(f"[bronze_mongo] collection '{collection}' failed: {exc}", file=sys.stderr)
            failed.append(collection)
    if failed:
        raise RuntimeError(f"Mongo load failed for: {failed}")


if __name__ == "__main__":  # pragma: no cover
    main()
