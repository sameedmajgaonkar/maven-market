"""Job entry point: drain new Kafka events (orders, inventory) into Bronze.

Run as a Databricks python_wheel_task (``bronze_kafka`` console script). Each topic is processed
independently; a failure is recorded in the audit table and the job fails at the end.
"""
from __future__ import annotations

import argparse
import sys

from maven_market.config import get, load_config
from maven_market.ingestion import kafka_stream
from maven_market.jobs.bronze_csv import audit_table
from maven_market.logging_utils import AuditLogger


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Load Kafka topics into Bronze")
    p.add_argument("--env", default="dev")
    p.add_argument("--config-dir", default=None)
    p.add_argument(
        "--topics", default=None, type=lambda s: [x.strip() for x in s.split(",") if x.strip()],
        help="Comma-separated subset of configured topics (default: all)",
    )
    return p.parse_args(argv)


def select_topics(cfg: dict, requested: list[str] | None) -> list[str]:
    available = get(cfg, "kafka.topics")
    if requested is None:
        return list(available)
    unknown = [t for t in requested if t not in available]
    if unknown:
        raise KeyError(f"Unknown topic(s) {unknown}; configured: {sorted(available)}")
    return list(requested)


def main(argv: list[str] | None = None) -> None:  # pragma: no cover - needs Databricks
    args = parse_args(argv if argv is not None else sys.argv[1:])
    cfg = load_config(args.env, config_dir=args.config_dir)

    from pyspark.sql import SparkSession

    from maven_market.databricks_utils import get_dbutils

    spark = SparkSession.builder.getOrCreate()
    dbutils = get_dbutils(spark)
    audit = AuditLogger(table=audit_table(cfg))

    failed = []
    for topic in select_topics(cfg, args.topics):
        try:
            kafka_stream.ingest(spark, dbutils, cfg, topic, audit=audit)
        except Exception as exc:  # already recorded as FAILED by audit.track
            print(f"[bronze_kafka] topic '{topic}' failed: {exc}", file=sys.stderr)
            failed.append(topic)
    if failed:
        raise RuntimeError(f"Kafka load failed for: {failed}")


if __name__ == "__main__":  # pragma: no cover
    main()
