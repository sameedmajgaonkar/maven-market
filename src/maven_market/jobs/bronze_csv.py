"""Job entry point: load the configured CSV sources into Bronze with Auto Loader.

Run as a Databricks python_wheel_task (``bronze_csv`` console script). The config directory is
passed in because a packaged wheel cannot locate the repo's ``config/`` folder by itself; the
bundle passes ``${workspace.file_path}/config``.

A failure in one source is logged to the audit table and the remaining sources still run; the
job fails at the end if any source failed.
"""
from __future__ import annotations

import argparse
import sys

from maven_market.config import get, load_config
from maven_market.ingestion import autoloader
from maven_market.logging_utils import AuditLogger


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Load CSV sources into Bronze")
    p.add_argument("--env", default="dev")
    p.add_argument("--config-dir", default=None)
    p.add_argument(
        "--sources", default=None, type=lambda s: [x.strip() for x in s.split(",") if x.strip()],
        help="Comma-separated subset of configured sources (default: all)",
    )
    return p.parse_args(argv)


def select_sources(cfg: dict, requested: list[str] | None) -> list[str]:
    available = get(cfg, "sources")
    if requested is None:
        return list(available)
    unknown = [s for s in requested if s not in available]
    if unknown:
        raise KeyError(f"Unknown source(s) {unknown}; configured: {sorted(available)}")
    return list(requested)


def audit_table(cfg: dict) -> str:
    return f"{get(cfg, 'catalog')}.{get(cfg, 'audit.table')}"


def main(argv: list[str] | None = None) -> None:  # pragma: no cover - needs Spark
    args = parse_args(argv if argv is not None else sys.argv[1:])
    cfg = load_config(args.env, config_dir=args.config_dir)

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    audit = AuditLogger(table=audit_table(cfg))

    failed = []
    for source in select_sources(cfg, args.sources):
        try:
            autoloader.ingest(spark, cfg, source, audit=audit)
        except Exception as exc:  # already recorded as FAILED by audit.track
            print(f"[bronze_csv] source '{source}' failed: {exc}", file=sys.stderr)
            failed.append(source)
    if failed:
        raise RuntimeError(f"Bronze CSV load failed for: {failed}")


if __name__ == "__main__":  # pragma: no cover
    main()
