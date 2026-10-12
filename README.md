# Maven Market — Secure Retail Analytics Lakehouse

End-to-end, config-driven, governed Databricks lakehouse on Azure. Ingests batch (CSV via Auto Loader), streaming (Confluent Kafka), and NoSQL (MongoDB) retail data through a Bronze→Silver→Gold medallion, enforces Unity Catalog ABAC (governed tags, column masks, row filters), ships via Databricks Asset Bundles + GitHub Actions CI/CD, and surfaces four AI/BI dashboards.

Capstone for the Rising Star Batch 9 program — evaluation Oct 15–16, 2026.

## Layout

| Path | Purpose |
|------|---------|
| `databricks.yml` | DAB root — bundle, resources, dev/prod targets |
| `config/` | Environment + per-source config (zero hardcoding) |
| `src/maven_market/` | Python package: config, logging, ingestion, producers, pipelines |
| `resources/` | DAB resource definitions (pipelines, jobs, dashboards) |
| `policies/` | Cluster policy JSON |
| `sql/governance/` | Catalog/schema DDL, governed tags, ABAC policies, row filters |
| `tests/` | pytest suite (run in CI) |
| `dashboards/` | AI/BI dashboards as `.lvdash.json` |
| `docs/` | Architecture, cost, WAF, POC write-ups; implementation plan |

## Local dev

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
pytest          # run tests
flake8          # lint
databricks bundle validate -t dev
```

Implementation plan: `docs/superpowers/plans/2026-10-11-maven-market-lakehouse.md`.
