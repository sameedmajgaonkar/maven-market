"""Dry-run the Silver SQL / SCD2 expressions against the real Bronze tables via a SQL warehouse.

Catches SQL errors (typos, ANSI-mode failures, bad regexes) BEFORE a pipeline update, without
creating anything: every statement is a read-only SELECT.

    python scripts/dry_run_transforms.py --profile dbw-maven-market --warehouse-id <id>

Requires the Databricks CLI on PATH, already authenticated for ``--profile``.
Gold SQL reads Silver tables, which only exist after the first pipeline run, so it is not covered.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from maven_market.config import load_config  # noqa: E402
from maven_market.pipelines import transforms as T  # noqa: E402


def run_sql(profile: str, warehouse_id: str, statement: str) -> dict:
    body = json.dumps({"warehouse_id": warehouse_id, "wait_timeout": "50s", "statement": statement})
    proc = subprocess.run(
        ["databricks", "api", "post", "/api/2.0/sql/statements", "--json", body, "-p", profile],
        capture_output=True, text=True,
    )
    out = "\n".join(line for line in proc.stdout.splitlines() if not line.startswith("Warn"))
    try:
        return json.loads(out)
    except json.JSONDecodeError:
        return {"status": {"state": "FAILED", "error": {"message": (proc.stderr or out)[:500]}}}


def check(profile: str, warehouse_id: str, label: str, sql: str) -> bool:
    count = run_sql(profile, warehouse_id, f"SELECT count(*) FROM ({sql})")
    state = count.get("status", {}).get("state")
    if state != "SUCCEEDED":
        msg = count.get("status", {}).get("error", {}).get("message", "unknown error")
        print(f"FAIL  {label}\n      {msg}")
        return False
    rows = count["result"]["data_array"][0][0]
    print(f"ok    {label:34} rows={rows}")
    return True


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", required=True)
    ap.add_argument("--warehouse-id", required=True)
    ap.add_argument("--env", default="dev")
    args = ap.parse_args()

    cfg = load_config(args.env)
    ok = True
    for name, spec in T.SCD2_SOURCES.items():
        bronze = T.names(cfg)[f"bronze_{spec['bronze']}"]
        sql = f"SELECT {', '.join(spec['exprs'])} FROM {bronze}"
        ok &= check(args.profile, args.warehouse_id, f"silver.{name} (scd2 exprs)", sql)
    for name, template in T.SILVER_SQL.items():
        ok &= check(args.profile, args.warehouse_id, f"silver.{name}", T.render(template, cfg))
    print("\nALL OK" if ok else "\nFAILURES ABOVE")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
