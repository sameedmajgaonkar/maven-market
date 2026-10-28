"""Build the external CSV used by the Lakeflow Designer POC.

The file simulates a district manager's annual store targets exported from a
planning system. It is deliberately *outside* the medallion pipeline; Designer
joins it to Gold dimensions to produce an analyst-owned attainment table.

Run with the Databricks CLI profile that can read Gold:
    python scripts/build_designer_targets_csv.py --profile ak-dbx \
        --warehouse-id <warehouse-id>
"""
import argparse
import csv
import json
import subprocess
from pathlib import Path

import yaml

OUT = Path("data/external/store_targets_1998.csv")
CONFIG = Path(__file__).resolve().parents[1] / "config" / "defaults.yml"

DISTRICT_MANAGERS = [
    "A. Rao",
    "M. Chen",
    "S. Patel",
    "J. Garcia",
    "L. Nguyen",
    "R. Okafor",
    "T. Andersson",
    "K. Watanabe",
]


def query(profile: str, warehouse_id: str, statement: str) -> list[list]:
    body = json.dumps({"warehouse_id": warehouse_id, "statement": statement, "wait_timeout": "50s"})
    proc = subprocess.run(
        ["databricks", "api", "post", "/api/2.0/sql/statements", "-p", profile, "--json", body, "-o", "json"],
        capture_output=True,
        text=True,
        check=True,
    )
    payload = json.loads(proc.stdout)
    if payload.get("status", {}).get("state") != "SUCCEEDED":
        raise RuntimeError(payload.get("status"))
    return payload.get("result", {}).get("data_array", [])


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", required=True, help="Databricks CLI profile that can read Gold")
    parser.add_argument("--warehouse-id", required=True, help="SQL warehouse used for the two reads")
    args = parser.parse_args()

    cfg = yaml.safe_load(CONFIG.read_text())
    gold = f"{cfg['catalog']}.{cfg['schemas']['gold']}"

    stores = query(
        args.profile,
        args.warehouse_id,
        f"SELECT store_id, revenue FROM {gold}.agg_store_performance ORDER BY store_id",
    )
    brands = [
        row[0]
        for row in query(
            args.profile,
            args.warehouse_id,
            f"SELECT DISTINCT product_brand FROM {gold}.dim_product ORDER BY product_brand",
        )
    ]

    revenues = sorted(float(row[1]) for row in stores)
    a_cut = revenues[int(len(revenues) * 0.75)]
    b_cut = revenues[int(len(revenues) * 0.25)]

    OUT.parent.mkdir(parents=True, exist_ok=True)
    with OUT.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            ["store_id", "district_manager", "annual_revenue_target", "focus_brand", "priority_tier", "plan_year"]
        )
        for store_id, revenue in stores:
            store_id = int(store_id)
            revenue = float(revenue)
            factor = 0.90 + (store_id % 11) * 0.025
            target = round(revenue * factor / 1000) * 1000
            tier = "A" if revenue >= a_cut else "B" if revenue >= b_cut else "C"
            writer.writerow(
                [
                    store_id,
                    DISTRICT_MANAGERS[(store_id - 1) % len(DISTRICT_MANAGERS)],
                    target,
                    brands[(store_id * 7) % len(brands)],
                    tier,
                    1998,
                ]
            )

    print(f"Wrote {len(stores)} rows to {OUT}")


if __name__ == "__main__":
    main()
