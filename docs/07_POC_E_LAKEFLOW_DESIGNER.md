# POC E — Lakeflow Designer visual data preparation

## Goal

Show that an analyst can build a governed, no-code data-preparation flow from Gold
data plus an external CSV, add a Genie Code complex transformation, and write an
analyst-owned output table — without hand-writing the pipeline.

## Workspace artifact

| Item | Value |
|---|---|
| Designer file | `/Workspace/Shared/Maven Market/dev/Maven Market - Store Target Attainment.designer.ipynb` |
| Object ID | `2963651668431013` |
| Owner | Akanksha |
| Output table | `maven_market_uc.sandbox.store_target_attainment` |
| Output mode | `Create or replace` |
| External CSV | `/Volumes/maven_market_uc/sandbox/external/store_targets_1998.csv` |
| CSV generator | `scripts/build_designer_targets_csv.py` (the CSV itself is git-ignored) |

The `sandbox` schema is deliberate: the Gold structural-tag policy would reject a
new untagged Gold table. POC E is an analyst sandbox, not a production Gold table.

## Canvas flow

1. **Source** — `maven_market_uc.gold.dim_store`
2. **Source** — external CSV `store_targets_1998.csv` (header row, inferred types)
3. **Source** — `maven_market_uc.gold.agg_store_performance`
4. **Join** — `dim_store.store_id = csv.store_id`
5. **Join** — `dim_store.store_id = agg_store_performance.store_id`
6. **Filter** — `plan_year = 1998 AND priority_tier = 'A'` (18 stores)
7. **Aggregate** by `sales_region`, `district_manager`:
   - `target_revenue = SUM(annual_revenue_target)`
   - `actual_revenue = SUM(revenue)`
   - `profit = SUM(profit)`
   - `store_count = COUNT(*)`
8. **Genie Code transformation** — calculate attainment, band, and rank.
9. **Output** — replace `maven_market_uc.sandbox.store_target_attainment`.

## Genie Code prompt

This is the prompt embedded as a canvas note and used to generate the complex SQL step:

```text
Build a visual data prep from maven_market_uc.gold.dim_store, the external CSV /Volumes/maven_market_uc/sandbox/external/store_targets_1998.csv, and maven_market_uc.gold.agg_store_performance. Join dim_store to the CSV on store_id, join that to agg_store_performance on store_id, filter plan_year = 1998 and priority_tier = 'A', aggregate by sales_region and district_manager, then add a SQL step for attainment_pct, performance_band, and attainment_rank, and write to maven_market_uc.sandbox.store_target_attainment.
```

The generated SQL step:

```sql
SELECT
  *,
  ROUND(actual_revenue / NULLIF(target_revenue, 0), 4) AS attainment_pct,
  CASE
    WHEN actual_revenue / NULLIF(target_revenue, 0) >= 1.05 THEN 'Exceeds'
    WHEN actual_revenue / NULLIF(target_revenue, 0) >= 0.95 THEN 'Meets'
    WHEN actual_revenue / NULLIF(target_revenue, 0) >= 0.85 THEN 'Below'
    ELSE 'Critical'
  END AS performance_band,
  DENSE_RANK() OVER (
    ORDER BY actual_revenue / NULLIF(target_revenue, 0) DESC
  ) AS attainment_rank
FROM `Aggregate_by_region_and_manager`
```

## Verified result

The live table contains:

| Metric | Value |
|---|---|
| Rows | 17 |
| Distinct sales regions | 9 |
| Stores covered | 18 |
| Columns | 9 |
| Target revenue | 2,329,000 |
| Actual revenue | 2,338,670.74 |
| Profit | 1,395,445.90 |
| Overall attainment | 100.42% |

Rows by data region: EU 6, NA 7, APAC 4. The filter retains 18 stores; the
aggregate produces 17 region/manager rows because one North West manager covers
two stores.

Output columns: `sales_region`, `district_manager`, `target_revenue`,
`actual_revenue`, `profit`, `store_count`, `attainment_pct`,
`performance_band`, `attainment_rank`.

## ABAC behavior

The Designer run executes as the viewer, so the Gold row filter applies:

- Akanksha (admin) ran the canvas and produced all regions.
- Sameed (`regional_managers_EU`) has `SELECT` on the output and sees EU rows.
- A regional manager who reruns the canvas only sees their region; because the
  output mode is `Create or replace`, only Akanksha should own and run it.

## Reproduce

Generate the external CSV from actual Gold store revenue:

```bash
python scripts/build_designer_targets_csv.py \
  --profile ak-dbx \
  --warehouse-id <warehouse-id>
```

Upload the CSV to the sandbox volume, open the Designer file, and click **Run all**.
The current file and output table are owned by Akanksha.
