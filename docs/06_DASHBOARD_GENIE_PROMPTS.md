# Maven Market — Genie prompts to improve the AI/BI dashboards

Use these prompts in the Databricks **Genie** pane (or Databricks Assistant) with the
corresponding dashboard open. Each prompt is written to be pasted as-is.

## Before you paste

- Work only with the existing `maven_market_uc.gold` and `maven_market_uc.audit` tables.
- Do **not** create or alter tables, change dashboard permissions, change
  `embed_credentials`, change the SQL warehouse, or publish a dashboard without review.
- Preserve row-level ABAC. Never query unfiltered data or use owner credentials to
  bypass a viewer's row filter.
- Do not invent targets, budgets, forecasts, or statistical significance. If a metric
  is not supported by the current tables, say so and propose the smallest schema change.
- Return SQL and a widget plan first; apply and publish only after the queries have been
  validated.
- Keep the visual language: light canvas `#F5F7FA`, white cards, accents `#0B6E99`,
  `#1B998B`, `#F4A261`, `#E76F51`, 10px rounded corners, compact currency, left-aligned
  headers.

| Dashboard | ID |
|---|---|
| Maven Market — Executive Overview | `01f1d30411b714f5a4aeaa09169ac0e6` |
| Maven Market — Real-Time Operations | `01f1d30411c3144ba76479f2e5367938` |
| Maven Market — Regional Sales | `01f1d30411c8107288fc2bce866069c8` |
| Maven Market — Platform Health | `01f1d30411c51e4ea1386dcf133468c6` |

---

## 1. Executive Overview

```text
You are a senior retail analytics editor improving the Databricks AI/BI dashboard
"Maven Market — Executive Overview" (dashboard ID 01f1d30411b714f5a4aeaa09169ac0e6).

CONTEXT
- Gold tables: maven_market_uc.gold.fact_sales (809,162 transactions, Jan 1997–Dec 1998),
  dim_product (1,560 products), dim_store (72 stores), agg_store_performance.
- Current widgets: four KPI counters (revenue, profit, margin, units), a monthly
  revenue/profit line chart, revenue by sales region, top 10 brands, and top 12 stores.
- Audience: the account admin and regional managers. It should read as an executive
  story, not an operations console.
- The dashboard currently shows all-time totals. The business question it answers least
  well is "how are we doing now, and what changed?"

TASK
1. Audit the current page for narrative gaps and visual clutter.
2. Add a date-range control (default: full period) and make the headline KPIs respond to it.
3. Add decision-grade "now" context:
   - latest month vs prior month (revenue, profit, margin, units);
   - latest month vs the same month one year earlier;
   - which regions and brands drove the change.
4. Add 2–3 concentration or outlier insights, for example:
   - share of revenue from the top region and top brand;
   - stores with strong revenue but weak margin;
   - dependence on the top 10 stores.
   Do not fabricate targets or call a change "significant" without a statistical test.
5. Propose a cleaner 12-column grid. Keep the four KPIs and the monthly trend above the
   fold; move detail tables below. Every widget needs a short title and a one-line
   "so what" subtitle.
6. Return:
   (a) a revised widget plan with grid positions;
   (b) Databricks SQL for every new dataset or measure, using fully qualified table names;
   (c) concise callout copy for the executive summary;
   (d) validation queries and expected row counts.

CONSTRAINTS
- Do not change dashboard permissions, credential mode, warehouse, or tables.
- Use only the tables above. If a requested metric is unsupported, explain the gap.
- Keep the existing color palette and compact currency formatting.
```

---

## 2. Real-Time Operations

```text
You are an operations analytics engineer improving the Databricks AI/BI dashboard
"Maven Market — Real-Time Operations" (dashboard ID 01f1d30411c3144ba76479f2e5367938).

CONTEXT
- Gold tables: maven_market_uc.gold.fact_orders (438 orders), fact_inventory
  (161 inventory events), dim_product, dim_store.
- Current widgets: counters for orders, revenue, average order value, and low-stock items;
  orders per minute; orders by region; a low-stock table (quantity_on_hand <= 50); and the
  20 most recent orders.
- The data arrives through the Kafka producer and medallion pipeline, but the dashboard
  reads Gold tables. It is a micro-batch operations view, not a sub-second live feed.
- Audience: operations leads and data engineers who need to spot freshness issues and
  stock risk quickly.

TASK
1. Turn the page into an operations console with a clear "is the feed healthy?" answer.
2. Add freshness indicators:
   - minutes since the latest order;
   - minutes since the latest inventory event;
   - a visible "last updated" timestamp and a stale-data warning threshold.
3. Add short-window activity:
   - orders and revenue in the last 15 and 60 minutes;
   - order velocity in the last 15 minutes vs the prior 15 minutes;
   - the busiest region in the current window.
4. Improve the low-stock view:
   - severity buckets: 0, 1–10, 11–25, 26–50 units;
   - low-stock counts by region and store;
   - the products and stores most exposed;
   - a clear empty state when no items are below threshold.
5. Add an operator runbook callout with 3–4 concrete next steps when the feed is stale or
   low-stock counts spike. You may propose alert thresholds, but do not create alerts.
6. Because the order volume is small, show counts alongside rates and avoid overstating
   statistical confidence.
7. Return:
   (a) the revised widget plan and grid positions;
   (b) SQL for every new dataset, including the freshness and window calculations;
   (c) the runbook copy and empty-state text;
   (d) validation queries and expected results against the current 438 orders and
       161 inventory events.

CONSTRAINTS
- Do not change permissions, credential mode, warehouse, or tables.
- Do not claim the dashboard is a real-time stream if it reads Gold tables.
- Use only the tables above and keep the existing visual style.
```

---

## 3. Regional Sales

```text
You are a regional commercial analyst improving the Databricks AI/BI dashboard
"Maven Market — Regional Sales" (dashboard ID 01f1d30411c8107288fc2bce866069c8).

CONTEXT
- Gold tables: maven_market_uc.gold.fact_sales, dim_store, dim_product,
  agg_store_performance, agg_product_performance.
- The dashboard is protected by row-level ABAC on data_region. Regional managers in
  regional_managers_NA and regional_managers_EU must see only their region. The dashboard
  uses embed_credentials=false, so the viewer's own permissions and row filter apply.
- Current widgets: revenue, profit, margin, and units counters; monthly revenue/profit
  trend; return rate by brand; top 12 stores; top 15 products.
- Audience: a regional manager who needs to run their region, not compare it with
  another region they are not allowed to see.

TASK
1. Strengthen the regional story without weakening ABAC:
   - add current month vs prior month and current month vs the same month last year;
   - add region-local top and bottom stores by revenue and by margin;
   - show product mix and the brands with the highest return rates;
   - add a "your region" context line that names the visible region(s) and period.
2. Add 2–3 decision prompts, for example:
   - which stores are growing but below the region's median margin;
   - which products combine high revenue with above-baseline returns;
   - where a store's revenue per square foot trails the region baseline.
3. Do not add cross-region league tables, national totals, or any query that could expose
   another region. If you need a benchmark, compute it from the viewer's filtered rows and
   label it "region baseline". Never use owner credentials to bypass the row filter.
4. Handle the empty case: if the viewer has no rows for the selected period, show a clear
   "No data for your region and period" message instead of blank charts.
5. Propose a cleaner layout: four KPIs, trend, and one regional insight above the fold;
   stores and products below. Keep titles short and add a one-line "so what" subtitle.
6. Return:
   (a) the revised widget plan and grid positions;
   (b) SQL for each new dataset, with the ABAC-safe aggregation logic called out;
   (c) the "your region" and empty-state copy;
   (d) validation queries to run as a regional manager, including a check that no rows
       from another data_region are returned.

CONSTRAINTS
- Do not change the row filter, permissions, credential mode, warehouse, or tables.
- Use only the tables above.
- If a metric would require unfiltered data, say so and omit it.
```

---

## 4. Platform Health

```text
You are a data platform reliability engineer improving the Databricks AI/BI dashboard
"Maven Market — Platform Health" (dashboard ID 01f1d30411c51e4ea1386dcf133468c6).

CONTEXT
- Audit table: maven_market_uc.audit.audit_logs with event_ts, stage, source, status,
  rows_in, rows_out, rows_rejected, duration_s, error_type, and error.
- Current data: 47 audit events, 45 successes, 2 failures, covering the medallion,
  governance, and pipeline stages.
- Current widgets: counters for runs, success rate, rows rejected, and average duration;
  run outcomes by minute; rows by stage; a stage health table; and a failure detail table.
- The dashboard uses embed_credentials=true so regional managers can view it without
  audit-log access. Only admins and data_engineers should be able to run it.
- The sample is small and comes from a limited number of runs. Do not present the numbers
  as a mature SLA history.

TASK
1. Make the page a triage tool that answers: "Is the platform healthy, what failed, and
   what should I do next?"
2. Add a health score with an explicit, visible formula (for example: success rate,
   reject rate, and freshness, each with a stated weight). Show the inputs next to the
   score so it is auditable.
3. Add:
   - success-rate and failure-rate trend by hour or day, with counts;
   - last successful run and data freshness;
   - rows rejected and reject rate by stage, not only totals;
   - stage duration p50/p95 if the SQL engine supports it, otherwise average plus max;
   - top error types and the most recent failures with a short remediation hint.
4. Improve the failure table: event time, stage, source, error type, a readable error
   summary, duration, and a "what to check" column. Do not expose secrets or full stack
   traces.
5. Add empty states for "no failures in the selected period" and "no audit events yet".
   Make it obvious when the sample is too small for a meaningful rate.
6. Return:
   (a) the revised widget plan and grid positions;
   (b) SQL for the health score, stage percentiles, failure taxonomy, and freshness;
   (c) the health-score definition and remediation copy;
   (d) validation queries against the current 47 events, including the expected
       45/2 success/failure split.

CONSTRAINTS
- Do not change credential mode, permissions, warehouse, or tables.
- Do not expose raw error payloads that may contain secrets or personal data.
- Use only maven_market_uc.audit.audit_logs and keep the existing visual style.
```
