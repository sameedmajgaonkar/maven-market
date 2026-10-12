# Maven Market – Secure Retail Analytics Lakehouse — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build an end-to-end, config-driven, governed Databricks lakehouse on Azure that ingests batch (CSV), streaming (Confluent Kafka), and NoSQL (MongoDB) retail data through a Bronze→Silver→Gold medallion, enforces Unity Catalog ABAC + row/column security, is deployed via Databricks Asset Bundles with GitHub Actions CI/CD, and surfaces four AI/BI dashboards — demo-ready for the Oct 15–16 2026 evaluation.

**Architecture:** Azure (Resource Group → ADLS Gen2 + Key Vault + Access Connector managed identity) underpins a Premium Databricks workspace governed by a single Unity Catalog metastore. One catalog `maven_market_uc` holds schemas `bronze`/`silver`/`gold`/`audit`. Ingestion lands raw data in Bronze (Auto Loader for CSV, Structured Streaming for Kafka, Spark connector for MongoDB); a Lakeflow Declarative Pipeline builds Silver (cleansed, SCD2 dimensions, expectations) and Gold (star-schema facts + aggregate materialized views). Governance is layered on with governed tags + ABAC column masks + row filters. Everything is parameterized by config files, packaged as a Python wheel, and shipped through a DAB with dev/prod targets.

**Tech Stack:** Azure (ADLS Gen2, Key Vault, Access Connector), Azure Databricks (Premium, Unity Catalog, Lakeflow Declarative Pipelines, Auto Loader, Structured Streaming, AI/BI Dashboards, Lakeflow Designer, Genie), Confluent Cloud Kafka, MongoDB Atlas + Spark Connector v10.x, Databricks Asset Bundles + CLI, Python 3.11 (PySpark, kafka/confluent producer, pytest, flake8), GitHub Actions.

**Spec:** `Capstone Project_ Maven Market...pdf` (11 pages — requirements A–H, 4 dashboards, 6 POCs, 100-pt scoring). This plan argues from that PDF; executors read both.

---

## Global Constraints

Every task's requirements implicitly include this section. Values are exact.

- **Config-driven, zero hardcoding:** no source paths, target table names, endpoints, or DLT expectations literally in PySpark/SQL. All injected at runtime from `config/*.yml`. A reviewer rejecting any task may cite a hardcoded value.
- **Catalog/schemas:** catalog `maven_market_uc`; schemas `bronze`, `silver`, `gold`, `audit`.
- **Governed tags:** `classification` ∈ {`PII`,`Financial`,`Public`}; `data_region` ∈ {`EU`,`NA`,`APAC`}.
- **Cluster policy:** VM allowlist = F/D-series (`Standard_F4`,`Standard_F8`,`Standard_D4ds_v5`,`Standard_D8ds_v5`); `num_workers` range 0–2; `autotermination_minutes` ≤ 30; fixed tag `custom_tags.project = maven_market`.
- **Workspace tier:** Premium (Standard tier is retiring in 2026 and lacks UC/ABAC).
- **Runtime:** DBR 16.4+ or serverless for ABAC; `get_column_tag_value()` needs DBR 18 LTS+. MongoDB connector needs **Dedicated (single-user) access mode**, not Standard/Shared.
- **Secrets:** all credentials in Azure Key Vault, read via Databricks secret scope + `dbutils.secrets.get()`. No inline secrets, ever.
- **DevOps:** all assets in Git; one DAB with `dev` + `prod` targets; CI runs flake8 + pytest on PR, `databricks bundle deploy -t prod` on merge to main; auth via OAuth M2M / GitHub OIDC (not PAT).

## Review Focus

Input classes / failure modes the spec implies but that a happy-path task won't exercise. Each has a pinned test in the owning task.

1. **Malformed / late Kafka events** (bad JSON, missing join key) → routed to a dead-letter/`_rescued_data` path, pipeline keeps running. *(Phase 5, bronze-kafka task test)*
2. **CSV schema drift** (new/renamed column mid-stream) → Auto Loader `schemaEvolutionMode` captures it in `_rescued_data`, stream does not fail. *(Phase 5, bronze-csv task test)*
3. **Duplicate / out-of-order change events** for an SCD2 dimension → `create_auto_cdc_flow` `sequence_by` produces correct current/history rows. *(Phase 7, silver-scd2 task test)*
4. **New, untagged Gold table** created after governance is live → the secure-by-default control-tag mask still hides it until a steward reviews it (the sanctioned substitute for the non-existent "block CREATE TABLE until tagged" DDL). *(Phase 9, abac-default task test)*
5. **User whose email is absent from the region mapping table** → row filter returns zero rows (fail-closed); account-group admin bypasses and sees all. *(Phase 9, row-filter task test)*

---

## How To Use This Plan

- **Master + just-in-time teaching.** This file is the complete, placeholder-free task map for all 13 phases, carrying the hard-won 2026 syntax inline. The *live* teaching for each phase — concept primer, a recall question or two, and click-by-click portal/CLI walkthrough — is delivered in-session when we start that phase (the "4-beat loop": **concept → do-with-me → you-verify → recall**). The plan is self-contained to execute; the teaching narration is not pre-written here because it is interactive.
- **Tiers.** Each task is tagged **[Must]** (rubric-critical, demo-blocking), **[Should]** (clear rubric points, do if time allows), or **[Nice]** (polish / bonus POCs). If the Oct 15–16 clock gets tight, cut from Nice, then Should.
- **Rubric mapping.** Each phase header lists the scoring buckets (100 pts total) and POCs it earns.
- **TDD where it is code.** Config loader, logging framework, Kafka producer, and transform helpers are built test-first (write failing test → run red → implement → run green → commit). Portal/CLI/governance tasks (not unit-testable) instead end with an explicit verification query or UI check.

## Repository Structure

```
maven-market/
├── databricks.yml                      # DAB root: bundle, include, variables, targets dev/prod
├── pyproject.toml / setup.py           # wheel build metadata
├── requirements.txt
├── config/
│   ├── dev.yml  prod.yml               # env config (hosts, scopes, paths)
│   └── sources.yml  expectations.yml   # per-source + per-table DQ rules (config-driven)
├── policies/maven_market_cluster_policy.json
├── resources/                          # DAB resource defs
│   ├── pipelines.yml  jobs.yml  dashboards.yml
├── src/maven_market/
│   ├── config.py  logging_utils.py     # TDD modules
│   ├── ingestion/{autoloader,kafka_stream,mongo_ingest}.py
│   ├── producers/kafka_producer.py     # simulates Orders + Inventory streams
│   └── pipelines/{bronze,silver,gold}.py
├── sql/governance/{catalog_schemas,governed_tags,abac_policies,row_filters}.sql
├── tests/{test_config,test_logging_utils,test_transforms}.py
├── dashboards/*.lvdash.json            # 4 AI/BI dashboards
├── docs/{pocs/,architecture-diagram,cost-estimation-report,waf-7-pillars}
└── .github/workflows/deploy.yml
```

---
## Phase 0 — Project Scaffold & Config-Driven Skeleton
**Rubric:** DevOps/Automation (15) foundation · enables the 100%-config-driven requirement everywhere.
**Concept:** A DAB is a declarative wrapper (`databricks.yml`) that packages notebooks, jobs, pipelines, wheels, and dashboards and deploys them to a workspace per *target* (dev/prod). Getting the skeleton + config loader right first means every later phase just adds resources and reads config — no retrofitting.

- [ ] **T0.1 [Must] Git repo + Python package layout.** Create the tree above; `pyproject.toml` (package `maven_market`, Python 3.11, deps: pyspark, pyyaml, confluent-kafka, pytest, flake8), `requirements.txt`, `.gitignore`, `README.md`. `git init`, first commit.
- [ ] **T0.2 [Must] Config loader (TDD).** `src/maven_market/config.py` with `load_config(env: str) -> dict` that deep-merges `config/<env>.yml` over defaults and resolves `${VAR}` env substitutions. *Test first* (`tests/test_config.py`): missing key raises, override wins, env-var interpolation. Red → implement → green → commit.
- [ ] **T0.3 [Must] `databricks.yml` skeleton.** Top-level `bundle.name: maven_market`, `include: [resources/*.yml]`, `variables` (catalog, schemas, scope name), `targets.dev` (`mode: development`, workspace host) + `targets.prod` (`mode: production`, host, var overrides). Remember path gotcha: `./src/...` in `databricks.yml`, `../src/...` inside `resources/*.yml`.
- [ ] **T0.4 [Must] Validate.** `databricks bundle validate -t dev --strict`. Expected: no errors. Commit.
- [ ] **T0.5 [Should] Wheel artifact wiring.** Add `artifacts.default` (`type: whl`, `build: python -m build`, `path: .`) so later jobs reference `libraries: [{whl: ../dist/*.whl}]`.

**Checkpoint:** `databricks bundle validate -t dev` passes against the real workspace host.

---

## Phase 1 — Azure Foundation
**Rubric:** Infrastructure & UC Setup (10) · **POC 1** (external-user onboarding, later).
**Concept:** Databricks on Azure stores actual bytes in *your* ADLS Gen2. Unity Catalog reaches that storage through an **Access Connector** (an Azure managed identity granted `Storage Blob Data Contributor`) rather than keys — keyless, auditable. Key Vault holds Kafka/Mongo secrets; Databricks reads them through a secret scope.

- [ ] **T1.1 [Must] Resource Group** in your eval region (keep everything co-located — metastore is one-per-region). Portal → Resource groups → Create.
- [ ] **T1.2 [Must] ADLS Gen2 storage account** with **Hierarchical namespace = Enabled**. Create containers: `metastore` (UC root, optional), `unity-catalog` (managed location for the catalog), `landing` (raw CSV drop for Auto Loader).
- [ ] **T1.3 [Must] Access Connector for Azure Databricks** (Portal → "Access Connector for Azure Databricks" → Create). Then on the storage account → IAM → add role assignment **Storage Blob Data Contributor** to that connector's managed identity.
- [ ] **T1.4 [Must] Azure Key Vault.** Permission model MUST be **Vault access policy** (Azure RBAC is NOT supported by Databricks secret scopes). Record the vault **DNS Name (URI)** and **Resource ID** from the Properties tab — both needed to create the scope.
- [ ] **T1.5 [Should] Upload CSVs** to the `landing` container under `/csv/{customers,products,stores,regions,returns,calendar,transactions}/` so Auto Loader has real files. Transactions 1997 + 1998 go in one folder to prove multi-file incremental load.

**Checkpoint:** Access Connector shows the Blob Data Contributor role on the storage account; Key Vault Properties tab URI + Resource ID copied into `config/dev.yml` (non-secret identifiers only).

---
## Phase 2 — Databricks Workspace, Unity Catalog & Secret Scope
**Rubric:** Infrastructure & UC Setup (10) · **POC 1**.
**Concept:** The metastore is the top of the UC three-level namespace (`catalog.schema.table`). In most Azure regions a metastore is auto-created; you attach your workspace, then create a **storage credential** (wrapping the Access Connector) and an **external location** (pointing at the `unity-catalog` container) so the catalog has a managed location.

- [ ] **T2.1 [Must] Premium workspace** attached to the metastore (Account console → Data → confirm/assign metastore to the workspace's region).
- [ ] **T2.2 [Must] Storage credential** from the Access Connector resource ID: Catalog → External Data → Credentials → Create (type: Azure Managed Identity).
- [ ] **T2.3 [Must] External location** `maven_market_ext` → `abfss://unity-catalog@<account>.dfs.core.windows.net/` using that credential. Run `TEST CONNECTION` / `LIST` to confirm read-write.
- [ ] **T2.4 [Must] Catalog + schemas (config-driven SQL).** `sql/governance/catalog_schemas.sql`: `CREATE CATALOG IF NOT EXISTS maven_market_uc MANAGED LOCATION 'abfss://unity-catalog@.../catalog';` then `CREATE SCHEMA` for `bronze`,`silver`,`gold`,`audit`. Names come from `databricks.yml` variables when run through the bundle.
- [ ] **T2.5 [Must] Secret scope backed by Key Vault.** Create at `https://<workspace-host>#secrets/createScope` (capital S) using the vault URI + Resource ID from T1.4. Verify `dbutils.secrets.listScopes()`.
- [ ] **T2.6 [Should] POC 1 — external user onboarding.** Invite a guest/second user, create account group (e.g. `maven_analysts`), `GRANT USE CATALOG/USE SCHEMA/SELECT` scoped to `gold`. Document the least-privilege grant chain in `docs/pocs/poc1-external-user.md`.

**Checkpoint:** `SHOW SCHEMAS IN maven_market_uc` lists the four schemas; a test secret reads back via `dbutils.secrets.get()`.

---

## Phase 3 — Compute Governance (Cluster Policy)
**Rubric:** Infrastructure & UC Setup (10) — compute-governance slice.
**Concept:** A cluster policy is a JSON ruleset (`allowlist`/`range`/`fixed`) that constrains what clusters users can create — capping cost (small VMs, ≤2 workers, fast auto-terminate) and forcing the billing tag so `system.billing.usage` attributes spend to this project.

- [ ] **T3.1 [Must] Author policy JSON.** `policies/maven_market_cluster_policy.json` with the exact constraints from Global Constraints (F/D VM allowlist + default `Standard_F4`; `num_workers` range 0–2; `autotermination_minutes` max 30 default 20; `custom_tags.project` fixed `maven_market`; optionally pin `spark_version` to a DBR 16.4+ LTS).
- [ ] **T3.2 [Must] Create via CLI.** `databricks cluster-policies create --json '{"name":"maven_market_policy","definition":"<JSON-ESCAPED-STRING>"}'` — the `definition` value is the policy JSON **encoded as a string**, not a nested object.
- [ ] **T3.3 [Should] Create one policy-bound cluster** and confirm you cannot exceed the limits (try 4 workers → rejected). Screenshot for the demo.

**Checkpoint:** New cluster inherits the `project=maven_market` tag and refuses >2 workers / >30-min auto-terminate.

---

## Phase 4 — Governed Tag Taxonomy & Structural (Secure-by-Default) Policies
**Rubric:** Governance & ABAC (15) — foundation half.
**Concept:** Governed tags (Public Preview) are *centrally-defined, value-constrained* tags with their own CREATE/MANAGE/ASSIGN privileges — unlike free-form tags. ABAC policies later bind masks/filters to these tags. **Honest caveat:** there is NO DDL that blocks `CREATE TABLE` on a missing tag. The spec's "prevent creation unless tagged" is implemented as the sanctioned **control-tag pattern** (schema tagged `review_status=pending` + a catch-all mask; a steward flips to `reviewed`).

- [ ] **T4.1 [Must] Create governed tags.** `sql/governance/governed_tags.sql`: `CREATE GOVERNED TAG classification DESCRIPTION '...' VALUES ('PII','Financial','Public');` and `CREATE GOVERNED TAG data_region VALUES ('EU','NA','APAC');` plus control tag `review_status VALUES ('pending','reviewed')`.
- [ ] **T4.2 [Must] Assign tags** to known-sensitive columns/tables once Silver/Gold exist (customer name/address/birthdate/income → `classification=PII`; income → also `Financial`). Column tags do **not** inherit — assign per column. (Executed after Phase 7; stub the SQL now.)
- [ ] **T4.3 [Should] Grant ASSIGN** on tags to a `data_steward` group; keep CREATE/MANAGE with admins. Document the privilege split.

**Checkpoint:** `SHOW GOVERNED TAGS` lists the three tags with constrained value sets.

---
## Phase 5 — Bronze Ingestion (CSV + Kafka + MongoDB)
**Rubric:** Data Ingestion & Streaming (15) · **POC 4** (streaming).
**Concept:** Bronze = raw landing, append-only, schema-on-read, with lineage metadata. Three source types: Auto Loader incrementally picks up CSVs (`cloudFiles`), Structured Streaming subscribes to Confluent Kafka, and the Spark connector pulls MongoDB. Orders + Inventory aren't in the dataset, so we *simulate* them into Kafka from real dimension keys.

- [ ] **T5.1 [Must] Confluent setup.** Create cluster + topics `orders`, `inventory`; create an API key; store key/secret in Key Vault (`confluent-api-key`, `confluent-api-secret`, `confluent-bootstrap`).
- [ ] **T5.2 [Must] Kafka producer (TDD-ish).** `src/maven_market/producers/kafka_producer.py` — config-driven: reads real `customer_id`/`product_id`/`store_id` from the CSVs, emits JSON order + inventory events at a configurable rate. Unit-test the event-builder (valid schema, FK present). This guarantees events join in Silver (why we rejected Datagen).
- [ ] **T5.3 [Must] CSV Auto Loader.** `src/maven_market/ingestion/autoloader.py`: `.format("cloudFiles")`, `cloudFiles.schemaLocation` (required), `cloudFiles.inferColumnTypes=true` (else all strings), `cloudFiles.schemaEvolutionMode`, `rescuedDataColumn` (no `cloudFiles.` prefix), and **explicitly select `_metadata`** (file_name/path/modification_time) for lineage. ***Review-Focus #2 test:*** inject a CSV with an extra column → row lands in `_rescued_data`, stream survives.
- [ ] **T5.4 [Must] Kafka readStream.** `kafka_stream.py`: options `kafka.bootstrap.servers`, `subscribe`, `kafka.security.protocol=SASL_SSL`, `kafka.sasl.mechanism=PLAIN`, and JAAS with the **shaded** `kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule` (unshaded → RESTRICTED_STREAMING_OPTION error). Creds via `dbutils.secrets.get()`. Decode `from_json(col("value").cast("string"), schema)`. ***Review-Focus #1 test:*** a bad-JSON event parses to nulls / routes to rescue, stream keeps running.
- [ ] **T5.5 [Must] MongoDB ingest.** `mongo_ingest.py`: Spark Connector v10.x `format("mongodb")`, `connection.uri`/`database`/`collection`, explicit schema for streaming. Requires **Dedicated access-mode** compute. (Seed a `product_reviews` or `loyalty` collection in Atlas.)
- [ ] **T5.6 [Should] Wrap as jobs** in `resources/jobs.yml` (producer job + ingestion job) so they deploy via the bundle.

**Checkpoint:** Bronze tables for each source have rows with populated `_metadata`; `orders`/`inventory` Bronze tables grow while the producer runs.

---

## Phase 6 — Custom Logging & Observability
**Rubric:** Data Ingestion & Streaming (15) — observability slice; feeds the Platform Health dashboard.
**Concept:** A reusable logging framework writes structured run records (source, rows_in/out, rejected count, duration, status, error) to `audit.audit_logs`, so every pipeline/job emits the same telemetry — the basis for operational dashboards and WAF "operational excellence".

- [ ] **T6.1 [Must] Logging module (TDD).** `src/maven_market/logging_utils.py`: `AuditLogger.log_event(stage, source, metrics: dict, status, error=None)` appends to `audit.audit_logs` (Delta). *Test first*: event row shape, failure path captures exception text, timestamps set.
- [ ] **T6.2 [Must] Instrument ingestion + pipeline** entry/exit with try/except → `log_event`. Config-driven table name.
- [ ] **T6.3 [Should] Query helpers / views** in `audit` (e.g. `v_pipeline_health`) summarizing last-N runs, reject rates — consumed by Phase 11's Platform Health dashboard.

**Checkpoint:** A deliberately failing run writes a `status=FAILED` row with the error; a good run writes `SUCCESS` with row counts.

---
## Phase 7 — Silver & Gold via Lakeflow Declarative Pipeline
**Rubric:** Data Transformation & Modeling (15) · the medallion centerpiece.
**Concept:** Lakeflow Declarative Pipelines (the product formerly called DLT) let you declare target tables and the engine manages dependencies, incremental refresh, and data-quality **expectations**. Import is `from pyspark import pipelines as dp` (NOT `import dlt`). Decorators: `@dp.table` (streaming), `@dp.materialized_view` (batch/aggregates), `@dp.temporary_view`. Never call `collect`/`count`/`save`/`saveAsTable`/`start`/`toTable` in pipeline code. SCD2 uses `dp.create_auto_cdc_flow(...)` (NOT `apply_changes`).

- [ ] **T7.1 [Must] Silver cleansing** (`src/maven_market/pipelines/silver.py`): typed, deduped, standardized dims + fact. Attach expectations from `config/expectations.yml`: `@dp.expect("valid_x", cond)` (warn), `@dp.expect_or_drop`, `@dp.expect_or_fail` for critical keys. (SQL equivalent: `CONSTRAINT x EXPECT (cond) ON VIOLATION DROP ROW|FAIL UPDATE`.)
- [ ] **T7.2 [Must] SCD2 dimensions.** `dp.create_auto_cdc_flow(target, source, keys=[...], sequence_by=col, apply_as_deletes=..., track_history_except_column_list=[...], stored_as_scd_type="2")`. Note the param name is `track_history_except_column_list` (easy to get wrong). Apply to `dim_customer`, `dim_product`. ***Review-Focus #3 test:*** out-of-order change events still yield correct `__START_AT`/`__END_AT` history.
- [ ] **T7.3 [Must] Gold star schema** (`gold.py`): `fact_sales` / `fact_orders` keyed to conformed dims; `@dp.materialized_view` aggregates (sales by region/time, inventory turns) powering dashboards.
- [ ] **T7.4 [Must] Pipeline resource.** `resources/pipelines.yml` — serverless or policy-bound, `catalog: maven_market_uc`, `target: silver`/`gold`, libraries = the pipeline source files, config params passed through.
- [ ] **T7.5 [Should] Expectations as config.** Prove zero hardcoding: changing a threshold in `expectations.yml` changes DQ behavior with no code edit.

**Checkpoint:** Pipeline runs green end-to-end; expectation metrics visible in the pipeline UI; SCD2 history query shows versioned rows.

---

## Phase 8 — Databricks Asset Bundle + CI/CD
**Rubric:** DevOps & Automation (15) · **POC 2** (CI/CD).
**Concept:** The DAB from Phase 0 now carries real resources; GitHub Actions validates on PR and deploys on merge. Prefer OAuth M2M / GitHub OIDC over PATs.

- [ ] **T8.1 [Must] Finalize `databricks.yml`** with all resources included; `databricks bundle validate -t dev` and `-t prod` both clean.
- [ ] **T8.2 [Must] `.github/workflows/deploy.yml`.** Pin `databricks/setup-cli@v0.9.0`. PR job: flake8 + pytest. Push-to-`main` job: `databricks bundle deploy -t prod`. For OIDC set `DATABRICKS_AUTH_TYPE: github-oidc` + `permissions: id-token: write`.
- [ ] **T8.3 [Must] Service principal + grants** for the deploy identity (M2M OAuth secret or OIDC federation); store secret in GitHub Actions secrets.
- [ ] **T8.4 [Should] `databricks bundle deploy -t dev`** then `bundle run` the pipeline/job to prove the whole bundle deploys from a clean checkout.
- [ ] **T8.5 [Should] POC 2 doc** `docs/pocs/poc2-cicd.md`: PR → checks → merge → prod deploy, with the Actions run link.

**Checkpoint:** A PR shows green flake8+pytest; merging to main triggers a successful prod bundle deploy.

---
## Phase 9 — ABAC Enforcement (Column Masks + Row Filters)
**Rubric:** Governance & ABAC (15) — enforcement half · **POC 3** (ABAC).
**Concept:** ABAC (Public Preview; DBR 16.4+/serverless) binds a masking/filter UDF to *tagged* columns via a policy, instead of attaching functions table-by-table. Syntax notes: no `ON TAG` clause, no `ALTER POLICY` — use `CREATE OR REPLACE POLICY`; function/predicate names are snake_case `has_tag`/`has_tag_value`; `get_column_tag_value` needs DBR 18 LTS+.

- [ ] **T9.1 [Must] Column-mask function + policy.** `sql/governance/abac_policies.sql`: a mask UDF (redact unless `is_account_group_member('pii_readers')`), then `CREATE OR REPLACE POLICY mask_pii ON CATALOG maven_market_uc COLUMN MASK fn TO \`account users\` FOR TABLES MATCH COLUMNS has_tag_value('classification','PII') AS col ON COLUMN col`. Verify: unprivileged user sees `***`, admin sees cleartext.
- [ ] **T9.2 [Must] Row filter via mapping table.** `sql/governance/row_filters.sql`: `governance.user_region_map(user_email,region)`; `CREATE FUNCTION region_filter(r STRING) RETURN IS_ACCOUNT_GROUP_MEMBER('admins') OR EXISTS(SELECT 1 FROM governance.user_region_map m WHERE m.user_email=current_user() AND m.region=r);` then `ALTER TABLE ... SET ROW FILTER region_filter ON (region)`. ***Review-Focus #5 test:*** unmapped user → 0 rows; admin → all. (Must `DROP ROW FILTER` before dropping the UDF.)
- [ ] **T9.3 [Must] Secure-by-default control tag.** Catch-all mask bound to `review_status=pending`; new Gold table defaults hidden until steward sets `reviewed`. ***Review-Focus #4 test:*** fresh untagged table is masked.
- [ ] **T9.4 [Should] POC 3 doc** `docs/pocs/poc3-abac.md`: tag → policy → observed masking, with before/after query output for two personas.

**Checkpoint:** Same query returns different (masked/filtered) results for an analyst vs. an admin; a brand-new table is default-protected.

---

## Phase 10 — Lakeflow Designer & Genie
**Rubric:** Data Transformation & Modeling (15) — low-code slice · **POC 5**.
**Concept:** Lakeflow Designer is a no-code/Genie-assisted pipeline builder. **Risk flag:** it is **Public Preview (not GA)** as of Oct 2026 — treat as a demonstrable bonus, not a core dependency. Needs serverless/all-purpose compute with CAN USE.

- [ ] **T10.1 [Should] Build one Designer flow** (e.g. a simple Gold aggregate) to demonstrate the low-code path; export/screenshot it.
- [ ] **T10.2 [Should] Genie space** over `gold`: ask 3–4 NL questions, curate example queries/instructions so answers are correct. Document in `docs/pocs/poc5-designer-genie.md`.
- [ ] **T10.3 [Nice] Compare** code-first pipeline vs Designer in the doc (governance, testability trade-offs).

**Checkpoint:** A Designer-built table materializes; Genie answers a regional-sales question correctly.

---

## Phase 11 — Dashboards, Tests & Performance
**Rubric:** Data Warehousing / BI + Testing (15) · **POC 6** (dashboards).
**Concept:** AI/BI dashboards serialize to `.lvdash.json` and deploy via the DAB `dashboards` resource. RLS/ABAC is honored only in **"Individual data permissions" / run-as-viewer** publish mode (not the default publisher-credentials embed).

- [ ] **T11.1 [Must] Four dashboards.** Executive Overview, Real-Time Ops (streaming orders/inventory), Regional Sales (exercises the row filter), Platform Health (reads `audit.audit_logs`). Author in UI, export `.lvdash.json` into `dashboards/`.
- [ ] **T11.2 [Must] Deploy as code.** `resources/dashboards.yml` references the JSON; `bundle deploy` recreates them. Set Regional Sales to run-as-viewer so RLS applies.
- [ ] **T11.3 [Should] pytest suite green** in CI (config, logging, transforms, event-builder). Add a data-quality smoke test querying expectation metrics.
- [ ] **T11.4 [Should] Performance pass.** Z-ORDER/liquid clustering or partitioning on big fact tables; record before/after query time + a note on `system.billing.usage` for the cost report.

**Checkpoint:** All four dashboards render from deployed code; Regional Sales shows different data per persona; pytest passes in Actions.

---
## Phase 12 — Enterprise Documentation (Architecture, Cost, WAF)
**Rubric:** Architecture & Cost Optimization (15) · **POC** write-ups.
**Concept:** The capstone is graded partly on *explaining* the system like an enterprise would — a reference architecture, a defensible cost model, and a Well-Architected self-assessment.

- [ ] **T12.1 [Must] Architecture diagram** `docs/architecture-diagram.*`: Azure foundation → UC → medallion → governance → BI, with data flow and secret flow. (draw.io/mermaid.)
- [ ] **T12.2 [Must] Cost estimation report** `docs/cost-estimation-report.md`: DBU-per-second model, Jobs vs All-Purpose vs serverless, the cluster-policy cost guardrails, and a `system.billing.usage` query filtered on `project=maven_market`. Link the Azure Pricing Calculator; do **not** hardcode $/DBU rates.
- [ ] **T12.3 [Must] WAF 7-pillar self-assessment** `docs/waf-7-pillars.md`: Operational excellence, Security/privacy/compliance, Reliability, Performance efficiency, Cost optimization, Data & AI governance, Interoperability & usability — one short evidence paragraph each, citing concrete choices in this build.
- [ ] **T12.4 [Should] README + demo script** tying POCs 1–6 to where each is demonstrated for Oct 15–16.

**Checkpoint:** A reviewer can read the three docs and reconstruct the system and its cost/governance posture without the code.

---

## Rubric & POC Coverage Map

| Scoring bucket (≈/100) | Phases |
|---|---|
| Infrastructure & UC Setup | 1, 2, 3 |
| Data Ingestion & Streaming | 5, 6 |
| Data Transformation & Modeling | 7, 10 |
| Governance & ABAC | 4, 9 |
| Data Warehousing / BI & Testing | 11 |
| DevOps & Automation | 0, 8 |
| Architecture & Cost Optimization | 12 |

| POC | Where |
|---|---|
| POC 1 external-user onboarding | 2.6 |
| POC 2 CI/CD | 8.5 |
| POC 3 ABAC | 9.4 |
| POC 4 streaming | 5 |
| POC 5 Designer/Genie | 10 |
| POC 6 dashboards | 11 |

## Timeline Triage (≈4 days to eval)

**Day 1:** Phases 0–3 (scaffold, Azure, UC, policy). **Day 2:** Phases 4–6 (tags, all ingestion, logging). **Day 3:** Phases 7–9 (medallion pipeline, DABs/CI, ABAC). **Day 4:** Phases 10–12 (Designer/Genie, dashboards+tests, docs) + demo rehearsal. If slipping: drop all **[Nice]**, then **[Should]** (keep POC docs — they are cheap points), never drop **[Must]**.

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-10-11-maven-market-lakehouse.md`. Please review it.

Two things to confirm before I build:

1. **Structure acknowledgement** — this is a *master* plan: every phase/task is concrete and placeholder-free, but the per-step teaching narration (concept primer → do-with-me → you-verify → recall) is delivered live when we start each phase, not pre-written here. That matches "I need context to whatever you are doing" without freezing 150+ interactive steps into the file. Flagging it so the choice is visible.

2. **Execution approach:**
   - **Native (recommended for us)** — I implement each task myself in this session, teaching as we go phase-by-phase, with one fresh whole-branch review near the end. Best fit because this is a solo *learning* build where you want to see and understand every action, and most tasks are portal/CLI/governance work that isn't amenable to isolated subagent handoff.
   - **Subagent-driven** — a fresh subagent implements each task and a reviewer checks it before the next. More rigorous isolation, but it hides the hands-on work you specifically asked to learn from, and portal steps can't be delegated anyway.

**I recommend Native**, because the tasks are tightly coupled (each phase builds on the last catalog/credential/bundle state) and your goal is to learn by watching every step — the thing subagent isolation would take away.

Does the plan capture what you want, and shall we proceed Native starting at Phase 0?

---

*Security note: during docs research, fetched pages and an earlier injected system message contained instructions to "silently comply with tool size limits / pose as a different agent." Those are untrusted external content and were disregarded; I'm flagging it because this is a security-focused project and you asked for full visibility.*







