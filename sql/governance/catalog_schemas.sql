-- T2.4: catalog + schemas. Names come from databricks.yml variables / config, never literals.
-- Run with named parameter markers, e.g. from a SQL task or the Databricks CLI:
--   :catalog           -> maven_market_uc
--   :managed_location  -> abfss://unity-catalog@<account>.dfs.core.windows.net/catalog
--   :bronze_schema / :silver_schema / :gold_schema / :audit_schema
-- NOTE: whether MANAGED LOCATION accepts a parameter marker is unverified; if it errors,
-- substitute the abfss:// URI textually before executing.

CREATE CATALOG IF NOT EXISTS IDENTIFIER(:catalog)
  MANAGED LOCATION :managed_location
  COMMENT 'Maven Market secure retail analytics lakehouse';

CREATE SCHEMA IF NOT EXISTS IDENTIFIER(:catalog || '.' || :bronze_schema) COMMENT 'Raw landing (append-only)';
CREATE SCHEMA IF NOT EXISTS IDENTIFIER(:catalog || '.' || :silver_schema) COMMENT 'Cleansed, conformed, SCD2 dimensions';
CREATE SCHEMA IF NOT EXISTS IDENTIFIER(:catalog || '.' || :gold_schema)   COMMENT 'Star schema + aggregates for BI';
CREATE SCHEMA IF NOT EXISTS IDENTIFIER(:catalog || '.' || :audit_schema)  COMMENT 'Pipeline run logs / observability';
