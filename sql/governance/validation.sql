-- Persona validation for the ABAC policies (POC 3). Run each section in the SQL Editor signed in as
-- that person, on a serverless SQL warehouse, and screenshot the result next to the expected values.
-- Plain SQL with the catalog spelled out so it can be pasted straight into the editor.
-- Row counts are for the dev dataset after the full load (fact_sales 809,162).

-- =====================================================================================================
-- 0. Everyone: who am I, and which identity does the policy see?  (must equal a user_email row)
-- =====================================================================================================
SELECT current_user() AS me,
       is_account_group_member('admin')           AS is_admin,
       is_account_group_member('data_compliance') AS is_data_compliance;

-- =====================================================================================================
-- A. Saakshi (admin + data_compliance) - EXEMPT: sees everything, unmasked
-- =====================================================================================================
SELECT data_region, count(*) AS rows_visible
FROM maven_market_uc.gold.fact_sales GROUP BY 1 ORDER BY 1;
--   expected: APAC 269,722 | EU 269,720 | NA 269,720

SELECT customer_id, first_name, last_name, customer_address, customer_postal_code, customer_acct_num, birthdate
FROM maven_market_uc.gold.dim_customer WHERE customer_id = 1;
--   expected: real values  (Sheri | Nowmer | ... | 15057 | 87462024688 | 1961-08-26)

-- =====================================================================================================
-- B. Akanksha (regional_managers_NA, data_engineers) - mapped to NA only
-- =====================================================================================================
SELECT data_region, count(*) AS rows_visible
FROM maven_market_uc.gold.fact_sales GROUP BY 1 ORDER BY 1;
--   expected: ONE row: NA 269,720   (EU and APAC rows are filtered out)

SELECT data_region, count(DISTINCT store_id) AS stores FROM maven_market_uc.gold.dim_store GROUP BY 1;
--   expected: NA 24

SELECT data_region, count(*) FROM maven_market_uc.gold.agg_product_performance GROUP BY 1;
--   expected: NA 1,559  (aggregates are filtered too, so no company-wide totals leak)

SELECT customer_id, first_name, last_name, customer_address, customer_postal_code, customer_acct_num, birthdate,
       yearly_income
FROM maven_market_uc.gold.dim_customer WHERE customer_id = 1;
--   expected: names/address '***', postal code and account number 0, birthdate 1970-01-01;
--             yearly_income stays readable ($50K - $100K): it is tagged Financial, not PII

SELECT first_name, birthdate FROM maven_market_uc.silver.dim_customer LIMIT 1;   -- masked as well
SELECT first_name, birthdate FROM maven_market_uc.bronze.customers   LIMIT 1;   -- masked as well (data_engineers can read bronze)

-- =====================================================================================================
-- C. Sameed (regional_managers_EU) - mapped to EU only
-- =====================================================================================================
SELECT data_region, count(*) AS rows_visible
FROM maven_market_uc.gold.fact_sales GROUP BY 1 ORDER BY 1;
--   expected: ONE row: EU 269,720

SELECT data_region, count(*) FROM maven_market_uc.gold.fact_returns GROUP BY 1;
--   expected: EU 7,094

SELECT first_name, last_name, birthdate FROM maven_market_uc.gold.dim_customer LIMIT 1;
--   expected: '***' | '***' | 1970-01-01

SELECT count(*) FROM maven_market_uc.bronze.customers;
--   expected: permission error. Sameed is only in regional_managers_EU, which can read gold only.

-- If the FIRST query in this section fails with a permission error on audit.user_region_mapping, the
-- filter function cannot read the mapping table as the querying user. Tell the platform admin; the fix
-- is a one-line GRANT SELECT on that table (see docs).

-- =====================================================================================================
-- D. Review Focus #5 - a user absent from the mapping table gets ZERO rows (fail closed)
--    Run as ADMIN: temporarily remove Sameed's row, then ask Sameed to re-run C (expected: 0 rows, no error),
--    then restore the row with sql/governance/user_region_mapping.seed.sql.
-- =====================================================================================================
-- DELETE FROM maven_market_uc.audit.user_region_mapping WHERE region = 'EU';

-- =====================================================================================================
-- E. Review Focus #4 - a NEW, untagged gold table is unusable until a steward tags it. Run as ADMIN.
-- =====================================================================================================
-- E1. create an untagged table in gold (admin is exempt, so admin still sees its rows)
-- CREATE TABLE maven_market_uc.gold.zz_untagged_demo AS SELECT 1 AS id, 'EU' AS data_region;
-- SELECT count(*) FROM maven_market_uc.gold.zz_untagged_demo;                       -- admin: 1
--
-- E2. as Akanksha:  SELECT count(*) FROM maven_market_uc.gold.zz_untagged_demo;     -- expected: 0
--
-- E3. the pipeline gate flags it (fails the run):
--       databricks bundle run governance_bootstrap -t dev       -- gold_tag_gate FAILED: zz_untagged_demo
--
-- E4. a steward tags it, and the table becomes usable under the normal region rule:
-- SET TAG ON TABLE maven_market_uc.gold.zz_untagged_demo `owner_team` = `data_platform`;
-- SET TAG ON TABLE maven_market_uc.gold.zz_untagged_demo `data_sensitivity` = `internal`;
--
-- E5. clean up:
-- DROP TABLE maven_market_uc.gold.zz_untagged_demo;

-- =====================================================================================================
-- F. Evidence for the write-up (run as admin): the objects behind the behaviour
-- =====================================================================================================
SHOW POLICIES ON CATALOG maven_market_uc;
SHOW POLICIES ON SCHEMA maven_market_uc.gold;
SELECT table_name, tag_name, tag_value FROM maven_market_uc.information_schema.table_tags
WHERE schema_name = 'gold' ORDER BY 1, 2;
SELECT table_name, column_name, tag_name, tag_value FROM maven_market_uc.information_schema.column_tags ORDER BY 1, 2;
SELECT * FROM maven_market_uc.audit.user_region_mapping;
