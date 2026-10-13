-- TEMPLATE. Copy to user_region_mapping.seed.sql (git-ignored), put the real logins in, then run it
-- once in the SQL Editor (or via the SQL API) after row_filters.sql has created the table.
--
-- user_email must equal what `SELECT current_user();` returns for that person. Invited (guest) users
-- in an Entra tenant usually appear in the `name_gmail.com#ext#@tenant.onmicrosoft.com` form; list both
-- forms if you are unsure. Matching is case-insensitive. APAC is deliberately left unmapped so a user
-- outside the table (or mapped elsewhere) visibly gets zero rows: the fail-closed demonstration.
--
-- {catalog} and {audit} are rendered from config, like the other governance files.
DELETE FROM {catalog}.{audit}.user_region_mapping;

INSERT INTO {catalog}.{audit}.user_region_mapping VALUES
  ('na.manager_example.com#ext#@tenant.onmicrosoft.com', 'NA', 'Regional manager NA'),
  ('eu.manager_example.com#ext#@tenant.onmicrosoft.com', 'EU', 'Regional manager EU');
