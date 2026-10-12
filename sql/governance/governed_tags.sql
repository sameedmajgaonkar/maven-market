-- T4.1: governed tag taxonomy (Public Preview). Run as a metastore/account admin.
-- Governed tags are value-constrained and have their own CREATE/MANAGE/ASSIGN privileges.
-- Syntax follows the project plan (Phase 4); verify against current docs when first executed.

CREATE GOVERNED TAG classification
  DESCRIPTION 'Data sensitivity class used by ABAC masking policies'
  VALUES ('PII', 'Financial', 'Public');

CREATE GOVERNED TAG data_region
  DESCRIPTION 'Geographic region the data belongs to; drives row-level filtering'
  VALUES ('EU', 'NA', 'APAC');

-- Control tag for the secure-by-default pattern (Phase 9, T9.3):
-- new tables are tagged review_status=pending and masked until a steward sets 'reviewed'.
CREATE GOVERNED TAG review_status
  DESCRIPTION 'Steward review state; pending objects are masked by default'
  VALUES ('pending', 'reviewed');

-- T4.3: split of duties. Admins keep CREATE/MANAGE; stewards may only ASSIGN.
-- GRANT ASSIGN ON GOVERNED TAG classification   TO `data_steward`;
-- GRANT ASSIGN ON GOVERNED TAG data_region      TO `data_steward`;
-- GRANT ASSIGN ON GOVERNED TAG review_status    TO `data_steward`;
