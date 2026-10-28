# POC 2 - CI/CD with GitHub Actions and Databricks Asset Bundles

Workflow: `.github/workflows/deploy.yml`

| Event | Jobs |
|---|---|
| Pull request to `main` | `test` (flake8 + pytest + wheel build) -> `validate` (`bundle validate` for dev and prod) |
| Push / merge to `main` | `test` -> `validate` -> `deploy-prod` (`bundle deploy -t prod`) |

**Authentication is OIDC workload-identity federation.** GitHub issues a short-lived token for the
run, Databricks exchanges it for an OAuth token for a service principal. No personal access token
and no long-lived Databricks secret is stored anywhere.

The `validate` and `deploy-prod` jobs are skipped until the repository variable `DATABRICKS_HOST`
exists, so the workflow can be merged before the setup below is finished. `test` always runs.

## One-time setup

### 1. Service principal (Databricks account console)
1. `accounts.azuredatabricks.net` -> **User management -> Service principals -> Add service principal**.
2. Name it `github-cicd`. Note its **Application (client) ID** and its numeric **ID** (shown in the URL).
3. **Workspaces** tab of the account console: add the service principal to the `dbw-maven-market`
   workspace (permission **User** is enough to deploy a bundle).

### 2. Federation policy (account-level CLI)
The subject must match the token's `sub` claim **exactly**. GitHub repositories created on or after
2026-07-15 use an immutable format with numeric IDs. This repository's value was read from
`gh api repos/sameedmajgaonkar/maven-market/actions/oidc/customization/sub`:

```
repo:sameedmajgaonkar@120316421/maven-market@1415953781
```

so, for jobs that use the `prod` environment, the full subject is
`repo:sameedmajgaonkar@120316421/maven-market@1415953781:environment:prod`.

```bash
databricks auth login --host https://accounts.azuredatabricks.net --account-id <ACCOUNT_ID>
databricks account service-principal-federation-policy create <SP_NUMERIC_ID> --json '{
  "oidc_policy": {
    "issuer": "https://token.actions.githubusercontent.com",
    "audiences": ["<ACCOUNT_ID>"],
    "subject": "repo:sameedmajgaonkar@120316421/maven-market@1415953781:environment:prod"
  }
}'
```

### 3. Permissions for the service principal
`bundle deploy` writes to `/Workspace/Shared/.bundle/maven_market/prod` and creates jobs and a
pipeline. Grant, as a workspace/metastore admin:

```sql
GRANT USE CATALOG ON CATALOG maven_market_uc TO `<SP_APPLICATION_ID>`;
GRANT USE SCHEMA, CREATE TABLE, SELECT, MODIFY ON SCHEMA maven_market_uc.bronze TO `<SP_APPLICATION_ID>`;
GRANT USE SCHEMA, CREATE TABLE, CREATE MATERIALIZED VIEW ON SCHEMA maven_market_uc.silver TO `<SP_APPLICATION_ID>`;
GRANT USE SCHEMA, CREATE TABLE, CREATE MATERIALIZED VIEW ON SCHEMA maven_market_uc.gold TO `<SP_APPLICATION_ID>`;
GRANT USE SCHEMA, CREATE TABLE, SELECT, MODIFY ON SCHEMA maven_market_uc.audit TO `<SP_APPLICATION_ID>`;
GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION loc_landing TO `<SP_APPLICATION_ID>`;
GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION loc_unity_catalog TO `<SP_APPLICATION_ID>`;
```
Deploying alone needs far less; these let the service principal also *run* the prod jobs.

### 4. GitHub configuration
```bash
gh variable set DATABRICKS_HOST --body "https://adb-7405615136388825.5.azuredatabricks.net"
```
Then in the repo: **Settings -> Environments -> New environment -> `prod`** and add the environment
secret `DATABRICKS_CLIENT_ID` = the service principal's application (client) ID.

Do **not** add required reviewers to `prod` if you want pull-request validation to run
automatically: both `validate` and `deploy-prod` use this environment (their OIDC subject depends on it).

Optional: **Settings -> Branches -> add rule for `main`** requiring the `Lint and unit tests` and
`Validate bundle (dev + prod)` checks, so nothing merges unless CI is green.

## Demonstration for the evaluation
1. Create a branch, change something small, open a pull request.
2. Show the PR checks: lint, tests, validate (dev + prod).
3. Break something deliberately (an unused import) and show flake8 fail the PR; fix it.
4. Merge. Show the push-to-main run: `deploy-prod` deploys the bundle.
5. In the workspace, show the prod jobs/pipeline created by the service principal.

Actions run link: https://github.com/sameedmajgaonkar/maven-market/actions/runs/38306674680.

## Known limitation
`dev` and `prod` share one workspace and one catalog here, so a **prod pipeline would write the same
`silver`/`gold` tables as the dev pipeline**; Unity Catalog allows only one pipeline to own a
table. Deploying prod is safe; *running* both pipelines against the same tables is not. A real
setup would use a separate workspace or catalog per environment (a variable override in the `prod`
target). Say this in the architecture write-up rather than hiding it.
