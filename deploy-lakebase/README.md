# Deploy Lakebase

A single-script tool to deploy, manage credentials for, and tear down a Databricks Lakebase instance using the Databricks CLI.

## Prerequisites

- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/install.html) installed and authenticated
- [uv](https://docs.astral.sh/uv/getting-started/installation/) installed
- A Databricks workspace with Lakebase enabled

## Setup

Copy the sample environment file and fill in your values:

```bash
cp .env.sample .env
```

| Variable | Description |
|---|---|
| `LAKEBASE_PROJECT_NAME` | Project ID — lowercase letters, digits, and hyphens only (e.g. `my-demo`) |
| `DATABRICKS_PROFILE` | Profile name from `~/.databrickscfg` (default: `DEFAULT`) |

## Usage

```bash
# Create a new Lakebase project and get connection details
./deploy.py deploy

# Generate fresh OAuth credentials (tokens expire after 1 hour)
./deploy.py refresh

# Delete the project and all its resources
./deploy.py destroy
```

## Test the instance

After deploying, run the test script to load sample data and run queries:

```bash
./test_lakebase.py
```

This will:
1. Read connection details from the saved `*-connection.json` file
2. Create tables (`companies`, `asset_managers`, `asset_manager_holdings`, `financial_metrics`)
3. Load CSV data from `data/`
4. Run sample queries showing holdings by manager and company

If the OAuth token has expired, refresh it first:

```bash
./deploy.py refresh
./test_lakebase.py
```

## Output

Both `deploy` and `refresh` save a `<project>-connection.json` file with everything needed to connect:

```json
{
  "project": "my-demo",
  "branch": "production",
  "endpoint": "primary",
  "connection": {
    "host": "ep-xxx.database.eastus.azuredatabricks.net",
    "port": 5432,
    "database": "databricks_postgres",
    "username": "user@example.com",
    "password": "<oauth-token>",
    "token_expires": "2026-04-03T22:16:56Z",
    "jdbc_url": "jdbc:postgresql://ep-xxx...:5432/databricks_postgres",
    "psql": "PGPASSWORD='...' psql 'host=... port=5432 dbname=databricks_postgres user=...'"
  }
}
```

## Authentication

The scripts use OAuth credentials tied to your Databricks identity. Tokens expire after 1 hour — run `./deploy.py refresh` to get a new one.
