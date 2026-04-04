#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["psycopg[binary]", "python-dotenv"]
# ///
"""Load CSV data into a Lakebase instance and run sample queries."""

import csv
import glob
import json
import os
import sys

import psycopg


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS companies (
    company_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    ticker TEXT,
    cik INTEGER,
    cusip TEXT
);

CREATE TABLE IF NOT EXISTS asset_managers (
    manager_id TEXT PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS asset_manager_holdings (
    manager_id TEXT REFERENCES asset_managers(manager_id),
    company_id TEXT REFERENCES companies(company_id),
    shares BIGINT,
    PRIMARY KEY (manager_id, company_id)
);

CREATE TABLE IF NOT EXISTS financial_metrics (
    company_id TEXT REFERENCES companies(company_id),
    metric_id TEXT,
    name TEXT,
    value TEXT,
    period TEXT,
    PRIMARY KEY (company_id, metric_id)
);
"""

QUERIES = [
    (
        "Total companies and asset managers",
        """
        SELECT
            (SELECT COUNT(*) FROM companies) AS companies,
            (SELECT COUNT(*) FROM asset_managers) AS asset_managers,
            (SELECT COUNT(*) FROM asset_manager_holdings) AS holdings,
            (SELECT COUNT(*) FROM financial_metrics) AS metrics
        """,
    ),
    (
        "Top 5 asset managers by total shares held",
        """
        SELECT am.name, SUM(h.shares) AS total_shares
        FROM asset_manager_holdings h
        JOIN asset_managers am ON am.manager_id = h.manager_id
        GROUP BY am.name
        ORDER BY total_shares DESC
        LIMIT 5
        """,
    ),
    (
        "Holdings by company (total shares across all managers)",
        """
        SELECT c.ticker, c.name, SUM(h.shares) AS total_shares
        FROM asset_manager_holdings h
        JOIN companies c ON c.company_id = h.company_id
        GROUP BY c.ticker, c.name
        ORDER BY total_shares DESC
        """,
    ),
    (
        "Berkshire Hathaway portfolio",
        """
        SELECT c.ticker, c.name, h.shares
        FROM asset_manager_holdings h
        JOIN companies c ON c.company_id = h.company_id
        JOIN asset_managers am ON am.manager_id = h.manager_id
        WHERE am.name LIKE '%Berkshire%'
        ORDER BY h.shares DESC
        """,
    ),
]


def load_connection():
    """Load connection details from the most recent *-connection.json file."""
    script_dir = os.path.dirname(__file__)
    files = sorted(
        glob.glob(os.path.join(script_dir, "*-connection.json")),
        key=os.path.getmtime,
        reverse=True,
    )
    if not files:
        print("Error: no *-connection.json file found. Run ./deploy.py deploy first.", file=sys.stderr)
        sys.exit(1)
    with open(files[0]) as f:
        data = json.load(f)
    print(f"Using connection from {os.path.basename(files[0])}")
    return data["connection"]


def load_csv(cursor, table, csv_path, columns):
    """Load a CSV file into a table."""
    with open(csv_path) as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    if not rows:
        return 0
    placeholders = ", ".join(["%s"] * len(columns))
    col_names = ", ".join(columns)
    sql = f"INSERT INTO {table} ({col_names}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"
    for row in rows:
        cursor.execute(sql, [row[col] for col in reader.fieldnames])
    return len(rows)


def main():
    conn_info = load_connection()

    conninfo = (
        f"host={conn_info['host']} port={conn_info['port']} "
        f"dbname={conn_info['database']} user={conn_info['username']} "
        f"password={conn_info['password']} sslmode=require"
    )

    print(f"Connecting to {conn_info['host']}...")
    with psycopg.connect(conninfo) as conn:
        with conn.cursor() as cur:
            # Create schema
            print("\nCreating tables...")
            cur.execute(SCHEMA_SQL)
            conn.commit()
            print("  Tables created.")

            # Load data
            data_dir = os.path.join(os.path.dirname(__file__), "data")
            loads = [
                ("companies", "companies.csv", ["companyId", "name", "ticker", "cik", "cusip"]),
                ("asset_managers", "asset_managers.csv", ["managerId", "name"]),
                ("asset_manager_holdings", "asset_manager_holdings.csv", ["managerId", "companyId", "shares"]),
                ("financial_metrics", "financial_metrics.csv", ["companyId", "metricId", "name", "value", "period"]),
            ]

            print("\nLoading data...")
            for table, filename, csv_cols in loads:
                path = os.path.join(data_dir, filename)
                # Map CSV column names to SQL column names (camelCase -> snake_case)
                col_map = {
                    "companyId": "company_id", "managerId": "manager_id",
                    "metricId": "metric_id", "name": "name", "ticker": "ticker",
                    "cik": "cik", "cusip": "cusip", "shares": "shares",
                    "value": "value", "period": "period",
                }
                sql_cols = [col_map[c] for c in csv_cols]
                placeholders = ", ".join(["%s"] * len(sql_cols))
                col_names = ", ".join(sql_cols)
                sql = f"INSERT INTO {table} ({col_names}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"

                with open(path) as f:
                    reader = csv.DictReader(f)
                    count = 0
                    for row in reader:
                        cur.execute(sql, [row[c] for c in csv_cols])
                        count += 1
                conn.commit()
                print(f"  {table}: {count} rows loaded")

            # Run queries
            print("\n" + "=" * 60)
            for title, sql in QUERIES:
                print(f"\n{title}")
                print("-" * len(title))
                cur.execute(sql)
                col_names = [desc[0] for desc in cur.description]
                rows = cur.fetchall()

                # Calculate column widths
                widths = [len(c) for c in col_names]
                str_rows = [[str(v) for v in row] for row in rows]
                for row in str_rows:
                    for i, val in enumerate(row):
                        widths[i] = max(widths[i], len(val))

                header = "  ".join(c.ljust(widths[i]) for i, c in enumerate(col_names))
                print(header)
                print("  ".join("-" * w for w in widths))
                for row in str_rows:
                    print("  ".join(val.ljust(widths[i]) for i, val in enumerate(row)))

    print("\n" + "=" * 60)
    print("Done.")


if __name__ == "__main__":
    main()
