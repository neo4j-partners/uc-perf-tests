# Neo4j Unity Catalog Federation: Performance Analysis

This project measures query performance for Neo4j running through Databricks Unity Catalog federation. Federated queries against Neo4j add 12 to 16 seconds of overhead per call. The test suite runs the same queries through four connection paths, from direct database access up through the full Databricks federation stack, to isolate exactly where that overhead originates. The finding was cross-validated against Databricks Lakebase using a standard PostgreSQL JDBC driver through the same SafeSpark path, confirming the overhead originates in the SafeSpark sandbox layer and applies to any custom JDBC driver.

## Performance Findings

The SafeSpark sandbox is the source of the overhead. The clearest evidence is a controlled test against the same database through two different connection paths: a federated query through Databricks' built-in PostgreSQL connector completes in 0.752 seconds, while the same query through a custom JDBC connection takes 10.434 seconds. The Neo4j connector, run directly on the Spark executor, completes the same queries in 0.35 to 0.61 seconds. The 12 to 16 second overhead appears only when queries route through SafeSpark.

The overhead is constant regardless of query complexity. A simple count and a multi-hop graph traversal both take the same 12 to 16 seconds. Repeated runs show no improvement over time, meaning the cost is paid on every query call.

See [PERFORMANCE_FINDINGS.md](PERFORMANCE_FINDINGS.md) for the full diagnostic report.

## SafeSpark Investigation

SafeSpark is the security sandbox within Databricks' Lakeguard framework that isolates custom database drivers. When a connection uses a custom JDBC driver, that driver runs in a separate sandboxed process rather than directly inside Spark. This isolation protects the cluster from vulnerabilities Databricks has documented in third-party JDBC drivers, including remote code execution and command injection risks.

The ~12 second per-query overhead reflects four costs at the sandbox boundary: starting the sandboxed process, running a schema inspection before each query, communicating results back to Spark through that boundary, and establishing the database connection. The Neo4j connector's size adds approximately 5 to 6 seconds of load time compared to a smaller driver like PostgreSQL. Increasing sandbox memory by 10x raised query times to 22-26 seconds, confirming the overhead comes from the sandbox architecture rather than resource constraints.

See [SAFESPARK_INVESTIGATION.md](SAFESPARK_INVESTIGATION.md) for the full investigation.

## Quick Start

Before running the tests, complete the cluster and connection setup described in the [Neo4j UC JDBC Guide](https://neo4j-partners.github.io/neo4j-uc-integration/neo4j_uc_jdbc_guide.html), which covers configuring the UC connection, uploading the connector JAR, and loading the lakehouse tables.

The cluster must run a Databricks Runtime with Unity Catalog enabled, have the Neo4j Spark Connector installed, have the Neo4j JDBC connector JAR uploaded to a UC Volume with its path set in `JDBC_JAR_PATH`, and have the Delta lakehouse tables loaded (aircraft, systems, sensors, sensor_readings).

```bash
cp env.sample .env
# Edit .env with your Databricks, Neo4j, and UC JDBC credentials
```

## Usage

### Run the full suite

```bash
python -m perf_scripts.cli run
```

Uploads all scripts to the Databricks workspace and runs every test.

### Run by group

```bash
python -m perf_scripts.cli run --group neo4j      # all Neo4j tests
python -m perf_scripts.cli run --group pg          # just PostgreSQL tests
python -m perf_scripts.cli run --group baseline    # connectivity + direct + delta
python -m perf_scripts.cli run --exclude pg        # everything except PG
```

### Run specific tests

```bash
python -m perf_scripts.cli run --tests warmup,delta
python -m perf_scripts.cli run --tests hello
```

### Upload only

```bash
python -m perf_scripts.cli upload --all            # upload all scripts
python -m perf_scripts.cli upload runner.py        # upload one file
```

### Submit without uploading

```bash
python -m perf_scripts.cli submit --group neo4j
python -m perf_scripts.cli submit --tests direct_neo4j,direct_jdbc --no-wait
```

### Skip upload on run

```bash
python -m perf_scripts.cli run --skip-upload --group baseline
```

## Tests

| Test | Groups | Description |
|------|--------|-------------|
| `hello` | — | Smoke test: verifies remote execution |
| `connectivity` | neo4j, baseline | Basic connectivity across all connection paths |
| `direct_neo4j` | neo4j, baseline | Python driver baseline (no Spark) |
| `direct_jdbc` | neo4j, baseline | Spark JDBC reader (no UC, no SafeSpark) |
| `remote_query` | neo4j, uc | Individual remote_query() timing through SafeSpark |
| `delta` | baseline | Delta lakehouse query baselines |
| `spark_connector` | neo4j | Schema inference vs data pull timing |
| `compound` | neo4j, uc | CROSS JOIN overhead for multiple remote_query() calls |
| `explain` | neo4j, uc | Spark explain plan capture |
| `federated` | neo4j, uc | Step-by-step federated query decomposition |
| `warmup` | neo4j, uc | Repeated query timing (5 runs) |
| `pg_builtin` | pg | PostgreSQL built-in connector (no SafeSpark) |
| `pg_jdbc` | pg | PostgreSQL custom JDBC (through SafeSpark) |

## Lakebase (PostgreSQL)

The `deploy-lakebase/` directory contains tooling for managing a Databricks Lakebase instance used by the `pg_builtin` and `pg_jdbc` tests. It has its own `.env` configuration — see `deploy-lakebase/README.md` for full details.

```bash
cd deploy-lakebase
cp .env.sample .env
# Set LAKEBASE_PROJECT_NAME and DATABRICKS_PROFILE

./deploy.py deploy     # create instance and save connection credentials
./deploy.py refresh    # regenerate expired OAuth token (expires after 1 hour)
./deploy.py destroy    # tear down the instance

./test_lakebase.py     # load sample data and run queries
```

Connection details are saved to `deploy-lakebase/perftest-connection.json`, which the main CLI reads automatically when building PG test parameters.

## Cleanup

Delete old Databricks job runs to keep the workspace tidy:

```bash
./cleanup_runs.sh              # delete all but 5 most recent runs
./cleanup_runs.sh --keep 10    # keep 10 most recent
./cleanup_runs.sh --all        # delete every run
./cleanup_runs.sh --dry-run    # preview what would be deleted
```

## Project structure

```
uc-perf-tests/
  perf_scripts/
    __init__.py
    config.py               # arg parsing, env loading, config derivation
    connections.py           # Neo4j, JDBC, Spark Connector, PG helpers
    reporting.py             # ValidationResults, timed() context manager
    queries.py               # centralized query definitions
    runner.py                # on-cluster entry point (--tests, --group, --exclude)
    cli.py                   # local CLI: upload, submit, run
    __main__.py              # allows `python -m perf_scripts`
    tests/
      __init__.py            # PerfTest base class, registry, TestContext
      test_hello.py
      test_connectivity.py
      test_direct_neo4j.py
      test_direct_jdbc.py
      test_remote_query.py
      test_delta.py
      test_spark_connector.py
      test_compound.py
      test_explain.py
      test_federated.py
      test_warmup.py
      test_pg_builtin.py
      test_pg_jdbc.py
  deploy-lakebase/
    deploy.py                # deploy, refresh credentials, or destroy a Lakebase instance
    test_lakebase.py         # load sample data and run queries against the instance
    data/                    # sample CSV data for test_lakebase.py
  cluster_utils.sh           # ensures cluster is running before submission
  cleanup_runs.sh            # deletes old Databricks job runs
  env.sample                 # configuration template
```
