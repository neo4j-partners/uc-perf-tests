# validate-federation: Performance Diagnostics Findings

## Environment

- **Databricks Runtime:** 4.1.0 (Spark 4.1.0, Python 3.12.3)
- **Connector JAR:** neo4j-unity-catalog-connector-1.3.1.jar
- **Neo4j Host:** e3790fca.databases.neo4j.io (Aura)
- **PostgreSQL:** Databricks Lakebase (ep-fancy-hat-e91ivpop.database.eastus.azuredatabricks.net)
- **UC Connection:** sample_neo4j_jdbc_connection
- **Workspace:** Azure (adb-1098933906466604)
- **Cluster:** Single-node, `safespark.jdbcSandbox.jvm.maxMetaspace.mib 128`
- **Date:** 2026-04-03
- **Script:** run_07_performance_diagnostics.py (45/45 PASSED)

## Executive Summary

The `remote_query()` path through Unity Catalog adds 12-16 seconds of overhead per query when using custom JDBC drivers. This overhead is in the SafeSpark sandbox, not the JDBC driver or `remote_query()` function itself. We confirmed this by testing a second database (Databricks Lakebase/PostgreSQL) through three paths: built-in connector, built-in `remote_query()`, and custom JDBC through SafeSpark.

The critical finding: `remote_query()` through the **built-in** PostgreSQL connection completes `SELECT 1` in 0.752 seconds. The same `remote_query()` through a **custom JDBC** PostgreSQL connection takes 10.434 seconds. This proves that `remote_query()` itself is fast — the 14x overhead comes specifically from the SafeSpark sandbox that isolates custom JDBC drivers.

A secondary finding: `remote_query()` silently returns incorrect results for SQL queries containing WHERE clauses with string literals.

## Finding 1: SafeSpark Sandbox Is the Bottleneck (Confirmed via Cross-Driver Test)

### The Four-Layer Test

The diagnostic script runs the same queries through four progressively deeper layers of the Databricks stack:

| Layer | What It Tests | Per-Query Time |
|-------|--------------|----------------|
| **Direct Python driver** | Neo4j round-trip over Bolt, no Spark | 0.006 - 0.219s |
| **Direct JDBC via Spark** | Spark DataFrame reader + Neo4j JDBC driver, no UC | 0.35 - 0.61s |
| **remote_query() via UC** | Spark SQL + UC + SafeSpark sandbox + JDBC | 12.9 - 16.1s |
| **PG built-in remote_query()** | Same `remote_query()` function, no SafeSpark | 0.66 - 0.75s |
| **PG custom JDBC via UC** | Same SafeSpark sandbox, different driver (1.1 MB JAR) | 8.5 - 10.4s |

### Where the Time Goes

```
  Query                            Python  JDBC+Sp   UC r_q  JDBC oh    UC oh
  MaintenanceEvent count           0.219s   0.610s  16.139s  +0.391s +15.529s
  Critical events                  0.010s   0.393s  13.641s  +0.383s +13.248s
  Flight count                     0.008s   0.366s  12.879s  +0.358s +12.513s
  Flight->Airport traversal        0.008s   0.401s  13.337s  +0.393s +12.936s
  Aircraft count                   0.006s   0.346s  12.866s  +0.340s +12.520s
  TOTAL                            0.251s   2.116s  68.863s  +1.865s +66.747s
```

The Spark JDBC layer (no UC, no SafeSpark) adds ~0.4s per query. The UC/SafeSpark layer adds ~13s per query on top of that.

### PostgreSQL Confirms SafeSpark Is the Bottleneck

The critical experiment: run `SELECT 1` against the same PostgreSQL database (Databricks Lakebase) through three different connection types to isolate exactly where the overhead originates.

**Test setup:** The diagnostic script creates two UC connections to the same Lakebase instance:

1. **Built-in connection** (`CREATE CONNECTION pg_perf_builtin TYPE postgresql`) — Databricks recognizes PostgreSQL as a natively supported database. The driver ships as part of the Databricks Runtime, is already loaded in the executor JVM's classpath, and is treated as trusted code. Queries through this connection run in-process with no sandbox isolation.

2. **Custom JDBC connection** (`CREATE CONNECTION pg_perf_jdbc TYPE JDBC`) — Uses a standard PostgreSQL 42.7.10 JAR (1.1 MB) uploaded to a UC Volume and referenced via `java_dependencies`. Despite being the same open-source PostgreSQL driver that Databricks bundles internally, the `TYPE JDBC` declaration marks it as untrusted third-party code. Databricks runs it inside SafeSpark: a separate child JVM process with OS-level sandboxing (Linux namespaces, seccomp-bpf). Each query pays the cost of spawning this sandbox, classloading the JAR, executing, and tearing down.

Both connections point to the same host, port, database, and credentials. The only variable is the code path through Databricks.

| Path | Connection Type | SafeSpark? | Time |
|------|----------------|-----------|------|
| **Built-in foreign catalog** | `TYPE postgresql` (pg_perf_builtin) | No | **0.100s** |
| **Built-in `remote_query()`** | `TYPE postgresql` (pg_perf_builtin) | No | **0.752s** |
| **Custom JDBC `remote_query()`** | `TYPE JDBC` + postgresql-42.7.10.jar (pg_perf_jdbc) | Yes | **10.434s** |
| **Neo4j UC `remote_query()`** | `TYPE JDBC` + neo4j-uc-connector-1.3.1.jar | Yes | **16.139s** |

Key conclusions:
- **`remote_query()` itself is fast.** Through the built-in PostgreSQL connection, `remote_query()` completes in 0.752s. The function is not the bottleneck.
- **SafeSpark adds ~10s minimum** regardless of which JDBC driver is used. The same `remote_query()` call to the same database through the custom JDBC PostgreSQL driver takes 10.4s — a 14x overhead from the SafeSpark sandbox alone.
- **The Neo4j JAR's size adds ~5-6s extra** over the PostgreSQL JAR (10.4s vs 16.1s), likely from classloading the 15+ MB shaded fat JAR and initializing the SQL-to-Cypher translator pipeline inside the sandbox.
- **Built-in connectors bypass SafeSpark entirely.** Databricks ships and trusts its own PostgreSQL driver, so it runs in-process without sandbox isolation. The same Lakebase instance responds in 0.1s through the foreign catalog and 0.75s through `remote_query()`. Custom JDBC SafeSpark adds a 100x overhead for the identical query against the identical database.

### Warm-up Effect

```
  Run 1: 12.711s  <-- cold start
  Run 2: 12.559s
  Run 3: 12.579s
  Run 4: 12.868s
  Run 5: 13.366s
  Warm-up penalty: -0.132s
```

No meaningful warm-up effect. The overhead is per-query, not per-session.

### Memory Is Not the Bottleneck

Increasing SafeSpark sandbox memory by 10x (metaspace 128→1280, heap 300→3000, container 512→5120 MiB) made performance **worse**, not better — queries took 22-26 seconds per call compared to the ~12s baseline. The overhead is structural (sandbox JVM lifecycle, IPC serialization, schema inference round-trips), not memory-constrained. Throwing more resources at the sandbox does not help.

## Finding 2: `remote_query()` WHERE Clause with String Literals Returns Incorrect Results

The same `WHERE severity = 'CRITICAL'` filter returns different results depending on the query path:

| Path | Query | Result |
|------|-------|--------|
| Python driver | `MATCH (m:MaintenanceEvent {severity: 'CRITICAL'}) RETURN COUNT(m) AS cnt` | **101** |
| Direct JDBC via Spark | `SELECT COUNT(*) AS cnt FROM MaintenanceEvent WHERE severity = 'CRITICAL'` | **101** |
| remote_query() via UC | `SELECT COUNT(*) AS cnt FROM MaintenanceEvent WHERE severity = 'CRITICAL'` | **0** |

The Python driver and direct JDBC both return 101, confirming that 101 critical events exist and that the Neo4j JDBC driver's SQL-to-Cypher translator handles the WHERE clause correctly. Only the `remote_query()` path returns 0.

This is related to the LIKE quoting issue documented in the validate-federation test suite: `''AC%''` was stripped to `AC%` (without quotes) at the wrong level. The WHERE clause issue is the same class of bug — string literals are being malformed as they pass through the SafeSpark sandbox, causing silent incorrect results rather than syntax errors.

## Finding 3: Spark Connector and Delta Performance Are Not Bottlenecks

### Spark Connector Performance

| Label | Schema Inference (.load) | Data Pull (.count) | Total | Rows |
|-------|------------------------|--------------------|-------|------|
| MaintenanceEvent | 0.186s | 0.307s | 0.493s | 300 |
| Flight | 0.052s | 0.181s | 0.233s | 800 |
| Aircraft | 0.063s | 0.229s | 0.292s | 20 |

The Spark Connector runs inside the executor JVM without SafeSpark isolation, which is why it is 30-60x faster than `remote_query()` for equivalent data.

### Delta Lakehouse Performance

| Query | Time |
|-------|------|
| sensor_readings COUNT (345,600 rows) | 1.045s |
| aircraft COUNT (20 rows) | 0.593s |
| sensors COUNT (160 rows) | 0.497s |
| systems COUNT (80 rows) | 0.543s |
| Sensor avg with JOIN (full scan) | 1.438s |
| Sensor health GROUP BY with 3-way JOIN | 1.614s |

Delta queries are not a performance concern. All queries complete under 2 seconds on a warm cluster.

### Federated Query Breakdown

| Step | Time | % of Total |
|------|------|-----------|
| Spark Connector .load() | 0.187s | 4% |
| Temp view + count | 0.212s | 4% |
| Delta sensor aggregation | 1.757s | 35% |
| Maintenance aggregation from temp view | 0.526s | 10% |
| Final 3-way JOIN + collect | 2.397s | 47% |
| **Total** | **5.079s** | |

The federated query pattern (Spark Connector into temp views, then JOIN with Delta) performs well because it avoids `remote_query()` entirely.

## Finding 4: Built-in Connectors Use a Fundamentally Faster Path

Databricks ships built-in federation connectors for PostgreSQL, MySQL, and SQL Server. These connectors are declared with `TYPE postgresql`, `TYPE mysql`, or `TYPE sqlserver` (rather than `TYPE JDBC`) and use drivers that Databricks bundles, vets, and trusts as part of the runtime. Because they are trusted code, they run inside the Spark executor JVM without SafeSpark sandbox isolation — no child process, no classloading from a Volume, no OS-level namespace isolation.

The PostgreSQL test confirms this by querying the same Lakebase instance through both the built-in and custom JDBC paths:

| Test | Connection Type | SafeSpark? | Time |
|------|----------------|-----------|------|
| `SELECT 1` via foreign catalog | `TYPE postgresql` (built-in) | No | **0.100s** |
| `SELECT 1` via `remote_query()` | `TYPE postgresql` (built-in) | No | **0.752s** |
| `COUNT(*)` via `remote_query()` | `TYPE postgresql` (built-in) | No | **0.664s** |
| `COUNT(*)` on information_schema | `TYPE postgresql` (built-in) | No | **3.075s** |
| `SELECT 1` via `remote_query()` | `TYPE JDBC` + PG JAR (custom) | Yes | **10.434s** |
| `COUNT(*)` via `remote_query()` | `TYPE JDBC` + PG JAR (custom) | Yes | **8.515s** |

The built-in connection supports two access patterns: foreign catalogs (which expose the remote database's schema as a browsable Unity Catalog namespace) and `remote_query()` (which sends arbitrary SQL). Both are fast because they bypass SafeSpark.

Neo4j is not a natively supported federation target, so it must use `TYPE JDBC` with a custom driver JAR uploaded to a UC Volume. Any `TYPE JDBC` connection — regardless of the driver — goes through SafeSpark. The only way to reach sub-second `remote_query()` performance for Neo4j is for Databricks to add it as a built-in connector type or to provide an ISV path for registering trusted third-party drivers outside the sandbox.

## Recommended Next Steps

### 1. Reduce the Neo4j Connector JAR Size

The PostgreSQL JAR (1.1 MB) takes ~10s through SafeSpark; the Neo4j shaded JAR (15+ MB) takes ~16s. The ~5-6s difference is likely classloading overhead for the larger JAR and its relocated dependencies. To reduce this:

- Evaluate whether the shaded JAR can be split: a minimal JDBC-only JAR for SafeSpark use, and the full shaded JAR for cluster library use
- Profile which classes are loaded during a `remote_query()` call and whether lazy-loading translators could reduce startup time
- Consider whether the ServiceLoader discovery of translator factories (`META-INF/services`) adds measurable overhead in the sandbox

### 2. Reproduce and File the WHERE String Literal Bug

The silent incorrect results for `WHERE severity = 'CRITICAL'` via `remote_query()` is a correctness bug. To isolate:

- Test with `read_neo4j_jdbc()` (DataFrame API with UC connection) to determine if the bug is in `remote_query()` specifically or in the UC JDBC bridge
- Test with integer WHERE clauses (`WHERE count > 10`) to confirm the issue is specific to string literals
- Test whether the same quoting issue occurs with the PostgreSQL custom JDBC connection through SafeSpark