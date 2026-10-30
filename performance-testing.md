# UC custom JDBC (SafeSpark) is slow on every call

We re-ran the tests in October 2026 on a classic cluster. A UC custom JDBC connection takes 20 to 45 seconds per query. The same query takes under 1 second through a direct Spark JDBC read to Postgres and about 4 seconds to Neo4j. The delay is on every call, not only the first one.

## 1. Neo4j

Same five queries, same data, same Neo4j instance. Each row is one query, in seconds.

| Query | Direct Python (Bolt) | Direct Spark JDBC | UC `remote_query()` through SafeSpark |
|---|---|---|---|
| MaintenanceEvent count | 0.745 | 4.207 | 45.138 |
| Critical events | 0.071 | 4.016 | 39.503 |
| Flight count | 0.069 | 3.946 | 36.422 |
| Flight to Airport traversal | 0.070 | 4.373 | 35.645 |
| Aircraft count | 0.071 | 3.885 | 38.976 |

All three paths return the same counts: 300, 101, 800, 800 and 20.

Run `407728827553864`, task `446187476316930`.

## 2. Postgres

We repeated the test with Postgres to rule out the Neo4j driver. The PG driver is 1.1 MB. Each query ran five times. The values are the average of runs 2 to 5, in seconds.

| Path | `SELECT 1` | `COUNT(*)` |
|---|---|---|
| Built-in `TYPE postgresql` federation | 0.700 | 0.890 |
| Direct Spark JDBC with the same PG driver | 0.842 | 0.461 |
| UC custom JDBC connection through SafeSpark | 24.713 | 24.123 |

The same driver and the same database are fast without SafeSpark and slow with it.

Run `221829741126737`, task `1043695638915985`.

## 3. A driver that does nothing

We built a JDBC driver that opens no connection and returns one row. It is in `trivial-jdbc-stub/`. We loaded it as a UC custom JDBC connection and ran `remote_query("SELECT 1")` five times.

| Run | Seconds |
|---|---|
| 1 | 55.697 |
| 2 | 22.972 |
| 3 | 20.223 |
| 4 | 23.531 |
| 5 | 20.715 |

The warm average is 21.860 seconds. The driver does no I/O, so this time is spent in the SafeSpark call path.

Run `1032214159193881`, task `91216401678314`.

## 4. This is not a cold start

- **Warm calls stay slow.** In section 3, runs 2 to 5 average 21.9 seconds. In section 2, they average about 24 seconds.
- **Neo4j shows no warm-up effect.** Run 1 took 36.1 seconds and runs 2 to 5 averaged 35.8 seconds, a difference of 0.3 seconds.
- **The cost is paid per call.** Four Neo4j queries run separately took 148.9 seconds in total. The same four queries in one CROSS JOIN query took 147.3 seconds.

## Setup

- **Compute:** Classic cluster `0918-064508-6ivjk53l`.
- **Workspace profile:** `aws-partner-rk`.
- **Connection type:** `TYPE JDBC` with the driver JAR loaded from a UC Volume through `java_dependencies`.
- **Reproduce:** `uv run python -m perf_scripts.cli submit --tests direct_neo4j,direct_jdbc,remote_query,compound,warmup`, then `--tests pg_builtin,pg_jdbc,pg_direct_jdbc`, then `--group trivial`. The setup is described in `README.md`.

## Serverless

Serverless Jobs and SQL Warehouse also fail inside SafeSpark with a gRPC `Channel shutdownNow` error. This is a separate issue. The details are in the earlier version of this file in git history.
