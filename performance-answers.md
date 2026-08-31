# JDBC Connections performance: current answer

- **Overview:** Classic compute now passes with the fixed Neo4j connector, while serverless Jobs and SQL Warehouse still fail inside SafeSpark.
- **Serverless UC:** Direct Python/Bolt works. UC JDBC DataFrame reads and `remote_query()` fail inside SafeSpark with gRPC `Channel shutdownNow` and keepalive errors.
- **UC DataFrame path:** Serverless Spark calls the JDBC DataFrame API with `databricks.connection`. Unity Catalog loads `1.3.7-local-test.jar` inside SafeSpark. The Neo4j JDBC driver then connects to Neo4j over Bolt.
- **UC DataFrame flow:** Serverless Spark → UC connection → SafeSpark → Neo4j JDBC driver → Neo4j Bolt endpoint.
- **Current conclusion:** The same fixed UC connection works on classic compute, so the remaining transport failure is specific to the serverless and SQL Warehouse SafeSpark execution environment.

## Latest live results

- **Fixed configuration:** The current UC connection uses `1.3.7-local-test.jar` and `timeout=30000`.
- **SQL Warehouse networking:** **Enable networking for isolated workloads in serverless SQL warehouses** is enabled.

| Path | Run or statement | Result | Timing or error |
|---|---|---|---|
| Classic UC, fixed-schema JDBC | `549708348775819` | Passed 7/7 | 144.410s |
| Classic UC, `remote_query()` | `549708348775819` | Passed | 90.638s |
| Classic UC, fixed-schema JDBC | `470501091459272` | Passed 7/7 | 154.353s |
| Classic UC, `remote_query()` | `470501091459272` | Passed | 87.728s |
| Classic UC warmup | `612990302058231` / task `897291985795599` | Passed | 65.756s, 52.251s, 58.714s, 54.764s, 56.274s |
| Classic warm average | Same run | Passed | 55.500s for calls 2–5; cold penalty +10.255s |
| Earlier classic baseline | `376682695044361` | Mixed | Direct Python 1.118s; direct JDBC 35.370s; UC 193.041s |
| Serverless direct Python/Bolt | `103299608079868` | Passed | First query ~0.74s; later queries ~0.07s |
| Serverless UC attempt 1 | `392554698316864` / task `354502971868908` | Failed 0/2 | 141s; DataFrame and `remote_query()` returned `Channel shutdownNow` |
| Serverless UC attempt 2 | `392554698316864` / task `741513816000228` | Failed 0/2 | 85s; same SafeSpark gRPC error |
| SQL Warehouse UC | `01f1a54a-56c2-144c-962e-78a5627dd4e2` | Failed | ~85s; `Channel shutdownNow` |

- **Earlier correctness warning:** The earlier classic UC result returned 0 critical events while both direct paths returned 101. Do not use that run as a correctness-valid latency comparison.

## What the results mean

| Finding | Meaning |
|---|---|
| Classic correctness | Two fixed-artifact runs passed all seven checks through both UC APIs. |
| Classic performance | Fixed-schema JDBC averaged 149.382s. `remote_query()` averaged 89.183s. |
| Warm performance | A reused classic SafeSpark path still averaged 55.500s per call. |
| Timeout | `timeout=30000` prevents the old 1,000ms failure. It does not make UC fast. |
| Serverless APIs | Both JDBC DataFrame reads and `remote_query()` fail. The error is below either Spark API. |
| Smoke query | The query has no string literals. SQL escaping does not cause this channel failure. |
| Direct connection | Python/Bolt succeeds on serverless. The Neo4j endpoint and credentials work outside SafeSpark. |
| Cross-compute result | The fixed UC connection passes on classic and fails on serverless and SQL Warehouse. The failure follows the SafeSpark environment. |
| Translator work | `SparkSubqueryCleaningTranslator` correctness work is separate from this transport failure. |

## Current cause and fix status

- **Fixed artifact:** JAR `1.3.7-local-test` is the tested connector artifact.
- **Fixed timeout:** The JDBC URL now uses `timeout=30000`.
- **Fixed classic path:** Classic UC reads pass repeatedly with the fixed artifact and timeout.
- **Open serverless path:** Serverless Jobs still lose the SafeSpark gRPC channel before either UC API returns a result.
- **Open SQL path:** SQL Warehouse shows the same failure after its isolated-workload networking preview was enabled.
- **Leading network cause:** Isolated-workload DNS, egress policy, private routing, firewall rules, or Aura IP filtering may block the SafeSpark sandbox path.
- **Leading sandbox cause:** SafeSpark may terminate the sandbox because of memory pressure, process failure, or another internal resource limit.
- **Memory limit:** Databricks documents a 400 MiB custom JDBC driver memory limit for the isolated sandbox.

## Next actions

- **Network check:** Verify DNS and outbound Bolt connectivity from the isolated serverless Jobs and SQL Warehouse environments, not only from direct Python code.
- **Support evidence:** Give Databricks Support serverless run `392554698316864`, task attempts `354502971868908` and `741513816000228`, and SQL statement `01f1a54a-56c2-144c-962e-78a5627dd4e2`.
- **Support request:** Ask for SafeSpark sandbox termination, memory-pressure, network, and gRPC channel logs.
- **Small retest:** Run one literal-free count query with the smallest practical connector artifact after isolated routing is verified.
- **Warm retest:** Run one cold call and at least 10 sequential warm calls after one serverless UC query succeeds.
- **Cross-compute retest:** Run the same query and artifact on classic isolated UC, serverless isolated UC, and SQL Warehouse UC.
- **Acceptance check:** Require equal counts and stable result hashes before accepting latency measurements.

## Performance comparison guidance

- **Primary comparison:** Compare direct JDBC and isolated UC JDBC with the same connector JAR, Neo4j data, query, schema, Spark action, and result check.
- **Classic direct:** Install the pinned JAR on dedicated compute and use Spark JDBC as the non-isolated baseline.
- **Classic isolated:** Use the UC JDBC connection on classic compute to measure SafeSpark overhead while keeping compute stable.
- **Serverless isolated:** Use the same UC connection on serverless compute to measure the managed-compute path.
- **SQL isolated:** Use `remote_query()` through SQL Warehouse as a separate SQL user-path measurement.
- **Python control:** Use the Neo4j Python driver only as a network and source-query floor because it bypasses Spark, JDBC, and SafeSpark.
- **API separation:** Report fixed-schema DataFrame reads and `remote_query()` separately because their setup and schema-resolution work differs.
- **Cold reporting:** Report the first call separately from warm calls.
- **Warm reporting:** Report at least 10 warm iterations with p50, p95, minimum, maximum, mean, and standard deviation.
- **Result sweep:** Test small and large result sets to separate fixed sandbox cost from transfer cost.
- **Concurrency sweep:** Test 1, 4, and 8 concurrent reads only after single-query correctness and stability pass.
- **Artifact record:** Record the JAR version, SHA-256, JDBC URL options, runtime, compute type, and test timestamp for every result.
- **Serverless interpretation:** Treat serverless timings as end-user managed-service latency because its worker hardware cannot be pinned like classic compute.

## Useful Databricks references

- **JDBC connections:** [Custom JDBC connection requirements and limits](https://docs.databricks.com/aws/en/connect/jdbc-connection).
- **Spark data sources:** [Spark data source connection guidance](https://docs.databricks.com/aws/en/connect/spark-data-sources).
- **Parallel reads:** [Lakehouse Federation performance recommendations](https://docs.databricks.com/aws/en/query-federation/performance-recommendations#enable-parallel-reads-for-jdbc-connectors).
