# SafeSpark Investigation: UC JDBC Sandbox Performance

## Purpose

This document records the research and investigation into the ~12-second per-query overhead observed when using `remote_query()` through Unity Catalog JDBC connections. The diagnostic script (`run_07_performance_diagnostics.py`) isolated the overhead to the UC/SafeSpark sandbox layer. This investigation seeks to understand why.

## Investigation Summary

SafeSpark is the internal name for Databricks' JDBC driver sandbox, part of the broader Lakeguard isolation framework. It runs custom JDBC drivers in an isolated child JVM process with its own heap, metaspace, and container memory limits. Public documentation covers SafeSpark through its configuration parameters (`spark.databricks.safespark.*`) and Databricks' Lakeguard documentation. The internal architecture described in this document is inferred from those sources, observed behavior, and diagnostic measurements.

The ~12-second overhead is consistent with a per-query child JVM lifecycle that includes: sandbox process startup or connection acquisition, classloading of the shaded connector JAR, a mandatory schema inference round-trip (`WHERE 1=0`), IPC serialization between the Spark executor and the sandbox, and JDBC connection establishment. The warm-up test showed no improvement across repeated queries, suggesting the overhead is structural and per-invocation rather than a one-time initialization cost.

---

## Part 1: What Is SafeSpark?

### Relationship to Lakeguard

SafeSpark is the JDBC-specific component of Databricks' Lakeguard isolation framework. Lakeguard enforces code isolation and data filtering across shared compute environments.

Lakeguard has three layers:

1. **Spark Connect decoupling**: client applications and the Spark driver no longer share a JVM or classpath (Apache Spark 3.4+)
2. **Container sandboxing**: each client application runs in its own isolated container with OS-level boundaries
3. **UDF isolation on executors**: sandbox execution environment, network egress isolation, environment replication

SafeSpark applies layer 2 (container sandboxing) specifically to custom JDBC drivers loaded through UC connections. When a `CREATE CONNECTION TYPE JDBC` specifies `java_dependencies`, the referenced JAR is loaded and executed inside an isolated sandbox rather than in the Spark executor's JVM.

**Source:** Databricks Lakeguard documentation
https://docs.databricks.com/aws/en/compute/lakeguard

> "Lakeguard is a set of technologies on Databricks that enforce code isolation and data filtering so multiple users can share the same compute resource securely and cost-effectively, and access data with fine-grained access controls in place on compute offering privileged machine access."

### Why SafeSpark Exists

Third-party JDBC drivers are inherently risky. They execute arbitrary Java code inside the Spark process, with potential for remote code execution, data exfiltration, and privilege escalation. Databricks has documented specific vulnerabilities in JDBC drivers:

- SQLite JDBC: Remote code execution via deserialization
- Hive JDBC: Command injection
- Redshift JDBC: File append attacks

SafeSpark isolates these drivers so that a compromised or malicious JDBC JAR cannot access other users' data, the underlying machine, or the Spark engine itself.

**Source:** Databricks blog, "Restricting Libraries in JVM Compute Platforms" (August 23, 2022)
https://www.databricks.com/blog/2022/08/23/restricting-libraries-in-jvm-compute-platforms.html

This blog post documents the evolution from Java SecurityManager-based isolation to OS-level sandboxing:

> "The Java team decided to deprecate the SecurityManager in JDK17 and eventually consider removing it. This change will affect the proposal in this blog post."

> "Another alternative approach is to use operating system sandboxing primitives for similar results. For example, on Linux we can use namespaces and seccomp-bpf to limit resource access. However, this approach requires significant changes in existing applications and may impact performance."

The Lakeguard blog post (April 2024) confirms Databricks chose OS-level sandboxing:

> "To enforce data governance at the compute level, we evolved our compute architecture from a security model where users share a JVM to a model where each user's code runs in full isolation from each other and the underlying Spark engine so that data governance is always enforced."

**Source:** Databricks blog, "Unity Catalog Lakeguard: Industry-first and only data governance for multi-user Apache Spark clusters" (April 25, 2024)
https://www.databricks.com/blog/unity-catalog-lakeguard-industry-first-and-only-data-governance-multi-user-apachetm-spark

---

## Part 2: SafeSpark Architecture (Inferred)

The following is inferred from configuration parameters, observed behavior, and Databricks documentation.

### Separate Child JVM Process

The SafeSpark JDBC sandbox runs as a **separate child JVM process** with its own isolated runtime. Evidence:

- It has its own **metaspace** allocation (`spark.databricks.safespark.jdbcSandbox.jvm.maxMetaspace.mib`). Metaspace is a JVM-level concept; classloader isolation within a single JVM shares metaspace.
- It has its own **heap** (`spark.databricks.safespark.jdbcSandbox.jvm.xmx.mib`). The `-Xmx` flag is a JVM startup parameter.
- It has an overall **container size** constraint (`spark.databricks.safespark.jdbcSandbox.size.default.mib`), suggesting a cgroup or container memory limit around the child process.
- When the sandbox exhausts memory, the **connection is killed** with "Connection was closed before the operation completed." This is consistent with a child process crash (OOM killer), not an in-process OutOfMemoryError.

### Known Configuration Parameters

| Parameter | Default | Purpose |
|-----------|---------|---------|
| `spark.databricks.safespark.jdbcSandbox.jvm.maxMetaspace.mib` | Unknown (low) | JVM metaspace for classloading in sandbox |
| `spark.databricks.safespark.jdbcSandbox.jvm.xmx.mib` | Unknown (low) | Maximum heap for sandbox JVM |
| `spark.databricks.safespark.jdbcSandbox.size.default.mib` | ~400 | Overall container memory limit |

The JDBC connection documentation states an overall memory limit of 400 MiB for JDBC drivers.

**Source:** Databricks JDBC connection documentation
https://docs.databricks.com/aws/en/connect/jdbc-connection

> "The connection enables you to specify and install the JDBC driver in an isolated sandbox accessible by Spark compute to ensure Spark security and Unity Catalog governance."

> "The memory limit for the JDBC driver is 400 MiB. Consider using a smaller fetchSize if the limit is reached."

Our cluster configuration uses:
```
spark.databricks.safespark.jdbcSandbox.jvm.maxMetaspace.mib 128
```

The parameters above are those found in public sources. Additional parameters controlling sandbox lifecycle, connection pooling, GC tuning, or JVM startup flags may exist.

### IPC Mechanism

Communication between the Spark executor and the sandbox JVM likely uses socket-based IPC or a lightweight RPC protocol. All JDBC calls (connection creation, statement execution, result set iteration) must cross this process boundary, with serialization/deserialization at each step.

---

## Part 3: The Schema Inference Round-Trip

### How Spark Infers JDBC Schema

When Spark reads from a JDBC source using the `query` option (or when `remote_query()` executes), it first sends a schema inference query to determine column names and types without fetching data. This is a core Spark JDBC mechanism, not a Databricks addition.

Spark wraps the user query as a subquery and appends `WHERE 1=0`:

```sql
SELECT * FROM (<user_query>) SPARK_GEN_SUBQ_0 WHERE 1=0
```

**Source:** Apache Spark source, `JDBCOptions.scala`
https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/jdbc/JDBCOptions.scala

```scala
// Line ~103
s"(${subquery.trim.replaceAll(";+$", "")}) SPARK_GEN_SUBQ_${curId.getAndIncrement()}"
```

**Source:** Apache Spark source, `JdbcDialects.scala`
https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/jdbc/JdbcDialects.scala

```scala
// Lines 295-310
def getSchemaQuery(table: String): String = {
  s"SELECT * FROM $table WHERE 1=0"
}
```

**Source:** Apache Spark source, `JDBCRDD.scala`
https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/jdbc/JDBCRDD.scala

```scala
// Lines 62-74: resolveTable calls getSchemaQuery
val fullQuery = prepareQuery + dialect.getSchemaQuery(table)
getQueryOutputSchema(fullQuery, options, dialect, conn)
```

### Implications for remote_query()

The `remote_query()` documentation confirms that schema inspection happens on every call:

> "The query you provide is executed directly on the remote database. Databricks validates that the query is read-only by performing schema inspection, but syntax and semantic validation are performed by the remote database."

**Source:** Databricks remote_query() documentation
https://docs.databricks.com/aws/en/query-federation/remote-queries

This means every `remote_query()` call sends **two** queries through the SafeSpark sandbox:
1. The schema inference query: `SELECT * FROM (<user_query>) SPARK_GEN_SUBQ_0 WHERE 1=0`
2. The actual data query

Both queries traverse the full pipeline: Spark executor -> IPC -> sandbox JVM -> JDBC driver -> SQL-to-Cypher translation -> Neo4j Bolt connection -> response -> IPC back.

### SparkSubqueryCleaningTranslator

The Neo4j UC connector includes a `SparkSubqueryCleaningTranslator` (highest precedence in the SPI translator chain) specifically to strip this wrapping. It detects the `SPARK_GEN_SUBQ_N WHERE 1=0` pattern and extracts the inner query before passing it to the SQL-to-Cypher translator.

This cleaning is necessary because Neo4j's Cypher engine cannot parse SQL subquery wrappers. The translator handles simple queries well but has documented issues with complex SQL (GROUP BY, HAVING, ORDER BY) where the cleaning interacts badly with the inner query structure.

---

## Part 4: Probable Sources of the 12-Second Overhead

Based on the architecture analysis and diagnostic measurements, the ~12-second per-query overhead likely comes from multiple compounding factors:

### A. Sandbox JVM Lifecycle (estimated 3-8 seconds)

If the sandbox JVM is started per-query or per-connection:
- JVM startup alone is 1-3 seconds
- Classloading of the Neo4j connector (a 15+ MB shaded fat JAR) requires loading thousands of classes, consuming metaspace
- ServiceLoader discovery of translator factories (`META-INF/services`) adds to classloading
- Translator pipeline initialization (SparkSubqueryCleaningTranslator + SqlToCypherTranslator)

The warm-up test (Test 8) showed:
```
Run 1: 12.334s
Run 2: 11.927s
Run 3: 12.046s
Run 4: 12.130s
Run 5: 12.004s
Warm-up penalty: +0.308s
```

The negligible warm-up penalty suggests either: (a) the sandbox JVM persists across queries but pays full per-query overhead for other reasons, or (b) the JVM restarts per-query but is pre-warmed by the cluster runtime.

### B. Schema Inference Round-Trip (estimated 2-4 seconds)

Each `remote_query()` call sends the `WHERE 1=0` schema query before the real query. Through the SafeSpark sandbox, this means:
1. Spark executor -> IPC to sandbox
2. Sandbox JVM -> JDBC connection to Neo4j
3. SparkSubqueryCleaningTranslator strips wrapping
4. SQL-to-Cypher translation
5. Cypher execution on Neo4j (fast: <50ms)
6. Result metadata -> IPC back to Spark

This doubles the number of JDBC round-trips per `remote_query()` call.

### C. IPC Serialization (estimated 1-2 seconds)

All JDBC operations are proxied across the sandbox process boundary:
- `Connection` creation
- `Statement` preparation and execution
- `ResultSet` metadata and row iteration
- `ResultSet` data serialization from sandbox JVM to Spark executor JVM

For schema inference, the `ResultSetMetaData` is serialized. For data queries, the actual row data is serialized. Even for a single-row `COUNT(*)` result, the full IPC ceremony occurs.

### D. Connection Establishment (estimated 1-2 seconds)

If connections are not pooled within the sandbox:
- JDBC connection creation triggers Neo4j Bolt protocol handshake
- TLS negotiation with Neo4j Aura (certificate verification, cipher suite selection)
- The Neo4j JDBC driver with `enableSQLTranslation=true` initializes the translator pipeline on each new connection
- This happens twice per query (once for schema inference, once for data) if connections aren't reused

### Supporting Evidence: Direct JDBC Timing

The direct JDBC test (Test 1b) completes the same queries in 0.3-0.5 seconds. This path uses `spark.read.format("jdbc")` with the Neo4j JDBC driver directly on the executor (no SafeSpark). The driver's JAR is loaded as a cluster library in the executor's own JVM. The 0.3-0.5s includes Spark DataFrame overhead, JDBC connection, SQL-to-Cypher translation, and Neo4j round-trip. This confirms the JDBC driver performs well when loaded directly on the executor.

---

## Part 5: Community Confirmation

### Databricks Community Forum

A thread on the Databricks community forum reports the same class of issue with SQL Server federation:

**Source:** "Performance Issue with UC Read from Federated SQL Table vs JDBC Read from SQL Server"
https://community.databricks.com/t5/data-engineering/performance-issue-with-uc-read-from-federated-sql-table-vs-jdbc/td-p/84075

User "Direo" (August 23, 2024):
> "I'm currently facing a significant performance issue when comparing the execution times of a query sent through JDBC versus a similar query executed through Databricks SQL (using Unity Catalog to access a federated SQL table)."
> "Execution Time: ~2 seconds" [for JDBC]
> "Execution Time: 6-7 minutes" [for Databricks SQL / UC]

User "CharlesRColas" (January 24, 2025):
> "I'm facing the exact same issue with the exact same amount of time that seems to be of waste. When running multiple federated queries in a job, the additional overhead begins to add up and makes the functionality cost prohibitive."

User "pdiamond" (February 11, 2025):
> "I've found the JDBC query to be faster than the federated query because in our testing, the federated query does not pass down the full query to the source database. Instead, it's running 'select * from table', pulling all of the data into Databricks and then filtering it before displaying/returning to the notebook."

The third user's observation about full-table scans is a distinct issue from the constant per-query overhead measured here, but the thread confirms that per-query overhead in UC federation is a recognized concern across different source systems and configurations.

### Databricks Performance Recommendations

The Lakehouse Federation performance documentation covers these query optimization patterns:

**Source:** Lakehouse Federation performance recommendations
https://docs.databricks.com/aws/en/query-federation/performance-recommendations

Key recommendations:
- Use `AND` to combine pushable and non-pushable predicates
- Set `fetchSize` to a large value (e.g., 100,000)
- Use partition parameters for parallel reads
- Join pushdown (Public Preview in DBR 17.2+) pushes entire joins to the remote database

The documentation focuses on query pushdown and parallel read optimization; SafeSpark-specific tuning is outside its scope.

---

## Part 6: Diagnostic Evidence

### Three-Layer Timing Comparison (from run_07)

```
                                   Python  JDBC+Sp   UC r_q  JDBC oh    UC oh
  MaintenanceEvent count           0.234s   0.502s  13.842s  +0.269s +13.339s
  Critical events                  0.029s   0.397s  12.539s  +0.368s +12.141s
  Flight count                     0.016s   0.342s  12.077s  +0.326s +11.735s
  Flight->Airport traversal        0.019s   0.422s  12.286s  +0.403s +11.865s
  Aircraft count                   0.012s   0.368s  12.104s  +0.356s +11.736s
  TOTAL                            0.309s   2.031s  62.847s  +1.722s +60.816s
```

**JDBC overhead (Spark DataFrame layer, no UC):** +1.722s total across 5 queries (~0.3s each)
**UC/SafeSpark overhead:** +60.816s total across 5 queries (~12s each)

The overhead is constant regardless of query complexity. A `COUNT(*)` on 20 Aircraft rows takes the same 12 seconds as a `COUNT(*)` on 800 Flight-Airport traversals.

### Warm-up Test

```
  Run 1: 12.334s  <-- cold start
  Run 2: 11.927s
  Run 3: 12.046s
  Run 4: 12.130s
  Run 5: 12.004s
  Warm-up penalty: +0.308s
```

No meaningful warm-up effect. The overhead is per-query, not per-session.

### Spark Explain Plan

The Spark physical plan for a simple `remote_query()` is minimal:
```
*(1) Scan JDBC v1 Relation from v2 scan remote_query('sample_neo4j_jdbc_connection')
     [cnt#24760L] PushedFilters: [], ReadSchema: struct<cnt:bigint>
```

The plan is a single JDBC scan with no additional stages, shuffles, or broadcasts. The overhead is below the plan level, in the JDBC execution layer.

---

## Part 7: PostgreSQL Comparison Test Plan

### Rationale

To conclusively determine whether the overhead is SafeSpark itself or something specific to the Neo4j connector JAR, we need a controlled comparison using a different JDBC driver through the same SafeSpark sandbox.

PostgreSQL is ideal because:
1. It is a **natively supported** Databricks federation target (built-in connector, TYPE postgresql)
2. It can also be configured as a **custom JDBC connection** (TYPE JDBC with java_dependencies)
3. The PostgreSQL JDBC driver is small (~1.1 MB) vs the Neo4j shaded JAR (~15+ MB)

### Test Matrix

| # | Path | SafeSpark? | What It Isolates |
|---|------|-----------|-----------------|
| A | PostgreSQL via built-in connector (`TYPE postgresql`) | No | Baseline: federation overhead without SafeSpark |
| B | PostgreSQL via custom JDBC JAR (`TYPE JDBC`) | Yes | SafeSpark overhead with a lightweight driver |
| C | Neo4j via custom JDBC JAR (`TYPE JDBC`) | Yes | SafeSpark overhead with a heavy shaded JAR |
| D | Neo4j direct JDBC (cluster library, no UC) | No | JDBC driver overhead without SafeSpark |

**Expected outcomes:**

- If **B ~= C ~= 12s**: SafeSpark sandbox overhead is the bottleneck regardless of driver. The Neo4j connector JAR size is not a factor.
- If **B << C**: The Neo4j connector's shaded JAR size or classloading complexity adds significant overhead within SafeSpark. Optimizing the JAR (reducing dependencies, lazy-loading translators) could help.
- If **A << B**: The built-in connector bypasses SafeSpark entirely, confirming that native federation avoids the sandbox penalty.
- If **A ~= B**: SafeSpark is not the issue; the overhead is in the UC federation layer itself (query routing, governance checks, etc.).

### PostgreSQL Setup

**Driver:** PostgreSQL JDBC 42.7.10
- JAR: `postgresql-42.7.10.jar` (~1.1 MB)
- Maven: `org.postgresql:postgresql:42.7.10`
- Download: https://jdbc.postgresql.org/download/postgresql-42.7.10.jar
- Also: https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.10/postgresql-42.7.10.jar
- Driver class: `org.postgresql.Driver`

**Azure PostgreSQL JDBC URL format:**
```
jdbc:postgresql://<server>.postgres.database.azure.com:5432/<database>?sslmode=require
```

**Test A: Built-in connector (no SafeSpark):**
```sql
CREATE CONNECTION pg_builtin TYPE postgresql
OPTIONS (
  host '<server>.postgres.database.azure.com',
  port '5432',
  user '<username>',
  password '<password>'
);

CREATE FOREIGN CATALOG pg_catalog USING CONNECTION pg_builtin
OPTIONS (database '<database>');

-- Then query via: SELECT COUNT(*) FROM pg_catalog.public.test_table
```

**Test B: Custom JDBC through SafeSpark:**
```sql
CREATE CONNECTION pg_jdbc TYPE JDBC
ENVIRONMENT (
  java_dependencies '["/Volumes/<catalog>/<schema>/jars/postgresql-42.7.10.jar"]'
)
OPTIONS (
  url 'jdbc:postgresql://<server>.postgres.database.azure.com:5432/<database>?sslmode=require',
  user '<username>',
  password '<password>',
  driver 'org.postgresql.Driver'
);

-- Then query via: SELECT * FROM remote_query('pg_jdbc', query => 'SELECT COUNT(*) AS cnt FROM test_table')
```

### Implementation

The PostgreSQL comparison tests will be added to `run_07_performance_diagnostics.py` as optional tests that run only when PostgreSQL configuration is provided (via new `--pg-*` arguments). The test creates a simple table in PostgreSQL if it doesn't exist, runs `SELECT 1` and `SELECT COUNT(*)` through all four paths, and includes the results in the performance report.

New `.env` variables:
```
PG_HOST=<server>.postgres.database.azure.com
PG_PORT=5432
PG_DATABASE=<database>
PG_USERNAME=<username>
PG_PASSWORD=<password>
PG_JDBC_JAR_PATH=/Volumes/<catalog>/<schema>/jars/postgresql-42.7.10.jar
PG_UC_BUILTIN_CONNECTION=pg_builtin
PG_UC_JDBC_CONNECTION=pg_jdbc
```

---

## Part 8: Open Questions

1. **Sandbox lifecycle:** Is the SafeSpark JVM started per-query, per-connection, or per-cluster-session? The warm-up test suggests per-query overhead is constant, but doesn't conclusively answer whether the JVM restarts.

2. **Undocumented parameters:** Are there `spark.databricks.safespark.*` parameters for controlling JVM reuse, connection pooling, or GC behavior within the sandbox? Databricks support may know.

3. **Schema inference caching:** Can the schema inference (`WHERE 1=0`) round-trip be cached or skipped for repeated queries to the same connection with the same output schema? The `customSchema` option skips inference for the DataFrame API, but `remote_query()` may always perform inspection.

4. **Built-in vs custom driver isolation:** Do Databricks' built-in federation connectors (PostgreSQL, MySQL, SQL Server) bypass SafeSpark entirely, or do they use a faster/lighter version of the sandbox? The PostgreSQL comparison test will answer this.

5. **Sandbox memory tuning:** Does increasing `spark.databricks.safespark.jdbcSandbox.size.default.mib` beyond 512 reduce per-query overhead, or is the overhead structural (IPC, process communication) rather than memory-related?

---

## References

| # | Source | URL |
|---|--------|-----|
| 1 | Databricks Lakeguard documentation | https://docs.databricks.com/aws/en/compute/lakeguard |
| 2 | Databricks JDBC connection documentation | https://docs.databricks.com/aws/en/connect/jdbc-connection |
| 3 | Databricks federation performance recommendations | https://docs.databricks.com/aws/en/query-federation/performance-recommendations |
| 4 | Databricks remote_query() guide | https://docs.databricks.com/aws/en/query-federation/remote-queries |
| 5 | Databricks remote_query() SQL reference | https://docs.databricks.com/aws/en/sql/language-manual/functions/remote_query |
| 6 | Databricks blog: Lakeguard (Apr 2024) | https://www.databricks.com/blog/unity-catalog-lakeguard-industry-first-and-only-data-governance-multi-user-apachetm-spark |
| 7 | Databricks blog: JVM library restriction (Aug 2022) | https://www.databricks.com/blog/2022/08/23/restricting-libraries-in-jvm-compute-platforms.html |
| 8 | Community forum: federation performance issue | https://community.databricks.com/t5/data-engineering/performance-issue-with-uc-read-from-federated-sql-table-vs-jdbc/td-p/84075 |
| 9 | Apache Spark: JDBCOptions.scala (SPARK_GEN_SUBQ) | https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/jdbc/JDBCOptions.scala |
| 10 | Apache Spark: JdbcDialects.scala (WHERE 1=0) | https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/jdbc/JdbcDialects.scala |
| 11 | Apache Spark: JDBCRDD.scala (resolveTable) | https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/jdbc/JDBCRDD.scala |
| 12 | PostgreSQL JDBC driver downloads | https://jdbc.postgresql.org/download/ |
| 13 | PostgreSQL JDBC on Maven Central | https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.10/ |
