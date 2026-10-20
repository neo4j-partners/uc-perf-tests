# Trivial JDBC stub

A 9 KB JDBC driver that does no real work. It is a control for measuring the fixed cost of `remote_query()` through the SafeSpark JDBC sandbox, separate from the cost of any real driver.

Every query returns one row with one INTEGER column named `val` set to 1. Spark's schema probe (`SELECT * FROM (<query>) alias WHERE 1=0`) returns the same column metadata with zero rows. The driver class is `trivial.TrivialDriver` and it accepts URLs that start with `jdbc:trivial:`.

## Results

On cluster `0918-064508-6ivjk53l` (DBR 17.3 ML, single user, single node m5.large), eight consecutive `remote_query()` calls took 15.3 to 22.1 seconds, with a mean of 18.3 seconds. Run 1 was 4 to 6 seconds slower than the rest. Runs 2 to 8 show no downward trend, so the cost is paid on every call. A plain `spark.range(1).collect()` on the same cluster takes about 0.3 seconds.

The Neo4j connector 1.3.7 JAR takes 35 to 45 seconds per call on the same cluster. The stub accounts for roughly half of that as fixed SafeSpark cost.

## Build

Requires a JDK. Tested with JDK 25 and `--release 8`.

```bash
./build.sh
```

This writes `trivial-jdbc-stub.jar` next to the script. The built JAR is checked in.

## Use

Upload the JAR to a Unity Catalog volume, then create a connection:

```sql
CREATE CONNECTION trivial_jdbc_perf_test TYPE JDBC
ENVIRONMENT (java_dependencies '["/Volumes/<catalog>/<schema>/<volume>/trivial-jdbc-stub.jar"]')
OPTIONS (
  url 'jdbc:trivial:mem',
  user 'dummy',
  password 'dummy',
  driver 'trivial.TrivialDriver',
  externalOptionsAllowList 'dbtable,query,partitionColumn,lowerBound,upperBound,numPartitions,fetchSize,customSchema'
);

SELECT * FROM remote_query('trivial_jdbc_perf_test', query => 'SELECT 1 AS val');
```

Time repeated calls and compare them with the same query through a real connection. Drop the connection when done.

A copy of the JAR is in the workspace at `/Volumes/aircraft-graphrag/sample/sample-jdbc/trivial-jdbc-stub.jar`.
