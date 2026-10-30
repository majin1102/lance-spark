# Dataset-independent Spark fragment scans

This demo uses the existing `lance` DataSource V2 connector with
`experimental_primitive_scan=true`. It requires the matching demo `lance-core` Java/JNI build;
released artifacts do not contain `FragmentReadTask` yet.

Build and install `lance-core` from the matching Lance demo branch, including its JNI library.
This branch uses version `14.0.0-beta.8`; fetching a published artifact with that version does not
substitute for installing the demo build. Driver and executors must load the same demo artifacts.
The commands below use Java 17, which supports the Spark 3.5 and 4.0 modules used here. Set
`JAVA_HOME` to a Java 17 installation before building or running either module.

In a Spark shell configured with those jars, enter `:paste`, paste the following block, then
press Ctrl-D:

```scala
import java.nio.file.Files
import org.lance.spark.LanceSparkReadOptions
import org.lance.spark.utils.Utils

val path = Files.createTempDirectory("lance-primitive-demo-").resolve("table.lance").toString
spark.range(0, 8, 1, 2)
  .selectExpr("id", "CASE WHEN id % 3 = 0 THEN CAST(NULL AS BIGINT) ELSE id * 2 END AS value")
  .write.format("lance").save(path)

val dataset = Utils.openDatasetBuilder(LanceSparkReadOptions.from(path)).build()
try dataset.delete("id = 2") finally dataset.close()

val rows = spark.read.format("lance")
  .option("experimental_primitive_scan", "true")
  .option("batch_size", "2")
  .load(path)

assert(rows.rdd.getNumPartitions == 2)
assert(rows.count() == 7)
rows.filter("value IS NULL OR value >= 8").select("id", "value").orderBy("id").show()
rows.select("id", "_rowaddr").orderBy("id").show()
rows.filter("value IS NOT NULL").orderBy(org.apache.spark.sql.functions.desc("value"))
  .limit(2).explain(true)
```

The physical plan contains `LancePrimitiveScan` and Spark operators for filtering and sorting.
Each task owns only one fragment descriptor. The driver still opens the Dataset during planning.
The default reader remains available by omitting the option.

The integration test also captures task bytes, closes the Dataset, makes a newer commit, and
temporarily moves the fixture's `_versions` directory. It then dispatches the captured tasks
through Spark `local[2]` and verifies the original snapshot's rows and deletions. This checks task
serialization and manifest-independent execution. It is not a separate-JVM or cluster benchmark.

After installing the matching demo `lance-core` artifact, run from the repository root:

```sh
./mvnw test -pl lance-spark-3.5_2.13 -am \
  -Dlance.version=14.0.0-beta.8 \
  -Dtest=LancePrimitiveScanTest \
  -Dsurefire.failIfNoSpecifiedTests=false
```

To run the same four tests against Spark 4.0:

```sh
./mvnw test -pl lance-spark-4.0_2.13 -am \
  -Dlance.version=14.0.0-beta.8 \
  -Dtest=LancePrimitiveScanTest \
  -Dsurefire.failIfNoSpecifiedTests=false
```

The base module only compiles the abstract test fixture. The selected Spark module runs its
concrete test class; its Surefire report should show four tests with no failures or errors.

Only the temporary test fixture's manifests are moved. Production snapshot files must stay
reachable for the lifetime of all scheduled tasks.

## BTree segments followed by distributed fragment takes

The same test suite includes `btreeSegmentsShuffleAddressesToIndependentFragmentTasks`. It shows
explicit Spark RDD composition; automatic scalar-index planning in the connector is not implemented
by this demo. The ordinary `experimental_primitive_scan` option remains a fragment scan.

The driver captures BTree segment descriptors and fragment descriptors from the same snapshot,
then closes the Dataset. A segment spanning two fragments receives only its own index metadata.
Live fragment descriptors form a separate keyed RDD:

```text
segment tasks -> typed BTree query -> physical row addresses
                                      |
                               distinct + group by fragment ID
                                      |
live fragment task RDD -------------- join
                                      |
                               fragment take(sorted offsets)
                                      |
unindexed fragment scans ----------- union
                                      |
                            Spark filter + sort + global limit
```

The inner join excludes hits for fragments outside the chosen snapshot. Fragment readers apply
the captured deletion state. The unindexed-fragment branch prevents an older index from hiding
newly appended data. Spark applies the predicate to the combined stream, including the fallback
rows, before residual predicates and global operators.

The test builds the index before appending another fragment and deleting a matching indexed row.
It tests typed `x = 7` and `x IS NULL`, compares both complete and limited results with the Dataset
scanner, and runs both stages while the fixture's manifests are inaccessible:

```sh
./mvnw test -pl lance-spark-3.5_2.13 -am \
  -Dlance.version=14.0.0-beta.8 \
  '-Dtest=LancePrimitiveScanTest#btreeSegmentsShuffleAddressesToIndependentFragmentTasks' \
  -Dsurefire.failIfNoSpecifiedTests=false
```

This fixture deliberately uses physical row IDs and four rows per fragment. The test collects each
small task's output to keep resource ownership visible. Production adapters need bounded streaming
and take groups; this example does not claim a bounded-memory implementation for arbitrarily large
hit sets, stable-ID remapping, ANN support, or a distributed query benchmark.
