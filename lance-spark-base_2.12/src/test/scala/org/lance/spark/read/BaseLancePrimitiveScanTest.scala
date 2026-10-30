/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lance.spark.read

import org.apache.arrow.vector.{BigIntVector, IntVector, UInt8Vector, VectorSchemaRoot}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.types.{IntegerType, LongType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.io.TempDir
import org.lance.fragment.FragmentReadTask
import org.lance.index.{IndexOptions, IndexParams, IndexSegmentReadTask, IndexType}
import org.lance.index.scalar.ScalarIndexParams
import org.lance.spark.{LanceDataset, LanceRef, LanceRuntime, LanceSparkReadOptions}
import org.lance.spark.utils.Utils

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.nio.file.{Files, Path}
import java.util.{Collections, HashMap}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

abstract class BaseLancePrimitiveScanTest {
  @TempDir var tempDir: Path = _
  private var spark: SparkSession = _

  @BeforeEach
  def setup(): Unit = {
    spark = SparkSession.builder()
      .master("local[2]")
      .appName("lance-primitive-scan-test")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
  }

  @AfterEach
  def teardown(): Unit = {
    if (spark != null) spark.stop()
  }

  private def fixture(): String = {
    val uri = tempDir.resolve("table.lance").toString
    spark.range(0, 8, 1, 2)
      .selectExpr("id", "CASE WHEN id % 3 = 0 THEN CAST(NULL AS BIGINT) ELSE id * 2 END AS value")
      .write.format("lance").save(uri)
    val dataset = Utils.openDatasetBuilder(LanceSparkReadOptions.from(uri)).build()
    try {
      assertEquals(2, dataset.getFragments.size())
      dataset.delete("id = 2")
    } finally dataset.close()
    uri
  }

  private def read(uri: String, primitive: Boolean): DataFrame =
    spark.read.format("lance")
      .option(LanceSparkReadOptions.CONFIG_PRIMITIVE_SCAN, primitive.toString)
      .option(LanceSparkReadOptions.CONFIG_BATCH_SIZE, "2")
      .load(uri)

  @Test
  def connectorKeepsResidualOperatorsAndEmptyProjectionCorrect(): Unit = {
    val uri = fixture()
    val primitive = read(uri, primitive = true)
    val reference = read(uri, primitive = false)
    assertTrue(primitive.queryExecution.executedPlan.toString.contains("LancePrimitiveScan"))
    assertEquals(2, primitive.rdd.getNumPartitions)

    val queries: Seq[DataFrame => DataFrame] = Seq(
      _.select("id", "value").orderBy("id"),
      _.filter("value IS NULL OR (value >= 8 AND id < 7)").select("id").orderBy("id"),
      _.filter("value IS NOT NULL").orderBy(desc("value"))
        .limit(2).select("id"),
      _.select("id", "_rowaddr").orderBy("id"))
    queries.zipWithIndex.foreach { case (query, index) =>
      assertEquals(
        query(reference).collect().toSeq,
        query(primitive).collect().toSeq,
        s"query $index must match the Dataset scanner")
    }
    assertEquals(7L, primitive.count())
    assertEquals(
      reference.filter("value IS NULL").count(),
      primitive.filter("value IS NULL").count())
    val unsupported = new LanceScanBuilder(
      new StructType().add("_rowid", LongType),
      LanceSparkReadOptions.builder().datasetUri(uri).primitiveScan(true).build(),
      null,
      null,
      null)
    val error = assertThrows(classOf[IllegalArgumentException], () => unsupported.build())
    assertTrue(
      error.getMessage.contains("experimental_primitive_scan does not support field '_rowid'"))
  }

  @Test
  def serializedTasksReadTheirSnapshotWithoutManifestsOnSparkWorkers(): Unit = {
    val uri = fixture()
    val options = LanceSparkReadOptions.from(
      Collections.singletonMap(LanceSparkReadOptions.CONFIG_PRIMITIVE_SCAN, "true"),
      uri)
    val schema = new StructType().add("id", LongType, false).add("value", LongType, true)
    val dataset = Utils.openDatasetBuilder(options).build()
    val captured =
      try {
        LancePrimitiveScan.capture(dataset, schema, options, null, null, null)
      } finally dataset.close()
    val tasks = captured.planInputPartitions().map { partition =>
      val task = partition.asInstanceOf[LancePrimitiveInputPartition].task
      FragmentReadTask.fromBytes(task.toBytes())
    }
    val preplanned = new LancePrimitiveScan(schema, tasks, options, null, null, null)

    // A later commit must not change the data or deletion files named by the captured tasks.
    spark.range(20, 21).selectExpr("id", "id * 2 AS value")
      .write.format("lance").mode("append").save(uri)
    val manifests = tempDir.resolve("table.lance").resolve("_versions")
    val hidden = tempDir.resolve("saved-manifests")
    Files.move(manifests, hidden)
    try {
      assertThrows(
        classOf[Exception],
        () => {
          val unavailable = Utils.openDatasetBuilder(options).build()
          unavailable.close()
        })
      val partitions = preplanned.planInputPartitions()
      assertEquals(2, partitions.length)
      val rows = spark.sparkContext.parallelize(partitions.toSeq, partitions.length)
        .mapPartitions { input =>
          input.flatMap { partition =>
            val reader = LancePrimitiveReaderFactory.createReader(partition)
            val values = ArrayBuffer.empty[(Long, Option[Long])]
            try {
              while (reader.next()) {
                val row = reader.get()
                values += ((row.getLong(0), if (row.isNullAt(1)) None else Some(row.getLong(1))))
              }
            } finally reader.close()
            values.iterator
          }
        }.collect().sortBy(_._1).toSeq
      val expected = (0L until 8L).filter(_ != 2L)
        .map(id => (id, if (id % 3 == 0) None else Some(id * 2)))
      assertEquals(expected, rows)
    } finally Files.move(hidden, manifests)
  }

  @Test
  def primitiveOptionSurvivesPinningSerializationAndScanOverrides(): Unit = {
    val original = LanceSparkReadOptions.from(
      Collections.singletonMap(LanceSparkReadOptions.CONFIG_PRIMITIVE_SCAN, "true"),
      "file:///example.lance").withRef(LanceRef.ofMain(42))
    val bytes = new ByteArrayOutputStream()
    val output = new ObjectOutputStream(bytes)
    try output.writeObject(original)
    finally output.close()
    val input = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray))
    val restored =
      try input.readObject().asInstanceOf[LanceSparkReadOptions]
      finally input.close()
    assertEquals(original, restored)
    assertTrue(restored.isPrimitiveScan)
    assertEquals(LanceRef.ofMain(42), restored.getRef)
    assertFalse(LanceSparkReadOptions.from("file:///example.lance").isPrimitiveScan)

    Seq((true, None, true), (true, Some(false), false), (false, Some(true), true))
      .foreach { case (tableMode, scanOverride, expectedMode) =>
        val options = LanceSparkReadOptions.builder().datasetUri("file:///example.lance")
          .primitiveScan(tableMode).build()
        val table = new LanceDataset(
          options,
          new StructType().add("id", LongType),
          Collections.emptyMap(),
          null,
          Collections.emptyMap(),
          false,
          null)
        val overrides = new HashMap[String, String]()
        overrides.put(LanceSparkReadOptions.CONFIG_BATCH_SIZE, "2")
        scanOverride.foreach { value =>
          overrides.put(LanceSparkReadOptions.CONFIG_PRIMITIVE_SCAN, value.toString)
        }
        val builder = table.newScanBuilder(new CaseInsensitiveStringMap(overrides))
          .asInstanceOf[LanceScanBuilder]
        // No Dataset is opened: accepting a native limit would show that primitive mode was lost.
        assertEquals(
          !expectedMode,
          builder.pushLimit(1),
          s"table primitive=$tableMode, scan override=$scanOverride")
      }
  }

  @Test
  def btreeSegmentsShuffleAddressesToIndependentFragmentTasks(): Unit = {
    val uri = tempDir.resolve("indexed.lance").toString
    val expression =
      "CAST(CASE WHEN id % 4 = 1 THEN NULL WHEN id % 4 = 3 THEN 9 ELSE 7 END AS INT) AS x"
    spark.range(0, 8, 1, 2).selectExpr("id", expression)
      .write.format("lance").option("enable_stable_row_ids", "false").save(uri)
    val options = LanceSparkReadOptions.from(uri)
    val indexed = Utils.openDatasetBuilder(options).build()
    try {
      assertEquals(2, indexed.getFragments.size())
      indexed.createIndex(IndexOptions.builder(
        Collections.singletonList("x"),
        IndexType.BTREE,
        IndexParams.builder().setScalarIndexParams(ScalarIndexParams.create("btree")).build())
        .withIndexName("x_idx").build())
    } finally indexed.close()

    // The new fragment is not covered by the existing segment.
    spark.range(8, 12, 1, 1).selectExpr("id", expression)
      .write.format("lance").mode("append").save(uri)
    val snapshot = Utils.openDatasetBuilder(options).build()
    val (segments, fragments) =
      try {
        snapshot.delete("id = 2")
        val segments = snapshot.getIndexes.asScala.filter(_.name() == "x_idx").map { index =>
          IndexSegmentReadTask.capture(snapshot, index.uuid())
        }.toVector
        val fragments = snapshot.getFragments.asScala.map { fragment =>
          val id = Integer.toUnsignedLong(fragment.getId)
          (id, FragmentReadTask.capture(snapshot, id, Seq("id", "x").asJava))
        }.toVector
        (segments, fragments)
      } finally snapshot.close()
    assertEquals(1, segments.size)
    assertEquals(2, segments.head.getFragmentIds.length, "one segment spans two fragments")
    val covered = segments.flatMap(_.getFragmentIds).toSet
    val fallback = fragments.filterNot { case (id, _) => covered.contains(id) }
    assertEquals(1, fallback.size)

    val rowSchema = new StructType().add("id", LongType, false).add("x", IntegerType, true)
    val predicates = Seq("x = 7", "x IS NULL")
    val expectedFull = predicates.map { predicate =>
      read(uri, primitive = false).filter(predicate).orderBy("id").collect().toSeq
    }
    val expected = predicates.map { predicate =>
      read(uri, primitive = false).filter(predicate).filter("id >= 4")
        .orderBy(desc("id")).limit(2).collect().toSeq
    }
    val manifests = tempDir.resolve("indexed.lance").resolve("_versions")
    val hidden = tempDir.resolve("indexed-manifests")
    Files.move(manifests, hidden)
    try {
      assertThrows(
        classOf[Exception],
        () => {
          val unavailable = Utils.openDatasetBuilder(options).build()
          unavailable.close()
        })
      val sc = spark.sparkContext
      // Only segment descriptors travel to index-search tasks. Fragment metadata is a separate RDD.
      val segmentRDD = sc.parallelize(segments, segments.size)
      val liveFragments = sc.parallelize(fragments, fragments.size)
      predicates.zipWithIndex.foreach { case (predicate, queryIndex) =>
        val isNullQuery = queryIndex == 1
        val hits = segmentRDD.flatMap { segment =>
          val allocator = LanceRuntime.allocator()
          val literal = VectorSchemaRoot.of(new IntVector("x", allocator))
          try {
            literal.allocateNew()
            literal.getVector(0).asInstanceOf[IntVector].setSafe(0, 7)
            literal.setRowCount(1)
            val builder = segment.reader().allocator(allocator)
              .session(LanceRuntime.session()).batchSize(2)
            if (isNullQuery) builder.isNull() else builder.equalsValue(literal)
            val reader = builder.open()
            val addresses = ArrayBuffer.empty[Long]
            try {
              while (reader.loadNextBatch()) {
                val values = reader.getVectorSchemaRoot.getVector("_rowaddr")
                  .asInstanceOf[UInt8Vector]
                (0 until values.getValueCount).foreach(i => addresses += values.get(i))
              }
            } finally reader.close()
            addresses.iterator
          } finally literal.close()
        }
        val offsets = hits.distinct().map(address => (address >>> 32, address & 0xFFFFFFFFL))
          .groupByKey(2)
        // The join also drops index hits whose fragment is absent from this snapshot.
        val indexedRows = offsets.join(liveFragments).flatMap { case (_, (rows, task)) =>
          PrimitiveScanTestRows.fragmentRows(task, Some(rows.toSet.toArray.sorted))
        }
        val fallbackRows = sc.parallelize(fallback, fallback.size).flatMap { case (_, task) =>
          PrimitiveScanTestRows.fragmentRows(task, None)
        }
        val materialized = spark.createDataFrame(indexedRows.union(fallbackRows), rowSchema)
          .filter(predicate).persist()
        try {
          assertEquals(
            expectedFull(queryIndex),
            materialized.orderBy("id").collect().toSeq,
            s"$predicate must apply deletes and include unindexed fragments")
          val result = materialized.filter("id >= 4").orderBy(desc("id")).limit(2).collect().toSeq
          assertEquals(expected(queryIndex), result, predicate)
        } finally materialized.unpersist()
      }
    } finally Files.move(hidden, manifests)
  }
}

private object PrimitiveScanTestRows {
  // This fixture has four physical rows per fragment. Collecting here keeps the test's resource
  // lifetime explicit; a production adapter would stream batches and bound each take group.
  def fragmentRows(task: FragmentReadTask, offsets: Option[Array[Long]]): Iterator[Row] = {
    val builder = task.reader().allocator(LanceRuntime.allocator())
      .session(LanceRuntime.session()).batchSize(2)
    offsets.foreach(builder.rowOffsets)
    val reader = builder.open()
    val rows = ArrayBuffer.empty[Row]
    try {
      while (reader.loadNextBatch()) {
        val root = reader.getVectorSchemaRoot
        val ids = root.getVector("id").asInstanceOf[BigIntVector]
        val values = root.getVector("x").asInstanceOf[IntVector]
        (0 until root.getRowCount).foreach { i =>
          val value: java.lang.Integer =
            if (values.isNull(i)) null else Integer.valueOf(values.get(i))
          rows += Row(ids.get(i), value)
        }
      }
    } finally reader.close()
    rows.iterator
  }
}
