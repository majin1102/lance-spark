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

import org.apache.arrow.vector.ipc.ArrowReader
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReader, PartitionReaderFactory, Scan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.lance.Dataset
import org.lance.fragment.FragmentReadTask
import org.lance.spark.{LanceConstant, LanceRuntime, LanceSparkReadOptions}
import org.lance.spark.internal.ExecutorNamespace
import org.lance.spark.search.LanceSearchRowPartitionReader
import org.lance.spark.utils.{BlobUtils, Utils}
import org.lance.spark.vectorized.LanceArrowColumnVector

import java.util.{Map => JMap}

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

/**
 * Experimental scan whose partitions contain complete fragment read tasks. The driver captures one
 * snapshot; workers open only the files named by their task. Spark evaluates all query operators.
 *
 * The constructor also accepts tasks produced by an external planner. Task bytes are experimental
 * and must come from the same lance-core build as the workers.
 */
final class LancePrimitiveScan(
    schema: StructType,
    tasks: Array[FragmentReadTask],
    readOptions: LanceSparkReadOptions,
    initialStorageOptions: JMap[String, String],
    namespaceImpl: String,
    namespaceProperties: JMap[String, String])
  extends Scan
  with Batch
  with Serializable {

  private val partitions: Array[InputPartition] = tasks.map { task =>
    LancePrimitiveInputPartition(
      schema,
      task,
      readOptions,
      initialStorageOptions,
      namespaceImpl,
      namespaceProperties)
  }

  override def readSchema(): StructType = schema
  override def description(): String = "LancePrimitiveScan"
  override def toBatch(): Batch = this
  override def planInputPartitions(): Array[InputPartition] = partitions.clone()
  override def createReaderFactory(): PartitionReaderFactory = LancePrimitiveReaderFactory
}

object LancePrimitiveScan {

  /** Capture only the metadata each fragment needs, while the driver's snapshot is still open. */
  def capture(
      dataset: Dataset,
      schema: StructType,
      readOptions: LanceSparkReadOptions,
      initialStorageOptions: JMap[String, String],
      namespaceImpl: String,
      namespaceProperties: JMap[String, String]): LancePrimitiveScan = {
    require(
      readOptions.getFullTextQuery == null,
      "experimental_primitive_scan does not support full-text search")
    val unsupported = Set(
      LanceConstant.ROW_ID,
      LanceConstant.FRAGMENT_ID,
      LanceConstant.ROW_CREATED_AT_VERSION,
      LanceConstant.ROW_LAST_UPDATED_AT_VERSION)
    schema.fields.foreach { field =>
      require(
        !unsupported.contains(field.name) && !Utils.isScoreMetadataColumn(field) &&
          !BlobUtils.isBlobReadColumn(field) &&
          !field.name.endsWith(LanceConstant.BLOB_POSITION_SUFFIX) &&
          !field.name.endsWith(LanceConstant.BLOB_SIZE_SUFFIX),
        s"experimental_primitive_scan does not support field '${field.name}'")
    }
    val columns = schema.fieldNames.filterNot(_ == LanceConstant.ROW_ADDRESS).toList.asJava
    val withRowAddress = schema.fieldNames.contains(LanceConstant.ROW_ADDRESS)
    val tasks = dataset.getFragments.asScala.map { fragment =>
      val task = FragmentReadTask.capture(dataset, Integer.toUnsignedLong(fragment.getId), columns)
      if (withRowAddress) task.withRowAddress() else task
    }.toArray
    new LancePrimitiveScan(
      schema,
      tasks,
      readOptions.withRef(Utils.pinOpenedRef(dataset, readOptions.getRef)),
      initialStorageOptions,
      namespaceImpl,
      namespaceProperties)
  }
}

final private[read] case class LancePrimitiveInputPartition(
    schema: StructType,
    task: FragmentReadTask,
    readOptions: LanceSparkReadOptions,
    initialStorageOptions: JMap[String, String],
    namespaceImpl: String,
    namespaceProperties: JMap[String, String])
  extends InputPartition

private[read] object LancePrimitiveReaderFactory extends PartitionReaderFactory {
  override def supportColumnarReads(partition: InputPartition): Boolean = true

  override def createColumnarReader(partition: InputPartition): PartitionReader[ColumnarBatch] = {
    LanceRuntime.enableOpenTelemetry()
    new LancePrimitiveColumnarReader(partition.asInstanceOf[LancePrimitiveInputPartition])
  }

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] =
    new LanceSearchRowPartitionReader(createColumnarReader(partition))
}

final private[read] class LancePrimitiveColumnarReader(partition: LancePrimitiveInputPartition)
  extends PartitionReader[ColumnarBatch] {
  private var namespace: ExecutorNamespace = _
  private var reader: ArrowReader = _
  private var batch: ColumnarBatch = _
  private var closed = false

  override def next(): Boolean = {
    if (closed) return false
    try {
      if (reader == null) {
        val options = partition.readOptions
        namespace = ExecutorNamespace.acquire(
          options,
          partition.namespaceImpl,
          partition.namespaceProperties)
        val builder = partition.task
          .reader()
          .allocator(LanceRuntime.allocator())
          .session(LanceRuntime.session(
            options.getCatalogName,
            options.getIndexCacheBackend,
            options.getMetadataCacheBackend))
          .storageOptions(LanceRuntime.mergeStorageOptions(
            options.getStorageOptions,
            partition.initialStorageOptions))
          .batchSize(options.getBatchSize)
        if (options.getBlockSize != null) builder.blockSize(options.getBlockSize)
        if (options.hasNamespace) {
          builder.namespaceClient(options.getNamespace).tableId(options.getTableId)
        }
        reader = builder.open()
      }
      if (batch != null) {
        batch.close()
        batch = null
      }
      if (!reader.loadNextBatch()) return false
      val root = reader.getVectorSchemaRoot
      val columns = partition.schema.fields.map { field =>
        val vector = root.getVector(field.name)
        require(vector != null, s"Primitive task did not return field '${field.name}'")
        new LanceArrowColumnVector(vector, false, field): ColumnVector
      }
      batch = new ColumnarBatch(columns, root.getRowCount)
      true
    } catch {
      case NonFatal(error) =>
        try close()
        catch { case NonFatal(closeError) => error.addSuppressed(closeError) }
        throw error
    }
  }

  override def get(): ColumnarBatch = batch

  override def close(): Unit = {
    if (!closed) {
      closed = true
      var failure: Throwable = null
      Seq[AutoCloseable](batch, reader, namespace).filter(_ != null).foreach { resource =>
        try resource.close()
        catch {
          case NonFatal(error) =>
            if (failure == null) failure = error
            else failure.addSuppressed(error)
        }
      }
      if (failure != null) throw failure
    }
  }
}
