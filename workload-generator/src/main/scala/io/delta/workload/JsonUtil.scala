/*
 * Copyright (2025) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.delta.workload

import java.nio.file.{Files, Path}

import com.fasterxml.jackson.annotation._
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.functions._

// =============================================================================
// Spec Expected types - success data or error info
// =============================================================================

/** Common error type for all specs. */
@JsonPropertyOrder(Array("errorCode", "errorMessage"))
case class SpecError(errorCode: String, errorMessage: String)

/** CDF success data. */
case class CdfExpected(rowCount: Long)

/** Read success data. */
@JsonPropertyOrder(Array("rowCount", "fileCount", "filesSkipped"))
case class ReadExpected(rowCount: Long, fileCount: Int, filesSkipped: Long)

/** Snapshot success data. */
@JsonPropertyOrder(Array("protocol", "metadata"))
case class SnapshotExpected(protocol: Any, metadata: Any)

/** Domain metadata success data. */
@JsonPropertyOrder(Array("domain", "configuration", "removed"))
case class DomainMetadataExpected(domain: String, configuration: String, removed: Boolean)

/** AppTxn (SetTransaction) success data. */
@JsonPropertyOrder(Array("appId", "txnVersion"))
case class TxnExpected(appId: String, txnVersion: Long)

// =============================================================================
// Spec case classes
// =============================================================================

@JsonPropertyOrder(Array("type", "startVersion", "startTimestamp", "endVersion", "endTimestamp",
  "predicate", "columns", "expected", "expectedError"))
@JsonInclude(JsonInclude.Include.NON_ABSENT)
case class CdfSpec(
    startVersion: Option[Long] = None,
    endVersion: Option[Long] = None,
    startTimestamp: Option[String] = None,
    endTimestamp: Option[String] = None,
    predicate: Option[String] = None,
    columns: Option[Seq[String]] = None,
    expected: Option[CdfExpected] = None,
    expectedError: Option[SpecError] = None) {
  val `type`: String = "cdf"
}

@JsonPropertyOrder(Array("type", "version", "timestamp", "predicate", "columns", "expected", "expectedError"))
@JsonInclude(JsonInclude.Include.NON_ABSENT)
case class ReadSpec(
    version: Option[Long] = None,
    timestamp: Option[String] = None,
    predicate: Option[String] = None,
    columns: Option[Seq[String]] = None,
    expected: Option[ReadExpected] = None,
    expectedError: Option[SpecError] = None) {
  val `type`: String = "read"
}

@JsonPropertyOrder(Array("type", "version", "timestamp", "expected", "expectedError"))
@JsonInclude(JsonInclude.Include.NON_ABSENT)
case class SnapshotSpec(
    version: Option[Long] = None,
    timestamp: Option[String] = None,
    expected: Option[SnapshotExpected] = None,
    expectedError: Option[SpecError] = None) {
  val `type`: String = "snapshot"
}

@JsonPropertyOrder(Array("type", "version", "expected", "expectedError"))
@JsonInclude(JsonInclude.Include.NON_ABSENT)
case class DomainMetadataSpec(
    version: Option[Long] = None,
    expected: Option[DomainMetadataExpected] = None,
    expectedError: Option[SpecError] = None) {
  val `type`: String = "domain_metadata"
}

@JsonPropertyOrder(Array("type", "version", "expected", "expectedError"))
@JsonInclude(JsonInclude.Include.NON_ABSENT)
case class TxnSpec(
    version: Option[Long] = None,
    expected: Option[TxnExpected] = None,
    expectedError: Option[SpecError] = None) {
  val `type`: String = "appTxn"
}

// =============================================================================
// TableInfo case classes
// =============================================================================

@JsonPropertyOrder(Array("minReaderVersion", "minWriterVersion", "readerFeatures", "writerFeatures"))
@JsonInclude(JsonInclude.Include.NON_ABSENT)
case class ProtocolInfo(
    minReaderVersion: Int,
    minWriterVersion: Int,
    readerFeatures: Option[Seq[String]] = None,
    writerFeatures: Option[Seq[String]] = None)

@JsonPropertyOrder(Array("numAddFiles", "numRemoveFiles", "sizeInBytes", "numCommits",
  "numActions", "lastCheckpointVersion", "lastCrcVersion", "numCheckpointFiles"))
case class LogInfo(
    numAddFiles: Long,
    numRemoveFiles: Long,
    sizeInBytes: Long,
    numCommits: Int,
    numActions: Long,
    lastCheckpointVersion: Long,
    lastCrcVersion: Long,
    numCheckpointFiles: Int)

@JsonPropertyOrder(Array("numClusteringColumns", "numPartitionColumns", "numDistinctPartitions"))
case class DataLayoutInfo(
    numClusteringColumns: Int,
    numPartitionColumns: Int,
    numDistinctPartitions: Long)

@JsonPropertyOrder(Array("name", "description", "schema", "protocol", "logInfo",
  "properties", "dataLayout", "tags"))
@JsonInclude(JsonInclude.Include.NON_ABSENT)
case class TableInfo(
    name: String,
    description: String,
    schema: Any,
    protocol: ProtocolInfo,
    logInfo: LogInfo,
    properties: Map[String, String],
    dataLayout: DataLayoutInfo,
    tags: Option[Seq[String]] = None)

@JsonPropertyOrder(Array("name", "description", "error"))
case class MinimalTableInfo(
    name: String,
    description: String,
    error: String)

// =============================================================================
// Low-level action types
// =============================================================================

/** Application transaction for idempotent writes. */
@JsonPropertyOrder(Array("appId", "version"))
case class AppTxn(appId: String, version: Long)

/** Deletion vector descriptor for low-level commits. */
@JsonPropertyOrder(Array("storageType", "pathOrInlineDv", "offset", "sizeInBytes", "cardinality"))
@JsonInclude(JsonInclude.Include.NON_ABSENT)
case class DeletionVectorAction(
    storageType: String,
    pathOrInlineDv: String,
    offset: Option[Int] = None,
    sizeInBytes: Int,
    cardinality: Long)

/** Add file action for low-level commits. */
@JsonPropertyOrder(Array("dataFile", "partitionValues", "dataChange", "deletionVector"))
@JsonInclude(JsonInclude.Include.NON_ABSENT)
case class AddFileAction(
    dataFile: String,
    partitionValues: Option[Map[String, String]] = None,
    dataChange: Option[Boolean] = None,
    deletionVector: Option[DeletionVectorAction] = None)

/** Remove file action for low-level commits. */
@JsonPropertyOrder(Array("path", "dataChange"))
@JsonInclude(JsonInclude.Include.NON_ABSENT)
case class RemoveFileAction(
    path: String,
    dataChange: Option[Boolean] = None)

/** Domain metadata entry for low-level commits (added domains). */
@JsonPropertyOrder(Array("domain", "configuration"))
case class AddDomainMetadata(
    domain: String,
    configuration: String)

// =============================================================================
// Delta log action case classes (for parsing commit JSON)
// =============================================================================

case class TxnAction(appId: String, version: Long)

case class TxnActionWrapper(txn: TxnAction)

// =============================================================================
// Last checkpoint case classes
// =============================================================================

@JsonInclude(JsonInclude.Include.NON_ABSENT)
case class V2CheckpointInfo(
    path: Option[String] = None,
    sizeInBytes: Option[Long] = None,
    nonFileActions: Option[Any] = None,
    sidecarFiles: Option[Any] = None)

@JsonInclude(JsonInclude.Include.NON_ABSENT)
case class LastCheckpointInfo(
    version: Long,
    size: Option[Long] = None,
    sizeInBytes: Option[Long] = None,
    numOfAddFiles: Option[Long] = None,
    checkpointSchema: Option[Any] = None,
    checksum: Option[String] = None,
    v2Checkpoint: Option[V2CheckpointInfo] = None)

// =============================================================================
// Checkpoint spec case classes
// =============================================================================

@JsonPropertyOrder(Array("type", "version", "expected"))
case class CheckpointSpec(
    version: Long,
    expected: CheckpointExpected) {
  val `type`: String = "checkpoint"
}

@JsonPropertyOrder(Array("protocol", "metadata", "txn", "domainMetadata"))
@JsonInclude(JsonInclude.Include.NON_ABSENT)
case class CheckpointExpected(
    protocol: Any,
    metadata: Any,
    txn: Option[Seq[TxnAction]] = None,
    domainMetadata: Option[Seq[DomainMetadataEntry]] = None)

@JsonInclude(JsonInclude.Include.NON_ABSENT)
case class DomainMetadataEntry(
    domain: String,
    configuration: Option[String] = None)

// =============================================================================
// CRC spec case classes
// =============================================================================

@JsonPropertyOrder(Array("type", "version", "expected"))
case class CrcSpec(
    version: Long,
    expected: CrcExpected) {
  val `type`: String = "crc"
}

@JsonPropertyOrder(Array("tableSizeBytes", "numFiles", "numRemoveFiles",
  "numTransactions", "numDomainMetadata", "protocol", "metadata",
  "txn", "histograms", "deletionVectors", "inCommitTimestamp"))
@JsonInclude(JsonInclude.Include.NON_ABSENT)
case class CrcExpected(
    tableSizeBytes: Option[Long] = None,
    numFiles: Option[Long] = None,
    numRemoveFiles: Option[Long] = None,
    numTransactions: Option[Long] = None,
    numDomainMetadata: Option[Long] = None,
    protocol: Option[ProtocolInfo] = None,
    metadata: Option[Any] = None,  // Complex nested structure, keep as Any
    txn: Option[Seq[TxnAction]] = None,
    histograms: Option[CrcHistograms] = None,
    deletionVectors: Option[CrcDeletionVectorStats] = None,
    inCommitTimestamp: Option[Long] = None)

@JsonInclude(JsonInclude.Include.NON_ABSENT)
case class CrcHistograms(
    addFiles: Option[Any] = None,
    removeFiles: Option[Any] = None)

@JsonInclude(JsonInclude.Include.NON_ABSENT)
case class CrcDeletionVectorStats(
    numDeletionVectors: Option[Long] = None,
    numLogicalDeletionVectorRows: Option[Long] = None,
    deletionVectorSizeInBytes: Option[Long] = None)

// =============================================================================
// JSON and DataFrame utilities
// =============================================================================

object JsonUtil {

  val mapper: ObjectMapper = {
    val m = new ObjectMapper()
    m.registerModule(DefaultScalaModule)
    m.enable(DeserializationFeature.USE_LONG_FOR_INTS)
    m.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    m
  }

  private val prettyWriter = mapper.writerWithDefaultPrettyPrinter()

  def writeSpec(path: Path, spec: Any): Unit =
    Files.write(path, prettyWriter.writeValueAsBytes(spec))

  def readCdfSpec(path: Path): CdfSpec =
    mapper.readValue(Files.readAllBytes(path), classOf[CdfSpec])

  def readReadSpec(path: Path): ReadSpec =
    mapper.readValue(Files.readAllBytes(path), classOf[ReadSpec])

  def readSnapshotSpec(path: Path): SnapshotSpec =
    mapper.readValue(Files.readAllBytes(path), classOf[SnapshotSpec])

  def readDomainMetadataSpec(path: Path): DomainMetadataSpec =
    mapper.readValue(Files.readAllBytes(path), classOf[DomainMetadataSpec])

  def readTxnSpec(path: Path): TxnSpec =
    mapper.readValue(Files.readAllBytes(path), classOf[TxnSpec])

  def toRowMultiset(df: DataFrame): Map[String, Int] =
    df.toJSON.collect().groupBy(identity).view.mapValues(_.length).toMap

  def columnRef(name: String): Column =
    if (name.startsWith("_metadata.")) col(name)
    else col(s"`${name.replace("`", "``")}`")

  def buildDeltaReader(spark: SparkSession, tablePath: Path,
      version: Option[Long], timestamp: Option[String]): DataFrame = {
    var reader = spark.read.format("delta")
    version.foreach(v => reader = reader.option("versionAsOf", v))
    timestamp.foreach(ts => reader = reader.option("timestampAsOf", ts))
    reader.load(tablePath.toString)
  }

  def applyFilters(df: DataFrame, predicate: Option[String],
      columns: Option[Seq[String]]): DataFrame = {
    var result = df
    predicate.foreach(p => result = result.filter(p))
    columns.foreach(cols => result = result.select(cols.map(columnRef): _*))
    result
  }

  def extractErrorCode(e: Exception): String = e match {
    case st: org.apache.spark.SparkThrowable =>
      Option(st.getErrorClass).getOrElse(e.getClass.getSimpleName)
    case _ => e.getClass.getSimpleName
  }

  def resolveSnapshot(deltaLog: DeltaLog, version: Option[Long],
      timestamp: Option[String]): org.apache.spark.sql.delta.Snapshot = {
    (version, timestamp) match {
      case (Some(v), _) => deltaLog.getSnapshotAt(v)
      case (_, Some(ts)) =>
        val tsValue = java.sql.Timestamp.valueOf(ts)
        deltaLog.getSnapshotAt(
          deltaLog.history.getActiveCommitAtTime(tsValue, None,
            canReturnLastCommit = true, mustBeRecreatable = false,
            canReturnEarliestCommit = false).version)
      case _ => deltaLog.update()
    }
  }

  def assertMultisetsEqual(expected: Map[String, Int], actual: Map[String, Int],
      specName: String): Unit = {
    if (expected != actual) {
      val missing = expected.keySet -- actual.keySet
      val extra = actual.keySet -- expected.keySet
      val countMismatches = (expected.keySet & actual.keySet).filter(k => expected(k) != actual(k))
      val details = new StringBuilder()
      if (missing.nonEmpty) {
        details.append(s"\n  Missing rows: ${missing.size}")
        missing.take(3).foreach(r => details.append(s"\n    $r"))
      }
      if (extra.nonEmpty) {
        details.append(s"\n  Extra rows: ${extra.size}")
        extra.take(3).foreach(r => details.append(s"\n    $r"))
      }
      if (countMismatches.nonEmpty) {
        details.append(s"\n  Count mismatches: ${countMismatches.size}")
        countMismatches.take(3).foreach { r =>
          details.append(s"\n    expected ${expected(r)}x, got ${actual(r)}x: $r")
        }
      }
      throw new RuntimeException(
        s"Validation FAILED for $specName: row-level mismatch" +
          s" (expected ${expected.values.sum}, got ${actual.values.sum})$details")
    }
  }
}
