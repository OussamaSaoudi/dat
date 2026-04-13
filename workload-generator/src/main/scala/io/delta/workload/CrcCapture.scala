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

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.DeltaLog

/**
 * Captures CRC (checksum) verification specs.
 *
 * For a given version, reads the .crc file from the delta log and captures:
 * - tableSizeBytes, numFiles, numRemoveFiles
 * - numTransactions, numDomainMetadata
 * - protocol, metadata
 * - appTxn / SetTransaction (application transactions)
 * - histograms, deletionVectors (if present)
 * - inCommitTimestamp (if present)
 */
object CrcCapture {

  def capture(
      spark: SparkSession,
      testId: String,
      tablePath: Path,
      specsDir: Path,
      version: Long): Unit = {

    val specName = s"${testId}_crc_v$version"

    // Read the CRC file
    val crcFile = tablePath.resolve("_delta_log").resolve(f"$version%020d.crc")
    require(Files.exists(crcFile),
      s"CRC file not found for version $version: $crcFile")

    val crcContent = new String(Files.readAllBytes(crcFile), "UTF-8")
    val crcNode = JsonUtil.mapper.readTree(crcContent)

    // Protocol from snapshot for canonical form
    DeltaLog.clearCache()
    val dl = DeltaLog.forTable(spark, tablePath.toString)
    val snapshot = dl.getSnapshotAt(version)

    val protocol = ProtocolInfo(
      minReaderVersion = snapshot.protocol.minReaderVersion,
      minWriterVersion = snapshot.protocol.minWriterVersion,
      readerFeatures = snapshot.protocol.readerFeatures.map(_.toSeq.sorted),
      writerFeatures = snapshot.protocol.writerFeatures.map(_.toSeq.sorted)
    )

    // Metadata from snapshot
    val metadata = JsonUtil.mapper.treeToValue(
      JsonUtil.mapper.readTree(snapshot.metadata.json).get("metaData"), classOf[Any])

    // Txn actions from CRC
    val txn = if (crcNode.has("txn") && !crcNode.get("txn").isNull) {
      val txnNode = crcNode.get("txn")
      if (txnNode.isArray) {
        Some(txnNode.elements().asScala.map { t =>
          TxnAction(t.get("appId").asText(), t.get("version").asLong())
        }.toSeq)
      } else if (txnNode.isObject) {
        Some(Seq(TxnAction(txnNode.get("appId").asText(), txnNode.get("version").asLong())))
      } else None
    } else None

    // Histograms
    val histograms = if (crcNode.has("histograms") && !crcNode.get("histograms").isNull) {
      val h = crcNode.get("histograms")
      Some(CrcHistograms(
        addFiles = getAnyField(h, "addFiles"),
        removeFiles = getAnyField(h, "removeFiles")
      ))
    } else None

    // Deletion vector stats
    val deletionVectors = if (crcNode.has("deletionVectors") && !crcNode.get("deletionVectors").isNull) {
      val dv = crcNode.get("deletionVectors")
      Some(CrcDeletionVectorStats(
        numDeletionVectors = getLongField(dv, "numDeletionVectors"),
        numLogicalDeletionVectorRows = getLongField(dv, "numLogicalDeletionVectorRows"),
        deletionVectorSizeInBytes = getLongField(dv, "deletionVectorSizeInBytes")
      ))
    } else None

    val expected = CrcExpected(
      tableSizeBytes = getLongField(crcNode, "tableSizeBytes"),
      numFiles = getLongField(crcNode, "numFiles"),
      numRemoveFiles = getLongField(crcNode, "numRemoveFiles"),
      numTransactions = getLongField(crcNode, "numTransactions"),
      numDomainMetadata = getLongField(crcNode, "numDomainMetadata"),
      protocol = Some(protocol),
      metadata = Some(metadata),
      txn = txn,
      histograms = histograms,
      deletionVectors = deletionVectors,
      inCommitTimestamp = getLongField(crcNode, "inCommitTimestamp")
    )

    val spec = CrcSpec(version, expected)
    JsonUtil.writeSpec(specsDir.resolve(s"$specName.json"), spec)

    // Validate: re-read CRC and compare key fields
    validate(spark, specName, tablePath, specsDir, version)

    println(s"  CRC spec captured: $specName (version=$version)")
  }

  private def getLongField(
      node: com.fasterxml.jackson.databind.JsonNode,
      fieldName: String): Option[Long] = {
    if (node.has(fieldName) && !node.get(fieldName).isNull) {
      Some(node.get(fieldName).asLong())
    } else None
  }

  private def getAnyField(
      node: com.fasterxml.jackson.databind.JsonNode,
      fieldName: String): Option[Any] = {
    if (node.has(fieldName) && !node.get(fieldName).isNull) {
      Some(JsonUtil.mapper.treeToValue(node.get(fieldName), classOf[Any]))
    } else None
  }

  /** Validate a captured CRC spec by re-reading the CRC file and comparing. */
  private[workload] def validate(
      spark: SparkSession,
      specName: String,
      tablePath: Path,
      specsDir: Path,
      version: Long): Unit = {
    val specFile = specsDir.resolve(s"$specName.json")
    require(Files.exists(specFile), s"CRC spec file missing after capture: $specFile")

    val spec = JsonUtil.mapper.readValue(Files.readAllBytes(specFile), classOf[CrcSpec])

    // Re-read CRC file
    val crcFile = tablePath.resolve("_delta_log").resolve(f"$version%020d.crc")
    require(Files.exists(crcFile), s"CRC file missing on validation: $crcFile")
    val crcNode = JsonUtil.mapper.readTree(
      new String(Files.readAllBytes(crcFile), "UTF-8"))

    // Validate counts
    validateLongField(specName, "tableSizeBytes", spec.expected.tableSizeBytes,
      getLongField(crcNode, "tableSizeBytes"))
    validateLongField(specName, "numFiles", spec.expected.numFiles,
      getLongField(crcNode, "numFiles"))
    validateLongField(specName, "numRemoveFiles", spec.expected.numRemoveFiles,
      getLongField(crcNode, "numRemoveFiles"))
    validateLongField(specName, "numTransactions", spec.expected.numTransactions,
      getLongField(crcNode, "numTransactions"))
    validateLongField(specName, "numDomainMetadata", spec.expected.numDomainMetadata,
      getLongField(crcNode, "numDomainMetadata"))

    // Validate protocol
    if (spec.expected.protocol.isDefined) {
      DeltaLog.clearCache()
      val dl = DeltaLog.forTable(spark, tablePath.toString)
      val snapshot = dl.getSnapshotAt(version)
      val actualProtocol = ProtocolInfo(
        minReaderVersion = snapshot.protocol.minReaderVersion,
        minWriterVersion = snapshot.protocol.minWriterVersion,
        readerFeatures = snapshot.protocol.readerFeatures.map(_.toSeq.sorted),
        writerFeatures = snapshot.protocol.writerFeatures.map(_.toSeq.sorted)
      )
      require(spec.expected.protocol.get == actualProtocol,
        s"CRC validation failed for $specName: protocol mismatch")
    }

    // Validate txn
    if (spec.expected.txn.isDefined && crcNode.has("txn")) {
      val txnNode = crcNode.get("txn")
      val actualTxn = if (txnNode.isArray) {
        txnNode.elements().asScala.map { t =>
          TxnAction(t.get("appId").asText(), t.get("version").asLong())
        }.toSeq
      } else if (txnNode.isObject) {
        Seq(TxnAction(txnNode.get("appId").asText(), txnNode.get("version").asLong()))
      } else Seq.empty
      require(spec.expected.txn.get.toSet == actualTxn.toSet,
        s"CRC validation failed for $specName: txn mismatch")
    }

    // Validate deletion vectors stats
    if (spec.expected.deletionVectors.isDefined && crcNode.has("deletionVectors")) {
      val dv = crcNode.get("deletionVectors")
      val actualDv = CrcDeletionVectorStats(
        numDeletionVectors = getLongField(dv, "numDeletionVectors"),
        numLogicalDeletionVectorRows = getLongField(dv, "numLogicalDeletionVectorRows"),
        deletionVectorSizeInBytes = getLongField(dv, "deletionVectorSizeInBytes")
      )
      require(spec.expected.deletionVectors.get == actualDv,
        s"CRC validation failed for $specName: deletionVectors mismatch")
    }
  }

  private def validateLongField(
      specName: String,
      fieldName: String,
      expected: Option[Long],
      actual: Option[Long]): Unit = {
    (expected, actual) match {
      case (Some(exp), Some(act)) =>
        require(exp == act,
          s"CRC validation failed for $specName: $fieldName expected=$exp actual=$act")
      case _ => // OK if either is missing
    }
  }
}
