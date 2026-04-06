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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.DeltaLog

/**
 * Captures CRC (checksum) verification specs.
 *
 * For a given version, reads the .crc file from the delta log and captures:
 * - tableSizeBytes, numFiles, numRemoveFiles, numMetadata, numProtocol
 * - numTransactions, numDomainMetadata
 * - protocol, metadata (inline)
 * - histograms (if present)
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

    val spec = new java.util.LinkedHashMap[String, Any]()
    spec.put("type", "crc")
    spec.put("version", version)

    val expected = new java.util.LinkedHashMap[String, Any]()

    // Standard CRC fields
    copyLongField(crcNode, expected, "tableSizeBytes")
    copyLongField(crcNode, expected, "numFiles")
    copyLongField(crcNode, expected, "numRemoveFiles")
    copyLongField(crcNode, expected, "numMetadata")
    copyLongField(crcNode, expected, "numProtocol")
    copyLongField(crcNode, expected, "numTransactions")
    copyLongField(crcNode, expected, "numDomainMetadata")

    // Protocol and metadata — capture from snapshot for canonical form
    DeltaLog.clearCache()
    val dl = DeltaLog.forTable(spark, tablePath.toString)
    val snapshot = dl.getSnapshotAt(version)

    val protocolTree = JsonUtil.mapper.readTree(snapshot.protocol.json)
    expected.put("protocol",
      JsonUtil.mapper.treeToValue(protocolTree.get("protocol"), classOf[Any]))

    val metadataTree = JsonUtil.mapper.readTree(snapshot.metadata.json)
    expected.put("metadata",
      JsonUtil.mapper.treeToValue(metadataTree.get("metaData"), classOf[Any]))

    // Txn from CRC
    if (crcNode.has("txn")) {
      expected.put("txn", JsonUtil.mapper.treeToValue(crcNode.get("txn"), classOf[Any]))
    }

    // Histograms
    if (crcNode.has("histograms")) {
      expected.put("histograms",
        JsonUtil.mapper.treeToValue(crcNode.get("histograms"), classOf[Any]))
    }

    // Deletion vector stats
    if (crcNode.has("deletionVectors")) {
      expected.put("deletionVectors",
        JsonUtil.mapper.treeToValue(crcNode.get("deletionVectors"), classOf[Any]))
    }

    // In-commit timestamp
    if (crcNode.has("inCommitTimestamp")) {
      expected.put("inCommitTimestamp", crcNode.get("inCommitTimestamp").asLong())
    }

    spec.put("expected", expected)
    JsonUtil.writeJson(specsDir.resolve(s"$specName.json"), spec)

    // Validate: re-read CRC and compare key fields
    validate(spark, specName, tablePath, specsDir, version)

    println(s"  CRC spec captured: $specName (version=$version)")
  }

  private def copyLongField(
      source: com.fasterxml.jackson.databind.JsonNode,
      target: java.util.LinkedHashMap[String, Any],
      fieldName: String): Unit = {
    if (source.has(fieldName) && !source.get(fieldName).isNull) {
      target.put(fieldName, source.get(fieldName).asLong())
    }
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
    val spec = JsonUtil.mapper.readTree(Files.readAllBytes(specFile))

    require(spec.has("expected"), s"CRC spec $specName missing 'expected' block")
    val expected = spec.get("expected")

    // Re-read CRC file
    val crcFile = tablePath.resolve("_delta_log").resolve(f"$version%020d.crc")
    require(Files.exists(crcFile), s"CRC file missing on validation: $crcFile")
    val crcNode = JsonUtil.mapper.readTree(
      new String(Files.readAllBytes(crcFile), "UTF-8"))

    // Validate counts
    Seq("tableSizeBytes", "numFiles", "numRemoveFiles", "numMetadata",
        "numProtocol", "numTransactions", "numDomainMetadata").foreach { field =>
      if (expected.has(field) && crcNode.has(field)) {
        val exp = expected.get(field).asLong()
        val act = crcNode.get(field).asLong()
        require(exp == act,
          s"CRC validation failed for $specName: $field expected=$exp actual=$act")
      }
    }

    // Validate protocol
    if (expected.has("protocol")) {
      DeltaLog.clearCache()
      val dl = DeltaLog.forTable(spark, tablePath.toString)
      val snapshot = dl.getSnapshotAt(version)
      val expectedProto = JsonUtil.mapper.readTree(
        JsonUtil.mapper.writeValueAsString(expected.get("protocol")))
      val actualProto = JsonUtil.mapper.readTree(
        JsonUtil.mapper.writeValueAsString(
          JsonUtil.mapper.treeToValue(
            JsonUtil.mapper.readTree(snapshot.protocol.json).get("protocol"), classOf[Any])))
      require(expectedProto.equals(actualProto),
        s"CRC validation failed for $specName: protocol mismatch")
    }
  }
}
