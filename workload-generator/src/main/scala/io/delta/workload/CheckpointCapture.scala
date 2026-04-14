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

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.delta.DeltaLog

/**
 * Captures checkpoint verification specs.
 *
 * For a given version, generates:
 * - specs/<testId>_checkpoint_v<N>.json with expected _last_checkpoint fields,
 *   protocol, metadata, txn, domainMetadata
 * - expected/<testId>_checkpoint_v<N>/expected_add_files/ (parquet)
 * - expected/<testId>_checkpoint_v<N>/expected_remove_files/ (parquet)
 */
object CheckpointCapture {

  def capture(
      spark: SparkSession,
      testId: String,
      tablePath: Path,
      outputDir: Path,
      specsDir: Path,
      version: Long): Unit = {

    val specName = s"${testId}_checkpoint_v$version"

    DeltaLog.clearCache()
    val dl = DeltaLog.forTable(spark, tablePath.toString)
    val snapshot = dl.getSnapshotAt(version)

    val spec = new java.util.LinkedHashMap[String, Any]()
    spec.put("type", "checkpoint")
    spec.put("version", version)

    val expected = new java.util.LinkedHashMap[String, Any]()

    // _last_checkpoint contents
    val lastCheckpointFile = tablePath.resolve("_delta_log").resolve("_last_checkpoint")
    if (Files.exists(lastCheckpointFile)) {
      val lcContent = new String(Files.readAllBytes(lastCheckpointFile), "UTF-8")
      val lcNode = JsonUtil.mapper.readTree(lcContent)
      val lastCheckpoint = new java.util.LinkedHashMap[String, Any]()

      // Standard V1 fields
      if (lcNode.has("version")) lastCheckpoint.put("version", lcNode.get("version").asLong())
      if (lcNode.has("size")) lastCheckpoint.put("size", lcNode.get("size").asLong())
      if (lcNode.has("sizeInBytes")) {
        lastCheckpoint.put("sizeInBytes", lcNode.get("sizeInBytes").asLong())
      }
      if (lcNode.has("numOfAddFiles")) {
        lastCheckpoint.put("numOfAddFiles", lcNode.get("numOfAddFiles").asLong())
      }
      if (lcNode.has("checkpointSchema")) {
        lastCheckpoint.put("checkpointSchema",
          JsonUtil.mapper.treeToValue(lcNode.get("checkpointSchema"), classOf[Any]))
      }
      if (lcNode.has("checksum")) {
        lastCheckpoint.put("checksum", lcNode.get("checksum").asText())
      }

      // V2 checkpoint fields
      if (lcNode.has("v2Checkpoint")) {
        val v2Node = lcNode.get("v2Checkpoint")
        val v2 = new java.util.LinkedHashMap[String, Any]()
        if (v2Node.has("path")) v2.put("path", v2Node.get("path").asText())
        if (v2Node.has("sizeInBytes")) v2.put("sizeInBytes", v2Node.get("sizeInBytes").asLong())
        if (v2Node.has("nonFileActions")) {
          v2.put("nonFileActions",
            JsonUtil.mapper.treeToValue(v2Node.get("nonFileActions"), classOf[Any]))
        }
        if (v2Node.has("sidecarFiles")) {
          v2.put("sidecarFiles",
            JsonUtil.mapper.treeToValue(v2Node.get("sidecarFiles"), classOf[Any]))
        }
        lastCheckpoint.put("v2Checkpoint", v2)
      }

      expected.put("lastCheckpoint", lastCheckpoint)
    }

    // Protocol
    val protocolTree = JsonUtil.mapper.readTree(snapshot.protocol.json)
    expected.put("protocol",
      JsonUtil.mapper.treeToValue(protocolTree.get("protocol"), classOf[Any]))

    // Metadata
    val metadataTree = JsonUtil.mapper.readTree(snapshot.metadata.json)
    expected.put("metadata",
      JsonUtil.mapper.treeToValue(metadataTree.get("metaData"), classOf[Any]))

    // SetTransaction actions
    val txns = extractTxnActions(tablePath, version)
    if (txns.nonEmpty) expected.put("txn", txns.asJava)

    // DomainMetadata actions
    val domainMetadata = extractDomainMetadata(tablePath, version)
    if (domainMetadata.nonEmpty) expected.put("domainMetadata", domainMetadata.asJava)

    spec.put("expected", expected)
    JsonUtil.writeSpec(specsDir.resolve(s"$specName.json"), spec)

    // Expected add/remove files as parquet
    val expectedDir = outputDir.resolve("expected").resolve(specName)
    Files.createDirectories(expectedDir)

    writeExpectedAddFiles(spark, snapshot, expectedDir)
    writeExpectedRemoveFiles(spark, tablePath, version, expectedDir)

    // Validate: re-read and compare
    validate(spark, specName, tablePath, specsDir, version)

    val addCount = snapshot.allFiles.count()
    println(s"  Checkpoint spec captured: $specName (version=$version, $addCount add files)")
  }

  private def writeExpectedAddFiles(
      spark: SparkSession,
      snapshot: org.apache.spark.sql.delta.Snapshot,
      expectedDir: Path): Unit = {
    val path = expectedDir.resolve("expected_add_files")
    if (path.toFile.exists()) FileUtils.deleteDirectory(path.toFile)
    val addFilesJson = snapshot.allFiles.collect().map(_.json)
    if (addFilesJson.nonEmpty) {
      val df = spark.createDataset(addFilesJson)(
        org.apache.spark.sql.Encoders.STRING).toDF("action")
      df.write.mode(SaveMode.Overwrite).parquet(path.toString)
    }
  }

  private def writeExpectedRemoveFiles(
      spark: SparkSession,
      tablePath: Path,
      version: Long,
      expectedDir: Path): Unit = {
    // Scan checkpoint file(s) for RemoveFile tombstones
    val path = expectedDir.resolve("expected_remove_files")
    if (path.toFile.exists()) FileUtils.deleteDirectory(path.toFile)

    val logDir = tablePath.resolve("_delta_log")
    val checkpointPattern = f"$version%020d.checkpoint"

    val stream = Files.list(logDir)
    val checkpointFiles = try {
      stream.iterator().asScala
        .filter(_.getFileName.toString.startsWith(checkpointPattern))
        .toSeq
    } finally { stream.close() }

    if (checkpointFiles.isEmpty) return

    // Read checkpoint parquet files and extract remove actions
    val removeJsons = checkpointFiles.flatMap { cpFile =>
      if (cpFile.toString.endsWith(".parquet")) {
        try {
          val df = spark.read.parquet(cpFile.toString)
          if (df.columns.contains("remove") && df.columns.contains("add")) {
            df.filter("remove is not null and add is null")
              .select("remove.*")
              .toJSON.collect().toSeq
          } else Seq.empty
        } catch { case _: Exception => Seq.empty }
      } else Seq.empty
    }

    if (removeJsons.nonEmpty) {
      val df = spark.createDataset(removeJsons)(
        org.apache.spark.sql.Encoders.STRING).toDF("action")
      df.write.mode(SaveMode.Overwrite).parquet(path.toString)
    }
  }

  /** Extract SetTransaction actions from delta log up to given version. */
  private def extractTxnActions(
      tablePath: Path, version: Long): Seq[java.util.LinkedHashMap[String, Any]] = {
    val logDir = tablePath.resolve("_delta_log")
    val txnState = new java.util.LinkedHashMap[String, java.util.LinkedHashMap[String, Any]]()

    val stream = Files.list(logDir)
    try {
      val commitFiles = stream.iterator().asScala
        .filter { p =>
          val name = p.getFileName.toString
          name.endsWith(".json") && !name.contains(".checkpoint.")
        }
        .toSeq.sortBy(_.getFileName.toString)
        .filter(_.getFileName.toString.stripSuffix(".json").toLong <= version)

      for (commitFile <- commitFiles) {
        val lines = new String(Files.readAllBytes(commitFile), "UTF-8").split("\n")
        for (line <- lines if line.contains("\"txn\"")) {
          try {
            val node = JsonUtil.mapper.readTree(line)
            val txnNode = node.get("txn")
            if (txnNode != null && txnNode.has("appId")) {
              val entry = new java.util.LinkedHashMap[String, Any]()
              entry.put("appId", txnNode.get("appId").asText())
              entry.put("version", txnNode.get("version").asLong())
              txnState.put(txnNode.get("appId").asText(), entry)
            }
          } catch { case _: Exception => }
        }
      }
    } finally { stream.close() }

    txnState.values().asScala.toSeq
  }

  /** Extract DomainMetadata actions from delta log up to given version. */
  private def extractDomainMetadata(
      tablePath: Path,
      version: Long): Seq[java.util.LinkedHashMap[String, Any]] = {
    val logDir = tablePath.resolve("_delta_log")
    val dmState = new java.util.LinkedHashMap[String, java.util.LinkedHashMap[String, Any]]()

    val stream = Files.list(logDir)
    try {
      val commitFiles = stream.iterator().asScala
        .filter { p =>
          val name = p.getFileName.toString
          name.endsWith(".json") && !name.contains(".checkpoint.")
        }
        .toSeq.sortBy(_.getFileName.toString)
        .filter(_.getFileName.toString.stripSuffix(".json").toLong <= version)

      for (commitFile <- commitFiles) {
        val lines = new String(Files.readAllBytes(commitFile), "UTF-8").split("\n")
        for (line <- lines if line.contains("\"domainMetadata\"")) {
          try {
            val node = JsonUtil.mapper.readTree(line)
            val dmNode = node.get("domainMetadata")
            if (dmNode != null && dmNode.has("domain")) {
              val domain = dmNode.get("domain").asText()
              val removed = dmNode.has("removed") && dmNode.get("removed").asBoolean()
              if (removed) {
                dmState.remove(domain)
              } else {
                val entry = new java.util.LinkedHashMap[String, Any]()
                entry.put("domain", domain)
                if (dmNode.has("configuration")) {
                  entry.put("configuration", dmNode.get("configuration").asText())
                }
                dmState.put(domain, entry)
              }
            }
          } catch { case _: Exception => }
        }
      }
    } finally { stream.close() }

    dmState.values().asScala.toSeq
  }

  /** Validate a captured checkpoint spec by re-loading and comparing. */
  private[workload] def validate(
      spark: SparkSession,
      specName: String,
      tablePath: Path,
      specsDir: Path,
      version: Long): Unit = {
    val specFile = specsDir.resolve(s"$specName.json")
    require(Files.exists(specFile), s"Checkpoint spec file missing after capture: $specFile")
    val spec = JsonUtil.mapper.readTree(Files.readAllBytes(specFile))

    DeltaLog.clearCache()
    val dl = DeltaLog.forTable(spark, tablePath.toString)
    val snapshot = dl.getSnapshotAt(version)

    require(spec.has("expected"), s"Checkpoint spec $specName missing 'expected' block")
    val expected = spec.get("expected")

    // Validate protocol
    if (expected.has("protocol")) {
      val expectedProto = JsonUtil.mapper.readTree(
        JsonUtil.mapper.writeValueAsString(expected.get("protocol")))
      val actualProto = JsonUtil.mapper.readTree(
        JsonUtil.mapper.writeValueAsString(
          JsonUtil.mapper.treeToValue(
            JsonUtil.mapper.readTree(snapshot.protocol.json).get("protocol"), classOf[Any])))
      require(expectedProto.equals(actualProto),
        s"Checkpoint validation failed for $specName: protocol mismatch")
    }

    // Validate metadata
    if (expected.has("metadata")) {
      val expectedMeta = JsonUtil.mapper.readTree(
        JsonUtil.mapper.writeValueAsString(expected.get("metadata")))
      val actualMeta = JsonUtil.mapper.readTree(
        JsonUtil.mapper.writeValueAsString(
          JsonUtil.mapper.treeToValue(
            JsonUtil.mapper.readTree(snapshot.metadata.json).get("metaData"), classOf[Any])))
      require(expectedMeta.equals(actualMeta),
        s"Checkpoint validation failed for $specName: metadata mismatch")
    }
  }
}
