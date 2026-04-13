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

import java.nio.file.{Files, Path, StandardCopyOption}

import scala.jdk.CollectionConverters._

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.actions.{DomainMetadata => DeltaDomainMetadata}

/**
 * Captures checkpoint verification specs.
 *
 * Uses DeltaLog API to extract checkpoint state and validates by comparing
 * against re-read snapshot data.
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
    val expectedDir = outputDir.resolve("expected").resolve(specName)
    Files.createDirectories(expectedDir)

    DeltaLog.clearCache()
    val dl = DeltaLog.forTable(spark, tablePath.toString)
    val snapshot = dl.getSnapshotAt(version)

    // Extract protocol
    val protocol = ProtocolInfo(
      minReaderVersion = snapshot.protocol.minReaderVersion,
      minWriterVersion = snapshot.protocol.minWriterVersion,
      readerFeatures = snapshot.protocol.readerFeatures.map(_.toSeq.sorted),
      writerFeatures = snapshot.protocol.writerFeatures.map(_.toSeq.sorted)
    )

    // Extract metadata
    val metadata = JsonUtil.mapper.treeToValue(
      JsonUtil.mapper.readTree(snapshot.metadata.json).get("metaData"), classOf[Any])

    // Extract txn actions
    val txnActions = snapshot.setTransactions.map { txn =>
      TxnAction(txn.appId, txn.version)
    }

    // Extract domain metadata
    val domainMetadataActions = try {
      snapshot.domainMetadata.map { dm =>
        DomainMetadataEntry(dm.domain, Option(dm.configuration))
      }
    } catch {
      case _: Exception => Seq.empty // domainMetadata may not exist in older versions
    }

    // Copy checkpoint files to expected_checkpoint directory
    val checkpointDir = expectedDir.resolve("expected_checkpoint")
    Files.createDirectories(checkpointDir)
    copyCheckpointFiles(tablePath, version, checkpointDir)

    // Save expected data (scan at checkpoint version)
    val df = spark.read.format("delta").option("versionAsOf", version).load(tablePath.toString)
    val expectedDataPath = expectedDir.resolve("expected_data")
    if (expectedDataPath.toFile.exists()) FileUtils.deleteDirectory(expectedDataPath.toFile)
    df.write.mode(SaveMode.Overwrite).parquet(expectedDataPath.toString)

    // Save expected metadata (AddFile actions)
    val addFilesJson = snapshot.allFiles.collect().map(_.json).toSeq
    val expectedMetaPath = expectedDir.resolve("expected_metadata")
    if (expectedMetaPath.toFile.exists()) FileUtils.deleteDirectory(expectedMetaPath.toFile)
    if (addFilesJson.nonEmpty) {
      spark.createDataset(addFilesJson)(org.apache.spark.sql.Encoders.STRING)
        .toDF("action").write.mode(SaveMode.Overwrite).parquet(expectedMetaPath.toString)
    }

    // Build expected checkpoint state
    val expected = CheckpointExpected(
      protocol = protocol,
      metadata = metadata,
      txn = if (txnActions.nonEmpty) Some(txnActions) else None,
      domainMetadata = if (domainMetadataActions.nonEmpty) Some(domainMetadataActions) else None
    )

    val spec = CheckpointSpec(version, expected)
    JsonUtil.writeSpec(specsDir.resolve(s"$specName.json"), spec)

    // Validate
    validate(spark, specName, tablePath, outputDir, specsDir, version)

    val addCount = snapshot.allFiles.count()
    println(s"  Checkpoint spec captured: $specName (version=$version, $addCount adds)")
  }

  private def copyCheckpointFiles(tablePath: Path, version: Long, destDir: Path): Unit = {
    val logDir = tablePath.resolve("_delta_log")
    val checkpointPrefix = f"$version%020d.checkpoint"

    val stream = Files.list(logDir)
    try {
      stream.iterator().asScala
        .filter(_.getFileName.toString.startsWith(checkpointPrefix))
        .foreach { src =>
          val dest = destDir.resolve(src.getFileName)
          Files.copy(src, dest, StandardCopyOption.REPLACE_EXISTING)
        }
    } finally { stream.close() }
  }

  private[workload] def validate(
      spark: SparkSession,
      specName: String,
      tablePath: Path,
      outputDir: Path,
      specsDir: Path,
      version: Long): Unit = {

    val specFile = specsDir.resolve(s"$specName.json")
    require(Files.exists(specFile), s"Checkpoint spec file missing: $specFile")
    val spec = JsonUtil.mapper.readValue(Files.readAllBytes(specFile), classOf[CheckpointSpec])

    DeltaLog.clearCache()
    val dl = DeltaLog.forTable(spark, tablePath.toString)
    val snapshot = dl.getSnapshotAt(version)

    // Validate protocol
    val actualProtocol = ProtocolInfo(
      minReaderVersion = snapshot.protocol.minReaderVersion,
      minWriterVersion = snapshot.protocol.minWriterVersion,
      readerFeatures = snapshot.protocol.readerFeatures.map(_.toSeq.sorted),
      writerFeatures = snapshot.protocol.writerFeatures.map(_.toSeq.sorted)
    )
    require(spec.expected.protocol == actualProtocol,
      s"Checkpoint validation failed for $specName: protocol mismatch")

    // Validate metadata
    val actualMetadata = JsonUtil.mapper.treeToValue(
      JsonUtil.mapper.readTree(snapshot.metadata.json).get("metaData"), classOf[Any])
    val expectedMetaJson = JsonUtil.mapper.writeValueAsString(spec.expected.metadata)
    val actualMetaJson = JsonUtil.mapper.writeValueAsString(actualMetadata)
    require(expectedMetaJson == actualMetaJson,
      s"Checkpoint validation failed for $specName: metadata mismatch")

    // Validate txn actions
    val actualTxn = snapshot.setTransactions.map { txn =>
      TxnAction(txn.appId, txn.version)
    }.toSet
    val expectedTxn = spec.expected.txn.getOrElse(Seq.empty).toSet
    require(expectedTxn == actualTxn,
      s"Checkpoint validation failed for $specName: txn mismatch " +
      s"(expected ${expectedTxn.size}, actual ${actualTxn.size})")

    // Validate domain metadata
    val actualDm = try {
      snapshot.domainMetadata.map { dm =>
        DomainMetadataEntry(dm.domain, Option(dm.configuration))
      }.toSet
    } catch {
      case _: Exception => Set.empty[DomainMetadataEntry]
    }
    val expectedDm = spec.expected.domainMetadata.getOrElse(Seq.empty).toSet
    require(expectedDm == actualDm,
      s"Checkpoint validation failed for $specName: domainMetadata mismatch " +
      s"(expected ${expectedDm.size}, actual ${actualDm.size})")

    // Validate expected data (row-level comparison)
    val expectedDir = outputDir.resolve("expected").resolve(specName)
    val expectedDataPath = expectedDir.resolve("expected_data")
    if (Files.exists(expectedDataPath)) {
      val expectedDf = spark.read.parquet(expectedDataPath.toString)
      val actualDf = spark.read.format("delta")
        .option("versionAsOf", version).load(tablePath.toString)
      val cols = expectedDf.columns.sorted
      val expectedSorted = expectedDf.select(cols.head, cols.tail: _*)
        .sort(cols.head, cols.tail: _*)
      val actualSorted = actualDf.select(cols.head, cols.tail: _*)
        .sort(cols.head, cols.tail: _*)
      require(expectedSorted.except(actualSorted).count() == 0 &&
        actualSorted.except(expectedSorted).count() == 0,
        s"Checkpoint validation failed for $specName: expected_data row mismatch")
    }
  }
}
