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

object TableInfoWriter {

  def write(
      spark: SparkSession,
      tablePath: Path,
      outputDir: Path,
      name: String,
      description: String,
      tags: Seq[String] = Seq.empty): Unit = {
    try {
      val deltaLog = DeltaLog.forTable(spark, tablePath.toString)
      val snapshot = deltaLog.update()

      val schemaObj = JsonUtil.mapper.readValue(snapshot.metadata.schemaString, classOf[Any])

      val readerFeatures = snapshot.protocol.readerFeatureNames
      val writerFeatures = snapshot.protocol.writerFeatureNames
      val protocol = ProtocolInfo(
        minReaderVersion = snapshot.protocol.minReaderVersion,
        minWriterVersion = snapshot.protocol.minWriterVersion,
        readerFeatures = if (readerFeatures.nonEmpty) Some(readerFeatures.toSeq.sorted) else None,
        writerFeatures = if (writerFeatures.nonEmpty) Some(writerFeatures.toSeq.sorted) else None)

      val logSegment = snapshot.logSegment

      val numActions = try {
        val deltaLogDir = tablePath.resolve("_delta_log")
        val stream = Files.list(deltaLogDir)
        try {
          stream.iterator().asScala
            .filter(p => p.toString.endsWith(".json") && !p.toString.contains("checkpoint"))
            .map(p => Files.readAllLines(p).size().toLong)
            .sum
        } finally { stream.close() }
      } catch { case _: Exception => 0L }

      val lastCrcVersion = try {
        val deltaLogDir = tablePath.resolve("_delta_log")
        val stream = Files.list(deltaLogDir)
        try {
          stream.iterator().asScala
            .map(_.getFileName.toString)
            .filter(_.endsWith(".crc"))
            .flatMap { n =>
              try { Some(n.stripSuffix(".crc").toLong) }
              catch { case _: NumberFormatException => None }
            }
            .toSeq.sorted.lastOption.getOrElse(-1L)
        } finally { stream.close() }
      } catch { case _: Exception => -1L }

      val numCheckpointFiles = try {
        logSegment.checkpointProvider.topLevelFiles.size
      } catch { case _: Exception => 0 }

      val logInfo = LogInfo(
        numAddFiles = snapshot.numOfFiles,
        numRemoveFiles = snapshot.numOfRemoves,
        sizeInBytes = snapshot.sizeInBytes,
        numCommits = logSegment.deltas.size,
        numActions = numActions,
        lastCheckpointVersion = logSegment.checkpointProvider.version,
        lastCrcVersion = lastCrcVersion,
        numCheckpointFiles = numCheckpointFiles)

      val partCols = snapshot.metadata.partitionColumns
      val numDistinctPartitions = if (partCols.nonEmpty) {
        try {
          snapshot.allFiles.select("partitionValues").distinct().count()
        } catch { case _: Exception => 0L }
      } else 0L

      val dataLayout = DataLayoutInfo(
        numClusteringColumns = 0, // Clustering is not available in OSS Delta
        numPartitionColumns = partCols.size,
        numDistinctPartitions = numDistinctPartitions)

      val tableInfo = TableInfo(
        name = name,
        description = description,
        schema = schemaObj,
        protocol = protocol,
        logInfo = logInfo,
        properties = snapshot.metadata.configuration,
        dataLayout = dataLayout,
        tags = if (tags.nonEmpty) Some(tags) else None)

      JsonUtil.writeSpec(outputDir.resolve("table_info.json"), tableInfo)
    } catch {
      case e: Exception =>
        System.err.println(s"WARN: Could not write full table_info.json: ${e.getMessage}")
        // Write minimal table_info.json so skip-on-rerun sentinel exists
        try {
          val minimal = MinimalTableInfo(
            name = name,
            description = description,
            error = s"Metadata scan failed: ${e.getMessage}")
          JsonUtil.writeSpec(outputDir.resolve("table_info.json"), minimal)
        } catch { case _: Exception => }
    }
  }
}
