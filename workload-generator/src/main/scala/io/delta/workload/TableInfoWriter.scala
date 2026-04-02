/*
 * Copyright (2024) The Delta Lake Project Authors.
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
      DeltaLog.clearCache()
      val deltaLog = DeltaLog.forTable(spark, tablePath.toString)
      val snapshot = deltaLog.update()

      val tableInfo = new java.util.LinkedHashMap[String, Any]()
      tableInfo.put("name", name)
      tableInfo.put("description", description)

      val schemaObj = JsonUtil.mapper.readValue(snapshot.metadata.schemaString, classOf[Any])
      tableInfo.put("schema", schemaObj)

      val protocolInfo = new java.util.LinkedHashMap[String, Any]()
      protocolInfo.put("minReaderVersion",
        snapshot.protocol.minReaderVersion)
      protocolInfo.put("minWriterVersion",
        snapshot.protocol.minWriterVersion)
      val readerFeatures = snapshot.protocol.readerFeatureNames
      if (readerFeatures.nonEmpty) {
        protocolInfo.put("readerFeatures", readerFeatures.toSeq.sorted.asJava)
      }
      val writerFeatures = snapshot.protocol.writerFeatureNames
      if (writerFeatures.nonEmpty) {
        protocolInfo.put("writerFeatures", writerFeatures.toSeq.sorted.asJava)
      }
      tableInfo.put("protocol", protocolInfo)

      val logInfoMap = new java.util.LinkedHashMap[String, Any]()
      logInfoMap.put("numAddFiles", snapshot.numOfFiles)
      logInfoMap.put("numRemoveFiles", snapshot.numOfRemoves)
      logInfoMap.put("sizeInBytes", snapshot.sizeInBytes)

      val logSegment = snapshot.logSegment
      logInfoMap.put("numCommits", logSegment.deltas.size)

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
      logInfoMap.put("numActions", numActions)

      val lastCheckpointVersion = logSegment.checkpointProvider.version
      logInfoMap.put("lastCheckpointVersion", lastCheckpointVersion)

      val lastCrcVersion = try {
        val deltaLogDir = tablePath.resolve("_delta_log")
        val stream = Files.list(deltaLogDir)
        try {
          stream.iterator().asScala
            .map(_.getFileName.toString)
            .filter(_.endsWith(".crc"))
            .flatMap { name =>
              try { Some(name.stripSuffix(".crc").toLong) }
              catch { case _: NumberFormatException => None }
            }
            .toSeq.sorted.lastOption.getOrElse(-1L)
        } finally { stream.close() }
      } catch { case _: Exception => -1L }
      logInfoMap.put("lastCrcVersion", lastCrcVersion)

      val numCheckpointFiles = try {
        logSegment.checkpointProvider.topLevelFiles.size
      } catch { case _: Exception => 0 }
      logInfoMap.put("numCheckpointFiles", numCheckpointFiles)

      tableInfo.put("logInfo", logInfoMap)

      val properties = snapshot.metadata.configuration
      tableInfo.put("properties", properties.asJava)

      val dataLayout = new java.util.LinkedHashMap[String, Any]()
      val numClusteringColumns = 0 // Clustering is not available in OSS Delta
      dataLayout.put("numClusteringColumns", numClusteringColumns)

      val partCols = snapshot.metadata.partitionColumns
      dataLayout.put("numPartitionColumns", partCols.size)

      val numDistinctPartitions = if (partCols.nonEmpty) {
        try {
          snapshot.allFiles.select("partitionValues").distinct().count()
        } catch { case _: Exception => 0L }
      } else 0L
      dataLayout.put("numDistinctPartitions", numDistinctPartitions)
      tableInfo.put("dataLayout", dataLayout)

      if (tags.nonEmpty) {
        tableInfo.put("tags", tags.asJava)
      }

      JsonUtil.writeJson(outputDir.resolve("table_info.json"), tableInfo)
    } catch {
      case e: Exception =>
        System.err.println(s"WARN: Could not write table_info.json: ${e.getMessage}")
    }
  }
}
