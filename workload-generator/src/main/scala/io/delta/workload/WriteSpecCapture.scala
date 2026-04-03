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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.DeltaLog

/**
 * Captures a write spec from an existing Delta table's commit history.
 *
 * Analyzes each commit in the delta log, extracts the operation type,
 * copies referenced data files, and produces a write_spec.json that
 * another engine can replay to produce the same table.
 */
object WriteSpecCapture {

  /** Map from Delta's commitInfo.operation to our operation types. */
  private val operationMap = Map(
    "CREATE TABLE" -> "create_table",
    "CREATE TABLE AS SELECT" -> "create_table",
    "CREATE OR REPLACE TABLE" -> "replace_table",
    "CREATE OR REPLACE TABLE AS SELECT" -> "replace_table",
    "REPLACE TABLE" -> "replace_table",
    "REPLACE TABLE AS SELECT" -> "replace_table",
    "WRITE" -> "insert",
    "STREAMING UPDATE" -> "insert",
    "DELETE" -> "delete",
    "UPDATE" -> "update",
    "TRUNCATE" -> "truncate",
    "SET TBLPROPERTIES" -> "alter_table",
    "UNSET TBLPROPERTIES" -> "alter_table",
    "ADD COLUMNS" -> "alter_table",
    "RENAME COLUMN" -> "alter_table",
    "DROP COLUMNS" -> "alter_table",
    "CHANGE COLUMN" -> "alter_table",
    "UPGRADE PROTOCOL" -> "alter_table",
    "ManualUpdate" -> "insert",
    "Convert" -> "create_table"
  )

  /**
   * Capture a write spec from a Delta table's commit history.
   *
   * @param spark     SparkSession
   * @param tablePath Path to the Delta table (the copied version in the output dir)
   * @param outputDir The workload output directory (parent of delta/)
   * @param testId    Test identifier for naming
   */
  def capture(
      spark: SparkSession,
      tablePath: Path,
      outputDir: Path,
      testId: String): Unit = {
    DeltaLog.clearCache()
    val deltaLog = DeltaLog.forTable(spark, tablePath.toString)
    val latestVersion = deltaLog.update().version

    val dataDir = outputDir.resolve("data")
    Files.createDirectories(dataDir)

    val commits = new java.util.ArrayList[Any]()

    for (version <- 0L to latestVersion) {
      val commitFile = tablePath.resolve("_delta_log")
        .resolve(f"$version%020d.json")
      if (Files.exists(commitFile)) {
        val commit = captureCommit(tablePath, commitFile, version, dataDir)
        if (commit != null) commits.add(commit)
      }
    }

    // Build verification list from existing spec files
    val specsDir = outputDir.resolve("specs")
    val verification = new java.util.ArrayList[String]()
    if (Files.exists(specsDir)) {
      Files.list(specsDir).iterator().asScala
        .filter(_.toString.endsWith(".json"))
        .map(_.getFileName.toString)
        .toSeq.sorted
        .foreach(verification.add)
    }

    val writeSpec = new java.util.LinkedHashMap[String, Any]()
    writeSpec.put("type", "write")
    writeSpec.put("description", testId)
    writeSpec.put("commits", commits)
    writeSpec.put("verification", verification)

    JsonUtil.writeJson(outputDir.resolve("write_spec.json"), writeSpec)
  }

  /** Analyze a single commit file and produce a commit spec object. */
  private def captureCommit(
      tablePath: Path,
      commitFile: Path,
      version: Long,
      dataDir: Path): java.util.LinkedHashMap[String, Any] = {
    val lines = new String(Files.readAllBytes(commitFile), "UTF-8")
      .split("\n").filter(_.trim.nonEmpty)

    // Parse all actions
    var commitInfo: com.fasterxml.jackson.databind.JsonNode = null
    var protocol: com.fasterxml.jackson.databind.JsonNode = null
    var metadata: com.fasterxml.jackson.databind.JsonNode = null
    val addFiles = new java.util.ArrayList[String]()
    val removeFiles = new java.util.ArrayList[String]()

    for (line <- lines) {
      val node = JsonUtil.mapper.readTree(line)
      if (node.has("commitInfo")) {
        commitInfo = node.get("commitInfo")
      } else if (node.has("protocol")) {
        protocol = node.get("protocol")
      } else if (node.has("metaData")) {
        metadata = node.get("metaData")
      } else if (node.has("add")) {
        val path = node.get("add").get("path").asText()
        addFiles.add(path)
      } else if (node.has("remove")) {
        val path = node.get("remove").get("path").asText()
        removeFiles.add(path)
      }
    }

    if (commitInfo == null) return null

    // Determine operation type
    val deltaOp = commitInfo.get("operation").asText()
    val operation = operationMap.getOrElse(deltaOp, "unknown")

    // Copy data files for this commit
    val commitDataDir = dataDir.resolve(s"commit_$version")
    if (!addFiles.isEmpty) {
      Files.createDirectories(commitDataDir)
      addFiles.asScala.foreach { relativePath =>
        val sourceFile = tablePath.resolve(relativePath)
        if (Files.exists(sourceFile)) {
          val destFile = commitDataDir.resolve(
            sourceFile.getFileName.toString)
          Files.copy(sourceFile, destFile, StandardCopyOption.REPLACE_EXISTING)
        }
      }
    }

    // Build commit spec
    val commit = new java.util.LinkedHashMap[String, Any]()
    commit.put("operation", operation)

    // Add operation-specific fields
    operation match {
      case "create_table" | "replace_table" =>
        if (metadata != null) {
          commit.put("schema",
            JsonUtil.mapper.readValue(
              metadata.get("schemaString").asText(), classOf[Any]))
          val partCols = metadata.get("partitionColumns")
          if (partCols != null && partCols.size() > 0) {
            commit.put("partitionColumns",
              JsonUtil.mapper.treeToValue(partCols, classOf[Any]))
          }
          val config = metadata.get("configuration")
          if (config != null && config.size() > 0) {
            commit.put("properties",
              JsonUtil.mapper.treeToValue(config, classOf[Any]))
          }
        }
        if (!addFiles.isEmpty) {
          commit.put("dataFiles", dataFilePaths(version, addFiles))
        }

      case "insert" =>
        if (!addFiles.isEmpty) {
          commit.put("dataFiles", dataFilePaths(version, addFiles))
        }
        // Detect INSERT OVERWRITE
        if (!removeFiles.isEmpty) {
          commit.put("operation", "insert_overwrite")
          val params = commitInfo.get("operationParameters")
          if (params != null && params.has("predicate")) {
            val pred = params.get("predicate").asText()
            if (pred != "[]" && pred.nonEmpty) {
              commit.put("predicate", pred)
            }
          }
        }

      case "delete" =>
        val params = commitInfo.get("operationParameters")
        if (params != null && params.has("predicate")) {
          commit.put("predicate",
            cleanPredicate(params.get("predicate").asText()))
        }

      case "update" =>
        val params = commitInfo.get("operationParameters")
        if (params != null) {
          if (params.has("predicate")) {
            commit.put("predicate",
              cleanPredicate(params.get("predicate").asText()))
          }
          // SET expressions are in operationParameters but format varies
        }

      case "truncate" =>
        // No additional fields needed

      case "alter_table" =>
        if (metadata != null) {
          val config = metadata.get("configuration")
          if (config != null && config.size() > 0) {
            commit.put("setProperties",
              JsonUtil.mapper.treeToValue(config, classOf[Any]))
          }
          commit.put("schema",
            JsonUtil.mapper.readValue(
              metadata.get("schemaString").asText(), classOf[Any]))
        }
        if (protocol != null) {
          val protoMap = new java.util.LinkedHashMap[String, Any]()
          protoMap.put("minReaderVersion",
            protocol.get("minReaderVersion").asInt())
          protoMap.put("minWriterVersion",
            protocol.get("minWriterVersion").asInt())
          if (protocol.has("readerFeatures")) {
            protoMap.put("readerFeatures",
              JsonUtil.mapper.treeToValue(
                protocol.get("readerFeatures"), classOf[Any]))
          }
          if (protocol.has("writerFeatures")) {
            protoMap.put("writerFeatures",
              JsonUtil.mapper.treeToValue(
                protocol.get("writerFeatures"), classOf[Any]))
          }
          commit.put("protocol", protoMap)
        }

      case _ => // unknown operation — include raw info
        commit.put("rawOperation", deltaOp)
    }

    // Add SQL hint from commitInfo
    val sql = extractSql(commitInfo, deltaOp)
    if (sql != null) commit.put("sql", sql)

    commit
  }

  /** Build relative data file paths for a commit's added files. */
  private def dataFilePaths(
      version: Long,
      addFiles: java.util.ArrayList[String]): java.util.ArrayList[String] = {
    val paths = new java.util.ArrayList[String]()
    addFiles.asScala.foreach { relativePath =>
      val fileName = java.nio.file.Paths.get(relativePath).getFileName.toString
      paths.add(s"data/commit_$version/$fileName")
    }
    paths
  }

  /** Clean up predicate strings from Delta's internal format. */
  private def cleanPredicate(pred: String): String = {
    // Delta stores predicates in various formats — clean common patterns
    pred.stripPrefix("[").stripSuffix("]")
      .replace("\"", "")
      .trim
  }

  /** Extract SQL hint from commitInfo. */
  private def extractSql(
      commitInfo: com.fasterxml.jackson.databind.JsonNode,
      operation: String): String = {
    // commitInfo doesn't always have the original SQL
    // We construct a hint from the operation and parameters
    null // SQL hints are populated by the generator, not from the log
  }
}
