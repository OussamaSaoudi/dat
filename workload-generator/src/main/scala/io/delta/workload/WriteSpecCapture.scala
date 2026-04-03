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

import scala.collection.mutable
import scala.jdk.CollectionConverters._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.DeltaLog

/** Column definition for createTableOp. */
case class Col(name: String, dataType: String, nullable: Boolean = true)

/**
 * Records structured write operations during body execution.
 * Does NOT capture data files yet — that happens in buildSpec() after table copy.
 */
class WriteSpecBuilder {
  case class OpRecord(
      operation: String,
      fields: java.util.LinkedHashMap[String, Any])

  private val ops = mutable.ArrayBuffer[OpRecord]()

  def recordCreateTable(
      schema: Seq[Col],
      properties: Map[String, String],
      partitionColumns: Seq[String]): Unit = {
    val fields = new java.util.LinkedHashMap[String, Any]()
    // Schema will be replaced with canonical form from delta log in buildSpec
    val schemaFields = new java.util.ArrayList[Any]()
    schema.foreach { col =>
      val f = new java.util.LinkedHashMap[String, Any]()
      f.put("name", col.name)
      f.put("type", col.dataType.toLowerCase)
      f.put("nullable", col.nullable)
      f.put("metadata", new java.util.LinkedHashMap[String, Any]())
      schemaFields.add(f)
    }
    val schemaObj = new java.util.LinkedHashMap[String, Any]()
    schemaObj.put("type", "struct")
    schemaObj.put("fields", schemaFields)
    fields.put("schema", schemaObj)
    if (partitionColumns.nonEmpty) fields.put("partitionColumns", partitionColumns.asJava)
    if (properties.nonEmpty) fields.put("properties", properties.asJava)
    ops += OpRecord("create_table", fields)
  }

  def recordInsert(): Unit = {
    // Data files captured later from delta log
    ops += OpRecord("insert", new java.util.LinkedHashMap[String, Any]())
  }

  def recordDelete(predicate: String): Unit = {
    val fields = new java.util.LinkedHashMap[String, Any]()
    fields.put("predicate", predicate)
    ops += OpRecord("delete", fields)
  }

  def recordUpdate(predicate: String, set: Map[String, String]): Unit = {
    val fields = new java.util.LinkedHashMap[String, Any]()
    fields.put("predicate", predicate)
    fields.put("set", set.asJava)
    ops += OpRecord("update", fields)
  }

  def recordSetProperties(properties: Map[String, String]): Unit = {
    val fields = new java.util.LinkedHashMap[String, Any]()
    fields.put("setProperties", properties.asJava)
    ops += OpRecord("alter_table", fields)
  }

  def recordAddColumn(col: Col): Unit = {
    val fields = new java.util.LinkedHashMap[String, Any]()
    val colMap = new java.util.LinkedHashMap[String, Any]()
    colMap.put("name", col.name)
    colMap.put("type", col.dataType.toLowerCase)
    colMap.put("nullable", col.nullable)
    fields.put("addColumns", java.util.Collections.singletonList(colMap))
    ops += OpRecord("alter_table", fields)
  }

  def recordTruncate(): Unit = {
    ops += OpRecord("truncate", new java.util.LinkedHashMap[String, Any]())
  }

  /**
   * Build write_spec.json from recorded operations + delta log data files.
   *
   * Called after table copy, so tablePath is the copied table in the output dir.
   * For each commit that has AddFile actions, copies the data files to data/commit_N/.
   */
  def buildSpec(
      spark: SparkSession,
      tablePath: Path,
      outputDir: Path,
      specsDir: Path): Unit = {
    DeltaLog.clearCache()
    val dl = DeltaLog.forTable(spark, tablePath.toString)
    val latestVersion = dl.update().version

    val dataDir = outputDir.resolve("data")

    val commits = new java.util.ArrayList[Any]()
    for ((op, version) <- ops.zipWithIndex) {
      val commit = new java.util.LinkedHashMap[String, Any]()
      commit.put("operation", op.operation)
      op.fields.forEach { (k, v) => commit.put(k, v) }

      // For commits that produce data files (create_table with data, insert),
      // capture the parquet files from the delta log
      if (version <= latestVersion) {
        val dataFiles = captureDataFiles(tablePath, version.toLong, dataDir)
        if (dataFiles.nonEmpty) {
          commit.put("dataFiles", dataFiles.asJava)
        }
      }

      // For create_table, replace schema with canonical form from delta log
      if (op.operation == "create_table") {
        val snapshot = dl.getSnapshotAt(version.toLong)
        commit.put("schema",
          JsonUtil.mapper.readValue(snapshot.metadata.schemaString, classOf[Any]))
      }

      commits.add(commit)
    }

    // Verification list
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
    writeSpec.put("commits", commits)
    writeSpec.put("verification", verification)

    JsonUtil.writeJson(outputDir.resolve("write_spec.json"), writeSpec)
  }

  /** Capture data files added in a specific commit version. */
  def captureDataFiles(tablePath: Path, version: Long, dataDir: Path): Seq[String] = {
    val commitFile = tablePath.resolve("_delta_log").resolve(f"$version%020d.json")
    if (!Files.exists(commitFile)) return Seq.empty

    val lines = new String(Files.readAllBytes(commitFile), "UTF-8")
      .split("\n").filter(_.trim.nonEmpty)

    val addPaths = lines.flatMap { line =>
      val node = JsonUtil.mapper.readTree(line)
      if (node.has("add")) Some(node.get("add").get("path").asText())
      else None
    }

    if (addPaths.isEmpty) return Seq.empty

    val commitDataDir = dataDir.resolve(s"commit_$version")
    Files.createDirectories(commitDataDir)

    addPaths.map { relativePath =>
      val sourceFile = tablePath.resolve(relativePath)
      val fileName = sourceFile.getFileName.toString
      if (Files.exists(sourceFile)) {
        Files.copy(sourceFile, commitDataDir.resolve(fileName),
          StandardCopyOption.REPLACE_EXISTING)
      }
      s"data/commit_$version/$fileName"
    }.toSeq
  }
}

/** Static methods for building write specs directly from the delta log. */
object WriteSpecCapture {

  /**
   * Build write_spec.json by analyzing the delta log commit-by-commit.
   * Used when no structured ops were recorded (the "auto" path).
   * Reads commitInfo.operation from each commit to determine the operation type,
   * and captures data files for each commit that has AddFile actions.
   *
   * @param sqlStatements Optional list of SQL statements executed (for the sql field)
   */
  def buildSpecFromLog(
      spark: SparkSession,
      tablePath: Path,
      outputDir: Path,
      specsDir: Path,
      sqlStatements: Seq[String] = Seq.empty): Unit = {
    DeltaLog.clearCache()
    val dl = DeltaLog.forTable(spark, tablePath.toString)
    val latestVersion = dl.update().version

    val dataDir = outputDir.resolve("data")
    val commits = new java.util.ArrayList[Any]()
    val builder = new WriteSpecBuilder()

    for (version <- 0L to latestVersion) {
      val commitFile = tablePath.resolve("_delta_log").resolve(f"$version%020d.json")
      if (Files.exists(commitFile)) {
        val lines = new String(Files.readAllBytes(commitFile), "UTF-8")
          .split("\n").filter(_.trim.nonEmpty)

        // Extract operation from commitInfo
        val operation = lines.flatMap { line =>
          val node = JsonUtil.mapper.readTree(line)
          if (node.has("commitInfo")) {
            val ci = node.get("commitInfo")
            if (ci.has("operation")) Some(ci.get("operation").asText()) else None
          } else None
        }.headOption.getOrElse("UNKNOWN")

        // Map Spark operation names to write spec operation types
        val opType = operation match {
          case "CREATE TABLE" | "CREATE TABLE AS SELECT" | "CREATE OR REPLACE TABLE" =>
            "create_table"
          case "WRITE" | "APPEND" | "Append" => "insert"
          case "DELETE" => "delete"
          case "UPDATE" => "update"
          case "MERGE" => "merge"
          case "TRUNCATE" => "truncate"
          case "SET TBLPROPERTIES" | "CHANGE COLUMN" | "ADD COLUMNS" |
               "RENAME COLUMN" | "DROP COLUMNS" | "UPGRADE PROTOCOL" =>
            "alter_table"
          case "OPTIMIZE" => "optimize"
          case "RESTORE" => "restore"
          case "VACUUM START" | "VACUUM END" => "vacuum"
          case "CHECKPOINT" => "checkpoint"
          case "REPLACE WHERE" => "insert_overwrite"
          case "OVERWRITE" => "insert_overwrite"
          case _ => operation.toLowerCase.replaceAll("\\s+", "_")
        }

        val commit = new java.util.LinkedHashMap[String, Any]()
        commit.put("operation", opType)

        // Add SQL if we have it (matched by index)
        if (version < sqlStatements.length) {
          commit.put("sql", sqlStatements(version.toInt))
        }

        // For create_table, capture schema from snapshot
        if (opType == "create_table") {
          try {
            val snapshot = dl.getSnapshotAt(version)
            commit.put("schema",
              JsonUtil.mapper.readValue(snapshot.metadata.schemaString, classOf[Any]))
            val partCols = snapshot.metadata.partitionColumns
            if (partCols.nonEmpty) {
              commit.put("partitionColumns",
                scala.collection.JavaConverters.seqAsJavaListConverter(partCols).asJava)
            }
            val props = snapshot.metadata.configuration
            if (props.nonEmpty) {
              commit.put("properties",
                scala.collection.JavaConverters.mapAsJavaMapConverter(props).asJava)
            }
          } catch { case _: Exception => }
        }

        // Extract predicates from commitInfo.operationParameters
        lines.foreach { line =>
          val node = JsonUtil.mapper.readTree(line)
          if (node.has("commitInfo")) {
            val ci = node.get("commitInfo")
            if (ci.has("operationParameters")) {
              val params = ci.get("operationParameters")
              if (params.has("predicate") && !params.get("predicate").isNull) {
                val pred = params.get("predicate").asText()
                if (pred.nonEmpty && pred != "[]" && pred != "true") {
                  commit.put("predicate", pred)
                }
              }
            }
          }
        }

        // Capture data files
        val dataFiles = builder.captureDataFiles(tablePath, version, dataDir)
        if (dataFiles.nonEmpty) {
          commit.put("dataFiles",
            scala.collection.JavaConverters.seqAsJavaListConverter(dataFiles).asJava)
        }

        commits.add(commit)
      }
    }

    // Verification list from specs directory
    val verification = new java.util.ArrayList[String]()
    if (Files.exists(specsDir)) {
      val stream = Files.list(specsDir)
      try {
        scala.collection.JavaConverters.asScalaIteratorConverter(stream.iterator()).asScala
          .filter(_.toString.endsWith(".json"))
          .map(_.getFileName.toString)
          .toSeq.sorted
          .foreach(verification.add)
      } finally { stream.close() }
    }

    val writeSpec = new java.util.LinkedHashMap[String, Any]()
    writeSpec.put("type", "write")
    writeSpec.put("commits", commits)
    writeSpec.put("verification", verification)

    JsonUtil.writeJson(outputDir.resolve("write_spec.json"), writeSpec)
  }
}
