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

  def recordUpdateProperties(
      setProps: Map[String, String],
      removeProps: Seq[String] = Seq.empty): Unit = {
    val fields = new java.util.LinkedHashMap[String, Any]()
    if (setProps.nonEmpty) fields.put("set", setProps.asJava)
    if (removeProps.nonEmpty) fields.put("remove", removeProps.asJava)
    ops += OpRecord("update_properties", fields)
  }

  def recordEvolveSchema(
      addColumns: Seq[Col] = Seq.empty,
      renameColumns: Map[String, String] = Map.empty,
      dropColumns: Seq[String] = Seq.empty): Unit = {
    val fields = new java.util.LinkedHashMap[String, Any]()
    if (addColumns.nonEmpty) {
      val cols = new java.util.ArrayList[Any]()
      addColumns.foreach { col =>
        val colMap = new java.util.LinkedHashMap[String, Any]()
        colMap.put("name", col.name)
        colMap.put("type", col.dataType.toLowerCase)
        colMap.put("nullable", col.nullable)
        cols.add(colMap)
      }
      fields.put("addColumns", cols)
    }
    if (renameColumns.nonEmpty) fields.put("renameColumns", renameColumns.asJava)
    if (dropColumns.nonEmpty) fields.put("dropColumns", dropColumns.asJava)
    ops += OpRecord("evolve_schema", fields)
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
          case "CREATE TABLE" | "CREATE TABLE AS SELECT" => "create_table"
          case "CREATE OR REPLACE TABLE" => "replace_table"
          case "WRITE" | "APPEND" | "Append" => "insert"
          case "DELETE" => "delete"
          case "UPDATE" => "update"
          case "MERGE" => "merge"
          case "TRUNCATE" => "truncate"
          case "SET TBLPROPERTIES" | "UPGRADE PROTOCOL" => "update_properties"
          case "ADD COLUMNS" | "CHANGE COLUMN" | "RENAME COLUMN" | "DROP COLUMNS" =>
            "evolve_schema"
          case "RESTORE" => "restore"
          case "VACUUM START" | "VACUUM END" => "vacuum"
          case "REPLACE WHERE" => "insert_overwrite"
          case "OVERWRITE" => "insert_overwrite"
          case _ => operation.toLowerCase.replaceAll("\\s+", "_")
        }

        val commit = new java.util.LinkedHashMap[String, Any]()
        commit.put("operation", opType)

        // Extract commitInfo and operationParameters for structured fields
        val (commitInfoOpt, operationParams) = {
          var ci: com.fasterxml.jackson.databind.JsonNode = null
          var params: com.fasterxml.jackson.databind.JsonNode = null
          lines.foreach { line =>
            val node = JsonUtil.mapper.readTree(line)
            if (node.has("commitInfo")) {
              ci = node.get("commitInfo")
              if (ci.has("operationParameters")) params = ci.get("operationParameters")
            }
          }
          (Option(ci), Option(params))
        }

        // For create_table/replace_table, capture schema, partitioning/clustering, properties
        if (opType == "create_table" || opType == "replace_table") {
          try {
            val snapshot = dl.getSnapshotAt(version)
            commit.put("schema",
              JsonUtil.mapper.readValue(snapshot.metadata.schemaString, classOf[Any]))
            val partCols = snapshot.metadata.partitionColumns
            if (partCols.nonEmpty) {
              commit.put("partitionColumns", partCols.asJava)
            }
            // Check for clustering columns in domain metadata
            extractClusteringColumns(tablePath, version).foreach { cols =>
              if (cols.nonEmpty) commit.put("clusteringColumns", cols.asJava)
            }
            val props = snapshot.metadata.configuration
            if (props.nonEmpty) {
              commit.put("properties", props.asJava)
            }
          } catch { case _: Exception => }
        }

        // For evolve_schema, capture schema changes
        if (opType == "evolve_schema") {
          extractSchemaEvolution(dl, version, operation, operationParams).foreach {
            case (k, v) => commit.put(k, v)
          }
        }

        // For update_properties, capture property changes
        if (opType == "update_properties") {
          extractPropertyChanges(dl, version, operationParams).foreach {
            case (k, v) => commit.put(k, v)
          }
        }

        // Extract predicates from operationParameters
        operationParams.foreach { params =>
          if (params.has("predicate") && !params.get("predicate").isNull) {
            val pred = params.get("predicate").asText()
            if (pred.nonEmpty && pred != "[]" && pred != "true") {
              commit.put("predicate", pred)
            }
          }
        }

        // For update operations, extract SET clause from operationParameters
        if (opType == "update") {
          operationParams.foreach { params =>
            if (params.has("set") && !params.get("set").isNull) {
              val setNode = params.get("set")
              val setMap = new java.util.LinkedHashMap[String, String]()
              val iter = setNode.fields()
              while (iter.hasNext) {
                val entry = iter.next()
                setMap.put(entry.getKey, entry.getValue.asText())
              }
              if (!setMap.isEmpty) commit.put("set", setMap)
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

  /** Extract clustering columns from delta.clustering domain metadata at a version. */
  private def extractClusteringColumns(
      tablePath: Path, version: Long): Option[Seq[String]] = {
    val commitFile = tablePath.resolve("_delta_log").resolve(f"$version%020d.json")
    if (!Files.exists(commitFile)) return None
    val lines = new String(Files.readAllBytes(commitFile), "UTF-8")
      .split("\n").filter(_.trim.nonEmpty)
    lines.flatMap { line =>
      val node = JsonUtil.mapper.readTree(line)
      if (node.has("domainMetadata")) {
        val dm = node.get("domainMetadata")
        if (dm.has("domain") && dm.get("domain").asText() == "delta.clustering") {
          val config = dm.get("configuration").asText()
          try {
            val configNode = JsonUtil.mapper.readTree(config)
            if (configNode.has("columns")) {
              val cols = configNode.get("columns")
              Some(scala.jdk.CollectionConverters.IteratorHasAsScala(cols.elements()).asScala
                .map(_.asText()).toSeq)
            } else None
          } catch { case _: Exception => None }
        } else None
      } else None
    }.headOption
  }

  /** Extract schema evolution fields by comparing metadata before/after this version. */
  private def extractSchemaEvolution(
      dl: DeltaLog,
      version: Long,
      operation: String,
      operationParams: Option[com.fasterxml.jackson.databind.JsonNode]
  ): Seq[(String, Any)] = {
    val fields = mutable.ArrayBuffer[(String, Any)]()
    try {
      operation match {
        case "ADD COLUMNS" =>
          operationParams.foreach { params =>
            if (params.has("columns")) {
              val colsNode = params.get("columns")
              val addCols = new java.util.ArrayList[Any]()
              val iter = colsNode.elements()
              while (iter.hasNext) {
                val colNode = iter.next()
                val col = new java.util.LinkedHashMap[String, Any]()
                if (colNode.isTextual) {
                  // Simple format: "name TYPE"
                  val parts = colNode.asText().split("\\s+", 2)
                  col.put("name", parts(0))
                  if (parts.length > 1) col.put("type", parts(1).toLowerCase)
                  col.put("nullable", true)
                } else if (colNode.isObject) {
                  if (colNode.has("name")) col.put("name", colNode.get("name").asText())
                  if (colNode.has("type")) col.put("type", colNode.get("type").asText().toLowerCase)
                  col.put("nullable",
                    if (colNode.has("nullable")) colNode.get("nullable").asBoolean() else true)
                }
                addCols.add(col)
              }
              if (!addCols.isEmpty) fields += ("addColumns" -> addCols)
            }
          }
        case "DROP COLUMNS" =>
          operationParams.foreach { params =>
            if (params.has("columns")) {
              val colsNode = params.get("columns")
              val dropCols = new java.util.ArrayList[String]()
              val iter = colsNode.elements()
              while (iter.hasNext) dropCols.add(iter.next().asText())
              if (!dropCols.isEmpty) fields += ("dropColumns" -> dropCols)
            }
          }
        case "RENAME COLUMN" =>
          operationParams.foreach { params =>
            if (params.has("oldColumnName") && params.has("newColumnName")) {
              val renames = new java.util.LinkedHashMap[String, String]()
              renames.put(params.get("oldColumnName").asText(),
                params.get("newColumnName").asText())
              fields += ("renameColumns" -> renames)
            }
          }
        case "CHANGE COLUMN" =>
          // Type change / nullability change — capture as schema diff
          operationParams.foreach { params =>
            if (params.has("column")) {
              val colName = params.get("column").asText()
              // Get the new type from the post-change metadata
              try {
                val snapshot = dl.getSnapshotAt(version)
                val schema = snapshot.metadata.schema
                schema.find(_.name == colName).foreach { field =>
                  val change = new java.util.LinkedHashMap[String, Any]()
                  change.put("name", field.name)
                  change.put("type", field.dataType.simpleString)
                  change.put("nullable", field.nullable)
                  val changes = new java.util.ArrayList[Any]()
                  changes.add(change)
                  fields += ("changeColumns" -> changes)
                }
              } catch { case _: Exception => }
            }
          }
        case _ => // Unknown schema evolution op
      }
    } catch { case _: Exception => }
    fields.toSeq
  }

  /** Extract property changes from operationParameters or metadata diff. */
  private def extractPropertyChanges(
      dl: DeltaLog,
      version: Long,
      operationParams: Option[com.fasterxml.jackson.databind.JsonNode]
  ): Seq[(String, Any)] = {
    val fields = mutable.ArrayBuffer[(String, Any)]()
    try {
      // Try to get set/removed properties from operationParameters
      operationParams.foreach { params =>
        if (params.has("properties")) {
          val propsNode = params.get("properties")
          val setProps = new java.util.LinkedHashMap[String, String]()
          val iter = propsNode.fields()
          while (iter.hasNext) {
            val entry = iter.next()
            setProps.put(entry.getKey, entry.getValue.asText())
          }
          if (!setProps.isEmpty) fields += ("set" -> setProps)
        }
      }

      // If we couldn't get from params, diff the metadata
      if (fields.isEmpty && version > 0) {
        val prevProps = dl.getSnapshotAt(version - 1).metadata.configuration
        val currProps = dl.getSnapshotAt(version).metadata.configuration

        val setProps = new java.util.LinkedHashMap[String, String]()
        currProps.foreach { case (k, v) =>
          if (!prevProps.contains(k) || prevProps(k) != v) setProps.put(k, v)
        }
        val removedProps = new java.util.ArrayList[String]()
        prevProps.keys.foreach { k =>
          if (!currProps.contains(k)) removedProps.add(k)
        }
        if (!setProps.isEmpty) fields += ("set" -> setProps)
        if (!removedProps.isEmpty) fields += ("remove" -> removedProps)
      }
    } catch { case _: Exception => }
    fields.toSeq
  }
}
