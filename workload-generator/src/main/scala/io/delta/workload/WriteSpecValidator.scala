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
 * Validates a write spec by replaying commits as SQL against a fresh table,
 * then comparing the result against the original table's expected data.
 *
 * This ensures the write_spec.json is self-sufficient: any engine that
 * correctly implements the described operations will produce the same table.
 */
object WriteSpecValidator {

  /**
   * Replay the write spec and validate the result matches the original table.
   *
   * @param spark       SparkSession
   * @param outputDir   The workload output directory (contains write_spec.json, delta/, expected/)
   * @param specName    Name for error reporting
   * @return Seq of warning messages (empty = all passed)
   */
  def validate(spark: SparkSession, outputDir: Path, specName: String): Seq[String] = {
    val writeSpecFile = outputDir.resolve("write_spec.json")
    if (!Files.exists(writeSpecFile)) return Seq.empty

    val warnings = scala.collection.mutable.ArrayBuffer[String]()
    val writeSpec = JsonUtil.mapper.readTree(Files.readAllBytes(writeSpecFile))
    val commits = writeSpec.get("commits")
    if (commits == null || !commits.isArray || commits.size() == 0) return Seq.empty

    // Create a temp directory for the replay table
    val tempDir = Files.createTempDirectory(s"write_replay_$specName")
    val replayTablePath = tempDir.resolve("replay_table").toString
    val replayTableRef = s"delta.`$replayTablePath`"

    try {
      // Replay each commit as SQL
      val iter = commits.elements()
      while (iter.hasNext) {
        val commit = iter.next()
        val operation = commit.get("operation").asText()
        try {
          replayCommit(spark, commit, operation, replayTableRef, replayTablePath, outputDir)
        } catch {
          case e: Exception =>
            warnings += s"Replay failed at $operation: ${e.getMessage}"
            // Can't continue if a commit fails
            return warnings.toSeq
        }
      }

      // Now compare: read the replayed table and the original table
      DeltaLog.clearCache()
      val originalTablePath = outputDir.resolve("delta")
      if (!Files.exists(originalTablePath)) {
        warnings += "No delta/ directory to compare against"
        return warnings.toSeq
      }

      val originalDf = spark.read.format("delta").load(originalTablePath.toString)
      val replayDf = spark.read.format("delta").load(replayTablePath)

      val originalMultiset = JsonUtil.toRowMultiset(originalDf)
      val replayMultiset = JsonUtil.toRowMultiset(replayDf)

      if (originalMultiset != replayMultiset) {
        val missing = originalMultiset.keySet -- replayMultiset.keySet
        val extra = replayMultiset.keySet -- originalMultiset.keySet
        val details = new StringBuilder()
        if (missing.nonEmpty) {
          details.append(s" Missing ${missing.size} rows.")
          missing.take(2).foreach(r => details.append(s"\n    $r"))
        }
        if (extra.nonEmpty) {
          details.append(s" Extra ${extra.size} rows.")
          extra.take(2).foreach(r => details.append(s"\n    $r"))
        }
        warnings += s"Write spec replay mismatch: original=${originalMultiset.values.sum} " +
          s"rows, replay=${replayMultiset.values.sum} rows.$details"
      }

      // Compare version counts
      val originalVersion = DeltaLog.forTable(spark, originalTablePath.toString).update().version
      val replayVersion = DeltaLog.forTable(spark, replayTablePath).update().version
      if (originalVersion != replayVersion) {
        warnings += s"Version mismatch: original=$originalVersion, replay=$replayVersion"
      }

    } finally {
      // Cleanup
      try {
        org.apache.commons.io.FileUtils.deleteDirectory(tempDir.toFile)
      } catch { case _: Exception => }
    }

    if (warnings.isEmpty) {
      println(s"  Write spec replay validation PASSED for $specName")
    }
    warnings.toSeq
  }

  private def replayCommit(
      spark: SparkSession,
      commit: com.fasterxml.jackson.databind.JsonNode,
      operation: String,
      tableRef: String,
      tablePath: String,
      outputDir: Path): Unit = {

    operation match {
      case "create_table" =>
        val schema = commit.get("schema")
        val fields = schema.get("fields")
        val colDefs = (0 until fields.size()).map { i =>
          val f = fields.get(i)
          val name = f.get("name").asText()
          val dtype = f.get("type").asText()
          val nullable = if (f.has("nullable")) f.get("nullable").asBoolean(true) else true
          s"`$name` $dtype${if (!nullable) " NOT NULL" else ""}"
        }.mkString(", ")

        var sql = s"CREATE TABLE $tableRef ($colDefs) USING delta"

        if (commit.has("partitionColumns")) {
          val parts = (0 until commit.get("partitionColumns").size())
            .map(i => commit.get("partitionColumns").get(i).asText())
          if (parts.nonEmpty) sql += s" PARTITIONED BY (${parts.mkString(", ")})"
        }

        if (commit.has("properties")) {
          val props = commit.get("properties")
          val propsIter = props.fields()
          val propsList = scala.collection.mutable.ArrayBuffer[String]()
          while (propsIter.hasNext) {
            val entry = propsIter.next()
            propsList += s"'${entry.getKey}' = '${entry.getValue.asText()}'"
          }
          if (propsList.nonEmpty) sql += s" TBLPROPERTIES (${propsList.mkString(", ")})"
        }

        spark.sql(sql)

        // Load data files if present
        if (commit.has("dataFiles")) {
          loadDataFiles(spark, commit.get("dataFiles"), outputDir, tableRef)
        }

      case "insert" =>
        if (commit.has("dataFiles")) {
          loadDataFiles(spark, commit.get("dataFiles"), outputDir, tableRef)
        }

      case "insert_overwrite" =>
        if (commit.has("dataFiles")) {
          val tempView = s"_replay_insert_overwrite_${System.nanoTime()}"
          loadDataFilesToView(spark, commit.get("dataFiles"), outputDir, tempView)
          if (commit.has("predicate")) {
            spark.sql(s"INSERT OVERWRITE $tableRef SELECT * FROM $tempView")
          } else {
            spark.sql(s"INSERT OVERWRITE $tableRef SELECT * FROM $tempView")
          }
          spark.catalog.dropTempView(tempView)
        }

      case "delete" =>
        if (commit.has("predicate")) {
          spark.sql(s"DELETE FROM $tableRef WHERE ${commit.get("predicate").asText()}")
        } else {
          spark.sql(s"DELETE FROM $tableRef")
        }

      case "update" =>
        val predicate = if (commit.has("predicate")) commit.get("predicate").asText() else "true"
        if (commit.has("set")) {
          val setNode = commit.get("set")
          val setIter = setNode.fields()
          val setClauses = scala.collection.mutable.ArrayBuffer[String]()
          while (setIter.hasNext) {
            val entry = setIter.next()
            setClauses += s"${entry.getKey} = ${entry.getValue.asText()}"
          }
          spark.sql(s"UPDATE $tableRef SET ${setClauses.mkString(", ")} WHERE $predicate")
        }

      case "truncate" =>
        spark.sql(s"DELETE FROM $tableRef")

      case "evolve_schema" =>
        if (commit.has("addColumns")) {
          val cols = commit.get("addColumns")
          for (i <- 0 until cols.size()) {
            val col = cols.get(i)
            val name = col.get("name").asText()
            val dtype = col.get("type").asText()
            spark.sql(s"ALTER TABLE $tableRef ADD COLUMN (`$name` $dtype)")
          }
        }
        if (commit.has("renameColumns")) {
          val renames = commit.get("renameColumns")
          val iter = renames.fields()
          while (iter.hasNext) {
            val entry = iter.next()
            spark.sql(s"ALTER TABLE $tableRef RENAME COLUMN `${entry.getKey}` TO `${entry.getValue.asText()}`")
          }
        }
        if (commit.has("dropColumns")) {
          val drops = commit.get("dropColumns")
          for (i <- 0 until drops.size()) {
            spark.sql(s"ALTER TABLE $tableRef DROP COLUMN `${drops.get(i).asText()}`")
          }
        }
        // If evolve_schema also has data files (auto-merge), insert them
        if (commit.has("dataFiles")) {
          loadDataFiles(spark, commit.get("dataFiles"), outputDir, tableRef)
        }

      case "update_properties" =>
        if (commit.has("set")) {
          val setNode = commit.get("set")
          val iter = setNode.fields()
          val props = scala.collection.mutable.ArrayBuffer[String]()
          while (iter.hasNext) {
            val entry = iter.next()
            props += s"'${entry.getKey}' = '${entry.getValue.asText()}'"
          }
          if (props.nonEmpty) {
            spark.sql(s"ALTER TABLE $tableRef SET TBLPROPERTIES (${props.mkString(", ")})")
          }
        }
        if (commit.has("remove")) {
          val removeNode = commit.get("remove")
          val keys = (0 until removeNode.size()).map(i => s"'${removeNode.get(i).asText()}'")
          if (keys.nonEmpty) {
            spark.sql(s"ALTER TABLE $tableRef UNSET TBLPROPERTIES (${keys.mkString(", ")})")
          }
        }

      case "restore" =>
        if (commit.has("version")) {
          spark.sql(s"RESTORE $tableRef TO VERSION AS OF ${commit.get("version").asInt()}")
        }

      case _ =>
        // Unknown operation — skip (engine may not support it)
    }
  }

  /** Load data files from the workload into the table via INSERT. */
  private def loadDataFiles(
      spark: SparkSession,
      dataFilesNode: com.fasterxml.jackson.databind.JsonNode,
      outputDir: Path,
      tableRef: String): Unit = {
    val paths = (0 until dataFilesNode.size())
      .map(i => outputDir.resolve(dataFilesNode.get(i).asText()).toString)
      .filter(p => Files.exists(java.nio.file.Paths.get(p)))
    if (paths.nonEmpty) {
      val df = spark.read.parquet(paths: _*)
      df.write.format("delta").mode("append").save(
        tableRef.stripPrefix("delta.`").stripSuffix("`"))
    }
  }

  /** Load data files into a temp view. */
  private def loadDataFilesToView(
      spark: SparkSession,
      dataFilesNode: com.fasterxml.jackson.databind.JsonNode,
      outputDir: Path,
      viewName: String): Unit = {
    val paths = (0 until dataFilesNode.size())
      .map(i => outputDir.resolve(dataFilesNode.get(i).asText()).toString)
      .filter(p => Files.exists(java.nio.file.Paths.get(p)))
    if (paths.nonEmpty) {
      spark.read.parquet(paths: _*).createOrReplaceTempView(viewName)
    }
  }
}
