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

import java.nio.file.Path

import scala.collection.mutable

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.types.{DataType, StructType}

/** Records structured write operations and serializes them as write_spec.json. */
class WriteSpecBuilder {

  private val commits = mutable.ArrayBuffer[WriteCommit]()

  // ---------------------------------------------------------------------------
  // High-level operations (SQL semantics)
  // ---------------------------------------------------------------------------

  def recordCreateTable(schemaDDL: String, properties: Map[String, String],
      partitionColumns: Seq[String]): Unit = {
    commits += WriteCommit(
      operation = "create_table",
      schema = Some(ddlToSchemaJson(schemaDDL)),
      partitionColumns = if (partitionColumns.nonEmpty) Some(partitionColumns) else None,
      properties = if (properties.nonEmpty) Some(properties) else None)
  }

  def recordReplaceTable(schemaDDL: String, properties: Map[String, String],
      partitionColumns: Seq[String]): Unit = {
    commits += WriteCommit(
      operation = "replace_table",
      schema = Some(ddlToSchemaJson(schemaDDL)),
      partitionColumns = if (partitionColumns.nonEmpty) Some(partitionColumns) else None,
      properties = if (properties.nonEmpty) Some(properties) else None)
  }

  def recordInsert(): Unit = commits += WriteCommit(operation = "insert")

  def recordDelete(predicate: String): Unit =
    commits += WriteCommit(operation = "delete", predicate = Some(predicate))

  def recordUpdate(predicate: String, set: Map[String, String]): Unit =
    commits += WriteCommit(operation = "update", predicate = Some(predicate), set = Some(set))

  def recordSetProperties(props: Map[String, String]): Unit =
    recordUpdateProperties(props, Seq.empty)

  def recordUnsetProperties(props: Seq[String]): Unit =
    recordUpdateProperties(Map.empty, props)

  def recordUpdateProperties(set: Map[String, String], unset: Seq[String]): Unit = {
    commits += WriteCommit(
      operation = "update_properties",
      set = if (set.nonEmpty) Some(set) else None,
      remove = if (unset.nonEmpty) Some(unset) else None)
  }

  def recordAddColumns(columnsDDL: String): Unit =
    recordEvolveSchema(columnsDDL, Map.empty, Seq.empty)

  def recordRenameColumn(oldName: String, newName: String): Unit =
    recordEvolveSchema("", Map(oldName -> newName), Seq.empty)

  def recordDropColumns(columns: Seq[String]): Unit =
    recordEvolveSchema("", Map.empty, columns)

  def recordEvolveSchema(
      addColumnsDDL: String,
      renameColumns: Map[String, String],
      dropColumns: Seq[String]): Unit = {
    val addCols = if (addColumnsDDL.nonEmpty) {
      val st = StructType.fromDDL(addColumnsDDL)
      Some(st.fields.map { f =>
        val typeJson = JsonUtil.mapper.readValue(f.dataType.json, classOf[Any])
        Map[String, Any]("name" -> f.name, "type" -> typeJson, "nullable" -> f.nullable)
      }.toSeq)
    } else None
    commits += WriteCommit(
      operation = "evolve_schema",
      addColumns = addCols,
      renameColumns = if (renameColumns.nonEmpty) Some(renameColumns) else None,
      dropColumns = if (dropColumns.nonEmpty) Some(dropColumns) else None)
  }

  def recordTruncate(): Unit = commits += WriteCommit(operation = "truncate")

  def recordRestore(version: Long): Unit =
    commits += WriteCommit(operation = "restore", version = Some(version))

  // ---------------------------------------------------------------------------
  // Low-level operation (raw Delta actions)
  // ---------------------------------------------------------------------------

  def recordCommit(
      schemaDDL: Option[String] = None,
      tableProperties: Option[Map[String, String]] = None,
      txn: Option[AppTxn] = None,
      addFiles: Option[Seq[AddFileAction]] = None,
      removeFiles: Option[Seq[RemoveFileAction]] = None,
      addDomainMetadata: Option[Seq[AddDomainMetadata]] = None,
      removeDomainMetadata: Option[Seq[String]] = None): Unit = {
    commits += WriteCommit(
      operation = "commit",
      schema = schemaDDL.map(ddlToSchemaJson),
      tableProperties = tableProperties,
      txn = txn,
      addFiles = addFiles,
      removeFiles = removeFiles,
      addDomainMetadata = addDomainMetadata,
      removeDomainMetadata = removeDomainMetadata)
  }

  // ---------------------------------------------------------------------------

  /** Serialize recorded operations to write_spec.json (no data file enrichment). */
  def buildSpec(outputDir: Path): Unit = {
    JsonUtil.writeSpec(outputDir.resolve("write_spec.json"), WriteSpec(commits.toSeq))
  }

  /**
   * Serialize recorded operations to write_spec.json, enriching high-level
   * create_table, replace_table, and insert operations with actual data file
   * paths from the DeltaLog.
   *
   * Newly added data files are copied to `outputDir/data/` and referenced via
   * relative paths in `dataFiles`, making the write spec self-sufficient for replay.
   */
  def buildSpec(spark: SparkSession, tablePath: Path, outputDir: Path): Unit = {
    DeltaLog.clearCache()
    val dl = DeltaLog.forTable(spark, tablePath.toString)
    val dataDir = outputDir.resolve("data")
    java.nio.file.Files.createDirectories(dataDir)

    val dataOps = Set("insert", "create_table", "replace_table")

    val enriched = commits.zipWithIndex.map { case (commit, idx) =>
      if (dataOps.contains(commit.operation)) {
        try {
          val snapshot = dl.getSnapshotAt(idx.toLong)
          val prevFiles = if (idx > 0) {
            dl.getSnapshotAt(idx.toLong - 1).allFiles.collect()
              .map(_.path).toSet
          } else Set.empty[String]
          val currentFiles = snapshot.allFiles.collect().map(_.path)
          val newFiles = currentFiles.filterNot(prevFiles.contains)

          if (newFiles.nonEmpty) {
            val paths = newFiles.map { f =>
              val src = tablePath.resolve(f)
              val commitDir = dataDir.resolve(s"commit_$idx")
              java.nio.file.Files.createDirectories(commitDir)
              val destName = java.nio.file.Paths.get(f).getFileName.toString
              val dest = commitDir.resolve(destName)
              if (java.nio.file.Files.exists(src)) {
                java.nio.file.Files.copy(src, dest,
                  java.nio.file.StandardCopyOption.REPLACE_EXISTING)
              }
              s"data/commit_$idx/$destName"
            }
            commit.copy(dataFiles = Some(paths.toSeq))
          } else commit
        } catch {
          case _: Exception => commit
        }
      } else commit
    }

    JsonUtil.writeSpec(outputDir.resolve("write_spec.json"), WriteSpec(enriched.toSeq))
  }

  /**
   * Parse a SQL DDL string (e.g. "id INT, name STRING NOT NULL") into the
   * Delta schema JSON format using Spark's type parser.
   */
  private def ddlToSchemaJson(ddl: String): Any = {
    val st = StructType.fromDDL(ddl)
    JsonUtil.mapper.readValue(st.json, classOf[Any])
  }
}
