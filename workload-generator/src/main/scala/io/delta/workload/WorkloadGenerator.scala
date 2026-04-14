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

import java.nio.file.{Files, Path, Paths}

import scala.collection.mutable
import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.DeltaLog

/**
 * Internal workload generation engine. Use [[WorkloadTestSuite]] as the public API:
 *
 * {{{
 * class DeletionVectorsSuite extends WorkloadTestSuite("deletion_vectors") {
 *   test("dv_delete_basic") {
 *     sql("CREATE TABLE tbl (id INT, name STRING) USING delta " +
 *       "TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")
 *     sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
 *     sql("DELETE FROM tbl WHERE id = 2")
 *     val t = registerTable("tbl")
 *     read(t)
 *     read(t, version = 0)
 *     read(t, predicate = "id > 1")
 *     snapshot(t)
 *   }
 * }
 * }}}
 */
object WorkloadGenerator {

  private def checkAssertion(
      config: HasAssertion[_], specPath: Path, warnings: mutable.ArrayBuffer[String]): Unit = {
    config.assertion.foreach { check =>
      try {
        val node = JsonUtil.mapper.readTree(Files.readAllBytes(specPath))
        check(node)
      } catch {
        case e: Exception =>
          warnings += s"Assertion failed for ${specPath.getFileName}: ${e.getMessage}"
      }
    }
  }

  private[workload] def generateTable(
      spark: SparkSession,
      ts: TableSpec,
      outputBase: Path): WorkloadResult = {
    val dirName = ts.outputName
    val testOutputDir = outputBase.resolve(dirName)

    println(s"--- $dirName ---")

    try {
      // Set up output directory
      if (Files.exists(testOutputDir)) FileUtils.deleteDirectory(testOutputDir.toFile)
      Files.createDirectories(testOutputDir)
      val specsDir = testOutputDir.resolve("specs")
      Files.createDirectories(specsDir)
      Files.createDirectories(testOutputDir.resolve("expected"))

      // Copy Delta table
      val destTablePath = testOutputDir.resolve("delta")
      FileUtils.copyDirectory(ts.sourcePath.toFile, destTablePath.toFile)

      // Apply mutations on the copied table
      ts.mutations.foreach { mutate =>
        mutate(destTablePath)
      }

      val warnings = mutable.ArrayBuffer[String]()

      // Snapshot specs
      val snapshotNames = mutable.ArrayBuffer[String]()
      val explicits = if (ts.snapshotSpecs.isEmpty) {
        Seq(SnapshotSpecConfig(None, None))
      } else ts.snapshotSpecs
      for (ss <- explicits) {
        try {
          SnapshotCapture.capture(spark, dirName, destTablePath, specsDir,
            version = ss.version, timestamp = ss.timestamp)
          val specName = (ss.version, ss.timestamp) match {
            case (Some(v), _) => s"${dirName}_snapshot_v$v"
            case (_, Some(t)) =>
              s"${dirName}_snapshot_ts_${t.replace(":", "-").replace(" ", "_")}"
            case _ => s"${dirName}_snapshot"
          }
          if (!snapshotNames.contains(specName)) snapshotNames += specName
          checkAssertion(ss, specsDir.resolve(s"$specName.json"), warnings)
        } catch {
          case e: Exception => warnings += s"Snapshot failed: ${e.getMessage}"
        }
      }

      // Read specs
      val readNames = mutable.ArrayBuffer[String]()
      for (rs <- ts.readSpecs) {
        try {
          ReadCapture.capture(spark, dirName, destTablePath, testOutputDir, specsDir,
            name = rs.name, predicate = rs.predicate, version = rs.version,
            timestamp = rs.timestamp, columns = rs.columns)
          val specName = s"${dirName}_${rs.name}"
          readNames += specName
          checkAssertion(rs, specsDir.resolve(s"$specName.json"), warnings)
        } catch {
          case e: Exception =>
            warnings += s"Read '${rs.name}' infra failure: ${e.getMessage}"
        }
      }

      // Checkpoint specs
      for (cs <- ts.checkpointSpecs) {
        try {
          CheckpointCapture.capture(spark, dirName, destTablePath,
            testOutputDir, specsDir, cs.version)
          checkAssertion(cs,
            specsDir.resolve(s"${dirName}_checkpoint_v${cs.version}.json"), warnings)
        } catch {
          case e: Exception =>
            warnings += s"Checkpoint v${cs.version}: ${e.getMessage}"
        }
      }

      // CRC specs
      for (cs <- ts.crcSpecs) {
        try {
          CrcCapture.capture(spark, dirName, destTablePath, specsDir, cs.version)
          checkAssertion(cs,
            specsDir.resolve(s"${dirName}_crc_v${cs.version}.json"), warnings)
        } catch {
          case e: Exception =>
            warnings += s"CRC v${cs.version}: ${e.getMessage}"
        }
      }

      // table_info.json
      TableInfoWriter.write(spark, destTablePath, testOutputDir,
        name = dirName, description = ts.description, tags = ts.tags)

      // Repro placeholder
      val reproDir = testOutputDir.resolve("repro")
      Files.createDirectories(reproDir)
      Files.write(reproDir.resolve("generate.scala"),
        s"// Generated by ${ts.outputName} test\n".getBytes("UTF-8"))

      val total = snapshotNames.size + readNames.size +
        ts.checkpointSpecs.size + ts.crcSpecs.size
      println(s"  $dirName: $total specs")

      WorkloadResult(testOutputDir.toString, dirName, total,
        readNames.toSeq, snapshotNames.toSeq,
        warnings.isEmpty, warnings.toSeq)
    } catch {
      case e: Exception =>
        System.err.println(s"  ERROR $dirName: ${e.getMessage}")
        e.printStackTrace()
        WorkloadResult(testOutputDir.toString, dirName, 0,
          Seq.empty, Seq.empty, false,
          Seq(s"Failed: ${e.getMessage}"))
    }
  }

}

// ---------------------------------------------------------------------------
// SpecRef — returned by spec declaration methods for optional assertions
// ---------------------------------------------------------------------------

/**
 * Handle to a declared spec. Attach assertions that are checked after capture.
 * `T` is the typed spec class (e.g. [[ReadSpec]], [[SnapshotSpec]]).
 */
class SpecRef[T] private[workload] (
    private[workload] val config: HasAssertion[T]) {

  /**
   * Assert that the captured spec is an error (has `expectedError`, no `expected`).
   * Works for any spec type that follows the `expected`/`expectedError` convention.
   */
  def assertError(): SpecRef[T] = {
    config.assertion = Some { node =>
      require(node.has("expectedError") && !node.get("expectedError").isNull,
        s"Expected an error spec but got a success result")
    }
    this
  }

  /** Assert conditions on the captured spec using the typed case class. */
  def assert(check: T => Unit): SpecRef[T] = {
    val deserialize = config.deserialize
    config.assertion = Some { node =>
      check(deserialize(node))
    }
    this
  }
}

private[workload] trait HasAssertion[T] {
  var assertion: Option[com.fasterxml.jackson.databind.JsonNode => Unit] = None
  def deserialize: com.fasterxml.jackson.databind.JsonNode => T
}

// ---------------------------------------------------------------------------
// TableHandle — returned by registerTable() / registerTableFromPath()
// ---------------------------------------------------------------------------

/**
 * Handle to a Delta table created via SQL. Used for declaring read specs
 * (read, snapshot), metadata specs (checkpoint, crc), and table mutations
 * (mutateTable, modifyCommitActions).
 */
class TableHandle private[workload] (
    private[workload] val tableName: String,
    private[workload] val sourcePath: Path,
    private[workload] val ctx: WorkloadContext) {

  /**
   * Get the commit timestamp for a specific version.
   * Returns formatted timestamp (yyyy-MM-dd HH:mm:ss.SSS).
   * Uses DeltaLog.history which properly handles In-Commit Timestamps (ICT).
   */
  def getTimestampForVersion(version: Long): String = {
    val deltaLog = DeltaLog.forTable(ctx.spark, sourcePath.toString)
    val history = deltaLog.history.getHistory(version, Some(version + 1), None)
    require(history.nonEmpty, s"No history entry for version $version")
    val ts = history.head.timestamp
    require(ts != null, s"No timestamp for version $version")
    val sessionTz = ctx.spark.conf.get("spark.sql.session.timeZone", "UTC")
    val fmt = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    fmt.setTimeZone(java.util.TimeZone.getTimeZone(sessionTz))
    fmt.format(ts)
  }
}

// ---------------------------------------------------------------------------
// WorkloadContext — the user's interface inside a workload() block
// ---------------------------------------------------------------------------

class WorkloadContext private[workload] (
    val spark: SparkSession,
    val workloadName: String,
    private[workload] val tags: Seq[String] = Seq.empty) {

  private val _createdTables = mutable.ArrayBuffer[String]()
  private[workload] val tableSpecs = mutable.ArrayBuffer[TableSpec]()

  /** Convert nullable java.lang.Long to Option[Long]. */
  private def opt(v: java.lang.Long): Option[Long] = Option(v).map(_.longValue())

  // Per-table state: spec names must be unique within each table
  private val _tableSpecNames = mutable.HashMap[String, mutable.HashSet[String]]()

  // ---- SQL ----

  private val _createTableRegex =
    """(?i)(?:CREATE|REPLACE)\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?`?(\w+)`?""".r

  /** Execute SQL. Tracks CREATE TABLE statements for cleanup. */
  def sql(statement: String): Unit = {
    _createTableRegex.findFirstMatchIn(statement).foreach { m =>
      _createdTables += m.group(1)
    }
    spark.sql(statement)
  }

  // ---- Table handles ----

  /** Register a managed Spark table for spec capture. */
  def registerTable(name: String): TableHandle = {
    val path = resolveTablePath(name)
    val handle = new TableHandle(name, path, this)
    ensureTableSpec(handle)
    handle
  }

  /** Register a table at a filesystem path for spec capture. */
  def registerTableFromPath(path: String): TableHandle = {
    val p = Paths.get(path)
    require(Files.exists(p.resolve("_delta_log")),
      s"No Delta table at path: $path")
    val name = p.getFileName.toString
    val handle = new TableHandle(name, p, this)
    ensureTableSpec(handle)
    handle
  }

  // ---- Spec declaration ----

  /** Read spec. Name auto-generated from parameters. */
  def read(
      table: TableHandle,
      predicate: String = null,
      version: java.lang.Long = null,
      timestamp: String = null,
      columns: Seq[String] = null,
      name: String = null): SpecRef[ReadSpec] = {
    val specName = if (name != null) name else autoReadName(
      Option(predicate), opt(version),
      Option(timestamp), Option(columns))
    requireUnique(table, specName)
    val config = ReadSpecConfig(
      specName, Option(predicate), opt(version),
      Option(timestamp), Option(columns))
    getTableSpec(table).readSpecs += config
    new SpecRef(config)
  }

  /** Snapshot spec. */
  def snapshot(
      table: TableHandle,
      version: java.lang.Long = null,
      timestamp: String = null): SpecRef[SnapshotSpec] = {
    val config = SnapshotSpecConfig(opt(version), Option(timestamp))
    getTableSpec(table).snapshotSpecs += config
    new SpecRef(config)
  }

  /**
   * Force Spark to write a checkpoint file for the given SQL table name.
   * Convenience wrapper around `DeltaLog.forTable(...).checkpoint()`.
   */
  def forceCheckpoint(tableName: String): Unit = {
    import org.apache.spark.sql.delta.DeltaLog
    val loc = spark.sql(s"DESCRIBE DETAIL `$tableName`").collect()(0).getAs[String]("location")
    DeltaLog.forTable(spark, loc).checkpoint()
  }

  /** Checkpoint verification spec at a specific version. */
  def checkpoint(table: TableHandle, version: Long): SpecRef[CheckpointSpec] = {
    val config = CheckpointSpecConfig(version)
    getTableSpec(table).checkpointSpecs += config
    new SpecRef(config)
  }

  /** CRC verification spec at a specific version. */
  def crc(table: TableHandle, version: Long): SpecRef[CrcSpec] = {
    val config = CrcSpecConfig(version)
    getTableSpec(table).crcSpecs += config
    new SpecRef(config)
  }

  // ---- Table mutations (applied to copied table before spec capture) ----

  /** Mutate the copied table's filesystem before specs are captured. */
  def mutateTable(table: TableHandle)(mutation: Path => Unit): Unit = {
    getTableSpec(table).mutations += mutation
  }

  /**
   * Modify actions in a specific commit version.
   * The modifier receives the full list of `(actionType, innerNode)` pairs
   * (e.g. `("add", <the add ObjectNode>)`) and returns the (possibly reordered,
   * filtered, or modified) list to write back.
   */
  def modifyCommitActions(table: TableHandle, version: Long)(
      modifier: Seq[(String, ObjectNode)] => Seq[(String, ObjectNode)]): Unit = {
    mutateTable(table) { tableDir =>
      val commitFile = tableDir.resolve("_delta_log").resolve(f"$version%020d.json")
      if (Files.exists(commitFile)) {
        val lines = new String(Files.readAllBytes(commitFile), "UTF-8").split("\n")
          .filter(_.trim.nonEmpty)
        val actions = lines.map { line =>
          val node = JsonUtil.mapper.readTree(line)
          val actionType = node.fieldNames().next()
          (actionType, node.get(actionType).asInstanceOf[ObjectNode])
        }.toSeq
        val result = modifier(actions)
        val newLines = result.map { case (actionType, innerNode) =>
          val wrapper = JsonUtil.mapper.createObjectNode()
          wrapper.set(actionType, innerNode)
          JsonUtil.mapper.writeValueAsString(wrapper)
        }
        Files.write(commitFile, newLines.mkString("\n").getBytes("UTF-8"))
      }
    }
  }

  // ---- Auto-naming ----

  private def autoReadName(
      predicate: Option[String],
      version: Option[Long],
      timestamp: Option[String],
      columns: Option[Seq[String]]): String = {
    val parts = mutable.ArrayBuffer[String]("read")
    version.foreach(v => parts += s"v$v")
    timestamp.foreach { ts =>
      parts += "ts_" + ts.replace(":", "-").replace(" ", "_").take(23)
    }
    predicate.foreach { p =>
      val simplified = p.toLowerCase
        .replaceAll(">=", "_gte_").replaceAll("<=", "_lte_")
        .replaceAll("<>", "_neq_").replaceAll("!=", "_neq_")
        .replaceAll(">", "_gt_").replaceAll("<", "_lt_")
        .replaceAll("=", "_eq_")
        .replaceAll("\\s+and\\s+", "_and_").replaceAll("\\s+or\\s+", "_or_")
        .replaceAll("\\s+is\\s+not\\s+null", "_is_not_null")
        .replaceAll("\\s+is\\s+null", "_is_null")
        .replaceAll("\\s+in\\s+", "_in_")
        .replaceAll("\\s+", "_")
        .replaceAll("[^a-z0-9_]", "")
        .replaceAll("_+", "_").stripPrefix("_").stripSuffix("_")
        .take(50)
      parts += simplified
    }
    columns.foreach { cols =>
      parts += "cols_" + cols.take(3).mkString("_")
      if (cols.size > 3) parts += s"plus${cols.size - 3}"
    }
    parts.mkString("_")
  }

  // ---- Internal ----

  private def ensureTableSpec(handle: TableHandle): Unit = {
    val outputName = s"${workloadName}_${handle.tableName}"
    if (!tableSpecs.exists(_.outputName == outputName)) {
      tableSpecs += new TableSpec(
        _outputName = outputName,
        description = s"$workloadName — ${handle.tableName}",
        tags = tags,
        sourcePath = handle.sourcePath
      )
      _tableSpecNames(outputName) = mutable.HashSet[String]()
    }
  }

  private def getTableSpec(handle: TableHandle): TableSpec = {
    val outputName = s"${workloadName}_${handle.tableName}"
    tableSpecs.find(_.outputName == outputName).getOrElse(
      throw new RuntimeException(s"No table spec for ${handle.tableName}. Call registerTable() first."))
  }

  private def requireUnique(handle: TableHandle, specName: String): Unit = {
    val outputName = s"${workloadName}_${handle.tableName}"
    val names = _tableSpecNames.getOrElseUpdate(outputName, mutable.HashSet[String]())
    require(names.add(specName),
      s"Duplicate spec name '$specName' for table '$outputName'")
  }

  private def resolveTablePath(tableName: String): Path = {
    try {
      val detail = spark.sql(s"DESCRIBE DETAIL $tableName").collect()
      require(detail.nonEmpty, s"DESCRIBE DETAIL returned no rows for '$tableName'")
      val location = detail(0).getAs[String]("location")
      require(location != null && location.nonEmpty, s"'$tableName' has no location")
      val path = if (location.startsWith("file:")) {
        Paths.get(new java.net.URI(location))
      } else Paths.get(location)
      require(Files.exists(path.resolve("_delta_log")),
        s"'$tableName' at $path has no _delta_log")
      path
    } catch {
      case e: org.apache.spark.sql.AnalysisException =>
        throw new RuntimeException(
          s"Table '$tableName' not found. Created tables: ${_createdTables.mkString(", ")}", e)
    }
  }

  private[workload] def cleanup(): Unit = {
    val warehouseDir = spark.conf.get("spark.sql.warehouse.dir", "")
    _createdTables.foreach { t =>
      // Get location before dropping
      val location = try {
        val tableId = spark.sessionState.catalog.getTableMetadata(
          org.apache.spark.sql.catalyst.TableIdentifier(t))
        Option(tableId.location).map(_.toString)
      } catch { case _: Exception => None }

      // Drop from catalog
      try { spark.sql(s"DROP TABLE IF EXISTS `$t`") } catch { case _: Exception => }

      // Delete directory: try catalog location, then fall back to warehouse/tableName
      val pathsToDelete = location.toSeq.map { loc =>
        if (loc.startsWith("file:")) Paths.get(new java.net.URI(loc)) else Paths.get(loc)
      } ++ (if (warehouseDir.nonEmpty) {
        val base = if (warehouseDir.startsWith("file:"))
          Paths.get(new java.net.URI(warehouseDir)) else Paths.get(warehouseDir)
        Seq(base.resolve(t))
      } else Seq.empty)

      pathsToDelete.distinct.foreach { path =>
        try {
          if (Files.exists(path)) {
            org.apache.commons.io.FileUtils.deleteDirectory(path.toFile)
          }
        } catch { case _: Exception => }
      }
    }
  }

}

object WorkloadContext {
  private val _current = new scala.util.DynamicVariable[WorkloadContext](null)

  /** Get the current WorkloadContext. Throws if called outside a test body. */
  def current: WorkloadContext = {
    val ctx = _current.value
    require(ctx != null, "No active WorkloadContext. This method must be called inside a test body.")
    ctx
  }

  /** Execute a block with the given context as current. */
  def withContext[T](ctx: WorkloadContext)(body: => T): T = _current.withValue(ctx)(body)
}

// ---------------------------------------------------------------------------
// WorkloadOps — DSL trait mixed into WorkloadSuite for clean syntax
// ---------------------------------------------------------------------------

/**
 * Provides DSL methods for workload generation. Mixed into WorkloadSuite
 * so that test bodies can call methods like `sql()`, `registerTable()`, `read()`
 * directly without a context prefix.
 */
trait WorkloadOps {
  import WorkloadContext.current

  /** The active SparkSession. */
  def spark: SparkSession = current.spark

  /** Execute SQL. Tracks CREATE TABLE statements for cleanup. */
  def sql(statement: String): Unit = current.sql(statement)

  /** Register a managed Spark table for spec capture. */
  def registerTable(name: String): TableHandle = current.registerTable(name)

  /** Register a table at a filesystem path for spec capture. */
  def registerTableFromPath(path: String): TableHandle = current.registerTableFromPath(path)

  /** Read spec. Name auto-generated from parameters. */
  def read(
      table: TableHandle,
      predicate: String = null,
      version: java.lang.Long = null,
      timestamp: String = null,
      columns: Seq[String] = null,
      name: String = null): SpecRef[ReadSpec] = current.read(table, predicate, version, timestamp, columns, name)

  /** Snapshot spec. */
  def snapshot(
      table: TableHandle,
      version: java.lang.Long = null,
      timestamp: String = null): SpecRef[SnapshotSpec] = current.snapshot(table, version, timestamp)

  /** Force Spark to write a checkpoint file for the given SQL table name. */
  def forceCheckpoint(tableName: String): Unit = current.forceCheckpoint(tableName)

  /** Checkpoint verification spec at a specific version. */
  def checkpoint(table: TableHandle, version: Long): SpecRef[CheckpointSpec] = current.checkpoint(table, version)

  /** CRC verification spec at a specific version. */
  def crc(table: TableHandle, version: Long): SpecRef[CrcSpec] = current.crc(table, version)

  /** Mutate the copied table's filesystem before specs are captured. */
  def mutateTable(table: TableHandle)(mutation: Path => Unit): Unit =
    current.mutateTable(table)(mutation)

  /** Modify actions in a specific commit version. */
  def modifyCommitActions(table: TableHandle, version: Long)(
      modifier: Seq[(String, ObjectNode)] => Seq[(String, ObjectNode)]): Unit =
    current.modifyCommitActions(table, version)(modifier)
}

// ---------------------------------------------------------------------------
// Internal data structures
// ---------------------------------------------------------------------------

private[workload] class TableSpec(
    private var _outputName: String,
    val description: String,
    val tags: Seq[String],
    val sourcePath: Path) {
  def outputName: String = _outputName
  /** Set once by the orchestrator after body execution. */
  private[workload] def resolveOutputName(name: String): Unit = { _outputName = name }
  val readSpecs = mutable.ArrayBuffer[ReadSpecConfig]()
  val snapshotSpecs = mutable.ArrayBuffer[SnapshotSpecConfig]()
  val mutations = mutable.ArrayBuffer[Path => Unit]()
  val checkpointSpecs = mutable.ArrayBuffer[CheckpointSpecConfig]()
  val crcSpecs = mutable.ArrayBuffer[CrcSpecConfig]()
}

case class WorkloadResult(
    outputDir: String,
    testId: String,
    specsGenerated: Int,
    readSpecs: Seq[String],
    snapshotSpecs: Seq[String],
    validationPassed: Boolean,
    warnings: Seq[String])

private[workload] case class ReadSpecConfig(
    name: String, predicate: Option[String], version: Option[Long],
    timestamp: Option[String], columns: Option[Seq[String]]) extends HasAssertion[ReadSpec] {
  val deserialize = (n: com.fasterxml.jackson.databind.JsonNode) =>
    JsonUtil.mapper.treeToValue(n, classOf[ReadSpec])
}

private[workload] case class SnapshotSpecConfig(
    version: Option[Long], timestamp: Option[String]) extends HasAssertion[SnapshotSpec] {
  val deserialize = (n: com.fasterxml.jackson.databind.JsonNode) =>
    JsonUtil.mapper.treeToValue(n, classOf[SnapshotSpec])
}

private[workload] case class CheckpointSpecConfig(version: Long) extends HasAssertion[CheckpointSpec] {
  val deserialize = (n: com.fasterxml.jackson.databind.JsonNode) =>
    JsonUtil.mapper.treeToValue(n, classOf[CheckpointSpec])
}

private[workload] case class CrcSpecConfig(version: Long) extends HasAssertion[CrcSpec] {
  val deserialize = (n: com.fasterxml.jackson.databind.JsonNode) =>
    JsonUtil.mapper.treeToValue(n, classOf[CrcSpec])
}
