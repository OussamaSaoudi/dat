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
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.DeltaLog

/**
 * Internal workload generation engine. Use [[WorkloadSuite]] as the public API:
 *
 * {{{
 * new WorkloadSuite("deletion_vectors") {
 *   test("dv_delete_basic", "Deletion vectors after DELETE") {
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

  private[workload] val registry = mutable.LinkedHashMap[String, WorkloadDef]()

  /**
   * Register a workload. The body creates tables via SQL, then declares
   * specs against table handles.
   */
  def workload(name: String, description: String, tags: String*)(
      body: WorkloadContext => Unit): Unit = {
    require(!registry.contains(name), s"Duplicate workload name: '$name'")
    registry(name) = WorkloadDef(name, description, tags, body)
  }

  /**
   * Generate all registered workloads.
   * @param force If true, regenerate even if output exists. Default: false (skip existing).
   */
  def generateAll(
      outputDir: String,
      sourceScript: String = null,
      force: Boolean = false): Seq[WorkloadResult] = {
    val spark = SparkSession.active
    val scriptPath = resolveSourceScript(sourceScript)
    val scriptContent = scriptPath.map(p => new String(Files.readAllBytes(p), "UTF-8"))

    println(s"Generating ${registry.size} workloads to $outputDir\n")

    // Process each workload sequentially: body → resolve → generate → cleanup
    // This ensures table names don't collide between workloads.
    val results = registry.values.toSeq.flatMap { wd =>
      val ctx = new WorkloadContext(spark, wd.name, wd.tags)
      try {
        wd.body(ctx)
        // Single-table workloads: use workload name as directory name
        // Multi-table workloads: use {workload}_{table}
        if (ctx.tableSpecs.size == 1) {
          ctx.tableSpecs.head.resolveOutputName(wd.name)
        }
        ctx.tableSpecs.map { ts =>
          generateTable(spark, ts, Paths.get(outputDir), scriptContent, force)
        }
      } catch {
        case e: Exception =>
          System.err.println(s"  ERROR in ${wd.name}: ${e.getMessage}")
          e.printStackTrace()
          Seq(WorkloadResult(outputDir, wd.name, 0,
            Seq.empty, Seq.empty, Seq.empty, false,
            Seq(s"Setup failed: ${e.getMessage}")))
      } finally {
        ctx.cleanup()
      }
    }

    registry.clear()
    results
  }

  /** Generate a single workload by name. */
  def generate(
      name: String,
      outputDir: String,
      sourceScript: String = null): Seq[WorkloadResult] = {
    val spark = SparkSession.active
    require(registry.contains(name), s"No workload: '$name'. " +
      s"Available: ${registry.keys.mkString(", ")}")
    val scriptPath = resolveSourceScript(sourceScript)
    val scriptContent = scriptPath.map(p => new String(Files.readAllBytes(p), "UTF-8"))

    val wd = registry(name)
    val ctx = new WorkloadContext(spark, name, wd.tags)
    try {
      wd.body(ctx)
      if (ctx.tableSpecs.size == 1) {
        ctx.tableSpecs.head.resolveOutputName(name)
      }
      ctx.tableSpecs.map(ts => generateTable(spark, ts, Paths.get(outputDir), scriptContent)).toSeq
    } finally {
      ctx.cleanup()
    }
  }

  def reset(): Unit = registry.clear()
  def list(): Seq[String] = registry.keys.toSeq

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
      outputBase: Path,
      scriptContent: Option[String],
      force: Boolean = false): WorkloadResult = {
    val dirName = ts.outputName
    val testOutputDir = outputBase.resolve(dirName)

    // Skip if already generated (incremental mode) unless forced
    if (!force && Files.exists(testOutputDir.resolve("table_info.json"))) {
      println(s"--- $dirName (exists, skipping) ---")
      return WorkloadResult(testOutputDir.toString, dirName, -1,
        Seq.empty, Seq.empty, Seq.empty, true, Seq.empty)
    }

    println(s"--- $dirName ---")

    try {
      // Copy the Delta table
      TableCopier.cleanOutputDir(testOutputDir)
      val specsDir = testOutputDir.resolve("specs")
      Files.createDirectories(specsDir)
      Files.createDirectories(testOutputDir.resolve("expected"))

      val needsTimestampSync = ts.cdfSpecs.nonEmpty ||
        ts.readSpecs.exists(_.timestamp.isDefined) ||
        ts.snapshotSpecs.exists(_.timestamp.isDefined)
      val destTablePath = testOutputDir.resolve("delta")
      TableCopier.copyTable(ts.sourcePath, destTablePath, syncTimestamps = needsTimestampSync)

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

      // CDF specs
      val cdfNames = mutable.ArrayBuffer[String]()
      for (cs <- ts.cdfSpecs) {
        try {
          CdfCapture.capture(spark, dirName, destTablePath, testOutputDir, specsDir,
            name = cs.name, startVersion = cs.startVersion, endVersion = cs.endVersion,
            startTimestamp = cs.startTimestamp, endTimestamp = cs.endTimestamp,
            predicate = cs.predicate, columns = cs.columns)
          val specName = s"${dirName}_${cs.name}"
          cdfNames += specName
          checkAssertion(cs, specsDir.resolve(s"$specName.json"), warnings)
        } catch {
          case e: Exception =>
            warnings += s"CDF '${cs.name}' infra failure: ${e.getMessage}"
        }
      }

      // Domain metadata specs
      for (dm <- ts.domainMetadataSpecs) {
        try {
          DomainMetadataCapture.capture(spark, dirName, destTablePath, specsDir,
            domain = dm.domain, configuration = dm.configuration, removed = dm.removed,
            version = dm.version, name = dm.name)
          checkAssertion(dm, specsDir.resolve(s"${dirName}_${dm.name}.json"), warnings)
        } catch {
          case e: Exception => warnings += s"DomainMetadata '${dm.name}': ${e.getMessage}"
        }
      }

      // AppTxn specs
      for (tx <- ts.txnSpecs) {
        try {
          AppTxnCapture.capture(spark, dirName, destTablePath, specsDir,
            appId = tx.appId, expectedTxnVersion = tx.txnVersion,
            version = tx.version, name = tx.name)
          checkAssertion(tx, specsDir.resolve(s"${dirName}_${tx.name}.json"), warnings)
        } catch {
          case e: Exception => warnings += s"Txn '${tx.name}': ${e.getMessage}"
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

      // Write spec
      if (ts.generateWriteSpec && ts.writeBuilder.isDefined) {
        try {
          ts.writeBuilder.get.buildSpec(spark, destTablePath, testOutputDir)
        } catch {
          case e: Exception => warnings += s"WriteSpec build: ${e.getMessage}"
        }
      }

      // table_info.json
      TableInfoWriter.write(spark, destTablePath, testOutputDir,
        name = dirName, description = ts.description, tags = ts.tags)

      // Repro
      saveRepro(testOutputDir, scriptContent)

      // Write spec validation: replay write spec, then verify all specs pass
      if (ts.generateWriteSpec && ts.writeBuilder.isDefined) {
        try {
          val replayWarnings = WriteSpecValidator.validate(spark, testOutputDir, dirName)
          warnings ++= replayWarnings
        } catch {
          case e: Exception => warnings += s"WriteSpec validation: ${e.getMessage}"
        }
      }

      val total = snapshotNames.size + readNames.size + cdfNames.size +
        ts.domainMetadataSpecs.size + ts.txnSpecs.size +
        ts.checkpointSpecs.size + ts.crcSpecs.size
      println(s"  $dirName: $total specs")

      WorkloadResult(testOutputDir.toString, dirName, total,
        readNames.toSeq, snapshotNames.toSeq, cdfNames.toSeq,
        warnings.isEmpty, warnings.toSeq)
    } catch {
      case e: Exception =>
        System.err.println(s"  ERROR $dirName: ${e.getMessage}")
        e.printStackTrace()
        WorkloadResult(testOutputDir.toString, dirName, 0,
          Seq.empty, Seq.empty, Seq.empty, false,
          Seq(s"Failed: ${e.getMessage}"))
    }
  }

  private def saveRepro(dir: Path, content: Option[String]): Unit = {
    val reproDir = dir.resolve("repro")
    Files.createDirectories(reproDir)
    content match {
      case Some(c) => Files.write(reproDir.resolve("generate.scala"), c.getBytes("UTF-8"))
      case None => Files.write(reproDir.resolve("generate.scala"),
        "// Generated interactively.\n".getBytes("UTF-8"))
    }
  }

  private def resolveSourceScript(explicit: String): Option[Path] = {
    def resolve(s: String): Option[Path] = {
      val p = Paths.get(s)
      if (Files.exists(p)) Some(p.toAbsolutePath) else None
    }

    Option(explicit).flatMap(resolve)
      .orElse(sys.env.get("WORKLOAD_SOURCE_SCRIPT").flatMap(resolve))
      .orElse {
        try {
          val cmd = System.getProperty("sun.java.command", "")
          val pat = """-i\s+"([^"]+)"|--init\s+"([^"]+)"|-i\s+(\S+)|--init\s+(\S+)""".r
          pat.findFirstMatchIn(cmd).flatMap { m =>
            Seq(m.group(1), m.group(2), m.group(3), m.group(4))
              .find(_ != null).flatMap(resolve)
          }
        } catch { case _: Exception => None }
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
 * (read, snapshot, cdf, etc.), metadata specs (domainMetadata, appTxn, checkpoint,
 * crc), and table mutations (mutateTable, modifyCommitActions).
 *
 * Write operations are not available on TableHandle — use [[WriteHandle]] instead.
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
// WriteHandle — returned by createTableOp() / writeSpec()
// ---------------------------------------------------------------------------

/**
 * Handle for structured write operations. Created from [[createTableOp]] (which
 * creates the table and records the DDL) or [[writeSpec]] (which wraps a
 * SQL-created [[TableHandle]]).
 *
 * Accepts write operations: insertOp, updateOp, deleteOp, truncateOp,
 * setPropertiesOp, addColumnsOp, etc.
 *
 * Call [[registerWriteSpec]] to finalize the write phase and obtain a
 * [[TableHandle]] for declaring read/snapshot/CDF specs.
 */
class WriteHandle private[workload] (
    private[workload] val table: TableHandle)

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

  /** CDF spec. Name auto-generated from version bounds. */
  def cdf(
      table: TableHandle,
      startVersion: java.lang.Long = null,
      endVersion: java.lang.Long = null,
      startTimestamp: String = null,
      endTimestamp: String = null,
      predicate: String = null,
      columns: Seq[String] = null,
      name: String = null): SpecRef[CdfSpec] = {
    val specName = if (name != null) name else autoCdfName(
      opt(startVersion),
      opt(endVersion),
      Option(startTimestamp), Option(endTimestamp))
    requireUnique(table, specName)
    val config = CdfSpecConfig(
      specName,
      opt(startVersion),
      opt(endVersion),
      Option(startTimestamp), Option(endTimestamp),
      Option(predicate), Option(columns))
    getTableSpec(table).cdfSpecs += config
    new SpecRef(config)
  }

  /** Domain metadata spec. */
  def domainMetadata(
      table: TableHandle,
      domain: String,
      configuration: String,
      removed: Boolean = false,
      version: java.lang.Long = null,
      name: String = null): SpecRef[DomainMetadataSpec] = {
    val specName = if (name != null) name else s"dm_$domain"
    requireUnique(table, specName)
    val config = DomainMetadataSpecConfig(
      specName, domain, configuration, removed,
      opt(version))
    getTableSpec(table).domainMetadataSpecs += config
    new SpecRef(config)
  }

  /** SetTransaction spec. */
  def appTxn(
      table: TableHandle,
      appId: String,
      txnVersion: Long,
      version: java.lang.Long = null,
      name: String = null): SpecRef[TxnSpec] = {
    val specName = if (name != null) name else s"txn_$appId"
    requireUnique(table, specName)
    val config = TxnSpecConfig(
      specName, appId, txnVersion,
      opt(version))
    getTableSpec(table).txnSpecs += config
    new SpecRef(config)
  }

  // ---- Write specs ----

  /** Wrap a SQL-created TableHandle for structured write operations. */
  def writeSpec(table: TableHandle): WriteHandle = {
    getWriteBuilder(table)
    new WriteHandle(table)
  }

  /** Finalize write operations and register the write spec for output. */
  def registerWriteSpec(w: WriteHandle): TableHandle = {
    getTableSpec(w.table).generateWriteSpec = true
    w.table
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

  /** Record a create_table operation in the write spec without executing SQL. */
  def recordCreateTable(w: WriteHandle, schemaDDL: String): Unit = {
    getWriteBuilder(w.table).recordCreateTable(schemaDDL, Map.empty, Seq.empty)
  }

  def recordCreateTable(w: WriteHandle, schemaDDL: String, props: Map[String, String]): Unit = {
    getWriteBuilder(w.table).recordCreateTable(schemaDDL, props, Seq.empty)
  }

  def recordCreateTable(
      w: WriteHandle,
      schemaDDL: String,
      props: Map[String, String],
      partCols: Seq[String]): Unit = {
    getWriteBuilder(w.table).recordCreateTable(schemaDDL, props, partCols)
  }

  /**
   * Create a table via SQL and record the operation for write_spec.json.
   * Schema is a SQL DDL string, e.g. "id INT, name STRING NOT NULL".
   * Returns a WriteHandle for further structured write operations.
   */
  def createTableOp(
      tableName: String,
      schema: String,
      properties: Map[String, String],
      partitionColumns: Seq[String]): WriteHandle = {
    createTableOpImpl(tableName, schema, properties, partitionColumns)
  }

  def createTableOp(
      tableName: String,
      schema: String,
      partitionColumns: Seq[String]): WriteHandle = {
    createTableOpImpl(tableName, schema, Map.empty, partitionColumns)
  }

  def createTableOp(
      tableName: String,
      schema: String,
      properties: Map[String, String]): WriteHandle = {
    createTableOpImpl(tableName, schema, properties, Seq.empty)
  }

  def createTableOp(tableName: String, schema: String): WriteHandle = {
    createTableOpImpl(tableName, schema, Map.empty, Seq.empty)
  }

  private def createTableOpImpl(
      tableName: String,
      schemaDDL: String,
      properties: Map[String, String],
      partitionColumns: Seq[String]): WriteHandle = {
    val partitionClause = if (partitionColumns.nonEmpty) {
      s" PARTITIONED BY (${partitionColumns.mkString(", ")})"
    } else ""

    val propsClause = if (properties.nonEmpty) {
      val propsStr = properties.map { case (k, v) => s"'$k' = '$v'" }.mkString(", ")
      s" TBLPROPERTIES ($propsStr)"
    } else ""

    val createSql = s"CREATE TABLE $tableName ($schemaDDL) USING delta$partitionClause$propsClause"
    sql(createSql)

    val t = registerTable(tableName)
    getWriteBuilder(t).recordCreateTable(schemaDDL, properties, partitionColumns)
    new WriteHandle(t)
  }

  /**
   * Execute CREATE OR REPLACE TABLE and record the operation for write_spec.json.
   * Used for RTAS (Replace Table As Select) style queries.
   */
  def replaceTableOp(
      w: WriteHandle,
      schemaDDL: String,
      properties: Map[String, String] = Map.empty,
      partitionColumns: Seq[String] = Seq.empty): Unit = {
    val partitionClause = if (partitionColumns.nonEmpty) {
      s" PARTITIONED BY (${partitionColumns.mkString(", ")})"
    } else ""
    val propsClause = if (properties.nonEmpty) {
      val propsStr = properties.map { case (k, v) => s"'$k' = '$v'" }.mkString(", ")
      s" TBLPROPERTIES ($propsStr)"
    } else ""
    val replaceSql =
      s"CREATE OR REPLACE TABLE ${w.table.tableName} ($schemaDDL) USING delta$partitionClause$propsClause"
    sql(replaceSql)
    getWriteBuilder(w.table).recordReplaceTable(schemaDDL, properties, partitionColumns)
  }

  def insertOp(w: WriteHandle): Unit = {
    getWriteBuilder(w.table).recordInsert()
  }

  def insertOp(w: WriteHandle, rows: Seq[Map[String, Any]]): Unit = {
    if (rows.isEmpty) {
      getWriteBuilder(w.table).recordInsert()
      return
    }

    val columns = rows.head.keys.toSeq
    val colList = columns.map(c => s"`$c`").mkString(", ")

    val valuesList = rows.map { row =>
      val values = columns.map { col =>
        row.get(col) match {
          case Some(s: String) => s"'$s'"
          case Some(null) => "NULL"
          case Some(v) => v.toString
          case None => "NULL"
        }
      }
      s"(${values.mkString(", ")})"
    }.mkString(", ")

    val insertSql = s"INSERT INTO ${w.table.tableName} ($colList) VALUES $valuesList"
    sql(insertSql)
    getWriteBuilder(w.table).recordInsert()
  }

  def deleteOp(w: WriteHandle, predicate: String): Unit = {
    val deleteSql = s"DELETE FROM ${w.table.tableName} WHERE $predicate"
    sql(deleteSql)
    getWriteBuilder(w.table).recordDelete(predicate)
  }

  def updateOp(w: WriteHandle, predicate: String, set: Map[String, String]): Unit = {
    val setClauses = set.map { case (k, v) => s"$k = $v" }.mkString(", ")
    val updateSql = s"UPDATE ${w.table.tableName} SET $setClauses WHERE $predicate"
    sql(updateSql)
    getWriteBuilder(w.table).recordUpdate(predicate, set)
  }

  def setPropertiesOp(w: WriteHandle, props: Map[String, String]): Unit = {
    require(props.nonEmpty, "setPropertiesOp requires at least one property")
    val setClause = props.map { case (k, v) => s"'$k' = '$v'" }.mkString(", ")
    sql(s"ALTER TABLE ${w.table.tableName} SET TBLPROPERTIES ($setClause)")
    getWriteBuilder(w.table).recordSetProperties(props)
  }

  def unsetPropertiesOp(w: WriteHandle, props: Seq[String]): Unit = {
    require(props.nonEmpty, "unsetPropertiesOp requires at least one property")
    val unsetClause = props.map(k => s"'$k'").mkString(", ")
    sql(s"ALTER TABLE ${w.table.tableName} UNSET TBLPROPERTIES ($unsetClause)")
    getWriteBuilder(w.table).recordUnsetProperties(props)
  }

  def addColumnsOp(w: WriteHandle, columnsDDL: String): Unit = {
    require(columnsDDL.nonEmpty, "addColumnsOp requires a non-empty DDL string")
    sql(s"ALTER TABLE ${w.table.tableName} ADD COLUMNS ($columnsDDL)")
    getWriteBuilder(w.table).recordAddColumns(columnsDDL)
  }

  def renameColumnOp(w: WriteHandle, oldName: String, newName: String): Unit = {
    sql(s"ALTER TABLE ${w.table.tableName} RENAME COLUMN $oldName TO $newName")
    getWriteBuilder(w.table).recordRenameColumn(oldName, newName)
  }

  def dropColumnsOp(w: WriteHandle, columns: Seq[String]): Unit = {
    require(columns.nonEmpty, "dropColumnsOp requires at least one column")
    if (columns.size == 1) {
      sql(s"ALTER TABLE ${w.table.tableName} DROP COLUMN ${columns.head}")
    } else {
      val colList = columns.mkString(", ")
      sql(s"ALTER TABLE ${w.table.tableName} DROP COLUMNS ($colList)")
    }
    getWriteBuilder(w.table).recordDropColumns(columns)
  }

  /**
   * Combined update_properties operation: SET and/or UNSET table properties in ONE commit.
   * Uses DeltaLog transaction API to ensure a single atomic commit.
   */
  def updatePropertiesOp(
      w: WriteHandle,
      setProps: Map[String, String] = Map.empty,
      unsetProps: Seq[String] = Seq.empty): Unit = {
    require(setProps.nonEmpty || unsetProps.nonEmpty,
      "updatePropertiesOp requires at least one property to set or unset")

    val deltaLog = DeltaLog.forTable(spark, w.table.sourcePath.toString)
    val txn = deltaLog.startTransaction()
    val currentMetadata = txn.metadata

    val newConfig = (currentMetadata.configuration ++ setProps) -- unsetProps
    val newMetadata = currentMetadata.copy(configuration = newConfig)

    txn.updateMetadata(newMetadata)
    val operation = if (unsetProps.isEmpty && setProps.nonEmpty) {
      org.apache.spark.sql.delta.DeltaOperations.SetTableProperties(setProps)
    } else {
      org.apache.spark.sql.delta.DeltaOperations.ManualUpdate
    }
    txn.commit(Seq.empty, operation)

    getWriteBuilder(w.table).recordUpdateProperties(setProps, unsetProps)
  }

  /**
   * Combined evolve_schema operation: ADD, RENAME, and/or DROP columns in ONE commit.
   * Uses DeltaLog transaction API to ensure a single atomic commit.
   */
  def evolveSchemaOp(
      w: WriteHandle,
      addColumnsDDL: String = "",
      renameColumns: Map[String, String] = Map.empty,
      dropColumns: Seq[String] = Seq.empty): Unit = {
    require(addColumnsDDL.nonEmpty || renameColumns.nonEmpty || dropColumns.nonEmpty,
      "evolveSchemaOp requires at least one schema change")

    val deltaLog = DeltaLog.forTable(spark, w.table.sourcePath.toString)
    val txn = deltaLog.startTransaction()
    val currentMetadata = txn.metadata
    val currentSchema = currentMetadata.schema

    import org.apache.spark.sql.types._

    var fields = currentSchema.fields.toBuffer

    if (addColumnsDDL.nonEmpty) {
      fields ++= StructType.fromDDL(addColumnsDDL).fields
    }

    for ((oldName, newName) <- renameColumns) {
      val idx = fields.indexWhere(_.name == oldName)
      if (idx >= 0) {
        fields(idx) = fields(idx).copy(name = newName)
      }
    }

    fields = fields.filterNot(f => dropColumns.contains(f.name))

    val newSchema = StructType(fields.toSeq)
    val newMetadata = currentMetadata.copy(schemaString = newSchema.json)
    txn.updateMetadata(newMetadata)
    txn.commit(Seq.empty, org.apache.spark.sql.delta.DeltaOperations.ManualUpdate)

    getWriteBuilder(w.table).recordEvolveSchema(addColumnsDDL, renameColumns, dropColumns)
  }

  def truncateOp(w: WriteHandle): Unit = {
    sql(s"TRUNCATE TABLE ${w.table.tableName}")
    getWriteBuilder(w.table).recordTruncate()
  }

  def restoreOp(w: WriteHandle, version: Long): Unit = {
    sql(s"RESTORE TABLE ${w.table.tableName} TO VERSION AS OF $version")
    getWriteBuilder(w.table).recordRestore(version)
  }

  /**
   * Record a low-level commit (raw Delta actions). No SQL is executed — the test
   * author is responsible for writing the actual commit via [[mutateTable]].
   * This only records the operation in the write spec so harness consumers know
   * what actions to replay.
   */
  def commitOp(
      w: WriteHandle,
      schemaDDL: Option[String] = None,
      tableProperties: Option[Map[String, String]] = None,
      txn: Option[AppTxn] = None,
      addFiles: Option[Seq[AddFileAction]] = None,
      removeFiles: Option[Seq[RemoveFileAction]] = None,
      addDomainMetadata: Option[Seq[AddDomainMetadata]] = None,
      removeDomainMetadata: Option[Seq[String]] = None): Unit = {
    getWriteBuilder(w.table).recordCommit(
      schemaDDL = schemaDDL,
      tableProperties = tableProperties,
      txn = txn,
      addFiles = addFiles,
      removeFiles = removeFiles,
      addDomainMetadata = addDomainMetadata,
      removeDomainMetadata = removeDomainMetadata)
  }

  private val _writeBuilders = mutable.HashMap[String, WriteSpecBuilder]()

  private def getWriteBuilder(table: TableHandle): WriteSpecBuilder = {
    val key = s"${workloadName}_${table.tableName}"
    val builder = _writeBuilders.getOrElseUpdate(key, new WriteSpecBuilder())
    getTableSpec(table).writeBuilder = Some(builder)
    builder
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

  private def autoCdfName(
      startVersion: Option[Long],
      endVersion: Option[Long],
      startTimestamp: Option[String],
      endTimestamp: Option[String]): String = {
    val parts = mutable.ArrayBuffer[String]("cdf")
    startVersion.foreach(v => parts += s"v$v")
    startTimestamp.foreach(ts => parts += "ts_" + ts.take(10))
    endVersion.foreach(v => parts += s"to_v$v")
    endTimestamp.foreach(ts => parts += "to_ts_" + ts.take(10))
    if (parts.size == 1) parts += "all"
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

  /** CDF spec. Name auto-generated from version bounds. */
  def cdf(
      table: TableHandle,
      startVersion: java.lang.Long = null,
      endVersion: java.lang.Long = null,
      startTimestamp: String = null,
      endTimestamp: String = null,
      predicate: String = null,
      columns: Seq[String] = null,
      name: String = null): SpecRef[CdfSpec] =
    current.cdf(table, startVersion, endVersion, startTimestamp, endTimestamp, predicate, columns, name)

  /** Domain metadata spec. */
  def domainMetadata(
      table: TableHandle,
      domain: String,
      configuration: String,
      removed: Boolean = false,
      version: java.lang.Long = null,
      name: String = null): SpecRef[DomainMetadataSpec] = current.domainMetadata(table, domain, configuration, removed, version, name)

  /** SetTransaction spec. */
  def appTxn(
      table: TableHandle,
      appId: String,
      txnVersion: Long,
      version: java.lang.Long = null,
      name: String = null): SpecRef[TxnSpec] = current.appTxn(table, appId, txnVersion, version, name)

  /** Wrap a SQL-created TableHandle for structured write operations. */
  def writeSpec(table: TableHandle): WriteHandle = current.writeSpec(table)

  /** Finalize write operations and register the write spec for output. */
  def registerWriteSpec(w: WriteHandle): TableHandle = current.registerWriteSpec(w)

  /** Force Spark to write a checkpoint file for the given SQL table name. */
  def forceCheckpoint(tableName: String): Unit = current.forceCheckpoint(tableName)

  /** Checkpoint verification spec at a specific version. */
  def checkpoint(table: TableHandle, version: Long): SpecRef[CheckpointSpec] = current.checkpoint(table, version)

  /** CRC verification spec at a specific version. */
  def crc(table: TableHandle, version: Long): SpecRef[CrcSpec] = current.crc(table, version)

  // Write operation methods (all take WriteHandle)
  def recordCreateTable(w: WriteHandle, schemaDDL: String): Unit =
    current.recordCreateTable(w, schemaDDL)
  def recordCreateTable(w: WriteHandle, schemaDDL: String, props: Map[String, String]): Unit =
    current.recordCreateTable(w, schemaDDL, props)
  def recordCreateTable(w: WriteHandle, schemaDDL: String, props: Map[String, String], partCols: Seq[String]): Unit =
    current.recordCreateTable(w, schemaDDL, props, partCols)

  def createTableOp(tableName: String, schema: String, properties: Map[String, String], partitionColumns: Seq[String]): WriteHandle =
    current.createTableOp(tableName, schema, properties, partitionColumns)
  def createTableOp(tableName: String, schema: String, partitionColumns: Seq[String]): WriteHandle =
    current.createTableOp(tableName, schema, partitionColumns)
  def createTableOp(tableName: String, schema: String, properties: Map[String, String]): WriteHandle =
    current.createTableOp(tableName, schema, properties)
  def createTableOp(tableName: String, schema: String): WriteHandle =
    current.createTableOp(tableName, schema)

  def replaceTableOp(w: WriteHandle, schemaDDL: String,
      properties: Map[String, String] = Map.empty,
      partitionColumns: Seq[String] = Seq.empty): Unit =
    current.replaceTableOp(w, schemaDDL, properties, partitionColumns)

  def insertOp(w: WriteHandle): Unit = current.insertOp(w)
  def insertOp(w: WriteHandle, rows: Seq[Map[String, Any]]): Unit = current.insertOp(w, rows)
  def deleteOp(w: WriteHandle, predicate: String): Unit = current.deleteOp(w, predicate)
  def updateOp(w: WriteHandle, predicate: String, set: Map[String, String]): Unit =
    current.updateOp(w, predicate, set)
  def setPropertiesOp(w: WriteHandle, props: Map[String, String]): Unit =
    current.setPropertiesOp(w, props)
  def unsetPropertiesOp(w: WriteHandle, props: Seq[String]): Unit =
    current.unsetPropertiesOp(w, props)
  def addColumnsOp(w: WriteHandle, columnsDDL: String): Unit =
    current.addColumnsOp(w, columnsDDL)
  def renameColumnOp(w: WriteHandle, oldName: String, newName: String): Unit =
    current.renameColumnOp(w, oldName, newName)
  def dropColumnsOp(w: WriteHandle, columns: Seq[String]): Unit =
    current.dropColumnsOp(w, columns)
  def truncateOp(w: WriteHandle): Unit = current.truncateOp(w)
  def restoreOp(w: WriteHandle, version: Long): Unit = current.restoreOp(w, version)
  def commitOp(
      w: WriteHandle,
      schemaDDL: Option[String] = None,
      tableProperties: Option[Map[String, String]] = None,
      txn: Option[AppTxn] = None,
      addFiles: Option[Seq[AddFileAction]] = None,
      removeFiles: Option[Seq[RemoveFileAction]] = None,
      addDomainMetadata: Option[Seq[AddDomainMetadata]] = None,
      removeDomainMetadata: Option[Seq[String]] = None): Unit =
    current.commitOp(w, schemaDDL, tableProperties, txn, addFiles, removeFiles,
      addDomainMetadata, removeDomainMetadata)

  def updatePropertiesOp(
      w: WriteHandle,
      setProps: Map[String, String] = Map.empty,
      unsetProps: Seq[String] = Seq.empty): Unit =
    current.updatePropertiesOp(w, setProps, unsetProps)
  def evolveSchemaOp(
      w: WriteHandle,
      addColumnsDDL: String = "",
      renameColumns: Map[String, String] = Map.empty,
      dropColumns: Seq[String] = Seq.empty): Unit =
    current.evolveSchemaOp(w, addColumnsDDL, renameColumns, dropColumns)

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
  val cdfSpecs = mutable.ArrayBuffer[CdfSpecConfig]()
  val domainMetadataSpecs = mutable.ArrayBuffer[DomainMetadataSpecConfig]()
  val txnSpecs = mutable.ArrayBuffer[TxnSpecConfig]()
  val mutations = mutable.ArrayBuffer[Path => Unit]()
  val checkpointSpecs = mutable.ArrayBuffer[CheckpointSpecConfig]()
  val crcSpecs = mutable.ArrayBuffer[CrcSpecConfig]()
  var generateWriteSpec: Boolean = false
  var writeBuilder: Option[WriteSpecBuilder] = None
}

private[workload] case class WorkloadDef(
    name: String,
    description: String,
    tags: Seq[String],
    body: WorkloadContext => Unit)

case class WorkloadResult(
    outputDir: String,
    testId: String,
    specsGenerated: Int,
    readSpecs: Seq[String],
    snapshotSpecs: Seq[String],
    cdfSpecs: Seq[String],
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

private[workload] case class CdfSpecConfig(
    name: String, startVersion: Option[Long], endVersion: Option[Long],
    startTimestamp: Option[String], endTimestamp: Option[String],
    predicate: Option[String], columns: Option[Seq[String]]) extends HasAssertion[CdfSpec] {
  val deserialize = (n: com.fasterxml.jackson.databind.JsonNode) =>
    JsonUtil.mapper.treeToValue(n, classOf[CdfSpec])
}

private[workload] case class DomainMetadataSpecConfig(
    name: String, domain: String, configuration: String,
    removed: Boolean, version: Option[Long]) extends HasAssertion[DomainMetadataSpec] {
  val deserialize = (n: com.fasterxml.jackson.databind.JsonNode) =>
    JsonUtil.mapper.treeToValue(n, classOf[DomainMetadataSpec])
}

private[workload] case class TxnSpecConfig(
    name: String, appId: String, txnVersion: Long, version: Option[Long]) extends HasAssertion[TxnSpec] {
  val deserialize = (n: com.fasterxml.jackson.databind.JsonNode) =>
    JsonUtil.mapper.treeToValue(n, classOf[TxnSpec])
}

private[workload] case class CheckpointSpecConfig(version: Long) extends HasAssertion[CheckpointSpec] {
  val deserialize = (n: com.fasterxml.jackson.databind.JsonNode) =>
    JsonUtil.mapper.treeToValue(n, classOf[CheckpointSpec])
}

private[workload] case class CrcSpecConfig(version: Long) extends HasAssertion[CrcSpec] {
  val deserialize = (n: com.fasterxml.jackson.databind.JsonNode) =>
    JsonUtil.mapper.treeToValue(n, classOf[CrcSpec])
}
