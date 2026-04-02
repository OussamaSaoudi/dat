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

import java.nio.file.{Files, Path, Paths}

import scala.collection.mutable

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.DeltaLog

/**
 * Internal workload generation engine. Use [[WorkloadSuite]] as the public API:
 *
 * {{{
 * new WorkloadSuite("deletion_vectors") {
 *   test("dv_delete_basic", "Deletion vectors after DELETE") { w =>
 *     w.sql("CREATE TABLE tbl (id INT, name STRING) USING delta " +
 *       "TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")
 *     w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
 *     w.sql("DELETE FROM tbl WHERE id = 2")
 *     val t = w.table("tbl")
 *     w.read(t)
 *     w.read(t, version = 0)
 *     w.read(t, predicate = "id > 1")
 *     w.snapshot(t)
 *   }
 * }.runAll()
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
      val ctx = new WorkloadContext(spark, wd.name)
      try {
        wd.body(ctx)
        // Single-table workloads: use workload name as directory name
        // Multi-table workloads: use {workload}_{table}
        if (ctx.tableSpecs.size == 1) {
          ctx.tableSpecs.head.outputName = wd.name
        }
        ctx.tableSpecs.map { ts =>
          generateTable(spark, ts, Paths.get(outputDir), scriptContent, force)
        }
      } catch {
        case e: Exception =>
          System.err.println(s"  ERROR in ${wd.name}: ${e.getMessage}")
          e.printStackTrace()
          Seq.empty
      } finally {
        ctx.cleanup()
      }
    }

    println("\n=== Results ===")
    results.foreach { r =>
      val status = if (r.validationPassed) "OK" else "WARN"
      println(f"  [$status%4s] ${r.testId}%-50s ${r.specsGenerated}%d specs" +
        (if (r.warnings.nonEmpty) s"  (${r.warnings.size} warnings)" else ""))
    }
    val passed = results.count(_.validationPassed)
    val failed = results.size - passed
    println(s"\n$passed passed" +
      (if (failed > 0) s", $failed with warnings" else ""))

    registry.clear()

    // Remind about System.exit(0) in spark-shell context
    if (System.getProperty("sun.java.command", "").contains("spark-shell") ||
        System.getProperty("sun.java.command", "").contains("SparkSubmit")) {
      println("\nDone. Call System.exit(0) to terminate spark-shell.")
    }

    results.toSeq
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

    val ctx = new WorkloadContext(spark, name)
    try {
      registry(name).body(ctx)
      ctx.tableSpecs.map(ts => generateTable(spark, ts, Paths.get(outputDir), scriptContent)).toSeq
    } finally {
      ctx.cleanup()
    }
  }

  def reset(): Unit = registry.clear()
  def list(): Seq[String] = registry.keys.toSeq

  // Internal: generate one table's workload directory

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
      DeltaLog.clearCache()

      // Apply mutations on the copied table
      ts.mutations.foreach { mutate =>
        mutate(destTablePath)
        DeltaLog.clearCache()
      }

      val warnings = mutable.ArrayBuffer[String]()

      // Snapshot specs
      val snapshotNames = mutable.ArrayBuffer[String]()
      if (ts.snapshotAllVersions) {
        try {
          val dl = DeltaLog.forTable(spark, destTablePath.toString)
          DeltaLog.clearCache()
          val latest = dl.update().version
          for (v <- 0L to latest) {
            SnapshotCapture.capture(spark, dirName, destTablePath, specsDir, version = Some(v))
            snapshotNames += s"${dirName}_snapshot_v$v"
          }
        } catch {
          case e: Exception =>
            warnings += s"Snapshot history failed: ${e.getMessage}"
        }
      }
      val explicits = if (ts.snapshotSpecs.isEmpty && !ts.snapshotAllVersions) {
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
          readNames += s"${dirName}_${rs.name}"
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
          cdfNames += s"${dirName}_${cs.name}"
        } catch {
          case e: Exception =>
            warnings += s"CDF '${cs.name}' infra failure: ${e.getMessage}"
        }
      }

      // Domain metadata specs
      for (dm <- ts.domainMetadataSpecs) {
        try {
          val specName = s"${dirName}_${dm.name}"

          // Try Snapshot API first (works on DBR), fall back to scanning commit JSON
          val domainJsons: Seq[com.fasterxml.jackson.databind.JsonNode] = try {
            DeltaLog.clearCache()
            val dl = DeltaLog.forTable(spark, destTablePath.toString)
            val snapshot = dm.version match {
              case Some(v) => dl.getSnapshotAt(v)
              case None => dl.update()
            }
            val method = snapshot.getClass.getMethod("domainMetadata")
            val actions = method.invoke(snapshot).asInstanceOf[Seq[_]]
            actions.map { action =>
              val jsonMethod = action.getClass.getMethod("json")
              JsonUtil.mapper.readTree(jsonMethod.invoke(action).asInstanceOf[String])
            }
          } catch {
            case _: NoSuchMethodException =>
              // OSS Delta: scan commit JSON files directly
              scanDomainMetadataFromLog(destTablePath, dm.version)
            case _: Exception =>
              scanDomainMetadataFromLog(destTablePath, dm.version)
          }

          // Find matching domain
          val matchingDomain = domainJsons.find { node =>
            val dmNode = Option(node.get("domainMetadata")).getOrElse(node)
            dmNode.has("domain") && dmNode.get("domain").asText() == dm.domain
          }
          if (dm.removed) {
            require(matchingDomain.isEmpty,
              s"Domain metadata validation FAILED for $specName: " +
                s"domain '${dm.domain}' should be removed but is still present")
          } else {
            require(matchingDomain.isDefined,
              s"Domain metadata validation FAILED for $specName: " +
                s"domain '${dm.domain}' not found in snapshot")
            val dmNode = matchingDomain.get
            val actual = Option(dmNode.get("domainMetadata")).getOrElse(dmNode)
            if (actual.has("configuration")) {
              val actualConfig = actual.get("configuration").asText()
              require(actualConfig == dm.configuration,
                s"Domain metadata validation FAILED for $specName: " +
                  s"configuration expected='${dm.configuration}' actual='$actualConfig'")
            }
          }

          val spec = new java.util.LinkedHashMap[String, Any]()
          spec.put("type", "domain_metadata")
          dm.version.foreach(v => spec.put("version", v))
          val expected = new java.util.LinkedHashMap[String, Any]()
          expected.put("domain", dm.domain)
          expected.put("configuration", dm.configuration)
          expected.put("removed", dm.removed)
          spec.put("expected", expected)
          JsonUtil.writeJson(specsDir.resolve(s"$specName.json"), spec)
        } catch {
          case e: Exception => warnings += s"DomainMetadata '${dm.name}': ${e.getMessage}"
        }
      }

      // Txn specs
      for (tx <- ts.txnSpecs) {
        try {
          val specName = s"${dirName}_${tx.name}"

          // Validate: scan commit files for the SetTransaction action
          val deltaLogDir = destTablePath.resolve("_delta_log")
          val txnStream = Files.list(deltaLogDir)
          val foundTxn = try {
            scala.collection.JavaConverters.asScalaIteratorConverter(txnStream.iterator()).asScala
              .filter(_.toString.endsWith(".json"))
              .toSeq.sortBy(_.getFileName.toString) // ensure lexicographic order
              .flatMap { commitFile =>
                new String(Files.readAllBytes(commitFile), "UTF-8").split("\n")
                  .filter(_.contains("\"txn\""))
                  .flatMap { line =>
                    try {
                      val node = JsonUtil.mapper.readTree(line)
                      val txnNode = node.get("txn")
                      if (txnNode != null && txnNode.has("appId") &&
                          txnNode.get("appId").asText() == tx.appId) {
                        Some(txnNode.get("version").asLong())
                      } else None
                    } catch { case _: Exception => None }
                  }
              }.toSeq.lastOption // last occurrence wins (latest version)
          } finally {
            txnStream.close()
          }

          require(foundTxn.isDefined,
            s"Txn validation FAILED for $specName: " +
              s"SetTransaction for appId='${tx.appId}' not found in delta log")
          require(foundTxn.get == tx.txnVersion,
            s"Txn validation FAILED for $specName: " +
              s"appId='${tx.appId}' version expected=${tx.txnVersion} actual=${foundTxn.get}")

          val spec = new java.util.LinkedHashMap[String, Any]()
          spec.put("type", "txn")
          tx.version.foreach(v => spec.put("version", v))
          val expected = new java.util.LinkedHashMap[String, Any]()
          expected.put("appId", tx.appId)
          expected.put("txnVersion", tx.txnVersion)
          spec.put("expected", expected)
          JsonUtil.writeJson(specsDir.resolve(s"$specName.json"), spec)
        } catch {
          case e: Exception => warnings += s"Txn '${tx.name}': ${e.getMessage}"
        }
      }

      // table_info.json + test_info.json
      TableInfoWriter.write(spark, destTablePath, testOutputDir,
        name = dirName, description = ts.description, tags = ts.tags)
      val testInfo = new java.util.LinkedHashMap[String, Any]()
      testInfo.put("test_id", dirName)
      testInfo.put("test_name", ts.description)
      testInfo.put("workload_count",
        (ts.readSpecs.size + ts.cdfSpecs.size + ts.snapshotSpecs.size +
          ts.domainMetadataSpecs.size + ts.txnSpecs.size))
      JsonUtil.writeJson(testOutputDir.resolve("test_info.json"), testInfo)

      // Repro
      saveRepro(testOutputDir, scriptContent)

      val total = snapshotNames.size + readNames.size + cdfNames.size
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

  /** Scan commit JSON files to reconstruct domain metadata state (OSS fallback). */
  private def scanDomainMetadataFromLog(
      tablePath: Path,
      version: Option[Long]): Seq[com.fasterxml.jackson.databind.JsonNode] = {
    val logDir = tablePath.resolve("_delta_log")
    val stream = Files.list(logDir)
    try {
      val commitFiles = scala.collection.JavaConverters
        .asScalaIteratorConverter(stream.iterator()).asScala
        .filter(_.toString.endsWith(".json"))
        .toSeq.sortBy(_.getFileName.toString)
      val domainState = new java.util.LinkedHashMap[String, com.fasterxml.jackson.databind.JsonNode]()
      for (commitFile <- commitFiles) {
        val fileVersion = commitFile.getFileName.toString.stripSuffix(".json").toLong
        if (version.forall(fileVersion <= _)) {
          val lines = new String(Files.readAllBytes(commitFile), "UTF-8").split("\n")
          for (line <- lines if line.contains("\"domainMetadata\"")) {
            try {
              val node = JsonUtil.mapper.readTree(line)
              val dmNode = node.get("domainMetadata")
              if (dmNode != null && dmNode.has("domain")) {
                val domain = dmNode.get("domain").asText()
                val removed = dmNode.has("removed") && dmNode.get("removed").asBoolean()
                if (removed) domainState.remove(domain)
                else domainState.put(domain, node)
              }
            } catch { case _: Exception => }
          }
        }
      }
      scala.collection.JavaConverters.asScalaIteratorConverter(
        domainState.values().iterator()).asScala.toSeq
    } finally { stream.close() }
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
// TableHandle — returned by w.table() / w.tableFromPath()
// ---------------------------------------------------------------------------

/**
 * Handle to a Delta table. Passed to spec methods to indicate which table
 * to capture from. Resolved from the Spark catalog or a filesystem path.
 */
class TableHandle private[workload] (
    private[workload] val tableName: String,
    private[workload] val sourcePath: Path,
    private[workload] val ctx: WorkloadContext) {

  /**
   * Get the commit timestamp for a specific version.
   * Returns formatted timestamp (yyyy-MM-dd HH:mm:ss.SSS).
   */
  def getTimestampForVersion(version: Long): String = {
    val commitFile = sourcePath.resolve("_delta_log").resolve(f"$version%020d.json")
    require(Files.exists(commitFile), s"No commit file for version $version: $commitFile")
    val content = new String(Files.readAllBytes(commitFile), "UTF-8")
    val tsMillis = content.split("\n").iterator
      .filter(_.contains("\"commitInfo\""))
      .map { line =>
        val ci = JsonUtil.mapper.readTree(line).get("commitInfo")
        if (ci.has("inCommitTimestamp")) ci.get("inCommitTimestamp").asLong()
        else if (ci.has("timestamp")) ci.get("timestamp").asLong()
        else throw new RuntimeException(s"No timestamp in commitInfo for version $version")
      }.next()
    val fmt = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    fmt.setTimeZone(java.util.TimeZone.getDefault)
    fmt.format(new java.util.Date(tsMillis))
  }
}

// ---------------------------------------------------------------------------
// WorkloadContext — the user's interface inside a workload() block
// ---------------------------------------------------------------------------

class WorkloadContext private[workload] (
    val spark: SparkSession,
    val workloadName: String) {

  private val _createdTables = mutable.ArrayBuffer[String]()
  private[workload] val tableSpecs = mutable.ArrayBuffer[TableSpec]()

  // Per-table state: spec names must be unique within each table
  private val _tableSpecNames = mutable.HashMap[String, mutable.HashSet[String]]()

  // ---- SQL ----

  /** Execute SQL. Tracks CREATE TABLE for cleanup. */
  def sql(statement: String): Unit = {
    val pat = """(?i)CREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\S+)""".r
    pat.findFirstMatchIn(statement).foreach { m =>
      _createdTables += m.group(1).replace("`", "")
    }
    spark.sql(statement)
  }

  // ---- Table handles ----

  /** Get a handle to a managed Spark table. */
  def table(name: String): TableHandle = {
    val path = resolveTablePath(name)
    val handle = new TableHandle(name, path, this)
    ensureTableSpec(handle)
    handle
  }

  /** Get a handle to a table at a filesystem path. */
  def tableFromPath(path: String): TableHandle = {
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
      name: String = null): Unit = {
    val specName = if (name != null) name else autoReadName(
      Option(predicate), Option(version).map(_.longValue()),
      Option(timestamp), Option(columns))
    requireUnique(table, specName)
    getTableSpec(table).readSpecs += ReadSpecConfig(
      specName, Option(predicate), Option(version).map(_.longValue()),
      Option(timestamp), Option(columns))
  }

  /** Snapshot spec. */
  def snapshot(
      table: TableHandle,
      version: java.lang.Long = null,
      timestamp: String = null): Unit = {
    getTableSpec(table).snapshotSpecs += SnapshotSpecConfig(
      Option(version).map(_.longValue()), Option(timestamp))
  }

  /** Snapshot at every version (0 to latest). */
  def snapshotHistory(table: TableHandle): Unit = {
    getTableSpec(table).snapshotAllVersions = true
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
      name: String = null): Unit = {
    val specName = if (name != null) name else autoCdfName(
      Option(startVersion).map(_.longValue()),
      Option(endVersion).map(_.longValue()),
      Option(startTimestamp), Option(endTimestamp))
    requireUnique(table, specName)
    getTableSpec(table).cdfSpecs += CdfSpecConfig(
      specName,
      Option(startVersion).map(_.longValue()),
      Option(endVersion).map(_.longValue()),
      Option(startTimestamp), Option(endTimestamp),
      Option(predicate), Option(columns))
  }

  /** Domain metadata spec. */
  def domainMetadata(
      table: TableHandle,
      domain: String,
      configuration: String,
      removed: Boolean = false,
      version: java.lang.Long = null,
      name: String = null): Unit = {
    val specName = if (name != null) name else s"dm_$domain"
    requireUnique(table, specName)
    getTableSpec(table).domainMetadataSpecs += DomainMetadataSpecConfig(
      specName, domain, configuration, removed,
      Option(version).map(_.longValue()))
  }

  /** SetTransaction spec. */
  def txn(
      table: TableHandle,
      appId: String,
      txnVersion: Long,
      version: java.lang.Long = null,
      name: String = null): Unit = {
    val specName = if (name != null) name else s"txn_$appId"
    requireUnique(table, specName)
    getTableSpec(table).txnSpecs += TxnSpecConfig(
      specName, appId, txnVersion,
      Option(version).map(_.longValue()))
  }

  // ---- Table mutations (applied to copied table before spec capture) ----

  /** Mutate the copied table's filesystem before specs are captured. */
  def mutateTable(table: TableHandle)(mutation: Path => Unit): Unit = {
    getTableSpec(table).mutations += mutation
  }

  /** Modify AddFile actions in a specific commit version. */
  def modifyCommitActions(table: TableHandle, version: Long)(
      modifier: ObjectNode => Unit): Unit = {
    mutateTable(table) { tableDir =>
      val commitFile = tableDir.resolve("_delta_log").resolve(f"$version%020d.json")
      if (Files.exists(commitFile)) {
        val mapper = new ObjectMapper().registerModule(DefaultScalaModule)
        val lines = new String(Files.readAllBytes(commitFile), "UTF-8").split("\n")
        val newLines = lines.map { line =>
          try {
            val node = mapper.readTree(line)
            if (node.has("add")) {
              modifier(node.get("add").asInstanceOf[ObjectNode])
              mapper.writeValueAsString(node)
            } else line
          } catch { case _: Exception => line }
        }
        Files.write(commitFile, java.util.Arrays.asList(newLines: _*))
        TableCopier.invalidateChecksumFilesForModifiedCommit(commitFile)
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
        outputName = outputName,
        description = s"$workloadName — ${handle.tableName}",
        tags = registry.get(workloadName).map(_.tags).getOrElse(Seq.empty),
        sourcePath = handle.sourcePath
      )
      _tableSpecNames(outputName) = mutable.HashSet[String]()
    }
  }

  private def getTableSpec(handle: TableHandle): TableSpec = {
    val outputName = s"${workloadName}_${handle.tableName}"
    tableSpecs.find(_.outputName == outputName).getOrElse(
      throw new RuntimeException(s"No table spec for ${handle.tableName}. Call w.table() first."))
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
    _createdTables.foreach { t =>
      try { spark.sql(s"DROP TABLE IF EXISTS $t") }
      catch { case _: Exception => }
    }
  }

  // Access registry for tags
  private def registry = WorkloadGenerator.registry
}

// ---------------------------------------------------------------------------
// Internal data structures
// ---------------------------------------------------------------------------

private[workload] class TableSpec(
    var outputName: String,
    val description: String,
    val tags: Seq[String],
    val sourcePath: Path) {
  val readSpecs = mutable.ArrayBuffer[ReadSpecConfig]()
  val snapshotSpecs = mutable.ArrayBuffer[SnapshotSpecConfig]()
  val cdfSpecs = mutable.ArrayBuffer[CdfSpecConfig]()
  val domainMetadataSpecs = mutable.ArrayBuffer[DomainMetadataSpecConfig]()
  val txnSpecs = mutable.ArrayBuffer[TxnSpecConfig]()
  val mutations = mutable.ArrayBuffer[Path => Unit]()
  var snapshotAllVersions: Boolean = false
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
    timestamp: Option[String], columns: Option[Seq[String]])

private[workload] case class SnapshotSpecConfig(
    version: Option[Long], timestamp: Option[String])

private[workload] case class CdfSpecConfig(
    name: String, startVersion: Option[Long], endVersion: Option[Long],
    startTimestamp: Option[String], endTimestamp: Option[String],
    predicate: Option[String], columns: Option[Seq[String]])

private[workload] case class DomainMetadataSpecConfig(
    name: String, domain: String, configuration: String,
    removed: Boolean, version: Option[Long])

private[workload] case class TxnSpecConfig(
    name: String, appId: String, txnVersion: Long, version: Option[Long])
