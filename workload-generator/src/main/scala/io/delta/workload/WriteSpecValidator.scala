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

import scala.collection.mutable
import scala.jdk.CollectionConverters._

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.types.{DataType, StructField, StructType}

/**
 * Validates a write spec by:
 *   1. Replaying the operations in write_spec.json against a fresh table,
 *      using the actual data files in `data/` for insert/create_table ops.
 *   2. Running every read/CDF spec in the `specs/` directory against the
 *      replayed table, comparing row data against `expected/`.
 */
object WriteSpecValidator {

  /**
   * Validate a write spec by replaying it, then running read/CDF specs against
   * the replayed table.
   *
   * @return Seq of warning/error messages (empty = all passed)
   */
  def validate(spark: SparkSession, outputDir: Path, specName: String): Seq[String] = {
    val writeSpecFile = outputDir.resolve("write_spec.json")
    if (!Files.exists(writeSpecFile)) return Seq.empty

    val warnings = mutable.ArrayBuffer[String]()
    val writeSpec = JsonUtil.mapper.readValue(
      Files.readAllBytes(writeSpecFile), classOf[WriteSpec])

    if (writeSpec.commits.isEmpty) return Seq.empty

    val tempDir = Files.createTempDirectory(s"write_replay_$specName")
    val replayTablePath = tempDir.resolve("replay_table")

    try {
      val replayRef = s"delta.`${replayTablePath.toAbsolutePath}`"
      val replayable = replayWriteSpec(spark, writeSpec, replayRef,
        replayTablePath.toAbsolutePath.toString, outputDir, warnings)

      if (!replayable) {
        println(s"  Write spec validation SKIPPED for $specName " +
          "(insert without data files — not fully replayable)")
        return Seq.empty
      }
      if (warnings.nonEmpty) return warnings.toSeq

      // Validate read/CDF specs against the replayed table
      val specsDir = outputDir.resolve("specs")
      val expectedBaseDir = outputDir.resolve("expected")
      if (!Files.isDirectory(specsDir)) return warnings.toSeq

      DeltaLog.clearCache()

      val specFiles = Files.list(specsDir).iterator().asScala
        .filter(_.toString.endsWith(".json"))
        .toSeq.sorted

      for (specFile <- specFiles) {
        val specJson = JsonUtil.mapper.readTree(Files.readAllBytes(specFile))
        val specType = if (specJson.has("type")) specJson.get("type").asText() else ""
        val specFileName = specFile.getFileName.toString.stripSuffix(".json")

        specType match {
          case "read" =>
            val expectedDir = expectedBaseDir.resolve(specFileName)
            // Recompute scanned AddFile JSON from the replayed table so metadata
            // validation compares against itself (replayed files differ from original).
            val spec = JsonUtil.readReadSpec(specFile)
            DeltaLog.clearCache()
            val dl = DeltaLog.forTable(spark, replayTablePath.toString)
            val snap = JsonUtil.resolveSnapshot(dl, spec.version, spec.timestamp)
            val replayAddFiles = snap.allFiles.collect().map(_.json).sorted.toSeq
            ReadCapture.validateFromSpec(spark, replayTablePath, expectedDir,
              specFile, originalAddFilesJson = replayAddFiles)

          case "cdf" =>
            val expectedDir = expectedBaseDir.resolve(specFileName)
            CdfCapture.validateFromSpec(spark, replayTablePath, expectedDir, specFile)

          case "checkpoint" =>
            val expectedDir = expectedBaseDir.resolve(specFileName)
            validateCheckpointStructurally(spark, replayTablePath, expectedDir,
              specFile, specFileName, warnings)

          case "snapshot" =>
            validateSnapshotStructurally(spark, replayTablePath, specFile, specFileName,
              warnings)

          case "domain_metadata" =>
            DomainMetadataCapture.validateFromSpec(spark, replayTablePath, specFile)

          case "appTxn" =>
            AppTxnCapture.validateFromSpec(spark, replayTablePath, specFile)

          case "crc" =>
            validateCrcStructurally(spark, replayTablePath, specFile, specFileName, warnings)

          case "write" => ()

          case _ => ()
        }
      }

      if (warnings.isEmpty) {
        val validatedTypes = Set("read", "cdf", "checkpoint")
        println(s"  Write spec validation PASSED for $specName " +
          s"(${specFiles.count(f => {
            val t = JsonUtil.mapper.readTree(Files.readAllBytes(f))
            val st = if (t.has("type")) t.get("type").asText() else ""
            validatedTypes.contains(st)
          })} specs verified against replayed table)")
      }
    } finally {
      try {
        org.apache.commons.io.FileUtils.deleteDirectory(tempDir.toFile)
      } catch { case _: Exception => }
    }

    warnings.toSeq
  }

  private def replayWriteSpec(
      spark: SparkSession,
      writeSpec: WriteSpec,
      tableRef: String,
      tablePath: String,
      outputDir: Path,
      warnings: mutable.ArrayBuffer[String]): Boolean = {

    for (commit <- writeSpec.commits) {
      val replayable = replayCommit(spark, commit, tableRef, tablePath, outputDir)
      if (!replayable) return false
    }
    true
  }

  /**
   * Replay a single commit.
   * @return true if replayable, false if data files are missing for an insert
   */
  private def replayCommit(
      spark: SparkSession,
      commit: WriteCommit,
      tableRef: String,
      tablePath: String,
      outputDir: Path): Boolean = {

    commit.operation match {
      case "create_table" =>
        commit.schema match {
          case Some(schema) =>
            val colDefs = schemaToColDefs(schema)
            var sql = s"CREATE TABLE $tableRef ($colDefs) USING delta"
            commit.partitionColumns.foreach { parts =>
              if (parts.nonEmpty) sql += s" PARTITIONED BY (${parts.mkString(", ")})"
            }
            commit.properties.foreach { props =>
              if (props.nonEmpty) {
                val propsList = props.map { case (k, v) => s"'$k' = '$v'" }
                sql += s" TBLPROPERTIES (${propsList.mkString(", ")})"
              }
            }
            spark.sql(sql)
            commit.dataFiles.foreach(loadDataFiles(spark, _, tablePath, outputDir))

          case None => return false
        }

      case "replace_table" =>
        commit.schema match {
          case Some(schema) =>
            val colDefs = schemaToColDefs(schema)
            var sql = s"CREATE OR REPLACE TABLE $tableRef ($colDefs) USING delta"
            commit.partitionColumns.foreach { parts =>
              if (parts.nonEmpty) sql += s" PARTITIONED BY (${parts.mkString(", ")})"
            }
            commit.properties.foreach { props =>
              if (props.nonEmpty) {
                val propsList = props.map { case (k, v) => s"'$k' = '$v'" }
                sql += s" TBLPROPERTIES (${propsList.mkString(", ")})"
              }
            }
            spark.sql(sql)
            commit.dataFiles.foreach(loadDataFiles(spark, _, tablePath, outputDir))

          case None => return false
        }

      case "insert" =>
        commit.dataFiles match {
          case Some(files) if files.nonEmpty =>
            loadDataFiles(spark, files, tablePath, outputDir)
          case _ =>
            return false
        }

      case "delete" =>
        commit.predicate match {
          case Some(pred) => spark.sql(s"DELETE FROM $tableRef WHERE $pred")
          case None => spark.sql(s"DELETE FROM $tableRef")
        }

      case "update" =>
        commit.set match {
          case Some(setMap) if setMap.nonEmpty =>
            val predicate = commit.predicate.getOrElse("true")
            val setClauses = setMap.map { case (k, v) => s"`$k` = $v" }.mkString(", ")
            spark.sql(s"UPDATE $tableRef SET $setClauses WHERE $predicate")
          case _ => return false
        }

      case "truncate" =>
        spark.sql(s"TRUNCATE TABLE $tableRef")

      case "evolve_schema" =>
        DeltaLog.clearCache()
        val deltaLog = DeltaLog.forTable(spark, tablePath)
        val txn = deltaLog.startTransaction()
        val currentMetadata = txn.metadata

        var fields = currentMetadata.schema.fields.toBuffer

        commit.addColumns.foreach { cols =>
          val colList = cols.asInstanceOf[Seq[Map[String, Any]]]
          for (col <- colList) {
            val name = col("name").toString
            val dtype = typeToSql(col("type"))
            val nullable = col.get("nullable").forall(_.asInstanceOf[Boolean])
            fields += StructField(name, DataType.fromDDL(dtype), nullable)
          }
        }
        commit.renameColumns.foreach { renames =>
          for ((oldName, newName) <- renames) {
            val idx = fields.indexWhere(_.name == oldName)
            if (idx >= 0) fields(idx) = fields(idx).copy(name = newName)
          }
        }
        commit.dropColumns.foreach { drops =>
          fields = fields.filterNot(f => drops.contains(f.name))
        }

        val newMetadata = currentMetadata.copy(schemaString = StructType(fields.toSeq).json)
        txn.updateMetadata(newMetadata)
        txn.commit(Seq.empty, org.apache.spark.sql.delta.DeltaOperations.ManualUpdate)
        DeltaLog.clearCache()

      case "update_properties" =>
        DeltaLog.clearCache()
        val deltaLog = DeltaLog.forTable(spark, tablePath)
        val txn = deltaLog.startTransaction()
        val currentMetadata = txn.metadata

        val setProps = commit.set.getOrElse(Map.empty)
        val unsetProps = commit.remove.getOrElse(Seq.empty)
        val newConfig = (currentMetadata.configuration ++ setProps) -- unsetProps
        val newMetadata = currentMetadata.copy(configuration = newConfig)

        txn.updateMetadata(newMetadata)
        val operation = if (unsetProps.isEmpty && setProps.nonEmpty) {
          org.apache.spark.sql.delta.DeltaOperations.SetTableProperties(setProps)
        } else {
          org.apache.spark.sql.delta.DeltaOperations.ManualUpdate
        }
        txn.commit(Seq.empty, operation)
        DeltaLog.clearCache()

      case "restore" =>
        commit.version.foreach { v =>
          spark.sql(s"RESTORE $tableRef TO VERSION AS OF $v")
        }

      case "commit" =>
        replayLowLevelCommit(spark, commit, tablePath, outputDir)

      case _ => ()
    }

    true
  }

  /**
   * Validate a checkpoint spec structurally against the replayed table.
   * Compares protocol, metadata (schema/partitionColumns/config/format, skipping id/createdTime),
   * txn entries, domainMetadata entries, and verifies a scan succeeds.
   */
  private def validateCheckpointStructurally(
      spark: SparkSession,
      replayTablePath: Path,
      expectedDir: Path,
      specFile: Path,
      specFileName: String,
      warnings: mutable.ArrayBuffer[String]): Unit = {

    val spec = JsonUtil.mapper.readValue(Files.readAllBytes(specFile), classOf[CheckpointSpec])
    DeltaLog.clearCache()
    val deltaLog = DeltaLog.forTable(spark, replayTablePath.toString)
    val snapshot = deltaLog.getSnapshotAt(spec.version)

    // Protocol
    val replayProtocol = JsonUtil.mapper.readValue(
      JsonUtil.mapper.readTree(snapshot.protocol.json).get("protocol").toString, classOf[Any])
    val expectedProtocol = spec.expected.protocol
    if (JsonUtil.mapper.writeValueAsString(replayProtocol) !=
        JsonUtil.mapper.writeValueAsString(expectedProtocol)) {
      warnings += s"Checkpoint '$specFileName': protocol mismatch"
    }

    // Metadata (structural comparison — skip id and createdTime)
    val replayMetaJson = JsonUtil.mapper.readTree(snapshot.metadata.json).get("metaData")
    val expectedMetaJson = JsonUtil.mapper.readTree(
      JsonUtil.mapper.writeValueAsString(spec.expected.metadata))
    val metaFieldsToCompare = Seq("schemaString", "partitionColumns", "configuration", "format")
    for (field <- metaFieldsToCompare) {
      val replayVal = if (replayMetaJson.has(field)) replayMetaJson.get(field).toString else "null"
      val expectedVal = if (expectedMetaJson.has(field)) expectedMetaJson.get(field).toString else "null"
      if (replayVal != expectedVal) {
        warnings += s"Checkpoint '$specFileName': metadata.$field mismatch"
      }
    }

    // Txn entries
    spec.expected.txn.foreach { expectedTxns =>
      val replayTxns = snapshot.transactions.map { case (appId, version) =>
        (appId, version)
      }.toSet
      val expectedSet = expectedTxns.map(t => (t.appId, t.version)).toSet
      if (replayTxns != expectedSet) {
        warnings += s"Checkpoint '$specFileName': txn mismatch " +
          s"(expected=$expectedSet, replay=$replayTxns)"
      }
    }

    // Domain metadata
    spec.expected.domainMetadata.foreach { expectedDMs =>
      val replayDMs = snapshot.domainMetadata.map { dm =>
        (dm.domain, Option(dm.configuration).filter(_.nonEmpty))
      }.toSet
      val expectedSet = expectedDMs.map(dm =>
        (dm.domain, dm.configuration)
      ).toSet
      if (replayDMs != expectedSet) {
        warnings += s"Checkpoint '$specFileName': domainMetadata mismatch"
      }
    }

    // Compare expected data (row-level scan comparison)
    val expectedDataPath = expectedDir.resolve("expected_data")
    if (Files.exists(expectedDataPath)) {
      val expectedDf = spark.read.parquet(expectedDataPath.toString)
      val replayDf = spark.read.format("delta")
        .option("versionAsOf", spec.version)
        .load(replayTablePath.toAbsolutePath.toString)
      val cols = expectedDf.columns.sorted
      val expectedSorted = expectedDf.select(cols.head, cols.tail: _*)
        .sort(cols.head, cols.tail: _*)
      val replaySorted = replayDf.select(cols.head, cols.tail: _*)
        .sort(cols.head, cols.tail: _*)
      if (expectedSorted.except(replaySorted).count() != 0 ||
          replaySorted.except(expectedSorted).count() != 0) {
        warnings += s"Checkpoint '$specFileName': expected_data row mismatch"
      }
    } else {
      spark.read.format("delta").load(replayTablePath.toAbsolutePath.toString).count()
    }
  }

  /**
   * Validates a snapshot spec against the replayed table using structural comparison.
   * Compares protocol exactly and metadata structurally (skipping id and createdTime).
   */
  private def validateSnapshotStructurally(
      spark: SparkSession,
      replayTablePath: Path,
      specFile: Path,
      specFileName: String,
      warnings: mutable.ArrayBuffer[String]): Unit = {

    val spec = JsonUtil.readSnapshotSpec(specFile)

    (spec.expected, spec.expectedError) match {
      case (Some(exp), _) =>
        DeltaLog.clearCache()
        val deltaLog = DeltaLog.forTable(spark, replayTablePath.toString)
        val snapshot = JsonUtil.resolveSnapshot(deltaLog, spec.version, spec.timestamp)

        val expectedProtoJson = JsonUtil.mapper.writeValueAsString(exp.protocol)
        val replayProto = JsonUtil.mapper.treeToValue(
          JsonUtil.mapper.readTree(snapshot.protocol.json).get("protocol"), classOf[Any])
        val replayProtoJson = JsonUtil.mapper.writeValueAsString(replayProto)
        if (expectedProtoJson != replayProtoJson) {
          warnings += s"Snapshot '$specFileName': protocol mismatch"
        }

        val expectedMetaTree = JsonUtil.mapper.readTree(
          JsonUtil.mapper.writeValueAsString(exp.metadata))
        val replayMetaTree = JsonUtil.mapper.readTree(snapshot.metadata.json).get("metaData")
        val metaFieldsToCompare =
          Seq("schemaString", "partitionColumns", "configuration", "format")
        for (field <- metaFieldsToCompare) {
          val expVal =
            if (expectedMetaTree.has(field)) expectedMetaTree.get(field).toString else "null"
          val repVal =
            if (replayMetaTree.has(field)) replayMetaTree.get(field).toString else "null"
          if (expVal != repVal) {
            warnings += s"Snapshot '$specFileName': metadata.$field mismatch"
          }
        }

      case (_, Some(err)) =>
        val actualCode = try {
          DeltaLog.clearCache()
          val deltaLog = DeltaLog.forTable(spark, replayTablePath.toString)
          val snapshot = JsonUtil.resolveSnapshot(deltaLog, spec.version, spec.timestamp)
          if (snapshot.version < 0) Some("DELTA_TABLE_NOT_FOUND") else None
        } catch {
          case e: Exception => Some(JsonUtil.extractErrorCode(e))
        }
        if (actualCode.isEmpty) {
          warnings += s"Snapshot '$specFileName': expected error '${err.errorCode}' but succeeded"
        }

      case _ => ()
    }
  }

  /**
   * Validates a CRC spec against the replayed table structurally.
   * Skips tableSizeBytes (file sizes differ across independent table creation).
   */
  private def validateCrcStructurally(
      spark: SparkSession,
      replayTablePath: Path,
      specFile: Path,
      specFileName: String,
      warnings: mutable.ArrayBuffer[String]): Unit = {

    val spec = JsonUtil.mapper.readValue(Files.readAllBytes(specFile), classOf[CrcSpec])

    DeltaLog.clearCache()
    val deltaLog = DeltaLog.forTable(spark, replayTablePath.toString)
    val snapshot = deltaLog.getSnapshotAt(spec.version)

    // numFiles
    spec.expected.numFiles.foreach { expected =>
      val actual = snapshot.allFiles.count()
      if (expected != actual) {
        warnings += s"CRC '$specFileName': numFiles mismatch (expected=$expected, actual=$actual)"
      }
    }

    // protocol
    spec.expected.protocol.foreach { expectedProtocol =>
      val actualProtocol = ProtocolInfo(
        minReaderVersion = snapshot.protocol.minReaderVersion,
        minWriterVersion = snapshot.protocol.minWriterVersion,
        readerFeatures = snapshot.protocol.readerFeatures.map(_.toSeq.sorted),
        writerFeatures = snapshot.protocol.writerFeatures.map(_.toSeq.sorted)
      )
      if (expectedProtocol != actualProtocol) {
        warnings += s"CRC '$specFileName': protocol mismatch"
      }
    }

    // txn
    spec.expected.txn.foreach { expectedTxns =>
      val replayTxns = snapshot.setTransactions.map { txn =>
        TxnAction(txn.appId, txn.version)
      }.toSet
      val expectedSet = expectedTxns.toSet
      if (replayTxns != expectedSet) {
        warnings += s"CRC '$specFileName': txn mismatch"
      }
    }

    // numDomainMetadata
    spec.expected.numDomainMetadata.foreach { expected =>
      val actual = snapshot.domainMetadata.size.toLong
      if (expected != actual) {
        warnings += s"CRC '$specFileName': numDomainMetadata mismatch " +
          s"(expected=$expected, actual=$actual)"
      }
    }

    // numTransactions
    spec.expected.numTransactions.foreach { expected =>
      val actual = snapshot.setTransactions.size.toLong
      if (expected != actual) {
        warnings += s"CRC '$specFileName': numTransactions mismatch " +
          s"(expected=$expected, actual=$actual)"
      }
    }
  }

  /** Read parquet data files and append to the replay table. */
  private def loadDataFiles(
      spark: SparkSession,
      files: Seq[String],
      tablePath: String,
      outputDir: Path): Unit = {
    val paths = files.map(f => outputDir.resolve(f))
      .filter(Files.exists(_))
      .map(_.toAbsolutePath.toString)
    if (paths.nonEmpty) {
      val df = spark.read.parquet(paths: _*)
      df.write.format("delta").mode(SaveMode.Append).save(tablePath)
    }
  }

  /** Replay a low-level commit using DeltaLog transaction API. */
  private def replayLowLevelCommit(
      spark: SparkSession,
      commit: WriteCommit,
      tablePath: String,
      outputDir: Path): Unit = {
    import org.apache.spark.sql.delta.actions._

    DeltaLog.clearCache()
    val deltaLog = DeltaLog.forTable(spark, tablePath)
    val txn = deltaLog.startTransaction()

    val actions = scala.collection.mutable.ArrayBuffer[Action]()

    // Schema / table properties → Metadata update
    if (commit.schema.isDefined || commit.tableProperties.isDefined) {
      val currentMetadata = txn.metadata
      val newSchemaString = commit.schema.map { s =>
        val json = JsonUtil.mapper.writeValueAsString(s)
        StructType.fromDDL(schemaToColDefs(s)).json
      }.getOrElse(currentMetadata.schemaString)
      val newConfig = commit.tableProperties.map { props =>
        currentMetadata.configuration ++ props
      }.getOrElse(currentMetadata.configuration)
      val newMetadata = currentMetadata.copy(
        schemaString = newSchemaString,
        configuration = newConfig)
      txn.updateMetadata(newMetadata)
    }

    // SetTransaction
    commit.txn.foreach { t =>
      actions += SetTransaction(t.appId, t.version, Some(System.currentTimeMillis()))
    }

    // AddFile actions
    commit.addFiles.foreach { files =>
      for (file <- files) {
        val srcPath = outputDir.resolve(file.dataFile)
        if (Files.exists(srcPath)) {
          val destRelative = java.nio.file.Paths.get(file.dataFile).getFileName.toString
          val dest = java.nio.file.Paths.get(tablePath).resolve(destRelative)
          java.nio.file.Files.createDirectories(dest.getParent)
          java.nio.file.Files.copy(srcPath, dest,
            java.nio.file.StandardCopyOption.REPLACE_EXISTING)
          actions += AddFile(
            path = destRelative,
            partitionValues = file.partitionValues.getOrElse(Map.empty),
            size = Files.size(dest),
            modificationTime = System.currentTimeMillis(),
            dataChange = file.dataChange.getOrElse(true))
        }
      }
    }

    // RemoveFile actions
    commit.removeFiles.foreach { files =>
      for (file <- files) {
        actions += RemoveFile(
          path = file.path,
          deletionTimestamp = Some(System.currentTimeMillis()),
          dataChange = file.dataChange.getOrElse(true))
      }
    }

    // DomainMetadata actions
    commit.addDomainMetadata.foreach { dms =>
      for (dm <- dms) {
        actions += DomainMetadata(dm.domain, dm.configuration, removed = false)
      }
    }
    commit.removeDomainMetadata.foreach { domains =>
      for (domain <- domains) {
        actions += DomainMetadata(domain, "", removed = true)
      }
    }

    txn.commit(actions.toSeq, org.apache.spark.sql.delta.DeltaOperations.ManualUpdate)
    DeltaLog.clearCache()
  }

  private def schemaToColDefs(schema: Any): String = {
    val schemaMap = schema.asInstanceOf[Map[String, Any]]
    val fields = schemaMap("fields").asInstanceOf[Seq[Map[String, Any]]]
    fields.map { f =>
      val name = f("name").toString
      val dtype = typeToSql(f("type"))
      val nullable = f.get("nullable").forall(_.asInstanceOf[Boolean])
      s"`$name` $dtype${if (!nullable) " NOT NULL" else ""}"
    }.mkString(", ")
  }

  private def typeToSql(typeValue: Any): String = typeValue match {
    case s: String => s match {
      case "integer" => "INT"
      case "long" => "BIGINT"
      case "short" => "SMALLINT"
      case "byte" => "TINYINT"
      case "float" => "FLOAT"
      case "double" => "DOUBLE"
      case "boolean" => "BOOLEAN"
      case "string" => "STRING"
      case "binary" => "BINARY"
      case "date" => "DATE"
      case "timestamp" => "TIMESTAMP"
      case "timestamp_ntz" => "TIMESTAMP_NTZ"
      case other => other.toUpperCase
    }
    case m: Map[_, _] =>
      val typeMap = m.asInstanceOf[Map[String, Any]]
      typeMap("type") match {
        case "struct" =>
          val fields = typeMap("fields").asInstanceOf[Seq[Map[String, Any]]]
          val fieldDefs = fields.map { f =>
            s"${f("name")}: ${typeToSql(f("type"))}"
          }
          s"STRUCT<${fieldDefs.mkString(", ")}>"
        case "array" =>
          s"ARRAY<${typeToSql(typeMap("elementType"))}>"
        case "map" =>
          s"MAP<${typeToSql(typeMap("keyType"))}, ${typeToSql(typeMap("valueType"))}>"
        case other => other.toString.toUpperCase
      }
    case _ => typeValue.toString.toUpperCase
  }
}
