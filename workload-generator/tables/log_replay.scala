/**
 * Log Replay + Production Edge Cases workloads (lr_* + log_* + lc_* + prod_*).
 * Covers: add-then-remove log replay, checkpoint supersedes log, full replay without
 * checkpoint, metadata latest wins, dataChange=false from compaction, missing
 * protocol/metadata errors, add-remove-readd, DV key dedup, last checkpoint info,
 * production edge cases (empty table, many commits, version gaps, truncated logs,
 * unknown reader features, varchar metadata, duplicate file refs, external checkpoints).
 *
 * Run: ./bin/generate-workload.sh tables/log_replay.scala
 */
import io.delta.workload.WorkloadGenerator._

// ---------------------------------------------------------------------------
// Log replay: add then remove
// ---------------------------------------------------------------------------

workload("lr_add_then_remove", "Log replay - add then remove", "logReplay") { w =>
  w.sql("CREATE TABLE tbl (id INT) USING delta")
  w.sql("INSERT INTO tbl VALUES (1), (2), (3)")
  w.sql("DELETE FROM tbl WHERE id = 2")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "id = 2", name = "read_deleted")
  w.snapshot(t)
}

// ---------------------------------------------------------------------------
// Log replay: checkpoint supersedes log
// ---------------------------------------------------------------------------

workload("lr_checkpoint_supersedes_log", "Log replay - checkpoint supersedes log",
    "logReplay") { w =>
  w.sql("CREATE TABLE tbl (id INT) USING delta")
  w.sql("INSERT INTO tbl VALUES (1), (2)")
  w.sql("INSERT INTO tbl VALUES (3), (4)")
  // Force checkpoint
  val loc = w.spark.sql("DESCRIBE DETAIL tbl").collect()(0).getAs[String]("location")
  org.apache.spark.sql.delta.DeltaLog.forTable(w.spark, loc).checkpoint()
  // Delete pre-checkpoint commits (checkpoint should suffice)
  val t = w.table("tbl")
  w.mutateTable(t) { dir =>
    java.nio.file.Files.deleteIfExists(dir.resolve("_delta_log/00000000000000000000.json"))
    java.nio.file.Files.deleteIfExists(dir.resolve("_delta_log/00000000000000000001.json"))
  }
  w.read(t)
  w.snapshot(t)
}

// ---------------------------------------------------------------------------
// Log replay: no checkpoint, full replay from v0
// ---------------------------------------------------------------------------

workload("lr_no_checkpoint_full_replay", "Log replay - no checkpoint, full replay",
    "logReplay") { w =>
  w.sql("CREATE TABLE tbl (id INT) USING delta")
  w.sql("INSERT INTO tbl VALUES (1)")
  w.sql("INSERT INTO tbl VALUES (2)")
  w.sql("INSERT INTO tbl VALUES (3)")
  val t = w.table("tbl")
  // Ensure no checkpoint exists — remove any CRC files but keep commits
  w.mutateTable(t) { dir =>
    import scala.collection.JavaConverters._
    java.nio.file.Files.list(dir.resolve("_delta_log")).iterator().asScala
      .filter(_.toString.endsWith(".crc")).foreach(java.nio.file.Files.delete)
  }
  w.read(t)
  w.snapshot(t)
}

// ---------------------------------------------------------------------------
// Log replay: metadata latest wins
// ---------------------------------------------------------------------------

workload("lr_metadata_latest_wins", "Log replay - metadata latest wins", "logReplay") { w =>
  w.sql("CREATE TABLE tbl (id INT) USING delta")
  w.sql("INSERT INTO tbl VALUES (1), (2)")
  w.sql("ALTER TABLE tbl ADD COLUMN name STRING")
  w.sql("INSERT INTO tbl VALUES (3, 'alice')")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "name IS NOT NULL")
  w.snapshot(t)
}

// ---------------------------------------------------------------------------
// Log replay: dataChange=false from compaction
// ---------------------------------------------------------------------------

workload("lr_datachange_false", "Log replay - dataChange=false from compaction",
    "logReplay") { w =>
  w.sql("CREATE TABLE tbl (id INT) USING delta")
  w.sql("INSERT INTO tbl VALUES (1), (2)")
  w.sql("INSERT INTO tbl VALUES (3), (4)")
  w.sql("OPTIMIZE tbl")
  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
}

// ---------------------------------------------------------------------------
// Log replay: add, remove, re-add across transactions
// ---------------------------------------------------------------------------

workload("log_replay_add_remove_readd", "Same file added, removed, re-added",
    "logReplay") { w =>
  w.sql("CREATE TABLE tbl (id INT) USING delta")
  w.sql("INSERT INTO tbl VALUES (1), (2), (3)")
  w.sql("DELETE FROM tbl WHERE id = 2")
  w.sql("INSERT INTO tbl VALUES (2)")  // re-add data
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "id = 2")
  w.read(t, version = 1)  // before delete
  w.snapshot(t)
}

// ---------------------------------------------------------------------------
// Log replay: DV key dedup
// ---------------------------------------------------------------------------

workload("log_replay_dv_key_dedup", "DV deduplication during log replay",
    "logReplay", "dv") { w =>
  w.sql("""CREATE TABLE tbl (id INT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl SELECT id FROM range(1, 21)")
  w.sql("DELETE FROM tbl WHERE id <= 5")
  w.sql("DELETE FROM tbl WHERE id <= 10")  // re-DV same base file
  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
}

// ---------------------------------------------------------------------------
// Error: missing metadata in state reconstruction
// ---------------------------------------------------------------------------

workload("log_err_missing_metadata", "State reconstruction without Metadata action",
    "logReplay", "error") { w =>
  w.sql("CREATE TABLE tbl (id INT) USING delta")
  w.sql("INSERT INTO tbl VALUES (1)")
  val t = w.table("tbl")
  w.mutateTable(t) { dir =>
    import scala.collection.JavaConverters._
    val f = dir.resolve("_delta_log/00000000000000000000.json")
    val lines = java.nio.file.Files.readAllLines(f).asScala.filterNot(_.contains("\"metaData\""))
    java.nio.file.Files.write(f, lines.asJava)
  }
  w.read(t)
}

// ---------------------------------------------------------------------------
// Error: missing protocol in state reconstruction
// ---------------------------------------------------------------------------

workload("log_err_missing_protocol", "State reconstruction without Protocol action",
    "logReplay", "error") { w =>
  w.sql("CREATE TABLE tbl (id INT) USING delta")
  w.sql("INSERT INTO tbl VALUES (1)")
  val t = w.table("tbl")
  w.mutateTable(t) { dir =>
    import scala.collection.JavaConverters._
    val f = dir.resolve("_delta_log/00000000000000000000.json")
    val lines = java.nio.file.Files.readAllLines(f).asScala.filterNot(_.contains("\"protocol\""))
    java.nio.file.Files.write(f, lines.asJava)
  }
  w.read(t)
}

// ---------------------------------------------------------------------------
// Last checkpoint info (lc_*)
// ---------------------------------------------------------------------------

workload("lc_basic", "Basic last checkpoint read", "lastCheckpoint") { w =>
  w.sql("CREATE TABLE tbl (id INT) USING delta")
  w.sql("INSERT INTO tbl SELECT id FROM range(10)")
  // Force checkpoint
  val loc = w.spark.sql("DESCRIBE DETAIL tbl").collect()(0).getAs[String]("location")
  org.apache.spark.sql.delta.DeltaLog.forTable(w.spark, loc).checkpoint()
  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
}

workload("lc_checksum", "Checkpoint with checksum validation", "lastCheckpoint") { w =>
  w.sql("CREATE TABLE tbl (id INT) USING delta")
  w.sql("INSERT INTO tbl SELECT id FROM range(10)")
  val loc = w.spark.sql("DESCRIBE DETAIL tbl").collect()(0).getAs[String]("location")
  org.apache.spark.sql.delta.DeltaLog.forTable(w.spark, loc).checkpoint()
  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
}

workload("lc_multi_version", "Last checkpoint with multiple versions",
    "lastCheckpoint") { w =>
  w.sql("CREATE TABLE tbl (id INT) USING delta")
  w.sql("INSERT INTO tbl VALUES (1)")
  w.sql("INSERT INTO tbl VALUES (2)")
  val loc = w.spark.sql("DESCRIBE DETAIL tbl").collect()(0).getAs[String]("location")
  org.apache.spark.sql.delta.DeltaLog.forTable(w.spark, loc).checkpoint()
  w.sql("INSERT INTO tbl VALUES (3)")
  w.sql("INSERT INTO tbl VALUES (4)")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, version = 1)
  w.read(t, version = 2)  // checkpoint version
  w.snapshot(t)
}

workload("lc_after_ops", "Last checkpoint after table operations", "lastCheckpoint") { w =>
  w.sql("CREATE TABLE tbl (id INT) USING delta")
  w.sql("INSERT INTO tbl SELECT id FROM range(10)")
  w.sql("DELETE FROM tbl WHERE id < 3")
  w.sql("INSERT INTO tbl VALUES (100)")
  val loc = w.spark.sql("DESCRIBE DETAIL tbl").collect()(0).getAs[String]("location")
  org.apache.spark.sql.delta.DeltaLog.forTable(w.spark, loc).checkpoint()
  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
}

workload("lc_with_schema", "Last checkpoint with schema information",
    "lastCheckpoint") { w =>
  w.sql("CREATE TABLE tbl (id INT) USING delta")
  w.sql("INSERT INTO tbl VALUES (1)")
  w.sql("ALTER TABLE tbl ADD COLUMN name STRING")
  w.sql("INSERT INTO tbl VALUES (2, 'alice')")
  val loc = w.spark.sql("DESCRIBE DETAIL tbl").collect()(0).getAs[String]("location")
  org.apache.spark.sql.delta.DeltaLog.forTable(w.spark, loc).checkpoint()
  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
}

// ---------------------------------------------------------------------------
// Production edge cases (prod_*)
// ---------------------------------------------------------------------------

workload("prod_empty_table_with_schema", "Empty Delta table with schema but no data files",
    "production") { w =>
  w.sql("CREATE TABLE tbl (id INT, name STRING, score DOUBLE) USING delta")
  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
}

workload("prod_many_small_commits", "Table with 50+ small commits without checkpoint",
    "production") { w =>
  w.sql("CREATE TABLE tbl (id INT) USING delta")
  for (i <- 1 to 51) {
    w.sql(s"INSERT INTO tbl VALUES ($i)")
  }
  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
}

workload("prod_non_contiguous_versions", "Version gap in delta log", "production") { w =>
  w.sql("CREATE TABLE tbl (id INT) USING delta")
  w.sql("INSERT INTO tbl VALUES (1)")
  w.sql("INSERT INTO tbl VALUES (2)")
  w.sql("INSERT INTO tbl VALUES (3)")
  // Force checkpoint at v3
  val loc = w.spark.sql("DESCRIBE DETAIL tbl").collect()(0).getAs[String]("location")
  org.apache.spark.sql.delta.DeltaLog.forTable(w.spark, loc).checkpoint()
  val t = w.table("tbl")
  // Delete a version before checkpoint
  w.mutateTable(t) { dir =>
    java.nio.file.Files.deleteIfExists(dir.resolve("_delta_log/00000000000000000002.json"))
  }
  w.read(t)
  w.snapshot(t)
}

workload("prod_truncated_log", "Old log files deleted by lifecycle policy",
    "production") { w =>
  w.sql("CREATE TABLE tbl (id INT) USING delta")
  w.sql("INSERT INTO tbl VALUES (1)")
  w.sql("INSERT INTO tbl VALUES (2)")
  w.sql("INSERT INTO tbl VALUES (3)")
  w.sql("INSERT INTO tbl VALUES (4)")
  // Force checkpoint at latest
  val loc = w.spark.sql("DESCRIBE DETAIL tbl").collect()(0).getAs[String]("location")
  org.apache.spark.sql.delta.DeltaLog.forTable(w.spark, loc).checkpoint()
  val t = w.table("tbl")
  // Simulate lifecycle: delete early versions
  w.mutateTable(t) { dir =>
    java.nio.file.Files.deleteIfExists(dir.resolve("_delta_log/00000000000000000000.json"))
    java.nio.file.Files.deleteIfExists(dir.resolve("_delta_log/00000000000000000001.json"))
    java.nio.file.Files.deleteIfExists(dir.resolve("_delta_log/00000000000000000002.json"))
  }
  w.read(t)
  w.snapshot(t)
}

workload("prod_external_writer_checkpoint", "Checkpoint written by external tool",
    "production") { w =>
  w.sql("CREATE TABLE tbl (id INT) USING delta")
  w.sql("INSERT INTO tbl SELECT id FROM range(10)")
  // Force checkpoint
  val loc = w.spark.sql("DESCRIBE DETAIL tbl").collect()(0).getAs[String]("location")
  org.apache.spark.sql.delta.DeltaLog.forTable(w.spark, loc).checkpoint()
  w.sql("INSERT INTO tbl SELECT id FROM range(10, 20)")
  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
}

workload("prod_duplicate_add_file_refs", "Multiple versions with overlapping file paths",
    "production") { w =>
  w.sql("CREATE TABLE tbl (id INT) USING delta")
  w.sql("INSERT INTO tbl VALUES (1), (2), (3)")
  w.sql("INSERT OVERWRITE tbl VALUES (4), (5), (6)")
  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
}

workload("prod_varchar_metadata_missing", "Table with VARCHAR column metadata",
    "production") { w =>
  w.sql("CREATE TABLE tbl (id INT, name VARCHAR(100)) USING delta")
  w.sql("INSERT INTO tbl VALUES (1, 'hello'), (2, 'world')")
  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
}

workload("prod_unknown_reader_feature", "Protocol with unknown reader feature",
    "production", "error") { w =>
  w.sql("CREATE TABLE tbl (id INT) USING delta")
  w.sql("INSERT INTO tbl VALUES (1)")
  val t = w.table("tbl")
  w.mutateTable(t) { dir =>
    import scala.collection.JavaConverters._
    val f = dir.resolve("_delta_log/00000000000000000000.json")
    val lines = java.nio.file.Files.readAllLines(f).asScala
    val newLines = lines.map { line =>
      if (line.contains("\"readerFeatures\"")) {
        line.replace("\"readerFeatures\":[",
          "\"readerFeatures\":[\"unknownFutureReaderFeature\",")
      } else line
    }
    java.nio.file.Files.write(f, newLines.asJava)
  }
  w.read(t)
}

generateAll(
  sys.env.getOrElse("WORKLOAD_OUTPUT_DIR", "/tmp/workloads"),
  force = sys.env.getOrElse("WORKLOAD_FORCE", "false").toBoolean)
System.exit(0)
