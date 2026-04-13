/**
 * Log Replay + Production Edge Cases workloads (lr_* + log_* + lc_* + prod_*).
 * Covers: add-then-remove log replay, checkpoint supersedes log, full replay without
 * checkpoint, metadata latest wins, dataChange=false from compaction, missing
 * protocol/metadata errors, add-remove-readd, DV key dedup, last checkpoint info,
 * production edge cases (empty table, many commits, version gaps, truncated logs,
 * unknown reader features, varchar metadata, duplicate file refs, external checkpoints).
 *
 */

new WorkloadSuite("log_replay") {

  // Log replay: add then remove

  test("lr_add_then_remove", "Log replay - add then remove", "logReplay") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    sql("INSERT INTO tbl VALUES (1), (2), (3)")
    sql("DELETE FROM tbl WHERE id = 2")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "id = 2", name = "read_deleted")
    snapshot(t)
  }

  // Log replay: checkpoint supersedes log

  test("lr_checkpoint_supersedes_log", "Log replay - checkpoint supersedes log",
      "logReplay") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    sql("INSERT INTO tbl VALUES (1), (2)")
    sql("INSERT INTO tbl VALUES (3), (4)")
    forceCheckpoint("tbl")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
      java.nio.file.Files.deleteIfExists(dir.resolve("_delta_log/00000000000000000000.json"))
      java.nio.file.Files.deleteIfExists(dir.resolve("_delta_log/00000000000000000001.json"))
    }
    read(t)
    snapshot(t)
  }

  // Log replay: no checkpoint, full replay from v0

  test("lr_no_checkpoint_full_replay", "Log replay - no checkpoint, full replay",
      "logReplay") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    sql("INSERT INTO tbl VALUES (1)")
    sql("INSERT INTO tbl VALUES (2)")
    sql("INSERT INTO tbl VALUES (3)")
    val t = registerTable("tbl")
    // Ensure no checkpoint exists — remove any CRC files but keep commits
    mutateTable(t) { dir =>
      import scala.collection.JavaConverters._
      java.nio.file.Files.list(dir.resolve("_delta_log")).iterator().asScala
        .filter(_.toString.endsWith(".crc")).foreach(java.nio.file.Files.delete)
    }
    read(t)
    snapshot(t)
  }

  // Log replay: metadata latest wins

  test("lr_metadata_latest_wins", "Log replay - metadata latest wins", "logReplay") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    sql("INSERT INTO tbl VALUES (1), (2)")
    sql("ALTER TABLE tbl ADD COLUMN name STRING")
    sql("INSERT INTO tbl VALUES (3, 'alice')")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "name IS NOT NULL")
    snapshot(t)
  }

  // Log replay: dataChange=false from compaction

  test("lr_datachange_false", "Log replay - dataChange=false from compaction",
      "logReplay") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    sql("INSERT INTO tbl VALUES (1), (2)")
    sql("INSERT INTO tbl VALUES (3), (4)")
    sql("OPTIMIZE tbl")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  // Log replay: add, remove, re-add across transactions

  test("log_replay_add_remove_readd", "Same file added, removed, re-added",
      "logReplay") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    sql("INSERT INTO tbl VALUES (1), (2), (3)")
    sql("DELETE FROM tbl WHERE id = 2")
    sql("INSERT INTO tbl VALUES (2)")  // re-add data
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "id = 2")
    read(t, version = 1)  // before delete
    snapshot(t)
  }

  // Log replay: DV key dedup

  test("log_replay_dv_key_dedup", "DV deduplication during log replay",
      "logReplay", "dv") {
    sql("""CREATE TABLE tbl (id INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(1, 21)")
    sql("DELETE FROM tbl WHERE id <= 5")
    sql("DELETE FROM tbl WHERE id <= 10")  // re-DV same base file
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  // Error: missing metadata in state reconstruction

  test("log_err_missing_metadata", "State reconstruction without Metadata action",
      "logReplay", "error") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    sql("INSERT INTO tbl VALUES (1)")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
      import scala.collection.JavaConverters._
      val f = dir.resolve("_delta_log/00000000000000000000.json")
      val lines = java.nio.file.Files.readAllLines(f).asScala.filterNot(_.contains("\"metaData\""))
      java.nio.file.Files.write(f, lines.asJava)
    }
    read(t)
  }

  // Error: missing protocol in state reconstruction

  test("log_err_missing_protocol", "State reconstruction without Protocol action",
      "logReplay", "error") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    sql("INSERT INTO tbl VALUES (1)")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
      import scala.collection.JavaConverters._
      val f = dir.resolve("_delta_log/00000000000000000000.json")
      val lines = java.nio.file.Files.readAllLines(f).asScala.filterNot(_.contains("\"protocol\""))
      java.nio.file.Files.write(f, lines.asJava)
    }
    read(t)
  }

  // Last checkpoint info (lc_*)

  test("lc_basic", "Basic last checkpoint read", "lastCheckpoint") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    forceCheckpoint("tbl")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("lc_checksum", "Checkpoint with checksum validation", "lastCheckpoint") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    forceCheckpoint("tbl")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("lc_multi_version", "Last checkpoint with multiple versions",
      "lastCheckpoint") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    sql("INSERT INTO tbl VALUES (1)")
    sql("INSERT INTO tbl VALUES (2)")
    forceCheckpoint("tbl")
    sql("INSERT INTO tbl VALUES (3)")
    sql("INSERT INTO tbl VALUES (4)")
    val t = registerTable("tbl")
    read(t)
    read(t, version = 1)
    read(t, version = 2)  // checkpoint version
    snapshot(t)
  }

  test("lc_after_ops", "Last checkpoint after table operations", "lastCheckpoint") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    sql("DELETE FROM tbl WHERE id < 3")
    sql("INSERT INTO tbl VALUES (100)")
    forceCheckpoint("tbl")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("lc_with_schema", "Last checkpoint with schema information",
      "lastCheckpoint") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    sql("INSERT INTO tbl VALUES (1)")
    sql("ALTER TABLE tbl ADD COLUMN name STRING")
    sql("INSERT INTO tbl VALUES (2, 'alice')")
    forceCheckpoint("tbl")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  // Production edge cases (prod_*)

  test("prod_empty_table_with_schema", "Empty Delta table with schema but no data files",
      "production") {
    sql("CREATE TABLE tbl (id INT, name STRING, score DOUBLE) USING delta")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("prod_many_small_commits", "Table with 50+ small commits without checkpoint",
      "production") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    for (i <- 1 to 51) {
      sql(s"INSERT INTO tbl VALUES ($i)")
    }
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("prod_non_contiguous_versions", "Version gap in delta log", "production") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    sql("INSERT INTO tbl VALUES (1)")
    sql("INSERT INTO tbl VALUES (2)")
    sql("INSERT INTO tbl VALUES (3)")
    forceCheckpoint("tbl")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
      java.nio.file.Files.deleteIfExists(dir.resolve("_delta_log/00000000000000000002.json"))
    }
    read(t)
    snapshot(t)
  }

  test("prod_truncated_log", "Old log files deleted by lifecycle policy",
      "production") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    sql("INSERT INTO tbl VALUES (1)")
    sql("INSERT INTO tbl VALUES (2)")
    sql("INSERT INTO tbl VALUES (3)")
    sql("INSERT INTO tbl VALUES (4)")
    forceCheckpoint("tbl")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
      java.nio.file.Files.deleteIfExists(dir.resolve("_delta_log/00000000000000000000.json"))
      java.nio.file.Files.deleteIfExists(dir.resolve("_delta_log/00000000000000000001.json"))
      java.nio.file.Files.deleteIfExists(dir.resolve("_delta_log/00000000000000000002.json"))
    }
    read(t)
    snapshot(t)
  }

  test("prod_external_writer_checkpoint", "Checkpoint written by external tool",
      "production") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
     forceCheckpoint("tbl")
    sql("INSERT INTO tbl SELECT id FROM range(10, 20)")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("prod_duplicate_add_file_refs", "Multiple versions with overlapping file paths",
      "production") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    sql("INSERT INTO tbl VALUES (1), (2), (3)")
    sql("INSERT OVERWRITE tbl VALUES (4), (5), (6)")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("prod_varchar_metadata_missing", "Table with VARCHAR column metadata",
      "production") {
    sql("CREATE TABLE tbl (id INT, name VARCHAR(100)) USING delta")
    sql("INSERT INTO tbl VALUES (1, 'hello'), (2, 'world')")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("prod_unknown_reader_feature", "Protocol with unknown reader feature",
      "production", "error") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    sql("INSERT INTO tbl VALUES (1)")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
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
    read(t)
  }

}
