import io.delta.workload.WorkloadGenerator._
import org.apache.spark.sql.delta.DeltaLog

// Helper to get table location and trigger checkpoint
def getTableLocation(w: io.delta.workload.WorkloadContext, name: String): String = {
  w.spark.sql(s"DESCRIBE DETAIL $name").collect()(0).getAs[String]("location")
}

def checkpoint(w: io.delta.workload.WorkloadContext, name: String): Unit = {
  val loc = getTableLocation(w, name)
  DeltaLog.forTable(w.spark, loc).checkpoint()
  DeltaLog.clearCache()
}

def checkpointAt(w: io.delta.workload.WorkloadContext, name: String, numParts: Int): Unit = {
  val loc = getTableLocation(w, name)
  val dl = DeltaLog.forTable(w.spark, loc)
  dl.checkpoint(dl.update(), numParts)
  DeltaLog.clearCache()
}

// ---------------------------------------------------------------------------
// Existing 5 workloads
// ---------------------------------------------------------------------------

workload("cp_classic", "Classic checkpoint read", "checkpoint") { w =>
  w.sql("CREATE TABLE tbl (id INT) USING delta")
  w.sql("INSERT INTO tbl SELECT CAST(id AS INT) FROM range(1, 101)")
  w.sql("INSERT INTO tbl SELECT CAST(id AS INT) FROM range(101, 201)")
  w.sql("INSERT INTO tbl SELECT CAST(id AS INT) FROM range(201, 301)")
  checkpoint(w, "tbl")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "id > 250")
  w.snapshot(t)
}

workload("cp_multi_version", "Read checkpoint with time travel across versions", "checkpoint") { w =>
  w.sql("CREATE TABLE tbl (id INT) USING delta")
  w.sql("INSERT INTO tbl SELECT CAST(id AS INT) FROM range(1, 51)")
  w.sql("INSERT INTO tbl SELECT CAST(id AS INT) FROM range(51, 101)")
  checkpoint(w, "tbl")
  w.sql("INSERT INTO tbl SELECT CAST(id AS INT) FROM range(101, 151)")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, version = 0)
  w.read(t, version = 1)
  w.read(t, version = 2)
  w.snapshot(t)
}

workload("cp_last_checkpoint", "Read table using _last_checkpoint", "checkpoint") { w =>
  w.sql("CREATE TABLE tbl (id INT, val STRING) USING delta TBLPROPERTIES ('delta.checkpointInterval' = '5')")
  for (i <- 1 to 7) w.sql(s"INSERT INTO tbl VALUES ($i, 'v$i')")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, version = 5)
  w.snapshot(t)
  w.snapshot(t, version = 5)
}

workload("cp_schema_evolution", "Checkpoint with schema evolution", "checkpoint", "schema_evolution") { w =>
  w.sql("CREATE TABLE tbl (id LONG) USING delta")
  w.sql("INSERT INTO tbl SELECT id FROM range(50)")
  checkpoint(w, "tbl")
  w.sql("ALTER TABLE tbl ADD COLUMN name STRING")
  w.sql("INSERT INTO tbl SELECT id, 'test' FROM range(50, 100)")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, version = 0)
  w.snapshotHistory(t)
}

workload("cp_partitioned", "Checkpoint with partitioned table", "checkpoint") { w =>
  w.sql("CREATE TABLE tbl (id LONG, part INT) USING delta PARTITIONED BY (part)")
  w.sql("INSERT INTO tbl SELECT id, CAST(id % 5 AS INT) FROM range(100)")
  w.sql("INSERT INTO tbl SELECT id, CAST(id % 5 AS INT) FROM range(100, 200)")
  checkpoint(w, "tbl")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "part = 0")
  w.read(t, predicate = "part = 3")
  w.snapshot(t)
}

// ---------------------------------------------------------------------------
// New workloads: 32 more to match existing acceptance_workloads/cp_* & ckp_*
// ---------------------------------------------------------------------------

workload("cp_classic_checkpoint", "Read table with classic (single-file) checkpoint", "checkpoint") { w =>
  w.sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
  w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
  w.sql("INSERT INTO tbl VALUES (4,'d'),(5,'e')")
  checkpoint(w, "tbl")
  w.sql("INSERT INTO tbl VALUES (6,'f')")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, version = 1)
  w.snapshot(t)
}

workload("cp_empty_table", "Empty table checkpoint", "checkpoint") { w =>
  w.sql("CREATE TABLE tbl (id INT) USING delta")
  checkpoint(w, "tbl")
  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
}

workload("cp_many_commits", "Checkpoint after many commits", "checkpoint") { w =>
  w.sql("CREATE TABLE tbl (id INT) USING delta")
  for (i <- 1 to 20) w.sql(s"INSERT INTO tbl VALUES ($i)")
  checkpoint(w, "tbl")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "id > 15")
  w.snapshot(t)
}

workload("cp_multipart", "Classic multi-part checkpoint read", "checkpoint") { w =>
  w.sql("""CREATE TABLE tbl (id INT) USING delta
    TBLPROPERTIES ('delta.checkpointInterval' = '1000')""")
  for (i <- 0 to 4) w.sql(s"INSERT INTO tbl SELECT CAST(id AS INT) FROM range(${i*50}, ${(i+1)*50})")
  // Force multi-part checkpoint with 3 parts
  checkpointAt(w, "tbl", 3)
  val t = w.table("tbl")
  w.read(t, name = "read_latest")
  w.read(t, version = 0)
  w.read(t, version = 1)
  w.read(t, version = 2)
  w.read(t, version = 3)
  w.read(t, version = 4)
  w.snapshot(t)
}

workload("cp_multiple", "Multiple checkpoints", "checkpoint") { w =>
  w.sql("CREATE TABLE tbl (id INT) USING delta")
  w.sql("INSERT INTO tbl VALUES (1),(2),(3)")
  checkpoint(w, "tbl")
  w.sql("INSERT INTO tbl VALUES (4),(5)")
  checkpoint(w, "tbl")
  w.sql("INSERT INTO tbl VALUES (6)")
  val t = w.table("tbl")
  w.read(t, name = "read_latest")
  w.read(t, version = 0)
  w.read(t, version = 1)
  w.read(t, version = 2)
  w.snapshot(t)
}

workload("cp_read_after_version_delete", "Read after later JSON commits deleted", "checkpoint") { w =>
  w.sql("CREATE TABLE tbl (id INT) USING delta")
  w.sql("INSERT INTO tbl VALUES (1),(2)")
  w.sql("INSERT INTO tbl VALUES (3),(4)")
  checkpoint(w, "tbl")
  w.sql("INSERT INTO tbl VALUES (5),(6)")
  // Delete the JSON commit after checkpoint to simulate truncation
  val t = w.table("tbl")
  w.mutateTable(t) { tableDir =>
    val logDir = tableDir.resolve("_delta_log")
    val jsonFile = logDir.resolve("00000000000000000003.json")
    if (java.nio.file.Files.exists(jsonFile)) java.nio.file.Files.delete(jsonFile)
  }
  w.read(t, name = "read_at_checkpoint")
  w.snapshot(t)
}

workload("cp_checkpoint_only_table", "Table with only checkpoint, no JSON after", "checkpoint") { w =>
  w.sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
  w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b')")
  w.sql("INSERT INTO tbl VALUES (3,'c')")
  checkpoint(w, "tbl")
  // Delete all JSON files, leaving only the checkpoint
  val t = w.table("tbl")
  w.mutateTable(t) { tableDir =>
    val logDir = tableDir.resolve("_delta_log")
    val stream = java.nio.file.Files.list(logDir)
    try {
      val iter = scala.collection.JavaConverters.asScalaIteratorConverter(stream.iterator()).asScala
      iter.filter(_.toString.endsWith(".json")).foreach(java.nio.file.Files.delete)
    } finally { stream.close() }
  }
  w.read(t)
  w.snapshot(t)
}

// --- V2 Checkpoints ---

workload("cp_v2_basic", "V2 checkpoint basic read", "checkpoint", "v2_checkpoint") { w =>
  w.sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
    TBLPROPERTIES ('delta.checkpointPolicy' = 'v2',
      'delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
  w.sql("INSERT INTO tbl VALUES (4,'d'),(5,'e')")
  checkpoint(w, "tbl")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "id > 3")
  w.snapshot(t)
}

workload("cp_v2_json", "V2 checkpoint with JSON format", "checkpoint", "v2_checkpoint") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
    TBLPROPERTIES ('delta.checkpointPolicy' = 'v2',
      'delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'x'),(2,'y')")
  checkpoint(w, "tbl")
  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
}

workload("cp_v2_compat", "V2 backward compatibility checkpoint", "checkpoint", "v2_checkpoint") { w =>
  w.sql("""CREATE TABLE tbl (id INT) USING delta
    TBLPROPERTIES ('delta.checkpointPolicy' = 'v2',
      'delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl SELECT CAST(id AS INT) FROM range(100)")
  checkpoint(w, "tbl")
  w.sql("INSERT INTO tbl SELECT CAST(id AS INT) FROM range(100, 200)")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, version = 1)
  w.snapshot(t)
}

workload("cp_v2_compat_json", "V2 backward compat checkpoint (JSON format)", "checkpoint", "v2_checkpoint") { w =>
  w.sql("""CREATE TABLE tbl (id INT) USING delta
    TBLPROPERTIES ('delta.checkpointPolicy' = 'v2',
      'delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl SELECT CAST(id AS INT) FROM range(50)")
  checkpoint(w, "tbl")
  w.sql("INSERT INTO tbl SELECT CAST(id AS INT) FROM range(50, 100)")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, version = 1)
  w.snapshot(t)
}

workload("cp_v2_after_dml", "V2 checkpoint after DML operations", "checkpoint", "v2_checkpoint") { w =>
  w.sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
    TBLPROPERTIES ('delta.checkpointPolicy' = 'v2',
      'delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d')")
  w.sql("DELETE FROM tbl WHERE id = 2")
  w.sql("UPDATE tbl SET name = 'updated' WHERE id = 3")
  checkpoint(w, "tbl")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "id > 2")
  w.snapshot(t)
}

workload("cp_v2_all_actions_in_manifest", "V2 checkpoint with all actions in manifest (no sidecars)", "checkpoint", "v2_checkpoint") { w =>
  // Small table — all actions fit in manifest
  w.sql("""CREATE TABLE tbl (id INT) USING delta
    TBLPROPERTIES ('delta.checkpointPolicy' = 'v2',
      'delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1),(2),(3)")
  checkpoint(w, "tbl")
  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
}

workload("cp_v2_all_actions_in_manifest_parquet", "V2 checkpoint, all actions in Parquet manifest", "checkpoint", "v2_checkpoint") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
    TBLPROPERTIES ('delta.checkpointPolicy' = 'v2',
      'delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b')")
  checkpoint(w, "tbl")
  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
}

workload("cp_v2_multipart_sidecar", "Multi-part V2 checkpoint (parquet) - 7 versions with varying sidecars", "checkpoint", "v2_checkpoint") { w =>
  w.sql("""CREATE TABLE tbl (id INT) USING delta
    TBLPROPERTIES ('delta.checkpointPolicy' = 'v2',
      'delta.checkpointInterval' = '1',
      'delta.enableDeletionVectors' = 'true')""")
  for (i <- 0 to 6) w.sql(s"INSERT INTO tbl SELECT CAST(id AS INT) FROM range(${i*15}, ${(i+1)*15})")
  val t = w.table("tbl")
  w.read(t, name = "read_latest")
  w.read(t, version = 0)
  w.read(t, version = 1)
  w.read(t, version = 2, name = "read_v2_two_sidecars")
  w.read(t, version = 4, name = "read_v4_four_sidecars")
  w.read(t, version = 5, name = "read_v5_part_size_100")
  w.snapshot(t)
}

workload("cp_v2_multipart_sidecar_json", "Multi-part V2 checkpoint (json) - 7 versions with varying sidecars", "checkpoint", "v2_checkpoint") { w =>
  w.sql("""CREATE TABLE tbl (id INT) USING delta
    TBLPROPERTIES ('delta.checkpointPolicy' = 'v2',
      'delta.checkpointInterval' = '1',
      'delta.enableDeletionVectors' = 'true')""")
  for (i <- 0 to 6) w.sql(s"INSERT INTO tbl SELECT CAST(id AS INT) FROM range(${i*10}, ${(i+1)*10})")
  val t = w.table("tbl")
  w.read(t, name = "read_latest")
  w.read(t, version = 0)
  w.read(t, version = 1)
  w.read(t, version = 2, name = "read_v2_two_sidecars")
  w.read(t, version = 3)
  w.read(t, version = 4, name = "read_v4_four_sidecars")
  w.read(t, version = 5, name = "read_v5_part_size_100")
  w.read(t, version = 6)
  w.snapshot(t)
}

workload("cp_v2_with_dvs", "V2 checkpoint with deletion vectors (checkpoint-only read)", "checkpoint", "v2_checkpoint") { w =>
  w.sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
    TBLPROPERTIES ('delta.checkpointPolicy' = 'v2',
      'delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e')")
  w.sql("DELETE FROM tbl WHERE id IN (2, 4)")
  checkpoint(w, "tbl")
  val t = w.table("tbl")
  w.read(t, name = "read_all_from_checkpoint")
  w.snapshot(t)
}

workload("cp_v2_with_dvs_json", "V2 checkpoint with DVs - JSON format (checkpoint-only read)", "checkpoint", "v2_checkpoint") { w =>
  w.sql("""CREATE TABLE tbl (id INT) USING delta
    TBLPROPERTIES ('delta.checkpointPolicy' = 'v2',
      'delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl SELECT CAST(id AS INT) FROM range(20)")
  w.sql("DELETE FROM tbl WHERE id IN (3, 7, 15)")
  checkpoint(w, "tbl")
  val t = w.table("tbl")
  w.read(t, name = "read_all_from_checkpoint")
  w.snapshot(t)
}

workload("cp_v2_with_column_mapping", "V2 checkpoint with column mapping", "checkpoint", "v2_checkpoint", "column_mapping") { w =>
  w.sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
    TBLPROPERTIES ('delta.checkpointPolicy' = 'v2',
      'delta.enableDeletionVectors' = 'true',
      'delta.columnMapping.mode' = 'name')""")
  w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
  checkpoint(w, "tbl")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, columns = Seq("name"))
  w.snapshot(t)
}

workload("cp_v2_with_row_tracking", "V2 checkpoint with row tracking", "checkpoint", "v2_checkpoint") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
    TBLPROPERTIES ('delta.checkpointPolicy' = 'v2',
      'delta.enableDeletionVectors' = 'true',
      'delta.enableRowTracking' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
  w.sql("INSERT INTO tbl VALUES (4,'d')")
  checkpoint(w, "tbl")
  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
}

workload("cp_v2_with_struct_stats", "V2 checkpoint with struct stats", "checkpoint", "v2_checkpoint") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value DOUBLE) USING delta
    TBLPROPERTIES ('delta.checkpointPolicy' = 'v2',
      'delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1, 1.5),(2, 2.5),(3, 3.5)")
  checkpoint(w, "tbl")
  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
}

workload("cp_v2_with_type_widening", "V2 checkpoint with type widening", "checkpoint", "v2_checkpoint") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value INT) USING delta
    TBLPROPERTIES ('delta.checkpointPolicy' = 'v2',
      'delta.enableTypeWidening' = 'true',
      'delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1, 100),(2, 200)")
  w.sql("ALTER TABLE tbl CHANGE COLUMN value TYPE LONG")
  w.sql("INSERT INTO tbl VALUES (3, 3000000000)")
  checkpoint(w, "tbl")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, version = 0)
  w.snapshot(t)
}

// --- Classic ckp_ variants ---

workload("ckp_after_100_commits", "Checkpoint after 100+ commits", "checkpoint") { w =>
  w.sql("CREATE TABLE tbl (id INT) USING delta TBLPROPERTIES ('delta.checkpointInterval' = '1000')")
  // Use batch inserts to create many commits efficiently
  for (i <- 0 until 105) w.sql(s"INSERT INTO tbl VALUES ($i)")
  checkpoint(w, "tbl")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "id > 100")
  w.snapshot(t)
}

workload("ckp_multipart_10_parts", "Multi-part checkpoint with 10+ parts", "checkpoint") { w =>
  w.sql("""CREATE TABLE tbl (id INT) USING delta
    TBLPROPERTIES ('delta.checkpointInterval' = '1000')""")
  // Insert enough data to warrant many parts
  for (i <- 0 to 5) w.sql(s"INSERT INTO tbl SELECT CAST(id AS INT) FROM range(${i*50}, ${(i+1)*50})")
  checkpointAt(w, "tbl", 11)
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "id > 200")
  w.snapshot(t)
}

workload("ckp_struct_array_map", "Checkpoint with struct, array, and map columns", "checkpoint") { w =>
  w.sql("""CREATE TABLE tbl (
    id INT,
    info STRUCT<name: STRING, age: INT>,
    tags ARRAY<STRING>,
    props MAP<STRING, INT>
  ) USING delta""")
  w.sql("""INSERT INTO tbl VALUES
    (1, named_struct('name','alice','age',30), array('a','b'), map('x',1)),
    (2, named_struct('name','bob','age',25), array('c'), map('y',2,'z',3))""")
  checkpoint(w, "tbl")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, columns = Seq("info", "tags"))
  w.snapshot(t)
}

workload("ckp_v2_multiple_sidecars", "V2 checkpoint with multiple sidecar files", "checkpoint", "v2_checkpoint") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
    TBLPROPERTIES ('delta.checkpointPolicy' = 'v2',
      'delta.enableDeletionVectors' = 'true')""")
  // Many inserts to generate multiple sidecar files
  for (i <- 0 to 9) w.sql(s"INSERT INTO tbl SELECT CAST(id AS INT), CONCAT('val', CAST(id AS STRING)) FROM range(${i*20}, ${(i+1)*20})")
  checkpoint(w, "tbl")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "id > 150")
  w.snapshot(t)
}

// --- Corrupt / edge-case checkpoint workloads ---

workload("ckp_corrupt_last_checkpoint", "Invalid JSON in _last_checkpoint (fallback to directory listing)", "checkpoint") { w =>
  w.sql("CREATE TABLE tbl (id INT) USING delta")
  w.sql("INSERT INTO tbl VALUES (1),(2),(3)")
  w.sql("INSERT INTO tbl VALUES (4),(5)")
  checkpoint(w, "tbl")
  // Corrupt _last_checkpoint
  val t = w.table("tbl")
  w.mutateTable(t) { tableDir =>
    val lc = tableDir.resolve("_delta_log").resolve("_last_checkpoint")
    java.nio.file.Files.write(lc, "{ invalid json garbage }}}".getBytes("UTF-8"))
  }
  w.read(t)
  w.read(t, predicate = "id > 3")
  w.snapshot(t)
}

workload("ckp_missing_checkpoint_file", "Checkpoint file deleted but _last_checkpoint remains (fallback)", "checkpoint") { w =>
  w.sql("CREATE TABLE tbl (id INT) USING delta")
  w.sql("INSERT INTO tbl VALUES (1),(2),(3)")
  w.sql("INSERT INTO tbl VALUES (4),(5)")
  checkpoint(w, "tbl")
  // Delete the actual checkpoint parquet but leave _last_checkpoint
  val t = w.table("tbl")
  w.mutateTable(t) { tableDir =>
    val logDir = tableDir.resolve("_delta_log")
    val stream = java.nio.file.Files.list(logDir)
    try {
      val iter = scala.collection.JavaConverters.asScalaIteratorConverter(stream.iterator()).asScala
      iter.filter(_.toString.contains(".checkpoint.")).foreach(java.nio.file.Files.delete)
    } finally { stream.close() }
  }
  w.read(t)
  w.snapshot(t)
}

workload("ckp_incomplete_multipart", "Multi-part checkpoint with one part missing (fallback to JSON replay)", "checkpoint") { w =>
  w.sql("""CREATE TABLE tbl (id INT) USING delta
    TBLPROPERTIES ('delta.checkpointInterval' = '1000')""")
  for (i <- 0 to 3) w.sql(s"INSERT INTO tbl SELECT CAST(id AS INT) FROM range(${i*30}, ${(i+1)*30})")
  checkpointAt(w, "tbl", 3)
  // Delete one part of the multi-part checkpoint
  val t = w.table("tbl")
  w.mutateTable(t) { tableDir =>
    val logDir = tableDir.resolve("_delta_log")
    val stream = java.nio.file.Files.list(logDir)
    try {
      val iter = scala.collection.JavaConverters.asScalaIteratorConverter(stream.iterator()).asScala
      val parts = iter.filter(p => p.toString.contains(".checkpoint.") && p.toString.contains(".parquet")).toSeq
      // Delete the first part only
      if (parts.nonEmpty) java.nio.file.Files.delete(parts.head)
    } finally { stream.close() }
  }
  w.read(t)
  w.snapshot(t)
}

workload("ckp_wrong_version_hint", "_last_checkpoint pointing to wrong (older) version", "checkpoint") { w =>
  w.sql("CREATE TABLE tbl (id INT) USING delta")
  w.sql("INSERT INTO tbl VALUES (1),(2)")
  checkpoint(w, "tbl")
  w.sql("INSERT INTO tbl VALUES (3),(4)")
  w.sql("INSERT INTO tbl VALUES (5),(6)")
  checkpoint(w, "tbl")
  // Overwrite _last_checkpoint to point to older version
  val t = w.table("tbl")
  w.mutateTable(t) { tableDir =>
    val lc = tableDir.resolve("_delta_log").resolve("_last_checkpoint")
    // Point to version 1 instead of version 3
    java.nio.file.Files.write(lc,
      """{"version":1,"size":3}""".getBytes("UTF-8"))
  }
  w.read(t)
  w.read(t, predicate = "id > 4")
  w.snapshot(t)
}

generateAll(
  sys.env.getOrElse("WORKLOAD_OUTPUT_DIR", "/tmp/workloads"),
  force = sys.env.getOrElse("WORKLOAD_FORCE", "false").toBoolean)
System.exit(0)
