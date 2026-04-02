import io.delta.workload.WorkloadGenerator._

workload("time_travel_versions", "Read at multiple versions") { w =>
  w.sql("CREATE TABLE tbl (id INT, val STRING) USING delta")
  w.sql("INSERT INTO tbl VALUES (1, 'v1')")
  w.sql("INSERT INTO tbl VALUES (2, 'v2')")
  w.sql("INSERT INTO tbl VALUES (3, 'v3')")
  w.sql("UPDATE tbl SET val = 'updated' WHERE id = 1")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, version = 0)
  w.read(t, version = 1)
  w.read(t, version = 2)
  w.read(t, version = 3)
  w.read(t, version = 4)
  w.snapshot(t)
  w.snapshot(t, version = 0)
  w.snapshot(t, version = 1)
}

workload("time_travel_timestamps", "Timestamp-based reads") { w =>
  w.sql("CREATE TABLE tbl (id INT) USING delta")
  w.sql("INSERT INTO tbl VALUES (1),(2)")
  w.sql("INSERT INTO tbl VALUES (3),(4)")
  val t = w.table("tbl")
  val ts1 = t.getTimestampForVersion(1)
  val ts2 = t.getTimestampForVersion(2)
  w.read(t, timestamp = ts1)
  w.read(t, timestamp = ts2)
  w.read(t, timestamp = "2099-01-01 00:00:00.000")
  w.read(t, timestamp = "1970-01-01 00:00:00.000")
  w.snapshot(t, timestamp = ts1)
  w.snapshot(t)
}

workload("time_travel_schema_change", "Time travel across schema change") { w =>
  w.sql("CREATE TABLE tbl (id BIGINT) USING delta")
  w.sql("INSERT INTO tbl SELECT id FROM range(10)")
  w.sql("ALTER TABLE tbl ADD COLUMNS (name STRING)")
  w.sql("INSERT INTO tbl SELECT id, 'name' FROM range(10, 20)")
  val t = w.table("tbl")
  w.read(t, version = 1)
  w.read(t)
  w.snapshotHistory(t)
}

workload("time_travel_dv", "Time travel with deletion vectors") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e')")
  w.sql("DELETE FROM tbl WHERE id IN (2, 4)")
  w.sql("INSERT INTO tbl VALUES (6,'f'),(7,'g')")
  val t = w.table("tbl")
  w.read(t, version = 1)
  w.read(t, version = 2)
  w.read(t)
  w.read(t, version = 2, predicate = "id > 2")
  w.snapshot(t)
}

workload("time_travel_bad_version", "Error: non-existent version") { w =>
  w.sql("CREATE TABLE tbl (id INT) USING delta")
  w.sql("INSERT INTO tbl VALUES (1)")
  val t = w.table("tbl")
  w.read(t, version = 999)
  w.snapshot(t)
}

workload("time_travel_negative_version", "Error: negative version") { w =>
  w.sql("CREATE TABLE tbl (id INT) USING delta")
  w.sql("INSERT INTO tbl VALUES (1)")
  val t = w.table("tbl")
  w.read(t, version = -1)
  w.snapshot(t)
}

workload("time_travel_deleted_version", "Error: deleted commit file") { w =>
  w.sql("CREATE TABLE tbl (id BIGINT) USING delta")
  w.sql("INSERT INTO tbl SELECT id FROM range(10)")
  w.sql("INSERT INTO tbl SELECT id FROM range(10, 20)")
  w.sql("INSERT INTO tbl SELECT id FROM range(20, 30)")
  val t = w.table("tbl")
  w.mutateTable(t) { dir =>
    java.nio.file.Files.delete(dir.resolve("_delta_log/00000000000000000000.json"))
  }
  w.read(t, version = 0)
  w.snapshot(t)
}

workload("time_travel_checkpoint", "Time travel across checkpoint") { w =>
  w.sql("CREATE TABLE tbl (id BIGINT) USING delta TBLPROPERTIES ('delta.checkpointInterval' = '5')")
  for (i <- 1 to 8) w.sql(s"INSERT INTO tbl SELECT id FROM range(${(i-1)*10}, ${i*10})")
  val t = w.table("tbl")
  w.read(t, version = 3)
  w.read(t, version = 5)
  w.read(t, version = 8)
  w.snapshot(t)
}

workload("time_travel_partition_filter", "Time travel + partition filter") { w =>
  w.sql("CREATE TABLE tbl (id BIGINT, part BIGINT) USING delta PARTITIONED BY (part)")
  w.sql("INSERT INTO tbl SELECT id, id % 4 FROM range(20)")
  w.sql("INSERT INTO tbl SELECT id, id % 4 FROM range(20, 40)")
  val t = w.table("tbl")
  w.read(t, version = 1, predicate = "part = 0")
  w.read(t, version = 2, predicate = "part = 0")
  w.read(t)
  w.snapshot(t)
}

// --- New workloads below ---

workload("time_travel_column_mapping", "Time travel with column mapping changes") { w =>
  w.sql("""CREATE TABLE tbl (id INT, old_name STRING) USING delta
    TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
      'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
  w.sql("INSERT INTO tbl VALUES (1,'before')")
  w.sql("ALTER TABLE tbl RENAME COLUMN old_name TO new_name")
  w.sql("INSERT INTO tbl VALUES (2,'after')")
  val t = w.table("tbl")
  w.read(t, version = 1)
  w.read(t, version = 2)
  w.read(t)
  w.snapshot(t)
  w.snapshot(t, version = 0)
  w.snapshot(t, version = 1)
  w.snapshot(t, version = 2)
  w.snapshot(t, version = 3)
}

workload("time_travel_column_defaults", "Time travel with column defaults") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value STRING DEFAULT 'default_val') USING delta""")
  // version 0: empty table
  w.sql("INSERT INTO tbl (id) VALUES (1)")
  // version 1: row with default
  w.sql("INSERT INTO tbl VALUES (2, 'explicit')")
  val t = w.table("tbl")
  w.read(t, version = 0)
  w.read(t, version = 1)
  w.read(t)
  w.snapshot(t)
  val ts0 = t.getTimestampForVersion(0)
  val ts1 = t.getTimestampForVersion(1)
  w.read(t, timestamp = ts0)
  w.read(t, timestamp = ts1)
}

workload("time_travel_deleted_retention", "Deleted version due to retention - version not found") { w =>
  w.sql("CREATE TABLE tbl (id BIGINT) USING delta")
  w.sql("INSERT INTO tbl SELECT id FROM range(10)")
  w.sql("INSERT INTO tbl SELECT id FROM range(10, 20)")
  w.sql("INSERT INTO tbl SELECT id FROM range(20, 30)")
  val t = w.table("tbl")
  // Delete version 1 commit file to simulate retention cleanup
  w.mutateTable(t) { dir =>
    java.nio.file.Files.delete(dir.resolve("_delta_log/00000000000000000001.json"))
  }
  w.read(t, version = 1)
  w.read(t)
  w.snapshot(t)
}

workload("time_travel_after_vacuum", "Time travel after VACUUM removes old files") { w =>
  w.sql("""CREATE TABLE tbl (id BIGINT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl SELECT id FROM range(10)")
  w.sql("INSERT INTO tbl SELECT id FROM range(10, 20)")
  // Overwrite data so old files become candidates for vacuum
  w.sql("DELETE FROM tbl WHERE id < 5")
  val t = w.table("tbl")
  // Simulate vacuum by deleting old data files
  w.mutateTable(t) { dir =>
    val logDir = dir.resolve("_delta_log")
    // Read remove actions from version 3 to find files to delete
    val removePattern = """"path":"([^"]+)""".r
    val v3 = new String(java.nio.file.Files.readAllBytes(logDir.resolve("00000000000000000003.json")))
    removePattern.findAllMatchIn(v3).foreach { m =>
      val f = dir.resolve(m.group(1))
      if (java.nio.file.Files.exists(f)) java.nio.file.Files.delete(f)
    }
  }
  w.read(t, version = 1)
  w.snapshot(t)
}

workload("time_travel_checkpoint_between", "Time travel with checkpoint between versions") { w =>
  w.sql("CREATE TABLE tbl (id BIGINT) USING delta TBLPROPERTIES ('delta.checkpointInterval' = '3')")
  w.sql("INSERT INTO tbl SELECT id FROM range(10)")
  w.sql("INSERT INTO tbl SELECT id FROM range(10, 20)")
  w.sql("INSERT INTO tbl SELECT id FROM range(20, 30)")
  // checkpoint at version 3
  w.sql("INSERT INTO tbl SELECT id FROM range(30, 40)")
  w.sql("INSERT INTO tbl SELECT id FROM range(40, 50)")
  val t = w.table("tbl")
  // Read before checkpoint
  w.read(t, version = 2)
  // Read at checkpoint
  w.read(t, version = 3)
  // Read after checkpoint
  w.read(t, version = 5)
  w.read(t)
  w.snapshot(t)
}

workload("time_travel_relation_caching", "Correct relation caching for queries with time travel spec") { w =>
  w.sql("CREATE TABLE tbl (id INT, val STRING) USING delta")
  w.sql("INSERT INTO tbl VALUES (1,'v1')")
  w.sql("INSERT INTO tbl VALUES (2,'v2')")
  val t = w.table("tbl")
  // Read version 0 and version 1 in sequence - caching must not mix them
  w.read(t, version = 0)
  w.read(t, version = 1)
  val ts0 = t.getTimestampForVersion(0)
  w.read(t, timestamp = ts0)
  w.read(t)
  w.snapshot(t)
}

workload("time_travel_sql_syntax", "Time travel SQL syntax variants") { w =>
  w.sql("CREATE TABLE tbl (id INT, value STRING) USING delta")
  w.sql("INSERT INTO tbl VALUES (1,'first')")
  w.sql("INSERT INTO tbl VALUES (2,'second')")
  val t = w.table("tbl")
  // Version-based reads
  w.read(t, version = 0)
  w.read(t, version = 1)
  // Timestamp-based reads
  val ts0 = t.getTimestampForVersion(0)
  w.read(t, timestamp = ts0)
  w.read(t)
  w.snapshot(t)
}

workload("time_travel_exact_timestamp", "As of exact timestamp of commit") { w =>
  w.sql("CREATE TABLE tbl (id INT) USING delta")
  w.sql("INSERT INTO tbl VALUES (1),(2)")
  w.sql("INSERT INTO tbl VALUES (3),(4)")
  val t = w.table("tbl")
  val ts0 = t.getTimestampForVersion(0)
  w.read(t, version = 0)
  w.read(t, timestamp = ts0)
  w.read(t, version = 1)
  w.read(t)
  w.snapshot(t)
}

workload("time_travel_multi_version_scans", "Scans on different versions of same table") { w =>
  w.sql("CREATE TABLE tbl (id INT, value INT) USING delta")
  w.sql("INSERT INTO tbl VALUES (1,10),(2,20)")
  w.sql("INSERT INTO tbl VALUES (3,30),(4,40)")
  val t = w.table("tbl")
  // Multiple scans at different versions in one "session"
  w.read(t, version = 0)
  w.read(t, version = 1)
  val ts0 = t.getTimestampForVersion(0)
  w.read(t, timestamp = ts0)
  w.read(t)
  w.snapshot(t)
}

workload("time_travel_partition_evolution", "Time travel with partition changes") { w =>
  w.sql("CREATE TABLE tbl (id INT, part STRING) USING delta PARTITIONED BY (part)")
  w.sql("INSERT INTO tbl VALUES (1,'A'),(2,'B')")
  // Overwrite partition A
  w.sql("INSERT OVERWRITE tbl PARTITION (part='A') VALUES (10,'A')")
  val t = w.table("tbl")
  w.read(t, version = 0)
  w.read(t, version = 1)
  w.read(t)
  w.snapshot(t)
  w.snapshot(t, version = 0)
  w.snapshot(t, version = 1)
  val ts0 = t.getTimestampForVersion(0)
  val ts1 = t.getTimestampForVersion(1)
  w.read(t, timestamp = ts0)
  w.read(t, timestamp = ts1)
}

workload("time_travel_timestamp_between", "Timestamp between commits resolves to earlier version") { w =>
  w.sql("CREATE TABLE tbl (id INT) USING delta")
  w.sql("INSERT INTO tbl VALUES (1)")
  // Force a delay so timestamps differ
  Thread.sleep(1100)
  w.sql("INSERT INTO tbl VALUES (2)")
  Thread.sleep(1100)
  w.sql("INSERT INTO tbl VALUES (3)")
  val t = w.table("tbl")
  val ts0 = t.getTimestampForVersion(0)
  val ts1 = t.getTimestampForVersion(1)
  val ts2 = t.getTimestampForVersion(2)
  w.read(t, version = 0)
  w.read(t, version = 1)
  w.read(t, version = 2)
  w.read(t, timestamp = ts0)
  w.read(t, timestamp = ts1)
  w.read(t)
  w.snapshot(t)
}

workload("time_travel_version_0_empty", "Time travel to version 0 of empty table") { w =>
  w.sql("CREATE TABLE tbl (id INT, value STRING) USING delta")
  w.sql("INSERT INTO tbl VALUES (1,'data')")
  val t = w.table("tbl")
  w.read(t, version = 0)
  w.read(t)
  w.snapshot(t)
}

workload("time_travel_future_timestamp_error", "As of timestamp after last commit should fail") { w =>
  w.sql("CREATE TABLE tbl (id INT) USING delta")
  w.sql("INSERT INTO tbl VALUES (1),(2)")
  val t = w.table("tbl")
  w.read(t, timestamp = "2099-12-31 23:59:59.999")
  w.snapshot(t)
}

workload("time_travel_invalid_timestamp_error", "As of timestamp on invalid timestamp") { w =>
  w.sql("CREATE TABLE tbl (id INT) USING delta")
  w.sql("INSERT INTO tbl VALUES (1)")
  val t = w.table("tbl")
  w.read(t, timestamp = "not-a-timestamp")
  w.snapshot(t)
}

generateAll(
  sys.env.getOrElse("WORKLOAD_OUTPUT_DIR", "/tmp/workloads"),
  force = sys.env.getOrElse("WORKLOAD_FORCE", "false").toBoolean)
System.exit(0)
