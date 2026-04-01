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

generateAll(
  sys.env.getOrElse("WORKLOAD_OUTPUT_DIR", "/tmp/workloads"),
  force = sys.env.getOrElse("WORKLOAD_FORCE", "false").toBoolean)
System.exit(0)
