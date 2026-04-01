import io.delta.workload.WorkloadGenerator._

// -- ntz_basic: basic TIMESTAMP_NTZ column --
workload("ntz_basic", "Table with TIMESTAMP_NTZ column", "timestampNTZ") { w =>
  w.sql("""CREATE TABLE tbl (id INT, ts TIMESTAMP_NTZ) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("""INSERT INTO tbl VALUES
    (1, TIMESTAMP_NTZ'2024-01-15 10:30:00'),
    (2, TIMESTAMP_NTZ'2024-06-20 14:00:00'),
    (3, TIMESTAMP_NTZ'2024-12-31 23:59:59')""")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "ts > TIMESTAMP_NTZ'2024-06-01 00:00:00'")
  w.snapshot(t)
}

// -- ntz_far_past: NTZ with old date value --
workload("ntz_far_past", "NTZ with old date value", "timestampNTZ") { w =>
  w.sql("""CREATE TABLE tbl (id INT, ts TIMESTAMP_NTZ) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("""INSERT INTO tbl VALUES
    (1, TIMESTAMP_NTZ'1800-01-01 00:00:00'),
    (2, TIMESTAMP_NTZ'1899-12-31 23:59:59'),
    (3, TIMESTAMP_NTZ'1970-01-01 00:00:00')""")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "ts < TIMESTAMP_NTZ'1900-01-01 00:00:00'")
  w.snapshot(t)
}

// -- ntz_mixed_tz_ntz: both TIMESTAMP and TIMESTAMP_NTZ columns --
workload("ntz_mixed_tz_ntz", "Both TIMESTAMP and TIMESTAMP_NTZ columns", "timestampNTZ") { w =>
  w.sql("""CREATE TABLE tbl (id INT, ts_tz TIMESTAMP, ts_ntz TIMESTAMP_NTZ) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("""INSERT INTO tbl VALUES
    (1, TIMESTAMP'2024-01-15 10:00:00', TIMESTAMP_NTZ'2024-01-15 10:00:00'),
    (2, TIMESTAMP'2024-06-20 14:00:00', TIMESTAMP_NTZ'2024-06-20 14:00:00'),
    (3, TIMESTAMP'2024-12-31 23:59:59', TIMESTAMP_NTZ'2024-12-31 23:59:59')""")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "ts_ntz >= TIMESTAMP_NTZ'2024-06-01 00:00:00'")
  w.snapshot(t)
}

// -- ntz_partition: TIMESTAMP_NTZ as partition column --
workload("ntz_partition", "TIMESTAMP_NTZ as partition column", "timestampNTZ") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value STRING, ts_part TIMESTAMP_NTZ) USING delta
    PARTITIONED BY (ts_part) TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("""INSERT INTO tbl VALUES
    (1, 'a', TIMESTAMP_NTZ'2024-01-01 00:00:00'),
    (2, 'b', TIMESTAMP_NTZ'2024-02-01 00:00:00'),
    (3, 'c', TIMESTAMP_NTZ'2024-01-01 00:00:00'),
    (4, 'd', TIMESTAMP_NTZ'2024-03-01 00:00:00')""")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "ts_part = TIMESTAMP_NTZ'2024-01-01 00:00:00'")
  w.read(t, predicate = "ts_part >= TIMESTAMP_NTZ'2024-02-01 00:00:00'")
  w.snapshot(t)
}

// -- ntz_stats: data skipping with NTZ min/max stats --
workload("ntz_stats", "Data skipping with NTZ min/max stats", "timestampNTZ") { w =>
  w.sql("""CREATE TABLE tbl (id INT, ts TIMESTAMP_NTZ) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1, TIMESTAMP_NTZ'2024-01-15 00:00:00'),(2, TIMESTAMP_NTZ'2024-03-20 00:00:00')")
  w.sql("INSERT INTO tbl VALUES (3, TIMESTAMP_NTZ'2024-06-15 00:00:00'),(4, TIMESTAMP_NTZ'2024-06-20 00:00:00')")
  w.sql("INSERT INTO tbl VALUES (5, TIMESTAMP_NTZ'2024-12-01 00:00:00'),(6, TIMESTAMP_NTZ'2024-12-31 00:00:00')")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "ts >= TIMESTAMP_NTZ'2024-06-01 00:00:00' AND ts < TIMESTAMP_NTZ'2024-07-01 00:00:00'")
  w.read(t, predicate = "ts >= TIMESTAMP_NTZ'2024-12-01 00:00:00'")
  w.snapshot(t)
}

// -- tntz_column_mapping: NTZ with column mapping --
workload("tntz_column_mapping", "NTZ with column mapping", "timestampNTZ", "columnMapping") { w =>
  w.sql("""CREATE TABLE tbl (id INT, event_time TIMESTAMP_NTZ) USING delta
    TBLPROPERTIES (
      'delta.columnMapping.mode' = 'name',
      'delta.enableDeletionVectors' = 'true')""")
  w.sql("""INSERT INTO tbl VALUES
    (1, TIMESTAMP_NTZ'2024-01-15 10:00:00'),
    (2, TIMESTAMP_NTZ'2024-06-20 14:00:00'),
    (3, TIMESTAMP_NTZ'2024-12-31 23:59:59')""")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "event_time > TIMESTAMP_NTZ'2024-06-01 00:00:00'")
  w.snapshot(t)
}

// -- tntz_epoch: NTZ epoch value --
workload("tntz_epoch", "NTZ epoch value", "timestampNTZ") { w =>
  w.sql("""CREATE TABLE tbl (id INT, ts TIMESTAMP_NTZ) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("""INSERT INTO tbl VALUES
    (1, TIMESTAMP_NTZ'1970-01-01 00:00:00'),
    (2, TIMESTAMP_NTZ'2024-01-01 00:00:00')""")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "ts = TIMESTAMP_NTZ'1970-01-01 00:00:00'")
  w.snapshot(t)
}

// -- tntz_partition_filter: NTZ partition filter --
workload("tntz_partition_filter", "NTZ partition filter", "timestampNTZ") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value STRING, ts_part TIMESTAMP_NTZ) USING delta
    PARTITIONED BY (ts_part) TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("""INSERT INTO tbl VALUES
    (1, 'a', TIMESTAMP_NTZ'2024-01-01 00:00:00'),
    (2, 'b', TIMESTAMP_NTZ'2024-06-15 00:00:00'),
    (3, 'c', TIMESTAMP_NTZ'2024-12-25 00:00:00')""")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "ts_part = TIMESTAMP_NTZ'2024-01-01 00:00:00'")
  w.read(t, predicate = "ts_part > TIMESTAMP_NTZ'2024-06-01 00:00:00'")
  w.snapshot(t)
}

// -- tntz_time_travel: NTZ with version-based time travel --
workload("tntz_time_travel", "NTZ with version-based time travel", "timestampNTZ") { w =>
  w.sql("""CREATE TABLE tbl (id INT, ts TIMESTAMP_NTZ) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1, TIMESTAMP_NTZ'2024-01-01 00:00:00')")
  w.sql("INSERT INTO tbl VALUES (2, TIMESTAMP_NTZ'2024-06-01 00:00:00')")
  w.sql("INSERT INTO tbl VALUES (3, TIMESTAMP_NTZ'2024-12-01 00:00:00')")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, version = 1)
  w.read(t, version = 2)
  w.snapshot(t)
}

generateAll(
  sys.env.getOrElse("WORKLOAD_OUTPUT_DIR", "/tmp/workloads"),
  force = sys.env.getOrElse("WORKLOAD_FORCE", "false").toBoolean)
System.exit(0)
