import io.delta.workload.WorkloadGenerator._

// -- ict_basic: basic ICT read --
workload("ict_basic", "Basic ICT read", "inCommitTimestamp") { w =>
  w.sql("""CREATE TABLE tbl (id LONG) USING delta
    TBLPROPERTIES ('delta.enableInCommitTimestamps' = 'true', 'delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl SELECT id FROM range(10)")
  val t = w.table("tbl")
  w.read(t, version = 0)
  w.snapshot(t)
}

// -- ict_create_or_replace: ICT preserved across REPLACE --
workload("ict_create_or_replace", "ICT preserved across REPLACE", "inCommitTimestamp") { w =>
  w.sql("""CREATE TABLE tbl (id LONG) USING delta
    TBLPROPERTIES ('delta.enableInCommitTimestamps' = 'true', 'delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl SELECT id FROM range(5)")
  w.sql("INSERT INTO tbl SELECT id + 5 FROM range(5)")
  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
}

// -- ict_dml: ICT with DML operations --
workload("ict_dml", "ICT with DML operations", "inCommitTimestamp") { w =>
  w.sql("""CREATE TABLE tbl (id LONG) USING delta
    TBLPROPERTIES ('delta.enableInCommitTimestamps' = 'true', 'delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl SELECT id FROM range(10)")
  w.sql("UPDATE tbl SET id = id + 100 WHERE id < 5")
  w.sql("DELETE FROM tbl WHERE id >= 105")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, version = 0)
  w.snapshot(t)
}

// -- ict_enable_later: enable ICT on existing table --
workload("ict_enable_later", "Enable ICT on existing table", "inCommitTimestamp") { w =>
  w.sql("""CREATE TABLE tbl (id LONG) USING delta
    TBLPROPERTIES ('delta.enableInCommitTimestamps' = 'false', 'delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl SELECT id FROM range(5)")
  w.sql("INSERT INTO tbl SELECT id + 5 FROM range(5)")
  w.sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.enableInCommitTimestamps' = 'true')")
  w.sql("INSERT INTO tbl SELECT id + 10 FROM range(5)")
  val t = w.table("tbl")
  w.read(t, version = 0)
  w.read(t, version = 1)
  w.read(t, version = 3)
  w.snapshot(t)
  w.snapshot(t, version = 0)
  w.snapshot(t, version = 1)
  w.snapshot(t, version = 2)
  w.snapshot(t, version = 3)
}

// -- ict_enabled_mid_lifecycle: ICT enabled at version N --
workload("ict_enabled_mid_lifecycle", "ICT enabled at version N", "inCommitTimestamp") { w =>
  w.sql("""CREATE TABLE tbl (id LONG) USING delta
    TBLPROPERTIES ('delta.enableInCommitTimestamps' = 'false', 'delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl SELECT id FROM range(5)")
  w.sql("INSERT INTO tbl SELECT id + 5 FROM range(5)")
  w.sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.enableInCommitTimestamps' = 'true')")
  w.sql("INSERT INTO tbl SELECT id + 10 FROM range(5)")
  w.sql("INSERT INTO tbl SELECT id + 15 FROM range(5)")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, version = 0)
  w.read(t, version = 1)
  w.snapshot(t)
  w.snapshot(t, version = 0)
  w.snapshot(t, version = 1)
  w.snapshot(t, version = 2)
  w.snapshot(t, version = 3)
  w.snapshot(t, version = 4)
}

// -- ict_from_checkpoint: ICT from checkpoint --
workload("ict_from_checkpoint", "ICT from checkpoint", "inCommitTimestamp") { w =>
  w.sql("""CREATE TABLE tbl (id LONG) USING delta
    TBLPROPERTIES (
      'delta.enableInCommitTimestamps' = 'true',
      'delta.checkpointInterval' = '5',
      'delta.enableDeletionVectors' = 'true')""")
  for (i <- 0 to 5) w.sql(s"INSERT INTO tbl SELECT id + ${i * 5} FROM range(5)")
  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
}

// -- ict_from_crc: ICT value read from CRC file --
workload("ict_from_crc", "ICT value read from CRC file", "inCommitTimestamp") { w =>
  w.sql("""CREATE TABLE tbl (id LONG) USING delta
    TBLPROPERTIES ('delta.enableInCommitTimestamps' = 'true', 'delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl SELECT id FROM range(5)")
  w.sql("INSERT INTO tbl SELECT id + 5 FROM range(5)")
  val t = w.table("tbl")
  w.read(t, version = 0)
  w.read(t, version = 1)
  w.snapshot(t)
}

// -- ict_multiple_commits: ICT with multiple commits --
workload("ict_multiple_commits", "ICT with multiple commits", "inCommitTimestamp") { w =>
  w.sql("""CREATE TABLE tbl (id LONG) USING delta
    TBLPROPERTIES ('delta.enableInCommitTimestamps' = 'true', 'delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl SELECT id FROM range(5)")
  w.sql("INSERT INTO tbl SELECT id + 5 FROM range(5)")
  w.sql("INSERT INTO tbl SELECT id + 10 FROM range(5)")
  val t = w.table("tbl")
  w.read(t, version = 0)
  w.read(t, version = 1)
  w.read(t, version = 2)
  w.snapshot(t)
}

// -- ict_time_travel: ICT with time travel --
workload("ict_time_travel", "ICT with time travel", "inCommitTimestamp") { w =>
  w.sql("""CREATE TABLE tbl (id LONG) USING delta
    TBLPROPERTIES ('delta.enableInCommitTimestamps' = 'true', 'delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl SELECT id FROM range(10)")
  w.sql("INSERT INTO tbl SELECT id + 10 FROM range(10)")
  w.sql("INSERT INTO tbl SELECT id + 20 FROM range(10)")
  val t = w.table("tbl")
  w.read(t, version = 0)
  w.read(t, version = 1)
  w.read(t, version = 2)
  w.snapshot(t)
}

// -- ict_with_checkpoint: ICT after checkpoint compaction --
workload("ict_with_checkpoint", "ICT after checkpoint compaction", "inCommitTimestamp") { w =>
  w.sql("""CREATE TABLE tbl (id LONG) USING delta
    TBLPROPERTIES (
      'delta.enableInCommitTimestamps' = 'true',
      'delta.checkpointInterval' = '3',
      'delta.enableDeletionVectors' = 'true')""")
  for (i <- 0 to 3) w.sql(s"INSERT INTO tbl SELECT id + ${i * 5} FROM range(5)")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, version = 0)
  w.read(t, version = 2)
  w.snapshot(t)
}

generateAll(
  sys.env.getOrElse("WORKLOAD_OUTPUT_DIR", "/tmp/workloads"),
  force = sys.env.getOrElse("WORKLOAD_FORCE", "false").toBoolean)
System.exit(0)
