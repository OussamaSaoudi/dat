new WorkloadSuite("in_commit_timestamp") {

  test("ict_basic", "Basic ICT read", "inCommitTimestamp") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableInCommitTimestamps' = 'true', 'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    val t = registerTable("tbl")
    read(t, version = 0)
    snapshot(t)
  }

  test("ict_create_or_replace", "ICT preserved across REPLACE", "inCommitTimestamp") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableInCommitTimestamps' = 'true', 'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(5)")
    sql("INSERT INTO tbl SELECT id + 5 FROM range(5)")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("ict_dml", "ICT with DML operations", "inCommitTimestamp") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableInCommitTimestamps' = 'true', 'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    sql("UPDATE tbl SET id = id + 100 WHERE id < 5")
    sql("DELETE FROM tbl WHERE id >= 105")
    val t = registerTable("tbl")
    read(t)
    read(t, version = 0)
    snapshot(t)
  }

  test("ict_enable_later", "Enable ICT on existing table", "inCommitTimestamp") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableInCommitTimestamps' = 'false', 'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(5)")
    sql("INSERT INTO tbl SELECT id + 5 FROM range(5)")
    sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.enableInCommitTimestamps' = 'true')")
    sql("INSERT INTO tbl SELECT id + 10 FROM range(5)")
    val t = registerTable("tbl")
    read(t, version = 0)
    read(t, version = 1)
    read(t, version = 3)
    snapshot(t)
    snapshot(t, version = 0)
    snapshot(t, version = 1)
    snapshot(t, version = 2)
    snapshot(t, version = 3)
  }

  test("ict_enabled_mid_lifecycle", "ICT enabled at version N", "inCommitTimestamp") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableInCommitTimestamps' = 'false', 'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(5)")
    sql("INSERT INTO tbl SELECT id + 5 FROM range(5)")
    sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.enableInCommitTimestamps' = 'true')")
    sql("INSERT INTO tbl SELECT id + 10 FROM range(5)")
    sql("INSERT INTO tbl SELECT id + 15 FROM range(5)")
    val t = registerTable("tbl")
    read(t)
    read(t, version = 0)
    read(t, version = 1)
    snapshot(t)
    snapshot(t, version = 0)
    snapshot(t, version = 1)
    snapshot(t, version = 2)
    snapshot(t, version = 3)
    snapshot(t, version = 4)
  }

  test("ict_from_checkpoint", "ICT from checkpoint", "inCommitTimestamp") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES (
        'delta.enableInCommitTimestamps' = 'true',
        'delta.checkpointInterval' = '5',
        'delta.enableDeletionVectors' = 'true')""")
    for (i <- 0 to 5) sql(s"INSERT INTO tbl SELECT id + ${i * 5} FROM range(5)")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("ict_from_crc", "ICT value read from CRC file", "inCommitTimestamp") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableInCommitTimestamps' = 'true', 'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(5)")
    sql("INSERT INTO tbl SELECT id + 5 FROM range(5)")
    val t = registerTable("tbl")
    read(t, version = 0)
    read(t, version = 1)
    snapshot(t)
  }

  test("ict_multiple_commits", "ICT with multiple commits", "inCommitTimestamp") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableInCommitTimestamps' = 'true', 'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(5)")
    sql("INSERT INTO tbl SELECT id + 5 FROM range(5)")
    sql("INSERT INTO tbl SELECT id + 10 FROM range(5)")
    val t = registerTable("tbl")
    read(t, version = 0)
    read(t, version = 1)
    read(t, version = 2)
    read(t, timestamp = t.getTimestampForVersion(0), name = "timestamp_v0")
    read(t, timestamp = t.getTimestampForVersion(1), name = "timestamp_v1")
    read(t, timestamp = t.getTimestampForVersion(2), name = "timestamp_v2")
    snapshot(t)
  }

  test("ict_time_travel", "ICT with time travel", "inCommitTimestamp") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableInCommitTimestamps' = 'true', 'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    sql("INSERT INTO tbl SELECT id + 10 FROM range(10)")
    sql("INSERT INTO tbl SELECT id + 20 FROM range(10)")
    val t = registerTable("tbl")
    read(t, version = 0)
    read(t, version = 1)
    read(t, version = 2)
    read(t, timestamp = t.getTimestampForVersion(0), name = "timestamp_v0")
    read(t, timestamp = t.getTimestampForVersion(1), name = "timestamp_v1")
    read(t, timestamp = t.getTimestampForVersion(2), name = "timestamp_v2")
    snapshot(t)
  }

  test("ict_with_checkpoint", "ICT after checkpoint compaction", "inCommitTimestamp") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES (
        'delta.enableInCommitTimestamps' = 'true',
        'delta.checkpointInterval' = '3',
        'delta.enableDeletionVectors' = 'true')""")
    for (i <- 0 to 3) sql(s"INSERT INTO tbl SELECT id + ${i * 5} FROM range(5)")
    val t = registerTable("tbl")
    read(t)
    read(t, version = 0)
    read(t, version = 2)
    snapshot(t)
  }

}
