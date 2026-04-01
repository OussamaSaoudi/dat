new WorkloadSuite("time_travel") {

  test("time_travel_versions", "Read at multiple versions") { w =>
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

  test("time_travel_timestamps", "Timestamp-based reads") { w =>
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

  test("time_travel_schema_change", "Time travel across schema change") { w =>
    w.sql("CREATE TABLE tbl (id BIGINT) USING delta")
    w.sql("INSERT INTO tbl SELECT id FROM range(10)")
    w.sql("ALTER TABLE tbl ADD COLUMNS (name STRING)")
    w.sql("INSERT INTO tbl SELECT id, 'name' FROM range(10, 20)")
    val t = w.table("tbl")
    w.read(t, version = 1)
    w.read(t)
    w.snapshotHistory(t)
  }

  test("time_travel_dv", "Time travel with deletion vectors") { w =>
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

  test("time_travel_bad_version", "Error: non-existent version") { w =>
    w.sql("CREATE TABLE tbl (id INT) USING delta")
    w.sql("INSERT INTO tbl VALUES (1)")
    val t = w.table("tbl")
    w.read(t, version = 999)
    w.snapshot(t)
  }

  test("time_travel_negative_version", "Error: negative version") { w =>
    w.sql("CREATE TABLE tbl (id INT) USING delta")
    w.sql("INSERT INTO tbl VALUES (1)")
    val t = w.table("tbl")
    w.read(t, version = -1)
    w.snapshot(t)
  }

  test("time_travel_deleted_version", "Error: deleted commit file") { w =>
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

  test("time_travel_checkpoint", "Time travel across checkpoint") { w =>
    w.sql("CREATE TABLE tbl (id BIGINT) USING delta TBLPROPERTIES ('delta.checkpointInterval' = '5')")
    for (i <- 1 to 8) w.sql(s"INSERT INTO tbl SELECT id FROM range(${(i-1)*10}, ${i*10})")
    val t = w.table("tbl")
    w.read(t, version = 3)
    w.read(t, version = 5)
    w.read(t, version = 8)
    w.snapshot(t)
  }

  test("time_travel_partition_filter", "Time travel + partition filter") { w =>
    w.sql("CREATE TABLE tbl (id BIGINT, part BIGINT) USING delta PARTITIONED BY (part)")
    w.sql("INSERT INTO tbl SELECT id, id % 4 FROM range(20)")
    w.sql("INSERT INTO tbl SELECT id, id % 4 FROM range(20, 40)")
    val t = w.table("tbl")
    w.read(t, version = 1, predicate = "part = 0")
    w.read(t, version = 2, predicate = "part = 0")
    w.read(t)
    w.snapshot(t)
  }


  test("time_travel_column_mapping", "Time travel with column mapping changes") { w =>
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

  test("time_travel_column_defaults", "Time travel with column defaults") { w =>
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

  test("time_travel_deleted_retention", "Deleted version due to retention - version not found") { w =>
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

  test("time_travel_after_vacuum", "Time travel after VACUUM removes old files") { w =>
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

  test("time_travel_checkpoint_between", "Time travel with checkpoint between versions") { w =>
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

  test("time_travel_relation_caching", "Correct relation caching for queries with time travel spec") { w =>
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

  test("time_travel_sql_syntax", "Time travel SQL syntax variants") { w =>
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

  test("time_travel_exact_timestamp", "As of exact timestamp of commit") { w =>
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

  test("time_travel_multi_version_scans", "Scans on different versions of same table") { w =>
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

  test("time_travel_partition_evolution", "Time travel with partition changes") { w =>
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

  test("time_travel_timestamp_between", "Timestamp between commits resolves to earlier version") { w =>
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

  test("time_travel_version_0_empty", "Time travel to version 0 of empty table") { w =>
    w.sql("CREATE TABLE tbl (id INT, value STRING) USING delta")
    w.sql("INSERT INTO tbl VALUES (1,'data')")
    val t = w.table("tbl")
    w.read(t, version = 0)
    w.read(t)
    w.snapshot(t)
  }

  test("time_travel_future_timestamp_error", "As of timestamp after last commit should fail") { w =>
    w.sql("CREATE TABLE tbl (id INT) USING delta")
    w.sql("INSERT INTO tbl VALUES (1),(2)")
    val t = w.table("tbl")
    w.read(t, timestamp = "2099-12-31 23:59:59.999")
    w.snapshot(t)
  }

  test("time_travel_invalid_timestamp_error", "As of timestamp on invalid timestamp") { w =>
    w.sql("CREATE TABLE tbl (id INT) USING delta")
    w.sql("INSERT INTO tbl VALUES (1)")
    val t = w.table("tbl")
    w.read(t, timestamp = "not-a-timestamp")
    w.snapshot(t)
  }


  test("tt_after_vacuum", "Time travel after VACUUM removes old files", "timeTravel") { w =>
    w.sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(10)")
    w.sql("INSERT OVERWRITE tbl SELECT id FROM range(10, 20)")
    val t = w.table("tbl")
    // Simulate vacuum
    w.mutateTable(t) { dir =>
      import scala.collection.JavaConverters._
      val logDir = dir.resolve("_delta_log")
      val v1 = new String(java.nio.file.Files.readAllBytes(logDir.resolve("00000000000000000001.json")))
      val removePattern = """"path":"([^"]+)""".r
      removePattern.findAllMatchIn(v1).foreach { m =>
        val f = dir.resolve(m.group(1))
        if (java.nio.file.Files.exists(f)) java.nio.file.Files.delete(f)
      }
      // Write vacuum start/end commits
      val ts = System.currentTimeMillis()
      val v2 = s"""{"commitInfo":{"timestamp":${ts},"operation":"VACUUM START","operationParameters":{"retentionCheckEnabled":false,"defaultRetentionMillis":604800000,"specifiedRetentionMillis":0},"isolationLevel":"SnapshotIsolation","isBlindAppend":true,"operationMetrics":{},"engineInfo":"Databricks-Runtime/<unknown>"}}"""
      java.nio.file.Files.write(logDir.resolve("00000000000000000002.json"), v2.getBytes)
      val v3 = s"""{"commitInfo":{"timestamp":${ts+1},"operation":"VACUUM END","operationParameters":{"status":"COMPLETED"},"isolationLevel":"SnapshotIsolation","isBlindAppend":true,"operationMetrics":{},"engineInfo":"Databricks-Runtime/<unknown>"}}"""
      java.nio.file.Files.write(logDir.resolve("00000000000000000003.json"), v3.getBytes)
    }
    w.read(t, version = 0)
    w.snapshot(t)
  }

  test("tt_at_syntax", "Time travel path with @ syntax", "timeTravel") { w =>
    w.sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(5)")
    w.sql("INSERT INTO tbl SELECT id FROM range(5, 10)")
    val t = w.table("tbl")
    w.read(t, version = 0)
    w.read(t, version = 1)
    val ts0 = t.getTimestampForVersion(0)
    w.read(t, timestamp = ts0)
    w.read(t)
    w.snapshot(t)
  }

  test("tt_checkpoint_between", "Time travel with checkpoint between versions", "timeTravel") { w =>
    w.sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true', 'delta.checkpointInterval' = '5')""")
    for (i <- 0 until 7)
      w.sql(s"INSERT INTO tbl SELECT id FROM range(${i*10}, ${(i+1)*10})")
    val t = w.table("tbl")
    w.read(t, version = 0)
    w.read(t, version = 3)
    w.read(t, version = 5)
    w.read(t, version = 7)
    w.read(t)
    w.snapshot(t)
  }

  test("tt_column_defaults", "Time travel support with column defaults", "timeTravel") { w =>
    w.sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (NULL)")
    w.sql("ALTER TABLE tbl ALTER COLUMN id SET DEFAULT 42")
    w.sql("INSERT INTO tbl VALUES (DEFAULT)")
    val t = w.table("tbl")
    w.read(t, version = 0)
    w.read(t, version = 1)
    w.read(t)
    val ts0 = t.getTimestampForVersion(0)
    val ts1 = t.getTimestampForVersion(1)
    w.read(t, timestamp = ts0)
    w.read(t, timestamp = ts1)
    w.snapshot(t)
  }

  test("tt_column_mapping", "Time travel with column mapping changes", "timeTravel", "columnMapping") { w =>
    w.sql("""CREATE TABLE tbl (id INT, name STRING, value INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true',
        'delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    w.sql("INSERT INTO tbl VALUES (1, 'alice', 100), (2, 'bob', 200)")
    w.sql("ALTER TABLE tbl RENAME COLUMN name TO full_name")
    w.sql("INSERT INTO tbl VALUES (3, 'charlie', 300), (4, 'diana', 400), (5, 'eve', 500)")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, version = 1)
    w.read(t, version = 2)
    w.snapshot(t)
    w.snapshotHistory(t)
  }

  test("tt_dv_between_versions", "Time travel with DV changes between versions", "timeTravel", "deletionVectors") { w =>
    w.sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e')")
    w.sql("DELETE FROM tbl WHERE id IN (2, 4)")
    w.sql("INSERT INTO tbl VALUES (6,'f'),(7,'g')")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, version = 1)
    w.read(t, version = 2)
    w.read(t, version = 2, predicate = "id > 2")
    w.snapshot(t)
  }

  test("tt_exact_timestamp", "As of exact timestamp of commit", "timeTravel") { w =>
    w.sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(5)")
    w.sql("INSERT INTO tbl SELECT id FROM range(5, 10)")
    val t = w.table("tbl")
    w.read(t, version = 0)
    val ts0 = t.getTimestampForVersion(0)
    w.read(t, timestamp = ts0)
    w.read(t, version = 1)
    w.read(t)
    w.snapshot(t)
  }

  test("tt_future_timestamp_error", "As of timestamp after last commit should fail", "timeTravel") { w =>
    w.sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(5)")
    val t = w.table("tbl")
    w.read(t, timestamp = "2099-12-31 23:59:59.999")
    w.snapshot(t)
  }

  test("tt_invalid_timestamp_error", "As of timestamp on invalid timestamp", "timeTravel") { w =>
    w.sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(5)")
    w.sql("INSERT INTO tbl SELECT id FROM range(5, 10)")
    val t = w.table("tbl")
    w.read(t, timestamp = "not-a-timestamp")
    w.snapshot(t)
  }

  test("tt_multi_version_scans", "Scans on different versions of same table", "timeTravel") { w =>
    w.sql("""CREATE TABLE tbl (key BIGINT, value BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id, id * 10 FROM range(3)")
    w.sql("INSERT INTO tbl SELECT id, id * 10 FROM range(3, 5)")
    val t = w.table("tbl")
    w.read(t, version = 0)
    w.read(t, version = 1)
    val ts0 = t.getTimestampForVersion(0)
    w.read(t, timestamp = ts0)
    w.read(t)
    w.snapshot(t)
  }

  test("tt_non_existent_version", "Time travel to non-existent version", "timeTravel") { w =>
    w.sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(10)")
    w.sql("INSERT INTO tbl SELECT id FROM range(10, 20)")
    val t = w.table("tbl")
    w.read(t, version = 5)
    w.snapshot(t)
  }

  test("tt_nonexistent_version_error", "As of with versions - non-existent version", "timeTravel") { w =>
    w.sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(5)")
    w.sql("INSERT INTO tbl SELECT id FROM range(5, 10)")
    w.sql("INSERT INTO tbl SELECT id FROM range(10, 15)")
    val t = w.table("tbl")
    w.read(t, version = 3)
    w.snapshot(t)
  }

  test("tt_partition_evolution", "Time travel with partition changes", "timeTravel") { w =>
    w.sql("""CREATE TABLE tbl (id BIGINT, part5 BIGINT) USING delta
      PARTITIONED BY (part5)
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id, id % 5 FROM range(10)")
    w.sql("INSERT OVERWRITE tbl SELECT id, id % 2 as part2 FROM range(10)")
    val t = w.table("tbl")
    w.read(t, version = 0)
    w.read(t, version = 1)
    val ts0 = t.getTimestampForVersion(0)
    val ts1 = t.getTimestampForVersion(1)
    w.read(t, timestamp = ts0)
    w.read(t, timestamp = ts1)
    w.snapshot(t)
    w.snapshotHistory(t)
  }

  test("tt_partition_filter", "Time travel with partition filter", "timeTravel") { w =>
    w.sql("""CREATE TABLE tbl (id BIGINT, part BIGINT) USING delta
      PARTITIONED BY (part)
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id, id % 4 FROM range(20)")
    w.sql("INSERT INTO tbl SELECT id, id % 4 FROM range(20, 40)")
    w.sql("INSERT INTO tbl SELECT id, id % 4 FROM range(40, 60)")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, version = 0, predicate = "part = 0")
    w.read(t, version = 0, predicate = "part IN (0, 1)")
    w.read(t, version = 1, predicate = "part = 0")
    w.snapshot(t)
  }

  test("tt_relation_caching", "Correct relation caching for queries with time travel", "timeTravel") { w =>
    w.sql("""CREATE TABLE tbl (c BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1)")
    w.sql("INSERT INTO tbl VALUES (2)")
    val t = w.table("tbl")
    w.read(t, version = 0)
    w.read(t, version = 1)
    val ts0 = t.getTimestampForVersion(0)
    w.read(t, timestamp = ts0)
    w.read(t)
    w.snapshot(t)
  }

  test("tt_sql_syntax", "Time travel support in SQL - underlying reads", "timeTravel") { w =>
    w.sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(5)")
    w.sql("INSERT INTO tbl SELECT id FROM range(5, 10)")
    val t = w.table("tbl")
    w.read(t, version = 0)
    w.read(t, version = 1)
    val ts0 = t.getTimestampForVersion(0)
    w.read(t, timestamp = ts0)
    w.read(t)
    w.snapshot(t)
  }

  test("tt_timestamp_between_commits", "Timestamp between commits resolves to earlier version", "timeTravel") { w =>
    w.sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(5)")
    Thread.sleep(1100)
    w.sql("INSERT INTO tbl SELECT id FROM range(5, 10)")
    Thread.sleep(1100)
    w.sql("INSERT INTO tbl SELECT id FROM range(10, 15)")
    val t = w.table("tbl")
    w.read(t, version = 0)
    w.read(t, version = 1)
    w.read(t, version = 2)
    val ts0 = t.getTimestampForVersion(0)
    val ts1 = t.getTimestampForVersion(1)
    w.read(t, timestamp = ts0)
    w.read(t, timestamp = ts1)
    w.read(t)
    w.snapshot(t)
  }

  test("tt_version_0", "Time travel to version 0 (initial commit)", "timeTravel") { w =>
    w.sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(10)")
    w.sql("INSERT INTO tbl SELECT id FROM range(10, 20)")
    w.sql("INSERT INTO tbl SELECT id FROM range(20, 50)")
    val t = w.table("tbl")
    w.read(t, version = 0)
    w.read(t, version = 0, predicate = "id < 5")
    w.read(t)
    w.snapshot(t)
  }

  test("tt_version_0_empty", "Time travel to version 0 of empty table", "timeTravel") { w =>
    w.sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    val t = w.table("tbl")
    w.read(t, version = 0)
    w.snapshot(t)
  }

  test("tt_version_read", "As of with versions", "timeTravel") { w =>
    w.sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(5)")
    w.sql("INSERT INTO tbl SELECT id FROM range(5, 10)")
    w.sql("INSERT INTO tbl SELECT id FROM range(10, 15)")
    val t = w.table("tbl")
    w.read(t, version = 0)
    w.read(t, version = 1)
    w.read(t, version = 2)
    val ts0 = t.getTimestampForVersion(0)
    val ts1 = t.getTimestampForVersion(1)
    w.read(t, timestamp = ts0)
    w.read(t, timestamp = ts1)
    w.read(t)
    w.snapshot(t)
  }

  test("tt_deleted_version_retention_error", "Deleted version due to retention - version not found", "timeTravel") { w =>
    w.sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(10)")
    w.sql("INSERT INTO tbl SELECT id FROM range(10, 20)")
    w.sql("INSERT INTO tbl SELECT id FROM range(20, 30)")
    w.sql("INSERT INTO tbl SELECT id FROM range(30, 40)")
    w.sql("INSERT INTO tbl SELECT id FROM range(40, 50)")
    val t = w.table("tbl")
    w.mutateTable(t) { dir =>
      java.nio.file.Files.delete(dir.resolve("_delta_log/00000000000000000000.json"))
    }
    w.read(t, version = 0)
    w.snapshot(t)
  }

  test("tt_schema_evolution", "Time travel with schema changes", "timeTravel") { w =>
    w.sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(5)")
    w.sql("ALTER TABLE tbl ADD COLUMNS (part BIGINT)")
    w.sql("INSERT INTO tbl SELECT id, id % 2 FROM range(5, 10)")
    val t = w.table("tbl")
    w.read(t, version = 0)
    w.read(t, version = 1)
    val ts0 = t.getTimestampForVersion(0)
    val ts1 = t.getTimestampForVersion(1)
    w.read(t, timestamp = ts0)
    w.read(t, timestamp = ts1)
    w.read(t)
    w.snapshot(t)
    w.snapshotHistory(t)
  }

  test("tt_timestamp_travel", "Basic timestamp-based time travel", "timeTravel") { w =>
    w.sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(5)")
    Thread.sleep(1100)
    w.sql("INSERT INTO tbl SELECT id FROM range(5, 10)")
    val t = w.table("tbl")
    val ts0 = t.getTimestampForVersion(0)
    val ts1 = t.getTimestampForVersion(1)
    w.read(t, timestamp = ts0)
    w.read(t, timestamp = ts1)
    w.read(t)
    w.snapshot(t)
  }

  test("tt_timestamp_before_retention_error", "Timestamp before retention window error", "timeTravel") { w =>
    import org.apache.spark.sql.delta.DeltaLog
    w.sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(10, 20)")
    w.sql("INSERT INTO tbl SELECT id FROM range(20, 30)")
    // Checkpoint at version 2 so we can delete early commits
    val loc = w.spark.sql("DESCRIBE DETAIL tbl").collect()(0).getAs[String]("location")
    DeltaLog.clearCache()
    DeltaLog.forTable(w.spark, loc).checkpoint()
    DeltaLog.clearCache()
    w.sql("INSERT INTO tbl SELECT id FROM range(30, 40)")
    w.sql("INSERT INTO tbl SELECT id FROM range(40, 50)")
    w.sql("INSERT INTO tbl SELECT id FROM range(50, 60)")
    val t = w.table("tbl")
    // Record the timestamp of version 0 before deleting it
    val ts0 = t.getTimestampForVersion(0)
    // Delete version 0 JSON to simulate log cleanup / retention
    w.mutateTable(t) { dir =>
      val v0json = dir.resolve("_delta_log/00000000000000000000.json")
      if (java.nio.file.Files.exists(v0json)) java.nio.file.Files.delete(v0json)
    }
    // Requesting snapshot at the old timestamp should fail with
    // DELTA_TIMESTAMP_EARLIER_THAN_COMMIT_RETENTION (caught automatically by SnapshotCapture)
    w.snapshot(t, timestamp = ts0)
    // Latest snapshot should succeed
    w.snapshot(t)
  }

}.runAll()
