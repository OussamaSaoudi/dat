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

package io.delta.workload.tables

import io.delta.workload.WorkloadTestSuite

class TimeTravelSuite extends WorkloadTestSuite("time_travel") {

  test("time_travel_versions") {
    sql("CREATE TABLE tbl (id INT, val STRING) USING delta")
    sql("INSERT INTO tbl VALUES (1, 'v1')")
    sql("INSERT INTO tbl VALUES (2, 'v2')")
    sql("INSERT INTO tbl VALUES (3, 'v3')")
    sql("UPDATE tbl SET val = 'updated' WHERE id = 1")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, version = 0)
    readSpec(t, version = 1)
    readSpec(t, version = 2)
    readSpec(t, version = 3)
    readSpec(t, version = 4)
    snapshotSpec(t)
    snapshotSpec(t, version = 0)
    snapshotSpec(t, version = 1)
  }

  test("time_travel_timestamps") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    sql("INSERT INTO tbl VALUES (1),(2)")
    sql("INSERT INTO tbl VALUES (3),(4)")
    val t = registerTable("tbl")
    val ts1 = t.getTimestampForVersion(1)
    val ts2 = t.getTimestampForVersion(2)
    readSpec(t, timestamp = ts1, name = "read_ts_v1")
    readSpec(t, timestamp = ts2, name = "read_ts_v2")
    readSpec(t, timestamp = "2099-01-01 00:00:00.000", name = "read_ts_future")
    readSpec(t, timestamp = "1970-01-01 00:00:00.000", name = "read_ts_epoch")
    snapshotSpec(t, timestamp = ts1)
    snapshotSpec(t)
  }

  test("time_travel_schema_change") {
    sql("CREATE TABLE tbl (id BIGINT) USING delta")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    sql("ALTER TABLE tbl ADD COLUMNS (name STRING)")
    sql("INSERT INTO tbl SELECT id, 'name' FROM range(10, 20)")
    val t = registerTable("tbl")
    readSpec(t, version = 1)
    readSpec(t)
    val N = 3L
    for (v <- 0L to N) snapshotSpec(t, version = v)
  }

  test("time_travel_dv") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e')")
    sql("DELETE FROM tbl WHERE id IN (2, 4)")
    sql("INSERT INTO tbl VALUES (6,'f'),(7,'g')")
    val t = registerTable("tbl")
    readSpec(t, version = 1)
    readSpec(t, version = 2)
    readSpec(t)
    readSpec(t, version = 2, predicate = "id > 2")
    snapshotSpec(t)
  }

  test("time_travel_bad_version") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    sql("INSERT INTO tbl VALUES (1)")
    val t = registerTable("tbl")
    readSpec(t, version = 999)
    snapshotSpec(t)
  }

  test("time_travel_negative_version") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    sql("INSERT INTO tbl VALUES (1)")
    val t = registerTable("tbl")
    readSpec(t, version = -1)
    snapshotSpec(t)
  }

  test("time_travel_deleted_version") {
    sql("CREATE TABLE tbl (id BIGINT) USING delta")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    sql("INSERT INTO tbl SELECT id FROM range(10, 20)")
    sql("INSERT INTO tbl SELECT id FROM range(20, 30)")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
      java.nio.file.Files.delete(dir.resolve("_delta_log/00000000000000000000.json"))
    }
    readSpec(t, version = 0)
    snapshotSpec(t)
  }

  test("time_travel_checkpoint") {
    sql("CREATE TABLE tbl (id BIGINT) USING delta TBLPROPERTIES ('delta.checkpointInterval' = '5')")
    for (i <- 1 to 8) sql(s"INSERT INTO tbl SELECT id FROM range(${(i-1)*10}, ${i*10})")
    val t = registerTable("tbl")
    readSpec(t, version = 3)
    readSpec(t, version = 5)
    readSpec(t, version = 8)
    snapshotSpec(t)
  }

  test("time_travel_partition_filter") {
    sql("CREATE TABLE tbl (id BIGINT, part BIGINT) USING delta PARTITIONED BY (part)")
    sql("INSERT INTO tbl SELECT id, id % 4 FROM range(20)")
    sql("INSERT INTO tbl SELECT id, id % 4 FROM range(20, 40)")
    val t = registerTable("tbl")
    readSpec(t, version = 1, predicate = "part = 0")
    readSpec(t, version = 2, predicate = "part = 0")
    readSpec(t)
    snapshotSpec(t)
  }


  test("time_travel_column_mapping") {
    sql("""CREATE TABLE tbl (id INT, old_name STRING) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    sql("INSERT INTO tbl VALUES (1,'before')")
    sql("ALTER TABLE tbl RENAME COLUMN old_name TO new_name")
    sql("INSERT INTO tbl VALUES (2,'after')")
    val t = registerTable("tbl")
    readSpec(t, version = 1)
    readSpec(t, version = 2)
    readSpec(t)
    snapshotSpec(t)
    snapshotSpec(t, version = 0)
    snapshotSpec(t, version = 1)
    snapshotSpec(t, version = 2)
    snapshotSpec(t, version = 3)
  }

  test("time_travel_column_defaults") {
    sql("""CREATE TABLE tbl (id INT, value STRING DEFAULT 'default_val') USING delta""")
    // version 0: empty table
    sql("INSERT INTO tbl (id) VALUES (1)")
    // version 1: row with default
    sql("INSERT INTO tbl VALUES (2, 'explicit')")
    val t = registerTable("tbl")
    readSpec(t, version = 0)
    readSpec(t, version = 1)
    readSpec(t)
    snapshotSpec(t)
    val ts0 = t.getTimestampForVersion(0)
    val ts1 = t.getTimestampForVersion(1)
    readSpec(t, timestamp = ts0)
    readSpec(t, timestamp = ts1)
  }

  test("time_travel_deleted_retention") {
    sql("CREATE TABLE tbl (id BIGINT) USING delta")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    sql("INSERT INTO tbl SELECT id FROM range(10, 20)")
    sql("INSERT INTO tbl SELECT id FROM range(20, 30)")
    val t = registerTable("tbl")
    // Delete version 1 commit file to simulate retention cleanup
    mutateTable(t) { dir =>
      java.nio.file.Files.delete(dir.resolve("_delta_log/00000000000000000001.json"))
    }
    readSpec(t, version = 1)
    readSpec(t)
    snapshotSpec(t)
  }

  test("time_travel_after_vacuum") {
    sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    sql("INSERT INTO tbl SELECT id FROM range(10, 20)")
    // Overwrite data so old files become candidates for vacuum
    sql("DELETE FROM tbl WHERE id < 5")
    val t = registerTable("tbl")
    // Simulate vacuum by deleting old data files
    mutateTable(t) { dir =>
      val logDir = dir.resolve("_delta_log")
      // Read remove actions from version 3 to find files to delete
      val removePattern = """"path":"([^"]+)""".r
      val v3 = new String(java.nio.file.Files.readAllBytes(logDir.resolve("00000000000000000003.json")))
      removePattern.findAllMatchIn(v3).foreach { m =>
        val f = dir.resolve(m.group(1))
        if (java.nio.file.Files.exists(f)) java.nio.file.Files.delete(f)
      }
    }
    readSpec(t, version = 1)
    snapshotSpec(t)
  }

  test("time_travel_checkpoint_between") {
    sql("CREATE TABLE tbl (id BIGINT) USING delta TBLPROPERTIES ('delta.checkpointInterval' = '3')")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    sql("INSERT INTO tbl SELECT id FROM range(10, 20)")
    sql("INSERT INTO tbl SELECT id FROM range(20, 30)")
    // checkpoint at version 3
    sql("INSERT INTO tbl SELECT id FROM range(30, 40)")
    sql("INSERT INTO tbl SELECT id FROM range(40, 50)")
    val t = registerTable("tbl")
    // Read before checkpoint
    readSpec(t, version = 2)
    // Read at checkpoint
    readSpec(t, version = 3)
    // Read after checkpoint
    readSpec(t, version = 5)
    readSpec(t)
    snapshotSpec(t)
  }

  test("time_travel_relation_caching") {
    sql("CREATE TABLE tbl (id INT, val STRING) USING delta")
    sql("INSERT INTO tbl VALUES (1,'v1')")
    sql("INSERT INTO tbl VALUES (2,'v2')")
    val t = registerTable("tbl")
    // Read version 0 and version 1 in sequence - caching must not mix them
    readSpec(t, version = 0)
    readSpec(t, version = 1)
    val ts0 = t.getTimestampForVersion(0)
    readSpec(t, timestamp = ts0)
    readSpec(t)
    snapshotSpec(t)
  }

  test("time_travel_sql_syntax") {
    sql("CREATE TABLE tbl (id INT, value STRING) USING delta")
    sql("INSERT INTO tbl VALUES (1,'first')")
    sql("INSERT INTO tbl VALUES (2,'second')")
    val t = registerTable("tbl")
    // Version-based reads
    readSpec(t, version = 0)
    readSpec(t, version = 1)
    // Timestamp-based reads
    val ts0 = t.getTimestampForVersion(0)
    readSpec(t, timestamp = ts0)
    readSpec(t)
    snapshotSpec(t)
  }

  test("time_travel_exact_timestamp") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    sql("INSERT INTO tbl VALUES (1),(2)")
    sql("INSERT INTO tbl VALUES (3),(4)")
    val t = registerTable("tbl")
    val ts0 = t.getTimestampForVersion(0)
    readSpec(t, version = 0)
    readSpec(t, timestamp = ts0)
    readSpec(t, version = 1)
    readSpec(t)
    snapshotSpec(t)
  }

  test("time_travel_multi_version_scans") {
    sql("CREATE TABLE tbl (id INT, value INT) USING delta")
    sql("INSERT INTO tbl VALUES (1,10),(2,20)")
    sql("INSERT INTO tbl VALUES (3,30),(4,40)")
    val t = registerTable("tbl")
    // Multiple scans at different versions in one "session"
    readSpec(t, version = 0)
    readSpec(t, version = 1)
    val ts0 = t.getTimestampForVersion(0)
    readSpec(t, timestamp = ts0)
    readSpec(t)
    snapshotSpec(t)
  }

  test("time_travel_partition_evolution") {
    sql("CREATE TABLE tbl (id INT, part STRING) USING delta PARTITIONED BY (part)")
    sql("INSERT INTO tbl VALUES (1,'A'),(2,'B')")
    // Overwrite partition A
    sql("INSERT OVERWRITE tbl PARTITION (part='A') VALUES (10,'A')")
    val t = registerTable("tbl")
    readSpec(t, version = 0)
    readSpec(t, version = 1)
    readSpec(t)
    snapshotSpec(t)
    snapshotSpec(t, version = 0)
    snapshotSpec(t, version = 1)
    val ts0 = t.getTimestampForVersion(0)
    val ts1 = t.getTimestampForVersion(1)
    readSpec(t, timestamp = ts0)
    readSpec(t, timestamp = ts1)
  }

  test("time_travel_timestamp_between") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    sql("INSERT INTO tbl VALUES (1)")
    // Force a delay so timestamps differ
    Thread.sleep(1100)
    sql("INSERT INTO tbl VALUES (2)")
    Thread.sleep(1100)
    sql("INSERT INTO tbl VALUES (3)")
    val t = registerTable("tbl")
    val ts0 = t.getTimestampForVersion(0)
    val ts1 = t.getTimestampForVersion(1)
    val ts2 = t.getTimestampForVersion(2)
    readSpec(t, version = 0)
    readSpec(t, version = 1)
    readSpec(t, version = 2)
    readSpec(t, timestamp = ts0)
    readSpec(t, timestamp = ts1)
    readSpec(t)
    snapshotSpec(t)
  }

  test("time_travel_version_0_empty") {
    sql("CREATE TABLE tbl (id INT, value STRING) USING delta")
    sql("INSERT INTO tbl VALUES (1,'data')")
    val t = registerTable("tbl")
    readSpec(t, version = 0)
    readSpec(t)
    snapshotSpec(t)
  }

  test("time_travel_future_timestamp_error") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    sql("INSERT INTO tbl VALUES (1),(2)")
    val t = registerTable("tbl")
    readSpec(t, timestamp = "2099-12-31 23:59:59.999")
    snapshotSpec(t)
  }

  test("time_travel_invalid_timestamp_error") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    sql("INSERT INTO tbl VALUES (1)")
    val t = registerTable("tbl")
    readSpec(t, timestamp = "not-a-timestamp")
    snapshotSpec(t)
  }


  test("tt_after_vacuum") {
    sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    sql("INSERT OVERWRITE tbl SELECT id FROM range(10, 20)")
    val t = registerTable("tbl")
    // Simulate vacuum
    mutateTable(t) { dir =>
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
      val v2 = s"""{"commitInfo":{"timestamp":${ts},"operation":"VACUUM START","operationParameters":{"retentionCheckEnabled":false,"defaultRetentionMillis":604800000,"specifiedRetentionMillis":0},"isolationLevel":"SnapshotIsolation","isBlindAppend":true,"operationMetrics":{},"engineInfo":"Delta-Standalone/<unknown>"}}"""
      java.nio.file.Files.write(logDir.resolve("00000000000000000002.json"), v2.getBytes)
      val v3 = s"""{"commitInfo":{"timestamp":${ts+1},"operation":"VACUUM END","operationParameters":{"status":"COMPLETED"},"isolationLevel":"SnapshotIsolation","isBlindAppend":true,"operationMetrics":{},"engineInfo":"Delta-Standalone/<unknown>"}}"""
      java.nio.file.Files.write(logDir.resolve("00000000000000000003.json"), v3.getBytes)
    }
    readSpec(t, version = 0)
    snapshotSpec(t)
  }

  test("tt_at_syntax") {
    sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(5)")
    sql("INSERT INTO tbl SELECT id FROM range(5, 10)")
    val t = registerTable("tbl")
    readSpec(t, version = 0)
    readSpec(t, version = 1)
    val ts0 = t.getTimestampForVersion(0)
    readSpec(t, timestamp = ts0)
    readSpec(t)
    snapshotSpec(t)
  }

  test("tt_checkpoint_between") {
    sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true', 'delta.checkpointInterval' = '5')""")
    for (i <- 0 until 7)
      sql(s"INSERT INTO tbl SELECT id FROM range(${i*10}, ${(i+1)*10})")
    val t = registerTable("tbl")
    readSpec(t, version = 0)
    readSpec(t, version = 3)
    readSpec(t, version = 5)
    readSpec(t, version = 7)
    readSpec(t)
    snapshotSpec(t)
  }

  test("tt_column_defaults") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (NULL)")
    sql("ALTER TABLE tbl ALTER COLUMN id SET DEFAULT 42")
    sql("INSERT INTO tbl VALUES (DEFAULT)")
    val t = registerTable("tbl")
    readSpec(t, version = 0)
    readSpec(t, version = 1)
    readSpec(t)
    val ts0 = t.getTimestampForVersion(0)
    val ts1 = t.getTimestampForVersion(1)
    readSpec(t, timestamp = ts0)
    readSpec(t, timestamp = ts1)
    snapshotSpec(t)
  }

  test("tt_column_mapping") {
    sql("""CREATE TABLE tbl (id INT, name STRING, value INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true',
        'delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    sql("INSERT INTO tbl VALUES (1, 'alice', 100), (2, 'bob', 200)")
    sql("ALTER TABLE tbl RENAME COLUMN name TO full_name")
    sql("INSERT INTO tbl VALUES (3, 'charlie', 300), (4, 'diana', 400), (5, 'eve', 500)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, version = 1)
    readSpec(t, version = 2)
    snapshotSpec(t)
    val N = 3L
    for (v <- 0L to N) snapshotSpec(t, version = v)
  }

  test("tt_dv_between_versions") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e')")
    sql("DELETE FROM tbl WHERE id IN (2, 4)")
    sql("INSERT INTO tbl VALUES (6,'f'),(7,'g')")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, version = 1)
    readSpec(t, version = 2)
    readSpec(t, version = 2, predicate = "id > 2")
    snapshotSpec(t)
  }

  test("tt_exact_timestamp") {
    sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(5)")
    sql("INSERT INTO tbl SELECT id FROM range(5, 10)")
    val t = registerTable("tbl")
    readSpec(t, version = 0)
    val ts0 = t.getTimestampForVersion(0)
    readSpec(t, timestamp = ts0)
    readSpec(t, version = 1)
    readSpec(t)
    snapshotSpec(t)
  }

  test("tt_future_timestamp_error") {
    sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(5)")
    val t = registerTable("tbl")
    readSpec(t, timestamp = "2099-12-31 23:59:59.999")
    snapshotSpec(t)
  }

  test("tt_invalid_timestamp_error") {
    sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(5)")
    sql("INSERT INTO tbl SELECT id FROM range(5, 10)")
    val t = registerTable("tbl")
    readSpec(t, timestamp = "not-a-timestamp")
    snapshotSpec(t)
  }

  test("tt_multi_version_scans") {
    sql("""CREATE TABLE tbl (key BIGINT, value BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id, id * 10 FROM range(3)")
    sql("INSERT INTO tbl SELECT id, id * 10 FROM range(3, 5)")
    val t = registerTable("tbl")
    readSpec(t, version = 0)
    readSpec(t, version = 1)
    val ts0 = t.getTimestampForVersion(0)
    readSpec(t, timestamp = ts0)
    readSpec(t)
    snapshotSpec(t)
  }

  test("tt_non_existent_version") {
    sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    sql("INSERT INTO tbl SELECT id FROM range(10, 20)")
    val t = registerTable("tbl")
    readSpec(t, version = 5)
    snapshotSpec(t)
  }

  test("tt_nonexistent_version_error") {
    sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(5)")
    sql("INSERT INTO tbl SELECT id FROM range(5, 10)")
    sql("INSERT INTO tbl SELECT id FROM range(10, 15)")
    val t = registerTable("tbl")
    readSpec(t, version = 3)
    snapshotSpec(t)
  }

  test("tt_partition_evolution") {
    sql("""CREATE TABLE tbl (id BIGINT, part5 BIGINT) USING delta
      PARTITIONED BY (part5)
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id, id % 5 FROM range(10)")
    sql("INSERT OVERWRITE tbl SELECT id, id % 2 as part2 FROM range(10)")
    val t = registerTable("tbl")
    readSpec(t, version = 0)
    readSpec(t, version = 1)
    val ts0 = t.getTimestampForVersion(0)
    val ts1 = t.getTimestampForVersion(1)
    readSpec(t, timestamp = ts0)
    readSpec(t, timestamp = ts1)
    snapshotSpec(t)
    val N = 2L
    for (v <- 0L to N) snapshotSpec(t, version = v)
  }

  test("tt_partition_filter") {
    sql("""CREATE TABLE tbl (id BIGINT, part BIGINT) USING delta
      PARTITIONED BY (part)
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id, id % 4 FROM range(20)")
    sql("INSERT INTO tbl SELECT id, id % 4 FROM range(20, 40)")
    sql("INSERT INTO tbl SELECT id, id % 4 FROM range(40, 60)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, version = 0, predicate = "part = 0")
    readSpec(t, version = 0, predicate = "part IN (0, 1)")
    readSpec(t, version = 1, predicate = "part = 0")
    snapshotSpec(t)
  }

  test("tt_relation_caching") {
    sql("""CREATE TABLE tbl (c BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1)")
    sql("INSERT INTO tbl VALUES (2)")
    val t = registerTable("tbl")
    readSpec(t, version = 0)
    readSpec(t, version = 1)
    val ts0 = t.getTimestampForVersion(0)
    readSpec(t, timestamp = ts0)
    readSpec(t)
    snapshotSpec(t)
  }

  test("tt_sql_syntax") {
    sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(5)")
    sql("INSERT INTO tbl SELECT id FROM range(5, 10)")
    val t = registerTable("tbl")
    readSpec(t, version = 0)
    readSpec(t, version = 1)
    val ts0 = t.getTimestampForVersion(0)
    readSpec(t, timestamp = ts0)
    readSpec(t)
    snapshotSpec(t)
  }

  test("tt_timestamp_between_commits") {
    sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(5)")
    Thread.sleep(1100)
    sql("INSERT INTO tbl SELECT id FROM range(5, 10)")
    Thread.sleep(1100)
    sql("INSERT INTO tbl SELECT id FROM range(10, 15)")
    val t = registerTable("tbl")
    readSpec(t, version = 0)
    readSpec(t, version = 1)
    readSpec(t, version = 2)
    val ts0 = t.getTimestampForVersion(0)
    val ts1 = t.getTimestampForVersion(1)
    readSpec(t, timestamp = ts0)
    readSpec(t, timestamp = ts1)
    readSpec(t)
    snapshotSpec(t)
  }

  test("tt_version_0") {
    sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    sql("INSERT INTO tbl SELECT id FROM range(10, 20)")
    sql("INSERT INTO tbl SELECT id FROM range(20, 50)")
    val t = registerTable("tbl")
    readSpec(t, version = 0)
    readSpec(t, version = 0, predicate = "id < 5")
    readSpec(t)
    snapshotSpec(t)
  }

  test("tt_version_0_empty") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    val t = registerTable("tbl")
    readSpec(t, version = 0)
    snapshotSpec(t)
  }

  test("tt_version_read") {
    sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(5)")
    sql("INSERT INTO tbl SELECT id FROM range(5, 10)")
    sql("INSERT INTO tbl SELECT id FROM range(10, 15)")
    val t = registerTable("tbl")
    readSpec(t, version = 0)
    readSpec(t, version = 1)
    readSpec(t, version = 2)
    val ts0 = t.getTimestampForVersion(0)
    val ts1 = t.getTimestampForVersion(1)
    readSpec(t, timestamp = ts0)
    readSpec(t, timestamp = ts1)
    readSpec(t)
    snapshotSpec(t)
  }

  test("tt_deleted_version_retention_error") {
    sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    sql("INSERT INTO tbl SELECT id FROM range(10, 20)")
    sql("INSERT INTO tbl SELECT id FROM range(20, 30)")
    sql("INSERT INTO tbl SELECT id FROM range(30, 40)")
    sql("INSERT INTO tbl SELECT id FROM range(40, 50)")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
      java.nio.file.Files.delete(dir.resolve("_delta_log/00000000000000000000.json"))
    }
    readSpec(t, version = 0)
    snapshotSpec(t)
  }

  test("tt_schema_evolution") {
    sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(5)")
    sql("ALTER TABLE tbl ADD COLUMNS (part BIGINT)")
    sql("INSERT INTO tbl SELECT id, id % 2 FROM range(5, 10)")
    val t = registerTable("tbl")
    readSpec(t, version = 0)
    readSpec(t, version = 1)
    val ts0 = t.getTimestampForVersion(0)
    val ts1 = t.getTimestampForVersion(1)
    readSpec(t, timestamp = ts0)
    readSpec(t, timestamp = ts1)
    readSpec(t)
    snapshotSpec(t)
    val N = 3L
    for (v <- 0L to N) snapshotSpec(t, version = v)
  }

  test("tt_timestamp_travel") {
    sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(5)")
    Thread.sleep(1100)
    sql("INSERT INTO tbl SELECT id FROM range(5, 10)")
    val t = registerTable("tbl")
    val ts0 = t.getTimestampForVersion(0)
    val ts1 = t.getTimestampForVersion(1)
    readSpec(t, timestamp = ts0)
    readSpec(t, timestamp = ts1)
    readSpec(t)
    snapshotSpec(t)
  }

  test("tt_timestamp_before_retention_error") {
    sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(10, 20)")
    sql("INSERT INTO tbl SELECT id FROM range(20, 30)")
    // Checkpoint at version 2 so we can delete early commits
    forceCheckpoint("tbl")
    sql("INSERT INTO tbl SELECT id FROM range(30, 40)")
    sql("INSERT INTO tbl SELECT id FROM range(40, 50)")
    sql("INSERT INTO tbl SELECT id FROM range(50, 60)")
    val t = registerTable("tbl")
    // Record the timestamp of version 0 before deleting it
    val ts0 = t.getTimestampForVersion(0)
    // Delete version 0 JSON to simulate log cleanup / retention
    mutateTable(t) { dir =>
      val v0json = dir.resolve("_delta_log/00000000000000000000.json")
      if (java.nio.file.Files.exists(v0json)) java.nio.file.Files.delete(v0json)
    }
    // Requesting snapshot at the old timestamp should fail with
    // DELTA_TIMESTAMP_EARLIER_THAN_COMMIT_RETENTION (caught automatically by SnapshotCapture)
    snapshotSpec(t, timestamp = ts0)
    // Latest snapshot should succeed
    snapshotSpec(t)
  }

}
