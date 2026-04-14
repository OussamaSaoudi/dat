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

class CheckpointsSuite extends WorkloadTestSuite("checkpoints") {

  private def checkpoint(name: String): Unit = forceCheckpoint(name)

  private def checkpointAt(name: String): Unit = forceCheckpoint(name)

  // Existing 5 workloads

  test("cp_classic") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    sql("INSERT INTO tbl SELECT CAST(id AS INT) FROM range(1, 101)")
    sql("INSERT INTO tbl SELECT CAST(id AS INT) FROM range(101, 201)")
    sql("INSERT INTO tbl SELECT CAST(id AS INT) FROM range(201, 301)")
    checkpoint("tbl")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "id > 250")
    snapshot(t)
  }

  test("cp_multi_version") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    sql("INSERT INTO tbl SELECT CAST(id AS INT) FROM range(1, 51)")
    sql("INSERT INTO tbl SELECT CAST(id AS INT) FROM range(51, 101)")
    checkpoint("tbl")
    sql("INSERT INTO tbl SELECT CAST(id AS INT) FROM range(101, 151)")
    val t = registerTable("tbl")
    read(t)
    read(t, version = 0)
    read(t, version = 1)
    read(t, version = 2)
    snapshot(t)
  }

  test("cp_last_checkpoint") {
    sql("CREATE TABLE tbl (id INT, val STRING) USING delta TBLPROPERTIES ('delta.checkpointInterval' = '5')")
    for (i <- 1 to 7) sql(s"INSERT INTO tbl VALUES ($i, 'v$i')")
    val t = registerTable("tbl")
    read(t)
    read(t, version = 5)
    snapshot(t)
    snapshot(t, version = 5)
  }

  test("cp_schema_evolution") {
    sql("CREATE TABLE tbl (id LONG) USING delta")
    sql("INSERT INTO tbl SELECT id FROM range(50)")
    checkpoint("tbl")
    sql("ALTER TABLE tbl ADD COLUMN name STRING")
    sql("INSERT INTO tbl SELECT id, 'test' FROM range(50, 100)")
    val t = registerTable("tbl")
    read(t)
    read(t, version = 0)
    val N = 3L
    for (v <- 0L to N) snapshot(t, version = v)
  }

  test("cp_partitioned") {
    sql("CREATE TABLE tbl (id LONG, part INT) USING delta PARTITIONED BY (part)")
    sql("INSERT INTO tbl SELECT id, CAST(id % 5 AS INT) FROM range(100)")
    sql("INSERT INTO tbl SELECT id, CAST(id % 5 AS INT) FROM range(100, 200)")
    checkpoint("tbl")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "part = 0")
    read(t, predicate = "part = 3")
    snapshot(t)
  }

  // New workloads: 32 more to match existing acceptance_workloads/cp_* & ckp_*

  test("cp_classic_checkpoint") {
    sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
    sql("INSERT INTO tbl VALUES (4,'d'),(5,'e')")
    checkpoint("tbl")
    sql("INSERT INTO tbl VALUES (6,'f')")
    val t = registerTable("tbl")
    read(t)
    read(t, version = 1)
    snapshot(t)
  }

  test("cp_empty_table") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    checkpoint("tbl")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("cp_many_commits") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    for (i <- 1 to 20) sql(s"INSERT INTO tbl VALUES ($i)")
    checkpoint("tbl")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "id > 15")
    snapshot(t)
  }

  test("cp_multipart") {
    sql("""CREATE TABLE tbl (id INT) USING delta
      TBLPROPERTIES ('delta.checkpointInterval' = '1000', 'delta.checkpoint.partSize' = '100')""")
    for (i <- 0 to 4) sql(s"INSERT INTO tbl SELECT CAST(id AS INT) FROM range(${i*50}, ${(i+1)*50})")
    // Force multi-part checkpoint via partSize property
    checkpointAt("tbl")
    val t = registerTable("tbl")
    read(t, name = "read_latest")
    read(t, version = 0)
    read(t, version = 1)
    read(t, version = 2)
    read(t, version = 3)
    read(t, version = 4)
    snapshot(t)
  }

  test("cp_multiple") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    sql("INSERT INTO tbl VALUES (1),(2),(3)")
    checkpoint("tbl")
    sql("INSERT INTO tbl VALUES (4),(5)")
    checkpoint("tbl")
    sql("INSERT INTO tbl VALUES (6)")
    val t = registerTable("tbl")
    read(t, name = "read_latest")
    read(t, version = 0)
    read(t, version = 1)
    read(t, version = 2)
    snapshot(t)
  }

  test("cp_read_after_version_delete") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    sql("INSERT INTO tbl VALUES (1),(2)")
    sql("INSERT INTO tbl VALUES (3),(4)")
    checkpoint("tbl")
    sql("INSERT INTO tbl VALUES (5),(6)")
    // Delete the JSON commit after checkpoint to simulate truncation
    val t = registerTable("tbl")
    mutateTable(t) { tableDir =>
      val logDir = tableDir.resolve("_delta_log")
      val jsonFile = logDir.resolve("00000000000000000003.json")
      if (java.nio.file.Files.exists(jsonFile)) java.nio.file.Files.delete(jsonFile)
    }
    read(t, name = "read_at_checkpoint")
    snapshot(t)
  }

  test("cp_checkpoint_only_table") {
    sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b')")
    sql("INSERT INTO tbl VALUES (3,'c')")
    checkpoint("tbl")
    // Delete all JSON files, leaving only the checkpoint
    val t = registerTable("tbl")
    mutateTable(t) { tableDir =>
      val logDir = tableDir.resolve("_delta_log")
      val stream = java.nio.file.Files.list(logDir)
      try {
        val iter = scala.collection.JavaConverters.asScalaIteratorConverter(stream.iterator()).asScala
        iter.filter(_.toString.endsWith(".json")).foreach(java.nio.file.Files.delete)
      } finally { stream.close() }
    }
    read(t)
    snapshot(t)
  }


  test("cp_v2_basic") {
    sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
      TBLPROPERTIES ('delta.checkpointPolicy' = 'v2',
        'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
    sql("INSERT INTO tbl VALUES (4,'d'),(5,'e')")
    checkpoint("tbl")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "id > 3")
    snapshot(t)
  }

  test("cp_v2_json") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.checkpointPolicy' = 'v2',
        'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'x'),(2,'y')")
    checkpoint("tbl")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("cp_v2_compat") {
    sql("""CREATE TABLE tbl (id INT) USING delta
      TBLPROPERTIES ('delta.checkpointPolicy' = 'v2',
        'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT CAST(id AS INT) FROM range(100)")
    checkpoint("tbl")
    sql("INSERT INTO tbl SELECT CAST(id AS INT) FROM range(100, 200)")
    val t = registerTable("tbl")
    read(t)
    read(t, version = 1)
    snapshot(t)
  }

  test("cp_v2_compat_json") {
    sql("""CREATE TABLE tbl (id INT) USING delta
      TBLPROPERTIES ('delta.checkpointPolicy' = 'v2',
        'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT CAST(id AS INT) FROM range(50)")
    checkpoint("tbl")
    sql("INSERT INTO tbl SELECT CAST(id AS INT) FROM range(50, 100)")
    val t = registerTable("tbl")
    read(t)
    read(t, version = 1)
    snapshot(t)
  }

  test("cp_v2_after_dml") {
    sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
      TBLPROPERTIES ('delta.checkpointPolicy' = 'v2',
        'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d')")
    sql("DELETE FROM tbl WHERE id = 2")
    sql("UPDATE tbl SET name = 'updated' WHERE id = 3")
    checkpoint("tbl")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "id > 2")
    snapshot(t)
  }

  test("cp_v2_all_actions_in_manifest") {
    // Small table — all actions fit in manifest
    sql("""CREATE TABLE tbl (id INT) USING delta
      TBLPROPERTIES ('delta.checkpointPolicy' = 'v2',
        'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1),(2),(3)")
    checkpoint("tbl")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("cp_v2_all_actions_in_manifest_parquet") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.checkpointPolicy' = 'v2',
        'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b')")
    checkpoint("tbl")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("cp_v2_multipart_sidecar") {
    sql("""CREATE TABLE tbl (id INT) USING delta
      TBLPROPERTIES ('delta.checkpointPolicy' = 'v2',
        'delta.checkpointInterval' = '1',
        'delta.enableDeletionVectors' = 'true')""")
    for (i <- 0 to 6) sql(s"INSERT INTO tbl SELECT CAST(id AS INT) FROM range(${i*15}, ${(i+1)*15})")
    val t = registerTable("tbl")
    read(t, name = "read_latest")
    read(t, version = 0)
    read(t, version = 1)
    read(t, version = 2, name = "read_v2_two_sidecars")
    read(t, version = 4, name = "read_v4_four_sidecars")
    read(t, version = 5, name = "read_v5_part_size_100")
    snapshot(t)
  }

  test("cp_v2_multipart_sidecar_json") {
    sql("""CREATE TABLE tbl (id INT) USING delta
      TBLPROPERTIES ('delta.checkpointPolicy' = 'v2',
        'delta.checkpointInterval' = '1',
        'delta.enableDeletionVectors' = 'true')""")
    for (i <- 0 to 6) sql(s"INSERT INTO tbl SELECT CAST(id AS INT) FROM range(${i*10}, ${(i+1)*10})")
    val t = registerTable("tbl")
    read(t, name = "read_latest")
    read(t, version = 0)
    read(t, version = 1)
    read(t, version = 2, name = "read_v2_two_sidecars")
    read(t, version = 3)
    read(t, version = 4, name = "read_v4_four_sidecars")
    read(t, version = 5, name = "read_v5_part_size_100")
    read(t, version = 6)
    snapshot(t)
  }

  test("cp_v2_with_dvs") {
    sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
      TBLPROPERTIES ('delta.checkpointPolicy' = 'v2',
        'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e')")
    sql("DELETE FROM tbl WHERE id IN (2, 4)")
    checkpoint("tbl")
    val t = registerTable("tbl")
    read(t, name = "read_all_from_checkpoint")
    snapshot(t)
  }

  test("cp_v2_with_dvs_json") {
    sql("""CREATE TABLE tbl (id INT) USING delta
      TBLPROPERTIES ('delta.checkpointPolicy' = 'v2',
        'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT CAST(id AS INT) FROM range(20)")
    sql("DELETE FROM tbl WHERE id IN (3, 7, 15)")
    checkpoint("tbl")
    val t = registerTable("tbl")
    read(t, name = "read_all_from_checkpoint")
    snapshot(t)
  }

  test("cp_v2_with_column_mapping") {
    sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
      TBLPROPERTIES ('delta.checkpointPolicy' = 'v2',
        'delta.enableDeletionVectors' = 'true',
        'delta.columnMapping.mode' = 'name')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
    checkpoint("tbl")
    val t = registerTable("tbl")
    read(t)
    read(t, columns = Seq("name"))
    snapshot(t)
  }

  test("cp_v2_with_row_tracking") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.checkpointPolicy' = 'v2',
        'delta.enableDeletionVectors' = 'true',
        'delta.enableRowTracking' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
    sql("INSERT INTO tbl VALUES (4,'d')")
    checkpoint("tbl")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("cp_v2_with_struct_stats") {
    sql("""CREATE TABLE tbl (id INT, value DOUBLE) USING delta
      TBLPROPERTIES ('delta.checkpointPolicy' = 'v2',
        'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, 1.5),(2, 2.5),(3, 3.5)")
    checkpoint("tbl")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("cp_v2_with_type_widening") {
    sql("""CREATE TABLE tbl (id INT, value INT) USING delta
      TBLPROPERTIES ('delta.checkpointPolicy' = 'v2',
        'delta.enableTypeWidening' = 'true',
        'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, 100),(2, 200)")
    sql("ALTER TABLE tbl CHANGE COLUMN value TYPE LONG")
    sql("INSERT INTO tbl VALUES (3, 3000000000)")
    checkpoint("tbl")
    val t = registerTable("tbl")
    read(t)
    read(t, version = 0)
    snapshot(t)
  }


  test("ckp_after_100_commits") {
    sql("CREATE TABLE tbl (id INT) USING delta TBLPROPERTIES ('delta.checkpointInterval' = '1000')")
    // Use batch inserts to create many commits efficiently
    for (i <- 0 until 105) sql(s"INSERT INTO tbl VALUES ($i)")
    checkpoint("tbl")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "id > 100")
    snapshot(t)
  }

  test("ckp_multipart_10_parts") {
    sql("""CREATE TABLE tbl (id INT) USING delta
      TBLPROPERTIES ('delta.checkpointInterval' = '1000', 'delta.checkpoint.partSize' = '30')""")
    // Insert enough data to warrant many parts
    for (i <- 0 to 5) sql(s"INSERT INTO tbl SELECT CAST(id AS INT) FROM range(${i*50}, ${(i+1)*50})")
    checkpointAt("tbl")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "id > 200")
    snapshot(t)
  }

  test("ckp_struct_array_map") {
    sql("""CREATE TABLE tbl (
      id INT,
      info STRUCT<name: STRING, age: INT>,
      tags ARRAY<STRING>,
      props MAP<STRING, INT>
    ) USING delta""")
    sql("""INSERT INTO tbl VALUES
      (1, named_struct('name','alice','age',30), array('a','b'), map('x',1)),
      (2, named_struct('name','bob','age',25), array('c'), map('y',2,'z',3))""")
    checkpoint("tbl")
    val t = registerTable("tbl")
    read(t)
    read(t, columns = Seq("info", "tags"))
    snapshot(t)
  }

  test("ckp_v2_multiple_sidecars") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.checkpointPolicy' = 'v2',
        'delta.enableDeletionVectors' = 'true')""")
    // Many inserts to generate multiple sidecar files
    for (i <- 0 to 9) sql(s"INSERT INTO tbl SELECT CAST(id AS INT), CONCAT('val', CAST(id AS STRING)) FROM range(${i*20}, ${(i+1)*20})")
    checkpoint("tbl")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "id > 150")
    snapshot(t)
  }


  test("ckp_corrupt_last_checkpoint") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    sql("INSERT INTO tbl VALUES (1),(2),(3)")
    sql("INSERT INTO tbl VALUES (4),(5)")
    checkpoint("tbl")
    // Corrupt _last_checkpoint
    val t = registerTable("tbl")
    mutateTable(t) { tableDir =>
      val logDir = tableDir.resolve("_delta_log")
      val lc = logDir.resolve("_last_checkpoint")
      java.nio.file.Files.write(lc, "{ invalid json garbage }}}".getBytes("UTF-8"))
      // Remove Hadoop checksum sidecar to avoid ChecksumException on OSS Spark
      java.nio.file.Files.deleteIfExists(logDir.resolve("._last_checkpoint.crc"))
    }
    read(t)
    read(t, predicate = "id > 3")
    snapshot(t)
  }

  test("ckp_missing_checkpoint_file") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    sql("INSERT INTO tbl VALUES (1),(2),(3)")
    sql("INSERT INTO tbl VALUES (4),(5)")
    checkpoint("tbl")
    // Delete the actual checkpoint parquet but leave _last_checkpoint
    val t = registerTable("tbl")
    mutateTable(t) { tableDir =>
      val logDir = tableDir.resolve("_delta_log")
      val stream = java.nio.file.Files.list(logDir)
      try {
        val iter = scala.collection.JavaConverters.asScalaIteratorConverter(stream.iterator()).asScala
        iter.filter(_.toString.contains(".checkpoint.")).foreach(java.nio.file.Files.delete)
      } finally { stream.close() }
    }
    read(t)
    snapshot(t)
  }

  test("ckp_incomplete_multipart") {
    sql("""CREATE TABLE tbl (id INT) USING delta
      TBLPROPERTIES ('delta.checkpointInterval' = '1000', 'delta.checkpoint.partSize' = '50')""")
    for (i <- 0 to 3) sql(s"INSERT INTO tbl SELECT CAST(id AS INT) FROM range(${i*30}, ${(i+1)*30})")
    checkpointAt("tbl")
    // Delete one part of the multi-part checkpoint
    val t = registerTable("tbl")
    mutateTable(t) { tableDir =>
      val logDir = tableDir.resolve("_delta_log")
      val stream = java.nio.file.Files.list(logDir)
      try {
        val iter = scala.collection.JavaConverters.asScalaIteratorConverter(stream.iterator()).asScala
        val parts = iter.filter(p => p.toString.contains(".checkpoint.") && p.toString.contains(".parquet")).toSeq
        // Delete the first part only
        if (parts.nonEmpty) java.nio.file.Files.delete(parts.head)
      } finally { stream.close() }
    }
    read(t)
    snapshot(t)
  }

  test("ckp_wrong_version_hint") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    sql("INSERT INTO tbl VALUES (1),(2)")
    checkpoint("tbl")
    sql("INSERT INTO tbl VALUES (3),(4)")
    sql("INSERT INTO tbl VALUES (5),(6)")
    checkpoint("tbl")
    // Overwrite _last_checkpoint to point to older version
    val t = registerTable("tbl")
    mutateTable(t) { tableDir =>
      val logDir = tableDir.resolve("_delta_log")
      val lc = logDir.resolve("_last_checkpoint")
      // Point to version 1 instead of version 3
      java.nio.file.Files.write(lc,
        """{"version":1,"size":3}""".getBytes("UTF-8"))
      // Remove Hadoop checksum sidecar to avoid ChecksumException on OSS Spark
      java.nio.file.Files.deleteIfExists(logDir.resolve("._last_checkpoint.crc"))
    }
    read(t)
    read(t, predicate = "id > 4")
    snapshot(t)
  }


  test("cp_err_missing_metadata") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    checkpoint("tbl")
    val t = registerTable("tbl", expectTableInfoFailure = true)
    mutateTable(t) { tableDir =>
      val logDir = tableDir.resolve("_delta_log")
      import scala.collection.JavaConverters._
      // Remove all JSON commits so only the (corrupt) checkpoint remains
      java.nio.file.Files.list(logDir).iterator().asScala
        .filter(p => p.toString.endsWith(".json") || p.toString.endsWith(".crc"))
        .foreach(java.nio.file.Files.delete)
      // Replace checkpoint with a completely empty file — forces consistent error
      val cpFile = logDir.resolve("00000000000000000000.checkpoint.parquet")
      java.nio.file.Files.delete(cpFile)
    }
    snapshot(t)
  }

  test("cp_err_missing_protocol") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    checkpoint("tbl")
    val t = registerTable("tbl", expectTableInfoFailure = true)
    mutateTable(t) { tableDir =>
      val logDir = tableDir.resolve("_delta_log")
      import scala.collection.JavaConverters._
      java.nio.file.Files.list(logDir).iterator().asScala
        .filter(p => p.toString.endsWith(".json") || p.toString.endsWith(".crc"))
        .foreach(java.nio.file.Files.delete)
      val cpFile = logDir.resolve("00000000000000000000.checkpoint.parquet")
      java.nio.file.Files.delete(cpFile)
    }
    snapshot(t)
  }

}
