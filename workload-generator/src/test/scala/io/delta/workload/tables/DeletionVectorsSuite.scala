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

class DeletionVectorsSuite extends WorkloadTestSuite("deletion_vectors") {

  // === Deletion Vectors ===

  test("dv_basic_delete") {
    sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e')")
    sql("DELETE FROM tbl WHERE id IN (2, 4)")
    val t = registerTable("tbl")
    read(t)
    read(t, version = 1)
    read(t, predicate = "id > 3")
    snapshot(t)
    snapshot(t, version = 1)
  }

  test("dv_large_table") {
    sql("""CREATE TABLE tbl (value INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT CAST(id AS INT) FROM range(2000)")
    sql("DELETE FROM tbl WHERE value IN (0, 180, 300, 700, 1800)")
    sql("INSERT INTO tbl VALUES (300), (700)")
    sql("DELETE FROM tbl WHERE value IN (300, 250, 350, 900, 1353, 1567, 1800)")
    sql("INSERT INTO tbl VALUES (900), (1567)")
    val t = registerTable("tbl")
    read(t, version = 0)
    read(t, version = 1)
    read(t, version = 2)
    read(t, version = 3)
    read(t, version = 4)
    snapshot(t)
  }

  test("dv_partitioned") {
    sql("""CREATE TABLE tbl (id INT, partCol INT) USING delta
      PARTITIONED BY (partCol) TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT CAST(id AS INT), CAST(id % 10 AS INT) FROM range(200)")
    sql("DELETE FROM tbl WHERE id IN (0, 18, 30, 75, 100, 150)")
    val t = registerTable("tbl")
    read(t)
    read(t, version = 1)
    read(t, predicate = "partCol = 3")
    read(t, predicate = "partCol = 3 AND id > 25")
    snapshot(t)
  }

  test("dv_all_deleted") {
    sql("""CREATE TABLE tbl (id INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1),(2),(3),(4),(5)")
    sql("DELETE FROM tbl WHERE true")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("dv_multiple_deletes") {
    sql("""CREATE TABLE tbl (id INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1),(2),(3),(4),(5),(6),(7),(8),(9),(10)")
    sql("DELETE FROM tbl WHERE id = 1")
    sql("DELETE FROM tbl WHERE id = 3")
    sql("DELETE FROM tbl WHERE id = 5")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("dv_with_column_mapping") {
    sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true',
        'delta.columnMapping.mode' = 'name', 'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    sql("INSERT INTO tbl VALUES (1,'alice'),(2,'bob'),(3,'charlie'),(4,'diana')")
    sql("ALTER TABLE tbl RENAME COLUMN name TO full_name")
    sql("INSERT INTO tbl VALUES (5, 'eve')")
    sql("DELETE FROM tbl WHERE id IN (2, 4)")
    val t = registerTable("tbl")
    read(t)
    read(t, columns = Seq("id", "full_name"))
    read(t, predicate = "id > 2")
    snapshot(t)
  }

  test("dv_no_dvs") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1)")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("dv_with_checkpoint") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
    sql("INSERT INTO tbl VALUES (4,'d'),(5,'e'),(6,'f')")
    sql("DELETE FROM tbl WHERE id IN (2, 5)")
    forceCheckpoint("tbl")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "id > 3")
    snapshot(t)
  }

  test("dv_insert_after_delete") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e')")
    sql("DELETE FROM tbl WHERE id IN (2, 4)")
    sql("INSERT INTO tbl VALUES (6,'f'),(7,'g'),(8,'h')")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "id > 5")
    read(t, predicate = "id <= 5")
    snapshot(t)
  }


  test("dv_with_merge") {
    sql("""CREATE TABLE target (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO target VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d')")
    sql("CREATE TABLE src (id INT, value STRING) USING delta")
    sql("INSERT INTO src VALUES (2,'B_updated'),(5,'e_new')")
    sql("""MERGE INTO target t USING src s ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET value = s.value
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerTable("target")
    read(t)
    read(t, version = 1)
    read(t, predicate = "id > 3")
    snapshot(t)
  }

  test("dv_with_update") {
    sql("""CREATE TABLE tbl (id INT, value INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,10),(2,20),(3,30),(4,40),(5,50)")
    sql("UPDATE tbl SET value = value * 100 WHERE id IN (2, 4)")
    val t = registerTable("tbl")
    read(t)
    read(t, version = 1)
    read(t, predicate = "value > 100")
    read(t, predicate = "value <= 50")
    snapshot(t)
  }

  test("dv_column_mapping_id") {
    sql("""CREATE TABLE tbl (id INT, category STRING, value STRING) USING delta
      PARTITIONED BY (category)
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true',
        'delta.columnMapping.mode' = 'id', 'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    sql("INSERT INTO tbl VALUES (1,'fruit','apple'),(2,'fruit','banana'),(3,'veggie','carrot'),(4,'veggie','daikon')")
    sql("DELETE FROM tbl WHERE id IN (2, 3)")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "category = 'fruit'")
    read(t, predicate = "category = 'veggie'")
    snapshot(t)
  }

  test("dv_partition_pruning_combined") {
    sql("""CREATE TABLE tbl (id INT, part STRING, value INT) USING delta
      PARTITIONED BY (part) TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'A',10),(2,'A',20),(3,'B',30),(4,'B',40),(5,'C',50),(6,'C',60)")
    sql("DELETE FROM tbl WHERE id IN (2, 4, 6)")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "part = 'A'")
    read(t, predicate = "part IN ('A','B')")
    read(t, predicate = "part = 'B' AND value > 20")
    read(t, predicate = "part = 'C'")
    snapshot(t)
  }

  test("dv_single_row_deleted") {
    sql("""CREATE TABLE tbl (id INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1)")
    sql("DELETE FROM tbl WHERE id = 1")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("dv_predicate_on_deleted") {
    sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'alice'),(2,'bob'),(3,'charlie'),(4,'diana')")
    sql("DELETE FROM tbl WHERE id IN (1, 3)")
    val t = registerTable("tbl")
    read(t)
    // Predicate that matches ONLY deleted rows
    read(t, predicate = "id = 1")
    read(t, predicate = "id = 3")
    // Predicate that matches surviving rows
    read(t, predicate = "id = 2")
    read(t, predicate = "id = 4")
    snapshot(t)
  }

  test("dv_multi_file_delete") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    // Insert in separate batches to create multiple files
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
    sql("INSERT INTO tbl VALUES (4,'d'),(5,'e'),(6,'f')")
    sql("INSERT INTO tbl VALUES (7,'g'),(8,'h'),(9,'i')")
    // Delete from each file
    sql("DELETE FROM tbl WHERE id IN (1, 5, 9)")
    val t = registerTable("tbl")
    read(t)
    read(t, version = 1)
    read(t, version = 2)
    read(t, version = 3)
    read(t, predicate = "id > 3 AND id < 8")
    snapshot(t)
  }

  test("dv_time_travel_pre_dv") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d')")
    sql("INSERT INTO tbl VALUES (5,'e'),(6,'f')")
    // Version 2: DV delete
    sql("DELETE FROM tbl WHERE id IN (2, 4)")
    val t = registerTable("tbl")
    // Read pre-DV version (should include all rows)
    read(t, version = 1)
    // Read post-DV version
    read(t)
    snapshot(t)
  }

  test("dv_insert_readback") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
    sql("DELETE FROM tbl WHERE id = 2")
    sql("INSERT INTO tbl VALUES (4,'d'),(5,'e')")
    val t = registerTable("tbl")
    read(t)
    // Only new rows
    read(t, predicate = "id > 3")
    // Surviving original rows
    read(t, predicate = "id <= 3")
    snapshot(t)
  }

  test("dv_column_projection") {
    sql("""CREATE TABLE tbl (id INT, name STRING, value DOUBLE) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'alice',1.1),(2,'bob',2.2),(3,'charlie',3.3),(4,'diana',4.4)")
    sql("DELETE FROM tbl WHERE id IN (2, 4)")
    val t = registerTable("tbl")
    read(t, columns = Seq("id", "name"))
    read(t, columns = Seq("value"))
    snapshot(t)
  }

  test("dv_with_null_values") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,NULL),(3,'c'),(4,NULL),(5,'e')")
    sql("DELETE FROM tbl WHERE value IS NULL")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("dv_projection_with_pred") {
    sql("""CREATE TABLE tbl (id INT, name STRING, value INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a',10),(2,'b',20),(3,'c',30),(4,'d',40),(5,'e',50)")
    sql("DELETE FROM tbl WHERE id IN (2, 4)")
    val t = registerTable("tbl")
    read(t, columns = Seq("id", "name"), predicate = "value > 20")
    read(t, columns = Seq("name", "value"), predicate = "id < 4")
    snapshot(t)
  }


  test("dv_all_rows_deleted") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
    sql("DELETE FROM tbl WHERE true")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("dv_checkpoint_only_read") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
    sql("DELETE FROM tbl WHERE id = 2")
    forceCheckpoint("tbl")
    val t = registerTable("tbl")
    // Remove JSON commit files, leaving only checkpoint
    mutateTable(t) { dir =>
      import scala.collection.JavaConverters._
      java.nio.file.Files.list(dir.resolve("_delta_log")).iterator().asScala
        .filter(_.toString.endsWith(".json")).foreach(java.nio.file.Files.delete)
    }
    snapshot(t)
  }

  test("dv_checkpoint_read") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
    sql("INSERT INTO tbl VALUES (4,'d'),(5,'e'),(6,'f')")
    sql("DELETE FROM tbl WHERE id IN (2, 5)")
    forceCheckpoint("tbl")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "id > 3")
    snapshot(t)
  }

  test("dv_cm_partition_combo") {
    sql("""CREATE TABLE tbl (id INT, category STRING, value INT) USING delta
      PARTITIONED BY (category)
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true',
        'delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    sql("""INSERT INTO tbl VALUES
      (1,'fruit',10),(2,'fruit',20),(3,'fruit',30),
      (4,'veggie',40),(5,'veggie',50),(6,'veggie',60),
      (7,'dairy',70),(8,'dairy',80)""")
    sql("DELETE FROM tbl WHERE id IN (2, 5, 8)")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "category = 'fruit'")
    read(t, predicate = "category = 'veggie'")
    snapshot(t)
  }

  test("dv_column_mapping_read") {
    sql("""CREATE TABLE tbl (id INT, name STRING, value INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true',
        'delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    sql("INSERT INTO tbl VALUES (1,'alice',100),(2,'bob',200)")
    sql("ALTER TABLE tbl RENAME COLUMN name TO full_name")
    sql("INSERT INTO tbl VALUES (3,'charlie',300),(4,'diana',400),(5,'eve',500)")
    sql("DELETE FROM tbl WHERE id IN (2, 4)")
    val t = registerTable("tbl")
    read(t)
    read(t, columns = Seq("id", "full_name"))
    read(t, predicate = "value > 200")
    snapshot(t)
  }

  test("dv_err_001_checksum") {
    sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(20)")
    sql("DELETE FROM tbl WHERE id < 10")
    val t = registerTable("tbl")
    // Corrupt DV checksum by modifying last byte of DV bin files
    mutateTable(t) { dir =>
      import scala.collection.JavaConverters._
      java.nio.file.Files.list(dir).iterator().asScala
        .filter(_.getFileName.toString.contains("deletion_vector"))
        .foreach { f =>
          val bytes = java.nio.file.Files.readAllBytes(f)
          if (bytes.length > 0) { bytes(bytes.length - 1) = (bytes(bytes.length - 1) ^ 0xFF).toByte }
          java.nio.file.Files.write(f, bytes)
        }
    }
    read(t)
    snapshot(t)
  }

  test("dv_err_002_missing_file") {
    sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(20)")
    sql("DELETE FROM tbl WHERE id < 10")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
      import scala.collection.JavaConverters._
      java.nio.file.Files.list(dir).iterator().asScala
        .filter(_.getFileName.toString.contains("deletion_vector"))
        .foreach(java.nio.file.Files.delete)
    }
    read(t)
  }

  test("dv_err_003_malformed_path") {
    sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    sql("DELETE FROM tbl WHERE id < 5")
    val t = registerTable("tbl")
    // Replace DV path in the commit with a malformed one
    mutateTable(t) { dir =>
      val f = dir.resolve("_delta_log/00000000000000000002.json")
      val content = new String(java.nio.file.Files.readAllBytes(f), "UTF-8")
      val patched = content.replace("deletion_vector_", "malformed/../../bad_dv_")
      java.nio.file.Files.write(f, patched.getBytes)
    }
    read(t)
    snapshot(t)
  }

  test("dv_inline_vs_ondisk") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT CAST(id AS INT), CAST(id AS STRING) FROM range(2, 100)")
    sql("INSERT INTO tbl SELECT CAST(id AS INT), CAST(id AS STRING) FROM range(100, 200)")
    sql("DELETE FROM tbl WHERE id = 1")
    sql("DELETE FROM tbl WHERE id >= 50 AND id < 100")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("dv_multiple_dvs_same_file") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e'),(6,'f'),(7,'g'),(8,'h'),(9,'i'),(10,'j')")
    sql("DELETE FROM tbl WHERE id = 1")
    sql("DELETE FROM tbl WHERE id = 3")
    sql("DELETE FROM tbl WHERE id = 5")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("dv_partition_pruning") {
    sql("""CREATE TABLE tbl (id INT, region STRING, amount INT) USING delta
      PARTITIONED BY (region)
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("""INSERT INTO tbl VALUES
      (1,'east',100),(2,'east',200),(3,'east',300),
      (4,'west',400),(5,'west',500),(6,'west',600),
      (7,'north',700),(8,'north',800),
      (9,'south',900)""")
    sql("DELETE FROM tbl WHERE id IN (1, 5, 9)")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "region = 'east'")
    read(t, predicate = "region IN ('north', 'east')")
    read(t, predicate = "region = 'west' AND amount > 400")
    snapshot(t)
  }

  test("dv_special_path_chars") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e')")
    sql("DELETE FROM tbl WHERE id = 3")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("dv_storage_type_i") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT CAST(id AS INT), CAST(id AS STRING) FROM range(1, 11)")
    sql("DELETE FROM tbl WHERE id <= 2")
    sql("DELETE FROM tbl WHERE id <= 2")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("dv_storage_type_p") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT CAST(id AS INT), CAST(id AS STRING) FROM range(1, 11)")
    sql("DELETE FROM tbl WHERE id <= 2")
    sql("DELETE FROM tbl WHERE id <= 2")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("dv_storage_type_u") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT CAST(id AS INT), CAST(id AS STRING) FROM range(1, 11)")
    sql("DELETE FROM tbl WHERE id <= 3")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("dv_with_cm_partitioned") {
    sql("""CREATE TABLE tbl (id INT, dept STRING, salary INT) USING delta
      PARTITIONED BY (dept)
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true',
        'delta.columnMapping.mode' = 'id',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    sql("""INSERT INTO tbl VALUES
      (1,'eng',100),(2,'eng',200),(3,'eng',300),
      (4,'sales',400),(5,'sales',500),
      (6,'hr',600),(7,'hr',700)""")
    sql("DELETE FROM tbl WHERE id IN (2, 5, 7)")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "dept = 'eng'")
    read(t, predicate = "dept = 'hr'")
    read(t, columns = Seq("id", "salary"))
    snapshot(t)
  }

  test("dv_with_offset") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
    sql("INSERT INTO tbl VALUES (4,'d'),(5,'e'),(6,'f')")
    sql("INSERT INTO tbl VALUES (7,'g'),(8,'h'),(9,'i')")
    sql("DELETE FROM tbl WHERE id IN (1, 4, 7)")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  // === DV Legacy ===

  /**
   * Legacy DV workloads (DV-001 through DV-018) from DeletionVectorsSuite.scala.
   *
   * These reproduce the original acceptance_workloads tables using SQL.
   * DV-001 through DV-005a share the same table pattern (2000 rows, alternating
   * delete/insert), while DV-005b through DV-018 have individual table shapes.
   *
   * NOTE: DV-017 (2B rows) is a pre-built golden table and cannot be practically
   * regenerated. The workload entry is provided for completeness but will take
   * extremely long to run.
   *
   * Run: sbt "Test/runMain io.delta.workload.TableScriptRunner tables/deletion_vectors.scala"
   */

  // DV-001: 2000 rows, 5 versions with deletes and inserts, reads at each version
  // Schema: value INT (originally created via spark.range(2000))
  // v0: CREATE TABLE + INSERT 2000 rows
  // v1: DELETE value IN (0, 180, 300, 700, 1800)
  // v2: INSERT (300, 700)
  // v3: DELETE value IN (300, 250, 350, 900, 1353, 1567, 1800)
  // v4: INSERT (900, 1567)
  test("DV-001") {
    sql("""CREATE TABLE tbl (value INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT CAST(id AS INT) FROM range(2000)")
    sql("DELETE FROM tbl WHERE value IN (0, 180, 300, 700, 1800)")
    sql("INSERT INTO tbl VALUES (300), (700)")
    sql("DELETE FROM tbl WHERE value IN (300, 250, 350, 900, 1353, 1567, 1800)")
    sql("INSERT INTO tbl VALUES (900), (1567)")
    val t = registerTable("tbl")
    read(t, version = 0, name = "version_0")
    read(t, version = 4, name = "version_4")
    snapshot(t)
  }

  // DV-002: partitioned table, 2000 rows with deletes/inserts, partition filters
  // Schema: id INT, name STRING, status STRING (default 'active')
  // Partitioned by a derived column; 2000 rows with alternating delete/insert
  test("DV-002") {
    sql("""CREATE TABLE tbl (id INT, name STRING, status STRING DEFAULT 'active') USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    // Insert 2000 rows: id 0..1999, name='name_{id}', status defaults to 'active'
    sql("""INSERT INTO tbl (id, name)
      SELECT CAST(id AS INT), CONCAT('name_', CAST(id AS STRING)) FROM range(2000)""")
    // Delete specific ids
    sql("DELETE FROM tbl WHERE id IN (0, 18, 30, 75, 100, 150, 300, 500, 700, 1000, 1500, 1800)")
    // Insert more rows
    sql("""INSERT INTO tbl (id, name)
      SELECT CAST(id AS INT), CONCAT('name_', CAST(id AS STRING)) FROM range(2000, 2500)""")
    // Second round of deletes
    sql("DELETE FROM tbl WHERE id IN (300, 350, 400, 900, 1200, 1353, 1567)")
    // Insert replacements
    sql("""INSERT INTO tbl (id, name)
      SELECT CAST(id AS INT), CONCAT('name_', CAST(id AS STRING)) FROM range(2500, 3000)""")
    val t = registerTable("tbl")
    read(t, version = 0, name = "version_0")
    read(t, version = 4, name = "version_4")
    snapshot(t)
  }

  // DV-003: metadata columns (same data as DV-001)
  test("DV-003") {
    sql("""CREATE TABLE tbl (value INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT CAST(id AS INT) FROM range(2000)")
    sql("DELETE FROM tbl WHERE value IN (0, 180, 300, 700, 1800)")
    sql("INSERT INTO tbl VALUES (300), (700)")
    sql("DELETE FROM tbl WHERE value IN (300, 250, 350, 900, 1353, 1567, 1800)")
    sql("INSERT INTO tbl VALUES (900), (1567)")
    val t = registerTable("tbl")
    read(t, columns = Seq("_metadata.file_path"), name = "metadata_file_path")
    snapshot(t)
  }

  // DV-004: filter on DV table (same data as DV-001)
  test("DV-004") {
    sql("""CREATE TABLE tbl (value INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT CAST(id AS INT) FROM range(2000)")
    sql("DELETE FROM tbl WHERE value IN (0, 180, 300, 700, 1800)")
    sql("INSERT INTO tbl VALUES (300), (700)")
    sql("DELETE FROM tbl WHERE value IN (300, 250, 350, 900, 1353, 1567, 1800)")
    sql("INSERT INTO tbl VALUES (900), (1567)")
    val t = registerTable("tbl")
    snapshot(t)
  }

  // DV-005a: subquery count on DV table (same data as DV-001)
  test("DV-005a") {
    sql("""CREATE TABLE tbl (value INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT CAST(id AS INT) FROM range(2000)")
    sql("DELETE FROM tbl WHERE value IN (0, 180, 300, 700, 1800)")
    sql("INSERT INTO tbl VALUES (300), (700)")
    sql("DELETE FROM tbl WHERE value IN (300, 250, 350, 900, 1353, 1567, 1800)")
    sql("INSERT INTO tbl VALUES (900), (1567)")
    val t = registerTable("tbl")
    read(t, name = "count")
    snapshot(t)
  }

  // DV-005b: second table for subquery test (small table)
  test("DV-005b") {
    sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, 'test')")
    val t = registerTable("tbl")
    read(t, name = "count")
    snapshot(t)
  }

  // DV-006: DELETE on table with no prior DVs (500 files, 2 rows each = 1000 rows)
  // DELETE even ids < 200 => removes 100 rows => 900 remain
  test("DV-006") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    // Create 1000 rows (id 0..999)
    sql("INSERT INTO tbl SELECT id FROM range(1000)")
    // Enable DVs explicitly (table was created without them in the original)
    sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")
    // Delete even ids < 200
    sql("DELETE FROM tbl WHERE id % 2 = 0 AND id < 200")
    val t = registerTable("tbl")
    read(t, name = "after_delete")
    snapshot(t)
  }

  // DV-007: DELETE on table that already has DVs
  // 50 rows (value 0..49), DELETE specific values twice
  test("DV-007") {
    sql("""CREATE TABLE tbl (value LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(50)")
    // First delete
    sql("DELETE FROM tbl WHERE value IN (0, 10, 20, 30, 40)")
    // Second delete (on table already containing DVs)
    sql("DELETE FROM tbl WHERE value IN (49, 29, 7, 8, 17, 36)")
    val t = registerTable("tbl")
    read(t, name = "after_additional_delete")
    snapshot(t)
  }

  // DV-008: JOIN with DVs - self-join (table2 is a small helper)
  // table2 has 1 row
  test("DV-008") {
    sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, 'test')")
    val t = registerTable("tbl")
    read(t, name = "table2_latest")
    snapshot(t)
  }

  // DV-009: JOIN with DVs - non-DV table joins DV table
  // table2 is a small helper, 1 row at v1, empty at v0
  test("DV-009") {
    sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, 'test')")
    val t = registerTable("tbl")
    read(t, name = "table2_latest_v1")
    read(t, version = 0, name = "table2_version_0")
    snapshot(t)
  }

  // DV-010: INSERT into DV table
  // 20 rows (value 0..19), DELETE 4, then INSERT 4 more
  test("DV-010") {
    sql("""CREATE TABLE tbl (value LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(20)")
    sql("DELETE FROM tbl WHERE value IN (0, 5, 10, 15)")
    sql("INSERT INTO tbl SELECT id FROM range(20, 24)")
    val t = registerTable("tbl")
    read(t, name = "after_insert")
    snapshot(t)
  }

  // DV-011: DELETE with DVs + column mapping mode
  // 10 partitions (part 0..9), 5 rows per partition
  // col1 = part + 10*i, col2 = "foo" + (part % 5)
  test("DV-011") {
    sql("""CREATE TABLE tbl (part INT, col1 INT, col2 STRING) USING delta
      PARTITIONED BY (part) TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    // Insert 50 rows: 10 partitions x 5 rows each
    sql("""INSERT INTO tbl
      SELECT
        CAST(id % 10 AS INT) as part,
        CAST(id AS INT) as col1,
        CONCAT('foo', CAST(id % 5 AS STRING)) as col2
      FROM range(50)""")
    sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")
    sql("DELETE FROM tbl WHERE col1 = 2")
    val t = registerTable("tbl")
    read(t, name = "after_delete")
    read(t, predicate = "col1 = 2", name = "filter_col1_eq_2")
    snapshot(t)
  }

  // DV-012: DELETE with DVs - packing multiple DVs
  // 200 rows in many files, DELETE even ids < 20
  test("DV-012") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(200)")
    sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")
    sql("DELETE FROM tbl WHERE id % 2 = 0 AND id < 20")
    val t = registerTable("tbl")
    read(t, name = "after_delete")
    snapshot(t)
  }

  // DV-013: MERGE with DVs - merge into DV table
  // 10 rows (value 0..9), DELETE (0, 9), then MERGE:
  //   source = range(10001, 10009) UNION values matching existing
  //   MATCHED -> UPDATE, NOT MATCHED -> INSERT
  test("DV-013") {
    sql("""CREATE TABLE tbl (value LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    sql("DELETE FROM tbl WHERE value IN (0, 9)")
    // Create source for MERGE: values 1-8 (matched) + 10001-10008 (not matched)
    sql("CREATE TABLE src (value LONG) USING delta")
    sql("INSERT INTO src SELECT id FROM range(1, 9)")
    sql("INSERT INTO src SELECT id + 10001 FROM range(8)")
    sql("""MERGE INTO tbl t USING src s ON t.value = s.value
      WHEN MATCHED THEN UPDATE SET value = s.value
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerTable("tbl")
    read(t, name = "after_merge")
    snapshot(t)
  }

  // DV-014: UPDATE with DVs - update rewrite files with DVs
  // 10 rows (value 0..9), DELETE (0, 9), then UPDATE value=1 SET value=-1
  test("DV-014") {
    sql("""CREATE TABLE tbl (value LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    sql("DELETE FROM tbl WHERE value IN (0, 9)")
    sql("UPDATE tbl SET value = -1 WHERE value = 1")
    val t = registerTable("tbl")
    read(t, name = "after_update")
    snapshot(t)
  }

  // DV-015: UPDATE with DVs - update deleted rows updates nothing
  // 10 rows (value 0..9), DELETE (0, 9), then UPDATE value=0 (no-op, already deleted)
  test("DV-015") {
    sql("""CREATE TABLE tbl (value LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    sql("DELETE FROM tbl WHERE value IN (0, 9)")
    // Trying to update a deleted row - should be a no-op
    sql("UPDATE tbl SET value = -1 WHERE value = 0")
    val t = registerTable("tbl")
    read(t, name = "after_noop_update")
    snapshot(t)
  }

  // DV-016: INSERT + DELETE + MERGE + UPDATE with DVs
  // Complex multi-step DML sequence:
  // v0: INSERT 10 rows (id 0..9)
  // v1: DELETE id IN (1, 8)
  // v2: UPDATE id=0 SET id=-1
  // v3: MERGE (source matches remaining, deletes matched, inserts new)
  test("DV-016") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    sql("DELETE FROM tbl WHERE id IN (1, 8)")
    sql("UPDATE tbl SET id = -1 WHERE id = 0")
    // MERGE: source has values matching remaining rows
    sql("CREATE TABLE src (value LONG) USING delta")
    sql("INSERT INTO src SELECT id FROM range(-1, 10)")
    sql("""MERGE INTO tbl t USING src s ON t.id = s.value
      WHEN MATCHED THEN UPDATE SET id = t.id
      WHEN NOT MATCHED THEN INSERT (id) VALUES (s.value)""")
    sql("DELETE FROM tbl WHERE id = 4")
    val t = registerTable("tbl")
    read(t, version = 0, name = "version_0_initial")
    read(t, version = 1, name = "version_1_after_delete")
    read(t, version = 2, name = "version_2_after_update")
    read(t, version = 3, name = "version_3_after_merge")
    read(t, version = 4, name = "version_4_final")
    snapshot(t)
  }

  // DV-017: Huge table - 2B+ rows with existing DV
  // WARNING: This workload will take extremely long to run. It is provided for
  // completeness. The original table was pre-built as a golden table.
  test("DV-017") {
    sql("""CREATE TABLE tbl (value INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    // WARNING: This generates ~2.1 billion rows. Only run if you have sufficient resources.
    sql("INSERT INTO tbl SELECT CAST(id AS INT) FROM range(2145386174)")
    // The original table has a DV that removes ~50000 rows
    sql("DELETE FROM tbl WHERE value >= 0 AND value < 50000")
    val t = registerTable("tbl")
    read(t, name = "full_table_count")
    snapshot(t)
  }

  // DV-018: DV feature enabled but no DVs produced
  test("DV-018") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1)")
    val t = registerTable("tbl")
    read(t, name = "read_no_dv_table")
    snapshot(t)
  }

}
