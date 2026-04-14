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

class CdcSuite extends WorkloadTestSuite("cdc") {

  test("cdc_by_path") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    val t = registerTable("tbl")
    read(t)
    read(t, version = 0)
    read(t, version = 1)
    cdf(t, startVersion = 0, endVersion = 1)
    snapshot(t)
  }

  test("cdc_version_range") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    sql("INSERT INTO tbl SELECT id + 10 FROM range(10)")
    sql("INSERT INTO tbl SELECT id + 20 FROM range(10)")
    val t = registerTable("tbl")
    read(t)
    read(t, version = 1)
    read(t, version = 2)
    read(t, version = 3)
    cdf(t, startVersion = 0, endVersion = 3)
    cdf(t, startVersion = 1, endVersion = 2)
    snapshot(t)
  }

  test("cdc_single_version") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    sql("INSERT INTO tbl SELECT id + 10 FROM range(10)")
    val t = registerTable("tbl")
    read(t, version = 1)
    read(t, version = 2)
    cdf(t, startVersion = 1, endVersion = 1)
    cdf(t, startVersion = 2, endVersion = 2)
    snapshot(t)
  }

  test("cdc_inserts") {
    sql("""CREATE TABLE tbl (id LONG, name STRING) USING delta
      TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'alice'),(2,'bob')")
    sql("INSERT INTO tbl VALUES (3,'charlie'),(4,'dave')")
    sql("INSERT INTO tbl VALUES (5,'eve'),(6,'frank')")
    val t = registerTable("tbl")
    read(t)
    read(t, version = 1)
    read(t, version = 2)
    read(t, version = 3)
    cdf(t, startVersion = 0, endVersion = 3)
    cdf(t, startVersion = 1, endVersion = 1)
    snapshot(t)
  }

  test("cdc_updates") {
    sql("""CREATE TABLE tbl (id LONG, value INT) USING delta
      TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,100),(2,200),(3,300)")
    sql("UPDATE tbl SET value = value * 2 WHERE id = 1")
    sql("UPDATE tbl SET value = value + 50 WHERE id IN (2, 3)")
    val t = registerTable("tbl")
    read(t)
    read(t, version = 1)
    read(t, version = 2)
    read(t, version = 3)
    read(t, predicate = "id = 1")
    cdf(t, startVersion = 0, endVersion = 3)
    cdf(t, startVersion = 1, endVersion = 3)
    snapshot(t)
  }

  test("cdc_deletes") {
    sql("""CREATE TABLE tbl (id LONG, name STRING) USING delta
      TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e')")
    sql("DELETE FROM tbl WHERE id = 3")
    sql("DELETE FROM tbl WHERE id IN (1, 5)")
    val t = registerTable("tbl")
    read(t)
    read(t, version = 1)
    read(t, version = 2)
    read(t, version = 3)
    cdf(t, startVersion = 0, endVersion = 3)
    cdf(t, startVersion = 1, endVersion = 3)
    snapshot(t)
  }

  test("cdc_merge") {
    sql("""CREATE TABLE target (id LONG, value INT) USING delta
      TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')""")
    sql("INSERT INTO target VALUES (1,100),(2,200),(3,300)")
    sql("CREATE TABLE src (id LONG, value INT) USING delta")
    sql("INSERT INTO src VALUES (2,250),(4,400)")
    sql("""MERGE INTO target t USING src s ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET value = s.value
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerTable("target")
    read(t)
    read(t, version = 1)
    read(t, version = 2)
    read(t, predicate = "id = 2")
    cdf(t, startVersion = 0, endVersion = 2)
    cdf(t, startVersion = 2, endVersion = 2)
    snapshot(t)
  }

  test("cdc_merge_delete") {
    sql("""CREATE TABLE tbl (id LONG, value INT, status STRING) USING delta
      TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,100,'active'),(2,200,'active'),(3,300,'inactive')")
    sql("CREATE TABLE msrc (id LONG, value INT, status STRING) USING delta")
    sql("INSERT INTO msrc VALUES (2,250,'updated'),(3,0,'deleted'),(4,400,'new')")
    sql("""MERGE INTO tbl t USING msrc s ON t.id = s.id
      WHEN MATCHED AND s.status = 'deleted' THEN DELETE
      WHEN MATCHED THEN UPDATE SET value = s.value, status = s.status
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerTable("tbl")
    read(t)
    read(t, version = 1)
    read(t, version = 2)
    cdf(t, startVersion = 0, endVersion = 2)
    cdf(t, startVersion = 2, endVersion = 2)
    snapshot(t)
  }

  test("cdc_multiple_refs") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    sql("INSERT INTO tbl SELECT id + 10 FROM range(10)")
    sql("INSERT INTO tbl SELECT id + 20 FROM range(10)")
    val t = registerTable("tbl")
    read(t, version = 1)
    read(t, version = 2)
    read(t, version = 3)
    read(t, predicate = "id < 10")
    read(t, predicate = "id >= 20")
    cdf(t, startVersion = 0, endVersion = 3)
    snapshot(t)
  }

  test("cdc_metadata_filter") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    sql("UPDATE tbl SET id = id + 100 WHERE id < 5")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "id >= 100")
    read(t, predicate = "id < 100")
    cdf(t, startVersion = 0)
    snapshot(t)
  }

  test("cdc_partitioned") {
    sql("""CREATE TABLE tbl (id LONG, category STRING, value INT) USING delta
      PARTITIONED BY (category) TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'A',100),(2,'A',200)")
    sql("INSERT INTO tbl VALUES (3,'B',300),(4,'B',400)")
    sql("INSERT INTO tbl VALUES (5,'C',500)")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "category = 'A'")
    read(t, predicate = "category = 'B'")
    read(t, predicate = "category = 'C'")
    cdf(t, startVersion = 0)
    snapshot(t)
  }

  test("cdc_partitioned_dml") {
    sql("""CREATE TABLE tbl (id LONG, category STRING, value INT) USING delta
      PARTITIONED BY (category) TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'A',10),(2,'A',20),(3,'B',30),(4,'B',40)")
    sql("UPDATE tbl SET value = 99 WHERE category = 'A' AND id = 1")
    sql("DELETE FROM tbl WHERE category = 'B' AND id = 4")
    sql("UPDATE tbl SET value = value + 1000")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "category = 'A'")
    read(t, predicate = "category = 'B'")
    cdf(t, startVersion = 0, endVersion = 4)
    cdf(t, startVersion = 2, endVersion = 2)
    cdf(t, startVersion = 3, endVersion = 3)
    cdf(t, startVersion = 4, endVersion = 4)
    snapshot(t)
  }

  test("cdc_column_mapping") {
    sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
      TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'delta.columnMapping.mode' = 'name')""")
    sql("INSERT INTO tbl VALUES (1,'alice'),(2,'bob')")
    sql("UPDATE tbl SET name = 'ALICE' WHERE id = 1")
    val t = registerTable("tbl")
    read(t)
    read(t, version = 1)
    read(t, predicate = "name = 'ALICE'")
    cdf(t, startVersion = 0)
    snapshot(t)
  }

  test("cdc_deletion_vectors") {
    sql("""CREATE TABLE tbl (id LONG, value INT) USING delta
      TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,100),(2,200),(3,300),(4,400),(5,500)")
    sql("UPDATE tbl SET value = value * 10 WHERE id <= 2")
    sql("DELETE FROM tbl WHERE id = 4")
    val t = registerTable("tbl")
    read(t)
    read(t, version = 1)
    read(t, version = 2)
    read(t, version = 3)
    cdf(t, startVersion = 0, endVersion = 3)
    cdf(t, startVersion = 2, endVersion = 2)
    cdf(t, startVersion = 3, endVersion = 3)
    snapshot(t)
  }

  test("cdc_dv_column_mapping") {
    sql("""CREATE TABLE tbl (id INT, name STRING, score DOUBLE) USING delta
      TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'delta.enableDeletionVectors' = 'true',
        'delta.columnMapping.mode' = 'name')""")
    sql("INSERT INTO tbl VALUES (1,'alice',90.0),(2,'bob',80.0),(3,'charlie',70.0)")
    sql("UPDATE tbl SET score = 95.0 WHERE name = 'alice'")
    sql("DELETE FROM tbl WHERE name = 'charlie'")
    sql("INSERT INTO tbl VALUES (4,'dave',85.0)")
    val t = registerTable("tbl")
    read(t)
    read(t, version = 1)
    read(t, version = 4)
    read(t, predicate = "name = 'alice'")
    cdf(t, startVersion = 0, endVersion = 4)
    cdf(t, startVersion = 1, endVersion = 3)
    snapshot(t)
  }

  test("cdc_data_skipping") {
    sql("""CREATE TABLE tbl (id LONG, category STRING, amount INT) USING delta
      TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'low',10),(2,'low',20)")
    sql("INSERT INTO tbl VALUES (3,'mid',50),(4,'mid',60)")
    sql("INSERT INTO tbl VALUES (5,'high',90),(6,'high',100)")
    sql("UPDATE tbl SET amount = amount + 1 WHERE category = 'mid'")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "amount <= 20")
    read(t, predicate = "amount >= 90")
    read(t, predicate = "category = 'mid'")
    cdf(t, startVersion = 0)
    cdf(t, startVersion = 1, endVersion = 3)
    cdf(t, startVersion = 4, endVersion = 4)
    snapshot(t)
  }

  test("cdc_schema_evolution") {
    sql("""CREATE TABLE tbl (id LONG, value INT) USING delta
      TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true',
        'delta.columnMapping.mode' = 'name', 'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    sql("INSERT INTO tbl VALUES (1,100),(2,200)")
    sql("ALTER TABLE tbl ADD COLUMN (label STRING)")
    sql("INSERT INTO tbl VALUES (3,300,'three'),(4,400,'four')")
    sql("UPDATE tbl SET label = 'one' WHERE id = 1")
    val t = registerTable("tbl")
    read(t)
    read(t, version = 1)
    read(t, version = 3)
    cdf(t, startVersion = 0, endVersion = 4)
    cdf(t, startVersion = 1, endVersion = 1)
    cdf(t, startVersion = 3, endVersion = 4)
    cdf(t, startVersion = 1, endVersion = 3)
    snapshot(t)
  }

  test("cdc_optimize") {
    sql("""CREATE TABLE tbl (id LONG, value INT) USING delta
      TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,100)")
    sql("INSERT INTO tbl VALUES (2,200)")
    sql("INSERT INTO tbl VALUES (3,300)")
    sql("OPTIMIZE tbl")
    sql("INSERT INTO tbl VALUES (4,400)")
    val t = registerTable("tbl")
    read(t)
    cdf(t, startVersion = 0, endVersion = 5)
    cdf(t, startVersion = 3, endVersion = 5)
    cdf(t, startVersion = 4, endVersion = 4)
    snapshot(t)
  }

  test("cdc_predicates") {
    sql("""CREATE TABLE tbl (id INT, status STRING, count INT) USING delta
      TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'active',10),(2,'inactive',20),(3,'active',30)")
    sql("INSERT INTO tbl VALUES (4,'pending',40),(5,'active',50)")
    sql("UPDATE tbl SET status = 'inactive' WHERE count > 40")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "status = 'active'")
    read(t, predicate = "status = 'inactive'")
    read(t, predicate = "count > 30")
    read(t, version = 2)
    cdf(t, startVersion = 0)
    snapshot(t)
  }

  test("cdc_transform") {
    sql("""CREATE TABLE tbl (id LONG, amount DOUBLE) USING delta
      TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,100.0),(2,200.0),(3,300.0)")
    sql("INSERT INTO tbl VALUES (4,400.0),(5,500.0)")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "amount > 250.0")
    read(t, predicate = "amount <= 200.0")
    cdf(t, startVersion = 0)
    snapshot(t)
  }

  test("cdc_multiple_types") {
    sql("""CREATE TABLE tbl (id INT, name STRING, score DOUBLE, is_active BOOLEAN,
      amount DECIMAL(10,2), created DATE, updated_at TIMESTAMP) USING delta
      TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'alice',95.5,true,1000.50,DATE'2024-01-01',TIMESTAMP'2024-01-01 10:00:00')")
    sql("INSERT INTO tbl VALUES (2,'bob',87.3,false,2000.75,DATE'2024-02-15',TIMESTAMP'2024-02-15 14:30:00')")
    sql("UPDATE tbl SET score = 99.0, is_active = false WHERE id = 1")
    val t = registerTable("tbl")
    read(t)
    cdf(t, startVersion = 0, endVersion = 3)
    cdf(t, startVersion = 3, endVersion = 3)
    snapshot(t)
  }

  test("cdc_nested_struct") {
    sql("""CREATE TABLE tbl (id INT, info STRUCT<name: STRING, age: INT>, tags ARRAY<STRING>) USING delta
      TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,struct('alice',30),array('a','b'))")
    sql("INSERT INTO tbl VALUES (2,struct('bob',25),array('c'))")
    sql("DELETE FROM tbl WHERE id = 1")
    val t = registerTable("tbl")
    read(t)
    cdf(t, startVersion = 0, endVersion = 3)
    cdf(t, startVersion = 3, endVersion = 3)
    snapshot(t)
  }

  test("cdc_map_array") {
    sql("""CREATE TABLE tbl (id INT, props MAP<STRING, STRING>, scores ARRAY<INT>) USING delta
      TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,map('k1','v1','k2','v2'),array(10,20,30))")
    sql("INSERT INTO tbl VALUES (2,map('k3','v3'),array(40))")
    sql("UPDATE tbl SET props = map('k1','updated') WHERE id = 1")
    val t = registerTable("tbl")
    read(t)
    cdf(t, startVersion = 0, endVersion = 3)
    cdf(t, startVersion = 3, endVersion = 3)
    snapshot(t)
  }

  test("cdc_generated_columns") {
    sql("""CREATE TABLE tbl (id INT, price DOUBLE, quantity INT,
      total DOUBLE GENERATED ALWAYS AS (price * quantity)) USING delta
      TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')""")
    sql("INSERT INTO tbl(id, price, quantity) VALUES (1,10.0,5)")
    sql("INSERT INTO tbl(id, price, quantity) VALUES (2,20.0,3)")
    sql("UPDATE tbl SET quantity = 10 WHERE id = 1")
    val t = registerTable("tbl")
    read(t)
    cdf(t, startVersion = 0, endVersion = 3)
    cdf(t, startVersion = 3, endVersion = 3)
    snapshot(t)
  }

  test("cdc_not_enabled") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    sql("INSERT INTO tbl VALUES (1),(2)")
    val t = registerTable("tbl")
    read(t)
    cdf(t, startVersion = 0)
    snapshot(t)
  }

  test("cdc_many_versions") {
    sql("""CREATE TABLE tbl (id INT, value INT) USING delta
      TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')""")
    for (i <- 1 to 5) sql(s"INSERT INTO tbl VALUES ($i, ${i * 10})")
    sql("UPDATE tbl SET value = value + 1 WHERE id <= 2")
    sql("DELETE FROM tbl WHERE id = 3")
    for (i <- 6 to 8) sql(s"INSERT INTO tbl VALUES ($i, ${i * 10})")
    sql("UPDATE tbl SET value = value * 2")
    sql("DELETE FROM tbl WHERE id = 1")
    val t = registerTable("tbl")
    read(t)
    cdf(t, startVersion = 1)
    cdf(t, startVersion = 1, endVersion = 6)
    cdf(t, startVersion = 7)
    snapshot(t)
  }

}
