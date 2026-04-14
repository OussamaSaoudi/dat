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

/**
 * Merge operation workloads covering all MERGE INTO scenarios.
 *
 * Categories:
 *   - Basic clause types (insert, update, delete, combinations)
 *   - Conditional clauses
 *   - Data types (boolean, decimal, timestamp, string keys)
 *   - Complex types (array, map, nested struct)
 *   - Partitioned tables
 *   - Deletion vectors (DV-enabled tables with prior deletes)
 *   - Low-shuffle merge variants
 *   - Schema evolution (add columns, nested fields, type widening, column mapping)
 *   - Struct evolution (deep nesting, null handling, arrays of structs, maps of structs)
 *   - Edge cases (empty source/target, null join keys, self-merge, duplicate source, aliases)
 *   - Error cases (ambiguous column, type mismatch, no match condition)
 *   - NOT MATCHED BY SOURCE clauses
 *   - CDF-enabled merge
 *
 */
class MergeSuite extends WorkloadTestSuite("merge") {

  // Basic clause types

  test("mergeBasicInsert") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,'x'),(4,'d'),(5,'e') AS s(id, value)) s
      ON t.id = s.id
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "id >= 4", name = "filter_new_rows")
    snapshot(t)
  }

  test("mergeBasicUpdate") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b')")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,'x') AS s(id, value)) s
      ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET value = s.value""")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "id = 2", name = "filter_updated")
    snapshot(t)
  }

  test("mergeBasicDelete") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,'x') AS s(id, value)) s
      ON t.id = s.id
      WHEN MATCHED THEN DELETE""")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("mergeInsertUpdate") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b')")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,'x'),(3,'c') AS s(id, value)) s
      ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET value = s.value
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "id = 2", name = "filter_id_2")
    snapshot(t)
  }

  test("mergeInsertDelete") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,'x'),(4,'d') AS s(id, value)) s
      ON t.id = s.id
      WHEN MATCHED THEN DELETE
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("mergeInsertUpdateDelete") {
    sql("""CREATE TABLE tbl (id INT, val INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,10),(2,20),(3,30)")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,40),(4,200) AS s(id, val)) s
      ON t.id = s.id
      WHEN MATCHED AND t.val > 15 THEN UPDATE SET val = s.val
      WHEN MATCHED THEN DELETE
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("mergeUpdateDelete") {
    sql("""CREATE TABLE tbl (id INT, val INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,10),(2,20),(3,30)")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (1,100),(2,200),(3,300) AS s(id, val)) s
      ON t.id = s.id
      WHEN MATCHED AND t.val > 15 THEN UPDATE SET val = s.val
      WHEN MATCHED THEN DELETE""")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("mergeMultipleMatched") {
    sql("""CREATE TABLE tbl (id INT, score INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,5),(2,15),(3,35)")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (1,50),(2,50),(3,50) AS s(id, score)) s
      ON t.id = s.id
      WHEN MATCHED AND t.score > 30 THEN DELETE
      WHEN MATCHED AND t.score > 10 THEN UPDATE SET score = s.score""")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  // Conditional clauses

  test("mergeConditionalInsert") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b')")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,'x'),(3,'c'),(4,'d') AS s(id, value)) s
      ON t.id = s.id
      WHEN NOT MATCHED AND s.id > 3 THEN INSERT *""")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("mergeConditionalUpdate") {
    sql("""CREATE TABLE tbl (id INT, status STRING, score INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'low',5),(2,'low',15),(3,'low',25)")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,'high',50),(3,'high',50) AS s(id, status, score)) s
      ON t.id = s.id
      WHEN MATCHED AND t.score >= 10 THEN UPDATE SET status = s.status, score = s.score""")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "status = 'high'", name = "filter_high")
    snapshot(t)
  }

  // Star syntax

  test("mergeStarInsert") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b')")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (3,'c'),(4,'d') AS s(id, value)) s
      ON t.id = s.id
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("mergeStarUpdate") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b')")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,'x') AS s(id, value)) s
      ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET *""")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  // Source variations

  test("mergeSourceSubquery") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b')")
    sql("""MERGE INTO tbl t
      USING (SELECT id, CONCAT(value, '_new') AS value FROM VALUES (2,'x'),(3,'c') AS s(id, value)) s
      ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET value = s.value
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("mergeSourceAggregation") {
    sql("""CREATE TABLE tbl (id INT, total BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,100),(2,200)")
    sql("""MERGE INTO tbl t
      USING (SELECT id, SUM(amount) AS total FROM VALUES (2,30),(2,50),(3,150) AS s(id, amount) GROUP BY id) s
      ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET total = s.total
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  // Data types

  test("mergeBooleanValues") {
    sql("""CREATE TABLE tbl (id INT, active BOOLEAN) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,true),(2,false),(3,true)")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,true),(4,false) AS s(id, active)) s
      ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET active = s.active
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "active = true", name = "filter_active")
    snapshot(t)
  }

  test("mergeDecimalValues") {
    sql("""CREATE TABLE tbl (id INT, price DECIMAL(10,2)) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,10.50),(2,20.75)")
    sql("INSERT INTO tbl VALUES (3,30.00)")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,99.99),(4,45.00) AS s(id, price)) s
      ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET price = s.price
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "price > 30.00", name = "filter_price")
    snapshot(t)
  }

  test("mergeTimestampValues") {
    sql("""CREATE TABLE tbl (id INT, ts TIMESTAMP) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, TIMESTAMP '2024-01-01 10:00:00')")
    sql("INSERT INTO tbl VALUES (2, TIMESTAMP '2024-01-02 10:00:00')")
    sql("""MERGE INTO tbl t USING (
      SELECT * FROM VALUES (2, TIMESTAMP '2024-06-01 12:00:00'),(3, TIMESTAMP '2024-07-01 14:00:00') AS s(id, ts)
    ) s ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET ts = s.ts
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("mergeStringKeys") {
    sql("""CREATE TABLE tbl (name STRING, amount INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES ('alice',100),('bob',200)")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES ('bob',999),('carol',300) AS s(name, amount)) s
      ON t.name = s.name
      WHEN MATCHED THEN UPDATE SET amount = s.amount
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "name = 'bob'", name = "filter_bob")
    snapshot(t)
  }

  // NULL handling

  test("mergeNullHandling") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a')")
    sql("INSERT INTO tbl VALUES (2, NULL)")
    sql("INSERT INTO tbl VALUES (3,'c')")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,'updated'),(4,CAST(NULL AS STRING)) AS s(id, value)) s
      ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET value = s.value
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "value IS NOT NULL", name = "filter_not_null")
    snapshot(t)
  }

  // Complex types

  test("mergeWithArrayCol") {
    sql("""CREATE TABLE tbl (id INT, tags ARRAY<STRING>) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, array('a','b'))")
    sql("INSERT INTO tbl VALUES (2, array('c'))")
    sql("""MERGE INTO tbl t USING (
      SELECT * FROM VALUES (2, array('c','d','e')),(3, array('f')) AS s(id, tags)
    ) s ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET tags = s.tags
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("mergeWithMapCol") {
    sql("""CREATE TABLE tbl (id INT, props MAP<STRING, STRING>) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, map('k1','v1'))")
    sql("INSERT INTO tbl VALUES (2, map('k2','v2'))")
    sql("""MERGE INTO tbl t USING (
      SELECT * FROM VALUES (2, map('k2','updated')),(3, map('k3','v3')) AS s(id, props)
    ) s ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET props = s.props
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("mergeWithNestedStruct") {
    sql("""CREATE TABLE tbl (id INT, info STRUCT<name: STRING, age: INT>) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, named_struct('name','alice','age',30))")
    sql("INSERT INTO tbl VALUES (2, named_struct('name','bob','age',25))")
    sql("""MERGE INTO tbl t USING (
      SELECT * FROM VALUES
        (2, named_struct('name','bob','age',26)),
        (3, named_struct('name','carol','age',35))
      AS s(id, info)
    ) s ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET info = s.info
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  // Partitioned tables

  test("mergePartitionedBasic") {
    sql("""CREATE TABLE tbl (id INT, region STRING, amount INT) USING delta
      PARTITIONED BY (region)
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'east',100),(2,'west',200),(3,'east',300)")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,'west',999),(4,'north',400) AS s(id, region, amount)) s
      ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET amount = s.amount
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "region = 'east'", name = "filter_east")
    snapshot(t)
  }

  test("mergePartitionedCrossPartition") {
    sql("""CREATE TABLE tbl (id INT, region STRING, amount INT) USING delta
      PARTITIONED BY (region)
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'east',100),(2,'east',200),(3,'west',300)")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (1,'west',150),(2,'west',250) AS s(id, region, amount)) s
      ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET region = s.region, amount = s.amount""")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "region = 'west'", name = "filter_west")
    snapshot(t)
  }

  test("mergePartitionedMultiCol") {
    sql("""CREATE TABLE tbl (id INT, country STRING, year INT, amount INT) USING delta
      PARTITIONED BY (country, year)
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'US',2024,100),(2,'UK',2024,200),(3,'US',2023,300)")
    sql("""MERGE INTO tbl t USING (
      SELECT * FROM VALUES (1,'US',2024,999),(4,'DE',2024,400) AS s(id, country, year, amount)
    ) s ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET amount = s.amount
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "country = 'US' AND year = 2024", name = "filter_us_2024")
    snapshot(t)
  }

  // Deletion vectors (DV-enabled tables with prior deletes)

  test("mergeDvBasicInsertUpdate") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
    sql("DELETE FROM tbl WHERE id = 2")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (1,'x'),(2,'x'),(4,'d') AS s(id, value)) s
      ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET value = s.value
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerTable("tbl")
    read(t, name = "readAll")
    snapshot(t)
  }

  test("mergeDvConditionalUpdate") {
    sql("""CREATE TABLE tbl (id INT, amount INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,100),(2,200),(3,300),(4,400)")
    sql("DELETE FROM tbl WHERE id = 4")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,250),(3,350),(5,500) AS s(id, amount)) s
      ON t.id = s.id
      WHEN MATCHED AND s.amount > 200 THEN UPDATE SET amount = s.amount
      WHEN NOT MATCHED AND s.amount >= 500 THEN INSERT *""")
    val t = registerTable("tbl")
    read(t, name = "readAll")
    snapshot(t)
  }

  test("mergeDvDeleteClause") {
    sql("""CREATE TABLE tbl (id INT, value STRING, amount INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a',10),(2,'b',20),(3,'c',40),(4,'d',50),(5,'e',60)")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,99),(3,99),(4,99) AS s(id, amount)) s
      ON t.id = s.id
      WHEN MATCHED AND t.amount > 30 THEN DELETE
      WHEN MATCHED THEN UPDATE SET amount = s.amount""")
    val t = registerTable("tbl")
    read(t, name = "readAll")
    snapshot(t)
  }

  test("mergeDvLargeTable") {
    sql("""CREATE TABLE tbl (id INT, name STRING, score INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id, CONCAT('name_', CAST(id AS STRING)), id * 100 FROM range(100)")
    sql("DELETE FROM tbl WHERE id <= 30 AND id % 3 = 0")
    sql("""MERGE INTO tbl t USING (
      SELECT id, CONCAT('updated_', CAST(id AS STRING)) AS name, id * 1000 AS score
      FROM range(50, 60)
    ) s ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET name = s.name, score = s.score
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerTable("tbl")
    read(t, name = "readAll")
    read(t, predicate = "score > 5000", name = "filterHighScore")
    snapshot(t)
  }

  test("mergeDvMultipleMatched") {
    sql("""CREATE TABLE tbl (id INT, value STRING, amount INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a',10),(2,'b',20),(3,'c',30),(4,'d',40),(5,'e',50)")
    sql("DELETE FROM tbl WHERE id = 1")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,99),(3,99),(4,99) AS s(id, amount)) s
      ON t.id = s.id
      WHEN MATCHED AND t.amount > 100 THEN DELETE
      WHEN MATCHED AND t.amount > 50 THEN UPDATE SET value = 'also', amount = s.amount
      WHEN MATCHED THEN UPDATE SET value = 'updated', amount = s.amount""")
    val t = registerTable("tbl")
    read(t, name = "readAll")
    snapshot(t)
  }

  test("mergeDvMultipleMerges") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
    sql("DELETE FROM tbl WHERE id = 2")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (1,'x'),(2,'new'),(4,'d') AS s(id, value)) s
      ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET value = s.value
      WHEN NOT MATCHED THEN INSERT *""")
    sql("DELETE FROM tbl WHERE id = 1")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (1,'back'),(5,'e') AS s(id, value)) s
      ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET value = s.value
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerTable("tbl")
    read(t, name = "readAll")
    snapshot(t)
  }

  test("mergeDvNullHandling") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,NULL),(3,'c')")
    sql("DELETE FROM tbl WHERE id = 1")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,'updated'),(4,CAST(NULL AS STRING)) AS s(id, value)) s
      ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET value = s.value
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerTable("tbl")
    read(t, name = "readAll")
    snapshot(t)
  }

  test("mergeDvPartitioned") {
    sql("""CREATE TABLE tbl (region STRING, id INT, value STRING) USING delta
      PARTITIONED BY (region)
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES ('us',1,'a'),('us',2,'b'),('eu',3,'c'),('eu',4,'d')")
    sql("DELETE FROM tbl WHERE region = 'us' AND id = 1")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES ('us',2,'x'),('eu',5,'e') AS s(region, id, value)) s
      ON t.id = s.id AND t.region = s.region
      WHEN MATCHED THEN UPDATE SET value = s.value
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerTable("tbl")
    read(t, name = "readAll")
    snapshot(t)
  }

  test("mergeDvSchemaEvolution") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
    sql("DELETE FROM tbl WHERE id = 3")
    sql("SET spark.delta.schema.autoMerge.enabled = true")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,'x',10),(4,'d',20) AS s(id, value, extra)) s
      ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *""")
    sql("SET spark.delta.schema.autoMerge.enabled = false")
    val t = registerTable("tbl")
    read(t, name = "readAll")
    snapshot(t)
  }

  test("mergeDvStarSyntax") {
    sql("""CREATE TABLE tbl (id INT, name STRING, score INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a',10),(2,'b',20),(3,'c',30)")
    sql("DELETE FROM tbl WHERE id = 2")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (1,'x',99),(2,'new',50),(4,'d',40) AS s(id, name, score)) s
      ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerTable("tbl")
    read(t, name = "readAll")
    snapshot(t)
  }

  // Low-shuffle merge variants

  test("mergeLowShuffleBasic") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,'x'),(4,'d') AS s(id, value)) s
      ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET value = s.value
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "id = 2", name = "filter_updated")
    snapshot(t)
  }

  test("mergeLowShuffleConditional") {
    sql("""CREATE TABLE tbl (id INT, value STRING, amount INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a',10),(2,'b',20),(3,'c',30),(4,'d',40)")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,'x',25),(3,'y',35),(5,'e',100) AS s(id, value, amount)) s
      ON t.id = s.id
      WHEN MATCHED AND s.amount > t.amount THEN UPDATE SET value = s.value, amount = s.amount
      WHEN NOT MATCHED AND s.amount > 50 THEN INSERT *""")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "id = 5", name = "filter_conditional_insert")
    snapshot(t)
  }

  test("mergeLowShuffleDecimal") {
    sql("""CREATE TABLE tbl (id INT, price DECIMAL(10,2)) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,10.50),(2,20.75),(3,30.00)")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,99.99),(4,45.00) AS s(id, price)) s
      ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET price = s.price
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "id = 2", name = "filter_updated")
    snapshot(t)
  }

  test("mergeLowShuffleLargeTable") {
    sql("""CREATE TABLE tbl (id BIGINT, value INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id, CAST(id * 10 AS INT) FROM range(200)")
    sql("""MERGE INTO tbl t USING (
      SELECT id, 9999 AS value FROM range(100, 110)
    ) s ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET value = s.value
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "value = 9999", name = "filter_updated")
    snapshot(t)
  }

  test("mergeLowShuffleMultiClause") {
    sql("""CREATE TABLE tbl (id INT, value STRING, amount INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a',10),(2,'b',20),(3,'c',0),(4,'d',40)")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,'x',25),(3,'y',35),(5,'e',50) AS s(id, value, amount)) s
      ON t.id = s.id
      WHEN MATCHED AND t.amount = 0 THEN DELETE
      WHEN MATCHED THEN UPDATE SET value = s.value, amount = s.amount
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "id = 3", name = "filter_deleted")
    snapshot(t)
  }

  test("mergeLowShuffleMultiMerge") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b')")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,'x'),(3,'c') AS s(id, value)) s
      ON t.id = s.id WHEN MATCHED THEN UPDATE SET value = s.value WHEN NOT MATCHED THEN INSERT *""")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (3,'y'),(4,'d') AS s(id, value)) s
      ON t.id = s.id WHEN MATCHED THEN UPDATE SET value = s.value WHEN NOT MATCHED THEN INSERT *""")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (4,'z'),(5,'e') AS s(id, value)) s
      ON t.id = s.id WHEN MATCHED THEN UPDATE SET value = s.value WHEN NOT MATCHED THEN INSERT *""")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "id = 5", name = "filter_last_merge")
    snapshot(t)
  }

  test("mergeLowShuffleNested") {
    sql("""CREATE TABLE tbl (id INT, info STRUCT<name: STRING, age: INT>) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, named_struct('name','alice','age',30)),(2, named_struct('name','bob','age',25))")
    sql("""MERGE INTO tbl t USING (
      SELECT * FROM VALUES (2, named_struct('name','bob','age',26)),(3, named_struct('name','carol','age',35)) AS s(id, info)
    ) s ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET info = s.info
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("mergeLowShufflePartitioned") {
    sql("""CREATE TABLE tbl (id INT, part STRING, amount INT) USING delta
      PARTITIONED BY (part)
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'x',10),(2,'y',20),(3,'x',30)")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,'y',99),(4,'z',40) AS s(id, part, amount)) s
      ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET amount = s.amount
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "part = 'y'", name = "filter_part_y")
    snapshot(t)
  }

  test("mergeLowShuffleStar") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b')")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,'x'),(3,'c') AS s(id, value)) s
      ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("mergeLowShuffleTimestamp") {
    sql("""CREATE TABLE tbl (id INT, ts TIMESTAMP, label STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, TIMESTAMP '2024-01-01 10:00:00', 'old1'),(2, TIMESTAMP '2024-01-02 10:00:00', 'old2')")
    sql("""MERGE INTO tbl t USING (
      SELECT * FROM VALUES (2, TIMESTAMP '2024-06-01 12:00:00', 'upd'),(3, TIMESTAMP '2024-07-01 14:00:00', 'new') AS s(id, ts, label)
    ) s ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET ts = s.ts, label = s.label
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "id = 3", name = "filter_new")
    snapshot(t)
  }

  // Edge cases

  test("mergeEdgeAllMatched") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (1,'z'),(2,'z'),(3,'z') AS s(id, value)) s
      ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET value = s.value""")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "value = 'z'", name = "filter_updated")
    snapshot(t)
  }

  test("mergeEdgeNoMatched") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b')")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (10,'x'),(20,'y') AS s(id, value)) s
      ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET value = s.value
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "id >= 10", name = "filter_new")
    snapshot(t)
  }

  test("mergeEdgeEmptySource") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (999,'x') AS s(id, value) WHERE false) s
      ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET value = s.value
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("mergeEdgeEmptyTarget") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (999,'placeholder')")
    sql("DELETE FROM tbl WHERE true")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (1,'a'),(2,'b') AS s(id, value)) s
      ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET value = s.value
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("mergeEdgeNullJoinKey") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(NULL,'c')")
    sql("""MERGE INTO tbl t USING (
      SELECT * FROM VALUES (2,'updated'),(4,'new'),(CAST(NULL AS INT),'null_src') AS s(id, value)
    ) s ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET value = s.value
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "id IS NULL", name = "filter_nulls")
    snapshot(t)
  }

  test("mergeEdgeSelfMerge") {
    sql("""CREATE TABLE tbl (id INT, value STRING, amount INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a',10),(2,'b',20),(3,'c',30)")
    sql("""MERGE INTO tbl t USING tbl s
      ON t.id = s.id AND t.amount < 25
      WHEN MATCHED THEN UPDATE SET amount = s.amount * 2""")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "amount = 20", name = "filter_doubled")
    snapshot(t)
  }

  test("mergeEdgeSourceAlias") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
    sql("""MERGE INTO tbl t USING (
      SELECT id AS src_id, value AS src_value FROM VALUES (2,'x') AS s(id, value)
    ) s ON t.id = s.src_id
      WHEN MATCHED THEN UPDATE SET value = s.src_value""")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "id = 2", name = "filter_updated")
    snapshot(t)
  }

  test("mergeEdgeMultiJoin") {
    sql("""CREATE TABLE tbl (id INT, key STRING, amount INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'x',10),(2,'y',20),(3,'z',30)")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,'y',999),(4,'w',400) AS s(id, key, amount)) s
      ON t.id = s.id AND t.key = s.key
      WHEN MATCHED THEN UPDATE SET amount = s.amount
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "id = 2 AND amount = 999", name = "filter_updated")
    snapshot(t)
  }

  test("mergeEdgeLargePayload") {
    val cols = (1 to 20).map(i => s"col_$i INT").mkString(", ")
    sql(s"""CREATE TABLE tbl (id INT, $cols) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    val vals1 = (1 to 20).map(i => i * 10).mkString(",")
    val vals2 = (1 to 20).map(i => i * 20).mkString(",")
    sql(s"INSERT INTO tbl VALUES (1,$vals1),(2,$vals2)")
    val srcVals2 = (1 to 20).map(i => i * 99).mkString(",")
    val srcVals3 = (1 to 20).map(i => i * 30).mkString(",")
    sql(s"""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,$srcVals2),(3,$srcVals3) AS s(id,${(1 to 20).map(i => s"col_$i").mkString(",")})) s
      ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET ${(1 to 20).map(i => s"col_$i = s.col_$i").mkString(",")}
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("mergeEdgeDuplicateSourceKeys") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b')")
    // Merge with duplicate source keys should fail; table stays at version 0
    try {
      sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (1,'x'),(1,'y') AS s(id, value)) s
        ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET value = s.value""")
    } catch { case _: Exception => /* expected failure */ }
    val t = registerTable("tbl")
    read(t, name = "readAll")
    snapshot(t)
  }

  // Error cases (table unchanged after failed merge)

  test("mergeErrAmbiguousColumn") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b')")
    try {
      sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,'x') AS s(id, value)) s
        ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET value = value""")
    } catch { case _: Exception => /* expected: ambiguous column reference */ }
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("mergeErrDuplicateSource") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b')")
    try {
      sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (1,'x'),(1,'y') AS s(id, value)) s
        ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET value = s.value""")
    } catch { case _: Exception => /* expected: duplicate source */ }
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("mergeErrNoMatchCondition") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b')")
    // This should fail at parse time (no ON clause)
    try {
      sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (1,'x') AS s(id, value)) s
        WHEN MATCHED THEN UPDATE SET value = s.value""")
    } catch { case _: Exception => /* expected: parse error */ }
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("mergeErrTypeMismatch") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b')")
    try {
      sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (1, ARRAY(1,2,3)) AS s(id, value)) s
        ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET value = s.value""")
    } catch { case _: Exception => /* expected: type mismatch */ }
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  // Schema evolution - basic column addition

  test("mergeSchemaEvoAddCol") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b')")
    sql("SET spark.delta.schema.autoMerge.enabled = true")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,'x',10),(3,'c',20) AS s(id, value, extra)) s
      ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *""")
    sql("SET spark.delta.schema.autoMerge.enabled = false")
    val t = registerTable("tbl")
    read(t, name = "readAll")
    read(t, predicate = "extra IS NOT NULL", name = "readNewCol")
    snapshot(t)
  }

  test("mergeSchemaEvoAddMultiCols") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b')")
    sql("SET spark.delta.schema.autoMerge.enabled = true")
    sql("""MERGE INTO tbl t USING (
      SELECT * FROM VALUES (2,'x',10,1.5,true),(3,'c',20,2.5,false) AS s(id, value, extra1, extra2, extra3)
    ) s ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *""")
    sql("SET spark.delta.schema.autoMerge.enabled = false")
    val t = registerTable("tbl")
    read(t, name = "readAll")
    read(t, predicate = "extra1 IS NOT NULL", name = "readExtra1NotNull")
    snapshot(t)
  }

  test("mergeSchemaEvoInsertNewCol") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b')")
    sql("SET spark.delta.schema.autoMerge.enabled = true")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (3,'c',100),(4,'d',200) AS s(id, value, score)) s
      ON t.id = s.id
      WHEN NOT MATCHED THEN INSERT *""")
    sql("SET spark.delta.schema.autoMerge.enabled = false")
    val t = registerTable("tbl")
    read(t, name = "readAll")
    read(t, predicate = "score IS NOT NULL", name = "readWithScore")
    snapshot(t)
  }

  test("mergeSchemaEvoInsertMultipleNewCols") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b')")
    sql("SET spark.delta.schema.autoMerge.enabled = true")
    sql("""MERGE INTO tbl t USING (
      SELECT * FROM VALUES (3,'c',10,1.5,'x',true),(4,'d',20,2.5,'y',false) AS s(id, value, col1, col2, col3, col4)
    ) s ON t.id = s.id
      WHEN NOT MATCHED THEN INSERT *""")
    sql("SET spark.delta.schema.autoMerge.enabled = false")
    val t = registerTable("tbl")
    read(t, name = "readAll")
    read(t, predicate = "col1 IS NOT NULL", name = "readNewCols")
    snapshot(t)
  }

  test("mergeSchemaEvoInsertWithDefault") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b')")
    sql("SET spark.delta.schema.autoMerge.enabled = true")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (3,'c',100) AS s(id, value, newcol)) s
      ON t.id = s.id
      WHEN NOT MATCHED THEN INSERT *""")
    sql("SET spark.delta.schema.autoMerge.enabled = false")
    val t = registerTable("tbl")
    read(t, name = "readAll")
    read(t, predicate = "newcol IS NULL", name = "readNullNewCol")
    snapshot(t)
  }

  test("mergeSchemaEvoUpdateNewCol") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b')")
    sql("SET spark.delta.schema.autoMerge.enabled = true")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,'x',5) AS s(id, value, rating)) s
      ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET *""")
    sql("SET spark.delta.schema.autoMerge.enabled = false")
    val t = registerTable("tbl")
    read(t, name = "readAll")
    read(t, predicate = "rating IS NOT NULL", name = "readRating")
    snapshot(t)
  }

  test("mergeSchemaEvoUpdateStarNewCol") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b')")
    sql("SET spark.delta.schema.autoMerge.enabled = true")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,'x','extra') AS s(id, value, bonus)) s
      ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET *""")
    sql("SET spark.delta.schema.autoMerge.enabled = false")
    val t = registerTable("tbl")
    read(t, name = "readAll")
    read(t, predicate = "bonus IS NOT NULL", name = "readBonusNotNull")
    snapshot(t)
  }

  test("mergeSchemaEvoInsertUpdateNewCol") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b')")
    sql("SET spark.delta.schema.autoMerge.enabled = true")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,'x',5),(3,'c',10) AS s(id, value, priority)) s
      ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *""")
    sql("SET spark.delta.schema.autoMerge.enabled = false")
    val t = registerTable("tbl")
    read(t, name = "readAll")
    read(t, predicate = "priority IS NOT NULL", name = "readPriority")
    snapshot(t)
  }

  test("mergeSchemaEvoInsertUpdateDiffCols") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b')")
    sql("SET spark.delta.schema.autoMerge.enabled = true")
    sql("""MERGE INTO tbl t USING (
      SELECT * FROM VALUES (2,'x',10,CAST(NULL AS STRING)),(3,'c',CAST(NULL AS INT),'new') AS s(id, value, colA, colB)
    ) s ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *""")
    sql("SET spark.delta.schema.autoMerge.enabled = false")
    val t = registerTable("tbl")
    read(t, name = "readAll")
    read(t, predicate = "colA IS NOT NULL", name = "readColA")
    snapshot(t)
  }

  test("mergeSchemaEvoUpdateMultipleClauses") {
    sql("""CREATE TABLE tbl (id INT, value STRING, amount INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a',10),(2,'b',25),(3,'c',30)")
    sql("SET spark.delta.schema.autoMerge.enabled = true")
    sql("""MERGE INTO tbl t USING (
      SELECT * FROM VALUES (1,'x',15,'low'),(2,'y',30,'high'),(3,'z',35,'high'),(4,'w',50,'new') AS s(id, value, amount, flag)
    ) s ON t.id = s.id
      WHEN MATCHED AND t.amount > 20 THEN UPDATE SET *
      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *""")
    sql("SET spark.delta.schema.autoMerge.enabled = false")
    val t = registerTable("tbl")
    read(t, name = "readAll")
    read(t, predicate = "flag IS NOT NULL", name = "readFlag")
    snapshot(t)
  }

  // Schema evolution - nested structs

  test("mergeSchemaEvoAddNestedField") {
    sql("""CREATE TABLE tbl (id INT, info STRUCT<name: STRING, age: INT>) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, named_struct('name','alice','age',30))")
    sql("INSERT INTO tbl VALUES (2, named_struct('name','bob','age',25))")
    sql("SET spark.delta.schema.autoMerge.enabled = true")
    sql("""MERGE INTO tbl t USING (
      SELECT * FROM VALUES (2, named_struct('name','bob','age',26,'email','bob@x.com')),(3, named_struct('name','carol','age',35,'email','carol@x.com'))
      AS s(id, info)
    ) s ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET info = s.info
      WHEN NOT MATCHED THEN INSERT *""")
    sql("SET spark.delta.schema.autoMerge.enabled = false")
    val t = registerTable("tbl")
    read(t, name = "readAll")
    snapshot(t)
  }

  test("mergeSchemaEvoInsertNestedNewField") {
    sql("""CREATE TABLE tbl (id INT, info STRUCT<name: STRING>) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, named_struct('name','alice'))")
    sql("SET spark.delta.schema.autoMerge.enabled = true")
    sql("""MERGE INTO tbl t USING (
      SELECT * FROM VALUES (2, named_struct('name','bob','city','NYC')) AS s(id, info)
    ) s ON t.id = s.id
      WHEN NOT MATCHED THEN INSERT *""")
    sql("SET spark.delta.schema.autoMerge.enabled = false")
    val t = registerTable("tbl")
    read(t, name = "readAll")
    snapshot(t)
  }

  test("mergeSchemaEvoUpdateNestedField") {
    sql("""CREATE TABLE tbl (id INT, details STRUCT<city: STRING, zip: STRING>) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, named_struct('city','NYC','zip','10001'))")
    sql("INSERT INTO tbl VALUES (2, named_struct('city','LA','zip','90001'))")
    sql("SET spark.delta.schema.autoMerge.enabled = true")
    sql("""MERGE INTO tbl t USING (
      SELECT * FROM VALUES (2, named_struct('city','LA','zip','90001','country','US')) AS s(id, details)
    ) s ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET details = s.details""")
    sql("SET spark.delta.schema.autoMerge.enabled = false")
    val t = registerTable("tbl")
    read(t, name = "readAll")
    snapshot(t)
  }

  test("mergeSchemaEvoNestedStructAdd") {
    sql("""CREATE TABLE tbl (id INT, outer_col STRUCT<inner: STRUCT<x: INT>>) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, named_struct('inner', named_struct('x', 10)))")
    sql("SET spark.delta.schema.autoMerge.enabled = true")
    sql("""MERGE INTO tbl t USING (
      SELECT * FROM VALUES (2, named_struct('inner', named_struct('x', 20, 'y', 30))) AS s(id, outer_col)
    ) s ON t.id = s.id
      WHEN NOT MATCHED THEN INSERT *""")
    sql("SET spark.delta.schema.autoMerge.enabled = false")
    val t = registerTable("tbl")
    read(t, name = "readAll")
    snapshot(t)
  }

  // Schema evolution - complex types (arrays, maps)

  test("mergeSchemaEvoAddArrayElement") {
    sql("""CREATE TABLE tbl (id INT, numbers ARRAY<INT>) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, array(1,2,3)),(2, array(4,5))")
    sql("SET spark.delta.schema.autoMerge.enabled = true")
    sql("""MERGE INTO tbl t USING (
      SELECT * FROM VALUES (2, array(4,5,6), 'active'),(3, array(7), 'new') AS s(id, numbers, status)
    ) s ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *""")
    sql("SET spark.delta.schema.autoMerge.enabled = false")
    val t = registerTable("tbl")
    read(t, name = "readAll")
    read(t, predicate = "status IS NOT NULL", name = "readNewStatus")
    snapshot(t)
  }

  test("mergeSchemaEvoAddMapEntry") {
    sql("""CREATE TABLE tbl (id INT, props MAP<STRING, STRING>) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, map('k1','v1')),(2, map('k2','v2'))")
    sql("SET spark.delta.schema.autoMerge.enabled = true")
    sql("""MERGE INTO tbl t USING (
      SELECT * FROM VALUES (2, map('k2','updated'), 10),(3, map('k3','v3'), 20) AS s(id, props, extra)
    ) s ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *""")
    sql("SET spark.delta.schema.autoMerge.enabled = false")
    val t = registerTable("tbl")
    read(t, name = "readAll")
    snapshot(t)
  }

  test("mergeSchemaEvoArrayStructEvolution") {
    sql("""CREATE TABLE tbl (id INT, items ARRAY<STRUCT<name: STRING, qty: INT>>) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, array(named_struct('name','item1','qty',10)))")
    sql("SET spark.delta.schema.autoMerge.enabled = true")
    sql("""MERGE INTO tbl t USING (
      SELECT * FROM VALUES (2, array(named_struct('name','item2','qty',20,'price',CAST(1.99 AS DECIMAL(3,2))))) AS s(id, items)
    ) s ON t.id = s.id
      WHEN NOT MATCHED THEN INSERT *""")
    sql("SET spark.delta.schema.autoMerge.enabled = false")
    val t = registerTable("tbl")
    read(t, name = "readAll")
    snapshot(t)
  }

  test("mergeSchemaEvoMapValueType") {
    sql("""CREATE TABLE tbl (id INT, labels MAP<STRING, STRING>) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, map('env','prod')),(2, map('env','dev'))")
    sql("SET spark.delta.schema.autoMerge.enabled = true")
    sql("""MERGE INTO tbl t USING (
      SELECT * FROM VALUES (2, map('env','staging'), 2),(3, map('env','test'), 3) AS s(id, labels, version)
    ) s ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *""")
    sql("SET spark.delta.schema.autoMerge.enabled = false")
    val t = registerTable("tbl")
    read(t, name = "readAll")
    read(t, predicate = "version IS NOT NULL", name = "readVersion")
    snapshot(t)
  }

  // Schema evolution - struct field operations

  test("mergeSchemaEvoStructAddField") {
    sql("""CREATE TABLE tbl (id INT, metadata STRUCT<key: STRING, val: INT>) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, named_struct('key','k1','val',10))")
    sql("INSERT INTO tbl VALUES (2, named_struct('key','k2','val',20))")
    sql("SET spark.delta.schema.autoMerge.enabled = true")
    sql("""MERGE INTO tbl t USING (
      SELECT * FROM VALUES
        (2, named_struct('key','k2','val',25,'source','api')),
        (3, named_struct('key','k3','val',30,'source','web'))
      AS s(id, metadata)
    ) s ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET metadata = s.metadata
      WHEN NOT MATCHED THEN INSERT *""")
    sql("SET spark.delta.schema.autoMerge.enabled = false")
    val t = registerTable("tbl")
    read(t, name = "readAll")
    snapshot(t)
  }

  test("mergeSchemaEvoStructRemoveField") {
    sql("""CREATE TABLE tbl (id INT, info STRUCT<name: STRING, age: INT, city: STRING>) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, named_struct('name','alice','age',30,'city','NYC'))")
    sql("SET spark.delta.schema.autoMerge.enabled = true")
    sql("""MERGE INTO tbl t USING (
      SELECT * FROM VALUES (2, named_struct('name','bob','age',25)) AS s(id, info)
    ) s ON t.id = s.id
      WHEN NOT MATCHED THEN INSERT *""")
    sql("SET spark.delta.schema.autoMerge.enabled = false")
    val t = registerTable("tbl")
    read(t, name = "readAll")
    snapshot(t)
  }

  test("mergeSchemaEvoStructReorderFields") {
    sql("""CREATE TABLE tbl (id INT, info STRUCT<first: STRING, last: STRING>) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, named_struct('first','Alice','last','Smith'))")
    sql("SET spark.delta.schema.autoMerge.enabled = true")
    sql("""MERGE INTO tbl t USING (
      SELECT * FROM VALUES (2, named_struct('first','Bob','last','Jones','middle','M')) AS s(id, info)
    ) s ON t.id = s.id
      WHEN NOT MATCHED THEN INSERT *""")
    sql("SET spark.delta.schema.autoMerge.enabled = false")
    val t = registerTable("tbl")
    read(t, name = "readAll")
    snapshot(t)
  }

  test("mergeSchemaEvoStructWithArray") {
    sql("""CREATE TABLE tbl (id INT, data STRUCT<tags: ARRAY<STRING>, count: INT>) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, named_struct('tags', array('a','b'), 'count', 2))")
    sql("SET spark.delta.schema.autoMerge.enabled = true")
    sql("""MERGE INTO tbl t USING (
      SELECT * FROM VALUES (2, named_struct('tags', array('c'), 'count', 1, 'active', true)) AS s(id, data)
    ) s ON t.id = s.id
      WHEN NOT MATCHED THEN INSERT *""")
    sql("SET spark.delta.schema.autoMerge.enabled = false")
    val t = registerTable("tbl")
    read(t, name = "readAll")
    snapshot(t)
  }

  // Schema evolution - partitioned tables

  test("mergeSchemaEvoPartitionedAddCol") {
    sql("""CREATE TABLE tbl (id INT, value STRING, part STRING) USING delta
      PARTITIONED BY (part)
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a','x'),(2,'b','y')")
    sql("SET spark.delta.schema.autoMerge.enabled = true")
    sql("""MERGE INTO tbl t USING (
      SELECT * FROM VALUES (2,'x','y',10),(3,'c','z',20) AS s(id, value, part, newcol)
    ) s ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *""")
    sql("SET spark.delta.schema.autoMerge.enabled = false")
    val t = registerTable("tbl")
    read(t, name = "readAll")
    read(t, predicate = "newcol IS NOT NULL", name = "readNewCol")
    snapshot(t)
  }

  test("mergeSchemaEvoPartitionedStructEvo") {
    sql("""CREATE TABLE tbl (id INT, info STRUCT<name: STRING>, part STRING) USING delta
      PARTITIONED BY (part)
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, named_struct('name','alice'), 'x')")
    sql("SET spark.delta.schema.autoMerge.enabled = true")
    sql("""MERGE INTO tbl t USING (
      SELECT * FROM VALUES (2, named_struct('name','bob','email','bob@x.com'), 'y') AS s(id, info, part)
    ) s ON t.id = s.id
      WHEN NOT MATCHED THEN INSERT *""")
    sql("SET spark.delta.schema.autoMerge.enabled = false")
    val t = registerTable("tbl")
    read(t, name = "readAll")
    snapshot(t)
  }

  // Schema evolution - type widening and column mapping

  test("mergeSchemaEvoWidenType") {
    sql("""CREATE TABLE tbl (id INT, amount INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,100),(2,200)")
    sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.enableTypeWidening' = 'true')")
    sql("SET spark.delta.schema.autoMerge.enabled = true")
    sql("""MERGE INTO tbl t USING (
      SELECT * FROM VALUES (2, CAST(3000000000 AS BIGINT)),(3, CAST(4000000000 AS BIGINT)) AS s(id, amount)
    ) s ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET amount = s.amount
      WHEN NOT MATCHED THEN INSERT *""")
    sql("SET spark.delta.schema.autoMerge.enabled = false")
    val t = registerTable("tbl")
    read(t, name = "readAll")
    read(t, predicate = "amount > 2000000000", name = "readLargeAmount")
    snapshot(t)
  }

  test("mergeSchemaEvoWithColumnMapping") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true',
        'delta.columnMapping.mode' = 'name')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b')")
    sql("SET spark.delta.schema.autoMerge.enabled = true")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,'x',5),(3,'c',10) AS s(id, value, score)) s
      ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *""")
    sql("SET spark.delta.schema.autoMerge.enabled = false")
    val t = registerTable("tbl")
    read(t, name = "readAll")
    read(t, predicate = "score IS NOT NULL", name = "readScore")
    snapshot(t)
  }

  test("mergeSchemaEvoDvSchemaEvo") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
    sql("DELETE FROM tbl WHERE id = 3")
    sql("SET spark.delta.schema.autoMerge.enabled = true")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,'x',10),(4,'d',20) AS s(id, value, extra)) s
      ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *""")
    sql("SET spark.delta.schema.autoMerge.enabled = false")
    val t = registerTable("tbl")
    read(t, name = "readAll")
    read(t, predicate = "extra IS NOT NULL", name = "readExtra")
    snapshot(t)
  }

  // Schema evolution - error cases

  test("mergeSchemaEvoErrDuplicateCol") {
    sql("""CREATE TABLE tbl (id INT, value STRING, extra_value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a','ea'),(2,'b','eb')")
    sql("SET spark.delta.schema.autoMerge.enabled = true")
    // Attempt merge that might cause duplicate column issue
    try {
      sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,'x','ex') AS s(id, value, extra_value)) s
        ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET *""")
    } catch { case _: Exception => /* may or may not fail */ }
    sql("SET spark.delta.schema.autoMerge.enabled = false")
    val t = registerTable("tbl")
    read(t, name = "readAll")
    snapshot(t)
  }

  test("mergeSchemaEvoErrIncompatibleType") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b')")
    sql("SET spark.delta.schema.autoMerge.enabled = true")
    try {
      sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2, ARRAY(1,2)) AS s(id, value)) s
        ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET value = s.value""")
    } catch { case _: Exception => /* expected: incompatible type */ }
    sql("SET spark.delta.schema.autoMerge.enabled = false")
    val t = registerTable("tbl")
    read(t, name = "readAll")
    snapshot(t)
  }

  test("mergeSchemaEvoErrNarrowType") {
    sql("""CREATE TABLE tbl (id INT, amount BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,100),(2,200)")
    sql("SET spark.delta.schema.autoMerge.enabled = true")
    try {
      sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2, CAST(50 AS INT)) AS s(id, amount)) s
        ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET amount = s.amount""")
    } catch { case _: Exception => /* may or may not fail depending on implicit cast */ }
    sql("SET spark.delta.schema.autoMerge.enabled = false")
    val t = registerTable("tbl")
    read(t, name = "readAll")
    snapshot(t)
  }

  // Struct evolution (deep nesting, null handling)

  test("mergeStructEvoNullNewField") {
    sql("""CREATE TABLE tbl (id INT, info STRUCT<a: INT>) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, named_struct('a',10)),(2, named_struct('a',20))")
    sql("SET spark.delta.schema.autoMerge.enabled = true")
    sql("""MERGE INTO tbl t USING (
      SELECT * FROM VALUES (2, named_struct('a',25,'b','new')),(3, named_struct('a',30,'b','also_new')) AS s(id, info)
    ) s ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET info = s.info
      WHEN NOT MATCHED THEN INSERT *""")
    sql("SET spark.delta.schema.autoMerge.enabled = false")
    val t = registerTable("tbl")
    read(t, name = "readAll")
    snapshot(t)
  }

  test("mergeStructEvoNullableToNonNull") {
    sql("""CREATE TABLE tbl (id INT, data STRUCT<x: INT>) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, named_struct('x',10)),(2, NULL)")
    sql("SET spark.delta.schema.autoMerge.enabled = true")
    sql("""MERGE INTO tbl t USING (
      SELECT * FROM VALUES (2, named_struct('x',20,'y','val')),(3, named_struct('x',30,'y','val2')) AS s(id, data)
    ) s ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET data = s.data
      WHEN NOT MATCHED THEN INSERT *""")
    sql("SET spark.delta.schema.autoMerge.enabled = false")
    val t = registerTable("tbl")
    read(t, name = "readAll")
    snapshot(t)
  }

  test("mergeStructEvoNestedNullField") {
    sql("""CREATE TABLE tbl (id INT, outer_col STRUCT<inner: STRUCT<p: INT>>) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, named_struct('inner', named_struct('p',10))),(2, named_struct('inner', named_struct('p',20)))")
    sql("SET spark.delta.schema.autoMerge.enabled = true")
    sql("""MERGE INTO tbl t USING (
      SELECT * FROM VALUES (2, named_struct('inner', named_struct('p',25,'q','new'))),(3, named_struct('inner', named_struct('p',30,'q','also'))) AS s(id, outer_col)
    ) s ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET outer_col = s.outer_col
      WHEN NOT MATCHED THEN INSERT *""")
    sql("SET spark.delta.schema.autoMerge.enabled = false")
    val t = registerTable("tbl")
    read(t, name = "readAll")
    snapshot(t)
  }

  test("mergeStructEvoMultiStructCols") {
    sql("""CREATE TABLE tbl (id INT, s1 STRUCT<a: INT>, s2 STRUCT<x: STRING>) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, named_struct('a',1), named_struct('x','hello'))")
    sql("SET spark.delta.schema.autoMerge.enabled = true")
    sql("""MERGE INTO tbl t USING (
      SELECT * FROM VALUES (1, named_struct('a',2,'b',3), named_struct('x','world')),(2, named_struct('a',4,'b',5), named_struct('x','new')) AS s(id, s1, s2)
    ) s ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET s1 = s.s1
      WHEN NOT MATCHED THEN INSERT *""")
    sql("SET spark.delta.schema.autoMerge.enabled = false")
    val t = registerTable("tbl")
    read(t, name = "readAll")
    snapshot(t)
  }

  test("mergeStructEvoNullInKey") {
    sql("""CREATE TABLE tbl (key STRUCT<k1: INT, k2: STRING>, value INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (named_struct('k1',1,'k2','a'), 10),(named_struct('k1',2,'k2',CAST(NULL AS STRING)), 20)")
    sql("""MERGE INTO tbl t USING (
      SELECT * FROM VALUES (named_struct('k1',1,'k2','a'), 99),(named_struct('k1',3,'k2','c'), 30) AS s(key, value)
    ) s ON t.key = s.key
      WHEN MATCHED THEN UPDATE SET value = s.value
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerTable("tbl")
    read(t, name = "readAll")
    snapshot(t)
  }

  test("mergeStructEvoArrayOfStructNull") {
    sql("""CREATE TABLE tbl (id INT, items ARRAY<STRUCT<a: INT>>) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, array(named_struct('a',1)))")
    sql("SET spark.delta.schema.autoMerge.enabled = true")
    sql("""MERGE INTO tbl t USING (
      SELECT * FROM VALUES (1, array(named_struct('a',2,'b','x'))),(2, array(named_struct('a',3,'b','y'))) AS s(id, items)
    ) s ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET items = s.items
      WHEN NOT MATCHED THEN INSERT *""")
    sql("SET spark.delta.schema.autoMerge.enabled = false")
    val t = registerTable("tbl")
    read(t, name = "readAll")
    snapshot(t)
  }

  test("mergeStructEvoMapValueStructNull") {
    sql("""CREATE TABLE tbl (id INT, props MAP<STRING, STRUCT<v: INT>>) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, map('k1', named_struct('v',10)))")
    sql("SET spark.delta.schema.autoMerge.enabled = true")
    sql("""MERGE INTO tbl t USING (
      SELECT * FROM VALUES (1, map('k1', named_struct('v',20,'extra',true))),(2, map('k2', named_struct('v',30,'extra',false))) AS s(id, props)
    ) s ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET props = s.props
      WHEN NOT MATCHED THEN INSERT *""")
    sql("SET spark.delta.schema.autoMerge.enabled = false")
    val t = registerTable("tbl")
    read(t, name = "readAll")
    snapshot(t)
  }

  test("mergeStructEvoDeepNested") {
    sql("""CREATE TABLE tbl (id INT, deep STRUCT<l1: STRUCT<l2: STRUCT<val: INT>>>) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, named_struct('l1', named_struct('l2', named_struct('val', 10))))")
    sql("SET spark.delta.schema.autoMerge.enabled = true")
    sql("""MERGE INTO tbl t USING (
      SELECT * FROM VALUES (1, named_struct('l1', named_struct('l2', named_struct('val', 20, 'tag', 'x')))),
        (2, named_struct('l1', named_struct('l2', named_struct('val', 30, 'tag', 'y')))) AS s(id, deep)
    ) s ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET deep = s.deep
      WHEN NOT MATCHED THEN INSERT *""")
    sql("SET spark.delta.schema.autoMerge.enabled = false")
    val t = registerTable("tbl")
    read(t, name = "readAll")
    snapshot(t)
  }

  test("mergeStructEvoMixedNull") {
    sql("""CREATE TABLE tbl (id INT, info STRUCT<a: INT>) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, named_struct('a',10)),(2, named_struct('a',20)),(3, named_struct('a',30))")
    sql("SET spark.delta.schema.autoMerge.enabled = true")
    sql("""MERGE INTO tbl t USING (
      SELECT * FROM VALUES (1, named_struct('a',15,'b','has_b')),(3, named_struct('a',35,'b',CAST(NULL AS STRING))),(4, named_struct('a',40,'b','new')) AS s(id, info)
    ) s ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET info = s.info
      WHEN NOT MATCHED THEN INSERT *""")
    sql("SET spark.delta.schema.autoMerge.enabled = false")
    val t = registerTable("tbl")
    read(t, name = "readAll")
    snapshot(t)
  }

  test("mergeStructEvoPartitionedStruct") {
    sql("""CREATE TABLE tbl (region STRING, id INT, metrics STRUCT<score: INT>) USING delta
      PARTITIONED BY (region)
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES ('us',1,named_struct('score',80)),('eu',2,named_struct('score',90))")
    sql("SET spark.delta.schema.autoMerge.enabled = true")
    sql("""MERGE INTO tbl t USING (
      SELECT * FROM VALUES ('us',1,named_struct('score',85,'grade','B')),('eu',3,named_struct('score',95,'grade','A')) AS s(region, id, metrics)
    ) s ON t.id = s.id AND t.region = s.region
      WHEN MATCHED THEN UPDATE SET metrics = s.metrics
      WHEN NOT MATCHED THEN INSERT *""")
    sql("SET spark.delta.schema.autoMerge.enabled = false")
    val t = registerTable("tbl")
    read(t, name = "readAll")
    snapshot(t)
  }

  // NOT MATCHED BY SOURCE (mrb_ prefix workloads)

  test("mrb_all_clause_types") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,'x'),(4,'d') AS s(id, value)) s
      ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET value = s.value
      WHEN NOT MATCHED THEN INSERT *
      WHEN NOT MATCHED BY SOURCE THEN DELETE""")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("mrb_not_matched_by_source_delete") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,'x') AS s(id, value)) s
      ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET value = s.value
      WHEN NOT MATCHED BY SOURCE THEN DELETE""")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("mrb_not_matched_by_source_update") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,'x') AS s(id, value)) s
      ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET value = s.value
      WHEN NOT MATCHED BY SOURCE THEN UPDATE SET value = 'orphan'""")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("mrb_with_change_tracking") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true',
        'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,'x'),(4,'d') AS s(id, value)) s
      ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET value = s.value
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("mrb_with_dv") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,'x') AS s(id, value)) s
      ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET value = s.value
      WHEN NOT MATCHED BY SOURCE THEN DELETE""")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("mrb_with_schema_evolution") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b')")
    sql("SET spark.delta.schema.autoMerge.enabled = true")
    sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,'x',5),(3,'c',10) AS s(id, value, score)) s
      ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *""")
    sql("SET spark.delta.schema.autoMerge.enabled = false")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "score IS NOT NULL", name = "read_evolved_cols")
    snapshot(t)
  }

  // Generate all workloads

}
