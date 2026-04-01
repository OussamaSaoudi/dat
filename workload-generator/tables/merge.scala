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
 * Run: ./bin/generate-workload.sh tables/merge.scala
 */
import io.delta.workload.WorkloadGenerator._

// =============================================================================
// Basic clause types
// =============================================================================

workload("mergeBasicInsert", "MERGE with only INSERT clause", "merge", "dml") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
  w.sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,'x'),(4,'d'),(5,'e') AS s(id, value)) s
    ON t.id = s.id
    WHEN NOT MATCHED THEN INSERT *""")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "id >= 4", name = "filter_new_rows")
  w.snapshot(t)
}

workload("mergeBasicUpdate", "MERGE with only UPDATE clause", "merge", "dml") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b')")
  w.sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,'x') AS s(id, value)) s
    ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET value = s.value""")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "id = 2", name = "filter_updated")
  w.snapshot(t)
}

workload("mergeBasicDelete", "MERGE with only DELETE clause", "merge", "dml") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
  w.sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,'x') AS s(id, value)) s
    ON t.id = s.id
    WHEN MATCHED THEN DELETE""")
  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
}

workload("mergeInsertUpdate", "MERGE with INSERT and UPDATE clauses", "merge", "dml") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b')")
  w.sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,'x'),(3,'c') AS s(id, value)) s
    ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET value = s.value
    WHEN NOT MATCHED THEN INSERT *""")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "id = 2", name = "filter_id_2")
  w.snapshot(t)
}

workload("mergeInsertDelete", "MERGE with INSERT and DELETE clauses", "merge", "dml") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
  w.sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,'x'),(4,'d') AS s(id, value)) s
    ON t.id = s.id
    WHEN MATCHED THEN DELETE
    WHEN NOT MATCHED THEN INSERT *""")
  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
}

workload("mergeInsertUpdateDelete", "MERGE with INSERT, UPDATE, and DELETE", "merge", "dml") { w =>
  w.sql("""CREATE TABLE tbl (id INT, val INT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,10),(2,20),(3,30)")
  w.sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,40),(4,200) AS s(id, val)) s
    ON t.id = s.id
    WHEN MATCHED AND t.val > 15 THEN UPDATE SET val = s.val
    WHEN MATCHED THEN DELETE
    WHEN NOT MATCHED THEN INSERT *""")
  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
}

workload("mergeUpdateDelete", "MERGE with conditional UPDATE and DELETE", "merge", "dml") { w =>
  w.sql("""CREATE TABLE tbl (id INT, val INT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,10),(2,20),(3,30)")
  w.sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (1,100),(2,200),(3,300) AS s(id, val)) s
    ON t.id = s.id
    WHEN MATCHED AND t.val > 15 THEN UPDATE SET val = s.val
    WHEN MATCHED THEN DELETE""")
  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
}

workload("mergeMultipleMatched", "MERGE with multiple WHEN MATCHED clauses", "merge", "dml") { w =>
  w.sql("""CREATE TABLE tbl (id INT, score INT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,5),(2,15),(3,35)")
  w.sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (1,50),(2,50),(3,50) AS s(id, score)) s
    ON t.id = s.id
    WHEN MATCHED AND t.score > 30 THEN DELETE
    WHEN MATCHED AND t.score > 10 THEN UPDATE SET score = s.score""")
  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
}

// =============================================================================
// Conditional clauses
// =============================================================================

workload("mergeConditionalInsert", "MERGE with conditional WHEN NOT MATCHED", "merge", "dml") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b')")
  w.sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,'x'),(3,'c'),(4,'d') AS s(id, value)) s
    ON t.id = s.id
    WHEN NOT MATCHED AND s.id > 3 THEN INSERT *""")
  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
}

workload("mergeConditionalUpdate", "MERGE with conditional WHEN MATCHED", "merge", "dml") { w =>
  w.sql("""CREATE TABLE tbl (id INT, status STRING, score INT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'low',5),(2,'low',15),(3,'low',25)")
  w.sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,'high',50),(3,'high',50) AS s(id, status, score)) s
    ON t.id = s.id
    WHEN MATCHED AND t.score >= 10 THEN UPDATE SET status = s.status, score = s.score""")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "status = 'high'", name = "filter_high")
  w.snapshot(t)
}

// =============================================================================
// Star syntax
// =============================================================================

workload("mergeStarInsert", "MERGE with INSERT * syntax", "merge", "dml") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b')")
  w.sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (3,'c'),(4,'d') AS s(id, value)) s
    ON t.id = s.id
    WHEN NOT MATCHED THEN INSERT *""")
  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
}

workload("mergeStarUpdate", "MERGE with UPDATE SET * syntax", "merge", "dml") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b')")
  w.sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,'x') AS s(id, value)) s
    ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET *""")
  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
}

// =============================================================================
// Source variations
// =============================================================================

workload("mergeSourceSubquery", "MERGE with source as subquery", "merge", "dml") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b')")
  w.sql("""MERGE INTO tbl t
    USING (SELECT id, CONCAT(value, '_new') AS value FROM VALUES (2,'x'),(3,'c') AS s(id, value)) s
    ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET value = s.value
    WHEN NOT MATCHED THEN INSERT *""")
  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
}

workload("mergeSourceAggregation", "MERGE with aggregated source", "merge", "dml") { w =>
  w.sql("""CREATE TABLE tbl (id INT, total BIGINT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,100),(2,200)")
  w.sql("""MERGE INTO tbl t
    USING (SELECT id, SUM(amount) AS total FROM VALUES (2,30),(2,50),(3,150) AS s(id, amount) GROUP BY id) s
    ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET total = s.total
    WHEN NOT MATCHED THEN INSERT *""")
  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
}

// =============================================================================
// Data types
// =============================================================================

workload("mergeBooleanValues", "MERGE with boolean columns", "merge", "dml") { w =>
  w.sql("""CREATE TABLE tbl (id INT, active BOOLEAN) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,true),(2,false),(3,true)")
  w.sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,true),(4,false) AS s(id, active)) s
    ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET active = s.active
    WHEN NOT MATCHED THEN INSERT *""")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "active = true", name = "filter_active")
  w.snapshot(t)
}

workload("mergeDecimalValues", "MERGE with decimal value columns", "merge", "dml") { w =>
  w.sql("""CREATE TABLE tbl (id INT, price DECIMAL(10,2)) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,10.50),(2,20.75)")
  w.sql("INSERT INTO tbl VALUES (3,30.00)")
  w.sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,99.99),(4,45.00) AS s(id, price)) s
    ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET price = s.price
    WHEN NOT MATCHED THEN INSERT *""")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "price > 30.00", name = "filter_price")
  w.snapshot(t)
}

workload("mergeTimestampValues", "MERGE with timestamp columns", "merge", "dml") { w =>
  w.sql("""CREATE TABLE tbl (id INT, ts TIMESTAMP) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1, TIMESTAMP '2024-01-01 10:00:00')")
  w.sql("INSERT INTO tbl VALUES (2, TIMESTAMP '2024-01-02 10:00:00')")
  w.sql("""MERGE INTO tbl t USING (
    SELECT * FROM VALUES (2, TIMESTAMP '2024-06-01 12:00:00'),(3, TIMESTAMP '2024-07-01 14:00:00') AS s(id, ts)
  ) s ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET ts = s.ts
    WHEN NOT MATCHED THEN INSERT *""")
  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
}

workload("mergeStringKeys", "MERGE on string key columns", "merge", "dml") { w =>
  w.sql("""CREATE TABLE tbl (name STRING, amount INT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES ('alice',100),('bob',200)")
  w.sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES ('bob',999),('carol',300) AS s(name, amount)) s
    ON t.name = s.name
    WHEN MATCHED THEN UPDATE SET amount = s.amount
    WHEN NOT MATCHED THEN INSERT *""")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "name = 'bob'", name = "filter_bob")
  w.snapshot(t)
}

// =============================================================================
// NULL handling
// =============================================================================

workload("mergeNullHandling", "MERGE with NULL values", "merge", "dml") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'a')")
  w.sql("INSERT INTO tbl VALUES (2, NULL)")
  w.sql("INSERT INTO tbl VALUES (3,'c')")
  w.sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,'updated'),(4,CAST(NULL AS STRING)) AS s(id, value)) s
    ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET value = s.value
    WHEN NOT MATCHED THEN INSERT *""")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "value IS NOT NULL", name = "filter_not_null")
  w.snapshot(t)
}

// =============================================================================
// Complex types
// =============================================================================

workload("mergeWithArrayCol", "MERGE with array columns", "merge", "dml") { w =>
  w.sql("""CREATE TABLE tbl (id INT, tags ARRAY<STRING>) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1, array('a','b'))")
  w.sql("INSERT INTO tbl VALUES (2, array('c'))")
  w.sql("""MERGE INTO tbl t USING (
    SELECT * FROM VALUES (2, array('c','d','e')),(3, array('f')) AS s(id, tags)
  ) s ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET tags = s.tags
    WHEN NOT MATCHED THEN INSERT *""")
  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
}

workload("mergeWithMapCol", "MERGE with map columns", "merge", "dml") { w =>
  w.sql("""CREATE TABLE tbl (id INT, props MAP<STRING, STRING>) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1, map('k1','v1'))")
  w.sql("INSERT INTO tbl VALUES (2, map('k2','v2'))")
  w.sql("""MERGE INTO tbl t USING (
    SELECT * FROM VALUES (2, map('k2','updated')),(3, map('k3','v3')) AS s(id, props)
  ) s ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET props = s.props
    WHEN NOT MATCHED THEN INSERT *""")
  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
}

workload("mergeWithNestedStruct", "MERGE with nested struct columns", "merge", "dml") { w =>
  w.sql("""CREATE TABLE tbl (id INT, info STRUCT<name: STRING, age: INT>) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1, named_struct('name','alice','age',30))")
  w.sql("INSERT INTO tbl VALUES (2, named_struct('name','bob','age',25))")
  w.sql("""MERGE INTO tbl t USING (
    SELECT * FROM VALUES
      (2, named_struct('name','bob','age',26)),
      (3, named_struct('name','carol','age',35))
    AS s(id, info)
  ) s ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET info = s.info
    WHEN NOT MATCHED THEN INSERT *""")
  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
}

// =============================================================================
// Partitioned tables
// =============================================================================

workload("mergePartitionedBasic", "MERGE on partitioned table", "merge", "dml", "partitioned") { w =>
  w.sql("""CREATE TABLE tbl (id INT, region STRING, amount INT) USING delta
    PARTITIONED BY (region)
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'east',100),(2,'west',200),(3,'east',300)")
  w.sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,'west',999),(4,'north',400) AS s(id, region, amount)) s
    ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET amount = s.amount
    WHEN NOT MATCHED THEN INSERT *""")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "region = 'east'", name = "filter_east")
  w.snapshot(t)
}

workload("mergePartitionedCrossPartition", "MERGE cross-partition row movement", "merge", "dml", "partitioned") { w =>
  w.sql("""CREATE TABLE tbl (id INT, region STRING, amount INT) USING delta
    PARTITIONED BY (region)
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'east',100),(2,'east',200),(3,'west',300)")
  w.sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (1,'west',150),(2,'west',250) AS s(id, region, amount)) s
    ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET region = s.region, amount = s.amount""")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "region = 'west'", name = "filter_west")
  w.snapshot(t)
}

workload("mergePartitionedMultiCol", "MERGE with multiple partition columns", "merge", "dml", "partitioned") { w =>
  w.sql("""CREATE TABLE tbl (id INT, country STRING, year INT, amount INT) USING delta
    PARTITIONED BY (country, year)
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'US',2024,100),(2,'UK',2024,200),(3,'US',2023,300)")
  w.sql("""MERGE INTO tbl t USING (
    SELECT * FROM VALUES (1,'US',2024,999),(4,'DE',2024,400) AS s(id, country, year, amount)
  ) s ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET amount = s.amount
    WHEN NOT MATCHED THEN INSERT *""")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "country = 'US' AND year = 2024", name = "filter_us_2024")
  w.snapshot(t)
}

// =============================================================================
// Deletion vectors (DV-enabled tables with prior deletes)
// =============================================================================

workload("mergeDvBasicInsertUpdate", "Basic merge on DV-enabled table with INSERT+UPDATE", "merge", "dml", "dv") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
  w.sql("DELETE FROM tbl WHERE id = 2")
  w.sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (1,'x'),(2,'x'),(4,'d') AS s(id, value)) s
    ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET value = s.value
    WHEN NOT MATCHED THEN INSERT *""")
  val t = w.table("tbl")
  w.read(t, name = "readAll")
  w.snapshot(t)
}

workload("mergeDvConditionalUpdate", "Conditional UPDATE with DVs", "merge", "dml", "dv") { w =>
  w.sql("""CREATE TABLE tbl (id INT, amount INT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,100),(2,200),(3,300),(4,400)")
  w.sql("DELETE FROM tbl WHERE id = 4")
  w.sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,250),(3,350),(5,500) AS s(id, amount)) s
    ON t.id = s.id
    WHEN MATCHED AND s.amount > 200 THEN UPDATE SET amount = s.amount
    WHEN NOT MATCHED AND s.amount >= 500 THEN INSERT *""")
  val t = w.table("tbl")
  w.read(t, name = "readAll")
  w.snapshot(t)
}

workload("mergeDvDeleteClause", "Merge with DELETE clause on DV-enabled table", "merge", "dml", "dv") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value STRING, amount INT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'a',10),(2,'b',20),(3,'c',40),(4,'d',50),(5,'e',60)")
  w.sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,99),(3,99),(4,99) AS s(id, amount)) s
    ON t.id = s.id
    WHEN MATCHED AND t.amount > 30 THEN DELETE
    WHEN MATCHED THEN UPDATE SET amount = s.amount""")
  val t = w.table("tbl")
  w.read(t, name = "readAll")
  w.snapshot(t)
}

workload("mergeDvLargeTable", "Merge on larger DV table (100+ rows)", "merge", "dml", "dv") { w =>
  w.sql("""CREATE TABLE tbl (id INT, name STRING, score INT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl SELECT id, CONCAT('name_', CAST(id AS STRING)), id * 100 FROM range(100)")
  w.sql("DELETE FROM tbl WHERE id <= 30 AND id % 3 = 0")
  w.sql("""MERGE INTO tbl t USING (
    SELECT id, CONCAT('updated_', CAST(id AS STRING)) AS name, id * 1000 AS score
    FROM range(50, 60)
  ) s ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET name = s.name, score = s.score
    WHEN NOT MATCHED THEN INSERT *""")
  val t = w.table("tbl")
  w.read(t, name = "readAll")
  w.read(t, predicate = "score > 5000", name = "filterHighScore")
  w.snapshot(t)
}

workload("mergeDvMultipleMatched", "Multiple WHEN MATCHED clauses with DVs", "merge", "dml", "dv") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value STRING, amount INT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'a',10),(2,'b',20),(3,'c',30),(4,'d',40),(5,'e',50)")
  w.sql("DELETE FROM tbl WHERE id = 1")
  w.sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,99),(3,99),(4,99) AS s(id, amount)) s
    ON t.id = s.id
    WHEN MATCHED AND t.amount > 100 THEN DELETE
    WHEN MATCHED AND t.amount > 50 THEN UPDATE SET value = 'also', amount = s.amount
    WHEN MATCHED THEN UPDATE SET value = 'updated', amount = s.amount""")
  val t = w.table("tbl")
  w.read(t, name = "readAll")
  w.snapshot(t)
}

workload("mergeDvMultipleMerges", "Multiple consecutive merges on DV table", "merge", "dml", "dv") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
  w.sql("DELETE FROM tbl WHERE id = 2")
  w.sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (1,'x'),(2,'new'),(4,'d') AS s(id, value)) s
    ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET value = s.value
    WHEN NOT MATCHED THEN INSERT *""")
  w.sql("DELETE FROM tbl WHERE id = 1")
  w.sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (1,'back'),(5,'e') AS s(id, value)) s
    ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET value = s.value
    WHEN NOT MATCHED THEN INSERT *""")
  val t = w.table("tbl")
  w.read(t, name = "readAll")
  w.snapshot(t)
}

workload("mergeDvNullHandling", "Merge with null values on DV table", "merge", "dml", "dv") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'a'),(2,NULL),(3,'c')")
  w.sql("DELETE FROM tbl WHERE id = 1")
  w.sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,'updated'),(4,CAST(NULL AS STRING)) AS s(id, value)) s
    ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET value = s.value
    WHEN NOT MATCHED THEN INSERT *""")
  val t = w.table("tbl")
  w.read(t, name = "readAll")
  w.snapshot(t)
}

workload("mergeDvPartitioned", "Merge on partitioned DV table", "merge", "dml", "dv", "partitioned") { w =>
  w.sql("""CREATE TABLE tbl (region STRING, id INT, value STRING) USING delta
    PARTITIONED BY (region)
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES ('us',1,'a'),('us',2,'b'),('eu',3,'c'),('eu',4,'d')")
  w.sql("DELETE FROM tbl WHERE region = 'us' AND id = 1")
  w.sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES ('us',2,'x'),('eu',5,'e') AS s(region, id, value)) s
    ON t.id = s.id AND t.region = s.region
    WHEN MATCHED THEN UPDATE SET value = s.value
    WHEN NOT MATCHED THEN INSERT *""")
  val t = w.table("tbl")
  w.read(t, name = "readAll")
  w.snapshot(t)
}

workload("mergeDvSchemaEvolution", "Schema evolution merge on DV table", "merge", "dml", "dv", "schema_evolution") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
  w.sql("DELETE FROM tbl WHERE id = 3")
  w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")
  w.sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,'x',10),(4,'d',20) AS s(id, value, extra)) s
    ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *""")
  w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = false")
  val t = w.table("tbl")
  w.read(t, name = "readAll")
  w.snapshot(t)
}

workload("mergeDvStarSyntax", "Merge with INSERT * and UPDATE SET * with DVs", "merge", "dml", "dv") { w =>
  w.sql("""CREATE TABLE tbl (id INT, name STRING, score INT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'a',10),(2,'b',20),(3,'c',30)")
  w.sql("DELETE FROM tbl WHERE id = 2")
  w.sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (1,'x',99),(2,'new',50),(4,'d',40) AS s(id, name, score)) s
    ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *""")
  val t = w.table("tbl")
  w.read(t, name = "readAll")
  w.snapshot(t)
}

// =============================================================================
// Low-shuffle merge variants
// =============================================================================

workload("mergeLowShuffleBasic", "Low shuffle merge basic read-back", "merge", "dml", "low_shuffle") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
  w.sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,'x'),(4,'d') AS s(id, value)) s
    ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET value = s.value
    WHEN NOT MATCHED THEN INSERT *""")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "id = 2", name = "filter_updated")
  w.snapshot(t)
}

workload("mergeLowShuffleConditional", "Low shuffle with conditional clauses", "merge", "dml", "low_shuffle") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value STRING, amount INT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'a',10),(2,'b',20),(3,'c',30),(4,'d',40)")
  w.sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,'x',25),(3,'y',35),(5,'e',100) AS s(id, value, amount)) s
    ON t.id = s.id
    WHEN MATCHED AND s.amount > t.amount THEN UPDATE SET value = s.value, amount = s.amount
    WHEN NOT MATCHED AND s.amount > 50 THEN INSERT *""")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "id = 5", name = "filter_conditional_insert")
  w.snapshot(t)
}

workload("mergeLowShuffleDecimal", "Low shuffle with decimal columns", "merge", "dml", "low_shuffle") { w =>
  w.sql("""CREATE TABLE tbl (id INT, price DECIMAL(10,2)) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,10.50),(2,20.75),(3,30.00)")
  w.sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,99.99),(4,45.00) AS s(id, price)) s
    ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET price = s.price
    WHEN NOT MATCHED THEN INSERT *""")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "id = 2", name = "filter_updated")
  w.snapshot(t)
}

workload("mergeLowShuffleLargeTable", "Low shuffle on larger table (200+ rows)", "merge", "dml", "low_shuffle") { w =>
  w.sql("""CREATE TABLE tbl (id BIGINT, value INT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl SELECT id, CAST(id * 10 AS INT) FROM range(200)")
  w.sql("""MERGE INTO tbl t USING (
    SELECT id, 9999 AS value FROM range(100, 110)
  ) s ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET value = s.value
    WHEN NOT MATCHED THEN INSERT *""")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "value = 9999", name = "filter_updated")
  w.snapshot(t)
}

workload("mergeLowShuffleMultiClause", "Low shuffle with INSERT+UPDATE+DELETE", "merge", "dml", "low_shuffle") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value STRING, amount INT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'a',10),(2,'b',20),(3,'c',0),(4,'d',40)")
  w.sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,'x',25),(3,'y',35),(5,'e',50) AS s(id, value, amount)) s
    ON t.id = s.id
    WHEN MATCHED AND t.amount = 0 THEN DELETE
    WHEN MATCHED THEN UPDATE SET value = s.value, amount = s.amount
    WHEN NOT MATCHED THEN INSERT *""")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "id = 3", name = "filter_deleted")
  w.snapshot(t)
}

workload("mergeLowShuffleMultiMerge", "Multiple low shuffle merges in sequence", "merge", "dml", "low_shuffle") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b')")
  w.sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,'x'),(3,'c') AS s(id, value)) s
    ON t.id = s.id WHEN MATCHED THEN UPDATE SET value = s.value WHEN NOT MATCHED THEN INSERT *""")
  w.sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (3,'y'),(4,'d') AS s(id, value)) s
    ON t.id = s.id WHEN MATCHED THEN UPDATE SET value = s.value WHEN NOT MATCHED THEN INSERT *""")
  w.sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (4,'z'),(5,'e') AS s(id, value)) s
    ON t.id = s.id WHEN MATCHED THEN UPDATE SET value = s.value WHEN NOT MATCHED THEN INSERT *""")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "id = 5", name = "filter_last_merge")
  w.snapshot(t)
}

workload("mergeLowShuffleNested", "Low shuffle with nested struct columns", "merge", "dml", "low_shuffle") { w =>
  w.sql("""CREATE TABLE tbl (id INT, info STRUCT<name: STRING, age: INT>) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1, named_struct('name','alice','age',30)),(2, named_struct('name','bob','age',25))")
  w.sql("""MERGE INTO tbl t USING (
    SELECT * FROM VALUES (2, named_struct('name','bob','age',26)),(3, named_struct('name','carol','age',35)) AS s(id, info)
  ) s ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET info = s.info
    WHEN NOT MATCHED THEN INSERT *""")
  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
}

workload("mergeLowShufflePartitioned", "Low shuffle merge on partitioned table", "merge", "dml", "low_shuffle", "partitioned") { w =>
  w.sql("""CREATE TABLE tbl (id INT, part STRING, amount INT) USING delta
    PARTITIONED BY (part)
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'x',10),(2,'y',20),(3,'x',30)")
  w.sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,'y',99),(4,'z',40) AS s(id, part, amount)) s
    ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET amount = s.amount
    WHEN NOT MATCHED THEN INSERT *""")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "part = 'y'", name = "filter_part_y")
  w.snapshot(t)
}

workload("mergeLowShuffleStar", "Low shuffle with * syntax", "merge", "dml", "low_shuffle") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b')")
  w.sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,'x'),(3,'c') AS s(id, value)) s
    ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *""")
  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
}

workload("mergeLowShuffleTimestamp", "Low shuffle with timestamp columns", "merge", "dml", "low_shuffle") { w =>
  w.sql("""CREATE TABLE tbl (id INT, ts TIMESTAMP, label STRING) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1, TIMESTAMP '2024-01-01 10:00:00', 'old1'),(2, TIMESTAMP '2024-01-02 10:00:00', 'old2')")
  w.sql("""MERGE INTO tbl t USING (
    SELECT * FROM VALUES (2, TIMESTAMP '2024-06-01 12:00:00', 'upd'),(3, TIMESTAMP '2024-07-01 14:00:00', 'new') AS s(id, ts, label)
  ) s ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET ts = s.ts, label = s.label
    WHEN NOT MATCHED THEN INSERT *""")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "id = 3", name = "filter_new")
  w.snapshot(t)
}

// =============================================================================
// Edge cases
// =============================================================================

workload("mergeEdgeAllMatched", "MERGE where every source row matches target", "merge", "dml", "edge") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
  w.sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (1,'z'),(2,'z'),(3,'z') AS s(id, value)) s
    ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET value = s.value""")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "value = 'z'", name = "filter_updated")
  w.snapshot(t)
}

workload("mergeEdgeNoMatched", "MERGE where no source row matches target (all inserts)", "merge", "dml", "edge") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b')")
  w.sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (10,'x'),(20,'y') AS s(id, value)) s
    ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET value = s.value
    WHEN NOT MATCHED THEN INSERT *""")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "id >= 10", name = "filter_new")
  w.snapshot(t)
}

workload("mergeEdgeEmptySource", "MERGE with empty source (no changes)", "merge", "dml", "edge") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
  w.sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (999,'x') AS s(id, value) WHERE false) s
    ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET value = s.value
    WHEN NOT MATCHED THEN INSERT *""")
  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
}

workload("mergeEdgeEmptyTarget", "MERGE into empty target table", "merge", "dml", "edge") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (999,'placeholder')")
  w.sql("DELETE FROM tbl WHERE true")
  w.sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (1,'a'),(2,'b') AS s(id, value)) s
    ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET value = s.value
    WHEN NOT MATCHED THEN INSERT *""")
  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
}

workload("mergeEdgeNullJoinKey", "MERGE with null values in join key", "merge", "dml", "edge") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(NULL,'c')")
  w.sql("""MERGE INTO tbl t USING (
    SELECT * FROM VALUES (2,'updated'),(4,'new'),(CAST(NULL AS INT),'null_src') AS s(id, value)
  ) s ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET value = s.value
    WHEN NOT MATCHED THEN INSERT *""")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "id IS NULL", name = "filter_nulls")
  w.snapshot(t)
}

workload("mergeEdgeSelfMerge", "MERGE table into itself (self-join)", "merge", "dml", "edge") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value STRING, amount INT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'a',10),(2,'b',20),(3,'c',30)")
  w.sql("""MERGE INTO tbl t USING tbl s
    ON t.id = s.id AND t.amount < 25
    WHEN MATCHED THEN UPDATE SET amount = s.amount * 2""")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "amount = 20", name = "filter_doubled")
  w.snapshot(t)
}

workload("mergeEdgeSourceAlias", "MERGE with aliased source subquery", "merge", "dml", "edge") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
  w.sql("""MERGE INTO tbl t USING (
    SELECT id AS src_id, value AS src_value FROM VALUES (2,'x') AS s(id, value)
  ) s ON t.id = s.src_id
    WHEN MATCHED THEN UPDATE SET value = s.src_value""")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "id = 2", name = "filter_updated")
  w.snapshot(t)
}

workload("mergeEdgeMultiJoin", "MERGE with complex multi-column join condition", "merge", "dml", "edge") { w =>
  w.sql("""CREATE TABLE tbl (id INT, key STRING, amount INT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'x',10),(2,'y',20),(3,'z',30)")
  w.sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,'y',999),(4,'w',400) AS s(id, key, amount)) s
    ON t.id = s.id AND t.key = s.key
    WHEN MATCHED THEN UPDATE SET amount = s.amount
    WHEN NOT MATCHED THEN INSERT *""")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "id = 2 AND amount = 999", name = "filter_updated")
  w.snapshot(t)
}

workload("mergeEdgeLargePayload", "MERGE with wide rows (many columns)", "merge", "dml", "edge") { w =>
  val cols = (1 to 20).map(i => s"col_$i INT").mkString(", ")
  w.sql(s"""CREATE TABLE tbl (id INT, $cols) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  val vals1 = (1 to 20).map(i => i * 10).mkString(",")
  val vals2 = (1 to 20).map(i => i * 20).mkString(",")
  w.sql(s"INSERT INTO tbl VALUES (1,$vals1),(2,$vals2)")
  val srcVals2 = (1 to 20).map(i => i * 99).mkString(",")
  val srcVals3 = (1 to 20).map(i => i * 30).mkString(",")
  w.sql(s"""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,$srcVals2),(3,$srcVals3) AS s(id,${(1 to 20).map(i => s"col_$i").mkString(",")})) s
    ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET ${(1 to 20).map(i => s"col_$i = s.col_$i").mkString(",")}
    WHEN NOT MATCHED THEN INSERT *""")
  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
}

workload("mergeEdgeDuplicateSourceKeys", "MERGE with duplicate keys in source (write-path error, table unchanged)", "merge", "dml", "edge") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b')")
  // Merge with duplicate source keys should fail; table stays at version 0
  try {
    w.sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (1,'x'),(1,'y') AS s(id, value)) s
      ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET value = s.value""")
  } catch { case _: Exception => /* expected failure */ }
  val t = w.table("tbl")
  w.read(t, name = "readAll")
  w.snapshot(t)
}

// =============================================================================
// Error cases (table unchanged after failed merge)
// =============================================================================

workload("mergeErrAmbiguousColumn", "Table state after failed MERGE with ambiguous column", "merge", "dml", "error") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b')")
  try {
    w.sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,'x') AS s(id, value)) s
      ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET value = value""")
  } catch { case _: Exception => /* expected: ambiguous column reference */ }
  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
}

workload("mergeErrDuplicateSource", "Table state after failed MERGE with duplicate source rows", "merge", "dml", "error") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b')")
  try {
    w.sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (1,'x'),(1,'y') AS s(id, value)) s
      ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET value = s.value""")
  } catch { case _: Exception => /* expected: duplicate source */ }
  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
}

workload("mergeErrNoMatchCondition", "Table state after failed MERGE without ON condition", "merge", "dml", "error") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b')")
  // This should fail at parse time (no ON clause)
  try {
    w.sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (1,'x') AS s(id, value)) s
      WHEN MATCHED THEN UPDATE SET value = s.value""")
  } catch { case _: Exception => /* expected: parse error */ }
  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
}

workload("mergeErrTypeMismatch", "Table state after failed MERGE with type mismatch", "merge", "dml", "error") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b')")
  try {
    w.sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (1, ARRAY(1,2,3)) AS s(id, value)) s
      ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET value = s.value""")
  } catch { case _: Exception => /* expected: type mismatch */ }
  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
}

// =============================================================================
// Schema evolution - basic column addition
// =============================================================================

workload("mergeSchemaEvoAddCol", "Merge adds new column via schema evolution", "merge", "dml", "schema_evolution") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b')")
  w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")
  w.sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,'x',10),(3,'c',20) AS s(id, value, extra)) s
    ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *""")
  w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = false")
  val t = w.table("tbl")
  w.read(t, name = "readAll")
  w.read(t, predicate = "extra IS NOT NULL", name = "readNewCol")
  w.snapshot(t)
}

workload("mergeSchemaEvoAddMultiCols", "Schema evolution adds multiple new columns", "merge", "dml", "schema_evolution") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b')")
  w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")
  w.sql("""MERGE INTO tbl t USING (
    SELECT * FROM VALUES (2,'x',10,1.5,true),(3,'c',20,2.5,false) AS s(id, value, extra1, extra2, extra3)
  ) s ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *""")
  w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = false")
  val t = w.table("tbl")
  w.read(t, name = "readAll")
  w.read(t, predicate = "extra1 IS NOT NULL", name = "readExtra1NotNull")
  w.snapshot(t)
}

workload("mergeSchemaEvoInsertNewCol", "INSERT * with extra column in source", "merge", "dml", "schema_evolution") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b')")
  w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")
  w.sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (3,'c',100),(4,'d',200) AS s(id, value, score)) s
    ON t.id = s.id
    WHEN NOT MATCHED THEN INSERT *""")
  w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = false")
  val t = w.table("tbl")
  w.read(t, name = "readAll")
  w.read(t, predicate = "score IS NOT NULL", name = "readWithScore")
  w.snapshot(t)
}

workload("mergeSchemaEvoInsertMultipleNewCols", "INSERT adds 3+ new columns", "merge", "dml", "schema_evolution") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b')")
  w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")
  w.sql("""MERGE INTO tbl t USING (
    SELECT * FROM VALUES (3,'c',10,1.5,'x',true),(4,'d',20,2.5,'y',false) AS s(id, value, col1, col2, col3, col4)
  ) s ON t.id = s.id
    WHEN NOT MATCHED THEN INSERT *""")
  w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = false")
  val t = w.table("tbl")
  w.read(t, name = "readAll")
  w.read(t, predicate = "col1 IS NOT NULL", name = "readNewCols")
  w.snapshot(t)
}

workload("mergeSchemaEvoInsertWithDefault", "INSERT new col, existing rows get null", "merge", "dml", "schema_evolution") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b')")
  w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")
  w.sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (3,'c',100) AS s(id, value, newcol)) s
    ON t.id = s.id
    WHEN NOT MATCHED THEN INSERT *""")
  w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = false")
  val t = w.table("tbl")
  w.read(t, name = "readAll")
  w.read(t, predicate = "newcol IS NULL", name = "readNullNewCol")
  w.snapshot(t)
}

workload("mergeSchemaEvoUpdateNewCol", "UPDATE SET with column not in target", "merge", "dml", "schema_evolution") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b')")
  w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")
  w.sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,'x',5) AS s(id, value, rating)) s
    ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET *""")
  w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = false")
  val t = w.table("tbl")
  w.read(t, name = "readAll")
  w.read(t, predicate = "rating IS NOT NULL", name = "readRating")
  w.snapshot(t)
}

workload("mergeSchemaEvoUpdateStarNewCol", "UPDATE SET * with extra column in source", "merge", "dml", "schema_evolution") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b')")
  w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")
  w.sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,'x','extra') AS s(id, value, bonus)) s
    ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET *""")
  w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = false")
  val t = w.table("tbl")
  w.read(t, name = "readAll")
  w.read(t, predicate = "bonus IS NOT NULL", name = "readBonusNotNull")
  w.snapshot(t)
}

workload("mergeSchemaEvoInsertUpdateNewCol", "INSERT and UPDATE add same new column", "merge", "dml", "schema_evolution") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b')")
  w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")
  w.sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,'x',5),(3,'c',10) AS s(id, value, priority)) s
    ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *""")
  w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = false")
  val t = w.table("tbl")
  w.read(t, name = "readAll")
  w.read(t, predicate = "priority IS NOT NULL", name = "readPriority")
  w.snapshot(t)
}

workload("mergeSchemaEvoInsertUpdateDiffCols", "INSERT adds col A, UPDATE adds col B", "merge", "dml", "schema_evolution") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b')")
  w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")
  w.sql("""MERGE INTO tbl t USING (
    SELECT * FROM VALUES (2,'x',10,CAST(NULL AS STRING)),(3,'c',CAST(NULL AS INT),'new') AS s(id, value, colA, colB)
  ) s ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *""")
  w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = false")
  val t = w.table("tbl")
  w.read(t, name = "readAll")
  w.read(t, predicate = "colA IS NOT NULL", name = "readColA")
  w.snapshot(t)
}

workload("mergeSchemaEvoUpdateMultipleClauses", "Multiple WHEN MATCHED clauses with schema evolution", "merge", "dml", "schema_evolution") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value STRING, amount INT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'a',10),(2,'b',25),(3,'c',30)")
  w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")
  w.sql("""MERGE INTO tbl t USING (
    SELECT * FROM VALUES (1,'x',15,'low'),(2,'y',30,'high'),(3,'z',35,'high'),(4,'w',50,'new') AS s(id, value, amount, flag)
  ) s ON t.id = s.id
    WHEN MATCHED AND t.amount > 20 THEN UPDATE SET *
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *""")
  w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = false")
  val t = w.table("tbl")
  w.read(t, name = "readAll")
  w.read(t, predicate = "flag IS NOT NULL", name = "readFlag")
  w.snapshot(t)
}

// =============================================================================
// Schema evolution - nested structs
// =============================================================================

workload("mergeSchemaEvoAddNestedField", "Add nested struct field via merge", "merge", "dml", "schema_evolution") { w =>
  w.sql("""CREATE TABLE tbl (id INT, info STRUCT<name: STRING, age: INT>) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1, named_struct('name','alice','age',30))")
  w.sql("INSERT INTO tbl VALUES (2, named_struct('name','bob','age',25))")
  w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")
  w.sql("""MERGE INTO tbl t USING (
    SELECT * FROM VALUES (2, named_struct('name','bob','age',26,'email','bob@x.com')),(3, named_struct('name','carol','age',35,'email','carol@x.com'))
    AS s(id, info)
  ) s ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET info = s.info
    WHEN NOT MATCHED THEN INSERT *""")
  w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = false")
  val t = w.table("tbl")
  w.read(t, name = "readAll")
  w.snapshot(t)
}

workload("mergeSchemaEvoInsertNestedNewField", "INSERT with nested struct extra field", "merge", "dml", "schema_evolution") { w =>
  w.sql("""CREATE TABLE tbl (id INT, info STRUCT<name: STRING>) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1, named_struct('name','alice'))")
  w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")
  w.sql("""MERGE INTO tbl t USING (
    SELECT * FROM VALUES (2, named_struct('name','bob','city','NYC')) AS s(id, info)
  ) s ON t.id = s.id
    WHEN NOT MATCHED THEN INSERT *""")
  w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = false")
  val t = w.table("tbl")
  w.read(t, name = "readAll")
  w.snapshot(t)
}

workload("mergeSchemaEvoUpdateNestedField", "UPDATE nested struct with new field", "merge", "dml", "schema_evolution") { w =>
  w.sql("""CREATE TABLE tbl (id INT, details STRUCT<city: STRING, zip: STRING>) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1, named_struct('city','NYC','zip','10001'))")
  w.sql("INSERT INTO tbl VALUES (2, named_struct('city','LA','zip','90001'))")
  w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")
  w.sql("""MERGE INTO tbl t USING (
    SELECT * FROM VALUES (2, named_struct('city','LA','zip','90001','country','US')) AS s(id, details)
  ) s ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET details = s.details""")
  w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = false")
  val t = w.table("tbl")
  w.read(t, name = "readAll")
  w.snapshot(t)
}

workload("mergeSchemaEvoNestedStructAdd", "Add field to deeply nested struct", "merge", "dml", "schema_evolution") { w =>
  w.sql("""CREATE TABLE tbl (id INT, outer_col STRUCT<inner: STRUCT<x: INT>>) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1, named_struct('inner', named_struct('x', 10)))")
  w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")
  w.sql("""MERGE INTO tbl t USING (
    SELECT * FROM VALUES (2, named_struct('inner', named_struct('x', 20, 'y', 30))) AS s(id, outer_col)
  ) s ON t.id = s.id
    WHEN NOT MATCHED THEN INSERT *""")
  w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = false")
  val t = w.table("tbl")
  w.read(t, name = "readAll")
  w.snapshot(t)
}

// =============================================================================
// Schema evolution - complex types (arrays, maps)
// =============================================================================

workload("mergeSchemaEvoAddArrayElement", "Merge with array and schema evolution", "merge", "dml", "schema_evolution") { w =>
  w.sql("""CREATE TABLE tbl (id INT, numbers ARRAY<INT>) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1, array(1,2,3)),(2, array(4,5))")
  w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")
  w.sql("""MERGE INTO tbl t USING (
    SELECT * FROM VALUES (2, array(4,5,6), 'active'),(3, array(7), 'new') AS s(id, numbers, status)
  ) s ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *""")
  w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = false")
  val t = w.table("tbl")
  w.read(t, name = "readAll")
  w.read(t, predicate = "status IS NOT NULL", name = "readNewStatus")
  w.snapshot(t)
}

workload("mergeSchemaEvoAddMapEntry", "Merge adds map data with schema evolution", "merge", "dml", "schema_evolution") { w =>
  w.sql("""CREATE TABLE tbl (id INT, props MAP<STRING, STRING>) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1, map('k1','v1')),(2, map('k2','v2'))")
  w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")
  w.sql("""MERGE INTO tbl t USING (
    SELECT * FROM VALUES (2, map('k2','updated'), 10),(3, map('k3','v3'), 20) AS s(id, props, extra)
  ) s ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *""")
  w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = false")
  val t = w.table("tbl")
  w.read(t, name = "readAll")
  w.snapshot(t)
}

workload("mergeSchemaEvoArrayStructEvolution", "Array of structs, add struct field", "merge", "dml", "schema_evolution") { w =>
  w.sql("""CREATE TABLE tbl (id INT, items ARRAY<STRUCT<name: STRING, qty: INT>>) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1, array(named_struct('name','item1','qty',10)))")
  w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")
  w.sql("""MERGE INTO tbl t USING (
    SELECT * FROM VALUES (2, array(named_struct('name','item2','qty',20,'price',CAST(1.99 AS DECIMAL(3,2))))) AS s(id, items)
  ) s ON t.id = s.id
    WHEN NOT MATCHED THEN INSERT *""")
  w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = false")
  val t = w.table("tbl")
  w.read(t, name = "readAll")
  w.snapshot(t)
}

workload("mergeSchemaEvoMapValueType", "Merge with map column and schema evolution", "merge", "dml", "schema_evolution") { w =>
  w.sql("""CREATE TABLE tbl (id INT, labels MAP<STRING, STRING>) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1, map('env','prod')),(2, map('env','dev'))")
  w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")
  w.sql("""MERGE INTO tbl t USING (
    SELECT * FROM VALUES (2, map('env','staging'), 2),(3, map('env','test'), 3) AS s(id, labels, version)
  ) s ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *""")
  w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = false")
  val t = w.table("tbl")
  w.read(t, name = "readAll")
  w.read(t, predicate = "version IS NOT NULL", name = "readVersion")
  w.snapshot(t)
}

// =============================================================================
// Schema evolution - struct field operations
// =============================================================================

workload("mergeSchemaEvoStructAddField", "Merge adds field to existing struct", "merge", "dml", "schema_evolution") { w =>
  w.sql("""CREATE TABLE tbl (id INT, metadata STRUCT<key: STRING, val: INT>) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1, named_struct('key','k1','val',10))")
  w.sql("INSERT INTO tbl VALUES (2, named_struct('key','k2','val',20))")
  w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")
  w.sql("""MERGE INTO tbl t USING (
    SELECT * FROM VALUES
      (2, named_struct('key','k2','val',25,'source','api')),
      (3, named_struct('key','k3','val',30,'source','web'))
    AS s(id, metadata)
  ) s ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET metadata = s.metadata
    WHEN NOT MATCHED THEN INSERT *""")
  w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = false")
  val t = w.table("tbl")
  w.read(t, name = "readAll")
  w.snapshot(t)
}

workload("mergeSchemaEvoStructRemoveField", "Merge with struct missing field (null filled)", "merge", "dml", "schema_evolution") { w =>
  w.sql("""CREATE TABLE tbl (id INT, info STRUCT<name: STRING, age: INT, city: STRING>) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1, named_struct('name','alice','age',30,'city','NYC'))")
  w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")
  w.sql("""MERGE INTO tbl t USING (
    SELECT * FROM VALUES (2, named_struct('name','bob','age',25)) AS s(id, info)
  ) s ON t.id = s.id
    WHEN NOT MATCHED THEN INSERT *""")
  w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = false")
  val t = w.table("tbl")
  w.read(t, name = "readAll")
  w.snapshot(t)
}

workload("mergeSchemaEvoStructReorderFields", "Merge with reordered struct fields", "merge", "dml", "schema_evolution") { w =>
  w.sql("""CREATE TABLE tbl (id INT, info STRUCT<first: STRING, last: STRING>) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1, named_struct('first','Alice','last','Smith'))")
  w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")
  w.sql("""MERGE INTO tbl t USING (
    SELECT * FROM VALUES (2, named_struct('first','Bob','last','Jones','middle','M')) AS s(id, info)
  ) s ON t.id = s.id
    WHEN NOT MATCHED THEN INSERT *""")
  w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = false")
  val t = w.table("tbl")
  w.read(t, name = "readAll")
  w.snapshot(t)
}

workload("mergeSchemaEvoStructWithArray", "Struct containing array, add new field", "merge", "dml", "schema_evolution") { w =>
  w.sql("""CREATE TABLE tbl (id INT, data STRUCT<tags: ARRAY<STRING>, count: INT>) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1, named_struct('tags', array('a','b'), 'count', 2))")
  w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")
  w.sql("""MERGE INTO tbl t USING (
    SELECT * FROM VALUES (2, named_struct('tags', array('c'), 'count', 1, 'active', true)) AS s(id, data)
  ) s ON t.id = s.id
    WHEN NOT MATCHED THEN INSERT *""")
  w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = false")
  val t = w.table("tbl")
  w.read(t, name = "readAll")
  w.snapshot(t)
}

// =============================================================================
// Schema evolution - partitioned tables
// =============================================================================

workload("mergeSchemaEvoPartitionedAddCol", "Schema evolution on partitioned table", "merge", "dml", "schema_evolution", "partitioned") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value STRING, part STRING) USING delta
    PARTITIONED BY (part)
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'a','x'),(2,'b','y')")
  w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")
  w.sql("""MERGE INTO tbl t USING (
    SELECT * FROM VALUES (2,'x','y',10),(3,'c','z',20) AS s(id, value, part, newcol)
  ) s ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *""")
  w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = false")
  val t = w.table("tbl")
  w.read(t, name = "readAll")
  w.read(t, predicate = "newcol IS NOT NULL", name = "readNewCol")
  w.snapshot(t)
}

workload("mergeSchemaEvoPartitionedStructEvo", "Struct evolution on partitioned table", "merge", "dml", "schema_evolution", "partitioned") { w =>
  w.sql("""CREATE TABLE tbl (id INT, info STRUCT<name: STRING>, part STRING) USING delta
    PARTITIONED BY (part)
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1, named_struct('name','alice'), 'x')")
  w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")
  w.sql("""MERGE INTO tbl t USING (
    SELECT * FROM VALUES (2, named_struct('name','bob','email','bob@x.com'), 'y') AS s(id, info, part)
  ) s ON t.id = s.id
    WHEN NOT MATCHED THEN INSERT *""")
  w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = false")
  val t = w.table("tbl")
  w.read(t, name = "readAll")
  w.snapshot(t)
}

// =============================================================================
// Schema evolution - type widening and column mapping
// =============================================================================

workload("mergeSchemaEvoWidenType", "Widen int to long via merge", "merge", "dml", "schema_evolution", "type_widening") { w =>
  w.sql("""CREATE TABLE tbl (id INT, amount INT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,100),(2,200)")
  w.sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.enableTypeWidening' = 'true')")
  w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")
  w.sql("""MERGE INTO tbl t USING (
    SELECT * FROM VALUES (2, CAST(3000000000 AS BIGINT)),(3, CAST(4000000000 AS BIGINT)) AS s(id, amount)
  ) s ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET amount = s.amount
    WHEN NOT MATCHED THEN INSERT *""")
  w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = false")
  val t = w.table("tbl")
  w.read(t, name = "readAll")
  w.read(t, predicate = "amount > 2000000000", name = "readLargeAmount")
  w.snapshot(t)
}

workload("mergeSchemaEvoWithColumnMapping", "Schema evolution with column mapping", "merge", "dml", "schema_evolution", "column_mapping") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true',
      'delta.columnMapping.mode' = 'name')""")
  w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b')")
  w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")
  w.sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,'x',5),(3,'c',10) AS s(id, value, score)) s
    ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *""")
  w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = false")
  val t = w.table("tbl")
  w.read(t, name = "readAll")
  w.read(t, predicate = "score IS NOT NULL", name = "readScore")
  w.snapshot(t)
}

workload("mergeSchemaEvoDvSchemaEvo", "Schema evolution with deletion vectors", "merge", "dml", "schema_evolution", "dv") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
  w.sql("DELETE FROM tbl WHERE id = 3")
  w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")
  w.sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,'x',10),(4,'d',20) AS s(id, value, extra)) s
    ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *""")
  w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = false")
  val t = w.table("tbl")
  w.read(t, name = "readAll")
  w.read(t, predicate = "extra IS NOT NULL", name = "readExtra")
  w.snapshot(t)
}

// =============================================================================
// Schema evolution - error cases
// =============================================================================

workload("mergeSchemaEvoErrDuplicateCol", "Table state after duplicate column error", "merge", "dml", "schema_evolution", "error") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value STRING, extra_value STRING) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'a','ea'),(2,'b','eb')")
  w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")
  // Attempt merge that might cause duplicate column issue
  try {
    w.sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,'x','ex') AS s(id, value, extra_value)) s
      ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET *""")
  } catch { case _: Exception => /* may or may not fail */ }
  w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = false")
  val t = w.table("tbl")
  w.read(t, name = "readAll")
  w.snapshot(t)
}

workload("mergeSchemaEvoErrIncompatibleType", "Table state after incompatible type evolution error", "merge", "dml", "schema_evolution", "error") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b')")
  w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")
  try {
    w.sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2, ARRAY(1,2)) AS s(id, value)) s
      ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET value = s.value""")
  } catch { case _: Exception => /* expected: incompatible type */ }
  w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = false")
  val t = w.table("tbl")
  w.read(t, name = "readAll")
  w.snapshot(t)
}

workload("mergeSchemaEvoErrNarrowType", "Table state after narrowing type error", "merge", "dml", "schema_evolution", "error") { w =>
  w.sql("""CREATE TABLE tbl (id INT, amount BIGINT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,100),(2,200)")
  w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")
  try {
    w.sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2, CAST(50 AS INT)) AS s(id, amount)) s
      ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET amount = s.amount""")
  } catch { case _: Exception => /* may or may not fail depending on implicit cast */ }
  w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = false")
  val t = w.table("tbl")
  w.read(t, name = "readAll")
  w.snapshot(t)
}

// =============================================================================
// Struct evolution (deep nesting, null handling)
// =============================================================================

workload("mergeStructEvoNullNewField", "Merge adds new struct field, existing rows get null for new field", "merge", "dml", "struct_evolution") { w =>
  w.sql("""CREATE TABLE tbl (id INT, info STRUCT<a: INT>) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1, named_struct('a',10)),(2, named_struct('a',20))")
  w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")
  w.sql("""MERGE INTO tbl t USING (
    SELECT * FROM VALUES (2, named_struct('a',25,'b','new')),(3, named_struct('a',30,'b','also_new')) AS s(id, info)
  ) s ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET info = s.info
    WHEN NOT MATCHED THEN INSERT *""")
  w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = false")
  val t = w.table("tbl")
  w.read(t, name = "readAll")
  w.snapshot(t)
}

workload("mergeStructEvoNullableToNonNull", "Merge target has nullable struct field, source has non-null values", "merge", "dml", "struct_evolution") { w =>
  w.sql("""CREATE TABLE tbl (id INT, data STRUCT<x: INT>) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1, named_struct('x',10)),(2, NULL)")
  w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")
  w.sql("""MERGE INTO tbl t USING (
    SELECT * FROM VALUES (2, named_struct('x',20,'y','val')),(3, named_struct('x',30,'y','val2')) AS s(id, data)
  ) s ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET data = s.data
    WHEN NOT MATCHED THEN INSERT *""")
  w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = false")
  val t = w.table("tbl")
  w.read(t, name = "readAll")
  w.snapshot(t)
}

workload("mergeStructEvoNestedNullField", "Nested struct gets new nullable field via merge", "merge", "dml", "struct_evolution") { w =>
  w.sql("""CREATE TABLE tbl (id INT, outer_col STRUCT<inner: STRUCT<p: INT>>) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1, named_struct('inner', named_struct('p',10))),(2, named_struct('inner', named_struct('p',20)))")
  w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")
  w.sql("""MERGE INTO tbl t USING (
    SELECT * FROM VALUES (2, named_struct('inner', named_struct('p',25,'q','new'))),(3, named_struct('inner', named_struct('p',30,'q','also'))) AS s(id, outer_col)
  ) s ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET outer_col = s.outer_col
    WHEN NOT MATCHED THEN INSERT *""")
  w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = false")
  val t = w.table("tbl")
  w.read(t, name = "readAll")
  w.snapshot(t)
}

workload("mergeStructEvoMultiStructCols", "Table with multiple struct columns, merge evolves one", "merge", "dml", "struct_evolution") { w =>
  w.sql("""CREATE TABLE tbl (id INT, s1 STRUCT<a: INT>, s2 STRUCT<x: STRING>) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1, named_struct('a',1), named_struct('x','hello'))")
  w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")
  w.sql("""MERGE INTO tbl t USING (
    SELECT * FROM VALUES (1, named_struct('a',2,'b',3), named_struct('x','world')),(2, named_struct('a',4,'b',5), named_struct('x','new')) AS s(id, s1, s2)
  ) s ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET s1 = s.s1
    WHEN NOT MATCHED THEN INSERT *""")
  w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = false")
  val t = w.table("tbl")
  w.read(t, name = "readAll")
  w.snapshot(t)
}

workload("mergeStructEvoNullInKey", "Merge with null values in struct-type key column", "merge", "dml", "struct_evolution") { w =>
  w.sql("""CREATE TABLE tbl (key STRUCT<k1: INT, k2: STRING>, value INT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (named_struct('k1',1,'k2','a'), 10),(named_struct('k1',2,'k2',CAST(NULL AS STRING)), 20)")
  w.sql("""MERGE INTO tbl t USING (
    SELECT * FROM VALUES (named_struct('k1',1,'k2','a'), 99),(named_struct('k1',3,'k2','c'), 30) AS s(key, value)
  ) s ON t.key = s.key
    WHEN MATCHED THEN UPDATE SET value = s.value
    WHEN NOT MATCHED THEN INSERT *""")
  val t = w.table("tbl")
  w.read(t, name = "readAll")
  w.snapshot(t)
}

workload("mergeStructEvoArrayOfStructNull", "Array of structs with null fields after merge evolution", "merge", "dml", "struct_evolution") { w =>
  w.sql("""CREATE TABLE tbl (id INT, items ARRAY<STRUCT<a: INT>>) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1, array(named_struct('a',1)))")
  w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")
  w.sql("""MERGE INTO tbl t USING (
    SELECT * FROM VALUES (1, array(named_struct('a',2,'b','x'))),(2, array(named_struct('a',3,'b','y'))) AS s(id, items)
  ) s ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET items = s.items
    WHEN NOT MATCHED THEN INSERT *""")
  w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = false")
  val t = w.table("tbl")
  w.read(t, name = "readAll")
  w.snapshot(t)
}

workload("mergeStructEvoMapValueStructNull", "Map with struct values, merge adds nullable field to struct", "merge", "dml", "struct_evolution") { w =>
  w.sql("""CREATE TABLE tbl (id INT, props MAP<STRING, STRUCT<v: INT>>) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1, map('k1', named_struct('v',10)))")
  w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")
  w.sql("""MERGE INTO tbl t USING (
    SELECT * FROM VALUES (1, map('k1', named_struct('v',20,'extra',true))),(2, map('k2', named_struct('v',30,'extra',false))) AS s(id, props)
  ) s ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET props = s.props
    WHEN NOT MATCHED THEN INSERT *""")
  w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = false")
  val t = w.table("tbl")
  w.read(t, name = "readAll")
  w.snapshot(t)
}

workload("mergeStructEvoDeepNested", "Deeply nested struct (3 levels), add field at leaf level", "merge", "dml", "struct_evolution") { w =>
  w.sql("""CREATE TABLE tbl (id INT, deep STRUCT<l1: STRUCT<l2: STRUCT<val: INT>>>) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1, named_struct('l1', named_struct('l2', named_struct('val', 10))))")
  w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")
  w.sql("""MERGE INTO tbl t USING (
    SELECT * FROM VALUES (1, named_struct('l1', named_struct('l2', named_struct('val', 20, 'tag', 'x')))),
      (2, named_struct('l1', named_struct('l2', named_struct('val', 30, 'tag', 'y')))) AS s(id, deep)
  ) s ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET deep = s.deep
    WHEN NOT MATCHED THEN INSERT *""")
  w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = false")
  val t = w.table("tbl")
  w.read(t, name = "readAll")
  w.snapshot(t)
}

workload("mergeStructEvoMixedNull", "Merge where some matched rows get null, others get values for new field", "merge", "dml", "struct_evolution") { w =>
  w.sql("""CREATE TABLE tbl (id INT, info STRUCT<a: INT>) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1, named_struct('a',10)),(2, named_struct('a',20)),(3, named_struct('a',30))")
  w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")
  w.sql("""MERGE INTO tbl t USING (
    SELECT * FROM VALUES (1, named_struct('a',15,'b','has_b')),(3, named_struct('a',35,'b',CAST(NULL AS STRING))),(4, named_struct('a',40,'b','new')) AS s(id, info)
  ) s ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET info = s.info
    WHEN NOT MATCHED THEN INSERT *""")
  w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = false")
  val t = w.table("tbl")
  w.read(t, name = "readAll")
  w.snapshot(t)
}

workload("mergeStructEvoPartitionedStruct", "Struct evolution on partitioned table with null handling", "merge", "dml", "struct_evolution", "partitioned") { w =>
  w.sql("""CREATE TABLE tbl (region STRING, id INT, metrics STRUCT<score: INT>) USING delta
    PARTITIONED BY (region)
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES ('us',1,named_struct('score',80)),('eu',2,named_struct('score',90))")
  w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")
  w.sql("""MERGE INTO tbl t USING (
    SELECT * FROM VALUES ('us',1,named_struct('score',85,'grade','B')),('eu',3,named_struct('score',95,'grade','A')) AS s(region, id, metrics)
  ) s ON t.id = s.id AND t.region = s.region
    WHEN MATCHED THEN UPDATE SET metrics = s.metrics
    WHEN NOT MATCHED THEN INSERT *""")
  w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = false")
  val t = w.table("tbl")
  w.read(t, name = "readAll")
  w.snapshot(t)
}

// =============================================================================
// NOT MATCHED BY SOURCE (mrb_ prefix workloads)
// =============================================================================

workload("mrb_all_clause_types", "MERGE with all three clause types", "merge", "dml", "not_matched_by_source") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
  w.sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,'x'),(4,'d') AS s(id, value)) s
    ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET value = s.value
    WHEN NOT MATCHED THEN INSERT *
    WHEN NOT MATCHED BY SOURCE THEN DELETE""")
  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
}

workload("mrb_not_matched_by_source_delete", "MERGE with NOT MATCHED BY SOURCE delete", "merge", "dml", "not_matched_by_source") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
  w.sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,'x') AS s(id, value)) s
    ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET value = s.value
    WHEN NOT MATCHED BY SOURCE THEN DELETE""")
  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
}

workload("mrb_not_matched_by_source_update", "MERGE with NOT MATCHED BY SOURCE update", "merge", "dml", "not_matched_by_source") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
  w.sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,'x') AS s(id, value)) s
    ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET value = s.value
    WHEN NOT MATCHED BY SOURCE THEN UPDATE SET value = 'orphan'""")
  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
}

workload("mrb_with_cdf", "MERGE with CDF enabled", "merge", "dml", "not_matched_by_source", "cdf") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true',
      'delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
  w.sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,'x'),(4,'d') AS s(id, value)) s
    ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET value = s.value
    WHEN NOT MATCHED THEN INSERT *""")
  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
  w.cdf(t, startVersion = 0, endVersion = 2, name = "cdf_all")
  w.cdf(t, startVersion = 2, endVersion = 2, name = "cdf_merge")
}

workload("mrb_with_dv", "MERGE with deletion vectors", "merge", "dml", "not_matched_by_source", "dv") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
  w.sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,'x') AS s(id, value)) s
    ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET value = s.value
    WHEN NOT MATCHED BY SOURCE THEN DELETE""")
  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
}

workload("mrb_with_schema_evolution", "MERGE triggering auto schema evolution", "merge", "dml", "not_matched_by_source", "schema_evolution") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b')")
  w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")
  w.sql("""MERGE INTO tbl t USING (SELECT * FROM VALUES (2,'x',5),(3,'c',10) AS s(id, value, score)) s
    ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *""")
  w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = false")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "score IS NOT NULL", name = "read_evolved_cols")
  w.snapshot(t)
}

// =============================================================================
// Generate all workloads
// =============================================================================

generateAll(
  sys.env.getOrElse("WORKLOAD_OUTPUT_DIR", "/tmp/workloads"),
  force = sys.env.getOrElse("WORKLOAD_FORCE", "false").toBoolean)
System.exit(0)
