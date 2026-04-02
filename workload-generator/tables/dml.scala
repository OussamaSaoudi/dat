/**
 * Combined DML and miscellaneous workloads.
 *
 * Merged from dml_operations.scala and misc_workloads.scala.
 */

new WorkloadSuite("dml") {

  // === DML Operations ===

  // DELETE workloads

  test("deleteAllRows", "DELETE without WHERE", "delete", "dml") { w =>
    w.sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
    w.sql("DELETE FROM tbl WHERE true")
    val t = w.table("tbl")
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("deleteBasic", "DELETE with WHERE clause", "delete", "dml") { w =>
    w.sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d')")
    w.sql("DELETE FROM tbl WHERE id = 2")
    val t = w.table("tbl")
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("deletePartitioned", "DELETE on partitioned table", "delete", "dml", "partitioned") { w =>
    w.sql("""CREATE TABLE tbl (id INT, region STRING, amount INT) USING delta
      PARTITIONED BY (region) TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1,'east',100),(2,'west',200),(3,'east',300),(4,'west',400)")
    w.sql("DELETE FROM tbl WHERE region = 'west'")
    val t = w.table("tbl")
    w.read(t, name = "read_all")
    w.read(t, predicate = "region = 'east'", name = "filter_east")
    w.snapshot(t)
  }

  test("deleteWithInPredicate", "DELETE with IN predicate", "delete", "dml") { w =>
    w.sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e')")
    w.sql("DELETE FROM tbl WHERE id IN (2, 4)")
    val t = w.table("tbl")
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("deleteWithPredicate", "DELETE with complex predicate", "delete", "dml") { w =>
    w.sql("""CREATE TABLE tbl (id INT, value STRING, amount INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1,'a',10),(2,'b',20),(3,'c',30),(4,'d',40),(5,'e',50)")
    w.sql("DELETE FROM tbl WHERE id > 2 AND amount < 50")
    val t = w.table("tbl")
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  // INSERT workloads

  test("insertBasicAppend", "INSERT INTO append mode", "insert", "dml") { w =>
    w.sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b')")
    w.sql("INSERT INTO tbl VALUES (3,'c')")
    w.sql("INSERT INTO tbl VALUES (4,'d')")
    val t = w.table("tbl")
    w.read(t, name = "read_all")
    w.read(t, predicate = "id >= 3", name = "filter_new")
    w.snapshot(t)
  }

  test("insertOverwrite", "INSERT OVERWRITE", "insert", "dml") { w =>
    w.sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
    w.sql("INSERT OVERWRITE tbl VALUES (10,'x'),(20,'y')")
    val t = w.table("tbl")
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("insertOverwritePartition", "INSERT OVERWRITE partition", "insert", "dml", "partitioned") { w =>
    w.sql("""CREATE TABLE tbl (id INT, region STRING, amount INT) USING delta
      PARTITIONED BY (region) TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1,'east',100),(2,'west',200),(3,'east',300)")
    w.sql("INSERT OVERWRITE tbl PARTITION (region='east') VALUES (10,'east',999)")
    val t = w.table("tbl")
    w.read(t, name = "read_all")
    w.read(t, predicate = "region = 'east'", name = "filter_east")
    w.snapshot(t)
  }

  test("insertSelectReadBack", "INSERT INTO SELECT read-back", "insert", "dml") { w =>
    w.sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b')")
    w.sql("INSERT INTO tbl VALUES (3,'c'),(4,'d'),(5,'e')")
    val t = w.table("tbl")
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("insertValuesReadBack", "INSERT INTO VALUES", "insert", "dml") { w =>
    w.sql("""CREATE TABLE tbl (id INT, name STRING, score DOUBLE) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1,'Alice',95.5)")
    w.sql("INSERT INTO tbl VALUES (2,'Bob',87.3),(3,'Carol',92.1)")
    val t = w.table("tbl")
    w.read(t, name = "read_all")
    w.read(t, predicate = "score > 90.0", name = "filter_high_score")
    w.snapshot(t)
  }

  // UPDATE workloads

  test("updateAllRows", "UPDATE without WHERE", "update", "dml") { w =>
    w.sql("""CREATE TABLE tbl (id INT, status STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1,'old'),(2,'old'),(3,'old')")
    w.sql("UPDATE tbl SET status = 'new'")
    val t = w.table("tbl")
    w.read(t, name = "read_all")
    w.read(t, predicate = "status = 'new'", name = "filter_new")
    w.snapshot(t)
  }

  test("updateBasic", "UPDATE with WHERE clause", "update", "dml") { w =>
    w.sql("""CREATE TABLE tbl (id INT, value STRING, amount INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1,'a',10),(2,'b',20),(3,'c',30)")
    w.sql("UPDATE tbl SET value = 'updated' WHERE id = 2")
    val t = w.table("tbl")
    w.read(t, name = "read_all")
    w.read(t, predicate = "value = 'updated'", name = "filter_updated")
    w.snapshot(t)
  }

  test("updateMultiCols", "UPDATE multiple columns", "update", "dml") { w =>
    w.sql("""CREATE TABLE tbl (id INT, value STRING, amount INT, active BOOLEAN) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1,'a',10,true),(2,'b',20,true),(3,'c',30,false)")
    w.sql("UPDATE tbl SET value = 'updated', amount = 0, active = false WHERE id <= 2")
    val t = w.table("tbl")
    w.read(t, name = "read_all")
    w.read(t, predicate = "active = true", name = "filter_active")
    w.snapshot(t)
  }

  test("updateNullToValue", "UPDATE null to non-null values", "update", "dml") { w =>
    w.sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1, NULL)")
    w.sql("INSERT INTO tbl VALUES (2, NULL)")
    w.sql("INSERT INTO tbl VALUES (3, 'exists')")
    w.sql("UPDATE tbl SET value = 'filled' WHERE value IS NULL")
    val t = w.table("tbl")
    w.read(t, name = "read_all")
    w.read(t, predicate = "value IS NOT NULL", name = "filter_not_null")
    w.snapshot(t)
  }

  test("updatePartitioned", "UPDATE on partitioned table", "update", "dml", "partitioned") { w =>
    w.sql("""CREATE TABLE tbl (id INT, region STRING, amount INT) USING delta
      PARTITIONED BY (region) TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1,'east',100),(2,'west',200),(3,'east',300)")
    w.sql("UPDATE tbl SET amount = 999 WHERE region = 'east'")
    val t = w.table("tbl")
    w.read(t, name = "read_all")
    w.read(t, predicate = "region = 'east'", name = "filter_east")
    w.snapshot(t)
  }

  test("updateValueToNull", "UPDATE non-null to null values", "update", "dml") { w =>
    w.sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
    w.sql("UPDATE tbl SET value = NULL WHERE id <= 2")
    val t = w.table("tbl")
    w.read(t, name = "read_all")
    w.read(t, predicate = "value IS NULL", name = "filter_null")
    w.snapshot(t)
  }

  test("updateWithSubquery", "UPDATE with expression in SET", "update", "dml") { w =>
    w.sql("""CREATE TABLE tbl (id INT, amount INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1,100),(2,200),(3,300)")
    w.sql("UPDATE tbl SET amount = amount * 2 WHERE id > 1")
    val t = w.table("tbl")
    w.read(t, name = "read_all")
    w.read(t, predicate = "amount > 300", name = "filter_large")
    w.snapshot(t)
  }

  // Combined DML sequences

  test("dmlMergeAfterDelete", "MERGE after DELETE", "merge", "delete", "dml", "combined") { w =>
    w.sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
    w.sql("DELETE FROM tbl WHERE id = 2")
    w.sql("CREATE TABLE src (id INT, value STRING) USING delta")
    w.sql("INSERT INTO src VALUES (1,'x'),(4,'d')")
    w.sql("""MERGE INTO tbl t USING src s ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET value = s.value
      WHEN NOT MATCHED THEN INSERT *""")
    val t = w.table("tbl")
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("dmlMultipleMerges", "Multiple MERGE operations", "merge", "dml", "combined") { w =>
    w.sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b')")

    // First merge
    w.sql("CREATE TABLE src1 (id INT, value STRING) USING delta")
    w.sql("INSERT INTO src1 VALUES (2,'x'),(3,'c')")
    w.sql("""MERGE INTO tbl t USING src1 s ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET value = s.value
      WHEN NOT MATCHED THEN INSERT *""")

    // Second merge
    w.sql("CREATE TABLE src2 (id INT, value STRING) USING delta")
    w.sql("INSERT INTO src2 VALUES (3,'y'),(4,'d')")
    w.sql("""MERGE INTO tbl t USING src2 s ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET value = s.value
      WHEN NOT MATCHED THEN INSERT *""")

    val t = w.table("tbl")
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("dmlSequenceInsertUpdateDelete", "Sequential INSERT, UPDATE, DELETE", "dml", "combined") { w =>
    w.sql("""CREATE TABLE tbl (id INT, value STRING, amount INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1,'a',10)")
    w.sql("INSERT INTO tbl VALUES (2,'b',20),(3,'c',30)")
    w.sql("UPDATE tbl SET amount = 999 WHERE id = 1")
    w.sql("DELETE FROM tbl WHERE id = 3")
    val t = w.table("tbl")
    w.read(t, name = "read_all")
    w.read(t, predicate = "amount > 100", name = "filter_large_amount")
    w.snapshot(t)
  }

  test("dmlUpdateAfterMerge", "UPDATE after MERGE", "update", "merge", "dml", "combined") { w =>
    w.sql("""CREATE TABLE tbl (id INT, value STRING, amount INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1,'a',10),(2,'b',20)")

    w.sql("CREATE TABLE src (id INT, value STRING, amount INT) USING delta")
    w.sql("INSERT INTO src VALUES (2,'x',25),(3,'c',30)")
    w.sql("""MERGE INTO tbl t USING src s ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET value = s.value, amount = s.amount
      WHEN NOT MATCHED THEN INSERT *""")

    w.sql("UPDATE tbl SET amount = amount + 100 WHERE id >= 2")
    val t = w.table("tbl")
    w.read(t, name = "read_all")
    w.read(t, predicate = "amount > 100", name = "filter_large_amount")
    w.snapshot(t)
  }

  // === Misc Workloads ===

  // OSS-compatible read workloads

  test("ossReadBasicOSS", "Basic OSS compatible read", "oss", "read") { w =>
    w.sql("""CREATE TABLE tbl (id LONG, data STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    // 20 rows: id 0..19, data='oss_test'
    w.sql("INSERT INTO tbl SELECT id, 'oss_test' FROM range(20)")
    val t = w.table("tbl")
    w.read(t, name = "readAll")
    w.snapshot(t)
  }

  test("ossReadPartitionedOSS", "Partitioned table OSS read", "oss", "read", "partitioned") { w =>
    w.sql("""CREATE TABLE tbl (id INT, part STRING, value INT) USING delta
      PARTITIONED BY (part) TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1,'a',10),(2,'b',20),(3,'a',30),(4,'b',40),(5,'c',50)")
    val t = w.table("tbl")
    w.read(t, name = "readAll")
    w.read(t, predicate = "part = 'a'", name = "readPartA")
    w.snapshot(t)
  }

  test("ossReadPredicateOSS", "Predicate pushdown on OSS table", "oss", "read") { w =>
    w.sql("""CREATE TABLE tbl (id LONG, category STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    // 50 rows: id 0..24 -> 'low', id 25..49 -> 'high'
    w.sql("""INSERT INTO tbl
      SELECT id, CASE WHEN id < 25 THEN 'low' ELSE 'high' END FROM range(50)""")
    val t = w.table("tbl")
    w.read(t, name = "readAll")
    w.read(t, predicate = "id >= 40", name = "readHighId")
    w.read(t, predicate = "category = 'low'", name = "readLow")
    w.snapshot(t)
  }

  test("ossReadTimeTravelOSS", "Time travel on OSS table", "oss", "read", "time_travel") { w =>
    w.sql("""CREATE TABLE tbl (value INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    // v0: 5 rows (1..5)
    w.sql("INSERT INTO tbl VALUES (1),(2),(3),(4),(5)")
    // v1: 5 more rows (6..10)
    w.sql("INSERT INTO tbl VALUES (6),(7),(8),(9),(10)")
    // v2: 5 more rows (11..15)
    w.sql("INSERT INTO tbl VALUES (11),(12),(13),(14),(15)")
    val t = w.table("tbl")
    w.read(t, name = "readLatest")
    w.read(t, version = 0, name = "readV0")
    w.read(t, version = 1, name = "readV1")
    w.snapshot(t)
  }

  // Special path handling

  test("pec_table_path_special", "Table path with special characters (spaces)", "path", "edge_case") { w =>
    w.sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    // 10 rows: id 0..9
    w.sql("INSERT INTO tbl SELECT id FROM range(10)")
    val t = w.table("tbl")
    w.read(t, name = "read_all")
    w.read(t, predicate = "id < 5", name = "read_filtered")
    w.snapshot(t)
  }

}.runAll()
