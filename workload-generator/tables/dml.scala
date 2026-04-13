/**
 * Combined DML and miscellaneous workloads.
 *
 * Merged from dml_operations.scala and misc_workloads.scala.
 */

new WorkloadSuite("dml") {

  // === DML Operations ===

  // DELETE workloads

  test("deleteAllRows", "DELETE without WHERE", "delete", "dml") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
    sql("DELETE FROM tbl WHERE true")
    val t = registerTable("tbl")
    read(t, name = "read_all")
    snapshot(t)
  }

  test("deleteBasic", "DELETE with WHERE clause", "delete", "dml") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d')")
    sql("DELETE FROM tbl WHERE id = 2")
    val t = registerTable("tbl")
    read(t, name = "read_all")
    snapshot(t)
  }

  test("deletePartitioned", "DELETE on partitioned table", "delete", "dml", "partitioned") {
    sql("""CREATE TABLE tbl (id INT, region STRING, amount INT) USING delta
      PARTITIONED BY (region) TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'east',100),(2,'west',200),(3,'east',300),(4,'west',400)")
    sql("DELETE FROM tbl WHERE region = 'west'")
    val t = registerTable("tbl")
    read(t, name = "read_all")
    read(t, predicate = "region = 'east'", name = "filter_east")
    snapshot(t)
  }

  test("deleteWithInPredicate", "DELETE with IN predicate", "delete", "dml") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e')")
    sql("DELETE FROM tbl WHERE id IN (2, 4)")
    val t = registerTable("tbl")
    read(t, name = "read_all")
    snapshot(t)
  }

  test("deleteWithPredicate", "DELETE with complex predicate", "delete", "dml") {
    sql("""CREATE TABLE tbl (id INT, value STRING, amount INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a',10),(2,'b',20),(3,'c',30),(4,'d',40),(5,'e',50)")
    sql("DELETE FROM tbl WHERE id > 2 AND amount < 50")
    val t = registerTable("tbl")
    read(t, name = "read_all")
    snapshot(t)
  }

  // INSERT workloads

  test("insertBasicAppend", "INSERT INTO append mode", "insert", "dml") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b')")
    sql("INSERT INTO tbl VALUES (3,'c')")
    sql("INSERT INTO tbl VALUES (4,'d')")
    val t = registerTable("tbl")
    read(t, name = "read_all")
    read(t, predicate = "id >= 3", name = "filter_new")
    snapshot(t)
  }

  test("insertOverwrite", "INSERT OVERWRITE", "insert", "dml") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
    sql("INSERT OVERWRITE tbl VALUES (10,'x'),(20,'y')")
    val t = registerTable("tbl")
    read(t, name = "read_all")
    snapshot(t)
  }

  test("insertOverwritePartition", "INSERT OVERWRITE partition", "insert", "dml", "partitioned") {
    sql("""CREATE TABLE tbl (id INT, region STRING, amount INT) USING delta
      PARTITIONED BY (region) TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'east',100),(2,'west',200),(3,'east',300)")
    sql("INSERT OVERWRITE tbl PARTITION (region='east') VALUES (10,'east',999)")
    val t = registerTable("tbl")
    read(t, name = "read_all")
    read(t, predicate = "region = 'east'", name = "filter_east")
    snapshot(t)
  }

  test("insertSelectReadBack", "INSERT INTO SELECT read-back", "insert", "dml") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b')")
    sql("INSERT INTO tbl VALUES (3,'c'),(4,'d'),(5,'e')")
    val t = registerTable("tbl")
    read(t, name = "read_all")
    snapshot(t)
  }

  test("insertValuesReadBack", "INSERT INTO VALUES", "insert", "dml") {
    sql("""CREATE TABLE tbl (id INT, name STRING, score DOUBLE) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'Alice',95.5)")
    sql("INSERT INTO tbl VALUES (2,'Bob',87.3),(3,'Carol',92.1)")
    val t = registerTable("tbl")
    read(t, name = "read_all")
    read(t, predicate = "score > 90.0", name = "filter_high_score")
    snapshot(t)
  }

  // UPDATE workloads

  test("updateAllRows", "UPDATE without WHERE", "update", "dml") {
    sql("""CREATE TABLE tbl (id INT, status STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'old'),(2,'old'),(3,'old')")
    sql("UPDATE tbl SET status = 'new'")
    val t = registerTable("tbl")
    read(t, name = "read_all")
    read(t, predicate = "status = 'new'", name = "filter_new")
    snapshot(t)
  }

  test("updateBasic", "UPDATE with WHERE clause", "update", "dml") {
    sql("""CREATE TABLE tbl (id INT, value STRING, amount INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a',10),(2,'b',20),(3,'c',30)")
    sql("UPDATE tbl SET value = 'updated' WHERE id = 2")
    val t = registerTable("tbl")
    read(t, name = "read_all")
    read(t, predicate = "value = 'updated'", name = "filter_updated")
    snapshot(t)
  }

  test("updateMultiCols", "UPDATE multiple columns", "update", "dml") {
    sql("""CREATE TABLE tbl (id INT, value STRING, amount INT, active BOOLEAN) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a',10,true),(2,'b',20,true),(3,'c',30,false)")
    sql("UPDATE tbl SET value = 'updated', amount = 0, active = false WHERE id <= 2")
    val t = registerTable("tbl")
    read(t, name = "read_all")
    read(t, predicate = "active = true", name = "filter_active")
    snapshot(t)
  }

  test("updateNullToValue", "UPDATE null to non-null values", "update", "dml") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, NULL)")
    sql("INSERT INTO tbl VALUES (2, NULL)")
    sql("INSERT INTO tbl VALUES (3, 'exists')")
    sql("UPDATE tbl SET value = 'filled' WHERE value IS NULL")
    val t = registerTable("tbl")
    read(t, name = "read_all")
    read(t, predicate = "value IS NOT NULL", name = "filter_not_null")
    snapshot(t)
  }

  test("updatePartitioned", "UPDATE on partitioned table", "update", "dml", "partitioned") {
    sql("""CREATE TABLE tbl (id INT, region STRING, amount INT) USING delta
      PARTITIONED BY (region) TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'east',100),(2,'west',200),(3,'east',300)")
    sql("UPDATE tbl SET amount = 999 WHERE region = 'east'")
    val t = registerTable("tbl")
    read(t, name = "read_all")
    read(t, predicate = "region = 'east'", name = "filter_east")
    snapshot(t)
  }

  test("updateValueToNull", "UPDATE non-null to null values", "update", "dml") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
    sql("UPDATE tbl SET value = NULL WHERE id <= 2")
    val t = registerTable("tbl")
    read(t, name = "read_all")
    read(t, predicate = "value IS NULL", name = "filter_null")
    snapshot(t)
  }

  test("updateWithSubquery", "UPDATE with expression in SET", "update", "dml") {
    sql("""CREATE TABLE tbl (id INT, amount INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,100),(2,200),(3,300)")
    sql("UPDATE tbl SET amount = amount * 2 WHERE id > 1")
    val t = registerTable("tbl")
    read(t, name = "read_all")
    read(t, predicate = "amount > 300", name = "filter_large")
    snapshot(t)
  }

  // Combined DML sequences

  test("dmlMergeAfterDelete", "MERGE after DELETE", "merge", "delete", "dml", "combined") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
    sql("DELETE FROM tbl WHERE id = 2")
    sql("CREATE TABLE src (id INT, value STRING) USING delta")
    sql("INSERT INTO src VALUES (1,'x'),(4,'d')")
    sql("""MERGE INTO tbl t USING src s ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET value = s.value
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerTable("tbl")
    read(t, name = "read_all")
    snapshot(t)
  }

  test("dmlMultipleMerges", "Multiple MERGE operations", "merge", "dml", "combined") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b')")

    // First merge
    sql("CREATE TABLE src1 (id INT, value STRING) USING delta")
    sql("INSERT INTO src1 VALUES (2,'x'),(3,'c')")
    sql("""MERGE INTO tbl t USING src1 s ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET value = s.value
      WHEN NOT MATCHED THEN INSERT *""")

    // Second merge
    sql("CREATE TABLE src2 (id INT, value STRING) USING delta")
    sql("INSERT INTO src2 VALUES (3,'y'),(4,'d')")
    sql("""MERGE INTO tbl t USING src2 s ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET value = s.value
      WHEN NOT MATCHED THEN INSERT *""")

    val t = registerTable("tbl")
    read(t, name = "read_all")
    snapshot(t)
  }

  test("dmlSequenceInsertUpdateDelete", "Sequential INSERT, UPDATE, DELETE", "dml", "combined") {
    sql("""CREATE TABLE tbl (id INT, value STRING, amount INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a',10)")
    sql("INSERT INTO tbl VALUES (2,'b',20),(3,'c',30)")
    sql("UPDATE tbl SET amount = 999 WHERE id = 1")
    sql("DELETE FROM tbl WHERE id = 3")
    val t = registerTable("tbl")
    read(t, name = "read_all")
    read(t, predicate = "amount > 100", name = "filter_large_amount")
    snapshot(t)
  }

  test("dmlUpdateAfterMerge", "UPDATE after MERGE", "update", "merge", "dml", "combined") {
    sql("""CREATE TABLE tbl (id INT, value STRING, amount INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a',10),(2,'b',20)")

    sql("CREATE TABLE src (id INT, value STRING, amount INT) USING delta")
    sql("INSERT INTO src VALUES (2,'x',25),(3,'c',30)")
    sql("""MERGE INTO tbl t USING src s ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET value = s.value, amount = s.amount
      WHEN NOT MATCHED THEN INSERT *""")

    sql("UPDATE tbl SET amount = amount + 100 WHERE id >= 2")
    val t = registerTable("tbl")
    read(t, name = "read_all")
    read(t, predicate = "amount > 100", name = "filter_large_amount")
    snapshot(t)
  }

  // === Misc Workloads ===

  // OSS-compatible read workloads

  test("ossReadBasicOSS", "Basic OSS compatible read", "oss", "read") {
    sql("""CREATE TABLE tbl (id LONG, data STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    // 20 rows: id 0..19, data='oss_test'
    sql("INSERT INTO tbl SELECT id, 'oss_test' FROM range(20)")
    val t = registerTable("tbl")
    read(t, name = "readAll")
    snapshot(t)
  }

  test("ossReadPartitionedOSS", "Partitioned table OSS read", "oss", "read", "partitioned") {
    sql("""CREATE TABLE tbl (id INT, part STRING, value INT) USING delta
      PARTITIONED BY (part) TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a',10),(2,'b',20),(3,'a',30),(4,'b',40),(5,'c',50)")
    val t = registerTable("tbl")
    read(t, name = "readAll")
    read(t, predicate = "part = 'a'", name = "readPartA")
    snapshot(t)
  }

  test("ossReadPredicateOSS", "Predicate pushdown on OSS table", "oss", "read") {
    sql("""CREATE TABLE tbl (id LONG, category STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    // 50 rows: id 0..24 -> 'low', id 25..49 -> 'high'
    sql("""INSERT INTO tbl
      SELECT id, CASE WHEN id < 25 THEN 'low' ELSE 'high' END FROM range(50)""")
    val t = registerTable("tbl")
    read(t, name = "readAll")
    read(t, predicate = "id >= 40", name = "readHighId")
    read(t, predicate = "category = 'low'", name = "readLow")
    snapshot(t)
  }

  test("ossReadTimeTravelOSS", "Time travel on OSS table", "oss", "read", "time_travel") {
    sql("""CREATE TABLE tbl (value INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    // v0: 5 rows (1..5)
    sql("INSERT INTO tbl VALUES (1),(2),(3),(4),(5)")
    // v1: 5 more rows (6..10)
    sql("INSERT INTO tbl VALUES (6),(7),(8),(9),(10)")
    // v2: 5 more rows (11..15)
    sql("INSERT INTO tbl VALUES (11),(12),(13),(14),(15)")
    val t = registerTable("tbl")
    read(t, name = "readLatest")
    read(t, version = 0, name = "readV0")
    read(t, version = 1, name = "readV1")
    snapshot(t)
  }

  // Special path handling

  test("pec_table_path_special", "Table path with special characters (spaces)", "path", "edge_case") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    // 10 rows: id 0..9
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    val t = registerTable("tbl")
    read(t, name = "read_all")
    read(t, predicate = "id < 5", name = "read_filtered")
    snapshot(t)
  }

}
