/**
 * DML operation workloads: DELETE, INSERT, UPDATE, MERGE sequences.
 *
 * Each workload matches an existing acceptance_workloads directory by name.
 * All tables use DVs enabled to match the original captures.
 *
 * Run: ./bin/generate-workload.sh tables/dml_operations.scala
 */
import io.delta.workload.WorkloadGenerator._

// =============================================================================
// DELETE workloads
// =============================================================================

workload("deleteAllRows", "DELETE without WHERE", "delete", "dml") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
  w.sql("DELETE FROM tbl WHERE true")
  val t = w.table("tbl")
  w.read(t, name = "read_all")
  w.snapshot(t)
}

workload("deleteBasic", "DELETE with WHERE clause", "delete", "dml") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d')")
  w.sql("DELETE FROM tbl WHERE id = 2")
  val t = w.table("tbl")
  w.read(t, name = "read_all")
  w.snapshot(t)
}

workload("deletePartitioned", "DELETE on partitioned table", "delete", "dml", "partitioned") { w =>
  w.sql("""CREATE TABLE tbl (id INT, region STRING, amount INT) USING delta
    PARTITIONED BY (region) TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'east',100),(2,'west',200),(3,'east',300),(4,'west',400)")
  w.sql("DELETE FROM tbl WHERE region = 'west'")
  val t = w.table("tbl")
  w.read(t, name = "read_all")
  w.read(t, predicate = "region = 'east'", name = "filter_east")
  w.snapshot(t)
}

workload("deleteWithInPredicate", "DELETE with IN predicate", "delete", "dml") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e')")
  w.sql("DELETE FROM tbl WHERE id IN (2, 4)")
  val t = w.table("tbl")
  w.read(t, name = "read_all")
  w.snapshot(t)
}

workload("deleteWithPredicate", "DELETE with complex predicate", "delete", "dml") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value STRING, amount INT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'a',10),(2,'b',20),(3,'c',30),(4,'d',40),(5,'e',50)")
  w.sql("DELETE FROM tbl WHERE id > 2 AND amount < 50")
  val t = w.table("tbl")
  w.read(t, name = "read_all")
  w.snapshot(t)
}

// =============================================================================
// INSERT workloads
// =============================================================================

workload("insertBasicAppend", "INSERT INTO append mode", "insert", "dml") { w =>
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

workload("insertOverwrite", "INSERT OVERWRITE", "insert", "dml") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
  w.sql("INSERT OVERWRITE tbl VALUES (10,'x'),(20,'y')")
  val t = w.table("tbl")
  w.read(t, name = "read_all")
  w.snapshot(t)
}

workload("insertOverwritePartition", "INSERT OVERWRITE partition", "insert", "dml", "partitioned") { w =>
  w.sql("""CREATE TABLE tbl (id INT, region STRING, amount INT) USING delta
    PARTITIONED BY (region) TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'east',100),(2,'west',200),(3,'east',300)")
  w.sql("INSERT OVERWRITE tbl PARTITION (region='east') VALUES (10,'east',999)")
  val t = w.table("tbl")
  w.read(t, name = "read_all")
  w.read(t, predicate = "region = 'east'", name = "filter_east")
  w.snapshot(t)
}

workload("insertSelectReadBack", "INSERT INTO SELECT read-back", "insert", "dml") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b')")
  w.sql("INSERT INTO tbl VALUES (3,'c'),(4,'d'),(5,'e')")
  val t = w.table("tbl")
  w.read(t, name = "read_all")
  w.snapshot(t)
}

workload("insertValuesReadBack", "INSERT INTO VALUES", "insert", "dml") { w =>
  w.sql("""CREATE TABLE tbl (id INT, name STRING, score DOUBLE) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'Alice',95.5)")
  w.sql("INSERT INTO tbl VALUES (2,'Bob',87.3),(3,'Carol',92.1)")
  val t = w.table("tbl")
  w.read(t, name = "read_all")
  w.read(t, predicate = "score > 90.0", name = "filter_high_score")
  w.snapshot(t)
}

// =============================================================================
// UPDATE workloads
// =============================================================================

workload("updateAllRows", "UPDATE without WHERE", "update", "dml") { w =>
  w.sql("""CREATE TABLE tbl (id INT, status STRING) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'old'),(2,'old'),(3,'old')")
  w.sql("UPDATE tbl SET status = 'new'")
  val t = w.table("tbl")
  w.read(t, name = "read_all")
  w.read(t, predicate = "status = 'new'", name = "filter_new")
  w.snapshot(t)
}

workload("updateBasic", "UPDATE with WHERE clause", "update", "dml") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value STRING, amount INT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'a',10),(2,'b',20),(3,'c',30)")
  w.sql("UPDATE tbl SET value = 'updated' WHERE id = 2")
  val t = w.table("tbl")
  w.read(t, name = "read_all")
  w.read(t, predicate = "value = 'updated'", name = "filter_updated")
  w.snapshot(t)
}

workload("updateMultiCols", "UPDATE multiple columns", "update", "dml") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value STRING, amount INT, active BOOLEAN) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'a',10,true),(2,'b',20,true),(3,'c',30,false)")
  w.sql("UPDATE tbl SET value = 'updated', amount = 0, active = false WHERE id <= 2")
  val t = w.table("tbl")
  w.read(t, name = "read_all")
  w.read(t, predicate = "active = true", name = "filter_active")
  w.snapshot(t)
}

workload("updateNullToValue", "UPDATE null to non-null values", "update", "dml") { w =>
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

workload("updatePartitioned", "UPDATE on partitioned table", "update", "dml", "partitioned") { w =>
  w.sql("""CREATE TABLE tbl (id INT, region STRING, amount INT) USING delta
    PARTITIONED BY (region) TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'east',100),(2,'west',200),(3,'east',300)")
  w.sql("UPDATE tbl SET amount = 999 WHERE region = 'east'")
  val t = w.table("tbl")
  w.read(t, name = "read_all")
  w.read(t, predicate = "region = 'east'", name = "filter_east")
  w.snapshot(t)
}

workload("updateValueToNull", "UPDATE non-null to null values", "update", "dml") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
  w.sql("UPDATE tbl SET value = NULL WHERE id <= 2")
  val t = w.table("tbl")
  w.read(t, name = "read_all")
  w.read(t, predicate = "value IS NULL", name = "filter_null")
  w.snapshot(t)
}

workload("updateWithSubquery", "UPDATE with expression in SET", "update", "dml") { w =>
  w.sql("""CREATE TABLE tbl (id INT, amount INT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,100),(2,200),(3,300)")
  w.sql("UPDATE tbl SET amount = amount * 2 WHERE id > 1")
  val t = w.table("tbl")
  w.read(t, name = "read_all")
  w.read(t, predicate = "amount > 300", name = "filter_large")
  w.snapshot(t)
}

// =============================================================================
// Combined DML sequences
// =============================================================================

workload("dmlMergeAfterDelete", "MERGE after DELETE", "merge", "delete", "dml", "combined") { w =>
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

workload("dmlMultipleMerges", "Multiple MERGE operations", "merge", "dml", "combined") { w =>
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

workload("dmlSequenceInsertUpdateDelete", "Sequential INSERT, UPDATE, DELETE", "dml", "combined") { w =>
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

workload("dmlUpdateAfterMerge", "UPDATE after MERGE", "update", "merge", "dml", "combined") { w =>
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

generateAll(
  sys.env.getOrElse("WORKLOAD_OUTPUT_DIR", "/tmp/workloads"),
  force = sys.env.getOrElse("WORKLOAD_FORCE", "false").toBoolean)
System.exit(0)
