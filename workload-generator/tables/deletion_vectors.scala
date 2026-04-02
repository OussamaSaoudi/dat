import io.delta.workload.WorkloadGenerator._

workload("dv_basic_delete", "DVs after DELETE", "dv") { w =>
  w.sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e')")
  w.sql("DELETE FROM tbl WHERE id IN (2, 4)")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, version = 1)
  w.read(t, predicate = "id > 3")
  w.snapshot(t)
  w.snapshot(t, version = 1)
}

workload("dv_large_table", "DVs on 2000-row table", "dv") { w =>
  w.sql("""CREATE TABLE tbl (value INT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl SELECT CAST(id AS INT) FROM range(2000)")
  w.sql("DELETE FROM tbl WHERE value IN (0, 180, 300, 700, 1800)")
  w.sql("INSERT INTO tbl VALUES (300), (700)")
  w.sql("DELETE FROM tbl WHERE value IN (300, 250, 350, 900, 1353, 1567, 1800)")
  w.sql("INSERT INTO tbl VALUES (900), (1567)")
  val t = w.table("tbl")
  w.read(t, version = 0)
  w.read(t, version = 1)
  w.read(t, version = 2)
  w.read(t, version = 3)
  w.read(t, version = 4)
  w.snapshot(t)
}

workload("dv_partitioned", "DVs on partitioned table", "dv") { w =>
  w.sql("""CREATE TABLE tbl (id INT, partCol INT) USING delta
    PARTITIONED BY (partCol) TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl SELECT CAST(id AS INT), CAST(id % 10 AS INT) FROM range(200)")
  w.sql("DELETE FROM tbl WHERE id IN (0, 18, 30, 75, 100, 150)")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, version = 1)
  w.read(t, predicate = "partCol = 3")
  w.read(t, predicate = "partCol = 3 AND id > 25")
  w.snapshot(t)
}

workload("dv_all_deleted", "All rows deleted via DVs", "dv") { w =>
  w.sql("""CREATE TABLE tbl (id INT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1),(2),(3),(4),(5)")
  w.sql("DELETE FROM tbl WHERE true")
  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
}

workload("dv_multiple_deletes", "Multiple DELETE operations on same file", "dv") { w =>
  w.sql("""CREATE TABLE tbl (id INT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1),(2),(3),(4),(5),(6),(7),(8),(9),(10)")
  w.sql("DELETE FROM tbl WHERE id = 1")
  w.sql("DELETE FROM tbl WHERE id = 3")
  w.sql("DELETE FROM tbl WHERE id = 5")
  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
}

workload("dv_with_column_mapping", "DVs + column mapping", "dv", "column_mapping") { w =>
  w.sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true',
      'delta.columnMapping.mode' = 'name', 'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
  w.sql("INSERT INTO tbl VALUES (1,'alice'),(2,'bob'),(3,'charlie'),(4,'diana')")
  w.sql("ALTER TABLE tbl RENAME COLUMN name TO full_name")
  w.sql("INSERT INTO tbl VALUES (5, 'eve')")
  w.sql("DELETE FROM tbl WHERE id IN (2, 4)")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, columns = Seq("id", "full_name"))
  w.read(t, predicate = "id > 2")
  w.snapshot(t)
}

workload("dv_no_dvs", "DV feature enabled but no DVs produced", "dv") { w =>
  w.sql("""CREATE TABLE tbl (id LONG) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1)")
  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
}

workload("dv_with_checkpoint", "DVs + checkpoint", "dv", "checkpoint") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
  w.sql("INSERT INTO tbl VALUES (4,'d'),(5,'e'),(6,'f')")
  w.sql("DELETE FROM tbl WHERE id IN (2, 5)")
  val loc = w.spark.sql("DESCRIBE DETAIL tbl").collect()(0).getAs[String]("location")
  org.apache.spark.sql.delta.DeltaLog.forTable(w.spark, loc).checkpoint()
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "id > 3")
  w.snapshot(t)
}

workload("dv_insert_after_delete", "Insert after DV delete", "dv") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e')")
  w.sql("DELETE FROM tbl WHERE id IN (2, 4)")
  w.sql("INSERT INTO tbl VALUES (6,'f'),(7,'g'),(8,'h')")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "id > 5")
  w.read(t, predicate = "id <= 5")
  w.snapshot(t)
}

// --- New workloads below ---

workload("dv_with_merge", "DVs produced by MERGE", "dv", "merge") { w =>
  w.sql("""CREATE TABLE target (id INT, value STRING) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO target VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d')")
  w.sql("CREATE TABLE src (id INT, value STRING) USING delta")
  w.sql("INSERT INTO src VALUES (2,'B_updated'),(5,'e_new')")
  w.sql("""MERGE INTO target t USING src s ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET value = s.value
    WHEN NOT MATCHED THEN INSERT *""")
  val t = w.table("target")
  w.read(t)
  w.read(t, version = 1)
  w.read(t, predicate = "id > 3")
  w.snapshot(t)
}

workload("dv_with_update", "DVs produced by UPDATE", "dv") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value INT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,10),(2,20),(3,30),(4,40),(5,50)")
  w.sql("UPDATE tbl SET value = value * 100 WHERE id IN (2, 4)")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, version = 1)
  w.read(t, predicate = "value > 100")
  w.read(t, predicate = "value <= 50")
  w.snapshot(t)
}

workload("dv_column_mapping_id", "DVs + column mapping id mode + partitioned", "dv", "column_mapping") { w =>
  w.sql("""CREATE TABLE tbl (id INT, category STRING, value STRING) USING delta
    PARTITIONED BY (category)
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true',
      'delta.columnMapping.mode' = 'id', 'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
  w.sql("INSERT INTO tbl VALUES (1,'fruit','apple'),(2,'fruit','banana'),(3,'veggie','carrot'),(4,'veggie','daikon')")
  w.sql("DELETE FROM tbl WHERE id IN (2, 3)")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "category = 'fruit'")
  w.read(t, predicate = "category = 'veggie'")
  w.snapshot(t)
}

workload("dv_partition_pruning_combined", "DV + partition pruning with complex predicates", "dv") { w =>
  w.sql("""CREATE TABLE tbl (id INT, part STRING, value INT) USING delta
    PARTITIONED BY (part) TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'A',10),(2,'A',20),(3,'B',30),(4,'B',40),(5,'C',50),(6,'C',60)")
  w.sql("DELETE FROM tbl WHERE id IN (2, 4, 6)")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "part = 'A'")
  w.read(t, predicate = "part IN ('A','B')")
  w.read(t, predicate = "part = 'B' AND value > 20")
  w.read(t, predicate = "part = 'C'")
  w.snapshot(t)
}

workload("dv_single_row_deleted", "Single-row file with DV", "dv") { w =>
  w.sql("""CREATE TABLE tbl (id INT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1)")
  w.sql("DELETE FROM tbl WHERE id = 1")
  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
}

workload("dv_predicate_on_deleted", "Predicate matching only deleted rows returns 0 rows", "dv") { w =>
  w.sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'alice'),(2,'bob'),(3,'charlie'),(4,'diana')")
  w.sql("DELETE FROM tbl WHERE id IN (1, 3)")
  val t = w.table("tbl")
  w.read(t)
  // Predicate that matches ONLY deleted rows
  w.read(t, predicate = "id = 1")
  w.read(t, predicate = "id = 3")
  // Predicate that matches surviving rows
  w.read(t, predicate = "id = 2")
  w.read(t, predicate = "id = 4")
  w.snapshot(t)
}

workload("dv_multi_file_delete", "DVs across multiple data files", "dv") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  // Insert in separate batches to create multiple files
  w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
  w.sql("INSERT INTO tbl VALUES (4,'d'),(5,'e'),(6,'f')")
  w.sql("INSERT INTO tbl VALUES (7,'g'),(8,'h'),(9,'i')")
  // Delete from each file
  w.sql("DELETE FROM tbl WHERE id IN (1, 5, 9)")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, version = 1)
  w.read(t, version = 2)
  w.read(t, version = 3)
  w.read(t, predicate = "id > 3 AND id < 8")
  w.snapshot(t)
}

workload("dv_time_travel_pre_dv", "Time travel before DV exists", "dv") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d')")
  w.sql("INSERT INTO tbl VALUES (5,'e'),(6,'f')")
  // Version 2: DV delete
  w.sql("DELETE FROM tbl WHERE id IN (2, 4)")
  val t = w.table("tbl")
  // Read pre-DV version (should include all rows)
  w.read(t, version = 1)
  // Read post-DV version
  w.read(t)
  w.snapshot(t)
}

workload("dv_insert_readback", "Insert into DV table verify read-back", "dv") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
  w.sql("DELETE FROM tbl WHERE id = 2")
  w.sql("INSERT INTO tbl VALUES (4,'d'),(5,'e')")
  val t = w.table("tbl")
  w.read(t)
  // Only new rows
  w.read(t, predicate = "id > 3")
  // Surviving original rows
  w.read(t, predicate = "id <= 3")
  w.snapshot(t)
}

workload("dv_column_projection", "Column subset with DV selection vector", "dv") { w =>
  w.sql("""CREATE TABLE tbl (id INT, name STRING, value DOUBLE) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'alice',1.1),(2,'bob',2.2),(3,'charlie',3.3),(4,'diana',4.4)")
  w.sql("DELETE FROM tbl WHERE id IN (2, 4)")
  val t = w.table("tbl")
  w.read(t, columns = Seq("id", "name"))
  w.read(t, columns = Seq("value"))
  w.snapshot(t)
}

workload("dv_with_null_values", "DVs with NULL row values", "dv") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'a'),(2,NULL),(3,'c'),(4,NULL),(5,'e')")
  w.sql("DELETE FROM tbl WHERE value IS NULL")
  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
}

workload("dv_projection_with_pred", "Predicate + projection + DV (triple combo)", "dv") { w =>
  w.sql("""CREATE TABLE tbl (id INT, name STRING, value INT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'a',10),(2,'b',20),(3,'c',30),(4,'d',40),(5,'e',50)")
  w.sql("DELETE FROM tbl WHERE id IN (2, 4)")
  val t = w.table("tbl")
  w.read(t, columns = Seq("id", "name"), predicate = "value > 20")
  w.read(t, columns = Seq("name", "value"), predicate = "id < 4")
  w.snapshot(t)
}

generateAll(
  sys.env.getOrElse("WORKLOAD_OUTPUT_DIR", "/tmp/workloads"),
  force = sys.env.getOrElse("WORKLOAD_FORCE", "false").toBoolean)
System.exit(0)
