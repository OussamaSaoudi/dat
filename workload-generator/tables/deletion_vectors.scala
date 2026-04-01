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

generateAll(
  sys.env.getOrElse("WORKLOAD_OUTPUT_DIR", "/tmp/workloads"),
  force = sys.env.getOrElse("WORKLOAD_FORCE", "false").toBoolean)
System.exit(0)
