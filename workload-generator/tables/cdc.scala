import io.delta.workload.WorkloadGenerator._

workload("cdc_inserts", "CDF with INSERT operations", "cdf") { w =>
  w.sql("""CREATE TABLE tbl (id LONG, name STRING) USING delta
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'alice')")
  w.sql("INSERT INTO tbl VALUES (2,'bob'),(3,'charlie')")
  val t = w.table("tbl")
  w.read(t)
  w.cdf(t, startVersion = 0)
  w.cdf(t, startVersion = 1, endVersion = 1)
  w.snapshot(t)
}

workload("cdc_updates", "CDF with UPDATE operations", "cdf") { w =>
  w.sql("""CREATE TABLE tbl (id LONG, value INT) USING delta
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,100),(2,200),(3,300)")
  w.sql("UPDATE tbl SET value = value * 2 WHERE id = 1")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, version = 1)
  w.cdf(t, startVersion = 0)
  w.cdf(t, startVersion = 2, endVersion = 2)
  w.snapshot(t)
}

workload("cdc_deletes", "CDF with DELETE operations", "cdf") { w =>
  w.sql("""CREATE TABLE tbl (id LONG, name STRING) USING delta
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e')")
  w.sql("DELETE FROM tbl WHERE id = 3")
  w.sql("DELETE FROM tbl WHERE id IN (1, 5)")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, version = 1)
  w.cdf(t, startVersion = 0)
  w.cdf(t, startVersion = 2, endVersion = 2)
  w.cdf(t, startVersion = 3, endVersion = 3)
  w.snapshot(t)
}

workload("cdc_merge", "CDF with MERGE", "cdf", "merge") { w =>
  w.sql("""CREATE TABLE target (id LONG, value INT) USING delta
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')""")
  w.sql("INSERT INTO target VALUES (1,100),(2,200),(3,300)")
  w.sql("CREATE TABLE src (id LONG, value INT) USING delta")
  w.sql("INSERT INTO src VALUES (2,250),(4,400)")
  w.sql("""MERGE INTO target t USING src s ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET value = s.value
    WHEN NOT MATCHED THEN INSERT *""")
  val t = w.table("target")
  w.read(t)
  w.read(t, version = 1)
  w.cdf(t, startVersion = 0)
  w.cdf(t, startVersion = 2, endVersion = 2)
  w.snapshot(t)
}

workload("cdc_partitioned", "CDF with partitioned table", "cdf") { w =>
  w.sql("""CREATE TABLE tbl (id LONG, category STRING, value INT) USING delta
    PARTITIONED BY (category) TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'A',100),(2,'A',200),(3,'B',300),(4,'B',400)")
  w.sql("UPDATE tbl SET value = 99 WHERE category = 'A' AND id = 1")
  w.sql("DELETE FROM tbl WHERE category = 'B' AND id = 4")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "category = 'A'")
  w.cdf(t, startVersion = 0)
  w.cdf(t, startVersion = 2, endVersion = 2)
  w.cdf(t, startVersion = 3, endVersion = 3)
  w.snapshot(t)
}

workload("cdc_with_dv", "CDF with deletion vectors", "cdf", "dv") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d')")
  w.sql("DELETE FROM tbl WHERE id IN (2, 4)")
  w.sql("UPDATE tbl SET value = 'UPDATED' WHERE id = 1")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, version = 1)
  w.cdf(t, startVersion = 0)
  w.cdf(t, startVersion = 2, endVersion = 2)
  w.cdf(t, startVersion = 3, endVersion = 3)
  w.snapshot(t)
}

workload("cdc_schema_evolution", "CDF across ADD COLUMN", "cdf", "schema_evolution") { w =>
  w.sql("""CREATE TABLE tbl (id LONG, value INT) USING delta
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true',
      'delta.columnMapping.mode' = 'name', 'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
  w.sql("INSERT INTO tbl VALUES (1,100),(2,200)")
  w.sql("ALTER TABLE tbl ADD COLUMN (label STRING)")
  w.sql("INSERT INTO tbl VALUES (3,300,'three'),(4,400,'four')")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, version = 1)
  w.cdf(t, startVersion = 0)
  w.cdf(t, startVersion = 1, endVersion = 1)
  w.cdf(t, startVersion = 3, endVersion = 3)
  w.snapshot(t)
}

workload("cdc_not_enabled", "Error: CDF on table without CDF", "cdf", "error") { w =>
  w.sql("CREATE TABLE tbl (id INT) USING delta")
  w.sql("INSERT INTO tbl VALUES (1),(2)")
  val t = w.table("tbl")
  w.read(t)
  w.cdf(t, startVersion = 0)
  w.snapshot(t)
}

workload("cdc_many_versions", "CDF with 10+ versions", "cdf") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value INT) USING delta
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')""")
  for (i <- 1 to 5) w.sql(s"INSERT INTO tbl VALUES ($i, ${i * 10})")
  w.sql("UPDATE tbl SET value = value + 1 WHERE id <= 2")
  w.sql("DELETE FROM tbl WHERE id = 3")
  for (i <- 6 to 8) w.sql(s"INSERT INTO tbl VALUES ($i, ${i * 10})")
  w.sql("UPDATE tbl SET value = value * 2")
  w.sql("DELETE FROM tbl WHERE id = 1")
  val t = w.table("tbl")
  w.read(t)
  w.cdf(t, startVersion = 1)
  w.cdf(t, startVersion = 1, endVersion = 6)
  w.cdf(t, startVersion = 7)
  w.snapshot(t)
}

generateAll(
  sys.env.getOrElse("WORKLOAD_OUTPUT_DIR", "/tmp/workloads"),
  force = sys.env.getOrElse("WORKLOAD_FORCE", "false").toBoolean)
System.exit(0)
