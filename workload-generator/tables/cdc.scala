import io.delta.workload.WorkloadGenerator._

workload("cdc_by_path", "CDC by path", "cdf") { w =>
  w.sql("""CREATE TABLE tbl (id LONG) USING delta
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl SELECT id FROM range(10)")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, version = 0)
  w.read(t, version = 1)
  w.cdf(t, startVersion = 0, endVersion = 1)
  w.snapshot(t)
}

workload("cdc_version_range", "CDF version ranges", "cdf") { w =>
  w.sql("""CREATE TABLE tbl (id LONG) USING delta
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')""")
  w.sql("INSERT INTO tbl SELECT id FROM range(10)")
  w.sql("INSERT INTO tbl SELECT id + 10 FROM range(10)")
  w.sql("INSERT INTO tbl SELECT id + 20 FROM range(10)")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, version = 1)
  w.read(t, version = 2)
  w.read(t, version = 3)
  w.cdf(t, startVersion = 0, endVersion = 3)
  w.cdf(t, startVersion = 1, endVersion = 2)
  w.snapshot(t)
}

workload("cdc_single_version", "CDF for single version", "cdf") { w =>
  w.sql("""CREATE TABLE tbl (id LONG) USING delta
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')""")
  w.sql("INSERT INTO tbl SELECT id FROM range(10)")
  w.sql("INSERT INTO tbl SELECT id + 10 FROM range(10)")
  val t = w.table("tbl")
  w.read(t, version = 1)
  w.read(t, version = 2)
  w.cdf(t, startVersion = 1, endVersion = 1)
  w.cdf(t, startVersion = 2, endVersion = 2)
  w.snapshot(t)
}

workload("cdc_inserts", "CDF with INSERT operations", "cdf") { w =>
  w.sql("""CREATE TABLE tbl (id LONG, name STRING) USING delta
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'alice'),(2,'bob')")
  w.sql("INSERT INTO tbl VALUES (3,'charlie'),(4,'dave')")
  w.sql("INSERT INTO tbl VALUES (5,'eve'),(6,'frank')")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, version = 1)
  w.read(t, version = 2)
  w.read(t, version = 3)
  w.cdf(t, startVersion = 0, endVersion = 3)
  w.cdf(t, startVersion = 1, endVersion = 1)
  w.snapshot(t)
}

workload("cdc_updates", "CDF with UPDATE operations", "cdf") { w =>
  w.sql("""CREATE TABLE tbl (id LONG, value INT) USING delta
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,100),(2,200),(3,300)")
  w.sql("UPDATE tbl SET value = value * 2 WHERE id = 1")
  w.sql("UPDATE tbl SET value = value + 50 WHERE id IN (2, 3)")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, version = 1)
  w.read(t, version = 2)
  w.read(t, version = 3)
  w.read(t, predicate = "id = 1")
  w.cdf(t, startVersion = 0, endVersion = 3)
  w.cdf(t, startVersion = 1, endVersion = 3)
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
  w.read(t, version = 2)
  w.read(t, version = 3)
  w.cdf(t, startVersion = 0, endVersion = 3)
  w.cdf(t, startVersion = 1, endVersion = 3)
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
  w.read(t, version = 2)
  w.read(t, predicate = "id = 2")
  w.cdf(t, startVersion = 0, endVersion = 2)
  w.cdf(t, startVersion = 2, endVersion = 2)
  w.snapshot(t)
}

workload("cdc_merge_delete", "MERGE with matched delete clause", "cdf", "merge") { w =>
  w.sql("""CREATE TABLE tbl (id LONG, value INT, status STRING) USING delta
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,100,'active'),(2,200,'active'),(3,300,'inactive')")
  w.sql("CREATE TABLE msrc (id LONG, value INT, status STRING) USING delta")
  w.sql("INSERT INTO msrc VALUES (2,250,'updated'),(3,0,'deleted'),(4,400,'new')")
  w.sql("""MERGE INTO tbl t USING msrc s ON t.id = s.id
    WHEN MATCHED AND s.status = 'deleted' THEN DELETE
    WHEN MATCHED THEN UPDATE SET value = s.value, status = s.status
    WHEN NOT MATCHED THEN INSERT *""")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, version = 1)
  w.read(t, version = 2)
  w.cdf(t, startVersion = 0, endVersion = 2)
  w.cdf(t, startVersion = 2, endVersion = 2)
  w.snapshot(t)
}

workload("cdc_multiple_refs", "Multiple CDF reads", "cdf") { w =>
  w.sql("""CREATE TABLE tbl (id LONG) USING delta
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')""")
  w.sql("INSERT INTO tbl SELECT id FROM range(10)")
  w.sql("INSERT INTO tbl SELECT id + 10 FROM range(10)")
  w.sql("INSERT INTO tbl SELECT id + 20 FROM range(10)")
  val t = w.table("tbl")
  w.read(t, version = 1)
  w.read(t, version = 2)
  w.read(t, version = 3)
  w.read(t, predicate = "id < 10")
  w.read(t, predicate = "id >= 20")
  w.cdf(t, startVersion = 0, endVersion = 3)
  w.snapshot(t)
}

workload("cdc_metadata_filter", "CDC filtering by data values", "cdf") { w =>
  w.sql("""CREATE TABLE tbl (id LONG) USING delta
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')""")
  w.sql("INSERT INTO tbl SELECT id FROM range(10)")
  w.sql("UPDATE tbl SET id = id + 100 WHERE id < 5")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "id >= 100")
  w.read(t, predicate = "id < 100")
  w.cdf(t, startVersion = 0)
  w.snapshot(t)
}

workload("cdc_partitioned", "CDC with partitioned table", "cdf") { w =>
  w.sql("""CREATE TABLE tbl (id LONG, category STRING, value INT) USING delta
    PARTITIONED BY (category) TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'A',100),(2,'A',200)")
  w.sql("INSERT INTO tbl VALUES (3,'B',300),(4,'B',400)")
  w.sql("INSERT INTO tbl VALUES (5,'C',500)")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "category = 'A'")
  w.read(t, predicate = "category = 'B'")
  w.read(t, predicate = "category = 'C'")
  w.cdf(t, startVersion = 0)
  w.snapshot(t)
}

workload("cdc_partitioned_dml", "CDC partitioned DML", "cdf") { w =>
  w.sql("""CREATE TABLE tbl (id LONG, category STRING, value INT) USING delta
    PARTITIONED BY (category) TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'A',10),(2,'A',20),(3,'B',30),(4,'B',40)")
  w.sql("UPDATE tbl SET value = 99 WHERE category = 'A' AND id = 1")
  w.sql("DELETE FROM tbl WHERE category = 'B' AND id = 4")
  w.sql("UPDATE tbl SET value = value + 1000")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "category = 'A'")
  w.read(t, predicate = "category = 'B'")
  w.cdf(t, startVersion = 0, endVersion = 4)
  w.cdf(t, startVersion = 2, endVersion = 2)
  w.cdf(t, startVersion = 3, endVersion = 3)
  w.cdf(t, startVersion = 4, endVersion = 4)
  w.snapshot(t)
}

workload("cdc_column_mapping", "CDC with column mapping", "cdf", "column_mapping") { w =>
  w.sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'delta.columnMapping.mode' = 'name')""")
  w.sql("INSERT INTO tbl VALUES (1,'alice'),(2,'bob')")
  w.sql("UPDATE tbl SET name = 'ALICE' WHERE id = 1")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, version = 1)
  w.read(t, predicate = "name = 'ALICE'")
  w.cdf(t, startVersion = 0)
  w.snapshot(t)
}

workload("cdc_deletion_vectors", "CDC with deletion vectors", "cdf", "dv") { w =>
  w.sql("""CREATE TABLE tbl (id LONG, value INT) USING delta
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,100),(2,200),(3,300),(4,400),(5,500)")
  w.sql("UPDATE tbl SET value = value * 10 WHERE id <= 2")
  w.sql("DELETE FROM tbl WHERE id = 4")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, version = 1)
  w.read(t, version = 2)
  w.read(t, version = 3)
  w.cdf(t, startVersion = 0, endVersion = 3)
  w.cdf(t, startVersion = 2, endVersion = 2)
  w.cdf(t, startVersion = 3, endVersion = 3)
  w.snapshot(t)
}

workload("cdc_dv_column_mapping", "CDC + DV + column mapping", "cdf", "dv", "column_mapping") { w =>
  w.sql("""CREATE TABLE tbl (id INT, name STRING, score DOUBLE) USING delta
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'delta.enableDeletionVectors' = 'true',
      'delta.columnMapping.mode' = 'name')""")
  w.sql("INSERT INTO tbl VALUES (1,'alice',90.0),(2,'bob',80.0),(3,'charlie',70.0)")
  w.sql("UPDATE tbl SET score = 95.0 WHERE name = 'alice'")
  w.sql("DELETE FROM tbl WHERE name = 'charlie'")
  w.sql("INSERT INTO tbl VALUES (4,'dave',85.0)")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, version = 1)
  w.read(t, version = 4)
  w.read(t, predicate = "name = 'alice'")
  w.cdf(t, startVersion = 0, endVersion = 4)
  w.cdf(t, startVersion = 1, endVersion = 3)
  w.snapshot(t)
}

workload("cdc_data_skipping", "CDC with data skipping", "cdf") { w =>
  w.sql("""CREATE TABLE tbl (id LONG, category STRING, amount INT) USING delta
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'low',10),(2,'low',20)")
  w.sql("INSERT INTO tbl VALUES (3,'mid',50),(4,'mid',60)")
  w.sql("INSERT INTO tbl VALUES (5,'high',90),(6,'high',100)")
  w.sql("UPDATE tbl SET amount = amount + 1 WHERE category = 'mid'")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "amount <= 20")
  w.read(t, predicate = "amount >= 90")
  w.read(t, predicate = "category = 'mid'")
  w.cdf(t, startVersion = 0)
  w.cdf(t, startVersion = 1, endVersion = 3)
  w.cdf(t, startVersion = 4, endVersion = 4)
  w.snapshot(t)
}

workload("cdc_schema_evolution", "CDC across ADD COLUMN", "cdf", "schema_evolution") { w =>
  w.sql("""CREATE TABLE tbl (id LONG, value INT) USING delta
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true',
      'delta.columnMapping.mode' = 'name', 'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
  w.sql("INSERT INTO tbl VALUES (1,100),(2,200)")
  w.sql("ALTER TABLE tbl ADD COLUMN (label STRING)")
  w.sql("INSERT INTO tbl VALUES (3,300,'three'),(4,400,'four')")
  w.sql("UPDATE tbl SET label = 'one' WHERE id = 1")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, version = 1)
  w.read(t, version = 3)
  w.cdf(t, startVersion = 0, endVersion = 4)
  w.cdf(t, startVersion = 1, endVersion = 1)
  w.cdf(t, startVersion = 3, endVersion = 4)
  w.cdf(t, startVersion = 1, endVersion = 3)
  w.snapshot(t)
}

workload("cdc_optimize", "CDC after OPTIMIZE", "cdf") { w =>
  w.sql("""CREATE TABLE tbl (id LONG, value INT) USING delta
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,100)")
  w.sql("INSERT INTO tbl VALUES (2,200)")
  w.sql("INSERT INTO tbl VALUES (3,300)")
  w.sql("OPTIMIZE tbl")
  w.sql("INSERT INTO tbl VALUES (4,400)")
  val t = w.table("tbl")
  w.read(t)
  w.cdf(t, startVersion = 0, endVersion = 5)
  w.cdf(t, startVersion = 3, endVersion = 5)
  w.cdf(t, startVersion = 4, endVersion = 4)
  w.snapshot(t)
}

workload("cdc_predicates", "CDC read with predicates", "cdf") { w =>
  w.sql("""CREATE TABLE tbl (id INT, status STRING, count INT) USING delta
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'active',10),(2,'inactive',20),(3,'active',30)")
  w.sql("INSERT INTO tbl VALUES (4,'pending',40),(5,'active',50)")
  w.sql("UPDATE tbl SET status = 'inactive' WHERE count > 40")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "status = 'active'")
  w.read(t, predicate = "status = 'inactive'")
  w.read(t, predicate = "count > 30")
  w.read(t, version = 2)
  w.cdf(t, startVersion = 0)
  w.snapshot(t)
}

workload("cdc_transform", "CDC data transformation", "cdf") { w =>
  w.sql("""CREATE TABLE tbl (id LONG, amount DOUBLE) USING delta
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,100.0),(2,200.0),(3,300.0)")
  w.sql("INSERT INTO tbl VALUES (4,400.0),(5,500.0)")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "amount > 250.0")
  w.read(t, predicate = "amount <= 200.0")
  w.cdf(t, startVersion = 0)
  w.snapshot(t)
}

workload("cdc_multiple_types", "CDC with diverse types", "cdf") { w =>
  w.sql("""CREATE TABLE tbl (id INT, name STRING, score DOUBLE, is_active BOOLEAN,
    amount DECIMAL(10,2), created DATE, updated_at TIMESTAMP) USING delta
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'alice',95.5,true,1000.50,DATE'2024-01-01',TIMESTAMP'2024-01-01 10:00:00')")
  w.sql("INSERT INTO tbl VALUES (2,'bob',87.3,false,2000.75,DATE'2024-02-15',TIMESTAMP'2024-02-15 14:30:00')")
  w.sql("UPDATE tbl SET score = 99.0, is_active = false WHERE id = 1")
  val t = w.table("tbl")
  w.read(t)
  w.cdf(t, startVersion = 0, endVersion = 3)
  w.cdf(t, startVersion = 3, endVersion = 3)
  w.snapshot(t)
}

workload("cdc_nested_struct", "CDC with nested types", "cdf") { w =>
  w.sql("""CREATE TABLE tbl (id INT, info STRUCT<name: STRING, age: INT>, tags ARRAY<STRING>) USING delta
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,struct('alice',30),array('a','b'))")
  w.sql("INSERT INTO tbl VALUES (2,struct('bob',25),array('c'))")
  w.sql("DELETE FROM tbl WHERE id = 1")
  val t = w.table("tbl")
  w.read(t)
  w.cdf(t, startVersion = 0, endVersion = 3)
  w.cdf(t, startVersion = 3, endVersion = 3)
  w.snapshot(t)
}

workload("cdc_map_array", "CDC with map and array", "cdf") { w =>
  w.sql("""CREATE TABLE tbl (id INT, props MAP<STRING, STRING>, scores ARRAY<INT>) USING delta
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,map('k1','v1','k2','v2'),array(10,20,30))")
  w.sql("INSERT INTO tbl VALUES (2,map('k3','v3'),array(40))")
  w.sql("UPDATE tbl SET props = map('k1','updated') WHERE id = 1")
  val t = w.table("tbl")
  w.read(t)
  w.cdf(t, startVersion = 0, endVersion = 3)
  w.cdf(t, startVersion = 3, endVersion = 3)
  w.snapshot(t)
}

workload("cdc_generated_columns", "CDC with generated columns", "cdf") { w =>
  w.sql("""CREATE TABLE tbl (id INT, price DOUBLE, quantity INT,
    total DOUBLE GENERATED ALWAYS AS (price * quantity)) USING delta
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')""")
  w.sql("INSERT INTO tbl(id, price, quantity) VALUES (1,10.0,5)")
  w.sql("INSERT INTO tbl(id, price, quantity) VALUES (2,20.0,3)")
  w.sql("UPDATE tbl SET quantity = 10 WHERE id = 1")
  val t = w.table("tbl")
  w.read(t)
  w.cdf(t, startVersion = 0, endVersion = 3)
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
