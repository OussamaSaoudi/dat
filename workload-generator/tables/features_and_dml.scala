/**
 * Schema evolution, table features, and DML operations.
 * Run: ./bin/generate-workload.sh examples/features_and_dml.scala
 */
import io.delta.workload.WorkloadGenerator._

// --- Schema evolution ---
workload("schema_add_column", "ADD COLUMN", "schema_evolution") { w =>
  w.sql("CREATE TABLE tbl (id INT, value STRING) USING delta")
  w.sql("INSERT INTO tbl VALUES (1, 'before')")
  w.sql("ALTER TABLE tbl ADD COLUMN (new_col DOUBLE)")
  w.sql("INSERT INTO tbl VALUES (2, 'after', 3.14)")

  val t = w.table("tbl")
  w.read(t)
  w.read(t, version = 1)
  w.read(t, columns = Seq("id", "new_col"))
  w.snapshotHistory(t)
}

workload("schema_rename", "RENAME COLUMN", "schema_evolution", "column_mapping") { w =>
  w.sql("""CREATE TABLE tbl (id INT, old_name STRING) USING delta
    TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
      'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
  w.sql("INSERT INTO tbl VALUES (1, 'before')")
  w.sql("ALTER TABLE tbl RENAME COLUMN old_name TO new_name")
  w.sql("INSERT INTO tbl VALUES (2, 'after')")

  val t = w.table("tbl")
  w.read(t)
  w.read(t, version = 1)
  w.snapshotHistory(t)
}

// --- Table features ---
workload("dv_basic", "Deletion vectors", "dv") { w =>
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

workload("cdf_lifecycle", "CDF across operations", "cdf") { w =>
  w.sql("""CREATE TABLE tbl (id INT, val STRING) USING delta
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
  w.sql("UPDATE tbl SET val = 'updated' WHERE id = 2")
  w.sql("DELETE FROM tbl WHERE id = 3")
  w.sql("INSERT INTO tbl VALUES (4,'d'),(5,'e')")

  val t = w.table("tbl")
  w.read(t)
  w.read(t, version = 1)
  w.cdf(t, startVersion = 0)
  w.cdf(t, startVersion = 2, endVersion = 2)
  w.cdf(t, startVersion = 3, endVersion = 3)
  w.snapshot(t)
}

workload("check_constraint", "CHECK constraint", "constraints") { w =>
  w.sql("CREATE TABLE tbl (id INT, amount DOUBLE) USING delta")
  w.sql("ALTER TABLE tbl ADD CONSTRAINT positive CHECK (amount > 0)")
  w.sql("INSERT INTO tbl VALUES (1, 10.0), (2, 99.9)")

  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
}

workload("with_checkpoint", "Checkpoint at v5", "checkpoint") { w =>
  w.sql("""CREATE TABLE tbl (id INT, val STRING) USING delta
    TBLPROPERTIES ('delta.checkpointInterval' = '5')""")
  for (i <- 1 to 6) w.sql(s"INSERT INTO tbl VALUES ($i, 'v$i')")

  val t = w.table("tbl")
  w.read(t)
  w.read(t, version = 5)
  w.snapshot(t)
  w.snapshot(t, version = 5)
}

// --- DML ---
workload("dml_insert", "Multiple appends", "dml") { w =>
  w.sql("CREATE TABLE tbl (id INT, val STRING) USING delta")
  w.sql("INSERT INTO tbl VALUES (1, 'first')")
  w.sql("INSERT INTO tbl VALUES (2, 'second'), (3, 'third')")

  val t = w.table("tbl")
  w.read(t)
  w.read(t, version = 1)
  w.read(t, version = 2)
  w.snapshot(t)
}

workload("dml_overwrite", "INSERT OVERWRITE", "dml") { w =>
  w.sql("""CREATE TABLE tbl (id INT, region STRING)
    USING delta PARTITIONED BY (region)""")
  w.sql("INSERT INTO tbl VALUES (1,'us'),(2,'eu'),(3,'us')")
  w.sql("INSERT OVERWRITE tbl PARTITION (region='us') VALUES (10,'us'),(11,'us')")

  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "region = 'us'")
  w.read(t, version = 1)
  w.snapshot(t)
}

workload("dml_delete", "Conditional DELETE", "dml") { w =>
  w.sql("CREATE TABLE tbl (id INT, val STRING) USING delta")
  w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e')")
  w.sql("DELETE FROM tbl WHERE id <= 2")

  val t = w.table("tbl")
  w.read(t)
  w.read(t, version = 1)
  w.snapshot(t)
}

workload("dml_update", "UPDATE with predicate", "dml") { w =>
  w.sql("CREATE TABLE tbl (id INT, val STRING, amount DOUBLE) USING delta")
  w.sql("INSERT INTO tbl VALUES (1,'a',10.0),(2,'b',20.0),(3,'c',30.0)")
  w.sql("UPDATE tbl SET val = 'updated', amount = amount * 2 WHERE id >= 2")

  val t = w.table("tbl")
  w.read(t)
  w.read(t, version = 1)
  w.read(t, predicate = "amount > 25.0")
  w.snapshot(t)
}

workload("dml_merge_full", "MERGE: update + delete + insert", "dml", "merge") { w =>
  w.sql("""CREATE TABLE tgt (id INT, val STRING, active BOOLEAN) USING delta
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')""")
  w.sql("INSERT INTO tgt VALUES (1,'a',true),(2,'b',true),(3,'c',true)")
  w.sql("CREATE TABLE src (id INT, val STRING) USING delta")
  w.sql("INSERT INTO src VALUES (2,'new_b'),(3,'delete_me'),(4,'new_d')")
  w.sql("""MERGE INTO tgt t USING src s ON t.id = s.id
    WHEN MATCHED AND s.val = 'delete_me' THEN DELETE
    WHEN MATCHED THEN UPDATE SET val = s.val
    WHEN NOT MATCHED THEN INSERT (id, val, active) VALUES (s.id, s.val, true)""")

  val tgt = w.table("tgt")
  w.read(tgt)
  w.read(tgt, version = 1)
  w.snapshot(tgt)
  w.cdf(tgt, startVersion = 2)
}

workload("dml_sequence", "Multi-step DML lifecycle", "dml") { w =>
  w.sql("""CREATE TABLE tbl (id INT, status STRING) USING delta
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'new'),(2,'new'),(3,'new')")
  w.sql("UPDATE tbl SET status = 'active' WHERE id IN (1, 2)")
  w.sql("DELETE FROM tbl WHERE id = 3")
  w.sql("INSERT INTO tbl VALUES (4,'new'),(5,'new')")
  w.sql("UPDATE tbl SET status = 'archived' WHERE id = 1")

  val t = w.table("tbl")
  w.read(t)
  w.read(t, version = 1)
  w.read(t, version = 3)
  w.snapshot(t)
  w.cdf(t, startVersion = 0)
  w.cdf(t, startVersion = 2, endVersion = 2)
}

generateAll(
  sys.env.getOrElse("WORKLOAD_OUTPUT_DIR", "/tmp/workloads"),
  force = sys.env.getOrElse("WORKLOAD_FORCE", "false").toBoolean)
System.exit(0)
