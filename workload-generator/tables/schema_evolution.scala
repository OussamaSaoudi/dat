import io.delta.workload.WorkloadGenerator._

workload("schema_add_column", "ADD COLUMN", "schema_evolution") { w =>
  w.sql("CREATE TABLE tbl (id INT, value STRING) USING delta")
  w.sql("INSERT INTO tbl VALUES (1, 'before')")
  w.sql("ALTER TABLE tbl ADD COLUMN (new_col DOUBLE)")
  w.sql("INSERT INTO tbl VALUES (2, 'after', 3.14)")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, version = 1)
  w.read(t, columns = Seq("id", "new_col"))
  w.read(t, predicate = "new_col IS NOT NULL")
  w.snapshotHistory(t)
}

workload("schema_add_nested_field", "Add field to nested struct", "schema_evolution") { w =>
  w.sql("CREATE TABLE tbl (id INT, info STRUCT<name: STRING, age: INT>) USING delta")
  w.sql("INSERT INTO tbl VALUES (1, named_struct('name','alice','age',30))")
  w.sql("ALTER TABLE tbl ADD COLUMNS (info.email STRING)")
  w.sql("INSERT INTO tbl VALUES (2, named_struct('name','bob','age',25,'email','bob@test.com'))")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "info.email IS NULL")
  w.read(t, predicate = "info.email IS NOT NULL")
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
  w.read(t, columns = Seq("id", "new_name"))
  w.snapshotHistory(t)
}

workload("schema_drop_column", "DROP COLUMN", "schema_evolution", "column_mapping") { w =>
  w.sql("""CREATE TABLE tbl (id INT, name STRING, value STRING) USING delta
    TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
      'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
  w.sql("INSERT INTO tbl VALUES (1,'alice','v1'),(2,'bob','v2')")
  w.sql("ALTER TABLE tbl DROP COLUMN value")
  w.sql("INSERT INTO tbl VALUES (3,'charlie')")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, version = 2)
  w.snapshotHistory(t)
}

workload("schema_multiple_renames", "Chain of renames a→b→c", "schema_evolution", "column_mapping") { w =>
  w.sql("""CREATE TABLE tbl (id INT, a STRING) USING delta
    TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
      'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
  w.sql("INSERT INTO tbl VALUES (1,'first')")
  w.sql("ALTER TABLE tbl RENAME COLUMN a TO b")
  w.sql("INSERT INTO tbl VALUES (2,'second')")
  w.sql("ALTER TABLE tbl RENAME COLUMN b TO c")
  w.sql("INSERT INTO tbl VALUES (3,'third')")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, columns = Seq("id", "c"))
  w.snapshotHistory(t)
}

workload("schema_predicate_on_added", "Predicate on null-filled added column", "schema_evolution") { w =>
  w.sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
  w.sql("INSERT INTO tbl VALUES (1,'alice'),(2,'bob')")
  w.sql("ALTER TABLE tbl ADD COLUMNS (score INT)")
  w.sql("INSERT INTO tbl VALUES (3,'charlie',95),(4,'diana',88)")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "score > 90")
  w.read(t, predicate = "score IS NULL")
  w.read(t, predicate = "score IS NOT NULL")
  w.snapshotHistory(t)
}

generateAll(
  sys.env.getOrElse("WORKLOAD_OUTPUT_DIR", "/tmp/workloads"),
  force = sys.env.getOrElse("WORKLOAD_FORCE", "false").toBoolean)
System.exit(0)
