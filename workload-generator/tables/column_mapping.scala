import io.delta.workload.WorkloadGenerator._

workload("colmap_name_mode", "Column mapping name mode", "column_mapping") { w =>
  w.sql("""CREATE TABLE tbl (id INT, name STRING, value DOUBLE) USING delta
    TBLPROPERTIES ('delta.columnMapping.mode' = 'name')""")
  w.sql("INSERT INTO tbl VALUES (1,'alice',100),(2,'bob',200),(3,'charlie',300)")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, columns = Seq("name", "value"))
  w.read(t, predicate = "value > 150")
  w.snapshot(t)
}

workload("colmap_rename", "Rename column", "column_mapping") { w =>
  w.sql("""CREATE TABLE tbl (id INT, old_name STRING) USING delta
    TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
      'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
  w.sql("INSERT INTO tbl VALUES (1,'alice'),(2,'bob')")
  w.sql("ALTER TABLE tbl RENAME COLUMN old_name TO new_name")
  w.sql("INSERT INTO tbl VALUES (3,'charlie')")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, version = 2)
  w.read(t, columns = Seq("id", "new_name"))
  w.snapshotHistory(t)
}

workload("colmap_drop_column", "Drop column", "column_mapping") { w =>
  w.sql("""CREATE TABLE tbl (id INT, name STRING, to_drop STRING) USING delta
    TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
      'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
  w.sql("INSERT INTO tbl VALUES (1,'a','drop1'),(2,'b','drop2')")
  w.sql("ALTER TABLE tbl DROP COLUMN to_drop")
  w.sql("INSERT INTO tbl VALUES (3,'c')")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, version = 2)
  w.snapshotHistory(t)
}

workload("colmap_drop_readd", "Drop and re-add same name", "column_mapping") { w =>
  w.sql("""CREATE TABLE tbl (id INT, x STRING) USING delta
    TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
      'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
  w.sql("INSERT INTO tbl VALUES (1, 'hello')")
  w.sql("ALTER TABLE tbl DROP COLUMN x")
  w.sql("ALTER TABLE tbl ADD COLUMNS (x INT)")
  w.sql("INSERT INTO tbl VALUES (2, 42)")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "x IS NULL")
  w.read(t, predicate = "x IS NOT NULL")
  w.snapshotHistory(t)
}

workload("colmap_nested", "Nested struct + column mapping", "column_mapping") { w =>
  w.sql("""CREATE TABLE tbl (id INT, info STRUCT<name: STRING, age: INT>) USING delta
    TBLPROPERTIES ('delta.columnMapping.mode' = 'name')""")
  w.sql("INSERT INTO tbl VALUES (1, named_struct('name','alice','age',30))")
  w.sql("INSERT INTO tbl VALUES (2, named_struct('name','bob','age',25))")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "info.age > 27")
  w.snapshot(t)
}

workload("colmap_upgrade", "Upgrade none to name mode", "column_mapping") { w =>
  w.sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
  w.sql("INSERT INTO tbl VALUES (1,'before'),(2,'before')")
  w.sql("""ALTER TABLE tbl SET TBLPROPERTIES (
    'delta.columnMapping.mode' = 'name',
    'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
  w.sql("INSERT INTO tbl VALUES (3,'after')")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, version = 0)
  w.snapshotHistory(t)
}

generateAll(
  sys.env.getOrElse("WORKLOAD_OUTPUT_DIR", "/tmp/workloads"),
  force = sys.env.getOrElse("WORKLOAD_FORCE", "false").toBoolean)
System.exit(0)
