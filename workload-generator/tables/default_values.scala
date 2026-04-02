new WorkloadSuite("default_values") {

  test("ddefReadWithDefaults", "Read table with column defaults") { w =>
    w.sql("""CREATE TABLE tbl (id INT, name STRING DEFAULT 'unknown', score DOUBLE DEFAULT 0.0)
      USING delta TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'enabled')""")
    w.sql("INSERT INTO tbl(id) VALUES (1),(2)")
    w.sql("INSERT INTO tbl VALUES (3, 'alice', 95.0)")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "name = 'unknown'")
    w.snapshot(t)
  }

  test("ddefReadDefaultAfterAdd", "Read defaults after ADD COLUMN") { w =>
    w.sql("""CREATE TABLE tbl (id INT) USING delta
      TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'enabled')""")
    w.sql("INSERT INTO tbl VALUES (1)")
    w.sql("ALTER TABLE tbl ADD COLUMN status STRING DEFAULT 'active'")
    w.sql("INSERT INTO tbl(id) VALUES (2)")
    w.sql("INSERT INTO tbl VALUES (3, 'inactive')")
    val t = w.table("tbl")
    w.read(t)
    w.snapshotHistory(t)
  }

  test("ddefReadDefaultTypes", "Column defaults with various types") { w =>
    w.sql("""CREATE TABLE tbl (
      id INT, int_col INT DEFAULT 42, str_col STRING DEFAULT 'hello',
      bool_col BOOLEAN DEFAULT true, double_col DOUBLE DEFAULT 3.14
    ) USING delta TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'enabled')""")
    w.sql("INSERT INTO tbl(id) VALUES (1),(2)")
    val t = w.table("tbl")
    w.read(t)
    w.snapshot(t)
  }

  test("ddefReadDefaultNested", "Column defaults with nested struct") { w =>
    w.sql("""CREATE TABLE tbl (id INT, info STRUCT<name: STRING, age: INT>)
      USING delta TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'enabled')""")
    w.sql("INSERT INTO tbl VALUES (1, named_struct('name','alice','age',30))")
    w.sql("INSERT INTO tbl VALUES (2, NULL)")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "info IS NOT NULL")
    w.snapshot(t)
  }

}.runAll()
