new WorkloadSuite("default_values") {

  test("ddefReadWithDefaults", "Read table with column defaults") {
    sql("""CREATE TABLE tbl (id INT, name STRING DEFAULT 'unknown', score DOUBLE DEFAULT 0.0)
      USING delta TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'enabled')""")
    sql("INSERT INTO tbl(id) VALUES (1),(2)")
    sql("INSERT INTO tbl VALUES (3, 'alice', 95.0)")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "name = 'unknown'")
    snapshot(t)
  }

  test("ddefReadDefaultAfterAdd", "Read defaults after ADD COLUMN") {
    sql("""CREATE TABLE tbl (id INT) USING delta
      TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'enabled')""")
    sql("INSERT INTO tbl VALUES (1)")
    sql("ALTER TABLE tbl ADD COLUMN status STRING DEFAULT 'active'")
    sql("INSERT INTO tbl(id) VALUES (2)")
    sql("INSERT INTO tbl VALUES (3, 'inactive')")
    val t = registerTable("tbl")
    read(t)
    val N = 4L
    for (v <- 0L to N) snapshot(t, version = v)
  }

  test("ddefReadDefaultTypes", "Column defaults with various types") {
    sql("""CREATE TABLE tbl (
      id INT, int_col INT DEFAULT 42, str_col STRING DEFAULT 'hello',
      bool_col BOOLEAN DEFAULT true, double_col DOUBLE DEFAULT 3.14
    ) USING delta TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'enabled')""")
    sql("INSERT INTO tbl(id) VALUES (1),(2)")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("ddefReadDefaultNested", "Column defaults with nested struct") {
    sql("""CREATE TABLE tbl (id INT, info STRUCT<name: STRING, age: INT>)
      USING delta TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'enabled')""")
    sql("INSERT INTO tbl VALUES (1, named_struct('name','alice','age',30))")
    sql("INSERT INTO tbl VALUES (2, NULL)")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "info IS NOT NULL")
    snapshot(t)
  }

}
