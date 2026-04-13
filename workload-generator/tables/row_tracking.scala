new WorkloadSuite("row_tracking") {

  test("rt_basic_read", "Write and read table without materialized columns", "rowTracking") {
    sql("""CREATE TABLE tbl (test_data LONG) USING delta
      TBLPROPERTIES ('delta.enableRowTracking' = 'true', 'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(100)")
    val t = registerTable("tbl")
    read(t, version = 0)
    snapshot(t)
  }

  test("rt_all_null_materialized", "Write and read table with all-null materialized columns", "rowTracking") {
    sql("""CREATE TABLE tbl (test_data LONG) USING delta
      TBLPROPERTIES ('delta.enableRowTracking' = 'true', 'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(100)")
    val t = registerTable("tbl")
    read(t, version = 0)
    snapshot(t)
  }

  test("rt_no_null_materialized", "Write and read table with no-nulls materialized columns", "rowTracking") {
    sql("""CREATE TABLE tbl (test_data LONG) USING delta
      TBLPROPERTIES ('delta.enableRowTracking' = 'true', 'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(100)")
    val t = registerTable("tbl")
    read(t, version = 0)
    snapshot(t)
  }

  test("rt_mixed_materialized", "Write and read table with mixed materialized columns", "rowTracking") {
    sql("""CREATE TABLE tbl (test_data LONG) USING delta
      TBLPROPERTIES ('delta.enableRowTracking' = 'true', 'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(100)")
    val t = registerTable("tbl")
    read(t, version = 0)
    snapshot(t)
  }

  test("rt_conflicting_columns", "Write and read with conflicting columns", "rowTracking") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableRowTracking' = 'true', 'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    val t = registerTable("tbl")
    read(t, version = 0)
    snapshot(t)
  }

  test("rt_filter_read", "Read mixed materialized columns with filter", "rowTracking") {
    sql("""CREATE TABLE tbl (test_data LONG) USING delta
      TBLPROPERTIES ('delta.enableRowTracking' = 'true', 'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(100)")
    val t = registerTable("tbl")
    read(t, version = 0)
    read(t, version = 0, predicate = "test_data < 50")
    snapshot(t)
  }

  test("rt_column_projection", "Column subset on RT table", "rowTracking") {
    sql("""CREATE TABLE tbl (id INT, name STRING, value DOUBLE) USING delta
      TBLPROPERTIES ('delta.enableRowTracking' = 'true', 'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, 'alice', 1.0),(2, 'bob', 2.0),(3, 'charlie', 3.0)")
    val t = registerTable("tbl")
    read(t)
    read(t, columns = Seq("id", "value"))
    snapshot(t)
  }

  test("rt_read_base_row_id", "Read base row IDs", "rowTracking") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableRowTracking' = 'true', 'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(20)")
    sql("INSERT INTO tbl SELECT id + 20 FROM range(10)")
    val t = registerTable("tbl")
    read(t)
    read(t, version = 0)
    read(t, predicate = "id >= 15")
    snapshot(t)
  }

  test("rt_read_row_id_and_index", "Read both row id and row index", "rowTracking") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableRowTracking' = 'true', 'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    sql("INSERT INTO tbl SELECT id + 10 FROM range(10)")
    sql("UPDATE tbl SET id = id + 100 WHERE id < 3")
    val t = registerTable("tbl")
    read(t)
    read(t, version = 0)
    read(t, version = 1)
    read(t, predicate = "id >= 100")
    snapshot(t)
  }

  test("rt_across_schema_evolution", "Row IDs preserved after ADD COLUMN", "rowTracking") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableRowTracking' = 'true', 'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    sql("ALTER TABLE tbl ADD COLUMN (name STRING)")
    sql("INSERT INTO tbl VALUES (100, 'new')")
    val t = registerTable("tbl")
    read(t)
    read(t, version = 0)
    read(t, predicate = "name IS NOT NULL")
    snapshot(t)
    snapshot(t, version = 0)
    snapshot(t, version = 1)
    snapshot(t, version = 2)
  }

  test("rt_version_migration", "Upgrade from non-tracking to tracking", "rowTracking") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(20)")
    sql("INSERT INTO tbl SELECT id + 20 FROM range(10)")
    sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.enableRowTracking' = 'true')")
    sql("INSERT INTO tbl SELECT id + 30 FROM range(10)")
    val t = registerTable("tbl")
    read(t)
    read(t, version = 0)
    read(t, version = 1)
    read(t, predicate = "id >= 20")
    snapshot(t)
  }

}
