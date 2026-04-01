import io.delta.workload.WorkloadGenerator._

// -- rt_basic_read: basic row tracking table --
workload("rt_basic_read", "Write and read table without materialized columns", "rowTracking") { w =>
  w.sql("""CREATE TABLE tbl (test_data LONG) USING delta
    TBLPROPERTIES ('delta.enableRowTracking' = 'true', 'delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl SELECT id FROM range(100)")
  val t = w.table("tbl")
  w.read(t, version = 0)
  w.snapshot(t)
}

// -- rt_all_null_materialized: all-null materialized RT columns --
workload("rt_all_null_materialized", "Write and read table with all-null materialized columns", "rowTracking") { w =>
  w.sql("""CREATE TABLE tbl (test_data LONG) USING delta
    TBLPROPERTIES ('delta.enableRowTracking' = 'true', 'delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl SELECT id FROM range(100)")
  val t = w.table("tbl")
  w.read(t, version = 0)
  w.snapshot(t)
}

// -- rt_no_null_materialized: no-nulls materialized RT columns --
workload("rt_no_null_materialized", "Write and read table with no-nulls materialized columns", "rowTracking") { w =>
  w.sql("""CREATE TABLE tbl (test_data LONG) USING delta
    TBLPROPERTIES ('delta.enableRowTracking' = 'true', 'delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl SELECT id FROM range(100)")
  val t = w.table("tbl")
  w.read(t, version = 0)
  w.snapshot(t)
}

// -- rt_mixed_materialized: mixed materialized RT columns --
workload("rt_mixed_materialized", "Write and read table with mixed materialized columns", "rowTracking") { w =>
  w.sql("""CREATE TABLE tbl (test_data LONG) USING delta
    TBLPROPERTIES ('delta.enableRowTracking' = 'true', 'delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl SELECT id FROM range(100)")
  val t = w.table("tbl")
  w.read(t, version = 0)
  w.snapshot(t)
}

// -- rt_conflicting_columns: conflicting column names with RT --
workload("rt_conflicting_columns", "Write and read with conflicting columns", "rowTracking") { w =>
  w.sql("""CREATE TABLE tbl (id LONG) USING delta
    TBLPROPERTIES ('delta.enableRowTracking' = 'true', 'delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl SELECT id FROM range(10)")
  val t = w.table("tbl")
  w.read(t, version = 0)
  w.snapshot(t)
}

// -- rt_filter_read: read with filter on RT table --
workload("rt_filter_read", "Read mixed materialized columns with filter", "rowTracking") { w =>
  w.sql("""CREATE TABLE tbl (test_data LONG) USING delta
    TBLPROPERTIES ('delta.enableRowTracking' = 'true', 'delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl SELECT id FROM range(100)")
  val t = w.table("tbl")
  w.read(t, version = 0)
  w.read(t, version = 0, predicate = "test_data < 50")
  w.snapshot(t)
}

// -- rt_column_projection: column subset on RT table --
workload("rt_column_projection", "Column subset on RT table", "rowTracking") { w =>
  w.sql("""CREATE TABLE tbl (id INT, name STRING, value DOUBLE) USING delta
    TBLPROPERTIES ('delta.enableRowTracking' = 'true', 'delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1, 'alice', 1.0),(2, 'bob', 2.0),(3, 'charlie', 3.0)")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, columns = Seq("id", "value"))
  w.snapshot(t)
}

// -- rt_read_base_row_id: read base row IDs across inserts --
workload("rt_read_base_row_id", "Read base row IDs", "rowTracking") { w =>
  w.sql("""CREATE TABLE tbl (id LONG) USING delta
    TBLPROPERTIES ('delta.enableRowTracking' = 'true', 'delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl SELECT id FROM range(20)")
  w.sql("INSERT INTO tbl SELECT id + 20 FROM range(10)")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, version = 0)
  w.read(t, predicate = "id >= 15")
  w.snapshot(t)
}

// -- rt_read_row_id_and_index: row id and index after UPDATE --
workload("rt_read_row_id_and_index", "Read both row id and row index", "rowTracking") { w =>
  w.sql("""CREATE TABLE tbl (id LONG) USING delta
    TBLPROPERTIES ('delta.enableRowTracking' = 'true', 'delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl SELECT id FROM range(10)")
  w.sql("INSERT INTO tbl SELECT id + 10 FROM range(10)")
  w.sql("UPDATE tbl SET id = id + 100 WHERE id < 3")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, version = 0)
  w.read(t, version = 1)
  w.read(t, predicate = "id >= 100")
  w.snapshot(t)
}

// -- rt_across_schema_evolution: RT preserved after ADD COLUMN --
workload("rt_across_schema_evolution", "Row IDs preserved after ADD COLUMN", "rowTracking") { w =>
  w.sql("""CREATE TABLE tbl (id LONG) USING delta
    TBLPROPERTIES ('delta.enableRowTracking' = 'true', 'delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl SELECT id FROM range(10)")
  w.sql("ALTER TABLE tbl ADD COLUMN (name STRING)")
  w.sql("INSERT INTO tbl VALUES (100, 'new')")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, version = 0)
  w.read(t, predicate = "name IS NOT NULL")
  w.snapshot(t)
  w.snapshot(t, version = 0)
  w.snapshot(t, version = 1)
  w.snapshot(t, version = 2)
}

// -- rt_version_migration: upgrade from non-tracking to tracking --
workload("rt_version_migration", "Upgrade from non-tracking to tracking", "rowTracking") { w =>
  w.sql("""CREATE TABLE tbl (id LONG) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl SELECT id FROM range(20)")
  w.sql("INSERT INTO tbl SELECT id + 20 FROM range(10)")
  w.sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.enableRowTracking' = 'true')")
  w.sql("INSERT INTO tbl SELECT id + 30 FROM range(10)")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, version = 0)
  w.read(t, version = 1)
  w.read(t, predicate = "id >= 20")
  w.snapshot(t)
}

generateAll(
  sys.env.getOrElse("WORKLOAD_OUTPUT_DIR", "/tmp/workloads"),
  force = sys.env.getOrElse("WORKLOAD_FORCE", "false").toBoolean)
System.exit(0)
