import io.delta.workload.WorkloadGenerator._

// -- void_001_void_top_level: top-level NullType column --
workload("void_001_void_top_level", "Top-level NullType column", "void", "unsupportedType") { w =>
  w.sql("""CREATE TABLE tbl (id INT, void_col VOID) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl SELECT id, null FROM range(5)")
  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
}

// -- void_002_void_nested_struct: NullType inside struct --
workload("void_002_void_nested_struct", "NullType inside struct", "void", "unsupportedType") { w =>
  w.sql("""CREATE TABLE tbl (
    id INT,
    info STRUCT<name: STRING, void_field: VOID>
  ) USING delta TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("""INSERT INTO tbl SELECT id, named_struct('name', CAST(id AS STRING), 'void_field', null) FROM range(3)""")
  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
}

// -- void_005_void_schema_evolution: NullType added via schema evolution --
workload("void_005_void_schema_evolution", "NullType added via schema evolution", "void", "unsupportedType") { w =>
  w.sql("""CREATE TABLE tbl (id INT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1),(2),(3)")
  w.sql("ALTER TABLE tbl ADD COLUMN (void_col VOID)")
  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
}

// -- void_006_void_multiple_columns: multiple NullType columns --
workload("void_006_void_multiple_columns", "Multiple NullType columns", "void", "unsupportedType") { w =>
  w.sql("""CREATE TABLE tbl (id INT, void_a VOID, void_b VOID) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl SELECT id, null, null FROM range(3)")
  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
}

// -- void_007_void_with_backticks: NullType column with special name --
workload("void_007_void_with_backticks", "NullType column with special name", "void", "unsupportedType") { w =>
  w.sql("""CREATE TABLE tbl (id INT, `my.void` VOID) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl SELECT id, null FROM range(3)")
  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
}

// -- void_in_struct: NullType in struct field --
workload("void_in_struct", "NullType in struct field", "void", "unsupportedType") { w =>
  w.sql("""CREATE TABLE tbl (
    id INT,
    info STRUCT<label: STRING, void_val: VOID>
  ) USING delta TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("""INSERT INTO tbl SELECT id, named_struct('label', CAST(id AS STRING), 'void_val', null) FROM range(3)""")
  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
}

generateAll(
  sys.env.getOrElse("WORKLOAD_OUTPUT_DIR", "/tmp/workloads"),
  force = sys.env.getOrElse("WORKLOAD_FORCE", "false").toBoolean)
System.exit(0)
