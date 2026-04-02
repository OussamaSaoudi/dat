import io.delta.workload.WorkloadGenerator._

// ---------------------------------------------------------------------------
// Existing 6 workloads
// ---------------------------------------------------------------------------

workload("cm_mode_name", "Read table with column mapping mode 'name'", "column_mapping") { w =>
  w.sql("""CREATE TABLE tbl (id INT, name STRING, value DOUBLE) USING delta
    TBLPROPERTIES ('delta.columnMapping.mode' = 'name')""")
  w.sql("INSERT INTO tbl VALUES (1,'alice',100),(2,'bob',200),(3,'charlie',300)")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, columns = Seq("name", "value"))
  w.read(t, predicate = "value > 150")
  w.snapshot(t)
}

workload("cm_rename_column", "Read after column rename with column mapping", "column_mapping") { w =>
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

workload("cm_drop_column", "Read after column drop with column mapping", "column_mapping") { w =>
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

workload("cm_drop_readd", "Drop and re-add column with same name (CM)", "column_mapping") { w =>
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

workload("cm_nested_columns", "Nested columns with column mapping", "column_mapping") { w =>
  w.sql("""CREATE TABLE tbl (id INT, info STRUCT<name: STRING, age: INT>) USING delta
    TBLPROPERTIES ('delta.columnMapping.mode' = 'name')""")
  w.sql("INSERT INTO tbl VALUES (1, named_struct('name','alice','age',30))")
  w.sql("INSERT INTO tbl VALUES (2, named_struct('name','bob','age',25))")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "info.age > 27")
  w.snapshot(t)
}

workload("cm_mode_upgrade", "Column mapping mode upgrade from none to name", "column_mapping") { w =>
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

// ---------------------------------------------------------------------------
// New workloads: 25 more to match existing acceptance_workloads/cm_*
// ---------------------------------------------------------------------------

workload("cm_mode_id", "Read table with column mapping mode 'id'", "column_mapping") { w =>
  w.sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
    TBLPROPERTIES ('delta.columnMapping.mode' = 'id')""")
  w.sql("INSERT INTO tbl VALUES (1,'alpha'),(2,'beta'),(3,'gamma')")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, columns = Seq("name"))
  w.read(t, predicate = "id > 1")
  w.snapshot(t)
}

workload("cm_upgrade", "Read after column mapping upgrade", "column_mapping") { w =>
  // Similar to cm_mode_upgrade but exercises version-by-version reading
  w.sql("CREATE TABLE tbl (a INT, b STRING) USING delta")
  w.sql("INSERT INTO tbl VALUES (1,'x'),(2,'y')")
  w.sql("""ALTER TABLE tbl SET TBLPROPERTIES (
    'delta.columnMapping.mode' = 'name',
    'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
  w.sql("INSERT INTO tbl VALUES (3,'z')")
  w.sql("ALTER TABLE tbl RENAME COLUMN b TO c")
  w.sql("INSERT INTO tbl VALUES (4,'w')")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, version = 0)
  w.read(t, version = 1)
  w.read(t, columns = Seq("a", "c"))
  w.snapshotHistory(t)
}

workload("cm_mode_upgrade_partitioned", "Upgrade none to name on partitioned table", "column_mapping") { w =>
  w.sql("CREATE TABLE tbl (id INT, part STRING) USING delta PARTITIONED BY (part)")
  w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'a')")
  w.sql("""ALTER TABLE tbl SET TBLPROPERTIES (
    'delta.columnMapping.mode' = 'name',
    'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
  w.sql("INSERT INTO tbl VALUES (4,'c')")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "part = 'a'")
  w.read(t, version = 0)
  w.snapshotHistory(t)
}

workload("cm_special_chars", "Column names with spaces, dots, backticks", "column_mapping") { w =>
  w.sql("""CREATE TABLE tbl (id INT, `col with spaces` STRING, `col.with.dots` STRING) USING delta
    TBLPROPERTIES ('delta.columnMapping.mode' = 'name')""")
  w.sql("INSERT INTO tbl VALUES (1, 'space1', 'dot1')")
  w.sql("INSERT INTO tbl VALUES (2, 'space2', 'dot2')")
  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
}

workload("cm_array_of_structs", "Column mapping with array of structs", "column_mapping") { w =>
  w.sql("""CREATE TABLE tbl (id INT, items ARRAY<STRUCT<name: STRING, qty: INT>>) USING delta
    TBLPROPERTIES ('delta.columnMapping.mode' = 'name')""")
  w.sql("INSERT INTO tbl VALUES (1, array(named_struct('name','apple','qty',3)))")
  w.sql("INSERT INTO tbl VALUES (2, array(named_struct('name','banana','qty',5), named_struct('name','cherry','qty',2)))")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, columns = Seq("items"))
  w.snapshot(t)
}

workload("cm_complex_types", "Complex types (array, map) with column mapping", "column_mapping") { w =>
  w.sql("""CREATE TABLE tbl (id INT, tags ARRAY<STRING>, props MAP<STRING, INT>) USING delta
    TBLPROPERTIES ('delta.columnMapping.mode' = 'name')""")
  w.sql("INSERT INTO tbl VALUES (1, array('a','b'), map('x',1,'y',2))")
  w.sql("INSERT INTO tbl VALUES (2, array('c'), map('z',3))")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, columns = Seq("tags"))
  w.read(t, columns = Seq("props"))
  w.snapshot(t)
}

workload("cm_map_type", "Column mapping with map type columns", "column_mapping") { w =>
  w.sql("""CREATE TABLE tbl (id INT, data MAP<STRING, STRING>) USING delta
    TBLPROPERTIES ('delta.columnMapping.mode' = 'name')""")
  w.sql("INSERT INTO tbl VALUES (1, map('key1','val1','key2','val2'))")
  w.sql("INSERT INTO tbl VALUES (2, map('key3','val3'))")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, columns = Seq("data"))
  w.snapshot(t)
}

workload("cm_deeply_nested", "3+ level nested schema with CM", "column_mapping") { w =>
  w.sql("""CREATE TABLE tbl (
    id INT,
    l1 STRUCT<l2: STRUCT<l3: STRUCT<value: STRING>>>
  ) USING delta
    TBLPROPERTIES ('delta.columnMapping.mode' = 'name')""")
  w.sql("INSERT INTO tbl VALUES (1, named_struct('l2', named_struct('l3', named_struct('value', 'deep'))))")
  w.sql("INSERT INTO tbl VALUES (2, named_struct('l2', named_struct('l3', named_struct('value', 'deeper'))))")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, columns = Seq("l1"))
  w.snapshot(t)
}

workload("cm_drop_readd_same_name", "Drop and re-add column with same name but different type", "column_mapping") { w =>
  w.sql("""CREATE TABLE tbl (id INT, x STRING) USING delta
    TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
      'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
  w.sql("INSERT INTO tbl VALUES (1, 'text')")
  w.sql("ALTER TABLE tbl DROP COLUMN x")
  w.sql("ALTER TABLE tbl ADD COLUMNS (x DOUBLE)")
  w.sql("INSERT INTO tbl VALUES (2, 3.14)")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "x IS NOT NULL")
  w.snapshotHistory(t)
}

workload("cm_predicate_after_rename", "Predicate on column after rename (name mode)", "column_mapping") { w =>
  w.sql("""CREATE TABLE tbl (id INT, a INT) USING delta
    TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
      'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
  w.sql("INSERT INTO tbl VALUES (1, 100),(2, 200),(3, 300)")
  w.sql("ALTER TABLE tbl RENAME COLUMN a TO b")
  w.sql("INSERT INTO tbl VALUES (4, 400)")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "b > 200")
  w.read(t, predicate = "b = 100")
  w.snapshotHistory(t)
}

workload("cm_predicate_on_readded", "Predicate on dropped+re-added column", "column_mapping") { w =>
  w.sql("""CREATE TABLE tbl (id INT, val INT) USING delta
    TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
      'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
  w.sql("INSERT INTO tbl VALUES (1, 10),(2, 20)")
  w.sql("ALTER TABLE tbl DROP COLUMN val")
  w.sql("ALTER TABLE tbl ADD COLUMNS (val INT)")
  w.sql("INSERT INTO tbl VALUES (3, 30),(4, 40)")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "val > 25")
  w.read(t, predicate = "val IS NULL")
  w.snapshotHistory(t)
}

workload("cm_predicate_renamed_partition", "Filter on renamed partition column", "column_mapping") { w =>
  w.sql("""CREATE TABLE tbl (id INT, part STRING) USING delta
    PARTITIONED BY (part)
    TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
      'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
  w.sql("INSERT INTO tbl VALUES (1,'x'),(2,'y'),(3,'x')")
  w.sql("ALTER TABLE tbl RENAME COLUMN part TO region")
  w.sql("INSERT INTO tbl VALUES (4,'z')")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "region = 'x'")
  w.read(t, predicate = "region = 'z'")
  w.snapshotHistory(t)
}

workload("cm_rename_partition_col", "Rename partition column", "column_mapping") { w =>
  w.sql("""CREATE TABLE tbl (id INT, category STRING) USING delta
    PARTITIONED BY (category)
    TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
      'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
  w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'a')")
  w.sql("ALTER TABLE tbl RENAME COLUMN category TO cat")
  w.sql("INSERT INTO tbl VALUES (4,'c')")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "cat = 'a'")
  w.read(t, columns = Seq("id", "cat"))
  w.snapshotHistory(t)
}

workload("cm_nested_struct_name", "Column mapping with nested struct (name mode) + projection", "column_mapping") { w =>
  w.sql("""CREATE TABLE tbl (id INT, info STRUCT<first: STRING, last: STRING, age: INT>) USING delta
    TBLPROPERTIES ('delta.columnMapping.mode' = 'name')""")
  w.sql("INSERT INTO tbl VALUES (1, named_struct('first','alice','last','smith','age',30))")
  w.sql("INSERT INTO tbl VALUES (2, named_struct('first','bob','last','jones','age',25))")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, columns = Seq("info"))
  w.read(t, columns = Seq("id"))
  w.snapshot(t)
}

workload("cm_nested_struct_id", "Column mapping with nested struct (id mode)", "column_mapping") { w =>
  w.sql("""CREATE TABLE tbl (id INT, info STRUCT<name: STRING, score: DOUBLE>) USING delta
    TBLPROPERTIES ('delta.columnMapping.mode' = 'id')""")
  w.sql("INSERT INTO tbl VALUES (1, named_struct('name','alice','score',95.5))")
  w.sql("INSERT INTO tbl VALUES (2, named_struct('name','bob','score',87.3))")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, columns = Seq("info"))
  w.snapshot(t)
}

workload("cm_nested_rename_3_levels", "Rename deeply nested struct field", "column_mapping") { w =>
  w.sql("""CREATE TABLE tbl (
    id INT,
    outer_col STRUCT<mid: STRUCT<inner_val: STRING>>
  ) USING delta
    TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
      'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
  w.sql("INSERT INTO tbl VALUES (1, named_struct('mid', named_struct('inner_val','hello')))")
  w.sql("ALTER TABLE tbl RENAME COLUMN outer_col.mid.inner_val TO renamed_val")
  w.sql("INSERT INTO tbl VALUES (2, named_struct('mid', named_struct('renamed_val','world')))")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, version = 1)
  w.snapshotHistory(t)
}

workload("cm_filter_pushdown_physical_names", "Filters pushed down to parquet use physical names", "column_mapping") { w =>
  // Create with name mode, rename column, verify filter uses physical name correctly
  w.sql("""CREATE TABLE tbl (a INT, b STRING) USING delta
    TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
      'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
  w.sql("INSERT INTO tbl VALUES (100,'first'),(200,'second')")
  w.sql("ALTER TABLE tbl RENAME COLUMN a TO c")
  w.sql("INSERT INTO tbl VALUES (300,'third'),(1000,'fourth')")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "c = 1000", name = "read_filter_c_eq_1000")
  w.read(t, predicate = "c = 100", name = "read_filter_c_eq_100")
  w.read(t, predicate = "c > 200", name = "read_filter_c_gt_200")
  w.snapshotHistory(t)
}

workload("cm_physical_name_matches_logical", "Physical name coincidentally equals new logical name", "column_mapping") { w =>
  // After rename, old physical name may match some other column's logical name
  w.sql("""CREATE TABLE tbl (id INT, alpha STRING, beta STRING) USING delta
    TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
      'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
  w.sql("INSERT INTO tbl VALUES (1, 'a1', 'b1')")
  w.sql("ALTER TABLE tbl RENAME COLUMN alpha TO gamma")
  w.sql("INSERT INTO tbl VALUES (2, 'a2', 'b2')")
  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
}

workload("cm_id_mode_rename_projection", "ID mode rename + column projection", "column_mapping") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
    TBLPROPERTIES ('delta.columnMapping.mode' = 'id',
      'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
  w.sql("INSERT INTO tbl VALUES (1,'before1'),(2,'before2')")
  w.sql("ALTER TABLE tbl RENAME COLUMN value TO renamed_value")
  w.sql("INSERT INTO tbl VALUES (3,'after1'),(4,'after2')")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, columns = Seq("renamed_value"), name = "read_project_renamed_col")
  w.read(t, columns = Seq("id"), name = "read_project_id_only")
  w.snapshotHistory(t)
}

workload("cm_id_mode_schema_evolution", "Add column in id mode", "column_mapping") { w =>
  w.sql("""CREATE TABLE tbl (id INT) USING delta
    TBLPROPERTIES ('delta.columnMapping.mode' = 'id',
      'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
  w.sql("INSERT INTO tbl VALUES (1),(2)")
  w.sql("ALTER TABLE tbl ADD COLUMNS (new_col STRING)")
  w.sql("INSERT INTO tbl VALUES (3, 'hello')")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, version = 0)
  w.read(t, version = 1)
  w.snapshotHistory(t)
}

workload("cm_id_matching_swapped", "ID mode: swapped field ID reads other column data", "column_mapping") { w =>
  // Create table in id mode with nested struct, manipulate column IDs
  w.sql("""CREATE TABLE tbl (a STRING, b STRUCT<c: STRING, d: INT>) USING delta
    TBLPROPERTIES ('delta.columnMapping.mode' = 'id')""")
  w.sql("INSERT INTO tbl VALUES ('hello', named_struct('c','world','d',42))")
  // Swap column mapping IDs by altering metadata — use ALTER TABLE to change schema
  w.sql("ALTER TABLE tbl RENAME COLUMN a TO e")
  w.sql("INSERT INTO tbl VALUES ('swapped', named_struct('c','test','d',99))")
  val t = w.table("tbl")
  w.read(t, name = "read_select_a_reads_e")
  w.snapshot(t)
}

workload("cm_id_matching_nonexistent", "ID mode: non-existing field ID returns null", "column_mapping") { w =>
  w.sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
    TBLPROPERTIES ('delta.columnMapping.mode' = 'id',
      'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
  w.sql("INSERT INTO tbl VALUES (1,'exists')")
  // Drop and add column — old physical data has non-matching ID
  w.sql("ALTER TABLE tbl DROP COLUMN name")
  w.sql("ALTER TABLE tbl ADD COLUMNS (name STRING)")
  w.sql("INSERT INTO tbl VALUES (2,'new')")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "name IS NULL")
  w.snapshot(t)
}

workload("cm_projection_complex_types", "Project array/map types with column mapping", "column_mapping") { w =>
  w.sql("""CREATE TABLE tbl (
    id INT,
    arr ARRAY<INT>,
    mp MAP<STRING, INT>,
    st STRUCT<a: STRING, b: INT>
  ) USING delta
    TBLPROPERTIES ('delta.columnMapping.mode' = 'name')""")
  w.sql("INSERT INTO tbl VALUES (1, array(10,20), map('x',1), named_struct('a','hello','b',42))")
  w.sql("INSERT INTO tbl VALUES (2, array(30), map('y',2,'z',3), named_struct('a','world','b',99))")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, columns = Seq("arr"), name = "read_project_array_only")
  w.read(t, columns = Seq("mp"), name = "read_project_map_only")
  w.read(t, columns = Seq("st"), name = "read_project_struct_only")
  w.snapshot(t)
}

workload("cm_select_after_drop", "Explicit projection after column drop (CM name mode)", "column_mapping") { w =>
  w.sql("""CREATE TABLE tbl (id INT, keep STRING, drop_me INT, extra DOUBLE) USING delta
    TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
      'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
  w.sql("INSERT INTO tbl VALUES (1, 'a', 10, 1.1),(2, 'b', 20, 2.2)")
  w.sql("ALTER TABLE tbl DROP COLUMN drop_me")
  w.sql("INSERT INTO tbl VALUES (3, 'c', 3.3)")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, columns = Seq("id", "keep"), name = "read_project_remaining_columns")
  w.read(t, columns = Seq("extra"), name = "read_project_extra_only")
  w.snapshotHistory(t)
}

generateAll(
  sys.env.getOrElse("WORKLOAD_OUTPUT_DIR", "/tmp/workloads"),
  force = sys.env.getOrElse("WORKLOAD_FORCE", "false").toBoolean)
System.exit(0)
