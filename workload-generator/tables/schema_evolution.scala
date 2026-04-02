new WorkloadSuite("schema_evolution") {

  test("schema_add_column", "ADD COLUMN", "schema_evolution") { w =>
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

  test("schema_add_nested_field", "Add field to nested struct", "schema_evolution") { w =>
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

  test("schema_rename", "RENAME COLUMN", "schema_evolution", "column_mapping") { w =>
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

  test("schema_drop_column", "DROP COLUMN", "schema_evolution", "column_mapping") { w =>
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

  test("schema_multiple_renames", "Chain of renames a→b→c", "schema_evolution", "column_mapping") { w =>
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

  test("schema_predicate_on_added", "Predicate on null-filled added column", "schema_evolution") { w =>
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

  // --- New workloads below ---

  test("schema_add_col_pred_eq", "Equality predicate on column missing stats in old files", "schema_evolution") { w =>
    w.sql("CREATE TABLE tbl (id INT, value STRING) USING delta")
    w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
    w.sql("ALTER TABLE tbl ADD COLUMNS (score INT)")
    w.sql("INSERT INTO tbl VALUES (4,'d',100),(5,'e',200),(6,'f',300)")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "score = 200")
    w.read(t, predicate = "score = 100 OR score IS NULL")
    w.snapshotHistory(t)
  }

  test("schema_drop_col_pred", "Data skipping after column drop", "schema_evolution", "column_mapping") { w =>
    w.sql("""CREATE TABLE tbl (id INT, name STRING, category STRING) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    w.sql("INSERT INTO tbl VALUES (1,'alice','A'),(2,'bob','B'),(3,'charlie','A')")
    w.sql("ALTER TABLE tbl DROP COLUMN category")
    w.sql("INSERT INTO tbl VALUES (4,'diana'),(5,'eve')")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "id > 3")
    w.read(t, predicate = "name = 'alice'")
    w.snapshotHistory(t)
  }

  test("schema_rename_pred", "Predicate on renamed column", "schema_evolution", "column_mapping") { w =>
    w.sql("""CREATE TABLE tbl (id INT, old_name STRING) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    w.sql("INSERT INTO tbl VALUES (1,'alice'),(2,'bob'),(3,'charlie')")
    w.sql("ALTER TABLE tbl RENAME COLUMN old_name TO new_name")
    w.sql("INSERT INTO tbl VALUES (4,'diana')")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "new_name = 'alice'")
    w.read(t, predicate = "new_name = 'diana'")
    w.snapshotHistory(t)
  }

  test("schema_rename_partition", "Partition pruning on renamed partition column", "schema_evolution", "column_mapping") { w =>
    w.sql("""CREATE TABLE tbl (id INT, category STRING) USING delta
      PARTITIONED BY (category)
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    w.sql("INSERT INTO tbl VALUES (1,'A'),(2,'B'),(3,'A'),(4,'C')")
    w.sql("ALTER TABLE tbl RENAME COLUMN category TO cat")
    w.sql("INSERT INTO tbl VALUES (5,'A'),(6,'C')")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "cat = 'A'")
    w.read(t, predicate = "cat = 'C'")
    w.snapshotHistory(t)
  }

  test("schema_drop_readd_same_name", "Drop and re-add column with different type", "schema_evolution", "column_mapping") { w =>
    w.sql("""CREATE TABLE tbl (id INT, x STRING) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    w.sql("INSERT INTO tbl VALUES (1,'hello'),(2,'world')")
    w.sql("ALTER TABLE tbl DROP COLUMN x")
    w.sql("ALTER TABLE tbl ADD COLUMN (x INT)")
    w.sql("INSERT INTO tbl VALUES (3,100),(4,200)")
    val t = w.table("tbl")
    w.read(t)
    w.snapshotHistory(t)
  }

  test("schema_readd_pred", "Predicate on re-added column (new physical name)", "schema_evolution", "column_mapping") { w =>
    w.sql("""CREATE TABLE tbl (id INT, x STRING) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    w.sql("INSERT INTO tbl VALUES (1,'old1'),(2,'old2')")
    w.sql("ALTER TABLE tbl DROP COLUMN x")
    w.sql("ALTER TABLE tbl ADD COLUMN (x INT)")
    w.sql("INSERT INTO tbl VALUES (3,100),(4,200)")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "x = 200")
    w.read(t, predicate = "x IS NULL")
    w.snapshotHistory(t)
  }

  test("schema_dv_pred_null", "Null-fill predicate + DVs combined", "schema_evolution", "dv") { w =>
    w.sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1,'alice'),(2,'bob'),(3,'charlie'),(4,'diana')")
    w.sql("ALTER TABLE tbl ADD COLUMNS (score INT)")
    w.sql("INSERT INTO tbl VALUES (5,'eve',90),(6,'frank',80)")
    w.sql("DELETE FROM tbl WHERE id IN (2, 5)")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "score IS NULL")
    w.read(t, predicate = "score IS NOT NULL")
    w.snapshotHistory(t)
  }

  test("schema_rename_read_v1", "Version read before rename", "schema_evolution", "column_mapping") { w =>
    w.sql("""CREATE TABLE tbl (id INT, old_name STRING) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    w.sql("INSERT INTO tbl VALUES (1,'before_rename')")
    w.sql("ALTER TABLE tbl RENAME COLUMN old_name TO new_name")
    w.sql("INSERT INTO tbl VALUES (2,'after_rename')")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, version = 1)
    w.snapshotHistory(t)
  }

  test("schema_nested_field_pred", "Predicate on added nested field", "schema_evolution") { w =>
    w.sql("CREATE TABLE tbl (id INT, info STRUCT<name: STRING>) USING delta")
    w.sql("INSERT INTO tbl VALUES (1, named_struct('name','alice'))")
    w.sql("ALTER TABLE tbl ADD COLUMNS (info.email STRING)")
    w.sql("INSERT INTO tbl VALUES (2, named_struct('name','bob','email','bob@x.com'))")
    w.sql("INSERT INTO tbl VALUES (3, named_struct('name','charlie','email','charlie@y.com'))")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "info.email IS NULL")
    w.read(t, predicate = "info.email IS NOT NULL")
    w.snapshotHistory(t)
  }

  test("schema_proj_at_old_version", "Project column at version before schema change", "schema_evolution") { w =>
    w.sql("CREATE TABLE tbl (id INT, value STRING) USING delta")
    w.sql("INSERT INTO tbl VALUES (1,'one'),(2,'two')")
    w.sql("ALTER TABLE tbl ADD COLUMNS (extra INT)")
    w.sql("INSERT INTO tbl VALUES (3,'three',300)")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, version = 1, columns = Seq("id", "value"))
    w.read(t, columns = Seq("id", "extra"))
    w.snapshotHistory(t)
  }

  test("schema_type_coercion_insert", "INSERT with implicit type cast int to long", "schema_evolution") { w =>
    w.sql("CREATE TABLE tbl (id LONG, value LONG) USING delta")
    w.sql("INSERT INTO tbl VALUES (1, 100)")
    // Insert int values into long columns (implicit coercion)
    w.sql("INSERT INTO tbl SELECT CAST(2 AS INT), CAST(200 AS INT)")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "value > 150")
    w.snapshotHistory(t)
  }

  test("schema_merge_with_evolution", "MERGE with schema evolution", "schema_evolution") { w =>
    w.sql("""CREATE TABLE target (id INT, name STRING) USING delta
      TBLPROPERTIES ('delta.enableTypeWidening' = 'false')""")
    w.sql("INSERT INTO target VALUES (1,'alice'),(2,'bob')")
    w.sql("CREATE TABLE src (id INT, name STRING, score INT) USING delta")
    w.sql("INSERT INTO src VALUES (2,'bob_updated',95),(3,'charlie',88)")
    w.sql("""MERGE INTO target t USING src s ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *""")
    val t = w.table("target")
    w.read(t)
    w.read(t, columns = Seq("id", "score"))
    w.snapshotHistory(t)
  }

  // --- se_* named workloads (matching acceptance_workloads directories) ---

  test("se_add_col_pred_eq", "Equality predicate on column missing stats in old files", "schema_evolution", "predicate") { w =>
    w.sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1, 'alice')")
    w.sql("INSERT INTO tbl VALUES (2, 'bob')")
    w.sql("ALTER TABLE tbl ADD COLUMNS (score INT)")
    w.sql("INSERT INTO tbl VALUES (3, 'charlie', 95)")
    w.sql("INSERT INTO tbl VALUES (4, 'diana', 88)")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "score = 0")
    w.read(t, predicate = "score = 95")
    w.snapshot(t)
  }

  test("se_add_col_pred_null", "Predicate on null-filled column from old files", "schema_evolution", "predicate") { w =>
    w.sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1, 'alice')")
    w.sql("INSERT INTO tbl VALUES (2, 'bob')")
    w.sql("ALTER TABLE tbl ADD COLUMNS (score INT)")
    w.sql("INSERT INTO tbl VALUES (3, 'charlie', 95)")
    w.sql("INSERT INTO tbl VALUES (4, 'diana', 88)")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "score > 90")
    w.read(t, predicate = "score IS NOT NULL")
    w.read(t, predicate = "score IS NULL")
    w.snapshot(t)
  }

  test("se_add_col_read_v1", "Time travel to version before column was added", "schema_evolution", "timeTravel") { w =>
    w.sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1, 'alice')")
    w.sql("INSERT INTO tbl VALUES (2, 'bob')")
    w.sql("ALTER TABLE tbl ADD COLUMNS (score INT)")
    w.sql("INSERT INTO tbl VALUES (3, 'charlie', 95)")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, version = 1)
    w.snapshot(t)
    w.snapshotHistory(t)
  }

  test("se_add_column_with_default", "Column added then populated with explicit values", "schema_evolution") { w =>
    w.sql("""CREATE TABLE tbl (id INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1)")
    w.sql("INSERT INTO tbl VALUES (2)")
    w.sql("ALTER TABLE tbl ADD COLUMNS (status STRING)")
    w.sql("INSERT INTO tbl VALUES (3, 'active')")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, columns = Seq("id", "status"))
    w.snapshot(t)
  }

  test("se_drop_column", "Read after column drop with column mapping", "schemaEvolution") { w =>
    w.sql("""CREATE TABLE tbl (id INT, name STRING, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true',
        'delta.columnMapping.mode' = 'name')""")
    w.sql("INSERT INTO tbl VALUES (1, 'alice', 'v1')")
    w.sql("INSERT INTO tbl VALUES (2, 'bob', 'v2')")
    w.sql("ALTER TABLE tbl DROP COLUMN value")
    w.sql("INSERT INTO tbl VALUES (3, 'charlie')")
    val t = w.table("tbl")
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("se_drop_col_pred", "Data skipping after column drop", "schema_evolution", "column_mapping") { w =>
    w.sql("""CREATE TABLE tbl (id INT, name STRING, value INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true',
        'delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    w.sql("INSERT INTO tbl VALUES (1, 'alice', 10)")
    w.sql("INSERT INTO tbl VALUES (2, 'bob', 20)")
    w.sql("INSERT INTO tbl VALUES (3, 'charlie', 30)")
    w.sql("ALTER TABLE tbl DROP COLUMN value")
    w.sql("INSERT INTO tbl VALUES (4, 'diana')")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "id > 2")
    w.read(t, predicate = "name = 'alice'")
    w.snapshot(t)
    w.snapshotHistory(t)
  }

  test("se_drop_col_read_v1", "Time travel to version before column was dropped", "schema_evolution", "column_mapping") { w =>
    w.sql("""CREATE TABLE tbl (id INT, name STRING, value INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true',
        'delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    w.sql("INSERT INTO tbl VALUES (1, 'alice', 10)")
    w.sql("INSERT INTO tbl VALUES (2, 'bob', 20)")
    w.sql("ALTER TABLE tbl DROP COLUMN value")
    w.sql("INSERT INTO tbl VALUES (3, 'charlie')")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, version = 1)
    w.snapshot(t)
    w.snapshotHistory(t)
  }

  test("se_drop_readd_same_name", "Drop and re-add column with different type", "schema_evolution", "column_mapping") { w =>
    w.sql("""CREATE TABLE tbl (id INT, x STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true',
        'delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    w.sql("INSERT INTO tbl VALUES (1, 'hello')")
    w.sql("ALTER TABLE tbl DROP COLUMN x")
    w.sql("ALTER TABLE tbl ADD COLUMN (x INT)")
    w.sql("INSERT INTO tbl VALUES (2, 42)")
    val t = w.table("tbl")
    w.read(t)
    w.snapshot(t)
  }

  test("se_dv_pred_null", "Null-fill predicate + DVs combined", "schema_evolution", "dv") { w =>
    w.sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1, 'alice')")
    w.sql("INSERT INTO tbl VALUES (2, 'bob')")
    w.sql("INSERT INTO tbl VALUES (3, 'charlie')")
    w.sql("ALTER TABLE tbl ADD COLUMNS (score INT)")
    w.sql("INSERT INTO tbl VALUES (4, 'diana', 95)")
    w.sql("INSERT INTO tbl VALUES (5, 'eve', 88)")
    w.sql("""ALTER TABLE tbl SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("DELETE FROM tbl WHERE id = 2")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "score IS NOT NULL")
    w.read(t, predicate = "score IS NULL")
    w.snapshot(t)
  }

  test("se_merge_with_evolution", "MERGE with schema evolution", "schema_evolution") { w =>
    w.sql("""CREATE TABLE target (id INT, name STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO target VALUES (1, 'alice')")
    w.sql("INSERT INTO target VALUES (2, 'bob')")
    w.sql("CREATE TABLE src (id INT, name STRING, score DOUBLE) USING delta")
    w.sql("INSERT INTO src VALUES (2, 'bob_updated', 95.0), (3, 'charlie', 87.5)")
    w.sql("""MERGE INTO target t USING src s ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *""")
    val t = w.table("target")
    w.read(t)
    w.read(t, columns = Seq("id", "name", "score"))
    w.snapshot(t)
  }

  test("se_nested_field_pred", "Predicate on added nested field", "schema_evolution") { w =>
    w.sql("""CREATE TABLE tbl (id INT, info STRUCT<name: STRING, age: INT>) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1, named_struct('name','alice','age',30))")
    w.sql("INSERT INTO tbl VALUES (2, named_struct('name','bob','age',25))")
    w.sql("ALTER TABLE tbl ADD COLUMNS (info.email STRING)")
    w.sql("INSERT INTO tbl VALUES (3, named_struct('name','charlie','age',35,'email','c@test.com'))")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "info.email IS NOT NULL")
    w.read(t, predicate = "info.email IS NULL")
    w.snapshot(t)
  }

  test("se_nested_field_project", "Nested struct projection with null-fill for old files", "schema_evolution") { w =>
    w.sql("""CREATE TABLE tbl (id INT, info STRUCT<name: STRING, age: INT>) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1, named_struct('name','alice','age',30))")
    w.sql("INSERT INTO tbl VALUES (2, named_struct('name','bob','age',25))")
    w.sql("ALTER TABLE tbl ADD COLUMNS (info.email STRING)")
    w.sql("INSERT INTO tbl VALUES (3, named_struct('name','charlie','age',35,'email','c@test.com'))")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, columns = Seq("id"))
    w.read(t, columns = Seq("id", "info"))
    w.snapshot(t)
  }

  test("se_pred_on_added_col", "IS NOT NULL on added column", "schema_evolution", "predicate") { w =>
    w.sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1, 'alice')")
    w.sql("INSERT INTO tbl VALUES (2, 'bob')")
    w.sql("ALTER TABLE tbl ADD COLUMNS (score INT)")
    w.sql("INSERT INTO tbl VALUES (3, 'charlie', 95)")
    w.sql("INSERT INTO tbl VALUES (4, 'diana', 88)")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "score IS NOT NULL")
    w.snapshot(t)
  }

  test("se_proj_at_old_version", "Project column at version before schema change", "schema_evolution") { w =>
    w.sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1, 'alice')")
    w.sql("INSERT INTO tbl VALUES (2, 'bob')")
    w.sql("ALTER TABLE tbl ADD COLUMNS (score INT)")
    w.sql("INSERT INTO tbl VALUES (3, 'charlie', 95)")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, version = 1, columns = Seq("id"))
    w.read(t, version = 2, columns = Seq("id", "name"))
    w.snapshot(t)
    w.snapshotHistory(t)
  }

  test("se_readd_pred", "Predicate on re-added column (new physical name)", "schema_evolution", "column_mapping") { w =>
    w.sql("""CREATE TABLE tbl (id INT, x STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true',
        'delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    w.sql("INSERT INTO tbl VALUES (1, 'old')")
    w.sql("ALTER TABLE tbl DROP COLUMN x")
    w.sql("ALTER TABLE tbl ADD COLUMN (x INT)")
    w.sql("INSERT INTO tbl VALUES (2, 200)")
    w.sql("INSERT INTO tbl VALUES (3, 300)")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "x = 200")
    w.read(t, predicate = "x IS NULL")
    w.snapshot(t)
    w.snapshotHistory(t)
  }

  test("se_rename_chain", "Multiple renames a -> b -> c", "schema_evolution", "column_mapping") { w =>
    w.sql("""CREATE TABLE tbl (id INT, a STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true',
        'delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    w.sql("INSERT INTO tbl VALUES (1, 'first')")
    w.sql("ALTER TABLE tbl RENAME COLUMN a TO b")
    w.sql("INSERT INTO tbl VALUES (2, 'second')")
    w.sql("ALTER TABLE tbl RENAME COLUMN b TO c")
    w.sql("INSERT INTO tbl VALUES (3, 'third')")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, columns = Seq("id", "c"))
    w.snapshot(t)
  }

  test("se_rename_column", "Read after column rename", "schema_evolution", "column_mapping") { w =>
    w.sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true',
        'delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    w.sql("INSERT INTO tbl VALUES (1, 'alice')")
    w.sql("INSERT INTO tbl VALUES (2, 'bob')")
    w.sql("ALTER TABLE tbl RENAME COLUMN name TO full_name")
    w.sql("INSERT INTO tbl VALUES (3, 'charlie')")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, columns = Seq("id", "full_name"))
    w.snapshot(t)
  }

  test("se_rename_part_pred", "Partition pruning on renamed partition column", "schema_evolution", "column_mapping") { w =>
    w.sql("""CREATE TABLE tbl (id INT, category STRING, value INT) USING delta
      PARTITIONED BY (category)
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true',
        'delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    w.sql("INSERT INTO tbl VALUES (1, 'A', 100)")
    w.sql("INSERT INTO tbl VALUES (2, 'B', 200)")
    w.sql("INSERT INTO tbl VALUES (3, 'A', 300)")
    w.sql("ALTER TABLE tbl RENAME COLUMN category TO cat")
    w.sql("INSERT INTO tbl VALUES (4, 'C', 400)")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "cat = 'A'")
    w.read(t, predicate = "cat = 'C'")
    w.snapshot(t)
    w.snapshotHistory(t)
  }

  test("se_rename_partition_column", "Rename partition column", "schema_evolution", "column_mapping") { w =>
    w.sql("""CREATE TABLE tbl (id INT, category STRING, value INT) USING delta
      PARTITIONED BY (category)
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true',
        'delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    w.sql("INSERT INTO tbl VALUES (1, 'A', 100)")
    w.sql("INSERT INTO tbl VALUES (2, 'B', 200)")
    w.sql("ALTER TABLE tbl RENAME COLUMN category TO group_name")
    w.sql("INSERT INTO tbl VALUES (3, 'C', 300)")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, columns = Seq("id", "group_name", "value"))
    w.snapshot(t)
  }

  test("se_rename_pred", "Predicate on renamed column", "schema_evolution", "column_mapping") { w =>
    w.sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true',
        'delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    w.sql("INSERT INTO tbl VALUES (1, 'alice')")
    w.sql("INSERT INTO tbl VALUES (2, 'bob')")
    w.sql("ALTER TABLE tbl RENAME COLUMN name TO full_name")
    w.sql("INSERT INTO tbl VALUES (3, 'charlie')")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "full_name = 'alice'")
    w.read(t, predicate = "full_name LIKE '%ob'")
    w.snapshot(t)
    w.snapshotHistory(t)
  }

  test("se_rename_read_v1", "Version read before rename", "schema_evolution", "column_mapping") { w =>
    w.sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true',
        'delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    w.sql("INSERT INTO tbl VALUES (1, 'alice')")
    w.sql("INSERT INTO tbl VALUES (2, 'bob')")
    w.sql("ALTER TABLE tbl RENAME COLUMN name TO full_name")
    w.sql("INSERT INTO tbl VALUES (3, 'charlie')")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, version = 1)
    w.snapshot(t)
    w.snapshotHistory(t)
  }

  test("se_type_coercion_insert", "INSERT with implicit type cast int to long", "schema_evolution") { w =>
    w.sql("""CREATE TABLE tbl (id LONG, value LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1, 100)")
    w.sql("INSERT INTO tbl VALUES (2, 200)")
    w.sql("INSERT INTO tbl VALUES (3, 300)")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "value > 150")
    w.snapshot(t)
  }

  test("se_add_nested_struct_field", "Add field to nested struct", "schema_evolution") { w =>
    w.sql("""CREATE TABLE tbl (id INT, info STRUCT<name: STRING, age: INT>) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1, named_struct('name','alice','age',30))")
    w.sql("INSERT INTO tbl VALUES (2, named_struct('name','bob','age',25))")
    w.sql("ALTER TABLE tbl ADD COLUMNS (info.email STRING)")
    w.sql("INSERT INTO tbl VALUES (3, named_struct('name','charlie','age',35,'email','c@test.com'))")
    val t = w.table("tbl")
    w.read(t)
    w.snapshot(t)
  }

  test("se_drop_and_readd_same_name", "Drop and re-add column with different type", "schema_evolution", "column_mapping") { w =>
    w.sql("""CREATE TABLE tbl (id INT, x STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true',
        'delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    w.sql("INSERT INTO tbl VALUES (1, 'hello')")
    w.sql("ALTER TABLE tbl DROP COLUMN x")
    w.sql("ALTER TABLE tbl ADD COLUMN (x INT)")
    w.sql("INSERT INTO tbl VALUES (2, 42)")
    val t = w.table("tbl")
    w.read(t)
    w.snapshot(t)
  }

  test("se_add_top_level_column", "Add top-level column via auto merge", "schema_evolution") { w =>
    w.sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true',
        'delta.enableTypeWidening' = 'false')""")
    w.sql("INSERT INTO tbl VALUES (1, 'alice')")
    w.sql("INSERT INTO tbl VALUES (2, 'bob')")
    w.sql("ALTER TABLE tbl ADD COLUMNS (age INT)")
    w.sql("INSERT INTO tbl VALUES (3, 'charlie', 30)")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, columns = Seq("id", "name", "age"))
    w.snapshot(t)
  }

}.runAll()
