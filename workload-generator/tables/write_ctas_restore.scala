/**
 * CTAS, Restore, Optimize, and Log Replay write workloads converted 1-to-1 from
 * runtime capture suites.
 *
 * Sources:
 *   - CTASWriteCaptureSuite.scala (CT-001 through CT-015)
 *   - RestoreWriteCaptureSuite.scala (RS-001 through RS-015)
 *   - OptimizeCompactionSuiteWriteCapture.scala (OPT-001 through OPT-005)
 *   - LogReplayWriteCapture.scala (LR-001 through LR-020)
 */

new WorkloadSuite("write_ctas_restore") {

  // ==========================================================================
  // CTAS Tests (CT-*)
  // Source: CTASWriteCaptureSuite.scala / DeltaTableCreationTests.scala
  // ==========================================================================

  // CT-001: Basic CTAS with DataFrame API (uses SQL equivalent)
  // NOTE: CTAS is not supported as structured op - commenting out
  // test("CT_001_basic_ctas", "Create table via CTAS with 3 rows",
  //     "write", "create", "ctas") {
  //   sql("CREATE TABLE tbl USING delta AS SELECT * FROM VALUES (1,'a'),(2,'b'),(3,'c') AS t(key, value)")
  //   val t = registerTable("tbl")
  //   writeSpec(t)
  //   read(t, name = "read_all")
  //   snapshot(t)
  // }

  // CT-002: CTAS with SQL syntax
  // NOTE: CTAS is not supported as structured op - commenting out
  // test("CT_002_ctas_sql", "CTAS via SQL with temp view source",
  //     "write", "create", "ctas", "sql") {
  //   sql("CREATE TEMPORARY VIEW source_ct002 AS SELECT * FROM VALUES (1,'a'),(2,'b'),(3,'c') AS t(key, value)")
  //   sql("CREATE TABLE tbl USING delta AS SELECT * FROM source_ct002")
  //   val t = registerTable("tbl")
  //   writeSpec(t)
  //   read(t, name = "read_all")
  //   snapshot(t)
  // }

  // CT-003: CTAS with partitioning
  // NOTE: CTAS is not supported as structured op - commenting out
  // test("CT_003_ctas_partitioned", "CTAS with PARTITIONED BY",
  //     "write", "create", "ctas", "partitioned") {
  //   sql("CREATE TEMPORARY VIEW source_ct003 AS SELECT * FROM VALUES (1,'a','p1'),(2,'b','p2'),(3,'c','p1') AS t(key, value, part)")
  //   sql("CREATE TABLE tbl USING delta PARTITIONED BY (part) AS SELECT * FROM source_ct003")
  //   val t = registerTable("tbl")
  //   writeSpec(t)
  //   read(t, name = "read_all")
  //   read(t, predicate = "part = 'p1'", name = "read_p1")
  //   read(t, predicate = "part = 'p2'", name = "read_p2")
  //   snapshot(t)
  // }

  // CT-004: CTAS with table properties
  // NOTE: CTAS is not supported as structured op - commenting out
  // test("CT_004_ctas_properties", "CTAS with appendOnly property",
  //     "write", "create", "ctas", "properties") {
  //   sql("""CREATE TABLE tbl USING delta
  //     TBLPROPERTIES ('delta.appendOnly' = 'true')
  //     AS SELECT * FROM VALUES (1,'a'),(2,'b'),(3,'c') AS t(key, value)""")
  //   val t = registerTable("tbl")
  //   writeSpec(t)
  //   read(t, name = "read_all")
  //   snapshot(t)
  // }

  // CT-005: INSERT INTO SELECT from another Delta table
  // NOTE: INSERT INTO SELECT requires sql() - cannot convert to insertOp
  test("CT_005_insert_select_delta", "Create target, insert from another table",
      "write", "create", "insert", "select") {
    val w = createTableOp("tbl",
      schema = "key INT, value STRING")
    insertOp(w, Seq(
      Map("key" -> 1, "value" -> "a"),
      Map("key" -> 2, "value" -> "b")))
    sql("CREATE TEMPORARY VIEW source_ct005 AS SELECT * FROM VALUES (3,'c'),(4,'d') AS t(key, value)")
    sql("INSERT INTO tbl SELECT * FROM source_ct005")
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    read(t, version = 1, name = "read_before_insert_select")
    snapshot(t)
  }

  // CT-006: INSERT INTO SELECT with transformation
  // NOTE: INSERT INTO SELECT with transformation requires sql()
  test("CT_006_insert_select_transform", "INSERT INTO SELECT with value * 2",
      "write", "insert", "select", "transform") {
    val w = createTableOp("tbl",
      schema = "key INT, value INT")
    insertOp(w, Seq(Map("key" -> 1, "value" -> 10)))
    sql("CREATE TEMPORARY VIEW source_ct006 AS SELECT * FROM VALUES (2,20),(3,30) AS t(key, value)")
    sql("INSERT INTO tbl SELECT key, value * 2 as value FROM source_ct006")
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  // CT-007: INSERT INTO SELECT with WHERE clause
  // NOTE: INSERT INTO SELECT with WHERE requires sql()
  test("CT_007_insert_select_filter", "INSERT INTO SELECT with WHERE value > 10",
      "write", "insert", "select", "filter") {
    val w = createTableOp("tbl",
      schema = "key INT, value INT")
    insertOp(w, Seq(Map("key" -> 1, "value" -> 100)))
    sql("CREATE TEMPORARY VIEW source_ct007 AS SELECT * FROM VALUES (2,5),(3,15),(4,25),(5,3) AS t(key, value)")
    sql("INSERT INTO tbl SELECT * FROM source_ct007 WHERE value > 10")
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  // CT-008: INSERT INTO SELECT with JOIN
  // NOTE: INSERT INTO SELECT with JOIN requires sql()
  test("CT_008_insert_select_join", "INSERT INTO SELECT with JOIN on two sources",
      "write", "insert", "select", "join") {
    val w = createTableOp("tbl",
      schema = "key INT, name STRING")
    sql("CREATE TEMPORARY VIEW table_a AS SELECT * FROM VALUES (1,100),(2,200),(3,300) AS t(key, amount)")
    sql("CREATE TEMPORARY VIEW table_b AS SELECT * FROM VALUES (1,'alice'),(2,'bob'),(4,'dave') AS t(key, name)")
    sql("INSERT INTO tbl SELECT a.key, b.name FROM table_a a JOIN table_b b ON a.key = b.key")
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  // CT-009: INSERT INTO SELECT with aggregation
  // NOTE: INSERT INTO SELECT with aggregation requires sql()
  test("CT_009_insert_select_aggregate", "INSERT INTO SELECT with SUM GROUP BY",
      "write", "insert", "select", "aggregate") {
    val w = createTableOp("tbl",
      schema = "category STRING, total BIGINT")
    sql("CREATE TEMPORARY VIEW source_ct009 AS SELECT * FROM VALUES ('electronics',100),('electronics',200),('books',50),('books',30) AS t(category, amount)")
    sql("INSERT INTO tbl SELECT category, SUM(amount) as total FROM source_ct009 GROUP BY category")
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  // CT-010: CREATE OR REPLACE TABLE (overwrite existing)
  // NOTE: INSERT OVERWRITE requires sql()
  test("CT_010_create_or_replace", "Create then overwrite with mode overwrite",
      "write", "create", "overwrite") {
    val w = createTableOp("tbl",
      schema = "key INT, value STRING")
    insertOp(w, Seq(
      Map("key" -> 1, "value" -> "old_a"),
      Map("key" -> 2, "value" -> "old_b")))
    sql("INSERT OVERWRITE tbl VALUES (10,'new_x'),(20,'new_y'),(30,'new_z')")
    val t = registerWriteSpec(w)
    read(t, name = "read_after_overwrite")
    read(t, version = 1, name = "read_before_overwrite")
    snapshot(t)
  }

  // CT-011: CTAS with multiple data types
  // NOTE: CTAS is not supported as structured op - commenting out
  // test("CT_011_ctas_mixed_types", "CTAS with INT, STRING, DOUBLE, BOOLEAN, DATE",
  //     "write", "create", "ctas", "mixedTypes") {
  //   sql("""CREATE TABLE tbl USING delta AS
  //     SELECT 1 as id, 'hello' as name, 3.14 as score, true as active, DATE '2024-01-15' as created_date
  //     UNION ALL SELECT 2, 'world', 2.72, false, DATE '2024-06-30'""")
  //   val t = registerTable("tbl")
  //   writeSpec(t)
  //   read(t, name = "read_all")
  //   snapshot(t)
  // }

  // CT-012: CTAS with nested struct
  // NOTE: CTAS is not supported as structured op - commenting out
  // test("CT_012_ctas_struct", "CTAS with nested struct column",
  //     "write", "create", "ctas", "struct") {
  //   sql("""CREATE TABLE tbl USING delta AS
  //     SELECT 1 as id, named_struct('name', 'Alice', 'age', 30) as info
  //     UNION ALL SELECT 2, named_struct('name', 'Bob', 'age', 25)""")
  //   val t = registerTable("tbl")
  //   writeSpec(t)
  //   read(t, name = "read_all")
  //   snapshot(t)
  // }

  // CT-013: INSERT INTO SELECT with UNION ALL
  // NOTE: INSERT INTO SELECT with UNION ALL requires sql()
  test("CT_013_insert_select_union", "INSERT INTO SELECT with UNION ALL from two sources",
      "write", "insert", "select", "union") {
    val w = createTableOp("tbl",
      schema = "key INT, value STRING")
    insertOp(w, Seq(Map("key" -> 1, "value" -> "initial")))
    sql("CREATE TEMPORARY VIEW source1 AS SELECT * FROM VALUES (2,'from_source1'),(3,'from_source1') AS t(key, value)")
    sql("CREATE TEMPORARY VIEW source2 AS SELECT * FROM VALUES (4,'from_source2'),(5,'from_source2') AS t(key, value)")
    sql("INSERT INTO tbl SELECT * FROM source1 UNION ALL SELECT * FROM source2")
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    read(t, version = 1, name = "read_after_initial")
    snapshot(t)
  }

  // CT-014: CTAS from empty source
  // Converted to createTableOp (no data inserted)
  test("CT_014_ctas_empty", "CTAS from empty source - schema with no data",
      "write", "create", "ctas", "empty") {
    val w = createTableOp("tbl",
      schema = "key INT, value STRING")
    val t = registerWriteSpec(w)
    read(t, name = "read_empty")
    snapshot(t)
  }

  // CT-015: INSERT INTO SELECT with column reordering
  // NOTE: INSERT INTO SELECT with column reordering requires sql()
  test("CT_015_insert_select_reorder", "INSERT INTO with explicit column reordering",
      "write", "insert", "select", "reorder") {
    val w = createTableOp("tbl",
      schema = "key INT, value STRING")
    insertOp(w, Seq(Map("key" -> 1, "value" -> "a")))
    sql("CREATE TEMPORARY VIEW source_ct015 AS SELECT * FROM VALUES ('b',2),('c',3) AS t(value, key)")
    sql("INSERT INTO tbl (value, key) SELECT value, key FROM source_ct015")
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  // ==========================================================================
  // Restore Tests (RS-*)
  // Source: RestoreWriteCaptureSuite.scala / RestoreTableSuite.scala
  // NOTE: RESTORE is NOT supported as structured op - all tests commented out
  // ==========================================================================

  // RS-001: RESTORE to version 0 after INSERT
  // NOTE: RESTORE is not supported as structured op - commenting out
  // test("RS_001_restore_after_insert", "RESTORE to v0 after INSERT - revert to initial 3 rows",
  //     "write", "restore") {
  //   sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
  //   sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
  //   sql("INSERT INTO tbl VALUES (4,'d'),(5,'e')")
  //   sql("RESTORE TABLE tbl TO VERSION AS OF 1")
  //   val t = registerTable("tbl")
  //   writeSpec(t)
  //   read(t, name = "read_after_restore")
  //   read(t, version = 2, name = "read_before_restore")
  //   snapshot(t)
  // }

  // RS-002: RESTORE to version 0 after DELETE
  // NOTE: RESTORE is not supported as structured op - commenting out
  // test("RS_002_restore_after_delete", "RESTORE to v0 after DELETE - recover deleted rows",
  //     "write", "restore", "delete") {
  //   sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
  //   sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e')")
  //   sql("DELETE FROM tbl WHERE id > 3")
  //   sql("RESTORE TABLE tbl TO VERSION AS OF 1")
  //   val t = registerTable("tbl")
  //   writeSpec(t)
  //   read(t, name = "read_after_restore")
  //   read(t, version = 2, name = "read_after_delete")
  //   snapshot(t)
  // }

  // RS-003: RESTORE to version 0 after UPDATE
  // NOTE: RESTORE is not supported as structured op - commenting out
  // test("RS_003_restore_after_update", "RESTORE to v0 after UPDATE - revert updated values",
  //     "write", "restore", "update") {
  //   sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
  //   sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
  //   sql("UPDATE tbl SET name = 'updated'")
  //   sql("RESTORE TABLE tbl TO VERSION AS OF 1")
  //   val t = registerTable("tbl")
  //   writeSpec(t)
  //   read(t, name = "read_after_restore")
  //   read(t, version = 2, name = "read_after_update")
  //   snapshot(t)
  // }

  // RS-004: RESTORE to intermediate version
  // NOTE: RESTORE is not supported as structured op - commenting out
  // test("RS_004_restore_intermediate", "RESTORE to v1 - revert delete but keep insert",
  //     "write", "restore") {
  //   sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
  //   sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
  //   sql("INSERT INTO tbl VALUES (4,'d'),(5,'e')")
  //   sql("DELETE FROM tbl WHERE id = 1")
  //   sql("RESTORE TABLE tbl TO VERSION AS OF 2")
  //   val t = registerTable("tbl")
  //   writeSpec(t)
  //   read(t, name = "read_after_restore")
  //   read(t, version = 3, name = "read_after_delete")
  //   snapshot(t)
  // }

  // RS-005: RESTORE after multiple operations
  // NOTE: RESTORE is not supported as structured op - commenting out
  // test("RS_005_restore_multi_ops", "RESTORE to v0 after insert, update, delete",
  //     "write", "restore") {
  //   sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
  //   sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
  //   sql("INSERT INTO tbl VALUES (4,'d'),(5,'e')")
  //   sql("UPDATE tbl SET name = 'x' WHERE id <= 2")
  //   sql("DELETE FROM tbl WHERE id = 3")
  //   sql("RESTORE TABLE tbl TO VERSION AS OF 1")
  //   val t = registerTable("tbl")
  //   writeSpec(t)
  //   read(t, name = "read_after_restore")
  //   read(t, version = 4, name = "read_before_restore")
  //   snapshot(t)
  // }

  // RS-006: RESTORE on partitioned table
  // NOTE: RESTORE is not supported as structured op - commenting out
  // test("RS_006_restore_partitioned", "RESTORE partitioned table to v0 - revert partition changes",
  //     "write", "restore", "partitioned") {
  //   sql("CREATE TABLE tbl (id INT, name STRING, part STRING) USING delta PARTITIONED BY (part)")
  //   sql("INSERT INTO tbl VALUES (1,'a','p1'),(2,'b','p1'),(3,'c','p2'),(4,'d','p2')")
  //   sql("INSERT INTO tbl VALUES (5,'e','p1'),(6,'f','p1')")
  //   sql("DELETE FROM tbl WHERE part = 'p2'")
  //   sql("RESTORE TABLE tbl TO VERSION AS OF 1")
  //   val t = registerTable("tbl")
  //   writeSpec(t)
  //   read(t, name = "read_after_restore")
  //   read(t, predicate = "part = 'p1'", name = "read_p1")
  //   read(t, predicate = "part = 'p2'", name = "read_p2")
  //   snapshot(t)
  // }

  // RS-007: RESTORE after schema evolution (ADD COLUMN)
  // NOTE: RESTORE is not supported as structured op - commenting out
  // test("RS_007_restore_schema_evolution", "RESTORE to v0 after ADD COLUMN - revert schema",
  //     "write", "restore", "schemaEvolution") {
  //   sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
  //   sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
  //   sql("ALTER TABLE tbl ADD COLUMNS (score INT)")
  //   sql("INSERT INTO tbl VALUES (4,'d',100),(5,'e',200)")
  //   sql("RESTORE TABLE tbl TO VERSION AS OF 1")
  //   val t = registerTable("tbl")
  //   writeSpec(t)
  //   read(t, name = "read_after_restore")
  //   read(t, version = 3, name = "read_with_score_column")
  //   snapshot(t)
  // }

  // RS-008: RESTORE after MERGE
  // NOTE: RESTORE is not supported as structured op - commenting out
  // test("RS_008_restore_after_merge", "RESTORE to v0 after MERGE upsert",
  //     "write", "restore", "merge") {
  //   sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
  //   sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
  //   sql("CREATE TEMPORARY VIEW merge_source AS SELECT * FROM VALUES (2,'merged_b'),(4,'d'),(5,'e') AS t(id, name)")
  //   sql("""MERGE INTO tbl AS target
  //     USING merge_source AS source
  //     ON target.id = source.id
  //     WHEN MATCHED THEN UPDATE SET target.name = source.name
  //     WHEN NOT MATCHED THEN INSERT (id, name) VALUES (source.id, source.name)""")
  //   sql("RESTORE TABLE tbl TO VERSION AS OF 1")
  //   val t = registerTable("tbl")
  //   writeSpec(t)
  //   read(t, name = "read_after_restore")
  //   read(t, version = 2, name = "read_after_merge")
  //   snapshot(t)
  // }

  // RS-009: RESTORE with deletion vectors
  // NOTE: RESTORE is not supported as structured op - commenting out
  // test("RS_009_restore_with_dvs", "RESTORE to v0 after DV-based delete",
  //     "write", "restore", "dv") {
  //   sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
  //     TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  //   sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e')")
  //   sql("DELETE FROM tbl WHERE id IN (2, 4)")
  //   sql("RESTORE TABLE tbl TO VERSION AS OF 1")
  //   val t = registerTable("tbl")
  //   writeSpec(t)
  //   read(t, name = "read_after_restore")
  //   read(t, version = 2, name = "read_after_dv_delete")
  //   snapshot(t)
  // }

  // RS-010: RESTORE to version 0 of empty table
  // NOTE: RESTORE is not supported as structured op - commenting out
  // test("RS_010_restore_empty_table", "RESTORE to v0 of empty table",
  //     "write", "restore", "empty") {
  //   sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
  //   sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
  //   sql("RESTORE TABLE tbl TO VERSION AS OF 0")
  //   val t = registerTable("tbl")
  //   writeSpec(t)
  //   read(t, name = "read_after_restore_empty")
  //   read(t, version = 1, name = "read_before_restore")
  //   snapshot(t)
  // }

  // RS-011: Double RESTORE
  // NOTE: RESTORE is not supported as structured op - commenting out
  // test("RS_011_double_restore", "RESTORE to v1 then RESTORE to v0",
  //     "write", "restore") {
  //   sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
  //   sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
  //   sql("INSERT INTO tbl VALUES (4,'d'),(5,'e')")
  //   sql("DELETE FROM tbl WHERE id = 5")
  //   sql("RESTORE TABLE tbl TO VERSION AS OF 2")
  //   sql("RESTORE TABLE tbl TO VERSION AS OF 1")
  //   val t = registerTable("tbl")
  //   writeSpec(t)
  //   read(t, name = "read_after_double_restore")
  //   read(t, version = 4, name = "read_after_first_restore")
  //   snapshot(t)
  // }

  // RS-012: RESTORE after INSERT OVERWRITE
  // NOTE: RESTORE is not supported as structured op - commenting out
  // test("RS_012_restore_after_overwrite", "RESTORE to v0 after INSERT OVERWRITE",
  //     "write", "restore", "overwrite") {
  //   sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
  //   sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
  //   sql("INSERT OVERWRITE tbl VALUES (10,'x'),(20,'y')")
  //   sql("RESTORE TABLE tbl TO VERSION AS OF 1")
  //   val t = registerTable("tbl")
  //   writeSpec(t)
  //   read(t, name = "read_after_restore")
  //   read(t, version = 2, name = "read_after_overwrite")
  //   snapshot(t)
  // }

  // RS-013: RESTORE after OPTIMIZE
  // NOTE: RESTORE is not supported as structured op - commenting out
  // test("RS_013_restore_after_optimize", "RESTORE to pre-optimize version",
  //     "write", "restore", "optimize") {
  //   sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
  //   // 5 small batches to create many files
  //   sql("INSERT INTO tbl VALUES (1,'v1'),(2,'v2')")
  //   sql("INSERT INTO tbl VALUES (3,'v3'),(4,'v4')")
  //   sql("INSERT INTO tbl VALUES (5,'v5'),(6,'v6')")
  //   sql("INSERT INTO tbl VALUES (7,'v7'),(8,'v8')")
  //   sql("INSERT INTO tbl VALUES (9,'v9'),(10,'v10')")
  //   sql("OPTIMIZE tbl")
  //   // RESTORE to pre-optimize version (v5 = last insert)
  //   sql("RESTORE TABLE tbl TO VERSION AS OF 5")
  //   val t = registerTable("tbl")
  //   writeSpec(t)
  //   read(t, name = "read_after_restore")
  //   read(t, version = 6, name = "read_after_optimize")
  //   snapshot(t)
  // }

  // RS-014: RESTORE with row tracking enabled
  // NOTE: RESTORE is not supported as structured op - commenting out
  // test("RS_014_restore_row_tracking", "RESTORE to v0 with row tracking enabled",
  //     "write", "restore", "rowTracking") {
  //   sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
  //     TBLPROPERTIES ('delta.enableRowTracking' = 'true')""")
  //   sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
  //   sql("INSERT INTO tbl VALUES (4,'d'),(5,'e')")
  //   sql("UPDATE tbl SET name = 'updated' WHERE id = 1")
  //   sql("RESTORE TABLE tbl TO VERSION AS OF 1")
  //   val t = registerTable("tbl")
  //   writeSpec(t)
  //   read(t, name = "read_after_restore")
  //   read(t, version = 3, name = "read_after_update")
  //   snapshot(t)
  // }

  // RS-015: RESTORE after ALTER TABLE SET TBLPROPERTIES
  // NOTE: RESTORE is not supported as structured op - commenting out
  // test("RS_015_restore_after_tblproperties", "RESTORE to v0 reverts property and data",
  //     "write", "restore", "alterTable") {
  //   sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
  //   sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
  //   sql("ALTER TABLE tbl SET TBLPROPERTIES ('custom.prop' = 'value1')")
  //   sql("INSERT INTO tbl VALUES (4,'d'),(5,'e')")
  //   sql("RESTORE TABLE tbl TO VERSION AS OF 1")
  //   val t = registerTable("tbl")
  //   writeSpec(t)
  //   read(t, name = "read_after_restore")
  //   read(t, version = 3, name = "read_before_restore")
  //   snapshot(t)
  // }

  // ==========================================================================
  // Optimize Tests (OPT-*)
  // Source: OptimizeCompactionSuiteWriteCapture.scala / OptimizeCompactionSuite.scala
  // ==========================================================================

  // OPT-001: Basic unpartitioned OPTIMIZE
  test("OPT_001_basic_optimize", "Two inserts then OPTIMIZE to compact",
      "write", "optimize") {
    val w = createTableOp("tbl",
      schema = "value INT")
    insertOp(w, Seq(
      Map("value" -> 1),
      Map("value" -> 2),
      Map("value" -> 3)))
    insertOp(w, Seq(
      Map("value" -> 4),
      Map("value" -> 5),
      Map("value" -> 6)))
    sql("OPTIMIZE tbl")
    val t = registerWriteSpec(w)
    read(t, name = "read_after_optimize")
    read(t, version = 2, name = "read_before_optimize")
    snapshot(t)
  }

  // OPT-002: Partitioned OPTIMIZE - all partitions
  test("OPT_002_partitioned_optimize_all", "Partitioned table OPTIMIZE all partitions",
      "write", "optimize", "partitioned") {
    val w = createTableOp("tbl",
      schema = "value INT, id INT",
      partitionColumns = Seq("id"))
    insertOp(w, Seq(
      Map("value" -> 1, "id" -> 1),
      Map("value" -> 2, "id" -> 0),
      Map("value" -> 3, "id" -> 1)))
    insertOp(w, Seq(
      Map("value" -> 4, "id" -> 0),
      Map("value" -> 5, "id" -> 1),
      Map("value" -> 6, "id" -> 0)))
    sql("OPTIMIZE tbl")
    val t = registerWriteSpec(w)
    read(t, name = "read_after_optimize")
    read(t, predicate = "id = 0", name = "read_id_0")
    read(t, predicate = "id = 1", name = "read_id_1")
    snapshot(t)
  }

  // OPT-003: Partitioned OPTIMIZE with WHERE clause
  test("OPT_003_partitioned_optimize_where", "OPTIMIZE only partition id=0",
      "write", "optimize", "partitioned", "where") {
    val w = createTableOp("tbl",
      schema = "value INT, id INT",
      partitionColumns = Seq("id"))
    insertOp(w, Seq(
      Map("value" -> 1, "id" -> 1),
      Map("value" -> 2, "id" -> 0),
      Map("value" -> 3, "id" -> 1)))
    insertOp(w, Seq(
      Map("value" -> 4, "id" -> 0),
      Map("value" -> 5, "id" -> 1),
      Map("value" -> 6, "id" -> 0)))
    sql("OPTIMIZE tbl WHERE id = 0")
    val t = registerWriteSpec(w)
    read(t, name = "read_after_optimize")
    read(t, predicate = "id = 0", name = "read_optimized_partition")
    read(t, predicate = "id = 1", name = "read_unoptimized_partition")
    snapshot(t)
  }

  // OPT-004: Multi-partition OPTIMIZE with WHERE
  // NOTE: INSERT INTO SELECT with range() requires sql()
  test("OPT_004_multi_partition_optimize", "Multi-partition OPTIMIZE on specific partition",
      "write", "optimize", "partitioned", "where") {
    val w = createTableOp("tbl",
      schema = "id LONG, date DATE, part LONG",
      partitionColumns = Seq("date", "part"))
    sql("CREATE TEMPORARY VIEW opt004_data1 AS SELECT id, DATE '2017-10-10' as date, id % 5 as part FROM range(10)")
    sql("INSERT INTO tbl SELECT * FROM opt004_data1")
    sql("CREATE TEMPORARY VIEW opt004_data2 AS SELECT id, DATE '2017-10-10' as date, id % 5 as part FROM range(100)")
    sql("INSERT INTO tbl SELECT * FROM opt004_data2")
    sql("OPTIMIZE tbl WHERE date = '2017-10-10' and part = 3")
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    read(t, predicate = "part = 3", name = "read_optimized_partition")
    snapshot(t)
  }

  // OPT-005: OPTIMIZE after many small inserts
  test("OPT_005_optimize_many_small_inserts", "5 small inserts then OPTIMIZE",
      "write", "optimize") {
    val w = createTableOp("tbl",
      schema = "id INT, data STRING")
    insertOp(w, Seq(Map("id" -> 1, "data" -> "row1")))
    insertOp(w, Seq(Map("id" -> 2, "data" -> "row2")))
    insertOp(w, Seq(Map("id" -> 3, "data" -> "row3")))
    insertOp(w, Seq(Map("id" -> 4, "data" -> "row4")))
    insertOp(w, Seq(Map("id" -> 5, "data" -> "row5")))
    sql("OPTIMIZE tbl")
    val t = registerWriteSpec(w)
    read(t, name = "read_after_optimize")
    read(t, version = 5, name = "read_before_optimize")
    snapshot(t)
  }

  // ==========================================================================
  // Log Replay Tests (LR-*)
  // Source: LogReplayWriteCapture.scala (synthetic log replay scenarios)
  // ==========================================================================

  // LR-001: Add then remove - INSERT then DELETE all
  test("LR_001_add_then_remove", "INSERT 5 rows then DELETE all - empty table",
      "write", "log_replay", "insert", "delete") {
    val w = createTableOp("tbl",
      schema = "id INT, val STRING")
    insertOp(w, Seq(
      Map("id" -> 1, "val" -> "a"),
      Map("id" -> 2, "val" -> "b"),
      Map("id" -> 3, "val" -> "c"),
      Map("id" -> 4, "val" -> "d"),
      Map("id" -> 5, "val" -> "e")))
    sql("DELETE FROM tbl")
    val t = registerWriteSpec(w)
    read(t, name = "read_latest_empty")
    read(t, version = 1, name = "read_before_delete")
    snapshot(t)
  }

  // LR-002: Add remove re-add
  test("LR_002_add_remove_readd", "INSERT data A, DELETE all, INSERT data B - only B remains",
      "write", "log_replay", "insert", "delete") {
    val w = createTableOp("tbl",
      schema = "id INT, val STRING")
    insertOp(w, Seq(
      Map("id" -> 1, "val" -> "a"),
      Map("id" -> 2, "val" -> "b"),
      Map("id" -> 3, "val" -> "c")))
    sql("DELETE FROM tbl")
    insertOp(w, Seq(
      Map("id" -> 10, "val" -> "x"),
      Map("id" -> 20, "val" -> "y")))
    val t = registerWriteSpec(w)
    read(t, name = "read_latest_only_B")
    read(t, version = 1, name = "read_data_A")
    snapshot(t)
  }

  // LR-003: Checkpoint supersedes log - 11+ INSERTs to force checkpoint
  test("LR_003_checkpoint_supersedes_log", "11 inserts force checkpoint at v10",
      "write", "log_replay", "checkpoint") {
    val w = createTableOp("tbl",
      schema = "id INT, val STRING",
      properties = Map("delta.checkpointInterval" -> "10"))
    for (i <- 1 to 11) {
      insertOp(w, Seq(Map("id" -> i, "val" -> ('a' + i - 1).toChar.toString)))
    }
    val t = registerWriteSpec(w)
    read(t, name = "read_latest_all_11")
    snapshot(t)
  }

  // LR-004: No checkpoint full replay - 5 INSERTs
  test("LR_004_no_checkpoint_full_replay", "5 inserts without checkpoint",
      "write", "log_replay", "insert") {
    val w = createTableOp("tbl",
      schema = "id INT, val STRING")
    for (i <- 1 to 5) {
      insertOp(w, Seq(Map("id" -> i, "val" -> ('a' + i - 1).toChar.toString)))
    }
    val t = registerWriteSpec(w)
    read(t, name = "read_latest_all_5")
    snapshot(t)
  }

  // LR-005: Metadata latest wins - ALTER TABLE ADD COLUMN
  test("LR_005_metadata_latest_wins", "ADD COLUMN then insert with new schema",
      "write", "log_replay", "metadata", "schema_evolution") {
    val w = createTableOp("tbl",
      schema = "id INT")
    insertOp(w, Seq(
      Map("id" -> 1),
      Map("id" -> 2),
      Map("id" -> 3)))
    sql("ALTER TABLE tbl ADD COLUMN name STRING")
    sql("INSERT INTO tbl VALUES (4,'alice'),(5,'bob')")
    val t = registerWriteSpec(w)
    read(t, name = "read_latest_two_cols")
    read(t, version = 1, name = "read_before_alter")
    snapshot(t)
  }

  // LR-006: Protocol latest wins - enable deletion vectors
  test("LR_006_protocol_latest_wins", "Enable DVs upgrades protocol",
      "write", "log_replay", "protocol") {
    val w = createTableOp("tbl",
      schema = "id INT, val STRING")
    insertOp(w, Seq(
      Map("id" -> 1, "val" -> "a"),
      Map("id" -> 2, "val" -> "b")))
    sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")
    insertOp(w, Seq(Map("id" -> 3, "val" -> "c")))
    val t = registerWriteSpec(w)
    read(t, name = "read_latest_after_protocol_upgrade")
    read(t, version = 1, name = "read_before_upgrade")
    snapshot(t)
  }

  // LR-007: DataChange false (OPTIMIZE)
  test("LR_007_datachange_false_optimize", "5 inserts then OPTIMIZE - same logical data",
      "write", "log_replay", "optimize") {
    val w = createTableOp("tbl",
      schema = "id INT, val STRING")
    for (i <- 1 to 5) {
      insertOp(w, Seq(
        Map("id" -> (i * 2 - 1), "val" -> s"v${i * 2 - 1}"),
        Map("id" -> (i * 2), "val" -> s"v${i * 2}")))
    }
    sql("OPTIMIZE tbl")
    val t = registerWriteSpec(w)
    read(t, name = "read_latest_after_optimize")
    read(t, version = 5, name = "read_before_optimize")
    snapshot(t)
  }

  // LR-008: Many small inserts - 12 sequential single-row inserts
  test("LR_008_many_small_inserts", "12 single-row inserts",
      "write", "log_replay", "insert") {
    val w = createTableOp("tbl",
      schema = "id INT, val STRING")
    for (i <- 1 to 12) {
      insertOp(w, Seq(Map("id" -> i, "val" -> s"row$i")))
    }
    val t = registerWriteSpec(w)
    read(t, name = "read_latest_all_12")
    snapshot(t)
  }

  // LR-009: Insert + Update sequence
  test("LR_009_insert_update_sequence", "INSERT 5 rows then UPDATE 2",
      "write", "log_replay", "insert", "update") {
    val w = createTableOp("tbl",
      schema = "id INT, amount INT")
    insertOp(w, Seq(
      Map("id" -> 1, "amount" -> 100),
      Map("id" -> 2, "amount" -> 200),
      Map("id" -> 3, "amount" -> 300),
      Map("id" -> 4, "amount" -> 400),
      Map("id" -> 5, "amount" -> 500)))
    sql("UPDATE tbl SET amount = amount * 2 WHERE id <= 2")
    val t = registerWriteSpec(w)
    read(t, name = "read_latest_after_update")
    read(t, version = 1, name = "read_before_update")
    snapshot(t)
  }

  // LR-010: Insert + Merge sequence
  // NOTE: MERGE is not allowed - commenting out
  // test("LR_010_insert_merge_sequence", "INSERT then MERGE with overlapping keys",
  //     "write", "log_replay", "insert", "merge") {
  //   sql("CREATE TABLE tbl (id INT, val STRING) USING delta")
  //   sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
  //   sql("CREATE TEMPORARY VIEW lr010_source AS SELECT * FROM VALUES (2,'B'),(3,'C'),(4,'D') AS t(id, val)")
  //   sql("""MERGE INTO tbl AS t
  //     USING lr010_source AS s
  //     ON t.id = s.id
  //     WHEN MATCHED THEN UPDATE SET t.val = s.val
  //     WHEN NOT MATCHED THEN INSERT *""")
  //   val t = registerTable("tbl")
  //   writeSpec(t)
  //   read(t, name = "read_latest_after_merge")
  //   read(t, version = 1, name = "read_before_merge")
  //   snapshot(t)
  // }

  // LR-011: Multi-partition replay - 5 partitions
  test("LR_011_multi_partition_replay", "Inserts across 5 partitions",
      "write", "log_replay", "insert", "partitioned") {
    val w = createTableOp("tbl",
      schema = "id INT, val STRING, part STRING",
      partitionColumns = Seq("part"))
    insertOp(w, Seq(
      Map("id" -> 1, "val" -> "a1", "part" -> "A"),
      Map("id" -> 2, "val" -> "a2", "part" -> "A")))
    insertOp(w, Seq(
      Map("id" -> 3, "val" -> "b1", "part" -> "B"),
      Map("id" -> 4, "val" -> "b2", "part" -> "B")))
    insertOp(w, Seq(
      Map("id" -> 5, "val" -> "c1", "part" -> "C"),
      Map("id" -> 6, "val" -> "c2", "part" -> "C")))
    insertOp(w, Seq(
      Map("id" -> 7, "val" -> "d1", "part" -> "D"),
      Map("id" -> 8, "val" -> "d2", "part" -> "D")))
    insertOp(w, Seq(
      Map("id" -> 9, "val" -> "e1", "part" -> "E"),
      Map("id" -> 10, "val" -> "e2", "part" -> "E")))
    val t = registerWriteSpec(w)
    read(t, name = "read_all_partitions")
    read(t, predicate = "part = 'A'", name = "read_partA")
    read(t, predicate = "part = 'C'", name = "read_partC")
    read(t, predicate = "part = 'E'", name = "read_partE")
    snapshot(t)
  }

  // LR-012: Overwrite then read
  test("LR_012_overwrite_then_read", "INSERT OVERWRITE replaces all data",
      "write", "log_replay", "insert", "overwrite") {
    val w = createTableOp("tbl",
      schema = "id INT, val STRING")
    insertOp(w, Seq(
      Map("id" -> 1, "val" -> "a"),
      Map("id" -> 2, "val" -> "b"),
      Map("id" -> 3, "val" -> "c"),
      Map("id" -> 4, "val" -> "d"),
      Map("id" -> 5, "val" -> "e")))
    sql("INSERT OVERWRITE tbl VALUES (10,'x'),(20,'y'),(30,'z')")
    val t = registerWriteSpec(w)
    read(t, name = "read_latest_overwritten")
    read(t, version = 1, name = "read_before_overwrite")
    snapshot(t)
  }

  // LR-013: Schema evolution replay - add column
  test("LR_013_schema_evolution_replay", "ADD COLUMN then insert with new column",
      "write", "log_replay", "schema_evolution") {
    val w = createTableOp("tbl",
      schema = "id INT, val STRING")
    insertOp(w, Seq(
      Map("id" -> 1, "val" -> "a"),
      Map("id" -> 2, "val" -> "b")))
    sql("ALTER TABLE tbl ADD COLUMN extra_col DOUBLE")
    sql("INSERT INTO tbl VALUES (3,'c',3.14),(4,'d',2.72)")
    val t = registerWriteSpec(w)
    read(t, name = "read_latest_evolved_schema")
    read(t, version = 1, name = "read_before_evolution")
    snapshot(t)
  }

  // LR-014: Property change replay
  test("LR_014_property_change_replay", "SET TBLPROPERTIES then continue inserts",
      "write", "log_replay", "metadata", "properties") {
    val w = createTableOp("tbl",
      schema = "id INT, val STRING")
    insertOp(w, Seq(
      Map("id" -> 1, "val" -> "a"),
      Map("id" -> 2, "val" -> "b")))
    sql("ALTER TABLE tbl SET TBLPROPERTIES ('my.custom.key' = 'my_value')")
    insertOp(w, Seq(Map("id" -> 3, "val" -> "c")))
    val t = registerWriteSpec(w)
    read(t, name = "read_latest_with_props")
    read(t, version = 1, name = "read_before_props")
    snapshot(t)
  }

  // LR-015: Delete from partition
  test("LR_015_delete_from_partition", "Delete partition B, verify A and C remain",
      "write", "log_replay", "delete", "partitioned") {
    val w = createTableOp("tbl",
      schema = "id INT, val STRING, part STRING",
      partitionColumns = Seq("part"))
    insertOp(w, Seq(
      Map("id" -> 1, "val" -> "a1", "part" -> "A"),
      Map("id" -> 2, "val" -> "a2", "part" -> "A")))
    insertOp(w, Seq(
      Map("id" -> 3, "val" -> "b1", "part" -> "B"),
      Map("id" -> 4, "val" -> "b2", "part" -> "B")))
    insertOp(w, Seq(
      Map("id" -> 5, "val" -> "c1", "part" -> "C"),
      Map("id" -> 6, "val" -> "c2", "part" -> "C")))
    sql("DELETE FROM tbl WHERE part = 'B'")
    val t = registerWriteSpec(w)
    read(t, name = "read_latest_no_B")
    read(t, predicate = "part = 'A'", name = "read_partA")
    read(t, predicate = "part = 'B'", name = "read_partB_empty")
    read(t, predicate = "part = 'C'", name = "read_partC")
    snapshot(t)
  }

  // LR-016: MERGE upsert pattern
  // NOTE: MERGE is not allowed - commenting out
  // test("LR_016_merge_upsert", "MERGE with 3 overlapping + 2 new keys",
  //     "write", "log_replay", "merge", "upsert") {
  //   sql("CREATE TABLE tbl (id INT, val STRING) USING delta")
  //   sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e')")
  //   sql("CREATE TEMPORARY VIEW lr016_source AS SELECT * FROM VALUES (1,'A'),(3,'C'),(5,'E'),(6,'f'),(7,'g') AS t(id, val)")
  //   sql("""MERGE INTO tbl AS t
  //     USING lr016_source AS s
  //     ON t.id = s.id
  //     WHEN MATCHED THEN UPDATE SET t.val = s.val
  //     WHEN NOT MATCHED THEN INSERT *""")
  //   val t = registerTable("tbl")
  //   writeSpec(t)
  //   read(t, name = "read_latest_after_upsert")
  //   read(t, version = 1, name = "read_before_merge")
  //   snapshot(t)
  // }

  // LR-017: Checkpoint after DML
  test("LR_017_checkpoint_after_dml", "insert, delete, insert, force checkpoint at interval=5",
      "write", "log_replay", "checkpoint") {
    val w = createTableOp("tbl",
      schema = "id INT, val STRING",
      properties = Map("delta.checkpointInterval" -> "5"))
    insertOp(w, Seq(
      Map("id" -> 1, "val" -> "a"),
      Map("id" -> 2, "val" -> "b"),
      Map("id" -> 3, "val" -> "c"),
      Map("id" -> 4, "val" -> "d"),
      Map("id" -> 5, "val" -> "e")))
    sql("DELETE FROM tbl WHERE id > 3")
    insertOp(w, Seq(
      Map("id" -> 6, "val" -> "f"),
      Map("id" -> 7, "val" -> "g")))
    insertOp(w, Seq(Map("id" -> 8, "val" -> "h")))
    insertOp(w, Seq(Map("id" -> 9, "val" -> "i")))
    val t = registerWriteSpec(w)
    read(t, name = "read_latest_after_checkpoint")
    read(t, version = 1, name = "read_initial_insert")
    snapshot(t)
  }

  // LR-018: Time travel across versions
  test("LR_018_time_travel_versions", "5 inserts, read each version",
      "write", "log_replay", "time_travel") {
    val w = createTableOp("tbl",
      schema = "id INT, val STRING")
    for (i <- 1 to 5) {
      insertOp(w, Seq(Map("id" -> i, "val" -> s"v$i")))
    }
    val t = registerWriteSpec(w)
    read(t, version = 0, name = "read_v0_empty")
    read(t, version = 1, name = "read_v1")
    read(t, version = 2, name = "read_v2")
    read(t, version = 3, name = "read_v3")
    read(t, version = 4, name = "read_v4")
    read(t, version = 5, name = "read_v5_latest")
    snapshot(t)
  }

  // LR-019: Vacuum doesn't affect current data
  test("LR_019_vacuum_no_effect", "INSERT, DELETE, VACUUM - current data intact",
      "write", "log_replay", "vacuum") {
    val w = createTableOp("tbl",
      schema = "id INT, val STRING")
    insertOp(w, Seq(
      Map("id" -> 1, "val" -> "a"),
      Map("id" -> 2, "val" -> "b"),
      Map("id" -> 3, "val" -> "c"),
      Map("id" -> 4, "val" -> "d"),
      Map("id" -> 5, "val" -> "e")))
    sql("DELETE FROM tbl WHERE id > 3")
    // VACUUM removes unreferenced files but does not change table state
    // Note: retention=0 requires spark.delta.retentionDurationCheck.enabled=false
    sql("VACUUM tbl RETAIN 0 HOURS")
    val t = registerWriteSpec(w)
    read(t, name = "read_latest_after_vacuum")
    read(t, version = 1, name = "read_before_delete")
    snapshot(t)
  }

  // LR-020: Large number of files - 50 batches of 2 rows
  test("LR_020_large_number_of_files", "50 batches of 2 rows each - 100 rows total",
      "write", "log_replay", "insert", "many_files") {
    val w = createTableOp("tbl",
      schema = "id INT, val STRING")
    for (batch <- 1 to 50) {
      val id1 = batch * 2 - 1
      val id2 = batch * 2
      insertOp(w, Seq(
        Map("id" -> id1, "val" -> s"r$id1"),
        Map("id" -> id2, "val" -> s"r$id2")))
    }
    val t = registerWriteSpec(w)
    read(t, name = "read_latest_all_100")
    snapshot(t)
  }

}
