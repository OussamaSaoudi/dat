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
  test("CT_001_basic_ctas", "Create table via CTAS with 3 rows",
      "write", "create", "ctas") { w =>
    w.sql("CREATE TABLE tbl USING delta AS SELECT * FROM VALUES (1,'a'),(2,'b'),(3,'c') AS t(key, value)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  // CT-002: CTAS with SQL syntax
  test("CT_002_ctas_sql", "CTAS via SQL with temp view source",
      "write", "create", "ctas", "sql") { w =>
    w.sql("CREATE TEMPORARY VIEW source_ct002 AS SELECT * FROM VALUES (1,'a'),(2,'b'),(3,'c') AS t(key, value)")
    w.sql("CREATE TABLE tbl USING delta AS SELECT * FROM source_ct002")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  // CT-003: CTAS with partitioning
  test("CT_003_ctas_partitioned", "CTAS with PARTITIONED BY",
      "write", "create", "ctas", "partitioned") { w =>
    w.sql("CREATE TEMPORARY VIEW source_ct003 AS SELECT * FROM VALUES (1,'a','p1'),(2,'b','p2'),(3,'c','p1') AS t(key, value, part)")
    w.sql("CREATE TABLE tbl USING delta PARTITIONED BY (part) AS SELECT * FROM source_ct003")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, predicate = "part = 'p1'", name = "read_p1")
    w.read(t, predicate = "part = 'p2'", name = "read_p2")
    w.snapshot(t)
  }

  // CT-004: CTAS with table properties
  test("CT_004_ctas_properties", "CTAS with appendOnly property",
      "write", "create", "ctas", "properties") { w =>
    w.sql("""CREATE TABLE tbl USING delta
      TBLPROPERTIES ('delta.appendOnly' = 'true')
      AS SELECT * FROM VALUES (1,'a'),(2,'b'),(3,'c') AS t(key, value)""")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  // CT-005: INSERT INTO SELECT from another Delta table
  test("CT_005_insert_select_delta", "Create target, insert from another table",
      "write", "create", "insert", "select") { w =>
    w.sql("CREATE TABLE tbl (key INT, value STRING) USING delta")
    w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b')")
    w.sql("CREATE TEMPORARY VIEW source_ct005 AS SELECT * FROM VALUES (3,'c'),(4,'d') AS t(key, value)")
    w.sql("INSERT INTO tbl SELECT * FROM source_ct005")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, version = 1, name = "read_before_insert_select")
    w.snapshot(t)
  }

  // CT-006: INSERT INTO SELECT with transformation
  test("CT_006_insert_select_transform", "INSERT INTO SELECT with value * 2",
      "write", "insert", "select", "transform") { w =>
    w.sql("CREATE TABLE tbl (key INT, value INT) USING delta")
    w.sql("INSERT INTO tbl VALUES (1, 10)")
    w.sql("CREATE TEMPORARY VIEW source_ct006 AS SELECT * FROM VALUES (2,20),(3,30) AS t(key, value)")
    w.sql("INSERT INTO tbl SELECT key, value * 2 as value FROM source_ct006")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  // CT-007: INSERT INTO SELECT with WHERE clause
  test("CT_007_insert_select_filter", "INSERT INTO SELECT with WHERE value > 10",
      "write", "insert", "select", "filter") { w =>
    w.sql("CREATE TABLE tbl (key INT, value INT) USING delta")
    w.sql("INSERT INTO tbl VALUES (1, 100)")
    w.sql("CREATE TEMPORARY VIEW source_ct007 AS SELECT * FROM VALUES (2,5),(3,15),(4,25),(5,3) AS t(key, value)")
    w.sql("INSERT INTO tbl SELECT * FROM source_ct007 WHERE value > 10")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  // CT-008: INSERT INTO SELECT with JOIN
  test("CT_008_insert_select_join", "INSERT INTO SELECT with JOIN on two sources",
      "write", "insert", "select", "join") { w =>
    w.sql("CREATE TABLE tbl (key INT, name STRING) USING delta")
    w.sql("CREATE TEMPORARY VIEW table_a AS SELECT * FROM VALUES (1,100),(2,200),(3,300) AS t(key, amount)")
    w.sql("CREATE TEMPORARY VIEW table_b AS SELECT * FROM VALUES (1,'alice'),(2,'bob'),(4,'dave') AS t(key, name)")
    w.sql("INSERT INTO tbl SELECT a.key, b.name FROM table_a a JOIN table_b b ON a.key = b.key")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  // CT-009: INSERT INTO SELECT with aggregation
  test("CT_009_insert_select_aggregate", "INSERT INTO SELECT with SUM GROUP BY",
      "write", "insert", "select", "aggregate") { w =>
    w.sql("CREATE TABLE tbl (category STRING, total BIGINT) USING delta")
    w.sql("CREATE TEMPORARY VIEW source_ct009 AS SELECT * FROM VALUES ('electronics',100),('electronics',200),('books',50),('books',30) AS t(category, amount)")
    w.sql("INSERT INTO tbl SELECT category, SUM(amount) as total FROM source_ct009 GROUP BY category")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  // CT-010: CREATE OR REPLACE TABLE (overwrite existing)
  test("CT_010_create_or_replace", "Create then overwrite with mode overwrite",
      "write", "create", "overwrite") { w =>
    w.sql("CREATE TABLE tbl (key INT, value STRING) USING delta")
    w.sql("INSERT INTO tbl VALUES (1,'old_a'),(2,'old_b')")
    w.sql("INSERT OVERWRITE tbl VALUES (10,'new_x'),(20,'new_y'),(30,'new_z')")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_after_overwrite")
    w.read(t, version = 1, name = "read_before_overwrite")
    w.snapshot(t)
  }

  // CT-011: CTAS with multiple data types
  test("CT_011_ctas_mixed_types", "CTAS with INT, STRING, DOUBLE, BOOLEAN, DATE",
      "write", "create", "ctas", "mixedTypes") { w =>
    w.sql("""CREATE TABLE tbl USING delta AS
      SELECT 1 as id, 'hello' as name, 3.14 as score, true as active, DATE '2024-01-15' as created_date
      UNION ALL SELECT 2, 'world', 2.72, false, DATE '2024-06-30'""")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  // CT-012: CTAS with nested struct
  test("CT_012_ctas_struct", "CTAS with nested struct column",
      "write", "create", "ctas", "struct") { w =>
    w.sql("""CREATE TABLE tbl USING delta AS
      SELECT 1 as id, named_struct('name', 'Alice', 'age', 30) as info
      UNION ALL SELECT 2, named_struct('name', 'Bob', 'age', 25)""")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  // CT-013: INSERT INTO SELECT with UNION ALL
  test("CT_013_insert_select_union", "INSERT INTO SELECT with UNION ALL from two sources",
      "write", "insert", "select", "union") { w =>
    w.sql("CREATE TABLE tbl (key INT, value STRING) USING delta")
    w.sql("INSERT INTO tbl VALUES (1,'initial')")
    w.sql("CREATE TEMPORARY VIEW source1 AS SELECT * FROM VALUES (2,'from_source1'),(3,'from_source1') AS t(key, value)")
    w.sql("CREATE TEMPORARY VIEW source2 AS SELECT * FROM VALUES (4,'from_source2'),(5,'from_source2') AS t(key, value)")
    w.sql("INSERT INTO tbl SELECT * FROM source1 UNION ALL SELECT * FROM source2")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, version = 1, name = "read_after_initial")
    w.snapshot(t)
  }

  // CT-014: CTAS from empty source
  test("CT_014_ctas_empty", "CTAS from empty source - schema with no data",
      "write", "create", "ctas", "empty") { w =>
    w.sql("CREATE TABLE tbl (key INT, value STRING) USING delta")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_empty")
    w.snapshot(t)
  }

  // CT-015: INSERT INTO SELECT with column reordering
  test("CT_015_insert_select_reorder", "INSERT INTO with explicit column reordering",
      "write", "insert", "select", "reorder") { w =>
    w.sql("CREATE TABLE tbl (key INT, value STRING) USING delta")
    w.sql("INSERT INTO tbl VALUES (1,'a')")
    w.sql("CREATE TEMPORARY VIEW source_ct015 AS SELECT * FROM VALUES ('b',2),('c',3) AS t(value, key)")
    w.sql("INSERT INTO tbl (value, key) SELECT value, key FROM source_ct015")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  // ==========================================================================
  // Restore Tests (RS-*)
  // Source: RestoreWriteCaptureSuite.scala / RestoreTableSuite.scala
  // ==========================================================================

  // RS-001: RESTORE to version 0 after INSERT
  test("RS_001_restore_after_insert", "RESTORE to v0 after INSERT - revert to initial 3 rows",
      "write", "restore") { w =>
    w.sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
    w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
    w.sql("INSERT INTO tbl VALUES (4,'d'),(5,'e')")
    w.sql("RESTORE TABLE tbl TO VERSION AS OF 1")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_after_restore")
    w.read(t, version = 2, name = "read_before_restore")
    w.snapshot(t)
  }

  // RS-002: RESTORE to version 0 after DELETE
  test("RS_002_restore_after_delete", "RESTORE to v0 after DELETE - recover deleted rows",
      "write", "restore", "delete") { w =>
    w.sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
    w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e')")
    w.sql("DELETE FROM tbl WHERE id > 3")
    w.sql("RESTORE TABLE tbl TO VERSION AS OF 1")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_after_restore")
    w.read(t, version = 2, name = "read_after_delete")
    w.snapshot(t)
  }

  // RS-003: RESTORE to version 0 after UPDATE
  test("RS_003_restore_after_update", "RESTORE to v0 after UPDATE - revert updated values",
      "write", "restore", "update") { w =>
    w.sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
    w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
    w.sql("UPDATE tbl SET name = 'updated'")
    w.sql("RESTORE TABLE tbl TO VERSION AS OF 1")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_after_restore")
    w.read(t, version = 2, name = "read_after_update")
    w.snapshot(t)
  }

  // RS-004: RESTORE to intermediate version
  test("RS_004_restore_intermediate", "RESTORE to v1 - revert delete but keep insert",
      "write", "restore") { w =>
    w.sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
    w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
    w.sql("INSERT INTO tbl VALUES (4,'d'),(5,'e')")
    w.sql("DELETE FROM tbl WHERE id = 1")
    w.sql("RESTORE TABLE tbl TO VERSION AS OF 2")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_after_restore")
    w.read(t, version = 3, name = "read_after_delete")
    w.snapshot(t)
  }

  // RS-005: RESTORE after multiple operations
  test("RS_005_restore_multi_ops", "RESTORE to v0 after insert, update, delete",
      "write", "restore") { w =>
    w.sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
    w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
    w.sql("INSERT INTO tbl VALUES (4,'d'),(5,'e')")
    w.sql("UPDATE tbl SET name = 'x' WHERE id <= 2")
    w.sql("DELETE FROM tbl WHERE id = 3")
    w.sql("RESTORE TABLE tbl TO VERSION AS OF 1")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_after_restore")
    w.read(t, version = 4, name = "read_before_restore")
    w.snapshot(t)
  }

  // RS-006: RESTORE on partitioned table
  test("RS_006_restore_partitioned", "RESTORE partitioned table to v0 - revert partition changes",
      "write", "restore", "partitioned") { w =>
    w.sql("CREATE TABLE tbl (id INT, name STRING, part STRING) USING delta PARTITIONED BY (part)")
    w.sql("INSERT INTO tbl VALUES (1,'a','p1'),(2,'b','p1'),(3,'c','p2'),(4,'d','p2')")
    w.sql("INSERT INTO tbl VALUES (5,'e','p1'),(6,'f','p1')")
    w.sql("DELETE FROM tbl WHERE part = 'p2'")
    w.sql("RESTORE TABLE tbl TO VERSION AS OF 1")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_after_restore")
    w.read(t, predicate = "part = 'p1'", name = "read_p1")
    w.read(t, predicate = "part = 'p2'", name = "read_p2")
    w.snapshot(t)
  }

  // RS-007: RESTORE after schema evolution (ADD COLUMN)
  test("RS_007_restore_schema_evolution", "RESTORE to v0 after ADD COLUMN - revert schema",
      "write", "restore", "schemaEvolution") { w =>
    w.sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
    w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
    w.sql("ALTER TABLE tbl ADD COLUMNS (score INT)")
    w.sql("INSERT INTO tbl VALUES (4,'d',100),(5,'e',200)")
    w.sql("RESTORE TABLE tbl TO VERSION AS OF 1")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_after_restore")
    w.read(t, version = 3, name = "read_with_score_column")
    w.snapshot(t)
  }

  // RS-008: RESTORE after MERGE
  test("RS_008_restore_after_merge", "RESTORE to v0 after MERGE upsert",
      "write", "restore", "merge") { w =>
    w.sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
    w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
    w.sql("CREATE TEMPORARY VIEW merge_source AS SELECT * FROM VALUES (2,'merged_b'),(4,'d'),(5,'e') AS t(id, name)")
    w.sql("""MERGE INTO tbl AS target
      USING merge_source AS source
      ON target.id = source.id
      WHEN MATCHED THEN UPDATE SET target.name = source.name
      WHEN NOT MATCHED THEN INSERT (id, name) VALUES (source.id, source.name)""")
    w.sql("RESTORE TABLE tbl TO VERSION AS OF 1")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_after_restore")
    w.read(t, version = 2, name = "read_after_merge")
    w.snapshot(t)
  }

  // RS-009: RESTORE with deletion vectors
  test("RS_009_restore_with_dvs", "RESTORE to v0 after DV-based delete",
      "write", "restore", "dv") { w =>
    w.sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e')")
    w.sql("DELETE FROM tbl WHERE id IN (2, 4)")
    w.sql("RESTORE TABLE tbl TO VERSION AS OF 1")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_after_restore")
    w.read(t, version = 2, name = "read_after_dv_delete")
    w.snapshot(t)
  }

  // RS-010: RESTORE to version 0 of empty table
  test("RS_010_restore_empty_table", "RESTORE to v0 of empty table",
      "write", "restore", "empty") { w =>
    w.sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
    w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
    w.sql("RESTORE TABLE tbl TO VERSION AS OF 0")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_after_restore_empty")
    w.read(t, version = 1, name = "read_before_restore")
    w.snapshot(t)
  }

  // RS-011: Double RESTORE
  test("RS_011_double_restore", "RESTORE to v1 then RESTORE to v0",
      "write", "restore") { w =>
    w.sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
    w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
    w.sql("INSERT INTO tbl VALUES (4,'d'),(5,'e')")
    w.sql("DELETE FROM tbl WHERE id = 5")
    w.sql("RESTORE TABLE tbl TO VERSION AS OF 2")
    w.sql("RESTORE TABLE tbl TO VERSION AS OF 1")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_after_double_restore")
    w.read(t, version = 4, name = "read_after_first_restore")
    w.snapshot(t)
  }

  // RS-012: RESTORE after INSERT OVERWRITE
  test("RS_012_restore_after_overwrite", "RESTORE to v0 after INSERT OVERWRITE",
      "write", "restore", "overwrite") { w =>
    w.sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
    w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
    w.sql("INSERT OVERWRITE tbl VALUES (10,'x'),(20,'y')")
    w.sql("RESTORE TABLE tbl TO VERSION AS OF 1")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_after_restore")
    w.read(t, version = 2, name = "read_after_overwrite")
    w.snapshot(t)
  }

  // RS-013: RESTORE after OPTIMIZE
  test("RS_013_restore_after_optimize", "RESTORE to pre-optimize version",
      "write", "restore", "optimize") { w =>
    w.sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
    // 5 small batches to create many files
    w.sql("INSERT INTO tbl VALUES (1,'v1'),(2,'v2')")
    w.sql("INSERT INTO tbl VALUES (3,'v3'),(4,'v4')")
    w.sql("INSERT INTO tbl VALUES (5,'v5'),(6,'v6')")
    w.sql("INSERT INTO tbl VALUES (7,'v7'),(8,'v8')")
    w.sql("INSERT INTO tbl VALUES (9,'v9'),(10,'v10')")
    w.sql("OPTIMIZE tbl")
    // RESTORE to pre-optimize version (v5 = last insert)
    w.sql("RESTORE TABLE tbl TO VERSION AS OF 5")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_after_restore")
    w.read(t, version = 6, name = "read_after_optimize")
    w.snapshot(t)
  }

  // RS-014: RESTORE with row tracking enabled
  test("RS_014_restore_row_tracking", "RESTORE to v0 with row tracking enabled",
      "write", "restore", "rowTracking") { w =>
    w.sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
      TBLPROPERTIES ('delta.enableRowTracking' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
    w.sql("INSERT INTO tbl VALUES (4,'d'),(5,'e')")
    w.sql("UPDATE tbl SET name = 'updated' WHERE id = 1")
    w.sql("RESTORE TABLE tbl TO VERSION AS OF 1")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_after_restore")
    w.read(t, version = 3, name = "read_after_update")
    w.snapshot(t)
  }

  // RS-015: RESTORE after ALTER TABLE SET TBLPROPERTIES
  test("RS_015_restore_after_tblproperties", "RESTORE to v0 reverts property and data",
      "write", "restore", "alterTable") { w =>
    w.sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
    w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
    w.sql("ALTER TABLE tbl SET TBLPROPERTIES ('custom.prop' = 'value1')")
    w.sql("INSERT INTO tbl VALUES (4,'d'),(5,'e')")
    w.sql("RESTORE TABLE tbl TO VERSION AS OF 1")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_after_restore")
    w.read(t, version = 3, name = "read_before_restore")
    w.snapshot(t)
  }

  // ==========================================================================
  // Optimize Tests (OPT-*)
  // Source: OptimizeCompactionSuiteWriteCapture.scala / OptimizeCompactionSuite.scala
  // ==========================================================================

  // OPT-001: Basic unpartitioned OPTIMIZE
  test("OPT_001_basic_optimize", "Two inserts then OPTIMIZE to compact",
      "write", "optimize") { w =>
    w.sql("CREATE TABLE tbl (value INT) USING delta")
    w.sql("INSERT INTO tbl VALUES (1),(2),(3)")
    w.sql("INSERT INTO tbl VALUES (4),(5),(6)")
    w.sql("OPTIMIZE tbl")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_after_optimize")
    w.read(t, version = 2, name = "read_before_optimize")
    w.snapshot(t)
  }

  // OPT-002: Partitioned OPTIMIZE - all partitions
  test("OPT_002_partitioned_optimize_all", "Partitioned table OPTIMIZE all partitions",
      "write", "optimize", "partitioned") { w =>
    w.sql("CREATE TABLE tbl (value INT, id INT) USING delta PARTITIONED BY (id)")
    w.sql("INSERT INTO tbl VALUES (1,1),(2,0),(3,1)")
    w.sql("INSERT INTO tbl VALUES (4,0),(5,1),(6,0)")
    w.sql("OPTIMIZE tbl")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_after_optimize")
    w.read(t, predicate = "id = 0", name = "read_id_0")
    w.read(t, predicate = "id = 1", name = "read_id_1")
    w.snapshot(t)
  }

  // OPT-003: Partitioned OPTIMIZE with WHERE clause
  test("OPT_003_partitioned_optimize_where", "OPTIMIZE only partition id=0",
      "write", "optimize", "partitioned", "where") { w =>
    w.sql("CREATE TABLE tbl (value INT, id INT) USING delta PARTITIONED BY (id)")
    w.sql("INSERT INTO tbl VALUES (1,1),(2,0),(3,1)")
    w.sql("INSERT INTO tbl VALUES (4,0),(5,1),(6,0)")
    w.sql("OPTIMIZE tbl WHERE id = 0")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_after_optimize")
    w.read(t, predicate = "id = 0", name = "read_optimized_partition")
    w.read(t, predicate = "id = 1", name = "read_unoptimized_partition")
    w.snapshot(t)
  }

  // OPT-004: Multi-partition OPTIMIZE with WHERE
  test("OPT_004_multi_partition_optimize", "Multi-partition OPTIMIZE on specific partition",
      "write", "optimize", "partitioned", "where") { w =>
    w.sql("CREATE TABLE tbl (id LONG, date DATE, part LONG) USING delta PARTITIONED BY (date, part)")
    w.sql("CREATE TEMPORARY VIEW opt004_data1 AS SELECT id, DATE '2017-10-10' as date, id % 5 as part FROM range(10)")
    w.sql("INSERT INTO tbl SELECT * FROM opt004_data1")
    w.sql("CREATE TEMPORARY VIEW opt004_data2 AS SELECT id, DATE '2017-10-10' as date, id % 5 as part FROM range(100)")
    w.sql("INSERT INTO tbl SELECT * FROM opt004_data2")
    w.sql("OPTIMIZE tbl WHERE date = '2017-10-10' and part = 3")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, predicate = "part = 3", name = "read_optimized_partition")
    w.snapshot(t)
  }

  // OPT-005: OPTIMIZE after many small inserts
  test("OPT_005_optimize_many_small_inserts", "5 small inserts then OPTIMIZE",
      "write", "optimize") { w =>
    w.sql("CREATE TABLE tbl (id INT, data STRING) USING delta")
    w.sql("INSERT INTO tbl VALUES (1, 'row1')")
    w.sql("INSERT INTO tbl VALUES (2, 'row2')")
    w.sql("INSERT INTO tbl VALUES (3, 'row3')")
    w.sql("INSERT INTO tbl VALUES (4, 'row4')")
    w.sql("INSERT INTO tbl VALUES (5, 'row5')")
    w.sql("OPTIMIZE tbl")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_after_optimize")
    w.read(t, version = 5, name = "read_before_optimize")
    w.snapshot(t)
  }

  // ==========================================================================
  // Log Replay Tests (LR-*)
  // Source: LogReplayWriteCapture.scala (synthetic log replay scenarios)
  // ==========================================================================

  // LR-001: Add then remove - INSERT then DELETE all
  test("LR_001_add_then_remove", "INSERT 5 rows then DELETE all - empty table",
      "write", "log_replay", "insert", "delete") { w =>
    w.sql("CREATE TABLE tbl (id INT, val STRING) USING delta")
    w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e')")
    w.sql("DELETE FROM tbl")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_latest_empty")
    w.read(t, version = 1, name = "read_before_delete")
    w.snapshot(t)
  }

  // LR-002: Add remove re-add
  test("LR_002_add_remove_readd", "INSERT data A, DELETE all, INSERT data B - only B remains",
      "write", "log_replay", "insert", "delete") { w =>
    w.sql("CREATE TABLE tbl (id INT, val STRING) USING delta")
    w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
    w.sql("DELETE FROM tbl")
    w.sql("INSERT INTO tbl VALUES (10,'x'),(20,'y')")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_latest_only_B")
    w.read(t, version = 1, name = "read_data_A")
    w.snapshot(t)
  }

  // LR-003: Checkpoint supersedes log - 11+ INSERTs to force checkpoint
  test("LR_003_checkpoint_supersedes_log", "11 inserts force checkpoint at v10",
      "write", "log_replay", "checkpoint") { w =>
    w.sql("CREATE TABLE tbl (id INT, val STRING) USING delta TBLPROPERTIES ('delta.checkpointInterval' = '10')")
    for (i <- 1 to 11) {
      w.sql(s"INSERT INTO tbl VALUES ($i, '${('a' + i - 1).toChar}')")
    }
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_latest_all_11")
    w.snapshot(t)
  }

  // LR-004: No checkpoint full replay - 5 INSERTs
  test("LR_004_no_checkpoint_full_replay", "5 inserts without checkpoint",
      "write", "log_replay", "insert") { w =>
    w.sql("CREATE TABLE tbl (id INT, val STRING) USING delta")
    for (i <- 1 to 5) {
      w.sql(s"INSERT INTO tbl VALUES ($i, '${('a' + i - 1).toChar}')")
    }
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_latest_all_5")
    w.snapshot(t)
  }

  // LR-005: Metadata latest wins - ALTER TABLE ADD COLUMN
  test("LR_005_metadata_latest_wins", "ADD COLUMN then insert with new schema",
      "write", "log_replay", "metadata", "schema_evolution") { w =>
    w.sql("CREATE TABLE tbl (id INT) USING delta")
    w.sql("INSERT INTO tbl VALUES (1),(2),(3)")
    w.sql("ALTER TABLE tbl ADD COLUMN name STRING")
    w.sql("INSERT INTO tbl VALUES (4,'alice'),(5,'bob')")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_latest_two_cols")
    w.read(t, version = 1, name = "read_before_alter")
    w.snapshot(t)
  }

  // LR-006: Protocol latest wins - enable deletion vectors
  test("LR_006_protocol_latest_wins", "Enable DVs upgrades protocol",
      "write", "log_replay", "protocol") { w =>
    w.sql("CREATE TABLE tbl (id INT, val STRING) USING delta")
    w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b')")
    w.sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")
    w.sql("INSERT INTO tbl VALUES (3,'c')")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_latest_after_protocol_upgrade")
    w.read(t, version = 1, name = "read_before_upgrade")
    w.snapshot(t)
  }

  // LR-007: DataChange false (OPTIMIZE)
  test("LR_007_datachange_false_optimize", "5 inserts then OPTIMIZE - same logical data",
      "write", "log_replay", "optimize") { w =>
    w.sql("CREATE TABLE tbl (id INT, val STRING) USING delta")
    for (i <- 1 to 5) {
      w.sql(s"INSERT INTO tbl VALUES (${i * 2 - 1},'v${i * 2 - 1}'),(${i * 2},'v${i * 2}')")
    }
    w.sql("OPTIMIZE tbl")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_latest_after_optimize")
    w.read(t, version = 5, name = "read_before_optimize")
    w.snapshot(t)
  }

  // LR-008: Many small inserts - 12 sequential single-row inserts
  test("LR_008_many_small_inserts", "12 single-row inserts",
      "write", "log_replay", "insert") { w =>
    w.sql("CREATE TABLE tbl (id INT, val STRING) USING delta")
    for (i <- 1 to 12) {
      w.sql(s"INSERT INTO tbl VALUES ($i, 'row$i')")
    }
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_latest_all_12")
    w.snapshot(t)
  }

  // LR-009: Insert + Update sequence
  test("LR_009_insert_update_sequence", "INSERT 5 rows then UPDATE 2",
      "write", "log_replay", "insert", "update") { w =>
    w.sql("CREATE TABLE tbl (id INT, amount INT) USING delta")
    w.sql("INSERT INTO tbl VALUES (1,100),(2,200),(3,300),(4,400),(5,500)")
    w.sql("UPDATE tbl SET amount = amount * 2 WHERE id <= 2")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_latest_after_update")
    w.read(t, version = 1, name = "read_before_update")
    w.snapshot(t)
  }

  // LR-010: Insert + Merge sequence
  test("LR_010_insert_merge_sequence", "INSERT then MERGE with overlapping keys",
      "write", "log_replay", "insert", "merge") { w =>
    w.sql("CREATE TABLE tbl (id INT, val STRING) USING delta")
    w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
    w.sql("CREATE TEMPORARY VIEW lr010_source AS SELECT * FROM VALUES (2,'B'),(3,'C'),(4,'D') AS t(id, val)")
    w.sql("""MERGE INTO tbl AS t
      USING lr010_source AS s
      ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET t.val = s.val
      WHEN NOT MATCHED THEN INSERT *""")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_latest_after_merge")
    w.read(t, version = 1, name = "read_before_merge")
    w.snapshot(t)
  }

  // LR-011: Multi-partition replay - 5 partitions
  test("LR_011_multi_partition_replay", "Inserts across 5 partitions",
      "write", "log_replay", "insert", "partitioned") { w =>
    w.sql("CREATE TABLE tbl (id INT, val STRING, part STRING) USING delta PARTITIONED BY (part)")
    w.sql("INSERT INTO tbl VALUES (1,'a1','A'),(2,'a2','A')")
    w.sql("INSERT INTO tbl VALUES (3,'b1','B'),(4,'b2','B')")
    w.sql("INSERT INTO tbl VALUES (5,'c1','C'),(6,'c2','C')")
    w.sql("INSERT INTO tbl VALUES (7,'d1','D'),(8,'d2','D')")
    w.sql("INSERT INTO tbl VALUES (9,'e1','E'),(10,'e2','E')")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all_partitions")
    w.read(t, predicate = "part = 'A'", name = "read_partA")
    w.read(t, predicate = "part = 'C'", name = "read_partC")
    w.read(t, predicate = "part = 'E'", name = "read_partE")
    w.snapshot(t)
  }

  // LR-012: Overwrite then read
  test("LR_012_overwrite_then_read", "INSERT OVERWRITE replaces all data",
      "write", "log_replay", "insert", "overwrite") { w =>
    w.sql("CREATE TABLE tbl (id INT, val STRING) USING delta")
    w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e')")
    w.sql("INSERT OVERWRITE tbl VALUES (10,'x'),(20,'y'),(30,'z')")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_latest_overwritten")
    w.read(t, version = 1, name = "read_before_overwrite")
    w.snapshot(t)
  }

  // LR-013: Schema evolution replay - add column
  test("LR_013_schema_evolution_replay", "ADD COLUMN then insert with new column",
      "write", "log_replay", "schema_evolution") { w =>
    w.sql("CREATE TABLE tbl (id INT, val STRING) USING delta")
    w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b')")
    w.sql("ALTER TABLE tbl ADD COLUMN extra_col DOUBLE")
    w.sql("INSERT INTO tbl VALUES (3,'c',3.14),(4,'d',2.72)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_latest_evolved_schema")
    w.read(t, version = 1, name = "read_before_evolution")
    w.snapshot(t)
  }

  // LR-014: Property change replay
  test("LR_014_property_change_replay", "SET TBLPROPERTIES then continue inserts",
      "write", "log_replay", "metadata", "properties") { w =>
    w.sql("CREATE TABLE tbl (id INT, val STRING) USING delta")
    w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b')")
    w.sql("ALTER TABLE tbl SET TBLPROPERTIES ('my.custom.key' = 'my_value')")
    w.sql("INSERT INTO tbl VALUES (3,'c')")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_latest_with_props")
    w.read(t, version = 1, name = "read_before_props")
    w.snapshot(t)
  }

  // LR-015: Delete from partition
  test("LR_015_delete_from_partition", "Delete partition B, verify A and C remain",
      "write", "log_replay", "delete", "partitioned") { w =>
    w.sql("CREATE TABLE tbl (id INT, val STRING, part STRING) USING delta PARTITIONED BY (part)")
    w.sql("INSERT INTO tbl VALUES (1,'a1','A'),(2,'a2','A')")
    w.sql("INSERT INTO tbl VALUES (3,'b1','B'),(4,'b2','B')")
    w.sql("INSERT INTO tbl VALUES (5,'c1','C'),(6,'c2','C')")
    w.sql("DELETE FROM tbl WHERE part = 'B'")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_latest_no_B")
    w.read(t, predicate = "part = 'A'", name = "read_partA")
    w.read(t, predicate = "part = 'B'", name = "read_partB_empty")
    w.read(t, predicate = "part = 'C'", name = "read_partC")
    w.snapshot(t)
  }

  // LR-016: MERGE upsert pattern
  test("LR_016_merge_upsert", "MERGE with 3 overlapping + 2 new keys",
      "write", "log_replay", "merge", "upsert") { w =>
    w.sql("CREATE TABLE tbl (id INT, val STRING) USING delta")
    w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e')")
    w.sql("CREATE TEMPORARY VIEW lr016_source AS SELECT * FROM VALUES (1,'A'),(3,'C'),(5,'E'),(6,'f'),(7,'g') AS t(id, val)")
    w.sql("""MERGE INTO tbl AS t
      USING lr016_source AS s
      ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET t.val = s.val
      WHEN NOT MATCHED THEN INSERT *""")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_latest_after_upsert")
    w.read(t, version = 1, name = "read_before_merge")
    w.snapshot(t)
  }

  // LR-017: Checkpoint after DML
  test("LR_017_checkpoint_after_dml", "insert, delete, insert, force checkpoint at interval=5",
      "write", "log_replay", "checkpoint") { w =>
    w.sql("CREATE TABLE tbl (id INT, val STRING) USING delta TBLPROPERTIES ('delta.checkpointInterval' = '5')")
    w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e')")
    w.sql("DELETE FROM tbl WHERE id > 3")
    w.sql("INSERT INTO tbl VALUES (6,'f'),(7,'g')")
    w.sql("INSERT INTO tbl VALUES (8,'h')")
    w.sql("INSERT INTO tbl VALUES (9,'i')")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_latest_after_checkpoint")
    w.read(t, version = 1, name = "read_initial_insert")
    w.snapshot(t)
  }

  // LR-018: Time travel across versions
  test("LR_018_time_travel_versions", "5 inserts, read each version",
      "write", "log_replay", "time_travel") { w =>
    w.sql("CREATE TABLE tbl (id INT, val STRING) USING delta")
    for (i <- 1 to 5) {
      w.sql(s"INSERT INTO tbl VALUES ($i, 'v$i')")
    }
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, version = 0, name = "read_v0_empty")
    w.read(t, version = 1, name = "read_v1")
    w.read(t, version = 2, name = "read_v2")
    w.read(t, version = 3, name = "read_v3")
    w.read(t, version = 4, name = "read_v4")
    w.read(t, version = 5, name = "read_v5_latest")
    w.snapshot(t)
  }

  // LR-019: Vacuum doesn't affect current data
  test("LR_019_vacuum_no_effect", "INSERT, DELETE, VACUUM - current data intact",
      "write", "log_replay", "vacuum") { w =>
    w.sql("CREATE TABLE tbl (id INT, val STRING) USING delta")
    w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e')")
    w.sql("DELETE FROM tbl WHERE id > 3")
    // VACUUM removes unreferenced files but does not change table state
    // Note: retention=0 requires spark.databricks.delta.retentionDurationCheck.enabled=false
    w.sql("VACUUM tbl RETAIN 0 HOURS")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_latest_after_vacuum")
    w.read(t, version = 1, name = "read_before_delete")
    w.snapshot(t)
  }

  // LR-020: Large number of files - 50 batches of 2 rows
  test("LR_020_large_number_of_files", "50 batches of 2 rows each - 100 rows total",
      "write", "log_replay", "insert", "many_files") { w =>
    w.sql("CREATE TABLE tbl (id INT, val STRING) USING delta")
    for (batch <- 1 to 50) {
      val id1 = batch * 2 - 1
      val id2 = batch * 2
      w.sql(s"INSERT INTO tbl VALUES ($id1,'r$id1'),($id2,'r$id2')")
    }
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_latest_all_100")
    w.snapshot(t)
  }

}.runAll()
