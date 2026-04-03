/**
 * Merge write workloads converted 1-to-1 from runtime capture suites.
 *
 * Sources:
 *   - MergeIntoSuiteBaseWriteCapture.scala (MRG-001 through MRG-020)
 *   - MergeIntoSQLSuiteWriteCapture.scala (MSQL-001, MSQL-002)
 *   - MergeStructEvolutionWriteCaptureSuite.scala (MS-001 through MS-015)
 *   - MergeStarExceptWriteCaptureSuite.scala (MX-001 through MX-015)
 *   - MergeNMBSWriteCaptureSuite.scala (NM-001 through NM-015)
 *
 * Each test generates a write_spec (commit history) plus read/snapshot verification specs.
 * Path-based tables are converted to managed tables via CREATE TABLE ... USING delta.
 * Source data for MERGE is created as separate managed tables.
 */

new WorkloadSuite("write_merge") {

  // ==========================================================================
  // MergeIntoSuiteBase tests (MRG-001 through MRG-020)
  // ==========================================================================

  // MRG-001: basic case - merge to Delta table, non-partitioned
  test("MRG_001_basic_merge_non_partitioned",
      "Basic MERGE with matched update and not-matched insert on non-partitioned table",
      "write", "merge") { w =>
    w.sql("CREATE TABLE target (key2 INT, value INT) USING delta")
    w.sql("INSERT INTO target VALUES (2, 2), (1, 4)")
    w.sql("CREATE TABLE src (key1 INT, value INT) USING delta")
    w.sql("INSERT INTO src VALUES (1, 1), (0, 3)")
    w.sql("""MERGE INTO target AS trgNew USING src AS src
      ON src.key1 = trgNew.key2
      WHEN MATCHED THEN UPDATE SET key2 = 20 + key1, value = 20 + src.value
      WHEN NOT MATCHED THEN INSERT (key2, value) VALUES (key1 - 10, src.value + 10)""")
    val t = w.table("target")
    w.writeSpec(t)
    w.read(t)
    w.read(t, version = 1, name = "before_merge")
    w.snapshot(t)
  }

  // MRG-002: basic case - merge to Delta table, partitioned
  test("MRG_002_basic_merge_partitioned",
      "Basic MERGE with matched update and not-matched insert on partitioned table",
      "write", "merge", "partitioned") { w =>
    w.sql("CREATE TABLE target (key2 INT, value INT) USING delta PARTITIONED BY (key2)")
    w.sql("INSERT INTO target VALUES (2, 2), (1, 4)")
    w.sql("CREATE TABLE src (key1 INT, value INT) USING delta")
    w.sql("INSERT INTO src VALUES (1, 1), (0, 3)")
    w.sql("""MERGE INTO target AS trgNew USING src AS src
      ON src.key1 = trgNew.key2
      WHEN MATCHED THEN UPDATE SET key2 = 20 + key1, value = 20 + src.value
      WHEN NOT MATCHED THEN INSERT (key2, value) VALUES (key1 - 10, src.value + 10)""")
    val t = w.table("target")
    w.writeSpec(t)
    w.read(t)
    w.read(t, version = 1, name = "before_merge")
    w.snapshot(t)
  }

  // MRG-003: update value from both source and target table
  test("MRG_003_update_from_both_source_and_target",
      "MERGE with UPDATE SET using both source and target columns",
      "write", "merge") { w =>
    w.sql("CREATE TABLE target (key2 INT, value INT) USING delta")
    w.sql("INSERT INTO target VALUES (2, 2), (1, 4)")
    w.sql("CREATE TABLE src (key1 INT, value INT) USING delta")
    w.sql("INSERT INTO src VALUES (1, 1), (0, 3)")
    w.sql("""MERGE INTO target AS trgNew USING src AS src
      ON src.key1 = trgNew.key2
      WHEN MATCHED THEN UPDATE SET key2 = 20 + trgNew.key2, value = trgNew.value + src.value
      WHEN NOT MATCHED THEN INSERT (key2, value) VALUES (key1 - 10, src.value + 10)""")
    val t = w.table("target")
    w.writeSpec(t)
    w.read(t)
    w.read(t, version = 1, name = "before_merge")
    w.snapshot(t)
  }

  // MRG-004: not all columns are specified in update
  test("MRG_004_not_all_columns_in_update",
      "MERGE where UPDATE SET only specifies value column, key2 unchanged",
      "write", "merge") { w =>
    w.sql("CREATE TABLE target (key2 INT, value INT) USING delta")
    w.sql("INSERT INTO target VALUES (2, 2), (1, 4)")
    w.sql("CREATE TABLE src (key1 INT, value INT) USING delta")
    w.sql("INSERT INTO src VALUES (1, 1), (0, 3)")
    w.sql("""MERGE INTO target AS trgNew USING src AS src
      ON src.key1 = trgNew.key2
      WHEN MATCHED THEN UPDATE SET value = trgNew.value + 3
      WHEN NOT MATCHED THEN INSERT (key2, value) VALUES (key1 - 10, src.value + 10)""")
    val t = w.table("target")
    w.writeSpec(t)
    w.read(t)
    w.read(t, version = 1, name = "before_merge")
    w.snapshot(t)
  }

  // MRG-005: multiple inserts with conditional NOT MATCHED
  test("MRG_005_multiple_inserts",
      "MERGE with two NOT MATCHED clauses using conditions",
      "write", "merge") { w =>
    w.sql("CREATE TABLE target (key2 INT, value INT) USING delta")
    w.sql("INSERT INTO target VALUES (2, 2), (1, 4)")
    w.sql("CREATE TABLE src (key1 INT, value INT) USING delta")
    w.sql("INSERT INTO src VALUES (1, 1), (0, 3), (3, 5)")
    w.sql("""MERGE INTO target AS trgNew USING src AS src
      ON src.key1 = trgNew.key2
      WHEN NOT MATCHED AND key1 = 0 THEN INSERT (key2, value) VALUES (src.key1, src.value + 3)
      WHEN NOT MATCHED THEN INSERT (key2, value) VALUES (src.key1 - 10, src.value + 10)""")
    val t = w.table("target")
    w.writeSpec(t)
    w.read(t)
    w.read(t, version = 1, name = "before_merge")
    w.snapshot(t)
  }

  // MRG-006: only insert into empty target
  test("MRG_006_only_insert_empty_target",
      "MERGE into empty table - all source rows are NOT MATCHED",
      "write", "merge") { w =>
    w.sql("CREATE TABLE target (key2 INT, value INT) USING delta")
    w.sql("CREATE TABLE src (key1 INT, value INT) USING delta")
    w.sql("INSERT INTO src VALUES (5, 5)")
    w.sql("""MERGE INTO target USING src AS src
      ON src.key1 = target.key2
      WHEN MATCHED THEN UPDATE SET key2 = 20 + key1, value = 20 + src.value
      WHEN NOT MATCHED THEN INSERT (key2, value) VALUES (key1 - 10, src.value + 10)""")
    val t = w.table("target")
    w.writeSpec(t)
    w.read(t)
    w.snapshot(t)
  }

  // MRG-007: only update (all source rows match)
  test("MRG_007_only_update",
      "MERGE where all source rows match target - only updates, no inserts",
      "write", "merge") { w =>
    w.sql("CREATE TABLE target (key2 INT, value INT) USING delta")
    w.sql("INSERT INTO target VALUES (2, 2), (1, 4)")
    w.sql("CREATE TABLE src (key1 INT, value INT) USING delta")
    w.sql("INSERT INTO src VALUES (1, 5), (2, 9)")
    w.sql("""MERGE INTO target USING src AS src
      ON src.key1 = target.key2
      WHEN MATCHED THEN UPDATE SET key2 = 20 + key1, value = 20 + src.value
      WHEN NOT MATCHED THEN INSERT (key2, value) VALUES (key1 - 10, src.value + 10)""")
    val t = w.table("target")
    w.writeSpec(t)
    w.read(t)
    w.read(t, version = 1, name = "before_merge")
    w.snapshot(t)
  }

  // MRG-008: null value in target
  test("MRG_008_null_value_in_target",
      "MERGE with NULL key in target - NULL does not match any source row",
      "write", "merge") { w =>
    w.sql("CREATE TABLE target (key INT, value INT) USING delta")
    w.sql("INSERT INTO target VALUES (CAST(NULL AS INT), CAST(NULL AS INT)), (1, 1)")
    w.sql("CREATE TABLE src (key INT, value INT) USING delta")
    w.sql("INSERT INTO src VALUES (1, 10), (2, 20)")
    w.sql("""MERGE INTO target AS t USING src AS s
      ON s.key = t.key
      WHEN MATCHED THEN UPDATE SET t.value = s.value
      WHEN NOT MATCHED THEN INSERT (key, value) VALUES (s.key, s.value)""")
    val t = w.table("target")
    w.writeSpec(t)
    w.read(t)
    w.read(t, version = 1, name = "before_merge")
    w.snapshot(t)
  }

  // MRG-009: null value in source
  test("MRG_009_null_value_in_source",
      "MERGE with NULL key in source - NULL source row inserted as NOT MATCHED",
      "write", "merge") { w =>
    w.sql("CREATE TABLE target (key INT, value INT) USING delta")
    w.sql("INSERT INTO target VALUES (1, 1)")
    w.sql("CREATE TABLE src (key INT, value INT) USING delta")
    w.sql("INSERT INTO src VALUES (1, 10), (2, 20), (CAST(NULL AS INT), CAST(NULL AS INT))")
    w.sql("""MERGE INTO target AS t USING src AS s
      ON s.key = t.key
      WHEN MATCHED THEN UPDATE SET t.value = s.value
      WHEN NOT MATCHED THEN INSERT (key, value) VALUES (s.key, s.value)""")
    val t = w.table("target")
    w.writeSpec(t)
    w.read(t)
    w.read(t, version = 1, name = "before_merge")
    w.snapshot(t)
  }

  // MRG-010: extended syntax - only delete
  test("MRG_010_only_delete",
      "MERGE with only WHEN MATCHED THEN DELETE clause",
      "write", "merge") { w =>
    w.sql("CREATE TABLE target (key INT, value INT) USING delta")
    w.sql("INSERT INTO target VALUES (1, 1), (2, 2)")
    w.sql("CREATE TABLE src (key INT, value INT) USING delta")
    w.sql("INSERT INTO src VALUES (0, 0), (1, 10), (3, 30)")
    w.sql("""MERGE INTO target AS t USING src AS s
      ON s.key = t.key
      WHEN MATCHED THEN DELETE""")
    val t = w.table("target")
    w.writeSpec(t)
    w.read(t)
    w.read(t, version = 1, name = "before_merge")
    w.snapshot(t)
  }

  // MRG-011: extended syntax - only conditional delete
  test("MRG_011_only_conditional_delete",
      "MERGE with conditional WHEN MATCHED AND ... THEN DELETE",
      "write", "merge") { w =>
    w.sql("CREATE TABLE target (key INT, value INT) USING delta")
    w.sql("INSERT INTO target VALUES (1, 1), (2, 2), (3, 3)")
    w.sql("CREATE TABLE src (key INT, value INT) USING delta")
    w.sql("INSERT INTO src VALUES (0, 0), (1, 10), (2, 20), (3, 30)")
    w.sql("""MERGE INTO target AS t USING src AS s
      ON s.key = t.key
      WHEN MATCHED AND s.value <> 20 AND t.value <> 3 THEN DELETE""")
    val t = w.table("target")
    w.writeSpec(t)
    w.read(t)
    w.read(t, version = 1, name = "before_merge")
    w.snapshot(t)
  }

  // MRG-012: extended syntax - conditional update + delete
  test("MRG_012_conditional_update_and_delete",
      "MERGE with conditional UPDATE on s.key <> 1, otherwise DELETE",
      "write", "merge") { w =>
    w.sql("CREATE TABLE target (key INT, value INT) USING delta")
    w.sql("INSERT INTO target VALUES (1, 1), (2, 2), (3, 3)")
    w.sql("CREATE TABLE src (key INT, value INT) USING delta")
    w.sql("INSERT INTO src VALUES (0, 0), (1, 10), (2, 20)")
    w.sql("""MERGE INTO target AS t USING src AS s
      ON s.key = t.key
      WHEN MATCHED AND s.key <> 1 THEN UPDATE SET key = s.key, value = s.value
      WHEN MATCHED THEN DELETE""")
    val t = w.table("target")
    w.writeSpec(t)
    w.read(t)
    w.read(t, version = 1, name = "before_merge")
    w.snapshot(t)
  }

  // MRG-013: extended syntax - only update
  test("MRG_013_extended_only_update",
      "MERGE with only WHEN MATCHED THEN UPDATE (no insert/delete)",
      "write", "merge") { w =>
    w.sql("CREATE TABLE target (key INT, value INT) USING delta")
    w.sql("INSERT INTO target VALUES (1, 1), (2, 2)")
    w.sql("CREATE TABLE src (key INT, value INT) USING delta")
    w.sql("INSERT INTO src VALUES (0, 0), (1, 10), (3, 30)")
    w.sql("""MERGE INTO target AS t USING src AS s
      ON s.key = t.key
      WHEN MATCHED THEN UPDATE SET key = s.key, value = s.value""")
    val t = w.table("target")
    w.writeSpec(t)
    w.read(t)
    w.read(t, version = 1, name = "before_merge")
    w.snapshot(t)
  }

  // MRG-014: extended syntax - only conditional update
  test("MRG_014_extended_only_conditional_update",
      "MERGE with conditional UPDATE matching only specific rows",
      "write", "merge") { w =>
    w.sql("CREATE TABLE target (key INT, value INT) USING delta")
    w.sql("INSERT INTO target VALUES (1, 1), (2, 2), (3, 3)")
    w.sql("CREATE TABLE src (key INT, value INT) USING delta")
    w.sql("INSERT INTO src VALUES (0, 0), (1, 10), (2, 20), (3, 30)")
    w.sql("""MERGE INTO target AS t USING src AS s
      ON s.key = t.key
      WHEN MATCHED AND s.value <> 20 AND t.value <> 3 THEN UPDATE SET key = s.key, value = s.value""")
    val t = w.table("target")
    w.writeSpec(t)
    w.read(t)
    w.read(t, version = 1, name = "before_merge")
    w.snapshot(t)
  }

  // MRG-015: extended syntax - only insert
  test("MRG_015_extended_only_insert",
      "MERGE with only WHEN NOT MATCHED THEN INSERT clause",
      "write", "merge") { w =>
    w.sql("CREATE TABLE target (key INT, value INT) USING delta")
    w.sql("INSERT INTO target VALUES (1, 1), (2, 2)")
    w.sql("CREATE TABLE src (key INT, value INT) USING delta")
    w.sql("INSERT INTO src VALUES (0, 0), (1, 10), (3, 30)")
    w.sql("""MERGE INTO target AS t USING src AS s
      ON s.key = t.key
      WHEN NOT MATCHED THEN INSERT (key, value) VALUES (s.key, s.value)""")
    val t = w.table("target")
    w.writeSpec(t)
    w.read(t)
    w.read(t, version = 1, name = "before_merge")
    w.snapshot(t)
  }

  // MRG-016: upsert with only rows inserted (match condition never true)
  test("MRG_016_upsert_only_inserts",
      "MERGE with conditional UPDATE that never fires - only inserts happen",
      "write", "merge") { w =>
    w.sql("CREATE TABLE target (key2 INT, value INT) USING delta")
    w.sql("INSERT INTO target VALUES (2, 2), (1, 4)")
    w.sql("CREATE TABLE src (key1 INT, value INT) USING delta")
    w.sql("INSERT INTO src VALUES (1, 1), (0, 3)")
    w.sql("""MERGE INTO target AS trgNew USING src AS src
      ON src.key1 = trgNew.key2
      WHEN MATCHED AND key2 = 5 THEN UPDATE SET value = src.value + 3
      WHEN NOT MATCHED THEN INSERT (key2, value) VALUES (src.key1 - 10, src.value + 10)""")
    val t = w.table("target")
    w.writeSpec(t)
    w.read(t)
    w.read(t, version = 1, name = "before_merge")
    w.snapshot(t)
  }

  // MRG-017: extended syntax - update + insert
  test("MRG_017_update_and_insert",
      "MERGE with both MATCHED UPDATE and NOT MATCHED INSERT",
      "write", "merge") { w =>
    w.sql("CREATE TABLE target (key INT, value INT) USING delta")
    w.sql("INSERT INTO target VALUES (1, 1), (2, 2)")
    w.sql("CREATE TABLE src (key INT, value INT) USING delta")
    w.sql("INSERT INTO src VALUES (0, 0), (1, 10), (3, 30)")
    w.sql("""MERGE INTO target AS t USING src AS s
      ON s.key = t.key
      WHEN MATCHED THEN UPDATE SET key = s.key, value = s.value
      WHEN NOT MATCHED THEN INSERT (key, value) VALUES (s.key, s.value)""")
    val t = w.table("target")
    w.writeSpec(t)
    w.read(t)
    w.read(t, version = 1, name = "before_merge")
    w.snapshot(t)
  }

  // MRG-018: extended syntax - delete + insert
  test("MRG_018_delete_and_insert",
      "MERGE with MATCHED DELETE and NOT MATCHED INSERT",
      "write", "merge") { w =>
    w.sql("CREATE TABLE target (key INT, value INT) USING delta")
    w.sql("INSERT INTO target VALUES (1, 1), (2, 2)")
    w.sql("CREATE TABLE src (key INT, value INT) USING delta")
    w.sql("INSERT INTO src VALUES (0, 0), (1, 10), (3, 30)")
    w.sql("""MERGE INTO target AS t USING src AS s
      ON s.key = t.key
      WHEN MATCHED THEN DELETE
      WHEN NOT MATCHED THEN INSERT (key, value) VALUES (s.key, s.value)""")
    val t = w.table("target")
    w.writeSpec(t)
    w.read(t)
    w.read(t, version = 1, name = "before_merge")
    w.snapshot(t)
  }

  // MRG-019: extended syntax - conditional update + delete + insert
  test("MRG_019_conditional_update_delete_insert",
      "MERGE with all three clause types: conditional update, delete, and insert",
      "write", "merge") { w =>
    w.sql("CREATE TABLE target (key INT, value INT) USING delta")
    w.sql("INSERT INTO target VALUES (1, 1), (2, 2), (3, 3)")
    w.sql("CREATE TABLE src (key INT, value INT) USING delta")
    w.sql("INSERT INTO src VALUES (0, 0), (1, 10), (2, 20)")
    w.sql("""MERGE INTO target AS t USING src AS s
      ON s.key = t.key
      WHEN MATCHED AND s.key <> 1 THEN UPDATE SET key = s.key, value = s.value
      WHEN MATCHED THEN DELETE
      WHEN NOT MATCHED THEN INSERT (key, value) VALUES (s.key, s.value)""")
    val t = w.table("target")
    w.writeSpec(t)
    w.read(t)
    w.read(t, version = 1, name = "before_merge")
    w.snapshot(t)
  }

  // MRG-020: both source and target empty (no-op merge)
  test("MRG_020_both_empty",
      "MERGE with empty source and empty target - may be no-op",
      "write", "merge") { w =>
    w.sql("CREATE TABLE target (key2 INT, value INT) USING delta")
    w.sql("CREATE TABLE src (key1 INT, value INT) USING delta")
    w.sql("""MERGE INTO target USING src AS src
      ON src.key1 = target.key2
      WHEN MATCHED THEN UPDATE SET key2 = 20 + key1, value = 20 + src.value
      WHEN NOT MATCHED THEN INSERT (key2, value) VALUES (key1 - 10, src.value + 10)""")
    val t = w.table("target")
    w.writeSpec(t)
    w.read(t)
    w.snapshot(t)
  }

  // ==========================================================================
  // MergeIntoSQLSuite tests (MSQL-001, MSQL-002)
  // ==========================================================================

  // MSQL-001: CTE as source in MERGE
  test("MSQL_001_cte_as_source",
      "MERGE using WITH clause CTE as source",
      "write", "merge", "cte") { w =>
    w.sql("CREATE TABLE target (key2 INT, value INT) USING delta")
    w.sql("INSERT INTO target VALUES (2, 2), (1, 4)")
    w.sql("CREATE TABLE src (key1 INT, value INT) USING delta")
    w.sql("INSERT INTO src VALUES (1, 1), (0, 3)")
    w.sql("""WITH cte1 AS (SELECT key1 + 2 AS key3, value FROM src)
      MERGE INTO target
      USING cte1 src
      ON src.key3 = target.key2
      WHEN MATCHED THEN UPDATE SET key2 = 20 + src.key3, value = 20 + src.value
      WHEN NOT MATCHED THEN INSERT (key2, value) VALUES (src.key3 - 10, src.value + 10)""")
    val t = w.table("target")
    w.writeSpec(t)
    w.read(t)
    w.read(t, version = 1, name = "before_merge")
    w.snapshot(t)
  }

  // MSQL-002: Inline tables with UNION in MERGE source
  test("MSQL_002_inline_union_source",
      "MERGE using inline VALUES with UNION as source query",
      "write", "merge") { w =>
    w.sql("CREATE TABLE target (key2 INT, value INT) USING delta")
    w.sql("INSERT INTO target VALUES (2, 2), (1, 4)")
    w.sql("""MERGE INTO target AS trg
      USING (
        SELECT * FROM VALUES (1, 6, 'a') AS t1(key1, value, others)
        UNION
        SELECT * FROM VALUES (0, 3, 'b') AS t2(key1, value, others)
      ) src
      ON src.key1 = trg.key2
      WHEN MATCHED THEN UPDATE SET trg.key2 = 20 + key1, value = 20 + src.value
      WHEN NOT MATCHED THEN INSERT (trg.key2, value) VALUES (key1 - 10, src.value + 10)""")
    val t = w.table("target")
    w.writeSpec(t)
    w.read(t)
    w.read(t, version = 1, name = "before_merge")
    w.snapshot(t)
  }

  // ==========================================================================
  // MergeStructEvolution tests (MS-001 through MS-015)
  // ==========================================================================

  // MS-001: MERGE with new top-level column (schema evolution)
  test("MS_001_new_top_level_column",
      "MERGE with schema evolution adding a new top-level column from source",
      "write", "merge", "schemaEvolution") { w =>
    w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")
    w.sql("CREATE TABLE target (key INT, value STRING) USING delta")
    w.sql("INSERT INTO target VALUES (1, 'a'), (2, 'b')")
    w.sql("CREATE TABLE src (key INT, value STRING, extra INT) USING delta")
    w.sql("INSERT INTO src VALUES (1, 'x', 100), (3, 'c', 300)")
    w.sql("""MERGE INTO target
      USING src AS source ON target.key = source.key
      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *""")
    w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = false")
    val t = w.table("target")
    w.writeSpec(t)
    w.read(t)
    w.read(t, version = 1, name = "before_merge")
    w.snapshot(t)
  }

  // MS-002: MERGE with new nested struct field
  test("MS_002_new_nested_struct_field",
      "MERGE with schema evolution adding a new field inside a nested struct",
      "write", "merge", "schemaEvolution", "struct") { w =>
    w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")
    w.sql("CREATE TABLE target (id INT, info STRUCT<name: STRING>) USING delta")
    w.sql("INSERT INTO target VALUES (1, named_struct('name', 'alice'))")
    w.sql("CREATE TABLE src (id INT, info STRUCT<name: STRING, age: INT>) USING delta")
    w.sql("INSERT INTO src VALUES (1, named_struct('name', 'alice_updated', 'age', 30))")
    w.sql("""MERGE INTO target
      USING src AS source ON target.id = source.id
      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *""")
    w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = false")
    val t = w.table("target")
    w.writeSpec(t)
    w.read(t)
    w.read(t, version = 1, name = "before_merge")
    w.snapshot(t)
  }

  // MS-003: MERGE insert new rows with struct evolution
  test("MS_003_insert_with_struct_evolution",
      "MERGE insert-only with schema evolution adding new struct field",
      "write", "merge", "schemaEvolution", "struct") { w =>
    w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")
    w.sql("CREATE TABLE target (id INT, info STRUCT<name: STRING>) USING delta")
    w.sql("INSERT INTO target VALUES (1, named_struct('name', 'alice'))")
    w.sql("CREATE TABLE src (id INT, info STRUCT<name: STRING, score: INT>) USING delta")
    w.sql("INSERT INTO src VALUES (2, named_struct('name', 'bob', 'score', 95))")
    w.sql("""MERGE INTO target
      USING src AS source ON target.id = source.id
      WHEN NOT MATCHED THEN INSERT *""")
    w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = false")
    val t = w.table("target")
    w.writeSpec(t)
    w.read(t)
    w.read(t, version = 1, name = "before_merge")
    w.snapshot(t)
  }

  // MS-004: MERGE with nested struct update (update inner field only)
  test("MS_004_nested_struct_update",
      "MERGE updating only the struct column on matched rows",
      "write", "merge", "struct") { w =>
    w.sql("CREATE TABLE target (id INT, info STRUCT<name: STRING, score: INT>) USING delta")
    w.sql("""INSERT INTO target VALUES
      (1, named_struct('name', 'alice', 'score', 80)),
      (2, named_struct('name', 'bob', 'score', 70))""")
    w.sql("CREATE TABLE src (id INT, info STRUCT<name: STRING, score: INT>) USING delta")
    w.sql("INSERT INTO src VALUES (1, named_struct('name', 'alice', 'score', 95))")
    w.sql("""MERGE INTO target
      USING src AS source ON target.id = source.id
      WHEN MATCHED THEN UPDATE SET target.info = source.info""")
    val t = w.table("target")
    w.writeSpec(t)
    w.read(t)
    w.read(t, version = 1, name = "before_merge")
    w.snapshot(t)
  }

  // MS-005: MERGE with array column
  test("MS_005_array_column",
      "MERGE with ARRAY<STRING> column - update and insert",
      "write", "merge", "array") { w =>
    w.sql("CREATE TABLE target (id INT, tags ARRAY<STRING>) USING delta")
    w.sql("""INSERT INTO target VALUES
      (1, array('a', 'b')),
      (2, array('c'))""")
    w.sql("CREATE TABLE src (id INT, tags ARRAY<STRING>) USING delta")
    w.sql("""INSERT INTO src
      SELECT 1 as id, array('x', 'y', 'z') as tags
      UNION ALL
      SELECT 3 as id, array('d') as tags""")
    w.sql("""MERGE INTO target
      USING src AS source ON target.id = source.id
      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *""")
    val t = w.table("target")
    w.writeSpec(t)
    w.read(t)
    w.read(t, version = 1, name = "before_merge")
    w.snapshot(t)
  }

  // MS-006: MERGE with map column
  test("MS_006_map_column",
      "MERGE with MAP<STRING, INT> column - update and insert",
      "write", "merge", "map") { w =>
    w.sql("CREATE TABLE target (id INT, props MAP<STRING, INT>) USING delta")
    w.sql("""INSERT INTO target VALUES
      (1, map('a', 1, 'b', 2)),
      (2, map('c', 3))""")
    w.sql("CREATE TABLE src (id INT, props MAP<STRING, INT>) USING delta")
    w.sql("""INSERT INTO src
      SELECT 1 as id, map('a', 10, 'd', 4) as props
      UNION ALL
      SELECT 3 as id, map('e', 5) as props""")
    w.sql("""MERGE INTO target
      USING src AS source ON target.id = source.id
      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *""")
    val t = w.table("target")
    w.writeSpec(t)
    w.read(t)
    w.read(t, version = 1, name = "before_merge")
    w.snapshot(t)
  }

  // MS-007: MERGE evolve struct with nullable field
  test("MS_007_struct_evolution_nullable_field",
      "MERGE with schema evolution adding nullable field inside struct",
      "write", "merge", "schemaEvolution", "struct") { w =>
    w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")
    w.sql("CREATE TABLE target (id INT, data STRUCT<x: INT>) USING delta")
    w.sql("INSERT INTO target VALUES (1, named_struct('x', 10))")
    w.sql("CREATE TABLE src (id INT, data STRUCT<x: INT, y: INT>) USING delta")
    w.sql("INSERT INTO src VALUES (1, named_struct('x', 20, 'y', CAST(null AS INT)))")
    w.sql("""MERGE INTO target
      USING src AS source ON target.id = source.id
      WHEN MATCHED THEN UPDATE SET *""")
    w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = false")
    val t = w.table("target")
    w.writeSpec(t)
    w.read(t)
    w.read(t, version = 1, name = "before_merge")
    w.snapshot(t)
  }

  // MS-008: MERGE with deeply nested struct (struct inside struct)
  test("MS_008_deeply_nested_struct",
      "MERGE with schema evolution on struct inside struct",
      "write", "merge", "schemaEvolution", "struct") { w =>
    w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")
    w.sql("CREATE TABLE target (id INT, outer_s STRUCT<inner_s: STRUCT<val: INT>>) USING delta")
    w.sql("INSERT INTO target VALUES (1, named_struct('inner_s', named_struct('val', 10)))")
    w.sql("CREATE TABLE src (id INT, outer_s STRUCT<inner_s: STRUCT<val: INT, extra: STRING>>) USING delta")
    w.sql("INSERT INTO src VALUES (1, named_struct('inner_s', named_struct('val', 20, 'extra', 'hello')))")
    w.sql("""MERGE INTO target
      USING src AS source ON target.id = source.id
      WHEN MATCHED THEN UPDATE SET *""")
    w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = false")
    val t = w.table("target")
    w.writeSpec(t)
    w.read(t)
    w.read(t, version = 1, name = "before_merge")
    w.snapshot(t)
  }

  // MS-009: MERGE with multiple struct columns evolving simultaneously
  test("MS_009_multiple_struct_columns_evolving",
      "MERGE with schema evolution on two struct columns at once",
      "write", "merge", "schemaEvolution", "struct") { w =>
    w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")
    w.sql("CREATE TABLE target (id INT, info STRUCT<name: STRING>, metrics STRUCT<score: INT>) USING delta")
    w.sql("INSERT INTO target VALUES (1, named_struct('name', 'alice'), named_struct('score', 80))")
    w.sql("CREATE TABLE src (id INT, info STRUCT<name: STRING, title: STRING>, metrics STRUCT<score: INT, grade: STRING>) USING delta")
    w.sql("INSERT INTO src VALUES (1, named_struct('name', 'alice', 'title', 'Dr'), named_struct('score', 95, 'grade', 'A'))")
    w.sql("""MERGE INTO target
      USING src AS source ON target.id = source.id
      WHEN MATCHED THEN UPDATE SET *""")
    w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = false")
    val t = w.table("target")
    w.writeSpec(t)
    w.read(t)
    w.read(t, version = 1, name = "before_merge")
    w.snapshot(t)
  }

  // MS-010: MERGE struct evolution with partitioned table
  test("MS_010_struct_evolution_partitioned",
      "MERGE with struct schema evolution on a partitioned table",
      "write", "merge", "schemaEvolution", "struct", "partitioned") { w =>
    w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")
    w.sql("CREATE TABLE target (id INT, data STRUCT<val: INT>, part STRING) USING delta PARTITIONED BY (part)")
    w.sql("""INSERT INTO target VALUES
      (1, named_struct('val', 10), 'p1'),
      (2, named_struct('val', 20), 'p2')""")
    w.sql("CREATE TABLE src (id INT, data STRUCT<val: INT, label: STRING>, part STRING) USING delta")
    w.sql("INSERT INTO src VALUES (1, named_struct('val', 100, 'label', 'updated'), 'p1')")
    w.sql("""MERGE INTO target
      USING src AS source ON target.id = source.id
      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *""")
    w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = false")
    val t = w.table("target")
    w.writeSpec(t)
    w.read(t)
    w.read(t, version = 1, name = "before_merge")
    w.snapshot(t)
  }

  // MS-011: MERGE with struct nullability (insert null struct)
  test("MS_011_null_struct_insert",
      "MERGE inserting a row with NULL struct value",
      "write", "merge", "struct") { w =>
    w.sql("CREATE TABLE target (id INT, info STRUCT<name: STRING, score: INT>) USING delta")
    w.sql("INSERT INTO target VALUES (1, named_struct('name', 'alice', 'score', 80))")
    w.sql("CREATE TABLE src (id INT, info STRUCT<name: STRING, score: INT>) USING delta")
    w.sql("INSERT INTO src VALUES (2, CAST(null AS STRUCT<name: STRING, score: INT>))")
    w.sql("""MERGE INTO target
      USING src AS source ON target.id = source.id
      WHEN NOT MATCHED THEN INSERT *""")
    val t = w.table("target")
    w.writeSpec(t)
    w.read(t)
    w.read(t, version = 1, name = "before_merge")
    w.snapshot(t)
  }

  // MS-012: MERGE with mixed update and insert struct evolution
  test("MS_012_mixed_update_insert_evolution",
      "MERGE with schema evolution on both update and insert paths",
      "write", "merge", "schemaEvolution") { w =>
    w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")
    w.sql("CREATE TABLE target (id INT, value STRING) USING delta")
    w.sql("INSERT INTO target VALUES (1, 'a'), (2, 'b')")
    w.sql("CREATE TABLE src (id INT, value STRING, extra INT) USING delta")
    w.sql("INSERT INTO src VALUES (1, 'x', 10), (3, 'c', 30)")
    w.sql("""MERGE INTO target
      USING src AS source ON target.id = source.id
      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *""")
    w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = false")
    val t = w.table("target")
    w.writeSpec(t)
    w.read(t)
    w.read(t, version = 1, name = "before_merge")
    w.snapshot(t)
  }

  // MS-013: MERGE with array of structs evolution
  test("MS_013_array_of_structs_evolution",
      "MERGE with schema evolution on ARRAY<STRUCT> column",
      "write", "merge", "schemaEvolution", "struct", "array") { w =>
    w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")
    w.sql("CREATE TABLE target (id INT, items ARRAY<STRUCT<name: STRING>>) USING delta")
    w.sql("INSERT INTO target VALUES (1, array(named_struct('name', 'item1')))")
    w.sql("CREATE TABLE src (id INT, items ARRAY<STRUCT<name: STRING, price: DOUBLE>>) USING delta")
    w.sql("INSERT INTO src VALUES (1, array(named_struct('name', 'item1_updated', 'price', 9.99)))")
    w.sql("""MERGE INTO target
      USING src AS source ON target.id = source.id
      WHEN MATCHED THEN UPDATE SET *""")
    w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = false")
    val t = w.table("target")
    w.writeSpec(t)
    w.read(t)
    w.read(t, version = 1, name = "before_merge")
    w.snapshot(t)
  }

  // MS-014: MERGE with conditional struct update
  test("MS_014_conditional_struct_update",
      "MERGE updating struct only when target.info.level < 3",
      "write", "merge", "struct") { w =>
    w.sql("CREATE TABLE target (id INT, info STRUCT<name: STRING, level: INT>) USING delta")
    w.sql("""INSERT INTO target VALUES
      (1, named_struct('name', 'alice', 'level', 1)),
      (2, named_struct('name', 'bob', 'level', 5)),
      (3, named_struct('name', 'carol', 'level', 3))""")
    w.sql("CREATE TABLE src (id INT, info STRUCT<name: STRING, level: INT>) USING delta")
    w.sql("""INSERT INTO src
      SELECT 1 as id, named_struct('name', 'alice', 'level', 10) as info
      UNION ALL
      SELECT 2 as id, named_struct('name', 'bob', 'level', 10) as info""")
    w.sql("""MERGE INTO target
      USING src AS source ON target.id = source.id
      WHEN MATCHED AND target.info.level < 3 THEN UPDATE SET target.info = source.info""")
    val t = w.table("target")
    w.writeSpec(t)
    w.read(t)
    w.read(t, version = 1, name = "before_merge")
    w.snapshot(t)
  }

  // MS-015: MERGE with struct and CDC
  test("MS_015_struct_evolution_with_cdc",
      "MERGE with struct schema evolution and CDF enabled",
      "write", "merge", "schemaEvolution", "struct", "cdf") { w =>
    w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")
    w.sql("""CREATE TABLE target (id INT, info STRUCT<name: STRING>) USING delta
      TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')""")
    w.sql("""INSERT INTO target VALUES
      (1, named_struct('name', 'alice')),
      (2, named_struct('name', 'bob'))""")
    w.sql("CREATE TABLE src (id INT, info STRUCT<name: STRING, score: INT>) USING delta")
    w.sql("""INSERT INTO src
      SELECT 1 as id, named_struct('name', 'alice_updated', 'score', 95) as info
      UNION ALL
      SELECT 3 as id, named_struct('name', 'carol', 'score', 80) as info""")
    w.sql("""MERGE INTO target
      USING src AS source ON target.id = source.id
      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *""")
    w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = false")
    val t = w.table("target")
    w.writeSpec(t)
    w.read(t)
    w.read(t, version = 1, name = "before_merge")
    w.cdf(t, startVersion = 2, endVersion = 2)
    w.snapshot(t)
  }

  // ==========================================================================
  // MergeStarExcept tests (MX-001 through MX-015)
  // ==========================================================================

  // MX-001: MERGE UPDATE SET * EXCEPT one column
  test("MX_001_update_star_except_one_column",
      "MERGE with UPDATE SET * EXCEPT (protected_col) - protected column unchanged",
      "write", "merge", "starExcept") { w =>
    w.sql("CREATE TABLE target (key INT, value STRING, protected_col STRING) USING delta")
    w.sql("INSERT INTO target VALUES (1, 'a', 'keep1'), (2, 'b', 'keep2')")
    w.sql("CREATE TABLE src (key INT, value STRING, protected_col STRING) USING delta")
    w.sql("INSERT INTO src VALUES (1, 'x', 'override1'), (3, 'c', 'new3')")
    w.sql("""MERGE INTO target
      USING src AS source ON target.key = source.key
      WHEN MATCHED THEN UPDATE SET * EXCEPT (protected_col)
      WHEN NOT MATCHED THEN INSERT *""")
    val t = w.table("target")
    w.writeSpec(t)
    w.read(t)
    w.read(t, version = 1, name = "before_merge")
    w.snapshot(t)
  }

  // MX-002: MERGE UPDATE SET * EXCEPT multiple columns
  test("MX_002_update_star_except_multiple",
      "MERGE with UPDATE SET * EXCEPT (val2, val3) - only key and val1 updated",
      "write", "merge", "starExcept") { w =>
    w.sql("CREATE TABLE target (key INT, val1 STRING, val2 STRING, val3 STRING) USING delta")
    w.sql("INSERT INTO target VALUES (1, 'a', 'b', 'c'), (2, 'd', 'e', 'f')")
    w.sql("CREATE TABLE src (key INT, val1 STRING, val2 STRING, val3 STRING) USING delta")
    w.sql("INSERT INTO src VALUES (1, 'x', 'y', 'z')")
    w.sql("""MERGE INTO target
      USING src AS source ON target.key = source.key
      WHEN MATCHED THEN UPDATE SET * EXCEPT (val2, val3)""")
    val t = w.table("target")
    w.writeSpec(t)
    w.read(t)
    w.read(t, version = 1, name = "before_merge")
    w.snapshot(t)
  }

  // MX-003: MERGE INSERT * EXCEPT one column
  test("MX_003_insert_star_except_one_column",
      "MERGE with INSERT * EXCEPT (auto_col) - inserted rows have null auto_col",
      "write", "merge", "starExcept") { w =>
    w.sql("CREATE TABLE target (key INT, value STRING, auto_col STRING) USING delta")
    w.sql("INSERT INTO target VALUES (1, 'a', 'auto1')")
    w.sql("CREATE TABLE src (key INT, value STRING, auto_col STRING) USING delta")
    w.sql("INSERT INTO src VALUES (1, 'x', 'src_auto'), (2, 'b', 'src_auto2')")
    w.sql("""MERGE INTO target
      USING src AS source ON target.key = source.key
      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT * EXCEPT (auto_col)""")
    val t = w.table("target")
    w.writeSpec(t)
    w.read(t)
    w.read(t, version = 1, name = "before_merge")
    w.snapshot(t)
  }

  // MX-004: Both UPDATE and INSERT with EXCEPT
  test("MX_004_both_clauses_star_except",
      "MERGE with UPDATE SET * EXCEPT (created_at) and INSERT * EXCEPT (updated_at)",
      "write", "merge", "starExcept") { w =>
    w.sql("CREATE TABLE target (key INT, value STRING, created_at STRING, updated_at STRING) USING delta")
    w.sql("INSERT INTO target VALUES (1, 'a', '2024-01-01', '2024-01-01'), (2, 'b', '2024-02-01', '2024-02-01')")
    w.sql("CREATE TABLE src (key INT, value STRING, created_at STRING, updated_at STRING) USING delta")
    w.sql("INSERT INTO src VALUES (1, 'x', '2024-03-01', '2024-03-01'), (3, 'c', '2024-04-01', '2024-04-01')")
    w.sql("""MERGE INTO target
      USING src AS source ON target.key = source.key
      WHEN MATCHED THEN UPDATE SET * EXCEPT (created_at)
      WHEN NOT MATCHED THEN INSERT * EXCEPT (updated_at)""")
    val t = w.table("target")
    w.writeSpec(t)
    w.read(t)
    w.read(t, version = 1, name = "before_merge")
    w.snapshot(t)
  }

  // MX-005: MERGE star-except on partitioned table
  test("MX_005_star_except_partitioned",
      "MERGE with UPDATE SET * EXCEPT (region) on partitioned table",
      "write", "merge", "starExcept", "partitioned") { w =>
    w.sql("CREATE TABLE target (key INT, region STRING, amount INT) USING delta PARTITIONED BY (region)")
    w.sql("INSERT INTO target VALUES (1, 'east', 100), (2, 'west', 200), (3, 'east', 300)")
    w.sql("CREATE TABLE src (key INT, region STRING, amount INT) USING delta")
    w.sql("INSERT INTO src VALUES (1, 'east', 150), (2, 'west', 250), (4, 'east', 400)")
    w.sql("""MERGE INTO target
      USING src AS source ON target.key = source.key
      WHEN MATCHED THEN UPDATE SET * EXCEPT (region)
      WHEN NOT MATCHED THEN INSERT *""")
    val t = w.table("target")
    w.writeSpec(t)
    w.read(t)
    w.read(t, version = 1, name = "before_merge")
    w.snapshot(t)
  }

  // MX-006: MERGE UPDATE SET * baseline (no except)
  test("MX_006_update_star_baseline",
      "MERGE with UPDATE SET * and INSERT * (no EXCEPT) for baseline comparison",
      "write", "merge") { w =>
    w.sql("CREATE TABLE target (key INT, value STRING, protected_col STRING) USING delta")
    w.sql("INSERT INTO target VALUES (1, 'a', 'keep1'), (2, 'b', 'keep2')")
    w.sql("CREATE TABLE src (key INT, value STRING, protected_col STRING) USING delta")
    w.sql("INSERT INTO src VALUES (1, 'x', 'override1'), (3, 'c', 'new3')")
    w.sql("""MERGE INTO target
      USING src AS source ON target.key = source.key
      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *""")
    val t = w.table("target")
    w.writeSpec(t)
    w.read(t)
    w.read(t, version = 1, name = "before_merge")
    w.snapshot(t)
  }

  // MX-007: MERGE INSERT * baseline (no except, insert only)
  test("MX_007_insert_star_baseline",
      "MERGE with only INSERT * clause - no matching rows, all inserted",
      "write", "merge") { w =>
    w.sql("CREATE TABLE target (key INT, value STRING, amount INT) USING delta")
    w.sql("INSERT INTO target VALUES (1, 'a', 100), (2, 'b', 200)")
    w.sql("CREATE TABLE src (key INT, value STRING, amount INT) USING delta")
    w.sql("INSERT INTO src VALUES (3, 'c', 300), (4, 'd', 400)")
    w.sql("""MERGE INTO target
      USING src AS source ON target.key = source.key
      WHEN NOT MATCHED THEN INSERT *""")
    val t = w.table("target")
    w.writeSpec(t)
    w.read(t)
    w.read(t, version = 1, name = "before_merge")
    w.snapshot(t)
  }

  // MX-008: MERGE conditional clause + star except
  test("MX_008_conditional_star_except",
      "MERGE with WHEN MATCHED AND condition THEN UPDATE SET * EXCEPT (key)",
      "write", "merge", "starExcept") { w =>
    w.sql("CREATE TABLE target (key INT, value STRING, amount INT) USING delta")
    w.sql("INSERT INTO target VALUES (1, 'a', 100), (2, 'b', 200), (3, 'c', 300)")
    w.sql("CREATE TABLE src (key INT, value STRING, amount INT) USING delta")
    w.sql("INSERT INTO src VALUES (1, 'a', 150), (2, 'x', 250)")
    w.sql("""MERGE INTO target
      USING src AS source ON target.key = source.key
      WHEN MATCHED AND target.value <> source.value
        THEN UPDATE SET * EXCEPT (key)""")
    val t = w.table("target")
    w.writeSpec(t)
    w.read(t)
    w.read(t, version = 1, name = "before_merge")
    w.snapshot(t)
  }

  // MX-009: MERGE star except with type coercion
  test("MX_009_star_except_type_coercion",
      "MERGE with UPDATE SET * EXCEPT (status) where source has narrower types",
      "write", "merge", "starExcept") { w =>
    w.sql("CREATE TABLE target (key BIGINT, amount DOUBLE, status STRING) USING delta")
    w.sql("INSERT INTO target VALUES (1, 1.5, 'keep'), (2, 2.5, 'keep')")
    w.sql("CREATE TABLE src (key INT, amount FLOAT, status STRING) USING delta")
    w.sql("INSERT INTO src VALUES (1, 10.0, 'override'), (3, 30.0, 'new')")
    w.sql("""MERGE INTO target
      USING src AS source ON target.key = source.key
      WHEN MATCHED THEN UPDATE SET * EXCEPT (status)
      WHEN NOT MATCHED THEN INSERT *""")
    val t = w.table("target")
    w.writeSpec(t)
    w.read(t)
    w.read(t, version = 1, name = "before_merge")
    w.snapshot(t)
  }

  // MX-010: MERGE star except with schema evolution
  test("MX_010_star_except_schema_evolution",
      "MERGE with schema evolution and INSERT * EXCEPT (extra_col)",
      "write", "merge", "starExcept", "schemaEvolution") { w =>
    w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")
    w.sql("CREATE TABLE target (key INT, value STRING) USING delta")
    w.sql("INSERT INTO target VALUES (1, 'a'), (2, 'b')")
    w.sql("CREATE TABLE src (key INT, value STRING, extra_col INT) USING delta")
    w.sql("INSERT INTO src VALUES (1, 'x', 100), (3, 'c', 300)")
    w.sql("""MERGE INTO target
      USING src AS source ON target.key = source.key
      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT * EXCEPT (extra_col)""")
    w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = false")
    val t = w.table("target")
    w.writeSpec(t)
    w.read(t)
    w.read(t, version = 1, name = "before_merge")
    w.snapshot(t)
  }

  // MX-011: MERGE with NMBS + star except
  test("MX_011_nmbs_star_except",
      "MERGE with UPDATE SET * EXCEPT (timestamp_col) and NOT MATCHED BY SOURCE update",
      "write", "merge", "starExcept", "notMatchedBySource") { w =>
    w.sql("CREATE TABLE target (key INT, value STRING, timestamp_col STRING) USING delta")
    w.sql("INSERT INTO target VALUES (1, 'a', '2024-01-01'), (2, 'b', '2024-01-02'), (3, 'c', '2024-01-03')")
    w.sql("CREATE TABLE src (key INT, value STRING, timestamp_col STRING) USING delta")
    w.sql("INSERT INTO src VALUES (1, 'x', '2024-02-01')")
    w.sql("""MERGE INTO target
      USING src AS source ON target.key = source.key
      WHEN MATCHED THEN UPDATE SET * EXCEPT (timestamp_col)
      WHEN NOT MATCHED BY SOURCE THEN UPDATE SET value = 'default'""")
    val t = w.table("target")
    w.writeSpec(t)
    w.read(t)
    w.read(t, version = 1, name = "before_merge")
    w.snapshot(t)
  }

  // MX-012: MERGE star except preserving defaults
  test("MX_012_star_except_preserving_defaults",
      "MERGE with UPDATE SET * EXCEPT (status) - status column preserved",
      "write", "merge", "starExcept") { w =>
    w.sql("CREATE TABLE target (key INT, value STRING, status STRING) USING delta")
    w.sql("INSERT INTO target VALUES (1, 'a', 'active'), (2, 'b', 'active'), (3, 'c', 'inactive')")
    w.sql("CREATE TABLE src (key INT, value STRING, status STRING) USING delta")
    w.sql("INSERT INTO src VALUES (1, 'x', 'suspended'), (2, 'y', 'suspended')")
    w.sql("""MERGE INTO target
      USING src AS source ON target.key = source.key
      WHEN MATCHED THEN UPDATE SET * EXCEPT (status)""")
    val t = w.table("target")
    w.writeSpec(t)
    w.read(t)
    w.read(t, version = 1, name = "before_merge")
    w.snapshot(t)
  }

  // MX-013: MERGE star except with wide table (8+ columns)
  test("MX_013_star_except_wide_table",
      "MERGE with UPDATE SET * EXCEPT (c6, c7, c8) on 9-column table",
      "write", "merge", "starExcept") { w =>
    w.sql("CREATE TABLE target (key INT, c1 STRING, c2 STRING, c3 STRING, c4 STRING, c5 STRING, c6 STRING, c7 STRING, c8 STRING) USING delta")
    w.sql("INSERT INTO target VALUES (1, 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h')")
    w.sql("CREATE TABLE src (key INT, c1 STRING, c2 STRING, c3 STRING, c4 STRING, c5 STRING, c6 STRING, c7 STRING, c8 STRING) USING delta")
    w.sql("INSERT INTO src VALUES (1, 'x1', 'x2', 'x3', 'x4', 'x5', 'x6', 'x7', 'x8'), (2, 'n1', 'n2', 'n3', 'n4', 'n5', 'n6', 'n7', 'n8')")
    w.sql("""MERGE INTO target
      USING src AS source ON target.key = source.key
      WHEN MATCHED THEN UPDATE SET * EXCEPT (c6, c7, c8)
      WHEN NOT MATCHED THEN INSERT *""")
    val t = w.table("target")
    w.writeSpec(t)
    w.read(t)
    w.read(t, version = 1, name = "before_merge")
    w.snapshot(t)
  }

  // MX-014: MERGE star except with NULL values in source
  test("MX_014_star_except_null_values",
      "MERGE with UPDATE SET * EXCEPT (protected_col) where source has NULLs",
      "write", "merge", "starExcept") { w =>
    w.sql("CREATE TABLE target (key INT, value STRING, protected_col STRING) USING delta")
    w.sql("INSERT INTO target VALUES (1, 'a', 'keep1'), (2, 'b', 'keep2')")
    w.sql("CREATE TABLE src (key INT, value STRING, protected_col STRING) USING delta")
    w.sql("INSERT INTO src VALUES (1, CAST(NULL AS STRING), 'override1'), (2, CAST(NULL AS STRING), 'override2')")
    w.sql("""MERGE INTO target
      USING src AS source ON target.key = source.key
      WHEN MATCHED THEN UPDATE SET * EXCEPT (protected_col)""")
    val t = w.table("target")
    w.writeSpec(t)
    w.read(t)
    w.read(t, version = 1, name = "before_merge")
    w.snapshot(t)
  }

  // MX-015: MERGE star except with all rows matched (no inserts)
  test("MX_015_star_except_all_matched",
      "MERGE with UPDATE SET * EXCEPT (id) where all rows match",
      "write", "merge", "starExcept") { w =>
    w.sql("CREATE TABLE target (id INT, value STRING, amount INT) USING delta")
    w.sql("INSERT INTO target VALUES (1, 'a', 100), (2, 'b', 200), (3, 'c', 300)")
    w.sql("CREATE TABLE src (id INT, value STRING, amount INT) USING delta")
    w.sql("INSERT INTO src VALUES (1, 'x', 150), (2, 'y', 250), (3, 'z', 350)")
    w.sql("""MERGE INTO target
      USING src AS source ON target.id = source.id
      WHEN MATCHED THEN UPDATE SET * EXCEPT (id)""")
    val t = w.table("target")
    w.writeSpec(t)
    w.read(t)
    w.read(t, version = 1, name = "before_merge")
    w.snapshot(t)
  }

  // ==========================================================================
  // MergeNMBS tests (NM-001 through NM-015)
  // ==========================================================================

  // NM-001: NMBS unconditional delete
  test("NM_001_nmbs_unconditional_delete",
      "MERGE with NOT MATCHED BY SOURCE THEN DELETE - unmatched rows deleted",
      "write", "merge", "notMatchedBySource") { w =>
    w.sql("CREATE TABLE target (key INT, value STRING) USING delta")
    w.sql("INSERT INTO target VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')")
    w.sql("CREATE TABLE src (key INT, value STRING) USING delta")
    w.sql("INSERT INTO src VALUES (1, 'x'), (2, 'y')")
    w.sql("""MERGE INTO target
      USING src AS source ON target.key = source.key
      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED BY SOURCE THEN DELETE""")
    val t = w.table("target")
    w.writeSpec(t)
    w.read(t)
    w.read(t, version = 1, name = "before_merge")
    w.snapshot(t)
  }

  // NM-002: NMBS conditional delete
  test("NM_002_nmbs_conditional_delete",
      "MERGE with NOT MATCHED BY SOURCE AND target.value > 20 THEN DELETE",
      "write", "merge", "notMatchedBySource") { w =>
    w.sql("CREATE TABLE target (key INT, value INT) USING delta")
    w.sql("INSERT INTO target VALUES (1, 10), (2, 20), (3, 30), (4, 40)")
    w.sql("CREATE TABLE src (key INT, value INT) USING delta")
    w.sql("INSERT INTO src VALUES (1, 100)")
    w.sql("""MERGE INTO target
      USING src AS source ON target.key = source.key
      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED BY SOURCE AND target.value > 20 THEN DELETE""")
    val t = w.table("target")
    w.writeSpec(t)
    w.read(t)
    w.read(t, version = 1, name = "before_merge")
    w.snapshot(t)
  }

  // NM-003: NMBS unconditional update
  test("NM_003_nmbs_unconditional_update",
      "MERGE with NOT MATCHED BY SOURCE THEN UPDATE SET value = 'nmbs_updated'",
      "write", "merge", "notMatchedBySource") { w =>
    w.sql("CREATE TABLE target (key INT, value STRING) USING delta")
    w.sql("INSERT INTO target VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')")
    w.sql("CREATE TABLE src (key INT, value STRING) USING delta")
    w.sql("INSERT INTO src VALUES (1, 'x')")
    w.sql("""MERGE INTO target
      USING src AS source ON target.key = source.key
      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED BY SOURCE THEN UPDATE SET value = 'nmbs_updated'""")
    val t = w.table("target")
    w.writeSpec(t)
    w.read(t)
    w.read(t, version = 1, name = "before_merge")
    w.snapshot(t)
  }

  // NM-004: NMBS conditional update
  test("NM_004_nmbs_conditional_update",
      "MERGE with NOT MATCHED BY SOURCE AND target.value >= 30 THEN UPDATE SET value = -1",
      "write", "merge", "notMatchedBySource") { w =>
    w.sql("CREATE TABLE target (key INT, value INT) USING delta")
    w.sql("INSERT INTO target VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)")
    w.sql("CREATE TABLE src (key INT, value INT) USING delta")
    w.sql("INSERT INTO src VALUES (1, 100)")
    w.sql("""MERGE INTO target
      USING src AS source ON target.key = source.key
      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED BY SOURCE AND target.value >= 30 THEN UPDATE SET value = -1""")
    val t = w.table("target")
    w.writeSpec(t)
    w.read(t)
    w.read(t, version = 1, name = "before_merge")
    w.snapshot(t)
  }

  // NM-005: NMBS update + delete combined
  test("NM_005_nmbs_update_and_delete",
      "MERGE with NMBS conditional DELETE for value > 30 and UPDATE to 0 for the rest",
      "write", "merge", "notMatchedBySource") { w =>
    w.sql("CREATE TABLE target (key INT, value INT) USING delta")
    w.sql("INSERT INTO target VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)")
    w.sql("CREATE TABLE src (key INT, value INT) USING delta")
    w.sql("INSERT INTO src VALUES (1, 100)")
    w.sql("""MERGE INTO target
      USING src AS source ON target.key = source.key
      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED BY SOURCE AND target.value > 30 THEN DELETE
      WHEN NOT MATCHED BY SOURCE THEN UPDATE SET value = 0""")
    val t = w.table("target")
    w.writeSpec(t)
    w.read(t)
    w.read(t, version = 1, name = "before_merge")
    w.snapshot(t)
  }

  // NM-006: All 3 clause types - matched + not matched + NMBS
  test("NM_006_all_three_clause_types",
      "MERGE with matched UPDATE, not matched INSERT, and NMBS conditional DELETE",
      "write", "merge", "notMatchedBySource") { w =>
    w.sql("CREATE TABLE target (key INT, value STRING) USING delta")
    w.sql("INSERT INTO target VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')")
    w.sql("CREATE TABLE src (key INT, value STRING) USING delta")
    w.sql("INSERT INTO src VALUES (1, 'x'), (6, 'f'), (7, 'g')")
    w.sql("""MERGE INTO target
      USING src AS source ON target.key = source.key
      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *
      WHEN NOT MATCHED BY SOURCE AND target.key > 3 THEN DELETE""")
    val t = w.table("target")
    w.writeSpec(t)
    w.read(t)
    w.read(t, version = 1, name = "before_merge")
    w.snapshot(t)
  }

  // NM-007: NMBS with partitioned table
  test("NM_007_nmbs_partitioned",
      "MERGE with NMBS DELETE on partitioned table - unmatched rows across partitions deleted",
      "write", "merge", "notMatchedBySource", "partitioned") { w =>
    w.sql("CREATE TABLE target (key INT, value STRING, part STRING) USING delta PARTITIONED BY (part)")
    w.sql("INSERT INTO target VALUES (1, 'a', 'p1'), (2, 'b', 'p1'), (3, 'c', 'p2'), (4, 'd', 'p2')")
    w.sql("CREATE TABLE src (key INT, value STRING, part STRING) USING delta")
    w.sql("INSERT INTO src VALUES (1, 'x', 'p1')")
    w.sql("""MERGE INTO target
      USING src AS source ON target.key = source.key
      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED BY SOURCE THEN DELETE""")
    val t = w.table("target")
    w.writeSpec(t)
    w.read(t)
    w.read(t, version = 1, name = "before_merge")
    w.snapshot(t)
  }

  // NM-008: NMBS delete only (no matched clause)
  test("NM_008_nmbs_delete_only",
      "MERGE with only NMBS DELETE clause - matched rows unchanged, unmatched deleted",
      "write", "merge", "notMatchedBySource") { w =>
    w.sql("CREATE TABLE target (key INT, value STRING) USING delta")
    w.sql("INSERT INTO target VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')")
    w.sql("CREATE TABLE src (key INT, value STRING) USING delta")
    w.sql("INSERT INTO src VALUES (2, 'x'), (3, 'y')")
    w.sql("""MERGE INTO target
      USING src AS source ON target.key = source.key
      WHEN NOT MATCHED BY SOURCE THEN DELETE""")
    val t = w.table("target")
    w.writeSpec(t)
    w.read(t)
    w.read(t, version = 1, name = "before_merge")
    w.snapshot(t)
  }

  // NM-009: NMBS with empty source
  test("NM_009_nmbs_empty_source",
      "MERGE with empty source - all target rows are NOT MATCHED BY SOURCE and deleted",
      "write", "merge", "notMatchedBySource") { w =>
    w.sql("CREATE TABLE target (key INT, value STRING) USING delta")
    w.sql("INSERT INTO target VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    w.sql("CREATE TABLE src (key INT, value STRING) USING delta")
    w.sql("""MERGE INTO target
      USING src AS source ON target.key = source.key
      WHEN NOT MATCHED BY SOURCE THEN DELETE""")
    val t = w.table("target")
    w.writeSpec(t)
    w.read(t)
    w.read(t, version = 1, name = "before_merge")
    w.snapshot(t)
  }

  // NM-010: NMBS update + not matched insert (no matched clause)
  test("NM_010_nmbs_insert_and_update",
      "MERGE with NOT MATCHED INSERT and NMBS UPDATE, no MATCHED clause",
      "write", "merge", "notMatchedBySource") { w =>
    w.sql("CREATE TABLE target (key INT, value INT) USING delta")
    w.sql("INSERT INTO target VALUES (1, 10), (2, 20), (3, 30)")
    w.sql("CREATE TABLE src (key INT, value INT) USING delta")
    w.sql("INSERT INTO src VALUES (1, 100), (4, 40), (5, 50)")
    w.sql("""MERGE INTO target
      USING src AS source ON target.key = source.key
      WHEN NOT MATCHED THEN INSERT *
      WHEN NOT MATCHED BY SOURCE THEN UPDATE SET value = 0""")
    val t = w.table("target")
    w.writeSpec(t)
    w.read(t)
    w.read(t, version = 1, name = "before_merge")
    w.snapshot(t)
  }

  // NM-011: NMBS with DVs enabled
  test("NM_011_nmbs_with_dvs",
      "MERGE with NMBS DELETE on DV-enabled table",
      "write", "merge", "notMatchedBySource", "dv") { w =>
    w.sql("""CREATE TABLE target (key INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO target VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')")
    w.sql("CREATE TABLE src (key INT, value STRING) USING delta")
    w.sql("INSERT INTO src VALUES (1, 'x'), (2, 'y')")
    w.sql("""MERGE INTO target
      USING src AS source ON target.key = source.key
      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED BY SOURCE THEN DELETE""")
    val t = w.table("target")
    w.writeSpec(t)
    w.read(t)
    w.read(t, version = 1, name = "before_merge")
    w.snapshot(t)
  }

  // NM-012: NMBS conditional update with conditional matched
  test("NM_012_nmbs_conditional_both",
      "MERGE with conditional MATCHED and conditional NMBS update clauses",
      "write", "merge", "notMatchedBySource") { w =>
    w.sql("CREATE TABLE target (key INT, value STRING, score INT) USING delta")
    w.sql("INSERT INTO target VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30), (4, 'd', 40)")
    w.sql("CREATE TABLE src (key INT, value STRING, score INT) USING delta")
    w.sql("INSERT INTO src VALUES (1, 'x', 100), (2, 'y', 200)")
    w.sql("""MERGE INTO target
      USING src AS source ON target.key = source.key
      WHEN MATCHED AND source.score > 150 THEN UPDATE SET target.value = source.value
      WHEN NOT MATCHED BY SOURCE AND target.score > 20 THEN UPDATE SET target.value = 'skipped'""")
    val t = w.table("target")
    w.writeSpec(t)
    w.read(t)
    w.read(t, version = 1, name = "before_merge")
    w.snapshot(t)
  }

  // NM-013: Insert only with dummy NMBS (no effect)
  test("NM_013_nmbs_no_effect",
      "MERGE with NOT MATCHED INSERT and NMBS with impossible condition (key < 0)",
      "write", "merge", "notMatchedBySource") { w =>
    w.sql("CREATE TABLE target (key INT, value STRING) USING delta")
    w.sql("INSERT INTO target VALUES (1, 'a'), (2, 'b')")
    w.sql("CREATE TABLE src (key INT, value STRING) USING delta")
    w.sql("INSERT INTO src VALUES (3, 'c'), (4, 'd')")
    w.sql("""MERGE INTO target
      USING src AS source ON target.key = source.key
      WHEN NOT MATCHED THEN INSERT *
      WHEN NOT MATCHED BY SOURCE AND target.key < 0 THEN DELETE""")
    val t = w.table("target")
    w.writeSpec(t)
    w.read(t)
    w.read(t, version = 1, name = "before_merge")
    w.snapshot(t)
  }

  // NM-014: NMBS with multiple NMBS clauses
  test("NM_014_nmbs_multi_clause",
      "MERGE with three NMBS clauses: delete high, negate mid, zero rest",
      "write", "merge", "notMatchedBySource") { w =>
    w.sql("CREATE TABLE target (key INT, value INT) USING delta")
    w.sql("INSERT INTO target VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)")
    w.sql("CREATE TABLE src (key INT, value INT) USING delta")
    w.sql("INSERT INTO src VALUES (1, 100)")
    w.sql("""MERGE INTO target
      USING src AS source ON target.key = source.key
      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED BY SOURCE AND target.value > 40 THEN DELETE
      WHEN NOT MATCHED BY SOURCE AND target.value > 20 THEN UPDATE SET value = -target.value
      WHEN NOT MATCHED BY SOURCE THEN UPDATE SET value = 0""")
    val t = w.table("target")
    w.writeSpec(t)
    w.read(t)
    w.read(t, version = 1, name = "before_merge")
    w.snapshot(t)
  }

  // NM-015: All clauses - matched update+delete, not matched insert, NMBS update+delete
  test("NM_015_all_clauses_full",
      "MERGE with all clause types: conditional matched update+delete, insert, NMBS update+delete",
      "write", "merge", "notMatchedBySource") { w =>
    w.sql("CREATE TABLE target (key INT, value INT, cat STRING) USING delta")
    w.sql("INSERT INTO target VALUES (1, 10, 'a'), (2, 20, 'b'), (3, 30, 'c'), (4, 40, 'd'), (5, 50, 'e')")
    w.sql("CREATE TABLE src (key INT, value INT, cat STRING) USING delta")
    w.sql("INSERT INTO src VALUES (1, 100, 'x'), (2, 200, 'y'), (6, 60, 'f')")
    w.sql("""MERGE INTO target
      USING src AS source ON target.key = source.key
      WHEN MATCHED AND source.value > 150 THEN UPDATE SET target.value = source.value, target.cat = source.cat
      WHEN MATCHED THEN DELETE
      WHEN NOT MATCHED THEN INSERT *
      WHEN NOT MATCHED BY SOURCE AND target.value > 40 THEN DELETE
      WHEN NOT MATCHED BY SOURCE THEN UPDATE SET target.cat = 'nmbs'""")
    val t = w.table("target")
    w.writeSpec(t)
    w.read(t)
    w.read(t, version = 1, name = "before_merge")
    w.snapshot(t)
  }

}.runAll()
