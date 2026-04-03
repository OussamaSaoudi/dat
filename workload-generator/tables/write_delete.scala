/**
 * Delete write workloads converted 1-to-1 from runtime capture suites.
 *
 * Sources:
 *   - DeleteSuiteBaseWriteCapture.scala (DB-001 through DB-026)
 *   - DeleteDvSuiteWriteCapture.scala (DDV-001 through DDV-005)
 *   - DeleteCDCSuiteWriteCapture.scala (DeleteCDC-001 through DeleteCDC-014)
 *
 * Skipped tests (with justification):
 *   DB-008/009/010: NullType columns (no SQL equivalent)
 *   DB-011/012/013: Error tests (not write specs)
 *   DB-016/017/018: Time-dependent predicates (non-reproducible)
 *   DB-023/024: Schema pruning (execution plan tests, not data)
 *   DB-025: Unsupported expressions (error tests)
 *   DB-027: Variant type (DBR-only parse_json)
 *   DeleteCDC-006: DBR-only randomized file prefixes
 *   DeleteCDC-007/008/009: Edge-only subquery tests
 *   DeleteCDC-011: Multi-version CDC with internal computeCDC helper
 *   DeleteCDC-012: Edge-only usage metrics
 *   DeleteCDC-013: Edge-only subquery variant
 *   DeleteCDC-015/016: Edge-only DV+subquery tests
 *   DeleteCDC-017: Multi-version DV CDC with addDeletionVectorsToTable
 *   DeleteCDC-018: Edge-only DV metrics
 */

new WorkloadSuite("write_delete") {

  // ==========================================================================
  // DeleteSuiteBase tests (DB-*)
  // ==========================================================================

  // DB-001-NP: basic case - non-partitioned
  test("DB_001_NP_basic_case", "Delete all rows from non-partitioned table",
      "delete") { w =>
    val t = w.createTableOp("tbl",
      schema = Seq(Col("key", "INT"), Col("value", "INT")))
    w.insertOp(t, Seq(
      Map("key" -> 2, "value" -> 2),
      Map("key" -> 1, "value" -> 4),
      Map("key" -> 1, "value" -> 1),
      Map("key" -> 0, "value" -> 3)))
    w.sql("DELETE FROM tbl")
    w.read(t)
    w.read(t, version = 1, name = "before_delete")
    w.snapshot(t)
  }

  // DB-001-P: basic case - partitioned
  test("DB_001_P_basic_case_partitioned", "Delete all rows from partitioned table",
      "delete", "partitioned") { w =>
    val t = w.createTableOp("tbl",
      schema = Seq(Col("key", "INT"), Col("value", "INT")),
      partitionColumns = Seq("key"))
    w.insertOp(t, Seq(
      Map("key" -> 2, "value" -> 2),
      Map("key" -> 1, "value" -> 4),
      Map("key" -> 1, "value" -> 1),
      Map("key" -> 0, "value" -> 3)))
    w.sql("DELETE FROM tbl")
    w.read(t)
    w.read(t, version = 1, name = "before_delete")
    w.snapshot(t)
  }

  // DB-002-NP: sequential deletes - non-partitioned
  test("DB_002_NP_sequential_deletes", "4 sequential deletes on non-partitioned table",
      "delete") { w =>
    val t = w.createTableOp("tbl",
      schema = Seq(Col("key", "INT"), Col("value", "INT")))
    w.insertOp(t, Seq(
      Map("key" -> 2, "value" -> 2),
      Map("key" -> 1, "value" -> 4),
      Map("key" -> 1, "value" -> 1),
      Map("key" -> 0, "value" -> 3)))
    // First delete is no-op (no row has key=3 AND value=4)
    w.deleteOp(t, predicate = "value = 4 AND key = 3")
    w.deleteOp(t, predicate = "value = 4 AND key = 1")
    w.deleteOp(t, predicate = "value = 2 OR key = 1")
    w.deleteOp(t, predicate = "key = 0 OR value = 99")
    w.read(t)
    w.read(t, version = 1, name = "after_insert")
    w.snapshot(t)
  }

  // DB-002-P: sequential deletes - partitioned
  test("DB_002_P_sequential_deletes_partitioned",
      "4 sequential deletes on partitioned table",
      "delete", "partitioned") { w =>
    val t = w.createTableOp("tbl",
      schema = Seq(Col("key", "INT"), Col("value", "INT")),
      partitionColumns = Seq("key"))
    w.insertOp(t, Seq(
      Map("key" -> 2, "value" -> 2),
      Map("key" -> 1, "value" -> 4),
      Map("key" -> 1, "value" -> 1),
      Map("key" -> 0, "value" -> 3)))
    w.deleteOp(t, predicate = "value = 4 AND key = 3")
    w.deleteOp(t, predicate = "value = 4 AND key = 1")
    w.deleteOp(t, predicate = "value = 2 OR key = 1")
    w.deleteOp(t, predicate = "key = 0 OR value = 99")
    w.read(t)
    w.read(t, version = 1, name = "after_insert")
    w.snapshot(t)
  }

  // DB-003-NP: basic key columns - non-partitioned
  test("DB_003_NP_key_columns", "Sequential deletes by key column, non-partitioned",
      "delete") { w =>
    val t = w.createTableOp("tbl",
      schema = Seq(Col("key", "INT"), Col("value", "INT")))
    w.insertOp(t, Seq(
      Map("key" -> 2, "value" -> 2),
      Map("key" -> 1, "value" -> 4),
      Map("key" -> 1, "value" -> 1),
      Map("key" -> 0, "value" -> 3)))
    // First delete is no-op (no key > 2)
    w.deleteOp(t, predicate = "key > 2")
    w.deleteOp(t, predicate = "key < 2")
    w.deleteOp(t, predicate = "key = 2")
    w.read(t)
    w.read(t, version = 1, name = "after_insert")
    w.snapshot(t)
  }

  // DB-003-P: basic key columns - partitioned
  test("DB_003_P_key_columns_partitioned",
      "Sequential deletes by key column, partitioned",
      "delete", "partitioned") { w =>
    val t = w.createTableOp("tbl",
      schema = Seq(Col("key", "INT"), Col("value", "INT")),
      partitionColumns = Seq("key"))
    w.insertOp(t, Seq(
      Map("key" -> 2, "value" -> 2),
      Map("key" -> 1, "value" -> 4),
      Map("key" -> 1, "value" -> 1),
      Map("key" -> 0, "value" -> 3)))
    w.deleteOp(t, predicate = "key > 2")
    w.deleteOp(t, predicate = "key < 2")
    w.deleteOp(t, predicate = "key = 2")
    w.read(t)
    w.read(t, version = 1, name = "after_insert")
    w.snapshot(t)
  }

  // DB-004-NP: where key columns - non-partitioned
  test("DB_004_NP_where_key_columns", "Delete by exact key values, non-partitioned",
      "delete") { w =>
    val t = w.createTableOp("tbl",
      schema = Seq(Col("key", "INT"), Col("value", "INT")))
    w.insertOp(t, Seq(
      Map("key" -> 2, "value" -> 2),
      Map("key" -> 1, "value" -> 4),
      Map("key" -> 1, "value" -> 1),
      Map("key" -> 0, "value" -> 3)))
    w.deleteOp(t, predicate = "key = 1")
    w.deleteOp(t, predicate = "key = 2")
    w.deleteOp(t, predicate = "key = 0")
    w.read(t)
    w.read(t, version = 1, name = "after_insert")
    w.snapshot(t)
  }

  // DB-004-P: where key columns - partitioned
  test("DB_004_P_where_key_columns_partitioned",
      "Delete by exact key values, partitioned",
      "delete", "partitioned") { w =>
    val t = w.createTableOp("tbl",
      schema = Seq(Col("key", "INT"), Col("value", "INT")),
      partitionColumns = Seq("key"))
    w.insertOp(t, Seq(
      Map("key" -> 2, "value" -> 2),
      Map("key" -> 1, "value" -> 4),
      Map("key" -> 1, "value" -> 1),
      Map("key" -> 0, "value" -> 3)))
    w.deleteOp(t, predicate = "key = 1")
    w.deleteOp(t, predicate = "key = 2")
    w.deleteOp(t, predicate = "key = 0")
    w.read(t)
    w.read(t, version = 1, name = "after_insert")
    w.snapshot(t)
  }

  // DB-005-NP: where data columns - non-partitioned
  test("DB_005_NP_where_data_columns", "Delete by value column predicates, non-partitioned",
      "delete") { w =>
    val t = w.createTableOp("tbl",
      schema = Seq(Col("key", "INT"), Col("value", "INT")))
    w.insertOp(t, Seq(
      Map("key" -> 2, "value" -> 2),
      Map("key" -> 1, "value" -> 4),
      Map("key" -> 1, "value" -> 1),
      Map("key" -> 0, "value" -> 3)))
    w.deleteOp(t, predicate = "value <= 2")
    w.deleteOp(t, predicate = "value = 3")
    w.deleteOp(t, predicate = "value != 0")
    w.read(t)
    w.read(t, version = 1, name = "after_insert")
    w.snapshot(t)
  }

  // DB-005-P: where data columns - partitioned
  test("DB_005_P_where_data_columns_partitioned",
      "Delete by value column predicates, partitioned",
      "delete", "partitioned") { w =>
    val t = w.createTableOp("tbl",
      schema = Seq(Col("key", "INT"), Col("value", "INT")),
      partitionColumns = Seq("key"))
    w.insertOp(t, Seq(
      Map("key" -> 2, "value" -> 2),
      Map("key" -> 1, "value" -> 4),
      Map("key" -> 1, "value" -> 1),
      Map("key" -> 0, "value" -> 3)))
    w.deleteOp(t, predicate = "value <= 2")
    w.deleteOp(t, predicate = "value = 3")
    w.deleteOp(t, predicate = "value != 0")
    w.read(t)
    w.read(t, version = 1, name = "after_insert")
    w.snapshot(t)
  }

  // DB-006: where data columns and partition columns (partitioned only)
  test("DB_006_data_and_partition_columns",
      "Delete with compound predicates on data and partition columns",
      "delete", "partitioned") { w =>
    val t = w.createTableOp("tbl",
      schema = Seq(Col("key", "INT"), Col("value", "INT")),
      partitionColumns = Seq("key"))
    w.insertOp(t, Seq(
      Map("key" -> 2, "value" -> 2),
      Map("key" -> 1, "value" -> 4),
      Map("key" -> 1, "value" -> 1),
      Map("key" -> 0, "value" -> 3)))
    // First is no-op (no row with key=3 AND value=4)
    w.deleteOp(t, predicate = "value = 4 AND key = 3")
    w.deleteOp(t, predicate = "value = 4 AND key = 1")
    w.deleteOp(t, predicate = "value = 2 OR key = 1")
    w.deleteOp(t, predicate = "key = 0 OR value = 99")
    w.read(t)
    w.read(t, version = 1, name = "after_insert")
    w.snapshot(t)
  }

  // DB-007: data and partition columns with skipping variants
  // Skipping config affects scan performance, not logical result.
  // All 4 variants included for 1-to-1 mapping.

  // DB-007-NP-S
  test("DB_007_NP_S_data_partition_cols",
      "Data and partition columns, non-partitioned, skipping enabled",
      "delete") { w =>
    val t = w.createTableOp("tbl",
      schema = Seq(Col("key", "INT"), Col("value", "INT")))
    w.insertOp(t, Seq(
      Map("key" -> 2, "value" -> 2),
      Map("key" -> 1, "value" -> 4),
      Map("key" -> 1, "value" -> 1),
      Map("key" -> 0, "value" -> 3)))
    w.deleteOp(t, predicate = "value = 4 AND key = 3")
    w.deleteOp(t, predicate = "value = 4 AND key = 1")
    w.deleteOp(t, predicate = "value = 2 OR key = 1")
    w.deleteOp(t, predicate = "key = 0 OR value = 99")
    w.read(t)
    w.read(t, version = 1, name = "after_insert")
    w.snapshot(t)
  }

  // DB-007-NP-NS
  test("DB_007_NP_NS_data_partition_cols",
      "Data and partition columns, non-partitioned, skipping disabled",
      "delete") { w =>
    val t = w.createTableOp("tbl",
      schema = Seq(Col("key", "INT"), Col("value", "INT")))
    w.insertOp(t, Seq(
      Map("key" -> 2, "value" -> 2),
      Map("key" -> 1, "value" -> 4),
      Map("key" -> 1, "value" -> 1),
      Map("key" -> 0, "value" -> 3)))
    w.deleteOp(t, predicate = "value = 4 AND key = 3")
    w.deleteOp(t, predicate = "value = 4 AND key = 1")
    w.deleteOp(t, predicate = "value = 2 OR key = 1")
    w.deleteOp(t, predicate = "key = 0 OR value = 99")
    w.read(t)
    w.read(t, version = 1, name = "after_insert")
    w.snapshot(t)
  }

  // DB-007-P-S
  test("DB_007_P_S_data_partition_cols",
      "Data and partition columns, partitioned, skipping enabled",
      "delete", "partitioned") { w =>
    val t = w.createTableOp("tbl",
      schema = Seq(Col("key", "INT"), Col("value", "INT")),
      partitionColumns = Seq("key"))
    w.insertOp(t, Seq(
      Map("key" -> 2, "value" -> 2),
      Map("key" -> 1, "value" -> 4),
      Map("key" -> 1, "value" -> 1),
      Map("key" -> 0, "value" -> 3)))
    w.deleteOp(t, predicate = "value = 4 AND key = 3")
    w.deleteOp(t, predicate = "value = 4 AND key = 1")
    w.deleteOp(t, predicate = "value = 2 OR key = 1")
    w.deleteOp(t, predicate = "key = 0 OR value = 99")
    w.read(t)
    w.read(t, version = 1, name = "after_insert")
    w.snapshot(t)
  }

  // DB-007-P-NS
  test("DB_007_P_NS_data_partition_cols",
      "Data and partition columns, partitioned, skipping disabled",
      "delete", "partitioned") { w =>
    val t = w.createTableOp("tbl",
      schema = Seq(Col("key", "INT"), Col("value", "INT")),
      partitionColumns = Seq("key"))
    w.insertOp(t, Seq(
      Map("key" -> 2, "value" -> 2),
      Map("key" -> 1, "value" -> 4),
      Map("key" -> 1, "value" -> 1),
      Map("key" -> 0, "value" -> 3)))
    w.deleteOp(t, predicate = "value = 4 AND key = 3")
    w.deleteOp(t, predicate = "value = 4 AND key = 1")
    w.deleteOp(t, predicate = "value = 2 OR key = 1")
    w.deleteOp(t, predicate = "key = 0 OR value = 99")
    w.read(t)
    w.read(t, version = 1, name = "after_insert")
    w.snapshot(t)
  }

  // DB-014: delete cached table by name
  test("DB_014_delete_cached_table", "Delete from cached table (caching is orthogonal)",
      "delete") { w =>
    val t = w.createTableOp("tbl",
      schema = Seq(Col("key", "INT"), Col("value", "INT")))
    w.insertOp(t, Seq(
      Map("key" -> 2, "value" -> 2),
      Map("key" -> 1, "value" -> 4)))
    w.deleteOp(t, predicate = "key = 2")
    w.read(t)
    w.read(t, version = 1, name = "before_delete")
    w.snapshot(t)
  }

  // DB-015: delete cached table
  test("DB_015_delete_cached_table_2", "Delete from cached table variant",
      "delete") { w =>
    val t = w.createTableOp("tbl",
      schema = Seq(Col("key", "INT"), Col("value", "INT")))
    w.insertOp(t, Seq(
      Map("key" -> 2, "value" -> 2),
      Map("key" -> 1, "value" -> 4)))
    w.deleteOp(t, predicate = "key = 2")
    w.read(t)
    w.read(t, version = 1, name = "before_delete")
    w.snapshot(t)
  }

  // DB-019-NP: foldable condition - non-partitioned
  test("DB_019_NP_foldable_condition",
      "Delete with foldable conditions (false, 1<>1, 1>null, true, 1=1), non-partitioned",
      "delete") { w =>
    val t = w.createTableOp("tbl",
      schema = Seq(Col("key", "INT"), Col("value", "INT")))
    w.insertOp(t, Seq(
      Map("key" -> 2, "value" -> 2),
      Map("key" -> 1, "value" -> 4),
      Map("key" -> 1, "value" -> 1),
      Map("key" -> 0, "value" -> 3)))
    // false, 1<>1, 1>null are all no-ops; true and 1=1 delete all rows
    w.deleteOp(t, predicate = "false")
    w.deleteOp(t, predicate = "1 <> 1")
    w.deleteOp(t, predicate = "1 > null")
    w.deleteOp(t, predicate = "true")
    w.deleteOp(t, predicate = "1 = 1")
    w.read(t)
    w.read(t, version = 1, name = "after_insert")
    w.snapshot(t)
  }

  // DB-019-P: foldable condition - partitioned
  test("DB_019_P_foldable_condition_partitioned",
      "Delete with foldable conditions, partitioned",
      "delete", "partitioned") { w =>
    val t = w.createTableOp("tbl",
      schema = Seq(Col("key", "INT"), Col("value", "INT")),
      partitionColumns = Seq("key"))
    w.insertOp(t, Seq(
      Map("key" -> 2, "value" -> 2),
      Map("key" -> 1, "value" -> 4),
      Map("key" -> 1, "value" -> 1),
      Map("key" -> 0, "value" -> 3)))
    w.deleteOp(t, predicate = "false")
    w.deleteOp(t, predicate = "1 <> 1")
    w.deleteOp(t, predicate = "1 > null")
    w.deleteOp(t, predicate = "true")
    w.deleteOp(t, predicate = "1 = 1")
    w.read(t)
    w.read(t, version = 1, name = "after_insert")
    w.snapshot(t)
  }

  // DB-020: null evaluation - condition evaluates to null
  test("DB_020_null_evaluation",
      "Delete where condition evaluates to null should not delete rows",
      "delete") { w =>
    val t = w.createTableOp("tbl",
      schema = Seq(Col("key", "STRING"), Col("value", "STRING")))
    w.insertOp(t, Seq(
      Map("key" -> "a", "value" -> null),
      Map("key" -> "b", "value" -> null),
      Map("key" -> "c", "value" -> "v"),
      Map("key" -> "d", "value" -> "vv")))
    // "value = null" evaluates to null -> no rows deleted (no-op)
    w.deleteOp(t, predicate = "value = null")
    // "value = 'v'" -> deletes ('c', 'v')
    w.deleteOp(t, predicate = "value = 'v'")
    // "value <> 'v'" -> deletes ('d', 'vv') (nulls evaluate to null, not deleted)
    w.deleteOp(t, predicate = "value <> 'v'")
    w.read(t)
    w.read(t, version = 1, name = "after_insert")
    w.snapshot(t)
  }

  // DB-021: delete rows with null values using isNull
  test("DB_021_delete_with_isNull",
      "Delete rows with null values using IS NULL predicate",
      "delete") { w =>
    val t = w.createTableOp("tbl",
      schema = Seq(Col("key", "STRING"), Col("value", "STRING")))
    w.insertOp(t, Seq(
      Map("key" -> "a", "value" -> null),
      Map("key" -> "b", "value" -> null),
      Map("key" -> "c", "value" -> "v"),
      Map("key" -> "d", "value" -> "vv")))
    // "value IS NULL" -> deletes ('a', null) and ('b', null)
    w.deleteOp(t, predicate = "value IS NULL")
    w.read(t)
    w.read(t, version = 1, name = "before_delete")
    w.snapshot(t)
  }

  // DB-022: delete rows with null values using EqualNullSafe
  test("DB_022_delete_with_nullsafe_equal",
      "Delete rows with null values using null-safe equality (<=>)",
      "delete") { w =>
    val t = w.createTableOp("tbl",
      schema = Seq(Col("key", "STRING"), Col("value", "STRING")))
    w.insertOp(t, Seq(
      Map("key" -> "a", "value" -> null),
      Map("key" -> "b", "value" -> null),
      Map("key" -> "c", "value" -> "v"),
      Map("key" -> "d", "value" -> "vv")))
    // "value <=> null" (null-safe equality) -> deletes ('a', null) and ('b', null)
    w.deleteOp(t, predicate = "value <=> null")
    w.read(t)
    w.read(t, version = 1, name = "before_delete")
    w.snapshot(t)
  }

  // DB-026: delete on partitioned table with special chars
  test("DB_026_special_chars_partition",
      "Delete on partitioned table with special characters in partition values",
      "delete", "partitioned") { w =>
    val t = w.createTableOp("tbl",
      schema = Seq(Col("key", "BIGINT"), Col("value", "STRING")),
      partitionColumns = Seq("value"))
    w.insertOp(t, Seq(
      Map("key" -> 0, "value" -> "part%one"),
      Map("key" -> 1, "value" -> "part%one"),
      Map("key" -> 2, "value" -> "part%one")))
    w.deleteOp(t, predicate = "value = 'part%one' AND key = 1")
    w.deleteOp(t, predicate = "value = 'part%one' AND key = 2")
    w.deleteOp(t, predicate = "value = 'part%one'")
    w.read(t)
    w.read(t, version = 1, name = "after_insert")
    w.snapshot(t)
  }

  // ==========================================================================
  // DeleteDvSuite tests (DDV-*)
  // ==========================================================================

  // DDV-001: partial deletes produce deletion vectors
  // Data: range(0,50), delete even ids
  test("DDV_001_partial_delete_dv", "Partial delete produces deletion vectors",
      "delete", "dv") { w =>
    val t = w.createTableOp("tbl",
      schema = Seq(Col("id", "BIGINT")),
      properties = Map("delta.enableDeletionVectors" -> "true"))
    w.sql("INSERT INTO tbl SELECT id FROM RANGE(50)")
    w.deleteOp(t, predicate = "id % 2 = 0")
    w.read(t)
    w.read(t, version = 1, name = "before_delete")
    w.snapshot(t)
  }

  // DDV-002: repeated partial deletes merge deletion vectors
  // Data: range(0,50), delete even ids, then delete multiples of 3
  test("DDV_002_repeated_partial_delete_dv",
      "Repeated partial deletes merge deletion vectors",
      "delete", "dv") { w =>
    val t = w.createTableOp("tbl",
      schema = Seq(Col("id", "BIGINT")),
      properties = Map("delta.enableDeletionVectors" -> "true"))
    w.sql("INSERT INTO tbl SELECT id FROM RANGE(50)")
    w.deleteOp(t, predicate = "id % 2 = 0")
    w.deleteOp(t, predicate = "id % 3 = 0")
    w.read(t)
    w.read(t, version = 1, name = "after_insert")
    w.read(t, version = 2, name = "after_first_delete")
    w.snapshot(t)
  }

  // DDV-003: delete all rows in file without stats
  // Data: range(0,5) with no stats, delete (0,2,4) then (1,3)
  test("DDV_003_delete_all_rows_no_stats",
      "Delete all rows from file created without stats",
      "delete", "dv") { w =>
    // Create with stats disabled via SQL conf
    w.sql("SET spark.databricks.delta.stats.collect = false")
    val t = w.createTableOp("tbl",
      schema = Seq(Col("id", "BIGINT")),
      properties = Map("delta.enableDeletionVectors" -> "true"))
    w.sql("INSERT INTO tbl SELECT id FROM RANGE(5)")
    w.deleteOp(t, predicate = "id IN (0, 2, 4)")
    w.sql("SET spark.databricks.delta.stats.collect = true")
    // Second delete with stats enabled (default)
    w.deleteOp(t, predicate = "id IN (1, 3)")
    w.read(t)
    w.read(t, version = 1, name = "after_insert")
    w.snapshot(t)
  }

  // DDV-004: DV maxRowIndex not saved in log
  // Data: range(0,100), delete id IN (35, 88), then id = 93
  test("DDV_004_dv_maxRowIndex_not_saved",
      "DV maxRowIndex not saved in log after multi-file deletes",
      "delete", "dv") { w =>
    val t = w.createTableOp("tbl",
      schema = Seq(Col("id", "BIGINT")),
      properties = Map("delta.enableDeletionVectors" -> "true"))
    w.sql("INSERT INTO tbl SELECT id FROM RANGE(100)")
    w.deleteOp(t, predicate = "id IN (35, 88)")
    w.deleteOp(t, predicate = "id = 93")
    w.read(t)
    w.read(t, version = 1, name = "after_insert")
    w.read(t, version = 2, name = "after_first_delete")
    w.snapshot(t)
  }

  // DDV-005: persistent deletion vectors not cleaned up
  // Data: range(0,50), delete id IN (1, 4, 25)
  test("DDV_005_persistent_dv_not_cleaned",
      "Persistent deletion vectors remain after delete",
      "delete", "dv") { w =>
    val t = w.createTableOp("tbl",
      schema = Seq(Col("id", "BIGINT")),
      properties = Map("delta.enableDeletionVectors" -> "true"))
    w.sql("INSERT INTO tbl SELECT id FROM RANGE(50)")
    w.deleteOp(t, predicate = "id IN (1, 4, 25)")
    w.read(t)
    w.read(t, version = 1, name = "before_delete")
    w.snapshot(t)
  }

  // ==========================================================================
  // DeleteCDCSuite tests (DeleteCDC-*)
  // ==========================================================================

  // DeleteCDC-001: CDC - unconditional delete
  // Data: range(0,10), delete all
  test("DeleteCDC_001_unconditional", "CDC - unconditional delete of all rows",
      "delete", "cdc") { w =>
    val t = w.createTableOp("tbl",
      schema = Seq(Col("id", "BIGINT")),
      properties = Map("delta.enableChangeDataFeed" -> "true"))
    w.sql("INSERT INTO tbl SELECT id FROM RANGE(10)")
    w.sql("DELETE FROM tbl")
    w.read(t)
    w.read(t, version = 1, name = "before_delete")
    w.cdf(t, startVersion = 2)
    w.snapshot(t)
  }

  // DeleteCDC-002: CDC - conditional covering all rows
  // Data: range(0,10), delete WHERE id < 100 (covers all)
  test("DeleteCDC_002_conditional_all_rows",
      "CDC - conditional delete covering all rows",
      "delete", "cdc") { w =>
    val t = w.createTableOp("tbl",
      schema = Seq(Col("id", "BIGINT")),
      properties = Map("delta.enableChangeDataFeed" -> "true"))
    w.sql("INSERT INTO tbl SELECT id FROM RANGE(10)")
    w.deleteOp(t, predicate = "id < 100")
    w.read(t)
    w.read(t, version = 1, name = "before_delete")
    w.cdf(t, startVersion = 2)
    w.snapshot(t)
  }

  // DeleteCDC-003: CDC - two specific rows
  // Data: range(0,10), delete id = 2 OR id = 8
  test("DeleteCDC_003_two_rows", "CDC - delete two specific rows",
      "delete", "cdc") { w =>
    val t = w.createTableOp("tbl",
      schema = Seq(Col("id", "BIGINT")),
      properties = Map("delta.enableChangeDataFeed" -> "true"))
    w.sql("INSERT INTO tbl SELECT id FROM RANGE(10)")
    w.deleteOp(t, predicate = "id = 2 OR id = 8")
    w.read(t)
    w.read(t, version = 1, name = "before_delete")
    w.cdf(t, startVersion = 2)
    w.snapshot(t)
  }

  // DeleteCDC-004: CDC - unconditional delete on partitioned table
  // Data: CTAS with range(0,100) partitioned by id % 10
  test("DeleteCDC_004_unconditional_partitioned",
      "CDC - unconditional delete on partitioned table",
      "delete", "cdc", "partitioned") { w =>
    w.sql("""CREATE TABLE tbl USING delta
      TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
      PARTITIONED BY (part)
      AS SELECT CAST(id % 10 AS INT) AS part, id FROM RANGE(100)""")
    val t = w.table("tbl")
    w.sql("DELETE FROM tbl")
    w.read(t)
    w.read(t, version = 0, name = "before_delete")
    w.cdf(t, startVersion = 1)
    w.snapshot(t)
  }

  // DeleteCDC-005: CDC - delete all rows by condition on partitioned table
  // Data: same CTAS as above, delete WHERE id < 1000 (covers all)
  test("DeleteCDC_005_conditional_all_partitioned",
      "CDC - conditional delete covering all rows on partitioned table",
      "delete", "cdc", "partitioned") { w =>
    w.sql("""CREATE TABLE tbl USING delta
      TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
      PARTITIONED BY (part)
      AS SELECT CAST(id % 10 AS INT) AS part, id FROM RANGE(100)""")
    val t = w.table("tbl")
    w.deleteOp(t, predicate = "id < 1000")
    w.read(t)
    w.read(t, version = 0, name = "before_delete")
    w.cdf(t, startVersion = 1)
    w.snapshot(t)
  }

  // DeleteCDC-010: CDC - partition-optimized delete
  // Data: same CTAS partitioned table, delete entire partition (part = 3)
  test("DeleteCDC_010_partition_optimized",
      "CDC - partition-optimized delete of entire partition",
      "delete", "cdc", "partitioned") { w =>
    w.sql("""CREATE TABLE tbl USING delta
      TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
      PARTITIONED BY (part)
      AS SELECT CAST(id % 10 AS INT) AS part, id FROM RANGE(100)""")
    val t = w.table("tbl")
    w.deleteOp(t, predicate = "part = 3")
    w.read(t)
    w.read(t, version = 0, name = "before_delete")
    w.cdf(t, startVersion = 1)
    w.snapshot(t)
  }

  // DeleteCDC-014: CDC - delete from file with DV
  // Data: range(0,10), delete id IN (0, 3)
  test("DeleteCDC_014_dv_delete_with_cdc",
      "CDC - delete with deletion vectors enabled",
      "delete", "cdc", "dv") { w =>
    val t = w.createTableOp("tbl",
      schema = Seq(Col("id", "BIGINT")),
      properties = Map("delta.enableChangeDataFeed" -> "true"))
    w.sql("INSERT INTO tbl SELECT id FROM RANGE(10)")
    w.deleteOp(t, predicate = "id IN (0, 3)")
    w.read(t)
    w.read(t, version = 1, name = "before_delete")
    w.cdf(t, startVersion = 2)
    w.snapshot(t)
  }

}.runAll()
