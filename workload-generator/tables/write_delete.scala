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
 *   DB-027: Variant type (requires parse_json UDF)
 *   DeleteCDC-006: Randomized file prefixes (internal feature)
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
      "delete") {
    val w = createTableOp("tbl",
      schema = "key INT, value INT")
    insertOp(w, Seq(
      Map("key" -> 2, "value" -> 2),
      Map("key" -> 1, "value" -> 4),
      Map("key" -> 1, "value" -> 1),
      Map("key" -> 0, "value" -> 3)))
    deleteOp(w, predicate = "true")
    val t = registerWriteSpec(w)
    read(t)
    read(t, version = 1, name = "before_delete")
    snapshot(t)
  }

  // DB-001-P: basic case - partitioned
  test("DB_001_P_basic_case_partitioned", "Delete all rows from partitioned table",
      "delete", "partitioned") {
    val w = createTableOp("tbl",
      schema = "key INT, value INT",
      partitionColumns = Seq("key"))
    insertOp(w, Seq(
      Map("key" -> 2, "value" -> 2),
      Map("key" -> 1, "value" -> 4),
      Map("key" -> 1, "value" -> 1),
      Map("key" -> 0, "value" -> 3)))
    deleteOp(w, predicate = "true")
    val t = registerWriteSpec(w)
    read(t)
    read(t, version = 1, name = "before_delete")
    snapshot(t)
  }

  // DB-002-NP: sequential deletes - non-partitioned
  test("DB_002_NP_sequential_deletes", "4 sequential deletes on non-partitioned table",
      "delete") {
    val w = createTableOp("tbl",
      schema = "key INT, value INT")
    insertOp(w, Seq(
      Map("key" -> 2, "value" -> 2),
      Map("key" -> 1, "value" -> 4),
      Map("key" -> 1, "value" -> 1),
      Map("key" -> 0, "value" -> 3)))
    // First delete is no-op (no row has key=3 AND value=4)
    deleteOp(w, predicate = "value = 4 AND key = 3")
    deleteOp(w, predicate = "value = 4 AND key = 1")
    deleteOp(w, predicate = "value = 2 OR key = 1")
    deleteOp(w, predicate = "key = 0 OR value = 99")
    val t = registerWriteSpec(w)
    read(t)
    read(t, version = 1, name = "after_insert")
    snapshot(t)
  }

  // DB-002-P: sequential deletes - partitioned
  test("DB_002_P_sequential_deletes_partitioned",
      "4 sequential deletes on partitioned table",
      "delete", "partitioned") {
    val w = createTableOp("tbl",
      schema = "key INT, value INT",
      partitionColumns = Seq("key"))
    insertOp(w, Seq(
      Map("key" -> 2, "value" -> 2),
      Map("key" -> 1, "value" -> 4),
      Map("key" -> 1, "value" -> 1),
      Map("key" -> 0, "value" -> 3)))
    deleteOp(w, predicate = "value = 4 AND key = 3")
    deleteOp(w, predicate = "value = 4 AND key = 1")
    deleteOp(w, predicate = "value = 2 OR key = 1")
    deleteOp(w, predicate = "key = 0 OR value = 99")
    val t = registerWriteSpec(w)
    read(t)
    read(t, version = 1, name = "after_insert")
    snapshot(t)
  }

  // DB-003-NP: basic key columns - non-partitioned
  test("DB_003_NP_key_columns", "Sequential deletes by key column, non-partitioned",
      "delete") {
    val w = createTableOp("tbl",
      schema = "key INT, value INT")
    insertOp(w, Seq(
      Map("key" -> 2, "value" -> 2),
      Map("key" -> 1, "value" -> 4),
      Map("key" -> 1, "value" -> 1),
      Map("key" -> 0, "value" -> 3)))
    // First delete is no-op (no key > 2)
    deleteOp(w, predicate = "key > 2")
    deleteOp(w, predicate = "key < 2")
    deleteOp(w, predicate = "key = 2")
    val t = registerWriteSpec(w)
    read(t)
    read(t, version = 1, name = "after_insert")
    snapshot(t)
  }

  // DB-003-P: basic key columns - partitioned
  test("DB_003_P_key_columns_partitioned",
      "Sequential deletes by key column, partitioned",
      "delete", "partitioned") {
    val w = createTableOp("tbl",
      schema = "key INT, value INT",
      partitionColumns = Seq("key"))
    insertOp(w, Seq(
      Map("key" -> 2, "value" -> 2),
      Map("key" -> 1, "value" -> 4),
      Map("key" -> 1, "value" -> 1),
      Map("key" -> 0, "value" -> 3)))
    deleteOp(w, predicate = "key > 2")
    deleteOp(w, predicate = "key < 2")
    deleteOp(w, predicate = "key = 2")
    val t = registerWriteSpec(w)
    read(t)
    read(t, version = 1, name = "after_insert")
    snapshot(t)
  }

  // DB-004-NP: where key columns - non-partitioned
  test("DB_004_NP_where_key_columns", "Delete by exact key values, non-partitioned",
      "delete") {
    val w = createTableOp("tbl",
      schema = "key INT, value INT")
    insertOp(w, Seq(
      Map("key" -> 2, "value" -> 2),
      Map("key" -> 1, "value" -> 4),
      Map("key" -> 1, "value" -> 1),
      Map("key" -> 0, "value" -> 3)))
    deleteOp(w, predicate = "key = 1")
    deleteOp(w, predicate = "key = 2")
    deleteOp(w, predicate = "key = 0")
    val t = registerWriteSpec(w)
    read(t)
    read(t, version = 1, name = "after_insert")
    snapshot(t)
  }

  // DB-004-P: where key columns - partitioned
  test("DB_004_P_where_key_columns_partitioned",
      "Delete by exact key values, partitioned",
      "delete", "partitioned") {
    val w = createTableOp("tbl",
      schema = "key INT, value INT",
      partitionColumns = Seq("key"))
    insertOp(w, Seq(
      Map("key" -> 2, "value" -> 2),
      Map("key" -> 1, "value" -> 4),
      Map("key" -> 1, "value" -> 1),
      Map("key" -> 0, "value" -> 3)))
    deleteOp(w, predicate = "key = 1")
    deleteOp(w, predicate = "key = 2")
    deleteOp(w, predicate = "key = 0")
    val t = registerWriteSpec(w)
    read(t)
    read(t, version = 1, name = "after_insert")
    snapshot(t)
  }

  // DB-005-NP: where data columns - non-partitioned
  test("DB_005_NP_where_data_columns", "Delete by value column predicates, non-partitioned",
      "delete") {
    val w = createTableOp("tbl",
      schema = "key INT, value INT")
    insertOp(w, Seq(
      Map("key" -> 2, "value" -> 2),
      Map("key" -> 1, "value" -> 4),
      Map("key" -> 1, "value" -> 1),
      Map("key" -> 0, "value" -> 3)))
    deleteOp(w, predicate = "value <= 2")
    deleteOp(w, predicate = "value = 3")
    deleteOp(w, predicate = "value != 0")
    val t = registerWriteSpec(w)
    read(t)
    read(t, version = 1, name = "after_insert")
    snapshot(t)
  }

  // DB-005-P: where data columns - partitioned
  test("DB_005_P_where_data_columns_partitioned",
      "Delete by value column predicates, partitioned",
      "delete", "partitioned") {
    val w = createTableOp("tbl",
      schema = "key INT, value INT",
      partitionColumns = Seq("key"))
    insertOp(w, Seq(
      Map("key" -> 2, "value" -> 2),
      Map("key" -> 1, "value" -> 4),
      Map("key" -> 1, "value" -> 1),
      Map("key" -> 0, "value" -> 3)))
    deleteOp(w, predicate = "value <= 2")
    deleteOp(w, predicate = "value = 3")
    deleteOp(w, predicate = "value != 0")
    val t = registerWriteSpec(w)
    read(t)
    read(t, version = 1, name = "after_insert")
    snapshot(t)
  }

  // DB-006: where data columns and partition columns (partitioned only)
  test("DB_006_data_and_partition_columns",
      "Delete with compound predicates on data and partition columns",
      "delete", "partitioned") {
    val w = createTableOp("tbl",
      schema = "key INT, value INT",
      partitionColumns = Seq("key"))
    insertOp(w, Seq(
      Map("key" -> 2, "value" -> 2),
      Map("key" -> 1, "value" -> 4),
      Map("key" -> 1, "value" -> 1),
      Map("key" -> 0, "value" -> 3)))
    // First is no-op (no row with key=3 AND value=4)
    deleteOp(w, predicate = "value = 4 AND key = 3")
    deleteOp(w, predicate = "value = 4 AND key = 1")
    deleteOp(w, predicate = "value = 2 OR key = 1")
    deleteOp(w, predicate = "key = 0 OR value = 99")
    val t = registerWriteSpec(w)
    read(t)
    read(t, version = 1, name = "after_insert")
    snapshot(t)
  }

  // DB-007: data and partition columns with skipping variants
  // Skipping config affects scan performance, not logical result.
  // All 4 variants included for 1-to-1 mapping.

  // DB-007-NP-S
  test("DB_007_NP_S_data_partition_cols",
      "Data and partition columns, non-partitioned, skipping enabled",
      "delete") {
    val w = createTableOp("tbl",
      schema = "key INT, value INT")
    insertOp(w, Seq(
      Map("key" -> 2, "value" -> 2),
      Map("key" -> 1, "value" -> 4),
      Map("key" -> 1, "value" -> 1),
      Map("key" -> 0, "value" -> 3)))
    deleteOp(w, predicate = "value = 4 AND key = 3")
    deleteOp(w, predicate = "value = 4 AND key = 1")
    deleteOp(w, predicate = "value = 2 OR key = 1")
    deleteOp(w, predicate = "key = 0 OR value = 99")
    val t = registerWriteSpec(w)
    read(t)
    read(t, version = 1, name = "after_insert")
    snapshot(t)
  }

  // DB-007-NP-NS
  test("DB_007_NP_NS_data_partition_cols",
      "Data and partition columns, non-partitioned, skipping disabled",
      "delete") {
    val w = createTableOp("tbl",
      schema = "key INT, value INT")
    insertOp(w, Seq(
      Map("key" -> 2, "value" -> 2),
      Map("key" -> 1, "value" -> 4),
      Map("key" -> 1, "value" -> 1),
      Map("key" -> 0, "value" -> 3)))
    deleteOp(w, predicate = "value = 4 AND key = 3")
    deleteOp(w, predicate = "value = 4 AND key = 1")
    deleteOp(w, predicate = "value = 2 OR key = 1")
    deleteOp(w, predicate = "key = 0 OR value = 99")
    val t = registerWriteSpec(w)
    read(t)
    read(t, version = 1, name = "after_insert")
    snapshot(t)
  }

  // DB-007-P-S
  test("DB_007_P_S_data_partition_cols",
      "Data and partition columns, partitioned, skipping enabled",
      "delete", "partitioned") {
    val w = createTableOp("tbl",
      schema = "key INT, value INT",
      partitionColumns = Seq("key"))
    insertOp(w, Seq(
      Map("key" -> 2, "value" -> 2),
      Map("key" -> 1, "value" -> 4),
      Map("key" -> 1, "value" -> 1),
      Map("key" -> 0, "value" -> 3)))
    deleteOp(w, predicate = "value = 4 AND key = 3")
    deleteOp(w, predicate = "value = 4 AND key = 1")
    deleteOp(w, predicate = "value = 2 OR key = 1")
    deleteOp(w, predicate = "key = 0 OR value = 99")
    val t = registerWriteSpec(w)
    read(t)
    read(t, version = 1, name = "after_insert")
    snapshot(t)
  }

  // DB-007-P-NS
  test("DB_007_P_NS_data_partition_cols",
      "Data and partition columns, partitioned, skipping disabled",
      "delete", "partitioned") {
    val w = createTableOp("tbl",
      schema = "key INT, value INT",
      partitionColumns = Seq("key"))
    insertOp(w, Seq(
      Map("key" -> 2, "value" -> 2),
      Map("key" -> 1, "value" -> 4),
      Map("key" -> 1, "value" -> 1),
      Map("key" -> 0, "value" -> 3)))
    deleteOp(w, predicate = "value = 4 AND key = 3")
    deleteOp(w, predicate = "value = 4 AND key = 1")
    deleteOp(w, predicate = "value = 2 OR key = 1")
    deleteOp(w, predicate = "key = 0 OR value = 99")
    val t = registerWriteSpec(w)
    read(t)
    read(t, version = 1, name = "after_insert")
    snapshot(t)
  }

  // DB-014: delete cached table by name
  test("DB_014_delete_cached_table", "Delete from cached table (caching is orthogonal)",
      "delete") {
    val w = createTableOp("tbl",
      schema = "key INT, value INT")
    insertOp(w, Seq(
      Map("key" -> 2, "value" -> 2),
      Map("key" -> 1, "value" -> 4)))
    deleteOp(w, predicate = "key = 2")
    val t = registerWriteSpec(w)
    read(t)
    read(t, version = 1, name = "before_delete")
    snapshot(t)
  }

  // DB-015: delete cached table
  test("DB_015_delete_cached_table_2", "Delete from cached table variant",
      "delete") {
    val w = createTableOp("tbl",
      schema = "key INT, value INT")
    insertOp(w, Seq(
      Map("key" -> 2, "value" -> 2),
      Map("key" -> 1, "value" -> 4)))
    deleteOp(w, predicate = "key = 2")
    val t = registerWriteSpec(w)
    read(t)
    read(t, version = 1, name = "before_delete")
    snapshot(t)
  }

  // DB-019-NP: foldable condition - non-partitioned
  test("DB_019_NP_foldable_condition",
      "Delete with foldable conditions (false, 1<>1, 1>null, true, 1=1), non-partitioned",
      "delete") {
    val w = createTableOp("tbl",
      schema = "key INT, value INT")
    insertOp(w, Seq(
      Map("key" -> 2, "value" -> 2),
      Map("key" -> 1, "value" -> 4),
      Map("key" -> 1, "value" -> 1),
      Map("key" -> 0, "value" -> 3)))
    // false, 1<>1, 1>null are all no-ops; true and 1=1 delete all rows
    deleteOp(w, predicate = "false")
    deleteOp(w, predicate = "1 <> 1")
    deleteOp(w, predicate = "1 > null")
    deleteOp(w, predicate = "true")
    deleteOp(w, predicate = "1 = 1")
    val t = registerWriteSpec(w)
    read(t)
    read(t, version = 1, name = "after_insert")
    snapshot(t)
  }

  // DB-019-P: foldable condition - partitioned
  test("DB_019_P_foldable_condition_partitioned",
      "Delete with foldable conditions, partitioned",
      "delete", "partitioned") {
    val w = createTableOp("tbl",
      schema = "key INT, value INT",
      partitionColumns = Seq("key"))
    insertOp(w, Seq(
      Map("key" -> 2, "value" -> 2),
      Map("key" -> 1, "value" -> 4),
      Map("key" -> 1, "value" -> 1),
      Map("key" -> 0, "value" -> 3)))
    deleteOp(w, predicate = "false")
    deleteOp(w, predicate = "1 <> 1")
    deleteOp(w, predicate = "1 > null")
    deleteOp(w, predicate = "true")
    deleteOp(w, predicate = "1 = 1")
    val t = registerWriteSpec(w)
    read(t)
    read(t, version = 1, name = "after_insert")
    snapshot(t)
  }

  // DB-020: null evaluation - condition evaluates to null
  test("DB_020_null_evaluation",
      "Delete where condition evaluates to null should not delete rows",
      "delete") {
    val w = createTableOp("tbl",
      schema = "key STRING, value STRING")
    insertOp(w, Seq(
      Map("key" -> "a", "value" -> null),
      Map("key" -> "b", "value" -> null),
      Map("key" -> "c", "value" -> "v"),
      Map("key" -> "d", "value" -> "vv")))
    // "value = null" evaluates to null -> no rows deleted (no-op)
    deleteOp(w, predicate = "value = null")
    // "value = 'v'" -> deletes ('c', 'v')
    deleteOp(w, predicate = "value = 'v'")
    // "value <> 'v'" -> deletes ('d', 'vv') (nulls evaluate to null, not deleted)
    deleteOp(w, predicate = "value <> 'v'")
    val t = registerWriteSpec(w)
    read(t)
    read(t, version = 1, name = "after_insert")
    snapshot(t)
  }

  // DB-021: delete rows with null values using isNull
  test("DB_021_delete_with_isNull",
      "Delete rows with null values using IS NULL predicate",
      "delete") {
    val w = createTableOp("tbl",
      schema = "key STRING, value STRING")
    insertOp(w, Seq(
      Map("key" -> "a", "value" -> null),
      Map("key" -> "b", "value" -> null),
      Map("key" -> "c", "value" -> "v"),
      Map("key" -> "d", "value" -> "vv")))
    // "value IS NULL" -> deletes ('a', null) and ('b', null)
    deleteOp(w, predicate = "value IS NULL")
    val t = registerWriteSpec(w)
    read(t)
    read(t, version = 1, name = "before_delete")
    snapshot(t)
  }

  // DB-022: delete rows with null values using EqualNullSafe
  test("DB_022_delete_with_nullsafe_equal",
      "Delete rows with null values using null-safe equality (<=>)",
      "delete") {
    val w = createTableOp("tbl",
      schema = "key STRING, value STRING")
    insertOp(w, Seq(
      Map("key" -> "a", "value" -> null),
      Map("key" -> "b", "value" -> null),
      Map("key" -> "c", "value" -> "v"),
      Map("key" -> "d", "value" -> "vv")))
    // "value <=> null" (null-safe equality) -> deletes ('a', null) and ('b', null)
    deleteOp(w, predicate = "value <=> null")
    val t = registerWriteSpec(w)
    read(t)
    read(t, version = 1, name = "before_delete")
    snapshot(t)
  }

  // DB-026: delete on partitioned table with special chars
  test("DB_026_special_chars_partition",
      "Delete on partitioned table with special characters in partition values",
      "delete", "partitioned") {
    val w = createTableOp("tbl",
      schema = "key BIGINT, value STRING",
      partitionColumns = Seq("value"))
    insertOp(w, Seq(
      Map("key" -> 0, "value" -> "part%one"),
      Map("key" -> 1, "value" -> "part%one"),
      Map("key" -> 2, "value" -> "part%one")))
    deleteOp(w, predicate = "value = 'part%one' AND key = 1")
    deleteOp(w, predicate = "value = 'part%one' AND key = 2")
    deleteOp(w, predicate = "value = 'part%one'")
    val t = registerWriteSpec(w)
    read(t)
    read(t, version = 1, name = "after_insert")
    snapshot(t)
  }

  // ==========================================================================
  // DeleteDvSuite tests (DDV-*)
  // ==========================================================================

  // DDV-001: partial deletes produce deletion vectors
  // Data: range(0,50), delete even ids
  test("DDV_001_partial_delete_dv", "Partial delete produces deletion vectors",
      "delete", "dv") {
    val w = createTableOp("tbl",
      schema = "id BIGINT",
      properties = Map("delta.enableDeletionVectors" -> "true"))
    insertOp(w, (0 until 50).map(i => Map("id" -> i)))
    deleteOp(w, predicate = "id % 2 = 0")
    val t = registerWriteSpec(w)
    read(t)
    read(t, version = 1, name = "before_delete")
    snapshot(t)
  }

  // DDV-002: repeated partial deletes merge deletion vectors
  // Data: range(0,50), delete even ids, then delete multiples of 3
  test("DDV_002_repeated_partial_delete_dv",
      "Repeated partial deletes merge deletion vectors",
      "delete", "dv") {
    val w = createTableOp("tbl",
      schema = "id BIGINT",
      properties = Map("delta.enableDeletionVectors" -> "true"))
    insertOp(w, (0 until 50).map(i => Map("id" -> i)))
    deleteOp(w, predicate = "id % 2 = 0")
    deleteOp(w, predicate = "id % 3 = 0")
    val t = registerWriteSpec(w)
    read(t)
    read(t, version = 1, name = "after_insert")
    read(t, version = 2, name = "after_first_delete")
    snapshot(t)
  }

  // DDV-003: delete all rows in file without stats
  // Data: range(0,5) with no stats, delete (0,2,4) then (1,3)
  test("DDV_003_delete_all_rows_no_stats",
      "Delete all rows from file created without stats",
      "delete", "dv") {
    // Create with stats disabled via SQL conf
    sql("SET spark.delta.stats.collect = false")
    val w = createTableOp("tbl",
      schema = "id BIGINT",
      properties = Map("delta.enableDeletionVectors" -> "true"))
    insertOp(w, (0 until 5).map(i => Map("id" -> i)))
    deleteOp(w, predicate = "id IN (0, 2, 4)")
    sql("SET spark.delta.stats.collect = true")
    // Second delete with stats enabled (default)
    deleteOp(w, predicate = "id IN (1, 3)")
    val t = registerWriteSpec(w)
    read(t)
    read(t, version = 1, name = "after_insert")
    snapshot(t)
  }

  // DDV-004: DV maxRowIndex not saved in log
  // Data: range(0,100), delete id IN (35, 88), then id = 93
  test("DDV_004_dv_maxRowIndex_not_saved",
      "DV maxRowIndex not saved in log after multi-file deletes",
      "delete", "dv") {
    val w = createTableOp("tbl",
      schema = "id BIGINT",
      properties = Map("delta.enableDeletionVectors" -> "true"))
    insertOp(w, (0 until 100).map(i => Map("id" -> i)))
    deleteOp(w, predicate = "id IN (35, 88)")
    deleteOp(w, predicate = "id = 93")
    val t = registerWriteSpec(w)
    read(t)
    read(t, version = 1, name = "after_insert")
    read(t, version = 2, name = "after_first_delete")
    snapshot(t)
  }

  // DDV-005: persistent deletion vectors not cleaned up
  // Data: range(0,50), delete id IN (1, 4, 25)
  test("DDV_005_persistent_dv_not_cleaned",
      "Persistent deletion vectors remain after delete",
      "delete", "dv") {
    val w = createTableOp("tbl",
      schema = "id BIGINT",
      properties = Map("delta.enableDeletionVectors" -> "true"))
    insertOp(w, (0 until 50).map(i => Map("id" -> i)))
    deleteOp(w, predicate = "id IN (1, 4, 25)")
    val t = registerWriteSpec(w)
    read(t)
    read(t, version = 1, name = "before_delete")
    snapshot(t)
  }

  // ==========================================================================
  // DeleteCDCSuite tests (DeleteCDC-*)
  // ==========================================================================

  // DeleteCDC-001: CDC - unconditional delete
  // Data: range(0,10), delete all
  test("DeleteCDC_001_unconditional", "CDC - unconditional delete of all rows",
      "delete", "cdc") {
    val w = createTableOp("tbl",
      schema = "id BIGINT",
      properties = Map("delta.enableChangeDataFeed" -> "true"))
    insertOp(w, (0 until 10).map(i => Map("id" -> i)))
    deleteOp(w, predicate = "true")
    val t = registerWriteSpec(w)
    read(t)
    read(t, version = 1, name = "before_delete")
    cdf(t, startVersion = 2)
    snapshot(t)
  }

  // DeleteCDC-002: CDC - conditional covering all rows
  // Data: range(0,10), delete WHERE id < 100 (covers all)
  test("DeleteCDC_002_conditional_all_rows",
      "CDC - conditional delete covering all rows",
      "delete", "cdc") {
    val w = createTableOp("tbl",
      schema = "id BIGINT",
      properties = Map("delta.enableChangeDataFeed" -> "true"))
    insertOp(w, (0 until 10).map(i => Map("id" -> i)))
    deleteOp(w, predicate = "id < 100")
    val t = registerWriteSpec(w)
    read(t)
    read(t, version = 1, name = "before_delete")
    cdf(t, startVersion = 2)
    snapshot(t)
  }

  // DeleteCDC-003: CDC - two specific rows
  // Data: range(0,10), delete id = 2 OR id = 8
  test("DeleteCDC_003_two_rows", "CDC - delete two specific rows",
      "delete", "cdc") {
    val w = createTableOp("tbl",
      schema = "id BIGINT",
      properties = Map("delta.enableChangeDataFeed" -> "true"))
    insertOp(w, (0 until 10).map(i => Map("id" -> i)))
    deleteOp(w, predicate = "id = 2 OR id = 8")
    val t = registerWriteSpec(w)
    read(t)
    read(t, version = 1, name = "before_delete")
    cdf(t, startVersion = 2)
    snapshot(t)
  }

  // DeleteCDC-004: CDC - unconditional delete on partitioned table
  test("DeleteCDC_004_unconditional_partitioned",
      "CDC - unconditional delete on partitioned table",
      "delete", "cdc", "partitioned") {
    val w = createTableOp("tbl",
      schema = "part INT, id BIGINT",
      properties = Map("delta.enableChangeDataFeed" -> "true"),
      partitionColumns = Seq("part"))
    insertOp(w, (0 until 100).map(i => Map("part" -> (i % 10), "id" -> i)))
    deleteOp(w, predicate = "true")
    val t = registerWriteSpec(w)
    read(t)
    read(t, version = 1, name = "before_delete")
    cdf(t, startVersion = 2)
    snapshot(t)
  }

  // DeleteCDC-005: CDC - delete all rows by condition on partitioned table
  test("DeleteCDC_005_conditional_all_partitioned",
      "CDC - conditional delete covering all rows on partitioned table",
      "delete", "cdc", "partitioned") {
    val w = createTableOp("tbl",
      schema = "part INT, id BIGINT",
      properties = Map("delta.enableChangeDataFeed" -> "true"),
      partitionColumns = Seq("part"))
    insertOp(w, (0 until 100).map(i => Map("part" -> (i % 10), "id" -> i)))
    deleteOp(w, predicate = "id < 1000")
    val t = registerWriteSpec(w)
    read(t)
    read(t, version = 1, name = "before_delete")
    cdf(t, startVersion = 2)
    snapshot(t)
  }

  // DeleteCDC-010: CDC - partition-optimized delete
  test("DeleteCDC_010_partition_optimized",
      "CDC - partition-optimized delete of entire partition",
      "delete", "cdc", "partitioned") {
    val w = createTableOp("tbl",
      schema = "part INT, id BIGINT",
      properties = Map("delta.enableChangeDataFeed" -> "true"),
      partitionColumns = Seq("part"))
    insertOp(w, (0 until 100).map(i => Map("part" -> (i % 10), "id" -> i)))
    deleteOp(w, predicate = "part = 3")
    val t = registerWriteSpec(w)
    read(t)
    read(t, version = 1, name = "before_delete")
    cdf(t, startVersion = 2)
    snapshot(t)
  }

  // DeleteCDC-014: CDC - delete from file with DV
  // Data: range(0,10), delete id IN (0, 3)
  test("DeleteCDC_014_dv_delete_with_cdc",
      "CDC - delete with deletion vectors enabled",
      "delete", "cdc", "dv") {
    val w = createTableOp("tbl",
      schema = "id BIGINT",
      properties = Map("delta.enableChangeDataFeed" -> "true"))
    insertOp(w, (0 until 10).map(i => Map("id" -> i)))
    deleteOp(w, predicate = "id IN (0, 3)")
    val t = registerWriteSpec(w)
    read(t)
    read(t, version = 1, name = "before_delete")
    cdf(t, startVersion = 2)
    snapshot(t)
  }

}
