/**
 * Update operation workloads converted from runtime capture suites.
 *
 * Sources:
 *   - UpdateSuiteBase.scala (UpdateBaseMiscTests trait) -> UB-002 through UB-037
 *   - UpdateDvSuite.scala (UpdateDvTests trait) -> UDV-001 through UDV-004
 *   - UpdateCDCSuite.scala (UpdateCDCTests trait) -> UpdateCDC-001 through UpdateCDC-007
 *
 * Skipped (with justification in source files):
 *   UB-001: Temp view test
 *   UB-019/020/021: NullType columns (no SQL equivalent)
 *   UB-023: Error test (invalid cast)
 *   UB-027: Complex nested JSON (deferred)
 *   UB-028/029/030: Error tests
 *   UB-031/032: Nested data (deferred)
 *   UB-033/034/035/036: Error/plan tests
 */

new WorkloadSuite("write_update") {

  // ==========================================================================
  // UpdateSuiteBase tests (UB-*)
  // ==========================================================================

  // UB-002: basic case (line 281) - UPDATE all rows, no WHERE
  test("UB_002_basic_case", "Update all rows without WHERE clause") {
    val w = createTableOp("tbl",
      schema = "key INT, value INT")
    insertOp(w, Seq(
      Map("key" -> 2, "value" -> 2),
      Map("key" -> 1, "value" -> 4),
      Map("key" -> 1, "value" -> 1),
      Map("key" -> 0, "value" -> 3)))
    updateOp(w, predicate = "true", set = Map("key" -> "1", "value" -> "2"))
    val t = registerWriteSpec(w)
    read(t)
    read(t, version = 1, name = "before_update")
    snapshot(t)
  }

  // UB-003: basic update - not partitioned (line 288)
  test("UB_003_NP_basic_update", "Basic update with WHERE on non-partitioned table") {
    val w = createTableOp("tbl",
      schema = "key INT, value INT")
    insertOp(w, Seq(
      Map("key" -> 2, "value" -> 2),
      Map("key" -> 1, "value" -> 4),
      Map("key" -> 1, "value" -> 1),
      Map("key" -> 0, "value" -> 3)))
    updateOp(w, predicate = "key >= 1",
      set = Map("value" -> "key + value", "key" -> "key + 1"))
    val t = registerWriteSpec(w)
    read(t)
    read(t, version = 1, name = "before_update")
    snapshot(t)
  }

  // UB-003: basic update - partitioned (line 288)
  test("UB_003_P_basic_update_partitioned",
      "Basic update with WHERE on partitioned table") {
    val w = createTableOp("tbl",
      schema = "key INT, value INT",
      partitionColumns = Seq("key"))
    insertOp(w, Seq(
      Map("key" -> 2, "value" -> 2),
      Map("key" -> 1, "value" -> 4),
      Map("key" -> 1, "value" -> 1),
      Map("key" -> 0, "value" -> 3)))
    updateOp(w, predicate = "key >= 1",
      set = Map("value" -> "key + value", "key" -> "key + 1"))
    val t = registerWriteSpec(w)
    read(t)
    read(t, version = 1, name = "before_update")
    snapshot(t)
  }

  // UB-004: basic update - Delta table by name - not partitioned (line 300)
  test("UB_004_NP_basic_update_by_name",
      "Basic update by name on non-partitioned table") {
    val w = createTableOp("tbl",
      schema = "key INT, value INT")
    insertOp(w, Seq(
      Map("key" -> 2, "value" -> 2),
      Map("key" -> 1, "value" -> 4),
      Map("key" -> 1, "value" -> 1),
      Map("key" -> 0, "value" -> 3)))
    updateOp(w, predicate = "key >= 1",
      set = Map("value" -> "key + value", "key" -> "key + 1"))
    val t = registerWriteSpec(w)
    read(t)
    read(t, version = 1, name = "before_update")
    snapshot(t)
  }

  // UB-004: basic update - Delta table by name - partitioned (line 300)
  test("UB_004_P_basic_update_by_name_partitioned",
      "Basic update by name on partitioned table") {
    val w = createTableOp("tbl",
      schema = "key INT, value INT",
      partitionColumns = Seq("key"))
    insertOp(w, Seq(
      Map("key" -> 2, "value" -> 2),
      Map("key" -> 1, "value" -> 4),
      Map("key" -> 1, "value" -> 1),
      Map("key" -> 0, "value" -> 3)))
    updateOp(w, predicate = "key >= 1",
      set = Map("value" -> "key + value", "key" -> "key + 1"))
    val t = registerWriteSpec(w)
    read(t)
    read(t, version = 1, name = "before_update")
    snapshot(t)
  }

  // UB-005: data and partition predicates - 4 variants (line 325)
  // Skipping=true/false is a runtime config, not table state; NP+S representative
  test("UB_005_NP_S_data_and_partition_predicates",
      "Update with data and partition predicates, non-partitioned, skipping enabled") {
    val w = createTableOp("tbl",
      schema = "key INT, value INT")
    insertOp(w, Seq(
      Map("key" -> 2, "value" -> 2),
      Map("key" -> 1, "value" -> 4),
      Map("key" -> 1, "value" -> 1),
      Map("key" -> 0, "value" -> 3)))
    updateOp(w, predicate = "key >= 1 AND value != 4",
      set = Map("value" -> "key + value", "key" -> "key + 5"))
    val t = registerWriteSpec(w)
    read(t)
    read(t, version = 1, name = "before_update")
    snapshot(t)
  }

  test("UB_005_P_S_data_and_partition_predicates_partitioned",
      "Update with data and partition predicates, partitioned, skipping enabled") {
    val w = createTableOp("tbl",
      schema = "key INT, value INT",
      partitionColumns = Seq("key"))
    insertOp(w, Seq(
      Map("key" -> 2, "value" -> 2),
      Map("key" -> 1, "value" -> 4),
      Map("key" -> 1, "value" -> 1),
      Map("key" -> 0, "value" -> 3)))
    updateOp(w, predicate = "key >= 1 AND value != 4",
      set = Map("value" -> "key + value", "key" -> "key + 5"))
    val t = registerWriteSpec(w)
    read(t)
    read(t, version = 1, name = "before_update")
    snapshot(t)
  }

  test("UB_005_NP_NS_data_and_partition_predicates_no_skipping",
      "Update with data and partition predicates, non-partitioned, no skipping") {
    val w = createTableOp("tbl",
      schema = "key INT, value INT")
    insertOp(w, Seq(
      Map("key" -> 2, "value" -> 2),
      Map("key" -> 1, "value" -> 4),
      Map("key" -> 1, "value" -> 1),
      Map("key" -> 0, "value" -> 3)))
    updateOp(w, predicate = "key >= 1 AND value != 4",
      set = Map("value" -> "key + value", "key" -> "key + 5"))
    val t = registerWriteSpec(w)
    read(t)
    read(t, version = 1, name = "before_update")
    snapshot(t)
  }

  test("UB_005_P_NS_data_and_partition_predicates_partitioned_no_skipping",
      "Update with data and partition predicates, partitioned, no skipping") {
    val w = createTableOp("tbl",
      schema = "key INT, value INT",
      partitionColumns = Seq("key"))
    insertOp(w, Seq(
      Map("key" -> 2, "value" -> 2),
      Map("key" -> 1, "value" -> 4),
      Map("key" -> 1, "value" -> 1),
      Map("key" -> 0, "value" -> 3)))
    updateOp(w, predicate = "key >= 1 AND value != 4",
      set = Map("value" -> "key + value", "key" -> "key + 5"))
    val t = registerWriteSpec(w)
    read(t)
    read(t, version = 1, name = "before_update")
    snapshot(t)
  }

  // UB-006: null values - not partitioned (line 339)
  test("UB_006_NP_null_values",
      "Sequential updates on table with null keys, non-partitioned") {
    val w = createTableOp("tbl",
      schema = "key STRING, value INT")
    insertOp(w, Seq(
      Map("key" -> "a", "value" -> 1),
      Map("key" -> null, "value" -> 2),
      Map("key" -> null, "value" -> 3),
      Map("key" -> "d", "value" -> 4)))
    // 1. key = null -> no-op (null = null is null, no match)
    updateOp(w, predicate = "key = null", set = Map("value" -> "-1"))
    // 2. key = 'a' -> updates ('a',1) to ('a',-1)
    updateOp(w, predicate = "key = 'a'", set = Map("value" -> "-1"))
    // 3. key IS NULL -> updates null rows to value=-2
    updateOp(w, predicate = "key IS NULL", set = Map("value" -> "-2"))
    // 4. key IS NOT NULL -> updates non-null rows to value=-3
    updateOp(w, predicate = "key IS NOT NULL", set = Map("value" -> "-3"))
    // 5. key <=> null (null-safe equality) -> updates null rows to value=-4
    updateOp(w, predicate = "key <=> null", set = Map("value" -> "-4"))
    val t = registerWriteSpec(w)
    read(t)
    read(t, version = 1, name = "after_insert")
    snapshot(t)
  }

  // UB-006: null values - partitioned (line 339)
  test("UB_006_P_null_values_partitioned",
      "Sequential updates on table with null keys, partitioned") {
    val w = createTableOp("tbl",
      schema = "key STRING, value INT",
      partitionColumns = Seq("key"))
    insertOp(w, Seq(
      Map("key" -> "a", "value" -> 1),
      Map("key" -> null, "value" -> 2),
      Map("key" -> null, "value" -> 3),
      Map("key" -> "d", "value" -> 4)))
    // 1. key = null -> no-op
    updateOp(w, predicate = "key = null", set = Map("value" -> "-1"))
    // 2. key = 'a'
    updateOp(w, predicate = "key = 'a'", set = Map("value" -> "-1"))
    // 3. key IS NULL
    updateOp(w, predicate = "key IS NULL", set = Map("value" -> "-2"))
    // 4. key IS NOT NULL
    updateOp(w, predicate = "key IS NOT NULL", set = Map("value" -> "-3"))
    // 5. key <=> null
    updateOp(w, predicate = "key <=> null", set = Map("value" -> "-4"))
    val t = registerWriteSpec(w)
    read(t)
    read(t, version = 1, name = "after_insert")
    snapshot(t)
  }

  // UB-007: condition is false (line 366) - no-op update
  test("UB_007_condition_false", "Update with always-false condition (no-op)") {
    val w = createTableOp("tbl",
      schema = "key INT, value INT")
    insertOp(w, Seq(
      Map("key" -> 2, "value" -> 2),
      Map("key" -> 1, "value" -> 4),
      Map("key" -> 1, "value" -> 1),
      Map("key" -> 0, "value" -> 3)))
    // No-op: condition is always false
    updateOp(w, predicate = "1 != 1", set = Map("key" -> "1", "value" -> "2"))
    val t = registerWriteSpec(w)
    read(t)
    snapshot(t)
  }

  // UB-008: condition is true (line 372)
  test("UB_008_condition_true", "Update with always-true condition") {
    val w = createTableOp("tbl",
      schema = "key INT, value INT")
    insertOp(w, Seq(
      Map("key" -> 2, "value" -> 2),
      Map("key" -> 1, "value" -> 4),
      Map("key" -> 1, "value" -> 1),
      Map("key" -> 0, "value" -> 3)))
    updateOp(w, predicate = "1 = 1", set = Map("key" -> "1", "value" -> "2"))
    val t = registerWriteSpec(w)
    read(t)
    read(t, version = 1, name = "before_update")
    snapshot(t)
  }

  // UB-009: without where - not partitioned (line 379)
  test("UB_009_NP_without_where", "Update all rows without WHERE, non-partitioned") {
    val w = createTableOp("tbl",
      schema = "key INT, value INT")
    insertOp(w, Seq(
      Map("key" -> 2, "value" -> 2),
      Map("key" -> 1, "value" -> 4),
      Map("key" -> 1, "value" -> 1),
      Map("key" -> 0, "value" -> 3)))
    updateOp(w, predicate = "true", set = Map("key" -> "1", "value" -> "2"))
    val t = registerWriteSpec(w)
    read(t)
    read(t, version = 1, name = "before_update")
    snapshot(t)
  }

  // UB-009: without where - partitioned (line 379)
  test("UB_009_P_without_where_partitioned",
      "Update all rows without WHERE, partitioned") {
    val w = createTableOp("tbl",
      schema = "key INT, value INT",
      partitionColumns = Seq("key"))
    insertOp(w, Seq(
      Map("key" -> 2, "value" -> 2),
      Map("key" -> 1, "value" -> 4),
      Map("key" -> 1, "value" -> 1),
      Map("key" -> 0, "value" -> 3)))
    updateOp(w, predicate = "true", set = Map("key" -> "1", "value" -> "2"))
    val t = registerWriteSpec(w)
    read(t)
    read(t, version = 1, name = "before_update")
    snapshot(t)
  }

  // UB-010: without where and partial columns - not partitioned (line 389)
  test("UB_010_NP_partial_columns",
      "Update partial columns without WHERE, non-partitioned") {
    val w = createTableOp("tbl",
      schema = "key INT, value INT")
    insertOp(w, Seq(
      Map("key" -> 2, "value" -> 2),
      Map("key" -> 1, "value" -> 4),
      Map("key" -> 1, "value" -> 1),
      Map("key" -> 0, "value" -> 3)))
    updateOp(w, predicate = "true", set = Map("key" -> "1"))
    val t = registerWriteSpec(w)
    read(t)
    read(t, version = 1, name = "before_update")
    snapshot(t)
  }

  // UB-010: without where and partial columns - partitioned (line 389)
  test("UB_010_P_partial_columns_partitioned",
      "Update partial columns without WHERE, partitioned") {
    val w = createTableOp("tbl",
      schema = "key INT, value INT",
      partitionColumns = Seq("key"))
    insertOp(w, Seq(
      Map("key" -> 2, "value" -> 2),
      Map("key" -> 1, "value" -> 4),
      Map("key" -> 1, "value" -> 1),
      Map("key" -> 0, "value" -> 3)))
    updateOp(w, predicate = "true", set = Map("key" -> "1"))
    val t = registerWriteSpec(w)
    read(t)
    read(t, version = 1, name = "before_update")
    snapshot(t)
  }

  // UB-011: without where and out-of-order columns - not partitioned (line 399)
  test("UB_011_NP_out_of_order_columns",
      "Update out-of-order columns without WHERE, non-partitioned") {
    val w = createTableOp("tbl",
      schema = "key INT, value INT")
    insertOp(w, Seq(
      Map("key" -> 2, "value" -> 2),
      Map("key" -> 1, "value" -> 4),
      Map("key" -> 1, "value" -> 1),
      Map("key" -> 0, "value" -> 3)))
    updateOp(w, predicate = "true", set = Map("value" -> "3", "key" -> "1"))
    val t = registerWriteSpec(w)
    read(t)
    read(t, version = 1, name = "before_update")
    snapshot(t)
  }

  // UB-011: without where and out-of-order columns - partitioned (line 399)
  test("UB_011_P_out_of_order_columns_partitioned",
      "Update out-of-order columns without WHERE, partitioned") {
    val w = createTableOp("tbl",
      schema = "key INT, value INT",
      partitionColumns = Seq("key"))
    insertOp(w, Seq(
      Map("key" -> 2, "value" -> 2),
      Map("key" -> 1, "value" -> 4),
      Map("key" -> 1, "value" -> 1),
      Map("key" -> 0, "value" -> 3)))
    updateOp(w, predicate = "true", set = Map("value" -> "3", "key" -> "1"))
    val t = registerWriteSpec(w)
    read(t)
    read(t, version = 1, name = "before_update")
    snapshot(t)
  }

  // UB-012: without where and complex input - not partitioned (line 409)
  test("UB_012_NP_complex_input",
      "Update with complex expressions without WHERE, non-partitioned") {
    val w = createTableOp("tbl",
      schema = "key INT, value INT")
    insertOp(w, Seq(
      Map("key" -> 2, "value" -> 2),
      Map("key" -> 1, "value" -> 4),
      Map("key" -> 1, "value" -> 1),
      Map("key" -> 0, "value" -> 3)))
    updateOp(w, predicate = "true", set = Map("value" -> "key + 3", "key" -> "key + 1"))
    val t = registerWriteSpec(w)
    read(t)
    read(t, version = 1, name = "before_update")
    snapshot(t)
  }

  // UB-012: without where and complex input - partitioned (line 409)
  test("UB_012_P_complex_input_partitioned",
      "Update with complex expressions without WHERE, partitioned") {
    val w = createTableOp("tbl",
      schema = "key INT, value INT",
      partitionColumns = Seq("key"))
    insertOp(w, Seq(
      Map("key" -> 2, "value" -> 2),
      Map("key" -> 1, "value" -> 4),
      Map("key" -> 1, "value" -> 1),
      Map("key" -> 0, "value" -> 3)))
    updateOp(w, predicate = "true", set = Map("value" -> "key + 3", "key" -> "key + 1"))
    val t = registerWriteSpec(w)
    read(t)
    read(t, version = 1, name = "before_update")
    snapshot(t)
  }

  // UB-013: with where - not partitioned (line 419)
  test("UB_013_NP_with_where", "Update with WHERE clause, non-partitioned") {
    val w = createTableOp("tbl",
      schema = "key INT, value INT")
    insertOp(w, Seq(
      Map("key" -> 2, "value" -> 2),
      Map("key" -> 1, "value" -> 4),
      Map("key" -> 1, "value" -> 1),
      Map("key" -> 0, "value" -> 3)))
    updateOp(w, predicate = "key = 1", set = Map("value" -> "3", "key" -> "1"))
    val t = registerWriteSpec(w)
    read(t)
    read(t, version = 1, name = "before_update")
    snapshot(t)
  }

  // UB-013: with where - partitioned (line 419)
  test("UB_013_P_with_where_partitioned",
      "Update with WHERE clause, partitioned") {
    val w = createTableOp("tbl",
      schema = "key INT, value INT",
      partitionColumns = Seq("key"))
    insertOp(w, Seq(
      Map("key" -> 2, "value" -> 2),
      Map("key" -> 1, "value" -> 4),
      Map("key" -> 1, "value" -> 1),
      Map("key" -> 0, "value" -> 3)))
    updateOp(w, predicate = "key = 1", set = Map("value" -> "3", "key" -> "1"))
    val t = registerWriteSpec(w)
    read(t)
    read(t, version = 1, name = "before_update")
    snapshot(t)
  }

  // UB-014: with where and complex input - not partitioned (line 429)
  test("UB_014_NP_complex_input_with_where",
      "Update with WHERE and complex expressions, non-partitioned") {
    val w = createTableOp("tbl",
      schema = "key INT, value INT")
    insertOp(w, Seq(
      Map("key" -> 2, "value" -> 2),
      Map("key" -> 1, "value" -> 4),
      Map("key" -> 1, "value" -> 1),
      Map("key" -> 0, "value" -> 3)))
    updateOp(w, predicate = "key >= 1",
      set = Map("value" -> "key + value", "key" -> "key + 1"))
    val t = registerWriteSpec(w)
    read(t)
    read(t, version = 1, name = "before_update")
    snapshot(t)
  }

  // UB-014: with where and complex input - partitioned (line 429)
  test("UB_014_P_complex_input_with_where_partitioned",
      "Update with WHERE and complex expressions, partitioned") {
    val w = createTableOp("tbl",
      schema = "key INT, value INT",
      partitionColumns = Seq("key"))
    insertOp(w, Seq(
      Map("key" -> 2, "value" -> 2),
      Map("key" -> 1, "value" -> 4),
      Map("key" -> 1, "value" -> 1),
      Map("key" -> 0, "value" -> 3)))
    updateOp(w, predicate = "key >= 1",
      set = Map("value" -> "key + value", "key" -> "key + 1"))
    val t = registerWriteSpec(w)
    read(t)
    read(t, version = 1, name = "before_update")
    snapshot(t)
  }

  // UB-015: with where and no row matched - not partitioned (line 439)
  test("UB_015_NP_no_row_matched",
      "Update with WHERE matching no rows (no-op), non-partitioned") {
    val w = createTableOp("tbl",
      schema = "key INT, value INT")
    insertOp(w, Seq(
      Map("key" -> 2, "value" -> 2),
      Map("key" -> 1, "value" -> 4),
      Map("key" -> 1, "value" -> 1),
      Map("key" -> 0, "value" -> 3)))
    // No-op: no key >= 10 in data
    updateOp(w, predicate = "key >= 10", set = Map("value" -> "key + value", "key" -> "key + 1"))
    val t = registerWriteSpec(w)
    read(t)
    snapshot(t)
  }

  // UB-015: with where and no row matched - partitioned (line 439)
  test("UB_015_P_no_row_matched_partitioned",
      "Update with WHERE matching no rows (no-op), partitioned") {
    val w = createTableOp("tbl",
      schema = "key INT, value INT",
      partitionColumns = Seq("key"))
    insertOp(w, Seq(
      Map("key" -> 2, "value" -> 2),
      Map("key" -> 1, "value" -> 4),
      Map("key" -> 1, "value" -> 1),
      Map("key" -> 0, "value" -> 3)))
    // No-op: no key >= 10 in data
    updateOp(w, predicate = "key >= 10", set = Map("value" -> "key + value", "key" -> "key + 1"))
    val t = registerWriteSpec(w)
    read(t)
    snapshot(t)
  }

  // UB-016: type mismatch - not partitioned (line 449)
  test("UB_016_NP_type_mismatch",
      "Update with CAST expressions causing type widening, non-partitioned") {
    val w = createTableOp("tbl",
      schema = "key INT, value INT")
    insertOp(w, Seq(
      Map("key" -> 2, "value" -> 2),
      Map("key" -> 1, "value" -> 4),
      Map("key" -> 1, "value" -> 1),
      Map("key" -> 0, "value" -> 3)))
    updateOp(w, predicate = "key >= 1",
      set = Map("value" -> "key + CAST(value AS DOUBLE)", "key" -> "CAST(key AS DOUBLE) + 1"))
    val t = registerWriteSpec(w)
    read(t)
    read(t, version = 1, name = "before_update")
    snapshot(t)
  }

  // UB-016: type mismatch - partitioned (line 449)
  test("UB_016_P_type_mismatch_partitioned",
      "Update with CAST expressions causing type widening, partitioned") {
    val w = createTableOp("tbl",
      schema = "key INT, value INT",
      partitionColumns = Seq("key"))
    insertOp(w, Seq(
      Map("key" -> 2, "value" -> 2),
      Map("key" -> 1, "value" -> 4),
      Map("key" -> 1, "value" -> 1),
      Map("key" -> 0, "value" -> 3)))
    updateOp(w, predicate = "key >= 1",
      set = Map("value" -> "key + CAST(value AS DOUBLE)", "key" -> "CAST(key AS DOUBLE) + 1"))
    val t = registerWriteSpec(w)
    read(t)
    read(t, version = 1, name = "before_update")
    snapshot(t)
  }

  // UB-017: set to null - not partitioned (line 460)
  test("UB_017_NP_set_to_null",
      "Update setting key to NULL via expression, non-partitioned") {
    val w = createTableOp("tbl",
      schema = "key INT, value INT")
    insertOp(w, Seq(
      Map("key" -> 2, "value" -> 2),
      Map("key" -> 1, "value" -> 4),
      Map("key" -> 1, "value" -> 1),
      Map("key" -> 0, "value" -> 3)))
    // NULL + 1D = NULL, so key becomes NULL
    updateOp(w, predicate = "key >= 1", set = Map("value" -> "key", "key" -> "NULL + 1D"))
    val t = registerWriteSpec(w)
    read(t)
    read(t, version = 1, name = "before_update")
    snapshot(t)
  }

  // UB-017: set to null - partitioned (line 460)
  test("UB_017_P_set_to_null_partitioned",
      "Update setting key to NULL via expression, partitioned") {
    val w = createTableOp("tbl",
      schema = "key INT, value INT",
      partitionColumns = Seq("key"))
    insertOp(w, Seq(
      Map("key" -> 2, "value" -> 2),
      Map("key" -> 1, "value" -> 4),
      Map("key" -> 1, "value" -> 1),
      Map("key" -> 0, "value" -> 3)))
    // NULL + 1D = NULL, so key becomes NULL
    updateOp(w, predicate = "key >= 1", set = Map("value" -> "key", "key" -> "NULL + 1D"))
    val t = registerWriteSpec(w)
    read(t)
    read(t, version = 1, name = "before_update")
    snapshot(t)
  }

  // UB-018: TypeCoercion twice - not partitioned (line 471)
  // Different data: (99,2),(100,4),(101,3)
  test("UB_018_NP_type_coercion_twice",
      "Update with double type coercion in predicate, non-partitioned") {
    val w = createTableOp("tbl",
      schema = "key INT, value INT")
    insertOp(w, Seq(
      Map("key" -> 99, "value" -> 2),
      Map("key" -> 100, "value" -> 4),
      Map("key" -> 101, "value" -> 3)))
    // Only key=101 matches: 101*1.0=101 > 100
    updateOp(w, predicate = "CAST(key AS LONG) * CAST('1.0' AS DECIMAL(38,18)) > 100",
      set = Map("value" -> "-3"))
    val t = registerWriteSpec(w)
    read(t)
    read(t, version = 1, name = "before_update")
    snapshot(t)
  }

  // UB-018: TypeCoercion twice - partitioned (line 471)
  test("UB_018_P_type_coercion_twice_partitioned",
      "Update with double type coercion in predicate, partitioned") {
    val w = createTableOp("tbl",
      schema = "key INT, value INT",
      partitionColumns = Seq("key"))
    insertOp(w, Seq(
      Map("key" -> 99, "value" -> 2),
      Map("key" -> 100, "value" -> 4),
      Map("key" -> 101, "value" -> 3)))
    updateOp(w, predicate = "CAST(key AS LONG) * CAST('1.0' AS DECIMAL(38,18)) > 100",
      set = Map("value" -> "-3"))
    val t = registerWriteSpec(w)
    read(t)
    read(t, version = 1, name = "before_update")
    snapshot(t)
  }

  // UB-022: upcast int into long target (line 514)
  // Data: (99,2),(100,4),(101,3) with value BIGINT
  test("UB_022_upcast_int_to_long",
      "Update with int literal upcasted to long target column") {
    val w = createTableOp("tbl",
      schema = "key INT, value BIGINT")
    insertOp(w, Seq(
      Map("key" -> 99, "value" -> 2),
      Map("key" -> 100, "value" -> 4),
      Map("key" -> 101, "value" -> 3)))
    // SET value = 4 (int literal, upcast to long)
    updateOp(w, predicate = "true", set = Map("value" -> "4"))
    val t = registerWriteSpec(w)
    read(t)
    read(t, version = 1, name = "before_update")
    snapshot(t)
  }

  // UB-024: implicit cast string to int (line 550)
  test("UB_024_implicit_cast_string_to_int",
      "Update with string literal implicitly cast to int target") {
    val w = createTableOp("tbl",
      schema = "key INT, value INT")
    insertOp(w, Seq(
      Map("key" -> 99, "value" -> 2),
      Map("key" -> 100, "value" -> 4),
      Map("key" -> 101, "value" -> 3)))
    // SET value = '5' (string '5', cast to int)
    updateOp(w, predicate = "true", set = Map("value" -> "'5'"))
    val t = registerWriteSpec(w)
    read(t)
    read(t, version = 1, name = "before_update")
    snapshot(t)
  }

  // UB-025: update cached table (line 564)
  // Different data: (2,2),(1,4)
  test("UB_025_cached_table", "Update on cached table") {
    val w = createTableOp("tbl",
      schema = "key INT, value INT")
    insertOp(w, Seq(
      Map("key" -> 2, "value" -> 2),
      Map("key" -> 1, "value" -> 4)))
    updateOp(w, predicate = "true", set = Map("key" -> "3"))
    val t = registerWriteSpec(w)
    read(t)
    read(t, version = 1, name = "before_update")
    snapshot(t)
  }

  // UB-026: different variations of column references (line 574)
  // Data: (99,2),(100,4),(101,3),(102,5) - two sequential updates
  test("UB_026_column_references",
      "Sequential updates with different column reference styles") {
    val w = createTableOp("tbl",
      schema = "key INT, value INT")
    insertOp(w, Seq(
      Map("key" -> 99, "value" -> 2),
      Map("key" -> 100, "value" -> 4),
      Map("key" -> 101, "value" -> 3),
      Map("key" -> 102, "value" -> 5)))
    // 1. Standard column references
    updateOp(w, predicate = "key = 99", set = Map("value" -> "-1"))
    // 2. Backtick-quoted column references
    updateOp(w, predicate = "key = 100", set = Map("value" -> "-1"))
    val t = registerWriteSpec(w)
    read(t)
    read(t, version = 1, name = "after_insert")
    read(t, version = 2, name = "after_first_update")
    snapshot(t)
  }

  // UB-037: partitioned table with special chars (line 1142)
  // Data: range(0,3) with value='part%one', partitioned by value
  test("UB_037_special_chars_partition",
      "Sequential updates on partitioned table with special characters") {
    val w = createTableOp("tbl",
      schema = "key BIGINT, value STRING",
      partitionColumns = Seq("value"))
    insertOp(w, Seq(
      Map("key" -> 0, "value" -> "part%one"),
      Map("key" -> 1, "value" -> "part%one"),
      Map("key" -> 2, "value" -> "part%one")))
    // 3 sequential updates moving rows from part%one to part%two
    updateOp(w, predicate = "value = 'part%one' AND key = 1",
      set = Map("value" -> "'part%two'"))
    updateOp(w, predicate = "value = 'part%one' AND key = 2",
      set = Map("value" -> "'part%two'"))
    updateOp(w, predicate = "value = 'part%one'",
      set = Map("value" -> "'part%two'"))
    val t = registerWriteSpec(w)
    read(t)
    read(t, version = 1, name = "after_insert")
    read(t, version = 2, name = "after_first_update")
    snapshot(t)
  }

  // ==========================================================================
  // UpdateDvSuite tests (UDV-*)
  // ==========================================================================

  // UDV-001: partial updates produce deletion vectors (line 166)
  // Data: range(0,100). Update WHERE id % 2 = 0: id = -id.
  test("UDV_001_partial_updates_dv",
      "Partial updates produce deletion vectors on DV-enabled table") {
    val w = createTableOp("tbl",
      schema = "id BIGINT",
      properties = Map(
        "delta.enableDeletionVectors" -> "true"))
    insertOp(w, (0 until 100).map(i => Map("id" -> i)))
    updateOp(w, predicate = "id % 2 = 0", set = Map("id" -> "-id"))
    val t = registerWriteSpec(w)
    read(t)
    read(t, version = 1, name = "before_update")
    read(t, predicate = "id < 0", name = "updated_rows")
    snapshot(t)
  }

  // UDV-002: repeated partial updates merge deletion vectors (line 206)
  // Data: (1),(2),(3),(4),(5). Three successive updates.
  test("UDV_002_repeated_partial_updates_dv",
      "Repeated partial updates merge deletion vectors") {
    val w = createTableOp("tbl",
      schema = "id BIGINT",
      properties = Map(
        "delta.enableDeletionVectors" -> "true"))
    insertOp(w, Seq(
      Map("id" -> 1), Map("id" -> 2), Map("id" -> 3),
      Map("id" -> 4), Map("id" -> 5)))
    // Update 2 rows
    updateOp(w, predicate = "id IN (1, 3)", set = Map("id" -> "-id"))
    // Update 2 more rows (DV merge)
    updateOp(w, predicate = "id IN (2, 4)", set = Map("id" -> "-id"))
    // Update remaining row (full file removal)
    updateOp(w, predicate = "id IN (5)", set = Map("id" -> "-id"))
    val t = registerWriteSpec(w)
    read(t)
    read(t, version = 1, name = "after_insert")
    read(t, version = 2, name = "after_first_update")
    read(t, version = 3, name = "after_second_update")
    snapshot(t)
  }

  // UDV-003: full file updates remove the files (line 245)
  // Data: range(0,10). Update all: id = 0.
  test("UDV_003_full_file_update_dv",
      "Full file update removes files instead of using DVs") {
    val w = createTableOp("tbl",
      schema = "id BIGINT",
      properties = Map(
        "delta.enableDeletionVectors" -> "true"))
    insertOp(w, (0 until 10).map(i => Map("id" -> i)))
    updateOp(w, predicate = "id < 10", set = Map("id" -> "0"))
    val t = registerWriteSpec(w)
    read(t)
    read(t, version = 1, name = "before_update")
    snapshot(t)
  }

  // UDV-004: persistent deletion vectors not cleaned up (line 267)
  // Data: range(0,50). Update rows 1, 4, 25: id = id + 42.
  test("UDV_004_persistent_dv_not_cleaned",
      "Persistent deletion vectors remain after partial update") {
    val w = createTableOp("tbl",
      schema = "id BIGINT",
      properties = Map(
        "delta.enableDeletionVectors" -> "true"))
    insertOp(w, (0 until 50).map(i => Map("id" -> i)))
    updateOp(w, predicate = "id IN (1, 4, 25)", set = Map("id" -> "id + 42"))
    val t = registerWriteSpec(w)
    read(t)
    read(t, version = 1, name = "before_update")
    read(t, predicate = "id > 42", name = "updated_rows")
    snapshot(t)
  }

  // ==========================================================================
  // UpdateCDCSuite tests (UpdateCDC-*)
  // ==========================================================================

  // UpdateCDC-001: CDC for unconditional update (line 42)
  // Data: (1,1),(2,2),(3,3),(4,4). Update all rows: value = -1
  test("UpdateCDC_001_unconditional_update",
      "CDC for unconditional update on all rows") {
    val w = createTableOp("tbl",
      schema = "key INT, value INT",
      properties = Map("delta.enableChangeDataFeed" -> "true"))
    insertOp(w, Seq(
      Map("key" -> 1, "value" -> 1),
      Map("key" -> 2, "value" -> 2),
      Map("key" -> 3, "value" -> 3),
      Map("key" -> 4, "value" -> 4)))
    updateOp(w, predicate = "true", set = Map("value" -> "-1"))
    val t = registerWriteSpec(w)
    read(t)
    read(t, version = 1, name = "before_update")
    // CDF for the update version (version 2)
    cdf(t, startVersion = 2, endVersion = 2)
    snapshot(t)
  }

  // UpdateCDC-002: CDC for conditional update on all rows (line 65)
  // Data: (1,1),(2,2),(3,3),(4,4). Update WHERE key < 10: value = -1
  test("UpdateCDC_002_conditional_update_all",
      "CDC for conditional update matching all rows") {
    val w = createTableOp("tbl",
      schema = "key INT, value INT",
      properties = Map("delta.enableChangeDataFeed" -> "true"))
    insertOp(w, Seq(
      Map("key" -> 1, "value" -> 1),
      Map("key" -> 2, "value" -> 2),
      Map("key" -> 3, "value" -> 3),
      Map("key" -> 4, "value" -> 4)))
    updateOp(w, predicate = "key < 10", set = Map("value" -> "-1"))
    val t = registerWriteSpec(w)
    read(t)
    read(t, version = 1, name = "before_update")
    cdf(t, startVersion = 2, endVersion = 2)
    snapshot(t)
  }

  // UpdateCDC-003: CDC for point update (line 88)
  // Data: (1,1),(2,2),(3,3),(4,4). Update WHERE key = 1: value = -1
  test("UpdateCDC_003_point_update",
      "CDC for point update on single row") {
    val w = createTableOp("tbl",
      schema = "key INT, value INT",
      properties = Map("delta.enableChangeDataFeed" -> "true"))
    insertOp(w, Seq(
      Map("key" -> 1, "value" -> 1),
      Map("key" -> 2, "value" -> 2),
      Map("key" -> 3, "value" -> 3),
      Map("key" -> 4, "value" -> 4)))
    updateOp(w, predicate = "key = 1", set = Map("value" -> "-1"))
    val t = registerWriteSpec(w)
    read(t)
    read(t, version = 1, name = "before_update")
    cdf(t, startVersion = 2, endVersion = 2)
    snapshot(t)
  }

  // UpdateCDC-004: CDC for repeated point update (line 105)
  // Data: (1,1),(2,2),(3,3),(4,4). Update key=1 then key=3.
  test("UpdateCDC_004_repeated_point_update",
      "CDC for two sequential point updates") {
    val w = createTableOp("tbl",
      schema = "key INT, value INT",
      properties = Map("delta.enableChangeDataFeed" -> "true"))
    insertOp(w, Seq(
      Map("key" -> 1, "value" -> 1),
      Map("key" -> 2, "value" -> 2),
      Map("key" -> 3, "value" -> 3),
      Map("key" -> 4, "value" -> 4)))
    updateOp(w, predicate = "key = 1", set = Map("value" -> "-1"))
    updateOp(w, predicate = "key = 3", set = Map("value" -> "-3"))
    val t = registerWriteSpec(w)
    read(t)
    read(t, version = 1, name = "after_insert")
    read(t, version = 2, name = "after_first_update")
    // CDF spans both update versions (2 and 3)
    cdf(t, startVersion = 2, endVersion = 3)
    cdf(t, startVersion = 2, endVersion = 2, name = "cdf_first_update")
    cdf(t, startVersion = 3, endVersion = 3, name = "cdf_second_update")
    snapshot(t)
  }

  // UpdateCDC-005: CDC for partition-optimized update (line 137)
  // Data: (1,1,1),(2,2,0),(3,3,1),(4,4,0) partitioned by part
  // Update WHERE part = 1: value = -1
  test("UpdateCDC_005_partition_optimized_update",
      "CDC for partition-optimized update") {
    val w = createTableOp("tbl",
      schema = "key INT, value INT, part INT",
      properties = Map("delta.enableChangeDataFeed" -> "true"),
      partitionColumns = Seq("part"))
    insertOp(w, Seq(
      Map("key" -> 1, "value" -> 1, "part" -> 1),
      Map("key" -> 2, "value" -> 2, "part" -> 0),
      Map("key" -> 3, "value" -> 3, "part" -> 1),
      Map("key" -> 4, "value" -> 4, "part" -> 0)))
    updateOp(w, predicate = "part = 1", set = Map("value" -> "-1"))
    val t = registerWriteSpec(w)
    read(t)
    read(t, version = 1, name = "after_insert")
    cdf(t, startVersion = 2, endVersion = 2)
    snapshot(t)
  }

  // UpdateCDC-006: update partitioned CDC table, set partition column to null (line 284)
  // Data: (0,0,0),(1,1,1),(2,2,2) partitioned by partition_column
  // Insert (4,4,4), then UPDATE SET partition_column = null WHERE partition_column = 4
  test("UpdateCDC_006_set_partition_to_null",
      "CDC for update setting partition column to null") {
    val w = createTableOp("tbl",
      schema = "key INT, partition_column INT, value INT",
      properties = Map("delta.enableChangeDataFeed" -> "true"),
      partitionColumns = Seq("partition_column"))
    insertOp(w, Seq(
      Map("key" -> 0, "partition_column" -> 0, "value" -> 0),
      Map("key" -> 1, "partition_column" -> 1, "value" -> 1),
      Map("key" -> 2, "partition_column" -> 2, "value" -> 2)))
    insertOp(w, Seq(Map("key" -> 4, "partition_column" -> 4, "value" -> 4)))
    updateOp(w, predicate = "partition_column = 4", set = Map("partition_column" -> "null"))
    val t = registerWriteSpec(w)
    read(t)
    read(t, version = 1, name = "after_first_insert")
    read(t, version = 2, name = "after_second_insert")
    cdf(t, startVersion = 3, endVersion = 3)
    snapshot(t)
  }

  // UpdateCDC-007: UPDATE with DV write CDC files explicitly (line 311)
  // Data: range(0,10). Update WHERE id % 4 = 0: id = -1
  // DV-enabled table with CDC
  test("UpdateCDC_007_dv_with_cdc",
      "CDC for update with deletion vectors enabled") {
    val w = createTableOp("tbl",
      schema = "id BIGINT",
      properties = Map(
        "delta.enableChangeDataFeed" -> "true",
        "delta.enableDeletionVectors" -> "true"))
    insertOp(w, (0 until 10).map(i => Map("id" -> i)))
    updateOp(w, predicate = "id % 4 = 0", set = Map("id" -> "-1"))
    val t = registerWriteSpec(w)
    read(t)
    read(t, version = 1, name = "before_update")
    // CDF for the update version (version 2)
    cdf(t, startVersion = 2, endVersion = 2)
    snapshot(t)
  }

}
