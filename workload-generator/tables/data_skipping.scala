/**
 * Data skipping, statistics, and partitioning workloads.
 * Covers: equality, range, IN, IS NULL, BETWEEN, LIKE, NOT, AND/OR combinations,
 * nested field predicates, boolean predicates, typed stats, null handling,
 * multiple files for skipping, missing stats, long strings, column mapping stats,
 * generated columns, DVs, partitioned skipping, partition pruning, projection,
 * stats edge cases (null min/max, numRecords-only, truncated strings).
 */

new WorkloadSuite("data_skipping") {

  // === Data Skipping ===

  // Top-level single value: all comparison operators

  test("ds_top_level_single_1", "top level, single 1", "dataSkipping") { w =>
    w.sql("CREATE TABLE tbl (a LONG) USING delta")
    w.sql("INSERT INTO tbl VALUES (1), (2)")
    val t = w.table("tbl")
    // hits
    w.read(t, predicate = "a = 1")
    w.read(t, predicate = "a >= 1")
    w.read(t, predicate = "a <= 1")
    w.read(t, predicate = "a >= 0")
    w.read(t, predicate = "a <= 2")
    w.read(t, predicate = "0 <= a")
    w.read(t, predicate = "1 <= a")
    w.read(t, predicate = "1 >= a")
    w.read(t, predicate = "2 >= a")
    w.read(t, predicate = "1 = a")
    w.read(t, predicate = "a <=> 1")
    w.read(t, predicate = "1 <=> a")
    w.read(t, predicate = "NOT (a <=> 2)", name = "read_not_a_nse_2")
    w.read(t, predicate = "true", name = "read_true")
    // misses
    w.read(t, predicate = "NOT (a = 1)", name = "read_miss_not_a_eq_1")
    w.read(t, predicate = "NOT (a <=> 1)", name = "read_miss_not_a_nse_1")
    w.read(t, predicate = "a = 2", name = "read_miss_a_eq_2")
    w.read(t, predicate = "a <=> 2", name = "read_miss_a_nse_2")
    w.read(t, predicate = "a > 1", name = "read_miss_a_gt_1")
    w.read(t, predicate = "a >= 2", name = "read_miss_a_gte_2")
    w.read(t, predicate = "a <= 0", name = "read_miss_a_lte_0")
    w.read(t, predicate = "a = 0", name = "read_miss_a_eq_0")
    w.read(t, predicate = "a > 2", name = "read_miss_a_gt_2")
    w.read(t, predicate = "a < 1", name = "read_miss_a_lt_1")
    w.read(t, predicate = "a <> 1", name = "read_miss_a_neq_1")
    w.read(t, predicate = "1 != a", name = "read_miss_1_neq_a")
    w.read(t, predicate = "2 <=> a", name = "read_miss_2_nse_a")
    w.read(t, predicate = "0 >= a", name = "read_miss_0_gte_a")
    w.read(t, predicate = "0 = a", name = "read_miss_0_eq_a")
    w.read(t, predicate = "1 > a", name = "read_miss_1_gt_a")
    w.read(t, predicate = "1 < a", name = "read_miss_1_lt_a")
    w.read(t, predicate = "0 > a", name = "read_miss_0_gt_a")
    w.read(t, predicate = "2 = a", name = "read_miss_2_eq_a")
    w.read(t, predicate = "2 <= a", name = "read_miss_2_lte_a")
    w.read(t, predicate = "0 < a AND a < 1", name = "read_miss_between_0_1")
    w.snapshot(t)
  }

  // Nested field predicates

  test("ds_nested_single_1", "nested, single 1", "dataSkipping") { w =>
    w.sql("CREATE TABLE tbl (a STRUCT<b: LONG>) USING delta")
    w.sql("INSERT INTO tbl VALUES (named_struct('b', 1))")
    val t = w.table("tbl")
    w.read(t, predicate = "a.b = 1")
    w.read(t, predicate = "a.b >= 0")
    w.read(t, predicate = "a.b >= 1")
    w.read(t, predicate = "a.b <= 1")
    w.read(t, predicate = "a.b <= 2")
    w.read(t, predicate = "a.b = 2", name = "read_miss_ab_eq_2")
    w.read(t, predicate = "a.b > 1", name = "read_miss_ab_gt_1")
    w.read(t, predicate = "a.b < 1", name = "read_miss_ab_lt_1")
    w.snapshot(t)
  }

  test("ds_double_nested_single_1", "double nested, single 1", "dataSkipping") { w =>
    w.sql("CREATE TABLE tbl (a STRUCT<b: STRUCT<c: LONG>>) USING delta")
    w.sql("INSERT INTO tbl VALUES (named_struct('b', named_struct('c', 1)))")
    val t = w.table("tbl")
    w.read(t, predicate = "a.b.c = 1")
    w.read(t, predicate = "a.b.c >= 0")
    w.read(t, predicate = "a.b.c >= 1")
    w.read(t, predicate = "a.b.c <= 1")
    w.read(t, predicate = "a.b.c <= 2")
    w.read(t, predicate = "a.b.c = 2", name = "read_miss_abc_eq_2")
    w.read(t, predicate = "a.b.c > 1", name = "read_miss_abc_gt_1")
    w.read(t, predicate = "a.b.c < 1", name = "read_miss_abc_lt_1")
    w.snapshot(t)
  }

  test("ds_nested_struct_predicate", "Nested struct field predicate", "dataSkipping") { w =>
    w.sql("CREATE TABLE tbl (id INT, info STRUCT<score: INT, name: STRING>) USING delta")
    w.sql("INSERT INTO tbl VALUES (1, named_struct('score', 90, 'name', 'alice'))")
    w.sql("INSERT INTO tbl VALUES (2, named_struct('score', 50, 'name', 'bob'))")
    val t = w.table("tbl")
    w.read(t, predicate = "info.score > 80")
    w.read(t, predicate = "info.score < 40", name = "read_miss_low_score")
    w.snapshot(t)
  }

  test("ds_complex_nested", "Complex nested predicates", "dataSkipping") { w =>
    w.sql("CREATE TABLE tbl (a INT, b STRUCT<x: INT, y: INT>) USING delta")
    w.sql("INSERT INTO tbl VALUES (1, named_struct('x', 10, 'y', 20))")
    w.sql("INSERT INTO tbl VALUES (2, named_struct('x', 30, 'y', 40))")
    val t = w.table("tbl")
    w.read(t, predicate = "a = 1 AND b.x = 10")
    w.read(t, predicate = "b.x > 20 OR b.y < 25")
    w.read(t, predicate = "a > 5 AND b.x > 50", name = "read_miss_complex")
    w.snapshot(t)
  }

  // AND / OR / NOT combinations

  test("ds_and_simple", "and statements - simple", "dataSkipping") { w =>
    w.sql("CREATE TABLE tbl (a LONG) USING delta")
    w.sql("INSERT INTO tbl VALUES (1), (2)")
    val t = w.table("tbl")
    w.read(t, predicate = "a <= 1 AND a > -1", name = "read_hit_and_bound")
    w.read(t, predicate = "a >= 1 AND a <= 2", name = "read_hit_and_range")
    w.read(t, predicate = "a > 5 AND a < 10", name = "read_miss_and_outside")
    w.snapshot(t)
  }

  test("ds_and_two_fields", "and statements - two fields", "dataSkipping") { w =>
    w.sql("CREATE TABLE tbl (a LONG, b LONG) USING delta")
    w.sql("INSERT INTO tbl VALUES (1, 10), (2, 20)")
    val t = w.table("tbl")
    w.read(t, predicate = "a = 1 AND b = 10")
    w.read(t, predicate = "a >= 1 AND b <= 20")
    w.read(t, predicate = "a = 1 AND b > 100", name = "read_miss_b_out")
    w.read(t, predicate = "a > 5 AND b > 5", name = "read_miss_both_out")
    w.snapshot(t)
  }

  test("ds_and_one_side_unsupported", "AND one side unsupported", "dataSkipping") { w =>
    w.sql("CREATE TABLE tbl (a LONG) USING delta")
    w.sql("INSERT INTO tbl VALUES (1), (2)")
    val t = w.table("tbl")
    w.read(t, predicate = "a = 1 AND CAST(a AS STRING) LIKE '%1'")
    w.read(t, predicate = "a > 5 AND CAST(a AS STRING) LIKE '%x'", name = "read_miss_and_unsupported")
    w.snapshot(t)
  }

  test("ds_or_simple", "or statements - simple", "dataSkipping") { w =>
    w.sql("CREATE TABLE tbl (a LONG) USING delta")
    w.sql("INSERT INTO tbl VALUES (1), (2)")
    val t = w.table("tbl")
    w.read(t, predicate = "a = 1 OR a = 3")
    w.read(t, predicate = "a < 0 OR a > 0")
    w.read(t, predicate = "a = 5 OR a = 6", name = "read_miss_or")
    w.snapshot(t)
  }

  test("ds_or_two_fields", "or statements - two fields", "dataSkipping") { w =>
    w.sql("CREATE TABLE tbl (a LONG, b LONG) USING delta")
    w.sql("INSERT INTO tbl VALUES (1, 10), (2, 20)")
    val t = w.table("tbl")
    w.read(t, predicate = "a = 1 OR b = 20")
    w.read(t, predicate = "a = 5 OR b = 10")
    w.read(t, predicate = "a > 0 OR b > 0")
    w.read(t, predicate = "a = 5 OR b = 50", name = "read_miss_or_both")
    w.snapshot(t)
  }

  test("ds_or_one_side_unsupported", "OR one side unsupported", "dataSkipping") { w =>
    w.sql("CREATE TABLE tbl (a LONG) USING delta")
    w.sql("INSERT INTO tbl VALUES (1), (2)")
    val t = w.table("tbl")
    // OR with unsupported side forces full scan
    w.read(t, predicate = "a = 1 OR CAST(a AS STRING) LIKE '%x'")
    w.read(t, predicate = "a > 5 OR CAST(a AS STRING) LIKE '%1'")
    w.snapshot(t)
  }

  test("ds_not_simple", "not statements - simple", "dataSkipping") { w =>
    w.sql("CREATE TABLE tbl (a LONG) USING delta")
    w.sql("INSERT INTO tbl VALUES (1), (2)")
    val t = w.table("tbl")
    w.read(t, predicate = "NOT (a > 5)")
    w.read(t, predicate = "NOT (a < 0)", name = "read_not_lt_0")
    w.snapshot(t)
  }

  test("ds_not_and", "NOT with AND (De Morgan)", "dataSkipping") { w =>
    w.sql("CREATE TABLE tbl (a LONG) USING delta")
    w.sql("INSERT INTO tbl VALUES (1), (2)")
    val t = w.table("tbl")
    w.read(t, predicate = "NOT (a > 5 AND a < 10)")
    w.read(t, predicate = "NOT (a > 0 AND a < 3)")
    w.read(t, predicate = "NOT (a = 1 AND a = 2)", name = "read_not_and_contra")
    w.snapshot(t)
  }

  test("ds_not_or", "NOT with OR (De Morgan)", "dataSkipping") { w =>
    w.sql("CREATE TABLE tbl (a LONG) USING delta")
    w.sql("INSERT INTO tbl VALUES (1), (2)")
    val t = w.table("tbl")
    w.read(t, predicate = "NOT (a > 5 OR a < -5)")
    w.read(t, predicate = "NOT (a = 1 OR a = 2)", name = "read_not_or_all")
    w.snapshot(t)
  }

  // LIKE / starts with

  test("ds_starts_with", "starts with", "dataSkipping") { w =>
    w.sql("CREATE TABLE tbl (a STRING) USING delta")
    w.sql("INSERT INTO tbl VALUES ('apple'), ('banana')")
    val t = w.table("tbl")
    w.read(t, predicate = "a LIKE 'a%'")
    w.read(t, predicate = "a LIKE 'b%'")
    w.read(t, predicate = "a LIKE 'app%'")
    w.read(t, predicate = "a LIKE 'z%'", name = "read_miss_z")
    w.read(t, predicate = "a LIKE 'c%'", name = "read_miss_c")
    w.snapshot(t)
  }

  test("ds_starts_with_nested", "LIKE on nested string fields", "dataSkipping") { w =>
    w.sql("CREATE TABLE tbl (a STRUCT<b: STRING>) USING delta")
    w.sql("INSERT INTO tbl VALUES (named_struct('b', 'apple'))")
    w.sql("INSERT INTO tbl VALUES (named_struct('b', 'banana'))")
    val t = w.table("tbl")
    w.read(t, predicate = "a.b LIKE 'a%'")
    w.read(t, predicate = "a.b LIKE 'b%'")
    w.read(t, predicate = "a.b LIKE 'app%'")
    w.read(t, predicate = "a.b LIKE 'z%'", name = "read_miss_z")
    w.read(t, predicate = "a.b LIKE 'c%'", name = "read_miss_c")
    w.snapshot(t)
  }

  test("ds_string_patterns", "String comparisons and LIKE", "dataSkipping") { w =>
    w.sql("CREATE TABLE tbl (name STRING) USING delta")
    w.sql("INSERT INTO tbl VALUES ('alice'), ('bob')")
    w.sql("INSERT INTO tbl VALUES ('charlie'), ('diana')")
    val t = w.table("tbl")
    w.read(t, predicate = "name = 'alice'")
    w.read(t, predicate = "name >= 'c'")
    w.read(t, predicate = "name < 'b'")
    w.read(t, predicate = "name LIKE 'a%'")
    w.read(t, predicate = "name LIKE 'ch%'")
    w.read(t, predicate = "name LIKE 'z%'", name = "read_miss_z")
    w.read(t, predicate = "name > 'e'", name = "read_miss_gt_e")
    w.read(t, predicate = "name LIKE 'x%'", name = "read_miss_x")
    w.snapshot(t)
  }

  // Long strings (prefix truncation edge cases)

  test("ds_long_strings_min", "long strings, long min", "dataSkipping") { w =>
    w.sql("CREATE TABLE tbl (a STRING) USING delta")
    // 33-char prefix: "aaa...a" then differ
    val longA = "a" * 32 + "x"
    val longB = "a" * 32 + "y"
    w.sql(s"INSERT INTO tbl VALUES ('$longA'), ('$longB')")
    val t = w.table("tbl")
    w.read(t, predicate = s"a = '$longA'")
    w.read(t, predicate = s"a >= '${"a" * 32}'")
    w.read(t, predicate = "a LIKE 'aaa%'")
    w.read(t, predicate = "a = 'z'", name = "read_miss_z")
    w.read(t, predicate = "a < 'a'", name = "read_miss_lt_a")
    w.snapshot(t)
  }

  test("ds_long_strings_max", "long strings, long max", "dataSkipping") { w =>
    w.sql("CREATE TABLE tbl (a STRING) USING delta")
    val longZ = "z" * 32 + "a"
    val longY = "z" * 32 + "b"
    w.sql(s"INSERT INTO tbl VALUES ('$longZ'), ('$longY')")
    val t = w.table("tbl")
    w.read(t, predicate = s"a = '$longZ'")
    w.read(t, predicate = s"a >= '${"z" * 32}'")
    w.read(t, predicate = "a LIKE 'zzz%'")
    w.read(t, predicate = s"a <= '${"z" * 33}'")
    w.read(t, predicate = "a = 'a'", name = "read_miss_a")
    w.read(t, predicate = "a < 'z'", name = "read_miss_lt_z")
    w.read(t, predicate = "a > 'zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz'", name = "read_miss_gt_long_z")
    w.snapshot(t)
  }

  // IN predicates

  test("ds_in_set", "IN set predicates", "dataSkipping") { w =>
    w.sql("CREATE TABLE tbl (a INT) USING delta")
    w.sql("INSERT INTO tbl VALUES (1), (2), (3)")
    val t = w.table("tbl")
    w.read(t, predicate = "a IN (1, 2)")
    w.read(t, predicate = "a IN (3)")
    w.read(t, predicate = "a IN (10, 20)", name = "read_miss_in")
    w.snapshot(t)
  }

  test("ds_in_list", "IN list predicate", "dataSkipping") { w =>
    w.sql("CREATE TABLE tbl (a INT) USING delta")
    w.sql("INSERT INTO tbl VALUES (10), (20)")
    w.sql("INSERT INTO tbl VALUES (30), (40)")
    val t = w.table("tbl")
    w.read(t, predicate = "a IN (10, 30)")
    w.read(t, predicate = "a IN (99)", name = "read_miss_in_99")
    w.snapshot(t)
  }

  test("ds_in_nested", "IN on nested field", "dataSkipping") { w =>
    w.sql("CREATE TABLE tbl (s STRUCT<x: INT>) USING delta")
    w.sql("INSERT INTO tbl VALUES (named_struct('x', 1))")
    w.sql("INSERT INTO tbl VALUES (named_struct('x', 5))")
    val t = w.table("tbl")
    w.read(t, predicate = "s.x IN (1, 5)")
    w.read(t, predicate = "s.x IN (99)", name = "read_miss_nested_in")
    w.snapshot(t)
  }

  test("ds_in_with_nulls_mixed", "IN list containing NULL", "dataSkipping") { w =>
    w.sql("CREATE TABLE tbl (a INT) USING delta")
    w.sql("INSERT INTO tbl VALUES (1), (NULL), (3)")
    val t = w.table("tbl")
    w.read(t, predicate = "a IN (1, NULL)")
    w.read(t, predicate = "a IN (99, NULL)", name = "read_in_null_miss")
    w.snapshot(t)
  }

  test("ds_in_with_nulls_only", "IN list with only nulls in file", "dataSkipping") { w =>
    w.sql("CREATE TABLE tbl (a INT) USING delta")
    w.sql("INSERT INTO tbl VALUES (NULL), (NULL)")
    val t = w.table("tbl")
    w.read(t, predicate = "a IN (1)")
    w.read(t, predicate = "a IN (NULL)")
    w.snapshot(t)
  }

  test("ds_in_with_thresholds", "IN with varying size", "dataSkipping") { w =>
    w.sql("CREATE TABLE tbl (a INT) USING delta")
    w.sql("INSERT INTO tbl SELECT id FROM range(1, 11)")
    val t = w.table("tbl")
    // Small IN list
    w.read(t, predicate = "a IN (1, 2, 3)")
    // Larger IN list (may exceed threshold and become range)
    w.read(t, predicate = "a IN (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20)",
      name = "read_in_large")
    w.read(t, predicate = "a IN (100, 200)", name = "read_miss_in_large")
    w.snapshot(t)
  }

  test("ds_not_in", "NOT IN predicate", "dataSkipping") { w =>
    w.sql("CREATE TABLE tbl (a INT) USING delta")
    w.sql("INSERT INTO tbl VALUES (1), (2), (3)")
    val t = w.table("tbl")
    w.read(t, predicate = "a NOT IN (4, 5)")
    w.read(t, predicate = "a NOT IN (1, 2, 3)", name = "read_not_in_all")
    w.read(t, predicate = "a NOT IN (1)")
    w.snapshot(t)
  }

  // NULL predicates

  test("ds_is_null", "IS NULL predicate pushdown", "dataSkipping") { w =>
    w.sql("CREATE TABLE tbl (a INT) USING delta")
    w.sql("INSERT INTO tbl VALUES (1), (NULL), (3)")
    val t = w.table("tbl")
    w.read(t, predicate = "a IS NULL")
    w.snapshot(t)
  }

  test("ds_is_not_null", "IS NOT NULL predicate pushdown", "dataSkipping") { w =>
    w.sql("CREATE TABLE tbl (a INT) USING delta")
    w.sql("INSERT INTO tbl VALUES (1), (NULL), (3)")
    val t = w.table("tbl")
    w.read(t, predicate = "a IS NOT NULL")
    w.snapshot(t)
  }

  test("ds_isnull_complex_expr", "IS NULL with complex expressions", "dataSkipping") { w =>
    w.sql("CREATE TABLE tbl (a INT, b STRING) USING delta")
    w.sql("INSERT INTO tbl VALUES (1, 'x'), (NULL, NULL)")
    w.sql("INSERT INTO tbl VALUES (3, 'y'), (NULL, 'z')")
    val t = w.table("tbl")
    w.read(t, predicate = "a IS NULL AND b IS NULL")
    w.read(t, predicate = "a IS NULL OR b IS NULL")
    w.read(t, predicate = "a IS NOT NULL AND b IS NOT NULL")
    w.snapshot(t)
  }

  test("ds_nulls_only_null", "nulls - only null in file", "dataSkipping") { w =>
    w.sql("CREATE TABLE tbl (a LONG) USING delta")
    w.sql("INSERT INTO tbl VALUES (NULL)")
    val t = w.table("tbl")
    w.read(t, predicate = "a IS NULL")
    w.read(t, predicate = "a IS NOT NULL", name = "read_is_not_null")
    w.read(t, predicate = "a = 1", name = "read_eq_1")
    w.read(t, predicate = "a > 0", name = "read_gt_0")
    w.read(t, predicate = "a < 0", name = "read_lt_0")
    w.read(t, predicate = "a >= 0", name = "read_gte_0")
    w.read(t, predicate = "a <= 0", name = "read_lte_0")
    w.read(t, predicate = "a <=> NULL", name = "read_nse_null")
    w.read(t, predicate = "a <=> 1", name = "read_nse_1")
    w.read(t, predicate = "a IN (1, 2)", name = "read_in_1_2")
    w.read(t, predicate = "NOT (a = 1)", name = "read_not_eq_1")
    w.read(t, predicate = "NOT (a IS NULL)", name = "read_not_is_null")
    w.read(t, predicate = "NOT (a IS NOT NULL)", name = "read_not_is_not_null")
    w.read(t, predicate = "a = 1 OR a IS NULL", name = "read_eq_or_null")
    w.read(t, predicate = "a = 1 AND a IS NULL", name = "read_eq_and_null")
    w.read(t, predicate = "a LIKE 'x%'", name = "read_like_x")
    w.snapshot(t)
  }

  test("ds_nulls_only_nonnull", "only non-null in file", "dataSkipping") { w =>
    w.sql("CREATE TABLE tbl (a LONG) USING delta")
    w.sql("INSERT INTO tbl VALUES (1)")
    val t = w.table("tbl")
    w.read(t, predicate = "a IS NULL", name = "read_is_null")
    w.read(t, predicate = "a IS NOT NULL")
    w.snapshot(t)
  }

  test("ds_nulls_mixed", "nulls - null + not-null in same file", "dataSkipping") { w =>
    w.sql("CREATE TABLE tbl (a LONG) USING delta")
    w.sql("INSERT INTO tbl VALUES (1), (NULL), (3)")
    val t = w.table("tbl")
    w.read(t, predicate = "a IS NULL")
    w.read(t, predicate = "a IS NOT NULL")
    w.read(t, predicate = "a = 1")
    w.read(t, predicate = "a > 2")
    w.read(t, predicate = "a < 2")
    w.read(t, predicate = "a >= 1")
    w.read(t, predicate = "a <= 3")
    w.read(t, predicate = "a <=> NULL", name = "read_nse_null")
    w.read(t, predicate = "a <=> 1", name = "read_nse_1")
    w.read(t, predicate = "a IN (1, 3)")
    w.read(t, predicate = "a IN (5)", name = "read_in_miss_5")
    w.read(t, predicate = "a = 1 OR a IS NULL")
    w.read(t, predicate = "a = 1 AND a IS NOT NULL")
    w.read(t, predicate = "NOT (a = 1)")
    w.read(t, predicate = "NOT (a IS NULL)", name = "read_not_is_null")
    w.read(t, predicate = "a > 5", name = "read_miss_gt_5")
    w.read(t, predicate = "a < 0", name = "read_miss_lt_0")
    w.snapshot(t)
  }

  test("ds_nulls_nonnulls_only", "non-nulls only in file", "dataSkipping") { w =>
    w.sql("CREATE TABLE tbl (a LONG) USING delta")
    w.sql("INSERT INTO tbl VALUES (1), (2), (3)")
    val t = w.table("tbl")
    w.read(t, predicate = "a IS NULL")
    w.read(t, predicate = "a IS NOT NULL")
    w.read(t, predicate = "a = 2")
    w.read(t, predicate = "a > 5", name = "read_miss_gt_5")
    w.snapshot(t)
  }

  test("ds_nulls_partial_stats", "non-nulls only, partial stats", "dataSkipping") { w =>
    w.sql("""CREATE TABLE tbl (a LONG, b STRING) USING delta
      TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = '1')""")
    w.sql("INSERT INTO tbl VALUES (1, 'x'), (2, 'y'), (3, 'z')")
    val t = w.table("tbl")
    // a has stats, b does not
    w.read(t, predicate = "a = 1")
    w.read(t, predicate = "a > 5", name = "read_miss_a_gt_5")
    w.read(t, predicate = "b = 'x'")
    w.read(t, predicate = "b = 'nonexistent'", name = "read_b_no_stats")
    w.read(t, predicate = "a = 1 AND b = 'x'")
    w.read(t, predicate = "a > 5 AND b = 'x'", name = "read_miss_a_has_stats")
    w.read(t, predicate = "a = 1 OR b = 'nonexistent'")
    w.read(t, predicate = "a IS NULL", name = "read_a_is_null")
    w.snapshot(t)
  }

  test("ds_null_safe_eq", "Null-safe equals operator (<=>)", "dataSkipping") { w =>
    w.sql("CREATE TABLE tbl (a INT) USING delta")
    w.sql("INSERT INTO tbl VALUES (1), (NULL), (3)")
    val t = w.table("tbl")
    w.read(t, predicate = "a <=> 1")
    w.read(t, predicate = "a <=> NULL")
    w.read(t, predicate = "a <=> 3")
    w.read(t, predicate = "a <=> 99", name = "read_miss_nse_99")
    w.snapshot(t)
  }

  test("ds_null_string_partition", "Null string partition values", "dataSkipping") { w =>
    w.sql("CREATE TABLE tbl (id INT, part STRING) USING delta PARTITIONED BY (part)")
    w.sql("INSERT INTO tbl VALUES (1, 'a'), (2, NULL), (3, 'b')")
    val t = w.table("tbl")
    w.read(t, predicate = "part IS NULL")
    w.read(t, predicate = "part = 'a'")
    w.snapshot(t)
  }

  test("ds_null_mixed_partitions", "Mix of null and non-null across partitions",
      "dataSkipping") { w =>
    w.sql("""CREATE TABLE tbl (id INT, p1 STRING, p2 INT) USING delta
      PARTITIONED BY (p1, p2)""")
    w.sql("INSERT INTO tbl VALUES (1, 'a', 1), (2, NULL, 2), (3, 'b', NULL), (4, NULL, NULL)")
    val t = w.table("tbl")
    w.read(t, predicate = "p1 IS NULL")
    w.read(t, predicate = "p2 IS NULL")
    w.read(t, predicate = "p1 IS NULL AND p2 IS NULL")
    w.snapshot(t)
  }

  // BETWEEN

  test("ds_between", "BETWEEN predicate for range queries", "dataSkipping") { w =>
    w.sql("CREATE TABLE tbl (a INT) USING delta")
    w.sql("INSERT INTO tbl VALUES (1), (5), (10)")
    w.sql("INSERT INTO tbl VALUES (15), (20), (25)")
    val t = w.table("tbl")
    w.read(t, predicate = "a BETWEEN 1 AND 10")
    w.read(t, predicate = "a BETWEEN 5 AND 20")
    w.read(t, predicate = "a BETWEEN 1 AND 25", name = "read_between_all")
    w.read(t, predicate = "a BETWEEN 50 AND 100", name = "read_miss_between")
    w.snapshot(t)
  }

  // Boolean column

  test("ds_boolean", "boolean comparisons", "dataSkipping") { w =>
    w.sql("CREATE TABLE tbl (a BOOLEAN) USING delta")
    w.sql("INSERT INTO tbl VALUES (true)")
    w.sql("INSERT INTO tbl VALUES (false)")
    val t = w.table("tbl")
    w.read(t, predicate = "a = true")
    w.read(t, predicate = "a = false")
    w.read(t, predicate = "a IS NOT NULL")
    w.read(t, predicate = "a IS NULL", name = "read_miss_null")
    w.snapshot(t)
  }

  test("ds_boolean_column", "Boolean column skipping", "dataSkipping") { w =>
    w.sql("CREATE TABLE tbl (id INT, active BOOLEAN) USING delta")
    w.sql("INSERT INTO tbl VALUES (1, true), (2, true)")
    w.sql("INSERT INTO tbl VALUES (3, false), (4, false)")
    val t = w.table("tbl")
    w.read(t, predicate = "active = true")
    w.read(t, predicate = "active = false")
    w.snapshot(t)
  }

  // Numeric types

  test("ds_numeric_types", "Numeric type comparisons", "dataSkipping") { w =>
    w.sql("CREATE TABLE tbl (i INT, l LONG, f FLOAT, d DOUBLE) USING delta")
    w.sql("INSERT INTO tbl VALUES (1, 100, 1.5, 2.5)")
    w.sql("INSERT INTO tbl VALUES (10, 1000, 10.5, 20.5)")
    val t = w.table("tbl")
    w.read(t, predicate = "i = 1")
    w.read(t, predicate = "l > 500")
    w.read(t, predicate = "f < 2.0")
    w.read(t, predicate = "d >= 20.0")
    w.read(t, predicate = "i > 100", name = "read_miss_i")
    w.read(t, predicate = "d < 1.0", name = "read_miss_d")
    w.snapshot(t)
  }

  test("ds_tinyint_smallint", "TINYINT and SMALLINT column stats", "dataSkipping") { w =>
    w.sql("CREATE TABLE tbl (t TINYINT, s SMALLINT) USING delta")
    w.sql("INSERT INTO tbl VALUES (1, 100)")
    w.sql("INSERT INTO tbl VALUES (127, 32767)")
    val t = w.table("tbl")
    w.read(t, predicate = "t = 1")
    w.read(t, predicate = "s > 30000")
    w.read(t, predicate = "t > 127", name = "read_miss_t")
    w.snapshot(t)
  }

  test("ds_float_special_values", "FLOAT with NaN, Infinity, -0.0", "dataSkipping") { w =>
    w.sql("CREATE TABLE tbl (f FLOAT) USING delta")
    w.sql("INSERT INTO tbl VALUES (CAST('NaN' AS FLOAT)), (CAST('Infinity' AS FLOAT)), (CAST('-0.0' AS FLOAT))")
    val t = w.table("tbl")
    w.read(t, predicate = "f > 0")
    w.read(t, predicate = "f IS NOT NULL")
    w.snapshot(t)
  }

  test("ds_binary_type", "BINARY column (no min/max, only nullCount)", "dataSkipping") { w =>
    w.sql("CREATE TABLE tbl (id INT, data BINARY) USING delta")
    w.sql("INSERT INTO tbl VALUES (1, X'0102'), (2, NULL)")
    val t = w.table("tbl")
    w.read(t, predicate = "data IS NOT NULL")
    w.read(t, predicate = "data IS NULL")
    w.snapshot(t)
  }

  test("ds_implicit_cast", "Implicit type cast in predicate", "dataSkipping") { w =>
    w.sql("CREATE TABLE tbl (a LONG) USING delta")
    w.sql("INSERT INTO tbl VALUES (1), (2)")
    val t = w.table("tbl")
    // int literal vs long column
    w.read(t, predicate = "a = 1")
    w.read(t, predicate = "a > 0")
    w.snapshot(t)
  }

  // Date/time predicates

  test("ds_datetime", "Date and timestamp predicates", "dataSkipping") { w =>
    w.sql("CREATE TABLE tbl (d DATE, ts TIMESTAMP) USING delta")
    w.sql("INSERT INTO tbl VALUES (DATE'2024-01-01', TIMESTAMP'2024-01-01 00:00:00')")
    w.sql("INSERT INTO tbl VALUES (DATE'2024-06-15', TIMESTAMP'2024-06-15 12:00:00')")
    val t = w.table("tbl")
    w.read(t, predicate = "d = DATE'2024-01-01'")
    w.read(t, predicate = "d > DATE'2024-03-01'")
    w.read(t, predicate = "ts < TIMESTAMP'2024-03-01 00:00:00'")
    w.read(t, predicate = "d > DATE'2025-01-01'", name = "read_miss_future")
    w.snapshot(t)
  }

  test("ds_timestamp_microsecond", "Microsecond precision timestamp skipping",
      "dataSkipping") { w =>
    w.sql("CREATE TABLE tbl (ts TIMESTAMP) USING delta")
    w.sql("INSERT INTO tbl VALUES (TIMESTAMP'2024-01-01 00:00:00.000001')")
    w.sql("INSERT INTO tbl VALUES (TIMESTAMP'2024-01-01 00:00:00.000002')")
    val t = w.table("tbl")
    w.read(t, predicate = "ts = TIMESTAMP'2024-01-01 00:00:00.000001'")
    w.read(t, predicate = "ts > TIMESTAMP'2024-01-01 00:00:00.000002'",
      name = "read_miss_after")
    w.snapshot(t)
  }

  test("ds_timestamp_ntz_skipping", "NTZ-specific data skipping", "dataSkipping") { w =>
    w.sql("""CREATE TABLE tbl (ts TIMESTAMP_NTZ) USING delta
      TBLPROPERTIES ('delta.minReaderVersion' = '3', 'delta.minWriterVersion' = '7',
        'delta.feature.timestampNtz' = 'supported')""")
    w.sql("INSERT INTO tbl VALUES (TIMESTAMP_NTZ'2024-01-01 00:00:00')")
    w.sql("INSERT INTO tbl VALUES (TIMESTAMP_NTZ'2024-06-15 12:00:00')")
    val t = w.table("tbl")
    w.read(t, predicate = "ts = TIMESTAMP_NTZ'2024-01-01 00:00:00'")
    w.read(t, predicate = "ts > TIMESTAMP_NTZ'2025-01-01 00:00:00'",
      name = "read_miss_future_ntz")
    w.snapshot(t)
  }

  test("ds_year_function", "year() extraction on Date", "dataSkipping") { w =>
    w.sql("CREATE TABLE tbl (d DATE, value INT) USING delta")
    w.sql("INSERT INTO tbl VALUES (DATE'2024-03-15', 1), (DATE'2024-11-20', 2)")
    w.sql("INSERT INTO tbl VALUES (DATE'2025-01-05', 3)")
    val t = w.table("tbl")
    w.read(t, predicate = "year(d) = 2024")
    w.read(t, predicate = "year(d) = 2025")
    w.read(t, predicate = "year(d) = 2020", name = "read_miss_year")
    w.snapshot(t)
  }

  test("ds_month_function", "month() with predicates", "dataSkipping") { w =>
    w.sql("CREATE TABLE tbl (d DATE, value INT) USING delta")
    w.sql("INSERT INTO tbl VALUES (DATE'2024-01-15', 1), (DATE'2024-06-20', 2)")
    w.sql("INSERT INTO tbl VALUES (DATE'2024-12-05', 3)")
    val t = w.table("tbl")
    w.read(t, predicate = "month(d) = 1")
    w.read(t, predicate = "month(d) = 6")
    w.read(t, predicate = "month(d) = 8", name = "read_miss_month")
    w.snapshot(t)
  }

  test("ds_trunc_date", "trunc() on Date", "dataSkipping") { w =>
    w.sql("CREATE TABLE tbl (d DATE) USING delta")
    w.sql("INSERT INTO tbl VALUES (DATE'2024-03-15'), (DATE'2024-03-20')")
    w.sql("INSERT INTO tbl VALUES (DATE'2024-06-01'), (DATE'2024-06-30')")
    val t = w.table("tbl")
    w.read(t, predicate = "trunc(d, 'MONTH') = DATE'2024-03-01'")
    w.read(t, predicate = "trunc(d, 'MONTH') = DATE'2024-06-01'")
    w.read(t, predicate = "trunc(d, 'YEAR') = DATE'2025-01-01'", name = "read_miss_trunc")
    w.snapshot(t)
  }

  test("ds_date_trunc_timestamp", "date_trunc() on Timestamp", "dataSkipping") { w =>
    w.sql("CREATE TABLE tbl (ts TIMESTAMP) USING delta")
    w.sql("INSERT INTO tbl VALUES (TIMESTAMP'2024-03-15 10:30:00')")
    w.sql("INSERT INTO tbl VALUES (TIMESTAMP'2024-06-01 14:00:00')")
    val t = w.table("tbl")
    w.read(t, predicate = "date_trunc('MONTH', ts) = TIMESTAMP'2024-03-01 00:00:00'")
    w.read(t, predicate = "date_trunc('YEAR', ts) = TIMESTAMP'2025-01-01 00:00:00'",
      name = "read_miss_trunc_ts")
    w.snapshot(t)
  }

  test("ds_datediff", "datediff() predicate", "dataSkipping") { w =>
    w.sql("CREATE TABLE tbl (d DATE) USING delta")
    w.sql("INSERT INTO tbl VALUES (DATE'2024-01-01'), (DATE'2024-01-10')")
    w.sql("INSERT INTO tbl VALUES (DATE'2024-06-01')")
    val t = w.table("tbl")
    w.read(t, predicate = "datediff(d, DATE'2024-01-01') <= 10")
    w.read(t, predicate = "datediff(d, DATE'2024-01-01') > 100")
    w.snapshot(t)
  }

  test("ds_date_add_sub", "date_add()/date_sub()", "dataSkipping") { w =>
    w.sql("CREATE TABLE tbl (d DATE) USING delta")
    w.sql("INSERT INTO tbl VALUES (DATE'2024-01-01'), (DATE'2024-01-15')")
    w.sql("INSERT INTO tbl VALUES (DATE'2024-06-01')")
    val t = w.table("tbl")
    w.read(t, predicate = "d >= date_add(DATE'2024-01-01', -1)")
    w.read(t, predicate = "d <= date_sub(DATE'2024-01-01', 10)",
      name = "read_miss_date_sub")
    w.snapshot(t)
  }

  // Multi-file range skipping

  test("ds_multi_file_ranges", "Multiple files with different ranges", "dataSkipping") { w =>
    w.sql("CREATE TABLE tbl (a INT) USING delta")
    w.sql("INSERT INTO tbl SELECT id FROM range(1, 11)")   // file 1: 1-10
    w.sql("INSERT INTO tbl SELECT id FROM range(11, 21)")  // file 2: 11-20
    w.sql("INSERT INTO tbl SELECT id FROM range(21, 31)")  // file 3: 21-30
    val t = w.table("tbl")
    w.read(t, name = "read_full_scan")
    w.read(t, predicate = "a <= 10", name = "read_hit_file1_only")
    w.read(t, predicate = "a > 10 AND a <= 20", name = "read_hit_file2_only")
    w.read(t, predicate = "a > 20", name = "read_hit_file3_only")
    w.read(t, predicate = "a <= 15", name = "read_hit_file1_and_2")
    w.read(t, predicate = "a > 15", name = "read_hit_file2_and_3")
    w.read(t, predicate = "a > 100", name = "read_miss_all_gt_100")
    w.read(t, predicate = "a < 0", name = "read_miss_all_lt_0")
    w.snapshot(t)
  }

  test("ds_multi_file_time", "TIME type multi-file data skipping", "dataSkipping") { w =>
    w.sql("CREATE TABLE tbl (ts TIMESTAMP, value INT) USING delta")
    w.sql("INSERT INTO tbl VALUES (TIMESTAMP'2024-01-01 00:00:00', 1), (TIMESTAMP'2024-01-15 00:00:00', 2)")
    w.sql("INSERT INTO tbl VALUES (TIMESTAMP'2024-06-01 00:00:00', 3), (TIMESTAMP'2024-12-31 00:00:00', 4)")
    val t = w.table("tbl")
    w.read(t, predicate = "ts < TIMESTAMP'2024-02-01 00:00:00'")
    w.read(t, predicate = "ts >= TIMESTAMP'2024-06-01 00:00:00'")
    w.read(t, predicate = "ts > TIMESTAMP'2025-01-01 00:00:00'", name = "read_miss_future")
    w.snapshot(t)
  }

  // Typed stats (decimal, date, timestamp, float, double)

  test("ds_typed_stats", "DECIMAL/DATE/TIMESTAMP/FLOAT/DOUBLE data skipping stats",
      "dataSkipping") { w =>
    w.sql("""CREATE TABLE tbl (
      c1 LONG, c2 STRING, c3 FLOAT, c4 DOUBLE,
      c5 TIMESTAMP, c6 TIMESTAMP_NTZ, c7 DATE,
      c8 BYTE, c9 SHORT, c10 DECIMAL(3,2)
    ) USING delta
    TBLPROPERTIES ('delta.minReaderVersion' = '3', 'delta.minWriterVersion' = '7',
      'delta.feature.timestampNtz' = 'supported')""")
    w.sql("""INSERT INTO tbl VALUES
      (1, 'abc', 1.5, 2.5, TIMESTAMP'2024-01-01 00:00:00',
       TIMESTAMP_NTZ'2024-01-01 00:00:00', DATE'2024-01-01', 1, 100, 1.23)""")
    w.sql("""INSERT INTO tbl VALUES
      (100, 'xyz', 99.9, 199.9, TIMESTAMP'2024-12-31 23:59:59',
       TIMESTAMP_NTZ'2024-12-31 23:59:59', DATE'2024-12-31', 127, 32000, 9.87)""")
    val t = w.table("tbl")
    w.read(t, predicate = "c1 = 1")
    w.read(t, predicate = "c1 > 50")
    w.read(t, predicate = "c2 = 'abc'")
    w.read(t, predicate = "c3 < 2.0")
    w.read(t, predicate = "c4 >= 100.0")
    w.read(t, predicate = "c5 = TIMESTAMP'2024-01-01 00:00:00'")
    w.read(t, predicate = "c6 > TIMESTAMP_NTZ'2024-06-01 00:00:00'")
    w.read(t, predicate = "c7 = DATE'2024-01-01'")
    w.read(t, predicate = "c7 > DATE'2024-06-01'")
    w.read(t, predicate = "c8 = 1")
    w.read(t, predicate = "c9 > 20000")
    w.read(t, predicate = "c10 > 5.00")
    w.read(t, predicate = "c10 = 1.23")
    w.read(t, predicate = "c1 > 200", name = "read_miss_c1")
    w.read(t, predicate = "c3 > 200.0", name = "read_miss_c3")
    w.read(t, predicate = "c4 < 1.0", name = "read_miss_c4")
    w.read(t, predicate = "c5 > TIMESTAMP'2025-06-01 00:00:00'", name = "read_miss_c5")
    w.read(t, predicate = "c7 > DATE'2025-01-01'", name = "read_miss_c7")
    w.read(t, predicate = "c10 > 9.99", name = "read_miss_c10")
    w.snapshot(t)
  }

  // Variant null stats

  test("ds_variant_null_stats", "VARIANT NULL/NOT NULL data skipping", "dataSkipping") { w =>
    w.sql("""CREATE TABLE tbl (
      v VARIANT, v_struct STRUCT<v: VARIANT>,
      null_v VARIANT, null_v_struct STRUCT<v: VARIANT>
    ) USING delta
    TBLPROPERTIES ('delta.feature.variantType-preview' = 'supported')""")
    w.sql("""INSERT INTO tbl VALUES (
      PARSE_JSON('1'), named_struct('v', PARSE_JSON('"hello"')),
      NULL, named_struct('v', NULL))""")
    val t = w.table("tbl")
    w.read(t, predicate = "v IS NOT NULL")
    w.read(t, predicate = "v IS NULL")
    w.read(t, predicate = "null_v IS NULL")
    w.read(t, predicate = "null_v IS NOT NULL")
    w.read(t, predicate = "v_struct.v IS NOT NULL")
    w.read(t, predicate = "v_struct.v IS NULL")
    w.read(t, predicate = "null_v_struct.v IS NULL")
    w.snapshot(t)
  }

  // Indexed columns / stats configuration

  test("ds_indexed_names_empty", "empty indexed names disables stats", "dataSkipping") { w =>
    w.sql("""CREATE TABLE tbl (a LONG, b LONG) USING delta
      TBLPROPERTIES ('delta.dataSkippingStatsColumns' = '')""")
    w.sql("INSERT INTO tbl VALUES (1, 10), (2, 20)")
    val t = w.table("tbl")
    w.read(t, predicate = "a = 1")
    w.read(t, predicate = "b = 10")
    w.read(t, predicate = "a > 100", name = "read_a_gt_100")
    w.snapshot(t)
  }

  test("ds_indexed_names_subset", "index subset of leaf columns", "dataSkipping") { w =>
    w.sql("""CREATE TABLE tbl (a LONG, b LONG, c LONG) USING delta
      TBLPROPERTIES ('delta.dataSkippingStatsColumns' = 'a,c')""")
    w.sql("INSERT INTO tbl VALUES (1, 10, 100)")
    w.sql("INSERT INTO tbl VALUES (5, 50, 500)")
    val t = w.table("tbl")
    // a has stats
    w.read(t, predicate = "a = 1")
    w.read(t, predicate = "a > 10", name = "read_miss_a")
    // b has no stats
    w.read(t, predicate = "b = 10")
    w.read(t, predicate = "b > 100", name = "read_b_no_skip")
    // c has stats
    w.read(t, predicate = "c = 100")
    w.read(t, predicate = "c > 1000", name = "read_miss_c")
    w.read(t, predicate = "a = 1 AND c = 100")
    w.read(t, predicate = "a > 10 AND c > 1000", name = "read_miss_ac")
    w.read(t, predicate = "a = 1 OR b = 10")
    w.snapshot(t)
  }

  test("ds_indexed_names_nested", "naming nested column indexes leaves",
      "dataSkipping") { w =>
    w.sql("""CREATE TABLE tbl (
      a STRUCT<x: LONG, y: LONG>,
      b STRUCT<p: LONG, q: LONG>
    ) USING delta
    TBLPROPERTIES ('delta.dataSkippingStatsColumns' = 'a.x,b.q')""")
    w.sql("INSERT INTO tbl VALUES (named_struct('x',1,'y',10), named_struct('p',100,'q',1000))")
    w.sql("INSERT INTO tbl VALUES (named_struct('x',5,'y',50), named_struct('p',500,'q',5000))")
    val t = w.table("tbl")
    // a.x has stats
    w.read(t, predicate = "a.x = 1")
    w.read(t, predicate = "a.x > 10", name = "read_miss_ax")
    // a.y no stats
    w.read(t, predicate = "a.y = 10")
    w.read(t, predicate = "a.y > 100", name = "read_ay_no_skip")
    // b.p no stats
    w.read(t, predicate = "b.p = 100")
    // b.q has stats
    w.read(t, predicate = "b.q = 1000")
    w.read(t, predicate = "b.q > 10000", name = "read_miss_bq")
    w.read(t, predicate = "a.x = 1 AND b.q = 1000")
    w.read(t, predicate = "a.x > 10 AND b.q > 10000", name = "read_miss_ax_bq")
    w.snapshot(t)
  }

  test("ds_indexed_names_complex", "nested columns with complex types", "dataSkipping") { w =>
    w.sql("""CREATE TABLE tbl (
      a STRUCT<x: LONG, y: STRUCT<z: LONG>>,
      b LONG
    ) USING delta
    TBLPROPERTIES ('delta.dataSkippingStatsColumns' = 'a.x,a.y.z,b')""")
    w.sql("INSERT INTO tbl VALUES (named_struct('x',1,'y',named_struct('z',10)), 100)")
    w.sql("INSERT INTO tbl VALUES (named_struct('x',5,'y',named_struct('z',50)), 500)")
    val t = w.table("tbl")
    w.read(t, predicate = "a.x = 1")
    w.read(t, predicate = "a.y.z = 10")
    w.read(t, predicate = "b = 100")
    w.read(t, predicate = "a.x > 10", name = "read_miss_ax")
    w.read(t, predicate = "a.y.z > 100", name = "read_miss_ayz")
    w.snapshot(t)
  }

  test("ds_indexed_names_backtick", "backtick escapes", "dataSkipping") { w =>
    w.sql("CREATE TABLE tbl (`a.b` LONG, `c d` LONG) USING delta")
    w.sql("INSERT INTO tbl VALUES (1, 10)")
    val t = w.table("tbl")
    w.read(t, predicate = "`a.b` = 1")
    w.read(t, predicate = "`c d` = 10")
    w.read(t, predicate = "`a.b` > 5", name = "read_miss_ab")
    w.snapshot(t)
  }

  test("ds_more_cols_than_indexed", "more columns than indexed", "dataSkipping") { w =>
    w.sql("""CREATE TABLE tbl (a LONG, b LONG, c LONG, d LONG) USING delta
      TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = '2')""")
    w.sql("INSERT INTO tbl VALUES (1, 10, 100, 1000)")
    val t = w.table("tbl")
    w.read(t, predicate = "a = 1")
    w.read(t, predicate = "b = 10")
    w.read(t, predicate = "c = 100")  // no stats
    w.read(t, predicate = "d = 1000")  // no stats
    w.snapshot(t)
  }

  test("ds_missing_stats_cols", "missing stats columns", "dataSkipping") { w =>
    w.sql("""CREATE TABLE tbl (a LONG) USING delta
      TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = '1')""")
    w.sql("INSERT INTO tbl VALUES (1), (2)")
    w.sql("ALTER TABLE tbl ADD COLUMN b LONG")
    w.sql("INSERT INTO tbl VALUES (3, 30)")
    val t = w.table("tbl")
    w.read(t, predicate = "a = 1")
    w.read(t, predicate = "b = 30")
    w.read(t, predicate = "a > 5", name = "read_miss_a")
    w.read(t, predicate = "b IS NULL")
    w.snapshot(t)
  }

  test("ds_missing_stats_graceful", "Stats missing, read still works (full scan)",
      "dataSkipping") { w =>
    w.sql("CREATE TABLE tbl (a INT) USING delta")
    w.sql("INSERT INTO tbl VALUES (1), (2), (3)")
    val t = w.table("tbl")
    // Strip stats from commit
    w.modifyCommitActions(t, version = 1) { case ("add", node) =>
      node.remove("stats"); true
      case _ => true
    }
    w.read(t, predicate = "a = 1")
    w.read(t, predicate = "a > 99")
    w.snapshot(t)
  }

  test("ds_stats_config_change", "Stats config changing across versions",
      "dataSkipping") { w =>
    w.sql("""CREATE TABLE tbl (a INT, b INT) USING delta
      TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = '2')""")
    w.sql("INSERT INTO tbl VALUES (1, 10)")
    w.sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = '1')")
    w.sql("INSERT INTO tbl VALUES (2, 20)")
    val t = w.table("tbl")
    // First file: both a and b have stats; second file: only a
    w.read(t, predicate = "a = 1")
    w.read(t, predicate = "b = 10")
    w.snapshot(t)
  }

  // Nested indexed columns (delta.dataSkippingNumIndexedCols with nested schema)

  test("ds_nested_indexed_0", "nested schema, indexed=0", "dataSkipping") { w =>
    w.sql("""CREATE TABLE tbl (a STRUCT<x: LONG, y: LONG>, b LONG) USING delta
      TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = '0')""")
    w.sql("INSERT INTO tbl VALUES (named_struct('x',1,'y',10), 100)")
    val t = w.table("tbl")
    w.read(t, predicate = "a.x = 1")
    w.read(t, predicate = "a.y = 10")
    w.read(t, predicate = "b = 100")
    w.read(t, predicate = "b > 500", name = "read_b_no_stats")
    w.snapshot(t)
  }

  test("ds_nested_indexed_3", "nested schema, indexed=3", "dataSkipping") { w =>
    w.sql("""CREATE TABLE tbl (
      a STRUCT<x: LONG, y: LONG>,
      b LONG,
      c STRUCT<p: LONG>
    ) USING delta
    TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = '3')""")
    w.sql("INSERT INTO tbl VALUES (named_struct('x',1,'y',10), 100, named_struct('p',1000))")
    w.sql("INSERT INTO tbl VALUES (named_struct('x',5,'y',50), 500, named_struct('p',5000))")
    val t = w.table("tbl")
    w.read(t, predicate = "a.x = 1")
    w.read(t, predicate = "a.y = 10")
    w.read(t, predicate = "b = 100")
    w.read(t, predicate = "c.p = 1000")
    w.read(t, predicate = "a.x > 10", name = "read_miss_ax")
    w.read(t, predicate = "b > 1000", name = "read_miss_b")
    w.read(t, predicate = "a.x = 1 AND b = 100")
    w.read(t, predicate = "a.x = 1 OR c.p = 5000")
    w.snapshot(t)
  }

  test("ds_nested_indexed_6", "nested schema, indexed=6", "dataSkipping") { w =>
    w.sql("""CREATE TABLE tbl (
      a STRUCT<x: LONG, y: LONG, z: LONG>,
      b STRUCT<p: LONG, q: LONG>,
      c LONG
    ) USING delta
    TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = '6')""")
    w.sql("INSERT INTO tbl VALUES (named_struct('x',1,'y',2,'z',3), named_struct('p',4,'q',5), 6)")
    val t = w.table("tbl")
    w.read(t, predicate = "a.x = 1")
    w.read(t, predicate = "a.z = 3")
    w.read(t, predicate = "b.p = 4")
    w.read(t, predicate = "b.q = 5")
    w.read(t, predicate = "c = 6")
    w.snapshot(t)
  }

  test("ds_nested_indexed_9", "nested schema, indexed=9", "dataSkipping") { w =>
    w.sql("""CREATE TABLE tbl (
      a STRUCT<x: LONG, y: LONG, z: LONG>,
      b STRUCT<p: LONG, q: LONG, r: LONG>,
      c STRUCT<s: LONG, t: LONG, u: LONG>
    ) USING delta
    TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = '9')""")
    w.sql("""INSERT INTO tbl VALUES (
      named_struct('x',1,'y',2,'z',3),
      named_struct('p',4,'q',5,'r',6),
      named_struct('s',7,'t',8,'u',9))""")
    val t = w.table("tbl")
    w.read(t, predicate = "a.x = 1")
    w.read(t, predicate = "b.r = 6")
    w.read(t, predicate = "c.u = 9")
    w.read(t, predicate = "c.u > 100", name = "read_miss_cu")
    w.read(t, predicate = "a.x = 1 AND c.u = 9")
    w.snapshot(t)
  }

  // Partitioned + stats combined

  test("ds_partitioned", "Partitioned table data skipping", "dataSkipping") { w =>
    w.sql("CREATE TABLE tbl (id INT, part STRING) USING delta PARTITIONED BY (part)")
    w.sql("INSERT INTO tbl VALUES (1, 'a'), (2, 'a')")
    w.sql("INSERT INTO tbl VALUES (3, 'b'), (4, 'b')")
    w.sql("INSERT INTO tbl VALUES (5, 'c')")
    val t = w.table("tbl")
    w.read(t, predicate = "part = 'a'")
    w.read(t, predicate = "part = 'b' AND id > 3")
    w.read(t, predicate = "id < 3")
    w.read(t, predicate = "part = 'z'", name = "read_miss_part")
    w.read(t, predicate = "id > 100", name = "read_miss_id")
    w.snapshot(t)
  }

  test("ds_partition_and_stats", "Partition pruning + stats skipping combined",
      "dataSkipping") { w =>
    w.sql("CREATE TABLE tbl (id INT, value INT, part STRING) USING delta PARTITIONED BY (part)")
    w.sql("INSERT INTO tbl VALUES (1, 10, 'a'), (2, 20, 'a')")
    w.sql("INSERT INTO tbl VALUES (3, 30, 'b'), (4, 40, 'b')")
    val t = w.table("tbl")
    w.read(t, predicate = "part = 'a' AND value > 15")
    w.read(t, predicate = "part = 'b' AND value < 25", name = "read_miss_combined")
    w.read(t, predicate = "part = 'a' OR value > 35")
    w.snapshot(t)
  }

  test("ds_partition_or_predicate", "OR predicate on partition column", "dataSkipping") { w =>
    w.sql("CREATE TABLE tbl (id INT, part STRING) USING delta PARTITIONED BY (part)")
    w.sql("INSERT INTO tbl VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    val t = w.table("tbl")
    w.read(t, predicate = "part = 'a' OR part = 'c'")
    w.read(t, predicate = "part = 'z'", name = "read_miss_part")
    w.snapshot(t)
  }

  // Schema order mismatch and nonexistent col filter

  test("ds_schema_order_mismatch", "Query columns in different order", "dataSkipping") { w =>
    w.sql("CREATE TABLE tbl (a INT, b INT, c INT) USING delta")
    w.sql("INSERT INTO tbl VALUES (1, 2, 3)")
    val t = w.table("tbl")
    w.read(t, predicate = "c = 3")
    w.read(t, columns = Seq("c", "a"))
    w.snapshot(t)
  }

  test("ds_nonexistent_col_filter", "Filter on non-existent column", "dataSkipping") { w =>
    w.sql("CREATE TABLE tbl (a INT) USING delta")
    w.sql("INSERT INTO tbl VALUES (1), (2)")
    val t = w.table("tbl")
    w.read(t, predicate = "a = 1")
    // nonexistent column handled gracefully
    w.snapshot(t)
  }

  // Generated columns

  test("ds_generated_col_skipping", "Skipping using generated column stats",
      "dataSkipping") { w =>
    w.sql("""CREATE TABLE tbl (
      date_col DATE, value INT,
      year_col INT GENERATED ALWAYS AS (YEAR(date_col))
    ) USING delta""")
    w.sql("INSERT INTO tbl (date_col, value) VALUES (DATE'2024-03-15', 1), (DATE'2024-11-20', 2)")
    w.sql("INSERT INTO tbl (date_col, value) VALUES (DATE'2025-01-05', 3)")
    val t = w.table("tbl")
    w.read(t, predicate = "year_col = 2024")
    w.read(t, predicate = "year_col = 2025")
    w.snapshot(t)
  }

  // DVs + data skipping

  test("ds_with_dvs_edge", "Data skipping + DVs combined", "dataSkipping", "dv") { w =>
    w.sql("""CREATE TABLE tbl (a INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(1, 11)")   // file 1: 1-10
    w.sql("INSERT INTO tbl SELECT id FROM range(11, 21)")  // file 2: 11-20
    w.sql("DELETE FROM tbl WHERE a = 5")  // DV on file 1
    val t = w.table("tbl")
    w.read(t, predicate = "a = 5", name = "read_deleted_row")
    w.read(t, predicate = "a > 15")
    w.read(t, predicate = "a = 1")
    w.snapshot(t)
  }

  test("ds_with_dvs_edge_1", "Data skipping + DVs: predicate skips file with DV",
      "dataSkipping", "dv") { w =>
    w.sql("""CREATE TABLE tbl (a INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(1, 11)")
    w.sql("INSERT INTO tbl SELECT id FROM range(11, 21)")
    w.sql("DELETE FROM tbl WHERE a <= 5")
    val t = w.table("tbl")
    w.read(t, predicate = "a > 10")
    w.read(t, predicate = "a <= 5", name = "read_all_deleted")
    w.snapshot(t)
  }

  test("ds_with_dvs_edge_2", "Data skipping + DVs: entire file deleted via DV",
      "dataSkipping", "dv") { w =>
    w.sql("""CREATE TABLE tbl (a INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(1, 6)")
    w.sql("INSERT INTO tbl SELECT id FROM range(6, 11)")
    w.sql("DELETE FROM tbl WHERE a <= 5")  // all of file 1 deleted
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "a > 5")
    w.snapshot(t)
  }

  // Column mapping: stats after drop/rename

  test("ds_stats_col_drop", "Data skipping after column drop (CM=name)",
      "dataSkipping", "columnMapping") { w =>
    w.sql("""CREATE TABLE tbl (a INT, b INT, c INT) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    w.sql("INSERT INTO tbl VALUES (1, 10, 100)")
    w.sql("ALTER TABLE tbl DROP COLUMN b")
    w.sql("INSERT INTO tbl VALUES (2, 200)")
    val t = w.table("tbl")
    w.read(t, predicate = "a = 1")
    w.read(t, predicate = "c = 100")
    w.snapshot(t)
  }

  test("ds_stats_col_rename", "Data skipping after column rename (CM=name)",
      "dataSkipping", "columnMapping") { w =>
    w.sql("""CREATE TABLE tbl (a INT, old_name INT) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    w.sql("INSERT INTO tbl VALUES (1, 100)")
    w.sql("ALTER TABLE tbl RENAME COLUMN old_name TO new_name")
    w.sql("INSERT INTO tbl VALUES (2, 200)")
    val t = w.table("tbl")
    w.read(t, predicate = "a = 1")
    w.read(t, predicate = "new_name > 150")
    w.snapshot(t)
  }

  test("ds_stats_after_drop", "Stats after column drop with column mapping",
      "dataSkipping", "columnMapping") { w =>
    w.sql("""CREATE TABLE tbl (
      c1 LONG, c2 STRING, c3 FLOAT, c4 DOUBLE,
      c5 TIMESTAMP, c6 TIMESTAMP_NTZ, c7 DATE,
      c8 BYTE, c9 SHORT, c10 DECIMAL(3,2)
    ) USING delta
    TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
      'delta.minReaderVersion' = '3', 'delta.minWriterVersion' = '7',
      'delta.feature.timestampNtz' = 'supported')""")
    w.sql("""INSERT INTO tbl VALUES (1, 'a', 1.5, 2.5,
      TIMESTAMP'2024-01-01 00:00:00', TIMESTAMP_NTZ'2024-01-01 00:00:00',
      DATE'2024-01-01', 1, 100, 1.23)""")
    w.sql("ALTER TABLE tbl DROP COLUMN c2")
    w.sql("ALTER TABLE tbl DROP COLUMN c8")
    w.sql("ALTER TABLE tbl DROP COLUMN c9")
    w.sql("""INSERT INTO tbl VALUES (2, 3.0, 4.5,
      TIMESTAMP'2024-06-15 00:00:00', TIMESTAMP_NTZ'2024-06-15 00:00:00',
      DATE'2024-06-15', 2.34)""")
    val t = w.table("tbl")
    w.read(t, predicate = "c1 = 1")
    w.read(t, predicate = "c1 > 1")
    w.read(t, predicate = "c3 < 2.0")
    w.read(t, predicate = "c4 > 3.0")
    w.read(t, predicate = "c5 = TIMESTAMP'2024-01-01 00:00:00'")
    w.read(t, predicate = "c6 > TIMESTAMP_NTZ'2024-03-01 00:00:00'")
    w.read(t, predicate = "c7 = DATE'2024-01-01'")
    w.read(t, predicate = "c10 > 2.00")
    w.read(t, predicate = "c1 > 100", name = "read_miss_c1")
    w.read(t, predicate = "c3 > 100.0", name = "read_miss_c3")
    w.read(t, predicate = "c7 > DATE'2025-01-01'", name = "read_miss_c7")
    w.read(t, predicate = "c10 > 9.99", name = "read_miss_c10")
    w.read(t, predicate = "c1 = 1 AND c10 = 1.23")
    w.snapshot(t)
  }

  test("ds_stats_after_rename", "Stats after column rename with column mapping",
      "dataSkipping", "columnMapping") { w =>
    w.sql("""CREATE TABLE tbl (
      c1 LONG, c2 STRING, c3 FLOAT, c4 DOUBLE,
      c5 TIMESTAMP, c6 TIMESTAMP_NTZ, c7 DATE,
      c8 BYTE, c9 SHORT, c10 DECIMAL(3,2)
    ) USING delta
    TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
      'delta.minReaderVersion' = '3', 'delta.minWriterVersion' = '7',
      'delta.feature.timestampNtz' = 'supported')""")
    w.sql("""INSERT INTO tbl VALUES (1, 'a', 1.5, 2.5,
      TIMESTAMP'2024-01-01 00:00:00', TIMESTAMP_NTZ'2024-01-01 00:00:00',
      DATE'2024-01-01', 1, 100, 1.23)""")
    w.sql("ALTER TABLE tbl RENAME COLUMN c2 TO renamed_c2")
    w.sql("ALTER TABLE tbl RENAME COLUMN c8 TO renamed_c8")
    w.sql("""INSERT INTO tbl VALUES (2, 'b', 3.0, 4.5,
      TIMESTAMP'2024-06-15 00:00:00', TIMESTAMP_NTZ'2024-06-15 00:00:00',
      DATE'2024-06-15', 2, 200, 2.34)""")
    val t = w.table("tbl")
    w.read(t, predicate = "c1 = 1")
    w.read(t, predicate = "c1 > 1")
    w.read(t, predicate = "renamed_c2 = 'a'")
    w.read(t, predicate = "c3 < 2.0")
    w.read(t, predicate = "c4 > 3.0")
    w.read(t, predicate = "c5 = TIMESTAMP'2024-01-01 00:00:00'")
    w.read(t, predicate = "c6 > TIMESTAMP_NTZ'2024-03-01 00:00:00'")
    w.read(t, predicate = "c7 = DATE'2024-01-01'")
    w.read(t, predicate = "renamed_c8 = 1")
    w.read(t, predicate = "c9 > 150")
    w.read(t, predicate = "c10 > 2.00")
    w.read(t, predicate = "c1 > 100", name = "read_miss_c1")
    w.read(t, predicate = "renamed_c2 = 'z'", name = "read_miss_renamed_c2")
    w.read(t, predicate = "c3 > 100.0", name = "read_miss_c3")
    w.read(t, predicate = "c7 > DATE'2025-01-01'", name = "read_miss_c7")
    w.read(t, predicate = "c10 > 9.99", name = "read_miss_c10")
    w.read(t, predicate = "c1 = 1 AND renamed_c2 = 'a'")
    w.read(t, predicate = "c1 = 1 AND c10 = 1.23")
    w.read(t, predicate = "c1 > 100 AND c10 > 9.99", name = "read_miss_c1_c10")
    w.snapshot(t)
  }

  // Error test: field not found

  test("ds_err_001_field_not_found", "FIELD_NOT_FOUND for row commit version filter",
      "dataSkipping", "error") { w =>
    w.sql("""CREATE TABLE tbl (a INT) USING delta
      TBLPROPERTIES ('delta.enableRowTracking' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1)")
    val t = w.table("tbl")
    w.read(t)
    w.snapshot(t)
  }

  // === Statistics ===

  test("stats_null_in_min_max", "Stats with all-null column (null min/max)", "data-skipping", "stats", "edge-case") { w =>
    w.sql("""CREATE TABLE tbl (id INT, nullable_col STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1, null),(2, null),(3, null)")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "nullable_col = 'a'")
    w.read(t, predicate = "nullable_col IS NULL")
    w.snapshot(t)
  }

  test("stats_numrecords_only", "Stats with only numRecords", "data-skipping", "stats", "edge-case") { w =>
    w.sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1, 'a'),(2, 'b')")
    val t = w.table("tbl")
    // Strip min/max stats, keep only numRecords
    w.modifyCommitActions(t, 0) { case ("add", node) =>
      if (node.has("stats")) {
        val stats = node.get("stats").asText()
        if (stats.contains("numRecords")) {
          import com.fasterxml.jackson.databind.ObjectMapper
          val mapper = new ObjectMapper()
          val statsNode = mapper.readTree(stats)
          val newStats = mapper.createObjectNode()
          newStats.set("numRecords", statsNode.get("numRecords"))
          node.put("stats", mapper.writeValueAsString(newStats))
        }
      }
      true
      case _ => true
    }
    w.snapshot(t)
  }

  test("stats_numrecords_with_dv", "numRecords is physical count, not logical (with DVs)", "data-skipping", "stats", "deletion-vectors", "edge-case") { w =>
    w.sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(10)")
    w.sql("DELETE FROM tbl WHERE id < 3")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "id >= 5")
    w.snapshot(t)
  }

  test("stats_partition_col_no_stats", "Partition column excluded from data statistics", "data-skipping", "stats", "partition", "edge-case") { w =>
    w.sql("""CREATE TABLE tbl (id INT, country STRING, amount INT) USING delta
      PARTITIONED BY (country) TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1, 'US', 100),(2, 'UK', 200),(3, 'US', 300),(4, 'UK', 400)")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "country = 'US'")
    w.read(t, predicate = "amount > 200")
    w.snapshot(t)
  }

  test("stats_string_truncation", "Stats with truncated string min/max (must not over-prune)", "data-skipping", "stats", "string-truncation", "edge-case") { w =>
    w.sql("""CREATE TABLE tbl (id INT, long_str STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    // Include a string longer than 32 chars to trigger truncation
    w.sql("""INSERT INTO tbl VALUES
      (1, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaxyz'),
      (2, 'short'),
      (3, 'medium_length_string')""")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "long_str = 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaxyz'")
    w.read(t, predicate = "long_str = 'short'")
    w.snapshot(t)
  }

  test("stats_empty_string", "Stats with empty string values", "data-skipping", "stats", "edge-case") { w =>
    w.sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1, ''),(2, 'a'),(3, '')")
    val t = w.table("tbl")
    // Strip stats completely to simulate empty stats string
    w.modifyCommitActions(t, 0) { case ("add", node) =>
      if (node.has("stats")) node.put("stats", "")
      true
      case _ => true
    }
    w.snapshot(t)
  }

  test("stats_missing_entirely", "Stats field missing entirely", "data-skipping", "stats", "edge-case") { w =>
    w.sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1, 'a'),(2, 'b')")
    val t = w.table("tbl")
    // Remove stats field entirely
    w.modifyCommitActions(t, 0) { case ("add", node) =>
      node.remove("stats"); true
      case _ => true
    }
    w.snapshot(t)
  }

  // === Partitioning ===

  test("single_partition", "Single partition column", "partitioned") { w =>
    w.sql("""CREATE TABLE tbl (id INT, region STRING, value DOUBLE)
      USING delta PARTITIONED BY (region)""")
    w.sql("INSERT INTO tbl VALUES (1,'us',10),(2,'us',20),(3,'eu',30),(4,'eu',40),(5,'asia',50)")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "region = 'us'")
    w.read(t, predicate = "region = 'eu'")
    w.read(t, predicate = "region = 'antarctica'")
    w.snapshot(t)
  }

  test("multi_partition", "Multiple partition columns", "partitioned") { w =>
    w.sql("""CREATE TABLE tbl (id INT, year INT, month INT, data STRING)
      USING delta PARTITIONED BY (year, month)""")
    w.sql("""INSERT INTO tbl VALUES
      (1,2024,1,'jan24'),(2,2024,2,'feb24'),(3,2024,3,'mar24'),
      (4,2025,1,'jan25'),(5,2025,2,'feb25')""")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "year = 2024")
    w.read(t, predicate = "year = 2025 AND month = 1")
    w.read(t, columns = Seq("id", "year"))
    w.snapshot(t)
  }

  test("null_partition", "NULL partition values", "partitioned", "nulls") { w =>
    w.sql("""CREATE TABLE tbl (id INT, category STRING, value INT)
      USING delta PARTITIONED BY (category)""")
    w.sql("INSERT INTO tbl VALUES (1,'a',10),(2,NULL,20),(3,'b',30),(4,NULL,40)")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "category IS NULL")
    w.read(t, predicate = "category IS NOT NULL")
    w.read(t, predicate = "category = 'a'")
    w.snapshot(t)
  }

  test("stats_skipping", "Data skipping via column stats", "skipping") { w =>
    w.sql("CREATE TABLE tbl (id INT, value INT) USING delta")
    w.sql("INSERT INTO tbl SELECT id, id * 10 FROM range(100) WHERE id < 50")
    w.sql("INSERT INTO tbl SELECT id, id * 10 FROM range(100) WHERE id >= 50")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "id < 25")
    w.read(t, predicate = "id >= 75")
    w.read(t, predicate = "id > 999")
    w.read(t, predicate = "id >= 0")
    w.snapshot(t)
  }

  test("partition_pruning", "Partition pruning + stats", "partitioned", "skipping") { w =>
    w.sql("""CREATE TABLE tbl (id INT, region STRING, amount DOUBLE)
      USING delta PARTITIONED BY (region)""")
    w.sql("INSERT INTO tbl VALUES (1,'us',10),(2,'us',20)")
    w.sql("INSERT INTO tbl VALUES (3,'eu',30),(4,'eu',40)")
    w.sql("INSERT INTO tbl VALUES (5,'asia',50)")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "region = 'us'")
    w.read(t, predicate = "region = 'us' AND amount > 15")
    w.read(t, predicate = "region = 'mars'")
    w.snapshot(t)
  }

  test("column_projection", "Column subsets", "projection") { w =>
    w.sql("CREATE TABLE tbl (a INT, b STRING, c DOUBLE, d BOOLEAN, e DATE) USING delta")
    w.sql("""INSERT INTO tbl VALUES
      (1,'x',1.1,true,DATE'2024-01-01'),
      (2,'y',2.2,false,DATE'2024-06-15'),
      (3,'z',3.3,true,DATE'2025-01-01')""")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, columns = Seq("a"))
    w.read(t, columns = Seq("b", "d"))
    w.read(t, columns = Seq("e", "c", "a"))
    w.read(t, predicate = "a > 1", columns = Seq("a", "b"))
    w.snapshot(t)
  }

  test("part_date_type", "Partition with date type", "partitioned") { w =>
    w.sql("""CREATE TABLE tbl (id INT, value STRING, dt DATE) USING delta
      PARTITIONED BY (dt)
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("""INSERT INTO tbl VALUES
      (1,'jan',DATE'2024-01-01'),(2,'jun',DATE'2024-06-01'),
      (3,'dec',DATE'2024-12-01'),(4,'jan2',DATE'2024-01-01')""")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "dt = DATE'2024-01-01'")
    w.read(t, predicate = "dt >= DATE'2024-06-01'")
    w.snapshot(t)
  }

  test("part_null_values", "Partition with NULL values", "partitioned") { w =>
    w.sql("""CREATE TABLE tbl (id INT, value STRING, part STRING) USING delta
      PARTITIONED BY (part)
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1,'a','x'),(2,'b',NULL),(3,'c','y'),(4,'d',NULL)")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "part IS NULL")
    w.read(t, predicate = "part IS NOT NULL")
    w.snapshot(t)
  }

  test("part_or_predicate", "Partition pruning with OR predicate", "partitioned") { w =>
    w.sql("""CREATE TABLE tbl (id INT, value STRING, part STRING) USING delta
      PARTITIONED BY (part)
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1,'a1','A'),(2,'a2','A'),(3,'b1','B'),(4,'c1','C'),(5,'c2','C')")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "part = 'A' OR part = 'C'")
    w.read(t, predicate = "part = 'B'")
    w.snapshot(t)
  }

  test("part_multi_column", "Multi-column partitioning with 3 columns", "partitioned") { w =>
    w.sql("""CREATE TABLE tbl (id INT, value STRING, a INT, b STRING, c BOOLEAN) USING delta
      PARTITIONED BY (a, b, c)
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("""INSERT INTO tbl VALUES
      (1,'v1',1,'x',true),(2,'v2',1,'y',false),
      (3,'v3',2,'y',true),(4,'v4',2,'z',false),
      (5,'v5',3,'z',true)""")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "a = 1")
    w.read(t, predicate = "a = 2 AND b = 'y'")
    w.read(t, predicate = "a = 3 AND b = 'z' AND c = true")
    w.snapshot(t)
  }

}.runAll()
