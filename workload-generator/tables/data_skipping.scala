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

  test("ds_top_level_single_1", "top level, single 1", "dataSkipping") {
    sql("CREATE TABLE tbl (a LONG) USING delta")
    sql("INSERT INTO tbl VALUES (1), (2)")
    val t = registerTable("tbl")
    // hits
    read(t, predicate = "a = 1")
    read(t, predicate = "a >= 1")
    read(t, predicate = "a <= 1")
    read(t, predicate = "a >= 0")
    read(t, predicate = "a <= 2")
    read(t, predicate = "0 <= a")
    read(t, predicate = "1 <= a")
    read(t, predicate = "1 >= a")
    read(t, predicate = "2 >= a")
    read(t, predicate = "1 = a")
    read(t, predicate = "a <=> 1")
    read(t, predicate = "1 <=> a")
    read(t, predicate = "NOT (a <=> 2)", name = "read_not_a_nse_2")
    read(t, predicate = "true", name = "read_true")
    // misses
    read(t, predicate = "NOT (a = 1)", name = "read_miss_not_a_eq_1")
    read(t, predicate = "NOT (a <=> 1)", name = "read_miss_not_a_nse_1")
    read(t, predicate = "a = 2", name = "read_miss_a_eq_2")
    read(t, predicate = "a <=> 2", name = "read_miss_a_nse_2")
    read(t, predicate = "a > 1", name = "read_miss_a_gt_1")
    read(t, predicate = "a >= 2", name = "read_miss_a_gte_2")
    read(t, predicate = "a <= 0", name = "read_miss_a_lte_0")
    read(t, predicate = "a = 0", name = "read_miss_a_eq_0")
    read(t, predicate = "a > 2", name = "read_miss_a_gt_2")
    read(t, predicate = "a < 1", name = "read_miss_a_lt_1")
    read(t, predicate = "a <> 1", name = "read_miss_a_neq_1")
    read(t, predicate = "1 != a", name = "read_miss_1_neq_a")
    read(t, predicate = "2 <=> a", name = "read_miss_2_nse_a")
    read(t, predicate = "0 >= a", name = "read_miss_0_gte_a")
    read(t, predicate = "0 = a", name = "read_miss_0_eq_a")
    read(t, predicate = "1 > a", name = "read_miss_1_gt_a")
    read(t, predicate = "1 < a", name = "read_miss_1_lt_a")
    read(t, predicate = "0 > a", name = "read_miss_0_gt_a")
    read(t, predicate = "2 = a", name = "read_miss_2_eq_a")
    read(t, predicate = "2 <= a", name = "read_miss_2_lte_a")
    read(t, predicate = "0 < a AND a < 1", name = "read_miss_between_0_1")
    snapshot(t)
  }

  // Nested field predicates

  test("ds_nested_single_1", "nested, single 1", "dataSkipping") {
    sql("CREATE TABLE tbl (a STRUCT<b: LONG>) USING delta")
    sql("INSERT INTO tbl VALUES (named_struct('b', 1))")
    val t = registerTable("tbl")
    read(t, predicate = "a.b = 1")
    read(t, predicate = "a.b >= 0")
    read(t, predicate = "a.b >= 1")
    read(t, predicate = "a.b <= 1")
    read(t, predicate = "a.b <= 2")
    read(t, predicate = "a.b = 2", name = "read_miss_ab_eq_2")
    read(t, predicate = "a.b > 1", name = "read_miss_ab_gt_1")
    read(t, predicate = "a.b < 1", name = "read_miss_ab_lt_1")
    snapshot(t)
  }

  test("ds_double_nested_single_1", "double nested, single 1", "dataSkipping") {
    sql("CREATE TABLE tbl (a STRUCT<b: STRUCT<c: LONG>>) USING delta")
    sql("INSERT INTO tbl VALUES (named_struct('b', named_struct('c', 1)))")
    val t = registerTable("tbl")
    read(t, predicate = "a.b.c = 1")
    read(t, predicate = "a.b.c >= 0")
    read(t, predicate = "a.b.c >= 1")
    read(t, predicate = "a.b.c <= 1")
    read(t, predicate = "a.b.c <= 2")
    read(t, predicate = "a.b.c = 2", name = "read_miss_abc_eq_2")
    read(t, predicate = "a.b.c > 1", name = "read_miss_abc_gt_1")
    read(t, predicate = "a.b.c < 1", name = "read_miss_abc_lt_1")
    snapshot(t)
  }

  test("ds_nested_struct_predicate", "Nested struct field predicate", "dataSkipping") {
    sql("CREATE TABLE tbl (id INT, info STRUCT<score: INT, name: STRING>) USING delta")
    sql("INSERT INTO tbl VALUES (1, named_struct('score', 90, 'name', 'alice'))")
    sql("INSERT INTO tbl VALUES (2, named_struct('score', 50, 'name', 'bob'))")
    val t = registerTable("tbl")
    read(t, predicate = "info.score > 80")
    read(t, predicate = "info.score < 40", name = "read_miss_low_score")
    snapshot(t)
  }

  test("ds_complex_nested", "Complex nested predicates", "dataSkipping") {
    sql("CREATE TABLE tbl (a INT, b STRUCT<x: INT, y: INT>) USING delta")
    sql("INSERT INTO tbl VALUES (1, named_struct('x', 10, 'y', 20))")
    sql("INSERT INTO tbl VALUES (2, named_struct('x', 30, 'y', 40))")
    val t = registerTable("tbl")
    read(t, predicate = "a = 1 AND b.x = 10")
    read(t, predicate = "b.x > 20 OR b.y < 25")
    read(t, predicate = "a > 5 AND b.x > 50", name = "read_miss_complex")
    snapshot(t)
  }

  // AND / OR / NOT combinations

  test("ds_and_simple", "and statements - simple", "dataSkipping") {
    sql("CREATE TABLE tbl (a LONG) USING delta")
    sql("INSERT INTO tbl VALUES (1), (2)")
    val t = registerTable("tbl")
    read(t, predicate = "a <= 1 AND a > -1", name = "read_hit_and_bound")
    read(t, predicate = "a >= 1 AND a <= 2", name = "read_hit_and_range")
    read(t, predicate = "a > 5 AND a < 10", name = "read_miss_and_outside")
    snapshot(t)
  }

  test("ds_and_two_fields", "and statements - two fields", "dataSkipping") {
    sql("CREATE TABLE tbl (a LONG, b LONG) USING delta")
    sql("INSERT INTO tbl VALUES (1, 10), (2, 20)")
    val t = registerTable("tbl")
    read(t, predicate = "a = 1 AND b = 10")
    read(t, predicate = "a >= 1 AND b <= 20")
    read(t, predicate = "a = 1 AND b > 100", name = "read_miss_b_out")
    read(t, predicate = "a > 5 AND b > 5", name = "read_miss_both_out")
    snapshot(t)
  }

  test("ds_and_one_side_unsupported", "AND one side unsupported", "dataSkipping") {
    sql("CREATE TABLE tbl (a LONG) USING delta")
    sql("INSERT INTO tbl VALUES (1), (2)")
    val t = registerTable("tbl")
    read(t, predicate = "a = 1 AND CAST(a AS STRING) LIKE '%1'")
    read(t, predicate = "a > 5 AND CAST(a AS STRING) LIKE '%x'", name = "read_miss_and_unsupported")
    snapshot(t)
  }

  test("ds_or_simple", "or statements - simple", "dataSkipping") {
    sql("CREATE TABLE tbl (a LONG) USING delta")
    sql("INSERT INTO tbl VALUES (1), (2)")
    val t = registerTable("tbl")
    read(t, predicate = "a = 1 OR a = 3")
    read(t, predicate = "a < 0 OR a > 0")
    read(t, predicate = "a = 5 OR a = 6", name = "read_miss_or")
    snapshot(t)
  }

  test("ds_or_two_fields", "or statements - two fields", "dataSkipping") {
    sql("CREATE TABLE tbl (a LONG, b LONG) USING delta")
    sql("INSERT INTO tbl VALUES (1, 10), (2, 20)")
    val t = registerTable("tbl")
    read(t, predicate = "a = 1 OR b = 20")
    read(t, predicate = "a = 5 OR b = 10")
    read(t, predicate = "a > 0 OR b > 0")
    read(t, predicate = "a = 5 OR b = 50", name = "read_miss_or_both")
    snapshot(t)
  }

  test("ds_or_one_side_unsupported", "OR one side unsupported", "dataSkipping") {
    sql("CREATE TABLE tbl (a LONG) USING delta")
    sql("INSERT INTO tbl VALUES (1), (2)")
    val t = registerTable("tbl")
    // OR with unsupported side forces full scan
    read(t, predicate = "a = 1 OR CAST(a AS STRING) LIKE '%x'")
    read(t, predicate = "a > 5 OR CAST(a AS STRING) LIKE '%1'")
    snapshot(t)
  }

  test("ds_not_simple", "not statements - simple", "dataSkipping") {
    sql("CREATE TABLE tbl (a LONG) USING delta")
    sql("INSERT INTO tbl VALUES (1), (2)")
    val t = registerTable("tbl")
    read(t, predicate = "NOT (a > 5)")
    read(t, predicate = "NOT (a < 0)", name = "read_not_lt_0")
    snapshot(t)
  }

  test("ds_not_and", "NOT with AND (De Morgan)", "dataSkipping") {
    sql("CREATE TABLE tbl (a LONG) USING delta")
    sql("INSERT INTO tbl VALUES (1), (2)")
    val t = registerTable("tbl")
    read(t, predicate = "NOT (a > 5 AND a < 10)")
    read(t, predicate = "NOT (a > 0 AND a < 3)")
    read(t, predicate = "NOT (a = 1 AND a = 2)", name = "read_not_and_contra")
    snapshot(t)
  }

  test("ds_not_or", "NOT with OR (De Morgan)", "dataSkipping") {
    sql("CREATE TABLE tbl (a LONG) USING delta")
    sql("INSERT INTO tbl VALUES (1), (2)")
    val t = registerTable("tbl")
    read(t, predicate = "NOT (a > 5 OR a < -5)")
    read(t, predicate = "NOT (a = 1 OR a = 2)", name = "read_not_or_all")
    snapshot(t)
  }

  // LIKE / starts with

  test("ds_starts_with", "starts with", "dataSkipping") {
    sql("CREATE TABLE tbl (a STRING) USING delta")
    sql("INSERT INTO tbl VALUES ('apple'), ('banana')")
    val t = registerTable("tbl")
    read(t, predicate = "a LIKE 'a%'")
    read(t, predicate = "a LIKE 'b%'")
    read(t, predicate = "a LIKE 'app%'")
    read(t, predicate = "a LIKE 'z%'", name = "read_miss_z")
    read(t, predicate = "a LIKE 'c%'", name = "read_miss_c")
    snapshot(t)
  }

  test("ds_starts_with_nested", "LIKE on nested string fields", "dataSkipping") {
    sql("CREATE TABLE tbl (a STRUCT<b: STRING>) USING delta")
    sql("INSERT INTO tbl VALUES (named_struct('b', 'apple'))")
    sql("INSERT INTO tbl VALUES (named_struct('b', 'banana'))")
    val t = registerTable("tbl")
    read(t, predicate = "a.b LIKE 'a%'")
    read(t, predicate = "a.b LIKE 'b%'")
    read(t, predicate = "a.b LIKE 'app%'")
    read(t, predicate = "a.b LIKE 'z%'", name = "read_miss_z")
    read(t, predicate = "a.b LIKE 'c%'", name = "read_miss_c")
    snapshot(t)
  }

  test("ds_string_patterns", "String comparisons and LIKE", "dataSkipping") {
    sql("CREATE TABLE tbl (name STRING) USING delta")
    sql("INSERT INTO tbl VALUES ('alice'), ('bob')")
    sql("INSERT INTO tbl VALUES ('charlie'), ('diana')")
    val t = registerTable("tbl")
    read(t, predicate = "name = 'alice'")
    read(t, predicate = "name >= 'c'")
    read(t, predicate = "name < 'b'")
    read(t, predicate = "name LIKE 'a%'")
    read(t, predicate = "name LIKE 'ch%'")
    read(t, predicate = "name LIKE 'z%'", name = "read_miss_z")
    read(t, predicate = "name > 'e'", name = "read_miss_gt_e")
    read(t, predicate = "name LIKE 'x%'", name = "read_miss_x")
    snapshot(t)
  }

  // Long strings (prefix truncation edge cases)

  test("ds_long_strings_min", "long strings, long min", "dataSkipping") {
    sql("CREATE TABLE tbl (a STRING) USING delta")
    // 33-char prefix: "aaa...a" then differ
    val longA = "a" * 32 + "x"
    val longB = "a" * 32 + "y"
    sql(s"INSERT INTO tbl VALUES ('$longA'), ('$longB')")
    val t = registerTable("tbl")
    read(t, predicate = s"a = '$longA'")
    read(t, predicate = s"a >= '${"a" * 32}'")
    read(t, predicate = "a LIKE 'aaa%'")
    read(t, predicate = "a = 'z'", name = "read_miss_z")
    read(t, predicate = "a < 'a'", name = "read_miss_lt_a")
    snapshot(t)
  }

  test("ds_long_strings_max", "long strings, long max", "dataSkipping") {
    sql("CREATE TABLE tbl (a STRING) USING delta")
    val longZ = "z" * 32 + "a"
    val longY = "z" * 32 + "b"
    sql(s"INSERT INTO tbl VALUES ('$longZ'), ('$longY')")
    val t = registerTable("tbl")
    read(t, predicate = s"a = '$longZ'")
    read(t, predicate = s"a >= '${"z" * 32}'")
    read(t, predicate = "a LIKE 'zzz%'")
    read(t, predicate = s"a <= '${"z" * 33}'")
    read(t, predicate = "a = 'a'", name = "read_miss_a")
    read(t, predicate = "a < 'z'", name = "read_miss_lt_z")
    read(t, predicate = "a > 'zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz'", name = "read_miss_gt_long_z")
    snapshot(t)
  }

  // IN predicates

  test("ds_in_set", "IN set predicates", "dataSkipping") {
    sql("CREATE TABLE tbl (a INT) USING delta")
    sql("INSERT INTO tbl VALUES (1), (2), (3)")
    val t = registerTable("tbl")
    read(t, predicate = "a IN (1, 2)")
    read(t, predicate = "a IN (3)")
    read(t, predicate = "a IN (10, 20)", name = "read_miss_in")
    snapshot(t)
  }

  test("ds_in_list", "IN list predicate", "dataSkipping") {
    sql("CREATE TABLE tbl (a INT) USING delta")
    sql("INSERT INTO tbl VALUES (10), (20)")
    sql("INSERT INTO tbl VALUES (30), (40)")
    val t = registerTable("tbl")
    read(t, predicate = "a IN (10, 30)")
    read(t, predicate = "a IN (99)", name = "read_miss_in_99")
    snapshot(t)
  }

  test("ds_in_nested", "IN on nested field", "dataSkipping") {
    sql("CREATE TABLE tbl (s STRUCT<x: INT>) USING delta")
    sql("INSERT INTO tbl VALUES (named_struct('x', 1))")
    sql("INSERT INTO tbl VALUES (named_struct('x', 5))")
    val t = registerTable("tbl")
    read(t, predicate = "s.x IN (1, 5)")
    read(t, predicate = "s.x IN (99)", name = "read_miss_nested_in")
    snapshot(t)
  }

  test("ds_in_with_nulls_mixed", "IN list containing NULL", "dataSkipping") {
    sql("CREATE TABLE tbl (a INT) USING delta")
    sql("INSERT INTO tbl VALUES (1), (NULL), (3)")
    val t = registerTable("tbl")
    read(t, predicate = "a IN (1, NULL)")
    read(t, predicate = "a IN (99, NULL)", name = "read_in_null_miss")
    snapshot(t)
  }

  test("ds_in_with_nulls_only", "IN list with only nulls in file", "dataSkipping") {
    sql("CREATE TABLE tbl (a INT) USING delta")
    sql("INSERT INTO tbl VALUES (NULL), (NULL)")
    val t = registerTable("tbl")
    read(t, predicate = "a IN (1)")
    read(t, predicate = "a IN (NULL)")
    snapshot(t)
  }

  test("ds_in_with_thresholds", "IN with varying size", "dataSkipping") {
    sql("CREATE TABLE tbl (a INT) USING delta")
    sql("INSERT INTO tbl SELECT id FROM range(1, 11)")
    val t = registerTable("tbl")
    // Small IN list
    read(t, predicate = "a IN (1, 2, 3)")
    // Larger IN list (may exceed threshold and become range)
    read(t, predicate = "a IN (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20)",
      name = "read_in_large")
    read(t, predicate = "a IN (100, 200)", name = "read_miss_in_large")
    snapshot(t)
  }

  test("ds_not_in", "NOT IN predicate", "dataSkipping") {
    sql("CREATE TABLE tbl (a INT) USING delta")
    sql("INSERT INTO tbl VALUES (1), (2), (3)")
    val t = registerTable("tbl")
    read(t, predicate = "a NOT IN (4, 5)")
    read(t, predicate = "a NOT IN (1, 2, 3)", name = "read_not_in_all")
    read(t, predicate = "a NOT IN (1)")
    snapshot(t)
  }

  // NULL predicates

  test("ds_is_null", "IS NULL predicate pushdown", "dataSkipping") {
    sql("CREATE TABLE tbl (a INT) USING delta")
    sql("INSERT INTO tbl VALUES (1), (NULL), (3)")
    val t = registerTable("tbl")
    read(t, predicate = "a IS NULL")
    snapshot(t)
  }

  test("ds_is_not_null", "IS NOT NULL predicate pushdown", "dataSkipping") {
    sql("CREATE TABLE tbl (a INT) USING delta")
    sql("INSERT INTO tbl VALUES (1), (NULL), (3)")
    val t = registerTable("tbl")
    read(t, predicate = "a IS NOT NULL")
    snapshot(t)
  }

  test("ds_isnull_complex_expr", "IS NULL with complex expressions", "dataSkipping") {
    sql("CREATE TABLE tbl (a INT, b STRING) USING delta")
    sql("INSERT INTO tbl VALUES (1, 'x'), (NULL, NULL)")
    sql("INSERT INTO tbl VALUES (3, 'y'), (NULL, 'z')")
    val t = registerTable("tbl")
    read(t, predicate = "a IS NULL AND b IS NULL")
    read(t, predicate = "a IS NULL OR b IS NULL")
    read(t, predicate = "a IS NOT NULL AND b IS NOT NULL")
    snapshot(t)
  }

  test("ds_nulls_only_null", "nulls - only null in file", "dataSkipping") {
    sql("CREATE TABLE tbl (a LONG) USING delta")
    sql("INSERT INTO tbl VALUES (NULL)")
    val t = registerTable("tbl")
    read(t, predicate = "a IS NULL")
    read(t, predicate = "a IS NOT NULL", name = "read_is_not_null")
    read(t, predicate = "a = 1", name = "read_eq_1")
    read(t, predicate = "a > 0", name = "read_gt_0")
    read(t, predicate = "a < 0", name = "read_lt_0")
    read(t, predicate = "a >= 0", name = "read_gte_0")
    read(t, predicate = "a <= 0", name = "read_lte_0")
    read(t, predicate = "a <=> NULL", name = "read_nse_null")
    read(t, predicate = "a <=> 1", name = "read_nse_1")
    read(t, predicate = "a IN (1, 2)", name = "read_in_1_2")
    read(t, predicate = "NOT (a = 1)", name = "read_not_eq_1")
    read(t, predicate = "NOT (a IS NULL)", name = "read_not_is_null")
    read(t, predicate = "NOT (a IS NOT NULL)", name = "read_not_is_not_null")
    read(t, predicate = "a = 1 OR a IS NULL", name = "read_eq_or_null")
    read(t, predicate = "a = 1 AND a IS NULL", name = "read_eq_and_null")
    read(t, predicate = "a LIKE 'x%'", name = "read_like_x")
    snapshot(t)
  }

  test("ds_nulls_only_nonnull", "only non-null in file", "dataSkipping") {
    sql("CREATE TABLE tbl (a LONG) USING delta")
    sql("INSERT INTO tbl VALUES (1)")
    val t = registerTable("tbl")
    read(t, predicate = "a IS NULL", name = "read_is_null")
    read(t, predicate = "a IS NOT NULL")
    snapshot(t)
  }

  test("ds_nulls_mixed", "nulls - null + not-null in same file", "dataSkipping") {
    sql("CREATE TABLE tbl (a LONG) USING delta")
    sql("INSERT INTO tbl VALUES (1), (NULL), (3)")
    val t = registerTable("tbl")
    read(t, predicate = "a IS NULL")
    read(t, predicate = "a IS NOT NULL")
    read(t, predicate = "a = 1")
    read(t, predicate = "a > 2")
    read(t, predicate = "a < 2")
    read(t, predicate = "a >= 1")
    read(t, predicate = "a <= 3")
    read(t, predicate = "a <=> NULL", name = "read_nse_null")
    read(t, predicate = "a <=> 1", name = "read_nse_1")
    read(t, predicate = "a IN (1, 3)")
    read(t, predicate = "a IN (5)", name = "read_in_miss_5")
    read(t, predicate = "a = 1 OR a IS NULL")
    read(t, predicate = "a = 1 AND a IS NOT NULL")
    read(t, predicate = "NOT (a = 1)")
    read(t, predicate = "NOT (a IS NULL)", name = "read_not_is_null")
    read(t, predicate = "a > 5", name = "read_miss_gt_5")
    read(t, predicate = "a < 0", name = "read_miss_lt_0")
    snapshot(t)
  }

  test("ds_nulls_nonnulls_only", "non-nulls only in file", "dataSkipping") {
    sql("CREATE TABLE tbl (a LONG) USING delta")
    sql("INSERT INTO tbl VALUES (1), (2), (3)")
    val t = registerTable("tbl")
    read(t, predicate = "a IS NULL")
    read(t, predicate = "a IS NOT NULL")
    read(t, predicate = "a = 2")
    read(t, predicate = "a > 5", name = "read_miss_gt_5")
    snapshot(t)
  }

  test("ds_nulls_partial_stats", "non-nulls only, partial stats", "dataSkipping") {
    sql("""CREATE TABLE tbl (a LONG, b STRING) USING delta
      TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = '1')""")
    sql("INSERT INTO tbl VALUES (1, 'x'), (2, 'y'), (3, 'z')")
    val t = registerTable("tbl")
    // a has stats, b does not
    read(t, predicate = "a = 1")
    read(t, predicate = "a > 5", name = "read_miss_a_gt_5")
    read(t, predicate = "b = 'x'")
    read(t, predicate = "b = 'nonexistent'", name = "read_b_no_stats")
    read(t, predicate = "a = 1 AND b = 'x'")
    read(t, predicate = "a > 5 AND b = 'x'", name = "read_miss_a_has_stats")
    read(t, predicate = "a = 1 OR b = 'nonexistent'")
    read(t, predicate = "a IS NULL", name = "read_a_is_null")
    snapshot(t)
  }

  test("ds_null_safe_eq", "Null-safe equals operator (<=>)", "dataSkipping") {
    sql("CREATE TABLE tbl (a INT) USING delta")
    sql("INSERT INTO tbl VALUES (1), (NULL), (3)")
    val t = registerTable("tbl")
    read(t, predicate = "a <=> 1")
    read(t, predicate = "a <=> NULL")
    read(t, predicate = "a <=> 3")
    read(t, predicate = "a <=> 99", name = "read_miss_nse_99")
    snapshot(t)
  }

  test("ds_null_string_partition", "Null string partition values", "dataSkipping") {
    sql("CREATE TABLE tbl (id INT, part STRING) USING delta PARTITIONED BY (part)")
    sql("INSERT INTO tbl VALUES (1, 'a'), (2, NULL), (3, 'b')")
    val t = registerTable("tbl")
    read(t, predicate = "part IS NULL")
    read(t, predicate = "part = 'a'")
    snapshot(t)
  }

  test("ds_null_mixed_partitions", "Mix of null and non-null across partitions",
      "dataSkipping") {
    sql("""CREATE TABLE tbl (id INT, p1 STRING, p2 INT) USING delta
      PARTITIONED BY (p1, p2)""")
    sql("INSERT INTO tbl VALUES (1, 'a', 1), (2, NULL, 2), (3, 'b', NULL), (4, NULL, NULL)")
    val t = registerTable("tbl")
    read(t, predicate = "p1 IS NULL")
    read(t, predicate = "p2 IS NULL")
    read(t, predicate = "p1 IS NULL AND p2 IS NULL")
    snapshot(t)
  }

  // BETWEEN

  test("ds_between", "BETWEEN predicate for range queries", "dataSkipping") {
    sql("CREATE TABLE tbl (a INT) USING delta")
    sql("INSERT INTO tbl VALUES (1), (5), (10)")
    sql("INSERT INTO tbl VALUES (15), (20), (25)")
    val t = registerTable("tbl")
    read(t, predicate = "a BETWEEN 1 AND 10")
    read(t, predicate = "a BETWEEN 5 AND 20")
    read(t, predicate = "a BETWEEN 1 AND 25", name = "read_between_all")
    read(t, predicate = "a BETWEEN 50 AND 100", name = "read_miss_between")
    snapshot(t)
  }

  // Boolean column

  test("ds_boolean", "boolean comparisons", "dataSkipping") {
    sql("CREATE TABLE tbl (a BOOLEAN) USING delta")
    sql("INSERT INTO tbl VALUES (true)")
    sql("INSERT INTO tbl VALUES (false)")
    val t = registerTable("tbl")
    read(t, predicate = "a = true")
    read(t, predicate = "a = false")
    read(t, predicate = "a IS NOT NULL")
    read(t, predicate = "a IS NULL", name = "read_miss_null")
    snapshot(t)
  }

  test("ds_boolean_column", "Boolean column skipping", "dataSkipping") {
    sql("CREATE TABLE tbl (id INT, active BOOLEAN) USING delta")
    sql("INSERT INTO tbl VALUES (1, true), (2, true)")
    sql("INSERT INTO tbl VALUES (3, false), (4, false)")
    val t = registerTable("tbl")
    read(t, predicate = "active = true")
    read(t, predicate = "active = false")
    snapshot(t)
  }

  // Numeric types

  test("ds_numeric_types", "Numeric type comparisons", "dataSkipping") {
    sql("CREATE TABLE tbl (i INT, l LONG, f FLOAT, d DOUBLE) USING delta")
    sql("INSERT INTO tbl VALUES (1, 100, 1.5, 2.5)")
    sql("INSERT INTO tbl VALUES (10, 1000, 10.5, 20.5)")
    val t = registerTable("tbl")
    read(t, predicate = "i = 1")
    read(t, predicate = "l > 500")
    read(t, predicate = "f < 2.0")
    read(t, predicate = "d >= 20.0")
    read(t, predicate = "i > 100", name = "read_miss_i")
    read(t, predicate = "d < 1.0", name = "read_miss_d")
    snapshot(t)
  }

  test("ds_tinyint_smallint", "TINYINT and SMALLINT column stats", "dataSkipping") {
    sql("CREATE TABLE tbl (t TINYINT, s SMALLINT) USING delta")
    sql("INSERT INTO tbl VALUES (1, 100)")
    sql("INSERT INTO tbl VALUES (127, 32767)")
    val t = registerTable("tbl")
    read(t, predicate = "t = 1")
    read(t, predicate = "s > 30000")
    read(t, predicate = "t > 127", name = "read_miss_t")
    snapshot(t)
  }

  test("ds_float_special_values", "FLOAT with NaN, Infinity, -0.0", "dataSkipping") {
    sql("CREATE TABLE tbl (f FLOAT) USING delta")
    sql("INSERT INTO tbl VALUES (CAST('NaN' AS FLOAT)), (CAST('Infinity' AS FLOAT)), (CAST('-0.0' AS FLOAT))")
    val t = registerTable("tbl")
    read(t, predicate = "f > 0")
    read(t, predicate = "f IS NOT NULL")
    snapshot(t)
  }

  test("ds_binary_type", "BINARY column (no min/max, only nullCount)", "dataSkipping") {
    sql("CREATE TABLE tbl (id INT, data BINARY) USING delta")
    sql("INSERT INTO tbl VALUES (1, X'0102'), (2, NULL)")
    val t = registerTable("tbl")
    read(t, predicate = "data IS NOT NULL")
    read(t, predicate = "data IS NULL")
    snapshot(t)
  }

  test("ds_implicit_cast", "Implicit type cast in predicate", "dataSkipping") {
    sql("CREATE TABLE tbl (a LONG) USING delta")
    sql("INSERT INTO tbl VALUES (1), (2)")
    val t = registerTable("tbl")
    // int literal vs long column
    read(t, predicate = "a = 1")
    read(t, predicate = "a > 0")
    snapshot(t)
  }

  // Date/time predicates

  test("ds_datetime", "Date and timestamp predicates", "dataSkipping") {
    sql("CREATE TABLE tbl (d DATE, ts TIMESTAMP) USING delta")
    sql("INSERT INTO tbl VALUES (DATE'2024-01-01', TIMESTAMP'2024-01-01 00:00:00')")
    sql("INSERT INTO tbl VALUES (DATE'2024-06-15', TIMESTAMP'2024-06-15 12:00:00')")
    val t = registerTable("tbl")
    read(t, predicate = "d = DATE'2024-01-01'")
    read(t, predicate = "d > DATE'2024-03-01'")
    read(t, predicate = "ts < TIMESTAMP'2024-03-01 00:00:00'")
    read(t, predicate = "d > DATE'2025-01-01'", name = "read_miss_future")
    snapshot(t)
  }

  test("ds_timestamp_microsecond", "Microsecond precision timestamp skipping",
      "dataSkipping") {
    sql("CREATE TABLE tbl (ts TIMESTAMP) USING delta")
    sql("INSERT INTO tbl VALUES (TIMESTAMP'2024-01-01 00:00:00.000001')")
    sql("INSERT INTO tbl VALUES (TIMESTAMP'2024-01-01 00:00:00.000002')")
    val t = registerTable("tbl")
    read(t, predicate = "ts = TIMESTAMP'2024-01-01 00:00:00.000001'")
    read(t, predicate = "ts > TIMESTAMP'2024-01-01 00:00:00.000002'",
      name = "read_miss_after")
    snapshot(t)
  }

  test("ds_timestamp_ntz_skipping", "NTZ-specific data skipping", "dataSkipping") {
    sql("""CREATE TABLE tbl (ts TIMESTAMP_NTZ) USING delta
      TBLPROPERTIES ('delta.minReaderVersion' = '3', 'delta.minWriterVersion' = '7',
        'delta.feature.timestampNtz' = 'supported')""")
    sql("INSERT INTO tbl VALUES (TIMESTAMP_NTZ'2024-01-01 00:00:00')")
    sql("INSERT INTO tbl VALUES (TIMESTAMP_NTZ'2024-06-15 12:00:00')")
    val t = registerTable("tbl")
    read(t, predicate = "ts = TIMESTAMP_NTZ'2024-01-01 00:00:00'")
    read(t, predicate = "ts > TIMESTAMP_NTZ'2025-01-01 00:00:00'",
      name = "read_miss_future_ntz")
    snapshot(t)
  }

  test("ds_year_function", "year() extraction on Date", "dataSkipping") {
    sql("CREATE TABLE tbl (d DATE, value INT) USING delta")
    sql("INSERT INTO tbl VALUES (DATE'2024-03-15', 1), (DATE'2024-11-20', 2)")
    sql("INSERT INTO tbl VALUES (DATE'2025-01-05', 3)")
    val t = registerTable("tbl")
    read(t, predicate = "year(d) = 2024")
    read(t, predicate = "year(d) = 2025")
    read(t, predicate = "year(d) = 2020", name = "read_miss_year")
    snapshot(t)
  }

  test("ds_month_function", "month() with predicates", "dataSkipping") {
    sql("CREATE TABLE tbl (d DATE, value INT) USING delta")
    sql("INSERT INTO tbl VALUES (DATE'2024-01-15', 1), (DATE'2024-06-20', 2)")
    sql("INSERT INTO tbl VALUES (DATE'2024-12-05', 3)")
    val t = registerTable("tbl")
    read(t, predicate = "month(d) = 1")
    read(t, predicate = "month(d) = 6")
    read(t, predicate = "month(d) = 8", name = "read_miss_month")
    snapshot(t)
  }

  test("ds_trunc_date", "trunc() on Date", "dataSkipping") {
    sql("CREATE TABLE tbl (d DATE) USING delta")
    sql("INSERT INTO tbl VALUES (DATE'2024-03-15'), (DATE'2024-03-20')")
    sql("INSERT INTO tbl VALUES (DATE'2024-06-01'), (DATE'2024-06-30')")
    val t = registerTable("tbl")
    read(t, predicate = "trunc(d, 'MONTH') = DATE'2024-03-01'")
    read(t, predicate = "trunc(d, 'MONTH') = DATE'2024-06-01'")
    read(t, predicate = "trunc(d, 'YEAR') = DATE'2025-01-01'", name = "read_miss_trunc")
    snapshot(t)
  }

  test("ds_date_trunc_timestamp", "date_trunc() on Timestamp", "dataSkipping") {
    sql("CREATE TABLE tbl (ts TIMESTAMP) USING delta")
    sql("INSERT INTO tbl VALUES (TIMESTAMP'2024-03-15 10:30:00')")
    sql("INSERT INTO tbl VALUES (TIMESTAMP'2024-06-01 14:00:00')")
    val t = registerTable("tbl")
    read(t, predicate = "date_trunc('MONTH', ts) = TIMESTAMP'2024-03-01 00:00:00'")
    read(t, predicate = "date_trunc('YEAR', ts) = TIMESTAMP'2025-01-01 00:00:00'",
      name = "read_miss_trunc_ts")
    snapshot(t)
  }

  test("ds_datediff", "datediff() predicate", "dataSkipping") {
    sql("CREATE TABLE tbl (d DATE) USING delta")
    sql("INSERT INTO tbl VALUES (DATE'2024-01-01'), (DATE'2024-01-10')")
    sql("INSERT INTO tbl VALUES (DATE'2024-06-01')")
    val t = registerTable("tbl")
    read(t, predicate = "datediff(d, DATE'2024-01-01') <= 10")
    read(t, predicate = "datediff(d, DATE'2024-01-01') > 100")
    snapshot(t)
  }

  test("ds_date_add_sub", "date_add()/date_sub()", "dataSkipping") {
    sql("CREATE TABLE tbl (d DATE) USING delta")
    sql("INSERT INTO tbl VALUES (DATE'2024-01-01'), (DATE'2024-01-15')")
    sql("INSERT INTO tbl VALUES (DATE'2024-06-01')")
    val t = registerTable("tbl")
    read(t, predicate = "d >= date_add(DATE'2024-01-01', -1)")
    read(t, predicate = "d <= date_sub(DATE'2024-01-01', 10)",
      name = "read_miss_date_sub")
    snapshot(t)
  }

  // Multi-file range skipping

  test("ds_multi_file_ranges", "Multiple files with different ranges", "dataSkipping") {
    sql("CREATE TABLE tbl (a INT) USING delta")
    sql("INSERT INTO tbl SELECT id FROM range(1, 11)")   // file 1: 1-10
    sql("INSERT INTO tbl SELECT id FROM range(11, 21)")  // file 2: 11-20
    sql("INSERT INTO tbl SELECT id FROM range(21, 31)")  // file 3: 21-30
    val t = registerTable("tbl")
    read(t, name = "read_full_scan")
    read(t, predicate = "a <= 10", name = "read_hit_file1_only")
    read(t, predicate = "a > 10 AND a <= 20", name = "read_hit_file2_only")
    read(t, predicate = "a > 20", name = "read_hit_file3_only")
    read(t, predicate = "a <= 15", name = "read_hit_file1_and_2")
    read(t, predicate = "a > 15", name = "read_hit_file2_and_3")
    read(t, predicate = "a > 100", name = "read_miss_all_gt_100")
    read(t, predicate = "a < 0", name = "read_miss_all_lt_0")
    snapshot(t)
  }

  test("ds_multi_file_time", "TIME type multi-file data skipping", "dataSkipping") {
    sql("CREATE TABLE tbl (ts TIMESTAMP, value INT) USING delta")
    sql("INSERT INTO tbl VALUES (TIMESTAMP'2024-01-01 00:00:00', 1), (TIMESTAMP'2024-01-15 00:00:00', 2)")
    sql("INSERT INTO tbl VALUES (TIMESTAMP'2024-06-01 00:00:00', 3), (TIMESTAMP'2024-12-31 00:00:00', 4)")
    val t = registerTable("tbl")
    read(t, predicate = "ts < TIMESTAMP'2024-02-01 00:00:00'")
    read(t, predicate = "ts >= TIMESTAMP'2024-06-01 00:00:00'")
    read(t, predicate = "ts > TIMESTAMP'2025-01-01 00:00:00'", name = "read_miss_future")
    snapshot(t)
  }

  // Typed stats (decimal, date, timestamp, float, double)

  test("ds_typed_stats", "DECIMAL/DATE/TIMESTAMP/FLOAT/DOUBLE data skipping stats",
      "dataSkipping") {
    sql("""CREATE TABLE tbl (
      c1 LONG, c2 STRING, c3 FLOAT, c4 DOUBLE,
      c5 TIMESTAMP, c6 TIMESTAMP_NTZ, c7 DATE,
      c8 BYTE, c9 SHORT, c10 DECIMAL(3,2)
    ) USING delta
    TBLPROPERTIES ('delta.minReaderVersion' = '3', 'delta.minWriterVersion' = '7',
      'delta.feature.timestampNtz' = 'supported')""")
    sql("""INSERT INTO tbl VALUES
      (1, 'abc', 1.5, 2.5, TIMESTAMP'2024-01-01 00:00:00',
       TIMESTAMP_NTZ'2024-01-01 00:00:00', DATE'2024-01-01', 1, 100, 1.23)""")
    sql("""INSERT INTO tbl VALUES
      (100, 'xyz', 99.9, 199.9, TIMESTAMP'2024-12-31 23:59:59',
       TIMESTAMP_NTZ'2024-12-31 23:59:59', DATE'2024-12-31', 127, 32000, 9.87)""")
    val t = registerTable("tbl")
    read(t, predicate = "c1 = 1")
    read(t, predicate = "c1 > 50")
    read(t, predicate = "c2 = 'abc'")
    read(t, predicate = "c3 < 2.0")
    read(t, predicate = "c4 >= 100.0")
    read(t, predicate = "c5 = TIMESTAMP'2024-01-01 00:00:00'")
    read(t, predicate = "c6 > TIMESTAMP_NTZ'2024-06-01 00:00:00'")
    read(t, predicate = "c7 = DATE'2024-01-01'")
    read(t, predicate = "c7 > DATE'2024-06-01'")
    read(t, predicate = "c8 = 1")
    read(t, predicate = "c9 > 20000")
    read(t, predicate = "c10 > 5.00")
    read(t, predicate = "c10 = 1.23")
    read(t, predicate = "c1 > 200", name = "read_miss_c1")
    read(t, predicate = "c3 > 200.0", name = "read_miss_c3")
    read(t, predicate = "c4 < 1.0", name = "read_miss_c4")
    read(t, predicate = "c5 > TIMESTAMP'2025-06-01 00:00:00'", name = "read_miss_c5")
    read(t, predicate = "c7 > DATE'2025-01-01'", name = "read_miss_c7")
    read(t, predicate = "c10 > 9.99", name = "read_miss_c10")
    snapshot(t)
  }

  // Variant null stats

  test("ds_variant_null_stats", "VARIANT NULL/NOT NULL data skipping", "dataSkipping") {
    sql("""CREATE TABLE tbl (
      v VARIANT, v_struct STRUCT<v: VARIANT>,
      null_v VARIANT, null_v_struct STRUCT<v: VARIANT>
    ) USING delta
    TBLPROPERTIES ('delta.feature.variantType-preview' = 'supported')""")
    sql("""INSERT INTO tbl VALUES (
      PARSE_JSON('1'), named_struct('v', PARSE_JSON('"hello"')),
      NULL, named_struct('v', NULL))""")
    val t = registerTable("tbl")
    read(t, predicate = "v IS NOT NULL")
    read(t, predicate = "v IS NULL")
    read(t, predicate = "null_v IS NULL")
    read(t, predicate = "null_v IS NOT NULL")
    read(t, predicate = "v_struct.v IS NOT NULL")
    read(t, predicate = "v_struct.v IS NULL")
    read(t, predicate = "null_v_struct.v IS NULL")
    snapshot(t)
  }

  // Indexed columns / stats configuration

  test("ds_indexed_names_empty", "empty indexed names disables stats", "dataSkipping") {
    sql("""CREATE TABLE tbl (a LONG, b LONG) USING delta
      TBLPROPERTIES ('delta.dataSkippingStatsColumns' = '')""")
    sql("INSERT INTO tbl VALUES (1, 10), (2, 20)")
    val t = registerTable("tbl")
    read(t, predicate = "a = 1")
    read(t, predicate = "b = 10")
    read(t, predicate = "a > 100", name = "read_a_gt_100")
    snapshot(t)
  }

  test("ds_indexed_names_subset", "index subset of leaf columns", "dataSkipping") {
    sql("""CREATE TABLE tbl (a LONG, b LONG, c LONG) USING delta
      TBLPROPERTIES ('delta.dataSkippingStatsColumns' = 'a,c')""")
    sql("INSERT INTO tbl VALUES (1, 10, 100)")
    sql("INSERT INTO tbl VALUES (5, 50, 500)")
    val t = registerTable("tbl")
    // a has stats
    read(t, predicate = "a = 1")
    read(t, predicate = "a > 10", name = "read_miss_a")
    // b has no stats
    read(t, predicate = "b = 10")
    read(t, predicate = "b > 100", name = "read_b_no_skip")
    // c has stats
    read(t, predicate = "c = 100")
    read(t, predicate = "c > 1000", name = "read_miss_c")
    read(t, predicate = "a = 1 AND c = 100")
    read(t, predicate = "a > 10 AND c > 1000", name = "read_miss_ac")
    read(t, predicate = "a = 1 OR b = 10")
    snapshot(t)
  }

  test("ds_indexed_names_nested", "naming nested column indexes leaves",
      "dataSkipping") {
    sql("""CREATE TABLE tbl (
      a STRUCT<x: LONG, y: LONG>,
      b STRUCT<p: LONG, q: LONG>
    ) USING delta
    TBLPROPERTIES ('delta.dataSkippingStatsColumns' = 'a.x,b.q')""")
    sql("INSERT INTO tbl VALUES (named_struct('x',1,'y',10), named_struct('p',100,'q',1000))")
    sql("INSERT INTO tbl VALUES (named_struct('x',5,'y',50), named_struct('p',500,'q',5000))")
    val t = registerTable("tbl")
    // a.x has stats
    read(t, predicate = "a.x = 1")
    read(t, predicate = "a.x > 10", name = "read_miss_ax")
    // a.y no stats
    read(t, predicate = "a.y = 10")
    read(t, predicate = "a.y > 100", name = "read_ay_no_skip")
    // b.p no stats
    read(t, predicate = "b.p = 100")
    // b.q has stats
    read(t, predicate = "b.q = 1000")
    read(t, predicate = "b.q > 10000", name = "read_miss_bq")
    read(t, predicate = "a.x = 1 AND b.q = 1000")
    read(t, predicate = "a.x > 10 AND b.q > 10000", name = "read_miss_ax_bq")
    snapshot(t)
  }

  test("ds_indexed_names_complex", "nested columns with complex types", "dataSkipping") {
    sql("""CREATE TABLE tbl (
      a STRUCT<x: LONG, y: STRUCT<z: LONG>>,
      b LONG
    ) USING delta
    TBLPROPERTIES ('delta.dataSkippingStatsColumns' = 'a.x,a.y.z,b')""")
    sql("INSERT INTO tbl VALUES (named_struct('x',1,'y',named_struct('z',10)), 100)")
    sql("INSERT INTO tbl VALUES (named_struct('x',5,'y',named_struct('z',50)), 500)")
    val t = registerTable("tbl")
    read(t, predicate = "a.x = 1")
    read(t, predicate = "a.y.z = 10")
    read(t, predicate = "b = 100")
    read(t, predicate = "a.x > 10", name = "read_miss_ax")
    read(t, predicate = "a.y.z > 100", name = "read_miss_ayz")
    snapshot(t)
  }

  test("ds_indexed_names_backtick", "backtick escapes", "dataSkipping") {
    sql("CREATE TABLE tbl (`a.b` LONG, `c d` LONG) USING delta")
    sql("INSERT INTO tbl VALUES (1, 10)")
    val t = registerTable("tbl")
    read(t, predicate = "`a.b` = 1")
    read(t, predicate = "`c d` = 10")
    read(t, predicate = "`a.b` > 5", name = "read_miss_ab")
    snapshot(t)
  }

  test("ds_more_cols_than_indexed", "more columns than indexed", "dataSkipping") {
    sql("""CREATE TABLE tbl (a LONG, b LONG, c LONG, d LONG) USING delta
      TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = '2')""")
    sql("INSERT INTO tbl VALUES (1, 10, 100, 1000)")
    val t = registerTable("tbl")
    read(t, predicate = "a = 1")
    read(t, predicate = "b = 10")
    read(t, predicate = "c = 100")  // no stats
    read(t, predicate = "d = 1000")  // no stats
    snapshot(t)
  }

  test("ds_missing_stats_cols", "missing stats columns", "dataSkipping") {
    sql("""CREATE TABLE tbl (a LONG) USING delta
      TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = '1')""")
    sql("INSERT INTO tbl VALUES (1), (2)")
    sql("ALTER TABLE tbl ADD COLUMN b LONG")
    sql("INSERT INTO tbl VALUES (3, 30)")
    val t = registerTable("tbl")
    read(t, predicate = "a = 1")
    read(t, predicate = "b = 30")
    read(t, predicate = "a > 5", name = "read_miss_a")
    read(t, predicate = "b IS NULL")
    snapshot(t)
  }

  test("ds_missing_stats_graceful", "Stats missing, read still works (full scan)",
      "dataSkipping") {
    sql("CREATE TABLE tbl (a INT) USING delta")
    sql("INSERT INTO tbl VALUES (1), (2), (3)")
    val t = registerTable("tbl")
    // Strip stats from commit
    modifyCommitActions(t, version = 1) { actions =>
      actions.map { case ("add", node) =>
        node.remove("stats"); ("add", node)
        case other => other
      }
    }
    read(t, predicate = "a = 1")
    read(t, predicate = "a > 99")
    snapshot(t)
  }

  test("ds_stats_config_change", "Stats config changing across versions",
      "dataSkipping") {
    sql("""CREATE TABLE tbl (a INT, b INT) USING delta
      TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = '2')""")
    sql("INSERT INTO tbl VALUES (1, 10)")
    sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = '1')")
    sql("INSERT INTO tbl VALUES (2, 20)")
    val t = registerTable("tbl")
    // First file: both a and b have stats; second file: only a
    read(t, predicate = "a = 1")
    read(t, predicate = "b = 10")
    snapshot(t)
  }

  // Nested indexed columns (delta.dataSkippingNumIndexedCols with nested schema)

  test("ds_nested_indexed_0", "nested schema, indexed=0", "dataSkipping") {
    sql("""CREATE TABLE tbl (a STRUCT<x: LONG, y: LONG>, b LONG) USING delta
      TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = '0')""")
    sql("INSERT INTO tbl VALUES (named_struct('x',1,'y',10), 100)")
    val t = registerTable("tbl")
    read(t, predicate = "a.x = 1")
    read(t, predicate = "a.y = 10")
    read(t, predicate = "b = 100")
    read(t, predicate = "b > 500", name = "read_b_no_stats")
    snapshot(t)
  }

  test("ds_nested_indexed_3", "nested schema, indexed=3", "dataSkipping") {
    sql("""CREATE TABLE tbl (
      a STRUCT<x: LONG, y: LONG>,
      b LONG,
      c STRUCT<p: LONG>
    ) USING delta
    TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = '3')""")
    sql("INSERT INTO tbl VALUES (named_struct('x',1,'y',10), 100, named_struct('p',1000))")
    sql("INSERT INTO tbl VALUES (named_struct('x',5,'y',50), 500, named_struct('p',5000))")
    val t = registerTable("tbl")
    read(t, predicate = "a.x = 1")
    read(t, predicate = "a.y = 10")
    read(t, predicate = "b = 100")
    read(t, predicate = "c.p = 1000")
    read(t, predicate = "a.x > 10", name = "read_miss_ax")
    read(t, predicate = "b > 1000", name = "read_miss_b")
    read(t, predicate = "a.x = 1 AND b = 100")
    read(t, predicate = "a.x = 1 OR c.p = 5000")
    snapshot(t)
  }

  test("ds_nested_indexed_6", "nested schema, indexed=6", "dataSkipping") {
    sql("""CREATE TABLE tbl (
      a STRUCT<x: LONG, y: LONG, z: LONG>,
      b STRUCT<p: LONG, q: LONG>,
      c LONG
    ) USING delta
    TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = '6')""")
    sql("INSERT INTO tbl VALUES (named_struct('x',1,'y',2,'z',3), named_struct('p',4,'q',5), 6)")
    val t = registerTable("tbl")
    read(t, predicate = "a.x = 1")
    read(t, predicate = "a.z = 3")
    read(t, predicate = "b.p = 4")
    read(t, predicate = "b.q = 5")
    read(t, predicate = "c = 6")
    snapshot(t)
  }

  test("ds_nested_indexed_9", "nested schema, indexed=9", "dataSkipping") {
    sql("""CREATE TABLE tbl (
      a STRUCT<x: LONG, y: LONG, z: LONG>,
      b STRUCT<p: LONG, q: LONG, r: LONG>,
      c STRUCT<s: LONG, t: LONG, u: LONG>
    ) USING delta
    TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = '9')""")
    sql("""INSERT INTO tbl VALUES (
      named_struct('x',1,'y',2,'z',3),
      named_struct('p',4,'q',5,'r',6),
      named_struct('s',7,'t',8,'u',9))""")
    val t = registerTable("tbl")
    read(t, predicate = "a.x = 1")
    read(t, predicate = "b.r = 6")
    read(t, predicate = "c.u = 9")
    read(t, predicate = "c.u > 100", name = "read_miss_cu")
    read(t, predicate = "a.x = 1 AND c.u = 9")
    snapshot(t)
  }

  // Partitioned + stats combined

  test("ds_partitioned", "Partitioned table data skipping", "dataSkipping") {
    sql("CREATE TABLE tbl (id INT, part STRING) USING delta PARTITIONED BY (part)")
    sql("INSERT INTO tbl VALUES (1, 'a'), (2, 'a')")
    sql("INSERT INTO tbl VALUES (3, 'b'), (4, 'b')")
    sql("INSERT INTO tbl VALUES (5, 'c')")
    val t = registerTable("tbl")
    read(t, predicate = "part = 'a'")
    read(t, predicate = "part = 'b' AND id > 3")
    read(t, predicate = "id < 3")
    read(t, predicate = "part = 'z'", name = "read_miss_part")
    read(t, predicate = "id > 100", name = "read_miss_id")
    snapshot(t)
  }

  test("ds_partition_and_stats", "Partition pruning + stats skipping combined",
      "dataSkipping") {
    sql("CREATE TABLE tbl (id INT, value INT, part STRING) USING delta PARTITIONED BY (part)")
    sql("INSERT INTO tbl VALUES (1, 10, 'a'), (2, 20, 'a')")
    sql("INSERT INTO tbl VALUES (3, 30, 'b'), (4, 40, 'b')")
    val t = registerTable("tbl")
    read(t, predicate = "part = 'a' AND value > 15")
    read(t, predicate = "part = 'b' AND value < 25", name = "read_miss_combined")
    read(t, predicate = "part = 'a' OR value > 35")
    snapshot(t)
  }

  test("ds_partition_or_predicate", "OR predicate on partition column", "dataSkipping") {
    sql("CREATE TABLE tbl (id INT, part STRING) USING delta PARTITIONED BY (part)")
    sql("INSERT INTO tbl VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    val t = registerTable("tbl")
    read(t, predicate = "part = 'a' OR part = 'c'")
    read(t, predicate = "part = 'z'", name = "read_miss_part")
    snapshot(t)
  }

  // Schema order mismatch and nonexistent col filter

  test("ds_schema_order_mismatch", "Query columns in different order", "dataSkipping") {
    sql("CREATE TABLE tbl (a INT, b INT, c INT) USING delta")
    sql("INSERT INTO tbl VALUES (1, 2, 3)")
    val t = registerTable("tbl")
    read(t, predicate = "c = 3")
    read(t, columns = Seq("c", "a"))
    snapshot(t)
  }

  test("ds_nonexistent_col_filter", "Filter on non-existent column", "dataSkipping") {
    sql("CREATE TABLE tbl (a INT) USING delta")
    sql("INSERT INTO tbl VALUES (1), (2)")
    val t = registerTable("tbl")
    read(t, predicate = "a = 1")
    // nonexistent column handled gracefully
    snapshot(t)
  }

  // Generated columns

  test("ds_generated_col_skipping", "Skipping using generated column stats",
      "dataSkipping") {
    sql("""CREATE TABLE tbl (
      date_col DATE, value INT,
      year_col INT GENERATED ALWAYS AS (YEAR(date_col))
    ) USING delta""")
    sql("INSERT INTO tbl (date_col, value) VALUES (DATE'2024-03-15', 1), (DATE'2024-11-20', 2)")
    sql("INSERT INTO tbl (date_col, value) VALUES (DATE'2025-01-05', 3)")
    val t = registerTable("tbl")
    read(t, predicate = "year_col = 2024")
    read(t, predicate = "year_col = 2025")
    snapshot(t)
  }

  // DVs + data skipping

  test("ds_with_dvs_edge", "Data skipping + DVs combined", "dataSkipping", "dv") {
    sql("""CREATE TABLE tbl (a INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(1, 11)")   // file 1: 1-10
    sql("INSERT INTO tbl SELECT id FROM range(11, 21)")  // file 2: 11-20
    sql("DELETE FROM tbl WHERE a = 5")  // DV on file 1
    val t = registerTable("tbl")
    read(t, predicate = "a = 5", name = "read_deleted_row")
    read(t, predicate = "a > 15")
    read(t, predicate = "a = 1")
    snapshot(t)
  }

  test("ds_with_dvs_edge_1", "Data skipping + DVs: predicate skips file with DV",
      "dataSkipping", "dv") {
    sql("""CREATE TABLE tbl (a INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(1, 11)")
    sql("INSERT INTO tbl SELECT id FROM range(11, 21)")
    sql("DELETE FROM tbl WHERE a <= 5")
    val t = registerTable("tbl")
    read(t, predicate = "a > 10")
    read(t, predicate = "a <= 5", name = "read_all_deleted")
    snapshot(t)
  }

  test("ds_with_dvs_edge_2", "Data skipping + DVs: entire file deleted via DV",
      "dataSkipping", "dv") {
    sql("""CREATE TABLE tbl (a INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(1, 6)")
    sql("INSERT INTO tbl SELECT id FROM range(6, 11)")
    sql("DELETE FROM tbl WHERE a <= 5")  // all of file 1 deleted
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "a > 5")
    snapshot(t)
  }

  // Column mapping: stats after drop/rename

  test("ds_stats_col_drop", "Data skipping after column drop (CM=name)",
      "dataSkipping", "columnMapping") {
    sql("""CREATE TABLE tbl (a INT, b INT, c INT) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    sql("INSERT INTO tbl VALUES (1, 10, 100)")
    sql("ALTER TABLE tbl DROP COLUMN b")
    sql("INSERT INTO tbl VALUES (2, 200)")
    val t = registerTable("tbl")
    read(t, predicate = "a = 1")
    read(t, predicate = "c = 100")
    snapshot(t)
  }

  test("ds_stats_col_rename", "Data skipping after column rename (CM=name)",
      "dataSkipping", "columnMapping") {
    sql("""CREATE TABLE tbl (a INT, old_name INT) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    sql("INSERT INTO tbl VALUES (1, 100)")
    sql("ALTER TABLE tbl RENAME COLUMN old_name TO new_name")
    sql("INSERT INTO tbl VALUES (2, 200)")
    val t = registerTable("tbl")
    read(t, predicate = "a = 1")
    read(t, predicate = "new_name > 150")
    snapshot(t)
  }

  test("ds_stats_after_drop", "Stats after column drop with column mapping",
      "dataSkipping", "columnMapping") {
    sql("""CREATE TABLE tbl (
      c1 LONG, c2 STRING, c3 FLOAT, c4 DOUBLE,
      c5 TIMESTAMP, c6 TIMESTAMP_NTZ, c7 DATE,
      c8 BYTE, c9 SHORT, c10 DECIMAL(3,2)
    ) USING delta
    TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
      'delta.minReaderVersion' = '3', 'delta.minWriterVersion' = '7',
      'delta.feature.timestampNtz' = 'supported')""")
    sql("""INSERT INTO tbl VALUES (1, 'a', 1.5, 2.5,
      TIMESTAMP'2024-01-01 00:00:00', TIMESTAMP_NTZ'2024-01-01 00:00:00',
      DATE'2024-01-01', 1, 100, 1.23)""")
    sql("ALTER TABLE tbl DROP COLUMN c2")
    sql("ALTER TABLE tbl DROP COLUMN c8")
    sql("ALTER TABLE tbl DROP COLUMN c9")
    sql("""INSERT INTO tbl VALUES (2, 3.0, 4.5,
      TIMESTAMP'2024-06-15 00:00:00', TIMESTAMP_NTZ'2024-06-15 00:00:00',
      DATE'2024-06-15', 2.34)""")
    val t = registerTable("tbl")
    read(t, predicate = "c1 = 1")
    read(t, predicate = "c1 > 1")
    read(t, predicate = "c3 < 2.0")
    read(t, predicate = "c4 > 3.0")
    read(t, predicate = "c5 = TIMESTAMP'2024-01-01 00:00:00'")
    read(t, predicate = "c6 > TIMESTAMP_NTZ'2024-03-01 00:00:00'")
    read(t, predicate = "c7 = DATE'2024-01-01'")
    read(t, predicate = "c10 > 2.00")
    read(t, predicate = "c1 > 100", name = "read_miss_c1")
    read(t, predicate = "c3 > 100.0", name = "read_miss_c3")
    read(t, predicate = "c7 > DATE'2025-01-01'", name = "read_miss_c7")
    read(t, predicate = "c10 > 9.99", name = "read_miss_c10")
    read(t, predicate = "c1 = 1 AND c10 = 1.23")
    snapshot(t)
  }

  test("ds_stats_after_rename", "Stats after column rename with column mapping",
      "dataSkipping", "columnMapping") {
    sql("""CREATE TABLE tbl (
      c1 LONG, c2 STRING, c3 FLOAT, c4 DOUBLE,
      c5 TIMESTAMP, c6 TIMESTAMP_NTZ, c7 DATE,
      c8 BYTE, c9 SHORT, c10 DECIMAL(3,2)
    ) USING delta
    TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
      'delta.minReaderVersion' = '3', 'delta.minWriterVersion' = '7',
      'delta.feature.timestampNtz' = 'supported')""")
    sql("""INSERT INTO tbl VALUES (1, 'a', 1.5, 2.5,
      TIMESTAMP'2024-01-01 00:00:00', TIMESTAMP_NTZ'2024-01-01 00:00:00',
      DATE'2024-01-01', 1, 100, 1.23)""")
    sql("ALTER TABLE tbl RENAME COLUMN c2 TO renamed_c2")
    sql("ALTER TABLE tbl RENAME COLUMN c8 TO renamed_c8")
    sql("""INSERT INTO tbl VALUES (2, 'b', 3.0, 4.5,
      TIMESTAMP'2024-06-15 00:00:00', TIMESTAMP_NTZ'2024-06-15 00:00:00',
      DATE'2024-06-15', 2, 200, 2.34)""")
    val t = registerTable("tbl")
    read(t, predicate = "c1 = 1")
    read(t, predicate = "c1 > 1")
    read(t, predicate = "renamed_c2 = 'a'")
    read(t, predicate = "c3 < 2.0")
    read(t, predicate = "c4 > 3.0")
    read(t, predicate = "c5 = TIMESTAMP'2024-01-01 00:00:00'")
    read(t, predicate = "c6 > TIMESTAMP_NTZ'2024-03-01 00:00:00'")
    read(t, predicate = "c7 = DATE'2024-01-01'")
    read(t, predicate = "renamed_c8 = 1")
    read(t, predicate = "c9 > 150")
    read(t, predicate = "c10 > 2.00")
    read(t, predicate = "c1 > 100", name = "read_miss_c1")
    read(t, predicate = "renamed_c2 = 'z'", name = "read_miss_renamed_c2")
    read(t, predicate = "c3 > 100.0", name = "read_miss_c3")
    read(t, predicate = "c7 > DATE'2025-01-01'", name = "read_miss_c7")
    read(t, predicate = "c10 > 9.99", name = "read_miss_c10")
    read(t, predicate = "c1 = 1 AND renamed_c2 = 'a'")
    read(t, predicate = "c1 = 1 AND c10 = 1.23")
    read(t, predicate = "c1 > 100 AND c10 > 9.99", name = "read_miss_c1_c10")
    snapshot(t)
  }

  // Error test: field not found

  test("ds_err_001_field_not_found", "FIELD_NOT_FOUND for row commit version filter",
      "dataSkipping", "error") {
    sql("""CREATE TABLE tbl (a INT) USING delta
      TBLPROPERTIES ('delta.enableRowTracking' = 'true')""")
    sql("INSERT INTO tbl VALUES (1)")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  // === Statistics ===

  test("stats_null_in_min_max", "Stats with all-null column (null min/max)", "data-skipping", "stats", "edge-case") {
    sql("""CREATE TABLE tbl (id INT, nullable_col STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, null),(2, null),(3, null)")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "nullable_col = 'a'")
    read(t, predicate = "nullable_col IS NULL")
    snapshot(t)
  }

  test("stats_numrecords_only", "Stats with only numRecords", "data-skipping", "stats", "edge-case") {
    sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, 'a'),(2, 'b')")
    val t = registerTable("tbl")
    // Strip min/max stats, keep only numRecords
    modifyCommitActions(t, 0) { actions =>
      actions.map { case ("add", node) =>
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
        ("add", node)
        case other => other
      }
    }
    snapshot(t)
  }

  test("stats_numrecords_with_dv", "numRecords is physical count, not logical (with DVs)", "data-skipping", "stats", "deletion-vectors", "edge-case") {
    sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    sql("DELETE FROM tbl WHERE id < 3")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "id >= 5")
    snapshot(t)
  }

  test("stats_partition_col_no_stats", "Partition column excluded from data statistics", "data-skipping", "stats", "partition", "edge-case") {
    sql("""CREATE TABLE tbl (id INT, country STRING, amount INT) USING delta
      PARTITIONED BY (country) TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, 'US', 100),(2, 'UK', 200),(3, 'US', 300),(4, 'UK', 400)")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "country = 'US'")
    read(t, predicate = "amount > 200")
    snapshot(t)
  }

  test("stats_string_truncation", "Stats with truncated string min/max (must not over-prune)", "data-skipping", "stats", "string-truncation", "edge-case") {
    sql("""CREATE TABLE tbl (id INT, long_str STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    // Include a string longer than 32 chars to trigger truncation
    sql("""INSERT INTO tbl VALUES
      (1, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaxyz'),
      (2, 'short'),
      (3, 'medium_length_string')""")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "long_str = 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaxyz'")
    read(t, predicate = "long_str = 'short'")
    snapshot(t)
  }

  test("stats_empty_string", "Stats with empty string values", "data-skipping", "stats", "edge-case") {
    sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, ''),(2, 'a'),(3, '')")
    val t = registerTable("tbl")
    // Strip stats completely to simulate empty stats string
    modifyCommitActions(t, 0) { actions =>
      actions.map { case ("add", node) =>
        if (node.has("stats")) node.put("stats", "")
        ("add", node)
        case other => other
      }
    }
    snapshot(t)
  }

  test("stats_missing_entirely", "Stats field missing entirely", "data-skipping", "stats", "edge-case") {
    sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, 'a'),(2, 'b')")
    val t = registerTable("tbl")
    // Remove stats field entirely
    modifyCommitActions(t, 0) { actions =>
      actions.map { case ("add", node) =>
        node.remove("stats"); ("add", node)
        case other => other
      }
    }
    snapshot(t)
  }

  // === Partitioning ===

  test("single_partition", "Single partition column", "partitioned") {
    sql("""CREATE TABLE tbl (id INT, region STRING, value DOUBLE)
      USING delta PARTITIONED BY (region)""")
    sql("INSERT INTO tbl VALUES (1,'us',10),(2,'us',20),(3,'eu',30),(4,'eu',40),(5,'asia',50)")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "region = 'us'")
    read(t, predicate = "region = 'eu'")
    read(t, predicate = "region = 'antarctica'")
    snapshot(t)
  }

  test("multi_partition", "Multiple partition columns", "partitioned") {
    sql("""CREATE TABLE tbl (id INT, year INT, month INT, data STRING)
      USING delta PARTITIONED BY (year, month)""")
    sql("""INSERT INTO tbl VALUES
      (1,2024,1,'jan24'),(2,2024,2,'feb24'),(3,2024,3,'mar24'),
      (4,2025,1,'jan25'),(5,2025,2,'feb25')""")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "year = 2024")
    read(t, predicate = "year = 2025 AND month = 1")
    read(t, columns = Seq("id", "year"))
    snapshot(t)
  }

  test("null_partition", "NULL partition values", "partitioned", "nulls") {
    sql("""CREATE TABLE tbl (id INT, category STRING, value INT)
      USING delta PARTITIONED BY (category)""")
    sql("INSERT INTO tbl VALUES (1,'a',10),(2,NULL,20),(3,'b',30),(4,NULL,40)")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "category IS NULL")
    read(t, predicate = "category IS NOT NULL")
    read(t, predicate = "category = 'a'")
    snapshot(t)
  }

  test("stats_skipping", "Data skipping via column stats", "skipping") {
    sql("CREATE TABLE tbl (id INT, value INT) USING delta")
    sql("INSERT INTO tbl SELECT id, id * 10 FROM range(100) WHERE id < 50")
    sql("INSERT INTO tbl SELECT id, id * 10 FROM range(100) WHERE id >= 50")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "id < 25")
    read(t, predicate = "id >= 75")
    read(t, predicate = "id > 999")
    read(t, predicate = "id >= 0")
    snapshot(t)
  }

  test("partition_pruning", "Partition pruning + stats", "partitioned", "skipping") {
    sql("""CREATE TABLE tbl (id INT, region STRING, amount DOUBLE)
      USING delta PARTITIONED BY (region)""")
    sql("INSERT INTO tbl VALUES (1,'us',10),(2,'us',20)")
    sql("INSERT INTO tbl VALUES (3,'eu',30),(4,'eu',40)")
    sql("INSERT INTO tbl VALUES (5,'asia',50)")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "region = 'us'")
    read(t, predicate = "region = 'us' AND amount > 15")
    read(t, predicate = "region = 'mars'")
    snapshot(t)
  }

  test("column_projection", "Column subsets", "projection") {
    sql("CREATE TABLE tbl (a INT, b STRING, c DOUBLE, d BOOLEAN, e DATE) USING delta")
    sql("""INSERT INTO tbl VALUES
      (1,'x',1.1,true,DATE'2024-01-01'),
      (2,'y',2.2,false,DATE'2024-06-15'),
      (3,'z',3.3,true,DATE'2025-01-01')""")
    val t = registerTable("tbl")
    read(t)
    read(t, columns = Seq("a"))
    read(t, columns = Seq("b", "d"))
    read(t, columns = Seq("e", "c", "a"))
    read(t, predicate = "a > 1", columns = Seq("a", "b"))
    snapshot(t)
  }

  test("part_date_type", "Partition with date type", "partitioned") {
    sql("""CREATE TABLE tbl (id INT, value STRING, dt DATE) USING delta
      PARTITIONED BY (dt)
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("""INSERT INTO tbl VALUES
      (1,'jan',DATE'2024-01-01'),(2,'jun',DATE'2024-06-01'),
      (3,'dec',DATE'2024-12-01'),(4,'jan2',DATE'2024-01-01')""")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "dt = DATE'2024-01-01'")
    read(t, predicate = "dt >= DATE'2024-06-01'")
    snapshot(t)
  }

  test("part_null_values", "Partition with NULL values", "partitioned") {
    sql("""CREATE TABLE tbl (id INT, value STRING, part STRING) USING delta
      PARTITIONED BY (part)
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a','x'),(2,'b',NULL),(3,'c','y'),(4,'d',NULL)")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "part IS NULL")
    read(t, predicate = "part IS NOT NULL")
    snapshot(t)
  }

  test("part_or_predicate", "Partition pruning with OR predicate", "partitioned") {
    sql("""CREATE TABLE tbl (id INT, value STRING, part STRING) USING delta
      PARTITIONED BY (part)
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a1','A'),(2,'a2','A'),(3,'b1','B'),(4,'c1','C'),(5,'c2','C')")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "part = 'A' OR part = 'C'")
    read(t, predicate = "part = 'B'")
    snapshot(t)
  }

  test("part_multi_column", "Multi-column partitioning with 3 columns", "partitioned") {
    sql("""CREATE TABLE tbl (id INT, value STRING, a INT, b STRING, c BOOLEAN) USING delta
      PARTITIONED BY (a, b, c)
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("""INSERT INTO tbl VALUES
      (1,'v1',1,'x',true),(2,'v2',1,'y',false),
      (3,'v3',2,'y',true),(4,'v4',2,'z',false),
      (5,'v5',3,'z',true)""")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "a = 1")
    read(t, predicate = "a = 2 AND b = 'y'")
    read(t, predicate = "a = 3 AND b = 'z' AND c = true")
    snapshot(t)
  }

}
