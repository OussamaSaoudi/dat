/**
 * Data type coverage and data skipping write workloads.
 *
 * DataTypeCoverage (DT-001..DT-026): Every supported Delta data type including
 * primitives, complex types, nested combinations, and edge cases.
 *
 * DataSkipping (DSK-001..DSK-015): Multi-file tables with predicates that
 * exercise file skipping via column stats.
 */

// =============================================================================
// Data Type Coverage
// =============================================================================

new WorkloadSuite("write_datatype_coverage") {

  test("dt_001_boolean", "Boolean type coverage", "write", "datatype", "boolean") { w =>
    w.sql("CREATE TABLE tbl (id INT, flag BOOLEAN) USING delta")
    w.sql("INSERT INTO tbl VALUES (1, true), (2, false), (3, null)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("dt_002_tinyint", "Byte/tinyint type coverage", "write", "datatype", "tinyint") { w =>
    w.sql("CREATE TABLE tbl (id INT, val TINYINT) USING delta")
    w.sql(
      "INSERT INTO tbl VALUES (1, CAST(-128 AS TINYINT)), " +
      "(2, CAST(0 AS TINYINT)), (3, CAST(127 AS TINYINT)), (4, null)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("dt_003_smallint", "Short/smallint type coverage", "write", "datatype", "smallint") { w =>
    w.sql("CREATE TABLE tbl (id INT, val SMALLINT) USING delta")
    w.sql(
      "INSERT INTO tbl VALUES (1, CAST(-32768 AS SMALLINT)), " +
      "(2, CAST(0 AS SMALLINT)), (3, CAST(32767 AS SMALLINT)), (4, null)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("dt_004_int", "Int type coverage", "write", "datatype", "int") { w =>
    w.sql("CREATE TABLE tbl (id INT, val INT) USING delta")
    w.sql("INSERT INTO tbl VALUES (1, -2147483648), (2, 0), (3, 2147483647), (4, null)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("dt_005_bigint", "Long/bigint type coverage", "write", "datatype", "bigint") { w =>
    w.sql("CREATE TABLE tbl (id INT, val BIGINT) USING delta")
    w.sql(
      "INSERT INTO tbl VALUES (1, -9223372036854775808), (2, 0), " +
      "(3, 9223372036854775807), (4, null)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("dt_006_float", "Float type coverage with special values", "write", "datatype", "float") { w =>
    w.sql("CREATE TABLE tbl (id INT, val FLOAT) USING delta")
    w.sql(
      "INSERT INTO tbl VALUES " +
      "(1, CAST('NaN' AS FLOAT)), " +
      "(2, CAST('Infinity' AS FLOAT)), " +
      "(3, CAST('-Infinity' AS FLOAT)), " +
      "(4, CAST(0.0 AS FLOAT)), " +
      "(5, CAST(-3.4028235E38 AS FLOAT)), " +
      "(6, CAST(3.4028235E38 AS FLOAT)), " +
      "(7, null)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("dt_007_double", "Double type coverage with special values", "write", "datatype", "double") { w =>
    w.sql("CREATE TABLE tbl (id INT, val DOUBLE) USING delta")
    w.sql(
      "INSERT INTO tbl VALUES " +
      "(1, CAST('NaN' AS DOUBLE)), " +
      "(2, CAST('Infinity' AS DOUBLE)), " +
      "(3, CAST('-Infinity' AS DOUBLE)), " +
      "(4, 0.0D), " +
      "(5, -1.7976931348623157E308D), " +
      "(6, 1.7976931348623157E308D), " +
      "(7, null)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("dt_008_string", "String type coverage with unicode and edge cases",
      "write", "datatype", "string") { w =>
    w.sql("CREATE TABLE tbl (id INT, val STRING) USING delta")
    w.sql(
      "INSERT INTO tbl VALUES " +
      "(1, ''), " +
      "(2, '" + "a" * 1000 + "'), " +
      "(3, 'hello world'), " +
      "(4, 'line1\\nline2'), " +
      "(5, null)")
    w.sql("INSERT INTO tbl SELECT 6, decode(unhex('C3A9C3A0C3BCE4B896E7958C'), 'UTF-8')")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("dt_009_binary", "Binary type coverage", "write", "datatype", "binary") { w =>
    w.sql("CREATE TABLE tbl (id INT, val BINARY) USING delta")
    w.sql(
      "INSERT INTO tbl VALUES " +
      "(1, CAST('hello' AS BINARY)), " +
      "(2, CAST('' AS BINARY)), " +
      "(3, X'DEADBEEF'), " +
      "(4, null)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("dt_010_date", "Date type coverage", "write", "datatype", "date") { w =>
    w.sql("CREATE TABLE tbl (id INT, val DATE) USING delta")
    w.sql(
      "INSERT INTO tbl VALUES " +
      "(1, DATE'1970-01-01'), " +
      "(2, DATE'2024-06-15'), " +
      "(3, DATE'9999-12-31'), " +
      "(4, DATE'0001-01-01'), " +
      "(5, null)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("dt_011_timestamp", "Timestamp type coverage", "write", "datatype", "timestamp") { w =>
    w.sql("CREATE TABLE tbl (id INT, val TIMESTAMP) USING delta")
    w.sql(
      "INSERT INTO tbl VALUES " +
      "(1, TIMESTAMP'1970-01-01 00:00:00'), " +
      "(2, TIMESTAMP'2024-06-15 12:30:45.123'), " +
      "(3, TIMESTAMP'9999-12-31 23:59:59'), " +
      "(4, null)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("dt_012_timestamp_ntz", "Timestamp NTZ type coverage",
      "write", "datatype", "timestamp_ntz") { w =>
    w.sql("CREATE TABLE tbl (id INT, val TIMESTAMP_NTZ) USING delta")
    w.sql(
      "INSERT INTO tbl VALUES " +
      "(1, TIMESTAMP_NTZ'1970-01-01 00:00:00'), " +
      "(2, TIMESTAMP_NTZ'2024-06-15 12:30:45.123'), " +
      "(3, TIMESTAMP_NTZ'9999-12-31 23:59:59'), " +
      "(4, null)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("dt_013_decimal_10_2", "Decimal(10,2) type coverage", "write", "datatype", "decimal") { w =>
    w.sql("CREATE TABLE tbl (id INT, val DECIMAL(10,2)) USING delta")
    w.sql(
      "INSERT INTO tbl VALUES " +
      "(1, 0.00), (2, 99999999.99), (3, -99999999.99), (4, 12345.67), (5, null)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("dt_014_decimal_38_18", "Decimal(38,18) max precision",
      "write", "datatype", "decimal", "high_precision") { w =>
    w.sql("CREATE TABLE tbl (id INT, val DECIMAL(38,18)) USING delta")
    w.sql(
      "INSERT INTO tbl VALUES " +
      "(1, CAST(0 AS DECIMAL(38,18))), " +
      "(2, CAST(12345678901234567890.123456789012345678 AS DECIMAL(38,18))), " +
      "(3, CAST(-12345678901234567890.123456789012345678 AS DECIMAL(38,18))), " +
      "(4, null)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("dt_015_struct", "Struct type coverage", "write", "datatype", "struct", "complex") { w =>
    w.sql("CREATE TABLE tbl (id INT, info STRUCT<name: STRING, age: INT>) USING delta")
    w.sql(
      "INSERT INTO tbl VALUES " +
      "(1, named_struct('name', 'Alice', 'age', 30)), " +
      "(2, named_struct('name', 'Bob', 'age', 25)), " +
      "(3, named_struct('name', '', 'age', null)), " +
      "(4, null)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("dt_016_array", "Array type coverage", "write", "datatype", "array", "complex") { w =>
    w.sql("CREATE TABLE tbl (id INT, vals ARRAY<INT>) USING delta")
    w.sql(
      "INSERT INTO tbl VALUES " +
      "(1, array(1, 2, 3)), " +
      "(2, array()), " +
      "(3, array(null, 42, null)), " +
      "(4, null)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("dt_017_map", "Map type coverage", "write", "datatype", "map", "complex") { w =>
    w.sql("CREATE TABLE tbl (id INT, props MAP<STRING, INT>) USING delta")
    w.sql(
      "INSERT INTO tbl VALUES " +
      "(1, map('x', 1, 'y', 2)), " +
      "(2, map()), " +
      "(3, map('a', null, 'b', 42)), " +
      "(4, null)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("dt_018_array_of_struct", "Array of struct type coverage",
      "write", "datatype", "array", "struct", "nested") { w =>
    w.sql("CREATE TABLE tbl (id INT, points ARRAY<STRUCT<x: INT, y: INT>>) USING delta")
    w.sql(
      "INSERT INTO tbl VALUES " +
      "(1, array(named_struct('x', 1, 'y', 2), named_struct('x', 3, 'y', 4))), " +
      "(2, array()), " +
      "(3, array(named_struct('x', null, 'y', 10))), " +
      "(4, null)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("dt_019_map_with_array_values", "Map with array values",
      "write", "datatype", "map", "array", "nested") { w =>
    w.sql("CREATE TABLE tbl (id INT, data MAP<STRING, ARRAY<INT>>) USING delta")
    w.sql(
      "INSERT INTO tbl VALUES " +
      "(1, map('nums', array(1, 2, 3), 'more', array(4, 5))), " +
      "(2, map('empty', array())), " +
      "(3, map()), " +
      "(4, null)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("dt_020_deeply_nested_struct", "Deeply nested struct type",
      "write", "datatype", "struct", "nested") { w =>
    w.sql(
      "CREATE TABLE tbl " +
      "(id INT, outer_col STRUCT<nested: STRUCT<inner: INT, label: STRING>>) USING delta")
    w.sql(
      "INSERT INTO tbl VALUES " +
      "(1, named_struct('nested', named_struct('inner', 42, 'label', 'deep'))), " +
      "(2, named_struct('nested', named_struct('inner', null, 'label', ''))), " +
      "(3, named_struct('nested', null)), " +
      "(4, null)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("dt_021_all_numeric_types", "All numeric types combined",
      "write", "datatype", "numeric", "combined") { w =>
    w.sql(
      "CREATE TABLE tbl (" +
      "id INT, b TINYINT, s SMALLINT, i INT, l BIGINT, " +
      "f FLOAT, d DOUBLE, dec DECIMAL(18,6)" +
      ") USING delta")
    w.sql(
      "INSERT INTO tbl VALUES " +
      "(1, CAST(1 AS TINYINT), CAST(100 AS SMALLINT), 1000, 1000000L, " +
      "1.5, 2.5, 12345.678900), " +
      "(2, CAST(-1 AS TINYINT), CAST(-100 AS SMALLINT), -1000, -1000000L, " +
      "CAST('-Infinity' AS FLOAT), CAST('NaN' AS DOUBLE), -12345.678900), " +
      "(3, null, null, null, null, null, null, null)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("dt_022_all_temporal_types", "All temporal types combined",
      "write", "datatype", "temporal", "combined") { w =>
    w.sql("CREATE TABLE tbl (id INT, dt DATE, ts TIMESTAMP, ts_ntz TIMESTAMP_NTZ) USING delta")
    w.sql(
      "INSERT INTO tbl VALUES " +
      "(1, DATE'2024-01-15', TIMESTAMP'2024-01-15 10:30:00', TIMESTAMP_NTZ'2024-01-15 10:30:00'), " +
      "(2, DATE'1970-01-01', TIMESTAMP'1970-01-01 00:00:00', TIMESTAMP_NTZ'1970-01-01 00:00:00'), " +
      "(3, DATE'9999-12-31', TIMESTAMP'9999-12-31 23:59:59', TIMESTAMP_NTZ'9999-12-31 23:59:59'), " +
      "(4, null, null, null)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("dt_023_mixed_primitive_complex", "Mixed primitive and complex types",
      "write", "datatype", "mixed", "complex") { w =>
    w.sql(
      "CREATE TABLE tbl (" +
      "id INT, name STRING, " +
      "info STRUCT<city: STRING, zip: INT>, " +
      "tags ARRAY<STRING>, " +
      "attrs MAP<STRING, STRING>" +
      ") USING delta")
    w.sql(
      "INSERT INTO tbl VALUES " +
      "(1, 'Alice', named_struct('city', 'NYC', 'zip', 10001), " +
      "array('admin', 'user'), map('role', 'admin', 'dept', 'eng')), " +
      "(2, 'Bob', named_struct('city', 'LA', 'zip', 90001), " +
      "array('user'), map('role', 'viewer')), " +
      "(3, null, null, null, null)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("dt_024_reserved_words_columns", "Reserved words as column names",
      "write", "datatype", "column_names", "edge_case") { w =>
    w.sql(
      "CREATE TABLE tbl (" +
      "`select` INT, `from` STRING, `where` BOOLEAN, " +
      "`table` DOUBLE, `order` BIGINT" +
      ") USING delta")
    w.sql(
      "INSERT INTO tbl VALUES " +
      "(1, 'val1', true, 1.5, 100), " +
      "(2, 'val2', false, 2.5, 200), " +
      "(3, null, null, null, null)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("dt_025_wide_table_24_cols", "Wide table with 24 columns",
      "write", "datatype", "wide_table", "edge_case") { w =>
    w.sql(
      "CREATE TABLE tbl (" +
      "c01 INT, c02 BIGINT, c03 FLOAT, c04 DOUBLE, c05 STRING, " +
      "c06 BOOLEAN, c07 DATE, c08 TIMESTAMP, c09 DECIMAL(10,2), c10 TINYINT, " +
      "c11 SMALLINT, c12 BINARY, c13 STRING, c14 INT, c15 BIGINT, " +
      "c16 FLOAT, c17 DOUBLE, c18 BOOLEAN, c19 STRING, c20 INT, " +
      "c21 BIGINT, c22 STRING, c23 DOUBLE, c24 BOOLEAN" +
      ") USING delta")
    w.sql(
      "INSERT INTO tbl VALUES (" +
      "1, 2L, 3.0, 4.0, 'five', " +
      "true, DATE'2024-01-01', TIMESTAMP'2024-01-01 00:00:00', 9.99, CAST(10 AS TINYINT), " +
      "CAST(11 AS SMALLINT), CAST('twelve' AS BINARY), 'thirteen', 14, 15L, " +
      "16.0, 17.0, false, 'nineteen', 20, " +
      "21L, 'twenty-two', 23.0, true" +
      ")")
    w.sql(
      "INSERT INTO tbl VALUES (" +
      "null, null, null, null, null, " +
      "null, null, null, null, null, " +
      "null, null, null, null, null, " +
      "null, null, null, null, null, " +
      "null, null, null, null" +
      ")")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("dt_026_implicit_type_casting", "Implicit type casting on insert",
      "write", "datatype", "type_casting", "edge_case") { w =>
    w.sql("CREATE TABLE tbl (id BIGINT, price DOUBLE, label STRING, flag BOOLEAN) USING delta")
    w.sql(
      "INSERT INTO tbl VALUES " +
      "(1, 9.99, 'item1', true), " +
      "(2, 19, 'item2', false), " +
      "(2147483647, 1.7976931348623157E308, 'max', true)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

}.runAll()

// =============================================================================
// Data Skipping
// =============================================================================

new WorkloadSuite("write_data_skipping") {

  test("dsk_001_int_non_overlapping", "INT column non-overlapping ranges for file skipping",
      "write", "data_skipping", "int") { w =>
    w.sql("CREATE TABLE tbl (id INT, value STRING) USING delta")
    w.sql("INSERT INTO tbl SELECT id, CAST(id AS STRING) FROM RANGE(1, 101)")
    w.sql("INSERT INTO tbl SELECT id, CAST(id AS STRING) FROM RANGE(101, 201)")
    w.sql("INSERT INTO tbl SELECT id, CAST(id AS STRING) FROM RANGE(201, 301)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, predicate = "id > 200", name = "read_gt_200")
    w.read(t, predicate = "id <= 200", name = "read_le_200")
    w.snapshot(t)
  }

  test("dsk_002_string_equality", "STRING column equality predicate",
      "write", "data_skipping", "string") { w =>
    w.sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
    w.sql("INSERT INTO tbl VALUES (1, 'alpha'), (2, 'avocado'), (3, 'apple')")
    w.sql("INSERT INTO tbl VALUES (4, 'mango'), (5, 'melon'), (6, 'mint')")
    w.sql("INSERT INTO tbl VALUES (7, 'zebra'), (8, 'zinc'), (9, 'zucchini')")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, predicate = "name = 'mango'", name = "read_eq_mango")
    w.read(t, predicate = "name = 'zebra'", name = "read_eq_zebra")
    w.snapshot(t)
  }

  test("dsk_003_date_range", "DATE column range predicate",
      "write", "data_skipping", "date") { w =>
    w.sql("CREATE TABLE tbl (id INT, dt DATE) USING delta")
    w.sql("INSERT INTO tbl VALUES (1, DATE'2023-01-15'), (2, DATE'2023-06-30'), (3, DATE'2023-12-31')")
    w.sql("INSERT INTO tbl VALUES (4, DATE'2024-01-15'), (5, DATE'2024-03-20'), (6, DATE'2024-06-30')")
    w.sql("INSERT INTO tbl VALUES (7, DATE'2024-07-01'), (8, DATE'2024-10-15'), (9, DATE'2024-12-31')")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, predicate = "dt > DATE'2024-01-01'", name = "read_gt_2024")
    w.read(t, predicate = "dt <= DATE'2023-12-31'", name = "read_le_2023")
    w.snapshot(t)
  }

  test("dsk_004_timestamp_range", "TIMESTAMP column range predicate",
      "write", "data_skipping", "timestamp") { w =>
    w.sql("CREATE TABLE tbl (id INT, ts TIMESTAMP) USING delta")
    w.sql(
      "INSERT INTO tbl VALUES " +
      "(1, TIMESTAMP'2024-01-15 08:00:00'), " +
      "(2, TIMESTAMP'2024-01-15 09:30:00'), " +
      "(3, TIMESTAMP'2024-01-15 11:00:00')")
    w.sql(
      "INSERT INTO tbl VALUES " +
      "(4, TIMESTAMP'2024-01-15 13:00:00'), " +
      "(5, TIMESTAMP'2024-01-15 15:30:00'), " +
      "(6, TIMESTAMP'2024-01-15 17:00:00')")
    w.sql(
      "INSERT INTO tbl VALUES " +
      "(7, TIMESTAMP'2024-01-15 19:00:00'), " +
      "(8, TIMESTAMP'2024-01-15 21:30:00'), " +
      "(9, TIMESTAMP'2024-01-15 23:00:00')")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, predicate = "ts >= TIMESTAMP'2024-01-15 13:00:00'", name = "read_afternoon_plus")
    w.read(t, predicate = "ts < TIMESTAMP'2024-01-15 12:00:00'", name = "read_morning_only")
    w.snapshot(t)
  }

  test("dsk_005_decimal_range", "DECIMAL column range predicate",
      "write", "data_skipping", "decimal") { w =>
    w.sql("CREATE TABLE tbl (id INT, amount DECIMAL(10,2)) USING delta")
    w.sql("INSERT INTO tbl VALUES (1, 1.50), (2, 5.75), (3, 9.99)")
    w.sql("INSERT INTO tbl VALUES (4, 100.00), (5, 250.50), (6, 499.99)")
    w.sql("INSERT INTO tbl VALUES (7, 1000.00), (8, 5000.50), (9, 9999.99)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, predicate = "amount > 500.00", name = "read_gt_500")
    w.read(t, predicate = "amount <= 10.00", name = "read_le_10")
    w.snapshot(t)
  }

  test("dsk_006_compound_and", "Compound AND predicate",
      "write", "data_skipping", "compound_predicate") { w =>
    w.sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
    w.sql("INSERT INTO tbl VALUES (1, 'foo'), (2, 'foo'), (50, 'foo')")
    w.sql("INSERT INTO tbl VALUES (101, 'bar'), (150, 'bar'), (200, 'bar')")
    w.sql("INSERT INTO tbl VALUES (101, 'foo'), (200, 'foo'), (300, 'foo')")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, predicate = "id > 100 AND name = 'foo'", name = "read_compound")
    w.read(t, predicate = "id > 100", name = "read_id_only")
    w.snapshot(t)
  }

  test("dsk_007_or_predicate", "OR predicate on edges",
      "write", "data_skipping", "or_predicate") { w =>
    w.sql("CREATE TABLE tbl (id INT, value STRING) USING delta")
    w.sql("INSERT INTO tbl SELECT id, CAST(id AS STRING) FROM RANGE(1, 10)")
    w.sql("INSERT INTO tbl SELECT id, CAST(id AS STRING) FROM RANGE(500, 510)")
    w.sql("INSERT INTO tbl SELECT id, CAST(id AS STRING) FROM RANGE(991, 1001)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, predicate = "id < 10 OR id > 990", name = "read_or_edges")
    w.read(t, predicate = "id >= 500 AND id < 510", name = "read_middle")
    w.snapshot(t)
  }

  test("dsk_008_null_predicates", "IS NULL and IS NOT NULL predicates",
      "write", "data_skipping", "null_predicate") { w =>
    w.sql("CREATE TABLE tbl (id INT, value STRING) USING delta")
    w.sql("INSERT INTO tbl VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    w.sql("INSERT INTO tbl VALUES (4, NULL), (5, NULL), (6, NULL)")
    w.sql("INSERT INTO tbl VALUES (7, 'd'), (8, NULL), (9, 'e')")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, predicate = "value IS NOT NULL", name = "read_not_null")
    w.read(t, predicate = "value IS NULL", name = "read_is_null")
    w.snapshot(t)
  }

  test("dsk_009_multi_column_stats", "Multi-column stats filtering",
      "write", "data_skipping", "multi_column") { w =>
    w.sql("CREATE TABLE tbl (id INT, category STRING, score DOUBLE) USING delta")
    w.sql("INSERT INTO tbl VALUES (1, 'A', 10.0), (2, 'A', 20.0), (3, 'A', 30.0)")
    w.sql("INSERT INTO tbl VALUES (4, 'B', 50.0), (5, 'B', 60.0), (6, 'B', 70.0)")
    w.sql("INSERT INTO tbl VALUES (7, 'C', 90.0), (8, 'C', 95.0), (9, 'C', 100.0)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, predicate = "score > 80.0", name = "read_score_gt_80")
    w.read(t, predicate = "category = 'B'", name = "read_category_B")
    w.read(t, predicate = "id <= 3", name = "read_id_le_3")
    w.snapshot(t)
  }

  test("dsk_010_between", "BETWEEN predicate",
      "write", "data_skipping", "between_predicate") { w =>
    w.sql("CREATE TABLE tbl (id INT, value STRING) USING delta")
    w.sql("INSERT INTO tbl SELECT id, CAST(id AS STRING) FROM RANGE(1, 51)")
    w.sql("INSERT INTO tbl SELECT id, CAST(id AS STRING) FROM RANGE(51, 101)")
    w.sql("INSERT INTO tbl SELECT id, CAST(id AS STRING) FROM RANGE(101, 151)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, predicate = "id BETWEEN 60 AND 90", name = "read_between_60_90")
    w.read(t, predicate = "id BETWEEN 1 AND 50", name = "read_between_1_50")
    w.snapshot(t)
  }

  test("dsk_011_in_predicate", "IN predicate",
      "write", "data_skipping", "in_predicate") { w =>
    w.sql("CREATE TABLE tbl (id INT, value STRING) USING delta")
    w.sql("INSERT INTO tbl SELECT id, CAST(id AS STRING) FROM RANGE(1, 11)")
    w.sql("INSERT INTO tbl SELECT id, CAST(id AS STRING) FROM RANGE(101, 111)")
    w.sql("INSERT INTO tbl SELECT id, CAST(id AS STRING) FROM RANGE(201, 211)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, predicate = "id IN (1, 5, 10)", name = "read_in_low")
    w.read(t, predicate = "id IN (1, 105, 210)", name = "read_in_mixed")
    w.snapshot(t)
  }

  test("dsk_012_nested_struct_field", "Nested struct field predicate",
      "write", "data_skipping", "nested_struct") { w =>
    w.sql("CREATE TABLE tbl (id INT, info STRUCT<name: STRING, score: INT>) USING delta")
    w.sql(
      "INSERT INTO tbl VALUES " +
      "(1, named_struct('name', 'Alice', 'score', 10)), " +
      "(2, named_struct('name', 'Bob', 'score', 20))")
    w.sql(
      "INSERT INTO tbl VALUES " +
      "(3, named_struct('name', 'Carol', 'score', 50)), " +
      "(4, named_struct('name', 'Dave', 'score', 60))")
    w.sql(
      "INSERT INTO tbl VALUES " +
      "(5, named_struct('name', 'Eve', 'score', 90)), " +
      "(6, named_struct('name', 'Frank', 'score', 100))")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, predicate = "info.score > 80", name = "read_high_score")
    w.read(t, predicate = "info.score <= 20", name = "read_low_score")
    w.snapshot(t)
  }

  test("dsk_013_many_files", "Many files (6 inserts) with selective predicate",
      "write", "data_skipping", "many_files") { w =>
    w.sql("CREATE TABLE tbl (id INT, value STRING) USING delta")
    w.sql("INSERT INTO tbl SELECT id, CAST(id AS STRING) FROM RANGE(1, 101)")
    w.sql("INSERT INTO tbl SELECT id, CAST(id AS STRING) FROM RANGE(101, 201)")
    w.sql("INSERT INTO tbl SELECT id, CAST(id AS STRING) FROM RANGE(201, 301)")
    w.sql("INSERT INTO tbl SELECT id, CAST(id AS STRING) FROM RANGE(301, 401)")
    w.sql("INSERT INTO tbl SELECT id, CAST(id AS STRING) FROM RANGE(401, 501)")
    w.sql("INSERT INTO tbl SELECT id, CAST(id AS STRING) FROM RANGE(501, 601)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, predicate = "id > 500", name = "read_gt_500")
    w.read(t, predicate = "id <= 100", name = "read_le_100")
    w.read(t, predicate = "id BETWEEN 250 AND 350", name = "read_between_250_350")
    w.snapshot(t)
  }

  test("dsk_014_delete_then_read", "Delete then read with predicate",
      "write", "data_skipping", "delete", "dv") { w =>
    w.sql("CREATE TABLE tbl (id INT, value STRING) USING delta")
    w.sql("INSERT INTO tbl SELECT id, CAST(id AS STRING) FROM RANGE(1, 51)")
    w.sql("INSERT INTO tbl SELECT id, CAST(id AS STRING) FROM RANGE(51, 101)")
    w.sql("INSERT INTO tbl SELECT id, CAST(id AS STRING) FROM RANGE(101, 151)")
    w.sql("DELETE FROM tbl WHERE id >= 60 AND id <= 70")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, predicate = "id > 100", name = "read_gt_100")
    w.read(t, predicate = "id <= 50", name = "read_le_50")
    w.read(t, predicate = "id BETWEEN 55 AND 75", name = "read_between_55_75")
    w.snapshot(t)
  }

  test("dsk_015_update_then_read", "Update then read with predicate",
      "write", "data_skipping", "update") { w =>
    w.sql("CREATE TABLE tbl (id INT, value STRING) USING delta")
    w.sql("INSERT INTO tbl SELECT id, CAST(id AS STRING) FROM RANGE(1, 51)")
    w.sql("INSERT INTO tbl SELECT id, CAST(id AS STRING) FROM RANGE(51, 101)")
    w.sql("INSERT INTO tbl SELECT id, CAST(id AS STRING) FROM RANGE(101, 151)")
    w.sql("UPDATE tbl SET value = 'updated' WHERE id >= 60 AND id <= 70")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, predicate = "id > 100", name = "read_gt_100")
    w.read(t, predicate = "id <= 50", name = "read_le_50")
    w.read(t, predicate = "value = 'updated'", name = "read_updated")
    w.snapshot(t)
  }

}.runAll()
