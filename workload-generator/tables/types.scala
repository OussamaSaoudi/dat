/**
 * Types, edge cases, time travel, and error handling.
 */

new WorkloadSuite("types") {

  // === Basic Types ===

  test("all_primitive_types", "Every primitive Delta type", "types") {
    sql("""CREATE TABLE tbl (
      int_col INT, long_col BIGINT, double_col DOUBLE, float_col FLOAT,
      string_col STRING, bool_col BOOLEAN, binary_col BINARY,
      decimal_col DECIMAL(18,6), date_col DATE, ts_col TIMESTAMP
    ) USING delta""")
    sql("""INSERT INTO tbl VALUES
      (1, 100000000000, 3.14, 2.718, 'hello', true, X'DEADBEEF', 123456.789012, DATE'2024-01-15', TIMESTAMP'2024-01-15 10:30:00'),
      (2, 200000000000, -1.5, 0.0, 'world', false, X'CAFEBABE', -99999.000001, DATE'2025-06-30', TIMESTAMP'2025-06-30 23:59:59'),
      (42, 0, 0.0, -1.0, '', true, X'00', 0.000000, DATE'1970-01-01', TIMESTAMP'1970-01-01 00:00:00')""")
    val t = registerTable("tbl")
    read(t)
    read(t, columns = Seq("int_col", "string_col"))
    snapshot(t)
  }

  test("nested_types", "Struct, array, and map columns", "types") {
    sql("""CREATE TABLE tbl (
      id INT, info STRUCT<name: STRING, age: INT>,
      tags ARRAY<STRING>, props MAP<STRING, INT>
    ) USING delta""")
    sql("""INSERT INTO tbl VALUES
      (1, named_struct('name','alice','age',30), array('a','b'), map('x',1,'y',2)),
      (2, named_struct('name','bob','age',25), array('c'), map('z',3))""")
    val t = registerTable("tbl")
    read(t)
    read(t, columns = Seq("id", "info"))
    snapshot(t)
  }

  test("null_values", "NULLs across types", "types", "nulls") {
    sql("CREATE TABLE tbl (int_col INT, string_col STRING, double_col DOUBLE) USING delta")
    sql("INSERT INTO tbl VALUES (NULL, NULL, NULL)")
    sql("INSERT INTO tbl VALUES (1, 'not null', 1.5)")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "int_col IS NULL")
    read(t, predicate = "int_col IS NOT NULL")
    snapshot(t)
  }

  test("empty_table", "Zero rows", "types", "edge") {
    sql("CREATE TABLE tbl (id INT, value STRING) USING delta")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("single_row", "Exactly one row", "types", "edge") {
    sql("CREATE TABLE tbl (id INT, value STRING) USING delta")
    sql("INSERT INTO tbl VALUES (1, 'only')")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("large_table", "10000 rows", "types", "scale") {
    sql("CREATE TABLE tbl (id BIGINT, value DOUBLE, category STRING) USING delta")
    sql("""INSERT INTO tbl
      SELECT id, rand() as value,
        CASE WHEN id % 5 = 0 THEN 'A' WHEN id % 5 = 1 THEN 'B'
             WHEN id % 5 = 2 THEN 'C' WHEN id % 5 = 3 THEN 'D' ELSE 'E' END
      FROM range(10000)""")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "category = 'A'")
    read(t, columns = Seq("id", "category"))
    snapshot(t)
  }

  test("time_travel", "Multiple versions", "time_travel") {
    sql("CREATE TABLE tbl (id INT, val STRING) USING delta")
    sql("INSERT INTO tbl VALUES (1, 'v1')")
    sql("INSERT INTO tbl VALUES (2, 'v2')")
    sql("UPDATE tbl SET val = 'updated' WHERE id = 1")
    val t = registerTable("tbl")
    read(t)
    read(t, version = 0)
    read(t, version = 1)
    read(t, version = 2)
    snapshot(t)
    snapshot(t, version = 0)
  }

  test("error_bad_version", "Non-existent version", "error") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    sql("INSERT INTO tbl VALUES (1)")
    val t = registerTable("tbl")
    read(t)
    read(t, version = 999)
    snapshot(t)
  }

  test("error_cdf_not_enabled", "CDF on table without CDF", "error", "cdf") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    sql("INSERT INTO tbl VALUES (1)")
    val t = registerTable("tbl")
    read(t)
    cdf(t, startVersion = 0)
    snapshot(t)
  }

  // === Void Type ===

  test("void_001_void_top_level", "Top-level NullType column", "void", "unsupportedType") {
    sql("""CREATE TABLE tbl (id INT, void_col VOID) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id, null FROM range(5)")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("void_002_void_nested_struct", "NullType inside struct", "void", "unsupportedType") {
    sql("""CREATE TABLE tbl (
      id INT,
      info STRUCT<name: STRING, void_field: VOID>
    ) USING delta TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("""INSERT INTO tbl SELECT id, named_struct('name', CAST(id AS STRING), 'void_field', null) FROM range(3)""")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("void_005_void_schema_evolution", "NullType added via schema evolution", "void", "unsupportedType") {
    sql("""CREATE TABLE tbl (id INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1),(2),(3)")
    sql("ALTER TABLE tbl ADD COLUMN (void_col VOID)")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("void_006_void_multiple_columns", "Multiple NullType columns", "void", "unsupportedType") {
    sql("""CREATE TABLE tbl (id INT, void_a VOID, void_b VOID) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id, null, null FROM range(3)")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("void_007_void_with_backticks", "NullType column with special name", "void", "unsupportedType") {
    sql("""CREATE TABLE tbl (id INT, `my.void` VOID) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id, null FROM range(3)")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("void_in_struct", "NullType in struct field", "void", "unsupportedType") {
    sql("""CREATE TABLE tbl (
      id INT,
      info STRUCT<label: STRING, void_val: VOID>
    ) USING delta TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("""INSERT INTO tbl SELECT id, named_struct('label', CAST(id AS STRING), 'void_val', null) FROM range(3)""")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  // === Interval Type ===

  test("intv_001_interval_ym_basic", "YearMonthIntervalType column", "interval", "unsupportedType") {
    sql("""CREATE TABLE tbl (id INT, period INTERVAL YEAR TO MONTH) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, INTERVAL '1-6' YEAR TO MONTH)")
    sql("INSERT INTO tbl VALUES (2, INTERVAL '2-3' YEAR TO MONTH),(3, INTERVAL '0-9' YEAR TO MONTH)")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("intv_002_interval_dt_basic", "DayTimeIntervalType column", "interval", "unsupportedType") {
    sql("""CREATE TABLE tbl (id INT, duration INTERVAL DAY TO SECOND) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, INTERVAL '1 02:30:00' DAY TO SECOND)")
    sql("INSERT INTO tbl VALUES (2, INTERVAL '3 06:45:30' DAY TO SECOND),(3, INTERVAL '0 00:15:00' DAY TO SECOND)")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("intv_003_interval_partitioned", "Partitioned by interval", "interval", "unsupportedType") {
    sql("""CREATE TABLE tbl (id INT, period INTERVAL YEAR TO MONTH) USING delta
      PARTITIONED BY (period) TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, INTERVAL '1-0' YEAR TO MONTH)")
    sql("INSERT INTO tbl VALUES (2, INTERVAL '2-0' YEAR TO MONTH)")
    sql("INSERT INTO tbl VALUES (3, INTERVAL '1-0' YEAR TO MONTH)")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("intv_004_interval_negative", "Negative interval values", "interval", "unsupportedType") {
    sql("""CREATE TABLE tbl (id INT, period INTERVAL YEAR TO MONTH) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, INTERVAL '-1-6' YEAR TO MONTH)")
    sql("INSERT INTO tbl VALUES (2, INTERVAL '-0-3' YEAR TO MONTH)")
    sql("INSERT INTO tbl VALUES (3, INTERVAL '0-0' YEAR TO MONTH)")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("intv_005_interval_mixed", "Both YM and DT interval columns", "interval", "unsupportedType") {
    sql("""CREATE TABLE tbl (
      id INT,
      period INTERVAL YEAR TO MONTH,
      duration INTERVAL DAY TO SECOND
    ) USING delta TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("""INSERT INTO tbl VALUES
      (1, INTERVAL '1-0' YEAR TO MONTH, INTERVAL '1 00:00:00' DAY TO SECOND),
      (2, INTERVAL '0-6' YEAR TO MONTH, INTERVAL '0 12:30:00' DAY TO SECOND)""")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("intv_boundary_values", "Max/min interval values", "interval", "unsupportedType") {
    sql("""CREATE TABLE tbl (
      id INT,
      period INTERVAL YEAR TO MONTH,
      duration INTERVAL DAY TO SECOND
    ) USING delta TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("""INSERT INTO tbl VALUES
      (1, INTERVAL '178956970-7' YEAR TO MONTH, INTERVAL '106751991 04:00:54.775807' DAY TO SECOND),
      (2, INTERVAL '-178956970-8' YEAR TO MONTH, INTERVAL '-106751991 04:00:54.775808' DAY TO SECOND)""")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("intv_sub_second", "Sub-second DayTimeInterval", "interval", "unsupportedType") {
    sql("""CREATE TABLE tbl (id INT, duration INTERVAL DAY TO SECOND) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("""INSERT INTO tbl VALUES
      (1, INTERVAL '0 00:00:00.001' DAY TO SECOND),
      (2, INTERVAL '0 00:00:00.999999' DAY TO SECOND),
      (3, INTERVAL '0 00:00:01.5' DAY TO SECOND)""")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  // === Timestamp NTZ ===

  test("ntz_basic", "Table with TIMESTAMP_NTZ column", "timestampNTZ") {
    sql("""CREATE TABLE tbl (id INT, ts TIMESTAMP_NTZ) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("""INSERT INTO tbl VALUES
      (1, TIMESTAMP_NTZ'2024-01-15 10:30:00'),
      (2, TIMESTAMP_NTZ'2024-06-20 14:00:00'),
      (3, TIMESTAMP_NTZ'2024-12-31 23:59:59')""")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "ts > TIMESTAMP_NTZ'2024-06-01 00:00:00'")
    snapshot(t)
  }

  test("ntz_far_past", "NTZ with old date value", "timestampNTZ") {
    sql("""CREATE TABLE tbl (id INT, ts TIMESTAMP_NTZ) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("""INSERT INTO tbl VALUES
      (1, TIMESTAMP_NTZ'1800-01-01 00:00:00'),
      (2, TIMESTAMP_NTZ'1899-12-31 23:59:59'),
      (3, TIMESTAMP_NTZ'1970-01-01 00:00:00')""")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "ts < TIMESTAMP_NTZ'1900-01-01 00:00:00'")
    snapshot(t)
  }

  test("ntz_mixed_tz_ntz", "Both TIMESTAMP and TIMESTAMP_NTZ columns", "timestampNTZ") {
    sql("""CREATE TABLE tbl (id INT, ts_tz TIMESTAMP, ts_ntz TIMESTAMP_NTZ) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("""INSERT INTO tbl VALUES
      (1, TIMESTAMP'2024-01-15 10:00:00', TIMESTAMP_NTZ'2024-01-15 10:00:00'),
      (2, TIMESTAMP'2024-06-20 14:00:00', TIMESTAMP_NTZ'2024-06-20 14:00:00'),
      (3, TIMESTAMP'2024-12-31 23:59:59', TIMESTAMP_NTZ'2024-12-31 23:59:59')""")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "ts_ntz >= TIMESTAMP_NTZ'2024-06-01 00:00:00'")
    snapshot(t)
  }

  test("ntz_partition", "TIMESTAMP_NTZ as partition column", "timestampNTZ") {
    sql("""CREATE TABLE tbl (id INT, value STRING, ts_part TIMESTAMP_NTZ) USING delta
      PARTITIONED BY (ts_part) TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("""INSERT INTO tbl VALUES
      (1, 'a', TIMESTAMP_NTZ'2024-01-01 00:00:00'),
      (2, 'b', TIMESTAMP_NTZ'2024-02-01 00:00:00'),
      (3, 'c', TIMESTAMP_NTZ'2024-01-01 00:00:00'),
      (4, 'd', TIMESTAMP_NTZ'2024-03-01 00:00:00')""")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "ts_part = TIMESTAMP_NTZ'2024-01-01 00:00:00'")
    read(t, predicate = "ts_part >= TIMESTAMP_NTZ'2024-02-01 00:00:00'")
    snapshot(t)
  }

  test("ntz_stats", "Data skipping with NTZ min/max stats", "timestampNTZ") {
    sql("""CREATE TABLE tbl (id INT, ts TIMESTAMP_NTZ) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, TIMESTAMP_NTZ'2024-01-15 00:00:00'),(2, TIMESTAMP_NTZ'2024-03-20 00:00:00')")
    sql("INSERT INTO tbl VALUES (3, TIMESTAMP_NTZ'2024-06-15 00:00:00'),(4, TIMESTAMP_NTZ'2024-06-20 00:00:00')")
    sql("INSERT INTO tbl VALUES (5, TIMESTAMP_NTZ'2024-12-01 00:00:00'),(6, TIMESTAMP_NTZ'2024-12-31 00:00:00')")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "ts >= TIMESTAMP_NTZ'2024-06-01 00:00:00' AND ts < TIMESTAMP_NTZ'2024-07-01 00:00:00'")
    read(t, predicate = "ts >= TIMESTAMP_NTZ'2024-12-01 00:00:00'")
    snapshot(t)
  }

  test("tntz_column_mapping", "NTZ with column mapping", "timestampNTZ", "columnMapping") {
    sql("""CREATE TABLE tbl (id INT, event_time TIMESTAMP_NTZ) USING delta
      TBLPROPERTIES (
        'delta.columnMapping.mode' = 'name',
        'delta.enableDeletionVectors' = 'true')""")
    sql("""INSERT INTO tbl VALUES
      (1, TIMESTAMP_NTZ'2024-01-15 10:00:00'),
      (2, TIMESTAMP_NTZ'2024-06-20 14:00:00'),
      (3, TIMESTAMP_NTZ'2024-12-31 23:59:59')""")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "event_time > TIMESTAMP_NTZ'2024-06-01 00:00:00'")
    snapshot(t)
  }

  test("tntz_epoch", "NTZ epoch value", "timestampNTZ") {
    sql("""CREATE TABLE tbl (id INT, ts TIMESTAMP_NTZ) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("""INSERT INTO tbl VALUES
      (1, TIMESTAMP_NTZ'1970-01-01 00:00:00'),
      (2, TIMESTAMP_NTZ'2024-01-01 00:00:00')""")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "ts = TIMESTAMP_NTZ'1970-01-01 00:00:00'")
    snapshot(t)
  }

  test("tntz_partition_filter", "NTZ partition filter", "timestampNTZ") {
    sql("""CREATE TABLE tbl (id INT, value STRING, ts_part TIMESTAMP_NTZ) USING delta
      PARTITIONED BY (ts_part) TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("""INSERT INTO tbl VALUES
      (1, 'a', TIMESTAMP_NTZ'2024-01-01 00:00:00'),
      (2, 'b', TIMESTAMP_NTZ'2024-06-15 00:00:00'),
      (3, 'c', TIMESTAMP_NTZ'2024-12-25 00:00:00')""")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "ts_part = TIMESTAMP_NTZ'2024-01-01 00:00:00'")
    read(t, predicate = "ts_part > TIMESTAMP_NTZ'2024-06-01 00:00:00'")
    snapshot(t)
  }

  test("tntz_time_travel", "NTZ with version-based time travel", "timestampNTZ") {
    sql("""CREATE TABLE tbl (id INT, ts TIMESTAMP_NTZ) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, TIMESTAMP_NTZ'2024-01-01 00:00:00')")
    sql("INSERT INTO tbl VALUES (2, TIMESTAMP_NTZ'2024-06-01 00:00:00')")
    sql("INSERT INTO tbl VALUES (3, TIMESTAMP_NTZ'2024-12-01 00:00:00')")
    val t = registerTable("tbl")
    read(t)
    read(t, version = 1)
    read(t, version = 2)
    snapshot(t)
  }

}
