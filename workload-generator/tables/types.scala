/**
 * Types, edge cases, time travel, and error handling.
 */

new WorkloadSuite("types") {

  // === Basic Types ===

  test("all_primitive_types", "Every primitive Delta type", "types") { w =>
    w.sql("""CREATE TABLE tbl (
      int_col INT, long_col BIGINT, double_col DOUBLE, float_col FLOAT,
      string_col STRING, bool_col BOOLEAN, binary_col BINARY,
      decimal_col DECIMAL(18,6), date_col DATE, ts_col TIMESTAMP
    ) USING delta""")
    w.sql("""INSERT INTO tbl VALUES
      (1, 100000000000, 3.14, 2.718, 'hello', true, X'DEADBEEF', 123456.789012, DATE'2024-01-15', TIMESTAMP'2024-01-15 10:30:00'),
      (2, 200000000000, -1.5, 0.0, 'world', false, X'CAFEBABE', -99999.000001, DATE'2025-06-30', TIMESTAMP'2025-06-30 23:59:59'),
      (42, 0, 0.0, -1.0, '', true, X'00', 0.000000, DATE'1970-01-01', TIMESTAMP'1970-01-01 00:00:00')""")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, columns = Seq("int_col", "string_col"))
    w.snapshot(t)
  }

  test("nested_types", "Struct, array, and map columns", "types") { w =>
    w.sql("""CREATE TABLE tbl (
      id INT, info STRUCT<name: STRING, age: INT>,
      tags ARRAY<STRING>, props MAP<STRING, INT>
    ) USING delta""")
    w.sql("""INSERT INTO tbl VALUES
      (1, named_struct('name','alice','age',30), array('a','b'), map('x',1,'y',2)),
      (2, named_struct('name','bob','age',25), array('c'), map('z',3))""")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, columns = Seq("id", "info"))
    w.snapshot(t)
  }

  test("null_values", "NULLs across types", "types", "nulls") { w =>
    w.sql("CREATE TABLE tbl (int_col INT, string_col STRING, double_col DOUBLE) USING delta")
    w.sql("INSERT INTO tbl VALUES (NULL, NULL, NULL)")
    w.sql("INSERT INTO tbl VALUES (1, 'not null', 1.5)")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "int_col IS NULL")
    w.read(t, predicate = "int_col IS NOT NULL")
    w.snapshot(t)
  }

  test("empty_table", "Zero rows", "types", "edge") { w =>
    w.sql("CREATE TABLE tbl (id INT, value STRING) USING delta")
    val t = w.table("tbl")
    w.read(t)
    w.snapshot(t)
  }

  test("single_row", "Exactly one row", "types", "edge") { w =>
    w.sql("CREATE TABLE tbl (id INT, value STRING) USING delta")
    w.sql("INSERT INTO tbl VALUES (1, 'only')")
    val t = w.table("tbl")
    w.read(t)
    w.snapshot(t)
  }

  test("large_table", "10000 rows", "types", "scale") { w =>
    w.sql("CREATE TABLE tbl (id BIGINT, value DOUBLE, category STRING) USING delta")
    w.sql("""INSERT INTO tbl
      SELECT id, rand() as value,
        CASE WHEN id % 5 = 0 THEN 'A' WHEN id % 5 = 1 THEN 'B'
             WHEN id % 5 = 2 THEN 'C' WHEN id % 5 = 3 THEN 'D' ELSE 'E' END
      FROM range(10000)""")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "category = 'A'")
    w.read(t, columns = Seq("id", "category"))
    w.snapshot(t)
  }

  test("time_travel", "Multiple versions", "time_travel") { w =>
    w.sql("CREATE TABLE tbl (id INT, val STRING) USING delta")
    w.sql("INSERT INTO tbl VALUES (1, 'v1')")
    w.sql("INSERT INTO tbl VALUES (2, 'v2')")
    w.sql("UPDATE tbl SET val = 'updated' WHERE id = 1")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, version = 0)
    w.read(t, version = 1)
    w.read(t, version = 2)
    w.snapshot(t)
    w.snapshot(t, version = 0)
  }

  test("error_bad_version", "Non-existent version", "error") { w =>
    w.sql("CREATE TABLE tbl (id INT) USING delta")
    w.sql("INSERT INTO tbl VALUES (1)")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, version = 999)
    w.snapshot(t)
  }

  test("error_cdf_not_enabled", "CDF on table without CDF", "error", "cdf") { w =>
    w.sql("CREATE TABLE tbl (id INT) USING delta")
    w.sql("INSERT INTO tbl VALUES (1)")
    val t = w.table("tbl")
    w.read(t)
    w.cdf(t, startVersion = 0)
    w.snapshot(t)
  }

  // === Void Type ===

  test("void_001_void_top_level", "Top-level NullType column", "void", "unsupportedType") { w =>
    w.sql("""CREATE TABLE tbl (id INT, void_col VOID) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id, null FROM range(5)")
    val t = w.table("tbl")
    w.read(t)
    w.snapshot(t)
  }

  test("void_002_void_nested_struct", "NullType inside struct", "void", "unsupportedType") { w =>
    w.sql("""CREATE TABLE tbl (
      id INT,
      info STRUCT<name: STRING, void_field: VOID>
    ) USING delta TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("""INSERT INTO tbl SELECT id, named_struct('name', CAST(id AS STRING), 'void_field', null) FROM range(3)""")
    val t = w.table("tbl")
    w.read(t)
    w.snapshot(t)
  }

  test("void_005_void_schema_evolution", "NullType added via schema evolution", "void", "unsupportedType") { w =>
    w.sql("""CREATE TABLE tbl (id INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1),(2),(3)")
    w.sql("ALTER TABLE tbl ADD COLUMN (void_col VOID)")
    val t = w.table("tbl")
    w.read(t)
    w.snapshot(t)
  }

  test("void_006_void_multiple_columns", "Multiple NullType columns", "void", "unsupportedType") { w =>
    w.sql("""CREATE TABLE tbl (id INT, void_a VOID, void_b VOID) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id, null, null FROM range(3)")
    val t = w.table("tbl")
    w.read(t)
    w.snapshot(t)
  }

  test("void_007_void_with_backticks", "NullType column with special name", "void", "unsupportedType") { w =>
    w.sql("""CREATE TABLE tbl (id INT, `my.void` VOID) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id, null FROM range(3)")
    val t = w.table("tbl")
    w.read(t)
    w.snapshot(t)
  }

  test("void_in_struct", "NullType in struct field", "void", "unsupportedType") { w =>
    w.sql("""CREATE TABLE tbl (
      id INT,
      info STRUCT<label: STRING, void_val: VOID>
    ) USING delta TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("""INSERT INTO tbl SELECT id, named_struct('label', CAST(id AS STRING), 'void_val', null) FROM range(3)""")
    val t = w.table("tbl")
    w.read(t)
    w.snapshot(t)
  }

  // === Interval Type ===

  test("intv_001_interval_ym_basic", "YearMonthIntervalType column", "interval", "unsupportedType") { w =>
    w.sql("""CREATE TABLE tbl (id INT, period INTERVAL YEAR TO MONTH) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1, INTERVAL '1-6' YEAR TO MONTH)")
    w.sql("INSERT INTO tbl VALUES (2, INTERVAL '2-3' YEAR TO MONTH),(3, INTERVAL '0-9' YEAR TO MONTH)")
    val t = w.table("tbl")
    w.read(t)
    w.snapshot(t)
  }

  test("intv_002_interval_dt_basic", "DayTimeIntervalType column", "interval", "unsupportedType") { w =>
    w.sql("""CREATE TABLE tbl (id INT, duration INTERVAL DAY TO SECOND) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1, INTERVAL '1 02:30:00' DAY TO SECOND)")
    w.sql("INSERT INTO tbl VALUES (2, INTERVAL '3 06:45:30' DAY TO SECOND),(3, INTERVAL '0 00:15:00' DAY TO SECOND)")
    val t = w.table("tbl")
    w.read(t)
    w.snapshot(t)
  }

  test("intv_003_interval_partitioned", "Partitioned by interval", "interval", "unsupportedType") { w =>
    w.sql("""CREATE TABLE tbl (id INT, period INTERVAL YEAR TO MONTH) USING delta
      PARTITIONED BY (period) TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1, INTERVAL '1-0' YEAR TO MONTH)")
    w.sql("INSERT INTO tbl VALUES (2, INTERVAL '2-0' YEAR TO MONTH)")
    w.sql("INSERT INTO tbl VALUES (3, INTERVAL '1-0' YEAR TO MONTH)")
    val t = w.table("tbl")
    w.read(t)
    w.snapshot(t)
  }

  test("intv_004_interval_negative", "Negative interval values", "interval", "unsupportedType") { w =>
    w.sql("""CREATE TABLE tbl (id INT, period INTERVAL YEAR TO MONTH) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1, INTERVAL '-1-6' YEAR TO MONTH)")
    w.sql("INSERT INTO tbl VALUES (2, INTERVAL '-0-3' YEAR TO MONTH)")
    w.sql("INSERT INTO tbl VALUES (3, INTERVAL '0-0' YEAR TO MONTH)")
    val t = w.table("tbl")
    w.read(t)
    w.snapshot(t)
  }

  test("intv_005_interval_mixed", "Both YM and DT interval columns", "interval", "unsupportedType") { w =>
    w.sql("""CREATE TABLE tbl (
      id INT,
      period INTERVAL YEAR TO MONTH,
      duration INTERVAL DAY TO SECOND
    ) USING delta TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("""INSERT INTO tbl VALUES
      (1, INTERVAL '1-0' YEAR TO MONTH, INTERVAL '1 00:00:00' DAY TO SECOND),
      (2, INTERVAL '0-6' YEAR TO MONTH, INTERVAL '0 12:30:00' DAY TO SECOND)""")
    val t = w.table("tbl")
    w.read(t)
    w.snapshot(t)
  }

  test("intv_boundary_values", "Max/min interval values", "interval", "unsupportedType") { w =>
    w.sql("""CREATE TABLE tbl (
      id INT,
      period INTERVAL YEAR TO MONTH,
      duration INTERVAL DAY TO SECOND
    ) USING delta TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("""INSERT INTO tbl VALUES
      (1, INTERVAL '178956970-7' YEAR TO MONTH, INTERVAL '106751991 04:00:54.775807' DAY TO SECOND),
      (2, INTERVAL '-178956970-8' YEAR TO MONTH, INTERVAL '-106751991 04:00:54.775808' DAY TO SECOND)""")
    val t = w.table("tbl")
    w.read(t)
    w.snapshot(t)
  }

  test("intv_sub_second", "Sub-second DayTimeInterval", "interval", "unsupportedType") { w =>
    w.sql("""CREATE TABLE tbl (id INT, duration INTERVAL DAY TO SECOND) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("""INSERT INTO tbl VALUES
      (1, INTERVAL '0 00:00:00.001' DAY TO SECOND),
      (2, INTERVAL '0 00:00:00.999999' DAY TO SECOND),
      (3, INTERVAL '0 00:00:01.5' DAY TO SECOND)""")
    val t = w.table("tbl")
    w.read(t)
    w.snapshot(t)
  }

  // === Timestamp NTZ ===

  test("ntz_basic", "Table with TIMESTAMP_NTZ column", "timestampNTZ") { w =>
    w.sql("""CREATE TABLE tbl (id INT, ts TIMESTAMP_NTZ) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("""INSERT INTO tbl VALUES
      (1, TIMESTAMP_NTZ'2024-01-15 10:30:00'),
      (2, TIMESTAMP_NTZ'2024-06-20 14:00:00'),
      (3, TIMESTAMP_NTZ'2024-12-31 23:59:59')""")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "ts > TIMESTAMP_NTZ'2024-06-01 00:00:00'")
    w.snapshot(t)
  }

  test("ntz_far_past", "NTZ with old date value", "timestampNTZ") { w =>
    w.sql("""CREATE TABLE tbl (id INT, ts TIMESTAMP_NTZ) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("""INSERT INTO tbl VALUES
      (1, TIMESTAMP_NTZ'1800-01-01 00:00:00'),
      (2, TIMESTAMP_NTZ'1899-12-31 23:59:59'),
      (3, TIMESTAMP_NTZ'1970-01-01 00:00:00')""")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "ts < TIMESTAMP_NTZ'1900-01-01 00:00:00'")
    w.snapshot(t)
  }

  test("ntz_mixed_tz_ntz", "Both TIMESTAMP and TIMESTAMP_NTZ columns", "timestampNTZ") { w =>
    w.sql("""CREATE TABLE tbl (id INT, ts_tz TIMESTAMP, ts_ntz TIMESTAMP_NTZ) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("""INSERT INTO tbl VALUES
      (1, TIMESTAMP'2024-01-15 10:00:00', TIMESTAMP_NTZ'2024-01-15 10:00:00'),
      (2, TIMESTAMP'2024-06-20 14:00:00', TIMESTAMP_NTZ'2024-06-20 14:00:00'),
      (3, TIMESTAMP'2024-12-31 23:59:59', TIMESTAMP_NTZ'2024-12-31 23:59:59')""")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "ts_ntz >= TIMESTAMP_NTZ'2024-06-01 00:00:00'")
    w.snapshot(t)
  }

  test("ntz_partition", "TIMESTAMP_NTZ as partition column", "timestampNTZ") { w =>
    w.sql("""CREATE TABLE tbl (id INT, value STRING, ts_part TIMESTAMP_NTZ) USING delta
      PARTITIONED BY (ts_part) TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("""INSERT INTO tbl VALUES
      (1, 'a', TIMESTAMP_NTZ'2024-01-01 00:00:00'),
      (2, 'b', TIMESTAMP_NTZ'2024-02-01 00:00:00'),
      (3, 'c', TIMESTAMP_NTZ'2024-01-01 00:00:00'),
      (4, 'd', TIMESTAMP_NTZ'2024-03-01 00:00:00')""")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "ts_part = TIMESTAMP_NTZ'2024-01-01 00:00:00'")
    w.read(t, predicate = "ts_part >= TIMESTAMP_NTZ'2024-02-01 00:00:00'")
    w.snapshot(t)
  }

  test("ntz_stats", "Data skipping with NTZ min/max stats", "timestampNTZ") { w =>
    w.sql("""CREATE TABLE tbl (id INT, ts TIMESTAMP_NTZ) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1, TIMESTAMP_NTZ'2024-01-15 00:00:00'),(2, TIMESTAMP_NTZ'2024-03-20 00:00:00')")
    w.sql("INSERT INTO tbl VALUES (3, TIMESTAMP_NTZ'2024-06-15 00:00:00'),(4, TIMESTAMP_NTZ'2024-06-20 00:00:00')")
    w.sql("INSERT INTO tbl VALUES (5, TIMESTAMP_NTZ'2024-12-01 00:00:00'),(6, TIMESTAMP_NTZ'2024-12-31 00:00:00')")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "ts >= TIMESTAMP_NTZ'2024-06-01 00:00:00' AND ts < TIMESTAMP_NTZ'2024-07-01 00:00:00'")
    w.read(t, predicate = "ts >= TIMESTAMP_NTZ'2024-12-01 00:00:00'")
    w.snapshot(t)
  }

  test("tntz_column_mapping", "NTZ with column mapping", "timestampNTZ", "columnMapping") { w =>
    w.sql("""CREATE TABLE tbl (id INT, event_time TIMESTAMP_NTZ) USING delta
      TBLPROPERTIES (
        'delta.columnMapping.mode' = 'name',
        'delta.enableDeletionVectors' = 'true')""")
    w.sql("""INSERT INTO tbl VALUES
      (1, TIMESTAMP_NTZ'2024-01-15 10:00:00'),
      (2, TIMESTAMP_NTZ'2024-06-20 14:00:00'),
      (3, TIMESTAMP_NTZ'2024-12-31 23:59:59')""")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "event_time > TIMESTAMP_NTZ'2024-06-01 00:00:00'")
    w.snapshot(t)
  }

  test("tntz_epoch", "NTZ epoch value", "timestampNTZ") { w =>
    w.sql("""CREATE TABLE tbl (id INT, ts TIMESTAMP_NTZ) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("""INSERT INTO tbl VALUES
      (1, TIMESTAMP_NTZ'1970-01-01 00:00:00'),
      (2, TIMESTAMP_NTZ'2024-01-01 00:00:00')""")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "ts = TIMESTAMP_NTZ'1970-01-01 00:00:00'")
    w.snapshot(t)
  }

  test("tntz_partition_filter", "NTZ partition filter", "timestampNTZ") { w =>
    w.sql("""CREATE TABLE tbl (id INT, value STRING, ts_part TIMESTAMP_NTZ) USING delta
      PARTITIONED BY (ts_part) TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("""INSERT INTO tbl VALUES
      (1, 'a', TIMESTAMP_NTZ'2024-01-01 00:00:00'),
      (2, 'b', TIMESTAMP_NTZ'2024-06-15 00:00:00'),
      (3, 'c', TIMESTAMP_NTZ'2024-12-25 00:00:00')""")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "ts_part = TIMESTAMP_NTZ'2024-01-01 00:00:00'")
    w.read(t, predicate = "ts_part > TIMESTAMP_NTZ'2024-06-01 00:00:00'")
    w.snapshot(t)
  }

  test("tntz_time_travel", "NTZ with version-based time travel", "timestampNTZ") { w =>
    w.sql("""CREATE TABLE tbl (id INT, ts TIMESTAMP_NTZ) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1, TIMESTAMP_NTZ'2024-01-01 00:00:00')")
    w.sql("INSERT INTO tbl VALUES (2, TIMESTAMP_NTZ'2024-06-01 00:00:00')")
    w.sql("INSERT INTO tbl VALUES (3, TIMESTAMP_NTZ'2024-12-01 00:00:00')")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, version = 1)
    w.read(t, version = 2)
    w.snapshot(t)
  }

}.runAll()
