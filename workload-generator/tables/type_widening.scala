/**
 * Type widening workloads: numeric chains, float->double, decimal precision,
 * date->timestampNTZ, nested fields, arrays, maps, partitions, DVs, CDF,
 * column mapping, data skipping, projections, and null handling.
 */
import io.delta.workload.WorkloadGenerator._

// ---------------------------------------------------------------------------
// Simple type widenings
// ---------------------------------------------------------------------------

workload("tw_byte_to_int", "Multiple successive type widenings (byte -> short -> int)", "type_widening") { w =>
  w.sql("""CREATE TABLE tbl (a BYTE) USING delta
    TBLPROPERTIES ('delta.enableTypeWidening' = 'true', 'delta.enableDeletionVectors' = 'true')""")
  // v1: insert byte-range values
  w.sql("INSERT INTO tbl VALUES (CAST(1 AS BYTE)), (CAST(127 AS BYTE))")
  // v2: widen byte -> short
  w.sql("ALTER TABLE tbl ALTER COLUMN a TYPE SHORT")
  // v3: insert short-range values
  w.sql("INSERT INTO tbl VALUES (CAST(128 AS SHORT)), (CAST(32767 AS SHORT))")
  // v4: widen short -> int
  w.sql("ALTER TABLE tbl ALTER COLUMN a TYPE INT")
  // v5: insert int-range values
  w.sql("INSERT INTO tbl VALUES (32768), (2147483647)")
  val t = w.table("tbl")
  w.read(t, name = "read_all")
  w.read(t, predicate = "a > 127", name = "read_gt_max_byte")
  w.read(t, predicate = "a > 32767", name = "read_gt_max_short")
  w.snapshotHistory(t)
}

workload("tw_short_to_int", "Short to int widening", "type_widening") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value SHORT) USING delta
    TBLPROPERTIES ('delta.enableTypeWidening' = 'true', 'delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1, CAST(100 AS SHORT)), (2, CAST(32767 AS SHORT))")
  w.sql("ALTER TABLE tbl ALTER COLUMN value TYPE INT")
  w.sql("INSERT INTO tbl VALUES (3, 40000), (4, 100000)")
  val t = w.table("tbl")
  w.read(t, name = "read_all")
  w.read(t, predicate = "value > 32767", name = "read_gt_max_short")
  w.snapshot(t)
}

workload("tw_short_to_long", "Short to long widening (via int)", "type_widening") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value SHORT) USING delta
    TBLPROPERTIES ('delta.enableTypeWidening' = 'true', 'delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1, CAST(100 AS SHORT)), (2, CAST(32767 AS SHORT))")
  w.sql("ALTER TABLE tbl ALTER COLUMN value TYPE INT")
  w.sql("INSERT INTO tbl VALUES (3, 40000), (4, 2147483647)")
  w.sql("ALTER TABLE tbl ALTER COLUMN value TYPE LONG")
  w.sql("INSERT INTO tbl VALUES (5, 3000000000L), (6, 9223372036854775807L)")
  val t = w.table("tbl")
  w.read(t, name = "read_all")
  w.read(t, predicate = "value > 32767", name = "read_gt_max_short")
  w.read(t, predicate = "value > 2147483647", name = "read_gt_max_int")
  w.snapshotHistory(t)
}

workload("tw_int_to_long", "Read after int to long widening", "type_widening") { w =>
  w.sql("""CREATE TABLE tbl (a INT) USING delta
    TBLPROPERTIES ('delta.enableTypeWidening' = 'true', 'delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1), (2147483647)")
  w.sql("ALTER TABLE tbl ALTER COLUMN a TYPE LONG")
  w.sql("INSERT INTO tbl VALUES (2147483648L), (9223372036854775807L)")
  val t = w.table("tbl")
  w.read(t, name = "read_all")
  w.read(t, predicate = "a > 2147483647", name = "read_gt_max_int")
  w.read(t, version = 1, name = "read_v1_before_widening")
  w.snapshotHistory(t)
}

workload("tw_full_numeric_chain", "Full widening chain: byte -> short -> int -> long", "type_widening") { w =>
  w.sql("""CREATE TABLE tbl (a BYTE) USING delta
    TBLPROPERTIES ('delta.enableTypeWidening' = 'true', 'delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (CAST(1 AS BYTE)), (CAST(100 AS BYTE))")
  w.sql("ALTER TABLE tbl ALTER COLUMN a TYPE SHORT")
  w.sql("INSERT INTO tbl VALUES (CAST(200 AS SHORT)), (CAST(30000 AS SHORT))")
  w.sql("ALTER TABLE tbl ALTER COLUMN a TYPE INT")
  w.sql("INSERT INTO tbl VALUES (40000), (2000000000)")
  w.sql("ALTER TABLE tbl ALTER COLUMN a TYPE LONG")
  w.sql("INSERT INTO tbl VALUES (3000000000L), (9000000000000000000L)")
  val t = w.table("tbl")
  w.read(t, name = "read_all")
  w.read(t, version = 1, name = "read_v1_byte_only")
  w.read(t, version = 3, name = "read_v3_through_short")
  w.read(t, version = 5, name = "read_v5_through_int")
  w.snapshotHistory(t)
}

workload("tw_float_to_double", "Float to double widening", "type_widening") { w =>
  w.sql("""CREATE TABLE tbl (value FLOAT) USING delta
    TBLPROPERTIES ('delta.enableTypeWidening' = 'true', 'delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (CAST(1.5 AS FLOAT)), (CAST(3.14 AS FLOAT))")
  w.sql("ALTER TABLE tbl ALTER COLUMN value TYPE DOUBLE")
  w.sql("INSERT INTO tbl VALUES (3.141592653589793), (1.7976931348623157E308)")
  val t = w.table("tbl")
  w.read(t, name = "read_all")
  w.read(t, predicate = "value > 100.0", name = "read_high_precision")
  w.snapshotHistory(t)
}

workload("tw_decimal_precision", "Decimal precision widening (5,2) -> (10,2)", "type_widening") { w =>
  w.sql("""CREATE TABLE tbl (amount DECIMAL(5,2)) USING delta
    TBLPROPERTIES ('delta.enableTypeWidening' = 'true', 'delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (123.45), (999.99)")
  w.sql("ALTER TABLE tbl ALTER COLUMN amount TYPE DECIMAL(10,2)")
  w.sql("INSERT INTO tbl VALUES (12345678.90), (99999999.99)")
  val t = w.table("tbl")
  w.read(t, name = "read_all")
  w.read(t, predicate = "amount > 1000", name = "read_large_values")
  w.snapshotHistory(t)
}

workload("tw_cross_physical_decimal", "Cross-physical-type decimal widening (INT32->INT64->FIXED)", "type_widening") { w =>
  w.sql("""CREATE TABLE tbl (amount DECIMAL(9,2)) USING delta
    TBLPROPERTIES ('delta.enableTypeWidening' = 'true', 'delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (123.45), (9999999.99)")
  w.sql("ALTER TABLE tbl ALTER COLUMN amount TYPE DECIMAL(18,2)")
  w.sql("INSERT INTO tbl VALUES (1234567890123.45), (9999999999999999.99)")
  w.sql("ALTER TABLE tbl ALTER COLUMN amount TYPE DECIMAL(28,3)")
  w.sql("INSERT INTO tbl VALUES (1234567890123456789012345.678)")
  val t = w.table("tbl")
  w.read(t, name = "read_all")
  w.read(t, version = 1, name = "read_v1_int32_only")
  w.read(t, version = 3, name = "read_v3_through_int64")
  w.snapshotHistory(t)
}

workload("tw_date_to_timestamp_ntz", "Date to TimestampNTZ widening", "type_widening") { w =>
  w.sql("""CREATE TABLE tbl (a DATE) USING delta
    TBLPROPERTIES ('delta.enableTypeWidening' = 'true', 'delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (DATE'2024-01-15'), (DATE'2024-06-30')")
  w.sql("ALTER TABLE tbl ALTER COLUMN a TYPE TIMESTAMP_NTZ")
  w.sql("INSERT INTO tbl VALUES (TIMESTAMP_NTZ'2024-12-31 23:59:59'), (TIMESTAMP_NTZ'2025-01-01 12:30:00')")
  val t = w.table("tbl")
  w.read(t, name = "read_all")
  w.read(t, version = 1, name = "read_v1_before_widening")
  w.snapshotHistory(t)
}

// ---------------------------------------------------------------------------
// Nested, array, and map type widening
// ---------------------------------------------------------------------------

workload("tw_nested_field", "Nested field type widening (struct.count: int -> long)", "type_widening") { w =>
  w.sql("""CREATE TABLE tbl (data STRUCT<id: INT, count: INT>) USING delta
    TBLPROPERTIES ('delta.enableTypeWidening' = 'true', 'delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (named_struct('id', 1, 'count', 100))")
  w.sql("INSERT INTO tbl VALUES (named_struct('id', 2, 'count', 2000000000))")
  w.sql("ALTER TABLE tbl ALTER COLUMN data.count TYPE LONG")
  w.sql("INSERT INTO tbl VALUES (named_struct('id', 3, 'count', 3000000000L))")
  val t = w.table("tbl")
  w.read(t, name = "read_all")
  w.read(t, predicate = "data.count > 2147483647", name = "read_large_count")
  w.snapshotHistory(t)
}

workload("tw_array_element", "Array element type widening (array<int> -> array<long>)", "type_widening") { w =>
  w.sql("""CREATE TABLE tbl (values ARRAY<INT>) USING delta
    TBLPROPERTIES ('delta.enableTypeWidening' = 'true', 'delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (array(1, 2, 3))")
  w.sql("INSERT INTO tbl VALUES (array(100, 200, 2147483647))")
  w.sql("ALTER TABLE tbl ALTER COLUMN values.element TYPE LONG")
  w.sql("INSERT INTO tbl VALUES (array(3000000000L, 9000000000000L))")
  val t = w.table("tbl")
  w.read(t, name = "read_all")
  w.snapshotHistory(t)
}

workload("tw_map_key_value_widening", "Map key/value type widening (map<byte,short> -> map<int,int>)", "type_widening") { w =>
  w.sql("""CREATE TABLE tbl (
    s STRUCT<a: BYTE>,
    m MAP<BYTE, SHORT>,
    a ARRAY<BYTE>
  ) USING delta
    TBLPROPERTIES ('delta.enableTypeWidening' = 'true', 'delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (named_struct('a', CAST(1 AS BYTE)), map(CAST(1 AS BYTE), CAST(10 AS SHORT)), array(CAST(1 AS BYTE), CAST(2 AS BYTE)))")
  // Widen map key: byte -> int
  w.sql("ALTER TABLE tbl ALTER COLUMN m.key TYPE INT")
  // Widen map value: short -> int
  w.sql("ALTER TABLE tbl ALTER COLUMN m.value TYPE INT")
  // Widen array element: byte -> int
  w.sql("ALTER TABLE tbl ALTER COLUMN a.element TYPE INT")
  // Widen struct field: byte -> int
  w.sql("ALTER TABLE tbl ALTER COLUMN s.a TYPE INT")
  w.sql("INSERT INTO tbl VALUES (named_struct('a', 50000), map(50000, 100000), array(50000, 60000))")
  val t = w.table("tbl")
  w.read(t, name = "read_all")
  w.read(t, version = 1, name = "read_v1_before_widening")
  w.snapshotHistory(t)
}

// ---------------------------------------------------------------------------
// Type widening with other features
// ---------------------------------------------------------------------------

workload("tw_with_dv", "Type widening with deletion vectors", "type_widening", "dv") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value SHORT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true', 'delta.enableTypeWidening' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1, CAST(10 AS SHORT)), (2, CAST(20 AS SHORT)), (3, CAST(30 AS SHORT)), (4, CAST(40 AS SHORT)), (5, CAST(50 AS SHORT))")
  w.sql("ALTER TABLE tbl ALTER COLUMN value TYPE INT")
  w.sql("INSERT INTO tbl VALUES (6, 40000), (7, 50000)")
  w.sql("DELETE FROM tbl WHERE id IN (2, 4)")
  val t = w.table("tbl")
  w.read(t, name = "read_all")
  w.read(t, predicate = "value > 32767", name = "read_wide_values")
  w.snapshot(t)
}

workload("tw_with_partition", "Type widening in partitioned table", "type_widening", "partition") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value SHORT, category STRING) USING delta
    PARTITIONED BY (category) TBLPROPERTIES ('delta.enableTypeWidening' = 'true', 'delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1, CAST(10 AS SHORT), 'A'), (2, CAST(20 AS SHORT), 'B'), (3, CAST(30 AS SHORT), 'A')")
  w.sql("ALTER TABLE tbl ALTER COLUMN value TYPE INT")
  w.sql("INSERT INTO tbl VALUES (4, 40000, 'A'), (5, 50000, 'B')")
  val t = w.table("tbl")
  w.read(t, name = "read_all")
  w.read(t, predicate = "category = 'A'", name = "read_partition_A")
  w.read(t, predicate = "value > 32767", name = "read_wide_values")
  w.snapshot(t)
}

workload("tw_with_column_mapping", "Column mapping + type widening combined", "type_widening", "column_mapping") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value INT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true', 'delta.enableTypeWidening' = 'true',
      'delta.columnMapping.mode' = 'name')""")
  w.sql("INSERT INTO tbl VALUES (1, 100), (2, 2000000000)")
  // Rename column first
  w.sql("ALTER TABLE tbl RENAME COLUMN value TO score")
  // Then widen
  w.sql("ALTER TABLE tbl ALTER COLUMN score TYPE LONG")
  w.sql("INSERT INTO tbl VALUES (3, 3000000000L), (4, 9000000000000L)")
  val t = w.table("tbl")
  w.read(t, name = "read_all")
  w.read(t, predicate = "score > 2147483647", name = "filter_on_widened")
  w.read(t, columns = Seq("id", "score"), name = "project_renamed_widened")
  w.snapshotHistory(t)
}

workload("tw_colmap_rename", "Type change + column rename combined", "type_widening", "column_mapping") { w =>
  w.sql("""CREATE TABLE tbl (id INT, val SHORT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true', 'delta.enableTypeWidening' = 'true',
      'delta.columnMapping.mode' = 'name')""")
  w.sql("INSERT INTO tbl VALUES (1, CAST(10 AS SHORT)), (2, CAST(32767 AS SHORT))")
  // Widen first, then rename
  w.sql("ALTER TABLE tbl ALTER COLUMN val TYPE INT")
  w.sql("ALTER TABLE tbl RENAME COLUMN val TO amount")
  w.sql("INSERT INTO tbl VALUES (3, 40000), (4, 100000)")
  val t = w.table("tbl")
  w.read(t, name = "read_all")
  w.read(t, predicate = "amount > 32767", name = "filter_wide")
  w.read(t, columns = Seq("id", "amount"), name = "project_renamed")
  w.snapshotHistory(t)
}

workload("tw_cdf_across_widening", "CDF spanning type widening change", "type_widening", "cdf") { w =>
  w.sql("""CREATE TABLE tbl (id INT, amount SHORT) USING delta
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'delta.enableTypeWidening' = 'true',
      'delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1, CAST(100 AS SHORT)), (2, CAST(200 AS SHORT))")
  w.sql("ALTER TABLE tbl ALTER COLUMN amount TYPE INT")
  w.sql("INSERT INTO tbl VALUES (3, 40000), (4, 50000)")
  val t = w.table("tbl")
  w.read(t, name = "read_all")
  w.read(t, version = 1, name = "read_original")
  w.cdf(t, startVersion = 0, name = "cdf_all_versions")
  w.snapshot(t)
}

workload("tw_row_tracking_combo", "Type widening + row tracking combined", "type_widening") { w =>
  w.sql("""CREATE TABLE tbl (id INT, score SHORT) USING delta
    TBLPROPERTIES ('delta.enableTypeWidening' = 'true',
      'spark.databricks.delta.properties.defaults.enableRowTracking' = 'true',
      'delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1, CAST(10 AS SHORT)), (2, CAST(20 AS SHORT))")
  w.sql("ALTER TABLE tbl ALTER COLUMN score TYPE INT")
  w.sql("INSERT INTO tbl VALUES (3, 40000), (4, 50000)")
  val t = w.table("tbl")
  w.read(t, name = "read_all")
  w.read(t, predicate = "score > 32767", name = "read_wide_values")
  w.snapshotHistory(t)
}

// ---------------------------------------------------------------------------
// Data skipping and projections with type widening
// ---------------------------------------------------------------------------

workload("tw_with_data_skipping", "Widened type with predicate pushdown", "type_widening") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value INT) USING delta
    TBLPROPERTIES ('delta.enableTypeWidening' = 'true', 'delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1, 100), (2, 200)")
  w.sql("INSERT INTO tbl VALUES (3, 300), (4, 400)")
  w.sql("ALTER TABLE tbl ALTER COLUMN value TYPE LONG")
  w.sql("INSERT INTO tbl VALUES (5, 3000000000L), (6, 9000000000000L)")
  val t = w.table("tbl")
  w.read(t, name = "read_all")
  w.read(t, predicate = "value > 2147483647", name = "read_large_values_only")
  w.read(t, predicate = "value > 150 AND value < 350", name = "read_with_predicate_on_widened")
  w.snapshot(t)
}

workload("tw_stats_after_change", "Data skipping after type change", "type_widening") { w =>
  w.sql("""CREATE TABLE tbl (id INT, metric SHORT) USING delta
    TBLPROPERTIES ('delta.enableTypeWidening' = 'true', 'delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1, CAST(10 AS SHORT)), (2, CAST(100 AS SHORT))")
  w.sql("ALTER TABLE tbl ALTER COLUMN metric TYPE INT")
  w.sql("INSERT INTO tbl VALUES (3, 40000), (4, 50000)")
  val t = w.table("tbl")
  w.read(t, name = "read_all")
  w.read(t, predicate = "metric <= 100", name = "predicate_old_range")
  w.read(t, predicate = "metric > 32767", name = "predicate_new_range")
  w.read(t, predicate = "metric >= 100 AND metric <= 40000", name = "predicate_cross_range")
  w.snapshotHistory(t)
}

workload("tw_project_widened", "Project only the widened column", "type_widening") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value INT, label STRING) USING delta
    TBLPROPERTIES ('delta.enableTypeWidening' = 'true', 'delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1, 100, 'a'), (2, 200, 'b')")
  w.sql("ALTER TABLE tbl ALTER COLUMN value TYPE LONG")
  w.sql("INSERT INTO tbl VALUES (3, 3000000000L, 'c'), (4, 4000000000L, 'd')")
  val t = w.table("tbl")
  w.read(t, name = "read_all")
  w.read(t, columns = Seq("value"), name = "project_widened_only")
  w.read(t, columns = Seq("id", "value"), name = "project_widened_with_id")
  w.snapshotHistory(t)
}

workload("tw_project_non_widened", "Project excluding widened column", "type_widening") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value INT, label STRING) USING delta
    TBLPROPERTIES ('delta.enableTypeWidening' = 'true', 'delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1, 100, 'a'), (2, 200, 'b')")
  w.sql("ALTER TABLE tbl ALTER COLUMN value TYPE LONG")
  w.sql("INSERT INTO tbl VALUES (3, 3000000000L, 'c'), (4, 4000000000L, 'd')")
  val t = w.table("tbl")
  w.read(t, name = "read_all")
  w.read(t, columns = Seq("id", "label"), name = "project_non_widened")
  w.snapshotHistory(t)
}

// ---------------------------------------------------------------------------
// Null handling
// ---------------------------------------------------------------------------

workload("tw_null_handling", "Nulls preserved across type widening (short -> int)", "type_widening") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value SHORT) USING delta
    TBLPROPERTIES ('delta.enableTypeWidening' = 'true', 'delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1, CAST(10 AS SHORT)), (2, CAST(NULL AS SHORT)), (3, CAST(30 AS SHORT)), (4, CAST(NULL AS SHORT))")
  w.sql("ALTER TABLE tbl ALTER COLUMN value TYPE INT")
  w.sql("INSERT INTO tbl VALUES (5, 40000), (6, CAST(NULL AS INT))")
  val t = w.table("tbl")
  w.read(t, name = "read_all")
  w.read(t, predicate = "value IS NOT NULL", name = "read_non_null")
  w.read(t, predicate = "value IS NULL", name = "read_nulls_only")
  w.snapshot(t)
}

generateAll(
  sys.env.getOrElse("WORKLOAD_OUTPUT_DIR", "/tmp/workloads"),
  force = sys.env.getOrElse("WORKLOAD_FORCE", "false").toBoolean)
System.exit(0)
