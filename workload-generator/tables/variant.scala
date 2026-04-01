/**
 * VARIANT type workloads: basic reads, data skipping, nested JSON, array/map variants,
 * column mapping, schema evolution, time travel, CDF, and edge cases.
 */
import io.delta.workload.WorkloadGenerator._

// ---------------------------------------------------------------------------
// var_001-006: Basic variant reads and stats
// ---------------------------------------------------------------------------

workload("var_001_basic", "Read table with VARIANT column", "variant") { w =>
  w.sql("""CREATE TABLE tbl (id INT, data VARIANT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("""INSERT INTO tbl VALUES
    (1, PARSE_JSON('{"name":"alice","age":30}')),
    (2, PARSE_JSON('{"name":"bob","age":25}')),
    (3, PARSE_JSON('{"name":"charlie","age":35}'))""")
  val t = w.table("tbl")
  w.read(t, name = "read_all")
  w.read(t, columns = Seq("data"), name = "select_variant_col")
  w.snapshot(t)
}

workload("var_002_basic_stats", "Basic variant stats", "variant") { w =>
  w.sql("""CREATE TABLE tbl (v VARIANT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("""INSERT INTO tbl VALUES
    (PARSE_JSON('{"a":1}')),
    (PARSE_JSON('{"a":2}')),
    (PARSE_JSON('{"a":3}'))""")
  val t = w.table("tbl")
  w.read(t, name = "read_all")
  w.snapshot(t)
}

workload("var_003_nested_stats", "Nested variant stats", "variant") { w =>
  w.sql("""CREATE TABLE tbl (v VARIANT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("""INSERT INTO tbl VALUES
    (PARSE_JSON('{"outer":{"inner":1}}')),
    (PARSE_JSON('{"outer":{"inner":2}}')),
    (PARSE_JSON('{"outer":{"inner":3}}'))""")
  val t = w.table("tbl")
  w.read(t, name = "read_all")
  w.snapshot(t)
}

workload("var_004_non_objects", "Non-objects in variant", "variant") { w =>
  w.sql("""CREATE TABLE tbl (id INT, v VARIANT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("""INSERT INTO tbl VALUES
    (1, PARSE_JSON('42')),
    (2, PARSE_JSON('"hello"')),
    (3, PARSE_JSON('true')),
    (4, PARSE_JSON('[1,2,3]')),
    (5, PARSE_JSON('null'))""")
  val t = w.table("tbl")
  w.read(t, name = "read_all")
  w.read(t, predicate = "id <= 3", name = "filter_first_three")
  w.snapshot(t)
}

workload("var_005_null_counts", "Null counts in variant", "variant") { w =>
  w.sql("""CREATE TABLE tbl (id INT, v VARIANT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("""INSERT INTO tbl VALUES
    (1, PARSE_JSON('{"a":1}')),
    (2, CAST(NULL AS VARIANT)),
    (3, PARSE_JSON('{"a":3}')),
    (4, CAST(NULL AS VARIANT)),
    (5, PARSE_JSON('{"a":5}'))""")
  val t = w.table("tbl")
  w.read(t, name = "read_all")
  w.read(t, predicate = "v IS NOT NULL", name = "filter_non_null")
  w.snapshot(t)
}

workload("var_006_different_types", "Variant stats with different data types", "variant") { w =>
  w.sql("""CREATE TABLE tbl (id INT, v VARIANT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("""INSERT INTO tbl VALUES
    (1, PARSE_JSON('{"int_val":42}')),
    (2, PARSE_JSON('{"str_val":"hello"}')),
    (3, PARSE_JSON('{"bool_val":true}')),
    (4, PARSE_JSON('{"float_val":3.14}'))""")
  val t = w.table("tbl")
  w.read(t, name = "read_all")
  w.read(t, predicate = "id <= 2", name = "filter_by_id")
  w.snapshot(t)
}

// ---------------------------------------------------------------------------
// var_007: Partitioned variant table
// ---------------------------------------------------------------------------

workload("var_007_partitions", "Variant stats with multiple partitions", "variant", "partition") { w =>
  w.sql("""CREATE TABLE tbl (part INT, v VARIANT) USING delta
    PARTITIONED BY (part) TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("""INSERT INTO tbl VALUES
    (1, PARSE_JSON('{"x":10}')),
    (1, PARSE_JSON('{"x":20}')),
    (2, PARSE_JSON('{"x":30}')),
    (2, PARSE_JSON('{"x":40}'))""")
  val t = w.table("tbl")
  w.read(t, name = "read_all")
  w.read(t, predicate = "part = 1", name = "filter_partition")
  w.snapshot(t)
}

// ---------------------------------------------------------------------------
// var_008-013: Various variant patterns
// ---------------------------------------------------------------------------

workload("var_008_many_fields", "More than 10 fields in variant", "variant") { w =>
  w.sql("""CREATE TABLE tbl (id INT, v VARIANT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("""INSERT INTO tbl VALUES
    (1, PARSE_JSON('{"f1":1,"f2":2,"f3":3,"f4":4,"f5":5,"f6":6,"f7":7,"f8":8,"f9":9,"f10":10,"f11":11}')),
    (2, PARSE_JSON('{"f1":20,"f2":21,"f3":22,"f4":23,"f5":24,"f6":25,"f7":26,"f8":27,"f9":28,"f10":29,"f11":30}'))""")
  val t = w.table("tbl")
  w.read(t, name = "read_all")
  w.snapshot(t)
}

workload("var_009_unusual_chars", "Unusual characters in field names", "variant") { w =>
  w.sql("""CREATE TABLE tbl (id INT, v VARIANT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("""INSERT INTO tbl VALUES
    (1, PARSE_JSON('{"field with spaces":1,"field.with.dots":2,"field/slash":3}')),
    (2, PARSE_JSON('{"field with spaces":10,"field.with.dots":20,"field/slash":30}'))""")
  val t = w.table("tbl")
  w.read(t, name = "read_all")
  w.snapshot(t)
}

workload("var_010_nested_fields", "Deeply nested fields in variant", "variant") { w =>
  w.sql("""CREATE TABLE tbl (id INT, v VARIANT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("""INSERT INTO tbl VALUES
    (1, PARSE_JSON('{"a":{"b":{"c":{"d":1}}}}')),
    (2, PARSE_JSON('{"a":{"b":{"c":{"d":2}}}}'))""")
  val t = w.table("tbl")
  w.read(t, name = "read_all")
  w.snapshot(t)
}

workload("var_011_missing_values", "Missing values in variant", "variant") { w =>
  w.sql("""CREATE TABLE tbl (id INT, v VARIANT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("""INSERT INTO tbl VALUES
    (1, PARSE_JSON('{"a":1,"b":2}')),
    (2, PARSE_JSON('{"a":3}')),
    (3, PARSE_JSON('{"b":4}'))""")
  val t = w.table("tbl")
  w.read(t, name = "read_all")
  w.snapshot(t)
}

workload("var_012_mixed_types", "Mixed types for same field", "variant") { w =>
  w.sql("""CREATE TABLE tbl (id INT, v VARIANT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("""INSERT INTO tbl VALUES
    (1, PARSE_JSON('{"x":1}')),
    (2, PARSE_JSON('{"x":"hello"}')),
    (3, PARSE_JSON('{"x":true}')),
    (4, PARSE_JSON('{"x":[1,2]}')),
    (5, PARSE_JSON('{"x":null}')),
    (6, PARSE_JSON('{"x":3.14}'))""")
  val t = w.table("tbl")
  w.read(t, name = "read_all")
  w.read(t, predicate = "id <= 3", name = "filter_half")
  w.snapshot(t)
}

workload("var_013_extreme_values", "Extreme values in variant", "variant") { w =>
  w.sql("""CREATE TABLE tbl (id INT, v VARIANT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("""INSERT INTO tbl VALUES
    (1, PARSE_JSON('{"big":9999999999999999}')),
    (2, PARSE_JSON('{"tiny":0.000000001}')),
    (3, PARSE_JSON('{"neg":-9999999999999999}')),
    (4, PARSE_JSON('{"empty_str":""}'))""")
  val t = w.table("tbl")
  w.read(t, name = "read_all")
  w.snapshot(t)
}

// ---------------------------------------------------------------------------
// var_014-015: Variant in struct and string skipping
// ---------------------------------------------------------------------------

workload("var_014_variant_in_struct", "Variant in struct for data skipping", "variant") { w =>
  w.sql("""CREATE TABLE tbl (id INT, wrapper STRUCT<data: VARIANT, label: STRING>) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("""INSERT INTO tbl VALUES
    (1, named_struct('data', PARSE_JSON('{"v":1}'), 'label', 'first')),
    (2, named_struct('data', PARSE_JSON('{"v":2}'), 'label', 'second'))""")
  val t = w.table("tbl")
  w.read(t, name = "read_all")
  w.read(t, predicate = "wrapper.label = 'first'", name = "filter_label")
  w.snapshot(t)
}

workload("var_015_string_skipping", "Data skipping with string values", "variant") { w =>
  w.sql("""CREATE TABLE tbl (id INT, v VARIANT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1, PARSE_JSON('{\"name\":\"alpha\"}'))")
  w.sql("INSERT INTO tbl VALUES (2, PARSE_JSON('{\"name\":\"beta\"}'))")
  w.sql("INSERT INTO tbl VALUES (3, PARSE_JSON('{\"name\":\"gamma\"}'))")
  val t = w.table("tbl")
  w.read(t, name = "read_all")
  w.read(t, predicate = "id = 2", name = "filter_middle")
  w.snapshot(t)
}

// ---------------------------------------------------------------------------
// var_016-017: Array and map variant
// ---------------------------------------------------------------------------

workload("var_016_array_variant", "Read ARRAY<VARIANT> column", "variant") { w =>
  w.sql("""CREATE TABLE tbl (id INT, items ARRAY<VARIANT>) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("""INSERT INTO tbl VALUES
    (1, array(PARSE_JSON('{"item":"a"}'), PARSE_JSON('{"item":"b"}')))""")
  w.sql("""INSERT INTO tbl VALUES
    (2, array(PARSE_JSON('{"item":"c"}'), PARSE_JSON('{"item":"d"}'), PARSE_JSON('{"item":"e"}')))""")
  val t = w.table("tbl")
  w.read(t, name = "read_all")
  w.read(t, predicate = "size(items) > 2", name = "filter_array_size")
  w.snapshot(t)
}

workload("var_017_map_variant", "Read MAP<STRING, VARIANT> column", "variant") { w =>
  w.sql("""CREATE TABLE tbl (id INT, attributes MAP<STRING, VARIANT>) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("""INSERT INTO tbl VALUES
    (1, map('color', PARSE_JSON('"red"'), 'size', PARSE_JSON('10')))""")
  w.sql("""INSERT INTO tbl VALUES
    (2, map('color', PARSE_JSON('"blue"'), 'weight', PARSE_JSON('5.5')))""")
  val t = w.table("tbl")
  w.read(t, name = "read_all")
  w.read(t, predicate = "id = 1", name = "filter_by_id")
  w.snapshot(t)
}

// ---------------------------------------------------------------------------
// var_018: Column mapping + variant
// ---------------------------------------------------------------------------

workload("var_018_column_mapping", "VARIANT with column mapping", "variant", "column_mapping") { w =>
  w.sql("""CREATE TABLE tbl (id INT, json_col VARIANT) USING delta
    TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'delta.enableDeletionVectors' = 'true')""")
  w.sql("""INSERT INTO tbl VALUES
    (1, PARSE_JSON('{"key":"value1"}'))""")
  w.sql("""INSERT INTO tbl VALUES
    (2, PARSE_JSON('{"key":"value2"}'))""")
  val t = w.table("tbl")
  w.read(t, name = "read_all")
  w.read(t, predicate = "id = 1", name = "filter_by_id")
  w.snapshot(t)
}

// ---------------------------------------------------------------------------
// var_019: Schema evolution with variant
// ---------------------------------------------------------------------------

workload("var_019_schema_evolution", "Schema evolution with VARIANT column", "variant", "schema_evolution") { w =>
  w.sql("""CREATE TABLE tbl (v VARIANT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (PARSE_JSON('{\"a\":1}'))")
  w.sql("INSERT INTO tbl VALUES (PARSE_JSON('{\"a\":2}'))")
  w.sql("ALTER TABLE tbl ADD COLUMN (s STRING)")
  w.sql("INSERT INTO tbl VALUES (PARSE_JSON('{\"a\":3}'), 'after_evolution')")
  w.sql("INSERT INTO tbl VALUES (PARSE_JSON('{\"a\":4}'), 'second_after')")
  val t = w.table("tbl")
  w.read(t, name = "read_all")
  w.read(t, version = 2, name = "read_v2_before_evolution")
  w.read(t, predicate = "s IS NOT NULL", name = "filter_new_column")
  w.snapshotHistory(t)
}

// ---------------------------------------------------------------------------
// var_020: Time travel with variant
// ---------------------------------------------------------------------------

workload("var_020_time_travel", "Time travel with VARIANT column", "variant") { w =>
  w.sql("""CREATE TABLE tbl (id INT, payload VARIANT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1, PARSE_JSON('{\"v\":\"first\"}'))")
  w.sql("INSERT INTO tbl VALUES (2, PARSE_JSON('{\"v\":\"second\"}'))")
  w.sql("INSERT INTO tbl VALUES (3, PARSE_JSON('{\"v\":\"third\"}'))")
  val t = w.table("tbl")
  w.read(t, name = "read_latest")
  w.read(t, version = 1, name = "read_v1")
  w.read(t, version = 2, name = "read_v2")
  w.snapshotHistory(t)
}

// ---------------------------------------------------------------------------
// var_021: Variant after OPTIMIZE
// ---------------------------------------------------------------------------

workload("var_021_optimized", "Read VARIANT after OPTIMIZE", "variant") { w =>
  w.sql("""CREATE TABLE tbl (id INT, data VARIANT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1, PARSE_JSON('{\"x\":1}'))")
  w.sql("INSERT INTO tbl VALUES (2, PARSE_JSON('{\"x\":2}'))")
  w.sql("INSERT INTO tbl VALUES (3, PARSE_JSON('{\"x\":3}'))")
  w.sql("INSERT INTO tbl VALUES (4, PARSE_JSON('{\"x\":4}'))")
  w.sql("INSERT INTO tbl VALUES (5, PARSE_JSON('{\"x\":5}'))")
  w.sql("OPTIMIZE tbl")
  val t = w.table("tbl")
  w.read(t, name = "read_all")
  w.read(t, predicate = "id > 3", name = "filter_after_optimize")
  w.snapshot(t)
}

// ---------------------------------------------------------------------------
// var_022: Variant stat fields property
// ---------------------------------------------------------------------------

workload("var_022_stat_fields", "VARIANT_DATA_SKIPPING_STAT_FIELDS property", "variant") { w =>
  w.sql("""CREATE TABLE tbl (id INT, v VARIANT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("""INSERT INTO tbl VALUES
    (1, PARSE_JSON('{"a":1,"b":"x"}')),
    (2, PARSE_JSON('{"a":2,"b":"y"}')),
    (3, PARSE_JSON('{"a":3,"b":"z"}'))""")
  val t = w.table("tbl")
  w.read(t, name = "read_all")
  w.read(t, predicate = "id >= 2", name = "filter_by_id")
  w.snapshot(t)
}

// ---------------------------------------------------------------------------
// var_all_json_types - var_unicode_escapes: Additional variant patterns
// ---------------------------------------------------------------------------

workload("var_all_json_types", "Variant with all JSON types in one value", "variant") { w =>
  w.sql("""CREATE TABLE tbl (id INT, data VARIANT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("""INSERT INTO tbl VALUES
    (1, PARSE_JSON('{"str":"hello","num":42,"float":3.14,"bool":true,"null_val":null,"arr":[1,2],"obj":{"nested":"yes"}}'))""")
  w.sql("""INSERT INTO tbl VALUES
    (2, PARSE_JSON('{"str":"world","num":-1,"float":0.0,"bool":false,"null_val":null,"arr":[],"obj":{}}'))""")
  val t = w.table("tbl")
  w.read(t, name = "read_all")
  w.snapshot(t)
}

workload("var_cdf_read", "Variant with CDF enabled", "variant", "cdf") { w =>
  w.sql("""CREATE TABLE tbl (id INT, data VARIANT) USING delta
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'delta.enableDeletionVectors' = 'true')""")
  w.sql("""INSERT INTO tbl VALUES
    (1, PARSE_JSON('{"v":"original"}')),
    (2, PARSE_JSON('{"v":"original"}'))""")
  w.sql("UPDATE tbl SET data = PARSE_JSON('{\"v\":\"updated\"}') WHERE id = 1")
  val t = w.table("tbl")
  w.read(t, name = "read_all")
  w.cdf(t, startVersion = 0, name = "cdf_all")
  w.snapshot(t)
}

workload("var_deeply_nested", "Variant with deeply nested JSON", "variant") { w =>
  w.sql("""CREATE TABLE tbl (id INT, data VARIANT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("""INSERT INTO tbl VALUES
    (1, PARSE_JSON('{"l1":{"l2":{"l3":{"l4":{"l5":{"l6":"deep"}}}}}}')),
    (2, PARSE_JSON('{"l1":{"l2":{"l3":{"l4":{"l5":{"l6":"also_deep"}}}}}}'))""")
  val t = w.table("tbl")
  w.read(t, name = "read_all")
  w.snapshot(t)
}

workload("var_large_array", "Variant with large JSON array", "variant") { w =>
  w.sql("""CREATE TABLE tbl (id INT, data VARIANT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  // Build a JSON array with 100 elements
  val arr = (0 until 100).mkString("[", ",", "]")
  w.sql(s"""INSERT INTO tbl VALUES
    (1, PARSE_JSON('$arr')),
    (2, PARSE_JSON('$arr'))""")
  val t = w.table("tbl")
  w.read(t, name = "read_all")
  w.snapshot(t)
}

workload("var_null_top_level", "Variant with SQL NULL top-level value", "variant") { w =>
  w.sql("""CREATE TABLE tbl (id INT, data VARIANT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("""INSERT INTO tbl VALUES
    (1, PARSE_JSON('{"a":1}')),
    (2, CAST(NULL AS VARIANT)),
    (3, PARSE_JSON('{"a":3}')),
    (4, CAST(NULL AS VARIANT))""")
  val t = w.table("tbl")
  w.read(t, name = "read_all")
  w.read(t, predicate = "data IS NULL", name = "filter_null")
  w.read(t, predicate = "data IS NOT NULL", name = "filter_not_null")
  w.snapshot(t)
}

workload("var_numeric_precision", "Variant with numeric precision edge cases", "variant") { w =>
  w.sql("""CREATE TABLE tbl (id INT, data VARIANT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("""INSERT INTO tbl VALUES
    (1, PARSE_JSON('{"val":0.1}')),
    (2, PARSE_JSON('{"val":0.2}')),
    (3, PARSE_JSON('{"val":0.30000000000000004}')),
    (4, PARSE_JSON('{"val":9007199254740992}')),
    (5, PARSE_JSON('{"val":9007199254740993}'))""")
  val t = w.table("tbl")
  w.read(t, name = "read_all")
  w.snapshot(t)
}

workload("var_predicate_non_variant", "Predicate on non-variant column with variant present", "variant") { w =>
  w.sql("""CREATE TABLE tbl (id INT, category STRING, data VARIANT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("""INSERT INTO tbl VALUES
    (1, 'A', PARSE_JSON('{"x":1}')),
    (2, 'B', PARSE_JSON('{"x":2}')),
    (3, 'A', PARSE_JSON('{"x":3}')),
    (4, 'B', PARSE_JSON('{"x":4}'))""")
  val t = w.table("tbl")
  w.read(t, name = "read_all")
  w.read(t, predicate = "category = 'A'", name = "filter_category_A")
  w.snapshot(t)
}

workload("var_projection", "Column projection on variant table", "variant") { w =>
  w.sql("""CREATE TABLE tbl (id INT, name STRING, data VARIANT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("""INSERT INTO tbl VALUES
    (1, 'alice', PARSE_JSON('{"score":90}')),
    (2, 'bob', PARSE_JSON('{"score":85}')),
    (3, 'charlie', PARSE_JSON('{"score":95}'))""")
  val t = w.table("tbl")
  w.read(t, name = "read_all")
  w.read(t, columns = Seq("id", "data"), name = "project_id_data")
  w.snapshot(t)
}

workload("var_unicode_escapes", "Variant with unicode and escape sequences", "variant") { w =>
  w.sql("""CREATE TABLE tbl (id INT, data VARIANT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("""INSERT INTO tbl VALUES
    (1, PARSE_JSON('{"emoji":"\u2764","tab":"a\\tb","newline":"a\\nb"}')),
    (2, PARSE_JSON('{"unicode":"\u00e9\u00e0\u00fc","backslash":"a\\\\b"}'))""")
  val t = w.table("tbl")
  w.read(t, name = "read_all")
  w.snapshot(t)
}

generateAll(
  sys.env.getOrElse("WORKLOAD_OUTPUT_DIR", "/tmp/workloads"),
  force = sys.env.getOrElse("WORKLOAD_FORCE", "false").toBoolean)
System.exit(0)
