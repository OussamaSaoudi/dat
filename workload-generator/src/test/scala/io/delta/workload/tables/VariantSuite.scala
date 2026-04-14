/*
 * Copyright (2025) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.delta.workload.tables

import io.delta.workload.WorkloadTestSuite

/**
 * VARIANT type workloads: basic reads, data skipping, nested JSON, array/map variants,
 * column mapping, schema evolution, time travel, and edge cases.
 */
class VariantSuite extends WorkloadTestSuite("variant") {

  // var_001-006: Basic variant reads and stats

  test("var_001_basic") {
    sql("""CREATE TABLE tbl (id INT, data VARIANT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("""INSERT INTO tbl VALUES
      (1, PARSE_JSON('{"name":"alice","age":30}')),
      (2, PARSE_JSON('{"name":"bob","age":25}')),
      (3, PARSE_JSON('{"name":"charlie","age":35}'))""")
    val t = registerTable("tbl")
    read(t, name = "read_all")
    read(t, columns = Seq("data"), name = "select_variant_col")
    snapshot(t)
  }

  test("var_002_basic_stats") {
    sql("""CREATE TABLE tbl (v VARIANT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("""INSERT INTO tbl VALUES
      (PARSE_JSON('{"a":1}')),
      (PARSE_JSON('{"a":2}')),
      (PARSE_JSON('{"a":3}'))""")
    val t = registerTable("tbl")
    read(t, name = "read_all")
    snapshot(t)
  }

  test("var_003_nested_stats") {
    sql("""CREATE TABLE tbl (v VARIANT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("""INSERT INTO tbl VALUES
      (PARSE_JSON('{"outer":{"inner":1}}')),
      (PARSE_JSON('{"outer":{"inner":2}}')),
      (PARSE_JSON('{"outer":{"inner":3}}'))""")
    val t = registerTable("tbl")
    read(t, name = "read_all")
    snapshot(t)
  }

  test("var_004_non_objects") {
    sql("""CREATE TABLE tbl (id INT, v VARIANT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("""INSERT INTO tbl VALUES
      (1, PARSE_JSON('42')),
      (2, PARSE_JSON('"hello"')),
      (3, PARSE_JSON('true')),
      (4, PARSE_JSON('[1,2,3]')),
      (5, PARSE_JSON('null'))""")
    val t = registerTable("tbl")
    read(t, name = "read_all")
    read(t, predicate = "id <= 3", name = "filter_first_three")
    snapshot(t)
  }

  test("var_005_null_counts") {
    sql("""CREATE TABLE tbl (id INT, v VARIANT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("""INSERT INTO tbl VALUES
      (1, PARSE_JSON('{"a":1}')),
      (2, CAST(NULL AS VARIANT)),
      (3, PARSE_JSON('{"a":3}')),
      (4, CAST(NULL AS VARIANT)),
      (5, PARSE_JSON('{"a":5}'))""")
    val t = registerTable("tbl")
    read(t, name = "read_all")
    read(t, predicate = "v IS NOT NULL", name = "filter_non_null")
    snapshot(t)
  }

  test("var_006_different_types") {
    sql("""CREATE TABLE tbl (id INT, v VARIANT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("""INSERT INTO tbl VALUES
      (1, PARSE_JSON('{"int_val":42}')),
      (2, PARSE_JSON('{"str_val":"hello"}')),
      (3, PARSE_JSON('{"bool_val":true}')),
      (4, PARSE_JSON('{"float_val":3.14}'))""")
    val t = registerTable("tbl")
    read(t, name = "read_all")
    read(t, predicate = "id <= 2", name = "filter_by_id")
    snapshot(t)
  }

  // var_007: Partitioned variant table

  test("var_007_partitions") {
    sql("""CREATE TABLE tbl (part INT, v VARIANT) USING delta
      PARTITIONED BY (part) TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("""INSERT INTO tbl VALUES
      (1, PARSE_JSON('{"x":10}')),
      (1, PARSE_JSON('{"x":20}')),
      (2, PARSE_JSON('{"x":30}')),
      (2, PARSE_JSON('{"x":40}'))""")
    val t = registerTable("tbl")
    read(t, name = "read_all")
    read(t, predicate = "part = 1", name = "filter_partition")
    snapshot(t)
  }

  // var_008-013: Various variant patterns

  test("var_008_many_fields") {
    sql("""CREATE TABLE tbl (id INT, v VARIANT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("""INSERT INTO tbl VALUES
      (1, PARSE_JSON('{"f1":1,"f2":2,"f3":3,"f4":4,"f5":5,"f6":6,"f7":7,"f8":8,"f9":9,"f10":10,"f11":11}')),
      (2, PARSE_JSON('{"f1":20,"f2":21,"f3":22,"f4":23,"f5":24,"f6":25,"f7":26,"f8":27,"f9":28,"f10":29,"f11":30}'))""")
    val t = registerTable("tbl")
    read(t, name = "read_all")
    snapshot(t)
  }

  test("var_009_unusual_chars") {
    sql("""CREATE TABLE tbl (id INT, v VARIANT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("""INSERT INTO tbl VALUES
      (1, PARSE_JSON('{"field with spaces":1,"field.with.dots":2,"field/slash":3}')),
      (2, PARSE_JSON('{"field with spaces":10,"field.with.dots":20,"field/slash":30}'))""")
    val t = registerTable("tbl")
    read(t, name = "read_all")
    snapshot(t)
  }

  test("var_010_nested_fields") {
    sql("""CREATE TABLE tbl (id INT, v VARIANT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("""INSERT INTO tbl VALUES
      (1, PARSE_JSON('{"a":{"b":{"c":{"d":1}}}}')),
      (2, PARSE_JSON('{"a":{"b":{"c":{"d":2}}}}'))""")
    val t = registerTable("tbl")
    read(t, name = "read_all")
    snapshot(t)
  }

  test("var_011_missing_values") {
    sql("""CREATE TABLE tbl (id INT, v VARIANT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("""INSERT INTO tbl VALUES
      (1, PARSE_JSON('{"a":1,"b":2}')),
      (2, PARSE_JSON('{"a":3}')),
      (3, PARSE_JSON('{"b":4}'))""")
    val t = registerTable("tbl")
    read(t, name = "read_all")
    snapshot(t)
  }

  test("var_012_mixed_types") {
    sql("""CREATE TABLE tbl (id INT, v VARIANT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("""INSERT INTO tbl VALUES
      (1, PARSE_JSON('{"x":1}')),
      (2, PARSE_JSON('{"x":"hello"}')),
      (3, PARSE_JSON('{"x":true}')),
      (4, PARSE_JSON('{"x":[1,2]}')),
      (5, PARSE_JSON('{"x":null}')),
      (6, PARSE_JSON('{"x":3.14}'))""")
    val t = registerTable("tbl")
    read(t, name = "read_all")
    read(t, predicate = "id <= 3", name = "filter_half")
    snapshot(t)
  }

  test("var_013_extreme_values") {
    sql("""CREATE TABLE tbl (id INT, v VARIANT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("""INSERT INTO tbl VALUES
      (1, PARSE_JSON('{"big":9999999999999999}')),
      (2, PARSE_JSON('{"tiny":0.000000001}')),
      (3, PARSE_JSON('{"neg":-9999999999999999}')),
      (4, PARSE_JSON('{"empty_str":""}'))""")
    val t = registerTable("tbl")
    read(t, name = "read_all")
    snapshot(t)
  }

  // var_014-015: Variant in struct and string skipping

  test("var_014_variant_in_struct") {
    sql("""CREATE TABLE tbl (id INT, wrapper STRUCT<data: VARIANT, label: STRING>) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("""INSERT INTO tbl VALUES
      (1, named_struct('data', PARSE_JSON('{"v":1}'), 'label', 'first')),
      (2, named_struct('data', PARSE_JSON('{"v":2}'), 'label', 'second'))""")
    val t = registerTable("tbl")
    read(t, name = "read_all")
    read(t, predicate = "wrapper.label = 'first'", name = "filter_label")
    snapshot(t)
  }

  test("var_015_string_skipping") {
    sql("""CREATE TABLE tbl (id INT, v VARIANT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, PARSE_JSON('{\"name\":\"alpha\"}'))")
    sql("INSERT INTO tbl VALUES (2, PARSE_JSON('{\"name\":\"beta\"}'))")
    sql("INSERT INTO tbl VALUES (3, PARSE_JSON('{\"name\":\"gamma\"}'))")
    val t = registerTable("tbl")
    read(t, name = "read_all")
    read(t, predicate = "id = 2", name = "filter_middle")
    snapshot(t)
  }

  // var_016-017: Array and map variant

  test("var_016_array_variant") {
    sql("""CREATE TABLE tbl (id INT, items ARRAY<VARIANT>) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("""INSERT INTO tbl VALUES
      (1, array(PARSE_JSON('{"item":"a"}'), PARSE_JSON('{"item":"b"}')))""")
    sql("""INSERT INTO tbl VALUES
      (2, array(PARSE_JSON('{"item":"c"}'), PARSE_JSON('{"item":"d"}'), PARSE_JSON('{"item":"e"}')))""")
    val t = registerTable("tbl")
    read(t, name = "read_all")
    read(t, predicate = "size(items) > 2", name = "filter_array_size")
    snapshot(t)
  }

  test("var_017_map_variant") {
    sql("""CREATE TABLE tbl (id INT, attributes MAP<STRING, VARIANT>) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("""INSERT INTO tbl VALUES
      (1, map('color', PARSE_JSON('"red"'), 'size', PARSE_JSON('10')))""")
    sql("""INSERT INTO tbl VALUES
      (2, map('color', PARSE_JSON('"blue"'), 'weight', PARSE_JSON('5.5')))""")
    val t = registerTable("tbl")
    read(t, name = "read_all")
    read(t, predicate = "id = 1", name = "filter_by_id")
    snapshot(t)
  }

  // var_018: Column mapping + variant

  test("var_018_column_mapping") {
    sql("""CREATE TABLE tbl (id INT, json_col VARIANT) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'delta.enableDeletionVectors' = 'true')""")
    sql("""INSERT INTO tbl VALUES
      (1, PARSE_JSON('{"key":"value1"}'))""")
    sql("""INSERT INTO tbl VALUES
      (2, PARSE_JSON('{"key":"value2"}'))""")
    val t = registerTable("tbl")
    read(t, name = "read_all")
    read(t, predicate = "id = 1", name = "filter_by_id")
    snapshot(t)
  }

  // var_019: Schema evolution with variant

  test("var_019_schema_evolution") {
    sql("""CREATE TABLE tbl (v VARIANT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (PARSE_JSON('{\"a\":1}'))")
    sql("INSERT INTO tbl VALUES (PARSE_JSON('{\"a\":2}'))")
    sql("ALTER TABLE tbl ADD COLUMN (s STRING)")
    sql("INSERT INTO tbl VALUES (PARSE_JSON('{\"a\":3}'), 'after_evolution')")
    sql("INSERT INTO tbl VALUES (PARSE_JSON('{\"a\":4}'), 'second_after')")
    val t = registerTable("tbl")
    read(t, name = "read_all")
    read(t, version = 2, name = "read_v2_before_evolution")
    read(t, predicate = "s IS NOT NULL", name = "filter_new_column")
    val N = 5L
    for (v <- 0L to N) snapshot(t, version = v)
  }

  // var_020: Time travel with variant

  test("var_020_time_travel") {
    sql("""CREATE TABLE tbl (id INT, payload VARIANT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, PARSE_JSON('{\"v\":\"first\"}'))")
    sql("INSERT INTO tbl VALUES (2, PARSE_JSON('{\"v\":\"second\"}'))")
    sql("INSERT INTO tbl VALUES (3, PARSE_JSON('{\"v\":\"third\"}'))")
    val t = registerTable("tbl")
    read(t, name = "read_latest")
    read(t, version = 1, name = "read_v1")
    read(t, version = 2, name = "read_v2")
    val N = 3L
    for (v <- 0L to N) snapshot(t, version = v)
  }

  // var_021: Variant after OPTIMIZE

  test("var_021_optimized") {
    sql("""CREATE TABLE tbl (id INT, data VARIANT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, PARSE_JSON('{\"x\":1}'))")
    sql("INSERT INTO tbl VALUES (2, PARSE_JSON('{\"x\":2}'))")
    sql("INSERT INTO tbl VALUES (3, PARSE_JSON('{\"x\":3}'))")
    sql("INSERT INTO tbl VALUES (4, PARSE_JSON('{\"x\":4}'))")
    sql("INSERT INTO tbl VALUES (5, PARSE_JSON('{\"x\":5}'))")
    sql("OPTIMIZE tbl")
    val t = registerTable("tbl")
    read(t, name = "read_all")
    read(t, predicate = "id > 3", name = "filter_after_optimize")
    snapshot(t)
  }

  // var_022: Variant stat fields property

  test("var_022_stat_fields") {
    sql("""CREATE TABLE tbl (id INT, v VARIANT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("""INSERT INTO tbl VALUES
      (1, PARSE_JSON('{"a":1,"b":"x"}')),
      (2, PARSE_JSON('{"a":2,"b":"y"}')),
      (3, PARSE_JSON('{"a":3,"b":"z"}'))""")
    val t = registerTable("tbl")
    read(t, name = "read_all")
    read(t, predicate = "id >= 2", name = "filter_by_id")
    snapshot(t)
  }

  // var_all_json_types - var_unicode_escapes: Additional variant patterns

  test("var_all_json_types") {
    sql("""CREATE TABLE tbl (id INT, data VARIANT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("""INSERT INTO tbl VALUES
      (1, PARSE_JSON('{"str":"hello","num":42,"float":3.14,"bool":true,"null_val":null,"arr":[1,2],"obj":{"nested":"yes"}}'))""")
    sql("""INSERT INTO tbl VALUES
      (2, PARSE_JSON('{"str":"world","num":-1,"float":0.0,"bool":false,"null_val":null,"arr":[],"obj":{}}'))""")
    val t = registerTable("tbl")
    read(t, name = "read_all")
    snapshot(t)
  }

  test("var_change_tracking_read") {
    sql("""CREATE TABLE tbl (id INT, data VARIANT) USING delta
      TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'delta.enableDeletionVectors' = 'true')""")
    sql("""INSERT INTO tbl VALUES
      (1, PARSE_JSON('{"v":"original"}')),
      (2, PARSE_JSON('{"v":"original"}'))""")
    sql("UPDATE tbl SET data = PARSE_JSON('{\"v\":\"updated\"}') WHERE id = 1")
    val t = registerTable("tbl")
    read(t, name = "read_all")
    snapshot(t)
  }

  test("var_deeply_nested") {
    sql("""CREATE TABLE tbl (id INT, data VARIANT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("""INSERT INTO tbl VALUES
      (1, PARSE_JSON('{"l1":{"l2":{"l3":{"l4":{"l5":{"l6":"deep"}}}}}}')),
      (2, PARSE_JSON('{"l1":{"l2":{"l3":{"l4":{"l5":{"l6":"also_deep"}}}}}}'))""")
    val t = registerTable("tbl")
    read(t, name = "read_all")
    snapshot(t)
  }

  test("var_large_array") {
    sql("""CREATE TABLE tbl (id INT, data VARIANT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    // Build a JSON array with 100 elements
    val arr = (0 until 100).mkString("[", ",", "]")
    sql(s"""INSERT INTO tbl VALUES
      (1, PARSE_JSON('$arr')),
      (2, PARSE_JSON('$arr'))""")
    val t = registerTable("tbl")
    read(t, name = "read_all")
    snapshot(t)
  }

  test("var_null_top_level") {
    sql("""CREATE TABLE tbl (id INT, data VARIANT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("""INSERT INTO tbl VALUES
      (1, PARSE_JSON('{"a":1}')),
      (2, CAST(NULL AS VARIANT)),
      (3, PARSE_JSON('{"a":3}')),
      (4, CAST(NULL AS VARIANT))""")
    val t = registerTable("tbl")
    read(t, name = "read_all")
    read(t, predicate = "data IS NULL", name = "filter_null")
    read(t, predicate = "data IS NOT NULL", name = "filter_not_null")
    snapshot(t)
  }

  test("var_numeric_precision") {
    sql("""CREATE TABLE tbl (id INT, data VARIANT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("""INSERT INTO tbl VALUES
      (1, PARSE_JSON('{"val":0.1}')),
      (2, PARSE_JSON('{"val":0.2}')),
      (3, PARSE_JSON('{"val":0.30000000000000004}')),
      (4, PARSE_JSON('{"val":9007199254740992}')),
      (5, PARSE_JSON('{"val":9007199254740993}'))""")
    val t = registerTable("tbl")
    read(t, name = "read_all")
    snapshot(t)
  }

  test("var_predicate_non_variant") {
    sql("""CREATE TABLE tbl (id INT, category STRING, data VARIANT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("""INSERT INTO tbl VALUES
      (1, 'A', PARSE_JSON('{"x":1}')),
      (2, 'B', PARSE_JSON('{"x":2}')),
      (3, 'A', PARSE_JSON('{"x":3}')),
      (4, 'B', PARSE_JSON('{"x":4}'))""")
    val t = registerTable("tbl")
    read(t, name = "read_all")
    read(t, predicate = "category = 'A'", name = "filter_category_A")
    snapshot(t)
  }

  test("var_projection") {
    sql("""CREATE TABLE tbl (id INT, name STRING, data VARIANT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("""INSERT INTO tbl VALUES
      (1, 'alice', PARSE_JSON('{"score":90}')),
      (2, 'bob', PARSE_JSON('{"score":85}')),
      (3, 'charlie', PARSE_JSON('{"score":95}'))""")
    val t = registerTable("tbl")
    read(t, name = "read_all")
    read(t, columns = Seq("id", "data"), name = "project_id_data")
    snapshot(t)
  }

  test("var_unicode_escapes") {
    sql("""CREATE TABLE tbl (id INT, data VARIANT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("""INSERT INTO tbl VALUES
      (1, PARSE_JSON('{"emoji":"\u2764","tab":"a\\tb","newline":"a\\nb"}')),
      (2, PARSE_JSON('{"unicode":"\u00e9\u00e0\u00fc","backslash":"a\\\\b"}'))""")
    val t = registerTable("tbl")
    read(t, name = "read_all")
    snapshot(t)
  }

}
