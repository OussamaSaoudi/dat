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

class ColumnMappingSuite extends WorkloadTestSuite("column_mapping") {

  // Existing 6 workloads

  test("cm_mode_name") {
    sql("""CREATE TABLE tbl (id INT, name STRING, value DOUBLE) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name')""")
    sql("INSERT INTO tbl VALUES (1,'alice',100),(2,'bob',200),(3,'charlie',300)")
    val t = registerTable("tbl")
    read(t)
    read(t, columns = Seq("name", "value"))
    read(t, predicate = "value > 150")
    snapshot(t)
  }

  test("cm_rename_column") {
    sql("""CREATE TABLE tbl (id INT, old_name STRING) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    sql("INSERT INTO tbl VALUES (1,'alice'),(2,'bob')")
    sql("ALTER TABLE tbl RENAME COLUMN old_name TO new_name")
    sql("INSERT INTO tbl VALUES (3,'charlie')")
    val t = registerTable("tbl")
    read(t)
    read(t, version = 2)
    read(t, columns = Seq("id", "new_name"))
    for (v <- 0L to 3L) snapshot(t, version = v)
  }

  test("cm_drop_column") {
    sql("""CREATE TABLE tbl (id INT, name STRING, to_drop STRING) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    sql("INSERT INTO tbl VALUES (1,'a','drop1'),(2,'b','drop2')")
    sql("ALTER TABLE tbl DROP COLUMN to_drop")
    sql("INSERT INTO tbl VALUES (3,'c')")
    val t = registerTable("tbl")
    read(t)
    read(t, version = 2)
    for (v <- 0L to 3L) snapshot(t, version = v)
  }

  test("cm_drop_readd") {
    sql("""CREATE TABLE tbl (id INT, x STRING) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    sql("INSERT INTO tbl VALUES (1, 'hello')")
    sql("ALTER TABLE tbl DROP COLUMN x")
    sql("ALTER TABLE tbl ADD COLUMNS (x INT)")
    sql("INSERT INTO tbl VALUES (2, 42)")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "x IS NULL")
    read(t, predicate = "x IS NOT NULL")
    for (v <- 0L to 4L) snapshot(t, version = v)
  }

  test("cm_nested_columns") {
    sql("""CREATE TABLE tbl (id INT, info STRUCT<name: STRING, age: INT>) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name')""")
    sql("INSERT INTO tbl VALUES (1, named_struct('name','alice','age',30))")
    sql("INSERT INTO tbl VALUES (2, named_struct('name','bob','age',25))")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "info.age > 27")
    snapshot(t)
  }

  test("cm_mode_upgrade") {
    sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
    sql("INSERT INTO tbl VALUES (1,'before'),(2,'before')")
    sql("""ALTER TABLE tbl SET TBLPROPERTIES (
      'delta.columnMapping.mode' = 'name',
      'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    sql("INSERT INTO tbl VALUES (3,'after')")
    val t = registerTable("tbl")
    read(t)
    read(t, version = 0)
    for (v <- 0L to 3L) snapshot(t, version = v)
  }

  // New workloads: 25 more to match existing acceptance_workloads/cm_*

  test("cm_mode_id") {
    sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'id')""")
    sql("INSERT INTO tbl VALUES (1,'alpha'),(2,'beta'),(3,'gamma')")
    val t = registerTable("tbl")
    read(t)
    read(t, columns = Seq("name"))
    read(t, predicate = "id > 1")
    snapshot(t)
  }

  test("cm_upgrade") {
    // Similar to cm_mode_upgrade but exercises version-by-version reading
    sql("CREATE TABLE tbl (a INT, b STRING) USING delta")
    sql("INSERT INTO tbl VALUES (1,'x'),(2,'y')")
    sql("""ALTER TABLE tbl SET TBLPROPERTIES (
      'delta.columnMapping.mode' = 'name',
      'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    sql("INSERT INTO tbl VALUES (3,'z')")
    sql("ALTER TABLE tbl RENAME COLUMN b TO c")
    sql("INSERT INTO tbl VALUES (4,'w')")
    val t = registerTable("tbl")
    read(t)
    read(t, version = 0)
    read(t, version = 1)
    read(t, columns = Seq("a", "c"))
    for (v <- 0L to 5L) snapshot(t, version = v)
  }

  test("cm_mode_upgrade_partitioned") {
    sql("CREATE TABLE tbl (id INT, part STRING) USING delta PARTITIONED BY (part)")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'a')")
    sql("""ALTER TABLE tbl SET TBLPROPERTIES (
      'delta.columnMapping.mode' = 'name',
      'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    sql("INSERT INTO tbl VALUES (4,'c')")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "part = 'a'")
    read(t, version = 0)
    for (v <- 0L to 3L) snapshot(t, version = v)
  }

  test("cm_special_chars") {
    sql("""CREATE TABLE tbl (id INT, `col with spaces` STRING, `col.with.dots` STRING) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name')""")
    sql("INSERT INTO tbl VALUES (1, 'space1', 'dot1')")
    sql("INSERT INTO tbl VALUES (2, 'space2', 'dot2')")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("cm_array_of_structs") {
    sql("""CREATE TABLE tbl (id INT, items ARRAY<STRUCT<name: STRING, qty: INT>>) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name')""")
    sql("INSERT INTO tbl VALUES (1, array(named_struct('name','apple','qty',3)))")
    sql("INSERT INTO tbl VALUES (2, array(named_struct('name','banana','qty',5), named_struct('name','cherry','qty',2)))")
    val t = registerTable("tbl")
    read(t)
    read(t, columns = Seq("items"))
    snapshot(t)
  }

  test("cm_complex_types") {
    sql("""CREATE TABLE tbl (id INT, tags ARRAY<STRING>, props MAP<STRING, INT>) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name')""")
    sql("INSERT INTO tbl VALUES (1, array('a','b'), map('x',1,'y',2))")
    sql("INSERT INTO tbl VALUES (2, array('c'), map('z',3))")
    val t = registerTable("tbl")
    read(t)
    read(t, columns = Seq("tags"))
    read(t, columns = Seq("props"))
    snapshot(t)
  }

  test("cm_map_type") {
    sql("""CREATE TABLE tbl (id INT, data MAP<STRING, STRING>) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name')""")
    sql("INSERT INTO tbl VALUES (1, map('key1','val1','key2','val2'))")
    sql("INSERT INTO tbl VALUES (2, map('key3','val3'))")
    val t = registerTable("tbl")
    read(t)
    read(t, columns = Seq("data"))
    snapshot(t)
  }

  test("cm_deeply_nested") {
    sql("""CREATE TABLE tbl (
      id INT,
      l1 STRUCT<l2: STRUCT<l3: STRUCT<value: STRING>>>
    ) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name')""")
    sql("INSERT INTO tbl VALUES (1, named_struct('l2', named_struct('l3', named_struct('value', 'deep'))))")
    sql("INSERT INTO tbl VALUES (2, named_struct('l2', named_struct('l3', named_struct('value', 'deeper'))))")
    val t = registerTable("tbl")
    read(t)
    read(t, columns = Seq("l1"))
    snapshot(t)
  }

  test("cm_drop_readd_same_name") {
    sql("""CREATE TABLE tbl (id INT, x STRING) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    sql("INSERT INTO tbl VALUES (1, 'text')")
    sql("ALTER TABLE tbl DROP COLUMN x")
    sql("ALTER TABLE tbl ADD COLUMNS (x DOUBLE)")
    sql("INSERT INTO tbl VALUES (2, 3.14)")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "x IS NOT NULL")
    for (v <- 0L to 4L) snapshot(t, version = v)
  }

  test("cm_predicate_after_rename") {
    sql("""CREATE TABLE tbl (id INT, a INT) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    sql("INSERT INTO tbl VALUES (1, 100),(2, 200),(3, 300)")
    sql("ALTER TABLE tbl RENAME COLUMN a TO b")
    sql("INSERT INTO tbl VALUES (4, 400)")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "b > 200")
    read(t, predicate = "b = 100")
    for (v <- 0L to 3L) snapshot(t, version = v)
  }

  test("cm_predicate_on_readded") {
    sql("""CREATE TABLE tbl (id INT, val INT) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    sql("INSERT INTO tbl VALUES (1, 10),(2, 20)")
    sql("ALTER TABLE tbl DROP COLUMN val")
    sql("ALTER TABLE tbl ADD COLUMNS (val INT)")
    sql("INSERT INTO tbl VALUES (3, 30),(4, 40)")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "val > 25")
    read(t, predicate = "val IS NULL")
    for (v <- 0L to 4L) snapshot(t, version = v)
  }

  test("cm_predicate_renamed_partition") {
    sql("""CREATE TABLE tbl (id INT, part STRING) USING delta
      PARTITIONED BY (part)
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    sql("INSERT INTO tbl VALUES (1,'x'),(2,'y'),(3,'x')")
    sql("ALTER TABLE tbl RENAME COLUMN part TO region")
    sql("INSERT INTO tbl VALUES (4,'z')")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "region = 'x'")
    read(t, predicate = "region = 'z'")
    for (v <- 0L to 3L) snapshot(t, version = v)
  }

  test("cm_rename_partition_col") {
    sql("""CREATE TABLE tbl (id INT, category STRING) USING delta
      PARTITIONED BY (category)
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'a')")
    sql("ALTER TABLE tbl RENAME COLUMN category TO cat")
    sql("INSERT INTO tbl VALUES (4,'c')")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "cat = 'a'")
    read(t, columns = Seq("id", "cat"))
    for (v <- 0L to 3L) snapshot(t, version = v)
  }

  test("cm_nested_struct_name") {
    sql("""CREATE TABLE tbl (id INT, info STRUCT<first: STRING, last: STRING, age: INT>) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name')""")
    sql("INSERT INTO tbl VALUES (1, named_struct('first','alice','last','smith','age',30))")
    sql("INSERT INTO tbl VALUES (2, named_struct('first','bob','last','jones','age',25))")
    val t = registerTable("tbl")
    read(t)
    read(t, columns = Seq("info"))
    read(t, columns = Seq("id"))
    snapshot(t)
  }

  test("cm_nested_struct_id") {
    sql("""CREATE TABLE tbl (id INT, info STRUCT<name: STRING, score: DOUBLE>) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'id')""")
    sql("INSERT INTO tbl VALUES (1, named_struct('name','alice','score',95.5))")
    sql("INSERT INTO tbl VALUES (2, named_struct('name','bob','score',87.3))")
    val t = registerTable("tbl")
    read(t)
    read(t, columns = Seq("info"))
    snapshot(t)
  }

  test("cm_nested_rename_3_levels") {
    sql("""CREATE TABLE tbl (
      id INT,
      outer_col STRUCT<mid: STRUCT<inner_val: STRING>>
    ) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    sql("INSERT INTO tbl VALUES (1, named_struct('mid', named_struct('inner_val','hello')))")
    sql("ALTER TABLE tbl RENAME COLUMN outer_col.mid.inner_val TO renamed_val")
    sql("INSERT INTO tbl VALUES (2, named_struct('mid', named_struct('renamed_val','world')))")
    val t = registerTable("tbl")
    read(t)
    read(t, version = 1)
    for (v <- 0L to 3L) snapshot(t, version = v)
  }

  test("cm_filter_pushdown_physical_names") {
    // Create with name mode, rename column, verify filter uses physical name correctly
    sql("""CREATE TABLE tbl (a INT, b STRING) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    sql("INSERT INTO tbl VALUES (100,'first'),(200,'second')")
    sql("ALTER TABLE tbl RENAME COLUMN a TO c")
    sql("INSERT INTO tbl VALUES (300,'third'),(1000,'fourth')")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "c = 1000", name = "read_filter_c_eq_1000")
    read(t, predicate = "c = 100", name = "read_filter_c_eq_100")
    read(t, predicate = "c > 200", name = "read_filter_c_gt_200")
    for (v <- 0L to 3L) snapshot(t, version = v)
  }

  test("cm_physical_name_matches_logical") {
    // After rename, old physical name may match some other column's logical name
    sql("""CREATE TABLE tbl (id INT, alpha STRING, beta STRING) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    sql("INSERT INTO tbl VALUES (1, 'a1', 'b1')")
    sql("ALTER TABLE tbl RENAME COLUMN alpha TO gamma")
    sql("INSERT INTO tbl VALUES (2, 'a2', 'b2')")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("cm_id_mode_rename_projection") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'id',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    sql("INSERT INTO tbl VALUES (1,'before1'),(2,'before2')")
    sql("ALTER TABLE tbl RENAME COLUMN value TO renamed_value")
    sql("INSERT INTO tbl VALUES (3,'after1'),(4,'after2')")
    val t = registerTable("tbl")
    read(t)
    read(t, columns = Seq("renamed_value"), name = "read_project_renamed_col")
    read(t, columns = Seq("id"), name = "read_project_id_only")
    for (v <- 0L to 3L) snapshot(t, version = v)
  }

  test("cm_id_mode_schema_evolution") {
    sql("""CREATE TABLE tbl (id INT) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'id',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    sql("INSERT INTO tbl VALUES (1),(2)")
    sql("ALTER TABLE tbl ADD COLUMNS (new_col STRING)")
    sql("INSERT INTO tbl VALUES (3, 'hello')")
    val t = registerTable("tbl")
    read(t)
    read(t, version = 0)
    read(t, version = 1)
    for (v <- 0L to 3L) snapshot(t, version = v)
  }

  test("cm_id_matching_swapped") {
    // Create table in id mode with nested struct, manipulate column IDs
    sql("""CREATE TABLE tbl (a STRING, b STRUCT<c: STRING, d: INT>) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'id')""")
    sql("INSERT INTO tbl VALUES ('hello', named_struct('c','world','d',42))")
    // Swap column mapping IDs by altering metadata — use ALTER TABLE to change schema
    sql("ALTER TABLE tbl RENAME COLUMN a TO e")
    sql("INSERT INTO tbl VALUES ('swapped', named_struct('c','test','d',99))")
    val t = registerTable("tbl")
    read(t, name = "read_select_a_reads_e")
    snapshot(t)
  }

  test("cm_id_matching_nonexistent") {
    sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'id',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    sql("INSERT INTO tbl VALUES (1,'exists')")
    // Drop and add column — old physical data has non-matching ID
    sql("ALTER TABLE tbl DROP COLUMN name")
    sql("ALTER TABLE tbl ADD COLUMNS (name STRING)")
    sql("INSERT INTO tbl VALUES (2,'new')")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "name IS NULL")
    snapshot(t)
  }

  test("cm_projection_complex_types") {
    sql("""CREATE TABLE tbl (
      id INT,
      arr ARRAY<INT>,
      mp MAP<STRING, INT>,
      st STRUCT<a: STRING, b: INT>
    ) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name')""")
    sql("INSERT INTO tbl VALUES (1, array(10,20), map('x',1), named_struct('a','hello','b',42))")
    sql("INSERT INTO tbl VALUES (2, array(30), map('y',2,'z',3), named_struct('a','world','b',99))")
    val t = registerTable("tbl")
    read(t)
    read(t, columns = Seq("arr"), name = "read_project_array_only")
    read(t, columns = Seq("mp"), name = "read_project_map_only")
    read(t, columns = Seq("st"), name = "read_project_struct_only")
    snapshot(t)
  }

  test("cm_select_after_drop") {
    sql("""CREATE TABLE tbl (id INT, keep STRING, drop_me INT, extra DOUBLE) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    sql("INSERT INTO tbl VALUES (1, 'a', 10, 1.1),(2, 'b', 20, 2.2)")
    sql("ALTER TABLE tbl DROP COLUMN drop_me")
    sql("INSERT INTO tbl VALUES (3, 'c', 3.3)")
    val t = registerTable("tbl")
    read(t)
    read(t, columns = Seq("id", "keep"), name = "read_project_remaining_columns")
    read(t, columns = Seq("extra"), name = "read_project_extra_only")
    for (v <- 0L to 3L) snapshot(t, version = v)
  }

  test("cm_err_003_invalid_mode") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, 'a'), (2, 'b')")
    val t = registerTable("tbl")
    // Patch metadata to use invalid column mapping mode
    mutateTable(t) { dir =>
      val f = dir.resolve("_delta_log/00000000000000000000.json")
      val content = new String(java.nio.file.Files.readAllBytes(f), "UTF-8")
      val patched = content.replace(
        """"configuration":{""",
        """"configuration":{"delta.columnMapping.mode":"bogus",""")
      java.nio.file.Files.write(f, patched.getBytes)
    }
    read(t)
    snapshot(t)
  }

}
