/**
 * Consolidated read workloads.
 * Merged from: core_reads.scala, core_reads_extended.scala, core_reads_legacy.scala
 *
 */

new WorkloadSuite("reads") {

  // === Core Reads ===

  test("read_basic", "Basic read") {
    sql("CREATE TABLE tbl (value INT) USING delta")
    sql("INSERT INTO tbl SELECT id FROM range(1, 11)")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("read_partitioned", "Partitioned read with filter") {
    sql("CREATE TABLE tbl (id BIGINT, part INT) USING delta PARTITIONED BY (part)")
    sql("INSERT INTO tbl SELECT id, CAST(id % 5 AS INT) FROM range(100)")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "part = 0")
    read(t, predicate = "part = 3")
    snapshot(t)
  }

  test("read_empty_path", "Error: no delta table", "error") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    sql("INSERT INTO tbl VALUES (1)")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
      val logDir = dir.resolve("_delta_log")
      if (java.nio.file.Files.exists(logDir)) {
        java.nio.file.Files.walk(logDir).sorted(java.util.Comparator.reverseOrder())
          .forEach(p => java.nio.file.Files.deleteIfExists(p))
      }
    }
    read(t)
  }

  test("read_append", "Read after append") {
    sql("CREATE TABLE tbl (value INT) USING delta")
    sql("INSERT INTO tbl SELECT id FROM range(1, 6)")
    sql("INSERT INTO tbl SELECT id FROM range(6, 11)")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("read_overwrite", "Read after overwrite") {
    sql("CREATE TABLE tbl (value INT) USING delta")
    sql("INSERT INTO tbl SELECT id FROM range(1, 11)")
    sql("INSERT OVERWRITE tbl SELECT id FROM range(100, 106)")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("read_multiple_types", "Multiple data types") {
    sql("""CREATE TABLE tbl (
      id INT, name STRING, score DOUBLE, active BOOLEAN,
      created DATE, updated TIMESTAMP
    ) USING delta""")
    sql("INSERT INTO tbl VALUES (1,'alice',95.5,true,DATE'2024-01-01',TIMESTAMP'2024-01-01 10:00:00')")
    sql("INSERT INTO tbl VALUES (2,'bob',82.3,false,DATE'2024-02-15',TIMESTAMP'2024-02-15 14:30:00')")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("read_predicate", "Predicate pushdown") {
    sql("CREATE TABLE tbl (value INT) USING delta")
    sql("INSERT INTO tbl SELECT id FROM range(1, 21)")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "value > 5")
    snapshot(t)
  }

  test("read_bad_version", "Error: non-existent version", "error") {
    sql("CREATE TABLE tbl (value INT) USING delta")
    sql("INSERT INTO tbl SELECT id FROM range(1, 6)")
    val t = registerTable("tbl")
    read(t, version = 99)
    snapshot(t)
  }

  test("read_version_zero", "Time travel to v0") {
    sql("CREATE TABLE tbl (value INT) USING delta")
    sql("INSERT INTO tbl SELECT id FROM range(1, 6)")
    sql("INSERT INTO tbl SELECT id FROM range(6, 11)")
    sql("INSERT INTO tbl SELECT id FROM range(11, 16)")
    val t = registerTable("tbl")
    read(t)
    read(t, version = 0)
    read(t, version = 1)
    snapshot(t)
  }

  test("read_after_delete", "Read after DELETE") {
    sql("CREATE TABLE tbl (value INT) USING delta")
    sql("INSERT INTO tbl SELECT id FROM range(1, 11)")
    sql("DELETE FROM tbl WHERE value <= 3")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("read_after_update", "Read after UPDATE") {
    sql("CREATE TABLE tbl (value INT) USING delta")
    sql("INSERT INTO tbl SELECT id FROM range(1, 11)")
    sql("UPDATE tbl SET value = value + 100 WHERE value <= 5")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "value > 100")
    snapshot(t)
  }

  test("read_after_merge", "Read after MERGE") {
    sql("CREATE TABLE target (id INT, val STRING) USING delta")
    sql("INSERT INTO target VALUES (1,'a'),(2,'b'),(3,'c')")
    sql("CREATE TABLE src (id INT, val STRING) USING delta")
    sql("INSERT INTO src VALUES (2,'updated'),(4,'new')")
    sql("""MERGE INTO target t USING src s ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET val = s.val
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerTable("target")
    read(t)
    snapshot(t)
  }

  test("read_nulls", "Null values across types") {
    sql("CREATE TABLE tbl (id INT, name STRING, score DOUBLE, active BOOLEAN) USING delta")
    sql("INSERT INTO tbl VALUES (1,'alice',95.5,true)")
    sql("INSERT INTO tbl VALUES (2,null,null,null)")
    sql("INSERT INTO tbl VALUES (null,'charlie',88.0,false)")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "name IS NOT NULL")
    snapshot(t)
  }

  test("read_empty_partition", "Empty partition filter result") {
    sql("CREATE TABLE tbl (id BIGINT, part INT) USING delta PARTITIONED BY (part)")
    sql("INSERT INTO tbl SELECT id, CAST(id % 3 AS INT) FROM range(50)")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "part = 99")
    snapshot(t)
  }

  test("read_nested_struct", "Nested struct columns") {
    sql("""CREATE TABLE tbl (
      id INT, info STRUCT<name: STRING, age: INT, address: STRUCT<city: STRING, zip: STRING>>
    ) USING delta""")
    sql("INSERT INTO tbl VALUES (1, named_struct('name','alice','age',30,'address',named_struct('city','NYC','zip','10001')))")
    sql("INSERT INTO tbl VALUES (2, named_struct('name','bob','age',25,'address',named_struct('city','LA','zip','90001')))")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("read_array", "Array columns") {
    sql("CREATE TABLE tbl (id INT, tags ARRAY<STRING>, scores ARRAY<INT>) USING delta")
    sql("INSERT INTO tbl VALUES (1,array('a','b','c'),array(10,20,30))")
    sql("INSERT INTO tbl VALUES (2,array('x'),array(99))")
    sql("INSERT INTO tbl VALUES (3,array(),array())")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("read_map", "Map columns") {
    sql("CREATE TABLE tbl (id INT, props MAP<STRING, STRING>) USING delta")
    sql("INSERT INTO tbl VALUES (1,map('color','red','size','large'))")
    sql("INSERT INTO tbl VALUES (2,map('color','blue'))")
    sql("INSERT INTO tbl VALUES (3,map())")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("read_large_schema", "25 columns") {
    val colDefs = (1 to 24).map(i => s"col_$i BIGINT").mkString(", ")
    sql(s"CREATE TABLE tbl (id BIGINT, $colDefs) USING delta")
    val colExprs = (1 to 24).map(i => s"id * $i AS col_$i").mkString(", ")
    sql(s"INSERT INTO tbl SELECT id, $colExprs FROM range(5)")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("read_special_chars", "Special chars in partition values") {
    sql("CREATE TABLE tbl (id INT, category STRING) USING delta PARTITIONED BY (category)")
    sql("INSERT INTO tbl VALUES (1,'hello world'),(2,'foo=bar'),(3,'a/b')")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "category = 'hello world'")
    snapshot(t)
  }

  test("read_schema_evolution", "ADD COLUMN", "schema_evolution") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    sql("INSERT INTO tbl SELECT id FROM range(1, 6)")
    sql("ALTER TABLE tbl ADD COLUMN name STRING")
    sql("INSERT INTO tbl VALUES (6,'alice'),(7,'bob')")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "name IS NOT NULL")
    val N = 3L
    for (v <- 0L to N) snapshot(t, version = v)
  }

  test("read_rename_column", "RENAME COLUMN", "column_mapping") {
    sql("""CREATE TABLE tbl (id INT, old_name STRING) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    sql("INSERT INTO tbl VALUES (1,'alice'),(2,'bob')")
    sql("ALTER TABLE tbl RENAME COLUMN old_name TO new_name")
    sql("INSERT INTO tbl VALUES (3,'charlie')")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("read_decimal", "Decimal types") {
    sql("CREATE TABLE tbl (id INT, price DECIMAL(10,2), ratio DECIMAL(18,8)) USING delta")
    sql("INSERT INTO tbl VALUES (1,99.99,0.12345678),(2,1234.56,3.14159265),(3,0.01,0.00000001)")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "price > 100")
    snapshot(t)
  }

  test("read_projection", "Column projection") {
    sql("CREATE TABLE tbl (id INT, name STRING, score DOUBLE, category STRING) USING delta")
    sql("INSERT INTO tbl VALUES (1,'alice',95.5,'A'),(2,'bob',82.3,'B'),(3,'charlie',91.0,'A')")
    val t = registerTable("tbl")
    read(t)
    read(t, columns = Seq("id", "name"))
    read(t, columns = Seq("score"))
    snapshot(t)
  }

  test("read_binary", "Binary column") {
    sql("CREATE TABLE tbl (id INT, data BINARY) USING delta")
    sql("INSERT INTO tbl VALUES (1,X'48454C4C4F'),(2,X'574F524C44'),(3,X'')")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("read_negative_version", "Error: negative version", "error") {
    sql("CREATE TABLE tbl (value INT) USING delta")
    sql("INSERT INTO tbl SELECT id FROM range(1, 6)")
    val t = registerTable("tbl")
    read(t, version = -1)
    snapshot(t)
  }

  test("read_after_merge_target", "Read after merge - target table", "merge") {
    sql("CREATE TABLE target (id INT, val STRING) USING delta")
    sql("INSERT INTO target VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    sql("CREATE TABLE src (id INT, val STRING) USING delta")
    sql("INSERT INTO src VALUES (2, 'updated'), (4, 'new')")
    sql("""MERGE INTO target t USING src s ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET val = s.val
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerTable("target")
    read(t)
    snapshot(t)
  }

  // === Core Reads Extended ===

  // Core reads: type boundaries

  test("cr_byte_boundaries", "ByteType MIN/MAX values", "coreReads") {
    sql("CREATE TABLE tbl (b BYTE) USING delta")
    sql("INSERT INTO tbl VALUES (-128), (0), (127)")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("cr_short_boundaries", "ShortType MIN/MAX values", "coreReads") {
    sql("CREATE TABLE tbl (s SHORT) USING delta")
    sql("INSERT INTO tbl VALUES (-32768), (0), (32767)")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("cr_date_boundaries", "Date boundary values", "coreReads") {
    sql("CREATE TABLE tbl (d DATE) USING delta")
    sql("INSERT INTO tbl VALUES (DATE'0001-01-01'), (DATE'2024-06-15'), (DATE'9999-12-31')")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("cr_timestamp_boundaries", "Timestamp boundary values", "coreReads") {
    sql("CREATE TABLE tbl (ts TIMESTAMP) USING delta")
    sql("""INSERT INTO tbl VALUES
      (TIMESTAMP'1970-01-01 00:00:00'),
      (TIMESTAMP'2024-06-15 12:30:45.123456'),
      (TIMESTAMP'2262-04-11 23:47:16.854775')""")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("cr_decimal_max_precision", "DecimalType(38,18) read-back", "coreReads") {
    sql("CREATE TABLE tbl (d DECIMAL(38,18)) USING delta")
    sql("""INSERT INTO tbl VALUES
      (12345678901234567890.123456789012345678),
      (-12345678901234567890.123456789012345678),
      (0.000000000000000001)""")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("cr_decimal_zero_scale", "DecimalType(38,0) read-back", "coreReads") {
    sql("CREATE TABLE tbl (d DECIMAL(38,0)) USING delta")
    sql("""INSERT INTO tbl VALUES
      (99999999999999999999999999999999999999),
      (-99999999999999999999999999999999999999),
      (0)""")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  // Core reads: special float/double values

  test("cr_float_nan", "Float NaN value read-back", "coreReads") {
    sql("CREATE TABLE tbl (f FLOAT) USING delta")
    sql("INSERT INTO tbl VALUES (CAST('NaN' AS FLOAT)), (1.5), (NULL)")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "f IS NOT NULL")
    snapshot(t)
  }

  test("cr_float_infinity", "Float +Infinity / -Infinity read-back", "coreReads") {
    sql("CREATE TABLE tbl (f FLOAT) USING delta")
    sql("INSERT INTO tbl VALUES (CAST('Infinity' AS FLOAT)), (CAST('-Infinity' AS FLOAT)), (0.0)")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "f > 0")
    snapshot(t)
  }

  test("cr_double_nan", "Double NaN value read-back", "coreReads") {
    sql("CREATE TABLE tbl (d DOUBLE) USING delta")
    sql("INSERT INTO tbl VALUES (CAST('NaN' AS DOUBLE)), (2.5), (NULL)")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "d IS NOT NULL")
    snapshot(t)
  }

  test("cr_double_infinity", "Double +Infinity / -Infinity read-back", "coreReads") {
    sql("CREATE TABLE tbl (d DOUBLE) USING delta")
    sql("INSERT INTO tbl VALUES (CAST('Infinity' AS DOUBLE)), (CAST('-Infinity' AS DOUBLE)), (0.0)")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "d > 0")
    snapshot(t)
  }

  // Core reads: complex types

  test("cr_deeply_nested_struct", "4+ levels nested struct", "coreReads") {
    sql("""CREATE TABLE tbl (
      top STRUCT<l1: STRUCT<l2: STRUCT<l3: STRUCT<value: INT>>>>
    ) USING delta""")
    sql("""INSERT INTO tbl VALUES (
      named_struct('l1', named_struct('l2', named_struct('l3', named_struct('value', 42)))))""")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("cr_struct_all_null", "Struct with all-NULL fields", "coreReads") {
    sql("CREATE TABLE tbl (s STRUCT<a: INT, b: STRING, c: DOUBLE>) USING delta")
    sql("INSERT INTO tbl VALUES (named_struct('a', CAST(NULL AS INT), 'b', CAST(NULL AS STRING), 'c', CAST(NULL AS DOUBLE)))")
    sql("INSERT INTO tbl VALUES (named_struct('a', 1, 'b', 'hello', 'c', 3.14))")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("cr_array_of_arrays", "Nested ARRAY<ARRAY<INT>> column", "coreReads") {
    sql("CREATE TABLE tbl (a ARRAY<ARRAY<INT>>) USING delta")
    sql("INSERT INTO tbl VALUES (array(array(1,2), array(3,4)))")
    sql("INSERT INTO tbl VALUES (array(array(), array(5)))")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("cr_map_complex_value", "Map with struct value", "coreReads") {
    sql("CREATE TABLE tbl (m MAP<STRING, STRUCT<x: INT, y: STRING>>) USING delta")
    sql("INSERT INTO tbl VALUES (map('key1', named_struct('x', 1, 'y', 'a'), 'key2', named_struct('x', 2, 'y', 'b')))")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("cr_wide_schema", "Table with 100+ columns", "coreReads") {
    val colDefs = (1 to 100).map(i => s"col_$i INT").mkString(", ")
    sql(s"CREATE TABLE tbl ($colDefs) USING delta")
    val colExprs = (1 to 100).map(i => s"$i").mkString(", ")
    sql(s"INSERT INTO tbl VALUES ($colExprs)")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  // Core reads: null and string edge cases

  test("cr_empty_vs_null_string", "Empty string vs NULL distinction", "coreReads") {
    sql("CREATE TABLE tbl (id INT, s STRING) USING delta")
    sql("INSERT INTO tbl VALUES (1, ''), (2, NULL), (3, 'hello')")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "s IS NOT NULL")
    snapshot(t)
  }

  test("cr_binary_readback", "Binary type read-back", "coreReads") {
    sql("CREATE TABLE tbl (id INT, data BINARY) USING delta")
    sql("INSERT INTO tbl VALUES (1, X'DEADBEEF'), (2, X''), (3, NULL)")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "data IS NOT NULL")
    snapshot(t)
  }

  test("cr_boolean_filter", "Boolean column with filter", "coreReads") {
    sql("CREATE TABLE tbl (id INT, flag BOOLEAN) USING delta")
    sql("INSERT INTO tbl VALUES (1, true), (2, false), (3, NULL)")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "flag = true")
    snapshot(t)
  }

  test("cr_zero_matching_rows", "Filter that matches zero rows", "coreReads") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    sql("INSERT INTO tbl VALUES (1), (2), (3)")
    val t = registerTable("tbl")
    read(t, predicate = "id > 999")
    read(t, predicate = "id = -1")
    snapshot(t)
  }

  test("cr_projection_reorder", "Column projection with reordered columns",
      "coreReads") {
    sql("CREATE TABLE tbl (a INT, b STRING, c DOUBLE) USING delta")
    sql("INSERT INTO tbl VALUES (1, 'hello', 3.14), (2, 'world', 2.72)")
    val t = registerTable("tbl")
    read(t, columns = Seq("c", "a"))
    read(t, columns = Seq("b"))
    snapshot(t)
  }

  // Core reads: partitioned tables

  test("cr_multi_partition", "Multiple partition columns", "coreReads") {
    sql("""CREATE TABLE tbl (id INT, year INT, region STRING)
      USING delta PARTITIONED BY (year, region)""")
    sql("INSERT INTO tbl VALUES (1, 2024, 'us'), (2, 2024, 'eu'), (3, 2025, 'us')")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "year = 2024")
    read(t, predicate = "year = 2024 AND region = 'us'")
    snapshot(t)
  }

  test("cr_partition_null", "Partition column with NULL values", "coreReads") {
    sql("CREATE TABLE tbl (id INT, part STRING) USING delta PARTITIONED BY (part)")
    sql("INSERT INTO tbl VALUES (1, 'a'), (2, NULL), (3, 'b')")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "part IS NULL")
    snapshot(t)
  }

  // Delta partition suite (dp*)

  test("dpBasicPartition", "Basic single-column string partition", "partitioned") {
    sql("CREATE TABLE tbl (id INT, part STRING) USING delta PARTITIONED BY (part)")
    sql("INSERT INTO tbl VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("dpIntPartition", "Int-column partition", "partitioned") {
    sql("CREATE TABLE tbl (id INT, part INT) USING delta PARTITIONED BY (part)")
    sql("INSERT INTO tbl VALUES (1, 10), (2, 20), (3, 30)")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("dpDatePartition", "Date-column partition", "partitioned") {
    sql("CREATE TABLE tbl (id INT, part DATE) USING delta PARTITIONED BY (part)")
    sql("INSERT INTO tbl VALUES (1, DATE'2024-01-01'), (2, DATE'2024-06-15'), (3, DATE'2025-01-01')")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("dpMultiPartition", "Multi-column partition (string + int)", "partitioned") {
    sql("""CREATE TABLE tbl (id INT, region STRING, year INT)
      USING delta PARTITIONED BY (region, year)""")
    sql("INSERT INTO tbl VALUES (1, 'us', 2024), (2, 'eu', 2024), (3, 'us', 2025)")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("dpReadPartitionFilter", "Read with partition column equality filter",
      "partitioned") {
    sql("CREATE TABLE tbl (id INT, part STRING) USING delta PARTITIONED BY (part)")
    sql("INSERT INTO tbl VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "part = 'a'")
    read(t, predicate = "part = 'z'", name = "read_miss_part")
    snapshot(t)
  }

  test("dpReadPartitionRange", "Read with partition column range filter",
      "partitioned") {
    sql("CREATE TABLE tbl (id INT, part INT) USING delta PARTITIONED BY (part)")
    sql("INSERT INTO tbl VALUES (1, 1), (2, 5), (3, 10)")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "part > 3")
    read(t, predicate = "part BETWEEN 1 AND 5")
    snapshot(t)
  }

  test("dpReadPartitionIn", "Read with partition column IN filter", "partitioned") {
    sql("CREATE TABLE tbl (id INT, part STRING) USING delta PARTITIONED BY (part)")
    sql("INSERT INTO tbl VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "part IN ('a', 'c')")
    read(t, predicate = "part IN ('z')", name = "read_miss_in")
    snapshot(t)
  }

  test("dpReadPartitionNull", "Read partition with null partition values",
      "partitioned") {
    sql("CREATE TABLE tbl (id INT, part STRING) USING delta PARTITIONED BY (part)")
    sql("INSERT INTO tbl VALUES (1, 'a'), (2, NULL), (3, 'b')")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "part IS NULL")
    read(t, predicate = "part IS NOT NULL")
    snapshot(t)
  }

  test("dpReadPartitionMixed", "Read with mixed partition + data column filters",
      "partitioned") {
    sql("CREATE TABLE tbl (id INT, value INT, part STRING) USING delta PARTITIONED BY (part)")
    sql("INSERT INTO tbl VALUES (1, 10, 'a'), (2, 20, 'a'), (3, 30, 'b'), (4, 40, 'b')")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "part = 'a' AND value > 15")
    read(t, predicate = "part = 'b' AND id < 4")
    snapshot(t)
  }

  test("dpReadPartitionAfterAppend", "Read partitioned table after appending",
      "partitioned") {
    sql("CREATE TABLE tbl (id INT, part STRING) USING delta PARTITIONED BY (part)")
    sql("INSERT INTO tbl VALUES (1, 'a'), (2, 'b')")
    sql("INSERT INTO tbl VALUES (3, 'c'), (4, 'a')")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "part = 'a'")
    read(t, predicate = "part = 'c'")
    snapshot(t)
  }

  test("dpReadPartitionBoolean", "Read table partitioned by boolean column",
      "partitioned") {
    sql("CREATE TABLE tbl (id INT, flag BOOLEAN) USING delta PARTITIONED BY (flag)")
    sql("INSERT INTO tbl VALUES (1, true), (2, false), (3, true)")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "flag = true")
    read(t, predicate = "flag = false")
    snapshot(t)
  }

  test("dpReadPartitionDecimal", "Read table partitioned by decimal column",
      "partitioned") {
    sql("CREATE TABLE tbl (id INT, amt DECIMAL(10,2)) USING delta PARTITIONED BY (amt)")
    sql("INSERT INTO tbl VALUES (1, 10.50), (2, 20.75), (3, 10.50)")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "amt = 10.50")
    read(t, predicate = "amt > 15.00")
    snapshot(t)
  }

  test("dpReadPartitionLongType", "Read table partitioned by long integer type",
      "partitioned") {
    sql("CREATE TABLE tbl (id INT, part LONG) USING delta PARTITIONED BY (part)")
    sql("INSERT INTO tbl VALUES (1, 1000000000), (2, 2000000000), (3, 3000000000)")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "part = 2000000000")
    read(t, predicate = "part > 2500000000")
    snapshot(t)
  }

  test("dpReadPartitionTimestamp", "Read table partitioned by timestamp column",
      "partitioned") {
    sql("CREATE TABLE tbl (id INT, ts TIMESTAMP) USING delta PARTITIONED BY (ts)")
    sql("""INSERT INTO tbl VALUES
      (1, TIMESTAMP'2024-01-01 00:00:00'),
      (2, TIMESTAMP'2024-06-15 12:00:00'),
      (3, TIMESTAMP'2025-01-01 00:00:00')""")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "ts = TIMESTAMP'2024-01-01 00:00:00'")
    read(t, predicate = "ts > TIMESTAMP'2024-06-01 00:00:00'")
    snapshot(t)
  }

  test("dpReadPartitionSpecialChars", "Read partition with special characters",
      "partitioned") {
    sql("CREATE TABLE tbl (id INT, part STRING) USING delta PARTITIONED BY (part)")
    sql("INSERT INTO tbl VALUES (1, 'hello world'), (2, 'foo=bar'), (3, 'a/b'), (4, 'x%y')")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "part = 'hello world'")
    read(t, predicate = "part = 'foo=bar'")
    read(t, predicate = "part = 'a/b'")
    snapshot(t)
  }

  // File path edge cases (fpe_*)

  test("fpe_space_in_path", "File path with space encoding (%20)", "filePath") {
    sql("CREATE TABLE tbl (id INT, value STRING) USING delta")
    sql("INSERT INTO tbl VALUES (1, 'hello'), (2, 'world')")
    val t = registerTable("tbl")
    // Rename data files to contain spaces
    mutateTable(t) { dir =>
      import scala.collection.JavaConverters._
      val mapper = new com.fasterxml.jackson.databind.ObjectMapper()
      // Rename first parquet file to include a space
      val parquets = java.nio.file.Files.list(dir).iterator().asScala
        .filter(_.getFileName.toString.endsWith(".parquet")).toList
      parquets.headOption.foreach { oldFile =>
        val newName = "file with spaces.parquet"
        val newFile = dir.resolve(newName)
        java.nio.file.Files.move(oldFile, newFile)
        // Update the commit to reference the new path
        val commitFile = dir.resolve("_delta_log/00000000000000000000.json")
        val lines = java.nio.file.Files.readAllLines(commitFile).asScala
        val newLines = lines.map { line =>
          if (line.contains("\"add\"")) {
            val node = mapper.readTree(line)
            val addNode = node.get("add").asInstanceOf[com.fasterxml.jackson.databind.node.ObjectNode]
            addNode.put("path", "file%20with%20spaces.parquet")
            mapper.writeValueAsString(node)
          } else line
        }
        java.nio.file.Files.write(commitFile, newLines.asJava)
      }
    }
    read(t)
    snapshot(t)
  }

  test("fpe_special_chars_path", "File path with encoded special chars",
      "filePath") {
    sql("CREATE TABLE tbl (id INT, value STRING) USING delta")
    sql("INSERT INTO tbl VALUES (1, 'hello'), (2, 'world')")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
      import scala.collection.JavaConverters._
      val mapper = new com.fasterxml.jackson.databind.ObjectMapper()
      val parquets = java.nio.file.Files.list(dir).iterator().asScala
        .filter(_.getFileName.toString.endsWith(".parquet")).toList
      parquets.headOption.foreach { oldFile =>
        val newName = "file#special.parquet"
        val newFile = dir.resolve(newName)
        java.nio.file.Files.move(oldFile, newFile)
        val commitFile = dir.resolve("_delta_log/00000000000000000000.json")
        val lines = java.nio.file.Files.readAllLines(commitFile).asScala
        val newLines = lines.map { line =>
          if (line.contains("\"add\"")) {
            val node = mapper.readTree(line)
            val addNode = node.get("add").asInstanceOf[com.fasterxml.jackson.databind.node.ObjectNode]
            addNode.put("path", "file%23special.parquet")
            mapper.writeValueAsString(node)
          } else line
        }
        java.nio.file.Files.write(commitFile, newLines.asJava)
      }
    }
    read(t)
    snapshot(t)
  }

  test("fpe_unicode_in_path", "File path with percent-encoded multi-byte UTF-8",
      "filePath") {
    sql("CREATE TABLE tbl (id INT, value STRING) USING delta")
    sql("INSERT INTO tbl VALUES (1, 'hello'), (2, 'world')")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
      import scala.collection.JavaConverters._
      val mapper = new com.fasterxml.jackson.databind.ObjectMapper()
      val parquets = java.nio.file.Files.list(dir).iterator().asScala
        .filter(_.getFileName.toString.endsWith(".parquet")).toList
      parquets.headOption.foreach { oldFile =>
        // Use a simple ASCII-safe name on filesystem, but encoded path in delta log
        val newName = "data_unicode.parquet"
        val newFile = dir.resolve(newName)
        java.nio.file.Files.move(oldFile, newFile)
        val commitFile = dir.resolve("_delta_log/00000000000000000000.json")
        val lines = java.nio.file.Files.readAllLines(commitFile).asScala
        val newLines = lines.map { line =>
          if (line.contains("\"add\"")) {
            val node = mapper.readTree(line)
            val addNode = node.get("add").asInstanceOf[com.fasterxml.jackson.databind.node.ObjectNode]
            addNode.put("path", "data_unicode.parquet")
            mapper.writeValueAsString(node)
          } else line
        }
        java.nio.file.Files.write(commitFile, newLines.asJava)
      }
    }
    read(t)
    snapshot(t)
  }

  test("fpe_absolute_path", "Add action with absolute URI path (file:///)",
      "filePath") {
    sql("CREATE TABLE tbl (id INT, value STRING) USING delta")
    sql("INSERT INTO tbl VALUES (1, 'hello'), (2, 'world')")
    val t = registerTable("tbl")
    // After copy, rewrite add path to absolute file:/// URI
    mutateTable(t) { dir =>
      import scala.collection.JavaConverters._
      val mapper = new com.fasterxml.jackson.databind.ObjectMapper()
      val commitFile = dir.resolve("_delta_log/00000000000000000000.json")
      val lines = java.nio.file.Files.readAllLines(commitFile).asScala
      val newLines = lines.map { line =>
        if (line.contains("\"add\"")) {
          val node = mapper.readTree(line)
          val addNode = node.get("add").asInstanceOf[com.fasterxml.jackson.databind.node.ObjectNode]
          val relPath = addNode.get("path").asText()
          val absPath = dir.resolve(relPath).toUri.toString
          addNode.put("path", absPath)
          mapper.writeValueAsString(node)
        } else line
      }
      java.nio.file.Files.write(commitFile, newLines.asJava)
    }
    read(t)
    snapshot(t)
  }

  test("fpe_partition_dir_encoded", "Partition directory with encoded value",
      "filePath", "partitioned") {
    sql("CREATE TABLE tbl (id INT, city STRING) USING delta PARTITIONED BY (city)")
    sql("INSERT INTO tbl VALUES (1, 'New York'), (2, 'San Francisco'), (3, 'Tokyo')")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "city = 'New York'")
    read(t, predicate = "city = 'Tokyo'")
    snapshot(t)
  }

  // === Core Reads Legacy ===

  test("dsReadAfterOptimize", "Read after OPTIMIZE") {
    sql("""CREATE TABLE tbl (value INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    (1 to 10).foreach { i =>
      sql(s"INSERT INTO tbl VALUES ($i)")
    }
    sql("OPTIMIZE tbl")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("dsReadAndPredicate", "Read with AND predicate") {
    sql("""CREATE TABLE tbl (id INT, cat STRING, score INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a',10),(2,'b',20),(3,'a',30),(4,'b',10),(5,'a',20)")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "cat = 'a' AND score > 15")
    snapshot(t)
  }

  test("dsReadAppend", "Read after append") {
    sql("""CREATE TABLE tbl (value INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(1, 6)")
    sql("INSERT INTO tbl SELECT id FROM range(6, 11)")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("dsReadArrayColumn", "Array columns") {
    sql("""CREATE TABLE tbl (id INT, tags ARRAY<STRING>, scores ARRAY<INT>) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, array('a','b','c'), array(10,20,30))")
    sql("INSERT INTO tbl VALUES (2, array('x'), array(99))")
    sql("INSERT INTO tbl VALUES (3, array(), array())")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("dsReadBadProtocol", "Error: unsupported reader version", "error") {
    sql("""CREATE TABLE tbl (id INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1),(2),(3)")
    val t = registerTable("tbl")
    // Bump minReaderVersion to an unsupported value
    mutateTable(t) { dir =>
      import scala.collection.JavaConverters._
      val mapper = new com.fasterxml.jackson.databind.ObjectMapper()
      val commitFile = dir.resolve("_delta_log/00000000000000000000.json")
      val lines = java.nio.file.Files.readAllLines(commitFile).asScala
      val newLines = lines.map { line =>
        if (line.contains("\"protocol\"")) {
          val node = mapper.readTree(line)
          val proto = node.get("protocol").asInstanceOf[com.fasterxml.jackson.databind.node.ObjectNode]
          proto.put("minReaderVersion", 99)
          mapper.writeValueAsString(node)
        } else line
      }
      java.nio.file.Files.write(commitFile, newLines.asJava)
    }
    snapshot(t)
  }

  test("dsReadBasic", "Basic read") {
    sql("""CREATE TABLE tbl (value INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(1, 11)")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("dsReadBetweenPredicate", "Read with BETWEEN predicate") {
    sql("""CREATE TABLE tbl (value INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(1, 21)")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "value BETWEEN 5 AND 15")
    snapshot(t)
  }

  test("dsReadBinaryType", "Binary type") {
    sql("""CREATE TABLE tbl (id INT, data BINARY) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, X'48454C4C4F')")
    sql("INSERT INTO tbl VALUES (2, X'574F524C44')")
    sql("INSERT INTO tbl VALUES (3, X'')")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("dsReadBooleanFilter", "Boolean filter") {
    sql("""CREATE TABLE tbl (id INT, active BOOLEAN) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, true)")
    sql("INSERT INTO tbl VALUES (2, false)")
    sql("INSERT INTO tbl VALUES (3, true)")
    sql("INSERT INTO tbl VALUES (4, false)")
    sql("INSERT INTO tbl VALUES (5, true)")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "active = true")
    read(t, predicate = "active = false")
    snapshot(t)
  }

  test("dsReadCaseSensitive", "Case-sensitive column names") {
    sql("""CREATE TABLE tbl (Id INT, FirstName STRING, lastName STRING, AGE INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, 'Alice', 'Smith', 30)")
    sql("INSERT INTO tbl VALUES (2, 'Bob', 'Jones', 25)")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("dsReadDateType", "Date type with filter") {
    sql("""CREATE TABLE tbl (id INT, event_date DATE) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, DATE'2024-01-01')")
    sql("INSERT INTO tbl VALUES (2, DATE'2024-06-15')")
    sql("INSERT INTO tbl VALUES (3, DATE'2024-12-31')")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "event_date > DATE'2024-06-01'")
    snapshot(t)
  }

  test("dsReadDecimalType", "Decimal type") {
    sql("""CREATE TABLE tbl (id INT, price DECIMAL(10,2), ratio DECIMAL(18,8)) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, 99.99, 0.12345678)")
    sql("INSERT INTO tbl VALUES (2, 1234.56, 3.14159265)")
    sql("INSERT INTO tbl VALUES (3, 0.01, 0.00000001)")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "price > 100")
    snapshot(t)
  }

  test("dsReadDeleteThenRead", "Read after DELETE") {
    sql("""CREATE TABLE tbl (value INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(1, 11)")
    sql("DELETE FROM tbl WHERE value <= 3")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("dsReadDoubleType", "Double type") {
    sql("""CREATE TABLE tbl (id INT, dval DOUBLE) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, 3.14159265358979), (2, -0.001), (3, 1.7976931348623157E308), (4, 0.0)")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("dsReadEmptyPartition", "Empty partition filter result") {
    sql("""CREATE TABLE tbl (id BIGINT, part INT) USING delta
      PARTITIONED BY (part)
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id, CAST(id % 3 AS INT) FROM range(50)")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "part = 99")
    snapshot(t)
  }

  test("dsReadEmptyString", "Error: no delta table at empty subdir", "error") {
    sql("""CREATE TABLE tbl (id INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1)")
    val t = registerTable("tbl")
    // Delete the delta log to simulate missing table
    mutateTable(t) { dir =>
      val logDir = dir.resolve("_delta_log")
      if (java.nio.file.Files.exists(logDir)) {
        java.nio.file.Files.walk(logDir).sorted(java.util.Comparator.reverseOrder())
          .forEach(p => java.nio.file.Files.deleteIfExists(p))
      }
    }
    snapshot(t)
  }

  test("dsReadEmptyTable", "Error: no delta table", "error") {
    sql("""CREATE TABLE tbl (id INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1)")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
      val logDir = dir.resolve("_delta_log")
      if (java.nio.file.Files.exists(logDir)) {
        java.nio.file.Files.walk(logDir).sorted(java.util.Comparator.reverseOrder())
          .forEach(p => java.nio.file.Files.deleteIfExists(p))
      }
    }
    snapshot(t)
  }

  test("dsReadFloatType", "Float type") {
    sql("""CREATE TABLE tbl (id INT, fval FLOAT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, CAST(1.5 AS FLOAT)), (2, CAST(-2.5 AS FLOAT)), (3, CAST(0.0 AS FLOAT)), (4, CAST(3.4028235E38 AS FLOAT))")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("dsReadInPredicate", "Read with IN predicate") {
    sql("""CREATE TABLE tbl (value INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(1, 21)")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "value IN (1, 5, 10, 15, 20)")
    snapshot(t)
  }

  test("dsReadIsNotNullPredicate", "IS NOT NULL predicate") {
    sql("""CREATE TABLE tbl (id INT, label STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(NULL,'b'),(3,'c'),(NULL,'d'),(5,'e')")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "id IS NOT NULL")
    snapshot(t)
  }

  test("dsReadIsNullPredicate", "IS NULL predicate") {
    sql("""CREATE TABLE tbl (id INT, label STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(NULL,'b'),(3,'c'),(NULL,'d'),(5,'e')")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "id IS NULL")
    snapshot(t)
  }

  test("dsReadLargeSchema", "25 columns") {
    val colDefs = (1 to 24).map(i => s"col_$i BIGINT").mkString(", ")
    sql(s"""CREATE TABLE tbl (id BIGINT, $colDefs) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    val colExprs = (1 to 24).map(i => s"id * $i AS col_$i").mkString(", ")
    sql(s"INSERT INTO tbl SELECT id, $colExprs FROM range(5)")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("dsReadLikePredicate", "LIKE predicate") {
    sql("""CREATE TABLE tbl (id INT, word STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'alpha'),(2,'beta'),(3,'alphabet'),(4,'gamma'),(5,'alpine')")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "word LIKE 'alp%'")
    snapshot(t)
  }

  test("dsReadLongType", "Long type") {
    sql("""CREATE TABLE tbl (lval BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1),(9223372036854775807),(-9223372036854775808),(0),(9876543210)")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("dsReadMapColumn", "Map columns") {
    sql("""CREATE TABLE tbl (id INT, props MAP<STRING, STRING>) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, map('color','red','size','large'))")
    sql("INSERT INTO tbl VALUES (2, map('color','blue'))")
    sql("INSERT INTO tbl VALUES (3, map())")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("dsReadMergeThenRead", "Read after MERGE") {
    sql("""CREATE TABLE target (id INT, val STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO target VALUES (1,'a'),(2,'b'),(3,'c')")
    sql("""CREATE TABLE src (id INT, val STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO src VALUES (2,'updated'),(4,'new')")
    sql("""MERGE INTO target t USING src s ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET val = s.val
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerTable("target")
    read(t)
    snapshot(t)
  }

  test("dsReadMultiPartition", "Multiple partition columns") {
    sql("""CREATE TABLE tbl (id INT, country STRING, city STRING, amount INT) USING delta
      PARTITIONED BY (country, city)
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'US','NY',100),(2,'US','CA',200),(3,'UK','LON',150),(4,'UK','MAN',120),(5,'US','NY',300)")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "country = 'US'")
    read(t, predicate = "country = 'US' AND city = 'NY'")
    snapshot(t)
  }

  test("dsReadMultipleAppends", "Multiple appends") {
    sql("""CREATE TABLE tbl (id INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    (1 to 7).foreach { i =>
      sql(s"INSERT INTO tbl VALUES ($i)")
    }
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("dsReadMultipleTypes", "Multiple data types") {
    sql("""CREATE TABLE tbl (
      id INT, name STRING, score DOUBLE, active BOOLEAN,
      created DATE, updated TIMESTAMP
    ) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'alice',95.5,true,DATE'2024-01-01',TIMESTAMP'2024-01-01 10:00:00')")
    sql("INSERT INTO tbl VALUES (2,'bob',82.3,false,DATE'2024-02-15',TIMESTAMP'2024-02-15 14:30:00')")
    sql("INSERT INTO tbl VALUES (3,'charlie',91.0,true,DATE'2024-03-20',TIMESTAMP'2024-03-20 08:15:00')")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("dsReadNestedStruct", "Nested struct columns") {
    sql("""CREATE TABLE tbl (
      id INT, info STRUCT<name: STRING, age: INT, address: STRUCT<city: STRING, zip: STRING>>
    ) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, named_struct('name','alice','age',30,'address',named_struct('city','NYC','zip','10001')))")
    sql("INSERT INTO tbl VALUES (2, named_struct('name','bob','age',25,'address',named_struct('city','LA','zip','90001')))")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("dsReadNotEqualPredicate", "Not equal predicate") {
    sql("""CREATE TABLE tbl (id INT, cat STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c'),(4,'a'),(5,'d')")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "cat != 'a'")
    snapshot(t)
  }

  test("dsReadNullValues", "Null values across types") {
    sql("""CREATE TABLE tbl (id INT, name STRING, score DOUBLE, active BOOLEAN) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'alice',95.5,true)")
    sql("INSERT INTO tbl VALUES (2,null,null,null)")
    sql("INSERT INTO tbl VALUES (null,'charlie',88.0,false)")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "name IS NOT NULL")
    snapshot(t)
  }

  test("dsReadOrPredicate", "OR predicate") {
    sql("""CREATE TABLE tbl (id INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(1, 11)")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "id = 1 OR id = 5 OR id = 10")
    snapshot(t)
  }

  test("dsReadOverwrite", "Read after overwrite") {
    sql("""CREATE TABLE tbl (value INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(1, 11)")
    sql("INSERT OVERWRITE tbl SELECT id FROM range(100, 106)")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("dsReadPartitioned", "Partitioned read with filter") {
    sql("""CREATE TABLE tbl (id BIGINT, part INT) USING delta
      PARTITIONED BY (part)
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id, CAST(id % 5 AS INT) FROM range(100)")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "part = 0")
    read(t, predicate = "part = 3")
    snapshot(t)
  }

  test("dsReadPathWithSpaces", "Error: path with spaces", "error") {
    sql("""CREATE TABLE tbl (id INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1)")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
      val logDir = dir.resolve("_delta_log")
      if (java.nio.file.Files.exists(logDir)) {
        java.nio.file.Files.walk(logDir).sorted(java.util.Comparator.reverseOrder())
          .forEach(p => java.nio.file.Files.deleteIfExists(p))
      }
    }
    snapshot(t)
  }

  test("dsReadSaveMode", "Save mode Append") {
    sql("""CREATE TABLE tbl (id INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1),(2),(3)")
    sql("INSERT INTO tbl VALUES (4),(5)")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("dsReadSaveModeErrorIfExists", "Save mode ErrorIfExists") {
    sql("""CREATE TABLE tbl (id INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1),(2),(3)")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("dsReadSaveModeIgnore", "Save mode Ignore") {
    sql("""CREATE TABLE tbl (id INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1),(2),(3)")
    // Second write is Ignore mode - table already exists, so data is NOT written
    // The result is still just the original 3 rows
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("dsReadSaveModeOverwrite", "Save mode Overwrite") {
    sql("""CREATE TABLE tbl (id INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1),(2),(3)")
    sql("INSERT OVERWRITE tbl VALUES (10),(20)")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("dsReadSchemaEvolution", "Schema evolution with ADD COLUMN") {
    sql("""CREATE TABLE tbl (id INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(1, 6)")
    sql("ALTER TABLE tbl ADD COLUMN name STRING")
    sql("INSERT INTO tbl VALUES (6,'alice')")
    sql("INSERT INTO tbl VALUES (7,'bob')")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "name IS NOT NULL")
    snapshot(t, version = 0)
    snapshot(t, version = 1)
    snapshot(t, version = 2)
    snapshot(t, version = 3)
    snapshot(t)
  }

  test("dsReadSelectColumns", "Column projection") {
    sql("""CREATE TABLE tbl (id INT, name STRING, score DOUBLE, category STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'alice',95.5,'A')")
    sql("INSERT INTO tbl VALUES (2,'bob',82.3,'B')")
    sql("INSERT INTO tbl VALUES (3,'charlie',91.0,'A')")
    val t = registerTable("tbl")
    read(t)
    read(t, columns = Seq("id", "name"))
    read(t, columns = Seq("score"))
    snapshot(t)
  }

  test("dsReadSnapshot", "Time travel snapshot read") {
    sql("""CREATE TABLE tbl (value INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(1, 11)")
    sql("INSERT INTO tbl SELECT id FROM range(11, 21)")
    val t = registerTable("tbl")
    read(t)
    read(t, version = 0)
    read(t, version = 1)
    snapshot(t)
  }

  test("dsReadSnapshotPartitioned", "Partitioned snapshot read") {
    sql("""CREATE TABLE tbl (region STRING, category INT, amount INT) USING delta
      PARTITIONED BY (region, category)
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES ('a',1,100),('b',2,200),('a',2,300)")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("dsReadSnapshotWithProperties", "Snapshot with table properties") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES (
        'delta.enableDeletionVectors' = 'true',
        'delta.logRetentionDuration' = 'interval 60 days',
        'delta.deletedFileRetentionDuration' = 'interval 30 days'
      )""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("dsReadSpecialChars", "Special chars in partition values") {
    sql("""CREATE TABLE tbl (id INT, category STRING) USING delta
      PARTITIONED BY (category)
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'hello world'),(2,'foo=bar'),(3,'a/b'),(4,'c%20d')")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "category = 'hello world'")
    snapshot(t)
  }

  test("dsReadStringFilter", "String equality filter") {
    sql("""CREATE TABLE tbl (id INT, fruit STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'apple'),(2,'banana'),(3,'cherry'),(4,'apple'),(5,'date')")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "fruit = 'apple'")
    snapshot(t)
  }

  test("dsReadTimestampType", "Timestamp type") {
    sql("""CREATE TABLE tbl (id INT, event_time TIMESTAMP) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, TIMESTAMP'2024-01-15 10:30:00')")
    sql("INSERT INTO tbl VALUES (2, TIMESTAMP'2024-06-20 22:59:59')")
    sql("INSERT INTO tbl VALUES (3, TIMESTAMP'2024-12-31 00:00:00')")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("dsReadUpdateThenRead", "Read after UPDATE") {
    sql("""CREATE TABLE tbl (value INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(1, 11)")
    sql("UPDATE tbl SET value = value + 100 WHERE value <= 5")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "value > 100")
    snapshot(t)
  }

  test("dsReadVersionNegative", "Error: negative version", "error") {
    sql("""CREATE TABLE tbl (value INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(1, 6)")
    val t = registerTable("tbl")
    snapshot(t, version = -1)
    snapshot(t)
  }

  test("dsReadVersionOutOfRange", "Error: version out of range", "error") {
    sql("""CREATE TABLE tbl (id INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(1, 6)")
    val t = registerTable("tbl")
    snapshot(t, version = 100)
  }

  test("dsReadVersionZero", "Time travel to version 0") {
    sql("""CREATE TABLE tbl (value INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(1, 6)")
    sql("INSERT INTO tbl SELECT id FROM range(6, 11)")
    sql("INSERT INTO tbl SELECT id FROM range(11, 16)")
    val t = registerTable("tbl")
    read(t)
    read(t, version = 0)
    read(t, version = 1)
    snapshot(t)
  }

  test("dsReadWithAlias", "Read with alias") {
    sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'alpha'),(2,'beta'),(3,'gamma')")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("dsReadWithLimit", "Read with limit") {
    sql("""CREATE TABLE tbl (id INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(1, 21)")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("dsReadWithOrderBy", "Read with order by") {
    sql("""CREATE TABLE tbl (id INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (5),(3),(1),(4),(2)")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("dsReadWithPredicate", "Predicate pushdown") {
    sql("""CREATE TABLE tbl (value INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(1, 21)")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "value > 5")
    snapshot(t)
  }

  // dse* workloads (Delta Suite Extended)

  test("dseReadAfterAlterTable", "Read after ALTER TABLE SET TBLPROPERTIES") {
    sql("""CREATE TABLE tbl (value INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(1, 11)")
    sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.appendOnly' = 'true')")
    sql("INSERT INTO tbl SELECT id FROM range(11, 16)")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("dseReadAfterSchemaChange", "Read after schema change") {
    sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'alpha'),(2,'beta')")
    sql("ALTER TABLE tbl ADD COLUMN score DOUBLE")
    sql("INSERT INTO tbl VALUES (3,'gamma',99.9)")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "score IS NOT NULL")
    read(t, predicate = "score IS NULL")
    snapshot(t)
  }

  test("dseReadLargeFile", "Large file read") {
    sql("""CREATE TABLE tbl (id BIGINT, data STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id, 'payload' FROM range(10000)")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "id < 100")
    snapshot(t)
  }

  test("dseReadSmallFiles", "Many small files") {
    sql("""CREATE TABLE tbl (value INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    (1 to 10).foreach { i =>
      sql(s"INSERT INTO tbl VALUES ($i)")
    }
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("dseReadRepartitioned", "Repartitioned read") {
    sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(100)")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("dseReadWithColumnPruning", "Column pruning") {
    sql("""CREATE TABLE tbl (id INT, name STRING, score DOUBLE, active BOOLEAN) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'alpha',10.0,true),(2,'beta',20.0,false),(3,'gamma',30.0,true)")
    val t = registerTable("tbl")
    read(t)
    read(t, columns = Seq("id", "name"))
    read(t, columns = Seq("score"))
    snapshot(t)
  }

  test("dseReadWithStats", "Read with stats-based filtering") {
    sql("""CREATE TABLE tbl (id INT, batch STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    // 5 separate appends so each batch lands in its own file (for stats skipping)
    sql("INSERT INTO tbl VALUES (1,'batch0'),(2,'batch0'),(3,'batch0')")
    sql("INSERT INTO tbl VALUES (101,'batch1'),(102,'batch1'),(103,'batch1')")
    sql("INSERT INTO tbl VALUES (201,'batch2'),(202,'batch2'),(203,'batch2')")
    sql("INSERT INTO tbl VALUES (301,'batch3'),(302,'batch3'),(303,'batch3')")
    sql("INSERT INTO tbl VALUES (401,'batch4'),(402,'batch4'),(403,'batch4')")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "id = 1")
    read(t, predicate = "id >= 201 AND id <= 203")
    snapshot(t)
  }

  test("dseSnapshotVersion", "Snapshot version read") {
    sql("""CREATE TABLE tbl (value INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(1, 6)")
    sql("INSERT INTO tbl SELECT id FROM range(6, 11)")
    sql("INSERT INTO tbl SELECT id FROM range(11, 16)")
    val t = registerTable("tbl")
    read(t)
    read(t, version = 0)
    read(t, version = 1)
    read(t, version = 2)
    snapshot(t)
  }


  test("dsReadAfterVacuum", "Read after VACUUM") {
    sql("""CREATE TABLE tbl (value BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id + 1 FROM range(10)")
    sql("INSERT OVERWRITE tbl SELECT id + 11 FROM range(10)")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
      val logDir = dir.resolve("_delta_log")
      val v1 = new String(java.nio.file.Files.readAllBytes(logDir.resolve("00000000000000000001.json")))
      val removePattern = """"path":"([^"]+)""".r
      removePattern.findAllMatchIn(v1).foreach { m =>
        val f = dir.resolve(m.group(1))
        if (java.nio.file.Files.exists(f)) java.nio.file.Files.delete(f)
      }
      val ts = System.currentTimeMillis()
      java.nio.file.Files.write(logDir.resolve("00000000000000000002.json"),
        s"""{"commitInfo":{"timestamp":${ts},"operation":"VACUUM START","operationParameters":{"retentionCheckEnabled":false,"defaultRetentionMillis":604800000,"specifiedRetentionMillis":0},"isBlindAppend":true}}""".getBytes)
      java.nio.file.Files.write(logDir.resolve("00000000000000000003.json"),
        s"""{"commitInfo":{"timestamp":${ts+1},"operation":"VACUUM END","operationParameters":{"status":"COMPLETED"},"isBlindAppend":true}}""".getBytes)
    }
    read(t)
    snapshot(t)
  }

  test("dsReadByteShortType", "Read table with byte and short integer types", "types") {
    sql("""CREATE TABLE tbl (bval TINYINT, sval SMALLINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, 100), (-128, -32768), (127, 32767), (0, 0)")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("dsReadCdcEnabled", "Read base table with CDC enabled", "cdc") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true',
        'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')")
    sql("UPDATE tbl SET value = 'alpha_v2' WHERE id = 1")
    sql("DELETE FROM tbl WHERE id = 3")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "id = 1")
    read(t, predicate = "id = 3")
    snapshot(t)
  }

  test("dsReadColumnReorder", "Read table where columns are in different order", "schema") {
    sql("""CREATE TABLE tbl (id INT, name STRING, score DOUBLE) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, 'alpha', 10.0), (2, 'beta', 20.0), (3, 'gamma', 30.0)")
    val t = registerTable("tbl")
    read(t)
    read(t, columns = Seq("score", "id", "name"))
    snapshot(t)
  }

  test("dsReadCorruptCheckpoint", "Error reading table with corrupt checkpoint") {
    sql("""CREATE TABLE tbl (id INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true', 'delta.checkpointInterval' = '5')""")
    for (i <- 0 until 6) sql(s"INSERT INTO tbl VALUES ($i)")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
      import scala.collection.JavaConverters._
      java.nio.file.Files.list(dir.resolve("_delta_log")).iterator().asScala
        .filter(_.toString.endsWith(".checkpoint.parquet"))
        .foreach(f => java.nio.file.Files.write(f, Array[Byte](0, 1, 2, 3)))
      for (i <- 0 until 5) {
        val jf = dir.resolve("_delta_log/%020d.json".format(i))
        if (java.nio.file.Files.exists(jf)) java.nio.file.Files.delete(jf)
      }
    }
    read(t)
  }

  test("dsReadCorruptJson", "Error: corrupt JSON in commit file") {
    sql("""CREATE TABLE tbl (id INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1)")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
      java.nio.file.Files.write(dir.resolve("_delta_log/00000000000000000000.json"),
        "NOT VALID JSON{{{".getBytes)
    }
    read(t)
    snapshot(t)
  }

  test("dsReadDuplicateColumns", "Error: schema with duplicate column names", "error") {
    sql("""CREATE TABLE tbl (id INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1)")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
      val v0 = new String(java.nio.file.Files.readAllBytes(
        dir.resolve("_delta_log/00000000000000000000.json")), "UTF-8")
      val mdLine = v0.split("\n").find(_.contains("\"metaData\"")).getOrElse("")
      val dupSchema = mdLine.replace(
        """{"name":"id","type":"integer""",
        """{"name":"id","type":"integer","nullable":true,"metadata":{}},{"name":"id","type":"integer""")
      val ci = s"""{"commitInfo":{"timestamp":${System.currentTimeMillis()},"operation":"SET TBLPROPERTIES","operationParameters":{},"isBlindAppend":true}}"""
      java.nio.file.Files.write(dir.resolve("_delta_log/00000000000000000001.json"),
        (dupSchema + "\n" + ci + "\n").getBytes)
    }
    read(t)
  }

  test("dsReadEmptyDataFrame", "Read after writing empty DataFrame", "emptyTable") {
    sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT CAST(id AS INT), CAST(id AS STRING) FROM range(0)")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("dsReadInvalidColumnName", "Error: predicate on non-existent column", "error") {
    sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    val t = registerTable("tbl")
    read(t, predicate = "nonExistentCol = 1")
    snapshot(t)
  }

  test("dsReadMissingCommitFile", "Error: _delta_log exists but commit file missing") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    sql("INSERT INTO tbl VALUES (1)")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
      import scala.collection.JavaConverters._
      java.nio.file.Files.list(dir.resolve("_delta_log")).iterator().asScala
        .filter(_.toString.endsWith(".json")).foreach(java.nio.file.Files.delete)
    }
    read(t)
  }

  test("dsReadMissingDeltaLog", "Error: path exists but _delta_log is missing") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    sql("INSERT INTO tbl VALUES (1)")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
      import scala.collection.JavaConverters._
      val logDir = dir.resolve("_delta_log")
      java.nio.file.Files.list(logDir).iterator().asScala.foreach(java.nio.file.Files.delete)
      java.nio.file.Files.delete(logDir)
    }
    read(t)
  }

  test("dsReadMixedCasePartition", "Read with mixed case partition column names", "partitioned") {
    sql("""CREATE TABLE tbl (PartCol STRING, Value INT) USING delta
      PARTITIONED BY (PartCol)
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES ('x', 1), ('y', 2), ('x', 3)")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "PartCol = 'x'")
    snapshot(t)
  }

  test("dsReadModifyCheckpoint", "Error: table with corrupted checkpoint") {
    sql("""CREATE TABLE tbl (value BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true', 'delta.checkpointInterval' = '3')""")
    for (i <- 0 until 10) sql(s"INSERT INTO tbl SELECT id FROM range(${i*10}, ${(i+1)*10})")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
      import scala.collection.JavaConverters._
      java.nio.file.Files.list(dir.resolve("_delta_log")).iterator().asScala
        .filter(_.toString.endsWith(".checkpoint.parquet"))
        .foreach(f => java.nio.file.Files.write(f, Array[Byte](0, 1, 2, 3)))
    }
    read(t)
    snapshot(t)
  }

  test("dsReadNonExistentVersion", "Error on non-existent version", "error") {
    sql("""CREATE TABLE tbl (value BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    val t = registerTable("tbl")
    read(t, version = 99)
  }

  test("dsReadRenameColumn", "Read after column mapping rename", "columnMapping") {
    sql("""CREATE TABLE tbl (id INT, old_name STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true',
        'delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    sql("INSERT INTO tbl VALUES (1, 'alice')")
    sql("INSERT INTO tbl VALUES (2, 'bob')")
    sql("ALTER TABLE tbl RENAME COLUMN old_name TO new_name")
    sql("INSERT INTO tbl VALUES (3, 'charlie')")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("dsReadReplaceWhere", "Read after replaceWhere partition overwrite", "partitioned") {
    sql("""CREATE TABLE tbl (part STRING, value INT) USING delta
      PARTITIONED BY (part)
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES ('a', 1), ('a', 2), ('b', 3), ('b', 4)")
    sql("INSERT OVERWRITE tbl PARTITION (part='a') VALUES ('a', 10), ('a', 20)")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "part = 'a'")
    snapshot(t)
  }

  test("dsReadSingleRow", "Read table with single row", "basic") {
    sql("""CREATE TABLE tbl (value BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (42)")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("dsReadStringWithSpecialChars", "Read string values with special characters", "types") {
    sql("""CREATE TABLE tbl (id INT, text STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("""INSERT INTO tbl VALUES
      (1, 'hello\nworld'), (2, 'tab\there'),
      (3, 'with"quotes'), (4, 'unicode\u00e9\u00f1'), (5, 'normal')""")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("dseReadAfterTruncate", "Read after TRUNCATE (DELETE all)") {
    sql("""CREATE TABLE tbl (value BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    sql("DELETE FROM tbl WHERE true")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("dseReadCoalesced", "Read after coalesce write") {
    sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(100)")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("dseReadInvalidTableProperty", "Error: unsupported protocol version") {
    sql("""CREATE TABLE tbl (value BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
      val ts = System.currentTimeMillis()
      val v1Meta = """{"metaData":{"id":"test","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"value\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{"delta.enableDeletionVectors":"true"}}}"""
      val v1Proto = """{"protocol":{"minReaderVersion":99,"minWriterVersion":99}}"""
      val ci = s"""{"commitInfo":{"timestamp":${ts},"operation":"SET TBLPROPERTIES","operationParameters":{},"isBlindAppend":true}}"""
      java.nio.file.Files.write(dir.resolve("_delta_log/00000000000000000001.json"),
        (v1Meta + "\n" + v1Proto + "\n" + ci + "\n").getBytes)
    }
    read(t)
  }

  test("dseReadNonDeltaPath", "Error reading non-delta path as delta") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    sql("INSERT INTO tbl VALUES (1)")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
      import scala.collection.JavaConverters._
      val logDir = dir.resolve("_delta_log")
      java.nio.file.Files.list(logDir).iterator().asScala.foreach(java.nio.file.Files.delete)
      java.nio.file.Files.delete(logDir)
    }
    read(t)
    snapshot(t)
  }

  test("dseReadSortedData", "Read table written with sorted data") {
    sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT 99 - id FROM range(100)")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "id >= 90")
    snapshot(t)
  }

}
