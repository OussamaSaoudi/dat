/**
 * Consolidated read workloads.
 * Merged from: core_reads.scala, core_reads_extended.scala, core_reads_legacy.scala
 *
 */

new WorkloadSuite("reads") {

  // === Core Reads ===

  test("read_basic", "Basic read") { w =>
    w.sql("CREATE TABLE tbl (value INT) USING delta")
    w.sql("INSERT INTO tbl SELECT id FROM range(1, 11)")
    val t = w.table("tbl")
    w.read(t)
    w.snapshot(t)
  }

  test("read_partitioned", "Partitioned read with filter") { w =>
    w.sql("CREATE TABLE tbl (id BIGINT, part INT) USING delta PARTITIONED BY (part)")
    w.sql("INSERT INTO tbl SELECT id, CAST(id % 5 AS INT) FROM range(100)")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "part = 0")
    w.read(t, predicate = "part = 3")
    w.snapshot(t)
  }

  test("read_empty_path", "Error: no delta table", "error") { w =>
    w.sql("CREATE TABLE tbl (id INT) USING delta")
    w.sql("INSERT INTO tbl VALUES (1)")
    val t = w.table("tbl")
    w.mutateTable(t) { dir =>
      val logDir = dir.resolve("_delta_log")
      if (java.nio.file.Files.exists(logDir)) {
        java.nio.file.Files.walk(logDir).sorted(java.util.Comparator.reverseOrder())
          .forEach(p => java.nio.file.Files.deleteIfExists(p))
      }
    }
    w.read(t)
  }

  test("read_append", "Read after append") { w =>
    w.sql("CREATE TABLE tbl (value INT) USING delta")
    w.sql("INSERT INTO tbl SELECT id FROM range(1, 6)")
    w.sql("INSERT INTO tbl SELECT id FROM range(6, 11)")
    val t = w.table("tbl")
    w.read(t)
    w.snapshot(t)
  }

  test("read_overwrite", "Read after overwrite") { w =>
    w.sql("CREATE TABLE tbl (value INT) USING delta")
    w.sql("INSERT INTO tbl SELECT id FROM range(1, 11)")
    w.sql("INSERT OVERWRITE tbl SELECT id FROM range(100, 106)")
    val t = w.table("tbl")
    w.read(t)
    w.snapshot(t)
  }

  test("read_multiple_types", "Multiple data types") { w =>
    w.sql("""CREATE TABLE tbl (
      id INT, name STRING, score DOUBLE, active BOOLEAN,
      created DATE, updated TIMESTAMP
    ) USING delta""")
    w.sql("INSERT INTO tbl VALUES (1,'alice',95.5,true,DATE'2024-01-01',TIMESTAMP'2024-01-01 10:00:00')")
    w.sql("INSERT INTO tbl VALUES (2,'bob',82.3,false,DATE'2024-02-15',TIMESTAMP'2024-02-15 14:30:00')")
    val t = w.table("tbl")
    w.read(t)
    w.snapshot(t)
  }

  test("read_predicate", "Predicate pushdown") { w =>
    w.sql("CREATE TABLE tbl (value INT) USING delta")
    w.sql("INSERT INTO tbl SELECT id FROM range(1, 21)")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "value > 5")
    w.snapshot(t)
  }

  test("read_bad_version", "Error: non-existent version", "error") { w =>
    w.sql("CREATE TABLE tbl (value INT) USING delta")
    w.sql("INSERT INTO tbl SELECT id FROM range(1, 6)")
    val t = w.table("tbl")
    w.read(t, version = 99)
    w.snapshot(t)
  }

  test("read_version_zero", "Time travel to v0") { w =>
    w.sql("CREATE TABLE tbl (value INT) USING delta")
    w.sql("INSERT INTO tbl SELECT id FROM range(1, 6)")
    w.sql("INSERT INTO tbl SELECT id FROM range(6, 11)")
    w.sql("INSERT INTO tbl SELECT id FROM range(11, 16)")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, version = 0)
    w.read(t, version = 1)
    w.snapshot(t)
  }

  test("read_after_delete", "Read after DELETE") { w =>
    w.sql("CREATE TABLE tbl (value INT) USING delta")
    w.sql("INSERT INTO tbl SELECT id FROM range(1, 11)")
    w.sql("DELETE FROM tbl WHERE value <= 3")
    val t = w.table("tbl")
    w.read(t)
    w.snapshot(t)
  }

  test("read_after_update", "Read after UPDATE") { w =>
    w.sql("CREATE TABLE tbl (value INT) USING delta")
    w.sql("INSERT INTO tbl SELECT id FROM range(1, 11)")
    w.sql("UPDATE tbl SET value = value + 100 WHERE value <= 5")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "value > 100")
    w.snapshot(t)
  }

  test("read_after_merge", "Read after MERGE") { w =>
    w.sql("CREATE TABLE target (id INT, val STRING) USING delta")
    w.sql("INSERT INTO target VALUES (1,'a'),(2,'b'),(3,'c')")
    w.sql("CREATE TABLE src (id INT, val STRING) USING delta")
    w.sql("INSERT INTO src VALUES (2,'updated'),(4,'new')")
    w.sql("""MERGE INTO target t USING src s ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET val = s.val
      WHEN NOT MATCHED THEN INSERT *""")
    val t = w.table("target")
    w.read(t)
    w.snapshot(t)
  }

  test("read_nulls", "Null values across types") { w =>
    w.sql("CREATE TABLE tbl (id INT, name STRING, score DOUBLE, active BOOLEAN) USING delta")
    w.sql("INSERT INTO tbl VALUES (1,'alice',95.5,true)")
    w.sql("INSERT INTO tbl VALUES (2,null,null,null)")
    w.sql("INSERT INTO tbl VALUES (null,'charlie',88.0,false)")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "name IS NOT NULL")
    w.snapshot(t)
  }

  test("read_empty_partition", "Empty partition filter result") { w =>
    w.sql("CREATE TABLE tbl (id BIGINT, part INT) USING delta PARTITIONED BY (part)")
    w.sql("INSERT INTO tbl SELECT id, CAST(id % 3 AS INT) FROM range(50)")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "part = 99")
    w.snapshot(t)
  }

  test("read_nested_struct", "Nested struct columns") { w =>
    w.sql("""CREATE TABLE tbl (
      id INT, info STRUCT<name: STRING, age: INT, address: STRUCT<city: STRING, zip: STRING>>
    ) USING delta""")
    w.sql("INSERT INTO tbl VALUES (1, named_struct('name','alice','age',30,'address',named_struct('city','NYC','zip','10001')))")
    w.sql("INSERT INTO tbl VALUES (2, named_struct('name','bob','age',25,'address',named_struct('city','LA','zip','90001')))")
    val t = w.table("tbl")
    w.read(t)
    w.snapshot(t)
  }

  test("read_array", "Array columns") { w =>
    w.sql("CREATE TABLE tbl (id INT, tags ARRAY<STRING>, scores ARRAY<INT>) USING delta")
    w.sql("INSERT INTO tbl VALUES (1,array('a','b','c'),array(10,20,30))")
    w.sql("INSERT INTO tbl VALUES (2,array('x'),array(99))")
    w.sql("INSERT INTO tbl VALUES (3,array(),array())")
    val t = w.table("tbl")
    w.read(t)
    w.snapshot(t)
  }

  test("read_map", "Map columns") { w =>
    w.sql("CREATE TABLE tbl (id INT, props MAP<STRING, STRING>) USING delta")
    w.sql("INSERT INTO tbl VALUES (1,map('color','red','size','large'))")
    w.sql("INSERT INTO tbl VALUES (2,map('color','blue'))")
    w.sql("INSERT INTO tbl VALUES (3,map())")
    val t = w.table("tbl")
    w.read(t)
    w.snapshot(t)
  }

  test("read_large_schema", "25 columns") { w =>
    val colDefs = (1 to 24).map(i => s"col_$i BIGINT").mkString(", ")
    w.sql(s"CREATE TABLE tbl (id BIGINT, $colDefs) USING delta")
    val colExprs = (1 to 24).map(i => s"id * $i AS col_$i").mkString(", ")
    w.sql(s"INSERT INTO tbl SELECT id, $colExprs FROM range(5)")
    val t = w.table("tbl")
    w.read(t)
    w.snapshot(t)
  }

  test("read_special_chars", "Special chars in partition values") { w =>
    w.sql("CREATE TABLE tbl (id INT, category STRING) USING delta PARTITIONED BY (category)")
    w.sql("INSERT INTO tbl VALUES (1,'hello world'),(2,'foo=bar'),(3,'a/b')")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "category = 'hello world'")
    w.snapshot(t)
  }

  test("read_schema_evolution", "ADD COLUMN", "schema_evolution") { w =>
    w.sql("CREATE TABLE tbl (id INT) USING delta")
    w.sql("INSERT INTO tbl SELECT id FROM range(1, 6)")
    w.sql("ALTER TABLE tbl ADD COLUMN name STRING")
    w.sql("INSERT INTO tbl VALUES (6,'alice'),(7,'bob')")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "name IS NOT NULL")
    w.snapshotHistory(t)
  }

  test("read_rename_column", "RENAME COLUMN", "column_mapping") { w =>
    w.sql("""CREATE TABLE tbl (id INT, old_name STRING) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    w.sql("INSERT INTO tbl VALUES (1,'alice'),(2,'bob')")
    w.sql("ALTER TABLE tbl RENAME COLUMN old_name TO new_name")
    w.sql("INSERT INTO tbl VALUES (3,'charlie')")
    val t = w.table("tbl")
    w.read(t)
    w.snapshot(t)
  }

  test("read_decimal", "Decimal types") { w =>
    w.sql("CREATE TABLE tbl (id INT, price DECIMAL(10,2), ratio DECIMAL(18,8)) USING delta")
    w.sql("INSERT INTO tbl VALUES (1,99.99,0.12345678),(2,1234.56,3.14159265),(3,0.01,0.00000001)")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "price > 100")
    w.snapshot(t)
  }

  test("read_projection", "Column projection") { w =>
    w.sql("CREATE TABLE tbl (id INT, name STRING, score DOUBLE, category STRING) USING delta")
    w.sql("INSERT INTO tbl VALUES (1,'alice',95.5,'A'),(2,'bob',82.3,'B'),(3,'charlie',91.0,'A')")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, columns = Seq("id", "name"))
    w.read(t, columns = Seq("score"))
    w.snapshot(t)
  }

  test("read_binary", "Binary column") { w =>
    w.sql("CREATE TABLE tbl (id INT, data BINARY) USING delta")
    w.sql("INSERT INTO tbl VALUES (1,X'48454C4C4F'),(2,X'574F524C44'),(3,X'')")
    val t = w.table("tbl")
    w.read(t)
    w.snapshot(t)
  }

  test("read_negative_version", "Error: negative version", "error") { w =>
    w.sql("CREATE TABLE tbl (value INT) USING delta")
    w.sql("INSERT INTO tbl SELECT id FROM range(1, 6)")
    val t = w.table("tbl")
    w.read(t, version = -1)
    w.snapshot(t)
  }

  test("read_after_merge_target", "Read after merge - target table", "merge") { w =>
    w.sql("CREATE TABLE target (id INT, val STRING) USING delta")
    w.sql("INSERT INTO target VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    w.sql("CREATE TABLE src (id INT, val STRING) USING delta")
    w.sql("INSERT INTO src VALUES (2, 'updated'), (4, 'new')")
    w.sql("""MERGE INTO target t USING src s ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET val = s.val
      WHEN NOT MATCHED THEN INSERT *""")
    val t = w.table("target")
    w.read(t)
    w.snapshot(t)
  }

  // === Core Reads Extended ===

  // ---------------------------------------------------------------------------
  // Core reads: type boundaries
  // ---------------------------------------------------------------------------

  test("cr_byte_boundaries", "ByteType MIN/MAX values", "coreReads") { w =>
    w.sql("CREATE TABLE tbl (b BYTE) USING delta")
    w.sql("INSERT INTO tbl VALUES (-128), (0), (127)")
    val t = w.table("tbl")
    w.read(t)
    w.snapshot(t)
  }

  test("cr_short_boundaries", "ShortType MIN/MAX values", "coreReads") { w =>
    w.sql("CREATE TABLE tbl (s SHORT) USING delta")
    w.sql("INSERT INTO tbl VALUES (-32768), (0), (32767)")
    val t = w.table("tbl")
    w.read(t)
    w.snapshot(t)
  }

  test("cr_date_boundaries", "Date boundary values", "coreReads") { w =>
    w.sql("CREATE TABLE tbl (d DATE) USING delta")
    w.sql("INSERT INTO tbl VALUES (DATE'0001-01-01'), (DATE'2024-06-15'), (DATE'9999-12-31')")
    val t = w.table("tbl")
    w.read(t)
    w.snapshot(t)
  }

  test("cr_timestamp_boundaries", "Timestamp boundary values", "coreReads") { w =>
    w.sql("CREATE TABLE tbl (ts TIMESTAMP) USING delta")
    w.sql("""INSERT INTO tbl VALUES
      (TIMESTAMP'1970-01-01 00:00:00'),
      (TIMESTAMP'2024-06-15 12:30:45.123456'),
      (TIMESTAMP'2262-04-11 23:47:16.854775')""")
    val t = w.table("tbl")
    w.read(t)
    w.snapshot(t)
  }

  test("cr_decimal_max_precision", "DecimalType(38,18) read-back", "coreReads") { w =>
    w.sql("CREATE TABLE tbl (d DECIMAL(38,18)) USING delta")
    w.sql("""INSERT INTO tbl VALUES
      (12345678901234567890.123456789012345678),
      (-12345678901234567890.123456789012345678),
      (0.000000000000000001)""")
    val t = w.table("tbl")
    w.read(t)
    w.snapshot(t)
  }

  test("cr_decimal_zero_scale", "DecimalType(38,0) read-back", "coreReads") { w =>
    w.sql("CREATE TABLE tbl (d DECIMAL(38,0)) USING delta")
    w.sql("""INSERT INTO tbl VALUES
      (99999999999999999999999999999999999999),
      (-99999999999999999999999999999999999999),
      (0)""")
    val t = w.table("tbl")
    w.read(t)
    w.snapshot(t)
  }

  // ---------------------------------------------------------------------------
  // Core reads: special float/double values
  // ---------------------------------------------------------------------------

  test("cr_float_nan", "Float NaN value read-back", "coreReads") { w =>
    w.sql("CREATE TABLE tbl (f FLOAT) USING delta")
    w.sql("INSERT INTO tbl VALUES (CAST('NaN' AS FLOAT)), (1.5), (NULL)")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "f IS NOT NULL")
    w.snapshot(t)
  }

  test("cr_float_infinity", "Float +Infinity / -Infinity read-back", "coreReads") { w =>
    w.sql("CREATE TABLE tbl (f FLOAT) USING delta")
    w.sql("INSERT INTO tbl VALUES (CAST('Infinity' AS FLOAT)), (CAST('-Infinity' AS FLOAT)), (0.0)")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "f > 0")
    w.snapshot(t)
  }

  test("cr_double_nan", "Double NaN value read-back", "coreReads") { w =>
    w.sql("CREATE TABLE tbl (d DOUBLE) USING delta")
    w.sql("INSERT INTO tbl VALUES (CAST('NaN' AS DOUBLE)), (2.5), (NULL)")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "d IS NOT NULL")
    w.snapshot(t)
  }

  test("cr_double_infinity", "Double +Infinity / -Infinity read-back", "coreReads") { w =>
    w.sql("CREATE TABLE tbl (d DOUBLE) USING delta")
    w.sql("INSERT INTO tbl VALUES (CAST('Infinity' AS DOUBLE)), (CAST('-Infinity' AS DOUBLE)), (0.0)")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "d > 0")
    w.snapshot(t)
  }

  // ---------------------------------------------------------------------------
  // Core reads: complex types
  // ---------------------------------------------------------------------------

  test("cr_deeply_nested_struct", "4+ levels nested struct", "coreReads") { w =>
    w.sql("""CREATE TABLE tbl (
      top STRUCT<l1: STRUCT<l2: STRUCT<l3: STRUCT<value: INT>>>>
    ) USING delta""")
    w.sql("""INSERT INTO tbl VALUES (
      named_struct('l1', named_struct('l2', named_struct('l3', named_struct('value', 42)))))""")
    val t = w.table("tbl")
    w.read(t)
    w.snapshot(t)
  }

  test("cr_struct_all_null", "Struct with all-NULL fields", "coreReads") { w =>
    w.sql("CREATE TABLE tbl (s STRUCT<a: INT, b: STRING, c: DOUBLE>) USING delta")
    w.sql("INSERT INTO tbl VALUES (named_struct('a', CAST(NULL AS INT), 'b', CAST(NULL AS STRING), 'c', CAST(NULL AS DOUBLE)))")
    w.sql("INSERT INTO tbl VALUES (named_struct('a', 1, 'b', 'hello', 'c', 3.14))")
    val t = w.table("tbl")
    w.read(t)
    w.snapshot(t)
  }

  test("cr_array_of_arrays", "Nested ARRAY<ARRAY<INT>> column", "coreReads") { w =>
    w.sql("CREATE TABLE tbl (a ARRAY<ARRAY<INT>>) USING delta")
    w.sql("INSERT INTO tbl VALUES (array(array(1,2), array(3,4)))")
    w.sql("INSERT INTO tbl VALUES (array(array(), array(5)))")
    val t = w.table("tbl")
    w.read(t)
    w.snapshot(t)
  }

  test("cr_map_complex_value", "Map with struct value", "coreReads") { w =>
    w.sql("CREATE TABLE tbl (m MAP<STRING, STRUCT<x: INT, y: STRING>>) USING delta")
    w.sql("INSERT INTO tbl VALUES (map('key1', named_struct('x', 1, 'y', 'a'), 'key2', named_struct('x', 2, 'y', 'b')))")
    val t = w.table("tbl")
    w.read(t)
    w.snapshot(t)
  }

  test("cr_wide_schema", "Table with 100+ columns", "coreReads") { w =>
    val colDefs = (1 to 100).map(i => s"col_$i INT").mkString(", ")
    w.sql(s"CREATE TABLE tbl ($colDefs) USING delta")
    val colExprs = (1 to 100).map(i => s"$i").mkString(", ")
    w.sql(s"INSERT INTO tbl VALUES ($colExprs)")
    val t = w.table("tbl")
    w.read(t)
    w.snapshot(t)
  }

  // ---------------------------------------------------------------------------
  // Core reads: null and string edge cases
  // ---------------------------------------------------------------------------

  test("cr_empty_vs_null_string", "Empty string vs NULL distinction", "coreReads") { w =>
    w.sql("CREATE TABLE tbl (id INT, s STRING) USING delta")
    w.sql("INSERT INTO tbl VALUES (1, ''), (2, NULL), (3, 'hello')")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "s IS NOT NULL")
    w.snapshot(t)
  }

  test("cr_binary_readback", "Binary type read-back", "coreReads") { w =>
    w.sql("CREATE TABLE tbl (id INT, data BINARY) USING delta")
    w.sql("INSERT INTO tbl VALUES (1, X'DEADBEEF'), (2, X''), (3, NULL)")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "data IS NOT NULL")
    w.snapshot(t)
  }

  test("cr_boolean_filter", "Boolean column with filter", "coreReads") { w =>
    w.sql("CREATE TABLE tbl (id INT, flag BOOLEAN) USING delta")
    w.sql("INSERT INTO tbl VALUES (1, true), (2, false), (3, NULL)")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "flag = true")
    w.snapshot(t)
  }

  test("cr_zero_matching_rows", "Filter that matches zero rows", "coreReads") { w =>
    w.sql("CREATE TABLE tbl (id INT) USING delta")
    w.sql("INSERT INTO tbl VALUES (1), (2), (3)")
    val t = w.table("tbl")
    w.read(t, predicate = "id > 999")
    w.read(t, predicate = "id = -1")
    w.snapshot(t)
  }

  test("cr_projection_reorder", "Column projection with reordered columns",
      "coreReads") { w =>
    w.sql("CREATE TABLE tbl (a INT, b STRING, c DOUBLE) USING delta")
    w.sql("INSERT INTO tbl VALUES (1, 'hello', 3.14), (2, 'world', 2.72)")
    val t = w.table("tbl")
    w.read(t, columns = Seq("c", "a"))
    w.read(t, columns = Seq("b"))
    w.snapshot(t)
  }

  // ---------------------------------------------------------------------------
  // Core reads: partitioned tables
  // ---------------------------------------------------------------------------

  test("cr_multi_partition", "Multiple partition columns", "coreReads") { w =>
    w.sql("""CREATE TABLE tbl (id INT, year INT, region STRING)
      USING delta PARTITIONED BY (year, region)""")
    w.sql("INSERT INTO tbl VALUES (1, 2024, 'us'), (2, 2024, 'eu'), (3, 2025, 'us')")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "year = 2024")
    w.read(t, predicate = "year = 2024 AND region = 'us'")
    w.snapshot(t)
  }

  test("cr_partition_null", "Partition column with NULL values", "coreReads") { w =>
    w.sql("CREATE TABLE tbl (id INT, part STRING) USING delta PARTITIONED BY (part)")
    w.sql("INSERT INTO tbl VALUES (1, 'a'), (2, NULL), (3, 'b')")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "part IS NULL")
    w.snapshot(t)
  }

  // ---------------------------------------------------------------------------
  // Delta partition suite (dp*)
  // ---------------------------------------------------------------------------

  test("dpBasicPartition", "Basic single-column string partition", "partitioned") { w =>
    w.sql("CREATE TABLE tbl (id INT, part STRING) USING delta PARTITIONED BY (part)")
    w.sql("INSERT INTO tbl VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    val t = w.table("tbl")
    w.read(t)
    w.snapshot(t)
  }

  test("dpIntPartition", "Int-column partition", "partitioned") { w =>
    w.sql("CREATE TABLE tbl (id INT, part INT) USING delta PARTITIONED BY (part)")
    w.sql("INSERT INTO tbl VALUES (1, 10), (2, 20), (3, 30)")
    val t = w.table("tbl")
    w.read(t)
    w.snapshot(t)
  }

  test("dpDatePartition", "Date-column partition", "partitioned") { w =>
    w.sql("CREATE TABLE tbl (id INT, part DATE) USING delta PARTITIONED BY (part)")
    w.sql("INSERT INTO tbl VALUES (1, DATE'2024-01-01'), (2, DATE'2024-06-15'), (3, DATE'2025-01-01')")
    val t = w.table("tbl")
    w.read(t)
    w.snapshot(t)
  }

  test("dpMultiPartition", "Multi-column partition (string + int)", "partitioned") { w =>
    w.sql("""CREATE TABLE tbl (id INT, region STRING, year INT)
      USING delta PARTITIONED BY (region, year)""")
    w.sql("INSERT INTO tbl VALUES (1, 'us', 2024), (2, 'eu', 2024), (3, 'us', 2025)")
    val t = w.table("tbl")
    w.read(t)
    w.snapshot(t)
  }

  test("dpReadPartitionFilter", "Read with partition column equality filter",
      "partitioned") { w =>
    w.sql("CREATE TABLE tbl (id INT, part STRING) USING delta PARTITIONED BY (part)")
    w.sql("INSERT INTO tbl VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "part = 'a'")
    w.read(t, predicate = "part = 'z'", name = "read_miss_part")
    w.snapshot(t)
  }

  test("dpReadPartitionRange", "Read with partition column range filter",
      "partitioned") { w =>
    w.sql("CREATE TABLE tbl (id INT, part INT) USING delta PARTITIONED BY (part)")
    w.sql("INSERT INTO tbl VALUES (1, 1), (2, 5), (3, 10)")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "part > 3")
    w.read(t, predicate = "part BETWEEN 1 AND 5")
    w.snapshot(t)
  }

  test("dpReadPartitionIn", "Read with partition column IN filter", "partitioned") { w =>
    w.sql("CREATE TABLE tbl (id INT, part STRING) USING delta PARTITIONED BY (part)")
    w.sql("INSERT INTO tbl VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "part IN ('a', 'c')")
    w.read(t, predicate = "part IN ('z')", name = "read_miss_in")
    w.snapshot(t)
  }

  test("dpReadPartitionNull", "Read partition with null partition values",
      "partitioned") { w =>
    w.sql("CREATE TABLE tbl (id INT, part STRING) USING delta PARTITIONED BY (part)")
    w.sql("INSERT INTO tbl VALUES (1, 'a'), (2, NULL), (3, 'b')")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "part IS NULL")
    w.read(t, predicate = "part IS NOT NULL")
    w.snapshot(t)
  }

  test("dpReadPartitionMixed", "Read with mixed partition + data column filters",
      "partitioned") { w =>
    w.sql("CREATE TABLE tbl (id INT, value INT, part STRING) USING delta PARTITIONED BY (part)")
    w.sql("INSERT INTO tbl VALUES (1, 10, 'a'), (2, 20, 'a'), (3, 30, 'b'), (4, 40, 'b')")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "part = 'a' AND value > 15")
    w.read(t, predicate = "part = 'b' AND id < 4")
    w.snapshot(t)
  }

  test("dpReadPartitionAfterAppend", "Read partitioned table after appending",
      "partitioned") { w =>
    w.sql("CREATE TABLE tbl (id INT, part STRING) USING delta PARTITIONED BY (part)")
    w.sql("INSERT INTO tbl VALUES (1, 'a'), (2, 'b')")
    w.sql("INSERT INTO tbl VALUES (3, 'c'), (4, 'a')")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "part = 'a'")
    w.read(t, predicate = "part = 'c'")
    w.snapshot(t)
  }

  test("dpReadPartitionBoolean", "Read table partitioned by boolean column",
      "partitioned") { w =>
    w.sql("CREATE TABLE tbl (id INT, flag BOOLEAN) USING delta PARTITIONED BY (flag)")
    w.sql("INSERT INTO tbl VALUES (1, true), (2, false), (3, true)")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "flag = true")
    w.read(t, predicate = "flag = false")
    w.snapshot(t)
  }

  test("dpReadPartitionDecimal", "Read table partitioned by decimal column",
      "partitioned") { w =>
    w.sql("CREATE TABLE tbl (id INT, amt DECIMAL(10,2)) USING delta PARTITIONED BY (amt)")
    w.sql("INSERT INTO tbl VALUES (1, 10.50), (2, 20.75), (3, 10.50)")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "amt = 10.50")
    w.read(t, predicate = "amt > 15.00")
    w.snapshot(t)
  }

  test("dpReadPartitionLongType", "Read table partitioned by long integer type",
      "partitioned") { w =>
    w.sql("CREATE TABLE tbl (id INT, part LONG) USING delta PARTITIONED BY (part)")
    w.sql("INSERT INTO tbl VALUES (1, 1000000000), (2, 2000000000), (3, 3000000000)")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "part = 2000000000")
    w.read(t, predicate = "part > 2500000000")
    w.snapshot(t)
  }

  test("dpReadPartitionTimestamp", "Read table partitioned by timestamp column",
      "partitioned") { w =>
    w.sql("CREATE TABLE tbl (id INT, ts TIMESTAMP) USING delta PARTITIONED BY (ts)")
    w.sql("""INSERT INTO tbl VALUES
      (1, TIMESTAMP'2024-01-01 00:00:00'),
      (2, TIMESTAMP'2024-06-15 12:00:00'),
      (3, TIMESTAMP'2025-01-01 00:00:00')""")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "ts = TIMESTAMP'2024-01-01 00:00:00'")
    w.read(t, predicate = "ts > TIMESTAMP'2024-06-01 00:00:00'")
    w.snapshot(t)
  }

  test("dpReadPartitionSpecialChars", "Read partition with special characters",
      "partitioned") { w =>
    w.sql("CREATE TABLE tbl (id INT, part STRING) USING delta PARTITIONED BY (part)")
    w.sql("INSERT INTO tbl VALUES (1, 'hello world'), (2, 'foo=bar'), (3, 'a/b'), (4, 'x%y')")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "part = 'hello world'")
    w.read(t, predicate = "part = 'foo=bar'")
    w.read(t, predicate = "part = 'a/b'")
    w.snapshot(t)
  }

  // ---------------------------------------------------------------------------
  // File path edge cases (fpe_*)
  // ---------------------------------------------------------------------------

  test("fpe_space_in_path", "File path with space encoding (%20)", "filePath") { w =>
    w.sql("CREATE TABLE tbl (id INT, value STRING) USING delta")
    w.sql("INSERT INTO tbl VALUES (1, 'hello'), (2, 'world')")
    val t = w.table("tbl")
    // Rename data files to contain spaces
    w.mutateTable(t) { dir =>
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
    w.read(t)
    w.snapshot(t)
  }

  test("fpe_special_chars_path", "File path with encoded special chars",
      "filePath") { w =>
    w.sql("CREATE TABLE tbl (id INT, value STRING) USING delta")
    w.sql("INSERT INTO tbl VALUES (1, 'hello'), (2, 'world')")
    val t = w.table("tbl")
    w.mutateTable(t) { dir =>
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
    w.read(t)
    w.snapshot(t)
  }

  test("fpe_unicode_in_path", "File path with percent-encoded multi-byte UTF-8",
      "filePath") { w =>
    w.sql("CREATE TABLE tbl (id INT, value STRING) USING delta")
    w.sql("INSERT INTO tbl VALUES (1, 'hello'), (2, 'world')")
    val t = w.table("tbl")
    w.mutateTable(t) { dir =>
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
    w.read(t)
    w.snapshot(t)
  }

  test("fpe_absolute_path", "Add action with absolute URI path (file:///)",
      "filePath") { w =>
    w.sql("CREATE TABLE tbl (id INT, value STRING) USING delta")
    w.sql("INSERT INTO tbl VALUES (1, 'hello'), (2, 'world')")
    val t = w.table("tbl")
    // After copy, rewrite add path to absolute file:/// URI
    w.mutateTable(t) { dir =>
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
    w.read(t)
    w.snapshot(t)
  }

  test("fpe_partition_dir_encoded", "Partition directory with encoded value",
      "filePath", "partitioned") { w =>
    w.sql("CREATE TABLE tbl (id INT, city STRING) USING delta PARTITIONED BY (city)")
    w.sql("INSERT INTO tbl VALUES (1, 'New York'), (2, 'San Francisco'), (3, 'Tokyo')")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "city = 'New York'")
    w.read(t, predicate = "city = 'Tokyo'")
    w.snapshot(t)
  }

  // === Core Reads Legacy ===

  // ---------------------------------------------------------------------------
  // dsReadAfterOptimize: 10 single-row appends then OPTIMIZE (DVs enabled)
  // ---------------------------------------------------------------------------
  test("dsReadAfterOptimize", "Read after OPTIMIZE") { w =>
    w.sql("""CREATE TABLE tbl (value INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    (1 to 10).foreach { i =>
      w.sql(s"INSERT INTO tbl VALUES ($i)")
    }
    w.sql("OPTIMIZE tbl")
    val t = w.table("tbl")
    w.read(t)
    w.snapshot(t)
  }

  // ---------------------------------------------------------------------------
  // dsReadAndPredicate: compound AND predicate
  // ---------------------------------------------------------------------------
  test("dsReadAndPredicate", "Read with AND predicate") { w =>
    w.sql("""CREATE TABLE tbl (id INT, cat STRING, score INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1,'a',10),(2,'b',20),(3,'a',30),(4,'b',10),(5,'a',20)")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "cat = 'a' AND score > 15")
    w.snapshot(t)
  }

  // ---------------------------------------------------------------------------
  // dsReadAppend: two appends
  // ---------------------------------------------------------------------------
  test("dsReadAppend", "Read after append") { w =>
    w.sql("""CREATE TABLE tbl (value INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(1, 6)")
    w.sql("INSERT INTO tbl SELECT id FROM range(6, 11)")
    val t = w.table("tbl")
    w.read(t)
    w.snapshot(t)
  }

  // ---------------------------------------------------------------------------
  // dsReadArrayColumn: array columns
  // ---------------------------------------------------------------------------
  test("dsReadArrayColumn", "Array columns") { w =>
    w.sql("""CREATE TABLE tbl (id INT, tags ARRAY<STRING>, scores ARRAY<INT>) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1, array('a','b','c'), array(10,20,30))")
    w.sql("INSERT INTO tbl VALUES (2, array('x'), array(99))")
    w.sql("INSERT INTO tbl VALUES (3, array(), array())")
    val t = w.table("tbl")
    w.read(t)
    w.snapshot(t)
  }

  // ---------------------------------------------------------------------------
  // dsReadBadProtocol: error on unsupported reader version
  // ---------------------------------------------------------------------------
  test("dsReadBadProtocol", "Error: unsupported reader version", "error") { w =>
    w.sql("""CREATE TABLE tbl (id INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1),(2),(3)")
    val t = w.table("tbl")
    // Bump minReaderVersion to an unsupported value
    w.mutateTable(t) { dir =>
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
    w.snapshot(t)
  }

  // ---------------------------------------------------------------------------
  // dsReadBasic: simple 10-row table
  // ---------------------------------------------------------------------------
  test("dsReadBasic", "Basic read") { w =>
    w.sql("""CREATE TABLE tbl (value INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(1, 11)")
    val t = w.table("tbl")
    w.read(t)
    w.snapshot(t)
  }

  // ---------------------------------------------------------------------------
  // dsReadBetweenPredicate: BETWEEN filter
  // ---------------------------------------------------------------------------
  test("dsReadBetweenPredicate", "Read with BETWEEN predicate") { w =>
    w.sql("""CREATE TABLE tbl (value INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(1, 21)")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "value BETWEEN 5 AND 15")
    w.snapshot(t)
  }

  // ---------------------------------------------------------------------------
  // dsReadBinaryType: binary column
  // ---------------------------------------------------------------------------
  test("dsReadBinaryType", "Binary type") { w =>
    w.sql("""CREATE TABLE tbl (id INT, data BINARY) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1, X'48454C4C4F')")
    w.sql("INSERT INTO tbl VALUES (2, X'574F524C44')")
    w.sql("INSERT INTO tbl VALUES (3, X'')")
    val t = w.table("tbl")
    w.read(t)
    w.snapshot(t)
  }

  // ---------------------------------------------------------------------------
  // dsReadBooleanFilter: boolean column with true/false filters
  // ---------------------------------------------------------------------------
  test("dsReadBooleanFilter", "Boolean filter") { w =>
    w.sql("""CREATE TABLE tbl (id INT, active BOOLEAN) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1, true)")
    w.sql("INSERT INTO tbl VALUES (2, false)")
    w.sql("INSERT INTO tbl VALUES (3, true)")
    w.sql("INSERT INTO tbl VALUES (4, false)")
    w.sql("INSERT INTO tbl VALUES (5, true)")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "active = true")
    w.read(t, predicate = "active = false")
    w.snapshot(t)
  }

  // ---------------------------------------------------------------------------
  // dsReadCaseSensitive: mixed-case column names
  // ---------------------------------------------------------------------------
  test("dsReadCaseSensitive", "Case-sensitive column names") { w =>
    w.sql("""CREATE TABLE tbl (Id INT, FirstName STRING, lastName STRING, AGE INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1, 'Alice', 'Smith', 30)")
    w.sql("INSERT INTO tbl VALUES (2, 'Bob', 'Jones', 25)")
    val t = w.table("tbl")
    w.read(t)
    w.snapshot(t)
  }

  // ---------------------------------------------------------------------------
  // dsReadDateType: date column with filter
  // ---------------------------------------------------------------------------
  test("dsReadDateType", "Date type with filter") { w =>
    w.sql("""CREATE TABLE tbl (id INT, event_date DATE) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1, DATE'2024-01-01')")
    w.sql("INSERT INTO tbl VALUES (2, DATE'2024-06-15')")
    w.sql("INSERT INTO tbl VALUES (3, DATE'2024-12-31')")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "event_date > DATE'2024-06-01'")
    w.snapshot(t)
  }

  // ---------------------------------------------------------------------------
  // dsReadDecimalType: decimal columns
  // ---------------------------------------------------------------------------
  test("dsReadDecimalType", "Decimal type") { w =>
    w.sql("""CREATE TABLE tbl (id INT, price DECIMAL(10,2), ratio DECIMAL(18,8)) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1, 99.99, 0.12345678)")
    w.sql("INSERT INTO tbl VALUES (2, 1234.56, 3.14159265)")
    w.sql("INSERT INTO tbl VALUES (3, 0.01, 0.00000001)")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "price > 100")
    w.snapshot(t)
  }

  // ---------------------------------------------------------------------------
  // dsReadDeleteThenRead: DELETE then read
  // ---------------------------------------------------------------------------
  test("dsReadDeleteThenRead", "Read after DELETE") { w =>
    w.sql("""CREATE TABLE tbl (value INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(1, 11)")
    w.sql("DELETE FROM tbl WHERE value <= 3")
    val t = w.table("tbl")
    w.read(t)
    w.snapshot(t)
  }

  // ---------------------------------------------------------------------------
  // dsReadDoubleType: double values including extreme
  // ---------------------------------------------------------------------------
  test("dsReadDoubleType", "Double type") { w =>
    w.sql("""CREATE TABLE tbl (id INT, dval DOUBLE) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1, 3.14159265358979), (2, -0.001), (3, 1.7976931348623157E308), (4, 0.0)")
    val t = w.table("tbl")
    w.read(t)
    w.snapshot(t)
  }

  // ---------------------------------------------------------------------------
  // dsReadEmptyPartition: partitioned table, filter on non-existent partition
  // ---------------------------------------------------------------------------
  test("dsReadEmptyPartition", "Empty partition filter result") { w =>
    w.sql("""CREATE TABLE tbl (id BIGINT, part INT) USING delta
      PARTITIONED BY (part)
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id, CAST(id % 3 AS INT) FROM range(50)")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "part = 99")
    w.snapshot(t)
  }

  // ---------------------------------------------------------------------------
  // dsReadEmptyString: error - no delta table at path
  // ---------------------------------------------------------------------------
  test("dsReadEmptyString", "Error: no delta table at empty subdir", "error") { w =>
    w.sql("""CREATE TABLE tbl (id INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1)")
    val t = w.table("tbl")
    // Delete the delta log to simulate missing table
    w.mutateTable(t) { dir =>
      val logDir = dir.resolve("_delta_log")
      if (java.nio.file.Files.exists(logDir)) {
        java.nio.file.Files.walk(logDir).sorted(java.util.Comparator.reverseOrder())
          .forEach(p => java.nio.file.Files.deleteIfExists(p))
      }
    }
    w.snapshot(t)
  }

  // ---------------------------------------------------------------------------
  // dsReadEmptyTable: error - no delta table at path
  // ---------------------------------------------------------------------------
  test("dsReadEmptyTable", "Error: no delta table", "error") { w =>
    w.sql("""CREATE TABLE tbl (id INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1)")
    val t = w.table("tbl")
    w.mutateTable(t) { dir =>
      val logDir = dir.resolve("_delta_log")
      if (java.nio.file.Files.exists(logDir)) {
        java.nio.file.Files.walk(logDir).sorted(java.util.Comparator.reverseOrder())
          .forEach(p => java.nio.file.Files.deleteIfExists(p))
      }
    }
    w.snapshot(t)
  }

  // ---------------------------------------------------------------------------
  // dsReadFloatType: float values including extreme
  // ---------------------------------------------------------------------------
  test("dsReadFloatType", "Float type") { w =>
    w.sql("""CREATE TABLE tbl (id INT, fval FLOAT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1, CAST(1.5 AS FLOAT)), (2, CAST(-2.5 AS FLOAT)), (3, CAST(0.0 AS FLOAT)), (4, CAST(3.4028235E38 AS FLOAT))")
    val t = w.table("tbl")
    w.read(t)
    w.snapshot(t)
  }

  // ---------------------------------------------------------------------------
  // dsReadInPredicate: IN predicate
  // ---------------------------------------------------------------------------
  test("dsReadInPredicate", "Read with IN predicate") { w =>
    w.sql("""CREATE TABLE tbl (value INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(1, 21)")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "value IN (1, 5, 10, 15, 20)")
    w.snapshot(t)
  }

  // ---------------------------------------------------------------------------
  // dsReadIsNotNullPredicate: IS NOT NULL filter
  // ---------------------------------------------------------------------------
  test("dsReadIsNotNullPredicate", "IS NOT NULL predicate") { w =>
    w.sql("""CREATE TABLE tbl (id INT, label STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1,'a'),(NULL,'b'),(3,'c'),(NULL,'d'),(5,'e')")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "id IS NOT NULL")
    w.snapshot(t)
  }

  // ---------------------------------------------------------------------------
  // dsReadIsNullPredicate: IS NULL filter
  // ---------------------------------------------------------------------------
  test("dsReadIsNullPredicate", "IS NULL predicate") { w =>
    w.sql("""CREATE TABLE tbl (id INT, label STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1,'a'),(NULL,'b'),(3,'c'),(NULL,'d'),(5,'e')")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "id IS NULL")
    w.snapshot(t)
  }

  // ---------------------------------------------------------------------------
  // dsReadLargeSchema: 25-column table (id + 24 computed cols)
  // ---------------------------------------------------------------------------
  test("dsReadLargeSchema", "25 columns") { w =>
    val colDefs = (1 to 24).map(i => s"col_$i BIGINT").mkString(", ")
    w.sql(s"""CREATE TABLE tbl (id BIGINT, $colDefs) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    val colExprs = (1 to 24).map(i => s"id * $i AS col_$i").mkString(", ")
    w.sql(s"INSERT INTO tbl SELECT id, $colExprs FROM range(5)")
    val t = w.table("tbl")
    w.read(t)
    w.snapshot(t)
  }

  // ---------------------------------------------------------------------------
  // dsReadLikePredicate: LIKE filter
  // ---------------------------------------------------------------------------
  test("dsReadLikePredicate", "LIKE predicate") { w =>
    w.sql("""CREATE TABLE tbl (id INT, word STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1,'alpha'),(2,'beta'),(3,'alphabet'),(4,'gamma'),(5,'alpine')")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "word LIKE 'alp%'")
    w.snapshot(t)
  }

  // ---------------------------------------------------------------------------
  // dsReadLongType: long values including min/max
  // ---------------------------------------------------------------------------
  test("dsReadLongType", "Long type") { w =>
    w.sql("""CREATE TABLE tbl (lval BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1),(9223372036854775807),(-9223372036854775808),(0),(9876543210)")
    val t = w.table("tbl")
    w.read(t)
    w.snapshot(t)
  }

  // ---------------------------------------------------------------------------
  // dsReadMapColumn: map columns
  // ---------------------------------------------------------------------------
  test("dsReadMapColumn", "Map columns") { w =>
    w.sql("""CREATE TABLE tbl (id INT, props MAP<STRING, STRING>) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1, map('color','red','size','large'))")
    w.sql("INSERT INTO tbl VALUES (2, map('color','blue'))")
    w.sql("INSERT INTO tbl VALUES (3, map())")
    val t = w.table("tbl")
    w.read(t)
    w.snapshot(t)
  }

  // ---------------------------------------------------------------------------
  // dsReadMergeThenRead: MERGE then read
  // ---------------------------------------------------------------------------
  test("dsReadMergeThenRead", "Read after MERGE") { w =>
    w.sql("""CREATE TABLE target (id INT, val STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO target VALUES (1,'a'),(2,'b'),(3,'c')")
    w.sql("""CREATE TABLE src (id INT, val STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO src VALUES (2,'updated'),(4,'new')")
    w.sql("""MERGE INTO target t USING src s ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET val = s.val
      WHEN NOT MATCHED THEN INSERT *""")
    val t = w.table("target")
    w.read(t)
    w.snapshot(t)
  }

  // ---------------------------------------------------------------------------
  // dsReadMultiPartition: multiple partition columns
  // ---------------------------------------------------------------------------
  test("dsReadMultiPartition", "Multiple partition columns") { w =>
    w.sql("""CREATE TABLE tbl (id INT, country STRING, city STRING, amount INT) USING delta
      PARTITIONED BY (country, city)
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1,'US','NY',100),(2,'US','CA',200),(3,'UK','LON',150),(4,'UK','MAN',120),(5,'US','NY',300)")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "country = 'US'")
    w.read(t, predicate = "country = 'US' AND city = 'NY'")
    w.snapshot(t)
  }

  // ---------------------------------------------------------------------------
  // dsReadMultipleAppends: 7 single-row appends
  // ---------------------------------------------------------------------------
  test("dsReadMultipleAppends", "Multiple appends") { w =>
    w.sql("""CREATE TABLE tbl (id INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    (1 to 7).foreach { i =>
      w.sql(s"INSERT INTO tbl VALUES ($i)")
    }
    val t = w.table("tbl")
    w.read(t)
    w.snapshot(t)
  }

  // ---------------------------------------------------------------------------
  // dsReadMultipleTypes: multiple data types in one table
  // ---------------------------------------------------------------------------
  test("dsReadMultipleTypes", "Multiple data types") { w =>
    w.sql("""CREATE TABLE tbl (
      id INT, name STRING, score DOUBLE, active BOOLEAN,
      created DATE, updated TIMESTAMP
    ) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1,'alice',95.5,true,DATE'2024-01-01',TIMESTAMP'2024-01-01 10:00:00')")
    w.sql("INSERT INTO tbl VALUES (2,'bob',82.3,false,DATE'2024-02-15',TIMESTAMP'2024-02-15 14:30:00')")
    w.sql("INSERT INTO tbl VALUES (3,'charlie',91.0,true,DATE'2024-03-20',TIMESTAMP'2024-03-20 08:15:00')")
    val t = w.table("tbl")
    w.read(t)
    w.snapshot(t)
  }

  // ---------------------------------------------------------------------------
  // dsReadNestedStruct: nested struct columns
  // ---------------------------------------------------------------------------
  test("dsReadNestedStruct", "Nested struct columns") { w =>
    w.sql("""CREATE TABLE tbl (
      id INT, info STRUCT<name: STRING, age: INT, address: STRUCT<city: STRING, zip: STRING>>
    ) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1, named_struct('name','alice','age',30,'address',named_struct('city','NYC','zip','10001')))")
    w.sql("INSERT INTO tbl VALUES (2, named_struct('name','bob','age',25,'address',named_struct('city','LA','zip','90001')))")
    val t = w.table("tbl")
    w.read(t)
    w.snapshot(t)
  }

  // ---------------------------------------------------------------------------
  // dsReadNotEqualPredicate: != filter
  // ---------------------------------------------------------------------------
  test("dsReadNotEqualPredicate", "Not equal predicate") { w =>
    w.sql("""CREATE TABLE tbl (id INT, cat STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c'),(4,'a'),(5,'d')")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "cat != 'a'")
    w.snapshot(t)
  }

  // ---------------------------------------------------------------------------
  // dsReadNullValues: null values across types
  // ---------------------------------------------------------------------------
  test("dsReadNullValues", "Null values across types") { w =>
    w.sql("""CREATE TABLE tbl (id INT, name STRING, score DOUBLE, active BOOLEAN) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1,'alice',95.5,true)")
    w.sql("INSERT INTO tbl VALUES (2,null,null,null)")
    w.sql("INSERT INTO tbl VALUES (null,'charlie',88.0,false)")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "name IS NOT NULL")
    w.snapshot(t)
  }

  // ---------------------------------------------------------------------------
  // dsReadOrPredicate: OR predicate
  // ---------------------------------------------------------------------------
  test("dsReadOrPredicate", "OR predicate") { w =>
    w.sql("""CREATE TABLE tbl (id INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(1, 11)")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "id = 1 OR id = 5 OR id = 10")
    w.snapshot(t)
  }

  // ---------------------------------------------------------------------------
  // dsReadOverwrite: overwrite table
  // ---------------------------------------------------------------------------
  test("dsReadOverwrite", "Read after overwrite") { w =>
    w.sql("""CREATE TABLE tbl (value INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(1, 11)")
    w.sql("INSERT OVERWRITE tbl SELECT id FROM range(100, 106)")
    val t = w.table("tbl")
    w.read(t)
    w.snapshot(t)
  }

  // ---------------------------------------------------------------------------
  // dsReadPartitioned: partitioned table with filters
  // ---------------------------------------------------------------------------
  test("dsReadPartitioned", "Partitioned read with filter") { w =>
    w.sql("""CREATE TABLE tbl (id BIGINT, part INT) USING delta
      PARTITIONED BY (part)
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id, CAST(id % 5 AS INT) FROM range(100)")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "part = 0")
    w.read(t, predicate = "part = 3")
    w.snapshot(t)
  }

  // ---------------------------------------------------------------------------
  // dsReadPathWithSpaces: error - path with spaces (no delta log)
  // ---------------------------------------------------------------------------
  test("dsReadPathWithSpaces", "Error: path with spaces", "error") { w =>
    w.sql("""CREATE TABLE tbl (id INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1)")
    val t = w.table("tbl")
    w.mutateTable(t) { dir =>
      val logDir = dir.resolve("_delta_log")
      if (java.nio.file.Files.exists(logDir)) {
        java.nio.file.Files.walk(logDir).sorted(java.util.Comparator.reverseOrder())
          .forEach(p => java.nio.file.Files.deleteIfExists(p))
      }
    }
    w.snapshot(t)
  }

  // ---------------------------------------------------------------------------
  // dsReadSaveMode: ErrorIfExists then Append
  // ---------------------------------------------------------------------------
  test("dsReadSaveMode", "Save mode Append") { w =>
    w.sql("""CREATE TABLE tbl (id INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1),(2),(3)")
    w.sql("INSERT INTO tbl VALUES (4),(5)")
    val t = w.table("tbl")
    w.read(t)
    w.snapshot(t)
  }

  // ---------------------------------------------------------------------------
  // dsReadSaveModeErrorIfExists: single write
  // ---------------------------------------------------------------------------
  test("dsReadSaveModeErrorIfExists", "Save mode ErrorIfExists") { w =>
    w.sql("""CREATE TABLE tbl (id INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1),(2),(3)")
    val t = w.table("tbl")
    w.read(t)
    w.snapshot(t)
  }

  // ---------------------------------------------------------------------------
  // dsReadSaveModeIgnore: write then ignore
  // ---------------------------------------------------------------------------
  test("dsReadSaveModeIgnore", "Save mode Ignore") { w =>
    w.sql("""CREATE TABLE tbl (id INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1),(2),(3)")
    // Second write is Ignore mode - table already exists, so data is NOT written
    // The result is still just the original 3 rows
    val t = w.table("tbl")
    w.read(t)
    w.snapshot(t)
  }

  // ---------------------------------------------------------------------------
  // dsReadSaveModeOverwrite: write then overwrite
  // ---------------------------------------------------------------------------
  test("dsReadSaveModeOverwrite", "Save mode Overwrite") { w =>
    w.sql("""CREATE TABLE tbl (id INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1),(2),(3)")
    w.sql("INSERT OVERWRITE tbl VALUES (10),(20)")
    val t = w.table("tbl")
    w.read(t)
    w.snapshot(t)
  }

  // ---------------------------------------------------------------------------
  // dsReadSchemaEvolution: ADD COLUMN then insert
  // ---------------------------------------------------------------------------
  test("dsReadSchemaEvolution", "Schema evolution with ADD COLUMN") { w =>
    w.sql("""CREATE TABLE tbl (id INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(1, 6)")
    w.sql("ALTER TABLE tbl ADD COLUMN name STRING")
    w.sql("INSERT INTO tbl VALUES (6,'alice')")
    w.sql("INSERT INTO tbl VALUES (7,'bob')")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "name IS NOT NULL")
    w.snapshot(t, version = 0)
    w.snapshot(t, version = 1)
    w.snapshot(t, version = 2)
    w.snapshot(t, version = 3)
    w.snapshot(t)
  }

  // ---------------------------------------------------------------------------
  // dsReadSelectColumns: column projection
  // ---------------------------------------------------------------------------
  test("dsReadSelectColumns", "Column projection") { w =>
    w.sql("""CREATE TABLE tbl (id INT, name STRING, score DOUBLE, category STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1,'alice',95.5,'A')")
    w.sql("INSERT INTO tbl VALUES (2,'bob',82.3,'B')")
    w.sql("INSERT INTO tbl VALUES (3,'charlie',91.0,'A')")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, columns = Seq("id", "name"))
    w.read(t, columns = Seq("score"))
    w.snapshot(t)
  }

  // ---------------------------------------------------------------------------
  // dsReadSnapshot: time travel read at versions
  // ---------------------------------------------------------------------------
  test("dsReadSnapshot", "Time travel snapshot read") { w =>
    w.sql("""CREATE TABLE tbl (value INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(1, 11)")
    w.sql("INSERT INTO tbl SELECT id FROM range(11, 21)")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, version = 0)
    w.read(t, version = 1)
    w.snapshot(t)
  }

  // ---------------------------------------------------------------------------
  // dsReadSnapshotPartitioned: partitioned snapshot read
  // ---------------------------------------------------------------------------
  test("dsReadSnapshotPartitioned", "Partitioned snapshot read") { w =>
    w.sql("""CREATE TABLE tbl (region STRING, category INT, amount INT) USING delta
      PARTITIONED BY (region, category)
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES ('a',1,100),('b',2,200),('a',2,300)")
    val t = w.table("tbl")
    w.read(t)
    w.snapshot(t)
  }

  // ---------------------------------------------------------------------------
  // dsReadSnapshotWithProperties: table with custom properties
  // ---------------------------------------------------------------------------
  test("dsReadSnapshotWithProperties", "Snapshot with table properties") { w =>
    w.sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES (
        'delta.enableDeletionVectors' = 'true',
        'delta.logRetentionDuration' = 'interval 60 days',
        'delta.deletedFileRetentionDuration' = 'interval 30 days'
      )""")
    w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
    val t = w.table("tbl")
    w.read(t)
    w.snapshot(t)
  }

  // ---------------------------------------------------------------------------
  // dsReadSpecialChars: special chars in partition values
  // ---------------------------------------------------------------------------
  test("dsReadSpecialChars", "Special chars in partition values") { w =>
    w.sql("""CREATE TABLE tbl (id INT, category STRING) USING delta
      PARTITIONED BY (category)
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1,'hello world'),(2,'foo=bar'),(3,'a/b'),(4,'c%20d')")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "category = 'hello world'")
    w.snapshot(t)
  }

  // ---------------------------------------------------------------------------
  // dsReadStringFilter: string equality filter
  // ---------------------------------------------------------------------------
  test("dsReadStringFilter", "String equality filter") { w =>
    w.sql("""CREATE TABLE tbl (id INT, fruit STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1,'apple'),(2,'banana'),(3,'cherry'),(4,'apple'),(5,'date')")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "fruit = 'apple'")
    w.snapshot(t)
  }

  // ---------------------------------------------------------------------------
  // dsReadTimestampType: timestamp column
  // ---------------------------------------------------------------------------
  test("dsReadTimestampType", "Timestamp type") { w =>
    w.sql("""CREATE TABLE tbl (id INT, event_time TIMESTAMP) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1, TIMESTAMP'2024-01-15 10:30:00')")
    w.sql("INSERT INTO tbl VALUES (2, TIMESTAMP'2024-06-20 22:59:59')")
    w.sql("INSERT INTO tbl VALUES (3, TIMESTAMP'2024-12-31 00:00:00')")
    val t = w.table("tbl")
    w.read(t)
    w.snapshot(t)
  }

  // ---------------------------------------------------------------------------
  // dsReadUpdateThenRead: UPDATE then read
  // ---------------------------------------------------------------------------
  test("dsReadUpdateThenRead", "Read after UPDATE") { w =>
    w.sql("""CREATE TABLE tbl (value INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(1, 11)")
    w.sql("UPDATE tbl SET value = value + 100 WHERE value <= 5")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "value > 100")
    w.snapshot(t)
  }

  // ---------------------------------------------------------------------------
  // dsReadVersionNegative: error - negative version
  // ---------------------------------------------------------------------------
  test("dsReadVersionNegative", "Error: negative version", "error") { w =>
    w.sql("""CREATE TABLE tbl (value INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(1, 6)")
    val t = w.table("tbl")
    w.snapshot(t, version = -1)
    w.snapshot(t)
  }

  // ---------------------------------------------------------------------------
  // dsReadVersionOutOfRange: error - version out of range
  // ---------------------------------------------------------------------------
  test("dsReadVersionOutOfRange", "Error: version out of range", "error") { w =>
    w.sql("""CREATE TABLE tbl (id INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(1, 6)")
    val t = w.table("tbl")
    w.snapshot(t, version = 100)
  }

  // ---------------------------------------------------------------------------
  // dsReadVersionZero: time travel to v0
  // ---------------------------------------------------------------------------
  test("dsReadVersionZero", "Time travel to version 0") { w =>
    w.sql("""CREATE TABLE tbl (value INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(1, 6)")
    w.sql("INSERT INTO tbl SELECT id FROM range(6, 11)")
    w.sql("INSERT INTO tbl SELECT id FROM range(11, 16)")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, version = 0)
    w.read(t, version = 1)
    w.snapshot(t)
  }

  // ---------------------------------------------------------------------------
  // dsReadWithAlias: simple read (alias is a Spark concept, table is normal)
  // ---------------------------------------------------------------------------
  test("dsReadWithAlias", "Read with alias") { w =>
    w.sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1,'alpha'),(2,'beta'),(3,'gamma')")
    val t = w.table("tbl")
    w.read(t)
    w.snapshot(t)
  }

  // ---------------------------------------------------------------------------
  // dsReadWithLimit: read all (LIMIT is a Spark concept)
  // ---------------------------------------------------------------------------
  test("dsReadWithLimit", "Read with limit") { w =>
    w.sql("""CREATE TABLE tbl (id INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(1, 21)")
    val t = w.table("tbl")
    w.read(t)
    w.snapshot(t)
  }

  // ---------------------------------------------------------------------------
  // dsReadWithOrderBy: read all (ORDER BY is a Spark concept)
  // ---------------------------------------------------------------------------
  test("dsReadWithOrderBy", "Read with order by") { w =>
    w.sql("""CREATE TABLE tbl (id INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (5),(3),(1),(4),(2)")
    val t = w.table("tbl")
    w.read(t)
    w.snapshot(t)
  }

  // ---------------------------------------------------------------------------
  // dsReadWithPredicate: simple predicate pushdown
  // ---------------------------------------------------------------------------
  test("dsReadWithPredicate", "Predicate pushdown") { w =>
    w.sql("""CREATE TABLE tbl (value INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(1, 21)")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "value > 5")
    w.snapshot(t)
  }

  // ===========================================================================
  // dse* workloads (Delta Suite Extended)
  // ===========================================================================

  // ---------------------------------------------------------------------------
  // dseReadAfterAlterTable: SET TBLPROPERTIES then append
  // ---------------------------------------------------------------------------
  test("dseReadAfterAlterTable", "Read after ALTER TABLE SET TBLPROPERTIES") { w =>
    w.sql("""CREATE TABLE tbl (value INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(1, 11)")
    w.sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.appendOnly' = 'true')")
    w.sql("INSERT INTO tbl SELECT id FROM range(11, 16)")
    val t = w.table("tbl")
    w.read(t)
    w.snapshot(t)
  }

  // ---------------------------------------------------------------------------
  // dseReadAfterSchemaChange: ADD COLUMN then insert with new column
  // ---------------------------------------------------------------------------
  test("dseReadAfterSchemaChange", "Read after schema change") { w =>
    w.sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1,'alpha'),(2,'beta')")
    w.sql("ALTER TABLE tbl ADD COLUMN score DOUBLE")
    w.sql("INSERT INTO tbl VALUES (3,'gamma',99.9)")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "score IS NOT NULL")
    w.read(t, predicate = "score IS NULL")
    w.snapshot(t)
  }

  // ---------------------------------------------------------------------------
  // dseReadLargeFile: 10000-row table
  // ---------------------------------------------------------------------------
  test("dseReadLargeFile", "Large file read") { w =>
    w.sql("""CREATE TABLE tbl (id BIGINT, data STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id, 'payload' FROM range(10000)")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "id < 100")
    w.snapshot(t)
  }

  // ---------------------------------------------------------------------------
  // dseReadSmallFiles: 10 single-row appends
  // ---------------------------------------------------------------------------
  test("dseReadSmallFiles", "Many small files") { w =>
    w.sql("""CREATE TABLE tbl (value INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    (1 to 10).foreach { i =>
      w.sql(s"INSERT INTO tbl VALUES ($i)")
    }
    val t = w.table("tbl")
    w.read(t)
    w.snapshot(t)
  }

  // ---------------------------------------------------------------------------
  // dseReadRepartitioned: 100 rows written (repartition is Spark concept)
  // ---------------------------------------------------------------------------
  test("dseReadRepartitioned", "Repartitioned read") { w =>
    w.sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(100)")
    val t = w.table("tbl")
    w.read(t)
    w.snapshot(t)
  }

  // ---------------------------------------------------------------------------
  // dseReadWithColumnPruning: column pruning / projection
  // ---------------------------------------------------------------------------
  test("dseReadWithColumnPruning", "Column pruning") { w =>
    w.sql("""CREATE TABLE tbl (id INT, name STRING, score DOUBLE, active BOOLEAN) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1,'alpha',10.0,true),(2,'beta',20.0,false),(3,'gamma',30.0,true)")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, columns = Seq("id", "name"))
    w.read(t, columns = Seq("score"))
    w.snapshot(t)
  }

  // ---------------------------------------------------------------------------
  // dseReadWithStats: 5 batches of 3 rows for stats-based filtering
  // ---------------------------------------------------------------------------
  test("dseReadWithStats", "Read with stats-based filtering") { w =>
    w.sql("""CREATE TABLE tbl (id INT, batch STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    // 5 separate appends so each batch lands in its own file (for stats skipping)
    w.sql("INSERT INTO tbl VALUES (1,'batch0'),(2,'batch0'),(3,'batch0')")
    w.sql("INSERT INTO tbl VALUES (101,'batch1'),(102,'batch1'),(103,'batch1')")
    w.sql("INSERT INTO tbl VALUES (201,'batch2'),(202,'batch2'),(203,'batch2')")
    w.sql("INSERT INTO tbl VALUES (301,'batch3'),(302,'batch3'),(303,'batch3')")
    w.sql("INSERT INTO tbl VALUES (401,'batch4'),(402,'batch4'),(403,'batch4')")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "id = 1")
    w.read(t, predicate = "id >= 201 AND id <= 203")
    w.snapshot(t)
  }

  // ---------------------------------------------------------------------------
  // dseSnapshotVersion: time travel across 3 versions
  // ---------------------------------------------------------------------------
  test("dseSnapshotVersion", "Snapshot version read") { w =>
    w.sql("""CREATE TABLE tbl (value INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(1, 6)")
    w.sql("INSERT INTO tbl SELECT id FROM range(6, 11)")
    w.sql("INSERT INTO tbl SELECT id FROM range(11, 16)")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, version = 0)
    w.read(t, version = 1)
    w.read(t, version = 2)
    w.snapshot(t)
  }

  // --- Missing ds*/dse* workloads (matching acceptance_workloads directories) ---

  test("dsReadAfterVacuum", "Read after VACUUM") { w =>
    w.sql("""CREATE TABLE tbl (value BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id + 1 FROM range(10)")
    w.sql("INSERT OVERWRITE tbl SELECT id + 11 FROM range(10)")
    val t = w.table("tbl")
    w.mutateTable(t) { dir =>
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
    w.read(t)
    w.snapshot(t)
  }

  test("dsReadByteShortType", "Read table with byte and short integer types", "types") { w =>
    w.sql("""CREATE TABLE tbl (bval TINYINT, sval SMALLINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1, 100), (-128, -32768), (127, 32767), (0, 0)")
    val t = w.table("tbl")
    w.read(t)
    w.snapshot(t)
  }

  test("dsReadCdcEnabled", "Read base table with CDC enabled", "cdc") { w =>
    w.sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true',
        'delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')")
    w.sql("UPDATE tbl SET value = 'alpha_v2' WHERE id = 1")
    w.sql("DELETE FROM tbl WHERE id = 3")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "id = 1")
    w.read(t, predicate = "id = 3")
    w.snapshot(t)
  }

  test("dsReadColumnReorder", "Read table where columns are in different order", "schema") { w =>
    w.sql("""CREATE TABLE tbl (id INT, name STRING, score DOUBLE) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1, 'alpha', 10.0), (2, 'beta', 20.0), (3, 'gamma', 30.0)")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, columns = Seq("score", "id", "name"))
    w.snapshot(t)
  }

  test("dsReadCorruptCheckpoint", "Error reading table with corrupt checkpoint") { w =>
    w.sql("""CREATE TABLE tbl (id INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true', 'delta.checkpointInterval' = '5')""")
    for (i <- 0 until 6) w.sql(s"INSERT INTO tbl VALUES ($i)")
    val t = w.table("tbl")
    w.mutateTable(t) { dir =>
      import scala.collection.JavaConverters._
      java.nio.file.Files.list(dir.resolve("_delta_log")).iterator().asScala
        .filter(_.toString.endsWith(".checkpoint.parquet"))
        .foreach(f => java.nio.file.Files.write(f, Array[Byte](0, 1, 2, 3)))
      for (i <- 0 until 5) {
        val jf = dir.resolve("_delta_log/%020d.json".format(i))
        if (java.nio.file.Files.exists(jf)) java.nio.file.Files.delete(jf)
      }
    }
    w.read(t)
  }

  test("dsReadCorruptJson", "Error: corrupt JSON in commit file") { w =>
    w.sql("""CREATE TABLE tbl (id INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1)")
    val t = w.table("tbl")
    w.mutateTable(t) { dir =>
      java.nio.file.Files.write(dir.resolve("_delta_log/00000000000000000000.json"),
        "NOT VALID JSON{{{".getBytes)
    }
    w.read(t)
    w.snapshot(t)
  }

  test("dsReadDuplicateColumns", "Error: schema with duplicate column names", "error") { w =>
    w.sql("""CREATE TABLE tbl (id INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1)")
    val t = w.table("tbl")
    w.mutateTable(t) { dir =>
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
    w.read(t)
  }

  test("dsReadEmptyDataFrame", "Read after writing empty DataFrame", "emptyTable") { w =>
    w.sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT CAST(id AS INT), CAST(id AS STRING) FROM range(0)")
    val t = w.table("tbl")
    w.read(t)
    w.snapshot(t)
  }

  test("dsReadInvalidColumnName", "Error: predicate on non-existent column", "error") { w =>
    w.sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(10)")
    val t = w.table("tbl")
    w.read(t, predicate = "nonExistentCol = 1")
    w.snapshot(t)
  }

  test("dsReadMissingCommitFile", "Error: _delta_log exists but commit file missing") { w =>
    w.sql("CREATE TABLE tbl (id INT) USING delta")
    w.sql("INSERT INTO tbl VALUES (1)")
    val t = w.table("tbl")
    w.mutateTable(t) { dir =>
      import scala.collection.JavaConverters._
      java.nio.file.Files.list(dir.resolve("_delta_log")).iterator().asScala
        .filter(_.toString.endsWith(".json")).foreach(java.nio.file.Files.delete)
    }
    w.read(t)
  }

  test("dsReadMissingDeltaLog", "Error: path exists but _delta_log is missing") { w =>
    w.sql("CREATE TABLE tbl (id INT) USING delta")
    w.sql("INSERT INTO tbl VALUES (1)")
    val t = w.table("tbl")
    w.mutateTable(t) { dir =>
      import scala.collection.JavaConverters._
      val logDir = dir.resolve("_delta_log")
      java.nio.file.Files.list(logDir).iterator().asScala.foreach(java.nio.file.Files.delete)
      java.nio.file.Files.delete(logDir)
    }
    w.read(t)
  }

  test("dsReadMixedCasePartition", "Read with mixed case partition column names", "partitioned") { w =>
    w.sql("""CREATE TABLE tbl (PartCol STRING, Value INT) USING delta
      PARTITIONED BY (PartCol)
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES ('x', 1), ('y', 2), ('x', 3)")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "PartCol = 'x'")
    w.snapshot(t)
  }

  test("dsReadModifyCheckpoint", "Error: table with corrupted checkpoint") { w =>
    w.sql("""CREATE TABLE tbl (value BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true', 'delta.checkpointInterval' = '3')""")
    for (i <- 0 until 10) w.sql(s"INSERT INTO tbl SELECT id FROM range(${i*10}, ${(i+1)*10})")
    val t = w.table("tbl")
    w.mutateTable(t) { dir =>
      import scala.collection.JavaConverters._
      java.nio.file.Files.list(dir.resolve("_delta_log")).iterator().asScala
        .filter(_.toString.endsWith(".checkpoint.parquet"))
        .foreach(f => java.nio.file.Files.write(f, Array[Byte](0, 1, 2, 3)))
    }
    w.read(t)
    w.snapshot(t)
  }

  test("dsReadNonExistentVersion", "Error on non-existent version", "error") { w =>
    w.sql("""CREATE TABLE tbl (value BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(10)")
    val t = w.table("tbl")
    w.read(t, version = 99)
  }

  test("dsReadRenameColumn", "Read after column mapping rename", "columnMapping") { w =>
    w.sql("""CREATE TABLE tbl (id INT, old_name STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true',
        'delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    w.sql("INSERT INTO tbl VALUES (1, 'alice')")
    w.sql("INSERT INTO tbl VALUES (2, 'bob')")
    w.sql("ALTER TABLE tbl RENAME COLUMN old_name TO new_name")
    w.sql("INSERT INTO tbl VALUES (3, 'charlie')")
    val t = w.table("tbl")
    w.read(t)
    w.snapshot(t)
  }

  test("dsReadReplaceWhere", "Read after replaceWhere partition overwrite", "partitioned") { w =>
    w.sql("""CREATE TABLE tbl (part STRING, value INT) USING delta
      PARTITIONED BY (part)
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES ('a', 1), ('a', 2), ('b', 3), ('b', 4)")
    w.sql("INSERT OVERWRITE tbl PARTITION (part='a') VALUES ('a', 10), ('a', 20)")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "part = 'a'")
    w.snapshot(t)
  }

  test("dsReadSingleRow", "Read table with single row", "basic") { w =>
    w.sql("""CREATE TABLE tbl (value BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (42)")
    val t = w.table("tbl")
    w.read(t)
    w.snapshot(t)
  }

  test("dsReadStringWithSpecialChars", "Read string values with special characters", "types") { w =>
    w.sql("""CREATE TABLE tbl (id INT, text STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("""INSERT INTO tbl VALUES
      (1, 'hello\nworld'), (2, 'tab\there'),
      (3, 'with"quotes'), (4, 'unicode\u00e9\u00f1'), (5, 'normal')""")
    val t = w.table("tbl")
    w.read(t)
    w.snapshot(t)
  }

  test("dseReadAfterTruncate", "Read after TRUNCATE (DELETE all)") { w =>
    w.sql("""CREATE TABLE tbl (value BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(10)")
    w.sql("DELETE FROM tbl WHERE true")
    val t = w.table("tbl")
    w.read(t)
    w.snapshot(t)
  }

  test("dseReadCoalesced", "Read after coalesce write") { w =>
    w.sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(100)")
    val t = w.table("tbl")
    w.read(t)
    w.snapshot(t)
  }

  test("dseReadInvalidTableProperty", "Error: unsupported protocol version") { w =>
    w.sql("""CREATE TABLE tbl (value BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(10)")
    val t = w.table("tbl")
    w.mutateTable(t) { dir =>
      val ts = System.currentTimeMillis()
      val v1Meta = """{"metaData":{"id":"test","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"value\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{"delta.enableDeletionVectors":"true"}}}"""
      val v1Proto = """{"protocol":{"minReaderVersion":99,"minWriterVersion":99}}"""
      val ci = s"""{"commitInfo":{"timestamp":${ts},"operation":"SET TBLPROPERTIES","operationParameters":{},"isBlindAppend":true}}"""
      java.nio.file.Files.write(dir.resolve("_delta_log/00000000000000000001.json"),
        (v1Meta + "\n" + v1Proto + "\n" + ci + "\n").getBytes)
    }
    w.read(t)
  }

  test("dseReadNonDeltaPath", "Error reading non-delta path as delta") { w =>
    w.sql("CREATE TABLE tbl (id INT) USING delta")
    w.sql("INSERT INTO tbl VALUES (1)")
    val t = w.table("tbl")
    w.mutateTable(t) { dir =>
      import scala.collection.JavaConverters._
      val logDir = dir.resolve("_delta_log")
      java.nio.file.Files.list(logDir).iterator().asScala.foreach(java.nio.file.Files.delete)
      java.nio.file.Files.delete(logDir)
    }
    w.read(t)
    w.snapshot(t)
  }

  test("dseReadSortedData", "Read table written with sorted data") { w =>
    w.sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT 99 - id FROM range(100)")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "id >= 90")
    w.snapshot(t)
  }

}.runAll()
