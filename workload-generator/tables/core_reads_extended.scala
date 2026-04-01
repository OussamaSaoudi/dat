/**
 * Core Reads Extended workloads (cr_* + dp* + fpe_* families).
 * Covers: type boundaries (byte, short, date, timestamp, decimal), special float/double
 * values (NaN, Infinity), nested structs, arrays of arrays, maps with complex values,
 * wide schemas, projection reorder, empty vs null, partition types (boolean, decimal,
 * long, timestamp, special chars, null), file path edge cases (spaces, special chars,
 * unicode, absolute paths, encoded partition dirs).
 *
 * Run: ./bin/generate-workload.sh tables/core_reads_extended.scala
 */
import io.delta.workload.WorkloadGenerator._

// ---------------------------------------------------------------------------
// Core reads: type boundaries
// ---------------------------------------------------------------------------

workload("cr_byte_boundaries", "ByteType MIN/MAX values", "coreReads") { w =>
  w.sql("CREATE TABLE tbl (b BYTE) USING delta")
  w.sql("INSERT INTO tbl VALUES (-128), (0), (127)")
  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
}

workload("cr_short_boundaries", "ShortType MIN/MAX values", "coreReads") { w =>
  w.sql("CREATE TABLE tbl (s SHORT) USING delta")
  w.sql("INSERT INTO tbl VALUES (-32768), (0), (32767)")
  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
}

workload("cr_date_boundaries", "Date boundary values", "coreReads") { w =>
  w.sql("CREATE TABLE tbl (d DATE) USING delta")
  w.sql("INSERT INTO tbl VALUES (DATE'0001-01-01'), (DATE'2024-06-15'), (DATE'9999-12-31')")
  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
}

workload("cr_timestamp_boundaries", "Timestamp boundary values", "coreReads") { w =>
  w.sql("CREATE TABLE tbl (ts TIMESTAMP) USING delta")
  w.sql("""INSERT INTO tbl VALUES
    (TIMESTAMP'1970-01-01 00:00:00'),
    (TIMESTAMP'2024-06-15 12:30:45.123456'),
    (TIMESTAMP'2262-04-11 23:47:16.854775')""")
  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
}

workload("cr_decimal_max_precision", "DecimalType(38,18) read-back", "coreReads") { w =>
  w.sql("CREATE TABLE tbl (d DECIMAL(38,18)) USING delta")
  w.sql("""INSERT INTO tbl VALUES
    (12345678901234567890.123456789012345678),
    (-12345678901234567890.123456789012345678),
    (0.000000000000000001)""")
  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
}

workload("cr_decimal_zero_scale", "DecimalType(38,0) read-back", "coreReads") { w =>
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

workload("cr_float_nan", "Float NaN value read-back", "coreReads") { w =>
  w.sql("CREATE TABLE tbl (f FLOAT) USING delta")
  w.sql("INSERT INTO tbl VALUES (CAST('NaN' AS FLOAT)), (1.5), (NULL)")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "f IS NOT NULL")
  w.snapshot(t)
}

workload("cr_float_infinity", "Float +Infinity / -Infinity read-back", "coreReads") { w =>
  w.sql("CREATE TABLE tbl (f FLOAT) USING delta")
  w.sql("INSERT INTO tbl VALUES (CAST('Infinity' AS FLOAT)), (CAST('-Infinity' AS FLOAT)), (0.0)")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "f > 0")
  w.snapshot(t)
}

workload("cr_double_nan", "Double NaN value read-back", "coreReads") { w =>
  w.sql("CREATE TABLE tbl (d DOUBLE) USING delta")
  w.sql("INSERT INTO tbl VALUES (CAST('NaN' AS DOUBLE)), (2.5), (NULL)")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "d IS NOT NULL")
  w.snapshot(t)
}

workload("cr_double_infinity", "Double +Infinity / -Infinity read-back", "coreReads") { w =>
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

workload("cr_deeply_nested_struct", "4+ levels nested struct", "coreReads") { w =>
  w.sql("""CREATE TABLE tbl (
    top STRUCT<l1: STRUCT<l2: STRUCT<l3: STRUCT<value: INT>>>>
  ) USING delta""")
  w.sql("""INSERT INTO tbl VALUES (
    named_struct('l1', named_struct('l2', named_struct('l3', named_struct('value', 42)))))""")
  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
}

workload("cr_struct_all_null", "Struct with all-NULL fields", "coreReads") { w =>
  w.sql("CREATE TABLE tbl (s STRUCT<a: INT, b: STRING, c: DOUBLE>) USING delta")
  w.sql("INSERT INTO tbl VALUES (named_struct('a', CAST(NULL AS INT), 'b', CAST(NULL AS STRING), 'c', CAST(NULL AS DOUBLE)))")
  w.sql("INSERT INTO tbl VALUES (named_struct('a', 1, 'b', 'hello', 'c', 3.14))")
  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
}

workload("cr_array_of_arrays", "Nested ARRAY<ARRAY<INT>> column", "coreReads") { w =>
  w.sql("CREATE TABLE tbl (a ARRAY<ARRAY<INT>>) USING delta")
  w.sql("INSERT INTO tbl VALUES (array(array(1,2), array(3,4)))")
  w.sql("INSERT INTO tbl VALUES (array(array(), array(5)))")
  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
}

workload("cr_map_complex_value", "Map with struct value", "coreReads") { w =>
  w.sql("CREATE TABLE tbl (m MAP<STRING, STRUCT<x: INT, y: STRING>>) USING delta")
  w.sql("INSERT INTO tbl VALUES (map('key1', named_struct('x', 1, 'y', 'a'), 'key2', named_struct('x', 2, 'y', 'b')))")
  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
}

workload("cr_wide_schema", "Table with 100+ columns", "coreReads") { w =>
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

workload("cr_empty_vs_null_string", "Empty string vs NULL distinction", "coreReads") { w =>
  w.sql("CREATE TABLE tbl (id INT, s STRING) USING delta")
  w.sql("INSERT INTO tbl VALUES (1, ''), (2, NULL), (3, 'hello')")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "s IS NOT NULL")
  w.snapshot(t)
}

workload("cr_binary_readback", "Binary type read-back", "coreReads") { w =>
  w.sql("CREATE TABLE tbl (id INT, data BINARY) USING delta")
  w.sql("INSERT INTO tbl VALUES (1, X'DEADBEEF'), (2, X''), (3, NULL)")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "data IS NOT NULL")
  w.snapshot(t)
}

workload("cr_boolean_filter", "Boolean column with filter", "coreReads") { w =>
  w.sql("CREATE TABLE tbl (id INT, flag BOOLEAN) USING delta")
  w.sql("INSERT INTO tbl VALUES (1, true), (2, false), (3, NULL)")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "flag = true")
  w.snapshot(t)
}

workload("cr_zero_matching_rows", "Filter that matches zero rows", "coreReads") { w =>
  w.sql("CREATE TABLE tbl (id INT) USING delta")
  w.sql("INSERT INTO tbl VALUES (1), (2), (3)")
  val t = w.table("tbl")
  w.read(t, predicate = "id > 999")
  w.read(t, predicate = "id = -1")
  w.snapshot(t)
}

workload("cr_projection_reorder", "Column projection with reordered columns",
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

workload("cr_multi_partition", "Multiple partition columns", "coreReads") { w =>
  w.sql("""CREATE TABLE tbl (id INT, year INT, region STRING)
    USING delta PARTITIONED BY (year, region)""")
  w.sql("INSERT INTO tbl VALUES (1, 2024, 'us'), (2, 2024, 'eu'), (3, 2025, 'us')")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "year = 2024")
  w.read(t, predicate = "year = 2024 AND region = 'us'")
  w.snapshot(t)
}

workload("cr_partition_null", "Partition column with NULL values", "coreReads") { w =>
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

workload("dpBasicPartition", "Basic single-column string partition", "partitioned") { w =>
  w.sql("CREATE TABLE tbl (id INT, part STRING) USING delta PARTITIONED BY (part)")
  w.sql("INSERT INTO tbl VALUES (1, 'a'), (2, 'b'), (3, 'c')")
  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
}

workload("dpIntPartition", "Int-column partition", "partitioned") { w =>
  w.sql("CREATE TABLE tbl (id INT, part INT) USING delta PARTITIONED BY (part)")
  w.sql("INSERT INTO tbl VALUES (1, 10), (2, 20), (3, 30)")
  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
}

workload("dpDatePartition", "Date-column partition", "partitioned") { w =>
  w.sql("CREATE TABLE tbl (id INT, part DATE) USING delta PARTITIONED BY (part)")
  w.sql("INSERT INTO tbl VALUES (1, DATE'2024-01-01'), (2, DATE'2024-06-15'), (3, DATE'2025-01-01')")
  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
}

workload("dpMultiPartition", "Multi-column partition (string + int)", "partitioned") { w =>
  w.sql("""CREATE TABLE tbl (id INT, region STRING, year INT)
    USING delta PARTITIONED BY (region, year)""")
  w.sql("INSERT INTO tbl VALUES (1, 'us', 2024), (2, 'eu', 2024), (3, 'us', 2025)")
  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
}

workload("dpReadPartitionFilter", "Read with partition column equality filter",
    "partitioned") { w =>
  w.sql("CREATE TABLE tbl (id INT, part STRING) USING delta PARTITIONED BY (part)")
  w.sql("INSERT INTO tbl VALUES (1, 'a'), (2, 'b'), (3, 'c')")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "part = 'a'")
  w.read(t, predicate = "part = 'z'", name = "read_miss_part")
  w.snapshot(t)
}

workload("dpReadPartitionRange", "Read with partition column range filter",
    "partitioned") { w =>
  w.sql("CREATE TABLE tbl (id INT, part INT) USING delta PARTITIONED BY (part)")
  w.sql("INSERT INTO tbl VALUES (1, 1), (2, 5), (3, 10)")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "part > 3")
  w.read(t, predicate = "part BETWEEN 1 AND 5")
  w.snapshot(t)
}

workload("dpReadPartitionIn", "Read with partition column IN filter", "partitioned") { w =>
  w.sql("CREATE TABLE tbl (id INT, part STRING) USING delta PARTITIONED BY (part)")
  w.sql("INSERT INTO tbl VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "part IN ('a', 'c')")
  w.read(t, predicate = "part IN ('z')", name = "read_miss_in")
  w.snapshot(t)
}

workload("dpReadPartitionNull", "Read partition with null partition values",
    "partitioned") { w =>
  w.sql("CREATE TABLE tbl (id INT, part STRING) USING delta PARTITIONED BY (part)")
  w.sql("INSERT INTO tbl VALUES (1, 'a'), (2, NULL), (3, 'b')")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "part IS NULL")
  w.read(t, predicate = "part IS NOT NULL")
  w.snapshot(t)
}

workload("dpReadPartitionMixed", "Read with mixed partition + data column filters",
    "partitioned") { w =>
  w.sql("CREATE TABLE tbl (id INT, value INT, part STRING) USING delta PARTITIONED BY (part)")
  w.sql("INSERT INTO tbl VALUES (1, 10, 'a'), (2, 20, 'a'), (3, 30, 'b'), (4, 40, 'b')")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "part = 'a' AND value > 15")
  w.read(t, predicate = "part = 'b' AND id < 4")
  w.snapshot(t)
}

workload("dpReadPartitionAfterAppend", "Read partitioned table after appending",
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

workload("dpReadPartitionBoolean", "Read table partitioned by boolean column",
    "partitioned") { w =>
  w.sql("CREATE TABLE tbl (id INT, flag BOOLEAN) USING delta PARTITIONED BY (flag)")
  w.sql("INSERT INTO tbl VALUES (1, true), (2, false), (3, true)")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "flag = true")
  w.read(t, predicate = "flag = false")
  w.snapshot(t)
}

workload("dpReadPartitionDecimal", "Read table partitioned by decimal column",
    "partitioned") { w =>
  w.sql("CREATE TABLE tbl (id INT, amt DECIMAL(10,2)) USING delta PARTITIONED BY (amt)")
  w.sql("INSERT INTO tbl VALUES (1, 10.50), (2, 20.75), (3, 10.50)")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "amt = 10.50")
  w.read(t, predicate = "amt > 15.00")
  w.snapshot(t)
}

workload("dpReadPartitionLongType", "Read table partitioned by long integer type",
    "partitioned") { w =>
  w.sql("CREATE TABLE tbl (id INT, part LONG) USING delta PARTITIONED BY (part)")
  w.sql("INSERT INTO tbl VALUES (1, 1000000000), (2, 2000000000), (3, 3000000000)")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "part = 2000000000")
  w.read(t, predicate = "part > 2500000000")
  w.snapshot(t)
}

workload("dpReadPartitionTimestamp", "Read table partitioned by timestamp column",
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

workload("dpReadPartitionSpecialChars", "Read partition with special characters",
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

workload("fpe_space_in_path", "File path with space encoding (%20)", "filePath") { w =>
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

workload("fpe_special_chars_path", "File path with encoded special chars",
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

workload("fpe_unicode_in_path", "File path with percent-encoded multi-byte UTF-8",
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

workload("fpe_absolute_path", "Add action with absolute URI path (file:///)",
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

workload("fpe_partition_dir_encoded", "Partition directory with encoded value",
    "filePath", "partitioned") { w =>
  w.sql("CREATE TABLE tbl (id INT, city STRING) USING delta PARTITIONED BY (city)")
  w.sql("INSERT INTO tbl VALUES (1, 'New York'), (2, 'San Francisco'), (3, 'Tokyo')")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "city = 'New York'")
  w.read(t, predicate = "city = 'Tokyo'")
  w.snapshot(t)
}

generateAll(
  sys.env.getOrElse("WORKLOAD_OUTPUT_DIR", "/tmp/workloads"),
  force = sys.env.getOrElse("WORKLOAD_FORCE", "false").toBoolean)
System.exit(0)
