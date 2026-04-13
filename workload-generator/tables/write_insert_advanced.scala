/**
 * Advanced insert workloads: CopyInto, ReplaceWhere, InsertOverwrite, and
 * InsertSchemaEvolution, converted 1-to-1 from runtime capture suites.
 *
 * Sources:
 *   - CopyIntoWriteCaptureSuite.scala (CI-001 through CI-015)
 *   - ReplaceWhereWriteCaptureSuite.scala (RW-001 through RW-015)
 *   - InsertOverwriteWriteCaptureSuite.scala (IO-001 through IO-015)
 *   - InsertSchemaEvolutionWriteCaptureSuite.scala (SE-001 through SE-015)
 */

new WorkloadSuite("write_insert_advanced") {

  // ==========================================================================
  // CopyInto Tests (CI-*)
  // Source: CopyIntoWriteCaptureSuite.scala / CopyIntoSuite.scala
  //
  // Note: COPY INTO is NOT supported as a structured operation.
  // All CI-* tests are commented out because COPY INTO cannot be converted
  // to insertOp - it requires loading external files which is not supported.
  // ==========================================================================

  // CI-001: Basic COPY INTO from parquet
  // COMMENTED OUT: COPY INTO is not supported as a structured operation
  // test("CI_001_copy_into_parquet", "COPY INTO from parquet source - basic load",
  //     "write", "copyInto", "parquet") {
  //   sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
  //   // Stage source parquet data
  //   sql("CREATE TABLE source_ci001 (id INT, name STRING) USING parquet")
  //   sql("INSERT INTO source_ci001 VALUES (1,'a'),(2,'b'),(3,'c')")
  //   sql("COPY INTO tbl FROM (SELECT * FROM source_ci001) FILEFORMAT = PARQUET")
  //   val t = registerTable("tbl")
  //   writeSpec(t)
  //   read(t, name = "read_all")
  //   snapshot(t)
  // }

  // CI-002: COPY INTO from CSV
  // COMMENTED OUT: COPY INTO is not supported as a structured operation
  // test("CI_002_copy_into_csv", "COPY INTO from CSV with header",
  //     "write", "copyInto", "csv") {
  //   sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
  //   sql("CREATE TABLE source_ci002 (id INT, name STRING) USING csv OPTIONS (header 'true')")
  //   sql("INSERT INTO source_ci002 VALUES (1,'alice'),(2,'bob')")
  //   sql("COPY INTO tbl FROM (SELECT * FROM source_ci002) FILEFORMAT = CSV FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true')")
  //   val t = registerTable("tbl")
  //   writeSpec(t)
  //   read(t, name = "read_all")
  //   snapshot(t)
  // }

  // CI-003: COPY INTO from JSON
  // COMMENTED OUT: COPY INTO is not supported as a structured operation
  // test("CI_003_copy_into_json", "COPY INTO from JSON source",
  //     "write", "copyInto", "json") {
  //   sql("CREATE TABLE tbl (id BIGINT, name STRING) USING delta")
  //   sql("CREATE TABLE source_ci003 (id BIGINT, name STRING) USING json")
  //   sql("INSERT INTO source_ci003 VALUES (1,'x'),(2,'y'),(3,'z')")
  //   sql("COPY INTO tbl FROM (SELECT * FROM source_ci003) FILEFORMAT = JSON")
  //   val t = registerTable("tbl")
  //   writeSpec(t)
  //   read(t, name = "read_all")
  //   snapshot(t)
  // }

  // CI-004: COPY INTO with schema evolution (mergeSchema)
  // COMMENTED OUT: COPY INTO is not supported as a structured operation
  // test("CI_004_copy_into_merge_schema", "COPY INTO with mergeSchema adding new column",
  //     "write", "copyInto", "schemaEvolution") {
  //   sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
  //   sql("INSERT INTO tbl VALUES (1,'a')")
  //   // Source has extra column 'score'
  //   sql("CREATE TABLE source_ci004 (id INT, name STRING, score INT) USING parquet")
  //   sql("INSERT INTO source_ci004 VALUES (2,'b',100),(3,'c',200)")
  //   sql("COPY INTO tbl FROM (SELECT * FROM source_ci004) FILEFORMAT = PARQUET COPY_OPTIONS ('mergeSchema' = 'true')")
  //   val t = registerTable("tbl")
  //   writeSpec(t)
  //   read(t, name = "read_all")
  //   snapshot(t)
  // }

  // CI-005: COPY INTO with CSV format options (header, delimiter)
  // COMMENTED OUT: COPY INTO is not supported as a structured operation
  // test("CI_005_copy_into_csv_options", "COPY INTO with pipe delimiter",
  //     "write", "copyInto", "csv", "formatOptions") {
  //   sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
  //   sql("CREATE TABLE source_ci005 (id INT, name STRING) USING csv OPTIONS (header 'true', delimiter '|')")
  //   sql("INSERT INTO source_ci005 VALUES (1,'alice'),(2,'bob')")
  //   sql("COPY INTO tbl FROM (SELECT * FROM source_ci005) FILEFORMAT = CSV FORMAT_OPTIONS ('header' = 'true', 'delimiter' = '|', 'inferSchema' = 'true')")
  //   val t = registerTable("tbl")
  //   writeSpec(t)
  //   read(t, name = "read_all")
  //   snapshot(t)
  // }

  // CI-006: COPY INTO idempotent (duplicate files skipped)
  // COMMENTED OUT: COPY INTO is not supported as a structured operation
  // test("CI_006_copy_into_idempotent", "COPY INTO twice from same source - second is no-op",
  //     "write", "copyInto", "idempotent") {
  //   sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
  //   sql("CREATE TABLE source_ci006 (id INT, name STRING) USING parquet")
  //   sql("INSERT INTO source_ci006 VALUES (1,'a'),(2,'b')")
  //   sql("COPY INTO tbl FROM (SELECT * FROM source_ci006) FILEFORMAT = PARQUET")
  //   sql("COPY INTO tbl FROM (SELECT * FROM source_ci006) FILEFORMAT = PARQUET")
  //   val t = registerTable("tbl")
  //   writeSpec(t)
  //   read(t, name = "read_all")
  //   snapshot(t)
  // }

  // CI-007: COPY INTO into partitioned table
  // COMMENTED OUT: COPY INTO is not supported as a structured operation
  // test("CI_007_copy_into_partitioned", "COPY INTO partitioned table",
  //     "write", "copyInto", "partitioned") {
  //   sql("CREATE TABLE tbl (id INT, name STRING, part STRING) USING delta PARTITIONED BY (part)")
  //   sql("CREATE TABLE source_ci007 (id INT, name STRING, part STRING) USING parquet")
  //   sql("INSERT INTO source_ci007 VALUES (1,'a','p1'),(2,'b','p2'),(3,'c','p1')")
  //   sql("COPY INTO tbl FROM (SELECT * FROM source_ci007) FILEFORMAT = PARQUET")
  //   val t = registerTable("tbl")
  //   writeSpec(t)
  //   read(t, name = "read_all")
  //   read(t, predicate = "part = 'p1'", name = "read_p1")
  //   read(t, predicate = "part = 'p2'", name = "read_p2")
  //   snapshot(t)
  // }

  // CI-008: COPY INTO with column mapping
  // COMMENTED OUT: COPY INTO is not supported as a structured operation
  // test("CI_008_copy_into_column_mapping", "COPY INTO with column mapping name mode",
  //     "write", "copyInto", "columnMapping") {
  //   sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
  //     TBLPROPERTIES (
  //       'delta.columnMapping.mode' = 'name',
  //       'delta.minReaderVersion' = '2',
  //       'delta.minWriterVersion' = '5'
  //     )""")
  //   sql("CREATE TABLE source_ci008 (id INT, name STRING) USING parquet")
  //   sql("INSERT INTO source_ci008 VALUES (1,'alice'),(2,'bob')")
  //   sql("COPY INTO tbl FROM (SELECT * FROM source_ci008) FILEFORMAT = PARQUET")
  //   val t = registerTable("tbl")
  //   writeSpec(t)
  //   read(t, name = "read_all")
  //   snapshot(t)
  // }

  // CI-009: COPY INTO with subset of columns
  // COMMENTED OUT: COPY INTO is not supported as a structured operation
  // test("CI_009_copy_into_subset_columns", "COPY INTO with source having fewer columns",
  //     "write", "copyInto", "subsetColumns") {
  //   sql("CREATE TABLE tbl (id INT, name STRING, score DOUBLE) USING delta")
  //   sql("CREATE TABLE source_ci009 (id INT, name STRING) USING parquet")
  //   sql("INSERT INTO source_ci009 VALUES (1,'a'),(2,'b')")
  //   sql("COPY INTO tbl FROM (SELECT * FROM source_ci009) FILEFORMAT = PARQUET COPY_OPTIONS ('mergeSchema' = 'false')")
  //   val t = registerTable("tbl")
  //   writeSpec(t)
  //   read(t, name = "read_all")
  //   snapshot(t)
  // }

  // CI-010: COPY INTO multiple batches
  // COMMENTED OUT: COPY INTO is not supported as a structured operation
  // test("CI_010_copy_into_multi_batch", "Two sequential COPY INTO from different sources",
  //     "write", "copyInto", "multiBatch") {
  //   sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
  //   sql("CREATE TABLE source_ci010a (id INT, name STRING) USING parquet")
  //   sql("INSERT INTO source_ci010a VALUES (1,'a'),(2,'b')")
  //   sql("COPY INTO tbl FROM (SELECT * FROM source_ci010a) FILEFORMAT = PARQUET")
  //   sql("CREATE TABLE source_ci010b (id INT, name STRING) USING parquet")
  //   sql("INSERT INTO source_ci010b VALUES (3,'c'),(4,'d')")
  //   sql("COPY INTO tbl FROM (SELECT * FROM source_ci010b) FILEFORMAT = PARQUET")
  //   val t = registerTable("tbl")
  //   writeSpec(t)
  //   read(t, name = "read_all")
  //   snapshot(t)
  // }

  // CI-011: COPY INTO into empty table
  // COMMENTED OUT: COPY INTO is not supported as a structured operation
  // test("CI_011_copy_into_empty_table", "COPY INTO empty table - first data load",
  //     "write", "copyInto", "emptyTable") {
  //   sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
  //   sql("CREATE TABLE source_ci011 (id INT, name STRING) USING parquet")
  //   sql("INSERT INTO source_ci011 VALUES (1,'first'),(2,'second'),(3,'third')")
  //   sql("COPY INTO tbl FROM (SELECT * FROM source_ci011) FILEFORMAT = PARQUET")
  //   val t = registerTable("tbl")
  //   writeSpec(t)
  //   read(t, name = "read_all")
  //   snapshot(t)
  // }

  // CI-012: COPY INTO from directory with multiple files
  // COMMENTED OUT: COPY INTO is not supported as a structured operation
  // test("CI_012_copy_into_multi_files", "COPY INTO from directory with multiple parquet files",
  //     "write", "copyInto", "multipleFiles") {
  //   sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
  //   // Multiple inserts into source create multiple files
  //   sql("CREATE TABLE source_ci012 (id INT, name STRING) USING parquet")
  //   sql("INSERT INTO source_ci012 VALUES (1,'a'),(2,'b')")
  //   sql("INSERT INTO source_ci012 VALUES (3,'c'),(4,'d')")
  //   sql("INSERT INTO source_ci012 VALUES (5,'e')")
  //   sql("COPY INTO tbl FROM (SELECT * FROM source_ci012) FILEFORMAT = PARQUET")
  //   val t = registerTable("tbl")
  //   writeSpec(t)
  //   read(t, name = "read_all")
  //   snapshot(t)
  // }

  // CI-013: COPY INTO with BIGINT column
  // COMMENTED OUT: COPY INTO is not supported as a structured operation
  // test("CI_013_copy_into_bigint", "COPY INTO with BIGINT column matching source",
  //     "write", "copyInto", "bigint") {
  //   sql("CREATE TABLE tbl (id BIGINT, name STRING) USING delta")
  //   sql("CREATE TABLE source_ci013 (id BIGINT, name STRING) USING parquet")
  //   sql("INSERT INTO source_ci013 VALUES (1,'a'),(2,'b')")
  //   sql("COPY INTO tbl FROM (SELECT * FROM source_ci013) FILEFORMAT = PARQUET COPY_OPTIONS ('mergeSchema' = 'false')")
  //   val t = registerTable("tbl")
  //   writeSpec(t)
  //   read(t, name = "read_all")
  //   snapshot(t)
  // }

  // CI-014: COPY INTO with nested struct
  // COMMENTED OUT: COPY INTO is not supported as a structured operation
  // test("CI_014_copy_into_nested_struct", "COPY INTO with nested struct data",
  //     "write", "copyInto", "nestedStruct") {
  //   sql("CREATE TABLE tbl (id INT, info STRUCT<name: STRING, age: INT>) USING delta")
  //   sql("CREATE TABLE source_ci014 (id INT, info STRUCT<name: STRING, age: INT>) USING parquet")
  //   sql("INSERT INTO source_ci014 VALUES (1, named_struct('name','alice','age',30)),(2, named_struct('name','bob','age',25))")
  //   sql("COPY INTO tbl FROM (SELECT * FROM source_ci014) FILEFORMAT = PARQUET")
  //   val t = registerTable("tbl")
  //   writeSpec(t)
  //   read(t, name = "read_all")
  //   snapshot(t)
  // }

  // CI-015: COPY INTO append semantics
  // COMMENTED OUT: COPY INTO is not supported as a structured operation
  // test("CI_015_copy_into_append", "COPY INTO preserves existing data",
  //     "write", "copyInto", "append") {
  //   sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
  //   sql("INSERT INTO tbl VALUES (1,'existing1'),(2,'existing2')")
  //   sql("CREATE TABLE source_ci015 (id INT, name STRING) USING parquet")
  //   sql("INSERT INTO source_ci015 VALUES (3,'new1'),(4,'new2')")
  //   sql("COPY INTO tbl FROM (SELECT * FROM source_ci015) FILEFORMAT = PARQUET")
  //   val t = registerTable("tbl")
  //   writeSpec(t)
  //   read(t, name = "read_all")
  //   read(t, version = 1, name = "read_before_copy")
  //   snapshot(t)
  // }

  // ==========================================================================
  // ReplaceWhere Tests (RW-*)
  // Source: ReplaceWhereWriteCaptureSuite.scala / DeltaInsertReplaceWhereSuite.scala
  // ==========================================================================

  // RW-001: ReplaceWhere with partition predicate
  test("RW_001_replacewhere_partition", "ReplaceWhere partition p1",
      "write", "replaceWhere", "overwrite", "partitioned") {
    sql("CREATE TABLE tbl (key INT, value STRING, part STRING) USING delta PARTITIONED BY (part)")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("key" -> 1, "value" -> "a", "part" -> "p1"),
      Map("key" -> 2, "value" -> "b", "part" -> "p1"),
      Map("key" -> 3, "value" -> "c", "part" -> "p2"),
      Map("key" -> 4, "value" -> "d", "part" -> "p2")))
    sql("INSERT INTO tbl REPLACE WHERE part = 'p1' SELECT 10 as key, 'x' as value, 'p1' as part UNION ALL SELECT 20, 'y', 'p1'")
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    read(t, predicate = "part = 'p1'", name = "read_p1")
    read(t, predicate = "part = 'p2'", name = "read_p2_unchanged")
    read(t, version = 1, name = "read_before_replace")
    snapshot(t)
  }

  // RW-002: ReplaceWhere with data predicate (non-partitioned)
  test("RW_002_replacewhere_data_predicate", "ReplaceWhere on non-partitioned table key <= 2",
      "write", "replaceWhere", "overwrite") {
    sql("CREATE TABLE tbl (key INT, value INT) USING delta")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("key" -> 1, "value" -> 10),
      Map("key" -> 2, "value" -> 20),
      Map("key" -> 3, "value" -> 30),
      Map("key" -> 4, "value" -> 40),
      Map("key" -> 5, "value" -> 50)))
    sql("INSERT INTO tbl REPLACE WHERE key <= 2 SELECT * FROM VALUES (1,100),(2,200) AS t(key, value)")
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    read(t, predicate = "key <= 2", name = "read_replaced")
    read(t, version = 1, name = "read_before_replace")
    snapshot(t)
  }

  // RW-003: INSERT OVERWRITE whole table (replace all)
  test("RW_003_overwrite_all", "INSERT OVERWRITE replaces all rows",
      "write", "overwrite") {
    sql("CREATE TABLE tbl (key INT, value STRING) USING delta")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("key" -> 1, "value" -> "a"),
      Map("key" -> 2, "value" -> "b"),
      Map("key" -> 3, "value" -> "c")))
    sql("INSERT OVERWRITE tbl VALUES (10,'x'),(20,'y')")
    val t = registerWriteSpec(w)
    read(t, name = "read_after_overwrite")
    read(t, version = 1, name = "read_before_overwrite")
    snapshot(t)
  }

  // RW-004: Dynamic partition overwrite
  test("RW_004_dynamic_partition_overwrite", "Dynamic partition overwrite - only p1 replaced",
      "write", "overwrite", "dynamicPartition", "partitioned") {
    sql("CREATE TABLE tbl (key INT, value STRING, part STRING) USING delta PARTITIONED BY (part)")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("key" -> 1, "value" -> "a", "part" -> "p1"),
      Map("key" -> 2, "value" -> "b", "part" -> "p1"),
      Map("key" -> 3, "value" -> "c", "part" -> "p2"),
      Map("key" -> 4, "value" -> "d", "part" -> "p2")))
    // Dynamic partition overwrite: only replace partitions present in source
    sql("SET spark.sql.sources.partitionOverwriteMode=dynamic")
    sql("INSERT OVERWRITE tbl PARTITION (part) VALUES (10,'x','p1')")
    sql("SET spark.sql.sources.partitionOverwriteMode=static")
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    read(t, predicate = "part = 'p1'", name = "read_p1_replaced")
    read(t, predicate = "part = 'p2'", name = "read_p2_unchanged")
    snapshot(t)
  }

  // RW-005: ReplaceWhere matching no existing rows
  test("RW_005_replacewhere_no_match", "ReplaceWhere matching no rows - new data added",
      "write", "replaceWhere", "overwrite") {
    sql("CREATE TABLE tbl (key INT, value INT) USING delta")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("key" -> 1, "value" -> 10),
      Map("key" -> 2, "value" -> 20),
      Map("key" -> 3, "value" -> 30)))
    sql("INSERT INTO tbl REPLACE WHERE key > 100 SELECT * FROM VALUES (101,999) AS t(key, value)")
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    read(t, version = 1, name = "read_before_replace")
    snapshot(t)
  }

  // RW-006: ReplaceWhere with IN predicate
  test("RW_006_replacewhere_in_predicate", "ReplaceWhere with part IN ('p1','p2')",
      "write", "replaceWhere", "overwrite", "partitioned") {
    sql("CREATE TABLE tbl (key INT, value STRING, part STRING) USING delta PARTITIONED BY (part)")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("key" -> 1, "value" -> "a", "part" -> "p1"),
      Map("key" -> 2, "value" -> "b", "part" -> "p2"),
      Map("key" -> 3, "value" -> "c", "part" -> "p3"),
      Map("key" -> 4, "value" -> "d", "part" -> "p1")))
    sql("""INSERT INTO tbl REPLACE WHERE part IN ('p1', 'p2')
      SELECT * FROM VALUES (10,'x','p1'),(20,'y','p2') AS t(key, value, part)""")
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    read(t, predicate = "part = 'p3'", name = "read_p3_unchanged")
    snapshot(t)
  }

  // RW-007: INSERT INTO REPLACE WHERE via SQL
  test("RW_007_sql_replace_where", "SQL INSERT INTO REPLACE WHERE partition p1",
      "write", "replaceWhere", "overwrite", "sql", "partitioned") {
    sql("CREATE TABLE tbl (key INT, value INT, part STRING) USING delta PARTITIONED BY (part)")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("key" -> 1, "value" -> 10, "part" -> "p1"),
      Map("key" -> 2, "value" -> 20, "part" -> "p1"),
      Map("key" -> 3, "value" -> 30, "part" -> "p2")))
    sql("INSERT INTO tbl REPLACE WHERE part = 'p1' SELECT 10 as key, 100 as value, 'p1' as part")
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    read(t, predicate = "part = 'p1'", name = "read_p1_replaced")
    read(t, predicate = "part = 'p2'", name = "read_p2_unchanged")
    snapshot(t)
  }

  // RW-008: ReplaceWhere with compound predicate (AND)
  test("RW_008_replacewhere_compound", "ReplaceWhere with category='a' AND value > 20",
      "write", "replaceWhere", "overwrite", "compoundPredicate") {
    sql("CREATE TABLE tbl (key INT, value INT, category STRING) USING delta")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("key" -> 1, "value" -> 10, "category" -> "a"),
      Map("key" -> 2, "value" -> 20, "category" -> "b"),
      Map("key" -> 3, "value" -> 30, "category" -> "a"),
      Map("key" -> 4, "value" -> 40, "category" -> "b"),
      Map("key" -> 5, "value" -> 50, "category" -> "a")))
    sql("""INSERT INTO tbl REPLACE WHERE category = 'a' AND value > 20
      SELECT * FROM VALUES (3,300,'a'),(5,500,'a') AS t(key, value, category)""")
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    read(t, version = 1, name = "read_before_replace")
    snapshot(t)
  }

  // RW-009: Overwrite empty table
  test("RW_009_overwrite_empty_table", "Overwrite empty table - first data via overwrite",
      "write", "overwrite", "emptyTable") {
    sql("CREATE TABLE tbl (key INT, value STRING) USING delta")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    sql("INSERT OVERWRITE tbl VALUES (1,'a'),(2,'b')")
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    read(t, version = 0, name = "read_empty")
    snapshot(t)
  }

  // RW-010: Overwrite to empty (replace all with no data)
  test("RW_010_overwrite_to_empty", "Overwrite with empty DataFrame - clears table",
      "write", "overwrite", "empty") {
    sql("CREATE TABLE tbl (key INT, value STRING) USING delta")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("key" -> 1, "value" -> "a"),
      Map("key" -> 2, "value" -> "b"),
      Map("key" -> 3, "value" -> "c")))
    sql("INSERT OVERWRITE tbl SELECT key, value FROM tbl WHERE false")
    val t = registerWriteSpec(w)
    read(t, name = "read_after_overwrite_empty")
    read(t, version = 1, name = "read_before_overwrite")
    snapshot(t)
  }

  // RW-011: ReplaceWhere with deletion vectors enabled
  test("RW_011_replacewhere_dvs", "ReplaceWhere with DVs enabled",
      "write", "replaceWhere", "overwrite", "dv") {
    sql("""CREATE TABLE tbl (key INT, value INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("key" -> 1, "value" -> 10),
      Map("key" -> 2, "value" -> 20),
      Map("key" -> 3, "value" -> 30),
      Map("key" -> 4, "value" -> 40),
      Map("key" -> 5, "value" -> 50)))
    sql("INSERT INTO tbl REPLACE WHERE key <= 2 SELECT * FROM VALUES (1,100),(2,200) AS t(key, value)")
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    read(t, predicate = "key <= 2", name = "read_replaced")
    read(t, version = 1, name = "read_before_replace")
    snapshot(t)
  }

  // RW-012: Multiple overwrites in sequence
  test("RW_012_multiple_overwrites", "Two sequential overwrites - final state from last",
      "write", "overwrite") {
    sql("CREATE TABLE tbl (key INT, value STRING) USING delta")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("key" -> 1, "value" -> "a"),
      Map("key" -> 2, "value" -> "b"),
      Map("key" -> 3, "value" -> "c")))
    sql("INSERT OVERWRITE tbl VALUES (10,'x'),(20,'y')")
    sql("INSERT OVERWRITE tbl VALUES (100,'z')")
    val t = registerWriteSpec(w)
    read(t, name = "read_final")
    read(t, version = 1, name = "read_original")
    read(t, version = 2, name = "read_after_first_overwrite")
    snapshot(t)
  }

  // RW-013: Replace entire partition
  test("RW_013_replace_entire_partition", "Replace partition p1 with more rows",
      "write", "replaceWhere", "overwrite", "partitioned") {
    sql("CREATE TABLE tbl (key INT, value STRING, part STRING) USING delta PARTITIONED BY (part)")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("key" -> 1, "value" -> "old_a", "part" -> "p1"),
      Map("key" -> 2, "value" -> "old_b", "part" -> "p1"),
      Map("key" -> 3, "value" -> "old_c", "part" -> "p2")))
    sql("""INSERT INTO tbl REPLACE WHERE part = 'p1'
      SELECT * FROM VALUES (10,'new_a','p1'),(20,'new_b','p1'),(30,'new_c','p1') AS t(key, value, part)""")
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    read(t, predicate = "part = 'p1'", name = "read_p1_replaced")
    read(t, predicate = "part = 'p2'", name = "read_p2_unchanged")
    snapshot(t)
  }

  // RW-014: Overwrite with schema evolution
  test("RW_014_overwrite_schema_evolution", "Overwrite with mergeSchema adding new column",
      "write", "overwrite", "schemaEvolution") {
    sql("CREATE TABLE tbl (key INT, value STRING) USING delta")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("key" -> 1, "value" -> "a"),
      Map("key" -> 2, "value" -> "b")))
    sql("SET spark.delta.schema.autoMerge.enabled=true")
    sql("INSERT OVERWRITE tbl SELECT 10 as key, 'x' as value, 100 as extra")
    sql("SET spark.delta.schema.autoMerge.enabled=false")
    val t = registerWriteSpec(w)
    read(t, name = "read_after_overwrite")
    read(t, version = 1, name = "read_before_overwrite")
    snapshot(t)
  }

  // RW-015: ReplaceWhere idempotent
  test("RW_015_replacewhere_idempotent", "ReplaceWhere with same data - idempotent",
      "write", "replaceWhere", "overwrite", "idempotent") {
    sql("CREATE TABLE tbl (key INT, value INT) USING delta")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("key" -> 1, "value" -> 10),
      Map("key" -> 2, "value" -> 20),
      Map("key" -> 3, "value" -> 30)))
    sql("INSERT INTO tbl REPLACE WHERE key = 2 SELECT * FROM VALUES (2,20) AS t(key, value)")
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    read(t, version = 1, name = "read_before_replace")
    snapshot(t)
  }

  // ==========================================================================
  // InsertOverwrite Tests (IO-*)
  // Source: InsertOverwriteWriteCaptureSuite.scala / DeltaInsertIntoSuite.scala
  // ==========================================================================

  // IO-001: INSERT OVERWRITE full table (SQL)
  test("IO_001_insert_overwrite_full_sql", "INSERT OVERWRITE full table via SQL",
      "write", "insertOverwrite") {
    sql("CREATE TABLE tbl (key INT, value STRING) USING delta")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("key" -> 1, "value" -> "a"),
      Map("key" -> 2, "value" -> "b"),
      Map("key" -> 3, "value" -> "c"),
      Map("key" -> 4, "value" -> "d"),
      Map("key" -> 5, "value" -> "e")))
    sql("INSERT OVERWRITE tbl SELECT * FROM VALUES (10,'x'),(20,'y'),(30,'z') AS t(key, value)")
    val t = registerWriteSpec(w)
    read(t, name = "read_after_overwrite")
    read(t, version = 1, name = "read_before_overwrite")
    snapshot(t)
  }

  // IO-002: INSERT OVERWRITE single partition
  test("IO_002_insert_overwrite_single_partition", "INSERT OVERWRITE partition a only",
      "write", "insertOverwrite", "partitioned") {
    sql("CREATE TABLE tbl (key INT, value STRING, part STRING) USING delta PARTITIONED BY (part)")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("key" -> 1, "value" -> "a1", "part" -> "a"),
      Map("key" -> 2, "value" -> "a2", "part" -> "a"),
      Map("key" -> 3, "value" -> "a3", "part" -> "a"),
      Map("key" -> 4, "value" -> "b1", "part" -> "b"),
      Map("key" -> 5, "value" -> "b2", "part" -> "b")))
    sql("INSERT OVERWRITE tbl PARTITION (part = 'a') SELECT key, value FROM VALUES (10,'new1'),(20,'new2') AS t(key, value)")
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    read(t, predicate = "part = 'a'", name = "read_part_a")
    read(t, predicate = "part = 'b'", name = "read_part_b_unchanged")
    snapshot(t)
  }

  // IO-003: Dynamic partition overwrite (DPO)
  test("IO_003_dynamic_partition_overwrite", "DPO - only source partitions overwritten",
      "write", "insertOverwrite", "dynamicPartition") {
    sql("CREATE TABLE tbl (key INT, value STRING, part STRING) USING delta PARTITIONED BY (part)")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("key" -> 1, "value" -> "a1", "part" -> "p1"),
      Map("key" -> 2, "value" -> "a2", "part" -> "p1"),
      Map("key" -> 3, "value" -> "b1", "part" -> "p2"),
      Map("key" -> 4, "value" -> "b2", "part" -> "p2")))
    sql("SET spark.sql.sources.partitionOverwriteMode=dynamic")
    sql("INSERT OVERWRITE tbl PARTITION (part) VALUES (10,'new1','p1'),(20,'new2','p1')")
    sql("SET spark.sql.sources.partitionOverwriteMode=static")
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    read(t, predicate = "part = 'p1'", name = "read_p1_replaced")
    read(t, predicate = "part = 'p2'", name = "read_p2_unchanged")
    snapshot(t)
  }

  // IO-004: INSERT OVERWRITE with new schema (overwriteSchema)
  test("IO_004_insert_overwrite_schema", "INSERT OVERWRITE with schema change adding extra column",
      "write", "insertOverwrite", "schemaOverwrite") {
    sql("CREATE TABLE tbl (key INT, value STRING) USING delta")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("key" -> 1, "value" -> "a"),
      Map("key" -> 2, "value" -> "b")))
    sql("SET spark.delta.schema.autoMerge.enabled=true")
    sql("INSERT OVERWRITE tbl SELECT 10 as key, 'x' as value, 1.5 as extra")
    sql("SET spark.delta.schema.autoMerge.enabled=false")
    val t = registerWriteSpec(w)
    read(t, name = "read_after_overwrite")
    read(t, version = 1, name = "read_before_overwrite")
    snapshot(t)
  }

  // IO-005: INSERT OVERWRITE empty replacement
  test("IO_005_insert_overwrite_empty", "INSERT OVERWRITE with empty result - clears table",
      "write", "insertOverwrite", "empty") {
    sql("CREATE TABLE tbl (key INT, value STRING) USING delta")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("key" -> 1, "value" -> "a"),
      Map("key" -> 2, "value" -> "b"),
      Map("key" -> 3, "value" -> "c")))
    sql("INSERT OVERWRITE tbl SELECT key, value FROM tbl WHERE false")
    val t = registerWriteSpec(w)
    read(t, name = "read_after_overwrite_empty")
    read(t, version = 1, name = "read_before_overwrite")
    snapshot(t)
  }

  // IO-006: INSERT OVERWRITE multiple partitions
  test("IO_006_insert_overwrite_multi_partition", "DPO overwrite p1 and p2, leave p3",
      "write", "insertOverwrite", "multiPartition") {
    sql("CREATE TABLE tbl (key INT, value STRING, part STRING) USING delta PARTITIONED BY (part)")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("key" -> 1, "value" -> "a1", "part" -> "p1"),
      Map("key" -> 2, "value" -> "a2", "part" -> "p1"),
      Map("key" -> 3, "value" -> "b1", "part" -> "p2"),
      Map("key" -> 4, "value" -> "b2", "part" -> "p2"),
      Map("key" -> 5, "value" -> "c1", "part" -> "p3"),
      Map("key" -> 6, "value" -> "c2", "part" -> "p3")))
    sql("SET spark.sql.sources.partitionOverwriteMode=dynamic")
    sql("INSERT OVERWRITE tbl PARTITION (part) VALUES (10,'x1','p1'),(30,'x2','p2')")
    sql("SET spark.sql.sources.partitionOverwriteMode=static")
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    read(t, predicate = "part = 'p3'", name = "read_p3_unchanged")
    snapshot(t)
  }

  // IO-007: INSERT OVERWRITE with SQL VALUES
  test("IO_007_insert_overwrite_values", "INSERT OVERWRITE with SQL VALUES",
      "write", "insertOverwrite") {
    sql("CREATE TABLE tbl (key INT, value STRING) USING delta")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("key" -> 1, "value" -> "a"),
      Map("key" -> 2, "value" -> "b"),
      Map("key" -> 3, "value" -> "c")))
    sql("INSERT OVERWRITE tbl VALUES (1,'x'),(2,'y')")
    val t = registerWriteSpec(w)
    read(t, name = "read_after_overwrite")
    read(t, version = 1, name = "read_before_overwrite")
    snapshot(t)
  }

  // IO-008: INSERT OVERWRITE preserving other partitions
  test("IO_008_insert_overwrite_preserve_partitions", "Overwrite p1 only, p2 and p3 untouched",
      "write", "insertOverwrite", "partitioned") {
    sql("CREATE TABLE tbl (key INT, value STRING, part STRING) USING delta PARTITIONED BY (part)")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("key" -> 1, "value" -> "a1", "part" -> "p1"),
      Map("key" -> 2, "value" -> "a2", "part" -> "p1"),
      Map("key" -> 3, "value" -> "b1", "part" -> "p2"),
      Map("key" -> 4, "value" -> "b2", "part" -> "p2"),
      Map("key" -> 5, "value" -> "c1", "part" -> "p3"),
      Map("key" -> 6, "value" -> "c2", "part" -> "p3")))
    sql("INSERT OVERWRITE tbl PARTITION (part = 'p1') SELECT key, value FROM VALUES (10,'new1') AS t(key, value)")
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    read(t, predicate = "part = 'p1'", name = "read_p1_replaced")
    read(t, predicate = "part = 'p2'", name = "read_p2_unchanged")
    read(t, predicate = "part = 'p3'", name = "read_p3_unchanged")
    snapshot(t)
  }

  // IO-009: Dynamic partition overwrite adding new partition
  test("IO_009_dpo_add_new_partition", "DPO adds new partition p3, p1 and p2 untouched",
      "write", "insertOverwrite", "dynamicPartition", "newPartition") {
    sql("CREATE TABLE tbl (key INT, value STRING, part STRING) USING delta PARTITIONED BY (part)")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("key" -> 1, "value" -> "a1", "part" -> "p1"),
      Map("key" -> 2, "value" -> "a2", "part" -> "p1"),
      Map("key" -> 3, "value" -> "b1", "part" -> "p2")))
    sql("SET spark.sql.sources.partitionOverwriteMode=dynamic")
    sql("INSERT OVERWRITE tbl PARTITION (part) VALUES (10,'new1','p3'),(20,'new2','p3')")
    sql("SET spark.sql.sources.partitionOverwriteMode=static")
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    read(t, predicate = "part = 'p1'", name = "read_p1_unchanged")
    read(t, predicate = "part = 'p3'", name = "read_p3_new")
    snapshot(t)
  }

  // IO-010: INSERT OVERWRITE with deletion vectors
  test("IO_010_insert_overwrite_dvs", "INSERT OVERWRITE on table with prior DV deletes",
      "write", "insertOverwrite", "dv") {
    sql("""CREATE TABLE tbl (key INT, value INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("key" -> 1, "value" -> 10),
      Map("key" -> 2, "value" -> 20),
      Map("key" -> 3, "value" -> 30),
      Map("key" -> 4, "value" -> 40),
      Map("key" -> 5, "value" -> 50)))
    sql("DELETE FROM tbl WHERE key IN (2, 4)")
    sql("INSERT OVERWRITE tbl VALUES (10,100),(20,200),(30,300)")
    val t = registerWriteSpec(w)
    read(t, name = "read_after_overwrite")
    read(t, version = 2, name = "read_after_dv_delete")
    snapshot(t)
  }

  // IO-011: INSERT OVERWRITE with transformation
  test("IO_011_insert_overwrite_transform", "INSERT OVERWRITE with value doubled",
      "write", "insertOverwrite", "transform") {
    sql("CREATE TABLE tbl (key INT, value INT) USING delta")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("key" -> 1, "value" -> 10),
      Map("key" -> 2, "value" -> 20),
      Map("key" -> 3, "value" -> 30)))
    sql("CREATE TEMPORARY VIEW io011_source AS SELECT * FROM VALUES (1,10),(2,20),(3,30) AS t(key, value)")
    sql("INSERT OVERWRITE tbl SELECT key, value * 2 as value FROM io011_source")
    val t = registerWriteSpec(w)
    read(t, name = "read_after_overwrite")
    read(t, version = 1, name = "read_before_overwrite")
    snapshot(t)
  }

  // IO-012: Repeated INSERT OVERWRITE (idempotent)
  test("IO_012_insert_overwrite_idempotent", "Second overwrite with same data",
      "write", "insertOverwrite", "idempotent") {
    sql("CREATE TABLE tbl (key INT, value STRING) USING delta")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("key" -> 1, "value" -> "a"),
      Map("key" -> 2, "value" -> "b"),
      Map("key" -> 3, "value" -> "c")))
    sql("INSERT OVERWRITE tbl VALUES (10,'x'),(20,'y')")
    sql("INSERT OVERWRITE tbl VALUES (10,'x'),(20,'y')")
    val t = registerWriteSpec(w)
    read(t, name = "read_final")
    read(t, version = 1, name = "read_original")
    snapshot(t)
  }

  // IO-013: INSERT OVERWRITE with nested partition columns
  test("IO_013_insert_overwrite_nested_partition", "Overwrite (year=2024,month=1) partition",
      "write", "insertOverwrite", "nestedPartition") {
    sql("CREATE TABLE tbl (key INT, value STRING, year INT, month INT) USING delta PARTITIONED BY (year, month)")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("key" -> 1, "value" -> "a", "year" -> 2024, "month" -> 1),
      Map("key" -> 2, "value" -> "b", "year" -> 2024, "month" -> 1),
      Map("key" -> 3, "value" -> "c", "year" -> 2024, "month" -> 2),
      Map("key" -> 4, "value" -> "d", "year" -> 2024, "month" -> 2),
      Map("key" -> 5, "value" -> "e", "year" -> 2025, "month" -> 1)))
    sql("INSERT OVERWRITE tbl PARTITION (year = 2024, month = 1) SELECT key, value FROM VALUES (10,'new1'),(20,'new2') AS t(key, value)")
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    read(t, predicate = "year = 2024 AND month = 1", name = "read_replaced_partition")
    read(t, predicate = "year = 2024 AND month = 2", name = "read_unchanged_partition")
    snapshot(t)
  }

  // IO-014: Dynamic partition overwrite with larger replacement
  test("IO_014_dpo_larger_replacement", "DPO p2 grows from 2 to 5 rows",
      "write", "insertOverwrite", "dynamicPartition") {
    sql("CREATE TABLE tbl (key INT, value STRING, part STRING) USING delta PARTITIONED BY (part)")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("key" -> 1, "value" -> "a", "part" -> "p1"),
      Map("key" -> 2, "value" -> "b", "part" -> "p2"),
      Map("key" -> 3, "value" -> "c", "part" -> "p2")))
    sql("SET spark.sql.sources.partitionOverwriteMode=dynamic")
    sql("INSERT OVERWRITE tbl PARTITION (part) VALUES (10,'x1','p2'),(20,'x2','p2'),(30,'x3','p2'),(40,'x4','p2'),(50,'x5','p2')")
    sql("SET spark.sql.sources.partitionOverwriteMode=static")
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    read(t, predicate = "part = 'p1'", name = "read_p1_unchanged")
    read(t, predicate = "part = 'p2'", name = "read_p2_grown")
    snapshot(t)
  }

  // IO-015: INSERT OVERWRITE on unpartitioned table
  test("IO_015_insert_overwrite_unpartitioned", "Full replace on unpartitioned table",
      "write", "insertOverwrite") {
    sql("CREATE TABLE tbl (key INT, value STRING) USING delta")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("key" -> 1, "value" -> "a"),
      Map("key" -> 2, "value" -> "b"),
      Map("key" -> 3, "value" -> "c"),
      Map("key" -> 4, "value" -> "d")))
    sql("INSERT OVERWRITE tbl VALUES (10,'x'),(20,'y'),(30,'z')")
    val t = registerWriteSpec(w)
    read(t, name = "read_after_overwrite")
    read(t, version = 1, name = "read_before_overwrite")
    snapshot(t)
  }

  // ==========================================================================
  // Schema Evolution Tests (SE-*)
  // Source: InsertSchemaEvolutionWriteCaptureSuite.scala
  // ==========================================================================

  // SE-001: INSERT with extra column (auto schema evolution)
  test("SE_001_insert_extra_column", "Auto schema evolution adds extra column",
      "write", "insert", "schemaEvolution") {
    sql("CREATE TABLE tbl (key INT, value STRING) USING delta")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(Map("key" -> 1, "value" -> "a")))
    sql("SET spark.delta.schema.autoMerge.enabled=true")
    sql("INSERT INTO tbl SELECT 1 as key, 'a' as value, 100 as extra")
    sql("SET spark.delta.schema.autoMerge.enabled=false")
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    read(t, version = 1, name = "read_before_evolution")
    snapshot(t)
  }

  // SE-002: INSERT with missing column (nullable fills null)
  test("SE_002_insert_missing_column", "Missing column filled with null",
      "write", "insert", "schemaEvolution") {
    sql("CREATE TABLE tbl (key INT, value STRING, extra INT) USING delta")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(Map("key" -> 1, "value" -> "a", "extra" -> 100)))
    sql("SET spark.delta.schema.autoMerge.enabled=true")
    sql("INSERT INTO tbl (key, value) VALUES (2,'b')")
    sql("SET spark.delta.schema.autoMerge.enabled=false")
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  // SE-003: INSERT with reordered columns
  test("SE_003_insert_reordered_columns", "Columns matched by name despite different order",
      "write", "insert", "schemaEvolution") {
    sql("CREATE TABLE tbl (key INT, value STRING, score DOUBLE) USING delta")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(Map("key" -> 1, "value" -> "a", "score" -> 1.0)))
    sql("INSERT INTO tbl (score, key, value) VALUES (2.0, 2, 'b')")
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  // SE-004: INSERT with nested struct evolution
  test("SE_004_insert_nested_struct_evolution", "Add field to nested struct via mergeSchema",
      "write", "insert", "schemaEvolution", "nestedStruct") {
    sql("CREATE TABLE tbl (id INT, info STRUCT<name: STRING>) USING delta")
    sql("INSERT INTO tbl VALUES (1, named_struct('name','alice'))")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    sql("SET spark.delta.schema.autoMerge.enabled=true")
    sql("INSERT INTO tbl SELECT 2 as id, named_struct('name','bob','age',25) as info")
    sql("SET spark.delta.schema.autoMerge.enabled=false")
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    read(t, version = 1, name = "read_before_evolution")
    snapshot(t)
  }

  // SE-005: INSERT with type widening (int to long)
  test("SE_005_insert_type_widening", "Type widens from INT to LONG via mergeSchema",
      "write", "insert", "schemaEvolution", "typeWidening") {
    sql("CREATE TABLE tbl (id INT, value INT) USING delta")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(Map("id" -> 1, "value" -> 10)))
    sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.enableTypeWidening' = 'true')")
    sql("SET spark.delta.schema.autoMerge.enabled=true")
    sql("INSERT INTO tbl SELECT 2 as id, CAST(20000000000 AS BIGINT) as value")
    sql("SET spark.delta.schema.autoMerge.enabled=false")
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    read(t, version = 1, name = "read_before_widening")
    snapshot(t)
  }

  // SE-006: INSERT OVERWRITE with schema evolution
  test("SE_006_overwrite_schema_evolution", "Overwrite adds extra column via schema evolution",
      "write", "insert", "schemaEvolution", "overwrite") {
    sql("CREATE TABLE tbl (key INT, value STRING) USING delta")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("key" -> 1, "value" -> "a"),
      Map("key" -> 2, "value" -> "b")))
    sql("SET spark.delta.schema.autoMerge.enabled=true")
    sql("INSERT OVERWRITE tbl SELECT 10 as key, 'x' as value, 100 as extra")
    sql("SET spark.delta.schema.autoMerge.enabled=false")
    val t = registerWriteSpec(w)
    read(t, name = "read_after_overwrite")
    read(t, version = 1, name = "read_before_overwrite")
    snapshot(t)
  }

  // SE-007: INSERT with multiple extra columns
  test("SE_007_insert_multiple_extra_columns", "Schema evolves to add age, city, rating",
      "write", "insert", "schemaEvolution") {
    sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(Map("id" -> 1, "name" -> "a")))
    sql("SET spark.delta.schema.autoMerge.enabled=true")
    sql("INSERT INTO tbl SELECT 2 as id, 'b' as name, 25 as age, 'NYC' as city, 3.5 as rating")
    sql("SET spark.delta.schema.autoMerge.enabled=false")
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    read(t, version = 1, name = "read_before_evolution")
    snapshot(t)
  }

  // SE-008: INSERT into partitioned table with schema evolution
  test("SE_008_insert_partitioned_schema_evolution", "Schema evolves on partitioned table",
      "write", "insert", "schemaEvolution", "partitioned") {
    sql("CREATE TABLE tbl (id INT, value STRING, part STRING) USING delta PARTITIONED BY (part)")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "value" -> "a", "part" -> "p1"),
      Map("id" -> 2, "value" -> "b", "part" -> "p2")))
    sql("SET spark.delta.schema.autoMerge.enabled=true")
    sql("INSERT INTO tbl SELECT 3 as id, 'c' as value, 100 as score, 'p1' as part")
    sql("SET spark.delta.schema.autoMerge.enabled=false")
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    read(t, predicate = "part = 'p1'", name = "read_p1")
    snapshot(t)
  }

  // SE-009: INSERT with nested array of structs evolution
  test("SE_009_insert_array_struct_evolution", "Array element struct evolves to add price field",
      "write", "insert", "schemaEvolution", "arrayOfStructs") {
    sql("CREATE TABLE tbl (id INT, items ARRAY<STRUCT<name: STRING>>) USING delta")
    sql("INSERT INTO tbl VALUES (1, array(named_struct('name','item1')))")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    sql("SET spark.delta.schema.autoMerge.enabled=true")
    sql("INSERT INTO tbl SELECT 2 as id, array(named_struct('name','item2','price',9.99)) as items")
    sql("SET spark.delta.schema.autoMerge.enabled=false")
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    read(t, version = 1, name = "read_before_evolution")
    snapshot(t)
  }

  // SE-010: INSERT with map type (no evolution)
  test("SE_010_insert_map_type", "INSERT map type - same schema, no evolution",
      "write", "insert", "schemaEvolution", "mapType") {
    sql("CREATE TABLE tbl (id INT, props MAP<STRING, STRING>) USING delta")
    sql("INSERT INTO tbl VALUES (1, map('a','1'))")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    sql("INSERT INTO tbl VALUES (2, map('b','2'))")
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  // SE-011: Consecutive inserts with incremental schema evolution
  test("SE_011_incremental_evolution", "3 inserts: (id) -> (id,name) -> (id,name,age)",
      "write", "insert", "schemaEvolution", "incremental") {
    sql("CREATE TABLE tbl (id INT) USING delta")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(Map("id" -> 1)))
    sql("SET spark.delta.schema.autoMerge.enabled=true")
    sql("INSERT INTO tbl SELECT 2 as id, 'bob' as name")
    sql("INSERT INTO tbl SELECT 3 as id, 'carol' as name, 30 as age")
    sql("SET spark.delta.schema.autoMerge.enabled=false")
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    read(t, version = 1, name = "read_v1_id_only")
    read(t, version = 2, name = "read_v2_id_name")
    snapshot(t)
  }

  // SE-012: INSERT with column name case difference
  test("SE_012_insert_case_insensitive", "Case-insensitive column name match",
      "write", "insert", "schemaEvolution", "caseInsensitive") {
    sql("CREATE TABLE tbl (Id INT, Name STRING) USING delta")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(Map("Id" -> 1, "Name" -> "alice")))
    sql("INSERT INTO tbl (id, name) VALUES (2,'bob')")
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  // SE-013: INSERT with null typed extra column
  test("SE_013_insert_null_extra_column", "Extra STRING column with null value",
      "write", "insert", "schemaEvolution") {
    sql("CREATE TABLE tbl (id INT, value STRING) USING delta")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(Map("id" -> 1, "value" -> "a")))
    sql("SET spark.delta.schema.autoMerge.enabled=true")
    sql("INSERT INTO tbl SELECT 2 as id, 'b' as value, CAST(null AS STRING) as extra")
    sql("SET spark.delta.schema.autoMerge.enabled=false")
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  // SE-014: INSERT with null values for existing columns
  test("SE_014_insert_null_values", "INSERT with null for name and score columns",
      "write", "insert", "schemaEvolution", "nullValues") {
    sql("CREATE TABLE tbl (id INT, name STRING, score DOUBLE) USING delta")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(Map("id" -> 1, "name" -> "alice", "score" -> 95.0)))
    sql("INSERT INTO tbl VALUES (2, null, null)")
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  // SE-015: INSERT OVERWRITE partition with schema evolution
  test("SE_015_overwrite_partition_schema_evolution", "Overwrite partition p1 with extra column",
      "write", "insert", "schemaEvolution", "overwrite", "partitioned") {
    sql("CREATE TABLE tbl (id INT, value STRING, part STRING) USING delta PARTITIONED BY (part)")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "value" -> "a", "part" -> "p1"),
      Map("id" -> 2, "value" -> "b", "part" -> "p2")))
    sql("SET spark.delta.schema.autoMerge.enabled=true")
    sql("""INSERT INTO tbl REPLACE WHERE part = 'p1'
      SELECT 10 as id, 'x' as value, 100 as extra, 'p1' as part""")
    sql("SET spark.delta.schema.autoMerge.enabled=false")
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    read(t, predicate = "part = 'p1'", name = "read_p1_replaced")
    read(t, predicate = "part = 'p2'", name = "read_p2_unchanged")
    snapshot(t)
  }

}
