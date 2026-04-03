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
  // Note: COPY INTO loads external files into Delta. In the WorkloadSuite
  // context we simulate by creating source data via SQL, writing to a temp
  // location with Spark, then COPY INTO the target. Since the workload
  // generator runs in Spark, we use temp views + INSERT to stage data,
  // then COPY INTO from parquet files.
  // ==========================================================================

  // CI-001: Basic COPY INTO from parquet
  test("CI_001_copy_into_parquet", "COPY INTO from parquet source - basic load",
      "write", "copyInto", "parquet") { w =>
    w.sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
    // Stage source parquet data
    w.sql("CREATE TABLE source_ci001 (id INT, name STRING) USING parquet")
    w.sql("INSERT INTO source_ci001 VALUES (1,'a'),(2,'b'),(3,'c')")
    w.sql("COPY INTO tbl FROM (SELECT * FROM source_ci001) FILEFORMAT = PARQUET")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  // CI-002: COPY INTO from CSV
  test("CI_002_copy_into_csv", "COPY INTO from CSV with header",
      "write", "copyInto", "csv") { w =>
    w.sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
    w.sql("CREATE TABLE source_ci002 (id INT, name STRING) USING csv OPTIONS (header 'true')")
    w.sql("INSERT INTO source_ci002 VALUES (1,'alice'),(2,'bob')")
    w.sql("COPY INTO tbl FROM (SELECT * FROM source_ci002) FILEFORMAT = CSV FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true')")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  // CI-003: COPY INTO from JSON
  test("CI_003_copy_into_json", "COPY INTO from JSON source",
      "write", "copyInto", "json") { w =>
    w.sql("CREATE TABLE tbl (id BIGINT, name STRING) USING delta")
    w.sql("CREATE TABLE source_ci003 (id BIGINT, name STRING) USING json")
    w.sql("INSERT INTO source_ci003 VALUES (1,'x'),(2,'y'),(3,'z')")
    w.sql("COPY INTO tbl FROM (SELECT * FROM source_ci003) FILEFORMAT = JSON")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  // CI-004: COPY INTO with schema evolution (mergeSchema)
  test("CI_004_copy_into_merge_schema", "COPY INTO with mergeSchema adding new column",
      "write", "copyInto", "schemaEvolution") { w =>
    w.sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
    w.sql("INSERT INTO tbl VALUES (1,'a')")
    // Source has extra column 'score'
    w.sql("CREATE TABLE source_ci004 (id INT, name STRING, score INT) USING parquet")
    w.sql("INSERT INTO source_ci004 VALUES (2,'b',100),(3,'c',200)")
    w.sql("COPY INTO tbl FROM (SELECT * FROM source_ci004) FILEFORMAT = PARQUET COPY_OPTIONS ('mergeSchema' = 'true')")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  // CI-005: COPY INTO with CSV format options (header, delimiter)
  test("CI_005_copy_into_csv_options", "COPY INTO with pipe delimiter",
      "write", "copyInto", "csv", "formatOptions") { w =>
    w.sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
    w.sql("CREATE TABLE source_ci005 (id INT, name STRING) USING csv OPTIONS (header 'true', delimiter '|')")
    w.sql("INSERT INTO source_ci005 VALUES (1,'alice'),(2,'bob')")
    w.sql("COPY INTO tbl FROM (SELECT * FROM source_ci005) FILEFORMAT = CSV FORMAT_OPTIONS ('header' = 'true', 'delimiter' = '|', 'inferSchema' = 'true')")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  // CI-006: COPY INTO idempotent (duplicate files skipped)
  test("CI_006_copy_into_idempotent", "COPY INTO twice from same source - second is no-op",
      "write", "copyInto", "idempotent") { w =>
    w.sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
    w.sql("CREATE TABLE source_ci006 (id INT, name STRING) USING parquet")
    w.sql("INSERT INTO source_ci006 VALUES (1,'a'),(2,'b')")
    w.sql("COPY INTO tbl FROM (SELECT * FROM source_ci006) FILEFORMAT = PARQUET")
    w.sql("COPY INTO tbl FROM (SELECT * FROM source_ci006) FILEFORMAT = PARQUET")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  // CI-007: COPY INTO into partitioned table
  test("CI_007_copy_into_partitioned", "COPY INTO partitioned table",
      "write", "copyInto", "partitioned") { w =>
    w.sql("CREATE TABLE tbl (id INT, name STRING, part STRING) USING delta PARTITIONED BY (part)")
    w.sql("CREATE TABLE source_ci007 (id INT, name STRING, part STRING) USING parquet")
    w.sql("INSERT INTO source_ci007 VALUES (1,'a','p1'),(2,'b','p2'),(3,'c','p1')")
    w.sql("COPY INTO tbl FROM (SELECT * FROM source_ci007) FILEFORMAT = PARQUET")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, predicate = "part = 'p1'", name = "read_p1")
    w.read(t, predicate = "part = 'p2'", name = "read_p2")
    w.snapshot(t)
  }

  // CI-008: COPY INTO with column mapping
  test("CI_008_copy_into_column_mapping", "COPY INTO with column mapping name mode",
      "write", "copyInto", "columnMapping") { w =>
    w.sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
      TBLPROPERTIES (
        'delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2',
        'delta.minWriterVersion' = '5'
      )""")
    w.sql("CREATE TABLE source_ci008 (id INT, name STRING) USING parquet")
    w.sql("INSERT INTO source_ci008 VALUES (1,'alice'),(2,'bob')")
    w.sql("COPY INTO tbl FROM (SELECT * FROM source_ci008) FILEFORMAT = PARQUET")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  // CI-009: COPY INTO with subset of columns
  test("CI_009_copy_into_subset_columns", "COPY INTO with source having fewer columns",
      "write", "copyInto", "subsetColumns") { w =>
    w.sql("CREATE TABLE tbl (id INT, name STRING, score DOUBLE) USING delta")
    w.sql("CREATE TABLE source_ci009 (id INT, name STRING) USING parquet")
    w.sql("INSERT INTO source_ci009 VALUES (1,'a'),(2,'b')")
    w.sql("COPY INTO tbl FROM (SELECT * FROM source_ci009) FILEFORMAT = PARQUET COPY_OPTIONS ('mergeSchema' = 'false')")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  // CI-010: COPY INTO multiple batches
  test("CI_010_copy_into_multi_batch", "Two sequential COPY INTO from different sources",
      "write", "copyInto", "multiBatch") { w =>
    w.sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
    w.sql("CREATE TABLE source_ci010a (id INT, name STRING) USING parquet")
    w.sql("INSERT INTO source_ci010a VALUES (1,'a'),(2,'b')")
    w.sql("COPY INTO tbl FROM (SELECT * FROM source_ci010a) FILEFORMAT = PARQUET")
    w.sql("CREATE TABLE source_ci010b (id INT, name STRING) USING parquet")
    w.sql("INSERT INTO source_ci010b VALUES (3,'c'),(4,'d')")
    w.sql("COPY INTO tbl FROM (SELECT * FROM source_ci010b) FILEFORMAT = PARQUET")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  // CI-011: COPY INTO into empty table
  test("CI_011_copy_into_empty_table", "COPY INTO empty table - first data load",
      "write", "copyInto", "emptyTable") { w =>
    w.sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
    w.sql("CREATE TABLE source_ci011 (id INT, name STRING) USING parquet")
    w.sql("INSERT INTO source_ci011 VALUES (1,'first'),(2,'second'),(3,'third')")
    w.sql("COPY INTO tbl FROM (SELECT * FROM source_ci011) FILEFORMAT = PARQUET")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  // CI-012: COPY INTO from directory with multiple files
  test("CI_012_copy_into_multi_files", "COPY INTO from directory with multiple parquet files",
      "write", "copyInto", "multipleFiles") { w =>
    w.sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
    // Multiple inserts into source create multiple files
    w.sql("CREATE TABLE source_ci012 (id INT, name STRING) USING parquet")
    w.sql("INSERT INTO source_ci012 VALUES (1,'a'),(2,'b')")
    w.sql("INSERT INTO source_ci012 VALUES (3,'c'),(4,'d')")
    w.sql("INSERT INTO source_ci012 VALUES (5,'e')")
    w.sql("COPY INTO tbl FROM (SELECT * FROM source_ci012) FILEFORMAT = PARQUET")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  // CI-013: COPY INTO with BIGINT column
  test("CI_013_copy_into_bigint", "COPY INTO with BIGINT column matching source",
      "write", "copyInto", "bigint") { w =>
    w.sql("CREATE TABLE tbl (id BIGINT, name STRING) USING delta")
    w.sql("CREATE TABLE source_ci013 (id BIGINT, name STRING) USING parquet")
    w.sql("INSERT INTO source_ci013 VALUES (1,'a'),(2,'b')")
    w.sql("COPY INTO tbl FROM (SELECT * FROM source_ci013) FILEFORMAT = PARQUET COPY_OPTIONS ('mergeSchema' = 'false')")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  // CI-014: COPY INTO with nested struct
  test("CI_014_copy_into_nested_struct", "COPY INTO with nested struct data",
      "write", "copyInto", "nestedStruct") { w =>
    w.sql("CREATE TABLE tbl (id INT, info STRUCT<name: STRING, age: INT>) USING delta")
    w.sql("CREATE TABLE source_ci014 (id INT, info STRUCT<name: STRING, age: INT>) USING parquet")
    w.sql("INSERT INTO source_ci014 VALUES (1, named_struct('name','alice','age',30)),(2, named_struct('name','bob','age',25))")
    w.sql("COPY INTO tbl FROM (SELECT * FROM source_ci014) FILEFORMAT = PARQUET")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  // CI-015: COPY INTO append semantics
  test("CI_015_copy_into_append", "COPY INTO preserves existing data",
      "write", "copyInto", "append") { w =>
    w.sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
    w.sql("INSERT INTO tbl VALUES (1,'existing1'),(2,'existing2')")
    w.sql("CREATE TABLE source_ci015 (id INT, name STRING) USING parquet")
    w.sql("INSERT INTO source_ci015 VALUES (3,'new1'),(4,'new2')")
    w.sql("COPY INTO tbl FROM (SELECT * FROM source_ci015) FILEFORMAT = PARQUET")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, version = 1, name = "read_before_copy")
    w.snapshot(t)
  }

  // ==========================================================================
  // ReplaceWhere Tests (RW-*)
  // Source: ReplaceWhereWriteCaptureSuite.scala / DeltaInsertReplaceWhereSuite.scala
  // ==========================================================================

  // RW-001: ReplaceWhere with partition predicate
  test("RW_001_replacewhere_partition", "ReplaceWhere partition p1",
      "write", "replaceWhere", "overwrite", "partitioned") { w =>
    w.sql("CREATE TABLE tbl (key INT, value STRING, part STRING) USING delta PARTITIONED BY (part)")
    w.sql("INSERT INTO tbl VALUES (1,'a','p1'),(2,'b','p1'),(3,'c','p2'),(4,'d','p2')")
    w.sql("INSERT INTO tbl REPLACE WHERE part = 'p1' SELECT 10 as key, 'x' as value, 'p1' as part UNION ALL SELECT 20, 'y', 'p1'")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, predicate = "part = 'p1'", name = "read_p1")
    w.read(t, predicate = "part = 'p2'", name = "read_p2_unchanged")
    w.read(t, version = 1, name = "read_before_replace")
    w.snapshot(t)
  }

  // RW-002: ReplaceWhere with data predicate (non-partitioned)
  test("RW_002_replacewhere_data_predicate", "ReplaceWhere on non-partitioned table key <= 2",
      "write", "replaceWhere", "overwrite") { w =>
    w.sql("CREATE TABLE tbl (key INT, value INT) USING delta")
    w.sql("INSERT INTO tbl VALUES (1,10),(2,20),(3,30),(4,40),(5,50)")
    w.sql("INSERT INTO tbl REPLACE WHERE key <= 2 SELECT * FROM VALUES (1,100),(2,200) AS t(key, value)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, predicate = "key <= 2", name = "read_replaced")
    w.read(t, version = 1, name = "read_before_replace")
    w.snapshot(t)
  }

  // RW-003: INSERT OVERWRITE whole table (replace all)
  test("RW_003_overwrite_all", "INSERT OVERWRITE replaces all rows",
      "write", "overwrite") { w =>
    w.sql("CREATE TABLE tbl (key INT, value STRING) USING delta")
    w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
    w.sql("INSERT OVERWRITE tbl VALUES (10,'x'),(20,'y')")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_after_overwrite")
    w.read(t, version = 1, name = "read_before_overwrite")
    w.snapshot(t)
  }

  // RW-004: Dynamic partition overwrite
  test("RW_004_dynamic_partition_overwrite", "Dynamic partition overwrite - only p1 replaced",
      "write", "overwrite", "dynamicPartition", "partitioned") { w =>
    w.sql("CREATE TABLE tbl (key INT, value STRING, part STRING) USING delta PARTITIONED BY (part)")
    w.sql("INSERT INTO tbl VALUES (1,'a','p1'),(2,'b','p1'),(3,'c','p2'),(4,'d','p2')")
    // Dynamic partition overwrite: only replace partitions present in source
    w.sql("SET spark.sql.sources.partitionOverwriteMode=dynamic")
    w.sql("INSERT OVERWRITE tbl PARTITION (part) VALUES (10,'x','p1')")
    w.sql("SET spark.sql.sources.partitionOverwriteMode=static")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, predicate = "part = 'p1'", name = "read_p1_replaced")
    w.read(t, predicate = "part = 'p2'", name = "read_p2_unchanged")
    w.snapshot(t)
  }

  // RW-005: ReplaceWhere matching no existing rows
  test("RW_005_replacewhere_no_match", "ReplaceWhere matching no rows - new data added",
      "write", "replaceWhere", "overwrite") { w =>
    w.sql("CREATE TABLE tbl (key INT, value INT) USING delta")
    w.sql("INSERT INTO tbl VALUES (1,10),(2,20),(3,30)")
    w.sql("INSERT INTO tbl REPLACE WHERE key > 100 SELECT * FROM VALUES (101,999) AS t(key, value)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, version = 1, name = "read_before_replace")
    w.snapshot(t)
  }

  // RW-006: ReplaceWhere with IN predicate
  test("RW_006_replacewhere_in_predicate", "ReplaceWhere with part IN ('p1','p2')",
      "write", "replaceWhere", "overwrite", "partitioned") { w =>
    w.sql("CREATE TABLE tbl (key INT, value STRING, part STRING) USING delta PARTITIONED BY (part)")
    w.sql("INSERT INTO tbl VALUES (1,'a','p1'),(2,'b','p2'),(3,'c','p3'),(4,'d','p1')")
    w.sql("""INSERT INTO tbl REPLACE WHERE part IN ('p1', 'p2')
      SELECT * FROM VALUES (10,'x','p1'),(20,'y','p2') AS t(key, value, part)""")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, predicate = "part = 'p3'", name = "read_p3_unchanged")
    w.snapshot(t)
  }

  // RW-007: INSERT INTO REPLACE WHERE via SQL
  test("RW_007_sql_replace_where", "SQL INSERT INTO REPLACE WHERE partition p1",
      "write", "replaceWhere", "overwrite", "sql", "partitioned") { w =>
    w.sql("CREATE TABLE tbl (key INT, value INT, part STRING) USING delta PARTITIONED BY (part)")
    w.sql("INSERT INTO tbl VALUES (1,10,'p1'),(2,20,'p1'),(3,30,'p2')")
    w.sql("INSERT INTO tbl REPLACE WHERE part = 'p1' SELECT 10 as key, 100 as value, 'p1' as part")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, predicate = "part = 'p1'", name = "read_p1_replaced")
    w.read(t, predicate = "part = 'p2'", name = "read_p2_unchanged")
    w.snapshot(t)
  }

  // RW-008: ReplaceWhere with compound predicate (AND)
  test("RW_008_replacewhere_compound", "ReplaceWhere with category='a' AND value > 20",
      "write", "replaceWhere", "overwrite", "compoundPredicate") { w =>
    w.sql("CREATE TABLE tbl (key INT, value INT, category STRING) USING delta")
    w.sql("INSERT INTO tbl VALUES (1,10,'a'),(2,20,'b'),(3,30,'a'),(4,40,'b'),(5,50,'a')")
    w.sql("""INSERT INTO tbl REPLACE WHERE category = 'a' AND value > 20
      SELECT * FROM VALUES (3,300,'a'),(5,500,'a') AS t(key, value, category)""")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, version = 1, name = "read_before_replace")
    w.snapshot(t)
  }

  // RW-009: Overwrite empty table
  test("RW_009_overwrite_empty_table", "Overwrite empty table - first data via overwrite",
      "write", "overwrite", "emptyTable") { w =>
    w.sql("CREATE TABLE tbl (key INT, value STRING) USING delta")
    w.sql("INSERT OVERWRITE tbl VALUES (1,'a'),(2,'b')")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, version = 0, name = "read_empty")
    w.snapshot(t)
  }

  // RW-010: Overwrite to empty (replace all with no data)
  test("RW_010_overwrite_to_empty", "Overwrite with empty DataFrame - clears table",
      "write", "overwrite", "empty") { w =>
    w.sql("CREATE TABLE tbl (key INT, value STRING) USING delta")
    w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
    w.sql("INSERT OVERWRITE tbl SELECT key, value FROM tbl WHERE false")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_after_overwrite_empty")
    w.read(t, version = 1, name = "read_before_overwrite")
    w.snapshot(t)
  }

  // RW-011: ReplaceWhere with deletion vectors enabled
  test("RW_011_replacewhere_dvs", "ReplaceWhere with DVs enabled",
      "write", "replaceWhere", "overwrite", "dv") { w =>
    w.sql("""CREATE TABLE tbl (key INT, value INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1,10),(2,20),(3,30),(4,40),(5,50)")
    w.sql("INSERT INTO tbl REPLACE WHERE key <= 2 SELECT * FROM VALUES (1,100),(2,200) AS t(key, value)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, predicate = "key <= 2", name = "read_replaced")
    w.read(t, version = 1, name = "read_before_replace")
    w.snapshot(t)
  }

  // RW-012: Multiple overwrites in sequence
  test("RW_012_multiple_overwrites", "Two sequential overwrites - final state from last",
      "write", "overwrite") { w =>
    w.sql("CREATE TABLE tbl (key INT, value STRING) USING delta")
    w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
    w.sql("INSERT OVERWRITE tbl VALUES (10,'x'),(20,'y')")
    w.sql("INSERT OVERWRITE tbl VALUES (100,'z')")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_final")
    w.read(t, version = 1, name = "read_original")
    w.read(t, version = 2, name = "read_after_first_overwrite")
    w.snapshot(t)
  }

  // RW-013: Replace entire partition
  test("RW_013_replace_entire_partition", "Replace partition p1 with more rows",
      "write", "replaceWhere", "overwrite", "partitioned") { w =>
    w.sql("CREATE TABLE tbl (key INT, value STRING, part STRING) USING delta PARTITIONED BY (part)")
    w.sql("INSERT INTO tbl VALUES (1,'old_a','p1'),(2,'old_b','p1'),(3,'old_c','p2')")
    w.sql("""INSERT INTO tbl REPLACE WHERE part = 'p1'
      SELECT * FROM VALUES (10,'new_a','p1'),(20,'new_b','p1'),(30,'new_c','p1') AS t(key, value, part)""")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, predicate = "part = 'p1'", name = "read_p1_replaced")
    w.read(t, predicate = "part = 'p2'", name = "read_p2_unchanged")
    w.snapshot(t)
  }

  // RW-014: Overwrite with schema evolution
  test("RW_014_overwrite_schema_evolution", "Overwrite with mergeSchema adding new column",
      "write", "overwrite", "schemaEvolution") { w =>
    w.sql("CREATE TABLE tbl (key INT, value STRING) USING delta")
    w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b')")
    w.sql("SET spark.databricks.delta.schema.autoMerge.enabled=true")
    w.sql("INSERT OVERWRITE tbl SELECT 10 as key, 'x' as value, 100 as extra")
    w.sql("SET spark.databricks.delta.schema.autoMerge.enabled=false")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_after_overwrite")
    w.read(t, version = 1, name = "read_before_overwrite")
    w.snapshot(t)
  }

  // RW-015: ReplaceWhere idempotent
  test("RW_015_replacewhere_idempotent", "ReplaceWhere with same data - idempotent",
      "write", "replaceWhere", "overwrite", "idempotent") { w =>
    w.sql("CREATE TABLE tbl (key INT, value INT) USING delta")
    w.sql("INSERT INTO tbl VALUES (1,10),(2,20),(3,30)")
    w.sql("INSERT INTO tbl REPLACE WHERE key = 2 SELECT * FROM VALUES (2,20) AS t(key, value)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, version = 1, name = "read_before_replace")
    w.snapshot(t)
  }

  // ==========================================================================
  // InsertOverwrite Tests (IO-*)
  // Source: InsertOverwriteWriteCaptureSuite.scala / DeltaInsertIntoSuite.scala
  // ==========================================================================

  // IO-001: INSERT OVERWRITE full table (SQL)
  test("IO_001_insert_overwrite_full_sql", "INSERT OVERWRITE full table via SQL",
      "write", "insertOverwrite") { w =>
    w.sql("CREATE TABLE tbl (key INT, value STRING) USING delta")
    w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e')")
    w.sql("INSERT OVERWRITE tbl SELECT * FROM VALUES (10,'x'),(20,'y'),(30,'z') AS t(key, value)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_after_overwrite")
    w.read(t, version = 1, name = "read_before_overwrite")
    w.snapshot(t)
  }

  // IO-002: INSERT OVERWRITE single partition
  test("IO_002_insert_overwrite_single_partition", "INSERT OVERWRITE partition a only",
      "write", "insertOverwrite", "partitioned") { w =>
    w.sql("CREATE TABLE tbl (key INT, value STRING, part STRING) USING delta PARTITIONED BY (part)")
    w.sql("INSERT INTO tbl VALUES (1,'a1','a'),(2,'a2','a'),(3,'a3','a'),(4,'b1','b'),(5,'b2','b')")
    w.sql("INSERT OVERWRITE tbl PARTITION (part = 'a') SELECT key, value FROM VALUES (10,'new1'),(20,'new2') AS t(key, value)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, predicate = "part = 'a'", name = "read_part_a")
    w.read(t, predicate = "part = 'b'", name = "read_part_b_unchanged")
    w.snapshot(t)
  }

  // IO-003: Dynamic partition overwrite (DPO)
  test("IO_003_dynamic_partition_overwrite", "DPO - only source partitions overwritten",
      "write", "insertOverwrite", "dynamicPartition") { w =>
    w.sql("CREATE TABLE tbl (key INT, value STRING, part STRING) USING delta PARTITIONED BY (part)")
    w.sql("INSERT INTO tbl VALUES (1,'a1','p1'),(2,'a2','p1'),(3,'b1','p2'),(4,'b2','p2')")
    w.sql("SET spark.sql.sources.partitionOverwriteMode=dynamic")
    w.sql("INSERT OVERWRITE tbl PARTITION (part) VALUES (10,'new1','p1'),(20,'new2','p1')")
    w.sql("SET spark.sql.sources.partitionOverwriteMode=static")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, predicate = "part = 'p1'", name = "read_p1_replaced")
    w.read(t, predicate = "part = 'p2'", name = "read_p2_unchanged")
    w.snapshot(t)
  }

  // IO-004: INSERT OVERWRITE with new schema (overwriteSchema)
  test("IO_004_insert_overwrite_schema", "INSERT OVERWRITE with schema change adding extra column",
      "write", "insertOverwrite", "schemaOverwrite") { w =>
    w.sql("CREATE TABLE tbl (key INT, value STRING) USING delta")
    w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b')")
    w.sql("SET spark.databricks.delta.schema.autoMerge.enabled=true")
    w.sql("INSERT OVERWRITE tbl SELECT 10 as key, 'x' as value, 1.5 as extra")
    w.sql("SET spark.databricks.delta.schema.autoMerge.enabled=false")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_after_overwrite")
    w.read(t, version = 1, name = "read_before_overwrite")
    w.snapshot(t)
  }

  // IO-005: INSERT OVERWRITE empty replacement
  test("IO_005_insert_overwrite_empty", "INSERT OVERWRITE with empty result - clears table",
      "write", "insertOverwrite", "empty") { w =>
    w.sql("CREATE TABLE tbl (key INT, value STRING) USING delta")
    w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
    w.sql("INSERT OVERWRITE tbl SELECT key, value FROM tbl WHERE false")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_after_overwrite_empty")
    w.read(t, version = 1, name = "read_before_overwrite")
    w.snapshot(t)
  }

  // IO-006: INSERT OVERWRITE multiple partitions
  test("IO_006_insert_overwrite_multi_partition", "DPO overwrite p1 and p2, leave p3",
      "write", "insertOverwrite", "multiPartition") { w =>
    w.sql("CREATE TABLE tbl (key INT, value STRING, part STRING) USING delta PARTITIONED BY (part)")
    w.sql("INSERT INTO tbl VALUES (1,'a1','p1'),(2,'a2','p1'),(3,'b1','p2'),(4,'b2','p2'),(5,'c1','p3'),(6,'c2','p3')")
    w.sql("SET spark.sql.sources.partitionOverwriteMode=dynamic")
    w.sql("INSERT OVERWRITE tbl PARTITION (part) VALUES (10,'x1','p1'),(30,'x2','p2')")
    w.sql("SET spark.sql.sources.partitionOverwriteMode=static")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, predicate = "part = 'p3'", name = "read_p3_unchanged")
    w.snapshot(t)
  }

  // IO-007: INSERT OVERWRITE with SQL VALUES
  test("IO_007_insert_overwrite_values", "INSERT OVERWRITE with SQL VALUES",
      "write", "insertOverwrite") { w =>
    w.sql("CREATE TABLE tbl (key INT, value STRING) USING delta")
    w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
    w.sql("INSERT OVERWRITE tbl VALUES (1,'x'),(2,'y')")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_after_overwrite")
    w.read(t, version = 1, name = "read_before_overwrite")
    w.snapshot(t)
  }

  // IO-008: INSERT OVERWRITE preserving other partitions
  test("IO_008_insert_overwrite_preserve_partitions", "Overwrite p1 only, p2 and p3 untouched",
      "write", "insertOverwrite", "partitioned") { w =>
    w.sql("CREATE TABLE tbl (key INT, value STRING, part STRING) USING delta PARTITIONED BY (part)")
    w.sql("INSERT INTO tbl VALUES (1,'a1','p1'),(2,'a2','p1'),(3,'b1','p2'),(4,'b2','p2'),(5,'c1','p3'),(6,'c2','p3')")
    w.sql("INSERT OVERWRITE tbl PARTITION (part = 'p1') SELECT key, value FROM VALUES (10,'new1') AS t(key, value)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, predicate = "part = 'p1'", name = "read_p1_replaced")
    w.read(t, predicate = "part = 'p2'", name = "read_p2_unchanged")
    w.read(t, predicate = "part = 'p3'", name = "read_p3_unchanged")
    w.snapshot(t)
  }

  // IO-009: Dynamic partition overwrite adding new partition
  test("IO_009_dpo_add_new_partition", "DPO adds new partition p3, p1 and p2 untouched",
      "write", "insertOverwrite", "dynamicPartition", "newPartition") { w =>
    w.sql("CREATE TABLE tbl (key INT, value STRING, part STRING) USING delta PARTITIONED BY (part)")
    w.sql("INSERT INTO tbl VALUES (1,'a1','p1'),(2,'a2','p1'),(3,'b1','p2')")
    w.sql("SET spark.sql.sources.partitionOverwriteMode=dynamic")
    w.sql("INSERT OVERWRITE tbl PARTITION (part) VALUES (10,'new1','p3'),(20,'new2','p3')")
    w.sql("SET spark.sql.sources.partitionOverwriteMode=static")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, predicate = "part = 'p1'", name = "read_p1_unchanged")
    w.read(t, predicate = "part = 'p3'", name = "read_p3_new")
    w.snapshot(t)
  }

  // IO-010: INSERT OVERWRITE with deletion vectors
  test("IO_010_insert_overwrite_dvs", "INSERT OVERWRITE on table with prior DV deletes",
      "write", "insertOverwrite", "dv") { w =>
    w.sql("""CREATE TABLE tbl (key INT, value INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1,10),(2,20),(3,30),(4,40),(5,50)")
    w.sql("DELETE FROM tbl WHERE key IN (2, 4)")
    w.sql("INSERT OVERWRITE tbl VALUES (10,100),(20,200),(30,300)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_after_overwrite")
    w.read(t, version = 2, name = "read_after_dv_delete")
    w.snapshot(t)
  }

  // IO-011: INSERT OVERWRITE with transformation
  test("IO_011_insert_overwrite_transform", "INSERT OVERWRITE with value doubled",
      "write", "insertOverwrite", "transform") { w =>
    w.sql("CREATE TABLE tbl (key INT, value INT) USING delta")
    w.sql("INSERT INTO tbl VALUES (1,10),(2,20),(3,30)")
    w.sql("CREATE TEMPORARY VIEW io011_source AS SELECT * FROM VALUES (1,10),(2,20),(3,30) AS t(key, value)")
    w.sql("INSERT OVERWRITE tbl SELECT key, value * 2 as value FROM io011_source")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_after_overwrite")
    w.read(t, version = 1, name = "read_before_overwrite")
    w.snapshot(t)
  }

  // IO-012: Repeated INSERT OVERWRITE (idempotent)
  test("IO_012_insert_overwrite_idempotent", "Second overwrite with same data",
      "write", "insertOverwrite", "idempotent") { w =>
    w.sql("CREATE TABLE tbl (key INT, value STRING) USING delta")
    w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
    w.sql("INSERT OVERWRITE tbl VALUES (10,'x'),(20,'y')")
    w.sql("INSERT OVERWRITE tbl VALUES (10,'x'),(20,'y')")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_final")
    w.read(t, version = 1, name = "read_original")
    w.snapshot(t)
  }

  // IO-013: INSERT OVERWRITE with nested partition columns
  test("IO_013_insert_overwrite_nested_partition", "Overwrite (year=2024,month=1) partition",
      "write", "insertOverwrite", "nestedPartition") { w =>
    w.sql("CREATE TABLE tbl (key INT, value STRING, year INT, month INT) USING delta PARTITIONED BY (year, month)")
    w.sql("INSERT INTO tbl VALUES (1,'a',2024,1),(2,'b',2024,1),(3,'c',2024,2),(4,'d',2024,2),(5,'e',2025,1)")
    w.sql("INSERT OVERWRITE tbl PARTITION (year = 2024, month = 1) SELECT key, value FROM VALUES (10,'new1'),(20,'new2') AS t(key, value)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, predicate = "year = 2024 AND month = 1", name = "read_replaced_partition")
    w.read(t, predicate = "year = 2024 AND month = 2", name = "read_unchanged_partition")
    w.snapshot(t)
  }

  // IO-014: Dynamic partition overwrite with larger replacement
  test("IO_014_dpo_larger_replacement", "DPO p2 grows from 2 to 5 rows",
      "write", "insertOverwrite", "dynamicPartition") { w =>
    w.sql("CREATE TABLE tbl (key INT, value STRING, part STRING) USING delta PARTITIONED BY (part)")
    w.sql("INSERT INTO tbl VALUES (1,'a','p1'),(2,'b','p2'),(3,'c','p2')")
    w.sql("SET spark.sql.sources.partitionOverwriteMode=dynamic")
    w.sql("INSERT OVERWRITE tbl PARTITION (part) VALUES (10,'x1','p2'),(20,'x2','p2'),(30,'x3','p2'),(40,'x4','p2'),(50,'x5','p2')")
    w.sql("SET spark.sql.sources.partitionOverwriteMode=static")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, predicate = "part = 'p1'", name = "read_p1_unchanged")
    w.read(t, predicate = "part = 'p2'", name = "read_p2_grown")
    w.snapshot(t)
  }

  // IO-015: INSERT OVERWRITE on unpartitioned table
  test("IO_015_insert_overwrite_unpartitioned", "Full replace on unpartitioned table",
      "write", "insertOverwrite") { w =>
    w.sql("CREATE TABLE tbl (key INT, value STRING) USING delta")
    w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d')")
    w.sql("INSERT OVERWRITE tbl VALUES (10,'x'),(20,'y'),(30,'z')")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_after_overwrite")
    w.read(t, version = 1, name = "read_before_overwrite")
    w.snapshot(t)
  }

  // ==========================================================================
  // Schema Evolution Tests (SE-*)
  // Source: InsertSchemaEvolutionWriteCaptureSuite.scala
  // ==========================================================================

  // SE-001: INSERT with extra column (auto schema evolution)
  test("SE_001_insert_extra_column", "Auto schema evolution adds extra column",
      "write", "insert", "schemaEvolution") { w =>
    w.sql("CREATE TABLE tbl (key INT, value STRING) USING delta")
    w.sql("INSERT INTO tbl VALUES (1,'a')")
    w.sql("SET spark.databricks.delta.schema.autoMerge.enabled=true")
    w.sql("INSERT INTO tbl SELECT 1 as key, 'a' as value, 100 as extra")
    w.sql("SET spark.databricks.delta.schema.autoMerge.enabled=false")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, version = 1, name = "read_before_evolution")
    w.snapshot(t)
  }

  // SE-002: INSERT with missing column (nullable fills null)
  test("SE_002_insert_missing_column", "Missing column filled with null",
      "write", "insert", "schemaEvolution") { w =>
    w.sql("CREATE TABLE tbl (key INT, value STRING, extra INT) USING delta")
    w.sql("INSERT INTO tbl VALUES (1,'a',100)")
    w.sql("SET spark.databricks.delta.schema.autoMerge.enabled=true")
    w.sql("INSERT INTO tbl (key, value) VALUES (2,'b')")
    w.sql("SET spark.databricks.delta.schema.autoMerge.enabled=false")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  // SE-003: INSERT with reordered columns
  test("SE_003_insert_reordered_columns", "Columns matched by name despite different order",
      "write", "insert", "schemaEvolution") { w =>
    w.sql("CREATE TABLE tbl (key INT, value STRING, score DOUBLE) USING delta")
    w.sql("INSERT INTO tbl VALUES (1,'a',1.0)")
    w.sql("INSERT INTO tbl (score, key, value) VALUES (2.0, 2, 'b')")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  // SE-004: INSERT with nested struct evolution
  test("SE_004_insert_nested_struct_evolution", "Add field to nested struct via mergeSchema",
      "write", "insert", "schemaEvolution", "nestedStruct") { w =>
    w.sql("CREATE TABLE tbl (id INT, info STRUCT<name: STRING>) USING delta")
    w.sql("INSERT INTO tbl VALUES (1, named_struct('name','alice'))")
    w.sql("SET spark.databricks.delta.schema.autoMerge.enabled=true")
    w.sql("INSERT INTO tbl SELECT 2 as id, named_struct('name','bob','age',25) as info")
    w.sql("SET spark.databricks.delta.schema.autoMerge.enabled=false")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, version = 1, name = "read_before_evolution")
    w.snapshot(t)
  }

  // SE-005: INSERT with type widening (int to long)
  test("SE_005_insert_type_widening", "Type widens from INT to LONG via mergeSchema",
      "write", "insert", "schemaEvolution", "typeWidening") { w =>
    w.sql("CREATE TABLE tbl (id INT, value INT) USING delta")
    w.sql("INSERT INTO tbl VALUES (1, 10)")
    w.sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.enableTypeWidening' = 'true')")
    w.sql("SET spark.databricks.delta.schema.autoMerge.enabled=true")
    w.sql("INSERT INTO tbl SELECT 2 as id, CAST(20000000000 AS BIGINT) as value")
    w.sql("SET spark.databricks.delta.schema.autoMerge.enabled=false")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, version = 1, name = "read_before_widening")
    w.snapshot(t)
  }

  // SE-006: INSERT OVERWRITE with schema evolution
  test("SE_006_overwrite_schema_evolution", "Overwrite adds extra column via schema evolution",
      "write", "insert", "schemaEvolution", "overwrite") { w =>
    w.sql("CREATE TABLE tbl (key INT, value STRING) USING delta")
    w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b')")
    w.sql("SET spark.databricks.delta.schema.autoMerge.enabled=true")
    w.sql("INSERT OVERWRITE tbl SELECT 10 as key, 'x' as value, 100 as extra")
    w.sql("SET spark.databricks.delta.schema.autoMerge.enabled=false")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_after_overwrite")
    w.read(t, version = 1, name = "read_before_overwrite")
    w.snapshot(t)
  }

  // SE-007: INSERT with multiple extra columns
  test("SE_007_insert_multiple_extra_columns", "Schema evolves to add age, city, rating",
      "write", "insert", "schemaEvolution") { w =>
    w.sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
    w.sql("INSERT INTO tbl VALUES (1,'a')")
    w.sql("SET spark.databricks.delta.schema.autoMerge.enabled=true")
    w.sql("INSERT INTO tbl SELECT 2 as id, 'b' as name, 25 as age, 'NYC' as city, 3.5 as rating")
    w.sql("SET spark.databricks.delta.schema.autoMerge.enabled=false")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, version = 1, name = "read_before_evolution")
    w.snapshot(t)
  }

  // SE-008: INSERT into partitioned table with schema evolution
  test("SE_008_insert_partitioned_schema_evolution", "Schema evolves on partitioned table",
      "write", "insert", "schemaEvolution", "partitioned") { w =>
    w.sql("CREATE TABLE tbl (id INT, value STRING, part STRING) USING delta PARTITIONED BY (part)")
    w.sql("INSERT INTO tbl VALUES (1,'a','p1'),(2,'b','p2')")
    w.sql("SET spark.databricks.delta.schema.autoMerge.enabled=true")
    w.sql("INSERT INTO tbl SELECT 3 as id, 'c' as value, 100 as score, 'p1' as part")
    w.sql("SET spark.databricks.delta.schema.autoMerge.enabled=false")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, predicate = "part = 'p1'", name = "read_p1")
    w.snapshot(t)
  }

  // SE-009: INSERT with nested array of structs evolution
  test("SE_009_insert_array_struct_evolution", "Array element struct evolves to add price field",
      "write", "insert", "schemaEvolution", "arrayOfStructs") { w =>
    w.sql("CREATE TABLE tbl (id INT, items ARRAY<STRUCT<name: STRING>>) USING delta")
    w.sql("INSERT INTO tbl VALUES (1, array(named_struct('name','item1')))")
    w.sql("SET spark.databricks.delta.schema.autoMerge.enabled=true")
    w.sql("INSERT INTO tbl SELECT 2 as id, array(named_struct('name','item2','price',9.99)) as items")
    w.sql("SET spark.databricks.delta.schema.autoMerge.enabled=false")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, version = 1, name = "read_before_evolution")
    w.snapshot(t)
  }

  // SE-010: INSERT with map type (no evolution)
  test("SE_010_insert_map_type", "INSERT map type - same schema, no evolution",
      "write", "insert", "schemaEvolution", "mapType") { w =>
    w.sql("CREATE TABLE tbl (id INT, props MAP<STRING, STRING>) USING delta")
    w.sql("INSERT INTO tbl VALUES (1, map('a','1'))")
    w.sql("INSERT INTO tbl VALUES (2, map('b','2'))")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  // SE-011: Consecutive inserts with incremental schema evolution
  test("SE_011_incremental_evolution", "3 inserts: (id) -> (id,name) -> (id,name,age)",
      "write", "insert", "schemaEvolution", "incremental") { w =>
    w.sql("CREATE TABLE tbl (id INT) USING delta")
    w.sql("INSERT INTO tbl VALUES (1)")
    w.sql("SET spark.databricks.delta.schema.autoMerge.enabled=true")
    w.sql("INSERT INTO tbl SELECT 2 as id, 'bob' as name")
    w.sql("INSERT INTO tbl SELECT 3 as id, 'carol' as name, 30 as age")
    w.sql("SET spark.databricks.delta.schema.autoMerge.enabled=false")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, version = 1, name = "read_v1_id_only")
    w.read(t, version = 2, name = "read_v2_id_name")
    w.snapshot(t)
  }

  // SE-012: INSERT with column name case difference
  test("SE_012_insert_case_insensitive", "Case-insensitive column name match",
      "write", "insert", "schemaEvolution", "caseInsensitive") { w =>
    w.sql("CREATE TABLE tbl (Id INT, Name STRING) USING delta")
    w.sql("INSERT INTO tbl VALUES (1,'alice')")
    w.sql("INSERT INTO tbl (id, name) VALUES (2,'bob')")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  // SE-013: INSERT with null typed extra column
  test("SE_013_insert_null_extra_column", "Extra STRING column with null value",
      "write", "insert", "schemaEvolution") { w =>
    w.sql("CREATE TABLE tbl (id INT, value STRING) USING delta")
    w.sql("INSERT INTO tbl VALUES (1,'a')")
    w.sql("SET spark.databricks.delta.schema.autoMerge.enabled=true")
    w.sql("INSERT INTO tbl SELECT 2 as id, 'b' as value, CAST(null AS STRING) as extra")
    w.sql("SET spark.databricks.delta.schema.autoMerge.enabled=false")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  // SE-014: INSERT with null values for existing columns
  test("SE_014_insert_null_values", "INSERT with null for name and score columns",
      "write", "insert", "schemaEvolution", "nullValues") { w =>
    w.sql("CREATE TABLE tbl (id INT, name STRING, score DOUBLE) USING delta")
    w.sql("INSERT INTO tbl VALUES (1,'alice',95.0)")
    w.sql("INSERT INTO tbl VALUES (2, null, null)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  // SE-015: INSERT OVERWRITE partition with schema evolution
  test("SE_015_overwrite_partition_schema_evolution", "Overwrite partition p1 with extra column",
      "write", "insert", "schemaEvolution", "overwrite", "partitioned") { w =>
    w.sql("CREATE TABLE tbl (id INT, value STRING, part STRING) USING delta PARTITIONED BY (part)")
    w.sql("INSERT INTO tbl VALUES (1,'a','p1'),(2,'b','p2')")
    w.sql("SET spark.databricks.delta.schema.autoMerge.enabled=true")
    w.sql("""INSERT INTO tbl REPLACE WHERE part = 'p1'
      SELECT 10 as id, 'x' as value, 100 as extra, 'p1' as part""")
    w.sql("SET spark.databricks.delta.schema.autoMerge.enabled=false")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, predicate = "part = 'p1'", name = "read_p1_replaced")
    w.read(t, predicate = "part = 'p2'", name = "read_p2_unchanged")
    w.snapshot(t)
  }

}.runAll()
