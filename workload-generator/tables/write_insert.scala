/**
 * Insert and CTAS workloads converted from runtime capture suites.
 *
 * Sources:
 *   - DeltaInsertIntoSuiteWriteCapture.scala (DII-001 through DII-018)
 *   - CTASWriteCaptureSuite.scala (CT-001 through CT-015)
 *
 * Uses createTableOp + insertOp for simple create/append patterns.
 * Falls back to sql() for INSERT OVERWRITE, CTAS, schema evolution,
 * and complex SELECT-based inserts (no dedicated Op methods for those).
 */

new WorkloadSuite("write_insert") {

  // ==========================================================================
  // DII-001: insert overwrite with selecting constants (partitioned)
  // ==========================================================================
  test("dii001_insert_overwrite_partition_constants",
      "Partitioned table with INSERT OVERWRITE using static partition clauses",
      "write", "insert", "overwrite", "partitioned") { w =>
    val t = w.createTableOp("tbl",
      schema = Seq(Col("a", "INT"), Col("b", "INT"), Col("c", "INT")),
      partitionColumns = Seq("b", "c"))
    w.sql("INSERT OVERWRITE TABLE tbl PARTITION (c=3) SELECT 1, 2")
    w.sql("INSERT OVERWRITE TABLE tbl PARTITION (b=2, c=3) SELECT 1")
    w.sql("INSERT OVERWRITE TABLE tbl PARTITION (b=2, c) SELECT 1, 3")
    w.writeSpec(t)
    w.read(t, name = "read_final")
    w.read(t, version = 1, name = "after_first_overwrite")
    w.read(t, version = 2, name = "after_second_overwrite")
    w.read(t, version = 3, name = "after_third_overwrite")
    w.snapshot(t)
  }

  // ==========================================================================
  // DII-002: append by name
  // ==========================================================================
  test("dii002_append_by_name",
      "INSERT INTO with explicit column list and different column orders",
      "write", "insert") { w =>
    val t = w.createTableOp("tbl",
      schema = Seq(Col("id", "BIGINT"), Col("data", "STRING")))
    w.sql("INSERT INTO tbl(id, data) VALUES(1, 'a')")
    w.sql("INSERT INTO tbl(data, id) VALUES('b', 2)")
    w.sql("INSERT INTO tbl(data, id) VALUES('c', 3)")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, version = 1, name = "after_first_insert")
    w.read(t, version = 2, name = "after_second_insert")
    w.snapshot(t)
  }

  // ==========================================================================
  // DII-003: overwrite by name
  // ==========================================================================
  test("dii003_overwrite_by_name",
      "INSERT OVERWRITE with explicit column list and different column orders",
      "write", "insert", "overwrite") { w =>
    val t = w.createTableOp("tbl",
      schema = Seq(Col("id", "BIGINT"), Col("data", "STRING")))
    w.sql("INSERT OVERWRITE tbl(id, data) VALUES(1, 'a')")
    w.sql("INSERT OVERWRITE tbl(data, id) VALUES('b', 2)")
    w.sql("INSERT OVERWRITE tbl(data, id) VALUES('c', 3)")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, version = 1, name = "after_first_overwrite")
    w.read(t, version = 2, name = "after_second_overwrite")
    w.snapshot(t)
  }

  // ==========================================================================
  // DII-004: basic append
  // ==========================================================================
  test("dii004_append_basic",
      "Basic INSERT INTO with three rows",
      "write", "insert") { w =>
    val t = w.createTableOp("tbl",
      schema = Seq(Col("id", "BIGINT"), Col("data", "STRING")))
    w.insertOp(t, Seq(
      Map("id" -> 1, "data" -> "a"),
      Map("id" -> 2, "data" -> "b"),
      Map("id" -> 3, "data" -> "c")))
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  // ==========================================================================
  // DII-005: append partitioned table
  // ==========================================================================
  test("dii005_append_partitioned",
      "INSERT INTO a partitioned table",
      "write", "insert", "partitioned") { w =>
    val t = w.createTableOp("tbl",
      schema = Seq(Col("id", "BIGINT"), Col("data", "STRING")),
      partitionColumns = Seq("id"))
    w.insertOp(t, Seq(
      Map("id" -> 1, "data" -> "a"),
      Map("id" -> 2, "data" -> "b"),
      Map("id" -> 3, "data" -> "c")))
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, predicate = "id = 2", name = "read_partition_2")
    w.snapshot(t)
  }

  // ==========================================================================
  // DII-006: overwrite non-partitioned table
  // ==========================================================================
  test("dii006_overwrite_non_partitioned",
      "INSERT then INSERT OVERWRITE on non-partitioned table",
      "write", "insert", "overwrite") { w =>
    val t = w.createTableOp("tbl",
      schema = Seq(Col("id", "BIGINT"), Col("data", "STRING")))
    w.insertOp(t, Seq(
      Map("id" -> 1, "data" -> "a"),
      Map("id" -> 2, "data" -> "b"),
      Map("id" -> 3, "data" -> "c")))
    w.sql("INSERT OVERWRITE tbl VALUES (4, 'd'), (5, 'e'), (6, 'f')")
    w.writeSpec(t)
    w.read(t, name = "read_after_overwrite")
    w.read(t, version = 1, name = "read_before_overwrite")
    w.snapshot(t)
  }

  // ==========================================================================
  // DII-007: overwrite partitioned table in static mode
  // ==========================================================================
  test("dii007_overwrite_partitioned_static",
      "INSERT OVERWRITE partitioned table in static partition overwrite mode",
      "write", "insert", "overwrite", "partitioned") { w =>
    val t = w.createTableOp("tbl",
      schema = Seq(Col("id", "BIGINT"), Col("data", "STRING")),
      partitionColumns = Seq("id"))
    w.insertOp(t, Seq(
      Map("id" -> 2, "data" -> "dummy"),
      Map("id" -> 4, "data" -> "keep")))
    w.sql("SET spark.sql.sources.partitionOverwriteMode = static")
    w.sql("INSERT OVERWRITE tbl VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    w.writeSpec(t)
    w.read(t, name = "read_after_overwrite")
    w.read(t, version = 1, name = "read_before_overwrite")
    w.snapshot(t)
  }

  // ==========================================================================
  // DII-008: append to partitioned table with static clause
  // ==========================================================================
  test("dii008_append_partitioned_static_clause",
      "INSERT INTO with PARTITION clause specifying partition value",
      "write", "insert", "partitioned") { w =>
    val t = w.createTableOp("tbl",
      schema = Seq(Col("id", "BIGINT"), Col("data", "STRING")),
      partitionColumns = Seq("id"))
    w.sql("INSERT INTO tbl PARTITION (id = 23) VALUES ('a'), ('b'), ('c')")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, predicate = "id = 23", name = "read_partition_23")
    w.snapshot(t)
  }

  // ==========================================================================
  // DII-009: overwrite dynamic clause static mode
  // ==========================================================================
  test("dii009_overwrite_dynamic_clause_static_mode",
      "INSERT OVERWRITE with PARTITION(id) in static overwrite mode",
      "write", "insert", "overwrite", "partitioned") { w =>
    val t = w.createTableOp("tbl",
      schema = Seq(Col("id", "BIGINT"), Col("data", "STRING")),
      partitionColumns = Seq("id"))
    w.insertOp(t, Seq(
      Map("id" -> 2, "data" -> "dummy"),
      Map("id" -> 4, "data" -> "also-deleted")))
    // Create source data as a temp view
    w.sql("CREATE OR REPLACE TEMP VIEW src_dii009 AS SELECT * FROM VALUES (1, 'a'), (2, 'b'), (3, 'c') AS t(id, data)")
    w.sql("SET spark.sql.sources.partitionOverwriteMode = static")
    w.sql("INSERT OVERWRITE TABLE tbl PARTITION (id) SELECT * FROM src_dii009")
    w.writeSpec(t)
    w.read(t, name = "read_after_overwrite")
    w.read(t, version = 1, name = "read_before_overwrite")
    w.snapshot(t)
  }

  // ==========================================================================
  // DII-010: overwrite dynamic clause dynamic mode
  // ==========================================================================
  test("dii010_overwrite_dynamic_clause_dynamic_mode",
      "INSERT OVERWRITE with PARTITION(id) in dynamic mode preserves unaffected partitions",
      "write", "insert", "overwrite", "partitioned") { w =>
    val t = w.createTableOp("tbl",
      schema = Seq(Col("id", "BIGINT"), Col("data", "STRING")),
      partitionColumns = Seq("id"))
    w.insertOp(t, Seq(
      Map("id" -> 2, "data" -> "dummy"),
      Map("id" -> 4, "data" -> "keep")))
    w.sql("CREATE OR REPLACE TEMP VIEW src_dii010 AS SELECT * FROM VALUES (1, 'a'), (2, 'b'), (3, 'c') AS t(id, data)")
    w.sql("SET spark.sql.sources.partitionOverwriteMode = dynamic")
    w.sql("INSERT OVERWRITE TABLE tbl PARTITION (id) SELECT * FROM src_dii010")
    w.writeSpec(t)
    w.read(t, name = "read_after_overwrite")
    w.read(t, version = 1, name = "read_before_overwrite")
    w.read(t, predicate = "id = 4", name = "read_preserved_partition")
    w.snapshot(t)
  }

  // ==========================================================================
  // DII-011: schema evolution via mergeSchema
  // ==========================================================================
  test("dii011_schema_evolution_merge_schema",
      "INSERT with extra column and auto schema merge enabled",
      "write", "insert", "schema_evolution") { w =>
    val t = w.createTableOp("tbl",
      schema = Seq(Col("id", "BIGINT"), Col("data", "STRING")))
    w.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")
    w.sql("INSERT INTO tbl VALUES (1, 'a', 'mango')")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  // ==========================================================================
  // DII-012: overwrite missing clause static mode
  // ==========================================================================
  test("dii012_overwrite_missing_clause_static",
      "INSERT OVERWRITE without PARTITION clause in static mode replaces all data",
      "write", "insert", "overwrite", "partitioned") { w =>
    val t = w.createTableOp("tbl",
      schema = Seq(Col("id", "BIGINT"), Col("data", "STRING")),
      partitionColumns = Seq("id"))
    w.insertOp(t, Seq(
      Map("id" -> 2, "data" -> "dummy"),
      Map("id" -> 4, "data" -> "also-deleted")))
    w.sql("CREATE OR REPLACE TEMP VIEW src_dii012 AS SELECT * FROM VALUES (1, 'a'), (2, 'b'), (3, 'c') AS t(id, data)")
    w.sql("SET spark.sql.sources.partitionOverwriteMode = static")
    w.sql("INSERT OVERWRITE TABLE tbl SELECT * FROM src_dii012")
    w.writeSpec(t)
    w.read(t, name = "read_after_overwrite")
    w.read(t, version = 1, name = "read_before_overwrite")
    w.snapshot(t)
  }

  // ==========================================================================
  // DII-013: insert nested struct literal
  // ==========================================================================
  test("dii013_insert_nested_struct",
      "INSERT of nested struct values into delta table",
      "write", "insert", "nested") { w =>
    val t = w.createTableOp("tbl",
      schema = Seq(
        Col("num", "INT"),
        Col("text", "STRING"),
        Col("s", "STRUCT<a: STRING, s2: STRUCT<c: STRING, d: STRING>, b: STRING>")))
    w.sql("INSERT INTO tbl VALUES (1, 'a', struct('a', struct('c', 'd'), 'b'))")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  // ==========================================================================
  // DII-014: CTAS basic
  // ==========================================================================
  test("dii014_ctas_basic",
      "CREATE TABLE AS SELECT with RANGE function producing 100 rows",
      "write", "insert", "ctas") { w =>
    w.sql("CREATE TABLE tbl USING delta AS SELECT id, CAST(id AS STRING) AS data FROM RANGE(100)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, predicate = "id < 10", name = "read_first_10")
    w.snapshot(t)
  }

  // ==========================================================================
  // DII-015: CTAS partitioned
  // ==========================================================================
  test("dii015_ctas_partitioned",
      "CTAS with PARTITIONED BY producing 100 rows across 5 partitions",
      "write", "insert", "ctas", "partitioned") { w =>
    w.sql("CREATE TABLE tbl USING delta PARTITIONED BY (part) AS SELECT id, CAST(id % 5 AS INT) AS part FROM RANGE(100)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, predicate = "part = 0", name = "read_partition_0")
    w.read(t, predicate = "part = 3", name = "read_partition_3")
    w.snapshot(t)
  }

  // ==========================================================================
  // DII-016: INSERT with various data types
  // ==========================================================================
  test("dii016_various_data_types",
      "INSERT with INT, BIGINT, FLOAT, DOUBLE, STRING, BOOLEAN, DATE, and NULLs",
      "write", "insert", "types") { w =>
    val t = w.createTableOp("tbl",
      schema = Seq(
        Col("i", "INT"), Col("l", "BIGINT"), Col("f", "FLOAT"),
        Col("d", "DOUBLE"), Col("s", "STRING"), Col("b", "BOOLEAN"), Col("dt", "DATE")))
    w.sql("""INSERT INTO tbl VALUES
      (1, 100, 1.5, 2.5, 'hello', true, DATE'2024-01-15'),
      (2, 200, 3.14, 6.28, 'world', false, DATE'2024-06-30'),
      (NULL, NULL, NULL, NULL, NULL, NULL, NULL)""")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, predicate = "i IS NOT NULL", name = "read_non_null")
    w.read(t, predicate = "b = true", name = "read_boolean_true")
    w.snapshot(t)
  }

  // ==========================================================================
  // DII-017: INSERT OVERWRITE with CDC enabled
  // ==========================================================================
  test("dii017_insert_overwrite_with_cdc",
      "INSERT OVERWRITE on CDC-enabled table produces change data files",
      "write", "insert", "overwrite", "cdc") { w =>
    val t = w.createTableOp("tbl",
      schema = Seq(Col("id", "INT"), Col("data", "STRING")),
      properties = Map("delta.enableChangeDataFeed" -> "true"))
    w.insertOp(t, Seq(
      Map("id" -> 1, "data" -> "a"),
      Map("id" -> 2, "data" -> "b"),
      Map("id" -> 3, "data" -> "c")))
    w.sql("INSERT OVERWRITE tbl VALUES (4, 'd'), (5, 'e')")
    w.writeSpec(t)
    w.read(t, name = "read_after_overwrite")
    w.read(t, version = 1, name = "read_before_overwrite")
    w.cdf(t, startVersion = 2, endVersion = 2)
    w.snapshot(t)
  }

  // ==========================================================================
  // DII-018: INSERT with map and array types
  // ==========================================================================
  test("dii018_insert_map_and_array",
      "INSERT of ARRAY and MAP columns including empty collections",
      "write", "insert", "complex_types") { w =>
    val t = w.createTableOp("tbl",
      schema = Seq(
        Col("id", "INT"),
        Col("tags", "ARRAY<STRING>"),
        Col("props", "MAP<STRING, INT>")))
    w.sql("""INSERT INTO tbl VALUES
      (1, array('a', 'b'), map('x', 1, 'y', 2)),
      (2, array('c'), map('z', 3)),
      (3, array(), map())""")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, predicate = "id = 1", name = "read_id_1")
    w.snapshot(t)
  }

  // ==========================================================================
  // CT-001: Basic CTAS with DataFrame API (via SQL equivalent)
  // ==========================================================================
  test("ct001_ctas_basic_values",
      "CTAS from VALUES producing 3 rows with key/value columns",
      "write", "ctas") { w =>
    w.sql("CREATE TABLE tbl USING delta AS SELECT * FROM VALUES (1, 'a'), (2, 'b'), (3, 'c') AS t(key, value)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  // ==========================================================================
  // CT-002: CTAS with SQL syntax (same as CT-001 but explicit SQL)
  // ==========================================================================
  test("ct002_ctas_sql",
      "CTAS using SQL syntax with temp view source",
      "write", "ctas") { w =>
    w.sql("CREATE OR REPLACE TEMP VIEW src_ct002 AS SELECT * FROM VALUES (1, 'a'), (2, 'b'), (3, 'c') AS t(key, value)")
    w.sql("CREATE TABLE tbl USING delta AS SELECT * FROM src_ct002")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  // ==========================================================================
  // CT-003: CTAS with partitioning
  // ==========================================================================
  test("ct003_ctas_partitioned",
      "CTAS with PARTITIONED BY producing 3 rows across 2 partitions",
      "write", "ctas", "partitioned") { w =>
    w.sql("CREATE OR REPLACE TEMP VIEW src_ct003 AS SELECT * FROM VALUES (1, 'a', 'p1'), (2, 'b', 'p2'), (3, 'c', 'p1') AS t(key, value, part)")
    w.sql("CREATE TABLE tbl USING delta PARTITIONED BY (part) AS SELECT * FROM src_ct003")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, predicate = "part = 'p1'", name = "read_p1")
    w.read(t, predicate = "part = 'p2'", name = "read_p2")
    w.snapshot(t)
  }

  // ==========================================================================
  // CT-004: CTAS with table properties (appendOnly)
  // ==========================================================================
  test("ct004_ctas_with_properties",
      "CTAS with delta.appendOnly property set to true",
      "write", "ctas") { w =>
    w.sql("""CREATE TABLE tbl USING delta
      TBLPROPERTIES ('delta.appendOnly' = 'true')
      AS SELECT * FROM VALUES (1, 'a'), (2, 'b'), (3, 'c') AS t(key, value)""")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  // ==========================================================================
  // CT-005: INSERT INTO SELECT from another table
  // ==========================================================================
  test("ct005_insert_select_from_table",
      "Create target, create source, INSERT INTO SELECT from source",
      "write", "insert") { w =>
    // Create target with initial data
    w.sql("CREATE TABLE tbl USING delta AS SELECT * FROM VALUES (1, 'a'), (2, 'b') AS t(key, value)")
    // Create source as a temp view
    w.sql("CREATE OR REPLACE TEMP VIEW src_ct005 AS SELECT * FROM VALUES (3, 'c'), (4, 'd') AS t(key, value)")
    w.sql("INSERT INTO tbl SELECT * FROM src_ct005")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, version = 0, name = "read_initial")
    w.snapshot(t)
  }

  // ==========================================================================
  // CT-006: INSERT INTO SELECT with transformation
  // ==========================================================================
  test("ct006_insert_select_transform",
      "INSERT INTO SELECT with value * 2 transformation",
      "write", "insert") { w =>
    w.sql("CREATE TABLE tbl USING delta AS SELECT * FROM VALUES (1, 10) AS t(key, value)")
    w.sql("CREATE OR REPLACE TEMP VIEW src_ct006 AS SELECT * FROM VALUES (2, 20), (3, 30) AS t(key, value)")
    w.sql("INSERT INTO tbl SELECT key, value * 2 as value FROM src_ct006")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, version = 0, name = "read_initial")
    w.snapshot(t)
  }

  // ==========================================================================
  // CT-007: INSERT INTO SELECT with WHERE clause
  // ==========================================================================
  test("ct007_insert_select_with_filter",
      "INSERT INTO SELECT with WHERE value > 10 filter",
      "write", "insert") { w =>
    w.sql("CREATE TABLE tbl USING delta AS SELECT * FROM VALUES (1, 100) AS t(key, value)")
    w.sql("CREATE OR REPLACE TEMP VIEW src_ct007 AS SELECT * FROM VALUES (2, 5), (3, 15), (4, 25), (5, 3) AS t(key, value)")
    w.sql("INSERT INTO tbl SELECT * FROM src_ct007 WHERE value > 10")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, version = 0, name = "read_initial")
    w.snapshot(t)
  }

  // ==========================================================================
  // CT-008: INSERT INTO SELECT with JOIN
  // ==========================================================================
  test("ct008_insert_select_join",
      "INSERT INTO SELECT with inner JOIN between two sources",
      "write", "insert") { w =>
    val t = w.createTableOp("tbl",
      schema = Seq(Col("key", "INT"), Col("name", "STRING")))
    w.sql("CREATE OR REPLACE TEMP VIEW table_a AS SELECT * FROM VALUES (1, 100), (2, 200), (3, 300) AS t(key, amount)")
    w.sql("CREATE OR REPLACE TEMP VIEW table_b AS SELECT * FROM VALUES (1, 'alice'), (2, 'bob'), (4, 'dave') AS t(key, name)")
    w.sql("INSERT INTO tbl SELECT a.key, b.name FROM table_a a JOIN table_b b ON a.key = b.key")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  // ==========================================================================
  // CT-009: INSERT INTO SELECT with aggregation
  // ==========================================================================
  test("ct009_insert_select_aggregate",
      "INSERT INTO SELECT with SUM GROUP BY aggregation",
      "write", "insert") { w =>
    val t = w.createTableOp("tbl",
      schema = Seq(Col("category", "STRING"), Col("total", "BIGINT")))
    w.sql("CREATE OR REPLACE TEMP VIEW src_ct009 AS SELECT * FROM VALUES ('electronics', 100), ('electronics', 200), ('books', 50), ('books', 30) AS t(category, amount)")
    w.sql("INSERT INTO tbl SELECT category, SUM(amount) as total FROM src_ct009 GROUP BY category")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, predicate = "category = 'electronics'", name = "read_electronics")
    w.snapshot(t)
  }

  // ==========================================================================
  // CT-010: CREATE OR REPLACE TABLE (overwrite existing)
  // ==========================================================================
  test("ct010_create_or_replace",
      "Create table then overwrite with new data via INSERT OVERWRITE",
      "write", "insert", "overwrite") { w =>
    w.sql("CREATE TABLE tbl USING delta AS SELECT * FROM VALUES (1, 'old_a'), (2, 'old_b') AS t(key, value)")
    w.sql("INSERT OVERWRITE tbl SELECT * FROM VALUES (10, 'new_x'), (20, 'new_y'), (30, 'new_z') AS t(key, value)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_after_overwrite")
    w.read(t, version = 0, name = "read_original")
    w.snapshot(t)
  }

  // ==========================================================================
  // CT-011: CTAS with multiple data types
  // ==========================================================================
  test("ct011_ctas_mixed_types",
      "CTAS with INT, STRING, DOUBLE, BOOLEAN, DATE columns",
      "write", "ctas", "types") { w =>
    w.sql("""CREATE TABLE tbl USING delta AS
      SELECT 1 as id, 'hello' as name, 3.14 as score, true as active, DATE '2024-01-15' as created_date
      UNION ALL
      SELECT 2, 'world', 2.72, false, DATE '2024-06-30'""")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, predicate = "active = true", name = "read_active")
    w.snapshot(t)
  }

  // ==========================================================================
  // CT-012: CTAS with nested struct
  // ==========================================================================
  test("ct012_ctas_nested_struct",
      "CTAS producing table with nested struct column",
      "write", "ctas", "nested") { w =>
    w.sql("""CREATE TABLE tbl USING delta AS
      SELECT 1 as id, named_struct('name', 'Alice', 'age', 30) as info
      UNION ALL
      SELECT 2, named_struct('name', 'Bob', 'age', 25)""")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  // ==========================================================================
  // CT-013: INSERT INTO SELECT with UNION ALL
  // ==========================================================================
  test("ct013_insert_select_union_all",
      "INSERT INTO SELECT with UNION ALL from two source views",
      "write", "insert") { w =>
    w.sql("CREATE TABLE tbl USING delta AS SELECT * FROM VALUES (1, 'initial') AS t(key, value)")
    w.sql("CREATE OR REPLACE TEMP VIEW src1_ct013 AS SELECT * FROM VALUES (2, 'from_source1'), (3, 'from_source1') AS t(key, value)")
    w.sql("CREATE OR REPLACE TEMP VIEW src2_ct013 AS SELECT * FROM VALUES (4, 'from_source2'), (5, 'from_source2') AS t(key, value)")
    w.sql("INSERT INTO tbl SELECT * FROM src1_ct013 UNION ALL SELECT * FROM src2_ct013")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, version = 0, name = "read_initial")
    w.snapshot(t)
  }

  // ==========================================================================
  // CT-014: CTAS from empty source
  // ==========================================================================
  test("ct014_ctas_empty",
      "CTAS from empty source creates schema with no data rows",
      "write", "ctas") { w =>
    w.sql("CREATE OR REPLACE TEMP VIEW src_ct014 AS SELECT CAST(NULL AS INT) AS key, CAST(NULL AS STRING) AS value WHERE false")
    w.sql("CREATE TABLE tbl USING delta AS SELECT * FROM src_ct014")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_empty")
    w.snapshot(t)
  }

  // ==========================================================================
  // CT-015: INSERT INTO SELECT with column reordering
  // ==========================================================================
  test("ct015_insert_select_column_reorder",
      "INSERT INTO with explicit column reordering (value, key) from source",
      "write", "insert") { w =>
    w.sql("CREATE TABLE tbl USING delta AS SELECT * FROM VALUES (1, 'a') AS t(key, value)")
    w.sql("CREATE OR REPLACE TEMP VIEW src_ct015 AS SELECT * FROM VALUES ('b', 2), ('c', 3) AS t(value, key)")
    w.sql("INSERT INTO tbl (value, key) SELECT value, key FROM src_ct015")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, version = 0, name = "read_initial")
    w.snapshot(t)
  }

}.runAll()
