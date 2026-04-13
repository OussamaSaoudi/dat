/**
 * Generated column, identity column, row tracking, check constraint,
 * and schema edge case write workloads.
 *
 * GeneratedColumn (GC-001..GC-015): Tables with auto-computed generated columns.
 * IdentityColumn (IC-001..IC-015): Tables with auto-incrementing identity columns.
 * RowTracking (RT-001..RT-015): Row tracking with stable row IDs and commit versions.
 * CheckConstraints (CC-001..CC-015): Tables with CHECK constraints.
 * SchemaEdgeCase (SCH-001..SCH-020): Schema serialization edge cases.
 */

// =============================================================================
// Generated Columns
// =============================================================================

new WorkloadSuite("write_generated_columns") {

  test("gc_001_concat", "INSERT with generated column (concat)",
      "write", "generatedColumn", "concat", "insert") {
    sql("""CREATE TABLE tbl (
        id INT, first_name STRING, last_name STRING,
        full_name STRING GENERATED ALWAYS AS (concat(first_name, ' ', last_name))
      ) USING delta""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(Map("id" -> 1, "first_name" -> "John", "last_name" -> "Doe")))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("gc_002_multi_row", "INSERT multiple rows with generated column",
      "write", "generatedColumn", "insert", "multiRow") {
    sql("""CREATE TABLE tbl (
        id INT, first_name STRING, last_name STRING,
        full_name STRING GENERATED ALWAYS AS (concat(first_name, ' ', last_name))
      ) USING delta""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "first_name" -> "John", "last_name" -> "Doe"),
      Map("id" -> 2, "first_name" -> "Jane", "last_name" -> "Smith"),
      Map("id" -> 3, "first_name" -> "Bob", "last_name" -> "Jones")))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("gc_003_arithmetic", "Generated column with arithmetic expression",
      "write", "generatedColumn", "arithmetic") {
    sql("""CREATE TABLE tbl (
        id INT, price DOUBLE, quantity INT,
        total DOUBLE GENERATED ALWAYS AS (price * quantity)
      ) USING delta""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "price" -> 10.50, "quantity" -> 3),
      Map("id" -> 2, "price" -> 25.00, "quantity" -> 2)))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("gc_004_upper", "Generated column with UPPER function",
      "write", "generatedColumn", "stringFunction") {
    sql("""CREATE TABLE tbl (
        id INT, name STRING,
        upper_name STRING GENERATED ALWAYS AS (UPPER(name))
      ) USING delta""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "name" -> "alice"),
      Map("id" -> 2, "name" -> "bob")))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("gc_005_year_extraction", "Generated column with YEAR extraction",
      "write", "generatedColumn", "dateExtraction") {
    sql("""CREATE TABLE tbl (
        id INT, event_date DATE,
        year_col INT GENERATED ALWAYS AS (YEAR(event_date))
      ) USING delta""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "event_date" -> "2023-06-15"),
      Map("id" -> 2, "event_date" -> "2024-01-20")))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("gc_006_dataframe_api", "INSERT via DataFrame API with generated column",
      "write", "generatedColumn", "dataframeApi") {
    sql("""CREATE TABLE tbl (
        id INT, first_name STRING, last_name STRING,
        full_name STRING GENERATED ALWAYS AS (concat(first_name, ' ', last_name))
      ) USING delta""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "first_name" -> "John", "last_name" -> "Doe"),
      Map("id" -> 2, "first_name" -> "Jane", "last_name" -> "Smith")))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("gc_007_update_recompute", "UPDATE triggers generated column recompute",
      "write", "generatedColumn", "update") {
    sql("""CREATE TABLE tbl (
        id INT, first_name STRING, last_name STRING,
        full_name STRING GENERATED ALWAYS AS (concat(first_name, ' ', last_name))
      ) USING delta""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "first_name" -> "John", "last_name" -> "Doe"),
      Map("id" -> 2, "first_name" -> "Jane", "last_name" -> "Smith")))
    updateOp(w, "id = 1", Map("first_name" -> "'Johnny'"))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("gc_008_merge", "MERGE with generated column",
      "write", "generatedColumn", "merge") {
    sql("""CREATE TABLE tbl (
        id INT, first_name STRING, last_name STRING,
        full_name STRING GENERATED ALWAYS AS (concat(first_name, ' ', last_name))
      ) USING delta""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "first_name" -> "John", "last_name" -> "Doe"),
      Map("id" -> 2, "first_name" -> "Jane", "last_name" -> "Smith")))
    sql("CREATE TABLE src (id INT, first_name STRING, last_name STRING) USING delta")
    sql("INSERT INTO src VALUES (1, 'Jonathan', 'Doe'), (3, 'Bob', 'Jones')")
    sql("""MERGE INTO tbl AS target USING src AS source ON target.id = source.id
      WHEN MATCHED THEN UPDATE SET target.first_name = source.first_name, target.last_name = source.last_name
      WHEN NOT MATCHED THEN INSERT (id, first_name, last_name) VALUES (source.id, source.first_name, source.last_name)""")
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("gc_009_multiple_generated", "Multiple generated columns",
      "write", "generatedColumn", "multiple") {
    sql("""CREATE TABLE tbl (
        id INT, first_name STRING, last_name STRING,
        full_name STRING GENERATED ALWAYS AS (concat(first_name, ' ', last_name)),
        email STRING GENERATED ALWAYS AS (concat(lower(first_name), '@test.com'))
      ) USING delta""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "first_name" -> "John", "last_name" -> "Doe"),
      Map("id" -> 2, "first_name" -> "Jane", "last_name" -> "Smith")))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("gc_010_partition_key", "Generated column as partition key",
      "write", "generatedColumn", "partition") {
    sql("""CREATE TABLE tbl (
        id INT, date_col DATE,
        year_part INT GENERATED ALWAYS AS (YEAR(date_col))
      ) USING delta PARTITIONED BY (year_part)""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "date_col" -> "2023-03-15"),
      Map("id" -> 2, "date_col" -> "2024-07-20"),
      Map("id" -> 3, "date_col" -> "2023-11-01")))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("gc_011_insert_overwrite", "INSERT OVERWRITE with generated column",
      "write", "generatedColumn", "overwrite") {
    sql("""CREATE TABLE tbl (
        id INT, first_name STRING, last_name STRING,
        full_name STRING GENERATED ALWAYS AS (concat(first_name, ' ', last_name))
      ) USING delta""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "first_name" -> "John", "last_name" -> "Doe"),
      Map("id" -> 2, "first_name" -> "Jane", "last_name" -> "Smith")))
    sql("INSERT OVERWRITE tbl (id, first_name, last_name) VALUES (10, 'Alice', 'Wonder'), (20, 'Bob', 'Builder')")
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("gc_012_coalesce", "Generated column with COALESCE",
      "write", "generatedColumn", "coalesce") {
    sql("""CREATE TABLE tbl (
        id INT, first_name STRING, nickname STRING,
        display_name STRING GENERATED ALWAYS AS (COALESCE(nickname, first_name))
      ) USING delta""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "first_name" -> "Robert", "nickname" -> "Bob"),
      Map("id" -> 2, "first_name" -> "Elizabeth", "nickname" -> null)))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("gc_013_case_expression", "Generated column with CASE expression",
      "write", "generatedColumn", "caseExpression") {
    sql("""CREATE TABLE tbl (
        id INT, status INT,
        status_label STRING GENERATED ALWAYS AS (
          CASE WHEN status = 1 THEN 'active'
               WHEN status = 2 THEN 'inactive'
               ELSE 'unknown' END)
      ) USING delta""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "status" -> 1),
      Map("id" -> 2, "status" -> 2),
      Map("id" -> 3, "status" -> 99)))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("gc_014_delete", "DELETE from table with generated columns",
      "write", "generatedColumn", "delete") {
    sql("""CREATE TABLE tbl (
        id INT, first_name STRING, last_name STRING,
        full_name STRING GENERATED ALWAYS AS (concat(first_name, ' ', last_name))
      ) USING delta""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "first_name" -> "John", "last_name" -> "Doe"),
      Map("id" -> 2, "first_name" -> "Jane", "last_name" -> "Smith"),
      Map("id" -> 3, "first_name" -> "Bob", "last_name" -> "Jones")))
    deleteOp(w, "id = 2")
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("gc_015_explicit_value", "INSERT with explicit generated column value",
      "write", "generatedColumn", "explicitValue") {
    sql("""CREATE TABLE tbl (
        id INT, first_name STRING, last_name STRING,
        full_name STRING GENERATED ALWAYS AS (concat(first_name, ' ', last_name))
      ) USING delta""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(Map("id" -> 1, "first_name" -> "John", "last_name" -> "Doe", "full_name" -> "John Doe")))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

}

// =============================================================================
// Identity Columns
// =============================================================================

new WorkloadSuite("write_identity_columns") {

  test("ic_001_always_identity", "INSERT with GENERATED ALWAYS identity column",
      "write", "identityColumn", "always", "insert") {
    sql("CREATE TABLE tbl (id BIGINT GENERATED ALWAYS AS IDENTITY, name STRING) USING delta")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("name" -> "Alice"),
      Map("name" -> "Bob"),
      Map("name" -> "Charlie")))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("ic_002_by_default_identity", "INSERT with GENERATED BY DEFAULT identity column",
      "write", "identityColumn", "byDefault", "insert") {
    sql("CREATE TABLE tbl (id BIGINT GENERATED BY DEFAULT AS IDENTITY, name STRING) USING delta")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("name" -> "Alice"),
      Map("name" -> "Bob")))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("ic_003_custom_start_step", "INSERT with custom START WITH and INCREMENT BY",
      "write", "identityColumn", "customStart") {
    sql("CREATE TABLE tbl (id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 100 INCREMENT BY 10), name STRING) USING delta")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("name" -> "Alice"),
      Map("name" -> "Bob"),
      Map("name" -> "Charlie")))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("ic_004_sequential_batches", "Multiple INSERT batches preserve identity sequence",
      "write", "identityColumn", "sequential") {
    sql("CREATE TABLE tbl (id BIGINT GENERATED ALWAYS AS IDENTITY, name STRING) USING delta")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("name" -> "Alice"),
      Map("name" -> "Bob")))
    insertOp(w, Seq(
      Map("name" -> "Charlie"),
      Map("name" -> "Diana")))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("ic_005_explicit_id", "INSERT with BY DEFAULT allows explicit id",
      "write", "identityColumn", "byDefault", "explicitValue") {
    sql("CREATE TABLE tbl (id BIGINT GENERATED BY DEFAULT AS IDENTITY, name STRING) USING delta")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(Map("id" -> 999, "name" -> "explicit")))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("ic_006_merge", "MERGE with identity column",
      "write", "identityColumn", "merge") {
    sql("CREATE TABLE tbl (id BIGINT GENERATED ALWAYS AS IDENTITY, name STRING) USING delta")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("name" -> "Alice"),
      Map("name" -> "Bob")))
    sql("CREATE TABLE src (name STRING) USING delta")
    sql("INSERT INTO src VALUES ('Charlie'), ('Diana')")
    sql("""MERGE INTO tbl AS target USING src AS source ON target.name = source.name
      WHEN NOT MATCHED THEN INSERT (name) VALUES (source.name)""")
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("ic_007_delete", "DELETE from table with identity column",
      "write", "identityColumn", "delete") {
    sql("CREATE TABLE tbl (id BIGINT GENERATED ALWAYS AS IDENTITY, name STRING) USING delta")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("name" -> "Alice"),
      Map("name" -> "Bob"),
      Map("name" -> "Charlie")))
    deleteOp(w, "name = 'Bob'")
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("ic_008_update", "UPDATE non-identity column",
      "write", "identityColumn", "update") {
    sql("CREATE TABLE tbl (id BIGINT GENERATED ALWAYS AS IDENTITY, name STRING) USING delta")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("name" -> "Alice"),
      Map("name" -> "Bob"),
      Map("name" -> "Charlie")))
    updateOp(w, "name = 'Bob'", Map("name" -> "'Robert'"))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("ic_009_partitioned", "Identity column with partitioned table",
      "write", "identityColumn", "partitioned") {
    sql("CREATE TABLE tbl (id BIGINT GENERATED ALWAYS AS IDENTITY, name STRING, category STRING) USING delta PARTITIONED BY (category)")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("name" -> "Alice", "category" -> "A"),
      Map("name" -> "Bob", "category" -> "B"),
      Map("name" -> "Charlie", "category" -> "A")))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("ic_010_negative_increment", "Identity column with negative increment",
      "write", "identityColumn", "negativeIncrement") {
    sql("CREATE TABLE tbl (id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 0 INCREMENT BY -1), name STRING) USING delta")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("name" -> "Alice"),
      Map("name" -> "Bob"),
      Map("name" -> "Charlie")))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("ic_011_insert_overwrite", "INSERT OVERWRITE with identity column",
      "write", "identityColumn", "overwrite") {
    sql("CREATE TABLE tbl (id BIGINT GENERATED ALWAYS AS IDENTITY, name STRING) USING delta")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("name" -> "Alice"),
      Map("name" -> "Bob")))
    sql("INSERT OVERWRITE tbl (name) VALUES ('Xavier'), ('Yara'), ('Zoe')")
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("ic_012_large_batch", "Large batch INSERT with 100 rows and identity column",
      "write", "identityColumn", "largeBatch") {
    sql("CREATE TABLE tbl (id BIGINT GENERATED ALWAYS AS IDENTITY, name STRING) USING delta")
    sql("INSERT INTO tbl (name) SELECT concat('user_', id) FROM range(100)")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("ic_013_multi_column", "Identity column with additional columns",
      "write", "identityColumn", "multiColumn") {
    sql("CREATE TABLE tbl (id BIGINT GENERATED ALWAYS AS IDENTITY, name STRING, age INT, email STRING) USING delta")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("name" -> "Alice", "age" -> 30, "email" -> "alice@test.com"),
      Map("name" -> "Bob", "age" -> 25, "email" -> "bob@test.com"),
      Map("name" -> "Charlie", "age" -> 35, "email" -> "charlie@test.com")))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("ic_014_identity_plus_generated", "Identity column with generated column",
      "write", "identityColumn", "combined") {
    sql("""CREATE TABLE tbl (
        id BIGINT GENERATED ALWAYS AS IDENTITY, name STRING,
        name_upper STRING GENERATED ALWAYS AS (UPPER(name))
      ) USING delta""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("name" -> "Alice"),
      Map("name" -> "Bob"),
      Map("name" -> "Charlie")))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("ic_015_merge_upsert", "MERGE upsert with identity column",
      "write", "identityColumn", "merge", "upsert") {
    sql("CREATE TABLE tbl (id BIGINT GENERATED ALWAYS AS IDENTITY, name STRING, value INT) USING delta")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("name" -> "Alice", "value" -> 10),
      Map("name" -> "Bob", "value" -> 20),
      Map("name" -> "Charlie", "value" -> 30)))
    sql("CREATE TABLE src (name STRING, value INT) USING delta")
    sql("INSERT INTO src VALUES ('Bob', 25), ('Diana', 40)")
    sql("""MERGE INTO tbl AS target USING src AS source ON target.name = source.name
      WHEN MATCHED THEN UPDATE SET target.value = source.value
      WHEN NOT MATCHED THEN INSERT (name, value) VALUES (source.name, source.value)""")
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

}

// =============================================================================
// Row Tracking
// =============================================================================

new WorkloadSuite("write_row_tracking") {

  test("rt_001_insert", "INSERT into row-tracking-enabled table",
      "write", "rowTracking", "insert") {
    sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableRowTracking' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    sql("INSERT INTO tbl SELECT id FROM range(10, 20)")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("rt_002_update", "UPDATE on row-tracking table",
      "write", "rowTracking", "update") {
    sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableRowTracking' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    updateOp(w, "id < 5", Map("id" -> "id + 100"))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("rt_003_delete", "DELETE on row-tracking table",
      "write", "rowTracking", "delete") {
    sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableRowTracking' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    deleteOp(w, "id >= 7")
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("rt_004_merge", "MERGE on row-tracking table",
      "write", "rowTracking", "merge") {
    sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableRowTracking' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    sql("CREATE TABLE src (id BIGINT) USING delta")
    sql("INSERT INTO src SELECT id FROM range(5, 15)")
    sql("""MERGE INTO tbl t USING src s ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET t.id = s.id
      WHEN NOT MATCHED THEN INSERT (id) VALUES (s.id)""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("rt_005_with_dvs", "Row tracking with deletion vectors",
      "write", "rowTracking", "deletionVectors") {
    sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES (
        'delta.enableRowTracking' = 'true',
        'delta.enableDeletionVectors' = 'true'
      )""")
    sql("INSERT INTO tbl SELECT id FROM range(20)")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    deleteOp(w, "id IN (3, 7, 11, 15)")
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("rt_006_partitioned", "Row tracking on partitioned table",
      "write", "rowTracking", "partitioned") {
    sql("""CREATE TABLE tbl (id INT, part STRING) USING delta
      PARTITIONED BY (part)
      TBLPROPERTIES ('delta.enableRowTracking' = 'true')""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "part" -> "a"),
      Map("id" -> 2, "part" -> "a"),
      Map("id" -> 3, "part" -> "b"),
      Map("id" -> 4, "part" -> "b"),
      Map("id" -> 5, "part" -> "c")))
    insertOp(w, Seq(
      Map("id" -> 6, "part" -> "a"),
      Map("id" -> 7, "part" -> "c")))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("rt_007_insert_overwrite", "Row tracking with INSERT OVERWRITE",
      "write", "rowTracking", "insertOverwrite") {
    sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableRowTracking' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    sql("INSERT OVERWRITE tbl SELECT id FROM range(20, 25)")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("rt_008_optimize", "Row tracking with OPTIMIZE",
      "write", "rowTracking", "optimize") {
    sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableRowTracking' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(0, 10)")
    sql("INSERT INTO tbl SELECT id FROM range(10, 20)")
    sql("INSERT INTO tbl SELECT id FROM range(20, 30)")
    sql("OPTIMIZE tbl")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("rt_009_multi_insert", "Multiple INSERTs with row tracking",
      "write", "rowTracking", "multipleInserts") {
    sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableRowTracking' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(0, 5)")
    sql("INSERT INTO tbl SELECT id FROM range(5, 10)")
    sql("INSERT INTO tbl SELECT id FROM range(10, 15)")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("rt_010_schema_evolution", "Row tracking with schema evolution",
      "write", "rowTracking", "schemaEvolution") {
    sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableRowTracking' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(5)")
    sql("ALTER TABLE tbl ADD COLUMN (name STRING)")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 5, "name" -> "five"),
      Map("id" -> 6, "name" -> "six"),
      Map("id" -> 7, "name" -> "seven")))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("rt_011_merge_nmbs", "Row tracking with MERGE NOT MATCHED BY SOURCE",
      "write", "rowTracking", "merge", "nmbs") {
    sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableRowTracking' = 'true')""")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    sql("CREATE TABLE src (id BIGINT) USING delta")
    sql("INSERT INTO src SELECT id FROM range(0, 5)")
    sql("""MERGE INTO tbl t USING src s ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET t.id = s.id
      WHEN NOT MATCHED BY SOURCE THEN DELETE""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("rt_012_conditional_update", "Row tracking with conditional UPDATE",
      "write", "rowTracking", "conditionalUpdate") {
    sql("""CREATE TABLE tbl (id INT, value INT) USING delta
      TBLPROPERTIES ('delta.enableRowTracking' = 'true')""")
    sql("INSERT INTO tbl SELECT id, id * 10 FROM range(10)")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    updateOp(w, "id % 2 = 0", Map("value" -> "value + 1000"))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("rt_013_with_cdc", "Row tracking with CDC",
      "write", "rowTracking", "cdc") {
    sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES (
        'delta.enableRowTracking' = 'true',
        'delta.enableChangeDataFeed' = 'true'
      )""")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    updateOp(w, "id < 3", Map("id" -> "id + 100"))
    deleteOp(w, "id >= 8")
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("rt_014_enable_on_existing", "Enable row tracking on existing table",
      "write", "rowTracking", "enable", "alter") {
    sql("CREATE TABLE tbl (id BIGINT) USING delta")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.enableRowTracking' = 'true')")
    sql("INSERT INTO tbl SELECT id FROM range(10, 20)")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("rt_015_type_widening", "Row tracking with type widening",
      "write", "rowTracking", "typeWidening") {
    sql("""CREATE TABLE tbl (id INT, value TINYINT) USING delta
      TBLPROPERTIES (
        'delta.enableRowTracking' = 'true',
        'delta.enableTypeWidening' = 'true'
      )""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "value" -> 10),
      Map("id" -> 2, "value" -> 20),
      Map("id" -> 3, "value" -> 30)))
    sql("ALTER TABLE tbl ALTER COLUMN value TYPE INT")
    insertOp(w, Seq(
      Map("id" -> 4, "value" -> 1000),
      Map("id" -> 5, "value" -> 2000)))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

}

// =============================================================================
// Check Constraints
// =============================================================================

new WorkloadSuite("write_check_constraints") {

  test("cc_001_simple_positive", "INSERT satisfying simple CHECK constraint (value > 0)",
      "write", "checkConstraint", "insert") {
    sql("CREATE TABLE tbl (id INT, value INT) USING delta")
    sql("ALTER TABLE tbl ADD CONSTRAINT positive_value CHECK (value > 0)")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "value" -> 10),
      Map("id" -> 2, "value" -> 20),
      Map("id" -> 3, "value" -> 30)))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("cc_002_not_null", "INSERT satisfying NOT NULL constraint",
      "write", "checkConstraint", "notNull") {
    sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
    sql("ALTER TABLE tbl ADD CONSTRAINT name_not_null CHECK (name IS NOT NULL)")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "name" -> "alice"),
      Map("id" -> 2, "name" -> "bob"),
      Map("id" -> 3, "name" -> "carol")))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("cc_003_range", "INSERT satisfying range constraint",
      "write", "checkConstraint", "range") {
    sql("CREATE TABLE tbl (id INT, name STRING, age INT) USING delta")
    sql("ALTER TABLE tbl ADD CONSTRAINT valid_age CHECK (age >= 0 AND age <= 150)")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "name" -> "alice", "age" -> 30),
      Map("id" -> 2, "name" -> "bob", "age" -> 0),
      Map("id" -> 3, "name" -> "carol", "age" -> 150)))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("cc_004_enum", "INSERT satisfying string enum constraint",
      "write", "checkConstraint", "enum") {
    sql("CREATE TABLE tbl (id INT, status STRING) USING delta")
    sql("ALTER TABLE tbl ADD CONSTRAINT valid_status CHECK (status IN ('active', 'inactive', 'pending'))")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "status" -> "active"),
      Map("id" -> 2, "status" -> "inactive"),
      Map("id" -> 3, "status" -> "pending")))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("cc_005_update", "UPDATE satisfying constraint",
      "write", "checkConstraint", "update") {
    sql("CREATE TABLE tbl (id INT, value INT) USING delta")
    sql("ALTER TABLE tbl ADD CONSTRAINT positive_value CHECK (value > 0)")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "value" -> 10),
      Map("id" -> 2, "value" -> 20),
      Map("id" -> 3, "value" -> 30)))
    updateOp(w, "id = 2", Map("value" -> "99"))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("cc_006_merge", "MERGE satisfying constraint",
      "write", "checkConstraint", "merge") {
    sql("CREATE TABLE tbl (id INT, value INT) USING delta")
    sql("ALTER TABLE tbl ADD CONSTRAINT positive_value CHECK (value > 0)")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "value" -> 10),
      Map("id" -> 2, "value" -> 20),
      Map("id" -> 3, "value" -> 30)))
    sql("CREATE TABLE src (id INT, value INT) USING delta")
    sql("INSERT INTO src VALUES (2, 25), (4, 40), (5, 50)")
    sql("""MERGE INTO tbl AS target USING src AS source ON target.id = source.id
      WHEN MATCHED THEN UPDATE SET target.value = source.value
      WHEN NOT MATCHED THEN INSERT (id, value) VALUES (source.id, source.value)""")
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("cc_007_multiple", "Multiple CHECK constraints on same table",
      "write", "checkConstraint", "multiple") {
    sql("CREATE TABLE tbl (id INT, value INT, name STRING) USING delta")
    sql("ALTER TABLE tbl ADD CONSTRAINT positive_value CHECK (value > 0)")
    sql("ALTER TABLE tbl ADD CONSTRAINT name_not_null CHECK (name IS NOT NULL)")
    sql("ALTER TABLE tbl ADD CONSTRAINT name_nonempty CHECK (length(name) > 0)")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "value" -> 10, "name" -> "alice"),
      Map("id" -> 2, "value" -> 20, "name" -> "bob"),
      Map("id" -> 3, "value" -> 30, "name" -> "carol")))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("cc_008_compound", "CHECK constraint with compound expression",
      "write", "checkConstraint", "compound") {
    sql("CREATE TABLE tbl (id INT, price DOUBLE, quantity INT) USING delta")
    sql("ALTER TABLE tbl ADD CONSTRAINT valid_order CHECK ((price > 0 AND quantity >= 0) OR (price = 0 AND quantity = 0))")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "price" -> 9.99, "quantity" -> 5),
      Map("id" -> 2, "price" -> 19.99, "quantity" -> 1),
      Map("id" -> 3, "price" -> 0.0, "quantity" -> 0)))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("cc_009_add_drop", "ADD then DROP constraint - insert after drop",
      "write", "checkConstraint", "drop") {
    sql("CREATE TABLE tbl (id INT, value INT) USING delta")
    sql("ALTER TABLE tbl ADD CONSTRAINT positive_value CHECK (value > 0)")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "value" -> 10),
      Map("id" -> 2, "value" -> 20)))
    sql("ALTER TABLE tbl DROP CONSTRAINT positive_value")
    insertOp(w, Seq(
      Map("id" -> 3, "value" -> -5),
      Map("id" -> 4, "value" -> 0)))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("cc_010_partitioned", "CHECK constraint on partitioned table",
      "write", "checkConstraint", "partitioned") {
    sql("CREATE TABLE tbl (id INT, value INT, category STRING) USING delta PARTITIONED BY (category)")
    sql("ALTER TABLE tbl ADD CONSTRAINT positive_value CHECK (value > 0)")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "value" -> 10, "category" -> "A"),
      Map("id" -> 2, "value" -> 20, "category" -> "B"),
      Map("id" -> 3, "value" -> 30, "category" -> "A"),
      Map("id" -> 4, "value" -> 40, "category" -> "C")))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("cc_011_string_functions", "CHECK constraint with string functions",
      "write", "checkConstraint", "stringFunction") {
    sql("CREATE TABLE tbl (id INT, email STRING) USING delta")
    sql("ALTER TABLE tbl ADD CONSTRAINT valid_email CHECK (length(email) > 5 AND email LIKE '%@%')")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "email" -> "alice@example.com"),
      Map("id" -> 2, "email" -> "bob@test.org"),
      Map("id" -> 3, "email" -> "carol@foo.io")))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("cc_012_delete", "DELETE from constrained table",
      "write", "checkConstraint", "delete") {
    sql("CREATE TABLE tbl (id INT, value INT) USING delta")
    sql("ALTER TABLE tbl ADD CONSTRAINT positive_value CHECK (value > 0)")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "value" -> 10),
      Map("id" -> 2, "value" -> 20),
      Map("id" -> 3, "value" -> 30),
      Map("id" -> 4, "value" -> 40)))
    deleteOp(w, "id = 2")
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("cc_013_insert_overwrite", "INSERT OVERWRITE satisfying constraint",
      "write", "checkConstraint", "overwrite") {
    sql("CREATE TABLE tbl (id INT, value INT) USING delta")
    sql("ALTER TABLE tbl ADD CONSTRAINT positive_value CHECK (value > 0)")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "value" -> 10),
      Map("id" -> 2, "value" -> 20)))
    sql("INSERT OVERWRITE tbl VALUES (10, 100), (20, 200), (30, 300)")
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("cc_014_date_comparison", "CHECK constraint with date comparison",
      "write", "checkConstraint", "dateComparison") {
    sql("CREATE TABLE tbl (id INT, start_date DATE, end_date DATE) USING delta")
    sql("ALTER TABLE tbl ADD CONSTRAINT valid_dates CHECK (end_date >= start_date)")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "start_date" -> "2024-01-01", "end_date" -> "2024-06-30"),
      Map("id" -> 2, "start_date" -> "2024-03-15", "end_date" -> "2024-03-15"),
      Map("id" -> 3, "start_date" -> "2023-12-01", "end_date" -> "2025-01-01")))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("cc_015_between", "CHECK constraint with BETWEEN",
      "write", "checkConstraint", "between") {
    sql("CREATE TABLE tbl (id INT, name STRING, score INT) USING delta")
    sql("ALTER TABLE tbl ADD CONSTRAINT valid_score CHECK (score BETWEEN 0 AND 100)")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "name" -> "alice", "score" -> 95),
      Map("id" -> 2, "name" -> "bob", "score" -> 0),
      Map("id" -> 3, "name" -> "carol", "score" -> 100),
      Map("id" -> 4, "name" -> "dave", "score" -> 50)))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

}

// =============================================================================
// Schema Edge Cases
// =============================================================================

new WorkloadSuite("write_schema_edge_cases") {

  test("sch_001_all_primitives", "All primitive types in one table",
      "write", "schema", "primitives") {
    sql("""CREATE TABLE tbl (
        id INT, col_boolean BOOLEAN, col_byte TINYINT, col_short SMALLINT,
        col_int INT, col_long BIGINT, col_float FLOAT, col_double DOUBLE,
        col_string STRING, col_binary BINARY, col_date DATE,
        col_timestamp TIMESTAMP, col_decimal DECIMAL(10,2)
      ) USING delta""")
    sql("""INSERT INTO tbl VALUES
      (1, true, CAST(1 AS TINYINT), CAST(100 AS SMALLINT), 1000, 100000L,
       1.5, 2.5, 'hello', CAST('abc' AS BINARY), DATE'2024-01-01',
       TIMESTAMP'2024-01-01 12:00:00', 12345.67),
      (2, false, CAST(-1 AS TINYINT), CAST(-100 AS SMALLINT), -1000, -100000L,
       -1.5, -2.5, 'world', CAST('xyz' AS BINARY), DATE'1970-01-01',
       TIMESTAMP'1970-01-01 00:00:00', -12345.67),
      (3, null, null, null, null, null, null, null, null, null, null, null, null)""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("sch_002_nested_struct_3_levels", "Nested struct 3 levels deep",
      "write", "schema", "nested-struct") {
    sql("CREATE TABLE tbl (id INT, nested STRUCT<a: STRUCT<b: STRUCT<c: INT>>>) USING delta")
    sql("""INSERT INTO tbl
      SELECT 1, named_struct('a', named_struct('b', named_struct('c', 42)))
      UNION ALL SELECT 2, named_struct('a', named_struct('b', named_struct('c', -1)))
      UNION ALL SELECT 3, null""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("sch_003_array_of_struct", "Array of struct column",
      "write", "schema", "array-struct") {
    sql("CREATE TABLE tbl (id INT, items ARRAY<STRUCT<x: INT, y: STRING>>) USING delta")
    sql("""INSERT INTO tbl
      SELECT 1, array(named_struct('x', 10, 'y', 'alpha'), named_struct('x', 20, 'y', 'beta'))
      UNION ALL SELECT 2, array(named_struct('x', 30, 'y', 'gamma'))
      UNION ALL SELECT 3, null""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("sch_004_map_to_struct", "Map from string to struct",
      "write", "schema", "map-struct") {
    sql("CREATE TABLE tbl (id INT, kv MAP<STRING, STRUCT<v: INT>>) USING delta")
    sql("""INSERT INTO tbl
      SELECT 1, map('key1', named_struct('v', 100), 'key2', named_struct('v', 200))
      UNION ALL SELECT 2, map('only', named_struct('v', 999))
      UNION ALL SELECT 3, null""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("sch_005_array_null_elements", "Array with null elements (containsNull=true)",
      "write", "schema", "array-null") {
    sql("CREATE TABLE tbl (id INT, vals ARRAY<INT>) USING delta")
    sql("""INSERT INTO tbl
      SELECT 1, array(1, null, 3) UNION ALL SELECT 2, array(null, null)
      UNION ALL SELECT 3, array(42) UNION ALL SELECT 4, null""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("sch_006_map_null_values", "Map with null values (valueContainsNull=true)",
      "write", "schema", "map-null-value") {
    sql("CREATE TABLE tbl (id INT, kv MAP<STRING, INT>) USING delta")
    sql("""INSERT INTO tbl
      SELECT 1, map('a', 1, 'b', null) UNION ALL SELECT 2, map('c', null)
      UNION ALL SELECT 3, map('d', 42) UNION ALL SELECT 4, null""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("sch_007_decimal_max_precision", "Decimal(38,18) max precision",
      "write", "schema", "decimal-max-precision") {
    sql("CREATE TABLE tbl (id INT, val DECIMAL(38,18)) USING delta")
    sql("""INSERT INTO tbl VALUES
      (1, CAST('12345678901234567890.123456789012345678' AS DECIMAL(38,18))),
      (2, CAST('-12345678901234567890.123456789012345678' AS DECIMAL(38,18))),
      (3, CAST('0.000000000000000001' AS DECIMAL(38,18))),
      (4, null)""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("sch_008_decimal_zero_scale", "Decimal(10,0) zero scale",
      "write", "schema", "decimal-zero-scale") {
    sql("CREATE TABLE tbl (id INT, val DECIMAL(10,0)) USING delta")
    sql("""INSERT INTO tbl VALUES
      (1, CAST(1234567890 AS DECIMAL(10,0))),
      (2, CAST(-1234567890 AS DECIMAL(10,0))),
      (3, CAST(0 AS DECIMAL(10,0))),
      (4, null)""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("sch_009_not_null", "Column with NOT NULL constraint",
      "write", "schema", "not-null") {
    sql("CREATE TABLE tbl (id INT NOT NULL, name STRING NOT NULL, optional_val INT) USING delta")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "name" -> "alice", "optional_val" -> 10),
      Map("id" -> 2, "name" -> "bob", "optional_val" -> null),
      Map("id" -> 3, "name" -> "charlie", "optional_val" -> 30)))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("sch_010_deep_nested_map_array", "Deeply nested map<string, array<struct>>",
      "write", "schema", "deep-nested") {
    sql("CREATE TABLE tbl (id INT, data MAP<STRING, ARRAY<STRUCT<k: INT>>>) USING delta")
    sql("""INSERT INTO tbl
      SELECT 1, map('group1', array(named_struct('k', 1), named_struct('k', 2)),
                  'group2', array(named_struct('k', 3)))
      UNION ALL SELECT 2, map('single', array(named_struct('k', 99)))
      UNION ALL SELECT 3, null""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("sch_011_multi_array", "Multiple array columns in one table",
      "write", "schema", "multi-array") {
    sql("CREATE TABLE tbl (id INT, int_arr ARRAY<INT>, str_arr ARRAY<STRING>, dbl_arr ARRAY<DOUBLE>) USING delta")
    sql("""INSERT INTO tbl
      SELECT 1, array(1, 2, 3), array('a', 'b'), array(1.1, 2.2)
      UNION ALL SELECT 2, array(4), array('c', 'd', 'e'), array(3.3)
      UNION ALL SELECT 3, null, null, null""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("sch_012_multi_map", "Multiple map columns in one table",
      "write", "schema", "multi-map") {
    sql("CREATE TABLE tbl (id INT, str_int_map MAP<STRING, INT>, int_str_map MAP<INT, STRING>) USING delta")
    sql("""INSERT INTO tbl
      SELECT 1, map('x', 10, 'y', 20), map(1, 'one', 2, 'two')
      UNION ALL SELECT 2, map('z', 30), map(3, 'three')
      UNION ALL SELECT 3, null, null""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("sch_013_mixed_complex", "Struct + array + map in one table",
      "write", "schema", "mixed-complex") {
    sql("""CREATE TABLE tbl (
        id INT, info STRUCT<name: STRING, age: INT>,
        tags ARRAY<STRING>, attrs MAP<STRING, STRING>
      ) USING delta""")
    sql("""INSERT INTO tbl
      SELECT 1, named_struct('name', 'alice', 'age', 30),
             array('admin', 'user'), map('dept', 'eng', 'level', 'senior')
      UNION ALL
      SELECT 2, named_struct('name', 'bob', 'age', 25),
             array('user'), map('dept', 'sales')
      UNION ALL SELECT 3, null, null, null""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("sch_014_long_strings", "Strings with 10000+ characters",
      "write", "schema", "long-string") {
    sql("CREATE TABLE tbl (id INT, val STRING) USING delta")
    sql(s"INSERT INTO tbl VALUES (1, '${"a" * 10000}'), (2, '${"b" * 50000}'), (3, null)")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("sch_015_binary_data", "Binary column with various data patterns",
      "write", "schema", "binary") {
    sql("CREATE TABLE tbl (id INT, data BINARY) USING delta")
    sql("""INSERT INTO tbl VALUES
      (1, X''), (2, X'DEADBEEF'), (3, X'00FF00FF00FF'),
      (4, CAST('hello binary world' AS BINARY)), (5, null)""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("sch_016_date_edge", "Date edge cases - epoch, far future, far past",
      "write", "schema", "date-edge") {
    sql("CREATE TABLE tbl (id INT, val DATE) USING delta")
    sql("""INSERT INTO tbl VALUES
      (1, DATE'1970-01-01'), (2, DATE'9999-12-31'), (3, DATE'0001-01-01'),
      (4, DATE'2000-02-29'), (5, DATE'1969-12-31'), (6, null)""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("sch_017_timestamp_microsecond", "Timestamp microsecond precision",
      "write", "schema", "timestamp-edge") {
    sql("CREATE TABLE tbl (id INT, val TIMESTAMP) USING delta")
    sql("""INSERT INTO tbl VALUES
      (1, TIMESTAMP'1970-01-01 00:00:00'),
      (2, TIMESTAMP'2024-06-15 12:30:45.123456'),
      (3, TIMESTAMP'9999-12-31 23:59:59.999999'),
      (4, TIMESTAMP'2000-01-01 00:00:00.000001'),
      (5, null)""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("sch_018_decimal_boundary", "Max and min decimal boundary values",
      "write", "schema", "decimal-boundary") {
    sql("CREATE TABLE tbl (id INT, small_dec DECIMAL(5,2), large_dec DECIMAL(38,0)) USING delta")
    sql("""INSERT INTO tbl VALUES
      (1, CAST(999.99 AS DECIMAL(5,2)),
          CAST('99999999999999999999999999999999999999' AS DECIMAL(38,0))),
      (2, CAST(-999.99 AS DECIMAL(5,2)),
          CAST('-99999999999999999999999999999999999999' AS DECIMAL(38,0))),
      (3, CAST(0.00 AS DECIMAL(5,2)), CAST(0 AS DECIMAL(38,0))),
      (4, null, null)""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("sch_019_boolean_patterns", "Boolean column patterns - all combinations",
      "write", "schema", "boolean-patterns") {
    sql("CREATE TABLE tbl (id INT, flag1 BOOLEAN, flag2 BOOLEAN) USING delta")
    sql("""INSERT INTO tbl VALUES
      (1, true, true), (2, false, false), (3, true, false),
      (4, false, true), (5, null, true), (6, false, null), (7, null, null)""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("sch_020_empty_vs_null", "Empty string vs null semantics",
      "write", "schema", "empty-vs-null") {
    sql("CREATE TABLE tbl (id INT, str_val STRING, bin_val BINARY) USING delta")
    sql("""INSERT INTO tbl VALUES
      (1, '', CAST('' AS BINARY)),
      (2, null, null),
      (3, ' ', CAST(' ' AS BINARY)),
      (4, 'nonempty', CAST('nonempty' AS BINARY))""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

}
