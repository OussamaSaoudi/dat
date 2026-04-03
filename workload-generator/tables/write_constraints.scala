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
      "write", "generatedColumn", "concat", "insert") { w =>
    w.sql("""CREATE TABLE tbl (
        id INT, first_name STRING, last_name STRING,
        full_name STRING GENERATED ALWAYS AS (concat(first_name, ' ', last_name))
      ) USING delta""")
    w.sql("INSERT INTO tbl (id, first_name, last_name) VALUES (1, 'John', 'Doe')")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("gc_002_multi_row", "INSERT multiple rows with generated column",
      "write", "generatedColumn", "insert", "multiRow") { w =>
    w.sql("""CREATE TABLE tbl (
        id INT, first_name STRING, last_name STRING,
        full_name STRING GENERATED ALWAYS AS (concat(first_name, ' ', last_name))
      ) USING delta""")
    w.sql("""INSERT INTO tbl (id, first_name, last_name) VALUES
      (1, 'John', 'Doe'), (2, 'Jane', 'Smith'), (3, 'Bob', 'Jones')""")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("gc_003_arithmetic", "Generated column with arithmetic expression",
      "write", "generatedColumn", "arithmetic") { w =>
    w.sql("""CREATE TABLE tbl (
        id INT, price DOUBLE, quantity INT,
        total DOUBLE GENERATED ALWAYS AS (price * quantity)
      ) USING delta""")
    w.sql("INSERT INTO tbl (id, price, quantity) VALUES (1, 10.50, 3), (2, 25.00, 2)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("gc_004_upper", "Generated column with UPPER function",
      "write", "generatedColumn", "stringFunction") { w =>
    w.sql("""CREATE TABLE tbl (
        id INT, name STRING,
        upper_name STRING GENERATED ALWAYS AS (UPPER(name))
      ) USING delta""")
    w.sql("INSERT INTO tbl (id, name) VALUES (1, 'alice'), (2, 'bob')")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("gc_005_year_extraction", "Generated column with YEAR extraction",
      "write", "generatedColumn", "dateExtraction") { w =>
    w.sql("""CREATE TABLE tbl (
        id INT, event_date DATE,
        year_col INT GENERATED ALWAYS AS (YEAR(event_date))
      ) USING delta""")
    w.sql("INSERT INTO tbl (id, event_date) VALUES (1, DATE '2023-06-15'), (2, DATE '2024-01-20')")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("gc_006_dataframe_api", "INSERT via DataFrame API with generated column",
      "write", "generatedColumn", "dataframeApi") { w =>
    w.sql("""CREATE TABLE tbl (
        id INT, first_name STRING, last_name STRING,
        full_name STRING GENERATED ALWAYS AS (concat(first_name, ' ', last_name))
      ) USING delta""")
    w.sql("INSERT INTO tbl (id, first_name, last_name) VALUES (1, 'John', 'Doe'), (2, 'Jane', 'Smith')")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("gc_007_update_recompute", "UPDATE triggers generated column recompute",
      "write", "generatedColumn", "update") { w =>
    w.sql("""CREATE TABLE tbl (
        id INT, first_name STRING, last_name STRING,
        full_name STRING GENERATED ALWAYS AS (concat(first_name, ' ', last_name))
      ) USING delta""")
    w.sql("INSERT INTO tbl (id, first_name, last_name) VALUES (1, 'John', 'Doe'), (2, 'Jane', 'Smith')")
    w.sql("UPDATE tbl SET first_name = 'Johnny' WHERE id = 1")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("gc_008_merge", "MERGE with generated column",
      "write", "generatedColumn", "merge") { w =>
    w.sql("""CREATE TABLE tbl (
        id INT, first_name STRING, last_name STRING,
        full_name STRING GENERATED ALWAYS AS (concat(first_name, ' ', last_name))
      ) USING delta""")
    w.sql("INSERT INTO tbl (id, first_name, last_name) VALUES (1, 'John', 'Doe'), (2, 'Jane', 'Smith')")
    w.sql("CREATE TABLE src (id INT, first_name STRING, last_name STRING) USING delta")
    w.sql("INSERT INTO src VALUES (1, 'Jonathan', 'Doe'), (3, 'Bob', 'Jones')")
    w.sql("""MERGE INTO tbl AS target USING src AS source ON target.id = source.id
      WHEN MATCHED THEN UPDATE SET target.first_name = source.first_name, target.last_name = source.last_name
      WHEN NOT MATCHED THEN INSERT (id, first_name, last_name) VALUES (source.id, source.first_name, source.last_name)""")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("gc_009_multiple_generated", "Multiple generated columns",
      "write", "generatedColumn", "multiple") { w =>
    w.sql("""CREATE TABLE tbl (
        id INT, first_name STRING, last_name STRING,
        full_name STRING GENERATED ALWAYS AS (concat(first_name, ' ', last_name)),
        email STRING GENERATED ALWAYS AS (concat(lower(first_name), '@test.com'))
      ) USING delta""")
    w.sql("INSERT INTO tbl (id, first_name, last_name) VALUES (1, 'John', 'Doe'), (2, 'Jane', 'Smith')")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("gc_010_partition_key", "Generated column as partition key",
      "write", "generatedColumn", "partition") { w =>
    w.sql("""CREATE TABLE tbl (
        id INT, date_col DATE,
        year_part INT GENERATED ALWAYS AS (YEAR(date_col))
      ) USING delta PARTITIONED BY (year_part)""")
    w.sql("""INSERT INTO tbl (id, date_col) VALUES
      (1, DATE '2023-03-15'), (2, DATE '2024-07-20'), (3, DATE '2023-11-01')""")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("gc_011_insert_overwrite", "INSERT OVERWRITE with generated column",
      "write", "generatedColumn", "overwrite") { w =>
    w.sql("""CREATE TABLE tbl (
        id INT, first_name STRING, last_name STRING,
        full_name STRING GENERATED ALWAYS AS (concat(first_name, ' ', last_name))
      ) USING delta""")
    w.sql("INSERT INTO tbl (id, first_name, last_name) VALUES (1, 'John', 'Doe'), (2, 'Jane', 'Smith')")
    w.sql("INSERT OVERWRITE tbl (id, first_name, last_name) VALUES (10, 'Alice', 'Wonder'), (20, 'Bob', 'Builder')")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("gc_012_coalesce", "Generated column with COALESCE",
      "write", "generatedColumn", "coalesce") { w =>
    w.sql("""CREATE TABLE tbl (
        id INT, first_name STRING, nickname STRING,
        display_name STRING GENERATED ALWAYS AS (COALESCE(nickname, first_name))
      ) USING delta""")
    w.sql("INSERT INTO tbl (id, first_name, nickname) VALUES (1, 'Robert', 'Bob'), (2, 'Elizabeth', NULL)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("gc_013_case_expression", "Generated column with CASE expression",
      "write", "generatedColumn", "caseExpression") { w =>
    w.sql("""CREATE TABLE tbl (
        id INT, status INT,
        status_label STRING GENERATED ALWAYS AS (
          CASE WHEN status = 1 THEN 'active'
               WHEN status = 2 THEN 'inactive'
               ELSE 'unknown' END)
      ) USING delta""")
    w.sql("INSERT INTO tbl (id, status) VALUES (1, 1), (2, 2), (3, 99)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("gc_014_delete", "DELETE from table with generated columns",
      "write", "generatedColumn", "delete") { w =>
    w.sql("""CREATE TABLE tbl (
        id INT, first_name STRING, last_name STRING,
        full_name STRING GENERATED ALWAYS AS (concat(first_name, ' ', last_name))
      ) USING delta""")
    w.sql("INSERT INTO tbl (id, first_name, last_name) VALUES (1, 'John', 'Doe'), (2, 'Jane', 'Smith'), (3, 'Bob', 'Jones')")
    w.sql("DELETE FROM tbl WHERE id = 2")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("gc_015_explicit_value", "INSERT with explicit generated column value",
      "write", "generatedColumn", "explicitValue") { w =>
    w.sql("""CREATE TABLE tbl (
        id INT, first_name STRING, last_name STRING,
        full_name STRING GENERATED ALWAYS AS (concat(first_name, ' ', last_name))
      ) USING delta""")
    w.sql("INSERT INTO tbl (id, first_name, last_name, full_name) VALUES (1, 'John', 'Doe', 'John Doe')")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

}.runAll()

// =============================================================================
// Identity Columns
// =============================================================================

new WorkloadSuite("write_identity_columns") {

  test("ic_001_always_identity", "INSERT with GENERATED ALWAYS identity column",
      "write", "identityColumn", "always", "insert") { w =>
    w.sql("CREATE TABLE tbl (id BIGINT GENERATED ALWAYS AS IDENTITY, name STRING) USING delta")
    w.sql("INSERT INTO tbl (name) VALUES ('Alice'), ('Bob'), ('Charlie')")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("ic_002_by_default_identity", "INSERT with GENERATED BY DEFAULT identity column",
      "write", "identityColumn", "byDefault", "insert") { w =>
    w.sql("CREATE TABLE tbl (id BIGINT GENERATED BY DEFAULT AS IDENTITY, name STRING) USING delta")
    w.sql("INSERT INTO tbl (name) VALUES ('Alice'), ('Bob')")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("ic_003_custom_start_step", "INSERT with custom START WITH and INCREMENT BY",
      "write", "identityColumn", "customStart") { w =>
    w.sql("CREATE TABLE tbl (id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 100 INCREMENT BY 10), name STRING) USING delta")
    w.sql("INSERT INTO tbl (name) VALUES ('Alice'), ('Bob'), ('Charlie')")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("ic_004_sequential_batches", "Multiple INSERT batches preserve identity sequence",
      "write", "identityColumn", "sequential") { w =>
    w.sql("CREATE TABLE tbl (id BIGINT GENERATED ALWAYS AS IDENTITY, name STRING) USING delta")
    w.sql("INSERT INTO tbl (name) VALUES ('Alice'), ('Bob')")
    w.sql("INSERT INTO tbl (name) VALUES ('Charlie'), ('Diana')")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("ic_005_explicit_id", "INSERT with BY DEFAULT allows explicit id",
      "write", "identityColumn", "byDefault", "explicitValue") { w =>
    w.sql("CREATE TABLE tbl (id BIGINT GENERATED BY DEFAULT AS IDENTITY, name STRING) USING delta")
    w.sql("INSERT INTO tbl (id, name) VALUES (999, 'explicit')")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("ic_006_merge", "MERGE with identity column",
      "write", "identityColumn", "merge") { w =>
    w.sql("CREATE TABLE tbl (id BIGINT GENERATED ALWAYS AS IDENTITY, name STRING) USING delta")
    w.sql("INSERT INTO tbl (name) VALUES ('Alice'), ('Bob')")
    w.sql("CREATE TABLE src (name STRING) USING delta")
    w.sql("INSERT INTO src VALUES ('Charlie'), ('Diana')")
    w.sql("""MERGE INTO tbl AS target USING src AS source ON target.name = source.name
      WHEN NOT MATCHED THEN INSERT (name) VALUES (source.name)""")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("ic_007_delete", "DELETE from table with identity column",
      "write", "identityColumn", "delete") { w =>
    w.sql("CREATE TABLE tbl (id BIGINT GENERATED ALWAYS AS IDENTITY, name STRING) USING delta")
    w.sql("INSERT INTO tbl (name) VALUES ('Alice'), ('Bob'), ('Charlie')")
    w.sql("DELETE FROM tbl WHERE name = 'Bob'")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("ic_008_update", "UPDATE non-identity column",
      "write", "identityColumn", "update") { w =>
    w.sql("CREATE TABLE tbl (id BIGINT GENERATED ALWAYS AS IDENTITY, name STRING) USING delta")
    w.sql("INSERT INTO tbl (name) VALUES ('Alice'), ('Bob'), ('Charlie')")
    w.sql("UPDATE tbl SET name = 'Robert' WHERE name = 'Bob'")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("ic_009_partitioned", "Identity column with partitioned table",
      "write", "identityColumn", "partitioned") { w =>
    w.sql("CREATE TABLE tbl (id BIGINT GENERATED ALWAYS AS IDENTITY, name STRING, category STRING) USING delta PARTITIONED BY (category)")
    w.sql("INSERT INTO tbl (name, category) VALUES ('Alice', 'A'), ('Bob', 'B'), ('Charlie', 'A')")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("ic_010_negative_increment", "Identity column with negative increment",
      "write", "identityColumn", "negativeIncrement") { w =>
    w.sql("CREATE TABLE tbl (id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 0 INCREMENT BY -1), name STRING) USING delta")
    w.sql("INSERT INTO tbl (name) VALUES ('Alice'), ('Bob'), ('Charlie')")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("ic_011_insert_overwrite", "INSERT OVERWRITE with identity column",
      "write", "identityColumn", "overwrite") { w =>
    w.sql("CREATE TABLE tbl (id BIGINT GENERATED ALWAYS AS IDENTITY, name STRING) USING delta")
    w.sql("INSERT INTO tbl (name) VALUES ('Alice'), ('Bob')")
    w.sql("INSERT OVERWRITE tbl (name) VALUES ('Xavier'), ('Yara'), ('Zoe')")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("ic_012_large_batch", "Large batch INSERT with 100 rows and identity column",
      "write", "identityColumn", "largeBatch") { w =>
    w.sql("CREATE TABLE tbl (id BIGINT GENERATED ALWAYS AS IDENTITY, name STRING) USING delta")
    w.sql("INSERT INTO tbl (name) SELECT concat('user_', id) FROM range(100)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("ic_013_multi_column", "Identity column with additional columns",
      "write", "identityColumn", "multiColumn") { w =>
    w.sql("CREATE TABLE tbl (id BIGINT GENERATED ALWAYS AS IDENTITY, name STRING, age INT, email STRING) USING delta")
    w.sql("""INSERT INTO tbl (name, age, email) VALUES
      ('Alice', 30, 'alice@test.com'),
      ('Bob', 25, 'bob@test.com'),
      ('Charlie', 35, 'charlie@test.com')""")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("ic_014_identity_plus_generated", "Identity column with generated column",
      "write", "identityColumn", "combined") { w =>
    w.sql("""CREATE TABLE tbl (
        id BIGINT GENERATED ALWAYS AS IDENTITY, name STRING,
        name_upper STRING GENERATED ALWAYS AS (UPPER(name))
      ) USING delta""")
    w.sql("INSERT INTO tbl (name) VALUES ('Alice'), ('Bob'), ('Charlie')")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("ic_015_merge_upsert", "MERGE upsert with identity column",
      "write", "identityColumn", "merge", "upsert") { w =>
    w.sql("CREATE TABLE tbl (id BIGINT GENERATED ALWAYS AS IDENTITY, name STRING, value INT) USING delta")
    w.sql("INSERT INTO tbl (name, value) VALUES ('Alice', 10), ('Bob', 20), ('Charlie', 30)")
    w.sql("CREATE TABLE src (name STRING, value INT) USING delta")
    w.sql("INSERT INTO src VALUES ('Bob', 25), ('Diana', 40)")
    w.sql("""MERGE INTO tbl AS target USING src AS source ON target.name = source.name
      WHEN MATCHED THEN UPDATE SET target.value = source.value
      WHEN NOT MATCHED THEN INSERT (name, value) VALUES (source.name, source.value)""")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

}.runAll()

// =============================================================================
// Row Tracking
// =============================================================================

new WorkloadSuite("write_row_tracking") {

  test("rt_001_insert", "INSERT into row-tracking-enabled table",
      "write", "rowTracking", "insert") { w =>
    w.sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableRowTracking' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(10)")
    w.sql("INSERT INTO tbl SELECT id FROM range(10, 20)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("rt_002_update", "UPDATE on row-tracking table",
      "write", "rowTracking", "update") { w =>
    w.sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableRowTracking' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(10)")
    w.sql("UPDATE tbl SET id = id + 100 WHERE id < 5")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("rt_003_delete", "DELETE on row-tracking table",
      "write", "rowTracking", "delete") { w =>
    w.sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableRowTracking' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(10)")
    w.sql("DELETE FROM tbl WHERE id >= 7")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("rt_004_merge", "MERGE on row-tracking table",
      "write", "rowTracking", "merge") { w =>
    w.sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableRowTracking' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(10)")
    w.sql("CREATE TABLE src (id BIGINT) USING delta")
    w.sql("INSERT INTO src SELECT id FROM range(5, 15)")
    w.sql("""MERGE INTO tbl t USING src s ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET t.id = s.id
      WHEN NOT MATCHED THEN INSERT (id) VALUES (s.id)""")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("rt_005_with_dvs", "Row tracking with deletion vectors",
      "write", "rowTracking", "deletionVectors") { w =>
    w.sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES (
        'delta.enableRowTracking' = 'true',
        'delta.enableDeletionVectors' = 'true'
      )""")
    w.sql("INSERT INTO tbl SELECT id FROM range(20)")
    w.sql("DELETE FROM tbl WHERE id IN (3, 7, 11, 15)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("rt_006_partitioned", "Row tracking on partitioned table",
      "write", "rowTracking", "partitioned") { w =>
    w.sql("""CREATE TABLE tbl (id INT, part STRING) USING delta
      PARTITIONED BY (part)
      TBLPROPERTIES ('delta.enableRowTracking' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1, 'a'), (2, 'a'), (3, 'b'), (4, 'b'), (5, 'c')")
    w.sql("INSERT INTO tbl VALUES (6, 'a'), (7, 'c')")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("rt_007_insert_overwrite", "Row tracking with INSERT OVERWRITE",
      "write", "rowTracking", "insertOverwrite") { w =>
    w.sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableRowTracking' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(10)")
    w.sql("INSERT OVERWRITE tbl SELECT id FROM range(20, 25)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("rt_008_optimize", "Row tracking with OPTIMIZE",
      "write", "rowTracking", "optimize") { w =>
    w.sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableRowTracking' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(0, 10)")
    w.sql("INSERT INTO tbl SELECT id FROM range(10, 20)")
    w.sql("INSERT INTO tbl SELECT id FROM range(20, 30)")
    w.sql("OPTIMIZE tbl")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("rt_009_multi_insert", "Multiple INSERTs with row tracking",
      "write", "rowTracking", "multipleInserts") { w =>
    w.sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableRowTracking' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(0, 5)")
    w.sql("INSERT INTO tbl SELECT id FROM range(5, 10)")
    w.sql("INSERT INTO tbl SELECT id FROM range(10, 15)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("rt_010_schema_evolution", "Row tracking with schema evolution",
      "write", "rowTracking", "schemaEvolution") { w =>
    w.sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableRowTracking' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(5)")
    w.sql("ALTER TABLE tbl ADD COLUMN (name STRING)")
    w.sql("INSERT INTO tbl VALUES (5, 'five'), (6, 'six'), (7, 'seven')")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("rt_011_merge_nmbs", "Row tracking with MERGE NOT MATCHED BY SOURCE",
      "write", "rowTracking", "merge", "nmbs") { w =>
    w.sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES ('delta.enableRowTracking' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(10)")
    w.sql("CREATE TABLE src (id BIGINT) USING delta")
    w.sql("INSERT INTO src SELECT id FROM range(0, 5)")
    w.sql("""MERGE INTO tbl t USING src s ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET t.id = s.id
      WHEN NOT MATCHED BY SOURCE THEN DELETE""")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("rt_012_conditional_update", "Row tracking with conditional UPDATE",
      "write", "rowTracking", "conditionalUpdate") { w =>
    w.sql("""CREATE TABLE tbl (id INT, value INT) USING delta
      TBLPROPERTIES ('delta.enableRowTracking' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id, id * 10 FROM range(10)")
    w.sql("UPDATE tbl SET value = value + 1000 WHERE id % 2 = 0")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("rt_013_with_cdc", "Row tracking with CDC",
      "write", "rowTracking", "cdc") { w =>
    w.sql("""CREATE TABLE tbl (id BIGINT) USING delta
      TBLPROPERTIES (
        'delta.enableRowTracking' = 'true',
        'delta.enableChangeDataFeed' = 'true'
      )""")
    w.sql("INSERT INTO tbl SELECT id FROM range(10)")
    w.sql("UPDATE tbl SET id = id + 100 WHERE id < 3")
    w.sql("DELETE FROM tbl WHERE id >= 8")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("rt_014_enable_on_existing", "Enable row tracking on existing table",
      "write", "rowTracking", "enable", "alter") { w =>
    w.sql("CREATE TABLE tbl (id BIGINT) USING delta")
    w.sql("INSERT INTO tbl SELECT id FROM range(10)")
    w.sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.enableRowTracking' = 'true')")
    w.sql("INSERT INTO tbl SELECT id FROM range(10, 20)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("rt_015_type_widening", "Row tracking with type widening",
      "write", "rowTracking", "typeWidening") { w =>
    w.sql("""CREATE TABLE tbl (id INT, value TINYINT) USING delta
      TBLPROPERTIES (
        'delta.enableRowTracking' = 'true',
        'delta.enableTypeWidening' = 'true'
      )""")
    w.sql("INSERT INTO tbl VALUES (1, 10), (2, 20), (3, 30)")
    w.sql("ALTER TABLE tbl ALTER COLUMN value TYPE INT")
    w.sql("INSERT INTO tbl VALUES (4, 1000), (5, 2000)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

}.runAll()

// =============================================================================
// Check Constraints
// =============================================================================

new WorkloadSuite("write_check_constraints") {

  test("cc_001_simple_positive", "INSERT satisfying simple CHECK constraint (value > 0)",
      "write", "checkConstraint", "insert") { w =>
    w.sql("CREATE TABLE tbl (id INT, value INT) USING delta")
    w.sql("ALTER TABLE tbl ADD CONSTRAINT positive_value CHECK (value > 0)")
    w.sql("INSERT INTO tbl VALUES (1, 10), (2, 20), (3, 30)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("cc_002_not_null", "INSERT satisfying NOT NULL constraint",
      "write", "checkConstraint", "notNull") { w =>
    w.sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
    w.sql("ALTER TABLE tbl ADD CONSTRAINT name_not_null CHECK (name IS NOT NULL)")
    w.sql("INSERT INTO tbl VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("cc_003_range", "INSERT satisfying range constraint",
      "write", "checkConstraint", "range") { w =>
    w.sql("CREATE TABLE tbl (id INT, name STRING, age INT) USING delta")
    w.sql("ALTER TABLE tbl ADD CONSTRAINT valid_age CHECK (age >= 0 AND age <= 150)")
    w.sql("INSERT INTO tbl VALUES (1, 'alice', 30), (2, 'bob', 0), (3, 'carol', 150)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("cc_004_enum", "INSERT satisfying string enum constraint",
      "write", "checkConstraint", "enum") { w =>
    w.sql("CREATE TABLE tbl (id INT, status STRING) USING delta")
    w.sql("ALTER TABLE tbl ADD CONSTRAINT valid_status CHECK (status IN ('active', 'inactive', 'pending'))")
    w.sql("INSERT INTO tbl VALUES (1, 'active'), (2, 'inactive'), (3, 'pending')")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("cc_005_update", "UPDATE satisfying constraint",
      "write", "checkConstraint", "update") { w =>
    w.sql("CREATE TABLE tbl (id INT, value INT) USING delta")
    w.sql("ALTER TABLE tbl ADD CONSTRAINT positive_value CHECK (value > 0)")
    w.sql("INSERT INTO tbl VALUES (1, 10), (2, 20), (3, 30)")
    w.sql("UPDATE tbl SET value = 99 WHERE id = 2")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("cc_006_merge", "MERGE satisfying constraint",
      "write", "checkConstraint", "merge") { w =>
    w.sql("CREATE TABLE tbl (id INT, value INT) USING delta")
    w.sql("ALTER TABLE tbl ADD CONSTRAINT positive_value CHECK (value > 0)")
    w.sql("INSERT INTO tbl VALUES (1, 10), (2, 20), (3, 30)")
    w.sql("CREATE TABLE src (id INT, value INT) USING delta")
    w.sql("INSERT INTO src VALUES (2, 25), (4, 40), (5, 50)")
    w.sql("""MERGE INTO tbl AS target USING src AS source ON target.id = source.id
      WHEN MATCHED THEN UPDATE SET target.value = source.value
      WHEN NOT MATCHED THEN INSERT (id, value) VALUES (source.id, source.value)""")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("cc_007_multiple", "Multiple CHECK constraints on same table",
      "write", "checkConstraint", "multiple") { w =>
    w.sql("CREATE TABLE tbl (id INT, value INT, name STRING) USING delta")
    w.sql("ALTER TABLE tbl ADD CONSTRAINT positive_value CHECK (value > 0)")
    w.sql("ALTER TABLE tbl ADD CONSTRAINT name_not_null CHECK (name IS NOT NULL)")
    w.sql("ALTER TABLE tbl ADD CONSTRAINT name_nonempty CHECK (length(name) > 0)")
    w.sql("INSERT INTO tbl VALUES (1, 10, 'alice'), (2, 20, 'bob'), (3, 30, 'carol')")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("cc_008_compound", "CHECK constraint with compound expression",
      "write", "checkConstraint", "compound") { w =>
    w.sql("CREATE TABLE tbl (id INT, price DOUBLE, quantity INT) USING delta")
    w.sql("ALTER TABLE tbl ADD CONSTRAINT valid_order CHECK ((price > 0 AND quantity >= 0) OR (price = 0 AND quantity = 0))")
    w.sql("INSERT INTO tbl VALUES (1, 9.99, 5), (2, 19.99, 1), (3, 0.0, 0)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("cc_009_add_drop", "ADD then DROP constraint - insert after drop",
      "write", "checkConstraint", "drop") { w =>
    w.sql("CREATE TABLE tbl (id INT, value INT) USING delta")
    w.sql("ALTER TABLE tbl ADD CONSTRAINT positive_value CHECK (value > 0)")
    w.sql("INSERT INTO tbl VALUES (1, 10), (2, 20)")
    w.sql("ALTER TABLE tbl DROP CONSTRAINT positive_value")
    w.sql("INSERT INTO tbl VALUES (3, -5), (4, 0)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("cc_010_partitioned", "CHECK constraint on partitioned table",
      "write", "checkConstraint", "partitioned") { w =>
    w.sql("CREATE TABLE tbl (id INT, value INT, category STRING) USING delta PARTITIONED BY (category)")
    w.sql("ALTER TABLE tbl ADD CONSTRAINT positive_value CHECK (value > 0)")
    w.sql("INSERT INTO tbl VALUES (1, 10, 'A'), (2, 20, 'B'), (3, 30, 'A'), (4, 40, 'C')")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("cc_011_string_functions", "CHECK constraint with string functions",
      "write", "checkConstraint", "stringFunction") { w =>
    w.sql("CREATE TABLE tbl (id INT, email STRING) USING delta")
    w.sql("ALTER TABLE tbl ADD CONSTRAINT valid_email CHECK (length(email) > 5 AND email LIKE '%@%')")
    w.sql("INSERT INTO tbl VALUES (1, 'alice@example.com'), (2, 'bob@test.org'), (3, 'carol@foo.io')")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("cc_012_delete", "DELETE from constrained table",
      "write", "checkConstraint", "delete") { w =>
    w.sql("CREATE TABLE tbl (id INT, value INT) USING delta")
    w.sql("ALTER TABLE tbl ADD CONSTRAINT positive_value CHECK (value > 0)")
    w.sql("INSERT INTO tbl VALUES (1, 10), (2, 20), (3, 30), (4, 40)")
    w.sql("DELETE FROM tbl WHERE id = 2")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("cc_013_insert_overwrite", "INSERT OVERWRITE satisfying constraint",
      "write", "checkConstraint", "overwrite") { w =>
    w.sql("CREATE TABLE tbl (id INT, value INT) USING delta")
    w.sql("ALTER TABLE tbl ADD CONSTRAINT positive_value CHECK (value > 0)")
    w.sql("INSERT INTO tbl VALUES (1, 10), (2, 20)")
    w.sql("INSERT OVERWRITE tbl VALUES (10, 100), (20, 200), (30, 300)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("cc_014_date_comparison", "CHECK constraint with date comparison",
      "write", "checkConstraint", "dateComparison") { w =>
    w.sql("CREATE TABLE tbl (id INT, start_date DATE, end_date DATE) USING delta")
    w.sql("ALTER TABLE tbl ADD CONSTRAINT valid_dates CHECK (end_date >= start_date)")
    w.sql("""INSERT INTO tbl VALUES
      (1, DATE '2024-01-01', DATE '2024-06-30'),
      (2, DATE '2024-03-15', DATE '2024-03-15'),
      (3, DATE '2023-12-01', DATE '2025-01-01')""")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("cc_015_between", "CHECK constraint with BETWEEN",
      "write", "checkConstraint", "between") { w =>
    w.sql("CREATE TABLE tbl (id INT, name STRING, score INT) USING delta")
    w.sql("ALTER TABLE tbl ADD CONSTRAINT valid_score CHECK (score BETWEEN 0 AND 100)")
    w.sql("INSERT INTO tbl VALUES (1, 'alice', 95), (2, 'bob', 0), (3, 'carol', 100), (4, 'dave', 50)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

}.runAll()

// =============================================================================
// Schema Edge Cases
// =============================================================================

new WorkloadSuite("write_schema_edge_cases") {

  test("sch_001_all_primitives", "All primitive types in one table",
      "write", "schema", "primitives") { w =>
    w.sql("""CREATE TABLE tbl (
        id INT, col_boolean BOOLEAN, col_byte TINYINT, col_short SMALLINT,
        col_int INT, col_long BIGINT, col_float FLOAT, col_double DOUBLE,
        col_string STRING, col_binary BINARY, col_date DATE,
        col_timestamp TIMESTAMP, col_decimal DECIMAL(10,2)
      ) USING delta""")
    w.sql("""INSERT INTO tbl VALUES
      (1, true, CAST(1 AS TINYINT), CAST(100 AS SMALLINT), 1000, 100000L,
       1.5, 2.5, 'hello', CAST('abc' AS BINARY), DATE'2024-01-01',
       TIMESTAMP'2024-01-01 12:00:00', 12345.67),
      (2, false, CAST(-1 AS TINYINT), CAST(-100 AS SMALLINT), -1000, -100000L,
       -1.5, -2.5, 'world', CAST('xyz' AS BINARY), DATE'1970-01-01',
       TIMESTAMP'1970-01-01 00:00:00', -12345.67),
      (3, null, null, null, null, null, null, null, null, null, null, null, null)""")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("sch_002_nested_struct_3_levels", "Nested struct 3 levels deep",
      "write", "schema", "nested-struct") { w =>
    w.sql("CREATE TABLE tbl (id INT, nested STRUCT<a: STRUCT<b: STRUCT<c: INT>>>) USING delta")
    w.sql("""INSERT INTO tbl
      SELECT 1, named_struct('a', named_struct('b', named_struct('c', 42)))
      UNION ALL SELECT 2, named_struct('a', named_struct('b', named_struct('c', -1)))
      UNION ALL SELECT 3, null""")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("sch_003_array_of_struct", "Array of struct column",
      "write", "schema", "array-struct") { w =>
    w.sql("CREATE TABLE tbl (id INT, items ARRAY<STRUCT<x: INT, y: STRING>>) USING delta")
    w.sql("""INSERT INTO tbl
      SELECT 1, array(named_struct('x', 10, 'y', 'alpha'), named_struct('x', 20, 'y', 'beta'))
      UNION ALL SELECT 2, array(named_struct('x', 30, 'y', 'gamma'))
      UNION ALL SELECT 3, null""")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("sch_004_map_to_struct", "Map from string to struct",
      "write", "schema", "map-struct") { w =>
    w.sql("CREATE TABLE tbl (id INT, kv MAP<STRING, STRUCT<v: INT>>) USING delta")
    w.sql("""INSERT INTO tbl
      SELECT 1, map('key1', named_struct('v', 100), 'key2', named_struct('v', 200))
      UNION ALL SELECT 2, map('only', named_struct('v', 999))
      UNION ALL SELECT 3, null""")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("sch_005_array_null_elements", "Array with null elements (containsNull=true)",
      "write", "schema", "array-null") { w =>
    w.sql("CREATE TABLE tbl (id INT, vals ARRAY<INT>) USING delta")
    w.sql("""INSERT INTO tbl
      SELECT 1, array(1, null, 3) UNION ALL SELECT 2, array(null, null)
      UNION ALL SELECT 3, array(42) UNION ALL SELECT 4, null""")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("sch_006_map_null_values", "Map with null values (valueContainsNull=true)",
      "write", "schema", "map-null-value") { w =>
    w.sql("CREATE TABLE tbl (id INT, kv MAP<STRING, INT>) USING delta")
    w.sql("""INSERT INTO tbl
      SELECT 1, map('a', 1, 'b', null) UNION ALL SELECT 2, map('c', null)
      UNION ALL SELECT 3, map('d', 42) UNION ALL SELECT 4, null""")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("sch_007_decimal_max_precision", "Decimal(38,18) max precision",
      "write", "schema", "decimal-max-precision") { w =>
    w.sql("CREATE TABLE tbl (id INT, val DECIMAL(38,18)) USING delta")
    w.sql("""INSERT INTO tbl VALUES
      (1, CAST('12345678901234567890.123456789012345678' AS DECIMAL(38,18))),
      (2, CAST('-12345678901234567890.123456789012345678' AS DECIMAL(38,18))),
      (3, CAST('0.000000000000000001' AS DECIMAL(38,18))),
      (4, null)""")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("sch_008_decimal_zero_scale", "Decimal(10,0) zero scale",
      "write", "schema", "decimal-zero-scale") { w =>
    w.sql("CREATE TABLE tbl (id INT, val DECIMAL(10,0)) USING delta")
    w.sql("""INSERT INTO tbl VALUES
      (1, CAST(1234567890 AS DECIMAL(10,0))),
      (2, CAST(-1234567890 AS DECIMAL(10,0))),
      (3, CAST(0 AS DECIMAL(10,0))),
      (4, null)""")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("sch_009_not_null", "Column with NOT NULL constraint",
      "write", "schema", "not-null") { w =>
    w.sql("CREATE TABLE tbl (id INT NOT NULL, name STRING NOT NULL, optional_val INT) USING delta")
    w.sql("INSERT INTO tbl VALUES (1, 'alice', 10), (2, 'bob', null), (3, 'charlie', 30)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("sch_010_deep_nested_map_array", "Deeply nested map<string, array<struct>>",
      "write", "schema", "deep-nested") { w =>
    w.sql("CREATE TABLE tbl (id INT, data MAP<STRING, ARRAY<STRUCT<k: INT>>>) USING delta")
    w.sql("""INSERT INTO tbl
      SELECT 1, map('group1', array(named_struct('k', 1), named_struct('k', 2)),
                  'group2', array(named_struct('k', 3)))
      UNION ALL SELECT 2, map('single', array(named_struct('k', 99)))
      UNION ALL SELECT 3, null""")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("sch_011_multi_array", "Multiple array columns in one table",
      "write", "schema", "multi-array") { w =>
    w.sql("CREATE TABLE tbl (id INT, int_arr ARRAY<INT>, str_arr ARRAY<STRING>, dbl_arr ARRAY<DOUBLE>) USING delta")
    w.sql("""INSERT INTO tbl
      SELECT 1, array(1, 2, 3), array('a', 'b'), array(1.1, 2.2)
      UNION ALL SELECT 2, array(4), array('c', 'd', 'e'), array(3.3)
      UNION ALL SELECT 3, null, null, null""")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("sch_012_multi_map", "Multiple map columns in one table",
      "write", "schema", "multi-map") { w =>
    w.sql("CREATE TABLE tbl (id INT, str_int_map MAP<STRING, INT>, int_str_map MAP<INT, STRING>) USING delta")
    w.sql("""INSERT INTO tbl
      SELECT 1, map('x', 10, 'y', 20), map(1, 'one', 2, 'two')
      UNION ALL SELECT 2, map('z', 30), map(3, 'three')
      UNION ALL SELECT 3, null, null""")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("sch_013_mixed_complex", "Struct + array + map in one table",
      "write", "schema", "mixed-complex") { w =>
    w.sql("""CREATE TABLE tbl (
        id INT, info STRUCT<name: STRING, age: INT>,
        tags ARRAY<STRING>, attrs MAP<STRING, STRING>
      ) USING delta""")
    w.sql("""INSERT INTO tbl
      SELECT 1, named_struct('name', 'alice', 'age', 30),
             array('admin', 'user'), map('dept', 'eng', 'level', 'senior')
      UNION ALL
      SELECT 2, named_struct('name', 'bob', 'age', 25),
             array('user'), map('dept', 'sales')
      UNION ALL SELECT 3, null, null, null""")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("sch_014_long_strings", "Strings with 10000+ characters",
      "write", "schema", "long-string") { w =>
    w.sql("CREATE TABLE tbl (id INT, val STRING) USING delta")
    w.sql(s"INSERT INTO tbl VALUES (1, '${"a" * 10000}'), (2, '${"b" * 50000}'), (3, null)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("sch_015_binary_data", "Binary column with various data patterns",
      "write", "schema", "binary") { w =>
    w.sql("CREATE TABLE tbl (id INT, data BINARY) USING delta")
    w.sql("""INSERT INTO tbl VALUES
      (1, X''), (2, X'DEADBEEF'), (3, X'00FF00FF00FF'),
      (4, CAST('hello binary world' AS BINARY)), (5, null)""")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("sch_016_date_edge", "Date edge cases - epoch, far future, far past",
      "write", "schema", "date-edge") { w =>
    w.sql("CREATE TABLE tbl (id INT, val DATE) USING delta")
    w.sql("""INSERT INTO tbl VALUES
      (1, DATE'1970-01-01'), (2, DATE'9999-12-31'), (3, DATE'0001-01-01'),
      (4, DATE'2000-02-29'), (5, DATE'1969-12-31'), (6, null)""")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("sch_017_timestamp_microsecond", "Timestamp microsecond precision",
      "write", "schema", "timestamp-edge") { w =>
    w.sql("CREATE TABLE tbl (id INT, val TIMESTAMP) USING delta")
    w.sql("""INSERT INTO tbl VALUES
      (1, TIMESTAMP'1970-01-01 00:00:00'),
      (2, TIMESTAMP'2024-06-15 12:30:45.123456'),
      (3, TIMESTAMP'9999-12-31 23:59:59.999999'),
      (4, TIMESTAMP'2000-01-01 00:00:00.000001'),
      (5, null)""")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("sch_018_decimal_boundary", "Max and min decimal boundary values",
      "write", "schema", "decimal-boundary") { w =>
    w.sql("CREATE TABLE tbl (id INT, small_dec DECIMAL(5,2), large_dec DECIMAL(38,0)) USING delta")
    w.sql("""INSERT INTO tbl VALUES
      (1, CAST(999.99 AS DECIMAL(5,2)),
          CAST('99999999999999999999999999999999999999' AS DECIMAL(38,0))),
      (2, CAST(-999.99 AS DECIMAL(5,2)),
          CAST('-99999999999999999999999999999999999999' AS DECIMAL(38,0))),
      (3, CAST(0.00 AS DECIMAL(5,2)), CAST(0 AS DECIMAL(38,0))),
      (4, null, null)""")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("sch_019_boolean_patterns", "Boolean column patterns - all combinations",
      "write", "schema", "boolean-patterns") { w =>
    w.sql("CREATE TABLE tbl (id INT, flag1 BOOLEAN, flag2 BOOLEAN) USING delta")
    w.sql("""INSERT INTO tbl VALUES
      (1, true, true), (2, false, false), (3, true, false),
      (4, false, true), (5, null, true), (6, false, null), (7, null, null)""")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("sch_020_empty_vs_null", "Empty string vs null semantics",
      "write", "schema", "empty-vs-null") { w =>
    w.sql("CREATE TABLE tbl (id INT, str_val STRING, bin_val BINARY) USING delta")
    w.sql("""INSERT INTO tbl VALUES
      (1, '', CAST('' AS BINARY)),
      (2, null, null),
      (3, ' ', CAST(' ' AS BINARY)),
      (4, 'nonempty', CAST('nonempty' AS BINARY))""")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

}.runAll()
