/**
 * Write workloads for ALTER TABLE (properties, schema) and protocol upgrade operations.
 *
 * Converted from runtime capture suites:
 *   - AlterTableSuiteWriteCapture.scala (ALT-001 to ALT-010)
 *   - ColumnMappingWriteCaptureSuite.scala (CM-001 to CM-015)
 *   - ProtocolUpgradeWriteCaptureSuite.scala (PU-001 to PU-015)
 *
 * Skipped tests (ops not yet available):
 *   - ALT-007, ALT-008: CHANGE COLUMN reorder (FIRST/AFTER) -- no reorder op
 *   - CM-003, CM-005, CM-006, CM-007: RENAME COLUMN -- no renameColumnOp
 *   - CM-004: DROP COLUMN -- no dropColumnOp
 *   - CM-011: MERGE with schema auto-migrate -- no mergeOp + sqlConf
 *   - PU-013: CHANGE COLUMN SET NOT NULL -- no column constraint op
 */

new WorkloadSuite("write_alter") {

  // ===========================================================================
  // ALTER TABLE: Properties
  // Source: AlterTableSuiteWriteCapture.scala
  // ===========================================================================

  test("alt_001_set_tblproperties",
      "SET TBLPROPERTIES on empty table (checkpointInterval + custom key)",
      "write", "alter_table", "properties") { w =>
    val t = w.createTableOp("tbl",
      schema = Seq(Col("v1", "INT"), Col("v2", "STRING")))
    w.setPropertiesOp(t, Map(
      "delta.checkpointInterval" -> "20",
      "key" -> "value"))
    w.writeSpec(t)
    w.read(t)
    w.snapshot(t)
  }

  test("alt_002_unset_tblproperties",
      "SET then UNSET TBLPROPERTIES to restore defaults",
      "write", "alter_table", "properties") { w =>
    val t = w.createTableOp("tbl",
      schema = Seq(Col("v1", "INT"), Col("v2", "STRING")))
    w.setPropertiesOp(t, Map(
      "delta.checkpointInterval" -> "20",
      "key" -> "value"))
    w.sql("ALTER TABLE tbl UNSET TBLPROPERTIES ('delta.checkpointInterval', 'key')")
    w.writeSpec(t)
    w.read(t)
    w.snapshot(t)
    w.snapshot(t, version = 1)  // after SET, before UNSET
  }

  test("alt_009_set_comment",
      "SET comment via TBLPROPERTIES",
      "write", "alter_table", "properties") { w =>
    val t = w.createTableOp("tbl",
      schema = Seq(Col("v1", "INT")))
    w.setPropertiesOp(t, Map("comment" -> "test table"))
    w.writeSpec(t)
    w.read(t)
    w.snapshot(t)
  }

  // ===========================================================================
  // ALTER TABLE: Schema (ADD COLUMNS)
  // Source: AlterTableSuiteWriteCapture.scala
  // ===========================================================================

  test("alt_003_add_columns_simple",
      "ADD two new columns to empty table",
      "write", "alter_table", "schema") { w =>
    val t = w.createTableOp("tbl",
      schema = Seq(Col("v1", "INT"), Col("v2", "STRING")))
    w.addColumnOp(t, Col("v3", "LONG"))
    w.addColumnOp(t, Col("v4", "DOUBLE"))
    w.writeSpec(t)
    w.read(t)
    w.snapshot(t)
  }

  test("alt_004_add_columns_then_insert",
      "Insert data, add columns, then insert with new schema",
      "write", "alter_table", "schema", "insert") { w =>
    val t = w.createTableOp("tbl",
      schema = Seq(Col("id", "INT"), Col("name", "STRING")))
    w.insertOp(t, Seq(
      Map("id" -> 1, "name" -> "alice"),
      Map("id" -> 2, "name" -> "bob")))
    w.addColumnOp(t, Col("age", "INT"))
    w.addColumnOp(t, Col("active", "BOOLEAN"))
    w.sql("INSERT INTO tbl VALUES (3, 'carol', 30, true)")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, version = 1, name = "read_before_alter")
    w.read(t, predicate = "age IS NULL", name = "read_null_age")
    w.read(t, predicate = "age IS NOT NULL", name = "read_with_age")
    w.snapshot(t)
  }

  test("alt_005_add_columns_nested",
      "ADD nested column to struct field",
      "write", "alter_table", "schema", "nested") { w =>
    w.sql("CREATE TABLE tbl (v1 INT, v2 STRUCT<a: INT, b: STRING>) USING delta")
    val t = w.table("tbl")
    w.sql("ALTER TABLE tbl ADD COLUMNS (v2.c LONG)")
    w.writeSpec(t)
    w.read(t)
    w.snapshot(t)
  }

  test("alt_010_multiple_schema_changes",
      "Insert, add column, add another column, insert with full schema",
      "write", "alter_table", "schema") { w =>
    val t = w.createTableOp("tbl",
      schema = Seq(Col("id", "INT"), Col("name", "STRING")))
    w.insertOp(t, Seq(Map("id" -> 1, "name" -> "alice")))
    w.addColumnOp(t, Col("age", "INT"))
    w.addColumnOp(t, Col("email", "STRING"))
    w.sql("INSERT INTO tbl VALUES (2, 'bob', 25, 'bob@test.com')")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, version = 1, name = "read_after_first_insert")
    w.read(t, predicate = "email IS NOT NULL", name = "read_with_email")
    w.snapshot(t)
  }

  // ===========================================================================
  // ALTER TABLE: Enable CDF via properties
  // Source: AlterTableSuiteWriteCapture.scala
  // ===========================================================================

  test("alt_006_enable_cdf",
      "Enable CDF via SET TBLPROPERTIES, then UPDATE to generate CDF data",
      "write", "alter_table", "properties", "cdf") { w =>
    val t = w.createTableOp("tbl",
      schema = Seq(Col("id", "INT"), Col("data", "STRING")))
    w.insertOp(t, Seq(
      Map("id" -> 1, "data" -> "a"),
      Map("id" -> 2, "data" -> "b")))
    w.setPropertiesOp(t, Map("delta.enableChangeDataFeed" -> "true"))
    w.sql("UPDATE tbl SET data = 'updated' WHERE id = 1")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, version = 1, name = "read_before_cdf")
    w.cdf(t, startVersion = 3, endVersion = 3)
    w.snapshot(t)
  }

  // ===========================================================================
  // Column Mapping: Write operations
  // Source: ColumnMappingWriteCaptureSuite.scala
  // ===========================================================================

  test("cm_001_insert_colmap_name",
      "INSERT into table with columnMapping.mode = name",
      "write", "column_mapping", "insert") { w =>
    val t = w.createTableOp("tbl",
      schema = Seq(Col("id", "INT"), Col("name", "STRING"), Col("value", "DOUBLE")),
      properties = Map(
        "delta.columnMapping.mode" -> "name",
        "delta.minReaderVersion" -> "2",
        "delta.minWriterVersion" -> "5"))
    w.insertOp(t, Seq(
      Map("id" -> 1, "name" -> "alice", "value" -> 10.0),
      Map("id" -> 2, "name" -> "bob", "value" -> 20.0),
      Map("id" -> 3, "name" -> "charlie", "value" -> 30.0)))
    w.writeSpec(t)
    w.read(t)
    w.snapshot(t)
  }

  test("cm_002_insert_colmap_id",
      "INSERT into table with columnMapping.mode = id",
      "write", "column_mapping", "insert") { w =>
    val t = w.createTableOp("tbl",
      schema = Seq(Col("id", "INT"), Col("name", "STRING"), Col("value", "DOUBLE")),
      properties = Map(
        "delta.columnMapping.mode" -> "id",
        "delta.minReaderVersion" -> "2",
        "delta.minWriterVersion" -> "5"))
    w.insertOp(t, Seq(
      Map("id" -> 1, "name" -> "alice", "value" -> 10.0),
      Map("id" -> 2, "name" -> "bob", "value" -> 20.0),
      Map("id" -> 3, "name" -> "charlie", "value" -> 30.0)))
    w.writeSpec(t)
    w.read(t)
    w.snapshot(t)
  }

  test("cm_008_merge_colmap",
      "MERGE INTO table with column mapping mode = name",
      "write", "column_mapping", "merge") { w =>
    val t = w.createTableOp("tbl",
      schema = Seq(Col("id", "INT"), Col("name", "STRING"), Col("value", "DOUBLE")),
      properties = Map(
        "delta.columnMapping.mode" -> "name",
        "delta.minReaderVersion" -> "2",
        "delta.minWriterVersion" -> "5"))
    w.insertOp(t, Seq(
      Map("id" -> 1, "name" -> "alice", "value" -> 10.0),
      Map("id" -> 2, "name" -> "bob", "value" -> 20.0)))
    // Create source as temp view via SQL
    w.sql("CREATE OR REPLACE TEMP VIEW source_cm008 AS SELECT * FROM VALUES (2, 'bob_updated', 25.0), (3, 'charlie', 30.0) AS t(id, name, value)")
    w.sql("""MERGE INTO tbl t
      USING source_cm008 s ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *""")
    w.writeSpec(t)
    w.read(t, name = "read_after_merge")
    w.snapshot(t)
  }

  test("cm_009_partitioned_colmap",
      "INSERT into partitioned table with column mapping",
      "write", "column_mapping", "partitioned") { w =>
    val t = w.createTableOp("tbl",
      schema = Seq(Col("id", "INT"), Col("name", "STRING"), Col("category", "STRING")),
      partitionColumns = Seq("category"),
      properties = Map(
        "delta.columnMapping.mode" -> "name",
        "delta.minReaderVersion" -> "2",
        "delta.minWriterVersion" -> "5"))
    w.insertOp(t, Seq(
      Map("id" -> 1, "name" -> "alice", "category" -> "A"),
      Map("id" -> 2, "name" -> "bob", "category" -> "B"),
      Map("id" -> 3, "name" -> "charlie", "category" -> "A"),
      Map("id" -> 4, "name" -> "diana", "category" -> "B")))
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, predicate = "category = 'A'", name = "read_cat_a")
    w.read(t, predicate = "category = 'B'", name = "read_cat_b")
    w.snapshot(t)
  }

  test("cm_010_add_column_colmap",
      "ADD COLUMN with column mapping then INSERT with new column",
      "write", "column_mapping", "alter_table", "schema") { w =>
    val t = w.createTableOp("tbl",
      schema = Seq(Col("id", "INT"), Col("name", "STRING")),
      properties = Map(
        "delta.columnMapping.mode" -> "name",
        "delta.minReaderVersion" -> "2",
        "delta.minWriterVersion" -> "5"))
    w.insertOp(t, Seq(
      Map("id" -> 1, "name" -> "alice"),
      Map("id" -> 2, "name" -> "bob")))
    w.addColumnOp(t, Col("age", "INT"))
    w.sql("INSERT INTO tbl VALUES (3, 'charlie', 30)")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, version = 1, name = "read_before_add_column")
    w.read(t, predicate = "age IS NULL", name = "read_null_age")
    w.snapshot(t)
  }

  test("cm_012_colmap_dv",
      "DELETE with deletion vectors on column-mapped table",
      "write", "column_mapping", "dv", "delete") { w =>
    val t = w.createTableOp("tbl",
      schema = Seq(Col("id", "INT"), Col("name", "STRING"), Col("value", "DOUBLE")),
      properties = Map(
        "delta.columnMapping.mode" -> "name",
        "delta.enableDeletionVectors" -> "true",
        "delta.minReaderVersion" -> "3",
        "delta.minWriterVersion" -> "7"))
    w.insertOp(t, Seq(
      Map("id" -> 1, "name" -> "alice", "value" -> 10.0),
      Map("id" -> 2, "name" -> "bob", "value" -> 20.0),
      Map("id" -> 3, "name" -> "charlie", "value" -> 30.0),
      Map("id" -> 4, "name" -> "diana", "value" -> 40.0)))
    w.deleteOp(t, predicate = "id = 2")
    w.writeSpec(t)
    w.read(t, name = "read_after_delete")
    w.read(t, version = 1, name = "read_before_delete")
    w.snapshot(t)
  }

  test("cm_013_insert_overwrite_colmap",
      "INSERT OVERWRITE on column-mapped table",
      "write", "column_mapping", "insert_overwrite") { w =>
    val t = w.createTableOp("tbl",
      schema = Seq(Col("id", "INT"), Col("name", "STRING"), Col("value", "DOUBLE")),
      properties = Map(
        "delta.columnMapping.mode" -> "name",
        "delta.minReaderVersion" -> "2",
        "delta.minWriterVersion" -> "5"))
    w.insertOp(t, Seq(
      Map("id" -> 1, "name" -> "alice", "value" -> 10.0),
      Map("id" -> 2, "name" -> "bob", "value" -> 20.0)))
    w.sql("INSERT OVERWRITE tbl VALUES (10, 'xavier', 100.0), (11, 'yara', 110.0), (12, 'zack', 120.0)")
    w.writeSpec(t)
    w.read(t, name = "read_after_overwrite")
    w.read(t, version = 1, name = "read_before_overwrite")
    w.snapshot(t)
  }

  test("cm_014_special_chars_colmap",
      "Column mapping with special characters in column names",
      "write", "column_mapping") { w =>
    w.sql("""CREATE TABLE tbl (
      `first name` STRING,
      `last name` STRING,
      `age (years)` INT
    ) USING delta
      TBLPROPERTIES (
        'delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2',
        'delta.minWriterVersion' = '5')""")
    val t = w.table("tbl")
    w.sql("INSERT INTO tbl VALUES ('Alice', 'Smith', 30), ('Bob', 'Jones', 25), ('Charlie', 'Brown', 35)")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("cm_015_enable_colmap_on_existing",
      "Enable column mapping on existing table via SET TBLPROPERTIES then INSERT",
      "write", "column_mapping", "alter_table") { w =>
    val t = w.createTableOp("tbl",
      schema = Seq(Col("id", "INT"), Col("name", "STRING"), Col("value", "DOUBLE")))
    w.insertOp(t, Seq(
      Map("id" -> 1, "name" -> "alice", "value" -> 10.0),
      Map("id" -> 2, "name" -> "bob", "value" -> 20.0)))
    w.setPropertiesOp(t, Map(
      "delta.columnMapping.mode" -> "name",
      "delta.minReaderVersion" -> "2",
      "delta.minWriterVersion" -> "5"))
    w.sql("INSERT INTO tbl VALUES (3, 'charlie', 30.0)")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, version = 1, name = "read_before_colmap")
    w.snapshot(t)
  }

  // ===========================================================================
  // Protocol Upgrades
  // Source: ProtocolUpgradeWriteCaptureSuite.scala
  // ===========================================================================

  test("pu_001_enable_dvs",
      "Enable deletion vectors via SET TBLPROPERTIES",
      "write", "protocol", "dv") { w =>
    val t = w.createTableOp("tbl",
      schema = Seq(Col("id", "INT"), Col("name", "STRING")))
    w.insertOp(t, Seq(
      Map("id" -> 1, "name" -> "a"),
      Map("id" -> 2, "name" -> "b"),
      Map("id" -> 3, "name" -> "c")))
    w.setPropertiesOp(t, Map("delta.enableDeletionVectors" -> "true"))
    w.writeSpec(t)
    w.read(t)
    w.snapshot(t)
    w.snapshot(t, version = 1)  // before upgrade
  }

  test("pu_002_enable_cdf",
      "Enable change data feed via SET TBLPROPERTIES",
      "write", "protocol", "cdf") { w =>
    val t = w.createTableOp("tbl",
      schema = Seq(Col("id", "INT"), Col("name", "STRING")))
    w.insertOp(t, Seq(
      Map("id" -> 1, "name" -> "a"),
      Map("id" -> 2, "name" -> "b")))
    w.setPropertiesOp(t, Map("delta.enableChangeDataFeed" -> "true"))
    w.writeSpec(t)
    w.read(t)
    w.snapshot(t)
    w.snapshot(t, version = 1)  // before upgrade
  }

  test("pu_003_enable_row_tracking",
      "Enable row tracking via SET TBLPROPERTIES",
      "write", "protocol", "row_tracking") { w =>
    val t = w.createTableOp("tbl",
      schema = Seq(Col("id", "INT"), Col("name", "STRING")))
    w.insertOp(t, Seq(
      Map("id" -> 1, "name" -> "a"),
      Map("id" -> 2, "name" -> "b")))
    w.setPropertiesOp(t, Map("delta.enableRowTracking" -> "true"))
    w.writeSpec(t)
    w.read(t)
    w.snapshot(t)
    w.snapshot(t, version = 1)  // before upgrade
  }

  test("pu_004_enable_type_widening",
      "Enable type widening via SET TBLPROPERTIES",
      "write", "protocol", "type_widening") { w =>
    val t = w.createTableOp("tbl",
      schema = Seq(Col("id", "INT"), Col("value", "INT")))
    w.insertOp(t, Seq(
      Map("id" -> 1, "value" -> 10),
      Map("id" -> 2, "value" -> 20)))
    w.setPropertiesOp(t, Map("delta.enableTypeWidening" -> "true"))
    w.writeSpec(t)
    w.read(t)
    w.snapshot(t)
    w.snapshot(t, version = 1)  // before upgrade
  }

  test("pu_005_enable_column_mapping",
      "Enable column mapping name mode with protocol version bump",
      "write", "protocol", "column_mapping") { w =>
    val t = w.createTableOp("tbl",
      schema = Seq(Col("id", "INT"), Col("name", "STRING")))
    w.insertOp(t, Seq(
      Map("id" -> 1, "name" -> "a"),
      Map("id" -> 2, "name" -> "b")))
    w.setPropertiesOp(t, Map(
      "delta.columnMapping.mode" -> "name",
      "delta.minReaderVersion" -> "2",
      "delta.minWriterVersion" -> "5"))
    w.writeSpec(t)
    w.read(t)
    w.snapshot(t)
    w.snapshot(t, version = 1)  // before upgrade
  }

  test("pu_006_upgrade_reader_version",
      "Upgrade minReaderVersion to 2 and minWriterVersion to 5",
      "write", "protocol") { w =>
    val t = w.createTableOp("tbl",
      schema = Seq(Col("id", "INT"), Col("name", "STRING")))
    w.insertOp(t, Seq(
      Map("id" -> 1, "name" -> "a"),
      Map("id" -> 2, "name" -> "b")))
    w.setPropertiesOp(t, Map(
      "delta.minWriterVersion" -> "5",
      "delta.minReaderVersion" -> "2"))
    w.writeSpec(t)
    w.read(t)
    w.snapshot(t)
    w.snapshot(t, version = 1)  // before upgrade
  }

  test("pu_007_upgrade_writer_version",
      "Upgrade minWriterVersion to 4",
      "write", "protocol") { w =>
    val t = w.createTableOp("tbl",
      schema = Seq(Col("id", "INT"), Col("name", "STRING")))
    w.insertOp(t, Seq(
      Map("id" -> 1, "name" -> "a"),
      Map("id" -> 2, "name" -> "b")))
    w.setPropertiesOp(t, Map(
      "delta.minReaderVersion" -> "1",
      "delta.minWriterVersion" -> "4"))
    w.writeSpec(t)
    w.read(t)
    w.snapshot(t)
    w.snapshot(t, version = 1)  // before upgrade
  }

  test("pu_008_enable_append_only",
      "Enable appendOnly via SET TBLPROPERTIES",
      "write", "protocol") { w =>
    val t = w.createTableOp("tbl",
      schema = Seq(Col("id", "INT"), Col("name", "STRING")))
    w.insertOp(t, Seq(
      Map("id" -> 1, "name" -> "a"),
      Map("id" -> 2, "name" -> "b")))
    w.setPropertiesOp(t, Map("delta.appendOnly" -> "true"))
    w.writeSpec(t)
    w.read(t)
    w.snapshot(t)
    w.snapshot(t, version = 1)  // before upgrade
  }

  test("pu_009_enable_multi_features",
      "Enable deletion vectors and CDF in single SET TBLPROPERTIES",
      "write", "protocol", "dv", "cdf") { w =>
    val t = w.createTableOp("tbl",
      schema = Seq(Col("id", "INT"), Col("name", "STRING")))
    w.insertOp(t, Seq(
      Map("id" -> 1, "name" -> "a"),
      Map("id" -> 2, "name" -> "b"),
      Map("id" -> 3, "name" -> "c")))
    w.setPropertiesOp(t, Map(
      "delta.enableDeletionVectors" -> "true",
      "delta.enableChangeDataFeed" -> "true"))
    w.writeSpec(t)
    w.read(t)
    w.snapshot(t)
    w.snapshot(t, version = 1)  // before upgrade
  }

  test("pu_010_upgrade_then_insert",
      "Enable DVs then INSERT new rows",
      "write", "protocol", "dv", "insert") { w =>
    val t = w.createTableOp("tbl",
      schema = Seq(Col("id", "INT"), Col("name", "STRING")))
    w.insertOp(t, Seq(
      Map("id" -> 1, "name" -> "a"),
      Map("id" -> 2, "name" -> "b")))
    w.setPropertiesOp(t, Map("delta.enableDeletionVectors" -> "true"))
    w.insertOp(t, Seq(
      Map("id" -> 3, "name" -> "c"),
      Map("id" -> 4, "name" -> "d")))
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, version = 1, name = "read_before_upgrade")
    w.snapshot(t)
  }

  test("pu_011_upgrade_then_merge",
      "Enable DVs then MERGE to update and insert rows",
      "write", "protocol", "dv", "merge") { w =>
    val t = w.createTableOp("tbl",
      schema = Seq(Col("id", "INT"), Col("name", "STRING")))
    w.insertOp(t, Seq(
      Map("id" -> 1, "name" -> "a"),
      Map("id" -> 2, "name" -> "b"),
      Map("id" -> 3, "name" -> "c")))
    w.setPropertiesOp(t, Map("delta.enableDeletionVectors" -> "true"))
    // Source data for merge
    w.sql("CREATE OR REPLACE TEMP VIEW source_pu011 AS SELECT * FROM VALUES (2, 'updated_b'), (4, 'd') AS t(id, name)")
    w.sql("""MERGE INTO tbl t
      USING source_pu011 s ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET t.name = s.name
      WHEN NOT MATCHED THEN INSERT *""")
    w.writeSpec(t)
    w.read(t, name = "read_after_merge")
    w.read(t, version = 1, name = "read_before_upgrade")
    w.snapshot(t)
  }

  test("pu_012_enable_check_constraints",
      "Enable checkConstraints feature via SET TBLPROPERTIES",
      "write", "protocol") { w =>
    val t = w.createTableOp("tbl",
      schema = Seq(Col("id", "INT"), Col("value", "INT")))
    w.insertOp(t, Seq(
      Map("id" -> 1, "value" -> 10),
      Map("id" -> 2, "value" -> 20)))
    w.setPropertiesOp(t, Map("delta.feature.checkConstraints" -> "supported"))
    w.writeSpec(t)
    w.read(t)
    w.snapshot(t)
    w.snapshot(t, version = 1)  // before upgrade
  }

  test("pu_014_upgrade_table_features",
      "Upgrade to table features protocol (writer 7 then reader 3)",
      "write", "protocol") { w =>
    val t = w.createTableOp("tbl",
      schema = Seq(Col("id", "INT"), Col("name", "STRING")))
    w.insertOp(t, Seq(
      Map("id" -> 1, "name" -> "a"),
      Map("id" -> 2, "name" -> "b")))
    w.setPropertiesOp(t, Map("delta.minWriterVersion" -> "7"))
    w.setPropertiesOp(t, Map("delta.minReaderVersion" -> "3"))
    w.writeSpec(t)
    w.read(t)
    w.snapshot(t)
    w.snapshot(t, version = 1)  // before first upgrade
    w.snapshot(t, version = 2)  // after writer upgrade, before reader upgrade
  }

  test("pu_015_upgrade_partitioned",
      "Enable DVs on partitioned table with existing data",
      "write", "protocol", "dv", "partitioned") { w =>
    val t = w.createTableOp("tbl",
      schema = Seq(Col("id", "INT"), Col("category", "STRING"), Col("value", "DOUBLE")),
      partitionColumns = Seq("category"))
    w.insertOp(t, Seq(
      Map("id" -> 1, "category" -> "x", "value" -> 10.0),
      Map("id" -> 2, "category" -> "x", "value" -> 20.0),
      Map("id" -> 3, "category" -> "y", "value" -> 30.0),
      Map("id" -> 4, "category" -> "y", "value" -> 40.0)))
    w.setPropertiesOp(t, Map("delta.enableDeletionVectors" -> "true"))
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, predicate = "category = 'x'", name = "read_cat_x")
    w.read(t, predicate = "category = 'y'", name = "read_cat_y")
    w.snapshot(t)
    w.snapshot(t, version = 1)  // before upgrade
  }

}.runAll()
