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
      "write", "alter_table", "properties") {
    val w = createTableOp("tbl",
      schema = "v1 INT, v2 STRING")
    updatePropertiesOp(w, setProps = Map(
      "delta.checkpointInterval" -> "20",
      "key" -> "value"))
    val t = registerWriteSpec(w)
    read(t)
    snapshot(t)
  }

  test("alt_002_unset_tblproperties",
      "SET then UNSET TBLPROPERTIES to restore defaults",
      "write", "alter_table", "properties") {
    val w = createTableOp("tbl",
      schema = "v1 INT, v2 STRING")
    updatePropertiesOp(w, setProps = Map(
      "delta.checkpointInterval" -> "20",
      "key" -> "value"))
    updatePropertiesOp(w, unsetProps = Seq("delta.checkpointInterval", "key"))
    val t = registerWriteSpec(w)
    read(t)
    snapshot(t)
    snapshot(t, version = 1)  // after SET, before UNSET
  }

  test("alt_009_set_comment",
      "SET comment via TBLPROPERTIES",
      "write", "alter_table", "properties") {
    val w = createTableOp("tbl",
      schema = "v1 INT")
    updatePropertiesOp(w, setProps = Map("comment" -> "test table"))
    val t = registerWriteSpec(w)
    read(t)
    snapshot(t)
  }

  // ===========================================================================
  // ALTER TABLE: Schema (ADD COLUMNS)
  // Source: AlterTableSuiteWriteCapture.scala
  // ===========================================================================

  test("alt_003_add_columns_simple",
      "ADD two new columns to empty table",
      "write", "alter_table", "schema") {
    val w = createTableOp("tbl",
      schema = "v1 INT, v2 STRING")
    evolveSchemaOp(w, addColumnsDDL = "v3 LONG")
    evolveSchemaOp(w, addColumnsDDL = "v4 DOUBLE")
    val t = registerWriteSpec(w)
    read(t)
    snapshot(t)
  }

  test("alt_004_add_columns_then_insert",
      "Insert data, add columns, then insert with new schema",
      "write", "alter_table", "schema", "insert") {
    val w = createTableOp("tbl",
      schema = "id INT, name STRING")
    insertOp(w, Seq(
      Map("id" -> 1, "name" -> "alice"),
      Map("id" -> 2, "name" -> "bob")))
    evolveSchemaOp(w, addColumnsDDL = "age INT")
    evolveSchemaOp(w, addColumnsDDL = "active BOOLEAN")
    insertOp(w, Seq(Map("id" -> 3, "name" -> "carol", "age" -> 30, "active" -> true)))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    read(t, version = 1, name = "read_before_alter")
    read(t, predicate = "age IS NULL", name = "read_null_age")
    read(t, predicate = "age IS NOT NULL", name = "read_with_age")
    snapshot(t)
  }

  test("alt_005_add_columns_nested",
      "ADD nested column to struct field",
      "write", "alter_table", "schema", "nested") {
    sql("CREATE TABLE tbl (v1 INT, v2 STRUCT<a: INT, b: STRING>) USING delta")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    sql("ALTER TABLE tbl ADD COLUMNS (v2.c LONG)")
    val t = registerWriteSpec(w)
    read(t)
    snapshot(t)
  }

  test("alt_010_multiple_schema_changes",
      "Insert, add column, add another column, insert with full schema",
      "write", "alter_table", "schema") {
    val w = createTableOp("tbl",
      schema = "id INT, name STRING")
    insertOp(w, Seq(Map("id" -> 1, "name" -> "alice")))
    evolveSchemaOp(w, addColumnsDDL = "age INT")
    evolveSchemaOp(w, addColumnsDDL = "email STRING")
    insertOp(w, Seq(Map("id" -> 2, "name" -> "bob", "age" -> 25, "email" -> "bob@test.com")))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    read(t, version = 1, name = "read_after_first_insert")
    read(t, predicate = "email IS NOT NULL", name = "read_with_email")
    snapshot(t)
  }

  // ===========================================================================
  // ALTER TABLE: Enable CDF via properties
  // Source: AlterTableSuiteWriteCapture.scala
  // ===========================================================================

  test("alt_006_enable_cdf",
      "Enable CDF via SET TBLPROPERTIES, then UPDATE to generate CDF data",
      "write", "alter_table", "properties", "cdf") {
    val w = createTableOp("tbl",
      schema = "id INT, data STRING")
    insertOp(w, Seq(
      Map("id" -> 1, "data" -> "a"),
      Map("id" -> 2, "data" -> "b")))
    updatePropertiesOp(w, setProps = Map("delta.enableChangeDataFeed" -> "true"))
    updateOp(w, "id = 1", Map("data" -> "'updated'"))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    read(t, version = 1, name = "read_before_cdf")
    cdf(t, startVersion = 3, endVersion = 3)
    snapshot(t)
  }

  // ===========================================================================
  // Column Mapping: Write operations
  // Source: ColumnMappingWriteCaptureSuite.scala
  // ===========================================================================

  test("cm_001_insert_colmap_name",
      "INSERT into table with columnMapping.mode = name",
      "write", "column_mapping", "insert") {
    val w = createTableOp("tbl",
      schema = "id INT, name STRING, value DOUBLE",
      properties = Map(
        "delta.columnMapping.mode" -> "name",
        "delta.minReaderVersion" -> "2",
        "delta.minWriterVersion" -> "5"))
    insertOp(w, Seq(
      Map("id" -> 1, "name" -> "alice", "value" -> 10.0),
      Map("id" -> 2, "name" -> "bob", "value" -> 20.0),
      Map("id" -> 3, "name" -> "charlie", "value" -> 30.0)))
    val t = registerWriteSpec(w)
    read(t)
    snapshot(t)
  }

  test("cm_002_insert_colmap_id",
      "INSERT into table with columnMapping.mode = id",
      "write", "column_mapping", "insert") {
    val w = createTableOp("tbl",
      schema = "id INT, name STRING, value DOUBLE",
      properties = Map(
        "delta.columnMapping.mode" -> "id",
        "delta.minReaderVersion" -> "2",
        "delta.minWriterVersion" -> "5"))
    insertOp(w, Seq(
      Map("id" -> 1, "name" -> "alice", "value" -> 10.0),
      Map("id" -> 2, "name" -> "bob", "value" -> 20.0),
      Map("id" -> 3, "name" -> "charlie", "value" -> 30.0)))
    val t = registerWriteSpec(w)
    read(t)
    snapshot(t)
  }

  // Skipped: MERGE is not allowed - no mergeOp available
  // test("cm_008_merge_colmap",
  //     "MERGE INTO table with column mapping mode = name",
  //     "write", "column_mapping", "merge") {
  //   val w = createTableOp("tbl",
  //     schema = "id INT, name STRING, value DOUBLE",
  //     properties = Map(
  //       "delta.columnMapping.mode" -> "name",
  //       "delta.minReaderVersion" -> "2",
  //       "delta.minWriterVersion" -> "5"))
  //   insertOp(w, Seq(
  //     Map("id" -> 1, "name" -> "alice", "value" -> 10.0),
  //     Map("id" -> 2, "name" -> "bob", "value" -> 20.0)))
  //   // Create source as temp view via SQL
  //   sql("CREATE OR REPLACE TEMP VIEW source_cm008 AS SELECT * FROM VALUES (2, 'bob_updated', 25.0), (3, 'charlie', 30.0) AS t(id, name, value)")
  //   sql("""MERGE INTO tbl t
  //     USING source_cm008 s ON t.id = s.id
  //     WHEN MATCHED THEN UPDATE SET *
  //     WHEN NOT MATCHED THEN INSERT *""")
  //   writeSpec(t)
  //   read(t, name = "read_after_merge")
  //   snapshot(t)
  // }

  test("cm_009_partitioned_colmap",
      "INSERT into partitioned table with column mapping",
      "write", "column_mapping", "partitioned") {
    val w = createTableOp("tbl",
      schema = "id INT, name STRING, category STRING",
      partitionColumns = Seq("category"),
      properties = Map(
        "delta.columnMapping.mode" -> "name",
        "delta.minReaderVersion" -> "2",
        "delta.minWriterVersion" -> "5"))
    insertOp(w, Seq(
      Map("id" -> 1, "name" -> "alice", "category" -> "A"),
      Map("id" -> 2, "name" -> "bob", "category" -> "B"),
      Map("id" -> 3, "name" -> "charlie", "category" -> "A"),
      Map("id" -> 4, "name" -> "diana", "category" -> "B")))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    read(t, predicate = "category = 'A'", name = "read_cat_a")
    read(t, predicate = "category = 'B'", name = "read_cat_b")
    snapshot(t)
  }

  test("cm_010_add_column_colmap",
      "ADD COLUMN with column mapping then INSERT with new column",
      "write", "column_mapping", "alter_table", "schema") {
    val w = createTableOp("tbl",
      schema = "id INT, name STRING",
      properties = Map(
        "delta.columnMapping.mode" -> "name",
        "delta.minReaderVersion" -> "2",
        "delta.minWriterVersion" -> "5"))
    insertOp(w, Seq(
      Map("id" -> 1, "name" -> "alice"),
      Map("id" -> 2, "name" -> "bob")))
    evolveSchemaOp(w, addColumnsDDL = "age INT")
    insertOp(w, Seq(Map("id" -> 3, "name" -> "charlie", "age" -> 30)))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    read(t, version = 1, name = "read_before_add_column")
    read(t, predicate = "age IS NULL", name = "read_null_age")
    snapshot(t)
  }

  test("cm_012_colmap_dv",
      "DELETE with deletion vectors on column-mapped table",
      "write", "column_mapping", "dv", "delete") {
    val w = createTableOp("tbl",
      schema = "id INT, name STRING, value DOUBLE",
      properties = Map(
        "delta.columnMapping.mode" -> "name",
        "delta.enableDeletionVectors" -> "true",
        "delta.minReaderVersion" -> "3",
        "delta.minWriterVersion" -> "7"))
    insertOp(w, Seq(
      Map("id" -> 1, "name" -> "alice", "value" -> 10.0),
      Map("id" -> 2, "name" -> "bob", "value" -> 20.0),
      Map("id" -> 3, "name" -> "charlie", "value" -> 30.0),
      Map("id" -> 4, "name" -> "diana", "value" -> 40.0)))
    deleteOp(w, predicate = "id = 2")
    val t = registerWriteSpec(w)
    read(t, name = "read_after_delete")
    read(t, version = 1, name = "read_before_delete")
    snapshot(t)
  }

  test("cm_013_insert_overwrite_colmap",
      "INSERT OVERWRITE on column-mapped table",
      "write", "column_mapping", "insert_overwrite") {
    val w = createTableOp("tbl",
      schema = "id INT, name STRING, value DOUBLE",
      properties = Map(
        "delta.columnMapping.mode" -> "name",
        "delta.minReaderVersion" -> "2",
        "delta.minWriterVersion" -> "5"))
    insertOp(w, Seq(
      Map("id" -> 1, "name" -> "alice", "value" -> 10.0),
      Map("id" -> 2, "name" -> "bob", "value" -> 20.0)))
    sql("INSERT OVERWRITE tbl VALUES (10, 'xavier', 100.0), (11, 'yara', 110.0), (12, 'zack', 120.0)")
    val t = registerWriteSpec(w)
    read(t, name = "read_after_overwrite")
    read(t, version = 1, name = "read_before_overwrite")
    snapshot(t)
  }

  test("cm_014_special_chars_colmap",
      "Column mapping with special characters in column names",
      "write", "column_mapping") {
    sql("""CREATE TABLE tbl (
      `first name` STRING,
      `last name` STRING,
      `age (years)` INT
    ) USING delta
      TBLPROPERTIES (
        'delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2',
        'delta.minWriterVersion' = '5')""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("first name" -> "Alice", "last name" -> "Smith", "age (years)" -> 30),
      Map("first name" -> "Bob", "last name" -> "Jones", "age (years)" -> 25),
      Map("first name" -> "Charlie", "last name" -> "Brown", "age (years)" -> 35)))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("cm_015_enable_colmap_on_existing",
      "Enable column mapping on existing table via SET TBLPROPERTIES then INSERT",
      "write", "column_mapping", "alter_table") {
    val w = createTableOp("tbl",
      schema = "id INT, name STRING, value DOUBLE")
    insertOp(w, Seq(
      Map("id" -> 1, "name" -> "alice", "value" -> 10.0),
      Map("id" -> 2, "name" -> "bob", "value" -> 20.0)))
    updatePropertiesOp(w, setProps = Map(
      "delta.columnMapping.mode" -> "name",
      "delta.minReaderVersion" -> "2",
      "delta.minWriterVersion" -> "5"))
    insertOp(w, Seq(Map("id" -> 3, "name" -> "charlie", "value" -> 30.0)))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    read(t, version = 1, name = "read_before_colmap")
    snapshot(t)
  }

  // ===========================================================================
  // Protocol Upgrades
  // Source: ProtocolUpgradeWriteCaptureSuite.scala
  // ===========================================================================

  test("pu_001_enable_dvs",
      "Enable deletion vectors via SET TBLPROPERTIES",
      "write", "protocol", "dv") {
    val w = createTableOp("tbl",
      schema = "id INT, name STRING")
    insertOp(w, Seq(
      Map("id" -> 1, "name" -> "a"),
      Map("id" -> 2, "name" -> "b"),
      Map("id" -> 3, "name" -> "c")))
    updatePropertiesOp(w, setProps = Map("delta.enableDeletionVectors" -> "true"))
    val t = registerWriteSpec(w)
    read(t)
    snapshot(t)
    snapshot(t, version = 1)  // before upgrade
  }

  test("pu_002_enable_cdf",
      "Enable change data feed via SET TBLPROPERTIES",
      "write", "protocol", "cdf") {
    val w = createTableOp("tbl",
      schema = "id INT, name STRING")
    insertOp(w, Seq(
      Map("id" -> 1, "name" -> "a"),
      Map("id" -> 2, "name" -> "b")))
    updatePropertiesOp(w, setProps = Map("delta.enableChangeDataFeed" -> "true"))
    val t = registerWriteSpec(w)
    read(t)
    snapshot(t)
    snapshot(t, version = 1)  // before upgrade
  }

  test("pu_003_enable_row_tracking",
      "Enable row tracking via SET TBLPROPERTIES",
      "write", "protocol", "row_tracking") {
    val w = createTableOp("tbl",
      schema = "id INT, name STRING")
    insertOp(w, Seq(
      Map("id" -> 1, "name" -> "a"),
      Map("id" -> 2, "name" -> "b")))
    updatePropertiesOp(w, setProps = Map("delta.enableRowTracking" -> "true"))
    val t = registerWriteSpec(w)
    read(t)
    snapshot(t)
    snapshot(t, version = 1)  // before upgrade
  }

  test("pu_004_enable_type_widening",
      "Enable type widening via SET TBLPROPERTIES",
      "write", "protocol", "type_widening") {
    val w = createTableOp("tbl",
      schema = "id INT, value INT")
    insertOp(w, Seq(
      Map("id" -> 1, "value" -> 10),
      Map("id" -> 2, "value" -> 20)))
    updatePropertiesOp(w, setProps = Map("delta.enableTypeWidening" -> "true"))
    val t = registerWriteSpec(w)
    read(t)
    snapshot(t)
    snapshot(t, version = 1)  // before upgrade
  }

  test("pu_005_enable_column_mapping",
      "Enable column mapping name mode with protocol version bump",
      "write", "protocol", "column_mapping") {
    val w = createTableOp("tbl",
      schema = "id INT, name STRING")
    insertOp(w, Seq(
      Map("id" -> 1, "name" -> "a"),
      Map("id" -> 2, "name" -> "b")))
    updatePropertiesOp(w, setProps = Map(
      "delta.columnMapping.mode" -> "name",
      "delta.minReaderVersion" -> "2",
      "delta.minWriterVersion" -> "5"))
    val t = registerWriteSpec(w)
    read(t)
    snapshot(t)
    snapshot(t, version = 1)  // before upgrade
  }

  test("pu_006_upgrade_reader_version",
      "Upgrade minReaderVersion to 2 and minWriterVersion to 5",
      "write", "protocol") {
    val w = createTableOp("tbl",
      schema = "id INT, name STRING")
    insertOp(w, Seq(
      Map("id" -> 1, "name" -> "a"),
      Map("id" -> 2, "name" -> "b")))
    updatePropertiesOp(w, setProps = Map(
      "delta.minWriterVersion" -> "5",
      "delta.minReaderVersion" -> "2"))
    val t = registerWriteSpec(w)
    read(t)
    snapshot(t)
    snapshot(t, version = 1)  // before upgrade
  }

  test("pu_007_upgrade_writer_version",
      "Upgrade minWriterVersion to 4",
      "write", "protocol") {
    val w = createTableOp("tbl",
      schema = "id INT, name STRING")
    insertOp(w, Seq(
      Map("id" -> 1, "name" -> "a"),
      Map("id" -> 2, "name" -> "b")))
    updatePropertiesOp(w, setProps = Map(
      "delta.minReaderVersion" -> "1",
      "delta.minWriterVersion" -> "4"))
    val t = registerWriteSpec(w)
    read(t)
    snapshot(t)
    snapshot(t, version = 1)  // before upgrade
  }

  test("pu_008_enable_append_only",
      "Enable appendOnly via SET TBLPROPERTIES",
      "write", "protocol") {
    val w = createTableOp("tbl",
      schema = "id INT, name STRING")
    insertOp(w, Seq(
      Map("id" -> 1, "name" -> "a"),
      Map("id" -> 2, "name" -> "b")))
    updatePropertiesOp(w, setProps = Map("delta.appendOnly" -> "true"))
    val t = registerWriteSpec(w)
    read(t)
    snapshot(t)
    snapshot(t, version = 1)  // before upgrade
  }

  test("pu_009_enable_multi_features",
      "Enable deletion vectors and CDF in single SET TBLPROPERTIES",
      "write", "protocol", "dv", "cdf") {
    val w = createTableOp("tbl",
      schema = "id INT, name STRING")
    insertOp(w, Seq(
      Map("id" -> 1, "name" -> "a"),
      Map("id" -> 2, "name" -> "b"),
      Map("id" -> 3, "name" -> "c")))
    updatePropertiesOp(w, setProps = Map(
      "delta.enableDeletionVectors" -> "true",
      "delta.enableChangeDataFeed" -> "true"))
    val t = registerWriteSpec(w)
    read(t)
    snapshot(t)
    snapshot(t, version = 1)  // before upgrade
  }

  test("pu_010_upgrade_then_insert",
      "Enable DVs then INSERT new rows",
      "write", "protocol", "dv", "insert") {
    val w = createTableOp("tbl",
      schema = "id INT, name STRING")
    insertOp(w, Seq(
      Map("id" -> 1, "name" -> "a"),
      Map("id" -> 2, "name" -> "b")))
    updatePropertiesOp(w, setProps = Map("delta.enableDeletionVectors" -> "true"))
    insertOp(w, Seq(
      Map("id" -> 3, "name" -> "c"),
      Map("id" -> 4, "name" -> "d")))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    read(t, version = 1, name = "read_before_upgrade")
    snapshot(t)
  }

  // Skipped: MERGE is not allowed - no mergeOp available
  // test("pu_011_upgrade_then_merge",
  //     "Enable DVs then MERGE to update and insert rows",
  //     "write", "protocol", "dv", "merge") {
  //   val w = createTableOp("tbl",
  //     schema = "id INT, name STRING")
  //   insertOp(w, Seq(
  //     Map("id" -> 1, "name" -> "a"),
  //     Map("id" -> 2, "name" -> "b"),
  //     Map("id" -> 3, "name" -> "c")))
  //   updatePropertiesOp(w, setProps = Map("delta.enableDeletionVectors" -> "true"))
  //   // Source data for merge
  //   sql("CREATE OR REPLACE TEMP VIEW source_pu011 AS SELECT * FROM VALUES (2, 'updated_b'), (4, 'd') AS t(id, name)")
  //   sql("""MERGE INTO tbl t
  //     USING source_pu011 s ON t.id = s.id
  //     WHEN MATCHED THEN UPDATE SET t.name = s.name
  //     WHEN NOT MATCHED THEN INSERT *""")
  //   writeSpec(t)
  //   read(t, name = "read_after_merge")
  //   read(t, version = 1, name = "read_before_upgrade")
  //   snapshot(t)
  // }

  test("pu_012_enable_check_constraints",
      "Enable checkConstraints feature via SET TBLPROPERTIES",
      "write", "protocol") {
    val w = createTableOp("tbl",
      schema = "id INT, value INT")
    insertOp(w, Seq(
      Map("id" -> 1, "value" -> 10),
      Map("id" -> 2, "value" -> 20)))
    updatePropertiesOp(w, setProps = Map("delta.feature.checkConstraints" -> "supported"))
    val t = registerWriteSpec(w)
    read(t)
    snapshot(t)
    snapshot(t, version = 1)  // before upgrade
  }

  test("pu_014_upgrade_table_features",
      "Upgrade to table features protocol (writer 7 + reader 3 in one step)",
      "write", "protocol") {
    val w = createTableOp("tbl",
      schema = "id INT, name STRING")
    insertOp(w, Seq(
      Map("id" -> 1, "name" -> "a"),
      Map("id" -> 2, "name" -> "b")))
    updatePropertiesOp(w, setProps = Map(
      "delta.minWriterVersion" -> "7",
      "delta.minReaderVersion" -> "3"))
    val t = registerWriteSpec(w)
    read(t)
    snapshot(t)
    snapshot(t, version = 1)  // before upgrade
  }

  test("pu_015_upgrade_partitioned",
      "Enable DVs on partitioned table with existing data",
      "write", "protocol", "dv", "partitioned") {
    val w = createTableOp("tbl",
      schema = "id INT, category STRING, value DOUBLE",
      partitionColumns = Seq("category"))
    insertOp(w, Seq(
      Map("id" -> 1, "category" -> "x", "value" -> 10.0),
      Map("id" -> 2, "category" -> "x", "value" -> 20.0),
      Map("id" -> 3, "category" -> "y", "value" -> 30.0),
      Map("id" -> 4, "category" -> "y", "value" -> 40.0)))
    updatePropertiesOp(w, setProps = Map("delta.enableDeletionVectors" -> "true"))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    read(t, predicate = "category = 'x'", name = "read_cat_x")
    read(t, predicate = "category = 'y'", name = "read_cat_y")
    snapshot(t)
    snapshot(t, version = 1)  // before upgrade
  }

}
