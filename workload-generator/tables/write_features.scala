/**
 * Feature interaction, protocol upgrade, type widening, column mapping,
 * forward compatibility, and default values write workloads.
 *
 * FeatureInteraction (FI-001..FI-015): Combinations of two or more Delta features.
 * ProtocolUpgrade (PU-001..PU-015): Protocol version upgrades and table feature enablement.
 * TypeWidening (TW-001..TW-015): Column type widening operations.
 * ColumnMapping (CM-001..CM-015): Column mapping modes, rename, drop, special chars.
 * ForwardCompat (FC-001..FC-015): Log replay edge cases, checkpoint, compaction.
 * DefaultValues (DV-001..DV-015): DEFAULT column values.
 */

// =============================================================================
// Feature Interaction
// =============================================================================

new WorkloadSuite("write_feature_interaction") {

  test("fi_001_dv_column_mapping", "DV + Column Mapping (name mode) - insert then delete",
      "write", "dv", "columnMapping", "featureInteraction") {
    sql("""CREATE TABLE tbl (id INT, name STRING, value DOUBLE) USING delta
      TBLPROPERTIES (
        'delta.columnMapping.mode' = 'name',
        'delta.enableDeletionVectors' = 'true',
        'delta.minReaderVersion' = '3',
        'delta.minWriterVersion' = '7'
      )""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "name" -> "alice", "value" -> 10.0),
      Map("id" -> 2, "name" -> "bob", "value" -> 20.0),
      Map("id" -> 3, "name" -> "charlie", "value" -> 30.0),
      Map("id" -> 4, "name" -> "diana", "value" -> 40.0),
      Map("id" -> 5, "name" -> "eve", "value" -> 50.0)
    ))
    deleteOp(w, "id IN (2, 4)")
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("fi_002_dv_cm_cdc", "DV + Column Mapping + CDC - CDF captures DV deletes",
      "write", "dv", "columnMapping", "cdc", "featureInteraction") {
    sql("""CREATE TABLE tbl (id INT, name STRING, value DOUBLE) USING delta
      TBLPROPERTIES (
        'delta.columnMapping.mode' = 'name',
        'delta.enableDeletionVectors' = 'true',
        'delta.enableChangeDataFeed' = 'true',
        'delta.minReaderVersion' = '3',
        'delta.minWriterVersion' = '7'
      )""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "name" -> "alice", "value" -> 10.0),
      Map("id" -> 2, "name" -> "bob", "value" -> 20.0),
      Map("id" -> 3, "name" -> "charlie", "value" -> 30.0)
    ))
    deleteOp(w, "id = 2")
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("fi_003_type_widening_dv", "Type Widening + DV - widen INT to LONG then delete",
      "write", "typeWidening", "dv", "featureInteraction") {
    sql("""CREATE TABLE tbl (id INT, value INT) USING delta
      TBLPROPERTIES (
        'delta.enableDeletionVectors' = 'true',
        'delta.enableTypeWidening' = 'true'
      )""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "value" -> 100),
      Map("id" -> 2, "value" -> 200),
      Map("id" -> 3, "value" -> 300)
    ))
    sql("ALTER TABLE tbl ALTER COLUMN value TYPE LONG")
    insertOp(w, Seq(
      Map("id" -> 4, "value" -> 4000000000L),
      Map("id" -> 5, "value" -> 5000000000L)
    ))
    deleteOp(w, "id IN (2, 4)")
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("fi_004_type_widening_cm", "Type Widening + Column Mapping - CM name + INT to LONG",
      "write", "typeWidening", "columnMapping", "featureInteraction") {
    sql("""CREATE TABLE tbl (id INT, value INT) USING delta
      TBLPROPERTIES (
        'delta.columnMapping.mode' = 'name',
        'delta.enableTypeWidening' = 'true',
        'delta.minReaderVersion' = '3',
        'delta.minWriterVersion' = '7'
      )""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "value" -> 100),
      Map("id" -> 2, "value" -> 200)
    ))
    sql("ALTER TABLE tbl ALTER COLUMN value TYPE LONG")
    insertOp(w, Seq(
      Map("id" -> 3, "value" -> 3000000000L)
    ))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("fi_005_row_tracking_dv", "Row Tracking + DV - insert and delete",
      "write", "rowTracking", "dv", "featureInteraction") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES (
        'delta.enableRowTracking' = 'true',
        'delta.enableDeletionVectors' = 'true'
      )""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "value" -> "a"),
      Map("id" -> 2, "value" -> "b"),
      Map("id" -> 3, "value" -> "c"),
      Map("id" -> 4, "value" -> "d"),
      Map("id" -> 5, "value" -> "e")
    ))
    deleteOp(w, "id IN (2, 4)")
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("fi_006_cm_cdc_merge", "CM + CDC + Merge operation",
      "write", "columnMapping", "cdc", "merge", "featureInteraction") {
    sql("""CREATE TABLE tbl (id INT, name STRING, value DOUBLE) USING delta
      TBLPROPERTIES (
        'delta.columnMapping.mode' = 'name',
        'delta.enableChangeDataFeed' = 'true',
        'delta.minReaderVersion' = '2',
        'delta.minWriterVersion' = '5'
      )""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "name" -> "alice", "value" -> 10.0),
      Map("id" -> 2, "name" -> "bob", "value" -> 20.0),
      Map("id" -> 3, "name" -> "charlie", "value" -> 30.0)
    ))
    sql("""CREATE TABLE src (id INT, name STRING, value DOUBLE) USING delta""")
    sql("INSERT INTO src VALUES (2, 'bob_updated', 25.0), (4, 'diana', 40.0)")
    sql("""MERGE INTO tbl t
      USING src s ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("fi_007_dv_partition_delete", "DV + Partition + Delete - partitioned table DV delete",
      "write", "dv", "partition", "delete", "featureInteraction") {
    sql("""CREATE TABLE tbl (id INT, value STRING, category STRING) USING delta
      PARTITIONED BY (category)
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "value" -> "a", "category" -> "X"),
      Map("id" -> 2, "value" -> "b", "category" -> "X"),
      Map("id" -> 3, "value" -> "c", "category" -> "X"),
      Map("id" -> 4, "value" -> "d", "category" -> "Y"),
      Map("id" -> 5, "value" -> "e", "category" -> "Y"),
      Map("id" -> 6, "value" -> "f", "category" -> "Y")
    ))
    deleteOp(w, "category = 'X' AND id = 2")
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("fi_008_cm_partition_overwrite", "CM + Partition + Insert Overwrite",
      "write", "columnMapping", "partition", "insertOverwrite", "featureInteraction") {
    sql("""CREATE TABLE tbl (id INT, name STRING, category STRING) USING delta
      PARTITIONED BY (category)
      TBLPROPERTIES (
        'delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2',
        'delta.minWriterVersion' = '5'
      )""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "name" -> "alice", "category" -> "A"),
      Map("id" -> 2, "name" -> "bob", "category" -> "A"),
      Map("id" -> 3, "name" -> "charlie", "category" -> "B"),
      Map("id" -> 4, "name" -> "diana", "category" -> "B")
    ))
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    sql("INSERT OVERWRITE TABLE tbl VALUES (10, 'xavier', 'A'), (11, 'yara', 'A')")
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("fi_009_dv_generated_column", "DV + Generated Column",
      "write", "dv", "generatedColumn", "featureInteraction") {
    sql("""CREATE TABLE tbl (
        id INT, value INT,
        doubled INT GENERATED ALWAYS AS (value * 2)
      ) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "value" -> 10),
      Map("id" -> 2, "value" -> 20),
      Map("id" -> 3, "value" -> 30),
      Map("id" -> 4, "value" -> 40)
    ))
    deleteOp(w, "id = 3")
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("fi_010_cm_type_widening_merge", "CM + Type Widening + Merge - three features combined",
      "write", "columnMapping", "typeWidening", "merge", "featureInteraction") {
    sql("""CREATE TABLE tbl (id INT, value INT) USING delta
      TBLPROPERTIES (
        'delta.columnMapping.mode' = 'name',
        'delta.enableTypeWidening' = 'true',
        'delta.minReaderVersion' = '3',
        'delta.minWriterVersion' = '7'
      )""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "value" -> 100),
      Map("id" -> 2, "value" -> 200)
    ))
    sql("ALTER TABLE tbl ALTER COLUMN value TYPE LONG")
    sql("CREATE TABLE src (id INT, value LONG) USING delta")
    sql("INSERT INTO src VALUES (2, 2500000000), (3, 3000000000)")
    sql("""MERGE INTO tbl t
      USING src s ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET t.value = s.value
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("fi_011_dv_cdc_update", "DV + CDC + Update",
      "write", "dv", "cdc", "update", "featureInteraction") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES (
        'delta.enableDeletionVectors' = 'true',
        'delta.enableChangeDataFeed' = 'true'
      )""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "value" -> "a"),
      Map("id" -> 2, "value" -> "b"),
      Map("id" -> 3, "value" -> "c"),
      Map("id" -> 4, "value" -> "d")
    ))
    updateOp(w, "id IN (1, 3)", Map("value" -> "'updated'"))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("fi_012_row_tracking_cm", "Row Tracking + Column Mapping",
      "write", "rowTracking", "columnMapping", "featureInteraction") {
    sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
      TBLPROPERTIES (
        'delta.enableRowTracking' = 'true',
        'delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '3',
        'delta.minWriterVersion' = '7'
      )""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "name" -> "alice"),
      Map("id" -> 2, "name" -> "bob"),
      Map("id" -> 3, "name" -> "charlie")
    ))
    insertOp(w, Seq(
      Map("id" -> 4, "name" -> "diana"),
      Map("id" -> 5, "name" -> "eve")
    ))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("fi_013_timestamp_ntz_cm", "TimestampNTZ + Column Mapping",
      "write", "timestampNtz", "columnMapping", "featureInteraction") {
    sql("""CREATE TABLE tbl (
        id INT, event_time TIMESTAMP_NTZ, label STRING
      ) USING delta
      TBLPROPERTIES (
        'delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '3',
        'delta.minWriterVersion' = '7'
      )""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    sql("""INSERT INTO tbl VALUES
      (1, TIMESTAMP_NTZ'2024-01-15 10:30:00', 'first'),
      (2, TIMESTAMP_NTZ'2024-06-20 14:00:00', 'second'),
      (3, TIMESTAMP_NTZ'2024-12-31 23:59:59', 'third')""")
    insertOp(w)
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("fi_014_dv_check_constraint", "DV + Check Constraint",
      "write", "dv", "checkConstraint", "featureInteraction") {
    sql("""CREATE TABLE tbl (id INT, value INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("ALTER TABLE tbl ADD CONSTRAINT value_positive CHECK (value > 0)")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "value" -> 10),
      Map("id" -> 2, "value" -> 20),
      Map("id" -> 3, "value" -> 30),
      Map("id" -> 4, "value" -> 40)
    ))
    deleteOp(w, "id IN (1, 3)")
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("fi_015_all_features", "DV + CM + CDC + Row Tracking - all features combined",
      "write", "dv", "columnMapping", "cdc", "rowTracking", "featureInteraction") {
    sql("""CREATE TABLE tbl (id INT, name STRING, value DOUBLE) USING delta
      TBLPROPERTIES (
        'delta.columnMapping.mode' = 'name',
        'delta.enableDeletionVectors' = 'true',
        'delta.enableChangeDataFeed' = 'true',
        'delta.enableRowTracking' = 'true'
      )""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "name" -> "alice", "value" -> 10.0),
      Map("id" -> 2, "name" -> "bob", "value" -> 20.0),
      Map("id" -> 3, "name" -> "charlie", "value" -> 30.0),
      Map("id" -> 4, "name" -> "diana", "value" -> 40.0)
    ))
    deleteOp(w, "id = 2")
    sql("CREATE TABLE src (id INT, name STRING, value DOUBLE) USING delta")
    sql("INSERT INTO src VALUES (3, 'charlie_updated', 35.0), (5, 'eve', 50.0)")
    sql("""MERGE INTO tbl t
      USING src s ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

}

// =============================================================================
// Protocol Upgrade
// =============================================================================

new WorkloadSuite("write_protocol_upgrade") {

  test("pu_001_enable_dvs", "Enable deletion vectors via SET TBLPROPERTIES",
      "write", "protocolUpgrade", "deletionVectors") {
    sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "name" -> "a"),
      Map("id" -> 2, "name" -> "b"),
      Map("id" -> 3, "name" -> "c")
    ))
    sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("pu_002_enable_cdc", "Enable change data feed",
      "write", "protocolUpgrade", "cdc") {
    sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "name" -> "a"),
      Map("id" -> 2, "name" -> "b")
    ))
    sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')")
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("pu_003_enable_row_tracking", "Enable row tracking",
      "write", "protocolUpgrade", "rowTracking") {
    sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "name" -> "a"),
      Map("id" -> 2, "name" -> "b")
    ))
    sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.enableRowTracking' = 'true')")
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("pu_004_enable_type_widening", "Enable type widening",
      "write", "protocolUpgrade", "typeWidening") {
    sql("CREATE TABLE tbl (id INT, value INT) USING delta")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "value" -> 10),
      Map("id" -> 2, "value" -> 20)
    ))
    sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.enableTypeWidening' = 'true')")
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("pu_005_enable_column_mapping", "Enable column mapping name mode",
      "write", "protocolUpgrade", "columnMapping") {
    sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "name" -> "a"),
      Map("id" -> 2, "name" -> "b")
    ))
    sql("""ALTER TABLE tbl SET TBLPROPERTIES (
      'delta.columnMapping.mode' = 'name',
      'delta.minReaderVersion' = '2',
      'delta.minWriterVersion' = '5'
    )""")
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("pu_006_upgrade_reader_version", "Upgrade minReaderVersion to 2",
      "write", "protocolUpgrade", "readerVersion") {
    sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "name" -> "a"),
      Map("id" -> 2, "name" -> "b")
    ))
    sql("""ALTER TABLE tbl SET TBLPROPERTIES (
      'delta.minWriterVersion' = '5',
      'delta.minReaderVersion' = '2'
    )""")
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("pu_007_upgrade_writer_version", "Upgrade minWriterVersion to 4",
      "write", "protocolUpgrade", "writerVersion") {
    sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "name" -> "a"),
      Map("id" -> 2, "name" -> "b")
    ))
    sql("""ALTER TABLE tbl SET TBLPROPERTIES (
      'delta.minReaderVersion' = '1',
      'delta.minWriterVersion' = '4'
    )""")
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("pu_008_enable_append_only", "Enable appendOnly",
      "write", "protocolUpgrade", "appendOnly") {
    sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "name" -> "a"),
      Map("id" -> 2, "name" -> "b")
    ))
    sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.appendOnly' = 'true')")
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("pu_009_enable_multi_features", "Enable DVs and CDC together",
      "write", "protocolUpgrade", "deletionVectors", "cdc") {
    sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "name" -> "a"),
      Map("id" -> 2, "name" -> "b"),
      Map("id" -> 3, "name" -> "c")
    ))
    sql("""ALTER TABLE tbl SET TBLPROPERTIES (
      'delta.enableDeletionVectors' = 'true',
      'delta.enableChangeDataFeed' = 'true'
    )""")
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("pu_010_upgrade_then_insert", "Protocol upgrade then INSERT",
      "write", "protocolUpgrade", "deletionVectors", "insert") {
    sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "name" -> "a"),
      Map("id" -> 2, "name" -> "b")
    ))
    sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")
    insertOp(w, Seq(
      Map("id" -> 3, "name" -> "c"),
      Map("id" -> 4, "name" -> "d")
    ))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("pu_011_upgrade_then_merge", "Protocol upgrade then MERGE",
      "write", "protocolUpgrade", "deletionVectors", "merge") {
    sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "name" -> "a"),
      Map("id" -> 2, "name" -> "b"),
      Map("id" -> 3, "name" -> "c")
    ))
    sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")
    sql("CREATE TABLE src (id INT, name STRING) USING delta")
    sql("INSERT INTO src VALUES (2, 'updated_b'), (4, 'd')")
    sql("""MERGE INTO tbl t USING src s ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET t.name = s.name
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("pu_012_enable_check_constraints", "Enable checkConstraints feature",
      "write", "protocolUpgrade", "checkConstraints") {
    sql("CREATE TABLE tbl (id INT, value INT) USING delta")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "value" -> 10),
      Map("id" -> 2, "value" -> 20)
    ))
    sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.feature.checkConstraints' = 'supported')")
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("pu_013_invariants_not_null", "Enable invariants via NOT NULL constraint",
      "write", "protocolUpgrade", "invariants") {
    sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "name" -> "a"),
      Map("id" -> 2, "name" -> "b")
    ))
    sql("ALTER TABLE tbl CHANGE COLUMN name SET NOT NULL")
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("pu_014_table_features_protocol", "Upgrade to table features protocol (reader 3, writer 7)",
      "write", "protocolUpgrade", "tableFeatures") {
    sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "name" -> "a"),
      Map("id" -> 2, "name" -> "b")
    ))
    sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.minWriterVersion' = '7')")
    sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.minReaderVersion' = '3')")
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("pu_015_upgrade_partitioned", "Protocol upgrade on partitioned table with data",
      "write", "protocolUpgrade", "deletionVectors", "partitioned") {
    sql("""CREATE TABLE tbl (id INT, category STRING, value DOUBLE) USING delta
      PARTITIONED BY (category)""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "category" -> "x", "value" -> 10.0),
      Map("id" -> 2, "category" -> "x", "value" -> 20.0),
      Map("id" -> 3, "category" -> "y", "value" -> 30.0),
      Map("id" -> 4, "category" -> "y", "value" -> 40.0)
    ))
    sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

}

// =============================================================================
// Type Widening
// =============================================================================

new WorkloadSuite("write_type_widening") {

  test("tw_001_short_to_int", "SHORT to INT widening then INSERT",
      "write", "typeWidening", "shortToInt") {
    sql("CREATE TABLE tbl (id INT, value SHORT) USING delta")
    sql("INSERT INTO tbl VALUES (1, CAST(10 AS SHORT)), (2, CAST(20 AS SHORT))")
    sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.enableTypeWidening' = 'true')")
    sql("ALTER TABLE tbl ALTER COLUMN value TYPE INT")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 3, "value" -> 30000),
      Map("id" -> 4, "value" -> 40000)
    ))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("tw_002_int_to_long", "INT to LONG widening then INSERT",
      "write", "typeWidening", "intToLong") {
    sql("CREATE TABLE tbl (id INT, value INT) USING delta")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "value" -> 100),
      Map("id" -> 2, "value" -> 200)
    ))
    sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.enableTypeWidening' = 'true')")
    sql("ALTER TABLE tbl ALTER COLUMN value TYPE LONG")
    insertOp(w, Seq(
      Map("id" -> 3, "value" -> 3000000000L),
      Map("id" -> 4, "value" -> 4000000000L)
    ))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("tw_003_float_to_double", "FLOAT to DOUBLE widening then INSERT",
      "write", "typeWidening", "floatToDouble") {
    sql("CREATE TABLE tbl (id INT, value FLOAT) USING delta")
    sql("INSERT INTO tbl VALUES (1, CAST(1.5 AS FLOAT)), (2, CAST(2.5 AS FLOAT))")
    sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.enableTypeWidening' = 'true')")
    sql("ALTER TABLE tbl ALTER COLUMN value TYPE DOUBLE")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 3, "value" -> 3.141592653589793),
      Map("id" -> 4, "value" -> 2.718281828459045)
    ))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("tw_004_byte_to_short", "BYTE to SHORT widening then INSERT",
      "write", "typeWidening", "byteToShort") {
    sql("CREATE TABLE tbl (id INT, value TINYINT) USING delta")
    sql("INSERT INTO tbl VALUES (1, CAST(10 AS TINYINT)), (2, CAST(20 AS TINYINT))")
    sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.enableTypeWidening' = 'true')")
    sql("ALTER TABLE tbl ALTER COLUMN value TYPE SHORT")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 3, "value" -> 300),
      Map("id" -> 4, "value" -> 400)
    ))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("tw_005_byte_to_int", "BYTE to INT widening (multi-step)",
      "write", "typeWidening", "byteToInt") {
    sql("CREATE TABLE tbl (id INT, value TINYINT) USING delta")
    sql("INSERT INTO tbl VALUES (1, CAST(5 AS TINYINT)), (2, CAST(10 AS TINYINT))")
    sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.enableTypeWidening' = 'true')")
    sql("ALTER TABLE tbl ALTER COLUMN value TYPE INT")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 3, "value" -> 50000),
      Map("id" -> 4, "value" -> 60000)
    ))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("tw_006_int_to_double", "INT to DOUBLE widening then INSERT",
      "write", "typeWidening", "intToDouble") {
    sql("CREATE TABLE tbl (id INT, value INT) USING delta")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "value" -> 100),
      Map("id" -> 2, "value" -> 200)
    ))
    sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.enableTypeWidening' = 'true')")
    sql("ALTER TABLE tbl ALTER COLUMN value TYPE DOUBLE")
    insertOp(w, Seq(
      Map("id" -> 3, "value" -> 3.14),
      Map("id" -> 4, "value" -> 2.72)
    ))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("tw_007_widen_then_merge", "Type widening then MERGE",
      "write", "typeWidening", "merge") {
    sql("CREATE TABLE tbl (key INT, value SHORT) USING delta")
    sql("INSERT INTO tbl VALUES (1, CAST(10 AS SHORT)), (2, CAST(20 AS SHORT)), (3, CAST(30 AS SHORT))")
    sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.enableTypeWidening' = 'true')")
    sql("ALTER TABLE tbl ALTER COLUMN value TYPE INT")
    sql("CREATE TABLE src (key INT, value INT) USING delta")
    sql("INSERT INTO src VALUES (2, 20000), (4, 40000)")
    sql("""MERGE INTO tbl AS target USING src AS source ON target.key = source.key
      WHEN MATCHED THEN UPDATE SET target.value = source.value
      WHEN NOT MATCHED THEN INSERT *""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("tw_008_widen_then_update", "Type widening then UPDATE",
      "write", "typeWidening", "update") {
    sql("CREATE TABLE tbl (id INT, value SHORT) USING delta")
    sql("INSERT INTO tbl VALUES (1, CAST(10 AS SHORT)), (2, CAST(20 AS SHORT)), (3, CAST(30 AS SHORT))")
    sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.enableTypeWidening' = 'true')")
    sql("ALTER TABLE tbl ALTER COLUMN value TYPE INT")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    updateOp(w, "id = 1", Map("value" -> "50000"))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("tw_009_multi_column", "Multiple columns widened",
      "write", "typeWidening", "multiColumn") {
    sql("CREATE TABLE tbl (id INT, col_a SHORT, col_b FLOAT) USING delta")
    sql("INSERT INTO tbl VALUES (1, CAST(10 AS SHORT), CAST(1.5 AS FLOAT)), (2, CAST(20 AS SHORT), CAST(2.5 AS FLOAT))")
    sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.enableTypeWidening' = 'true')")
    sql("ALTER TABLE tbl ALTER COLUMN col_a TYPE INT")
    sql("ALTER TABLE tbl ALTER COLUMN col_b TYPE DOUBLE")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 3, "col_a" -> 50000, "col_b" -> 3.141592653589793)
    ))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("tw_010_partitioned", "Type widening on partitioned table",
      "write", "typeWidening", "partitioned") {
    sql("CREATE TABLE tbl (id INT, part STRING, value SHORT) USING delta PARTITIONED BY (part)")
    sql("INSERT INTO tbl VALUES (1, 'a', CAST(10 AS SHORT)), (2, 'b', CAST(20 AS SHORT)), (3, 'a', CAST(30 AS SHORT))")
    sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.enableTypeWidening' = 'true')")
    sql("ALTER TABLE tbl ALTER COLUMN value TYPE INT")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 4, "part" -> "a", "value" -> 40000),
      Map("id" -> 5, "part" -> "b", "value" -> 50000)
    ))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("tw_011_preserve_data", "Type widening preserves existing data",
      "write", "typeWidening", "preserveData") {
    sql("CREATE TABLE tbl (id INT, value SHORT) USING delta")
    sql("INSERT INTO tbl VALUES (1, CAST(10 AS SHORT)), (2, CAST(20 AS SHORT)), (3, CAST(30 AS SHORT))")
    sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.enableTypeWidening' = 'true')")
    sql("ALTER TABLE tbl ALTER COLUMN value TYPE INT")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("tw_012_short_to_long", "SHORT to LONG direct widening (skip INT)",
      "write", "typeWidening", "shortToLong") {
    sql("CREATE TABLE tbl (id INT, value SHORT) USING delta")
    sql("INSERT INTO tbl VALUES (1, CAST(10 AS SHORT)), (2, CAST(20 AS SHORT))")
    sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.enableTypeWidening' = 'true')")
    sql("ALTER TABLE tbl ALTER COLUMN value TYPE LONG")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 3, "value" -> 3000000000L),
      Map("id" -> 4, "value" -> 4000000000L)
    ))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("tw_013_decimal_widen", "DECIMAL widening precision increase",
      "write", "typeWidening", "decimal") {
    sql("CREATE TABLE tbl (id INT, amount DECIMAL(5,2)) USING delta")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "amount" -> 123.45),
      Map("id" -> 2, "amount" -> 678.90)
    ))
    sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.enableTypeWidening' = 'true')")
    sql("ALTER TABLE tbl ALTER COLUMN amount TYPE DECIMAL(10,2)")
    insertOp(w, Seq(
      Map("id" -> 3, "amount" -> 12345678.90)
    ))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("tw_014_widen_then_delete", "Type widening then DELETE",
      "write", "typeWidening", "delete") {
    sql("CREATE TABLE tbl (id INT, value SHORT) USING delta")
    sql("INSERT INTO tbl VALUES (1, CAST(10 AS SHORT)), (2, CAST(20 AS SHORT)), (3, CAST(30 AS SHORT)), (4, CAST(40 AS SHORT))")
    sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.enableTypeWidening' = 'true')")
    sql("ALTER TABLE tbl ALTER COLUMN value TYPE INT")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    deleteOp(w, "id > 2")
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("tw_015_schema_evolve_merge", "Type widening with schema evolution MERGE",
      "write", "typeWidening", "schemaEvolution", "merge") {
    sql("CREATE TABLE tbl (key INT, value SHORT) USING delta")
    sql("INSERT INTO tbl VALUES (1, CAST(10 AS SHORT)), (2, CAST(20 AS SHORT))")
    sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.enableTypeWidening' = 'true')")
    sql("ALTER TABLE tbl ALTER COLUMN value TYPE INT")
    sql("CREATE TABLE src (key INT, value INT, extra STRING) USING delta")
    sql("INSERT INTO src VALUES (2, 20000, 'updated'), (3, 30000, 'new')")
    spark.conf.set("spark.delta.schema.autoMerge.enabled", "true")
    sql("""MERGE INTO tbl AS target USING src AS source ON target.key = source.key
      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

}

// =============================================================================
// Column Mapping
// =============================================================================

new WorkloadSuite("write_column_mapping") {

  test("cm_001_name_mode_insert", "INSERT with column mapping mode = name",
      "write", "columnMapping", "name", "insert") {
    sql("""CREATE TABLE tbl (id INT, name STRING, value DOUBLE) USING delta
      TBLPROPERTIES (
        'delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2',
        'delta.minWriterVersion' = '5'
      )""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "name" -> "alice", "value" -> 10.0),
      Map("id" -> 2, "name" -> "bob", "value" -> 20.0),
      Map("id" -> 3, "name" -> "charlie", "value" -> 30.0)
    ))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("cm_002_id_mode_insert", "INSERT with column mapping mode = id",
      "write", "columnMapping", "id", "insert") {
    sql("""CREATE TABLE tbl (id INT, name STRING, value DOUBLE) USING delta
      TBLPROPERTIES (
        'delta.columnMapping.mode' = 'id',
        'delta.minReaderVersion' = '2',
        'delta.minWriterVersion' = '5'
      )""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "name" -> "alice", "value" -> 10.0),
      Map("id" -> 2, "name" -> "bob", "value" -> 20.0),
      Map("id" -> 3, "name" -> "charlie", "value" -> 30.0)
    ))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("cm_003_rename_then_insert", "RENAME COLUMN then INSERT",
      "write", "columnMapping", "rename", "insert") {
    sql("""CREATE TABLE tbl (id INT, name STRING, value DOUBLE) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(Map("id" -> 1, "name" -> "alice", "value" -> 10.0)))
    insertOp(w, Seq(Map("id" -> 2, "name" -> "bob", "value" -> 20.0)))
    sql("ALTER TABLE tbl RENAME COLUMN name TO full_name")
    insertOp(w, Seq(Map("id" -> 3, "full_name" -> "charlie", "value" -> 30.0)))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("cm_004_drop_then_insert", "DROP COLUMN then INSERT",
      "write", "columnMapping", "drop", "insert") {
    sql("""CREATE TABLE tbl (id INT, name STRING, value DOUBLE) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(Map("id" -> 1, "name" -> "alice", "value" -> 10.0)))
    insertOp(w, Seq(Map("id" -> 2, "name" -> "bob", "value" -> 20.0)))
    sql("ALTER TABLE tbl DROP COLUMN value")
    insertOp(w, Seq(Map("id" -> 3, "name" -> "charlie")))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("cm_005_multi_rename", "Multiple RENAME then INSERT",
      "write", "columnMapping", "multipleRename") {
    sql("""CREATE TABLE tbl (id INT, name STRING, value DOUBLE) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(Map("id" -> 1, "name" -> "alice", "value" -> 10.0)))
    sql("ALTER TABLE tbl RENAME COLUMN name TO full_name")
    sql("ALTER TABLE tbl RENAME COLUMN value TO score")
    insertOp(w, Seq(
      Map("id" -> 2, "full_name" -> "bob", "score" -> 20.0),
      Map("id" -> 3, "full_name" -> "charlie", "score" -> 30.0)
    ))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("cm_006_update_after_rename", "UPDATE after column rename",
      "write", "columnMapping", "rename", "update") {
    sql("""CREATE TABLE tbl (id INT, name STRING, value DOUBLE) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "name" -> "alice", "value" -> 10.0),
      Map("id" -> 2, "name" -> "bob", "value" -> 20.0),
      Map("id" -> 3, "name" -> "charlie", "value" -> 30.0)
    ))
    sql("ALTER TABLE tbl RENAME COLUMN name TO full_name")
    updateOp(w, "id = 1", Map("full_name" -> "'alice_updated'"))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("cm_007_delete_after_rename", "DELETE after column rename",
      "write", "columnMapping", "rename", "delete") {
    sql("""CREATE TABLE tbl (id INT, name STRING, value DOUBLE) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "name" -> "alice", "value" -> 10.0),
      Map("id" -> 2, "name" -> "bob", "value" -> 20.0),
      Map("id" -> 3, "name" -> "charlie", "value" -> 30.0)
    ))
    sql("ALTER TABLE tbl RENAME COLUMN name TO full_name")
    deleteOp(w, "full_name = 'bob'")
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("cm_008_merge", "MERGE with column mapping",
      "write", "columnMapping", "merge") {
    sql("""CREATE TABLE tbl (id INT, name STRING, value DOUBLE) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "name" -> "alice", "value" -> 10.0),
      Map("id" -> 2, "name" -> "bob", "value" -> 20.0)
    ))
    sql("CREATE TABLE src (id INT, name STRING, value DOUBLE) USING delta")
    sql("INSERT INTO src VALUES (2, 'bob_updated', 25.0), (3, 'charlie', 30.0)")
    sql("""MERGE INTO tbl t USING src s ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("cm_009_partitioned", "Column mapping on partitioned table",
      "write", "columnMapping", "partitioned") {
    sql("""CREATE TABLE tbl (id INT, name STRING, category STRING) USING delta
      PARTITIONED BY (category)
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "name" -> "alice", "category" -> "A"),
      Map("id" -> 2, "name" -> "bob", "category" -> "B"),
      Map("id" -> 3, "name" -> "charlie", "category" -> "A"),
      Map("id" -> 4, "name" -> "diana", "category" -> "B")
    ))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("cm_010_add_column", "ADD COLUMN with column mapping",
      "write", "columnMapping", "addColumn") {
    sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "name" -> "alice"),
      Map("id" -> 2, "name" -> "bob")
    ))
    sql("ALTER TABLE tbl ADD COLUMNS (age INT)")
    insertOp(w, Seq(Map("id" -> 3, "name" -> "charlie", "age" -> 30)))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("cm_011_schema_evolution_merge", "Column mapping + schema evolution in MERGE",
      "write", "columnMapping", "schemaEvolution") {
    sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "name" -> "alice"),
      Map("id" -> 2, "name" -> "bob")
    ))
    sql("CREATE TABLE src (id INT, name STRING, age INT) USING delta")
    sql("INSERT INTO src VALUES (2, 'bob_updated', 25), (3, 'charlie', 30)")
    spark.conf.set("spark.delta.schema.autoMerge.enabled", "true")
    sql("""MERGE INTO tbl t USING src s ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *""")
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("cm_012_deletion_vectors", "Column mapping + deletion vectors",
      "write", "columnMapping", "deletionVectors") {
    sql("""CREATE TABLE tbl (id INT, name STRING, value DOUBLE) USING delta
      TBLPROPERTIES (
        'delta.columnMapping.mode' = 'name',
        'delta.enableDeletionVectors' = 'true',
        'delta.minReaderVersion' = '3',
        'delta.minWriterVersion' = '7'
      )""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "name" -> "alice", "value" -> 10.0),
      Map("id" -> 2, "name" -> "bob", "value" -> 20.0),
      Map("id" -> 3, "name" -> "charlie", "value" -> 30.0),
      Map("id" -> 4, "name" -> "diana", "value" -> 40.0)
    ))
    deleteOp(w, "id = 2")
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("cm_013_insert_overwrite", "INSERT OVERWRITE with column mapping",
      "write", "columnMapping", "insertOverwrite") {
    sql("""CREATE TABLE tbl (id INT, name STRING, value DOUBLE) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "name" -> "alice", "value" -> 10.0),
      Map("id" -> 2, "name" -> "bob", "value" -> 20.0)
    ))
    sql("INSERT OVERWRITE tbl VALUES (10, 'xavier', 100.0), (11, 'yara', 110.0), (12, 'zack', 120.0)")
    insertOp(w)
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("cm_014_special_chars", "Column mapping with special characters in column names",
      "write", "columnMapping", "specialChars") {
    sql("""CREATE TABLE tbl (
        `first name` STRING, `last name` STRING, `age (years)` INT
      ) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("first name" -> "Alice", "last name" -> "Smith", "age (years)" -> 30),
      Map("first name" -> "Bob", "last name" -> "Jones", "age (years)" -> 25),
      Map("first name" -> "Charlie", "last name" -> "Brown", "age (years)" -> 35)
    ))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("cm_015_enable_on_existing", "Enable column mapping on existing table then write",
      "write", "columnMapping", "enable", "alter") {
    sql("CREATE TABLE tbl (id INT, name STRING, value DOUBLE) USING delta")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "name" -> "alice", "value" -> 10.0),
      Map("id" -> 2, "name" -> "bob", "value" -> 20.0)
    ))
    sql("""ALTER TABLE tbl SET TBLPROPERTIES (
      'delta.columnMapping.mode' = 'name',
      'delta.minReaderVersion' = '2',
      'delta.minWriterVersion' = '5'
    )""")
    insertOp(w, Seq(Map("id" -> 3, "name" -> "charlie", "value" -> 30.0)))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

}

// =============================================================================
// Forward Compatibility
// =============================================================================

new WorkloadSuite("write_forward_compat") {

  test("fc_001_add_then_remove", "Add then remove same file - delete all rows",
      "write", "forwardCompat", "addRemove") {
    sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "name" -> "a"),
      Map("id" -> 2, "name" -> "b"),
      Map("id" -> 3, "name" -> "c")
    ))
    deleteOp(w, "true")
    val t = registerWriteSpec(w)
    read(t, name = "read_after_delete")
    snapshot(t)
  }

  test("fc_002_add_remove_readd", "Add remove re-add - delete all then insert new",
      "write", "forwardCompat", "addRemoveReadd") {
    sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "name" -> "a"),
      Map("id" -> 2, "name" -> "b")
    ))
    deleteOp(w, "true")
    insertOp(w, Seq(
      Map("id" -> 10, "name" -> "x"),
      Map("id" -> 20, "name" -> "y"),
      Map("id" -> 30, "name" -> "z")
    ))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("fc_003_multiple_metadata_changes", "Multiple metadata changes - two ADD COLUMN then INSERT",
      "write", "forwardCompat", "metadataChange", "schemaEvolution") {
    sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(Map("id" -> 1, "name" -> "a")))
    sql("ALTER TABLE tbl ADD COLUMN (score DOUBLE)")
    sql("ALTER TABLE tbl ADD COLUMN (active BOOLEAN)")
    insertOp(w, Seq(Map("id" -> 2, "name" -> "b", "score" -> 9.5, "active" -> true)))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("fc_004_protocol_upgrade_midstream", "Protocol upgrade mid-stream - appendOnly then INSERT",
      "write", "forwardCompat", "protocolUpgrade", "appendOnly") {
    sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "name" -> "a"),
      Map("id" -> 2, "name" -> "b"),
      Map("id" -> 3, "name" -> "c")
    ))
    sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.appendOnly' = 'true')")
    insertOp(w, Seq(
      Map("id" -> 4, "name" -> "d"),
      Map("id" -> 5, "name" -> "e")
    ))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("fc_005_compaction_optimize", "DataChange=false compaction via OPTIMIZE",
      "write", "forwardCompat", "optimize", "compaction") {
    sql("CREATE TABLE tbl (key INT, value STRING) USING delta")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(Map("key" -> 1, "value" -> "val_1"), Map("key" -> 2, "value" -> "val_2")))
    insertOp(w, Seq(Map("key" -> 3, "value" -> "val_3"), Map("key" -> 4, "value" -> "val_4")))
    insertOp(w, Seq(Map("key" -> 5, "value" -> "val_5"), Map("key" -> 6, "value" -> "val_6")))
    insertOp(w, Seq(Map("key" -> 7, "value" -> "val_7"), Map("key" -> 8, "value" -> "val_8")))
    sql("OPTIMIZE tbl")
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("fc_006_stats_missing", "Stats missing from add - writes with stats disabled",
      "write", "forwardCompat", "noStats") {
    spark.conf.set("spark.delta.stats.collect", "false")
    sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "name" -> "a"),
      Map("id" -> 2, "name" -> "b"),
      Map("id" -> 3, "name" -> "c")
    ))
    insertOp(w, Seq(
      Map("id" -> 4, "name" -> "d"),
      Map("id" -> 5, "name" -> "e")
    ))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("fc_007_append_only", "Append-only table - multiple inserts",
      "write", "forwardCompat", "appendOnly") {
    spark.conf.set("spark.delta.properties.defaults.appendOnly", "true")
    sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(Map("id" -> 1, "name" -> "a")))
    insertOp(w, Seq(Map("id" -> 2, "name" -> "b"), Map("id" -> 3, "name" -> "c")))
    insertOp(w, Seq(Map("id" -> 4, "name" -> "d"), Map("id" -> 5, "name" -> "e"), Map("id" -> 6, "name" -> "f")))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("fc_009_empty_commit", "Empty commit - ALTER TABLE SET TBLPROPERTIES (metadata only)",
      "write", "forwardCompat", "emptyCommit", "metadataOnly") {
    sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "name" -> "a"),
      Map("id" -> 2, "name" -> "b")
    ))
    sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.checkpointInterval' = '20')")
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("fc_010_multiple_removes", "Multiple removes in one commit - DELETE across many files",
      "write", "forwardCompat", "multipleRemoves") {
    sql("CREATE TABLE tbl (id INT, label STRING) USING delta")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(Map("id" -> 10, "label" -> "batch_1")))
    insertOp(w, Seq(Map("id" -> 20, "label" -> "batch_2")))
    insertOp(w, Seq(Map("id" -> 30, "label" -> "batch_3")))
    insertOp(w, Seq(Map("id" -> 40, "label" -> "batch_4")))
    insertOp(w, Seq(Map("id" -> 50, "label" -> "batch_5")))
    deleteOp(w, "id <= 30")
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("fc_011_checkpoint_and_read", "Checkpoint and read - force checkpoint after 6 commits",
      "write", "forwardCompat", "checkpoint") {
    spark.conf.set("spark.delta.properties.defaults.checkpointInterval", "5")
    sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(Map("id" -> 1, "name" -> "val_1")))
    insertOp(w, Seq(Map("id" -> 2, "name" -> "val_2")))
    insertOp(w, Seq(Map("id" -> 3, "name" -> "val_3")))
    insertOp(w, Seq(Map("id" -> 4, "name" -> "val_4")))
    insertOp(w, Seq(Map("id" -> 5, "name" -> "val_5")))
    insertOp(w, Seq(Map("id" -> 6, "name" -> "val_6")))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("fc_012_large_schema_50_cols", "Large schema with 50 columns",
      "write", "forwardCompat", "wideSchema") {
    val cols = (1 to 50).map(i => s"col_$i INT").mkString(", ")
    sql(s"CREATE TABLE tbl ($cols) USING delta")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    val row = (1 to 50).map(i => s"col_$i" -> i).toMap
    insertOp(w, Seq(row))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("fc_013_deep_nesting", "Deep nesting - struct of struct of array",
      "write", "forwardCompat", "deepNesting", "struct", "array") {
    sql("""CREATE TABLE tbl (
        id INT,
        info STRUCT<
          personal: STRUCT<name: STRING, tags: ARRAY<STRING>>,
          scores: ARRAY<STRUCT<subject: STRING, grade: DOUBLE>>
        >
      ) USING delta""")
    sql("""INSERT INTO tbl VALUES
      (1, named_struct(
        'personal', named_struct('name', 'Alice', 'tags', array('a', 'b')),
        'scores', array(named_struct('subject', 'math', 'grade', 95.0))
      )),
      (2, named_struct(
        'personal', named_struct('name', 'Bob', 'tags', array('c')),
        'scores', array(
          named_struct('subject', 'math', 'grade', 88.0),
          named_struct('subject', 'science', 'grade', 92.0)
        )
      ))""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w)
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("fc_014_all_primitive_types", "All primitive types in one table",
      "write", "forwardCompat", "primitiveTypes") {
    sql("""CREATE TABLE tbl (
        col_byte TINYINT, col_short SMALLINT, col_int INT, col_long BIGINT,
        col_float FLOAT, col_double DOUBLE, col_decimal DECIMAL(18, 6),
        col_string STRING, col_binary BINARY, col_boolean BOOLEAN,
        col_date DATE, col_timestamp TIMESTAMP
      ) USING delta""")
    sql("""INSERT INTO tbl VALUES
      (1, 100, 1000, 10000, 1.5, 2.5, 123.456789,
       'hello', X'DEADBEEF', true,
       DATE '2024-01-15', TIMESTAMP '2024-01-15 10:30:00'),
      (2, 200, 2000, 20000, 3.5, 4.5, 987.654321,
       'world', X'CAFEBABE', false,
       DATE '2024-06-30', TIMESTAMP '2024-06-30 23:59:59')""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w)
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("fc_015_null_partition_values", "Null partition value handling",
      "write", "forwardCompat", "partition", "nullPartition") {
    sql("""CREATE TABLE tbl (id INT, value STRING, part STRING)
      USING delta PARTITIONED BY (part)""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "value" -> "a", "part" -> "p1"),
      Map("id" -> 2, "value" -> "b", "part" -> "p2"),
      Map("id" -> 3, "value" -> "c", "part" -> null),
      Map("id" -> 4, "value" -> "d", "part" -> "p1"),
      Map("id" -> 5, "value" -> "e", "part" -> null)
    ))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    read(t, predicate = "part IS NULL", name = "read_null_partition")
    snapshot(t)
  }

}

// =============================================================================
// Default Values
// =============================================================================

new WorkloadSuite("write_default_values") {

  test("dv_001_insert_with_default", "INSERT omitting column with DEFAULT",
      "write", "columnDefaults", "insert") {
    sql("""CREATE TABLE tbl (id INT, name STRING, status STRING DEFAULT 'active') USING delta
      TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(Map("id" -> 1, "name" -> "Alice")))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("dv_002_multi_rows_default", "INSERT multiple rows with DEFAULT",
      "write", "columnDefaults", "insert") {
    sql("""CREATE TABLE tbl (id INT, name STRING, status STRING DEFAULT 'active') USING delta
      TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "name" -> "Alice"),
      Map("id" -> 2, "name" -> "Bob"),
      Map("id" -> 3, "name" -> "Carol")
    ))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("dv_003_numeric_default", "DEFAULT with numeric type (INT DEFAULT 0)",
      "write", "columnDefaults", "insert", "numericDefault") {
    sql("""CREATE TABLE tbl (id INT, name STRING, score INT DEFAULT 0) USING delta
      TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(Map("id" -> 1, "name" -> "Alice")))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("dv_004_boolean_default", "DEFAULT with boolean (BOOLEAN DEFAULT true)",
      "write", "columnDefaults", "insert", "booleanDefault") {
    sql("""CREATE TABLE tbl (id INT, name STRING, is_active BOOLEAN DEFAULT true) USING delta
      TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(Map("id" -> 1, "name" -> "Alice")))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("dv_005_literal_default", "DEFAULT with literal expression (INT DEFAULT 2024)",
      "write", "columnDefaults", "insert", "literalDefault") {
    sql("""CREATE TABLE tbl (id INT, name STRING, created_year INT DEFAULT 2024) USING delta
      TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(Map("id" -> 1, "name" -> "Alice")))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("dv_006_multiple_defaults", "Multiple columns with DEFAULTs",
      "write", "columnDefaults", "insert", "multipleDefaults") {
    sql("""CREATE TABLE tbl (
        id INT, name STRING DEFAULT 'unknown',
        status STRING DEFAULT 'pending', priority INT DEFAULT 0
      ) USING delta
      TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(Map("id" -> 1)))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("dv_007_override_default", "INSERT explicit value overrides DEFAULT",
      "write", "columnDefaults", "insert", "overrideDefault") {
    sql("""CREATE TABLE tbl (id INT, name STRING, status STRING DEFAULT 'active') USING delta
      TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(Map("id" -> 1, "name" -> "Alice", "status" -> "inactive")))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("dv_008_merge_with_defaults", "MERGE with DEFAULT columns",
      "write", "columnDefaults", "merge") {
    sql("""CREATE TABLE tbl (id INT, name STRING, status STRING DEFAULT 'active') USING delta
      TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(Map("id" -> 1, "name" -> "Alice", "status" -> "active")))
    sql("CREATE TABLE src (id INT, name STRING) USING delta")
    sql("INSERT INTO src VALUES (1, 'Alice_Updated'), (2, 'Bob'), (3, 'Carol')")
    sql("""MERGE INTO tbl AS target USING src AS source ON target.id = source.id
      WHEN MATCHED THEN UPDATE SET target.name = source.name
      WHEN NOT MATCHED THEN INSERT (id, name) VALUES (source.id, source.name)""")
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("dv_009_add_column_with_default", "ALTER TABLE ADD COLUMN with DEFAULT then INSERT",
      "write", "columnDefaults", "alter", "addColumn") {
    sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
      TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(Map("id" -> 1, "name" -> "Alice")))
    sql("ALTER TABLE tbl ADD COLUMN status STRING")
    sql("ALTER TABLE tbl ALTER COLUMN status SET DEFAULT 'active'")
    insertOp(w, Seq(Map("id" -> 2, "name" -> "Bob")))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("dv_010_alter_set_default", "ALTER TABLE ALTER COLUMN SET DEFAULT then insert",
      "write", "columnDefaults", "alter", "setDefault") {
    sql("""CREATE TABLE tbl (id INT, name STRING, status STRING DEFAULT 'active') USING delta
      TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(Map("id" -> 1, "name" -> "Alice")))
    sql("ALTER TABLE tbl ALTER COLUMN status SET DEFAULT 'pending'")
    insertOp(w, Seq(Map("id" -> 2, "name" -> "Bob")))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("dv_011_drop_default", "ALTER TABLE ALTER COLUMN DROP DEFAULT then insert gets NULL",
      "write", "columnDefaults", "alter", "dropDefault") {
    sql("""CREATE TABLE tbl (id INT, name STRING, status STRING DEFAULT 'active') USING delta
      TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(Map("id" -> 1, "name" -> "Alice")))
    sql("ALTER TABLE tbl ALTER COLUMN status DROP DEFAULT")
    insertOp(w, Seq(Map("id" -> 2, "name" -> "Bob")))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("dv_012_partitioned_default", "DEFAULT on partitioned table",
      "write", "columnDefaults", "insert", "partitioned") {
    sql("""CREATE TABLE tbl (id INT, name STRING, status STRING DEFAULT 'active', region STRING)
      USING delta PARTITIONED BY (region)
      TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(Map("id" -> 1, "name" -> "Alice", "region" -> "us-east")))
    insertOp(w, Seq(Map("id" -> 2, "name" -> "Bob", "region" -> "eu-west")))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("dv_013_update_no_default_trigger", "UPDATE does not trigger defaults",
      "write", "columnDefaults", "update") {
    sql("""CREATE TABLE tbl (id INT, name STRING, status STRING DEFAULT 'active') USING delta
      TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(Map("id" -> 1, "name" -> "Alice")))
    insertOp(w, Seq(Map("id" -> 2, "name" -> "Bob")))
    updateOp(w, "id = 1", Map("name" -> "'Alice_Updated'"))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("dv_014_delete_with_defaults", "DELETE from table with defaults",
      "write", "columnDefaults", "delete") {
    sql("""CREATE TABLE tbl (id INT, name STRING, status STRING DEFAULT 'active') USING delta
      TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(Map("id" -> 1, "name" -> "Alice")))
    insertOp(w, Seq(Map("id" -> 2, "name" -> "Bob")))
    insertOp(w, Seq(Map("id" -> 3, "name" -> "Carol")))
    deleteOp(w, "id = 2")
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

  test("dv_015_schema_evolution_defaults", "INSERT with schema evolution + defaults",
      "write", "columnDefaults", "insert", "schemaEvolution") {
    sql("""CREATE TABLE tbl (id INT, name STRING, status STRING DEFAULT 'active') USING delta
      TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(Map("id" -> 1, "name" -> "Alice")))
    spark.conf.set("spark.delta.schema.autoMerge.enabled", "true")
    sql("CREATE TABLE src (id INT, name STRING, status STRING, score INT) USING delta")
    sql("INSERT INTO src VALUES (2, 'Bob', 'active', 95)")
    sql("INSERT INTO tbl SELECT * FROM src")
    insertOp(w)
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    snapshot(t)
  }

}
