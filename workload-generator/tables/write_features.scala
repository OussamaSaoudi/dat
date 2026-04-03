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
      "write", "dv", "columnMapping", "featureInteraction") { w =>
    w.sql("""CREATE TABLE tbl (id INT, name STRING, value DOUBLE) USING delta
      TBLPROPERTIES (
        'delta.columnMapping.mode' = 'name',
        'delta.enableDeletionVectors' = 'true',
        'delta.minReaderVersion' = '3',
        'delta.minWriterVersion' = '7'
      )""")
    w.sql("INSERT INTO tbl VALUES (1, 'alice', 10.0), (2, 'bob', 20.0), (3, 'charlie', 30.0), (4, 'diana', 40.0), (5, 'eve', 50.0)")
    w.sql("DELETE FROM tbl WHERE id IN (2, 4)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("fi_002_dv_cm_cdc", "DV + Column Mapping + CDC - CDF captures DV deletes",
      "write", "dv", "columnMapping", "cdc", "featureInteraction") { w =>
    w.sql("""CREATE TABLE tbl (id INT, name STRING, value DOUBLE) USING delta
      TBLPROPERTIES (
        'delta.columnMapping.mode' = 'name',
        'delta.enableDeletionVectors' = 'true',
        'delta.enableChangeDataFeed' = 'true',
        'delta.minReaderVersion' = '3',
        'delta.minWriterVersion' = '7'
      )""")
    w.sql("INSERT INTO tbl VALUES (1, 'alice', 10.0), (2, 'bob', 20.0), (3, 'charlie', 30.0)")
    w.sql("DELETE FROM tbl WHERE id = 2")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("fi_003_type_widening_dv", "Type Widening + DV - widen INT to LONG then delete",
      "write", "typeWidening", "dv", "featureInteraction") { w =>
    w.sql("""CREATE TABLE tbl (id INT, value INT) USING delta
      TBLPROPERTIES (
        'delta.enableDeletionVectors' = 'true',
        'delta.enableTypeWidening' = 'true'
      )""")
    w.sql("INSERT INTO tbl VALUES (1, 100), (2, 200), (3, 300)")
    w.sql("ALTER TABLE tbl ALTER COLUMN value TYPE LONG")
    w.sql("INSERT INTO tbl VALUES (4, 4000000000), (5, 5000000000)")
    w.sql("DELETE FROM tbl WHERE id IN (2, 4)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("fi_004_type_widening_cm", "Type Widening + Column Mapping - CM name + INT to LONG",
      "write", "typeWidening", "columnMapping", "featureInteraction") { w =>
    w.sql("""CREATE TABLE tbl (id INT, value INT) USING delta
      TBLPROPERTIES (
        'delta.columnMapping.mode' = 'name',
        'delta.enableTypeWidening' = 'true',
        'delta.minReaderVersion' = '3',
        'delta.minWriterVersion' = '7'
      )""")
    w.sql("INSERT INTO tbl VALUES (1, 100), (2, 200)")
    w.sql("ALTER TABLE tbl ALTER COLUMN value TYPE LONG")
    w.sql("INSERT INTO tbl VALUES (3, 3000000000)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("fi_005_row_tracking_dv", "Row Tracking + DV - insert and delete",
      "write", "rowTracking", "dv", "featureInteraction") { w =>
    w.sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES (
        'delta.enableRowTracking' = 'true',
        'delta.enableDeletionVectors' = 'true'
      )""")
    w.sql("INSERT INTO tbl VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')")
    w.sql("DELETE FROM tbl WHERE id IN (2, 4)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("fi_006_cm_cdc_merge", "CM + CDC + Merge operation",
      "write", "columnMapping", "cdc", "merge", "featureInteraction") { w =>
    w.sql("""CREATE TABLE tbl (id INT, name STRING, value DOUBLE) USING delta
      TBLPROPERTIES (
        'delta.columnMapping.mode' = 'name',
        'delta.enableChangeDataFeed' = 'true',
        'delta.minReaderVersion' = '2',
        'delta.minWriterVersion' = '5'
      )""")
    w.sql("INSERT INTO tbl VALUES (1, 'alice', 10.0), (2, 'bob', 20.0), (3, 'charlie', 30.0)")
    w.sql("""CREATE TABLE src (id INT, name STRING, value DOUBLE) USING delta""")
    w.sql("INSERT INTO src VALUES (2, 'bob_updated', 25.0), (4, 'diana', 40.0)")
    w.sql("""MERGE INTO tbl t
      USING src s ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *""")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("fi_007_dv_partition_delete", "DV + Partition + Delete - partitioned table DV delete",
      "write", "dv", "partition", "delete", "featureInteraction") { w =>
    w.sql("""CREATE TABLE tbl (id INT, value STRING, category STRING) USING delta
      PARTITIONED BY (category)
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("""INSERT INTO tbl VALUES
      (1, 'a', 'X'), (2, 'b', 'X'), (3, 'c', 'X'),
      (4, 'd', 'Y'), (5, 'e', 'Y'), (6, 'f', 'Y')""")
    w.sql("DELETE FROM tbl WHERE category = 'X' AND id = 2")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("fi_008_cm_partition_overwrite", "CM + Partition + Insert Overwrite",
      "write", "columnMapping", "partition", "insertOverwrite", "featureInteraction") { w =>
    w.sql("""CREATE TABLE tbl (id INT, name STRING, category STRING) USING delta
      PARTITIONED BY (category)
      TBLPROPERTIES (
        'delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2',
        'delta.minWriterVersion' = '5'
      )""")
    w.sql("""INSERT INTO tbl VALUES
      (1, 'alice', 'A'), (2, 'bob', 'A'),
      (3, 'charlie', 'B'), (4, 'diana', 'B')""")
    w.conf("spark.sql.sources.partitionOverwriteMode", "dynamic")
    w.sql("INSERT OVERWRITE TABLE tbl VALUES (10, 'xavier', 'A'), (11, 'yara', 'A')")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("fi_009_dv_generated_column", "DV + Generated Column",
      "write", "dv", "generatedColumn", "featureInteraction") { w =>
    w.sql("""CREATE TABLE tbl (
        id INT, value INT,
        doubled INT GENERATED ALWAYS AS (value * 2)
      ) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl (id, value) VALUES (1, 10), (2, 20), (3, 30), (4, 40)")
    w.sql("DELETE FROM tbl WHERE id = 3")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("fi_010_cm_type_widening_merge", "CM + Type Widening + Merge - three features combined",
      "write", "columnMapping", "typeWidening", "merge", "featureInteraction") { w =>
    w.sql("""CREATE TABLE tbl (id INT, value INT) USING delta
      TBLPROPERTIES (
        'delta.columnMapping.mode' = 'name',
        'delta.enableTypeWidening' = 'true',
        'delta.minReaderVersion' = '3',
        'delta.minWriterVersion' = '7'
      )""")
    w.sql("INSERT INTO tbl VALUES (1, 100), (2, 200)")
    w.sql("ALTER TABLE tbl ALTER COLUMN value TYPE LONG")
    w.sql("CREATE TABLE src (id INT, value LONG) USING delta")
    w.sql("INSERT INTO src VALUES (2, 2500000000), (3, 3000000000)")
    w.sql("""MERGE INTO tbl t
      USING src s ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET t.value = s.value
      WHEN NOT MATCHED THEN INSERT *""")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("fi_011_dv_cdc_update", "DV + CDC + Update",
      "write", "dv", "cdc", "update", "featureInteraction") { w =>
    w.sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES (
        'delta.enableDeletionVectors' = 'true',
        'delta.enableChangeDataFeed' = 'true'
      )""")
    w.sql("INSERT INTO tbl VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')")
    w.sql("UPDATE tbl SET value = 'updated' WHERE id IN (1, 3)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("fi_012_row_tracking_cm", "Row Tracking + Column Mapping",
      "write", "rowTracking", "columnMapping", "featureInteraction") { w =>
    w.sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
      TBLPROPERTIES (
        'delta.enableRowTracking' = 'true',
        'delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '3',
        'delta.minWriterVersion' = '7'
      )""")
    w.sql("INSERT INTO tbl VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie')")
    w.sql("INSERT INTO tbl VALUES (4, 'diana'), (5, 'eve')")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("fi_013_timestamp_ntz_cm", "TimestampNTZ + Column Mapping",
      "write", "timestampNtz", "columnMapping", "featureInteraction") { w =>
    w.sql("""CREATE TABLE tbl (
        id INT, event_time TIMESTAMP_NTZ, label STRING
      ) USING delta
      TBLPROPERTIES (
        'delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '3',
        'delta.minWriterVersion' = '7'
      )""")
    w.sql("""INSERT INTO tbl VALUES
      (1, TIMESTAMP_NTZ'2024-01-15 10:30:00', 'first'),
      (2, TIMESTAMP_NTZ'2024-06-20 14:00:00', 'second'),
      (3, TIMESTAMP_NTZ'2024-12-31 23:59:59', 'third')""")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("fi_014_dv_check_constraint", "DV + Check Constraint",
      "write", "dv", "checkConstraint", "featureInteraction") { w =>
    w.sql("""CREATE TABLE tbl (id INT, value INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("ALTER TABLE tbl ADD CONSTRAINT value_positive CHECK (value > 0)")
    w.sql("INSERT INTO tbl VALUES (1, 10), (2, 20), (3, 30), (4, 40)")
    w.sql("DELETE FROM tbl WHERE id IN (1, 3)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("fi_015_all_features", "DV + CM + CDC + Row Tracking - all features combined",
      "write", "dv", "columnMapping", "cdc", "rowTracking", "featureInteraction") { w =>
    w.sql("""CREATE TABLE tbl (id INT, name STRING, value DOUBLE) USING delta
      TBLPROPERTIES (
        'delta.columnMapping.mode' = 'name',
        'delta.enableDeletionVectors' = 'true',
        'delta.enableChangeDataFeed' = 'true',
        'delta.enableRowTracking' = 'true'
      )""")
    w.sql("""INSERT INTO tbl VALUES
      (1, 'alice', 10.0), (2, 'bob', 20.0),
      (3, 'charlie', 30.0), (4, 'diana', 40.0)""")
    w.sql("DELETE FROM tbl WHERE id = 2")
    w.sql("CREATE TABLE src (id INT, name STRING, value DOUBLE) USING delta")
    w.sql("INSERT INTO src VALUES (3, 'charlie_updated', 35.0), (5, 'eve', 50.0)")
    w.sql("""MERGE INTO tbl t
      USING src s ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *""")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

}.runAll()

// =============================================================================
// Protocol Upgrade
// =============================================================================

new WorkloadSuite("write_protocol_upgrade") {

  test("pu_001_enable_dvs", "Enable deletion vectors via SET TBLPROPERTIES",
      "write", "protocolUpgrade", "deletionVectors") { w =>
    w.sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
    w.sql("INSERT INTO tbl VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    w.sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("pu_002_enable_cdc", "Enable change data feed",
      "write", "protocolUpgrade", "cdc") { w =>
    w.sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
    w.sql("INSERT INTO tbl VALUES (1, 'a'), (2, 'b')")
    w.sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("pu_003_enable_row_tracking", "Enable row tracking",
      "write", "protocolUpgrade", "rowTracking") { w =>
    w.sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
    w.sql("INSERT INTO tbl VALUES (1, 'a'), (2, 'b')")
    w.sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.enableRowTracking' = 'true')")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("pu_004_enable_type_widening", "Enable type widening",
      "write", "protocolUpgrade", "typeWidening") { w =>
    w.sql("CREATE TABLE tbl (id INT, value INT) USING delta")
    w.sql("INSERT INTO tbl VALUES (1, 10), (2, 20)")
    w.sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.enableTypeWidening' = 'true')")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("pu_005_enable_column_mapping", "Enable column mapping name mode",
      "write", "protocolUpgrade", "columnMapping") { w =>
    w.sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
    w.sql("INSERT INTO tbl VALUES (1, 'a'), (2, 'b')")
    w.sql("""ALTER TABLE tbl SET TBLPROPERTIES (
      'delta.columnMapping.mode' = 'name',
      'delta.minReaderVersion' = '2',
      'delta.minWriterVersion' = '5'
    )""")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("pu_006_upgrade_reader_version", "Upgrade minReaderVersion to 2",
      "write", "protocolUpgrade", "readerVersion") { w =>
    w.sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
    w.sql("INSERT INTO tbl VALUES (1, 'a'), (2, 'b')")
    w.sql("""ALTER TABLE tbl SET TBLPROPERTIES (
      'delta.minWriterVersion' = '5',
      'delta.minReaderVersion' = '2'
    )""")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("pu_007_upgrade_writer_version", "Upgrade minWriterVersion to 4",
      "write", "protocolUpgrade", "writerVersion") { w =>
    w.sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
    w.sql("INSERT INTO tbl VALUES (1, 'a'), (2, 'b')")
    w.sql("""ALTER TABLE tbl SET TBLPROPERTIES (
      'delta.minReaderVersion' = '1',
      'delta.minWriterVersion' = '4'
    )""")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("pu_008_enable_append_only", "Enable appendOnly",
      "write", "protocolUpgrade", "appendOnly") { w =>
    w.sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
    w.sql("INSERT INTO tbl VALUES (1, 'a'), (2, 'b')")
    w.sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.appendOnly' = 'true')")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("pu_009_enable_multi_features", "Enable DVs and CDC together",
      "write", "protocolUpgrade", "deletionVectors", "cdc") { w =>
    w.sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
    w.sql("INSERT INTO tbl VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    w.sql("""ALTER TABLE tbl SET TBLPROPERTIES (
      'delta.enableDeletionVectors' = 'true',
      'delta.enableChangeDataFeed' = 'true'
    )""")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("pu_010_upgrade_then_insert", "Protocol upgrade then INSERT",
      "write", "protocolUpgrade", "deletionVectors", "insert") { w =>
    w.sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
    w.sql("INSERT INTO tbl VALUES (1, 'a'), (2, 'b')")
    w.sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")
    w.sql("INSERT INTO tbl VALUES (3, 'c'), (4, 'd')")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("pu_011_upgrade_then_merge", "Protocol upgrade then MERGE",
      "write", "protocolUpgrade", "deletionVectors", "merge") { w =>
    w.sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
    w.sql("INSERT INTO tbl VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    w.sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")
    w.sql("CREATE TABLE src (id INT, name STRING) USING delta")
    w.sql("INSERT INTO src VALUES (2, 'updated_b'), (4, 'd')")
    w.sql("""MERGE INTO tbl t USING src s ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET t.name = s.name
      WHEN NOT MATCHED THEN INSERT *""")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("pu_012_enable_check_constraints", "Enable checkConstraints feature",
      "write", "protocolUpgrade", "checkConstraints") { w =>
    w.sql("CREATE TABLE tbl (id INT, value INT) USING delta")
    w.sql("INSERT INTO tbl VALUES (1, 10), (2, 20)")
    w.sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.feature.checkConstraints' = 'supported')")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("pu_013_invariants_not_null", "Enable invariants via NOT NULL constraint",
      "write", "protocolUpgrade", "invariants") { w =>
    w.sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
    w.sql("INSERT INTO tbl VALUES (1, 'a'), (2, 'b')")
    w.sql("ALTER TABLE tbl CHANGE COLUMN name SET NOT NULL")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("pu_014_table_features_protocol", "Upgrade to table features protocol (reader 3, writer 7)",
      "write", "protocolUpgrade", "tableFeatures") { w =>
    w.sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
    w.sql("INSERT INTO tbl VALUES (1, 'a'), (2, 'b')")
    w.sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.minWriterVersion' = '7')")
    w.sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.minReaderVersion' = '3')")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("pu_015_upgrade_partitioned", "Protocol upgrade on partitioned table with data",
      "write", "protocolUpgrade", "deletionVectors", "partitioned") { w =>
    w.sql("""CREATE TABLE tbl (id INT, category STRING, value DOUBLE) USING delta
      PARTITIONED BY (category)""")
    w.sql("INSERT INTO tbl VALUES (1, 'x', 10.0), (2, 'x', 20.0), (3, 'y', 30.0), (4, 'y', 40.0)")
    w.sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

}.runAll()

// =============================================================================
// Type Widening
// =============================================================================

new WorkloadSuite("write_type_widening") {

  test("tw_001_short_to_int", "SHORT to INT widening then INSERT",
      "write", "typeWidening", "shortToInt") { w =>
    w.sql("CREATE TABLE tbl (id INT, value SHORT) USING delta")
    w.sql("INSERT INTO tbl VALUES (1, CAST(10 AS SHORT)), (2, CAST(20 AS SHORT))")
    w.sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.enableTypeWidening' = 'true')")
    w.sql("ALTER TABLE tbl ALTER COLUMN value TYPE INT")
    w.sql("INSERT INTO tbl VALUES (3, 30000), (4, 40000)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("tw_002_int_to_long", "INT to LONG widening then INSERT",
      "write", "typeWidening", "intToLong") { w =>
    w.sql("CREATE TABLE tbl (id INT, value INT) USING delta")
    w.sql("INSERT INTO tbl VALUES (1, 100), (2, 200)")
    w.sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.enableTypeWidening' = 'true')")
    w.sql("ALTER TABLE tbl ALTER COLUMN value TYPE LONG")
    w.sql("INSERT INTO tbl VALUES (3, 3000000000), (4, 4000000000)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("tw_003_float_to_double", "FLOAT to DOUBLE widening then INSERT",
      "write", "typeWidening", "floatToDouble") { w =>
    w.sql("CREATE TABLE tbl (id INT, value FLOAT) USING delta")
    w.sql("INSERT INTO tbl VALUES (1, CAST(1.5 AS FLOAT)), (2, CAST(2.5 AS FLOAT))")
    w.sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.enableTypeWidening' = 'true')")
    w.sql("ALTER TABLE tbl ALTER COLUMN value TYPE DOUBLE")
    w.sql("INSERT INTO tbl VALUES (3, 3.141592653589793), (4, 2.718281828459045)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("tw_004_byte_to_short", "BYTE to SHORT widening then INSERT",
      "write", "typeWidening", "byteToShort") { w =>
    w.sql("CREATE TABLE tbl (id INT, value TINYINT) USING delta")
    w.sql("INSERT INTO tbl VALUES (1, CAST(10 AS TINYINT)), (2, CAST(20 AS TINYINT))")
    w.sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.enableTypeWidening' = 'true')")
    w.sql("ALTER TABLE tbl ALTER COLUMN value TYPE SHORT")
    w.sql("INSERT INTO tbl VALUES (3, 300), (4, 400)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("tw_005_byte_to_int", "BYTE to INT widening (multi-step)",
      "write", "typeWidening", "byteToInt") { w =>
    w.sql("CREATE TABLE tbl (id INT, value TINYINT) USING delta")
    w.sql("INSERT INTO tbl VALUES (1, CAST(5 AS TINYINT)), (2, CAST(10 AS TINYINT))")
    w.sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.enableTypeWidening' = 'true')")
    w.sql("ALTER TABLE tbl ALTER COLUMN value TYPE INT")
    w.sql("INSERT INTO tbl VALUES (3, 50000), (4, 60000)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("tw_006_int_to_double", "INT to DOUBLE widening then INSERT",
      "write", "typeWidening", "intToDouble") { w =>
    w.sql("CREATE TABLE tbl (id INT, value INT) USING delta")
    w.sql("INSERT INTO tbl VALUES (1, 100), (2, 200)")
    w.sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.enableTypeWidening' = 'true')")
    w.sql("ALTER TABLE tbl ALTER COLUMN value TYPE DOUBLE")
    w.sql("INSERT INTO tbl VALUES (3, 3.14), (4, 2.72)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("tw_007_widen_then_merge", "Type widening then MERGE",
      "write", "typeWidening", "merge") { w =>
    w.sql("CREATE TABLE tbl (key INT, value SHORT) USING delta")
    w.sql("INSERT INTO tbl VALUES (1, CAST(10 AS SHORT)), (2, CAST(20 AS SHORT)), (3, CAST(30 AS SHORT))")
    w.sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.enableTypeWidening' = 'true')")
    w.sql("ALTER TABLE tbl ALTER COLUMN value TYPE INT")
    w.sql("CREATE TABLE src (key INT, value INT) USING delta")
    w.sql("INSERT INTO src VALUES (2, 20000), (4, 40000)")
    w.sql("""MERGE INTO tbl AS target USING src AS source ON target.key = source.key
      WHEN MATCHED THEN UPDATE SET target.value = source.value
      WHEN NOT MATCHED THEN INSERT *""")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("tw_008_widen_then_update", "Type widening then UPDATE",
      "write", "typeWidening", "update") { w =>
    w.sql("CREATE TABLE tbl (id INT, value SHORT) USING delta")
    w.sql("INSERT INTO tbl VALUES (1, CAST(10 AS SHORT)), (2, CAST(20 AS SHORT)), (3, CAST(30 AS SHORT))")
    w.sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.enableTypeWidening' = 'true')")
    w.sql("ALTER TABLE tbl ALTER COLUMN value TYPE INT")
    w.sql("UPDATE tbl SET value = 50000 WHERE id = 1")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("tw_009_multi_column", "Multiple columns widened",
      "write", "typeWidening", "multiColumn") { w =>
    w.sql("CREATE TABLE tbl (id INT, col_a SHORT, col_b FLOAT) USING delta")
    w.sql("INSERT INTO tbl VALUES (1, CAST(10 AS SHORT), CAST(1.5 AS FLOAT)), (2, CAST(20 AS SHORT), CAST(2.5 AS FLOAT))")
    w.sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.enableTypeWidening' = 'true')")
    w.sql("ALTER TABLE tbl ALTER COLUMN col_a TYPE INT")
    w.sql("ALTER TABLE tbl ALTER COLUMN col_b TYPE DOUBLE")
    w.sql("INSERT INTO tbl VALUES (3, 50000, 3.141592653589793)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("tw_010_partitioned", "Type widening on partitioned table",
      "write", "typeWidening", "partitioned") { w =>
    w.sql("CREATE TABLE tbl (id INT, part STRING, value SHORT) USING delta PARTITIONED BY (part)")
    w.sql("INSERT INTO tbl VALUES (1, 'a', CAST(10 AS SHORT)), (2, 'b', CAST(20 AS SHORT)), (3, 'a', CAST(30 AS SHORT))")
    w.sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.enableTypeWidening' = 'true')")
    w.sql("ALTER TABLE tbl ALTER COLUMN value TYPE INT")
    w.sql("INSERT INTO tbl VALUES (4, 'a', 40000), (5, 'b', 50000)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("tw_011_preserve_data", "Type widening preserves existing data",
      "write", "typeWidening", "preserveData") { w =>
    w.sql("CREATE TABLE tbl (id INT, value SHORT) USING delta")
    w.sql("INSERT INTO tbl VALUES (1, CAST(10 AS SHORT)), (2, CAST(20 AS SHORT)), (3, CAST(30 AS SHORT))")
    w.sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.enableTypeWidening' = 'true')")
    w.sql("ALTER TABLE tbl ALTER COLUMN value TYPE INT")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("tw_012_short_to_long", "SHORT to LONG direct widening (skip INT)",
      "write", "typeWidening", "shortToLong") { w =>
    w.sql("CREATE TABLE tbl (id INT, value SHORT) USING delta")
    w.sql("INSERT INTO tbl VALUES (1, CAST(10 AS SHORT)), (2, CAST(20 AS SHORT))")
    w.sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.enableTypeWidening' = 'true')")
    w.sql("ALTER TABLE tbl ALTER COLUMN value TYPE LONG")
    w.sql("INSERT INTO tbl VALUES (3, 3000000000), (4, 4000000000)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("tw_013_decimal_widen", "DECIMAL widening precision increase",
      "write", "typeWidening", "decimal") { w =>
    w.sql("CREATE TABLE tbl (id INT, amount DECIMAL(5,2)) USING delta")
    w.sql("INSERT INTO tbl VALUES (1, 123.45), (2, 678.90)")
    w.sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.enableTypeWidening' = 'true')")
    w.sql("ALTER TABLE tbl ALTER COLUMN amount TYPE DECIMAL(10,2)")
    w.sql("INSERT INTO tbl VALUES (3, 12345678.90)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("tw_014_widen_then_delete", "Type widening then DELETE",
      "write", "typeWidening", "delete") { w =>
    w.sql("CREATE TABLE tbl (id INT, value SHORT) USING delta")
    w.sql("INSERT INTO tbl VALUES (1, CAST(10 AS SHORT)), (2, CAST(20 AS SHORT)), (3, CAST(30 AS SHORT)), (4, CAST(40 AS SHORT))")
    w.sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.enableTypeWidening' = 'true')")
    w.sql("ALTER TABLE tbl ALTER COLUMN value TYPE INT")
    w.sql("DELETE FROM tbl WHERE id > 2")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("tw_015_schema_evolve_merge", "Type widening with schema evolution MERGE",
      "write", "typeWidening", "schemaEvolution", "merge") { w =>
    w.sql("CREATE TABLE tbl (key INT, value SHORT) USING delta")
    w.sql("INSERT INTO tbl VALUES (1, CAST(10 AS SHORT)), (2, CAST(20 AS SHORT))")
    w.sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.enableTypeWidening' = 'true')")
    w.sql("ALTER TABLE tbl ALTER COLUMN value TYPE INT")
    w.sql("CREATE TABLE src (key INT, value INT, extra STRING) USING delta")
    w.sql("INSERT INTO src VALUES (2, 20000, 'updated'), (3, 30000, 'new')")
    w.conf("spark.databricks.delta.schema.autoMerge.enabled", "true")
    w.sql("""MERGE INTO tbl AS target USING src AS source ON target.key = source.key
      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *""")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

}.runAll()

// =============================================================================
// Column Mapping
// =============================================================================

new WorkloadSuite("write_column_mapping") {

  test("cm_001_name_mode_insert", "INSERT with column mapping mode = name",
      "write", "columnMapping", "name", "insert") { w =>
    w.sql("""CREATE TABLE tbl (id INT, name STRING, value DOUBLE) USING delta
      TBLPROPERTIES (
        'delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2',
        'delta.minWriterVersion' = '5'
      )""")
    w.sql("INSERT INTO tbl VALUES (1, 'alice', 10.0), (2, 'bob', 20.0), (3, 'charlie', 30.0)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("cm_002_id_mode_insert", "INSERT with column mapping mode = id",
      "write", "columnMapping", "id", "insert") { w =>
    w.sql("""CREATE TABLE tbl (id INT, name STRING, value DOUBLE) USING delta
      TBLPROPERTIES (
        'delta.columnMapping.mode' = 'id',
        'delta.minReaderVersion' = '2',
        'delta.minWriterVersion' = '5'
      )""")
    w.sql("INSERT INTO tbl VALUES (1, 'alice', 10.0), (2, 'bob', 20.0), (3, 'charlie', 30.0)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("cm_003_rename_then_insert", "RENAME COLUMN then INSERT",
      "write", "columnMapping", "rename", "insert") { w =>
    w.sql("""CREATE TABLE tbl (id INT, name STRING, value DOUBLE) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    w.sql("INSERT INTO tbl VALUES (1, 'alice', 10.0)")
    w.sql("INSERT INTO tbl VALUES (2, 'bob', 20.0)")
    w.sql("ALTER TABLE tbl RENAME COLUMN name TO full_name")
    w.sql("INSERT INTO tbl VALUES (3, 'charlie', 30.0)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("cm_004_drop_then_insert", "DROP COLUMN then INSERT",
      "write", "columnMapping", "drop", "insert") { w =>
    w.sql("""CREATE TABLE tbl (id INT, name STRING, value DOUBLE) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    w.sql("INSERT INTO tbl VALUES (1, 'alice', 10.0)")
    w.sql("INSERT INTO tbl VALUES (2, 'bob', 20.0)")
    w.sql("ALTER TABLE tbl DROP COLUMN value")
    w.sql("INSERT INTO tbl VALUES (3, 'charlie')")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("cm_005_multi_rename", "Multiple RENAME then INSERT",
      "write", "columnMapping", "multipleRename") { w =>
    w.sql("""CREATE TABLE tbl (id INT, name STRING, value DOUBLE) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    w.sql("INSERT INTO tbl VALUES (1, 'alice', 10.0)")
    w.sql("ALTER TABLE tbl RENAME COLUMN name TO full_name")
    w.sql("ALTER TABLE tbl RENAME COLUMN value TO score")
    w.sql("INSERT INTO tbl VALUES (2, 'bob', 20.0), (3, 'charlie', 30.0)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("cm_006_update_after_rename", "UPDATE after column rename",
      "write", "columnMapping", "rename", "update") { w =>
    w.sql("""CREATE TABLE tbl (id INT, name STRING, value DOUBLE) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    w.sql("INSERT INTO tbl VALUES (1, 'alice', 10.0), (2, 'bob', 20.0), (3, 'charlie', 30.0)")
    w.sql("ALTER TABLE tbl RENAME COLUMN name TO full_name")
    w.sql("UPDATE tbl SET full_name = 'alice_updated' WHERE id = 1")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("cm_007_delete_after_rename", "DELETE after column rename",
      "write", "columnMapping", "rename", "delete") { w =>
    w.sql("""CREATE TABLE tbl (id INT, name STRING, value DOUBLE) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    w.sql("INSERT INTO tbl VALUES (1, 'alice', 10.0), (2, 'bob', 20.0), (3, 'charlie', 30.0)")
    w.sql("ALTER TABLE tbl RENAME COLUMN name TO full_name")
    w.sql("DELETE FROM tbl WHERE full_name = 'bob'")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("cm_008_merge", "MERGE with column mapping",
      "write", "columnMapping", "merge") { w =>
    w.sql("""CREATE TABLE tbl (id INT, name STRING, value DOUBLE) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    w.sql("INSERT INTO tbl VALUES (1, 'alice', 10.0), (2, 'bob', 20.0)")
    w.sql("CREATE TABLE src (id INT, name STRING, value DOUBLE) USING delta")
    w.sql("INSERT INTO src VALUES (2, 'bob_updated', 25.0), (3, 'charlie', 30.0)")
    w.sql("""MERGE INTO tbl t USING src s ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *""")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("cm_009_partitioned", "Column mapping on partitioned table",
      "write", "columnMapping", "partitioned") { w =>
    w.sql("""CREATE TABLE tbl (id INT, name STRING, category STRING) USING delta
      PARTITIONED BY (category)
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    w.sql("INSERT INTO tbl VALUES (1, 'alice', 'A'), (2, 'bob', 'B'), (3, 'charlie', 'A'), (4, 'diana', 'B')")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("cm_010_add_column", "ADD COLUMN with column mapping",
      "write", "columnMapping", "addColumn") { w =>
    w.sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    w.sql("INSERT INTO tbl VALUES (1, 'alice'), (2, 'bob')")
    w.sql("ALTER TABLE tbl ADD COLUMNS (age INT)")
    w.sql("INSERT INTO tbl VALUES (3, 'charlie', 30)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("cm_011_schema_evolution_merge", "Column mapping + schema evolution in MERGE",
      "write", "columnMapping", "schemaEvolution") { w =>
    w.sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    w.sql("INSERT INTO tbl VALUES (1, 'alice'), (2, 'bob')")
    w.sql("CREATE TABLE src (id INT, name STRING, age INT) USING delta")
    w.sql("INSERT INTO src VALUES (2, 'bob_updated', 25), (3, 'charlie', 30)")
    w.conf("spark.databricks.delta.schema.autoMerge.enabled", "true")
    w.sql("""MERGE INTO tbl t USING src s ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *""")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("cm_012_deletion_vectors", "Column mapping + deletion vectors",
      "write", "columnMapping", "deletionVectors") { w =>
    w.sql("""CREATE TABLE tbl (id INT, name STRING, value DOUBLE) USING delta
      TBLPROPERTIES (
        'delta.columnMapping.mode' = 'name',
        'delta.enableDeletionVectors' = 'true',
        'delta.minReaderVersion' = '3',
        'delta.minWriterVersion' = '7'
      )""")
    w.sql("INSERT INTO tbl VALUES (1, 'alice', 10.0), (2, 'bob', 20.0), (3, 'charlie', 30.0), (4, 'diana', 40.0)")
    w.sql("DELETE FROM tbl WHERE id = 2")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("cm_013_insert_overwrite", "INSERT OVERWRITE with column mapping",
      "write", "columnMapping", "insertOverwrite") { w =>
    w.sql("""CREATE TABLE tbl (id INT, name STRING, value DOUBLE) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    w.sql("INSERT INTO tbl VALUES (1, 'alice', 10.0), (2, 'bob', 20.0)")
    w.sql("INSERT OVERWRITE tbl VALUES (10, 'xavier', 100.0), (11, 'yara', 110.0), (12, 'zack', 120.0)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("cm_014_special_chars", "Column mapping with special characters in column names",
      "write", "columnMapping", "specialChars") { w =>
    w.sql("""CREATE TABLE tbl (
        `first name` STRING, `last name` STRING, `age (years)` INT
      ) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')""")
    w.sql("INSERT INTO tbl VALUES ('Alice', 'Smith', 30), ('Bob', 'Jones', 25), ('Charlie', 'Brown', 35)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("cm_015_enable_on_existing", "Enable column mapping on existing table then write",
      "write", "columnMapping", "enable", "alter") { w =>
    w.sql("CREATE TABLE tbl (id INT, name STRING, value DOUBLE) USING delta")
    w.sql("INSERT INTO tbl VALUES (1, 'alice', 10.0), (2, 'bob', 20.0)")
    w.sql("""ALTER TABLE tbl SET TBLPROPERTIES (
      'delta.columnMapping.mode' = 'name',
      'delta.minReaderVersion' = '2',
      'delta.minWriterVersion' = '5'
    )""")
    w.sql("INSERT INTO tbl VALUES (3, 'charlie', 30.0)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

}.runAll()

// =============================================================================
// Forward Compatibility
// =============================================================================

new WorkloadSuite("write_forward_compat") {

  test("fc_001_add_then_remove", "Add then remove same file - delete all rows",
      "write", "forwardCompat", "addRemove") { w =>
    w.sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
    w.sql("INSERT INTO tbl VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    w.sql("DELETE FROM tbl")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_after_delete")
    w.snapshot(t)
  }

  test("fc_002_add_remove_readd", "Add remove re-add - delete all then insert new",
      "write", "forwardCompat", "addRemoveReadd") { w =>
    w.sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
    w.sql("INSERT INTO tbl VALUES (1, 'a'), (2, 'b')")
    w.sql("DELETE FROM tbl")
    w.sql("INSERT INTO tbl VALUES (10, 'x'), (20, 'y'), (30, 'z')")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("fc_003_multiple_metadata_changes", "Multiple metadata changes - two ADD COLUMN then INSERT",
      "write", "forwardCompat", "metadataChange", "schemaEvolution") { w =>
    w.sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
    w.sql("INSERT INTO tbl VALUES (1, 'a')")
    w.sql("ALTER TABLE tbl ADD COLUMN (score DOUBLE)")
    w.sql("ALTER TABLE tbl ADD COLUMN (active BOOLEAN)")
    w.sql("INSERT INTO tbl VALUES (2, 'b', 9.5, true)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("fc_004_protocol_upgrade_midstream", "Protocol upgrade mid-stream - appendOnly then INSERT",
      "write", "forwardCompat", "protocolUpgrade", "appendOnly") { w =>
    w.sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
    w.sql("INSERT INTO tbl VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    w.sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.appendOnly' = 'true')")
    w.sql("INSERT INTO tbl VALUES (4, 'd'), (5, 'e')")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("fc_005_compaction_optimize", "DataChange=false compaction via OPTIMIZE",
      "write", "forwardCompat", "optimize", "compaction") { w =>
    w.sql("CREATE TABLE tbl (key INT, value STRING) USING delta")
    w.sql("INSERT INTO tbl VALUES (1, 'val_1'), (2, 'val_2')")
    w.sql("INSERT INTO tbl VALUES (3, 'val_3'), (4, 'val_4')")
    w.sql("INSERT INTO tbl VALUES (5, 'val_5'), (6, 'val_6')")
    w.sql("INSERT INTO tbl VALUES (7, 'val_7'), (8, 'val_8')")
    w.sql("OPTIMIZE tbl")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("fc_006_stats_missing", "Stats missing from add - writes with stats disabled",
      "write", "forwardCompat", "noStats") { w =>
    w.conf("spark.databricks.delta.stats.collect", "false")
    w.sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
    w.sql("INSERT INTO tbl VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    w.sql("INSERT INTO tbl VALUES (4, 'd'), (5, 'e')")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("fc_007_append_only", "Append-only table - multiple inserts",
      "write", "forwardCompat", "appendOnly") { w =>
    w.conf("spark.databricks.delta.properties.defaults.appendOnly", "true")
    w.sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
    w.sql("INSERT INTO tbl VALUES (1, 'a')")
    w.sql("INSERT INTO tbl VALUES (2, 'b'), (3, 'c')")
    w.sql("INSERT INTO tbl VALUES (4, 'd'), (5, 'e'), (6, 'f')")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("fc_009_empty_commit", "Empty commit - ALTER TABLE SET TBLPROPERTIES (metadata only)",
      "write", "forwardCompat", "emptyCommit", "metadataOnly") { w =>
    w.sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
    w.sql("INSERT INTO tbl VALUES (1, 'a'), (2, 'b')")
    w.sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.checkpointInterval' = '20')")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("fc_010_multiple_removes", "Multiple removes in one commit - DELETE across many files",
      "write", "forwardCompat", "multipleRemoves") { w =>
    w.sql("CREATE TABLE tbl (id INT, label STRING) USING delta")
    w.sql("INSERT INTO tbl VALUES (10, 'batch_1')")
    w.sql("INSERT INTO tbl VALUES (20, 'batch_2')")
    w.sql("INSERT INTO tbl VALUES (30, 'batch_3')")
    w.sql("INSERT INTO tbl VALUES (40, 'batch_4')")
    w.sql("INSERT INTO tbl VALUES (50, 'batch_5')")
    w.sql("DELETE FROM tbl WHERE id <= 30")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("fc_011_checkpoint_and_read", "Checkpoint and read - force checkpoint after 6 commits",
      "write", "forwardCompat", "checkpoint") { w =>
    w.conf("spark.databricks.delta.properties.defaults.checkpointInterval", "5")
    w.sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
    w.sql("INSERT INTO tbl VALUES (1, 'val_1')")
    w.sql("INSERT INTO tbl VALUES (2, 'val_2')")
    w.sql("INSERT INTO tbl VALUES (3, 'val_3')")
    w.sql("INSERT INTO tbl VALUES (4, 'val_4')")
    w.sql("INSERT INTO tbl VALUES (5, 'val_5')")
    w.sql("INSERT INTO tbl VALUES (6, 'val_6')")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("fc_012_large_schema_50_cols", "Large schema with 50 columns",
      "write", "forwardCompat", "wideSchema") { w =>
    val cols = (1 to 50).map(i => s"col_$i INT").mkString(", ")
    w.sql(s"CREATE TABLE tbl ($cols) USING delta")
    val values = (1 to 50).mkString(", ")
    w.sql(s"INSERT INTO tbl VALUES ($values)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("fc_013_deep_nesting", "Deep nesting - struct of struct of array",
      "write", "forwardCompat", "deepNesting", "struct", "array") { w =>
    w.sql("""CREATE TABLE tbl (
        id INT,
        info STRUCT<
          personal: STRUCT<name: STRING, tags: ARRAY<STRING>>,
          scores: ARRAY<STRUCT<subject: STRING, grade: DOUBLE>>
        >
      ) USING delta""")
    w.sql("""INSERT INTO tbl VALUES
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
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("fc_014_all_primitive_types", "All primitive types in one table",
      "write", "forwardCompat", "primitiveTypes") { w =>
    w.sql("""CREATE TABLE tbl (
        col_byte TINYINT, col_short SMALLINT, col_int INT, col_long BIGINT,
        col_float FLOAT, col_double DOUBLE, col_decimal DECIMAL(18, 6),
        col_string STRING, col_binary BINARY, col_boolean BOOLEAN,
        col_date DATE, col_timestamp TIMESTAMP
      ) USING delta""")
    w.sql("""INSERT INTO tbl VALUES
      (1, 100, 1000, 10000, 1.5, 2.5, 123.456789,
       'hello', X'DEADBEEF', true,
       DATE '2024-01-15', TIMESTAMP '2024-01-15 10:30:00'),
      (2, 200, 2000, 20000, 3.5, 4.5, 987.654321,
       'world', X'CAFEBABE', false,
       DATE '2024-06-30', TIMESTAMP '2024-06-30 23:59:59')""")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("fc_015_null_partition_values", "Null partition value handling",
      "write", "forwardCompat", "partition", "nullPartition") { w =>
    w.sql("""CREATE TABLE tbl (id INT, value STRING, part STRING)
      USING delta PARTITIONED BY (part)""")
    w.sql("""INSERT INTO tbl VALUES
      (1, 'a', 'p1'), (2, 'b', 'p2'), (3, 'c', null),
      (4, 'd', 'p1'), (5, 'e', null)""")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, predicate = "part IS NULL", name = "read_null_partition")
    w.snapshot(t)
  }

}.runAll()

// =============================================================================
// Default Values
// =============================================================================

new WorkloadSuite("write_default_values") {

  test("dv_001_insert_with_default", "INSERT omitting column with DEFAULT",
      "write", "columnDefaults", "insert") { w =>
    w.sql("""CREATE TABLE tbl (id INT, name STRING, status STRING DEFAULT 'active') USING delta
      TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')""")
    w.sql("INSERT INTO tbl (id, name) VALUES (1, 'Alice')")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("dv_002_multi_rows_default", "INSERT multiple rows with DEFAULT",
      "write", "columnDefaults", "insert") { w =>
    w.sql("""CREATE TABLE tbl (id INT, name STRING, status STRING DEFAULT 'active') USING delta
      TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')""")
    w.sql("INSERT INTO tbl (id, name) VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("dv_003_numeric_default", "DEFAULT with numeric type (INT DEFAULT 0)",
      "write", "columnDefaults", "insert", "numericDefault") { w =>
    w.sql("""CREATE TABLE tbl (id INT, name STRING, score INT DEFAULT 0) USING delta
      TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')""")
    w.sql("INSERT INTO tbl (id, name) VALUES (1, 'Alice')")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("dv_004_boolean_default", "DEFAULT with boolean (BOOLEAN DEFAULT true)",
      "write", "columnDefaults", "insert", "booleanDefault") { w =>
    w.sql("""CREATE TABLE tbl (id INT, name STRING, is_active BOOLEAN DEFAULT true) USING delta
      TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')""")
    w.sql("INSERT INTO tbl (id, name) VALUES (1, 'Alice')")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("dv_005_literal_default", "DEFAULT with literal expression (INT DEFAULT 2024)",
      "write", "columnDefaults", "insert", "literalDefault") { w =>
    w.sql("""CREATE TABLE tbl (id INT, name STRING, created_year INT DEFAULT 2024) USING delta
      TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')""")
    w.sql("INSERT INTO tbl (id, name) VALUES (1, 'Alice')")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("dv_006_multiple_defaults", "Multiple columns with DEFAULTs",
      "write", "columnDefaults", "insert", "multipleDefaults") { w =>
    w.sql("""CREATE TABLE tbl (
        id INT, name STRING DEFAULT 'unknown',
        status STRING DEFAULT 'pending', priority INT DEFAULT 0
      ) USING delta
      TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')""")
    w.sql("INSERT INTO tbl (id) VALUES (1)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("dv_007_override_default", "INSERT explicit value overrides DEFAULT",
      "write", "columnDefaults", "insert", "overrideDefault") { w =>
    w.sql("""CREATE TABLE tbl (id INT, name STRING, status STRING DEFAULT 'active') USING delta
      TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')""")
    w.sql("INSERT INTO tbl (id, name, status) VALUES (1, 'Alice', 'inactive')")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("dv_008_merge_with_defaults", "MERGE with DEFAULT columns",
      "write", "columnDefaults", "merge") { w =>
    w.sql("""CREATE TABLE tbl (id INT, name STRING, status STRING DEFAULT 'active') USING delta
      TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')""")
    w.sql("INSERT INTO tbl VALUES (1, 'Alice', 'active')")
    w.sql("CREATE TABLE src (id INT, name STRING) USING delta")
    w.sql("INSERT INTO src VALUES (1, 'Alice_Updated'), (2, 'Bob'), (3, 'Carol')")
    w.sql("""MERGE INTO tbl AS target USING src AS source ON target.id = source.id
      WHEN MATCHED THEN UPDATE SET target.name = source.name
      WHEN NOT MATCHED THEN INSERT (id, name) VALUES (source.id, source.name)""")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("dv_009_add_column_with_default", "ALTER TABLE ADD COLUMN with DEFAULT then INSERT",
      "write", "columnDefaults", "alter", "addColumn") { w =>
    w.sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
      TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')""")
    w.sql("INSERT INTO tbl VALUES (1, 'Alice')")
    w.sql("ALTER TABLE tbl ADD COLUMN status STRING")
    w.sql("ALTER TABLE tbl ALTER COLUMN status SET DEFAULT 'active'")
    w.sql("INSERT INTO tbl (id, name) VALUES (2, 'Bob')")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("dv_010_alter_set_default", "ALTER TABLE ALTER COLUMN SET DEFAULT then insert",
      "write", "columnDefaults", "alter", "setDefault") { w =>
    w.sql("""CREATE TABLE tbl (id INT, name STRING, status STRING DEFAULT 'active') USING delta
      TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')""")
    w.sql("INSERT INTO tbl (id, name) VALUES (1, 'Alice')")
    w.sql("ALTER TABLE tbl ALTER COLUMN status SET DEFAULT 'pending'")
    w.sql("INSERT INTO tbl (id, name) VALUES (2, 'Bob')")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("dv_011_drop_default", "ALTER TABLE ALTER COLUMN DROP DEFAULT then insert gets NULL",
      "write", "columnDefaults", "alter", "dropDefault") { w =>
    w.sql("""CREATE TABLE tbl (id INT, name STRING, status STRING DEFAULT 'active') USING delta
      TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')""")
    w.sql("INSERT INTO tbl (id, name) VALUES (1, 'Alice')")
    w.sql("ALTER TABLE tbl ALTER COLUMN status DROP DEFAULT")
    w.sql("INSERT INTO tbl (id, name) VALUES (2, 'Bob')")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("dv_012_partitioned_default", "DEFAULT on partitioned table",
      "write", "columnDefaults", "insert", "partitioned") { w =>
    w.sql("""CREATE TABLE tbl (id INT, name STRING, status STRING DEFAULT 'active', region STRING)
      USING delta PARTITIONED BY (region)
      TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')""")
    w.sql("INSERT INTO tbl (id, name, region) VALUES (1, 'Alice', 'us-east')")
    w.sql("INSERT INTO tbl (id, name, region) VALUES (2, 'Bob', 'eu-west')")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("dv_013_update_no_default_trigger", "UPDATE does not trigger defaults",
      "write", "columnDefaults", "update") { w =>
    w.sql("""CREATE TABLE tbl (id INT, name STRING, status STRING DEFAULT 'active') USING delta
      TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')""")
    w.sql("INSERT INTO tbl (id, name) VALUES (1, 'Alice')")
    w.sql("INSERT INTO tbl (id, name) VALUES (2, 'Bob')")
    w.sql("UPDATE tbl SET name = 'Alice_Updated' WHERE id = 1")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("dv_014_delete_with_defaults", "DELETE from table with defaults",
      "write", "columnDefaults", "delete") { w =>
    w.sql("""CREATE TABLE tbl (id INT, name STRING, status STRING DEFAULT 'active') USING delta
      TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')""")
    w.sql("INSERT INTO tbl (id, name) VALUES (1, 'Alice')")
    w.sql("INSERT INTO tbl (id, name) VALUES (2, 'Bob')")
    w.sql("INSERT INTO tbl (id, name) VALUES (3, 'Carol')")
    w.sql("DELETE FROM tbl WHERE id = 2")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("dv_015_schema_evolution_defaults", "INSERT with schema evolution + defaults",
      "write", "columnDefaults", "insert", "schemaEvolution") { w =>
    w.sql("""CREATE TABLE tbl (id INT, name STRING, status STRING DEFAULT 'active') USING delta
      TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')""")
    w.sql("INSERT INTO tbl (id, name) VALUES (1, 'Alice')")
    w.conf("spark.databricks.delta.schema.autoMerge.enabled", "true")
    w.sql("CREATE TABLE src (id INT, name STRING, status STRING, score INT) USING delta")
    w.sql("INSERT INTO src VALUES (2, 'Bob', 'active', 95)")
    w.sql("INSERT INTO tbl SELECT * FROM src")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

}.runAll()
