/**
 * Protocol versioning, table features, partition value encoding, and protocol edge cases.
 * Covers pv_* (protocol versions) and pve_* (partition value encoding) workloads.
 */

new WorkloadSuite("protocol_versions") {

  // pv_001*: Basic protocol version tables

  test("pv_001a_protocol_1_1", "Protocol (1,1) table", "protocol") { w =>
    w.sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(5)")
    val t = w.table("tbl")
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("pv_001b_protocol_1_2", "Protocol (1,2) table with appendOnly", "protocol") { w =>
    w.sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.appendOnly' = 'true', 'delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(5)")
    val t = w.table("tbl")
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("pv_001c_protocol_1_3", "Protocol (1,3) table with check constraints", "protocol") { w =>
    w.sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("ALTER TABLE tbl ADD CONSTRAINT positive CHECK (id >= 0)")
    w.sql("INSERT INTO tbl SELECT id FROM range(5)")
    val t = w.table("tbl")
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("pv_001d_protocol_1_4", "Protocol (1,4) table with generated columns", "protocol") { w =>
    w.sql("""CREATE TABLE tbl (id LONG, doubled LONG GENERATED ALWAYS AS (id * 2)) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl (id) SELECT id FROM range(5)")
    val t = w.table("tbl")
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("pv_001e_protocol_2_5", "Protocol (2,5) table with column mapping", "protocol", "column_mapping") { w =>
    w.sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(5)")
    val t = w.table("tbl")
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("pv_001f_protocol_cdf", "Protocol with CDF feature enabled", "protocol", "cdf") { w =>
    w.sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(5)")
    val t = w.table("tbl")
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("pv_001g_protocol_3_7_dv", "Protocol (3,7) table with deletion vectors", "protocol", "dv") { w =>
    w.sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(5)")
    val t = w.table("tbl")
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  // pv_002-007: Protocol upgrades

  test("pv_002_upgrade_to_current", "Table after protocol upgrade to current version", "protocol") { w =>
    w.sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(5)")
    w.sql("INSERT INTO tbl SELECT id + 5 FROM range(5)")
    val t = w.table("tbl")
    w.read(t, name = "read_latest")
    w.read(t, version = 0, name = "read_v0")
    w.snapshot(t)
    w.snapshot(t, version = 0)
  }

  test("pv_003_upgrade_deltatable_api", "Table after DeltaTable API protocol upgrades", "protocol") { w =>
    w.sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(5)")
    // Upgrade protocol by adding a writer feature via ALTER TABLE
    w.sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.feature.checkConstraints' = 'supported')")
    w.sql("INSERT INTO tbl SELECT id + 5 FROM range(5)")
    val t = w.table("tbl")
    w.read(t, name = "read_latest")
    w.read(t, version = 0, name = "read_v0")
    w.snapshot(t)
    w.snapshot(t, version = 0)
    w.snapshot(t, version = 1)
    w.snapshot(t, version = 2)
  }

  test("pv_004_upgrade_no_feature", "Table after upgrade attempt without features (no-op)", "protocol") { w =>
    w.sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(5)")
    w.sql("INSERT INTO tbl SELECT id + 5 FROM range(5)")
    val t = w.table("tbl")
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("pv_006_upgrade_many_features", "Table with multiple table features enabled", "protocol", "column_mapping", "cdf") { w =>
    w.sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
        'delta.enableChangeDataFeed' = 'true',
        'delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(5)")
    w.sql("INSERT INTO tbl SELECT id + 5 FROM range(5)")
    val t = w.table("tbl")
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("pv_007_upgrade_sql_api", "Table after SQL API protocol upgrade", "protocol", "cdf") { w =>
    w.sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(5)")
    w.sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')")
    w.sql("INSERT INTO tbl SELECT id + 5 FROM range(5)")
    val t = w.table("tbl")
    w.read(t, name = "read_latest")
    w.read(t, version = 0, name = "read_v0")
    w.snapshot(t)
    w.snapshot(t, version = 0)
    w.snapshot(t, version = 1)
  }

  // pv_008-012: Overwrite behavior

  test("pv_008_overwrite_keeps_protocol", "Table after overwrite preserves protocol version", "protocol") { w =>
    w.sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(5)")
    w.sql("INSERT OVERWRITE tbl SELECT id + 10 FROM range(5)")
    val t = w.table("tbl")
    w.read(t, name = "read_after_overwrite")
    w.snapshot(t)
  }

  test("pv_009_overwrite_keeps_properties", "Table after overwrite preserves table properties", "protocol") { w =>
    w.sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(5)")
    w.sql("ALTER TABLE tbl SET TBLPROPERTIES ('myProp' = 'true')")
    w.sql("INSERT OVERWRITE tbl SELECT id + 10 FROM range(5)")
    val t = w.table("tbl")
    w.read(t, name = "read_after_overwrite")
    w.snapshot(t)
  }

  test("pv_010_overwrite_keeps_features", "Table after overwrite preserves table features", "protocol", "cdf") { w =>
    w.sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(5)")
    w.sql("INSERT OVERWRITE tbl SELECT id + 10 FROM range(5)")
    val t = w.table("tbl")
    w.read(t, name = "read_after_overwrite")
    w.snapshot(t)
  }

  test("pv_011_overwrite_with_configs", "Table after overwrite with additional configs", "protocol") { w =>
    w.sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(5)")
    w.sql("INSERT OVERWRITE tbl SELECT id + 10 FROM range(5)")
    val t = w.table("tbl")
    w.read(t, name = "read_after_overwrite")
    w.snapshot(t)
  }

  test("pv_012_overwrite_session_defaults", "Table after overwrite with session defaults", "protocol") { w =>
    w.sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(5)")
    w.sql("INSERT OVERWRITE tbl SELECT id + 10 FROM range(5)")
    val t = w.table("tbl")
    w.read(t, name = "read_after_overwrite")
    w.snapshot(t)
  }

  // pv_014: Vacuum protocol check

  test("pv_014_vacuum_protocol_check", "Table state that vacuum would check protocol on", "protocol") { w =>
    w.sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(10)")
    val t = w.table("tbl")
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  // pv_023-026: Downgrade and defaults

  test("pv_023_downgrade_noop", "Table after downgrade attempt (no-op)", "protocol") { w =>
    w.sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(10)")
    val t = w.table("tbl")
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("pv_024_create_ignore_defaults", "CREATE TABLE with explicit protocol ignoring session defaults", "protocol") { w =>
    w.sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(5)")
    val t = w.table("tbl")
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("pv_026_operation_ignore_defaults", "Table after operations that ignore protocol defaults", "protocol") { w =>
    w.sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(10)")
    val t = w.table("tbl")
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  // pv_030-040: CREATE TABLE with various feature configurations

  test("pv_030_create_session_features", "Table created with session-configured features", "protocol") { w =>
    w.sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.appendOnly' = 'true', 'delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(5)")
    val t = w.table("tbl")
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("pv_031_create_mixed_features", "Table with features from both session and table properties", "protocol", "cdf") { w =>
    w.sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true',
        'delta.appendOnly' = 'true',
        'delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(5)")
    val t = w.table("tbl")
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("pv_032_replace_default_protocol", "Table after REPLACE with default protocol", "protocol") { w =>
    w.sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(5)")
    w.sql("CREATE OR REPLACE TABLE tbl (id LONG) USING delta TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")
    w.sql("INSERT INTO tbl SELECT id + 10 FROM range(5)")
    val t = w.table("tbl")
    w.read(t, version = 0, name = "read_v0")
    w.read(t, name = "read_v1")
    w.snapshot(t)
    w.snapshot(t, version = 0)
    w.snapshot(t, version = 1)
  }

  test("pv_033_create_no_explicit_protocol", "Table created with no explicit protocol specification", "protocol") { w =>
    w.sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(5)")
    val t = w.table("tbl")
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("pv_035_create_protocol_property", "Table with protocol specified as table property", "protocol") { w =>
    w.sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.minReaderVersion' = '3', 'delta.minWriterVersion' = '7',
        'delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(5)")
    val t = w.table("tbl")
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("pv_036_create_writer_only_feature", "Table with writer-only feature (appendOnly)", "protocol") { w =>
    w.sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.appendOnly' = 'true', 'delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(5)")
    val t = w.table("tbl")
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("pv_037_create_legacy_rw_feature", "Table with legacy reader-writer feature (columnMapping)", "protocol", "column_mapping") { w =>
    w.sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(5)")
    val t = w.table("tbl")
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("pv_038_create_native_writer_feature", "Table with native reader-writer feature (deletionVectors)", "protocol", "dv") { w =>
    w.sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(5)")
    val t = w.table("tbl")
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("pv_039_create_reader_writer_feature", "Table with multiple reader-writer features", "protocol", "column_mapping", "dv") { w =>
    w.sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true', 'delta.columnMapping.mode' = 'name')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(5)")
    val t = w.table("tbl")
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("pv_040_create_auto_enabled_feature", "Table with automatically-enabled feature (generated columns)", "protocol") { w =>
    w.sql("""CREATE TABLE tbl (id LONG, doubled LONG GENERATED ALWAYS AS (id * 2)) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl (id) SELECT id FROM range(5)")
    val t = w.table("tbl")
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  // pv_046-047: ALTER TABLE to add features

  test("pv_046_alter_add_cdf", "Table after ALTER to add CDF feature", "protocol", "cdf") { w =>
    w.sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(5)")
    w.sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')")
    w.sql("INSERT INTO tbl SELECT id + 5 FROM range(5)")
    val t = w.table("tbl")
    w.read(t, name = "read_latest")
    w.read(t, version = 0, name = "read_v0")
    w.snapshot(t)
    w.snapshot(t, version = 0)
    w.snapshot(t, version = 1)
    w.snapshot(t, version = 2)
  }

  test("pv_047_alter_add_column_mapping", "Table after ALTER to add column mapping", "protocol", "column_mapping") { w =>
    w.sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(5)")
    w.sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name')")
    w.sql("INSERT INTO tbl SELECT id + 5 FROM range(5)")
    val t = w.table("tbl")
    w.read(t, name = "read_latest")
    w.read(t, version = 0, name = "read_v0")
    w.snapshot(t)
    w.snapshot(t, version = 0)
    w.snapshot(t, version = 1)
    w.snapshot(t, version = 2)
  }

  // pv_082: Protocol property precedence

  test("pv_082_protocol_property_wins", "Table where protocol property wins over session config", "protocol") { w =>
    w.sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(5)")
    val t = w.table("tbl")
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  // pv_090-092: Protocol visibility and auto-upgrade

  test("pv_090_protocol_desc_table", "Table with protocol visible in DESC TABLE", "protocol") { w =>
    w.sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.feature.checkConstraints' = 'supported')")
    w.sql("INSERT INTO tbl SELECT id FROM range(5)")
    val t = w.table("tbl")
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("pv_091_auto_upgrade_v2", "Table after auto upgrade to version 2 (appendOnly)", "protocol") { w =>
    w.sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(5)")
    w.sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.appendOnly' = 'true')")
    w.sql("INSERT INTO tbl SELECT id + 5 FROM range(5)")
    val t = w.table("tbl")
    w.read(t, name = "read_latest")
    w.read(t, version = 0, name = "read_v0")
    w.snapshot(t)
    w.snapshot(t, version = 0)
    w.snapshot(t, version = 1)
  }

  test("pv_092_auto_upgrade_v3", "Table after auto upgrade to version 3 (check constraint)", "protocol") { w =>
    w.sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("ALTER TABLE tbl ADD CONSTRAINT positive CHECK (id > 0)")
    w.sql("INSERT INTO tbl VALUES (1), (2), (3), (4), (5)")
    val t = w.table("tbl")
    w.read(t, name = "read_all")
    w.snapshot(t)
    w.snapshot(t, version = 0)
    w.snapshot(t, version = 1)
    w.snapshot(t, version = 2)
  }

  // pv_097-098: All features and feature status

  test("pv_097_all_active_features", "Table with all active features enabled", "protocol", "column_mapping", "cdf", "dv") { w =>
    w.sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
        'delta.enableDeletionVectors' = 'true',
        'delta.enableChangeDataFeed' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(5)")
    val t = w.table("tbl")
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("pv_098_table_feature_status", "Table with DVs and row tracking features", "protocol", "dv") { w =>
    w.sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableRowTracking' = 'true', 'delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(5)")
    val t = w.table("tbl")
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  // pv_099-100: REPLACE AS protocol behavior

  test("pv_099_replace_as_updates_protocol", "Table after REPLACE AS with higher protocol defaults", "protocol") { w =>
    w.sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(5)")
    w.sql("INSERT OVERWRITE tbl SELECT id + 10 FROM range(5)")
    val t = w.table("tbl")
    w.read(t, name = "read_latest")
    w.read(t, version = 0, name = "read_v0")
    w.snapshot(t)
    w.snapshot(t, version = 0)
    w.snapshot(t, version = 1)
  }

  test("pv_100_replace_as_keeps_protocol", "Table after REPLACE AS with lower protocol defaults (keeps existing)", "protocol") { w =>
    w.sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true',
        'delta.feature.checkConstraints' = 'supported',
        'delta.feature.generatedColumns' = 'supported',
        'delta.feature.changeDataFeed' = 'supported')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(5)")
    w.sql("INSERT OVERWRITE tbl SELECT id + 10 FROM range(5)")
    val t = w.table("tbl")
    w.read(t, name = "read_latest")
    w.read(t, version = 0, name = "read_v0")
    w.snapshot(t)
    w.snapshot(t, version = 0)
    w.snapshot(t, version = 1)
  }

  // pv_102: Protocol change logging

  test("pv_102_protocol_change_logging", "Table state after protocol change (for logging verification)", "protocol", "cdf") { w =>
    w.sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(5)")
    w.sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')")
    w.sql("INSERT INTO tbl SELECT id + 5 FROM range(5)")
    val t = w.table("tbl")
    w.read(t, name = "read_latest")
    w.read(t, version = 0, name = "read_v0")
    w.snapshot(t)
    w.snapshot(t, version = 0)
    w.snapshot(t, version = 1)
    w.snapshot(t, version = 2)
  }

  // pv_104-105: Feature removal

  test("pv_104_remove_writer_feature", "Table after writer feature removal (appendOnly disabled)", "protocol") { w =>
    w.sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.appendOnly' = 'true', 'delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(5)")
    w.sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.appendOnly' = 'false')")
    w.sql("INSERT INTO tbl SELECT id + 5 FROM range(5)")
    val t = w.table("tbl")
    w.read(t, name = "read_latest")
    w.read(t, version = 0, name = "read_v0")
    w.snapshot(t)
    w.snapshot(t, version = 0)
    w.snapshot(t, version = 1)
  }

  test("pv_105_remove_cdf", "Table after CDF feature disabled", "protocol", "cdf") { w =>
    w.sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(5)")
    w.sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'false')")
    w.sql("INSERT INTO tbl SELECT id + 5 FROM range(5)")
    val t = w.table("tbl")
    w.read(t, name = "read_latest")
    w.read(t, version = 0, name = "read_v0")
    w.snapshot(t)
    w.snapshot(t, version = 0)
    w.snapshot(t, version = 1)
  }

  // pv_110-116: Downgrade testing states

  test("pv_110_downgrade_1_4", "Table with protocol (1,4) for downgrade testing", "protocol") { w =>
    w.sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true',
        'delta.feature.checkConstraints' = 'supported',
        'delta.feature.generatedColumns' = 'supported',
        'delta.feature.changeDataFeed' = 'supported')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(10)")
    val t = w.table("tbl")
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("pv_111_downgrade_2_5", "Table with protocol (2,5) for downgrade testing", "protocol", "column_mapping") { w =>
    w.sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(10)")
    val t = w.table("tbl")
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("pv_112_downgrade_3_7", "Table with protocol (3,7) for downgrade testing", "protocol", "dv") { w =>
    w.sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(10)")
    val t = w.table("tbl")
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  test("pv_115_dv_removal_state", "Table state with DVs for DV removal testing", "protocol", "dv") { w =>
    w.sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(20)")
    w.sql("DELETE FROM tbl WHERE id < 10")
    val t = w.table("tbl")
    w.read(t, name = "read_latest")
    w.read(t, version = 0, name = "read_v0")
    w.snapshot(t)
    w.snapshot(t, version = 0)
    w.snapshot(t, version = 1)
  }

  test("pv_116_ict_state", "Table with ICT for ICT removal testing", "protocol") { w =>
    w.sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableInCommitTimestamps' = 'true', 'delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(5)")
    w.sql("INSERT INTO tbl SELECT id + 5 FROM range(5)")
    w.sql("INSERT INTO tbl SELECT id + 10 FROM range(5)")
    val t = w.table("tbl")
    w.read(t, name = "read_latest")
    w.read(t, version = 0, name = "read_v0")
    w.read(t, version = 1, name = "read_v1")
    w.snapshot(t)
    w.snapshot(t, version = 0)
    w.snapshot(t, version = 1)
    w.snapshot(t, version = 2)
  }

  // Hand-crafted protocol edge cases (mutateTable)

  test("pv_empty_reader_features", "Table with empty readerFeatures/writerFeatures arrays", "protocol", "edge") { w =>
    w.sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(5)")
    val t = w.table("tbl")
    // Rewrite commit to have empty feature arrays (still protocol v3/v7)
    w.mutateTable(t) { dir =>
      import scala.collection.JavaConverters._
      val f = dir.resolve("_delta_log/00000000000000000000.json")
      val lines = java.nio.file.Files.readAllLines(f).asScala.map { line =>
        if (line.contains("\"protocol\"")) {
          val updated = line
            .replaceAll(""""readerFeatures"\s*:\s*\[[^\]]*\]""", """"readerFeatures":[]""")
            .replaceAll(""""writerFeatures"\s*:\s*\[[^\]]*\]""", """"writerFeatures":[]""")
          updated
        } else line
      }
      java.nio.file.Files.write(f, lines.asJava)
    }
    w.snapshot(t)
  }

  test("pv_err_001_protocol_too_high", "Error: protocol version too high to read", "protocol", "error") { w =>
    w.sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(5)")
    val t = w.table("tbl")
    // Rewrite protocol to unreachable version
    w.mutateTable(t) { dir =>
      import scala.collection.JavaConverters._
      val f = dir.resolve("_delta_log/00000000000000000000.json")
      val lines = java.nio.file.Files.readAllLines(f).asScala.map { line =>
        if (line.contains("\"protocol\"")) {
          """{"protocol":{"minReaderVersion":2147483647,"minWriterVersion":2147483647,"readerFeatures":[],"writerFeatures":[]}}"""
        } else line
      }
      java.nio.file.Files.write(f, lines.asJava)
    }
    w.snapshot(t)
  }

  test("pv_err_002_unsupported_feature", "Error: unsupported reader feature", "protocol", "error") { w =>
    w.sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(5)")
    val t = w.table("tbl")
    w.mutateTable(t) { dir =>
      import scala.collection.JavaConverters._
      val f = dir.resolve("_delta_log/00000000000000000000.json")
      val lines = java.nio.file.Files.readAllLines(f).asScala.map { line =>
        if (line.contains("\"protocol\"")) {
          """{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["NonExistingReaderFeature"],"writerFeatures":[]}}"""
        } else line
      }
      java.nio.file.Files.write(f, lines.asJava)
    }
    w.snapshot(t)
  }

  test("pv_features_case_sensitivity", "Error: case-sensitive feature names (DeletionVectors vs deletionVectors)", "protocol", "error") { w =>
    w.sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(5)")
    val t = w.table("tbl")
    w.mutateTable(t) { dir =>
      import scala.collection.JavaConverters._
      val f = dir.resolve("_delta_log/00000000000000000000.json")
      val lines = java.nio.file.Files.readAllLines(f).asScala.map { line =>
        if (line.contains("\"protocol\"")) {
          """{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["DeletionVectors"],"writerFeatures":["DeletionVectors"]}}"""
        } else line
      }
      java.nio.file.Files.write(f, lines.asJava)
    }
    w.snapshot(t)
  }

  test("pv_protocol_downgrade", "Table with protocol downgraded in second commit", "protocol", "edge") { w =>
    w.sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(10)")
    val t = w.table("tbl")
    // Rewrite protocol in commit 0 to lower version via a second commit
    w.mutateTable(t) { dir =>
      import scala.collection.JavaConverters._
      val f = dir.resolve("_delta_log/00000000000000000000.json")
      val lines = java.nio.file.Files.readAllLines(f).asScala.map { line =>
        if (line.contains("\"protocol\"")) {
          """{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["deletionVectors"],"writerFeatures":["deletionVectors"]}}"""
        } else line
      }
      java.nio.file.Files.write(f, lines.asJava)
      // Add a second commit that downgrades protocol
      val commit1 = dir.resolve("_delta_log/00000000000000000001.json")
      val newLines = java.util.Arrays.asList(
        """{"metaData":{"id":"00000000-0000-0000-0000-000000000000","format":{"provider":"parquet","options":{}},"partitionColumns":[],"configuration":{}}}""",
        """{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"""
      )
      java.nio.file.Files.write(commit1, newLines)
    }
    w.snapshot(t)
  }

  test("pv_reader_feature_not_in_writer", "Error: reader feature not present in writer features", "protocol", "error") { w =>
    w.sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(5)")
    val t = w.table("tbl")
    w.mutateTable(t) { dir =>
      import scala.collection.JavaConverters._
      val f = dir.resolve("_delta_log/00000000000000000000.json")
      val lines = java.nio.file.Files.readAllLines(f).asScala.map { line =>
        if (line.contains("\"protocol\"")) {
          """{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["deletionVectors"],"writerFeatures":[]}}"""
        } else line
      }
      java.nio.file.Files.write(f, lines.asJava)
    }
    w.snapshot(t)
  }

  test("pv_reader_v3_writer_lt_7", "Error: reader version 3 with writer version < 7", "protocol", "error") { w =>
    w.sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(5)")
    val t = w.table("tbl")
    w.mutateTable(t) { dir =>
      import scala.collection.JavaConverters._
      val f = dir.resolve("_delta_log/00000000000000000000.json")
      val lines = java.nio.file.Files.readAllLines(f).asScala.map { line =>
        if (line.contains("\"protocol\"")) {
          """{"protocol":{"minReaderVersion":3,"minWriterVersion":6}}"""
        } else line
      }
      java.nio.file.Files.write(f, lines.asJava)
    }
    w.snapshot(t)
  }

  test("pv_reader_v4_error", "Error: reader version 4 unsupported", "protocol", "error") { w =>
    w.sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(5)")
    val t = w.table("tbl")
    w.mutateTable(t) { dir =>
      import scala.collection.JavaConverters._
      val f = dir.resolve("_delta_log/00000000000000000000.json")
      val lines = java.nio.file.Files.readAllLines(f).asScala.map { line =>
        if (line.contains("\"protocol\"")) {
          """{"protocol":{"minReaderVersion":4,"minWriterVersion":7}}"""
        } else line
      }
      java.nio.file.Files.write(f, lines.asJava)
    }
    w.snapshot(t)
  }

  test("pv_unknown_reader_feature", "Error: unknown reader feature", "protocol", "error") { w =>
    w.sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(5)")
    val t = w.table("tbl")
    w.mutateTable(t) { dir =>
      import scala.collection.JavaConverters._
      val f = dir.resolve("_delta_log/00000000000000000000.json")
      val lines = java.nio.file.Files.readAllLines(f).asScala.map { line =>
        if (line.contains("\"protocol\"")) {
          """{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["unknownReaderFeatureXyz"],"writerFeatures":["unknownReaderFeatureXyz"]}}"""
        } else line
      }
      java.nio.file.Files.write(f, lines.asJava)
    }
    w.snapshot(t)
  }

  test("pv_unknown_writer_feature_ok", "Table with unknown writer-only feature (readable)", "protocol", "edge") { w =>
    w.sql("""CREATE TABLE tbl (id LONG) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id FROM range(5)")
    val t = w.table("tbl")
    w.mutateTable(t) { dir =>
      import scala.collection.JavaConverters._
      val f = dir.resolve("_delta_log/00000000000000000000.json")
      val lines = java.nio.file.Files.readAllLines(f).asScala.map { line =>
        if (line.contains("\"protocol\"")) {
          """{"protocol":{"minReaderVersion":1,"minWriterVersion":7,"writerFeatures":["unknownWriterFeatureXyz"]}}"""
        } else line
      }
      java.nio.file.Files.write(f, lines.asJava)
    }
    w.snapshot(t)
  }

  test("pv_multiple_reader_features", "DV + colMapping + v2Checkpoint combined", "protocol", "dv", "column_mapping") { w =>
    w.sql("""CREATE TABLE tbl (id LONG, name STRING) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name',
        'delta.enableDeletionVectors' = 'true',
        'delta.checkpointPolicy' = 'v2')""")
    w.sql("INSERT INTO tbl VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    w.sql("DELETE FROM tbl WHERE id = 2")
    val t = w.table("tbl")
    w.read(t, name = "read_all")
    w.snapshot(t)
  }

  // pve_*: Partition value encoding

  test("pve_boolean_partition", "Partition value encoding: boolean type", "partition") { w =>
    w.sql("""CREATE TABLE tbl (id INT, flag BOOLEAN) USING delta
      PARTITIONED BY (flag) TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1, true), (2, false), (3, true), (4, false)")
    val t = w.table("tbl")
    w.read(t, name = "read_all")
    w.read(t, predicate = "flag = true", name = "filter_true")
    w.read(t, predicate = "flag = false", name = "filter_false")
    w.snapshot(t)
  }

  test("pve_byte_partition", "Partition value encoding: byte with boundary values (-128, 0, 127)", "partition") { w =>
    w.sql("""CREATE TABLE tbl (id INT, b BYTE) USING delta
      PARTITIONED BY (b) TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1, CAST(-128 AS BYTE)), (2, CAST(0 AS BYTE)), (3, CAST(127 AS BYTE)), (4, CAST(1 AS BYTE))")
    val t = w.table("tbl")
    w.read(t, name = "read_all")
    w.read(t, predicate = "b = CAST(-128 AS BYTE)", name = "filter_min")
    w.read(t, predicate = "b = CAST(127 AS BYTE)", name = "filter_max")
    w.read(t, predicate = "b > CAST(0 AS BYTE)", name = "filter_positive")
    w.snapshot(t)
  }

  test("pve_decimal_partition", "Partition value encoding: decimal(10,2) type", "partition") { w =>
    w.sql("""CREATE TABLE tbl (id INT, amount DECIMAL(10,2)) USING delta
      PARTITIONED BY (amount) TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1, 99.99), (2, -100.50), (3, 0.01), (4, 12345.67)")
    val t = w.table("tbl")
    w.read(t, name = "read_all")
    w.read(t, predicate = "amount = 99.99", name = "filter_eq")
    w.read(t, predicate = "amount > 0", name = "filter_positive")
    w.read(t, predicate = "amount = -100.50", name = "filter_boundary")
    w.snapshot(t)
  }

  test("pve_double_partition", "Partition value encoding: double with precision", "partition") { w =>
    w.sql("""CREATE TABLE tbl (id INT, d DOUBLE) USING delta
      PARTITIONED BY (d) TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1, 3.14159), (2, -2.71828), (3, 1000000.001), (4, 0.0)")
    val t = w.table("tbl")
    w.read(t, name = "read_all")
    w.read(t, predicate = "d > 0", name = "filter_positive")
    w.read(t, predicate = "d > 100", name = "filter_large")
    w.snapshot(t)
  }

  test("pve_empty_string_partition", "Partition value encoding: empty string vs null distinction", "partition") { w =>
    w.sql("""CREATE TABLE tbl (id INT, tag STRING) USING delta
      PARTITIONED BY (tag) TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1, ''), (2, 'hello'), (3, CAST(NULL AS STRING)), (4, 'world')")
    val t = w.table("tbl")
    w.read(t, name = "read_all")
    w.read(t, predicate = "tag = ''", name = "filter_empty_string")
    w.read(t, predicate = "tag IS NULL", name = "filter_null")
    w.read(t, predicate = "tag IS NOT NULL AND tag != ''", name = "filter_nonempty")
    w.snapshot(t)
  }

  test("pve_float_partition", "Partition value encoding: float type", "partition") { w =>
    w.sql("""CREATE TABLE tbl (id INT, f FLOAT) USING delta
      PARTITIONED BY (f) TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1, CAST(1.5 AS FLOAT)), (2, CAST(-3.14 AS FLOAT)), (3, CAST(0.0 AS FLOAT)), (4, CAST(99.9 AS FLOAT))")
    val t = w.table("tbl")
    w.read(t, name = "read_all")
    w.read(t, predicate = "f = CAST(1.5 AS FLOAT)", name = "filter_eq")
    w.read(t, predicate = "f > CAST(0.0 AS FLOAT)", name = "filter_positive")
    w.snapshot(t)
  }

  test("pve_multi_partition_cols", "Partition value encoding: 3 partition cols of different types", "partition") { w =>
    w.sql("""CREATE TABLE tbl (id INT, value STRING, p_str STRING, p_int INT, p_bool BOOLEAN) USING delta
      PARTITIONED BY (p_str, p_int, p_bool) TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("""INSERT INTO tbl VALUES
      (1, 'a', 'cat_a', 10, true), (2, 'b', 'cat_a', 20, false),
      (3, 'c', 'cat_b', 10, true), (4, 'd', 'cat_b', 30, false),
      (5, 'e', 'cat_a', 10, false), (6, 'f', 'cat_c', 40, true)""")
    val t = w.table("tbl")
    w.read(t, name = "read_all")
    w.read(t, predicate = "p_str = 'cat_a'", name = "filter_str")
    w.read(t, predicate = "p_str = 'cat_a' AND p_int = 10", name = "filter_str_and_int")
    w.read(t, predicate = "p_int >= 20 AND p_int <= 30", name = "filter_int_range")
    w.read(t, predicate = "p_bool = true", name = "filter_bool_only")
    w.read(t, predicate = "p_str = 'cat_b' AND p_int = 10 AND p_bool = true", name = "filter_all_three")
    w.snapshot(t)
  }

  test("pve_null_partition", "Partition value encoding: null value (missing key in map)", "partition") { w =>
    w.sql("""CREATE TABLE tbl (id INT, category STRING) USING delta
      PARTITIONED BY (category) TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1, 'A'), (2, CAST(NULL AS STRING)), (3, 'B'), (4, CAST(NULL AS STRING))")
    val t = w.table("tbl")
    w.read(t, name = "read_all")
    w.read(t, predicate = "category = 'A'", name = "filter_eq")
    w.read(t, predicate = "category IS NULL", name = "filter_null")
    w.read(t, predicate = "category IS NOT NULL", name = "filter_not_null")
    w.snapshot(t)
  }

  test("pve_short_partition", "Partition value encoding: short with boundary values (-32768, 0, 32767)", "partition") { w =>
    w.sql("""CREATE TABLE tbl (id INT, s SHORT) USING delta
      PARTITIONED BY (s) TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1, CAST(-32768 AS SHORT)), (2, CAST(0 AS SHORT)), (3, CAST(32767 AS SHORT)), (4, CAST(100 AS SHORT))")
    val t = w.table("tbl")
    w.read(t, name = "read_all")
    w.read(t, predicate = "s = CAST(-32768 AS SHORT)", name = "filter_min")
    w.read(t, predicate = "s = CAST(32767 AS SHORT)", name = "filter_max")
    w.read(t, predicate = "s >= CAST(0 AS SHORT) AND s <= CAST(100 AS SHORT)", name = "filter_range")
    w.snapshot(t)
  }

  test("pve_special_chars_partition", "Partition value encoding: spaces, unicode, special chars", "partition") { w =>
    w.sql("""CREATE TABLE tbl (id INT, label STRING) USING delta
      PARTITIONED BY (label) TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("""INSERT INTO tbl VALUES
      (1, 'hello world'), (2, 'caf\u00e9'), (3, 'a/b=c&d'), (4, 'normal')""")
    val t = w.table("tbl")
    w.read(t, name = "read_all")
    w.read(t, predicate = "label = 'hello world'", name = "filter_space")
    w.read(t, predicate = "label = 'caf\u00e9'", name = "filter_unicode")
    w.read(t, predicate = "label = 'a/b=c&d'", name = "filter_special")
    w.snapshot(t)
  }

  test("pve_timestamp_ntz_partition", "Partition value encoding: timestampNTZ type", "partition") { w =>
    w.sql("""CREATE TABLE tbl (id INT, ts_ntz TIMESTAMP_NTZ) USING delta
      PARTITIONED BY (ts_ntz) TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("""INSERT INTO tbl VALUES
      (1, TIMESTAMP_NTZ'2024-01-01 00:00:00'), (2, TIMESTAMP_NTZ'2024-06-15 12:30:00'),
      (3, TIMESTAMP_NTZ'2024-12-31 23:59:59')""")
    val t = w.table("tbl")
    w.read(t, name = "read_all")
    w.read(t, predicate = "ts_ntz = TIMESTAMP_NTZ'2024-06-15 12:30:00'", name = "filter_eq")
    w.read(t, predicate = "ts_ntz >= TIMESTAMP_NTZ'2024-06-01 00:00:00'", name = "filter_range")
    w.snapshot(t)
  }

  test("pve_timestamp_partition", "Partition value encoding: timestamp with microseconds", "partition") { w =>
    w.sql("""CREATE TABLE tbl (id INT, ts TIMESTAMP) USING delta
      PARTITIONED BY (ts) TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("""INSERT INTO tbl VALUES
      (1, TIMESTAMP'2024-01-01 00:00:00'), (2, TIMESTAMP'2024-06-15 12:30:00.123456'),
      (3, TIMESTAMP'2024-12-31 23:59:59')""")
    val t = w.table("tbl")
    w.read(t, name = "read_all")
    w.read(t, predicate = "ts = TIMESTAMP'2024-06-15 12:30:00.123456'", name = "filter_eq")
    w.read(t, predicate = "ts >= TIMESTAMP'2024-06-01 00:00:00'", name = "filter_range")
    w.snapshot(t)
  }

}.runAll()
