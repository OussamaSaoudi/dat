/**
 * Write workloads for partitioned tables and data skipping verification.
 *
 * Converted from:
 *   - PartitionWriteCapture.scala (PT-001 through PT-015)
 *   - DataSkippingWriteCapture.scala (DSK-001 through DSK-015)
 *
 * Each test produces a write_spec.json (commit history) plus read verification specs
 * with predicates that exercise partition pruning and/or file-level data skipping.
 */

new WorkloadSuite("write_partitioned") {

  // =========================================================================
  // Partition Write Capture (PT-001 through PT-015)
  // =========================================================================

  // PT-001: Single string partition column, INSERT into 3 partitions
  test("pt001_single_string_partition",
      "Single string partition column with 3 regions",
      "write", "partitioned", "insert") { w =>
    w.sql("""CREATE TABLE tbl (id INT, data STRING, region STRING)
      USING delta PARTITIONED BY (region)""")
    w.sql("INSERT INTO tbl VALUES (1, 'a1', 'US'), (2, 'a2', 'US'), (3, 'a3', 'US')")
    w.sql("INSERT INTO tbl VALUES (4, 'b1', 'EU'), (5, 'b2', 'EU')")
    w.sql("INSERT INTO tbl VALUES (6, 'c1', 'APAC'), (7, 'c2', 'APAC'), (8, 'c3', 'APAC'), (9, 'c4', 'APAC')")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, predicate = "region = 'US'", name = "read_us")
    w.read(t, predicate = "region = 'EU'", name = "read_eu")
    w.read(t, predicate = "region = 'APAC'", name = "read_apac")
    w.snapshot(t)
  }

  // PT-002: Single int partition column
  test("pt002_single_int_partition",
      "Single int partition column with 3 categories",
      "write", "partitioned", "insert") { w =>
    w.sql("""CREATE TABLE tbl (data STRING, category INT)
      USING delta PARTITIONED BY (category)""")
    w.sql("INSERT INTO tbl VALUES ('a', 1), ('b', 1), ('c', 1)")
    w.sql("INSERT INTO tbl VALUES ('d', 2), ('e', 2)")
    w.sql("INSERT INTO tbl VALUES ('f', 3), ('g', 3), ('h', 3), ('i', 3)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, predicate = "category = 1", name = "read_cat_1")
    w.read(t, predicate = "category = 2", name = "read_cat_2")
    w.read(t, predicate = "category >= 2", name = "read_cat_ge_2")
    w.snapshot(t)
  }

  // PT-003: Two partition columns (year INT, month INT)
  test("pt003_two_partition_columns",
      "Two partition columns year and month",
      "write", "partitioned", "insert") { w =>
    w.sql("""CREATE TABLE tbl (id INT, data STRING, year INT, month INT)
      USING delta PARTITIONED BY (year, month)""")
    w.sql("INSERT INTO tbl VALUES (1, 'jan23', 2023, 1), (2, 'feb23', 2023, 2), (3, 'mar23', 2023, 3)")
    w.sql("INSERT INTO tbl VALUES (4, 'jan24', 2024, 1), (5, 'feb24', 2024, 2), (6, 'mar24', 2024, 3)")
    w.sql("INSERT INTO tbl VALUES (7, 'apr24', 2024, 4), (8, 'may24', 2024, 5)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, predicate = "year = 2023", name = "read_2023")
    w.read(t, predicate = "year = 2024", name = "read_2024")
    w.read(t, predicate = "year = 2024 AND month = 1", name = "read_2024_jan")
    w.read(t, predicate = "month <= 2", name = "read_month_le_2")
    w.snapshot(t)
  }

  // PT-004: Partition + non-partition columns with combined predicate
  test("pt004_partition_and_data_predicates",
      "Partition and data column predicates combined",
      "write", "partitioned", "insert", "dataSkipping") { w =>
    w.sql("""CREATE TABLE tbl (id INT, value DOUBLE, status STRING)
      USING delta PARTITIONED BY (status)""")
    w.sql("INSERT INTO tbl VALUES (1, 10.0, 'active'), (2, 20.0, 'active'), (3, 30.0, 'active')")
    w.sql("INSERT INTO tbl VALUES (4, 40.0, 'inactive'), (5, 50.0, 'inactive')")
    w.sql("INSERT INTO tbl VALUES (6, 60.0, 'pending'), (7, 70.0, 'pending'), (8, 80.0, 'pending')")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, predicate = "status = 'active'", name = "read_active")
    w.read(t, predicate = "status = 'active' AND value > 15.0", name = "read_active_gt_15")
    w.read(t, predicate = "status != 'inactive'", name = "read_not_inactive")
    w.snapshot(t)
  }

  // PT-005: INSERT OVERWRITE single partition
  test("pt005_insert_overwrite_partition",
      "INSERT OVERWRITE replaces single partition",
      "write", "partitioned", "insert") { w =>
    w.sql("""CREATE TABLE tbl (id INT, data STRING, part STRING)
      USING delta PARTITIONED BY (part)""")
    w.sql("INSERT INTO tbl VALUES (1, 'old_a', 'A'), (2, 'old_a2', 'A'), (3, 'old_b', 'B')")
    w.sql("INSERT OVERWRITE TABLE tbl PARTITION (part = 'A') VALUES (10, 'new_a'), (11, 'new_a2'), (12, 'new_a3')")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, predicate = "part = 'A'", name = "read_part_A")
    w.read(t, predicate = "part = 'B'", name = "read_part_B")
    w.snapshot(t)
  }

  // PT-006: INSERT OVERWRITE with dynamic partitioning
  test("pt006_insert_overwrite_dynamic",
      "INSERT OVERWRITE with dynamic partition mode",
      "write", "partitioned", "insert") { w =>
    w.sql("""CREATE TABLE tbl (id INT, data STRING, part STRING)
      USING delta PARTITIONED BY (part)""")
    w.sql("INSERT INTO tbl VALUES (1, 'old_a', 'A'), (2, 'old_b', 'B'), (3, 'old_c', 'C')")
    w.spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    w.sql("INSERT OVERWRITE TABLE tbl VALUES (10, 'new_a', 'A'), (11, 'new_b', 'B')")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, predicate = "part = 'A'", name = "read_part_A")
    w.read(t, predicate = "part = 'B'", name = "read_part_B")
    w.read(t, predicate = "part = 'C'", name = "read_part_C")
    w.snapshot(t)
  }

  // PT-007: DELETE with partition predicate
  test("pt007_delete_partition",
      "DELETE entire partition by predicate",
      "write", "partitioned", "delete") { w =>
    w.sql("""CREATE TABLE tbl (id INT, data STRING, region STRING)
      USING delta PARTITIONED BY (region)""")
    w.sql("INSERT INTO tbl VALUES (1, 'a', 'US'), (2, 'b', 'US'), (3, 'c', 'EU'), (4, 'd', 'EU'), (5, 'e', 'APAC')")
    w.sql("DELETE FROM tbl WHERE region = 'EU'")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, predicate = "region = 'US'", name = "read_us")
    w.read(t, predicate = "region = 'EU'", name = "read_eu")
    w.read(t, predicate = "region = 'APAC'", name = "read_apac")
    w.snapshot(t)
  }

  // PT-008: UPDATE with partition predicate
  test("pt008_update_partition",
      "UPDATE rows within a single partition",
      "write", "partitioned", "update") { w =>
    w.sql("""CREATE TABLE tbl (id INT, data STRING, region STRING)
      USING delta PARTITIONED BY (region)""")
    w.sql("INSERT INTO tbl VALUES (1, 'old', 'US'), (2, 'old', 'US'), (3, 'old', 'EU'), (4, 'old', 'EU'), (5, 'old', 'APAC')")
    w.sql("UPDATE tbl SET data = 'new' WHERE region = 'US'")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, predicate = "region = 'US'", name = "read_us")
    w.read(t, predicate = "data = 'new'", name = "read_updated")
    w.read(t, predicate = "region = 'EU'", name = "read_eu")
    w.snapshot(t)
  }

  // PT-009: MERGE with partition filter
  test("pt009_merge_partition",
      "MERGE with update/insert scoped to partition",
      "write", "partitioned", "merge") { w =>
    w.sql("""CREATE TABLE tbl (id INT, data STRING, region STRING)
      USING delta PARTITIONED BY (region)""")
    w.sql("INSERT INTO tbl VALUES (1, 'a', 'US'), (2, 'b', 'US'), (3, 'c', 'EU'), (4, 'd', 'EU')")
    w.sql("""CREATE OR REPLACE TEMP VIEW src AS
      SELECT * FROM VALUES (1, 'updated', 'US'), (5, 'new', 'US') AS t(id, data, region)""")
    w.sql("""MERGE INTO tbl AS target USING src AS source
      ON target.id = source.id AND target.region = source.region
      WHEN MATCHED THEN UPDATE SET data = source.data
      WHEN NOT MATCHED THEN INSERT *""")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, predicate = "region = 'US'", name = "read_us")
    w.read(t, predicate = "region = 'EU'", name = "read_eu")
    w.snapshot(t)
  }

  // PT-010: Date partition column
  test("pt010_date_partition",
      "Date partition column with range predicates",
      "write", "partitioned", "insert") { w =>
    w.sql("""CREATE TABLE tbl (id INT, data STRING, dt DATE)
      USING delta PARTITIONED BY (dt)""")
    w.sql("INSERT INTO tbl VALUES (1, 'a', DATE'2024-01-01'), (2, 'b', DATE'2024-01-01')")
    w.sql("INSERT INTO tbl VALUES (3, 'c', DATE'2024-06-15'), (4, 'd', DATE'2024-06-15')")
    w.sql("INSERT INTO tbl VALUES (5, 'e', DATE'2024-12-31'), (6, 'f', DATE'2024-12-31')")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, predicate = "dt = DATE'2024-01-01'", name = "read_jan")
    w.read(t, predicate = "dt > DATE'2024-06-15'", name = "read_gt_june")
    w.read(t, predicate = "dt <= DATE'2024-06-15'", name = "read_le_june")
    w.snapshot(t)
  }

  // PT-011: Boolean partition column (true/false/null)
  test("pt011_boolean_partition",
      "Boolean partition column with true, false, and null",
      "write", "partitioned", "insert") { w =>
    w.sql("""CREATE TABLE tbl (id INT, data STRING, flag BOOLEAN)
      USING delta PARTITIONED BY (flag)""")
    w.sql("INSERT INTO tbl VALUES (1, 'a', true), (2, 'b', true), (3, 'c', true)")
    w.sql("INSERT INTO tbl VALUES (4, 'd', false), (5, 'e', false)")
    w.sql("INSERT INTO tbl VALUES (6, 'f', NULL), (7, 'g', NULL)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, predicate = "flag = true", name = "read_true")
    w.read(t, predicate = "flag = false", name = "read_false")
    w.read(t, predicate = "flag IS NULL", name = "read_null")
    w.snapshot(t)
  }

  // PT-012: Partition with null values
  test("pt012_null_partition",
      "String partition with null partition values",
      "write", "partitioned", "insert") { w =>
    w.sql("""CREATE TABLE tbl (id INT, data STRING, part STRING)
      USING delta PARTITIONED BY (part)""")
    w.sql("INSERT INTO tbl VALUES (1, 'a', 'X'), (2, 'b', 'X')")
    w.sql("INSERT INTO tbl VALUES (3, 'c', 'Y'), (4, 'd', 'Y')")
    w.sql("INSERT INTO tbl VALUES (5, 'e', NULL), (6, 'f', NULL), (7, 'g', NULL)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, predicate = "part = 'X'", name = "read_X")
    w.read(t, predicate = "part IS NULL", name = "read_null_part")
    w.read(t, predicate = "part IS NOT NULL", name = "read_not_null_part")
    w.snapshot(t)
  }

  // PT-013: Many partitions (12) with selective read
  test("pt013_many_partitions",
      "12 partitions with selective predicate reads",
      "write", "partitioned", "insert") { w =>
    w.sql("""CREATE TABLE tbl (id INT, data STRING, part INT)
      USING delta PARTITIONED BY (part)""")
    w.sql("INSERT INTO tbl SELECT id, CAST(id AS STRING), CAST(id % 12 AS INT) FROM RANGE(1, 61)")
    w.sql("INSERT INTO tbl VALUES (61, 'extra', 0), (62, 'extra', 1), (63, 'extra', 2)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, predicate = "part = 0", name = "read_part_0")
    w.read(t, predicate = "part = 11", name = "read_part_11")
    w.read(t, predicate = "part < 3", name = "read_part_lt_3")
    w.snapshot(t)
  }

  // PT-014: Partition + data skipping combined (two files per partition)
  test("pt014_partition_plus_skipping",
      "Partition pruning combined with data skipping on score column",
      "write", "partitioned", "insert", "dataSkipping") { w =>
    w.sql("""CREATE TABLE tbl (id INT, score DOUBLE, region STRING)
      USING delta PARTITIONED BY (region)""")
    // US partition: two inserts = two data files
    w.sql("INSERT INTO tbl VALUES (1, 10.0, 'US'), (2, 20.0, 'US'), (3, 30.0, 'US')")
    w.sql("INSERT INTO tbl VALUES (4, 80.0, 'US'), (5, 90.0, 'US'), (6, 100.0, 'US')")
    // EU partition
    w.sql("INSERT INTO tbl VALUES (7, 40.0, 'EU'), (8, 50.0, 'EU'), (9, 60.0, 'EU')")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, predicate = "region = 'US'", name = "read_us_only")
    w.read(t, predicate = "region = 'US' AND score > 50.0", name = "read_us_high_score")
    w.read(t, predicate = "region = 'EU'", name = "read_eu_only")
    w.read(t, predicate = "score > 50.0", name = "read_score_gt_50")
    w.snapshot(t)
  }

  // PT-015: Three-level nested partitioning (country, year, quarter)
  test("pt015_three_level_partition",
      "Three partition columns: country, year, quarter",
      "write", "partitioned", "insert") { w =>
    w.sql("""CREATE TABLE tbl (id INT, data STRING, country STRING, year INT, quarter INT)
      USING delta PARTITIONED BY (country, year, quarter)""")
    w.sql("""INSERT INTO tbl VALUES
      (1, 'a', 'US', 2023, 1), (2, 'b', 'US', 2023, 2),
      (3, 'c', 'US', 2024, 1), (4, 'd', 'US', 2024, 2)""")
    w.sql("""INSERT INTO tbl VALUES
      (5, 'e', 'EU', 2023, 1), (6, 'f', 'EU', 2023, 2),
      (7, 'g', 'EU', 2024, 1)""")
    w.sql("""INSERT INTO tbl VALUES
      (8, 'h', 'APAC', 2024, 1), (9, 'i', 'APAC', 2024, 2),
      (10, 'j', 'APAC', 2024, 3)""")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, predicate = "country = 'US'", name = "read_us")
    w.read(t, predicate = "country = 'US' AND year = 2024", name = "read_us_2024")
    w.read(t, predicate = "country = 'US' AND year = 2024 AND quarter = 1", name = "read_us_2024_q1")
    w.read(t, predicate = "year = 2024", name = "read_2024")
    w.read(t, predicate = "quarter = 1", name = "read_q1")
    w.snapshot(t)
  }

  // =========================================================================
  // Data Skipping Write Capture (DSK-001 through DSK-015)
  // =========================================================================

  // DSK-001: INT column, 3 inserts with non-overlapping ranges
  test("dsk001_int_nonoverlapping_ranges",
      "INT column with non-overlapping ranges, range predicates skip files",
      "write", "insert", "dataSkipping") { w =>
    w.sql("CREATE TABLE tbl (id INT, value STRING) USING delta")
    w.sql("INSERT INTO tbl SELECT id, CAST(id AS STRING) FROM RANGE(1, 101)")
    w.sql("INSERT INTO tbl SELECT id, CAST(id AS STRING) FROM RANGE(101, 201)")
    w.sql("INSERT INTO tbl SELECT id, CAST(id AS STRING) FROM RANGE(201, 301)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, predicate = "id > 200", name = "read_gt_200")
    w.read(t, predicate = "id <= 200", name = "read_le_200")
    w.snapshot(t)
  }

  // DSK-002: STRING column equality predicate
  test("dsk002_string_equality",
      "STRING column with distinct value groups, equality predicate",
      "write", "insert", "dataSkipping") { w =>
    w.sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
    w.sql("INSERT INTO tbl VALUES (1, 'alpha'), (2, 'avocado'), (3, 'apple')")
    w.sql("INSERT INTO tbl VALUES (4, 'mango'), (5, 'melon'), (6, 'mint')")
    w.sql("INSERT INTO tbl VALUES (7, 'zebra'), (8, 'zinc'), (9, 'zucchini')")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, predicate = "name = 'mango'", name = "read_eq_mango")
    w.read(t, predicate = "name = 'zebra'", name = "read_eq_zebra")
    w.snapshot(t)
  }

  // DSK-003: DATE column range predicate
  test("dsk003_date_range",
      "DATE column with date range predicates",
      "write", "insert", "dataSkipping") { w =>
    w.sql("CREATE TABLE tbl (id INT, dt DATE) USING delta")
    w.sql("INSERT INTO tbl VALUES (1, DATE'2023-01-15'), (2, DATE'2023-06-30'), (3, DATE'2023-12-31')")
    w.sql("INSERT INTO tbl VALUES (4, DATE'2024-01-15'), (5, DATE'2024-03-20'), (6, DATE'2024-06-30')")
    w.sql("INSERT INTO tbl VALUES (7, DATE'2024-07-01'), (8, DATE'2024-10-15'), (9, DATE'2024-12-31')")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, predicate = "dt > DATE'2024-01-01'", name = "read_gt_2024")
    w.read(t, predicate = "dt <= DATE'2023-12-31'", name = "read_le_2023")
    w.snapshot(t)
  }

  // DSK-004: TIMESTAMP column range predicate
  test("dsk004_timestamp_range",
      "TIMESTAMP column with time-of-day range predicates",
      "write", "insert", "dataSkipping") { w =>
    w.sql("CREATE TABLE tbl (id INT, ts TIMESTAMP) USING delta")
    w.sql("""INSERT INTO tbl VALUES
      (1, TIMESTAMP'2024-01-15 08:00:00'),
      (2, TIMESTAMP'2024-01-15 09:30:00'),
      (3, TIMESTAMP'2024-01-15 11:00:00')""")
    w.sql("""INSERT INTO tbl VALUES
      (4, TIMESTAMP'2024-01-15 13:00:00'),
      (5, TIMESTAMP'2024-01-15 15:30:00'),
      (6, TIMESTAMP'2024-01-15 17:00:00')""")
    w.sql("""INSERT INTO tbl VALUES
      (7, TIMESTAMP'2024-01-15 19:00:00'),
      (8, TIMESTAMP'2024-01-15 21:30:00'),
      (9, TIMESTAMP'2024-01-15 23:00:00')""")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, predicate = "ts >= TIMESTAMP'2024-01-15 13:00:00'", name = "read_afternoon_plus")
    w.read(t, predicate = "ts < TIMESTAMP'2024-01-15 12:00:00'", name = "read_morning_only")
    w.snapshot(t)
  }

  // DSK-005: DECIMAL column range predicate
  test("dsk005_decimal_range",
      "DECIMAL(10,2) column with range predicates",
      "write", "insert", "dataSkipping") { w =>
    w.sql("CREATE TABLE tbl (id INT, amount DECIMAL(10,2)) USING delta")
    w.sql("INSERT INTO tbl VALUES (1, 1.50), (2, 5.75), (3, 9.99)")
    w.sql("INSERT INTO tbl VALUES (4, 100.00), (5, 250.50), (6, 499.99)")
    w.sql("INSERT INTO tbl VALUES (7, 1000.00), (8, 5000.50), (9, 9999.99)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, predicate = "amount > 500.00", name = "read_gt_500")
    w.read(t, predicate = "amount <= 10.00", name = "read_le_10")
    w.snapshot(t)
  }

  // DSK-006: Compound AND predicate
  test("dsk006_compound_and",
      "Compound AND predicate on id and name columns",
      "write", "insert", "dataSkipping") { w =>
    w.sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
    w.sql("INSERT INTO tbl VALUES (1, 'foo'), (2, 'foo'), (50, 'foo')")
    w.sql("INSERT INTO tbl VALUES (101, 'bar'), (150, 'bar'), (200, 'bar')")
    w.sql("INSERT INTO tbl VALUES (101, 'foo'), (200, 'foo'), (300, 'foo')")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, predicate = "id > 100 AND name = 'foo'", name = "read_compound")
    w.read(t, predicate = "id > 100", name = "read_id_only")
    w.snapshot(t)
  }

  // DSK-007: OR predicate on edges
  test("dsk007_or_predicate",
      "OR predicate selecting from first and last file ranges",
      "write", "insert", "dataSkipping") { w =>
    w.sql("CREATE TABLE tbl (id INT, value STRING) USING delta")
    w.sql("INSERT INTO tbl SELECT id, CAST(id AS STRING) FROM RANGE(1, 10)")
    w.sql("INSERT INTO tbl SELECT id, CAST(id AS STRING) FROM RANGE(500, 510)")
    w.sql("INSERT INTO tbl SELECT id, CAST(id AS STRING) FROM RANGE(991, 1001)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, predicate = "id < 10 OR id > 990", name = "read_or_edges")
    w.read(t, predicate = "id >= 500 AND id < 510", name = "read_middle")
    w.snapshot(t)
  }

  // DSK-008: IS NULL / IS NOT NULL predicates
  test("dsk008_null_predicates",
      "IS NULL and IS NOT NULL on nullable STRING column",
      "write", "insert", "dataSkipping") { w =>
    w.sql("CREATE TABLE tbl (id INT, value STRING) USING delta")
    w.sql("INSERT INTO tbl VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    w.sql("INSERT INTO tbl VALUES (4, NULL), (5, NULL), (6, NULL)")
    w.sql("INSERT INTO tbl VALUES (7, 'd'), (8, NULL), (9, 'e')")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, predicate = "value IS NOT NULL", name = "read_not_null")
    w.read(t, predicate = "value IS NULL", name = "read_is_null")
    w.snapshot(t)
  }

  // DSK-009: Multi-column stats filtering
  test("dsk009_multi_column_stats",
      "Three columns with distinct stats per file",
      "write", "insert", "dataSkipping") { w =>
    w.sql("CREATE TABLE tbl (id INT, category STRING, score DOUBLE) USING delta")
    w.sql("INSERT INTO tbl VALUES (1, 'A', 10.0), (2, 'A', 20.0), (3, 'A', 30.0)")
    w.sql("INSERT INTO tbl VALUES (4, 'B', 50.0), (5, 'B', 60.0), (6, 'B', 70.0)")
    w.sql("INSERT INTO tbl VALUES (7, 'C', 90.0), (8, 'C', 95.0), (9, 'C', 100.0)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, predicate = "score > 80.0", name = "read_score_gt_80")
    w.read(t, predicate = "category = 'B'", name = "read_category_B")
    w.read(t, predicate = "id <= 3", name = "read_id_le_3")
    w.snapshot(t)
  }

  // DSK-010: BETWEEN predicate
  test("dsk010_between_predicate",
      "BETWEEN predicate on non-overlapping ranges",
      "write", "insert", "dataSkipping") { w =>
    w.sql("CREATE TABLE tbl (id INT, value STRING) USING delta")
    w.sql("INSERT INTO tbl SELECT id, CAST(id AS STRING) FROM RANGE(1, 51)")
    w.sql("INSERT INTO tbl SELECT id, CAST(id AS STRING) FROM RANGE(51, 101)")
    w.sql("INSERT INTO tbl SELECT id, CAST(id AS STRING) FROM RANGE(101, 151)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, predicate = "id BETWEEN 60 AND 90", name = "read_between_60_90")
    w.read(t, predicate = "id BETWEEN 1 AND 50", name = "read_between_1_50")
    w.snapshot(t)
  }

  // DSK-011: IN predicate
  test("dsk011_in_predicate",
      "IN predicate selecting specific ids across files",
      "write", "insert", "dataSkipping") { w =>
    w.sql("CREATE TABLE tbl (id INT, value STRING) USING delta")
    w.sql("INSERT INTO tbl SELECT id, CAST(id AS STRING) FROM RANGE(1, 11)")
    w.sql("INSERT INTO tbl SELECT id, CAST(id AS STRING) FROM RANGE(101, 111)")
    w.sql("INSERT INTO tbl SELECT id, CAST(id AS STRING) FROM RANGE(201, 211)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, predicate = "id IN (1, 5, 10)", name = "read_in_low")
    w.read(t, predicate = "id IN (1, 105, 210)", name = "read_in_mixed")
    w.snapshot(t)
  }

  // DSK-012: Nested struct field predicate
  test("dsk012_nested_struct",
      "Nested struct field predicate on info.score",
      "write", "insert", "dataSkipping") { w =>
    w.sql("CREATE TABLE tbl (id INT, info STRUCT<name: STRING, score: INT>) USING delta")
    w.sql("""INSERT INTO tbl VALUES
      (1, named_struct('name', 'Alice', 'score', 10)),
      (2, named_struct('name', 'Bob', 'score', 20))""")
    w.sql("""INSERT INTO tbl VALUES
      (3, named_struct('name', 'Carol', 'score', 50)),
      (4, named_struct('name', 'Dave', 'score', 60))""")
    w.sql("""INSERT INTO tbl VALUES
      (5, named_struct('name', 'Eve', 'score', 90)),
      (6, named_struct('name', 'Frank', 'score', 100))""")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, predicate = "info.score > 80", name = "read_high_score")
    w.read(t, predicate = "info.score <= 20", name = "read_low_score")
    w.snapshot(t)
  }

  // DSK-013: Large number of files (6 inserts) with selective predicate
  test("dsk013_many_files",
      "6 files with non-overlapping ranges, selective predicates",
      "write", "insert", "dataSkipping") { w =>
    w.sql("CREATE TABLE tbl (id INT, value STRING) USING delta")
    w.sql("INSERT INTO tbl SELECT id, CAST(id AS STRING) FROM RANGE(1, 101)")
    w.sql("INSERT INTO tbl SELECT id, CAST(id AS STRING) FROM RANGE(101, 201)")
    w.sql("INSERT INTO tbl SELECT id, CAST(id AS STRING) FROM RANGE(201, 301)")
    w.sql("INSERT INTO tbl SELECT id, CAST(id AS STRING) FROM RANGE(301, 401)")
    w.sql("INSERT INTO tbl SELECT id, CAST(id AS STRING) FROM RANGE(401, 501)")
    w.sql("INSERT INTO tbl SELECT id, CAST(id AS STRING) FROM RANGE(501, 601)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, predicate = "id > 500", name = "read_gt_500")
    w.read(t, predicate = "id <= 100", name = "read_le_100")
    w.read(t, predicate = "id BETWEEN 250 AND 350", name = "read_between_250_350")
    w.snapshot(t)
  }

  // DSK-014: Delete + read with predicate (DV interaction with data skipping)
  test("dsk014_delete_then_read",
      "Delete rows from middle file, verify skipping with DVs",
      "write", "insert", "delete", "dataSkipping", "dv") { w =>
    w.sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id, CAST(id AS STRING) FROM RANGE(1, 51)")
    w.sql("INSERT INTO tbl SELECT id, CAST(id AS STRING) FROM RANGE(51, 101)")
    w.sql("INSERT INTO tbl SELECT id, CAST(id AS STRING) FROM RANGE(101, 151)")
    w.sql("DELETE FROM tbl WHERE id >= 60 AND id <= 70")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, predicate = "id > 100", name = "read_gt_100")
    w.read(t, predicate = "id <= 50", name = "read_le_50")
    w.read(t, predicate = "id BETWEEN 55 AND 75", name = "read_between_55_75")
    w.read(t, version = 3, name = "read_before_delete")
    w.snapshot(t)
  }

  // DSK-015: Update + read with predicate (rewritten files + data skipping)
  test("dsk015_update_then_read",
      "Update rows in middle file, verify skipping after rewrite",
      "write", "insert", "update", "dataSkipping", "dv") { w =>
    w.sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id, CAST(id AS STRING) FROM RANGE(1, 51)")
    w.sql("INSERT INTO tbl SELECT id, CAST(id AS STRING) FROM RANGE(51, 101)")
    w.sql("INSERT INTO tbl SELECT id, CAST(id AS STRING) FROM RANGE(101, 151)")
    w.sql("UPDATE tbl SET value = 'updated' WHERE id >= 60 AND id <= 70")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, predicate = "id > 100", name = "read_gt_100")
    w.read(t, predicate = "id <= 50", name = "read_le_50")
    w.read(t, predicate = "value = 'updated'", name = "read_updated")
    w.read(t, version = 3, name = "read_before_update")
    w.snapshot(t)
  }

}.runAll()
