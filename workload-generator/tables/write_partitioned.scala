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
      "write", "partitioned", "insert") {
    sql("""CREATE TABLE tbl (id INT, data STRING, region STRING)
      USING delta PARTITIONED BY (region)""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "data" -> "a1", "region" -> "US"),
      Map("id" -> 2, "data" -> "a2", "region" -> "US"),
      Map("id" -> 3, "data" -> "a3", "region" -> "US")))
    insertOp(w, Seq(
      Map("id" -> 4, "data" -> "b1", "region" -> "EU"),
      Map("id" -> 5, "data" -> "b2", "region" -> "EU")))
    insertOp(w, Seq(
      Map("id" -> 6, "data" -> "c1", "region" -> "APAC"),
      Map("id" -> 7, "data" -> "c2", "region" -> "APAC"),
      Map("id" -> 8, "data" -> "c3", "region" -> "APAC"),
      Map("id" -> 9, "data" -> "c4", "region" -> "APAC")))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    read(t, predicate = "region = 'US'", name = "read_us")
    read(t, predicate = "region = 'EU'", name = "read_eu")
    read(t, predicate = "region = 'APAC'", name = "read_apac")
    snapshot(t)
  }

  // PT-002: Single int partition column
  test("pt002_single_int_partition",
      "Single int partition column with 3 categories",
      "write", "partitioned", "insert") {
    sql("""CREATE TABLE tbl (data STRING, category INT)
      USING delta PARTITIONED BY (category)""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("data" -> "a", "category" -> 1),
      Map("data" -> "b", "category" -> 1),
      Map("data" -> "c", "category" -> 1)))
    insertOp(w, Seq(
      Map("data" -> "d", "category" -> 2),
      Map("data" -> "e", "category" -> 2)))
    insertOp(w, Seq(
      Map("data" -> "f", "category" -> 3),
      Map("data" -> "g", "category" -> 3),
      Map("data" -> "h", "category" -> 3),
      Map("data" -> "i", "category" -> 3)))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    read(t, predicate = "category = 1", name = "read_cat_1")
    read(t, predicate = "category = 2", name = "read_cat_2")
    read(t, predicate = "category >= 2", name = "read_cat_ge_2")
    snapshot(t)
  }

  // PT-003: Two partition columns (year INT, month INT)
  test("pt003_two_partition_columns",
      "Two partition columns year and month",
      "write", "partitioned", "insert") {
    sql("""CREATE TABLE tbl (id INT, data STRING, year INT, month INT)
      USING delta PARTITIONED BY (year, month)""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "data" -> "jan23", "year" -> 2023, "month" -> 1),
      Map("id" -> 2, "data" -> "feb23", "year" -> 2023, "month" -> 2),
      Map("id" -> 3, "data" -> "mar23", "year" -> 2023, "month" -> 3)))
    insertOp(w, Seq(
      Map("id" -> 4, "data" -> "jan24", "year" -> 2024, "month" -> 1),
      Map("id" -> 5, "data" -> "feb24", "year" -> 2024, "month" -> 2),
      Map("id" -> 6, "data" -> "mar24", "year" -> 2024, "month" -> 3)))
    insertOp(w, Seq(
      Map("id" -> 7, "data" -> "apr24", "year" -> 2024, "month" -> 4),
      Map("id" -> 8, "data" -> "may24", "year" -> 2024, "month" -> 5)))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    read(t, predicate = "year = 2023", name = "read_2023")
    read(t, predicate = "year = 2024", name = "read_2024")
    read(t, predicate = "year = 2024 AND month = 1", name = "read_2024_jan")
    read(t, predicate = "month <= 2", name = "read_month_le_2")
    snapshot(t)
  }

  // PT-004: Partition + non-partition columns with combined predicate
  test("pt004_partition_and_data_predicates",
      "Partition and data column predicates combined",
      "write", "partitioned", "insert", "dataSkipping") {
    sql("""CREATE TABLE tbl (id INT, value DOUBLE, status STRING)
      USING delta PARTITIONED BY (status)""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "value" -> 10.0, "status" -> "active"),
      Map("id" -> 2, "value" -> 20.0, "status" -> "active"),
      Map("id" -> 3, "value" -> 30.0, "status" -> "active")))
    insertOp(w, Seq(
      Map("id" -> 4, "value" -> 40.0, "status" -> "inactive"),
      Map("id" -> 5, "value" -> 50.0, "status" -> "inactive")))
    insertOp(w, Seq(
      Map("id" -> 6, "value" -> 60.0, "status" -> "pending"),
      Map("id" -> 7, "value" -> 70.0, "status" -> "pending"),
      Map("id" -> 8, "value" -> 80.0, "status" -> "pending")))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    read(t, predicate = "status = 'active'", name = "read_active")
    read(t, predicate = "status = 'active' AND value > 15.0", name = "read_active_gt_15")
    read(t, predicate = "status != 'inactive'", name = "read_not_inactive")
    snapshot(t)
  }

  // PT-005: INSERT OVERWRITE single partition
  test("pt005_insert_overwrite_partition",
      "INSERT OVERWRITE replaces single partition",
      "write", "partitioned", "insert") {
    sql("""CREATE TABLE tbl (id INT, data STRING, part STRING)
      USING delta PARTITIONED BY (part)""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "data" -> "old_a", "part" -> "A"),
      Map("id" -> 2, "data" -> "old_a2", "part" -> "A"),
      Map("id" -> 3, "data" -> "old_b", "part" -> "B")))
    sql("INSERT OVERWRITE TABLE tbl PARTITION (part = 'A') VALUES (10, 'new_a'), (11, 'new_a2'), (12, 'new_a3')")
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    read(t, predicate = "part = 'A'", name = "read_part_A")
    read(t, predicate = "part = 'B'", name = "read_part_B")
    snapshot(t)
  }

  // PT-006: INSERT OVERWRITE with dynamic partitioning
  test("pt006_insert_overwrite_dynamic",
      "INSERT OVERWRITE with dynamic partition mode",
      "write", "partitioned", "insert") {
    sql("""CREATE TABLE tbl (id INT, data STRING, part STRING)
      USING delta PARTITIONED BY (part)""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "data" -> "old_a", "part" -> "A"),
      Map("id" -> 2, "data" -> "old_b", "part" -> "B"),
      Map("id" -> 3, "data" -> "old_c", "part" -> "C")))
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    sql("INSERT OVERWRITE TABLE tbl VALUES (10, 'new_a', 'A'), (11, 'new_b', 'B')")
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    read(t, predicate = "part = 'A'", name = "read_part_A")
    read(t, predicate = "part = 'B'", name = "read_part_B")
    read(t, predicate = "part = 'C'", name = "read_part_C")
    snapshot(t)
  }

  // PT-007: DELETE with partition predicate
  test("pt007_delete_partition",
      "DELETE entire partition by predicate",
      "write", "partitioned", "delete") {
    sql("""CREATE TABLE tbl (id INT, data STRING, region STRING)
      USING delta PARTITIONED BY (region)""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "data" -> "a", "region" -> "US"),
      Map("id" -> 2, "data" -> "b", "region" -> "US"),
      Map("id" -> 3, "data" -> "c", "region" -> "EU"),
      Map("id" -> 4, "data" -> "d", "region" -> "EU"),
      Map("id" -> 5, "data" -> "e", "region" -> "APAC")))
    deleteOp(w, "region = 'EU'")
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    read(t, predicate = "region = 'US'", name = "read_us")
    read(t, predicate = "region = 'EU'", name = "read_eu")
    read(t, predicate = "region = 'APAC'", name = "read_apac")
    snapshot(t)
  }

  // PT-008: UPDATE with partition predicate
  test("pt008_update_partition",
      "UPDATE rows within a single partition",
      "write", "partitioned", "update") {
    sql("""CREATE TABLE tbl (id INT, data STRING, region STRING)
      USING delta PARTITIONED BY (region)""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "data" -> "old", "region" -> "US"),
      Map("id" -> 2, "data" -> "old", "region" -> "US"),
      Map("id" -> 3, "data" -> "old", "region" -> "EU"),
      Map("id" -> 4, "data" -> "old", "region" -> "EU"),
      Map("id" -> 5, "data" -> "old", "region" -> "APAC")))
    updateOp(w, "region = 'US'", Map("data" -> "'new'"))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    read(t, predicate = "region = 'US'", name = "read_us")
    read(t, predicate = "data = 'new'", name = "read_updated")
    read(t, predicate = "region = 'EU'", name = "read_eu")
    snapshot(t)
  }

  // PT-009: MERGE with partition filter
  test("pt009_merge_partition",
      "MERGE with update/insert scoped to partition",
      "write", "partitioned", "merge") {
    sql("""CREATE TABLE tbl (id INT, data STRING, region STRING)
      USING delta PARTITIONED BY (region)""")
    sql("INSERT INTO tbl VALUES (1, 'a', 'US'), (2, 'b', 'US'), (3, 'c', 'EU'), (4, 'd', 'EU')")
    sql("""CREATE OR REPLACE TEMP VIEW src AS
      SELECT * FROM VALUES (1, 'updated', 'US'), (5, 'new', 'US') AS t(id, data, region)""")
    sql("""MERGE INTO tbl AS target USING src AS source
      ON target.id = source.id AND target.region = source.region
      WHEN MATCHED THEN UPDATE SET data = source.data
      WHEN NOT MATCHED THEN INSERT *""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    read(t, predicate = "region = 'US'", name = "read_us")
    read(t, predicate = "region = 'EU'", name = "read_eu")
    snapshot(t)
  }

  // PT-010: Date partition column
  test("pt010_date_partition",
      "Date partition column with range predicates",
      "write", "partitioned", "insert") {
    sql("""CREATE TABLE tbl (id INT, data STRING, dt DATE)
      USING delta PARTITIONED BY (dt)""")
    sql("INSERT INTO tbl VALUES (1, 'a', DATE'2024-01-01'), (2, 'b', DATE'2024-01-01')")
    sql("INSERT INTO tbl VALUES (3, 'c', DATE'2024-06-15'), (4, 'd', DATE'2024-06-15')")
    sql("INSERT INTO tbl VALUES (5, 'e', DATE'2024-12-31'), (6, 'f', DATE'2024-12-31')")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    read(t, predicate = "dt = DATE'2024-01-01'", name = "read_jan")
    read(t, predicate = "dt > DATE'2024-06-15'", name = "read_gt_june")
    read(t, predicate = "dt <= DATE'2024-06-15'", name = "read_le_june")
    snapshot(t)
  }

  // PT-011: Boolean partition column (true/false/null)
  test("pt011_boolean_partition",
      "Boolean partition column with true, false, and null",
      "write", "partitioned", "insert") {
    sql("""CREATE TABLE tbl (id INT, data STRING, flag BOOLEAN)
      USING delta PARTITIONED BY (flag)""")
    sql("INSERT INTO tbl VALUES (1, 'a', true), (2, 'b', true), (3, 'c', true)")
    sql("INSERT INTO tbl VALUES (4, 'd', false), (5, 'e', false)")
    sql("INSERT INTO tbl VALUES (6, 'f', NULL), (7, 'g', NULL)")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    read(t, predicate = "flag = true", name = "read_true")
    read(t, predicate = "flag = false", name = "read_false")
    read(t, predicate = "flag IS NULL", name = "read_null")
    snapshot(t)
  }

  // PT-012: Partition with null values
  test("pt012_null_partition",
      "String partition with null partition values",
      "write", "partitioned", "insert") {
    sql("""CREATE TABLE tbl (id INT, data STRING, part STRING)
      USING delta PARTITIONED BY (part)""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "data" -> "a", "part" -> "X"),
      Map("id" -> 2, "data" -> "b", "part" -> "X")))
    insertOp(w, Seq(
      Map("id" -> 3, "data" -> "c", "part" -> "Y"),
      Map("id" -> 4, "data" -> "d", "part" -> "Y")))
    insertOp(w, Seq(
      Map("id" -> 5, "data" -> "e", "part" -> null),
      Map("id" -> 6, "data" -> "f", "part" -> null),
      Map("id" -> 7, "data" -> "g", "part" -> null)))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    read(t, predicate = "part = 'X'", name = "read_X")
    read(t, predicate = "part IS NULL", name = "read_null_part")
    read(t, predicate = "part IS NOT NULL", name = "read_not_null_part")
    snapshot(t)
  }

  // PT-013: Many partitions (12) with selective read
  test("pt013_many_partitions",
      "12 partitions with selective predicate reads",
      "write", "partitioned", "insert") {
    sql("""CREATE TABLE tbl (id INT, data STRING, part INT)
      USING delta PARTITIONED BY (part)""")
    sql("INSERT INTO tbl SELECT id, CAST(id AS STRING), CAST(id % 12 AS INT) FROM RANGE(1, 61)")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 61, "data" -> "extra", "part" -> 0),
      Map("id" -> 62, "data" -> "extra", "part" -> 1),
      Map("id" -> 63, "data" -> "extra", "part" -> 2)))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    read(t, predicate = "part = 0", name = "read_part_0")
    read(t, predicate = "part = 11", name = "read_part_11")
    read(t, predicate = "part < 3", name = "read_part_lt_3")
    snapshot(t)
  }

  // PT-014: Partition + data skipping combined (two files per partition)
  test("pt014_partition_plus_skipping",
      "Partition pruning combined with data skipping on score column",
      "write", "partitioned", "insert", "dataSkipping") {
    sql("""CREATE TABLE tbl (id INT, score DOUBLE, region STRING)
      USING delta PARTITIONED BY (region)""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    // US partition: two inserts = two data files
    insertOp(w, Seq(
      Map("id" -> 1, "score" -> 10.0, "region" -> "US"),
      Map("id" -> 2, "score" -> 20.0, "region" -> "US"),
      Map("id" -> 3, "score" -> 30.0, "region" -> "US")))
    insertOp(w, Seq(
      Map("id" -> 4, "score" -> 80.0, "region" -> "US"),
      Map("id" -> 5, "score" -> 90.0, "region" -> "US"),
      Map("id" -> 6, "score" -> 100.0, "region" -> "US")))
    // EU partition
    insertOp(w, Seq(
      Map("id" -> 7, "score" -> 40.0, "region" -> "EU"),
      Map("id" -> 8, "score" -> 50.0, "region" -> "EU"),
      Map("id" -> 9, "score" -> 60.0, "region" -> "EU")))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    read(t, predicate = "region = 'US'", name = "read_us_only")
    read(t, predicate = "region = 'US' AND score > 50.0", name = "read_us_high_score")
    read(t, predicate = "region = 'EU'", name = "read_eu_only")
    read(t, predicate = "score > 50.0", name = "read_score_gt_50")
    snapshot(t)
  }

  // PT-015: Three-level nested partitioning (country, year, quarter)
  test("pt015_three_level_partition",
      "Three partition columns: country, year, quarter",
      "write", "partitioned", "insert") {
    sql("""CREATE TABLE tbl (id INT, data STRING, country STRING, year INT, quarter INT)
      USING delta PARTITIONED BY (country, year, quarter)""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "data" -> "a", "country" -> "US", "year" -> 2023, "quarter" -> 1),
      Map("id" -> 2, "data" -> "b", "country" -> "US", "year" -> 2023, "quarter" -> 2),
      Map("id" -> 3, "data" -> "c", "country" -> "US", "year" -> 2024, "quarter" -> 1),
      Map("id" -> 4, "data" -> "d", "country" -> "US", "year" -> 2024, "quarter" -> 2)))
    insertOp(w, Seq(
      Map("id" -> 5, "data" -> "e", "country" -> "EU", "year" -> 2023, "quarter" -> 1),
      Map("id" -> 6, "data" -> "f", "country" -> "EU", "year" -> 2023, "quarter" -> 2),
      Map("id" -> 7, "data" -> "g", "country" -> "EU", "year" -> 2024, "quarter" -> 1)))
    insertOp(w, Seq(
      Map("id" -> 8, "data" -> "h", "country" -> "APAC", "year" -> 2024, "quarter" -> 1),
      Map("id" -> 9, "data" -> "i", "country" -> "APAC", "year" -> 2024, "quarter" -> 2),
      Map("id" -> 10, "data" -> "j", "country" -> "APAC", "year" -> 2024, "quarter" -> 3)))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    read(t, predicate = "country = 'US'", name = "read_us")
    read(t, predicate = "country = 'US' AND year = 2024", name = "read_us_2024")
    read(t, predicate = "country = 'US' AND year = 2024 AND quarter = 1", name = "read_us_2024_q1")
    read(t, predicate = "year = 2024", name = "read_2024")
    read(t, predicate = "quarter = 1", name = "read_q1")
    snapshot(t)
  }

  // =========================================================================
  // Data Skipping Write Capture (DSK-001 through DSK-015)
  // =========================================================================

  // DSK-001: INT column, 3 inserts with non-overlapping ranges
  test("dsk001_int_nonoverlapping_ranges",
      "INT column with non-overlapping ranges, range predicates skip files",
      "write", "insert", "dataSkipping") {
    sql("CREATE TABLE tbl (id INT, value STRING) USING delta")
    sql("INSERT INTO tbl SELECT id, CAST(id AS STRING) FROM RANGE(1, 101)")
    sql("INSERT INTO tbl SELECT id, CAST(id AS STRING) FROM RANGE(101, 201)")
    sql("INSERT INTO tbl SELECT id, CAST(id AS STRING) FROM RANGE(201, 301)")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    read(t, predicate = "id > 200", name = "read_gt_200")
    read(t, predicate = "id <= 200", name = "read_le_200")
    snapshot(t)
  }

  // DSK-002: STRING column equality predicate
  test("dsk002_string_equality",
      "STRING column with distinct value groups, equality predicate",
      "write", "insert", "dataSkipping") {
    sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "name" -> "alpha"),
      Map("id" -> 2, "name" -> "avocado"),
      Map("id" -> 3, "name" -> "apple")))
    insertOp(w, Seq(
      Map("id" -> 4, "name" -> "mango"),
      Map("id" -> 5, "name" -> "melon"),
      Map("id" -> 6, "name" -> "mint")))
    insertOp(w, Seq(
      Map("id" -> 7, "name" -> "zebra"),
      Map("id" -> 8, "name" -> "zinc"),
      Map("id" -> 9, "name" -> "zucchini")))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    read(t, predicate = "name = 'mango'", name = "read_eq_mango")
    read(t, predicate = "name = 'zebra'", name = "read_eq_zebra")
    snapshot(t)
  }

  // DSK-003: DATE column range predicate
  test("dsk003_date_range",
      "DATE column with date range predicates",
      "write", "insert", "dataSkipping") {
    sql("CREATE TABLE tbl (id INT, dt DATE) USING delta")
    sql("INSERT INTO tbl VALUES (1, DATE'2023-01-15'), (2, DATE'2023-06-30'), (3, DATE'2023-12-31')")
    sql("INSERT INTO tbl VALUES (4, DATE'2024-01-15'), (5, DATE'2024-03-20'), (6, DATE'2024-06-30')")
    sql("INSERT INTO tbl VALUES (7, DATE'2024-07-01'), (8, DATE'2024-10-15'), (9, DATE'2024-12-31')")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    read(t, predicate = "dt > DATE'2024-01-01'", name = "read_gt_2024")
    read(t, predicate = "dt <= DATE'2023-12-31'", name = "read_le_2023")
    snapshot(t)
  }

  // DSK-004: TIMESTAMP column range predicate
  test("dsk004_timestamp_range",
      "TIMESTAMP column with time-of-day range predicates",
      "write", "insert", "dataSkipping") {
    sql("CREATE TABLE tbl (id INT, ts TIMESTAMP) USING delta")
    sql("""INSERT INTO tbl VALUES
      (1, TIMESTAMP'2024-01-15 08:00:00'),
      (2, TIMESTAMP'2024-01-15 09:30:00'),
      (3, TIMESTAMP'2024-01-15 11:00:00')""")
    sql("""INSERT INTO tbl VALUES
      (4, TIMESTAMP'2024-01-15 13:00:00'),
      (5, TIMESTAMP'2024-01-15 15:30:00'),
      (6, TIMESTAMP'2024-01-15 17:00:00')""")
    sql("""INSERT INTO tbl VALUES
      (7, TIMESTAMP'2024-01-15 19:00:00'),
      (8, TIMESTAMP'2024-01-15 21:30:00'),
      (9, TIMESTAMP'2024-01-15 23:00:00')""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    read(t, predicate = "ts >= TIMESTAMP'2024-01-15 13:00:00'", name = "read_afternoon_plus")
    read(t, predicate = "ts < TIMESTAMP'2024-01-15 12:00:00'", name = "read_morning_only")
    snapshot(t)
  }

  // DSK-005: DECIMAL column range predicate
  test("dsk005_decimal_range",
      "DECIMAL(10,2) column with range predicates",
      "write", "insert", "dataSkipping") {
    sql("CREATE TABLE tbl (id INT, amount DECIMAL(10,2)) USING delta")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "amount" -> 1.50),
      Map("id" -> 2, "amount" -> 5.75),
      Map("id" -> 3, "amount" -> 9.99)))
    insertOp(w, Seq(
      Map("id" -> 4, "amount" -> 100.00),
      Map("id" -> 5, "amount" -> 250.50),
      Map("id" -> 6, "amount" -> 499.99)))
    insertOp(w, Seq(
      Map("id" -> 7, "amount" -> 1000.00),
      Map("id" -> 8, "amount" -> 5000.50),
      Map("id" -> 9, "amount" -> 9999.99)))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    read(t, predicate = "amount > 500.00", name = "read_gt_500")
    read(t, predicate = "amount <= 10.00", name = "read_le_10")
    snapshot(t)
  }

  // DSK-006: Compound AND predicate
  test("dsk006_compound_and",
      "Compound AND predicate on id and name columns",
      "write", "insert", "dataSkipping") {
    sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "name" -> "foo"),
      Map("id" -> 2, "name" -> "foo"),
      Map("id" -> 50, "name" -> "foo")))
    insertOp(w, Seq(
      Map("id" -> 101, "name" -> "bar"),
      Map("id" -> 150, "name" -> "bar"),
      Map("id" -> 200, "name" -> "bar")))
    insertOp(w, Seq(
      Map("id" -> 101, "name" -> "foo"),
      Map("id" -> 200, "name" -> "foo"),
      Map("id" -> 300, "name" -> "foo")))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    read(t, predicate = "id > 100 AND name = 'foo'", name = "read_compound")
    read(t, predicate = "id > 100", name = "read_id_only")
    snapshot(t)
  }

  // DSK-007: OR predicate on edges
  test("dsk007_or_predicate",
      "OR predicate selecting from first and last file ranges",
      "write", "insert", "dataSkipping") {
    sql("CREATE TABLE tbl (id INT, value STRING) USING delta")
    sql("INSERT INTO tbl SELECT id, CAST(id AS STRING) FROM RANGE(1, 10)")
    sql("INSERT INTO tbl SELECT id, CAST(id AS STRING) FROM RANGE(500, 510)")
    sql("INSERT INTO tbl SELECT id, CAST(id AS STRING) FROM RANGE(991, 1001)")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    read(t, predicate = "id < 10 OR id > 990", name = "read_or_edges")
    read(t, predicate = "id >= 500 AND id < 510", name = "read_middle")
    snapshot(t)
  }

  // DSK-008: IS NULL / IS NOT NULL predicates
  test("dsk008_null_predicates",
      "IS NULL and IS NOT NULL on nullable STRING column",
      "write", "insert", "dataSkipping") {
    sql("CREATE TABLE tbl (id INT, value STRING) USING delta")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "value" -> "a"),
      Map("id" -> 2, "value" -> "b"),
      Map("id" -> 3, "value" -> "c")))
    insertOp(w, Seq(
      Map("id" -> 4, "value" -> null),
      Map("id" -> 5, "value" -> null),
      Map("id" -> 6, "value" -> null)))
    insertOp(w, Seq(
      Map("id" -> 7, "value" -> "d"),
      Map("id" -> 8, "value" -> null),
      Map("id" -> 9, "value" -> "e")))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    read(t, predicate = "value IS NOT NULL", name = "read_not_null")
    read(t, predicate = "value IS NULL", name = "read_is_null")
    snapshot(t)
  }

  // DSK-009: Multi-column stats filtering
  test("dsk009_multi_column_stats",
      "Three columns with distinct stats per file",
      "write", "insert", "dataSkipping") {
    sql("CREATE TABLE tbl (id INT, category STRING, score DOUBLE) USING delta")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    insertOp(w, Seq(
      Map("id" -> 1, "category" -> "A", "score" -> 10.0),
      Map("id" -> 2, "category" -> "A", "score" -> 20.0),
      Map("id" -> 3, "category" -> "A", "score" -> 30.0)))
    insertOp(w, Seq(
      Map("id" -> 4, "category" -> "B", "score" -> 50.0),
      Map("id" -> 5, "category" -> "B", "score" -> 60.0),
      Map("id" -> 6, "category" -> "B", "score" -> 70.0)))
    insertOp(w, Seq(
      Map("id" -> 7, "category" -> "C", "score" -> 90.0),
      Map("id" -> 8, "category" -> "C", "score" -> 95.0),
      Map("id" -> 9, "category" -> "C", "score" -> 100.0)))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    read(t, predicate = "score > 80.0", name = "read_score_gt_80")
    read(t, predicate = "category = 'B'", name = "read_category_B")
    read(t, predicate = "id <= 3", name = "read_id_le_3")
    snapshot(t)
  }

  // DSK-010: BETWEEN predicate
  test("dsk010_between_predicate",
      "BETWEEN predicate on non-overlapping ranges",
      "write", "insert", "dataSkipping") {
    sql("CREATE TABLE tbl (id INT, value STRING) USING delta")
    sql("INSERT INTO tbl SELECT id, CAST(id AS STRING) FROM RANGE(1, 51)")
    sql("INSERT INTO tbl SELECT id, CAST(id AS STRING) FROM RANGE(51, 101)")
    sql("INSERT INTO tbl SELECT id, CAST(id AS STRING) FROM RANGE(101, 151)")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    read(t, predicate = "id BETWEEN 60 AND 90", name = "read_between_60_90")
    read(t, predicate = "id BETWEEN 1 AND 50", name = "read_between_1_50")
    snapshot(t)
  }

  // DSK-011: IN predicate
  test("dsk011_in_predicate",
      "IN predicate selecting specific ids across files",
      "write", "insert", "dataSkipping") {
    sql("CREATE TABLE tbl (id INT, value STRING) USING delta")
    sql("INSERT INTO tbl SELECT id, CAST(id AS STRING) FROM RANGE(1, 11)")
    sql("INSERT INTO tbl SELECT id, CAST(id AS STRING) FROM RANGE(101, 111)")
    sql("INSERT INTO tbl SELECT id, CAST(id AS STRING) FROM RANGE(201, 211)")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    read(t, predicate = "id IN (1, 5, 10)", name = "read_in_low")
    read(t, predicate = "id IN (1, 105, 210)", name = "read_in_mixed")
    snapshot(t)
  }

  // DSK-012: Nested struct field predicate
  test("dsk012_nested_struct",
      "Nested struct field predicate on info.score",
      "write", "insert", "dataSkipping") {
    sql("CREATE TABLE tbl (id INT, info STRUCT<name: STRING, score: INT>) USING delta")
    sql("""INSERT INTO tbl VALUES
      (1, named_struct('name', 'Alice', 'score', 10)),
      (2, named_struct('name', 'Bob', 'score', 20))""")
    sql("""INSERT INTO tbl VALUES
      (3, named_struct('name', 'Carol', 'score', 50)),
      (4, named_struct('name', 'Dave', 'score', 60))""")
    sql("""INSERT INTO tbl VALUES
      (5, named_struct('name', 'Eve', 'score', 90)),
      (6, named_struct('name', 'Frank', 'score', 100))""")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    read(t, predicate = "info.score > 80", name = "read_high_score")
    read(t, predicate = "info.score <= 20", name = "read_low_score")
    snapshot(t)
  }

  // DSK-013: Large number of files (6 inserts) with selective predicate
  test("dsk013_many_files",
      "6 files with non-overlapping ranges, selective predicates",
      "write", "insert", "dataSkipping") {
    sql("CREATE TABLE tbl (id INT, value STRING) USING delta")
    sql("INSERT INTO tbl SELECT id, CAST(id AS STRING) FROM RANGE(1, 101)")
    sql("INSERT INTO tbl SELECT id, CAST(id AS STRING) FROM RANGE(101, 201)")
    sql("INSERT INTO tbl SELECT id, CAST(id AS STRING) FROM RANGE(201, 301)")
    sql("INSERT INTO tbl SELECT id, CAST(id AS STRING) FROM RANGE(301, 401)")
    sql("INSERT INTO tbl SELECT id, CAST(id AS STRING) FROM RANGE(401, 501)")
    sql("INSERT INTO tbl SELECT id, CAST(id AS STRING) FROM RANGE(501, 601)")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    read(t, predicate = "id > 500", name = "read_gt_500")
    read(t, predicate = "id <= 100", name = "read_le_100")
    read(t, predicate = "id BETWEEN 250 AND 350", name = "read_between_250_350")
    snapshot(t)
  }

  // DSK-014: Delete + read with predicate (DV interaction with data skipping)
  test("dsk014_delete_then_read",
      "Delete rows from middle file, verify skipping with DVs",
      "write", "insert", "delete", "dataSkipping", "dv") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id, CAST(id AS STRING) FROM RANGE(1, 51)")
    sql("INSERT INTO tbl SELECT id, CAST(id AS STRING) FROM RANGE(51, 101)")
    sql("INSERT INTO tbl SELECT id, CAST(id AS STRING) FROM RANGE(101, 151)")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    deleteOp(w, "id >= 60 AND id <= 70")
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    read(t, predicate = "id > 100", name = "read_gt_100")
    read(t, predicate = "id <= 50", name = "read_le_50")
    read(t, predicate = "id BETWEEN 55 AND 75", name = "read_between_55_75")
    read(t, version = 3, name = "read_before_delete")
    snapshot(t)
  }

  // DSK-015: Update + read with predicate (rewritten files + data skipping)
  test("dsk015_update_then_read",
      "Update rows in middle file, verify skipping after rewrite",
      "write", "insert", "update", "dataSkipping", "dv") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl SELECT id, CAST(id AS STRING) FROM RANGE(1, 51)")
    sql("INSERT INTO tbl SELECT id, CAST(id AS STRING) FROM RANGE(51, 101)")
    sql("INSERT INTO tbl SELECT id, CAST(id AS STRING) FROM RANGE(101, 151)")
    val base = registerTable("tbl")
    val w = writeSpec(base)
    updateOp(w, "id >= 60 AND id <= 70", Map("value" -> "'updated'"))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    read(t, predicate = "id > 100", name = "read_gt_100")
    read(t, predicate = "id <= 50", name = "read_le_50")
    read(t, predicate = "value = 'updated'", name = "read_updated")
    read(t, version = 3, name = "read_before_update")
    snapshot(t)
  }

}
