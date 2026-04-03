/**
 * Write workloads with Change Data Feed (CDF) verification.
 *
 * Converted from CDFWriteCapture.scala (CDF-W001 through CDF-W008).
 * Each test uses the structured *Op API to build write specs, then
 * verifies both the table state (read/snapshot) and CDF output.
 *
 * Source: sql/core/src/test/scala/com/databricks/sql/transaction/tahoe/
 *         benchmarks/exact/CDFWriteCapture.scala
 */

new WorkloadSuite("write_cdf") {

  // CDF-W001: INSERT with CDF enabled - read changes from startVersion=1
  test("cdf_insert", "Insert rows into CDF-enabled table, verify CDF at insert version",
      "write", "cdf", "insert") { w =>
    val t = w.createTableOp("tbl",
      schema = Seq(Col("id", "INT"), Col("value", "STRING")),
      properties = Map("delta.enableChangeDataFeed" -> "true"))
    w.insertOp(t, Seq(
      Map("id" -> 1, "value" -> "a"),
      Map("id" -> 2, "value" -> "b"),
      Map("id" -> 3, "value" -> "c"),
      Map("id" -> 4, "value" -> "d"),
      Map("id" -> 5, "value" -> "e")))
    w.writeSpec(t)
    w.read(t)
    w.cdf(t, startVersion = 1, endVersion = 1)
    w.snapshot(t)
  }

  // CDF-W002: UPDATE with CDF - pre-image and post-image
  test("cdf_update", "Insert then update rows, verify CDF captures pre/post images",
      "write", "cdf", "update") { w =>
    val t = w.createTableOp("tbl",
      schema = Seq(Col("id", "INT"), Col("value", "STRING")),
      properties = Map("delta.enableChangeDataFeed" -> "true"))
    w.insertOp(t, Seq(
      Map("id" -> 1, "value" -> "a"),
      Map("id" -> 2, "value" -> "b"),
      Map("id" -> 3, "value" -> "c"),
      Map("id" -> 4, "value" -> "d"),
      Map("id" -> 5, "value" -> "e")))
    w.updateOp(t, "id <= 3", Map("value" -> "'updated'"))
    w.writeSpec(t)
    w.read(t)
    w.cdf(t, startVersion = 2, endVersion = 2)
    w.snapshot(t)
  }

  // CDF-W003: DELETE with CDF - deleted rows
  test("cdf_delete", "Insert then delete rows, verify CDF shows deletes",
      "write", "cdf", "delete") { w =>
    val t = w.createTableOp("tbl",
      schema = Seq(Col("id", "INT"), Col("value", "STRING")),
      properties = Map("delta.enableChangeDataFeed" -> "true"))
    w.insertOp(t, Seq(
      Map("id" -> 1, "value" -> "a"),
      Map("id" -> 2, "value" -> "b"),
      Map("id" -> 3, "value" -> "c"),
      Map("id" -> 4, "value" -> "d"),
      Map("id" -> 5, "value" -> "e")))
    w.deleteOp(t, "id > 3")
    w.writeSpec(t)
    w.read(t)
    w.cdf(t, startVersion = 2, endVersion = 2)
    w.snapshot(t)
  }

  // CDF-W004: MERGE with CDF - inserts, updates, deletes from merge
  test("cdf_merge", "MERGE with all clauses on CDF-enabled table",
      "write", "cdf", "merge") { w =>
    val t = w.createTableOp("tbl",
      schema = Seq(Col("id", "INT"), Col("value", "STRING")),
      properties = Map("delta.enableChangeDataFeed" -> "true"))
    w.insertOp(t, Seq(
      Map("id" -> 1, "value" -> "a"),
      Map("id" -> 2, "value" -> "b"),
      Map("id" -> 3, "value" -> "c"),
      Map("id" -> 4, "value" -> "d")))
    // Source has: (2, 'updated'), (3, 'updated'), (5, 'new'), (6, 'new')
    w.sql("CREATE TABLE src (id INT, value STRING) USING delta")
    w.sql("INSERT INTO src VALUES (2,'updated'),(3,'updated'),(5,'new'),(6,'new')")
    w.sql("""MERGE INTO tbl AS target
      USING src AS source ON target.id = source.id
      WHEN MATCHED THEN UPDATE SET target.value = source.value
      WHEN NOT MATCHED THEN INSERT (id, value) VALUES (source.id, source.value)
      WHEN NOT MATCHED BY SOURCE AND target.id = 1 THEN DELETE""")
    w.writeSpec(t)
    w.read(t)
    w.read(t, version = 1, name = "read_before_merge")
    w.cdf(t, startVersion = 2, endVersion = 2)
    w.snapshot(t)
  }

  // CDF-W005: Multiple operations - CDF for specific version range
  test("cdf_multi_op_version_range",
      "Insert/update/delete/insert, CDF for specific version ranges",
      "write", "cdf", "insert", "update", "delete") { w =>
    val t = w.createTableOp("tbl",
      schema = Seq(Col("id", "INT"), Col("value", "INT")),
      properties = Map("delta.enableChangeDataFeed" -> "true"))
    // v1: INSERT
    w.insertOp(t, Seq(
      Map("id" -> 1, "value" -> 10),
      Map("id" -> 2, "value" -> 20),
      Map("id" -> 3, "value" -> 30),
      Map("id" -> 4, "value" -> 40),
      Map("id" -> 5, "value" -> 50)))
    // v2: UPDATE
    w.updateOp(t, "id <= 2", Map("value" -> "value + 100"))
    // v3: DELETE
    w.deleteOp(t, "id = 5")
    // v4: INSERT more
    w.insertOp(t, Seq(
      Map("id" -> 6, "value" -> 60),
      Map("id" -> 7, "value" -> 70)))
    w.writeSpec(t)
    w.read(t)
    w.read(t, version = 1, name = "read_after_insert")
    w.read(t, version = 2, name = "read_after_update")
    w.read(t, version = 3, name = "read_after_delete")
    // CDF from v2 (UPDATE) to v3 (DELETE) only
    w.cdf(t, startVersion = 2, endVersion = 3, name = "cdf_v2_to_v3")
    // CDF across all data versions
    w.cdf(t, startVersion = 1, name = "cdf_all")
    w.snapshot(t)
  }

  // CDF-W006: Partitioned table with CDF
  test("cdf_partitioned_delete",
      "Partitioned table with CDF, delete one partition",
      "write", "cdf", "partitioned", "delete") { w =>
    val t = w.createTableOp("tbl",
      schema = Seq(Col("id", "INT"), Col("value", "STRING"), Col("part", "STRING")),
      properties = Map("delta.enableChangeDataFeed" -> "true"),
      partitionColumns = Seq("part"))
    w.insertOp(t, Seq(
      Map("id" -> 1, "value" -> "a", "part" -> "p1"),
      Map("id" -> 2, "value" -> "b", "part" -> "p1"),
      Map("id" -> 3, "value" -> "c", "part" -> "p2"),
      Map("id" -> 4, "value" -> "d", "part" -> "p2"),
      Map("id" -> 5, "value" -> "e", "part" -> "p3")))
    w.deleteOp(t, "part = 'p1'")
    w.writeSpec(t)
    w.read(t)
    w.read(t, version = 1, name = "read_before_delete")
    w.read(t, predicate = "part = 'p2'", name = "read_p2")
    w.cdf(t, startVersion = 2, endVersion = 2)
    w.snapshot(t)
  }

  // CDF-W007: CDF with predicate filter on data columns
  test("cdf_predicate_filter",
      "Multiple DML ops then CDF with predicate filter on data columns",
      "write", "cdf", "predicate", "update", "delete") { w =>
    val t = w.createTableOp("tbl",
      schema = Seq(Col("id", "INT"), Col("value", "INT")),
      properties = Map("delta.enableChangeDataFeed" -> "true"))
    w.insertOp(t, Seq(
      Map("id" -> 1, "value" -> 10),
      Map("id" -> 2, "value" -> 20),
      Map("id" -> 3, "value" -> 30),
      Map("id" -> 4, "value" -> 40)))
    w.updateOp(t, "1 = 1", Map("value" -> "value * 10"))
    w.deleteOp(t, "id = 4")
    w.writeSpec(t)
    w.read(t)
    w.cdf(t, startVersion = 1, name = "cdf_all")
    w.cdf(t, startVersion = 1, predicate = "id <= 2", name = "cdf_filtered_by_id")
    w.snapshot(t)
  }

  // CDF-W008: INSERT OVERWRITE with CDF on partitioned table
  test("cdf_insert_overwrite",
      "INSERT OVERWRITE partition on CDF-enabled partitioned table",
      "write", "cdf", "insert_overwrite", "partitioned") { w =>
    val t = w.createTableOp("tbl",
      schema = Seq(Col("id", "INT"), Col("value", "STRING"), Col("part", "STRING")),
      properties = Map("delta.enableChangeDataFeed" -> "true"),
      partitionColumns = Seq("part"))
    w.insertOp(t, Seq(
      Map("id" -> 1, "value" -> "a", "part" -> "p1"),
      Map("id" -> 2, "value" -> "b", "part" -> "p1"),
      Map("id" -> 3, "value" -> "c", "part" -> "p2"),
      Map("id" -> 4, "value" -> "d", "part" -> "p2")))
    // INSERT OVERWRITE partition p1
    w.sql("INSERT OVERWRITE TABLE tbl PARTITION (part = 'p1') VALUES (10, 'x'), (20, 'y')")
    w.writeSpec(t)
    w.read(t)
    w.read(t, version = 1, name = "read_before_overwrite")
    w.read(t, predicate = "part = 'p1'", name = "read_p1_after_overwrite")
    w.cdf(t, startVersion = 2, endVersion = 2, name = "cdf_overwrite")
    w.snapshot(t)
  }

}.runAll()
