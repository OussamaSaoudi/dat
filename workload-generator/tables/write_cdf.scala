/**
 * Write workloads with Change Data Feed (CDF) verification.
 *
 * Converted from CDFWriteCapture.scala (CDF-W001 through CDF-W008).
 * Each test uses the structured *Op API to build write specs, then
 * verifies both the table state (read/snapshot) and CDF output.
 *
 * Converted from internal CDF write capture tests.
 */

new WorkloadSuite("write_cdf") {

  // CDF-W001: INSERT with CDF enabled - read changes from startVersion=1
  test("cdf_insert", "Insert rows into CDF-enabled table, verify CDF at insert version",
      "write", "cdf", "insert") {
    val w = createTableOp("tbl",
      schema = "id INT, value STRING",
      properties = Map("delta.enableChangeDataFeed" -> "true"))
    insertOp(w, Seq(
      Map("id" -> 1, "value" -> "a"),
      Map("id" -> 2, "value" -> "b"),
      Map("id" -> 3, "value" -> "c"),
      Map("id" -> 4, "value" -> "d"),
      Map("id" -> 5, "value" -> "e")))
    val t = registerWriteSpec(w)
    read(t)
    cdf(t, startVersion = 1, endVersion = 1)
    snapshot(t)
  }

  // CDF-W002: UPDATE with CDF - pre-image and post-image
  test("cdf_update", "Insert then update rows, verify CDF captures pre/post images",
      "write", "cdf", "update") {
    val w = createTableOp("tbl",
      schema = "id INT, value STRING",
      properties = Map("delta.enableChangeDataFeed" -> "true"))
    insertOp(w, Seq(
      Map("id" -> 1, "value" -> "a"),
      Map("id" -> 2, "value" -> "b"),
      Map("id" -> 3, "value" -> "c"),
      Map("id" -> 4, "value" -> "d"),
      Map("id" -> 5, "value" -> "e")))
    updateOp(w, "id <= 3", Map("value" -> "'updated'"))
    val t = registerWriteSpec(w)
    read(t)
    cdf(t, startVersion = 2, endVersion = 2)
    snapshot(t)
  }

  // CDF-W003: DELETE with CDF - deleted rows
  test("cdf_delete", "Insert then delete rows, verify CDF shows deletes",
      "write", "cdf", "delete") {
    val w = createTableOp("tbl",
      schema = "id INT, value STRING",
      properties = Map("delta.enableChangeDataFeed" -> "true"))
    insertOp(w, Seq(
      Map("id" -> 1, "value" -> "a"),
      Map("id" -> 2, "value" -> "b"),
      Map("id" -> 3, "value" -> "c"),
      Map("id" -> 4, "value" -> "d"),
      Map("id" -> 5, "value" -> "e")))
    deleteOp(w, "id > 3")
    val t = registerWriteSpec(w)
    read(t)
    cdf(t, startVersion = 2, endVersion = 2)
    snapshot(t)
  }

  // CDF-W004: MERGE with CDF - inserts, updates, deletes from merge
  // NOTE: MERGE is not supported as a structured operation - test commented out
  // test("cdf_merge", "MERGE with all clauses on CDF-enabled table",
  //     "write", "cdf", "merge") {
  //   val w = createTableOp("tbl",
  //     schema = "id INT, value STRING",
  //     properties = Map("delta.enableChangeDataFeed" -> "true"))
  //   insertOp(w, Seq(
  //     Map("id" -> 1, "value" -> "a"),
  //     Map("id" -> 2, "value" -> "b"),
  //     Map("id" -> 3, "value" -> "c"),
  //     Map("id" -> 4, "value" -> "d")))
  //   // Source has: (2, 'updated'), (3, 'updated'), (5, 'new'), (6, 'new')
  //   sql("CREATE TABLE src (id INT, value STRING) USING delta")
  //   sql("INSERT INTO src VALUES (2,'updated'),(3,'updated'),(5,'new'),(6,'new')")
  //   sql("""MERGE INTO tbl AS target
  //     USING src AS source ON target.id = source.id
  //     WHEN MATCHED THEN UPDATE SET target.value = source.value
  //     WHEN NOT MATCHED THEN INSERT (id, value) VALUES (source.id, source.value)
  //     WHEN NOT MATCHED BY SOURCE AND target.id = 1 THEN DELETE""")
  //   val t = registerWriteSpec(w)
  //   read(t)
  //   read(t, version = 1, name = "read_before_merge")
  //   cdf(t, startVersion = 2, endVersion = 2)
  //   snapshot(t)
  // }

  // CDF-W005: Multiple operations - CDF for specific version range
  test("cdf_multi_op_version_range",
      "Insert/update/delete/insert, CDF for specific version ranges",
      "write", "cdf", "insert", "update", "delete") {
    val w = createTableOp("tbl",
      schema = "id INT, value INT",
      properties = Map("delta.enableChangeDataFeed" -> "true"))
    // v1: INSERT
    insertOp(w, Seq(
      Map("id" -> 1, "value" -> 10),
      Map("id" -> 2, "value" -> 20),
      Map("id" -> 3, "value" -> 30),
      Map("id" -> 4, "value" -> 40),
      Map("id" -> 5, "value" -> 50)))
    // v2: UPDATE
    updateOp(w, "id <= 2", Map("value" -> "value + 100"))
    // v3: DELETE
    deleteOp(w, "id = 5")
    // v4: INSERT more
    insertOp(w, Seq(
      Map("id" -> 6, "value" -> 60),
      Map("id" -> 7, "value" -> 70)))
    val t = registerWriteSpec(w)
    read(t)
    read(t, version = 1, name = "read_after_insert")
    read(t, version = 2, name = "read_after_update")
    read(t, version = 3, name = "read_after_delete")
    // CDF from v2 (UPDATE) to v3 (DELETE) only
    cdf(t, startVersion = 2, endVersion = 3, name = "cdf_v2_to_v3")
    // CDF across all data versions
    cdf(t, startVersion = 1, name = "cdf_all")
    snapshot(t)
  }

  // CDF-W006: Partitioned table with CDF
  test("cdf_partitioned_delete",
      "Partitioned table with CDF, delete one partition",
      "write", "cdf", "partitioned", "delete") {
    val w = createTableOp("tbl",
      schema = "id INT, value STRING, part STRING",
      properties = Map("delta.enableChangeDataFeed" -> "true"),
      partitionColumns = Seq("part"))
    insertOp(w, Seq(
      Map("id" -> 1, "value" -> "a", "part" -> "p1"),
      Map("id" -> 2, "value" -> "b", "part" -> "p1"),
      Map("id" -> 3, "value" -> "c", "part" -> "p2"),
      Map("id" -> 4, "value" -> "d", "part" -> "p2"),
      Map("id" -> 5, "value" -> "e", "part" -> "p3")))
    deleteOp(w, "part = 'p1'")
    val t = registerWriteSpec(w)
    read(t)
    read(t, version = 1, name = "read_before_delete")
    read(t, predicate = "part = 'p2'", name = "read_p2")
    cdf(t, startVersion = 2, endVersion = 2)
    snapshot(t)
  }

  // CDF-W007: CDF with predicate filter on data columns
  test("cdf_predicate_filter",
      "Multiple DML ops then CDF with predicate filter on data columns",
      "write", "cdf", "predicate", "update", "delete") {
    val w = createTableOp("tbl",
      schema = "id INT, value INT",
      properties = Map("delta.enableChangeDataFeed" -> "true"))
    insertOp(w, Seq(
      Map("id" -> 1, "value" -> 10),
      Map("id" -> 2, "value" -> 20),
      Map("id" -> 3, "value" -> 30),
      Map("id" -> 4, "value" -> 40)))
    updateOp(w, "1 = 1", Map("value" -> "value * 10"))
    deleteOp(w, "id = 4")
    val t = registerWriteSpec(w)
    read(t)
    cdf(t, startVersion = 1, name = "cdf_all")
    cdf(t, startVersion = 1, predicate = "id <= 2", name = "cdf_filtered_by_id")
    snapshot(t)
  }

  // CDF-W008: INSERT OVERWRITE with CDF on partitioned table
  // NOTE: INSERT OVERWRITE is not supported as a structured operation - test commented out
  // test("cdf_insert_overwrite",
  //     "INSERT OVERWRITE partition on CDF-enabled partitioned table",
  //     "write", "cdf", "insert_overwrite", "partitioned") {
  //   val w = createTableOp("tbl",
  //     schema = "id INT, value STRING, part STRING",
  //     properties = Map("delta.enableChangeDataFeed" -> "true"),
  //     partitionColumns = Seq("part"))
  //   insertOp(w, Seq(
  //     Map("id" -> 1, "value" -> "a", "part" -> "p1"),
  //     Map("id" -> 2, "value" -> "b", "part" -> "p1"),
  //     Map("id" -> 3, "value" -> "c", "part" -> "p2"),
  //     Map("id" -> 4, "value" -> "d", "part" -> "p2")))
  //   // INSERT OVERWRITE partition p1
  //   sql("INSERT OVERWRITE TABLE tbl PARTITION (part = 'p1') VALUES (10, 'x'), (20, 'y')")
  //   val t = registerWriteSpec(w)
  //   read(t)
  //   read(t, version = 1, name = "read_before_overwrite")
  //   read(t, predicate = "part = 'p1'", name = "read_p1_after_overwrite")
  //   cdf(t, startVersion = 2, endVersion = 2, name = "cdf_overwrite")
  //   snapshot(t)
  // }

}
