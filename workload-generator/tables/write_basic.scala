/**
 * Basic write workloads that produce write_spec.json files for kernel write testing.
 *
 * Each test generates a write spec (commit history) plus read/snapshot verification specs.
 * Covers: create, insert, delete, update, alter table, truncate, partitioning, DVs.
 */

new WorkloadSuite("write_basic") {

  test("create_and_read", "Create table, insert rows, verify read", "write", "create", "insert") {
    val w = createTableOp("tbl",
      schema = "id INT, name STRING, score DOUBLE")
    insertOp(w, Seq(
      Map("id" -> 1, "name" -> "alice", "score" -> 95.5),
      Map("id" -> 2, "name" -> "bob", "score" -> 87.3),
      Map("id" -> 3, "name" -> "charlie", "score" -> 72.1)))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    read(t, predicate = "score > 90.0", name = "read_high_score")
    snapshot(t)
  }

  test("create_with_properties", "Create with DV + CDF properties, verify protocol/metadata",
      "write", "create", "dv", "cdf") {
    val w = createTableOp("tbl",
      schema = "id INT, value STRING, amount INT",
      properties = Map(
        "delta.enableDeletionVectors" -> "true",
        "delta.enableChangeDataFeed" -> "true"))
    insertOp(w, Seq(
      Map("id" -> 1, "value" -> "alpha", "amount" -> 100),
      Map("id" -> 2, "value" -> "beta", "amount" -> 200),
      Map("id" -> 3, "value" -> "gamma", "amount" -> 300)))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    cdf(t, startVersion = 0, endVersion = 1)
    snapshot(t)
  }

  test("insert_multiple", "Three separate inserts, verify cumulative rows",
      "write", "insert") {
    val w = createTableOp("tbl",
      schema = "id INT, category STRING")
    insertOp(w, Seq(
      Map("id" -> 1, "category" -> "a"),
      Map("id" -> 2, "category" -> "a")))
    insertOp(w, Seq(
      Map("id" -> 3, "category" -> "b"),
      Map("id" -> 4, "category" -> "b"),
      Map("id" -> 5, "category" -> "b")))
    insertOp(w, Seq(
      Map("id" -> 6, "category" -> "c")))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    read(t, version = 1, name = "read_v1")
    read(t, version = 2, name = "read_v2")
    read(t, version = 3, name = "read_v3")
    read(t, predicate = "category = 'b'", name = "read_category_b")
    snapshot(t)
  }

  test("delete_basic", "Insert then delete rows with WHERE clause",
      "write", "delete", "dv") {
    val w = createTableOp("tbl",
      schema = "id INT, value STRING, amount INT",
      properties = Map("delta.enableDeletionVectors" -> "true"))
    insertOp(w, Seq(
      Map("id" -> 1, "value" -> "keep", "amount" -> 10),
      Map("id" -> 2, "value" -> "remove", "amount" -> 20),
      Map("id" -> 3, "value" -> "keep", "amount" -> 30),
      Map("id" -> 4, "value" -> "remove", "amount" -> 40),
      Map("id" -> 5, "value" -> "keep", "amount" -> 50)))
    deleteOp(w, predicate = "value = 'remove'")
    val t = registerWriteSpec(w)
    read(t, name = "read_after_delete")
    read(t, version = 1, name = "read_before_delete")
    read(t, predicate = "amount > 25", name = "read_large_amount")
    snapshot(t)
  }

  test("update_basic", "Insert then update rows with SET",
      "write", "update", "dv") {
    val w = createTableOp("tbl",
      schema = "id INT, status STRING, count INT",
      properties = Map("delta.enableDeletionVectors" -> "true"))
    insertOp(w, Seq(
      Map("id" -> 1, "status" -> "pending", "count" -> 0),
      Map("id" -> 2, "status" -> "pending", "count" -> 0),
      Map("id" -> 3, "status" -> "active", "count" -> 5),
      Map("id" -> 4, "status" -> "pending", "count" -> 0)))
    updateOp(w, predicate = "status = 'pending'", set = Map("status" -> "'active'", "count" -> "count + 1"))
    val t = registerWriteSpec(w)
    read(t, name = "read_after_update")
    read(t, version = 1, name = "read_before_update")
    read(t, predicate = "status = 'active'", name = "read_active")
    snapshot(t)
  }

  test("alter_add_column", "Add column then insert with new schema",
      "write", "alter_table", "schema_evolution") {
    val w = createTableOp("tbl",
      schema = "id INT, name STRING")
    insertOp(w, Seq(
      Map("id" -> 1, "name" -> "alice"),
      Map("id" -> 2, "name" -> "bob")))
    addColumnsOp(w, "email STRING")
    insertOp(w, Seq(
      Map("id" -> 3, "name" -> "charlie", "email" -> "charlie@test.com"),
      Map("id" -> 4, "name" -> "diana", "email" -> "diana@test.com")))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    read(t, version = 1, name = "read_before_alter")
    read(t, predicate = "email IS NULL", name = "read_null_email")
    read(t, predicate = "email IS NOT NULL", name = "read_with_email")
    snapshot(t)
  }

  test("alter_set_properties", "Enable CDF via SET TBLPROPERTIES then verify CDF works",
      "write", "alter_table", "cdf") {
    val w = createTableOp("tbl",
      schema = "id INT, value INT")
    insertOp(w, Seq(
      Map("id" -> 1, "value" -> 100),
      Map("id" -> 2, "value" -> 200),
      Map("id" -> 3, "value" -> 300)))
    setPropertiesOp(w, Map("delta.enableChangeDataFeed" -> "true"))
    insertOp(w, Seq(
      Map("id" -> 4, "value" -> 400),
      Map("id" -> 5, "value" -> 500)))
    updateOp(w, predicate = "id <= 2", set = Map("value" -> "value + 1000"))
    val t = registerWriteSpec(w)
    read(t, version = 1, name = "read_initial_insert")
    read(t, version = 3, name = "read_before_update")
    read(t, name = "read_all")
    read(t, predicate = "value > 1000", name = "read_updated")
    // CDF only available from version 3 onward (after enableChangeDataFeed was set)
    cdf(t, startVersion = 3, endVersion = 4)
    snapshot(t)
  }

  test("truncate_table", "Insert rows then delete all to empty table",
      "write", "truncate") {
    val w = createTableOp("tbl",
      schema = "id INT, data STRING",
      properties = Map("delta.enableDeletionVectors" -> "true"))
    insertOp(w, Seq(
      Map("id" -> 1, "data" -> "first"),
      Map("id" -> 2, "data" -> "second"),
      Map("id" -> 3, "data" -> "third"),
      Map("id" -> 4, "data" -> "fourth")))
    deleteOp(w, predicate = "true")
    val t = registerWriteSpec(w)
    read(t, name = "read_after_truncate")
    read(t, version = 1, name = "read_before_truncate")
    snapshot(t)
  }

  test("partitioned_insert", "Partitioned table with inserts to different partitions",
      "write", "insert", "partitioned") {
    val w = createTableOp("tbl",
      schema = "id INT, region STRING, revenue INT",
      partitionColumns = Seq("region"))
    insertOp(w, Seq(
      Map("id" -> 1, "region" -> "east", "revenue" -> 100),
      Map("id" -> 2, "region" -> "east", "revenue" -> 150)))
    insertOp(w, Seq(
      Map("id" -> 3, "region" -> "west", "revenue" -> 200),
      Map("id" -> 4, "region" -> "west", "revenue" -> 250),
      Map("id" -> 5, "region" -> "west", "revenue" -> 300)))
    insertOp(w, Seq(
      Map("id" -> 6, "region" -> "north", "revenue" -> 400)))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")
    read(t, predicate = "region = 'east'", name = "read_east")
    read(t, predicate = "region = 'west'", name = "read_west")
    read(t, predicate = "region = 'north'", name = "read_north")
    read(t, predicate = "revenue >= 200", name = "read_high_revenue")
    snapshot(t)
  }

  test("delete_with_dvs", "DV-enabled table with targeted deletes",
      "write", "delete", "dv") {
    val w = createTableOp("tbl",
      schema = "id INT, name STRING, score INT",
      properties = Map("delta.enableDeletionVectors" -> "true"))
    insertOp(w, Seq(
      Map("id" -> 1, "name" -> "alice", "score" -> 90),
      Map("id" -> 2, "name" -> "bob", "score" -> 75),
      Map("id" -> 3, "name" -> "charlie", "score" -> 88),
      Map("id" -> 4, "name" -> "diana", "score" -> 92),
      Map("id" -> 5, "name" -> "eve", "score" -> 60),
      Map("id" -> 6, "name" -> "frank", "score" -> 85)))
    deleteOp(w, predicate = "score < 80")
    val t = registerWriteSpec(w)
    read(t, name = "read_after_delete")
    read(t, version = 1, name = "read_before_delete")
    read(t, predicate = "score >= 85", name = "read_high_score")
    snapshot(t)
  }

}
