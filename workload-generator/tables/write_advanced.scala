/**
 * Advanced write workloads that verify write output is consumable by read engines.
 *
 * Covers: data skipping after writes, time travel after writes, and CDF reads after writes.
 * Each test produces a write_spec.json plus targeted read/snapshot/cdf verification specs
 * to confirm that written data is correctly queryable.
 */

new WorkloadSuite("write_advanced") {

  // === Data Skipping After Writes ===

  test("data_skipping_after_insert",
      "3 inserts with non-overlapping ranges, verify predicate pushdown skips files",
      "write", "insert", "dataSkipping") {
    val w = createTableOp("tbl",
      schema = "id INT, label STRING")
    insertOp(w, (1 until 11).map(i => Map("id" -> i, "label" -> "low")))       // ids 1-10
    insertOp(w, (100 until 111).map(i => Map("id" -> i, "label" -> "mid")))    // ids 100-110
    insertOp(w, (1000 until 1011).map(i => Map("id" -> i, "label" -> "high"))) // ids 1000-1010
    val t = registerWriteSpec(w)
    read(t, name = "read_all")                          // 33 rows
    read(t, predicate = "id < 50", name = "read_low")   // 10 rows (skips mid+high files)
    read(t, predicate = "id > 500", name = "read_high") // 11 rows (skips low+mid files)
    read(t, predicate = "id = 105", name = "read_exact_mid") // 1 row (skips low+high files)
    snapshot(t)
  }

  test("data_skipping_after_delete",
      "Insert 100 rows, delete rows > 50, verify reads with predicates",
      "write", "insert", "delete", "dataSkipping", "dv") {
    val w = createTableOp("tbl",
      schema = "id INT, value STRING",
      properties = Map("delta.enableDeletionVectors" -> "true"))
    insertOp(w, (1 until 101).map(i => Map("id" -> i, "value" -> s"val_$i"))) // ids 1-100
    deleteOp(w, "id > 50")
    val t = registerWriteSpec(w)
    read(t, name = "read_all")                               // 50 rows remaining
    read(t, predicate = "id > 75", name = "read_deleted_range")  // 0 rows (all deleted)
    read(t, predicate = "id <= 50", name = "read_surviving")     // 50 rows
    read(t, version = 1, name = "read_before_delete")            // 100 rows (pre-delete)
    snapshot(t)
  }

  test("skipping_partitioned_after_delete",
      "Partitioned table: insert to us/eu/asia, delete eu, verify partition pruning",
      "write", "insert", "delete", "dataSkipping", "partitioned", "dv") {
    val w = createTableOp("tbl",
      schema = "id INT, region STRING, amount INT",
      partitionColumns = Seq("region"),
      properties = Map("delta.enableDeletionVectors" -> "true"))
    insertOp(w, Seq(
      Map("id" -> 1, "region" -> "us", "amount" -> 100),
      Map("id" -> 2, "region" -> "us", "amount" -> 200),
      Map("id" -> 3, "region" -> "us", "amount" -> 300)))
    insertOp(w, Seq(
      Map("id" -> 4, "region" -> "eu", "amount" -> 400),
      Map("id" -> 5, "region" -> "eu", "amount" -> 500),
      Map("id" -> 6, "region" -> "eu", "amount" -> 600)))
    insertOp(w, Seq(
      Map("id" -> 7, "region" -> "asia", "amount" -> 700),
      Map("id" -> 8, "region" -> "asia", "amount" -> 800)))
    deleteOp(w, "region = 'eu'")
    val t = registerWriteSpec(w)
    read(t, name = "read_all")                                       // 5 rows (us + asia)
    read(t, predicate = "region = 'eu'", name = "read_eu_deleted")   // 0 rows
    read(t, predicate = "region = 'us'", name = "read_us")           // 3 rows (unchanged)
    read(t, predicate = "region = 'asia'", name = "read_asia")       // 2 rows (unchanged)
    read(t, version = 3, name = "read_before_delete")                // 8 rows (all regions)
    snapshot(t)
  }

  // === Time Travel After Writes ===

  test("time_travel_after_inserts",
      "3 cumulative inserts, read at each version to verify time travel",
      "write", "insert", "time_travel") {
    val w = createTableOp("tbl",
      schema = "id INT, name STRING")
    // v1: 2 rows
    insertOp(w, Seq(Map("id" -> 1, "name" -> "a"), Map("id" -> 2, "name" -> "b")))
    // v2: +3 rows = 5 total
    insertOp(w, Seq(
      Map("id" -> 3, "name" -> "c"),
      Map("id" -> 4, "name" -> "d"),
      Map("id" -> 5, "name" -> "e")))
    // v3: +5 rows = 10 total
    insertOp(w, Seq(
      Map("id" -> 6, "name" -> "f"),
      Map("id" -> 7, "name" -> "g"),
      Map("id" -> 8, "name" -> "h"),
      Map("id" -> 9, "name" -> "i"),
      Map("id" -> 10, "name" -> "j")))
    val t = registerWriteSpec(w)
    read(t, version = 1, name = "read_v1")   // 2 rows
    read(t, version = 2, name = "read_v2")   // 5 rows
    read(t, version = 3, name = "read_v3")   // 10 rows
    read(t, name = "read_latest")             // 10 rows
    snapshot(t)
    snapshot(t, version = 1)
    snapshot(t, version = 2)
    snapshot(t, version = 3)
  }

  test("time_travel_after_delete",
      "Insert 10 rows then delete id > 5, read before and after delete",
      "write", "insert", "delete", "time_travel", "dv") {
    val w = createTableOp("tbl",
      schema = "id INT, value STRING",
      properties = Map("delta.enableDeletionVectors" -> "true"))
    // v1: 10 rows
    insertOp(w, (1 until 11).map(i => Map("id" -> i, "value" -> s"val_$i")))
    // v2: delete id > 5 => 5 rows remain
    deleteOp(w, "id > 5")
    val t = registerWriteSpec(w)
    read(t, version = 1, name = "read_before_delete")  // 10 rows
    read(t, version = 2, name = "read_after_delete")    // 5 rows
    read(t, name = "read_latest")                       // 5 rows
    snapshot(t)
    snapshot(t, version = 1)
    snapshot(t, version = 2)
  }

  test("time_travel_after_update",
      "Insert 5 rows then update name where id=3, read before and after",
      "write", "insert", "update", "time_travel", "dv") {
    val w = createTableOp("tbl",
      schema = "id INT, name STRING",
      properties = Map("delta.enableDeletionVectors" -> "true"))
    // v1: 5 rows with original names
    insertOp(w, Seq(
      Map("id" -> 1, "name" -> "alice"),
      Map("id" -> 2, "name" -> "bob"),
      Map("id" -> 3, "name" -> "charlie"),
      Map("id" -> 4, "name" -> "dave"),
      Map("id" -> 5, "name" -> "eve")))
    // v2: update id=3
    updateOp(w, predicate = "id = 3", set = Map("name" -> "'CHARLIE_UPDATED'"))
    val t = registerWriteSpec(w)
    read(t, version = 1, name = "read_before_update")  // original values
    read(t, version = 2, name = "read_after_update")    // updated values
    read(t, name = "read_latest")                       // updated values
    read(t, predicate = "id = 3", name = "read_updated_row")  // 1 row with updated name
    snapshot(t)
    snapshot(t, version = 1)
    snapshot(t, version = 2)
  }

  // === CDF After Writes ===

  test("cdf_after_insert",
      "Enable CDF, insert 3 rows, verify CDF shows insert changes",
      "write", "insert", "cdf") {
    val w = createTableOp("tbl",
      schema = "id INT, name STRING",
      properties = Map("delta.enableChangeDataFeed" -> "true"))
    // v1: insert 3 rows
    insertOp(w, Seq(
      Map("id" -> 1, "name" -> "alice"),
      Map("id" -> 2, "name" -> "bob"),
      Map("id" -> 3, "name" -> "charlie")))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")                          // 3 rows
    read(t, version = 1, name = "read_v1")              // 3 rows
    cdf(t, startVersion = 1, endVersion = 1, name = "cdf_insert_only")  // 3 insert changes
    cdf(t, startVersion = 0, endVersion = 1, name = "cdf_full")
    snapshot(t)
  }

  test("cdf_after_delete",
      "Enable CDF, insert 5 rows, delete id > 3, verify CDF shows inserts + deletes",
      "write", "insert", "delete", "cdf", "dv") {
    val w = createTableOp("tbl",
      schema = "id INT, name STRING",
      properties = Map(
        "delta.enableChangeDataFeed" -> "true",
        "delta.enableDeletionVectors" -> "true"))
    // v1: insert 5 rows
    insertOp(w, Seq(
      Map("id" -> 1, "name" -> "alice"),
      Map("id" -> 2, "name" -> "bob"),
      Map("id" -> 3, "name" -> "charlie"),
      Map("id" -> 4, "name" -> "dave"),
      Map("id" -> 5, "name" -> "eve")))
    // v2: delete id > 3 => 3 rows remain
    deleteOp(w, "id > 3")
    val t = registerWriteSpec(w)
    read(t, name = "read_all")                                    // 3 rows
    read(t, version = 1, name = "read_before_delete")             // 5 rows
    read(t, version = 2, name = "read_after_delete")              // 3 rows
    cdf(t, startVersion = 1, endVersion = 1, name = "cdf_inserts")    // 5 insert changes
    cdf(t, startVersion = 2, endVersion = 2, name = "cdf_deletes")    // 2 delete changes
    cdf(t, startVersion = 1, endVersion = 2, name = "cdf_full")       // inserts + deletes
    snapshot(t)
  }

  test("cdf_after_update",
      "Enable CDF, insert 3 rows, update name where id=1, verify pre/post update in CDF",
      "write", "insert", "update", "cdf", "dv") {
    val w = createTableOp("tbl",
      schema = "id INT, name STRING",
      properties = Map(
        "delta.enableChangeDataFeed" -> "true",
        "delta.enableDeletionVectors" -> "true"))
    // v1: insert 3 rows
    insertOp(w, Seq(
      Map("id" -> 1, "name" -> "alice"),
      Map("id" -> 2, "name" -> "bob"),
      Map("id" -> 3, "name" -> "charlie")))
    // v2: update id=1
    updateOp(w, predicate = "id = 1", set = Map("name" -> "'ALICE_UPDATED'"))
    val t = registerWriteSpec(w)
    read(t, name = "read_all")                                    // 3 rows
    read(t, version = 1, name = "read_before_update")             // original values
    read(t, version = 2, name = "read_after_update")              // updated values
    read(t, predicate = "id = 1", name = "read_updated_row")     // 1 row with updated name
    cdf(t, startVersion = 1, endVersion = 1, name = "cdf_inserts")    // 3 insert changes
    cdf(t, startVersion = 2, endVersion = 2, name = "cdf_update")     // pre/post update
    cdf(t, startVersion = 1, endVersion = 2, name = "cdf_full")       // inserts + update
    snapshot(t)
  }

  test("cdf_combined",
      "Enable CDF, insert/update/delete/insert, CDF across full range plus version reads",
      "write", "insert", "update", "delete", "cdf", "dv", "combined") {
    val w = createTableOp("tbl",
      schema = "id INT, name STRING, amount INT",
      properties = Map(
        "delta.enableChangeDataFeed" -> "true",
        "delta.enableDeletionVectors" -> "true"))
    // v1: insert 5 rows
    insertOp(w, Seq(
      Map("id" -> 1, "name" -> "alice", "amount" -> 100),
      Map("id" -> 2, "name" -> "bob", "amount" -> 200),
      Map("id" -> 3, "name" -> "charlie", "amount" -> 300),
      Map("id" -> 4, "name" -> "dave", "amount" -> 400),
      Map("id" -> 5, "name" -> "eve", "amount" -> 500)))
    // v2: update id=2
    updateOp(w, predicate = "id = 2", set = Map("name" -> "'BOB_UPDATED'", "amount" -> "250"))
    // v3: delete id=4
    deleteOp(w, "id = 4")
    // v4: insert 2 more rows
    insertOp(w, Seq(
      Map("id" -> 6, "name" -> "frank", "amount" -> 600),
      Map("id" -> 7, "name" -> "grace", "amount" -> 700)))
    val t = registerWriteSpec(w)
    // Read at every meaningful version
    read(t, version = 1, name = "read_v1_after_insert")      // 5 rows
    read(t, version = 2, name = "read_v2_after_update")      // 5 rows (same count, bob updated)
    read(t, version = 3, name = "read_v3_after_delete")      // 4 rows (dave deleted)
    read(t, version = 4, name = "read_v4_after_insert2")     // 6 rows
    read(t, name = "read_latest")                             // 6 rows
    // CDF at each operation version
    cdf(t, startVersion = 1, endVersion = 1, name = "cdf_v1_inserts")
    cdf(t, startVersion = 2, endVersion = 2, name = "cdf_v2_update")
    cdf(t, startVersion = 3, endVersion = 3, name = "cdf_v3_delete")
    cdf(t, startVersion = 4, endVersion = 4, name = "cdf_v4_inserts")
    // CDF across full range
    cdf(t, startVersion = 1, endVersion = 4, name = "cdf_full_range")
    // CDF partial ranges
    cdf(t, startVersion = 1, endVersion = 2, name = "cdf_insert_and_update")
    cdf(t, startVersion = 2, endVersion = 4, name = "cdf_update_to_end")
    snapshot(t)
    snapshot(t, version = 1)
    snapshot(t, version = 4)
  }

}
