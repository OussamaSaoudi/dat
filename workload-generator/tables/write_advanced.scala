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
      "write", "insert", "dataSkipping") { w =>
    w.sql("CREATE TABLE tbl (id INT, label STRING) USING delta")
    w.sql("INSERT INTO tbl SELECT id, 'low' FROM range(1, 11)")       // ids 1-10
    w.sql("INSERT INTO tbl SELECT id, 'mid' FROM range(100, 111)")    // ids 100-110
    w.sql("INSERT INTO tbl SELECT id, 'high' FROM range(1000, 1011)") // ids 1000-1010
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")                          // 33 rows
    w.read(t, predicate = "id < 50", name = "read_low")   // 10 rows (skips mid+high files)
    w.read(t, predicate = "id > 500", name = "read_high") // 11 rows (skips low+mid files)
    w.read(t, predicate = "id = 105", name = "read_exact_mid") // 1 row (skips low+high files)
    w.snapshot(t)
  }

  test("data_skipping_after_delete",
      "Insert 100 rows, delete rows > 50, verify reads with predicates",
      "write", "insert", "delete", "dataSkipping", "dv") { w =>
    w.sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl SELECT id, concat('val_', id) FROM range(1, 101)") // ids 1-100
    w.sql("DELETE FROM tbl WHERE id > 50")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")                               // 50 rows remaining
    w.read(t, predicate = "id > 75", name = "read_deleted_range")  // 0 rows (all deleted)
    w.read(t, predicate = "id <= 50", name = "read_surviving")     // 50 rows
    w.read(t, version = 1, name = "read_before_delete")            // 100 rows (pre-delete)
    w.snapshot(t)
  }

  test("skipping_partitioned_after_delete",
      "Partitioned table: insert to us/eu/asia, delete eu, verify partition pruning",
      "write", "insert", "delete", "dataSkipping", "partitioned", "dv") { w =>
    w.sql("""CREATE TABLE tbl (id INT, region STRING, amount INT) USING delta
      PARTITIONED BY (region) TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1,'us',100),(2,'us',200),(3,'us',300)")
    w.sql("INSERT INTO tbl VALUES (4,'eu',400),(5,'eu',500),(6,'eu',600)")
    w.sql("INSERT INTO tbl VALUES (7,'asia',700),(8,'asia',800)")
    w.sql("DELETE FROM tbl WHERE region = 'eu'")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")                                       // 5 rows (us + asia)
    w.read(t, predicate = "region = 'eu'", name = "read_eu_deleted")   // 0 rows
    w.read(t, predicate = "region = 'us'", name = "read_us")           // 3 rows (unchanged)
    w.read(t, predicate = "region = 'asia'", name = "read_asia")       // 2 rows (unchanged)
    w.read(t, version = 3, name = "read_before_delete")                // 8 rows (all regions)
    w.snapshot(t)
  }

  // === Time Travel After Writes ===

  test("time_travel_after_inserts",
      "3 cumulative inserts, read at each version to verify time travel",
      "write", "insert", "time_travel") { w =>
    w.sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
    // v1: 2 rows
    w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b')")
    // v2: +3 rows = 5 total
    w.sql("INSERT INTO tbl VALUES (3,'c'),(4,'d'),(5,'e')")
    // v3: +5 rows = 10 total
    w.sql("INSERT INTO tbl VALUES (6,'f'),(7,'g'),(8,'h'),(9,'i'),(10,'j')")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, version = 1, name = "read_v1")   // 2 rows
    w.read(t, version = 2, name = "read_v2")   // 5 rows
    w.read(t, version = 3, name = "read_v3")   // 10 rows
    w.read(t, name = "read_latest")             // 10 rows
    w.snapshot(t)
    w.snapshot(t, version = 1)
    w.snapshot(t, version = 2)
    w.snapshot(t, version = 3)
  }

  test("time_travel_after_delete",
      "Insert 10 rows then delete id > 5, read before and after delete",
      "write", "insert", "delete", "time_travel", "dv") { w =>
    w.sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    // v1: 10 rows
    w.sql("INSERT INTO tbl SELECT id, concat('val_', id) FROM range(1, 11)")
    // v2: delete id > 5 => 5 rows remain
    w.sql("DELETE FROM tbl WHERE id > 5")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, version = 1, name = "read_before_delete")  // 10 rows
    w.read(t, version = 2, name = "read_after_delete")    // 5 rows
    w.read(t, name = "read_latest")                       // 5 rows
    w.snapshot(t)
    w.snapshot(t, version = 1)
    w.snapshot(t, version = 2)
  }

  test("time_travel_after_update",
      "Insert 5 rows then update name where id=3, read before and after",
      "write", "insert", "update", "time_travel", "dv") { w =>
    w.sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    // v1: 5 rows with original names
    w.sql("INSERT INTO tbl VALUES (1,'alice'),(2,'bob'),(3,'charlie'),(4,'dave'),(5,'eve')")
    // v2: update id=3
    w.sql("UPDATE tbl SET name = 'CHARLIE_UPDATED' WHERE id = 3")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, version = 1, name = "read_before_update")  // original values
    w.read(t, version = 2, name = "read_after_update")    // updated values
    w.read(t, name = "read_latest")                       // updated values
    w.read(t, predicate = "id = 3", name = "read_updated_row")  // 1 row with updated name
    w.snapshot(t)
    w.snapshot(t, version = 1)
    w.snapshot(t, version = 2)
  }

  // === CDF After Writes ===

  test("cdf_after_insert",
      "Enable CDF, insert 3 rows, verify CDF shows insert changes",
      "write", "insert", "cdf") { w =>
    w.sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
      TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')""")
    // v1: insert 3 rows
    w.sql("INSERT INTO tbl VALUES (1,'alice'),(2,'bob'),(3,'charlie')")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")                          // 3 rows
    w.read(t, version = 1, name = "read_v1")              // 3 rows
    w.cdf(t, startVersion = 1, endVersion = 1, name = "cdf_insert_only")  // 3 insert changes
    w.cdf(t, startVersion = 0, endVersion = 1, name = "cdf_full")
    w.snapshot(t)
  }

  test("cdf_after_delete",
      "Enable CDF, insert 5 rows, delete id > 3, verify CDF shows inserts + deletes",
      "write", "insert", "delete", "cdf", "dv") { w =>
    w.sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
      TBLPROPERTIES (
        'delta.enableChangeDataFeed' = 'true',
        'delta.enableDeletionVectors' = 'true'
      )""")
    // v1: insert 5 rows
    w.sql("INSERT INTO tbl VALUES (1,'alice'),(2,'bob'),(3,'charlie'),(4,'dave'),(5,'eve')")
    // v2: delete id > 3 => 3 rows remain
    w.sql("DELETE FROM tbl WHERE id > 3")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")                                    // 3 rows
    w.read(t, version = 1, name = "read_before_delete")             // 5 rows
    w.read(t, version = 2, name = "read_after_delete")              // 3 rows
    w.cdf(t, startVersion = 1, endVersion = 1, name = "cdf_inserts")    // 5 insert changes
    w.cdf(t, startVersion = 2, endVersion = 2, name = "cdf_deletes")    // 2 delete changes
    w.cdf(t, startVersion = 1, endVersion = 2, name = "cdf_full")       // inserts + deletes
    w.snapshot(t)
  }

  test("cdf_after_update",
      "Enable CDF, insert 3 rows, update name where id=1, verify pre/post update in CDF",
      "write", "insert", "update", "cdf", "dv") { w =>
    w.sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
      TBLPROPERTIES (
        'delta.enableChangeDataFeed' = 'true',
        'delta.enableDeletionVectors' = 'true'
      )""")
    // v1: insert 3 rows
    w.sql("INSERT INTO tbl VALUES (1,'alice'),(2,'bob'),(3,'charlie')")
    // v2: update id=1
    w.sql("UPDATE tbl SET name = 'ALICE_UPDATED' WHERE id = 1")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")                                    // 3 rows
    w.read(t, version = 1, name = "read_before_update")             // original values
    w.read(t, version = 2, name = "read_after_update")              // updated values
    w.read(t, predicate = "id = 1", name = "read_updated_row")     // 1 row with updated name
    w.cdf(t, startVersion = 1, endVersion = 1, name = "cdf_inserts")    // 3 insert changes
    w.cdf(t, startVersion = 2, endVersion = 2, name = "cdf_update")     // pre/post update
    w.cdf(t, startVersion = 1, endVersion = 2, name = "cdf_full")       // inserts + update
    w.snapshot(t)
  }

  test("cdf_combined",
      "Enable CDF, insert/update/delete/insert, CDF across full range plus version reads",
      "write", "insert", "update", "delete", "cdf", "dv", "combined") { w =>
    w.sql("""CREATE TABLE tbl (id INT, name STRING, amount INT) USING delta
      TBLPROPERTIES (
        'delta.enableChangeDataFeed' = 'true',
        'delta.enableDeletionVectors' = 'true'
      )""")
    // v1: insert 5 rows
    w.sql("INSERT INTO tbl VALUES (1,'alice',100),(2,'bob',200),(3,'charlie',300),(4,'dave',400),(5,'eve',500)")
    // v2: update id=2
    w.sql("UPDATE tbl SET name = 'BOB_UPDATED', amount = 250 WHERE id = 2")
    // v3: delete id=4
    w.sql("DELETE FROM tbl WHERE id = 4")
    // v4: insert 2 more rows
    w.sql("INSERT INTO tbl VALUES (6,'frank',600),(7,'grace',700)")
    val t = w.table("tbl")
    w.writeSpec(t)
    // Read at every meaningful version
    w.read(t, version = 1, name = "read_v1_after_insert")      // 5 rows
    w.read(t, version = 2, name = "read_v2_after_update")      // 5 rows (same count, bob updated)
    w.read(t, version = 3, name = "read_v3_after_delete")      // 4 rows (dave deleted)
    w.read(t, version = 4, name = "read_v4_after_insert2")     // 6 rows
    w.read(t, name = "read_latest")                             // 6 rows
    // CDF at each operation version
    w.cdf(t, startVersion = 1, endVersion = 1, name = "cdf_v1_inserts")
    w.cdf(t, startVersion = 2, endVersion = 2, name = "cdf_v2_update")
    w.cdf(t, startVersion = 3, endVersion = 3, name = "cdf_v3_delete")
    w.cdf(t, startVersion = 4, endVersion = 4, name = "cdf_v4_inserts")
    // CDF across full range
    w.cdf(t, startVersion = 1, endVersion = 4, name = "cdf_full_range")
    // CDF partial ranges
    w.cdf(t, startVersion = 1, endVersion = 2, name = "cdf_insert_and_update")
    w.cdf(t, startVersion = 2, endVersion = 4, name = "cdf_update_to_end")
    w.snapshot(t)
    w.snapshot(t, version = 1)
    w.snapshot(t, version = 4)
  }

}.runAll()
