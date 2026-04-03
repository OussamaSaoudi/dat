/**
 * Basic write workloads that produce write_spec.json files for kernel write testing.
 *
 * Each test generates a write spec (commit history) plus read/snapshot verification specs.
 * Covers: create, insert, delete, update, alter table, truncate, partitioning, DVs.
 */

new WorkloadSuite("write_basic") {

  test("create_and_read", "Create table, insert rows, verify read", "write", "create", "insert") { w =>
    w.sql("""CREATE TABLE tbl (id INT, name STRING, score DOUBLE) USING delta""")
    w.sql("INSERT INTO tbl VALUES (1,'alice',95.5),(2,'bob',87.3),(3,'charlie',72.1)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, predicate = "score > 90.0", name = "read_high_score")
    w.snapshot(t)
  }

  test("create_with_properties", "Create with DV + CDF properties, verify protocol/metadata",
      "write", "create", "dv", "cdf") { w =>
    w.sql("""CREATE TABLE tbl (id INT, value STRING, amount INT) USING delta
      TBLPROPERTIES (
        'delta.enableDeletionVectors' = 'true',
        'delta.enableChangeDataFeed' = 'true'
      )""")
    w.sql("INSERT INTO tbl VALUES (1,'alpha',100),(2,'beta',200),(3,'gamma',300)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.cdf(t, startVersion = 0, endVersion = 1)
    w.snapshot(t)
  }

  test("insert_multiple", "Three separate inserts, verify cumulative rows",
      "write", "insert") { w =>
    w.sql("""CREATE TABLE tbl (id INT, category STRING) USING delta""")
    w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'a')")
    w.sql("INSERT INTO tbl VALUES (3,'b'),(4,'b'),(5,'b')")
    w.sql("INSERT INTO tbl VALUES (6,'c')")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, version = 1, name = "read_v1")
    w.read(t, version = 2, name = "read_v2")
    w.read(t, version = 3, name = "read_v3")
    w.read(t, predicate = "category = 'b'", name = "read_category_b")
    w.snapshot(t)
  }

  test("delete_basic", "Insert then delete rows with WHERE clause",
      "write", "delete", "dv") { w =>
    w.sql("""CREATE TABLE tbl (id INT, value STRING, amount INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1,'keep',10),(2,'remove',20),(3,'keep',30),(4,'remove',40),(5,'keep',50)")
    w.sql("DELETE FROM tbl WHERE value = 'remove'")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_after_delete")
    w.read(t, version = 1, name = "read_before_delete")
    w.read(t, predicate = "amount > 25", name = "read_large_amount")
    w.snapshot(t)
  }

  test("update_basic", "Insert then update rows with SET",
      "write", "update", "dv") { w =>
    w.sql("""CREATE TABLE tbl (id INT, status STRING, count INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1,'pending',0),(2,'pending',0),(3,'active',5),(4,'pending',0)")
    w.sql("UPDATE tbl SET status = 'active', count = count + 1 WHERE status = 'pending'")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_after_update")
    w.read(t, version = 1, name = "read_before_update")
    w.read(t, predicate = "status = 'active'", name = "read_active")
    w.snapshot(t)
  }

  test("alter_add_column", "Add column then insert with new schema",
      "write", "alter_table", "schema_evolution") { w =>
    w.sql("""CREATE TABLE tbl (id INT, name STRING) USING delta""")
    w.sql("INSERT INTO tbl VALUES (1,'alice'),(2,'bob')")
    w.sql("ALTER TABLE tbl ADD COLUMN (email STRING)")
    w.sql("INSERT INTO tbl VALUES (3,'charlie','charlie@test.com'),(4,'diana','diana@test.com')")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, version = 1, name = "read_before_alter")
    w.read(t, predicate = "email IS NULL", name = "read_null_email")
    w.read(t, predicate = "email IS NOT NULL", name = "read_with_email")
    w.snapshot(t)
  }

  test("alter_set_properties", "Enable CDF via SET TBLPROPERTIES then verify CDF works",
      "write", "alter_table", "cdf") { w =>
    w.sql("""CREATE TABLE tbl (id INT, value INT) USING delta""")
    w.sql("INSERT INTO tbl VALUES (1,100),(2,200),(3,300)")
    w.sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')")
    w.sql("INSERT INTO tbl VALUES (4,400),(5,500)")
    w.sql("UPDATE tbl SET value = value + 1000 WHERE id <= 2")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, predicate = "value > 1000", name = "read_updated")
    // CDF only available from version 3 onward (after enableChangeDataFeed was set)
    w.cdf(t, startVersion = 3, endVersion = 4)
    w.snapshot(t)
  }

  test("truncate_table", "Insert rows then truncate to empty table",
      "write", "truncate") { w =>
    w.sql("""CREATE TABLE tbl (id INT, data STRING) USING delta""")
    w.sql("INSERT INTO tbl VALUES (1,'first'),(2,'second'),(3,'third'),(4,'fourth')")
    w.sql("TRUNCATE TABLE tbl")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_after_truncate")
    w.read(t, version = 1, name = "read_before_truncate")
    w.snapshot(t)
  }

  test("partitioned_insert", "Partitioned table with inserts to different partitions",
      "write", "insert", "partitioned") { w =>
    w.sql("""CREATE TABLE tbl (id INT, region STRING, revenue INT) USING delta
      PARTITIONED BY (region)""")
    w.sql("INSERT INTO tbl VALUES (1,'east',100),(2,'east',150)")
    w.sql("INSERT INTO tbl VALUES (3,'west',200),(4,'west',250),(5,'west',300)")
    w.sql("INSERT INTO tbl VALUES (6,'north',400)")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_all")
    w.read(t, predicate = "region = 'east'", name = "read_east")
    w.read(t, predicate = "region = 'west'", name = "read_west")
    w.read(t, predicate = "region = 'north'", name = "read_north")
    w.read(t, predicate = "revenue >= 200", name = "read_high_revenue")
    w.snapshot(t)
  }

  test("delete_with_dvs", "DV-enabled table with targeted deletes",
      "write", "delete", "dv") { w =>
    w.sql("""CREATE TABLE tbl (id INT, name STRING, score INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("""INSERT INTO tbl VALUES
      (1,'alice',90),(2,'bob',75),(3,'charlie',88),
      (4,'diana',92),(5,'eve',60),(6,'frank',85)""")
    w.sql("DELETE FROM tbl WHERE score < 80")
    val t = w.table("tbl")
    w.writeSpec(t)
    w.read(t, name = "read_after_delete")
    w.read(t, version = 1, name = "read_before_delete")
    w.read(t, predicate = "score >= 85", name = "read_high_score")
    w.snapshot(t)
  }

}.runAll()
