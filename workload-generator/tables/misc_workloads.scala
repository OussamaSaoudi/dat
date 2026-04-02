/**
 * Miscellaneous workloads: OSS compatibility reads and special paths.
 *
 * Run: ./bin/generate-workload.sh tables/misc_workloads.scala
 */
import io.delta.workload.WorkloadGenerator._

// =============================================================================
// OSS-compatible read workloads
// =============================================================================

workload("ossReadBasicOSS", "Basic OSS compatible read", "oss", "read") { w =>
  w.sql("""CREATE TABLE tbl (id LONG, data STRING) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  // 20 rows: id 0..19, data='oss_test'
  w.sql("INSERT INTO tbl SELECT id, 'oss_test' FROM range(20)")
  val t = w.table("tbl")
  w.read(t, name = "readAll")
  w.snapshot(t)
}

workload("ossReadPartitionedOSS", "Partitioned table OSS read", "oss", "read", "partitioned") { w =>
  w.sql("""CREATE TABLE tbl (id INT, part STRING, value INT) USING delta
    PARTITIONED BY (part) TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'a',10),(2,'b',20),(3,'a',30),(4,'b',40),(5,'c',50)")
  val t = w.table("tbl")
  w.read(t, name = "readAll")
  w.read(t, predicate = "part = 'a'", name = "readPartA")
  w.snapshot(t)
}

workload("ossReadPredicateOSS", "Predicate pushdown on OSS table", "oss", "read") { w =>
  w.sql("""CREATE TABLE tbl (id LONG, category STRING) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  // 50 rows: id 0..24 -> 'low', id 25..49 -> 'high'
  w.sql("""INSERT INTO tbl
    SELECT id, CASE WHEN id < 25 THEN 'low' ELSE 'high' END FROM range(50)""")
  val t = w.table("tbl")
  w.read(t, name = "readAll")
  w.read(t, predicate = "id >= 40", name = "readHighId")
  w.read(t, predicate = "category = 'low'", name = "readLow")
  w.snapshot(t)
}

workload("ossReadTimeTravelOSS", "Time travel on OSS table", "oss", "read", "time_travel") { w =>
  w.sql("""CREATE TABLE tbl (value INT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  // v0: 5 rows (1..5)
  w.sql("INSERT INTO tbl VALUES (1),(2),(3),(4),(5)")
  // v1: 5 more rows (6..10)
  w.sql("INSERT INTO tbl VALUES (6),(7),(8),(9),(10)")
  // v2: 5 more rows (11..15)
  w.sql("INSERT INTO tbl VALUES (11),(12),(13),(14),(15)")
  val t = w.table("tbl")
  w.read(t, name = "readLatest")
  w.read(t, version = 0, name = "readV0")
  w.read(t, version = 1, name = "readV1")
  w.snapshot(t)
}

// =============================================================================
// Special path handling
// =============================================================================

workload("pec_table_path_special", "Table path with special characters (spaces)", "path", "edge_case") { w =>
  w.sql("""CREATE TABLE tbl (id LONG) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  // 10 rows: id 0..9
  w.sql("INSERT INTO tbl SELECT id FROM range(10)")
  val t = w.table("tbl")
  w.read(t, name = "read_all")
  w.read(t, predicate = "id < 5", name = "read_filtered")
  w.snapshot(t)
}

generateAll(
  sys.env.getOrElse("WORKLOAD_OUTPUT_DIR", "/tmp/workloads"),
  force = sys.env.getOrElse("WORKLOAD_FORCE", "false").toBoolean)
System.exit(0)
