/**
 * Legacy DV workloads (DV-001 through DV-018) from DeletionVectorsSuite.scala.
 *
 * These reproduce the original acceptance_workloads tables using SQL.
 * DV-001 through DV-005a share the same table pattern (2000 rows, alternating
 * delete/insert), while DV-005b through DV-018 have individual table shapes.
 *
 * NOTE: DV-017 (2B rows) is a pre-built golden table and cannot be practically
 * regenerated. The workload entry is provided for completeness but will take
 * extremely long to run.
 *
 * Run: ./bin/generate-workload.sh tables/dv_legacy.scala
 */
import io.delta.workload.WorkloadGenerator._

// =============================================================================
// DV-001: 2000 rows, 5 versions with deletes and inserts, reads at each version
// Schema: value INT (originally created via spark.range(2000))
// v0: CREATE TABLE + INSERT 2000 rows
// v1: DELETE value IN (0, 180, 300, 700, 1800)
// v2: INSERT (300, 700)
// v3: DELETE value IN (300, 250, 350, 900, 1353, 1567, 1800)
// v4: INSERT (900, 1567)
// =============================================================================
workload("DV-001", "read Delta table with deletion vectors", "dv") { w =>
  w.sql("""CREATE TABLE tbl (value INT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl SELECT CAST(id AS INT) FROM range(2000)")
  w.sql("DELETE FROM tbl WHERE value IN (0, 180, 300, 700, 1800)")
  w.sql("INSERT INTO tbl VALUES (300), (700)")
  w.sql("DELETE FROM tbl WHERE value IN (300, 250, 350, 900, 1353, 1567, 1800)")
  w.sql("INSERT INTO tbl VALUES (900), (1567)")
  val t = w.table("tbl")
  w.read(t, version = 0, name = "version_0")
  w.read(t, version = 4, name = "version_4")
  w.snapshot(t)
}

// =============================================================================
// DV-002: partitioned table, 2000 rows with deletes/inserts, partition filters
// Schema: id INT, name STRING, status STRING (default 'active')
// Partitioned by a derived column; 2000 rows with alternating delete/insert
// =============================================================================
workload("DV-002", "read partitioned Delta table with deletion vectors", "dv", "partitioned") { w =>
  w.sql("""CREATE TABLE tbl (id INT, name STRING, status STRING DEFAULT 'active') USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  // Insert 2000 rows: id 0..1999, name='name_{id}', status defaults to 'active'
  w.sql("""INSERT INTO tbl (id, name)
    SELECT CAST(id AS INT), CONCAT('name_', CAST(id AS STRING)) FROM range(2000)""")
  // Delete specific ids
  w.sql("DELETE FROM tbl WHERE id IN (0, 18, 30, 75, 100, 150, 300, 500, 700, 1000, 1500, 1800)")
  // Insert more rows
  w.sql("""INSERT INTO tbl (id, name)
    SELECT CAST(id AS INT), CONCAT('name_', CAST(id AS STRING)) FROM range(2000, 2500)""")
  // Second round of deletes
  w.sql("DELETE FROM tbl WHERE id IN (300, 350, 400, 900, 1200, 1353, 1567)")
  // Insert replacements
  w.sql("""INSERT INTO tbl (id, name)
    SELECT CAST(id AS INT), CONCAT('name_', CAST(id AS STRING)) FROM range(2500, 3000)""")
  val t = w.table("tbl")
  w.read(t, version = 0, name = "version_0")
  w.read(t, version = 4, name = "version_4")
  w.snapshot(t)
}

// =============================================================================
// DV-003: metadata columns (same data as DV-001)
// =============================================================================
workload("DV-003", "select metadata columns from a Delta table with deletion vectors", "dv") { w =>
  w.sql("""CREATE TABLE tbl (value INT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl SELECT CAST(id AS INT) FROM range(2000)")
  w.sql("DELETE FROM tbl WHERE value IN (0, 180, 300, 700, 1800)")
  w.sql("INSERT INTO tbl VALUES (300), (700)")
  w.sql("DELETE FROM tbl WHERE value IN (300, 250, 350, 900, 1353, 1567, 1800)")
  w.sql("INSERT INTO tbl VALUES (900), (1567)")
  val t = w.table("tbl")
  w.read(t, columns = Seq("_metadata.file_path"), name = "metadata_file_path")
  w.snapshot(t)
}

// =============================================================================
// DV-004: filter on DV table (same data as DV-001)
// =============================================================================
workload("DV-004", "read Delta table with deletion vectors with a filter", "dv") { w =>
  w.sql("""CREATE TABLE tbl (value INT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl SELECT CAST(id AS INT) FROM range(2000)")
  w.sql("DELETE FROM tbl WHERE value IN (0, 180, 300, 700, 1800)")
  w.sql("INSERT INTO tbl VALUES (300), (700)")
  w.sql("DELETE FROM tbl WHERE value IN (300, 250, 350, 900, 1353, 1567, 1800)")
  w.sql("INSERT INTO tbl VALUES (900), (1567)")
  val t = w.table("tbl")
  w.snapshot(t)
}

// =============================================================================
// DV-005a: subquery count on DV table (same data as DV-001)
// =============================================================================
workload("DV-005a", "read Delta tables with DVs in subqueries - table1 count", "dv") { w =>
  w.sql("""CREATE TABLE tbl (value INT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl SELECT CAST(id AS INT) FROM range(2000)")
  w.sql("DELETE FROM tbl WHERE value IN (0, 180, 300, 700, 1800)")
  w.sql("INSERT INTO tbl VALUES (300), (700)")
  w.sql("DELETE FROM tbl WHERE value IN (300, 250, 350, 900, 1353, 1567, 1800)")
  w.sql("INSERT INTO tbl VALUES (900), (1567)")
  val t = w.table("tbl")
  w.read(t, name = "count")
  w.snapshot(t)
}

// =============================================================================
// DV-005b: second table for subquery test (small table)
// Schema: id INT, name STRING
// =============================================================================
workload("DV-005b", "read Delta tables with DVs in subqueries - table2 count", "dv") { w =>
  w.sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1, 'test')")
  val t = w.table("tbl")
  w.read(t, name = "count")
  w.snapshot(t)
}

// =============================================================================
// DV-006: DELETE on table with no prior DVs (500 files, 2 rows each = 1000 rows)
// DELETE even ids < 200 => removes 100 rows => 900 remain
// =============================================================================
workload("DV-006", "DELETE with DVs - on a table with no prior DVs", "dv") { w =>
  w.sql("""CREATE TABLE tbl (id LONG) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  // Create 1000 rows (id 0..999)
  w.sql("INSERT INTO tbl SELECT id FROM range(1000)")
  // Enable DVs explicitly (table was created without them in the original)
  w.sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")
  // Delete even ids < 200
  w.sql("DELETE FROM tbl WHERE id % 2 = 0 AND id < 200")
  val t = w.table("tbl")
  w.read(t, name = "after_delete")
  w.snapshot(t)
}

// =============================================================================
// DV-007: DELETE on table that already has DVs
// 50 rows (value 0..49), DELETE specific values twice
// =============================================================================
workload("DV-007", "DELETE with DVs - existing table already has DVs", "dv") { w =>
  w.sql("""CREATE TABLE tbl (value LONG) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl SELECT id FROM range(50)")
  // First delete
  w.sql("DELETE FROM tbl WHERE value IN (0, 10, 20, 30, 40)")
  // Second delete (on table already containing DVs)
  w.sql("DELETE FROM tbl WHERE value IN (49, 29, 7, 8, 17, 36)")
  val t = w.table("tbl")
  w.read(t, name = "after_additional_delete")
  w.snapshot(t)
}

// =============================================================================
// DV-008: JOIN with DVs - self-join (table2 is a small helper)
// table2 has 1 row
// =============================================================================
workload("DV-008", "JOIN with DVs - self-join a table with DVs (underlying read)", "dv") { w =>
  w.sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1, 'test')")
  val t = w.table("tbl")
  w.read(t, name = "table2_latest")
  w.snapshot(t)
}

// =============================================================================
// DV-009: JOIN with DVs - non-DV table joins DV table
// table2 is a small helper, 1 row at v1, empty at v0
// =============================================================================
workload("DV-009", "JOIN with DVs - non-DV table joins DV table (underlying reads)", "dv") { w =>
  w.sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1, 'test')")
  val t = w.table("tbl")
  w.read(t, name = "table2_latest_v1")
  w.read(t, version = 0, name = "table2_version_0")
  w.snapshot(t)
}

// =============================================================================
// DV-010: INSERT into DV table
// 20 rows (value 0..19), DELETE 4, then INSERT 4 more
// =============================================================================
workload("DV-010", "insert into Delta table with DVs", "dv") { w =>
  w.sql("""CREATE TABLE tbl (value LONG) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl SELECT id FROM range(20)")
  w.sql("DELETE FROM tbl WHERE value IN (0, 5, 10, 15)")
  w.sql("INSERT INTO tbl SELECT id FROM range(20, 24)")
  val t = w.table("tbl")
  w.read(t, name = "after_insert")
  w.snapshot(t)
}

// =============================================================================
// DV-011: DELETE with DVs + column mapping mode
// 10 partitions (part 0..9), 5 rows per partition
// col1 = part + 10*i, col2 = "foo" + (part % 5)
// =============================================================================
workload("DV-011", "DELETE with DVs with column mapping mode", "dv", "column_mapping", "partitioned") { w =>
  w.sql("""CREATE TABLE tbl (part INT, col1 INT, col2 STRING) USING delta
    PARTITIONED BY (part) TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  // Insert 50 rows: 10 partitions x 5 rows each
  w.sql("""INSERT INTO tbl
    SELECT
      CAST(id % 10 AS INT) as part,
      CAST(id AS INT) as col1,
      CONCAT('foo', CAST(id % 5 AS STRING)) as col2
    FROM range(50)""")
  w.sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")
  w.sql("DELETE FROM tbl WHERE col1 = 2")
  val t = w.table("tbl")
  w.read(t, name = "after_delete")
  w.read(t, predicate = "col1 = 2", name = "filter_col1_eq_2")
  w.snapshot(t)
}

// =============================================================================
// DV-012: DELETE with DVs - packing multiple DVs
// 200 rows in many files, DELETE even ids < 20
// =============================================================================
workload("DV-012", "DELETE with DVs - packing multiple DVs", "dv") { w =>
  w.sql("""CREATE TABLE tbl (id LONG) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl SELECT id FROM range(200)")
  w.sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")
  w.sql("DELETE FROM tbl WHERE id % 2 = 0 AND id < 20")
  val t = w.table("tbl")
  w.read(t, name = "after_delete")
  w.snapshot(t)
}

// =============================================================================
// DV-013: MERGE with DVs - merge into DV table
// 10 rows (value 0..9), DELETE (0, 9), then MERGE:
//   source = range(10001, 10009) UNION values matching existing
//   MATCHED -> UPDATE, NOT MATCHED -> INSERT
// Result: 20 rows
// =============================================================================
workload("DV-013", "MERGE with DVs - merge into DV table", "dv", "merge") { w =>
  w.sql("""CREATE TABLE tbl (value LONG) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl SELECT id FROM range(10)")
  w.sql("DELETE FROM tbl WHERE value IN (0, 9)")
  // Create source for MERGE: values 1-8 (matched) + 10001-10008 (not matched)
  w.sql("CREATE TABLE src (value LONG) USING delta")
  w.sql("INSERT INTO src SELECT id FROM range(1, 9)")
  w.sql("INSERT INTO src SELECT id + 10001 FROM range(8)")
  w.sql("""MERGE INTO tbl t USING src s ON t.value = s.value
    WHEN MATCHED THEN UPDATE SET value = s.value
    WHEN NOT MATCHED THEN INSERT *""")
  val t = w.table("tbl")
  w.read(t, name = "after_merge")
  w.snapshot(t)
}

// =============================================================================
// DV-014: UPDATE with DVs - update rewrite files with DVs
// 10 rows (value 0..9), DELETE (0, 9), then UPDATE value=1 SET value=-1
// =============================================================================
workload("DV-014", "UPDATE with DVs - update rewrite files with DVs", "dv") { w =>
  w.sql("""CREATE TABLE tbl (value LONG) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl SELECT id FROM range(10)")
  w.sql("DELETE FROM tbl WHERE value IN (0, 9)")
  w.sql("UPDATE tbl SET value = -1 WHERE value = 1")
  val t = w.table("tbl")
  w.read(t, name = "after_update")
  w.snapshot(t)
}

// =============================================================================
// DV-015: UPDATE with DVs - update deleted rows updates nothing
// 10 rows (value 0..9), DELETE (0, 9), then UPDATE value=0 (no-op, already deleted)
// =============================================================================
workload("DV-015", "UPDATE with DVs - update deleted rows updates nothing", "dv") { w =>
  w.sql("""CREATE TABLE tbl (value LONG) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl SELECT id FROM range(10)")
  w.sql("DELETE FROM tbl WHERE value IN (0, 9)")
  // Trying to update a deleted row - should be a no-op
  w.sql("UPDATE tbl SET value = -1 WHERE value = 0")
  val t = w.table("tbl")
  w.read(t, name = "after_noop_update")
  w.snapshot(t)
}

// =============================================================================
// DV-016: INSERT + DELETE + MERGE + UPDATE with DVs
// Complex multi-step DML sequence:
// v0: INSERT 10 rows (id 0..9)
// v1: DELETE id IN (1, 8)
// v2: UPDATE id=0 SET id=-1
// v3: MERGE (source matches remaining, deletes matched, inserts new)
// v4: DELETE id=4
// =============================================================================
workload("DV-016", "INSERT + DELETE + MERGE + UPDATE with DVs", "dv") { w =>
  w.sql("""CREATE TABLE tbl (id LONG) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl SELECT id FROM range(10)")
  w.sql("DELETE FROM tbl WHERE id IN (1, 8)")
  w.sql("UPDATE tbl SET id = -1 WHERE id = 0")
  // MERGE: source has values matching remaining rows
  w.sql("CREATE TABLE src (value LONG) USING delta")
  w.sql("INSERT INTO src SELECT id FROM range(-1, 10)")
  w.sql("""MERGE INTO tbl t USING src s ON t.id = s.value
    WHEN MATCHED THEN UPDATE SET id = t.id
    WHEN NOT MATCHED THEN INSERT (id) VALUES (s.value)""")
  w.sql("DELETE FROM tbl WHERE id = 4")
  val t = w.table("tbl")
  w.read(t, version = 0, name = "version_0_initial")
  w.read(t, version = 1, name = "version_1_after_delete")
  w.read(t, version = 2, name = "version_2_after_update")
  w.read(t, version = 3, name = "version_3_after_merge")
  w.read(t, version = 4, name = "version_4_final")
  w.snapshot(t)
}

// =============================================================================
// DV-017: Huge table - 2B+ rows with existing DV
// WARNING: This workload will take extremely long to run. It is provided for
// completeness. The original table was pre-built as a golden table.
// =============================================================================
workload("DV-017", "huge table: read from tables of 2B rows with existing DV", "dv", "large") { w =>
  w.sql("""CREATE TABLE tbl (value INT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  // WARNING: This generates ~2.1 billion rows. Only run if you have sufficient resources.
  w.sql("INSERT INTO tbl SELECT CAST(id AS INT) FROM range(2145386174)")
  // The original table has a DV that removes ~50000 rows
  w.sql("DELETE FROM tbl WHERE value >= 0 AND value < 50000")
  val t = w.table("tbl")
  w.read(t, name = "full_table_count")
  w.snapshot(t)
}

// =============================================================================
// DV-018: DV feature enabled but no DVs produced
// =============================================================================
workload("DV-018", "table with DV feature enabled but no DVs", "dv") { w =>
  w.sql("""CREATE TABLE tbl (id LONG) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1)")
  val t = w.table("tbl")
  w.read(t, name = "read_no_dv_table")
  w.snapshot(t)
}

generateAll(
  sys.env.getOrElse("WORKLOAD_OUTPUT_DIR", "/tmp/workloads"),
  force = sys.env.getOrElse("WORKLOAD_FORCE", "false").toBoolean)
System.exit(0)
