/**
 * Deletion vectors, MERGE, partitioning, corruption.
 * Run: ./bin/generate-workload.sh examples/dv_delete_basic.scala
 */
import io.delta.workload.WorkloadGenerator._

// --- DVs after DELETE ---
workload("dv_delete", "Deletion vectors after DELETE", "dv", "cdf") { w =>
  w.sql("""CREATE TABLE tbl (id BIGINT, name STRING) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true',
      'delta.enableChangeDataFeed' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'alice'),(2,'bob'),(3,'charlie'),(4,'diana'),(5,'eve')")
  w.sql("DELETE FROM tbl WHERE id <= 2")

  val t = w.table("tbl")
  w.read(t)
  w.read(t, version = 0)
  w.read(t, version = 1)
  w.read(t, predicate = "id > 3")
  w.read(t, version = 999)  // error auto-captured
  w.snapshot(t)
  w.snapshot(t, version = 0)
  w.cdf(t, startVersion = 2)
  w.cdf(t, startVersion = 0)
}

// --- MERGE ---
workload("merge", "MERGE with source table", "merge") { w =>
  w.sql("""CREATE TABLE target (id INT, val STRING) USING delta
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')""")
  w.sql("INSERT INTO target VALUES (1,'old_a'),(2,'old_b'),(3,'old_c')")
  w.sql("CREATE TABLE source (id INT, val STRING) USING delta")
  w.sql("INSERT INTO source VALUES (2,'new_b'),(4,'new_d')")
  w.sql("""MERGE INTO target t USING source s ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET val = s.val
    WHEN NOT MATCHED THEN INSERT *""")

  val tgt = w.table("target")
  val src = w.table("source")
  w.read(tgt)
  w.read(tgt, version = 0)
  w.read(src)
  w.snapshot(tgt)
  w.cdf(tgt, startVersion = 2)
}

// --- Corrupt: missing file ---
workload("corrupt_missing", "Deleted data file", "corrupt") { w =>
  w.sql("CREATE TABLE tbl (id INT) USING delta")
  w.sql("INSERT INTO tbl SELECT id FROM range(100)")

  val t = w.table("tbl")
  w.mutateTable(t) { dir =>
    import scala.collection.JavaConverters._
    java.nio.file.Files.list(dir).iterator().asScala
      .filter(_.toString.endsWith(".parquet"))
      .take(1).foreach(java.nio.file.Files.delete)
  }
  w.read(t)
  w.snapshot(t)
}

// --- Corrupt: truncated commit ---
workload("corrupt_truncated", "Truncated commit JSON", "corrupt") { w =>
  w.sql("CREATE TABLE tbl (id INT) USING delta")
  w.sql("INSERT INTO tbl SELECT id FROM range(10)")

  val t = w.table("tbl")
  w.mutateTable(t) { dir =>
    val f = dir.resolve("_delta_log/00000000000000000001.json")
    if (java.nio.file.Files.exists(f)) {
      val bytes = java.nio.file.Files.readAllBytes(f)
      java.nio.file.Files.write(f, java.util.Arrays.copyOf(bytes, 10))
    }
  }
  w.read(t)
}

generateAll(sys.env.getOrElse("WORKLOAD_OUTPUT_DIR", "/tmp/workloads"))
System.exit(0)
