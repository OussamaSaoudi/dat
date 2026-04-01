/**
 * Minimal example.
 * Run: ./bin/generate-workload.sh examples/simple_read.scala
 */
import io.delta.workload.WorkloadGenerator._

workload("simple", "Simple table with 100 rows", "basic") { w =>
  w.sql("CREATE TABLE tbl (id INT, value DOUBLE) USING delta")
  w.sql("INSERT INTO tbl SELECT id, rand() as value FROM range(100)")

  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "id >= 50")
  w.read(t, columns = Seq("id"))
  w.snapshot(t)
}

generateAll(sys.env.getOrElse("WORKLOAD_OUTPUT_DIR", "/tmp/workloads"))
System.exit(0)
