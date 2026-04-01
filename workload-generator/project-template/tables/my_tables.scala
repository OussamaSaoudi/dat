/**
 * Example workload script with full IDE support.
 *
 * Run: ../bin/generate-workload.sh tables/my_tables.scala
 */
import io.delta.workload.WorkloadGenerator._

workload("example", "Example table") { w =>
  w.sql("CREATE TABLE tbl (id INT, value STRING) USING delta")
  w.sql("INSERT INTO tbl VALUES (1, 'hello'), (2, 'world')")

  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "id > 1")
  w.snapshot(t)
}

generateAll(sys.env.getOrElse("WORKLOAD_OUTPUT_DIR", "/tmp/workloads"))
System.exit(0)
