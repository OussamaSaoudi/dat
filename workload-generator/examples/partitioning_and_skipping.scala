/**
 * Partitioning, data skipping, and predicate pushdown.
 * Run: ./bin/generate-workload.sh examples/partitioning_and_skipping.scala
 */
import io.delta.workload.WorkloadGenerator._

workload("single_partition", "Single partition column", "partitioned") { w =>
  w.sql("""CREATE TABLE tbl (id INT, region STRING, value DOUBLE)
    USING delta PARTITIONED BY (region)""")
  w.sql("INSERT INTO tbl VALUES (1,'us',10),(2,'us',20),(3,'eu',30),(4,'eu',40),(5,'asia',50)")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "region = 'us'")
  w.read(t, predicate = "region = 'eu'")
  w.read(t, predicate = "region = 'antarctica'")
  w.snapshot(t)
}

workload("multi_partition", "Multiple partition columns", "partitioned") { w =>
  w.sql("""CREATE TABLE tbl (id INT, year INT, month INT, data STRING)
    USING delta PARTITIONED BY (year, month)""")
  w.sql("""INSERT INTO tbl VALUES
    (1,2024,1,'jan24'),(2,2024,2,'feb24'),(3,2024,3,'mar24'),
    (4,2025,1,'jan25'),(5,2025,2,'feb25')""")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "year = 2024")
  w.read(t, predicate = "year = 2025 AND month = 1")
  w.read(t, columns = Seq("id", "year"))
  w.snapshot(t)
}

workload("null_partition", "NULL partition values", "partitioned", "nulls") { w =>
  w.sql("""CREATE TABLE tbl (id INT, category STRING, value INT)
    USING delta PARTITIONED BY (category)""")
  w.sql("INSERT INTO tbl VALUES (1,'a',10),(2,NULL,20),(3,'b',30),(4,NULL,40)")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "category IS NULL")
  w.read(t, predicate = "category IS NOT NULL")
  w.read(t, predicate = "category = 'a'")
  w.snapshot(t)
}

workload("stats_skipping", "Data skipping via column stats", "skipping") { w =>
  w.sql("CREATE TABLE tbl (id INT, value INT) USING delta")
  w.sql("INSERT INTO tbl SELECT id, id * 10 FROM range(100) WHERE id < 50")
  w.sql("INSERT INTO tbl SELECT id, id * 10 FROM range(100) WHERE id >= 50")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "id < 25")
  w.read(t, predicate = "id >= 75")
  w.read(t, predicate = "id > 999")
  w.read(t, predicate = "id >= 0")
  w.snapshot(t)
}

workload("partition_pruning", "Partition pruning + stats", "partitioned", "skipping") { w =>
  w.sql("""CREATE TABLE tbl (id INT, region STRING, amount DOUBLE)
    USING delta PARTITIONED BY (region)""")
  w.sql("INSERT INTO tbl VALUES (1,'us',10),(2,'us',20)")
  w.sql("INSERT INTO tbl VALUES (3,'eu',30),(4,'eu',40)")
  w.sql("INSERT INTO tbl VALUES (5,'asia',50)")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "region = 'us'")
  w.read(t, predicate = "region = 'us' AND amount > 15")
  w.read(t, predicate = "region = 'mars'")
  w.snapshot(t)
}

workload("column_projection", "Column subsets", "projection") { w =>
  w.sql("CREATE TABLE tbl (a INT, b STRING, c DOUBLE, d BOOLEAN, e DATE) USING delta")
  w.sql("""INSERT INTO tbl VALUES
    (1,'x',1.1,true,DATE'2024-01-01'),
    (2,'y',2.2,false,DATE'2024-06-15'),
    (3,'z',3.3,true,DATE'2025-01-01')""")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, columns = Seq("a"))
  w.read(t, columns = Seq("b", "d"))
  w.read(t, columns = Seq("e", "c", "a"))
  w.read(t, predicate = "a > 1", columns = Seq("a", "b"))
  w.snapshot(t)
}

generateAll(sys.env.getOrElse("WORKLOAD_OUTPUT_DIR", "/tmp/workloads"))
System.exit(0)
