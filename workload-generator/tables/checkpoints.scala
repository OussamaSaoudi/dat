import io.delta.workload.WorkloadGenerator._

workload("checkpoint_classic", "Classic checkpoint read", "checkpoint") { w =>
  w.sql("CREATE TABLE tbl (id INT) USING delta")
  w.sql("INSERT INTO tbl SELECT CAST(id AS INT) FROM range(1, 101)")
  w.sql("INSERT INTO tbl SELECT CAST(id AS INT) FROM range(101, 201)")
  w.sql("INSERT INTO tbl SELECT CAST(id AS INT) FROM range(201, 301)")
  val loc = w.spark.sql("DESCRIBE DETAIL tbl").collect()(0).getAs[String]("location")
  org.apache.spark.sql.delta.DeltaLog.forTable(w.spark, loc).checkpoint()
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "id > 250")
  w.snapshot(t)
}

workload("checkpoint_time_travel", "Checkpoint with time travel", "checkpoint") { w =>
  w.sql("CREATE TABLE tbl (id INT) USING delta")
  w.sql("INSERT INTO tbl SELECT CAST(id AS INT) FROM range(1, 51)")
  w.sql("INSERT INTO tbl SELECT CAST(id AS INT) FROM range(51, 101)")
  val loc = w.spark.sql("DESCRIBE DETAIL tbl").collect()(0).getAs[String]("location")
  org.apache.spark.sql.delta.DeltaLog.forTable(w.spark, loc).checkpoint()
  w.sql("INSERT INTO tbl SELECT CAST(id AS INT) FROM range(101, 151)")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, version = 0)
  w.read(t, version = 1)
  w.read(t, version = 2)
  w.snapshot(t)
}

workload("checkpoint_interval", "Checkpoint at interval=5", "checkpoint") { w =>
  w.sql("CREATE TABLE tbl (id INT, val STRING) USING delta TBLPROPERTIES ('delta.checkpointInterval' = '5')")
  for (i <- 1 to 7) w.sql(s"INSERT INTO tbl VALUES ($i, 'v$i')")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, version = 5)
  w.snapshot(t)
  w.snapshot(t, version = 5)
}

workload("checkpoint_schema_evolution", "Checkpoint + schema evolution", "checkpoint", "schema_evolution") { w =>
  w.sql("CREATE TABLE tbl (id LONG) USING delta")
  w.sql("INSERT INTO tbl SELECT id FROM range(50)")
  val loc = w.spark.sql("DESCRIBE DETAIL tbl").collect()(0).getAs[String]("location")
  org.apache.spark.sql.delta.DeltaLog.forTable(w.spark, loc).checkpoint()
  w.sql("ALTER TABLE tbl ADD COLUMN name STRING")
  w.sql("INSERT INTO tbl SELECT id, 'test' FROM range(50, 100)")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, version = 0)
  w.snapshotHistory(t)
}

workload("checkpoint_partitioned", "Checkpoint + partitioned table", "checkpoint") { w =>
  w.sql("CREATE TABLE tbl (id LONG, part INT) USING delta PARTITIONED BY (part)")
  w.sql("INSERT INTO tbl SELECT id, CAST(id % 5 AS INT) FROM range(100)")
  w.sql("INSERT INTO tbl SELECT id, CAST(id % 5 AS INT) FROM range(100, 200)")
  val loc = w.spark.sql("DESCRIBE DETAIL tbl").collect()(0).getAs[String]("location")
  org.apache.spark.sql.delta.DeltaLog.forTable(w.spark, loc).checkpoint()
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "part = 0")
  w.read(t, predicate = "part = 3")
  w.snapshot(t)
}

generateAll(
  sys.env.getOrElse("WORKLOAD_OUTPUT_DIR", "/tmp/workloads"),
  force = sys.env.getOrElse("WORKLOAD_FORCE", "false").toBoolean)
System.exit(0)
