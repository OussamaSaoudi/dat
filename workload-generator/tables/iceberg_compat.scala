/**
 * Iceberg Compatibility tests.
 * Run: ./bin/generate-workload.sh examples/iceberg_compat.scala
 */
import io.delta.workload.WorkloadGenerator._

workload("ice_compat_v1", "IcebergCompatV1", "icebergCompat") { w =>
  w.sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
    TBLPROPERTIES ('delta.enableIcebergCompatV1' = 'true',
      'delta.columnMapping.mode' = 'name')""")
  w.sql("INSERT INTO tbl VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie')")
  val t = w.table("tbl")
  w.read(t, name = "read_all")
  w.read(t, predicate = "name = 'bob'", name = "read_by_name")
  w.read(t, predicate = "id > 1", name = "read_id_gt_1")
  w.snapshot(t)
}

workload("ice_compat_v2", "IcebergCompatV2", "icebergCompat") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value DOUBLE, category STRING) USING delta
    TBLPROPERTIES ('delta.enableIcebergCompatV2' = 'true',
      'delta.columnMapping.mode' = 'name')""")
  w.sql("INSERT INTO tbl VALUES (1,1.0,'A'),(2,2.0,'B'),(3,3.0,'A')")
  w.sql("INSERT INTO tbl VALUES (4,4.0,'B'),(5,5.0,'C')")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "category = 'A'")
  w.read(t, predicate = "value > 3.0")
  w.read(t, version = 1)
  w.snapshot(t)
}

workload("ice_column_mapping", "IcebergCompat + column mapping", "icebergCompat") { w =>
  w.sql("""CREATE TABLE tbl (id INT, name STRING, value DOUBLE) USING delta
    TBLPROPERTIES ('delta.enableIcebergCompatV2' = 'true',
      'delta.columnMapping.mode' = 'name')""")
  w.sql("INSERT INTO tbl VALUES (1,'one',1.0),(2,'two',2.0)")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "id = 1")
  w.snapshot(t)
}

workload("ice_nested_map", "IcebergCompat V2 + MAP", "icebergCompat") { w =>
  w.sql("""CREATE TABLE tbl (id INT, col2 MAP<INT, INT>) USING delta
    TBLPROPERTIES ('delta.enableIcebergCompatV2' = 'true',
      'delta.columnMapping.mode' = 'name')""")
  w.sql("INSERT INTO tbl VALUES (1, map(1, 10)), (2, map(2, 20))")
  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
}

workload("ice_nested_array", "IcebergCompat V2 + ARRAY", "icebergCompat") { w =>
  w.sql("""CREATE TABLE tbl (id INT, col2 ARRAY<INT>) USING delta
    TBLPROPERTIES ('delta.enableIcebergCompatV2' = 'true',
      'delta.columnMapping.mode' = 'name')""")
  w.sql("INSERT INTO tbl VALUES (1, array(1, 2)), (2, array(3))")
  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
}

workload("ice_partitioned", "IcebergCompat + partitions", "icebergCompat") { w =>
  w.sql("""CREATE TABLE tbl (id INT, name STRING, category STRING) USING delta
    PARTITIONED BY (category)
    TBLPROPERTIES ('delta.enableIcebergCompatV2' = 'true',
      'delta.columnMapping.mode' = 'name')""")
  w.sql("INSERT INTO tbl VALUES (1, 'alice', 'A'), (2, 'bob', 'B')")
  w.sql("INSERT INTO tbl VALUES (3, 'charlie', 'A'), (4, 'diana', 'C')")
  w.sql("INSERT INTO tbl VALUES (5, 'eve', 'B'), (6, 'frank', 'C')")
  val t = w.table("tbl")
  w.read(t, name = "read_all")
  w.read(t, predicate = "category = 'A'", name = "read_category_A")
  w.read(t, predicate = "category = 'B'", name = "read_category_B")
  w.read(t, predicate = "category = 'C'", name = "read_category_C")
  w.snapshot(t)
}

workload("ice_with_dv", "IcebergCompat + DVs", "icebergCompat", "dv") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
    TBLPROPERTIES ('delta.enableIcebergCompatV2' = 'true',
      'delta.columnMapping.mode' = 'name',
      'delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
  w.sql("DELETE FROM tbl WHERE id = 2")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, version = 1)
  w.snapshot(t)
}

generateAll(
  sys.env.getOrElse("WORKLOAD_OUTPUT_DIR", "/tmp/workloads"),
  force = sys.env.getOrElse("WORKLOAD_FORCE", "false").toBoolean)
System.exit(0)
