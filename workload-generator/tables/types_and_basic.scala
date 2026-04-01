/**
 * Types, edge cases, time travel, and error handling.
 * Run: ./bin/generate-workload.sh examples/types_and_basic.scala
 */
import io.delta.workload.WorkloadGenerator._

workload("all_primitive_types", "Every primitive Delta type", "types") { w =>
  w.sql("""CREATE TABLE tbl (
    int_col INT, long_col BIGINT, double_col DOUBLE, float_col FLOAT,
    string_col STRING, bool_col BOOLEAN, binary_col BINARY,
    decimal_col DECIMAL(18,6), date_col DATE, ts_col TIMESTAMP
  ) USING delta""")
  w.sql("""INSERT INTO tbl VALUES
    (1, 100000000000, 3.14, 2.718, 'hello', true, X'DEADBEEF', 123456.789012, DATE'2024-01-15', TIMESTAMP'2024-01-15 10:30:00'),
    (2, 200000000000, -1.5, 0.0, 'world', false, X'CAFEBABE', -99999.000001, DATE'2025-06-30', TIMESTAMP'2025-06-30 23:59:59'),
    (42, 0, 0.0, -1.0, '', true, X'00', 0.000000, DATE'1970-01-01', TIMESTAMP'1970-01-01 00:00:00')""")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, columns = Seq("int_col", "string_col"))
  w.snapshot(t)
}

workload("nested_types", "Struct, array, and map columns", "types") { w =>
  w.sql("""CREATE TABLE tbl (
    id INT, info STRUCT<name: STRING, age: INT>,
    tags ARRAY<STRING>, props MAP<STRING, INT>
  ) USING delta""")
  w.sql("""INSERT INTO tbl VALUES
    (1, named_struct('name','alice','age',30), array('a','b'), map('x',1,'y',2)),
    (2, named_struct('name','bob','age',25), array('c'), map('z',3))""")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, columns = Seq("id", "info"))
  w.snapshot(t)
}

workload("null_values", "NULLs across types", "types", "nulls") { w =>
  w.sql("CREATE TABLE tbl (int_col INT, string_col STRING, double_col DOUBLE) USING delta")
  w.sql("INSERT INTO tbl VALUES (NULL, NULL, NULL)")
  w.sql("INSERT INTO tbl VALUES (1, 'not null', 1.5)")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "int_col IS NULL")
  w.read(t, predicate = "int_col IS NOT NULL")
  w.snapshot(t)
}

workload("empty_table", "Zero rows", "types", "edge") { w =>
  w.sql("CREATE TABLE tbl (id INT, value STRING) USING delta")
  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
}

workload("single_row", "Exactly one row", "types", "edge") { w =>
  w.sql("CREATE TABLE tbl (id INT, value STRING) USING delta")
  w.sql("INSERT INTO tbl VALUES (1, 'only')")
  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
}

workload("large_table", "10000 rows", "types", "scale") { w =>
  w.sql("CREATE TABLE tbl (id BIGINT, value DOUBLE, category STRING) USING delta")
  w.sql("""INSERT INTO tbl
    SELECT id, rand() as value,
      CASE WHEN id % 5 = 0 THEN 'A' WHEN id % 5 = 1 THEN 'B'
           WHEN id % 5 = 2 THEN 'C' WHEN id % 5 = 3 THEN 'D' ELSE 'E' END
    FROM range(10000)""")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "category = 'A'")
  w.read(t, columns = Seq("id", "category"))
  w.snapshot(t)
}

workload("time_travel", "Multiple versions", "time_travel") { w =>
  w.sql("CREATE TABLE tbl (id INT, val STRING) USING delta")
  w.sql("INSERT INTO tbl VALUES (1, 'v1')")
  w.sql("INSERT INTO tbl VALUES (2, 'v2')")
  w.sql("UPDATE tbl SET val = 'updated' WHERE id = 1")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, version = 0)
  w.read(t, version = 1)
  w.read(t, version = 2)
  w.snapshot(t)
  w.snapshot(t, version = 0)
}

workload("error_bad_version", "Non-existent version", "error") { w =>
  w.sql("CREATE TABLE tbl (id INT) USING delta")
  w.sql("INSERT INTO tbl VALUES (1)")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, version = 999)
  w.snapshot(t)
}

workload("error_cdf_not_enabled", "CDF on table without CDF", "error", "cdf") { w =>
  w.sql("CREATE TABLE tbl (id INT) USING delta")
  w.sql("INSERT INTO tbl VALUES (1)")
  val t = w.table("tbl")
  w.read(t)
  w.cdf(t, startVersion = 0)
  w.snapshot(t)
}

generateAll(
  sys.env.getOrElse("WORKLOAD_OUTPUT_DIR", "/tmp/workloads"),
  force = sys.env.getOrElse("WORKLOAD_FORCE", "false").toBoolean)
System.exit(0)
