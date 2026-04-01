import io.delta.workload.WorkloadGenerator._

// -- intv_001_interval_ym_basic: YearMonthIntervalType column --
workload("intv_001_interval_ym_basic", "YearMonthIntervalType column", "interval", "unsupportedType") { w =>
  w.sql("""CREATE TABLE tbl (id INT, period INTERVAL YEAR TO MONTH) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1, INTERVAL '1-6' YEAR TO MONTH)")
  w.sql("INSERT INTO tbl VALUES (2, INTERVAL '2-3' YEAR TO MONTH),(3, INTERVAL '0-9' YEAR TO MONTH)")
  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
}

// -- intv_002_interval_dt_basic: DayTimeIntervalType column --
workload("intv_002_interval_dt_basic", "DayTimeIntervalType column", "interval", "unsupportedType") { w =>
  w.sql("""CREATE TABLE tbl (id INT, duration INTERVAL DAY TO SECOND) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1, INTERVAL '1 02:30:00' DAY TO SECOND)")
  w.sql("INSERT INTO tbl VALUES (2, INTERVAL '3 06:45:30' DAY TO SECOND),(3, INTERVAL '0 00:15:00' DAY TO SECOND)")
  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
}

// -- intv_003_interval_partitioned: partitioned by interval --
workload("intv_003_interval_partitioned", "Partitioned by interval", "interval", "unsupportedType") { w =>
  w.sql("""CREATE TABLE tbl (id INT, period INTERVAL YEAR TO MONTH) USING delta
    PARTITIONED BY (period) TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1, INTERVAL '1-0' YEAR TO MONTH)")
  w.sql("INSERT INTO tbl VALUES (2, INTERVAL '2-0' YEAR TO MONTH)")
  w.sql("INSERT INTO tbl VALUES (3, INTERVAL '1-0' YEAR TO MONTH)")
  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
}

// -- intv_004_interval_negative: negative interval values --
workload("intv_004_interval_negative", "Negative interval values", "interval", "unsupportedType") { w =>
  w.sql("""CREATE TABLE tbl (id INT, period INTERVAL YEAR TO MONTH) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1, INTERVAL '-1-6' YEAR TO MONTH)")
  w.sql("INSERT INTO tbl VALUES (2, INTERVAL '-0-3' YEAR TO MONTH)")
  w.sql("INSERT INTO tbl VALUES (3, INTERVAL '0-0' YEAR TO MONTH)")
  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
}

// -- intv_005_interval_mixed: both YM and DT interval columns --
workload("intv_005_interval_mixed", "Both YM and DT interval columns", "interval", "unsupportedType") { w =>
  w.sql("""CREATE TABLE tbl (
    id INT,
    period INTERVAL YEAR TO MONTH,
    duration INTERVAL DAY TO SECOND
  ) USING delta TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("""INSERT INTO tbl VALUES
    (1, INTERVAL '1-0' YEAR TO MONTH, INTERVAL '1 00:00:00' DAY TO SECOND),
    (2, INTERVAL '0-6' YEAR TO MONTH, INTERVAL '0 12:30:00' DAY TO SECOND)""")
  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
}

// -- intv_boundary_values: max/min interval values --
workload("intv_boundary_values", "Max/min interval values", "interval", "unsupportedType") { w =>
  w.sql("""CREATE TABLE tbl (
    id INT,
    period INTERVAL YEAR TO MONTH,
    duration INTERVAL DAY TO SECOND
  ) USING delta TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("""INSERT INTO tbl VALUES
    (1, INTERVAL '178956970-7' YEAR TO MONTH, INTERVAL '106751991 04:00:54.775807' DAY TO SECOND),
    (2, INTERVAL '-178956970-8' YEAR TO MONTH, INTERVAL '-106751991 04:00:54.775808' DAY TO SECOND)""")
  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
}

// -- intv_sub_second: sub-second DayTimeInterval --
workload("intv_sub_second", "Sub-second DayTimeInterval", "interval", "unsupportedType") { w =>
  w.sql("""CREATE TABLE tbl (id INT, duration INTERVAL DAY TO SECOND) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("""INSERT INTO tbl VALUES
    (1, INTERVAL '0 00:00:00.001' DAY TO SECOND),
    (2, INTERVAL '0 00:00:00.999999' DAY TO SECOND),
    (3, INTERVAL '0 00:00:01.5' DAY TO SECOND)""")
  val t = w.table("tbl")
  w.read(t)
  w.snapshot(t)
}

generateAll(
  sys.env.getOrElse("WORKLOAD_OUTPUT_DIR", "/tmp/workloads"),
  force = sys.env.getOrElse("WORKLOAD_FORCE", "false").toBoolean)
System.exit(0)
