import io.delta.workload.WorkloadGenerator._

// -- stats_null_in_min_max: all-null column (null min/max) --
workload("stats_null_in_min_max", "Stats with all-null column (null min/max)", "data-skipping", "stats", "edge-case") { w =>
  w.sql("""CREATE TABLE tbl (id INT, nullable_col STRING) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1, null),(2, null),(3, null)")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "nullable_col = 'a'")
  w.read(t, predicate = "nullable_col IS NULL")
  w.snapshot(t)
}

// -- stats_numrecords_only: stats with only numRecords --
workload("stats_numrecords_only", "Stats with only numRecords", "data-skipping", "stats", "edge-case") { w =>
  w.sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1, 'a'),(2, 'b')")
  val t = w.table("tbl")
  // Strip min/max stats, keep only numRecords
  w.modifyCommitActions(t, 0) { addNode =>
    if (addNode.has("stats")) {
      val stats = addNode.get("stats").asText()
      if (stats.contains("numRecords")) {
        import com.fasterxml.jackson.databind.ObjectMapper
        val mapper = new ObjectMapper()
        val statsNode = mapper.readTree(stats)
        val newStats = mapper.createObjectNode()
        newStats.set("numRecords", statsNode.get("numRecords"))
        addNode.put("stats", mapper.writeValueAsString(newStats))
      }
    }
  }
  w.snapshot(t)
}

// -- stats_numrecords_with_dv: numRecords is physical count with DVs --
workload("stats_numrecords_with_dv", "numRecords is physical count, not logical (with DVs)", "data-skipping", "stats", "deletion-vectors", "edge-case") { w =>
  w.sql("""CREATE TABLE tbl (id LONG) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl SELECT id FROM range(10)")
  w.sql("DELETE FROM tbl WHERE id < 3")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "id >= 5")
  w.snapshot(t)
}

// -- stats_partition_col_no_stats: partition column excluded from data statistics --
workload("stats_partition_col_no_stats", "Partition column excluded from data statistics", "data-skipping", "stats", "partition", "edge-case") { w =>
  w.sql("""CREATE TABLE tbl (id INT, country STRING, amount INT) USING delta
    PARTITIONED BY (country) TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1, 'US', 100),(2, 'UK', 200),(3, 'US', 300),(4, 'UK', 400)")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "country = 'US'")
  w.read(t, predicate = "amount > 200")
  w.snapshot(t)
}

// -- stats_string_truncation: truncated string min/max stats --
workload("stats_string_truncation", "Stats with truncated string min/max (must not over-prune)", "data-skipping", "stats", "string-truncation", "edge-case") { w =>
  w.sql("""CREATE TABLE tbl (id INT, long_str STRING) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  // Include a string longer than 32 chars to trigger truncation
  w.sql("""INSERT INTO tbl VALUES
    (1, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaxyz'),
    (2, 'short'),
    (3, 'medium_length_string')""")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "long_str = 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaxyz'")
  w.read(t, predicate = "long_str = 'short'")
  w.snapshot(t)
}

// -- stats_empty_string: stats with empty string values --
workload("stats_empty_string", "Stats with empty string values", "data-skipping", "stats", "edge-case") { w =>
  w.sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1, ''),(2, 'a'),(3, '')")
  val t = w.table("tbl")
  // Strip stats completely to simulate empty stats string
  w.modifyCommitActions(t, 0) { addNode =>
    if (addNode.has("stats")) {
      addNode.put("stats", "")
    }
  }
  w.snapshot(t)
}

// -- stats_missing_entirely: stats field missing entirely --
workload("stats_missing_entirely", "Stats field missing entirely", "data-skipping", "stats", "edge-case") { w =>
  w.sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1, 'a'),(2, 'b')")
  val t = w.table("tbl")
  // Remove stats field entirely
  w.modifyCommitActions(t, 0) { addNode =>
    addNode.remove("stats")
  }
  w.snapshot(t)
}

generateAll(
  sys.env.getOrElse("WORKLOAD_OUTPUT_DIR", "/tmp/workloads"),
  force = sys.env.getOrElse("WORKLOAD_FORCE", "false").toBoolean)
System.exit(0)
