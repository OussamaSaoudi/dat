import io.delta.workload.WorkloadGenerator._

workload("corrupt_missing_file", "Deleted parquet file", "corrupt") { w =>
  w.sql("CREATE TABLE tbl (id INT) USING delta")
  w.sql("INSERT INTO tbl SELECT id FROM range(100)")
  val t = w.table("tbl")
  w.mutateTable(t) { dir =>
    import scala.collection.JavaConverters._
    java.nio.file.Files.list(dir).iterator().asScala
      .filter(_.toString.endsWith(".parquet")).take(1).foreach(java.nio.file.Files.delete)
  }
  w.read(t)
  w.snapshot(t)
}

workload("corrupt_truncated_commit", "Half-written commit", "corrupt") { w =>
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

workload("corrupt_no_crc", "No CRC files", "corrupt") { w =>
  w.sql("CREATE TABLE tbl (id LONG) USING delta")
  w.sql("INSERT INTO tbl SELECT id FROM range(10)")
  val t = w.table("tbl")
  w.mutateTable(t) { dir =>
    import scala.collection.JavaConverters._
    java.nio.file.Files.list(dir.resolve("_delta_log")).iterator().asScala
      .filter(_.toString.endsWith(".crc")).foreach(java.nio.file.Files.delete)
  }
  w.read(t)
  w.read(t, predicate = "id < 5")
  w.snapshot(t)
}

workload("corrupt_empty_crc", "Empty CRC file", "corrupt") { w =>
  w.sql("CREATE TABLE tbl (id LONG) USING delta")
  w.sql("INSERT INTO tbl SELECT id FROM range(10)")
  val t = w.table("tbl")
  w.mutateTable(t) { dir =>
    val crc = dir.resolve("_delta_log/00000000000000000000.crc")
    java.nio.file.Files.write(crc, Array.emptyByteArray)
  }
  w.read(t)
  w.snapshot(t)
}

workload("corrupt_bad_stats", "Bogus file stats", "corrupt") { w =>
  w.sql("CREATE TABLE tbl (id INT, value STRING) USING delta")
  w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
  val t = w.table("tbl")
  w.modifyCommitActions(t, version = 1) { addNode =>
    addNode.put("stats", """{"numRecords":999}""")
  }
  w.read(t)
  w.read(t, predicate = "id > 1")
  w.snapshot(t)
}

workload("corrupt_version_gap", "Missing version in log", "corrupt") { w =>
  w.sql("CREATE TABLE tbl (id INT) USING delta")
  w.sql("INSERT INTO tbl VALUES (1)")
  w.sql("INSERT INTO tbl VALUES (2)")
  w.sql("INSERT INTO tbl VALUES (3)")
  val t = w.table("tbl")
  w.mutateTable(t) { dir =>
    java.nio.file.Files.delete(dir.resolve("_delta_log/00000000000000000002.json"))
  }
  w.read(t)
}

workload("corrupt_no_protocol", "Protocol stripped from commit 0", "corrupt") { w =>
  w.sql("CREATE TABLE tbl (id INT) USING delta")
  w.sql("INSERT INTO tbl VALUES (1)")
  val t = w.table("tbl")
  w.mutateTable(t) { dir =>
    import scala.collection.JavaConverters._
    val f = dir.resolve("_delta_log/00000000000000000000.json")
    val lines = java.nio.file.Files.readAllLines(f).asScala.filterNot(_.contains("\"protocol\""))
    java.nio.file.Files.write(f, lines.asJava)
  }
  w.read(t)
}

workload("corrupt_no_metadata", "Metadata stripped from commit 0", "corrupt") { w =>
  w.sql("CREATE TABLE tbl (id INT) USING delta")
  w.sql("INSERT INTO tbl VALUES (1)")
  val t = w.table("tbl")
  w.mutateTable(t) { dir =>
    import scala.collection.JavaConverters._
    val f = dir.resolve("_delta_log/00000000000000000000.json")
    val lines = java.nio.file.Files.readAllLines(f).asScala.filterNot(_.contains("\"metaData\""))
    java.nio.file.Files.write(f, lines.asJava)
  }
  w.read(t)
}

workload("corrupt_stale_last_checkpoint", "Stale _last_checkpoint hint", "corrupt") { w =>
  w.sql("CREATE TABLE tbl (id INT) USING delta")
  w.sql("INSERT INTO tbl SELECT id FROM range(10)")
  val loc = w.spark.sql("DESCRIBE DETAIL tbl").collect()(0).getAs[String]("location")
  org.apache.spark.sql.delta.DeltaLog.forTable(w.spark, loc).checkpoint()
  val t = w.table("tbl")
  w.mutateTable(t) { dir =>
    java.nio.file.Files.write(dir.resolve("_delta_log/_last_checkpoint"), """{"version":999}""".getBytes)
  }
  w.read(t)
  w.snapshot(t)
}

workload("corrupt_invalid_last_checkpoint", "Invalid JSON in _last_checkpoint", "corrupt") { w =>
  w.sql("CREATE TABLE tbl (id INT) USING delta")
  w.sql("INSERT INTO tbl SELECT id FROM range(10)")
  val loc = w.spark.sql("DESCRIBE DETAIL tbl").collect()(0).getAs[String]("location")
  org.apache.spark.sql.delta.DeltaLog.forTable(w.spark, loc).checkpoint()
  val t = w.table("tbl")
  w.mutateTable(t) { dir =>
    java.nio.file.Files.write(dir.resolve("_delta_log/_last_checkpoint"), "NOT VALID JSON".getBytes)
  }
  w.read(t)
  w.snapshot(t)
}

workload("corrupt_empty_delta_log", "Empty _delta_log", "corrupt") { w =>
  w.sql("CREATE TABLE tbl (id INT) USING delta")
  w.sql("INSERT INTO tbl VALUES (1)")
  val t = w.table("tbl")
  w.mutateTable(t) { dir =>
    import scala.collection.JavaConverters._
    java.nio.file.Files.list(dir.resolve("_delta_log")).iterator().asScala.foreach(java.nio.file.Files.delete)
  }
  w.read(t)
}

workload("corrupt_zero_byte_commit", "Zero-byte commit file", "corrupt") { w =>
  w.sql("CREATE TABLE tbl (id INT) USING delta")
  w.sql("INSERT INTO tbl VALUES (1)")
  val t = w.table("tbl")
  w.mutateTable(t) { dir =>
    java.nio.file.Files.write(dir.resolve("_delta_log/00000000000000000000.json"), Array.emptyByteArray)
  }
  w.read(t)
}

workload("corrupt_dv_garbled", "Garbled DV binary", "corrupt", "dv") { w =>
  w.sql("""CREATE TABLE tbl (id INT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl SELECT id FROM range(100)")
  w.sql("DELETE FROM tbl WHERE id < 10")
  val t = w.table("tbl")
  w.mutateTable(t) { dir =>
    import scala.collection.JavaConverters._
    java.nio.file.Files.list(dir).iterator().asScala
      .filter(_.getFileName.toString.contains("deletion_vector"))
      .foreach(f => java.nio.file.Files.write(f, Array[Byte](0,1,2,3)))
  }
  w.read(t)
}

workload("corrupt_unknown_action", "Unknown action type in commit", "corrupt") { w =>
  w.sql("CREATE TABLE tbl (id LONG) USING delta")
  w.sql("INSERT INTO tbl SELECT id FROM range(10)")
  val t = w.table("tbl")
  w.mutateTable(t) { dir =>
    val f = dir.resolve("_delta_log/00000000000000000000.json")
    val content = new String(java.nio.file.Files.readAllBytes(f), "UTF-8")
    java.nio.file.Files.write(f, (content.trim + "\n" + """{"unknownAction":{"key":"val"}}""" + "\n").getBytes)
  }
  w.read(t)
  w.snapshot(t)
}

generateAll(
  sys.env.getOrElse("WORKLOAD_OUTPUT_DIR", "/tmp/workloads"),
  force = sys.env.getOrElse("WORKLOAD_FORCE", "false").toBoolean)
System.exit(0)
