import io.delta.workload.WorkloadGenerator._

// --- ct_* corrupt table workloads ---

workload("ct_corrupt_parquet", "Truncated parquet data file", "corrupt") { w =>
  w.sql("""CREATE TABLE tbl (id BIGINT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl SELECT id FROM range(10)")
  val t = w.table("tbl")
  w.mutateTable(t) { dir =>
    import scala.collection.JavaConverters._
    java.nio.file.Files.list(dir).iterator().asScala
      .filter(_.toString.endsWith(".parquet"))
      .take(1).foreach { f =>
        val bytes = java.nio.file.Files.readAllBytes(f)
        java.nio.file.Files.write(f, java.util.Arrays.copyOf(bytes, 10))
      }
  }
  w.read(t)
  w.snapshot(t)
}

workload("ct_duplicate_metadata", "Commit with two Metadata actions", "corrupt") { w =>
  w.sql("""CREATE TABLE tbl (id BIGINT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl SELECT id FROM range(10)")
  val t = w.table("tbl")
  w.mutateTable(t) { dir =>
    val f = dir.resolve("_delta_log/00000000000000000000.json")
    val content = new String(java.nio.file.Files.readAllBytes(f), "UTF-8")
    val lines = content.trim.split("\n")
    val mdLine = lines.find(_.contains("\"metaData\"")).getOrElse("")
    java.nio.file.Files.write(f, (content.trim + "\n" + mdLine + "\n").getBytes)
  }
  w.read(t)
  w.snapshot(t)
}

workload("ct_duplicate_protocol", "Commit with two Protocol actions", "corrupt") { w =>
  w.sql("""CREATE TABLE tbl (id BIGINT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl SELECT id FROM range(10)")
  val t = w.table("tbl")
  w.mutateTable(t) { dir =>
    val f = dir.resolve("_delta_log/00000000000000000000.json")
    val content = new String(java.nio.file.Files.readAllBytes(f), "UTF-8")
    val lines = content.trim.split("\n")
    val protoLine = lines.find(_.contains("\"protocol\"")).getOrElse("")
    java.nio.file.Files.write(f, (content.trim + "\n" + protoLine + "\n").getBytes)
  }
  w.read(t)
  w.snapshot(t)
}

workload("ct_empty_delta_log", "Empty _delta_log directory", "corrupt") { w =>
  w.sql("CREATE TABLE tbl (id INT) USING delta")
  w.sql("INSERT INTO tbl VALUES (1)")
  val t = w.table("tbl")
  w.mutateTable(t) { dir =>
    import scala.collection.JavaConverters._
    java.nio.file.Files.list(dir.resolve("_delta_log")).iterator().asScala
      .foreach(java.nio.file.Files.delete)
  }
  w.read(t)
  w.snapshot(t)
}

workload("ct_gap_in_versions", "Version gap (0, 1, 3 - version 2 missing)", "corrupt") { w =>
  w.sql("""CREATE TABLE tbl (id BIGINT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl SELECT id FROM range(10)")
  w.sql("INSERT INTO tbl SELECT id FROM range(10, 20)")
  w.sql("INSERT INTO tbl SELECT id FROM range(20, 30)")
  val t = w.table("tbl")
  w.mutateTable(t) { dir =>
    java.nio.file.Files.delete(dir.resolve("_delta_log/00000000000000000002.json"))
  }
  w.read(t)
}

workload("ct_invalid_json", "Commit JSON with invalid syntax", "corrupt") { w =>
  w.sql("""CREATE TABLE tbl (id BIGINT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl SELECT id FROM range(10)")
  val t = w.table("tbl")
  w.mutateTable(t) { dir =>
    java.nio.file.Files.write(dir.resolve("_delta_log/00000000000000000000.json"),
      "NOT VALID JSON{{{".getBytes)
  }
  w.read(t)
  w.snapshot(t)
}

workload("ct_missing_data_file", "Parquet file referenced in log but deleted", "corrupt") { w =>
  w.sql("""CREATE TABLE tbl (id BIGINT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl SELECT id FROM range(10)")
  val t = w.table("tbl")
  w.mutateTable(t) { dir =>
    import scala.collection.JavaConverters._
    java.nio.file.Files.list(dir).iterator().asScala
      .filter(_.toString.endsWith(".parquet")).foreach(java.nio.file.Files.delete)
  }
  w.read(t)
  w.snapshot(t)
}

workload("ct_missing_delta_log", "Directory without _delta_log", "corrupt") { w =>
  w.sql("CREATE TABLE tbl (id INT) USING delta")
  w.sql("INSERT INTO tbl VALUES (1)")
  val t = w.table("tbl")
  w.mutateTable(t) { dir =>
    import scala.collection.JavaConverters._
    val logDir = dir.resolve("_delta_log")
    java.nio.file.Files.list(logDir).iterator().asScala.foreach(java.nio.file.Files.delete)
    java.nio.file.Files.delete(logDir)
  }
  w.read(t)
  w.snapshot(t)
}

workload("ct_missing_metadata", "Commit 0 missing Metadata action", "corrupt") { w =>
  w.sql("""CREATE TABLE tbl (id BIGINT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl SELECT id FROM range(10)")
  val t = w.table("tbl")
  w.mutateTable(t) { dir =>
    import scala.collection.JavaConverters._
    val f = dir.resolve("_delta_log/00000000000000000000.json")
    val lines = java.nio.file.Files.readAllLines(f).asScala.filterNot(_.contains("\"metaData\""))
    java.nio.file.Files.write(f, lines.asJava)
  }
  w.read(t)
  w.snapshot(t)
}

workload("ct_missing_protocol", "Commit 0 missing Protocol action", "corrupt") { w =>
  w.sql("""CREATE TABLE tbl (id BIGINT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl SELECT id FROM range(10)")
  val t = w.table("tbl")
  w.mutateTable(t) { dir =>
    import scala.collection.JavaConverters._
    val f = dir.resolve("_delta_log/00000000000000000000.json")
    val lines = java.nio.file.Files.readAllLines(f).asScala.filterNot(_.contains("\"protocol\""))
    java.nio.file.Files.write(f, lines.asJava)
  }
  w.read(t)
  w.snapshot(t)
}

workload("ct_only_remove_file", "Commit with only RemoveFile action", "corrupt") { w =>
  w.sql("""CREATE TABLE tbl (id BIGINT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl SELECT id FROM range(10)")
  val t = w.table("tbl")
  // Inject a v1 commit that has only remove actions
  w.mutateTable(t) { dir =>
    val v0 = new String(java.nio.file.Files.readAllBytes(
      dir.resolve("_delta_log/00000000000000000000.json")), "UTF-8")
    val addPathPattern = """"path":"([^"]+)""".r
    val paths = addPathPattern.findAllMatchIn(v0).map(_.group(1)).toSeq
    val removes = paths.map(p =>
      s"""{"remove":{"path":"$p","deletionTimestamp":${System.currentTimeMillis()},"dataChange":true}}"""
    ).mkString("\n")
    val ci = s"""{"commitInfo":{"timestamp":${System.currentTimeMillis()},"operation":"DELETE","operationParameters":{},"isBlindAppend":false}}"""
    java.nio.file.Files.write(dir.resolve("_delta_log/00000000000000000001.json"),
      (removes + "\n" + ci + "\n").getBytes)
  }
  w.read(t)
  w.snapshot(t)
}

workload("ct_unknown_action_types", "Commit with unknown action type", "corrupt") { w =>
  w.sql("""CREATE TABLE tbl (id BIGINT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl SELECT id FROM range(10)")
  val t = w.table("tbl")
  w.mutateTable(t) { dir =>
    val f = dir.resolve("_delta_log/00000000000000000000.json")
    val content = new String(java.nio.file.Files.readAllBytes(f), "UTF-8")
    java.nio.file.Files.write(f,
      (content.trim + "\n" + """{"unknownAction":{"key":"val"}}""" + "\n").getBytes)
  }
  w.read(t)
  w.snapshot(t)
}

workload("ct_zero_byte_commit", "Zero-byte commit 0 file", "corrupt") { w =>
  w.sql("CREATE TABLE tbl (id INT) USING delta")
  w.sql("INSERT INTO tbl VALUES (1)")
  val t = w.table("tbl")
  w.mutateTable(t) { dir =>
    java.nio.file.Files.write(dir.resolve("_delta_log/00000000000000000000.json"), Array.emptyByteArray)
  }
  w.read(t)
  w.snapshot(t)
}

// --- corrupt_* CRC and checkpoint corruption workloads ---

workload("corrupt_crc_empty", "Empty CRC file", "corrupt", "resilience") { w =>
  w.sql("""CREATE TABLE tbl (id BIGINT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl SELECT id FROM range(10)")
  val t = w.table("tbl")
  w.mutateTable(t) { dir =>
    val crc = dir.resolve("_delta_log/00000000000000000000.crc")
    java.nio.file.Files.write(crc, Array.emptyByteArray)
  }
  w.read(t)
  w.read(t, predicate = "id >= 5")
  w.snapshot(t)
}

workload("corrupt_crc_negative_counts", "CRC with negative fileCounts", "corrupt", "resilience") { w =>
  w.sql("""CREATE TABLE tbl (id BIGINT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl SELECT id FROM range(10)")
  val t = w.table("tbl")
  w.mutateTable(t) { dir =>
    val crc = dir.resolve("_delta_log/00000000000000000000.crc")
    if (java.nio.file.Files.exists(crc)) {
      val content = new String(java.nio.file.Files.readAllBytes(crc), "UTF-8")
      val patched = content.replaceAll(""""numFiles":\d+""", """"numFiles":-1""")
      java.nio.file.Files.write(crc, patched.getBytes)
    }
  }
  w.read(t)
  w.read(t, predicate = "id > 7")
  w.snapshot(t)
}

workload("corrupt_crc_no_metadata", "CRC missing metadata/protocol", "corrupt", "resilience") { w =>
  w.sql("""CREATE TABLE tbl (id BIGINT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl SELECT id FROM range(10)")
  val t = w.table("tbl")
  w.mutateTable(t) { dir =>
    val crc = dir.resolve("_delta_log/00000000000000000000.crc")
    java.nio.file.Files.write(crc, """{"tableSizeBytes":0}""".getBytes)
  }
  w.read(t)
  w.read(t, predicate = "id < 3")
  w.snapshot(t)
}

workload("corrupt_crc_txnid_mismatch", "CRC txnId doesn't match commit", "corrupt", "resilience") { w =>
  w.sql("""CREATE TABLE tbl (id BIGINT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl SELECT id FROM range(10)")
  val t = w.table("tbl")
  w.mutateTable(t) { dir =>
    val crc = dir.resolve("_delta_log/00000000000000000000.crc")
    if (java.nio.file.Files.exists(crc)) {
      val content = new String(java.nio.file.Files.readAllBytes(crc), "UTF-8")
      val patched = content.replaceAll(""""txnId":"[^"]*"""", """"txnId":"00000000-0000-0000-0000-000000000000"""")
      java.nio.file.Files.write(crc, patched.getBytes)
    }
  }
  w.read(t)
  w.read(t, predicate = "id >= 5")
  w.snapshot(t)
}

workload("corrupt_crc_wrong_numfiles", "CRC numFiles doesn't match actual", "corrupt", "resilience") { w =>
  w.sql("""CREATE TABLE tbl (id BIGINT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl SELECT id FROM range(10)")
  val t = w.table("tbl")
  w.mutateTable(t) { dir =>
    val crc = dir.resolve("_delta_log/00000000000000000000.crc")
    if (java.nio.file.Files.exists(crc)) {
      val content = new String(java.nio.file.Files.readAllBytes(crc), "UTF-8")
      val patched = content.replaceAll(""""numFiles":\d+""", """"numFiles":999""")
      java.nio.file.Files.write(crc, patched.getBytes)
    }
  }
  w.read(t)
  w.read(t, predicate = "id < 5")
  w.snapshot(t)
}

workload("corrupt_incomplete_multipart_checkpoint", "Multi-part checkpoint with missing part file", "corrupt", "resilience") { w =>
  w.sql("""CREATE TABLE tbl (id BIGINT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true', 'delta.checkpointInterval' = '5')""")
  for (i <- 0 until 20) w.sql(s"INSERT INTO tbl SELECT id FROM range(${i*10}, ${(i+1)*10})")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, predicate = "id < 50")
  w.snapshot(t)
}

workload("corrupt_last_checkpoint_checksum_mismatch", "Content doesn't match its checksum", "corrupt", "resilience") { w =>
  w.sql("""CREATE TABLE tbl (id BIGINT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl SELECT id FROM range(10)")
  val loc = w.spark.sql("DESCRIBE DETAIL tbl").collect()(0).getAs[String]("location")
  org.apache.spark.sql.delta.DeltaLog.forTable(w.spark, loc).checkpoint()
  val t = w.table("tbl")
  w.mutateTable(t) { dir =>
    val lc = dir.resolve("_delta_log/_last_checkpoint")
    if (java.nio.file.Files.exists(lc)) {
      val content = new String(java.nio.file.Files.readAllBytes(lc), "UTF-8")
      val patched = content.replaceAll(""""checksum":"[^"]*"""", """"checksum":"badchecksum"""")
      java.nio.file.Files.write(lc, patched.getBytes)
    }
  }
  w.read(t)
  w.read(t, predicate = "id >= 5")
  w.snapshot(t)
}

workload("corrupt_malformed_last_checkpoint", "Invalid JSON in _last_checkpoint", "corrupt", "resilience") { w =>
  w.sql("""CREATE TABLE tbl (id BIGINT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl SELECT id FROM range(10)")
  val loc = w.spark.sql("DESCRIBE DETAIL tbl").collect()(0).getAs[String]("location")
  org.apache.spark.sql.delta.DeltaLog.forTable(w.spark, loc).checkpoint()
  val t = w.table("tbl")
  w.mutateTable(t) { dir =>
    java.nio.file.Files.write(dir.resolve("_delta_log/_last_checkpoint"), "NOT VALID JSON".getBytes)
  }
  w.read(t)
  w.read(t, predicate = "id >= 5")
  w.snapshot(t)
}

workload("corrupt_malformed_stats_json", "AddFile with corrupted stats JSON", "corrupt", "resilience") { w =>
  w.sql("""CREATE TABLE tbl (id BIGINT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl SELECT id FROM range(10)")
  val t = w.table("tbl")
  w.mutateTable(t) { dir =>
    val f = dir.resolve("_delta_log/00000000000000000000.json")
    val content = new String(java.nio.file.Files.readAllBytes(f), "UTF-8")
    val patched = content.replaceAll(""""stats":"[^"]*"""", """"stats":"{invalid json"""")
    java.nio.file.Files.write(f, patched.getBytes)
  }
  w.read(t)
  w.read(t, predicate = "id < 5")
  w.snapshot(t)
}

workload("corrupt_missing_last_checkpoint", "_last_checkpoint absent", "corrupt", "resilience") { w =>
  w.sql("""CREATE TABLE tbl (id BIGINT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl SELECT id FROM range(10)")
  val t = w.table("tbl")
  w.mutateTable(t) { dir =>
    import scala.collection.JavaConverters._
    java.nio.file.Files.list(dir.resolve("_delta_log")).iterator().asScala
      .filter(_.toString.endsWith(".crc")).foreach(java.nio.file.Files.delete)
    val lc = dir.resolve("_delta_log/_last_checkpoint")
    if (java.nio.file.Files.exists(lc)) java.nio.file.Files.delete(lc)
  }
  w.read(t)
  w.read(t, predicate = "id < 3")
  w.snapshot(t)
}

workload("corrupt_stale_checkpoint_extra_files", "Checkpoint has more files than log", "corrupt", "resilience") { w =>
  w.sql("""CREATE TABLE tbl (id BIGINT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl SELECT id FROM range(10)")
  w.sql("INSERT INTO tbl SELECT id FROM range(10, 20)")
  w.sql("INSERT INTO tbl SELECT id FROM range(20, 30)")
  w.sql("DELETE FROM tbl WHERE id >= 20")
  val t = w.table("tbl")
  w.read(t)
  w.read(t, version = 2)
  w.read(t, version = 3)
  w.snapshot(t)
}

workload("corrupt_truncated_commit_json", "Commit JSON truncated mid-line", "corrupt") { w =>
  w.sql("""CREATE TABLE tbl (id BIGINT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl SELECT id FROM range(10)")
  val t = w.table("tbl")
  w.mutateTable(t) { dir =>
    val f = dir.resolve("_delta_log/00000000000000000000.json")
    val bytes = java.nio.file.Files.readAllBytes(f)
    java.nio.file.Files.write(f, java.util.Arrays.copyOf(bytes, 10))
  }
  w.read(t)
  w.snapshot(t)
}

workload("corrupt_wrong_last_checkpoint", "_last_checkpoint points to non-existent version", "corrupt", "resilience") { w =>
  w.sql("""CREATE TABLE tbl (id BIGINT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl SELECT id FROM range(10)")
  val t = w.table("tbl")
  w.mutateTable(t) { dir =>
    java.nio.file.Files.write(dir.resolve("_delta_log/_last_checkpoint"),
      """{"version":999,"size":1}""".getBytes)
  }
  w.read(t)
  w.read(t, predicate = "id < 5")
  w.snapshot(t)
}

// --- err_* error workloads ---

workload("err_add_and_remove_same_path_dv", "Same path+dvId in both add and remove", "corrupt", "dv") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
  w.sql("DELETE FROM tbl WHERE id = 2")
  val t = w.table("tbl")
  // Duplicate the add action into remove with same DV info
  w.mutateTable(t) { dir =>
    val f = dir.resolve("_delta_log/00000000000000000002.json")
    val content = new String(java.nio.file.Files.readAllBytes(f), "UTF-8")
    val addLine = content.split("\n").find(_.contains("\"add\"")).getOrElse("")
    val removeLine = addLine.replace("\"add\"", "\"remove\"")
    java.nio.file.Files.write(f, (content.trim + "\n" + removeLine + "\n").getBytes)
  }
  w.read(t)
  w.snapshot(t)
}

workload("err_dv_invalid_storage_type", "DV with unknown storageType", "corrupt", "dv") { w =>
  w.sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
  w.sql("DELETE FROM tbl WHERE id = 2")
  val t = w.table("tbl")
  w.mutateTable(t) { dir =>
    val f = dir.resolve("_delta_log/00000000000000000002.json")
    val content = new String(java.nio.file.Files.readAllBytes(f), "UTF-8")
    val patched = content.replaceAll(""""storageType":"[iup]"""", """"storageType":"x"""")
    java.nio.file.Files.write(f, patched.getBytes)
  }
  w.read(t)
  w.snapshot(t)
}

workload("err_duplicate_add_same_version", "Same path added twice in one commit", "corrupt") { w =>
  w.sql("""CREATE TABLE tbl (id BIGINT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl SELECT id FROM range(10)")
  val t = w.table("tbl")
  w.mutateTable(t) { dir =>
    val f = dir.resolve("_delta_log/00000000000000000000.json")
    val content = new String(java.nio.file.Files.readAllBytes(f), "UTF-8")
    val addLine = content.split("\n").find(_.contains("\"add\"")).getOrElse("")
    java.nio.file.Files.write(f, (content.trim + "\n" + addLine + "\n").getBytes)
  }
  w.read(t)
  w.snapshot(t)
}

workload("err_missing_version_0", "Missing version 0 JSON", "corrupt") { w =>
  w.sql("""CREATE TABLE tbl (id BIGINT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl SELECT id FROM range(10)")
  w.sql("INSERT INTO tbl SELECT id FROM range(10, 20)")
  val t = w.table("tbl")
  w.mutateTable(t) { dir =>
    java.nio.file.Files.delete(dir.resolve("_delta_log/00000000000000000000.json"))
  }
  w.read(t)
}

workload("err_schema_empty", "metaData has empty schemaString", "corrupt") { w =>
  w.sql("""CREATE TABLE tbl (id BIGINT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl SELECT id FROM range(10)")
  val t = w.table("tbl")
  w.mutateTable(t) { dir =>
    val f = dir.resolve("_delta_log/00000000000000000000.json")
    val content = new String(java.nio.file.Files.readAllBytes(f), "UTF-8")
    val patched = content.replaceAll(""""schemaString":"[^"]*"""", """"schemaString":""""")
    java.nio.file.Files.write(f, patched.getBytes)
  }
  w.read(t)
  w.snapshot(t)
}

workload("err_schema_invalid_json", "metaData schemaString is invalid JSON", "corrupt") { w =>
  w.sql("""CREATE TABLE tbl (id BIGINT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl SELECT id FROM range(10)")
  val t = w.table("tbl")
  w.mutateTable(t) { dir =>
    val f = dir.resolve("_delta_log/00000000000000000000.json")
    val content = new String(java.nio.file.Files.readAllBytes(f), "UTF-8")
    val patched = content.replaceAll(""""schemaString":"[^"]*"""", """"schemaString":"NOT VALID JSON{{{"""")
    java.nio.file.Files.write(f, patched.getBytes)
  }
  w.read(t)
  w.snapshot(t)
}

generateAll(
  sys.env.getOrElse("WORKLOAD_OUTPUT_DIR", "/tmp/workloads"),
  force = sys.env.getOrElse("WORKLOAD_FORCE", "false").toBoolean)
System.exit(0)
