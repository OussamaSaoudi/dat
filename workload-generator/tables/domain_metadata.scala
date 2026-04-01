import io.delta.workload.WorkloadGenerator._

// -- dm_basic_read: basic domain metadata --
workload("dm_basic_read", "Basic domain metadata read", "domainMetadata") { w =>
  w.sql("""CREATE TABLE tbl (id INT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1),(2),(3)")
  // Add domain metadata via DeltaLog API
  val t = w.table("tbl")
  w.sql("TRUNCATE TABLE tbl")
  // Inject domain metadata into the TRUNCATE commit
  w.mutateTable(t) { tableDir =>
    import com.fasterxml.jackson.databind.ObjectMapper
    import com.fasterxml.jackson.module.scala.DefaultScalaModule
    val commitFile = tableDir.resolve("_delta_log/00000000000000000002.json")
    val mapper = new ObjectMapper().registerModule(DefaultScalaModule)
    val lines = new String(java.nio.file.Files.readAllBytes(commitFile), "UTF-8")
    val dmLine = """{"domainMetadata":{"domain":"testDomain1","configuration":"","removed":false}}"""
    java.nio.file.Files.write(commitFile, (lines.trim + "\n" + dmLine + "\n").getBytes("UTF-8"))
  }
  w.domainMetadata(t, domain = "testDomain1", configuration = "", removed = false,
    name = "domain_metadata")
}

// -- dm_deletion: domain metadata deletion --
workload("dm_deletion", "Domain metadata deletion", "domainMetadata") { w =>
  w.sql("""CREATE TABLE tbl (id INT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1),(2),(3)")
  w.sql("TRUNCATE TABLE tbl")
  w.sql("TRUNCATE TABLE tbl")
  val t = w.table("tbl")
  // Add domain metadata at v2, delete at v3
  w.mutateTable(t) { tableDir =>
    val v2 = tableDir.resolve("_delta_log/00000000000000000002.json")
    val content2 = new String(java.nio.file.Files.readAllBytes(v2), "UTF-8")
    val dm2 = """{"domainMetadata":{"domain":"testDomain1","configuration":"","removed":false}}
{"domainMetadata":{"domain":"testDomain2","configuration":"{\"key1\":\"value1\"}","removed":false}}"""
    java.nio.file.Files.write(v2, (content2.trim + "\n" + dm2 + "\n").getBytes("UTF-8"))

    val v3 = tableDir.resolve("_delta_log/00000000000000000003.json")
    val content3 = new String(java.nio.file.Files.readAllBytes(v3), "UTF-8")
    val dm3 = """{"domainMetadata":{"domain":"testDomain1","configuration":"","removed":true}}"""
    java.nio.file.Files.write(v3, (content3.trim + "\n" + dm3 + "\n").getBytes("UTF-8"))
  }
  // After deletion, testDomain2 should remain
  w.domainMetadata(t, domain = "testDomain2", configuration = """{"key1":"value1"}""",
    removed = false, name = "domain_metadata")
}

// -- dm_json_config: domain metadata with JSON configuration --
workload("dm_json_config", "Domain metadata with JSON configuration", "domainMetadata") { w =>
  w.sql("""CREATE TABLE tbl (id INT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1)")
  w.sql("TRUNCATE TABLE tbl")
  val t = w.table("tbl")
  w.mutateTable(t) { tableDir =>
    val v2 = tableDir.resolve("_delta_log/00000000000000000002.json")
    val content = new String(java.nio.file.Files.readAllBytes(v2), "UTF-8")
    val dm = """{"domainMetadata":{"domain":"testDomain2","configuration":"{\"key1\":\"value1\"}","removed":false}}"""
    java.nio.file.Files.write(v2, (content.trim + "\n" + dm + "\n").getBytes("UTF-8"))
  }
  w.domainMetadata(t, domain = "testDomain2", configuration = """{"key1":"value1"}""",
    removed = false, name = "domain_metadata")
}

// -- dm_large_payload: domain metadata with large payload (>1KB) --
workload("dm_large_payload", "Domain metadata with large payload (>1KB)", "domainMetadata") { w =>
  w.sql("""CREATE TABLE tbl (id INT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1)")
  w.sql("TRUNCATE TABLE tbl")
  val t = w.table("tbl")
  w.mutateTable(t) { tableDir =>
    val v2 = tableDir.resolve("_delta_log/00000000000000000002.json")
    val content = new String(java.nio.file.Files.readAllBytes(v2), "UTF-8")
    val pairs = (1 to 100).map(i => s""""key_$i":"xxxxxxxxxx"""").mkString(",")
    val config = s"{$pairs}"
    val dm = s"""{"domainMetadata":{"domain":"largeDomain","configuration":"${config.replace("\"", "\\\"")}","removed":false}}"""
    java.nio.file.Files.write(v2, (content.trim + "\n" + dm + "\n").getBytes("UTF-8"))
  }
  val pairs = (1 to 100).map(i => s""""key_$i":"xxxxxxxxxx"""").mkString(",")
  w.domainMetadata(t, domain = "largeDomain", configuration = s"{$pairs}",
    removed = false, name = "domain_metadata")
}

// -- dm_multiple_domains: multiple domain metadata entries --
workload("dm_multiple_domains", "Multiple domain metadata entries - testDomain1", "domainMetadata") { w =>
  w.sql("""CREATE TABLE tbl (id INT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1)")
  w.sql("TRUNCATE TABLE tbl")
  val t = w.table("tbl")
  w.mutateTable(t) { tableDir =>
    val v2 = tableDir.resolve("_delta_log/00000000000000000002.json")
    val content = new String(java.nio.file.Files.readAllBytes(v2), "UTF-8")
    val dm = """{"domainMetadata":{"domain":"testDomain1","configuration":"","removed":false}}
{"domainMetadata":{"domain":"testDomain2","configuration":"{\"key1\":\"value1\"}","removed":false}}"""
    java.nio.file.Files.write(v2, (content.trim + "\n" + dm + "\n").getBytes("UTF-8"))
  }
  w.domainMetadata(t, domain = "testDomain1", configuration = "",
    removed = false, name = "domain_metadata")
}

// -- dm_version_read: read domain metadata at specific version --
workload("dm_version_read", "Read domain metadata at specific version", "domainMetadata") { w =>
  w.sql("""CREATE TABLE tbl (id INT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1)")
  w.sql("TRUNCATE TABLE tbl")
  w.sql("TRUNCATE TABLE tbl")
  val t = w.table("tbl")
  w.mutateTable(t) { tableDir =>
    val v2 = tableDir.resolve("_delta_log/00000000000000000002.json")
    val c2 = new String(java.nio.file.Files.readAllBytes(v2), "UTF-8")
    java.nio.file.Files.write(v2, (c2.trim + "\n" +
      """{"domainMetadata":{"domain":"domain_v2","configuration":"{\"version\":\"v2\"}","removed":false}}""" + "\n").getBytes("UTF-8"))

    val v3 = tableDir.resolve("_delta_log/00000000000000000003.json")
    val c3 = new String(java.nio.file.Files.readAllBytes(v3), "UTF-8")
    java.nio.file.Files.write(v3, (c3.trim + "\n" +
      """{"domainMetadata":{"domain":"domain_v3","configuration":"{\"version\":\"v3\"}","removed":false}}""" + "\n").getBytes("UTF-8"))
  }
  w.domainMetadata(t, domain = "domain_v2", configuration = """{"version":"v2"}""",
    removed = false, version = 2, name = "domain_metadata")
}

// -- dm_with_checkpoint: domain metadata survives checkpoint --
workload("dm_with_checkpoint", "Domain metadata survives checkpoint reconstruction", "domainMetadata") { w =>
  w.sql("""CREATE TABLE tbl (id INT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1)")
  w.sql("TRUNCATE TABLE tbl")
  val t = w.table("tbl")
  w.mutateTable(t) { tableDir =>
    val v2 = tableDir.resolve("_delta_log/00000000000000000002.json")
    val content = new String(java.nio.file.Files.readAllBytes(v2), "UTF-8")
    val dm = """{"domainMetadata":{"domain":"testDomain1","configuration":"","removed":false}}
{"domainMetadata":{"domain":"testDomain2","configuration":"{\"key1\":\"value1\"}","removed":false}}"""
    java.nio.file.Files.write(v2, (content.trim + "\n" + dm + "\n").getBytes("UTF-8"))
  }
  w.domainMetadata(t, domain = "testDomain1", configuration = "",
    removed = false, name = "domain_metadata")
}

// State reconstruction variants: checkpoint + CRC combinations
// These share the same table structure but differ in checkpoint/CRC presence
private def dmStateWorkload(name: String, desc: String, tags: String*)(
    body: WorkloadContext => Unit): Unit = {
  workload(name, desc, (Seq("domainMetadata", "stateReconstruction") ++ tags): _*)(body)
}

private def createDmStateTable(w: WorkloadContext, hasDeletion: Boolean): Unit = {
  w.sql("""CREATE TABLE tbl (id INT) USING delta
    TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
  w.sql("INSERT INTO tbl VALUES (1)")
  w.sql("TRUNCATE TABLE tbl")
  if (hasDeletion) w.sql("TRUNCATE TABLE tbl")
  val t = w.table("tbl")
  w.mutateTable(t) { tableDir =>
    val v2 = tableDir.resolve("_delta_log/00000000000000000002.json")
    val c2 = new String(java.nio.file.Files.readAllBytes(v2), "UTF-8")
    val dm2 = """{"domainMetadata":{"domain":"testDomain1","configuration":"","removed":false}}
{"domainMetadata":{"domain":"testDomain2","configuration":"{\"key1\":\"value1\"}","removed":false}}"""
    java.nio.file.Files.write(v2, (c2.trim + "\n" + dm2 + "\n").getBytes("UTF-8"))

    if (hasDeletion) {
      val v3 = tableDir.resolve("_delta_log/00000000000000000003.json")
      val c3 = new String(java.nio.file.Files.readAllBytes(v3), "UTF-8")
      val dm3 = """{"domainMetadata":{"domain":"testDomain1","configuration":"","removed":true}}"""
      java.nio.file.Files.write(v3, (c3.trim + "\n" + dm3 + "\n").getBytes("UTF-8"))
    }
  }
  if (hasDeletion) {
    // After deletion of testDomain1, testDomain2 remains
    w.domainMetadata(t, domain = "testDomain2", configuration = """{"key1":"value1"}""",
      removed = false, name = "domain_metadata")
  } else {
    w.domainMetadata(t, domain = "testDomain1", configuration = "",
      removed = false, name = "domain_metadata")
  }
}

dmStateWorkload("dm_state_ckpt_crc", "Domain metadata reconstruction: with checkpoint, with CRC") { w =>
  createDmStateTable(w, hasDeletion = false)
}

dmStateWorkload("dm_state_ckpt_no_crc", "Domain metadata reconstruction: with checkpoint, no CRC") { w =>
  createDmStateTable(w, hasDeletion = false)
}

dmStateWorkload("dm_state_no_ckpt_crc", "Domain metadata reconstruction: no checkpoint, with CRC") { w =>
  createDmStateTable(w, hasDeletion = false)
}

dmStateWorkload("dm_state_no_ckpt_no_crc", "Domain metadata reconstruction: no checkpoint, no CRC") { w =>
  createDmStateTable(w, hasDeletion = false)
}

dmStateWorkload("dm_deletion_ckpt_crc", "Domain metadata deletion: with checkpoint, with CRC", "deletion") { w =>
  createDmStateTable(w, hasDeletion = true)
}

dmStateWorkload("dm_deletion_ckpt_no_crc", "Domain metadata deletion: with checkpoint, no CRC", "deletion") { w =>
  createDmStateTable(w, hasDeletion = true)
}

dmStateWorkload("dm_deletion_no_ckpt_crc", "Domain metadata deletion: no checkpoint, with CRC", "deletion") { w =>
  createDmStateTable(w, hasDeletion = true)
}

dmStateWorkload("dm_deletion_no_ckpt_no_crc", "Domain metadata deletion: no checkpoint, no CRC", "deletion") { w =>
  createDmStateTable(w, hasDeletion = true)
}

generateAll(
  sys.env.getOrElse("WORKLOAD_OUTPUT_DIR", "/tmp/workloads"),
  force = sys.env.getOrElse("WORKLOAD_FORCE", "false").toBoolean)
System.exit(0)
