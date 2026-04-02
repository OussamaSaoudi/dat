new WorkloadSuite("domain_metadata") {

  def injectDomainMetadata(w: WorkloadContext, t: TableHandle,
      version: Int, entries: Seq[(String, String, Boolean)]): Unit = {
    w.mutateTable(t) { tableDir =>
      val commitFile = tableDir.resolve(
        "_delta_log/" + f"$version%020d.json")
      val content = new String(java.nio.file.Files.readAllBytes(commitFile), "UTF-8")
      val lines = entries.map { case (domain, config, removed) =>
        val escapedConfig = config.replace("\"", "\\\"")
        s"""{"domainMetadata":{"domain":"$domain","configuration":"$escapedConfig","removed":$removed}}"""
      }
      java.nio.file.Files.write(commitFile,
        (content.trim + "\n" + lines.mkString("\n") + "\n").getBytes("UTF-8"))
    }
  }

  // -- Basic domain metadata --

  test("dm_basic_read", "Basic domain metadata read", "domainMetadata") { w =>
    w.sql("""CREATE TABLE tbl (id INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1),(2),(3)")
    w.sql("DELETE FROM tbl")
    val t = w.table("tbl")
    injectDomainMetadata(w, t, 2, Seq(("testDomain1", "", false)))
    w.domainMetadata(t, domain = "testDomain1", configuration = "",
      removed = false, name = "domain_metadata")
  }

  test("dm_json_config", "Domain metadata with JSON configuration", "domainMetadata") { w =>
    w.sql("""CREATE TABLE tbl (id INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1)")
    w.sql("DELETE FROM tbl")
    val t = w.table("tbl")
    injectDomainMetadata(w, t, 2,
      Seq(("testDomain2", """{"key1":"value1"}""", false)))
    w.domainMetadata(t, domain = "testDomain2", configuration = """{"key1":"value1"}""",
      removed = false, name = "domain_metadata")
  }

  test("dm_large_payload", "Domain metadata with large payload (>1KB)", "domainMetadata") { w =>
    w.sql("""CREATE TABLE tbl (id INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1)")
    w.sql("DELETE FROM tbl")
    val t = w.table("tbl")
    val pairs = (1 to 100).map(i => s""""key_$i":"xxxxxxxxxx"""").mkString(",")
    val config = s"{$pairs}"
    injectDomainMetadata(w, t, 2, Seq(("largeDomain", config, false)))
    w.domainMetadata(t, domain = "largeDomain", configuration = config,
      removed = false, name = "domain_metadata")
  }

  test("dm_multiple_domains", "Multiple domain metadata entries", "domainMetadata") { w =>
    w.sql("""CREATE TABLE tbl (id INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1)")
    w.sql("DELETE FROM tbl")
    val t = w.table("tbl")
    injectDomainMetadata(w, t, 2, Seq(
      ("testDomain1", "", false),
      ("testDomain2", """{"key1":"value1"}""", false)))
    w.domainMetadata(t, domain = "testDomain1", configuration = "",
      removed = false, name = "domain_metadata")
  }

  // -- Deletion --

  test("dm_deletion", "Domain metadata deletion", "domainMetadata") { w =>
    w.sql("""CREATE TABLE tbl (id INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1),(2),(3)")
    w.sql("DELETE FROM tbl")
    w.sql("INSERT INTO tbl VALUES (4)")
    w.sql("DELETE FROM tbl")
    val t = w.table("tbl")
    injectDomainMetadata(w, t, 2, Seq(
      ("testDomain1", "", false),
      ("testDomain2", """{"key1":"value1"}""", false)))
    injectDomainMetadata(w, t, 4, Seq(("testDomain1", "", true)))
    w.domainMetadata(t, domain = "testDomain2", configuration = """{"key1":"value1"}""",
      removed = false, name = "domain_metadata")
  }

  // -- Version-specific --

  test("dm_version_read", "Read domain metadata at specific version", "domainMetadata") { w =>
    w.sql("""CREATE TABLE tbl (id INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1)")
    w.sql("DELETE FROM tbl")
    w.sql("INSERT INTO tbl VALUES (2)")
    w.sql("DELETE FROM tbl")
    val t = w.table("tbl")
    injectDomainMetadata(w, t, 2,
      Seq(("domain_v2", """{"version":"v2"}""", false)))
    injectDomainMetadata(w, t, 4,
      Seq(("domain_v4", """{"version":"v4"}""", false)))
    w.domainMetadata(t, domain = "domain_v2", configuration = """{"version":"v2"}""",
      removed = false, version = 2, name = "domain_metadata")
  }

  // -- Checkpoint reconstruction --

  test("dm_with_checkpoint", "Domain metadata survives checkpoint", "domainMetadata") { w =>
    w.sql("""CREATE TABLE tbl (id INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1)")
    w.sql("DELETE FROM tbl")
    val t = w.table("tbl")
    injectDomainMetadata(w, t, 2, Seq(
      ("testDomain1", "", false),
      ("testDomain2", """{"key1":"value1"}""", false)))
    w.domainMetadata(t, domain = "testDomain1", configuration = "",
      removed = false, name = "domain_metadata")
  }

  // -- State reconstruction: checkpoint x CRC combinations --

  def stateReconstructionTest(
      name: String, desc: String,
      hasDeletion: Boolean, withCheckpoint: Boolean, withCrc: Boolean): Unit = {
    val tags = Seq("domainMetadata", "stateReconstruction") ++
      (if (hasDeletion) Seq("deletion") else Seq.empty)
    test(name, desc, tags: _*) { w =>
      import org.apache.spark.sql.delta.DeltaLog
      w.sql("""CREATE TABLE tbl (id INT) USING delta
        TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
      w.sql("INSERT INTO tbl VALUES (1)")
      w.sql("DELETE FROM tbl")
      if (hasDeletion) {
        w.sql("INSERT INTO tbl VALUES (2)")
        w.sql("DELETE FROM tbl")
      }
      val t = w.table("tbl")
      injectDomainMetadata(w, t, 2, Seq(
        ("testDomain1", "", false),
        ("testDomain2", """{"key1":"value1"}""", false)))
      if (hasDeletion) {
        injectDomainMetadata(w, t, 4, Seq(("testDomain1", "", true)))
      }
      if (withCheckpoint) {
        val loc = w.spark.sql("DESCRIBE DETAIL tbl").collect()(0).getAs[String]("location")
        DeltaLog.clearCache()
        DeltaLog.forTable(w.spark, loc).checkpoint()
        DeltaLog.clearCache()
      }
      if (!withCrc) {
        w.mutateTable(t) { tableDir =>
          val logDir = tableDir.resolve("_delta_log")
          val stream = java.nio.file.Files.list(logDir)
          try {
            import scala.collection.JavaConverters._
            stream.iterator().asScala
              .filter(_.toString.endsWith(".crc"))
              .foreach(java.nio.file.Files.delete)
          } finally { stream.close() }
        }
      }
      if (hasDeletion) {
        w.domainMetadata(t, domain = "testDomain2", configuration = """{"key1":"value1"}""",
          removed = false, name = "domain_metadata")
      } else {
        w.domainMetadata(t, domain = "testDomain1", configuration = "",
          removed = false, name = "domain_metadata")
        w.domainMetadata(t, domain = "testDomain2", configuration = """{"key1":"value1"}""",
          removed = false, name = "dm2")
      }
      w.snapshot(t)
    }
  }

  stateReconstructionTest("dm_state_ckpt_crc", "State: checkpoint + CRC",
    hasDeletion = false, withCheckpoint = true, withCrc = true)
  stateReconstructionTest("dm_state_ckpt_no_crc", "State: checkpoint, no CRC",
    hasDeletion = false, withCheckpoint = true, withCrc = false)
  stateReconstructionTest("dm_state_no_ckpt_crc", "State: no checkpoint, CRC",
    hasDeletion = false, withCheckpoint = false, withCrc = true)
  stateReconstructionTest("dm_state_no_ckpt_no_crc", "State: no checkpoint, no CRC",
    hasDeletion = false, withCheckpoint = false, withCrc = false)
  stateReconstructionTest("dm_deletion_ckpt_crc", "Deletion: checkpoint + CRC",
    hasDeletion = true, withCheckpoint = true, withCrc = true)
  stateReconstructionTest("dm_deletion_ckpt_no_crc", "Deletion: checkpoint, no CRC",
    hasDeletion = true, withCheckpoint = true, withCrc = false)
  stateReconstructionTest("dm_deletion_no_ckpt_crc", "Deletion: no checkpoint, CRC",
    hasDeletion = true, withCheckpoint = false, withCrc = true)
  stateReconstructionTest("dm_deletion_no_ckpt_no_crc", "Deletion: no checkpoint, no CRC",
    hasDeletion = true, withCheckpoint = false, withCrc = false)

}.runAll()
