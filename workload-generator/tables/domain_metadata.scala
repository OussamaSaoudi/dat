new WorkloadSuite("domain_metadata") {

  def injectDomainMetadata(t: TableHandle,
      version: Int, entries: Seq[(String, String, Boolean)]): Unit = {
    mutateTable(t) { tableDir =>
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



  test("dm_basic_read", "Basic domain metadata read", "domainMetadata") {
    sql("""CREATE TABLE tbl (id INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1),(2),(3)")
    sql("DELETE FROM tbl")
    val t = registerTable("tbl")
    injectDomainMetadata(t, 2, Seq(("testDomain1", "", false)))
    domainMetadata(t, domain = "testDomain1", configuration = "",
      removed = false, name = "domain_metadata")
  }

  test("dm_json_config", "Domain metadata with JSON configuration", "domainMetadata") {
    sql("""CREATE TABLE tbl (id INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1)")
    sql("DELETE FROM tbl")
    val t = registerTable("tbl")
    injectDomainMetadata(t, 2,
      Seq(("testDomain2", """{"key1":"value1"}""", false)))
    domainMetadata(t, domain = "testDomain2", configuration = """{"key1":"value1"}""",
      removed = false, name = "domain_metadata")
  }

  test("dm_large_payload", "Domain metadata with large payload (>1KB)", "domainMetadata") {
    sql("""CREATE TABLE tbl (id INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1)")
    sql("DELETE FROM tbl")
    val t = registerTable("tbl")
    val pairs = (1 to 100).map(i => s""""key_$i":"xxxxxxxxxx"""").mkString(",")
    val config = s"{$pairs}"
    injectDomainMetadata(t, 2, Seq(("largeDomain", config, false)))
    domainMetadata(t, domain = "largeDomain", configuration = config,
      removed = false, name = "domain_metadata")
  }

  test("dm_multiple_domains", "Multiple domain metadata entries", "domainMetadata") {
    sql("""CREATE TABLE tbl (id INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1)")
    sql("DELETE FROM tbl")
    val t = registerTable("tbl")
    injectDomainMetadata(t, 2, Seq(
      ("testDomain1", "", false),
      ("testDomain2", """{"key1":"value1"}""", false)))
    domainMetadata(t, domain = "testDomain1", configuration = "",
      removed = false, name = "domain_metadata")
  }


  test("dm_deletion", "Domain metadata deletion", "domainMetadata") {
    sql("""CREATE TABLE tbl (id INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1),(2),(3)")
    sql("DELETE FROM tbl")
    sql("INSERT INTO tbl VALUES (4)")
    sql("DELETE FROM tbl")
    val t = registerTable("tbl")
    injectDomainMetadata(t, 2, Seq(
      ("testDomain1", "", false),
      ("testDomain2", """{"key1":"value1"}""", false)))
    injectDomainMetadata(t, 4, Seq(("testDomain1", "", true)))
    domainMetadata(t, domain = "testDomain2", configuration = """{"key1":"value1"}""",
      removed = false, name = "domain_metadata")
  }


  test("dm_version_read", "Read domain metadata at specific version", "domainMetadata") {
    sql("""CREATE TABLE tbl (id INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1)")
    sql("DELETE FROM tbl")
    sql("INSERT INTO tbl VALUES (2)")
    sql("DELETE FROM tbl")
    val t = registerTable("tbl")
    injectDomainMetadata(t, 2,
      Seq(("domain_v2", """{"version":"v2"}""", false)))
    injectDomainMetadata(t, 4,
      Seq(("domain_v4", """{"version":"v4"}""", false)))
    domainMetadata(t, domain = "domain_v2", configuration = """{"version":"v2"}""",
      removed = false, version = 2, name = "domain_metadata")
  }


  test("dm_with_checkpoint", "Domain metadata survives checkpoint", "domainMetadata") {
    sql("""CREATE TABLE tbl (id INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1)")
    sql("DELETE FROM tbl")
    val t = registerTable("tbl")
    injectDomainMetadata(t, 2, Seq(
      ("testDomain1", "", false),
      ("testDomain2", """{"key1":"value1"}""", false)))
    domainMetadata(t, domain = "testDomain1", configuration = "",
      removed = false, name = "domain_metadata")
  }


  def stateReconstructionTest(
      name: String, desc: String,
      hasDeletion: Boolean, withCheckpoint: Boolean, withCrc: Boolean): Unit = {
    val tags = Seq("domainMetadata", "stateReconstruction") ++
      (if (hasDeletion) Seq("deletion") else Seq.empty)
    test(name, desc, tags: _*) {
      sql("""CREATE TABLE tbl (id INT) USING delta
        TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
      sql("INSERT INTO tbl VALUES (1)")
      if (withCheckpoint) {
        forceCheckpoint("tbl")
      }
      sql("DELETE FROM tbl")
      if (hasDeletion) {
        sql("INSERT INTO tbl VALUES (2)")
        sql("DELETE FROM tbl")
      }
      val t = registerTable("tbl")
      injectDomainMetadata(t, 2, Seq(
        ("testDomain1", "", false),
        ("testDomain2", """{"key1":"value1"}""", false)))
      if (hasDeletion) {
        injectDomainMetadata(t, 4, Seq(("testDomain1", "", true)))
      }
      if (!withCrc) {
        mutateTable(t) { tableDir =>
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
        domainMetadata(t, domain = "testDomain2", configuration = """{"key1":"value1"}""",
          removed = false, name = "domain_metadata")
      } else {
        domainMetadata(t, domain = "testDomain1", configuration = "",
          removed = false, name = "domain_metadata")
        domainMetadata(t, domain = "testDomain2", configuration = """{"key1":"value1"}""",
          removed = false, name = "dm2")
      }
      snapshot(t)
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

}
