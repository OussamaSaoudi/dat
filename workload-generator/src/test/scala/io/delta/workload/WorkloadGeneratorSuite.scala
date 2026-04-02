/*
 * Copyright (2025) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.delta.workload

import java.nio.file.{Files, Path}

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.DeltaLog
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class WorkloadGeneratorSuite extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private var outputDir: Path = _
  private var warehouseDir: Path = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    outputDir = Files.createTempDirectory("workload-test-")
    warehouseDir = Files.createTempDirectory("workload-warehouse-")
    spark = SparkSession.builder()
      .master("local[2]")
      .appName("WorkloadGeneratorSuite")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.warehouse.dir", warehouseDir.toString)
      .config("spark.ui.enabled", "false")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
    Seq(outputDir, warehouseDir).foreach { d =>
      if (d != null) org.apache.commons.io.FileUtils.deleteDirectory(d.toFile)
    }
    super.afterAll()
  }

  private def run(force: Boolean = true)(body: WorkloadSuite => Unit): Seq[TestResult] = {
    val suite = new WorkloadSuite("test") {}
    body(suite)
    sys.props("WORKLOAD_OUTPUT_DIR") = outputDir.toString
    sys.props("WORKLOAD_FORCE") = force.toString
    suite.run()
  }

  private def assertPassed(results: Seq[TestResult]): Unit = {
    results.foreach { r =>
      assert(r.passed, s"${r.testId} failed: ${r.errors.mkString("; ")}")
    }
  }

  private def dir(name: String): Path = outputDir.resolve(name)
  private def specs(name: String): Path = dir(name).resolve("specs")
  private def expected(name: String): Path = dir(name).resolve("expected")
  private def delta(name: String): Path = dir(name).resolve("delta")

  private def readSpec(name: String, specName: String): com.fasterxml.jackson.databind.JsonNode = {
    val f = specs(name).resolve(s"${name}_$specName.json")
    assert(Files.exists(f), s"Spec file missing: $f")
    JsonUtil.mapper.readTree(Files.readAllBytes(f))
  }

  // =========================================================================
  // Read specs
  // =========================================================================

  test("read: basic table read with row validation") {
    val results = run() { s =>
      s.test("t_r1", "basic read") { w =>
        w.sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
        w.sql("INSERT INTO tbl VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        val t = w.table("tbl")
        w.read(t)
      }
    }
    assertPassed(results)

    // Verify expected data matches actual table
    DeltaLog.clearCache()
    val actual = JsonUtil.toRowMultiset(
      spark.read.format("delta").load(delta("t_r1").toString))
    val expectedData = JsonUtil.toRowMultiset(
      spark.read.parquet(expected("t_r1").resolve("t_r1_read/expected_data").toString))
    assert(actual == expectedData, "Expected data should match actual table data")
    assert(actual.values.sum == 3, "Should have 3 rows")
  }

  test("read: predicate filters rows correctly") {
    val results = run() { s =>
      s.test("t_r2", "predicate read") { w =>
        w.sql("CREATE TABLE tbl (id INT, val STRING) USING delta")
        w.sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e')")
        val t = w.table("tbl")
        w.read(t, predicate = "id > 3")
      }
    }
    assertPassed(results)
    val expectedData = JsonUtil.toRowMultiset(
      spark.read.parquet(expected("t_r2").resolve("t_r2_read_id_gt_3/expected_data").toString))
    assert(expectedData.values.sum == 2, "Predicate id > 3 should yield 2 rows")
  }

  test("read: version time travel") {
    val results = run() { s =>
      s.test("t_r3", "version read") { w =>
        w.sql("CREATE TABLE tbl (id INT) USING delta")
        w.sql("INSERT INTO tbl VALUES (1)")
        w.sql("INSERT INTO tbl VALUES (2)")
        w.sql("INSERT INTO tbl VALUES (3)")
        val t = w.table("tbl")
        w.read(t, version = 1)
        w.read(t, version = 2)
        w.read(t)
      }
    }
    assertPassed(results)
    val v1 = JsonUtil.toRowMultiset(
      spark.read.parquet(expected("t_r3").resolve("t_r3_read_v1/expected_data").toString))
    val v2 = JsonUtil.toRowMultiset(
      spark.read.parquet(expected("t_r3").resolve("t_r3_read_v2/expected_data").toString))
    val latest = JsonUtil.toRowMultiset(
      spark.read.parquet(expected("t_r3").resolve("t_r3_read/expected_data").toString))
    assert(v1.values.sum == 1, "Version 1 should have 1 row")
    assert(v2.values.sum == 2, "Version 2 should have 2 rows")
    assert(latest.values.sum == 3, "Latest should have 3 rows")
  }

  test("read: column projection") {
    val results = run() { s =>
      s.test("t_r4", "column projection") { w =>
        w.sql("CREATE TABLE tbl (a INT, b STRING, c DOUBLE) USING delta")
        w.sql("INSERT INTO tbl VALUES (1, 'x', 1.1), (2, 'y', 2.2)")
        val t = w.table("tbl")
        w.read(t, columns = Seq("a", "c"))
      }
    }
    assertPassed(results)
    val df = spark.read.parquet(
      expected("t_r4").resolve("t_r4_read_cols_a_c/expected_data").toString)
    assert(df.columns.toSet == Set("a", "c"), "Should only have projected columns")
    assert(df.count() == 2)
  }

  test("read: partitioned table") {
    val results = run() { s =>
      s.test("t_r5", "partitioned read") { w =>
        w.sql("""CREATE TABLE tbl (id INT, region STRING)
          USING delta PARTITIONED BY (region)""")
        w.sql("INSERT INTO tbl VALUES (1,'us'),(2,'us'),(3,'eu'),(4,'eu')")
        val t = w.table("tbl")
        w.read(t, predicate = "region = 'us'")
        w.read(t)
      }
    }
    assertPassed(results)
    val filtered = JsonUtil.toRowMultiset(
      spark.read.parquet(
        expected("t_r5").resolve("t_r5_read_region_eq_us/expected_data").toString))
    val all = JsonUtil.toRowMultiset(
      spark.read.parquet(expected("t_r5").resolve("t_r5_read/expected_data").toString))
    assert(filtered.values.sum == 2, "Filtered should have 2 rows")
    assert(all.values.sum == 4, "All should have 4 rows")
  }

  test("read: empty table") {
    val results = run() { s =>
      s.test("t_r6", "empty table read") { w =>
        w.sql("CREATE TABLE tbl (id INT) USING delta")
        val t = w.table("tbl")
        w.read(t)
        w.snapshot(t)
      }
    }
    assertPassed(results)
  }

  test("read: null values preserved") {
    val results = run() { s =>
      s.test("t_r7", "null values") { w =>
        w.sql("CREATE TABLE tbl (id INT, name STRING) USING delta")
        w.sql("INSERT INTO tbl VALUES (1, null), (2, 'b'), (null, 'c')")
        val t = w.table("tbl")
        w.read(t)
      }
    }
    assertPassed(results)
    val data = JsonUtil.toRowMultiset(
      spark.read.parquet(expected("t_r7").resolve("t_r7_read/expected_data").toString))
    assert(data.values.sum == 3)
  }

  test("read: spec JSON has correct structure") {
    val results = run() { s =>
      s.test("t_r8", "spec structure") { w =>
        w.sql("CREATE TABLE tbl (id INT) USING delta")
        w.sql("INSERT INTO tbl VALUES (1)")
        val t = w.table("tbl")
        w.read(t, predicate = "id > 0", version = 1, columns = Seq("id"))
      }
    }
    assertPassed(results)
    val spec = readSpec("t_r8", "read_v1_id_gt_0_cols_id")
    assert(spec.get("type").asText() == "read")
    assert(spec.get("version").asInt() == 1)
    assert(spec.get("predicate").asText() == "id > 0")
    assert(spec.get("expected").get("rowCount").asInt() == 1,
      "One row matches id > 0 at version 1")
  }

  // =========================================================================
  // Error specs
  // =========================================================================

  test("error: nonexistent version produces error spec") {
    val results = run() { s =>
      s.test("t_e1", "error on bad version") { w =>
        w.sql("CREATE TABLE tbl (id INT) USING delta")
        w.sql("INSERT INTO tbl VALUES (1)")
        val t = w.table("tbl")
        w.read(t, version = 999)
      }
    }
    assert(results.size == 1)
    if (results.head.passed) {
      val spec = readSpec("t_e1", "read_v999")
      assert(spec.get("type").asText() == "read")
      assert(spec.get("version").asInt() == 999)
      assert(spec.get("error").get("errorCode").asText() == "VersionNotFoundException",
        s"Error code should be VersionNotFoundException, got: ${spec.get("error").get("errorCode").asText()}")
      assert(spec.get("error").get("errorMessage").asText().contains("999"),
        "Error message should reference version 999")
    }
  }

  // =========================================================================
  // Snapshot specs
  // =========================================================================

  test("snapshot: captures protocol and metadata") {
    val results = run() { s =>
      s.test("t_s1", "basic snapshot") { w =>
        w.sql("CREATE TABLE tbl (id INT) USING delta")
        w.sql("INSERT INTO tbl VALUES (1)")
        val t = w.table("tbl")
        w.snapshot(t)
      }
    }
    assertPassed(results)
    val spec = readSpec("t_s1", "snapshot")
    assert(spec.get("type").asText() == "snapshot")
    val exp = spec.get("expected")
    // Verify protocol exact values for a basic INT table
    val proto = exp.get("protocol")
    val minReader = proto.get("minReaderVersion").asInt()
    val minWriter = proto.get("minWriterVersion").asInt()
    assert(minReader == 1, s"minReaderVersion should be 1, got $minReader")
    assert(minWriter == 2, s"minWriterVersion should be 2, got $minWriter")
    // Verify metadata schema is exactly one INT column named "id"
    val meta = exp.get("metadata")
    val schemaStr = meta.get("schemaString").asText()
    val schema = JsonUtil.mapper.readTree(schemaStr)
    assert(schema.get("type").asText() == "struct")
    val fields = schema.get("fields")
    assert(fields.size() == 1, s"Should have exactly 1 field, got ${fields.size()}")
    assert(fields.get(0).get("name").asText() == "id")
    assert(fields.get(0).get("type").asText() == "integer")
    assert(fields.get(0).get("nullable").asBoolean() == true)
    // Metadata ID is a UUID
    val metaId = meta.get("id").asText()
    assert(metaId.matches("[0-9a-f-]{36}"), s"metadata id should be UUID, got: $metaId")
  }

  test("snapshot: at specific version") {
    val results = run() { s =>
      s.test("t_s2", "snapshot at version") { w =>
        w.sql("CREATE TABLE tbl (id INT) USING delta")
        w.sql("INSERT INTO tbl VALUES (1)")
        w.sql("INSERT INTO tbl VALUES (2)")
        val t = w.table("tbl")
        w.snapshot(t, version = 1)
        w.snapshot(t, version = 2)
      }
    }
    assertPassed(results)
    val s1 = readSpec("t_s2", "snapshot_v1")
    val s2 = readSpec("t_s2", "snapshot_v2")
    assert(s1.get("version").asInt() == 1)
    assert(s2.get("version").asInt() == 2)
  }

  test("snapshot: snapshotHistory captures all versions") {
    val results = run() { s =>
      s.test("t_s3", "snapshot history") { w =>
        w.sql("CREATE TABLE tbl (id INT) USING delta")
        w.sql("INSERT INTO tbl VALUES (1)")
        w.sql("INSERT INTO tbl VALUES (2)")
        val t = w.table("tbl")
        w.snapshotHistory(t)
      }
    }
    assertPassed(results)
    // Should have snapshots for v0, v1, v2
    for (v <- 0 to 2) {
      assert(Files.exists(specs("t_s3").resolve(s"t_s3_snapshot_v$v.json")),
        s"Missing snapshot for version $v")
    }
  }

  // =========================================================================
  // CDF specs
  // =========================================================================

  test("cdf: captures change data feed rows") {
    val results = run() { s =>
      s.test("t_c1", "basic cdf") { w =>
        w.sql("""CREATE TABLE tbl (id INT, val STRING) USING delta
          TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')""")
        w.sql("INSERT INTO tbl VALUES (1, 'a'), (2, 'b')")
        w.sql("UPDATE tbl SET val = 'updated' WHERE id = 1")
        val t = w.table("tbl")
        w.cdf(t, startVersion = 1)
      }
    }
    assertPassed(results)
    val spec = readSpec("t_c1", "cdf_v1")
    assert(spec.get("type").asText() == "cdf")
    val rowCount = spec.get("expected").get("rowCount").asInt()
    assert(rowCount >= 2, s"CDF should capture at least 2 change rows (insert + update), got $rowCount")
  }

  test("cdf: version range") {
    val results = run() { s =>
      s.test("t_c2", "cdf version range") { w =>
        w.sql("""CREATE TABLE tbl (id INT) USING delta
          TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')""")
        w.sql("INSERT INTO tbl VALUES (1)")
        w.sql("INSERT INTO tbl VALUES (2)")
        w.sql("INSERT INTO tbl VALUES (3)")
        val t = w.table("tbl")
        w.cdf(t, startVersion = 1, endVersion = 2)
      }
    }
    assertPassed(results)
    val spec = readSpec("t_c2", "cdf_v1_to_v2")
    assert(spec.get("type").asText() == "cdf")
  }

  // =========================================================================
  // Domain metadata specs
  // =========================================================================

  test("domain metadata: injected and validated") {
    val results = run() { s =>
      s.test("t_dm1", "domain metadata") { w =>
        w.sql("""CREATE TABLE tbl (id INT) USING delta
          TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
        w.sql("INSERT INTO tbl VALUES (1)")
        w.sql("DELETE FROM tbl")
        val t = w.table("tbl")
        w.mutateTable(t) { tableDir =>
          val f = tableDir.resolve("_delta_log/00000000000000000002.json")
          val c = new String(Files.readAllBytes(f), "UTF-8")
          Files.write(f, (c.trim + "\n" +
            """{"domainMetadata":{"domain":"d1","configuration":"cfg1","removed":false}}""" +
            "\n").getBytes("UTF-8"))
          TableCopier.invalidateChecksumFilesForModifiedCommit(f)
        }
        w.domainMetadata(t, domain = "d1", configuration = "cfg1", name = "dm")
      }
    }
    assertPassed(results)
    val spec = readSpec("t_dm1", "dm")
    assert(spec.get("type").asText() == "domain_metadata")
    assert(spec.get("expected").get("domain").asText() == "d1")
    assert(spec.get("expected").get("configuration").asText() == "cfg1")
    assert(spec.get("expected").get("removed").asBoolean() == false)
  }

  test("domain metadata: removed domain not found") {
    val results = run() { s =>
      s.test("t_dm2", "domain removed") { w =>
        w.sql("""CREATE TABLE tbl (id INT) USING delta
          TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
        w.sql("INSERT INTO tbl VALUES (1)")
        w.sql("DELETE FROM tbl")
        w.sql("INSERT INTO tbl VALUES (2)")
        w.sql("DELETE FROM tbl")
        val t = w.table("tbl")
        w.mutateTable(t) { tableDir =>
          val f2 = tableDir.resolve("_delta_log/00000000000000000002.json")
          val c2 = new String(Files.readAllBytes(f2), "UTF-8")
          Files.write(f2, (c2.trim + "\n" +
            """{"domainMetadata":{"domain":"d1","configuration":"","removed":false}}""" +
            "\n").getBytes("UTF-8"))
          TableCopier.invalidateChecksumFilesForModifiedCommit(f2)
          val f4 = tableDir.resolve("_delta_log/00000000000000000004.json")
          val c4 = new String(Files.readAllBytes(f4), "UTF-8")
          Files.write(f4, (c4.trim + "\n" +
            """{"domainMetadata":{"domain":"d1","configuration":"","removed":true}}""" +
            "\n").getBytes("UTF-8"))
          TableCopier.invalidateChecksumFilesForModifiedCommit(f4)
        }
        w.domainMetadata(t, domain = "d1", configuration = "", removed = true, name = "dm")
      }
    }
    assertPassed(results)
  }

  // =========================================================================
  // Txn specs
  // =========================================================================

  test("txn: injected and validated") {
    val results = run() { s =>
      s.test("t_tx1", "basic txn") { w =>
        w.sql("CREATE TABLE tbl (id INT) USING delta")
        w.sql("INSERT INTO tbl VALUES (1)")
        val t = w.table("tbl")
        w.mutateTable(t) { tableDir =>
          val f = tableDir.resolve("_delta_log/00000000000000000001.json")
          val c = new String(Files.readAllBytes(f), "UTF-8")
          Files.write(f, (c.trim + "\n" +
            """{"txn":{"appId":"myapp","version":42,"lastUpdated":1000}}""" +
            "\n").getBytes("UTF-8"))
          TableCopier.invalidateChecksumFilesForModifiedCommit(f)
        }
        w.txn(t, appId = "myapp", txnVersion = 42, name = "tx")
      }
    }
    assertPassed(results)
    val spec = readSpec("t_tx1", "tx")
    assert(spec.get("type").asText() == "txn")
    assert(spec.get("expected").get("appId").asText() == "myapp")
    assert(spec.get("expected").get("txnVersion").asLong() == 42)
  }

  // =========================================================================
  // Validation catches wrong output
  // =========================================================================

  test("validation: tampered expected data detected") {
    run() { s =>
      s.test("t_v1", "tamper target") { w =>
        w.sql("CREATE TABLE tbl (id INT) USING delta")
        w.sql("INSERT INTO tbl VALUES (1), (2), (3)")
        val t = w.table("tbl")
        w.read(t)
      }
    }
    val dataDir = expected("t_v1").resolve("t_v1_read/expected_data")
    org.apache.commons.io.FileUtils.deleteDirectory(dataDir.toFile)
    spark.sql("SELECT 100 AS id UNION ALL SELECT 200")
      .write.parquet(dataDir.toString)

    DeltaLog.clearCache()
    val actual = JsonUtil.toRowMultiset(
      spark.read.format("delta").load(delta("t_v1").toString))
    val tampered = JsonUtil.toRowMultiset(spark.read.parquet(dataDir.toString))
    val ex = intercept[RuntimeException] {
      JsonUtil.assertMultisetsEqual(tampered, actual, "t_v1_read")
    }
    assert(ex.getMessage.contains("mismatch"))
  }

  test("validation: tampered snapshot protocol detected") {
    run() { s =>
      s.test("t_v2", "tamper snapshot") { w =>
        w.sql("CREATE TABLE tbl (id INT) USING delta")
        w.sql("INSERT INTO tbl VALUES (1)")
        val t = w.table("tbl")
        w.snapshot(t)
      }
    }
    val specFile = specs("t_v2").resolve("t_v2_snapshot.json")
    val content = new String(Files.readAllBytes(specFile), "UTF-8")
    // Replace minReaderVersion with 99 regardless of original value
    val tampered = content.replaceAll(
      """"minReaderVersion"\s*:\s*\d+""", """"minReaderVersion":99""")
    assert(tampered != content, "Tamper should have changed something")
    Files.write(specFile, tampered.getBytes("UTF-8"))

    DeltaLog.clearCache()
    val dl = DeltaLog.forTable(spark, delta("t_v2").toString)
    val actualProto = JsonUtil.mapper.readTree(dl.update().protocol.json).get("protocol")
    val tamperedProto = JsonUtil.mapper.readTree(
      Files.readAllBytes(specFile)).get("expected").get("protocol")
    assert(!actualProto.equals(tamperedProto),
      "Tampered protocol (minReaderVersion=99) should not match actual")
  }

  test("validation: domain metadata config mismatch caught") {
    val results = run() { s =>
      s.test("t_v3", "dm mismatch") { w =>
        w.sql("""CREATE TABLE tbl (id INT) USING delta
          TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
        w.sql("INSERT INTO tbl VALUES (1)")
        w.sql("DELETE FROM tbl")
        val t = w.table("tbl")
        w.mutateTable(t) { tableDir =>
          val f = tableDir.resolve("_delta_log/00000000000000000002.json")
          val c = new String(Files.readAllBytes(f), "UTF-8")
          Files.write(f, (c.trim + "\n" +
            """{"domainMetadata":{"domain":"d","configuration":"real","removed":false}}""" +
            "\n").getBytes("UTF-8"))
          TableCopier.invalidateChecksumFilesForModifiedCommit(f)
        }
        w.domainMetadata(t, domain = "d", configuration = "WRONG", name = "dm")
      }
    }
    assert(!results.head.passed)
    assert(results.head.errors.exists(_.contains("configuration")))
  }

  test("validation: txn version mismatch caught") {
    val results = run() { s =>
      s.test("t_v4", "txn mismatch") { w =>
        w.sql("CREATE TABLE tbl (id INT) USING delta")
        w.sql("INSERT INTO tbl VALUES (1)")
        val t = w.table("tbl")
        w.mutateTable(t) { tableDir =>
          val f = tableDir.resolve("_delta_log/00000000000000000001.json")
          val c = new String(Files.readAllBytes(f), "UTF-8")
          Files.write(f, (c.trim + "\n" +
            """{"txn":{"appId":"a","version":10,"lastUpdated":1}}""" +
            "\n").getBytes("UTF-8"))
          TableCopier.invalidateChecksumFilesForModifiedCommit(f)
        }
        w.txn(t, appId = "a", txnVersion = 999, name = "tx")
      }
    }
    assert(!results.head.passed)
    assert(results.head.errors.exists(_.contains("version")))
  }

  test("validation: extra rows in expected data detected") {
    run() { s =>
      s.test("t_v5", "extra rows") { w =>
        w.sql("CREATE TABLE tbl (id INT) USING delta")
        w.sql("INSERT INTO tbl VALUES (1)")
        val t = w.table("tbl")
        w.read(t)
      }
    }
    val dataDir = expected("t_v5").resolve("t_v5_read/expected_data")
    spark.sql("SELECT 50 AS id UNION ALL SELECT 51")
      .write.mode("append").parquet(dataDir.toString)

    DeltaLog.clearCache()
    val actual = JsonUtil.toRowMultiset(
      spark.read.format("delta").load(delta("t_v5").toString))
    val tampered = JsonUtil.toRowMultiset(spark.read.parquet(dataDir.toString))
    intercept[Exception] {
      JsonUtil.assertMultisetsEqual(tampered, actual, "t_v5_read")
    }
  }

  test("validation: assertMultisetsEqual reports count mismatches") {
    val ex = intercept[RuntimeException] {
      JsonUtil.assertMultisetsEqual(
        Map("r" -> 2, "s" -> 1), Map("r" -> 3, "s" -> 1), "spec")
    }
    assert(ex.getMessage.contains("Count mismatches"))
    assert(ex.getMessage.contains("expected 2x, got 3x"))
  }

  test("validation: assertMultisetsEqual reports missing rows") {
    val ex = intercept[RuntimeException] {
      JsonUtil.assertMultisetsEqual(
        Map("a" -> 1, "b" -> 1), Map("a" -> 1), "spec")
    }
    assert(ex.getMessage.contains("Missing rows"))
  }

  test("validation: assertMultisetsEqual reports extra rows") {
    val ex = intercept[RuntimeException] {
      JsonUtil.assertMultisetsEqual(
        Map("a" -> 1), Map("a" -> 1, "c" -> 1), "spec")
    }
    assert(ex.getMessage.contains("Extra rows"))
  }

  // =========================================================================
  // Table copy and mutations
  // =========================================================================

  test("table copy: delta log and data files present") {
    run() { s =>
      s.test("t_cp1", "table copy") { w =>
        w.sql("CREATE TABLE tbl (id INT) USING delta")
        w.sql("INSERT INTO tbl VALUES (1),(2),(3)")
        val t = w.table("tbl")
        w.read(t)
      }
    }
    val logDir = delta("t_cp1").resolve("_delta_log")
    assert(Files.exists(logDir))
    val jsonFiles = Files.list(logDir).iterator().asScala
      .filter(_.toString.endsWith(".json")).toSeq
    assert(jsonFiles.nonEmpty, "Should have commit JSON files")
    val parquetFiles = Files.list(delta("t_cp1")).iterator().asScala
      .filter(_.toString.endsWith(".parquet")).toSeq
    assert(parquetFiles.nonEmpty, "Should have data parquet files")
  }

  test("mutateTable: modifies copied table, not source") {
    var sourcePath: Path = null
    run() { s =>
      s.test("t_mt1", "mutate copy") { w =>
        w.sql("CREATE TABLE tbl (id INT) USING delta")
        w.sql("INSERT INTO tbl VALUES (1),(2)")
        // Capture source path before cleanup
        val loc = w.spark.sql("DESCRIBE DETAIL tbl").collect()(0).getAs[String]("location")
        sourcePath = if (loc.startsWith("file:")) {
          java.nio.file.Paths.get(new java.net.URI(loc))
        } else java.nio.file.Paths.get(loc)
        val t = w.table("tbl")
        w.mutateTable(t) { tableDir =>
          Files.write(tableDir.resolve("MARKER"), "test".getBytes("UTF-8"))
        }
        w.read(t)
      }
    }
    assert(Files.exists(delta("t_mt1").resolve("MARKER")),
      "MARKER should exist in copied table")
    assert(!Files.exists(sourcePath.resolve("MARKER")),
      "MARKER should NOT exist in source table")
  }

  test("modifyCommitActions: modifies add, preserves commitInfo") {
    val results = run() { s =>
      s.test("t_mc1", "modify actions") { w =>
        w.sql("CREATE TABLE tbl (id INT) USING delta")
        w.sql("INSERT INTO tbl VALUES (1),(2)")
        val t = w.table("tbl")
        w.modifyCommitActions(t, version = 1) { case ("add", node) =>
          node.put("stats", """{"numRecords":999}"""); true
          case _ => true
        }
        w.snapshot(t)
      }
    }
    assertPassed(results)
    val content = new String(Files.readAllBytes(
      delta("t_mc1").resolve("_delta_log/00000000000000000001.json")), "UTF-8")
    assert(content.contains("commitInfo"), "commitInfo preserved")
    assert(content.contains("999"), "stats modified")
  }

  test("modifyCommitActions: can drop actions") {
    val results = run() { s =>
      s.test("t_mc2", "drop actions") { w =>
        w.sql("CREATE TABLE tbl (id INT) USING delta")
        w.sql("INSERT INTO tbl VALUES (1),(2)")
        val t = w.table("tbl")
        // Drop all add actions from the INSERT commit
        w.modifyCommitActions(t, version = 1) { case ("add", _) =>
          false // drop
          case _ => true
        }
        w.snapshot(t)
      }
    }
    assertPassed(results)
    val content = new String(Files.readAllBytes(
      delta("t_mc2").resolve("_delta_log/00000000000000000001.json")), "UTF-8")
    assert(!content.contains("\"add\""), "add actions should be dropped")
    assert(content.contains("commitInfo"), "commitInfo preserved")
  }

  // =========================================================================
  // Multi-table workloads
  // =========================================================================

  test("multi-table: two tables from one test") {
    val results = run() { s =>
      s.test("t_mt", "multi table") { w =>
        w.sql("CREATE TABLE src (id INT) USING delta")
        w.sql("INSERT INTO src VALUES (1),(2)")
        w.sql("CREATE TABLE dst (id INT) USING delta")
        w.sql("INSERT INTO dst VALUES (3),(4),(5)")
        val s1 = w.table("src")
        val d1 = w.table("dst")
        w.read(s1)
        w.read(d1)
      }
    }
    assert(results.size == 2, "Should produce 2 workload results")
    assertPassed(results)
    assert(Files.exists(dir("t_mt_src")))
    assert(Files.exists(dir("t_mt_dst")))
  }

  // =========================================================================
  // Auto-naming
  // =========================================================================

  test("auto-naming: predicate operators") {
    val results = run() { s =>
      s.test("t_an1", "auto names") { w =>
        w.sql("CREATE TABLE tbl (id INT) USING delta")
        w.sql("INSERT INTO tbl VALUES (1),(2),(3)")
        val t = w.table("tbl")
        w.read(t, predicate = "id >= 2")
        w.read(t, predicate = "id < 3")
        w.read(t, predicate = "id = 1")
        w.read(t, predicate = "id IS NULL")
      }
    }
    assertPassed(results)
    assert(Files.exists(specs("t_an1").resolve("t_an1_read_id_gte_2.json")))
    assert(Files.exists(specs("t_an1").resolve("t_an1_read_id_lt_3.json")))
    assert(Files.exists(specs("t_an1").resolve("t_an1_read_id_eq_1.json")))
    assert(Files.exists(specs("t_an1").resolve("t_an1_read_id_is_null.json")))
  }

  test("auto-naming: cdf version range") {
    val results = run() { s =>
      s.test("t_an2", "cdf names") { w =>
        w.sql("""CREATE TABLE tbl (id INT) USING delta
          TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')""")
        w.sql("INSERT INTO tbl VALUES (1)")
        w.sql("INSERT INTO tbl VALUES (2)")
        w.sql("INSERT INTO tbl VALUES (3)")
        val t = w.table("tbl")
        w.cdf(t, startVersion = 1, endVersion = 2)
        w.cdf(t, startVersion = 2)
      }
    }
    assertPassed(results)
    assert(Files.exists(specs("t_an2").resolve("t_an2_cdf_v1_to_v2.json")))
    assert(Files.exists(specs("t_an2").resolve("t_an2_cdf_v2.json")))
  }

  // =========================================================================
  // Framework behavior
  // =========================================================================

  test("framework: failed test cleans up output directory") {
    val results = run() { s =>
      s.test("t_fw1", "will fail") { w =>
        throw new RuntimeException("boom")
      }
    }
    assert(!results.head.passed)
    assert(!Files.exists(dir("t_fw1")))
  }

  test("framework: passing test skipped on re-run") {
    run() { s =>
      s.test("t_fw2", "first run") { w =>
        w.sql("CREATE TABLE tbl (id INT) USING delta")
        w.sql("INSERT INTO tbl VALUES (1)")
        val t = w.table("tbl")
        w.read(t)
        w.snapshot(t)
      }
    }
    assert(Files.exists(dir("t_fw2").resolve("table_info.json")))
    val second = run(force = false) { s =>
      s.test("t_fw2", "second run") { w =>
        w.sql("CREATE TABLE tbl (id INT) USING delta")
        w.sql("INSERT INTO tbl VALUES (1)")
        val t = w.table("tbl")
        w.read(t)
        w.snapshot(t)
      }
    }
    assert(second.head.skipped)
  }

  test("framework: zero-table test warns") {
    val results = run() { s =>
      s.test("t_fw3", "no tables") { w =>
        w.sql("CREATE TABLE tbl (id INT) USING delta")
        // Never call w.table() — should warn
      }
    }
    assert(results.size == 1)
    assert(results.head.passed) // passes but with warning
  }

  test("framework: table_info.json written") {
    run() { s =>
      s.test("t_fw4", "table info") { w =>
        w.sql("CREATE TABLE tbl (id INT) USING delta")
        w.sql("INSERT INTO tbl VALUES (1)")
        val t = w.table("tbl")
        w.read(t)
      }
    }
    val tableInfo = dir("t_fw4").resolve("table_info.json")
    assert(Files.exists(tableInfo))
    val info = JsonUtil.mapper.readTree(Files.readAllBytes(tableInfo))
    assert(info.get("name").asText() == "t_fw4")
    // Verify schema has exactly one INT column "id"
    val schema = info.get("schema")
    assert(schema.get("type").asText() == "struct")
    assert(schema.get("fields").size() == 1)
    assert(schema.get("fields").get(0).get("name").asText() == "id")
    assert(schema.get("fields").get(0).get("type").asText() == "integer")
    // Verify protocol
    val proto = info.get("protocol")
    assert(proto.get("minReaderVersion").asInt() == 1)
    assert(proto.get("minWriterVersion").asInt() == 2)
  }
}
