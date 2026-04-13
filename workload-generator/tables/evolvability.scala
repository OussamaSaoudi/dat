/**
 * Evolvability + Format Compatibility workloads (ev_* + fc_* family).
 * Covers: unknown action types in commit, unknown fields in add/metadata/protocol,
 * unknown protocol features, schema evolution across versions, partition null values,
 * empty JSON lines, extra metadata keys, forward compatibility.
 *
 */

new WorkloadSuite("evolvability") {

  // Evolvability: basic reads with unknown actions

  test("ev_batch_read", "Transaction log schema evolvability - batch read",
      "evolvability") {
    sql("CREATE TABLE tbl (id LONG) USING delta")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    val t = registerTable("tbl")
    // Inject unknown action type alongside valid data
    mutateTable(t) { dir =>
      val f = dir.resolve("_delta_log/00000000000000000000.json")
      val content = new String(java.nio.file.Files.readAllBytes(f), "UTF-8")
      java.nio.file.Files.write(f,
        (content.trim + "\n" + """{"unknownAction":{"key":"val"}}""" + "\n").getBytes)
    }
    read(t)
    snapshot(t)
  }

  test("ev_unknown_action_type", "Commit log with unknown action type", "evolvability") {
    sql("CREATE TABLE tbl (id LONG) USING delta")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
      val f = dir.resolve("_delta_log/00000000000000000000.json")
      val content = new String(java.nio.file.Files.readAllBytes(f), "UTF-8")
      java.nio.file.Files.write(f,
        (content.trim + "\n" + """{"unknownAction":{"key":"value"}}""" + "\n").getBytes)
    }
    read(t)
    snapshot(t)
  }

  // Protocol evolvability

  test("ev_protocol", "Protocol evolvability", "evolvability") {
    sql("CREATE TABLE tbl (id LONG) USING delta")
    sql("INSERT INTO tbl SELECT id FROM range(5)")
    val t = registerTable("tbl")
    snapshot(t)
  }

  test("ev_extra_protocol_fields", "Protocol with extra unknown fields (forward compat)",
      "evolvability") {
    sql("CREATE TABLE tbl (id LONG) USING delta")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    val t = registerTable("tbl")
    // Add unknown fields to protocol action
    mutateTable(t) { dir =>
      import scala.collection.JavaConverters._
      val f = dir.resolve("_delta_log/00000000000000000000.json")
      val lines = java.nio.file.Files.readAllLines(f).asScala
      val newLines = lines.map { line =>
        if (line.contains("\"protocol\"")) {
          line.replace("\"protocol\":{",
            "\"protocol\":{\"futureField\":\"futureValue\",\"anotherUnknown\":42,")
        } else line
      }
      java.nio.file.Files.write(f, newLines.asJava)
    }
    read(t)
    snapshot(t)
  }

  // Unknown protocol features

  test("ev_unknown_protocol_feature", "Protocol with unrecognized writer feature",
      "evolvability") {
    sql("CREATE TABLE tbl (id LONG) USING delta")
    sql("INSERT INTO tbl SELECT id FROM range(5)")
    val t = registerTable("tbl")
    // Add unknown writer feature — should NOT block read
    mutateTable(t) { dir =>
      import scala.collection.JavaConverters._
      val f = dir.resolve("_delta_log/00000000000000000000.json")
      val lines = java.nio.file.Files.readAllLines(f).asScala
      val newLines = lines.map { line =>
        if (line.contains("\"writerFeatures\"")) {
          line.replace("\"writerFeatures\":[",
            "\"writerFeatures\":[\"unknownFutureWriterFeature\",")
        } else line
      }
      java.nio.file.Files.write(f, newLines.asJava)
    }
    snapshot(t)
  }

  test("ev_unknown_writer_feature", "Unknown writer feature does not block read",
      "evolvability") {
    sql("CREATE TABLE tbl (id LONG) USING delta")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
      import scala.collection.JavaConverters._
      val f = dir.resolve("_delta_log/00000000000000000000.json")
      val lines = java.nio.file.Files.readAllLines(f).asScala
      val newLines = lines.map { line =>
        if (line.contains("\"writerFeatures\"")) {
          line.replace("\"writerFeatures\":[",
            "\"writerFeatures\":[\"unknownFutureWriterFeature\",")
        } else line
      }
      java.nio.file.Files.write(f, newLines.asJava)
    }
    read(t)
    snapshot(t)
  }

  test("ev_unknown_reader_feature", "Unknown reader feature blocks read",
      "evolvability", "error") {
    sql("CREATE TABLE tbl (id LONG) USING delta")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    val t = registerTable("tbl")
    // Add unknown reader feature — should block read
    mutateTable(t) { dir =>
      import scala.collection.JavaConverters._
      val f = dir.resolve("_delta_log/00000000000000000000.json")
      val lines = java.nio.file.Files.readAllLines(f).asScala
      val newLines = lines.map { line =>
        if (line.contains("\"readerFeatures\"")) {
          line.replace("\"readerFeatures\":[",
            "\"readerFeatures\":[\"unknownFutureReaderFeature\",")
        } else line
      }
      java.nio.file.Files.write(f, newLines.asJava)
    }
    read(t)
    snapshot(t)
  }

  // Schema evolution

  test("ev_schema_evolution", "Schema evolution across versions",
      "evolvability", "schemaEvolution") {
    sql("CREATE TABLE tbl (id LONG) USING delta")
    sql("INSERT INTO tbl SELECT id FROM range(5)")
    sql("ALTER TABLE tbl ADD COLUMN name STRING")
    sql("INSERT INTO tbl VALUES (5, 'alice'), (6, 'bob')")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "name IS NOT NULL")
    snapshot(t)
  }

  test("ev_data_types", "Multiple data types evolvability", "evolvability") {
    sql("""CREATE TABLE tbl (
      id LONG, name STRING, score DOUBLE, active BOOLEAN, created DATE
    ) USING delta""")
    sql("INSERT INTO tbl VALUES (1, 'alice', 95.5, true, DATE'2024-01-01')")
    sql("INSERT INTO tbl VALUES (2, 'bob', 82.3, false, DATE'2024-06-15')")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  // Partitioned + null partition values

  test("ev_partitioned", "Partitioned table evolvability", "evolvability") {
    sql("CREATE TABLE tbl (id LONG, part STRING) USING delta PARTITIONED BY (part)")
    sql("INSERT INTO tbl VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "part = 'a'")
    snapshot(t)
  }

  test("ev_partition_null", "Serialized partition values with null values",
      "evolvability") {
    sql("CREATE TABLE tbl (id LONG, part STRING) USING delta PARTITIONED BY (part)")
    sql("INSERT INTO tbl VALUES (1, 'a'), (2, NULL), (3, 'b')")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "part IS NULL")
    snapshot(t)
  }

  // CommitInfo with future fields

  test("ev_future_commit_info", "CommitInfo with unknown future fields",
      "evolvability") {
    sql("CREATE TABLE tbl (id LONG) USING delta")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
      import scala.collection.JavaConverters._
      val f = dir.resolve("_delta_log/00000000000000000000.json")
      val lines = java.nio.file.Files.readAllLines(f).asScala
      val newLines = lines.map { line =>
        if (line.contains("\"commitInfo\"")) {
          line.replace("\"commitInfo\":{",
            "\"commitInfo\":{\"futureCommitField\":\"someValue\",\"futureNestedField\":{\"x\":1},")
        } else line
      }
      java.nio.file.Files.write(f, newLines.asJava)
    }
    read(t)
    snapshot(t)
  }

  // Missing intermediate version

  test("ev_missing_intermediate_version", "Log with version gap",
      "evolvability") {
    sql("CREATE TABLE tbl (id LONG) USING delta")
    // Create 5 versions (0-4) with 2 files each
    sql("INSERT INTO tbl SELECT id FROM range(0, 10)")
    sql("INSERT INTO tbl SELECT id FROM range(10, 20)")
    sql("INSERT INTO tbl SELECT id FROM range(20, 30)")
    sql("INSERT INTO tbl SELECT id FROM range(30, 40)")
    // Force checkpoint at version 4
    forceCheckpoint("tbl")
    sql("INSERT INTO tbl SELECT id FROM range(40, 50)")
    val t = registerTable("tbl")
    // Delete intermediate versions (checkpoint covers them)
    mutateTable(t) { dir =>
      java.nio.file.Files.deleteIfExists(dir.resolve("_delta_log/00000000000000000001.json"))
      java.nio.file.Files.deleteIfExists(dir.resolve("_delta_log/00000000000000000002.json"))
      java.nio.file.Files.deleteIfExists(dir.resolve("_delta_log/00000000000000000003.json"))
    }
    read(t)
    read(t, predicate = "id >= 40")
    snapshot(t)
  }

  // Format Compatibility: unknown fields in add

  test("fc_unknown_field_in_add", "Forward compat - unknown field in add action",
      "formatCompat") {
    sql("CREATE TABLE tbl (id LONG) USING delta")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
      import scala.collection.JavaConverters._
      val mapper = new com.fasterxml.jackson.databind.ObjectMapper()
      val f = dir.resolve("_delta_log/00000000000000000000.json")
      val lines = java.nio.file.Files.readAllLines(f).asScala
      val newLines = lines.map { line =>
        if (line.contains("\"add\"")) {
          val node = mapper.readTree(line)
          val addNode = node.get("add").asInstanceOf[com.fasterxml.jackson.databind.node.ObjectNode]
          addNode.put("unknownField", "testValue")
          mapper.writeValueAsString(node)
        } else line
      }
      java.nio.file.Files.write(f, newLines.asJava)
    }
    read(t)
    snapshot(t)
  }

  // Format Compatibility: unknown field in metadata

  test("fc_unknown_field_in_metadata", "Forward compat - unknown field in metadata",
      "formatCompat") {
    sql("CREATE TABLE tbl (id LONG) USING delta")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
      import scala.collection.JavaConverters._
      val f = dir.resolve("_delta_log/00000000000000000000.json")
      val lines = java.nio.file.Files.readAllLines(f).asScala
      val newLines = lines.map { line =>
        if (line.contains("\"metaData\"")) {
          line.replace("\"metaData\":{",
            "\"metaData\":{\"unknownMetadataField\":\"testValue\",")
        } else line
      }
      java.nio.file.Files.write(f, newLines.asJava)
    }
    read(t)
    snapshot(t)
  }

  // Format Compatibility: unknown field in protocol

  test("fc_unknown_field_in_protocol", "Forward compat - unknown field in protocol",
      "formatCompat") {
    sql("CREATE TABLE tbl (id LONG) USING delta")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
      import scala.collection.JavaConverters._
      val f = dir.resolve("_delta_log/00000000000000000000.json")
      val lines = java.nio.file.Files.readAllLines(f).asScala
      val newLines = lines.map { line =>
        if (line.contains("\"protocol\"")) {
          line.replace("\"protocol\":{",
            "\"protocol\":{\"unknownProtocolField\":\"testValue\",")
        } else line
      }
      java.nio.file.Files.write(f, newLines.asJava)
    }
    read(t)
    snapshot(t)
  }

  // Format Compatibility: unknown action at top level

  test("fc_unknown_action_top_level", "Forward compat - unknown action at top level",
      "formatCompat") {
    sql("CREATE TABLE tbl (id LONG) USING delta")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
      val f = dir.resolve("_delta_log/00000000000000000000.json")
      val content = new String(java.nio.file.Files.readAllBytes(f), "UTF-8")
      java.nio.file.Files.write(f,
        (content.trim + "\n" + """{"futureAction":{"data":"test","version":99}}""" + "\n").getBytes)
    }
    read(t)
    snapshot(t)
  }

  // Format Compatibility: null fields in add action

  test("fc_null_fields_in_add", "Forward compat - null fields in add action",
      "formatCompat") {
    sql("CREATE TABLE tbl (id LONG) USING delta")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
      import scala.collection.JavaConverters._
      val mapper = new com.fasterxml.jackson.databind.ObjectMapper()
      val f = dir.resolve("_delta_log/00000000000000000000.json")
      val lines = java.nio.file.Files.readAllLines(f).asScala
      val newLines = lines.map { line =>
        if (line.contains("\"add\"")) {
          val node = mapper.readTree(line)
          val addNode = node.get("add").asInstanceOf[com.fasterxml.jackson.databind.node.ObjectNode]
          addNode.putNull("nullField")
          addNode.putNull("anotherNullField")
          mapper.writeValueAsString(node)
        } else line
      }
      java.nio.file.Files.write(f, newLines.asJava)
    }
    read(t)
    snapshot(t)
  }

  // Format Compatibility: empty JSON line in commit file

  test("fc_empty_json_line", "Forward compat - empty JSON line in commit file",
      "formatCompat") {
    sql("CREATE TABLE tbl (id LONG) USING delta")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
      val f = dir.resolve("_delta_log/00000000000000000000.json")
      val content = new String(java.nio.file.Files.readAllBytes(f), "UTF-8")
      // Insert blank lines between actions
      val withBlanks = content.split("\n").flatMap(line => Seq(line, "")).mkString("\n")
      java.nio.file.Files.write(f, withBlanks.getBytes)
    }
    read(t)
    snapshot(t)
  }

  // Format Compatibility: extra metadata configuration keys

  test("fc_extra_metadata_keys", "Forward compat - extra metadata configuration keys",
      "formatCompat") {
    sql("CREATE TABLE tbl (id LONG) USING delta")
    sql("INSERT INTO tbl SELECT id FROM range(10)")
    val t = registerTable("tbl")
    mutateTable(t) { dir =>
      import scala.collection.JavaConverters._
      val mapper = new com.fasterxml.jackson.databind.ObjectMapper()
      val f = dir.resolve("_delta_log/00000000000000000000.json")
      val lines = java.nio.file.Files.readAllLines(f).asScala
      val newLines = lines.map { line =>
        if (line.contains("\"metaData\"")) {
          val node = mapper.readTree(line)
          val metaNode = node.get("metaData").asInstanceOf[com.fasterxml.jackson.databind.node.ObjectNode]
          val config = metaNode.get("configuration").asInstanceOf[com.fasterxml.jackson.databind.node.ObjectNode]
          config.put("delta.unknownFutureConfig", "someValue")
          config.put("custom.vendor.setting", "42")
          mapper.writeValueAsString(node)
        } else line
      }
      java.nio.file.Files.write(f, newLines.asJava)
    }
    read(t)
    snapshot(t)
  }

}
