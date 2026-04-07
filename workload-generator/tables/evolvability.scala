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
      "evolvability") { w =>
    w.sql("CREATE TABLE tbl (id LONG) USING delta")
    w.sql("INSERT INTO tbl SELECT id FROM range(10)")
    val t = w.table("tbl")
    // Inject unknown action type alongside valid data
    w.mutateTable(t) { dir =>
      val f = dir.resolve("_delta_log/00000000000000000000.json")
      val content = new String(java.nio.file.Files.readAllBytes(f), "UTF-8")
      java.nio.file.Files.write(f,
        (content.trim + "\n" + """{"unknownAction":{"key":"val"}}""" + "\n").getBytes)
    }
    w.read(t)
    w.snapshot(t)
  }

  test("ev_unknown_action_type", "Commit log with unknown action type", "evolvability") { w =>
    w.sql("CREATE TABLE tbl (id LONG) USING delta")
    w.sql("INSERT INTO tbl SELECT id FROM range(10)")
    val t = w.table("tbl")
    w.mutateTable(t) { dir =>
      val f = dir.resolve("_delta_log/00000000000000000000.json")
      val content = new String(java.nio.file.Files.readAllBytes(f), "UTF-8")
      java.nio.file.Files.write(f,
        (content.trim + "\n" + """{"unknownAction":{"key":"value"}}""" + "\n").getBytes)
    }
    w.read(t)
    w.snapshot(t)
  }

  // Protocol evolvability

  test("ev_protocol", "Protocol evolvability", "evolvability") { w =>
    w.sql("CREATE TABLE tbl (id LONG) USING delta")
    w.sql("INSERT INTO tbl SELECT id FROM range(5)")
    val t = w.table("tbl")
    w.snapshot(t)
  }

  test("ev_extra_protocol_fields", "Protocol with extra unknown fields (forward compat)",
      "evolvability") { w =>
    w.sql("CREATE TABLE tbl (id LONG) USING delta")
    w.sql("INSERT INTO tbl SELECT id FROM range(10)")
    val t = w.table("tbl")
    // Add unknown fields to protocol action
    w.mutateTable(t) { dir =>
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
    w.read(t)
    w.snapshot(t)
  }

  // Unknown protocol features

  test("ev_unknown_protocol_feature", "Protocol with unrecognized writer feature",
      "evolvability") { w =>
    w.sql("CREATE TABLE tbl (id LONG) USING delta")
    w.sql("INSERT INTO tbl SELECT id FROM range(5)")
    val t = w.table("tbl")
    // Add unknown writer feature — should NOT block read
    w.mutateTable(t) { dir =>
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
    w.snapshot(t)
  }

  test("ev_unknown_writer_feature", "Unknown writer feature does not block read",
      "evolvability") { w =>
    w.sql("CREATE TABLE tbl (id LONG) USING delta")
    w.sql("INSERT INTO tbl SELECT id FROM range(10)")
    val t = w.table("tbl")
    w.mutateTable(t) { dir =>
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
    w.read(t)
    w.snapshot(t)
  }

  test("ev_unknown_reader_feature", "Unknown reader feature blocks read",
      "evolvability", "error") { w =>
    w.sql("CREATE TABLE tbl (id LONG) USING delta")
    w.sql("INSERT INTO tbl SELECT id FROM range(10)")
    val t = w.table("tbl")
    // Add unknown reader feature — should block read
    w.mutateTable(t) { dir =>
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
    w.read(t)
    w.snapshot(t)
  }

  // Schema evolution

  test("ev_schema_evolution", "Schema evolution across versions",
      "evolvability", "schemaEvolution") { w =>
    w.sql("CREATE TABLE tbl (id LONG) USING delta")
    w.sql("INSERT INTO tbl SELECT id FROM range(5)")
    w.sql("ALTER TABLE tbl ADD COLUMN name STRING")
    w.sql("INSERT INTO tbl VALUES (5, 'alice'), (6, 'bob')")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "name IS NOT NULL")
    w.snapshot(t)
  }

  test("ev_data_types", "Multiple data types evolvability", "evolvability") { w =>
    w.sql("""CREATE TABLE tbl (
      id LONG, name STRING, score DOUBLE, active BOOLEAN, created DATE
    ) USING delta""")
    w.sql("INSERT INTO tbl VALUES (1, 'alice', 95.5, true, DATE'2024-01-01')")
    w.sql("INSERT INTO tbl VALUES (2, 'bob', 82.3, false, DATE'2024-06-15')")
    val t = w.table("tbl")
    w.read(t)
    w.snapshot(t)
  }

  // Partitioned + null partition values

  test("ev_partitioned", "Partitioned table evolvability", "evolvability") { w =>
    w.sql("CREATE TABLE tbl (id LONG, part STRING) USING delta PARTITIONED BY (part)")
    w.sql("INSERT INTO tbl VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "part = 'a'")
    w.snapshot(t)
  }

  test("ev_partition_null", "Serialized partition values with null values",
      "evolvability") { w =>
    w.sql("CREATE TABLE tbl (id LONG, part STRING) USING delta PARTITIONED BY (part)")
    w.sql("INSERT INTO tbl VALUES (1, 'a'), (2, NULL), (3, 'b')")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "part IS NULL")
    w.snapshot(t)
  }

  // CommitInfo with future fields

  test("ev_future_commit_info", "CommitInfo with unknown future fields",
      "evolvability") { w =>
    w.sql("CREATE TABLE tbl (id LONG) USING delta")
    w.sql("INSERT INTO tbl SELECT id FROM range(10)")
    val t = w.table("tbl")
    w.mutateTable(t) { dir =>
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
    w.read(t)
    w.snapshot(t)
  }

  // Missing intermediate version

  test("ev_missing_intermediate_version", "Log with version gap",
      "evolvability") { w =>
    w.sql("CREATE TABLE tbl (id LONG) USING delta")
    // Create 5 versions (0-4) with 2 files each
    w.sql("INSERT INTO tbl SELECT id FROM range(0, 10)")
    w.sql("INSERT INTO tbl SELECT id FROM range(10, 20)")
    w.sql("INSERT INTO tbl SELECT id FROM range(20, 30)")
    w.sql("INSERT INTO tbl SELECT id FROM range(30, 40)")
    // Force checkpoint at version 4
    val loc = w.spark.sql("DESCRIBE DETAIL tbl").collect()(0).getAs[String]("location")
    org.apache.spark.sql.delta.DeltaLog.forTable(w.spark, loc).checkpoint()
    w.sql("INSERT INTO tbl SELECT id FROM range(40, 50)")
    val t = w.table("tbl")
    // Delete intermediate versions (checkpoint covers them)
    w.mutateTable(t) { dir =>
      java.nio.file.Files.deleteIfExists(dir.resolve("_delta_log/00000000000000000001.json"))
      java.nio.file.Files.deleteIfExists(dir.resolve("_delta_log/00000000000000000002.json"))
      java.nio.file.Files.deleteIfExists(dir.resolve("_delta_log/00000000000000000003.json"))
    }
    w.read(t)
    w.read(t, predicate = "id >= 40")
    w.snapshot(t)
  }

  // Format Compatibility: unknown fields in add

  test("fc_unknown_field_in_add", "Forward compat - unknown field in add action",
      "formatCompat") { w =>
    w.sql("CREATE TABLE tbl (id LONG) USING delta")
    w.sql("INSERT INTO tbl SELECT id FROM range(10)")
    val t = w.table("tbl")
    w.mutateTable(t) { dir =>
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
    w.read(t)
    w.snapshot(t)
  }

  // Format Compatibility: unknown field in metadata

  test("fc_unknown_field_in_metadata", "Forward compat - unknown field in metadata",
      "formatCompat") { w =>
    w.sql("CREATE TABLE tbl (id LONG) USING delta")
    w.sql("INSERT INTO tbl SELECT id FROM range(10)")
    val t = w.table("tbl")
    w.mutateTable(t) { dir =>
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
    w.read(t)
    w.snapshot(t)
  }

  // Format Compatibility: unknown field in protocol

  test("fc_unknown_field_in_protocol", "Forward compat - unknown field in protocol",
      "formatCompat") { w =>
    w.sql("CREATE TABLE tbl (id LONG) USING delta")
    w.sql("INSERT INTO tbl SELECT id FROM range(10)")
    val t = w.table("tbl")
    w.mutateTable(t) { dir =>
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
    w.read(t)
    w.snapshot(t)
  }

  // Format Compatibility: unknown action at top level

  test("fc_unknown_action_top_level", "Forward compat - unknown action at top level",
      "formatCompat") { w =>
    w.sql("CREATE TABLE tbl (id LONG) USING delta")
    w.sql("INSERT INTO tbl SELECT id FROM range(10)")
    val t = w.table("tbl")
    w.mutateTable(t) { dir =>
      val f = dir.resolve("_delta_log/00000000000000000000.json")
      val content = new String(java.nio.file.Files.readAllBytes(f), "UTF-8")
      java.nio.file.Files.write(f,
        (content.trim + "\n" + """{"futureAction":{"data":"test","version":99}}""" + "\n").getBytes)
    }
    w.read(t)
    w.snapshot(t)
  }

  // Format Compatibility: null fields in add action

  test("fc_null_fields_in_add", "Forward compat - null fields in add action",
      "formatCompat") { w =>
    w.sql("CREATE TABLE tbl (id LONG) USING delta")
    w.sql("INSERT INTO tbl SELECT id FROM range(10)")
    val t = w.table("tbl")
    w.mutateTable(t) { dir =>
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
    w.read(t)
    w.snapshot(t)
  }

  // Format Compatibility: empty JSON line in commit file

  test("fc_empty_json_line", "Forward compat - empty JSON line in commit file",
      "formatCompat") { w =>
    w.sql("CREATE TABLE tbl (id LONG) USING delta")
    w.sql("INSERT INTO tbl SELECT id FROM range(10)")
    val t = w.table("tbl")
    w.mutateTable(t) { dir =>
      val f = dir.resolve("_delta_log/00000000000000000000.json")
      val content = new String(java.nio.file.Files.readAllBytes(f), "UTF-8")
      // Insert blank lines between actions
      val withBlanks = content.split("\n").flatMap(line => Seq(line, "")).mkString("\n")
      java.nio.file.Files.write(f, withBlanks.getBytes)
    }
    w.read(t)
    w.snapshot(t)
  }

  // Format Compatibility: extra metadata configuration keys

  test("fc_extra_metadata_keys", "Forward compat - extra metadata configuration keys",
      "formatCompat") { w =>
    w.sql("CREATE TABLE tbl (id LONG) USING delta")
    w.sql("INSERT INTO tbl SELECT id FROM range(10)")
    val t = w.table("tbl")
    w.mutateTable(t) { dir =>
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
    w.read(t)
    w.snapshot(t)
  }

}.runAll()
