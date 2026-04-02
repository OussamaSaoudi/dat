new WorkloadSuite("transactions") {

  // Helper to create a table and inject SetTransaction actions via mutation
  def txnTable(w: WorkloadContext, numInserts: Int): Unit = {
    w.sql("""CREATE TABLE tbl USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')
      AS SELECT 1 AS value""")
    for (_ <- 1 until numInserts) {
      w.sql("INSERT INTO tbl VALUES (1)")
    }
  }

  // -- txn_basic: basic SetTransaction --
  test("txn_basic", "Basic SetTransaction tracking", "txn") { w =>
    txnTable(w, 2)
    val t = w.table("tbl")
    w.mutateTable(t) { tableDir =>
      val v1 = tableDir.resolve("_delta_log/00000000000000000001.json")
      val c = new String(java.nio.file.Files.readAllBytes(v1), "UTF-8")
      java.nio.file.Files.write(v1, (c.trim + "\n" +
        """{"txn":{"appId":"app-1","version":1,"lastUpdated":1}}""" + "\n").getBytes("UTF-8"))
    }
    w.txn(t, appId = "app-1", txnVersion = 1, version = 1, name = "txn")
  }

  // -- txn_update: SetTransaction version update --
  test("txn_update", "SetTransaction version update", "txn") { w =>
    txnTable(w, 3)
    val t = w.table("tbl")
    w.mutateTable(t) { tableDir =>
      val v1 = tableDir.resolve("_delta_log/00000000000000000001.json")
      val c1 = new String(java.nio.file.Files.readAllBytes(v1), "UTF-8")
      java.nio.file.Files.write(v1, (c1.trim + "\n" +
        """{"txn":{"appId":"app-1","version":1,"lastUpdated":1}}""" + "\n").getBytes("UTF-8"))

      val v2 = tableDir.resolve("_delta_log/00000000000000000002.json")
      val c2 = new String(java.nio.file.Files.readAllBytes(v2), "UTF-8")
      java.nio.file.Files.write(v2, (c2.trim + "\n" +
        """{"txn":{"appId":"app-1","version":3,"lastUpdated":2}}""" + "\n").getBytes("UTF-8"))
    }
    w.txn(t, appId = "app-1", txnVersion = 3, name = "txn")
  }

  // -- txn_duplicate_appid: same appId in separate commits --
  test("txn_duplicate_appid", "Two SetTransaction for same appId in separate commits", "txn") { w =>
    txnTable(w, 3)
    val t = w.table("tbl")
    w.mutateTable(t) { tableDir =>
      val v1 = tableDir.resolve("_delta_log/00000000000000000001.json")
      val c1 = new String(java.nio.file.Files.readAllBytes(v1), "UTF-8")
      java.nio.file.Files.write(v1, (c1.trim + "\n" +
        """{"txn":{"appId":"app-dup","version":1,"lastUpdated":1}}""" + "\n").getBytes("UTF-8"))

      val v2 = tableDir.resolve("_delta_log/00000000000000000002.json")
      val c2 = new String(java.nio.file.Files.readAllBytes(v2), "UTF-8")
      java.nio.file.Files.write(v2, (c2.trim + "\n" +
        """{"txn":{"appId":"app-dup","version":5,"lastUpdated":2}}""" + "\n").getBytes("UTF-8"))
    }
    w.txn(t, appId = "app-dup", txnVersion = 5, name = "txn")
  }

  // -- txn_multiple_apps_1: multiple apps - query app-1 --
  test("txn_multiple_apps_1", "Multiple app transactions - app-1", "txn") { w =>
    txnTable(w, 3)
    val t = w.table("tbl")
    w.mutateTable(t) { tableDir =>
      val v1 = tableDir.resolve("_delta_log/00000000000000000001.json")
      val c1 = new String(java.nio.file.Files.readAllBytes(v1), "UTF-8")
      java.nio.file.Files.write(v1, (c1.trim + "\n" +
        """{"txn":{"appId":"app-1","version":1,"lastUpdated":1}}""" + "\n").getBytes("UTF-8"))

      val v2 = tableDir.resolve("_delta_log/00000000000000000002.json")
      val c2 = new String(java.nio.file.Files.readAllBytes(v2), "UTF-8")
      java.nio.file.Files.write(v2, (c2.trim + "\n" +
        """{"txn":{"appId":"app-2","version":100,"lastUpdated":2}}""" + "\n").getBytes("UTF-8"))
    }
    w.txn(t, appId = "app-1", txnVersion = 1, name = "txn")
  }

  // -- txn_multiple_apps_2: multiple apps - query app-2 --
  test("txn_multiple_apps_2", "Multiple app transactions - app-2", "txn") { w =>
    txnTable(w, 3)
    val t = w.table("tbl")
    w.mutateTable(t) { tableDir =>
      val v1 = tableDir.resolve("_delta_log/00000000000000000001.json")
      val c1 = new String(java.nio.file.Files.readAllBytes(v1), "UTF-8")
      java.nio.file.Files.write(v1, (c1.trim + "\n" +
        """{"txn":{"appId":"app-1","version":1,"lastUpdated":1}}""" + "\n").getBytes("UTF-8"))

      val v2 = tableDir.resolve("_delta_log/00000000000000000002.json")
      val c2 = new String(java.nio.file.Files.readAllBytes(v2), "UTF-8")
      java.nio.file.Files.write(v2, (c2.trim + "\n" +
        """{"txn":{"appId":"app-2","version":100,"lastUpdated":2}}""" + "\n").getBytes("UTF-8"))
    }
    w.txn(t, appId = "app-2", txnVersion = 100, name = "txn")
  }

  // -- txn_batch_app1: multiple txns in single commit - query app-1 --
  test("txn_batch_app1", "Multiple SetTransactions in single commit - app-1", "txn") { w =>
    txnTable(w, 2)
    val t = w.table("tbl")
    w.mutateTable(t) { tableDir =>
      val v1 = tableDir.resolve("_delta_log/00000000000000000001.json")
      val c1 = new String(java.nio.file.Files.readAllBytes(v1), "UTF-8")
      java.nio.file.Files.write(v1, (c1.trim + "\n" +
        """{"txn":{"appId":"app-1","version":100,"lastUpdated":1}}""" + "\n" +
        """{"txn":{"appId":"app-3","version":300,"lastUpdated":1}}""" + "\n").getBytes("UTF-8"))
    }
    w.txn(t, appId = "app-1", txnVersion = 100, name = "txn")
  }

  // -- txn_batch_app3: multiple txns in single commit - query app-3 --
  test("txn_batch_app3", "Multiple SetTransactions in single commit - app-3", "txn") { w =>
    txnTable(w, 2)
    val t = w.table("tbl")
    w.mutateTable(t) { tableDir =>
      val v1 = tableDir.resolve("_delta_log/00000000000000000001.json")
      val c1 = new String(java.nio.file.Files.readAllBytes(v1), "UTF-8")
      java.nio.file.Files.write(v1, (c1.trim + "\n" +
        """{"txn":{"appId":"app-1","version":100,"lastUpdated":1}}""" + "\n" +
        """{"txn":{"appId":"app-3","version":300,"lastUpdated":1}}""" + "\n").getBytes("UTF-8"))
    }
    w.txn(t, appId = "app-3", txnVersion = 300, name = "txn")
  }

  // -- txn_with_last_updated: SetTransaction with lastUpdated --
  test("txn_with_last_updated", "SetTransaction with lastUpdated", "txn") { w =>
    txnTable(w, 2)
    val t = w.table("tbl")
    w.mutateTable(t) { tableDir =>
      val v1 = tableDir.resolve("_delta_log/00000000000000000001.json")
      val c1 = new String(java.nio.file.Files.readAllBytes(v1), "UTF-8")
      java.nio.file.Files.write(v1, (c1.trim + "\n" +
        """{"txn":{"appId":"app-with-ts","version":42,"lastUpdated":1709251200000}}""" + "\n").getBytes("UTF-8"))
    }
    w.txn(t, appId = "app-with-ts", txnVersion = 42, name = "txn")
  }

  // -- txn_without_last_updated: SetTransaction without lastUpdated --
  test("txn_without_last_updated", "SetTransaction without lastUpdated", "txn") { w =>
    txnTable(w, 2)
    val t = w.table("tbl")
    w.mutateTable(t) { tableDir =>
      val v1 = tableDir.resolve("_delta_log/00000000000000000001.json")
      val c1 = new String(java.nio.file.Files.readAllBytes(v1), "UTF-8")
      java.nio.file.Files.write(v1, (c1.trim + "\n" +
        """{"txn":{"appId":"app-no-ts","version":7}}""" + "\n").getBytes("UTF-8"))
    }
    w.txn(t, appId = "app-no-ts", txnVersion = 7, name = "txn")
  }

  // -- txn_after_checkpoint: SetTransaction survives checkpoint --
  test("txn_after_checkpoint", "SetTransaction survives checkpoint", "txn") { w =>
    txnTable(w, 3)
    val t = w.table("tbl")
    w.mutateTable(t) { tableDir =>
      val v1 = tableDir.resolve("_delta_log/00000000000000000001.json")
      val c1 = new String(java.nio.file.Files.readAllBytes(v1), "UTF-8")
      java.nio.file.Files.write(v1, (c1.trim + "\n" +
        """{"txn":{"appId":"app-checkpoint","version":10,"lastUpdated":100}}""" + "\n").getBytes("UTF-8"))

      val v2 = tableDir.resolve("_delta_log/00000000000000000002.json")
      val c2 = new String(java.nio.file.Files.readAllBytes(v2), "UTF-8")
      java.nio.file.Files.write(v2, (c2.trim + "\n" +
        """{"txn":{"appId":"app-checkpoint","version":20,"lastUpdated":200}}""" + "\n").getBytes("UTF-8"))
    }
    w.txn(t, appId = "app-checkpoint", txnVersion = 20, name = "txn")
  }

  // -- txn_at_version_1: SetTransaction at specific version --
  test("txn_at_version_1", "SetTransaction at version 1", "txn") { w =>
    txnTable(w, 4)
    val t = w.table("tbl")
    w.mutateTable(t) { tableDir =>
      val v1 = tableDir.resolve("_delta_log/00000000000000000001.json")
      val c1 = new String(java.nio.file.Files.readAllBytes(v1), "UTF-8")
      java.nio.file.Files.write(v1, (c1.trim + "\n" +
        """{"txn":{"appId":"app-1","version":1,"lastUpdated":1}}""" + "\n").getBytes("UTF-8"))

      val v2 = tableDir.resolve("_delta_log/00000000000000000002.json")
      val c2 = new String(java.nio.file.Files.readAllBytes(v2), "UTF-8")
      java.nio.file.Files.write(v2, (c2.trim + "\n" +
        """{"txn":{"appId":"app-2","version":50,"lastUpdated":2}}""" + "\n").getBytes("UTF-8"))

      val v3 = tableDir.resolve("_delta_log/00000000000000000003.json")
      val c3 = new String(java.nio.file.Files.readAllBytes(v3), "UTF-8")
      java.nio.file.Files.write(v3, (c3.trim + "\n" +
        """{"txn":{"appId":"app-1","version":10,"lastUpdated":3}}""" + "\n").getBytes("UTF-8"))
    }
    w.txn(t, appId = "app-1", txnVersion = 1, version = 1, name = "txn")
  }

  // -- txn_future_version: SetTransaction version greater than commit version --
  test("txn_future_version", "SetTransaction version greater than commit version", "txn") { w =>
    txnTable(w, 2)
    val t = w.table("tbl")
    w.mutateTable(t) { tableDir =>
      val v1 = tableDir.resolve("_delta_log/00000000000000000001.json")
      val c1 = new String(java.nio.file.Files.readAllBytes(v1), "UTF-8")
      java.nio.file.Files.write(v1, (c1.trim + "\n" +
        """{"txn":{"appId":"app-future","version":999999,"lastUpdated":1}}""" + "\n").getBytes("UTF-8"))
    }
    w.txn(t, appId = "app-future", txnVersion = 999999, name = "txn")
  }

}.runAll()
