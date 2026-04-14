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

package io.delta.workload.tables

import io.delta.workload.WorkloadTestSuite

class TransactionsSuite extends WorkloadTestSuite("transactions") {

  // Helper to create a table and inject SetTransaction actions via mutation
  def txnTable(numInserts: Int): Unit = {
    sql("""CREATE TABLE tbl USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')
      AS SELECT 1 AS value""")
    for (_ <- 1 until numInserts) {
      sql("INSERT INTO tbl VALUES (1)")
    }
  }

  test("txn_basic") {
    txnTable(2)
    val t = registerTable("tbl")
    mutateTable(t) { tableDir =>
      val v1 = tableDir.resolve("_delta_log/00000000000000000001.json")
      val c = new String(java.nio.file.Files.readAllBytes(v1), "UTF-8")
      java.nio.file.Files.write(v1, (c.trim + "\n" +
        """{"txn":{"appId":"app-1","version":1,"lastUpdated":1}}""" + "\n").getBytes("UTF-8"))
    }
    appTxn(t, appId = "app-1", txnVersion = 1, version = 1, name = "txn")
  }

  test("txn_update") {
    txnTable(3)
    val t = registerTable("tbl")
    mutateTable(t) { tableDir =>
      val v1 = tableDir.resolve("_delta_log/00000000000000000001.json")
      val c1 = new String(java.nio.file.Files.readAllBytes(v1), "UTF-8")
      java.nio.file.Files.write(v1, (c1.trim + "\n" +
        """{"txn":{"appId":"app-1","version":1,"lastUpdated":1}}""" + "\n").getBytes("UTF-8"))

      val v2 = tableDir.resolve("_delta_log/00000000000000000002.json")
      val c2 = new String(java.nio.file.Files.readAllBytes(v2), "UTF-8")
      java.nio.file.Files.write(v2, (c2.trim + "\n" +
        """{"txn":{"appId":"app-1","version":3,"lastUpdated":2}}""" + "\n").getBytes("UTF-8"))
    }
    appTxn(t, appId = "app-1", txnVersion = 3, name = "txn")
  }

  test("txn_duplicate_appid") {
    txnTable(3)
    val t = registerTable("tbl")
    mutateTable(t) { tableDir =>
      val v1 = tableDir.resolve("_delta_log/00000000000000000001.json")
      val c1 = new String(java.nio.file.Files.readAllBytes(v1), "UTF-8")
      java.nio.file.Files.write(v1, (c1.trim + "\n" +
        """{"txn":{"appId":"app-dup","version":1,"lastUpdated":1}}""" + "\n").getBytes("UTF-8"))

      val v2 = tableDir.resolve("_delta_log/00000000000000000002.json")
      val c2 = new String(java.nio.file.Files.readAllBytes(v2), "UTF-8")
      java.nio.file.Files.write(v2, (c2.trim + "\n" +
        """{"txn":{"appId":"app-dup","version":5,"lastUpdated":2}}""" + "\n").getBytes("UTF-8"))
    }
    appTxn(t, appId = "app-dup", txnVersion = 5, name = "txn")
  }

  test("txn_multiple_apps_1") {
    txnTable(3)
    val t = registerTable("tbl")
    mutateTable(t) { tableDir =>
      val v1 = tableDir.resolve("_delta_log/00000000000000000001.json")
      val c1 = new String(java.nio.file.Files.readAllBytes(v1), "UTF-8")
      java.nio.file.Files.write(v1, (c1.trim + "\n" +
        """{"txn":{"appId":"app-1","version":1,"lastUpdated":1}}""" + "\n").getBytes("UTF-8"))

      val v2 = tableDir.resolve("_delta_log/00000000000000000002.json")
      val c2 = new String(java.nio.file.Files.readAllBytes(v2), "UTF-8")
      java.nio.file.Files.write(v2, (c2.trim + "\n" +
        """{"txn":{"appId":"app-2","version":100,"lastUpdated":2}}""" + "\n").getBytes("UTF-8"))
    }
    appTxn(t, appId = "app-1", txnVersion = 1, name = "txn")
  }

  test("txn_multiple_apps_2") {
    txnTable(3)
    val t = registerTable("tbl")
    mutateTable(t) { tableDir =>
      val v1 = tableDir.resolve("_delta_log/00000000000000000001.json")
      val c1 = new String(java.nio.file.Files.readAllBytes(v1), "UTF-8")
      java.nio.file.Files.write(v1, (c1.trim + "\n" +
        """{"txn":{"appId":"app-1","version":1,"lastUpdated":1}}""" + "\n").getBytes("UTF-8"))

      val v2 = tableDir.resolve("_delta_log/00000000000000000002.json")
      val c2 = new String(java.nio.file.Files.readAllBytes(v2), "UTF-8")
      java.nio.file.Files.write(v2, (c2.trim + "\n" +
        """{"txn":{"appId":"app-2","version":100,"lastUpdated":2}}""" + "\n").getBytes("UTF-8"))
    }
    appTxn(t, appId = "app-2", txnVersion = 100, name = "txn")
  }

  test("txn_batch_app1") {
    txnTable(2)
    val t = registerTable("tbl")
    mutateTable(t) { tableDir =>
      val v1 = tableDir.resolve("_delta_log/00000000000000000001.json")
      val c1 = new String(java.nio.file.Files.readAllBytes(v1), "UTF-8")
      java.nio.file.Files.write(v1, (c1.trim + "\n" +
        """{"txn":{"appId":"app-1","version":100,"lastUpdated":1}}""" + "\n" +
        """{"txn":{"appId":"app-3","version":300,"lastUpdated":1}}""" + "\n").getBytes("UTF-8"))
    }
    appTxn(t, appId = "app-1", txnVersion = 100, name = "txn")
  }

  test("txn_batch_app3") {
    txnTable(2)
    val t = registerTable("tbl")
    mutateTable(t) { tableDir =>
      val v1 = tableDir.resolve("_delta_log/00000000000000000001.json")
      val c1 = new String(java.nio.file.Files.readAllBytes(v1), "UTF-8")
      java.nio.file.Files.write(v1, (c1.trim + "\n" +
        """{"txn":{"appId":"app-1","version":100,"lastUpdated":1}}""" + "\n" +
        """{"txn":{"appId":"app-3","version":300,"lastUpdated":1}}""" + "\n").getBytes("UTF-8"))
    }
    appTxn(t, appId = "app-3", txnVersion = 300, name = "txn")
  }

  test("txn_with_last_updated") {
    txnTable(2)
    val t = registerTable("tbl")
    mutateTable(t) { tableDir =>
      val v1 = tableDir.resolve("_delta_log/00000000000000000001.json")
      val c1 = new String(java.nio.file.Files.readAllBytes(v1), "UTF-8")
      java.nio.file.Files.write(v1, (c1.trim + "\n" +
        """{"txn":{"appId":"app-with-ts","version":42,"lastUpdated":1709251200000}}""" + "\n").getBytes("UTF-8"))
    }
    appTxn(t, appId = "app-with-ts", txnVersion = 42, name = "txn")
  }

  test("txn_without_last_updated") {
    txnTable(2)
    val t = registerTable("tbl")
    mutateTable(t) { tableDir =>
      val v1 = tableDir.resolve("_delta_log/00000000000000000001.json")
      val c1 = new String(java.nio.file.Files.readAllBytes(v1), "UTF-8")
      java.nio.file.Files.write(v1, (c1.trim + "\n" +
        """{"txn":{"appId":"app-no-ts","version":7}}""" + "\n").getBytes("UTF-8"))
    }
    appTxn(t, appId = "app-no-ts", txnVersion = 7, name = "txn")
  }

  test("txn_after_checkpoint") {
    txnTable(3)
    val t = registerTable("tbl")
    mutateTable(t) { tableDir =>
      val v1 = tableDir.resolve("_delta_log/00000000000000000001.json")
      val c1 = new String(java.nio.file.Files.readAllBytes(v1), "UTF-8")
      java.nio.file.Files.write(v1, (c1.trim + "\n" +
        """{"txn":{"appId":"app-checkpoint","version":10,"lastUpdated":100}}""" + "\n").getBytes("UTF-8"))

      val v2 = tableDir.resolve("_delta_log/00000000000000000002.json")
      val c2 = new String(java.nio.file.Files.readAllBytes(v2), "UTF-8")
      java.nio.file.Files.write(v2, (c2.trim + "\n" +
        """{"txn":{"appId":"app-checkpoint","version":20,"lastUpdated":200}}""" + "\n").getBytes("UTF-8"))
    }
    appTxn(t, appId = "app-checkpoint", txnVersion = 20, name = "txn")
  }

  test("txn_at_version_1") {
    txnTable(4)
    val t = registerTable("tbl")
    mutateTable(t) { tableDir =>
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
    appTxn(t, appId = "app-1", txnVersion = 1, version = 1, name = "txn")
  }

  test("txn_future_version") {
    txnTable(2)
    val t = registerTable("tbl")
    mutateTable(t) { tableDir =>
      val v1 = tableDir.resolve("_delta_log/00000000000000000001.json")
      val c1 = new String(java.nio.file.Files.readAllBytes(v1), "UTF-8")
      java.nio.file.Files.write(v1, (c1.trim + "\n" +
        """{"txn":{"appId":"app-future","version":999999,"lastUpdated":1}}""" + "\n").getBytes("UTF-8"))
    }
    appTxn(t, appId = "app-future", txnVersion = 999999, name = "txn")
  }

}
