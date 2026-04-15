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

package io.delta.workload.tables.unsupported

import io.delta.workload.WorkloadTestSuite

/**
 * Interval types are NOT supported by Delta Lake.
 * These tests document expected failures - Delta rejects interval columns at write time.
 * Spark supports interval types, but Delta's type system excludes them for cross-engine compatibility.
 *
 * Run separately: sbt "testOnly *IntervalTypesSuite"
 */
class IntervalTypesSuite extends WorkloadTestSuite("unsupported/interval") {

  test("intv_001_interval_ym_basic") {
    sql("""CREATE TABLE tbl (id INT, period INTERVAL YEAR TO MONTH) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, INTERVAL '1-6' YEAR TO MONTH)")
    sql("INSERT INTO tbl VALUES (2, INTERVAL '2-3' YEAR TO MONTH),(3, INTERVAL '0-9' YEAR TO MONTH)")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("intv_002_interval_dt_basic") {
    sql("""CREATE TABLE tbl (id INT, duration INTERVAL DAY TO SECOND) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, INTERVAL '1 02:30:00' DAY TO SECOND)")
    sql("INSERT INTO tbl VALUES (2, INTERVAL '3 06:45:30' DAY TO SECOND),(3, INTERVAL '0 00:15:00' DAY TO SECOND)")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("intv_003_interval_partitioned") {
    sql("""CREATE TABLE tbl (id INT, period INTERVAL YEAR TO MONTH) USING delta
      PARTITIONED BY (period) TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, INTERVAL '1-0' YEAR TO MONTH)")
    sql("INSERT INTO tbl VALUES (2, INTERVAL '2-0' YEAR TO MONTH)")
    sql("INSERT INTO tbl VALUES (3, INTERVAL '1-0' YEAR TO MONTH)")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("intv_004_interval_negative") {
    sql("""CREATE TABLE tbl (id INT, period INTERVAL YEAR TO MONTH) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1, INTERVAL '-1-6' YEAR TO MONTH)")
    sql("INSERT INTO tbl VALUES (2, INTERVAL '-0-3' YEAR TO MONTH)")
    sql("INSERT INTO tbl VALUES (3, INTERVAL '0-0' YEAR TO MONTH)")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("intv_005_interval_mixed") {
    sql("""CREATE TABLE tbl (
      id INT,
      period INTERVAL YEAR TO MONTH,
      duration INTERVAL DAY TO SECOND
    ) USING delta TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("""INSERT INTO tbl VALUES
      (1, INTERVAL '1-0' YEAR TO MONTH, INTERVAL '1 00:00:00' DAY TO SECOND),
      (2, INTERVAL '0-6' YEAR TO MONTH, INTERVAL '0 12:30:00' DAY TO SECOND)""")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("intv_boundary_values") {
    sql("""CREATE TABLE tbl (
      id INT,
      period INTERVAL YEAR TO MONTH,
      duration INTERVAL DAY TO SECOND
    ) USING delta TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("""INSERT INTO tbl VALUES
      (1, INTERVAL '178956970-7' YEAR TO MONTH, INTERVAL '106751991 04:00:54.775807' DAY TO SECOND),
      (2, INTERVAL '-178956970-8' YEAR TO MONTH, INTERVAL '-106751991 04:00:54.775808' DAY TO SECOND)""")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

  test("intv_sub_second") {
    sql("""CREATE TABLE tbl (id INT, duration INTERVAL DAY TO SECOND) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("""INSERT INTO tbl VALUES
      (1, INTERVAL '0 00:00:00.001' DAY TO SECOND),
      (2, INTERVAL '0 00:00:00.999999' DAY TO SECOND),
      (3, INTERVAL '0 00:00:01.5' DAY TO SECOND)""")
    val t = registerTable("tbl")
    readSpec(t)
    snapshotSpec(t)
  }

}
