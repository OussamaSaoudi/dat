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

class GeneratedColumnsSuite extends WorkloadTestSuite("generated_columns") {

  test("gc_basic") {
    sql("""CREATE TABLE tbl (
      id LONG,
      doubled LONG GENERATED ALWAYS AS (id * 2)
    ) USING delta TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl (id) VALUES (1),(2),(3)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "doubled = 4")
    snapshotSpec(t)
  }

  test("gc_arithmetic_expr") {
    sql("""CREATE TABLE tbl (
      price DOUBLE,
      quantity INT,
      total DOUBLE GENERATED ALWAYS AS (price * quantity)
    ) USING delta TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl (price, quantity) VALUES (10.5, 3),(25.0, 4),(5.99, 10)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "total > 50.0")
    snapshotSpec(t)
  }

  test("gc_case_when") {
    sql("""CREATE TABLE tbl (
      value INT,
      category STRING GENERATED ALWAYS AS (CASE WHEN value >= 100 THEN 'high' ELSE 'low' END)
    ) USING delta TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl (value) VALUES (50),(100),(150),(30),(200)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "category = 'high'")
    readSpec(t, predicate = "category = 'low'")
    snapshotSpec(t)
  }

  test("gc_coalesce_null") {
    sql("""CREATE TABLE tbl (
      nickname STRING,
      first_name STRING,
      display_name STRING GENERATED ALWAYS AS (COALESCE(nickname, first_name, 'Unknown'))
    ) USING delta TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl (nickname, first_name) VALUES ('Al', 'Alice'),('Bo', 'Bob'),(null, 'Charlie'),(null, null)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "display_name = 'Al'")
    readSpec(t, predicate = "display_name = 'Unknown'")
    snapshotSpec(t)
  }

  test("gc_concat_expr") {
    sql("""CREATE TABLE tbl (
      first_name STRING,
      last_name STRING,
      full_name STRING GENERATED ALWAYS AS (CONCAT(first_name, ' ', last_name))
    ) USING delta TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl (first_name, last_name) VALUES ('Alice', 'Johnson'),('Bob', 'Smith')")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "full_name = 'Alice Johnson'")
    snapshotSpec(t)
  }

  test("gc_date_format_expr") {
    sql("""CREATE TABLE tbl (
      event_date DATE,
      formatted_date STRING GENERATED ALWAYS AS (DATE_FORMAT(event_date, 'yyyy-MM'))
    ) USING delta TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl (event_date) VALUES ('2024-01-15'),('2024-02-20'),('2024-01-30')")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "formatted_date = '2024-01'")
    snapshotSpec(t)
  }

  test("gc_datetime") {
    sql("""CREATE TABLE tbl (
      event_time TIMESTAMP,
      event_date DATE GENERATED ALWAYS AS (CAST(event_time AS DATE)),
      event_hour INT GENERATED ALWAYS AS (HOUR(event_time))
    ) USING delta TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("""INSERT INTO tbl (event_time) VALUES
      (TIMESTAMP'2024-01-15 09:30:00'),
      (TIMESTAMP'2024-01-15 14:45:00'),
      (TIMESTAMP'2024-02-01 22:00:00')""")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "event_date = '2024-01-15'")
    readSpec(t, predicate = "event_hour < 12")
    snapshotSpec(t)
  }

  test("gc_math") {
    sql("""CREATE TABLE tbl (
      x DOUBLE,
      y DOUBLE,
      distance DOUBLE GENERATED ALWAYS AS (SQRT(x * x + y * y))
    ) USING delta TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl (x, y) VALUES (3.0, 4.0),(0.0, 0.0),(1.0, 1.0)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "distance = 5.0")
    snapshotSpec(t)
  }

  test("gc_multiple") {
    sql("""CREATE TABLE tbl (
      id LONG,
      doubled LONG GENERATED ALWAYS AS (id * 2),
      tripled LONG GENERATED ALWAYS AS (id * 3),
      squared LONG GENERATED ALWAYS AS (id * id)
    ) USING delta TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl (id) VALUES (1),(2),(3),(4),(5)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "squared > 10")
    snapshotSpec(t)
  }

  test("gc_nested") {
    sql("""CREATE TABLE tbl (
      data STRUCT<x: INT, y: INT>,
      sum_xy INT GENERATED ALWAYS AS (data.x + data.y)
    ) USING delta TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("""INSERT INTO tbl (data) VALUES
      (named_struct('x', 3, 'y', 7)),
      (named_struct('x', 5, 'y', 5)),
      (named_struct('x', 1, 'y', 2))""")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "sum_xy = 10")
    snapshotSpec(t)
  }

  test("gc_null_expression_result") {
    sql("""CREATE TABLE tbl (
      value STRING,
      parsed_int INT GENERATED ALWAYS AS (CAST(value AS INT))
    ) USING delta TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl (value) VALUES ('42')")
    sql("INSERT INTO tbl (value) VALUES ('not_a_number')")
    sql("INSERT INTO tbl (value) VALUES ('99')")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "parsed_int IS NOT NULL")
    readSpec(t, predicate = "parsed_int IS NULL")
    snapshotSpec(t)
  }

  test("gc_partition_col") {
    sql("""CREATE TABLE tbl (
      date_col DATE,
      value INT,
      year INT GENERATED ALWAYS AS (YEAR(date_col))
    ) USING delta PARTITIONED BY (year)
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("""INSERT INTO tbl (date_col, value) VALUES
      ('2023-06-15', 100),('2024-01-20', 200),('2023-12-31', 300),('2024-07-04', 400)""")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "year = 2023")
    readSpec(t, predicate = "year = 2024")
    snapshotSpec(t)
  }

  test("gc_partitioned") {
    sql("""CREATE TABLE tbl (
      id LONG,
      value STRING,
      date_part DATE GENERATED ALWAYS AS (CAST('2024-01-01' AS DATE))
    ) USING delta PARTITIONED BY (date_part)
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl (id, value) VALUES (1, 'a'),(2, 'b'),(3, 'c')")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "date_part = '2024-01-01'")
    snapshotSpec(t)
  }

  test("gc_reference") {
    sql("""CREATE TABLE tbl (
      first_name STRING,
      last_name STRING,
      full_name STRING GENERATED ALWAYS AS (CONCAT(first_name, ' ', last_name))
    ) USING delta TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl (first_name, last_name) VALUES ('John', 'Doe'),('Jane', 'Smith')")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "full_name = 'John Doe'")
    snapshotSpec(t)
  }

  test("gc_string") {
    sql("""CREATE TABLE tbl (
      email STRING,
      domain STRING GENERATED ALWAYS AS (SUBSTRING_INDEX(email, '@', -1))
    ) USING delta TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl (email) VALUES ('alice@example.com'),('bob@example.com'),('charlie@other.org')")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "domain = 'example.com'")
    snapshotSpec(t)
  }

  test("gc_ctas") {
    sql("""CREATE TABLE tbl (
      id LONG,
      doubled LONG GENERATED ALWAYS AS (id * 2)
    ) USING delta TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl (id) VALUES (1),(2),(3),(4),(5)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, predicate = "doubled >= 10")
    snapshotSpec(t)
  }

  test("gc_time_travel") {
    sql("""CREATE TABLE tbl (
      id LONG,
      doubled LONG GENERATED ALWAYS AS (id * 2)
    ) USING delta TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl (id) VALUES (1)")
    sql("INSERT INTO tbl (id) VALUES (2)")
    sql("INSERT INTO tbl (id) VALUES (3)")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, version = 0)
    readSpec(t, version = 1)
    readSpec(t, version = 2)
    snapshotSpec(t)
  }

  test("gc_added_via_alter_table") {
    sql("""CREATE TABLE tbl (
      id INT,
      doubled INT GENERATED ALWAYS AS (id * 2)
    ) USING delta TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl (id) VALUES (1),(2)")
    sql("ALTER TABLE tbl ADD COLUMN (extra STRING)")
    sql("INSERT INTO tbl (id, extra) VALUES (3, 'hello')")
    val t = registerTable("tbl")
    readSpec(t)
    readSpec(t, version = 1)
    readSpec(t, predicate = "extra IS NOT NULL")
    snapshotSpec(t)
    snapshotSpec(t, version = 0)
    snapshotSpec(t, version = 1)
    snapshotSpec(t, version = 2)
    snapshotSpec(t, version = 3)
  }

}
