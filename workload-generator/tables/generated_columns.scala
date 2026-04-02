new WorkloadSuite("generated_columns") {

  // -- gc_basic: basic generated column --
  test("gc_basic", "Basic generated column (id * 2)", "generatedColumns") { w =>
    w.sql("""CREATE TABLE tbl (
      id LONG,
      doubled LONG GENERATED ALWAYS AS (id * 2)
    ) USING delta TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl (id) VALUES (1),(2),(3)")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "doubled = 4")
    w.snapshot(t)
  }

  // -- gc_arithmetic_expr: price * quantity --
  test("gc_arithmetic_expr", "Generated column with arithmetic expression", "generatedColumns") { w =>
    w.sql("""CREATE TABLE tbl (
      price DOUBLE,
      quantity INT,
      total DOUBLE GENERATED ALWAYS AS (price * quantity)
    ) USING delta TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl (price, quantity) VALUES (10.5, 3),(25.0, 4),(5.99, 10)")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "total > 50.0")
    w.snapshot(t)
  }

  // -- gc_case_when: CASE WHEN expression --
  test("gc_case_when", "Generated column with CASE WHEN", "generatedColumns") { w =>
    w.sql("""CREATE TABLE tbl (
      value INT,
      category STRING GENERATED ALWAYS AS (CASE WHEN value >= 100 THEN 'high' ELSE 'low' END)
    ) USING delta TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl (value) VALUES (50),(100),(150),(30),(200)")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "category = 'high'")
    w.read(t, predicate = "category = 'low'")
    w.snapshot(t)
  }

  // -- gc_coalesce_null: COALESCE expression --
  test("gc_coalesce_null", "Generated column with COALESCE", "generatedColumns") { w =>
    w.sql("""CREATE TABLE tbl (
      nickname STRING,
      first_name STRING,
      display_name STRING GENERATED ALWAYS AS (COALESCE(nickname, first_name, 'Unknown'))
    ) USING delta TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl (nickname, first_name) VALUES ('Al', 'Alice'),('Bo', 'Bob'),(null, 'Charlie'),(null, null)")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "display_name = 'Al'")
    w.read(t, predicate = "display_name = 'Unknown'")
    w.snapshot(t)
  }

  // -- gc_concat_expr: CONCAT expression --
  test("gc_concat_expr", "Generated column with CONCAT", "generatedColumns") { w =>
    w.sql("""CREATE TABLE tbl (
      first_name STRING,
      last_name STRING,
      full_name STRING GENERATED ALWAYS AS (CONCAT(first_name, ' ', last_name))
    ) USING delta TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl (first_name, last_name) VALUES ('Alice', 'Johnson'),('Bob', 'Smith')")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "full_name = 'Alice Johnson'")
    w.snapshot(t)
  }

  // -- gc_date_format_expr: DATE_FORMAT expression --
  test("gc_date_format_expr", "Generated column with DATE_FORMAT", "generatedColumns") { w =>
    w.sql("""CREATE TABLE tbl (
      event_date DATE,
      formatted_date STRING GENERATED ALWAYS AS (DATE_FORMAT(event_date, 'yyyy-MM'))
    ) USING delta TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl (event_date) VALUES ('2024-01-15'),('2024-02-20'),('2024-01-30')")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "formatted_date = '2024-01'")
    w.snapshot(t)
  }

  // -- gc_datetime: CAST timestamp to date/hour --
  test("gc_datetime", "Generated columns from timestamp", "generatedColumns") { w =>
    w.sql("""CREATE TABLE tbl (
      event_time TIMESTAMP,
      event_date DATE GENERATED ALWAYS AS (CAST(event_time AS DATE)),
      event_hour INT GENERATED ALWAYS AS (HOUR(event_time))
    ) USING delta TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("""INSERT INTO tbl (event_time) VALUES
      (TIMESTAMP'2024-01-15 09:30:00'),
      (TIMESTAMP'2024-01-15 14:45:00'),
      (TIMESTAMP'2024-02-01 22:00:00')""")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "event_date = '2024-01-15'")
    w.read(t, predicate = "event_hour < 12")
    w.snapshot(t)
  }

  // -- gc_math: SQRT(x^2 + y^2) --
  test("gc_math", "Generated column with SQRT expression", "generatedColumns") { w =>
    w.sql("""CREATE TABLE tbl (
      x DOUBLE,
      y DOUBLE,
      distance DOUBLE GENERATED ALWAYS AS (SQRT(x * x + y * y))
    ) USING delta TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl (x, y) VALUES (3.0, 4.0),(0.0, 0.0),(1.0, 1.0)")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "distance = 5.0")
    w.snapshot(t)
  }

  // -- gc_multiple: multiple generated columns --
  test("gc_multiple", "Multiple generated columns", "generatedColumns") { w =>
    w.sql("""CREATE TABLE tbl (
      id LONG,
      doubled LONG GENERATED ALWAYS AS (id * 2),
      tripled LONG GENERATED ALWAYS AS (id * 3),
      squared LONG GENERATED ALWAYS AS (id * id)
    ) USING delta TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl (id) VALUES (1),(2),(3),(4),(5)")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "squared > 10")
    w.snapshot(t)
  }

  // -- gc_nested: generated from nested struct field --
  test("gc_nested", "Generated column from struct field", "generatedColumns") { w =>
    w.sql("""CREATE TABLE tbl (
      data STRUCT<x: INT, y: INT>,
      sum_xy INT GENERATED ALWAYS AS (data.x + data.y)
    ) USING delta TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("""INSERT INTO tbl (data) VALUES
      (named_struct('x', 3, 'y', 7)),
      (named_struct('x', 5, 'y', 5)),
      (named_struct('x', 1, 'y', 2))""")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "sum_xy = 10")
    w.snapshot(t)
  }

  // -- gc_null_expression_result: CAST that can return NULL --
  test("gc_null_expression_result", "Generated column that can be NULL", "generatedColumns") { w =>
    w.sql("""CREATE TABLE tbl (
      value STRING,
      parsed_int INT GENERATED ALWAYS AS (CAST(value AS INT))
    ) USING delta TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl (value) VALUES ('42')")
    w.sql("INSERT INTO tbl (value) VALUES ('not_a_number')")
    w.sql("INSERT INTO tbl (value) VALUES ('99')")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "parsed_int IS NOT NULL")
    w.read(t, predicate = "parsed_int IS NULL")
    w.snapshot(t)
  }

  // -- gc_partition_col: generated column used as partition --
  test("gc_partition_col", "Generated column as partition column", "generatedColumns") { w =>
    w.sql("""CREATE TABLE tbl (
      date_col DATE,
      value INT,
      year INT GENERATED ALWAYS AS (YEAR(date_col))
    ) USING delta PARTITIONED BY (year)
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("""INSERT INTO tbl (date_col, value) VALUES
      ('2023-06-15', 100),('2024-01-20', 200),('2023-12-31', 300),('2024-07-04', 400)""")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "year = 2023")
    w.read(t, predicate = "year = 2024")
    w.snapshot(t)
  }

  // -- gc_partitioned: partitioned table with generated column --
  test("gc_partitioned", "Partitioned table with generated column", "generatedColumns") { w =>
    w.sql("""CREATE TABLE tbl (
      id LONG,
      value STRING,
      date_part DATE GENERATED ALWAYS AS (CAST('2024-01-01' AS DATE))
    ) USING delta PARTITIONED BY (date_part)
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl (id, value) VALUES (1, 'a'),(2, 'b'),(3, 'c')")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "date_part = '2024-01-01'")
    w.snapshot(t)
  }

  // -- gc_reference: generated column referencing other columns --
  test("gc_reference", "Generated column referencing other columns", "generatedColumns") { w =>
    w.sql("""CREATE TABLE tbl (
      first_name STRING,
      last_name STRING,
      full_name STRING GENERATED ALWAYS AS (CONCAT(first_name, ' ', last_name))
    ) USING delta TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl (first_name, last_name) VALUES ('John', 'Doe'),('Jane', 'Smith')")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "full_name = 'John Doe'")
    w.snapshot(t)
  }

  // -- gc_string: SUBSTRING_INDEX for domain extraction --
  test("gc_string", "Generated column with string function", "generatedColumns") { w =>
    w.sql("""CREATE TABLE tbl (
      email STRING,
      domain STRING GENERATED ALWAYS AS (SUBSTRING_INDEX(email, '@', -1))
    ) USING delta TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl (email) VALUES ('alice@example.com'),('bob@example.com'),('charlie@other.org')")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "domain = 'example.com'")
    w.snapshot(t)
  }

  // -- gc_ctas: generated column via CTAS (actually just create + insert) --
  test("gc_ctas", "Generated column with CTAS pattern", "generatedColumns") { w =>
    w.sql("""CREATE TABLE tbl (
      id LONG,
      doubled LONG GENERATED ALWAYS AS (id * 2)
    ) USING delta TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl (id) VALUES (1),(2),(3),(4),(5)")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "doubled >= 10")
    w.snapshot(t)
  }

  // -- gc_time_travel: read generated columns at different versions --
  test("gc_time_travel", "Generated columns with time travel", "generatedColumns") { w =>
    w.sql("""CREATE TABLE tbl (
      id LONG,
      doubled LONG GENERATED ALWAYS AS (id * 2)
    ) USING delta TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl (id) VALUES (1)")
    w.sql("INSERT INTO tbl (id) VALUES (2)")
    w.sql("INSERT INTO tbl (id) VALUES (3)")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, version = 0)
    w.read(t, version = 1)
    w.read(t, version = 2)
    w.snapshot(t)
  }

  // -- gc_added_via_alter_table: column added via ALTER TABLE --
  test("gc_added_via_alter_table", "Column added via ALTER TABLE alongside generated column", "generatedColumns") { w =>
    w.sql("""CREATE TABLE tbl (
      id INT,
      doubled INT GENERATED ALWAYS AS (id * 2)
    ) USING delta TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl (id) VALUES (1),(2)")
    w.sql("ALTER TABLE tbl ADD COLUMN (extra STRING)")
    w.sql("INSERT INTO tbl (id, extra) VALUES (3, 'hello')")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, version = 1)
    w.read(t, predicate = "extra IS NOT NULL")
    w.snapshot(t)
    w.snapshot(t, version = 0)
    w.snapshot(t, version = 1)
    w.snapshot(t, version = 2)
    w.snapshot(t, version = 3)
  }

}.runAll()
