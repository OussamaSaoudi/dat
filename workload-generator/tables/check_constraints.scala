new WorkloadSuite("check_constraints") {

  test("cc_001_create_with_constraint", "CREATE TABLE with valid check constraint", "checkConstraints") { w =>
    w.sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("ALTER TABLE tbl ADD CONSTRAINT positive_id CHECK (id > 0)")
    w.sql("INSERT INTO tbl VALUES (1, 'a'),(2, 'b'),(3, 'c')")
    w.sql("INSERT INTO tbl VALUES (7, 'd'),(8, 'e')")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "id > 5")
    w.snapshot(t)
  }

  test("cc_002_show_tblproperties", "See constraints in table properties", "checkConstraints") { w =>
    w.sql("""CREATE TABLE tbl (x INT, y INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1, 10),(2, 20)")
    w.sql("ALTER TABLE tbl ADD CONSTRAINT myconstraint CHECK (x > 0)")
    w.sql("INSERT INTO tbl VALUES (5, 50),(6, 60)")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "x > 3")
    w.snapshot(t)
    w.snapshot(t, version = 0)
    w.snapshot(t, version = 1)
    w.snapshot(t, version = 2)
    w.snapshot(t, version = 3)
  }

  test("cc_003_delta_history", "Delta history for constraints", "checkConstraints") { w =>
    w.sql("""CREATE TABLE tbl (x INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1),(2),(3)")
    w.sql("ALTER TABLE tbl ADD CONSTRAINT positive CHECK (x > 0)")
    w.sql("INSERT INTO tbl VALUES (4),(5)")
    w.sql("ALTER TABLE tbl DROP CONSTRAINT positive")
    w.sql("INSERT INTO tbl VALUES (-1),(0)")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, version = 3)
    w.snapshotHistory(t)
  }

  test("cc_004_case_insensitive_drop", "Drop constraint is case insensitive", "checkConstraints") { w =>
    w.sql("""CREATE TABLE tbl (x INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1),(2),(3)")
    w.sql("ALTER TABLE tbl ADD CONSTRAINT MyConstraint CHECK (x > 0)")
    w.sql("INSERT INTO tbl VALUES (4),(5)")
    w.sql("ALTER TABLE tbl DROP CONSTRAINT MYCONSTRAINT")
    // After drop, negative values allowed
    w.sql("INSERT INTO tbl VALUES (-1),(0)")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "x < 0")
    w.snapshotHistory(t)
  }

  test("cc_005_varchar_constraint", "Constraint induced by varchar", "checkConstraints") { w =>
    w.sql("""CREATE TABLE tbl (id INT, s VARCHAR(10)) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1, 'ab'),(2, 'cdef')")
    w.sql("INSERT INTO tbl VALUES (3, 'ghij')")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "length(s) < 4")
    w.snapshot(t)
  }

  test("cc_006_basic_constraint", "Read table with check constraint", "checkConstraints") { w =>
    w.sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("ALTER TABLE tbl ADD CONSTRAINT positive_id CHECK (id > 0)")
    w.sql("INSERT INTO tbl VALUES (1, 'first'),(2, 'second'),(3, 'third')")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "id = 1")
    w.snapshot(t)
  }

  test("cc_007_multiple_constraints", "Read table with multiple check constraints", "checkConstraints") { w =>
    w.sql("""CREATE TABLE tbl (id INT, amount DECIMAL(10,2), status STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("ALTER TABLE tbl ADD CONSTRAINT positive_id CHECK (id > 0)")
    w.sql("ALTER TABLE tbl ADD CONSTRAINT positive_amount CHECK (amount >= 0)")
    w.sql("ALTER TABLE tbl ADD CONSTRAINT valid_status CHECK (status IN ('active', 'pending', 'closed'))")
    w.sql("INSERT INTO tbl VALUES (1, 10.50, 'active'),(2, 25.00, 'pending'),(3, 0.00, 'closed')")
    w.sql("INSERT INTO tbl VALUES (4, 100.00, 'active'),(5, 50.75, 'pending')")
    w.sql("INSERT INTO tbl VALUES (6, 200.00, 'closed')")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "amount > 50")
    w.read(t, predicate = "status = 'active'")
    w.snapshot(t)
  }

  test("cc_008_nested_constraint", "Read with nested column constraint", "checkConstraints") { w =>
    w.sql("""CREATE TABLE tbl (id INT, info STRUCT<name: STRING, age: INT>) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("ALTER TABLE tbl ADD CONSTRAINT adult CHECK (info.age >= 18)")
    w.sql("INSERT INTO tbl VALUES (1, named_struct('name', 'Alice', 'age', 25))")
    w.sql("INSERT INTO tbl VALUES (2, named_struct('name', 'Bob', 'age', 30))")
    w.sql("INSERT INTO tbl VALUES (3, named_struct('name', 'Charlie', 'age', 18))")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "info.age > 27")
    w.snapshot(t)
  }

  test("cc_009_array_constraint", "Read with array size constraint", "checkConstraints") { w =>
    w.sql("""CREATE TABLE tbl (id INT, tags ARRAY<STRING>) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("ALTER TABLE tbl ADD CONSTRAINT at_least_one_tag CHECK (size(tags) >= 1)")
    w.sql("INSERT INTO tbl VALUES (1, array('a','b','c'))")
    w.sql("INSERT INTO tbl VALUES (2, array('x'))")
    w.sql("INSERT INTO tbl VALUES (3, array('p','q','r','s'))")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "size(tags) > 2")
    w.snapshot(t)
  }

  test("cc_010_length_constraint", "Read with string length constraint", "checkConstraints") { w =>
    w.sql("""CREATE TABLE tbl (code STRING, description STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("ALTER TABLE tbl ADD CONSTRAINT code_length CHECK (length(code) = 5)")
    w.sql("INSERT INTO tbl VALUES ('ABCDE', 'first')")
    w.sql("INSERT INTO tbl VALUES ('FGHIJ', 'second')")
    w.sql("INSERT INTO tbl VALUES ('KLMNO', 'third')")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "code = 'ABCDE'")
    w.snapshot(t)
  }

  test("cc_011_compound_constraint", "Read with compound constraint", "checkConstraints") { w =>
    w.sql("""CREATE TABLE tbl (start_date DATE, end_date DATE, amount DECIMAL(10,2)) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("ALTER TABLE tbl ADD CONSTRAINT valid_range CHECK (end_date >= start_date AND amount > 0)")
    w.sql("INSERT INTO tbl VALUES ('2024-01-01', '2024-01-31', 100.00)")
    w.sql("INSERT INTO tbl VALUES ('2024-02-01', '2024-02-28', 200.00)")
    w.sql("INSERT INTO tbl VALUES ('2024-03-01', '2024-03-31', 50.00)")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "start_date >= '2024-02-01'")
    w.snapshot(t)
  }

  test("cc_012_not_null_constraint", "Read with NOT NULL-like constraint", "checkConstraints") { w =>
    w.sql("""CREATE TABLE tbl (id INT, required_field STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("ALTER TABLE tbl ADD CONSTRAINT required CHECK (required_field IS NOT NULL)")
    w.sql("INSERT INTO tbl VALUES (1, 'present')")
    w.sql("INSERT INTO tbl VALUES (2, 'also_present')")
    w.sql("INSERT INTO tbl VALUES (3, 'here_too')")
    val t = w.table("tbl")
    w.read(t)
    w.snapshot(t)
  }

  test("cc_013_time_travel", "Time travel with constraints", "checkConstraints") { w =>
    w.sql("""CREATE TABLE tbl (value INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1),(2),(3)")
    w.sql("INSERT INTO tbl VALUES (-1),(0)")
    w.sql("ALTER TABLE tbl ADD CONSTRAINT positive CHECK (value > 0)")
    w.sql("INSERT INTO tbl VALUES (10),(20)")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, version = 2)
    w.snapshotHistory(t)
  }

  test("cc_014_time_type_constraint", "CHECK constraints with TIME type columns", "checkConstraints") { w =>
    w.sql("""CREATE TABLE tbl (id INT, event_time STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("ALTER TABLE tbl ADD CONSTRAINT valid_time CHECK (event_time >= '09:00:00')")
    w.sql("INSERT INTO tbl VALUES (1, '09:00:00')")
    w.sql("INSERT INTO tbl VALUES (2, '10:30:00'),(3, '14:00:00')")
    w.sql("INSERT INTO tbl VALUES (4, '12:00:00'),(5, '17:30:00')")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "event_time >= '12:00:00'")
    w.snapshot(t)
  }

  test("cc_015_time_multiple_conditions", "CHECK constraints with TIME type - multiple conditions", "checkConstraints") { w =>
    w.sql("""CREATE TABLE tbl (id INT, start_time STRING, end_time STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("ALTER TABLE tbl ADD CONSTRAINT valid_range CHECK (end_time > start_time)")
    w.sql("INSERT INTO tbl VALUES (1, '08:00:00', '17:00:00')")
    w.sql("INSERT INTO tbl VALUES (2, '09:30:00', '18:00:00')")
    w.sql("INSERT INTO tbl VALUES (3, '06:00:00', '14:00:00')")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "start_time < '10:00:00'")
    w.snapshot(t)
  }

  test("cc_016_allowed_expressions", "Creating constraints with allowed expressions", "checkConstraints") { w =>
    w.sql("""CREATE TABLE tbl (num INT, text STRING, d DOUBLE) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("ALTER TABLE tbl ADD CONSTRAINT c1 CHECK (num > 0)")
    w.sql("ALTER TABLE tbl ADD CONSTRAINT c2 CHECK (length(text) <= 10)")
    w.sql("ALTER TABLE tbl ADD CONSTRAINT c3 CHECK (d >= 0.0)")
    w.sql("INSERT INTO tbl VALUES (1, 'short', 1.5)")
    w.sql("INSERT INTO tbl VALUES (5, 'hello', 3.14)")
    w.sql("INSERT INTO tbl VALUES (10, 'world', 0.0)")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "num > 7")
    w.snapshot(t)
  }

  test("cc_017_column_mapping", "Read constraints with column mapping", "checkConstraints", "columnMapping") { w =>
    w.sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'delta.enableDeletionVectors' = 'true')""")
    w.sql("ALTER TABLE tbl ADD CONSTRAINT positive_id CHECK (id > 0)")
    w.sql("INSERT INTO tbl VALUES (1, 'a'),(2, 'b'),(3, 'c')")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "value = 'a'")
    w.snapshot(t)
  }

  test("cc_018_drop_feature", "Drop constraint before drop feature", "checkConstraints") { w =>
    w.sql("""CREATE TABLE tbl (x INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("INSERT INTO tbl VALUES (1),(2),(3)")
    w.sql("ALTER TABLE tbl ADD CONSTRAINT positive CHECK (x > 0)")
    w.sql("INSERT INTO tbl VALUES (4),(5)")
    w.sql("ALTER TABLE tbl DROP CONSTRAINT positive")
    w.sql("INSERT INTO tbl VALUES (-1),(0)")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, version = 3)
    w.snapshotHistory(t)
  }

  test("cc_019_boolean_column_names", "Boolean column with constraints", "checkConstraints") { w =>
    w.sql("""CREATE TABLE tbl (id INT, flag BOOLEAN) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("ALTER TABLE tbl ADD CONSTRAINT flag_required CHECK (flag IS NOT NULL)")
    w.sql("INSERT INTO tbl VALUES (1, true),(2, false)")
    w.sql("INSERT INTO tbl VALUES (3, true),(4, true)")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "flag = true")
    w.snapshot(t)
  }

  test("cc_020_decimal_constraint", "Constraint with decimal column", "checkConstraints") { w =>
    w.sql("""CREATE TABLE tbl (id INT, price DECIMAL(10,2), quantity INT) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("ALTER TABLE tbl ADD CONSTRAINT positive_price CHECK (price > 0)")
    w.sql("ALTER TABLE tbl ADD CONSTRAINT positive_qty CHECK (quantity > 0)")
    w.sql("INSERT INTO tbl VALUES (1, 9.99, 5)")
    w.sql("INSERT INTO tbl VALUES (2, 49.99, 2)")
    w.sql("INSERT INTO tbl VALUES (3, 99.99, 1)")
    w.sql("INSERT INTO tbl VALUES (4, 149.99, 3)")
    w.sql("INSERT INTO tbl VALUES (5, 199.99, 10)")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "price > 100")
    w.snapshot(t)
  }

  test("cc_complex_expr", "Check constraint with AND/OR expression", "checkConstraints") { w =>
    w.sql("""CREATE TABLE tbl (age INT, name STRING) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("ALTER TABLE tbl ADD CONSTRAINT ck CHECK (age > 0 AND age < 200 OR name IS NOT NULL)")
    w.sql("INSERT INTO tbl VALUES (25, 'Alice'),(150, 'Bob')")
    w.sql("INSERT INTO tbl VALUES (30, 'Charlie')")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "age > 100")
    w.snapshot(t)
  }

  test("cc_null_aware", "IS NOT NULL constraint on nested struct", "checkConstraints") { w =>
    w.sql("""CREATE TABLE tbl (id INT, info STRUCT<name: STRING, age: INT>) USING delta
      TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')""")
    w.sql("ALTER TABLE tbl ADD CONSTRAINT ck_name CHECK (info.name IS NOT NULL)")
    w.sql("INSERT INTO tbl VALUES (1, named_struct('name', 'Alice', 'age', 25))")
    w.sql("INSERT INTO tbl VALUES (2, named_struct('name', 'Bob', 'age', 30))")
    val t = w.table("tbl")
    w.read(t)
    w.read(t, predicate = "info.age > 28")
    w.snapshot(t)
  }

}.runAll()
