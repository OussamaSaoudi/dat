/**
 * Iceberg Compatibility tests.
 */

new WorkloadSuite("iceberg_compat") {

  test("ice_compat_v1", "IcebergCompatV1", "icebergCompat") {
    sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
      TBLPROPERTIES ('delta.enableIcebergCompatV1' = 'true',
        'delta.columnMapping.mode' = 'name')""")
    sql("INSERT INTO tbl VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie')")
    val t = registerTable("tbl")
    read(t, name = "read_all")
    read(t, predicate = "name = 'bob'", name = "read_by_name")
    read(t, predicate = "id > 1", name = "read_id_gt_1")
    snapshot(t)
  }

  test("ice_compat_v2", "IcebergCompatV2", "icebergCompat") {
    sql("""CREATE TABLE tbl (id INT, value DOUBLE, category STRING) USING delta
      TBLPROPERTIES ('delta.enableIcebergCompatV2' = 'true',
        'delta.columnMapping.mode' = 'name')""")
    sql("INSERT INTO tbl VALUES (1,1.0,'A'),(2,2.0,'B'),(3,3.0,'A')")
    sql("INSERT INTO tbl VALUES (4,4.0,'B'),(5,5.0,'C')")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "category = 'A'")
    read(t, predicate = "value > 3.0")
    read(t, version = 1)
    snapshot(t)
  }

  test("ice_column_mapping", "IcebergCompat + column mapping", "icebergCompat") {
    sql("""CREATE TABLE tbl (id INT, name STRING, value DOUBLE) USING delta
      TBLPROPERTIES ('delta.enableIcebergCompatV2' = 'true',
        'delta.columnMapping.mode' = 'name')""")
    sql("INSERT INTO tbl VALUES (1,'one',1.0),(2,'two',2.0)")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "id = 1")
    snapshot(t)
  }

  test("ice_nested_map", "IcebergCompat V2 + MAP", "icebergCompat") {
    sql("""CREATE TABLE tbl (id INT, col2 MAP<INT, INT>) USING delta
      TBLPROPERTIES ('delta.enableIcebergCompatV2' = 'true',
        'delta.columnMapping.mode' = 'name')""")
    sql("INSERT INTO tbl VALUES (1, map(1, 10)), (2, map(2, 20))")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("ice_nested_array", "IcebergCompat V2 + ARRAY", "icebergCompat") {
    sql("""CREATE TABLE tbl (id INT, col2 ARRAY<INT>) USING delta
      TBLPROPERTIES ('delta.enableIcebergCompatV2' = 'true',
        'delta.columnMapping.mode' = 'name')""")
    sql("INSERT INTO tbl VALUES (1, array(1, 2)), (2, array(3))")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("ice_partitioned", "IcebergCompat + partitions", "icebergCompat") {
    sql("""CREATE TABLE tbl (id INT, name STRING, category STRING) USING delta
      PARTITIONED BY (category)
      TBLPROPERTIES ('delta.enableIcebergCompatV2' = 'true',
        'delta.columnMapping.mode' = 'name')""")
    sql("INSERT INTO tbl VALUES (1, 'alice', 'A'), (2, 'bob', 'B')")
    sql("INSERT INTO tbl VALUES (3, 'charlie', 'A'), (4, 'diana', 'C')")
    sql("INSERT INTO tbl VALUES (5, 'eve', 'B'), (6, 'frank', 'C')")
    val t = registerTable("tbl")
    read(t, name = "read_all")
    read(t, predicate = "category = 'A'", name = "read_category_A")
    read(t, predicate = "category = 'B'", name = "read_category_B")
    read(t, predicate = "category = 'C'", name = "read_category_C")
    snapshot(t)
  }

  test("ice_with_dv", "IcebergCompat + DVs", "icebergCompat", "dv") {
    sql("""CREATE TABLE tbl (id INT, value STRING) USING delta
      TBLPROPERTIES ('delta.enableIcebergCompatV2' = 'true',
        'delta.columnMapping.mode' = 'name',
        'delta.enableDeletionVectors' = 'true')""")
    sql("INSERT INTO tbl VALUES (1,'a'),(2,'b'),(3,'c')")
    sql("DELETE FROM tbl WHERE id = 2")
    val t = registerTable("tbl")
    read(t)
    read(t, version = 1)
    snapshot(t)
  }

  test("ice_metadata", "IcebergCompat table metadata verification", "icebergCompat") {
    sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
      TBLPROPERTIES ('delta.enableIcebergCompatV2' = 'true',
        'delta.enableChangeDataFeed' = 'true',
        'delta.columnMapping.mode' = 'name')""")
    sql("INSERT INTO tbl VALUES (1, 'alice'), (2, 'bob')")
    val t = registerTable("tbl")
    read(t)
    snapshot(t)
  }

  test("ice_nested_types", "IcebergCompat V2 with nested types", "icebergCompat") {
    sql("""CREATE TABLE tbl (id INT, tags ARRAY<STRING>, attrs MAP<STRING, STRING>) USING delta
      TBLPROPERTIES ('delta.enableIcebergCompatV2' = 'true',
        'delta.columnMapping.mode' = 'name')""")
    sql("INSERT INTO tbl VALUES (1, array('a','b'), map('key1','val1'))")
    sql("INSERT INTO tbl VALUES (2, array('c'), map('key2','val2','key3','val3'))")
    val t = registerTable("tbl")
    read(t)
    read(t, predicate = "id = 1")
    snapshot(t)
  }

  test("ice_complex_types", "IcebergCompatV2 with complex types (array/map/struct)", "icebergCompat") {
    sql("""CREATE TABLE tbl (
      id INT,
      tags ARRAY<STRING>,
      props MAP<STRING, STRING>,
      info STRUCT<name: STRING, score: DOUBLE>
    ) USING delta
      TBLPROPERTIES ('delta.enableIcebergCompatV2' = 'true',
        'delta.columnMapping.mode' = 'name')""")
    sql("""INSERT INTO tbl VALUES
      (1, array('tag1','tag2'), map('k1','v1'), named_struct('name','alice','score',92.5)),
      (2, array('tag3'), map('k2','v2','k3','v3'), named_struct('name','bob','score',87.5)),
      (3, array('tag4','tag5','tag6'), map('k4','v4'), named_struct('name','charlie','score',95.0))""")
    val t = registerTable("tbl")
    read(t, name = "read_all")
    read(t, predicate = "id = 2", name = "filter_id")
    snapshot(t)
  }

  test("ice_partition_evolution", "IcebergCompatV2 with partition columns", "icebergCompat") {
    sql("""CREATE TABLE tbl (id INT, value STRING, category STRING) USING delta
      PARTITIONED BY (category)
      TBLPROPERTIES ('delta.enableIcebergCompatV2' = 'true',
        'delta.columnMapping.mode' = 'name')""")
    sql("""INSERT INTO tbl VALUES
      (1, 'v1', 'cat_a'), (2, 'v2', 'cat_a'),
      (3, 'v3', 'cat_b'),
      (4, 'v4', 'cat_c')""")
    val t = registerTable("tbl")
    read(t, name = "read_all")
    read(t, predicate = "category = 'cat_a'", name = "filter_partition")
    snapshot(t)
  }

  test("ice_schema_evolution", "IcebergCompatV2 with schema evolution (ADD COLUMN)", "icebergCompat") {
    sql("""CREATE TABLE tbl (id INT, name STRING) USING delta
      TBLPROPERTIES ('delta.enableIcebergCompatV2' = 'true',
        'delta.columnMapping.mode' = 'name')""")
    sql("INSERT INTO tbl VALUES (1, 'alice'), (2, 'bob')")
    sql("ALTER TABLE tbl ADD COLUMN (score DOUBLE)")
    sql("INSERT INTO tbl VALUES (3, 'charlie', 95.0)")
    val t = registerTable("tbl")
    read(t, name = "read_latest")
    read(t, version = 1, name = "read_v1")
    snapshot(t)
    snapshot(t, version = 0)
    snapshot(t, version = 1)
    snapshot(t, version = 2)
    snapshot(t, version = 3)
  }

}
