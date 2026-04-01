# Delta Workload Generator

Generate acceptance test workloads for Delta implementations (e.g., [delta-kernel-rs](https://github.com/delta-io/delta-kernel-rs)) from Apache Spark.

Write a script that creates Delta tables with normal SQL, declare what specs to capture, and the framework generates complete workload directories with expected data, validated row-for-row against Spark.

## Requirements

- Java 17+
- Apache Spark 4.1.x with Scala 2.13
- Delta Spark 4.1.0 (pulled automatically via `--packages`)
- sbt 1.9+ (for building)

## Quick Start

```bash
cd workload-generator
sbt assembly

# Run one suite
./bin/generate-workload.sh tables/reads.scala --output-dir /tmp/workloads

# Run all suites
./bin/generate-workload.sh tables/ --output-dir /tmp/workloads

# Interactive spark-shell with workload library
./bin/generate-workload.sh --interactive
```

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        WorkloadSuite                                 │
│  ┌─────────────────────────────────────────────────────────────────┐│
│  │ test("name", "description") { w =>                              ││
│  │   w.sql("CREATE TABLE ...")  // Setup tables via SQL            ││
│  │   val t = w.table("tbl")     // Get table handle                ││
│  │   w.read(t)                  // Declare read spec               ││
│  │   w.snapshot(t)              // Declare snapshot spec           ││
│  │   w.cdf(t, startVersion=1)   // Declare CDF spec                ││
│  │ }                                                               ││
│  └─────────────────────────────────────────────────────────────────┘│
└───────────────────────────────┬─────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     WorkloadGenerator                                │
│  1. Copy Delta table to output directory                            │
│  2. For each declared spec:                                         │
│     - Execute against copied table                                  │
│     - Capture results/expected data                                 │
│     - Validate by re-execution                                      │
│     - Write spec JSON + expected data                               │
│  3. Write table_info.json                                           │
└───────────────────────────────┬─────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    Output Directory                                  │
│  <testname>/                                                        │
│    delta/           # Copied Delta table                            │
│    specs/           # Spec JSON files                               │
│    expected/        # Parquet expected data                         │
│    table_info.json  # Table metadata                                │
│    repro/           # Script to reproduce                           │
└─────────────────────────────────────────────────────────────────────┘
```

### Core Components

| Component | Purpose |
|-----------|---------|
| `WorkloadSuite` | Test-suite semantics: register tests, run all, handle failures |
| `WorkloadContext` | User API inside tests: `sql()`, `table()`, `read()`, `snapshot()` |
| `WorkloadGenerator` | Orchestrates table copy, spec capture, and validation |
| `ReadCapture` | Captures read specs with expected row data |
| `SnapshotCapture` | Captures snapshot specs with protocol/metadata |
| `CdfCapture` | Captures change data feed specs |
| `TableCopier` | Copies Delta tables with optional timestamp sync |
| `TableInfoWriter` | Writes table metadata (schema, protocol, stats) |
| `JsonUtil` | Shared JSON utilities, multiset comparison |

## Writing a Script

Each script is a `WorkloadSuite` - like a test suite. Each `test` creates tables, declares specs, and the framework handles the rest.

```scala
new WorkloadSuite("cdc") {

  test("cdf_merge", "MERGE with CDC enabled") { w =>
    w.sql("""CREATE TABLE target (id INT, val STRING) USING delta
      TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')""")
    w.sql("INSERT INTO target VALUES (1, 'old'), (2, 'old')")
    w.sql("CREATE TABLE source (id INT, val STRING) USING delta")
    w.sql("INSERT INTO source VALUES (2, 'new'), (3, 'new')")
    w.sql("""MERGE INTO target t USING source s ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET val = s.val
      WHEN NOT MATCHED THEN INSERT *""")

    val tgt = w.table("target")
    w.read(tgt)
    w.read(tgt, version = 0)
    w.read(tgt, predicate = "id > 1")
    w.snapshot(tgt)
    w.cdf(tgt, startVersion = 2)
  }

  test("cdf_insert_delete", "INSERT then DELETE with CDC") { w =>
    // ...
  }

}.runAll()
```

### Test Semantics

- **Pass**: output kept, skipped on re-run (incremental)
- **Fail**: output deleted, auto-retries next run (no `--force` needed)
- **Each test is independent**: failures don't stop the suite

## Output Structure

Each `w.table()` call produces a flat top-level directory:

```
cdf_merge_target/
  delta/                                  # Copy of the Delta table
    _delta_log/
      00000000000000000000.json
      ...
    part-00000-*.parquet
    ...
  specs/
    cdf_merge_target_read.json            # Read spec (latest)
    cdf_merge_target_read_v0.json         # Read at version 0
    cdf_merge_target_read_id_gt_1.json    # Filtered read
    cdf_merge_target_snapshot.json        # Snapshot construction
    cdf_merge_target_cdf_v2.json          # CDF spec
  expected/
    cdf_merge_target_read/
      expected_data/                      # Parquet expected rows
      expected_metadata/                  # AddFile actions
    ...
  table_info.json                         # Table schema, protocol, stats
  repro/generate.scala                    # Script to reproduce
```

### Spec JSON Format

**Read spec:**
```json
{
  "type": "read",
  "version": 0,
  "predicate": "id > 5",
  "columns": ["id", "name"],
  "expected": {
    "rowCount": 42,
    "fileCount": 3,
    "filesSkipped": 7
  }
}
```

**Snapshot spec:**
```json
{
  "type": "snapshot",
  "version": 2,
  "expected": {
    "protocol": { "minReaderVersion": 1, "minWriterVersion": 2 },
    "metadata": { "id": "...", "schemaString": "...", ... }
  }
}
```

**Error spec:**
```json
{
  "type": "read",
  "version": 999,
  "error": {
    "errorCode": "VERSION_NOT_FOUND",
    "errorMessage": "Cannot find version 999"
  }
}
```

## API Reference

### Inside a test body

| Method | Description |
|--------|-------------|
| `w.sql(stmt)` | Execute Spark SQL |
| `w.spark` | Direct SparkSession access |
| `w.table("name")` | Get handle to a managed Spark table |
| `w.tableFromPath("/path")` | Get handle to a table on disk |

### Spec declaration

| Method | Auto-generated name |
|--------|-------------------|
| `w.read(t)` | `read` |
| `w.read(t, version = 0)` | `read_v0` |
| `w.read(t, predicate = "id > 5")` | `read_id_gt_5` |
| `w.read(t, columns = Seq("id"))` | `read_cols_id` |
| `w.snapshot(t)` | `snapshot` |
| `w.snapshot(t, version = 0)` | `snapshot_v0` |
| `w.snapshotHistory(t)` | Snapshot at every version |
| `w.cdf(t, startVersion = 1)` | `cdf_v1` |
| `w.cdf(t, startVersion = 1, endVersion = 3)` | `cdf_v1_to_v3` |
| `w.domainMetadata(t, domain, config)` | `dm_<domain>` |
| `w.txn(t, appId, txnVersion)` | `txn_<appId>` |

All spec methods accept an optional `name` parameter to override auto-naming.

### Table mutation

Mutate the copied table before specs are captured:

```scala
// Delete files
w.mutateTable(t) { tableDir =>
  java.nio.file.Files.delete(tableDir.resolve("some_file.parquet"))
}

// Modify commit actions
w.modifyCommitActions(t, version = 1) { (actionType, actionNode) =>
  if (actionType == "add") {
    actionNode.put("stats", """{"numRecords":999}""")
  }
  true  // keep the action (return false to drop)
}
```

Failed reads are auto-captured as error specs.

### Utilities

```scala
// Get timestamp for time-travel
val ts = t.getTimestampForVersion(1)
w.read(t, timestamp = ts)
```

## Validation

Every spec is self-validated after capture to ensure reproducibility:

### Read/CDF specs
1. Execute read against copied table
2. Write expected data to Parquet
3. Re-execute same read
4. Compare results using canonical JSON multiset (order-independent)
5. Compare AddFile actions for data skipping validation

### Error specs
1. Capture error code and message
2. Re-execute to confirm error is reproducible
3. Verify error code matches

### Snapshot specs
1. Load snapshot, extract protocol/metadata
2. Write spec with expected values
3. Re-load snapshot and deep-compare protocol/metadata

This ensures:
- Test workloads are deterministic
- Expected data exactly matches what Spark produces
- Error conditions are stable, not transient

## Workload Suites

Workload scripts are in `tables/`. Each file is a `WorkloadSuite` covering a specific Delta feature area:

| Suite | Features covered |
|-------|-----------------|
| `reads.scala` | Basic reads, predicates, time travel, column selection |
| `deletion_vectors.scala` | Deletion vectors, DV-enabled deletes/updates |
| `checkpoints.scala` | Checkpointing, multi-part checkpoints |
| `log_replay.scala` | Log replay, version reconstruction |
| `column_mapping.scala` | Column mapping modes (id, name) |
| `time_travel.scala` | Time travel by version and timestamp |
| `cdf.scala` | Change data feed (CDC) |
| `protocol_versions.scala` | Protocol version handling |
| `evolvability.scala` | Schema evolution |
| `corruption.scala` | Error handling for corrupted tables |
| `domain_metadata.scala` | Domain metadata feature |

Run all suites: `./bin/generate-workload.sh tables/`

## CI Integration

The workload generator includes self-validation, making it suitable for CI:

```bash
# Generate workloads and fail on any validation errors
./bin/generate-workload.sh tables/ --output-dir /tmp/workloads

# Exit code:
#   0 = all tests passed
#   1 = one or more tests failed validation
```

To integrate with delta-kernel-rs CI:
1. Generate workloads in CI or pre-commit
2. Commit generated workloads to the test resources
3. delta-kernel-rs acceptance tests read specs and validate against expected data

## Troubleshooting

### Spark version mismatch
If you see `NoSuchMethodError` related to Delta APIs, ensure:
- build.sbt uses Delta 3.3.2 / Spark 3.5.x
- generate-workload.sh uses matching `--packages` version
- Your Spark installation matches (Spark 3.5.x with Scala 2.13)

### Memory issues
For large workloads:
```bash
export SPARK_DRIVER_MEMORY=4g
./bin/generate-workload.sh tables/large_suite.scala
```

### Re-running failed tests
Failed tests auto-cleanup their output. Just re-run without `--force`:
```bash
./bin/generate-workload.sh tables/reads.scala
```

Use `--force` to regenerate all tests (including passed ones):
```bash
./bin/generate-workload.sh tables/reads.scala --force
```
