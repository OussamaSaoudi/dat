# Harness Implementation Guide

This guide walks you through building a test harness that runs workload generator output against your Delta Lake implementation. For a concrete reference, see [`delta-kernel-rs/acceptance`](https://github.com/delta-incubator/delta-kernel-rs/tree/main/acceptance) — a production harness for the Rust Delta Kernel.

## Layout

Each workload directory is self-contained:

```
<workload>/
  table_info.json           # Table metadata (protocol, schema, tags)
  delta/                    # The Delta table under test
    _delta_log/
  specs/                    # One JSON spec per test operation
    workload_read.json
    workload_snapshot_v0.json
  expected/                 # Golden results
    workload_read/
      expected_data/        # Parquet files with expected rows
      expected_metadata/    # AddFile actions for data skipping validation
```

Your harness iterates over `specs/*.json`, executes each operation against `delta/`, and compares results to `expected/`.

---

## Step 1: Obtain Workloads

**Option A:** Download a pre-built tarball (recommended for CI). The delta-kernel-rs harness does this in `build.rs`:

```rust
// build.rs — downloads and extracts workloads at build time
fn extract_acceptance_workloads() {
    let tarball_url = format!(
        "https://github.com/delta-incubator/dat/releases/download/v0.04-preview/\
         v{VERSION}_dat_workloads.tar.gz"
    );
    let tarball_data = download_tarball(&tarball_url);
    let decoder = GzDecoder::new(BufReader::new(&tarball_data[..]));
    let mut archive = Archive::new(decoder);
    std::fs::create_dir_all(&output_dir).unwrap();
    for entry in archive.entries().unwrap() {
        entry.unwrap().unpack_in(&output_dir).unwrap();
    }
}
```

**Option B:** Generate from source:

```bash
cd workload-generator
WORKLOAD_PARALLEL=4 WORKLOAD_OUTPUT_DIR=/path/to/workloads sbt "Test/runMain io.delta.workload.TableScriptRunner tables/*.scala"
```

---

## Step 2: Discover and Run Specs

Each `specs/*.json` file is one test. The delta-kernel-rs harness uses [`datatest-stable`](https://docs.rs/datatest-stable) to turn every spec file into its own test case automatically:

```rust
datatest_stable::harness! {
    {
        test = acceptance_workloads_test,
        root = "workloads/",
        pattern = r"specs/.*\.json$"
    },
}
```

From the spec path, derive the test case root and load the spec:

```rust
pub fn from_spec_path(spec_path: impl AsRef<Path>) -> TestCase {
    let spec_path = spec_path.as_ref();
    let workload_name = spec_path.file_stem().unwrap().to_str().unwrap().to_string();
    let root_dir = spec_path.parent().unwrap()  // specs/
                            .parent().unwrap()   // <workload>/
                            .to_path_buf();
    let content = std::fs::read_to_string(spec_path).unwrap();
    let spec: Spec = serde_json::from_str(&content).unwrap();

    TestCase { root_dir, spec, workload_name, table_info: None }
}
```

The table is at `<root_dir>/delta/`. Expected data is at `<root_dir>/expected/<workload_name>/`.

---

## Step 3: Implement Spec Types

Specs are JSON files with a `"type"` discriminator. See the [Spec Format Reference](spec-reference.md) for the full JSON schema.

### Read Specs

```json
{
  "type": "read",
  "version": 2,
  "predicate": "id > 5",
  "columns": ["id", "name"],
  "expected": { "rowCount": 42, "fileCount": 3, "filesSkipped": 7 }
}
```

**Execute:** open `delta/`, apply time travel (if `version` or `timestamp`), scan with column projection, filter with predicate.

**Validate:** compare results against `expected/<spec_name>/expected_data/*.parquet` as an **order-independent multiset** (sort rows before comparing). Also assert `rowCount`. `fileCount`/`filesSkipped` are optional data-skipping checks.

The delta-kernel-rs implementation:

```rust
pub fn execute_read_workload(
    engine: Arc<dyn Engine>, table_root: &Url, read_spec: &ReadSpec,
) -> DeltaResult<ReadResult> {
    let predicate = read_spec.predicate.as_deref()
        .map(parse_predicate).transpose()?;

    let snapshot = build_snapshot(engine.as_ref(), table_root,
                                  read_spec.time_travel.as_ref())?;
    let mut scan_builder = snapshot.scan_builder();
    if let Some(ref cols) = read_spec.columns {
        let projected = snapshot.schema().project(cols)?;
        scan_builder = scan_builder.with_schema(projected);
    }

    let batches: Vec<RecordBatch> = scan_builder.build()?.execute(engine)?
        .map(|data| data?.try_into_record_batch())
        .try_collect()?;

    let batches = filter_batches_with_predicate(batches, predicate.as_ref())?;
    let row_count = batches.iter().map(|b| b.num_rows() as u64).sum();
    Ok(ReadResult { batches, schema: scan.logical_schema().clone(), row_count })
}
```

Validation loads the expected Parquet, strips hidden columns (`._`, `_SUCCESS`), and does a sorted row-by-row comparison:

```rust
pub fn validate_read_result(
    result: DeltaResult<ReadResult>, expected_dir: &Path, expected: &ReadExpected,
) -> Result<(), String> {
    match (result, expected) {
        (Ok(read_result), ReadExpected::Success { expected: exp }) => {
            let expected_data = read_expected_parquet(expected_dir)?;
            assert_data_matches(read_result.batches, &schema, expected_data)?;
            assert_eq!(read_result.row_count, exp.row_count);
            Ok(())
        }
        (Err(_), ReadExpected::Error { .. }) => Ok(()),
        (Ok(_), ReadExpected::Error { error }) =>
            Err(format!("Expected error '{}' but succeeded", error.error_code)),
        (Err(e), ReadExpected::Success { .. }) =>
            Err(format!("Expected success but got error: {}", e)),
    }
}
```

### Snapshot Specs

```json
{
  "type": "snapshot",
  "version": 3,
  "expected": {
    "protocol": { "minReaderVersion": 3, "minWriterVersion": 7, ... },
    "metadata": { "id": "abc123", "schemaString": "{...}", ... }
  }
}
```

**Execute:** build a snapshot at the given version.

**Validate:** assert `protocol == expected.protocol` and `metadata == expected.metadata` (deep equality).

```rust
pub fn validate_snapshot(
    result: DeltaResult<SnapshotResult>, expected: &SnapshotExpected,
) -> Result<(), String> {
    match (result, expected) {
        (Ok(snap), SnapshotExpected::Success { expected }) => {
            assert_eq!(snap.protocol, *expected.protocol);
            assert_eq!(snap.metadata, *expected.metadata);
            Ok(())
        }
        (Err(_), SnapshotExpected::Error { .. }) => Ok(()),
        (Ok(_), SnapshotExpected::Error { error }) =>
            Err(format!("Expected error '{}' but succeeded", error.error_code)),
        (Err(e), SnapshotExpected::Success { .. }) =>
            Err(format!("Expected success but got error: {}", e)),
    }
}
```

### Error Specs

Any spec type can have `"expectedError"` instead of `"expected"`:

```json
{
  "type": "read",
  "version": 999,
  "expectedError": { "errorCode": "DELTA_VERSION_NOT_FOUND", "errorMessage": "..." }
}
```

Run the operation and assert it fails. Matching the exact error code is ideal but optional — just asserting failure is a valid starting point.

### CDF Specs (Change Data Feed)

```json
{
  "type": "cdf",
  "startVersion": 1,
  "endVersion": 3,
  "expected": { "rowCount": 150 }
}
```

**Execute:** read change data feed between the given versions (or timestamps if `startTimestamp`/`endTimestamp` provided).

**Validate:** compare CDF results against `expected/<spec_name>/expected_data/*.parquet`. The expected data includes `_change_type`, `_commit_version`, and `_commit_timestamp` columns.

```rust
pub fn execute_cdf_workload(
    engine: Arc<dyn Engine>, table_root: &Url, cdf_spec: &CdfSpec,
) -> DeltaResult<CdfResult> {
    let table = Table::new(table_root.clone());
    let cdf_scan = table.table_changes(engine.as_ref(),
        cdf_spec.start_version, cdf_spec.end_version)?;

    let batches: Vec<RecordBatch> = cdf_scan.execute(engine)?
        .map(|data| data?.try_into_record_batch())
        .try_collect()?;

    Ok(CdfResult { batches, row_count: batches.iter().map(|b| b.num_rows()).sum() })
}
```

### Domain Metadata Specs

```json
{
  "type": "domain_metadata",
  "version": 2,
  "expected": {
    "domains": [
      { "domain": "myApp.txnState", "configuration": "{\"key\":\"value\"}", "removed": false }
    ]
  }
}
```

**Execute:** build a snapshot at the given version and extract domain metadata actions.

**Validate:** assert the domain metadata entries match expected (domain name, configuration JSON, removed flag).

```rust
pub fn execute_domain_metadata_workload(
    engine: Arc<dyn Engine>, table_root: &Url, spec: &DomainMetadataSpec,
) -> DeltaResult<Vec<DomainMetadata>> {
    let snapshot = Snapshot::try_new(table_root.clone(), engine.as_ref(),
        Some(spec.version))?;
    Ok(snapshot.domain_metadata().collect())
}
```

### AppTxn Specs (Application Transactions)

```json
{
  "type": "appTxn",
  "version": 3,
  "expected": {
    "transactions": [
      { "appId": "myApp", "version": 5, "lastUpdated": 1234567890 }
    ]
  }
}
```

**Execute:** build a snapshot and extract SetTransaction actions for the given app IDs.

**Validate:** assert transaction versions match expected.

```rust
pub fn execute_app_txn_workload(
    engine: Arc<dyn Engine>, table_root: &Url, spec: &AppTxnSpec,
) -> DeltaResult<Vec<SetTransaction>> {
    let snapshot = Snapshot::try_new(table_root.clone(), engine.as_ref(),
        Some(spec.version))?;
    Ok(snapshot.transactions().collect())
}
```

### Write Specs

```json
{
  "type": "write",
  "commits": [
    {
      "operation": "create_table",
      "schema": { "type": "struct", "fields": [...] },
      "properties": { "delta.enableDeletionVectors": "true" }
    },
    {
      "operation": "insert",
      "dataFiles": ["data/commit_1/part-0000-abc.parquet"]
    },
    {
      "operation": "delete",
      "predicate": "id > 100"
    }
  ]
}
```

**Execute:** replay each commit in order using your writer implementation:
1. For `create_table`: create a new Delta table with the given schema and properties
2. For `insert`: append the referenced data files to the table
3. For `update`/`delete`: apply the operation with the given predicate
4. For `evolve_schema`: apply schema changes (add/rename/drop columns)
5. For low-level `commit`: directly apply the specified Delta actions

**Validate:** after replaying all commits, compare the resulting table against `expected/latest/table_content/*.parquet`.

```rust
pub fn execute_write_workload(
    engine: Arc<dyn Engine>, output_dir: &Path, write_spec: &WriteSpec,
) -> DeltaResult<()> {
    let table_path = output_dir.join("result_table");

    for (idx, commit) in write_spec.commits.iter().enumerate() {
        match commit.operation.as_str() {
            "create_table" => {
                create_table(engine.clone(), &table_path, &commit.schema,
                    commit.partition_columns.as_deref(),
                    commit.properties.as_ref())?;
            }
            "insert" => {
                let data_files = commit.data_files.as_ref()
                    .map(|files| resolve_data_paths(output_dir, files));
                insert_data(engine.clone(), &table_path, data_files)?;
            }
            "delete" => {
                delete_rows(engine.clone(), &table_path,
                    commit.predicate.as_deref().unwrap())?;
            }
            "update" => {
                update_rows(engine.clone(), &table_path,
                    commit.predicate.as_deref().unwrap(),
                    commit.set.as_ref().unwrap())?;
            }
            // ... handle other operations
            _ => {}
        }
    }
    Ok(())
}
```

### Checkpoint Specs

```json
{
  "type": "checkpoint",
  "version": 10,
  "expected": {
    "protocol": { "minReaderVersion": 1, "minWriterVersion": 2 },
    "metadata": { "id": "abc123", ... },
    "txn": [{ "appId": "app-1", "version": 42 }],
    "domainMetadata": [{ "domain": "myApp.config", "configuration": "{}" }]
  }
}
```

**Execute:** build a snapshot at the checkpoint version and extract protocol, metadata, transactions, and domain metadata.

**Validate:** assert all extracted values match the expected checkpoint state. Optionally validate that the checkpoint file itself can be read directly.

```rust
pub fn execute_checkpoint_workload(
    engine: Arc<dyn Engine>, table_root: &Url, spec: &CheckpointSpec,
) -> DeltaResult<CheckpointResult> {
    let snapshot = Snapshot::try_new(table_root.clone(), engine.as_ref(),
        Some(spec.version))?;

    Ok(CheckpointResult {
        protocol: snapshot.protocol().clone(),
        metadata: snapshot.metadata().clone(),
        txn: snapshot.transactions().collect(),
        domain_metadata: snapshot.domain_metadata().collect(),
    })
}

pub fn validate_checkpoint(
    result: DeltaResult<CheckpointResult>, expected: &CheckpointExpected,
) -> Result<(), String> {
    let result = result.map_err(|e| format!("Checkpoint read failed: {}", e))?;

    assert_eq!(result.protocol, expected.protocol,
        "Protocol mismatch");
    assert_eq!(result.metadata.id, expected.metadata.id,
        "Metadata ID mismatch");

    if let Some(expected_txn) = &expected.txn {
        let actual_txn: HashSet<_> = result.txn.iter().collect();
        let expected_txn: HashSet<_> = expected_txn.iter().collect();
        assert_eq!(actual_txn, expected_txn, "Transaction mismatch");
    }

    Ok(())
}
```

### CRC Specs (Checksum)

```json
{
  "type": "crc",
  "version": 5,
  "expected": {
    "tableSizeBytes": 12345,
    "numFiles": 3,
    "numRemoveFiles": 0,
    "protocol": { ... },
    "metadata": { ... }
  }
}
```

**Execute:** read the `.crc` sidecar file for the given version and parse its contents.

**Validate:** assert that the CRC statistics match the expected values. This tests that your implementation correctly reads and interprets CRC files.

```rust
pub fn execute_crc_workload(
    table_root: &Url, spec: &CrcSpec,
) -> DeltaResult<CrcResult> {
    let crc_path = format!("{}/_delta_log/{:020}.crc",
        table_root.path(), spec.version);
    let crc_content = std::fs::read_to_string(&crc_path)?;
    let crc: CrcFile = serde_json::from_str(&crc_content)?;

    Ok(CrcResult {
        table_size_bytes: crc.table_size_bytes,
        num_files: crc.num_files,
        num_remove_files: crc.num_remove_files,
        num_transactions: crc.num_transactions,
        num_domain_metadata: crc.num_domain_metadata,
        deletion_vectors: crc.deletion_vectors,
    })
}

pub fn validate_crc(
    result: DeltaResult<CrcResult>, expected: &CrcExpected,
) -> Result<(), String> {
    let result = result.map_err(|e| format!("CRC read failed: {}", e))?;

    if let Some(exp) = expected.table_size_bytes {
        assert_eq!(result.table_size_bytes, exp, "tableSizeBytes mismatch");
    }
    if let Some(exp) = expected.num_files {
        assert_eq!(result.num_files, exp, "numFiles mismatch");
    }
    // ... validate other fields

    Ok(())
}
```

---

## Step 4: Incremental Adoption

Don't try to pass every test at once. Use `table_info.json` protocol fields and the skip list to gate features:

| Phase | Scope | Approximate tests |
|-------|-------|-----------------:|
| 1 | Basic reads (`minReaderVersion <= 1`) | ~200 |
| 2 | Time travel, predicates | ~500 |
| 3 | Deletion vectors | ~600 |
| 4 | Column mapping | ~650 |
| 5 | Checkpoints, CRC, snapshot specs | ~800 |
| 6 | Error handling | ~900 |
| 7 | CDF, metadata | ~1200 |
| 8 | Write specs (basic: create, insert) | ~1400 |
| 9 | Write specs (full: update, delete, schema evolution) | ~1600+ |

---

## Debugging Failures

- **`repro/generate.scala`** in each workload shows the exact Scala that created the table.
- **`delta/_delta_log/*.json`** contains raw commits — read them to understand table state.
- **`table_info.json` → `logInfo`** gives a quick summary: `numCommits`, `numAddFiles`, `lastCheckpointVersion`, `sizeInBytes`.

| Symptom | Likely Cause |
|---------|-------------|
| Row count matches but multiset differs | Type coercion (int vs long, null handling, timestamp precision) |
| All reads fail for DV tables | Deletion vector support missing |
| Snapshot protocol doesn't match | Missing reader/writer feature parsing |
| Time travel fails | Version resolution logic incorrect |
| `fileCount` mismatch but `rowCount` matches | Data skipping not working (reading all files) |
| Error spec passes (should fail) | Missing validation (e.g., not checking protocol version) |
