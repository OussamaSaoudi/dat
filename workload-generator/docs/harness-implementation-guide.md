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
WORKLOAD_OUTPUT_DIR=/path/to/workloads sbt "testOnly io.delta.workload.tables.*"
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

### CDF Specs

```json
{
  "type": "cdf",
  "startVersion": 0,
  "endVersion": 3,
  "predicate": "id > 5",
  "columns": ["id", "name"],
  "expected": { "rowCount": 42 }
}
```

**Execute:** Open `delta/`, read the change data feed from `startVersion` to `endVersion` (or `startTimestamp`/`endTimestamp`), apply predicate and column projection.

**Validate:** Compare results against `expected/<spec_name>/expected_data/*.parquet` as an **order-independent multiset**. CDF rows include metadata columns: `_change_type` (insert/update_preimage/update_postimage/delete), `_commit_version`, `_commit_timestamp`.

```rust
pub fn execute_cdf_workload(
    engine: Arc<dyn Engine>, table_root: &Url, cdf_spec: &CdfSpec,
) -> DeltaResult<CdfResult> {
    let table = Table::try_from_uri(table_root)?;
    let mut builder = table.change_data_feed_builder();

    if let Some(start) = cdf_spec.start_version {
        builder = builder.with_starting_version(start);
    }
    if let Some(end) = cdf_spec.end_version {
        builder = builder.with_ending_version(end);
    }

    let batches: Vec<RecordBatch> = builder.build()?.execute(engine)?
        .map(|data| data?.try_into_record_batch())
        .try_collect()?;

    let row_count = batches.iter().map(|b| b.num_rows() as u64).sum();
    Ok(CdfResult { batches, row_count })
}
```

### Domain Metadata Specs

```json
{
  "type": "domain_metadata",
  "version": 2,
  "expected": {
    "domain": "testDomain1",
    "configuration": "{\"key\":\"value\"}",
    "removed": false
  }
}
```

**Execute:** Build a snapshot at the given version. Extract domain metadata actions.

**Validate:** Assert the specified domain exists (if `removed: false`) or is absent (if `removed: true`), and that `configuration` matches.

```rust
pub fn validate_domain_metadata(
    result: DeltaResult<Snapshot>, expected: &DomainMetadataExpected,
) -> Result<(), String> {
    let snapshot = result?;
    let domain_actions = snapshot.domain_metadata();
    let matching = domain_actions.iter()
        .find(|dm| dm.domain == expected.domain);

    if expected.removed {
        if matching.is_some() {
            return Err(format!("Domain '{}' should be removed but is present", expected.domain));
        }
    } else {
        let dm = matching.ok_or_else(||
            format!("Domain '{}' not found", expected.domain))?;
        if dm.configuration != expected.configuration {
            return Err(format!("Configuration mismatch for domain '{}'", expected.domain));
        }
    }
    Ok(())
}
```

### AppTxn Specs

```json
{
  "type": "appTxn",
  "version": 1,
  "expected": {
    "appId": "myapp",
    "txnVersion": 42
  }
}
```

**Execute:** Build a snapshot at the given version. Extract SetTransaction (txn) actions.

**Validate:** Assert a SetTransaction for `appId` exists with the expected `txnVersion`.

```rust
pub fn validate_app_txn(
    result: DeltaResult<Snapshot>, expected: &TxnExpected,
) -> Result<(), String> {
    let snapshot = result?;
    let txn_map = snapshot.transactions();
    let actual_version = txn_map.get(&expected.app_id)
        .ok_or_else(|| format!("No SetTransaction for appId '{}'", expected.app_id))?;

    if *actual_version != expected.txn_version {
        return Err(format!(
            "SetTransaction version mismatch for '{}': expected {}, got {}",
            expected.app_id, expected.txn_version, actual_version
        ));
    }
    Ok(())
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

---

## Step 4: Incremental Adoption

Don't try to pass every test at once. Use `table_info.json` protocol fields and the skip list to gate features:

| Phase | Scope | Approximate tests |
|-------|-------|-----------------:|
| 1 | Basic reads (`minReaderVersion <= 1`) | ~200 |
| 2 | Time travel, predicates | ~500 |
| 3 | Deletion vectors | ~600 |
| 4 | Column mapping | ~650 |
| 5 | Checkpoints, snapshot specs | ~800 |
| 6 | Error handling | ~900 |

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
