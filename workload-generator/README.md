# Delta Workload Generator

Generate acceptance test workloads for Delta implementations (e.g., [delta-kernel-rs](https://github.com/delta-io/delta-kernel-rs)) from Apache Spark.

Write a script that creates Delta tables with normal SQL, declare what specs to capture, and the framework generates complete workload directories with expected data validated row-for-row against Spark.

## Documentation

| Document | Description |
|----------|-------------|
| **[Spec Format Reference](docs/spec-reference.md)** | Complete JSON schema for every spec type (read, snapshot, CDF, checkpoint, CRC, domain metadata, txn) with exhaustive examples |
| **[Coverage Matrix](docs/coverage-matrix.md)** | All tests across 40 suites — what Delta features your engine gets tested on |
| **[Harness Implementation Guide](docs/harness-implementation-guide.md)** | Step-by-step guide to build a test harness that runs workloads against your engine, with Rust and Python examples |
| **[Authoring Guide](docs/authoring-guide.md)** | How to write new workload suites, patterns, recipes, and debugging tips |
| **[Design Doc](docs/design-doc.md)** | Architecture decisions, alternatives considered, and rationale for each choice |

## Requirements

- Java 17+
- Apache Spark 4.1.x with Scala 2.13
- Delta Spark 4.1.0 (pulled automatically via `--packages`)
- sbt 1.9+ (for building)

## Quick Start

```bash
cd workload-generator

# Run one suite
WORKLOAD_OUTPUT_DIR=/tmp/workloads sbt "testOnly *ReadsSuite"

# Run all table suites
WORKLOAD_OUTPUT_DIR=/tmp/workloads sbt "testOnly io.delta.workload.tables.*"

# Run a specific test
WORKLOAD_OUTPUT_DIR=/tmp/workloads sbt "testOnly *ReadsSuite -- -t read_basic"

# Force regeneration (even if output exists)
WORKLOAD_FORCE=true WORKLOAD_OUTPUT_DIR=/tmp/workloads sbt "testOnly *ReadsSuite"
```

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                      WorkloadTestSuite                               │
│  ┌─────────────────────────────────────────────────────────────────┐│
│  │ test("name") {                                                  ││
│  │   sql("CREATE TABLE ...")      // Setup tables via SQL          ││
│  │   val t = registerTable("tbl") // Get table handle              ││
│  │   read(t)                      // Declare read spec             ││
│  │   snapshot(t)                  // Declare snapshot spec         ││
│  │   cdf(t, startVersion=1)       // Declare CDF spec              ││
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
| `WorkloadTestSuite` | ScalaTest base class with workload generation integration |
| `WorkloadOps` | DSL trait: `sql()`, `registerTable()`, `read()`, `snapshot()` |
| `WorkloadGenerator` | Orchestrates table copy, spec capture, and validation |
| `ReadCapture` | Captures read specs with expected row data |
| `SnapshotCapture` | Captures snapshot specs with protocol/metadata |
| `CdfCapture` | Captures change data feed specs |
| `DomainMetadataCapture` | Captures domain metadata entries |
| `AppTxnCapture` | Captures SetTransaction (appTxn) entries |
| `TableCopier` | Copies Delta tables with optional timestamp sync |
| `TableInfoWriter` | Writes table metadata (schema, protocol, stats) |
| `JsonUtil` | Shared JSON utilities, multiset comparison |

## Writing a Suite

Each suite is a ScalaTest class extending `WorkloadTestSuite`. Each `test` creates tables, declares specs, and the framework handles the rest.

```scala
// src/test/scala/io/delta/workload/tables/CdcSuite.scala
package io.delta.workload.tables

import io.delta.workload.WorkloadTestSuite

class CdcSuite extends WorkloadTestSuite("cdc") {

  test("cdf_merge") {
    sql("""CREATE TABLE target (id INT, val STRING) USING delta
      TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')""")
    sql("INSERT INTO target VALUES (1, 'old'), (2, 'old')")
    sql("CREATE TABLE source (id INT, val STRING) USING delta")
    sql("INSERT INTO source VALUES (2, 'new'), (3, 'new')")
    sql("""MERGE INTO target t USING source s ON t.id = s.id
      WHEN MATCHED THEN UPDATE SET val = s.val
      WHEN NOT MATCHED THEN INSERT *""")

    val tgt = registerTable("target")
    read(tgt)
    read(tgt, version = 0)
    read(tgt, predicate = "id > 1")
    snapshot(tgt)
    cdf(tgt, startVersion = 2)
  }

}
```

See the [Authoring Guide](docs/authoring-guide.md) for the complete DSL reference, patterns, and debugging tips.

### Test Semantics

- **Pass**: output kept, skipped on re-run (incremental)
- **Fail**: output deleted, auto-retries next run (no `--force` needed)
- **Each test is independent**: failures don't stop the suite

## Output Structure

Each `registerTable()` call produces a flat top-level directory:

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

See the [Spec Format Reference](docs/spec-reference.md) for the complete JSON schema of every spec type.

## Spec Types

| Type | What It Tests | Details |
|------|--------------|---------|
| **Read** | Data reads with time travel, predicates, column projection, data skipping | [Reference](docs/spec-reference.md#read-spec) |
| **Snapshot** | Protocol and metadata reconstruction from log replay | [Reference](docs/spec-reference.md#snapshot-spec) |
| **CDF** | Change data feed across version/timestamp ranges | [Reference](docs/spec-reference.md#cdf-spec-change-data-feed) |
| **Domain Metadata** | Domain metadata entries in the log | [Reference](docs/spec-reference.md#domain-metadata-spec) |
| **AppTxn** | Application transaction IDs (SetTransaction) | [Reference](docs/spec-reference.md#apptxn-spec-application-transaction) |

## Workload Suites

Workload suites are in `src/test/scala/io/delta/workload/tables/`. Each suite extends `WorkloadTestSuite` and covers a specific Delta feature area. See the [Coverage Matrix](docs/coverage-matrix.md) for the full inventory of tests.

## Building a Test Harness

If you're implementing a Delta engine and want to use these workloads for acceptance testing, see the [Harness Implementation Guide](docs/harness-implementation-guide.md). It walks through:

1. Discovering and filtering workloads
2. Implementing each spec type handler
3. Multiset row comparison
4. Error code mapping
5. CI integration
6. Incremental adoption strategy

## CI Integration

```bash
# Generate workloads and fail on any validation errors
WORKLOAD_OUTPUT_DIR=/tmp/workloads sbt "testOnly io.delta.workload.tables.*"

# Exit code:
#   0 = all tests passed
#   1 = one or more tests failed validation
```

## Troubleshooting

### Spark version mismatch
If you see `NoSuchMethodError` related to Delta APIs, ensure:
- build.sbt uses Delta 4.1.0 / Spark 4.1.x
- Your Spark installation matches (Spark 4.1.x with Scala 2.13)

### Memory issues
For large workloads:
```bash
export SBT_OPTS="-Xmx4g"
WORKLOAD_OUTPUT_DIR=/tmp/workloads sbt "testOnly *LargeSuite"
```

### Re-running failed tests
Failed tests auto-cleanup their output. Just re-run:
```bash
WORKLOAD_OUTPUT_DIR=/tmp/workloads sbt "testOnly *ReadsSuite"
```

Use `WORKLOAD_FORCE=true` to regenerate all tests (including passed ones):
```bash
WORKLOAD_FORCE=true WORKLOAD_OUTPUT_DIR=/tmp/workloads sbt "testOnly *ReadsSuite"
```
