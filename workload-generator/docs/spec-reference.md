# Spec Format Reference

This document defines every spec type produced by the workload generator. Each spec is a self-contained JSON file describing an operation to execute against a Delta table and the expected result. Specs live in the `specs/` directory of each workload output.

All specs follow the same top-level pattern:

```json
{
  "type": "<spec_type>",
  ...operation parameters...,
  "expected": { ...success expectations... },
  "expectedError": { "errorCode": "...", "errorMessage": "..." }
}
```

Exactly one of `expected` or `expectedError` is present. The other is omitted (not `null`).

---

## Table of Contents

- [Common Types](#common-types)
- [Read Spec](#read-spec)
- [Snapshot Spec](#snapshot-spec)
- [CDF Spec](#cdf-spec)
- [Domain Metadata Spec](#domain-metadata-spec)
- [AppTxn Spec](#apptxn-spec)
- [table_info.json](#table_infojson)
- [Expected Data Layout](#expected-data-layout)

---

## Common Types

### SpecError

Every error spec uses this shape:

| Field | Type | Description |
|-------|------|-------------|
| `errorCode` | `string` | Error class identifier. Spark implementations use `SparkThrowable.getErrorClass`. Non-Spark engines should map to equivalent codes. |
| `errorMessage` | `string` | Human-readable message. Engines need not match this exactly — it is informational. |

```json
{
  "errorCode": "DELTA_TABLE_NOT_FOUND",
  "errorMessage": "Delta table not found at path /tmp/missing"
}
```

The `errorCode` is the primary field for harness assertion. Error messages vary across implementations and should be treated as advisory.

### ProtocolInfo

Appears in snapshot, checkpoint, CRC, and table_info specs:

| Field | Type | Description |
|-------|------|-------------|
| `minReaderVersion` | `int` | Minimum reader protocol version (1–3) |
| `minWriterVersion` | `int` | Minimum writer protocol version (1–7) |
| `readerFeatures` | `string[]?` | Reader features (present only when minReaderVersion ≥ 3). Sorted alphabetically. |
| `writerFeatures` | `string[]?` | Writer features (present only when minWriterVersion ≥ 7). Sorted alphabetically. |

```json
{
  "minReaderVersion": 3,
  "minWriterVersion": 7,
  "readerFeatures": ["deletionVectors"],
  "writerFeatures": ["deletionVectors"]
}
```

---

## Read Spec

**Type:** `"read"`

Tests reading data from a Delta table with optional time travel, predicate pushdown, and column projection.

### Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `type` | `string` | yes | Always `"read"` |
| `version` | `long` | no | Time travel to this version |
| `timestamp` | `string` | no | Time travel to this timestamp (format: `yyyy-MM-dd HH:mm:ss.SSS`) |
| `predicate` | `string` | no | SQL WHERE clause to apply (e.g., `"id > 5"`) |
| `columns` | `string[]` | no | Columns to select (projection pushdown) |
| `expected` | `ReadExpected` | no | Present on success |
| `expectedError` | `SpecError` | no | Present on expected failure |

Only one of `version` or `timestamp` may be set.

### ReadExpected

| Field | Type | Description |
|-------|------|-------------|
| `rowCount` | `long` | Total rows returned |
| `fileCount` | `int` | Number of data files actually scanned |
| `filesSkipped` | `long` | Files skipped via data skipping (stats-based pruning) |

The relationship `fileCount + filesSkipped = total files in table at that version` always holds for unpartitioned tables. For partitioned tables, `filesSkipped` reflects both partition pruning and stats-based skipping.

### Expected Data

When `expected` is present, the directory `expected/<spec_name>/` contains:

- **`expected_data/`** — Parquet files with the exact rows the read should return. Row order is irrelevant; comparison is multiset-based (each row appears the correct number of times). Capped at 5,000,000 rows.
- **`expected_metadata/`** — Parquet file with one column `action` containing the JSON `AddFile` actions for files that were scanned (not skipped). Use this to validate data skipping behavior.

### Examples

**Latest version, no filters:**

```json
{
  "type": "read",
  "expected": {
    "rowCount": 3,
    "fileCount": 1,
    "filesSkipped": 0
  }
}
```

**Time travel by version:**

```json
{
  "type": "read",
  "version": 0,
  "expected": {
    "rowCount": 100,
    "fileCount": 2,
    "filesSkipped": 0
  }
}
```

**Time travel by timestamp:**

```json
{
  "type": "read",
  "timestamp": "2025-01-15 10:30:00.000",
  "expected": {
    "rowCount": 50,
    "fileCount": 1,
    "filesSkipped": 0
  }
}
```

**Predicate pushdown with data skipping:**

```json
{
  "type": "read",
  "predicate": "id > 500",
  "expected": {
    "rowCount": 500,
    "fileCount": 5,
    "filesSkipped": 5
  }
}
```

**Column projection:**

```json
{
  "type": "read",
  "columns": ["id", "name"],
  "expected": {
    "rowCount": 1000,
    "fileCount": 10,
    "filesSkipped": 0
  }
}
```

**Combined: version + predicate + columns:**

```json
{
  "type": "read",
  "version": 2,
  "predicate": "category = 'electronics'",
  "columns": ["id", "category", "price"],
  "expected": {
    "rowCount": 42,
    "fileCount": 3,
    "filesSkipped": 7
  }
}
```

**Error: version does not exist:**

```json
{
  "type": "read",
  "version": 999,
  "expectedError": {
    "errorCode": "DELTA_VERSION_NOT_FOUND",
    "errorMessage": "Cannot find version 999"
  }
}
```

**Error: unsupported protocol:**

```json
{
  "type": "read",
  "expectedError": {
    "errorCode": "DELTA_UNSUPPORTED_FEATURES_FOR_READ",
    "errorMessage": "Required reader features not supported: [futureFeature]"
  }
}
```

**Error: corrupt data file:**

```json
{
  "type": "read",
  "expectedError": {
    "errorCode": "FAILED_READ_FILE",
    "errorMessage": "Failed to read file: part-00000-abc.parquet"
  }
}
```

---

## Snapshot Spec

**Type:** `"snapshot"`

Tests constructing a table snapshot at a given version, validating the protocol and metadata are correctly reconstructed from the log.

### Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `type` | `string` | yes | Always `"snapshot"` |
| `version` | `long` | no | Snapshot at this version (latest if omitted and no timestamp) |
| `timestamp` | `string` | no | Snapshot at this timestamp |
| `expected` | `SnapshotExpected` | no | Present on success |
| `expectedError` | `SpecError` | no | Present on expected failure |

### SnapshotExpected

| Field | Type | Description |
|-------|------|-------------|
| `protocol` | `object` | The protocol action from the Delta log. Contains `minReaderVersion`, `minWriterVersion`, and optionally `readerFeatures`/`writerFeatures`. |
| `metadata` | `object` | The metaData action from the Delta log. Contains `id`, `name`, `format`, `schemaString`, `partitionColumns`, `configuration`, `createdTime`. |

These are the raw Delta protocol and metadata JSON structures — not simplified. Your engine should produce equivalent JSON.

### Examples

**Latest snapshot:**

```json
{
  "type": "snapshot",
  "version": 3,
  "expected": {
    "protocol": {
      "minReaderVersion": 1,
      "minWriterVersion": 2
    },
    "metadata": {
      "id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
      "format": { "provider": "parquet", "options": {} },
      "schemaString": "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}",
      "partitionColumns": [],
      "configuration": {},
      "createdTime": 1705000000000
    }
  }
}
```

**Snapshot with table features (v3/v7 protocol):**

```json
{
  "type": "snapshot",
  "version": 5,
  "expected": {
    "protocol": {
      "minReaderVersion": 3,
      "minWriterVersion": 7,
      "readerFeatures": ["deletionVectors"],
      "writerFeatures": ["deletionVectors"]
    },
    "metadata": {
      "id": "abc123",
      "format": { "provider": "parquet", "options": {} },
      "schemaString": "{\"type\":\"struct\",\"fields\":[...]}",
      "partitionColumns": ["date"],
      "configuration": {
        "delta.enableChangeDataFeed": "true",
        "delta.enableDeletionVectors": "true"
      },
      "createdTime": 1705000000000
    }
  }
}
```

**Snapshot at version 0 (initial commit):**

```json
{
  "type": "snapshot",
  "version": 0,
  "expected": {
    "protocol": {
      "minReaderVersion": 1,
      "minWriterVersion": 2
    },
    "metadata": {
      "id": "...",
      "format": { "provider": "parquet", "options": {} },
      "schemaString": "...",
      "partitionColumns": [],
      "configuration": {},
      "createdTime": 1705000000000
    }
  }
}
```

**Error: unsupported reader feature:**

```json
{
  "type": "snapshot",
  "expectedError": {
    "errorCode": "DELTA_UNSUPPORTED_FEATURES_FOR_READ",
    "errorMessage": "Table requires reader feature 'unknownFeature' which is not supported"
  }
}
```

---

## CDF Spec

**Type:** `"cdf"`

Tests reading Change Data Feed (CDF) from a Delta table with version ranges and optional filtering.

### Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `type` | `string` | yes | Always `"cdf"` |
| `startVersion` | `long` | no* | Start of version range |
| `endVersion` | `long` | no | End of version range (latest if omitted) |
| `startTimestamp` | `string` | no* | Alternative to startVersion (format: `yyyy-MM-dd HH:mm:ss.SSS`) |
| `endTimestamp` | `string` | no | Alternative to endVersion |
| `predicate` | `string` | no | SQL WHERE clause to filter changes |
| `columns` | `string[]` | no | Columns to select (projection pushdown) |
| `expected` | `CdfExpected` | no | Present on success |
| `expectedError` | `SpecError` | no | Present on expected failure |

*Either `startVersion` or `startTimestamp` is required. Cannot specify both version and timestamp bounds.

### CdfExpected

| Field | Type | Description |
|-------|------|-------------|
| `rowCount` | `long` | Total change rows returned |

### Expected Data

When `expected` is present, the directory `expected/<spec_name>/` contains:

- **`expected_data/`** — Parquet files with the exact change rows. Includes metadata columns: `_change_type` (insert/update_preimage/update_postimage/delete), `_commit_version`, `_commit_timestamp`.

### Examples

**Basic CDF read:**

```json
{
  "type": "cdf",
  "startVersion": 0,
  "endVersion": 3,
  "expected": {
    "rowCount": 10
  }
}
```

**CDF with single version:**

```json
{
  "type": "cdf",
  "startVersion": 2,
  "endVersion": 2,
  "expected": {
    "rowCount": 3
  }
}
```

**CDF with timestamp bounds:**

```json
{
  "type": "cdf",
  "startTimestamp": "2025-01-15 10:00:00.000",
  "endTimestamp": "2025-01-15 12:00:00.000",
  "expected": {
    "rowCount": 50
  }
}
```

**CDF with predicate:**

```json
{
  "type": "cdf",
  "startVersion": 0,
  "predicate": "id > 100",
  "expected": {
    "rowCount": 25
  }
}
```

**Error: CDF not enabled:**

```json
{
  "type": "cdf",
  "startVersion": 0,
  "expectedError": {
    "errorCode": "DELTA_CHANGE_DATA_FEED_DISABLED",
    "errorMessage": "Change Data Feed is not enabled for this table"
  }
}
```

---

## Domain Metadata Spec

**Type:** `"domain_metadata"`

Tests reading domain metadata actions from the Delta transaction log.

### Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `type` | `string` | yes | Always `"domain_metadata"` |
| `version` | `long` | no | Check at this version (latest if omitted) |
| `expected` | `DomainMetadataExpected` | no | Present on success |
| `expectedError` | `SpecError` | no | Present on expected failure |

### DomainMetadataExpected

| Field | Type | Description |
|-------|------|-------------|
| `domain` | `string` | Domain name to check |
| `configuration` | `string` | Expected configuration string (often JSON) |
| `removed` | `boolean` | If true, domain should be absent; if false, domain should exist |

### Examples

**Domain present:**

```json
{
  "type": "domain_metadata",
  "version": 2,
  "expected": {
    "domain": "testDomain1",
    "configuration": "",
    "removed": false
  }
}
```

**Domain with JSON configuration:**

```json
{
  "type": "domain_metadata",
  "expected": {
    "domain": "myFeature",
    "configuration": "{\"key\":\"value\",\"enabled\":true}",
    "removed": false
  }
}
```

**Domain removed:**

```json
{
  "type": "domain_metadata",
  "expected": {
    "domain": "oldDomain",
    "configuration": "",
    "removed": true
  }
}
```

---

## AppTxn Spec

**Type:** `"appTxn"`

Tests reading SetTransaction actions used for application-level idempotency (e.g., streaming writers).

### Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `type` | `string` | yes | Always `"appTxn"` |
| `version` | `long` | no | Check at this version (latest if omitted) |
| `expected` | `TxnExpected` | no | Present on success |
| `expectedError` | `SpecError` | no | Present on expected failure |

### TxnExpected

| Field | Type | Description |
|-------|------|-------------|
| `appId` | `string` | Application identifier |
| `txnVersion` | `long` | Expected transaction version for this appId |

### Examples

**Basic SetTransaction:**

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

**SetTransaction at latest version:**

```json
{
  "type": "appTxn",
  "expected": {
    "appId": "streaming-writer-1",
    "txnVersion": 1000
  }
}
```

**SetTransaction with large version:**

```json
{
  "type": "appTxn",
  "expected": {
    "appId": "app-future",
    "txnVersion": 999999
  }
}
```

---

## table_info.json

Written to the workload output root. Provides metadata about the generated table for discovery and filtering.

### Fields

| Field | Type | Description |
|-------|------|-------------|
| `name` | `string` | Test identifier |
| `description` | `string` | Human-readable description |
| `schema` | `object` | Table schema (parsed JSON, not a string) |
| `protocol` | `ProtocolInfo` | Protocol version and features |
| `logInfo` | `LogInfo` | Log statistics |
| `properties` | `map` | Table properties (e.g., `delta.enableChangeDataFeed`) |
| `dataLayout` | `DataLayoutInfo` | Partition and clustering info |
| `tags` | `string[]?` | Feature tags for filtering |

### LogInfo

| Field | Type | Description |
|-------|------|-------------|
| `numAddFiles` | `long` | Active data files |
| `numRemoveFiles` | `long` | Tombstoned files |
| `sizeInBytes` | `long` | Total active data size |
| `numCommits` | `int` | Number of JSON commit files |
| `numActions` | `long` | Total actions across all commits |
| `lastCheckpointVersion` | `long` | Version of last checkpoint (-1 if none) |
| `lastCrcVersion` | `long` | Version of last CRC file (-1 if none) |
| `numCheckpointFiles` | `int` | Number of checkpoint files |

### DataLayoutInfo

| Field | Type | Description |
|-------|------|-------------|
| `numClusteringColumns` | `int` | Clustering columns (always 0 in OSS Delta) |
| `numPartitionColumns` | `int` | Partition columns |
| `numDistinctPartitions` | `long` | Distinct partition values |

### Example

```json
{
  "name": "basic_read_tbl",
  "description": "basic_read — tbl",
  "schema": {
    "type": "struct",
    "fields": [
      { "name": "id", "type": "integer", "nullable": false, "metadata": {} },
      { "name": "name", "type": "string", "nullable": true, "metadata": {} }
    ]
  },
  "protocol": {
    "minReaderVersion": 1,
    "minWriterVersion": 2
  },
  "logInfo": {
    "numAddFiles": 1,
    "numRemoveFiles": 0,
    "sizeInBytes": 534,
    "numCommits": 2,
    "numActions": 4,
    "lastCheckpointVersion": -1,
    "lastCrcVersion": 1,
    "numCheckpointFiles": 0
  },
  "properties": {},
  "dataLayout": {
    "numClusteringColumns": 0,
    "numPartitionColumns": 0,
    "numDistinctPartitions": 0
  },
  "tags": ["basic", "read"]
}
```

On failure, a minimal sentinel is written:

```json
{
  "name": "failed_test",
  "description": "test that failed during generation",
  "error": "Metadata scan failed: table not found"
}
```

---

## Expected Data Layout

Each workload output directory has this structure:

```
<test_name>/
├── delta/                          # The Delta table itself
│   └── _delta_log/
│       ├── 00000000000000000000.json
│       ├── 00000000000000000001.json
│       └── ...
├── specs/                          # Spec JSON files
│   ├── <test>_read.json
│   ├── <test>_read_v0.json
│   ├── <test>_snapshot.json
│   ├── <test>_read_v0.json
│   └── ...
├── expected/                       # Expected data per spec
│   ├── <test>_read/
│   │   ├── expected_data/          # Parquet: expected rows
│   │   └── expected_metadata/      # Parquet: AddFile actions scanned
│   ├── <test>_read_v0/
│   │   ├── expected_data/
│   │   └── expected_metadata/
│   └── <test>_read_v0/
│       ├── expected_data/
│       └── expected_metadata/
├── table_info.json                 # Table metadata
└── repro/
    └── generate.scala              # Script to reproduce this workload
```

### Naming Conventions

Spec files and expected directories share names derived from the test and spec parameters:

| API Call | Spec File Name |
|----------|---------------|
| `read(t)` | `<test>_read.json` |
| `read(t, version=0)` | `<test>_read_v0.json` |
| `read(t, predicate="id > 5")` | `<test>_read_id_gt_5.json` |
| `read(t, columns=Seq("id"))` | `<test>_read_cols_id.json` |
| `snapshot(t)` | `<test>_snapshot.json` |
| `snapshot(t, version=2)` | `<test>_snapshot_v2.json` |
| `cdf(t, startVersion=0)` | `<test>_cdf_v0.json` |
| `cdf(t, startVersion=1, endVersion=3)` | `<test>_cdf_v1_to_v3.json` |
| `domainMetadata(t, domain="myDomain", ...)` | `<test>_dm_myDomain.json` |
| `appTxn(t, appId="myapp", ...)` | `<test>_txn_myapp.json` |
