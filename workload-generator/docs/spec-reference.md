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
- [Write Spec](#write-spec)
- [Snapshot Spec](#snapshot-spec)
- [CDF Spec (Change Data Feed)](#cdf-spec-change-data-feed)
- [Domain Metadata Spec](#domain-metadata-spec)
- [AppTxn Spec (Application Transaction)](#apptxn-spec-application-transaction)
- [Checkpoint Spec](#checkpoint-spec)
- [CRC Spec (Checksum)](#crc-spec-checksum)
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
  "writerFeatures": ["deletionVectors", "domainMetadata"]
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

## Write Spec

**Type:** `"write"`

Tests Delta writer implementations by providing a sequence of write operations to replay. Unlike read specs (which verify reading an existing table), write specs describe *how to construct* a table from scratch.

A write spec is a portable, implementation-agnostic recipe: it specifies what operations to perform (create table, insert, delete, update, etc.) declaratively, so any conforming Delta writer can interpret and execute it. After replaying all commits, the resulting table is compared against expected data.

### Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `type` | `string` | yes | Always `"write"` |
| `commits` | `WriteCommit[]` | yes | Ordered list of commits to replay |

### WriteCommit

Each commit represents a single Delta transaction. There are two categories: high-level operations (SQL semantics) and low-level operations (raw Delta actions).

#### High-Level Operations

These map to SQL-like operations. The writer should translate them to appropriate Delta actions.

| Operation | Fields | Description |
|-----------|--------|-------------|
| `create_table` | `schema`, `partitionColumns?`, `properties?`, `dataFiles?` | Create a new table with the given schema |
| `replace_table` | `schema`, `partitionColumns?`, `properties?`, `dataFiles?` | Replace table with new schema |
| `insert` | `dataFiles?` | Append rows from data files |
| `update` | `predicate`, `set` | Update rows matching predicate |
| `delete` | `predicate` | Delete rows matching predicate |
| `truncate` | — | Remove all rows |
| `evolve_schema` | `addColumns?`, `renameColumns?`, `dropColumns?` | Modify table schema |
| `update_properties` | `set?`, `remove?` | Modify table properties |
| `restore` | `version` | Restore table to previous version |

#### Low-Level Operation

| Operation | Fields | Description |
|-----------|--------|-------------|
| `commit` | `schema?`, `tableProperties?`, `txn?`, `addFiles?`, `removeFiles?`, `addDomainMetadata?`, `removeDomainMetadata?` | Directly specify Delta actions |

### WriteCommit Fields

| Field | Type | Description |
|-------|------|-------------|
| `operation` | `string` | Operation type (see above) |
| `schema` | `object` | Table schema in Delta JSON format |
| `partitionColumns` | `string[]` | Partition column names |
| `properties` | `map` | Table properties (e.g., `delta.enableDeletionVectors`) |
| `dataFiles` | `string[]` | Relative paths to Parquet data files in `data/` directory |
| `predicate` | `string` | SQL WHERE clause for update/delete |
| `set` | `map` | Column assignments for update (column → expression) or properties to set |
| `remove` | `string[]` | Property names to remove |
| `addColumns` | `object[]` | Columns to add (each with `name`, `type`, `nullable`) |
| `renameColumns` | `map` | Column renames (old name → new name) |
| `dropColumns` | `string[]` | Column names to drop |
| `version` | `long` | Target version for restore |
| `tableProperties` | `map` | Properties for low-level commit |
| `txn` | `AppTxn` | Application transaction for idempotent writes |
| `addFiles` | `AddFileAction[]` | Files to add (low-level) |
| `removeFiles` | `RemoveFileAction[]` | Files to remove (low-level) |
| `addDomainMetadata` | `DomainMetadata[]` | Domain metadata to add |
| `removeDomainMetadata` | `string[]` | Domain names to remove |

### AddFileAction (Low-Level)

| Field | Type | Description |
|-------|------|-------------|
| `dataFile` | `string` | Relative path to Parquet file |
| `partitionValues` | `map?` | Partition values for this file |
| `dataChange` | `boolean?` | Whether this is a data change (default true) |
| `deletionVector` | `DeletionVector?` | Deletion vector descriptor |

### RemoveFileAction (Low-Level)

| Field | Type | Description |
|-------|------|-------------|
| `path` | `string` | Path of file to remove |
| `dataChange` | `boolean?` | Whether this is a data change (default true) |

### Expected Data

Write specs include expected data in the same format as read specs:

- **`expected/latest/`** — Expected table state after replaying all commits
  - `table_content/` — Parquet files with expected rows
  - `table_version_metadata.json` — Protocol and metadata
- **`expected/v{N}/`** — Expected state at intermediate versions (optional)

### Directory Structure

```
<test_name>/
├── write_spec.json              # The write spec
├── data/                        # Data files referenced by commits
│   ├── commit_0/
│   │   └── part-0000-xxx.parquet
│   ├── commit_1/
│   │   └── part-0000-yyy.parquet
│   └── ...
├── expected/
│   ├── latest/
│   │   ├── table_content/
│   │   └── table_version_metadata.json
│   └── v1/
│       ├── table_content/
│       └── table_version_metadata.json
└── test_case_info.json
```

### Examples

**Create table and insert:**

```json
{
  "type": "write",
  "commits": [
    {
      "operation": "create_table",
      "schema": {
        "type": "struct",
        "fields": [
          { "name": "id", "type": "integer", "nullable": false, "metadata": {} },
          { "name": "name", "type": "string", "nullable": true, "metadata": {} }
        ]
      },
      "properties": {
        "delta.enableDeletionVectors": "true"
      }
    },
    {
      "operation": "insert",
      "dataFiles": ["data/commit_1/part-0000-abc.parquet"]
    }
  ]
}
```

**Delete with predicate:**

```json
{
  "type": "write",
  "commits": [
    {
      "operation": "create_table",
      "schema": { ... }
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

**Update with SET:**

```json
{
  "type": "write",
  "commits": [
    {
      "operation": "create_table",
      "schema": { ... }
    },
    {
      "operation": "insert",
      "dataFiles": ["data/commit_1/part-0000-abc.parquet"]
    },
    {
      "operation": "update",
      "predicate": "status = 'pending'",
      "set": {
        "status": "'active'",
        "updated_at": "current_timestamp()"
      }
    }
  ]
}
```

**Schema evolution (add column):**

```json
{
  "type": "write",
  "commits": [
    {
      "operation": "create_table",
      "schema": {
        "type": "struct",
        "fields": [
          { "name": "id", "type": "integer", "nullable": false, "metadata": {} }
        ]
      }
    },
    {
      "operation": "insert",
      "dataFiles": ["data/commit_1/part-0000-abc.parquet"]
    },
    {
      "operation": "evolve_schema",
      "addColumns": [
        { "name": "email", "type": "string", "nullable": true }
      ]
    },
    {
      "operation": "insert",
      "dataFiles": ["data/commit_3/part-0000-def.parquet"]
    }
  ]
}
```

**Partitioned table:**

```json
{
  "type": "write",
  "commits": [
    {
      "operation": "create_table",
      "schema": {
        "type": "struct",
        "fields": [
          { "name": "id", "type": "integer", "nullable": false, "metadata": {} },
          { "name": "region", "type": "string", "nullable": false, "metadata": {} },
          { "name": "value", "type": "double", "nullable": true, "metadata": {} }
        ]
      },
      "partitionColumns": ["region"]
    },
    {
      "operation": "insert",
      "dataFiles": [
        "data/commit_1/region=east/part-0000-abc.parquet",
        "data/commit_1/region=west/part-0000-def.parquet"
      ]
    }
  ]
}
```

**Low-level commit with domain metadata:**

```json
{
  "type": "write",
  "commits": [
    {
      "operation": "create_table",
      "schema": { ... },
      "properties": {
        "delta.feature.domainMetadata": "enabled"
      }
    },
    {
      "operation": "commit",
      "addFiles": [
        {
          "dataFile": "data/commit_1/part-0000-abc.parquet",
          "partitionValues": {},
          "dataChange": true
        }
      ],
      "addDomainMetadata": [
        {
          "domain": "myApp.config",
          "configuration": "{\"version\": 1}"
        }
      ]
    }
  ]
}
```

**Low-level commit with application transaction:**

```json
{
  "type": "write",
  "commits": [
    {
      "operation": "create_table",
      "schema": { ... }
    },
    {
      "operation": "commit",
      "txn": {
        "appId": "streaming-job-1",
        "version": 42
      },
      "addFiles": [
        {
          "dataFile": "data/commit_1/part-0000-abc.parquet"
        }
      ]
    }
  ]
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
      "writerFeatures": ["deletionVectors", "domainMetadata"]
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

## CDF Spec (Change Data Feed)

**Type:** `"cdf"`

Tests reading the change data feed between version or timestamp bounds. Requires the table to have `delta.enableChangeDataFeed = true`.

### Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `type` | `string` | yes | Always `"cdf"` |
| `startVersion` | `long` | no* | Starting version (inclusive) |
| `endVersion` | `long` | no | Ending version (inclusive). Defaults to latest. |
| `startTimestamp` | `string` | no* | Starting timestamp |
| `endTimestamp` | `string` | no | Ending timestamp |
| `predicate` | `string` | no | SQL filter on change rows |
| `columns` | `string[]` | no | Column projection |
| `expected` | `CdfExpected` | no | Present on success |
| `expectedError` | `SpecError` | no | Present on expected failure |

*Exactly one of `startVersion` or `startTimestamp` is required. Cannot mix version and timestamp bounds.

### CdfExpected

| Field | Type | Description |
|-------|------|-------------|
| `rowCount` | `long` | Total change rows returned |

### Expected Data

When `expected` is present: `expected/<spec_name>/expected_data/` contains Parquet files with all change rows. These include the CDF system columns:

| Column | Type | Description |
|--------|------|-------------|
| `_change_type` | `string` | One of: `insert`, `update_preimage`, `update_postimage`, `delete` |
| `_commit_version` | `long` | Commit version that produced this change |
| `_commit_timestamp` | `timestamp` | Commit timestamp |

### Examples

**Version range:**

```json
{
  "type": "cdf",
  "startVersion": 1,
  "endVersion": 3,
  "expected": {
    "rowCount": 150
  }
}
```

**Start version only (reads to latest):**

```json
{
  "type": "cdf",
  "startVersion": 2,
  "expected": {
    "rowCount": 75
  }
}
```

**With predicate filter:**

```json
{
  "type": "cdf",
  "startVersion": 1,
  "predicate": "id > 100",
  "expected": {
    "rowCount": 30
  }
}
```

**With column projection:**

```json
{
  "type": "cdf",
  "startVersion": 0,
  "endVersion": 5,
  "columns": ["id", "status", "_change_type", "_commit_version"],
  "expected": {
    "rowCount": 200
  }
}
```

**Error: CDF not enabled on table:**

```json
{
  "type": "cdf",
  "startVersion": 0,
  "expectedError": {
    "errorCode": "DELTA_CHANGE_DATA_CAPTURE_NOT_ENABLED",
    "errorMessage": "Change Data Feed is not enabled for this table"
  }
}
```

---

## Domain Metadata Spec

**Type:** `"domain_metadata"`

Tests reading domain metadata entries from the Delta log. Domain metadata is a protocol feature (writer feature `domainMetadata`) that allows applications to attach arbitrary key-value metadata to a table's log.

### Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `type` | `string` | yes | Always `"domain_metadata"` |
| `version` | `long` | no | Read domain metadata at this version (latest if omitted) |
| `expected` | `DomainMetadataExpected` | yes | The expected domain metadata entry |

Domain metadata specs never have `expectedError` — invalid states are tested via read/snapshot error specs instead.

### DomainMetadataExpected

| Field | Type | Description |
|-------|------|-------------|
| `domain` | `string` | Domain name (e.g., `"delta.rowTracking"`) |
| `configuration` | `string` | Domain configuration JSON string |
| `removed` | `boolean` | Whether this domain was removed at this version |

### Examples

**Basic domain metadata:**

```json
{
  "type": "domain_metadata",
  "expected": {
    "domain": "myApp.featureFlags",
    "configuration": "{\"enabled\":true,\"version\":2}",
    "removed": false
  }
}
```

**Domain metadata at specific version:**

```json
{
  "type": "domain_metadata",
  "version": 3,
  "expected": {
    "domain": "delta.rowTracking",
    "configuration": "{}",
    "removed": false
  }
}
```

**Removed domain:**

```json
{
  "type": "domain_metadata",
  "expected": {
    "domain": "myApp.deprecated",
    "configuration": "{}",
    "removed": true
  }
}
```

---

## AppTxn Spec (Application Transaction)

**Type:** `"appTxn"`

Tests reading application transaction (`SetTransaction`) entries from the Delta log. These are used for idempotent writes — an application writes its transaction ID so it can detect duplicate commits.

### Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `type` | `string` | yes | Always `"appTxn"` |
| `version` | `long` | no | Read txn state at this version (latest if omitted) |
| `expected` | `TxnExpected` | yes | The expected txn entry |

### TxnExpected

| Field | Type | Description |
|-------|------|-------------|
| `appId` | `string` | Application identifier |
| `txnVersion` | `long` | Transaction version for this application |

### Examples

**Basic appTxn:**

```json
{
  "type": "appTxn",
  "expected": {
    "appId": "my-streaming-app",
    "txnVersion": 42
  }
}
```

**AppTxn at specific version:**

```json
{
  "type": "appTxn",
  "version": 5,
  "expected": {
    "appId": "batch-etl-job",
    "txnVersion": 100
  }
}
```

**Multiple apps (separate spec per app):**

```json
{
  "type": "appTxn",
  "expected": {
    "appId": "app-A",
    "txnVersion": 10
  }
}
```

---

## Checkpoint Spec

**Type:** `"checkpoint"`

Tests reading and validating Delta checkpoint files. Checkpoints are periodic snapshots of the table state that allow readers to skip replaying the entire log history.

### Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `type` | `string` | yes | Always `"checkpoint"` |
| `version` | `long` | yes | Checkpoint version to validate |
| `expected` | `CheckpointExpected` | yes | Expected checkpoint state |

Checkpoint specs never have `expectedError` — invalid checkpoint scenarios are tested via read/snapshot error specs.

### CheckpointExpected

| Field | Type | Description |
|-------|------|-------------|
| `protocol` | `ProtocolInfo` | Protocol from the checkpoint |
| `metadata` | `object` | Metadata action from the checkpoint |
| `txn` | `TxnAction[]?` | Application transactions (SetTransaction) in the checkpoint |
| `domainMetadata` | `DomainMetadataEntry[]?` | Domain metadata entries in the checkpoint |

### DomainMetadataEntry

| Field | Type | Description |
|-------|------|-------------|
| `domain` | `string` | Domain identifier |
| `configuration` | `string?` | Domain configuration JSON |

### Expected Data

When a checkpoint spec is captured, the directory `expected/<spec_name>/` contains:

- **`expected_checkpoint/`** — Raw checkpoint files copied from `_delta_log/` (e.g., `00000000000000000010.checkpoint.parquet`)
- **`expected_data/`** — Parquet files with expected rows at the checkpoint version
- **`expected_metadata/`** — Parquet file with `AddFile` action JSON for files at that version

### Examples

**Basic checkpoint:**

```json
{
  "type": "checkpoint",
  "version": 10,
  "expected": {
    "protocol": {
      "minReaderVersion": 1,
      "minWriterVersion": 2
    },
    "metadata": {
      "id": "abc123",
      "format": { "provider": "parquet", "options": {} },
      "schemaString": "{\"type\":\"struct\",\"fields\":[...]}",
      "partitionColumns": [],
      "configuration": {},
      "createdTime": 1705000000000
    }
  }
}
```

**Checkpoint with table features:**

```json
{
  "type": "checkpoint",
  "version": 20,
  "expected": {
    "protocol": {
      "minReaderVersion": 3,
      "minWriterVersion": 7,
      "readerFeatures": ["deletionVectors"],
      "writerFeatures": ["deletionVectors", "domainMetadata"]
    },
    "metadata": {
      "id": "def456",
      "format": { "provider": "parquet", "options": {} },
      "schemaString": "...",
      "partitionColumns": ["date"],
      "configuration": {
        "delta.enableDeletionVectors": "true"
      },
      "createdTime": 1705000000000
    }
  }
}
```

**Checkpoint with application transactions:**

```json
{
  "type": "checkpoint",
  "version": 15,
  "expected": {
    "protocol": {
      "minReaderVersion": 1,
      "minWriterVersion": 2
    },
    "metadata": { ... },
    "txn": [
      { "appId": "streaming-app-1", "version": 100 },
      { "appId": "batch-job-2", "version": 50 }
    ]
  }
}
```

**Checkpoint with domain metadata:**

```json
{
  "type": "checkpoint",
  "version": 25,
  "expected": {
    "protocol": {
      "minReaderVersion": 3,
      "minWriterVersion": 7,
      "readerFeatures": ["deletionVectors"],
      "writerFeatures": ["deletionVectors", "domainMetadata"]
    },
    "metadata": { ... },
    "domainMetadata": [
      { "domain": "delta.rowTracking", "configuration": "{}" },
      { "domain": "myApp.config", "configuration": "{\"version\":2}" }
    ]
  }
}
```

---

## CRC Spec (Checksum)

**Type:** `"crc"`

Tests reading and validating Delta CRC (checksum) sidecar files. CRC files contain pre-computed table statistics that allow readers to quickly validate table state without scanning all log entries.

### Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `type` | `string` | yes | Always `"crc"` |
| `version` | `long` | yes | Version of the CRC file to validate |
| `expected` | `CrcExpected` | yes | Expected CRC contents |

CRC specs never have `expectedError` — they are only generated when a valid CRC file exists.

### CrcExpected

| Field | Type | Description |
|-------|------|-------------|
| `tableSizeBytes` | `long?` | Total size of active data files |
| `numFiles` | `long?` | Number of active data files (AddFile actions) |
| `numRemoveFiles` | `long?` | Number of tombstoned files (RemoveFile actions) |
| `numTransactions` | `long?` | Number of application transactions |
| `numDomainMetadata` | `long?` | Number of domain metadata entries |
| `protocol` | `ProtocolInfo?` | Protocol at this version |
| `metadata` | `object?` | Metadata at this version |
| `txn` | `TxnAction[]?` | Application transactions |
| `histograms` | `CrcHistograms?` | File size histograms |
| `deletionVectors` | `CrcDeletionVectorStats?` | Deletion vector statistics |
| `inCommitTimestamp` | `long?` | In-commit timestamp (if enabled) |

### CrcHistograms

| Field | Type | Description |
|-------|------|-------------|
| `addFiles` | `object?` | Histogram of AddFile sizes |
| `removeFiles` | `object?` | Histogram of RemoveFile sizes |

### CrcDeletionVectorStats

| Field | Type | Description |
|-------|------|-------------|
| `numDeletionVectors` | `long?` | Total number of deletion vectors |
| `numLogicalDeletionVectorRows` | `long?` | Total rows marked deleted by DVs |
| `deletionVectorSizeInBytes` | `long?` | Total size of deletion vectors |

### Examples

**Basic CRC:**

```json
{
  "type": "crc",
  "version": 5,
  "expected": {
    "tableSizeBytes": 12345,
    "numFiles": 3,
    "numRemoveFiles": 0,
    "protocol": {
      "minReaderVersion": 1,
      "minWriterVersion": 2
    },
    "metadata": {
      "id": "abc123",
      "format": { "provider": "parquet", "options": {} },
      "schemaString": "...",
      "partitionColumns": [],
      "configuration": {},
      "createdTime": 1705000000000
    }
  }
}
```

**CRC with deletion vectors:**

```json
{
  "type": "crc",
  "version": 10,
  "expected": {
    "tableSizeBytes": 50000,
    "numFiles": 5,
    "numRemoveFiles": 2,
    "protocol": {
      "minReaderVersion": 3,
      "minWriterVersion": 7,
      "readerFeatures": ["deletionVectors"],
      "writerFeatures": ["deletionVectors"]
    },
    "metadata": { ... },
    "deletionVectors": {
      "numDeletionVectors": 2,
      "numLogicalDeletionVectorRows": 150,
      "deletionVectorSizeInBytes": 1024
    }
  }
}
```

**CRC with transactions and domain metadata:**

```json
{
  "type": "crc",
  "version": 15,
  "expected": {
    "tableSizeBytes": 100000,
    "numFiles": 10,
    "numRemoveFiles": 3,
    "numTransactions": 2,
    "numDomainMetadata": 1,
    "protocol": { ... },
    "metadata": { ... },
    "txn": [
      { "appId": "app-1", "version": 42 },
      { "appId": "app-2", "version": 17 }
    ]
  }
}
```

**CRC with histograms:**

```json
{
  "type": "crc",
  "version": 20,
  "expected": {
    "tableSizeBytes": 500000,
    "numFiles": 25,
    "numRemoveFiles": 5,
    "protocol": { ... },
    "metadata": { ... },
    "histograms": {
      "addFiles": {
        "sortedBinBoundaries": [0, 1024, 10240, 102400, 1048576],
        "fileCounts": [0, 5, 15, 5, 0],
        "totalBytes": [0, 4096, 122880, 307200, 0]
      }
    }
  }
}
```

**CRC with in-commit timestamp:**

```json
{
  "type": "crc",
  "version": 8,
  "expected": {
    "tableSizeBytes": 25000,
    "numFiles": 4,
    "protocol": { ... },
    "metadata": { ... },
    "inCommitTimestamp": 1705123456789
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
│   ├── <test>_cdf_v1.json
│   └── ...
├── expected/                       # Expected data per spec
│   ├── <test>_read/
│   │   ├── expected_data/          # Parquet: expected rows
│   │   └── expected_metadata/      # Parquet: AddFile actions scanned
│   ├── <test>_read_v0/
│   │   ├── expected_data/
│   │   └── expected_metadata/
│   └── <test>_cdf_v1/
│       └── expected_data/          # Parquet: change rows
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
| `cdf(t, startVersion=1)` | `<test>_cdf_v1.json` |
| `cdf(t, startVersion=1, endVersion=3)` | `<test>_cdf_v1_to_v3.json` |
| `domainMetadata(t, "myDom", ...)` | `<test>_dm_myDom.json` |
| `appTxn(t, "app-1", ...)` | `<test>_txn_app-1.json` |
