# Delta Workload Generator

Generate acceptance test workloads for Delta implementations (e.g., [delta-kernel-rs](https://github.com/delta-io/delta-kernel-rs)) from Apache Spark.

Write a script that creates Delta tables with normal SQL, declare what specs to capture, and the framework generates complete workload directories with expected data, validated row-for-row against Spark.

## Requirements

- Java 17+
- Apache Spark 3.5.x
- Delta Spark 3.3.2 (pulled automatically via `--packages`)
- sbt 1.9+ (for building)

## Quick Start

```bash
cd workload-generator
sbt assembly
./bin/generate-workload.sh tables/simple_read.scala --output-dir /tmp/workloads
```

## Writing a Script

```scala
import io.delta.workload.WorkloadGenerator._

workload("cdf_merge", "MERGE with CDC enabled") { w =>
  w.sql("""CREATE TABLE target (id INT, val STRING) USING delta
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')""")
  w.sql("INSERT INTO target VALUES (1, 'old'), (2, 'old')")
  w.sql("CREATE TABLE source (id INT, val STRING) USING delta")
  w.sql("INSERT INTO source VALUES (2, 'new'), (3, 'new')")
  w.sql("""MERGE INTO target t USING source s ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET val = s.val
    WHEN NOT MATCHED THEN INSERT *""")

  val tgt = w.table("target")
  val src = w.table("source")

  w.read(tgt)
  w.read(tgt, version = 0)
  w.read(tgt, predicate = "id > 1")
  w.read(src)
  w.snapshot(tgt)
  w.cdf(tgt, startVersion = 2)
}

generateAll("/tmp/workloads")
System.exit(0)  // Required for spark-shell scripts
```

## Output

Each `w.table()` call produces a flat top-level directory:

```
cdf_merge_target/
  delta/                                  # Copy of the Delta table
  specs/
    cdf_merge_target_read.json            # Read spec (latest)
    cdf_merge_target_read_v0.json         # Read at version 0
    cdf_merge_target_read_id_gt_1.json    # Filtered read
    cdf_merge_target_snapshot.json        # Snapshot construction
    cdf_merge_target_cdf_v2.json          # CDF spec
  expected/
    cdf_merge_target_read/expected_data/  # Parquet expected rows
    ...
  table_info.json
  test_info.json
  repro/generate.scala

cdf_merge_source/
  delta/
  specs/
  expected/
  ...
```

## API Reference

### Registration

```scala
workload(name: String, description: String, tags: String*)(body: WorkloadContext => Unit)
generateAll(outputDir: String)
generate(name: String, outputDir: String)  // single workload
```

### Inside a workload body

| Method | Description |
|--------|-------------|
| `w.sql(stmt)` | Execute Spark SQL |
| `w.spark` | Direct SparkSession access |
| `w.table("name")` | Get handle to a managed Spark table → `TableHandle` |
| `w.tableFromPath("/path")` | Get handle to a table on disk → `TableHandle` |

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

### Table corruption

```scala
w.mutateTable(t) { tableDir =>
  java.nio.file.Files.delete(tableDir.resolve("some_file.parquet"))
}

w.modifyCommitActions(t, version = 1) { addNode =>
  addNode.put("stats", """{"numRecords":999}""")
}
```

Failed reads are auto-captured as error specs.

### Utilities

```scala
val ts = t.getTimestampForVersion(1)
w.read(t, timestamp = ts)
```

## Validation

Every spec is validated after capture:
- **Read/CDF specs**: Row-for-row comparison using canonical JSON multiset
- **Error specs**: Re-attempted to confirm reproducibility
- **Snapshot specs**: Protocol and metadata deep comparison

## IDE Support

For full LSP/autocomplete, copy `project-template/`:

```bash
cp -r project-template/ my-workloads/
# Open in IntelliJ or VS Code with Metals
```

Scripts also work standalone: `./bin/generate-workload.sh my_script.scala`

## Important Notes

- Scripts run via `spark-shell` must end with `System.exit(0)` (otherwise the REPL hangs)
- Re-running skips existing workloads (incremental). Use `--force` to regenerate all.
- If one workload fails, others still generate. Use `generate("name", dir)` to re-run one.
- Failed reads are auto-captured as error specs (`{"error": {"errorCode": "..."}}`). No special handling needed — just declare the spec and the framework records what happens.

## Examples

Example scripts are in the `tables/` directory:

| File | What it tests |
|------|---------------|
| `simple_read.scala` | Minimal example |
| `core_reads.scala` | Basic reads, predicates, time travel, errors |
| `types_and_basic.scala` | Primitives, nested types, NULLs, edge cases |
| `partitioning_and_skipping.scala` | Partition pruning, data skipping |
| `features_and_dml.scala` | Schema evolution, DVs, CDF, checkpoints, DML |
| `iceberg_compat.scala` | Iceberg compatibility features |
| `dv_delete_basic.scala` | DVs, MERGE, table corruption |
