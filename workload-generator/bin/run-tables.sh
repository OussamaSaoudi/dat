#!/usr/bin/env bash
#
# Run table definition scripts through sbt console.
#
# Usage:
#   ./bin/run-tables.sh tables/write_basic.scala    # Run one script
#   ./bin/run-tables.sh tables/                     # Run all scripts in directory
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# Require Java 17+
if [[ -z "${JAVA_HOME:-}" ]]; then
  echo "ERROR: JAVA_HOME must be set to Java 17+"
  exit 1
fi

JAVA_VERSION=$(java -version 2>&1 | head -1 | cut -d'"' -f2 | cut -d'.' -f1)
if [[ "$JAVA_VERSION" -lt 17 ]]; then
  echo "ERROR: Java 17+ required, found Java $JAVA_VERSION"
  exit 1
fi

export PATH="$JAVA_HOME/bin:$PATH"
echo "Using Java: $(java -version 2>&1 | head -1)"

INPUT="${1:?Usage: $0 <script.scala|directory/>}"
OUTPUT_DIR="${WORKLOAD_OUTPUT_DIR:-/tmp/workloads}"
FORCE="${WORKLOAD_FORCE:-true}"

# Resolve input: single file or directory
if [[ -d "$INPUT" ]]; then
  mapfile -t SCRIPTS < <(find "$INPUT" -name '*.scala' -type f | sort)
  echo "Running ${#SCRIPTS[@]} scripts from $INPUT"
else
  SCRIPTS=("$INPUT")
fi

echo "Output: $OUTPUT_DIR"
echo ""

cd "$PROJECT_DIR"

# Run all scripts via TableScriptRunner (properly forked with Spark)
WORKLOAD_OUTPUT_DIR="$OUTPUT_DIR" WORKLOAD_FORCE="$FORCE" \
  sbt -Dsbt.log.noformat=true "Test/runMain io.delta.workload.TableScriptRunner ${SCRIPTS[*]}" 2>&1 | \
  grep -v "^\[info\]" | grep -v "^$" || true

echo "=== Done ==="
echo "Output written to: $OUTPUT_DIR"
