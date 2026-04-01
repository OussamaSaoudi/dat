#!/usr/bin/env bash
#
# Generate Delta acceptance test workloads.
#
# Usage:
#   ./generate-workload.sh <script.scala> [--output-dir DIR]
#   ./generate-workload.sh --interactive
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
JAR_PATH="$PROJECT_DIR/target/scala-2.13/delta-workload-generator-assembly-0.1.0.jar"

# Auto-build if needed
if [[ ! -f "$JAR_PATH" ]]; then
  echo "Building workload generator jar..."
  (cd "$PROJECT_DIR" && sbt -batch assembly)
elif [[ -n "$(find "$PROJECT_DIR/src" -name '*.scala' -newer "$JAR_PATH" 2>/dev/null | head -1)" ]]; then
  echo "Sources changed, rebuilding..."
  (cd "$PROJECT_DIR" && sbt -batch assembly)
fi

if [[ ! -f "$JAR_PATH" ]]; then
  echo "ERROR: Assembly jar not found. Run: cd $PROJECT_DIR && sbt assembly"
  exit 1
fi

SPARK_SHELL="${SPARK_HOME:-}/bin/spark-shell"
if [[ ! -x "$SPARK_SHELL" ]]; then
  SPARK_SHELL="$(which spark-shell 2>/dev/null || true)"
fi
if [[ -z "$SPARK_SHELL" || ! -x "$SPARK_SHELL" ]]; then
  echo "ERROR: spark-shell not found. Set SPARK_HOME or add spark-shell to PATH."
  exit 1
fi

SPARK_CONF=(
  --packages "io.delta:delta-spark_2.13:3.3.2"
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension"
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
  --jars "$JAR_PATH"
)

if [[ "${1:-}" == "--interactive" ]]; then
  echo "Launching spark-shell with WorkloadGenerator..."
  echo "  import io.delta.workload.WorkloadGenerator._"
  echo ""
  exec "$SPARK_SHELL" "${SPARK_CONF[@]}"
fi

SCALA_SCRIPT="${1:?Usage: $0 <script.scala> [--output-dir DIR] | --interactive}"
shift

OUTPUT_DIR="/tmp/workloads"
FORCE=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --output-dir) OUTPUT_DIR="$2"; shift 2;;
    --force) FORCE="true"; shift;;
    *) echo "Unknown option: $1"; exit 1;;
  esac
done

export WORKLOAD_OUTPUT_DIR="$OUTPUT_DIR"
export WORKLOAD_FORCE="${FORCE:-false}"
export WORKLOAD_SOURCE_SCRIPT="$(cd "$(dirname "$SCALA_SCRIPT")" && pwd)/$(basename "$SCALA_SCRIPT")"

echo "Running: $SCALA_SCRIPT"
echo "Output:  $OUTPUT_DIR"
echo ""

exec "$SPARK_SHELL" "${SPARK_CONF[@]}" -i "$SCALA_SCRIPT"
