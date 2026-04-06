#!/usr/bin/env bash
#
# Generate Delta acceptance test workloads on Databricks Runtime (DBR).
#
# Requires:
#   - Running on a DBR cluster or devbox with spark-shell available
#   - The workload-generator assembly jar (built with sbt assembly)
#   - The DBR shim jar (built with cd shim && sbt package)
#
# Usage:
#   ./generate-workload-dbr.sh tables/reads.scala
#   ./generate-workload-dbr.sh tables/ --output-dir /tmp/workloads
#   ./generate-workload-dbr.sh --interactive
#
# Options:
#   --output-dir DIR   Output directory (default: /tmp/workloads)
#   --force            Regenerate even if output exists
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
JAR_PATH="$PROJECT_DIR/target/scala-2.13/delta-workload-generator-assembly-0.1.0.jar"
SHIM_JAR="$PROJECT_DIR/shim/target/scala-2.13/workload-generator-dbr-shim_2.13-0.1.0.jar"

if [[ ! -f "$JAR_PATH" ]]; then
  echo "ERROR: Assembly jar not found. Build: cd $PROJECT_DIR && sbt assembly"
  exit 1
fi

if [[ ! -f "$SHIM_JAR" ]]; then
  echo "ERROR: DBR shim jar not found. Build: cd $PROJECT_DIR/shim && sbt package"
  exit 1
fi

# Find spark-shell: prefer SPARK_HOME, then PATH
SPARK_SHELL="${SPARK_HOME:-}/bin/spark-shell"
if [[ ! -x "$SPARK_SHELL" ]]; then
  SPARK_SHELL="$(which spark-shell 2>/dev/null || true)"
fi
if [[ -z "$SPARK_SHELL" || ! -x "$SPARK_SHELL" ]]; then
  echo "ERROR: spark-shell not found. Set SPARK_HOME or add spark-shell to PATH."
  exit 1
fi

# DBR already has Delta on the classpath — no --packages needed.
# Shim jar must come BEFORE assembly jar so the type aliases are loaded first.
SPARK_CONF=(
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension"
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
  --jars "$SHIM_JAR,$JAR_PATH"
)

if [[ "${1:-}" == "--interactive" ]]; then
  echo "Launching spark-shell with WorkloadGenerator (DBR)..."
  echo "  import io.delta.workload._"
  echo ""
  exec "$SPARK_SHELL" "${SPARK_CONF[@]}"
fi

INPUT="${1:?Usage: $0 <script.scala|dir/> [--output-dir DIR] [--force]}"
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

# Resolve input: single file or directory of .scala files
if [[ -d "$INPUT" ]]; then
  mapfile -t SCRIPTS < <(find "$INPUT" -name '*.scala' -type f | sort)
  echo "Running ${#SCRIPTS[@]} suites from $INPUT"
else
  SCRIPTS=("$INPUT")
fi

echo "Output: $OUTPUT_DIR"
echo ""

if [[ ${#SCRIPTS[@]} -eq 1 ]]; then
  {
    echo 'import io.delta.workload._'
    cat "${SCRIPTS[0]}"
  } | "$SPARK_SHELL" "${SPARK_CONF[@]}"
else
  for script in "${SCRIPTS[@]}"; do
    echo "--- Running $(basename "$script") ---"
    {
      echo 'import io.delta.workload._'
      cat "$script"
    } | "$SPARK_SHELL" "${SPARK_CONF[@]}" || true
  done
fi
