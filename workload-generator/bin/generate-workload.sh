#!/usr/bin/env bash
#
# Generate Delta acceptance test workloads.
#
# Usage:
#   ./generate-workload.sh tables/reads.scala           # Run one suite
#   ./generate-workload.sh tables/                      # Run all suites in directory
#   ./generate-workload.sh --interactive                 # Launch spark-shell
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

# Detect environment: DBR vs OSS
if [[ -n "${DATABRICKS_RUNTIME_VERSION:-}" ]] || [[ -f "/databricks/spark/conf/spark-defaults.conf" ]]; then
  ENV="dbr"
else
  ENV="oss"
fi

# Auto-build main jar if needed
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

# Auto-build shim on DBR if needed
if [[ "$ENV" == "dbr" && ! -f "$SHIM_JAR" ]]; then
  echo "Building DBR shim jar..."
  (cd "$PROJECT_DIR/shim" && sbt -batch package) || {
    echo "WARN: Could not build shim. DBR may fail to resolve Delta types."
  }
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

# Configure classpath based on environment
if [[ "$ENV" == "dbr" ]]; then
  echo "Detected: Databricks Runtime"
  JARS="$JAR_PATH"
  [[ -f "$SHIM_JAR" ]] && JARS="$JAR_PATH,$SHIM_JAR"
  SPARK_CONF=(
    --jars "$JARS"
  )
else
  echo "Detected: OSS Spark"
  DELTA_SPARK_JAR="$(find ~/.cache/coursier -name 'delta-spark_2.13-*.jar' -path '*/4.1.0/*' 2>/dev/null | head -1)"
  DELTA_STORAGE_JAR="$(find ~/.cache/coursier -name 'delta-storage-*.jar' -path '*/4.1.0/*' 2>/dev/null | head -1)"
  if [[ -z "$DELTA_SPARK_JAR" || -z "$DELTA_STORAGE_JAR" ]]; then
    echo "ERROR: Delta jars not found in coursier cache. Run: cd $PROJECT_DIR && sbt compile"
    exit 1
  fi
  SPARK_CONF=(
    --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension"
    --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
    --jars "$JAR_PATH,$DELTA_SPARK_JAR,$DELTA_STORAGE_JAR"
  )
fi

if [[ "${1:-}" == "--interactive" ]]; then
  echo "Launching spark-shell with WorkloadGenerator..."
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
  # Single file: pipe directly
  {
    echo 'import io.delta.workload._'
    cat "${SCRIPTS[0]}"
  } | "$SPARK_SHELL" "${SPARK_CONF[@]}"
else
  # Multiple files: run each suite individually
  for script in "${SCRIPTS[@]}"; do
    echo "--- Running $(basename "$script") ---"
    {
      echo 'import io.delta.workload._'
      cat "$script"
    } | "$SPARK_SHELL" "${SPARK_CONF[@]}" || true
  done
fi
