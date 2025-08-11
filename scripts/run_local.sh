#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  run_local.sh <pipeline.yaml> <connection.yaml> [extra spark args]
  run_local.sh --config <unified.yaml> [extra spark args]

Environment:
  TRINO_JDBC_JAR   Path to Trino/Starburst JDBC jar (required)
  MASTER           Spark master (default: local[4])
USAGE
}

# Validate spark-submit
if ! command -v spark-submit >/dev/null 2>&1; then
  echo "Error: spark-submit not found in PATH" >&2
  exit 1
fi

# Parse arguments
PIPELINE=""
CONNECTION=""
UNIFIED_CONFIG=""
if [[ $# -lt 1 ]]; then
  usage; exit 1
fi
if [[ "$1" == "--config" ]]; then
  if [[ $# -lt 2 ]]; then usage; exit 1; fi
  UNIFIED_CONFIG="$2"; shift 2
else
  if [[ $# -lt 2 ]]; then usage; exit 1; fi
  PIPELINE="$1"; CONNECTION="$2"; shift 2
fi

# Remaining args are extra spark args
EXTRA_SPARK_ARGS=("$@")

# JDBC jar checks
JAR=${TRINO_JDBC_JAR:-}
if [[ -z "${JAR}" ]]; then
  echo "Error: TRINO_JDBC_JAR is not set" >&2
  exit 1
fi
if [[ ! -f "${JAR}" ]]; then
  echo "Error: JDBC jar not found at '${JAR}'" >&2
  exit 1
fi

# Use venv Python for PySpark if available
if [[ -x ".venv/bin/python" ]]; then
  export PYSPARK_PYTHON="$(pwd)/.venv/bin/python"
fi

MASTER_VAL=${MASTER:-local[4]}

# Build CLI args
CLI_ARGS=()
if [[ -n "${UNIFIED_CONFIG}" ]]; then
  CLI_ARGS+=("--config" "${UNIFIED_CONFIG}")
else
  CLI_ARGS+=("--pipeline-config" "${PIPELINE}" "--connection-config" "${CONNECTION}")
fi

spark-submit \
  --master "${MASTER_VAL}" \
  --jars "${JAR}" \
  --driver-class-path "${JAR}" \
  --conf spark.executor.memory=4g \
  --conf spark.driver.memory=2g \
  "${EXTRA_SPARK_ARGS[@]}" \
  -m transform_service.cli "${CLI_ARGS[@]}"
