#!/bin/bash

set -euo pipefail

# Enhanced run_local.sh with performance optimizations
# This script demonstrates the new performance features

echo "🚀 Starting Enhanced Transform Service with Performance Monitoring..."

# Set environment variables for testing
export STARBURST_USER="demo_user"
export STARBURST_PASSWORD="demo_password"

# Prefer venv Python for PySpark if available
if [[ -x ".venv/bin/python" ]]; then
  export PYSPARK_DRIVER_PYTHON="$(pwd)/.venv/bin/python"
  export PYSPARK_PYTHON="$(pwd)/.venv/bin/python"
else
  # Fallback
  export PYSPARK_DRIVER_PYTHON=${PYSPARK_DRIVER_PYTHON:-python}
  export PYSPARK_PYTHON=${PYSPARK_PYTHON:-python}
fi

# Create logs directory
mkdir -p logs

# Validate spark-submit
if ! command -v spark-submit >/dev/null 2>&1; then
  echo "Error: spark-submit not found in PATH" >&2
  exit 1
fi

# JDBC jar checks
JAR=${TRINO_JDBC_JAR:-}
if [[ -z "${JAR}" ]]; then
  echo "Error: TRINO_JDBC_JAR is not set" >&2
  echo "Hint: export TRINO_JDBC_JAR=/path/to/trino-jdbc-xxx.jar" >&2
  exit 1
fi
if [[ ! -f "${JAR}" ]]; then
  echo "Error: JDBC jar not found at '${JAR}'" >&2
  exit 1
fi

MASTER_VAL=${MASTER:-local[4]}

# Run the transformation pipeline with enhanced configuration
spark-submit \
  --master "${MASTER_VAL}" \
  --driver-memory 4g \
  --executor-memory 8g \
  --executor-cores 4 \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.adaptive.coalescePartitions.enabled=true \
  --conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128MB \
  --conf spark.sql.adaptive.skewJoin.enabled=true \
  --conf spark.dynamicAllocation.enabled=true \
  --conf spark.dynamicAllocation.minExecutors=2 \
  --conf spark.dynamicAllocation.maxExecutors=8 \
  --conf spark.sql.shuffle.partitions=200 \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.sql.execution.arrow.pyspark.enabled=true \
  --conf spark.storage.level=MEMORY_AND_DISK_SER \
  --conf spark.network.timeout=800s \
  --jars "${JAR}" \
  --driver-class-path "${JAR}" \
  -m transform_service.cli \
  --pipeline-config configs/pipeline.yaml \
  --connection-config configs/connection.yaml \
  --job-id "enhanced_demo_$(date +%Y%m%d_%H%M%S)" \
  --log-level INFO

echo "✅ Pipeline execution completed!"

# Display performance summary
echo ""
echo "📊 Performance Summary:"
echo "Check the logs above for detailed performance metrics including:"
echo "  - Step execution times"
echo "  - Memory usage patterns"
echo "  - Caching strategy decisions"
echo "  - Row counts and partition information"
echo "  - Spark metrics (jobs, stages, executors)"

echo ""
echo "🔧 Configuration Features Used:"
echo "  ✓ Connection pooling (max 10 connections)"
echo "  ✓ Smart caching with size estimation"
echo "  ✓ Enhanced Spark configuration"
echo "  ✓ Performance monitoring and metrics"
echo "  ✓ Optimized JDBC batch sizes"
echo "  ✓ Adaptive query execution"
