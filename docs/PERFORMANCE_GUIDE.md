# Performance Optimization Guide

This document explains the performance-related features in the Transform Service and how to configure them effectively.

## Caching Strategy

Enable automatic, size-aware caching on specific steps via the pipeline configuration:

```yaml
# In pipeline.yaml
transformations:
  - name: "clean_orders"
    sql_file: 05_clean_orders.sql
    depends_on: [load_orders]
    cache_strategy: auto   # enable intelligent caching for this step
```

Caching levels chosen automatically (when Spark is available):
- <100MB: MEMORY_ONLY
- 100MB–1GB: MEMORY_AND_DISK
- 1GB–5GB: MEMORY_AND_DISK_SER
- >5GB: Not cached

Notes:
- The pipeline estimates DataFrame size using sampling; if PySpark isn’t present, estimation returns a harmless default and no caching is applied.
- You can still call connector-level basic cache if needed (`StarburstConnector.cache_dataframe`).

## Connector Tuning

Configure the Starburst connector for throughput and resiliency:

```yaml
# In connection.yaml
starburst:
  connection_pool_size: 10   # simple connection pool
  fetchsize: 50000           # read batch size
  batchsize: 50000           # write batch size
  max_retries: 3             # retries within the connector
  retry_backoff_seconds: 1.0 # exponential backoff base
  use_temp_views: false      # optional: register temp views for placeholders
```

Benefits:
- Reduced connection overhead via pooling for parallel steps
- Better throughput with larger fetch/batch sizes
- Built-in retries for transient failures

## Performance Monitoring

The pipeline collects and logs step metrics:
- Execution time
- Row count (best-effort; errors recorded as -1)
- Partition count (when RDD available)
- Memory usage (psutil when available)
- Spark status metrics (active jobs/stages/executors) when an active SparkContext exists

Examples:
```
Performance metrics for load_customers: Execution time: 45.23s, Rows: 1,234,567, Partitions: 8, Memory: 512MB
Performance metrics for clean_orders:   Execution time: 78.45s, Rows: 5,678,901, Partitions: 16, Memory: 1024MB
```

A pipeline-level summary is logged at the end of successful runs, including totals and the top 3 slowest steps.

## Spark Configuration Tips

```yaml
spark:
  configs:
    # Adaptive Query Execution
    spark.sql.adaptive.enabled: "true"
    spark.sql.adaptive.coalescePartitions.enabled: "true"
    spark.sql.adaptive.advisoryPartitionSizeInBytes: "128MB"
    spark.sql.adaptive.skewJoin.enabled: "true"

    # Dynamic Resource Allocation
    spark.dynamicAllocation.enabled: "true"
    spark.dynamicAllocation.minExecutors: "2"
    spark.dynamicAllocation.maxExecutors: "20"

    # Memory and Parallelism
    spark.executor.memory: "8g"
    spark.executor.cores: "4"
    spark.sql.shuffle.partitions: "200"
```

Adjust based on workload size and cluster resources.

## Troubleshooting

- PySpark not present:
  - Metrics and caching gracefully degrade. Data size estimation returns:
    `{"estimated_rows": 0, "estimated_size_mb": 0, "error": "PySpark not available"}`
- Slow reads/writes:
  - Increase `fetchsize`/`batchsize`
  - Review parallelism (`max_parallel_workers`)
- Excessive memory usage:
  - Reduce shuffle partitions or disable caching for large steps
  - Increase executor memory
