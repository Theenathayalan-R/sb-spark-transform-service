from __future__ import annotations

import logging
import time
from datetime import datetime
from typing import Dict, Any, Optional, Iterable, Sized, cast

try:
    from pyspark.sql import DataFrame
    from pyspark import SparkContext
    HAS_PYSPARK = True  # For backwards compatibility with tests
except ImportError:
    DataFrame = Any  # type: ignore
    SparkContext = Any  # type: ignore
    HAS_PYSPARK = False  # For backwards compatibility with tests

HAS_PSUTIL = True
try:
    import psutil  # type: ignore[import-untyped]
except ImportError:
    HAS_PSUTIL = False

logger = logging.getLogger(__name__)


class PerformanceTracker:
    """Performance monitoring and metrics collection for transformation steps."""

    def __init__(self, job_id: str):
        self.job_id = job_id
        self.step_metrics: Dict[str, Dict[str, Any]] = {}
        self.start_times: Dict[str, float] = {}

    def start_step_tracking(self, step_name: str) -> None:
        """Start performance tracking for a step."""
        self.start_times[step_name] = time.time()

    def track_step_performance(self, step_name: str, df: Any) -> Dict[str, Any]:
        """Track performance metrics for a completed step."""
        # If PySpark isn't available, return a clear error per tests
        if not HAS_PYSPARK:
            return {'step_name': step_name, 'error': 'PySpark not available'}

        end_time = time.time()
        start_time = self.start_times.get(step_name, end_time)
        execution_time = end_time - start_time

        # Get DataFrame metrics
        try:
            row_count = df.count() if hasattr(df, 'count') else 0
        except Exception:
            row_count = -1  # Unable to count

        partition_count = df.rdd.getNumPartitions() if hasattr(df, 'rdd') else 0

        # Get system metrics
        memory_usage = self._get_memory_usage()
        spark_metrics = self._get_spark_metrics()

        metrics = {
            'step_name': step_name,
            'execution_time_seconds': round(execution_time, 2),
            'row_count': row_count,
            'partition_count': partition_count,
            'memory_usage_mb': memory_usage,
            'timestamp': datetime.now().isoformat(),
            'spark_metrics': spark_metrics
        }

        self.step_metrics[step_name] = metrics

        # Log performance summary (be robust to mocks/non-numeric row_count)
        try:
            rows_display = f"{int(row_count):,}"
        except Exception:
            rows_display = str(row_count)

        logger.info(
            f"Performance metrics for {step_name}: "
            f"Execution time: {execution_time:.2f}s, "
            f"Rows: {rows_display}, "
            f"Partitions: {partition_count}, "
            f"Memory: {memory_usage}MB"
        )

        return metrics

    def _get_memory_usage(self) -> float:
        """Get current memory usage in MB."""
        if not HAS_PSUTIL:
            return 0.0
        try:
            process = psutil.Process()  # type: ignore[name-defined]
            memory_info = process.memory_info()
            return round(memory_info.rss / 1024 / 1024, 2)
        except Exception:
            return 0.0

    def _get_spark_metrics(self) -> Dict[str, Any]:
        """Get Spark-specific metrics."""
        # Respect availability flag and import at call time so tests can patch imports
        if not HAS_PYSPARK:
            return {}
        try:
            pyspark = __import__('pyspark')
            sc = pyspark.SparkContext._active_spark_context  # type: ignore[attr-defined]
            if sc is None:
                return {}

            status = sc.statusTracker()
            metrics: Dict[str, Any] = {}

            get_active_jobs = getattr(status, 'getActiveJobIds', None)
            if callable(get_active_jobs):
                try:
                    res = get_active_jobs()
                    if hasattr(res, '__len__'):
                        metrics['active_jobs'] = len(cast(Sized, res))
                    else:
                        metrics['active_jobs'] = len(list(cast(Iterable[Any], res)))
                except Exception:
                    pass

            get_active_stages = getattr(status, 'getActiveStageIds', None)
            if callable(get_active_stages):
                try:
                    res = get_active_stages()
                    if hasattr(res, '__len__'):
                        metrics['active_stages'] = len(cast(Sized, res))
                    else:
                        metrics['active_stages'] = len(list(cast(Iterable[Any], res)))
                except Exception:
                    pass

            get_executor_infos = getattr(status, 'getExecutorInfos', None)
            if callable(get_executor_infos):
                try:
                    res = get_executor_infos()
                    if hasattr(res, '__len__'):
                        metrics['executor_count'] = len(cast(Sized, res))
                    else:
                        metrics['executor_count'] = len(list(cast(Iterable[Any], res)))
                except Exception:
                    pass

            return metrics
        except Exception:
            return {}

    def get_pipeline_summary(self) -> Dict[str, Any]:
        """Get overall pipeline performance summary."""
        if not self.step_metrics:
            return {}

        total_execution_time = sum(
            metrics['execution_time_seconds']
            for metrics in self.step_metrics.values()
        )

        total_rows = sum(
            metrics['row_count']
            for metrics in self.step_metrics.values()
            if metrics['row_count'] > 0
        )

        return {
            'job_id': self.job_id,
            'total_steps': len(self.step_metrics),
            'total_execution_time_seconds': round(total_execution_time, 2),
            'total_rows_processed': total_rows,
            'average_step_time': round(total_execution_time / len(self.step_metrics), 2),
            'steps_performance': self.step_metrics
        }

    def log_pipeline_summary(self) -> None:
        """Log comprehensive pipeline performance summary."""
        summary = self.get_pipeline_summary()
        if not summary:
            return

        logger.info(f"=== Pipeline Performance Summary (Job: {self.job_id}) ===")
        logger.info(f"Total Steps: {summary['total_steps']}")
        logger.info(f"Total Execution Time: {summary['total_execution_time_seconds']}s")
        logger.info(f"Total Rows Processed: {summary['total_rows_processed']:,}")
        logger.info(f"Average Step Time: {summary['average_step_time']}s")

        # Log slowest steps
        sorted_steps = sorted(
            summary['steps_performance'].values(),
            key=lambda x: x['execution_time_seconds'],
            reverse=True
        )

        logger.info("Top 3 slowest steps:")
        for i, step in enumerate(sorted_steps[:3], 1):
            logger.info(
                f"  {i}. {step['step_name']}: {step['execution_time_seconds']}s "
                f"({step['row_count']:,} rows)"
            )


class DataSizeEstimator:
    """Utility class to estimate DataFrame sizes for caching decisions."""

    @staticmethod
    def estimate_dataframe_size(df: Any, sample_fraction: float = 0.01) -> Dict[str, Any]:
        """Estimate DataFrame size using sampling."""
        # If PySpark isn't available, return a clear, non-error result
        if not HAS_PYSPARK:
            return {
                'estimated_rows': 0,
                'estimated_size_mb': 0,
                'error': 'PySpark not available'
            }
        try:
            # Sample the DataFrame
            sample_df = df.sample(sample_fraction)
            sample_count = sample_df.count()

            if sample_count == 0:
                return {'estimated_rows': 0, 'estimated_size_mb': 0}

            # Estimate total rows
            estimated_total_rows = int(sample_count / sample_fraction)

            # Estimate row size (rough approximation)
            # This is a simple heuristic - in production you might want more sophisticated estimation
            estimated_row_size_bytes = len(str(sample_df.collect()[0])) if sample_count > 0 else 100
            estimated_size_mb = (estimated_total_rows * estimated_row_size_bytes) / 1024 / 1024

            return {
                'estimated_rows': estimated_total_rows,
                'estimated_size_mb': round(estimated_size_mb, 2),
                'sample_count': sample_count,
                'sample_fraction': sample_fraction
            }
        except Exception as e:
            logger.warning(f"Failed to estimate DataFrame size: {e}")
            return {'estimated_rows': -1, 'estimated_size_mb': -1}


class CacheStrategy:
    """Smart caching strategy based on data size and memory availability."""

    # Thresholds in MB
    SMALL_DATA_THRESHOLD = 100  # Cache in memory
    MEDIUM_DATA_THRESHOLD = 1000  # Cache with disk backup
    LARGE_DATA_THRESHOLD = 5000  # Don't cache, too large

    @staticmethod
    def get_optimal_storage_level(estimated_size_mb: float) -> Optional[str]:
        """Determine optimal storage level based on estimated data size."""
        if estimated_size_mb < 0:
            return "MEMORY_AND_DISK"  # Default fallback
        elif estimated_size_mb < CacheStrategy.SMALL_DATA_THRESHOLD:
            return "MEMORY_ONLY"
        elif estimated_size_mb < CacheStrategy.MEDIUM_DATA_THRESHOLD:
            return "MEMORY_AND_DISK"
        elif estimated_size_mb < CacheStrategy.LARGE_DATA_THRESHOLD:
            return "MEMORY_AND_DISK_SER"
        else:
            return None  # Don't cache

    @staticmethod
    def should_cache(estimated_size_mb: float) -> bool:
        """Determine if DataFrame should be cached."""
        return (estimated_size_mb > 0 and
                estimated_size_mb < CacheStrategy.LARGE_DATA_THRESHOLD)
