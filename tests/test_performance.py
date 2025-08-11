"""Comprehensive tests for the performance module."""

import time
from datetime import datetime
from unittest.mock import Mock, patch, MagicMock

import pytest

from transform_service.performance import (
    PerformanceTracker,
    DataSizeEstimator,
    CacheStrategy
)


class TestPerformanceTracker:
    """Comprehensive tests for PerformanceTracker class."""

    def test_initialization(self):
        """Test PerformanceTracker initialization."""
        job_id = "test-job-123"
        tracker = PerformanceTracker(job_id)
        
        assert tracker.job_id == job_id
        assert tracker.step_metrics == {}
        assert tracker.start_times == {}

    def test_start_step_tracking(self):
        """Test starting step tracking."""
        tracker = PerformanceTracker("test-job")
        
        with patch('time.time', return_value=1234567890.0):
            tracker.start_step_tracking("step1")
        
        assert tracker.start_times["step1"] == 1234567890.0

    def test_start_step_tracking_multiple_steps(self):
        """Test starting tracking for multiple steps."""
        tracker = PerformanceTracker("test-job")
        
        with patch('time.time', side_effect=[1000.0, 2000.0]):
            tracker.start_step_tracking("step1")
            tracker.start_step_tracking("step2")
        
        assert tracker.start_times["step1"] == 1000.0
        assert tracker.start_times["step2"] == 2000.0

    @patch('transform_service.performance.HAS_PYSPARK', True)
    def test_track_step_performance_success(self):
        """Test successful step performance tracking."""
        tracker = PerformanceTracker("test-job")
        
        # Mock DataFrame
        mock_df = Mock()
        mock_df.count.return_value = 10000
        mock_rdd = Mock()
        mock_rdd.getNumPartitions.return_value = 4
        mock_df.rdd = mock_rdd
        
        # Set start time
        tracker.start_times["test_step"] = 1000.0
        
        with patch('time.time', return_value=1010.5), \
             patch.object(tracker, '_get_memory_usage', return_value=512.75), \
             patch.object(tracker, '_get_spark_metrics', return_value={'active_jobs': 2}), \
             patch('transform_service.performance.datetime') as mock_datetime:
            
            mock_datetime.now.return_value.isoformat.return_value = "2025-01-01T12:00:00"
            
            metrics = tracker.track_step_performance("test_step", mock_df)
        
        expected_metrics = {
            'step_name': 'test_step',
            'execution_time_seconds': 10.5,
            'row_count': 10000,
            'partition_count': 4,
            'memory_usage_mb': 512.75,
            'timestamp': "2025-01-01T12:00:00",
            'spark_metrics': {'active_jobs': 2}
        }
        
        assert metrics == expected_metrics
        assert tracker.step_metrics["test_step"] == expected_metrics

    @patch('transform_service.performance.HAS_PYSPARK', False)
    def test_track_step_performance_no_pyspark(self):
        """Test step performance tracking when PySpark is not available."""
        tracker = PerformanceTracker("test-job")
        mock_df = Mock()
        
        metrics = tracker.track_step_performance("test_step", mock_df)
        
        assert metrics == {'step_name': 'test_step', 'error': 'PySpark not available'}

    @patch('transform_service.performance.HAS_PYSPARK', True)
    def test_track_step_performance_count_exception(self):
        """Test step performance tracking when DataFrame count raises exception."""
        tracker = PerformanceTracker("test-job")
        
        # Mock DataFrame that raises exception on count
        mock_df = Mock()
        mock_df.count.side_effect = Exception("Count failed")
        mock_rdd = Mock()
        mock_rdd.getNumPartitions.return_value = 8
        mock_df.rdd = mock_rdd
        
        tracker.start_times["test_step"] = 1000.0
        
        with patch('time.time', return_value=1005.0), \
             patch.object(tracker, '_get_memory_usage', return_value=256.0), \
             patch.object(tracker, '_get_spark_metrics', return_value={}):
            
            metrics = tracker.track_step_performance("test_step", mock_df)
        
        assert metrics['row_count'] == -1  # Count failed
        assert metrics['partition_count'] == 8
        assert metrics['execution_time_seconds'] == 5.0

    @patch('transform_service.performance.HAS_PYSPARK', True)
    def test_track_step_performance_no_start_time(self):
        """Test step performance tracking when no start time is recorded."""
        tracker = PerformanceTracker("test-job")
        
        mock_df = Mock()
        mock_df.count.return_value = 5000
        mock_rdd = Mock()
        mock_rdd.getNumPartitions.return_value = 2
        mock_df.rdd = mock_rdd
        
        with patch('time.time', return_value=2000.0), \
             patch.object(tracker, '_get_memory_usage', return_value=128.0), \
             patch.object(tracker, '_get_spark_metrics', return_value={}):
            
            metrics = tracker.track_step_performance("unknown_step", mock_df)
        
        # Execution time should be 0 when no start time is recorded
        assert metrics['execution_time_seconds'] == 0.0

    @patch('transform_service.performance.HAS_PSUTIL', True)
    def test_get_memory_usage_success(self):
        """Test successful memory usage retrieval."""
        tracker = PerformanceTracker("test-job")
        
        with patch('psutil.Process') as mock_process_class:
            mock_process = Mock()
            mock_memory_info = Mock()
            mock_memory_info.rss = 1024 * 1024 * 256  # 256 MB in bytes
            mock_process.memory_info.return_value = mock_memory_info
            mock_process_class.return_value = mock_process
            
            memory_usage = tracker._get_memory_usage()
        
        assert memory_usage == 256.0

    @patch('transform_service.performance.HAS_PSUTIL', False)
    def test_get_memory_usage_no_psutil(self):
        """Test memory usage when psutil is not available."""
        tracker = PerformanceTracker("test-job")
        
        memory_usage = tracker._get_memory_usage()
        
        assert memory_usage == 0.0

    @patch('transform_service.performance.HAS_PSUTIL', True)
    def test_get_memory_usage_exception(self):
        """Test memory usage when psutil raises exception."""
        tracker = PerformanceTracker("test-job")
        
        with patch('psutil.Process', side_effect=Exception("Process error")):
            memory_usage = tracker._get_memory_usage()
        
        assert memory_usage == 0.0

    @patch('transform_service.performance.HAS_PYSPARK', True)
    def test_get_spark_metrics_success(self):
        """Test successful Spark metrics retrieval."""
        tracker = PerformanceTracker("test-job")
        
        # Mock SparkContext and StatusTracker
        mock_sc = Mock()
        mock_status = Mock()
        
        # Configure status tracker methods
        mock_status.getActiveJobIds.return_value = [1, 2, 3]
        mock_status.getActiveStageIds.return_value = [10, 11]
        mock_status.getExecutorInfos.return_value = ['exec1', 'exec2', 'exec3', 'exec4']
        
        mock_sc.statusTracker.return_value = mock_status
        
        # Mock the SparkContext import within the method
        with patch('builtins.__import__') as mock_import:
            def side_effect(name, *args):
                if name == 'pyspark':
                    mock_pyspark = Mock()
                    mock_pyspark.SparkContext = Mock()
                    mock_pyspark.SparkContext._active_spark_context = mock_sc
                    return mock_pyspark
                return __import__(name, *args)
            
            mock_import.side_effect = side_effect
            
            metrics = tracker._get_spark_metrics()
        
        expected_metrics = {
            'active_jobs': 3,
            'active_stages': 2,
            'executor_count': 4
        }
        
        assert metrics == expected_metrics

    @patch('transform_service.performance.HAS_PYSPARK', False)
    def test_get_spark_metrics_no_pyspark(self):
        """Test Spark metrics when PySpark is not available."""
        tracker = PerformanceTracker("test-job")
        
        metrics = tracker._get_spark_metrics()
        
        assert metrics == {}

    @patch('transform_service.performance.HAS_PYSPARK', True)
    def test_get_spark_metrics_no_active_context(self):
        """Test Spark metrics when no active SparkContext."""
        tracker = PerformanceTracker("test-job")
        
        # Mock the SparkContext import within the method
        with patch('builtins.__import__') as mock_import:
            def side_effect(name, *args):
                if name == 'pyspark':
                    mock_pyspark = Mock()
                    mock_pyspark.SparkContext = Mock()
                    mock_pyspark.SparkContext._active_spark_context = None
                    return mock_pyspark
                return __import__(name, *args)
            
            mock_import.side_effect = side_effect
            
            metrics = tracker._get_spark_metrics()
        
        assert metrics == {}

    @patch('transform_service.performance.HAS_PYSPARK', True)
    def test_get_spark_metrics_partial_methods(self):
        """Test Spark metrics when only some methods are available."""
        tracker = PerformanceTracker("test-job")
        
        mock_sc = Mock()
        mock_status = Mock()
        
        # Only some methods are available
        mock_status.getActiveJobIds.return_value = [1, 2]
        # getActiveStageIds and getExecutorInfos not available
        del mock_status.getActiveStageIds
        del mock_status.getExecutorInfos
        
        mock_sc.statusTracker.return_value = mock_status
        
        # Mock the SparkContext import within the method
        with patch('builtins.__import__') as mock_import:
            def side_effect(name, *args):
                if name == 'pyspark':
                    mock_pyspark = Mock()
                    mock_pyspark.SparkContext = Mock()
                    mock_pyspark.SparkContext._active_spark_context = mock_sc
                    return mock_pyspark
                return __import__(name, *args)
            
            mock_import.side_effect = side_effect
            
            metrics = tracker._get_spark_metrics()
        
        assert metrics == {'active_jobs': 2}

    @patch('transform_service.performance.HAS_PYSPARK', True)
    def test_get_spark_metrics_exception(self):
        """Test Spark metrics when exception is raised."""
        tracker = PerformanceTracker("test-job")
        
        # Mock the SparkContext import to raise exception
        with patch('builtins.__import__', side_effect=Exception("Import error")):
            metrics = tracker._get_spark_metrics()
        
        assert metrics == {}

    def test_get_pipeline_summary_empty(self):
        """Test pipeline summary when no steps have been tracked."""
        tracker = PerformanceTracker("test-job")
        
        summary = tracker.get_pipeline_summary()
        
        assert summary == {}

    def test_get_pipeline_summary_with_steps(self):
        """Test pipeline summary with tracked steps."""
        tracker = PerformanceTracker("test-job-123")
        
        # Add some mock step metrics
        tracker.step_metrics = {
            "step1": {
                "step_name": "step1",
                "execution_time_seconds": 5.5,
                "row_count": 1000,
                "partition_count": 2,
                "memory_usage_mb": 256.0,
                "timestamp": "2025-01-01T12:00:00",
                "spark_metrics": {}
            },
            "step2": {
                "step_name": "step2",
                "execution_time_seconds": 3.2,
                "row_count": 2000,
                "partition_count": 4,
                "memory_usage_mb": 512.0,
                "timestamp": "2025-01-01T12:00:05",
                "spark_metrics": {}
            },
            "step3": {
                "step_name": "step3",
                "execution_time_seconds": 7.1,
                "row_count": -1,  # Failed count
                "partition_count": 6,
                "memory_usage_mb": 1024.0,
                "timestamp": "2025-01-01T12:00:10",
                "spark_metrics": {}
            }
        }
        
        summary = tracker.get_pipeline_summary()
        
        expected_summary = {
            'job_id': 'test-job-123',
            'total_steps': 3,
            'total_execution_time_seconds': 15.8,  # 5.5 + 3.2 + 7.1
            'total_rows_processed': 3000,  # 1000 + 2000 (step3 excluded due to -1)
            'average_step_time': 5.27,  # 15.8 / 3
            'steps_performance': tracker.step_metrics
        }
        
        assert summary == expected_summary

    def test_log_pipeline_summary_empty(self, caplog):
        """Test logging pipeline summary when no steps tracked."""
        tracker = PerformanceTracker("test-job")
        
        tracker.log_pipeline_summary()
        
        # Should not log anything when no steps
        assert len(caplog.records) == 0

    def test_log_pipeline_summary_with_steps(self, caplog):
        """Test logging pipeline summary with steps."""
        tracker = PerformanceTracker("test-job-456")
        
        # Add mock step metrics with different execution times
        tracker.step_metrics = {
            "fast_step": {
                "step_name": "fast_step",
                "execution_time_seconds": 1.0,
                "row_count": 100,
            },
            "slow_step": {
                "step_name": "slow_step",
                "execution_time_seconds": 10.0,
                "row_count": 10000,
            },
            "medium_step": {
                "step_name": "medium_step",
                "execution_time_seconds": 5.0,
                "row_count": 5000,
            }
        }
        
        with caplog.at_level('INFO'):
            tracker.log_pipeline_summary()
        
        # Check that summary information was logged
        log_messages = [record.message for record in caplog.records]
        
        assert any("Pipeline Performance Summary" in msg for msg in log_messages)
        assert any("Total Steps: 3" in msg for msg in log_messages)
        assert any("Total Execution Time: 16.0s" in msg for msg in log_messages)
        assert any("Total Rows Processed: 15,100" in msg for msg in log_messages)
        assert any("Average Step Time: 5.33s" in msg for msg in log_messages)
        
        # Check that slowest steps are logged (should be sorted by execution time)
        assert any("1. slow_step: 10.0s" in msg for msg in log_messages)
        assert any("2. medium_step: 5.0s" in msg for msg in log_messages)
        assert any("3. fast_step: 1.0s" in msg for msg in log_messages)


class TestDataSizeEstimator:
    """Tests for DataSizeEstimator class."""

    @patch('transform_service.performance.HAS_PYSPARK', True)
    def test_estimate_dataframe_size_success(self):
        """Test successful DataFrame size estimation."""
        # Mock DataFrame and sample
        mock_df = Mock()
        mock_sample_df = Mock()
        mock_sample_df.count.return_value = 10  # 10 rows in 1% sample
        
        # Mock collected row for size estimation
        mock_row = Mock()
        mock_row.__str__ = Mock(return_value="a" * 100)  # 100 bytes per row
        mock_sample_df.collect.return_value = [mock_row]
        
        mock_df.sample.return_value = mock_sample_df
        
        result = DataSizeEstimator.estimate_dataframe_size(mock_df, sample_fraction=0.01)
        
        expected_result = {
            'estimated_rows': 1000,  # 10 / 0.01
            'estimated_size_mb': 0.10,  # (1000 * 100) / 1024 / 1024, rounded to 2 decimals
            'sample_count': 10,
            'sample_fraction': 0.01
        }
        
        assert result == expected_result
        mock_df.sample.assert_called_once_with(0.01)

    @patch('transform_service.performance.HAS_PYSPARK', False)
    def test_estimate_dataframe_size_no_pyspark(self):
        """Test DataFrame size estimation when PySpark is not available."""
        mock_df = Mock()
        
        result = DataSizeEstimator.estimate_dataframe_size(mock_df)
        
        expected_result = {
            'estimated_rows': 0,
            'estimated_size_mb': 0,
            'error': 'PySpark not available'
        }
        
        assert result == expected_result

    @patch('transform_service.performance.HAS_PYSPARK', True)
    def test_estimate_dataframe_size_empty_sample(self):
        """Test DataFrame size estimation with empty sample."""
        mock_df = Mock()
        mock_sample_df = Mock()
        mock_sample_df.count.return_value = 0  # Empty sample
        
        mock_df.sample.return_value = mock_sample_df
        
        result = DataSizeEstimator.estimate_dataframe_size(mock_df)
        
        expected_result = {
            'estimated_rows': 0,
            'estimated_size_mb': 0
        }
        
        assert result == expected_result

    @patch('transform_service.performance.HAS_PYSPARK', True)
    def test_estimate_dataframe_size_exception(self):
        """Test DataFrame size estimation when exception is raised."""
        mock_df = Mock()
        mock_df.sample.side_effect = Exception("Sampling failed")
        
        with patch('transform_service.performance.logger') as mock_logger:
            result = DataSizeEstimator.estimate_dataframe_size(mock_df)
        
        expected_result = {
            'estimated_rows': -1,
            'estimated_size_mb': -1
        }
        
        assert result == expected_result
        mock_logger.warning.assert_called_once_with("Failed to estimate DataFrame size: Sampling failed")

    @patch('transform_service.performance.HAS_PYSPARK', True)
    def test_estimate_dataframe_size_custom_sample_fraction(self):
        """Test DataFrame size estimation with custom sample fraction."""
        mock_df = Mock()
        mock_sample_df = Mock()
        mock_sample_df.count.return_value = 50  # 50 rows in 5% sample
        
        mock_row = Mock()
        mock_row.__str__ = Mock(return_value="x" * 200)  # 200 bytes per row
        mock_sample_df.collect.return_value = [mock_row]
        
        mock_df.sample.return_value = mock_sample_df
        
        result = DataSizeEstimator.estimate_dataframe_size(mock_df, sample_fraction=0.05)
        
        expected_result = {
            'estimated_rows': 1000,  # 50 / 0.05
            'estimated_size_mb': 0.19,  # (1000 * 200) / 1024 / 1024, rounded to 2 decimals
            'sample_count': 50,
            'sample_fraction': 0.05
        }
        
        assert result == expected_result
        mock_df.sample.assert_called_once_with(0.05)


class TestCacheStrategy:
    """Tests for CacheStrategy class."""

    def test_get_optimal_storage_level_small_data(self):
        """Test optimal storage level for small data."""
        # Small data (< 100MB) should use MEMORY_ONLY
        result = CacheStrategy.get_optimal_storage_level(50.0)
        assert result == "MEMORY_ONLY"
        
        result = CacheStrategy.get_optimal_storage_level(99.9)
        assert result == "MEMORY_ONLY"

    def test_get_optimal_storage_level_medium_data(self):
        """Test optimal storage level for medium data."""
        # Medium data (100MB - 1000MB) should use MEMORY_AND_DISK
        result = CacheStrategy.get_optimal_storage_level(500.0)
        assert result == "MEMORY_AND_DISK"
        
        result = CacheStrategy.get_optimal_storage_level(999.9)
        assert result == "MEMORY_AND_DISK"

    def test_get_optimal_storage_level_large_data(self):
        """Test optimal storage level for large data."""
        # Large data (1000MB - 5000MB) should use MEMORY_AND_DISK_SER
        result = CacheStrategy.get_optimal_storage_level(2500.0)
        assert result == "MEMORY_AND_DISK_SER"
        
        result = CacheStrategy.get_optimal_storage_level(4999.9)
        assert result == "MEMORY_AND_DISK_SER"

    def test_get_optimal_storage_level_very_large_data(self):
        """Test optimal storage level for very large data."""
        # Very large data (>= 5000MB) should not be cached
        result = CacheStrategy.get_optimal_storage_level(5000.0)
        assert result is None
        
        result = CacheStrategy.get_optimal_storage_level(10000.0)
        assert result is None

    def test_get_optimal_storage_level_negative_size(self):
        """Test optimal storage level for negative size (error case)."""
        result = CacheStrategy.get_optimal_storage_level(-1.0)
        assert result == "MEMORY_AND_DISK"  # Default fallback

    def test_get_optimal_storage_level_boundary_conditions(self):
        """Test optimal storage level at exact boundary values."""
        # Test exact boundaries
        assert CacheStrategy.get_optimal_storage_level(100.0) == "MEMORY_AND_DISK"
        assert CacheStrategy.get_optimal_storage_level(1000.0) == "MEMORY_AND_DISK_SER"
        assert CacheStrategy.get_optimal_storage_level(5000.0) is None

    def test_should_cache_positive_cases(self):
        """Test should_cache for cases where caching is recommended."""
        # Small data
        assert CacheStrategy.should_cache(50.0) is True
        
        # Medium data
        assert CacheStrategy.should_cache(500.0) is True
        
        # Large data (but still under threshold)
        assert CacheStrategy.should_cache(4999.9) is True

    def test_should_cache_negative_cases(self):
        """Test should_cache for cases where caching is not recommended."""
        # Very large data
        assert CacheStrategy.should_cache(5000.0) is False
        assert CacheStrategy.should_cache(10000.0) is False
        
        # Negative size (error case)
        assert CacheStrategy.should_cache(-1.0) is False
        
        # Zero size
        assert CacheStrategy.should_cache(0.0) is False

    def test_should_cache_boundary_condition(self):
        """Test should_cache at exact boundary."""
        # At the exact threshold
        assert CacheStrategy.should_cache(5000.0) is False
        assert CacheStrategy.should_cache(4999.99) is True

    def test_cache_strategy_constants(self):
        """Test that cache strategy constants are properly defined."""
        assert CacheStrategy.SMALL_DATA_THRESHOLD == 100
        assert CacheStrategy.MEDIUM_DATA_THRESHOLD == 1000
        assert CacheStrategy.LARGE_DATA_THRESHOLD == 5000
        
        # Verify ordering
        assert CacheStrategy.SMALL_DATA_THRESHOLD < CacheStrategy.MEDIUM_DATA_THRESHOLD
        assert CacheStrategy.MEDIUM_DATA_THRESHOLD < CacheStrategy.LARGE_DATA_THRESHOLD
