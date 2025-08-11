from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
from typing import Dict, List, Any
import hashlib

try:
    from pyspark.sql import SparkSession as _SparkSession
except ImportError:
    # Stub for testing environments without PySpark
    class _SparkSession:  # type: ignore
        class builder:  # type: ignore
            @staticmethod
            def appName(name: str):  # type: ignore
                return _SparkSession.builder

            @staticmethod
            def config(key: str, value: str):  # type: ignore
                return _SparkSession.builder

            @staticmethod
            def getOrCreate():  # type: ignore
                class MockSession:
                    def stop(self):
                        pass
                return MockSession()
    _SparkSession = _SparkSession  # type: ignore

from .config import ConfigLoader
from .connectors import StarburstConnector
from .steps import TransformationStep
from .status import JobStatusTracker
from .logging_utils import JobIdLogger
from .performance import PerformanceTracker, DataSizeEstimator, CacheStrategy

logger = logging.getLogger(__name__)


class TransformationPipeline:
    def __init__(
        self,
        config_path: str | None,
        job_id: str | None = None,
        restart: bool = False,
        *,
        pipeline_config: str | None = None,
        connection_config: str | None = None,
    ):
        if config_path:
            self.config = ConfigLoader.load(config_path)
        elif pipeline_config and connection_config:
            self.config = ConfigLoader.load_separate(pipeline_config, connection_config)
        else:
            # Allow programmatic injection of config in tests or advanced usage
            self.config = {}
        self.job_id = job_id or self._generate_job_id()
        self.logger = JobIdLogger(logger, {"job_id": self.job_id})
        self.restart = restart
        self.spark: Any = self._create_spark_session()
        # StarburstConnector expects starburst section; tests may monkeypatch this class
        self.connector = StarburstConnector(self.config.get("starburst", {}), self.spark)
        self.status_tracker: Any = JobStatusTracker(
            self.connector, self.job_id, self.config.get("job_tracking", {})
        )
        self.performance_tracker = PerformanceTracker(self.job_id)
        self.steps = self._create_steps()
        self.execution_context: Dict[str, Any] = {}
        self.completed_steps: set[str] = set()
        self.failed_steps: set[str] = set()
        if restart:
            self._load_restart_state()

    def _generate_job_id(self) -> str:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"transform_job_{timestamp}_{uuid.uuid4().hex[:8]}"

    def _create_spark_session(self) -> Any:
        spark_config = self.config.get("spark", {})
        # When PySpark is not present, this will raise AttributeError; tests stub this method
        builder = _SparkSession.builder.appName(
            spark_config.get("app_name", "TransformationPipeline")
        )
        if "jdbc_jar_path" in spark_config:
            builder = builder.config("spark.jars", spark_config["jdbc_jar_path"])

        # Apply all user-provided configurations
        for k, v in spark_config.get("configs", {}).items():
            builder = builder.config(k, v)
        return builder.getOrCreate()

    def _create_steps(self) -> List[TransformationStep]:
        base_path = self.config.get("sql_base_path", "sql")
        steps_cfg = self.config.get("transformations", [])
        return [TransformationStep(step_cfg, base_path) for step_cfg in steps_cfg]

    def _validate_final_outputs(self) -> None:
        if not self.config.get("final_outputs"):
            return
        step_names = {s.name for s in self.steps}
        invalid = [name for name in self.config["final_outputs"].keys() if name not in step_names]
        if invalid:
            raise ValueError(f"final_outputs reference unknown steps: {invalid}")

    def _load_restart_state(self) -> None:
        self.completed_steps = set(self.status_tracker.get_completed_steps())
        self.failed_steps = set(self.status_tracker.get_failed_steps())

    def _get_execution_order(self) -> List[List[TransformationStep]]:
        remaining_steps = [
            s for s in self.steps if not (self.restart and s.name in self.completed_steps)
        ]
        if not remaining_steps:
            return []
        execution_order: List[List[TransformationStep]] = []
        executed = set(self.completed_steps) if self.restart else set()
        while len(executed) < len(self.steps):
            current_batch: List[TransformationStep] = []
            for step in self.steps:
                if step.name in executed:
                    continue
                if all(dep in executed for dep in step.depends_on):
                    current_batch.append(step)
            if not current_batch:
                remaining_names = [s.name for s in self.steps if s.name not in executed]
                if remaining_names:
                    raise RuntimeError(
                        "Circular dependency detected or missing dependencies for steps: "
                        f"{remaining_names}"
                    )
                break
            parallel_groups: Dict[str, List[TransformationStep]] = {}
            sequential: List[TransformationStep] = []
            for step in current_batch:
                if step.parallel_group:
                    parallel_groups.setdefault(step.parallel_group, []).append(step)
                else:
                    sequential.append(step)
            for step in sequential:
                execution_order.append([step])
                executed.add(step.name)
            for group_steps in parallel_groups.values():
                execution_order.append(group_steps)
                for step in group_steps:
                    executed.add(step.name)
        return execution_order

    def _call_step_execute(self, step: TransformationStep) -> Any:
        """Call step.execute with backward-compat for 2-arg signatures used in tests."""
        # Check if the result is already cached
        if step.name in self.execution_context and getattr(self.execution_context[step.name], 'is_cached', False):
            self.logger.info("Using cached result for step: %s", step.name)
            return self.execution_context[step.name]

        try:
            return step.execute(self.connector, self.execution_context, self.logger)
        except TypeError:
            # Fallback to 2-arg execute(connector, context)
            return step.execute(self.connector, self.execution_context)

    def _execute_step_batch(self, steps: List[TransformationStep]) -> None:
        # Import StorageLevel when needed
        StorageLevel: Any = None
        try:
            from pyspark.storagelevel import StorageLevel  # type: ignore[no-redef]
        except ImportError:
            pass

        if len(steps) == 1:
            step = steps[0]
            self.status_tracker.start_step(
                step.name, step.depends_on, step.parallel_group
            )
            self.performance_tracker.start_step_tracking(step.name)
            # Respect optional timeout
            step_timeout = self.config.get("step_timeout_seconds")
            if step_timeout:
                with ThreadPoolExecutor(max_workers=1) as ex:
                    fut = ex.submit(self._call_step_execute, step)
                    try:
                        result = fut.result(timeout=step_timeout)
                    except TimeoutError as te:
                        self.status_tracker.fail_step(step.name, f"Timeout after {step_timeout}s")
                        self.logger.error("Step timed out: %s", step.name)
                        raise te
            else:
                result = self._call_step_execute(step)

            # Smart Caching
            if getattr(step, 'cache_strategy', 'never') == "auto":
                estimated_size_mb = DataSizeEstimator.estimate_dataframe_size(result).get(
                    "estimated_size_mb", 0
                )
                if CacheStrategy.should_cache(estimated_size_mb):
                    storage_level_str = CacheStrategy.get_optimal_storage_level(
                        estimated_size_mb
                    )
                    if storage_level_str and StorageLevel is not None:
                        storage_level = getattr(StorageLevel, storage_level_str, None)
                        if storage_level is not None and hasattr(result, 'persist'):
                            result.persist(storage_level)
                            self.logger.info(
                                "Auto-caching step %s with level %s (est. size: %.2f MB)",
                                step.name,
                                storage_level_str,
                                estimated_size_mb,
                            )

            self.execution_context[step.name] = result
            self.performance_tracker.track_step_performance(step.name, result)
            self.status_tracker.complete_step(step.name)
            return
            self.status_tracker.complete_step(step.name)
            return
        self.logger.info("Executing %d steps in parallel", len(steps))
        max_workers = self.config.get("max_parallel_workers", 4)
        step_timeout = self.config.get("step_timeout_seconds")

        def run(step: TransformationStep):
            self.status_tracker.start_step(
                step.name, step.depends_on, step.parallel_group
            )
            self.performance_tracker.start_step_tracking(step.name)
            res = self._call_step_execute(step)
            self.performance_tracker.track_step_performance(step.name, res)
            return step.name, res

        first_error: Exception | None = None
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futures = {ex.submit(run, s): s for s in steps}
            for fut in as_completed(futures):
                step = futures[fut]
                try:
                    if step_timeout:
                        name, res = fut.result(timeout=step_timeout)
                    else:
                        name, res = fut.result()
                    self.execution_context[name] = res
                    self.status_tracker.complete_step(name)
                    self.logger.info("Parallel step completed: %s", name)
                except TimeoutError as te:
                    self.status_tracker.fail_step(step.name, f"Timeout after {step_timeout}s")
                    self.logger.error("Parallel step timed out: %s", step.name)
                    if first_error is None:
                        first_error = te
                except Exception as e:  # noqa: BLE001
                    self.status_tracker.fail_step(step.name, str(e))
                    self.logger.error(
                        "Parallel step failed: %s", step.name, exc_info=True
                    )
                    if first_error is None:
                        first_error = e
        if first_error is not None:
            raise first_error

    def execute(self) -> Dict[str, Any]:
        self.logger.info("Starting transformation pipeline")
        start_time = datetime.now()
        try:
            order = self._get_execution_order()
            if not order:
                self.logger.info("No steps to execute")
                return self.execution_context
            self._validate_final_outputs()
            # Stable config hash
            config_hash = hashlib.sha256(json.dumps(self.config, sort_keys=True).encode()).hexdigest()
            total_steps = len(self.steps)
            self.status_tracker.start_job(
                "TransformationPipeline", total_steps, config_hash
            )
            for i, batch in enumerate(order, 1):
                names = [s.name for s in batch]
                self.logger.info("Executing batch %d/%d: %s", i, len(order), names)
                self._execute_step_batch(batch)
            self._write_final_outputs()
            dur = datetime.now() - start_time
            self.logger.info("Pipeline completed successfully in %s", dur)

            # Log performance summary
            self.performance_tracker.log_pipeline_summary()

            self.status_tracker.update_job_status(
                "COMPLETED", completed_steps=total_steps
            )
            return self.execution_context
        except Exception as e:  # noqa: BLE001
            self.logger.exception("Pipeline failed: %s", e)
            self.status_tracker.update_job_status("FAILED", error_message=str(e))
            raise
        finally:
            # Some tests stub Spark and don't have stop(); guard it
            try:
                self.spark.stop()
            except Exception:
                pass

    def _write_final_outputs(self) -> None:
        final_outputs = self.config.get("final_outputs", {})
        for name, cfg in final_outputs.items():
            if name in self.execution_context:
                table = cfg["table"]
                mode = cfg.get("mode", "overwrite")
                self.logger.info("Writing final output %s to %s", name, table)
                self.connector.write_table(self.execution_context[name], table, mode)
