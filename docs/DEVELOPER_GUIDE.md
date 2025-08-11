# Developer Guide: Transform Service

Audience: engineers building and maintaining configuration-driven Spark + Starburst/Trino transformations. This document explains the architecture, development workflow, testing, and the coding standards enforced in this repository.

---

## 1. Architecture Overview

The system is modular and layered. Instead of monolithic single-file scripts, it separates concerns into small, testable units.

- CLI (`transform_service/cli.py`)
  - Parses args: unified or split YAML configs, flags like `--dry-run`, `--restart`, `--job-id`, `--log-level`.
  - Boots logging, constructs and runs the `TransformationPipeline`.

- Pipeline (`transform_service/pipeline.py`)
  - Loads config via `ConfigLoader`.
  - Builds a DAG from step `depends_on` definitions.
  - Batches steps by dependency frontier; supports parallel batches via `parallel_group` and a thread pool sized by `max_parallel_workers`.
  - Orchestrates: start/complete/fail steps, restart behavior, writing final outputs, job-level status updates.
  - Creates a SparkSession with optional Iceberg integration and arbitrary Spark configs.
  - Optional per-step timeout via `step_timeout_seconds`.

- Steps (`transform_service/steps.py`)
  - `TransformationStep` encapsulates one SQL file.
  - Replaces placeholders `${step_name}` with temp identifiers; optionally registers temp views from prior step DataFrames when `use_temp_views` is enabled under `starburst` config.
  - Supports per-step caching via pipeline’s auto-caching (see performance section) and optional `output_table` writes.

- Connectors (`transform_service/connectors.py`)
  - Abstraction for reading from Starburst/Trino into Spark and writing DataFrames back to tables.
  - Simple connection pooling and retries.

- Configuration (`transform_service/config.py`, `transform_service/schemas.py`)
  - YAML → Python dict with environment variable expansion `${VAR}`.
  - Validated and normalized using Pydantic models for safety and great error messages.
  - Supports unified (`--config`) or split files (`--pipeline-config`, `--connection-config`).

- Status Tracking (`transform_service/status.py`)
  - `JobStatusTracker` interface records job/step lifecycle.
  - Default implementation prints/logs; can be extended to persist state and enable `--restart` durability.

- Logging (`transform_service/logging_utils.py`)
  - Structured JSON logs using `python-json-logger` when available, with a consistent `job_id` injected via `JobIdLogger` wrapper.

- Performance (`transform_service/performance.py`)
  - `PerformanceTracker` collects step metrics and logs a summary.
  - Lazy import of PySpark inside metric collection functions for testability.
  - `DataSizeEstimator` estimates DF size; `CacheStrategy` suggests storage levels.

The modular approach enables unit tests to mock connectors and Spark, resulting in fast and deterministic CI.

---

## 2. Configuration-Driven Pipelines

- `configs/pipeline.yaml`: declares ordered transformations and optional `parallel_group`, `cache_strategy: auto`, `output_table`, and timeouts.
- `configs/connection.yaml`: declares Spark and Starburst connectivity, plus arbitrary Spark configs and job tracking options. `use_temp_views` toggles registering temp views.
- Environment variables can be injected with `${VAR}` patterns; unresolved vars remain as literal text to surface misconfiguration early.
- The loader validates shape via Pydantic and merges split configs into a single runtime structure.

Place SQL files under `sql/` (override path with `sql_base_path`). Use placeholders like `${clean_orders}` to reference outputs of prior steps.

---

## 3. Execution Model

1) Build DAG from `depends_on`.
2) Determine frontier batches. Within a batch:
   - Sequential steps: those without `parallel_group` are executed in their own batch to preserve ordering.
   - Parallel groups: steps sharing `parallel_group` are run concurrently (thread pool sized by `max_parallel_workers`).
3) Each step:
   - Loads SQL, optionally registers previous outputs as temp views, replaces placeholders, executes via connector.
   - Optional auto-caching based on estimated size.
4) After all batches, write declared `final_outputs` using connector.
5) Update job status and stop the Spark session.

Restart: when `--restart` is set, completed and failed steps are fetched from `JobStatusTracker` and the DAG is re-batched to skip completed work.

---

## 4. Local Development Workflow

- Prereqs: Python 3.11–3.13, Java + Spark 3.4.4.
- Create venv and install dev deps:
  - `make install-dev`
- Inner loop:
  - Lint: `flake8 transform_service tests`
  - Type-check: `mypy transform_service`
  - Tests: `pytest -q` (use `-k` to select tests)
- Example runs:
  - Dry run: `make demo-dry-run`
  - Full local run: export `TRINO_JDBC_JAR` and `make demo-run`

---

## 5. Testing Strategy

- Unit tests mock Spark and Starburst connectors; CI does not need a cluster.
- Use `pytest` fixtures in `tests/conftest.py` to centralize mocks and sample configs.
- Coverage is configured in `pyproject.toml`. Generate HTML coverage with `pytest --cov-report=html`.
- Multi-version testing via `tox` (py311, py312, py313).

---

## 6. Code Quality and Tooling

Configuration lives in `pyproject.toml` to centralize tooling.

- Flake8 (`[tool.flake8]`)
  - `max-line-length = 120`
  - Ignores `E203`, `W503` to be black-compatible if desired.
  - Run: `flake8 transform_service tests`

- Mypy (`[tool.mypy]`)
  - Targets Python 3.11 type system.
  - `warn_return_any`, `no_implicit_optional`, `warn_unused_ignores` for stricter correctness.
  - Run: `mypy transform_service`

- Pytest (`[tool.pytest.ini_options]`)
  - `-q` for quiet, coverage enabled by default.
  - `testpaths = ["tests"]` so `pytest` runs from repo root.

- Makefile
  - Provides stable entry points for CI and developers.
  - Abstracts venv activation, ensuring consistent tool versions.

- Tox (`tox.ini`)
  - Runs tests across multiple Python versions.
  - Can be used locally (`pip install tox`) or in CI matrices.

---

## 7. Design Principles and Best Practices

- Single Responsibility
  - Each module does one thing well (pipeline, steps, connectors, config, status, logging).

- Dependency Inversion
  - Pipeline depends on connector interfaces. Connector details (JDBC specifics) are isolated and can be mocked.

- Pure Functions where possible
  - Steps return DataFrames and avoid global state; `execution_context` is explicit.

- Explicit Configuration
  - YAML + Pydantic models document and validate configuration points.

- Deterministic Logs
  - JSON logs with `job_id` to trace multi-step runs and correlate across systems.

- Safe Parallelism
  - Batching + named `parallel_group` to avoid uncontrolled concurrency.

- Restartability
  - `JobStatusTracker` enables idempotence and recovery; persistence can be added without changing the orchestrator.

- Testability
  - Layered design with mocks for Spark/Starburst yields fast tests; CI does not need a cluster.

- Backward Compatibility
  - `TransformationPipeline._call_step_execute` supports older two-argument `execute(connector, context)` signatures used in some tests.

---

## 8. Project Conventions

- File/Folder Layout
  - `transform_service/`: library code
  - `configs/`: sample or default configs
  - `sql/`: SQL sources
  - `examples/`: runnable example configs and SQL
  - `scripts/`: helper shell scripts
  - `tests/`: pytest suite

- Naming
  - Steps are kebab/snake-case names aligned with SQL filenames.
  - SQL filenames are zero-padded numeric prefixes to reflect logical ordering in examples.

- Exceptions
  - Fail fast with clear messages (e.g., missing SQL file raises `FileNotFoundError`).
  - The pipeline logs and propagates exceptions; tracker marks failures.

- Type Hints
  - Use modern annotations (`from __future__ import annotations`).

---

## 9. Extending the System

- New Step Types
  - Create a subclass of `TransformationStep` or add method hooks for non-SQL (e.g., UDF preparation, Python transforms).

- Alternative Connectors
  - Implement a connector with `read_sql(sql) -> DataFrame` and `write_table(df, table, mode)`.

- Persistent Job Tracking
  - Implement `JobStatusTracker` with durable storage; wire via config under `job_tracking`.

- Advanced Catalogs
  - Adjust Spark configs for Iceberg/HMS/Glue; set them via `connection.yaml`.

---

## 10. Frequently Used Commands

- Bootstrap dev env:
  - `make install-dev`
- Run linters and types:
  - `flake8 transform_service tests && mypy transform_service`
- Run tests with coverage:
  - `pytest -q`
- Validate example pipeline without execution:
  - `make demo-dry-run`
- Run end-to-end example locally (needs JDBC jar):
  - `make demo-run`

---

## 11. Migration from Single .py Scripts

- Break logic into modules:
  - CLI/argparse → `cli.py`
  - Orchestration/DAG → `pipeline.py`
  - Individual units of work → `steps.py` + SQL files
  - I/O → `connectors.py`
  - Config parsing/validation → `config.py` + `schemas.py`
  - Observability → `logging_utils.py`, `status.py`

- Externalize SQL from code to versioned files under `sql/`.
- Replace ad-hoc global variables with explicit config and `execution_context`.
- Introduce tests for each module; mock external systems.
- Adopt flake8/mypy to keep quality high as complexity grows.

---

## 12. Notes on Performance Module

- `PerformanceTracker.track_step_performance` handles environments without PySpark by returning `{step_name, error}`.
- `_get_spark_metrics` imports `pyspark` lazily and safely checks status tracker methods.
- `DataSizeEstimator` returns a clear message when PySpark is unavailable and falls back gracefully on errors.

