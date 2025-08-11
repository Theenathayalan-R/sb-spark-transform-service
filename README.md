# Transform Service (PySpark + Starburst/Trino)

A comprehensive and robust configuration-driven data transformation service built on PySpark 3.4.4 and Starburst/Trino via JDBC.

- Python: 3.11–3.13
- Spark: 3.4.4
- JDBC: Trino/Starburst

## Highlights
- Config-driven pipelines
- Dependency-aware execution with parallel groups
- Restart from completed steps (pluggable tracker)
- JSON logging with auto-injected job_id
- Performance tracking and auto-caching heuristics
- Thorough unit tests that mock Spark/Starburst

## Requirements
- Python 3.11, 3.12, or 3.13
- Java + Spark 3.4.4 (for real execution)
- Trino/Starburst JDBC JAR (for Starburst I/O)

## Installation

Create a virtual environment and install the package (dev extras optional):

```bash
python -m venv .venv
source .venv/bin/activate
pip install -U pip
# Library only
pip install -e .
# Or include dev tools (pytest, flake8, mypy, etc.)
pip install -e .[dev]
```

## Configuration

You can provide configuration in two ways:

1) Split YAML files (recommended)
- Pipeline: `configs/pipeline.yaml`
- Connection: `configs/connection.yaml`

2) Unified YAML file containing both pipeline and connection sections

Environment variables in YAML are expanded using `${VAR_NAME}`.

### Example: connection.yaml
```yaml
starburst:
  host: "coordinator.example"
  port: 443         # defaults to 443 if omitted
  catalog: "hive"
  schema: "analytics"
  user: ${STARBURST_USER}
  password: ${STARBURST_PASSWORD}
  # JDBC tuning
  connection_pool_size: 10       # simple connection pool in connector
  fetchsize: 50000
  batchsize: 50000
  max_retries: 3                 # connector-level retries
  retry_backoff_seconds: 1.0
  # Optional: let steps register temp views for placeholders
  use_temp_views: false

spark:
  app_name: "TransformService"
  # jdbc_jar_path: "/path/to/trino-jdbc-xxx.jar"   # optionally set here
  iceberg: true
  configs:
    spark.sql.shuffle.partitions: "8"

job_tracking:
  enabled: true
  table: "job_status"   # logical name if you implement persistence
```

### Example: pipeline.yaml
```yaml
sql_base_path: "sql"
# optional execution-wide settings
max_parallel_workers: 4
step_timeout_seconds: 0   # omit or set >0 to enforce per-step timeout

transformations:
  - name: load_customers
    sql_file: 01_load_customers.sql
    depends_on: []
  - name: load_orders
    sql_file: 02_load_orders.sql
    depends_on: []
    parallel_group: g1
  - name: clean_orders
    sql_file: 05_clean_orders.sql
    depends_on: [load_orders]
    # enable automatic, size-aware caching for this step
    cache_strategy: auto
  - name: customer_metrics
    sql_file: 06_customer_metrics.sql
    depends_on: [load_customers, clean_orders]

final_outputs:
  customer_metrics:
    table: analytics.customer_metrics
    mode: overwrite
```

### Unified YAML
If preferred, you can merge both into one YAML. The loader validates with Pydantic and merges accordingly.

### Placeholders
Within SQL, you may reference previous step outputs as placeholders:

```sql
SELECT *
FROM ${clean_orders} o
JOIN ${load_customers} c ON o.customer_id = c.customer_id;
```

The engine replaces `${step_name}` with a temp identifier before execution. If `use_temp_views: true` is enabled in `starburst` config and Spark is available, prior step DataFrames are also registered as `temp_{step}` views for more robust SQL resolution.

## SQL Files
Place transformation SQL under the `sql/` directory (or override `sql_base_path`). Sample files are provided in `sql/`.

### Examples
A minimal, runnable example is provided under `examples/`:
- `examples/pipeline.yaml`
- `examples/connection.yaml`
- `examples/sql/*.sql`

Run a dry-run of the example:
```bash
transform-service \
  --pipeline-config examples/pipeline.yaml \
  --connection-config examples/connection.yaml \
  --dry-run
```

Run the example with Spark (requires JDBC jar):
```bash
export TRINO_JDBC_JAR=/path/to/trino-jdbc-xxx.jar
scripts/run_local.sh examples/pipeline.yaml examples/connection.yaml
```

## Running

Set credentials in your shell (example):
```bash
export STARBURST_USER=your_user
export STARBURST_PASSWORD=your_password
```

Set (or export) the Trino JDBC JAR for Spark to load:
```bash
export TRINO_JDBC_JAR=/path/to/trino-jdbc-xxx.jar
```

### Dry run (validates dependency order only)
This does not execute queries and can run without a real Spark cluster.
```bash
transform-service \
  --pipeline-config configs/pipeline.yaml \
  --connection-config configs/connection.yaml \
  --dry-run
```

### Local run (Spark execution)
Use the helper script (wraps spark-submit):
```bash
scripts/run_local.sh configs/pipeline.yaml configs/connection.yaml
```

Or call spark-submit directly:
```bash
spark-submit \
  --master local[4] \
  --jars "$TRINO_JDBC_JAR" \
  --driver-class-path "$TRINO_JDBC_JAR" \
  transform_service/cli.py \
  --pipeline-config configs/pipeline.yaml \
  --connection-config configs/connection.yaml
```

Command-line options:
- `--config` Use a unified YAML instead of split files
- `--pipeline-config` and `--connection-config` Use split YAMLs
- `--dry-run` Validate only
- `--restart` Restart from the last successful step if your tracker persists status
- `--job-id` Provide a job identifier injected into all logs
- `--log-level` CRITICAL|ERROR|WARNING|INFO|DEBUG (default INFO)

## Performance and Caching
- Step-level metrics captured: execution time, row count, partition count, memory usage, Spark job stats.
- When `cache_strategy: auto` is set on a step, the pipeline estimates DataFrame size and automatically persists it with an appropriate storage level.
- Data size estimation gracefully returns `{'estimated_rows': 0, 'estimated_size_mb': 0, 'error': 'PySpark not available'}` when PySpark isn’t present.
- Spark metrics are obtained only when an active SparkContext is available.

## Connector: Pooling and Retries
The Starburst connector provides:
- Simple connection pooling (`connection_pool_size`)
- Tunable fetch/batch sizes (`fetchsize`, `batchsize`)
- Optional retries (`max_retries`, `retry_backoff_seconds`)
- Minimal overhead by avoiding implicit counts (`log_counts` defaults to false behavior)

## Logging
Structured JSON logs are enabled with `python-json-logger`. Each log includes a `job_id` field for traceability.

## Status Tracking
`transform_service.status.JobStatusTracker` is a simple, pluggable tracker. By default, it logs to stdout during tests. You can implement persistence (JDBC, Iceberg) by extending the tracker methods.

## Parallelism and Dependencies
- The pipeline builds a DAG from `depends_on` lists.
- Steps without a `parallel_group` run sequentially in their turn.
- Steps sharing the same `parallel_group` run concurrently within a batch.
- `max_parallel_workers` controls thread pool size for parallel batches.
- Optional `step_timeout_seconds` enforces a per-step timeout.

## Testing
Run all tests with coverage:
```bash
pytest -q
```

Common options:
```bash
pytest -q --cov=transform_service --cov-report=term-missing
pytest --cov-report=html && open htmlcov/index.html
```

## Troubleshooting
- Missing JDBC driver: ensure `TRINO_JDBC_JAR` is exported or set `spark.jdbc_jar_path` in config.
- Python JSON logger: we import the new module path; if unavailable, we fallback to basic logging.
- Spark config: tune via `spark.configs` in connection YAML.

## Project Layout
```
transform_service/
  __init__.py
  cli.py
  config.py
  connectors.py
  logging_utils.py
  pipeline.py
  schemas.py
  status.py
  steps.py
configs/
  pipeline.yaml
  connection.yaml
scripts/
  run_local.sh
sql/
  01_load_customers.sql
  02_load_orders.sql
  03_load_products.sql
  04_clean_customers.sql
  05_clean_orders.sql
  06_customer_metrics.sql
  07_product_metrics.sql
  08_final_report.sql
examples/
  pipeline.yaml
  connection.yaml
  sql/
    01_load_customers.sql
    02_load_orders.sql
    03_join_customers_orders.sql
```

## For Developers
- Modular, testable design. Core modules: `cli.py` (entry), `pipeline.py` (DAG + execution), `steps.py` (SQL step abstraction), `connectors.py` (Starburst I/O), `config.py` (YAML + Pydantic validation), `schemas.py` (typed models), `status.py` (job/step tracking), `logging_utils.py` (JSON logging with `job_id`).
- Quality gates and tooling:
  - Lint: `flake8` (configured in `pyproject.toml`).
  - Types: `mypy` (see `[tool.mypy]`).
  - Tests: `pytest` with coverage (see `[tool.pytest.ini_options]`).
  - Multi-Python: `tox` runs tests on 3.11–3.13.
- Makefile shortcuts (see `Makefile`):
  - `make venv` create venv; `make install` library; `make install-dev` with dev tools.
  - `make test` run `pytest` via the venv.
  - `make demo-dry-run` and `make demo-run` to validate/run the example.
- Recommended local workflow:
  1) `make install-dev`
  2) `flake8 transform_service tests`
  3) `mypy transform_service`
  4) `pytest -q`
- Detailed technical design and best practices: see `docs/DEVELOPER_GUIDE.md` and `docs/PERFORMANCE_GUIDE.md`.

## License
Apache-2.0
