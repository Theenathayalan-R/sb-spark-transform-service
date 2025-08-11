from __future__ import annotations

import logging
import time
from typing import Any, Dict, Optional

try:
    from pyspark.sql.functions import current_timestamp  # type: ignore[import-untyped]
except ImportError:
    # Stub for testing environments
    def current_timestamp():  # type: ignore
        return "mock_timestamp"

logger = logging.getLogger(__name__)


class ConnectionPool:
    """Simple connection pool for JDBC connections."""

    def __init__(self, max_connections: int = 10):
        self.max_connections = max_connections
        self.active_connections = 0
        self._connection_semaphore = None

    def acquire_connection(self):
        """Acquire a connection from the pool."""
        if self._connection_semaphore is None:
            import threading
            self._connection_semaphore = threading.Semaphore(self.max_connections)

        self._connection_semaphore.acquire()
        self.active_connections += 1
        logger.debug(f"Acquired connection. Active: {self.active_connections}/{self.max_connections}")

    def release_connection(self):
        """Release a connection back to the pool."""
        if self._connection_semaphore:
            self._connection_semaphore.release()
            self.active_connections = max(0, self.active_connections - 1)
            logger.debug(f"Released connection. Active: {self.active_connections}/{self.max_connections}")


class StarburstConnector:
    def __init__(self, config: Dict[str, Any], spark: Any):
        self.config = config
        self.spark = spark
        self.jdbc_url = self._build_jdbc_url()
        self.connection_properties = self._build_connection_properties()
        self.log_counts = bool(self.config.get("log_counts", False))
        self.max_retries = int(self.config.get("max_retries", 0))
        self.retry_backoff_seconds = float(self.config.get("retry_backoff_seconds", 1.0))

        # Connection pooling
        pool_size = int(self.config.get("connection_pool_size", 10))
        self.connection_pool = ConnectionPool(pool_size)

    def _build_jdbc_url(self) -> str:
        host = self.config.get("host", "")
        port = self.config.get("port", 443)
        catalog = self.config.get("catalog", "")
        schema = self.config.get("schema", self.config.get("schema_", ""))
        # Preserve existing behavior: catalog + schema in URL
        url = f"jdbc:trino://{host}:{port}/{catalog}/{schema}"
        params = []
        if self.config.get("ssl", True):
            params.append("SSL=true")
        if self.config.get("application_name"):
            params.append(
                f"applicationNamePrefix={self.config['application_name']}"
            )
        if params:
            url += "?" + "&".join(params)
        return url

    def _build_connection_properties(self) -> Dict[str, str]:
        return {
            "user": self.config.get("user", ""),
            "password": self.config.get("password", ""),
            "driver": "io.trino.jdbc.TrinoDriver",
            "fetchsize": str(self.config.get("fetchsize", 10000)),
            "batchsize": str(self.config.get("batchsize", 10000)),
        }

    def _with_retries(self, func, *args, **kwargs):
        attempt = 0
        last_exc = None

        # Acquire connection from pool
        self.connection_pool.acquire_connection()

        try:
            while attempt <= self.max_retries:
                try:
                    return func(*args, **kwargs)
                except Exception as e:  # noqa: BLE001
                    last_exc = e
                    if attempt == self.max_retries:
                        break
                    sleep_for = self.retry_backoff_seconds * (2 ** attempt)
                    logger.warning(
                        "Retrying after error (attempt %d/%d) in %.2fs: %s",
                        attempt + 1,
                        self.max_retries + 1,
                        sleep_for,
                        e,
                    )
                    time.sleep(sleep_for)
                    attempt += 1
        finally:
            # Always release connection back to pool
            self.connection_pool.release_connection()

        assert last_exc is not None
        raise last_exc

    def read_sql(self, sql_query: str) -> Any:
        clean_query = sql_query.strip().rstrip(";")

        def _do_read():
            return self.spark.read.jdbc(
                url=self.jdbc_url,
                table=f"({clean_query}) as query_result",
                properties=self.connection_properties,
            )

        df = self._with_retries(_do_read)
        # Avoid counting here to reduce overhead; counts can be gathered by PerformanceTracker
        logger.info("Executed query")
        return df

    def write_table(self, df: Any, table_name: str, mode: str = "overwrite") -> None:
        # Add metadata timestamp
        df_with_metadata = df
        try:
            df_with_metadata = df.withColumn("_processed_at", current_timestamp())
        except Exception:
            df_with_metadata = df

        # URL already includes schema, so default to plain table name unless schema-qualified is provided
        full_table_name = table_name

        def _do_write():
            return df_with_metadata.write.jdbc(
                url=self.jdbc_url,
                table=full_table_name,
                mode=mode,
                properties=self.connection_properties,
            )

        self._with_retries(_do_write)
        logger.info("Wrote to %s", full_table_name)

    def cache_dataframe(self, df: Any, storage_level: Optional[str] = None) -> Any:
        """Cache DataFrame with intelligent storage level selection."""
        if storage_level is None:
            storage_level = "MEMORY_AND_DISK"

        # Use basic caching - in production with proper PySpark setup, this can be enhanced
        if hasattr(df, 'cache'):
            cached_df = df.cache()
            logger.info(f"Cached DataFrame with basic cache (requested: {storage_level})")
            return cached_df
        return df
