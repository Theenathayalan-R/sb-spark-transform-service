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
        # Honor explicit scheme
        scheme = str(self.config.get("scheme", "")).lower()
        if scheme == "https":
            params.append("SSL=true")
        elif scheme == "http":
            params.append("SSL=false")
        else:
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
        props: Dict[str, str] = {
            "user": self.config.get("user", ""),
            "password": self.config.get("password", ""),
            "driver": "io.trino.jdbc.TrinoDriver",
            "fetchsize": str(self.config.get("fetchsize", 10000)),
            "batchsize": str(self.config.get("batchsize", 10000)),
        }

        # Map verify setting
        verify = self.config.get("verify")
        if isinstance(verify, bool):
            if not verify:
                props["SSLVerification"] = "NONE"
        elif isinstance(verify, str):
            v = verify.strip()
            if v.lower() in {"full", "ca", "none"}:
                props["SSLVerification"] = v.upper()
            elif v.lower() == "system":
                props["SSLUseSystemTrustStore"] = "true"
            else:
                # Assume it's a path to a CA bundle or trust store
                props["SSLTrustStorePath"] = v
                # Infer type
                low = v.lower()
                if low.endswith(".jks"):
                    props["SSLTrustStoreType"] = "JKS"
                elif low.endswith(".p12") or low.endswith(".pfx"):
                    props["SSLTrustStoreType"] = "PKCS12"
                elif low.endswith(".pem") or low.endswith(".crt") or low.endswith(".cer"):
                    # Trino JDBC accepts PEM for KeyStore/TrustStore path when type PEM
                    props["SSLTrustStoreType"] = "PEM"
        # If a truststore password is provided in config, pass it along
        trust_pw = self.config.get("ssl_trust_store_password")
        if isinstance(trust_pw, str) and trust_pw:
            props["SSLTrustStorePassword"] = trust_pw

        return props

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

    # --- Type Harmonization helpers -------------------------------------------------
    def _get_type_harmonization_settings(self) -> Dict[str, Any]:
        """Return toggles and parameters for automatic type harmonization.
        All values have safe defaults so no config changes are required.
        """
        return {
            "enabled": bool(self.config.get("auto_cast_types", False)),
            "sample_rows": int(self.config.get("type_inference_sample_rows", 1000)),
            "threshold": float(self.config.get("type_inference_threshold", 0.9)),
            # If true, normalize parsed timestamps to UTC; otherwise keep as naive timestamp
            "normalize_to_utc": bool(self.config.get("normalize_timestamp_to_utc", False)),
            # Session tz used when normalizing timestamps to UTC
            "session_tz": str(self.config.get("session_time_zone", "UTC")),
            # Broader conversions toggles and hints
            "broad_enabled": bool(self.config.get("auto_cast_broad_types", False)),
            "null_sentinels": [str(x).lower() for x in self.config.get("null_sentinels", ["", "null", "NULL", "NaN", "N/A"])],
            "bool_true": [str(x).lower() for x in self.config.get("boolean_true_values", ["true", "1", "y", "yes", "t"])],
            "bool_false": [str(x).lower() for x in self.config.get("boolean_false_values", ["false", "0", "n", "no", "f"])],
            "date_formats": list(self.config.get("date_inference_formats", [
                "yyyy-MM-dd",
                "MM/dd/yyyy",
                "dd/MM/yyyy",
                "yyyyMMdd",
            ])),
            "ts_formats": list(self.config.get("timestamp_inference_formats", [
                "yyyy-MM-dd HH:mm:ss.SSS",
                "yyyy-MM-dd HH:mm:ss",
                "yyyy-MM-dd'T'HH:mm:ss.SSS",
                "yyyy-MM-dd'T'HH:mm:ss",
            ])),
            "decimal_max_scale": int(self.config.get("decimal_max_scale", 6)),
            "decimal_fallback_to_double": bool(self.config.get("decimal_fallback_to_double", True)),
        }

    def _infer_timestamp_string_columns(self, df: Any, sample_rows: int, threshold: float) -> Dict[str, str]:
        """Heuristically find string columns that are actually timestamps with tz.
        Returns a map of column -> best matching timestamp pattern.
        """
        try:
            from pyspark.sql import functions as F, types as T  # type: ignore
        except Exception:
            return {}

        patterns = [
            "yyyy-MM-dd'T'HH:mm:ss.SSSXXX",
            "yyyy-MM-dd'T'HH:mm:ssXXX",
            "yyyy-MM-dd HH:mm:ss.SSS XXX",
            "yyyy-MM-dd HH:mm:ss XXX",
            "yyyy-MM-dd'T'HH:mm:ss.SSSX",
            "yyyy-MM-dd'T'HH:mm:ssX",
        ]

        sdf = df.limit(int(sample_rows)) if sample_rows and sample_rows > 0 else df
        cast_map: Dict[str, str] = {}
        for field in df.schema.fields:
            # Only consider strings (Spark doesn't carry varchar/char separately)
            try:
                if not isinstance(field.dataType, T.StringType):
                    continue
            except Exception:
                continue
            c = field.name
            try:
                total_non_null = (
                    (sdf.agg((F.sum(F.when(F.col(c).isNotNull(), F.lit(1)).otherwise(F.lit(0)))).alias("total")))
                    .collect()[0]["total"]
                )
            except Exception:
                # If aggregation fails for any reason, skip detection for this column
                continue
            if not total_non_null:
                continue
            best_count = -1
            best_pattern = None
            for p in patterns:
                try:
                    cnt = (
                        sdf.agg(
                            (
                                F.sum(
                                    F.when(F.to_timestamp(F.col(c), p).isNotNull(), F.lit(1)).otherwise(F.lit(0))
                                )
                            ).alias("cnt")
                        ).collect()[0]["cnt"]
                    )
                except Exception:
                    cnt = 0
                if cnt is not None and cnt > best_count:
                    best_count = int(cnt)
                    best_pattern = p
            if best_count > 0 and (best_count / float(total_non_null)) >= threshold and best_pattern:
                cast_map[c] = best_pattern
        return cast_map

    def _infer_broad_string_columns(self, df: Any, settings: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
        """Infer broader conversions for string columns: boolean, integer, float/decimal, date, timestamp.
        Returns a map: column -> { kind: one of [bool,int,float,date,timestamp], fmt?: str, scale?: int }
        """
        try:
            from pyspark.sql import functions as F, types as T  # type: ignore
        except Exception:
            return {}

        if not settings.get("broad_enabled"):
            return {}

        sample_rows = int(settings.get("sample_rows", 1000))
        threshold = float(settings.get("threshold", 0.9))
        sentinels = set([s.lower() for s in settings.get("null_sentinels", [])])
        true_vals = set([s.lower() for s in settings.get("bool_true", [])])
        false_vals = set([s.lower() for s in settings.get("bool_false", [])])
        date_formats = list(settings.get("date_formats", []))
        ts_formats = list(settings.get("ts_formats", []))

        sdf = df.limit(int(sample_rows)) if sample_rows and sample_rows > 0 else df
        result: Dict[str, Dict[str, Any]] = {}

        for field in df.schema.fields:
            try:
                if not isinstance(field.dataType, T.StringType):
                    continue
            except Exception:
                continue
            c = field.name
            lc = F.lower(F.trim(F.col(c)))
            # Cleaned column treating sentinel values as null
            c_clean = F.when(lc.isin(list(sentinels)), F.lit(None)).otherwise(F.col(c))

            # Total non-null after cleaning
            try:
                total_non_null = (
                    sdf.select(c_clean.alias("v")).agg((F.sum(F.when(F.col("v").isNotNull(), F.lit(1)).otherwise(F.lit(0)))).alias("total")).collect()[0]["total"]
                )
            except Exception:
                continue
            if not total_non_null:
                continue

            best_kind = None
            best_score = -1.0
            best_extra: Dict[str, Any] = {}

            # Boolean
            try:
                bool_match = (
                    sdf.select(lc.alias("v"))
                    .agg((F.sum(F.when(F.col("v").isin(list(true_vals | false_vals)), F.lit(1)).otherwise(F.lit(0)))).alias("cnt"))
                    .collect()[0]["cnt"]
                )
                score = (bool_match or 0) / float(total_non_null)
                if score >= threshold and score > best_score:
                    best_kind = "bool"
                    best_score = score
                    best_extra = {}
            except Exception:
                pass

            # Integer (bigint)
            try:
                # regex for integer
                int_match = (
                    sdf.select(lc.alias("v"))
                    .agg((F.sum(F.when(F.col("v").rlike(r"^[+-]?\d+$"), F.lit(1)).otherwise(F.lit(0)))).alias("cnt"))
                    .collect()[0]["cnt"]
                )
                score = (int_match or 0) / float(total_non_null)
                if score >= threshold and score > best_score:
                    best_kind = "int"
                    best_score = score
                    best_extra = {}
            except Exception:
                pass

            # Float/Decimal
            try:
                float_match = (
                    sdf.select(lc.alias("v"))
                    .agg((F.sum(F.when(F.col("v").rlike(r"^[+-]?(?:\d+\.\d*|\d*\.\d+|\d+)(?:[eE][+-]?\d+)?$"), F.lit(1)).otherwise(F.lit(0)))).alias("cnt"))
                    .collect()[0]["cnt"]
                )
                score = (float_match or 0) / float(total_non_null)
                if score >= threshold and score > best_score:
                    best_kind = "float"
                    best_score = score
                    # Estimate scale up to max
                    scale = 0
                    try:
                        # get max of decimals after dot in sample
                        dec_cnt = (
                            sdf.select(lc.alias("v"))
                            .select(F.regexp_extract(F.col("v"), r"^[+-]?(?:\d*\.(\d+))", 1).alias("d"))
                            .agg(F.max(F.length(F.col("d"))).alias("mx"))
                            .collect()[0]["mx"]
                        )
                        if dec_cnt is not None:
                            scale = int(dec_cnt)
                    except Exception:
                        scale = 0
                    best_extra = {"scale": min(int(settings.get("decimal_max_scale", 6)), int(scale or 0))}
            except Exception:
                pass

            # Date
            try:
                best_date_cnt = -1
                best_date_fmt = None
                for fmt in date_formats:
                    cnt = (
                        sdf.select(F.to_date(c_clean, fmt).alias("d"))
                        .agg((F.sum(F.when(F.col("d").isNotNull(), F.lit(1)).otherwise(F.lit(0)))).alias("cnt"))
                        .collect()[0]["cnt"]
                    )
                    if cnt is not None and cnt > best_date_cnt:
                        best_date_cnt = int(cnt)
                        best_date_fmt = fmt
                score = (best_date_cnt or 0) / float(total_non_null)
                if best_date_fmt and score >= threshold and score > best_score:
                    best_kind = "date"
                    best_score = score
                    best_extra = {"fmt": best_date_fmt}
            except Exception:
                pass

            # Timestamp (no timezone)
            try:
                best_ts_cnt = -1
                best_ts_fmt = None
                for fmt in ts_formats:
                    cnt = (
                        sdf.select(F.to_timestamp(c_clean, fmt).alias("t"))
                        .agg((F.sum(F.when(F.col("t").isNotNull(), F.lit(1)).otherwise(F.lit(0)))).alias("cnt"))
                        .collect()[0]["cnt"]
                    )
                    if cnt is not None and cnt > best_ts_cnt:
                        best_ts_cnt = int(cnt)
                        best_ts_fmt = fmt
                score = (best_ts_cnt or 0) / float(total_non_null)
                if best_ts_fmt and score >= threshold and score > best_score:
                    best_kind = "timestamp"
                    best_score = score
                    best_extra = {"fmt": best_ts_fmt}
            except Exception:
                pass

            if best_kind:
                result[c] = {"kind": best_kind, **best_extra}

        return result

    def _harmonize_types(self, df: Any) -> Any:
        """Apply general, column-agnostic casts:
        - Strings that look like TIMESTAMP WITH TIME ZONE -> cast to naive TimestampType
          (optionally normalize to UTC when enabled)
        - Optional broad casting of strings to booleans, integers, floats/decimals, date, timestamp
        - Leave other types as-is (varchar/char already come as StringType)
        """
        try:
            from pyspark.sql import functions as F, types as T  # type: ignore
        except Exception:
            return df

        settings = self._get_type_harmonization_settings()
        if not settings["enabled"]:
            return df

        # Detect which string columns should be treated as timestamps with timezone
        tz_cast_map = self._infer_timestamp_string_columns(
            df, settings["sample_rows"], settings["threshold"]
        )
        # Detect broader types if enabled
        broad_cast_map = self._infer_broad_string_columns(df, settings) if settings.get("broad_enabled") else {}

        if not tz_cast_map and not broad_cast_map:
            return df

        projected_cols = []
        for c in df.columns:
            if c in tz_cast_map:
                ts = F.to_timestamp(F.col(c), tz_cast_map[c])
                if settings["normalize_to_utc"]:
                    ts = F.to_utc_timestamp(ts, settings["session_tz"])
                projected_cols.append(ts.alias(c))
            elif c in broad_cast_map:
                info = broad_cast_map[c]
                kind = info.get("kind")
                if kind == "bool":
                    lc = F.lower(F.trim(F.col(c)))
                    true_vals = settings.get("bool_true", [])
                    false_vals = settings.get("bool_false", [])
                    casted = F.when(lc.isin(true_vals), F.lit(True))\
                             .when(lc.isin(false_vals), F.lit(False))\
                             .otherwise(F.lit(None))
                    projected_cols.append(casted.cast("boolean").alias(c))
                elif kind == "int":
                    projected_cols.append(F.col(c).cast("bigint").alias(c))
                elif kind == "float":
                    scale = int(info.get("scale", 0))
                    if settings.get("decimal_fallback_to_double", True):
                        projected_cols.append(F.col(c).cast("double").alias(c))
                    else:
                        try:
                            from pyspark.sql.types import DecimalType  # type: ignore
                            projected_cols.append(F.col(c).cast(DecimalType(38, scale)).alias(c))
                        except Exception:
                            projected_cols.append(F.col(c).cast("double").alias(c))
                elif kind == "date":
                    fmt = info.get("fmt")
                    projected_cols.append(F.to_date(F.col(c), fmt).alias(c))
                elif kind == "timestamp":
                    fmt = info.get("fmt")
                    ts = F.to_timestamp(F.col(c), fmt)
                    if settings["normalize_to_utc"]:
                        ts = F.to_utc_timestamp(ts, settings["session_tz"])
                    projected_cols.append(ts.alias(c))
                else:
                    projected_cols.append(F.col(c))
            else:
                projected_cols.append(F.col(c))
        return df.select(*projected_cols)

    # -------------------------------------------------------------------------------

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

        # Auto type harmonization (optional, safe defaults)
        try:
            df = self._harmonize_types(df)
        except Exception:
            # Best-effort; never fail pipeline due to harmonization
            pass

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
