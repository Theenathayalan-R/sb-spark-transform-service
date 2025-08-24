from __future__ import annotations

from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional


class SparkConfig(BaseModel):
    app_name: str = "TransformationPipeline"
    jdbc_jar_path: Optional[str] = None
    iceberg: bool = True
    configs: Dict[str, str] = Field(default_factory=dict)


class StarburstConfig(BaseModel):
    host: str
    port: int = 443
    user: str
    password: str
    catalog: str
    schema_: str = Field(..., alias="schema")
    # logging / retry options
    log_counts: bool = False
    max_retries: int = 0
    retry_backoff_seconds: float = 1.0
    # connection & behavior options
    connection_pool_size: int = 10
    fetchsize: int = 10000
    batchsize: int = 10000
    application_name: Optional[str] = None
    ssl: bool = True
    # Optional: explicitly declare HTTP scheme; https implies SSL=true, http implies SSL=false
    scheme: Optional[str] = None
    # Optional: verification setting
    # - boolean false disables verification (maps to SSLVerification=NONE)
    # - string FULL|CA|NONE selects verification mode
    # - string 'system' uses system trust store
    # - path to a CA bundle (.pem/.crt etc.) or Java truststore (.jks/.p12)
    verify: Optional[Any] = None
    use_temp_views: bool = False
    # --- Automatic type harmonization (optional) ---
    auto_cast_types: bool = False
    type_inference_sample_rows: int = 1000
    type_inference_threshold: float = 0.9
    # Session time zone to use when normalizing timestamps
    session_time_zone: str = "UTC"
    # Normalize timestamps to UTC when parsing; kept optional and off by default
    normalize_timestamp_to_utc: bool = False
    # Broader conversions (beyond timestamp parsing). When enabled, the connector will
    # try to coerce common string representations into typed columns (date, boolean,
    # integer/long, double/decimal) using heuristic sampling with the same threshold.
    auto_cast_broad_types: bool = False
    # Treat these string values as nulls during inference/casting (case-insensitive)
    null_sentinels: List[str] = Field(
        default_factory=lambda: ["", "null", "NULL", "NaN", "N/A"]
    )
    # Boolean parsing configuration (values are compared case-insensitively after trim)
    boolean_true_values: List[str] = Field(
        default_factory=lambda: ["true", "1", "y", "yes", "t"]
    )
    boolean_false_values: List[str] = Field(
        default_factory=lambda: ["false", "0", "n", "no", "f"]
    )
    # Date and timestamp format hints for inference (Spark SimpleDateFormat patterns)
    date_inference_formats: List[str] = Field(
        default_factory=lambda: [
            "yyyy-MM-dd",
            "MM/dd/yyyy",
            "dd/MM/yyyy",
            "yyyyMMdd",
        ]
    )
    timestamp_inference_formats: List[str] = Field(
        default_factory=lambda: [
            "yyyy-MM-dd HH:mm:ss.SSS",
            "yyyy-MM-dd HH:mm:ss",
            "yyyy-MM-dd'T'HH:mm:ss.SSS",
            "yyyy-MM-dd'T'HH:mm:ss",
        ]
    )
    # Decimal casting options
    decimal_max_scale: int = 6
    decimal_fallback_to_double: bool = True


class JobTrackingConfig(BaseModel):
    enabled: bool = True
    status_table: str = "job_execution_status"
    step_status_table: str = "step_execution_status"
    persist: bool = False


class TransformationStepConfig(BaseModel):
    name: str
    sql_file: str
    type: str = "spark_sql"
    depends_on: List[str] = Field(default_factory=list)
    parallel_group: Optional[str] = None
    placeholders: Dict[str, Any] = Field(default_factory=dict)
    write_options: Dict[str, Any] = Field(default_factory=dict)
    cache_strategy: str = "never"


class FinalOutputConfig(BaseModel):
    table: str
    mode: str = "overwrite"


class PipelineConfig(BaseModel):
    sql_base_path: str = "sql"
    transformations: List[TransformationStepConfig]
    final_outputs: Dict[str, FinalOutputConfig] = Field(default_factory=dict)
    max_parallel_workers: int = 4
    step_timeout_seconds: Optional[float] = None


class ConnectionConfig(BaseModel):
    spark: SparkConfig = Field(default_factory=SparkConfig)
    starburst: StarburstConfig
    job_tracking: JobTrackingConfig = Field(default_factory=JobTrackingConfig)


class UnifiedConfig(PipelineConfig, ConnectionConfig):
    pass
