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
    use_temp_views: bool = False


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
