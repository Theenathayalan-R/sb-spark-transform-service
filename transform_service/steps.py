from __future__ import annotations

import os
import logging
from typing import Dict, Optional, Any

try:
    from pyspark.sql import DataFrame
    HAS_PYSPARK = True  # For backwards compatibility with tests
except ImportError:
    DataFrame = Any  # type: ignore
    HAS_PYSPARK = False  # For backwards compatibility with tests

logger = logging.getLogger(__name__)


class TransformationStep:
    """Represents a single SQL transformation step.

    Placeholders of the form ${step_name} are replaced with temp identifiers.
    In production you may register temp views for prior step DataFrames to avoid string substitution.
    """
    def __init__(self, step_config: dict, base_path: str = ""):
        self.name = step_config["name"]
        self.sql_file = step_config["sql_file"]
        self.depends_on = step_config.get("depends_on", [])
        self.parallel_group = step_config.get("parallel_group")
        self.output_table = step_config.get("output_table")
        self.cache_strategy = step_config.get("cache_strategy", "never")
        self.base_path = base_path
        self.result_df: Optional[Any] = None

    def get_sql_content(self) -> str:
        sql_path = os.path.join(self.base_path, self.sql_file)
        if not os.path.exists(sql_path):
            raise FileNotFoundError(f"SQL file not found: {sql_path}")
        with open(sql_path, "r") as f:
            return f.read()

    def _replace_placeholders(self, sql_content: str, context: Dict[str, Any]) -> str:
        for key in context.keys():
            placeholder = f"${{{key}}}"
            if placeholder in sql_content:
                temp_table = f"temp_{key}"
                sql_content = sql_content.replace(placeholder, temp_table)
        return sql_content

    def execute(self, connector, context: Dict[str, Any], logger: Optional[Any] = None) -> Any:
        log = logger or globals().get("logger")
        if log:
            log.info("Executing step: %s", self.name)
        sql = self.get_sql_content()

        # Optional: register temp views when supported and enabled
        try:
            use_temp_views = bool(getattr(connector, "config", {}).get("use_temp_views", False))
        except Exception:
            use_temp_views = False
        if use_temp_views and hasattr(connector, "spark"):
            for key, df in context.items():
                try:
                    if hasattr(df, "createOrReplaceTempView"):
                        df.createOrReplaceTempView(f"temp_{key}")
                except Exception:
                    # Best-effort; fall back to string substitution
                    pass
        sql = self._replace_placeholders(sql, context)

        # Read data using standard JDBC connector
        df = connector.read_sql(sql)

        self.result_df = df

        if self.output_table:
            connector.write_table(df, self.output_table)

        if log:
            log.info("Completed step: %s", self.name)
        return df
