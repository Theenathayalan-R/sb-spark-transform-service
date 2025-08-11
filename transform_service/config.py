from __future__ import annotations

import os
import re
from typing import Any, Dict, Type

import yaml
from pydantic import BaseModel, ValidationError

from .schemas import ConnectionConfig, PipelineConfig, UnifiedConfig


class ConfigLoader:
    _env_pattern = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")

    @classmethod
    def _expand_env_in_obj(cls, obj: Any) -> Any:
        if isinstance(obj, str):

            def repl(match: re.Match[str]) -> str:
                var = match.group(1)
                return os.environ.get(var, match.group(0))

            return cls._env_pattern.sub(repl, obj)
        if isinstance(obj, dict):
            return {k: cls._expand_env_in_obj(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [cls._expand_env_in_obj(v) for v in obj]
        return obj

    @staticmethod
    def _load_yaml(path: str) -> Dict[str, Any]:
        with open(path, "r") as f:
            raw_config = yaml.safe_load(f) or {}
        return ConfigLoader._expand_env_in_obj(raw_config)

    @staticmethod
    def _load_and_validate(path: str, model: Type[BaseModel]) -> Dict[str, Any]:
        """Load and validate a single YAML file."""
        config_data = ConfigLoader._load_yaml(path)
        try:
            return model(**config_data).model_dump(by_alias=True)
        except ValidationError as e:
            raise ValueError(f"Validation error in '{path}': {e}") from e

    @staticmethod
    def load(path: str) -> Dict[str, Any]:
        return ConfigLoader._load_and_validate(path, UnifiedConfig)

    @staticmethod
    def load_separate(pipeline_path: str, connection_path: str) -> Dict[str, Any]:
        validated_pipe = ConfigLoader._load_and_validate(pipeline_path, PipelineConfig)
        validated_conn = ConfigLoader._load_and_validate(
            connection_path, ConnectionConfig
        )

        merged_config = validated_conn
        merged_config.update(validated_pipe)
        return merged_config
