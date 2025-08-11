from transform_service.pipeline import TransformationPipeline
from transform_service.steps import TransformationStep

import json
from pathlib import Path
import pytest


def write_cfg(tmp_path: Path, steps):
    cfg = {
        "starburst": {
            "host": "h",
            "catalog": "c",
            "schema": "s",
            "user": "u",
            "password": "p",
        },
        "spark": {"app_name": "t"},
        "sql_base_path": str(tmp_path),
        "transformations": steps,
    }
    p = tmp_path / "c.yaml"
    p.write_text(json.dumps(cfg))  # pipeline uses yaml.safe_load; JSON is valid YAML
    return str(p)


def test_execution_order_linear(tmp_path: Path):
    cfg = write_cfg(tmp_path, [
        {"name": "a", "sql_file": "a.sql", "depends_on": []},
        {"name": "b", "sql_file": "b.sql", "depends_on": ["a"]},
        {"name": "c", "sql_file": "c.sql", "depends_on": ["b"]},
    ])
    for name in ["a.sql", "b.sql", "c.sql"]:
        (tmp_path / name).write_text("select 1;")
    # patch Spark heavy parts by monkeypatching methods
    from transform_service import pipeline as pl
    def _create_spark_session_stub(self):
        class S:  # minimal SparkSession stub
            def stop(self):
                pass
        return S()
    pl.TransformationPipeline._create_spark_session = _create_spark_session_stub
    pl.StarburstConnector = lambda *a, **k: None  # not used in order calc

    pipe = TransformationPipeline(cfg)
    order = pipe._get_execution_order()
    assert [[s.name for s in batch] for batch in order] == [["a"], ["b"], ["c"]]


def test_execution_order_parallel_groups(tmp_path: Path):
    cfg = write_cfg(tmp_path, [
        {"name": "a", "sql_file": "a.sql", "depends_on": []},
        {"name": "b", "sql_file": "b.sql", "depends_on": ["a"], "parallel_group": "g"},
        {"name": "c", "sql_file": "c.sql", "depends_on": ["a"], "parallel_group": "g"},
        {"name": "d", "sql_file": "d.sql", "depends_on": ["b", "c"]},
    ])
    for name in ["a.sql", "b.sql", "c.sql", "d.sql"]:
        (tmp_path / name).write_text("select 1;")
    from transform_service import pipeline as pl
    pl.TransformationPipeline._create_spark_session = lambda self: type("S", (), {"stop": lambda _: None})()
    pl.StarburstConnector = lambda *a, **k: None
    pipe = TransformationPipeline(cfg)
    order = pipe._get_execution_order()
    assert [[s.name for s in batch] for batch in order] == [["a"], ["b", "c"], ["d"]]


def test_execution_order_circular_dependency(tmp_path: Path):
    cfg = write_cfg(tmp_path, [
        {"name": "a", "sql_file": "a.sql", "depends_on": ["c"]},
        {"name": "b", "sql_file": "b.sql", "depends_on": ["a"]},
        {"name": "c", "sql_file": "c.sql", "depends_on": ["b"]},
    ])
    for name in ["a.sql", "b.sql", "c.sql"]:
        (tmp_path / name).write_text("select 1;")
    from transform_service import pipeline as pl
    pl.TransformationPipeline._create_spark_session = lambda self: type("S", (), {"stop": lambda _: None})()
    pl.StarburstConnector = lambda *a, **k: None
    pipe = TransformationPipeline(cfg)
    with pytest.raises(RuntimeError, match="Circular dependency detected"):
        pipe._get_execution_order()
