from pathlib import Path
import pytest

from transform_service import pipeline as pl


class FakeDF:
    def __init__(self):
        self.cached = False
        self._count = 1

    def cache(self):
        self.cached = True
        return self

    def count(self):
        return self._count


class FakeConnector:
    def __init__(self, *_, **__):
        self.jdbc_url = "jdbc:trino://h:443/c/s"
        self.connection_properties = {"user": "u", "password": "p", "driver": "io.trino.jdbc.TrinoDriver"}
        self.read_calls = []
        self.write_calls = []

    def read_sql(self, sql: str):
        self.read_calls.append(sql)
        return FakeDF()

    def write_table(self, df, table_name: str, mode: str = "overwrite"):
        self.write_calls.append((table_name, mode, isinstance(df, FakeDF)))


class DummyStatus:
    def __init__(self):
        self.failed = False
        self.started_steps = []
        self.completed_steps = []
        self.failed_steps = []

    def start_job(self, *a, **k):
        pass

    def update_job_status(self, status: str, **_):
        if status == "FAILED":
            self.failed = True

    def start_step(self, name, *_):
        self.started_steps.append(name)

    def complete_step(self, name, *_):
        self.completed_steps.append(name)

    def fail_step(self, name, *_):
        self.failed_steps.append(name)

    def get_completed_steps(self):
        return []

    def get_failed_steps(self):
        return []


def _stub_spark(self):
    class S:
        def stop(self):
            pass
    return S()


def test_pipeline_execute_success(tmp_path: Path, monkeypatch):
    # Prepare minimal SQL files
    (tmp_path / "sql").mkdir()
    (tmp_path / "sql" / "a.sql").write_text("select 1;")
    (tmp_path / "sql" / "b.sql").write_text("select 2;")

    cfg = {
        "starburst": {"host": "h", "catalog": "c", "schema": "s", "user": "u", "password": "p"},
        "spark": {"app_name": "t"},
        "sql_base_path": str(tmp_path / "sql"),
        "transformations": [
            {"name": "a", "sql_file": "a.sql", "depends_on": [], "output_table": "t1"},
            {"name": "b", "sql_file": "b.sql", "depends_on": ["a"], "output_table": "t2"},
        ],
        "final_outputs": {"b": {"table": "out.tbl", "mode": "overwrite"}},
    }

    # Patch internals to avoid real Spark/Starburst
    monkeypatch.setattr(pl.TransformationPipeline, "_create_spark_session", _stub_spark, raising=True)
    monkeypatch.setattr(pl, "StarburstConnector", FakeConnector, raising=True)

    p = pl.TransformationPipeline(config_path=None, pipeline_config="", connection_config="")
    # inject config directly
    p.config = cfg
    p.steps = p._create_steps()
    p.status_tracker = DummyStatus()

    res = p.execute()

    # assertions
    assert set(res.keys()) == {"a", "b"}
    # final output write invoked via FakeConnector in steps and pipeline
    assert isinstance(p.connector, FakeConnector)


def test_pipeline_execute_failure(tmp_path: Path, monkeypatch):
    (tmp_path / "sql").mkdir()
    (tmp_path / "sql" / "a.sql").write_text("select 1;")
    (tmp_path / "sql" / "b.sql").write_text("select 2;")

    cfg = {
        "starburst": {"host": "h", "catalog": "c", "schema": "s", "user": "u", "password": "p"},
        "spark": {"app_name": "t"},
        "sql_base_path": str(tmp_path / "sql"),
        "transformations": [
            {"name": "a", "sql_file": "a.sql", "depends_on": []},
            {"name": "b", "sql_file": "b.sql", "depends_on": ["a"]},
        ],
    }

    monkeypatch.setattr(pl.TransformationPipeline, "_create_spark_session", _stub_spark, raising=True)
    monkeypatch.setattr(pl, "StarburstConnector", FakeConnector, raising=True)

    p = pl.TransformationPipeline(config_path=None, pipeline_config="", connection_config="")
    p.config = cfg
    p.steps = p._create_steps()
    p.status_tracker = DummyStatus()

    # Force failure on executing step 'b'
    def failing_execute(conn, ctx):
        raise RuntimeError("boom")

    # Replace execute on the second step
    p.steps[1].execute = failing_execute  # type: ignore

    with pytest.raises(RuntimeError):
        p.execute()

    assert p.status_tracker.failed is True


def test_pipeline_execute_parallel_failure(tmp_path: Path, monkeypatch):
    (tmp_path / "sql").mkdir()
    (tmp_path / "sql" / "a.sql").write_text("select 1;")
    (tmp_path / "sql" / "b.sql").write_text("select 2;")
    (tmp_path / "sql" / "c.sql").write_text("select 3;")

    cfg = {
        "starburst": {"host": "h", "catalog": "c", "schema": "s", "user": "u", "password": "p"},
        "spark": {"app_name": "t"},
        "sql_base_path": str(tmp_path / "sql"),
        "transformations": [
            {"name": "a", "sql_file": "a.sql", "depends_on": [], "parallel_group": "g"},
            {"name": "b", "sql_file": "b.sql", "depends_on": [], "parallel_group": "g"},
            {"name": "c", "sql_file": "c.sql", "depends_on": ["a", "b"]},
        ],
        "max_parallel_workers": 2,
    }

    monkeypatch.setattr(pl.TransformationPipeline, "_create_spark_session", _stub_spark, raising=True)
    monkeypatch.setattr(pl, "StarburstConnector", FakeConnector, raising=True)

    p = pl.TransformationPipeline(config_path=None, pipeline_config="", connection_config="")
    p.config = cfg
    p.steps = p._create_steps()
    p.status_tracker = DummyStatus()

    # Force failure on executing step 'b'
    original_execute = p.steps[1].execute
    def failing_execute(connector, context, logger):
        if context.get("fail"):
            raise RuntimeError("boom")
        return original_execute(connector, context, logger)

    p.steps[1].execute = failing_execute

    # a and b run in parallel. b will fail. c should not run.
    with pytest.raises(RuntimeError, match="boom"):
        p.execution_context["fail"] = True
        p.execute()

    assert p.status_tracker.failed is True
    assert "a" in p.status_tracker.completed_steps
    assert "b" in p.status_tracker.failed_steps
    assert "c" not in p.status_tracker.started_steps
