import os
from pathlib import Path
from transform_service import config as cfg
from transform_service.cli import main as cli_main


def test_env_expansion_in_connection_yaml(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("STARBURST_USER", "user1")
    monkeypatch.setenv("STARBURST_PASSWORD", "pass1")
    conn = tmp_path / "connection.yaml"
    conn.write_text(
        """
starburst:
  host: h
  catalog: c
  schema: s
  user: ${STARBURST_USER}
  password: ${STARBURST_PASSWORD}
"""
    )
    pipe = tmp_path / "pipeline.yaml"
    pipe.write_text("sql_base_path: sql\ntransformations: []\n")
    merged = cfg.ConfigLoader.load_separate(str(pipe), str(conn))
    assert merged["starburst"]["user"] == "user1"
    assert merged["starburst"]["password"] == "pass1"


def test_cli_dry_run_with_split_configs(tmp_path: Path, monkeypatch):
    # Build minimal files
    (tmp_path / "sql").mkdir()
    (tmp_path / "sql/a.sql").write_text("select 1;")
    pipeline_yaml = tmp_path / "pipeline.yaml"
    pipeline_yaml.write_text(
        """
sql_base_path: "sql"
transformations:
  - name: "a"
    sql_file: "a.sql"
    depends_on: []
"""
    )
    connection_yaml = tmp_path / "connection.yaml"
    connection_yaml.write_text(
        """
starburst:
  host: h
  catalog: c
  schema: s
  user: u
  password: p
spark:
  app_name: t
"""
    )
    # Patch spark inside pipeline to avoid real Spark
    from transform_service import pipeline as pl

    def _create_spark_session_stub(self):
        class S:
            def stop(self):
                pass
        return S()

    pl.TransformationPipeline._create_spark_session = _create_spark_session_stub  # type: ignore
    pl.StarburstConnector = lambda *a, **k: None  # type: ignore

    # Run CLI
    rc = cli_main([
        "--pipeline-config",
        str(pipeline_yaml),
        "--connection-config",
        str(connection_yaml),
        "--dry-run",
    ])
    assert rc == 0
