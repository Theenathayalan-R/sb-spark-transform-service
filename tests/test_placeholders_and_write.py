from unittest.mock import Mock

from transform_service.steps import TransformationStep
from transform_service.connectors import StarburstConnector


def test_placeholder_substitution_multiple(tmp_path):
    sql = "SELECT * FROM ${a} JOIN ${b} ON 1=1;"
    p = tmp_path / "q.sql"
    p.write_text(sql)
    step = TransformationStep({"name": "s", "sql_file": "q.sql"}, base_path=str(tmp_path))

    class DF:
        pass

    out = step._replace_placeholders(sql, {"a": DF(), "b": DF()})
    assert "temp_a" in out and "temp_b" in out


def test_write_table_adds_metadata(monkeypatch):
    # Mock Spark write path: ensure write_table calls write.jdbc with expected table
    spark = Mock()
    cfg = {
        "host": "host",
        "catalog": "cat",
        "schema": "sch",
        "user": "u",
        "password": "p",
    }
    conn = StarburstConnector(cfg, spark)

    class DF:
        def __init__(self):
            self.meta_added = False

        def withColumn(self, *_args, **_kwargs):
            self.meta_added = True
            return self

        class Writer:
            def __init__(self, parent):
                self.parent = parent

            def jdbc(self, url, table, mode, properties):
                self.parent.written = (url, table, mode, tuple(sorted(properties.keys())))

        @property
        def write(self):
            return DF.Writer(self)

        def count(self):
            return 1

    df = DF()
    conn.write_table(df, "tname")
    assert df.meta_added is True
    assert conn.config["catalog"] in conn.jdbc_url
