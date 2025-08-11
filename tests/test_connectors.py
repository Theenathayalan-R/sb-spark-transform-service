from unittest.mock import Mock

from transform_service.connectors import StarburstConnector


def test_build_jdbc_url_and_props(monkeypatch):
    spark = Mock()
    cfg = {
        "host": "host",
        "port": 443,
        "catalog": "cat",
        "schema": "sch",
        "user": "u",
        "password": "p",
        "ssl": True,
        "application_name": "app",
        "fetchsize": 123,
        "batchsize": 456,
    }
    c = StarburstConnector(cfg, spark)
    assert c.jdbc_url.startswith("jdbc:trino://host:443/cat/sch")
    assert c.connection_properties["driver"] == "io.trino.jdbc.TrinoDriver"
    assert c.connection_properties["fetchsize"] == "123"
    assert c.connection_properties["batchsize"] == "456"
