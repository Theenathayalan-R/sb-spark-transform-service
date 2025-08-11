import builtins
import io
import os
import textwrap
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock

import pytest

from transform_service.steps import TransformationStep


def test_get_sql_content_reads_file(tmp_path: Path):
    sql = "SELECT 1;\n"
    p = tmp_path / "q.sql"
    p.write_text(sql)
    step = TransformationStep({"name": "s", "sql_file": str(p.name)}, base_path=str(tmp_path))
    assert step.get_sql_content() == sql


def test_get_sql_content_missing_file(tmp_path: Path):
    step = TransformationStep({"name": "s", "sql_file": "missing.sql"}, base_path=str(tmp_path))
    with pytest.raises(FileNotFoundError):
        step.get_sql_content()


def test_execute_calls_connector_and_writes(monkeypatch, tmp_path: Path):
    sql = "SELECT * FROM t;"
    p = tmp_path / "q.sql"
    p.write_text(sql)
    step = TransformationStep({
        "name": "s",
        "sql_file": str(p.name),
        "output_table": "out",
        "cache": False,
    }, base_path=str(tmp_path))

    mock_df = Mock(name="DataFrame")
    connector = Mock()
    connector.read_sql.return_value = mock_df

    res = step.execute(connector, {})

    connector.read_sql.assert_called_once_with(sql)
    connector.write_table.assert_called_once_with(mock_df, "out")
    assert res is mock_df


def test_replace_placeholders(tmp_path: Path):
    p = tmp_path / "q.sql"
    p.write_text("SELECT * FROM ${prev};")
    step = TransformationStep({"name": "s", "sql_file": str(p.name)}, base_path=str(tmp_path))

    class DF:  # minimal placeholder
        pass

    out = step._replace_placeholders(p.read_text(), {"prev": DF()})
    assert "temp_prev" in out


# Extended Tests
class TestTransformationStepExtended:
    """Extended test coverage for TransformationStep class."""

    def test_initialization_with_all_options(self):
        """Test TransformationStep initialization with all configuration options."""
        config = {
            "name": "test_step",
            "sql_file": "query.sql",
            "depends_on": ["step1", "step2"],
            "parallel_group": "group_a",
            "output_table": "output_table",
            "cache_strategy": "smart"
        }
        base_path = "/tmp/sql"
        
        step = TransformationStep(config, base_path)
        
        assert step.name == "test_step"
        assert step.sql_file == "query.sql"
        assert step.depends_on == ["step1", "step2"]
        assert step.parallel_group == "group_a"
        assert step.output_table == "output_table"
        assert step.cache_strategy == "smart"
        assert step.base_path == base_path
        assert step.result_df is None

    def test_initialization_with_minimal_config(self):
        """Test TransformationStep initialization with minimal configuration."""
        config = {
            "name": "minimal_step",
            "sql_file": "minimal.sql"
        }
        
        step = TransformationStep(config)
        
        assert step.name == "minimal_step"
        assert step.sql_file == "minimal.sql"
        assert step.depends_on == []
        assert step.parallel_group is None
        assert step.output_table is None
        assert step.cache_strategy == "never"
        assert step.base_path == ""

    def test_get_sql_content_with_absolute_path(self, tmp_path: Path):
        """Test getting SQL content when using absolute path."""
        sql_content = "SELECT * FROM customers WHERE active = 1;"
        sql_file = tmp_path / "absolute_test.sql"
        sql_file.write_text(sql_content)
        
        step = TransformationStep({
            "name": "abs_test",
            "sql_file": str(sql_file)  # Absolute path
        })
        
        assert step.get_sql_content() == sql_content

    def test_replace_placeholders_multiple_occurrences(self):
        """Test placeholder replacement with multiple occurrences."""
        step = TransformationStep({"name": "test", "sql_file": "test.sql"})
        
        sql_content = """
        SELECT c.id, o.amount
        FROM ${customers} c
        JOIN ${orders} o ON c.id = o.customer_id
        WHERE c.region IN (SELECT region FROM ${customers})
        """
        
        context = {
            "customers": Mock(),
            "orders": Mock()
        }
        
        result = step._replace_placeholders(sql_content, context)
        
        assert "${customers}" not in result
        assert "${orders}" not in result
        assert "temp_customers" in result
        assert "temp_orders" in result
        # Verify multiple occurrences are replaced
        assert result.count("temp_customers") == 2

    def test_replace_placeholders_no_placeholders(self):
        """Test placeholder replacement when no placeholders exist."""
        step = TransformationStep({"name": "test", "sql_file": "test.sql"})
        
        sql_content = "SELECT * FROM static_table WHERE id = 1"
        context = {"some_table": Mock()}
        
        result = step._replace_placeholders(sql_content, context)
        
        assert result == sql_content

    def test_replace_placeholders_empty_context(self):
        """Test placeholder replacement with empty context."""
        step = TransformationStep({"name": "test", "sql_file": "test.sql"})
        
        sql_content = "SELECT * FROM ${some_table}"
        context = {}
        
        result = step._replace_placeholders(sql_content, context)
        
        # Placeholder should remain unchanged
        assert "${some_table}" in result

    def test_execute_with_temp_views_enabled(self, tmp_path: Path):
        """Test execution with temp views enabled."""
        sql_content = "SELECT * FROM temp_input"
        sql_file = tmp_path / "temp_view_test.sql"
        sql_file.write_text(sql_content)
        
        step = TransformationStep({
            "name": "temp_view_test",
            "sql_file": str(sql_file.name)
        }, base_path=str(tmp_path))
        
        # Mock connector with temp views support
        connector = Mock()
        connector.config = {"use_temp_views": True}
        connector.spark = Mock()
        connector.read_sql.return_value = Mock(name="result_df")
        
        # Mock DataFrame with temp view capability
        input_df = Mock()
        input_df.createOrReplaceTempView = Mock()
        
        context = {"input": input_df}
        
        result = step.execute(connector, context)
        
        # Verify temp view was created
        input_df.createOrReplaceTempView.assert_called_once_with("temp_input")
        connector.read_sql.assert_called_once_with(sql_content)
        assert step.result_df == result

    def test_execute_with_temp_views_exception(self, tmp_path: Path):
        """Test execution when temp view creation fails."""
        sql_content = "SELECT * FROM temp_input"
        sql_file = tmp_path / "temp_view_fail_test.sql"
        sql_file.write_text(sql_content)
        
        step = TransformationStep({
            "name": "temp_view_fail_test",
            "sql_file": str(sql_file.name)
        }, base_path=str(tmp_path))
        
        # Mock connector with temp views support
        connector = Mock()
        connector.config = {"use_temp_views": True}
        connector.spark = Mock()
        connector.read_sql.return_value = Mock(name="result_df")
        
        # Mock DataFrame that raises exception on temp view creation
        input_df = Mock()
        input_df.createOrReplaceTempView.side_effect = Exception("Temp view failed")
        
        context = {"input": input_df}
        
        # Should not raise exception, should fall back to string replacement
        result = step.execute(connector, context)
        
        connector.read_sql.assert_called_once()
        assert step.result_df == result

    def test_execute_without_output_table(self, tmp_path: Path):
        """Test execution without output table (no write operation)."""
        sql_content = "SELECT * FROM source"
        sql_file = tmp_path / "no_output_test.sql"
        sql_file.write_text(sql_content)
        
        step = TransformationStep({
            "name": "no_output_test",
            "sql_file": str(sql_file.name)
            # No output_table specified
        }, base_path=str(tmp_path))
        
        connector = Mock()
        connector.read_sql.return_value = Mock(name="result_df")
        
        result = step.execute(connector, {})
        
        connector.read_sql.assert_called_once_with(sql_content)
        # write_table should not be called
        assert not hasattr(connector, 'write_table') or not connector.write_table.called
        assert step.result_df == result

    def test_execute_with_custom_logger(self, tmp_path: Path):
        """Test execution with custom logger."""
        sql_content = "SELECT 1"
        sql_file = tmp_path / "logger_test.sql"
        sql_file.write_text(sql_content)
        
        step = TransformationStep({
            "name": "logger_test",
            "sql_file": str(sql_file.name)
        }, base_path=str(tmp_path))
        
        connector = Mock()
        connector.read_sql.return_value = Mock(name="result_df")
        
        custom_logger = Mock()
        
        step.execute(connector, {}, logger=custom_logger)
        
        # Verify custom logger was used
        custom_logger.info.assert_any_call("Executing step: %s", "logger_test")
        custom_logger.info.assert_any_call("Completed step: %s", "logger_test")

    def test_execute_connector_without_config(self, tmp_path: Path):
        """Test execution with connector that doesn't have config attribute."""
        sql_content = "SELECT 1"
        sql_file = tmp_path / "no_config_test.sql"
        sql_file.write_text(sql_content)
        
        step = TransformationStep({
            "name": "no_config_test",
            "sql_file": str(sql_file.name)
        }, base_path=str(tmp_path))
        
        # Connector without config attribute
        connector = Mock(spec=['read_sql', 'write_table'])
        connector.read_sql.return_value = Mock(name="result_df")
        
        # Should not raise exception
        result = step.execute(connector, {})
        
        connector.read_sql.assert_called_once_with(sql_content)
        assert step.result_df == result

    @patch('transform_service.steps.HAS_PYSPARK', False)
    def test_execute_without_pyspark(self, tmp_path: Path):
        """Test execution when PySpark is not available."""
        sql_content = "SELECT 1"
        sql_file = tmp_path / "no_pyspark_test.sql"
        sql_file.write_text(sql_content)
        
        step = TransformationStep({
            "name": "no_pyspark_test",
            "sql_file": str(sql_file.name)
        }, base_path=str(tmp_path))
        
        connector = Mock()
        connector.read_sql.return_value = Mock(name="result_df")
        
        # Should still work without PySpark
        result = step.execute(connector, {})
        
        connector.read_sql.assert_called_once_with(sql_content)
        assert step.result_df == result

    def test_cache_strategy_configuration(self):
        """Test various cache strategy configurations."""
        # Test default cache strategy
        step1 = TransformationStep({"name": "test1", "sql_file": "test.sql"})
        assert step1.cache_strategy == "never"
        
        # Test explicit cache strategy
        step2 = TransformationStep({
            "name": "test2", 
            "sql_file": "test.sql",
            "cache_strategy": "smart"
        })
        assert step2.cache_strategy == "smart"
        
        # Test memory cache strategy
        step3 = TransformationStep({
            "name": "test3", 
            "sql_file": "test.sql",
            "cache_strategy": "memory"
        })
        assert step3.cache_strategy == "memory"

    def test_result_df_assignment(self, tmp_path: Path):
        """Test that result_df is properly assigned after execution."""
        sql_content = "SELECT 1 as id"
        sql_file = tmp_path / "result_test.sql"
        sql_file.write_text(sql_content)
        
        step = TransformationStep({
            "name": "result_test",
            "sql_file": str(sql_file.name)
        }, base_path=str(tmp_path))
        
        mock_df = Mock(name="result_dataframe")
        connector = Mock()
        connector.read_sql.return_value = mock_df
        
        # Initially result_df should be None
        assert step.result_df is None
        
        # After execution, result_df should be set
        result = step.execute(connector, {})
        
        assert step.result_df == mock_df
        assert result == mock_df
