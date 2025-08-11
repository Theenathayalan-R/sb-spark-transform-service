import sys
import types

# Lightweight stubs for pyspark to enable unit tests without installing Spark
pyspark_mod = types.ModuleType("pyspark")
sql_mod = types.ModuleType("pyspark.sql")
functions_mod = types.ModuleType("pyspark.sql.functions")


class SparkSession:  # minimal stub
    class builder:
        @staticmethod
        def appName(_):
            return SparkSession.builder

        @staticmethod
        def config(*_a, **_k):
            return SparkSession.builder

        @staticmethod
        def getOrCreate():
            return SparkSession()

    def stop(self):
        pass


class DataFrame:  # marker class for typing compatibility
    pass


def current_timestamp():
    return None


# Attach stubs to module objects
setattr(sql_mod, "SparkSession", SparkSession)
setattr(sql_mod, "DataFrame", DataFrame)
setattr(functions_mod, "current_timestamp", current_timestamp)

setattr(pyspark_mod, "sql", sql_mod)

sys.modules.setdefault("pyspark", pyspark_mod)
sys.modules.setdefault("pyspark.sql", sql_mod)
sys.modules.setdefault("pyspark.sql.functions", functions_mod)
