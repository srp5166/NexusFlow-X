import sys

import pytest
from pyspark.sql import SparkSession

# PySpark local mode is not reliable on very new CPython versions (worker/driver mismatch).
# CI uses Python 3.11; use 3.10–3.12 locally for Spark-backed tests.
_MAX_PY_FOR_LOCAL_SPARK = (3, 12)


@pytest.fixture(scope="session")
def spark():
    if sys.version_info[:2] > _MAX_PY_FOR_LOCAL_SPARK:
        pytest.skip(
            "PySpark local tests require Python <= 3.12 (CI uses 3.11); "
            f"this interpreter is {sys.version_info.major}.{sys.version_info.minor}."
        )
    session = (
        SparkSession.builder.master("local[1]")
        .appName("nexusflow-pytest")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
