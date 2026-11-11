import os
import shutil

import pytest


@pytest.fixture(scope="session")
def spark():
    """Local Spark session. Skips Spark tests when pyspark or Java is not installed.

    CI sets TAMARACK_REQUIRE_SPARK=1 so a missing Spark fails the run instead of skipping.
    """
    unavailable = pytest.fail if os.environ.get("TAMARACK_REQUIRE_SPARK") == "1" else pytest.skip
    try:
        import pyspark  # noqa: F401
    except ImportError:
        unavailable("pyspark is required for the Spark tests")
    if not (os.environ.get("JAVA_HOME") or shutil.which("java")):
        unavailable("Java is required for a local Spark session")
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("tamarack-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()
