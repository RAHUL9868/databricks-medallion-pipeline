"""Pytest configuration and shared fixtures for pipeline tests."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from data_generation.generate_sample_data import generate_all
from test_support import dicts_to_bronze_dataframe, run_silver_pipeline

SRC_ROOT = Path(__file__).resolve().parents[1] / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

TESTS_ROOT = Path(__file__).resolve().parent
if str(TESTS_ROOT) not in sys.path:
    sys.path.insert(0, str(TESTS_ROOT))

from quality_expectations import DEFAULT_GENERATION_SEED


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.master("local[2]")
        .appName("ecommerce-medallion-tests")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.warehouse.dir", str(TESTS_ROOT / ".spark-warehouse"))
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def generated_sample_data():
    """
    Deterministic full sample datasets (seed=42).

    Validates: generator output matches assignment volumes before Silver tests run.
    """
    customers, products, orders = generate_all(DEFAULT_GENERATION_SEED)
    return {
        "customers": customers,
        "products": products,
        "orders": orders,
    }


@pytest.fixture(scope="session")
def bronze_sample_frames(spark: SparkSession, generated_sample_data):
    """Bronze-shaped DataFrames built from generated sample CSV records."""
    return {
        "customers": dicts_to_bronze_dataframe(
            spark,
            "customers",
            generated_sample_data["customers"],
        ),
        "orders": dicts_to_bronze_dataframe(
            spark,
            "orders",
            generated_sample_data["orders"],
        ),
        "products": dicts_to_bronze_dataframe(
            spark,
            "products",
            generated_sample_data["products"],
        ),
    }


@pytest.fixture(scope="session")
def silver_sample_pipeline(spark: SparkSession, bronze_sample_frames):
    """
    Full Silver pipeline output for seed=42 sample data (computed once per session).

    Validates: end-to-end Silver quality pipeline on assignment-scale data.
    """
    return run_silver_pipeline(
        spark,
        bronze_sample_frames["customers"],
        bronze_sample_frames["orders"],
        bronze_sample_frames["products"],
        run_id="assignment-seed-42",
    )
