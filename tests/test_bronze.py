"""
Tests for Bronze-layer ingestion behavior.

Each test documents:
- what it validates
- expected result
- actual result (via ValidationCheck on failure)
"""

from __future__ import annotations

import csv
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SRC_ROOT = Path(__file__).resolve().parents[1] / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

TESTS_ROOT = Path(__file__).resolve().parent
if str(TESTS_ROOT) not in sys.path:
    sys.path.insert(0, str(TESTS_ROOT))

from bronze.bronze_ingest import (
    add_bronze_metadata,
    align_to_bronze_schema,
    path_exists,
    read_source_csv,
)
from bronze.bronze_schemas import ENTITY_SCHEMAS, SOURCE_COLUMN_NAMES
from data_generation.generate_sample_data import CUSTOMER_CSV_COLUMNS, generate_all
from quality_expectations import CUSTOMER_TOTAL_COUNT, DEFAULT_GENERATION_SEED
from test_support import ValidationCheck, assert_all_pass


def _write_customer_csv(path: Path, rows) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CUSTOMER_CSV_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {col: ("" if row.get(col) is None else row[col]) for col in CUSTOMER_CSV_COLUMNS},
            )


class TestBronzeCsvIngestion:
    def test_reads_csv_with_string_schema(self, spark: SparkSession, tmp_path: Path) -> None:
        """
        Validates: Bronze CSV read keeps business columns as strings.
        Expected: customer_id column has StringType in Spark schema.
        """
        customers, _, _ = generate_all(DEFAULT_GENERATION_SEED)
        csv_path = tmp_path / "customers.csv"
        _write_customer_csv(csv_path, customers[:5])

        raw_df = read_source_csv(spark, str(csv_path), "customers")
        dtype = dict(raw_df.dtypes)["customer_id"]

        ValidationCheck(
            name="customer_id_string_type",
            validates="Bronze customer_id stored as STRING",
            expected="string",
            actual=dtype,
        ).assert_pass()

    def test_bronze_metadata_columns_added(self, spark: SparkSession, tmp_path: Path) -> None:
        """
        Validates: ingest metadata columns are appended without changing row count.
        Expected: 5 input rows -> 5 Bronze rows with metadata columns present.
        """
        customers, _, _ = generate_all(DEFAULT_GENERATION_SEED)
        csv_path = tmp_path / "customers.csv"
        _write_customer_csv(csv_path, customers[:5])

        raw_df = read_source_csv(spark, str(csv_path), "customers")
        bronze_df = align_to_bronze_schema(
            add_bronze_metadata(
                raw_df,
                source_path=str(csv_path),
                source_file="customers.csv",
                batch_id="bronze-test-batch",
            ),
            "customers",
        )

        checks = [
            ValidationCheck(
                name="bronze_row_count",
                validates="Bronze ingest preserves CSV row count",
                expected=5,
                actual=bronze_df.count(),
            ),
            ValidationCheck(
                name="metadata_column_count",
                validates="Bronze schema includes metadata fields",
                expected=len(ENTITY_SCHEMAS["customers"].fields),
                actual=len(bronze_df.columns),
            ),
        ]
        assert_all_pass(checks)

        required_metadata = {"_ingest_ts", "_source_file", "_source_path", "_batch_id", "_source_row_num"}
        actual_columns = set(bronze_df.columns)
        missing = required_metadata - actual_columns
        ValidationCheck(
            name="required_metadata_present",
            validates="Bronze metadata columns exist for lineage",
            expected=set(),
            actual=missing,
        ).assert_pass()


class TestBronzeSampleDataShape:
    def test_generated_customer_csv_maps_to_full_bronze_volume(
        self,
        spark: SparkSession,
        tmp_path: Path,
    ) -> None:
        """
        Validates: full generated customer CSV can be ingested at assignment volume.
        Expected: 10,000 Bronze customer rows.
        """
        customers, _, _ = generate_all(DEFAULT_GENERATION_SEED)
        csv_path = tmp_path / "customers.csv"
        _write_customer_csv(csv_path, customers)

        raw_df = read_source_csv(spark, str(csv_path), "customers")
        bronze_df = add_bronze_metadata(
            raw_df,
            source_path=str(csv_path),
            source_file="customers.csv",
            batch_id="bronze-volume-test",
        )

        ValidationCheck(
            name="full_customer_bronze_row_count",
            validates="Bronze customers row count from generated CSV",
            expected=CUSTOMER_TOTAL_COUNT,
            actual=bronze_df.count(),
        ).assert_pass()

    def test_source_columns_match_bronze_contract(self) -> None:
        """
        Validates: Bronze source column contract for all entities.
        Expected: customers/orders/products source column lists are non-empty.
        """
        checks = [
            ValidationCheck(
                name=f"{entity}_source_columns",
                validates=f"Bronze source columns defined for {entity}",
                expected=True,
                actual=len(SOURCE_COLUMN_NAMES[entity]) > 0,
            )
            for entity in ("customers", "orders", "products")
        ]
        assert_all_pass(checks)

    def test_null_email_preserved_as_blank_string_in_csv(
        self,
        spark: SparkSession,
        tmp_path: Path,
    ) -> None:
        """
        Validates: NULL emails in generated data become blank strings in Bronze CSV read.
        Expected: at least one row with blank email after CSV round-trip.
        """
        customers, _, _ = generate_all(DEFAULT_GENERATION_SEED)
        csv_path = tmp_path / "customers.csv"
        _write_customer_csv(csv_path, customers)

        raw_df = read_source_csv(spark, str(csv_path), "customers")
        blank_email_count = raw_df.filter(
            F.col("email").isNull() | (F.trim(F.col("email")) == ""),
        ).count()

        ValidationCheck(
            name="blank_email_rows_after_csv_read",
            validates="NULL emails survive CSV ingest as blank strings",
            expected=True,
            actual=blank_email_count > 0,
        ).assert_pass()


class TestPathExists:
    def test_local_file_uri_without_jvm(self, spark: SparkSession, tmp_path: Path) -> None:
        """path_exists must work for file:/ URIs without spark._jvm (serverless-safe)."""
        csv_path = tmp_path / "probe.csv"
        csv_path.write_text("a\n", encoding="utf-8")
        uri = csv_path.resolve().as_uri()

        assert path_exists(spark, uri) is True
        assert path_exists(spark, str(csv_path)) is True
        assert path_exists(spark, str(tmp_path / "missing.csv")) is False

    def test_missing_local_file_uri(self, spark: SparkSession, tmp_path: Path) -> None:
        missing_uri = (tmp_path / "does_not_exist.csv").resolve().as_uri()
        assert path_exists(spark, missing_uri) is False
