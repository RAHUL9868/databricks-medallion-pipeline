#!/usr/bin/env python3
"""
Bronze ingestion entry point: customers.csv -> bronze_customers Delta table.

Databricks usage:
    %run ./bronze_ingest
    %run ./01_ingest_customers

Or as a Python task with Spark available in the environment.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

_SRC_ROOT = Path(__file__).resolve().parents[1]
if str(_SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(_SRC_ROOT))

from bronze.bronze_ingest import BronzeIngestError, get_spark, ingest_entity
from config.pipeline_config import load_config


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest customers CSV into Bronze Delta.")
    parser.add_argument(
        "--source-base-path",
        help="Directory containing customers.csv (overrides PIPELINE_SOURCE_BASE_PATH).",
    )
    parser.add_argument("--catalog", help="Unity Catalog name (optional).")
    parser.add_argument("--schema", help="Database/schema name (default: ecommerce).")
    parser.add_argument(
        "--table-name",
        help="Bronze table name (default: bronze_customers).",
    )
    parser.add_argument(
        "--write-mode",
        choices=["overwrite", "append"],
        help="Delta write mode (default: overwrite).",
    )
    parser.add_argument("--batch-id", help="Ingest batch/run identifier.")
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    overrides = {
        "source_base_path": args.source_base_path,
        "catalog": args.catalog,
        "schema_name": args.schema,
        "bronze_customers_table": args.table_name,
        "bronze_write_mode": args.write_mode,
        "batch_id": args.batch_id,
    }
    config = load_config(**{k: v for k, v in overrides.items() if v is not None})

    try:
        spark = get_spark()
        result = ingest_entity(
            entity="customers",
            source_filename=config.customers_csv,
            target_table_name=config.bronze_customers_table,
            spark=spark,
            config=config,
        )
        logging.info(
            "customers Bronze ingest succeeded: %s rows written to %s",
            result.row_count,
            result.target_table,
        )
        return 0
    except BronzeIngestError as exc:
        logging.error("customers Bronze ingest failed: %s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
