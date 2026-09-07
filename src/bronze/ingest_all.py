#!/usr/bin/env python3
"""
Orchestrate Bronze ingestion for customers, products, and orders.

Databricks usage:
    %run ./bronze_ingest
    %run ./ingest_all

Recommended ingest order:
    customers -> products -> orders
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

_SRC_ROOT = Path(__file__).resolve().parents[1]
if str(_SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(_SRC_ROOT))

from bronze.bronze_ingest import BronzeIngestError, get_spark, ingest_all_entities
from config.pipeline_config import load_config


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest all Bronze CSV sources.")
    parser.add_argument(
        "--source-base-path",
        help="Directory containing source CSV files (overrides PIPELINE_SOURCE_BASE_PATH).",
    )
    parser.add_argument("--catalog", help="Unity Catalog name (optional).")
    parser.add_argument("--schema", help="Database/schema name (default: ecommerce).")
    parser.add_argument(
        "--write-mode",
        choices=["overwrite", "append"],
        help="Delta write mode for Bronze tables (default: overwrite).",
    )
    parser.add_argument(
        "--batch-id",
        help="Shared ingest batch/run identifier for all three datasets.",
    )
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
        "bronze_write_mode": args.write_mode,
        "batch_id": args.batch_id,
    }
    config = load_config(**{k: v for k, v in overrides.items() if v is not None})

    try:
        spark = get_spark()
        results = ingest_all_entities(spark=spark, config=config)

        total_rows = sum(result.row_count for result in results)
        logging.info(
            "Bronze ingest_all completed batch_id=%s datasets=%s total_rows=%s",
            results[0].batch_id if results else config.resolved_batch_id(),
            len(results),
            total_rows,
        )
        for result in results:
            logging.info(
                "  - %s: %s rows -> %s (source_columns=%s bronze_columns=%s corrupt_rows=%s)",
                result.entity,
                result.row_count,
                result.target_table,
                result.source_column_count,
                result.bronze_column_count,
                result.corrupt_record_count,
            )
        return 0
    except BronzeIngestError as exc:
        logging.error("Bronze ingest_all failed: %s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
