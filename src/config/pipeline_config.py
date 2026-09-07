"""
Pipeline configuration for the e-commerce Medallion architecture.

Values can be overridden via environment variables or by passing a config object
to ingest functions. No workspace-specific paths are hard-coded.
"""

import os
from dataclasses import dataclass, field, fields, replace
from datetime import datetime, timezone
from typing import Dict, Optional


def _env(name: str, default: Optional[str] = None) -> Optional[str]:
    value = os.environ.get(name)
    return value if value not in (None, "") else default


@dataclass(frozen=True)
class PipelineConfig:
    """Runtime configuration for Bronze ingestion."""

    source_base_path: str = field(
        default_factory=lambda: _env("PIPELINE_SOURCE_BASE_PATH", "./data") or "./data",
    )
    catalog: Optional[str] = field(default_factory=lambda: _env("PIPELINE_CATALOG"))
    schema_name: str = field(
        default_factory=lambda: _env("PIPELINE_SCHEMA", "ecommerce") or "ecommerce",
    )
    bronze_write_mode: str = field(
        default_factory=lambda: _env("PIPELINE_BRONZE_WRITE_MODE", "overwrite") or "overwrite",
    )
    batch_id: Optional[str] = field(default_factory=lambda: _env("PIPELINE_BATCH_ID"))

    bronze_customers_table: str = "bronze_customers"
    bronze_orders_table: str = "bronze_orders"
    bronze_products_table: str = "bronze_products"
    bronze_ingest_audit_table: str = "bronze_ingest_audit"

    silver_write_mode: str = field(
        default_factory=lambda: _env("PIPELINE_SILVER_WRITE_MODE", "overwrite") or "overwrite",
    )
    run_id: Optional[str] = field(default_factory=lambda: _env("PIPELINE_RUN_ID"))

    silver_customers_table: str = "silver_customers"
    silver_orders_table: str = "silver_orders"
    silver_products_table: str = "silver_products"
    silver_dq_metrics_table: str = "silver_dq_metrics"
    silver_dq_report_table: str = "silver_dq_report"

    gold_write_mode: str = field(
        default_factory=lambda: _env("PIPELINE_GOLD_WRITE_MODE", "overwrite") or "overwrite",
    )

    gold_sales_by_product_table: str = "gold_sales_by_product"
    gold_revenue_by_customer_table: str = "gold_revenue_by_customer"
    gold_daily_weekly_trends_table: str = "gold_daily_weekly_trends"
    gold_customer_segmentation_table: str = "gold_customer_segmentation"

    customers_csv: str = "customers.csv"
    orders_csv: str = "orders.csv"
    products_csv: str = "products.csv"

    def resolved_batch_id(self) -> str:
        if self.batch_id:
            return self.batch_id
        return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    def resolved_run_id(self) -> str:
        if self.run_id:
            return self.run_id
        return self.resolved_batch_id()

    def source_path(self, filename: str) -> str:
        base = self.source_base_path.rstrip("/")
        return f"{base}/{filename}"

    def qualified_table_name(self, table_name: str) -> str:
        if self.catalog:
            return f"{self.catalog}.{self.schema_name}.{table_name}"
        return f"{self.schema_name}.{table_name}"

    def bronze_table_names(self) -> Dict[str, str]:
        return {
            "customers": self.bronze_customers_table,
            "orders": self.bronze_orders_table,
            "products": self.bronze_products_table,
        }


def load_config(**overrides: object) -> PipelineConfig:
    """Build config from defaults, environment variables, and explicit overrides."""
    config = PipelineConfig()
    if not overrides:
        return config

    field_names = {field_def.name for field_def in fields(PipelineConfig)}
    valid_overrides = {
        key: value
        for key, value in overrides.items()
        if key in field_names and value is not None
    }
    return replace(config, **valid_overrides)
