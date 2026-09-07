"""
Explicit Bronze schemas: all source business columns as STRING plus ingest metadata.

Casting and validation happen in Silver, not Bronze.
"""

from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

BRONZE_METADATA_FIELDS = [
    StructField("_ingest_ts", TimestampType(), nullable=False),
    StructField("_source_file", StringType(), nullable=False),
    StructField("_source_path", StringType(), nullable=False),
    StructField("_batch_id", StringType(), nullable=False),
    StructField("_source_row_num", LongType(), nullable=False),
    StructField("_corrupt_record", StringType(), nullable=True),
]

CUSTOMERS_SOURCE_FIELDS = [
    StructField("customer_id", StringType(), nullable=True),
    StructField("customer_name", StringType(), nullable=True),
    StructField("email", StringType(), nullable=True),
    StructField("country", StringType(), nullable=True),
    StructField("signup_date", StringType(), nullable=True),
    StructField("customer_segment", StringType(), nullable=True),
    StructField("lifetime_value", StringType(), nullable=True),
]

ORDERS_SOURCE_FIELDS = [
    StructField("order_id", StringType(), nullable=True),
    StructField("customer_id", StringType(), nullable=True),
    StructField("order_date", StringType(), nullable=True),
    StructField("product_id", StringType(), nullable=True),
    StructField("quantity", StringType(), nullable=True),
    StructField("unit_price", StringType(), nullable=True),
    StructField("total_amount", StringType(), nullable=True),
    StructField("order_status", StringType(), nullable=True),
    StructField("payment_date", StringType(), nullable=True),
]

PRODUCTS_SOURCE_FIELDS = [
    StructField("product_id", StringType(), nullable=True),
    StructField("product_name", StringType(), nullable=True),
    StructField("category", StringType(), nullable=True),
    StructField("price", StringType(), nullable=True),
    StructField("cost", StringType(), nullable=True),
    StructField("stock_quantity", StringType(), nullable=True),
    StructField("reorder_level", StringType(), nullable=True),
]

CUSTOMERS_BRONZE_SCHEMA = StructType(CUSTOMERS_SOURCE_FIELDS + BRONZE_METADATA_FIELDS)
ORDERS_BRONZE_SCHEMA = StructType(ORDERS_SOURCE_FIELDS + BRONZE_METADATA_FIELDS)
PRODUCTS_BRONZE_SCHEMA = StructType(PRODUCTS_SOURCE_FIELDS + BRONZE_METADATA_FIELDS)

ENTITY_SCHEMAS = {
    "customers": CUSTOMERS_BRONZE_SCHEMA,
    "orders": ORDERS_BRONZE_SCHEMA,
    "products": PRODUCTS_BRONZE_SCHEMA,
}

SOURCE_COLUMN_NAMES = {
    "customers": [field.name for field in CUSTOMERS_SOURCE_FIELDS],
    "orders": [field.name for field in ORDERS_SOURCE_FIELDS],
    "products": [field.name for field in PRODUCTS_SOURCE_FIELDS],
}
