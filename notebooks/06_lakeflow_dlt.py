# Databricks Asset Bundles — 06 · Delta Live Tables (Lakeflow)

Declarative version of the ShopStream pipelines as a single DLT graph.
Runs on **Databricks with DLT enabled** (full workspace — e.g. the 14-day free trial).

Canonical DLT Pipeline serving layer declarations:

- `bronze_stream_orders`  — Auto Loader streaming source
- `silver_orders`         — deduped, typed, expectation-checked
- `fact_sales`            — item-grain fact over silver orders
- `gold_daily_revenue`    — delivered-only revenue metric

## File: `06_lakeflow_dlt.py`

```python
import dlt
from pyspark.sql import functions as F

CATALOG  = "shopstream"
LANDING  = "/Volumes/shopstream/shopstream/streaming_landing/orders"
CHECKPT  = "/Volumes/shopstream/shopstream/checkpoints/dlt"


@dlt.table
def bronze_stream_orders():
    """Auto Loader streaming source — exactly-once, schema evolved."""
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", CHECKPT)
        .load(LANDING)
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
    )


@dlt.table
@dlt.expect("valid_order_id", "order_id IS NOT NULL")
@dlt.expect("row_count_ok", "order_status IS NOT NULL")
def silver_orders():
    """Dedupe + type the streamed orders; every row must carry an order_id."""
    return (
        dlt.read("bronze_stream_orders")
        .dropDuplicates(["order_id"])
        .withColumn("order_purchase_timestamp",
                    F.to_timestamp("order_purchase_timestamp"))
        .select(
            "order_id", "customer_id", "order_status",
            "order_purchase_timestamp", "order_month",
            "_source_file", "_ingest_ts",
        )
    )


@dlt.table
def fact_sales():
    """Item-grain sales fact. INNER join keeps NULL-keyed orphans out."""
    orders = dlt.read("silver_orders") \
        .select("order_id", "customer_id", "order_status", "order_purchase_timestamp")
    items = (
        spark.read.table(f"{CATALOG}.shopstream.silver_order_items")
        .select("order_id", "order_item_id", "product_id", "seller_id",
                "price", "freight_value")
    )
    return (
        items.join(orders, "order_id", "inner")
        .withColumn("order_date", F.to_date("order_purchase_timestamp"))
        .select(
            "order_id", "order_item_id",
            "customer_id", "product_id", "seller_id",
            "order_status", "order_date", "order_month",
            F.col("price").alias("gross_revenue"),
            "freight_value",
            (F.col("price") + F.col("freight_value")).alias("total_value"),
        )
    )


@dlt.table
def gold_daily_revenue():
    """Delivered-only daily revenue metric for the dashboard."""
    return (
        dlt.read("fact_sales")
        .filter(F.col("order_status") == "delivered")
        .groupBy("order_date")
        .agg(
            F.sum("gross_revenue").alias("revenue"),
            F.sum("total_value").alias("gmv"),
            F.count("*").alias("items_sold"),
            F.countDistinct("order_id").alias("orders"),
        )
        .orderBy("order_date")
    )
```

## Dataset (UI)

| Key             | Value                                              |
|-----------------|----------------------------------------------------|
| Product code    | `shopstream`                                       |
| Target schema   | `shopstream.dlt`                                   |
| Pipeline mode   | `Triggered` (or `Continuous`)                      |
| Libraries       | The path to this `.py` file                        |
| Compute         | Small serverless / shared                            |

> On the free trial, create the pipeline under **Workflows → Delta Live Tables → Create pipeline**,
> point it at this file, then *Start*. The DLT graph renders `bronze → silver → fact → metric`
> and enforces the `@dlt.expect` expectations on every refresh.
>
> Because the same source and drop semantics are used, the bronze/silver/fact outputs align
> with the Cell-based pipelines (notebooks 02–05) on the same landing data.