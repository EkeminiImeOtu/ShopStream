# 05 · Streaming Layer — Auto Loader (Exactly-Once)

Notebook for Databricks Community Edition (`05_streaming_auto_loader`).

Flow:
- **Auto Loader** (`cloudFiles`) watches `streaming_landing/orders` for new CSV drops
- Two incremental waves are seeded (4,000 then 2,000 rows) to simulate live landing files
- Rows stream into `bronze_stream_orders`, then a silver pass dedupes + types
- **Exactly-once is proven** by re-running the trigger: totals stay 6,000, never 8,000

---

## Cell 1 (Markdown)

```md
# 05 · Streaming Layer — Auto Loader

Why Auto Loader:
- Incremental file discovery with checkpointing → exactly-once pickup
- Schema inference + evolution built in (new files with new columns don't break the run)
- `trigger(availableNow=True)` runs it as a bounded, repeatable batch on CE

Schema of a landed file (sub-sampled from silver_orders):
order_id, customer_id, order_status, order_purchase_timestamp, order_month
```

---

## Cell 2 (Python — create the streaming schema + tables)

```python
from pyspark.sql import functions as F

CATALOG = "shopstream"
STREAM  = f"{CATALOG}.streaming"
LANDING = f"/Volumes/shopstream/shopstream/streaming_landing/orders"
CHECKPOINT = f"/Volumes/shopstream/shopstream/checkpoints/orders"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {STREAM}")
spark.sql(f"DROP TABLE IF EXISTS {STREAM}.bronze_stream_orders")
spark.sql(f"CREATE TABLE IF NOT EXISTS {STREAM}.bronze_stream_orders USING DELTA")
```

> Same CE rule as COPY INTO: the streaming target must exist before the stream writes.

---

## Cell 3 (Python — run the Auto Loader stream)

```python
read_stream = (spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaLocation", CHECKPOINT)
    .load(LANDING)
    .withColumn("_ingest_ts",   F.current_timestamp())
    .withColumn("_source_file", F.input_file_name()))

write_stream = (read_stream.writeStream.format("delta")
    .option("checkpointLocation", CHECKPOINT)
    .option("mergeSchema", "true")
    .outputMode("append")
    .trigger(availableNow=True)          # bounded, re-runnable on CE
    .toTable(f"{STREAM}.bronze_stream_orders"))

query = write_stream.start()
query.awaitTermination()
print("stream finished ->", spark.table(f"{STREAM}.bronze_stream_orders").count())
```

---

## Cell 4 (Python — seed **Wave A**: 4,000 rows)

```python
base = (spark.read.table("shopstream.shopstream.silver_orders")
        .select("order_id", "customer_id", "order_status",
                "order_purchase_timestamp", "order_month"))

wave_a = base.limit(4000)
(wave_a.write.mode("overwrite").option("header", "true")
        .csv(f"{LANDING}"))
```

Run **Cell 3** → bronze_stream_orders should report **4,000**.

---

## Cell 5 (Python — seed **Wave B**: +2,000 more rows)

```python
wave_b = base.subtract(wave_a.select("order_id", "customer_id", "order_status",
                                     "order_purchase_timestamp", "order_month"))
wave_b = wave_b.limit(2000).select("order_id", "customer_id", "order_status",
                                   "order_purchase_timestamp", "order_month")
(wave_b.write.mode("append").option("header", "true")
        .csv(f"{LANDING}"))
```

Run **Cell 3 again** → bronze_stream_orders should now report **6,000**.

---

## Cell 6 (SQL — exactly-once proof, by source file)

```sql
SELECT substring_index(_source_file, '/', -1) AS file, count(*) AS rows_loaded
FROM   shopstream.streaming.bronze_stream_orders
GROUP BY 1
ORDER BY 1;
```

Expected: the Wave A file = 4,000 rows, the Wave B file = 2,000 rows.

---

## Cell 7 (SQL — idempotent re-run proof)

Re-run **Cell 3** once more (no new files land). The count stays **6,000** —
Auto Loader's checkpoint tracked every already-seen file. That's exactly-once pickup:

```sql
SELECT count(*) AS total_rows FROM shopstream.streaming.bronze_stream_orders;
-- still 6000, never 8000
```

---

## Cell 8 (Python — silver_stream_orders)

```python
silver_stream = (spark.read.table(f"{STREAM}.bronze_stream_orders")
    .dropDuplicates(["order_id"])
    .filter(F.col("order_id").isNotNull())
    .withColumn("order_purchase_timestamp",
                F.to_timestamp("order_purchase_timestamp"))
    .select("order_id", "customer_id", "order_status",
            "order_purchase_timestamp", "order_month",
            "_source_file", "_ingest_ts"))

silver_stream = (
    silver_stream.write.mode("overwrite").format("delta")
    .saveAsTable(f"{STREAM}.silver_stream_orders"))

print("silver_stream_orders:", spark.table(f"{STREAM}.silver_stream_orders").count())
```

Expected: **6,000** — same rows as bronze, now deduped and typed.

---

## Cell 9 (SQL — optimize the streaming table)

```sql
OPTIMIZE shopstream.streaming.bronze_stream_orders ZORDER BY (order_id);
OPTIMIZE shopstream.streaming.silver_stream_orders ZORDER BY (order_id);
ANALYZE TABLE shopstream.streaming.silver_stream_orders COMPUTE STATISTICS;
```

---

## Expected highlights to note in the portfolio

- Wave A (4,000) + Wave B (2,000) = **6,000** rows via incremental file pickup
- Re-running the trigger adds **0 rows** → exactly-once pickup, proven
- Schema evolution + checkpointing handled by Auto Loader, no manual bookkeeping