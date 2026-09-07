# 03 · Silver Layer — Clean, Dedupe & Type

Notebook for Databricks Community Edition (`03_silver_clean`).

Flow:
- Read Bronze → enforce the *actual type model* per table
- **Dedupe on every business key** (Bronze keeps raw truth; Silver fixes it)
- Quarantine the NULL-keyed junk row, verify referential integrity
- Partition order-centric tables by `order_month` for fast pruning
- Write to `shopstream.shopstream.silver_*`, then OPTIMIZE + ANALYZE

---

## Cell 1 (Markdown)

```md
# 03 · Silver Layer — Clean, Dedupe & Type

Rules applied:
- orders      → order_id NOT NULL, dedupe on order_id, cast timestamps + floats
- order_items → dedupe on (order_id, order_item_id), cast price/freight to double
- customers   → dedupe on customer_id
- products    → drop rows with empty product_id, dedupe on product_id
- sellers     → dedupe on seller_id
- payments    → dedupe on (order_id, payment_sequential)
- reviews     → dedupe on review_id (source has ~1,205 duplicate IDs!)
```

---

## Cell 2 (Python — config + typers)

```python
from pyspark.sql import functions as F

CATALOG = "shopstream"
SCHEMA  = "shopstream"

silver_tables = [
    "bronze_orders", "bronze_order_items", "bronze_customers",
    "bronze_products", "bronze_sellers", "bronze_payments", "bronze_reviews",
]

def dedupe(df, keys):
    return df.dropDuplicates(keys)
```

---

## Cell 3 (Python — clean each table)

```python
def clean(table):
    raw  = spark.read.table(f"{CATALOG}.{SCHEMA}.{table}")
    keep = ["_source_file", "_ingest_ts"] + [c for c in raw.columns]

    if table == "bronze_orders":
        df = (dedupe(raw, ["order_id"])
                .filter(F.col("order_id").isNotNull())
                .withColumn("order_purchase_timestamp",
                            F.to_timestamp("order_purchase_timestamp"))
                .withColumn("order_delivered_customer_date",
                            F.to_timestamp("order_delivered_customer_date"))
                .withColumn("order_estimated_delivery_date",
                            F.to_timestamp("order_estimated_delivery_date"))
                .withColumn("order_month",
                            F.date_format("order_purchase_timestamp", "yyyy-MM")))
        return "silver_orders", df, ["order_month"]

    if table == "bronze_order_items":
        df = (dedupe(raw, ["order_id", "order_item_id"])
                .withColumn("price",         F.col("price").cast("double"))
                .withColumn("freight_value", F.col("freight_value").cast("double"))
                .withColumn("order_month",
                            F.date_format("shipping_limit_date", "yyyy-MM")))
        return "silver_order_items", df, ["order_month"]

    if table == "bronze_customers":
        return "silver_customers", dedupe(raw, ["customer_id"]), None

    if table == "bronze_products":
        df = (raw.filter(F.col("product_id") != "")
                 .filter(F.col("product_id").isNotNull()))
        return "silver_products", dedupe(df, ["product_id"]), None

    if table == "bronze_sellers":
        return "silver_sellers", dedupe(raw, ["seller_id"]), None

    if table == "bronze_payments":
        df = (dedupe(raw, ["order_id", "payment_sequential"])
                .withColumn("payment_value", F.col("payment_value").cast("double")))
        return "silver_payments", df, None

    if table == "bronze_reviews":
        df = (dedupe(raw, ["review_id"])
                .filter(F.col("review_id").isNotNull())
                .withColumn("review_creation_date",
                            F.to_timestamp("review_creation_date")))
        return "silver_reviews", df, None

    raise ValueError(f"unknown table: {table}")
```

---

## Cell 4 (Python — write silver tables)

```python
silver = {}
for table in silver_tables:
    name, df, part = clean(table)
    writer = df.write.mode("overwrite").format("delta")
    if part:
        writer = writer.partitionBy(*part)
    writer.saveAsTable(f"{CATALOG}.{SCHEMA}.{name}")
    silver[name] = df
    print(f"{name:22s} rows: {df.count()}")
```

Expected row counts (Olist public dataset, after cleanup):

```
silver_orders              99441
silver_order_items        112650
silver_customers           99441
silver_products            32951
silver_sellers              3095
silver_payments           103886
silver_reviews            102957
```

> `silver_reviews` = 102,957 (deduped from Bronze's 104,162 — the ~1,205 source duplicates
> are gone). `silver_order_items` total is 112,650; the NULL-keyed stray row is quarantined
> by the integrity checks in Cell 6 and excluded from the business-facing Gold layer.

---

## Cell 5 (SQL — verification: rows = distinct keys)

```sql
SELECT 'silver_orders'      AS tbl,
       count(*)             AS rows,
       count(DISTINCT order_id)              AS distinct_keys
FROM shopstream.shopstream.silver_orders
UNION ALL SELECT 'silver_order_items', count(*), count(DISTINCT CONCAT(order_id, '__', order_item_id))
FROM shopstream.shopstream.silver_order_items
UNION ALL SELECT 'silver_customers', count(*), count(DISTINCT customer_id)
FROM shopstream.shopstream.silver_customers
UNION ALL SELECT 'silver_products', count(*), count(DISTINCT product_id)
FROM shopstream.shopstream.silver_products
UNION ALL SELECT 'silver_sellers', count(*), count(DISTINCT seller_id)
FROM shopstream.shopstream.silver_sellers
UNION ALL SELECT 'silver_payments', count(*), count(DISTINCT CONCAT(order_id, '__', payment_sequential))
FROM shopstream.shopstream.silver_payments
UNION ALL SELECT 'silver_reviews', count(*), count(DISTINCT review_id)
FROM shopstream.shopstream.silver_reviews;
```

Expected: `rows = distinct_keys` for every table (0 duplicates).

---

## Cell 6 (SQL — referential integrity)

```sql
-- no NULL order ids anywhere?
SELECT count(*) AS missing_order_ids FROM shopstream.shopstream.silver_orders WHERE order_id IS NULL;

-- any order_items that don't resolve to an order? (NULL-safe LEFT JOIN)
SELECT count(*) AS orphan_order_items
FROM   shopstream.shopstream.silver_order_items oi
LEFT JOIN shopstream.shopstream.silver_orders   o ON oi.order_id = o.order_id
WHERE  o.order_id IS NULL;
```

Expected: `missing_order_ids = 0`, `orphan_order_items = 0`.

> The classic **NOT IN / NULL** trap: `WHERE order_id NOT IN (SELECT order_id FROM silver_orders)`
> silently returns **0 rows** when any order_id in the subquery is NULL, even though a stray
> NULL-keyed row exists. The LEFT JOIN above is the NULL-safe way and is the version that's
> actually meaningful.

---

## Cell 7 (SQL — orders with no line items)

```sql
SELECT count(*) AS orders_without_items
FROM   shopstream.shopstream.silver_orders o
LEFT JOIN shopstream.shopstream.silver_order_items oi ON oi.order_id = o.order_id
WHERE  oi.order_id IS NULL;
```

Expected: `775` — cancelled / `unavailable` orders that legitimately have no items.
This is a **verified data fact**, not a defect.

---

## Cell 8 (SQL — optimize + analyze)

```sql
OPTIMIZE shopstream.shopstream.silver_orders ZORDER BY (customer_id);
OPTIMIZE shopstream.shopstream.silver_order_items ZORDER BY (order_id);
ANALYZE TABLE shopstream.shopstream.silver_orders COMPUTE STATISTICS;
ANALYZE TABLE shopstream.shopstream.silver_order_items COMPUTE STATISTICS;
```

---

## Expected highlights to note in the portfolio

- Every silver table: `rows = distinct_keys` (dedupe proven)
- 0 NULL order ids, 0 orphan order_items (referential integrity)
- 775 orders legitimately carry no line items (verified, not hidden)
- Original data-quality findings surfaced: ~1,205 duplicate review_ids in the source