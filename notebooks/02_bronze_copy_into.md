# 02 · Bronze Layer — COPY INTO Ingestion

Notebook for Databricks Community Edition. Copy the cells below into a new notebook named `02_bronze_copy_into`.

> **Ordering matters:** `COPY INTO` requires the target Delta table to already exist.
> So we create 8 empty tables first, then `VALIDATE`, then load.

---

## Cell 1 (Markdown)

```md
# 02 · Bronze Layer — COPY INTO

Ingest the raw Olist CSVs from the volume into Bronze Delta tables.

Deliberate production choices:
- **`inferSchema = false`** → everything lands as STRING at ingest.
  Real raw feeds are untrusted; we never let source types leak in.
  Types are enforced deliberately in the Silver layer.
- **Audit columns** on every row: `_source_file` and `_ingest_ts`.
- **Idempotent**: `COPY INTO` skips files already loaded. Re-run freely.
```

---

## Cell 2 (Python — config + verify landing zone)

```python
# --- CONFIG -----------------------------------------------------------------
CATALOG = "shopstream"
SCHEMA  = "shopstream"
RAW_DIR = "/Volumes/shopstream/shopstream/raw_uploads/olist"

bronze_tables = {
    "bronze_orders":             "olist_orders_dataset.csv",
    "bronze_order_items":        "olist_order_items_dataset.csv",
    "bronze_customers":          "olist_customers_dataset.csv",
    "bronze_products":           "olist_products_dataset.csv",
    "bronze_sellers":            "olist_sellers_dataset.csv",
    "bronze_payments":           "olist_order_payments_dataset.csv",
    "bronze_reviews":            "olist_order_reviews_dataset.csv",
    "bronze_product_categories": "product_category_name_translation.csv",
}

# Sanity check: every CSV we expect is actually in the landing folder
landed = [f.name for f in dbutils.fs.ls(RAW_DIR)]
missing = [v for v in bronze_tables.values() if v not in landed]
assert not missing, f"Missing files in landing zone: {missing}"
print(f"Landing zone OK - {len(landed)} files present")
```

---

## Cell 3 (Python — create 8 empty Bronze tables)

`COPY INTO` will NOT auto-create the table on Community Edition. And an empty-schema
`CREATE TABLE ... USING DELTA` is exactly how you create the placeholder that
`COPY INTO` then fills + infer-schemas.

```python
for table in bronze_tables:
    full_table = f"{CATALOG}.{SCHEMA}.{table}"
    spark.sql(f"CREATE TABLE IF NOT EXISTS {full_table} USING DELTA")
    print(f"ready: {full_table}")
```

---

## Cell 4 (SQL — dry-run VALIDATE before loading)

```sql
COPY INTO shopstream.shopstream.bronze_orders
FROM (
  SELECT *, current_timestamp() AS _ingest_ts, _metadata.file_name AS _source_file
  FROM '/Volumes/shopstream/shopstream/raw_uploads/olist/olist_orders_dataset.csv'
)
FILEFORMAT = CSV
VALIDATE 10 ROWS
FORMAT_OPTIONS ('header' = 'true')
```

> Validates 10 rows against the now-existing empty table. No data is written.

---

## Cell 5 (Python — COPY INTO all 8 tables)

```python
for table, csv_file in bronze_tables.items():
    full_table = f"{CATALOG}.{SCHEMA}.{table}"
    file_path  = f"{RAW_DIR}/{csv_file}"

    res = spark.sql(f"""
        COPY INTO {full_table}
        FROM (
          SELECT *, current_timestamp() AS _ingest_ts,
                 _metadata.file_name AS _source_file
          FROM '{file_path}'
        )
        FILEFORMAT = CSV
        FORMAT_OPTIONS ('header' = 'true')
    """).collect()

    print(f"{table:26s} rows loaded: {res[0][0]}")
```

Expected result (Olist public dataset):

```
bronze_orders              rows loaded: 99441
bronze_order_items         rows loaded: 98666
bronze_customers           rows loaded: 99441
bronze_products            rows loaded: 32951
bronze_sellers             rows loaded: 3095
bronze_payments            rows loaded: 99441
bronze_reviews             rows loaded: 99224
bronze_product_categories  rows loaded: 71
```

---

## Cell 6 (SQL — row counts per bronze table)

```sql
SELECT table_name, row_count FROM (
  SELECT 'bronze_orders'              AS table_name, count(*) AS row_count FROM shopstream.shopstream.bronze_orders
  UNION ALL SELECT 'bronze_order_items',        count(*) FROM shopstream.shopstream.bronze_order_items
  UNION ALL SELECT 'bronze_customers',          count(*) FROM shopstream.shopstream.bronze_customers
  UNION ALL SELECT 'bronze_products',           count(*) FROM shopstream.shopstream.bronze_products
  UNION ALL SELECT 'bronze_sellers',            count(*) FROM shopstream.shopstream.bronze_sellers
  UNION ALL SELECT 'bronze_payments',           count(*) FROM shopstream.shopstream.bronze_payments
  UNION ALL SELECT 'bronze_reviews',            count(*) FROM shopstream.shopstream.bronze_reviews
  UNION ALL SELECT 'bronze_product_categories', count(*) FROM shopstream.shopstream.bronze_product_categories
)
ORDER BY table_name;
-- Expected: same numbers as Cell 5
```

---

## Cell 7 (SQL — peek at audit columns)

```sql
SELECT order_id, customer_id, order_status, _source_file, _ingest_ts
FROM shopstream.shopstream.bronze_orders
LIMIT 5;
```

Note: `_ingest_ts` is the batch load timestamp (all rows in this batch share it),
`_source_file` tells you exactly which file each row came from.

---

## Cell 8 (Markdown)

```md
## Idempotency proof (do this for the demo / screenshots)

1. Run **Cell 5 again** (second COPY INTO over the same files).
2. It prints `rows loaded: 0` — no duplicates, no re-parse.
3. Re-run Cell 6 — counts are identical.
4. Run Cell 9 below to read the table history and see why.
```

---

## Cell 9 (SQL — table history)

```sql
DESCRIBE HISTORY shopstream.shopstream.bronze_orders;
```

You'll see each COPY INTO as its own Delta COMMIT; the re-runs add a commit with
**0 added files** because `COPY INTO` tracks which files it already loaded.