# 01 · Extract — Landing the Raw Data

Unpack the uploaded `olist_zip.zip` into the `olist/` landing folder inside the volume, and verify all 9 CSVs arrived.

> Community Edition has no `unzip` CLI, so extraction happens with Python's standard `zipfile` library, streamed so it never pins the zip (44 MB) fully in memory twice.

## Cell 1 (Python — extract)

```python
import zipfile

ZIP_PATH  = "/Volumes/shopstream/shopstream/raw_uploads/olist_zip.zip"
OUT_DIR   = "/Volumes/shopstream/shopstream/raw_uploads/olist"

with zipfile.ZipFile(ZIP_PATH) as zf:
    for member in zf.infolist():
        if not member.is_dir() and not member.filename.startswith("__MACOSX"):
            with zf.open(member) as src, open(f"/dbfs{OUT_DIR}/{member.filename}", "wb") as dst:
                dst.write(src.read())
            print("extracted:", member.filename)
```

Note: `/dbfs` prefix is required when writing with plain Python file handles on Databricks.

## Cell 2 (Python — verify all 9 files landed)

```python
landed = sorted(f.name for f in dbutils.fs.ls(OUT_DIR))
for f in landed:
    print(f)
assert len(landed) == 9, f"Expected 9 CSVs, found {len(landed)}"
```

Expected files:

```
olist_customers_dataset.csv            olist_order_items_dataset.csv
olist_order_payments_dataset.csv       olist_order_reviews_dataset.csv
olist_orders_dataset.csv               olist_products_dataset.csv
olist_sellers_dataset.csv              product_category_name_translation.csv
```

> The 8 problem-domain CSVs flow into Bronze via COPY INTO (notebook **02**).
> `product_category_name_translation.csv` is loaded too — it becomes the english category
> enrichment used by the Silver + Gold layers.