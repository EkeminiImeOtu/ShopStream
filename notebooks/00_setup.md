# 00 · Setup — Catalog, Schema & Volumes

Bootstrap the Unity Catalog objects for the ShopStream lakehouse on **Databricks Community Edition**.

## Cell 1 (SQL — create catalog)

```sql
CREATE CATALOG IF NOT EXISTS shopstream;
```

## Cell 2 (SQL — create schemas)

```sql
CREATE SCHEMA IF NOT EXISTS shopstream.shopstream;   -- bronze + silver + raw volume
CREATE SCHEMA IF NOT EXISTS shopstream.streaming;    -- Auto Loader streaming tables
CREATE SCHEMA IF NOT EXISTS shopstream.gold;         -- star schema + metrics
```

## Cell 3 (SQL — create the raw uploads volume)

```sql
CREATE VOLUME IF NOT EXISTS shopstream.shopstream.raw_uploads;
```

Upload `olist_zip.zip` here via the **Catalog → shopstream.shopstream → raw_uploads** → *Upload files to this volume* button.

Paths used throughout the project:

```
/Volumes/shopstream/shopstream/raw_uploads/olist_zip.zip
/Volumes/shopstream/shopstream/raw_uploads/olist/           # extracted CSVs
/Volumes/shopstream/shopstream/streaming_landing/orders/    # streaming drops
/Volumes/shopstream/shopstream/checkpoints/                 # streaming checkpoints
```