# ShopStream — End-to-End Databricks Lakehouse

A production-style lakehouse on **Databricks Community Edition** (built 100 % free), running real
[Brazilian E-Commerce by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
data — 100k+ orders — through the medallion architecture on Delta Lake.

```
CSV drops ──▶ RAW (volume) ──▶ BRONZE (COPY INTO / Auto Loader) ──▶ SILVER (dedupe + type)
                                                                       ──▶ GOLD (star schema) ──▶ SQL analytics
                                                                       ──▶ STREAMING (exactly-once) ──▶ silver_stream_orders
```

## Repository layout

```
ShopStream/
├─ notebooks/                        # Databricks notebooks, cell by cell (.md)
│  ├─ 00_setup.md                    #   catalog / schemas / volume bootstrap
│  ├─ 01_extract_landing.md          #   unzip the raw volume
│  ├─ 02_bronze_copy_into.md         #   idempotent COPY INTO ingestion
│  ├─ 03_silver_clean.md             #   dedupe, typing, integrity checks
│  ├─ 04_gold_star_schema.md         #   star schema + analytical SQL
│  ├─ 05_streaming_auto_loader.md    #   Auto Loader exactly-once streaming
│  └─ 06_lakeflow_dlt.py             #   Delta Live Tables (runs on 14-day trial)
├─ architecture/shopstream-architecture.html
└─ README.md
```

## Verified results (actual run)

| Layer     | Table                | Rows          | Check                                           |
|-----------|----------------------|---------------|-------------------------------------------------|
| Bronze    | `bronze_reviews`     | 104,162       | source keeps ~1,205 duplicate `review_id`s     |
| Silver    | `silver_reviews`     | 102,957       | `rows = distinct_keys` on every silver table   |
| Silver    | `silver_order_items` | 112,650       | 0 NULL order_ids, 0 orphans (LEFT JOIN, NULL-safe) |
| Silver    | orders w/o items     | 775           | verified, not hidden (cancelled / unavailable) |
| Gold      | `fact_sales`         | 112,650       | item-grain, INNER join keeps junk out          |
| Gold      | `dim_date`           | 852           | 2016-09-01 → 2018-12-31                        |
| Streaming | `bronze_stream_orders` | 6,000       | Wave A (4k) + Wave B (2k); re-run adds **0**   |

## How to run on Community Edition

1. Notebook `00_setup` — create catalog, schemas, volume; upload `olist_zip.zip`.
2. Notebook `01` — extract the 9 CSVs into the volume.
3. Notebooks `02 → 03 → 04` — Bronze, Silver, Gold layers.
4. Notebook `05` — Auto Loader streaming with the exactly-once proof.
5. Notebook `06` — DLT graph on a full workspace (e.g. the free 14-day trial).

## Data quality findings (interview stories)

- **~1,205 duplicated `review_id`s in the source** — Bronze preserves, Silver dedupes.
- **A NULL-keyed `order_id` row** — quarantined by NULL-safe LEFT JOIN checks; INNER join
  keeps it out of the business model.
- **The `NOT IN` / NULL trap** — `NOT IN` silently returns nothing when a NULL key exists;
  `LEFT JOIN` finds the real row.
- **775 orders with no line items** — legitimate (cancelled / `unavailable`), documented.

## Credits & license

- Dataset: [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce),
  redistributable under **CC BY-NC-SA 4.0**. Covers 2016–2018.
- Built by [Ekemini Otu](https://github.com/EkeminiImeOtu).