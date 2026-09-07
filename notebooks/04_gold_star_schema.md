# 04 · Gold Layer — Star Schema & Analytics

Notebook for Databricks Community Edition (`04_gold_star_schema`).

Flow:
- Build a `gold` schema with star-schema dimension + fact tables
- `dim_customer`, `dim_product`, `dim_seller`, `dim_date`
- `fact_sales` (item-grain sales with status), `gold_daily_revenue`
- Analytical queries = the future dashboard's SQL

---

## Cell 1 (Markdown)

```md
# 04 · Gold Layer — Star Schema & Analytics

Purpose: business-ready models on a classic star schema.
- dimensions: customer, product, seller, date (hierarchy of Y/M/Q/D)
- fact: item-grain sales, joined from silver_order_items → silver_orders (INNER join
  so the 1 NULL-keyed junk row and orphan data stay OUT of the business layer)
- metrics: gold_daily_revenue (delivered orders only)
```

---

## Cell 2 (Python — create gold schema + config)

```python
from pyspark.sql import functions as F

CATALOG = "shopstream"
GOLD    = f"{CATALOG}.gold"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD}")
```

---

## Cell 3 (Python — dim_date)

```python
dates = spark.sql("""
    SELECT explode(sequence(to_date('2016-09-01'), to_date('2018-12-31'))) AS date
""")

dim_date = (dates
    .withColumn("date_key",    F.date_format("date", "yyyyMMdd").cast("int"))
    .withColumn("year",        F.year("date"))
    .withColumn("month",       F.month("date"))
    .withColumn("day",         F.dayofmonth("date"))
    .withColumn("month_name",  F.date_format("date", "MMM"))
    .withColumn("quarter",     F.quarter("date"))
    .withColumn("day_of_week", F.dayofweek("date"))
    .withColumn("is_weekend",  F.when(F.dayofweek("date").isin(1, 7), 1).otherwise(0))
    .select("date_key", "date", "year", "month", "month_name", "quarter",
            "day", "day_of_week", "is_weekend"))

dim_date.write.mode("overwrite").format("delta").saveAsTable(f"{GOLD}.dim_date")
print("dim_date:", dim_date.count())   # days in range
```

---

## Cell 4 (Python — dim_customer, dim_product, dim_seller)

```python
# dim_customer — one row per customer (+ unique customer id for cohort analysis)
dim_customer = spark.read.table("shopstream.shopstream.silver_customers") \
    .select("customer_id", "customer_unique_id",
            "customer_zip_code_prefix", "customer_city", "customer_state")
dim_customer.write.mode("overwrite").format("delta").saveAsTable(f"{GOLD}.dim_customer")

# dim_product — english category added in silver
dim_product = spark.read.table("shopstream.shopstream.silver_products") \
    .select("product_id", "product_category_name", "product_category_name_english",
            "product_name_length", "product_description_length",
            "product_photos_qty", "product_weight_g",
            "product_length_cm", "product_height_cm", "product_width_cm")
dim_product.write.mode("overwrite").format("delta").saveAsTable(f"{GOLD}.dim_product")

# dim_seller
dim_seller = spark.read.table("shopstream.shopstream.silver_sellers") \
    .select("seller_id", "seller_zip_code_prefix", "seller_city", "seller_state")
dim_seller.write.mode("overwrite").format("delta").saveAsTable(f"{GOLD}.dim_seller")

print("dims:", dim_customer.count(), dim_product.count(), dim_seller.count())
# expect 99441 | 32951 | 3095
```

---

## Cell 5 (Python — fact_sales, item grain)

```python
items   = spark.read.table("shopstream.shopstream.silver_order_items")
orders  = spark.read.table("shopstream.shopstream.silver_orders") \
    .select("order_id", "customer_id", "order_status", "order_purchase_timestamp", "order_month")

fact_sales = (items
    .join(orders, "order_id", "inner")              # only valid orders → orphan stays out
    .withColumn("order_date", F.to_date("order_purchase_timestamp"))
    .select("order_id", "order_item_id",
            "customer_id", "product_id", "seller_id",
            "order_status", "order_date", "order_month",
            F.col("price").alias("gross_revenue"),
            "freight_value",
            (F.col("price") + F.col("freight_value")).alias("total_value")))

fact_sales.write.mode("overwrite").format("delta") \
    .partitionBy("order_month").saveAsTable(f"{GOLD}.fact_sales")
print("fact_sales:", fact_sales.count())   # 112650 verified (junk row excluded via inner join)
```

---

## Cell 6 (Python — gold_daily_revenue metric table)

```python
daily_revenue = (spark.read.table(f"{GOLD}.fact_sales")
    .filter(F.col("order_status") == "delivered")
    .groupBy("order_date")
    .agg(F.sum("gross_revenue").alias("revenue"),
         F.sum("total_value")    .alias("gmv"),
         F.count("*")            .alias("items_sold"),
         F.countDistinct("order_id").alias("orders"))
    .orderBy("order_date"))

daily_revenue.write.mode("overwrite").format("delta").saveAsTable(f"{GOLD}.gold_daily_revenue")
print("gold_daily_revenue days:", daily_revenue.count())
```

---

## Cell 7 (SQL — verify counts)

```sql
SELECT 'dim_customer'  AS tbl, count(*) AS rows FROM shopstream.gold.dim_customer
UNION ALL SELECT 'dim_product',   count(*) FROM shopstream.gold.dim_product
UNION ALL SELECT 'dim_seller',    count(*) FROM shopstream.gold.dim_seller
UNION ALL SELECT 'dim_date',      count(*) FROM shopstream.gold.dim_date
UNION ALL SELECT 'fact_sales',    count(*) FROM shopstream.gold.fact_sales
UNION ALL SELECT 'gold_daily_revenue', count(*) FROM shopstream.gold.gold_daily_revenue;
-- expect 99441 | 32951 | 3095 | 852 | 112650 | days-with-delivered-orders
```

---

## Cell 8 (SQL — analytics: monthly revenue trend)

```sql
SELECT order_month,
       round(sum(gross_revenue), 2)          AS revenue,
       round(sum(total_value), 2)            AS gmv,
       count(DISTINCT order_id)              AS orders,
       count(DISTINCT customer_id)           AS customers
FROM shopstream.gold.fact_sales
WHERE order_status = 'delivered'
GROUP BY order_month
ORDER BY order_month;
```

---

## Cell 9 (SQL — top product categories by revenue, window function + CTE)

```sql
WITH cat_revenue AS (
  SELECT p.product_category_name_english AS category,
         round(sum(f.gross_revenue), 2)    AS revenue
  FROM shopstream.gold.fact_sales f
  JOIN shopstream.gold.dim_product p ON f.product_id = p.product_id
  WHERE f.order_status = 'delivered'
  GROUP BY 1
)
SELECT category, revenue,
       rank() OVER (ORDER BY revenue DESC) AS rnk
FROM cat_revenue
ORDER BY rnk
LIMIT 10;
```

---

## Cell 10 (SQL — one-time vs repeat customers, cohort style)

```sql
WITH purchases AS (
  SELECT customer_unique_id, count(DISTINCT order_id) AS orders
  FROM shopstream.gold.fact_sales
  GROUP BY 1
)
SELECT CASE WHEN orders >= 2 THEN 'repeat customer'
            ELSE 'one-time customer' END  AS customer_type,
       count(*)                            AS customers,
       round(100.0 * count(*) / sum(count(*)) OVER (), 2) AS pct
FROM purchases
GROUP BY 1;
```

---

## Cell 11 (SQL — delivery performance by state)

```sql
SELECT c.customer_state,
       count(*)                                                     AS orders,
       round(avg(date_diff(order_delivered_customer_date,
                           order_purchase_timestamp)), 1)           AS avg_delivery_days,
       round(100.0 * sum(CASE WHEN order_delivered_customer_date > order_estimated_delivery_date
                              THEN 1 ELSE 0 END) / count(*), 1)     AS late_pct
FROM shopstream.shopstream.silver_orders o
JOIN shopstream.shopstream.silver_customers c USING (customer_id)
WHERE order_status = 'delivered'
GROUP BY 1
ORDER BY orders DESC
LIMIT 10;
```

---

## Cell 12 (SQL — optimize the fact table)

```sql
OPTIMIZE shopstream.gold.fact_sales ZORDER BY (customer_id);
ANALYZE TABLE shopstream.gold.fact_sales COMPUTE STATISTICS;
```

---

## Expected highlights to note in the portfolio

- fact_sales = 112,650 rows (item grain, only valid orders — the NULL-keyed junk row is
  quarantined by the INNER join)
- gold_daily_revenue proves the metrics layer responds to status filters
- Query results: revenue trend over 2016→2018, top categories (health_beauty, watch_gifts, bed_bath_table…),
  ~repeat-customer split, and per-state delivery latency — all query data for the dashboard phase.