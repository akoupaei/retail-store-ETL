from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(name="stores_daily_sales")
def stores_daily_sales():
    s = spark.read.table("sales").alias("s")
    t = spark.read.table("transactions").alias("t")
    h = spark.read.table("holidays").alias("h")
    o = spark.read.table("oil_prices").alias("o")
    joined = s.join(t, (s.date == t.date) & (s.store_nbr == t.store_nbr))
    joined = joined.join(h, h.date == s.date, "left")
    joined = joined.join(o, o.date == s.date, "left")
    agg = (
        joined.groupBy(
            "s.date", "s.store_nbr", "city", "state", "store_type", "store_cluster"
        )
        .agg(
            F.round(F.sum("sales"), 2).alias("total_sales"),
            F.round(F.avg("sales"), 2).alias("avg_transcaction_amount"),
            F.sum("onpromotion").alias("total_items_on_promotion"),
            F.countDistinct("product_family").alias("num_product_families"),
            F.round(F.avg("dcoilwtico"), 2).alias("avg_oil_price"),
            F.coalesce(F.max("is_national_holiday"), F.lit(False)).alias(
                "is_national_holiday"
            ),
            F.max("is_weekend").alias("is_weekend"),
            F.max("transactions").alias("daily_transactions"),
        )
        .withColumn(
            "avg_basket_size",
            F.when(
                F.col("daily_transactions") > 0,
                F.round((F.col("total_sales") / F.col("daily_transactions")),2),
            ).otherwise(F.col("total_sales")),
        )
        .withColumn(
            "has_promotion", F.when(F.col("total_items_on_promotion") > 0, 1).otherwise(0)
        )
    )
    return agg


@dp.materialized_view(name="product_family_daily_sales")
def product_family_daily_sales():
    s = spark.read.table("sales").alias("s")
    return (
        s.groupBy("date", "product_family").agg(
            F.round(F.sum("sales"), 2).alias("total_sales"),
            F.countDistinct("store_nbr").alias("num_stores_selling"),
            F.round(F.avg("sales"), 2).alias("avg_sales_per_store"),
            F.sum("onpromotion").alias("total_items_on_promotion"),
        ).withColumn(
            "promotion_penetration",
            F.when(F.col("num_stores_selling") > 0, F.col("total_items_on_promotion") / F.col("num_stores_selling")).otherwise(0)
        )
    )


# @dp.materialized_view(name="store_performance_metrics")
# def store_performance_metrics():
#     pass
