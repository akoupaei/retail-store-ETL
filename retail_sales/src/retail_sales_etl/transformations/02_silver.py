from pyspark import pipelines as dp
from pyspark.sql.functions import col, expr, last, first
from pyspark.sql.window import Window


@dp.table(name="holidays", comment="Streaming table containing processed holidays data")
@dp.expect_or_drop("missing_date", "date IS NOT NULL")
@dp.expect_or_drop("missing_event_type", "event_type IS NOT NULL")
@dp.expect_or_drop("missing_locale_data", "locale IS NOT NULL")
def holidays():
    return (
        spark.readStream.table("holidays_raw")
        .selectExpr(
            "date",
            "type AS event_type",
            "locale",
            "locale_name",
            "description",
            "transferred",
        )
        .withColumn("is_national_holiday", expr("locale = 'National'"))
        .withColumn("is_regional_holiday", expr("locale = 'Regional'"))
        .withColumn("is_local_holiday", expr("locale = 'Local'"))
        .withColumn("is_holiday", expr("event_type = 'Holiday'"))
        .withColumn("is_event", expr("event_type = 'Event'"))
        .withColumn(
            "is_special_day", expr("event_type in ('Bridge', 'Work Day', 'Additional')")
        )
    )


@dp.table(
    name="oil_prices", comment="Streaming table containing processed oil prices data"
)
@dp.expect_or_drop("missing_date", "date IS NOT NULL")
@dp.expect_or_drop("missing_oil_price", "dcoilwtico IS NOT NULL")
def oil_prices():
    return spark.readStream.table("oil_prices_raw").selectExpr("date", "dcoilwtico")


@dp.table(name="sales", comment="Streaming table containing processed sales data")
@dp.expect_or_drop("missing_date", "date IS NOT NULL")
@dp.expect_or_drop("missing_store_nbr", "store_nbr IS NOT NULL")
def sales():
    stores_static = spark.read.table("stores_raw")
    joined = spark.readStream.table("sales_train_raw").join(
        stores_static, on="store_nbr", how="left"
    )
    return (
        joined.selectExpr(
            "id",
            "date",
            "store_nbr",
            "city",
            "state",
            "type AS store_type",
            "cluster AS store_cluster",
            "family AS product_family",
            "sales",
            "onpromotion",
        )
        .withColumn("day", expr("dayofmonth(date)"))
        .withColumn("month", expr("month(date)"))
        .withColumn("year", expr("year(date)"))
        .withColumn("day_of_week", expr("dayofweek(date)"))
        .withColumn("week_of_year", expr("weekofyear(date)"))
        .withColumn("is_weekend", expr("day_of_week in (1, 7)"))
        .withColumn("has_promotion", expr("onpromotion = 1"))
    )


valid_transaction = {
    "valid_date": "date is NOT NULL",
    "valid_transaction": "transactions >= 0",
}

@dp.table(
    name="transactions",
    comment="Streaming table containing processed transactions data",
)
@dp.expect_all_or_drop(valid_transaction)
def transactions():
    return spark.readStream.table("transactions_raw").selectExpr(
        "date",
        "store_nbr",
        "transactions",
    )
