from pyspark import pipelines as dp
from pyspark.sql.functions import col, expr



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
                "transferred"
            )
            .withColumn(
                "is_national_holiday", expr("locale = 'National'")
            )
            .withColumn(
                "is_regional_holiday", expr("locale = 'Regional'")
            )
            .withColumn(
                "is_local_holiday", expr("locale = 'Local'")
            )
            .withColumn(
                "is_holiday", expr("event_type = 'Holiday'")
            )
            .withColumn(
                "is_event", expr("event_type = 'Event'")
            )
            .withColumn(
                "is_special_day", expr("event_type in ('Bridge', 'Work Day', 'Additional')")
            )
    )