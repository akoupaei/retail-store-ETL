from pyspark import pipelines as dp
from pyspark.sql.functions import col

CATALOG = "cscie103_catalog"
SCHEMA = "final_project"
SOURCE_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/data/"


@dp.table(name="bronze_train_sales", comment="Raw training sales data streamed in from CSV files")
def train_sales_raw():
    """
    Reads the raw training sales CSV data as a streaming source.
    """
    path = SOURCE_PATH

    schema = """
        id INTEGER,
        data DATE,
        store_nbr INTEGER,
        family STRING,
        sales DOUBLE,
        onpromotion INTEGER
    """

    return (
        spark.readStream.schema(schema)
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .load(f"{path}train*.csv")
    )

@dp.table(name="bronze_stores", comment="Raw store data streamed in from CSV files")
def stores_raw():
    """
    Reads the raw store data as a streaming source.
    """
    path = SOURCE_PATH

    schema = """
        store_nbr INTEGER,
        city STRING,
        state STRING,
        type STRING,
        cluster INTEGER
    """

    return (
        spark.readStream.schema(schema)
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .load(f"{path}stores*.csv")
    )
            
@dp.table(name="bronze_oil_prices", comment="Raw oil price data streamed in from CSV files")
def oil_prices_raw():
    """
    Reads the raw oil price data as a streaming source.
    """
    path = SOURCE_PATH

    schema = """
        date DATE,
        dcoilwtico DOUBLE
    """

    return (
        spark.readStream.schema(schema)
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .load(f"{path}oil*.csv")
    )



