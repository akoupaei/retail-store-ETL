from pyspark import pipelines as dp
from pyspark.sql.functions import col

CATALOG = "cscie103_catalog"
SCHEMA = "final_project"
SOURCE_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/data/"


@dp.table(name="sales_train_raw", comment="Raw training sales data streamed in from CSV files")
def sales_train_raw():
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

@dp.table(name="sales_test_raw", comment="Raw test sales data streamed in from CSV files")
def sales_test_raw():
    """
    Reads the raw test sales CSV data as a streaming source.
    """
    path = SOURCE_PATH

    schema = """
        id INTEGER,
        date DATE,
        store_nbr INTEGER,
        family STRING,
        onpromotion INTEGER
    """

    return (
        spark.readStream.schema(schema)
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .load(f"{path}test*.csv")
    )

@dp.table(name="stores_raw", comment="Raw store data streamed in from CSV files")
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
            
@dp.table(name="oil_prices_raw", comment="Raw oil price data streamed in from CSV files")
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

@dp.table(name="holidays_raw", comment="Raw holidays events data streamed in from CSV files")
def holidays_raw():
    """
    Reads the raw holidays events data as a streaming source.
    """
    path = SOURCE_PATH

    schema = """
        date DATE,
        type STRING,
        locale STRING,
        locale_name STRING,
        description STRING,
        transferred BOOLEAN
    """ 

    return (
        spark.readStream.schema(schema)
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .load(f"{path}holidays*.csv")
    )


@dp.table(name="transactions_raw", comment="Raw transactions data streamed in from CSV files")
def transactions_raw():
    """
    Reads the raw transactions data as a streaming source.
    """
    path = SOURCE_PATH

    schema = """
        date DATE,
        store_nbr INTEGER,
        transactions INTEGER
    """

    return (
        spark.readStream.schema(schema)
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .load(f"{path}transactions*.csv")
    )
