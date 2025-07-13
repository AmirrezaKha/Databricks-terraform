# === scripts/base_etl.py ===

from pyspark.sql import SparkSession

class BaseETLJob:
    """
    BaseETLJob is an abstract base class that provides reusable ETL functionality 
    such as Spark session initialization, schema creation, reading CSVs, 
    and writing data in Delta format.
    """

    def __init__(self, app_name: str):
        """
        Initialize the Spark session with a given application name.

        :param app_name: Name for the Spark application.
        """
        self.spark = SparkSession.builder.appName(app_name).getOrCreate()

    def create_schema_if_not_exists(self, catalog: str, schema: str):
        """
        Create a schema in the specified catalog if it doesn't already exist.

        :param catalog: The catalog (e.g., 'main').
        :param schema: The schema name (e.g., 'etl').
        """
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

    def read_csv(self, url: str):
        """
        Read a CSV file from the specified URL into a Spark DataFrame.

        :param url: The URL of the CSV file.
        :return: Spark DataFrame containing the CSV data.
        """
        return self.spark.read.option("header", True).csv(url)

    def write_delta(self, df, table: str, mode: str = "overwrite"):
        """
        Write a DataFrame to a Delta Lake table.

        :param df: The DataFrame to write.
        :param table: Full table name (e.g., 'main.etl.my_table').
        :param mode: Write mode (default is 'overwrite').
        """
        df.write.format("delta").mode(mode).saveAsTable(table)

    def stop(self):
        """
        Stop the Spark session.
        """
        self.spark.stop()
