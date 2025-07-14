from pyspark.sql import SparkSession

class BaseETLJob:
    def __init__(self, job_name, spark=None):
        """
        Initialize the Spark session.

        :param app_name: Name for the Spark application.
        :param spark: Optional external SparkSession.
        """
        self.job_name = job_name
        if spark is None:
            from pyspark.sql import SparkSession
            self.spark = SparkSession.builder.getOrCreate()
        else:
            self.spark = spark

    def create_schema_if_not_exists(self, schema: str):
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")

    def write_delta(self, df, table_name: str):
        df.write.format("delta").mode("overwrite").saveAsTable(table_name)

    def stop(self):
        pass
