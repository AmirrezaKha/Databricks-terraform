# === scripts/etl_vix.py ===

import requests
from base_etl import BaseETLJob
from pyspark.sql.functions import col, sum, trunc


class VIXETL(BaseETLJob):
    """
    VIXETL handles the ETL process for daily VIX volatility data.
    This class inherits utility methods from BaseETLJob and performs:
    - Data extraction
    - Transformation and enrichment
    - Saving both raw and aggregated results
    """

    def __init__(self, spark=None):
        super().__init__("VIXETL", spark=spark)

    def download_csv_to_dbfs(self, url: str, dbfs_path: str):
        """
        Downloads the CSV file and saves it to DBFS.

        Args:
            url (str): URL of the CSV file
            dbfs_path (str): DBFS path to save the file (e.g. "/tmp/vix.csv")
        """
        response = requests.get(url)
        response.raise_for_status()

        with open(f"/dbfs{dbfs_path}", "wb") as f:
            f.write(response.content)

    def extract_and_transform(self):
        """
        Extract and transform the VIX dataset:
        - Casts 'Date' to date
        - Casts 'VIX Close' to double
        - Drops unneeded columns
        - Adds a 'Month' column by truncating date

        Returns:
            DataFrame: Cleaned DataFrame
        """
        url = "https://raw.githubusercontent.com/plotly/datasets/master/volatility-daily.csv"
        dbfs_path = "/tmp/vix.csv"
        self.download_csv_to_dbfs(url, dbfs_path)

        df = self.spark.read.format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load(dbfs_path)

        df_raw = df.withColumn("date", col("Date").cast("date")) \
                   .withColumn("vix_close", col("VIX Close").cast("double")) \
                   .drop("Date", "VIX Open")

        # Truncate date to first of month
        df_raw = df_raw.withColumn("Month", trunc(col("date"), "Month"))

        return df_raw

    def enrich(self, df):
        """
        Enrich data by aggregating monthly VIX values.

        Args:
            df (DataFrame): Cleaned daily data

        Returns:
            DataFrame: Aggregated by month
        """
        df_agg = df.groupBy("Month").agg(
            sum("vix_close").alias("total_vix_close")
        )
        return df_agg

    def run(self):
        """
        Run the complete ETL process for VIX data.
        Save raw and aggregated Delta tables, display them, 
        and print their storage locations.
        """
        try:
            df_raw = self.extract_and_transform()
            df_agg = self.enrich(df_raw)

            # Ensure the schema/database exists
            self.create_schema_if_not_exists(schema="etl")

            # Save raw and aggregated data as Delta tables
            df_raw.write.format("delta").mode("overwrite").saveAsTable("etl.vix_raw_daily")
            print("✅ Saved etl.vix_raw_daily")

            df_agg.write.format("delta").mode("overwrite").saveAsTable("etl.vix_monthly_agg")
            print("✅ Saved etl.vix_monthly_agg")

            # Display the saved tables (Databricks display function)
            print("\nDisplaying etl.vix_raw_daily:")
            display(self.spark.table("etl.vix_raw_daily"))

            print("\nDisplaying etl.vix_monthly_agg:")
            display(self.spark.table("etl.vix_monthly_agg"))

            # Retrieve and print the Delta table storage locations
            raw_loc = self.spark.sql("DESCRIBE DETAIL etl.vix_raw_daily").collect()[0]["location"]
            agg_loc = self.spark.sql("DESCRIBE DETAIL etl.vix_monthly_agg").collect()[0]["location"]

            print(f"\nRaw table location: {raw_loc}")
            print(f"Aggregated table location: {agg_loc}")

            print("✅ VIX ETL complete.")
        except Exception as e:
            raise RuntimeError(f"❌ VIX ETL job failed: {str(e)}")
        finally:
            self.stop()

