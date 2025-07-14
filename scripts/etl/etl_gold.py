# === scripts/etl_gold.py ===

import requests
from base_etl import BaseETLJob
from pyspark.sql.functions import col, to_date, month, year, avg, max, min


class GoldETL(BaseETLJob):
    """
    ETL job for loading and transforming monthly gold price data.
    Downloads CSV into DBFS, transforms, and writes to Delta tables.
    """

    def __init__(self, spark=None):
        super().__init__("GoldETL", spark=spark)

    def download_csv_to_dbfs(self, url: str, dbfs_path: str):
        """
        Downloads the CSV file and saves it to DBFS.

        Args:
            url (str): URL of the CSV file
            dbfs_path (str): DBFS path to save the file (e.g. "/tmp/gold.csv")
        """
        response = requests.get(url)
        response.raise_for_status()

        with open(f"/dbfs{dbfs_path}", "wb") as f:
            f.write(response.content)

    def extract_and_transform(self):
        """
        Extract and transform the gold price CSV data.

        Returns:
            DataFrame: Transformed DataFrame
        """
        url = "https://datahub.io/core/gold-prices/r/monthly.csv"
        dbfs_path = "/tmp/gold_prices.csv"
        self.download_csv_to_dbfs(url, dbfs_path)

        df = self.spark.read.format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load(dbfs_path)

        df = df.withColumn("Date", to_date(col("Date"))) \
               .withColumn("Price", col("Price").cast("double"))

        return df

    def enrich(self, df):
        """
        Enrich the data by aggregating average, max, and min prices.

        Args:
            df (DataFrame): Raw DataFrame

        Returns:
            DataFrame: Aggregated DataFrame
        """
        return df.withColumn("Year", year(col("Date"))) \
                 .withColumn("Month", month(col("Date"))) \
                 .groupBy("Year", "Month") \
                 .agg(
                     avg("Price").alias("avg_price"),
                     max("Price").alias("max_price"),
                     min("Price").alias("min_price")
                 )

    def run(self):
        """
        Run the ETL pipeline and write Delta outputs.
        Show the saved tables and print their storage locations.
        """
        try:
            df_raw = self.extract_and_transform()
            df_enriched = self.enrich(df_raw)

            # Ensure schema/database exists
            self.create_schema_if_not_exists(schema="etl")

            # Save raw data as Delta table
            df_raw.write.format("delta").mode("overwrite").saveAsTable("etl.gold_prices_raw")
            print("✅ Saved etl.gold_prices_raw")

            # Save enriched data as Delta table
            df_enriched.write.format("delta").mode("overwrite").saveAsTable("etl.gold_prices_monthly_agg")
            print("✅ Saved etl.gold_prices_monthly_agg")

            # Show the saved tables (Databricks display)
            print("\nDisplaying etl.gold_prices_raw:")
            display(self.spark.table("etl.gold_prices_raw"))

            print("\nDisplaying etl.gold_prices_monthly_agg:")
            display(self.spark.table("etl.gold_prices_monthly_agg"))

            # Get and print Delta table locations
            raw_loc = self.spark.sql("DESCRIBE DETAIL etl.gold_prices_raw").collect()[0]["location"]
            agg_loc = self.spark.sql("DESCRIBE DETAIL etl.gold_prices_monthly_agg").collect()[0]["location"]

            print(f"\nRaw table location: {raw_loc}")
            print(f"Aggregated table location: {agg_loc}")

            print("✅ Gold ETL complete.")
        except Exception as e:
            raise RuntimeError(f"❌ Gold ETL job failed: {str(e)}")
        finally:
            self.stop()
