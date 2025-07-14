import tempfile
import requests
from pyspark.sql.functions import col, to_date, month, year, avg, max, min


class GasETL(BaseETLJob):
    """
    ETL job for loading and transforming daily natural gas prices.
    Downloads CSV to a temporary file, then loads it with Spark.
    """

    def __init__(self, spark=None):
        super().__init__("GasETL", spark=spark)

    def download_csv_to_temp(self, url: str) -> str:
        """
        Downloads the CSV to a temporary file.

        Returns:
            str: Local path to the temporary CSV file.
        """
        response = requests.get(url)
        response.raise_for_status()

        tmp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
        tmp_file.write(response.content)
        tmp_file.close()
        return tmp_file.name

    def extract_and_transform(self):
        """
        Extract and transform the gas price CSV data.

        Returns:
            DataFrame: Transformed DataFrame with proper schema.
        """
        url = "https://datahub.io/core/natural-gas/r/daily.csv"
        local_path = self.download_csv_to_temp(url)

        df = self.spark.read.format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load(f"file://{local_path}")  # Local file URI

        df = df.withColumn("Date", to_date(col("Date"))) \
               .withColumn("Price", col("Price").cast("double"))

        return df

    def enrich(self, df):
        """
        Enrich the data with monthly aggregations.

        Args:
            df (DataFrame): Raw natural gas prices.

        Returns:
            DataFrame: Monthly aggregated DataFrame.
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
            df_raw.write.format("delta").mode("overwrite").saveAsTable("etl.natural_gas_prices_raw")
            print("✅ Saved etl.natural_gas_prices_raw")

            # Save enriched data as Delta table
            df_enriched.write.format("delta").mode("overwrite").saveAsTable("etl.natural_gas_prices_monthly_agg")
            print("✅ Saved etl.natural_gas_prices_monthly_agg")

            # Show the saved tables (display is Databricks built-in)
            print("\nDisplaying etl.natural_gas_prices_raw:")
            display(self.spark.table("etl.natural_gas_prices_raw"))

            print("\nDisplaying etl.natural_gas_prices_monthly_agg:")
            display(self.spark.table("etl.natural_gas_prices_monthly_agg"))

            # Get and print Delta table locations
            raw_loc = self.spark.sql("DESCRIBE DETAIL etl.natural_gas_prices_raw").collect()[0]["location"]
            agg_loc = self.spark.sql("DESCRIBE DETAIL etl.natural_gas_prices_monthly_agg").collect()[0]["location"]

            print(f"\nRaw table location: {raw_loc}")
            print(f"Aggregated table location: {agg_loc}")

            print("✅ Natural Gas ETL complete.")
        except Exception as e:
            raise RuntimeError(f"❌ Gas ETL job failed: {str(e)}")
        finally:
            self.stop()
