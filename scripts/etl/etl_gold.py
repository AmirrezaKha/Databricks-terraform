# === scripts/etl_gold.py ===

from base_etl import BaseETLJob
from pyspark.sql.functions import col, to_date, month, year, avg, max, min

class GoldETL(BaseETLJob):
    """
    ETL job for loading and transforming monthly gold price data.

    This class downloads the monthly gold price CSV file,
    converts data types appropriately, enriches the data,
    and writes both raw and aggregated data to Delta tables
    in the 'main.etl' schema on Databricks.
    """

    def __init__(self):
        """
        Initialize the ETL job with SparkSession.
        """
        super().__init__("GoldETL")

    def extract_and_transform(self):
        """
        Extract the raw gold price data and transform columns.

        Returns:
            DataFrame: Transformed raw DataFrame with proper data types.
        """
        url = "https://datahub.io/core/gold-prices/r/monthly.csv"
        df = self.read_csv(url)
        df = df.withColumn("Date", to_date(col("Date"))) \
               .withColumn("Price", col("Price").cast("double"))
        return df

    def enrich(self, df):
        """
        Enrich the data by aggregating monthly average, max, and min prices.

        Args:
            df (DataFrame): Raw DataFrame with gold price data.

        Returns:
            DataFrame: Aggregated DataFrame grouped by year and month.
        """
        df_enriched = df.withColumn("Year", year(col("Date"))) \
                        .withColumn("Month", month(col("Date"))) \
                        .groupBy("Year", "Month") \
                        .agg(
                            avg("Price").alias("avg_price"),
                            max("Price").alias("max_price"),
                            min("Price").alias("min_price")
                        )
        return df_enriched

    def run(self):
        """
        Run the ETL process:
        - Extract and transform raw data
        - Enrich with aggregates
        - Save raw and enriched data to Delta tables
        """
        try:
            df_raw = self.extract_and_transform()
            df_enriched = self.enrich(df_raw)

            self.create_schema_if_not_exists("main", "etl")
            self.write_delta(df_raw, "main.etl.gold_prices_raw")
            self.write_delta(df_enriched, "main.etl.gold_prices_monthly_agg")

            print("✅ Gold ETL complete.")
        except Exception as e:
            raise RuntimeError(f"❌ Gold ETL job failed: {str(e)}")
        finally:
            self.stop()


if __name__ == "__main__":
    GoldETL().run()
