# === scripts/etl_gas.py ===

from base_etl import BaseETLJob
from pyspark.sql.functions import col, to_date, month, year, avg, max, min

class GasETL(BaseETLJob):
    """
    ETL job for loading and transforming daily natural gas prices.

    The job reads gas prices from a remote CSV file, parses date and price columns,
    enriches the data with monthly aggregates, and writes both raw and aggregated
    datasets into Delta Lake tables in Databricks.
    """

    def __init__(self):
        """Initialize the ETL job with SparkSession."""
        super().__init__("GasETL")

    def extract_and_transform(self):
        """
        Extract raw data and transform columns.

        Returns:
            DataFrame: Transformed raw DataFrame with correct data types.
        """
        url = "https://datahub.io/core/natural-gas/r/daily.csv"
        df = self.read_csv(url)
        df = df.withColumn("Date", to_date(col("Date"))) \
               .withColumn("Price", col("Price").cast("double"))
        return df

    def enrich(self, df):
        """
        Enrich raw natural gas data by calculating monthly average, max, and min prices.

        Args:
            df (DataFrame): Raw DataFrame with natural gas prices.

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
        Run the ETL pipeline:
        - Extract and transform raw data
        - Enrich with monthly aggregations
        - Save both raw and enriched datasets
        """
        try:
            df_raw = self.extract_and_transform()
            df_enriched = self.enrich(df_raw)

            self.create_schema_if_not_exists("main", "etl")
            self.write_delta(df_raw, "main.etl.natural_gas_prices_raw")
            self.write_delta(df_enriched, "main.etl.natural_gas_prices_monthly_agg")

            print("✅ Natural Gas ETL complete.")
        except Exception as e:
            raise RuntimeError(f"❌ Gas ETL job failed: {str(e)}")
        finally:
            self.stop()


if __name__ == "__main__":
    GasETL().run()
