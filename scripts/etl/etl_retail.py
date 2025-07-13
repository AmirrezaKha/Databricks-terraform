# === scripts/etl_retail.py ===

from base_etl import BaseETLJob
from pyspark.sql.functions import to_date, col, sum


class RetailETL(BaseETLJob):
    """
    RetailETL is a subclass of BaseETLJob that handles data ingestion,
    transformation, and storage for monthly retail sales data.
    """

    def __init__(self):
        """
        Initialize the RetailETL job with the application name 'RetailETL'.
        """
        super().__init__("RetailETL")

    def extract_and_transform(self):
        """
        Ingest retail sales data and perform basic transformation:
        - Parse date
        - Cast numeric columns
        Returns transformed raw DataFrame.
        """
        url = "https://datahub.io/core/retail-sales/r/monthly-retail-trade.csv"
        df = self.read_csv(url)

        # Convert Month column to date format
        df = df.withColumn("Month", to_date(col("Month"), "yyyy-MM"))

        # Cast all columns except 'Month' to double
        for column in df.columns:
            if column != "Month":
                df = df.withColumn(column, col(column).cast("double"))

        return df

    def enrich(self, df):
        """
        Perform enrichment by aggregating monthly totals.
        Returns aggregated DataFrame.
        """
        aggregated_df = df.groupBy("Month").agg(
            *[sum(col(c)).alias(f"total_{c.lower().replace(' ', '_')}") for c in df.columns if c != "Month"]
        )
        return aggregated_df

    def run(self):
        """
        Run the complete ETL pipeline:
        - Extract and transform raw data
        - Save raw data to Delta table
        - Enrich data and save to a separate Delta table
        """
        try:
            df_raw = self.extract_and_transform()
            df_enriched = self.enrich(df_raw)

            self.create_schema_if_not_exists("main", "etl")
            self.write_delta(df_raw, "main.etl.retail_monthly_raw")
            self.write_delta(df_enriched, "main.etl.retail_monthly_sales")

            print("✅ Retail ETL complete.")
        except Exception as e:
            raise RuntimeError(f"❌ Retail ETL job failed: {str(e)}")
        finally:
            self.stop()


if __name__ == "__main__":
    job = RetailETL()
    job.run()
