# === scripts/etl_vix.py ===

from base_etl import BaseETLJob
from pyspark.sql.functions import to_date, col, sum


class VIXETL(BaseETLJob):
    """
    VIXETL handles the ETL process for daily VIX volatility data.
    This class inherits utility methods from BaseETLJob and performs:
    - Data extraction
    - Transformation and enrichment
    - Saving both raw and aggregated results
    """

    def __init__(self):
        """
        Initialize the ETL job with a specific Spark application name.
        """
        super().__init__("VIXETL")

    def extract_and_transform(self):
        """
        Extract the VIX dataset and perform basic transformation:
        - Casts 'Date' to 'date'
        - Casts 'VIX Close' to double
        - Drops unnecessary columns
        - Adds 'Month' column

        Returns:
            DataFrame: Cleaned and structured raw DataFrame
        """
        url = "https://raw.githubusercontent.com/plotly/datasets/master/volatility-daily.csv"
        df = self.read_csv(url)

        df_raw = df.withColumn("date", col("Date").cast("date")) \
                   .withColumn("vix_close", col("VIX Close").cast("double")) \
                   .drop("Date", "VIX Open")

        df_raw = df_raw.withColumn("Month", to_date(col("date"), "yyyy-MM-dd"))

        return df_raw

    def enrich(self, df):
        """
        Perform enrichment by aggregating monthly VIX values.

        Args:
            df (DataFrame): Raw cleaned DataFrame

        Returns:
            DataFrame: Aggregated DataFrame by month
        """
        df_agg = df.groupBy("Month").agg(
            *[sum(col(c)).alias(f"total_{c.lower().replace(' ', '_')}") for c in df.columns if c not in ["Month", "date"]]
        )
        return df_agg

    def run(self):
        """
        Execute the complete ETL pipeline:
        - Extract and transform raw data
        - Save raw data
        - Enrich and save aggregated data
        """
        try:
            df_raw = self.extract_and_transform()
            df_agg = self.enrich(df_raw)

            self.create_schema_if_not_exists("main", "etl")
            self.write_delta(df_raw, "main.etl.vix_raw_daily")
            self.write_delta(df_agg, "main.etl.vix_monthly_agg")

            print("✅ VIX ETL complete.")
        except Exception as e:
            raise RuntimeError(f"❌ VIX ETL job failed: {str(e)}")
        finally:
            self.stop()


if __name__ == "__main__":
    job = VIXETL()
    job.run()
