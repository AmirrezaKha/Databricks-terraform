# === scripts/etl_nasdaq.py ===

from base_etl import BaseETLJob
from pyspark.sql.functions import col, when, lit, year, current_date
from pyspark.sql.types import IntegerType

class NasdaqETL(BaseETLJob):
    """
    ETL job for ingesting and preparing NASDAQ-listed company data.

    This job extracts company listings from a CSV URL,
    performs transformation and enrichment,
    and stores both raw and enriched data in Delta tables.
    """

    def __init__(self):
        """Initialize the ETL job with SparkSession."""
        super().__init__("NasdaqETL")

    def transform(self, df):
        """
        Apply transformations such as casting numeric fields.

        Args:
            df (DataFrame): Raw input DataFrame.

        Returns:
            DataFrame: Cleaned and casted DataFrame.
        """
        # Cast relevant columns to double or integer
        if "Market Capitalization" in df.columns:
            df = df.withColumn("market_cap", col("Market Capitalization").cast("double"))
        if "IPO Year" in df.columns:
            df = df.withColumn("ipo_year", col("IPO Year").cast(IntegerType()))
        return df

    def enrich(self, df):
        """
        Enrich NASDAQ dataset with additional business logic features:
        - market_cap_billions: market cap scaled down to billions
        - market_cap_category: small, mid, or large cap based on market cap thresholds
        - is_recent_ipo: boolean flag if IPO Year within last 5 years

        Args:
            df (DataFrame): Transformed raw DataFrame.

        Returns:
            DataFrame: Enriched DataFrame with additional columns.
        """
        # Add market cap in billions
        df = df.withColumn("market_cap_billions", col("market_cap") / 1e9)

        # Market cap categories (you can tune thresholds)
        df = df.withColumn(
            "market_cap_category",
            when(col("market_cap_billions") >= 10, lit("Large Cap"))
            .when(col("market_cap_billions") >= 2, lit("Mid Cap"))
            .otherwise(lit("Small Cap"))
        )

        # Calculate current year
        current_year = int(self.spark.sql("SELECT year(current_date())").collect()[0][0])

        # Flag for recent IPO within last 5 years
        df = df.withColumn(
            "is_recent_ipo",
            when((col("ipo_year") >= current_year - 5) & (col("ipo_year") <= current_year), lit(True))
            .otherwise(lit(False))
        )

        return df

    def run(self):
        """
        Perform the full ETL job:
        - Read NASDAQ listings
        - Clean and cast fields
        - Enrich dataset with business logic columns
        - Write raw and enriched DataFrames to Delta tables
        """
        try:
            url = "https://datahub.io/core/nasdaq-listings/r/nasdaq-listed-symbols.csv"
            df_raw = self.read_csv(url)
            df_transformed = self.transform(df_raw)
            df_enriched = self.enrich(df_transformed)

            self.create_schema_if_not_exists("main", "etl")
            self.write_delta(df_transformed, "main.etl.nasdaq_listings_raw")
            self.write_delta(df_enriched, "main.etl.nasdaq_listings_enriched")

            print("✅ NASDAQ ETL complete.")
        except Exception as e:
            raise RuntimeError(f"❌ NASDAQ ETL job failed: {str(e)}")
        finally:
            self.stop()


if __name__ == "__main__":
    NasdaqETL().run()
