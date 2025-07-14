# === scripts/etl_nasdaq.py ===

import requests
from base_etl import BaseETLJob
from pyspark.sql.functions import col, when, lit
from pyspark.sql.types import IntegerType


class NasdaqETL(BaseETLJob):
    """
    ETL job for ingesting and preparing NASDAQ-listed company data.

    This job extracts company listings from a CSV URL,
    performs transformation and enrichment,
    and stores both raw and enriched data in Delta tables.
    """

    def __init__(self, spark=None):
        super().__init__("NasdaqETL", spark=spark)

    def download_csv_to_dbfs(self, url: str, dbfs_path: str):
        """
        Downloads the CSV file and saves it to DBFS.

        Args:
            url (str): URL of the CSV file
            dbfs_path (str): DBFS path to save the file (e.g. "/tmp/nasdaq.csv")
        """
        response = requests.get(url)
        response.raise_for_status()

        with open(f"/dbfs{dbfs_path}", "wb") as f:
            f.write(response.content)

    def extract(self):
        """
        Download and load the NASDAQ data.

        Returns:
            DataFrame: Raw DataFrame
        """
        url = "https://datahub.io/core/nasdaq-listings/r/nasdaq-listed-symbols.csv"
        dbfs_path = "/tmp/nasdaq_listings.csv"
        self.download_csv_to_dbfs(url, dbfs_path)

        df = self.spark.read.format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load(dbfs_path)

        return df

    def transform(self, df):
        """
        Apply transformations like casting numeric fields.

        Args:
            df (DataFrame): Raw input DataFrame.

        Returns:
            DataFrame: Cleaned and casted DataFrame.
        """
        if "Market Capitalization" in df.columns:
            df = df.withColumn("market_cap", col("Market Capitalization").cast("double"))
        if "IPO Year" in df.columns:
            df = df.withColumn("ipo_year", col("IPO Year").cast(IntegerType()))
        return df

    def enrich(self, df):
        """
        Enrich NASDAQ dataset:
        - market_cap_billions
        - market_cap_category
        - is_recent_ipo

        Args:
            df (DataFrame): Transformed DataFrame.

        Returns:
            DataFrame: Enriched DataFrame.
        """
        df = df.withColumn("market_cap_billions", col("market_cap") / 1e9)

        df = df.withColumn(
            "market_cap_category",
            when(col("market_cap_billions") >= 10, lit("Large Cap"))
            .when(col("market_cap_billions") >= 2, lit("Mid Cap"))
            .otherwise(lit("Small Cap"))
        )

        current_year = int(self.spark.sql("SELECT year(current_date())").collect()[0][0])

        df = df.withColumn(
            "is_recent_ipo",
            when((col("ipo_year") >= current_year - 5) & (col("ipo_year") <= current_year), lit(True))
            .otherwise(lit(False))
        )

        return df

    def run(self):
        """
        Run the full ETL pipeline for NASDAQ data.
        Save Delta tables, display results, and print table storage locations.
        """
        try:
            df_raw = self.extract()
            df_transformed = self.transform(df_raw)
            df_enriched = self.enrich(df_transformed)

            # Ensure schema/database exists
            self.create_schema_if_not_exists(schema="etl")

            # Save raw and enriched data as Delta tables
            df_transformed.write.format("delta").mode("overwrite").saveAsTable("etl.nasdaq_listings_raw")
            print("✅ Saved etl.nasdaq_listings_raw")

            df_enriched.write.format("delta").mode("overwrite").saveAsTable("etl.nasdaq_listings_enriched")
            print("✅ Saved etl.nasdaq_listings_enriched")

            # Display tables (Databricks display)
            print("\nDisplaying etl.nasdaq_listings_raw:")
            display(self.spark.table("etl.nasdaq_listings_raw"))

            print("\nDisplaying etl.nasdaq_listings_enriched:")
            display(self.spark.table("etl.nasdaq_listings_enriched"))

            # Print Delta table locations
            raw_loc = self.spark.sql("DESCRIBE DETAIL etl.nasdaq_listings_raw").collect()[0]["location"]
            enriched_loc = self.spark.sql("DESCRIBE DETAIL etl.nasdaq_listings_enriched").collect()[0]["location"]

            print(f"\nRaw table location: {raw_loc}")
            print(f"Enriched table location: {enriched_loc}")

            print("✅ NASDAQ ETL complete.")
        except Exception as e:
            raise RuntimeError(f"❌ NASDAQ ETL job failed: {str(e)}")
        finally:
            self.stop()

