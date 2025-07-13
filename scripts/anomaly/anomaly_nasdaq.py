# === anomaly/anomaly_nasdaq.py ===

from anomaly.base_anomaly import BaseAnomalyDetector

class NasdaqAnomalyDetector(BaseAnomalyDetector):
    def __init__(self, spark, method="isolation_forest"):
        super().__init__(spark, "main.etl.nasdaq_listings", method)


if __name__ == "__main__":
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    NasdaqAnomalyDetector(spark).run()
