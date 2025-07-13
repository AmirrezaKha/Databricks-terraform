# === anomaly/anomaly_gold.py ===

from anomaly.base_anomaly import BaseAnomalyDetector

class GoldAnomalyDetector(BaseAnomalyDetector):
    def __init__(self, spark, method="ocsvm"):
        super().__init__(spark, "main.etl.gold_prices", method)


if __name__ == "__main__":
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    GoldAnomalyDetector(spark).run()
