# === anomaly/anomaly_retail.py ===

from anomaly.base_anomaly import BaseAnomalyDetector

class RetailAnomalyDetector(BaseAnomalyDetector):
    def __init__(self, spark, method="isolation_forest"):
        super().__init__(spark, "main.etl.retail_monthly_sales", method)


if __name__ == "__main__":
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    RetailAnomalyDetector(spark).run()
