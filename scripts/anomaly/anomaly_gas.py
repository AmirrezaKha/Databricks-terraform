# === anomaly/anomaly_gas.py ===

from anomaly.base_anomaly import BaseAnomalyDetector

class GasAnomalyDetector(BaseAnomalyDetector):
    def __init__(self, spark, method="lof"):
        super().__init__(spark, "main.etl.natural_gas_prices", method)


if __name__ == "__main__":
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    GasAnomalyDetector(spark).run()
