# === anomaly/anomaly_vix.py ===

from anomaly.base_anomaly import BaseAnomalyDetector

class VIXAnomalyDetector(BaseAnomalyDetector):
    def __init__(self, spark, method="isolation_forest"):
        super().__init__(spark, "main.etl.vix_index", method)

    def plot_anomalies(self):
        import matplotlib.pyplot as plt
        df = self.pandas_df
        if "Date" in df.columns and "VIXClose" in df.columns:
            plt.figure(figsize=(12, 6))
            plt.plot(df["Date"], df["VIXClose"], label="VIX", color="blue")
            anomalies = df[df["anomaly"] == 1]
            plt.scatter(anomalies["Date"], anomalies["VIXClose"], color="red", label="Anomaly")
            plt.title("VIX Anomaly Detection")
            plt.xlabel("Date")
            plt.ylabel("VIX Close")
            plt.legend()
            plt.grid(True)
            plt.show()
        else:
            super().plot_anomalies()


if __name__ == "__main__":
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    VIXAnomalyDetector(spark).run()
