from sklearn.ensemble import IsolationForest
from sklearn.neighbors import LocalOutlierFactor
from sklearn.svm import OneClassSVM
import matplotlib.pyplot as plt
import pandas as pd

class BaseAnomalyDetector:
    def __init__(self, spark, delta_table, method='isolation_forest', sample_size=1000):
        self.spark = spark
        self.delta_table = delta_table
        self.method = method.lower()
        self.sample_size = sample_size
        self.df = None
        self.pandas_df = None
        self.predictions = None
        self.model = None

    def load_data(self):
        if not self.spark.catalog.tableExists(self.delta_table):
            raise ValueError(f"❌ Delta table '{self.delta_table}' does not exist.")
        self.df = self.spark.read.format("delta").table(self.delta_table).limit(self.sample_size)

    def preprocess(self):
        self.pandas_df = self.df.toPandas()
        numeric_df = self.pandas_df.select_dtypes(include=['number'])
        if numeric_df.empty:
            raise ValueError("❌ No numeric features found for anomaly detection.")
        self.pandas_df = numeric_df.reset_index(drop=True)

    def init_model(self):
        if self.method == 'isolation_forest':
            self.model = IsolationForest(random_state=42, contamination=0.05)
        elif self.method == 'lof':
            # LOF requires novelty=True to use predict on new data
            self.model = LocalOutlierFactor(n_neighbors=20, contamination=0.05, novelty=True)
        elif self.method == 'ocsvm':
            self.model = OneClassSVM(nu=0.05, kernel="rbf", gamma='scale')
        else:
            raise ValueError(f"❌ Unknown anomaly detection method: {self.method}")

    def fit(self):
        if self.method == 'lof':
            # For LOF with novelty=True, fit first, then predict separately
            self.model.fit(self.pandas_df)
        else:
            self.model.fit(self.pandas_df)

    def predict(self):
        if self.method == 'lof':
            self.predictions = self.model.predict(self.pandas_df)
        else:
            self.predictions = self.model.predict(self.pandas_df)

        # Convert sklearn output to anomaly labels: 1 for anomaly, 0 for normal
        self.pandas_df['anomaly'] = pd.Series(self.predictions).map({1: 0, -1: 1})

    def report(self):
        anomalies = self.pandas_df[self.pandas_df['anomaly'] == 1]
        print(f"🔍 Detected {len(anomalies)} anomalies out of {len(self.pandas_df)} records using {self.method}.")
        return anomalies

    def plot_anomalies(self, feature_x=None, feature_y=None):
        df = self.pandas_df
        numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
        # Remove the anomaly column from numeric columns if present
        if 'anomaly' in numeric_cols:
            numeric_cols.remove('anomaly')

        if feature_x is None or feature_y is None:
            if len(numeric_cols) < 2:
                print("⚠️ Not enough numeric features for scatter plot.")
                return
            feature_x, feature_y = numeric_cols[:2]

        plt.figure(figsize=(10, 6))
        plt.scatter(df[df['anomaly'] == 0][feature_x], df[df['anomaly'] == 0][feature_y],
                    c='blue', label='Normal', alpha=0.6)
        plt.scatter(df[df['anomaly'] == 1][feature_x], df[df['anomaly'] == 1][feature_y],
                    c='red', label='Anomaly', alpha=0.8)
        plt.xlabel(feature_x)
        plt.ylabel(feature_y)
        plt.title(f"Anomaly Detection using {self.method}")
        plt.legend()
        plt.grid(True)
        plt.show()

    def run(self):
        self.load_data()
        self.preprocess()
        self.init_model()
        self.fit()
        self.predict()
        anomalies = self.report()
        self.plot_anomalies()
        return anomalies
