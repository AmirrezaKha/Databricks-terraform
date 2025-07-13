from sklearn.ensemble import IsolationForest
from sklearn.neighbors import LocalOutlierFactor
from sklearn.svm import OneClassSVM
import matplotlib.pyplot as plt

class BaseAnomalyDetector:
    """
    Base class for detecting anomalies using scikit-learn models on data from a Delta Lake table.

    Supports Isolation Forest, Local Outlier Factor (LOF), and One-Class SVM.
    """
    def __init__(self, spark, delta_table, method='isolation_forest'):
        """
        Initialize the anomaly detector.

        Args:
            spark (SparkSession): Active Spark session.
            delta_table (str): Delta table name in catalog format (e.g., 'main.etl.table_name').
            method (str): Anomaly detection method - 'isolation_forest', 'lof', or 'ocsvm'.
        """
        self.spark = spark
        self.delta_table = delta_table
        self.method = method
        self.df = None
        self.pandas_df = None
        self.predictions = None
        self.model = None

    def load_data(self):
        """
        Load the Delta table into a Spark DataFrame.
        """
        if not self.spark.catalog.tableExists(self.delta_table):
            raise ValueError(f"Delta table {self.delta_table} does not exist.")
        self.df = self.spark.read.format("delta").table(self.delta_table)

    def preprocess(self):
        """
        Convert the Spark DataFrame to a Pandas DataFrame and select only numeric columns.
        """
        self.pandas_df = self.df.toPandas()
        self.pandas_df = self.pandas_df.select_dtypes(include=['number'])
        if self.pandas_df.empty:
            raise ValueError("No numeric features found for anomaly detection.")

    def init_model(self):
        """
        Initialize the anomaly detection model based on the specified method.
        """
        if self.method == 'isolation_forest':
            self.model = IsolationForest(random_state=42, contamination=0.05)
        elif self.method == 'lof':
            self.model = LocalOutlierFactor(n_neighbors=20, contamination=0.05, novelty=True)
        elif self.method == 'ocsvm':
            self.model = OneClassSVM(nu=0.05, kernel="rbf", gamma='scale')
        else:
            raise ValueError(f"Unknown anomaly detection method: {self.method}")

    def fit(self):
        """
        Fit the model on the preprocessed data.

        Note: For LOF with novelty=True, you must explicitly call `fit()` before `predict()`.
        """
        self.model.fit(self.pandas_df)

    def predict(self):
        """
        Predict anomalies on the dataset.

        Updates the Pandas DataFrame with an 'anomaly' column (1 = anomaly, 0 = normal).
        """
        self.predictions = self.model.predict(self.pandas_df)
        self.pandas_df['anomaly'] = self.predictions
        self.pandas_df['anomaly'] = self.pandas_df['anomaly'].map({1: 0, -1: 1})

    def report(self):
        """
        Print the number of anomalies and return them.

        Returns:
            DataFrame: Pandas DataFrame containing only the rows labeled as anomalies.
        """
        anomalies = self.pandas_df[self.pandas_df['anomaly'] == 1]
        print(f"🔍 Detected {len(anomalies)} anomalies out of {len(self.pandas_df)} records.")
        return anomalies

    def plot_anomalies(self, feature_x=None, feature_y=None):
        """
        Display a 2D scatter plot with anomalies in red.

        Args:
            feature_x (str): Column name for the x-axis.
            feature_y (str): Column name for the y-axis.
        """
        df = self.pandas_df
        if feature_x is None or feature_y is None:
            numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
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
        """
        Run the full anomaly detection pipeline:
        - Load data from Delta
        - Preprocess to numeric features
        - Initialize and fit the model
        - Predict and label anomalies
        - Report and plot results

        Returns:
            DataFrame: Anomalous rows from the dataset.
        """
        self.load_data()
        self.preprocess()
        self.init_model()
        self.fit()
        self.predict()
        anomalies = self.report()
        self.plot_anomalies()
        return anomalies
