import unittest
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import SparkSession
from src.main import load_data, filter_data, rename_columns  # adjust import path accordingly

class MainTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .master("local[1]") \
            .appName('KommatiParaTest') \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_load_data(self):
        path = "path_to_test_data/test_clients.csv"
        df = load_data(self.spark, path)
        # Verify the schema, number of rows, or other properties of df

    def test_filter_data(self):
        # Load test data
        path = "path_to_test_data/test_clients.csv"
        df = load_data(self.spark, path)
        # Filter data
        filtered_df = filter_data(df, ["United Kingdom", "Netherlands"])
        # Verify the result, e.g., number of rows, values in the 'country' column, etc.

    def test_rename_columns(self):
        # Load test data
        path = "path_to_test_data/test_financial.csv"
        df = load_data(self.spark, path)
        # Rename columns
        renamed_df = rename_columns(df)
        # Verify the result, e.g., column names

if __name__ == '__main__':
    unittest.main()