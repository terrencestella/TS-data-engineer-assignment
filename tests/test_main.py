import unittest
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import SparkSession
import src.terstel.functions as fn

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
        df = fn.load_data(self.spark, path)
        # Verify the schema, number of rows, or other properties of df

    def test_filter_data(self):
        # Load test data
        path = "path_to_test_data/test_clients.csv"
        df = fn.load_data(self.spark, path)
        # Filter data
        filtered_df = fn.filter_data(df, ["United Kingdom", "Netherlands"])
        # Verify the result, e.g., number of rows, values in the 'country' column, etc.

    def test_rename_columns(self):
        # Load test data
        path = "path_to_test_data/test_financial.csv"
        df = fn.load_data(self.spark, path)
        # Rename columns
        renamed_df = fn.rename_columns(df)
        # Verify the result, e.g., column names

if __name__ == '__main__':
    unittest.main()