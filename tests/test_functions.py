import unittest
from pyspark.sql import SparkSession
import chispa

from src.functions import (
    init_spark,
    load_data,
    filter_country,
    drop_column,
    join_dfs,
    rename_column
)

class TestFunctions(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = init_spark()

    def test_load_data(self):
        path = "tests/test.csv" 
        df = load_data(self.spark, path)
        self.assertIsNotNone(df)

    def test_filter_country(self):
        # Assuming you have a DataFrame with a country column
        data = [("USA",), ("Canada",), ("UK",)]
        df = self.spark.createDataFrame(data, ["country"])
        filtered_df = filter_country(df, ["UK", "Canada"])
        self.assertEqual(filtered_df.count(), 2)

    def test_drop_column(self):
        data = [("Alice", 1), ("Bob", 2)]
        df = self.spark.createDataFrame(data, ["name", "age"])
        df = drop_column(df, "age")
        self.assertFalse("age" in df.columns)

    def test_join_dfs(self):
        data_a = [("Alice", 1), ("Bob", 2)]
        data_b = [(1, "USA"), (2, "Canada")]
        df_a = self.spark.createDataFrame(data_a, ["name", "id"])
        df_b = self.spark.createDataFrame(data_b, ["id", "country"])
        joined_df = join_dfs(df_a, df_b, "id", "inner")
        self.assertEqual(joined_df.count(), 2)

    def test_rename_column(self):
        data = [("Alice", 1), ("Bob", 2)]
        df = self.spark.createDataFrame(data, ["name", "age"])
        df = rename_column(df, {"name": "full_name"})
        self.assertTrue("full_name" in df.columns)

if __name__ == '__main__':
    unittest.main()