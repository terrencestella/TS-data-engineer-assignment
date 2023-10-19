from pyspark.sql import SparkSession
from logger_config import logger

def init_spark():
    """
    Initialize a SparkSession with the application name "TS_exercise".

    :return: The active SparkSession.
    :rtype: pyspark.sql.SparkSession
    """
    try:
        spark = SparkSession.builder.appName("TS_exercise").getOrCreate()
        logger.info('Spark session started')
        return spark
    except Exception as e:
        logger.exception(e)
        

def load_data(spark, path):
    """
    Load a CSV file from the specified path into a DataFrame.

    :param spark: The SparkSession object.
    :type spark: pyspark.sql.SparkSession
    :param path: The path to the CSV file.
    :type path: str
    :return: The DataFrame loaded from the CSV file.
    :rtype: pyspark.sql.DataFrame
    """
    try:
        df = spark.read.csv(path, header=True, inferSchema=True)
        logger.info(f'successfully read CSV file stored in location: {path}')
        return df
    except Exception as e:
        logger.exception(e)

def filter_country(df,filter_countries):
    """
    Filter the DataFrame based on the specified countries.

    :param df: The input DataFrame.
    :type df: pyspark.sql.DataFrame
    :param filter_countries: A list of countries to filter the DataFrame on.
    :type filter_countries: list
    :return: The filtered DataFrame containing only the specified countries.
    :rtype: pyspark.sql.DataFrame
    """
    try: 
        df = df.filter(df['country'].isin(filter_countries))
        logger.info('filtered on following countries: {}'.format(str(filter_countries)))
        return df
    except Exception as e:
        logger.exception(e)    

def drop_column(df,column):
    """
    Drop the specified column from the DataFrame.

    :param df: The input DataFrame.
    :type df: pyspark.sql.DataFrame
    :param column: The name of the column to be dropped.
    :type column: str
    :return: The DataFrame with the specified column dropped.
    :rtype: pyspark.sql.DataFrame
    """
    try:    
        df = df.drop(column)
        logger.info('dropped column {}'.format(column))
        return df
    except Exception as e:
        logger.exception(e)   

def join_dfs(df_a,df_b,on_column,join_type):
    """
    Join two DataFrames on a specified column using a specified join type.

    :param df_a: The first DataFrame.
    :type df_a: pyspark.sql.DataFrame
    :param df_b: The second DataFrame.
    :type df_b: pyspark.sql.DataFrame
    :param on_column: The column on which to join the DataFrames.
    :type on_column: str
    :param join_type: The type of join to perform (e.g., 'inner', 'outer', 'left', 'right').
    :type join_type: str
    :return: The resulting DataFrame after performing the join.
    :rtype: pyspark.sql.DataFrame
    """
    try:
        df = df_a.join(df_b,[on_column], join_type)
        logger.info(f'successfully joined DataFrames on {on_column} column')
        return df
    except Exception as e:
        logger.exception(e)

def rename_column(df,rename_dict):
    """
    Rename columns of the DataFrame based on the provided mapping dictionary.

    :param df: The input DataFrame.
    :type df: pyspark.sql.DataFrame
    :param rename_dict: A dictionary where the keys are the original column names
                        and the values are the new column names.
    :type rename_dict: dict
    :return: The DataFrame with renamed columns.
    :rtype: pyspark.sql.DataFrame
    """
    try:
        logger.info('starting up renaming column(s)')
        for column in rename_dict:
            df = df.withColumnRenamed(column,rename_dict[column])
            logger.info(f'renamed column {column} into {rename_dict[column]}')
        logger.info('done renaming column(s)')
        return df
    except Exception as e:
        logger.exception(e)