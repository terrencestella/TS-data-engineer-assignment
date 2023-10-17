from pyspark.sql import SparkSession

def init_spark():
    spark = SparkSession.builder.appName("TS_exercise").getOrCreate()
    return spark

def load_data(spark, path):
    df = spark.read.csv(path, header=True, inferSchema=True)
    return df

# this function takes two arguments, 1) the dataframe which needs to be filtered
# 2) a list with values on which needs to be filtered 
def filter_country(df,filter_countries):
    # in this step, a filter on the attribute "country" is applied
    # for each country that is passed on in the list, that country will
    # be in the final version of the dataframe
    df = df.filter(df['country'].isin(filter_countries))
    # the function returns the filtered dataframe
    return df

# this function takes two arguments, 1) the dataframe which needs to be modified
# 2) a single column name which needs to be dropped from the dataframe
def drop_column(df,column):
    # in this step, a column is dropped from the dataframe
    df = df.drop(column)
    # the function returns the modified dataframe
    return df

def join_dfs(df_a,df_b,on_column,join_type):
    df = df_a.join(df_b,[on_column], join_type)
    return df

def rename_column(df,rename_dict):
    for column in rename_dict:
        df = df.withColumnRenamed(column,rename_dict[column])
    return df
