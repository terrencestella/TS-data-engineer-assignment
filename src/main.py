import argparse
from pyspark.sql import SparkSession
import os

#to get the current working directory
dir = os.getcwd()
# spark = SparkSession.builder.appName('pipeline').getOrCreate()
parser = argparse.ArgumentParser(description='Application should receive three arguments, \
the paths to each of the dataset files and also the countries to filter as \
the client wants to reuse the code for other countries.')

parser.add_argument('clients_file',type=str,help='The file name (including extension) containing the client data')           # positional argument
parser.add_argument('financials_file',type=str,help='The file name (including extension) containing the client financial data')        # option that takes a value
parser.add_argument('country_filters',type=str,help='The country or countries to be applied as filter on the dataset') 
args = parser.parse_args()

# clients = spark.read.csv(fr'{dir}\{args.clients_file}',header=True)
# financials = spark.read.csv(fr'{dir}\{args.financials_file}',header=True)
filter_countries = str(args.country_filters).split(',')

print(filter_countries)
