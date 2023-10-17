import argparse
from pyspark.sql import SparkSession
import os
import functions

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

renaming_columns = {
    'id':'client_identifier',
    'btc_a':'bitcoin_address',
    'cc_t':'credit_card_type'
    }

def main(client_path, financial_path, countries):
    spark = init_spark()
    client_df = load_data(spark, client_path)
    financial_df = load_data(spark, financial_path)
    filtered_client_df = filter_data(client_df, countries)
    renamed_financial_df = rename_columns(financial_df)
    joined_df = filtered_client_df.join(renamed_financial_df, "client_identifier")
    joined_df.write.csv("client_data/output.csv", header=True)

if __name__ == "__main__":
    main(args.clients_file,args.financials_file,str(args.country_filters).split(','))
