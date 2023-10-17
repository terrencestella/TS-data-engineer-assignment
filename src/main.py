import argparse
import os
import functions as f

#to get the current working directory
dir = os.getcwd()
parser = argparse.ArgumentParser(description='Application should receive three arguments, \
the paths to each of the dataset files and also the countries to filter as \
the client wants to reuse the code for other countries.')
parser.add_argument('clients_file',type=str,help='The file name (including extension) containing the client data')
parser.add_argument('financials_file',type=str,help='The file name (including extension) containing the client financial data')
parser.add_argument('country_filters',type=str,help='The country or countries to be applied as filter on the dataset')
args = parser.parse_args()

renaming_columns = {
    'id':'client_identifier',
    'btc_a':'bitcoin_address',
    'cc_t':'credit_card_type'
    }

def main(client_file, financial_file, countries):
    spark = f.init_spark()
    client_df = f.load_data(spark, f'{dir}\data\{client_file}')
    client_df = f.drop_column(client_df,'email')
    financial_df = f.load_data(spark, f'{dir}\data\{financial_file}')
    financial_df = f.drop_column(financial_df,'cc_n')
    filtered_client_df = f.filter_data(client_df, countries)
    joined_df = f.join_dfs(filtered_client_df,financial_df,'id','inner')
    df = f.rename_columns(joined_df,renaming_columns)
    df.write.csv("client_data\output.csv", header=True)

if __name__ == "__main__":
    main(args.clients_file,args.financials_file,str(args.country_filters).split(','))