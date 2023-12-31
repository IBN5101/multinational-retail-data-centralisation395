from botocore.exceptions import NoCredentialsError, ClientError
from database_utils import DatabaseConnector
import boto3
import json
import pandas as pd
import requests
import tabula


class DataExtraction:
    @staticmethod
    def read_rds_table(db_connector: DatabaseConnector, table_name: str):
        return pd.read_sql_table(table_name, db_connector.get_engine())

    @staticmethod
    def retrieve_pdf_data(pdf_path):
        df_tables = tabula.read_pdf(
            pdf_path,
            pages="all"
        )
        return pd.concat(df_tables, ignore_index=True)

    @staticmethod
    def list_number_of_stores(api_endpoint):
        with open("credentials/api_key.json", "r") as f:
            headers = json.load(f)
        response = requests.get(api_endpoint, headers=headers)

        number_of_stores = response.json()["number_stores"]
        print(f"Number of stores: {number_of_stores}")
        return number_of_stores

    @staticmethod
    def retrieve_stores_data(api_endpoint, number_of_stores):
        with open("credentials/api_key.json", "r") as f:
            headers = json.load(f)
        store_list = []
        for store_num in range(number_of_stores):
            response = requests.get(
                f"{api_endpoint}/{store_num}",
                headers=headers
            )
            store_list.append(response.json())
        df_stores = pd.DataFrame.from_dict(store_list)
        df_stores.set_index("index", inplace=True)

        return df_stores

    @staticmethod
    def extract_from_s3(data_path):
        try:
            s3 = boto3.client('s3')
            s3.download_file(
                data_path,
                'products.csv',
                'outputs/products.csv',
            )

        except NoCredentialsError:
            print("AWS credentials not found. Please configure your credentials.")

        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchBucket':
                print("The specified bucket does not exist.")
            else:
                print("An error occurred:", e)

        df_products = pd.read_csv("outputs/products.csv")
        df_products.set_index("Unnamed: 0", inplace=True)
        df_products.reset_index(drop=True, inplace=True)

        return df_products

    @staticmethod
    def extract_from_json(data_path):
        df_datetime = pd.read_json(data_path)
        return df_datetime


if __name__ == "__main__":
    # ['legacy_store_details', 'legacy_users', 'orders_table']
    # df1 = DataExtraction().read_rds_table(
    #     DatabaseConnector("credentials/db_creds.yaml"),
    #     "legacy_users"
    # )
    # print(df1.info())

    temp = DataExtraction.extract_from_json(
        "https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json"
    )
    print(temp.head(3))
    print(temp.info())

    print("data_extraction.py completed.")
