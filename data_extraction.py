from database_utils import DatabaseConnector
import pandas as pd
import tabula
import requests
import json


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


if __name__ == "__main__":
    # ['legacy_store_details', 'legacy_users', 'orders_table']
    # df1 = DataExtraction().read_rds_table(
    #     DatabaseConnector("credentials/db_creds.yaml"),
    #     "legacy_users"
    # )
    # print(df1.info())

    n_stores = DataExtraction.list_number_of_stores(
        "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
    )

    print("data_extraction.py completed.")
