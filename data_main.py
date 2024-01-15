from database_utils import DatabaseConnector
from data_cleaning import DataCleaning
from data_extraction import DataExtraction
import pandas as pd

if __name__ == "__main__":
    pd.set_option('display.max_columns', None)

    remote_db = DatabaseConnector("credentials/db_creds.yaml")
    local_db = DatabaseConnector("credentials/local.yaml")

    #   legacy_users
    # df_users = DataExtraction.read_rds_table(remote_db, "legacy_users")
    # df_users = DataCleaning.clean_user_data(df_users)
    # local_db.upload_to_db(df_users, "dim_users")

    #   card_details
    df_cards = DataExtraction.retrieve_pdf_data(
        "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
    )
    df_cards = DataCleaning.clean_card_data(df_cards)
    local_db.upload_to_db(df_cards, "dim_card_details")

    #   store_details
    # number_of_stores = DataExtraction.list_number_of_stores(
    #     "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
    # )
    # df_stores = DataExtraction.retrieve_stores_data(
    #     "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details",
    #     number_of_stores,
    # )
    # df_stores = DataCleaning.clean_store_data(df_stores)
    # local_db.upload_to_db(df_stores, "dim_store_details")

    #   products
    # df_products = DataExtraction.extract_from_s3("data-handling-public")
    # df_products = DataCleaning.convert_product_weights(df_products)
    # df_products = DataCleaning.clean_products_data(df_products)
    # local_db.upload_to_db(df_products, "dim_products")

    #   orders_table
    # df_orders = DataExtraction.read_rds_table(remote_db, "orders_table")
    # df_orders = DataCleaning.clean_orders_data(df_orders)
    # local_db.upload_to_db(df_orders, "orders_table")

    #   date_times
    # df_dt = DataExtraction.extract_from_json(
    #     "https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json"
    # )
    # df_dt = DataCleaning.clean_date_times_data(df_dt)
    # local_db.upload_to_db(df_dt, "dim_date_times")

    print("Pipeline completed.")
