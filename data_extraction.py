from database_utils import DatabaseConnector
import pandas as pd

class DataExtraction:
    @staticmethod
    def read_rds_table(db_connector: DatabaseConnector, table_name: str):
        return pd.read_sql_table(table_name, db_connector.get_engine())


if __name__ == "__main__":
    # ['legacy_store_details', 'legacy_users', 'orders_table']
    df1 = DataExtraction().read_rds_table(
        DatabaseConnector("credentials/db_creds.yaml"),
        "legacy_users"
    )
    print(df1.info())

