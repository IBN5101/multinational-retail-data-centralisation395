from database_utils import DatabaseConnector
from data_extraction import DataExtraction
from data_cleaning import DataCleaning

if __name__ == "__main__":
    remote_db = DatabaseConnector("credentials/db_creds.yaml")
    local_db = DatabaseConnector("credentials/local.yaml")

    # legacy_users
    df_users = DataExtraction.read_rds_table(remote_db, "legacy_users")
    df_users = DataCleaning.clean_user_data(df_users)
    local_db.upload_to_db(df_users, "dim_users")

    print("Pipeline completed.")