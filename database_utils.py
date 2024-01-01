from yaml import load, Loader
from sqlalchemy import create_engine, inspect, text
import pandas as pd


class DatabaseConnector:
    def __init__(self, file_location):
        db_creds = self.read_db_creds(file_location)
        self.engine = self.init_db_engine(db_creds)

    def read_db_creds(self, file_location):
        with open(file_location, "r") as file:
            data = load(file, Loader)
            return data

    def init_db_engine(self, database_creds):
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        HOST = database_creds["RDS_HOST"]
        USER = database_creds["RDS_USER"]
        PASSWORD = database_creds["RDS_PASSWORD"]
        DATABASE = database_creds["RDS_DATABASE"]
        PORT = database_creds["RDS_PORT"]

        engine = create_engine(
            f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}"
        )

        return engine

    def list_db_tables(self):
        inspector = inspect(self.engine)
        print(inspector.get_table_names())

    def upload_to_db(self, upload_data: pd.DataFrame, table_name: str):
        upload_data.to_sql(table_name, self.engine, if_exists="replace")

    def get_engine(self):
        return self.engine


if __name__ == "__main__":
    dc1 = DatabaseConnector("credentials/db_creds.yaml")
    dc1.list_db_tables()
