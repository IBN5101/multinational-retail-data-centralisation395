import pandas as pd

class DataCleaning:
    @staticmethod
    def clean_user_data(users: pd.DataFrame):
        # Datetime conversion
        users["date_of_birth"] = pd.to_datetime(
            users["date_of_birth"],
            format="mixed",
            errors="coerce"
        )
        users["join_date"] = pd.to_datetime(
            users["join_date"],
            format="mixed",
            errors="coerce"
        )
        users.dropna(inplace=True)

        return users

