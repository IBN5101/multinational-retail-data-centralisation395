import pandas as pd


class DataCleaning:
    @staticmethod
    def clean_user_data(users: pd.DataFrame):
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

    @staticmethod
    def clean_card_data(cards: pd.DataFrame):
        cards["expiry_date"] = pd.to_datetime(
            cards["expiry_date"],
            format="%m/%y",
            exact=True,
            errors="coerce",
        )
        cards["date_payment_confirmed"] = pd.to_datetime(
            cards["date_payment_confirmed"],
            errors="coerce",
        )
        cards.dropna(inplace=True)

        return cards

    @staticmethod
    def clean_store_data(stores: pd.DataFrame):
        stores["longitude"] = pd.to_numeric(
            stores["longitude"],
            errors="coerce"
        )
        stores["staff_numbers"] = pd.to_numeric(
            stores["staff_numbers"],
            errors="coerce"
        )
        stores["opening_date"] = pd.to_datetime(
            stores["opening_date"],
            format="mixed",
            errors="coerce"
        )
        stores["latitude"] = pd.to_numeric(
            stores["latitude"],
            errors="coerce"
        )
        stores["continent"] = stores["continent"].replace("eeEurope", "Europe")
        stores["continent"] = stores["continent"].replace("eeAmerica", "America")
        stores.drop("lat", axis=1, inplace=True)
        stores.dropna(subset=["staff_numbers"], inplace=True)

        return stores
