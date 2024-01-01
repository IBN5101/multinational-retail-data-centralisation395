import pandas as pd


class DataCleaning:
    @staticmethod
    def clean_user_data(users: pd.DataFrame):
        users.set_index("index", inplace=True)
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

        stores["staff_numbers"] = pd.to_numeric(
            stores["staff_numbers"],
            downcast="integer",
            errors="coerce"
        )

        return stores

    @staticmethod
    def convert_product_weights(products: pd.DataFrame):
        products["weight"] = products["weight"].apply(DataCleaning.convert_one_weight)
        return products

    @staticmethod
    def clean_products_data(products: pd.DataFrame):
        products["date_added"] = pd.to_datetime(
            products["date_added"],
            format="mixed",
            errors="coerce",
        )
        products.dropna(inplace=True)

        products["product_price"] = products["product_price"].apply(
            lambda x: x.removeprefix("£")
        )
        products["product_price"] = pd.to_numeric(
            products["product_price"],
            errors="coerce",
        )
        products["EAN"] = pd.to_numeric(
            products["EAN"],
            downcast="integer",
            errors="coerce",
        )
        products.dropna(inplace=True)

        return products

    @staticmethod
    def convert_one_weight(weight_string: str):
        if not weight_string:
            return None
        if type(weight_string) is float:
            return weight_string

        weight_string = weight_string.strip().lower()

        ratio = 1
        if "kg" in weight_string[-2:]:
            weight_string = weight_string.removesuffix("kg")
        elif "g" in weight_string[-1:]:
            weight_string = weight_string.removesuffix("g")
            ratio = 0.001
        elif "ml" in weight_string[-2:]:
            weight_string = weight_string.removesuffix("ml")
            ratio = 0.001
        else:
            return None

        if "x" in weight_string:
            weights = weight_string.split("x")
            weights = [float(w.strip()) for w in weights]
            if len(weights) == 2:
                return weights[0] * weights[1] * ratio
            else:
                return None
        else:
            return float(weight_string) * ratio

    @staticmethod
    def clean_orders_data(orders: pd.DataFrame):
        orders.drop(["first_name", "last_name", "1", "level_0"], axis=1, inplace=True)
        orders.set_index("index", inplace=True)
        orders["product_quantity"] = pd.to_numeric(
            orders["product_quantity"],
            downcast="integer",
            errors="raise",
        )

        return orders


if __name__ == "__main__":
    print(DataCleaning.convert_one_weight("1231.14kg"))
    print(DataCleaning.convert_one_weight("12031g"))
    print(DataCleaning.convert_one_weight("3 x 123g"))
    print(DataCleaning.convert_one_weight("55ml"))
    print(DataCleaning.convert_one_weight("None"))
