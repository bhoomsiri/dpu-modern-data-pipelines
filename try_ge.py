import json

import great_expectations as gx
import pandas as pd
#from great_expectations.dataset import PandasDataset


def _validate_data():
    columns = ["NewConfirmed", "Date"]
    my_df = pd.read_csv("data.csv", names=columns)
    #my_df
    #print(my_df)

import pandas as pd


def _validate_data():
    columns = ["NewConfirmed", "Date"]
    df = pd.read_csv("data.csv", names=columns)

    valid_df = df[(df.NewConfirmed >= 0) & (df.NewConfirmed <= 322315)]
    dq = len(valid_df) / len(df)
    print(dq)

    assert len(valid_df) == len(df)


_validate_data()

    #results = my_df.expect_column_values_to_be_between(
    #    column="NewConfirmed",
        min_value=0,
        max_value=322315,
    )
    print(results)

    element_count = results["result"]["element_count"]
    unexpected_count = results["result"]["unexpected_count"]
    expected_count = element_count - unexpected_count
    validity = expected_count / element_count * 100
    print(f"Validity: {validity}")

    assert results["success"] is True


_validate_data()


def _validate_temperature_data():
    with open("weather_data_2024-02-03T04:00:00+00:00.json", "r") as f:
        data = json.load(f)
        print(data["main"])

        df = pd.DataFrame.from_records(data["main"], index=[0])
        print(df.head())

        dataset = PandasDataset(df)
        print(dataset.head())

        results = dataset.expect_column_values_to_be_between(
            column="temp",
            min_value=0,
            max_value=31,
        )
        print(results)


_validate_temperature_data()