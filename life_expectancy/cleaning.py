import argparse
import re
import pandas as pd
from life_expectancy.io import load_data, save_data


def unpivot_data(data: pd.DataFrame) -> pd.DataFrame:
    """
    Unpivot the data by splitting the 'unit,sex,age,geo\\time' column
    into separate columns and melt the DataFrame.
    """
    split_columns = data["unit,sex,age,geo\\time"].str.split(",", expand=True)
    data["unit"] = split_columns[0]
    data["sex"] = split_columns[1]
    data["age"] = split_columns[2]
    data["region"] = split_columns[3]
    data = data.drop(columns=["unit,sex,age,geo\\time"])
    data = pd.melt(
        data,
        id_vars=["unit", "sex", "age", "region"],
        var_name="year",
        value_name="value",
    )
    return data


def clean_data_types(data: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the data types of the 'year' and 'value' columns.
    Convert 'year' to int and 'value' to float.
    Remove rows with NaN values in 'value' column.
    """
    data["year"] = data["year"].astype("int64")
    data["value"] = data["value"].apply(lambda x: re.sub("[^\\d.]", "", x))
    data["value"] = pd.to_numeric(data["value"], errors="coerce")
    data = data.dropna(subset=["value"])
    return data


def filter_data(data: pd.DataFrame, region: str) -> pd.DataFrame:
    """
    Filter the data to include rows where 'geo\time' column is equal to the given region.
    """
    return data[data["region"] == region]


def clean_data(data: pd.DataFrame, country: str = "PT") -> pd.DataFrame:
    """
    Clean the data by performing loading, unpivot, cleaning data types
    and filtering
    """
    data = unpivot_data(data)
    data = clean_data_types(data)
    data = filter_data(data, country)
    data = data.reset_index(drop=True)
    return data


if __name__ == "__main__":  # pragma: no cover
    parser = argparse.ArgumentParser(description="Clean life expectancy data")
    parser.add_argument(
        "--country", default="PT", help="Country code to filter the data (default: PT)"
    )
    parser.add_argument(
        "--load_path",
        default="life_expectancy/data/eu_life_expectancy_raw.tsv",
        help="TSV file path",
    )
    parser.add_argument(
        "--save_path", default="life_expectancy/data/pt_life_expectancy.csv"
    )
    args = parser.parse_args()
    dataset = load_data(args.load_path)
    dataset = clean_data(dataset, args.country)
    save_data(dataset, args.save_path)

    sample_data = pd.DataFrame(
        {
            "unit,sex,age,geo\\time": ["U1,M,O,A1,PT", "U2,F,Y,A2,DE"],
            "value_1": [10, 20],
            "value_2": [30, 40],
        }
    )
    print(unpivot_data(sample_data))
    expected_output = pd.DataFrame(
        {
            "unit": ["U1", "U2", "U1", "U2"],
            "sex": ["M", "F", "F", "M"],
            "age": ["F", "M", "F", "M"],
            "region": ["A1", "A2", "A1", "A2"],
            "year": ["value_1", "value_1", "value_2", "value_2"],
            "value": [10, 20, 30, 40],
        }
    )
    print(expected_output)
